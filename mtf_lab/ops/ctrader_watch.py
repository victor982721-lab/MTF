"""Bounded, read-only cTrader observation runner.

The runner is deliberately below the CLI and above the already prepared
``CTraderProvider``.  It does not connect, authenticate, discover accounts,
or construct an executor.  A caller owns those gates and supplies a provider
whose session is ready to be observed.  The normal path reads the provider's
single-reader queue and invokes the provider's real normalizer; a small
The provider/client queue is the only supported input boundary; keeping the
protocol-message unit here avoids an adapter-specific record cursor.

The durable checkpoint combines the existing :class:`RuntimeCoordinator`
checkpoint with the cTrader capture cursor and the provider quote-book
diagnostic.  Quote state is never restored merely because a numeric generation
matches: a new provider starts with an empty book and must observe a complete
bid/ask quote before the continuity gate can recover.  A new provider is
always treated as a new transport, even if its numeric generation repeats.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, cast

from ..configuration import EffectiveConfig
from ..core.canary_quote import CanaryZeroSpreadAuthorization
from ..core.canonical import canonical_json, fingerprint
from ..core.cfd_simulation import CFD_PRODUCT, CFDConfig, CFDSimulationError, CFDSimulator, CFDTrade
from ..data.ctrader_market import CTraderProvider
from ..data.ctrader_protocol import PAYLOAD, WireMessage
from ..data.models import Bar, Event
from ..runtime import RuntimeCoordinator, runtime_simulation_config
from .ctrader_paper_adapters import signal_to_cfd_signal, spot_event_to_cfd_quote
from .persistence import IdempotencyConflict, SQLiteStore, payload_hash

WATCH_CHECKPOINT_VERSION = 1
PAPER_WATCH_CHECKPOINT_VERSION = 1
PAPER_WATCH_VARIANT = "ctrader_watch_cfd_paper"
_CONTROL_PAYLOADS = frozenset(
    PAYLOAD[name]
    for name in (
        "PROTO_HEARTBEAT_EVENT",
        "PROTO_OA_CLIENT_DISCONNECT_EVENT",
        "PROTO_OA_ACCOUNT_DISCONNECT_EVENT",
        "PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT",
    )
)


class CTraderWatchError(RuntimeError):
    """A watch cannot continue without weakening a data or safety gate."""


class WatchStopReason(StrEnum):
    DURATION = "DURATION"
    MAX_EVENTS = "MAX_EVENTS"
    IDLE_TIMEOUT = "IDLE_TIMEOUT"
    STOP_REQUESTED = "STOP_REQUESTED"
    SOURCE_END = "SOURCE_END"
    ERROR = "ERROR"


class StopSignal(Protocol):
    def is_set(self) -> bool: ...


@dataclass(frozen=True, slots=True)
class CTraderWatchOptions:
    """Bounds and checkpoint cadence for one observation slice.

    ``max_events`` counts protocol ``WireMessage`` inputs per invocation;
    one message may yield multiple normalized events/bars atomically.
    """

    duration_seconds: float | None = None
    max_events: int | None = None
    idle_timeout_seconds: float | None = 5.0
    poll_timeout_seconds: float = 0.25
    checkpoint_every: int = 100
    checkpoint_name: str = "ctrader-watch"
    resume: bool = True
    max_candles: int | None = 5_000
    mode: str | None = None
    technical_canary_quote_gap_seconds: float | None = None

    def __post_init__(self) -> None:
        for name in (
            "duration_seconds",
            "idle_timeout_seconds",
            "poll_timeout_seconds",
            "technical_canary_quote_gap_seconds",
        ):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not math.isfinite(float(value)) or float(value) < 0):
                raise ValueError(f"{name} debe ser finito y no negativo")
        if self.poll_timeout_seconds <= 0:
            raise ValueError("poll_timeout_seconds debe ser positivo")
        if self.technical_canary_quote_gap_seconds is not None and self.technical_canary_quote_gap_seconds <= 0:
            raise ValueError("technical_canary_quote_gap_seconds debe ser positivo")
        for name in ("max_events", "checkpoint_every", "max_candles"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or int(value) <= 0):
                raise ValueError(f"{name} debe ser positivo")
        if self.duration_seconds is None and self.max_events is None and self.idle_timeout_seconds is None:
            raise ValueError("el runner requiere duration, max_events o idle_timeout_seconds")
        if not str(self.checkpoint_name).strip():
            raise ValueError("checkpoint_name no puede estar vacío")
        object.__setattr__(self, "checkpoint_name", str(self.checkpoint_name).strip())
        if self.mode is not None:
            object.__setattr__(self, "mode", str(self.mode).strip().upper())


@dataclass(slots=True)
class CTraderWatchContext:
    """Prepared provider and durable application boundary supplied by a caller."""

    provider: CTraderProvider
    config: EffectiveConfig
    store: SQLiteStore
    provenance: Mapping[str, Any] = field(default_factory=dict)
    session_id: str | None = None
    stop_event: StopSignal | None = None
    clock: Callable[[], datetime] | None = None
    monotonic: Callable[[], float] | None = None
    close_provider: bool = True
    on_initialized: Callable[[str, str], None] | None = None
    # The PAPER product is local-only and has no transport side effects.  It
    # is enabled by default so the cTrader observation route exercises the
    # complete SpotEvent -> signal -> bid/ask fill path rather than silently
    # stopping at the detector.  Callers may supply an explicit CFD mapping
    # when the profile does not contain a ``[cfd]`` section.
    paper_enabled: bool = True
    paper_config: CFDConfig | Mapping[str, Any] | None = None
    # Optional causal native-bar prefix fetched by the composition root. The
    # runner consumes it through the same RuntimeCoordinator before polling
    # live SpotEvents; it never performs history I/O itself.
    bootstrap_bars: Mapping[str, tuple[Bar, ...]] = field(default_factory=dict)
    bootstrap_metadata: Mapping[str, Any] = field(default_factory=dict)
    # A typed scope is required before the bounded technical canary may accept
    # a genuinely observed bid == ask quote.  Generic watch/PAPER paths keep
    # this None and therefore retain the strict bid < ask gate.
    zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None


@dataclass(frozen=True, slots=True)
class CTraderWatchResult:
    """Bounded result for UI/query consumers; no credentials or raw tokens."""

    session_id: str
    analysis_id: str
    semantic_identity: str
    data_semantic_hash: str
    result_semantic_hash: str
    messages: int
    messages_this_run: int
    events: int
    events_this_run: int
    bars: int
    snapshots: int
    heartbeats: int
    ignored_messages: int
    duplicates: int
    generation_changes: int
    stop_reason: str
    elapsed_seconds: float
    idle_seconds: float
    clean_stop: bool
    resumed: bool
    status: Mapping[str, Any]
    provenance: Mapping[str, Any]
    paper_analysis_id: str | None = None
    paper: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": WATCH_CHECKPOINT_VERSION,
            "session_id": self.session_id,
            "analysis_id": self.analysis_id,
            "semantic_identity": self.semantic_identity,
            "data_semantic_hash": self.data_semantic_hash,
            "result_semantic_hash": self.result_semantic_hash,
            "messages": self.messages,
            "messages_this_run": self.messages_this_run,
            "events": self.events,
            "events_this_run": self.events_this_run,
            "bars": self.bars,
            "snapshots": self.snapshots,
            "heartbeats": self.heartbeats,
            "ignored_messages": self.ignored_messages,
            "duplicates": self.duplicates,
            "generation_changes": self.generation_changes,
            "stop_reason": self.stop_reason,
            "elapsed_seconds": self.elapsed_seconds,
            "idle_seconds": self.idle_seconds,
            "clean_stop": self.clean_stop,
            "resumed": self.resumed,
            "status": dict(self.status),
            "provenance": dict(self.provenance),
            "paper_analysis_id": self.paper_analysis_id,
            "paper": dict(self.paper),
        }


@dataclass(frozen=True, slots=True)
class _WatchMetadata:
    provider: str
    source_mode: str
    synthetic: bool
    environment: str
    network_performed: bool
    execution_enabled: bool = False
    mode: str = "LIVE"
    data_identity: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "source_mode": self.source_mode,
            "synthetic": self.synthetic,
            "environment": self.environment,
            "network_performed": self.network_performed,
            "execution_enabled": False,
            "mode": self.mode,
            "data_identity_hash": _short_hash(self.data_identity),
        }


@dataclass(slots=True)
class _WatchStats:
    messages: int = 0
    events: int = 0
    bars: int = 0
    snapshots: int = 0
    heartbeats: int = 0
    ignored_messages: int = 0
    duplicates: int = 0
    generation_changes: int = 0
    last_sequence: int | None = None
    last_generation: int | None = None
    last_activity_monotonic: float = 0.0
    checkpoint_events: int = 0
    bootstrap_bars: int = 0


def _short_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _parse_generation(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CTraderWatchError("connection_generation inválida")
    return value


def _parse_sequence(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CTraderWatchError("ingest_sequence inválida")
    return value


def _mode_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    text = str(raw).strip().upper()
    if text in {"OBSERVACIÓN EN DIRECTO", "OBSERVACION_EN_DIRECTO", "LIVE"}:
        return "LIVE"
    if text in {"SYNTHETIC", "SINTÉTICO", "SINTETICO", "OFFLINE"}:
        return "SYNTHETIC"
    if text == "REPLAY":
        return "REPLAY"
    raise CTraderWatchError(f"modo de observación no soportado: {value!r}")


def _connection_value(status: Any) -> str:
    value = getattr(status, "connection", status)
    return str(getattr(value, "value", value)).upper()


def _provider_endpoint(provider: Any) -> str:
    config = getattr(provider, "config", None)
    host = str(getattr(config, "host", "") or "").strip()
    port = getattr(config, "port", None)
    if host and port is not None:
        return f"{host}:{port}"
    return ""


def _validate_provider_environment(provider: CTraderProvider) -> None:
    provider_config = getattr(provider, "config", None)
    environment = getattr(provider_config, "environment", None)
    if environment is not None and str(environment).strip().lower() != "demo":
        raise CTraderWatchError("el runner read-only sólo permite provider.config.environment=DEMO")


def _close_provider(provider: CTraderProvider) -> BaseException | None:
    close = getattr(provider, "close", None)
    if not callable(close):
        return None
    try:
        close()
    except BaseException as exc:  # close must be attempted even on setup failure
        return exc
    return None


def _provenance_source(raw: Mapping[str, Any]) -> tuple[str, bool]:
    requested = str(raw.get("source_mode") or "").strip().upper()
    synthetic_value = raw.get("synthetic")
    if synthetic_value is not None and not isinstance(synthetic_value, bool):
        raise CTraderWatchError("provenance.synthetic debe ser booleano")
    synthetic = bool(synthetic_value) if synthetic_value is not None else requested == "SYNTHETIC_FIXTURE"
    source_mode = requested or ("SYNTHETIC_FIXTURE" if synthetic else "LIVE")
    if synthetic != (source_mode == "SYNTHETIC_FIXTURE"):
        raise CTraderWatchError("source_mode y synthetic son incompatibles")
    return source_mode, synthetic


def _provenance_environment(raw: Mapping[str, Any], synthetic: bool) -> str:
    environment = str(raw.get("environment") or ("OFFLINE" if synthetic else "UNKNOWN")).strip().upper()
    return environment or "UNKNOWN"


def _provenance_network(raw: Mapping[str, Any], synthetic: bool) -> bool:
    network_value = raw.get("network_performed", False)
    if not isinstance(network_value, bool):
        raise CTraderWatchError("provenance.network_performed debe ser booleano")
    if synthetic and network_value:
        raise CTraderWatchError("una fixture sintética no puede declarar red realizada")
    if raw.get("execution_enabled", False) is not False:
        raise CTraderWatchError("el runner de observación siempre exige execution_enabled=false")
    return network_value


def _provenance_identity(
    raw: Mapping[str, Any], provider_name: str, config: EffectiveConfig, source_mode: str, environment: str
) -> str:
    value = raw.get("data_identity", raw.get("stream_identity"))
    if value is None:
        value = {
            "provider": provider_name,
            "instrument": config.instrument,
            "source_mode": source_mode,
            "environment": environment,
        }
    return canonical_json(value)


def _safe_metadata(
    provider: CTraderProvider, config: EffectiveConfig, raw: Mapping[str, Any], mode: str
) -> _WatchMetadata:
    provider_name = str(raw.get("provider") or getattr(provider, "name", type(provider).__name__)).strip()
    if not provider_name:
        raise CTraderWatchError("provenance.provider no puede estar vacío")
    source_mode, synthetic = _provenance_source(raw)
    environment = _provenance_environment(raw, synthetic)
    if environment in {"REAL", "LIVE", "PRODUCTION"}:
        raise CTraderWatchError("el runner read-only no admite entorno REAL/LIVE")
    network_value = _provenance_network(raw, synthetic)
    data_identity = _provenance_identity(raw, provider_name, config, source_mode, environment)
    effective_mode = "SYNTHETIC" if synthetic else mode
    if effective_mode not in {"LIVE", "SYNTHETIC", "REPLAY"}:
        raise CTraderWatchError(f"modo no soportado para runner: {effective_mode}")
    return _WatchMetadata(
        provider_name,
        source_mode,
        synthetic,
        environment,
        network_value,
        False,
        effective_mode,
        data_identity,
    )


def _record_metadata(record: Any) -> dict[str, Any]:
    value = getattr(record, "metadata", {})
    return dict(value) if isinstance(value, Mapping) else {}


def _apply_quote_quality(metadata: dict[str, Any]) -> None:
    """Project cTrader's typed quote state into the runtime quality contract."""

    state = str(metadata.get("quality_state", "")).strip().upper()
    flags_value = metadata.get("quality_flags", ())
    flags = (
        {str(item).strip().lower() for item in flags_value} if isinstance(flags_value, (list, tuple, set)) else set()
    )
    reasons_value = metadata.get("quality_reasons", ())
    reasons = [str(item) for item in reasons_value] if isinstance(reasons_value, (list, tuple)) else []
    state_flag = {
        "STALE": "stale",
        "INVALID": "invalid",
        "DISCONNECTED": "disconnected",
        "UNKNOWN": "insufficient",
        "INCOMPLETE": "insufficient",
    }.get(state)
    if state_flag:
        flags.add(state_flag)
        reasons.append(f"quote_quality:{state}")
    if metadata.get("quote_usable") is False:
        flags.add("invalid")
        reasons.append("quote_quality:not_usable")
    if flags:
        metadata["quality_flags"] = sorted(flags)
    if reasons:
        metadata["quality_reasons"] = list(dict.fromkeys(reasons))


def _decorate_record(
    record: Event | Bar,
    metadata: _WatchMetadata,
    raw_synthetic: bool,
    *,
    technical_quote_mode: bool = False,
) -> Event | Bar:
    if raw_synthetic != metadata.synthetic:
        raise CTraderWatchError("la etiqueta synthetic del protocolo no coincide con el contexto de procedencia")
    record_metadata = {
        **_record_metadata(record),
        "provider": metadata.provider,
        "mode": metadata.mode,
        "source_mode": metadata.source_mode,
        "synthetic": metadata.synthetic,
        "synthetic_fixture": metadata.synthetic,
        "environment": metadata.environment,
        "network_performed": metadata.network_performed,
        "execution_enabled": False,
    }
    # A normalized SpotEvent with both legs is a quote at the core translation
    # boundary.  Strict/default coverage is unchanged; the marker is consumed
    # only by the opted-in continuous quote contract.
    if technical_quote_mode and isinstance(record, Event) and record.bid is not None and record.ask is not None:
        record_metadata.setdefault("event_kind", "quote")
    # Provider adapters expose a human-readable source quality label.  The
    # runtime's ``quality`` key is reserved for the typed DataQuality contract;
    # preserve the source label under the established non-blocking name.
    if isinstance(record_metadata.get("quality"), str):
        record_metadata["source_quality"] = record_metadata.pop("quality")
    _apply_quote_quality(record_metadata)
    return replace(record, synthetic=metadata.synthetic, metadata=record_metadata)


def _semantic_record(record: Event | Bar) -> dict[str, Any]:
    """Remove connection/session identifiers while retaining market meaning."""
    metadata = _record_metadata(record)
    semantic_metadata = {
        key: metadata[key]
        for key in (
            "provider",
            "source_mode",
            "synthetic",
            "synthetic_fixture",
            "environment",
            "network_performed",
            "execution_enabled",
            "quote_basis",
            "quality_state",
            "quality_reasons",
            "quality_flags",
            "quote_usable",
            "partial_update",
            "canary_zero_spread_authorized",
            "canary_zero_spread_approval_digest",
            "canary_zero_spread_raw_relation",
            "native_price_basis",
            "period_code",
            "relative_scale",
            "protocol_volume",
            "no_tick_interpolation",
            "closed_evidence",
        )
        if key in metadata
    }
    if isinstance(record, Event):
        return {
            "type": "event",
            "instrument": record.instrument,
            "event_time": record.event_time.astimezone(UTC).isoformat(),
            "price": record.price,
            "bid": record.bid,
            "ask": record.ask,
            "mid": record.mid,
            "price_basis": record.price_basis,
            "quantity": record.quantity,
            "is_snapshot": record.is_snapshot,
            "synthetic": record.synthetic,
            "metadata": semantic_metadata,
        }
    return {
        "type": "bar",
        "instrument": record.instrument,
        "interval_start": record.interval_start.astimezone(UTC).isoformat(),
        "interval_end": record.interval_end.astimezone(UTC).isoformat(),
        "open": record.open,
        "high": record.high,
        "low": record.low,
        "close": record.close,
        "resolution_seconds": record.resolution_seconds,
        "volume": record.volume,
        "trade_count": record.trade_count,
        "price_basis": record.price_basis,
        "closed": record.closed,
        "synthetic": record.synthetic,
        "revision": record.revision,
        "metadata": semantic_metadata,
    }


def _append_chain(previous: str, value: Any) -> str:
    return fingerprint({"previous": previous, "value": value})


def _stop_is_set(value: StopSignal | None) -> bool:
    return bool(value is not None and value.is_set())


class _WatchPaper:
    """Small local PAPER sink for one cTrader watch session.

    The watch already owns the only provider reader and the only
    ``RuntimeCoordinator``.  This sink therefore consumes detector deltas and
    normalized ``Event`` objects; it never creates another reader, indicator
    engine, strategy, socket, OAuth client, or broker order path.
    """

    def __init__(
        self,
        *,
        store: SQLiteStore,
        session_id: str,
        config: EffectiveConfig,
        paper_config: CFDConfig | Mapping[str, Any] | None,
        semantic_identity: str,
        runtime_analysis_id: str,
    ) -> None:
        raw_config: CFDConfig | Mapping[str, Any]
        if paper_config is None:
            values = dict(config.cfd)
            values.setdefault("instrument", config.instrument)
            raw_config = values
        elif isinstance(paper_config, CFDConfig):
            raw_config = paper_config
        else:
            values = dict(paper_config)
            values.setdefault("instrument", config.instrument)
            raw_config = values
        try:
            self.config = raw_config if isinstance(raw_config, CFDConfig) else CFDConfig.from_mapping(raw_config)
        except (CFDSimulationError, TypeError, ValueError) as exc:
            raise CTraderWatchError(f"configuración CFD PAPER inválida: {exc}") from exc
        expected = str(config.instrument).strip().upper().replace("-", "/")
        if self.config.instrument != expected:
            raise CTraderWatchError("configuración CFD PAPER usa otro instrumento")
        self.store = store
        self.session_id = session_id
        self.application_config_hash = config.config_hash
        self.analysis_id = store.create_analysis(
            session_id,
            dataset_hash=f"ctrader-watch:{semantic_identity}",
            config_hash=self.config.config_hash,
            variant=PAPER_WATCH_VARIANT,
            contract_hash=self.config.config_hash,
            partition="paper",
            code_version=config.version,
            identity_extra={
                "paper_watch_version": PAPER_WATCH_CHECKPOINT_VERSION,
                "runtime_analysis_id": runtime_analysis_id,
                "semantic_identity": semantic_identity,
            },
        )
        self._issues: deque[str] = deque(maxlen=256)
        self._resumed = False
        self.simulator = CFDSimulator(self.config, terminal_lookup=self._lookup_terminal)

    def _lookup_terminal(self, trade_id: str) -> CFDTrade | None:
        row = self.store.get_cfd_trade(self.session_id, self.analysis_id, trade_id)
        if row is None:
            return None
        payload = row.get("payload") if isinstance(row, Mapping) else None
        source = payload if isinstance(payload, Mapping) else row
        if not isinstance(source, Mapping):
            raise CTraderWatchError("fila CFD PAPER sin payload recuperable")
        try:
            trade = CFDTrade.from_mapping(source)
        except (CFDSimulationError, TypeError, ValueError) as exc:
            raise CTraderWatchError(f"fila CFD PAPER inválida: {exc}") from exc
        return trade if trade.is_terminal else None

    def _persist(self, trades: Iterable[CFDTrade]) -> None:
        for trade in trades:
            self.store.save_cfd_trade(
                self.session_id,
                self.analysis_id,
                trade,
                variant=PAPER_WATCH_VARIANT,
                partition="paper",
                analysis_config_hash=self.application_config_hash,
                contract_hash=self.config.config_hash,
            )

    def _issue(self, reason: str, *, quote_id: str | None = None) -> None:
        value = str(reason).strip() or "UNKNOWN"
        self._issues.append(f"paper_quote_blocked:{quote_id}:{value}" if quote_id else f"paper:{value}")

    def note(self, reason: str) -> None:
        """Retain a bounded product diagnostic without changing state."""

        self._issue(reason)

    def _event_admissible(self, event: Event) -> bool:
        metadata = event.metadata if isinstance(event.metadata, Mapping) else {}
        quote_id = event.source_event_id or event.data_id
        if event.is_snapshot:
            self._issue("SNAPSHOT", quote_id=quote_id)
            return False
        if event.bid is None:
            self._issue("MISSING_BID", quote_id=quote_id)
            return False
        if event.ask is None:
            self._issue("MISSING_ASK", quote_id=quote_id)
            return False
        # Equality is not a zero-spread executable quote.  It is the same
        # crossed/invalid class as bid > ask and must not be normalized away.
        if event.bid >= event.ask:
            self._issue("CROSSED", quote_id=quote_id)
            return False
        if bool(metadata.get("partial_update", False)):
            self._issue("PARTIAL_UPDATE", quote_id=quote_id)
            return False
        quality_state = str(metadata.get("quality_state", "")).strip().upper()
        if quality_state and quality_state != "VALID":
            self._issue(f"QUALITY_{quality_state}", quote_id=quote_id)
            return False
        if "quote_usable" in metadata and metadata.get("quote_usable") is not True:
            self._issue("QUOTE_NOT_USABLE", quote_id=quote_id)
            return False
        return True

    def on_signal(self, signal: Any, *, capture_hash: str) -> None:
        try:
            converted = signal_to_cfd_signal(signal, capture_hash=capture_hash)
            trades = self.simulator.submit_all(converted)
        except (CFDSimulationError, TypeError, ValueError) as exc:
            self._issue(f"SIGNAL_{getattr(exc, 'code', type(exc).__name__)}")
            return
        self._persist(trades)

    def on_event(self, event: Event, *, capture_hash: str) -> None:
        if not self._event_admissible(event):
            return
        try:
            quote = spot_event_to_cfd_quote(event, capture_hash=capture_hash)
            changed = self.simulator.on_quote(quote)
        except (CFDSimulationError, TypeError, ValueError) as exc:
            self._issue(getattr(exc, "code", type(exc).__name__), quote_id=event.source_event_id or event.data_id)
            return
        self._persist(changed)

    def advance(self, watermark: datetime) -> None:
        current = watermark.astimezone(UTC)
        previous = self.simulator.snapshot().get("last_watermark")
        if previous is not None:
            previous_dt = datetime.fromisoformat(str(previous).replace("Z", "+00:00"))
            if current < previous_dt:
                # A caller's wall clock can lag the provider's stamped
                # availability (notably after a bounded resume).  Do not
                # move the PAPER clock backwards or turn this observation
                # mismatch into a fake expiry; the next monotone quote/clock
                # event remains authoritative.
                return
        self._persist(self.simulator.advance(current, capture_complete=False))

    def disconnect(self, *, reason: str = "DISCONNECTED") -> None:
        self.simulator.disconnect(reason=reason)

    def reconnect(self, generation: int | None = None) -> None:
        self.simulator.reconnect(generation)

    def restore(self, state: Mapping[str, Any], *, generation: int | None = None) -> None:
        if state.get("version") != PAPER_WATCH_CHECKPOINT_VERSION:
            raise CTraderWatchError("versión de checkpoint PAPER cTrader incompatible")
        if str(state.get("analysis_id")) != self.analysis_id:
            raise CTraderWatchError("checkpoint PAPER pertenece a otro análisis")
        snapshot = state.get("simulator")
        if not isinstance(snapshot, Mapping):
            raise CTraderWatchError("checkpoint PAPER sin snapshot del simulador")
        try:
            # Restore the durable trade/cursor ledger first.  ``reconnect``
            # immediately clears the restored in-memory quote book and forces
            # a fresh baseline; a prior process's quote can never fill a new
            # signal after resume.
            self.simulator.restore(snapshot, config=self.config)
            self.simulator.reconnect(generation)
            # ``ingest_sequence`` is owned by the newly prepared cTrader
            # reader and may restart at zero.  The watch capture cursor and
            # market/availability clock remain the durable ordering proofs;
            # retaining the old process-local sequence would reject every
            # post-resume quote as out of order.
            self.simulator._last_sequence = None
        except (CFDSimulationError, TypeError, ValueError) as exc:
            raise CTraderWatchError(f"checkpoint PAPER inválido: {exc}") from exc
        for item in state.get("issues", ()):
            self._issues.append(str(item))
        self._resumed = True

    def checkpoint(self) -> dict[str, Any]:
        return {
            "version": PAPER_WATCH_CHECKPOINT_VERSION,
            "analysis_id": self.analysis_id,
            "simulator": self.simulator.snapshot(),
            "issues": list(self._issues),
        }

    def projection(self) -> dict[str, Any]:
        counters = dict(self.simulator.counters)
        return {
            "enabled": True,
            "product": CFD_PRODUCT,
            "variant": PAPER_WATCH_VARIANT,
            "analysis_id": self.analysis_id,
            "capture_complete": False,
            "finished": self.simulator.finished,
            "resumed": self._resumed,
            "trades": [trade.to_dict() for trade in self.simulator.trades],
            "counters": counters,
            "issues": list(self._issues),
            "connected": self.simulator._connected,
            "session_generation": self.simulator.session_generation,
            "snapshot_hash": self.simulator.snapshot()["snapshot_hash"],
        }


class CTraderWatchRunner:
    """Run one bounded read-only slice against a prepared provider."""

    def __init__(self, context: CTraderWatchContext, options: CTraderWatchOptions | None = None) -> None:
        self.context = context
        self.options = options or CTraderWatchOptions()
        self._clock = context.clock or (lambda: datetime.now(UTC))
        self._monotonic = context.monotonic or __import__("time").monotonic
        requested_mode = self.options.mode or context.provenance.get("mode") or context.config.mode
        mode = _mode_value(requested_mode)
        _validate_provider_environment(context.provider)
        self.metadata = _safe_metadata(context.provider, context.config, context.provenance, mode)
        self.mode = self.metadata.mode
        self.zero_spread_authorization = context.zero_spread_authorization
        if not isinstance(context.paper_enabled, bool):
            raise CTraderWatchError("paper_enabled debe ser booleano")
        self._validate_zero_spread_context()
        # A generation change is not a reconciliation.  The composition root
        # may pass an explicit, independently verified recovery fact after a
        # bounded stop and a newly prepared provider session; the runner never
        # infers it from reconnect/heartbeat alone.
        recovery_verified = context.provenance.get("continuity_verified", False)
        feed_verified = context.provenance.get("reconciliation_verified", False)
        if not isinstance(recovery_verified, bool) or not isinstance(feed_verified, bool):
            raise CTraderWatchError("las verificaciones de procedencia deben ser booleanas")
        self._recovery_verified = recovery_verified
        self._feed_verified = feed_verified
        self.semantic_identity = self._semantic_identity()
        self.stats = _WatchStats()
        self._coordinator: RuntimeCoordinator | None = None
        self._previous_watch: Mapping[str, Any] | None = None
        self._generation_recovery_pending = False
        self._chain = fingerprint({"watch": WATCH_CHECKPOINT_VERSION, "identity": self.semantic_identity})
        self._resumed = False
        self._run_messages_start = 0
        self._run_events_start = 0
        self._last_idle_tick_monotonic = 0.0
        self._paper: _WatchPaper | None = None
        self._last_record_signals: tuple[Any, ...] = ()
        self._last_decorated_record: Event | Bar | None = None

    def _zero_spread_identity(self) -> tuple[str, str, str, str, str]:
        provenance = self.context.provenance
        account_id = str(provenance.get("account_id") or "").strip()
        session_id = str(provenance.get("session_id") or "").strip()
        generation = str(provenance.get("connection_generation") or "").strip()
        endpoint = str(provenance.get("endpoint") or _provider_endpoint(self.context.provider)).strip()
        symbol = str(self.context.config.instrument).strip().upper().replace("-", "/")
        return account_id, session_id, generation, endpoint, symbol

    def _validate_zero_spread_context(self) -> None:
        authorization = self.zero_spread_authorization
        if authorization is None:
            return
        if not isinstance(authorization, CanaryZeroSpreadAuthorization):
            raise CTraderWatchError("zero-spread authorization debe ser tipada")
        if self.mode != "LIVE" or self.context.paper_enabled:
            raise CTraderWatchError("zero-spread sólo admite el runner LIVE técnico sin PAPER")
        if self.context.provenance.get("manual_technical") is not True:
            raise CTraderWatchError("zero-spread requiere manual_technical=true")
        account_id, session_id, generation, endpoint, symbol = self._zero_spread_identity()
        if not authorization.matches(
            account_id=account_id,
            session_id=session_id,
            connection_generation=generation,
            endpoint=endpoint,
            symbol=symbol,
            now=self._clock(),
            require_order_window=False,
        ):
            raise CTraderWatchError("zero-spread authorization no coincide con el contexto LIVE")
        setter = getattr(self.context.provider, "set_canary_zero_spread_authorization", None)
        if not callable(setter):
            raise CTraderWatchError("provider no expone scope zero-spread canary")
        try:
            setter(authorization)
        except Exception as exc:
            raise CTraderWatchError("provider rechazó el scope zero-spread canary") from exc

    def _zero_spread_event_usable(self, event: Event) -> bool:
        authorization = self.zero_spread_authorization
        if event.bid is None or event.ask is None or event.bid != event.ask:
            return False
        if not isinstance(authorization, CanaryZeroSpreadAuthorization):
            return False
        metadata = _record_metadata(event)
        if (
            metadata.get("quality_state") != "VALID"
            or metadata.get("quote_usable") is not True
            or bool(metadata.get("partial_update", False))
            or metadata.get("canary_zero_spread_authorized") is not True
            or metadata.get("canary_zero_spread_approval_digest") != authorization.approval_digest
            or str(metadata.get("canary_zero_spread_raw_relation", "")).upper() != "BID_EQUALS_ASK"
        ):
            return False
        account_id, session_id, generation, endpoint, symbol = self._zero_spread_identity()
        if str(metadata.get("connection_generation", generation)) != generation:
            return False
        return authorization.matches(
            account_id=account_id,
            session_id=session_id,
            connection_generation=generation,
            endpoint=endpoint,
            symbol=symbol,
            now=self._clock(),
            require_order_window=False,
        )

    @property
    def coordinator(self) -> RuntimeCoordinator:
        if self._coordinator is None:
            raise RuntimeError("runner no inicializado")
        return self._coordinator

    @property
    def last_record_signals(self) -> tuple[Any, ...]:
        """Signals emitted by the last transition, independent of history retention."""
        return self._last_record_signals

    @property
    def paper(self) -> _WatchPaper | None:
        """The local PAPER sink, when enabled for this watch session."""

        return self._paper

    def _semantic_identity(self) -> str:
        spec = getattr(self.context.provider, "spec", None)
        spec_dict = spec.to_dict() if spec is not None and hasattr(spec, "to_dict") else {}
        identity = {
            "provider": self.metadata.provider,
            "instrument": self.context.config.instrument,
            "source_mode": self.metadata.source_mode,
            "synthetic": self.metadata.synthetic,
            "environment": self.metadata.environment,
            "network_performed": self.metadata.network_performed,
            "data_identity": self.metadata.data_identity,
            "spec": spec_dict,
            "zero_spread_authorization": (
                self.zero_spread_authorization.to_dict() if self.zero_spread_authorization is not None else None
            ),
        }
        return fingerprint(identity)

    def _session_id(self) -> str:
        if self.context.session_id:
            existing = self.context.store.get_session(self.context.session_id)
            if existing is None:
                raise CTraderWatchError(f"sesión no encontrada: {self.context.session_id}")
            if str(existing.get("instrument", "")).upper() != self.context.config.instrument.upper():
                raise CTraderWatchError("instrumento incompatible con la sesión")
            existing_mode = str(existing.get("mode", "")).upper()
            if existing_mode != self.mode:
                raise CTraderWatchError(f"modo incompatible con la sesión: {existing_mode} != {self.mode}")
            existing_metadata = existing.get("metadata")
            if isinstance(existing_metadata, Mapping):
                existing_identity = existing_metadata.get("semantic_identity")
                if existing_identity is not None and str(existing_identity) != self.semantic_identity:
                    raise CTraderWatchError("la sesión pertenece a otra identidad semántica de datos")
            return self.context.session_id
        session_metadata = {
            "ctrader_watch": self.metadata.to_dict(),
            "semantic_identity": self.semantic_identity,
        }
        session_config = {
            **self.context.config.to_dict(),
            "ctrader_watch": {
                "checkpoint_name": self.options.checkpoint_name,
                "checkpoint_every": self.options.checkpoint_every,
                **self.metadata.to_dict(),
            },
        }
        return self.context.store.create_session(
            mode=self.mode,
            provider=self.metadata.provider,
            instrument=self.context.config.instrument,
            config=session_config,
            dataset_ref=f"ctrader-watch:{self.semantic_identity}",
            metadata=session_metadata,
        )

    def _contract_hash(self) -> str:
        simulation = runtime_simulation_config(self.context.config)
        return hashlib.sha256(
            json.dumps(simulation.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def _expected_analysis_id(self, session_id: str) -> str:
        identity = {
            "session_id": session_id,
            "dataset_hash": f"ctrader-watch:{self.semantic_identity}",
            "config_hash": self.context.config.config_hash,
            "variant": "trend_pullback_v1",
            "contract_hash": self._contract_hash(),
            "partition": "all",
            "code_version": self.context.config.version,
            "identity_extra": {
                "source_mode": self.metadata.source_mode,
                "environment": self.metadata.environment,
                "synthetic": self.metadata.synthetic,
            },
        }
        return "an_" + payload_hash(identity)[:32]

    def _exact_resume_checkpoint(self, session_id: str, expected_analysis_id: str) -> Mapping[str, Any] | None:
        checkpoints = self.context.store.list_checkpoints(session_id, self.options.checkpoint_name)
        if not checkpoints:
            return None
        if any(str(item.get("analysis_id") or "") != expected_analysis_id for item in checkpoints):
            raise CTraderWatchError(
                "checkpoint cTrader watch pertenece a otro análisis/configuración; no se usa alternate"
            )
        checkpoint = checkpoints[0]
        if str(checkpoint.get("analysis_id")) != expected_analysis_id:
            raise CTraderWatchError("checkpoint cTrader watch no pertenece al análisis exacto")
        return checkpoint

    def _restore_watch_checkpoint(self, checkpoint: Mapping[str, Any]) -> None:
        state = checkpoint.get("state")
        if not isinstance(state, Mapping):
            raise CTraderWatchError("checkpoint cTrader watch sin estado")
        if state.get("runtime_checkpoint_version") != 1:
            raise CTraderWatchError("versión de contrato runtime incompatible")
        if str(state.get("config_hash")) != self.context.config.config_hash:
            raise CTraderWatchError("config_hash del checkpoint no coincide")
        processor = state.get("processor")
        if not isinstance(processor, Mapping) or processor.get("checkpoint_version") != 1:
            raise CTraderWatchError("versión de contrato processor incompatible")
        watch = state.get("watch")
        if watch is None:
            raise CTraderWatchError("checkpoint cTrader watch sin estado watch")
        if not isinstance(watch, Mapping) or watch.get("version") != WATCH_CHECKPOINT_VERSION:
            raise CTraderWatchError("checkpoint cTrader watch desconocido")
        if str(watch.get("semantic_identity")) != self.semantic_identity:
            raise CTraderWatchError("checkpoint pertenece a otra identidad semántica de datos")
        chain = watch.get("data_chain")
        if not isinstance(chain, str) or not chain:
            raise CTraderWatchError("checkpoint sin cadena semántica de datos")
        self._previous_watch = watch
        self._chain = chain
        self._resumed = True
        self.stats.last_sequence = _parse_sequence(watch.get("last_sequence"))
        self.stats.last_generation = _parse_generation(watch.get("last_generation"))
        self.stats.messages = int(watch.get("messages", 0))
        self.stats.events = int(watch.get("events", 0))
        self.stats.bars = int(watch.get("bars", 0))
        self.stats.snapshots = int(watch.get("snapshots", 0))
        self.stats.heartbeats = int(watch.get("heartbeats", 0))
        self.stats.ignored_messages = int(watch.get("ignored_messages", 0))
        self.stats.duplicates = int(watch.get("duplicates", 0))
        self.stats.generation_changes = int(watch.get("generation_changes", 0))
        self.stats.bootstrap_bars = int(watch.get("bootstrap_bars", 0) or 0)
        # The quote state from disk is diagnostic and is never restored.  A
        # new process can recreate
        # generation ``1`` and a numeric equality is not proof of the same
        # transport.  The first complete quote of the prepared new session is
        # required, and even that does not clear the continuity gate unless
        # the caller supplies an independent reconciliation fact.
        current_generation = int(getattr(self.context.provider, "generation", 0))
        if self.stats.last_generation is not None:
            self._generation_recovery_pending = True
            self.context.provider.reset_discontinuity("RESUME_REQUIRES_FRESH_QUOTE", generation=current_generation)

    def _assert_capture_boundary(self, session_id: str, checkpoint: Mapping[str, Any]) -> None:
        state = checkpoint.get("state")
        watch = state.get("watch") if isinstance(state, Mapping) else None
        if not isinstance(watch, Mapping):
            raise CTraderWatchError("checkpoint cTrader watch sin frontera durable")
        last_sequence = _parse_sequence(watch.get("last_sequence"))
        cursor = checkpoint.get("cursor")
        if isinstance(cursor, Mapping) and cursor.get("ingest_sequence") is not None:
            cursor_sequence = _parse_sequence(cursor.get("ingest_sequence"))
            if cursor_sequence != last_sequence:
                raise CTraderWatchError("cursor de captura no coincide con la frontera durable")
        rows = (
            self.context.store.iter_capture_envelopes(
                session_id,
                after_sequence=last_sequence,
            )
            if last_sequence is not None
            else self.context.store.iter_capture_envelopes(session_id)
        )
        try:
            next(rows)
        except StopIteration:
            return
        raise CTraderWatchError(
            "captura contiene observaciones posteriores al checkpoint; reconcilie antes de reanudar"
        )

    def _load_previous_checkpoint(self, session_id: str) -> None:
        if not self.options.resume:
            return
        checkpoint = self._exact_resume_checkpoint(session_id, self._expected_analysis_id(session_id))
        if checkpoint is None:
            if self.context.session_id is not None:
                raise CTraderWatchError("sesión existente sin checkpoint exacto; no se degrada a corrida nueva")
            return
        self._assert_capture_boundary(session_id, checkpoint)
        self._restore_watch_checkpoint(checkpoint)

    def _bootstrap_history(self) -> None:
        """Feed one validated native-bar prefix before live polling.

        History bars are evidence and indicator state only: ``bootstrap=True``
        suppresses strategy decisions and the PAPER sink never receives a
        bar.  The composition root owns the network query and supplies the
        already validated mapping, keeping this runner's single-reader rule
        intact.
        """

        if not self.context.bootstrap_bars:
            return
        configured = {str(item.name).upper(): int(item.seconds) for item in self.context.config.timeframes}
        expected_instrument = str(self.context.config.instrument).strip().upper().replace("-", "/")
        cutoff = self._bootstrap_cutoff()
        ordered = sorted(
            self.context.bootstrap_bars.items(),
            key=lambda item: configured.get(str(item[0]).upper(), -1),
            reverse=True,
        )
        for raw_timeframe, raw_bars in ordered:
            self._bootstrap_timeframe(str(raw_timeframe), raw_bars, configured, expected_instrument, cutoff)
        self.coordinator.update_feed_state(
            connection="CONNECTED",
            reconciliation=self.coordinator.reconciliation_state,
            freshness="UNKNOWN" if self.mode == "LIVE" else "NOT_APPLICABLE",
            continuity="CONTINUOUS",
            blocked_reasons=self.coordinator.external_blocked_reasons,
            block_details={"warmup": dict(self.context.bootstrap_metadata)},
        )

    def _bootstrap_timeframe(
        self,
        raw_timeframe: str,
        raw_bars: Any,
        configured: Mapping[str, int],
        expected_instrument: str,
        cutoff: datetime | None,
    ) -> None:
        timeframe = str(raw_timeframe).strip().upper()
        if timeframe not in configured:
            raise CTraderWatchError(f"warmup timeframe no configurado: {timeframe}")
        if not isinstance(raw_bars, (tuple, list)):
            raise CTraderWatchError(f"warmup {timeframe} requiere una secuencia de barras")
        previous: Bar | None = None
        ordered = sorted(
            raw_bars,
            key=lambda item: item.interval_start if isinstance(item, Bar) else datetime.min.replace(tzinfo=UTC),
        )
        for bar in ordered:
            if not isinstance(bar, Bar):
                raise CTraderWatchError(f"warmup {timeframe} contiene un registro no-Bar")
            if bar.instrument != expected_instrument or bar.timeframe != timeframe:
                raise CTraderWatchError(f"warmup {timeframe} no coincide con instrumento/temporalidad")
            if not bar.closed or bar.available_at is None or bar.available_at < bar.interval_end:
                raise CTraderWatchError(f"warmup {timeframe} contiene barra no cerrada/disponible")
            if cutoff is not None and bar.interval_end > cutoff:
                raise CTraderWatchError(f"warmup {timeframe} contiene datos posteriores al cutoff causal")
            if previous is not None and bar.interval_start != previous.interval_end:
                raise CTraderWatchError(f"warmup {timeframe} contiene un hueco o solapamiento")
            result = self.coordinator.process(bar, bootstrap=True)
            if not result.accepted:
                raise CTraderWatchError(f"warmup {timeframe} fue rechazado por el procesador")
            previous = bar
            self.stats.bootstrap_bars += 1
            self.stats.bars += 1

    def _bootstrap_cutoff(self) -> datetime | None:
        raw = self.context.bootstrap_metadata.get("cutoff")
        if raw is None:
            return None
        if not isinstance(raw, str) or not raw.strip():
            raise CTraderWatchError("warmup cutoff debe ser timestamp UTC")
        try:
            parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise CTraderWatchError("warmup cutoff inválido") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise CTraderWatchError("warmup cutoff debe incluir zona horaria")
        return parsed.astimezone(UTC)

    def _record_normalization_issues(self, issues: Iterable[Any]) -> None:
        bounded = [str(item)[:200] for item in tuple(issues)[:8]]
        if self._paper is not None:
            for issue in bounded:
                self._paper.note(f"NORMALIZATION_{issue}")
        self.coordinator.update_feed_state(
            connection="CONNECTED",
            reconciliation="NEEDS_RECONCILIATION",
            freshness="BLOCKED",
            continuity="BROKEN",
            blocked_reasons=tuple(dict.fromkeys((*self.coordinator.external_blocked_reasons, "normalization_error"))),
            block_details={"normalization_error": {"issues": bounded}},
        )

    def _prepare(self) -> None:
        session_id = self._session_id()
        self._load_previous_checkpoint(session_id)
        dataset_hash = f"ctrader-watch:{self.semantic_identity}"
        technical_gap = (
            self.options.technical_canary_quote_gap_seconds
            if self.context.provenance.get("manual_technical") is True
            else None
        )
        coordinator_kwargs: dict[str, Any] = {}
        if technical_gap is not None:
            coordinator_kwargs["technical_canary_quote_gap_seconds"] = technical_gap
        self._coordinator = RuntimeCoordinator(
            self.context.store,
            session_id,
            self.context.config,
            mode=self.mode,
            dataset_hash=dataset_hash,
            variant="trend_pullback_v1",
            partition="all",
            checkpoint_name=self.options.checkpoint_name,
            checkpoint_every=self.options.checkpoint_every,
            source=self.metadata.provider,
            max_candles=self.options.max_candles,
            resume=self.options.resume,
            clock=self._clock,
            identity_extra={
                "source_mode": self.metadata.source_mode,
                "environment": self.metadata.environment,
                "synthetic": self.metadata.synthetic,
            },
            **coordinator_kwargs,
        )
        if self.context.paper_enabled:
            self._paper = _WatchPaper(
                store=self.context.store,
                session_id=session_id,
                config=self.context.config,
                paper_config=self.context.paper_config,
                semantic_identity=self.semantic_identity,
                runtime_analysis_id=self.coordinator.analysis_id,
            )
            if self._previous_watch is not None:
                previous_paper = self._previous_watch.get("paper")
                if isinstance(previous_paper, Mapping):
                    self._paper.restore(
                        previous_paper,
                        generation=_parse_generation(getattr(self.context.provider, "generation", None)),
                    )
                else:
                    # A checkpoint created before the PAPER sink was added is
                    # still readable, but it cannot prove the prior product
                    # state.  Start only the new local namespace and expose
                    # the missing evidence instead of claiming a resume.
                    self._paper.note("PAPER_STATE_UNAVAILABLE_ON_RESUME")
        if self._previous_watch is None:
            self._bootstrap_history()
        elif self.context.bootstrap_bars and int(self._previous_watch.get("bootstrap_bars", 0) or 0) <= 0:
            raise CTraderWatchError("checkpoint existente no conserva el warmup causal requerido")
        # A bounded slice is resumable, not terminal.  PAUSED is intentionally
        # outside RuntimeCoordinator's terminal blocking states.
        self.coordinator.capture_state = "CAPTURING"
        if self._generation_recovery_pending:
            self.coordinator.update_feed_state(
                connection="CONNECTED",
                reconciliation="NEEDS_RECONCILIATION",
                continuity="BROKEN",
                blocked_reasons=("feed_discontinuity",),
                block_details={"feed_discontinuity": {"reason": "resume_requires_fresh_quote", "active": True}},
            )
        else:
            reconciliation = "NOT_APPLICABLE"
            if self.mode == "LIVE" and self._previous_watch is not None:
                reconciliation = "VERIFIED" if self._feed_verified else "PENDING"
            self.coordinator.update_feed_state(
                connection="CONNECTED",
                reconciliation=reconciliation,
                freshness="UNKNOWN" if self.mode == "LIVE" else "NOT_APPLICABLE",
            )
        if self.context.on_initialized is not None:
            self.context.on_initialized(self.coordinator.session_id, self.coordinator.analysis_id)

    def _check_provider_ready(self) -> None:
        status = getattr(self.context.provider, "status", None)
        if status is None:
            return
        connection = _connection_value(status)
        if connection not in {"CONNECTED", "HEALTHY"}:
            raise CTraderWatchError(f"provider no está preparado para observar: {connection}")

    def _watch_state(self) -> dict[str, Any]:
        provider = self.context.provider
        quote_state = provider.snapshot_quote_state() if hasattr(provider, "snapshot_quote_state") else None
        state = {
            "version": WATCH_CHECKPOINT_VERSION,
            "semantic_identity": self.semantic_identity,
            "data_chain": self._chain,
            "last_sequence": self.stats.last_sequence,
            "last_generation": self.stats.last_generation,
            "messages": self.stats.messages,
            "events": self.stats.events,
            "bars": self.stats.bars,
            "snapshots": self.stats.snapshots,
            "heartbeats": self.stats.heartbeats,
            "ignored_messages": self.stats.ignored_messages,
            "duplicates": self.stats.duplicates,
            "generation_changes": self.stats.generation_changes,
            "provider_generation": getattr(provider, "generation", None),
            "provider_quote_state": quote_state,
            "bootstrap_bars": self.stats.bootstrap_bars,
            "bootstrap": dict(self.context.bootstrap_metadata),
            "execution_enabled": False,
        }
        if self._paper is not None:
            state["paper"] = self._paper.checkpoint()
        return state

    def _checkpoint(self) -> None:
        with self.context.store.atomic_batch():
            state = self.coordinator.checkpoint()
            state = {**state, "watch": self._watch_state()}
            cursor = dict(state.get("cursor", {}))
            cursor.update(
                {
                    "analysis_id": self.coordinator.analysis_id,
                    "ingest_sequence": self.stats.last_sequence,
                    "connection_generation": self.stats.last_generation,
                    "semantic_identity": self.semantic_identity,
                }
            )
            self.context.store.save_checkpoint(
                self.coordinator.session_id,
                self.options.checkpoint_name,
                cursor=cursor,
                events_processed=int(state.get("cursor", {}).get("ordinal", 0)),
                last_event_id=self.coordinator.processor.last_event_id,
                state=state,
                analysis_id=self.coordinator.analysis_id,
            )
        self.stats.checkpoint_events = self.stats.events

    def _paper_reconnect(self, generation: int) -> None:
        if self._paper is not None:
            self._paper.reconnect(generation)

    def _observe_provider_health(self) -> None:
        """Project reader/backpressure failures into the operational gate."""

        status = getattr(self.context.provider, "status", None)
        if status is None:
            return
        connection = _connection_value(status)
        needs = bool(getattr(status, "needs_reconciliation", False))
        dropped = int(getattr(status, "dropped_messages", 0) or 0)
        if not needs and connection in {"CONNECTED", "HEALTHY"}:
            return
        if connection not in {"CONNECTED", "HEALTHY"}:
            reason = "reader_failure" if getattr(status, "last_error", None) else "feed_discontinuity"
            state = "DISCONNECTED"
        elif dropped:
            reason = "market_backpressure"
            state = "CONNECTED"
        else:
            reason = "feed_discontinuity"
            state = connection or "UNKNOWN"
        self._generation_recovery_pending = True
        self._recovery_verified = False
        if self._paper is not None:
            self._paper.disconnect(reason=reason)
        self.coordinator.update_feed_state(
            connection=state,
            reconciliation="NEEDS_RECONCILIATION",
            freshness="BLOCKED" if state in {"CONNECTED", "HEALTHY"} else "DISCONNECTED",
            continuity="BROKEN",
            blocked_reasons=tuple(dict.fromkeys((*self.coordinator.external_blocked_reasons, reason))),
            block_details={
                reason: {
                    "active": True,
                    "dropped_messages": dropped,
                    "last_error": str(getattr(status, "last_error", "") or "")[:240],
                }
            },
        )

    def _observe_generation(self, generation: int | None) -> None:
        if generation is None:
            return
        previous = self.stats.last_generation
        if previous is None:
            self.stats.last_generation = generation
            if self._previous_watch is not None:
                checkpoint_generation = _parse_generation(self._previous_watch.get("last_generation"))
                if checkpoint_generation is not None and generation != checkpoint_generation:
                    self._generation_recovery_pending = True
                    if generation < checkpoint_generation:
                        raise CTraderWatchError("connection_generation retrocedió al reanudar")
        elif generation < previous:
            first_after_resume = self._previous_watch is not None and self.stats.messages == self._run_messages_start
            if not first_after_resume:
                raise CTraderWatchError("connection_generation retrocedió durante la observación")
            self.stats.generation_changes += 1
            self.stats.last_generation = generation
            self._generation_recovery_pending = True
            self.context.provider.reset_generation(generation)
            self._paper_reconnect(generation)
            self.coordinator.update_feed_state(
                connection="CONNECTED",
                reconciliation="NEEDS_RECONCILIATION",
                continuity="BROKEN",
                blocked_reasons=("feed_discontinuity",),
                block_details={
                    "feed_discontinuity": {
                        "reason": "new_process_generation_boundary",
                        "previous_generation": previous,
                        "generation": generation,
                        "active": True,
                    }
                },
            )
        elif generation > previous:
            self.stats.generation_changes += 1
            self.stats.last_generation = generation
            self._generation_recovery_pending = True
            self._recovery_verified = False
            self.context.provider.reset_generation(generation)
            self._paper_reconnect(generation)
            self.coordinator.update_feed_state(
                connection="CONNECTED",
                reconciliation="NEEDS_RECONCILIATION",
                continuity="BROKEN",
                blocked_reasons=("feed_discontinuity",),
                block_details={
                    "feed_discontinuity": {
                        "reason": "connection_generation_changed",
                        "generation": generation,
                        "active": True,
                    }
                },
            )

    def _resume_duplicate(self, message: WireMessage) -> bool:
        # Do not use the persisted source sequence as a replay filter.  A new
        # cTrader process may restart that counter at zero; filtering by it
        # would silently discard new ticks.  Runtime/event idempotency and the
        # capture ledger handle exact replays, while a reset is observable via
        # the generation/continuity gates.
        del message
        return False

    def _save_message(self, message: WireMessage) -> bool:
        try:
            envelope = message.capture_envelope()
        except Exception:
            self.stats.ignored_messages += 1
            return False
        if envelope.get("ingest_sequence") is None or envelope.get("connection_generation") is None:
            self.stats.ignored_messages += 1
            return False
        source_sequence = _parse_sequence(envelope.get("ingest_sequence"))
        durable_sequence = source_sequence
        if source_sequence is not None and self.stats.last_sequence is not None:
            # ``CTraderClient`` normally owns a monotonic global counter.  A
            # newly prepared process may start that local counter at zero;
            # allocate the next durable capture sequence rather than dropping
            # the new observation or changing the normalized source sequence.
            durable_sequence = max(source_sequence, self.stats.last_sequence + 1)
        if durable_sequence is not None and durable_sequence != source_sequence:
            payload = dict(envelope.get("payload", {}))
            payload["_source_ingest_sequence"] = source_sequence
            envelope = {**envelope, "ingest_sequence": durable_sequence, "payload": payload}
        try:
            self.context.store.save_capture_envelope(self.coordinator.session_id, envelope)
        except IdempotencyConflict as exc:
            # A reset local counter is handled above by allocating the next
            # durable sequence.  Reaching this branch means the same durable
            # identity carries different bytes/generation; continuing would
            # corrupt the capture prefix used for resume.
            raise CTraderWatchError("conflicto de payload en la captura durable") from exc
        if durable_sequence is not None:
            self.stats.last_sequence = durable_sequence
        return True

    def _raw_synthetic(self, message: WireMessage) -> bool:
        payload = message.payload
        return (
            bool(payload.get("synthetic_fixture", payload.get("synthetic", False)))
            if isinstance(payload, Mapping)
            else False
        )

    def _recover_after_quote(self, record: Event | Bar) -> None:
        if not self._generation_recovery_pending or not self._recovery_verified:
            return
        if not isinstance(record, Event):
            return
        if record.is_snapshot:
            return
        if not self._live_event_usable(record):
            return
        self._generation_recovery_pending = False
        self._recovery_verified = False
        self.coordinator.update_feed_state(
            connection="CONNECTED",
            reconciliation="RECONCILED",
            continuity="RECOVERED_BOUNDED",
            freshness="VALID",
            blocked_reasons=(),
        )

    def _process_record(self, record: Event | Bar, raw_synthetic: bool) -> bool:
        self._last_record_signals = ()
        decorated = _decorate_record(
            record,
            self.metadata,
            raw_synthetic,
            technical_quote_mode=(
                self.context.provenance.get("manual_technical") is True
                and self.options.technical_canary_quote_gap_seconds is not None
            ),
        )
        self._last_decorated_record = decorated
        self._recover_after_quote(decorated)
        if (
            isinstance(decorated, Event)
            and decorated.bid is not None
            and decorated.ask is not None
            and (
                decorated.bid > decorated.ask
                or (decorated.bid == decorated.ask and not self._zero_spread_event_usable(decorated))
            )
        ):
            self._record_normalization_issues(("quote_invalid",))
            self.coordinator.capture_only(decorated)
            self._update_live_freshness(decorated)
            self._chain = _append_chain(self._chain, _semantic_record(decorated))
            self.stats.events += 1
            return False
        basis = str(self.context.config.price_base).strip().lower()
        analysis_record = (isinstance(decorated, Event) and basis in {"mid", "bid", "ask"}) or (
            isinstance(decorated, Bar) and basis == "native"
        )
        if isinstance(decorated, Event) and decorated.is_snapshot:
            self.coordinator.capture_only(decorated)
            self.stats.snapshots += 1
        elif not analysis_record:
            # A SpotEvent may carry a native trendbar for evidence.  When the
            # configured analysis basis is bid/ask/mid, persist that bar but
            # do not feed it into the MTF detector: doing so would create a
            # price_base_mismatch and silently suppress every later signal.
            # The converse keeps a native-only context from treating a quote
            # as an OHLC bar.
            self.coordinator.capture_only(decorated)
        else:
            result = self.coordinator.process(decorated)
            self._last_record_signals = tuple(result.signals)
            if self._paper is not None:
                for signal in self._last_record_signals:
                    self._paper.on_signal(signal, capture_hash=self._chain)
        self._update_live_freshness(decorated)
        self._chain = _append_chain(self._chain, _semantic_record(decorated))
        self.stats.events += 1
        if isinstance(decorated, Bar):
            self.stats.bars += 1
        return True

    def _paper_on_events(self, events: Iterable[Event]) -> None:
        if self._paper is None:
            return
        for event in events:
            self._paper.on_event(event, capture_hash=self._chain)

    def _advance_paper(self, watermark: datetime) -> None:
        if self._paper is not None:
            self._paper.advance(watermark)

    def _live_event_usable(self, event: Event) -> bool:
        metadata = event.metadata if isinstance(event.metadata, Mapping) else {}
        if event.bid is None or event.ask is None:
            return False
        price_relation_valid = event.bid < event.ask or (
            event.bid == event.ask and self._zero_spread_event_usable(event)
        )
        return (
            not event.is_snapshot
            and price_relation_valid
            and metadata.get("quality_state") == "VALID"
            and metadata.get("quote_usable") is True
            and not bool(metadata.get("partial_update", False))
        )

    def _update_live_freshness(self, record: Event | Bar) -> None:
        """Project provider quote quality without turning bars into health."""

        if self.mode != "LIVE" or self._generation_recovery_pending or not isinstance(record, Event):
            return
        blocked = [
            reason
            for reason in self.coordinator.external_blocked_reasons
            if reason not in {"feed_stale", "quote_invalid", "quote_partial", "quote_snapshot"}
        ]
        if self._live_event_usable(record):
            self.coordinator.update_feed_state(
                connection="CONNECTED",
                freshness="VALID",
                reconciliation=self.coordinator.reconciliation_state,
                blocked_reasons=blocked,
            )
            return
        metadata = record.metadata if isinstance(record.metadata, Mapping) else {}
        reason = (
            "quote_partial"
            if bool(metadata.get("partial_update", False))
            else "quote_snapshot"
            if record.is_snapshot
            else "quote_invalid"
        )
        self.coordinator.update_feed_state(
            connection="CONNECTED",
            freshness="BLOCKED",
            reconciliation=self.coordinator.reconciliation_state,
            blocked_reasons=(*blocked, reason),
        )

    def _process_message(self, message: WireMessage) -> None:
        if self._resume_duplicate(message):
            return
        self._observe_generation(_parse_generation(message.connection_generation))
        sequence = _parse_sequence(message.ingest_sequence)
        self.stats.messages += 1
        if not self._save_message(message):
            return
        payload_type = message.payload_type_id
        if payload_type == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
            self.stats.heartbeats += 1
            watermark = message.available_at or message.received_at or self._clock()
            self.coordinator.heartbeat(watermark)
            if self._paper is not None:
                self._paper.advance(watermark)
            return
        if payload_type in _CONTROL_PAYLOADS or message.message_class == "connection":
            generation = _parse_generation(message.connection_generation)
            if generation is not None:
                self.context.provider.reset_discontinuity("SESSION_CONTROL", generation=generation)
            if self._paper is not None:
                self._paper.disconnect(reason="SESSION_CONTROL")
            self._generation_recovery_pending = True
            self._recovery_verified = False
            self.coordinator.update_feed_state(
                connection="DISCONNECTED",
                reconciliation="NEEDS_RECONCILIATION",
                continuity="BROKEN",
                blocked_reasons=("feed_discontinuity",),
            )
            return
        if payload_type != PAYLOAD["PROTO_OA_SPOT_EVENT"]:
            self.stats.ignored_messages += 1
            return
        self._process_spot_message(message, sequence)

    def _scoped_zero_spread_normalization(
        self,
        records: Iterable[Event | Bar],
        issues: Iterable[Any],
        raw_synthetic: bool,
    ) -> bool:
        """Keep default crossed diagnostics without blocking valid canary data."""

        issue_values = tuple(str(item).lower() for item in issues)
        if not issue_values or any("cruzad" not in item and "spread cero" not in item for item in issue_values):
            return False
        events = tuple(item for item in records if isinstance(item, Event))
        if not events:
            return False
        for event in events:
            decorated = _decorate_record(
                event,
                self.metadata,
                raw_synthetic,
                technical_quote_mode=(
                    self.context.provenance.get("manual_technical") is True
                    and self.options.technical_canary_quote_gap_seconds is not None
                ),
            )
            if not isinstance(decorated, Event) or not self._zero_spread_event_usable(decorated):
                return False
            metadata = _record_metadata(decorated)
            default_reasons = metadata.get("default_quality_reasons", ())
            if (
                str(metadata.get("default_quality_state", "")).upper() != "INVALID"
                or not isinstance(default_reasons, (list, tuple))
                or not default_reasons
                or any(str(reason).upper() != "CROSSED" for reason in default_reasons)
            ):
                return False
        return True

    def _process_spot_message(self, message: WireMessage, sequence: int | None) -> None:
        raw_payload = message.payload
        normalized = self.context.provider.normalize_spot(
            raw_payload,
            received_at=message.received_at or self._clock(),
            available_at=message.available_at,
            snapshot=bool(raw_payload.get("snapshot", raw_payload.get("isSnapshot", False)))
            if isinstance(raw_payload, Mapping)
            else False,
            sequence=sequence,
            generation=_parse_generation(message.connection_generation),
        )
        if normalized.issues:
            # An authorized zero-spread observation retains the default
            # crossed classification in metadata, but its effective typed
            # quality is valid.  That data-only exception must not become a
            # socket/reconciliation failure; any other normalization issue
            # remains a hard operational block.
            raw_synthetic = self._raw_synthetic(message)
            if not self._scoped_zero_spread_normalization(
                normalized.records,
                normalized.issues,
                raw_synthetic,
            ):
                self._record_normalization_issues(normalized.issues)
        raw_synthetic = self._raw_synthetic(message)
        paper_events: list[Event] = []
        for record in normalized.records:
            self._process_record(record, raw_synthetic)
            if not normalized.issues and isinstance(self._last_decorated_record, Event):
                paper_events.append(self._last_decorated_record)
        # Deliver quotes after the message's detector records.  This keeps a
        # signal emitted at the close of the current bar causal while still
        # allowing its own valid bid/ask observation to be the first eligible
        # PAPER fill.  No quote is reconstructed from a bar or from a missing
        # side; only normalized Event records cross this boundary.
        self._paper_on_events(paper_events)

    def _poll_timeout(self, started: float, now: float) -> float:
        elapsed = now - started
        remaining_duration = (
            self.options.duration_seconds - elapsed
            if self.options.duration_seconds is not None
            else self.options.poll_timeout_seconds
        )
        idle_elapsed = now - self.stats.last_activity_monotonic
        remaining_idle = (
            self.options.idle_timeout_seconds - idle_elapsed
            if self.options.idle_timeout_seconds is not None
            else self.options.poll_timeout_seconds
        )
        return min(
            self.options.poll_timeout_seconds,
            max(0.0, remaining_duration),
            max(0.0, remaining_idle),
        )

    def _poll_stop_reason(self, started: float) -> WatchStopReason | None:
        elapsed = self._monotonic() - started
        if self.options.duration_seconds is not None and elapsed >= self.options.duration_seconds:
            return WatchStopReason.DURATION
        if self.options.max_events is not None and (
            self.stats.messages - self._run_messages_start >= self.options.max_events
        ):
            return WatchStopReason.MAX_EVENTS
        if _stop_is_set(self.context.stop_event):
            return WatchStopReason.STOP_REQUESTED
        return None

    def _post_poll_stop_reason(self, started: float) -> WatchStopReason | None:
        now = self._monotonic()
        if self.options.duration_seconds is not None and now - started >= self.options.duration_seconds:
            return WatchStopReason.DURATION
        if (
            self.options.idle_timeout_seconds is not None
            and now - self.stats.last_activity_monotonic >= self.options.idle_timeout_seconds
        ):
            return WatchStopReason.IDLE_TIMEOUT
        if _stop_is_set(self.context.stop_event):
            return WatchStopReason.STOP_REQUESTED
        return None

    def _run_poll(self, poll_event: Callable[[float], WireMessage | None], started: float) -> WatchStopReason:
        while True:
            stop_reason = self._poll_stop_reason(started)
            if stop_reason is not None:
                return stop_reason
            self._observe_provider_health()
            message = poll_event(self._poll_timeout(started, self._monotonic()))
            post_poll_stop = self._post_poll_stop_reason(started)
            if post_poll_stop is not None:
                return post_poll_stop
            if message is None:
                now = self._clock()
                current = self.coordinator.last_received_at
                if current is not None and now < current:
                    now = current
                if self._monotonic() - self._last_idle_tick_monotonic >= 1.0:
                    self._advance_paper(now)
                    self.coordinator.tick(now)
                    self._last_idle_tick_monotonic = self._monotonic()
                self._observe_provider_health()
                continue
            self.stats.last_activity_monotonic = self._monotonic()
            self._process_message(message)
            if self.stats.events - self.stats.checkpoint_events >= self.options.checkpoint_every:
                self._checkpoint()

    def _poll_event(self) -> Callable[[float], WireMessage | None]:
        client = getattr(self.context.provider, "client", None)
        poll_event = cast(Callable[[float], WireMessage | None] | None, getattr(client, "poll_event", None))
        if not callable(poll_event):
            raise CTraderWatchError("provider requiere client.poll_event para el runner cTrader")
        return poll_event

    def _status_mapping(self) -> dict[str, Any]:
        status = self.coordinator.status()
        result = {
            "capture_id": status.capture_id,
            "analysis_id": status.analysis_id,
            "mode": status.mode,
            "connection": status.connection,
            "analysis_enabled": status.analysis_enabled,
            "analysis_blocked_reasons": list(status.analysis_blocked_reasons),
            "pending_simulations": status.pending_simulations,
            "completed_simulations": status.completed_simulations,
            "last_checkpoint_events": status.last_checkpoint_events,
            "capture_state": status.capture_state,
            "reconciliation_state": status.reconciliation_state,
            "freshness_state": status.freshness_state,
            "continuity_state": status.continuity_state,
            "last_market_time": status.last_market_time,
            "last_processed_at": status.last_processed_at,
            "last_heartbeat_at": status.last_heartbeat_at,
            "block_details": dict(status.block_details),
            "block_history": list(status.block_history),
            "warmup_pending": dict(self.coordinator.processor.status.get("warmup_pending", {})),
            "bootstrap_bars": self.stats.bootstrap_bars,
            "bootstrap": dict(self.context.bootstrap_metadata),
        }
        if self._paper is not None:
            paper = self._paper.projection()
            result["paper_analysis_id"] = self._paper.analysis_id
            result["paper"] = {
                "enabled": True,
                "product": paper["product"],
                "trades": len(paper["trades"]),
                "filled": paper["counters"].get("fills", 0),
                "closed": paper["counters"].get("closures", 0),
                "unknown": paper["counters"].get("unknown", 0),
                "quote_rejections": len(paper["issues"]),
            }
        else:
            result["paper"] = {"enabled": False}
        return result

    def _result(self, stop_reason: WatchStopReason, started: float, clean_stop: bool) -> CTraderWatchResult:
        paper = self._paper.projection() if self._paper is not None else {"enabled": False}
        result_identity = fingerprint(
            {
                "semantic_identity": self.semantic_identity,
                "data_semantic_hash": self._chain,
                "signals": [_semantic_signal(item) for item in self.coordinator.signals],
                "paper": {
                    "trades": paper.get("trades", ()),
                    "counters": paper.get("counters", {}),
                    "issues": paper.get("issues", ()),
                },
            }
        )
        now = self._monotonic()
        return CTraderWatchResult(
            self.coordinator.session_id,
            self.coordinator.analysis_id,
            self.semantic_identity,
            self._chain,
            result_identity,
            self.stats.messages,
            self.stats.messages - self._run_messages_start,
            self.stats.events,
            self.stats.events - self._run_events_start,
            self.stats.bars,
            self.stats.snapshots,
            self.stats.heartbeats,
            self.stats.ignored_messages,
            self.stats.duplicates,
            self.stats.generation_changes,
            str(stop_reason.value),
            now - started,
            now - self.stats.last_activity_monotonic,
            clean_stop,
            self._resumed,
            self._status_mapping(),
            {
                **self.metadata.to_dict(),
                "zero_spread_authorization": (
                    self.zero_spread_authorization.to_dict() if self.zero_spread_authorization is not None else None
                ),
            },
            self._paper.analysis_id if self._paper is not None else None,
            paper,
        )

    def _cleanup(self, clean_stop: bool) -> BaseException | None:
        first_error: BaseException | None = None

        def attempt(action: Callable[[], Any]) -> None:
            nonlocal first_error
            try:
                action()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc

        if self._coordinator is not None:
            attempt(lambda: self._advance_paper(self._clock()))
            attempt(lambda: self.coordinator.advance(self._clock(), complete=False))
            if clean_stop:
                self.coordinator.capture_state = "PAUSED"
            attempt(self._checkpoint)
        paper = self._paper
        if paper is not None:
            attempt(lambda: paper.disconnect(reason="WATCH_CLOSED"))
        if self._coordinator is not None and self.context.close_provider:
            needs_reconciliation = self._previous_watch is not None or self._generation_recovery_pending
            attempt(
                lambda: self.coordinator.update_feed_state(
                    connection="DISCONNECTED",
                    reconciliation=("NEEDS_RECONCILIATION" if needs_reconciliation else "NOT_APPLICABLE")
                    if self.mode == "LIVE"
                    else "NOT_APPLICABLE",
                    freshness="DISCONNECTED" if self.mode == "LIVE" else "NOT_APPLICABLE",
                    blocked_reasons=("provider_closed",) if self.mode == "LIVE" and needs_reconciliation else (),
                )
            )
            attempt(self._checkpoint)
        if self.context.close_provider:
            close_error = _close_provider(self.context.provider)
            if first_error is None:
                first_error = close_error
        return first_error

    def run(self) -> CTraderWatchResult:
        started = self._monotonic()
        self.stats.last_activity_monotonic = started
        stop_reason = WatchStopReason.SOURCE_END
        clean_stop = False
        primary_error: BaseException | None = None
        try:
            self._prepare()
            self._check_provider_ready()
            # Causal bootstrap is deliberately performed before polling and
            # may take longer than the live idle timeout.  Start the idle
            # budget at the first live-reader poll, not at process startup.
            self.stats.last_activity_monotonic = self._monotonic()
            self._last_idle_tick_monotonic = self.stats.last_activity_monotonic
            poll_event = self._poll_event()
            self._run_messages_start = self.stats.messages
            self._run_events_start = self.stats.events
            stop_reason = self._run_poll(poll_event, started)
            clean_stop = True
        except BaseException as exc:
            primary_error = exc
            stop_reason = WatchStopReason.ERROR
            if self._coordinator is not None:
                self.coordinator.capture_state = "ERROR"
        cleanup_error = self._cleanup(clean_stop)
        if primary_error is not None:
            if cleanup_error is not None:
                raise CTraderWatchError("falló la corrida y también su persistencia/cierre") from primary_error
            raise primary_error
        if cleanup_error is not None:
            raise CTraderWatchError("la corrida no pudo cerrar/persistir limpiamente") from cleanup_error
        return self._result(stop_reason, started, clean_stop)


def _semantic_signal(signal: Any) -> dict[str, Any]:
    if hasattr(signal, "as_dict"):
        raw = signal.as_dict()
    elif hasattr(signal, "to_dict"):
        raw = signal.to_dict()
    elif isinstance(signal, Mapping):
        raw = dict(signal)
    else:
        return {"type": type(signal).__name__}
    if not isinstance(raw, Mapping):
        return {"type": type(signal).__name__}
    return {
        str(key): value
        for key, value in raw.items()
        if str(key) not in {"signal_id", "episode_id", "source_event_id", "source_sequence"}
    }


def run_ctrader_watch(
    context: CTraderWatchContext,
    options: CTraderWatchOptions | None = None,
) -> CTraderWatchResult:
    """Functional entry point used by the future CLI/UI composition root."""

    try:
        runner = CTraderWatchRunner(context, options)
    except BaseException as exc:
        close_error = _close_provider(context.provider) if context.close_provider else None
        if close_error is not None:
            raise CTraderWatchError("falló la construcción y también el cierre del provider") from exc
        raise
    return runner.run()


__all__ = [
    "CTraderWatchContext",
    "CTraderWatchError",
    "CTraderWatchOptions",
    "CTraderWatchResult",
    "CTraderWatchRunner",
    "PAPER_WATCH_CHECKPOINT_VERSION",
    "PAPER_WATCH_VARIANT",
    "WATCH_CHECKPOINT_VERSION",
    "WatchStopReason",
    "run_ctrader_watch",
]
