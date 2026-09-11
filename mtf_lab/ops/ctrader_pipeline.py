"""Pipeline local cTrader -> RuntimeCoordinator -> CFD paper.

Este módulo es un adaptador de integración, no un proveedor nuevo: usa la
normalización existente de :mod:`mtf_lab.data.ctrader`, el detector incremental
existente y :class:`CFDSimulator`.  No abre TCP/TLS, no autentica cuentas y no
contiene órdenes, ejecutor DEMO ni contratos binarios.

La captura conserva por separado:

* ``Event`` de spot con bid/ask/mid y snapshots;
* ``Bar`` nativas de cTrader, con escala relativa/procedencia;
* el conjunto que realmente alimenta al detector, elegido por la base de
  precio efectiva (MID/BID/ASK usa eventos; TRADED usa trendbars nativas).

La ruta de papel sólo consume señales auténticas emitidas por
``RuntimeCoordinator``.  Sus productos se guardan en la tabla v3 ``simulations``
como ``CFD_PAPER`` y el snapshot JSON se guarda en ``checkpoints``; no se
reutiliza la tabla para fingir una cuenta, un fill de broker o una orden.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
from typing import Any

from ..configuration import EffectiveConfig
from ..core import OperationMode
from ..core.strategy import Signal
from ..data.ctrader import (
    CTraderConfig,
    CTraderInstrumentSpec,
    CTraderNormalizationResult,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
    synthetic_spot_event,
    synthetic_trendbar,
)
from ..data.models import Bar, Event
from ..ops.cfd_simulation import (
    CFDConfig,
    CFDQuote,
    CFDReplayResult,
    CFDSignal,
    CFDSimulator,
    TradeState,
)
from ..ops.persistence import SQLiteStore, payload_hash
from ..runtime import ReplayResult, RuntimeCoordinator
from ..runtime.state import signal_from_dict


PAPER_PRODUCT = "FOREX_CFD_LOCAL_PAPER"
PAPER_VARIANT = "ctrader_cfd_paper"
SCHEMA_VERSION = 3


class CTraderPipelineError(ValueError):
    """Entrada o combinación incompatible en la frontera del pipeline."""


def _mode(value: str | OperationMode) -> str:
    if isinstance(value, OperationMode):
        return value.value
    text = str(value).strip().upper()
    if text in {"LIVE", "OBSERVACIÓN EN DIRECTO", "OBSERVACION_EN_DIRECTO"}:
        return "LIVE"
    if text == "REPLAY":
        return "REPLAY"
    raise CTraderPipelineError(f"modo cTrader no soportado por este pipeline: {value!r}")


def _utc(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CTraderPipelineError(f"{name} debe ser datetime con zona horaria")
    return value.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


def _payload(value: Any) -> Mapping[str, Any]:
    if isinstance(value, WireMessage):
        value = value.payload
    if not isinstance(value, Mapping) and hasattr(value, "DESCRIPTOR") and hasattr(value, "SerializeToString"):
        try:
            from google.protobuf.json_format import MessageToDict
            value = MessageToDict(value, preserving_proto_field_name=True)
        except Exception as exc:
            raise CTraderPipelineError("no se pudo convertir SpotEvent Protobuf a mapping") from exc
    if not isinstance(value, Mapping):
        raise CTraderPipelineError(f"SpotEvent debe ser mapping/WireMessage, llegó {type(value).__name__}")
    return value


def _payload_sequence(value: Mapping[str, Any]) -> int | str:
    for key in ("sequence", "sourceSequence", "source_sequence", "sequenceNumber", "sequence_number"):
        if value.get(key) is not None:
            return value[key]
    # La secuencia sintética debe ser estable cuando se reordena una captura;
    # nunca se usa el ordinal de llegada como identidad del mercado.
    return "payload-" + payload_hash(value)[:24]


def _payload_received_at(value: Mapping[str, Any]) -> datetime | None:
    raw = value.get("timestamp", value.get("timestamp_ms"))
    if raw is None:
        return None
    try:
        number = float(raw)
    except (TypeError, ValueError):
        return None
    # cTrader SpotEvent usa milisegundos desde Unix; valores pequeños se
    # aceptan como segundos sólo para fixtures explícitos.
    if abs(number) > 10_000_000_000:
        number /= 1000.0
    return datetime.fromtimestamp(number, UTC)


def _capture_processing_key(value: Mapping[str, Any]) -> tuple[Any, ...]:
    source_time = _payload_received_at(value)
    sequence = value.get("sequence", value.get("sourceSequence", value.get("source_sequence")))
    try:
        sequence_key = (0, int(sequence))
    except (TypeError, ValueError):
        sequence_key = (1, str(sequence or ""))
    return (source_time or datetime.min.replace(tzinfo=UTC), sequence_key, payload_hash(value))


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return _iso(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    if hasattr(value, "DESCRIPTOR") and hasattr(value, "SerializeToString"):
        try:
            from google.protobuf.json_format import MessageToDict
            return _jsonable(MessageToDict(value, preserving_proto_field_name=True))
        except Exception:
            return {"protobuf_type": type(value).__name__}
    if hasattr(value, "value"):
        return value.value
    return value


def _ordered_record_key(record: Event | Bar) -> tuple[Any, ...]:
    if isinstance(record, Event):
        return (record.effective_available_at, record.event_time, 1, record.data_id)
    return (record.available_at or record.interval_end, record.interval_end, 0, record.data_id)


def _decorate_provenance(
    metadata: Mapping[str, Any],
    *,
    mode: str,
    source_mode: str,
    instrument: str,
    price_basis: str,
    resolution: str | None = None,
    synthetic: bool = False,
) -> dict[str, Any]:
    result = dict(metadata)
    # cTrader normalizer exposes PUBLIC_PROVIDER as adapter metadata.  It is
    # not one of the core quality flags; preserve it under a source-only key
    # instead of letting the strict data->core translator reject the record.
    source_quality = result.pop("quality", None)
    if source_quality is not None:
        result["source_quality"] = source_quality
    result["source_mode"] = source_mode
    result["mode"] = mode
    result["synthetic_fixture"] = bool(synthetic)
    result["provenance"] = {
        **(dict(result.get("provenance")) if isinstance(result.get("provenance"), Mapping) else {}),
        "provider": "ctrader-open-api",
        "mode": mode,
        "source_mode": source_mode,
        "instrument": instrument,
        "price_basis": price_basis,
        "resolutions": [resolution] if resolution else [],
        "synthetic": bool(synthetic),
    }
    return result


def _rebind_event(event: Event, *, mode: str, synthetic: bool = False) -> Event:
    source_mode = "SYNTHETIC_FIXTURE" if synthetic else str((event.metadata or {}).get("mode", "LIVE"))
    metadata = _decorate_provenance(
        event.metadata,
        mode=mode,
        source_mode=source_mode,
        instrument=event.instrument,
        price_basis=event.price_basis,
        resolution="event",
        synthetic=synthetic,
    )
    return replace(event, metadata=metadata)


def _rebind_bar(bar: Bar, *, mode: str, synthetic: bool = False) -> Bar:
    source_mode = "SYNTHETIC_FIXTURE" if synthetic else str((bar.metadata or {}).get("mode", "LIVE"))
    metadata = _decorate_provenance(
        bar.metadata,
        mode=mode,
        source_mode=source_mode,
        instrument=bar.instrument,
        price_basis=bar.price_basis,
        resolution=bar.resolution,
        synthetic=synthetic,
    )
    return replace(bar, metadata=metadata)


@dataclass(frozen=True, slots=True)
class CTraderCapture:
    """Captura normalizada de SpotEvents y trendbars, con hash estable."""

    payloads: tuple[Mapping[str, Any], ...]
    records: tuple[Event | Bar, ...]
    quote_events: tuple[Event, ...]
    bars: tuple[Bar, ...]
    issues: tuple[str, ...]
    capture_id: str
    capture_hash: str
    provenance: Mapping[str, Any]
    snapshot_count: int = 0

    @property
    def events(self) -> tuple[Event, ...]:
        return self.quote_events

    @property
    def is_complete(self) -> bool:
        return bool(self.records) and not self.issues

    def to_dict(self, *, include_payloads: bool = False) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "capture_id": self.capture_id,
            "capture_hash": self.capture_hash,
            "record_count": len(self.records),
            "quote_event_count": len(self.quote_events),
            "bar_count": len(self.bars),
            "snapshot_count": self.snapshot_count,
            "synthetic": bool(self.provenance.get("synthetic", False)),
            "issues": list(self.issues),
            "provenance": dict(self.provenance),
        }
        if include_payloads:
            result["payloads"] = [_jsonable(item) for item in self.payloads]
        return result


@dataclass(frozen=True, slots=True)
class CTraderPipelineResult:
    """Resultado auditable de una corrida de captura y CFD paper."""

    capture: CTraderCapture
    session_id: str
    runtime_analysis_id: str
    paper_analysis_id: str
    analysis_basis: str
    analysis_records: tuple[Event | Bar, ...]
    runtime_result: ReplayResult
    runtime_status: Mapping[str, Any]
    signals: tuple[Signal, ...]
    cfd_signals: tuple[CFDSignal, ...]
    paper: CFDReplayResult
    snapshot: Mapping[str, Any]
    snapshot_hash: str

    @property
    def trades(self):
        return self.paper.trades

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "product": PAPER_PRODUCT,
            "session_id": self.session_id,
            "runtime_analysis_id": self.runtime_analysis_id,
            "paper_analysis_id": self.paper_analysis_id,
            "analysis_basis": self.analysis_basis,
            # Reports carry raw normalized-source payloads so the same input
            # can be replayed; compact SQLite/UI snapshots intentionally omit
            # them and retain hashes/provenance instead.
            "capture": self.capture.to_dict(include_payloads=True),
            "runtime_result": self.runtime_result.to_dict(),
            "runtime_status": _jsonable(self.runtime_status),
            "signals": [signal.as_dict() for signal in self.signals],
            "cfd_signals": [signal.to_dict() for signal in self.cfd_signals],
            "paper": self.paper.to_dict(),
            "snapshot_hash": self.snapshot_hash,
        }


def normalize_ctrader_capture(
    payloads: Iterable[Mapping[str, Any] | WireMessage],
    *,
    spec: CTraderInstrumentSpec,
    quote_basis: str = "mid",
    mode: str | OperationMode = "REPLAY",
    received_at: datetime | Callable[[int, Mapping[str, Any]], datetime] | None = None,
) -> CTraderCapture:
    """Normaliza una secuencia de ProtoOASpotEvent sin abrir red.

    ``received_at`` es inyectable para que los fixtures sean reproducibles; en
    un adaptador live el caller puede proveer ``datetime.now(UTC)`` por evento.
    Las identidades se derivan del payload/proveedor, no del orden local.
    """

    capture_mode = _mode(mode)
    materialized = tuple(_payload(item) for item in payloads)
    records: list[Event | Bar] = []
    quotes: list[Event] = []
    bars: list[Bar] = []
    issues: list[str] = []
    snapshot_count = 0
    # Reuse the provider's stateful bid/ask normalizer without connecting its
    # transport. This preserves partial quote legs during replay as well as in
    # live streaming, while retaining native trendbars unchanged.
    stateful_provider = CTraderProvider(
        CTraderConfig(
            symbol=spec.symbol,
            symbol_id=spec.symbol_id,
            digits=spec.digits,
            pip_position=spec.pip_position,
            price_scale=spec.price_scale,
            quote_basis=quote_basis,
        ),
        transport=DeterministicTransport(),
    )
    processing = tuple(sorted(enumerate(materialized), key=lambda item: _capture_processing_key(item[1])))
    normalized_rows: dict[int, tuple[CTraderNormalizationResult, bool]] = {}
    for original_index, raw in processing:
        if callable(received_at):
            received = _utc(received_at(original_index, raw), name="received_at")
        elif received_at is not None:
            received = _utc(received_at, name="received_at")
        else:
            source_time = _payload_received_at(raw)
            received = source_time or datetime.now(UTC)
        normalized: CTraderNormalizationResult = stateful_provider.normalize_spot(
            raw,
            received_at=received,
            snapshot=bool(raw.get("snapshot", raw.get("isSnapshot", False))),
            sequence=_payload_sequence(raw),
        )
        normalized_rows[original_index] = (
            normalized,
            bool(raw.get("synthetic_fixture", raw.get("synthetic", False))),
        )
        if normalized.snapshot:
            snapshot_count += 1
        # One-sided quote updates are temporary state conditions. If a later
        # payload supplies the missing leg, do not poison the completed capture
        # with the transient quote_basis diagnostic.
        issues.extend(issue for issue in normalized.issues if not issue.startswith("quote_basis="))
    # Preserve the caller's payload order in the capture view while stateful
    # normalization itself was applied in causal time order.
    for original_index in range(len(materialized)):
        normalized, synthetic = normalized_rows[original_index]
        for event in normalized.quote_events:
            rebound = _rebind_event(event, mode=capture_mode, synthetic=synthetic)
            quotes.append(rebound)
            records.append(rebound)
        for bar in normalized.bars:
            rebound = _rebind_bar(bar, mode=capture_mode, synthetic=synthetic)
            bars.append(rebound)
            records.append(rebound)
    # Hash normalized records in causal identity order, making replay of the
    # same capture independent of the input list order.
    ordered = tuple(sorted(records, key=_ordered_record_key))
    normalized_rows = [_jsonable(record.to_dict()) for record in ordered]
    digest = payload_hash(normalized_rows)
    capture_id = "cap_" + digest[:32]
    timestamps: list[datetime] = []
    for record in records:
        timestamps.append(record.event_time if isinstance(record, Event) else record.interval_start)
        timestamps.append(record.event_time if isinstance(record, Event) else record.interval_end)
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "provider": "ctrader-open-api",
        "mode": capture_mode,
        "source_mode": "SYNTHETIC_FIXTURE" if any(bool(raw.get("synthetic_fixture", raw.get("synthetic", False))) for raw in materialized) else "LIVE",
        "instrument": spec.symbol,
        "symbol_id": spec.symbol_id,
        "price_basis": str(quote_basis).lower(),
        "resolutions": sorted({bar.resolution for bar in bars}),
        "source_hash": digest,
        "capture_hash": digest,
        "coverage_start": _iso(min(timestamps)) if timestamps else None,
        "coverage_end": _iso(max(timestamps)) if timestamps else None,
        "synthetic": any(bool(raw.get("synthetic_fixture", raw.get("synthetic", False))) for raw in materialized),
        "notes": (
            "captura normalizada de ProtoOASpotEvent",
            "trendbars nativas; no se interpolan ticks",
            "fixture local si el caller usa synthetic_ctrader_payloads",
        ),
    }
    return CTraderCapture(
        payloads=materialized,
        records=tuple(records),
        quote_events=tuple(quotes),
        bars=tuple(bars),
        issues=tuple(issues),
        capture_id=capture_id,
        capture_hash=digest,
        provenance=provenance,
        snapshot_count=snapshot_count,
    )


def spot_event_to_cfd_quote(event: Event, *, capture_hash: str | None = None) -> CFDQuote:
    """Convierte sólo un quote con bid y ask explícitos a CFDQuote.

    Los snapshots y eventos sin timestamp de origen se conservan, pero quedan
    con calidad no utilizable por el simulador; nunca se convierten en un fill.
    """

    if not isinstance(event, Event):
        raise CTraderPipelineError("se esperaba un Event normalizado de cTrader")
    if event.bid is None or event.ask is None:
        raise CTraderPipelineError("SpotEvent sin bid/ask explícitos; no se fabrica quote")
    source_timestamp_missing = bool((event.metadata or {}).get("source_timestamp_missing", False))
    synthetic = bool((event.metadata or {}).get("synthetic_fixture", False) or ((event.metadata or {}).get("provenance", {}) or {}).get("synthetic", False))
    quality = "SNAPSHOT" if event.is_snapshot else ("UNKNOWN" if source_timestamp_missing else ("SYNTHETIC" if synthetic else "VALID"))
    metadata = {
        "provider": "ctrader-open-api",
        "event_data_id": event.data_id,
        "source_event_id": event.source_event_id,
        "source_timestamp_missing": source_timestamp_missing,
        "is_snapshot": event.is_snapshot,
        "synthetic": synthetic,
        "capture_hash": capture_hash,
        "provenance": dict((event.metadata or {}).get("provenance", {})) if isinstance((event.metadata or {}).get("provenance"), Mapping) else {},
    }
    return CFDQuote(
        instrument=event.instrument,
        market_time=event.event_time,
        bid=str(event.bid),
        ask=str(event.ask),
        quote_id=event.source_event_id or event.data_id,
        available_at=event.effective_available_at,
        source=event.source,
        sequence=event.source_sequence,
        quality=quality,
        metadata=metadata,
    )


def signal_to_cfd_signal(signal: Signal | Mapping[str, Any], *, capture_hash: str | None = None, strategy: str = "trend_pullback_v1") -> CFDSignal:
    """Adaptador puro: sólo acepta señales emitidas por el detector."""

    if isinstance(signal, Mapping):
        signal = signal_from_dict(signal)
    if not isinstance(signal, Signal):
        raise CTraderPipelineError("se esperaba Signal del detector RuntimeCoordinator")
    metadata = {
        "source": "RuntimeCoordinator",
        "capture_hash": capture_hash,
        "episode_id": signal.episode_id,
        "context_start": _iso(signal.context_start),
        "preparation_start": _iso(signal.preparation_start),
        "trigger_start": _iso(signal.trigger_start),
        "trigger_end": _iso(signal.trigger_end),
        "values": dict(signal.values),
        "mode": signal.mode.value,
        "quality_flags": sorted(flag.value for flag in signal.quality.flags),
    }
    return CFDSignal(
        signal_id=signal.signal_id,
        instrument=signal.instrument,
        direction=signal.direction,
        detected_at=signal.detected_at,
        available_at=signal.detected_at,
        strategy=strategy,
        quality=signal.quality.status,
        metadata=metadata,
    )


def _paper_outcome(trade: Any) -> str:
    if trade.state is TradeState.PENDING or trade.state is TradeState.FILLED:
        return "PENDING"
    if trade.state is TradeState.UNKNOWN or trade.state is TradeState.REJECTED:
        return "INDETERMINATE"
    if trade.net_pnl is None:
        return "INDETERMINATE"
    if trade.net_pnl > 0:
        return "WIN"
    if trade.net_pnl < 0:
        return "LOSS"
    return "TIE"


class _CFDOnlyRuntimeCoordinator(RuntimeCoordinator):
    """RuntimeCoordinator real, sin persistir su simulador de contratos.

    El runtime sigue agregando, calentando, evaluando y guardando decisiones y
    señales.  Se descarta únicamente su libreta virtual genérica para que esta
    ruta no mezcle productos binarios con el producto CFD PAPER.
    """

    def _persist_simulation(self, item: Any) -> None:  # pragma: no cover - exercised through process/replay
        return None

    def _clear_virtual_book(self) -> None:
        book = getattr(self.processor, "_simulation_book", None)
        if book is not None:
            book.pending.clear()
            book.completed.clear()
            book.completed_ids.clear()
            book.completed_order.clear()
            book.observations.clear()
            book._observation_ids.clear()
        self.processor.completed_simulations.clear()

    def process(self, record: Any, *, bootstrap: bool = False):
        result = super().process(record, bootstrap=bootstrap)
        self._clear_virtual_book()
        return result

    def advance(self, watermark: datetime, *, complete: bool = False):
        result = super().advance(watermark, complete=complete)
        self._clear_virtual_book()
        return result

    def checkpoint(self) -> None:
        self._clear_virtual_book()
        super().checkpoint()


class CTraderPipeline:
    """Orquestador offline/live-ready de captura normalizada y CFD paper."""

    def __init__(
        self,
        store: SQLiteStore,
        config: EffectiveConfig,
        *,
        spec: CTraderInstrumentSpec | None = None,
        cfd_config: CFDConfig | Mapping[str, Any] | None = None,
        mode: str | OperationMode | None = None,
        checkpoint_name: str = "runtime",
        pipeline_checkpoint_name: str = "pipeline",
        max_candles: int | None = 5000,
    ) -> None:
        if not isinstance(store, SQLiteStore):
            raise TypeError("store debe ser SQLiteStore")
        if not isinstance(config, EffectiveConfig):
            raise TypeError("config debe ser EffectiveConfig")
        self.store = store
        self.config = config
        self.mode = _mode(mode or config.mode)
        self.spec = spec or CTraderInstrumentSpec(
            symbol=config.instrument,
            symbol_id=(config.ctrader.get("symbol_id") if isinstance(config.ctrader, Mapping) else None),
            digits=int(config.ctrader.get("digits", 5)) if isinstance(config.ctrader, Mapping) else 5,
            pip_position=int(config.ctrader.get("pip_position", 4)) if isinstance(config.ctrader, Mapping) else 4,
            price_scale=int(config.ctrader.get("price_scale", 100_000)) if isinstance(config.ctrader, Mapping) else 100_000,
        )
        if self.spec.symbol != config.instrument.upper().replace("-", "/"):
            raise CTraderPipelineError(f"instrumento cTrader/config incompatible: {self.spec.symbol} vs {config.instrument}")
        raw_cfd = dict(config.cfd) if isinstance(config.cfd, Mapping) else {}
        if cfd_config is None:
            cfd_config = CFDConfig.from_mapping(raw_cfd or {"instrument": config.instrument})
        elif isinstance(cfd_config, Mapping):
            cfd_config = CFDConfig.from_mapping(cfd_config)
        if not isinstance(cfd_config, CFDConfig):
            raise TypeError("cfd_config debe ser CFDConfig o mapping")
        if cfd_config.instrument != self.spec.symbol:
            raise CTraderPipelineError(f"CFD/config incompatible con cTrader: {cfd_config.instrument} vs {self.spec.symbol}")
        self.cfd_config = cfd_config
        self.checkpoint_name = str(checkpoint_name)
        self.pipeline_checkpoint_name = str(pipeline_checkpoint_name)
        self.max_candles = max_candles

    @property
    def analysis_basis(self) -> str:
        base = "traded" if self.config.price_base == "close" else str(self.config.price_base).lower()
        if base not in {"traded", "bid", "ask", "mid"}:
            raise CTraderPipelineError(f"base de precio cTrader no soportada: {base!r}")
        return base

    def _session_config(self) -> dict[str, Any]:
        config = self.config.to_dict()
        config["pipeline"] = {
            "provider": "ctrader-open-api",
            "product": PAPER_PRODUCT,
            "schema_version": SCHEMA_VERSION,
            "analysis_basis": self.analysis_basis,
            "network_performed": False,
            "execution_enabled": False,
        }
        return config

    def _analysis_records(self, capture: CTraderCapture) -> tuple[Event | Bar, ...]:
        basis = self.analysis_basis
        if basis == "traded":
            rows: tuple[Event | Bar, ...] = tuple(capture.bars)
        else:
            rows = tuple(event for event in capture.quote_events if event.price_basis == basis)
        if not rows:
            raise CTraderPipelineError(f"la captura no contiene registros para la base de análisis {basis}")
        return tuple(sorted(rows, key=_ordered_record_key))

    def _persist_paper_trade(self, session_id: str, trade: Any, *, paper_analysis_id: str, capture: CTraderCapture) -> None:
        raw_trade = trade.to_dict()
        detected = trade.detected_at
        expiry = trade.close_target_at or (trade.entry_target_at + timedelta(seconds=float(trade.horizon_seconds)))
        assumptions = {
            "schema_version": SCHEMA_VERSION,
            "product": PAPER_PRODUCT,
            "paper_only": True,
            "provider": "ctrader-open-api",
            "capture_hash": capture.capture_hash,
            "analysis_basis": self.analysis_basis,
            "units": raw_trade["units"],
            "fill_policy": raw_trade["fill_policy"],
            "close_policy": raw_trade["close_policy"],
            "pip_size": raw_trade["pip_size"],
            "price_precision": raw_trade["price_precision"],
            "commission_quote": raw_trade["commission_quote"],
            "slippage_quote": raw_trade["slippage_quote"],
            "financing_quote": raw_trade["financing_quote"],
            "lineage": raw_trade["lineage"],
            "provenance": dict(capture.provenance),
        }
        row = {
            "product": PAPER_PRODUCT,
            "simulation_id": f"paper:{trade.trade_id}",
            "signal_id": trade.signal_id,
            "simulation_type": "CFD_PAPER",
            "horizon_seconds": float(trade.horizon_seconds),
            "direction": trade.direction.value,
            "detected_ts": detected,
            "entry_ts": trade.entry_available_at,
            "expiry_ts": expiry,
            "entry_price": trade.entry_price,
            "final_price": trade.close_price,
            "outcome": _paper_outcome(trade),
            # The v3 column is named stake; this product is linear and stores
            # units explicitly in assumptions rather than pretending risk.
            "stake": float(trade.units),
            "net_result": trade.net_pnl,
            "price_base": "bid_ask",
            "quality": trade.quality,
            "resolution": "spot",
            "assumptions": assumptions,
            "payload": {
                "schema_version": SCHEMA_VERSION,
                "product": PAPER_PRODUCT,
                "trade": raw_trade,
                "capture": capture.to_dict(),
            },
        }
        self.store.update_simulation(
            session_id,
            row,
            analysis_id=paper_analysis_id,
            variant=PAPER_VARIANT,
            analysis_config_hash=payload_hash({"runtime": self.config.config_hash, "cfd": self.cfd_config.config_hash}),
            contract_hash=self.cfd_config.config_hash,
            partition="paper",
            ignore_pending_terminal=True,
        )

    def _save_pipeline_snapshot(
        self,
        session_id: str,
        coordinator: _CFDOnlyRuntimeCoordinator,
        capture: CTraderCapture,
        paper_analysis_id: str,
        paper: CFDReplayResult,
    ) -> tuple[dict[str, Any], str]:
        status = asdict(coordinator.status())
        processor_status = coordinator.processor.status
        processor_snapshot = coordinator.processor.checkpoint()
        # The query/UI read model already consumes state.processor; keep the
        # warm-up projection there without changing the processor checkpoint
        # contract or inventing a readiness conclusion from row counts.
        processor_snapshot["warmup_pending"] = dict(processor_status.get("warmup_pending", {}))
        snapshot: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "product": PAPER_PRODUCT,
            "capture": capture.to_dict(),
            "runtime": {
                "analysis_id": coordinator.analysis_id,
                "status": _jsonable(status),
                "analysis_basis": self.analysis_basis,
            },
            "processor": processor_snapshot,
            "paper": {
                "analysis_id": paper_analysis_id,
                "product": PAPER_PRODUCT,
                "capture_complete": paper.capture_complete,
                "config": self.cfd_config.to_dict(),
                "trades": [trade.to_dict() for trade in paper.trades],
                "events": [_jsonable(event) for event in paper.events],
            },
            # QueryService consumes this compact projection without inferring
            # health from row counts; the complete state remains above.
            "status": _jsonable(status),
        }
        # Analysis/session ids are intentionally excluded from the parity
        # digest: replaying the same capture in another SQLite session must
        # yield the same market/product state while retaining distinct DB
        # lineage in the full snapshot.
        parity_material = {
            "schema_version": snapshot["schema_version"],
            "product": snapshot["product"],
            "capture": snapshot["capture"],
            "analysis_basis": snapshot["runtime"]["analysis_basis"],
            "processor": snapshot["processor"],
            "paper": {
                key: value for key, value in snapshot["paper"].items() if key != "analysis_id"
            },
        }
        digest = payload_hash(parity_material)
        snapshot["snapshot_hash"] = digest
        self.store.save_checkpoint(
            session_id,
            self.pipeline_checkpoint_name,
            analysis_id=paper_analysis_id,
            cursor={"analysis_id": paper_analysis_id, "runtime_analysis_id": coordinator.analysis_id, "capture_hash": capture.capture_hash},
            events_processed=coordinator.processor.events_processed + coordinator.processor.candles_processed,
            last_event_id=coordinator.processor.last_event_id,
            state=snapshot,
        )
        return snapshot, digest

    def run(
        self,
        capture: CTraderCapture | Iterable[Mapping[str, Any] | WireMessage],
        *,
        session_id: str | None = None,
        capture_complete: bool = True,
        received_at: datetime | Callable[[int, Mapping[str, Any]], datetime] | None = None,
        quote_basis: str = "mid",
        finish_session: bool = True,
    ) -> CTraderPipelineResult:
        if not isinstance(capture, CTraderCapture):
            capture = normalize_ctrader_capture(
                capture,
                spec=self.spec,
                quote_basis=quote_basis,
                mode=self.mode,
                received_at=received_at,
            )
        if capture.provenance.get("instrument") != self.spec.symbol:
            raise CTraderPipelineError("capture y spec cTrader no corresponden al mismo instrumento")
        analysis_records = self._analysis_records(capture)
        sid = self.store.create_session(
            session_id=session_id,
            mode=self.mode,
            provider="ctrader-open-api",
            instrument=self.spec.symbol,
            config=self._session_config(),
            code_version=self.config.version,
            dataset_ref=capture.capture_id,
            metadata={"schema_version": SCHEMA_VERSION, "product": PAPER_PRODUCT, "provenance": dict(capture.provenance), "capture": capture.to_dict()},
        )
        self.store.save_config(sid, "effective", self._session_config())
        self.store.save_config(sid, "cfd-paper", self.cfd_config.to_dict())
        coordinator = _CFDOnlyRuntimeCoordinator(
            self.store,
            sid,
            self.config,
            mode=self.mode,
            dataset_hash=capture.capture_hash,
            variant="trend_pullback_v1",
            partition="paper",
            checkpoint_name=self.checkpoint_name,
            source="ctrader-open-api",
            max_candles=self.max_candles,
            resume=True,
            identity_extra={"provider": "ctrader-open-api", "product": PAPER_PRODUCT, "analysis_basis": self.analysis_basis},
        )
        # Feed only the explicitly selected price basis to the detector.
        # Native bars/quotes of the other representation are captured after
        # processing so their durable payload is identical on a resumed run
        # (indicator provenance is already available in both runs).
        runtime_result = coordinator.replay(analysis_records, sort=True, bootstrap=False, complete=capture_complete)
        if self.analysis_basis == "traded":
            for event in capture.quote_events:
                coordinator.capture_only(event)
        else:
            for bar in capture.bars:
                coordinator.capture_only(bar)
        signals = tuple(coordinator.processor.signals)
        cfd_signals = tuple(signal_to_cfd_signal(signal, capture_hash=capture.capture_hash) for signal in signals)
        quotes = tuple(spot_event_to_cfd_quote(event, capture_hash=capture.capture_hash) for event in capture.quote_events)
        paper = CFDSimulator(self.cfd_config).replay(cfd_signals, quotes, capture_complete=capture_complete)
        paper_config_hash = payload_hash({"runtime": self.config.config_hash, "cfd": self.cfd_config.config_hash})
        paper_analysis_id = self.store.create_analysis(
            sid,
            dataset_hash=capture.capture_hash,
            config_hash=paper_config_hash,
            variant=PAPER_VARIANT,
            contract_hash=self.cfd_config.config_hash,
            partition="paper",
            code_version=self.config.version,
            metadata={"product": PAPER_PRODUCT, "runtime_analysis_id": coordinator.analysis_id, "schema_version": SCHEMA_VERSION},
            identity_extra={"runtime_analysis_id": coordinator.analysis_id},
        )
        for trade in paper.trades:
            self._persist_paper_trade(sid, trade, paper_analysis_id=paper_analysis_id, capture=capture)
        if finish_session and capture_complete:
            coordinator.finish(status="COMPLETED")
        else:
            coordinator.checkpoint()
        snapshot, snapshot_hash = self._save_pipeline_snapshot(sid, coordinator, capture, paper_analysis_id, paper)
        status = _jsonable(asdict(coordinator.status()))
        return CTraderPipelineResult(
            capture=capture,
            session_id=sid,
            runtime_analysis_id=coordinator.analysis_id,
            paper_analysis_id=paper_analysis_id,
            analysis_basis=self.analysis_basis,
            analysis_records=analysis_records,
            runtime_result=runtime_result,
            runtime_status=status,
            signals=signals,
            cfd_signals=cfd_signals,
            paper=paper,
            snapshot=snapshot,
            snapshot_hash=snapshot_hash,
        )


def _fixture_close_series(count: int) -> list[float]:
    if count < 130:
        raise CTraderPipelineError("el fixture cTrader requiere al menos 130 barras M1")
    closes: list[float] = []
    for index in range(100):
        closes.append(1.1000 + index * 0.0001)
    for _ in range(15):
        closes.append(closes[-1] + 0.0002)
    # Con indicadores pequeños este tramo forma contexto alcista, una
    # preparación M5 de retroceso y un cruce M1 auténtico en 02:02 UTC.
    closes.extend([1.1125, 1.1118, 1.1110, 1.1106, 1.1105, 1.1104, 1.1120, 1.1125, 1.1128, 1.1130])
    while len(closes) < count:
        closes.append(closes[-1] + 0.00015)
    return closes


def synthetic_ctrader_payloads(
    *,
    start: datetime = datetime(2026, 1, 1, tzinfo=UTC),
    symbol_id: int = 99,
    count: int = 190,
) -> tuple[Mapping[str, Any], ...]:
    """Fixture offline de SpotEvent + trendbar; no es mercado real."""

    start = _utc(start, name="start")
    spec = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=symbol_id)
    closes = _fixture_close_series(count)
    payloads: list[Mapping[str, Any]] = []
    scale = spec.price_scale
    for index, close in enumerate(closes):
        bar_start = start + timedelta(minutes=index)
        bar_end = bar_start + timedelta(minutes=1)
        previous = closes[index - 1] if index else close
        open_price = previous
        low_price = min(open_price, close) - 0.0001
        high_price = max(open_price, close) + 0.0001
        low_relative = int(round(low_price * scale))
        raw_bar = synthetic_trendbar(
            timestamp_minutes=int(bar_start.timestamp() // 60),
            period="M1",
            low_relative=low_relative,
            delta_open=int(round(open_price * scale)) - low_relative,
            delta_close=int(round(close * scale)) - low_relative,
            delta_high=int(round(high_price * scale)) - low_relative,
            volume=42,
        )
        bid_relative = int(round((close - 0.0001) * scale))
        ask_relative = int(round((close + 0.0001) * scale))
        payload = dict(
            synthetic_spot_event(
                timestamp_ms=int(bar_end.timestamp() * 1000),
                symbol_id=symbol_id,
                bid_relative=bid_relative,
                ask_relative=ask_relative,
                trendbars=(raw_bar,),
                snapshot=index == 0,
            )
        )
        # Stable source sequence survives reversed replay and makes identity
        # independent of local arrival ordinal.
        payload["sequence"] = index
        payloads.append(payload)
    return tuple(payloads)


def synthetic_ctrader_capture(
    *,
    start: datetime = datetime(2026, 1, 1, tzinfo=UTC),
    symbol_id: int = 99,
    count: int = 190,
    mode: str | OperationMode = "REPLAY",
) -> CTraderCapture:
    """Construye la captura normalizada usada por pruebas offline."""

    payloads = synthetic_ctrader_payloads(start=start, symbol_id=symbol_id, count=count)
    def receipt(index: int, raw: Mapping[str, Any]) -> datetime:
        source_time = _payload_received_at(raw)
        assert source_time is not None
        return source_time + timedelta(seconds=1)
    return normalize_ctrader_capture(
        payloads,
        spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=symbol_id),
        quote_basis="mid",
        mode=mode,
        received_at=receipt,
    )


__all__ = [
    "CTraderCapture",
    "CTraderPipeline",
    "CTraderPipelineError",
    "CTraderPipelineResult",
    "PAPER_PRODUCT",
    "PAPER_VARIANT",
    "normalize_ctrader_capture",
    "signal_to_cfd_signal",
    "spot_event_to_cfd_quote",
    "synthetic_ctrader_capture",
    "synthetic_ctrader_payloads",
]
