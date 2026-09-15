"""Composition root for causal cTrader capture -> detector -> local CFD PAPER.

Replay feeds the same resumable application session as incremental ingestion.
No binary book, OAuth, socket or external executor is constructed here.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from itertools import islice
from typing import Any

from ..configuration import EffectiveConfig
from ..core.canonical import canonical_value, fingerprint, instant_text
from ..core.strategy import Signal
from ..data.capture import CaptureEnvelope, CaptureIndex, CaptureOrder, MessageClass, ordering_key, parse_instant
from ..data.ctrader import CTraderInstrumentSpec, CTraderNormalizationResult
from ..data.models import Bar, Event
from ..data.paper_fixture import synthetic_ctrader_payloads
from ..runtime import ProcessResult, ReplayResult, RuntimeCoordinator
from ..runtime.consumers import CFDSignalConsumer
from ..runtime.state import signal_from_dict
from .cfd_simulation import CFDConfig, CFDReplayResult, CFDSignal, CFDSimulationError, CFDSimulator, CFDTrade
from .ctrader_capture import (
    NORMALIZATION_VERSION,
    CaptureCoverage,
    CausalNormalizer,
    CTraderCapture,
    CTraderPipelineError,
    capture_envelopes,
    capture_identity,
    normalize_ctrader_capture,
    synthetic_ctrader_capture,
)
from .ctrader_paper_adapters import signal_to_cfd_signal, spot_event_to_cfd_quote
from .persistence import SQLiteStore

PAPER_PRODUCT = "FOREX_CFD_LOCAL_PAPER"
PAPER_VARIANT = "ctrader_cfd_paper"
PAPER_SESSION_VERSION = 1
PAPER_REPORT_VERSION = 1
_RETENTION = 256


def _mode(value: Any) -> str:
    mode = value.value if hasattr(value, "value") else str(value)
    if mode.upper() in {"REPLAY", "SYNTHETIC"}:
        return "REPLAY"
    if mode.upper() in {"LIVE", "OBSERVACIÓN EN DIRECTO"}:
        return "LIVE"
    raise CTraderPipelineError(f"unsupported cTrader observation mode: {mode}")


def _snapshot_cursor(value: object) -> tuple[str, int] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise CTraderPipelineError("snapshot cursor requires [available_at, global_sequence]")
    when, sequence = value
    if not isinstance(when, str) or isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        raise CTraderPipelineError("invalid snapshot cursor types")
    parse_instant(when)
    return when, sequence


def _coverage_from_dict(raw: Mapping[str, Any]) -> CaptureCoverage:
    return CaptureCoverage(
        requested_start=parse_instant(raw.get("requested_start")),
        requested_end=parse_instant(raw.get("requested_end")),
        observed_start=parse_instant(raw.get("observed_start")),
        observed_end=parse_instant(raw.get("observed_end")),
        dataset_end_declared=raw.get("dataset_end_declared", False),
        continuity=raw.get("continuity", "UNKNOWN"),
        availability_known=raw.get("availability_known", False),
    )


def _bounded_selection_marker(envelope: CaptureEnvelope) -> bool | None:
    """Read the optional marker emitted by bounded history export."""

    payload = envelope.payload
    if payload.get("bounded_selection_verified") is True:
        return True
    if payload.get("bounded_selection_verified") is False:
        return False
    marker = payload.get("bounded_selection")
    if isinstance(marker, Mapping):
        return True
    provenance = payload.get("capture_provenance")
    if isinstance(provenance, Mapping) and isinstance(provenance.get("bounded_selection_verified"), bool):
        return bool(provenance["bounded_selection_verified"])
    return None


def _bounded_selection_verified_in_stream(envelopes: Iterable[CaptureEnvelope]) -> bool:
    """Verify the additive bounded marker across a capture stream.

    Trendbar pages are required to be marked.  An absent END marker is allowed
    for the first V22 export; an explicit END contradiction is not.
    """

    pages: list[CaptureEnvelope] = []
    ends: list[CaptureEnvelope] = []
    for envelope in envelopes:
        if envelope.message_class is MessageClass.TRENDBAR:
            pages.append(envelope)
        elif envelope.message_class is MessageClass.END:
            ends.append(envelope)
    return (
        bool(pages)
        and all(_bounded_selection_marker(item) is True for item in pages)
        and not any(_bounded_selection_marker(item) is False for item in ends)
    )


def _bounded_end_is_complete(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("complete") is True
        and payload.get("history_complete") is True
        and payload.get("has_more") is False
        and not payload.get("issues")
    )


@dataclass(frozen=True, slots=True)
class CTraderPipelineResult:
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
    def trades(self) -> tuple[CFDTrade, ...]:
        return self.paper.trades

    def to_dict(self, *, include_payloads: bool = False) -> dict[str, Any]:
        if include_payloads and not self.capture.envelopes:
            raise CTraderPipelineError(
                "full capture export requires the durable archive, not the compact result window"
            )
        return {
            "schema_version": PAPER_REPORT_VERSION,
            "product": PAPER_PRODUCT,
            "session_id": self.session_id,
            "runtime_analysis_id": self.runtime_analysis_id,
            "paper_analysis_id": self.paper_analysis_id,
            "analysis_basis": self.analysis_basis,
            "capture": self.capture.to_dict(include_payloads=include_payloads),
            "runtime_result": self.runtime_result.to_dict(),
            "runtime_status": canonical_value(self.runtime_status),
            "signals": [item.as_dict() for item in self.signals],
            "cfd_signals": [item.to_dict() for item in self.cfd_signals],
            "paper": self.paper.to_dict(),
            "snapshot_hash": self.snapshot_hash,
            "retention": {"result_window": _RETENTION, "archive": "SQLite capture_envelopes/cfd_trades"},
        }


class CTraderPipeline:
    """Validate configuration once and connect exactly one application session."""

    def __init__(
        self,
        store: SQLiteStore,
        config: EffectiveConfig,
        *,
        spec: CTraderInstrumentSpec | None = None,
        cfd_config: CFDConfig | Mapping[str, Any] | None = None,
        mode: Any = None,
        checkpoint_name: str = "runtime",
        pipeline_checkpoint_name: str = "pipeline",
        max_candles: int | None = 256,
    ) -> None:
        self.store = store
        self.config = config
        self.mode = _mode(mode or config.mode)
        self.spec = spec or CTraderInstrumentSpec(
            symbol=config.instrument,
            symbol_id=config.ctrader.get("symbol_id"),
            digits=int(config.ctrader.get("digits", 5)),
            pip_position=int(config.ctrader.get("pip_position", 4)),
            price_scale=int(config.ctrader.get("price_scale", 100_000)),
        )
        supplied = cfd_config if cfd_config is not None else config.cfd
        self.cfd_config = supplied if isinstance(supplied, CFDConfig) else CFDConfig.from_mapping(supplied)
        self.checkpoint_name = checkpoint_name
        self.pipeline_checkpoint_name = pipeline_checkpoint_name
        self.max_candles = max_candles if max_candles is not None else 256
        self._validate()

    def _validate(self) -> None:
        minimum = max(
            self.config.indicators.ema_slow,
            self.config.indicators.rsi_period + 1,
            self.config.indicators.atr_period + 1,
            self.config.strategy.context_lookback + 1,
            self.config.strategy.preparation_lookback + 1,
        )
        if isinstance(self.max_candles, bool) or self.max_candles < minimum:
            raise CTraderPipelineError(f"bounded history must retain at least {minimum} candles per timeframe")
        if self.spec.symbol != self.config.instrument.upper().replace("-", "/"):
            raise CTraderPipelineError("cTrader/config instrument mismatch")
        if self.cfd_config.instrument != self.spec.symbol:
            raise CTraderPipelineError("CFD/config instrument mismatch")
        if self.config.price_base not in {"native", "bid", "ask", "mid"}:
            raise CTraderPipelineError(
                "cTrader trendbars require price_base='native'; trade semantics are not established"
            )

    @property
    def analysis_basis(self) -> str:
        return self.config.price_base

    def session_config(self) -> dict[str, Any]:
        config = self.config.to_dict()
        config["pipeline"] = {
            "provider": "ctrader-open-api",
            "product": PAPER_PRODUCT,
            "session_version": PAPER_SESSION_VERSION,
            "normalization_version": NORMALIZATION_VERSION,
            "analysis_basis": self.analysis_basis,
            "history_retention": self.max_candles,
            "network_performed": False,
            "execution_enabled": False,
        }
        return config

    def open_session(
        self,
        *,
        dataset_id: str,
        session_id: str | None = None,
        coverage: CaptureCoverage | None = None,
        provenance: Mapping[str, Any] | None = None,
        resume: bool = True,
        order: CaptureOrder = "as_observed",
    ) -> CTraderPaperSession:
        return CTraderPaperSession(
            self,
            dataset_id=dataset_id,
            session_id=session_id,
            coverage=coverage,
            provenance=provenance,
            resume=resume,
            order=order,
        )

    def run(
        self,
        capture: CTraderCapture | Iterable[Any],
        *,
        session_id: str | None = None,
        capture_complete: bool = True,
        received_at: datetime | Callable[[int, Mapping[str, Any]], datetime] | None = None,
        quote_basis: str = "mid",
        finish_session: bool = True,
        chunk_size: int = 128,
        order: CaptureOrder = "as_observed",
    ) -> CTraderPipelineResult:
        if order not in {"as_observed", "market_time_corrected"}:
            raise CTraderPipelineError(f"unsupported replay order: {order}")
        if order == "market_time_corrected" and self.analysis_basis != "native":
            raise CTraderPipelineError(
                "market-time corrected historical replay requires price_base='native'; "
                "bid/ask PAPER cannot be reconstructed from trendbars"
            )
        if isinstance(capture, CTraderCapture):
            return self._run_capture(capture, session_id, capture_complete, finish_session, chunk_size, order)
        with CaptureIndex(capture_envelopes(capture, received_at=received_at), mode=order) as index:
            identity = capture_identity(index.capture_hash, self.spec, quote_basis)
            last = index.last_envelope
            coverage = _coverage_from_dict(last.payload) if last and last.message_class is MessageClass.END else None
            bounded_selection_verified = _bounded_selection_verified_in_stream(index)
            if (
                coverage is not None
                and coverage.continuity == "UNKNOWN"
                and bounded_selection_verified
                and last is not None
                and _bounded_end_is_complete(last.payload)
            ):
                # The original bounded V22 END lacked the additive marker and
                # repeated UNKNOWN continuity.  Its pages still carry the
                # selector's gap-checked marker, so recover the derived
                # continuity before replay rather than mutating the capture.
                coverage = replace(coverage, continuity="CONTINUOUS")
            marker = bounded_selection_verified if last is not None and last.message_class is MessageClass.END else None
            provenance = {
                "provider": "ctrader-open-api",
                "instrument": self.spec.symbol,
                "mode": self.mode,
                "order": order,
                **({"bounded_selection_verified": marker} if marker is not None else {}),
            }
            session = self.open_session(
                dataset_id=identity,
                session_id=session_id,
                coverage=coverage,
                provenance=provenance,
                order=order,
            )
            session.ingest_many(index.iter_after(session.cursor), chunk_size=chunk_size)
            return session.finish(capture_complete=capture_complete, finish_session=finish_session)

    def _run_capture(
        self,
        capture: CTraderCapture,
        session_id: str | None,
        complete: bool,
        finish: bool,
        chunk_size: int,
        order: CaptureOrder,
    ) -> CTraderPipelineResult:
        capture_order = capture.provenance.get("order", "as_observed")
        if capture_order != order:
            raise CTraderPipelineError("capture provenance order does not match the requested replay order")
        if capture.provenance.get("instrument") != self.spec.symbol:
            raise CTraderPipelineError("capture/spec instrument mismatch")
        if not capture.envelopes:
            raise CTraderPipelineError("replay requires versioned envelopes, not an unproven normalized record cache")
        session = self.open_session(
            dataset_id=capture.capture_hash,
            session_id=session_id,
            coverage=capture.coverage,
            provenance=capture.provenance,
            order=order,
        )
        with CaptureIndex(capture.envelopes, mode=order) as index:
            session.ingest_many(index.iter_after(session.cursor), chunk_size=chunk_size)
        result = session.finish(capture_complete=complete, finish_session=finish)
        return replace(result, capture=replace(result.capture, payloads=capture.payloads, envelopes=capture.envelopes))


class CTraderPaperSession:
    """Ingest/advance/snapshot/restore/finish with one consistent durable cursor.

    A batch commits capture facts, detector/product changes and both checkpoints
    together. On failure, in-memory state is restored to the previous commit.
    Windows in returned reports are bounded; full immutable inputs and product
    history belong to SQLite, not to an ever-growing session list.
    """

    def __init__(
        self,
        pipeline: CTraderPipeline,
        *,
        dataset_id: str,
        session_id: str | None,
        coverage: CaptureCoverage | None,
        provenance: Mapping[str, Any] | None,
        resume: bool,
        order: CaptureOrder,
    ) -> None:
        self.pipeline = pipeline
        self.store = pipeline.store
        self.dataset_id = dataset_id
        self.stream_identity_hash = fingerprint({"stream_ref": dataset_id, "capture_contract": 1})
        self.order: CaptureOrder = order
        self.input_prefix_hash = fingerprint({"capture_version": 1, "order": order})
        self.coverage = replace(
            coverage or CaptureCoverage(), observed_start=None, observed_end=None, availability_known=False
        )
        self.provenance = dict(
            provenance
            or {
                "provider": "ctrader-open-api",
                "instrument": pipeline.spec.symbol,
                "mode": pipeline.mode,
                "order": order,
            }
        )
        declared_bounded = self.provenance.get("bounded_selection_verified")
        self._bounded_selection_verified: bool | None = declared_bounded if isinstance(declared_bounded, bool) else None
        self.cursor: tuple[str, int] | None = None
        self.max_ingest_sequence = -1
        self.logical_time = datetime(1970, 1, 1, tzinfo=UTC)
        self.finished = False
        self.end_seen = False
        self.failed = False
        self._stats = dict.fromkeys(
            (
                "envelopes",
                "accepted",
                "duplicate",
                "rejected",
                "candles",
                "signals",
                "evaluations",
                "quotes",
                "bars",
                "snapshots",
                "quote_rejections",
            ),
            0,
        )
        self._issues: deque[str] = deque(maxlen=64)
        self._analysis_records: deque[Event | Bar] = deque(maxlen=_RETENTION)
        self._signals: deque[Signal] = deque(maxlen=_RETENTION)
        self._cfd_signals: deque[CFDSignal] = deque(maxlen=_RETENTION)
        self.session_id = self.store.create_session(
            session_id=session_id,
            mode=pipeline.mode,
            provider="ctrader-open-api",
            instrument=pipeline.spec.symbol,
            config=pipeline.session_config(),
            code_version=pipeline.config.version,
            dataset_ref=dataset_id,
            metadata={"product": PAPER_PRODUCT, "provenance": self.provenance},
        )
        self.normalizer = CausalNormalizer(
            pipeline.spec,
            quote_basis=(pipeline.analysis_basis if pipeline.analysis_basis != "native" else "mid"),
            mode=pipeline.mode,
            max_quote_age_seconds=pipeline.config.quality.max_feed_age_seconds,
        )
        self.coordinator = RuntimeCoordinator(
            self.store,
            self.session_id,
            pipeline.config,
            mode=pipeline.mode,
            dataset_hash=self.stream_identity_hash,
            variant="trend_pullback_v1",
            partition="paper",
            checkpoint_name=pipeline.checkpoint_name,
            source="ctrader-open-api",
            max_candles=pipeline.max_candles,
            resume=False,
            checkpoint_every=2**63 - 1,
            clock=lambda: self.logical_time,
            identity_extra={
                "product": PAPER_PRODUCT,
                "session_version": PAPER_SESSION_VERSION,
                "history_retention": pipeline.max_candles,
                "dataset_hash_kind": "logical_stream_identity",
            },
            signal_consumer=CFDSignalConsumer(max_events=_RETENTION, signal_sink=self._on_detector_signal),
        )
        self.paper_analysis_id = self.store.create_analysis(
            self.session_id,
            dataset_hash=self.stream_identity_hash,
            config_hash=fingerprint(pipeline.session_config()),
            variant=PAPER_VARIANT,
            contract_hash=pipeline.cfd_config.config_hash,
            partition="paper",
            code_version=pipeline.config.version,
            identity_extra={
                "runtime_analysis_id": self.coordinator.analysis_id,
                "session_version": PAPER_SESSION_VERSION,
            },
        )
        self.simulator = CFDSimulator(pipeline.cfd_config, terminal_lookup=self._terminal_trade)
        if resume:
            self._resume()

    def _resume(self) -> None:
        saved = self.store.get_checkpoint(
            self.session_id,
            self.pipeline.pipeline_checkpoint_name,
            analysis_id=self.paper_analysis_id,
            allow_alternate=False,
        )
        if saved:
            self.restore(saved["state"])

    def ingest(self, envelope: CaptureEnvelope) -> None:
        self._apply_batch((envelope,))

    def ingest_many(self, envelopes: Iterable[CaptureEnvelope], *, chunk_size: int = 128) -> None:
        if isinstance(chunk_size, bool) or chunk_size <= 0:
            raise CTraderPipelineError("chunk_size must be a positive integer")
        iterator = iter(envelopes)
        while batch := tuple(islice(iterator, chunk_size)):
            self._apply_batch(batch)

    def _apply_batch(self, batch: Iterable[CaptureEnvelope]) -> None:
        if self.failed:
            raise CTraderPipelineError("failed session must be restored before ingest")
        before = self.snapshot()
        try:
            with self.store.atomic_batch():
                for envelope in batch:
                    self._ingest(envelope)
                self.checkpoint()
        except BaseException:
            self._load_state(before)
            raise

    def _ingest(self, envelope: CaptureEnvelope) -> None:
        key = ordering_key(envelope, self.order)
        inserted = self.store.save_capture_envelope(self.session_id, envelope.to_dict())
        if self.cursor is not None and key <= self.cursor:
            if inserted:
                raise CTraderPipelineError(
                    "session requires causal order; use explicit CaptureIndex for unordered files"
                )
            return
        if self.finished or self.end_seen:
            raise CTraderPipelineError("dataset already finished; use another dataset/session for new input")
        if envelope.available_at is None:
            raise CTraderPipelineError("missing envelope availability")
        self.logical_time = envelope.available_at
        self.input_prefix_hash = fingerprint({"previous": self.input_prefix_hash, "envelope": envelope.to_dict()})
        self.max_ingest_sequence = max(self.max_ingest_sequence, envelope.ingest_sequence)
        self._stats["envelopes"] += 1
        self._observe_coverage(envelope)
        self._generation_gate(envelope)
        normalized = self.normalizer.normalize(envelope)
        self._control(envelope)
        self._consume_records(normalized)
        self.cursor = key

    def reader_resume_arguments(self) -> dict[str, int]:
        """Seed a newly constructed reader from the committed capture cursor."""
        return {
            "next_ingest_sequence": self.max_ingest_sequence + 1,
            "initial_generation": self.normalizer.generation or 0,
        }

    def _generation_gate(self, envelope: CaptureEnvelope) -> None:
        previous = self.normalizer.generation
        if previous is None or previous == envelope.connection_generation:
            return
        self.simulator.disconnect(reason="CONNECTION_GENERATION_CHANGED")
        self.coverage = replace(self.coverage, continuity="DISCONTINUOUS")
        self.coordinator.update_feed_state(
            continuity="UNKNOWN", reconciliation="PENDING", blocked_reasons=("reconciliation_required",)
        )

    def _observe_coverage(self, envelope: CaptureEnvelope) -> None:  # noqa: C901 - bounded marker gate
        if envelope.message_class is MessageClass.TRENDBAR:
            marker = _bounded_selection_marker(envelope)
            # Every bounded trendbar page must carry the selector marker.  An
            # unmarked page makes the whole stream unverified; mixing marked
            # and unmarked pages is rejected rather than changing axes midway.
            marked = marker is True
            if self._bounded_selection_verified is None:
                self._bounded_selection_verified = marked
                self.provenance["bounded_selection_verified"] = marked
            elif self._bounded_selection_verified != marked:
                raise CTraderPipelineError("bounded historical coverage marker is inconsistent")
        elif envelope.message_class is MessageClass.END:
            marker = _bounded_selection_marker(envelope)
            # The END marker was added after the first bounded V22 capture;
            # absence is tolerated there because all trendbar pages still
            # carry the selector marker.  An explicit contradiction is not.
            if marker is not None:
                if self._bounded_selection_verified is None:
                    self._bounded_selection_verified = marker
                    self.provenance["bounded_selection_verified"] = marker
                elif self._bounded_selection_verified != marker:
                    raise CTraderPipelineError("bounded historical coverage marker is inconsistent")
        # ``available_at`` is the causal watermark for normal observation.  A
        # market-time-corrected historical replay has a separate reconstructed
        # watermark and must express coverage in its market-time axis instead.
        instant = (
            envelope.event_time
            if self.order == "market_time_corrected" and self._bounded_selection_verified
            else envelope.available_at
        )
        if instant is None:
            raise CTraderPipelineError("coverage observation lacks its replay time")
        first = self.coverage.observed_start is None
        self.coverage = replace(
            self.coverage,
            observed_start=self.coverage.observed_start or instant,
            observed_end=instant,
            availability_known=(envelope.received_at is not None)
            if first
            else (self.coverage.availability_known and envelope.received_at is not None),
        )
        if envelope.message_class is MessageClass.END:
            self.end_seen = True
            continuity = envelope.payload.get("continuity", self.coverage.continuity)
            # Backward-compatible read of the first bounded V22 export: its
            # pages prove selector-based gap checking, but its END envelope
            # predates the explicit bounded/continuity fields.  Do not infer
            # this for a newer export that explicitly says UNKNOWN.
            if (
                continuity == "UNKNOWN"
                and self._bounded_selection_verified is True
                and "bounded_selection_verified" not in envelope.payload
                and envelope.payload.get("complete") is True
                and envelope.payload.get("history_complete") is True
                and envelope.payload.get("has_more") is False
                and not envelope.payload.get("issues")
            ):
                continuity = "CONTINUOUS"
            if continuity not in {"UNKNOWN", "CONTINUOUS", "DISCONTINUOUS"}:
                raise CTraderPipelineError("unknown end-of-dataset continuity state")
            self.coverage = replace(
                self.coverage,
                dataset_end_declared=True,
                continuity=continuity,
                requested_start=parse_instant(envelope.payload.get("requested_start")) or self.coverage.requested_start,
                requested_end=parse_instant(envelope.payload.get("requested_end")) or self.coverage.requested_end,
            )

    def _control(self, envelope: CaptureEnvelope) -> None:
        if envelope.message_class is MessageClass.CLOCK:
            result = (
                self.coordinator.heartbeat(self.logical_time)
                if envelope.payload.get("clock_kind") == "heartbeat"
                else self.coordinator.tick(self.logical_time)
            )
            self._detector_result(result)
            self._persist_trades(self.simulator.advance(self.logical_time, capture_complete=False))
        elif envelope.message_class is MessageClass.CONNECTION:
            self._connection(envelope)

    def _connection(self, envelope: CaptureEnvelope) -> None:
        state = envelope.payload.get("state")
        if state == "DISCONNECTED":
            self.simulator.disconnect()
            self.coverage = replace(self.coverage, continuity="DISCONTINUOUS")
            self.coordinator.update_feed_state(
                connection="DISCONNECTED", continuity="GAP", blocked_reasons=("capture_disconnected",)
            )
        elif state in {"CONNECTED", "RECONNECTED"}:
            self.simulator.disconnect(reason="RECONCILIATION_REQUIRED")
            self.coordinator.update_feed_state(
                connection="CONNECTED",
                reconciliation="PENDING",
                continuity="UNKNOWN",
                blocked_reasons=("reconciliation_required",),
            )
        elif state == "RECONCILED":
            self.simulator.reconnect(envelope.connection_generation)
            self.coverage = replace(self.coverage, continuity="CONTINUOUS")
            self.coordinator.update_feed_state(
                connection="CONNECTED", reconciliation="RECONCILED", continuity="CONTINUOUS", blocked_reasons=()
            )
        else:
            raise CTraderPipelineError(f"unknown recorded connection state {state!r}")

    def _consume_records(self, normalized: CTraderNormalizationResult) -> None:
        self._stats["quotes"] += len(normalized.quote_events)
        self._stats["bars"] += len(normalized.bars)
        self._stats["snapshots"] += int(normalized.snapshot)
        self._issues.extend(normalized.issues)
        self._refresh_price_freshness(normalized)
        records = normalized.bars if self.pipeline.analysis_basis == "native" else normalized.quote_events
        for record in records:
            if record.price_basis != self.pipeline.analysis_basis:
                raise CTraderPipelineError("mixed analysis price bases are not allowed")
            self._analysis_records.append(record)
            self._detector_result(self.coordinator.process(record))
        evidence = normalized.quote_events if self.pipeline.analysis_basis == "native" else normalized.bars
        for record in evidence:
            self.coordinator.capture_only(record)
        for event in normalized.quote_events:
            self._deliver_quote(event)
        self._persist_trades(self.simulator.advance(self.logical_time, capture_complete=False))

    def _refresh_price_freshness(self, normalized: CTraderNormalizationResult) -> None:
        if self.pipeline.mode != "LIVE" or normalized.quote_quality is None:
            return
        usable = normalized.quote_quality.usable
        blocked = self.coordinator.external_blocked_reasons
        if usable:
            blocked = [reason for reason in blocked if reason != "feed_stale"]
        self.coordinator.update_feed_state(freshness="FRESH" if usable else "BLOCKED", blocked_reasons=blocked)

    def _deliver_quote(self, event: Event) -> None:
        try:
            quote = spot_event_to_cfd_quote(event, capture_hash=self.capture_hash)
        except CFDSimulationError as exc:
            self._issues.append(f"cfd_quote_rejected:{exc.code}")
            self._stats["quote_rejections"] = self._stats.get("quote_rejections", 0) + 1
            return
        self._persist_trades(self.simulator.on_quote(quote))

    def _detector_result(self, result: ProcessResult) -> None:
        if result.accepted:
            self._stats["accepted"] += 1
        elif result.issues:
            key = (
                "duplicate"
                if all(item.code in {"duplicate_event", "duplicate_candle"} for item in result.issues)
                else "rejected"
            )
            self._stats[key] += 1
        self._stats["candles"] += len(result.candles)
        self._stats["signals"] += len(result.signals)
        self._stats["evaluations"] += len(result.evaluations)
        self._issues.extend(item.code for item in result.issues)
        self._signals.extend(result.signals)

    def _on_detector_signal(self, signal: Signal) -> None:
        """The sole CFD signal adapter, wired into the detector's consumer."""
        if self.coverage.continuity != "CONTINUOUS":
            self._issues.append("paper_continuity_not_established")
            return
        converted = signal_to_cfd_signal(signal, capture_hash=self.capture_hash)
        self._cfd_signals.append(converted)
        self._persist_trades(self.simulator.submit_all(converted))

    def _terminal_trade(self, trade_id: str) -> CFDTrade | None:
        """Resolve evicted terminal identities from the exact durable product log."""
        row = self.store.get_cfd_trade(self.session_id, self.paper_analysis_id, trade_id)
        if row is None or row["state"] not in {"CLOSED", "UNKNOWN", "REJECTED"}:
            return None
        return CFDTrade.from_mapping(row["payload"])

    def _persist_trades(self, trades: Iterable[CFDTrade]) -> None:
        for trade in trades:
            self.store.save_cfd_trade(
                self.session_id,
                self.paper_analysis_id,
                trade.to_dict(),
                variant=PAPER_VARIANT,
                partition="paper",
                analysis_config_hash=fingerprint(self.pipeline.session_config()),
                contract_hash=self.pipeline.cfd_config.config_hash,
            )

    def _advance(self, watermark: datetime) -> None:
        if watermark < self.logical_time:
            raise CTraderPipelineError("logical clock cannot go backwards")
        self.logical_time = watermark
        self._detector_result(self.coordinator.advance(watermark, complete=False))
        self._persist_trades(self.simulator.advance(watermark, capture_complete=False))

    def advance(self, watermark: datetime) -> None:
        """Record a logical clock event so its decision effects survive replay."""
        next_sequence = self.max_ingest_sequence + 1
        generation = self.normalizer.generation or 0
        self.ingest(CaptureEnvelope(watermark, watermark, watermark, next_sequence, generation, MessageClass.CLOCK, {}))

    @property
    def capture_hash(self) -> str:
        """Hash of the actually ingested causal prefix, never a caller's label."""
        return capture_identity(self.input_prefix_hash, self.pipeline.spec, self.normalizer.quote_basis)

    def capture_summary(self) -> CTraderCapture:
        return CTraderCapture(
            (),
            (),
            (),
            (),
            tuple(self._issues),
            "cap_" + self.capture_hash[:32],
            self.capture_hash,
            self.provenance,
            self._stats["snapshots"],
            coverage=self.coverage,
            record_count=self._stats["quotes"] + self._stats["bars"],
            quote_count=self._stats["quotes"],
            bar_count=self._stats["bars"],
        )

    def snapshot(self) -> dict[str, Any]:
        runtime = self.coordinator.export_state()
        # A compact query projection, not a second copy of the recovery state.
        processor = {
            key: runtime["processor"][key]
            for key in ("events_processed", "candles_processed", "last_event_time", "last_available_at")
        }
        processor.update(
            {
                "warmup_pending": dict(self.coordinator.processor.status.get("warmup_pending", {})),
                "signals": self._stats["signals"],
                "evaluations": self._stats["evaluations"],
                "errors": len(self._issues),
                "consumer_type": self.coordinator.processor.signal_consumer.consumer_type,
            }
        )
        paper = self.simulator.snapshot()
        capture = self.capture_summary().to_dict()
        state: dict[str, Any] = {
            "schema_version": PAPER_SESSION_VERSION,
            "product": PAPER_PRODUCT,
            "dataset_id": self.dataset_id,
            "order": self.order,
            "input_prefix_hash": self.input_prefix_hash,
            "max_ingest_sequence": self.max_ingest_sequence,
            "cfd_signals": [item.to_dict() for item in self._cfd_signals],
            "config_hash": fingerprint(self.pipeline.session_config()),
            "paper_analysis_id": self.paper_analysis_id,
            "capture": capture,
            "cursor": list(self.cursor) if self.cursor else None,
            "logical_time": instant_text(self.logical_time),
            "finished": self.finished,
            "end_seen": self.end_seen,
            "normalizer": self.normalizer.snapshot(),
            "runtime": runtime,
            "processor": processor,
            "paper": {
                **paper,
                "analysis_id": self.paper_analysis_id,
                "capture_complete": self.finished and self.coverage.complete,
            },
            "stats": dict(self._stats),
            "signals": [item.as_dict() for item in self._signals],
            "status": canonical_value(asdict(self.coordinator.status())),
        }
        economic = {
            key: state[key]
            for key in (
                "schema_version",
                "product",
                "dataset_id",
                "order",
                "input_prefix_hash",
                "max_ingest_sequence",
                "cfd_signals",
                "config_hash",
                "capture",
                "cursor",
                "logical_time",
                "finished",
                "end_seen",
                "normalizer",
                "processor",
                "stats",
                "signals",
            )
        }
        economic["capture"] = {key: value for key, value in capture.items() if key not in {"provenance", "synthetic"}}
        economic["processor"] = runtime["processor"]
        economic["paper"] = paper
        state["snapshot_hash"] = fingerprint(economic)
        state["integrity_hash"] = fingerprint(state)
        return state

    def checkpoint(self) -> dict[str, Any]:
        with self.store.atomic_batch():
            self.coordinator.checkpoint()
            state = self.snapshot()
            self.store.save_checkpoint(
                self.session_id,
                self.pipeline.pipeline_checkpoint_name,
                analysis_id=self.paper_analysis_id,
                cursor={
                    "analysis_id": self.paper_analysis_id,
                    "runtime_analysis_id": self.coordinator.analysis_id,
                    "capture_hash": self.dataset_id,
                    "availability_cursor": state["cursor"],
                },
                events_processed=self._stats["envelopes"],
                state=state,
            )
        return state

    def restore(self, state: Mapping[str, Any]) -> None:
        self._validate_snapshot_identity(state)
        self._validate_snapshot_progress(state)
        self._validate_snapshot_terminals(state)
        before = self.snapshot()
        try:
            self._load_state(state)
        except BaseException:
            self._load_state(before)
            raise

    def _validate_snapshot_identity(self, state: Mapping[str, Any]) -> None:
        material = dict(state)
        expected = material.pop("integrity_hash", None)
        if expected != fingerprint(material):
            raise CTraderPipelineError("PAPER checkpoint integrity mismatch")
        if state.get("schema_version") != PAPER_SESSION_VERSION or state.get("product") != PAPER_PRODUCT:
            raise CTraderPipelineError("unsupported PAPER session snapshot contract")
        if state.get("order", "as_observed") != self.order:
            raise CTraderPipelineError("snapshot replay order mismatch")
        if state.get("dataset_id") != self.dataset_id or state.get("config_hash") != fingerprint(
            self.pipeline.session_config()
        ):
            raise CTraderPipelineError("snapshot dataset/config mismatch")
        if state.get("paper_analysis_id") != self.paper_analysis_id:
            raise CTraderPipelineError("snapshot belongs to another paper analysis/session")

    def _validate_snapshot_progress(self, state: Mapping[str, Any]) -> None:
        if not isinstance(state.get("finished"), bool) or not isinstance(state.get("end_seen"), bool):
            raise CTraderPipelineError("snapshot lifecycle flags must be booleans")
        cursor = _snapshot_cursor(state.get("cursor"))
        if self.cursor is not None and (cursor is None or cursor < self.cursor):
            raise CTraderPipelineError("restore cannot rewind a committed session")

    def _validate_snapshot_terminals(self, state: Mapping[str, Any]) -> None:
        for trade in state["paper"]["trades"]:
            existing = self.store.get_cfd_trade(self.session_id, self.paper_analysis_id, trade["trade_id"])
            if (
                existing
                and existing["state"] in {"CLOSED", "UNKNOWN", "REJECTED"}
                and existing["state"] != trade["state"]
            ):
                raise CTraderPipelineError("restore cannot resurrect a durable terminal CFD trade")

    def _load_state(self, state: Mapping[str, Any]) -> None:
        self.input_prefix_hash = state["input_prefix_hash"]
        self.max_ingest_sequence = state["max_ingest_sequence"]
        self._cfd_signals = deque((CFDSignal.from_mapping(item) for item in state["cfd_signals"]), maxlen=_RETENTION)
        self.normalizer.restore(state["normalizer"])
        self.coordinator.restore_state(state["runtime"])
        paper = dict(state["paper"])
        paper.pop("analysis_id", None)
        paper.pop("capture_complete", None)
        self.simulator.restore(paper)
        self.cursor = _snapshot_cursor(state["cursor"])
        logical_time = parse_instant(state["logical_time"])
        if logical_time is None:
            raise CTraderPipelineError("checkpoint lacks logical_time")
        self.logical_time = logical_time
        self.finished = bool(state["finished"])
        self.end_seen = bool(state.get("end_seen", False))
        self._stats = dict(state["stats"])
        self.coverage = _coverage_from_dict(state["capture"]["coverage"])
        self._issues = deque(state["capture"]["issues"], maxlen=64)
        self._signals = deque((signal_from_dict(item) for item in state["signals"]), maxlen=_RETENTION)
        self._analysis_records.clear()
        self.failed = False

    def finish(self, *, capture_complete: bool = True, finish_session: bool = True) -> CTraderPipelineResult:
        if self.finished:
            return self.result()
        before = self.snapshot()
        try:
            with self.store.atomic_batch():
                self.coverage = replace(
                    self.coverage, dataset_end_declared=capture_complete or self.coverage.dataset_end_declared
                )
                self._advance(self.logical_time)
                if capture_complete and finish_session and self.coverage.complete:
                    self.simulator.finish(self.logical_time, capture_complete=True)
                    self._persist_trades(self.simulator.trades)
                    self.finished = True
                    self.coordinator.finish(status="COMPLETED")
                self.checkpoint()
        except BaseException:
            self._load_state(before)
            raise
        return self.result()

    def result(self) -> CTraderPipelineResult:
        state = self.snapshot()
        replay = ReplayResult(
            self._stats["accepted"],
            self._stats["duplicate"],
            self._stats["rejected"],
            self._stats["candles"],
            self._stats["signals"],
            self._stats["evaluations"],
            0,
            0,
        )
        paper_finished = self.finished and self.simulator.finished
        paper = CFDReplayResult(
            self.simulator.trades,
            self.simulator.events,
            capture_complete=paper_finished and self.coverage.complete,
            finished=paper_finished,
        )
        signals = tuple(self._signals)
        return CTraderPipelineResult(
            self.capture_summary(),
            self.session_id,
            self.coordinator.analysis_id,
            self.paper_analysis_id,
            self.pipeline.analysis_basis,
            tuple(self._analysis_records),
            replay,
            state["status"],
            signals,
            tuple(self._cfd_signals),
            paper,
            state,
            state["snapshot_hash"],
        )


__all__ = [
    "CTraderCapture",
    "CTraderPipeline",
    "CTraderPaperSession",
    "CTraderPipelineError",
    "CTraderPipelineResult",
    "CaptureCoverage",
    "PAPER_PRODUCT",
    "PAPER_VARIANT",
    "normalize_ctrader_capture",
    "signal_to_cfd_signal",
    "spot_event_to_cfd_quote",
    "synthetic_ctrader_capture",
    "synthetic_ctrader_payloads",
]
