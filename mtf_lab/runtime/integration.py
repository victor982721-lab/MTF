"""Coordinador común de captura, análisis incremental y persistencia.

``RuntimeCoordinator`` es el único adaptador que usan watch/replay para pasar
una secuencia normalizada por calidad, agregación, indicadores, estrategia y
liquidación virtual. Las escrituras son idempotentes; un corte antes del
checkpoint se reconstruye repitiendo como máximo el lote pendiente.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from ..configuration import EffectiveConfig
from ..core import Candle, MarketEvent, OperationMode, QualityFlag, Timeframe, parse_timeframe
from ..core.aggregation import BAR_CLOCK_BASIS, bar_clock_ordinal
from ..core.market_profiles import market_profile
from ..core.reference import m1_reference_signals
from ..ops.persistence import SQLiteStore, payload_hash
from .consumers import SignalConsumer
from .processor import IncrementalProcessor, ProcessResult, ReplayResult, RuntimeIssue, _looks_like_candle, _replay_key
from .state import SimulationConfig as RuntimeSimulationConfig
from .state import candle_dict, event_dict, to_core_candle, to_core_event


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return value


def capture_hash(records: Iterable[Any]) -> str:
    """Fingerprint de entrada incluyendo disponibilidad/calidad/base/identidad."""
    h = hashlib.sha256()
    for record in records:
        h.update(
            json.dumps(
                _jsonable(record), sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":")
            ).encode()
        )
        h.update(b"\n")
    return h.hexdigest()


def runtime_simulation_config(config: EffectiveConfig) -> RuntimeSimulationConfig:
    sim = config.simulation
    base = sim.requested_base_price or config.price_base
    if base in {"close", "trade"}:
        base = "traded"
    return RuntimeSimulationConfig(
        horizons_seconds=sim.horizons_seconds,
        entry_latency_seconds=sim.entry_latency_seconds,
        entry_rule=sim.entry_rule,
        exit_rule=sim.exit_rule,
        max_price_age_seconds=sim.max_price_age_seconds,
        stake=sim.stake,
        payout_net=sim.payout_net,
        loss_amount=sim.loss_amount,
        tie_net=sim.tie_net,
        tie_tolerance=sim.tie_tolerance,
        costs=sim.costs,
        horizon_from=sim.horizon_from,
        requested_base_price=base,
        resolution=config.timeframes[0].name,
    )


def _mode(value: str | OperationMode) -> OperationMode:
    if isinstance(value, OperationMode):
        return value
    text = str(value).upper()
    if text in {"LIVE", "OBSERVACIÓN EN DIRECTO", "OBSERVACION_EN_DIRECTO"}:
        return OperationMode.LIVE
    if text in {"SYNTHETIC", "SINTETICO", "SINTÉTICO", "OFFLINE"}:
        return OperationMode.SYNTHETIC
    return OperationMode.REPLAY


def _timeframe_name(value: Timeframe | str | int) -> str:
    return parse_timeframe(value).name


@dataclass(slots=True)
class CoordinatorStatus:
    capture_id: str
    analysis_id: str
    mode: str
    connection: str
    analysis_enabled: bool
    analysis_blocked_reasons: list[str]
    pending_simulations: int
    completed_simulations: int
    last_checkpoint_events: int
    capture_state: str = "CAPTURING"
    reconciliation_state: str = "NOT_APPLICABLE"
    freshness_state: str = "UNKNOWN"
    continuity_state: str = "UNKNOWN"
    last_market_time: str | None = None
    last_processed_at: str | None = None
    last_heartbeat_at: str | None = None
    block_details: dict[str, Any] = field(default_factory=dict)
    block_history: tuple[dict[str, Any], ...] = ()


_BLOCKING_ISSUE_CODES = frozenset(
    {
        "out_of_order_event",
        "out_of_order_candle",
        "event_invalid",
        "invalid_event",
        "candle_invalid",
        "price_base_mismatch",
        "quality_blocked",
        "partial_bucket",
        "gap",
        "out_of_order",
        "out_of_order_interval",
        "late_closed_interval",
        "mode_mismatch",
        "timeframe_not_configured",
    }
)
_DATA_RECORD_MODULE_PREFIX = "mtf_lab.data"
_CANDLE_COPY_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "closed",
    "source",
    "price_basis",
    "revision",
    "available_at",
    "received_at",
    "metadata",
)


def _is_data_record(record: Any) -> bool:
    return bool(record.__class__.__module__.startswith(_DATA_RECORD_MODULE_PREFIX))


def _record_mapping(record: Any) -> dict[str, Any]:
    if hasattr(record, "to_dict"):
        raw = record.to_dict()
        return dict(raw) if isinstance(raw, Mapping) else {}
    if isinstance(record, Mapping):
        return dict(record)
    return {}


def _record_is_synthetic(record: Any) -> bool:
    if isinstance(record, Mapping):
        return bool(record.get("synthetic", record.get("is_synthetic", False)))
    return bool(getattr(record, "synthetic", False))


def _fallback_data_candle_row(record: Any) -> dict[str, Any]:
    raw = _record_mapping(record)
    return {
        "candle_id": raw.get("data_id", raw.get("source_record_id")),
        "instrument": raw.get("instrument"),
        "timeframe": raw.get("resolution_seconds", raw.get("resolution")),
        "start_ts": raw.get("interval_start"),
        "end_ts": raw.get("interval_end"),
        **raw,
    }


def _data_candle_row(record: Any, mode: OperationMode) -> dict[str, Any]:
    try:
        return candle_dict(to_core_candle(record, mode=mode))
    except Exception:
        # A malformed capture is still durable evidence; it is not fed to the
        # processor and remains visibly blocked.
        return _fallback_data_candle_row(record)


def _mapping_candle_row(record: Any) -> dict[str, Any]:
    raw = _record_mapping(record)
    return {
        "candle_id": raw.get("data_id", raw.get("candle_id")),
        "instrument": raw.get("instrument"),
        "timeframe": raw.get("resolution", raw.get("timeframe")),
        "start_ts": raw.get("interval_start", raw.get("start")),
        "end_ts": raw.get("interval_end", raw.get("end")),
        **{key: raw.get(key) for key in _CANDLE_COPY_FIELDS},
    }


def _normalize_input_candle_row(row: dict[str, Any], record: Any, mode: OperationMode) -> dict[str, Any]:
    if "revision" not in row:
        metadata_value = row.get("metadata")
        metadata: Mapping[str, Any] = metadata_value if isinstance(metadata_value, Mapping) else {}
        row["revision"] = int(metadata.get("revision", 0) or 0)
    if isinstance(row.get("quality"), Mapping):
        row["quality"] = row["quality"].get("status", "UNKNOWN")
    if not _record_is_synthetic(record):
        row["mode"] = mode.value
    return row


def _indicator_values(point: Any) -> dict[str, Any]:
    return {"ema_fast": point.ema_fast, "ema_slow": point.ema_slow, "rsi": point.rsi, "atr": point.atr}


def _input_candle_provenance(processor: IncrementalProcessor, row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        start_value = row.get("start", row.get("start_ts", row.get("interval_start")))
        start_dt = (
            start_value
            if isinstance(start_value, datetime)
            else datetime.fromisoformat(str(start_value).replace("Z", "+00:00"))
        )
        tf_name = str(row.get("timeframe", "M1")).upper()
        absolute_index = processor._point_index_by_start.get(tf_name, {}).get(start_dt)
        index = (absolute_index - processor._point_base_index.get(tf_name, 0)) if absolute_index is not None else None
        if index is None or index < 0:
            return None
        point = processor.indicator_points[tf_name][index]
        metadata = row.get("metadata")
        return {
            "origin": row.get("origin", "native"),
            "metadata": dict(metadata) if isinstance(metadata, Mapping) else {},
            "indicators": _indicator_values(point),
        }
    except (AttributeError, IndexError, KeyError, TypeError, ValueError):
        return None


def _event_row(record: Any, mode: OperationMode) -> dict[str, Any] | None:
    if isinstance(record, MarketEvent):
        row = event_dict(record)
        if record.mode is not OperationMode.SYNTHETIC:
            row["mode"] = mode.value
        return row
    if _is_data_record(record):
        try:
            row = event_dict(to_core_event(record, mode=mode))
            if not bool(getattr(record, "synthetic", False)):
                row["mode"] = mode.value
        except Exception:
            row = _record_mapping(record)
        return row
    if hasattr(record, "to_dict") or isinstance(record, Mapping):
        return _record_mapping(record)
    return None


class RuntimeCoordinator:
    """Persistencia idempotente alrededor de :class:`IncrementalProcessor`."""

    def __init__(
        self,
        store: SQLiteStore,
        session_id: str,
        config: EffectiveConfig,
        *,
        mode: str | OperationMode,
        dataset_hash: str,
        variant: str = "trend_pullback_v1",
        partition: str = "all",
        contract_hash: str | None = None,
        checkpoint_name: str = "runtime",
        checkpoint_every: int = 100,
        source: str = "runtime",
        max_candles: int | None = 5000,
        signal_consumer: SignalConsumer | None = None,
        resume: bool = True,
        clock: Callable[[], datetime] | None = None,
        checkpoint_interval_seconds: float | None = None,
        identity_extra: Mapping[str, Any] | None = None,
    ) -> None:
        self.store = store
        self.session_id = session_id
        self.config = config
        # Estado externo de captura (socket/feed/reconciliación). El replay
        # local queda explícitamente fuera de estos bloqueos; watch lo actualiza
        # en cada transición observable del adaptador.
        normalized_mode = _mode(mode)
        self.connection_state = (
            "OFFLINE" if normalized_mode in {OperationMode.REPLAY, OperationMode.SYNTHETIC} else "CONNECTING"
        )
        self.reconciliation_state = (
            "NOT_APPLICABLE" if normalized_mode in {OperationMode.REPLAY, OperationMode.SYNTHETIC} else "PENDING"
        )
        self.external_blocked_reasons: list[str] = []
        self._external_block_details: dict[str, dict[str, Any]] = {}
        self._runtime_block_details: dict[str, dict[str, Any]] = {}
        self._startup_event_time: datetime | None = None
        self._startup_event_id: str | None = None
        self._block_history: list[dict[str, Any]] = []
        self.capture_state = "CAPTURING"
        self.freshness_state = (
            "NOT_APPLICABLE" if normalized_mode in {OperationMode.REPLAY, OperationMode.SYNTHETIC} else "UNKNOWN"
        )
        self.continuity_state = "UNKNOWN"
        self.last_heartbeat_at: datetime | None = None
        self.last_processed_at: datetime | None = None
        self.last_received_at: datetime | None = None
        self._clock = clock or (lambda: datetime.now(UTC))
        self.checkpoint_interval_seconds = (
            float(checkpoint_interval_seconds) if checkpoint_interval_seconds is not None else None
        )
        if self.checkpoint_interval_seconds is not None and self.checkpoint_interval_seconds <= 0:
            raise ValueError("checkpoint_interval_seconds debe ser positivo")
        self._last_checkpoint_at: datetime | None = None
        self.mode = _mode(mode)
        self.dataset_hash = str(dataset_hash)
        self.variant = str(variant)
        self.partition = str(partition)
        self.checkpoint_name = checkpoint_name
        self.checkpoint_every = max(1, int(checkpoint_every))
        self.source = source
        self.max_candles = max_candles
        self.analysis_config_hash = config.config_hash
        effective_simulation = runtime_simulation_config(config)
        self.contract_hash = (
            contract_hash
            or hashlib.sha256(
                json.dumps(effective_simulation.to_dict(), sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )
        self.analysis_id = store.create_analysis(
            session_id,
            dataset_hash=self.dataset_hash,
            config_hash=self.analysis_config_hash,
            variant=self.variant,
            contract_hash=self.contract_hash,
            partition=self.partition,
            code_version=config.version,
            metadata={"config_path": config.path},
            identity_extra=identity_extra or {},
        )
        self.capture_id = session_id
        self._last_checkpoint_events = 0
        self._input_ordinal = 0
        # Resume sólo dentro del namespace de este análisis. Nunca reutilizar
        # silenciosamente un checkpoint de otra configuración/contrato.
        snapshot = (
            store.get_checkpoint(session_id, checkpoint_name, analysis_id=self.analysis_id, allow_alternate=False)
            if resume
            else None
        )
        # Un cambio de configuración/contrato crea un análisis nuevo. Nunca se
        # mezcla con el snapshot de otro análisis: se conserva ese checkpoint y
        # se inicia una secuencia paralela con nombre determinista.
        processor_snapshot = snapshot.get("state", {}).get("processor") if snapshot else None
        snapshot_analysis = snapshot.get("cursor", {}).get("analysis_id") if snapshot else None
        if snapshot_analysis not in {None, self.analysis_id}:
            snapshot = None
            processor_snapshot = None
            self.checkpoint_name = f"{checkpoint_name}:{self.analysis_id[:12]}"
        if processor_snapshot and snapshot is not None:
            self.processor = IncrementalProcessor.from_checkpoint(
                processor_snapshot,
                signal_consumer=signal_consumer,
            )
            if self.processor.mode is not self.mode:
                raise ValueError("el checkpoint y el modo de ejecución no coinciden")
            self._input_ordinal = int(snapshot.get("events_processed", 0))
            self._last_checkpoint_events = self._input_ordinal
            operational = (
                snapshot.get("state", {}).get("operational", {}) if isinstance(snapshot.get("state"), Mapping) else {}
            )
            if isinstance(operational, Mapping):
                self.capture_state = str(operational.get("capture_state", self.capture_state))
                self.freshness_state = str(operational.get("freshness_state", self.freshness_state))
                self.continuity_state = str(operational.get("continuity_state", self.continuity_state))
                self.connection_state = str(operational.get("connection_state", self.connection_state))
                self.reconciliation_state = str(operational.get("reconciliation_state", self.reconciliation_state))
                self.external_blocked_reasons = [str(item) for item in operational.get("external_blocked_reasons", ())]
                self._external_block_details = {
                    str(key): dict(value)
                    for key, value in dict(operational.get("external_block_details", {})).items()
                    if isinstance(value, Mapping)
                }
                self._runtime_block_details = {
                    str(key): dict(value)
                    for key, value in dict(operational.get("runtime_block_details", {})).items()
                    if isinstance(value, Mapping)
                }
                self._block_history = [
                    dict(item) for item in operational.get("block_history", ()) if isinstance(item, Mapping)
                ]
                self._restore_startup_observation(operational)
                self.last_received_at = _parse_datetime(operational.get("last_received_at"))
                self.last_heartbeat_at = _parse_datetime(operational.get("last_heartbeat_at"))
                self.last_processed_at = _parse_datetime(operational.get("last_processed_at"))
                self._last_checkpoint_at = _parse_datetime(operational.get("last_checkpoint_at"))
        else:
            self.processor = IncrementalProcessor(
                strategy=config.strategy,
                simulation=runtime_simulation_config(config),
                timeframes=[tf.name for tf in config.timeframes],
                mode=self.mode,
                instrument=config.instrument,
                source=source,
                price_base=("traded" if config.price_base == "close" else config.price_base),
                max_candles=max_candles,
                signal_consumer=signal_consumer,
                market_candidate_id=config.execution.get("market_candidate_id"),
            )

    @property
    def signal_consumer(self) -> SignalConsumer:
        """Selected product boundary; no caller needs processor internals."""

        return self.processor.signal_consumer

    @property
    def signals(self) -> tuple[Any, ...]:
        """Detector signals retained by the processor."""

        return tuple(self.processor.signals)

    def market_profile_snapshot(self) -> dict[str, Any] | None:
        """Publish bounded, already-computed strategy context without SQL/I/O.

        A bar-clock ordinal is not a count of samples, sessions, or observed
        OHLC bars. It lets the executor identify overdue exits after a quote
        outage without manufacturing the missing market trajectory.
        """
        candidate_id = self.config.execution.get("market_candidate_id")
        if candidate_id is None:
            return None
        profile = market_profile(str(candidate_id))
        available = self.processor.last_available_at
        point = self.processor.latest_indicator_point(profile.trigger_timeframe, at=available)
        market_time = self.processor.last_event_time
        if market_time is None and point is not None and isinstance(point.end, datetime):
            market_time = point.end
        return {
            "market_candidate_id": profile.candidate_id,
            "trigger_timeframe": profile.trigger_timeframe,
            "trigger_bar_count": bar_clock_ordinal(market_time, profile.trigger_timeframe) if market_time else None,
            "risk_bar_clock_basis": BAR_CLOCK_BASIS,
            "latest_trigger_start": _jsonable(point.start) if point is not None else None,
            "latest_trigger_end": _jsonable(point.end) if point is not None else None,
            "latest_trigger_available_at": _jsonable(point.available_at) if point is not None else None,
            "atr": str(point.atr) if point is not None and point.atr is not None and point.quality.valid else None,
            "last_market": _jsonable(market_time),
            "last_available": _jsonable(available),
            "data_mode": self.mode.value,
            "indicator_updates": self.processor.indicator_updates.get(profile.trigger_timeframe, 0),
            "source": self.processor.source,
            "read_only_memory_projection": True,
        }

    def _analysis_fields(self, *, variant: str | None = None) -> dict[str, Any]:
        return {
            "analysis_id": self.analysis_id,
            "analysis": self.variant,
            "variant": variant or self.variant,
            "analysis_config_hash": self.analysis_config_hash,
            "contract_hash": self.contract_hash,
            "partition": self.partition,
        }

    def _input_candle_row(self, record: Any) -> dict[str, Any]:
        if isinstance(record, Candle):
            row = candle_dict(record)
        elif _is_data_record(record):
            row = _data_candle_row(record, self.mode)
        elif hasattr(record, "to_dict"):
            row = _mapping_candle_row(record)
        else:
            row = dict(record)
        row = _normalize_input_candle_row(row, record, self.mode)
        provenance = _input_candle_provenance(self.processor, row)
        if provenance is not None:
            row["provenance"] = provenance
        return row

    def _persist_input(self, record: Any, *, accepted: bool) -> None:
        if not accepted or (isinstance(record, Mapping) and record.get("_persisted_capture")):
            return
        if _looks_like_candle(record):
            self.store.save_candle(self.session_id, self._input_candle_row(record), ordinal=self._input_ordinal)
            return
        event_row = _event_row(record, self.mode)
        if event_row is not None:
            self.store.save_event(self.session_id, event_row, ordinal=self._input_ordinal)

    def _persist_simulation(self, item: Any) -> None:
        raw = item.to_dict() if hasattr(item, "to_dict") else dict(item)
        # Simulation identities are namespaced by analysis so a changed
        # contract/configuration cannot update an older result in-place.
        if raw.get("simulation_id") is not None:
            simulation_id = str(raw["simulation_id"])
            raw["simulation_id"] = f"{self.analysis_id}:{simulation_id}"
        raw.update(self._analysis_fields())
        raw.setdefault("simulation_type", "VIRTUAL_CONTRACT")
        raw.setdefault("detected_ts", raw.get("detected_at"))
        raw.setdefault("expiry_ts", raw.get("expiry_at"))
        raw.setdefault("entry_ts", raw.get("entry_at"))
        raw.setdefault("stake", self.config.simulation.stake)
        # PendingSimulation serializa ``outcome=None`` mientras sigue abierto;
        # la tabla pública distingue ese estado de INDETERMINATE definitivo.
        raw["outcome"] = raw.get("outcome") or (
            "PENDING" if str(raw.get("status", "PENDING")).upper() == "PENDING" else "INDETERMINATE"
        )
        raw.setdefault("net_result", None)
        raw.setdefault("price_base", raw.get("base_price", self.config.price_base))
        raw.setdefault("quality", "UNKNOWN")
        raw.setdefault("resolution", self.config.timeframes[0].name)
        assumptions_value = raw.get("assumptions")
        assumptions: Mapping[str, Any] = assumptions_value if isinstance(assumptions_value, Mapping) else {}
        raw["assumptions"] = {
            **dict(assumptions),
            "data_complete": bool(raw.get("capture_complete", False))
            or str(raw.get("status", "PENDING")).upper() in {"RESOLVED", "INDETERMINATE"},
            "capture_complete": bool(raw.get("capture_complete", False)),
            "analysis_id": self.analysis_id,
            "entry_rule": self.config.simulation.entry_rule,
            "exit_rule": self.config.simulation.exit_rule,
            "horizon_from": self.config.simulation.horizon_from,
            "virtual_contract": {
                "stake": self.config.simulation.stake,
                "payout_net": self.config.simulation.payout_net,
                "loss_amount": self.config.simulation.loss_amount,
                "tie_net": self.config.simulation.tie_net,
                "costs": self.config.simulation.costs,
            },
        }
        self.store.update_simulation(
            self.session_id,
            raw,
            analysis_id=self.analysis_id,
            variant=self.variant,
            analysis_config_hash=self.analysis_config_hash,
            contract_hash=self.contract_hash,
            partition=self.partition,
            ignore_pending_terminal=True,
        )

    def _persist_result_input(self, record: Any) -> Any:
        # La captura conserva también entradas rechazadas/duplicadas para
        # auditoría; sólo el procesador decide si alimentan el análisis.
        self._persist_input(record, accepted=True)
        if not _looks_like_candle(record):
            return None
        raw = _record_mapping(record)
        return raw.get("candle_id", raw.get("data_id", raw.get("source_record_id")))

    def _persist_result_events(self, events: Iterable[MarketEvent]) -> None:
        # Result.events contains the canonical representation (and is the
        # source of truth when a provider adapter already normalized the input).
        for event in events:
            self.store.save_event(
                self.session_id,
                {**event_dict(event), **self._analysis_fields()},
                ordinal=self._input_ordinal,
            )

    def _result_candle_point(self, candle: Candle) -> Any | None:
        try:
            timeframe = candle.timeframe_name
            absolute_index = self.processor._point_index_by_start.get(timeframe, {}).get(candle.start)
            index = (
                (absolute_index - self.processor._point_base_index.get(timeframe, 0))
                if absolute_index is not None
                else None
            )
            if index is not None and index >= 0:
                return self.processor.indicator_points[timeframe][index]
        except (AttributeError, IndexError, KeyError):
            pass
        return None

    def _persist_result_candles(self, candles: Iterable[Candle], input_candle_id: Any) -> None:
        for candle in candles:
            # Una vela nativa ya capturada conserva sus bytes/procedencia; no
            # se duplica como si fuera una vela derivada del mismo intervalo.
            if input_candle_id is not None and str(candle.candle_id) == str(input_candle_id):
                continue
            point = self._result_candle_point(candle)
            provenance = {
                "origin": candle.origin,
                "metadata": dict(candle.metadata),
                "indicators": _indicator_values(point) if point is not None else {},
            }
            row = candle_dict(candle)
            row["provenance"] = provenance
            self.store.save_candle(self.session_id, row, ordinal=self._input_ordinal)

    def _persist_discard_conditions(self, row: Mapping[str, Any]) -> None:
        for ordinal, condition in enumerate(row.get("conditions", ())):
            if not condition.get("mandatory", True) or str(condition.get("state", "")).lower() not in {
                "failed",
                "unknown",
            }:
                continue
            reason = condition.get("reason") or condition.get("state") or "mandatory_condition_not_met"
            discard = {
                "discard_id": f"{self.analysis_id}:discard:{row['decision_id']}:{ordinal}",
                "decision_id": row["decision_id"],
                "observed_ts": row.get("observed_ts"),
                "reason_code": str(reason),
                "required": True,
                "condition_status": condition.get("state", "unknown"),
                "payload": {"condition": condition, "decision": row},
                **self._analysis_fields(),
            }
            self.store.save_discard(
                self.session_id,
                discard,
                ordinal=ordinal,
                analysis_id=self.analysis_id,
                variant=self.variant,
                analysis_config_hash=self.analysis_config_hash,
                contract_hash=self.contract_hash,
                partition=self.partition,
            )

    def _persist_result_evaluations(self, evaluations: Iterable[Any], *, allow_signals: bool) -> None:
        for evaluation in evaluations:
            row = evaluation.as_dict() if hasattr(evaluation, "as_dict") else _jsonable(evaluation)
            if not allow_signals and str(row.get("decision", row.get("status", ""))).lower() == "signal":
                continue
            row.update(self._analysis_fields())
            row["observed_ts"] = row.get("timestamp")
            row["available_ts"] = row.get("available_at")
            row["kind"] = row.get("stage", "strategy_evaluation")
            row["status"] = row.get("decision", "UNKNOWN")
            row["decision_id"] = "dec_" + payload_hash({"analysis_id": self.analysis_id, "evaluation": row})[:32]
            self.store.save_decision(
                self.session_id,
                row,
                ordinal=self._input_ordinal,
                analysis_id=self.analysis_id,
                variant=self.variant,
                analysis_config_hash=self.analysis_config_hash,
                contract_hash=self.contract_hash,
                partition=self.partition,
            )
            if str(row.get("decision", "")).lower() in {"blocked", "discarded"}:
                self._persist_discard_conditions(row)

    def _persist_result_signals(self, signals: Iterable[Any], *, allow_signals: bool) -> None:
        if not allow_signals:
            return
        for signal in signals:
            row = {**(signal.as_dict() if hasattr(signal, "as_dict") else _jsonable(signal)), **self._analysis_fields()}
            self.store.save_signal(
                self.session_id,
                row,
                ordinal=self._input_ordinal,
                analysis_id=self.analysis_id,
                variant=self.variant,
                analysis_config_hash=self.analysis_config_hash,
                contract_hash=self.contract_hash,
                partition=self.partition,
            )

    def _persist_result_simulations(self, result: ProcessResult) -> None:
        for item in (*result.simulations, *result.pending_simulations):
            self._persist_simulation(item)

    def _persist_result(self, record: Any, result: ProcessResult, *, allow_signals: bool = True) -> None:
        input_candle_id = self._persist_result_input(record) if record else None
        if not record:
            self._persist_result_events(result.events)
        self._persist_result_candles(result.candles, input_candle_id)
        self._persist_result_evaluations(result.evaluations, allow_signals=allow_signals)
        self._persist_result_signals(result.signals, allow_signals=allow_signals)
        self._persist_result_simulations(result)

    def capture_only(self, record: Any) -> None:
        """Guarda una captura sin alimentar agregación, indicadores ni reglas."""
        self._persist_input(record, accepted=True)
        self._input_ordinal += 1
        self._update_last_received(record)
        self.last_processed_at = _clock_value(self._clock)
        self._checkpoint_if_due()

    def _restore_startup_observation(self, operational: Mapping[str, Any]) -> None:
        self._startup_event_time = _parse_datetime(operational.get("startup_event_time"))
        identity = operational.get("startup_event_id")
        self._startup_event_id = identity if isinstance(identity, str) else None

    def _remember_startup_observation(self, record: Any, result: ProcessResult) -> None:
        if self.mode is OperationMode.LIVE or _looks_like_candle(record):
            return
        if self._startup_event_time is not None or self.processor.events_processed != 1:
            return
        if result.accepted and len(result.events) == 1:
            event = result.events[0]
            self._startup_event_time = event.event_time
            self._startup_event_id = event.event_id

    def _is_startup_partial(self, issue: RuntimeIssue, result: ProcessResult) -> bool:
        """Recognize clipped first buckets, not an interruption of an existing stream.

        The first accepted event is the evidence: later partials, other quality
        flags, native input bars and LIVE are never classified this way.
        Indicator warmup and every operational gate continue to apply.
        """
        first = self._startup_event_time
        if self.mode is OperationMode.LIVE or first is None or self._startup_event_id is None:
            return False
        if issue.code == "partial_bucket":
            return issue.timestamp == first and issue.record_id == self._startup_event_id
        if issue.code != "quality_blocked":
            return False
        return any(
            candle.candle_id == issue.record_id
            and candle.closed
            and candle.origin == "aggregated"
            and candle.start < first < candle.end
            and candle.metadata.get("partial") is True
            and _parse_datetime(candle.metadata.get("coverage_start")) == first
            and QualityFlag.PARTIAL in candle.quality.flags
            and candle.quality.flags <= {QualityFlag.PARTIAL, QualityFlag.SYNTHETIC}
            for candle in result.candles
        )

    def _runtime_block_from_result(self, result: ProcessResult, *, aggregated_input: bool = False) -> None:
        for issue in result.issues:
            if issue.code not in _BLOCKING_ISSUE_CODES:
                continue
            key = f"continuity:{issue.code}"
            detail = {
                "code": issue.code,
                "message": issue.message,
                "record_id": issue.record_id,
                "timestamp": _jsonable(issue.timestamp),
                "active": True,
            }
            if aggregated_input and self._is_startup_partial(issue, result):
                self._block_history.append(
                    {
                        **detail,
                        "active": False,
                        "startup_partial": True,
                        "first_event_time": _jsonable(self._startup_event_time),
                    }
                )
                continue
            self._runtime_block_details[key] = detail
            self._block_history.append({**detail, "resolved": False})
            self.continuity_state = "BROKEN"

    def _checkpoint_if_due(self, *, force: bool = False) -> None:
        now = _clock_value(self._clock)
        count_due = self._input_ordinal - self._last_checkpoint_events >= self.checkpoint_every
        time_due = self.checkpoint_interval_seconds is not None and (
            self._last_checkpoint_at is None
            or (now - self._last_checkpoint_at).total_seconds() >= self.checkpoint_interval_seconds
        )
        if force or count_due or time_due:
            self.checkpoint()

    def process(self, record: Any, *, bootstrap: bool = False) -> ProcessResult:
        # The gate is checked before the transition. A blocked capture still
        # enters durable storage and the simulation book, but cannot emit new
        # strategy decisions/signals until an explicit resolution arrives.
        allow_signals = (not bootstrap) and self.can_emit_signals()
        result = (
            self.processor.process_bar(record, evaluate_strategy=allow_signals)
            if _looks_like_candle(record)
            else self.processor.process_event(record, evaluate_strategy=allow_signals)
        )
        self._remember_startup_observation(record, result)
        self._runtime_block_from_result(result, aggregated_input=not _looks_like_candle(record))
        if result.accepted and self.continuity_state == "UNKNOWN":
            self.continuity_state = "CONTINUOUS"
        self._persist_result(record, result, allow_signals=allow_signals)
        self._input_ordinal += 1
        self.last_processed_at = _clock_value(self._clock)
        self._update_last_received(record)
        self._checkpoint_if_due()
        return result

    def advance(self, watermark: datetime, *, complete: bool = False) -> ProcessResult:
        # ``complete`` only changes simulation grace/watermark. It never
        # bypasses an active gate for new decisions.
        allow_signals = self.can_emit_signals()
        if complete and self.processor.signal_consumer.consumer_type == "binary_simulation":
            watermark = watermark + timedelta(
                seconds=max(self.config.simulation.horizons_seconds) + self.config.simulation.max_price_age_seconds
            )
        result = self.processor.finalize(watermark, evaluate_strategy=allow_signals, capture_complete=complete)
        self._runtime_block_from_result(result, aggregated_input=True)
        self._persist_result({}, result, allow_signals=allow_signals)
        self.last_processed_at = _clock_value(self._clock)
        self.checkpoint()
        return result

    def can_emit_signals(self) -> bool:
        return self.status().analysis_enabled

    def _update_last_received(self, record: Any) -> None:
        value = _record_datetime(record, "received_at", "received_ts", "available_at", "available_ts")
        if value is not None:
            self.last_received_at = value if self.last_received_at is None else max(self.last_received_at, value)

    def heartbeat(self, now: datetime | None = None) -> ProcessResult:
        """Record transport liveness without pretending a price was received."""
        self.last_heartbeat_at = _parse_datetime(now) or _clock_value(self._clock)
        if self.mode is OperationMode.LIVE and self.connection_state not in {"ERROR", "DISCONNECTED"}:
            self.connection_state = "CONNECTED"
        return self.tick(self.last_heartbeat_at)

    def tick(self, now: datetime | None = None, *, force_checkpoint: bool = False) -> ProcessResult:
        """Advance clock-driven closures/expirations and checkpoint cadence."""
        current = _parse_datetime(now) or _clock_value(self._clock)
        if self.mode is OperationMode.LIVE and self.last_received_at is not None:
            age = (current - self.last_received_at).total_seconds()
            if age > self.config.quality.max_feed_age_seconds:
                self.update_feed_state(
                    freshness="STALE",
                    blocked_reasons=tuple(dict.fromkeys((*self.external_blocked_reasons, "feed_stale"))),
                )
        result = self.advance(current, complete=False)
        self._checkpoint_if_due(force=force_checkpoint)
        return result

    def export_state(self) -> dict[str, Any]:
        """Return coordinator state for an atomic application checkpoint.

        The returned mapping contains only JSON-compatible detector and
        operational state. Product sessions can persist it beside their own
        snapshot and cursor without reaching into processor internals.
        """
        return {
            "runtime_checkpoint_version": 1,
            "processor": self.processor.checkpoint(),
            "config_hash": self.config.config_hash,
            "analysis_id": self.analysis_id,
            "cursor": {"analysis_id": self.analysis_id, "ordinal": self._input_ordinal},
            "status": asdict(self.status()),
            "operational": {
                "capture_state": self.capture_state,
                "connection_state": self.connection_state,
                "reconciliation_state": self.reconciliation_state,
                "freshness_state": self.freshness_state,
                "continuity_state": self.continuity_state,
                "external_blocked_reasons": list(self.external_blocked_reasons),
                "external_block_details": _jsonable(self._external_block_details),
                "runtime_block_details": _jsonable(self._runtime_block_details),
                "startup_event_time": _jsonable(self._startup_event_time),
                "startup_event_id": self._startup_event_id,
                "block_history": _jsonable(self._block_history[-256:]),
                "last_received_at": _jsonable(self.last_received_at),
                "last_heartbeat_at": _jsonable(self.last_heartbeat_at),
                "last_processed_at": _jsonable(self.last_processed_at),
                "last_checkpoint_at": _jsonable(self._last_checkpoint_at),
            },
        }

    def restore_state(self, snapshot: Mapping[str, Any] | str) -> None:
        """Restore detector and coordinator state after validating identity."""
        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot)
        if not isinstance(snapshot, Mapping):
            raise TypeError("snapshot debe ser mapping o JSON")
        # Store checkpoints wrap the application state under ``state``; direct
        # exports do not. Accept both forms without silently accepting a third.
        payload = snapshot.get("state", snapshot)
        if not isinstance(payload, Mapping):
            raise ValueError("state de coordinador inválido")
        analysis_id = payload.get("analysis_id")
        if analysis_id is not None and str(analysis_id) != self.analysis_id:
            raise ValueError("analysis_id del checkpoint no coincide")
        config_hash = payload.get("config_hash")
        if config_hash is not None and str(config_hash) != self.config.config_hash:
            raise ValueError("config_hash del checkpoint no coincide")
        processor_snapshot = payload.get("processor")
        if not isinstance(processor_snapshot, Mapping):
            raise ValueError("checkpoint sin processor")
        # Keep the selected consumer object (and any application sink) while
        # restoring its versioned state through the public processor boundary.
        self.processor = IncrementalProcessor.from_checkpoint(
            processor_snapshot,
            signal_consumer=self.processor.signal_consumer,
        )
        cursor = payload.get("cursor", snapshot.get("cursor", {}))
        if not isinstance(cursor, Mapping):
            cursor = {}
        cursor_analysis = cursor.get("analysis_id")
        if cursor_analysis is not None and str(cursor_analysis) != self.analysis_id:
            raise ValueError("analysis_id del cursor no coincide")
        self._input_ordinal = int(cursor.get("ordinal", snapshot.get("events_processed", 0)))
        self._last_checkpoint_events = self._input_ordinal
        operational = payload.get("operational", {})
        if isinstance(operational, Mapping):
            self.capture_state = str(operational.get("capture_state", self.capture_state))
            self.freshness_state = str(operational.get("freshness_state", self.freshness_state))
            self.continuity_state = str(operational.get("continuity_state", self.continuity_state))
            self.connection_state = str(operational.get("connection_state", self.connection_state))
            self.reconciliation_state = str(operational.get("reconciliation_state", self.reconciliation_state))
            self.external_blocked_reasons = [str(item) for item in operational.get("external_blocked_reasons", ())]
            self._external_block_details = {
                str(key): dict(value)
                for key, value in dict(operational.get("external_block_details", {})).items()
                if isinstance(value, Mapping)
            }
            self._runtime_block_details = {
                str(key): dict(value)
                for key, value in dict(operational.get("runtime_block_details", {})).items()
                if isinstance(value, Mapping)
            }
            self._block_history = [
                dict(item) for item in operational.get("block_history", ()) if isinstance(item, Mapping)
            ]
            self._restore_startup_observation(operational)
            self.last_received_at = _parse_datetime(operational.get("last_received_at"))
            self.last_heartbeat_at = _parse_datetime(operational.get("last_heartbeat_at"))
            self.last_processed_at = _parse_datetime(operational.get("last_processed_at"))
            self._last_checkpoint_at = _parse_datetime(operational.get("last_checkpoint_at"))

    def checkpoint(self) -> dict[str, Any]:
        state = self.export_state()
        self.store.save_checkpoint(
            self.session_id,
            self.checkpoint_name,
            cursor={"analysis_id": self.analysis_id, "ordinal": self._input_ordinal},
            events_processed=self._input_ordinal,
            last_event_id=self.processor.last_event_id,
            state=state,
        )
        self._last_checkpoint_events = self._input_ordinal
        self._last_checkpoint_at = _clock_value(self._clock)
        return state

    def update_feed_state(
        self,
        *,
        connection: str | None = None,
        reconciliation: str | None = None,
        freshness: str | None = None,
        continuity: str | None = None,
        blocked_reasons: Iterable[str] = (),
        block_details: Mapping[str, Any] | None = None,
        heartbeat_at: datetime | None = None,
    ) -> None:
        """Replace the current operational gate; history never implies health."""
        if connection is not None:
            self.connection_state = str(connection).upper()
        if reconciliation is not None:
            self.reconciliation_state = str(reconciliation).upper()
        if freshness is not None:
            self.freshness_state = str(freshness).upper()
        if continuity is not None:
            self.continuity_state = str(continuity).upper()
            if self.continuity_state in {"CONTINUOUS", "VERIFIED", "RECOVERED", "RECOVERED_BOUNDED"}:
                self._runtime_block_details.clear()
        if heartbeat_at is not None:
            self.last_heartbeat_at = _parse_datetime(heartbeat_at)
        self.external_blocked_reasons = sorted({str(item) for item in blocked_reasons if str(item)})
        self._external_block_details = {
            str(key): (dict(value) if isinstance(value, Mapping) else {"reason": value})
            for key, value in (block_details or {}).items()
        }
        if self.external_blocked_reasons and block_details is None:
            self._external_block_details = {
                reason: {"reason": reason, "active": True} for reason in self.external_blocked_reasons
            }

    def resolve_continuity(self, *, reason: str = "explicit_recovery") -> None:
        """Resolve runtime continuity blocks only after an explicit recovery."""
        self._runtime_block_details.clear()
        self.continuity_state = "CONTINUOUS"
        self._block_history.append(
            {"reason": reason, "resolved": True, "timestamp": _jsonable(_clock_value(self._clock))}
        )

    def replay(
        self, records: Iterable[Any], *, sort: bool = True, bootstrap: bool = False, complete: bool = True
    ) -> ReplayResult:
        """Procesa una captura mixta mediante el mismo camino incremental."""
        materialized = list(records)
        if sort:
            materialized = [
                item for _, item in sorted(enumerate(materialized), key=lambda pair: _replay_key(pair[1], pair[0]))
            ]
        accepted = duplicate = rejected = candles = signals = evaluations = completed = 0
        issues: list[RuntimeIssue] = []
        last_watermark: datetime | None = None
        for record in materialized:
            result = self.process(record, bootstrap=bootstrap)
            if result.accepted:
                accepted += 1
            elif any(issue.code in {"duplicate_event", "duplicate_candle"} for issue in result.issues):
                duplicate += 1
            else:
                rejected += 1
            candles += len(result.candles)
            signals += len(result.signals)
            evaluations += len(result.evaluations)
            completed += len(result.completed_simulations)
            issues.extend(result.issues)
            for key in (
                "available_at",
                "available_ts",
                "received_at",
                "received_ts",
                "end",
                "end_ts",
                "interval_end",
                "event_time",
                "event_ts",
                "timestamp",
            ):
                value = record.get(key) if isinstance(record, Mapping) else getattr(record, key, None)
                if value is not None:
                    try:
                        candidate = (
                            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                            if not isinstance(value, datetime)
                            else value
                        )
                        if candidate.tzinfo is not None:
                            last_watermark = candidate if last_watermark is None else max(last_watermark, candidate)
                            break
                    except (TypeError, ValueError):
                        continue
        if complete and last_watermark is not None:
            final = self.advance(last_watermark, complete=True)
            candles += len(final.candles)
            signals += len(final.signals)
            evaluations += len(final.evaluations)
            completed += len(final.completed_simulations)
            issues.extend(final.issues)
        return ReplayResult(
            accepted,
            duplicate,
            rejected,
            candles,
            signals,
            evaluations,
            completed,
            len(self.processor.pending_simulations),
            tuple(issues),
        )

    def reference_signals(self) -> tuple[list[dict[str, Any]], str]:
        """Persiste la referencia M1 sobre la misma captura, aunque MTF sea cero."""
        name = _timeframe_name(self.config.strategy.trigger_timeframe)
        engine = self.processor.indicator_engines.get(name)
        if engine is None:
            return [], self.analysis_id
        series = engine.series
        rows = m1_reference_signals(
            series,
            rsi_threshold=self.config.strategy.rsi_threshold,
            mode=self.mode.value,
            identity_salt=self.config.config_hash,
        )
        reference_id = self.store.create_analysis(
            self.session_id,
            dataset_hash=self.dataset_hash,
            config_hash=self.analysis_config_hash,
            variant="m1_trigger_reference",
            contract_hash=self.contract_hash,
            partition=self.partition,
            code_version=self.config.version,
            metadata={"strategy": "m1_trigger_reference", "source_analysis_id": self.analysis_id},
        )
        for ordinal, signal in enumerate(rows):
            self.store.save_signal(
                self.session_id,
                signal,
                ordinal=ordinal,
                analysis_id=reference_id,
                variant="m1_trigger_reference",
                analysis_config_hash=self.analysis_config_hash,
                contract_hash=self.contract_hash,
                partition=self.partition,
            )
        return rows, reference_id

    def status(self) -> CoordinatorStatus:
        st = self.processor.status
        blocked = self._operational_blocked_reasons()
        blocked.extend(self._processor_blocked_reasons(st))
        blocked = sorted(set(blocked))
        enabled = not blocked
        detail = {
            **{key: value for key, value in self._external_block_details.items()},
            **{key: value for key, value in self._runtime_block_details.items()},
        }
        return CoordinatorStatus(
            self.capture_id,
            self.analysis_id,
            self.mode.value,
            self.connection_state,
            enabled,
            blocked,
            len(self.processor.pending_simulations),
            len(self.processor.completed_simulations),
            self._last_checkpoint_events,
            capture_state=self.capture_state,
            reconciliation_state=self.reconciliation_state,
            freshness_state=self.freshness_state,
            continuity_state=self.continuity_state,
            last_market_time=_jsonable(self.processor.last_event_time),
            last_processed_at=_jsonable(self.last_processed_at),
            last_heartbeat_at=_jsonable(self.last_heartbeat_at),
            block_details=detail,
            block_history=tuple(self._block_history[-256:]),
        )

    def _operational_blocked_reasons(self) -> list[str]:
        blocked = list(self.external_blocked_reasons)
        # Operational state is a separate gate from historical processor
        # issues: a socket/reconciliation/freshness failure remains blocking
        # until the adapter explicitly resolves it.
        if self.mode is OperationMode.LIVE and self.connection_state not in {"CONNECTED", "HEALTHY"}:
            blocked.append(f"connection:{self.connection_state.lower()}")
        if self.mode is OperationMode.LIVE and self.reconciliation_state not in {
            "VERIFIED",
            "RECONCILED",
            "NOT_APPLICABLE",
        }:
            blocked.append(f"reconciliation:{self.reconciliation_state.lower()}")
        if self.mode is OperationMode.LIVE and self.freshness_state in {"STALE", "UNKNOWN", "DISCONNECTED", "BLOCKED"}:
            blocked.append(f"freshness:{self.freshness_state.lower()}")
        if self.capture_state in {"STOPPED", "ERROR", "RECOVERING", "RECONCILING", "BLOCKED"}:
            blocked.append(f"capture:{self.capture_state.lower()}")
        if self.continuity_state in {"BROKEN", "UNVERIFIED", "BLOCKED"} or (
            self.continuity_state == "UNKNOWN" and self.processor.events_processed
        ):
            blocked.append(f"continuity:{self.continuity_state.lower()}")
        for key in self._runtime_block_details:
            blocked.append(key)
        return blocked

    def _processor_blocked_reasons(self, processor_status: Mapping[str, Any]) -> list[str]:
        blocked: list[str] = []
        for tf in (
            _timeframe_name(self.config.strategy.context_timeframe),
            _timeframe_name(self.config.strategy.preparation_timeframe),
            _timeframe_name(self.config.strategy.trigger_timeframe),
        ):
            if processor_status.get("warmup_pending", {}).get(tf, 0):
                blocked.append(f"warmup:{tf}")
        if self.continuity_state not in {"CONTINUOUS", "VERIFIED", "RECOVERED", "RECOVERED_BOUNDED"} and any(
            issue.code in _BLOCKING_ISSUE_CODES for issue in self.processor.issues[-20:]
        ):
            blocked.append("continuity_or_quality")
        return blocked

    def finish(self, *, status: str = "COMPLETED") -> None:
        self.capture_state = "STOPPED" if str(status).upper() in {"COMPLETED", "STOPPED"} else str(status).upper()
        self.checkpoint()
        self.store.finish_session(self.session_id, status=status)


def _parse_datetime(value: Any) -> datetime | None:
    """Parse an optional aware timestamp at the coordinator boundary."""
    if value is None:
        return None
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            result = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if result.tzinfo is None or result.utcoffset() is None:
        return None
    return result.astimezone(UTC)


def _clock_value(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    parsed = _parse_datetime(value)
    if parsed is None:
        raise ValueError("clock debe devolver datetime aware")
    return parsed


def _record_datetime(record: Any, *names: str) -> datetime | None:
    for name in names:
        value = record.get(name) if isinstance(record, Mapping) else getattr(record, name, None)
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


__all__ = ["CoordinatorStatus", "RuntimeCoordinator", "capture_hash", "runtime_simulation_config"]
