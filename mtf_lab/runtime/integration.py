"""Coordinador común de captura, análisis incremental y persistencia.

``RuntimeCoordinator`` es el único adaptador que usan watch/replay para pasar
una secuencia normalizada por calidad, agregación, indicadores, estrategia y
liquidación virtual. Las escrituras son idempotentes; un corte antes del
checkpoint se reconstruye repitiendo como máximo el lote pendiente.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from ..configuration import EffectiveConfig
from ..core import Candle, MarketEvent, OperationMode, PriceBase
from ..ops.persistence import SQLiteStore, payload_hash
from .processor import IncrementalProcessor, ProcessResult, ReplayResult, RuntimeIssue, _looks_like_candle, _replay_key
from .state import SimulationConfig as RuntimeSimulationConfig, candle_dict, event_dict, to_core_candle, to_core_event


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return value


def capture_hash(records: Iterable[Any]) -> str:
    """Fingerprint de entrada incluyendo disponibilidad/calidad/base/identidad."""
    h = hashlib.sha256()
    for record in records:
        h.update(json.dumps(_jsonable(record), sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":")).encode())
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
        resume: bool = True,
    ) -> None:
        self.store = store
        self.session_id = session_id
        self.config = config
        # Estado externo de captura (socket/feed/reconciliación). El replay
        # local queda explícitamente fuera de estos bloqueos; watch lo actualiza
        # en cada transición observable del adaptador.
        normalized_mode = _mode(mode)
        self.connection_state = "OFFLINE" if normalized_mode in {OperationMode.REPLAY, OperationMode.SYNTHETIC} else "CONNECTING"
        self.reconciliation_state = "NOT_APPLICABLE" if normalized_mode in {OperationMode.REPLAY, OperationMode.SYNTHETIC} else "PENDING"
        self.external_blocked_reasons: list[str] = []
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
        self.contract_hash = contract_hash or hashlib.sha256(json.dumps(effective_simulation.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.analysis_id = store.create_analysis(session_id, dataset_hash=self.dataset_hash, config_hash=self.analysis_config_hash, variant=self.variant, contract_hash=self.contract_hash, partition=self.partition, code_version=config.version, metadata={"config_path": config.path})
        self.capture_id = session_id
        self._last_checkpoint_events = 0
        self._input_ordinal = 0
        snapshot = store.get_checkpoint(session_id, checkpoint_name) if resume else None
        # Un cambio de configuración/contrato crea un análisis nuevo. Nunca se
        # mezcla con el snapshot de otro análisis: se conserva ese checkpoint y
        # se inicia una secuencia paralela con nombre determinista.
        processor_snapshot = snapshot.get("state", {}).get("processor") if snapshot else None
        snapshot_analysis = snapshot.get("cursor", {}).get("analysis_id") if snapshot else None
        if snapshot_analysis not in {None, self.analysis_id}:
            snapshot = None
            processor_snapshot = None
            self.checkpoint_name = f"{checkpoint_name}:{self.analysis_id[:12]}"
        if processor_snapshot:
            self.processor = IncrementalProcessor.from_checkpoint(processor_snapshot)
            if self.processor.mode is not self.mode:
                raise ValueError("el checkpoint y el modo de ejecución no coinciden")
            self._input_ordinal = int(snapshot.get("events_processed", 0))
            self._last_checkpoint_events = self._input_ordinal
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
            )

    def _analysis_fields(self, *, variant: str | None = None) -> dict[str, Any]:
        return {
            "analysis_id": self.analysis_id,
            "analysis": self.variant,
            "variant": variant or self.variant,
            "analysis_config_hash": self.analysis_config_hash,
            "contract_hash": self.contract_hash,
            "partition": self.partition,
        }

    def _persist_input(self, record: Any, *, accepted: bool) -> None:
        if not accepted:
            return
        if isinstance(record, Mapping) and record.get("_persisted_capture"):
            return
        if _looks_like_candle(record):
            if isinstance(record, Candle):
                row = candle_dict(record)
            elif record.__class__.__module__.startswith("mtf_lab.data"):
                # Reuse the strict data->core translator so persistence keeps
                # quality flags/reasons, revision, provenance and receipt time.
                try:
                    row = candle_dict(to_core_candle(record, mode=self.mode))
                except Exception:
                    # A malformed capture is still durable evidence; it is not
                    # fed to the processor and remains visibly blocked.
                    raw = record.to_dict() if hasattr(record, "to_dict") else {}
                    row = {"candle_id": raw.get("data_id", raw.get("source_record_id")), "instrument": raw.get("instrument"), "timeframe": raw.get("resolution_seconds", raw.get("resolution")), "start_ts": raw.get("interval_start"), "end_ts": raw.get("interval_end"), **raw}
            elif hasattr(record, "to_dict"):
                raw = record.to_dict(); row = {"candle_id": raw.get("data_id", raw.get("candle_id")), "instrument": raw.get("instrument"), "timeframe": raw.get("resolution", raw.get("timeframe")), "start_ts": raw.get("interval_start", raw.get("start")), "end_ts": raw.get("interval_end", raw.get("end")), **{key: raw.get(key) for key in ("open", "high", "low", "close", "volume", "closed", "source", "price_basis", "revision", "available_at", "received_at", "metadata")}}
            else:
                row = dict(record)
            # Store's scalar quality column is a public label; the complete
            # structured quality/provenance remains in payload_json.
            if "revision" not in row:
                metadata = row.get("metadata") if isinstance(row.get("metadata"), Mapping) else {}
                row["revision"] = int(metadata.get("revision", 0) or 0)
            if isinstance(row.get("quality"), Mapping):
                row["quality"] = row["quality"].get("status", "UNKNOWN")
            synthetic = bool(record.get("synthetic", record.get("is_synthetic", False))) if isinstance(record, Mapping) else bool(getattr(record, "synthetic", False))
            if not synthetic:
                row["mode"] = self.mode.value
            # The processor has already updated the point for this native
            # candle; expose it to the read model without a second calculation.
            try:
                start_value = row.get("start", row.get("start_ts", row.get("interval_start")))
                start_dt = start_value if isinstance(start_value, datetime) else datetime.fromisoformat(str(start_value).replace("Z", "+00:00"))
                index = self.processor._point_index_by_start.get(str(row.get("timeframe", "M1")).upper(), {}).get(start_dt)
                if index is not None:
                    point = self.processor.indicator_points[str(row.get("timeframe", "M1")).upper()][index]
                    row["provenance"] = {"origin": row.get("origin", "native"), "metadata": dict(row.get("metadata", {})) if isinstance(row.get("metadata"), Mapping) else {}, "indicators": {"ema_fast": point.ema_fast, "ema_slow": point.ema_slow, "rsi": point.rsi, "atr": point.atr}}
            except (AttributeError, IndexError, KeyError, TypeError, ValueError):
                pass
            self.store.save_candle(self.session_id, row, ordinal=self._input_ordinal)
        elif isinstance(record, MarketEvent):
            event_row = event_dict(record)
            if record.mode is not OperationMode.SYNTHETIC:
                event_row["mode"] = self.mode.value
            self.store.save_event(self.session_id, event_row, ordinal=self._input_ordinal)
        elif record.__class__.__module__.startswith("mtf_lab.data"):
            try:
                row = event_dict(to_core_event(record, mode=self.mode))
                if not bool(getattr(record, "synthetic", False)):
                    row["mode"] = self.mode.value
            except Exception:
                row = record.to_dict() if hasattr(record, "to_dict") else {}
            self.store.save_event(self.session_id, row, ordinal=self._input_ordinal)
        elif hasattr(record, "to_dict"):
            raw = record.to_dict(); self.store.save_event(self.session_id, raw, ordinal=self._input_ordinal)
        elif isinstance(record, Mapping):
            self.store.save_event(self.session_id, dict(record), ordinal=self._input_ordinal)

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
        raw["outcome"] = raw.get("outcome") or ("PENDING" if str(raw.get("status", "PENDING")).upper() == "PENDING" else "INDETERMINATE")
        raw.setdefault("net_result", None)
        raw.setdefault("price_base", raw.get("base_price", self.config.price_base))
        raw.setdefault("quality", "UNKNOWN")
        raw.setdefault("resolution", self.config.timeframes[0].name)
        assumptions = raw.get("assumptions") if isinstance(raw.get("assumptions"), Mapping) else {}
        raw["assumptions"] = {
            **dict(assumptions), "data_complete": str(raw.get("status", "PENDING")).upper() == "PENDING", "analysis_id": self.analysis_id,
            "entry_rule": self.config.simulation.entry_rule, "exit_rule": self.config.simulation.exit_rule,
            "horizon_from": self.config.simulation.horizon_from,
            "virtual_contract": {"stake": self.config.simulation.stake, "payout_net": self.config.simulation.payout_net, "loss_amount": self.config.simulation.loss_amount, "tie_net": self.config.simulation.tie_net, "costs": self.config.simulation.costs},
        }
        self.store.update_simulation(self.session_id, raw, analysis_id=self.analysis_id, variant=self.variant, analysis_config_hash=self.analysis_config_hash, contract_hash=self.contract_hash, partition=self.partition)

    def _persist_result(self, record: Any, result: ProcessResult) -> None:
        input_candle_id = None
        if record:
            # La captura conserva también entradas rechazadas/duplicadas para
            # auditoría; sólo el procesador decide si alimentan el análisis.
            self._persist_input(record, accepted=True)
            if _looks_like_candle(record):
                raw = record.to_dict() if hasattr(record, "to_dict") else (dict(record) if isinstance(record, Mapping) else {})
                input_candle_id = raw.get("candle_id", raw.get("data_id", raw.get("source_record_id")))
        # Result.events contains the canonical representation (and is the
        # source of truth when a provider adapter already normalized the input).
        if not record:
            for event in result.events:
                self.store.save_event(self.session_id, {**event_dict(event), **self._analysis_fields()}, ordinal=self._input_ordinal)
        for candle in result.candles:
            # Una vela nativa ya capturada conserva sus bytes/procedencia; no
            # se duplica como si fuera una vela derivada del mismo intervalo.
            if input_candle_id is not None and str(candle.candle_id) == str(input_candle_id):
                continue
            row = candle_dict(candle)
            point = None
            try:
                index = self.processor._point_index_by_start.get(candle.timeframe.name, {}).get(candle.start)
                if index is not None:
                    point = self.processor.indicator_points[candle.timeframe.name][index]
            except (AttributeError, IndexError, KeyError):
                point = None
            row["provenance"] = {"origin": candle.origin, "metadata": dict(candle.metadata), "indicators": ({"ema_fast": point.ema_fast, "ema_slow": point.ema_slow, "rsi": point.rsi, "atr": point.atr} if point is not None else {})}
            self.store.save_candle(self.session_id, row, ordinal=self._input_ordinal)
        for evaluation in result.evaluations:
            row = evaluation.as_dict() if hasattr(evaluation, "as_dict") else _jsonable(evaluation)
            row.update(self._analysis_fields())
            row["observed_ts"] = row.get("timestamp")
            row["available_ts"] = row.get("available_at")
            row["kind"] = row.get("stage", "strategy_evaluation")
            row["status"] = row.get("decision", "UNKNOWN")
            row["decision_id"] = "dec_" + payload_hash({"analysis_id": self.analysis_id, "evaluation": row})[:32]
            self.store.save_decision(self.session_id, row, ordinal=self._input_ordinal, analysis_id=self.analysis_id, variant=self.variant, analysis_config_hash=self.analysis_config_hash, contract_hash=self.contract_hash, partition=self.partition)
            if str(row.get("decision", "")).lower() in {"blocked", "discarded"}:
                for ordinal, condition in enumerate(row.get("conditions", ())):
                    if not condition.get("mandatory", True) or str(condition.get("state", "")).lower() not in {"failed", "unknown"}:
                        continue
                    reason = condition.get("reason") or condition.get("state") or "mandatory_condition_not_met"
                    self.store.save_discard(self.session_id, {"discard_id": f"{self.analysis_id}:discard:{row['decision_id']}:{ordinal}", "decision_id": row["decision_id"], "observed_ts": row.get("observed_ts"), "reason_code": str(reason), "required": True, "condition_status": condition.get("state", "unknown"), "payload": {"condition": condition, "decision": row}, **self._analysis_fields()}, ordinal=ordinal, analysis_id=self.analysis_id, variant=self.variant, analysis_config_hash=self.analysis_config_hash, contract_hash=self.contract_hash, partition=self.partition)
        for signal in result.signals:
            row = {**(signal.as_dict() if hasattr(signal, "as_dict") else _jsonable(signal)), **self._analysis_fields()}
            self.store.save_signal(self.session_id, row, ordinal=self._input_ordinal, analysis_id=self.analysis_id, variant=self.variant, analysis_config_hash=self.analysis_config_hash, contract_hash=self.contract_hash, partition=self.partition)
        for item in result.simulations:
            self._persist_simulation(item)
        for item in result.pending_simulations:
            self._persist_simulation(item)

    def capture_only(self, record: Any) -> None:
        """Guarda una vela/evento de bootstrap sin alimentar reglas ni indicadores."""
        self._persist_input(record, accepted=True)
        self._input_ordinal += 1
        if self._input_ordinal - self._last_checkpoint_events >= self.checkpoint_every:
            self.checkpoint()

    def process(self, record: Any, *, bootstrap: bool = False) -> ProcessResult:
        result = self.processor.process_bar(record, evaluate_strategy=not bootstrap) if _looks_like_candle(record) else self.processor.process_event(record, evaluate_strategy=not bootstrap)
        self._persist_result(record, result)
        self._input_ordinal += 1
        if self._input_ordinal - self._last_checkpoint_events >= self.checkpoint_every:
            self.checkpoint()
        return result

    def advance(self, watermark: datetime, *, complete: bool = False) -> ProcessResult:
        if complete:
            watermark = watermark + timedelta(seconds=max(self.config.simulation.horizons_seconds) + self.config.simulation.max_price_age_seconds)
        result = self.processor.finalize(watermark)
        self._persist_result({}, result)
        self.checkpoint()
        return result

    def checkpoint(self) -> None:
        import dataclasses
        state = {"processor": self.processor.checkpoint(), "config_hash": self.config.config_hash, "analysis_id": self.analysis_id, "status": dataclasses.asdict(self.status())}
        self.store.save_checkpoint(self.session_id, self.checkpoint_name, cursor={"analysis_id": self.analysis_id, "ordinal": self._input_ordinal}, events_processed=self._input_ordinal, last_event_id=self.processor.last_event_id, state=state)
        self._last_checkpoint_events = self._input_ordinal

    def update_feed_state(self, *, connection: str | None = None, reconciliation: str | None = None, blocked_reasons: Iterable[str] = ()) -> None:
        """Actualiza sólo el estado externo; no convierte etiquetas en calidad."""
        if connection is not None:
            self.connection_state = str(connection).upper()
        if reconciliation is not None:
            self.reconciliation_state = str(reconciliation).upper()
        self.external_blocked_reasons = sorted({str(item) for item in blocked_reasons if str(item)})

    def replay(self, records: Iterable[Any], *, sort: bool = True, bootstrap: bool = False, complete: bool = True) -> ReplayResult:
        """Procesa una captura mixta mediante el mismo camino incremental."""
        materialized = list(records)
        if sort:
            materialized = [item for _, item in sorted(enumerate(materialized), key=lambda pair: _replay_key(pair[1], pair[0]))]
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
            candles += len(result.candles); signals += len(result.signals); evaluations += len(result.evaluations); completed += len(result.completed_simulations)
            issues.extend(result.issues)
            for key in ("available_at", "available_ts", "received_at", "received_ts", "end", "end_ts", "interval_end", "event_time", "event_ts", "timestamp"):
                value = record.get(key) if isinstance(record, Mapping) else getattr(record, key, None)
                if value is not None:
                    try:
                        candidate = datetime.fromisoformat(str(value).replace("Z", "+00:00")) if not isinstance(value, datetime) else value
                        if candidate.tzinfo is not None:
                            last_watermark = candidate if last_watermark is None else max(last_watermark, candidate)
                            break
                    except (TypeError, ValueError):
                        continue
        if complete and last_watermark is not None:
            final = self.advance(last_watermark, complete=True)
            candles += len(final.candles); signals += len(final.signals); evaluations += len(final.evaluations); completed += len(final.completed_simulations); issues.extend(final.issues)
        return ReplayResult(accepted, duplicate, rejected, candles, signals, evaluations, completed, len(self.processor.pending_simulations), tuple(issues))

    def reference_signals(self) -> tuple[list[dict[str, Any]], str]:
        """Persiste la referencia M1 sobre la misma captura, aunque MTF sea cero."""
        from ..pipeline import m1_reference_signals
        name = self.config.strategy.trigger_timeframe.name
        engine = self.processor.indicator_engines.get(name)
        if engine is None:
            return [], self.analysis_id
        series = engine.series
        rows = m1_reference_signals(series, rsi_threshold=self.config.strategy.rsi_threshold, mode=self.mode.value, identity_salt=self.config.config_hash)
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
            self.store.save_signal(self.session_id, signal, ordinal=ordinal, analysis_id=reference_id, variant="m1_trigger_reference", analysis_config_hash=self.analysis_config_hash, contract_hash=self.contract_hash, partition=self.partition)
        return rows, reference_id

    def status(self) -> CoordinatorStatus:
        st = self.processor.status
        blocked: list[str] = list(self.external_blocked_reasons)
        for tf in (self.config.strategy.context_timeframe.name, self.config.strategy.preparation_timeframe.name, self.config.strategy.trigger_timeframe.name):
            if st.get("warmup_pending", {}).get(tf, 0):
                blocked.append(f"warmup:{tf}")
        if self.processor.issues:
            # Sólo la continuidad/quality visible bloquea el análisis; un
            # duplicado ya registrado no convierte una captura válida en stale.
            if any(issue.code in {"out_of_order_event", "out_of_order_candle", "event_invalid", "invalid_event", "candle_invalid", "price_base_mismatch", "quality_blocked", "gap", "out_of_order", "out_of_order_interval", "late_closed_interval", "mode_mismatch"} for issue in self.processor.issues[-20:]):
                blocked.append("continuity_or_quality")
        if self.mode is not OperationMode.REPLAY and self.reconciliation_state in {"PENDING", "NEEDS_RECONCILIATION", "UNVERIFIED", "BLOCKED"}:
            blocked.append(f"reconciliation:{self.reconciliation_state.lower()}")
        blocked = sorted(set(blocked))
        enabled = not blocked
        return CoordinatorStatus(self.capture_id, self.analysis_id, self.mode.value, self.connection_state, enabled, blocked, len(self.processor.pending_simulations), len(self.processor.completed_simulations), self._last_checkpoint_events)

    def finish(self, *, status: str = "COMPLETED") -> None:
        self.checkpoint()
        self.store.finish_session(self.session_id, status=status)


__all__ = ["CoordinatorStatus", "RuntimeCoordinator", "capture_hash", "runtime_simulation_config"]
