"""Procesador incremental neutral al proveedor.

``IncrementalProcessor`` es el adaptador de tiempo real/replay entre los
registros canónicos y el núcleo determinista. No contiene una segunda regla
financiera: agrega, calcula indicadores y llama a ``TrendPullbackStrategy``.
Las simulaciones aquí sólo son virtuales y quedan en estado ``PENDING`` hasta
que el flujo aporta observaciones causales suficientes.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from collections import deque
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence

from ..core import (
    Candle,
    CandleAggregator,
    DataQuality,
    IndicatorConfig,
    IndicatorPoint,
    IndicatorSeries,
    MarketEvent,
    OperationMode,
    PriceBase,
    Signal,
    StrategyConfig,
    StrategyResult,
    Timeframe,
    TrendPullbackStrategy,
    aggregate_events,
    compute_indicators,
    parse_timeframe,
)
from ..core.aggregation import _Bucket, interval_start  # type: ignore[attr-defined]
from ..core.quality import QualityFlag, merge_quality
from .state import (
    PendingSimulation,
    PriceObservation,
    SimulationConfig,
    candle_dict,
    candle_from_dict,
    config_hash,
    evaluation_dict,
    evaluation_from_dict,
    event_dict,
    event_from_dict,
    iso,
    mode_from,
    quality_from,
    quality_label_is_usable,
    signal_dict,
    signal_from_dict,
    to_core_candle,
    to_core_event,
    utc,
)


UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class RuntimeIssue:
    code: str
    message: str
    timestamp: datetime | None = None
    record_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "timestamp": iso(self.timestamp), "record_id": self.record_id}


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Cambios observables producidos por una entrada."""

    accepted: bool
    events: tuple[MarketEvent, ...] = ()
    candles: tuple[Candle, ...] = ()
    evaluations: tuple[Any, ...] = ()
    signals: tuple[Signal, ...] = ()
    simulations: tuple[PendingSimulation, ...] = ()
    pending_simulations: tuple[PendingSimulation, ...] = ()
    issues: tuple[RuntimeIssue, ...] = ()

    @property
    def emitted_candles(self) -> tuple[Candle, ...]:
        return self.candles

    @property
    def completed_simulations(self) -> tuple[PendingSimulation, ...]:
        return tuple(item for item in self.simulations if item.status != "PENDING")


@dataclass(frozen=True, slots=True)
class ReplayResult:
    accepted_records: int
    duplicate_records: int
    rejected_records: int
    candles: int
    signals: int
    evaluations: int
    completed_simulations: int
    pending_simulations: int
    issues: tuple[RuntimeIssue, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted_records": self.accepted_records,
            "duplicate_records": self.duplicate_records,
            "rejected_records": self.rejected_records,
            "candles": self.candles,
            "signals": self.signals,
            "evaluations": self.evaluations,
            "completed_simulations": self.completed_simulations,
            "pending_simulations": self.pending_simulations,
            "issues": [issue.to_dict() for issue in self.issues],
        }


class _SimulationBook:
    """Libro causal de evaluaciones virtuales aún no vencidas."""

    def __init__(self, config: SimulationConfig) -> None:
        self.config = config
        self.pending: dict[str, PendingSimulation] = {}
        self.completed: dict[str, PendingSimulation] = {}

    def add_signal(self, signal: Signal) -> tuple[PendingSimulation, ...]:
        created: list[PendingSimulation] = []
        for horizon in self.config.horizons_seconds:
            sim_id = "sim_" + hashlib.sha256(f"{signal.signal_id}|{horizon:g}".encode()).hexdigest()[:32]
            if sim_id in self.pending or sim_id in self.completed:
                continue
            detected = signal.detected_at
            entry_due = detected + timedelta(seconds=self.config.entry_latency_seconds)
            expiry_origin = entry_due if self.config.horizon_from == "entry" else detected
            expiry = expiry_origin + timedelta(seconds=horizon)
            item = PendingSimulation(
                simulation_id=sim_id,
                signal_id=signal.signal_id,
                instrument=signal.instrument,
                direction=signal.direction,
                horizon_seconds=horizon,
                detected_at=detected,
                entry_due_at=entry_due,
                expiry_at=expiry,
                entry_rule=self.config.entry_rule,
                exit_rule=self.config.exit_rule,
                price_base=self.config.requested_base_price,
                resolution=self.config.resolution,
                quality=signal.quality.status,
            )
            self.pending[sim_id] = item
            created.append(item)
        return tuple(created)

    def _eligible(self, observation: PriceObservation) -> bool:
        if not observation.closed:
            return False
        requested = str(self.config.requested_base_price).lower().replace("trade", "traded")
        actual = str(observation.base_price).lower().replace("trade", "traded")
        if actual != requested and not (requested == "traded" and actual == "close") and not (requested == "close" and actual == "traded"):
            return False
        # Quality values from adapters are labels, not assumptions. Unknown or
        # invalid values never become a virtual price silently.
        return quality_label_is_usable(observation.quality)

    def _resolve(self, item: PendingSimulation, *, outcome: str, net_result: float | None, final: PriceObservation | None = None, reason: str | None = None) -> PendingSimulation:
        updated = replace(
            item,
            status="RESOLVED" if outcome != "INDETERMINATE" else "INDETERMINATE",
            final_at=final.available_at if final else item.final_at,
            final_price=final.price if final else item.final_price,
            outcome=outcome,
            net_result=net_result,
            quality=final.quality if final else item.quality,
            reason=reason,
            final_observation=final,
        )
        self.pending.pop(item.simulation_id, None)
        self.completed[item.simulation_id] = updated
        return updated

    def _settle_with_final(self, item: PendingSimulation, observation: PriceObservation) -> PendingSimulation:
        assert item.entry_price is not None
        delta = observation.price - item.entry_price
        if abs(delta) <= self.config.tie_tolerance:
            outcome = "TIE"
            net = self.config.tie_net - self.config.costs
        else:
            won = delta > 0 if item.direction.upper() == "UP" else delta < 0
            outcome = "WIN" if won else "LOSS"
            net = (self.config.stake * self.config.payout_net - self.config.costs) if won else (-self.config.stake * self.config.loss_amount - self.config.costs)
        return self._resolve(item, outcome=outcome, net_result=net, final=observation)

    def observe(self, observation: PriceObservation, *, watermark: datetime | None = None) -> tuple[PendingSimulation, ...]:
        """Consume one observation; first eligible entry and last pre-expiry exit."""

        watermark = watermark or observation.available_at
        completed: list[PendingSimulation] = []
        if not self._eligible(observation):
            return completed
        # Use insertion order of ids (which is deterministic by signal/horizon)
        # and never search for a favorable price.
        for sim_id, item in tuple(self.pending.items()):
            if item.entry_price is None:
                if observation.timestamp >= item.entry_due_at and observation.available_at >= item.entry_due_at:
                    age = (observation.available_at - item.entry_due_at).total_seconds()
                    if age <= self.config.max_price_age_seconds:
                        item = replace(item, entry_at=observation.available_at, entry_price=observation.price, quality=observation.quality)
                        self.pending[sim_id] = item
                    elif watermark >= item.entry_due_at + timedelta(seconds=self.config.max_price_age_seconds):
                        completed.append(self._resolve(item, outcome="INDETERMINATE", net_result=None, reason="ENTRY_PRICE_NOT_AVAILABLE_WITHIN_MAX_AGE"))
                        continue
            if item.simulation_id not in self.pending:
                continue
            if item.entry_price is None:
                if watermark >= item.expiry_at + timedelta(seconds=self.config.max_price_age_seconds):
                    completed.append(self._resolve(item, outcome="INDETERMINATE", net_result=None, reason="ENTRY_PRICE_NOT_AVAILABLE"))
                continue
            # Keep exactly the declared final-price policy. For a before rule
            # only market observations not later than expiry are admissible;
            # the after rule waits for the first observation at/after expiry.
            admissible_final = (self.config.exit_rule == "last_observation_at_or_before" and observation.timestamp <= item.expiry_at) or (self.config.exit_rule == "first_observation_at_or_after" and observation.timestamp >= item.expiry_at)
            if admissible_final and observation.available_at <= item.expiry_at + timedelta(seconds=self.config.max_price_age_seconds):
                if item.final_observation is None or (self.config.exit_rule == "last_observation_at_or_before" and observation.timestamp >= item.final_observation.timestamp) or (self.config.exit_rule == "first_observation_at_or_after" and observation.timestamp < item.final_observation.timestamp):
                    self.pending[sim_id] = replace(item, final_observation=observation, final_price=observation.price, final_at=observation.available_at)
                    item = self.pending[sim_id]
            if watermark >= item.expiry_at + timedelta(seconds=self.config.max_price_age_seconds):
                final = item.final_observation
                if final is None or final.available_at > item.expiry_at + timedelta(seconds=self.config.max_price_age_seconds) or abs((item.expiry_at - final.timestamp).total_seconds()) > self.config.max_price_age_seconds or (self.config.exit_rule == "first_observation_at_or_after" and final.timestamp < item.expiry_at):
                    completed.append(self._resolve(item, outcome="INDETERMINATE", net_result=None, reason="FINAL_PRICE_NOT_AVAILABLE_WITHIN_MAX_AGE"))
                else:
                    completed.append(self._settle_with_final(item, final))
        return tuple(completed)

    def advance(self, watermark: datetime) -> tuple[PendingSimulation, ...]:
        """Advance virtual time without inventing observations."""

        watermark = watermark.astimezone(UTC)
        completed: list[PendingSimulation] = []
        for item in tuple(self.pending.values()):
            if watermark < item.expiry_at + timedelta(seconds=self.config.max_price_age_seconds):
                continue
            if item.entry_price is None:
                completed.append(self._resolve(item, outcome="INDETERMINATE", net_result=None, reason="ENTRY_PRICE_NOT_AVAILABLE"))
            elif item.final_observation is None or item.final_observation.available_at > item.expiry_at + timedelta(seconds=self.config.max_price_age_seconds) or abs((item.expiry_at - item.final_observation.timestamp).total_seconds()) > self.config.max_price_age_seconds or (self.config.exit_rule == "first_observation_at_or_after" and item.final_observation.timestamp < item.expiry_at):
                completed.append(self._resolve(item, outcome="INDETERMINATE", net_result=None, reason="FINAL_PRICE_NOT_AVAILABLE_WITHIN_MAX_AGE"))
            else:
                completed.append(self._settle_with_final(item, item.final_observation))
        return tuple(completed)


class IncrementalProcessor:
    """Motor incremental y reanudable para eventos y velas nativas."""

    CHECKPOINT_VERSION = 1
    DEFAULT_TIMEFRAMES = ("M1", "M5", "M15")

    def __init__(
        self,
        *,
        strategy: StrategyConfig | Mapping[str, Any] | None = None,
        simulation: SimulationConfig | Mapping[str, Any] | None = None,
        timeframes: Iterable[Timeframe | str] | None = None,
        mode: OperationMode | str = OperationMode.REPLAY,
        instrument: str | None = None,
        source: str = "runtime",
        price_base: PriceBase | str = PriceBase.TRADED,
        max_candles: int | None = None,
    ) -> None:
        self.mode = mode_from(mode)
        self.instrument = instrument.strip() if instrument else None
        self.source = str(source).strip() or "runtime"
        self.price_base = price_base if isinstance(price_base, PriceBase) else PriceBase(str(price_base).lower())
        self.timeframes = tuple(parse_timeframe(item) for item in (timeframes or self.DEFAULT_TIMEFRAMES))
        if len(set(tf.name for tf in self.timeframes)) != len(self.timeframes):
            raise ValueError("timeframes no puede contener duplicados")
        if not self.timeframes:
            raise ValueError("timeframes no puede estar vacío")
        strategy_cfg = strategy if isinstance(strategy, StrategyConfig) else StrategyConfig.from_mapping(strategy)
        if strategy_cfg.mode is not self.mode:
            strategy_cfg = replace(strategy_cfg, mode=self.mode)
        self.strategy_config = strategy_cfg
        self.strategy = TrendPullbackStrategy(strategy_cfg)
        self.simulation_config = simulation if isinstance(simulation, SimulationConfig) else SimulationConfig.from_mapping(simulation)
        self.max_candles = max_candles
        if max_candles is not None and (isinstance(max_candles, bool) or max_candles <= 0):
            raise ValueError("max_candles debe ser positivo")
        self.aggregators: dict[str, CandleAggregator] = {
            tf.name: CandleAggregator(tf, instrument=self.instrument, price_base=self.price_base, source=f"{self.source}:aggregated", mode=self.mode)
            for tf in self.timeframes
        }
        self.indicator_engines: dict[str, Any] = {}
        for tf in self.timeframes:
            from ..core.indicators import IncrementalIndicatorEngine

            self.indicator_engines[tf.name] = IncrementalIndicatorEngine(strategy_cfg.indicators)
        def _container():
            return deque(maxlen=self.max_candles) if self.max_candles is not None else []

        self.candles: dict[str, Any] = {tf.name: _container() for tf in self.timeframes}
        self.indicator_points: dict[str, Any] = {tf.name: _container() for tf in self.timeframes}
        self._logical_candle_keys: dict[str, set[tuple[str, datetime]]] = {tf.name: set() for tf in self.timeframes}
        self._candle_by_key: dict[str, dict[tuple[str, datetime], Candle]] = {tf.name: {} for tf in self.timeframes}
        self._resample_buffers: dict[str, dict[datetime, dict[datetime, Candle]]] = {tf.name: {} for tf in self.timeframes}
        self._candle_ids: set[str] = set()
        self._processed_candle_starts: dict[str, set[datetime]] = {tf.name: set() for tf in self.timeframes}
        self._point_index_by_start: dict[str, dict[datetime, int]] = {tf.name: {} for tf in self.timeframes}
        self._seen_event_ids: set[str] = set()
        self._events: dict[str, MarketEvent] = {}
        self.evaluations: list[Any] = []
        self.signals: list[Signal] = []
        self._decision_ids: set[str] = set()
        self._signal_ids: set[str] = set()
        self.episodes: dict[str, Any] = {}
        self.context: dict[str, Any] | None = None
        self._simulation_book = _SimulationBook(self.simulation_config)
        self.completed_simulations: list[PendingSimulation] = []
        self.issues: list[RuntimeIssue] = []
        self.last_event_time: datetime | None = None
        self.last_event_id: str | None = None
        self.last_available_at: datetime | None = None
        self.events_processed = 0
        self.candles_processed = 0
        self._native_candle_keys: set[tuple[str, datetime]] = set()
        self._derived_candle_keys: set[tuple[str, datetime]] = set()
        # Telemetry for the incremental strategy seam. Indicators are updated
        # once per accepted candle; strategy calls receive only this bounded
        # causal window, never the entire historical series.
        self.strategy_evaluations = 0
        self.last_strategy_window_sizes: dict[str, int] = {}
        self.indicator_updates: dict[str, int] = {tf.name: 0 for tf in self.timeframes}
        self._strategy_dirty = True
        self.strategy_skipped = 0

    @property
    def pending_simulations(self) -> tuple[PendingSimulation, ...]:
        return tuple(self._simulation_book.pending.values())

    @property
    def status(self) -> dict[str, Any]:
        return {
            "mode": self.mode.value,
            "instrument": self.instrument,
            "source": self.source,
            "price_base": self.price_base.value,
            "max_candles": self.max_candles,
            "timeframes": [tf.name for tf in self.timeframes],
            "last_event_time": iso(self.last_event_time),
            "last_event_id": self.last_event_id,
            "last_available_at": iso(self.last_available_at),
            "events_processed": self.events_processed,
            "candles_processed": self.candles_processed,
            "candles": {name: len(values) for name, values in self.candles.items()},
            "warmup_pending": {name: self._warmup_pending(name) for name in self.candles},
            "signals": len(self.signals),
            "evaluations": len(self.evaluations),
            "pending_simulations": len(self.pending_simulations),
            "completed_simulations": len(self.completed_simulations),
            "errors": len(self.issues),
            "strategy_evaluations": self.strategy_evaluations,
            "strategy_skipped": self.strategy_skipped,
            "last_strategy_window_sizes": dict(self.last_strategy_window_sizes),
            "indicator_updates": dict(self.indicator_updates),
        }

    def _warmup_pending(self, timeframe: str) -> int:
        points = self.indicator_points.get(timeframe, ())
        if points and points[-1].ready:
            return 0
        required = max(self.strategy_config.indicators.ema_slow, self.strategy_config.indicators.rsi_period + 1, self.strategy_config.indicators.atr_period)
        return max(1, required - len(points))

    def _issue(self, code: str, message: str, *, record: Any | None = None, timestamp: datetime | None = None) -> RuntimeIssue:
        record_id = _attr_id(record)
        issue = RuntimeIssue(code, message, timestamp or _attr_time(record), record_id)
        self._append_issue(issue)
        return issue

    def _ensure_instrument(self, value: str) -> bool:
        if self.instrument is None:
            self.instrument = value
            for aggregator in self.aggregators.values():
                aggregator.instrument = value
            return True
        return value == self.instrument

    def _logical_key(self, candle: Candle) -> tuple[str, datetime]:
        return candle.instrument, candle.start

    def _is_derived(self, candle: Candle) -> bool:
        return str(candle.origin).lower() in {"aggregated", "derived", "resampled_ohlc", "runtime_aggregate"} or "aggregated" in str(candle.source).lower()

    def _rebuild_indicator_engine(self, name: str) -> None:
        """Rebuild only after a late native replacement, never per event."""
        from ..core.indicators import IncrementalIndicatorEngine

        engine = IncrementalIndicatorEngine(self.strategy_config.indicators)
        points = []
        for candle in self.candles[name]:
            points.append(engine.update(candle))
        self.indicator_engines[name] = engine
        if self.max_candles is None:
            self.indicator_points[name] = points
        else:
            self.indicator_points[name] = deque(points, maxlen=self.max_candles)

    def _resample_from_candle(self, base_candle: Candle) -> list[ProcessResult]:
        """Build compatible higher OHLC bars; never creates ticks or prices."""
        results: list[ProcessResult] = []
        base_tf = base_candle.timeframe
        for target in sorted(self.timeframes, key=lambda item: item.seconds, reverse=True):
            if target.seconds <= base_tf.seconds or target.seconds % base_tf.seconds:
                continue
            bucket_start = interval_start(base_candle.start, target)
            # Cuando ya existe una vela nativa para ese intervalo, ésta tiene
            # precedencia y no se construye una derivada paralela ni se generan
            # falsos duplicados durante el bootstrap multitemporal.
            existing_target = self._candle_by_key[target.name].get((base_candle.instrument, bucket_start))
            if existing_target is not None and not self._is_derived(existing_target):
                continue
            buckets = self._resample_buffers[target.name]
            bucket = buckets.setdefault(bucket_start, {})
            bucket[base_candle.start] = base_candle
            ratio = target.seconds // base_tf.seconds
            expected = [bucket_start + base_tf.delta * offset for offset in range(ratio)]
            if not all(start in bucket for start in expected):
                # A bounded buffer prevents a long data interruption from
                # becoming an unbounded in-memory historical cache.
                if self.max_candles is not None and len(buckets) > self.max_candles:
                    for oldest in sorted(buckets)[:-self.max_candles]:
                        buckets.pop(oldest, None)
                continue
            group = [bucket[start] for start in expected]
            if any(not item.closed or not item.quality.valid for item in group):
                buckets.pop(bucket_start, None)
                continue
            derived = Candle(
                instrument=group[0].instrument,
                timeframe=target,
                start=bucket_start,
                end=bucket_start + target.delta,
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
                volume=sum(item.volume for item in group),
                event_count=sum(item.event_count for item in group),
                source=f"{self.source}:resampled",
                mode=self.mode,
                price_base=self.price_base,
                closed=True,
                available_at=max(item.available_at or item.end for item in group),
                received_at=max(item.received_at or item.end for item in group),
                quality=merge_quality(*(item.quality for item in group), source=f"{self.source}:resampled"),
                origin="resampled_ohlc",
                metadata={"built_from": "closed_ohlc_bars", "base_timeframe": base_tf.name, "ratio": ratio, "no_ticks_invented": True},
            )
            buckets.pop(bucket_start, None)
            results.append(self._accept_candle(derived, native=False, evaluate_strategy=False))
        return results

    def _append_bounded(self, name: str, candle: Candle, point: IndicatorPoint) -> None:
        candles = self.candles[name]
        if self.max_candles is None:
            candles.append(candle)
            self.indicator_points[name].append(point)
        else:
            # deque descarta el más antiguo de forma atómica; liberar sólo el
            # índice lógico evita crecimiento de las estructuras de consulta.
            if len(candles) >= self.max_candles:
                evicted = candles[0]
                logical = self._logical_key(evicted)
                self._logical_candle_keys[name].discard(logical)
                self._candle_by_key[name].pop(logical, None)
                self._native_candle_keys.discard(logical)
                self._derived_candle_keys.discard(logical)
                if evicted.candle_id not in {item.candle_id for item in list(candles)[1:]}:
                    self._candle_ids.discard(evicted.candle_id)
                self._point_index_by_start[name].pop(evicted.start, None)
            candles.append(candle)
            self.indicator_points[name].append(point)
            self._point_index_by_start[name] = {item.start: index for index, item in enumerate(self.indicator_points[name])}
        self._candle_by_key[name][self._logical_key(candle)] = candle

    def _append_evaluation(self, evaluation: Any) -> None:
        if self.max_candles is not None and len(self.evaluations) >= self.max_candles * max(1, len(self.timeframes)):
            del self.evaluations[0]
        self.evaluations.append(evaluation)

    def _append_issue(self, issue: RuntimeIssue) -> None:
        if self.max_candles is not None and len(self.issues) >= self.max_candles * max(1, len(self.timeframes)):
            del self.issues[0]
        self.issues.append(issue)

    def _accept_candle(self, candle: Candle, *, native: bool, evaluate_strategy: bool = True) -> ProcessResult:
        tf_name = candle.timeframe.name
        if tf_name not in self.candles:
            return ProcessResult(False, issues=(self._issue("timeframe_not_configured", f"Temporalidad no configurada: {tf_name}", record=candle),))
        if not self._ensure_instrument(candle.instrument):
            return ProcessResult(False, issues=(self._issue("instrument_mismatch", f"Se esperaba {self.instrument}, llegó {candle.instrument}", record=candle),))
        if candle.price_base is not self.price_base:
            return ProcessResult(False, issues=(self._issue("price_base_mismatch", f"Se esperaba {self.price_base.value}, llegó {candle.price_base.value}", record=candle),))
        quality_issue: RuntimeIssue | None = None
        if not candle.quality.valid:
            quality_issue = self._issue("quality_blocked", f"Calidad no admisible en {candle.timeframe.name}: {candle.quality.status}", record=candle)
        logical_key = self._logical_key(candle)
        existing = self._candle_by_key[tf_name].get(logical_key)
        if existing is not None:
            # Native provider OHLC has precedence over a derived OHLC bar. If
            # it arrives after a derived bar, replace the effective value for
            # future calculations and rebuild that timeframe once; prior
            # decisions remain immutable and are not rewritten.
            existing_revision = int((existing.metadata.get("revision", 0) if hasattr(existing.metadata, "get") else 0) or 0)
            new_revision = int((candle.metadata.get("revision", 0) if hasattr(candle.metadata, "get") else 0) or 0)
            if native and not self._is_derived(candle) and new_revision > existing_revision:
                rows = self.candles[tf_name]
                for index, previous in enumerate(rows):
                    if self._logical_key(previous) == logical_key:
                        rows[index] = candle
                        break
                self._candle_ids.add(candle.candle_id)
                self._native_candle_keys.add(logical_key)
                self._derived_candle_keys.discard(logical_key)
                self._candle_by_key[tf_name][logical_key] = candle
                self._rebuild_indicator_engine(tf_name)
                self.indicator_updates[tf_name] = self.indicator_updates.get(tf_name, 0) + 1
                evaluations: tuple[Any, ...] = ()
                signals: tuple[Signal, ...] = ()
                if evaluate_strategy and tf_name == self.strategy_config.trigger_timeframe.name and candle.closed:
                    evaluations, signals = self._evaluate_strategy()
                issue = self._issue("candle_revision", f"Revisión nativa {new_revision} reemplazó {existing_revision} en {tf_name}: {candle.start.isoformat()}", record=candle)
                return ProcessResult(True, candles=(candle,), evaluations=evaluations, signals=signals, pending_simulations=self.pending_simulations, issues=(issue,))
            if native and self._is_derived(existing) and not self._is_derived(candle):
                rows = self.candles[tf_name]
                for index, previous in enumerate(rows):
                    if self._logical_key(previous) == logical_key:
                        rows[index] = candle
                        break
                self._candle_ids.add(candle.candle_id)
                self._native_candle_keys.add(logical_key)
                self._derived_candle_keys.discard(logical_key)
                self._candle_by_key[tf_name][logical_key] = candle
                self._rebuild_indicator_engine(tf_name)
                evaluations: tuple[Any, ...] = ()
                signals: tuple[Signal, ...] = ()
                if evaluate_strategy and tf_name == self.strategy_config.trigger_timeframe.name and candle.closed:
                    evaluations, signals = self._evaluate_strategy()
                issue = self._issue("native_preferred", f"Vela nativa reemplazó derivada en {tf_name}: {candle.start.isoformat()}", record=candle)
                return ProcessResult(True, candles=(candle,), evaluations=evaluations, signals=signals, pending_simulations=self.pending_simulations, issues=(issue,))
            issue = self._issue("duplicate_candle", f"Vela duplicada o revisión ya observada: {candle.candle_id}", record=candle)
            return ProcessResult(False, issues=(issue,))
        if candle.candle_id in self._candle_ids:
            issue = self._issue("duplicate_candle", f"Vela duplicada o revisión ya observada: {candle.candle_id}", record=candle)
            return ProcessResult(False, issues=(issue,))
        if self.candles[tf_name] and candle.start <= self.candles[tf_name][-1].start:
            issue = self._issue("out_of_order_candle", f"Vela fuera de orden en {tf_name}: {candle.start.isoformat()}", record=candle)
            return ProcessResult(False, issues=(issue,))
        self._logical_candle_keys[tf_name].add(logical_key)
        self._candle_ids.add(candle.candle_id)
        (self._native_candle_keys if native else self._derived_candle_keys).add(logical_key)
        self.candles_processed += 1
        point = self.indicator_engines[tf_name].update(candle)
        self._append_bounded(tf_name, candle, point)
        self._point_index_by_start[tf_name][candle.start] = len(self.indicator_points[tf_name]) - 1
        self.indicator_updates[tf_name] = self.indicator_updates.get(tf_name, 0) + 1
        if tf_name in {self.strategy_config.context_timeframe.name, self.strategy_config.preparation_timeframe.name}:
            self._strategy_dirty = True
        evaluations: tuple[Any, ...] = ()
        signals: tuple[Signal, ...] = ()
        if evaluate_strategy and tf_name == self.strategy_config.trigger_timeframe.name and candle.closed:
            evaluations, signals = self._evaluate_strategy()
        return ProcessResult(True, candles=(candle,), evaluations=evaluations, signals=signals, pending_simulations=self.pending_simulations, issues=(quality_issue,) if quality_issue is not None else ())

    def _strategy_window_indices(self, name: str, as_of: datetime, *, lookback: int) -> list[int]:
        points = self.indicator_points.get(name, ())
        if not points:
            return []
        # Runtime input is monotonic, so inspect only the short future tail
        # rather than scanning the complete historical series per trigger.
        latest = len(points) - 1
        while latest >= 0 and point_available(points[latest]) is not None and point_available(points[latest]) > as_of:
            latest -= 1
        if latest < 0:
            return []
        first = max(0, latest - lookback)
        # Sólo el episodio más reciente puede afectar el disparador actual;
        # conservar todos los episodios usados volvería a ampliar la ventana
        # hasta el inicio del histórico.
        by_start = self._point_index_by_start.get(name, {})
        current = [episode for episode in self.episodes.values() if not episode.invalidated and episode.registered_at <= as_of and episode.expires_at >= as_of]
        if current:
            episode = max(current, key=lambda item: item.registered_at)
            index = by_start.get(episode.preparation_start)
            if index is not None and index <= latest:
                first = min(first, max(0, index - lookback))
        return list(range(first, latest + 1))

    def _bounded_strategy_streams(self, as_of: datetime) -> dict[str, IndicatorSeries]:
        context_tf = self.strategy_config.context_timeframe.name
        preparation_tf = self.strategy_config.preparation_timeframe.name
        trigger_tf = self.strategy_config.trigger_timeframe.name
        requests = {
            context_tf: self.strategy_config.context_lookback,
            preparation_tf: max(self.strategy_config.preparation_lookback, self.strategy_config.preparation_ttl_bars) + self.strategy_config.preparation_lookback,
            trigger_tf: 1,
        }
        streams: dict[str, IndicatorSeries] = {}
        self.last_strategy_window_sizes = {}
        for name, lookback in requests.items():
            if name not in self.indicator_engines or not self.indicator_points.get(name):
                continue
            indices = self._strategy_window_indices(name, as_of, lookback=lookback)
            if not indices:
                continue
            engine = self.indicator_engines[name]
            points = tuple(self.indicator_points[name][index] for index in indices)
            streams[name] = IndicatorSeries(engine.series.timeframe, engine.series.instrument, self.strategy_config.indicators, points)
            self.last_strategy_window_sizes[name] = len(points)
        return streams

    def _strategy_warmup_ready(self) -> bool:
        context = self.indicator_points.get(self.strategy_config.context_timeframe.name, ())
        preparation = self.indicator_points.get(self.strategy_config.preparation_timeframe.name, ())
        trigger = self.indicator_points.get(self.strategy_config.trigger_timeframe.name, ())
        if len(context) <= self.strategy_config.context_lookback or len(preparation) <= self.strategy_config.preparation_lookback or len(trigger) < 2:
            return False
        context_latest = context[-1]
        context_lag = context[-1 - self.strategy_config.context_lookback]
        prep_latest = preparation[-1]
        prep_lag = preparation[-1 - self.strategy_config.preparation_lookback]
        trigger_latest = trigger[-1]
        trigger_previous = trigger[-2]
        relevant = (context_latest, context_lag, prep_latest, prep_lag, trigger_previous, trigger_latest)
        return all(point.ready and point.closed for point in relevant)

    def _trigger_candidate(self) -> bool:
        trigger = self.indicator_points.get(self.strategy_config.trigger_timeframe.name, ())
        if len(trigger) < 2:
            return False
        current, previous = trigger[-1], trigger[-2]
        if not current.closed or not previous.closed:
            return False
        if any(value is None for value in (current.close, current.ema_fast, previous.close, previous.ema_fast)):
            return False
        return (previous.close <= previous.ema_fast < current.close) or (previous.close >= previous.ema_fast > current.close)

    def _evaluate_strategy(self) -> tuple[tuple[Any, ...], tuple[Signal, ...]]:
        trigger_points = self.indicator_points.get(self.strategy_config.trigger_timeframe.name, ())
        if not trigger_points:
            return (), ()
        # Before warmup, and on ordinary M1 bars that cannot cross EMA20, the
        # state is unchanged. A dirty context/preparation update still forces
        # one evaluation when the prerequisites become ready.
        if not self._strategy_warmup_ready() or (not self._strategy_dirty and not self._trigger_candidate()):
            self.strategy_skipped += 1
            return (), ()
        self._strategy_dirty = False
        latest_trigger = trigger_points[-1]
        as_of = point_available(latest_trigger) or latest_trigger.end
        streams = self._bounded_strategy_streams(as_of)
        self.strategy_evaluations += 1
        result = self.strategy.evaluate(streams)
        new_evaluations: list[Any] = []
        for evaluation in result.evaluations:
            decision_id = _decision_id(evaluation)
            if decision_id in self._decision_ids:
                continue
            self._decision_ids.add(decision_id)
            self._append_evaluation(evaluation)
            new_evaluations.append(evaluation)
        # Conservar primero los episodios que el nuevo resultado reobserva;
        # después las señales marcan ``used`` sin que un snapshot posterior lo
        # vuelva a poner en falso.
        for episode in result.episodes:
            previous = self.episodes.get(episode.episode_id)
            self.episodes[episode.episode_id] = replace(episode, used=True) if previous is not None and previous.used else episode
        new_signals: list[Signal] = []
        for signal in result.signals:
            if signal.signal_id in self._signal_ids:
                continue
            self._signal_ids.add(signal.signal_id)
            self.signals.append(signal)
            new_signals.append(signal)
            self._simulation_book.add_signal(signal)
            if signal.episode_id in self.episodes:
                self.episodes[signal.episode_id] = replace(self.episodes[signal.episode_id], used=True)
        # A compact current-context view for status/UI; detailed decisions stay
        # in ``evaluations``.
        latest = next((item for item in reversed(result.evaluations) if item.stage == "trigger" and item.values.get("context") is not None), None)
        if latest is not None:
            self.context = {"direction": latest.direction, "values": latest.values.get("context"), "timestamp": latest.timestamp}
        return tuple(new_evaluations), tuple(new_signals)

    def register_signal(self, signal: Signal) -> tuple[PendingSimulation, ...]:
        """Registra una señal ya validada y crea sus horizontes virtuales."""
        if not isinstance(signal, Signal):
            raise TypeError("register_signal requiere core.Signal")
        aliases = {"UP": "UP", "LONG": "UP", "BUY": "UP", "BULL": "UP", "DOWN": "DOWN", "SHORT": "DOWN", "SELL": "DOWN", "BEAR": "DOWN"}
        direction = aliases.get(str(signal.direction).upper())
        if direction is None:
            raise ValueError(f"dirección desconocida: {signal.direction!r}")
        if direction != signal.direction:
            signal = replace(signal, direction=direction)
        if not self._ensure_instrument(signal.instrument):
            raise ValueError(f"Se esperaba {self.instrument}, llegó {signal.instrument}")
        if signal.signal_id in self._signal_ids:
            return tuple(item for item in self.pending_simulations if item.signal_id == signal.signal_id)
        self._signal_ids.add(signal.signal_id)
        self.signals.append(signal)
        if signal.episode_id in self.episodes:
            self.episodes[signal.episode_id] = replace(self.episodes[signal.episode_id], used=True)
        return self._simulation_book.add_signal(signal)

    def _observe_event(self, event: MarketEvent) -> tuple[PendingSimulation, ...]:
        selected = event.selected_price
        if selected is None or not math.isfinite(float(selected)):
            return ()
        observation = PriceObservation(
            timestamp=event.event_time,
            available_at=max(event.effective_available_at, event.event_time),
            price=float(selected),
            base_price=event.price_base.value,
            source=event.source,
            resolution="event",
            closed=True,
            quality=event.quality.status,
        )
        return self._simulation_book.observe(observation, watermark=observation.available_at)

    def process_event(self, record: Any, *, evaluate_strategy: bool = True) -> ProcessResult:
        """Consume one event; aggregate all temporalities in close order.

        ``evaluate_strategy=False`` is used only for bootstrap/recovery history:
        it still warms aggregators, indicators and the observation book but does
        not emit historical decisions or signals.
        """

        try:
            event = to_core_event(record, mode=self.mode)
        except Exception as exc:
            issue = self._issue("event_invalid", str(exc), record=record)
            return ProcessResult(False, issues=(issue,))
        if not self._ensure_instrument(event.instrument):
            issue = self._issue("instrument_mismatch", f"Se esperaba {self.instrument}, llegó {event.instrument}", record=event)
            return ProcessResult(False, events=(event,), issues=(issue,))
        if event.mode is not self.mode:
            issue = self._issue("mode_mismatch", f"Se esperaba modo {self.mode.value}, llegó {event.mode.value}", record=event)
            return ProcessResult(False, events=(event,), issues=(issue,))
        if event.event_id in self._seen_event_ids:
            issue = self._issue("duplicate_event", f"Evento repetido: {event.event_id}", record=event)
            return ProcessResult(False, events=(event,), issues=(issue,))
        if self.last_event_time is not None and event.event_time < self.last_event_time:
            issue = self._issue("out_of_order_event", f"Evento fuera de orden: {event.event_time.isoformat()}", record=event)
            return ProcessResult(False, events=(event,), issues=(issue,))
        self._seen_event_ids.add(event.event_id)
        self._events[event.event_id] = event
        if self.max_candles is not None and len(self._events) > self.max_candles * max(1, len(self.timeframes)):
            oldest_id = next(iter(self._events))
            self._events.pop(oldest_id, None)
            self._seen_event_ids.discard(oldest_id)
        self.events_processed += 1
        self.last_event_id = event.event_id
        self.last_event_time = event.event_time if self.last_event_time is None else max(self.last_event_time, event.event_time)
        self.last_available_at = event.effective_available_at if self.last_available_at is None else max(self.last_available_at, event.effective_available_at)
        emitted: list[Candle] = []
        evaluations: list[Any] = []
        signals: list[Signal] = []
        issues: list[RuntimeIssue] = []
        aggregation_accepted = True
        aggregation_issue_keys: set[tuple[str, str, str | None]] = set()
        # Contexto primero (M15 > M5 > M1) cuando varias temporalidades
        # cierran al recibir un evento de frontera.
        for tf in sorted(self.timeframes, key=lambda item: item.seconds, reverse=True):
            result = self.aggregators[tf.name].add(event)
            if not result.accepted:
                aggregation_accepted = False
            for item in result.issues:
                issue_key = (item.code, item.message, item.record_id)
                if issue_key in aggregation_issue_keys:
                    continue
                aggregation_issue_keys.add(issue_key)
                issue = RuntimeIssue(item.code, item.message, item.timestamp, item.record_id)
                issues.append(issue)
                self._append_issue(issue)
            for candle in result.emitted:
                accepted = self._accept_candle(candle, native=False, evaluate_strategy=evaluate_strategy)
                if accepted.accepted:
                    emitted.extend(accepted.candles)
                    evaluations.extend(accepted.evaluations)
                    signals.extend(accepted.signals)
                issues.extend(accepted.issues)
        # Un evento que no pudo entrar de forma coherente en todas las
        # temporalidades no puede resolver simulaciones ni contarse como
        # entrada aceptada, aunque se conserve como evidencia capturada.
        completed = list(self._observe_event(event)) if aggregation_accepted else []
        self.completed_simulations.extend(completed)
        if self.max_candles is not None:
            del self.completed_simulations[:-self.max_candles]
        return ProcessResult(aggregation_accepted, events=(event,), candles=tuple(emitted), evaluations=tuple(evaluations), signals=tuple(signals), simulations=tuple(completed), pending_simulations=self.pending_simulations, issues=tuple(issues))

    # Common aliases used by replay/observation coordinators.
    update = process_event
    on_event = process_event

    def process_bar(self, record: Any, *, evaluate_strategy: bool = True) -> ProcessResult:
        """Consume one native candle and derive compatible higher OHLC bars.

        ``evaluate_strategy=False`` is reserved for bootstrap/recovery history:
        it warms indicators and continuity but cannot emit historical signals.
        """

        try:
            candle = to_core_candle(record, mode=self.mode)
        except Exception as exc:
            issue = self._issue("candle_invalid", str(exc), record=record)
            return ProcessResult(False, issues=(issue,))
        if candle.mode is not self.mode:
            issue = self._issue("mode_mismatch", f"Se esperaba modo {self.mode.value}, llegó {candle.mode.value}", record=candle)
            return ProcessResult(False, issues=(issue,))
        accepted = self._accept_candle(candle, native=True, evaluate_strategy=False)
        if not accepted.accepted:
            return accepted
        all_candles = list(accepted.candles)
        all_evaluations = list(accepted.evaluations)
        all_signals = list(accepted.signals)
        all_issues = list(accepted.issues)
        # Higher temporalities are produced only from complete lower OHLC bars;
        # unlike a trade stream, no intrabar ticks are reconstructed.
        for child in self._resample_from_candle(candle):
            all_issues.extend(child.issues)
            if child.accepted:
                all_candles.extend(child.candles)
                all_evaluations.extend(child.evaluations)
                all_signals.extend(child.signals)
        self.last_event_id = candle.candle_id
        self.last_event_time = candle.end if self.last_event_time is None else max(self.last_event_time, candle.end)
        available = candle.available_at or candle.end
        self.last_available_at = available if self.last_available_at is None else max(self.last_available_at, available)
        # Evaluate once, after all same-watermark context/preparation bars have
        # been installed. This is the same order used by event aggregation.
        if evaluate_strategy and candle.timeframe.name == self.strategy_config.trigger_timeframe.name and candle.closed:
            evaluations, signals = self._evaluate_strategy()
            all_evaluations.extend(evaluations)
            all_signals.extend(signals)
        obs = PriceObservation(candle.end, max(available, candle.end), candle.close, candle.price_base.value, candle.source, candle.timeframe.name, candle.closed, candle.quality.status)
        completed = self._simulation_book.observe(obs, watermark=obs.available_at)
        self.completed_simulations.extend(completed)
        if self.max_candles is not None:
            del self.completed_simulations[:-self.max_candles]
        return ProcessResult(True, candles=tuple(all_candles), evaluations=tuple(all_evaluations), signals=tuple(all_signals), simulations=tuple(completed), pending_simulations=self.pending_simulations, issues=tuple(all_issues))

    # Explicit aliases keep the adapter seam readable for providers and UI.
    process_candle = process_bar
    ingest_event = process_event
    ingest_bar = process_bar
    ingest_candle = process_bar
    feed_event = process_event
    feed_bar = process_bar

    def finalize(self, watermark: datetime | None = None) -> ProcessResult:
        """Close active buckets at an explicit watermark; no empty candle is made."""

        if watermark is None:
            watermark = self.last_available_at or self.last_event_time
        if watermark is None:
            return ProcessResult(True, pending_simulations=self.pending_simulations)
        watermark = utc(watermark)
        assert watermark is not None
        emitted: list[Candle] = []
        evaluations: list[Any] = []
        signals: list[Signal] = []
        issues: list[RuntimeIssue] = []
        for tf in sorted(self.timeframes, key=lambda item: item.seconds, reverse=True):
            result = self.aggregators[tf.name].close_until(watermark)
            for candle in result.emitted:
                accepted = self._accept_candle(candle, native=False)
                if accepted.accepted:
                    emitted.extend(accepted.candles)
                    evaluations.extend(accepted.evaluations)
                    signals.extend(accepted.signals)
                issues.extend(accepted.issues)
            for item in result.issues:
                issue = RuntimeIssue(item.code, item.message, item.timestamp, item.record_id)
                issues.append(issue)
                self._append_issue(issue)
        completed = list(self._simulation_book.advance(watermark))
        self.completed_simulations.extend(completed)
        if self.max_candles is not None:
            del self.completed_simulations[:-self.max_candles]
        return ProcessResult(True, candles=tuple(emitted), evaluations=tuple(evaluations), signals=tuple(signals), simulations=tuple(completed), pending_simulations=self.pending_simulations, issues=tuple(issues))

    def replay(self, records: Iterable[Any], *, sort: bool = True) -> ReplayResult:
        """Replay mixed events/native bars once, with deterministic tie ordering."""

        materialized = list(records)
        if sort:
            materialized = sorted(enumerate(materialized), key=lambda item: _replay_key(item[1], item[0]))
            materialized = [item[1] for item in materialized]
        accepted = duplicate = rejected = candles = signals = evaluations = completed = 0
        issues: list[RuntimeIssue] = []
        for record in materialized:
            is_bar = _looks_like_candle(record)
            result = self.process_bar(record) if is_bar else self.process_event(record)
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
        final = self.finalize()
        candles += len(final.candles); signals += len(final.signals); evaluations += len(final.evaluations); completed += len(final.completed_simulations); issues.extend(final.issues)
        return ReplayResult(accepted, duplicate, rejected, candles, signals, evaluations, completed, len(self.pending_simulations), tuple(issues))

    def checkpoint(self) -> dict[str, Any]:
        """Return a JSON-compatible snapshot; no pickle or live objects."""

        aggregator_state: dict[str, Any] = {}
        for name, aggregator in self.aggregators.items():
            bucket = getattr(aggregator, "_bucket", None)
            aggregator_state[name] = {
                "closed_through": iso(getattr(aggregator, "_closed_through", None)),
                "seen_event_ids": sorted(getattr(aggregator, "_seen_event_ids", set())),
                "last_event_time": iso(getattr(aggregator, "_last_event_time", None)),
                "bucket": {
                    "start": iso(bucket.start),
                    "end": iso(bucket.end),
                    "events": [event_dict(event) for event in bucket.events],
                } if bucket is not None else None,
            }
        strategy_data = {
            "name": self.strategy_config.name,
            "context_timeframe": self.strategy_config.context_timeframe.name,
            "preparation_timeframe": self.strategy_config.preparation_timeframe.name,
            "trigger_timeframe": self.strategy_config.trigger_timeframe.name,
            "context_lookback": self.strategy_config.context_lookback,
            "preparation_lookback": self.strategy_config.preparation_lookback,
            "max_distance_atr": self.strategy_config.max_distance_atr,
            "rsi_threshold": self.strategy_config.rsi_threshold,
            "preparation_ttl_bars": self.strategy_config.preparation_ttl_bars,
            "require_closed": self.strategy_config.require_closed,
            "one_signal_per_episode": self.strategy_config.one_signal_per_episode,
            "optional_filters": dict(self.strategy_config.optional_filters),
            "mode": self.mode.value,
            "indicators": {
                "ema_fast": self.strategy_config.indicators.ema_fast,
                "ema_slow": self.strategy_config.indicators.ema_slow,
                "rsi_period": self.strategy_config.indicators.rsi_period,
                "atr_period": self.strategy_config.indicators.atr_period,
                "wilder": self.strategy_config.indicators.wilder,
            },
        }
        return {
            "checkpoint_version": self.CHECKPOINT_VERSION,
            "config_hash": config_hash(self.strategy_config, self.simulation_config, self.timeframes, self.mode, self.price_base),
            "mode": self.mode.value,
            "instrument": self.instrument,
            "source": self.source,
            "price_base": self.price_base.value,
            "timeframes": [tf.name for tf in self.timeframes],
            "strategy": strategy_data,
            "simulation": self.simulation_config.to_dict(),
            "max_candles": self.max_candles,
            "seen_event_ids": sorted(self._seen_event_ids),
            "events": [event_dict(event) for event in self._events.values()],
            "aggregators": aggregator_state,
            "resample_buffers": {
                name: {iso(bucket_start): {iso(base_start): candle_dict(candle) for base_start, candle in bucket.items()} for bucket_start, bucket in buckets.items()}
                for name, buckets in self._resample_buffers.items()
            },
            "candles": {name: [candle_dict(candle) for candle in values] for name, values in self.candles.items()},
            "indicator_points": {name: [_indicator_point_dict(point) for point in values] for name, values in self.indicator_points.items()},
            "indicator_engines": {name: _engine_state(engine) for name, engine in self.indicator_engines.items()},
            "evaluations": [evaluation_dict(item) for item in self.evaluations],
            "signals": [signal_dict(item) for item in self.signals],
            "episodes": [_episode_dict(item) for item in self.episodes.values()],
            "context": {**self.context, "timestamp": iso(self.context.get("timestamp"))} if self.context else None,
            "pending_simulations": [item.to_dict() for item in self.pending_simulations],
            "completed_simulations": [item.to_dict() for item in self.completed_simulations],
            "last_event_time": iso(self.last_event_time),
            "last_available_at": iso(self.last_available_at),
            "events_processed": self.events_processed,
            "candles_processed": self.candles_processed,
            "strategy_evaluations": self.strategy_evaluations,
            "strategy_skipped": self.strategy_skipped,
            "strategy_dirty": self._strategy_dirty,
            "last_strategy_window_sizes": dict(self.last_strategy_window_sizes),
            "indicator_updates": dict(self.indicator_updates),
            "issues": [issue.to_dict() for issue in self.issues],
        }

    def checkpoint_json(self) -> str:
        return json.dumps(self.checkpoint(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    snapshot = checkpoint
    snapshot_json = checkpoint_json

    @classmethod
    def from_checkpoint(cls, snapshot: Mapping[str, Any] | str, *, allow_config_mismatch: bool = False) -> "IncrementalProcessor":
        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot)
        if not isinstance(snapshot, Mapping):
            raise TypeError("snapshot debe ser mapping o JSON")
        if int(snapshot.get("checkpoint_version", 0)) != cls.CHECKPOINT_VERSION:
            raise ValueError("versión de checkpoint no soportada")
        strategy_raw = dict(snapshot.get("strategy", {}))
        simulation_raw = dict(snapshot.get("simulation", {}))
        processor = cls(
            strategy=strategy_raw,
            simulation=simulation_raw,
            timeframes=snapshot.get("timeframes", cls.DEFAULT_TIMEFRAMES),
            mode=snapshot.get("mode", "REPLAY"),
            instrument=snapshot.get("instrument"),
            source=snapshot.get("source", "runtime"),
            price_base=snapshot.get("price_base", "traded"),
            max_candles=snapshot.get("max_candles"),
        )
        expected_hash = config_hash(processor.strategy_config, processor.simulation_config, processor.timeframes, processor.mode, processor.price_base)
        if not allow_config_mismatch and snapshot.get("config_hash") != expected_hash:
            raise ValueError("config_hash del checkpoint no coincide")
        processor._seen_event_ids = set(str(item) for item in snapshot.get("seen_event_ids", ()))
        processor._events = {event.event_id: event for event in (event_from_dict(item) for item in snapshot.get("events", ())) }
        for name, rows in dict(snapshot.get("candles", {})).items():
            if name not in processor.candles:
                continue
            for raw in rows:
                candle = candle_from_dict(raw)
                # Reconstruct the effective bounded history; the indicator
                # state below is restored separately, so a max_candles
                # checkpoint does not change EMA/RSI/ATR due to warmup loss.
                processor.candles[name].append(candle)
                logical = processor._logical_key(candle)
                processor._logical_candle_keys[name].add(logical)
                processor._candle_by_key[name][logical] = candle
                processor._candle_ids.add(candle.candle_id)
                processor._processed_candle_starts[name].add(candle.start)
                processor._point_index_by_start[name][candle.start] = len(processor.candles[name]) - 1
                (processor._native_candle_keys if not processor._is_derived(candle) else processor._derived_candle_keys).add(logical)
        raw_points = snapshot.get("indicator_points")
        if isinstance(raw_points, Mapping):
            for name, rows in raw_points.items():
                if name not in processor.indicator_points:
                    continue
                processor.indicator_points[name].clear()
                for raw in rows:
                    processor.indicator_points[name].append(_indicator_point_from_dict(raw))
        else:
            # Backward-compatible fallback for version-1 checkpoints created
            # before engine state was explicit.
            for name, values in processor.candles.items():
                for candle in values:
                    point = processor.indicator_engines[name].update(candle)
                    processor.indicator_points[name].append(point)
        for name, raw in dict(snapshot.get("indicator_engines", {})).items():
            if name in processor.indicator_engines and isinstance(raw, Mapping):
                _restore_engine_state(processor.indicator_engines[name], raw)
        processor.candles_processed = int(snapshot.get("candles_processed", sum(len(rows) for rows in processor.candles.values())))
        processor.strategy_evaluations = int(snapshot.get("strategy_evaluations", 0))
        processor.strategy_skipped = int(snapshot.get("strategy_skipped", 0))
        processor._strategy_dirty = bool(snapshot.get("strategy_dirty", True))
        processor.last_strategy_window_sizes = {str(key): int(value) for key, value in dict(snapshot.get("last_strategy_window_sizes", {})).items()}
        processor.indicator_updates = {str(key): int(value) for key, value in dict(snapshot.get("indicator_updates", {})).items()}
        processor.events_processed = int(snapshot.get("events_processed", len(processor._events)))
        processor.last_event_time = utc(snapshot.get("last_event_time"))
        processor.last_event_id = snapshot.get("last_event_id")
        processor.last_available_at = utc(snapshot.get("last_available_at"))
        processor.evaluations = [evaluation_from_dict(item) for item in snapshot.get("evaluations", ())]
        processor._decision_ids = {_decision_id(item) for item in processor.evaluations}
        processor.signals = [signal_from_dict(item) for item in snapshot.get("signals", ())]
        processor._signal_ids = {item.signal_id for item in processor.signals}
        processor.episodes = {item.episode_id: item for item in (_episode_from_dict(raw) for raw in snapshot.get("episodes", ())) }
        raw_context = snapshot.get("context")
        if isinstance(raw_context, Mapping):
            processor.context = dict(raw_context)
            processor.context["timestamp"] = utc(raw_context.get("timestamp"))
        processor._simulation_book.pending = {item.simulation_id: item for item in (PendingSimulation.from_dict(raw) for raw in snapshot.get("pending_simulations", ())) }
        processor.completed_simulations = [PendingSimulation.from_dict(raw) for raw in snapshot.get("completed_simulations", ())]
        processor._simulation_book.completed = {item.simulation_id: item for item in processor.completed_simulations}
        processor.issues = [RuntimeIssue(str(raw.get("code")), str(raw.get("message")), utc(raw.get("timestamp")), raw.get("record_id")) for raw in snapshot.get("issues", ())]
        for name, raw_buckets in dict(snapshot.get("resample_buffers", {})).items():
            if name not in processor._resample_buffers or not isinstance(raw_buckets, Mapping):
                continue
            for raw_start, raw_bucket in raw_buckets.items():
                bucket_start = utc(raw_start)
                if bucket_start is None or not isinstance(raw_bucket, Mapping):
                    continue
                processor._resample_buffers[name][bucket_start] = {
                    (utc(raw_base) or datetime.fromtimestamp(0, UTC)): candle_from_dict(raw_candle)
                    for raw_base, raw_candle in raw_bucket.items()
                }
        for name, raw in dict(snapshot.get("aggregators", {})).items():
            if name not in processor.aggregators:
                continue
            aggregator = processor.aggregators[name]
            aggregator._closed_through = utc(raw.get("closed_through"))
            aggregator._seen_event_ids = set(str(item) for item in raw.get("seen_event_ids", ()))
            aggregator._last_event_time = utc(raw.get("last_event_time"))
            bucket_raw = raw.get("bucket")
            if bucket_raw:
                bucket_events = [event_from_dict(item) for item in bucket_raw.get("events", ())]
                aggregator._bucket = _bucket_from_dict(bucket_raw, bucket_events)
                if bucket_events:
                    aggregator._last_order_key = max(aggregator._event_order_key(event) for event in bucket_events)
        # Reanudar debe considerar cualquier señal previa al checkpoint como
        # fuente para simulaciones, pero no reinsertarla ni duplicarla.
        for signal in processor.signals:
            if signal.signal_id not in {item.signal_id for item in processor._simulation_book.pending.values()}:
                # add_signal deduplica por simulation_id, por lo que no pisa
                # los estados restaurados si ya había pending/completed.
                processor._simulation_book.add_signal(signal)
        return processor


IncrementalRuntime = IncrementalProcessor
RuntimeService = IncrementalProcessor


def _bucket_from_dict(raw: Mapping[str, Any], events: Sequence[MarketEvent]) -> Any:
    start = utc(raw.get("start")); end = utc(raw.get("end"))
    if start is None or end is None:
        raise ValueError("bucket de checkpoint sin start/end")
    return _Bucket(start, end, list(events))


def _attr_id(record: Any) -> str | None:
    value = _attr(record, "event_id", "candle_id", "data_id", default=None)
    if callable(value):
        value = value()
    return str(value) if value is not None else None


def _attr_time(record: Any) -> datetime | None:
    value = _attr(record, "event_time", "start", "interval_start", "timestamp", default=None)
    try:
        return utc(value)
    except Exception:
        return None


def _attr(record: Any, *names: str, default: Any = None) -> Any:
    if isinstance(record, Mapping):
        for name in names:
            if name in record and record[name] is not None:
                return record[name]
        return default
    for name in names:
        value = getattr(record, name, None)
        if value is not None:
            return value
    return default


def _looks_like_candle(record: Any) -> bool:
    if isinstance(record, Candle):
        return True
    if isinstance(record, Mapping):
        return "close" in record and ("end" in record or "end_ts" in record or "interval_end" in record)
    return hasattr(record, "close") and (hasattr(record, "end") or hasattr(record, "interval_end"))


def _replay_key(record: Any, ordinal: int) -> tuple[Any, int, int, int]:
    is_bar = _looks_like_candle(record)
    available = _attr(record, "available_at", "available_ts", "received_at", default=None)
    timestamp = _attr(record, "end", "interval_end", "end_ts", "event_time", "event_ts", "timestamp", default=None)
    try:
        available_dt = utc(available) or utc(timestamp) or datetime.fromtimestamp(0, UTC)
    except Exception:
        available_dt = datetime.fromtimestamp(0, UTC)
    try:
        timestamp_dt = utc(timestamp) or datetime.fromtimestamp(0, UTC)
    except Exception:
        timestamp_dt = datetime.fromtimestamp(0, UTC)
    tf = parse_timeframe(_attr(record, "timeframe", "resolution", "resolution_seconds", default="M1")) if is_bar else None
    # Native bars at same availability are ingested before events; larger
    # native temporalities first, then event aggregation closes M15/M5/M1 in
    # the same deterministic order.
    kind_priority = 0 if is_bar else 1
    timeframe_priority = -tf.seconds if tf else 0
    return available_dt, kind_priority, timeframe_priority, ordinal


def _episode_dict(episode: Any) -> dict[str, Any]:
    return {
        "episode_id": episode.episode_id,
        "instrument": episode.instrument,
        "direction": episode.direction,
        "registered_at": iso(episode.registered_at),
        "preparation_start": iso(episode.preparation_start),
        "preparation_end": iso(episode.preparation_end),
        "expires_at": iso(episode.expires_at),
        "context_start": iso(episode.context_start),
        "used": episode.used,
        "invalidated": episode.invalidated,
        "invalidation_reason": episode.invalidation_reason,
    }


def _episode_from_dict(value: Mapping[str, Any]) -> Any:
    from ..core.strategy import PreparationEpisode
    return PreparationEpisode(
        episode_id=str(value["episode_id"]),
        instrument=str(value["instrument"]),
        direction=str(value["direction"]),
        registered_at=utc(value["registered_at"]) or datetime.fromtimestamp(0, UTC),
        preparation_start=utc(value["preparation_start"]) or datetime.fromtimestamp(0, UTC),
        preparation_end=utc(value["preparation_end"]) or datetime.fromtimestamp(0, UTC),
        expires_at=utc(value["expires_at"]) or datetime.fromtimestamp(0, UTC),
        context_start=utc(value.get("context_start")),
        used=bool(value.get("used", False)),
        invalidated=bool(value.get("invalidated", False)),
        invalidation_reason=value.get("invalidation_reason"),
    )


def _indicator_point_dict(point: IndicatorPoint) -> dict[str, Any]:
    return {
        "start": iso(point.start),
        "end": iso(point.end),
        "available_at": iso(point.available_at),
        "close": point.close,
        "ema_fast": point.ema_fast,
        "ema_slow": point.ema_slow,
        "rsi": point.rsi,
        "atr": point.atr,
        "closed": point.closed,
        "quality": {"flags": sorted(flag.value for flag in point.quality.flags), "reasons": list(point.quality.reasons), "source": point.quality.source},
        "candle_id": point.candle_id,
        "index": point.index,
    }


def _indicator_point_from_dict(value: Mapping[str, Any]) -> IndicatorPoint:
    return IndicatorPoint(
        start=utc(value["start"]),
        end=utc(value["end"]),
        available_at=utc(value.get("available_at")),
        close=float(value["close"]) if value.get("close") is not None else None,
        ema_fast=float(value["ema_fast"]) if value.get("ema_fast") is not None else None,
        ema_slow=float(value["ema_slow"]) if value.get("ema_slow") is not None else None,
        rsi=float(value["rsi"]) if value.get("rsi") is not None else None,
        atr=float(value["atr"]) if value.get("atr") is not None else None,
        closed=bool(value.get("closed", True)),
        quality=quality_from(value.get("quality")),
        candle_id=value.get("candle_id"),
        index=int(value.get("index", 0)),
    )


def _engine_state(engine: Any) -> dict[str, Any]:
    def series_state(state: Any) -> dict[str, Any]:
        return {"values": list(state.values), "current": state.current, "period": state.period}
    return {
        "previous_end": iso(engine._previous_end) if isinstance(engine._previous_end, datetime) else engine._previous_end,
        "timeframe": engine._timeframe.name if engine._timeframe is not None else None,
        "instrument": engine._instrument,
        "index": engine._index,
        "ema_fast": series_state(engine._ema_fast),
        "ema_slow": series_state(engine._ema_slow),
        "rsi": {"previous_close": engine._rsi.previous_close, "gains": list(engine._rsi.gains), "losses": list(engine._rsi.losses), "average_gain": engine._rsi.average_gain, "average_loss": engine._rsi.average_loss, "period": engine._rsi.period},
        "atr": {"previous_close": engine._atr.previous_close, "true_ranges": list(engine._atr.true_ranges), "current": engine._atr.current, "period": engine._atr.period},
    }


def _restore_engine_state(engine: Any, value: Mapping[str, Any]) -> None:
    from collections import deque as _deque
    previous_end = value.get("previous_end")
    engine._previous_end = utc(previous_end) if previous_end else None
    raw_tf = value.get("timeframe")
    engine._timeframe = parse_timeframe(raw_tf) if raw_tf else engine._timeframe
    engine._instrument = str(value.get("instrument", engine._instrument))
    engine._index = int(value.get("index", engine._index))
    for name in ("ema_fast", "ema_slow"):
        raw = value.get(name)
        if not isinstance(raw, Mapping):
            continue
        state = getattr(engine, f"_{name}")
        state.values = _deque((float(item) for item in raw.get("values", ())), maxlen=state.period)
        state.current = float(raw["current"]) if raw.get("current") is not None else None
    raw = value.get("rsi")
    if isinstance(raw, Mapping):
        engine._rsi.previous_close = float(raw["previous_close"]) if raw.get("previous_close") is not None else None
        engine._rsi.gains = _deque((float(item) for item in raw.get("gains", ())), maxlen=engine._rsi.period)
        engine._rsi.losses = _deque((float(item) for item in raw.get("losses", ())), maxlen=engine._rsi.period)
        engine._rsi.average_gain = float(raw["average_gain"]) if raw.get("average_gain") is not None else None
        engine._rsi.average_loss = float(raw["average_loss"]) if raw.get("average_loss") is not None else None
    raw = value.get("atr")
    if isinstance(raw, Mapping):
        engine._atr.previous_close = float(raw["previous_close"]) if raw.get("previous_close") is not None else None
        engine._atr.true_ranges = _deque((float(item) for item in raw.get("true_ranges", ())), maxlen=engine._atr.period)
        engine._atr.current = float(raw["current"]) if raw.get("current") is not None else None


def point_available(point: IndicatorPoint) -> datetime | None:
    if not point.closed:
        return None
    available = point.available_at or point.end
    if not isinstance(available, datetime) or available.tzinfo is None:
        return None
    end = point.end.astimezone(UTC)
    return max(available.astimezone(UTC), end)


def _decision_id(evaluation: Any) -> str:
    if hasattr(evaluation, "as_dict"):
        payload = evaluation.as_dict()
    else:
        payload = str(evaluation)
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


__all__ = [
    "IncrementalProcessor",
    "IncrementalRuntime",
    "ProcessResult",
    "ReplayResult",
    "RuntimeIssue",
    "RuntimeService",
    "SimulationConfig",
]
