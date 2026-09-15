"""Procesador incremental neutral al proveedor.

``IncrementalProcessor`` es el adaptador de tiempo real/replay entre los
registros canónicos y el núcleo determinista. No contiene una segunda regla
financiera: agrega, calcula indicadores y llama a ``TrendPullbackStrategy``.
Las simulaciones aquí sólo son virtuales y quedan en estado ``PENDING`` hasta
que el flujo aporta observaciones causales suficientes.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, cast

from ..core import (
    Candle,
    CandleAggregator,
    DecisionKind,
    Evaluation,
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
    parse_timeframe,
)
from ..core.aggregation import _Bucket, interval_start
from ..core.canonical import fingerprint
from ..core.historical_calendar import HistoricalQuoteCalendar
from ..core.quality import DataQuality, merge_quality
from .consumers import (
    BinarySimulationConsumer,
    SignalConsumer,
    SignalConsumerEvent,
    SignalConsumerResult,
    consumer_from_checkpoint,
)
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
    signal_dict,
    signal_from_dict,
    to_core_candle,
    to_core_event,
    utc,
)

_BLOCKING_RUNTIME_ISSUES = frozenset(
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


def _timeframe(value: Timeframe | str | int) -> Timeframe:
    """Resolve the union accepted by core models at runtime boundaries."""

    return parse_timeframe(value)


def _timeframe_name(value: Timeframe | str | int) -> str:
    return _timeframe(value).name


def _strategy_timeframe_names(strategy: StrategyConfig) -> tuple[str, str, str]:
    return (
        _timeframe_name(strategy.context_timeframe),
        _timeframe_name(strategy.preparation_timeframe),
        _timeframe_name(strategy.trigger_timeframe),
    )


def _normalise_market_candidate(value: str | None) -> str:
    if value is None or not str(value).strip():
        return "trend_pullback_v1"
    candidate = str(value).strip()
    if candidate == "trend_pullback_v1":
        return candidate
    from ..core.market_profiles import market_profile

    profile = market_profile(candidate)
    return profile.candidate_id


def _processor_config_hash(processor: Any) -> str:
    base = config_hash(
        processor.strategy_config,
        processor.simulation_config,
        processor.timeframes,
        processor.mode,
        processor.price_base,
    )
    extra = _processor_extra_config(processor)
    return fingerprint({"base": base, **extra}) if extra else base


def _processor_extra_config(processor: Any) -> dict[str, Any]:
    extra: dict[str, Any] = {}
    if processor.market_candidate_id != "trend_pullback_v1":
        extra["market_candidate_id"] = processor.market_candidate_id
    if processor.quote_coverage_mode != "strict":
        extra["quote_coverage_mode"] = processor.quote_coverage_mode
        extra["max_quote_gap_seconds"] = processor.max_quote_gap_seconds
    if processor.historical_calendar is not None:
        extra["historical_calendar"] = processor.historical_calendar.to_dict()
    return extra


def _checkpoint_historical_calendar(snapshot: Mapping[str, Any]) -> HistoricalQuoteCalendar | None:
    raw = snapshot.get("historical_calendar")
    return HistoricalQuoteCalendar.from_mapping(raw) if raw is not None else None


def _container(max_candles: int | None) -> deque[Any] | list[Any]:
    return deque(maxlen=max_candles) if max_candles is not None else []


def _event_identity(event: MarketEvent) -> str:
    event_id = event.event_id
    if event_id is None:
        raise ValueError("evento sin event_id")
    return event_id


@dataclass(frozen=True, slots=True)
class RuntimeIssue:
    code: str
    message: str
    timestamp: datetime | None = None
    record_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "timestamp": iso(self.timestamp),
            "record_id": self.record_id,
        }


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
    consumer_events: tuple[SignalConsumerEvent, ...] = ()

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


@dataclass(frozen=True, slots=True)
class PrecomputedFrame:
    """One causal candle and its already-computed indicator point.

    A frame is produced by :class:`SharedDataPlane` and consumed by one or
    more independent strategy processors.  The point is never recalculated by
    the consumer; compatibility and availability are checked at the public
    processor seam before the strategy sees it.
    """

    candle: Candle
    indicator_point: IndicatorPoint


@dataclass(frozen=True, slots=True)
class DataPlaneResult:
    """Bounded output of one shared data-plane ingestion step."""

    accepted: bool
    event: MarketEvent | None = None
    frames: tuple[PrecomputedFrame, ...] = ()
    issues: tuple[RuntimeIssue, ...] = ()
    cached: bool = False
    last_event_time: datetime | None = None
    last_event_id: str | None = None
    last_available_at: datetime | None = None

    @property
    def candles(self) -> tuple[Candle, ...]:
        return tuple(frame.candle for frame in self.frames)

    @property
    def indicator_points(self) -> tuple[IndicatorPoint, ...]:
        return tuple(frame.indicator_point for frame in self.frames)


class SharedDataPlane:
    """Single causal aggregation/indicator plane shared by strategies.

    The plane owns one ``CandleAggregator`` and one
    ``IncrementalIndicatorEngine`` per canonical data key.  Strategy
    processors attach to it and retain only their own detector, episode,
    signal, consumer and reporting state.  Quotes therefore cross the
    aggregation/indicator boundary once, while strategies receive immutable
    ``PrecomputedFrame`` values through :meth:`IncrementalProcessor.feed_precomputed`.
    """

    CHECKPOINT_VERSION = 1

    def __init__(
        self,
        *,
        timeframes: Iterable[Timeframe | str],
        indicator_config: IndicatorConfig | Mapping[str, Any] | None = None,
        mode: OperationMode | str = OperationMode.REPLAY,
        instrument: str | None = None,
        source: str = "runtime",
        price_base: PriceBase | str = PriceBase.TRADED,
        max_candles: int | None = None,
        coverage_mode: str = "strict",
        max_quote_gap_seconds: float | None = None,
        historical_calendar: HistoricalQuoteCalendar | None = None,
    ) -> None:
        self.mode = mode_from(mode)
        self.instrument = instrument.strip() if instrument else None
        self.source = str(source).strip() or "runtime"
        self.price_base = price_base if isinstance(price_base, PriceBase) else PriceBase(str(price_base).lower())
        selected = tuple(parse_timeframe(item) for item in timeframes)
        if not selected or len({item.name for item in selected}) != len(selected):
            raise ValueError("data plane requiere temporalidades únicas")
        self.timeframes = selected
        self.indicator_config = (
            indicator_config
            if isinstance(indicator_config, IndicatorConfig)
            else IndicatorConfig.from_mapping(indicator_config)
        )
        if max_candles is not None and (
            isinstance(max_candles, bool) or not isinstance(max_candles, int) or max_candles <= 0
        ):
            raise ValueError("max_candles debe ser entero positivo")
        self.max_candles = max_candles
        self.quote_coverage_mode = str(coverage_mode)
        self.max_quote_gap_seconds = max_quote_gap_seconds
        self.historical_calendar = historical_calendar
        self.aggregators: dict[str, CandleAggregator] = {
            timeframe.name: CandleAggregator(
                timeframe,
                instrument=self.instrument,
                price_base=self.price_base,
                source=f"{self.source}:aggregated",
                mode=self.mode,
                max_seen_event_ids=max_candles,
                max_issues=max_candles,
                coverage_mode=self.quote_coverage_mode,
                max_quote_gap_seconds=max_quote_gap_seconds,
                historical_calendar=historical_calendar,
            )
            for timeframe in self.timeframes
        }
        from ..core.indicators import IncrementalIndicatorEngine

        self.indicator_engines: dict[str, Any] = {
            timeframe.name: IncrementalIndicatorEngine(
                self.indicator_config,
                max_points=max_candles,
                historical_calendar=historical_calendar,
            )
            for timeframe in self.timeframes
        }
        self.candles: dict[str, deque[Candle] | list[Candle]] = {
            timeframe.name: _container(max_candles) for timeframe in self.timeframes
        }
        self.indicator_points: dict[str, deque[IndicatorPoint] | list[IndicatorPoint]] = {
            timeframe.name: _container(max_candles) for timeframe in self.timeframes
        }
        self._seen_event_ids: set[str] = set()
        self._seen_event_order: deque[str] = deque()
        self._events: dict[str, MarketEvent] = {}
        self._last_event_time: datetime | None = None
        self._last_event_id: str | None = None
        self._last_available_at: datetime | None = None
        self.events_processed = 0
        self.candles_processed = 0
        self.indicator_updates: dict[str, int] = {timeframe.name: 0 for timeframe in self.timeframes}
        self._last_result: DataPlaneResult | None = None

    @property
    def key(self) -> tuple[str | None, str, str, str, tuple[str, ...], str]:
        return (
            self.instrument,
            self.mode.value,
            self.price_base.value,
            self.quote_coverage_mode,
            tuple(item.name for item in self.timeframes),
            self.indicator_config_hash,
        )

    @property
    def indicator_config_hash(self) -> str:
        return fingerprint(
            {
                "ema_fast": self.indicator_config.ema_fast,
                "ema_slow": self.indicator_config.ema_slow,
                "rsi_period": self.indicator_config.rsi_period,
                "atr_period": self.indicator_config.atr_period,
                "wilder": self.indicator_config.wilder,
            }
        )

    @property
    def config_hash(self) -> str:
        return fingerprint(self._config_material())

    @property
    def last_event_time(self) -> datetime | None:
        return self._last_event_time

    @property
    def last_event_id(self) -> str | None:
        return self._last_event_id

    @property
    def last_available_at(self) -> datetime | None:
        return self._last_available_at

    @property
    def status(self) -> dict[str, Any]:
        result = {
            "config_hash": self.config_hash,
            "key": self.key,
            "mode": self.mode.value,
            "instrument": self.instrument,
            "price_base": self.price_base.value,
            "coverage_mode": self.quote_coverage_mode,
            "max_quote_gap_seconds": self.max_quote_gap_seconds,
            "timeframes": [item.name for item in self.timeframes],
            "aggregators": len(self.aggregators),
            "indicator_engines": len(self.indicator_engines),
            "events_processed": self.events_processed,
            "candles_processed": self.candles_processed,
            "indicator_updates": dict(self.indicator_updates),
            "last_event_time": iso(self.last_event_time),
            "last_event_id": self.last_event_id,
            "last_available_at": iso(self.last_available_at),
            "candles": {name: len(values) for name, values in self.candles.items()},
            "indicator_points": {name: len(values) for name, values in self.indicator_points.items()},
        }
        return result

    def _config_material(self) -> dict[str, Any]:
        return {
            "checkpoint_version": self.CHECKPOINT_VERSION,
            "mode": self.mode.value,
            "instrument": self.instrument,
            "source": self.source,
            "price_base": self.price_base.value,
            "timeframes": [item.name for item in self.timeframes],
            "indicator_config": {
                "ema_fast": self.indicator_config.ema_fast,
                "ema_slow": self.indicator_config.ema_slow,
                "rsi_period": self.indicator_config.rsi_period,
                "atr_period": self.indicator_config.atr_period,
                "wilder": self.indicator_config.wilder,
            },
            "max_candles": self.max_candles,
            "coverage_mode": self.quote_coverage_mode,
            "max_quote_gap_seconds": self.max_quote_gap_seconds,
            "historical_calendar": self.historical_calendar.to_dict() if self.historical_calendar is not None else None,
        }

    def _issue(self, code: str, message: str, record: Any | None = None) -> RuntimeIssue:
        return RuntimeIssue(code, message, _attr_time(record), _attr_id(record))

    def _remember_event(self, event: MarketEvent) -> None:
        event_id = _event_identity(event)
        self._seen_event_ids.add(event_id)
        self._seen_event_order.append(event_id)
        self._events[event_id] = event
        if self.max_candles is not None:
            limit = max(self.max_candles, self.max_candles * len(self.timeframes))
            while len(self._seen_event_order) > limit:
                old_id = self._seen_event_order.popleft()
                self._seen_event_ids.discard(old_id)
                self._events.pop(old_id, None)
        self.events_processed += 1
        self._last_event_id = event_id
        self._last_event_time = event.event_time
        self._last_available_at = event.effective_available_at

    def _append_frame(self, candle: Candle, point: IndicatorPoint) -> None:
        candles = self.candles[candle.timeframe_name]
        points = self.indicator_points[candle.timeframe_name]
        if self.max_candles is not None and len(candles) >= self.max_candles:
            candles.popleft() if isinstance(candles, deque) else candles.pop(0)
            points.popleft() if isinstance(points, deque) else points.pop(0)
        candles.append(candle)
        points.append(point)
        self.candles_processed += 1
        self.indicator_updates[candle.timeframe_name] = self.indicator_updates.get(candle.timeframe_name, 0) + 1

    def _feed_core_event(self, event: MarketEvent) -> DataPlaneResult:
        if self.instrument is None:
            self.instrument = event.instrument
            for aggregator in self.aggregators.values():
                aggregator.instrument = event.instrument
        if event.instrument != self.instrument:
            issue = self._issue(
                "instrument_mismatch", f"Se esperaba {self.instrument}, llegó {event.instrument}", event
            )
            return DataPlaneResult(
                False,
                event=event,
                issues=(issue,),
                last_event_time=self.last_event_time,
                last_event_id=self.last_event_id,
                last_available_at=self.last_available_at,
            )
        if event.mode is not self.mode:
            issue = self._issue("mode_mismatch", f"Se esperaba {self.mode.value}, llegó {event.mode.value}", event)
            return DataPlaneResult(
                False,
                event=event,
                issues=(issue,),
                last_event_time=self.last_event_time,
                last_event_id=self.last_event_id,
                last_available_at=self.last_available_at,
            )
        event_id = _event_identity(event)
        if event_id in self._seen_event_ids:
            if (
                self._last_result is not None
                and self._last_result.event is not None
                and _event_identity(self._last_result.event) == event_id
            ):
                return replace(self._last_result, cached=True)
            issue = self._issue("duplicate_event", f"Evento repetido: {event_id}", event)
            return DataPlaneResult(
                False,
                event=event,
                issues=(issue,),
                last_event_time=self.last_event_time,
                last_event_id=self.last_event_id,
                last_available_at=self.last_available_at,
            )
        if self._last_event_time is not None and event.event_time < self._last_event_time:
            issue = self._issue("out_of_order_event", f"Evento fuera de orden: {event.event_time.isoformat()}", event)
            return DataPlaneResult(
                False,
                event=event,
                issues=(issue,),
                last_event_time=self.last_event_time,
                last_event_id=self.last_event_id,
                last_available_at=self.last_available_at,
            )
        self._remember_event(event)
        frames: list[PrecomputedFrame] = []
        issues: list[RuntimeIssue] = []
        accepted = True
        for timeframe in sorted(self.timeframes, key=lambda item: item.seconds, reverse=True):
            result = self.aggregators[timeframe.name].add(event)
            accepted = accepted and result.accepted
            issues.extend(
                RuntimeIssue(item.code, item.message, item.timestamp, item.record_id) for item in result.issues
            )
            for candle in result.emitted:
                point = self.indicator_engines[timeframe.name].update(candle)
                self._append_frame(candle, point)
                frames.append(PrecomputedFrame(candle, point))
        output = DataPlaneResult(
            accepted,
            event=event,
            frames=tuple(frames),
            issues=tuple(issues),
            last_event_time=self.last_event_time,
            last_event_id=self.last_event_id,
            last_available_at=self.last_available_at,
        )
        self._last_result = output
        return output

    def feed_event(self, record: Any) -> DataPlaneResult:
        """Validate and ingest one event exactly once across all strategies."""

        try:
            event = to_core_event(record, mode=self.mode)
        except Exception as exc:
            issue = self._issue("event_invalid", str(exc), record)
            return DataPlaneResult(
                False,
                issues=(issue,),
                last_event_time=self.last_event_time,
                last_event_id=self.last_event_id,
                last_available_at=self.last_available_at,
            )
        return self._feed_core_event(event)

    def feed_candle(self, record: Any) -> DataPlaneResult:
        """Ingest one native candle and compute its indicator once.

        Native higher-timeframe candles are accepted independently.  Event
        streams should use :meth:`feed_event`, which owns all aggregation.
        No ticks or OHLC values are fabricated for a missing timeframe.
        """

        try:
            candle = to_core_candle(record, mode=self.mode)
        except Exception as exc:
            issue = self._issue("candle_invalid", str(exc), record)
            return DataPlaneResult(
                False,
                issues=(issue,),
                last_event_time=self.last_event_time,
                last_event_id=self.last_event_id,
                last_available_at=self.last_available_at,
            )
        if self.instrument is None:
            self.instrument = candle.instrument
        if candle.instrument != self.instrument:
            issue = self._issue(
                "instrument_mismatch", f"Se esperaba {self.instrument}, llegó {candle.instrument}", candle
            )
            return DataPlaneResult(
                False,
                issues=(issue,),
                last_event_time=self.last_event_time,
                last_event_id=self.last_event_id,
                last_available_at=self.last_available_at,
            )
        if candle.timeframe_name not in self.candles:
            issue = self._issue(
                "timeframe_not_configured", f"Temporalidad no configurada: {candle.timeframe_name}", candle
            )
            return DataPlaneResult(
                False,
                issues=(issue,),
                last_event_time=self.last_event_time,
                last_event_id=self.last_event_id,
                last_available_at=self.last_available_at,
            )
        point = self.indicator_engines[candle.timeframe_name].update(candle)
        self._append_frame(candle, point)
        self.candles_processed += 0
        available = candle.available_at or candle.end
        self._last_event_id = candle.candle_id
        self._last_event_time = candle.end
        self._last_available_at = available
        output = DataPlaneResult(
            True,
            frames=(PrecomputedFrame(candle, point),),
            last_event_time=self.last_event_time,
            last_event_id=self.last_event_id,
            last_available_at=self.last_available_at,
        )
        self._last_result = output
        return output

    def latest_indicator_point(
        self, timeframe: Timeframe | str | int, *, at: datetime | None = None
    ) -> IndicatorPoint | None:
        name = _timeframe_name(timeframe)
        points = self.indicator_points.get(name, ())
        if not points:
            return None
        if at is None:
            return points[-1]
        watermark = utc(at)
        if watermark is None:
            return None
        return next(
            (
                point
                for point in reversed(points)
                if (available := point_available(point)) is not None and available <= watermark
            ),
            None,
        )

    def _aggregator_checkpoint(self, aggregator: CandleAggregator) -> dict[str, Any]:
        # The bounded accumulator seam is deliberately opt-in.  The strict
        # aggregator still owns its event list and its checkpoint contract;
        # calling ``export_bucket_state`` for it is an error by design.
        bucket_state: dict[str, Any] | None = None
        if getattr(aggregator, "coverage_mode", "strict") == "continuous_quotes":
            export = getattr(aggregator, "export_bucket_state", None)
            if callable(export):
                exported = export()
                bucket_state = cast(dict[str, Any] | None, exported)
        if bucket_state is None:
            bucket = getattr(aggregator, "_bucket", None)
            bucket_state = {
                "version": 1,
                "start": iso(bucket.start) if bucket is not None else None,
                "end": iso(bucket.end) if bucket is not None else None,
                "events": [event_dict(event) for event in bucket.events] if bucket is not None else [],
                "partial": bucket.partial if bucket is not None else None,
            }
        return {
            "bucket": bucket_state,
            "closed_through": iso(getattr(aggregator, "_closed_through", None)),
            "seen_event_ids": sorted(getattr(aggregator, "_seen_event_ids", set())),
            "seen_event_order": list(getattr(aggregator, "_seen_event_order", ())),
            "last_event_time": iso(getattr(aggregator, "_last_event_time", None)),
        }

    def checkpoint(self) -> dict[str, Any]:
        """Serialize the shared plane once, without strategy/consumer state."""

        return {
            "checkpoint_version": self.CHECKPOINT_VERSION,
            "config_hash": self.config_hash,
            **self._config_material(),
            "seen_event_ids": sorted(self._seen_event_ids),
            "seen_event_order": list(self._seen_event_order),
            "events": [event_dict(event) for event in self._events.values()],
            "last_event_time": iso(self.last_event_time),
            "last_event_id": self.last_event_id,
            "last_available_at": iso(self.last_available_at),
            "events_processed": self.events_processed,
            "candles_processed": self.candles_processed,
            "indicator_updates": dict(self.indicator_updates),
            "aggregators": {
                name: self._aggregator_checkpoint(aggregator) for name, aggregator in self.aggregators.items()
            },
            "candles": {name: [candle_dict(candle) for candle in values] for name, values in self.candles.items()},
            "indicator_points": {
                name: [_indicator_point_dict(point) for point in values]
                for name, values in self.indicator_points.items()
            },
            "indicator_engines": {name: _engine_state(engine) for name, engine in self.indicator_engines.items()},
        }

    snapshot = checkpoint

    @classmethod
    def from_checkpoint(cls, snapshot: Mapping[str, Any] | str) -> SharedDataPlane:
        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot)
        if not isinstance(snapshot, Mapping):
            raise TypeError("data-plane checkpoint debe ser mapping o JSON")
        if int(snapshot.get("checkpoint_version", 0)) != cls.CHECKPOINT_VERSION:
            raise ValueError("versión de data-plane checkpoint no soportada")
        plane = cls._from_checkpoint_config(snapshot)
        if snapshot.get("config_hash") != plane.config_hash:
            raise ValueError("config_hash del data plane no coincide")
        plane._restore_checkpoint_state(snapshot)
        return plane

    @classmethod
    def _from_checkpoint_config(cls, snapshot: Mapping[str, Any]) -> SharedDataPlane:
        calendar_raw = snapshot.get("historical_calendar")
        calendar = HistoricalQuoteCalendar.from_mapping(calendar_raw) if isinstance(calendar_raw, Mapping) else None
        return cls(
            timeframes=tuple(str(item) for item in snapshot.get("timeframes", ())),
            indicator_config=snapshot.get("indicator_config")
            if isinstance(snapshot.get("indicator_config"), Mapping)
            else None,
            mode=snapshot.get("mode", "REPLAY"),
            instrument=snapshot.get("instrument"),
            source=snapshot.get("source", "runtime"),
            price_base=snapshot.get("price_base", "traded"),
            max_candles=snapshot.get("max_candles"),
            coverage_mode=snapshot.get("coverage_mode", "strict"),
            max_quote_gap_seconds=snapshot.get("max_quote_gap_seconds"),
            historical_calendar=calendar,
        )

    def _restore_checkpoint_state(self, snapshot: Mapping[str, Any]) -> None:
        self._seen_event_ids = {str(item) for item in snapshot.get("seen_event_ids", ())}
        self._seen_event_order = deque(str(item) for item in snapshot.get("seen_event_order", self._seen_event_ids))
        self._last_event_time = utc(snapshot.get("last_event_time"))
        self._last_event_id = str(snapshot["last_event_id"]) if snapshot.get("last_event_id") is not None else None
        self._last_available_at = utc(snapshot.get("last_available_at"))
        self.events_processed = int(snapshot.get("events_processed", len(self._events)))
        self.candles_processed = int(snapshot.get("candles_processed", 0))
        self.indicator_updates = {
            str(key): int(value) for key, value in dict(snapshot.get("indicator_updates", {})).items()
        }
        self._restore_checkpoint_events(snapshot)
        self._restore_checkpoint_candles(snapshot)
        self._restore_checkpoint_indicator_points(snapshot)
        self._restore_checkpoint_engines(snapshot)
        self._restore_checkpoint_aggregators(snapshot)

    def _restore_checkpoint_events(self, snapshot: Mapping[str, Any]) -> None:
        for raw in snapshot.get("events", ()):
            event = event_from_dict(raw)
            if event.event_id is not None:
                self._events[event.event_id] = event

    def _restore_checkpoint_candles(self, snapshot: Mapping[str, Any]) -> None:
        for name, rows in dict(snapshot.get("candles", {})).items():
            if name not in self.candles:
                continue
            for raw in rows:
                self.candles[name].append(candle_from_dict(raw))

    def _restore_checkpoint_indicator_points(self, snapshot: Mapping[str, Any]) -> None:
        for name, rows in dict(snapshot.get("indicator_points", {})).items():
            if name not in self.indicator_points:
                continue
            for raw in rows:
                self.indicator_points[name].append(_indicator_point_from_dict(raw))

    def _restore_checkpoint_engines(self, snapshot: Mapping[str, Any]) -> None:
        for name, raw in dict(snapshot.get("indicator_engines", {})).items():
            if name in self.indicator_engines and isinstance(raw, Mapping):
                _restore_engine_state(self.indicator_engines[name], raw)

    def _restore_checkpoint_aggregators(self, snapshot: Mapping[str, Any]) -> None:
        for name, raw in dict(snapshot.get("aggregators", {})).items():
            if name not in self.aggregators or not isinstance(raw, Mapping):
                continue
            _restore_shared_aggregator(self.aggregators[name], raw)

    restore = from_checkpoint


def _resolve_processor_data_plane(
    *,
    timeframes: tuple[Timeframe, ...],
    mode: OperationMode,
    price_base: PriceBase,
    instrument: str | None,
    strategy_config: StrategyConfig,
    data_plane: SharedDataPlane | None,
    quote_coverage_mode: str,
    max_quote_gap_seconds: float | None,
    historical_calendar: HistoricalQuoteCalendar | None,
) -> tuple[str | None, str, float | None, HistoricalQuoteCalendar | None]:
    """Validate a shared plane and resolve its data-owned settings."""

    if data_plane is None:
        return instrument, quote_coverage_mode, max_quote_gap_seconds, historical_calendar
    plane_timeframes = {item.name for item in data_plane.timeframes}
    missing = [item.name for item in timeframes if item.name not in plane_timeframes]
    if missing:
        raise ValueError(f"data_plane no contiene temporalidades: {missing}")
    if data_plane.mode is not mode or data_plane.price_base is not price_base:
        raise ValueError("data_plane no coincide en mode/price_base")
    if strategy_config.indicators != data_plane.indicator_config:
        raise ValueError("data_plane no coincide en indicator_config")
    if instrument is not None and data_plane.instrument is not None and instrument != data_plane.instrument:
        raise ValueError("data_plane no coincide en instrument")
    if quote_coverage_mode != "strict" and quote_coverage_mode != data_plane.quote_coverage_mode:
        raise ValueError("data_plane no coincide en quote_coverage_mode")
    if max_quote_gap_seconds is not None and max_quote_gap_seconds != data_plane.max_quote_gap_seconds:
        raise ValueError("data_plane no coincide en max_quote_gap_seconds")
    plane_calendar = data_plane.historical_calendar.to_dict() if data_plane.historical_calendar is not None else None
    if historical_calendar is not None and historical_calendar.to_dict() != plane_calendar:
        raise ValueError("data_plane no coincide en historical_calendar")
    return (
        instrument if instrument is not None else data_plane.instrument,
        data_plane.quote_coverage_mode,
        data_plane.max_quote_gap_seconds,
        data_plane.historical_calendar,
    )


def _new_processor_data_components(
    *,
    data_plane: SharedDataPlane | None,
    timeframes: tuple[Timeframe, ...],
    mode: OperationMode,
    instrument: str | None,
    source: str,
    price_base: PriceBase,
    max_candles: int | None,
    quote_coverage_mode: str,
    max_quote_gap_seconds: float | None,
    historical_calendar: HistoricalQuoteCalendar | None,
    indicator_config: IndicatorConfig,
) -> tuple[dict[str, CandleAggregator], dict[str, Any]]:
    """Build data-owned components, sharing them when a plane is supplied."""

    if data_plane is not None:
        return (
            {tf.name: data_plane.aggregators[tf.name] for tf in timeframes},
            {tf.name: data_plane.indicator_engines[tf.name] for tf in timeframes},
        )
    aggregators = {
        tf.name: CandleAggregator(
            tf,
            instrument=instrument,
            price_base=price_base,
            source=f"{source}:aggregated",
            mode=mode,
            max_seen_event_ids=max_candles,
            max_issues=max_candles,
            coverage_mode=quote_coverage_mode,
            max_quote_gap_seconds=max_quote_gap_seconds,
            historical_calendar=historical_calendar,
        )
        for tf in timeframes
    }
    from ..core.indicators import IncrementalIndicatorEngine

    engines = {
        tf.name: IncrementalIndicatorEngine(
            indicator_config,
            max_points=max_candles,
            historical_calendar=historical_calendar,
        )
        for tf in timeframes
    }
    return aggregators, engines


def _market_strategy_for_candidate(candidate_id: str) -> Any | None:
    if candidate_id == "trend_pullback_v1" or not candidate_id.startswith("dc_"):
        return None
    from ..core.market_profiles import market_profile
    from ..core.strategy_extensions import Donchian20Config, Donchian20M5Strategy, Donchian20Strategy

    target = market_profile(candidate_id).trigger_timeframe
    return Donchian20M5Strategy() if target == "M5" else Donchian20Strategy(Donchian20Config(timeframe=target))


def _resolve_signal_consumer(
    consumer: SignalConsumer | None,
    simulation_config: SimulationConfig,
) -> SignalConsumer:
    selected = consumer if consumer is not None else BinarySimulationConsumer(simulation_config)
    if not isinstance(selected, SignalConsumer):
        raise TypeError("signal_consumer no cumple el contrato SignalConsumer")
    return selected


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
        signal_consumer: SignalConsumer | None = None,
        market_candidate_id: str | None = None,
        data_plane: SharedDataPlane | None = None,
        quote_coverage_mode: str = "strict",
        max_quote_gap_seconds: float | None = None,
        historical_calendar: HistoricalQuoteCalendar | None = None,
    ) -> None:
        self.mode = mode_from(mode)
        self.instrument = instrument.strip() if instrument else None
        self.source = str(source).strip() or "runtime"
        self.price_base = price_base if isinstance(price_base, PriceBase) else PriceBase(str(price_base).lower())
        self.timeframes: tuple[Timeframe, ...] = tuple(
            parse_timeframe(item) for item in (timeframes or self.DEFAULT_TIMEFRAMES)
        )
        if len(set(tf.name for tf in self.timeframes)) != len(self.timeframes):
            raise ValueError("timeframes no puede contener duplicados")
        if not self.timeframes:
            raise ValueError("timeframes no puede estar vacío")
        strategy_cfg = strategy if isinstance(strategy, StrategyConfig) else StrategyConfig.from_mapping(strategy)
        if strategy_cfg.mode is not self.mode:
            strategy_cfg = replace(strategy_cfg, mode=self.mode)
        self.strategy_config = strategy_cfg
        self.strategy = TrendPullbackStrategy(strategy_cfg)
        self.market_candidate_id = _normalise_market_candidate(market_candidate_id)
        if data_plane is not None and not isinstance(data_plane, SharedDataPlane):
            raise TypeError("data_plane debe ser SharedDataPlane")
        self.instrument, quote_coverage_mode, max_quote_gap_seconds, historical_calendar = (
            _resolve_processor_data_plane(
                timeframes=self.timeframes,
                mode=self.mode,
                price_base=self.price_base,
                instrument=self.instrument,
                strategy_config=strategy_cfg,
                data_plane=data_plane,
                quote_coverage_mode=quote_coverage_mode,
                max_quote_gap_seconds=max_quote_gap_seconds,
                historical_calendar=historical_calendar,
            )
        )
        self.quote_coverage_mode = quote_coverage_mode
        self.max_quote_gap_seconds = max_quote_gap_seconds
        self.historical_calendar = historical_calendar
        self.data_plane = data_plane
        self._market_strategy = _market_strategy_for_candidate(self.market_candidate_id)
        self.simulation_config = (
            simulation if isinstance(simulation, SimulationConfig) else SimulationConfig.from_mapping(simulation)
        )
        self.signal_consumer = _resolve_signal_consumer(signal_consumer, self.simulation_config)
        self.max_candles = (
            max_candles if max_candles is not None else data_plane.max_candles if data_plane is not None else None
        )
        if max_candles is not None and (isinstance(max_candles, bool) or max_candles <= 0):
            raise ValueError("max_candles debe ser positivo")
        # Kept as a compatibility view for existing binary callers.  Non-binary
        # consumers intentionally expose no book object.
        self._simulation_book = getattr(self.signal_consumer, "book", None)
        self._consumer_pending: tuple[PendingSimulation, ...] = ()
        self._consumer_events: deque[SignalConsumerEvent] = deque(maxlen=max(256, self.max_candles or 4096))
        self._consumer_event_count = 0
        self._transition_consumer_events: list[SignalConsumerEvent] = []
        self.aggregators, self.indicator_engines = _new_processor_data_components(
            data_plane=data_plane,
            timeframes=self.timeframes,
            mode=self.mode,
            instrument=self.instrument,
            source=self.source,
            price_base=self.price_base,
            max_candles=self.max_candles,
            quote_coverage_mode=self.quote_coverage_mode,
            max_quote_gap_seconds=self.max_quote_gap_seconds,
            historical_calendar=self.historical_calendar,
            indicator_config=strategy_cfg.indicators,
        )

        self.candles: dict[str, Any] = {tf.name: _container(self.max_candles) for tf in self.timeframes}
        self.indicator_points: dict[str, Any] = {tf.name: _container(self.max_candles) for tf in self.timeframes}
        self._logical_candle_keys: dict[str, set[tuple[str, datetime]]] = {tf.name: set() for tf in self.timeframes}
        self._candle_by_key: dict[str, dict[tuple[str, datetime], Candle]] = {tf.name: {} for tf in self.timeframes}
        self._resample_buffers: dict[str, dict[datetime, dict[datetime, Candle]]] = {
            tf.name: {} for tf in self.timeframes
        }
        self._candle_ids: set[str | None] = set()
        self._candle_id_counts: dict[str, int] = {}
        self._processed_candle_starts: dict[str, set[datetime]] = {tf.name: set() for tf in self.timeframes}
        # Absolute indices avoid rebuilding a max-sized mapping on every
        # deque eviction; convert to deque-relative indices at read sites.
        self._point_index_by_start: dict[str, dict[datetime, int]] = {tf.name: {} for tf in self.timeframes}
        self._point_base_index: dict[str, int] = {tf.name: 0 for tf in self.timeframes}
        self._seen_event_ids: set[str] = set()
        self._events: dict[str, MarketEvent] = {}
        self.evaluations: list[Any] = []
        self.signals: list[Signal] = []
        self._decision_ids: set[str] = set()
        self._decision_order: deque[str] = deque()
        self._signal_ids: set[str] = set()
        self._signal_order: deque[str] = deque()
        self.episodes: dict[str, Any] = {}
        self.context: dict[str, Any] | None = None
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
        return self._consumer_pending

    def latest_indicator_point(
        self,
        timeframe: Timeframe | str | int,
        *,
        at: datetime | None = None,
    ) -> IndicatorPoint | None:
        """Return the latest already-computed point at or before ``at``.

        Historical runners and read-only observers use this seam to consume
        the processor's existing ATR/EMA/RSI state without touching private
        indicator-engine attributes or recalculating a valid prefix.
        """

        name = _timeframe_name(timeframe)
        points = cast(Sequence[IndicatorPoint], self.indicator_points.get(name, ()))
        if not points:
            return None
        if at is None:
            return points[-1]
        watermark = utc(at)
        if watermark is None:
            return None
        return next(
            (
                point
                for point in reversed(points)
                if (availability := point_available(point)) is not None and availability <= watermark
            ),
            None,
        )

    def _validate_data_plane(self, data_plane: SharedDataPlane) -> None:
        if data_plane.mode is not self.mode or data_plane.price_base is not self.price_base:
            raise ValueError("data_plane no coincide en mode/price_base")
        if self.strategy_config.indicators != data_plane.indicator_config:
            raise ValueError("data_plane no coincide en indicator_config")
        if (
            self.instrument is not None
            and data_plane.instrument is not None
            and self.instrument != data_plane.instrument
        ):
            raise ValueError("data_plane no coincide en instrument")
        missing = [item.name for item in self.timeframes if item.name not in data_plane.candles]
        if missing:
            raise ValueError(f"data_plane no contiene temporalidades: {missing}")

    def attach_data_plane(self, data_plane: SharedDataPlane) -> None:
        """Attach this strategy state to an already-built shared plane.

        Existing frames are copied as bounded references, not recalculated.
        The strategy keeps its own mutable detector/episode/signal state while
        indicator engines remain owned by ``data_plane``.
        """

        if not isinstance(data_plane, SharedDataPlane):
            raise TypeError("data_plane debe ser SharedDataPlane")
        self._validate_data_plane(data_plane)
        self.data_plane = data_plane
        self.instrument = self.instrument or data_plane.instrument
        self.quote_coverage_mode = data_plane.quote_coverage_mode
        self.max_quote_gap_seconds = data_plane.max_quote_gap_seconds
        self.historical_calendar = data_plane.historical_calendar
        self.aggregators = {tf.name: data_plane.aggregators[tf.name] for tf in self.timeframes}
        self.indicator_engines = {tf.name: data_plane.indicator_engines[tf.name] for tf in self.timeframes}
        for name in self.candles:
            self.candles[name].clear()
            self.indicator_points[name].clear()
            self._logical_candle_keys[name].clear()
            self._candle_by_key[name].clear()
            self._processed_candle_starts[name].clear()
            self._point_index_by_start[name].clear()
            self._point_base_index[name] = 0
            for candle, point in zip(
                data_plane.candles.get(name, ()), data_plane.indicator_points.get(name, ()), strict=True
            ):
                self._append_precomputed_frame(candle, point, watermark=data_plane.last_available_at)
        self._seen_event_ids = set(data_plane._seen_event_ids)
        self._events = dict(data_plane._events)
        self.last_event_time = data_plane.last_event_time
        self.last_event_id = data_plane.last_event_id
        self.last_available_at = data_plane.last_available_at
        self.events_processed = data_plane.events_processed
        self.candles_processed = sum(len(values) for values in self.candles.values())
        self.indicator_updates = {name: data_plane.indicator_updates.get(name, 0) for name in self.timeframes_by_name}

    @property
    def timeframes_by_name(self) -> tuple[str, ...]:
        return tuple(item.name for item in self.timeframes)

    def _precomputed_issue(self, code: str, message: str, candle: Candle | None = None) -> RuntimeIssue:
        return self._issue(code, message, record=candle)

    def _precomputed_identity_issue(self, frame: PrecomputedFrame) -> RuntimeIssue | None:
        candle = frame.candle
        name = candle.timeframe_name
        if name not in self.candles:
            return self._precomputed_issue("timeframe_not_configured", f"Temporalidad no configurada: {name}", candle)
        if candle.mode is not self.mode:
            return self._precomputed_issue(
                "mode_mismatch", f"Se esperaba modo {self.mode.value}, llegó {candle.mode.value}", candle
            )
        if self.instrument is not None and candle.instrument != self.instrument:
            return self._precomputed_issue(
                "instrument_mismatch", f"Se esperaba {self.instrument}, llegó {candle.instrument}", candle
            )
        if candle.price_base is not self.price_base:
            return self._precomputed_issue(
                "price_base_mismatch", f"Se esperaba {self.price_base.value}, llegó {candle.price_base.value}", candle
            )
        return None

    def _precomputed_point_issue(self, frame: PrecomputedFrame) -> RuntimeIssue | None:
        candle, point = frame.candle, frame.indicator_point
        if not isinstance(point, IndicatorPoint):
            return self._precomputed_issue("indicator_point_invalid", "indicator_point debe ser IndicatorPoint", candle)
        if point.start != candle.start or point.end != candle.end:
            return self._precomputed_issue(
                "indicator_point_incompatible", "IndicatorPoint no coincide temporalmente con Candle", candle
            )
        if point.candle_id not in (None, candle.candle_id):
            return self._precomputed_issue(
                "indicator_point_incompatible", "IndicatorPoint no coincide en candle_id", candle
            )
        if point.closed != candle.closed:
            return self._precomputed_issue(
                "indicator_point_incompatible", "IndicatorPoint no coincide en closed", candle
            )
        return None

    def _precomputed_availability_issue(
        self, frame: PrecomputedFrame, *, watermark: datetime | None
    ) -> RuntimeIssue | None:
        point_at = point_available(frame.indicator_point)
        limit = utc(watermark) if watermark is not None else utc(frame.candle.available_at or frame.candle.end)
        if point_at is None or limit is None or point_at > limit:
            return self._precomputed_issue(
                "indicator_point_future", "IndicatorPoint posterior al watermark de entrada", frame.candle
            )
        return None

    def _precomputed_order_issue(self, frame: PrecomputedFrame) -> RuntimeIssue | None:
        candle = frame.candle
        name = candle.timeframe_name
        logical = self._logical_key(candle)
        if logical in self._candle_by_key[name]:
            return self._precomputed_issue(
                "duplicate_candle", f"Candle precomputada repetida: {candle.candle_id}", candle
            )
        if self.candles[name] and candle.start <= self.candles[name][-1].start:
            return self._precomputed_issue(
                "out_of_order_candle", f"Candle precomputada fuera de orden: {candle.start.isoformat()}", candle
            )
        return None

    def _append_precomputed_frame(
        self,
        candle: Candle,
        point: IndicatorPoint,
        *,
        watermark: datetime | None,
    ) -> None:
        name = candle.timeframe_name
        logical = self._logical_key(candle)
        self._logical_candle_keys[name].add(logical)
        self._add_candle_id(candle.candle_id)
        (self._derived_candle_keys if self._is_derived(candle) else self._native_candle_keys).add(logical)
        self._append_bounded(name, candle, point)
        self._processed_candle_starts[name].add(candle.start)
        self._point_index_by_start[name][candle.start] = (
            self._point_base_index[name] + len(self.indicator_points[name]) - 1
        )
        self.indicator_updates[name] = self.indicator_updates.get(name, 0) + 1
        self.candles_processed += 1
        if watermark is not None:
            self.last_available_at = (
                watermark if self.last_available_at is None else max(self.last_available_at, watermark)
            )

    def _validate_precomputed_frame(
        self,
        frame: PrecomputedFrame,
        *,
        watermark: datetime | None,
    ) -> RuntimeIssue | None:
        issue = self._precomputed_identity_issue(frame)
        if issue is not None:
            return issue
        issue = self._precomputed_point_issue(frame)
        if issue is not None:
            return issue
        issue = self._precomputed_availability_issue(frame, watermark=watermark)
        if issue is not None:
            return issue
        issue = self._precomputed_order_issue(frame)
        if issue is not None:
            return issue
        return None

    def _resolve_precomputed_input(
        self,
        value: DataPlaneResult | PrecomputedFrame | Candle | Iterable[PrecomputedFrame],
        indicator_point: IndicatorPoint | None,
        event: MarketEvent | None,
        watermark: datetime | None,
    ) -> tuple[
        DataPlaneResult | None,
        MarketEvent | None,
        tuple[PrecomputedFrame, ...],
        datetime | None,
        ProcessResult | None,
    ]:
        source_result: DataPlaneResult | None = value if isinstance(value, DataPlaneResult) else None
        source_event = source_result.event if source_result is not None else event
        if source_result is not None:
            if not source_result.accepted:
                return (
                    source_result,
                    source_event,
                    (),
                    watermark,
                    ProcessResult(
                        False, events=(source_event,) if source_event is not None else (), issues=source_result.issues
                    ),
                )
            return source_result, source_event, source_result.frames, watermark or source_result.last_available_at, None
        if isinstance(value, PrecomputedFrame):
            return None, source_event, (value,), watermark, None
        if isinstance(value, Candle):
            if indicator_point is None:
                issue = self._precomputed_issue(
                    "indicator_point_required", "Candle precomputada requiere IndicatorPoint", value
                )
                return None, source_event, (), watermark, ProcessResult(False, issues=(issue,))
            return None, source_event, (PrecomputedFrame(value, indicator_point),), watermark, None
        return None, source_event, tuple(cast(Iterable[PrecomputedFrame], value)), watermark, None

    def _remember_precomputed_event(self, source_event: MarketEvent | None) -> ProcessResult | None:
        if source_event is None:
            return None
        if source_event.mode is not self.mode:
            issue = self._issue(
                "mode_mismatch",
                f"Se esperaba modo {self.mode.value}, llegó {source_event.mode.value}",
                record=source_event,
            )
            return ProcessResult(False, events=(source_event,), issues=(issue,))
        if self.instrument is None:
            self.instrument = source_event.instrument
        if source_event.instrument != self.instrument:
            issue = self._issue(
                "instrument_mismatch",
                f"Se esperaba {self.instrument}, llegó {source_event.instrument}",
                record=source_event,
            )
            return ProcessResult(False, events=(source_event,), issues=(issue,))
        event_id = _event_identity(source_event)
        if event_id in self._seen_event_ids:
            issue = self._issue("duplicate_event", f"Evento repetido: {event_id}", record=source_event)
            return ProcessResult(False, events=(source_event,), issues=(issue,))
        self._remember_event(source_event)
        return None

    def _accept_precomputed_frames(
        self,
        frames: tuple[PrecomputedFrame, ...],
        *,
        source_result: DataPlaneResult | None,
        watermark: datetime | None,
    ) -> tuple[list[Candle], list[RuntimeIssue], bool]:
        emitted: list[Candle] = []
        issues: list[RuntimeIssue] = list(source_result.issues) if source_result is not None else []
        trigger_closed = False
        trigger_timeframe = self._trigger_timeframe_name()
        for frame in frames:
            if frame.candle.timeframe_name not in self.candles and source_result is not None:
                # A shared plane can contain frames not required by this
                # strategy profile; they remain owned by the plane.
                continue
            issue = self._validate_precomputed_frame(frame, watermark=watermark)
            if issue is not None:
                issues.append(issue)
                continue
            self._append_precomputed_frame(frame.candle, frame.indicator_point, watermark=watermark)
            emitted.append(frame.candle)
            quality_issue: RuntimeIssue | None = None
            if not frame.candle.quality.usable:
                quality_issue = self._issue(
                    "quality_blocked",
                    f"Calidad no admisible en {frame.candle.timeframe_name}: {frame.candle.quality.status}",
                    record=frame.candle,
                )
                issues.append(quality_issue)
            if quality_issue is None and frame.candle.timeframe_name == trigger_timeframe and frame.candle.closed:
                trigger_closed = True
        return emitted, issues, trigger_closed

    def feed_precomputed(
        self,
        value: DataPlaneResult | PrecomputedFrame | Candle | Iterable[PrecomputedFrame],
        indicator_point: IndicatorPoint | None = None,
        *,
        event: MarketEvent | None = None,
        watermark: datetime | None = None,
        evaluate_strategy: bool = True,
    ) -> ProcessResult:
        """Consume validated candle/indicator frames without recalculation.

        ``DataPlaneResult`` is the normal multi-strategy path.  A single
        ``Candle`` plus ``IndicatorPoint`` is also accepted for provider
        adapters and tests.  A frame whose timestamp, identity or availability
        is incompatible is rejected before strategy state changes.
        """

        self._begin_transition()
        source_result, source_event, frames, watermark, early = self._resolve_precomputed_input(
            value, indicator_point, event, watermark
        )
        if early is not None:
            return early
        early = self._remember_precomputed_event(source_event)
        if early is not None:
            return early
        emitted, issues, trigger_closed = self._accept_precomputed_frames(
            frames, source_result=source_result, watermark=watermark
        )
        evaluations: tuple[Any, ...] = ()
        signals: tuple[Signal, ...] = ()
        blocked = any(issue.code in _BLOCKING_RUNTIME_ISSUES for issue in issues)
        if evaluate_strategy and trigger_closed and not blocked:
            evaluations, signals = self._evaluate_strategy()
        observation_allowed = source_result.accepted if source_result is not None else True
        completed = list(self._observe_event(source_event)) if source_event is not None and observation_allowed else []
        return ProcessResult(
            bool(source_result.accepted if source_result is not None else True)
            and not any(
                issue.code
                in {
                    "indicator_point_invalid",
                    "indicator_point_incompatible",
                    "indicator_point_future",
                    "timeframe_not_configured",
                }
                for issue in issues
            ),
            events=(source_event,) if source_event is not None else (),
            candles=tuple(emitted),
            evaluations=evaluations,
            signals=signals,
            simulations=tuple(completed),
            pending_simulations=self.pending_simulations,
            issues=tuple(issues),
            consumer_events=tuple(self._transition_consumer_events),
        )

    @property
    def consumer_events(self) -> tuple[SignalConsumerEvent, ...]:
        """Ventana acotada de deltas recientes del consumidor."""

        return tuple(self._consumer_events)

    @property
    def consumer_checkpoint(self) -> dict[str, Any]:
        """Checkpoint serializable del consumidor de producto."""

        return self.signal_consumer.checkpoint().to_dict()

    def _begin_transition(self) -> None:
        self._transition_consumer_events = []

    def _apply_consumer_result(self, result: SignalConsumerResult) -> tuple[PendingSimulation, ...]:
        self._consumer_pending = tuple(result.pending_simulations)
        for event in result.events:
            self._consumer_events.append(event)
            self._transition_consumer_events.append(event)
            self._consumer_event_count += 1
        completed = tuple(result.completed_simulations)
        if completed:
            self.completed_simulations.extend(completed)
            if self.max_candles is not None:
                del self.completed_simulations[: -self.max_candles]
        return completed

    def _dispatch_signal(self, signal: Signal) -> SignalConsumerResult:
        return self.signal_consumer.on_signal(signal)

    def _dispatch_observation(
        self,
        observation: PriceObservation,
        *,
        watermark: datetime,
    ) -> tuple[PendingSimulation, ...]:
        return self._apply_consumer_result(self.signal_consumer.on_observation(observation, watermark=watermark))

    def _dispatch_advance(
        self,
        watermark: datetime,
        *,
        capture_complete: bool,
    ) -> tuple[PendingSimulation, ...]:
        return self._apply_consumer_result(self.signal_consumer.advance(watermark, capture_complete=capture_complete))

    @property
    def status(self) -> dict[str, Any]:
        result = {
            "mode": self.mode.value,
            "instrument": self.instrument,
            "source": self.source,
            "price_base": self.price_base.value,
            "market_candidate_id": self.market_candidate_id,
            "consumer_type": self.signal_consumer.consumer_type,
            "consumer_product": getattr(self.signal_consumer, "product", self.signal_consumer.consumer_type),
            "consumer_event_count": self._consumer_event_count,
            "max_candles": self.max_candles,
            "timeframes": [_timeframe_name(tf) for tf in self.timeframes],
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
        if self.data_plane is not None:
            result["shared_data_plane"] = self.data_plane.status
        return result

    def _warmup_pending(self, timeframe: str) -> int:
        points = self.indicator_points.get(timeframe, ())
        if points and points[-1].ready:
            return 0
        required = max(
            self.strategy_config.indicators.ema_slow,
            self.strategy_config.indicators.rsi_period + 1,
            self.strategy_config.indicators.atr_period,
        )
        return max(1, required - len(points))

    def _issue(
        self, code: str, message: str, *, record: Any | None = None, timestamp: datetime | None = None
    ) -> RuntimeIssue:
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
        return (
            str(candle.origin).lower() in {"aggregated", "derived", "resampled_ohlc", "runtime_aggregate"}
            or "aggregated" in str(candle.source).lower()
        )

    def _rebuild_indicator_engine(self, name: str) -> None:
        """Rebuild only after a late native replacement, never per event."""
        from ..core.indicators import IncrementalIndicatorEngine

        engine = IncrementalIndicatorEngine(
            self.strategy_config.indicators, max_points=self.max_candles, historical_calendar=self.historical_calendar
        )
        points = []
        for candle in self.candles[name]:
            points.append(engine.update(candle))
        self.indicator_engines[name] = engine
        self._point_base_index[name] = 0
        self.indicator_points[name] = points if self.max_candles is None else deque(points, maxlen=self.max_candles)
        self._point_index_by_start[name] = {item.start: index for index, item in enumerate(self.indicator_points[name])}

    def _resample_from_candle(self, base_candle: Candle) -> list[ProcessResult]:
        """Build compatible higher OHLC bars; never creates ticks or prices."""
        results: list[ProcessResult] = []
        base_tf = base_candle.normalized_timeframe
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
                    for oldest in sorted(buckets)[: -self.max_candles]:
                        buckets.pop(oldest, None)
                continue
            group = [bucket[start] for start in expected]
            if any(not item.closed or not item.quality.usable for item in group):
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
                metadata={
                    "built_from": "closed_ohlc_bars",
                    "base_timeframe": base_tf.name,
                    "ratio": ratio,
                    "no_ticks_invented": True,
                },
            )
            buckets.pop(bucket_start, None)
            results.append(self._accept_candle(derived, native=False, evaluate_strategy=False))
        return results

    def _remember_id(self, value: str, values: set[str], order: deque[str]) -> None:
        if value in values:
            return
        values.add(value)
        order.append(value)
        if self.max_candles is None:
            return
        limit = max(self.max_candles, self.max_candles * max(1, len(self.timeframes)))
        while len(order) > limit:
            values.discard(order.popleft())

    def _prune_episodes(self) -> None:
        if self.max_candles is None or len(self.episodes) <= self.max_candles:
            return
        active = [
            item
            for item in self.episodes.values()
            if not getattr(item, "invalidated", False) and not getattr(item, "used", False)
        ]
        keep_ids = {
            item.episode_id for item in sorted(active, key=lambda item: item.registered_at)[-self.max_candles :]
        }
        ordered = sorted(self.episodes.values(), key=lambda item: item.registered_at)
        for item in ordered:
            if len(self.episodes) <= self.max_candles or item.episode_id in keep_ids:
                continue
            self.episodes.pop(item.episode_id, None)

    def _add_candle_id(self, candle_id: str | None) -> None:
        key = str(candle_id) if candle_id is not None else ""
        self._candle_id_counts[key] = self._candle_id_counts.get(key, 0) + 1
        self._candle_ids.add(candle_id)

    def _remove_candle_id(self, candle_id: str | None) -> None:
        key = str(candle_id) if candle_id is not None else ""
        count = self._candle_id_counts.get(key, 0) - 1
        if count > 0:
            self._candle_id_counts[key] = count
        else:
            self._candle_id_counts.pop(key, None)
            self._candle_ids.discard(candle_id)

    def _replace_candle_id(self, old_id: str | None, new_id: str | None) -> None:
        if old_id != new_id:
            self._remove_candle_id(old_id)
            self._add_candle_id(new_id)

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
                self._remove_candle_id(evicted.candle_id)
                self._point_index_by_start[name].pop(evicted.start, None)
                self._point_base_index[name] += 1
            candles.append(candle)
            self.indicator_points[name].append(point)
        self._candle_by_key[name][self._logical_key(candle)] = candle

    def _append_signal(self, signal: Signal) -> None:
        if self.max_candles is not None and len(self.signals) >= self.max_candles * max(1, len(self.timeframes)):
            del self.signals[0]
        self.signals.append(signal)

    def _append_evaluation(self, evaluation: Any) -> None:
        if self.max_candles is not None and len(self.evaluations) >= self.max_candles * max(1, len(self.timeframes)):
            del self.evaluations[0]
        self.evaluations.append(evaluation)

    def _append_issue(self, issue: RuntimeIssue) -> None:
        if self.max_candles is not None and len(self.issues) >= self.max_candles * max(1, len(self.timeframes)):
            del self.issues[0]
        self.issues.append(issue)

    def _candle_acceptance_issue(self, candle: Candle, timeframe: str) -> RuntimeIssue | None:
        if timeframe not in self.candles:
            return self._issue(
                "timeframe_not_configured",
                f"Temporalidad no configurada: {timeframe}",
                record=candle,
            )
        if not self._ensure_instrument(candle.instrument):
            return self._issue(
                "instrument_mismatch",
                f"Se esperaba {self.instrument}, llegó {candle.instrument}",
                record=candle,
            )
        if candle.price_base is not self.price_base:
            return self._issue(
                "price_base_mismatch",
                f"Se esperaba {self.price_base.value}, llegó {candle.price_base.value}",
                record=candle,
            )
        return None

    def _replace_existing_candle(
        self,
        timeframe: str,
        logical_key: tuple[str, datetime],
        existing: Candle,
        candle: Candle,
    ) -> None:
        rows = self.candles[timeframe]
        for index, previous in enumerate(rows):
            if self._logical_key(previous) == logical_key:
                rows[index] = candle
                break
        self._replace_candle_id(existing.candle_id, candle.candle_id)
        self._native_candle_keys.add(logical_key)
        self._derived_candle_keys.discard(logical_key)
        self._candle_by_key[timeframe][logical_key] = candle
        self._rebuild_indicator_engine(timeframe)
        self.indicator_updates[timeframe] = self.indicator_updates.get(timeframe, 0) + 1

    def _replacement_result(
        self,
        candle: Candle,
        *,
        timeframe: str,
        logical_key: tuple[str, datetime],
        existing: Candle,
        native: bool,
        evaluate_strategy: bool,
    ) -> ProcessResult:
        existing_revision = int(existing.metadata.get("revision", 0) or 0)
        new_revision = int(candle.metadata.get("revision", 0) or 0)
        if native and not self._is_derived(candle) and new_revision > existing_revision:
            issue_code = "candle_revision"
            issue_message = (
                f"Revisión nativa {new_revision} reemplazó {existing_revision} en {timeframe}: "
                f"{candle.start.isoformat()}"
            )
        elif native and self._is_derived(existing) and not self._is_derived(candle):
            issue_code = "native_preferred"
            issue_message = f"Vela nativa reemplazó derivada en {timeframe}: {candle.start.isoformat()}"
        else:
            issue = self._issue(
                "duplicate_candle", f"Vela duplicada o revisión ya observada: {candle.candle_id}", record=candle
            )
            return ProcessResult(False, issues=(issue,))
        self._replace_existing_candle(timeframe, logical_key, existing, candle)
        evaluations, signals = self._evaluate_trigger(candle, timeframe, evaluate_strategy)
        issue = self._issue(issue_code, issue_message, record=candle)
        return ProcessResult(
            True,
            candles=(candle,),
            evaluations=evaluations,
            signals=signals,
            pending_simulations=self.pending_simulations,
            issues=(issue,),
        )

    def _evaluate_trigger(
        self, candle: Candle, timeframe: str, evaluate_strategy: bool
    ) -> tuple[tuple[Any, ...], tuple[Signal, ...]]:
        trigger_timeframe = self._trigger_timeframe_name()
        if evaluate_strategy and timeframe == trigger_timeframe and candle.closed:
            return self._evaluate_strategy()
        return (), ()

    def _trigger_timeframe_name(self) -> str:
        if self.market_candidate_id != "trend_pullback_v1":
            from ..core.market_profiles import market_profile

            return market_profile(self.market_candidate_id).trigger_timeframe
        return _timeframe_name(self.strategy_config.trigger_timeframe)

    def _append_new_candle(
        self,
        candle: Candle,
        *,
        timeframe: str,
        logical_key: tuple[str, datetime],
        native: bool,
        evaluate_strategy: bool,
        quality_issue: RuntimeIssue | None,
    ) -> ProcessResult:
        self._logical_candle_keys[timeframe].add(logical_key)
        self._add_candle_id(candle.candle_id)
        (self._native_candle_keys if native else self._derived_candle_keys).add(logical_key)
        self.candles_processed += 1
        point = self.indicator_engines[timeframe].update(candle)
        self._append_bounded(timeframe, candle, point)
        self._point_index_by_start[timeframe][candle.start] = (
            self._point_base_index[timeframe] + len(self.indicator_points[timeframe]) - 1
        )
        self.indicator_updates[timeframe] = self.indicator_updates.get(timeframe, 0) + 1
        context_timeframe, preparation_timeframe, _trigger_timeframe = _strategy_timeframe_names(self.strategy_config)
        if timeframe in {context_timeframe, preparation_timeframe}:
            self._strategy_dirty = True
        evaluations, signals = self._evaluate_trigger(candle, timeframe, evaluate_strategy and quality_issue is None)
        return ProcessResult(
            True,
            candles=(candle,),
            evaluations=evaluations,
            signals=signals,
            pending_simulations=self.pending_simulations,
            issues=(quality_issue,) if quality_issue is not None else (),
        )

    def _accept_candle(self, candle: Candle, *, native: bool, evaluate_strategy: bool = True) -> ProcessResult:
        tf_name = candle.timeframe_name
        rejection = self._candle_acceptance_issue(candle, tf_name)
        if rejection is not None:
            return ProcessResult(False, issues=(rejection,))
        quality_issue: RuntimeIssue | None = None
        if not candle.quality.usable:
            quality_issue = self._issue(
                "quality_blocked",
                f"Calidad no admisible en {tf_name}: {candle.quality.status}",
                record=candle,
            )
        logical_key = self._logical_key(candle)
        existing = self._candle_by_key[tf_name].get(logical_key)
        if existing is not None:
            # Native provider OHLC has precedence over a derived OHLC bar. If
            # it arrives after a derived bar, replace the effective value for
            # future calculations and rebuild that timeframe once; prior
            # decisions remain immutable and are not rewritten.
            return self._replacement_result(
                candle,
                timeframe=tf_name,
                logical_key=logical_key,
                existing=existing,
                native=native,
                evaluate_strategy=evaluate_strategy,
            )
        if candle.candle_id in self._candle_ids:
            issue = self._issue(
                "duplicate_candle", f"Vela duplicada o revisión ya observada: {candle.candle_id}", record=candle
            )
            return ProcessResult(False, issues=(issue,))
        if self.candles[tf_name] and candle.start <= self.candles[tf_name][-1].start:
            issue = self._issue(
                "out_of_order_candle", f"Vela fuera de orden en {tf_name}: {candle.start.isoformat()}", record=candle
            )
            return ProcessResult(False, issues=(issue,))
        return self._append_new_candle(
            candle,
            timeframe=tf_name,
            logical_key=logical_key,
            native=native,
            evaluate_strategy=evaluate_strategy,
            quality_issue=quality_issue,
        )

    def _strategy_window_indices(self, name: str, as_of: datetime, *, lookback: int) -> list[int]:
        points = self.indicator_points.get(name, ())
        if not points:
            return []
        # Runtime input is monotonic, so inspect only the short future tail
        # rather than scanning the complete historical series per trigger.
        latest = len(points) - 1
        while latest >= 0:
            available = point_available(points[latest])
            if available is None or available <= as_of:
                break
            latest -= 1
        if latest < 0:
            return []
        first = max(0, latest - lookback)
        # Sólo el episodio más reciente puede afectar el disparador actual;
        # conservar todos los episodios usados volvería a ampliar la ventana
        # hasta el inicio del histórico.
        by_start = self._point_index_by_start.get(name, {})
        current = [
            episode
            for episode in self.episodes.values()
            if not episode.invalidated and episode.registered_at <= as_of and episode.expires_at >= as_of
        ]
        if current:
            episode = max(current, key=lambda item: item.registered_at)
            absolute_index = by_start.get(episode.preparation_start)
            index = (absolute_index - self._point_base_index.get(name, 0)) if absolute_index is not None else None
            if index is not None and index <= latest:
                first = min(first, max(0, index - lookback))
        return list(range(first, latest + 1))

    def _bounded_strategy_streams(self, as_of: datetime) -> dict[str, IndicatorSeries]:
        context_tf, preparation_tf, trigger_tf = _strategy_timeframe_names(self.strategy_config)
        requests = {
            context_tf: self.strategy_config.context_lookback,
            preparation_tf: max(self.strategy_config.preparation_lookback, self.strategy_config.preparation_ttl_bars)
            + self.strategy_config.preparation_lookback,
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
            streams[name] = IndicatorSeries(
                engine.series.timeframe, engine.series.instrument, self.strategy_config.indicators, points
            )
            self.last_strategy_window_sizes[name] = len(points)
        return streams

    def _strategy_warmup_ready(self) -> bool:
        context_tf, preparation_tf, trigger_tf = _strategy_timeframe_names(self.strategy_config)
        context = self.indicator_points.get(context_tf, ())
        preparation = self.indicator_points.get(preparation_tf, ())
        trigger = self.indicator_points.get(trigger_tf, ())
        if (
            len(context) <= self.strategy_config.context_lookback
            or len(preparation) <= self.strategy_config.preparation_lookback
            or len(trigger) < 2
        ):
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
        trigger = self.indicator_points.get(_timeframe_name(self.strategy_config.trigger_timeframe), ())
        if len(trigger) < 2:
            return False
        current, previous = trigger[-1], trigger[-2]
        if not current.closed or not previous.closed:
            return False
        if any(value is None for value in (current.close, current.ema_fast, previous.close, previous.ema_fast)):
            return False
        return bool(
            (previous.close <= previous.ema_fast < current.close)
            or (previous.close >= previous.ema_fast > current.close)
        )

    def _record_strategy_evaluations(self, result: StrategyResult) -> tuple[Any, ...]:
        new_evaluations: list[Any] = []
        for evaluation in result.evaluations:
            decision_id = _decision_id(evaluation)
            if decision_id in self._decision_ids:
                continue
            self._remember_id(decision_id, self._decision_ids, self._decision_order)
            self._append_evaluation(evaluation)
            new_evaluations.append(evaluation)
        return tuple(new_evaluations)

    def _merge_strategy_episodes(self, result: StrategyResult) -> None:
        # Conservar primero los episodios que el nuevo resultado reobserva;
        # después las señales marcan ``used`` sin que un snapshot posterior lo
        # vuelva a poner en falso.
        for episode in result.episodes:
            previous = self.episodes.get(episode.episode_id)
            self.episodes[episode.episode_id] = (
                replace(episode, used=True) if previous is not None and previous.used else episode
            )

    def _record_strategy_signals(self, result: StrategyResult) -> tuple[Signal, ...]:
        new_signals: list[Signal] = []
        for signal in result.signals:
            if signal.signal_id in self._signal_ids:
                continue
            # The episode state is authoritative across bounded strategy
            # windows and checkpoints; a new trigger identity cannot reopen
            # an episode already consumed by an earlier signal.
            previous_episode = self.episodes.get(signal.episode_id)
            if previous_episode is not None and previous_episode.used:
                continue
            self._remember_id(signal.signal_id, self._signal_ids, self._signal_order)
            self._append_signal(signal)
            new_signals.append(signal)
            self._apply_consumer_result(self._dispatch_signal(signal))
            if signal.episode_id in self.episodes:
                self.episodes[signal.episode_id] = replace(self.episodes[signal.episode_id], used=True)
        return tuple(new_signals)

    def _update_strategy_context(self, result: StrategyResult) -> None:
        # A compact current-context view for status/UI; detailed decisions stay
        # in ``evaluations``.
        latest = next(
            (
                item
                for item in reversed(result.evaluations)
                if item.stage == "trigger" and item.values.get("context") is not None
            ),
            None,
        )
        if latest is not None:
            self.context = {
                "direction": latest.direction,
                "values": latest.values.get("context"),
                "timestamp": latest.timestamp,
            }

    def _evaluate_strategy(self) -> tuple[tuple[Any, ...], tuple[Signal, ...]]:
        if self._market_strategy is not None:
            return self._evaluate_market_strategy()
        trigger_timeframe = self._trigger_timeframe_name()
        trigger_points = self.indicator_points.get(trigger_timeframe, ())
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
        new_evaluations = self._record_strategy_evaluations(result)
        self._merge_strategy_episodes(result)
        new_signals = self._record_strategy_signals(result)
        self._update_strategy_context(result)
        self._prune_episodes()
        return new_evaluations, new_signals

    def _evaluate_market_strategy(self) -> tuple[tuple[Any, ...], tuple[Signal, ...]]:
        """Evaluate an opt-in non-baseline strategy over bounded closed bars.

        The processor still owns aggregation and indicator updates.  The
        selected strategy receives only its target timeframe's bounded bars;
        its output is converted into the canonical ``Evaluation``/``Signal``
        types so checkpoints, consumers and callers retain one public seam.
        """

        from ..core.market_profiles import market_profile

        profile = market_profile(self.market_candidate_id)
        target = profile.trigger_timeframe
        bars = tuple(self.candles.get(target, ()))
        if not bars:
            return (), ()
        strategy = self._market_strategy
        if strategy is None:
            raise ValueError("la estrategia de mercado seleccionada no está construida")
        output = strategy.evaluate_causal({target: bars})
        raw_explanations = tuple(getattr(output, "explanations", ()))
        evaluations: list[Evaluation] = []
        for explanation in raw_explanations[-1:]:
            decision_text = str(getattr(explanation, "decision", "blocked")).lower()
            try:
                decision = DecisionKind(decision_text)
            except ValueError:
                decision = DecisionKind.BLOCKED
            evaluations.append(
                Evaluation(
                    timestamp=explanation.timestamp,
                    available_at=explanation.available_at,
                    instrument=str(explanation.values.get("instrument", self.instrument or "unknown")),
                    stage="donchian",
                    decision=decision,
                    direction=explanation.direction,
                    conditions=(),
                    values=dict(explanation.values),
                    reasons=tuple(explanation.reasons),
                    episode_id=None,
                    mode=self.mode,
                    quality=DataQuality.good(),
                )
            )
        core_signals = tuple(self._market_signal_to_core(signal) for signal in output.signals)
        result = StrategyResult(tuple(evaluations), core_signals, ())
        new_evaluations = self._record_strategy_evaluations(result)
        new_signals = self._record_strategy_signals(result)
        self.strategy_evaluations += 1
        return new_evaluations, new_signals

    def _market_signal_to_core(self, signal: Any) -> Signal:
        return Signal(
            signal_id=str(signal.signal_id),
            instrument=str(signal.instrument),
            direction=str(signal.direction),
            detected_at=signal.detected_at,
            context_start=signal.trigger_start,
            preparation_start=signal.trigger_start,
            trigger_start=signal.trigger_start,
            trigger_end=signal.trigger_end,
            episode_id=f"{self.market_candidate_id}:{signal.signal_id}",
            values=dict(getattr(signal, "values", {})),
            mode=self.mode,
            quality=DataQuality.good(),
        )

    def process_market_profile(self) -> tuple[tuple[Any, ...], tuple[Signal, ...]]:
        """Evaluate the selected opt-in market profile on current bars."""

        if self._market_strategy is None:
            raise ValueError("processor no tiene un market_candidate_id challenger")
        return self._evaluate_market_strategy()

    def register_signal(self, signal: Signal) -> tuple[PendingSimulation, ...]:
        """Registra una señal ya validada y crea sus horizontes virtuales."""
        if not isinstance(signal, Signal):
            raise TypeError("register_signal requiere core.Signal")
        aliases = {
            "UP": "UP",
            "LONG": "UP",
            "BUY": "UP",
            "BULL": "UP",
            "DOWN": "DOWN",
            "SHORT": "DOWN",
            "SELL": "DOWN",
            "BEAR": "DOWN",
        }
        direction = aliases.get(str(signal.direction).upper())
        if direction is None:
            raise ValueError(f"dirección desconocida: {signal.direction!r}")
        if direction != signal.direction:
            signal = replace(signal, direction=direction)
        if not self._ensure_instrument(signal.instrument):
            raise ValueError(f"Se esperaba {self.instrument}, llegó {signal.instrument}")
        if signal.signal_id in self._signal_ids:
            return tuple(item for item in self.pending_simulations if item.signal_id == signal.signal_id)
        self._remember_id(signal.signal_id, self._signal_ids, self._signal_order)
        self._append_signal(signal)
        if signal.episode_id in self.episodes:
            self.episodes[signal.episode_id] = replace(self.episodes[signal.episode_id], used=True)
        self._begin_transition()
        result = self._dispatch_signal(signal)
        self._apply_consumer_result(result)
        return result.pending_simulations

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
            instrument=event.instrument,
            observation_id=event.event_id,
            source_sequence=event.sequence,
        )
        return self._dispatch_observation(observation, watermark=observation.available_at)

    def _validate_event(self, event: MarketEvent) -> ProcessResult | None:
        event_id = _event_identity(event)
        if not self._ensure_instrument(event.instrument):
            issue = self._issue(
                "instrument_mismatch", f"Se esperaba {self.instrument}, llegó {event.instrument}", record=event
            )
            return ProcessResult(False, events=(event,), issues=(issue,))
        if event.mode is not self.mode:
            issue = self._issue(
                "mode_mismatch", f"Se esperaba modo {self.mode.value}, llegó {event.mode.value}", record=event
            )
            return ProcessResult(False, events=(event,), issues=(issue,))
        if event_id in self._seen_event_ids:
            issue = self._issue("duplicate_event", f"Evento repetido: {event_id}", record=event)
            return ProcessResult(False, events=(event,), issues=(issue,))
        if self.last_event_time is not None and event.event_time < self.last_event_time:
            issue = self._issue(
                "out_of_order_event", f"Evento fuera de orden: {event.event_time.isoformat()}", record=event
            )
            return ProcessResult(False, events=(event,), issues=(issue,))
        return None

    def _remember_event(self, event: MarketEvent) -> None:
        event_id = _event_identity(event)
        self._seen_event_ids.add(event_id)
        self._events[event_id] = event
        if self.max_candles is not None and len(self._events) > self.max_candles * max(1, len(self.timeframes)):
            oldest_id = next(iter(self._events))
            self._events.pop(oldest_id, None)
            self._seen_event_ids.discard(oldest_id)
        self.events_processed += 1
        self.last_event_id = event_id
        self.last_event_time = (
            event.event_time if self.last_event_time is None else max(self.last_event_time, event.event_time)
        )
        self.last_available_at = (
            event.effective_available_at
            if self.last_available_at is None
            else max(self.last_available_at, event.effective_available_at)
        )

    def _collect_aggregation_issues(
        self,
        raw_issues: Iterable[Any],
        seen_keys: set[tuple[str, str, str | None]],
    ) -> tuple[list[RuntimeIssue], bool]:
        issues: list[RuntimeIssue] = []
        blocked = False
        for item in raw_issues:
            issue_key = (item.code, item.message, item.record_id)
            if issue_key in seen_keys:
                continue
            seen_keys.add(issue_key)
            issue = RuntimeIssue(item.code, item.message, item.timestamp, item.record_id)
            issues.append(issue)
            self._append_issue(issue)
            blocked = blocked or issue.code in _BLOCKING_RUNTIME_ISSUES
        return issues, blocked

    def _collect_aggregation_candles(
        self, raw_candles: Iterable[Candle], *, trigger_timeframe: str
    ) -> tuple[list[Candle], list[RuntimeIssue], bool, bool]:
        emitted: list[Candle] = []
        issues: list[RuntimeIssue] = []
        blocked = False
        trigger_closed = False
        for candle in raw_candles:
            accepted = self._accept_candle(candle, native=False, evaluate_strategy=False)
            if accepted.accepted:
                emitted.extend(accepted.candles)
                if candle.timeframe_name == trigger_timeframe and candle.closed:
                    trigger_closed = True
            issues.extend(accepted.issues)
            blocked = blocked or any(issue.code in _BLOCKING_RUNTIME_ISSUES for issue in accepted.issues)
        return emitted, issues, blocked, trigger_closed

    def _aggregate_timeframe(
        self,
        event: MarketEvent,
        timeframe: Timeframe,
        seen_issue_keys: set[tuple[str, str, str | None]],
        *,
        trigger_timeframe: str,
    ) -> tuple[bool, list[Candle], list[RuntimeIssue], bool, bool]:
        result = self.aggregators[timeframe.name].add(event)
        aggregation_issues, issue_blocked = self._collect_aggregation_issues(result.issues, seen_issue_keys)
        emitted, candle_issues, candle_blocked, trigger_closed = self._collect_aggregation_candles(
            result.emitted, trigger_timeframe=trigger_timeframe
        )
        return (
            result.accepted,
            emitted,
            [*aggregation_issues, *candle_issues],
            issue_blocked or candle_blocked,
            trigger_closed,
        )

    def _aggregate_event(self, event: MarketEvent) -> tuple[bool, bool, bool, list[Candle], list[RuntimeIssue]]:
        aggregation_accepted = True
        aggregation_blocked = False
        trigger_closed = False
        emitted: list[Candle] = []
        issues: list[RuntimeIssue] = []
        seen_issue_keys: set[tuple[str, str, str | None]] = set()
        trigger_timeframe = self._trigger_timeframe_name()
        # Contexto primero (M15 > M5 > M1) cuando varias temporalidades
        # cierran al recibir un evento de frontera.
        for timeframe in sorted(self.timeframes, key=lambda item: item.seconds, reverse=True):
            accepted, candles, current_issues, blocked, closed = self._aggregate_timeframe(
                event,
                timeframe,
                seen_issue_keys,
                trigger_timeframe=trigger_timeframe,
            )
            aggregation_accepted = aggregation_accepted and accepted
            aggregation_blocked = aggregation_blocked or blocked
            trigger_closed = trigger_closed or closed
            emitted.extend(candles)
            issues.extend(current_issues)
        return aggregation_accepted, aggregation_blocked, trigger_closed, emitted, issues

    def process_event(self, record: Any, *, evaluate_strategy: bool = True) -> ProcessResult:
        """Consume one event; aggregate all temporalities in close order.

        ``evaluate_strategy=False`` is used only for bootstrap/recovery history:
        it still warms aggregators, indicators and the observation book but does
        not emit historical decisions or signals.
        """

        if self.data_plane is not None:
            return self.feed_precomputed(
                self.data_plane.feed_event(record),
                evaluate_strategy=evaluate_strategy,
            )

        self._begin_transition()
        try:
            event = to_core_event(record, mode=self.mode)
        except Exception as exc:
            issue = self._issue("event_invalid", str(exc), record=record)
            return ProcessResult(False, issues=(issue,))
        rejection = self._validate_event(event)
        if rejection is not None:
            return rejection
        self._remember_event(event)
        aggregation_accepted, aggregation_blocked, trigger_closed, emitted, issues = self._aggregate_event(event)
        evaluations: tuple[Any, ...] = ()
        signals: tuple[Signal, ...] = ()
        if evaluate_strategy and aggregation_accepted and not aggregation_blocked and trigger_closed:
            evaluations, signals = self._evaluate_strategy()
        # Un evento que no pudo entrar de forma coherente en todas las
        # temporalidades no puede resolver simulaciones ni contarse como
        # entrada aceptada, aunque se conserve como evidencia capturada.
        completed = list(self._observe_event(event)) if aggregation_accepted else []
        return ProcessResult(
            aggregation_accepted,
            events=(event,),
            candles=tuple(emitted),
            evaluations=evaluations,
            signals=signals,
            simulations=tuple(completed),
            pending_simulations=self.pending_simulations,
            issues=tuple(issues),
            consumer_events=tuple(self._transition_consumer_events),
        )

    # Common aliases used by replay/observation coordinators.
    update = process_event
    on_event = process_event

    def process_bar(self, record: Any, *, evaluate_strategy: bool = True) -> ProcessResult:
        """Consume one native candle and derive compatible higher OHLC bars.

        ``evaluate_strategy=False`` is reserved for bootstrap/recovery history:
        it warms indicators and continuity but cannot emit historical signals.
        """

        if self.data_plane is not None:
            return self.feed_precomputed(
                self.data_plane.feed_candle(record),
                evaluate_strategy=evaluate_strategy,
            )

        self._begin_transition()

        try:
            candle = to_core_candle(record, mode=self.mode)
        except Exception as exc:
            issue = self._issue("candle_invalid", str(exc), record=record)
            return ProcessResult(False, issues=(issue,))
        if candle.mode is not self.mode:
            issue = self._issue(
                "mode_mismatch", f"Se esperaba modo {self.mode.value}, llegó {candle.mode.value}", record=candle
            )
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
        if (
            evaluate_strategy
            and candle.timeframe_name == self._trigger_timeframe_name()
            and candle.closed
            and not any(issue.code in _BLOCKING_RUNTIME_ISSUES for issue in all_issues)
        ):
            evaluations, signals = self._evaluate_strategy()
            all_evaluations.extend(evaluations)
            all_signals.extend(signals)
        obs = PriceObservation(
            candle.end,
            max(available, candle.end),
            candle.close,
            candle.price_base.value,
            candle.source,
            candle.timeframe_name,
            candle.closed,
            candle.quality.status,
            instrument=candle.instrument,
            observation_id=candle.candle_id,
            source_ordinal=self.candles_processed,
        )
        completed = self._dispatch_observation(obs, watermark=obs.available_at)
        return ProcessResult(
            True,
            candles=tuple(all_candles),
            evaluations=tuple(all_evaluations),
            signals=tuple(all_signals),
            simulations=tuple(completed),
            pending_simulations=self.pending_simulations,
            issues=tuple(all_issues),
            consumer_events=tuple(self._transition_consumer_events),
        )

    # Explicit aliases keep the adapter seam readable for providers and UI.
    process_candle = process_bar
    ingest_event = process_event
    ingest_bar = process_bar
    ingest_candle = process_bar
    feed_event = process_event
    feed_bar = process_bar

    def finalize(
        self, watermark: datetime | None = None, *, evaluate_strategy: bool = True, capture_complete: bool = True
    ) -> ProcessResult:
        """Close active buckets at an explicit watermark; no empty candle is made."""
        self._begin_transition()

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
                accepted = self._accept_candle(candle, native=False, evaluate_strategy=evaluate_strategy)
                if accepted.accepted:
                    emitted.extend(accepted.candles)
                    evaluations.extend(accepted.evaluations)
                    signals.extend(accepted.signals)
                issues.extend(accepted.issues)
            for item in result.issues:
                issue = RuntimeIssue(item.code, item.message, item.timestamp, item.record_id)
                issues.append(issue)
                self._append_issue(issue)
        completed = list(self._dispatch_advance(watermark, capture_complete=capture_complete))
        return ProcessResult(
            True,
            candles=tuple(emitted),
            evaluations=tuple(evaluations),
            signals=tuple(signals),
            simulations=tuple(completed),
            pending_simulations=self.pending_simulations,
            issues=tuple(issues),
            consumer_events=tuple(self._transition_consumer_events),
        )

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
            len(self.pending_simulations),
            tuple(issues),
        )

    def _strategy_checkpoint_data(self) -> dict[str, Any]:
        context_timeframe, preparation_timeframe, trigger_timeframe = _strategy_timeframe_names(self.strategy_config)
        return {
            "name": self.strategy_config.name,
            "context_timeframe": context_timeframe,
            "preparation_timeframe": preparation_timeframe,
            "trigger_timeframe": trigger_timeframe,
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

    def _strategy_checkpoint_payload(self) -> dict[str, Any]:
        """Serialize only mutable strategy/consumer state for a shared plane."""

        completed_ids = (
            list(self._simulation_book.completed_order)
            if self._simulation_book is not None
            else [item.simulation_id for item in self.completed_simulations]
        )
        observations = self._simulation_book.observations if self._simulation_book is not None else ()
        return {
            "checkpoint_version": self.CHECKPOINT_VERSION,
            "checkpoint_scope": "strategy",
            "config_hash": _processor_config_hash(self),
            "data_plane_config_hash": self.data_plane.config_hash if self.data_plane is not None else None,
            **_processor_extra_config(self),
            "mode": self.mode.value,
            "instrument": self.instrument,
            "source": self.source,
            "price_base": self.price_base.value,
            "consumer_type": self.signal_consumer.consumer_type,
            "consumer_product": getattr(self.signal_consumer, "product", self.signal_consumer.consumer_type),
            "timeframes": [tf.name for tf in self.timeframes],
            "strategy": self._strategy_checkpoint_data(),
            "simulation": self.simulation_config.to_dict(),
            "market_candidate_id": self.market_candidate_id,
            "max_candles": self.max_candles,
            "evaluations": [evaluation_dict(item) for item in self.evaluations],
            "signals": [signal_dict(item) for item in self.signals],
            "decision_id_order": list(self._decision_order),
            "signal_id_order": list(self._signal_order),
            "episodes": [_episode_dict(item) for item in self.episodes.values()],
            "context": {**self.context, "timestamp": iso(self.context.get("timestamp"))} if self.context else None,
            "pending_simulations": [item.to_dict() for item in self.pending_simulations],
            "completed_simulations": [item.to_dict() for item in self.completed_simulations],
            "completed_simulation_ids": completed_ids,
            "simulation_observations": [item.to_dict() for item in observations],
            "consumer": self.consumer_checkpoint,
            "consumer_events": [event.to_dict() for event in self._consumer_events],
            "consumer_event_count": self._consumer_event_count,
            "last_event_time": iso(self.last_event_time),
            "last_event_id": self.last_event_id,
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

    def strategy_checkpoint(self) -> dict[str, Any]:
        """Return the strategy-only state when the data plane is shared."""

        if self.data_plane is None:
            raise ValueError("strategy_checkpoint requiere un SharedDataPlane")
        return self._strategy_checkpoint_payload()

    def checkpoint(self, *, include_data_plane: bool = True) -> dict[str, Any]:
        """Return a JSON-compatible snapshot; no pickle or live objects."""

        if self.data_plane is not None:
            payload = self._strategy_checkpoint_payload()
            if include_data_plane:
                payload["data_plane"] = self.data_plane.checkpoint()
            return payload

        aggregator_state: dict[str, Any] = {}
        for name, aggregator in self.aggregators.items():
            bucket = getattr(aggregator, "_bucket", None)
            aggregator_state[name] = {
                "closed_through": iso(getattr(aggregator, "_closed_through", None)),
                "seen_event_ids": sorted(getattr(aggregator, "_seen_event_ids", set())),
                "seen_event_order": list(getattr(aggregator, "_seen_event_order", ())),
                "last_event_time": iso(getattr(aggregator, "_last_event_time", None)),
                "bucket": {
                    "start": iso(bucket.start),
                    "end": iso(bucket.end),
                    "events": [event_dict(event) for event in bucket.events],
                }
                if bucket is not None
                else None,
            }
            if bucket is not None and self.quote_coverage_mode != "strict":
                aggregator_state[name]["bucket"]["partial"] = bucket.partial
        strategy_data = self._strategy_checkpoint_data()
        completed_ids = (
            list(self._simulation_book.completed_order)
            if self._simulation_book is not None
            else [item.simulation_id for item in self.completed_simulations]
        )
        observations = self._simulation_book.observations if self._simulation_book is not None else ()
        return {
            "checkpoint_version": self.CHECKPOINT_VERSION,
            "config_hash": _processor_config_hash(self),
            **_processor_extra_config(self),
            "mode": self.mode.value,
            "instrument": self.instrument,
            "source": self.source,
            "price_base": self.price_base.value,
            "consumer_type": self.signal_consumer.consumer_type,
            "consumer_product": getattr(self.signal_consumer, "product", self.signal_consumer.consumer_type),
            "timeframes": [tf.name for tf in self.timeframes],
            "strategy": strategy_data,
            "simulation": self.simulation_config.to_dict(),
            "max_candles": self.max_candles,
            "seen_event_ids": sorted(self._seen_event_ids),
            "events": [event_dict(event) for event in self._events.values()],
            "aggregators": aggregator_state,
            "resample_buffers": {
                name: {
                    iso(bucket_start): {iso(base_start): candle_dict(candle) for base_start, candle in bucket.items()}
                    for bucket_start, bucket in buckets.items()
                }
                for name, buckets in self._resample_buffers.items()
            },
            "candles": {name: [candle_dict(candle) for candle in values] for name, values in self.candles.items()},
            "indicator_points": {
                name: [_indicator_point_dict(point) for point in values]
                for name, values in self.indicator_points.items()
            },
            "indicator_engines": {name: _engine_state(engine) for name, engine in self.indicator_engines.items()},
            "evaluations": [evaluation_dict(item) for item in self.evaluations],
            "signals": [signal_dict(item) for item in self.signals],
            "decision_id_order": list(self._decision_order),
            "signal_id_order": list(self._signal_order),
            "episodes": [_episode_dict(item) for item in self.episodes.values()],
            "context": {**self.context, "timestamp": iso(self.context.get("timestamp"))} if self.context else None,
            # These top-level fields remain for v1 readers. The consumer block
            # is the authoritative product state for new snapshots.
            "pending_simulations": [item.to_dict() for item in self.pending_simulations],
            "completed_simulations": [item.to_dict() for item in self.completed_simulations],
            "completed_simulation_ids": completed_ids,
            "simulation_observations": [item.to_dict() for item in observations],
            "consumer": self.consumer_checkpoint,
            "consumer_events": [event.to_dict() for event in self._consumer_events],
            "consumer_event_count": self._consumer_event_count,
            "last_event_time": iso(self.last_event_time),
            "last_event_id": self.last_event_id,
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
    def from_strategy_checkpoint(
        cls,
        snapshot: Mapping[str, Any] | str,
        *,
        data_plane: SharedDataPlane,
        allow_config_mismatch: bool = False,
        signal_consumer: SignalConsumer | None = None,
    ) -> IncrementalProcessor:
        """Restore one strategy state onto an existing shared data plane."""

        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot)
        if not isinstance(snapshot, Mapping):
            raise TypeError("strategy checkpoint debe ser mapping o JSON")
        if snapshot.get("checkpoint_scope") != "strategy":
            raise ValueError("checkpoint no contiene scope strategy")
        if snapshot.get("data_plane_config_hash") not in (None, data_plane.config_hash):
            raise ValueError("data_plane_config_hash no coincide")
        strategy_raw = dict(snapshot.get("strategy", {}))
        simulation_raw = dict(snapshot.get("simulation", {}))
        consumer_raw = snapshot.get("consumer")
        selected_consumer = _checkpoint_consumer(signal_consumer, consumer_raw, simulation_raw)
        processor = cls(
            strategy=strategy_raw,
            simulation=simulation_raw,
            timeframes=snapshot.get("timeframes", ()),
            mode=snapshot.get("mode", "REPLAY"),
            instrument=snapshot.get("instrument"),
            source=snapshot.get("source", "runtime"),
            price_base=snapshot.get("price_base", "traded"),
            max_candles=snapshot.get("max_candles"),
            signal_consumer=selected_consumer,
            market_candidate_id=snapshot.get("market_candidate_id"),
            data_plane=data_plane,
        )
        expected_hash = _processor_config_hash(processor)
        if not allow_config_mismatch and snapshot.get("config_hash") != expected_hash:
            raise ValueError("config_hash del strategy checkpoint no coincide")
        processor.attach_data_plane(data_plane)
        _restore_strategy_state(processor, snapshot)
        return processor

    @classmethod
    def from_checkpoint(
        cls,
        snapshot: Mapping[str, Any] | str,
        *,
        allow_config_mismatch: bool = False,
        signal_consumer: SignalConsumer | None = None,
    ) -> IncrementalProcessor:
        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot)
        if not isinstance(snapshot, Mapping):
            raise TypeError("snapshot debe ser mapping o JSON")
        if int(snapshot.get("checkpoint_version", 0)) != cls.CHECKPOINT_VERSION:
            raise ValueError("versión de checkpoint no soportada")
        data_plane_raw = snapshot.get("data_plane")
        if isinstance(data_plane_raw, Mapping):
            data_plane = SharedDataPlane.from_checkpoint(data_plane_raw)
            return cls.from_strategy_checkpoint(
                snapshot,
                data_plane=data_plane,
                allow_config_mismatch=allow_config_mismatch,
                signal_consumer=signal_consumer,
            )
        strategy_raw = dict(snapshot.get("strategy", {}))
        simulation_raw = dict(snapshot.get("simulation", {}))
        consumer_raw = snapshot.get("consumer")
        signal_consumer = _checkpoint_consumer(
            signal_consumer,
            consumer_raw,
            simulation_raw,
        )
        processor = cls(
            strategy=strategy_raw,
            simulation=simulation_raw,
            timeframes=snapshot.get("timeframes", cls.DEFAULT_TIMEFRAMES),
            mode=snapshot.get("mode", "REPLAY"),
            instrument=snapshot.get("instrument"),
            source=snapshot.get("source", "runtime"),
            price_base=snapshot.get("price_base", "traded"),
            max_candles=snapshot.get("max_candles"),
            signal_consumer=signal_consumer,
            market_candidate_id=snapshot.get("market_candidate_id"),
            quote_coverage_mode=snapshot.get("quote_coverage_mode", "strict"),
            max_quote_gap_seconds=snapshot.get("max_quote_gap_seconds"),
            historical_calendar=_checkpoint_historical_calendar(snapshot),
        )
        expected_hash = _processor_config_hash(processor)
        if not allow_config_mismatch and snapshot.get("config_hash") != expected_hash:
            raise ValueError("config_hash del checkpoint no coincide")
        _restore_capture_history(processor, snapshot)
        _restore_indicator_state(processor, snapshot)
        _restore_detector_state(processor, snapshot)
        _restore_checkpoint_consumer(processor, consumer_raw, snapshot)
        _restore_issues(processor, snapshot)
        _restore_resample_buffers(processor, snapshot)
        _restore_aggregators(processor, snapshot)
        # No se recrean simulaciones a partir de señales históricas: si un
        # contrato no está en pending ni en el ledger terminal del checkpoint,
        # la persistencia durable es la autoridad y no se resucita en memoria.
        return processor


IncrementalRuntime = IncrementalProcessor
RuntimeService = IncrementalProcessor


def _checkpoint_consumer(
    selected: SignalConsumer | None,
    raw_checkpoint: Any,
    simulation_raw: Mapping[str, Any],
) -> SignalConsumer:
    if selected is not None:
        return selected
    simulation = SimulationConfig.from_mapping(simulation_raw)
    max_observations = 4096
    if isinstance(raw_checkpoint, Mapping):
        state = raw_checkpoint.get("state", {})
        limit = state.get("max_observations") if isinstance(state, Mapping) else None
        if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
            max_observations = limit
    return consumer_from_checkpoint(
        raw_checkpoint,
        simulation=simulation,
        max_observations=max_observations,
    )


def _restore_capture_history(
    processor: IncrementalProcessor,
    snapshot: Mapping[str, Any],
) -> None:
    processor._seen_event_ids = set(str(item) for item in snapshot.get("seen_event_ids", ()))
    processor._events = {}
    for raw in snapshot.get("events", ()):
        event = event_from_dict(raw)
        if event.event_id is not None:
            processor._events[event.event_id] = event
    for name, rows in dict(snapshot.get("candles", {})).items():
        if name not in processor.candles:
            continue
        _restore_candles_for_timeframe(processor, name, rows)


def _restore_candles_for_timeframe(
    processor: IncrementalProcessor,
    name: str,
    rows: Any,
) -> None:
    for raw in rows:
        candle = candle_from_dict(raw)
        processor.candles[name].append(candle)
        logical = processor._logical_key(candle)
        processor._logical_candle_keys[name].add(logical)
        processor._candle_by_key[name][logical] = candle
        processor._add_candle_id(candle.candle_id)
        processor._processed_candle_starts[name].add(candle.start)
        processor._point_index_by_start[name][candle.start] = len(processor.candles[name]) - 1
        target = processor._native_candle_keys if not processor._is_derived(candle) else processor._derived_candle_keys
        target.add(logical)


def _restore_indicator_state(
    processor: IncrementalProcessor,
    snapshot: Mapping[str, Any],
) -> None:
    raw_points = snapshot.get("indicator_points")
    if isinstance(raw_points, Mapping):
        _restore_indicator_points(processor, raw_points)
    else:
        _rebuild_indicator_points(processor)
    for name, raw in dict(snapshot.get("indicator_engines", {})).items():
        if name in processor.indicator_engines and isinstance(raw, Mapping):
            _restore_engine_state(processor.indicator_engines[name], raw)


def _restore_indicator_points(
    processor: IncrementalProcessor,
    raw_points: Mapping[str, Any],
) -> None:
    for name, rows in raw_points.items():
        if name not in processor.indicator_points:
            continue
        processor.indicator_points[name].clear()
        for raw in rows:
            processor.indicator_points[name].append(_indicator_point_from_dict(raw))


def _rebuild_indicator_points(processor: IncrementalProcessor) -> None:
    for name, values in processor.candles.items():
        for candle in values:
            point = processor.indicator_engines[name].update(candle)
            processor.indicator_points[name].append(point)


def _restore_detector_state(
    processor: IncrementalProcessor,
    snapshot: Mapping[str, Any],
) -> None:
    processor.candles_processed = int(
        snapshot.get("candles_processed", sum(len(rows) for rows in processor.candles.values()))
    )
    processor.strategy_evaluations = int(snapshot.get("strategy_evaluations", 0))
    processor.strategy_skipped = int(snapshot.get("strategy_skipped", 0))
    processor._strategy_dirty = bool(snapshot.get("strategy_dirty", True))
    processor.last_strategy_window_sizes = {
        str(key): int(value) for key, value in dict(snapshot.get("last_strategy_window_sizes", {})).items()
    }
    processor.indicator_updates = {
        str(key): int(value) for key, value in dict(snapshot.get("indicator_updates", {})).items()
    }
    processor.events_processed = int(snapshot.get("events_processed", len(processor._events)))
    processor.last_event_time = utc(snapshot.get("last_event_time"))
    processor.last_event_id = snapshot.get("last_event_id")
    processor.last_available_at = utc(snapshot.get("last_available_at"))
    processor.evaluations = [evaluation_from_dict(item) for item in snapshot.get("evaluations", ())]
    processor._decision_ids = {_decision_id(item) for item in processor.evaluations}
    processor._decision_order = deque(
        str(item)
        for item in snapshot.get("decision_id_order", processor._decision_ids)
        if str(item) in processor._decision_ids
    )
    processor.signals = [signal_from_dict(item) for item in snapshot.get("signals", ())]
    processor._signal_ids = {item.signal_id for item in processor.signals}
    processor._signal_order = deque(
        str(item)
        for item in snapshot.get("signal_id_order", processor._signal_ids)
        if str(item) in processor._signal_ids
    )
    processor.episodes = {
        item.episode_id: item for item in (_episode_from_dict(raw) for raw in snapshot.get("episodes", ()))
    }
    raw_context = snapshot.get("context")
    if isinstance(raw_context, Mapping):
        processor.context = dict(raw_context)
        processor.context["timestamp"] = utc(raw_context.get("timestamp"))


def _restore_issues(
    processor: IncrementalProcessor,
    snapshot: Mapping[str, Any],
) -> None:
    processor.issues = [
        RuntimeIssue(
            str(raw.get("code")),
            str(raw.get("message")),
            utc(raw.get("timestamp")),
            raw.get("record_id"),
        )
        for raw in snapshot.get("issues", ())
    ]


def _restore_resample_buffers(
    processor: IncrementalProcessor,
    snapshot: Mapping[str, Any],
) -> None:
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


def _restore_aggregators(
    processor: IncrementalProcessor,
    snapshot: Mapping[str, Any],
) -> None:
    for name, raw in dict(snapshot.get("aggregators", {})).items():
        if name not in processor.aggregators:
            continue
        _restore_aggregator(processor.aggregators[name], raw)


def _restore_aggregator(
    aggregator: Any,
    raw: Mapping[str, Any],
) -> None:
    aggregator._closed_through = utc(raw.get("closed_through"))
    aggregator._seen_event_ids = set(str(item) for item in raw.get("seen_event_ids", ()))
    aggregator._seen_event_order.clear()
    order = [str(item) for item in raw.get("seen_event_order", raw.get("seen_event_ids", ()))]
    for event_id in order:
        if event_id in aggregator._seen_event_ids:
            aggregator._seen_event_order.append(event_id)
    aggregator._last_event_time = utc(raw.get("last_event_time"))
    bucket_raw = raw.get("bucket")
    if not bucket_raw:
        return
    bucket_events = [event_from_dict(item) for item in bucket_raw.get("events", ())]
    aggregator._bucket = _bucket_from_dict(bucket_raw, bucket_events)
    if bucket_events:
        aggregator._last_order_key = max(aggregator._event_order_key(event) for event in bucket_events)


def _restore_shared_aggregator(aggregator: Any, raw: Mapping[str, Any]) -> None:
    """Restore the shared bucket through B's public seam when available."""

    bucket_raw = raw.get("bucket")
    # ``CandleAggregator`` intentionally raises when the continuous-only
    # seam is called for strict coverage.  Preserve the strict legacy event
    # list and bucket bytes; only continuous quotes use the bounded public
    # state importer.
    restore = getattr(aggregator, "restore_bucket_state", None)
    if getattr(aggregator, "coverage_mode", "strict") != "continuous_quotes" or not callable(restore):
        _restore_aggregator(aggregator, raw)
        return
    restore(bucket_raw)
    aggregator._closed_through = utc(raw.get("closed_through"))
    aggregator._seen_event_ids = set(str(item) for item in raw.get("seen_event_ids", ()))
    aggregator._seen_event_order.clear()
    order = [str(item) for item in raw.get("seen_event_order", raw.get("seen_event_ids", ()))]
    for event_id in order:
        if event_id in aggregator._seen_event_ids:
            aggregator._seen_event_order.append(event_id)
    aggregator._last_event_time = utc(raw.get("last_event_time"))


def _restore_strategy_state(processor: IncrementalProcessor, snapshot: Mapping[str, Any]) -> None:
    """Restore strategy/consumer fields after the shared plane is attached."""

    _restore_detector_state(processor, snapshot)
    _restore_checkpoint_consumer(processor, snapshot.get("consumer"), snapshot)
    _restore_issues(processor, snapshot)


def _restore_legacy_binary_book(
    processor: IncrementalProcessor,
    snapshot: Mapping[str, Any],
) -> None:
    book = processor._simulation_book
    if book is None:
        return
    book.pending = {
        item.simulation_id: item
        for item in (PendingSimulation.from_dict(raw) for raw in snapshot.get("pending_simulations", ()))
    }
    completed = [PendingSimulation.from_dict(raw) for raw in snapshot.get("completed_simulations", ())]
    book.completed = {item.simulation_id: item for item in completed}
    book.completed_ids = {str(item) for item in snapshot.get("completed_simulation_ids", book.completed)}
    book.completed_ids.update(book.completed)
    book.completed_order = deque(item for item in book.completed_ids if item in book.completed)
    book.observations = [PriceObservation.from_dict(raw) for raw in snapshot.get("simulation_observations", ())][
        -book.max_observations :
    ]
    book._observation_ids = {item.identity for item in book.observations}


def _restore_checkpoint_consumer(
    processor: IncrementalProcessor,
    raw_checkpoint: Any,
    snapshot: Mapping[str, Any],
) -> None:
    if isinstance(raw_checkpoint, Mapping):
        processor.signal_consumer.restore(raw_checkpoint)
    else:
        _restore_legacy_binary_book(processor, snapshot)
    processor._consumer_pending = tuple(getattr(processor.signal_consumer, "pending_simulations", ()))
    processor.completed_simulations = [
        PendingSimulation.from_dict(raw) for raw in snapshot.get("completed_simulations", ())
    ]
    processor._consumer_events.clear()
    for raw in snapshot.get("consumer_events", ()):
        if isinstance(raw, Mapping):
            processor._consumer_events.append(SignalConsumerEvent.from_mapping(raw))
    processor._consumer_event_count = int(snapshot.get("consumer_event_count", len(processor._consumer_events)))
    processor._transition_consumer_events = []


def _bucket_from_dict(raw: Mapping[str, Any], events: Sequence[MarketEvent]) -> Any:
    start = utc(raw.get("start"))
    end = utc(raw.get("end"))
    if start is None or end is None:
        raise ValueError("bucket de checkpoint sin start/end")
    partial = raw.get("partial")
    if partial is not None and not isinstance(partial, bool):
        raise ValueError("bucket.partial debe ser booleano")
    return _Bucket(start, end, list(events), partial=partial)


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
    tf = (
        parse_timeframe(_attr(record, "timeframe", "resolution", "resolution_seconds", default="M1"))
        if is_bar
        else None
    )
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
        "quality": {
            "flags": sorted(flag.value for flag in point.quality.flags),
            "reasons": list(point.quality.reasons),
            "source": point.quality.source,
        },
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
    snapshot = engine.snapshot()
    return cast(dict[str, Any], snapshot.to_dict())


def _restore_engine_state(engine: Any, value: Mapping[str, Any]) -> None:
    engine.restore(value, allow_legacy=True)


def point_available(point: IndicatorPoint) -> datetime | None:
    if not point.closed:
        return None
    end = point.end
    if not isinstance(end, datetime) or end.tzinfo is None:
        return None
    available = point.available_at or end
    if not isinstance(available, datetime) or available.tzinfo is None:
        return None
    return max(available.astimezone(UTC), end.astimezone(UTC))


def _decision_id(evaluation: Any) -> str:
    payload = evaluation.as_dict() if hasattr(evaluation, "as_dict") else str(evaluation)
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
