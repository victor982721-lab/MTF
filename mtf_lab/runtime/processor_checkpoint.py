"""Codec aislado para checkpoints del procesador incremental.

El codec mantiene el contrato ``checkpoint_version=1`` que históricamente
expone :class:`IncrementalProcessor`.  No conoce el procesador por import
para evitar ciclos: recibe la clase de construcción y los dos callbacks de
configuración que forman parte del hash de identidad.  Los métodos públicos
del procesador siguen siendo la fachada estable.
"""

from __future__ import annotations

import hashlib
import json
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from ..core import IndicatorPoint, MarketEvent, Timeframe, parse_timeframe
from ..core.aggregation import _Bucket
from ..core.historical_calendar import HistoricalQuoteCalendar
from .consumers import SignalConsumer, SignalConsumerEvent, consumer_from_checkpoint
from .state import (
    PendingSimulation,
    PriceObservation,
    SimulationConfig,
    candle_dict,
    candle_from_dict,
    evaluation_dict,
    evaluation_from_dict,
    event_dict,
    event_from_dict,
    iso,
    quality_from,
    signal_dict,
    signal_from_dict,
    utc,
)

if TYPE_CHECKING:
    from .processor import IncrementalProcessor, RuntimeIssue, SharedDataPlane


ProcessorConfigHash = Callable[["IncrementalProcessor"], str]
ProcessorExtraConfig = Callable[["IncrementalProcessor"], dict[str, Any]]
RuntimeIssueFactory = Callable[[str, str, datetime | None, str | None], "RuntimeIssue"]


def _timeframe_name(value: Timeframe | str | int) -> str:
    return parse_timeframe(value).name


def _checkpoint_historical_calendar(snapshot: Mapping[str, Any]) -> HistoricalQuoteCalendar | None:
    raw = snapshot.get("historical_calendar")
    return HistoricalQuoteCalendar.from_mapping(raw) if raw is not None else None


def _strategy_checkpoint_data(processor: IncrementalProcessor) -> dict[str, Any]:
    strategy = processor.strategy_config
    context_timeframe = _timeframe_name(strategy.context_timeframe)
    preparation_timeframe = _timeframe_name(strategy.preparation_timeframe)
    trigger_timeframe = _timeframe_name(strategy.trigger_timeframe)
    return {
        "name": strategy.name,
        "context_timeframe": context_timeframe,
        "preparation_timeframe": preparation_timeframe,
        "trigger_timeframe": trigger_timeframe,
        "context_lookback": strategy.context_lookback,
        "preparation_lookback": strategy.preparation_lookback,
        "max_distance_atr": strategy.max_distance_atr,
        "rsi_threshold": strategy.rsi_threshold,
        "preparation_ttl_bars": strategy.preparation_ttl_bars,
        "require_closed": strategy.require_closed,
        "one_signal_per_episode": strategy.one_signal_per_episode,
        "optional_filters": dict(strategy.optional_filters),
        "mode": processor.mode.value,
        "indicators": {
            "ema_fast": strategy.indicators.ema_fast,
            "ema_slow": strategy.indicators.ema_slow,
            "rsi_period": strategy.indicators.rsi_period,
            "atr_period": strategy.indicators.atr_period,
            "wilder": strategy.indicators.wilder,
        },
    }


def _mutable_checkpoint_fields(processor: IncrementalProcessor) -> dict[str, Any]:
    """Encode the state owned by one processor, after its scope-specific prefix."""

    completed_ids = (
        list(processor._simulation_book.completed_order)
        if processor._simulation_book is not None
        else [item.simulation_id for item in processor.completed_simulations]
    )
    observations = processor._simulation_book.observations if processor._simulation_book is not None else ()
    return {
        "evaluations": [evaluation_dict(item) for item in processor.evaluations],
        "signals": [signal_dict(item) for item in processor.signals],
        "decision_id_order": list(processor._decision_order),
        "signal_id_order": list(processor._signal_order),
        "episodes": [_episode_dict(item) for item in processor.episodes.values()],
        "context": {**processor.context, "timestamp": iso(processor.context.get("timestamp"))}
        if processor.context
        else None,
        "pending_simulations": [item.to_dict() for item in processor.pending_simulations],
        "completed_simulations": [item.to_dict() for item in processor.completed_simulations],
        "completed_simulation_ids": completed_ids,
        "simulation_observations": [item.to_dict() for item in observations],
        "consumer": processor.consumer_checkpoint,
        "consumer_events": [event.to_dict() for event in processor._consumer_events],
        "consumer_event_count": processor._consumer_event_count,
        "last_event_time": iso(processor.last_event_time),
        "last_event_id": processor.last_event_id,
        "last_available_at": iso(processor.last_available_at),
        "events_processed": processor.events_processed,
        "candles_processed": processor.candles_processed,
        "strategy_evaluations": processor.strategy_evaluations,
        "strategy_skipped": processor.strategy_skipped,
        "strategy_dirty": processor._strategy_dirty,
        "last_strategy_window_sizes": dict(processor.last_strategy_window_sizes),
        "indicator_updates": dict(processor.indicator_updates),
        "issues": [issue.to_dict() for issue in processor.issues],
    }


def strategy_checkpoint_payload(
    processor: IncrementalProcessor,
    *,
    config_hash: ProcessorConfigHash,
    extra_config: ProcessorExtraConfig,
) -> dict[str, Any]:
    """Serialize only mutable strategy/consumer state for a shared plane."""

    return {
        "checkpoint_version": processor.CHECKPOINT_VERSION,
        "checkpoint_scope": "strategy",
        "config_hash": config_hash(processor),
        "data_plane_config_hash": processor.data_plane.config_hash if processor.data_plane is not None else None,
        **extra_config(processor),
        "mode": processor.mode.value,
        "instrument": processor.instrument,
        "source": processor.source,
        "price_base": processor.price_base.value,
        "consumer_type": processor.signal_consumer.consumer_type,
        "consumer_product": getattr(processor.signal_consumer, "product", processor.signal_consumer.consumer_type),
        "timeframes": [tf.name for tf in processor.timeframes],
        "strategy": _strategy_checkpoint_data(processor),
        "simulation": processor.simulation_config.to_dict(),
        "market_candidate_id": processor.market_candidate_id,
        "max_candles": processor.max_candles,
        **_mutable_checkpoint_fields(processor),
    }


def _standalone_aggregator_state(processor: IncrementalProcessor, name: str, aggregator: Any) -> dict[str, Any]:
    bucket = getattr(aggregator, "_bucket", None)
    state: dict[str, Any] = {
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
    if bucket is not None and processor.quote_coverage_mode != "strict":
        state["bucket"]["partial"] = bucket.partial
    return state


def checkpoint_snapshot(
    processor: IncrementalProcessor,
    *,
    include_data_plane: bool = True,
    config_hash: ProcessorConfigHash,
    extra_config: ProcessorExtraConfig,
) -> dict[str, Any]:
    """Return a JSON-compatible standalone or shared-plane snapshot."""

    if processor.data_plane is not None:
        payload = strategy_checkpoint_payload(processor, config_hash=config_hash, extra_config=extra_config)
        if include_data_plane:
            payload["data_plane"] = processor.data_plane.checkpoint()
        return payload

    aggregator_state = {
        name: _standalone_aggregator_state(processor, name, aggregator)
        for name, aggregator in processor.aggregators.items()
    }
    strategy_data = _strategy_checkpoint_data(processor)
    return {
        "checkpoint_version": processor.CHECKPOINT_VERSION,
        "config_hash": config_hash(processor),
        **extra_config(processor),
        "mode": processor.mode.value,
        "instrument": processor.instrument,
        "source": processor.source,
        "price_base": processor.price_base.value,
        "consumer_type": processor.signal_consumer.consumer_type,
        "consumer_product": getattr(processor.signal_consumer, "product", processor.signal_consumer.consumer_type),
        "timeframes": [tf.name for tf in processor.timeframes],
        "strategy": strategy_data,
        "simulation": processor.simulation_config.to_dict(),
        "max_candles": processor.max_candles,
        "seen_event_ids": sorted(processor._seen_event_ids),
        "events": [event_dict(event) for event in processor._events.values()],
        "aggregators": aggregator_state,
        "resample_buffers": {
            name: {
                iso(bucket_start): {iso(base_start): candle_dict(candle) for base_start, candle in bucket.items()}
                for bucket_start, bucket in buckets.items()
            }
            for name, buckets in processor._resample_buffers.items()
        },
        "candles": {name: [candle_dict(candle) for candle in values] for name, values in processor.candles.items()},
        "indicator_points": {
            name: [_indicator_point_dict(point) for point in values]
            for name, values in processor.indicator_points.items()
        },
        "indicator_engines": {name: _engine_state(engine) for name, engine in processor.indicator_engines.items()},
        # These top-level fields remain for v1 readers. The consumer block
        # is the authoritative product state for new snapshots.
        **_mutable_checkpoint_fields(processor),
    }


def checkpoint_json(
    processor: IncrementalProcessor,
    *,
    config_hash: ProcessorConfigHash,
    extra_config: ProcessorExtraConfig,
) -> str:
    return json.dumps(
        checkpoint_snapshot(processor, config_hash=config_hash, extra_config=extra_config),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


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


def _restore_capture_history(processor: IncrementalProcessor, snapshot: Mapping[str, Any]) -> None:
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


def _restore_candles_for_timeframe(processor: IncrementalProcessor, name: str, rows: Any) -> None:
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


def _restore_indicator_state(processor: IncrementalProcessor, snapshot: Mapping[str, Any]) -> None:
    raw_points = snapshot.get("indicator_points")
    if isinstance(raw_points, Mapping):
        _restore_indicator_points(processor, raw_points)
    else:
        _rebuild_indicator_points(processor)
    for name, raw in dict(snapshot.get("indicator_engines", {})).items():
        if name in processor.indicator_engines and isinstance(raw, Mapping):
            _restore_engine_state(processor.indicator_engines[name], raw)


def _restore_indicator_points(processor: IncrementalProcessor, raw_points: Mapping[str, Any]) -> None:
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


def _restore_detector_state(processor: IncrementalProcessor, snapshot: Mapping[str, Any]) -> None:
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
    runtime_issue: RuntimeIssueFactory,
) -> None:
    processor.issues = [
        runtime_issue(
            str(raw.get("code")),
            str(raw.get("message")),
            utc(raw.get("timestamp")),
            raw.get("record_id"),
        )
        for raw in snapshot.get("issues", ())
    ]


def _restore_resample_buffers(processor: IncrementalProcessor, snapshot: Mapping[str, Any]) -> None:
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


def _restore_aggregators(processor: IncrementalProcessor, snapshot: Mapping[str, Any]) -> None:
    for name, raw in dict(snapshot.get("aggregators", {})).items():
        if name not in processor.aggregators:
            continue
        _restore_aggregator(processor.aggregators[name], raw)


def _restore_aggregator(aggregator: Any, raw: Mapping[str, Any]) -> None:
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


def _restore_strategy_state(
    processor: IncrementalProcessor,
    snapshot: Mapping[str, Any],
    runtime_issue: RuntimeIssueFactory,
) -> None:
    """Restore strategy/consumer fields after the shared plane is attached."""

    _restore_detector_state(processor, snapshot)
    _restore_checkpoint_consumer(processor, snapshot.get("consumer"), snapshot)
    _restore_issues(processor, snapshot, runtime_issue)


def _restore_legacy_binary_book(processor: IncrementalProcessor, snapshot: Mapping[str, Any]) -> None:
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


def restore_strategy_checkpoint(
    processor_cls: type[IncrementalProcessor],
    snapshot: Mapping[str, Any] | str,
    *,
    data_plane: SharedDataPlane,
    allow_config_mismatch: bool,
    signal_consumer: SignalConsumer | None,
    config_hash: ProcessorConfigHash,
    runtime_issue: RuntimeIssueFactory,
) -> IncrementalProcessor:
    """Restore strategy state onto an already restored shared data plane."""

    if isinstance(snapshot, str):
        snapshot = json.loads(snapshot)
    if not isinstance(snapshot, Mapping):
        raise TypeError("strategy checkpoint debe ser mapping o JSON")
    if int(snapshot.get("checkpoint_version", 0)) != processor_cls.CHECKPOINT_VERSION:
        raise ValueError("versión de strategy checkpoint no soportada")
    if snapshot.get("checkpoint_scope") != "strategy":
        raise ValueError("checkpoint no contiene scope strategy")
    if snapshot.get("data_plane_config_hash") not in (None, data_plane.config_hash):
        raise ValueError("data_plane_config_hash no coincide")
    strategy_raw = dict(snapshot.get("strategy", {}))
    simulation_raw = dict(snapshot.get("simulation", {}))
    consumer_raw = snapshot.get("consumer")
    selected_consumer = _checkpoint_consumer(signal_consumer, consumer_raw, simulation_raw)
    processor = processor_cls(
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
    expected_hash = config_hash(processor)
    if not allow_config_mismatch and snapshot.get("config_hash") != expected_hash:
        raise ValueError("config_hash del strategy checkpoint no coincide")
    processor.attach_data_plane(data_plane)
    _restore_strategy_state(processor, snapshot, runtime_issue)
    return processor


def restore_checkpoint(
    processor_cls: type[IncrementalProcessor],
    shared_data_plane_cls: type[SharedDataPlane],
    snapshot: Mapping[str, Any] | str,
    *,
    allow_config_mismatch: bool,
    signal_consumer: SignalConsumer | None,
    config_hash: ProcessorConfigHash,
    runtime_issue: RuntimeIssueFactory,
) -> IncrementalProcessor:
    """Restore either a standalone or shared-plane v1 processor snapshot."""

    if isinstance(snapshot, str):
        snapshot = json.loads(snapshot)
    if not isinstance(snapshot, Mapping):
        raise TypeError("snapshot debe ser mapping o JSON")
    if int(snapshot.get("checkpoint_version", 0)) != processor_cls.CHECKPOINT_VERSION:
        raise ValueError("versión de checkpoint no soportada")
    data_plane_raw = snapshot.get("data_plane")
    if "data_plane" in snapshot and data_plane_raw is not None and not isinstance(data_plane_raw, Mapping):
        raise ValueError("data_plane checkpoint debe ser mapping")
    if isinstance(data_plane_raw, Mapping):
        data_plane = shared_data_plane_cls.from_checkpoint(data_plane_raw)
        return restore_strategy_checkpoint(
            processor_cls,
            snapshot,
            data_plane=data_plane,
            allow_config_mismatch=allow_config_mismatch,
            signal_consumer=signal_consumer,
            config_hash=config_hash,
            runtime_issue=runtime_issue,
        )
    strategy_raw = dict(snapshot.get("strategy", {}))
    simulation_raw = dict(snapshot.get("simulation", {}))
    consumer_raw = snapshot.get("consumer")
    selected_consumer = _checkpoint_consumer(signal_consumer, consumer_raw, simulation_raw)
    processor = processor_cls(
        strategy=strategy_raw,
        simulation=simulation_raw,
        timeframes=snapshot.get("timeframes", processor_cls.DEFAULT_TIMEFRAMES),
        mode=snapshot.get("mode", "REPLAY"),
        instrument=snapshot.get("instrument"),
        source=snapshot.get("source", "runtime"),
        price_base=snapshot.get("price_base", "traded"),
        max_candles=snapshot.get("max_candles"),
        signal_consumer=selected_consumer,
        market_candidate_id=snapshot.get("market_candidate_id"),
        quote_coverage_mode=snapshot.get("quote_coverage_mode", "strict"),
        max_quote_gap_seconds=snapshot.get("max_quote_gap_seconds"),
        historical_calendar=_checkpoint_historical_calendar(snapshot),
    )
    expected_hash = config_hash(processor)
    if not allow_config_mismatch and snapshot.get("config_hash") != expected_hash:
        raise ValueError("config_hash del checkpoint no coincide")
    _restore_capture_history(processor, snapshot)
    _restore_indicator_state(processor, snapshot)
    _restore_detector_state(processor, snapshot)
    _restore_checkpoint_consumer(processor, consumer_raw, snapshot)
    _restore_issues(processor, snapshot, runtime_issue)
    _restore_resample_buffers(processor, snapshot)
    _restore_aggregators(processor, snapshot)
    # No se recrean simulaciones a partir de señales históricas: si un
    # contrato no está en pending ni en el ledger terminal del checkpoint,
    # la persistencia durable es la autoridad y no se resucita en memoria.
    return processor


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


def _decision_id(evaluation: Any) -> str:
    payload = evaluation.as_dict() if hasattr(evaluation, "as_dict") else str(evaluation)
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


__all__ = [
    "checkpoint_json",
    "checkpoint_snapshot",
    "restore_checkpoint",
    "restore_strategy_checkpoint",
    "strategy_checkpoint_payload",
]
