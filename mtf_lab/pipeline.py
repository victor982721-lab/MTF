"""Integración vertical de MTF Lab.

Este módulo es un adaptador fino: convierte registros de proveedores al modelo
canónico, construye temporalidades desde velas sin inventar ticks, ejecuta el
mismo detector para replay/sintético y deja todo en SQLite.  No contiene una
segunda implementación de indicadores ni de reglas.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from . import __version__
from .core import (
    Candle,
    DataQuality,
    EventKind,
    IndicatorConfig,
    IndicatorSeries,
    MarketEvent,
    OperationMode,
    PriceBase,
    StrategyResult,
    Timeframe,
    TrendPullbackStrategy,
    compute_indicators,
    parse_timeframe,
)
from .core.quality import merge_quality
from .core.reference import m1_reference_signals
from .data import Bar as ProviderBar
from .data import DataSet, SyntheticGenerator
from .data import Event as ProviderEvent
from .data.translation import TranslationError, data_bar_to_core, data_event_to_core
from .ops.backtest import BacktestRunner, VariantSpec
from .ops.logging_state import OperationTelemetry
from .ops.persistence import SQLiteStore, payload_hash
from .ops.reporting import ReportBuilder
from .ops.simulation import EvaluationSpec, VirtualContractSimulator
from .runtime import IncrementalProcessor
from .runtime import SimulationConfig as RuntimeSimulationConfig


@dataclass(slots=True)
class PipelineResult:
    dataset: DataSet
    streams: dict[str, list[Candle]]
    indicators: dict[str, IndicatorSeries]
    strategy: StrategyResult
    baseline_signals: list[dict[str, Any]] | None = None
    session_id: str | None = None
    simulations: list[dict[str, Any]] | None = None
    report: dict[str, Any] | None = None


def _mode(value: str | OperationMode) -> OperationMode:
    if isinstance(value, OperationMode):
        return value
    text = str(value).upper().replace("OBSERVACIÓN EN DIRECTO", "LIVE")
    return OperationMode(text)


def _provider_bar_fields(bar: Any) -> tuple[Any, Any, Any, Any, Mapping[str, Any]]:
    """Extract the two supported provider-record shapes without interpretation."""

    start = getattr(bar, "interval_start", getattr(bar, "start", None))
    end = getattr(bar, "interval_end", getattr(bar, "end", None))
    resolution = getattr(bar, "resolution", getattr(bar, "resolution_seconds", None))
    if start is None or end is None or resolution is None:
        row = dict(bar) if isinstance(bar, Mapping) else vars(bar)
        start = row.get("interval_start", row.get("start", row.get("start_ts")))
        end = row.get("interval_end", row.get("end", row.get("end_ts")))
        resolution = row.get("resolution", row.get("timeframe", row.get("resolution_seconds")))
        instrument = row.get("instrument", row.get("symbol", "unknown"))
        values = row
    else:
        instrument = getattr(bar, "instrument", "unknown")
        values = vars(bar) if hasattr(bar, "__dict__") else {}
    return start, end, resolution, instrument, values


def _provider_bar_price_basis(bar: Any, values: Mapping[str, Any]) -> PriceBase:
    if "price_basis" not in values and "price_type" not in values and not hasattr(bar, "price_basis"):
        raise TranslationError("vela sin base de precio explícita", code="PRICE_BASE_UNKNOWN")
    price_basis = getattr(bar, "price_basis", values.get("price_basis", values.get("price_type")))
    if hasattr(price_basis, "value"):
        price_basis = price_basis.value
    if str(price_basis).lower() == "close":
        # Candle.close es el valor de cierre de la base declarada; el modelo
        # canónico usa traded como alias numérico para una vela OHLC.
        price_basis = "traded"
    return PriceBase(str(price_basis).lower())


def _provider_bar_quality(bar: Any, values: Mapping[str, Any]) -> DataQuality:
    quality = getattr(bar, "quality", values.get("quality", None))
    if isinstance(quality, DataQuality):
        return quality
    from .runtime.state import quality_from

    return quality_from(quality, synthetic=bool(getattr(bar, "synthetic", values.get("synthetic", False))))


def _provider_required_value(bar: Any, values: Mapping[str, Any], name: str) -> Any:
    return getattr(bar, name, values.get(name))


def _provider_bar_to_candle(bar: Any, *, mode: str | OperationMode) -> Candle:
    start, end, resolution, instrument, values = _provider_bar_fields(bar)
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace("Z", "+00:00"))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace("Z", "+00:00"))
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("provider bar timestamps must include timezone")
    tf = parse_timeframe(resolution)
    price_basis = _provider_bar_price_basis(bar, values)
    quality = _provider_bar_quality(bar, values)
    closed = bool(getattr(bar, "closed", values.get("closed", True)))
    available = getattr(bar, "available_at", values.get("available_at", None))
    received = getattr(bar, "received_at", values.get("received_at", None))
    return Candle(
        instrument=str(instrument),
        timeframe=tf,
        start=start,
        end=end,
        open=float(_provider_required_value(bar, values, "open")),
        high=float(_provider_required_value(bar, values, "high")),
        low=float(_provider_required_value(bar, values, "low")),
        close=float(_provider_required_value(bar, values, "close")),
        volume=float(getattr(bar, "volume", values.get("volume", 0.0)) or 0.0),
        event_count=int(getattr(bar, "trade_count", values.get("trade_count", values.get("event_count", 0))) or 0),
        source=str(getattr(bar, "source", values.get("source", "provider"))),
        mode=_mode(mode),
        price_base=price_basis,
        closed=closed,
        available_at=available,
        received_at=received,
        quality=quality,
        candle_id=str(getattr(bar, "data_id", getattr(bar, "candle_id", values.get("candle_id", ""))) or "") or None,
        origin=str(getattr(bar, "origin", values.get("origin", "provider"))),
        metadata=getattr(bar, "metadata", values.get("metadata", {})) or {},
    )


def provider_bar_to_candle(bar: Any, *, mode: str | OperationMode = OperationMode.REPLAY) -> Candle:
    """Map a provider Bar preserving quality/provenance/revision exactly."""
    if isinstance(bar, Candle):
        return bar
    if isinstance(bar, ProviderBar):
        translated = data_bar_to_core(bar)
        requested_mode = _mode(mode)
        # El modo de una fuente sintética/live es parte de su procedencia; no
        # se pisa con un default, sólo se verifica que sea coherente.
        if translated.mode is not requested_mode and requested_mode is not OperationMode.REPLAY:
            raise TranslationError(
                f"modo de vela incompatible: {translated.mode.value} vs {requested_mode.value}", code="MODE_CONFLICT"
            )
        return translated
    return _provider_bar_to_candle(bar, mode=mode)


def _provider_event_basis(event: Any) -> str:
    basis = getattr(event, "price_basis", getattr(event, "price_base", getattr(event, "price_type", None)))
    if basis is None:
        raise TranslationError("evento sin base de precio explícita", code="PRICE_BASE_UNKNOWN")
    if hasattr(basis, "value"):
        basis = basis.value
    return "traded" if basis in {"trade", "close"} else str(basis).lower()


def _validate_provider_event_basis(event: Any, basis: str) -> None:
    if basis == "bid" and getattr(event, "bid", None) is None:
        raise TranslationError("evento BID sin bid explícito", code="PRICE_BASE_MISSING")
    if basis == "ask" and getattr(event, "ask", None) is None:
        raise TranslationError("evento ASK sin ask explícito", code="PRICE_BASE_MISSING")
    if basis == "mid" and (
        getattr(event, "mid", None) is None
        or getattr(event, "bid", None) is None
        or getattr(event, "ask", None) is None
    ):
        raise TranslationError("evento MID requiere mid, bid y ask explícitos", code="PRICE_BASE_MISSING")


def _provider_event_quality(event: Any) -> DataQuality:
    quality = getattr(event, "quality", None)
    if isinstance(quality, DataQuality):
        return quality
    from .runtime.state import quality_from

    return quality_from(quality, synthetic=bool(getattr(event, "synthetic", False)))


def _provider_event_kind(event: Any) -> EventKind:
    event_kind_raw = getattr(event, "event_kind", getattr(event, "kind", "trade"))
    event_kind_raw = getattr(event_kind_raw, "value", event_kind_raw)
    try:
        return EventKind(str(event_kind_raw).lower())
    except ValueError as exc:
        raise TranslationError(f"event_kind desconocido: {event_kind_raw!r}", code="EVENT_KIND_UNKNOWN") from exc


def _provider_event_to_core(event: Any, *, mode: str | OperationMode) -> MarketEvent:
    basis = _provider_event_basis(event)
    _validate_provider_event_basis(event, basis)
    et = getattr(event, "event_time", None)
    if isinstance(et, str):
        et = datetime.fromisoformat(et.replace("Z", "+00:00"))
    if et is None or et.tzinfo is None:
        raise ValueError("provider event timestamp must include timezone")
    quality = _provider_event_quality(event)
    event_kind = _provider_event_kind(event)
    return MarketEvent(
        instrument=str(getattr(event, "instrument", "unknown")),
        event_time=et,
        price=getattr(event, "price", None) if basis == "traded" else None,
        quantity=getattr(event, "quantity", None),
        bid=getattr(event, "bid", None),
        ask=getattr(event, "ask", None),
        received_at=getattr(event, "received_at", None),
        available_at=getattr(event, "available_at", None),
        source=str(getattr(event, "source", "provider")),
        mode=_mode(mode),
        price_base=PriceBase(basis),
        event_kind=event_kind,
        sequence=getattr(event, "source_sequence", getattr(event, "sequence", None)),
        source_event_id=getattr(event, "source_event_id", None),
        event_id=getattr(event, "event_id", None),
        quality=quality,
        metadata=getattr(event, "metadata", {}) or {},
    )


def provider_event_to_core(event: Any, *, mode: str | OperationMode = OperationMode.REPLAY) -> MarketEvent:
    """Map a provider Event preserving quality/provenance/base/identity."""
    if isinstance(event, MarketEvent):
        return event
    if isinstance(event, ProviderEvent):
        translated = data_event_to_core(event)
        requested_mode = _mode(mode)
        if translated.mode is not requested_mode and requested_mode is not OperationMode.REPLAY:
            raise TranslationError(
                f"modo de evento incompatible: {translated.mode.value} vs {requested_mode.value}", code="MODE_CONFLICT"
            )
        return translated
    return _provider_event_to_core(event, mode=mode)


def resample_candles(candles: Iterable[Candle], target: Timeframe | str) -> tuple[list[Candle], list[str]]:
    """OHLC-resample complete base candles; incomplete buckets remain gaps."""
    rows = sorted(list(candles), key=lambda x: x.start)
    if not rows:
        return [], []
    tf = parse_timeframe(target)
    base = parse_timeframe(rows[0].timeframe)
    if tf.seconds < base.seconds or tf.seconds % base.seconds:
        raise ValueError("target timeframe must be an integer multiple of base timeframe")
    ratio = tf.seconds // base.seconds
    buckets: dict[datetime, list[Candle]] = {}
    for row in rows:
        epoch = int(row.start.timestamp())
        start = datetime.fromtimestamp((epoch // tf.seconds) * tf.seconds, UTC)
        buckets.setdefault(start, []).append(row)
    output: list[Candle] = []
    gaps: list[str] = []
    for start in sorted(buckets):
        group = sorted(buckets[start], key=lambda x: x.start)
        expected = [start + timedelta(seconds=base.seconds * i) for i in range(ratio)]
        if len(group) != ratio or [x.start for x in group] != expected or any(not x.closed for x in group):
            gaps.append(start.isoformat().replace("+00:00", "Z"))
            continue
        end = start + tf.delta
        output.append(
            Candle(
                instrument=group[0].instrument,
                timeframe=tf,
                start=start,
                end=end,
                open=group[0].open,
                high=max(x.high for x in group),
                low=min(x.low for x in group),
                close=group[-1].close,
                volume=sum(x.volume for x in group),
                event_count=sum(x.event_count for x in group),
                source="aggregated-from-bars",
                mode=group[0].mode,
                price_base=group[0].price_base,
                closed=True,
                available_at=max(x.available_at or x.end for x in group),
                received_at=max(x.received_at or x.end for x in group),
                quality=merge_quality(*(x.quality for x in group), source="aggregated-from-bars"),
                origin="resampled_ohlc",
                metadata={"base_timeframe": base.name, "ratio": ratio},
            )
        )
    return output, gaps


def build_streams(
    bars: Iterable[Any], *, mode: str | OperationMode = OperationMode.SYNTHETIC, targets: Iterable[str] = ("M5", "M15")
) -> tuple[dict[str, list[Candle]], dict[str, list[str]]]:
    base = [provider_bar_to_candle(x, mode=mode) for x in bars]
    base.sort(key=lambda x: x.start)
    if not base:
        return {}, {}
    base_timeframe_name = base[0].timeframe_name
    streams = {base_timeframe_name: base}
    gaps: dict[str, list[str]] = {}
    for name in targets:
        name = parse_timeframe(name).name
        if name == base_timeframe_name:
            continue
        try:
            stream, stream_gaps = resample_candles(base, name)
        except ValueError:
            continue
        streams[name] = stream
        gaps[name] = stream_gaps
    return streams, gaps


def points_from_candles(candles: Iterable[Candle]) -> list[dict[str, Any]]:
    """Price points that preserve close, receipt and availability timestamps."""
    return [
        {
            "point_id": c.candle_id,
            "timestamp": c.end,
            "end_ts": c.end,
            "instrument": c.instrument,
            "price": c.close,
            "base_price": c.price_base.value,
            "source": c.source,
            "quality": c.quality.status,
            "resolution": c.timeframe_name,
            "closed": c.closed,
            "received_at": c.received_at,
            "available_at": c.available_at or c.end,
        }
        for c in candles
    ]


def generate_synthetic_streams(
    *, seed: int = 42, periods_each: int = 600, targets: Iterable[str] = ("M5", "M15"), instrument: str = "SYNTH/USD"
) -> tuple[DataSet, dict[str, list[Candle]], dict[str, list[str]]]:
    generator = SyntheticGenerator(seed=seed, instrument=instrument)
    dataset = generator.generate_scenarios(periods_each=periods_each)
    streams, gaps = build_streams(dataset.bars, mode=OperationMode.SYNTHETIC, targets=targets)
    return dataset, streams, gaps


def _config_mapping(config: Any | None) -> dict[str, Any]:
    if config is None:
        return {}
    if hasattr(config, "to_dict") and callable(config.to_dict):
        return dict(cast(Mapping[str, Any], config.to_dict()))
    if isinstance(config, Mapping):
        return dict(config)
    raise TypeError("config debe ser EffectiveConfig o mapping")


def run_detector(
    streams: Mapping[str, Iterable[Candle]],
    *,
    mode: str | OperationMode = OperationMode.SYNTHETIC,
    config: Mapping[str, Any] | None = None,
) -> tuple[dict[str, IndicatorSeries], StrategyResult]:
    config_map = _config_mapping(config)
    indicator_config = IndicatorConfig.from_mapping(config_map.get("indicators")) if config_map else IndicatorConfig()
    indicators: dict[str, IndicatorSeries] = {}
    for name, rows in streams.items():
        indicators[name] = compute_indicators(list(rows), indicator_config)
    strategy_cfg: dict[str, Any] = dict(config_map.get("strategy", {}))
    strategy_cfg["mode"] = _mode(mode)
    strategy_cfg.setdefault(
        "indicators",
        {
            "ema_fast": indicator_config.ema_fast,
            "ema_slow": indicator_config.ema_slow,
            "rsi_period": indicator_config.rsi_period,
            "atr_period": indicator_config.atr_period,
        },
    )
    # Accept the project config vocabulary while preserving strict StrategyConfig validation.
    if "setup_timeframe" in strategy_cfg and "preparation_timeframe" not in strategy_cfg:
        strategy_cfg["preparation_timeframe"] = strategy_cfg.pop("setup_timeframe")
    if "setup_max_atr" in strategy_cfg and "max_distance_atr" not in strategy_cfg:
        strategy_cfg["max_distance_atr"] = strategy_cfg.pop("setup_max_atr")
    if "lookback" in strategy_cfg:
        strategy_cfg.setdefault("context_lookback", strategy_cfg["lookback"])
        strategy_cfg.setdefault("preparation_lookback", strategy_cfg["lookback"])
        strategy_cfg.pop("lookback", None)
    strategy = TrendPullbackStrategy(strategy_cfg)
    return indicators, strategy.evaluate(indicators)


def evaluation_spec_from_config(config: Mapping[str, Any] | Any | None = None) -> EvaluationSpec:
    """Adaptador único hacia :class:`EvaluationSpec`."""
    from .configuration import normalize_simulation_mapping

    return EvaluationSpec(**normalize_simulation_mapping(config))


def _candle_row(c: Candle, point: Any | None = None) -> dict[str, Any]:
    return {
        "candle_id": c.candle_id,
        "instrument": c.instrument,
        "timeframe": c.timeframe_name,
        "start_ts": c.start,
        "end_ts": c.end,
        "open": c.open,
        "high": c.high,
        "low": c.low,
        "close": c.close,
        "volume": c.volume,
        "event_count": c.event_count,
        "closed": c.closed,
        "source": c.source,
        "mode": c.mode.value,
        "origin": c.origin,
        "price_base": c.price_base.value,
        "quality": c.quality.status,
        "available_ts": c.available_at or c.end,
        "received_ts": c.received_at,
        "provenance": {
            "origin": c.origin,
            "metadata": dict(c.metadata),
            "indicators": (
                {"ema_fast": point.ema_fast, "ema_slow": point.ema_slow, "rsi": point.rsi, "atr": point.atr}
                if point is not None
                else {}
            ),
        },
    }


@dataclass(frozen=True, slots=True)
class _PersistenceInput:
    db: Path
    config_effective: dict[str, Any]
    analysis_config_hash: str
    dataset_hash: str
    trigger_name: str
    baseline_signals: list[dict[str, Any]]


def _dataset_hash(dataset: DataSet) -> str:
    payload = json.dumps(
        [row.to_dict() for row in dataset.bars], sort_keys=True, default=str, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _prepare_persistence_input(
    result: PipelineResult,
    db_path: str | Path,
    *,
    seed: int,
    config: Mapping[str, Any] | None,
) -> _PersistenceInput:
    db = Path(db_path).expanduser()
    config_input = _config_mapping(config)
    # The analysis namespace is the effective financial/strategy profile, not
    # the capture seed, dataset hash or runtime telemetry. Those belong to
    # dataset/provenance fields and must not fork signal identities.
    declared_hash = config_input.get("config_hash")
    analysis_config_hash = str(declared_hash) if declared_hash else payload_hash(config_input)
    config_effective = dict(config_input)
    config_effective.setdefault("seed", seed)
    config_effective.setdefault("mode", "SYNTHETIC")
    dataset_hash = _dataset_hash(result.dataset)
    config_effective.setdefault("dataset_hash", dataset_hash)
    strategy_map = (
        config_effective.get("strategy", {}) if isinstance(config_effective.get("strategy", {}), Mapping) else {}
    )
    trigger_name = str(strategy_map.get("trigger_timeframe", "M1")).upper()
    baseline_signals = (
        m1_reference_signals(
            result.indicators[trigger_name],
            rsi_threshold=float(strategy_map.get("rsi_threshold", 50.0)),
            mode="SYNTHETIC",
            identity_salt=analysis_config_hash,
        )
        if trigger_name in result.indicators
        else []
    )
    return _PersistenceInput(db, config_effective, analysis_config_hash, dataset_hash, trigger_name, baseline_signals)


def _contract_hash(spec: EvaluationSpec) -> str:
    contract_payload = {
        "stake": spec.stake,
        "payout_net": spec.payout_net,
        "loss_amount": spec.loss_amount,
        "tie_net": spec.tie_net,
        "costs": spec.costs,
        "tie_tolerance": spec.tie_tolerance,
        "entry_rule": spec.entry_rule,
        "exit_rule": spec.exit_rule,
        "horizon_from": spec.horizon_from,
        "requested_base_price": spec.requested_base_price,
    }
    return hashlib.sha256(json.dumps(contract_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _create_session(store: SQLiteStore, result: PipelineResult, *, seed: int, config: Mapping[str, Any]) -> str:
    return store.create_session(
        mode="SYNTHETIC",
        provider=result.dataset.provenance.provider,
        instrument=result.dataset.provenance.instrument,
        code_version=__version__,
        seed=seed,
        dataset_ref=result.dataset.provenance.source_uri,
        config=config,
        metadata={
            "provenance": result.dataset.provenance.to_dict(),
            "quality": result.dataset.quality.to_dict(),
        },
    )


def _create_analyses(
    store: SQLiteStore,
    session_id: str,
    inputs: _PersistenceInput,
    spec: EvaluationSpec,
) -> tuple[str, str, str]:
    contract_hash = _contract_hash(spec)
    mtf_analysis_id = store.create_analysis(
        session_id,
        dataset_hash=inputs.dataset_hash,
        config_hash=inputs.analysis_config_hash,
        variant="trend_pullback_v1",
        contract_hash=contract_hash,
        code_version=__version__,
        metadata={"strategy": "trend_pullback_v1"},
    )
    baseline_analysis_id = store.create_analysis(
        session_id,
        dataset_hash=inputs.dataset_hash,
        config_hash=inputs.analysis_config_hash,
        variant="m1_trigger_reference",
        contract_hash=contract_hash,
        code_version=__version__,
        metadata={"strategy": "m1_trigger_reference"},
    )
    return contract_hash, mtf_analysis_id, baseline_analysis_id


def _start_telemetry(
    result: PipelineResult,
    inputs: _PersistenceInput,
    session_id: str,
    *,
    seed: int,
    log_path: str | Path | None,
) -> OperationTelemetry:
    telemetry = OperationTelemetry(
        log_path=log_path or inputs.db.with_suffix(".jsonl"),
        mode="SYNTHETIC",
        session_id=session_id,
        instrument=result.dataset.provenance.instrument,
    )
    gap_map = inputs.config_effective.get("gaps", {})
    if not isinstance(gap_map, Mapping):
        gap_map = {}
    has_gaps = any(bool(values) for values in gap_map.values() if isinstance(values, (list, tuple)))
    telemetry.state.update(
        connection="OFFLINE",
        data_quality="SYNTHETIC",
        continuity="GAPS" if has_gaps else "CONTINUOUS",
        warmup_pending={name: (series.ready_from or len(series.points)) for name, series in result.indicators.items()},
    )
    telemetry.event("pipeline_started", seed=seed, dataset="SINTETICO", bars=len(result.dataset.bars))
    return telemetry


def _persist_candles(store: SQLiteStore, session_id: str, result: PipelineResult) -> None:
    ordinal = 0
    for name in sorted(result.streams, key=lambda value: parse_timeframe(value).seconds):
        series = result.indicators.get(name)
        for index, candle in enumerate(result.streams[name]):
            point = series.points[index] if series is not None and index < len(series.points) else None
            store.save_candle(session_id, _candle_row(candle, point), ordinal=ordinal)
            ordinal += 1


def _persist_source_events(store: SQLiteStore, session_id: str, result: PipelineResult) -> None:
    # Source bars are persisted as candles; no synthetic ticks are fabricated.
    for index, candle in enumerate(result.streams.get("M1", [])):
        store.save_event(
            session_id,
            {
                "event_id": candle.candle_id,
                "source": candle.source,
                "instrument": candle.instrument,
                "event_ts": candle.end,
                "available_ts": candle.available_at or candle.end,
                "kind": "candle_close",
                "price": candle.close,
                "price_base": candle.price_base.value,
                "synthetic": True,
                "source_ordinal": index,
                "derived_from_candle": True,
                "no_tick_interpolation": True,
            },
            ordinal=index,
        )


def _persist_market_data(store: SQLiteStore, session_id: str, result: PipelineResult) -> None:
    _persist_candles(store, session_id, result)
    _persist_source_events(store, session_id, result)


def _decision_row(evaluation: Any) -> dict[str, Any]:
    stable_ts = evaluation.timestamp.isoformat()
    base_row = evaluation.as_dict()
    fingerprint = payload_hash(base_row)[:16]
    row = dict(base_row)
    row.update(
        {
            "decision_id": f"decision:{evaluation.stage}:{stable_ts}:{fingerprint}",
            "observed_ts": evaluation.timestamp,
            "available_ts": evaluation.available_at,
            "kind": evaluation.stage,
            "status": evaluation.decision.value,
        }
    )
    return row


def _blocking_conditions(evaluation: Any) -> list[Any]:
    if evaluation.decision.value not in {"discarded", "blocked"}:
        return []
    blocking = [
        condition
        for condition in evaluation.conditions
        if condition.mandatory and condition.state.value in {"failed", "unknown"}
    ]
    return blocking or [None]


def _discard_row(
    session_id: str, row: Mapping[str, Any], evaluation: Any, condition: Any, ordinal: int
) -> dict[str, Any]:
    reason = (
        (condition.reason if condition is not None else None)
        or (condition.state.value if condition is not None else None)
        or (evaluation.reasons[0] if evaluation.reasons else evaluation.decision.value)
    )
    payload = {"condition": condition.as_dict() if condition is not None else None, "decision": row}
    return {
        "discard_id": f"{session_id}:discard:{row['decision_id']}:{ordinal}:{reason}",
        "decision_id": row["decision_id"],
        "observed_ts": evaluation.timestamp,
        "reason_code": reason,
        "required": True,
        "condition_status": condition.state.value if condition is not None else evaluation.decision.value,
        "payload": payload,
    }


def _persist_decision(
    store: SQLiteStore,
    session_id: str,
    evaluation: Any,
    *,
    ordinal: int,
    analysis_id: str,
    analysis_config_hash: str,
    contract_hash: str,
) -> None:
    row = _decision_row(evaluation)
    store.save_decision(
        session_id,
        row,
        ordinal=ordinal,
        analysis_id=analysis_id,
        variant="trend_pullback_v1",
        analysis_config_hash=analysis_config_hash,
        contract_hash=contract_hash,
        partition="all",
    )
    for condition_ordinal, condition in enumerate(_blocking_conditions(evaluation)):
        store.save_discard(
            session_id,
            _discard_row(session_id, row, evaluation, condition, condition_ordinal),
            ordinal=ordinal * 100 + condition_ordinal,
            analysis_id=analysis_id,
            variant="trend_pullback_v1",
            analysis_config_hash=analysis_config_hash,
            contract_hash=contract_hash,
            partition="all",
        )


def _persist_decisions(
    store: SQLiteStore,
    session_id: str,
    result: PipelineResult,
    *,
    analysis_id: str,
    analysis_config_hash: str,
    contract_hash: str,
) -> None:
    for ordinal, evaluation in enumerate(result.strategy.evaluations):
        _persist_decision(
            store,
            session_id,
            evaluation,
            ordinal=ordinal,
            analysis_id=analysis_id,
            analysis_config_hash=analysis_config_hash,
            contract_hash=contract_hash,
        )


def _persist_signals(
    store: SQLiteStore,
    session_id: str,
    result: PipelineResult,
    baseline_signals: list[dict[str, Any]],
    *,
    analysis_id: str,
    baseline_analysis_id: str,
    analysis_config_hash: str,
    contract_hash: str,
) -> None:
    for ordinal, signal in enumerate(result.strategy.signals):
        store.save_signal(
            session_id,
            signal.as_dict(),
            ordinal=ordinal,
            analysis_id=analysis_id,
            variant="trend_pullback_v1",
            analysis_config_hash=analysis_config_hash,
            contract_hash=contract_hash,
            partition="all",
        )
    for ordinal, signal in enumerate(baseline_signals):
        store.save_signal(
            session_id,
            signal,
            ordinal=ordinal,
            analysis_id=baseline_analysis_id,
            variant="m1_trigger_reference",
            analysis_config_hash=analysis_config_hash,
            contract_hash=contract_hash,
            partition="all",
        )


def _update_telemetry(
    telemetry: OperationTelemetry, result: PipelineResult, baseline_signals: list[dict[str, Any]]
) -> None:
    m1_stream = result.streams.get("M1", [])
    telemetry.state.update(
        events_processed=len(m1_stream),
        candles_processed=sum(map(len, result.streams.values())),
        signals=len(result.strategy.signals) + len(baseline_signals),
        discards=sum(e.decision.value in {"discarded", "blocked"} for e in result.strategy.evaluations),
        last_received_ts=max((c.end for c in m1_stream), default=None),
        last_available_ts=max((c.available_at or c.end for c in m1_stream), default=None),
    )


def _backtest_variants() -> tuple[VariantSpec, VariantSpec]:
    return (
        VariantSpec(
            "m1_trigger_reference",
            "Referencia controlada: sólo disparador M1",
            {"context": False, "preparation": False},
            mode="M1_REFERENCE",
        ),
        VariantSpec(
            "trend_pullback_v1",
            "Contexto M15 + preparación M5 + disparador M1",
            {"context": True, "preparation": True},
            mode="MULTITIMEFRAME",
        ),
    )


def _run_backtest(
    store: SQLiteStore,
    session_id: str,
    result: PipelineResult,
    inputs: _PersistenceInput,
    spec: EvaluationSpec,
    telemetry: OperationTelemetry,
    *,
    mtf_analysis_id: str,
    baseline_analysis_id: str,
    contract_hash: str,
) -> list[dict[str, Any]]:
    points = points_from_candles(result.streams.get(inputs.trigger_name, []))
    simulator = VirtualContractSimulator(spec=spec)
    runner = BacktestRunner(simulator=simulator, store=store, session_id=session_id, logger=telemetry.logger)
    reference_variant, strategy_variant = _backtest_variants()
    backtests = runner.run(
        inputs.baseline_signals,
        points,
        variants=[reference_variant],
        data_quality="SYNTHETIC",
        resolution=inputs.trigger_name,
        analysis_id=baseline_analysis_id,
        analysis_name="m1_trigger_reference",
        analysis_config_hash=inputs.analysis_config_hash,
        contract_hash=contract_hash,
    )
    backtests += runner.run(
        [signal.as_dict() for signal in result.strategy.signals],
        points,
        variants=[strategy_variant],
        data_quality="SYNTHETIC",
        resolution=inputs.trigger_name,
        analysis_id=mtf_analysis_id,
        analysis_name="trend_pullback_v1",
        analysis_config_hash=inputs.analysis_config_hash,
        contract_hash=contract_hash,
    )
    simulations = [item.to_dict(include_simulations=False) for item in backtests]
    for item in backtests:
        store.save_metric(
            session_id,
            f"backtest:{item.variant}",
            {"variant": item.variant, "config_hash": item.config_hash},
            item.net_result,
            item.to_dict(include_simulations=False),
        )
    return simulations


def _finish_pipeline(
    store: SQLiteStore,
    session_id: str,
    result: PipelineResult,
    telemetry: OperationTelemetry,
    simulations: list[dict[str, Any]],
    db: Path,
) -> dict[str, Any]:
    store.save_checkpoint(
        session_id,
        "pipeline",
        cursor={"last_candle": len(result.streams.get("M1", []))},
        events_processed=len(result.streams.get("M1", [])),
        state={"signals": len(result.strategy.signals)},
    )
    telemetry.event("pipeline_complete", signals=len(result.strategy.signals), simulations=simulations, synthetic=True)
    telemetry.close()
    store.finish_session(session_id, status="COMPLETED")
    report = ReportBuilder(store, session_id).summary()
    report_path = db.with_name(f"{db.stem}-report.md")
    ReportBuilder(store, session_id).write(report_path, format="markdown", data=report)
    return report


def _persist_to_store(
    result: PipelineResult,
    inputs: _PersistenceInput,
    *,
    seed: int,
    log_path: str | Path | None,
    run_backtest: bool,
) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    with SQLiteStore(inputs.db) as store:
        session_id = _create_session(store, result, seed=seed, config=inputs.config_effective)
        store.save_config(session_id, "effective", inputs.config_effective)
        spec = evaluation_spec_from_config(inputs.config_effective)
        contract_hash, mtf_analysis_id, baseline_analysis_id = _create_analyses(store, session_id, inputs, spec)
        telemetry = _start_telemetry(result, inputs, session_id, seed=seed, log_path=log_path)
        _persist_market_data(store, session_id, result)
        _persist_decisions(
            store,
            session_id,
            result,
            analysis_id=mtf_analysis_id,
            analysis_config_hash=inputs.analysis_config_hash,
            contract_hash=contract_hash,
        )
        _persist_signals(
            store,
            session_id,
            result,
            inputs.baseline_signals,
            analysis_id=mtf_analysis_id,
            baseline_analysis_id=baseline_analysis_id,
            analysis_config_hash=inputs.analysis_config_hash,
            contract_hash=contract_hash,
        )
        _update_telemetry(telemetry, result, inputs.baseline_signals)
        simulations = (
            _run_backtest(
                store,
                session_id,
                result,
                inputs,
                spec,
                telemetry,
                mtf_analysis_id=mtf_analysis_id,
                baseline_analysis_id=baseline_analysis_id,
                contract_hash=contract_hash,
            )
            if run_backtest
            else []
        )
        report = _finish_pipeline(store, session_id, result, telemetry, simulations, inputs.db)
    return session_id, simulations, report


def persist_pipeline(
    result: PipelineResult,
    db_path: str | Path,
    *,
    seed: int,
    config: Mapping[str, Any] | None = None,
    log_path: str | Path | None = None,
    run_backtest: bool = True,
) -> PipelineResult:
    inputs = _prepare_persistence_input(result, db_path, seed=seed, config=config)
    session_id, simulations, report = _persist_to_store(
        result,
        inputs,
        seed=seed,
        log_path=log_path,
        run_backtest=run_backtest,
    )
    result.baseline_signals = inputs.baseline_signals
    result.session_id = session_id
    result.simulations = simulations
    result.report = report
    return result


def run_incremental_dataset(
    dataset: DataSet, *, config: Mapping[str, Any] | Any | None = None
) -> tuple[dict[str, list[Candle]], dict[str, IndicatorSeries], StrategyResult, IncrementalProcessor]:
    """Process a finite dataset through the same state machine used by watch."""
    config_map = _config_mapping(config)
    strategy_map = dict(config_map.get("strategy", {})) if isinstance(config_map.get("strategy", {}), Mapping) else {}
    sim_spec = evaluation_spec_from_config(config_map)
    instrument_cfg = config_map.get("instrument", {}) if isinstance(config_map.get("instrument", {}), Mapping) else {}
    declared_base = str(instrument_cfg.get("price_base", "traded")).lower()
    runtime_base = "traded" if declared_base in {"close", "trade"} else declared_base
    tf_section = config_map.get("timeframes", {}) if isinstance(config_map.get("timeframes", {}), Mapping) else {}
    timeframes = tf_section.get("values", ("M1", "M5", "M15"))
    runtime_sim = RuntimeSimulationConfig(
        horizons_seconds=sim_spec.horizons_seconds,
        entry_latency_seconds=sim_spec.entry_latency_seconds,
        entry_rule=sim_spec.entry_rule,
        exit_rule=sim_spec.exit_rule,
        max_price_age_seconds=sim_spec.max_price_age_seconds,
        stake=sim_spec.stake,
        payout_net=sim_spec.payout_net,
        loss_amount=sim_spec.loss_amount,
        tie_net=sim_spec.tie_net,
        tie_tolerance=sim_spec.tie_tolerance,
        costs=sim_spec.costs,
        horizon_from=sim_spec.horizon_from,
        requested_base_price=runtime_base,
        resolution="M1",
    )
    processor = IncrementalProcessor(
        strategy=strategy_map or None,
        simulation=runtime_sim,
        timeframes=timeframes,
        mode=OperationMode.SYNTHETIC,
        instrument=dataset.provenance.instrument,
        source="synthetic-runtime",
        price_base=runtime_base,
    )
    for bar in sorted(dataset.bars, key=lambda item: item.interval_start):
        processor.process_bar(bar)
    last = max((bar.interval_end for bar in dataset.bars), default=None)
    if last is not None:
        processor.finalize(last)
    streams = {name: list(values) for name, values in processor.candles.items()}
    indicators: dict[str, IndicatorSeries] = {}
    for name, engine in processor.indicator_engines.items():
        series = engine.series
        indicators[name] = IndicatorSeries(
            series.timeframe,
            series.instrument,
            processor.strategy_config.indicators,
            tuple(processor.indicator_points[name]),
        )
    strategy = StrategyResult(
        tuple(processor.evaluations), tuple(processor.signals), tuple(processor.episodes.values())
    )
    return streams, indicators, strategy, processor


def run_synthetic_pipeline(
    db_path: str | Path,
    *,
    seed: int = 42,
    periods_each: int = 600,
    config: Mapping[str, Any] | Any | None = None,
    log_path: str | Path | None = None,
) -> PipelineResult:
    config_map = _config_mapping(config)
    instrument_cfg = config_map.get("instrument", {})
    instrument = str(instrument_cfg.get("symbol", "SYNTH/USD")) if isinstance(instrument_cfg, Mapping) else "SYNTH/USD"
    generator = SyntheticGenerator(seed=seed, instrument=instrument)
    dataset = generator.generate_scenarios(periods_each=periods_each)
    streams, indicators, strategy, processor = run_incremental_dataset(dataset, config=config_map)
    result = PipelineResult(dataset, streams, indicators, strategy)
    # El mapa de gaps es visible aunque el dataset sintético esperado sea
    # continuo; el runtime no crea velas vacías para cubrirlos.
    gaps = {name: [] for name in streams if name != "M1"}
    result = persist_pipeline(
        result,
        db_path,
        seed=seed,
        config={**config_map, "synthetic": True, "gaps": gaps, "runtime_status": processor.status},
        log_path=log_path,
    )
    return result
