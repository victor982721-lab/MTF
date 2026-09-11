"""Integración vertical de MTF Lab.

Este módulo es un adaptador fino: convierte registros de proveedores al modelo
canónico, construye temporalidades desde velas sin inventar ticks, ejecuta el
mismo detector para replay/sintético y deja todo en SQLite.  No contiene una
segunda implementación de indicadores ni de reglas.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import __version__
from .core import (
    Candle,
    DataQuality,
    IndicatorConfig,
    IndicatorSeries,
    OperationMode,
    PriceBase,
    StrategyResult,
    Timeframe,
    TrendPullbackStrategy,
    aggregate_events,
    compute_indicators,
    parse_timeframe,
)
from .core.quality import merge_quality
from .data import DataSet, Event as ProviderEvent, Bar as ProviderBar, SyntheticGenerator
from .data.translation import data_bar_to_core, data_event_to_core, TranslationError
from .ops.backtest import BacktestRunner, VariantSpec
from .ops.logging_state import OperationTelemetry
from .ops.persistence import SQLiteStore, payload_hash
from .ops.reporting import ReportBuilder
from .ops.simulation import EvaluationSpec, PricePoint, VirtualContractSimulator
from .runtime import IncrementalProcessor, SimulationConfig as RuntimeSimulationConfig


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
            raise TranslationError(f"modo de vela incompatible: {translated.mode.value} vs {requested_mode.value}", code="MODE_CONFLICT")
        return translated
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
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace("Z", "+00:00"))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace("Z", "+00:00"))
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("provider bar timestamps must include timezone")
    tf = parse_timeframe(resolution)
    if "price_basis" not in values and "price_type" not in values and not hasattr(bar, "price_basis"):
        raise TranslationError("vela sin base de precio explícita", code="PRICE_BASE_UNKNOWN")
    price_basis = getattr(bar, "price_basis", values.get("price_basis", values.get("price_type")))
    if hasattr(price_basis, "value"):
        price_basis = price_basis.value
    if str(price_basis).lower() == "close":
        # Candle.close es el valor de cierre de la base declarada; el modelo
        # canónico usa traded como alias numérico para una vela OHLC.
        price_basis = "traded"
    quality = getattr(bar, "quality", values.get("quality", None))
    if not isinstance(quality, DataQuality):
        from .runtime.state import quality_from
        quality = quality_from(quality, synthetic=bool(getattr(bar, "synthetic", values.get("synthetic", False))))
    closed = bool(getattr(bar, "closed", values.get("closed", True)))
    available = getattr(bar, "available_at", values.get("available_at", None))
    received = getattr(bar, "received_at", values.get("received_at", None))
    return Candle(
        instrument=str(instrument), timeframe=tf, start=start, end=end,
        open=float(getattr(bar, "open", values.get("open"))),
        high=float(getattr(bar, "high", values.get("high"))),
        low=float(getattr(bar, "low", values.get("low"))),
        close=float(getattr(bar, "close", values.get("close"))),
        volume=float(getattr(bar, "volume", values.get("volume", 0.0)) or 0.0),
        event_count=int(getattr(bar, "trade_count", values.get("trade_count", values.get("event_count", 0))) or 0),
        source=str(getattr(bar, "source", values.get("source", "provider"))),
        mode=_mode(mode),
        price_base=PriceBase(str(price_basis).lower()), closed=closed,
        available_at=available, received_at=received, quality=quality,
        candle_id=str(getattr(bar, "data_id", getattr(bar, "candle_id", values.get("candle_id", ""))) or "") or None,
        origin=str(getattr(bar, "origin", values.get("origin", "provider"))),
        metadata=getattr(bar, "metadata", values.get("metadata", {})) or {},
    )


def provider_event_to_core(event: Any, *, mode: str | OperationMode = OperationMode.REPLAY):
    """Map a provider Event preserving quality/provenance/base/identity."""
    from .core import EventKind, MarketEvent
    if isinstance(event, MarketEvent):
        return event
    if isinstance(event, ProviderEvent):
        translated = data_event_to_core(event)
        requested_mode = _mode(mode)
        if translated.mode is not requested_mode and requested_mode is not OperationMode.REPLAY:
            raise TranslationError(f"modo de evento incompatible: {translated.mode.value} vs {requested_mode.value}", code="MODE_CONFLICT")
        return translated
    basis = getattr(event, "price_basis", getattr(event, "price_base", getattr(event, "price_type", None)))
    if basis is None:
        raise TranslationError("evento sin base de precio explícita", code="PRICE_BASE_UNKNOWN")
    if hasattr(basis, "value"):
        basis = basis.value
    basis = "traded" if basis in {"trade", "close"} else str(basis).lower()
    if basis == "bid" and getattr(event, "bid", None) is None:
        raise TranslationError("evento BID sin bid explícito", code="PRICE_BASE_MISSING")
    if basis == "ask" and getattr(event, "ask", None) is None:
        raise TranslationError("evento ASK sin ask explícito", code="PRICE_BASE_MISSING")
    if basis == "mid" and (getattr(event, "mid", None) is None or getattr(event, "bid", None) is None or getattr(event, "ask", None) is None):
        raise TranslationError("evento MID requiere mid, bid y ask explícitos", code="PRICE_BASE_MISSING")
    et = getattr(event, "event_time", None)
    if isinstance(et, str):
        et = datetime.fromisoformat(et.replace("Z", "+00:00"))
    if et is None or et.tzinfo is None:
        raise ValueError("provider event timestamp must include timezone")
    quality = getattr(event, "quality", None)
    if not isinstance(quality, DataQuality):
        from .runtime.state import quality_from
        quality = quality_from(quality, synthetic=bool(getattr(event, "synthetic", False)))
    event_kind_raw = getattr(event, "event_kind", getattr(event, "kind", "trade"))
    if hasattr(event_kind_raw, "value"):
        event_kind_raw = event_kind_raw.value
    try:
        event_kind = EventKind(str(event_kind_raw).lower())
    except ValueError as exc:
        raise TranslationError(f"event_kind desconocido: {event_kind_raw!r}", code="EVENT_KIND_UNKNOWN") from exc
    return MarketEvent(
        instrument=str(getattr(event, "instrument", "unknown")), event_time=et,
        price=getattr(event, "price", None) if basis == "traded" else None,
        quantity=getattr(event, "quantity", None),
        bid=getattr(event, "bid", None), ask=getattr(event, "ask", None),
        received_at=getattr(event, "received_at", None), available_at=getattr(event, "available_at", None),
        source=str(getattr(event, "source", "provider")), mode=_mode(mode),
        price_base=PriceBase(basis), event_kind=event_kind,
        sequence=getattr(event, "source_sequence", getattr(event, "sequence", None)),
        source_event_id=getattr(event, "source_event_id", None),
        event_id=getattr(event, "event_id", None), quality=quality,
        metadata=getattr(event, "metadata", {}) or {},
    )


def resample_candles(candles: Iterable[Candle], target: Timeframe | str) -> tuple[list[Candle], list[str]]:
    """OHLC-resample complete base candles; incomplete buckets remain gaps."""
    rows = sorted(list(candles), key=lambda x: x.start)
    if not rows:
        return [], []
    tf = parse_timeframe(target)
    base = rows[0].timeframe
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
        output.append(Candle(
            instrument=group[0].instrument, timeframe=tf, start=start, end=end,
            open=group[0].open, high=max(x.high for x in group), low=min(x.low for x in group),
            close=group[-1].close, volume=sum(x.volume for x in group),
            event_count=sum(x.event_count for x in group), source="aggregated-from-bars",
            mode=group[0].mode, price_base=group[0].price_base, closed=True,
            available_at=max((x.available_at or x.end for x in group)),
            received_at=max((x.received_at or x.end for x in group)),
            quality=merge_quality(*(x.quality for x in group), source="aggregated-from-bars"),
            origin="resampled_ohlc", metadata={"base_timeframe": base.name, "ratio": ratio},
        ))
    return output, gaps


def build_streams(bars: Iterable[Any], *, mode: str | OperationMode = OperationMode.SYNTHETIC, targets: Iterable[str] = ("M5", "M15")) -> tuple[dict[str, list[Candle]], dict[str, list[str]]]:
    base = [provider_bar_to_candle(x, mode=mode) for x in bars]
    base.sort(key=lambda x: x.start)
    if not base:
        return {}, {}
    base_timeframe_name = base[0].timeframe.name
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
    return [{"point_id": c.candle_id, "timestamp": c.end, "end_ts": c.end,
             "instrument": c.instrument, "price": c.close, "base_price": c.price_base.value, "source": c.source,
             "quality": c.quality.status, "resolution": c.timeframe.name,
             "closed": c.closed, "received_at": c.received_at,
             "available_at": c.available_at or c.end} for c in candles]


def generate_synthetic_streams(*, seed: int = 42, periods_each: int = 600, targets: Iterable[str] = ("M5", "M15"), instrument: str = "SYNTH/USD") -> tuple[DataSet, dict[str, list[Candle]], dict[str, list[str]]]:
    generator = SyntheticGenerator(seed=seed, instrument=instrument)
    dataset = generator.generate_scenarios(periods_each=periods_each)
    streams, gaps = build_streams(dataset.bars, mode=OperationMode.SYNTHETIC, targets=targets)
    return dataset, streams, gaps


def _config_mapping(config: Any | None) -> dict[str, Any]:
    if config is None:
        return {}
    if hasattr(config, "to_dict") and callable(config.to_dict):
        return dict(config.to_dict())
    if isinstance(config, Mapping):
        return dict(config)
    raise TypeError("config debe ser EffectiveConfig o mapping")


def run_detector(streams: Mapping[str, Iterable[Candle]], *, mode: str | OperationMode = OperationMode.SYNTHETIC, config: Mapping[str, Any] | None = None) -> tuple[dict[str, IndicatorSeries], StrategyResult]:
    config_map = _config_mapping(config)
    indicator_config = IndicatorConfig.from_mapping(config_map.get("indicators")) if config_map else IndicatorConfig()
    indicators: dict[str, IndicatorSeries] = {}
    for name, rows in streams.items():
        indicators[name] = compute_indicators(list(rows), indicator_config)
    strategy_cfg: dict[str, Any] = dict(config_map.get("strategy", {}))
    strategy_cfg["mode"] = _mode(mode)
    strategy_cfg.setdefault("indicators", {"ema_fast": indicator_config.ema_fast, "ema_slow": indicator_config.ema_slow, "rsi_period": indicator_config.rsi_period, "atr_period": indicator_config.atr_period})
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



from .core.reference import m1_reference_signals


def evaluation_spec_from_config(config: Mapping[str, Any] | Any | None = None) -> EvaluationSpec:
    """Adaptador único hacia :class:`EvaluationSpec`."""
    from .configuration import normalize_simulation_mapping
    return EvaluationSpec(**normalize_simulation_mapping(config))

def _candle_row(c: Candle, point: Any | None = None) -> dict[str, Any]:
    return {"candle_id": c.candle_id, "instrument": c.instrument, "timeframe": c.timeframe.name,
            "start_ts": c.start, "end_ts": c.end, "open": c.open, "high": c.high, "low": c.low,
            "close": c.close, "volume": c.volume, "event_count": c.event_count, "closed": c.closed,
            "source": c.source, "mode": c.mode.value, "origin": c.origin,
            "price_base": c.price_base.value, "quality": c.quality.status,
            "available_ts": c.available_at or c.end, "received_ts": c.received_at,
            "provenance": {"origin": c.origin, "metadata": dict(c.metadata),
                "indicators": ({"ema_fast": point.ema_fast, "ema_slow": point.ema_slow, "rsi": point.rsi, "atr": point.atr} if point is not None else {})}}


def persist_pipeline(result: PipelineResult, db_path: str | Path, *, seed: int, config: Mapping[str, Any] | None = None, log_path: str | Path | None = None, run_backtest: bool = True) -> PipelineResult:
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
    dataset_hash = hashlib.sha256(json.dumps([row.to_dict() for row in result.dataset.bars], sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()
    config_effective.setdefault("dataset_hash", dataset_hash)
    strategy_map = config_effective.get("strategy", {}) if isinstance(config_effective.get("strategy", {}), Mapping) else {}
    trigger_name = str(strategy_map.get("trigger_timeframe", "M1")).upper()
    baseline_signals = m1_reference_signals(result.indicators[trigger_name], rsi_threshold=float(strategy_map.get("rsi_threshold", 50.0)), mode="SYNTHETIC", identity_salt=analysis_config_hash) if trigger_name in result.indicators else []
    with SQLiteStore(db) as store:
        sid = store.create_session(mode="SYNTHETIC", provider=result.dataset.provenance.provider, instrument=result.dataset.provenance.instrument,
                                  code_version=__version__, seed=seed, dataset_ref=result.dataset.provenance.source_uri,
                                  config=config_effective, metadata={"provenance": result.dataset.provenance.to_dict(), "quality": result.dataset.quality.to_dict()})
        store.save_config(sid, "effective", config_effective)
        spec = evaluation_spec_from_config(config_effective)
        contract_payload = {"stake": spec.stake, "payout_net": spec.payout_net, "loss_amount": spec.loss_amount, "tie_net": spec.tie_net, "costs": spec.costs, "tie_tolerance": spec.tie_tolerance, "entry_rule": spec.entry_rule, "exit_rule": spec.exit_rule, "horizon_from": spec.horizon_from, "requested_base_price": spec.requested_base_price}
        contract_hash = hashlib.sha256(json.dumps(contract_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        mtf_analysis_id = store.create_analysis(sid, dataset_hash=dataset_hash, config_hash=analysis_config_hash, variant="trend_pullback_v1", contract_hash=contract_hash, code_version=__version__, metadata={"strategy": "trend_pullback_v1"})
        baseline_analysis_id = store.create_analysis(sid, dataset_hash=dataset_hash, config_hash=analysis_config_hash, variant="m1_trigger_reference", contract_hash=contract_hash, code_version=__version__, metadata={"strategy": "m1_trigger_reference"})
        telemetry = OperationTelemetry(log_path=log_path or db.with_suffix(".jsonl"), mode="SYNTHETIC", session_id=sid, instrument=result.dataset.provenance.instrument)
        gap_map = config_effective.get("gaps", {}) if isinstance(config_effective.get("gaps", {}), Mapping) else {}
        has_gaps = any(bool(values) for values in gap_map.values() if isinstance(values, (list, tuple)))
        telemetry.state.update(connection="OFFLINE", data_quality="SYNTHETIC", continuity="GAPS" if has_gaps else "CONTINUOUS", warmup_pending={name: (series.ready_from or len(series.points)) for name, series in result.indicators.items()})
        telemetry.event("pipeline_started", seed=seed, dataset="SINTETICO", bars=len(result.dataset.bars))
        ordinal = 0
        for name in sorted(result.streams, key=lambda x: parse_timeframe(x).seconds):
            series = result.indicators.get(name)
            for index, candle in enumerate(result.streams[name]):
                point = series.points[index] if series is not None and index < len(series.points) else None
                store.save_candle(sid, _candle_row(candle, point), ordinal=ordinal); ordinal += 1
        # Source bars are persisted as candles; no synthetic ticks are fabricated.
        for i, candle in enumerate(result.streams.get("M1", [])):
            store.save_event(sid, {"event_id": candle.candle_id, "source": candle.source, "instrument": candle.instrument,
                                   "event_ts": candle.end, "available_ts": candle.available_at or candle.end,
                                   "kind": "candle_close", "price": candle.close, "price_base": candle.price_base.value,
                                   "synthetic": True, "source_ordinal": i, "derived_from_candle": True, "no_tick_interpolation": True}, ordinal=i)
        for i, evaluation in enumerate(result.strategy.evaluations):
            stable_ts = evaluation.timestamp.isoformat()
            base_row = evaluation.as_dict()
            decision_fingerprint = payload_hash(base_row)[:16]
            row = dict(base_row); row.update({"decision_id": f"decision:{evaluation.stage}:{stable_ts}:{decision_fingerprint}", "observed_ts": evaluation.timestamp, "available_ts": evaluation.available_at, "kind": evaluation.stage, "status": evaluation.decision.value})
            store.save_decision(sid, row, ordinal=i, analysis_id=mtf_analysis_id, variant="trend_pullback_v1", analysis_config_hash=analysis_config_hash, contract_hash=contract_hash, partition="all")
            if evaluation.decision.value in {"discarded", "blocked"}:
                blocking = [condition for condition in evaluation.conditions if condition.mandatory and condition.state.value in {"failed", "unknown"}]
                if not blocking:
                    blocking = [None]
                for condition_ordinal, condition in enumerate(blocking):
                    reason = (condition.reason if condition is not None else None) or (condition.state.value if condition is not None else None) or (evaluation.reasons[0] if evaluation.reasons else evaluation.decision.value)
                    payload = {"condition": condition.as_dict() if condition is not None else None, "decision": row}
                    store.save_discard(sid, {"discard_id": f"{sid}:discard:{row["decision_id"]}:{condition_ordinal}:{reason}", "decision_id": row["decision_id"], "observed_ts": evaluation.timestamp, "reason_code": reason, "required": True, "condition_status": condition.state.value if condition is not None else evaluation.decision.value, "payload": payload}, ordinal=i * 100 + condition_ordinal, analysis_id=mtf_analysis_id, variant="trend_pullback_v1", analysis_config_hash=analysis_config_hash, contract_hash=contract_hash, partition="all")
        for i, signal in enumerate(result.strategy.signals):
            store.save_signal(sid, signal.as_dict(), ordinal=i, analysis_id=mtf_analysis_id, variant="trend_pullback_v1", analysis_config_hash=analysis_config_hash, contract_hash=contract_hash, partition="all")
        for i, signal in enumerate(baseline_signals):
            store.save_signal(sid, signal, ordinal=i, analysis_id=baseline_analysis_id, variant="m1_trigger_reference", analysis_config_hash=analysis_config_hash, contract_hash=contract_hash, partition="all")
        telemetry.state.update(events_processed=len(result.streams.get("M1", [])), candles_processed=sum(map(len, result.streams.values())), signals=len(result.strategy.signals) + len(baseline_signals), discards=sum(e.decision.value in {"discarded", "blocked"} for e in result.strategy.evaluations), last_received_ts=max((c.end for c in result.streams.get("M1", [])), default=None), last_available_ts=max((c.available_at or c.end for c in result.streams.get("M1", [])), default=None))
        simulations: list[dict[str, Any]] = []
        if run_backtest:
            points = points_from_candles(result.streams.get(trigger_name, []))
            simulator = VirtualContractSimulator(spec=spec)
            runner = BacktestRunner(simulator=simulator, store=store, session_id=sid, logger=telemetry.logger)
            variants = [VariantSpec("m1_trigger_reference", "Referencia controlada: sólo disparador M1", {"context": False, "preparation": False}, mode="M1_REFERENCE"), VariantSpec("trend_pullback_v1", "Contexto M15 + preparación M5 + disparador M1", {"context": True, "preparation": True}, mode="MULTITIMEFRAME")]
            bt = runner.run(baseline_signals, points, variants=[variants[0]], data_quality="SYNTHETIC", resolution=trigger_name, analysis_id=baseline_analysis_id, analysis_name="m1_trigger_reference", analysis_config_hash=analysis_config_hash, contract_hash=contract_hash)
            bt += runner.run([s.as_dict() for s in result.strategy.signals], points, variants=[variants[1]], data_quality="SYNTHETIC", resolution=trigger_name, analysis_id=mtf_analysis_id, analysis_name="trend_pullback_v1", analysis_config_hash=analysis_config_hash, contract_hash=contract_hash)
            simulations = [x.to_dict(include_simulations=False) for x in bt]
            for x in bt:
                store.save_metric(sid, f"backtest:{x.variant}", {"variant": x.variant, "config_hash": x.config_hash}, x.net_result, x.to_dict(include_simulations=False))
        store.save_checkpoint(sid, "pipeline", cursor={"last_candle": len(result.streams.get("M1", []))}, events_processed=len(result.streams.get("M1", [])), state={"signals": len(result.strategy.signals)})
        telemetry.event("pipeline_complete", signals=len(result.strategy.signals), simulations=simulations, synthetic=True)
        telemetry.close(); store.finish_session(sid, status="COMPLETED")
        report = ReportBuilder(store, sid).summary()
        report_path = db.with_name(f"{db.stem}-report.md")
        ReportBuilder(store, sid).write(report_path, format="markdown", data=report)
    result.baseline_signals = baseline_signals
    result.session_id = sid; result.simulations = simulations; result.report = report
    return result



def run_incremental_dataset(dataset: DataSet, *, config: Mapping[str, Any] | Any | None = None) -> tuple[dict[str, list[Candle]], dict[str, IndicatorSeries], StrategyResult, IncrementalProcessor]:
    """Process a finite dataset through the same state machine used by watch."""
    config_map = _config_mapping(config)
    strategy_map = dict(config_map.get("strategy", {})) if isinstance(config_map.get("strategy", {}), Mapping) else {}
    indicator_map = config_map.get("indicators", {}) if isinstance(config_map.get("indicators", {}), Mapping) else {}
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
        stake=sim_spec.stake, payout_net=sim_spec.payout_net,
        loss_amount=sim_spec.loss_amount, tie_net=sim_spec.tie_net,
        tie_tolerance=sim_spec.tie_tolerance, costs=sim_spec.costs,
        horizon_from=sim_spec.horizon_from,
        requested_base_price=runtime_base,
        resolution="M1",
    )
    processor = IncrementalProcessor(strategy=strategy_map or None, simulation=runtime_sim, timeframes=timeframes, mode=OperationMode.SYNTHETIC, instrument=dataset.provenance.instrument, source="synthetic-runtime", price_base=runtime_base)
    for bar in sorted(dataset.bars, key=lambda item: item.interval_start):
        processor.process_bar(bar)
    last = max((bar.interval_end for bar in dataset.bars), default=None)
    if last is not None:
        processor.finalize(last)
    streams = {name: list(values) for name, values in processor.candles.items()}
    indicators: dict[str, IndicatorSeries] = {}
    for name, engine in processor.indicator_engines.items():
        series = engine.series
        indicators[name] = IndicatorSeries(series.timeframe, series.instrument, processor.strategy_config.indicators, tuple(processor.indicator_points[name]))
    strategy = StrategyResult(tuple(processor.evaluations), tuple(processor.signals), tuple(processor.episodes.values()))
    return streams, indicators, strategy, processor

def run_synthetic_pipeline(db_path: str | Path, *, seed: int = 42, periods_each: int = 600, config: Mapping[str, Any] | Any | None = None, log_path: str | Path | None = None) -> PipelineResult:
    config_map = _config_mapping(config)
    target_values = config_map.get("timeframes", {}).get("values", ("1m", "5m", "15m")) if isinstance(config_map.get("timeframes", {}), Mapping) else ("1m", "5m", "15m")
    targets = [str(value) for value in target_values if str(value).upper().replace("M", "m") not in {"1m", "m1"}]
    instrument_cfg = config_map.get("instrument", {})
    instrument = str(instrument_cfg.get("symbol", "SYNTH/USD")) if isinstance(instrument_cfg, Mapping) else "SYNTH/USD"
    generator = SyntheticGenerator(seed=seed, instrument=instrument)
    dataset = generator.generate_scenarios(periods_each=periods_each)
    streams, indicators, strategy, processor = run_incremental_dataset(dataset, config=config_map)
    result = PipelineResult(dataset, streams, indicators, strategy)
    # El mapa de gaps es visible aunque el dataset sintético esperado sea
    # continuo; el runtime no crea velas vacías para cubrirlos.
    gaps = {name: [] for name in streams if name != "M1"}
    result = persist_pipeline(result, db_path, seed=seed, config={**config_map, "synthetic": True, "gaps": gaps, "runtime_status": processor.status}, log_path=log_path)
    return result
