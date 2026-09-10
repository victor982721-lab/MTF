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
from .ops.backtest import BacktestRunner, VariantSpec
from .ops.logging_state import OperationTelemetry
from .ops.persistence import SQLiteStore
from .ops.reporting import ReportBuilder
from .ops.simulation import EvaluationSpec, PricePoint, VirtualContractSimulator


@dataclass(slots=True)
class PipelineResult:
    dataset: DataSet
    streams: dict[str, list[Candle]]
    indicators: dict[str, IndicatorSeries]
    strategy: StrategyResult
    session_id: str | None = None
    simulations: list[dict[str, Any]] | None = None
    report: dict[str, Any] | None = None


def _mode(value: str | OperationMode) -> OperationMode:
    if isinstance(value, OperationMode):
        return value
    text = str(value).upper().replace("OBSERVACIÓN EN DIRECTO", "LIVE")
    return OperationMode(text)


def provider_bar_to_candle(bar: Any, *, mode: str | OperationMode = OperationMode.REPLAY) -> Candle:
    """Map a provider-neutral Bar to the core Candle without price swapping."""
    if isinstance(bar, Candle):
        return bar
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
    price_basis = getattr(bar, "price_basis", values.get("price_basis", "traded"))
    if hasattr(price_basis, "value"):
        price_basis = price_basis.value
    if str(price_basis).lower() == "close":
        # Candle.close es el valor de cierre de la base declarada; el modelo
        # canónico usa traded como alias numérico para una vela OHLC.
        price_basis = "traded"
    quality = getattr(bar, "quality", None)
    if not isinstance(quality, DataQuality):
        quality = DataQuality.good(synthetic=bool(getattr(bar, "synthetic", False)))
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
    """Map a provider Event to MarketEvent, preserving its declared price base."""
    from .core import EventKind, MarketEvent
    if isinstance(event, MarketEvent):
        return event
    basis = getattr(event, "price_basis", getattr(event, "price_base", "traded"))
    if hasattr(basis, "value"):
        basis = basis.value
    basis = "traded" if basis in {"trade", "close"} else str(basis).lower()
    et = getattr(event, "event_time", None)
    if isinstance(et, str):
        et = datetime.fromisoformat(et.replace("Z", "+00:00"))
    if et is None or et.tzinfo is None:
        raise ValueError("provider event timestamp must include timezone")
    quality = getattr(event, "quality", None)
    if not isinstance(quality, DataQuality):
        quality = DataQuality.good(synthetic=bool(getattr(event, "synthetic", False)))
    return MarketEvent(
        instrument=str(getattr(event, "instrument", "unknown")), event_time=et,
        price=getattr(event, "price", None) if basis == "traded" else None,
        quantity=getattr(event, "quantity", None),
        bid=getattr(event, "bid", None), ask=getattr(event, "ask", None),
        received_at=getattr(event, "received_at", None), available_at=getattr(event, "available_at", None),
        source=str(getattr(event, "source", "provider")), mode=_mode(mode),
        price_base=PriceBase(basis), event_kind=EventKind.TRADE,
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
    return [{"timestamp": c.end, "end_ts": c.end, "price": c.close,
             "base_price": c.price_base.value, "source": c.source,
             "quality": c.quality.status, "resolution": c.timeframe.name,
             "closed": c.closed} for c in candles]


def generate_synthetic_streams(*, seed: int = 42, periods_each: int = 600, targets: Iterable[str] = ("M5", "M15"), instrument: str = "SYNTH/USD") -> tuple[DataSet, dict[str, list[Candle]], dict[str, list[str]]]:
    generator = SyntheticGenerator(seed=seed, instrument=instrument)
    dataset = generator.generate_scenarios(periods_each=periods_each)
    streams, gaps = build_streams(dataset.bars, mode=OperationMode.SYNTHETIC, targets=targets)
    return dataset, streams, gaps


def run_detector(streams: Mapping[str, Iterable[Candle]], *, mode: str | OperationMode = OperationMode.SYNTHETIC, config: Mapping[str, Any] | None = None) -> tuple[dict[str, IndicatorSeries], StrategyResult]:
    indicator_config = IndicatorConfig.from_mapping((config or {}).get("indicators")) if config else IndicatorConfig()
    indicators: dict[str, IndicatorSeries] = {}
    for name, rows in streams.items():
        indicators[name] = compute_indicators(list(rows), indicator_config)
    strategy_cfg: dict[str, Any] = dict((config or {}).get("strategy", {}))
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



def m1_reference_signals(series: IndicatorSeries) -> list[dict[str, Any]]:
    """Referencia controlada: sólo cruce EMA20 + RSI en M1.

    Reutiliza los mismos puntos calculados por ``compute_indicators``; no es
    una segunda versión de trend_pullback_v1 y se identifica como variante.
    """
    rows: list[dict[str, Any]] = []
    for index, point in enumerate(series.points):
        if index == 0 or not point.ready:
            continue
        previous = series.points[index - 1]
        if not previous.ready or previous.ema_fast is None or point.ema_fast is None or point.rsi is None:
            continue
        direction: str | None = None
        if previous.close <= previous.ema_fast and point.close > point.ema_fast and point.rsi > 50.0:
            direction = "UP"
        elif previous.close >= previous.ema_fast and point.close < point.ema_fast and point.rsi < 50.0:
            direction = "DOWN"
        if direction is None:
            continue
        detected = point.available_at or point.end
        token = f"m1-reference|{series.instrument}|{point.start.isoformat()}|{direction}"
        rows.append({
            "signal_id": "m1ref_" + hashlib.sha256(token.encode()).hexdigest()[:32],
            "instrument": series.instrument, "direction": direction,
            "detected_at": detected, "timestamp": detected,
            "mode": "SYNTHETIC", "status": "REFERENCE_M1",
            "values": {"close": point.close, "ema_fast": point.ema_fast, "rsi": point.rsi, "variant": "m1_trigger_reference"},
            "quality": point.quality.status,
        })
    return rows


def evaluation_spec_from_config(config: Mapping[str, Any] | None = None) -> EvaluationSpec:
    """Construye el contrato de simulación desde TOML, con aliases explícitos."""
    raw = dict((config or {}).get("simulation", {}))
    if "horizons_seconds" not in raw and "horizons_minutes" in raw:
        raw["horizons_seconds"] = tuple(float(x) * 60.0 for x in raw.pop("horizons_minutes"))
    else:
        raw.pop("horizons_minutes", None)
    if "payout_net" not in raw and "net_payout" in raw:
        raw["payout_net"] = raw.pop("net_payout")
    else:
        raw.pop("net_payout", None)
    if "tie_net" not in raw and "tie_return" in raw:
        raw["tie_net"] = raw.pop("tie_return")
    else:
        raw.pop("tie_return", None)
    raw.pop("entry_rule", None); raw.pop("exit_rule", None)
    allowed = {"horizons_seconds", "entry_latency_seconds", "stake", "max_price_age_seconds", "payout_net", "loss_amount", "tie_net", "costs", "tie_tolerance", "requested_base_price", "require_closed", "horizon_from"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"simulation.clave desconocida: {sorted(unknown)}")
    return EvaluationSpec(**raw)

def _candle_row(c: Candle, point: Any | None = None) -> dict[str, Any]:
    return {"candle_id": c.candle_id, "instrument": c.instrument, "timeframe": c.timeframe.name,
            "start_ts": c.start, "end_ts": c.end, "open": c.open, "high": c.high, "low": c.low,
            "close": c.close, "volume": c.volume, "closed": c.closed, "source": c.source,
            "price_base": c.price_base.value, "quality": c.quality.status,
            "available_ts": c.available_at or c.end, "provenance": {"origin": c.origin, "metadata": dict(c.metadata),
                "indicators": ({"ema_fast": point.ema_fast, "ema_slow": point.ema_slow, "rsi": point.rsi, "atr": point.atr} if point is not None else {})}}


def persist_pipeline(result: PipelineResult, db_path: str | Path, *, seed: int, config: Mapping[str, Any] | None = None, log_path: str | Path | None = None, run_backtest: bool = True) -> PipelineResult:
    db = Path(db_path).expanduser()
    config_effective = dict(config or {})
    config_effective.setdefault("seed", seed)
    config_effective.setdefault("mode", "SYNTHETIC")
    dataset_hash = hashlib.sha256(json.dumps([row.to_dict() for row in result.dataset.bars], sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()
    config_effective.setdefault("dataset_hash", dataset_hash)
    with SQLiteStore(db) as store:
        sid = store.create_session(mode="SYNTHETIC", provider=result.dataset.provenance.provider, instrument=result.dataset.provenance.instrument,
                                  code_version=__version__, seed=seed, dataset_ref=result.dataset.provenance.source_uri,
                                  config=config_effective, metadata={"provenance": result.dataset.provenance.to_dict(), "quality": result.dataset.quality.to_dict()})
        store.save_config(sid, "effective", config_effective)
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
            row = evaluation.as_dict(); row.update({"decision_id": f"decision:{evaluation.stage}:{stable_ts}", "observed_ts": evaluation.timestamp, "available_ts": evaluation.available_at, "kind": evaluation.stage, "status": evaluation.decision.value})
            store.save_decision(sid, row, ordinal=i)
            if evaluation.decision.value in {"discarded", "blocked"}:
                reasons = evaluation.reasons or (evaluation.decision.value,)
                store.save_discard(sid, {"discard_id": f"{sid}:discard:{evaluation.stage}:{stable_ts}:{reasons[0]}", "decision_id": row["decision_id"], "observed_ts": evaluation.timestamp, "reason_code": reasons[0], "required": True, "condition_status": evaluation.decision.value, "payload": row}, ordinal=i)
        for i, signal in enumerate(result.strategy.signals):
            store.save_signal(sid, signal.as_dict(), ordinal=i)
        telemetry.state.update(events_processed=len(result.streams.get("M1", [])), candles_processed=sum(map(len, result.streams.values())), signals=len(result.strategy.signals), discards=sum(e.decision.value in {"discarded", "blocked"} for e in result.strategy.evaluations), last_received_ts=max((c.end for c in result.streams.get("M1", [])), default=None), last_available_ts=max((c.available_at or c.end for c in result.streams.get("M1", [])), default=None))
        simulations: list[dict[str, Any]] = []
        if run_backtest and result.strategy.signals:
            points = points_from_candles(result.streams.get("M1", []))
            spec = evaluation_spec_from_config(config_effective)
            # El flujo demo usa por defecto la base negociada, pero no sustituye
            # una base explícitamente configurada por otra.
            simulator = VirtualContractSimulator(spec=spec)
            runner = BacktestRunner(simulator=simulator, store=store, session_id=sid, logger=telemetry.logger)
            variants = [VariantSpec("m1_trigger_reference", "Referencia controlada: misma muestra, sólo disparador M1", {"context": False, "preparation": False}, mode="M1_REFERENCE"), VariantSpec("trend_pullback_v1", "Contexto M15 + preparación M5 + disparador M1", {"context": True, "preparation": True}, mode="MULTITIMEFRAME")]
            # La referencia M1 se genera con los mismos indicadores, pero sin
            # contexto/preparación; la variante MTF usa exclusivamente las
            # señales aceptadas por trend_pullback_v1.
            baseline = m1_reference_signals(result.indicators["M1"])
            bt = runner.run(baseline, points, variants=[variants[0]], data_quality="SYNTHETIC", resolution="M1")
            bt += runner.run([s.as_dict() for s in result.strategy.signals], points, variants=[variants[1]], data_quality="SYNTHETIC", resolution="M1")
            simulations = [x.to_dict(include_simulations=False) for x in bt]
            for x in bt:
                store.save_metric(sid, f"backtest:{x.variant}", {"variant": x.variant, "config_hash": x.config_hash}, x.net_result, x.to_dict(include_simulations=False))
        store.save_checkpoint(sid, "pipeline", cursor={"last_candle": len(result.streams.get("M1", []))}, events_processed=len(result.streams.get("M1", [])), state={"signals": len(result.strategy.signals)})
        telemetry.event("pipeline_complete", signals=len(result.strategy.signals), simulations=simulations, synthetic=True)
        telemetry.close(); store.finish_session(sid, status="COMPLETED")
        report = ReportBuilder(store, sid).summary()
        report_path = db.with_name(f"{db.stem}-report.md")
        ReportBuilder(store, sid).write(report_path, format="markdown", data=report)
    result.session_id = sid; result.simulations = simulations; result.report = report
    return result


def run_synthetic_pipeline(db_path: str | Path, *, seed: int = 42, periods_each: int = 600, config: Mapping[str, Any] | None = None, log_path: str | Path | None = None) -> PipelineResult:
    target_values = (config or {}).get("timeframes", {}).get("values", ("1m", "5m", "15m")) if isinstance((config or {}).get("timeframes", {}), Mapping) else ("1m", "5m", "15m")
    targets = [str(value) for value in target_values if str(value).upper().replace("M", "m") not in {"1m", "m1"}]
    instrument_cfg = (config or {}).get("instrument", {})
    instrument = str(instrument_cfg.get("symbol", "SYNTH/USD")) if isinstance(instrument_cfg, Mapping) else "SYNTH/USD"
    dataset, streams, gaps = generate_synthetic_streams(seed=seed, periods_each=periods_each, targets=targets or ("M5", "M15"), instrument=instrument)
    indicators, strategy = run_detector(streams, mode=OperationMode.SYNTHETIC, config=config)
    result = PipelineResult(dataset, streams, indicators, strategy)
    return persist_pipeline(result, db_path, seed=seed, config={**dict(config or {}), "synthetic": True, "gaps": gaps}, log_path=log_path)


__all__ = ["PipelineResult", "build_streams", "evaluation_spec_from_config", "generate_synthetic_streams", "m1_reference_signals", "persist_pipeline", "points_from_candles", "provider_bar_to_candle", "provider_event_to_core", "resample_candles", "run_detector", "run_synthetic_pipeline"]
