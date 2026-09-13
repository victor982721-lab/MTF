"""Causal CFD backtest composition for the research services.

This module is deliberately an orchestration layer.  It does not reimplement a
detector, a binary-options evaluator, or a second CFD state machine.  The
baseline capture is normalized by the existing cTrader adapter and passed
through ``CTraderPipeline`` (``RuntimeCoordinator`` + ``TrendPullbackStrategy``
+ ``CFDSignalConsumer`` + ``CFDSimulator``); the implemented challenger is
routed through ``StrategyProtocol`` and the same CFD consumer/simulator.  The
extra code here only builds a research ledger, mark-to-market observations,
and temporal split metadata.

The service is offline by construction: a capture path is read, never edited,
and the temporary SQLite session used by the shared pipeline is local and
discarded after the run.  A synthetic fixture is available only through the
explicit ``fixture=True`` flag.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, cast

from ..configuration import EffectiveConfig
from ..core.canonical import fingerprint, instant_text
from ..core.numeric import decimal_context
from ..data.capture import (
    CaptureContractError,
    CaptureEnvelope,
    CaptureIndex,
    envelope_from_raw,
    iter_jsonl,
    parse_instant,
)
from ..data.ctrader import CTraderInstrumentSpec
from ..ops.cfd_simulation import (
    CFDConfig,
    CFDQuote,
    CFDReplayResult,
    CFDSignal,
    CFDSimulator,
    CFDTrade,
    TradeState,
)
from ..ops.ctrader_capture import CaptureCoverage, CTraderCapture, normalize_ctrader_capture, synthetic_ctrader_capture
from ..ops.ctrader_paper_adapters import signal_to_cfd_signal, spot_event_to_cfd_quote
from ..ops.ctrader_pipeline import CTraderPipeline
from ..ops.persistence import SQLiteStore
from ..runtime import CFDSignalConsumer

ResearchOrder = Literal["as_observed", "market_time_corrected"]
ExecutionModel = Literal["full_fill", "ioc_partial", "rejected"]

_DEFAULT_CONFIG = "ctrader_pipeline_fixture.toml"
_PRODUCT = "FOREX_CFD_LOCAL_PAPER"
_DETECTOR = "TrendPullbackStrategy"
SUPPORTED_VARIANTS = frozenset({"trend_pullback_v1", "donchian20_m5_v1", "m1_trigger_reference"})
_BLOCKED_MARK_QUALITIES = frozenset(
    {
        "UNKNOWN",
        "INVALID",
        "STALE",
        "SNAPSHOT",
        "DISCONNECTED",
        "CROSSED",
        "OUT_OF_ORDER",
    }
)


class CFDBacktestError(ValueError):
    """A local CFD research input does not satisfy its explicit contract."""


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """One explicit local execution hypothesis; never a broker observation."""

    scenario: ExecutionModel
    fill_fraction: Decimal | None
    requested_quantity: Decimal
    effective_quantity: Decimal
    cancelled_quantity: Decimal
    rejected_quantity: Decimal

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "model": self.scenario,
            "synthetic_hypothesis": True,
            "quantity_basis": "cfd_config.units",
            "fill_fraction": str(self.fill_fraction) if self.fill_fraction is not None else None,
            "requested_quantity": str(self.requested_quantity),
            "effective_quantity": str(self.effective_quantity),
            "cancelled_quantity": str(self.cancelled_quantity),
            "rejected_quantity": str(self.rejected_quantity),
            "residual_policy": ("CANCELLED_ON_FIRST_FILL" if self.scenario == "ioc_partial" else "NOT_APPLICABLE"),
        }


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return instant_text(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    if hasattr(value, "as_dict") and callable(value.as_dict):
        return _jsonable(value.as_dict())
    return value


def _iso(value: datetime | None) -> str | None:
    return instant_text(value) if value is not None else None


def _aware_from_text(value: Any, *, name: str) -> datetime:
    try:
        parsed = parse_instant(value)
    except (CaptureContractError, TypeError, ValueError) as exc:
        raise CFDBacktestError(f"{name} debe ser timestamp ISO con zona") from exc
    if parsed is None:
        raise CFDBacktestError(f"{name} es obligatorio")
    return parsed


def _parse_decimal(value: Any, *, name: str, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise CFDBacktestError(f"{name} no admite booleanos")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CFDBacktestError(f"{name} no es decimal válido: {value!r}") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise CFDBacktestError(f"{name} no es finito o no es positivo")
    return parsed


def _normalise_execution_model(value: Any) -> ExecutionModel:
    text = str(value or "full_fill").strip().lower().replace("-", "_")
    if text not in {"full_fill", "ioc_partial", "rejected"}:
        raise CFDBacktestError(
            f"execution_model no soportado: {value!r}; disponibles=['full_fill', 'ioc_partial', 'rejected']"
        )
    return cast(ExecutionModel, text)


@decimal_context()
def build_execution_plan(
    requested_quantity: Decimal | int | str,
    *,
    execution_model: ExecutionModel | str = "full_fill",
    fill_fraction: Decimal | int | str | None = None,
) -> ExecutionPlan:
    """Resolve an explicit synthetic execution scenario before replay."""

    scenario = _normalise_execution_model(execution_model)
    requested = _parse_decimal(requested_quantity, name="requested_quantity", positive=True)
    if scenario == "ioc_partial":
        fraction = _parse_decimal(fill_fraction if fill_fraction is not None else "0.5", name="fill_fraction")
        if not 0 < fraction < 1:
            raise CFDBacktestError("fill_fraction para ioc_partial debe estar entre 0 y 1, sin incluir extremos")
        effective = requested * fraction
        if effective <= 0:
            raise CFDBacktestError("ioc_partial debe conservar una cantidad efectiva positiva")
        return ExecutionPlan(scenario, fraction, requested, effective, requested - effective, Decimal("0"))
    if fill_fraction is not None:
        raise CFDBacktestError("fill_fraction sólo aplica al escenario ioc_partial")
    if scenario == "rejected":
        return ExecutionPlan(scenario, None, requested, Decimal("0"), Decimal("0"), requested)
    return ExecutionPlan(scenario, None, requested, requested, Decimal("0"), Decimal("0"))


def _capture_values(value: Any) -> list[Any]:
    """Extract versioned envelopes from supported JSON capture containers."""

    if isinstance(value, Mapping):
        for key in ("envelopes", "payloads"):
            candidate = value.get(key)
            if isinstance(candidate, list):
                return candidate
        nested = value.get("capture")
        if isinstance(nested, Mapping):
            return _capture_values(nested)
        raise CFDBacktestError("captura JSON debe contener envelopes[] o payloads[]")
    if isinstance(value, list):
        return list(value)
    raise CFDBacktestError("captura JSON debe ser una lista u objeto de captura")


def _read_capture_values(path: Path) -> list[Any]:
    if not path.is_file():
        raise CFDBacktestError(f"captura no encontrada: {path}")
    try:
        if path.suffix.lower() in {".jsonl", ".ndjson"}:
            values = list(iter_jsonl(path))
        else:
            values = _capture_values(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError, CaptureContractError) as exc:
        raise CFDBacktestError(f"no se pudo leer captura local: {path}: {exc}") from exc
    if not values:
        raise CFDBacktestError(f"captura vacía: {path}")
    return values


def _as_envelopes(values: Iterable[Any]) -> tuple[CaptureEnvelope, ...]:
    envelopes: list[CaptureEnvelope] = []
    for index, value in enumerate(values):
        if isinstance(value, CaptureEnvelope):
            envelopes.append(value)
            continue
        if not isinstance(value, Mapping):
            raise CFDBacktestError(f"captura fila {index} no es un objeto")
        try:
            if "capture_schema" in value:
                envelopes.append(CaptureEnvelope.from_mapping(value))
            else:
                # A raw legacy cTrader payload is retained as historical
                # event-time evidence.  It is never relabelled as observed.
                envelopes.append(envelope_from_raw(value, index))
        except (CaptureContractError, TypeError, ValueError) as exc:
            raise CFDBacktestError(f"captura fila {index} inválida: {exc}") from exc
    return tuple(envelopes)


def _declared_coverage(envelopes: Sequence[CaptureEnvelope]) -> CaptureCoverage | None:
    for envelope in reversed(envelopes):
        if envelope.message_class.value != "end":
            continue
        payload = envelope.payload
        continuity = str(payload.get("continuity", "UNKNOWN"))
        if continuity not in {"UNKNOWN", "CONTINUOUS", "DISCONTINUOUS"}:
            raise CFDBacktestError(f"continuity de captura desconocida: {continuity!r}")
        return CaptureCoverage(
            requested_start=parse_instant(payload.get("requested_start")),
            requested_end=parse_instant(payload.get("requested_end")),
            dataset_end_declared=True,
            continuity=continuity,
        )
    return None


def _config_spec(config: EffectiveConfig) -> CTraderInstrumentSpec:
    ctrader = config.ctrader
    return CTraderInstrumentSpec(
        symbol=config.instrument,
        symbol_id=int(ctrader.get("symbol_id", 99)),
        digits=int(ctrader.get("digits", 5)),
        pip_position=int(ctrader.get("pip_position", 4)),
        price_scale=int(ctrader.get("price_scale", 100_000)),
    )


def _replay_config(config: EffectiveConfig) -> EffectiveConfig:
    base = str(config.price_base).strip().lower()
    if base == "trade":
        base = "traded"
    if base not in {"mid", "bid", "ask", "native", "traded"}:
        raise CFDBacktestError(f"base de análisis CFD no soportada: {config.price_base!r}")
    if base == "traded":
        # ``traded`` is a historical alias for native cTrader trendbars.  It
        # is not a synthetic bid/ask reconstruction.
        base = "native"
    data = dict(config.data)
    data.update({"mode": "REPLAY", "instrument": config.instrument, "price_base": base})
    return replace(config, mode="REPLAY", price_base=base, data=data)


def _cfd_config(config: EffectiveConfig, horizon: Decimal, *, known_cost_defaults: bool = False) -> CFDConfig:
    raw: dict[str, Any] = {str(key): value for key, value in config.cfd.items()}
    allowed = {field.name for field in fields(CFDConfig)}
    if "units" not in raw:
        for alias in ("default_quantity", "quantity"):
            if alias in raw:
                raw["units"] = raw[alias]
                break
    if "slippage_pips" not in raw and "slippage" in raw:
        raw["slippage_pips"] = raw["slippage"]
    # A missing real-world cost catalogue is not the same thing as a zero
    # cost.  The simulator's explicit unknown states are safer than silently
    # turning absent commission/financing information into profitable fills.
    if "commission_known" not in raw and not known_cost_defaults:
        raw["commission_known"] = False
    if "financing_required" not in raw and not known_cost_defaults:
        raw["financing_required"] = True
    if raw.get("commission_known") is True and not any(
        key in raw for key in ("commission_fixed", "commission_per_unit")
    ):
        raw["commission_known"] = False
    raw = {key: value for key, value in raw.items() if key in allowed}
    raw["instrument"] = config.instrument
    raw["horizons_seconds"] = (horizon,)
    try:
        return CFDConfig.from_mapping(raw)
    except (TypeError, ValueError) as exc:
        raise CFDBacktestError(f"configuración CFD inválida: {exc}") from exc


def load_cfd_capture(
    capture_path: str | Path,
    *,
    config: EffectiveConfig,
    order: ResearchOrder = "as_observed",
) -> CTraderCapture:
    """Read and normalize one explicit local capture without writing it."""

    path = Path(capture_path).expanduser()
    values = _read_capture_values(path)
    envelopes = _as_envelopes(values)
    try:
        return normalize_ctrader_capture(
            envelopes,
            spec=_config_spec(config),
            quote_basis=(config.price_base if config.price_base in {"mid", "bid", "ask"} else "mid"),
            mode="REPLAY",
            order=order,
            coverage=_declared_coverage(envelopes),
        )
    except (CaptureContractError, TypeError, ValueError) as exc:
        raise CFDBacktestError(f"captura no se pudo normalizar: {exc}") from exc


def synthetic_cfd_capture(*, config: EffectiveConfig, count: int = 190) -> CTraderCapture:
    """Return the existing deterministic fixture; never reads a corpus."""

    if isinstance(count, bool) or count <= 0:
        raise CFDBacktestError("fixture count debe ser entero positivo")
    return synthetic_ctrader_capture(
        start=datetime(2026, 1, 1, tzinfo=UTC),
        symbol_id=int(config.ctrader.get("symbol_id", 99)),
        count=int(count),
        mode="REPLAY",
    )


@dataclass(frozen=True, slots=True)
class ResearchWindow:
    """Chronological split metadata; warmup is retained, not deleted."""

    name: str
    kind: str
    index: int
    warmup_start: datetime
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime
    purge_holding_seconds: Decimal
    embargo_seconds: Decimal
    warmup_records: int
    train_records: int
    test_records: int
    embargo_records: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "index": self.index,
            "warmup_start": _iso(self.warmup_start),
            "train_start": _iso(self.train_start),
            "train_end": _iso(self.train_end),
            "test_start": _iso(self.test_start),
            "test_end": _iso(self.test_end),
            "purge_holding_seconds": str(self.purge_holding_seconds),
            "embargo_seconds": str(self.embargo_seconds),
            "warmup_records": self.warmup_records,
            "train_records": self.train_records,
            "test_records": self.test_records,
            "embargo_records": self.embargo_records,
            "warmup_preserved": True,
        }


def _capture_times(capture: CTraderCapture) -> tuple[datetime, ...]:
    if capture.quote_events:
        return tuple(event.effective_available_at for event in capture.quote_events)
    return tuple(
        timestamp
        for bar in capture.bars
        if bool(getattr(bar, "closed", True))
        for timestamp in (
            getattr(bar, "available_at", None) or getattr(bar, "interval_end", getattr(bar, "end", None)),
        )
        if timestamp is not None
    )


def build_research_windows(
    capture: CTraderCapture,
    *,
    holdout_fraction: float = 0.20,
    walkforward_folds: int = 4,
    walkforward_train_fraction: float = 0.40,
    walkforward_test_fraction: float = 0.10,
    purge_holding_seconds: Decimal = Decimal("0"),
    embargo_seconds: Decimal = Decimal("0"),
) -> tuple[ResearchWindow, ...]:
    """Build the required 20% holdout and 4×10% expanding windows.

    The split is a *research map*.  The shared detector still receives the
    complete causal capture once; the result ledger is assigned to windows
    only when its actual entry/close interval belongs to that window.  This
    preserves indicator warmup and avoids deleting history needed by the
    multitemporal strategy.
    """

    if not isinstance(walkforward_folds, int) or isinstance(walkforward_folds, bool) or walkforward_folds != 4:
        raise CFDBacktestError("walkforward_folds debe ser exactamente 4")
    fractions = (holdout_fraction, walkforward_train_fraction, walkforward_test_fraction)
    if any(isinstance(item, bool) or not 0 < float(item) < 1 for item in fractions):
        raise CFDBacktestError("fracciones temporales deben estar entre 0 y 1")
    if abs(float(holdout_fraction) - 0.20) > 1e-12:
        raise CFDBacktestError("holdout_fraction debe ser 0.20")
    if abs(float(walkforward_train_fraction) - 0.40) > 1e-12:
        raise CFDBacktestError("walkforward_train_fraction debe ser 0.40")
    if abs(float(walkforward_test_fraction) - 0.10) > 1e-12:
        raise CFDBacktestError("walkforward_test_fraction debe ser 0.10")
    if purge_holding_seconds < 0 or embargo_seconds < 0:
        raise CFDBacktestError("purge/embargo no pueden ser negativos")
    times = _capture_times(capture)
    if len(times) < 2:
        raise CFDBacktestError("se requieren al menos dos cotizaciones para dividir la captura")
    start, end = min(times), max(times)
    span = end - start

    def at(fraction: float) -> datetime:
        return start + timedelta(seconds=span.total_seconds() * fraction)

    holdout_start = at(1.0 - holdout_fraction)
    windows: list[ResearchWindow] = []
    for index in range(walkforward_folds):
        train_end_fraction = walkforward_train_fraction + index * walkforward_test_fraction
        test_start = at(train_end_fraction)
        test_end = at(train_end_fraction + walkforward_test_fraction)
        train_records = sum(item <= test_start for item in times)
        test_records = sum(test_start < item <= test_end for item in times)
        embargo_end = test_end + timedelta(seconds=float(embargo_seconds))
        embargo_records = sum(test_end < item <= embargo_end for item in times)
        windows.append(
            ResearchWindow(
                name=f"walkforward_{index + 1}",
                kind="walkforward",
                index=index,
                warmup_start=start,
                train_start=start,
                train_end=test_start,
                test_start=test_start,
                test_end=test_end,
                purge_holding_seconds=purge_holding_seconds,
                embargo_seconds=embargo_seconds,
                warmup_records=train_records,
                train_records=train_records,
                test_records=test_records,
                embargo_records=embargo_records,
            )
        )
    windows.append(
        ResearchWindow(
            name="holdout",
            kind="holdout",
            index=0,
            warmup_start=start,
            train_start=start,
            train_end=holdout_start,
            test_start=holdout_start,
            test_end=end,
            purge_holding_seconds=purge_holding_seconds,
            embargo_seconds=embargo_seconds,
            warmup_records=sum(item <= holdout_start for item in times),
            train_records=sum(item <= holdout_start for item in times),
            test_records=sum(item > holdout_start for item in times),
            embargo_records=sum(
                holdout_start < item <= holdout_start + timedelta(seconds=float(embargo_seconds)) for item in times
            ),
        )
    )
    return tuple(windows)


def _all_cfd_signals(store: SQLiteStore, result: Any, capture_hash: str) -> tuple[CFDSignal, ...]:
    signals: list[CFDSignal] = []
    for row in store.list_signals(result.session_id):
        payload = row.get("payload")
        if not isinstance(payload, Mapping):
            continue
        try:
            signals.append(signal_to_cfd_signal(payload, capture_hash=capture_hash))
        except (TypeError, ValueError, CaptureContractError) as exc:
            raise CFDBacktestError(f"señal persistida no es convertible a CFD: {exc}") from exc
    if signals:
        return tuple(signals)
    return tuple(result.cfd_signals)


def _reference_cfd_signals(rows: Sequence[Mapping[str, Any]], capture_hash: str) -> tuple[CFDSignal, ...]:
    """Adapt the canonical M1 reference projection through the CFD consumer."""

    from ..core.models import OperationMode
    from ..core.strategy import Signal
    from ..runtime.state import quality_from

    delivered: list[Signal] = []

    def sink(signal: Signal) -> None:
        delivered.append(signal)

    consumer = CFDSignalConsumer(signal_sink=sink)
    for row in rows:
        detected = row.get("detected_at", row.get("detected_ts", row.get("timestamp")))
        if detected is None:
            raise CFDBacktestError("M1 reference signal lacks detected timestamp")
        timestamp = _aware_from_text(detected, name="m1_reference.detected_at")
        direction = str(row.get("direction", "")).upper()
        if direction not in {"UP", "DOWN"}:
            raise CFDBacktestError(f"M1 reference direction inválida: {direction!r}")
        values = dict(row.get("values", {})) if isinstance(row.get("values"), Mapping) else {}
        values["adapter"] = "m1_reference"
        signal = Signal(
            signal_id=str(row.get("signal_id", "")),
            instrument=str(row.get("instrument", "")),
            direction=direction,
            detected_at=timestamp,
            context_start=timestamp,
            preparation_start=timestamp,
            trigger_start=timestamp,
            trigger_end=timestamp,
            episode_id=str(row.get("signal_id", "")),
            values=values,
            mode=OperationMode.REPLAY,
            quality=quality_from(row.get("quality"), synthetic=bool(row.get("mode") == "SYNTHETIC")),
        )
        consumer.on_signal(signal)
    return tuple(
        signal_to_cfd_signal(item, capture_hash=capture_hash, strategy="m1_trigger_reference") for item in delivered
    )


def _run_shared_pipeline(
    pipeline: CTraderPipeline,
    capture: CTraderCapture,
    *,
    session_id: str,
    order: ResearchOrder,
    reference: bool = False,
) -> tuple[Any, tuple[CFDSignal, ...], dict[str, Any]]:
    """Run one shared session and optionally obtain the canonical M1 control."""

    session = pipeline.open_session(
        dataset_id=capture.capture_hash,
        session_id=session_id,
        coverage=capture.coverage,
        provenance=capture.provenance,
        resume=False,
        order=order,
    )
    with CaptureIndex(capture.envelopes, mode=order) as index:
        session.ingest_many(index)
    pipeline_result = session.finish(capture_complete=capture.is_complete, finish_session=True)
    if reference:
        if capture.is_complete:
            reference_rows, _reference_id = session.coordinator.reference_signals()
            signals = _reference_cfd_signals(reference_rows, capture.capture_hash)
        else:
            signals = ()
        route = {
            "strategy_name": "m1_trigger_reference",
            "route": "core.reference.m1_reference_signals",
            "status": "ASSESSED" if signals else "NOT_ASSESSED",
            "reason": None if capture.is_complete else "capture_incomplete",
            "signals": len(signals),
        }
    else:
        signals = _all_cfd_signals(session.store, pipeline_result, capture.capture_hash)
        route = {
            "strategy_name": "trend_pullback_v1",
            "route": "RuntimeCoordinator",
            "status": "ASSESSED",
        }
    return pipeline_result, signals, route


def _donchian_signals(capture: CTraderCapture) -> tuple[tuple[CFDSignal, ...], dict[str, Any]]:
    """Route the implemented challenger through StrategyProtocol and CFD consumer."""

    from ..core.models import OperationMode
    from ..core.quality import DataQuality
    from ..core.strategy import Signal
    from ..core.strategy_extensions import Donchian20M5Strategy

    if not capture.is_complete:
        return (), {
            "strategy_name": "donchian20_m5_v1",
            "route": "StrategyProtocol",
            "status": "NOT_ASSESSED",
            "reason": "capture_incomplete",
        }
    bars = tuple(bar for bar in capture.bars if str(bar.resolution).upper() == "M5")
    if not bars:
        return (), {
            "strategy_name": "donchian20_m5_v1",
            "route": "StrategyProtocol",
            "status": "NOT_ASSESSED",
            "reason": "M5_bars_missing",
        }
    strategy = Donchian20M5Strategy(mode=OperationMode.REPLAY)
    output = strategy.evaluate_causal({"M5": bars})
    delivered: list[Signal] = []

    def sink(signal: Signal) -> None:
        delivered.append(signal)

    consumer = CFDSignalConsumer(signal_sink=sink)
    for item in output.signals:
        core_signal = Signal(
            signal_id=item.signal_id,
            instrument=item.instrument,
            direction=item.direction,
            detected_at=item.detected_at,
            context_start=item.trigger_start,
            preparation_start=item.trigger_start,
            trigger_start=item.trigger_start,
            trigger_end=item.trigger_end,
            episode_id=f"{item.strategy_name}:{item.trigger_start.isoformat()}",
            values={**dict(item.values), "strategy_name": item.strategy_name, "adapter": "StrategySignal"},
            mode=OperationMode.REPLAY,
            quality=DataQuality.good(),
        )
        consumer.on_signal(core_signal)
    signals = tuple(
        signal_to_cfd_signal(item, capture_hash=capture.capture_hash, strategy=output.strategy_name)
        for item in delivered
    )
    return signals, {
        "strategy_name": output.strategy_name,
        "route": "StrategyProtocol",
        "status": "ASSESSED",
        "explanations": len(output.explanations),
        "signals": len(output.signals),
        "warmup": strategy.warmup_requirements().to_dict(),
    }


def _quotes(capture: CTraderCapture) -> tuple[CFDQuote, ...]:
    quotes: list[CFDQuote] = []
    for event in capture.quote_events:
        try:
            quotes.append(spot_event_to_cfd_quote(event, capture_hash=capture.capture_hash))
        except (CaptureContractError, TypeError, ValueError) as exc:
            raise CFDBacktestError(f"quote CFD no convertible: {exc}") from exc
    return tuple(quotes)


def replay_interleaved(
    simulator: CFDSimulator,
    signals: Sequence[CFDSignal],
    quotes: Sequence[CFDQuote],
    *,
    horizon_seconds: Decimal,
    capture_complete: bool,
    mark_hook: Callable[[CFDQuote, tuple[CFDTrade, ...]], None] | None = None,
) -> CFDReplayResult:
    """Replay without preloading future signals into the simulator.

    Quotes retain their capture order.  Signals are submitted immediately
    before the first subsequent quote whose availability reaches the signal's
    availability; a signal at the same instant is submitted before that quote.
    This preserves causal capacity/state behavior and makes a later signal
    unable to occupy an earlier quote window.
    """

    ordered_signals = sorted(
        enumerate(signals),
        key=lambda item: (
            item[1].available_at or item[1].detected_at,
            item[0],
            item[1].signal_id,
        ),
    )
    signal_index = 0
    for quote in quotes:
        quote_time = quote.available_ts
        while signal_index < len(ordered_signals):
            signal = ordered_signals[signal_index][1]
            signal_time = signal.available_at or signal.detected_at
            if signal_time > quote_time:
                break
            simulator.submit(signal, horizon_seconds=horizon_seconds)
            signal_index += 1
        simulator.on_quote(quote, capture_complete=False)
        if mark_hook is not None:
            mark_hook(quote, simulator.trades)
    while signal_index < len(ordered_signals):
        simulator.submit(ordered_signals[signal_index][1], horizon_seconds=horizon_seconds)
        signal_index += 1
    last = quotes[-1].available_ts if quotes else None
    return simulator.finish(last, capture_complete=capture_complete)


def _markable_price(trade: CFDTrade, quote: CFDQuote, config: CFDConfig) -> Decimal | None:
    if quote.is_snapshot or quote.disconnected:
        return None
    quality = str(quote.quality).upper()
    if quality in _BLOCKED_MARK_QUALITIES:
        return None
    side = "bid" if trade.direction.value == "LONG" else "ask"
    if not quote.operable_for(side, at=quote.available_ts, max_age_seconds=config.max_quote_age_seconds):
        return None
    return quote.bid if side == "bid" else quote.ask


def _unrealized(trade: CFDTrade, quote: CFDQuote, config: CFDConfig) -> tuple[Decimal | None, Decimal | None]:
    if trade.state is not TradeState.FILLED or trade.entry_price is None:
        return Decimal("0"), Decimal("0")
    mark = _markable_price(trade, quote, config)
    if mark is None:
        return None, None
    sign = Decimal("1") if trade.direction.value == "LONG" else Decimal("-1")
    pnl = (mark - trade.entry_price) * trade.units * sign
    exposure = abs(mark * trade.units)
    return pnl, exposure


def _cost_summary(trades: Sequence[CFDTrade]) -> dict[str, Any]:
    known = Decimal("0")
    commission_known = Decimal("0")
    financing_known = Decimal("0")
    slippage_informative = Decimal("0")
    unknown_reasons: list[str] = []
    unknown_count = 0
    for trade in trades:
        if trade.costs_account is not None:
            known += trade.costs_account
        if trade.commission_quote is not None:
            commission_known += trade.commission_quote
        if trade.financing_quote is not None:
            financing_known += trade.financing_quote
        if trade.slippage_quote is not None:
            slippage_informative += trade.slippage_quote
        if trade.costs_account is not None:
            continue
        if trade.state is TradeState.REJECTED:
            # A local rejection has no fill and therefore no cost claim.  It
            # is not an unknown economic close.
            continue
        if trade.state in {TradeState.CLOSED, TradeState.UNKNOWN}:
            unknown_count += 1
            unknown_reasons.append(trade.reason or "COSTS_UNKNOWN")
        elif trade.state is TradeState.FILLED:
            unknown_count += 1
            unknown_reasons.append("OPEN_TRADE_COSTS_NOT_SETTLED")
    return {
        "known": str(known),
        "commission_known": str(commission_known),
        "financing_known": str(financing_known),
        "slippage_informative": str(slippage_informative),
        "unknown_count": unknown_count,
        "unknown_reasons": sorted(set(unknown_reasons)),
        "state": "KNOWN" if unknown_count == 0 else "PARTIAL_UNKNOWN",
    }


def _trade_execution_quantities(trade: CFDTrade, plan: ExecutionPlan) -> dict[str, Any]:
    """Expose requested/effective/cancelled quantities without mutating a trade."""

    has_entry = trade.entry_price is not None and trade.entry_available_at is not None
    filled = plan.effective_quantity if has_entry and trade.state is not TradeState.REJECTED else Decimal("0")
    cancelled = plan.cancelled_quantity if filled > 0 and plan.scenario == "ioc_partial" else Decimal("0")
    rejected = plan.requested_quantity if trade.state is TradeState.REJECTED else Decimal("0")
    return {
        "requested_quantity": plan.requested_quantity,
        "filled_quantity": filled,
        "cancelled_quantity": cancelled,
        "rejected_quantity": rejected,
        "executed": filled > 0,
        "cancelled_at": trade.entry_available_at if cancelled > 0 else None,
        "cancelled_on_first_fill": cancelled > 0,
        "cancel_reason": "IOC_RESIDUAL_CANCELLED_ON_FIRST_FILL" if cancelled > 0 else None,
    }


def _trade_ledger_row(
    trade: CFDTrade,
    *,
    variant: str,
    horizon: Decimal,
    plan: ExecutionPlan,
) -> dict[str, Any]:
    quantities = _trade_execution_quantities(trade, plan)
    costs_not_applicable = trade.state is TradeState.REJECTED
    return {
        **trade.to_dict(),
        "variant": variant,
        "research_horizon_seconds": str(horizon),
        "costs_known": costs_not_applicable or trade.costs_account is not None,
        "costs_unknown_reason": (
            None if costs_not_applicable or trade.costs_account is not None else (trade.reason or "COSTS_UNKNOWN")
        ),
        **{
            key: str(value) if isinstance(value, Decimal) else _iso(value) if isinstance(value, datetime) else value
            for key, value in quantities.items()
        },
    }


def _rejected_signals(signals: Sequence[CFDSignal]) -> tuple[CFDSignal, ...]:
    """Route the selected local rejection hypothesis through the shared simulator."""

    return tuple(replace(signal, quality="UNKNOWN") for signal in signals)


def _execution_scenarios(plan: ExecutionPlan) -> list[dict[str, Any]]:
    statuses = {
        "full_fill": ("MODELED", "CFDSimulator"),
        "ioc_partial": ("MODELED_SYNTHETIC_HYPOTHESIS", "CFDSimulator"),
        "rejected": ("MODELED_SYNTHETIC_LOCAL_REJECTION", "CFDSimulator"),
    }
    result: list[dict[str, Any]] = []
    for scenario, (status, engine) in statuses.items():
        selected = scenario == plan.scenario
        row: dict[str, Any] = {
            "name": scenario,
            "status": status if selected else "NOT_SELECTED",
            "engine": engine if selected else None,
            "synthetic_hypothesis": True,
        }
        if selected:
            row["parameters"] = plan.to_dict()
        else:
            row["reason"] = f"selected_scenario={plan.scenario}"
        result.append(row)
    return result


def _execution_model_payload(plan: ExecutionPlan, trades: Sequence[CFDTrade]) -> dict[str, Any]:
    rows = [_trade_execution_quantities(trade, plan) for trade in trades]
    totals = {
        key: sum((row[key] for row in rows), Decimal("0"))
        for key in ("requested_quantity", "filled_quantity", "cancelled_quantity", "rejected_quantity")
    }
    return {
        "scenario": plan.scenario,
        "model": plan.scenario,
        "status": {
            "full_fill": "MODELED",
            "ioc_partial": "MODELED_SYNTHETIC_HYPOTHESIS",
            "rejected": "MODELED_SYNTHETIC_LOCAL_REJECTION",
        }[plan.scenario],
        "synthetic_hypothesis": True,
        "parameters": plan.to_dict(),
        "requested_quantity": str(totals["requested_quantity"]),
        "filled_quantity": str(totals["filled_quantity"]),
        "cancelled_quantity": str(totals["cancelled_quantity"]),
        "rejected_quantity": str(totals["rejected_quantity"]),
        "orders": len(trades),
        "executed": totals["filled_quantity"] > 0,
        "partial_fills": "MODELED_SYNTHETIC_HYPOTHESIS" if plan.scenario == "ioc_partial" else "NOT_APPLICABLE",
        "rejections": "MODELED_SYNTHETIC_LOCAL" if plan.scenario == "rejected" else "LOCAL_GATES_ONLY",
        "external_broker_fills": "NOT_OBSERVED",
        "reconciliation": "NOT_APPLICABLE_OFFLINE",
        "scenarios": _execution_scenarios(plan),
    }


def _mark_row(
    quote: CFDQuote,
    trades: Sequence[CFDTrade],
    config: CFDConfig,
    *,
    realized: Decimal,
) -> dict[str, Any]:
    unrealized_values: list[Decimal] = []
    exposure_values: list[Decimal] = []
    unknown_marks = 0
    unknown_closed_costs = 0
    for trade in trades:
        if trade.state in {TradeState.CLOSED, TradeState.UNKNOWN} and trade.net_pnl is None:
            unknown_closed_costs += 1
        pnl, exposure = _unrealized(trade, quote, config)
        if pnl is None or exposure is None:
            if trade.state is TradeState.FILLED:
                unknown_marks += 1
            continue
        unrealized_values.append(pnl)
        exposure_values.append(exposure)
    unrealized = sum(unrealized_values, Decimal("0")) if unknown_marks == 0 else None
    exposure = sum(exposure_values, Decimal("0")) if unknown_marks == 0 else None
    equity = realized + unrealized if unrealized is not None and unknown_closed_costs == 0 else None
    return {
        "available_at": _iso(quote.available_ts),
        "quote_id": quote.identity,
        "equity": str(equity) if equity is not None else None,
        "realized": str(realized),
        "unrealized": str(unrealized) if unrealized is not None else None,
        "exposure": str(exposure) if exposure is not None else None,
        "unknown_marks": unknown_marks,
        "unknown_closed_costs": unknown_closed_costs,
        "state": "KNOWN" if equity is not None else "UNKNOWN",
    }


@decimal_context()
def run_cfd_backtest(
    capture: CTraderCapture,
    *,
    config: EffectiveConfig,
    variant: str,
    horizon_seconds: Decimal | int | str,
    order: ResearchOrder = "as_observed",
    max_candles: int = 5000,
    fixture: bool | None = None,
    execution_model: ExecutionModel | str = "full_fill",
    fill_fraction: Decimal | int | str | None = None,
) -> dict[str, Any]:
    """Run one isolated detector/CFD horizon and return auditable facts."""

    horizon = _parse_decimal(horizon_seconds, name="horizon_seconds", positive=True)
    known_cost_defaults = bool(fixture) if fixture is not None else bool(capture.provenance.get("synthetic", False))
    cfd_config = _cfd_config(config, horizon, known_cost_defaults=known_cost_defaults)
    plan = build_execution_plan(
        cfd_config.units,
        execution_model=execution_model,
        fill_fraction=fill_fraction,
    )
    simulation_config = (
        cfd_config if plan.scenario == "rejected" else replace(cfd_config, units=plan.effective_quantity)
    )
    if not variant.strip():
        raise CFDBacktestError("variant no puede estar vacío")
    if variant not in SUPPORTED_VARIANTS:
        raise CFDBacktestError(f"variant no implementada: {variant!r}; disponibles={sorted(SUPPORTED_VARIANTS)}")
    route: dict[str, Any]
    with TemporaryDirectory(prefix="mtf-research-") as temp:
        db = Path(temp) / "pipeline.sqlite3"
        with SQLiteStore(db) as store:
            if variant == "trend_pullback_v1":
                pipeline = CTraderPipeline(
                    store,
                    config,
                    spec=_config_spec(config),
                    cfd_config=simulation_config,
                    mode="REPLAY",
                    max_candles=max_candles,
                )
                session_id = f"research-{fingerprint({'capture': capture.capture_hash, 'variant': variant, 'horizon': str(horizon)})[:24]}"
                _pipeline_result, signals, route = _run_shared_pipeline(
                    pipeline,
                    capture,
                    session_id=session_id,
                    order=order,
                )
            elif variant == "m1_trigger_reference":
                pipeline = CTraderPipeline(
                    store,
                    config,
                    spec=_config_spec(config),
                    cfd_config=simulation_config,
                    mode="REPLAY",
                    max_candles=max_candles,
                )
                session_id = f"research-{fingerprint({'capture': capture.capture_hash, 'variant': variant, 'horizon': str(horizon)})[:24]}"
                _pipeline_result, signals, route = _run_shared_pipeline(
                    pipeline,
                    capture,
                    session_id=session_id,
                    order=order,
                    reference=True,
                )
            else:
                signals, route = _donchian_signals(capture)
        quotes = _quotes(capture)
    simulator = CFDSimulator(simulation_config)
    replay_signals = _rejected_signals(signals) if plan.scenario == "rejected" else signals
    marks: list[dict[str, Any]] = []

    def mark_hook(quote: CFDQuote, trades: tuple[CFDTrade, ...]) -> None:
        realized = sum(
            (trade.net_pnl for trade in trades if trade.state is TradeState.CLOSED and trade.net_pnl is not None),
            Decimal("0"),
        )
        marks.append(_mark_row(quote, trades, simulation_config, realized=realized))

    replay = replay_interleaved(
        simulator,
        replay_signals,
        quotes,
        horizon_seconds=horizon,
        capture_complete=capture.is_complete,
        mark_hook=mark_hook,
    )
    trades = tuple(replay.trades)
    closed_trades = [trade for trade in trades if trade.state is TradeState.CLOSED]
    closed_net = [trade.net_pnl for trade in closed_trades if trade.net_pnl is not None]
    unknown_economic = sum(trade.net_pnl is None for trade in closed_trades)
    unknown_lifecycle = sum(trade.state is TradeState.UNKNOWN for trade in trades)
    known_net_subtotal = sum(closed_net, Decimal("0")) if closed_net else None
    net = known_net_subtotal if closed_trades and unknown_economic == 0 else None
    known_marks = [Decimal(row["equity"]) for row in marks if row["equity"] is not None]
    exposures = [Decimal(row["exposure"]) for row in marks if row["exposure"] is not None]
    leakage_violations = _leakage_violations(trades)
    observed_days = sorted({timestamp.astimezone(UTC).date().isoformat() for timestamp in _capture_times(capture)})
    quote_evidence = "BID_ASK" if quotes else "ABSENT"
    ledger = [_trade_ledger_row(trade, variant=variant, horizon=horizon, plan=plan) for trade in trades]
    execution_payload = _execution_model_payload(plan, trades)
    rejected_only = bool(trades) and all(trade.state is TradeState.REJECTED for trade in trades)
    return {
        "product": _PRODUCT,
        "detector": (
            _DETECTOR
            if variant == "trend_pullback_v1"
            else "M1ReferenceProjection"
            if variant == "m1_trigger_reference"
            else "Donchian20M5Strategy"
        ),
        "detector_family": _DETECTOR,
        "strategy_route": route,
        "variant": variant,
        "horizon_seconds": str(horizon),
        "capture_hash": capture.capture_hash,
        "capture_complete": capture.is_complete,
        "observed_calendar_days": observed_days,
        "calendar_basis": "capture_quote_or_bar_availability",
        "quote_evidence": quote_evidence,
        "weekends_fabricated": False,
        "signals": len(signals),
        "trades": len(trades),
        "ledger": ledger,
        "equity_mark_to_market": marks,
        "equity_summary": {
            "net_pnl": str(net) if net is not None else None,
            "known_net_pnl_subtotal": str(known_net_subtotal) if known_net_subtotal is not None else None,
            "closed_known_count": len(closed_net),
            "closed_unknown_count": unknown_economic,
            "unknown_lifecycle_count": unknown_lifecycle,
            "economic_state": (
                "NO_TRADES"
                if not trades
                else "NO_EXECUTION"
                if rejected_only
                else "INDETERMINATE"
                if unknown_economic or unknown_lifecycle
                else "KNOWN"
                if closed_trades
                else "OPEN_OR_PENDING"
            ),
            "last_known_equity": str(known_marks[-1]) if known_marks else None,
            "peak_known_equity": str(max(known_marks)) if known_marks else None,
            "unknown_mark_count": sum(1 for row in marks if row["state"] == "UNKNOWN"),
        },
        "exposure": {
            "peak": str(max(exposures)) if exposures else None,
            "last_known": str(exposures[-1]) if exposures else None,
            "unknown_count": sum(1 for row in marks if row["exposure"] is None),
        },
        "costs": _cost_summary(trades),
        "quality": {
            "capture_issues": list(capture.issues),
            "continuity": capture.coverage.continuity,
            "availability_known": capture.coverage.availability_known,
        },
        "leakage_violations": leakage_violations,
        "config_hash": config.config_hash,
        "cfd_config_hash": simulation_config.config_hash,
        "requested_cfd_config_hash": cfd_config.config_hash,
        "execution_model": {
            "fill_policy": simulation_config.fill_policy,
            **execution_payload,
        },
    }


def _leakage_violations(trades: Sequence[CFDTrade]) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for trade in trades:
        if trade.entry_available_at is not None and trade.entry_available_at < trade.signal_available_at:
            violations.append(
                {
                    "trade_id": trade.trade_id,
                    "kind": "entry_before_signal_available",
                    "entry_available_at": _iso(trade.entry_available_at),
                    "signal_available_at": _iso(trade.signal_available_at),
                }
            )
        if (
            trade.close_available_at is not None
            and trade.entry_available_at is not None
            and trade.close_available_at < trade.entry_available_at
        ):
            violations.append({"trade_id": trade.trade_id, "kind": "close_before_entry"})
    return violations


def assign_trade_window(trade: Mapping[str, Any], window: ResearchWindow) -> tuple[str, str | None]:
    """Assign only trades whose *actual* holding is fully inside the test."""

    entry_value = trade.get("entry_available_at")
    close_value = trade.get("close_available_at") or trade.get("close_target_at")
    if not isinstance(entry_value, str) or not isinstance(close_value, str):
        return "NOT_ASSESSED", "holding_interval_unknown"
    try:
        entry = _aware_from_text(entry_value, name="entry_available_at")
        close = _aware_from_text(close_value, name="close_available_at")
    except CFDBacktestError:
        return "NOT_ASSESSED", "holding_interval_invalid"
    if entry < window.test_start or close > window.test_end:
        return "PURGED", "actual_holding_crosses_test_boundary"
    return "OOS", None


__all__ = [
    "CFDBacktestError",
    "ExecutionModel",
    "ExecutionPlan",
    "ResearchOrder",
    "ResearchWindow",
    "SUPPORTED_VARIANTS",
    "assign_trade_window",
    "build_research_windows",
    "build_execution_plan",
    "load_cfd_capture",
    "run_cfd_backtest",
    "synthetic_cfd_capture",
]
