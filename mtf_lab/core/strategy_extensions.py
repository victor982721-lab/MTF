"""Extensiones de investigación de estrategias, sin acoplamiento a brokers.

Este módulo contiene sólo contratos de investigación: calentamiento,
evaluación causal, explicación, señales virtuales y checkpoints de estrategia.
No crea órdenes ni conoce transportes, cuentas o credenciales.

``BaselineStrategyAdapter`` conserva el detector existente sin cambiar sus
parámetros. ``Donchian20M5Strategy`` ofrece un único challenger pequeño: su
canal siempre se calcula con las veinte barras M5 *anteriores* a la barra que
se está evaluando.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Protocol, TypeAlias, cast, runtime_checkable

from .canonical import fingerprint
from .indicators import IndicatorPoint, IndicatorSeries
from .models import Candle, OperationMode, Timeframe, normalize_utc, parse_timeframe
from .quality import DataQuality, QualityFlag
from .strategy import DecisionKind, Evaluation, Signal, StrategyConfig, TrendPullbackStrategy

StreamInput: TypeAlias = Mapping[str, Any] | Sequence[Any] | IndicatorSeries
QuoteInput: TypeAlias = "QuoteObservation | Mapping[str, Any]"


def _utc(value: datetime, name: str) -> datetime:
    return normalize_utc(value, name)


def _finite(value: Any, name: str, *, allow_none: bool = True) -> float | None:
    if value is None:
        if allow_none:
            return None
        raise TypeError(f"{name} debe ser numérico")
    if isinstance(value, bool):
        raise TypeError(f"{name} debe ser numérico")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{name} debe ser numérico") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} debe ser finito")
    return result


def _quality(value: Any) -> DataQuality:
    if value is None:
        return DataQuality.good()
    if isinstance(value, DataQuality):
        return value
    if isinstance(value, Mapping):
        flags = value.get("flags", ())
        if isinstance(flags, str):
            flags = (flags,)
        parsed: set[QualityFlag] = set()
        for flag in flags:
            try:
                parsed.add(flag if isinstance(flag, QualityFlag) else QualityFlag(str(flag).lower()))
            except ValueError:
                parsed.add(QualityFlag.INVALID)
        reasons = tuple(str(item) for item in value.get("reasons", ()))
        return DataQuality(frozenset(parsed), reasons, value.get("source"))
    if isinstance(value, str):
        try:
            return DataQuality.from_flag(QualityFlag(value.lower()))
        except ValueError:
            return DataQuality.from_flag(QualityFlag.INVALID, value)
    raise TypeError("quality debe ser DataQuality, string o mapping")


def _point_available(point: IndicatorPoint) -> datetime | None:
    if not point.closed:
        return None
    try:
        end = _utc(point.end, "point.end")
        available = _utc(point.available_at, "point.available_at") if point.available_at else end
    except (TypeError, ValueError):
        return None
    return max(end, available)


def session_causal(timestamp: datetime) -> str:
    """Clasifica una sesión usando sólo el instante ya observado, en UTC.

    Las ventanas son deliberadamente fijas y documentadas para que no haya
    dependencia de la zona horaria local ni de reglas DST futuras. Son
    etiquetas descriptivas, no un filtro de mercado implícito.
    """

    hour = _utc(timestamp, "timestamp").hour
    if hour < 7:
        return "ASIA"
    if hour < 12:
        return "LONDON"
    if hour < 17:
        return "NEW_YORK"
    if hour < 22:
        return "NY_LATE"
    return "OFF"


@dataclass(frozen=True, slots=True)
class VolatilityRegimeThreshold:
    """Umbral de volatilidad ajustado sólo con una muestra de entrenamiento."""

    threshold: float
    sample_count: int
    training_fingerprint: str
    trained_through: datetime | None = None

    def __post_init__(self) -> None:
        threshold = _finite(self.threshold, "threshold", allow_none=False)
        assert threshold is not None
        if threshold < 0.0:
            raise ValueError("threshold debe ser no negativo")
        if isinstance(self.sample_count, bool) or not isinstance(self.sample_count, int) or self.sample_count <= 0:
            raise ValueError("sample_count debe ser entero positivo")
        if not self.training_fingerprint:
            raise ValueError("training_fingerprint es obligatorio")
        if self.trained_through is not None:
            object.__setattr__(self, "trained_through", _utc(self.trained_through, "trained_through"))
        object.__setattr__(self, "threshold", threshold)

    @classmethod
    def fit(
        cls,
        points: Sequence[IndicatorPoint],
        *,
        quantile: float = 0.5,
    ) -> VolatilityRegimeThreshold:
        """Ajusta un umbral robusto a un prefijo de entrenamiento explícito.

        La función no conoce ningún conjunto de evaluación y nunca se llama
        desde ``compute_strategy_features``. El caller debe entregarle sólo
        el tramo de entrenamiento.
        """

        if isinstance(quantile, bool) or not math.isfinite(float(quantile)) or not 0.0 <= quantile <= 1.0:
            raise ValueError("quantile debe estar entre 0 y 1")
        values = tuple(ratio for point in points if (ratio := _atr_relative(point)) is not None)
        if not values:
            raise ValueError("no hay ATR relativo válido para entrenar el umbral")
        ordered = sorted(values)
        index = min(len(ordered) - 1, int(float(quantile) * (len(ordered) - 1)))
        through = _point_available(points[-1]) if points else None
        return cls(
            threshold=ordered[index],
            sample_count=len(ordered),
            training_fingerprint=fingerprint({"values": list(values), "quantile": float(quantile), "through": through}),
            trained_through=through,
        )

    @classmethod
    def from_training(cls, points: Sequence[IndicatorPoint], *, quantile: float = 0.5) -> VolatilityRegimeThreshold:
        """Alias explícito para callers que nombran la etapa de entrenamiento."""

        return cls.fit(points, quantile=quantile)

    def classify(self, atr_relative_price: float | None) -> str | None:
        if atr_relative_price is None:
            return None
        value = _finite(atr_relative_price, "atr_relative_price", allow_none=False)
        assert value is not None
        return "LOW" if value < self.threshold else "HIGH"

    def to_dict(self) -> dict[str, Any]:
        return {
            "threshold": self.threshold,
            "sample_count": self.sample_count,
            "training_fingerprint": self.training_fingerprint,
            "trained_through": self.trained_through.isoformat().replace("+00:00", "Z")
            if self.trained_through
            else None,
        }


@dataclass(frozen=True, slots=True)
class QuoteObservation:
    """Cotización opcional usada sólo para derivar spread/ATR causal."""

    timestamp: datetime
    bid: float | None = None
    ask: float | None = None
    available_at: datetime | None = None
    instrument: str = "unknown"
    spread: float | None = None

    def __post_init__(self) -> None:
        timestamp = _utc(self.timestamp, "quote.timestamp")
        available = _utc(self.available_at, "quote.available_at") if self.available_at else timestamp
        if available < timestamp:
            raise ValueError("quote.available_at no puede preceder a timestamp")
        bid = _finite(self.bid, "quote.bid")
        ask = _finite(self.ask, "quote.ask")
        spread = _finite(self.spread, "quote.spread")
        if spread is None and bid is not None and ask is not None:
            spread = ask - bid
        if spread is not None and spread < 0:
            raise ValueError("quote.spread no puede ser negativo")
        if bid is not None and ask is not None and ask < bid:
            raise ValueError("quote.ask debe ser mayor o igual que bid")
        if bid is None and ask is None and spread is None:
            raise ValueError("quote requiere bid/ask o spread")
        object.__setattr__(self, "timestamp", timestamp)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        object.__setattr__(self, "spread", spread)
        object.__setattr__(self, "instrument", str(self.instrument).strip() or "unknown")

    @property
    def effective_spread(self) -> float | None:
        if self.spread is not None:
            return self.spread
        if self.bid is None or self.ask is None:
            return None
        return self.ask - self.bid

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "available_at": self.available_at.isoformat().replace("+00:00", "Z") if self.available_at else None,
            "bid": self.bid,
            "ask": self.ask,
            "spread": self.effective_spread,
            "instrument": self.instrument,
        }


@dataclass(frozen=True, slots=True)
class StrategyFeatures:
    """Features causales calculadas al momento de disponibilidad del punto."""

    timestamp: datetime
    available_at: datetime | None
    atr_relative_price: float | None
    ema_slope_atr_normalized: float | None
    spread_atr: float | None
    session: str | None
    volatility_regime: str | None
    quality: DataQuality = field(default_factory=DataQuality.good)

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", _utc(self.timestamp, "features.timestamp"))
        if self.available_at is not None:
            object.__setattr__(self, "available_at", _utc(self.available_at, "features.available_at"))
        for name in ("atr_relative_price", "ema_slope_atr_normalized", "spread_atr"):
            object.__setattr__(self, name, _finite(getattr(self, name), name))
        if self.session is not None and not isinstance(self.session, str):
            raise TypeError("session debe ser texto o None")
        if self.volatility_regime not in {None, "LOW", "HIGH"}:
            raise ValueError("volatility_regime debe ser LOW, HIGH o None")

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "available_at": self.available_at.isoformat().replace("+00:00", "Z") if self.available_at else None,
            "atr_relative_price": self.atr_relative_price,
            "ema_slope_atr_normalized": self.ema_slope_atr_normalized,
            "spread_atr": self.spread_atr,
            "session": self.session,
            "volatility_regime": self.volatility_regime,
            "quality": {
                "status": self.quality.status,
                "flags": sorted(flag.value for flag in self.quality.flags),
                "reasons": list(self.quality.reasons),
            },
        }


def _atr_relative(point: IndicatorPoint) -> float | None:
    if not point.closed or not point.quality.valid or point.close is None or point.atr is None:
        return None
    close = _finite(point.close, "point.close", allow_none=False)
    atr = _finite(point.atr, "point.atr", allow_none=False)
    assert close is not None and atr is not None
    if close == 0.0:
        return None
    return atr / abs(close)


def _quote(value: QuoteInput, timestamp: datetime | None = None) -> QuoteObservation:
    if isinstance(value, QuoteObservation):
        return value
    if timestamp is not None and not isinstance(value, Mapping):
        return QuoteObservation(timestamp=timestamp, spread=value)
    if not isinstance(value, Mapping):
        raise TypeError("quote debe ser QuoteObservation o mapping")
    raw_timestamp = value.get("timestamp", value.get("event_time", value.get("time", timestamp)))
    if raw_timestamp is None:
        raise ValueError("quote requiere timestamp")
    if isinstance(raw_timestamp, datetime):
        quote_timestamp = raw_timestamp
    elif isinstance(raw_timestamp, str):
        text = raw_timestamp[:-1] + "+00:00" if raw_timestamp.endswith("Z") else raw_timestamp
        quote_timestamp = datetime.fromisoformat(text)
    else:
        raise TypeError("quote.timestamp debe ser datetime o timestamp ISO")
    return QuoteObservation(
        timestamp=quote_timestamp,
        bid=value.get("bid"),
        ask=value.get("ask"),
        available_at=value.get("available_at", value.get("available_ts")),
        instrument=str(value.get("instrument", value.get("symbol", "unknown"))),
        spread=value.get("spread"),
    )


def _quote_sequence(quotes: Sequence[QuoteInput] | Mapping[Any, Any]) -> tuple[QuoteObservation, ...]:
    if isinstance(quotes, Mapping):
        values: list[QuoteObservation] = []
        for key, value in quotes.items():
            values.append(_quote(value, key if isinstance(key, datetime) else None))
    else:
        values = [_quote(value) for value in quotes]
    return tuple(values)


def _latest_quote(
    quotes: Sequence[QuoteObservation],
    *,
    available_at: datetime | None,
) -> QuoteObservation | None:
    if available_at is None:
        return None
    candidates = tuple(
        quote
        for quote in quotes
        if quote.timestamp <= available_at and quote.available_at is not None and quote.available_at <= available_at
    )
    return (
        max(candidates, key=lambda quote: (quote.timestamp, quote.available_at or quote.timestamp))
        if candidates
        else None
    )


def _feature_point(
    point: IndicatorPoint,
    previous: IndicatorPoint | None,
    quotes: Sequence[QuoteObservation],
    threshold: VolatilityRegimeThreshold | None,
) -> StrategyFeatures:
    available = _point_available(point)
    atr_relative = _atr_relative(point)
    slope: float | None = None
    if (
        previous is not None
        and previous.end == point.start
        and _point_available(previous) is not None
        and available is not None
        and (_point_available(previous) or available) <= available
        and point.ema_fast is not None
        and previous.ema_fast is not None
        and point.atr is not None
        and point.atr > 0.0
        and point.quality.valid
        and previous.quality.valid
        and point.closed
        and previous.closed
    ):
        slope = (point.ema_fast - previous.ema_fast) / point.atr
    quote = _latest_quote(quotes, available_at=available)
    spread_atr = None
    if quote is not None and quote.effective_spread is not None and point.atr is not None and point.atr > 0.0:
        spread_atr = quote.effective_spread / point.atr
    session = session_causal(point.end) if available is not None else None
    return StrategyFeatures(
        timestamp=point.end,
        available_at=available,
        atr_relative_price=atr_relative,
        ema_slope_atr_normalized=slope,
        spread_atr=spread_atr,
        session=session,
        volatility_regime=threshold.classify(atr_relative) if threshold is not None else None,
        quality=point.quality,
    )


def compute_strategy_features(
    points: Sequence[IndicatorPoint],
    *,
    quotes: Sequence[QuoteInput] | Mapping[Any, Any] = (),
    volatility_threshold: VolatilityRegimeThreshold | None = None,
) -> tuple[StrategyFeatures, ...]:
    """Calcula features sin usar puntos ni cotizaciones futuras.

    ``volatility_threshold`` debe provenir de ``fit`` sobre el tramo de
    entrenamiento. La función no ajusta umbrales ni rellena spread desde OHLC;
    si no existe cotización admisible, ``spread_atr`` queda en ``None``.
    """

    if not isinstance(points, Sequence):
        raise TypeError("points debe ser una secuencia de IndicatorPoint")
    normalized_points = tuple(points)
    if not all(isinstance(point, IndicatorPoint) for point in normalized_points):
        raise TypeError("points debe contener sólo IndicatorPoint")
    quote_rows = _quote_sequence(quotes)
    return tuple(
        _feature_point(
            point,
            normalized_points[index - 1] if index else None,
            quote_rows,
            volatility_threshold,
        )
        for index, point in enumerate(normalized_points)
    )


@dataclass(frozen=True, slots=True)
class WarmupRequirements:
    """Requisitos declarativos de calentamiento por temporalidad."""

    strategy_name: str
    minimum_bars: Mapping[str, int]
    rationale: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        values: dict[str, int] = {}
        for timeframe, count in self.minimum_bars.items():
            if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
                raise ValueError("minimum_bars debe contener enteros positivos")
            values[parse_timeframe(timeframe).name] = count
        object.__setattr__(self, "minimum_bars", MappingProxyType(values))
        object.__setattr__(self, "rationale", tuple(str(item) for item in self.rationale))

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "minimum_bars": dict(self.minimum_bars),
            "rationale": list(self.rationale),
        }


@dataclass(frozen=True, slots=True)
class StrategyExplanation:
    """Explicación de una evaluación; no contiene una orden de broker."""

    strategy_name: str
    timestamp: datetime
    available_at: datetime | None
    decision: str
    direction: str | None
    reasons: tuple[str, ...] = ()
    values: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", _utc(self.timestamp, "explanation.timestamp"))
        if self.available_at is not None:
            object.__setattr__(self, "available_at", _utc(self.available_at, "explanation.available_at"))
        object.__setattr__(self, "reasons", tuple(str(reason) for reason in self.reasons))
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    @property
    def causal(self) -> bool:
        return self.available_at is not None and self.available_at >= self.timestamp

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "available_at": self.available_at.isoformat().replace("+00:00", "Z") if self.available_at else None,
            "decision": self.decision,
            "direction": self.direction,
            "reasons": list(self.reasons),
            "values": dict(self.values),
            "causal": self.causal,
        }


@dataclass(frozen=True, slots=True)
class StrategySignal:
    """Señal virtual explicable, deliberadamente sin campos de ejecución."""

    signal_id: str
    strategy_name: str
    instrument: str
    direction: str
    detected_at: datetime
    trigger_start: datetime
    trigger_end: datetime
    values: Mapping[str, Any] = field(default_factory=dict)
    explanation: StrategyExplanation | None = None

    def __post_init__(self) -> None:
        if not self.signal_id or not self.strategy_name or not self.instrument:
            raise ValueError("signal_id, strategy_name e instrument son obligatorios")
        if self.direction not in {"UP", "DOWN"}:
            raise ValueError("direction debe ser UP o DOWN")
        for name in ("detected_at", "trigger_start", "trigger_end"):
            object.__setattr__(self, name, _utc(getattr(self, name), f"signal.{name}"))
        if self.trigger_end <= self.trigger_start:
            raise ValueError("trigger_end debe ser posterior a trigger_start")
        if self.detected_at < self.trigger_end:
            raise ValueError("detected_at no puede preceder al cierre del disparador")
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "strategy_name": self.strategy_name,
            "instrument": self.instrument,
            "direction": self.direction,
            "detected_at": self.detected_at.isoformat().replace("+00:00", "Z"),
            "trigger_start": self.trigger_start.isoformat().replace("+00:00", "Z"),
            "trigger_end": self.trigger_end.isoformat().replace("+00:00", "Z"),
            "values": dict(self.values),
            "explanation": self.explanation.to_dict() if self.explanation else None,
        }


@dataclass(frozen=True, slots=True)
class StrategyOutput:
    """Salida completa de una evaluación causal de investigación."""

    strategy_name: str
    explanations: tuple[StrategyExplanation, ...]
    signals: tuple[StrategySignal, ...]

    @property
    def evaluations(self) -> tuple[StrategyExplanation, ...]:
        return self.explanations

    @property
    def signal_count(self) -> int:
        return len(self.signals)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "explanations": [item.to_dict() for item in self.explanations],
            "signals": [item.to_dict() for item in self.signals],
        }


@dataclass(frozen=True, slots=True)
class StrategyCheckpoint:
    """Checkpoint tipado de la estrategia y su identidad de configuración."""

    schema_version: int
    strategy_name: str
    config_identity: str
    state: Mapping[str, Any] = field(default_factory=dict)

    VERSION = 1

    def __post_init__(self) -> None:
        if self.schema_version != self.VERSION:
            raise ValueError(f"schema_version de estrategia no soportada: {self.schema_version!r}")
        if not self.strategy_name or not self.config_identity:
            raise ValueError("strategy_name y config_identity son obligatorios")
        object.__setattr__(self, "state", MappingProxyType(dict(self.state)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "strategy_name": self.strategy_name,
            "config_identity": self.config_identity,
            "state": dict(self.state),
        }

    @classmethod
    def from_mapping(cls, value: StrategyCheckpoint | Mapping[str, Any]) -> StrategyCheckpoint:
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("checkpoint de estrategia debe ser mapping")
        required = {"schema_version", "strategy_name", "config_identity", "state"}
        missing = required - set(value)
        if missing:
            raise ValueError(f"Faltan campos en checkpoint de estrategia: {sorted(missing)}")
        state = value["state"]
        if not isinstance(state, Mapping):
            raise TypeError("state de checkpoint de estrategia debe ser mapping")
        return cls(int(value["schema_version"]), str(value["strategy_name"]), str(value["config_identity"]), state)


@runtime_checkable
class StrategyProtocol(Protocol):
    """Contrato común para baseline y challengers de investigación."""

    name: str

    def warmup_requirements(self) -> WarmupRequirements: ...

    def evaluate_causal(
        self,
        streams: Mapping[str, Any],
        *,
        quotes: Sequence[QuoteInput] | Mapping[Any, Any] = (),
    ) -> StrategyOutput: ...

    def checkpoint(self) -> StrategyCheckpoint: ...

    def restore(self, checkpoint: StrategyCheckpoint | Mapping[str, Any]) -> None: ...


def _strategy_identity(config: Any) -> str:
    return fingerprint(config)


def _baseline_warmup(config: StrategyConfig) -> WarmupRequirements:
    indicator_bars = max(
        config.indicators.ema_slow,
        config.indicators.rsi_period + 1,
        config.indicators.atr_period,
    )
    return WarmupRequirements(
        config.name,
        {
            parse_timeframe(config.context_timeframe).name: indicator_bars + config.context_lookback,
            parse_timeframe(config.preparation_timeframe).name: indicator_bars + config.preparation_lookback,
            parse_timeframe(config.trigger_timeframe).name: indicator_bars + 2,
        },
        ("indicadores EMA/RSI/ATR", "lookback causal multitemporal", "cruce requiere barra previa"),
    )


def _explanation_from_evaluation(strategy_name: str, evaluation: Evaluation) -> StrategyExplanation:
    return StrategyExplanation(
        strategy_name=strategy_name,
        timestamp=evaluation.timestamp,
        available_at=evaluation.available_at,
        decision=evaluation.decision.value,
        direction=evaluation.direction,
        reasons=evaluation.reasons,
        values=evaluation.values,
    )


def _baseline_signal(
    strategy_name: str,
    signal: Signal,
    explanation: StrategyExplanation | None,
) -> StrategySignal:
    return StrategySignal(
        signal_id=signal.signal_id,
        strategy_name=strategy_name,
        instrument=signal.instrument,
        direction=signal.direction,
        detected_at=signal.detected_at,
        trigger_start=signal.trigger_start,
        trigger_end=signal.trigger_end,
        values=signal.values,
        explanation=explanation,
    )


class BaselineStrategyAdapter:
    """Adapter del detector existente al protocolo de investigación nuevo."""

    def __init__(self, config: StrategyConfig | Mapping[str, Any] | None = None) -> None:
        self._strategy = TrendPullbackStrategy(config)
        self.name = self._strategy.config.name

    @property
    def config(self) -> StrategyConfig:
        return self._strategy.config

    @property
    def config_identity(self) -> str:
        return _strategy_identity(self.config)

    def warmup_requirements(self) -> WarmupRequirements:
        return _baseline_warmup(self.config)

    def warmup(self) -> WarmupRequirements:
        return self.warmup_requirements()

    def evaluate_causal(
        self,
        streams: Mapping[str, Any],
        *,
        quotes: Sequence[QuoteInput] | Mapping[Any, Any] = (),
    ) -> StrategyOutput:
        del quotes  # La baseline no altera su semántica por features opt-in.
        result = self._strategy.evaluate(streams)
        explanations = tuple(_explanation_from_evaluation(self.name, item) for item in result.evaluations)
        by_trigger = {item.timestamp: item for item in explanations if item.decision == DecisionKind.SIGNAL.value}
        signals = tuple(
            _baseline_signal(self.name, signal, by_trigger.get(signal.trigger_end)) for signal in result.signals
        )
        return StrategyOutput(self.name, explanations, signals)

    def evaluate(
        self,
        streams: Mapping[str, Any],
        *,
        quotes: Sequence[QuoteInput] | Mapping[Any, Any] = (),
    ) -> StrategyOutput:
        return self.evaluate_causal(streams, quotes=quotes)

    def checkpoint(self) -> StrategyCheckpoint:
        return StrategyCheckpoint(StrategyCheckpoint.VERSION, self.name, self.config_identity)

    def restore(self, checkpoint: StrategyCheckpoint | Mapping[str, Any]) -> None:
        value = StrategyCheckpoint.from_mapping(checkpoint)
        if value.strategy_name != self.name:
            raise ValueError("checkpoint de estrategia incompatible")
        if value.config_identity != self.config_identity:
            raise ValueError("config_identity de estrategia no coincide")
        if value.state:
            raise ValueError("la baseline no admite estado adicional")


@dataclass(frozen=True, slots=True)
class Donchian20Config:
    """Configuración fija del challenger Donchian M5."""

    lookback: int = 20
    timeframe: Timeframe | str = "M5"
    mode: OperationMode = OperationMode.REPLAY
    allowed_sessions: tuple[str, ...] = ()
    volatility_threshold: VolatilityRegimeThreshold | None = None

    def __post_init__(self) -> None:
        if self.lookback != 20:
            raise ValueError("Donchian20Config exige lookback=20")
        object.__setattr__(self, "timeframe", parse_timeframe(self.timeframe))
        if parse_timeframe(self.timeframe).name != "M5":
            raise ValueError("Donchian20Config exige timeframe=M5")
        if not isinstance(self.mode, OperationMode):
            object.__setattr__(self, "mode", OperationMode(str(self.mode).upper()))
        sessions = tuple(str(item).upper() for item in self.allowed_sessions)
        unknown = set(sessions) - {"ASIA", "LONDON", "NEW_YORK", "NY_LATE", "OFF"}
        if unknown:
            raise ValueError(f"sesiones desconocidas: {sorted(unknown)}")
        object.__setattr__(self, "allowed_sessions", sessions)


@dataclass(frozen=True, slots=True)
class _BarView:
    timeframe: Timeframe
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    closed: bool
    available_at: datetime | None
    quality: DataQuality
    instrument: str
    candle_id: str


def _bar_view(value: Any) -> _BarView:
    if isinstance(value, Candle):
        return _BarView(
            value.normalized_timeframe,
            value.start,
            value.end,
            value.open,
            value.high,
            value.low,
            value.close,
            value.closed,
            value.available_at,
            value.quality,
            value.instrument,
            str(value.candle_id),
        )
    if isinstance(value, IndicatorPoint):
        raise TypeError("Donchian20M5Strategy requiere OHLC; IndicatorPoint no contiene high/low")
    if hasattr(value, "interval_start") and hasattr(value, "interval_end"):
        start = value.interval_start
        end = value.interval_end
        timeframe = parse_timeframe(getattr(value, "resolution", getattr(value, "resolution_seconds", "M5")))
        instrument = str(getattr(value, "instrument", "unknown"))
        candle_id = str(getattr(value, "data_id", getattr(value, "candle_id", "")))
        closed = bool(getattr(value, "closed", True))
        available = getattr(value, "available_at", None)
        quality = _quality(getattr(value, "quality", None))
        return _make_bar_view(
            start,
            end,
            timeframe,
            value.open,
            value.high,
            value.low,
            value.close,
            closed,
            available,
            quality,
            instrument,
            candle_id,
        )
    if isinstance(value, Mapping):
        start = value.get("start", value.get("interval_start", value.get("start_ts")))
        end = value.get("end", value.get("interval_end", value.get("end_ts")))
        if start is None or end is None:
            raise ValueError("barra requiere start/end")
        return _make_bar_view(
            start,
            end,
            parse_timeframe(value.get("timeframe", value.get("resolution", "M5"))),
            value.get("open"),
            value.get("high"),
            value.get("low"),
            value.get("close"),
            bool(value.get("closed", value.get("is_closed", True))),
            value.get("available_at", value.get("available_ts")),
            _quality(value.get("quality")),
            str(value.get("instrument", value.get("symbol", "unknown"))),
            str(value.get("candle_id", value.get("data_id", ""))),
        )
    raise TypeError("barra no es un Candle, proveedor normalizado o mapping OHLC")


def _make_bar_view(
    start: Any,
    end: Any,
    timeframe: Timeframe,
    open_value: Any,
    high_value: Any,
    low_value: Any,
    close_value: Any,
    closed: bool,
    available: Any,
    quality: DataQuality,
    instrument: str,
    candle_id: str,
) -> _BarView:
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        raise TypeError("start/end de barra deben ser datetime")
    normalized_start = _utc(start, "bar.start")
    normalized_end = _utc(end, "bar.end")
    open_number = _finite(open_value, "bar.open", allow_none=False)
    high_number = _finite(high_value, "bar.high", allow_none=False)
    low_number = _finite(low_value, "bar.low", allow_none=False)
    close_number = _finite(close_value, "bar.close", allow_none=False)
    assert open_number is not None and high_number is not None and low_number is not None and close_number is not None
    if normalized_end <= normalized_start or high_number < max(open_number, low_number, close_number):
        raise ValueError("OHLC de barra es inconsistente")
    if low_number > min(open_number, high_number, close_number):
        raise ValueError("OHLC de barra es inconsistente")
    normalized_available = _utc(available, "bar.available_at") if isinstance(available, datetime) else None
    if closed and normalized_available is None:
        normalized_available = normalized_end
    if closed and normalized_available is not None and normalized_available < normalized_end:
        raise ValueError("bar.available_at no puede preceder al cierre")
    return _BarView(
        timeframe,
        normalized_start,
        normalized_end,
        open_number,
        high_number,
        low_number,
        close_number,
        closed,
        normalized_available,
        quality,
        str(instrument).strip() or "unknown",
        candle_id or f"bar:{normalized_start.isoformat()}",
    )


def _stream_rows(streams: Mapping[str, Any], timeframe: Timeframe) -> Sequence[Any]:
    value: Any = None
    lookup = cast(Mapping[Any, Any], streams)
    for key in (timeframe.name, timeframe.name.lower(), timeframe):
        if key in lookup:
            value = lookup[key]
            break
    if value is None:
        raise ValueError(f"stream {timeframe.name} ausente")
    if isinstance(value, Mapping):
        rows = value.get("bars", value.get("candles", value.get("points")))
        if rows is None:
            raise ValueError(f"stream {timeframe.name} requiere bars/candles")
        value = rows
    if isinstance(value, IndicatorSeries):
        raise TypeError("Donchian20M5Strategy requiere barras OHLC, no IndicatorSeries")
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"stream {timeframe.name} debe ser una secuencia")
    return value


def _donchian_explanation(
    strategy_name: str,
    bar: _BarView,
    decision: str,
    direction: str | None,
    reasons: tuple[str, ...],
    values: Mapping[str, Any],
) -> StrategyExplanation:
    return StrategyExplanation(strategy_name, bar.end, bar.available_at, decision, direction, reasons, values)


def _donchian_signal(
    strategy_name: str,
    config_identity: str,
    bar: _BarView,
    direction: str,
    values: Mapping[str, Any],
    explanation: StrategyExplanation,
) -> StrategySignal:
    payload = f"{strategy_name}|{config_identity}|{bar.instrument}|{direction}|{bar.start.isoformat()}"
    signal_id = "sig_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
    available = bar.available_at or bar.end
    return StrategySignal(
        signal_id=signal_id,
        strategy_name=strategy_name,
        instrument=bar.instrument,
        direction=direction,
        detected_at=available,
        trigger_start=bar.start,
        trigger_end=bar.end,
        values=values,
        explanation=explanation,
    )


class Donchian20M5Strategy:
    """Challenger de ruptura causal del canal Donchian de veinte barras M5."""

    VERSION = "donchian20_m5_v1"

    def __init__(
        self,
        config: Donchian20Config | None = None,
        *,
        mode: OperationMode | str | None = None,
        allowed_sessions: Sequence[str] = (),
        volatility_threshold: VolatilityRegimeThreshold | None = None,
    ) -> None:
        if config is not None and (mode is not None or allowed_sessions or volatility_threshold is not None):
            raise ValueError("config no puede combinarse con overrides del challenger")
        if config is None:
            selected_mode = OperationMode.REPLAY if mode is None else mode
            config = Donchian20Config(
                mode=selected_mode
                if isinstance(selected_mode, OperationMode)
                else OperationMode(str(selected_mode).upper()),
                allowed_sessions=tuple(allowed_sessions),
                volatility_threshold=volatility_threshold,
            )
        self.config = config
        self.name = self.VERSION

    @property
    def config_identity(self) -> str:
        return _strategy_identity(self.config)

    def warmup_requirements(self) -> WarmupRequirements:
        return WarmupRequirements(
            self.name,
            {"M5": self.config.lookback},
            ("canal Donchian usa sólo las 20 barras M5 previas",),
        )

    def warmup(self) -> WarmupRequirements:
        return self.warmup_requirements()

    def _evaluate_bar(
        self,
        bars: Sequence[_BarView],
        index: int,
    ) -> tuple[StrategyExplanation, StrategySignal | None]:
        bar = bars[index]
        if not bar.closed or not bar.quality.valid or bar.available_at is None:
            explanation = _donchian_explanation(
                self.name, bar, "blocked", None, ("bar_not_available",), {"close": bar.close}
            )
            return explanation, None
        if index < self.config.lookback:
            explanation = _donchian_explanation(
                self.name,
                bar,
                "wait",
                None,
                ("warmup_pending",),
                {"close": bar.close, "required_bars": self.config.lookback, "observed_bars": index},
            )
            return explanation, None
        previous = bars[index - self.config.lookback : index]
        if not all(item.quality.valid and item.closed and item.available_at is not None for item in previous):
            explanation = _donchian_explanation(
                self.name, bar, "blocked", None, ("lookback_not_available",), {"close": bar.close}
            )
            return explanation, None
        if any(item.available_at > bar.available_at for item in previous if item.available_at is not None):
            explanation = _donchian_explanation(
                self.name, bar, "blocked", None, ("lookback_future_data",), {"close": bar.close}
            )
            return explanation, None
        upper = max(item.high for item in previous)
        lower = min(item.low for item in previous)
        session = session_causal(bar.end)
        values: dict[str, Any] = {
            "close": bar.close,
            "channel_upper": upper,
            "channel_lower": lower,
            "lookback": self.config.lookback,
            "channel_source": "previous_closed_bars_only",
            "session": session,
        }
        if self.config.volatility_threshold is not None:
            values["volatility_threshold"] = self.config.volatility_threshold.to_dict()
        if self.config.allowed_sessions and session not in self.config.allowed_sessions:
            explanation = _donchian_explanation(
                self.name,
                bar,
                "discarded",
                None,
                ("session_filter_failed",),
                values,
            )
            return explanation, None
        direction = "UP" if bar.close > upper else "DOWN" if bar.close < lower else None
        if direction is None:
            explanation = _donchian_explanation(self.name, bar, "discarded", None, ("channel_breakout_absent",), values)
            return explanation, None
        explanation = _donchian_explanation(self.name, bar, "signal", direction, (), values)
        return explanation, _donchian_signal(self.name, self.config_identity, bar, direction, values, explanation)

    def evaluate_causal(
        self,
        streams: Mapping[str, Any],
        *,
        quotes: Sequence[QuoteInput] | Mapping[Any, Any] = (),
    ) -> StrategyOutput:
        del quotes  # El challenger no fabrica spread desde OHLC.
        if not isinstance(streams, Mapping):
            raise TypeError("streams debe ser mapping por temporalidad")
        raw_rows = _stream_rows(streams, parse_timeframe(self.config.timeframe))
        bars = tuple(_bar_view(row) for row in raw_rows)
        if not bars:
            return StrategyOutput(self.name, (), ())
        expected_timeframe = parse_timeframe(self.config.timeframe)
        if any(bar.timeframe != expected_timeframe for bar in bars):
            raise ValueError("Donchian20M5Strategy sólo acepta barras M5")
        previous_end: datetime | None = None
        for bar in bars:
            if previous_end is not None and bar.start < previous_end:
                raise ValueError("las barras Donchian deben estar en orden causal")
            previous_end = bar.end
        instruments = {bar.instrument for bar in bars if bar.instrument != "unknown"}
        if len(instruments) > 1:
            raise ValueError(f"las barras deben compartir instrumento: {sorted(instruments)}")
        explanations: list[StrategyExplanation] = []
        signals: list[StrategySignal] = []
        for index in range(len(bars)):
            explanation, signal = self._evaluate_bar(bars, index)
            explanations.append(explanation)
            if signal is not None:
                signals.append(signal)
        return StrategyOutput(self.name, tuple(explanations), tuple(signals))

    def evaluate(
        self,
        streams: Mapping[str, Any],
        *,
        quotes: Sequence[QuoteInput] | Mapping[Any, Any] = (),
    ) -> StrategyOutput:
        return self.evaluate_causal(streams, quotes=quotes)

    def checkpoint(self) -> StrategyCheckpoint:
        return StrategyCheckpoint(StrategyCheckpoint.VERSION, self.name, self.config_identity)

    def restore(self, checkpoint: StrategyCheckpoint | Mapping[str, Any]) -> None:
        value = StrategyCheckpoint.from_mapping(checkpoint)
        if value.strategy_name != self.name:
            raise ValueError("checkpoint de estrategia incompatible")
        if value.config_identity != self.config_identity:
            raise ValueError("config_identity de estrategia no coincide")
        if value.state:
            raise ValueError("Donchian20M5Strategy no admite estado adicional")


# Nombres alternativos legibles para integradores que prefieren el orden de
# las palabras en inglés o español. Todos refieren a una única implementación.
BaselineStrategy = BaselineStrategyAdapter
BaselineAdapter = BaselineStrategyAdapter
Donchian20BarsM5Strategy = Donchian20M5Strategy
Donchian20Strategy = Donchian20M5Strategy
DonchianStrategy = Donchian20M5Strategy
SignalExplanation = StrategyExplanation
StrategyEvaluation = StrategyExplanation
ResearchSignal = StrategySignal
compute_features = compute_strategy_features


def strategy_extensions() -> tuple[StrategyProtocol, ...]:
    """Devuelve las implementaciones disponibles sin registrar brokers."""

    return (BaselineStrategyAdapter(), Donchian20M5Strategy())


__all__ = [
    "BaselineStrategy",
    "BaselineAdapter",
    "BaselineStrategyAdapter",
    "Donchian20BarsM5Strategy",
    "Donchian20Config",
    "Donchian20M5Strategy",
    "Donchian20Strategy",
    "DonchianStrategy",
    "QuoteObservation",
    "ResearchSignal",
    "StrategyCheckpoint",
    "StrategyEvaluation",
    "StrategyExplanation",
    "StrategyFeatures",
    "StrategyOutput",
    "StrategyProtocol",
    "StrategySignal",
    "SignalExplanation",
    "VolatilityRegimeThreshold",
    "WarmupRequirements",
    "compute_features",
    "compute_strategy_features",
    "session_causal",
    "strategy_extensions",
]
