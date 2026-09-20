"""Detector multitemporal causal ``trend_pullback_v1``.

La estrategia no conoce proveedores ni persiste nada. Recibe series de velas
cerradas (o ``IndicatorSeries``) y devuelve evaluaciones explicables y señales
virtuales. Para cada disparador M1 sólo se consideran puntos cuya
``available_at`` ya ocurrió; una vela M15 con fin 10:15 jamás participa en una
evaluación disponible a las 10:07.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

from .indicators import IndicatorConfig, IndicatorPoint, IndicatorSeries, compute_indicators
from .models import OperationMode, Timeframe, parse_timeframe
from .quality import DataQuality, QualityFlag


class ConditionState(str, Enum):  # noqa: UP042 - preserve the public enum MRO
    FULFILLED = "fulfilled"
    FAILED = "failed"
    UNKNOWN = "unknown"
    DISABLED = "disabled"


class DecisionKind(str, Enum):  # noqa: UP042 - preserve the public enum MRO
    SIGNAL = "signal"
    PREPARATION = "preparation"
    DISCARDED = "discarded"
    BLOCKED = "blocked"
    WAIT = "wait"


@dataclass(frozen=True, slots=True)
class ConditionResult:
    name: str
    state: ConditionState
    observed: Any = None
    expected: Any = None
    reason: str = ""
    mandatory: bool = True

    @property
    def fulfilled(self) -> bool:
        return self.state is ConditionState.FULFILLED

    @property
    def status(self) -> str:
        return self.state.value

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "observed": self.observed,
            "expected": self.expected,
            "reason": self.reason,
            "mandatory": self.mandatory,
        }


@dataclass(frozen=True, slots=True)
class PreparationEpisode:
    episode_id: str
    instrument: str
    direction: str
    registered_at: datetime
    preparation_start: datetime
    preparation_end: datetime
    expires_at: datetime
    context_start: datetime | None = None
    used: bool = False
    invalidated: bool = False
    invalidation_reason: str | None = None


@dataclass(frozen=True, slots=True)
class Evaluation:
    timestamp: datetime
    instrument: str
    stage: str
    decision: DecisionKind
    direction: str | None
    conditions: tuple[ConditionResult, ...]
    values: Mapping[str, Any] = field(default_factory=dict)
    reasons: tuple[str, ...] = ()
    episode_id: str | None = None
    mode: OperationMode = OperationMode.REPLAY
    quality: DataQuality = field(default_factory=DataQuality.good)
    available_at: datetime | None = None

    @property
    def outcome(self) -> str:
        return self.decision.value

    @property
    def blocked(self) -> bool:
        return self.decision is DecisionKind.BLOCKED

    @property
    def all_mandatory(self) -> bool:
        return all(condition.state is ConditionState.FULFILLED for condition in self.conditions if condition.mandatory)

    @property
    def condition_map(self) -> dict[str, ConditionResult]:
        return {condition.name: condition for condition in self.conditions}

    def as_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "available_at": self.available_at.isoformat() if self.available_at else None,
            "instrument": self.instrument,
            "stage": self.stage,
            "decision": self.decision.value,
            "direction": self.direction,
            "conditions": [condition.as_dict() for condition in self.conditions],
            "values": dict(self.values),
            "reasons": list(self.reasons),
            "episode_id": self.episode_id,
            "mode": self.mode.value,
            "quality": {"status": self.quality.status, "flags": sorted(flag.value for flag in self.quality.flags)},
        }


@dataclass(frozen=True, slots=True)
class Signal:
    signal_id: str
    instrument: str
    direction: str
    detected_at: datetime
    context_start: datetime
    preparation_start: datetime
    trigger_start: datetime
    trigger_end: datetime
    episode_id: str
    values: Mapping[str, Any] = field(default_factory=dict)
    mode: OperationMode = OperationMode.REPLAY
    quality: DataQuality = field(default_factory=DataQuality.good)

    @property
    def timestamp(self) -> datetime:
        return self.detected_at

    def as_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "instrument": self.instrument,
            "direction": self.direction,
            "detected_at": self.detected_at.isoformat(),
            "context_start": self.context_start.isoformat(),
            "preparation_start": self.preparation_start.isoformat(),
            "trigger_start": self.trigger_start.isoformat(),
            "trigger_end": self.trigger_end.isoformat(),
            "episode_id": self.episode_id,
            "values": dict(self.values),
            "mode": self.mode.value,
            "quality": {"status": self.quality.status, "flags": sorted(flag.value for flag in self.quality.flags)},
        }


@dataclass(frozen=True, slots=True)
class StrategyResult:
    evaluations: tuple[Evaluation, ...]
    signals: tuple[Signal, ...]
    episodes: tuple[PreparationEpisode, ...] = ()

    def __iter__(self) -> Iterator[tuple[Evaluation, ...] | tuple[Signal, ...]]:
        # Permite ``evaluations, signals = strategy.evaluate(...)`` sin perder
        # los metadatos adicionales disponibles en el objeto.
        yield self.evaluations
        yield self.signals

    @property
    def signal_count(self) -> int:
        return len(self.signals)

    @property
    def blocked_count(self) -> int:
        return sum(evaluation.decision is DecisionKind.BLOCKED for evaluation in self.evaluations)


def _strict_keys(mapping: Mapping[str, Any], allowed: set[str], section: str) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise ValueError(f"Claves desconocidas en {section}: {sorted(unknown)}")


def _as_positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} debe ser entero positivo")
    return value


def _as_number(value: Any, name: str, *, minimum: float | None = None, maximum: float | None = None) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} debe ser numérico")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser numérico") from exc
    if minimum is not None and number < minimum:
        raise ValueError(f"{name} debe ser >= {minimum}")
    if maximum is not None and number > maximum:
        raise ValueError(f"{name} debe ser <= {maximum}")
    return number


_STRATEGY_ALLOWED_KEYS = {
    "name",
    "context_timeframe",
    "preparation_timeframe",
    "trigger_timeframe",
    "context_lookback",
    "preparation_lookback",
    "max_distance_atr",
    "rsi_threshold",
    "preparation_ttl_bars",
    "preparation_ttl_minutes",
    "require_closed",
    "one_signal_per_episode",
    "setup_timeframe",
    "setup_max_atr",
    "lookback",
    "optional_filters",
    "indicators",
    "timeframes",
    "lookbacks",
    "conditions",
    "mode",
}
_TIMEFRAME_GROUP_KEYS = {
    "context",
    "preparation",
    "trigger",
    "context_timeframe",
    "preparation_timeframe",
    "trigger_timeframe",
}
_TIMEFRAME_TARGETS = (
    ("context_timeframe", ("context", "context_timeframe")),
    ("preparation_timeframe", ("preparation", "preparation_timeframe")),
    ("trigger_timeframe", ("trigger", "trigger_timeframe")),
)
_GROUPED_STRATEGY_FIELDS = (
    (
        "lookbacks",
        {
            "context": "context_lookback",
            "preparation": "preparation_lookback",
            "context_lookback": "context_lookback",
            "preparation_lookback": "preparation_lookback",
        },
    ),
    (
        "conditions",
        {
            "max_distance_atr": "max_distance_atr",
            "rsi_threshold": "rsi_threshold",
            "preparation_ttl_bars": "preparation_ttl_bars",
        },
    ),
)


def _unwrap_strategy_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(mapping)
    if "strategy" not in raw:
        return raw
    nested = raw.pop("strategy")
    if not isinstance(nested, Mapping):
        raise TypeError("strategy debe ser un mapping")
    merged = dict(nested)
    for key in ("indicators", "timeframes", "lookbacks", "conditions", "optional_filters"):
        if key not in raw:
            continue
        if key in merged:
            raise ValueError(f"Parámetro duplicado en strategy: {key}")
        merged[key] = raw.pop(key)
    if raw:
        raise ValueError(f"Claves desconocidas en config raíz: {sorted(raw)}")
    return merged


def _normalize_strategy_aliases(values: dict[str, Any]) -> None:
    aliases = {"setup_timeframe": "preparation_timeframe", "setup_max_atr": "max_distance_atr"}
    for alias, canonical in aliases.items():
        if alias not in values:
            continue
        if canonical in values:
            raise ValueError(f"Parámetro duplicado en strategy: {canonical}")
        values[canonical] = values.pop(alias)
    if "lookback" not in values:
        return
    if "context_lookback" in values or "preparation_lookback" in values:
        raise ValueError("lookback no puede combinarse con context_lookback/preparation_lookback")
    lookback = values.pop("lookback")
    values["context_lookback"] = lookback
    values["preparation_lookback"] = lookback


def _apply_timeframe_mapping(values: dict[str, Any], timeframes: Mapping[str, Any]) -> None:
    _strict_keys(dict(timeframes), _TIMEFRAME_GROUP_KEYS, "strategy.timeframes")
    for target, aliases in _TIMEFRAME_TARGETS:
        present = [key for key in aliases if key in timeframes]
        if len(present) > 1:
            raise ValueError(f"Parámetro duplicado en timeframes: {target}")
        if present:
            values[target] = timeframes[present[0]]


def _apply_timeframe_sequence(values: dict[str, Any], timeframes: Sequence[Any]) -> None:
    parsed = [parse_timeframe(value) for value in timeframes]
    by_seconds = {tf.seconds: tf for tf in parsed}
    for target, default_seconds in (
        ("trigger_timeframe", 60),
        ("preparation_timeframe", 300),
        ("context_timeframe", 900),
    ):
        if default_seconds not in by_seconds:
            raise ValueError(f"timeframes no contiene la temporalidad inicial {default_seconds}s")
        values[target] = by_seconds[default_seconds]


def _apply_timeframes(values: dict[str, Any]) -> None:
    timeframes = values.pop("timeframes", None)
    if timeframes is None:
        return
    if isinstance(timeframes, Mapping):
        _apply_timeframe_mapping(values, timeframes)
        return
    if isinstance(timeframes, Sequence) and not isinstance(timeframes, (str, bytes)):
        _apply_timeframe_sequence(values, timeframes)
        return
    raise TypeError("timeframes debe ser mapping o lista")


def _apply_ttl_minutes(values: dict[str, Any], config_type: type[Any]) -> None:
    ttl_minutes = values.pop("preparation_ttl_minutes", None)
    if ttl_minutes is None:
        return
    try:
        ttl_seconds = float(ttl_minutes) * 60.0
    except (TypeError, ValueError) as exc:
        raise ValueError("preparation_ttl_minutes debe ser numérico") from exc
    if ttl_seconds <= 0 or not ttl_seconds.is_integer():
        raise ValueError("preparation_ttl_minutes debe ser positivo")
    default_config = config_type()
    preparation_tf = parse_timeframe(values.get("preparation_timeframe", default_config.preparation_timeframe))
    if int(ttl_seconds) % preparation_tf.seconds != 0:
        raise ValueError("preparation_ttl_minutes debe ser múltiplo de la temporalidad de preparación")
    if "preparation_ttl_bars" in values:
        raise ValueError("preparation_ttl_minutes no puede combinarse con preparation_ttl_bars")
    values["preparation_ttl_bars"] = int(ttl_seconds) // preparation_tf.seconds


def _apply_grouped_strategy_values(values: dict[str, Any]) -> None:
    for grouping_name, fields in _GROUPED_STRATEGY_FIELDS:
        grouping = values.pop(grouping_name, None)
        if grouping is None:
            continue
        if not isinstance(grouping, Mapping):
            raise TypeError(f"{grouping_name} debe ser mapping")
        _strict_keys(grouping, set(fields), f"strategy.{grouping_name}")
        for key, target in fields.items():
            if key not in grouping:
                continue
            if target in values:
                raise ValueError(f"Parámetro duplicado en {grouping_name}: {target}")
            values[target] = grouping[key]


def _strategy_values(mapping: Mapping[str, Any], config_type: type[Any]) -> dict[str, Any]:
    raw = _unwrap_strategy_mapping(mapping)
    _strict_keys(raw, _STRATEGY_ALLOWED_KEYS, "strategy")
    values = dict(raw)
    _normalize_strategy_aliases(values)
    _apply_timeframes(values)
    _apply_ttl_minutes(values, config_type)
    _apply_grouped_strategy_values(values)
    if values.get("indicators") is not None:
        values["indicators"] = IndicatorConfig.from_mapping(values["indicators"])
    return values


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    name: str = "trend_pullback_v1"
    context_timeframe: Timeframe | str = "M15"
    preparation_timeframe: Timeframe | str = "M5"
    trigger_timeframe: Timeframe | str = "M1"
    context_lookback: int = 3
    preparation_lookback: int = 3
    max_distance_atr: float = 0.5
    rsi_threshold: float = 50.0
    preparation_ttl_bars: int = 3
    require_closed: bool = True
    one_signal_per_episode: bool = True
    optional_filters: Mapping[str, bool] = field(default_factory=dict)
    indicators: IndicatorConfig = field(default_factory=IndicatorConfig)
    mode: OperationMode = OperationMode.REPLAY

    def __post_init__(self) -> None:
        context = parse_timeframe(self.context_timeframe)
        preparation = parse_timeframe(self.preparation_timeframe)
        trigger = parse_timeframe(self.trigger_timeframe)
        if not (context.seconds > preparation.seconds > trigger.seconds):
            raise ValueError("Las temporalidades deben cumplir contexto > preparación > disparador")
        object.__setattr__(self, "context_timeframe", context)
        object.__setattr__(self, "preparation_timeframe", preparation)
        object.__setattr__(self, "trigger_timeframe", trigger)
        object.__setattr__(self, "context_lookback", _as_positive_int(self.context_lookback, "context_lookback"))
        object.__setattr__(
            self, "preparation_lookback", _as_positive_int(self.preparation_lookback, "preparation_lookback")
        )
        object.__setattr__(
            self, "preparation_ttl_bars", _as_positive_int(self.preparation_ttl_bars, "preparation_ttl_bars")
        )
        object.__setattr__(self, "max_distance_atr", _as_number(self.max_distance_atr, "max_distance_atr", minimum=0.0))
        object.__setattr__(
            self, "rsi_threshold", _as_number(self.rsi_threshold, "rsi_threshold", minimum=0.0, maximum=100.0)
        )
        if not isinstance(self.require_closed, bool):
            raise ValueError("require_closed debe ser booleano")
        if not isinstance(self.one_signal_per_episode, bool) or not self.one_signal_per_episode:
            raise ValueError("one_signal_per_episode=true es obligatorio para evitar señales duplicadas")
        filters = dict(self.optional_filters)
        if any(not isinstance(key, str) or not isinstance(value, bool) for key, value in filters.items()):
            raise ValueError("optional_filters debe mapear nombres a booleanos")
        object.__setattr__(self, "optional_filters", filters)
        if not isinstance(self.indicators, IndicatorConfig):
            object.__setattr__(self, "indicators", IndicatorConfig.from_mapping(self.indicators))
        if not isinstance(self.mode, OperationMode):
            object.__setattr__(self, "mode", OperationMode(str(self.mode).upper()))

    @property
    def preparation_ttl(self) -> timedelta:
        return parse_timeframe(self.preparation_timeframe).delta * self.preparation_ttl_bars

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> StrategyConfig:
        """Carga TOML-like mapping con rechazo estricto de claves desconocidas.

        Se aceptan tanto una sección ``[strategy]`` como un mapping directo,
        y se pueden agrupar ``timeframes``/``lookbacks``/``conditions`` sin
        perder validación.
        """

        if mapping is None:
            return cls()
        if not isinstance(mapping, Mapping):
            raise TypeError("strategy config debe ser un mapping")
        return cls(**_strategy_values(mapping, cls))


@dataclass(slots=True)
class _PreparedStream:
    timeframe: Timeframe
    instrument: str
    points: tuple[IndicatorPoint, ...]
    quality: DataQuality


@dataclass(frozen=True, slots=True)
class _ContextSnapshot:
    point: IndicatorPoint
    direction: str | None
    known: bool
    values: Mapping[str, Any]
    # The context may become usable later than the selected point when one of
    # its lookback points arrives late.  Keep that effective availability
    # separate from ``point.available_at`` so preparation/trigger evidence
    # cannot claim an earlier causal timestamp.
    effective_available_at: datetime | None = None


@dataclass(slots=True)
class _EvaluationState:
    context: _ContextSnapshot | None = None
    active: PreparationEpisode | None = None
    auxiliary_index: int = 0
    invalidation_reason: str = ""


def _quality_flag_or_invalid(value: Any) -> QualityFlag:
    try:
        return QualityFlag(str(value).lower())
    except ValueError:
        return QualityFlag.INVALID


def _quality_from_mapping(value: Mapping[str, Any]) -> DataQuality:
    flags = value.get("flags", ())
    if isinstance(flags, str):
        flags = [flags]
    parsed = {_quality_flag_or_invalid(flag) for flag in flags}
    if value.get("status") and str(value["status"]).lower() not in {"valid", "synthetic"}:
        parsed.add(_quality_flag_or_invalid(value["status"]))
    reasons = tuple(str(item) for item in value.get("reasons", ()))
    return DataQuality(frozenset(parsed), reasons)


def _quality_from(value: Any) -> DataQuality:
    if isinstance(value, DataQuality):
        return value
    if value is None:
        return DataQuality.good()
    if isinstance(value, str):
        try:
            return DataQuality.from_flag(QualityFlag(value.lower()))
        except ValueError:
            return DataQuality.from_flag(QualityFlag.INVALID, value)
    if isinstance(value, Mapping):
        return _quality_from_mapping(value)
    raise TypeError("quality debe ser DataQuality, string o mapping")


def _stream_value(streams: Mapping[Any, Any], timeframe: Timeframe) -> Any:
    for key in (timeframe.name, timeframe.name.lower(), timeframe):
        if key in streams:
            return streams[key]
    return None


def _point_available(point: IndicatorPoint) -> datetime | None:
    if not point.closed:
        return None
    available = point.available_at or point.end
    if not isinstance(available, datetime):
        return None
    if not isinstance(point.end, datetime) or available.tzinfo is None or point.end.tzinfo is None:
        return None
    # Una IndicatorPoint puede llegar de un integrador externo sin pasar por
    # Candle.__post_init__. Aun así, una vela cerrada nunca queda disponible
    # antes de su fin; se eleva el timestamp al límite causal y se evita
    # cualquier lookahead silencioso.
    end_utc = point.end.astimezone(UTC)
    return max(available.astimezone(UTC), end_utc)


class TrendPullbackStrategy:
    """Implementación única para replay, backtest y observación."""

    VERSION = "trend_pullback_v1"

    def __init__(self, config: StrategyConfig | Mapping[str, Any] | None = None) -> None:
        self.config = config if isinstance(config, StrategyConfig) else StrategyConfig.from_mapping(config)

    def _prepare_stream(self, value: Any, timeframe: Timeframe) -> _PreparedStream:
        metadata_quality = DataQuality.good()
        if isinstance(value, IndicatorSeries):
            if value.timeframe != timeframe:
                raise ValueError(f"Serie {value.timeframe} no coincide con {timeframe}")
            return _PreparedStream(timeframe, value.instrument, tuple(value.points), DataQuality.good())
        if value is None:
            return _PreparedStream(
                timeframe, "unknown", (), DataQuality.from_flag(QualityFlag.INSUFFICIENT, "stream_missing")
            )
        if isinstance(value, Mapping):
            metadata_quality = _quality_from(value.get("quality"))
            raw_points = value.get("points", value.get("candles", value.get("bars")))
            if raw_points is None:
                raise ValueError(f"stream {timeframe.name} requiere points/candles/bars")
            value = raw_points
        sequence = tuple(value)
        if sequence and all(isinstance(item, IndicatorPoint) for item in sequence):
            points = sequence
            instrument = "unknown"
        else:
            series = compute_indicators(sequence, self.config.indicators)
            points = tuple(series.points)
            instrument = series.instrument
        if not instrument and sequence:
            instrument = getattr(sequence[0], "instrument", "unknown")
        return _PreparedStream(timeframe, instrument or "unknown", points, metadata_quality)

    def _point_quality_ok(self, point: IndicatorPoint, stream_quality: DataQuality) -> bool:
        return stream_quality.valid and point.quality.valid and point.closed

    def _context_snapshot(
        self,
        stream: _PreparedStream,
        index: int,
        *,
        as_of: datetime | None = None,
    ) -> _ContextSnapshot:
        point = stream.points[index]
        point_available = _point_available(point)
        watermark = as_of or point_available
        values = {
            "close": point.close,
            "ema_fast": point.ema_fast,
            "ema_slow": point.ema_slow,
            "ema_fast_name": f"EMA{self.config.indicators.ema_fast}",
            "ema_slow_name": f"EMA{self.config.indicators.ema_slow}",
        }
        required_reason = ""
        if point_available is None or (watermark is not None and point_available > watermark):
            required_reason = "context_not_available"
            return _ContextSnapshot(
                point,
                None,
                False,
                {**values, "reason": required_reason},
                point_available,
            )
        lag_index = index - self.config.context_lookback
        if not self._point_quality_ok(point, stream.quality):
            required_reason = "context_quality_blocked"
            return _ContextSnapshot(
                point,
                None,
                False,
                {**values, "reason": required_reason},
                point_available,
            )
        if lag_index < 0:
            required_reason = "context_lookback_insufficient"
            return _ContextSnapshot(
                point,
                None,
                False,
                {**values, "reason": required_reason},
                point_available,
            )
        lag = stream.points[lag_index]
        lag_available = _point_available(lag)
        if lag_available is None or (watermark is not None and lag_available > watermark):
            required_reason = "context_lag_not_available"
            return _ContextSnapshot(
                point,
                None,
                False,
                {
                    **values,
                    "ema_slow_lag": getattr(lag, "ema_slow", None),
                    "reason": required_reason,
                },
                max(
                    (candidate for candidate in (point_available, lag_available) if candidate is not None),
                    default=point_available,
                ),
            )
        if (
            not self._point_quality_ok(lag, stream.quality)
            or lag.ema_slow is None
            or point.ema_fast is None
            or point.ema_slow is None
        ):
            required_reason = "context_indicator_not_ready"
            return _ContextSnapshot(
                point,
                None,
                False,
                {**values, "ema_slow_lag": getattr(lag, "ema_slow", None), "reason": required_reason},
                max(
                    (candidate for candidate in (point_available, lag_available) if candidate is not None),
                    default=point_available,
                ),
            )
        values["ema_slow_lag"] = lag.ema_slow
        bullish = point.ema_fast > point.ema_slow and point.ema_slow > lag.ema_slow
        bearish = point.ema_fast < point.ema_slow and point.ema_slow < lag.ema_slow
        direction = "UP" if bullish else "DOWN" if bearish else None
        values["bullish"] = bullish
        values["bearish"] = bearish
        return _ContextSnapshot(
            point,
            direction,
            True,
            values,
            max(
                (candidate for candidate in (point_available, lag_available) if candidate is not None),
                default=point_available,
            ),
        )

    def _episode_id(self, instrument: str, direction: str, prep: IndicatorPoint, context: _ContextSnapshot) -> str:
        payload = {
            "version": self.VERSION,
            "instrument": instrument,
            "direction": direction,
            "preparation_start": prep.start.isoformat(),
            "context_start": context.point.start.isoformat(),
            "config": {
                "context_lookback": self.config.context_lookback,
                "preparation_lookback": self.config.preparation_lookback,
                "max_distance_atr": self.config.max_distance_atr,
                "rsi_threshold": self.config.rsi_threshold,
                "ttl": self.config.preparation_ttl_bars,
            },
        }
        return "ep_" + hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:32]

    def _preparation_price_conditions(
        self,
        *,
        point: IndicatorPoint,
        lag: IndicatorPoint | None,
        stream_quality: DataQuality,
        direction: str | None,
        watermark: datetime | None,
    ) -> tuple[list[ConditionResult], dict[str, Any], datetime | None]:
        """Evaluate preparation price inputs only when every lag is causal."""

        values: dict[str, Any] = {}
        point_available = _point_available(point)
        if (
            point_available is None
            or (watermark is not None and point_available > watermark)
            or not self._point_quality_ok(point, stream_quality)
            or point.close is None
        ):
            return (
                [
                    self._condition(
                        "pullback",
                        ConditionState.UNKNOWN,
                        point.close,
                        "directional_close_change",
                        "preparation_quality_blocked",
                    ),
                    self._condition(
                        "distance_to_ema",
                        ConditionState.UNKNOWN,
                        None,
                        f"<= {self.config.max_distance_atr} ATR",
                        "preparation_quality_blocked",
                    ),
                ],
                values,
                None,
            )
        if lag is None or not self._point_quality_ok(lag, stream_quality) or lag.close is None:
            return (
                [
                    self._condition(
                        "pullback",
                        ConditionState.UNKNOWN,
                        None,
                        "directional_close_change",
                        "preparation_lookback_insufficient",
                    ),
                    self._condition(
                        "distance_to_ema",
                        ConditionState.UNKNOWN,
                        None,
                        f"<= {self.config.max_distance_atr} ATR",
                        "preparation_lookback_insufficient",
                    ),
                ],
                values,
                None,
            )
        lag_available = _point_available(lag)
        if lag_available is None or (watermark is not None and lag_available > watermark):
            return (
                [
                    self._condition(
                        "pullback",
                        ConditionState.UNKNOWN,
                        None,
                        "directional_close_change",
                        "preparation_lag_not_available",
                    ),
                    self._condition(
                        "distance_to_ema",
                        ConditionState.UNKNOWN,
                        None,
                        f"<= {self.config.max_distance_atr} ATR",
                        "preparation_lag_not_available",
                    ),
                ],
                values,
                None,
            )

        values["close_lag"] = lag.close
        if direction == "UP":
            pullback = point.close < lag.close
        elif direction == "DOWN":
            pullback = point.close > lag.close
        else:
            pullback = False
        conditions = [
            self._condition(
                "pullback",
                ConditionState.FULFILLED if pullback else ConditionState.FAILED,
                point.close - lag.close,
                "< 0 (UP) or > 0 (DOWN)",
                "" if pullback else "directional_pullback_failed",
            )
        ]
        if point.ema_fast is None or point.atr is None:
            conditions.append(
                self._condition(
                    "distance_to_ema",
                    ConditionState.UNKNOWN,
                    None,
                    f"<= {self.config.max_distance_atr} ATR",
                    "ema_or_atr_not_ready",
                )
            )
            return conditions, values, lag_available
        distance = abs(point.close - point.ema_fast)
        limit = self.config.max_distance_atr * point.atr
        values.update({"distance_to_ema": distance, "distance_limit": limit})
        within = distance <= limit if math_is_finite(limit) else False
        conditions.append(
            self._condition(
                "distance_to_ema",
                ConditionState.FULFILLED if within else ConditionState.FAILED,
                distance,
                f"<= {limit}",
                "" if within else "distance_exceeded",
            )
        )
        return conditions, values, lag_available

    def _condition(
        self,
        name: str,
        state: ConditionState,
        observed: Any = None,
        expected: Any = None,
        reason: str = "",
        mandatory: bool = True,
    ) -> ConditionResult:
        return ConditionResult(name, state, observed, expected, reason, mandatory)

    def _decision(
        self, conditions: Sequence[ConditionResult], *, preparation: bool = False, duplicate: bool = False
    ) -> DecisionKind:
        mandatory = [condition for condition in conditions if condition.mandatory]
        if duplicate or any(
            condition.reason in {"preparation_expired", "context_invalidated", "signal_duplicate"}
            for condition in mandatory
        ):
            return DecisionKind.BLOCKED
        if any(condition.state is ConditionState.UNKNOWN for condition in mandatory):
            return DecisionKind.BLOCKED
        if any(condition.state is ConditionState.FAILED for condition in mandatory):
            return DecisionKind.DISCARDED
        return DecisionKind.PREPARATION if preparation else DecisionKind.SIGNAL

    def _preparation_evaluation(
        self,
        stream: _PreparedStream,
        index: int,
        context: _ContextSnapshot | None,
        instrument: str,
        *,
        as_of: datetime | None = None,
    ) -> tuple[Evaluation, PreparationEpisode | None]:
        point = stream.points[index]
        point_available = _point_available(point)
        watermark = as_of or point_available
        available = point_available or point.end
        direction = context.direction if context and context.direction in {"UP", "DOWN"} else None
        conditions: list[ConditionResult] = []
        values: dict[str, Any] = {
            "close": point.close,
            "ema_fast": point.ema_fast,
            "atr": point.atr,
            "context_direction": context.direction if context else None,
        }
        if context is None:
            conditions.append(
                self._condition("context_direction", ConditionState.UNKNOWN, None, "UP/DOWN", "context_not_available")
            )
        elif context.direction in {"UP", "DOWN"}:
            conditions.append(
                self._condition("context_direction", ConditionState.FULFILLED, context.direction, "UP/DOWN")
            )
        elif context.known:
            conditions.append(
                self._condition(
                    "context_direction", ConditionState.FAILED, context.direction, "UP/DOWN", "context_neutral"
                )
            )
        else:
            conditions.append(
                self._condition(
                    "context_direction", ConditionState.UNKNOWN, None, "UP/DOWN", "context_indicator_not_ready"
                )
            )
        lag_index = index - self.config.preparation_lookback
        lag = stream.points[lag_index] if lag_index >= 0 else None
        price_conditions, price_values, lag_available = self._preparation_price_conditions(
            point=point,
            lag=lag,
            stream_quality=stream.quality,
            direction=direction,
            watermark=watermark,
        )
        conditions.extend(price_conditions)
        values.update(price_values)
        if lag_available is not None:
            available = max(available, lag_available)
            if context is not None and context.effective_available_at is not None:
                available = max(available, context.effective_available_at)
        quality_condition = (
            ConditionState.FULFILLED if self._point_quality_ok(point, stream.quality) else ConditionState.UNKNOWN
        )
        conditions.append(
            self._condition(
                "data_quality",
                quality_condition,
                point.quality.status,
                "valid and closed",
                "" if quality_condition is ConditionState.FULFILLED else "data_quality_blocked",
            )
        )
        conditions.append(
            self._condition(
                "news_filter", ConditionState.DISABLED, None, "not configured", "not_configured", mandatory=False
            )
        )
        decision = self._decision(conditions, preparation=True)
        reasons = tuple(dict.fromkeys(condition.reason for condition in conditions if condition.reason))
        episode: PreparationEpisode | None = None
        if decision is DecisionKind.PREPARATION and direction is not None and context is not None:
            episode_id = self._episode_id(instrument, direction, point, context)
            episode = PreparationEpisode(
                episode_id=episode_id,
                instrument=instrument,
                direction=direction,
                registered_at=available,
                preparation_start=point.start,
                preparation_end=point.end,
                expires_at=available + self.config.preparation_ttl,
                context_start=context.point.start,
            )
        evaluation = Evaluation(
            timestamp=point.end,
            available_at=available,
            instrument=instrument,
            stage="preparation",
            decision=decision,
            direction=direction,
            conditions=tuple(conditions),
            values={**values, "context": dict(context.values) if context else None},
            reasons=reasons,
            episode_id=episode.episode_id if episode else None,
            mode=self.config.mode,
            quality=point.quality,
        )
        return evaluation, episode

    def _resolve_active_episode(
        self,
        available: datetime,
        active: PreparationEpisode | None,
    ) -> tuple[str, PreparationEpisode | None, PreparationEpisode | None]:
        if active is not None and available >= active.expires_at:
            expired = replace(active, invalidated=True, invalidation_reason="preparation_expired")
            return "preparation_expired", expired, None
        if active is not None and active.invalidated:
            return active.invalidation_reason or "context_invalidated", active, None
        return "", active, active

    def _context_revalidation(
        self,
        context: _ContextSnapshot | None,
        direction: str | None,
        expected_context: str,
    ) -> tuple[ConditionResult, str]:
        if context is None:
            return self._condition(
                "context_revalidated", ConditionState.UNKNOWN, None, expected_context, "context_not_available"
            ), ""
        if direction in {"UP", "DOWN"} and context.direction == direction:
            return self._condition("context_revalidated", ConditionState.FULFILLED, context.direction, direction), ""
        if not context.known:
            return self._condition(
                "context_revalidated",
                ConditionState.UNKNOWN,
                context.direction,
                expected_context,
                "context_indicator_not_ready",
            ), ""
        return self._condition(
            "context_revalidated", ConditionState.FAILED, context.direction, direction, "context_invalidated"
        ), "context_invalidated"

    def _episode_condition(
        self,
        active_for_eval: PreparationEpisode | None,
        invalid_reason: str,
    ) -> ConditionResult:
        if active_for_eval is None:
            reason = invalid_reason or "preparation_not_registered"
            state = ConditionState.FAILED if invalid_reason == "preparation_expired" else ConditionState.UNKNOWN
            return self._condition("preparation_episode", state, None, "registered and unexpired", reason)
        if active_for_eval.used:
            return self._condition(
                "preparation_episode",
                ConditionState.FAILED,
                active_for_eval.episode_id,
                "unused episode",
                "signal_duplicate",
            )
        return self._condition(
            "preparation_episode", ConditionState.FULFILLED, active_for_eval.episode_id, "registered and unexpired"
        )

    def _cross_condition(
        self,
        stream: _PreparedStream,
        index: int,
        direction: str | None,
    ) -> ConditionResult:
        point = stream.points[index]
        previous = stream.points[index - 1] if index > 0 else None
        if direction not in {"UP", "DOWN"}:
            return self._condition(
                "close_cross_ema", ConditionState.UNKNOWN, None, "cross in direction", "direction_not_known"
            )
        if not self._point_quality_ok(point, stream.quality) or point.close is None or point.ema_fast is None:
            return self._condition(
                "close_cross_ema", ConditionState.UNKNOWN, None, "cross in direction", "trigger_indicator_not_ready"
            )
        if (
            previous is None
            or not self._point_quality_ok(previous, stream.quality)
            or previous.close is None
            or previous.ema_fast is None
            or previous.end != point.start
        ):
            return self._condition(
                "close_cross_ema",
                ConditionState.UNKNOWN,
                None,
                "cross in direction",
                "trigger_previous_bar_unavailable",
            )
        if direction == "UP":
            crossed = previous.close <= previous.ema_fast and point.close > point.ema_fast
        else:
            crossed = previous.close >= previous.ema_fast and point.close < point.ema_fast
        return self._condition(
            "close_cross_ema",
            ConditionState.FULFILLED if crossed else ConditionState.FAILED,
            {"previous_close": previous.close, "close": point.close},
            direction,
            "" if crossed else "no_directional_cross",
        )

    def _rsi_condition(self, point: IndicatorPoint, stream: _PreparedStream, direction: str | None) -> ConditionResult:
        if direction not in {"UP", "DOWN"} or point.rsi is None or not self._point_quality_ok(point, stream.quality):
            return self._condition(
                "rsi_threshold", ConditionState.UNKNOWN, point.rsi, self.config.rsi_threshold, "rsi_not_ready"
            )
        passed = point.rsi > self.config.rsi_threshold if direction == "UP" else point.rsi < self.config.rsi_threshold
        expected = f"> {self.config.rsi_threshold}" if direction == "UP" else f"< {self.config.rsi_threshold}"
        return self._condition(
            "rsi_threshold",
            ConditionState.FULFILLED if passed else ConditionState.FAILED,
            point.rsi,
            expected,
            "" if passed else "rsi_threshold_failed",
        )

    def _trigger_conditions(
        self,
        stream: _PreparedStream,
        index: int,
        context: _ContextSnapshot | None,
        active_for_eval: PreparationEpisode | None,
        direction: str | None,
        invalid_reason: str,
    ) -> tuple[list[ConditionResult], str]:
        point = stream.points[index]
        expected_context = direction or "UP/DOWN"
        context_condition, context_reason = self._context_revalidation(context, direction, expected_context)
        invalid_reason = invalid_reason or context_reason
        quality_condition = (
            ConditionState.FULFILLED if self._point_quality_ok(point, stream.quality) else ConditionState.UNKNOWN
        )
        conditions = [
            context_condition,
            self._episode_condition(active_for_eval, invalid_reason),
            self._cross_condition(stream, index, direction),
            self._rsi_condition(point, stream, direction),
            self._condition(
                "data_quality",
                quality_condition,
                point.quality.status,
                "valid and closed",
                "" if quality_condition is ConditionState.FULFILLED else "data_quality_blocked",
            ),
            self._condition(
                "news_filter", ConditionState.DISABLED, None, "not configured", "not_configured", mandatory=False
            ),
        ]
        return conditions, invalid_reason

    def _signal_for_trigger(
        self,
        decision: DecisionKind,
        point: IndicatorPoint,
        available: datetime,
        context: _ContextSnapshot | None,
        active_for_eval: PreparationEpisode | None,
        direction: str | None,
        instrument: str,
        values: Mapping[str, Any],
    ) -> tuple[Signal | None, PreparationEpisode | None]:
        if decision is not DecisionKind.SIGNAL or active_for_eval is None or context is None or direction is None:
            return None, active_for_eval
        payload = f"{self.VERSION}|{instrument}|{active_for_eval.episode_id}|{point.start.isoformat()}|indicators={self.config.indicators.ema_fast},{self.config.indicators.ema_slow},{self.config.indicators.rsi_period},{self.config.indicators.atr_period}"
        signal_id = "sig_" + hashlib.sha256(payload.encode()).hexdigest()[:32]
        signal = Signal(
            signal_id=signal_id,
            instrument=instrument,
            direction=direction,
            detected_at=available,
            context_start=context.point.start,
            preparation_start=active_for_eval.preparation_start,
            trigger_start=point.start,
            trigger_end=point.end,
            episode_id=active_for_eval.episode_id,
            values=values,
            mode=self.config.mode,
            quality=point.quality,
        )
        return signal, replace(active_for_eval, used=True)

    def _trigger_evaluation(
        self,
        stream: _PreparedStream,
        index: int,
        context: _ContextSnapshot | None,
        active: PreparationEpisode | None,
        instrument: str,
    ) -> tuple[Evaluation, Signal | None, PreparationEpisode | None]:
        point = stream.points[index]
        available = _point_available(point) or point.end
        invalid_reason, active, active_for_eval = self._resolve_active_episode(available, active)
        direction = (
            active_for_eval.direction
            if active_for_eval
            else (context.direction if context and context.direction in {"UP", "DOWN"} else None)
        )
        conditions, invalid_reason = self._trigger_conditions(
            stream, index, context, active_for_eval, direction, invalid_reason
        )
        duplicate = any(condition.reason == "signal_duplicate" for condition in conditions)
        decision = self._decision(conditions, duplicate=duplicate)
        reasons = tuple(dict.fromkeys(condition.reason for condition in conditions if condition.reason))
        values = {
            "close": point.close,
            "ema_fast": point.ema_fast,
            "rsi": point.rsi,
            "context": dict(context.values) if context else None,
            "preparation_expires_at": active.expires_at.isoformat() if active else None,
        }
        signal, next_active = self._signal_for_trigger(
            decision,
            point,
            available,
            context,
            active_for_eval,
            direction,
            instrument,
            values,
        )
        evaluation = Evaluation(
            timestamp=point.end,
            available_at=available,
            instrument=instrument,
            stage="trigger",
            decision=decision,
            direction=direction,
            conditions=tuple(conditions),
            values=values,
            reasons=reasons,
            episode_id=(active_for_eval.episode_id if active_for_eval else active.episode_id if active else None),
            mode=self.config.mode,
            quality=point.quality,
        )
        return evaluation, signal, next_active

    def _prepare_evaluation_streams(
        self,
        streams: Mapping[Any, Any],
    ) -> tuple[_PreparedStream, _PreparedStream, _PreparedStream, str]:
        config = self.config
        context_timeframe = parse_timeframe(config.context_timeframe)
        preparation_timeframe = parse_timeframe(config.preparation_timeframe)
        trigger_timeframe = parse_timeframe(config.trigger_timeframe)
        context_stream = self._prepare_stream(_stream_value(streams, context_timeframe), context_timeframe)
        preparation_stream = self._prepare_stream(_stream_value(streams, preparation_timeframe), preparation_timeframe)
        trigger_stream = self._prepare_stream(_stream_value(streams, trigger_timeframe), trigger_timeframe)
        instruments = {
            stream.instrument
            for stream in (context_stream, preparation_stream, trigger_stream)
            if stream.instrument not in {"", "unknown"}
        }
        if len(instruments) > 1:
            raise ValueError(f"Las temporalidades deben compartir instrumento, llegaron {sorted(instruments)}")
        return context_stream, preparation_stream, trigger_stream, next(iter(instruments), "unknown")

    @staticmethod
    def _effective_auxiliary_available(
        stream: _PreparedStream,
        index: int,
        lookback: int,
    ) -> datetime | None:
        available = _point_available(stream.points[index])
        if available is None:
            return None
        lag_index = index - lookback
        if lag_index < 0:
            return available
        lag_available = _point_available(stream.points[lag_index])
        if lag_available is None:
            return None
        return max(available, lag_available)

    def _ordered_auxiliary(
        self,
        context_stream: _PreparedStream,
        preparation_stream: _PreparedStream,
    ) -> list[tuple[datetime, int, int, str]]:
        auxiliary: list[tuple[datetime, int, int, str]] = []
        for index in range(len(context_stream.points)):
            available = self._effective_auxiliary_available(context_stream, index, self.config.context_lookback)
            if available is not None:
                auxiliary.append((available, 0, index, "context"))
        for index in range(len(preparation_stream.points)):
            available = self._effective_auxiliary_available(preparation_stream, index, self.config.preparation_lookback)
            if available is not None:
                auxiliary.append((available, 1, index, "preparation"))
        auxiliary.sort(key=lambda item: (item[0], item[1], item[2]))
        return auxiliary

    @staticmethod
    def _ordered_triggers(stream: _PreparedStream) -> list[tuple[datetime | None, int, IndicatorPoint]]:
        triggers = [(_point_available(point), index, point) for index, point in enumerate(stream.points)]
        triggers.sort(key=lambda item: (item[0] or item[2].end, item[2].start, item[1]))
        return triggers

    def _consume_auxiliary(
        self,
        state: _EvaluationState,
        auxiliary: Sequence[tuple[datetime, int, int, str]],
        watermark: datetime | None,
        context_stream: _PreparedStream,
        preparation_stream: _PreparedStream,
        instrument: str,
        evaluations: list[Evaluation],
        episodes: list[PreparationEpisode],
    ) -> None:
        if watermark is None:
            return
        while state.auxiliary_index < len(auxiliary) and auxiliary[state.auxiliary_index][0] <= watermark:
            available, _priority, item_index, kind = auxiliary[state.auxiliary_index]
            if kind == "context":
                state.context = self._context_snapshot(context_stream, item_index, as_of=available)
                if state.active is not None and (
                    state.context.direction != state.active.direction or not state.context.known
                ):
                    state.invalidation_reason = "context_invalidated"
                    state.active = replace(
                        state.active, invalidated=True, invalidation_reason=state.invalidation_reason
                    )
                    state.active = None
            else:
                prep_evaluation, episode = self._preparation_evaluation(
                    preparation_stream,
                    item_index,
                    state.context,
                    instrument,
                    as_of=available,
                )
                evaluations.append(prep_evaluation)
                if episode is not None:
                    state.active = episode
                    episodes.append(episode)
                    state.invalidation_reason = ""
            state.auxiliary_index += 1

    def _unavailable_trigger_evaluation(self, point: IndicatorPoint, instrument: str) -> Evaluation:
        conditions = (
            self._condition(
                "data_quality",
                ConditionState.UNKNOWN,
                point.quality.status,
                "valid and closed",
                "trigger_not_available",
            ),
            self._condition(
                "signal", ConditionState.UNKNOWN, None, "all mandatory conditions", "trigger_not_available"
            ),
            self._condition(
                "news_filter", ConditionState.DISABLED, None, "not configured", "not_configured", mandatory=False
            ),
        )
        return Evaluation(
            timestamp=point.end,
            instrument=instrument,
            stage="trigger",
            decision=DecisionKind.BLOCKED,
            direction=None,
            conditions=conditions,
            values={},
            reasons=("trigger_not_available",),
            episode_id=None,
            mode=self.config.mode,
            quality=point.quality,
            available_at=None,
        )

    def _process_trigger(
        self,
        state: _EvaluationState,
        trigger_stream: _PreparedStream,
        trigger_available: datetime | None,
        trigger_index: int,
        trigger_point: IndicatorPoint,
        instrument: str,
        evaluations: list[Evaluation],
        signals: list[Signal],
    ) -> None:
        if trigger_available is None:
            evaluations.append(self._unavailable_trigger_evaluation(trigger_point, instrument))
            return
        trigger_evaluation, signal, next_active = self._trigger_evaluation(
            trigger_stream,
            trigger_index,
            state.context,
            state.active,
            instrument,
        )
        if (
            state.invalidation_reason
            and "context_invalidated" not in trigger_evaluation.reasons
            and state.active is None
        ):
            # No se altera la decisión ni se transforma unknown en failed; se
            # conserva evidencia de la invalidación en reasons.
            trigger_evaluation = replace(
                trigger_evaluation,
                reasons=tuple(dict.fromkeys((*trigger_evaluation.reasons, state.invalidation_reason))),
            )
        evaluations.append(trigger_evaluation)
        if signal is not None:
            signals.append(signal)
            state.active = next_active
        elif next_active is not None and next_active.used:
            state.active = next_active
        elif state.active is not None and state.active.invalidated:
            state.active = None
        state.invalidation_reason = ""

    def evaluate(self, streams: Mapping[Any, Any]) -> StrategyResult:
        """Evalúa series de las tres temporalidades en orden causal.

        ``streams`` puede mapear ``M1``/``M5``/``M15`` a listas de ``Candle``,
        a ``IndicatorSeries`` o a ``{"candles": ..., "quality": ...}``.
        Todas las velas usadas por la estrategia deben estar cerradas; las
        abiertas se conservan como puntos sin estado y generan bloqueos.
        """

        if not isinstance(streams, Mapping):
            raise TypeError("streams debe ser un mapping por temporalidad")
        context_stream, preparation_stream, trigger_stream, instrument = self._prepare_evaluation_streams(streams)
        auxiliary = self._ordered_auxiliary(context_stream, preparation_stream)
        triggers = self._ordered_triggers(trigger_stream)
        evaluations: list[Evaluation] = []
        signals: list[Signal] = []
        episodes: list[PreparationEpisode] = []
        state = _EvaluationState()
        for trigger_available, trigger_index, trigger_point in triggers:
            self._consume_auxiliary(
                state,
                auxiliary,
                trigger_available,
                context_stream,
                preparation_stream,
                instrument,
                evaluations,
                episodes,
            )
            self._process_trigger(
                state,
                trigger_stream,
                trigger_available,
                trigger_index,
                trigger_point,
                instrument,
                evaluations,
                signals,
            )
        return StrategyResult(tuple(evaluations), tuple(signals), tuple(episodes))


def math_is_finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


__all__ = [
    "ConditionResult",
    "ConditionState",
    "DecisionKind",
    "Evaluation",
    "PreparationEpisode",
    "Signal",
    "StrategyConfig",
    "StrategyResult",
    "TrendPullbackStrategy",
]
