"""Indicadores incrementales y por lote.

Implementación sin dependencias externas:

* EMA: inicialización con SMA de los primeros ``n`` cierres y luego
  ``EMA_t = alpha * close_t + (1-alpha) * EMA_(t-1)``,
  ``alpha = 2/(n+1)``.
* RSI y ATR: suavizado de Wilder, con media simple inicial de ``n`` muestras.
  RSI necesita ``n`` cambios, por lo que su primer valor aparece después de
  ``n+1`` cierres. ATR usa ``n`` true ranges y el primer rango de una serie sin
  previo es ``high-low``.

Los puntos sin datos, abiertos o con una discontinuidad no actualizan el
estado y fuerzan un nuevo calentamiento, evitando que una señal cruce un hueco
como si fuera una observación continua.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast, overload

from .canonical import fingerprint
from .historical_calendar import HistoricalQuoteCalendar
from .models import Candle, Timeframe, normalize_utc, parse_timeframe
from .quality import DataQuality, QualityFlag, QualityIssue


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} debe ser entero positivo")
    return value


@dataclass(frozen=True, slots=True)
class IndicatorConfig:
    ema_fast: int = 20
    ema_slow: int = 50
    rsi_period: int = 14
    atr_period: int = 14
    wilder: bool = True

    def __post_init__(self) -> None:
        for name in ("ema_fast", "ema_slow", "rsi_period", "atr_period"):
            object.__setattr__(self, name, _positive_int(getattr(self, name), name))
        if self.ema_fast >= self.ema_slow:
            raise ValueError("ema_fast debe ser menor que ema_slow")
        if not isinstance(self.wilder, bool) or not self.wilder:
            raise ValueError("Esta implementación exige el suavizado Wilder (wilder=true)")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> IndicatorConfig:
        if mapping is None:
            return cls()
        if not isinstance(mapping, Mapping):
            raise TypeError("indicators debe ser un mapping")
        aliases = {
            "ema_fast_period": "ema_fast",
            "ema_slow_period": "ema_slow",
            "rsi": "rsi_period",
            "atr": "atr_period",
        }
        values: dict[str, Any] = {}
        allowed = {"ema_fast", "ema_slow", "rsi_period", "atr_period", "wilder", *aliases}
        unknown = set(mapping) - allowed
        if unknown:
            raise ValueError(f"Claves desconocidas en indicators: {sorted(unknown)}")
        for key, value in mapping.items():
            canonical = aliases.get(key, key)
            if canonical in values:
                raise ValueError(f"Parámetro duplicado en indicators: {canonical}")
            values[canonical] = value
        return cls(**values)


@dataclass(frozen=True, slots=True)
class IndicatorPoint:
    """Valores de indicadores disponibles al cierre de una vela."""

    start: Any
    end: Any
    available_at: Any | None
    close: float | None
    ema_fast: float | None
    ema_slow: float | None
    rsi: float | None
    atr: float | None
    closed: bool = True
    quality: DataQuality = field(default_factory=DataQuality.good)
    candle_id: str | None = None
    index: int = 0

    @property
    def timestamp(self) -> Any:
        """Alias temporal usado por la estrategia y callers sencillos."""

        return self.end

    @property
    def effective_available_at(self) -> Any | None:
        return self.available_at if self.closed else None

    @property
    def ready(self) -> bool:
        return (
            self.closed
            and self.quality.valid
            and all(value is not None for value in (self.close, self.ema_fast, self.ema_slow, self.rsi, self.atr))
        )

    @property
    def indicators_ready(self) -> bool:
        return self.ready

    def value(self, name: str) -> float | None:
        aliases = {"ema20": "ema_fast", "ema50": "ema_slow", "rsi14": "rsi", "atr14": "atr"}
        return cast(float | None, getattr(self, aliases.get(name, name)))


@dataclass(frozen=True, slots=True)
class IndicatorSeries:
    """Serie alineada con las velas procesadas, sin recalcular al consultar."""

    timeframe: Timeframe
    instrument: str
    config: IndicatorConfig
    points: tuple[IndicatorPoint, ...]
    issues: tuple[QualityIssue, ...] = ()

    @property
    def timestamps(self) -> tuple[Any, ...]:
        return tuple(point.end for point in self.points)

    @property
    def closes(self) -> tuple[float | None, ...]:
        return tuple(point.close for point in self.points)

    @property
    def ema20(self) -> tuple[float | None, ...]:
        return tuple(point.ema_fast for point in self.points)

    @property
    def ema50(self) -> tuple[float | None, ...]:
        return tuple(point.ema_slow for point in self.points)

    @property
    def rsi14(self) -> tuple[float | None, ...]:
        return tuple(point.rsi for point in self.points)

    @property
    def atr14(self) -> tuple[float | None, ...]:
        return tuple(point.atr for point in self.points)

    @property
    def ready_from(self) -> int | None:
        for idx, point in enumerate(self.points):
            if point.ready:
                return idx
        return None

    def __len__(self) -> int:
        return len(self.points)

    def __iter__(self) -> Iterator[IndicatorPoint]:
        return iter(self.points)

    @overload
    def __getitem__(self, item: int) -> IndicatorPoint: ...

    @overload
    def __getitem__(self, item: slice) -> tuple[IndicatorPoint, ...]: ...

    def __getitem__(self, item: int | slice) -> IndicatorPoint | tuple[IndicatorPoint, ...]:
        return self.points[item]


def _snapshot_period(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} debe ser entero positivo")
    return value


def _snapshot_float(value: Any, name: str, *, allow_none: bool = True) -> float | None:
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


def _snapshot_values(values: Any, name: str, period: int) -> tuple[float, ...]:
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{name} debe ser una secuencia numérica")
    try:
        result = tuple(_snapshot_float(item, f"{name}[]", allow_none=False) for item in values)
    except TypeError as exc:
        raise TypeError(f"{name} debe ser una secuencia numérica") from exc
    if len(result) > period:
        raise ValueError(f"{name} excede el periodo {period}")
    return cast(tuple[float, ...], result)


def _snapshot_timestamp(value: Any, name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return normalize_utc(value, name)
    if not isinstance(value, str):
        raise TypeError(f"{name} debe ser datetime o timestamp ISO")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return normalize_utc(datetime.fromisoformat(text), name)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser timestamp ISO con zona horaria") from exc


@dataclass(frozen=True, slots=True)
class IndicatorEngineConfigIdentity:
    """Identidad reproducible de la configuración de un engine incremental.

    El estado de EMA/RSI/ATR no puede restaurarse en un engine con periodos o
    límites de almacenamiento distintos.  Esta identidad forma parte del
    snapshot público para que el consumidor no tenga que inspeccionar los
    atributos internos del engine para validar una restauración.
    """

    ema_fast: int
    ema_slow: int
    rsi_period: int
    atr_period: int
    wilder: bool
    max_points: int | None
    max_issues: int | None
    historical_calendar: HistoricalQuoteCalendar | None = None

    def __post_init__(self) -> None:
        for name in ("ema_fast", "ema_slow", "rsi_period", "atr_period"):
            object.__setattr__(self, name, _snapshot_period(getattr(self, name), name))
        if not isinstance(self.wilder, bool):
            raise TypeError("wilder debe ser booleano")
        for name in ("max_points", "max_issues"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _snapshot_period(value, name))
        if self.historical_calendar is not None and not isinstance(self.historical_calendar, HistoricalQuoteCalendar):
            raise TypeError("historical_calendar debe ser HistoricalQuoteCalendar")

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ema_fast": self.ema_fast,
            "ema_slow": self.ema_slow,
            "rsi_period": self.rsi_period,
            "atr_period": self.atr_period,
            "wilder": self.wilder,
            "max_points": self.max_points,
            "max_issues": self.max_issues,
        }
        if self.historical_calendar is not None:
            result["historical_calendar"] = self.historical_calendar.to_dict()
        return result

    @property
    def fingerprint(self) -> str:
        return fingerprint(self.to_dict())

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> IndicatorEngineConfigIdentity:
        if not isinstance(value, Mapping):
            raise TypeError("config_identity debe ser un mapping")
        allowed = {
            "ema_fast",
            "ema_slow",
            "rsi_period",
            "atr_period",
            "wilder",
            "max_points",
            "max_issues",
            "historical_calendar",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"Claves desconocidas en config_identity: {sorted(unknown)}")
        required = allowed - {"max_points", "max_issues", "historical_calendar"}
        missing = required - set(value)
        if missing:
            raise ValueError(f"Faltan campos en config_identity: {sorted(missing)}")
        return cls(
            ema_fast=value["ema_fast"],
            ema_slow=value["ema_slow"],
            rsi_period=value["rsi_period"],
            atr_period=value["atr_period"],
            wilder=value["wilder"],
            max_points=value.get("max_points"),
            max_issues=value.get("max_issues"),
            historical_calendar=HistoricalQuoteCalendar.from_mapping(value["historical_calendar"])
            if value.get("historical_calendar") is not None
            else None,
        )


@dataclass(frozen=True, slots=True)
class EMAStateSnapshot:
    """Estado serializable de una EMA, sin exponer el objeto mutable interno."""

    period: int
    values: tuple[float, ...]
    current: float | None

    def __post_init__(self) -> None:
        period = _snapshot_period(self.period, "period")
        object.__setattr__(self, "period", period)
        object.__setattr__(self, "values", _snapshot_values(self.values, "values", period))
        object.__setattr__(self, "current", _snapshot_float(self.current, "current"))

    def to_dict(self) -> dict[str, Any]:
        return {"values": list(self.values), "current": self.current, "period": self.period}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], name: str = "ema") -> EMAStateSnapshot:
        if not isinstance(value, Mapping):
            raise TypeError(f"{name} debe ser un mapping")
        required = {"values", "current", "period"}
        missing = required - set(value)
        if missing:
            raise ValueError(f"Faltan campos en {name}: {sorted(missing)}")
        return cls(value["period"], tuple(value["values"]), value["current"])


@dataclass(frozen=True, slots=True)
class RSIStateSnapshot:
    """Estado serializable del suavizado Wilder del RSI."""

    period: int
    previous_close: float | None
    gains: tuple[float, ...]
    losses: tuple[float, ...]
    average_gain: float | None
    average_loss: float | None

    def __post_init__(self) -> None:
        period = _snapshot_period(self.period, "period")
        object.__setattr__(self, "period", period)
        object.__setattr__(self, "previous_close", _snapshot_float(self.previous_close, "previous_close"))
        object.__setattr__(self, "gains", _snapshot_values(self.gains, "gains", period))
        object.__setattr__(self, "losses", _snapshot_values(self.losses, "losses", period))
        object.__setattr__(self, "average_gain", _snapshot_float(self.average_gain, "average_gain"))
        object.__setattr__(self, "average_loss", _snapshot_float(self.average_loss, "average_loss"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "previous_close": self.previous_close,
            "gains": list(self.gains),
            "losses": list(self.losses),
            "average_gain": self.average_gain,
            "average_loss": self.average_loss,
            "period": self.period,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> RSIStateSnapshot:
        if not isinstance(value, Mapping):
            raise TypeError("rsi debe ser un mapping")
        required = {"previous_close", "gains", "losses", "average_gain", "average_loss", "period"}
        missing = required - set(value)
        if missing:
            raise ValueError(f"Faltan campos en rsi: {sorted(missing)}")
        return cls(
            value["period"],
            value["previous_close"],
            tuple(value["gains"]),
            tuple(value["losses"]),
            value["average_gain"],
            value["average_loss"],
        )


@dataclass(frozen=True, slots=True)
class ATRStateSnapshot:
    """Estado serializable del suavizado Wilder del ATR."""

    period: int
    previous_close: float | None
    true_ranges: tuple[float, ...]
    current: float | None

    def __post_init__(self) -> None:
        period = _snapshot_period(self.period, "period")
        object.__setattr__(self, "period", period)
        object.__setattr__(self, "previous_close", _snapshot_float(self.previous_close, "previous_close"))
        object.__setattr__(self, "true_ranges", _snapshot_values(self.true_ranges, "true_ranges", period))
        object.__setattr__(self, "current", _snapshot_float(self.current, "current"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "previous_close": self.previous_close,
            "true_ranges": list(self.true_ranges),
            "current": self.current,
            "period": self.period,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ATRStateSnapshot:
        if not isinstance(value, Mapping):
            raise TypeError("atr debe ser un mapping")
        required = {"previous_close", "true_ranges", "current", "period"}
        missing = required - set(value)
        if missing:
            raise ValueError(f"Faltan campos en atr: {sorted(missing)}")
        return cls(value["period"], value["previous_close"], tuple(value["true_ranges"]), value["current"])


@dataclass(frozen=True, slots=True)
class IndicatorEngineSnapshot:
    """Checkpoint tipado del estado incremental EMA/RSI/ATR.

    ``to_dict`` conserva la forma histórica de los bloques ``ema_fast``,
    ``ema_slow``, ``rsi`` y ``atr`` para que los checkpoints del runtime
    sigan siendo JSON compatibles. La identidad nueva impide restaurar un
    estado con una configuración diferente.
    """

    schema_version: int
    config_identity: IndicatorEngineConfigIdentity
    previous_end: datetime | None
    timeframe: Timeframe | None
    instrument: str
    index: int
    ema_fast: EMAStateSnapshot
    ema_slow: EMAStateSnapshot
    rsi: RSIStateSnapshot
    atr: ATRStateSnapshot

    VERSION = 1

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or self.schema_version != self.VERSION:
            raise ValueError(f"schema_version de engine no soportada: {self.schema_version!r}")
        if not isinstance(self.config_identity, IndicatorEngineConfigIdentity):
            raise TypeError("config_identity debe ser IndicatorEngineConfigIdentity")
        object.__setattr__(self, "previous_end", _snapshot_timestamp(self.previous_end, "previous_end"))
        if self.timeframe is not None and not isinstance(self.timeframe, Timeframe):
            object.__setattr__(self, "timeframe", parse_timeframe(self.timeframe))
        instrument = str(self.instrument).strip()
        if not instrument:
            raise ValueError("instrument no puede estar vacío")
        object.__setattr__(self, "instrument", instrument)
        if isinstance(self.index, bool) or not isinstance(self.index, int) or self.index < 0:
            raise ValueError("index debe ser entero no negativo")
        if not isinstance(self.ema_fast, EMAStateSnapshot):
            raise TypeError("ema_fast debe ser EMAStateSnapshot")
        if not isinstance(self.ema_slow, EMAStateSnapshot):
            raise TypeError("ema_slow debe ser EMAStateSnapshot")
        if not isinstance(self.rsi, RSIStateSnapshot):
            raise TypeError("rsi debe ser RSIStateSnapshot")
        if not isinstance(self.atr, ATRStateSnapshot):
            raise TypeError("atr debe ser ATRStateSnapshot")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "config_identity": self.config_identity.to_dict(),
            "config_hash": self.config_identity.fingerprint,
            "previous_end": self.previous_end.isoformat().replace("+00:00", "Z")
            if self.previous_end is not None
            else None,
            "timeframe": self.timeframe.name if self.timeframe is not None else None,
            "instrument": self.instrument,
            "index": self.index,
            "ema_fast": self.ema_fast.to_dict(),
            "ema_slow": self.ema_slow.to_dict(),
            "rsi": self.rsi.to_dict(),
            "atr": self.atr.to_dict(),
        }

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        default_identity: IndicatorEngineConfigIdentity | None = None,
        allow_legacy: bool = False,
    ) -> IndicatorEngineSnapshot:
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("snapshot de engine debe ser un mapping")
        has_identity = "config_identity" in value
        raw_identity = value.get("config_identity")
        if has_identity:
            if not isinstance(raw_identity, Mapping):
                raise TypeError("config_identity debe ser un mapping")
            identity = IndicatorEngineConfigIdentity.from_mapping(raw_identity)
        elif allow_legacy and default_identity is not None:
            identity = default_identity
        else:
            raise ValueError("snapshot de engine sin config_identity; sólo se acepta como checkpoint histórico")
        version = value.get("schema_version")
        if version is None:
            if not allow_legacy:
                raise ValueError("snapshot de engine sin schema_version")
            version = cls.VERSION
        if "config_hash" in value and has_identity and str(value["config_hash"]) != identity.fingerprint:
            raise ValueError("config_hash de snapshot de engine no coincide con config_identity")
        required = {"previous_end", "timeframe", "instrument", "index", "ema_fast", "ema_slow", "rsi", "atr"}
        missing = required - set(value)
        if missing:
            raise ValueError(f"Faltan campos en snapshot de engine: {sorted(missing)}")
        return cls(
            schema_version=int(version),
            config_identity=identity,
            previous_end=_snapshot_timestamp(value["previous_end"], "previous_end"),
            timeframe=parse_timeframe(value["timeframe"]) if value["timeframe"] else None,
            instrument=str(value["instrument"]),
            index=int(value["index"]),
            ema_fast=EMAStateSnapshot.from_mapping(value["ema_fast"], "ema_fast"),
            ema_slow=EMAStateSnapshot.from_mapping(value["ema_slow"], "ema_slow"),
            rsi=RSIStateSnapshot.from_mapping(value["rsi"]),
            atr=ATRStateSnapshot.from_mapping(value["atr"]),
        )


class _EMAState:
    def __init__(self, period: int) -> None:
        self.period = period
        self.values: deque[float] = deque(maxlen=period)
        self.current: float | None = None

    def reset(self) -> None:
        self.values.clear()
        self.current = None

    def update(self, value: float) -> float | None:
        if self.current is None:
            self.values.append(value)
            if len(self.values) < self.period:
                return None
            self.current = sum(self.values) / self.period
            return self.current
        alpha = 2.0 / (self.period + 1.0)
        self.current = alpha * value + (1.0 - alpha) * self.current
        return self.current


class _WilderRSIState:
    def __init__(self, period: int) -> None:
        self.period = period
        self.previous_close: float | None = None
        self.gains: deque[float] = deque(maxlen=period)
        self.losses: deque[float] = deque(maxlen=period)
        self.average_gain: float | None = None
        self.average_loss: float | None = None

    def reset(self) -> None:
        self.previous_close = None
        self.gains.clear()
        self.losses.clear()
        self.average_gain = None
        self.average_loss = None

    def update(self, close: float) -> float | None:
        if self.previous_close is None:
            self.previous_close = close
            return None
        change = close - self.previous_close
        self.previous_close = close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        if self.average_gain is None:
            self.gains.append(gain)
            self.losses.append(loss)
            if len(self.gains) < self.period:
                return None
            self.average_gain = sum(self.gains) / self.period
            self.average_loss = sum(self.losses) / self.period
        else:
            assert self.average_gain is not None and self.average_loss is not None
            self.average_gain = (self.average_gain * (self.period - 1) + gain) / self.period
            self.average_loss = (self.average_loss * (self.period - 1) + loss) / self.period
        assert self.average_gain is not None and self.average_loss is not None
        if self.average_loss == 0.0:
            return 50.0 if self.average_gain == 0.0 else 100.0
        if self.average_gain == 0.0:
            return 0.0
        rs = self.average_gain / self.average_loss
        return 100.0 - (100.0 / (1.0 + rs))


class _WilderATRState:
    def __init__(self, period: int) -> None:
        self.period = period
        self.previous_close: float | None = None
        self.true_ranges: deque[float] = deque(maxlen=period)
        self.current: float | None = None

    def reset(self) -> None:
        self.previous_close = None
        self.true_ranges.clear()
        self.current = None

    def update(self, high: float, low: float, close: float) -> float | None:
        if self.previous_close is None:
            true_range = high - low
        else:
            true_range = max(high - low, abs(high - self.previous_close), abs(low - self.previous_close))
        self.previous_close = close
        if self.current is None:
            self.true_ranges.append(true_range)
            if len(self.true_ranges) < self.period:
                return None
            self.current = sum(self.true_ranges) / self.period
            return self.current
        self.current = (self.current * (self.period - 1) + true_range) / self.period
        return self.current


def _coerce_timestamp(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        return normalize_utc(value, name)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return normalize_utc(datetime.fromisoformat(text), name)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser timestamp ISO con zona horaria") from exc


def _bar_fields(bar: Any) -> tuple[Any, Any, Any, Any, Any, Any, bool, DataQuality, str | None, Timeframe, str]:
    """Obtiene campos de Candle y acepta mappings de proveedores ya normalizados."""
    if isinstance(bar, Candle):
        return (
            bar.start,
            bar.end,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.closed,
            bar.quality,
            bar.candle_id,
            bar.normalized_timeframe,
            bar.instrument,
        )
    if isinstance(bar, Mapping):
        row = bar
        start = row.get("start", row.get("interval_start", row.get("start_ts")))
        end = row.get("end", row.get("interval_end", row.get("end_ts")))
        required = {"open", "high", "low", "close"}
        missing = [key for key in required if key not in row]
        if start is None:
            missing.append("start")
        if end is None:
            missing.append("end")
        if missing:
            raise ValueError(f"Faltan campos de vela: {sorted(set(missing))}")
        timeframe = parse_timeframe(row.get("timeframe", row.get("resolution", "M1")))
        quality = row.get("quality", DataQuality.good())
        if not isinstance(quality, DataQuality):
            # Un estado textual de proveedor se conserva como bloqueo cuando
            # no es valid/synthetic, evitando tratarlo como calidad buena.
            text = str(quality).lower()
            quality = (
                DataQuality.good()
                if text in {"valid", "synthetic", "validated_local", "public_provider_closed"}
                else DataQuality.from_flag(QualityFlag.INVALID, text)
            )
        return (
            start,
            end,
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            bool(row.get("closed", row.get("is_closed", True))),
            quality,
            str(row.get("candle_id", row.get("data_id")))
            if row.get("candle_id", row.get("data_id")) is not None
            else None,
            timeframe,
            str(row.get("instrument", row.get("symbol", "unknown"))),
        )
    # Dataclasses de proveedores (por ejemplo data.Bar) pueden adaptarse sin
    # importar el paquete de datos en el núcleo.
    if all(hasattr(bar, name) for name in ("interval_start", "interval_end", "open", "high", "low", "close")):
        tf = parse_timeframe(getattr(bar, "resolution", getattr(bar, "resolution_seconds", "M1")))
        quality = getattr(bar, "quality", DataQuality.good())
        if not isinstance(quality, DataQuality):
            quality = DataQuality.good(synthetic=bool(getattr(bar, "synthetic", False)))
        return (
            bar.interval_start,
            bar.interval_end,
            float(bar.open),
            float(bar.high),
            float(bar.low),
            float(bar.close),
            bool(getattr(bar, "closed", True)),
            quality,
            str(getattr(bar, "data_id", getattr(bar, "candle_id", ""))) or None,
            tf,
            str(getattr(bar, "instrument", "unknown")),
        )
    raise TypeError("compute_indicators requiere Candle o mapping de vela")


class IncrementalIndicatorEngine:
    """Estado incremental de EMA/RSI/ATR para una sola serie temporal."""

    def __init__(
        self,
        config: IndicatorConfig | Mapping[str, Any] | None = None,
        *,
        max_points: int | None = None,
        max_issues: int | None = None,
        historical_calendar: HistoricalQuoteCalendar | None = None,
    ) -> None:
        self.config = config if isinstance(config, IndicatorConfig) else IndicatorConfig.from_mapping(config)
        if historical_calendar is not None and not isinstance(historical_calendar, HistoricalQuoteCalendar):
            raise TypeError("historical_calendar debe ser HistoricalQuoteCalendar")
        self.historical_calendar = historical_calendar
        if max_points is not None and (isinstance(max_points, bool) or int(max_points) <= 0):
            raise ValueError("max_points debe ser entero positivo")
        if max_issues is not None and (isinstance(max_issues, bool) or int(max_issues) <= 0):
            raise ValueError("max_issues debe ser entero positivo")
        self.max_points = int(max_points) if max_points is not None else None
        self.max_issues = (
            int(max_issues) if max_issues is not None else (self.max_points if self.max_points is not None else None)
        )
        self._ema_fast = _EMAState(self.config.ema_fast)
        self._ema_slow = _EMAState(self.config.ema_slow)
        self._rsi = _WilderRSIState(self.config.rsi_period)
        self._atr = _WilderATRState(self.config.atr_period)
        self._previous_end: Any | None = None
        self._timeframe: Timeframe | None = None
        self._instrument: str = "unknown"
        self._index = 0
        self._points: deque[IndicatorPoint] | list[IndicatorPoint] = (
            deque(maxlen=self.max_points) if self.max_points is not None else []
        )
        self._issues: deque[QualityIssue] | list[QualityIssue] = (
            deque(maxlen=self.max_issues) if self.max_issues is not None else []
        )

    @property
    def config_identity(self) -> IndicatorEngineConfigIdentity:
        """Identidad tipada de la configuración efectiva del engine."""

        return IndicatorEngineConfigIdentity(
            ema_fast=self.config.ema_fast,
            ema_slow=self.config.ema_slow,
            rsi_period=self.config.rsi_period,
            atr_period=self.config.atr_period,
            wilder=self.config.wilder,
            max_points=self.max_points,
            max_issues=self.max_issues,
            historical_calendar=self.historical_calendar,
        )

    @property
    def config_hash(self) -> str:
        """Huella estable de :attr:`config_identity` para registros."""

        return self.config_identity.fingerprint

    @property
    def points(self) -> tuple[IndicatorPoint, ...]:
        return tuple(self._points)

    @property
    def series(self) -> IndicatorSeries:
        return IndicatorSeries(
            self._timeframe or Timeframe("M1", 60),
            self._instrument,
            self.config,
            tuple(self._points),
            tuple(self._issues),
        )

    def reset(self) -> None:
        self._ema_fast.reset()
        self._ema_slow.reset()
        self._rsi.reset()
        self._atr.reset()
        self._previous_end = None

    def snapshot(self) -> IndicatorEngineSnapshot:
        """Captura el estado mutable mediante una API pública y tipada."""

        return IndicatorEngineSnapshot(
            schema_version=IndicatorEngineSnapshot.VERSION,
            config_identity=self.config_identity,
            previous_end=(
                normalize_utc(self._previous_end, "previous_end") if isinstance(self._previous_end, datetime) else None
            ),
            timeframe=self._timeframe,
            instrument=self._instrument,
            index=self._index,
            ema_fast=EMAStateSnapshot(
                self._ema_fast.period,
                tuple(self._ema_fast.values),
                self._ema_fast.current,
            ),
            ema_slow=EMAStateSnapshot(
                self._ema_slow.period,
                tuple(self._ema_slow.values),
                self._ema_slow.current,
            ),
            rsi=RSIStateSnapshot(
                self._rsi.period,
                self._rsi.previous_close,
                tuple(self._rsi.gains),
                tuple(self._rsi.losses),
                self._rsi.average_gain,
                self._rsi.average_loss,
            ),
            atr=ATRStateSnapshot(
                self._atr.period,
                self._atr.previous_close,
                tuple(self._atr.true_ranges),
                self._atr.current,
            ),
        )

    def snapshot_json(self) -> str:
        """Codifica el snapshot tipado sin depender de pickle."""

        import json

        return json.dumps(self.snapshot().to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def restore(
        self,
        snapshot: IndicatorEngineSnapshot | Mapping[str, Any],
        *,
        allow_legacy: bool = False,
    ) -> None:
        """Restaura un snapshot después de validar identidad y contenido.

        ``allow_legacy`` sólo existe para lectores del checkpoint v1 del
        runtime, cuyo bloque histórico no llevaba identidad propia. Los
        snapshots nuevos siempre exigen ``config_identity``.
        """

        typed = (
            snapshot
            if isinstance(snapshot, IndicatorEngineSnapshot)
            else IndicatorEngineSnapshot.from_mapping(
                snapshot,
                default_identity=self.config_identity,
                allow_legacy=allow_legacy,
            )
        )
        expected = self.config_identity
        if typed.config_identity != expected:
            raise ValueError("config_identity del snapshot de engine no coincide con la configuración efectiva")
        if typed.ema_fast.period != self._ema_fast.period or typed.ema_slow.period != self._ema_slow.period:
            raise ValueError("periodos EMA del snapshot no coinciden")
        if typed.rsi.period != self._rsi.period or typed.atr.period != self._atr.period:
            raise ValueError("periodos RSI/ATR del snapshot no coinciden")

        # Todas las conversiones y validaciones ocurren antes de tocar el
        # objeto vivo; un snapshot corrupto no deja una restauración parcial.
        from collections import deque as _deque

        ema_fast_values = _deque(typed.ema_fast.values, maxlen=self._ema_fast.period)
        ema_slow_values = _deque(typed.ema_slow.values, maxlen=self._ema_slow.period)
        rsi_gains = _deque(typed.rsi.gains, maxlen=self._rsi.period)
        rsi_losses = _deque(typed.rsi.losses, maxlen=self._rsi.period)
        atr_ranges = _deque(typed.atr.true_ranges, maxlen=self._atr.period)

        self._previous_end = typed.previous_end
        self._timeframe = typed.timeframe
        self._instrument = typed.instrument
        self._index = typed.index
        self._ema_fast.values = ema_fast_values
        self._ema_fast.current = typed.ema_fast.current
        self._ema_slow.values = ema_slow_values
        self._ema_slow.current = typed.ema_slow.current
        self._rsi.previous_close = typed.rsi.previous_close
        self._rsi.gains = rsi_gains
        self._rsi.losses = rsi_losses
        self._rsi.average_gain = typed.rsi.average_gain
        self._rsi.average_loss = typed.rsi.average_loss
        self._atr.previous_close = typed.atr.previous_close
        self._atr.true_ranges = atr_ranges
        self._atr.current = typed.atr.current

    def _unknown_point(
        self,
        *,
        start: Any,
        end: Any,
        available_at: Any | None,
        close: float | None,
        closed: bool,
        quality: DataQuality,
        candle_id: str | None,
    ) -> IndicatorPoint:
        return IndicatorPoint(
            start, end, available_at, close, None, None, None, None, closed, quality, candle_id, self._index
        )

    def update(self, bar: Candle | Mapping[str, Any]) -> IndicatorPoint:
        fields = _bar_fields(bar)
        start, end, _open, high, low, close, closed, quality, candle_id, timeframe, instrument = fields
        # _bar_fields's tuple intentionally keeps names explicit; assert finite
        # values here so mappings get the same protections as Candle.
        if not all(math.isfinite(float(value)) for value in (high, low, close)):
            quality = quality.with_flags(QualityFlag.INVALID, reason="non_finite_ohlc")
        if self._timeframe is None:
            self._timeframe = timeframe
            self._instrument = instrument
        elif timeframe != self._timeframe:
            raise ValueError("No se pueden mezclar temporalidades en un engine incremental")
        if instrument != self._instrument and self._instrument != "unknown":
            raise ValueError("No se pueden mezclar instrumentos en un engine incremental")
        if isinstance(bar, Mapping):
            available_at = bar.get("available_at", bar.get("available_ts"))
        else:
            available_at = getattr(bar, "available_at", None)
        if available_at is None and closed:
            available_at = end

        # Una barra no cerrada se devuelve para visualización, pero jamás
        # contamina el estado de indicadores de velas cerradas.
        if not closed or not quality.valid:
            if not quality.valid:
                self._issues.append(
                    QualityIssue(
                        "quality_blocked",
                        ",".join(sorted(flag.value for flag in quality.flags)),
                        record_id=candle_id,
                        timestamp=end,
                    )
                )
            point = self._unknown_point(
                start=start,
                end=end,
                available_at=available_at,
                close=float(close) if close is not None else None,
                closed=closed,
                quality=quality,
                candle_id=candle_id,
            )
            self._points.append(point)
            self._index += 1
            self.reset()
            self._previous_end = end if closed else self._previous_end
            return point

        # La continuidad se comprueba antes de alimentar el siguiente cálculo.
        if self._previous_end is not None and start != self._previous_end and not self._scheduled_gap(start):
            quality = quality.with_flags(QualityFlag.GAP, reason=f"intervalo_no_contiguo:{self._previous_end}->{start}")
            issue = QualityIssue(
                "gap", "Intervalos no contiguos; se reinicia calentamiento", record_id=candle_id, timestamp=start
            )
            self._issues.append(issue)
            point = self._unknown_point(
                start=start,
                end=end,
                available_at=available_at,
                close=float(close),
                closed=closed,
                quality=quality,
                candle_id=candle_id,
            )
            self._points.append(point)
            self._index += 1
            self.reset()
            self._previous_end = end
            return point

        close_value = float(close)
        high_value = float(high)
        low_value = float(low)
        ema_fast = self._ema_fast.update(close_value)
        ema_slow = self._ema_slow.update(close_value)
        rsi = self._rsi.update(close_value)
        atr = self._atr.update(high_value, low_value, close_value)
        point = IndicatorPoint(
            start, end, available_at, close_value, ema_fast, ema_slow, rsi, atr, closed, quality, candle_id, self._index
        )
        self._points.append(point)
        self._index += 1
        self._previous_end = end
        return point

    def _scheduled_gap(self, start: datetime) -> bool:
        if self._previous_end is None or self.historical_calendar is None or start < self._previous_end:
            return False
        if not self.historical_calendar.covers_closed(self._previous_end, start):
            return False
        self._issues.append(
            QualityIssue(
                "modeled_scheduled_closure",
                f"Continuidad numérica bajo calendario modelado {self.historical_calendar.calendar_hash}; no cuenta verificada",
                timestamp=start,
            )
        )
        return True


def compute_indicators(
    candles: Iterable[Candle | Mapping[str, Any]],
    config: IndicatorConfig | Mapping[str, Any] | None = None,
) -> IndicatorSeries:
    """Calcula por lote usando exactamente el mismo estado incremental."""

    engine = IncrementalIndicatorEngine(config)
    for candle in candles:
        engine.update(candle)
    return engine.series


def compute_indicators_incremental(
    candles: Iterable[Candle | Mapping[str, Any]],
    config: IndicatorConfig | Mapping[str, Any] | None = None,
) -> IndicatorSeries:
    """Alias explícito para pruebas de equivalencia lote/streaming."""

    return compute_indicators(candles, config)


__all__ = [
    "ATRStateSnapshot",
    "EMAStateSnapshot",
    "IncrementalIndicatorEngine",
    "IndicatorConfig",
    "IndicatorEngineConfigIdentity",
    "IndicatorEngineSnapshot",
    "IndicatorPoint",
    "RSIStateSnapshot",
    "IndicatorSeries",
    "compute_indicators",
    "compute_indicators_incremental",
]
