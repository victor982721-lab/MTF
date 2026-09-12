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
    ) -> None:
        self.config = config if isinstance(config, IndicatorConfig) else IndicatorConfig.from_mapping(config)
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
        if self._previous_end is not None and start != self._previous_end:
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
    "IncrementalIndicatorEngine",
    "IndicatorConfig",
    "IndicatorPoint",
    "IndicatorSeries",
    "compute_indicators",
    "compute_indicators_incremental",
]
