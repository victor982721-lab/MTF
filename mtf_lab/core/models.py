"""Modelos de entrada y barras normalizadas.

El núcleo conserva por separado el tiempo del evento, el tiempo de recepción y
el momento de disponibilidad para el detector.  Todos los ``datetime``
aceptados deben ser conscientes de zona horaria y se almacenan como UTC.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any

from .canonical import canonical_json
from .quality import DataQuality, QualityFlag


def normalize_utc(value: datetime, field_name: str = "timestamp") -> datetime:
    """Valida y normaliza un timestamp a UTC.

    Los timestamps ingenuos se rechazan de forma intencional: asumir una zona
    local aquí produciría desplazamientos silenciosos en los límites de velas.
    """

    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} debe ser datetime con zona horaria")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} debe incluir zona horaria")
    return value.astimezone(UTC)


class OperationMode(str, Enum):  # noqa: UP042 - preserve the public enum MRO
    """Modo de procedencia que debe aparecer en todas las salidas."""

    LIVE = "LIVE"
    REPLAY = "REPLAY"
    SYNTHETIC = "SYNTHETIC"


# Alias corto para callers que prefieren ``Mode``.
Mode = OperationMode


class PriceBase(str, Enum):  # noqa: UP042 - preserve the public enum MRO
    """Base de precio, sin conversiones implícitas entre quote/trade/native."""

    TRADED = "traded"
    BID = "bid"
    ASK = "ask"
    MID = "mid"
    NATIVE = "native"
    # Alias semántico para integraciones que nombran la base por proveedor.
    # Comparte el valor ``native`` para que no exista una segunda base que
    # pueda mezclarse accidentalmente con la primera.
    PROVIDER_NATIVE = "native"


_PRICE_BASE_ALIASES = {
    "trade": PriceBase.TRADED,
    "close": PriceBase.TRADED,
    "provider_native": PriceBase.NATIVE,
    "provider-native": PriceBase.NATIVE,
}


def _coerce_price_base(value: PriceBase | str) -> PriceBase:
    if isinstance(value, PriceBase):
        return value
    if not isinstance(value, str):
        raise TypeError("price_base debe ser PriceBase o texto")
    raw = value.strip().lower()
    if raw in _PRICE_BASE_ALIASES:
        return _PRICE_BASE_ALIASES[raw]
    return PriceBase(raw)


def normalize_price_base(value: PriceBase | str) -> PriceBase:
    """Normaliza una base explícita; ``close``/``trade`` son aliases históricos.

    ``native`` y ``provider_native`` convergen a la misma identidad enum. No
    hay conversión entre una base nativa y traded/quote.
    """

    return _coerce_price_base(value)


class EventKind(str, Enum):  # noqa: UP042 - preserve the public enum MRO
    TRADE = "trade"
    OHLC = "ohlc"
    QUOTE = "quote"


@dataclass(frozen=True, slots=True)
class Timeframe:
    """Temporalidad de duración fija en segundos."""

    name: str
    seconds: int

    def __post_init__(self) -> None:
        normalized = str(self.name).strip().upper()
        if not normalized:
            raise ValueError("La temporalidad no puede estar vacía")
        if self.seconds <= 0:
            raise ValueError("La duración de una temporalidad debe ser positiva")
        object.__setattr__(self, "name", normalized)

    @property
    def delta(self) -> timedelta:
        return timedelta(seconds=self.seconds)

    def __str__(self) -> str:
        return self.name


_TIMEFRAME_SECONDS: dict[str, int] = {
    "M1": 60,
    "M5": 5 * 60,
    "M15": 15 * 60,
    "M30": 30 * 60,
    "H1": 60 * 60,
    "H4": 4 * 60 * 60,
    "D1": 24 * 60 * 60,
}


def parse_timeframe(value: Timeframe | str | int) -> Timeframe:
    """Convierte ``M1``, ``1m`` o segundos a un :class:`Timeframe`.

    Se aceptan sólo unidades enteras y positivas; no se redondean duraciones
    fraccionarias.  Los nombres de temporalidad desconocidos se rechazan.
    """

    if isinstance(value, Timeframe):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if value <= 0:
            raise ValueError("Los segundos deben ser positivos")
        return Timeframe(f"S{value}", value)
    text = str(value).strip().upper()
    if text in _TIMEFRAME_SECONDS:
        return Timeframe(text, _TIMEFRAME_SECONDS[text])
    # Forma explícita 1M/15M, 1H y 1D para archivos de configuración.
    if text[:-1].isdigit() and text[-1:] in {"S", "M", "H", "D"}:
        amount = int(text[:-1])
        unit = text[-1]
        multiplier = {"S": 1, "M": 60, "H": 3600, "D": 86400}[unit]
        seconds = amount * multiplier
        if amount <= 0:
            raise ValueError("La temporalidad debe ser positiva")
        canonical = f"{unit}{amount}" if unit == "S" else f"{unit}{amount}"
        return Timeframe(canonical, seconds)
    raise ValueError(f"Temporalidad no soportada: {value!r}")


def _finite(value: float | int | None, field_name: str, *, allow_none: bool = True) -> float | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool):
        raise TypeError(f"{field_name} debe ser numérico")
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field_name} debe ser numérico") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field_name} debe ser finito")
    return result


def _detach(value: Any) -> Any:
    """Copy nested mappings without trying to pickle an existing proxy."""

    if isinstance(value, Mapping):
        return {key: _detach(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_detach(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_detach(item) for item in value)
    if isinstance(value, set):
        return {_detach(item) for item in value}
    return deepcopy(value)


def _mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise TypeError("metadata debe ser un mapping")
    # Copia profunda defensiva: una dataclass frozen no protege un dict/lista
    # anidado que el caller conserve y modifique después de construir el
    # registro. Se mantiene la forma de listas para compatibilidad de payloads.
    return MappingProxyType(_detach(value))


def _event_identity_payload(
    *,
    instrument: str,
    source: str,
    event_kind: EventKind,
    event_time: datetime,
    received_at: datetime | None,
    available_at: datetime | None,
    sequence: str | int | None,
    source_event_id: str | None,
    price: float | None,
    quantity: float | None,
    bid: float | None,
    ask: float | None,
) -> bytes:
    payload = {
        "instrument": instrument,
        "source": source,
        "event_kind": event_kind.value,
        "event_time": event_time.isoformat(),
        "received_at": received_at.isoformat() if received_at else None,
        "available_at": available_at.isoformat() if available_at else None,
        # sequence/source_event_id hacen que la identidad no dependa sólo del
        # par precio-timestamp; si no existen, el resto del payload igualmente
        # evita colisiones simples y el proveedor puede aportar uno explícito.
        "sequence": sequence,
        "source_event_id": source_event_id,
        "price": price,
        "quantity": quantity,
        "bid": bid,
        "ask": ask,
    }
    return canonical_json(payload).encode("utf-8")


def _normalize_candle_identity(
    instrument: str,
    timeframe: Timeframe | str,
    start: datetime,
    end: datetime,
) -> tuple[str, Timeframe, datetime, datetime]:
    normalized_instrument = str(instrument).strip()
    if not normalized_instrument:
        raise ValueError("instrument no puede estar vacío")
    normalized_timeframe = parse_timeframe(timeframe)
    normalized_start = normalize_utc(start, "start")
    normalized_end = normalize_utc(end, "end")
    if normalized_end <= normalized_start:
        raise ValueError("end debe ser posterior a start")
    if normalized_end - normalized_start != normalized_timeframe.delta:
        raise ValueError("el intervalo de la vela no coincide con timeframe")
    return normalized_instrument, normalized_timeframe, normalized_start, normalized_end


def _normalize_candle_measurements(
    open_value: float,
    high_value: float,
    low_value: float,
    close_value: float,
    volume_value: float,
    event_count: int,
) -> tuple[dict[str, float], float, int]:
    values: dict[str, float] = {}
    for name, value in (
        ("open", open_value),
        ("high", high_value),
        ("low", low_value),
        ("close", close_value),
    ):
        normalized = _finite(value, name, allow_none=False)
        assert normalized is not None
        values[name] = normalized
    if values["high"] < max(values["open"], values["close"], values["low"]):
        raise ValueError("high debe ser mayor o igual que OHLC")
    if values["low"] > min(values["open"], values["close"], values["high"]):
        raise ValueError("low debe ser menor o igual que OHLC")
    normalized_volume = _finite(volume_value, "volume", allow_none=False)
    assert normalized_volume is not None
    if normalized_volume < 0:
        raise ValueError("volume no puede ser negativo")
    if isinstance(event_count, bool) or int(event_count) < 0:
        raise ValueError("event_count debe ser entero no negativo")
    return values, normalized_volume, int(event_count)


def _normalize_candle_availability(
    closed: bool,
    end: datetime,
    available_at: datetime | None,
    received_at: datetime | None,
) -> tuple[datetime | None, datetime | None]:
    normalized_available = normalize_utc(available_at, "available_at") if available_at else None
    normalized_received = normalize_utc(received_at, "received_at") if received_at else None
    if closed and normalized_available is None:
        normalized_available = end
    if normalized_available is not None and closed and normalized_available < end:
        # Una vela cerrada no puede estar disponible antes del cierre de su
        # intervalo; eventos tardíos pueden moverla después, nunca antes.
        raise ValueError("available_at no puede preceder al cierre de una vela cerrada")
    return normalized_available, normalized_received


@dataclass(frozen=True, slots=True)
class MarketEvent:
    """Evento de mercado normalizado.

    ``price`` sólo es precio negociado.  Para eventos de cotización se deben
    utilizar ``bid``/``ask`` y elegir explícitamente ``price_base``.  El
    núcleo jamás sustituye silenciosamente una base por otra.
    """

    instrument: str
    event_time: datetime
    price: float | None = None
    quantity: float | None = None
    bid: float | None = None
    ask: float | None = None
    received_at: datetime | None = None
    available_at: datetime | None = None
    source: str = "unknown"
    mode: OperationMode = OperationMode.REPLAY
    price_base: PriceBase = PriceBase.TRADED
    event_kind: EventKind = EventKind.TRADE
    sequence: str | int | None = None
    source_event_id: str | None = None
    event_id: str | None = None
    quality: DataQuality = field(default_factory=DataQuality.good)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        instrument = str(self.instrument).strip()
        if not instrument:
            raise ValueError("instrument no puede estar vacío")
        object.__setattr__(self, "instrument", instrument)
        event_time = normalize_utc(self.event_time, "event_time")
        received_at = normalize_utc(self.received_at, "received_at") if self.received_at else None
        available_at = normalize_utc(self.available_at, "available_at") if self.available_at else None
        if received_at is not None and received_at < event_time:
            raise ValueError("received_at no puede preceder a event_time")
        if available_at is not None and available_at < event_time:
            raise ValueError("available_at no puede preceder a event_time")
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "price", _finite(self.price, "price"))
        object.__setattr__(self, "quantity", _finite(self.quantity, "quantity"))
        object.__setattr__(self, "bid", _finite(self.bid, "bid"))
        object.__setattr__(self, "ask", _finite(self.ask, "ask"))
        if self.quantity is not None and self.quantity < 0:
            raise ValueError("quantity no puede ser negativa")
        object.__setattr__(self, "source", str(self.source).strip() or "unknown")
        if not isinstance(self.mode, OperationMode):
            object.__setattr__(self, "mode", OperationMode(str(self.mode).upper()))
        object.__setattr__(self, "price_base", _coerce_price_base(self.price_base))
        if not isinstance(self.event_kind, EventKind):
            object.__setattr__(self, "event_kind", EventKind(str(self.event_kind).lower()))
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        event_id = self.event_id
        if event_id is None:
            event_id = (
                "evt_"
                + hashlib.sha256(
                    _event_identity_payload(
                        instrument=instrument,
                        source=self.source,
                        event_kind=self.event_kind,
                        event_time=event_time,
                        received_at=received_at,
                        available_at=available_at,
                        sequence=self.sequence,
                        source_event_id=self.source_event_id,
                        price=self.price,
                        quantity=self.quantity,
                        bid=self.bid,
                        ask=self.ask,
                    )
                ).hexdigest()[:32]
            )
        event_id = str(event_id).strip()
        if not event_id:
            raise ValueError("event_id no puede estar vacío")
        object.__setattr__(self, "event_id", event_id)

    @property
    def selected_price(self) -> float | None:
        """Precio que corresponde exactamente a ``price_base``."""

        if self.price_base is PriceBase.TRADED:
            return self.price
        if self.price_base is PriceBase.BID:
            return self.bid
        if self.price_base is PriceBase.ASK:
            return self.ask
        if self.price_base is PriceBase.NATIVE:
            # Native es una base explícita, no un alias de MID. Para eventos
            # puntuales el campo ``price`` contiene el valor ya seleccionado
            # por el adaptador; las trendbars nativas usan Candle.
            return self.price
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2.0

    @property
    def availability_known(self) -> bool:
        """Indica si existe evidencia de recepción/disponibilidad local."""

        return self.available_at is not None or self.received_at is not None

    @property
    def effective_available_at(self) -> datetime:
        """Momento lógico legado; ``availability_known`` conserva la evidencia.

        El fallback a ``event_time`` existe sólo para compatibilidad de los
        agregadores históricos. Los adaptadores que no conocen recepción deben
        conservar ``available_at=None`` y marcar la política de disponibilidad;
        este valor no debe presentarse como latencia observada.
        """

        return self.available_at or self.received_at or self.event_time


@dataclass(frozen=True, slots=True)
class Candle:
    """Vela normalizada en el intervalo semiabierto ``[start, end)``."""

    instrument: str
    timeframe: Timeframe | str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    event_count: int = 0
    source: str = "unknown"
    mode: OperationMode = OperationMode.REPLAY
    price_base: PriceBase = PriceBase.TRADED
    closed: bool = True
    available_at: datetime | None = None
    received_at: datetime | None = None
    quality: DataQuality = field(default_factory=DataQuality.good)
    candle_id: str | None = None
    origin: str = "aggregated"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        instrument, timeframe, start, end = _normalize_candle_identity(
            self.instrument, self.timeframe, self.start, self.end
        )
        values, volume, event_count = _normalize_candle_measurements(
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.event_count,
        )
        available_at, received_at = _normalize_candle_availability(
            self.closed, end, self.available_at, self.received_at
        )
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "timeframe", timeframe)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        for name, value in values.items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "volume", volume)
        object.__setattr__(self, "event_count", event_count)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "source", str(self.source).strip() or "unknown")
        object.__setattr__(self, "origin", str(self.origin).strip() or "aggregated")
        if not isinstance(self.mode, OperationMode):
            object.__setattr__(self, "mode", OperationMode(str(self.mode).upper()))
        object.__setattr__(self, "price_base", _coerce_price_base(self.price_base))
        object.__setattr__(self, "metadata", _mapping(self.metadata))
        candle_id = self.candle_id
        if candle_id is None:
            payload = "|".join(
                (
                    instrument,
                    timeframe.name,
                    start.isoformat(),
                    end.isoformat(),
                    self.source,
                    self.price_base.value,
                    self.origin,
                )
            )
            candle_id = "bar_" + hashlib.sha256(payload.encode()).hexdigest()[:32]
        if not str(candle_id).strip():
            raise ValueError("candle_id no puede estar vacío")
        object.__setattr__(self, "candle_id", str(candle_id))

    @property
    def timeframe_name(self) -> str:
        return self.normalized_timeframe.name

    @property
    def normalized_timeframe(self) -> Timeframe:
        """Temporalidad normalizada tras la validación del constructor."""

        return parse_timeframe(self.timeframe)

    @property
    def effective_available_at(self) -> datetime | None:
        return self.available_at if self.closed else self.available_at

    @property
    def availability_known(self) -> bool:
        """Indica si la vela tiene evidencia local de disponibilidad."""

        return self.available_at is not None or self.received_at is not None

    @property
    def is_native(self) -> bool:
        """True sólo para la base explícita nativa del proveedor."""

        return self.price_base is PriceBase.NATIVE

    @property
    def is_synthetic(self) -> bool:
        return self.mode is OperationMode.SYNTHETIC or self.quality.has(QualityFlag.SYNTHETIC)


# Nombres cortos solicitados por la integración.
Event = MarketEvent
Bar = Candle


__all__ = [
    "Bar",
    "Candle",
    "Event",
    "EventKind",
    "MarketEvent",
    "Mode",
    "OperationMode",
    "PriceBase",
    "Timeframe",
    "UTC",
    "normalize_price_base",
    "normalize_utc",
    "parse_timeframe",
]
