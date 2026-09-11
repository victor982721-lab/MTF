"""Deterministic, local-only Forex/CFD bid/ask simulation.

This module is deliberately separate from the virtual UP/DOWN contract
simulator.  It models a linear position in explicit instrument units:

* LONG enters at ask and closes at bid;
* SHORT enters at bid and closes at ask;
* prices, units, pips and monetary values use :class:`decimal.Decimal`;
* no leverage, fixed payout or execution connector is present.

The same ``submit``/``on_quote`` state machine powers streaming and replay.
Missing future quotes keep a live stream ``PENDING``; a caller-declared complete
capture changes unresolved work to ``UNKNOWN``.  Missing conversion or required
financing data also yields ``UNKNOWN`` rather than an invented cost or rate.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
import hashlib
import json
import math
from typing import Any, Iterable, Iterator, Mapping, Sequence


D0 = Decimal("0")
D1 = Decimal("1")


class CFDSimulationError(ValueError):
    """Configuration or input error; no trade is silently fabricated."""


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class TradeState(str, Enum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


_ALLOWED_FILL_POLICIES = {"first_quote_at_or_after"}
_ALLOWED_QUALITY = {"VALID", "VALIDATED", "OK", "GOOD", "CLOSED_VALID", "SYNTHETIC", "SYNTHETIC_VALID", "SYNTHETIC_VALIDATED", "DATA_QUALITY_VALIDATED", "PUBLIC_PROVIDER_CLOSED"}


def decimal(value: Any, *, name: str, minimum: Decimal | None = None, positive: bool = False) -> Decimal:
    """Convert exact textual/numeric input to a finite Decimal."""

    if isinstance(value, bool):
        raise CFDSimulationError(f"{name} debe ser Decimal/número, no booleano")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CFDSimulationError(f"{name} no es decimal válido: {value!r}") from exc
    if not result.is_finite():
        raise CFDSimulationError(f"{name} debe ser finito")
    if positive and result <= D0:
        raise CFDSimulationError(f"{name} debe ser positivo")
    if minimum is not None and result < minimum:
        raise CFDSimulationError(f"{name} debe ser >= {minimum}")
    return result


def _utc(value: Any, *, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z") or text.endswith("z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError) as exc:
            raise CFDSimulationError(f"{name} debe ser ISO-8601 con zona: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CFDSimulationError(f"{name} debe incluir zona horaria")
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


def _direction(value: Any) -> Direction:
    if isinstance(value, Direction):
        return value
    raw = str(value).strip().upper()
    aliases = {"LONG": Direction.LONG, "BUY": Direction.LONG, "UP": Direction.LONG, "SHORT": Direction.SHORT, "SELL": Direction.SHORT, "DOWN": Direction.SHORT}
    try:
        return aliases[raw]
    except KeyError as exc:
        raise CFDSimulationError(f"dirección desconocida: {value!r}") from exc


def _bool(value: Any, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "si", "sí", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
    raise CFDSimulationError(f"{name} debe ser booleano: {value!r}")


def _id(value: Any, *, name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise CFDSimulationError(f"{name} no puede estar vacío")
    return text


def _quality_usable(value: Any) -> bool:
    if isinstance(value, Mapping):
        value = value.get("status", value.get("quality", "UNKNOWN"))
    raw = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if not raw or any(token in raw for token in ("INVALID", "UNKNOWN", "STALE", "DISCONNECT", "GAP", "OPEN", "PARTIAL", "UNRECONCILED", "OUT_OF_ORDER", "DUPLICATE", "LATE")):
        return False
    return raw in _ALLOWED_QUALITY or raw.startswith("VALID_")


def _instrument_quote_currency(instrument: str, configured: str | None) -> str | None:
    if configured:
        return configured.strip().upper() or None
    parts = instrument.upper().replace("-", "/").split("/")
    return parts[1] if len(parts) == 2 and parts[1] else None


def _config_hash(config: "CFDConfig") -> str:
    return hashlib.sha256(json.dumps(config.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CFDQuote:
    """Normalized bid/ask quote with separate market and availability times."""

    instrument: str
    market_time: datetime
    bid: Decimal
    ask: Decimal
    quote_id: str
    available_at: datetime | None = None
    source: str = "fixture"
    sequence: int | str | None = None
    quality: str = "VALID"
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        instrument = _id(self.instrument, name="instrument").upper()
        market = _utc(self.market_time, name="market_time")
        available = _utc(self.available_at, name="available_at") if self.available_at is not None else market
        bid = decimal(self.bid, name="bid", positive=True)
        ask = decimal(self.ask, name="ask", positive=True)
        if ask < bid:
            raise CFDSimulationError("ask debe ser mayor o igual que bid")
        if available < market:
            raise CFDSimulationError("available_at no puede preceder market_time")
        quote_id = _id(self.quote_id, name="quote_id")
        source = _id(self.source, name="source")
        quality = str(self.quality or "UNKNOWN").strip().upper()
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "market_time", market)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        object.__setattr__(self, "quote_id", quote_id)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "metadata", dict(self.metadata or {}))

    @property
    def identity(self) -> str:
        return self.quote_id

    @property
    def available_ts(self) -> datetime:
        return self.available_at or self.market_time

    @property
    def spread(self) -> Decimal:
        return self.ask - self.bid

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "market_time": _iso(self.market_time),
            "available_at": _iso(self.available_ts),
            "bid": str(self.bid),
            "ask": str(self.ask),
            "spread": str(self.spread),
            "quote_id": self.quote_id,
            "identity": self.identity,
            "source": self.source,
            "sequence": self.sequence,
            "quality": self.quality,
            "metadata": dict(self.metadata or {}),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CFDQuote":
        return cls(
            instrument=value.get("instrument", value.get("symbol", "")),
            market_time=value.get("market_time", value.get("timestamp", value.get("event_time"))),
            available_at=value.get("available_at", value.get("available_ts")),
            bid=value["bid"],
            ask=value["ask"],
            quote_id=value.get("quote_id", value.get("id", value.get("identity", ""))),
            source=value.get("source", value.get("provider", "fixture")),
            sequence=value.get("sequence"),
            quality=value.get("quality", "VALID"),
            metadata=value.get("metadata", {}),
        )


@dataclass(frozen=True, slots=True)
class CFDSignal:
    """A direction decision; it contains no executable broker instruction."""

    signal_id: str
    instrument: str
    direction: Direction | str
    detected_at: datetime
    available_at: datetime | None = None
    strategy: str = "unknown"
    quality: str = "VALID"
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "signal_id", _id(self.signal_id, name="signal_id"))
        object.__setattr__(self, "instrument", _id(self.instrument, name="instrument").upper())
        object.__setattr__(self, "direction", _direction(self.direction))
        detected = _utc(self.detected_at, name="detected_at")
        available = _utc(self.available_at, name="available_at") if self.available_at is not None else detected
        if available < detected:
            raise CFDSimulationError("signal available_at no puede preceder detected_at")
        object.__setattr__(self, "detected_at", detected)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "strategy", str(self.strategy or "unknown"))
        object.__setattr__(self, "quality", str(self.quality or "UNKNOWN").strip().upper())
        object.__setattr__(self, "metadata", dict(self.metadata or {}))

    @property
    def identity(self) -> str:
        return self.signal_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "identity": self.identity,
            "instrument": self.instrument,
            "direction": self.direction.value,
            "detected_at": _iso(self.detected_at),
            "available_at": _iso(self.available_at),
            "strategy": self.strategy,
            "quality": self.quality,
            "metadata": dict(self.metadata or {}),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CFDSignal":
        return cls(
            signal_id=value.get("signal_id", value.get("id", "")),
            instrument=value.get("instrument", value.get("symbol", "")),
            direction=value.get("direction", value.get("side", "")),
            detected_at=value.get("detected_at", value.get("detected_ts", value.get("timestamp"))),
            available_at=value.get("available_at", value.get("available_ts")),
            strategy=value.get("strategy", "unknown"),
            quality=value.get("quality", "VALID"),
            metadata=value.get("metadata", {}),
        )


@dataclass(frozen=True, slots=True)
class CFDConfig:
    """Explicit linear-unit Forex/CFD simulation assumptions."""

    instrument: str = "EUR/USD"
    units: Decimal = Decimal("1000")
    pip_size: Decimal = Decimal("0.0001")
    price_precision: int = 5
    horizons_seconds: tuple[Decimal, ...] = (Decimal("60"), Decimal("180"), Decimal("300"))
    decision_latency_seconds: Decimal = Decimal("0")
    entry_latency_seconds: Decimal = Decimal("0")
    close_latency_seconds: Decimal = Decimal("0")
    max_quote_age_seconds: Decimal = Decimal("5")
    max_spread: Decimal | None = None
    fill_policy: str = "first_quote_at_or_after"
    close_policy: str = "first_quote_at_or_after"
    commission_fixed: Decimal = Decimal("0")
    commission_per_unit: Decimal = Decimal("0")
    slippage_pips: Decimal = Decimal("0")
    account_currency: str = "USD"
    quote_currency: str | None = None
    conversion_rate: Decimal | None = None
    financing_required: bool = False
    financing_rate_per_second: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument", _id(self.instrument, name="instrument").upper())
        object.__setattr__(self, "units", decimal(self.units, name="units", positive=True))
        object.__setattr__(self, "pip_size", decimal(self.pip_size, name="pip_size", positive=True))
        if isinstance(self.price_precision, bool) or not isinstance(self.price_precision, int) or self.price_precision < 0:
            raise CFDSimulationError("price_precision debe ser entero no negativo")
        horizons = tuple(decimal(item, name="horizon", positive=True) for item in self.horizons_seconds)
        if not horizons:
            raise CFDSimulationError("horizons_seconds no puede estar vacío")
        object.__setattr__(self, "horizons_seconds", horizons)
        for name in ("decision_latency_seconds", "entry_latency_seconds", "close_latency_seconds", "max_quote_age_seconds"):
            object.__setattr__(self, name, decimal(getattr(self, name), name=name, minimum=D0))
        if self.max_spread is not None:
            object.__setattr__(self, "max_spread", decimal(self.max_spread, name="max_spread", minimum=D0))
        object.__setattr__(self, "fill_policy", str(self.fill_policy).strip())
        object.__setattr__(self, "close_policy", str(self.close_policy).strip())
        if self.fill_policy not in _ALLOWED_FILL_POLICIES or self.close_policy not in _ALLOWED_FILL_POLICIES:
            raise CFDSimulationError("fill_policy/close_policy desconocida; sólo first_quote_at_or_after")
        object.__setattr__(self, "commission_fixed", decimal(self.commission_fixed, name="commission_fixed", minimum=D0))
        object.__setattr__(self, "commission_per_unit", decimal(self.commission_per_unit, name="commission_per_unit", minimum=D0))
        object.__setattr__(self, "slippage_pips", decimal(self.slippage_pips, name="slippage_pips", minimum=D0))
        account = _id(self.account_currency, name="account_currency").upper()
        object.__setattr__(self, "account_currency", account)
        quote = _instrument_quote_currency(self.instrument, self.quote_currency)
        object.__setattr__(self, "quote_currency", quote)
        if self.conversion_rate is not None:
            object.__setattr__(self, "conversion_rate", decimal(self.conversion_rate, name="conversion_rate", positive=True))
        if not isinstance(self.financing_required, bool):
            raise CFDSimulationError("financing_required debe ser booleano")
        if self.financing_rate_per_second is not None:
            object.__setattr__(self, "financing_rate_per_second", decimal(self.financing_rate_per_second, name="financing_rate_per_second", minimum=D0))
        if self.financing_required and self.financing_rate_per_second is None:
            # This is a deliberate runtime UNKNOWN condition, not a config
            # error: a user may declare financing required and wait for a
            # provider/account-specific rate.
            pass

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CFDConfig":
        allowed = {item.name for item in fields(cls)}
        unknown = set(value) - allowed
        if unknown:
            raise CFDSimulationError(f"claves CFD desconocidas: {sorted(unknown)}")
        data = dict(value)
        for name in ("units", "pip_size", "decision_latency_seconds", "entry_latency_seconds", "close_latency_seconds", "max_quote_age_seconds", "max_spread", "commission_fixed", "commission_per_unit", "slippage_pips", "conversion_rate", "financing_rate_per_second"):
            if name in data and data[name] is not None:
                data[name] = Decimal(str(data[name]))
        if "horizons_seconds" in data:
            data["horizons_seconds"] = tuple(Decimal(str(item)) for item in data["horizons_seconds"])
        return cls(**data)

    @property
    def slippage_price(self) -> Decimal:
        return self.slippage_pips * self.pip_size

    @property
    def config_hash(self) -> str:
        return _config_hash(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "units": str(self.units),
            "pip_size": str(self.pip_size),
            "price_precision": self.price_precision,
            "horizons_seconds": [str(x) for x in self.horizons_seconds],
            "decision_latency_seconds": str(self.decision_latency_seconds),
            "entry_latency_seconds": str(self.entry_latency_seconds),
            "close_latency_seconds": str(self.close_latency_seconds),
            "max_quote_age_seconds": str(self.max_quote_age_seconds),
            "max_spread": str(self.max_spread) if self.max_spread is not None else None,
            "fill_policy": self.fill_policy,
            "close_policy": self.close_policy,
            "commission_fixed": str(self.commission_fixed),
            "commission_per_unit": str(self.commission_per_unit),
            "slippage_pips": str(self.slippage_pips),
            "account_currency": self.account_currency,
            "quote_currency": self.quote_currency,
            "conversion_rate": str(self.conversion_rate) if self.conversion_rate is not None else None,
            "financing_required": self.financing_required,
            "financing_rate_per_second": str(self.financing_rate_per_second) if self.financing_rate_per_second is not None else None,
        }


@dataclass(frozen=True, slots=True)
class CFDTrade:
    """Auditable state/result for one signal and horizon."""

    trade_id: str
    signal_id: str
    instrument: str
    direction: Direction | str
    units: Decimal
    horizon_seconds: Decimal
    state: TradeState | str
    detected_at: datetime
    signal_available_at: datetime
    decision_at: datetime
    entry_target_at: datetime
    fill_policy: str
    close_policy: str
    pip_size: Decimal
    price_precision: int
    account_currency: str
    quote_currency: str | None
    entry_market_at: datetime | None = None
    entry_available_at: datetime | None = None
    entry_quote_id: str | None = None
    entry_price: Decimal | None = None
    entry_side: str | None = None
    close_target_at: datetime | None = None
    close_market_at: datetime | None = None
    close_available_at: datetime | None = None
    close_quote_id: str | None = None
    close_price: Decimal | None = None
    pips: Decimal | None = None
    gross_pnl_quote: Decimal | None = None
    commission_quote: Decimal | None = None
    slippage_quote: Decimal | None = None
    financing_quote: Decimal | None = None
    gross_pnl_account: Decimal | None = None
    costs_account: Decimal | None = None
    net_pnl: Decimal | None = None
    conversion_rate: Decimal | None = None
    quality: str = "VALID"
    reason: str | None = None
    lineage: Mapping[str, Any] | None = None
    product: str = "FOREX_CFD_LOCAL_PAPER"

    def __post_init__(self) -> None:
        object.__setattr__(self, "trade_id", _id(self.trade_id, name="trade_id"))
        object.__setattr__(self, "signal_id", _id(self.signal_id, name="signal_id"))
        object.__setattr__(self, "instrument", _id(self.instrument, name="instrument").upper())
        object.__setattr__(self, "direction", _direction(self.direction))
        object.__setattr__(self, "units", decimal(self.units, name="units", positive=True))
        object.__setattr__(self, "horizon_seconds", decimal(self.horizon_seconds, name="horizon_seconds", positive=True))
        state = self.state if isinstance(self.state, TradeState) else TradeState(str(self.state).upper())
        object.__setattr__(self, "state", state)
        for name in ("detected_at", "signal_available_at", "decision_at", "entry_target_at"):
            object.__setattr__(self, name, _utc(getattr(self, name), name=name))
        for name in ("entry_market_at", "entry_available_at", "close_target_at", "close_market_at", "close_available_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value, name=name))
        for name in ("pip_size", "entry_price", "close_price", "pips", "gross_pnl_quote", "commission_quote", "slippage_quote", "financing_quote", "gross_pnl_account", "costs_account", "net_pnl", "conversion_rate"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal(value, name=name))
        if isinstance(self.price_precision, bool) or not isinstance(self.price_precision, int) or self.price_precision < 0:
            raise CFDSimulationError("price_precision inválido en trade")
        object.__setattr__(self, "account_currency", _id(self.account_currency, name="account_currency").upper())
        object.__setattr__(self, "quote_currency", self.quote_currency.upper() if self.quote_currency else None)
        object.__setattr__(self, "quality", str(self.quality or "UNKNOWN").strip().upper())
        object.__setattr__(self, "lineage", dict(self.lineage or {}))
        if str(self.product) != "FOREX_CFD_LOCAL_PAPER":
            raise CFDSimulationError("product de CFD no soportado")
        object.__setattr__(self, "product", "FOREX_CFD_LOCAL_PAPER")

    @property
    def identity(self) -> str:
        return self.trade_id

    @property
    def is_terminal(self) -> bool:
        return self.state in {TradeState.CLOSED, TradeState.REJECTED, TradeState.UNKNOWN}

    @property
    def effective_fill_at(self) -> datetime | None:
        return self.entry_available_at

    def to_dict(self) -> dict[str, Any]:
        def dec(value: Decimal | None) -> str | None:
            return str(value) if value is not None else None
        return {
            "product": self.product,
            "trade_id": self.trade_id,
            "identity": self.identity,
            "signal_id": self.signal_id,
            "instrument": self.instrument,
            "direction": self.direction.value,
            "units": dec(self.units),
            "horizon_seconds": dec(self.horizon_seconds),
            "state": self.state.value,
            "detected_at": _iso(self.detected_at),
            "signal_available_at": _iso(self.signal_available_at),
            "decision_at": _iso(self.decision_at),
            "entry_target_at": _iso(self.entry_target_at),
            "fill_policy": self.fill_policy,
            "close_policy": self.close_policy,
            "pip_size": dec(self.pip_size),
            "price_precision": self.price_precision,
            "account_currency": self.account_currency,
            "quote_currency": self.quote_currency,
            "entry_market_at": _iso(self.entry_market_at),
            "entry_available_at": _iso(self.entry_available_at),
            "entry_quote_id": self.entry_quote_id,
            "entry_price": dec(self.entry_price),
            "entry_side": self.entry_side,
            "close_target_at": _iso(self.close_target_at),
            "close_market_at": _iso(self.close_market_at),
            "close_available_at": _iso(self.close_available_at),
            "close_quote_id": self.close_quote_id,
            "close_price": dec(self.close_price),
            "pips": dec(self.pips),
            "gross_pnl_quote": dec(self.gross_pnl_quote),
            "commission_quote": dec(self.commission_quote),
            "slippage_quote": dec(self.slippage_quote),
            "financing_quote": dec(self.financing_quote),
            "gross_pnl_account": dec(self.gross_pnl_account),
            "costs_account": dec(self.costs_account),
            "net_pnl": dec(self.net_pnl),
            "conversion_rate": dec(self.conversion_rate),
            "quality": self.quality,
            "reason": self.reason,
            "lineage": dict(self.lineage or {}),
        }


@dataclass(frozen=True, slots=True)
class CFDReplayResult:
    """Sequence-like replay output with all terminal and pending trades."""

    trades: tuple[CFDTrade, ...]
    events: tuple[Mapping[str, Any], ...] = ()
    capture_complete: bool = True

    def __iter__(self) -> Iterator[CFDTrade]:
        return iter(self.trades)

    def __len__(self) -> int:
        return len(self.trades)

    def __getitem__(self, item: int | slice) -> CFDTrade | tuple[CFDTrade, ...]:
        return self.trades[item]

    @property
    def closed(self) -> tuple[CFDTrade, ...]:
        return tuple(item for item in self.trades if item.state is TradeState.CLOSED)

    def to_dict(self) -> dict[str, Any]:
        return {"capture_complete": self.capture_complete, "trades": [item.to_dict() for item in self.trades], "events": [dict(item) for item in self.events]}


class CFDSimulator:
    """Streaming/replay state machine for linear bid/ask positions."""

    def __init__(self, config: CFDConfig | Mapping[str, Any] | None = None) -> None:
        self.config = config if isinstance(config, CFDConfig) else CFDConfig.from_mapping(config or {})
        self._trades: dict[str, CFDTrade] = {}
        self._seen_quotes: set[str] = set()
        self._quote_order: list[str] = []
        self._events: list[dict[str, Any]] = []
        self._last_watermark: datetime | None = None

    @property
    def trades(self) -> tuple[CFDTrade, ...]:
        return tuple(self._trades.values())

    @property
    def positions(self) -> tuple[CFDTrade, ...]:
        return self.trades

    @property
    def events(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self._events)

    def reset(self) -> None:
        self._trades.clear()
        self._seen_quotes.clear()
        self._quote_order.clear()
        self._events.clear()
        self._last_watermark = None

    def submit(self, signal: CFDSignal | Mapping[str, Any], *, horizon_seconds: Decimal | int | str | None = None) -> CFDTrade:
        signal_obj = signal if isinstance(signal, CFDSignal) else CFDSignal.from_mapping(signal)
        horizon = decimal(horizon_seconds, name="horizon_seconds", positive=True) if horizon_seconds is not None else self.config.horizons_seconds[0]
        existing = next((item for item in self._trades.values() if item.signal_id == signal_obj.signal_id and item.horizon_seconds == horizon), None)
        if existing is not None:
            return existing
        trade_id = self._trade_id(signal_obj, horizon)
        decision_at = max(signal_obj.detected_at + timedelta(seconds=float(self.config.decision_latency_seconds)), signal_obj.available_at or signal_obj.detected_at)
        entry_target = decision_at + timedelta(seconds=float(self.config.entry_latency_seconds))
        base_kwargs = {
            "trade_id": trade_id,
            "signal_id": signal_obj.signal_id,
            "instrument": signal_obj.instrument,
            "direction": signal_obj.direction,
            "units": self.config.units,
            "horizon_seconds": horizon,
            "state": TradeState.PENDING,
            "detected_at": signal_obj.detected_at,
            "signal_available_at": signal_obj.available_at or signal_obj.detected_at,
            "decision_at": decision_at,
            "entry_target_at": entry_target,
            "fill_policy": self.config.fill_policy,
            "close_policy": self.config.close_policy,
            "pip_size": self.config.pip_size,
            "price_precision": self.config.price_precision,
            "account_currency": self.config.account_currency,
            "quote_currency": self.config.quote_currency,
            "quality": signal_obj.quality,
            "lineage": {"parent_signal_id": signal_obj.signal_id, "strategy": signal_obj.strategy, "config_hash": self.config.config_hash},
        }
        trade = CFDTrade(**base_kwargs)
        if signal_obj.instrument != self.config.instrument:
            trade = replace(trade, state=TradeState.REJECTED, reason="INSTRUMENT_MISMATCH", quality="UNKNOWN")
        elif not _quality_usable(signal_obj.quality):
            trade = replace(trade, state=TradeState.REJECTED, reason="SIGNAL_QUALITY_BLOCKED", quality="UNKNOWN")
        self._trades[trade.trade_id] = trade
        self._events.append({"event": "submitted", "trade_id": trade.trade_id, "signal_id": trade.signal_id, "state": trade.state.value, "at": _iso(decision_at)})
        return trade

    def submit_all(self, signal: CFDSignal | Mapping[str, Any]) -> tuple[CFDTrade, ...]:
        """Create one independent local experiment per configured horizon."""
        signal_obj = signal if isinstance(signal, CFDSignal) else CFDSignal.from_mapping(signal)
        return tuple(self.submit(signal_obj, horizon_seconds=horizon) for horizon in self.config.horizons_seconds)

    def on_quote(self, quote: CFDQuote | Mapping[str, Any], *, capture_complete: bool = False) -> tuple[CFDTrade, ...]:
        quote_obj = quote if isinstance(quote, CFDQuote) else CFDQuote.from_mapping(quote)
        if not isinstance(capture_complete, bool):
            raise CFDSimulationError("capture_complete debe ser booleano")
        watermark = quote_obj.available_ts
        if self._last_watermark is not None and watermark < self._last_watermark:
            raise CFDSimulationError("quote fuera de orden en streaming; use replay para ordenar explícitamente")
        self._last_watermark = watermark
        if quote_obj.identity in self._seen_quotes:
            self._events.append({"event": "duplicate_quote", "quote_id": quote_obj.identity, "at": _iso(watermark)})
            return ()
        self._seen_quotes.add(quote_obj.identity)
        self._quote_order.append(quote_obj.identity)
        changed: list[CFDTrade] = []
        for trade in tuple(self._trades.values()):
            if trade.state not in {TradeState.PENDING, TradeState.FILLED}:
                continue
            if trade.instrument != quote_obj.instrument:
                continue
            if not _quality_usable(quote_obj.quality):
                continue
            if trade.state is TradeState.PENDING:
                selected = self._entry_fill(trade, quote_obj, watermark)
                if selected is not None:
                    trade = selected
                    self._trades[trade.trade_id] = trade
                    changed.append(trade)
                    self._events.append({"event": "filled", "trade_id": trade.trade_id, "quote_id": quote_obj.identity, "at": _iso(trade.entry_available_at), "price": str(trade.entry_price)})
            if trade.state is TradeState.FILLED:
                selected_close = self._close_fill(trade, quote_obj, watermark)
                if selected_close is not None:
                    trade = selected_close
                    self._trades[trade.trade_id] = trade
                    changed.append(trade)
                    self._events.append({"event": "closed" if trade.state is TradeState.CLOSED else "unknown", "trade_id": trade.trade_id, "quote_id": quote_obj.identity, "at": _iso(trade.close_available_at), "reason": trade.reason})
        if capture_complete:
            changed.extend(self.advance(watermark, capture_complete=True))
        return tuple(dict((item.trade_id, item) for item in changed).values())

    def advance(self, watermark: datetime, *, capture_complete: bool = False) -> tuple[CFDTrade, ...]:
        """Advance the watermark without inventing a quote or a fill."""

        current = _utc(watermark, name="watermark")
        if self._last_watermark is not None and current < self._last_watermark:
            raise CFDSimulationError("watermark no puede retroceder")
        self._last_watermark = current
        if not isinstance(capture_complete, bool):
            raise CFDSimulationError("capture_complete debe ser booleano")
        if not capture_complete:
            return ()
        changed: list[CFDTrade] = []
        for trade in tuple(self._trades.values()):
            if trade.state not in {TradeState.PENDING, TradeState.FILLED}:
                continue
            reason = "ENTRY_QUOTE_NOT_AVAILABLE" if trade.state is TradeState.PENDING else "CLOSE_QUOTE_NOT_AVAILABLE"
            updated = replace(trade, state=TradeState.UNKNOWN, reason=reason, quality="UNKNOWN")
            self._trades[trade.trade_id] = updated
            changed.append(updated)
            self._events.append({"event": "unknown", "trade_id": trade.trade_id, "reason": reason, "at": _iso(current)})
        return tuple(changed)

    def replay(
        self,
        signals: Iterable[CFDSignal | Mapping[str, Any]],
        quotes: Iterable[CFDQuote | Mapping[str, Any]],
        *,
        capture_complete: bool = True,
    ) -> CFDReplayResult:
        """Replay a mixed causal timeline through the same streaming methods."""

        if not isinstance(capture_complete, bool):
            raise CFDSimulationError("capture_complete debe ser booleano")
        timeline: list[tuple[datetime, int, int, CFDSignal | CFDQuote]] = []
        for ordinal, signal in enumerate(signals):
            obj = signal if isinstance(signal, CFDSignal) else CFDSignal.from_mapping(signal)
            timeline.append((obj.available_at or obj.detected_at, 0, ordinal, obj))
        offset = len(timeline)
        for ordinal, quote in enumerate(quotes):
            obj = quote if isinstance(quote, CFDQuote) else CFDQuote.from_mapping(quote)
            timeline.append((obj.available_ts, 1, ordinal + offset, obj))
        timeline.sort(key=lambda item: (item[0], item[1], item[2], item[3].identity))
        for _when, kind, _ordinal, obj in timeline:
            if kind == 0:
                self.submit_all(obj)  # type: ignore[arg-type]
            else:
                self.on_quote(obj, capture_complete=False)  # type: ignore[arg-type]
        if timeline:
            self.advance(max(item[0] for item in timeline), capture_complete=capture_complete)
        return CFDReplayResult(self.trades, self.events, capture_complete)

    def stream(
        self,
        signals: Iterable[CFDSignal | Mapping[str, Any]],
        quotes: Iterable[CFDQuote | Mapping[str, Any]],
        *,
        capture_complete: bool = False,
    ) -> CFDReplayResult:
        """Run the same timeline state machine as replay, open by default."""
        return self.replay(signals, quotes, capture_complete=capture_complete)

    def _entry_fill(self, trade: CFDTrade, quote: CFDQuote, watermark: datetime) -> CFDTrade | None:
        target = trade.entry_target_at
        if quote.market_time < target or quote.available_ts < target:
            return None
        age = Decimal(str(max(0.0, (quote.available_ts - target).total_seconds())))
        if age > self.config.max_quote_age_seconds:
            return None
        if self.config.max_spread is not None and quote.spread > self.config.max_spread:
            return None
        raw = quote.ask if trade.direction is Direction.LONG else quote.bid
        slip = self.config.slippage_price
        price = raw + slip if trade.direction is Direction.LONG else raw - slip
        price = self._quantize(price)
        close_target = max(quote.available_ts, target) + timedelta(seconds=float(trade.horizon_seconds + self.config.close_latency_seconds))
        return replace(
            trade,
            state=TradeState.FILLED,
            entry_market_at=quote.market_time,
            entry_available_at=quote.available_ts,
            entry_quote_id=quote.identity,
            entry_price=price,
            entry_side="ask" if trade.direction is Direction.LONG else "bid",
            close_target_at=close_target,
            quality=quote.quality,
            lineage={**dict(trade.lineage or {}), "entry_quote_id": quote.identity, "entry_source": quote.source},
        )

    def _close_fill(self, trade: CFDTrade, quote: CFDQuote, watermark: datetime) -> CFDTrade | None:
        target = trade.close_target_at
        if target is None or quote.market_time < target or quote.available_ts < target:
            return None
        age = Decimal(str(max(0.0, (quote.available_ts - target).total_seconds())))
        if age > self.config.max_quote_age_seconds:
            return None
        if self.config.max_spread is not None and quote.spread > self.config.max_spread:
            return None
        raw = quote.bid if trade.direction is Direction.LONG else quote.ask
        slip = self.config.slippage_price
        price = raw - slip if trade.direction is Direction.LONG else raw + slip
        price = self._quantize(price)
        assert trade.entry_price is not None and trade.entry_available_at is not None
        sign = D1 if trade.direction is Direction.LONG else Decimal("-1")
        gross = (price - trade.entry_price) * trade.units * sign
        raw_entry = (quote.bid if trade.direction is Direction.LONG else quote.ask)  # only for fallback reporting
        del raw_entry
        commission = self.config.commission_fixed + self.config.commission_per_unit * trade.units
        slippage_cost = self._slippage_cost(trade, quote, price)
        hold_seconds = Decimal(str(max(0.0, (quote.available_ts - trade.entry_available_at).total_seconds())))
        financing_missing = self.config.financing_required and self.config.financing_rate_per_second is None
        financing = None if financing_missing else (abs(trade.entry_price * trade.units) * (self.config.financing_rate_per_second or D0) * hold_seconds)
        costs_quote = commission + (slippage_cost or D0) + (financing or D0)
        gross_account, costs_account, net, unknown_reason = self._accounting(gross, costs_quote, financing_missing)
        state = TradeState.UNKNOWN if unknown_reason else TradeState.CLOSED
        reason = unknown_reason
        quality = "UNKNOWN" if unknown_reason else quote.quality
        return replace(
            trade,
            state=state,
            close_market_at=quote.market_time,
            close_available_at=quote.available_ts,
            close_quote_id=quote.identity,
            close_price=price,
            pips=((price - trade.entry_price) / trade.pip_size) * sign,
            gross_pnl_quote=gross,
            commission_quote=commission,
            slippage_quote=slippage_cost,
            financing_quote=financing,
            gross_pnl_account=gross_account,
            costs_account=costs_account,
            net_pnl=net,
            conversion_rate=self.config.conversion_rate,
            quality=quality,
            reason=reason,
            lineage={**dict(trade.lineage or {}), "close_quote_id": quote.identity, "close_source": quote.source},
        )

    def _accounting(self, gross_quote: Decimal, costs_quote: Decimal, financing_missing: bool) -> tuple[Decimal | None, Decimal | None, Decimal | None, str | None]:
        if financing_missing:
            return None, None, None, "FINANCING_RATE_MISSING"
        quote = self.config.quote_currency
        account = self.config.account_currency
        if quote is None or account == quote:
            return gross_quote, costs_quote, gross_quote - costs_quote, None
        if self.config.conversion_rate is None:
            return None, None, None, "CONVERSION_RATE_MISSING"
        rate = self.config.conversion_rate
        gross_account = gross_quote * rate
        costs_account = costs_quote * rate
        return gross_account, costs_account, gross_account - costs_account, None

    def _slippage_cost(self, trade: CFDTrade, quote: CFDQuote, slipped_close: Decimal) -> Decimal:
        if self.config.slippage_price == D0 or trade.entry_price is None:
            return D0
        # Report slippage as an explicitly positive quote-currency cost.  It is
        # the adverse price movement on both sides, not an extra spread.
        return self.config.slippage_price * trade.units * Decimal("2")

    def _quantize(self, value: Decimal) -> Decimal:
        quantum = Decimal(1).scaleb(-self.config.price_precision)
        return value.quantize(quantum, rounding=ROUND_HALF_UP)

    def _trade_id(self, signal: CFDSignal, horizon: Decimal) -> str:
        payload = f"{signal.signal_id}|{signal.instrument}|{signal.direction.value}|{horizon}|{self.config.config_hash}"
        return "cfd_" + hashlib.sha256(payload.encode()).hexdigest()[:32]


# Clear aliases for callers who prefer a longer name.
ForexCFDSimulator = CFDSimulator
ForexCFDConfig = CFDConfig
ForexCFDSignal = CFDSignal
ForexCFDQuote = CFDQuote
ForexCFDTrade = CFDTrade


def known_fixture_eurusd_long() -> tuple[CFDSignal, tuple[CFDQuote, ...]]:
    """Offline fixture: 1000 EUR, ask 1.1002 to bid 1.1005 = 0.30 USD."""

    start = datetime(2026, 1, 1, tzinfo=UTC)
    signal = CFDSignal("fixture-long-1000", "EUR/USD", Direction.LONG, start, strategy="offline_fixture")
    quotes = (
        CFDQuote("EUR/USD", start, Decimal("1.1000"), Decimal("1.1002"), "fixture-entry"),
        CFDQuote("EUR/USD", start + timedelta(seconds=60), Decimal("1.1005"), Decimal("1.1007"), "fixture-close"),
    )
    return signal, quotes


__all__ = [
    "CFDConfig", "CFDQuote", "CFDReplayResult", "CFDSignal", "CFDSimulationError", "CFDSimulator", "CFDTrade",
    "Direction", "TradeState", "ForexCFDConfig", "ForexCFDQuote", "ForexCFDSignal", "ForexCFDSimulator", "ForexCFDTrade",
    "known_fixture_eurusd_long", "decimal",
]
