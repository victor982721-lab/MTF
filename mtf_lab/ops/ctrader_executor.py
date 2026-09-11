"""Explicitly gated cTrader *demo* execution seam.

This module is intentionally separate from MTF Lab's observation/strategy
pipeline.  It contains no real-account route, no OAuth/network client, and no
order auto-retry.  ``DemoTransport`` is an in-process deterministic fixture
for lifecycle tests; a future demo-only connector may implement the small
``ExecutionTransport`` protocol without changing the safety/persistence
state machine.

The order path is:

    signal + quote -> safety/risk checks -> intent journal -> transport submit

An intent is journaled before the transport call.  A timeout becomes UNKNOWN
and can only be resolved by querying the same client correlation id.  The
executor never blindly resubmits an uncertain request.
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib
import json
import math
import os
import threading
import uuid
from collections.abc import Iterable, Mapping, MutableMapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Errors and normalized records


class ExecutionError(RuntimeError):
    """Base class for a blocked or unresolved virtual execution."""


class SafetyViolation(ExecutionError):
    pass


class RealAccountForbidden(SafetyViolation):
    """Raised before any token/config/transport validation for REAL inputs."""


class DemoAccountRequired(SafetyViolation):
    pass


class EndpointRejected(SafetyViolation):
    pass


class ScopeRejected(SafetyViolation):
    pass


class ActivationRequired(SafetyViolation):
    pass


class ExecutionPaused(ExecutionError):
    pass


class RiskLimitRejected(ExecutionError):
    pass


class DuplicateIntent(ExecutionError):
    pass


class ForeignPosition(ExecutionError):
    pass


class CorrelationError(ExecutionError):
    pass


class OrderState(str, enum.Enum):
    INTENT_RECORDED = "INTENT_RECORDED"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    CLOSED = "CLOSED"
    CLOSE_PARTIAL = "CLOSE_PARTIAL"


_TERMINAL_STATES = frozenset({OrderState.FILLED, OrderState.REJECTED, OrderState.CLOSED})


class Side(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"

    @classmethod
    def parse(cls, value: Any) -> "Side":
        text = str(getattr(value, "value", value)).strip().upper()
        if text in {"BUY", "LONG", "UP", "ALCISTA"}:
            return cls.BUY
        if text in {"SELL", "SHORT", "DOWN", "BAJISTA"}:
            return cls.SELL
        raise ValueError(f"direction/side not supported: {value!r}")


class QuoteQuality(str, enum.Enum):
    VALID = "VALID"
    SYNTHETIC = "SYNTHETIC"
    UNKNOWN = "UNKNOWN"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    INVALID = "INVALID"


def _text_set(value: Iterable[str] | str) -> frozenset[str]:
    if isinstance(value, str): return frozenset({value.strip().lower()})
    return frozenset(str(item).strip().lower() for item in value)


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool): return value
    if isinstance(value, (int, float)) and value in {0, 1}: return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "si", "sí", "selected", "verified"}


@dataclasses.dataclass(frozen=True, slots=True)
class DemoAccount:
    """An explicitly selected and verified demo account descriptor."""

    account_id: str
    environment: str
    endpoint: str
    scopes: frozenset[str] = frozenset()
    selected: bool = False
    verified: bool = False
    token_ref: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", str(self.account_id).strip())
        object.__setattr__(self, "environment", str(self.environment).strip().upper())
        object.__setattr__(self, "endpoint", str(self.endpoint).strip())
        object.__setattr__(self, "scopes", _text_set(self.scopes))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DemoAccount":
        raw_demo = value.get("is_demo")
        environment = value.get("environment", value.get("mode", ""))
        if raw_demo is not None and "environment" not in value and "mode" not in value:
            environment = "DEMO" if _bool_value(raw_demo) else "REAL"
        return cls(
            account_id=str(value.get("account_id", value.get("accountId", value.get("id", "")))),
            environment=str(environment),
            endpoint=str(value.get("endpoint", value.get("base_url", value.get("api_endpoint", "")))),
            scopes=_text_set(value.get("scopes", value.get("permissions", ()))),
            selected=_bool_value(value.get("selected", value.get("is_selected", False))),
            verified=_bool_value(value.get("verified", value.get("is_verified", False))),
            token_ref=value.get("token_ref", value.get("token_reference")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "environment": self.environment,
            "endpoint": self.endpoint,
            "scopes": sorted(self.scopes),
            "selected": self.selected,
            "verified": self.verified,
            "token_ref": self.token_ref,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """Fail-closed demo policy and virtual risk caps."""

    required_scopes: frozenset[str] = frozenset({"trading"})
    max_quantity: float = 1.0
    fixed_quantity: float | None = 1.0
    max_exposure: float = 1000.0
    max_positions: int = 1
    max_spread: float = 1.0
    max_price_age_seconds: float = 30.0
    allowed_symbols: frozenset[str] = frozenset()
    no_martingale: bool = True
    close_only_own_positions: bool = True
    timeout_seconds: float = 5.0
    activated: bool = False

    def __post_init__(self) -> None:
        scopes = _text_set(self.required_scopes)
        symbols = frozenset(str(x).strip().upper() for x in self.allowed_symbols)
        object.__setattr__(self, "required_scopes", scopes)
        object.__setattr__(self, "allowed_symbols", symbols)
        for name in ("max_quantity", "max_exposure", "max_spread", "max_price_age_seconds", "timeout_seconds"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and non-negative")
            object.__setattr__(self, name, value)
        if self.max_quantity <= 0 or self.max_exposure <= 0 or self.timeout_seconds <= 0:
            raise ValueError("quantity, exposure and timeout caps must be positive")
        if self.fixed_quantity is not None:
            fixed = float(self.fixed_quantity)
            if not math.isfinite(fixed) or fixed <= 0 or fixed > self.max_quantity:
                raise ValueError("fixed_quantity must be positive and <= max_quantity")
            object.__setattr__(self, "fixed_quantity", fixed)
        if isinstance(self.max_positions, bool) or int(self.max_positions) < 1:
            raise ValueError("max_positions must be a positive integer")
        object.__setattr__(self, "max_positions", int(self.max_positions))
        if not isinstance(self.no_martingale, bool) or not isinstance(self.close_only_own_positions, bool):
            raise ValueError("risk policy flags must be boolean")
        if not self.no_martingale:
            raise ValueError("martingale is not supported by the demo executor")
        if not self.close_only_own_positions:
            raise ValueError("closing foreign positions is not supported by the demo executor")

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["required_scopes"] = sorted(self.required_scopes)
        result["allowed_symbols"] = sorted(self.allowed_symbols)
        return result


@dataclasses.dataclass(frozen=True, slots=True)
class Quote:
    """Explicit bid/ask observation used only for demo safety checks."""

    symbol: str
    bid: float
    ask: float
    timestamp: datetime
    available_at: datetime | None = None
    quality: str = "VALID"
    source: str = "demo"
    base_price: str = "bid_ask"

    def __post_init__(self) -> None:
        symbol = str(self.symbol).strip().upper()
        bid = float(self.bid); ask = float(self.ask)
        if not symbol or not math.isfinite(bid) or not math.isfinite(ask) or bid <= 0 or ask <= 0 or ask < bid:
            raise ValueError("quote symbol/bid/ask invalid")
        ts = _parse_time(self.timestamp); available = _parse_time(self.available_at) if self.available_at is not None else ts
        if available < ts:
            raise ValueError("quote available_at cannot precede timestamp")
        object.__setattr__(self, "symbol", symbol); object.__setattr__(self, "bid", bid); object.__setattr__(self, "ask", ask); object.__setattr__(self, "timestamp", ts); object.__setattr__(self, "available_at", available); object.__setattr__(self, "quality", str(self.quality).upper())

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def mid(self) -> float:
        return (self.ask + self.bid) / 2.0

    def price_for(self, side: Side) -> float:
        return self.ask if side is Side.BUY else self.bid

    def to_dict(self) -> dict[str, Any]:
        return {"symbol": self.symbol, "bid": self.bid, "ask": self.ask, "timestamp": _iso(self.timestamp), "available_at": _iso(self.available_at), "quality": self.quality, "source": self.source, "base_price": self.base_price, "spread": self.spread}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "Quote":
        return cls(symbol=str(value.get("symbol", value.get("instrument", ""))), bid=float(value["bid"]), ask=float(value["ask"]), timestamp=_parse_time(value.get("timestamp", value.get("event_time"))), available_at=_parse_time(value.get("available_at", value.get("received_at"))) if value.get("available_at", value.get("received_at")) is not None else None, quality=str(value.get("quality", "VALID")), source=str(value.get("source", "unknown")), base_price=str(value.get("base_price", value.get("price_base", "bid_ask"))))


@dataclasses.dataclass(frozen=True, slots=True)
class Fill:
    fill_id: str
    quantity: float
    price: float
    timestamp: datetime

    def __post_init__(self) -> None:
        quantity = float(self.quantity); price = float(self.price)
        if not math.isfinite(quantity) or quantity <= 0 or not math.isfinite(price) or price <= 0:
            raise ValueError("fill quantity/price invalid")
        object.__setattr__(self, "quantity", quantity); object.__setattr__(self, "price", price); object.__setattr__(self, "timestamp", _parse_time(self.timestamp))

    def to_dict(self) -> dict[str, Any]:
        return {"fill_id": self.fill_id, "quantity": self.quantity, "price": self.price, "timestamp": _iso(self.timestamp)}


@dataclasses.dataclass(frozen=True, slots=True)
class Position:
    position_id: str
    account_id: str
    symbol: str
    side: Side
    quantity: float
    entry_price: float
    client_order_id: str | None = None
    owner: str = "mtf-lab"

    def __post_init__(self) -> None:
        quantity = float(self.quantity); price = float(self.entry_price)
        if quantity <= 0 or not math.isfinite(quantity) or price <= 0 or not math.isfinite(price):
            raise ValueError("position quantity/entry_price invalid")
        object.__setattr__(self, "account_id", str(self.account_id)); object.__setattr__(self, "symbol", str(self.symbol).upper()); object.__setattr__(self, "side", self.side if isinstance(self.side, Side) else Side.parse(self.side)); object.__setattr__(self, "quantity", quantity); object.__setattr__(self, "entry_price", price)

    @property
    def notional(self) -> float:
        return self.quantity * self.entry_price

    def to_dict(self) -> dict[str, Any]:
        return {"position_id": self.position_id, "account_id": self.account_id, "symbol": self.symbol, "side": self.side.value, "quantity": self.quantity, "entry_price": self.entry_price, "client_order_id": self.client_order_id, "owner": self.owner, "notional": self.notional}


@dataclasses.dataclass(frozen=True, slots=True)
class ExecutionIntent:
    intent_id: str
    signal_id: str
    symbol: str
    side: Side
    quantity: float
    requested_price: float
    created_at: datetime
    account_id: str
    kind: str = "OPEN"
    position_id: str | None = None
    metadata: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        quantity = float(self.quantity); price = float(self.requested_price)
        if not self.intent_id or not self.signal_id or not self.symbol or quantity <= 0 or not math.isfinite(quantity) or price <= 0 or not math.isfinite(price):
            raise ValueError("execution intent fields invalid")
        object.__setattr__(self, "symbol", str(self.symbol).upper()); object.__setattr__(self, "side", self.side if isinstance(self.side, Side) else Side.parse(self.side)); object.__setattr__(self, "quantity", quantity); object.__setattr__(self, "requested_price", price); object.__setattr__(self, "created_at", _parse_time(self.created_at)); object.__setattr__(self, "metadata", dict(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {"intent_id": self.intent_id, "client_order_id": self.intent_id, "signal_id": self.signal_id, "symbol": self.symbol, "side": self.side.value, "quantity": self.quantity, "requested_price": self.requested_price, "created_at": _iso(self.created_at), "account_id": self.account_id, "kind": self.kind, "position_id": self.position_id, "metadata": dict(self.metadata)}


@dataclasses.dataclass(frozen=True, slots=True)
class OrderSnapshot:
    order_id: str | None
    client_order_id: str
    status: OrderState
    requested_quantity: float
    filled_quantity: float = 0.0
    fills: tuple[Fill, ...] = ()
    reject_reason: str | None = None
    position_ids: tuple[str, ...] = ()
    observed_at: datetime = dataclasses.field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        requested = float(self.requested_quantity); filled = float(self.filled_quantity)
        if requested <= 0 or filled < 0 or filled > requested + 1e-12:
            raise ValueError("order quantities invalid")
        object.__setattr__(self, "requested_quantity", requested); object.__setattr__(self, "filled_quantity", min(requested, filled)); object.__setattr__(self, "status", self.status if isinstance(self.status, OrderState) else OrderState(str(self.status).upper())); object.__setattr__(self, "fills", tuple(fill if isinstance(fill, Fill) else Fill(str(fill["fill_id"]), float(fill["quantity"]), float(fill["price"]), _parse_time(fill["timestamp"])) for fill in self.fills)); object.__setattr__(self, "observed_at", _parse_time(self.observed_at))

    def to_dict(self) -> dict[str, Any]:
        return {"order_id": self.order_id, "client_order_id": self.client_order_id, "status": self.status.value, "requested_quantity": self.requested_quantity, "filled_quantity": self.filled_quantity, "remaining_quantity": max(0.0, self.requested_quantity - self.filled_quantity), "fills": [fill.to_dict() for fill in self.fills], "reject_reason": self.reject_reason, "position_ids": list(self.position_ids), "observed_at": _iso(self.observed_at)}


@dataclasses.dataclass(frozen=True, slots=True)
class ExecutionEvent:
    event_type: str
    intent_id: str
    state: OrderState
    timestamp: datetime
    details: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"event_type": self.event_type, "intent_id": self.intent_id, "state": self.state.value, "timestamp": _iso(self.timestamp), "details": dict(self.details)}


@dataclasses.dataclass(slots=True)
class OrderResult:
    intent: ExecutionIntent
    state: OrderState
    order_id: str | None = None
    filled_quantity: float = 0.0
    fills: tuple[Fill, ...] = ()
    reject_reason: str | None = None
    position_ids: tuple[str, ...] = ()
    unknown_reason: str | None = None
    reconciled: bool = False

    @property
    def remaining_quantity(self) -> float:
        return max(0.0, self.intent.quantity - float(self.filled_quantity))

    def to_dict(self) -> dict[str, Any]:
        return {"intent": self.intent.to_dict(), "state": self.state.value, "order_id": self.order_id, "filled_quantity": self.filled_quantity, "remaining_quantity": self.remaining_quantity, "fills": [fill.to_dict() for fill in self.fills], "reject_reason": self.reject_reason, "position_ids": list(self.position_ids), "unknown_reason": self.unknown_reason, "reconciled": self.reconciled}


# ---------------------------------------------------------------------------
# Optional durable intent stores


@runtime_checkable
class IntentStore(Protocol):
    def record_intent(self, intent: Mapping[str, Any]) -> Any: ...
    def record_event(self, event: Mapping[str, Any]) -> Any: ...
    def update_intent(self, intent_id: str, update: Mapping[str, Any]) -> Any: ...


class MemoryIntentStore:
    """Deterministic fixture and safe default journal for tests."""

    def __init__(self) -> None:
        self.intents: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.updates: list[dict[str, Any]] = []
        self._lock = threading.RLock()

    def record_intent(self, intent: Mapping[str, Any]) -> None:
        with self._lock:
            key = str(intent["intent_id"])
            if key in self.intents and self.intents[key] != dict(intent):
                raise CorrelationError(f"intent already recorded with different payload: {key}")
            self.intents[key] = dict(intent)

    def record_event(self, event: Mapping[str, Any]) -> None:
        with self._lock: self.events.append(dict(event))

    def update_intent(self, intent_id: str, update: Mapping[str, Any]) -> None:
        with self._lock: self.updates.append({"intent_id": str(intent_id), **dict(update)})


class JsonlIntentStore(MemoryIntentStore):
    """Small fsync-backed journal; useful when an external DB adapter is absent."""

    def __init__(self, path: str | os.PathLike[str]):
        super().__init__(); self.path = Path(path).expanduser(); self.path.parent.mkdir(parents=True, exist_ok=True); self._stream = self.path.open("a", encoding="utf-8")

    def _write(self, record: Mapping[str, Any]) -> None:
        self._stream.write(json.dumps(dict(record), ensure_ascii=False, sort_keys=True, default=str) + "\n"); self._stream.flush(); os.fsync(self._stream.fileno())

    def record_intent(self, intent: Mapping[str, Any]) -> None:
        super().record_intent(intent); self._write({"journal_type": "intent", **dict(intent)})

    def record_event(self, event: Mapping[str, Any]) -> None:
        super().record_event(event); self._write({"journal_type": "event", **dict(event)})

    def update_intent(self, intent_id: str, update: Mapping[str, Any]) -> None:
        super().update_intent(intent_id, update); self._write({"journal_type": "update", "intent_id": intent_id, **dict(update)})

    def close(self) -> None:
        self._stream.flush(); self._stream.close()


# ---------------------------------------------------------------------------
# Transport protocol and deterministic demo fixture


@runtime_checkable
class ExecutionTransport(Protocol):
    def verify_endpoint(self, endpoint: str) -> bool: ...
    def available_scopes(self) -> frozenset[str]: ...
    def submit(self, intent: ExecutionIntent, *, timeout_seconds: float) -> OrderSnapshot: ...
    def get_order(self, client_order_id: str) -> OrderSnapshot | None: ...
    def list_positions(self, account_id: str) -> Sequence[Position]: ...
    def close_position(self, position: Position, *, client_order_id: str, timeout_seconds: float) -> OrderSnapshot: ...


class DemoTransport:
    """No-network deterministic transport for lifecycle/fixture tests."""

    def __init__(self, *, account_id: str = "demo-account", endpoint: str = "demo://ctrader", scopes: Iterable[str] = ("trading",), default_behavior: str = "full", partial_ratio: float = 0.5, clock: Callable[[], datetime] | None = None):
        self.account_id = str(account_id); self.endpoint = str(endpoint); self.scopes = _text_set(scopes); self.default_behavior = str(default_behavior).lower(); self.partial_ratio = float(partial_ratio); self.clock = clock or (lambda: datetime.now(UTC)); self.orders: dict[str, OrderSnapshot] = {}; self.positions_by_id: dict[str, Position] = {}; self.behaviors: list[str] = []; self._counter = 0; self._lock = threading.RLock()
        if not _is_demo_endpoint(self.endpoint): raise EndpointRejected("DemoTransport endpoint must be demo-only")
        if self.default_behavior not in {"full", "partial", "reject", "timeout"}: raise ValueError("unsupported demo behavior")
        if not 0 < self.partial_ratio < 1: raise ValueError("partial_ratio must be between 0 and 1")

    def verify_endpoint(self, endpoint: str) -> bool:
        return _is_demo_endpoint(endpoint) and str(endpoint) == self.endpoint

    def available_scopes(self) -> frozenset[str]: return self.scopes

    def queue_behavior(self, *behaviors: str) -> None:
        allowed = {"full", "partial", "reject", "timeout"}
        for behavior in behaviors:
            value = str(behavior).lower()
            if value not in allowed: raise ValueError(f"unsupported demo behavior: {behavior}")
            self.behaviors.append(value)

    def _next_behavior(self) -> str:
        return self.behaviors.pop(0) if self.behaviors else self.default_behavior

    def submit(self, intent: ExecutionIntent, *, timeout_seconds: float) -> OrderSnapshot:
        with self._lock:
            self._counter += 1; order_id = f"demo-order-{self._counter:06d}"; behavior = self._next_behavior(); now = _parse_time(self.clock())
            if behavior == "timeout":
                snapshot = OrderSnapshot(order_id=None, client_order_id=intent.intent_id, status=OrderState.UNKNOWN, requested_quantity=intent.quantity, observed_at=now)
                self.orders[intent.intent_id] = snapshot
                raise TimeoutError("demo transport timeout before acknowledgement")
            if behavior == "reject":
                snapshot = OrderSnapshot(order_id=order_id, client_order_id=intent.intent_id, status=OrderState.REJECTED, requested_quantity=intent.quantity, reject_reason="DEMO_REJECTED", observed_at=now); self.orders[intent.intent_id] = snapshot; return snapshot
            quantity = intent.quantity if behavior == "full" else intent.quantity * self.partial_ratio
            fill = Fill(f"{order_id}-fill-1", quantity, intent.requested_price, now)
            position_id = f"demo-position-{self._counter:06d}"
            position = Position(position_id, self.account_id, intent.symbol, intent.side, quantity, intent.requested_price, intent.intent_id)
            self.positions_by_id[position_id] = position
            state = OrderState.FILLED if quantity >= intent.quantity - 1e-12 else OrderState.PARTIAL
            snapshot = OrderSnapshot(order_id=order_id, client_order_id=intent.intent_id, status=state, requested_quantity=intent.quantity, filled_quantity=quantity, fills=(fill,), position_ids=(position_id,), observed_at=now); self.orders[intent.intent_id] = snapshot; return snapshot

    def get_order(self, client_order_id: str) -> OrderSnapshot | None:
        with self._lock: return self.orders.get(str(client_order_id))

    def advance_partial(self, client_order_id: str) -> OrderSnapshot:
        with self._lock:
            current = self.orders.get(str(client_order_id))
            if current is None: raise CorrelationError(f"unknown demo order: {client_order_id}")
            if current.status not in {OrderState.PARTIAL, OrderState.CLOSE_PARTIAL}: return current
            remaining = current.requested_quantity - current.filled_quantity; now = _parse_time(self.clock()); order_id = current.order_id or f"demo-order-recovered-{client_order_id[:8]}"; fill = Fill(f"{order_id}-fill-{len(current.fills)+1}", remaining, current.fills[-1].price if current.fills else 1.0, now); old_pos = current.position_ids[0] if current.position_ids else None
            if current.status is OrderState.PARTIAL:
                if old_pos and old_pos in self.positions_by_id:
                    p = self.positions_by_id[old_pos]; self.positions_by_id[old_pos] = dataclasses.replace(p, quantity=p.quantity + remaining)
                final_state = OrderState.FILLED
            else:
                if old_pos: self.positions_by_id.pop(old_pos, None)
                final_state = OrderState.CLOSED
            updated = OrderSnapshot(order_id=order_id, client_order_id=current.client_order_id, status=final_state, requested_quantity=current.requested_quantity, filled_quantity=current.requested_quantity, fills=(*current.fills, fill), position_ids=current.position_ids, observed_at=now); self.orders[client_order_id] = updated; return updated

    def list_positions(self, account_id: str) -> Sequence[Position]:
        with self._lock: return tuple(p for p in self.positions_by_id.values() if p.account_id == str(account_id))

    def add_foreign_position(self, *, account_id: str = "other-account", symbol: str = "EUR/USD", side: Side = Side.BUY, quantity: float = 1.0, entry_price: float = 1.0) -> str:
        with self._lock:
            self._counter += 1; pid = f"foreign-position-{self._counter:06d}"; self.positions_by_id[pid] = Position(pid, account_id, symbol, side, quantity, entry_price, owner="other-owner"); return pid

    def close_position(self, position: Position, *, client_order_id: str, timeout_seconds: float) -> OrderSnapshot:
        with self._lock:
            now = _parse_time(self.clock())
            if position.position_id not in self.positions_by_id: return OrderSnapshot(f"close-{client_order_id}", client_order_id, OrderState.REJECTED, position.quantity, reject_reason="POSITION_NOT_FOUND", observed_at=now)
            behavior = self._next_behavior()
            if behavior == "timeout":
                snapshot = OrderSnapshot(None, client_order_id, OrderState.UNKNOWN, position.quantity, observed_at=now); self.orders[client_order_id] = snapshot; raise TimeoutError("demo close timeout")
            if behavior == "reject":
                snapshot = OrderSnapshot(f"close-{client_order_id}", client_order_id, OrderState.REJECTED, position.quantity, reject_reason="DEMO_CLOSE_REJECTED", observed_at=now); self.orders[client_order_id] = snapshot; return snapshot
            if behavior == "partial":
                quantity = position.quantity * self.partial_ratio; fill = Fill(f"close-{client_order_id}-fill-1", quantity, position.entry_price, now); self.positions_by_id[position.position_id] = dataclasses.replace(position, quantity=position.quantity - quantity); snapshot = OrderSnapshot(f"close-{client_order_id}", client_order_id, OrderState.CLOSE_PARTIAL, position.quantity, quantity, (fill,), position_ids=(position.position_id,), observed_at=now); self.orders[client_order_id] = snapshot; return snapshot
            self.positions_by_id.pop(position.position_id, None); fill = Fill(f"close-{client_order_id}-fill", position.quantity, position.entry_price, now); snapshot = OrderSnapshot(f"close-{client_order_id}", client_order_id, OrderState.CLOSED, position.quantity, position.quantity, (fill,), position_ids=(position.position_id,), observed_at=now); self.orders[client_order_id] = snapshot; return snapshot


# ---------------------------------------------------------------------------
# Safety and executor state machine


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime): dt = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool): dt = datetime.fromtimestamp(float(value), UTC)
    else:
        text = str(value).strip(); text = text[:-1] + "+00:00" if text.endswith("Z") else text; dt = datetime.fromisoformat(text)
    if dt.tzinfo is None or dt.utcoffset() is None: raise ValueError("timestamp must include timezone")
    return dt.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return _parse_time(value).isoformat(timespec="microseconds").replace("+00:00", "Z") if value is not None else None


def _is_demo_endpoint(endpoint: str) -> bool:
    text = str(endpoint).strip().lower()
    if any(token in text for token in ("real", "live", "production")): return False
    return text.startswith("demo:") or "demo" in text or "sandbox" in text or "paper" in text


def _scope_satisfied(required: str, available: frozenset[str]) -> bool:
    value = required.lower()
    aliases = {value, value.replace(":read", ""), value.replace("read:", ""), value.replace("_", ":")}
    normalized_available = {str(item).lower() for item in available}
    if value == "trading": aliases |= {"trade", "trading:write", "trading:read"}
    return bool(aliases & normalized_available)


def verify_demo_account(account: DemoAccount | Mapping[str, Any], *, required_scopes: Iterable[str] = ("trading",), endpoint: str | None = None) -> DemoAccount:
    """Verify demo-only selection; REAL is rejected before other gates."""
    if not isinstance(account, DemoAccount):
        account = DemoAccount.from_mapping(account)
    # This check deliberately precedes account id, endpoint, token or scope
    # checks so malformed REAL configuration cannot reach a real transport.
    if account.environment in {"REAL", "LIVE", "PRODUCTION", "PAPER_LIVE"} or "REAL" in account.environment or "LIVE" in account.environment:
        raise RealAccountForbidden("REAL/LIVE account execution is forbidden by the demo executor")
    if account.environment not in {"DEMO", "SANDBOX", "PAPER"}:
        raise DemoAccountRequired("a selected DEMO account is required")
    target_endpoint = endpoint or account.endpoint
    if not _is_demo_endpoint(target_endpoint):
        raise EndpointRejected("endpoint must be explicitly demo/sandbox/paper")
    if not account.account_id or not account.selected or not account.verified:
        raise DemoAccountRequired("account_id, selected=true and verified=true are required")
    missing = [scope for scope in required_scopes if not _scope_satisfied(str(scope), account.scopes)]
    if missing: raise ScopeRejected(f"missing required demo scopes: {sorted(missing)}")
    return account


class CTraderDemoExecutor:
    """Fail-closed DEMO executor; fixture and official gateways stay separate."""

    VERSION = "ctrader-demo-executor-v1"

    def __init__(self, account: DemoAccount | Mapping[str, Any], *, policy: ExecutionPolicy | None = None, transport: ExecutionTransport | None = None, intent_store: IntentStore | None = None, clock: Callable[[], datetime] | None = None, server_observation: Any | None = None):
        # Precheck REAL using the raw mapping before constructing any transport.
        raw_environment = str(account.get("environment", account.get("mode", "")) if isinstance(account, Mapping) else getattr(account, "environment", "")).strip().upper()
        if "REAL" in raw_environment or "LIVE" in raw_environment or raw_environment in {"PRODUCTION", "PAPER_LIVE"}:
            raise RealAccountForbidden("REAL/LIVE account execution is forbidden by the demo executor")
        if transport is not None and not isinstance(transport, DemoTransport) and server_observation is None:
            raise DemoAccountRequired("external transport requires ServerAccountObservation")
        if server_observation is not None:
            # An external executor may derive verified=True only from the
            # normalized server proof object, never from a caller flag.
            from .ctrader_demo_transport import ServerAccountObservation
            if not isinstance(server_observation, ServerAccountObservation):
                raise DemoAccountRequired("server_observation must be a normalized ServerAccountObservation")
            account_obj = account if isinstance(account, DemoAccount) else DemoAccount.from_mapping(account)
            if server_observation.account_id != account_obj.account_id or server_observation.environment != account_obj.environment or server_observation.endpoint != account_obj.endpoint:
                raise DemoAccountRequired("server observation does not match selected account")
            observed_scopes = set(server_observation.scopes)
            if "scope_trade" in observed_scopes or "trade" in observed_scopes:
                observed_scopes.add("trading")
            account = dataclasses.replace(
                account_obj,
                verified=True,
                scopes=frozenset(set(account_obj.scopes) | observed_scopes),
            )
        self.policy = policy or ExecutionPolicy()
        self.account = verify_demo_account(account, required_scopes=self.policy.required_scopes)
        self.transport = transport or DemoTransport(account_id=self.account.account_id, endpoint=self.account.endpoint, scopes=self.account.scopes, clock=clock)
        transport_account = getattr(self.transport, "account_id", None)
        if transport_account is not None and str(transport_account) != self.account.account_id:
            raise DemoAccountRequired("transport account does not match the selected demo account")
        transport_endpoint = getattr(self.transport, "endpoint", self.account.endpoint)
        if not _is_demo_endpoint(str(transport_endpoint)):
            raise EndpointRejected("transport endpoint must be demo-only")
        if not self.transport.verify_endpoint(self.account.endpoint): raise EndpointRejected("transport endpoint does not match selected demo endpoint")
        available_scopes = self.transport.available_scopes()
        missing = [scope for scope in self.policy.required_scopes if not _scope_satisfied(str(scope), available_scopes)]
        if missing: raise ScopeRejected(f"transport missing required scopes: {sorted(missing)}")
        self.intent_store: IntentStore = intent_store or MemoryIntentStore()
        self.clock = clock or (lambda: datetime.now(UTC))
        self._virtual_only = isinstance(self.transport, DemoTransport)
        self._active = False
        self._paused = False
        self._pause_reason: str | None = None
        self._intents: dict[str, ExecutionIntent] = {}
        self._signal_intents: dict[str, str] = {}
        self._results: dict[str, OrderResult] = {}
        self._events: list[ExecutionEvent] = []
        self._lock = threading.RLock()

    @property
    def active(self) -> bool: return self._active
    @property
    def paused(self) -> bool: return self._paused

    def activate(self) -> dict[str, Any]:
        """Explicit human/config gate; construction alone never enables send."""
        with self._lock:
            verify_demo_account(self.account, required_scopes=self.policy.required_scopes, endpoint=self.account.endpoint)
            if not self.transport.verify_endpoint(self.account.endpoint): raise EndpointRejected("demo endpoint verification failed")
            self._emit("ACTIVATED", "INTENT_RECORDED", "activation", {"account_id": self.account.account_id})
            self._active = True
            return self.status()

    def deactivate(self, reason: str = "manual") -> None:
        with self._lock: self._active = False; self._pause_reason = str(reason)

    def pause(self, reason: str = "manual") -> None:
        with self._lock: self._paused = True; self._pause_reason = str(reason); self._emit("PAUSED", "UNKNOWN", "pause", {"reason": self._pause_reason})

    def resume(self) -> None:
        with self._lock: self._paused = False; self._pause_reason = None; self._emit("RESUMED", "UNKNOWN", "resume", {})

    def _emit(self, event_type: str, state: str | OrderState, intent_id: str = "", details: Mapping[str, Any] | None = None) -> ExecutionEvent:
        normalized = state if isinstance(state, OrderState) else (OrderState(str(state)) if str(state) in {x.value for x in OrderState} else OrderState.UNKNOWN)
        event = ExecutionEvent(event_type, str(intent_id), normalized, _parse_time(self.clock()), details or {})
        self._events.append(event)
        _store_call(self.intent_store, "record_event", event.to_dict())
        return event

    def _check_quote(self, symbol: str, quote: Quote, side: Side) -> None:
        if quote.symbol != symbol.upper(): raise RiskLimitRejected("quote symbol differs from signal")
        if self.policy.allowed_symbols and symbol.upper() not in self.policy.allowed_symbols: raise RiskLimitRejected("symbol is not allowlisted")
        blocked = ("UNKNOWN", "STALE", "DISCONNECTED", "INVALID", "GAP", "UNRECONCILED", "OPEN", "LATE")
        if any(token in quote.quality.upper() for token in blocked): raise RiskLimitRejected(f"quote quality blocks execution: {quote.quality}")
        age = (_parse_time(self.clock()) - (quote.available_at or quote.timestamp)).total_seconds()
        if age < 0 or age > self.policy.max_price_age_seconds: raise RiskLimitRejected(f"quote is stale: age={age:.3f}s")
        if quote.spread > self.policy.max_spread: raise RiskLimitRejected(f"spread cap exceeded: {quote.spread} > {self.policy.max_spread}")

    def _own_positions(self) -> tuple[Position, ...]:
        positions = tuple(position for position in self.transport.list_positions(self.account.account_id) if position.account_id == self.account.account_id)
        if not self.policy.close_only_own_positions:
            return positions
        # A position is owned only when it was correlated to an intent created
        # by this executor. The owner marker is an additional fixture guard;
        # account-id equality alone is not sufficient to close a user's other
        # demo position.
        return tuple(position for position in positions if position.owner == "mtf-lab" and position.client_order_id in self._intents)

    def _resolve_quantity(self, requested: float | None, *, kind: str = "OPEN") -> float:
        quantity = self.policy.fixed_quantity if requested is None and kind == "OPEN" else requested
        if quantity is None or not math.isfinite(float(quantity)) or float(quantity) <= 0: raise RiskLimitRejected("quantity must be positive and finite")
        quantity = float(quantity)
        if kind == "OPEN":
            if quantity > self.policy.max_quantity: raise RiskLimitRejected("quantity cap exceeded")
            if self.policy.fixed_quantity is not None and abs(quantity - self.policy.fixed_quantity) > 1e-12: raise RiskLimitRejected("fixed quantity policy rejects variable sizing")
        return quantity

    def _make_intent(self, signal: Any, quote: Quote, quantity: float | None, *, kind: str = "OPEN", position_id: str | None = None) -> ExecutionIntent:
        data = _as_mapping(signal)
        signal_id = str(data.get("signal_id", data.get("id", "")))
        symbol = str(data.get("instrument", data.get("symbol", quote.symbol))).upper()
        if not signal_id: raise ExecutionError("signal_id is required")
        mode = str(data.get("mode", "")).upper()
        if mode in {"REAL", "LIVE", "PRODUCTION"}: raise RealAccountForbidden("REAL/LIVE signal cannot reach demo executor")
        side = Side.parse(data.get("side", data.get("direction")))
        q = self._resolve_quantity(quantity, kind=kind)
        self._check_quote(symbol, quote, side)
        own = self._own_positions()
        if kind == "OPEN":
            unresolved = [result for result in self._results.values() if result.state in {OrderState.UNKNOWN, OrderState.SUBMITTED, OrderState.CLOSE_PARTIAL}]
            if unresolved: raise RiskLimitRejected("unresolved order must be reconciled before opening another intent")
            if len(own) >= self.policy.max_positions: raise RiskLimitRejected("max_positions cap exceeded")
            exposure = sum(position.notional for position in own) + q * quote.price_for(side)
            if exposure > self.policy.max_exposure + 1e-12: raise RiskLimitRejected("max_exposure cap exceeded")
        if kind == "OPEN" and signal_id in self._signal_intents:
            raise DuplicateIntent(f"signal already has an execution intent: {signal_id}")
        nonce = f"{signal_id}|{self.account.account_id}|{symbol}|{side.value}|{kind}|{position_id or ''}"
        intent_id = "intent_" + hashlib.sha256(nonce.encode()).hexdigest()[:32]
        if intent_id in self._intents or intent_id in self._results: raise DuplicateIntent(f"intent already exists: {intent_id}")
        return ExecutionIntent(intent_id, signal_id, symbol, side, q, quote.price_for(side), _parse_time(self.clock()), self.account.account_id, kind, position_id, {"executor_version": self.VERSION, "endpoint": self.account.endpoint, "quote": quote.to_dict(), "no_martingale": self.policy.no_martingale, "virtual_only": self._virtual_only})

    def submit_signal(self, signal: Any, quote: Quote | Mapping[str, Any], *, quantity: float | None = None) -> OrderResult:
        with self._lock:
            if not self._active: raise ActivationRequired("call activate() explicitly before demo execution")
            if self._paused: raise ExecutionPaused(f"new intents paused: {self._pause_reason or 'manual'}")
            quote_obj = quote if isinstance(quote, Quote) else Quote.from_mapping(quote)
            intent = self._make_intent(signal, quote_obj, quantity)
            # This call must complete before submit is attempted. A store error
            # therefore fails closed and no transport side effect occurs.
            _store_call(self.intent_store, "record_intent", intent.to_dict())
            self._intents[intent.intent_id] = intent
            if intent.kind == "OPEN": self._signal_intents[intent.signal_id] = intent.intent_id
            self._emit("INTENT_RECORDED", OrderState.INTENT_RECORDED, intent.intent_id, {"correlation_id": intent.intent_id, "before_transport": True})
            try:
                snapshot = self.transport.submit(intent, timeout_seconds=self.policy.timeout_seconds)
            except TimeoutError as exc:
                result = OrderResult(intent, OrderState.UNKNOWN, unknown_reason=f"transport_timeout: {exc}")
                self._results[intent.intent_id] = result; self._update(intent, result); self._emit("SUBMIT_TIMEOUT", OrderState.UNKNOWN, intent.intent_id, {"no_blind_retry": True, "correlation_id": intent.intent_id}); return result
            result = self._result_from_snapshot(intent, snapshot)
            self._results[intent.intent_id] = result; self._update(intent, result); return result

    def _result_from_snapshot(self, intent: ExecutionIntent, snapshot: OrderSnapshot, *, reconciled: bool = False) -> OrderResult:
        if snapshot.client_order_id != intent.intent_id:
            raise CorrelationError(f"transport response correlation mismatch: expected {intent.intent_id}, got {snapshot.client_order_id}")
        if abs(snapshot.requested_quantity - intent.quantity) > 1e-12:
            raise CorrelationError(f"transport quantity mismatch for {intent.intent_id}: expected {intent.quantity}, got {snapshot.requested_quantity}")
        state = snapshot.status
        if state is OrderState.SUBMITTED and snapshot.filled_quantity > 0: state = OrderState.PARTIAL if snapshot.filled_quantity < intent.quantity else OrderState.FILLED
        if state is OrderState.PARTIAL and snapshot.filled_quantity <= 0: state = OrderState.SUBMITTED
        result = OrderResult(intent, state, snapshot.order_id, snapshot.filled_quantity, snapshot.fills, snapshot.reject_reason, snapshot.position_ids, reconciled=reconciled)
        self._emit("ORDER_UPDATE", state, intent.intent_id, {"order_id": snapshot.order_id, "filled_quantity": snapshot.filled_quantity, "reconciled": reconciled})
        return result

    def _update(self, intent: ExecutionIntent, result: OrderResult) -> None:
        _store_call(self.intent_store, "update_intent", intent.intent_id, result.to_dict())

    def reconcile(self, intent_id: str) -> OrderResult:
        with self._lock:
            key = str(intent_id)
            if key not in self._intents: raise CorrelationError(f"unknown intent: {key}")
            existing = self._results.get(key)
            if existing and existing.state in _TERMINAL_STATES: return existing
            intent = self._intents[key]
            try: snapshot = self.transport.get_order(intent.intent_id)
            except TimeoutError as exc:
                if existing: existing.unknown_reason = f"reconcile_timeout: {exc}"; return existing
                return OrderResult(intent, OrderState.UNKNOWN, unknown_reason=f"reconcile_timeout: {exc}")
            if snapshot is None:
                result = existing or OrderResult(intent, OrderState.UNKNOWN, unknown_reason="order_not_found_for_correlation")
                result.reconciled = True; self._results[key] = result; self._update(intent, result); self._emit("RECONCILIATION_UNRESOLVED", OrderState.UNKNOWN, key, {"no_blind_retry": True}); return result
            result = self._result_from_snapshot(intent, snapshot, reconciled=True); self._results[key] = result; self._update(intent, result); return result

    def manage(self) -> tuple[OrderResult, ...]:
        """Poll unresolved/partial orders; works while new submissions are paused."""
        with self._lock: keys = [key for key, result in self._results.items() if result.state in {OrderState.UNKNOWN, OrderState.PARTIAL, OrderState.CLOSE_PARTIAL, OrderState.SUBMITTED}]
        return tuple(self.reconcile(key) for key in keys)

    def positions(self) -> tuple[Position, ...]:
        return self._own_positions()

    def close_position(self, position_id: str | Position) -> OrderResult:
        with self._lock:
            requested_position_id = position_id.position_id if isinstance(position_id, Position) else str(position_id)
            position = next((item for item in self._own_positions() if item.position_id == requested_position_id), None)
            if position is None:
                # Even while paused, management must not touch a foreign id.
                raise ForeignPosition(f"position is not owned by selected demo account: {requested_position_id}")
            if not self._active: raise ActivationRequired("demo executor is not activated")
            signal = {"signal_id": f"close:{position.position_id}", "instrument": position.symbol, "direction": "SELL" if position.side is Side.BUY else "BUY"}
            # A close intent uses the known entry price as an explicit fixture
            # quote; a real connector would require a fresh bid/ask quote.
            quote = Quote(position.symbol, position.entry_price, position.entry_price, _parse_time(self.clock()), quality="VALID", source="demo-close")
            intent = self._make_intent(signal, quote, position.quantity, kind="CLOSE", position_id=position.position_id)
            _store_call(self.intent_store, "record_intent", intent.to_dict()); self._intents[intent.intent_id] = intent; self._emit("INTENT_RECORDED", OrderState.INTENT_RECORDED, intent.intent_id, {"kind": "CLOSE", "position_id": position.position_id, "before_transport": True})
            try: snapshot = self.transport.close_position(position, client_order_id=intent.intent_id, timeout_seconds=self.policy.timeout_seconds)
            except TimeoutError as exc:
                result = OrderResult(intent, OrderState.UNKNOWN, unknown_reason=f"close_timeout: {exc}"); self._results[intent.intent_id] = result; self._update(intent, result); return result
            result = self._result_from_snapshot(intent, snapshot); self._results[intent.intent_id] = result; self._update(intent, result); return result

    def result(self, intent_id: str) -> OrderResult | None: return self._results.get(str(intent_id))
    def events(self) -> tuple[ExecutionEvent, ...]: return tuple(self._events)
    def execute(self, signal: Any, quote: Quote | Mapping[str, Any], quantity: float | None = None) -> OrderResult:
        return self.submit_signal(signal, quote, quantity=quantity)
    def submit(self, signal: Any, quote: Quote | Mapping[str, Any], quantity: float | None = None) -> OrderResult:
        return self.submit_signal(signal, quote, quantity=quantity)
    def reconcile_order(self, intent_id: str) -> OrderResult:
        return self.reconcile(intent_id)
    def close(self, position_id: str | Position) -> OrderResult:
        return self.close_position(position_id)

    def status(self) -> dict[str, Any]:
        with self._lock:
            counts: MutableMapping[str, int] = {state.value: 0 for state in OrderState}
            for result in self._results.values(): counts[result.state.value] = counts.get(result.state.value, 0) + 1
            return {"executor_version": self.VERSION, "environment": self.account.environment, "endpoint": self.account.endpoint, "account_id": self.account.account_id, "active": self._active, "paused": self._paused, "pause_reason": self._pause_reason, "new_intents_enabled": self._active and not self._paused, "virtual_only": self._virtual_only, "counts": dict(counts), "pending_management": sum(counts.get(state.value, 0) for state in (OrderState.UNKNOWN, OrderState.PARTIAL, OrderState.CLOSE_PARTIAL, OrderState.SUBMITTED)), "own_positions": [position.to_dict() for position in self._own_positions()], "risk_policy": self.policy.to_dict(), "no_blind_retry": True}


# Friendly aliases for a future adapter registry.
CTraderExecutor = CTraderDemoExecutor
DemoExecutionTransport = DemoTransport
SimulatedCTraderTransport = DemoTransport
CTraderDemoTransport = DemoTransport
DemoExecutionConfig = ExecutionPolicy


# ---------------------------------------------------------------------------
# Generic adapter helpers


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping): return dict(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type): return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    if hasattr(value, "to_dict") and callable(value.to_dict): return dict(value.to_dict())
    if hasattr(value, "as_dict") and callable(value.as_dict): return dict(value.as_dict())
    if hasattr(value, "model_dump") and callable(value.model_dump): return dict(value.model_dump())
    if hasattr(value, "__dict__"): return {key: val for key, val in vars(value).items() if not key.startswith("_")}
    raise TypeError(f"expected mapping/dataclass/object, got {type(value)!r}")


def _store_call(store: Any, method: str, *args: Any) -> Any:
    """Call one of a small set of optional journal method spellings."""
    candidates = {"record_intent": ("record_intent", "record_execution_intent", "persist_intent", "persist_execution_intent", "save_intent", "save_execution_intent"), "record_event": ("record_event", "record_execution_event", "append_event", "save_event"), "update_intent": ("update_intent", "update_execution_intent", "persist_update", "save_intent_update")}[method]
    for name in candidates:
        callback = getattr(store, name, None)
        if callable(callback): return callback(*args)
    raise TypeError(f"intent_store does not implement {method}: expected one of {candidates}")


__all__ = [
    "ActivationRequired", "CTraderDemoExecutor", "CTraderExecutor", "CorrelationError", "DemoAccount", "DemoAccountRequired", "DemoExecutionConfig", "DemoExecutionTransport", "DemoTransport", "SimulatedCTraderTransport", "CTraderDemoTransport", "DuplicateIntent", "EndpointRejected", "ExecutionError", "ExecutionEvent", "ExecutionIntent", "ExecutionPolicy", "ExecutionTransport", "Fill", "ForeignPosition", "IntentStore", "JsonlIntentStore", "MemoryIntentStore", "OrderResult", "OrderSnapshot", "OrderState", "Position", "Quote", "QuoteQuality", "RealAccountForbidden", "RiskLimitRejected", "SafetyViolation", "ScopeRejected", "Side", "ExecutionPaused", "verify_demo_account",
]
