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

import contextlib
import dataclasses
import enum
import fcntl
import hashlib
import json
import os
import stat
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Context, Decimal, InvalidOperation
from pathlib import Path
from types import NotImplementedType
from typing import Any, Protocol, cast, runtime_checkable

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


class RecoveryRequired(ExecutionPaused):
    """A durable journal must be recovered before another intent is admitted."""


class RiskLimitRejected(ExecutionError):
    pass


class DuplicateIntent(ExecutionError):
    pass


class ForeignPosition(ExecutionError):
    pass


class CorrelationError(ExecutionError):
    pass


_UNSET = object()
_STRUCTURAL_RISK_HALTS = frozenset({"foreign_positions_present", "position_reconciliation_failed"})
_LATCHED_RISK_BREACHES = frozenset({"max_daily_loss_exceeded", "max_drawdown_exceeded", "min_margin_level_breached"})
_PROTECTION_RISK_HALT = "protection_unverified"
_HOLDING_RISK_HALT = "opened_at_unobserved"
_HOLDING_FUTURE_HALT = "opened_at_future"


class SendPhase(str, enum.Enum):  # noqa: UP042 - preserve public string enum behavior
    """Shared send lifecycle phase used by the executor and adapters."""

    NOT_SENT = "NOT_SENT"
    CANCELLED_BEFORE_SEND = "CANCELLED_BEFORE_SEND"
    SENT = "SENT"
    RESPONSE_RECEIVED = "RESPONSE_RECEIVED"
    SENT_NO_RESPONSE = "SENT_NO_RESPONSE"
    LOCAL_ERROR = "LOCAL_ERROR"


class ExecutionLocalFailure(ExecutionError):
    """A local validation/configuration failure with no remote outcome."""

    def __init__(self, message: str, *, phase: SendPhase = SendPhase.LOCAL_ERROR) -> None:
        self.phase = phase
        super().__init__(message)

    @property
    def uncertain(self) -> bool:
        return False


class ExecutionTransportFailure(ExecutionError):
    """Failure carrying whether a remote outcome may be uncertain."""

    def __init__(self, message: str, *, phase: SendPhase = SendPhase.SENT) -> None:
        self.phase = phase
        super().__init__(message)

    @property
    def uncertain(self) -> bool:
        return self.phase in {SendPhase.SENT, SendPhase.RESPONSE_RECEIVED, SendPhase.SENT_NO_RESPONSE}


class ServerObservationProof:
    """Leaf proof boundary shared by executor and adapters.

    The executor depends on this narrow type instead of importing the
    transport module back, keeping the domain state machine acyclic. Concrete
    observations must implement ``matches_account`` and adapters add the
    session/generation provenance checks appropriate to their boundary.
    """

    __slots__ = ()

    def matches_account(self, account: DemoAccount) -> bool:
        raise NotImplementedError

    def valid_at(self, when: datetime) -> bool:
        """Return whether this proof is still valid at ``when`` when supported."""

        del when
        return True


class DecimalValue(Decimal):
    """Decimal value with a compatibility shim for legacy float callers.

    cTrader volumes are integer protocol units, while the public executor API
    historically accepted floats. Values are parsed from their textual form
    and remain Decimal-backed; arithmetic methods only coerce a legacy float
    through ``str`` so ``0.10`` does not become a binary approximation. The
    shim is local to this module and never changes the process-wide context.
    """

    def __new__(cls, value: Any = "0") -> DecimalValue:
        if isinstance(value, float):
            value = repr(value)
        return super().__new__(cls, value)

    @staticmethod
    def _operand(value: Any) -> Decimal | NotImplementedType:
        if isinstance(value, bool):
            raise TypeError("boolean is not a decimal value")
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (int, float, str)):
            return DecimalValue(value)
        return cast(Decimal | NotImplementedType, NotImplemented)

    @classmethod
    def _result(cls, value: Decimal) -> DecimalValue:
        return cls(str(value))

    def __add__(self, other: Any) -> DecimalValue:
        operand = self._operand(other)
        if isinstance(operand, NotImplementedType):
            return NotImplemented
        return self._result(_DECIMAL_CONTEXT.add(self, operand))

    def __radd__(self, other: Any) -> DecimalValue:
        return self.__add__(other)

    def __sub__(self, other: Any) -> DecimalValue:
        operand = self._operand(other)
        if isinstance(operand, NotImplementedType):
            return NotImplemented
        return self._result(_DECIMAL_CONTEXT.subtract(self, operand))

    def __rsub__(self, other: Any) -> DecimalValue:
        operand = self._operand(other)
        if isinstance(operand, NotImplementedType):
            return NotImplemented
        return self._result(_DECIMAL_CONTEXT.subtract(operand, self))

    def __mul__(self, other: Any) -> DecimalValue:
        operand = self._operand(other)
        if isinstance(operand, NotImplementedType):
            return NotImplemented
        return self._result(_DECIMAL_CONTEXT.multiply(self, operand))

    def __rmul__(self, other: Any) -> DecimalValue:
        return self.__mul__(other)

    def __truediv__(self, other: Any) -> DecimalValue:
        operand = self._operand(other)
        if isinstance(operand, NotImplementedType):
            return NotImplemented
        return self._result(_DECIMAL_CONTEXT.divide(self, operand))

    def __rtruediv__(self, other: Any) -> DecimalValue:
        operand = self._operand(other)
        if isinstance(operand, NotImplementedType):
            return NotImplemented
        return self._result(_DECIMAL_CONTEXT.divide(operand, self))

    def __abs__(self) -> DecimalValue:
        return self._result(_DECIMAL_CONTEXT.copy_abs(self))


_DECIMAL_CONTEXT = Context(prec=34, rounding=ROUND_HALF_EVEN)
_DECIMAL_TOLERANCE = DecimalValue("0.000000000001")


def _decimal_value(value: Any, name: str, *, positive: bool = False) -> DecimalValue:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a decimal number")
    try:
        parsed = DecimalValue(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a decimal number") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError(f"{name} must be finite" + (" and positive" if positive else ""))
    return parsed


def _decimal_text(value: DecimalValue | Decimal | Any) -> str:
    """Stable persistence representation for an exact quantity or price."""

    return format(DecimalValue(value), "f")


def _optional_decimal(value: Any, name: str) -> DecimalValue | None:
    return None if value is None else _decimal_value(value, name, positive=True)


class OrderState(str, enum.Enum):  # noqa: UP042 - preserve public string enum behavior
    INTENT_RECORDED = "INTENT_RECORDED"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    CLOSED = "CLOSED"
    CLOSE_PARTIAL = "CLOSE_PARTIAL"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


_TERMINAL_STATES = frozenset(
    {OrderState.FILLED, OrderState.REJECTED, OrderState.CLOSED, OrderState.CANCELLED, OrderState.EXPIRED}
)


class Side(str, enum.Enum):  # noqa: UP042 - preserve public string enum behavior
    BUY = "BUY"
    SELL = "SELL"

    @property
    def opposite(self) -> Side:
        return Side.SELL if self is Side.BUY else Side.BUY

    @classmethod
    def parse(cls, value: Any) -> Side:
        text = str(getattr(value, "value", value)).strip().upper()
        if text in {"BUY", "LONG", "UP", "ALCISTA"}:
            return cls.BUY
        if text in {"SELL", "SHORT", "DOWN", "BAJISTA"}:
            return cls.SELL
        raise ValueError(f"direction/side not supported: {value!r}")


class QuoteQuality(str, enum.Enum):  # noqa: UP042 - preserve public string enum behavior
    VALID = "VALID"
    SYNTHETIC = "SYNTHETIC"
    UNKNOWN = "UNKNOWN"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    INVALID = "INVALID"


def _text_set(value: Iterable[str] | str) -> frozenset[str]:
    if isinstance(value, str):
        return frozenset({value.strip().lower()})
    return frozenset(str(item).strip().lower() for item in value)


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in {0, 1}:
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "si", "sí", "selected", "verified"}


def _positive_protocol_int(value: Any, name: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise RiskLimitRejected(f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RiskLimitRejected(f"{name} must be an integer") from exc
    if parsed < 0 or (parsed == 0 and not allow_zero):
        raise RiskLimitRejected(f"{name} must be {'non-negative' if allow_zero else 'positive'}")
    return parsed


_ORDER_OPTION_ALIASES = {
    "relativeStopLoss": "relative_stop_loss",
    "relativeTakeProfit": "relative_take_profit",
    "stopLoss": "stop_loss",
    "takeProfit": "take_profit",
    "expirationTimestamp": "expiration_timestamp",
    "slippageInPoints": "slippage_in_points",
    "timeInForce": "time_in_force",
    "trailingStopLoss": "trailing_stop_loss",
    "guaranteedStopLoss": "guaranteed_stop_loss",
}
_ORDER_OPTION_ALLOWED = frozenset(
    {
        "relative_stop_loss",
        "relativeStopLoss",
        "relative_take_profit",
        "relativeTakeProfit",
        "stop_loss",
        "stopLoss",
        "take_profit",
        "takeProfit",
        "expiration_timestamp",
        "expirationTimestamp",
        "slippage_in_points",
        "slippageInPoints",
        "time_in_force",
        "timeInForce",
        "comment",
        "trailing_stop_loss",
        "trailingStopLoss",
        "guaranteed_stop_loss",
        "guaranteedStopLoss",
    }
)


def _raw_order_options(data: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = data.get("order_options", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        raise RiskLimitRejected("order_options must be a mapping")
    direct = {key: data[key] for key in _ORDER_OPTION_ALLOWED if key in data}
    merged = {**dict(raw), **direct}
    unknown = set(merged) - _ORDER_OPTION_ALLOWED
    if unknown:
        raise RiskLimitRejected(f"unsupported order option(s): {sorted(unknown)}")
    return merged


def _canonical_order_options(raw: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in raw.items():
        canonical = _ORDER_OPTION_ALIASES.get(str(key), str(key))
        if canonical in result and result[canonical] != value:
            raise RiskLimitRejected(f"duplicate order option: {canonical}")
        result[canonical] = value
    return result


def _apply_protection_defaults(result: dict[str, Any], policy: ExecutionPolicy) -> None:
    for key, default in (
        ("relative_stop_loss", policy.relative_stop_loss),
        ("relative_take_profit", policy.relative_take_profit),
    ):
        if key not in result and default is not None:
            result[key] = default


def _validate_relative_options(result: dict[str, Any], policy: ExecutionPolicy) -> None:
    for key in ("relative_stop_loss", "relative_take_profit"):
        if key in result and result[key] is not None:
            result[key] = _positive_protocol_int(result[key], key)
    for key, maximum in (
        ("relative_stop_loss", policy.max_relative_stop_loss),
        ("relative_take_profit", policy.max_relative_take_profit),
    ):
        if key in result and result[key] is not None and maximum is not None and result[key] > maximum:
            raise RiskLimitRejected(f"{key} exceeds configured maximum")


def _normalize_price_options(result: dict[str, Any]) -> None:
    for key in ("stop_loss", "take_profit"):
        if key in result and result[key] is not None:
            result[key] = _decimal_text(_decimal_value(result[key], key, positive=True))


def _normalize_expiration(result: dict[str, Any]) -> None:
    if "expiration_timestamp" in result and result["expiration_timestamp"] is not None:
        result["expiration_timestamp"] = _positive_protocol_int(
            result["expiration_timestamp"], "expiration_timestamp", allow_zero=False
        )


def _normalize_slippage(result: dict[str, Any], policy: ExecutionPolicy) -> None:
    if "slippage_in_points" not in result or result["slippage_in_points"] is None:
        return
    result["slippage_in_points"] = _positive_protocol_int(
        result["slippage_in_points"], "slippage_in_points", allow_zero=True
    )
    if policy.max_slippage_points is not None and result["slippage_in_points"] > policy.max_slippage_points:
        raise RiskLimitRejected("slippage_in_points exceeds configured maximum")


def _normalize_boolean_and_comment_options(result: dict[str, Any]) -> None:
    for key in ("trailing_stop_loss", "guaranteed_stop_loss"):
        if key in result and not isinstance(result[key], bool):
            raise RiskLimitRejected(f"{key} must be boolean")
    if "comment" in result and result["comment"] is not None:
        result["comment"] = str(result["comment"])
        if len(result["comment"]) > 128:
            raise RiskLimitRejected("comment is too long")


def _order_options(data: Mapping[str, Any], policy: ExecutionPolicy) -> dict[str, Any]:
    result = _canonical_order_options(_raw_order_options(data))
    _apply_protection_defaults(result, policy)
    _validate_relative_options(result, policy)
    _normalize_price_options(result)
    _normalize_expiration(result)
    _normalize_slippage(result, policy)
    _normalize_boolean_and_comment_options(result)
    return result


def _has_protective_stop(data: Mapping[str, Any], policy: ExecutionPolicy) -> bool:
    options = _order_options(data, policy)
    return any(options.get(key) is not None for key in ("relative_stop_loss", "stop_loss"))


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
    def from_mapping(cls, value: Mapping[str, Any]) -> DemoAccount:
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


def _normalize_policy_decimals(policy: Any) -> None:
    for name in (
        "max_quantity",
        "max_exposure",
        "max_spread",
        "max_price_age_seconds",
        "timeout_seconds",
        "order_window_seconds",
    ):
        value = _decimal_value(getattr(policy, name), name)
        if value < 0:
            raise ValueError(f"{name} must be finite and non-negative")
        object.__setattr__(policy, name, value)


def _validate_policy_quantities(policy: Any) -> None:
    if policy.max_quantity <= 0 or policy.max_exposure <= 0 or policy.timeout_seconds <= 0:
        raise ValueError("quantity, exposure and timeout caps must be positive")
    if policy.close_timeout_seconds is not None:
        close_timeout = _decimal_value(policy.close_timeout_seconds, "close_timeout_seconds", positive=True)
        object.__setattr__(policy, "close_timeout_seconds", close_timeout)
    if policy.max_holding_seconds is not None:
        holding = _decimal_value(policy.max_holding_seconds, "max_holding_seconds", positive=True)
        object.__setattr__(policy, "max_holding_seconds", holding)
    if policy.fixed_quantity is not None:
        fixed = _decimal_value(policy.fixed_quantity, "fixed_quantity", positive=True)
        if fixed > policy.max_quantity:
            raise ValueError("fixed_quantity must be positive and <= max_quantity")
        object.__setattr__(policy, "fixed_quantity", fixed)


def _validate_policy_counts(policy: Any) -> None:
    if isinstance(policy.max_positions, bool) or int(policy.max_positions) < 1:
        raise ValueError("max_positions must be a positive integer")
    object.__setattr__(policy, "max_positions", int(policy.max_positions))
    if policy.order_window_seconds <= 0:
        raise ValueError("order_window_seconds must be positive")
    if isinstance(policy.max_inflight_intents, bool) or int(policy.max_inflight_intents) < 1:
        raise ValueError("max_inflight_intents must be a positive integer")
    object.__setattr__(policy, "max_inflight_intents", int(policy.max_inflight_intents))
    if policy.max_orders_per_window is not None:
        if isinstance(policy.max_orders_per_window, bool) or int(policy.max_orders_per_window) < 1:
            raise ValueError("max_orders_per_window must be a positive integer or None")
        object.__setattr__(policy, "max_orders_per_window", int(policy.max_orders_per_window))


def _validate_policy_flags(policy: Any) -> None:
    if not isinstance(policy.no_martingale, bool) or not isinstance(policy.close_only_own_positions, bool):
        raise ValueError("risk policy flags must be boolean")
    if not isinstance(policy.block_on_foreign_positions, bool) or not isinstance(policy.require_protective_stops, bool):
        raise ValueError("risk policy flags must be boolean")
    if not policy.no_martingale:
        raise ValueError("martingale is not supported by the demo executor")
    if not policy.close_only_own_positions:
        raise ValueError("closing foreign positions is not supported by the demo executor")


def _normalize_policy_risk_limits(policy: Any) -> None:
    for name in ("max_daily_loss", "max_drawdown", "min_margin_level"):
        value = getattr(policy, name)
        if value is None:
            continue
        parsed = _decimal_value(value, name)
        if parsed < 0:
            raise ValueError(f"{name} must be non-negative or None")
        object.__setattr__(policy, name, parsed)


def _normalize_policy_protection(policy: Any) -> None:
    for name in (
        "relative_stop_loss",
        "relative_take_profit",
        "max_relative_stop_loss",
        "max_relative_take_profit",
        "max_slippage_points",
    ):
        value = getattr(policy, name)
        if value is None:
            continue
        if isinstance(value, bool) or int(value) <= 0:
            raise ValueError(f"{name} must be a positive integer or None")
        object.__setattr__(policy, name, int(value))
    for value_name, maximum_name in (
        ("relative_stop_loss", "max_relative_stop_loss"),
        ("relative_take_profit", "max_relative_take_profit"),
    ):
        value = getattr(policy, value_name)
        maximum = getattr(policy, maximum_name)
        if value is not None and maximum is not None and value > maximum:
            raise ValueError(f"{value_name} exceeds {maximum_name}")


@dataclasses.dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    """Fail-closed demo policy and virtual risk caps."""

    required_scopes: frozenset[str] = frozenset({"trading"})
    max_quantity: DecimalValue = DecimalValue("1.0")
    fixed_quantity: DecimalValue | None = DecimalValue("1.0")
    max_exposure: DecimalValue = DecimalValue("1000.0")
    max_positions: int = 1
    max_spread: DecimalValue = DecimalValue("1.0")
    max_price_age_seconds: DecimalValue = DecimalValue("30.0")
    allowed_symbols: frozenset[str] = frozenset()
    no_martingale: bool = True
    close_only_own_positions: bool = True
    timeout_seconds: DecimalValue = DecimalValue("5.0")
    close_timeout_seconds: DecimalValue | None = None
    max_holding_seconds: DecimalValue | None = None
    activated: bool = False
    # Operational limits are explicit and opt-in.  The executor never
    # invents account equity or broker limits; a supervisor may provide the
    # observed values through ``update_risk_metrics``.
    max_daily_loss: DecimalValue | None = None
    max_drawdown: DecimalValue | None = None
    min_margin_level: DecimalValue | None = None
    max_orders_per_window: int | None = None
    order_window_seconds: DecimalValue = DecimalValue("60.0")
    max_inflight_intents: int = 1
    block_on_foreign_positions: bool = True
    require_protective_stops: bool = False
    relative_stop_loss: int | None = None
    relative_take_profit: int | None = None
    max_relative_stop_loss: int | None = None
    max_relative_take_profit: int | None = None
    max_slippage_points: int | None = None

    def __post_init__(self) -> None:
        scopes = _text_set(self.required_scopes)
        symbols = frozenset(str(x).strip().upper() for x in self.allowed_symbols)
        object.__setattr__(self, "required_scopes", scopes)
        object.__setattr__(self, "allowed_symbols", symbols)
        _normalize_policy_decimals(self)
        _validate_policy_quantities(self)
        _validate_policy_counts(self)
        _validate_policy_flags(self)
        _normalize_policy_risk_limits(self)
        _normalize_policy_protection(self)

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["required_scopes"] = sorted(self.required_scopes)
        result["allowed_symbols"] = sorted(self.allowed_symbols)
        for name in (
            "max_quantity",
            "fixed_quantity",
            "max_exposure",
            "max_spread",
            "max_price_age_seconds",
            "timeout_seconds",
            "close_timeout_seconds",
            "max_holding_seconds",
            "max_daily_loss",
            "max_drawdown",
            "min_margin_level",
            "order_window_seconds",
        ):
            value = result.get(name)
            result[name] = _decimal_text(value) if value is not None else None
        return result


@dataclasses.dataclass(frozen=True, slots=True)
class Quote:
    """Explicit bid/ask observation used only for demo safety checks."""

    symbol: str
    bid: DecimalValue
    ask: DecimalValue
    timestamp: datetime
    available_at: datetime | None = None
    quality: str = "VALID"
    source: str = "demo"
    base_price: str = "bid_ask"
    source_identity: str | None = None
    session_id: str | None = None
    connection_generation: str | int | None = None
    data_mode: str | None = None
    synthetic: bool = False

    def __post_init__(self) -> None:
        symbol = str(self.symbol).strip().upper()
        bid = _decimal_value(self.bid, "bid", positive=True)
        ask = _decimal_value(self.ask, "ask", positive=True)
        if not symbol or ask < bid:
            raise ValueError("quote symbol/bid/ask invalid")
        ts = _parse_time(self.timestamp)
        available = _parse_time(self.available_at) if self.available_at is not None else ts
        if available < ts:
            raise ValueError("quote available_at cannot precede timestamp")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        object.__setattr__(self, "timestamp", ts)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "quality", str(self.quality).upper())
        object.__setattr__(self, "source", str(self.source).strip() or "unknown")
        source_identity = None if self.source_identity is None else str(self.source_identity).strip() or None
        session_id = None if self.session_id is None else str(self.session_id).strip() or None
        generation = None if self.connection_generation is None else str(self.connection_generation).strip() or None
        data_mode = None if self.data_mode is None else str(self.data_mode).strip().upper() or None
        if not isinstance(self.synthetic, bool):
            raise ValueError("quote synthetic must be boolean")
        object.__setattr__(self, "source_identity", source_identity)
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "connection_generation", generation)
        object.__setattr__(self, "data_mode", data_mode)

    @property
    def spread(self) -> DecimalValue:
        return self.ask - self.bid

    @property
    def mid(self) -> DecimalValue:
        return (self.ask + self.bid) / DecimalValue("2")

    def price_for(self, side: Side) -> DecimalValue:
        return self.ask if side is Side.BUY else self.bid

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "bid": _decimal_text(self.bid),
            "ask": _decimal_text(self.ask),
            "timestamp": _iso(self.timestamp),
            "available_at": _iso(self.available_at),
            "quality": self.quality,
            "source": self.source,
            "base_price": self.base_price,
            "source_identity": self.source_identity,
            "session_id": self.session_id,
            "connection_generation": self.connection_generation,
            "data_mode": self.data_mode,
            "synthetic": self.synthetic,
            "spread": _decimal_text(self.spread),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> Quote:
        return cls(
            symbol=str(value.get("symbol", value.get("instrument", ""))),
            bid=value["bid"],
            ask=value["ask"],
            timestamp=_parse_time(value.get("timestamp", value.get("event_time"))),
            available_at=_parse_time(value.get("available_at", value.get("received_at")))
            if value.get("available_at", value.get("received_at")) is not None
            else None,
            quality=str(value.get("quality", "VALID")),
            source=str(value.get("source", "unknown")),
            base_price=str(value.get("base_price", value.get("price_base", "bid_ask"))),
            source_identity=value.get("source_identity", value.get("source_event_id", value.get("quote_id"))),
            session_id=value.get("session_id", value.get("session")),
            connection_generation=value.get("connection_generation", value.get("generation")),
            data_mode=value.get("data_mode", value.get("market_data_mode")),
            synthetic=_bool_value(value.get("synthetic", value.get("synthetic_fixture", False))),
        )


@dataclasses.dataclass(frozen=True, slots=True)
class Fill:
    fill_id: str
    quantity: DecimalValue
    price: DecimalValue
    timestamp: datetime

    def __post_init__(self) -> None:
        quantity = _decimal_value(self.quantity, "fill quantity", positive=True)
        price = _decimal_value(self.price, "fill price", positive=True)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "timestamp", _parse_time(self.timestamp))

    def to_dict(self) -> dict[str, Any]:
        return {
            "fill_id": self.fill_id,
            "quantity": _decimal_text(self.quantity),
            "price": _decimal_text(self.price),
            "timestamp": _iso(self.timestamp),
        }


@dataclasses.dataclass(frozen=True, slots=True)
class Position:
    position_id: str
    account_id: str
    symbol: str
    side: Side
    quantity: DecimalValue
    entry_price: DecimalValue
    client_order_id: str | None = None
    owner: str = "mtf-lab"
    opened_at: datetime | None = None
    stop_loss: DecimalValue | None = None
    take_profit: DecimalValue | None = None

    def __post_init__(self) -> None:
        quantity = _decimal_value(self.quantity, "position quantity", positive=True)
        price = _decimal_value(self.entry_price, "position entry_price", positive=True)
        object.__setattr__(self, "account_id", str(self.account_id))
        object.__setattr__(self, "symbol", str(self.symbol).upper())
        object.__setattr__(self, "side", self.side if isinstance(self.side, Side) else Side.parse(self.side))
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "entry_price", price)
        object.__setattr__(self, "opened_at", _parse_time(self.opened_at) if self.opened_at is not None else None)
        object.__setattr__(self, "stop_loss", _optional_decimal(self.stop_loss, "position stop_loss"))
        object.__setattr__(self, "take_profit", _optional_decimal(self.take_profit, "position take_profit"))

    @property
    def notional(self) -> DecimalValue:
        return self.quantity * self.entry_price

    @property
    def protection_state(self) -> str:
        return "OBSERVED" if self.stop_loss is not None and self.take_profit is not None else "UNKNOWN"

    def to_dict(self) -> dict[str, Any]:
        return {
            "position_id": self.position_id,
            "account_id": self.account_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "quantity": _decimal_text(self.quantity),
            "entry_price": _decimal_text(self.entry_price),
            "client_order_id": self.client_order_id,
            "owner": self.owner,
            "opened_at": _iso(self.opened_at),
            "stop_loss": _decimal_text(self.stop_loss) if self.stop_loss is not None else None,
            "take_profit": _decimal_text(self.take_profit) if self.take_profit is not None else None,
            "protection_state": self.protection_state,
            "notional": _decimal_text(self.notional),
        }


@dataclasses.dataclass(frozen=True, slots=True)
class ExecutionIntent:
    intent_id: str
    signal_id: str
    symbol: str
    side: Side
    quantity: DecimalValue
    requested_price: DecimalValue | None
    created_at: datetime
    account_id: str
    kind: str = "OPEN"
    position_id: str | None = None
    metadata: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        quantity = _decimal_value(self.quantity, "intent quantity", positive=True)
        kind = str(getattr(self.kind, "value", self.kind)).strip().upper()
        if kind not in {"OPEN", "CLOSE"}:
            raise ValueError("execution intent kind must be OPEN or CLOSE")
        price = None
        if self.requested_price is not None:
            price = _decimal_value(self.requested_price, "intent requested_price", positive=True)
        elif kind == "OPEN":
            raise ValueError("OPEN intent requires requested_price")
        if kind == "CLOSE" and not str(self.position_id or "").strip():
            raise ValueError("CLOSE intent requires position_id")
        if not self.intent_id or not self.signal_id or not self.symbol:
            raise ValueError("execution intent fields invalid")
        object.__setattr__(self, "symbol", str(self.symbol).upper())
        object.__setattr__(self, "side", self.side if isinstance(self.side, Side) else Side.parse(self.side))
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "requested_price", price)
        object.__setattr__(self, "created_at", _parse_time(self.created_at))
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "position_id", str(self.position_id).strip() if self.position_id is not None else None)
        object.__setattr__(self, "metadata", dict(self.metadata))

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "client_order_id": self.intent_id,
            "signal_id": self.signal_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "quantity": _decimal_text(self.quantity),
            "requested_price": _decimal_text(self.requested_price) if self.requested_price is not None else None,
            "created_at": _iso(self.created_at),
            "account_id": self.account_id,
            "kind": self.kind,
            "position_id": self.position_id,
            "metadata": dict(self.metadata),
        }


def _snapshot_quantities(requested_value: Any, filled_value: Any) -> tuple[DecimalValue, DecimalValue]:
    requested = _decimal_value(requested_value, "order requested_quantity", positive=True)
    filled = _decimal_value(filled_value, "order filled_quantity")
    if filled < 0 or filled > requested + _DECIMAL_TOLERANCE:
        raise ValueError("order quantities invalid")
    return requested, min(requested, filled)


def _snapshot_fills(values: Iterable[Fill | Mapping[str, Any]]) -> tuple[Fill, ...]:
    result: list[Fill] = []
    for value in values:
        if isinstance(value, Fill):
            result.append(value)
            continue
        result.append(Fill(str(value["fill_id"]), value["quantity"], value["price"], _parse_time(value["timestamp"])))
    return tuple(result)


@dataclasses.dataclass(frozen=True, slots=True)
class OrderSnapshot:
    order_id: str | None
    client_order_id: str
    status: OrderState
    requested_quantity: DecimalValue
    filled_quantity: DecimalValue = DecimalValue("0")
    fills: tuple[Fill, ...] = ()
    reject_reason: str | None = None
    position_ids: tuple[str, ...] = ()
    observed_at: datetime = dataclasses.field(default_factory=lambda: datetime.now(UTC))
    uncertainty_reason: str | None = None
    stop_loss: DecimalValue | None = None
    take_profit: DecimalValue | None = None

    def __post_init__(self) -> None:
        requested, filled = _snapshot_quantities(self.requested_quantity, self.filled_quantity)
        fills = _snapshot_fills(self.fills)
        status = self.status if isinstance(self.status, OrderState) else OrderState(str(self.status).upper())
        object.__setattr__(self, "requested_quantity", requested)
        object.__setattr__(self, "filled_quantity", filled)
        object.__setattr__(self, "client_order_id", str(self.client_order_id))
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "fills", fills)
        object.__setattr__(self, "position_ids", tuple(str(item) for item in self.position_ids))
        object.__setattr__(self, "observed_at", _parse_time(self.observed_at))
        object.__setattr__(self, "stop_loss", _optional_decimal(self.stop_loss, "order stop_loss"))
        object.__setattr__(self, "take_profit", _optional_decimal(self.take_profit, "order take_profit"))

    @property
    def remaining_quantity(self) -> DecimalValue:
        remaining = self.requested_quantity - self.filled_quantity
        return remaining if remaining > 0 else DecimalValue("0")

    @property
    def protection_state(self) -> str:
        return "OBSERVED" if self.stop_loss is not None and self.take_profit is not None else "UNKNOWN"

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "status": self.status.value,
            "requested_quantity": _decimal_text(self.requested_quantity),
            "filled_quantity": _decimal_text(self.filled_quantity),
            "remaining_quantity": _decimal_text(self.remaining_quantity),
            "fills": [fill.to_dict() for fill in self.fills],
            "reject_reason": self.reject_reason,
            "position_ids": list(self.position_ids),
            "uncertainty_reason": self.uncertainty_reason,
            "stop_loss": _decimal_text(self.stop_loss) if self.stop_loss is not None else None,
            "take_profit": _decimal_text(self.take_profit) if self.take_profit is not None else None,
            "protection_state": self.protection_state,
            "observed_at": _iso(self.observed_at),
        }


@dataclasses.dataclass(frozen=True, slots=True)
class ExecutionEvent:
    event_type: str
    intent_id: str
    state: OrderState
    timestamp: datetime
    details: Mapping[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "intent_id": self.intent_id,
            "state": self.state.value,
            "timestamp": _iso(self.timestamp),
            "details": dict(self.details),
        }


@dataclasses.dataclass(slots=True)
class OrderResult:
    intent: ExecutionIntent
    state: OrderState
    order_id: str | None = None
    filled_quantity: DecimalValue = DecimalValue("0")
    fills: tuple[Fill, ...] = ()
    reject_reason: str | None = None
    position_ids: tuple[str, ...] = ()
    unknown_reason: str | None = None
    reconciled: bool = False
    uncertainty_reason: str | None = None
    stop_loss: DecimalValue | None = None
    take_profit: DecimalValue | None = None

    def __post_init__(self) -> None:
        self.filled_quantity = _decimal_value(self.filled_quantity, "result filled_quantity")
        if self.filled_quantity < 0 or self.filled_quantity > self.intent.quantity + _DECIMAL_TOLERANCE:
            raise ValueError("result filled_quantity must be between zero and the requested quantity")
        self.filled_quantity = min(self.intent.quantity, self.filled_quantity)
        self.fills = tuple(self.fills)
        self.state = self.state if isinstance(self.state, OrderState) else OrderState(str(self.state).upper())
        self.stop_loss = _optional_decimal(self.stop_loss, "result stop_loss")
        self.take_profit = _optional_decimal(self.take_profit, "result take_profit")

    @property
    def remaining_quantity(self) -> DecimalValue:
        remaining = self.intent.quantity - self.filled_quantity
        return remaining if remaining > 0 else DecimalValue("0")

    @property
    def protection_state(self) -> str:
        return "OBSERVED" if self.stop_loss is not None and self.take_profit is not None else "UNKNOWN"

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent.to_dict(),
            "state": self.state.value,
            "order_id": self.order_id,
            "filled_quantity": _decimal_text(self.filled_quantity),
            "remaining_quantity": _decimal_text(self.remaining_quantity),
            "fills": [fill.to_dict() for fill in self.fills],
            "reject_reason": self.reject_reason,
            "position_ids": list(self.position_ids),
            "unknown_reason": self.unknown_reason,
            "uncertainty_reason": self.uncertainty_reason,
            "stop_loss": _decimal_text(self.stop_loss) if self.stop_loss is not None else None,
            "take_profit": _decimal_text(self.take_profit) if self.take_profit is not None else None,
            "protection_state": self.protection_state,
            "reconciled": self.reconciled,
        }


# ---------------------------------------------------------------------------
# Optional durable intent stores


@runtime_checkable
class IntentStore(Protocol):
    def record_intent(self, intent: Mapping[str, Any]) -> Any: ...
    def record_event(self, event: Mapping[str, Any]) -> Any: ...
    def update_intent(self, intent_id: str, update: Mapping[str, Any]) -> Any: ...

    def has_intent(self, intent_id: str) -> bool: ...


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
        with self._lock:
            self.events.append(dict(event))

    def update_intent(self, intent_id: str, update: Mapping[str, Any]) -> None:
        with self._lock:
            self.updates.append({"intent_id": str(intent_id), **dict(update)})

    def has_intent(self, intent_id: str) -> bool:
        with self._lock:
            return str(intent_id) in self.intents


class JsonlIntentStore(MemoryIntentStore):
    """Private, fsync-backed, inter-process locked intent journal.

    The journal is deliberately not a generic append-only text file.  It is a
    local execution boundary: its parent must be a private directory, the
    final file is opened with ``O_NOFOLLOW`` and every append is serialized with
    ``flock``.  Existing records make the store recovery-pending; callers must
    explicitly hydrate an executor before another intent can be sent.
    """

    def __init__(self, path: str | os.PathLike[str]):
        super().__init__()
        raw_path = Path(path).expanduser()
        if not raw_path.is_absolute():
            raw_path = raw_path.resolve(strict=False)
        if raw_path.exists() and raw_path.is_symlink():
            raise ExecutionLocalFailure("intent journal cannot be a symlink", phase=SendPhase.LOCAL_ERROR)
        self.path = raw_path
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_info = self.path.parent.stat()
        if parent_info.st_uid != os.getuid() or stat.S_IMODE(parent_info.st_mode) != 0o700:
            raise ExecutionLocalFailure(
                "intent journal parent must be owned by the current user with mode 0700",
                phase=SendPhase.LOCAL_ERROR,
            )
        flags = os.O_RDWR | os.O_CREAT | os.O_APPEND
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(self.path, flags, 0o600)
        except OSError as exc:
            raise ExecutionLocalFailure("intent journal cannot be opened safely", phase=SendPhase.LOCAL_ERROR) from exc
        try:
            info = os.fstat(fd)
            if info.st_uid != os.getuid() or not stat.S_ISREG(info.st_mode):
                raise ExecutionLocalFailure(
                    "intent journal must be a user-owned regular file", phase=SendPhase.LOCAL_ERROR
                )
            os.fchmod(fd, 0o600)
            info = os.fstat(fd)
            if stat.S_IMODE(info.st_mode) != 0o600:
                raise ExecutionLocalFailure("intent journal must have exact mode 0600", phase=SendPhase.LOCAL_ERROR)
            self._stream = os.fdopen(fd, "a+", encoding="utf-8", newline="")
            fd = -1
        finally:
            if fd >= 0:
                os.close(fd)
        self._closed = False
        self._recovery_pending = self._load_existing_records()

    @property
    def requires_recovery(self) -> bool:
        return self._recovery_pending

    def mark_recovery_complete(self) -> None:
        self._recovery_pending = False

    def _load_existing_records(self) -> bool:
        """Load identity records before any caller can submit a duplicate."""

        with self._file_lock(shared=False):
            self._stream.flush()
            self._stream.seek(0)
            raw_lines = self._stream.read().splitlines()
            self._stream.seek(0, os.SEEK_END)
        found = False
        for line_number, line in enumerate(raw_lines, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ExecutionLocalFailure(
                    f"intent journal contains invalid JSON at line {line_number}", phase=SendPhase.LOCAL_ERROR
                ) from exc
            if not isinstance(record, Mapping):
                raise ExecutionLocalFailure(
                    f"intent journal line {line_number} is not an object", phase=SendPhase.LOCAL_ERROR
                )
            journal_type = str(record.get("journal_type", ""))
            if journal_type == "intent":
                super().record_intent({key: value for key, value in record.items() if key != "journal_type"})
                found = True
            elif journal_type in {"event", "update"}:
                found = True
        return found

    @contextlib.contextmanager
    def _file_lock(self, *, shared: bool) -> Iterator[None]:
        if self._closed:
            raise ExecutionLocalFailure("intent journal is closed", phase=SendPhase.LOCAL_ERROR)
        mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        fcntl.flock(self._stream.fileno(), mode)
        try:
            yield
        finally:
            fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)

    def _write(self, record: Mapping[str, Any]) -> None:
        clean = {key: value for key, value in dict(record).items() if key != "journal_type"}
        journal_type = str(record.get("journal_type", ""))
        if journal_type not in {"intent", "event", "update"}:
            raise ExecutionLocalFailure("invalid intent journal record type", phase=SendPhase.LOCAL_ERROR)
        line = (
            json.dumps(
                {"journal_type": journal_type, **clean},
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
            + "\n"
        )
        with self._file_lock(shared=False):
            self._stream.write(line)
            self._stream.flush()
            os.fsync(self._stream.fileno())

    def record_intent(self, intent: Mapping[str, Any]) -> None:
        with self._lock:
            before = self.has_intent(str(intent["intent_id"]))
            super().record_intent(intent)
            if not before:
                self._write({"journal_type": "intent", **dict(intent)})

    def record_event(self, event: Mapping[str, Any]) -> None:
        with self._lock:
            super().record_event(event)
            self._write({"journal_type": "event", **dict(event)})

    def update_intent(self, intent_id: str, update: Mapping[str, Any]) -> None:
        with self._lock:
            super().update_intent(intent_id, update)
            self._write({"journal_type": "update", "intent_id": intent_id, **dict(update)})

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            with self._file_lock(shared=False):
                self._stream.flush()
                os.fsync(self._stream.fileno())
            self._stream.close()
            self._closed = True


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

    def cancel_order(
        self, order_id: str, *, client_order_id: str | None = None, timeout_seconds: float
    ) -> OrderSnapshot: ...

    def register_intent(self, intent: ExecutionIntent) -> Any: ...


class DemoTransport:
    """No-network deterministic transport for lifecycle/fixture tests."""

    def __init__(
        self,
        *,
        account_id: str = "demo-account",
        endpoint: str = "demo://ctrader",
        scopes: Iterable[str] = ("trading",),
        default_behavior: str = "full",
        partial_ratio: float = 0.5,
        clock: Callable[[], datetime] | None = None,
    ):
        self.account_id = str(account_id)
        self.endpoint = str(endpoint)
        self.scopes = _text_set(scopes)
        self.default_behavior = str(default_behavior).lower()
        self.partial_ratio = _decimal_value(partial_ratio, "partial_ratio", positive=True)
        self.clock = clock or (lambda: datetime.now(UTC))
        self.orders: dict[str, OrderSnapshot] = {}
        self.positions_by_id: dict[str, Position] = {}
        self.behaviors: list[str] = []
        self._counter = 0
        self._lock = threading.RLock()
        if not _is_demo_endpoint(self.endpoint):
            raise EndpointRejected("DemoTransport endpoint must be demo-only")
        if self.default_behavior not in {"full", "partial", "reject", "timeout"}:
            raise ValueError("unsupported demo behavior")
        if not DecimalValue("0") < self.partial_ratio < DecimalValue("1"):
            raise ValueError("partial_ratio must be between 0 and 1")

    def verify_endpoint(self, endpoint: str) -> bool:
        return _is_demo_endpoint(endpoint) and str(endpoint) == self.endpoint

    def available_scopes(self) -> frozenset[str]:
        return self.scopes

    def queue_behavior(self, *behaviors: str) -> None:
        allowed = {"full", "partial", "reject", "timeout"}
        for behavior in behaviors:
            value = str(behavior).lower()
            if value not in allowed:
                raise ValueError(f"unsupported demo behavior: {behavior}")
            self.behaviors.append(value)

    def _next_behavior(self) -> str:
        return self.behaviors.pop(0) if self.behaviors else self.default_behavior

    def register_intent(self, intent: ExecutionIntent) -> None:
        if intent.account_id != self.account_id:
            raise CorrelationError("intent account does not match demo transport")

    def cancel_order(
        self, order_id: str, *, client_order_id: str | None = None, timeout_seconds: float
    ) -> OrderSnapshot:
        del timeout_seconds
        key = str(client_order_id or order_id)
        with self._lock:
            current = self.orders.get(key)
            now = _parse_time(self.clock())
            if current is None:
                raise CorrelationError("cannot cancel an unknown demo order")
            if current.status in _TERMINAL_STATES:
                return current
            cancelled = OrderSnapshot(
                current.order_id or str(order_id),
                current.client_order_id,
                OrderState.CANCELLED,
                current.requested_quantity,
                current.filled_quantity,
                current.fills,
                reject_reason=None,
                position_ids=current.position_ids,
                observed_at=now,
                uncertainty_reason=None,
                stop_loss=current.stop_loss,
                take_profit=current.take_profit,
            )
            self.orders[key] = cancelled
            return cancelled

    def submit(self, intent: ExecutionIntent, *, timeout_seconds: float) -> OrderSnapshot:
        with self._lock:
            if intent.account_id != self.account_id or intent.kind != "OPEN":
                raise CorrelationError("demo submit requires an OPEN intent for the selected account")
            self._counter += 1
            order_id = f"demo-order-{self._counter:06d}"
            behavior = self._next_behavior()
            now = _parse_time(self.clock())
            if behavior == "timeout":
                snapshot = OrderSnapshot(
                    order_id=None,
                    client_order_id=intent.intent_id,
                    status=OrderState.UNKNOWN,
                    requested_quantity=intent.quantity,
                    observed_at=now,
                )
                self.orders[intent.intent_id] = snapshot
                raise TimeoutError("demo transport timeout before acknowledgement")
            if behavior == "reject":
                snapshot = OrderSnapshot(
                    order_id=order_id,
                    client_order_id=intent.intent_id,
                    status=OrderState.REJECTED,
                    requested_quantity=intent.quantity,
                    reject_reason="DEMO_REJECTED",
                    observed_at=now,
                )
                self.orders[intent.intent_id] = snapshot
                return snapshot
            quantity = intent.quantity if behavior == "full" else intent.quantity * self.partial_ratio
            requested_price = intent.requested_price
            if requested_price is None:
                raise CorrelationError("OPEN intent lacks requested price")
            fill = Fill(f"{order_id}-fill-1", quantity, requested_price, now)
            position_id = f"demo-position-{self._counter:06d}"
            position = Position(
                position_id,
                self.account_id,
                intent.symbol,
                intent.side,
                quantity,
                requested_price,
                intent.intent_id,
            )
            self.positions_by_id[position_id] = position
            state = OrderState.FILLED if quantity >= intent.quantity - _DECIMAL_TOLERANCE else OrderState.PARTIAL
            snapshot = OrderSnapshot(
                order_id=order_id,
                client_order_id=intent.intent_id,
                status=state,
                requested_quantity=intent.quantity,
                filled_quantity=quantity,
                fills=(fill,),
                position_ids=(position_id,),
                observed_at=now,
            )
            self.orders[intent.intent_id] = snapshot
            return snapshot

    def get_order(self, client_order_id: str) -> OrderSnapshot | None:
        with self._lock:
            return self.orders.get(str(client_order_id))

    def advance_partial(self, client_order_id: str) -> OrderSnapshot:
        with self._lock:
            current = self.orders.get(str(client_order_id))
            if current is None:
                raise CorrelationError(f"unknown demo order: {client_order_id}")
            if current.status not in {OrderState.PARTIAL, OrderState.CLOSE_PARTIAL}:
                return current
            remaining = current.requested_quantity - current.filled_quantity
            now = _parse_time(self.clock())
            order_id = current.order_id or f"demo-order-recovered-{client_order_id[:8]}"
            if not current.fills:
                raise CorrelationError("partial order has no observed fill price")
            fill = Fill(f"{order_id}-fill-{len(current.fills) + 1}", remaining, current.fills[-1].price, now)
            old_pos = current.position_ids[0] if current.position_ids else None
            if current.status is OrderState.PARTIAL:
                if old_pos and old_pos in self.positions_by_id:
                    p = self.positions_by_id[old_pos]
                    self.positions_by_id[old_pos] = dataclasses.replace(p, quantity=p.quantity + remaining)
                final_state = OrderState.FILLED
            else:
                if old_pos:
                    self.positions_by_id.pop(old_pos, None)
                final_state = OrderState.CLOSED
            updated = OrderSnapshot(
                order_id=order_id,
                client_order_id=current.client_order_id,
                status=final_state,
                requested_quantity=current.requested_quantity,
                filled_quantity=current.requested_quantity,
                fills=(*current.fills, fill),
                position_ids=current.position_ids,
                observed_at=now,
                stop_loss=current.stop_loss,
                take_profit=current.take_profit,
            )
            self.orders[client_order_id] = updated
            return updated

    def list_positions(self, account_id: str) -> Sequence[Position]:
        with self._lock:
            return tuple(p for p in self.positions_by_id.values() if p.account_id == str(account_id))

    def add_foreign_position(
        self,
        *,
        account_id: str = "other-account",
        symbol: str = "EUR/USD",
        side: Side = Side.BUY,
        quantity: float = 1.0,
        entry_price: float = 1.0,
    ) -> str:
        with self._lock:
            self._counter += 1
            pid = f"foreign-position-{self._counter:06d}"
            self.positions_by_id[pid] = Position(
                pid,
                account_id,
                symbol,
                side,
                _decimal_value(quantity, "position quantity", positive=True),
                _decimal_value(entry_price, "position entry_price", positive=True),
                owner="other-owner",
            )
            return pid

    def close_position(self, position: Position, *, client_order_id: str, timeout_seconds: float) -> OrderSnapshot:
        with self._lock:
            if position.account_id != self.account_id or position.owner != "mtf-lab":
                raise ForeignPosition("demo transport refuses a foreign position")
            now = _parse_time(self.clock())
            if position.position_id not in self.positions_by_id:
                return OrderSnapshot(
                    f"close-{client_order_id}",
                    client_order_id,
                    OrderState.REJECTED,
                    position.quantity,
                    reject_reason="POSITION_NOT_FOUND",
                    observed_at=now,
                )
            behavior = self._next_behavior()
            if behavior == "timeout":
                snapshot = OrderSnapshot(None, client_order_id, OrderState.UNKNOWN, position.quantity, observed_at=now)
                self.orders[client_order_id] = snapshot
                raise TimeoutError("demo close timeout")
            if behavior == "reject":
                snapshot = OrderSnapshot(
                    f"close-{client_order_id}",
                    client_order_id,
                    OrderState.REJECTED,
                    position.quantity,
                    reject_reason="DEMO_CLOSE_REJECTED",
                    observed_at=now,
                )
                self.orders[client_order_id] = snapshot
                return snapshot
            if behavior == "partial":
                quantity = position.quantity * self.partial_ratio
                fill = Fill(f"close-{client_order_id}-fill-1", quantity, position.entry_price, now)
                self.positions_by_id[position.position_id] = dataclasses.replace(
                    position, quantity=position.quantity - quantity
                )
                snapshot = OrderSnapshot(
                    f"close-{client_order_id}",
                    client_order_id,
                    OrderState.CLOSE_PARTIAL,
                    position.quantity,
                    quantity,
                    (fill,),
                    position_ids=(position.position_id,),
                    observed_at=now,
                )
                self.orders[client_order_id] = snapshot
                return snapshot
            self.positions_by_id.pop(position.position_id, None)
            fill = Fill(f"close-{client_order_id}-fill", position.quantity, position.entry_price, now)
            snapshot = OrderSnapshot(
                f"close-{client_order_id}",
                client_order_id,
                OrderState.CLOSED,
                position.quantity,
                position.quantity,
                (fill,),
                position_ids=(position.position_id,),
                observed_at=now,
            )
            self.orders[client_order_id] = snapshot
            return snapshot


# ---------------------------------------------------------------------------
# Safety and executor state machine


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        dt = datetime.fromtimestamp(float(value), UTC)
    else:
        text = str(value).strip()
        text = text[:-1] + "+00:00" if text.endswith("Z") else text
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("timestamp must include timezone")
    return dt.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return _parse_time(value).isoformat(timespec="microseconds").replace("+00:00", "Z") if value is not None else None


def _is_demo_endpoint(endpoint: str) -> bool:
    text = str(endpoint).strip().lower()
    if any(token in text for token in ("real", "live", "production")):
        return False
    return text.startswith("demo:") or "demo" in text or "sandbox" in text or "paper" in text


def _scope_satisfied(required: str, available: frozenset[str]) -> bool:
    value = str(required).strip().lower()
    normalized_available = {str(item).strip().lower() for item in available}
    # Execution authorization is intentionally exact.  Read-only aliases,
    # substring matches and generic "trade" labels are not evidence that the
    # selected account may submit an order.
    if value == "trading":
        return "trading" in normalized_available
    return value in normalized_available


def verify_demo_account(
    account: DemoAccount | Mapping[str, Any],
    *,
    required_scopes: Iterable[str] = ("trading",),
    endpoint: str | None = None,
) -> DemoAccount:
    """Verify demo-only selection; REAL is rejected before other gates."""
    if not isinstance(account, DemoAccount):
        account = DemoAccount.from_mapping(account)
    # This check deliberately precedes account id, endpoint, token or scope
    # checks so malformed REAL configuration cannot reach a real transport.
    if (
        account.environment in {"REAL", "LIVE", "PRODUCTION", "PAPER_LIVE"}
        or "REAL" in account.environment
        or "LIVE" in account.environment
    ):
        raise RealAccountForbidden("REAL/LIVE account execution is forbidden by the demo executor")
    if account.environment not in {"DEMO", "SANDBOX", "PAPER"}:
        raise DemoAccountRequired("a selected DEMO account is required")
    target_endpoint = endpoint or account.endpoint
    if not _is_demo_endpoint(target_endpoint):
        raise EndpointRejected("endpoint must be explicitly demo/sandbox/paper")
    if not account.account_id or not account.selected or not account.verified:
        raise DemoAccountRequired("account_id, selected=true and verified=true are required")
    missing = [scope for scope in required_scopes if not _scope_satisfied(str(scope), account.scopes)]
    if missing:
        raise ScopeRejected(f"missing required demo scopes: {sorted(missing)}")
    return account


def _prepare_executor_account(
    account: DemoAccount | Mapping[str, Any], transport: ExecutionTransport | None, observation: Any | None
) -> DemoAccount | Mapping[str, Any]:
    raw_environment = (
        str(
            account.get("environment", account.get("mode", ""))
            if isinstance(account, Mapping)
            else getattr(account, "environment", "")
        )
        .strip()
        .upper()
    )
    if "REAL" in raw_environment or "LIVE" in raw_environment or raw_environment in {"PRODUCTION", "PAPER_LIVE"}:
        raise RealAccountForbidden("REAL/LIVE account execution is forbidden by the demo executor")
    if transport is not None and not isinstance(transport, DemoTransport) and observation is None:
        raise DemoAccountRequired("external transport requires ServerObservationProof")
    if observation is None:
        return account
    if not isinstance(observation, ServerObservationProof):
        raise DemoAccountRequired("server_observation must be a normalized ServerObservationProof")
    account_obj = account if isinstance(account, DemoAccount) else DemoAccount.from_mapping(account)
    if not observation.matches_account(account_obj):
        raise DemoAccountRequired("server observation does not match selected account")
    observed_scopes: set[str] = {
        str(item).strip().lower() for item in cast(Iterable[str], getattr(observation, "scopes", ()))
    }
    return dataclasses.replace(account_obj, verified=True, scopes=frozenset(set(account_obj.scopes) | observed_scopes))


def _validate_executor_transport(transport: ExecutionTransport, account: DemoAccount, policy: ExecutionPolicy) -> None:
    transport_account = getattr(transport, "account_id", None)
    if transport_account is not None and str(transport_account) != account.account_id:
        raise DemoAccountRequired("transport account does not match the selected demo account")
    transport_endpoint = getattr(transport, "endpoint", account.endpoint)
    if not _is_demo_endpoint(str(transport_endpoint)):
        raise EndpointRejected("transport endpoint must be demo-only")
    if not transport.verify_endpoint(account.endpoint):
        raise EndpointRejected("transport endpoint does not match selected demo endpoint")
    available_scopes = transport.available_scopes()
    missing = [scope for scope in policy.required_scopes if not _scope_satisfied(str(scope), available_scopes)]
    if missing:
        raise ScopeRejected(f"transport missing required scopes: {sorted(missing)}")


def _validate_external_execution_policy(policy: ExecutionPolicy) -> None:
    """Require explicit account-level safety inputs on non-fixture routes."""

    missing = [
        name
        for name in ("max_daily_loss", "max_drawdown", "min_margin_level", "max_holding_seconds")
        if getattr(policy, name) is None
    ]
    if missing:
        raise RiskLimitRejected("external DEMO transport requires explicit risk limits: " + ", ".join(missing))
    if not policy.require_protective_stops:
        raise RiskLimitRejected("external DEMO transport requires require_protective_stops=true")


def _parse_risk_metric(name: str, value: Any) -> DecimalValue | None:
    if value is None:
        return None
    parsed = _decimal_value(value, name)
    if name in {"equity", "margin_level", "used_margin"} and parsed < 0:
        raise ValueError(f"{name} must be non-negative")
    return parsed


class CTraderDemoExecutor:
    """Fail-closed DEMO executor; fixture and official gateways stay separate."""

    VERSION = "ctrader-demo-executor-v1"

    def __init__(
        self,
        account: DemoAccount | Mapping[str, Any],
        *,
        policy: ExecutionPolicy | None = None,
        transport: ExecutionTransport | None = None,
        intent_store: IntentStore | None = None,
        clock: Callable[[], datetime] | None = None,
        server_observation: Any | None = None,
        fixture_mode: bool = False,
    ):
        account = _prepare_executor_account(account, transport, server_observation)
        self.policy = policy or ExecutionPolicy()
        self.account = verify_demo_account(account, required_scopes=self.policy.required_scopes)
        self.transport = transport or DemoTransport(
            account_id=self.account.account_id, endpoint=self.account.endpoint, scopes=self.account.scopes, clock=clock
        )
        _validate_executor_transport(self.transport, self.account, self.policy)
        self.intent_store: IntentStore = intent_store or MemoryIntentStore()
        self.clock = clock or (lambda: datetime.now(UTC))
        self._virtual_only = isinstance(self.transport, DemoTransport)
        self._fixture_mode = bool(fixture_mode) or self._virtual_only
        if not self._fixture_mode:
            _validate_external_execution_policy(self.policy)
        self.server_observation = server_observation or getattr(self.transport, "server_observation", None)
        self._active = False
        self._paused = False
        self._pause_reason: str | None = None
        self._risk_halt_reason: str | None = None
        self._risk_metrics: dict[str, DecimalValue | None] = {
            "equity": None,
            "daily_pnl": None,
            "realized_daily_pnl": None,
            "unrealized_daily_pnl": None,
            "drawdown": None,
            "margin_level": None,
            "used_margin": None,
        }
        self._risk_metrics_observed_at: datetime | None = None
        self._risk_metrics_generation: str | None = None
        self._intents: dict[str, ExecutionIntent] = {}
        self._signal_intents: dict[str, str] = {}
        self._results: dict[str, OrderResult] = {}
        self._reserved_ids: set[str] = set()
        self._inflight_ids: set[str] = set()
        self._last_positions: tuple[Position, ...] | None = None
        self._last_positions_at: datetime | None = None
        self._last_positions_generation: str | None = None
        self._positions_complete = False
        self._events: list[ExecutionEvent] = []
        self._lock = threading.RLock()
        self._recovery_required = bool(getattr(self.intent_store, "requires_recovery", False))

    @property
    def active(self) -> bool:
        return self._active

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def recovery_required(self) -> bool:
        return self._recovery_required

    def complete_recovery(self) -> None:
        """Release the journal gate after the caller restored all intents."""

        with self._lock:
            self._recovery_required = False
            marker = getattr(self.intent_store, "mark_recovery_complete", None)
            if callable(marker):
                marker()

    def update_risk_metrics(
        self,
        *,
        equity: Any = _UNSET,
        daily_pnl: Any = _UNSET,
        realized_daily_pnl: Any = _UNSET,
        unrealized_daily_pnl: Any = _UNSET,
        drawdown: Any = _UNSET,
        margin_level: Any = _UNSET,
        used_margin: Any = _UNSET,
        observed_at: Any = _UNSET,
        connection_generation: Any = _UNSET,
    ) -> dict[str, Any]:
        """Record server-observed account metrics without inventing missing values."""

        with self._lock:
            metrics_changed = self._apply_risk_metric_values(
                equity,
                daily_pnl,
                realized_daily_pnl,
                unrealized_daily_pnl,
                drawdown,
                margin_level,
                used_margin,
            )
            self._apply_risk_metric_identity(observed_at, connection_generation, metrics_changed)
            return self.risk_status()

    def _apply_risk_metric_values(self, *values: Any) -> bool:
        names = (
            "equity",
            "daily_pnl",
            "realized_daily_pnl",
            "unrealized_daily_pnl",
            "drawdown",
            "margin_level",
            "used_margin",
        )
        changed = False
        for name, value in zip(names, values, strict=True):
            if value is _UNSET:
                continue
            changed = True
            self._risk_metrics[name] = _parse_risk_metric(name, value)
        if values[1] is not _UNSET and values[2] is _UNSET and values[3] is _UNSET:
            # A legacy aggregate update must not leave an older realized or
            # unrealized component eligible for an external gate.
            self._risk_metrics["realized_daily_pnl"] = None
            self._risk_metrics["unrealized_daily_pnl"] = None
        if values[2] is not _UNSET or values[3] is not _UNSET:
            realized = self._risk_metrics["realized_daily_pnl"]
            unrealized = self._risk_metrics["unrealized_daily_pnl"]
            self._risk_metrics["daily_pnl"] = (
                realized + unrealized if realized is not None and unrealized is not None else None
            )
        return changed

    def _apply_risk_metric_identity(self, observed_at: Any, connection_generation: Any, changed: bool) -> None:
        if observed_at is not _UNSET:
            self._risk_metrics_observed_at = None if observed_at is None else _parse_time(observed_at)
        elif changed:
            self._risk_metrics_observed_at = None
        if connection_generation is not _UNSET:
            self._risk_metrics_generation = (
                None if connection_generation is None else str(connection_generation).strip() or None
            )
        elif changed:
            self._risk_metrics_generation = None

    def _risk_limit_reason(self) -> str | None:
        if not self._fixture_mode:
            freshness_reason, daily = self._external_risk_values()
            if freshness_reason is not None:
                return freshness_reason
        else:
            daily = self._risk_metrics["daily_pnl"]
        return self._configured_risk_limit_reason(daily)

    def _external_risk_values(self) -> tuple[str | None, DecimalValue | None]:
        observed_at = self._risk_metrics_observed_at
        generation = self._risk_metrics_generation
        if observed_at is None or generation is None:
            return "risk_metrics_unobserved", None
        now = _parse_time(self.clock())
        if observed_at.date() != now.date():
            return "risk_metrics_epoch_changed", None
        age = (now - observed_at).total_seconds()
        if age < 0 or age > float(self.policy.max_price_age_seconds):
            return "risk_metrics_stale", None
        current_generation = self._current_connection_generation()
        if current_generation is None or generation != current_generation:
            return "risk_metrics_generation_unobserved", None
        realized = self._risk_metrics["realized_daily_pnl"]
        unrealized = self._risk_metrics["unrealized_daily_pnl"]
        if realized is None or unrealized is None:
            return "daily_pnl_components_unobserved", None
        return None, realized + unrealized

    def _configured_risk_limit_reason(self, daily: DecimalValue | None) -> str | None:
        if self.policy.max_daily_loss is not None:
            if daily is None:
                return "daily_pnl_unobserved"
            if daily <= -self.policy.max_daily_loss:
                return "max_daily_loss_exceeded"
        drawdown = self._risk_metrics["drawdown"]
        if self.policy.max_drawdown is not None:
            if drawdown is None:
                return "drawdown_unobserved"
            if drawdown >= self.policy.max_drawdown:
                return "max_drawdown_exceeded"
        return self._margin_limit_reason()

    def _margin_limit_reason(self) -> str | None:
        margin = self._risk_metrics["margin_level"]
        if self.policy.min_margin_level is not None:
            if margin is None:
                equity = self._risk_metrics["equity"]
                if self._risk_metrics["used_margin"] == 0 and equity is not None and equity > 0:
                    # Complete server observation of no used margin: the
                    # current ratio has no finite denominator. Never invent
                    # an observed infinity/large number to satisfy the gate.
                    return None
                return "margin_level_unobserved"
            if margin < self.policy.min_margin_level:
                return "min_margin_level_breached"
        return None

    def _current_connection_generation(self) -> str | None:
        """Read the generation currently held by the selected gateway.

        This is deliberately an attribute-only read.  In particular, the
        status cache must not call a gateway/session refresh method while
        checking whether its last account snapshot is still usable.
        """

        def read_generation(value: Any) -> str | None:
            if isinstance(value, Mapping):
                generation = value.get("connection_generation", value.get("generation"))
            else:
                generation = getattr(value, "connection_generation", None)
                if generation is None:
                    generation = getattr(value, "generation", None)
            if generation is None or callable(generation):
                return None
            normalized = str(generation).strip()
            return normalized or None

        transport_generation = read_generation(self.transport)
        if transport_generation is not None:
            return transport_generation
        gateway = getattr(self.transport, "client", None)
        # The gateway's frozen proof is deliberately not refreshed by this
        # projection. Prefer the actual single-reader client's live counter.
        raw_client = getattr(gateway, "client", None)
        generation = read_generation(raw_client)
        if generation is None:
            generation = read_generation(getattr(raw_client, "status", None))
        if generation is not None:
            return generation
        session = getattr(gateway, "session", None)
        generation = read_generation(session)
        if generation is not None:
            return generation
        transport_observation = getattr(self.transport, "server_observation", None)
        generation = read_generation(transport_observation)
        if generation is not None:
            return generation
        # Keep a caller-supplied observation as a last, read-only fallback
        # for small adapters that expose the proof only on the executor.
        return read_generation(self.server_observation)

    def _position_is_owned(self, position: Position) -> bool:
        if position.account_id != self.account.account_id:
            return False
        # The owner label is only a local fixture marker.  On an external
        # route, ownership must be bound to an intent restored/created by this
        # executor; a server payload cannot self-declare that it is ours.
        if not self._fixture_mode:
            return position.client_order_id in self._intents
        return position.owner == "mtf-lab" or position.client_order_id in self._intents

    def _active_open_results(self) -> tuple[OrderResult, ...]:
        closed_position_ids = {
            intent.position_id
            for intent_id, intent in self._intents.items()
            if intent.kind == "CLOSE"
            and intent.position_id is not None
            and self._results.get(intent_id) is not None
            and self._results[intent_id].state is OrderState.CLOSED
        }
        return tuple(
            result
            for result in self._results.values()
            if result.intent.kind == "OPEN"
            and result.state in {OrderState.FILLED, OrderState.PARTIAL}
            and not set(result.position_ids).intersection(closed_position_ids)
        )

    def _position_protection_state(self, positions: Sequence[Position]) -> str:
        if not self.policy.require_protective_stops:
            return "NOT_REQUIRED"
        owned = tuple(position for position in positions if self._position_is_owned(position))
        if not owned:
            expected_open = bool(self._active_open_results())
            if expected_open:
                return "UNKNOWN"
            return "NOT_APPLICABLE"
        return "OBSERVED" if all(position.protection_state == "OBSERVED" for position in owned) else "UNKNOWN"

    def _position_gate_reason(self, positions: Sequence[Position]) -> str | None:
        """Derive position/protection gates without changing executor state."""

        owned = tuple(position for position in positions if self._position_is_owned(position))
        position_halt: str | None = None
        if self.policy.require_protective_stops:
            expected_open_positions = self._active_open_results()
            observed_ids = {position.position_id for position in positions}
            missing_expected = any(
                not result.position_ids or not set(result.position_ids).issubset(observed_ids)
                for result in expected_open_positions
            )
            if missing_expected or any(position.protection_state != "OBSERVED" for position in owned):
                position_halt = _PROTECTION_RISK_HALT
        if position_halt is None and not self._fixture_mode and self.policy.max_holding_seconds is not None:
            now = _parse_time(self.clock())
            if any(position.opened_at is None for position in owned):
                position_halt = _HOLDING_RISK_HALT
            elif any(position.opened_at is not None and position.opened_at > now for position in owned):
                position_halt = _HOLDING_FUTURE_HALT
        return position_halt

    def _update_position_gates(self, positions: Sequence[Position]) -> None:
        position_halt = self._position_gate_reason(positions)
        derived = self._risk_limit_reason()
        if self._risk_halt_reason in _STRUCTURAL_RISK_HALTS or self._risk_halt_reason in _LATCHED_RISK_BREACHES:
            return
        self._risk_halt_reason = derived or position_halt

    def _account_positions(self) -> tuple[Position, ...]:
        # A failed refresh invalidates the read-only cache.  Retaining the old
        # tuple for diagnostics is harmless, but it must never be presented as
        # a current account snapshot after an incomplete network attempt.
        self._positions_complete = False
        raw = self.transport.list_positions(self.account.account_id)
        if raw is None:
            raise ExecutionTransportFailure(
                "transport did not return a complete account position snapshot",
                phase=SendPhase.SENT_NO_RESPONSE,
            )
        positions: list[Position] = []
        for position in tuple(raw):
            if not isinstance(position, Position):
                raise CorrelationError("transport returned an untyped position snapshot")
            if position.account_id != self.account.account_id:
                raise CorrelationError("transport returned a position for another account")
            if not position.position_id or position.quantity <= 0:
                raise CorrelationError("transport returned an invalid position")
            if position.owner != "mtf-lab" and position.client_order_id in self._intents:
                position = dataclasses.replace(position, owner="mtf-lab")
            positions.append(position)
        positions_tuple = tuple(positions)
        self._update_position_gates(positions_tuple)
        self._last_positions = positions_tuple
        self._last_positions_at = _parse_time(self.clock())
        self._last_positions_generation = self._current_connection_generation()
        self._positions_complete = True
        return positions_tuple

    def _cached_position_snapshot(self) -> tuple[tuple[Position, ...] | None, str | None]:
        """Return the last complete position snapshot when its proof is bounded.

        This helper is intentionally local and side-effect free: it only reads
        executor memory, the injected clock, and generation attributes already
        held by the transport.  It never invokes ``list_positions`` or a
        gateway/session refresh operation.
        """

        if self._last_positions is None:
            return None, "position_snapshot_missing"
        if not self._positions_complete:
            return None, "position_snapshot_incomplete"
        if self._last_positions_at is None:
            return None, "position_snapshot_time_missing"
        now = _parse_time(self.clock())
        age = (now - self._last_positions_at).total_seconds()
        if age < 0:
            return None, "position_snapshot_future"
        if age > float(self.policy.max_price_age_seconds):
            return None, "position_snapshot_stale"

        current_generation = self._current_connection_generation()
        saved_generation = self._last_positions_generation
        # The in-process fixture deliberately has no server generation.  Two
        # absent fixture generations are nevertheless the same stable local
        # session.  Non-fixture routes must prove both sides of the identity;
        # an absent external generation is never treated as a match.
        generation_matches = (
            current_generation == saved_generation
            if self._fixture_mode
            else current_generation is not None
            and saved_generation is not None
            and current_generation == saved_generation
        )
        if not generation_matches:
            return None, (
                "position_snapshot_generation_unobserved"
                if current_generation is None or saved_generation is None
                else "position_snapshot_generation_mismatch"
            )
        return self._last_positions, None

    def account_positions(self) -> tuple[Position, ...]:
        """Return the complete, account-scoped position snapshot."""

        with self._lock:
            return self._account_positions()

    def reconcile_positions(self) -> dict[str, Any]:
        """Refresh positions and expose ownership/exposure to a local supervisor."""

        with self._lock:
            try:
                positions = self._account_positions()
            except ExecutionError as exc:
                self._risk_halt_reason = "position_reconciliation_failed"
                return {
                    "state": "UNKNOWN",
                    "reason": type(exc).__name__,
                    "message": str(exc),
                    "positions": [],
                    "foreign_positions": [],
                    "exposure": None,
                    "protection_state": "UNKNOWN",
                }
            foreign = tuple(position for position in positions if not self._position_is_owned(position))
            exposure = sum((position.notional for position in positions), DecimalValue("0"))
            if not foreign and self._risk_halt_reason == "foreign_positions_present":
                self._risk_halt_reason = self._risk_limit_reason()
            return {
                "state": "VALID",
                "positions": [position.to_dict() for position in positions],
                "foreign_positions": [position.to_dict() for position in foreign],
                "owned_positions": [position.to_dict() for position in positions if position not in foreign],
                "exposure": _decimal_text(exposure),
                "protection_state": self._position_protection_state(positions),
                "observed_at": _iso(self._last_positions_at),
            }

    def _risk_status_projection(
        self,
        positions: tuple[Position, ...],
        *,
        position_state: str,
        position_error: str | None,
        positions_fresh: bool,
        refresh: bool,
    ) -> dict[str, Any]:
        """Build a risk projection from either a fresh or cached snapshot."""

        if position_state == "VALID":
            foreign = tuple(position for position in positions if not self._position_is_owned(position))
            exposure = sum((position.notional for position in positions), DecimalValue("0"))
            owned = tuple(position for position in positions if self._position_is_owned(position))
            if refresh:
                # A fresh query has already applied the stateful position
                # gates in ``_account_positions``.  Preserve the legacy
                # externally visible halt selection.
                halt_reason = self._risk_halt_reason
            elif self._risk_halt_reason in _STRUCTURAL_RISK_HALTS or self._risk_halt_reason in _LATCHED_RISK_BREACHES:
                halt_reason = self._risk_halt_reason
            else:
                # Cache reads must not mutate the latch, but they should still
                # reflect metric/protection/holding gates at read time.
                halt_reason = self._risk_limit_reason() or self._position_gate_reason(positions)
            state = "HALTED" if halt_reason else "VALID"
            protection_state = self._position_protection_state(positions)
        else:
            foreign = ()
            exposure = None
            owned = ()
            # The cache reason is diagnostic evidence, not a zero-position
            # observation.  Keep state UNKNOWN and make exposure unavailable.
            halt_reason = self._risk_halt_reason or position_error
            protection_state = "UNKNOWN"
            state = "HALTED" if refresh and self._risk_halt_reason else "UNKNOWN"

        return {
            "state": state,
            "halt_reason": halt_reason,
            "position_state": position_state,
            "position_error": position_error,
            "positions_fresh": bool(positions_fresh and position_state == "VALID"),
            "position_cache_reason": None if position_state == "VALID" else position_error,
            "position_observed_at": _iso(self._last_positions_at),
            "position_connection_generation": self._last_positions_generation,
            "positions": [position.to_dict() for position in positions],
            "owned_positions": [position.to_dict() for position in owned],
            "foreign_positions": [position.to_dict() for position in foreign],
            "position_count": len(positions),
            "foreign_position_count": len(foreign),
            "exposure": _decimal_text(exposure) if exposure is not None else None,
            "protection_state": protection_state,
            "reservations": len(self._reserved_ids),
            "inflight": len(self._inflight_ids),
            "metrics": {
                key: (_decimal_text(value) if value is not None else None) for key, value in self._risk_metrics.items()
            },
            "metrics_observed_at": _iso(self._risk_metrics_observed_at),
            "metrics_connection_generation": self._risk_metrics_generation,
            "recovery_required": self._recovery_required,
            "new_intents_enabled": self._active
            and not self._paused
            and not halt_reason
            and not self._recovery_required
            and bool(positions_fresh and position_state == "VALID"),
        }

    def risk_status(self, refresh: bool = True, *, refresh_positions: bool | None = None) -> dict[str, Any]:
        """Public risk state; refresh by default for backward compatibility.

        ``refresh=False`` is a pure projection over the latest bounded
        account snapshot.  It never calls the transport and reports UNKNOWN
        rather than converting a missing or invalid cache into zero exposure.
        """

        with self._lock:
            if refresh_positions is not None:
                refresh = refresh_positions
            if refresh:
                derived_halt = self._risk_limit_reason()
                if (
                    self._risk_halt_reason not in _STRUCTURAL_RISK_HALTS
                    and self._risk_halt_reason not in _LATCHED_RISK_BREACHES
                ):
                    self._risk_halt_reason = derived_halt
                try:
                    positions = self._account_positions()
                except BaseException as exc:
                    self._risk_halt_reason = self._risk_halt_reason or "position_reconciliation_failed"
                    return self._risk_status_projection(
                        (),
                        position_state="UNKNOWN",
                        position_error=type(exc).__name__,
                        positions_fresh=False,
                        refresh=True,
                    )
                return self._risk_status_projection(
                    positions,
                    position_state="VALID",
                    position_error=None,
                    positions_fresh=True,
                    refresh=True,
                )

            cached_positions, cache_reason = self._cached_position_snapshot()
            if cached_positions is None:
                return self._risk_status_projection(
                    (),
                    position_state="UNKNOWN",
                    position_error=cache_reason,
                    positions_fresh=False,
                    refresh=False,
                )
            return self._risk_status_projection(
                cached_positions,
                position_state="VALID",
                position_error=None,
                positions_fresh=True,
                refresh=False,
            )

    def cached_status(self) -> dict[str, Any]:
        """Return a no-I/O status projection from the bounded position cache."""

        return self.status(refresh=False)

    def activate(self) -> dict[str, Any]:
        """Explicit human/config gate; construction alone never enables send."""
        with self._lock:
            if self._recovery_required:
                raise RecoveryRequired("durable intent journal requires explicit recovery before activation")
            verify_demo_account(
                self.account, required_scopes=self.policy.required_scopes, endpoint=self.account.endpoint
            )
            if not self.transport.verify_endpoint(self.account.endpoint):
                raise EndpointRejected("demo endpoint verification failed")
            # Activation observes the whole account before arming new entries.
            # Foreign positions are not closed here; they are reported and
            # remain a hard block for new entries.
            try:
                positions = self._account_positions()
            except ExecutionError:
                # The explicit account/session gate remains visible, but no
                # new intent can be admitted until the position snapshot is
                # successfully reconciled.
                self._risk_halt_reason = "position_reconciliation_failed"
            else:
                if any(not self._position_is_owned(position) for position in positions):
                    self._risk_halt_reason = "foreign_positions_present"
                elif self._risk_halt_reason in {"position_reconciliation_failed", "foreign_positions_present"}:
                    self._risk_halt_reason = self._risk_limit_reason()
            self._risk_halt_reason = self._risk_halt_reason or self._risk_limit_reason()
            self._emit("ACTIVATED", "INTENT_RECORDED", "activation", {"account_id": self.account.account_id})
            self._active = True
            return self.status()

    def deactivate(self, reason: str = "manual") -> None:
        with self._lock:
            self._active = False
            self._pause_reason = str(reason)
            self._emit("DEACTIVATED", "UNKNOWN", "deactivation", {"reason": self._pause_reason})

    def pause(self, reason: str = "manual") -> None:
        with self._lock:
            self._paused = True
            self._pause_reason = str(reason)
            self._emit("PAUSED", "UNKNOWN", "pause", {"reason": self._pause_reason})

    def resume(self) -> None:
        with self._lock:
            self._paused = False
            self._pause_reason = None
            self._emit("RESUMED", "UNKNOWN", "resume", {})

    def _emit(
        self, event_type: str, state: str | OrderState, intent_id: str = "", details: Mapping[str, Any] | None = None
    ) -> ExecutionEvent:
        normalized = (
            state
            if isinstance(state, OrderState)
            else (OrderState(str(state)) if str(state) in {x.value for x in OrderState} else OrderState.UNKNOWN)
        )
        event = ExecutionEvent(event_type, str(intent_id), normalized, _parse_time(self.clock()), details or {})
        self._events.append(event)
        _store_call(self.intent_store, "record_event", event.to_dict())
        return event

    def _check_quote(self, symbol: str, quote: Quote, side: Side) -> None:
        if quote.symbol != symbol.upper():
            raise RiskLimitRejected("quote symbol differs from signal")
        if self.policy.allowed_symbols and symbol.upper() not in self.policy.allowed_symbols:
            raise RiskLimitRejected("symbol is not allowlisted")
        blocked = (
            "UNKNOWN",
            "STALE",
            "DISCONNECTED",
            "INVALID",
            "SYNTHETIC",
            "GAP",
            "UNRECONCILED",
            "OPEN",
            "LATE",
        )
        if quote.synthetic and not self._fixture_mode:
            raise RiskLimitRejected("synthetic quote cannot reach an external DEMO transport")
        if any(token in quote.quality.upper() for token in blocked) and not (
            self._fixture_mode and quote.quality.upper() == "SYNTHETIC"
        ):
            raise RiskLimitRejected(f"quote quality blocks execution: {quote.quality}")
        observation_source = str(getattr(self.server_observation, "source", "")).strip().lower()
        if not self._fixture_mode and observation_source not in {"fixture-server", "test"}:
            proof = self.server_observation
            valid_at = getattr(proof, "valid_at", None)
            if proof is None or not callable(valid_at) or not bool(valid_at(self.clock())):
                raise RiskLimitRejected("server observation is absent or expired")
            expected_session = getattr(proof, "session_id", None)
            expected_generation = getattr(proof, "connection_generation", None)
            if (
                not expected_session
                or quote.session_id != str(expected_session)
                or quote.connection_generation is None
                or str(quote.connection_generation) != str(expected_generation)
                or not quote.source_identity
            ):
                raise RiskLimitRejected("quote is not bound to the authenticated DEMO session")
        age = (_parse_time(self.clock()) - (quote.available_at or quote.timestamp)).total_seconds()
        if age < 0 or age > self.policy.max_price_age_seconds:
            raise RiskLimitRejected(f"quote is stale: age={age:.3f}s")
        if quote.spread > self.policy.max_spread:
            raise RiskLimitRejected(f"spread cap exceeded: {quote.spread} > {self.policy.max_spread}")

    def _own_positions(self) -> tuple[Position, ...]:
        positions = self._account_positions()
        if not self.policy.close_only_own_positions:
            return positions
        # A position is owned only when it was correlated to an intent created
        # by this executor. The owner marker is an additional fixture guard;
        # account-id equality alone is not sufficient to close a user's other
        # demo position.
        return tuple(position for position in positions if self._position_is_owned(position))

    def _resolve_quantity(self, requested: DecimalValue | float | str | None, *, kind: str = "OPEN") -> DecimalValue:
        quantity = self.policy.fixed_quantity if requested is None and kind == "OPEN" else requested
        if quantity is None:
            quantity = self.policy.fixed_quantity if kind == "OPEN" else quantity
        if quantity is None:
            raise RiskLimitRejected("quantity must be positive and finite")
        try:
            quantity = _decimal_value(quantity, "quantity", positive=True)
        except ValueError as exc:
            raise RiskLimitRejected(str(exc)) from exc
        if kind == "OPEN":
            if quantity > self.policy.max_quantity:
                raise RiskLimitRejected("quantity cap exceeded")
            if (
                self.policy.fixed_quantity is not None
                and abs(quantity - self.policy.fixed_quantity) > _DECIMAL_TOLERANCE
            ):
                raise RiskLimitRejected("fixed quantity policy rejects variable sizing")
        return quantity

    def _open_positions_for_entry(self) -> tuple[Position, ...]:
        try:
            return self._account_positions()
        except BaseException:
            self._risk_halt_reason = "position_reconciliation_failed"
            raise

    def _check_open_limits(self, data: Mapping[str, Any], quantity: DecimalValue, quote: Quote, side: Side) -> None:
        validator = getattr(self.transport, "validate_open_quantity", None)
        if callable(validator):
            validator(quantity)
        all_positions = self._open_positions_for_entry()
        if self._risk_halt_reason in {_PROTECTION_RISK_HALT, _HOLDING_RISK_HALT, _HOLDING_FUTURE_HALT}:
            raise RiskLimitRejected(f"new intents halted: {self._risk_halt_reason}")
        foreign = tuple(position for position in all_positions if not self._position_is_owned(position))
        if foreign and self.policy.block_on_foreign_positions:
            self._risk_halt_reason = "foreign_positions_present"
            raise RiskLimitRejected("unowned account positions require reconciliation before opening")
        unresolved = [
            result
            for result in self._results.values()
            if result.state in {OrderState.UNKNOWN, OrderState.SUBMITTED, OrderState.CLOSE_PARTIAL}
        ]
        if len(self._inflight_ids) >= self.policy.max_inflight_intents or unresolved or self._inflight_ids:
            raise RiskLimitRejected("unresolved order must be reconciled before opening another intent")
        self._check_order_window()
        if len(all_positions) >= self.policy.max_positions:
            raise RiskLimitRejected("max_positions cap exceeded")
        exposure = sum(
            (position.notional for position in all_positions), DecimalValue("0")
        ) + quantity * quote.price_for(side)
        if exposure > self.policy.max_exposure + _DECIMAL_TOLERANCE:
            raise RiskLimitRejected("max_exposure cap exceeded")
        if self.policy.require_protective_stops and not _has_protective_stop(data, self.policy):
            raise RiskLimitRejected("protective stop/take-profit is required by policy")

    def _check_order_window(self) -> None:
        if self.policy.max_orders_per_window is None:
            return
        now = _parse_time(self.clock())
        window = now - timedelta(seconds=float(self.policy.order_window_seconds))
        recent = sum(1 for item in self._intents.values() if item.kind == "OPEN" and item.created_at >= window)
        if recent >= self.policy.max_orders_per_window:
            raise RiskLimitRejected("max_orders_per_window cap exceeded")

    def _make_intent(
        self,
        signal: Any,
        quote: Quote,
        quantity: DecimalValue | float | str | None,
        *,
        kind: str = "OPEN",
        position_id: str | None = None,
    ) -> ExecutionIntent:
        data = _as_mapping(signal)
        kind = str(getattr(kind, "value", kind)).strip().upper()
        if kind not in {"OPEN", "CLOSE"}:
            raise ExecutionError("intent kind must be OPEN or CLOSE")
        signal_id = str(data.get("signal_id", data.get("id", "")))
        symbol = str(data.get("instrument", data.get("symbol", quote.symbol))).upper()
        if not signal_id:
            raise ExecutionError("signal_id is required")
        raw_mode = data.get("account_environment", data.get("account_mode", data.get("mode", "")))
        mode = str(getattr(raw_mode, "value", raw_mode)).strip().upper()
        if mode in {"REAL", "LIVE", "PRODUCTION"}:
            raise RealAccountForbidden("REAL/LIVE signal cannot reach demo executor")
        data_mode = data.get("data_mode", data.get("market_data_mode"))
        if data_mode is not None:
            data_mode = str(getattr(data_mode, "value", data_mode)).strip().upper() or None
        side = Side.parse(data.get("side", data.get("direction")))
        q = self._resolve_quantity(quantity, kind=kind)
        self._check_quote(symbol, quote, side)
        if kind == "OPEN":
            self._check_open_limits(data, q, quote, side)
        if kind == "OPEN" and signal_id in self._signal_intents:
            raise DuplicateIntent(f"signal already has an execution intent: {signal_id}")
        nonce = f"{signal_id}|{self.account.account_id}|{symbol}|{side.value}|{kind}|{position_id or ''}"
        intent_id = "intent_" + hashlib.sha256(nonce.encode()).hexdigest()[:32]
        if intent_id in self._intents or intent_id in self._results or _store_has_intent(self.intent_store, intent_id):
            raise DuplicateIntent(f"intent already exists: {intent_id}")
        options = _order_options(data, self.policy) if kind == "OPEN" else {}
        return ExecutionIntent(
            intent_id,
            signal_id,
            symbol,
            side,
            q,
            quote.price_for(side),
            _parse_time(self.clock()),
            self.account.account_id,
            kind,
            position_id,
            {
                "executor_version": self.VERSION,
                "endpoint": self.account.endpoint,
                "quote": quote.to_dict() if quote is not None else None,
                "no_martingale": self.policy.no_martingale,
                "virtual_only": self._virtual_only,
                "data_mode": data_mode,
                "order_options": options,
            },
        )

    def _make_close_intent(self, position: Position) -> ExecutionIntent:
        """Build a close intent without fabricating an entry/market quote."""

        signal_id = f"close:{position.position_id}"
        attempt = sum(
            1 for item in self._intents.values() if item.kind == "CLOSE" and item.position_id == position.position_id
        )
        nonce = (
            f"{signal_id}|{self.account.account_id}|{position.symbol}|{position.side.opposite.value}|CLOSE|"
            f"{_decimal_text(position.quantity)}|{attempt}"
        )
        intent_id = "intent_" + hashlib.sha256(nonce.encode()).hexdigest()[:32]
        if intent_id in self._intents or intent_id in self._results or _store_has_intent(self.intent_store, intent_id):
            raise DuplicateIntent(f"close intent already exists: {intent_id}")
        return ExecutionIntent(
            intent_id,
            signal_id,
            position.symbol,
            position.side.opposite,
            position.quantity,
            None,
            _parse_time(self.clock()),
            self.account.account_id,
            "CLOSE",
            position.position_id,
            {
                "executor_version": self.VERSION,
                "endpoint": self.account.endpoint,
                "quote": None,
                "economic_price_state": "UNKNOWN",
                "virtual_only": self._virtual_only,
            },
        )

    def _result_from_failure(self, intent: ExecutionIntent, failure: BaseException, *, event_type: str) -> OrderResult:
        phase = getattr(failure, "phase", SendPhase.SENT_NO_RESPONSE)
        if not isinstance(phase, SendPhase):
            try:
                phase = SendPhase(str(getattr(phase, "value", phase)))
            except ValueError:
                phase = SendPhase.SENT_NO_RESPONSE
        uncertain = bool(getattr(failure, "uncertain", True))
        state = OrderState.UNKNOWN if uncertain else OrderState.INTENT_RECORDED
        result = OrderResult(
            intent,
            state,
            unknown_reason=f"{event_type.lower()}:{phase.value}: {failure}" if uncertain else None,
            uncertainty_reason=phase.value,
        )
        self._results[intent.intent_id] = result
        self._update(intent, result)
        self._emit(
            event_type, state, intent.intent_id, {"phase": phase.value, "uncertain": uncertain, "no_blind_retry": True}
        )
        return result

    def _submit_intent(self, intent: ExecutionIntent) -> tuple[OrderResult, bool]:
        try:
            snapshot = self.transport.submit(intent, timeout_seconds=float(self.policy.timeout_seconds))
        except ExecutionLocalFailure as exc:
            result = self._result_from_failure(intent, exc, event_type="SUBMIT_LOCAL_ERROR")
            self._inflight_ids.discard(intent.intent_id)
            return result, False
        except ExecutionTransportFailure as exc:
            result = self._result_from_failure(
                intent, exc, event_type="SUBMIT_OUTCOME_UNKNOWN" if exc.uncertain else "SUBMIT_NOT_SENT"
            )
            self._inflight_ids.discard(intent.intent_id)
            return result, False
        except TimeoutError as exc:
            # Legacy in-process DemoTransport has no shared phase type; its
            # call is made after journaling, so retain the safe unknown.
            result = self._result_from_failure(intent, exc, event_type="SUBMIT_OUTCOME_UNKNOWN")
            self._inflight_ids.discard(intent.intent_id)
            return result, False
        except BaseException as exc:
            # A custom adapter may fail after writing bytes without using the
            # typed exception taxonomy.  Conservatively retain the intent as
            # uncertain rather than allowing a blind retry.
            result = self._result_from_failure(intent, exc, event_type="SUBMIT_OUTCOME_UNKNOWN")
            self._inflight_ids.discard(intent.intent_id)
            return result, False
        try:
            return self._result_from_snapshot(intent, snapshot), True
        except CorrelationError as exc:
            # Keep durable UNKNOWN evidence but surface the correlation
            # violation rather than hiding a server identity failure.
            self._result_from_failure(intent, exc, event_type="SUBMIT_OUTCOME_UNKNOWN")
            raise
        except BaseException as exc:
            return self._result_from_failure(intent, exc, event_type="SUBMIT_OUTCOME_UNKNOWN"), False
        finally:
            self._inflight_ids.discard(intent.intent_id)

    def submit_signal(
        self, signal: Any, quote: Quote | Mapping[str, Any], *, quantity: DecimalValue | float | str | None = None
    ) -> OrderResult:
        with self._lock:
            if self._recovery_required:
                raise RecoveryRequired("recover the durable intent journal before opening another intent")
            if not self._active:
                raise ActivationRequired("call activate() explicitly before demo execution")
            if self._paused:
                raise ExecutionPaused(f"new intents paused: {self._pause_reason or 'manual'}")
            if self._risk_halt_reason:
                raise RiskLimitRejected(f"new intents halted: {self._risk_halt_reason}")
            quote_obj = quote if isinstance(quote, Quote) else Quote.from_mapping(quote)
            intent = self._make_intent(signal, quote_obj, quantity)
            # This call must complete before submit is attempted. A store error
            # therefore fails closed and no transport side effect occurs.
            _store_call(self.intent_store, "record_intent", intent.to_dict())
            self._intents[intent.intent_id] = intent
            self._reserved_ids.add(intent.intent_id)
            self._inflight_ids.add(intent.intent_id)
            if intent.kind == "OPEN":
                self._signal_intents[intent.signal_id] = intent.intent_id
            self._emit(
                "INTENT_RECORDED",
                OrderState.INTENT_RECORDED,
                intent.intent_id,
                {"correlation_id": intent.intent_id, "before_transport": True},
            )
            result, persist = self._submit_intent(intent)
            if persist:
                self._results[intent.intent_id] = result
                self._update(intent, result)
            return result

    def _result_from_snapshot(
        self, intent: ExecutionIntent, snapshot: OrderSnapshot, *, reconciled: bool = False
    ) -> OrderResult:
        if not isinstance(snapshot, OrderSnapshot):
            raise CorrelationError("transport returned an invalid order snapshot")
        if snapshot.client_order_id != intent.intent_id:
            raise CorrelationError(
                f"transport response correlation mismatch: expected {intent.intent_id}, got {snapshot.client_order_id}"
            )
        if abs(snapshot.requested_quantity - intent.quantity) > _DECIMAL_TOLERANCE:
            raise CorrelationError(
                f"transport quantity mismatch for {intent.intent_id}: expected {intent.quantity}, "
                f"got {snapshot.requested_quantity}"
            )
        previous = self._results.get(intent.intent_id)
        if previous is not None and snapshot.filled_quantity + _DECIMAL_TOLERANCE < previous.filled_quantity:
            raise CorrelationError(f"transport fill quantity regressed for {intent.intent_id}")
        if (
            previous is not None
            and previous.order_id is not None
            and snapshot.order_id is not None
            and str(previous.order_id) != str(snapshot.order_id)
        ):
            raise CorrelationError(
                f"transport order identity changed for {intent.intent_id}: "
                f"expected {previous.order_id}, got {snapshot.order_id}"
            )
        if (
            previous is not None
            and previous.position_ids
            and snapshot.position_ids
            and set(previous.position_ids) != set(snapshot.position_ids)
        ):
            raise CorrelationError(f"transport position identity changed for {intent.intent_id}")
        if (
            intent.kind == "CLOSE"
            and intent.position_id is not None
            and snapshot.status in {OrderState.CLOSED, OrderState.CLOSE_PARTIAL}
            and intent.position_id not in snapshot.position_ids
        ):
            raise CorrelationError("close snapshot lacks the requested position identity")
        state = snapshot.status
        if state is OrderState.SUBMITTED and snapshot.filled_quantity > 0:
            state = OrderState.PARTIAL if snapshot.filled_quantity < intent.quantity else OrderState.FILLED
        if state is OrderState.PARTIAL and snapshot.filled_quantity <= 0:
            state = OrderState.SUBMITTED
        result = OrderResult(
            intent,
            state,
            snapshot.order_id,
            snapshot.filled_quantity,
            snapshot.fills,
            snapshot.reject_reason,
            snapshot.position_ids,
            unknown_reason=snapshot.uncertainty_reason,
            reconciled=reconciled,
            uncertainty_reason=snapshot.uncertainty_reason,
            stop_loss=snapshot.stop_loss,
            take_profit=snapshot.take_profit,
        )
        self._emit(
            "ORDER_UPDATE",
            state,
            intent.intent_id,
            {"order_id": snapshot.order_id, "filled_quantity": snapshot.filled_quantity, "reconciled": reconciled},
        )
        return result

    def _update(self, intent: ExecutionIntent, result: OrderResult) -> None:
        _store_call(self.intent_store, "update_intent", intent.intent_id, result.to_dict())

    def _reconcile_failure(
        self,
        intent: ExecutionIntent,
        existing: OrderResult | None,
        failure: BaseException,
    ) -> OrderResult:
        phase = getattr(failure, "phase", SendPhase.SENT_NO_RESPONSE)
        if not isinstance(phase, SendPhase):
            try:
                phase = SendPhase(str(getattr(phase, "value", phase)))
            except ValueError:
                phase = SendPhase.SENT_NO_RESPONSE
        uncertain = bool(getattr(failure, "uncertain", True))
        if existing is not None:
            if uncertain:
                existing.unknown_reason = f"reconcile_outcome_unknown:{phase.value}: {failure}"
                existing.uncertainty_reason = phase.value
                self._update(intent, existing)
                self._emit(
                    "RECONCILIATION_OUTCOME_UNKNOWN",
                    existing.state,
                    intent.intent_id,
                    {"phase": phase.value, "no_blind_retry": True},
                )
            return existing
        result = OrderResult(
            intent,
            OrderState.UNKNOWN if uncertain else OrderState.INTENT_RECORDED,
            unknown_reason=f"reconcile_outcome_unknown:{phase.value}: {failure}" if uncertain else None,
            uncertainty_reason=phase.value,
            reconciled=False,
        )
        self._results[intent.intent_id] = result
        self._update(intent, result)
        self._emit(
            "RECONCILIATION_OUTCOME_UNKNOWN" if uncertain else "RECONCILIATION_NOT_SENT",
            result.state,
            intent.intent_id,
            {"phase": phase.value, "no_blind_retry": True},
        )
        return result

    def _load_reconciliation_snapshot(
        self, intent: ExecutionIntent, existing: OrderResult | None
    ) -> OrderSnapshot | None | OrderResult:
        try:
            return self.transport.get_order(intent.intent_id)
        except ExecutionLocalFailure as exc:
            return self._reconcile_failure(intent, existing, exc)
        except ExecutionTransportFailure as exc:
            return self._reconcile_failure(intent, existing, exc)
        except TimeoutError as exc:
            return self._reconcile_failure(intent, existing, exc)
        except BaseException as exc:
            return self._reconcile_failure(intent, existing, exc)

    def _mark_reconciliation_missing(self, intent: ExecutionIntent, existing: OrderResult | None) -> OrderResult:
        result = existing or OrderResult(intent, OrderState.UNKNOWN, unknown_reason="order_not_found_for_correlation")
        result.reconciled = True
        result.uncertainty_reason = result.uncertainty_reason or "NOT_FOUND_NOT_PROOF"
        self._results[intent.intent_id] = result
        self._update(intent, result)
        self._emit("RECONCILIATION_UNRESOLVED", result.state, intent.intent_id, {"no_blind_retry": True})
        return result

    def reconcile(self, intent_id: str) -> OrderResult:
        with self._lock:
            key = str(intent_id)
            if key not in self._intents:
                raise CorrelationError(f"unknown intent: {key}")
            existing = self._results.get(key)
            if existing and existing.state in _TERMINAL_STATES:
                return existing
            intent = self._intents[key]
            snapshot = self._load_reconciliation_snapshot(intent, existing)
            if isinstance(snapshot, OrderResult):
                return snapshot
            if snapshot is None:
                return self._mark_reconciliation_missing(intent, existing)
            try:
                result = self._result_from_snapshot(intent, snapshot, reconciled=True)
            except BaseException as exc:
                return self._reconcile_failure(intent, existing, exc)
            if intent.kind == "CLOSE":
                result = self._confirm_close_state(intent, result)
            self._results[key] = result
            self._update(intent, result)
            return result

    def manage(self) -> tuple[OrderResult, ...]:
        """Poll orders and close known own positions past the holding deadline."""
        with self._lock:
            keys = [
                key
                for key, result in self._results.items()
                if result.state
                in {OrderState.UNKNOWN, OrderState.PARTIAL, OrderState.CLOSE_PARTIAL, OrderState.SUBMITTED}
            ]
            results = [self.reconcile(key) for key in keys]
            results.extend(self._holding_time_exits())
            return tuple(results)

    def _holding_time_exits(self) -> tuple[OrderResult, ...]:
        if self._fixture_mode or self.policy.max_holding_seconds is None:
            return ()
        try:
            positions = self._account_positions()
        except ExecutionError:
            self._risk_halt_reason = "position_reconciliation_failed"
            return ()
        now = _parse_time(self.clock())
        deadline_delta = timedelta(seconds=float(self.policy.max_holding_seconds))
        expired = tuple(
            position
            for position in positions
            if self._position_is_owned(position)
            and position.opened_at is not None
            and now >= position.opened_at + deadline_delta
        )
        return tuple(self.close_position(position.position_id) for position in expired)

    def positions(self) -> tuple[Position, ...]:
        return self._own_positions()

    def _confirm_close_state(self, intent: ExecutionIntent, result: OrderResult) -> OrderResult:
        """Confirm a close against a fresh account snapshot when possible."""

        if result.state not in {OrderState.CLOSED, OrderState.CLOSE_PARTIAL}:
            return result
        target_id = str(intent.position_id or "")
        try:
            positions = self._account_positions()
        except BaseException as exc:
            result.state = OrderState.UNKNOWN
            result.unknown_reason = f"close_confirmation_failed: {type(exc).__name__}"
            result.uncertainty_reason = "CLOSE_CONFIRMATION"
            result.reconciled = False
            self._emit(
                "CLOSE_CONFIRMATION_UNKNOWN",
                result.state,
                intent.intent_id,
                {"position_id": target_id, "no_blind_retry": True},
            )
            return result
        residual = next((position for position in positions if position.position_id == target_id), None)
        if residual is None:
            if result.state is OrderState.CLOSE_PARTIAL:
                result.state = OrderState.UNKNOWN
                result.unknown_reason = "close_response_partial_but_position_missing"
                result.uncertainty_reason = "CLOSE_CONFIRMATION"
            return result
        if result.state is OrderState.CLOSED:
            result.state = OrderState.CLOSE_PARTIAL
            result.filled_quantity = max(DecimalValue("0"), intent.quantity - residual.quantity)
            result.position_ids = (target_id,)
            result.unknown_reason = "residual_position_after_close"
            result.uncertainty_reason = "RESIDUAL_POSITION"
            result.reconciled = False
            self._emit(
                "CLOSE_RESIDUAL_POSITION",
                result.state,
                intent.intent_id,
                {"position_id": target_id, "remaining_quantity": residual.quantity, "no_blind_retry": True},
            )
        return result

    def close_position(self, position_id: str | Position) -> OrderResult:
        with self._lock:
            if self._recovery_required:
                raise RecoveryRequired("recover the durable intent journal before reducing exposure")
            requested_position_id = position_id.position_id if isinstance(position_id, Position) else str(position_id)
            position = self._owned_position(requested_position_id)
            if position is None:
                # Even while paused, management must not touch a foreign id.
                raise ForeignPosition(f"position is not owned by selected demo account: {requested_position_id}")
            prior = self._pending_close_for_position(requested_position_id)
            if prior is not None:
                # A late acknowledgement may still arrive for an UNKNOWN or
                # partial close.  Reconciliation is the only safe next step;
                # generating a new client id would create a second close for
                # the same position.
                reconciled = self.reconcile(prior.intent.intent_id)
                if reconciled.state in {
                    OrderState.UNKNOWN,
                    OrderState.SUBMITTED,
                    OrderState.PARTIAL,
                    OrderState.CLOSE_PARTIAL,
                }:
                    return reconciled
            intent = self._make_close_intent(position)
            recorded = self._record_close_intent(intent, position)
            if recorded is not None:
                return recorded
            result, from_snapshot = self._send_close_intent(intent, position)
            if not from_snapshot:
                return result
            result = self._confirm_close_state(intent, result)
            self._results[intent.intent_id] = result
            self._update(intent, result)
            return result

    def _owned_position(self, position_id: str) -> Position | None:
        return next((item for item in self._own_positions() if item.position_id == position_id), None)

    def _record_close_intent(self, intent: ExecutionIntent, position: Position) -> OrderResult | None:
        _store_call(self.intent_store, "record_intent", intent.to_dict())
        self._intents[intent.intent_id] = intent
        self._reserved_ids.add(intent.intent_id)
        self._inflight_ids.add(intent.intent_id)
        register = getattr(self.transport, "register_intent", None)
        if callable(register):
            try:
                register(intent)
            except BaseException as exc:
                result = self._result_from_failure(
                    intent,
                    ExecutionLocalFailure(f"transport intent registration failed: {exc}", phase=SendPhase.LOCAL_ERROR),
                    event_type="CLOSE_LOCAL_ERROR",
                )
                self._inflight_ids.discard(intent.intent_id)
                return result
        self._emit(
            "INTENT_RECORDED",
            OrderState.INTENT_RECORDED,
            intent.intent_id,
            {"kind": "CLOSE", "position_id": position.position_id, "before_transport": True},
        )
        return None

    def _send_close_intent(self, intent: ExecutionIntent, position: Position) -> tuple[OrderResult, bool]:
        try:
            snapshot = self.transport.close_position(
                position,
                client_order_id=intent.intent_id,
                timeout_seconds=float(self.policy.close_timeout_seconds or self.policy.timeout_seconds),
            )
        except ExecutionLocalFailure as exc:
            return self._result_from_failure(intent, exc, event_type="CLOSE_LOCAL_ERROR"), False
        except ExecutionTransportFailure as exc:
            event_type = "CLOSE_OUTCOME_UNKNOWN" if exc.uncertain else "CLOSE_NOT_SENT"
            return self._result_from_failure(intent, exc, event_type=event_type), False
        except TimeoutError as exc:
            return self._result_from_failure(intent, exc, event_type="CLOSE_OUTCOME_UNKNOWN"), False
        except BaseException as exc:
            return self._result_from_failure(intent, exc, event_type="CLOSE_OUTCOME_UNKNOWN"), False
        try:
            return self._result_from_snapshot(intent, snapshot), True
        except BaseException as exc:
            return self._result_from_failure(intent, exc, event_type="CLOSE_OUTCOME_UNKNOWN"), False
        finally:
            self._inflight_ids.discard(intent.intent_id)

    def _pending_close_for_position(self, position_id: str) -> OrderResult | None:
        """Return the newest non-terminal close for a position, if any."""

        candidates: list[tuple[datetime, str, OrderResult]] = []
        for intent_id, intent in self._intents.items():
            if intent.kind != "CLOSE" or intent.position_id != position_id:
                continue
            result = self._results.get(intent_id)
            if result is None:
                result = OrderResult(
                    intent,
                    OrderState.UNKNOWN,
                    unknown_reason="CLOSE_OUTCOME_NOT_RESTORED",
                    uncertainty_reason="RECOVERY",
                )
            if result.state in {
                OrderState.UNKNOWN,
                OrderState.SUBMITTED,
                OrderState.PARTIAL,
                OrderState.CLOSE_PARTIAL,
            }:
                candidates.append((intent.created_at, intent_id, result))
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item[0], item[1]))[2]

    def cancel_pending(self) -> tuple[OrderResult, ...]:
        """Cancel only orders with a known server order id.

        An order without a known id remains ``UNKNOWN``; this method never
        fabricates an id or treats a timeout as proof that cancellation won.
        """

        with self._lock:
            cancel = getattr(self.transport, "cancel_order", None)
            if not callable(cancel):
                return ()
            candidates = tuple(
                result
                for result in self._results.values()
                if result.state in {OrderState.SUBMITTED, OrderState.PARTIAL} and result.order_id
            )
            cancelled: list[OrderResult] = []
            for current in candidates:
                intent = current.intent
                try:
                    snapshot = cancel(
                        str(current.order_id),
                        client_order_id=intent.intent_id,
                        timeout_seconds=float(self.policy.close_timeout_seconds or self.policy.timeout_seconds),
                    )
                    if not isinstance(snapshot, OrderSnapshot):
                        raise CorrelationError("transport returned an invalid cancellation snapshot")
                    result = self._result_from_snapshot(intent, snapshot, reconciled=True)
                    # A cancellation response may omit previously observed
                    # fills; preserve the monotone local fill ledger.
                    if result.filled_quantity < current.filled_quantity:
                        result.filled_quantity = current.filled_quantity
                        result.fills = current.fills
                    self._results[intent.intent_id] = result
                    self._update(intent, result)
                except BaseException as exc:
                    result = self._reconcile_failure(intent, current, exc)
                cancelled.append(result)
            return tuple(cancelled)

    def reduce_exposure(self, position_ids: Iterable[str] | None = None) -> dict[str, Any]:
        """Pause entries and reduce only identified, owned positions.

        Foreign positions are returned as blocked evidence and are never sent
        to ``close_position``.  A position snapshot or close outcome that is
        not valid remains unresolved for the supervisor.
        """

        with self._lock:
            self._paused = True
            self._pause_reason = "risk_reduction"
            self._emit("RISK_REDUCTION_STARTED", "UNKNOWN", "risk-reduction", {})
            cancelled = self.cancel_pending()
            try:
                positions = self._account_positions()
            except BaseException as exc:
                self._risk_halt_reason = "position_reconciliation_failed"
                return {
                    "state": "UNKNOWN",
                    "reason": type(exc).__name__,
                    "cancelled": [item.to_dict() for item in cancelled],
                    "closed": [],
                    "blocked_foreign_positions": [],
                }
            requested = {str(item) for item in position_ids} if position_ids is not None else None
            selected = tuple(
                position
                for position in positions
                if (requested is None or position.position_id in requested) and self._position_is_owned(position)
            )
            blocked = tuple(
                position
                for position in positions
                if (requested is None or position.position_id in requested) and not self._position_is_owned(position)
            )
            closed: list[OrderResult] = []
            reduction_errors: list[str] = []
            for position in selected:
                try:
                    closed.append(self.close_position(position.position_id))
                except BaseException as exc:
                    reduction_errors.append(f"{position.position_id}:{type(exc).__name__}")
            unresolved = [
                item
                for item in (*cancelled, *closed)
                if item.state
                in {OrderState.UNKNOWN, OrderState.SUBMITTED, OrderState.PARTIAL, OrderState.CLOSE_PARTIAL}
            ]
            state = "UNKNOWN" if unresolved or reduction_errors else ("BLOCKED" if blocked else "COMPLETED")
            if blocked:
                self._risk_halt_reason = "foreign_positions_present"
            return {
                "state": state,
                "cancelled": [item.to_dict() for item in cancelled],
                "closed": [item.to_dict() for item in closed],
                "blocked_foreign_positions": [item.to_dict() for item in blocked],
                "errors": reduction_errors,
                "no_blind_retry": True,
            }

    def restore_intent(self, intent: ExecutionIntent, *, result: OrderResult | None = None) -> None:
        """Register a journaled intent for reconciliation without sending it.

        Recovery is deliberately a state-registration operation.  It never
        calls ``transport.submit``; a caller must invoke ``reconcile``
        explicitly against the same client correlation id.
        """
        if not isinstance(intent, ExecutionIntent):
            raise TypeError("restore_intent requires ExecutionIntent")
        if str(intent.account_id) != self.account.account_id:
            raise CorrelationError("recovered intent belongs to another demo account")
        with self._lock:
            existing = self._intents.get(intent.intent_id)
            if existing is not None and existing != intent:
                raise CorrelationError(f"intent already restored with different payload: {intent.intent_id}")
            self._intents[intent.intent_id] = intent
            self._reserved_ids.add(intent.intent_id)
            register = getattr(self.transport, "register_intent", None)
            if callable(register):
                register(intent)
            if intent.kind == "OPEN":
                prior = self._signal_intents.get(intent.signal_id)
                if prior is not None and prior != intent.intent_id:
                    raise CorrelationError(f"signal already mapped to another intent: {intent.signal_id}")
                self._signal_intents[intent.signal_id] = intent.intent_id
            if result is not None:
                if (
                    result.intent.intent_id != intent.intent_id
                    or str(result.intent.account_id) != self.account.account_id
                ):
                    raise CorrelationError("recovered result does not match intent/account")
                self._results[intent.intent_id] = result

    def clear_risk_halt(self) -> dict[str, Any]:
        """Clear a derived halt only after a fresh valid position snapshot."""

        with self._lock:
            positions = self._account_positions()
            if any(not self._position_is_owned(position) for position in positions):
                raise RiskLimitRejected("foreign positions remain; the risk halt stays latched")
            self._risk_halt_reason = self._risk_limit_reason()
            return self.risk_status()

    def result(self, intent_id: str) -> OrderResult | None:
        return self._results.get(str(intent_id))

    def events(self) -> tuple[ExecutionEvent, ...]:
        return tuple(self._events)

    def execute(
        self, signal: Any, quote: Quote | Mapping[str, Any], quantity: DecimalValue | float | str | None = None
    ) -> OrderResult:
        return self.submit_signal(signal, quote, quantity=quantity)

    def submit(
        self, signal: Any, quote: Quote | Mapping[str, Any], quantity: DecimalValue | float | str | None = None
    ) -> OrderResult:
        return self.submit_signal(signal, quote, quantity=quantity)

    def reconcile_order(self, intent_id: str) -> OrderResult:
        return self.reconcile(intent_id)

    def close(self, position_id: str | Position) -> OrderResult:
        return self.close_position(position_id)

    def status(self, refresh: bool = True, *, refresh_positions: bool | None = None) -> dict[str, Any]:
        """Return executor status, refreshing positions by default.

        ``refresh=False`` is kept as the explicit read-only path used by
        :meth:`cached_status`; the default remains a fresh account query for
        callers that relied on the historical ``status()`` behavior.
        """

        with self._lock:
            if refresh_positions is not None:
                refresh = refresh_positions
            counts: MutableMapping[str, int] = {state.value: 0 for state in OrderState}
            for result in self._results.values():
                counts[result.state.value] = counts.get(result.state.value, 0) + 1
            # Build the projection from exactly one risk/position read.  Do
            # not call ``_own_positions`` or another account query here.
            risk = self.risk_status(refresh=refresh)
            return {
                "executor_version": self.VERSION,
                "environment": self.account.environment,
                "endpoint": self.account.endpoint,
                "account_id": self.account.account_id,
                "active": self._active,
                "paused": self._paused,
                "pause_reason": self._pause_reason,
                "new_intents_enabled": bool(risk["new_intents_enabled"]),
                "virtual_only": self._virtual_only,
                "fixture_mode": self._fixture_mode,
                "recovery_required": self._recovery_required,
                "positions_fresh": bool(risk["positions_fresh"]),
                "position_cache_reason": risk["position_cache_reason"],
                "counts": dict(counts),
                "pending_management": sum(
                    counts.get(state.value, 0)
                    for state in (
                        OrderState.UNKNOWN,
                        OrderState.PARTIAL,
                        OrderState.CLOSE_PARTIAL,
                        OrderState.SUBMITTED,
                    )
                ),
                "own_positions": list(risk["owned_positions"]),
                "account_positions": list(risk["positions"]),
                "risk": risk,
                "risk_policy": self.policy.to_dict(),
                "no_blind_retry": True,
            }


# Friendly aliases for a future adapter registry.
CTraderExecutor = CTraderDemoExecutor
DemoExecutionTransport = DemoTransport
SimulatedCTraderTransport = DemoTransport
CTraderDemoTransport = DemoTransport
DemoExecutionConfig = ExecutionPolicy


# ---------------------------------------------------------------------------
# Generic adapter helpers


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    for method_name in ("to_dict", "as_dict", "model_dump"):
        method = getattr(value, method_name, None)
        if callable(method):
            payload = cast(Mapping[str, Any], method())
            return dict(payload)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    if hasattr(value, "__dict__"):
        return {key: val for key, val in vars(value).items() if not key.startswith("_")}
    raise TypeError(f"expected mapping/dataclass/object, got {type(value)!r}")


def _store_call(store: Any, method: str, *args: Any) -> Any:
    """Call one of a small set of optional journal method spellings."""
    candidates = {
        "record_intent": (
            "record_intent",
            "record_execution_intent",
            "persist_intent",
            "persist_execution_intent",
            "save_intent",
            "save_execution_intent",
        ),
        "record_event": ("record_event", "record_execution_event", "append_event", "save_event"),
        "update_intent": ("update_intent", "update_execution_intent", "persist_update", "save_intent_update"),
    }[method]
    for name in candidates:
        callback = getattr(store, name, None)
        if callable(callback):
            return callback(*args)
    raise TypeError(f"intent_store does not implement {method}: expected one of {candidates}")


def _store_has_intent(store: Any, intent_id: str) -> bool:
    callback = getattr(store, "has_intent", None)
    if callable(callback):
        try:
            return bool(callback(str(intent_id)))
        except Exception as exc:
            # A failing journal must never be treated as an empty journal.
            raise ExecutionLocalFailure("intent journal identity lookup failed", phase=SendPhase.LOCAL_ERROR) from exc
    return False


__all__ = [
    "ActivationRequired",
    "CTraderDemoExecutor",
    "CTraderExecutor",
    "CorrelationError",
    "DecimalValue",
    "DemoAccount",
    "DemoAccountRequired",
    "DemoExecutionConfig",
    "DemoExecutionTransport",
    "DemoTransport",
    "SimulatedCTraderTransport",
    "CTraderDemoTransport",
    "DuplicateIntent",
    "EndpointRejected",
    "ExecutionError",
    "ExecutionEvent",
    "ExecutionIntent",
    "ExecutionPolicy",
    "ExecutionTransport",
    "Fill",
    "ForeignPosition",
    "IntentStore",
    "JsonlIntentStore",
    "MemoryIntentStore",
    "OrderResult",
    "OrderSnapshot",
    "OrderState",
    "Position",
    "Quote",
    "QuoteQuality",
    "RecoveryRequired",
    "RealAccountForbidden",
    "RiskLimitRejected",
    "SafetyViolation",
    "ScopeRejected",
    "Side",
    "ExecutionPaused",
    "verify_demo_account",
]
