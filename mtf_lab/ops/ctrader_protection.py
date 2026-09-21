"""Project immutable risk levels into cTrader MARKET protection options.

The RiskExit/EntryPlan layer keeps its theoretical absolute levels.  cTrader's
MARKET order contract is different: protective levels are relative distances
from the broker's eventual fill, encoded as integer multiples of ``1e-5``
price units.  This module is deliberately a pure boundary between those two
representations.  It neither changes the plan nor submits an order.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal, InvalidOperation
from typing import Any

from ..core.numeric import decimal_context

_ZERO = Decimal("0")
_PROTOCOL_PRICE_UNIT = Decimal("0.00001")
_PROTOCOL_SCALE = Decimal("100000")
_INT64_MAX = 2**63 - 1
_ROUNDING_BASIS = "TOWARD_ENTRY_NO_RISK_INCREASE"
_RELATIVE_LEVELS_ANCHOR = "BROKER_ACTUAL_FILL"


class MarketProtectionError(ValueError):
    """The theoretical protection cannot be represented safely on cTrader."""


def _decimal(value: Any, *, name: str) -> Decimal:
    """Return a finite Decimal without inheriting the caller's context."""

    if value is None:
        raise MarketProtectionError(f"{name} es obligatorio")
    if isinstance(value, bool):
        raise MarketProtectionError(f"{name} no puede ser booleano")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MarketProtectionError(f"{name} no es decimal válido") from exc
    if not result.is_finite():
        raise MarketProtectionError(f"{name} debe ser finito")
    return result


def _positive(value: Any, *, name: str) -> Decimal:
    result = _decimal(value, name=name)
    if result <= _ZERO:
        raise MarketProtectionError(f"{name} debe ser positivo")
    return result


def _direction(value: Any) -> str:
    if value is None or isinstance(value, bool):
        raise MarketProtectionError("direction debe ser LONG/BUY/UP o SHORT/SELL/DOWN")
    raw = str(getattr(value, "value", value)).strip().upper()
    if raw in {"LONG", "BUY", "UP"}:
        return "BUY"
    if raw in {"SHORT", "SELL", "DOWN"}:
        return "SELL"
    raise MarketProtectionError("direction no soportada")


def _floor_distance(distance: Decimal, quantum: Decimal, *, name: str) -> tuple[Decimal, Decimal]:
    """Floor a distance toward the entry and return (wire distance, delta)."""

    with decimal_context():
        units = distance / quantum
        whole_units = units.to_integral_value(rounding=ROUND_FLOOR)
        projected = whole_units * quantum
        delta = distance - projected
    if projected <= _ZERO:
        raise MarketProtectionError(f"{name} queda en cero después de cuantizar hacia la entrada")
    if delta < _ZERO or delta >= quantum:
        raise MarketProtectionError(f"{name} no pudo cuantizarse de forma determinista")
    return projected, delta


def _wire_distance(distance: Decimal, *, name: str) -> tuple[int, Decimal]:
    """Encode a price distance as the official positive int64 wire value."""

    with decimal_context():
        scaled = distance * _PROTOCOL_SCALE
        integral = scaled.to_integral_value(rounding=ROUND_FLOOR)
    if scaled != integral:
        raise MarketProtectionError(f"{name} no es representable en unidades de 1e-5")
    if integral <= _ZERO or integral > _INT64_MAX:
        raise MarketProtectionError(f"{name} excede el rango positivo int64")
    try:
        wire = int(integral)
    except (OverflowError, ValueError) as exc:
        raise MarketProtectionError(f"{name} excede int64") from exc
    if wire <= 0 or wire > _INT64_MAX:
        raise MarketProtectionError(f"{name} excede el rango positivo int64")
    with decimal_context():
        wire_price = Decimal(wire) / _PROTOCOL_SCALE
    return wire, wire_price


@dataclass(frozen=True, slots=True)
class MarketProtection:
    """Immutable theoretical-to-wire protection projection for one MARKET order."""

    direction: str
    entry_price: Decimal
    planned_stop_loss: Decimal
    planned_take_profit: Decimal
    planned_stop_distance: Decimal
    planned_take_profit_distance: Decimal
    wire_stop_distance: Decimal
    wire_take_profit_distance: Decimal
    reference_stop_loss: Decimal
    reference_take_profit: Decimal
    relative_stop_loss: int
    relative_take_profit: int
    price_quantum: Decimal
    executable_bid: Decimal
    executable_ask: Decimal
    minimum_stop_distance: Decimal
    stop_rounding_delta: Decimal
    take_profit_rounding_delta: Decimal
    rounding_basis: str = _ROUNDING_BASIS
    relative_levels_anchored_to: str = _RELATIVE_LEVELS_ANCHOR
    reference_levels_observed: bool = False

    @property
    def side(self) -> str:
        """cTrader side spelling for callers that use ``side`` terminology."""

        return self.direction

    @property
    def planned_stop(self) -> Decimal:
        return self.planned_stop_loss

    @property
    def planned_take(self) -> Decimal:
        return self.planned_take_profit

    @property
    def wire_stop_loss(self) -> Decimal:
        """Reference absolute stop derived from the encoded wire distance."""

        return self.reference_stop_loss

    @property
    def wire_take_profit(self) -> Decimal:
        """Reference absolute target derived from the encoded wire distance."""

        return self.reference_take_profit

    @property
    def wire_stop_loss_distance(self) -> Decimal:
        return self.wire_stop_distance

    @property
    def projected_stop_loss(self) -> Decimal:
        return self.reference_stop_loss

    @property
    def projected_take_profit(self) -> Decimal:
        return self.reference_take_profit

    def to_order_options(self) -> dict[str, int]:
        """Return only the two cTrader relative MARKET protection options."""

        return {
            "relative_stop_loss": self.relative_stop_loss,
            "relative_take_profit": self.relative_take_profit,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return an audit-safe JSON-serializable representation.

        The absolute reference levels are derived from the wire distances and
        are explicitly marked as unobserved; the broker's actual fill may
        differ from ``entry_price``.
        """

        text = str
        return {
            "direction": self.direction,
            "entry_price": text(self.entry_price),
            "planned_stop_loss": text(self.planned_stop_loss),
            "planned_take_profit": text(self.planned_take_profit),
            "planned_stop_distance": text(self.planned_stop_distance),
            "planned_take_profit_distance": text(self.planned_take_profit_distance),
            "wire_stop_distance": text(self.wire_stop_distance),
            "wire_take_profit_distance": text(self.wire_take_profit_distance),
            "stop_rounding_delta": text(self.stop_rounding_delta),
            "take_profit_rounding_delta": text(self.take_profit_rounding_delta),
            "reference_stop_loss": text(self.reference_stop_loss),
            "reference_take_profit": text(self.reference_take_profit),
            "reference_levels_observed": self.reference_levels_observed,
            "relative_stop_loss": self.relative_stop_loss,
            "relative_take_profit": self.relative_take_profit,
            "price_quantum": text(self.price_quantum),
            "executable_bid": text(self.executable_bid),
            "executable_ask": text(self.executable_ask),
            "minimum_stop_distance": text(self.minimum_stop_distance),
            "protocol_price_scale": int(_PROTOCOL_SCALE),
            "rounding_basis": self.rounding_basis,
            "relative_levels_anchored_to": self.relative_levels_anchored_to,
        }


def project_market_protection(
    *,
    direction: Any,
    entry_price: Any,
    stop_loss: Any,
    take_profit: Any,
    price_quantum: Any,
    executable_bid: Any,
    executable_ask: Any,
    minimum_stop_distance: Any,
) -> MarketProtection:
    """Project immutable theoretical levels into cTrader MARKET wire values.

    ``stop_loss`` and ``take_profit`` remain the theoretical EntryPlan levels.
    Both distances are floored independently to the observed price quantum,
    never away from the entry.  The stop is then checked against the observed
    executable side without widening it to satisfy the broker minimum.
    """

    side = _direction(direction)
    entry = _positive(entry_price, name="entry_price")
    planned_stop = _positive(stop_loss, name="stop_loss")
    planned_take = _positive(take_profit, name="take_profit")
    quantum = _positive(price_quantum, name="price_quantum")
    bid = _positive(executable_bid, name="executable_bid")
    ask = _positive(executable_ask, name="executable_ask")
    minimum = _positive(minimum_stop_distance, name="minimum_stop_distance")

    with decimal_context():
        quantum_units = quantum / _PROTOCOL_PRICE_UNIT
        quantum_integral = quantum_units.to_integral_value(rounding=ROUND_FLOOR)
    if quantum_units != quantum_integral:
        raise MarketProtectionError("price_quantum debe ser múltiplo entero de 0.00001")
    if ask < bid:
        raise MarketProtectionError("executable_ask no puede ser menor que executable_bid")

    with decimal_context():
        if side == "BUY":
            planned_stop_distance = entry - planned_stop
            planned_take_distance = planned_take - entry
        else:
            planned_stop_distance = planned_stop - entry
            planned_take_distance = entry - planned_take
    if planned_stop_distance <= _ZERO:
        raise MarketProtectionError("stop_loss debe estar a distancia direccional positiva de entry_price")
    if planned_take_distance <= _ZERO:
        raise MarketProtectionError("take_profit debe estar a distancia direccional positiva de entry_price")

    wire_stop_distance, stop_delta = _floor_distance(planned_stop_distance, quantum, name="stop_loss distance")
    wire_take_distance, take_delta = _floor_distance(planned_take_distance, quantum, name="take_profit distance")
    relative_stop_loss, encoded_stop_distance = _wire_distance(wire_stop_distance, name="relative_stop_loss")
    relative_take_profit, encoded_take_distance = _wire_distance(wire_take_distance, name="relative_take_profit")

    with decimal_context():
        if side == "BUY":
            reference_stop = entry - encoded_stop_distance
            reference_take = entry + encoded_take_distance
            observed_stop_distance = bid - reference_stop
        else:
            reference_stop = entry + encoded_stop_distance
            reference_take = entry - encoded_take_distance
            observed_stop_distance = reference_stop - ask
    if reference_stop <= _ZERO or reference_take <= _ZERO:
        raise MarketProtectionError("nivel de protección wire no puede ser cero o negativo")
    if observed_stop_distance < minimum:
        raise MarketProtectionError("stop_loss wire queda demasiado cerca del precio ejecutable observado")

    return MarketProtection(
        direction=side,
        entry_price=entry,
        planned_stop_loss=planned_stop,
        planned_take_profit=planned_take,
        planned_stop_distance=planned_stop_distance,
        planned_take_profit_distance=planned_take_distance,
        wire_stop_distance=encoded_stop_distance,
        wire_take_profit_distance=encoded_take_distance,
        reference_stop_loss=reference_stop,
        reference_take_profit=reference_take,
        relative_stop_loss=relative_stop_loss,
        relative_take_profit=relative_take_profit,
        price_quantum=quantum,
        executable_bid=bid,
        executable_ask=ask,
        minimum_stop_distance=minimum,
        stop_rounding_delta=stop_delta,
        take_profit_rounding_delta=take_delta,
    )


__all__ = ["MarketProtection", "MarketProtectionError", "project_market_protection"]
