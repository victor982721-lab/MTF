"""Bounded, secret-free diagnostics for one cTrader spot observation.

The diagnostic is deliberately separate from normalization.  It records what
arrived on the wire and the local receipt/availability evidence without
changing timestamps, prices, or quote-book state.  It is suitable for a small
failure receipt when a source observation cannot become an ``Event``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


@dataclass(frozen=True, slots=True)
class CTraderSpotDiagnostic:
    """Minimal audit facts for a single raw ``ProtoOASpotEvent``."""

    event_time: datetime | None
    timestamp_original: Any
    timestamp_unit: str | None
    received_at: datetime | None
    available_at: datetime | None
    bid_raw: Any
    ask_raw: Any
    price_scale: int | None
    digits: int | None
    fields_present: tuple[str, ...]
    updated_sides: tuple[str, ...]
    partial_update: bool
    bid_ask_relation: str
    received_minus_event_seconds: float | None
    available_minus_event_seconds: float | None
    timing_status: str
    timing_attribution: str | None = None

    @property
    def timing_invalid(self) -> bool:
        return self.timing_status in {"RECEIVED_BEFORE_EVENT", "AVAILABLE_BEFORE_EVENT"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_time": _iso(self.event_time),
            "timestamp_original": self.timestamp_original,
            "timestamp_unit": self.timestamp_unit,
            "received_at": _iso(self.received_at),
            "available_at": _iso(self.available_at),
            "bid_raw": self.bid_raw,
            "ask_raw": self.ask_raw,
            "price_scale": self.price_scale,
            "digits": self.digits,
            "fields_present": list(self.fields_present),
            "updated_sides": list(self.updated_sides),
            "partial_update": self.partial_update,
            "bid_ask_relation": self.bid_ask_relation,
            "received_minus_event_seconds": self.received_minus_event_seconds,
            "available_minus_event_seconds": self.available_minus_event_seconds,
            "timing_status": self.timing_status,
            "timing_attribution": self.timing_attribution,
        }


def build_spot_diagnostic(
    *,
    event_time: datetime | None,
    timestamp_original: Any,
    timestamp_unit: str | None,
    received_at: datetime | None,
    available_at: datetime | None,
    bid_raw: Any,
    ask_raw: Any,
    price_scale: int | None,
    digits: int | None,
    fields_present: Iterable[str],
    updated_sides: Iterable[str] | None = None,
) -> CTraderSpotDiagnostic:
    """Build a bounded diagnostic from already-parsed adapter values.

    Raw quote relation is compared before any floating-point conversion.  A
    relation of ``bid >= ask`` is intentionally reported as ``CROSSED`` for
    the operational quote gate; this does not claim that the broker's source
    is corrupt.  Missing sides remain ``INCOMPLETE`` and preserve the partial
    update evidence.
    """

    fields = tuple(dict.fromkeys(str(item) for item in fields_present))
    if updated_sides is None:
        sides = tuple(side for side in ("bid", "ask") if side in fields)
    else:
        present_sides = {str(item) for item in updated_sides if str(item) in {"bid", "ask"}}
        sides = tuple(side for side in ("bid", "ask") if side in present_sides)
    partial = set(sides) != {"bid", "ask"}
    relation = _bid_ask_relation(bid_raw, ask_raw)
    received_delta = _delta_seconds(received_at, event_time)
    available_delta = _delta_seconds(available_at, event_time)
    timing_status, attribution = _timing_status(event_time, received_at, available_at)
    return CTraderSpotDiagnostic(
        event_time,
        timestamp_original,
        str(timestamp_unit) if timestamp_unit is not None else None,
        received_at,
        available_at,
        bid_raw,
        ask_raw,
        int(price_scale) if price_scale is not None else None,
        int(digits) if digits is not None else None,
        fields,
        sides,
        partial,
        relation,
        received_delta,
        available_delta,
        timing_status,
        attribution,
    )


def _bid_ask_relation(bid_raw: Any, ask_raw: Any) -> str:
    if bid_raw is None or ask_raw is None:
        return "INCOMPLETE"
    try:
        bid = _exact_number(bid_raw)
        ask = _exact_number(ask_raw)
    except (InvalidOperation, TypeError, ValueError):
        return "UNCOMPARABLE"
    if bid >= ask:
        return "CROSSED"
    return "ORDERED"


def _exact_number(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("boolean is not a quote")
    return Decimal(str(value))


def _delta_seconds(left: datetime | None, right: datetime | None) -> float | None:
    if left is None or right is None:
        return None
    return (left.astimezone(UTC) - right.astimezone(UTC)).total_seconds()


def _timing_status(
    event_time: datetime | None,
    received_at: datetime | None,
    available_at: datetime | None,
) -> tuple[str, str | None]:
    if event_time is None:
        return "UNKNOWN_EVENT_TIME", None
    if available_at is not None and available_at < event_time:
        return "AVAILABLE_BEFORE_EVENT", "UNRESOLVED_SERVER_OR_LOCAL_CLOCK"
    if received_at is not None and received_at < event_time:
        return "RECEIVED_BEFORE_EVENT", "UNRESOLVED_SERVER_OR_LOCAL_CLOCK"
    return "OK", None


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


__all__ = ["CTraderSpotDiagnostic", "build_spot_diagnostic"]
