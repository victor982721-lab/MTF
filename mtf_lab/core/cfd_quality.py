"""Typed bid/ask quality and freshness contracts for the CFD domain.

The provider-facing ``Event`` model is intentionally permissive and stores
some diagnostics in metadata.  A paper fill must not inherit that ambiguity.
This module gives the CFD adapter a small, exact vocabulary for the facts that
can make one side of a quote unusable.  It does not inspect free-form text and
it never upgrades a weaker quality label to a usable one.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any


class QuoteQuality(StrEnum):
    """Quality states understood by the CFD fill boundary."""

    VALID = "VALID"
    VALIDATED = "VALIDATED"
    OK = "OK"
    GOOD = "GOOD"
    CLOSED_VALID = "CLOSED_VALID"
    SYNTHETIC = "SYNTHETIC"
    SYNTHETIC_VALID = "SYNTHETIC_VALID"
    SYNTHETIC_VALIDATED = "SYNTHETIC_VALIDATED"
    DATA_QUALITY_VALIDATED = "DATA_QUALITY_VALIDATED"
    PUBLIC_PROVIDER_CLOSED = "PUBLIC_PROVIDER_CLOSED"
    UNKNOWN = "UNKNOWN"
    INVALID = "INVALID"
    SNAPSHOT = "SNAPSHOT"
    DISCONNECTED = "DISCONNECTED"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    SESSION_MISMATCH = "SESSION_MISMATCH"
    STALE = "STALE"


class QuoteReason(StrEnum):
    """Machine-readable reasons for a side or whole quote being unusable."""

    MISSING_BID = "MISSING_BID"
    MISSING_ASK = "MISSING_ASK"
    MISSING_SOURCE_TIMESTAMP = "MISSING_SOURCE_TIMESTAMP"
    STALE = "STALE"
    INVALID_QUALITY = "INVALID_QUALITY"
    SNAPSHOT = "SNAPSHOT"
    CROSSED = "CROSSED"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    DISCONNECTED = "DISCONNECTED"
    SESSION_MISMATCH = "SESSION_MISMATCH"
    FUTURE_SOURCE_TIMESTAMP = "FUTURE_SOURCE_TIMESTAMP"
    FUTURE_AVAILABILITY = "FUTURE_AVAILABILITY"
    SPREAD_UNAVAILABLE = "SPREAD_UNAVAILABLE"


class QuoteSide(StrEnum):
    BID = "bid"
    ASK = "ask"
    MID = "mid"


_USABLE_QUALITY = frozenset(
    {
        QuoteQuality.VALID,
        QuoteQuality.VALIDATED,
        QuoteQuality.OK,
        QuoteQuality.GOOD,
        QuoteQuality.CLOSED_VALID,
        QuoteQuality.SYNTHETIC,
        QuoteQuality.SYNTHETIC_VALID,
        QuoteQuality.SYNTHETIC_VALIDATED,
        QuoteQuality.DATA_QUALITY_VALIDATED,
        QuoteQuality.PUBLIC_PROVIDER_CLOSED,
    }
)


def _utc(value: Any, *, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{name} debe ser ISO-8601 con zona: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} debe incluir zona horaria")
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


def _decimal(value: Any, *, name: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} no es decimal válido: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"{name} debe ser finito")
    return result


def quality_from(value: Any) -> QuoteQuality:
    """Parse one exact quality value; unknown text is not accepted as valid."""

    if isinstance(value, QuoteQuality):
        return value
    if isinstance(value, Mapping):
        # A scalar VALID state cannot override a blocking structured reason or
        # flag.  The helper is defined below but resolved at call time.
        if quality_reasons_from(value):
            return QuoteQuality.UNKNOWN
        if "status" in value:
            value = value["status"]
        elif "state" in value:
            value = value["state"]
        elif "quality" in value:
            value = value["quality"]
        else:
            return QuoteQuality.UNKNOWN
    if value is None:
        return QuoteQuality.UNKNOWN
    raw = str(value).strip().upper().replace("-", "_").replace(" ", "_")
    # This is an explicit lookup, not a substring or prefix rule.  In
    # particular, ``VALID_WITH_WARNINGS`` does not become VALID.
    try:
        return QuoteQuality(raw)
    except ValueError:
        return QuoteQuality.UNKNOWN


_QUALITY_REASON_ALIASES = {
    "INVALID": QuoteReason.INVALID_QUALITY,
    "UNKNOWN": QuoteReason.INVALID_QUALITY,
    "VALID": None,
    "VALIDATED": None,
    "OK": None,
    "GOOD": None,
    "CLOSED_VALID": None,
    "SYNTHETIC": None,
    "SYNTHETIC_VALID": None,
    "SYNTHETIC_VALIDATED": None,
    "DATA_QUALITY_VALIDATED": None,
    "PUBLIC_PROVIDER_CLOSED": None,
}


def quality_reasons_from(value: Any) -> tuple[QuoteReason, ...]:
    """Extract structured quality flags/reasons without upgrading quality.

    ``quality_from`` intentionally returns the scalar state for compatibility.
    A structured provider value may nevertheless carry a blocking reason while
    its state says ``VALID``; this helper preserves that evidence for the leg
    assessment instead of silently discarding it.
    """

    if value is None:
        return ()
    raw_values: list[Any] = []
    if isinstance(value, Mapping):
        for key in ("reasons", "flags"):
            raw = value.get(key, ())
            if isinstance(raw, (str, QuoteReason)):
                raw_values.append(raw)
            else:
                try:
                    raw_values.extend(tuple(raw or ()))
                except TypeError:
                    raw_values.append(raw)
    elif isinstance(value, (str, QuoteReason)):
        raw_values.append(value)
    else:
        try:
            raw_values.extend(tuple(value))
        except TypeError:
            raw_values.append(value)
    result: list[QuoteReason] = []
    allowed = {reason.value for reason in QuoteReason}
    for raw in raw_values:
        normalized = (
            raw.value if isinstance(raw, QuoteReason) else str(raw).strip().upper().replace("-", "_").replace(" ", "_")
        )
        mapped = _QUALITY_REASON_ALIASES.get(
            normalized, QuoteReason(normalized) if normalized in allowed else QuoteReason.INVALID_QUALITY
        )
        if mapped is not None and mapped not in result:
            result.append(mapped)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class QuoteLegAssessment:
    """Assessment for exactly one bid or ask leg."""

    side: QuoteSide
    quality: QuoteQuality
    usable: bool
    source_timestamp: datetime | None
    age_seconds: Decimal | None
    reasons: tuple[QuoteReason, ...] = ()

    @property
    def status(self) -> QuoteQuality:
        """Stable alias used by adapters and reports."""

        return (
            self.quality
            if self.usable
            else (
                self.quality
                if self.quality
                in {
                    QuoteQuality.STALE,
                    QuoteQuality.SNAPSHOT,
                    QuoteQuality.DISCONNECTED,
                    QuoteQuality.OUT_OF_ORDER,
                    QuoteQuality.SESSION_MISMATCH,
                }
                else QuoteQuality.UNKNOWN
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "side": self.side.value,
            "quality": self.quality.value,
            "status": self.status.value,
            "usable": self.usable,
            "source_timestamp": _iso(self.source_timestamp),
            "age_seconds": str(self.age_seconds) if self.age_seconds is not None else None,
            "reasons": [reason.value for reason in self.reasons],
        }


@dataclass(frozen=True, slots=True)
class QuoteAssessment:
    """Assessment for a side or both sides at a specific logical time."""

    side: QuoteSide
    usable: bool
    reasons: tuple[QuoteReason, ...] = ()
    bid: QuoteLegAssessment | None = None
    ask: QuoteLegAssessment | None = None
    checked_at: datetime | None = None
    common_usable: bool = True

    @property
    def status(self) -> QuoteQuality:
        if self.usable:
            return QuoteQuality.VALID
        if QuoteReason.SNAPSHOT in self.reasons:
            return QuoteQuality.SNAPSHOT
        if QuoteReason.DISCONNECTED in self.reasons:
            return QuoteQuality.DISCONNECTED
        if QuoteReason.OUT_OF_ORDER in self.reasons:
            return QuoteQuality.OUT_OF_ORDER
        if QuoteReason.SESSION_MISMATCH in self.reasons:
            return QuoteQuality.SESSION_MISMATCH
        if QuoteReason.STALE in self.reasons:
            return QuoteQuality.STALE
        return QuoteQuality.UNKNOWN

    def to_dict(self) -> dict[str, Any]:
        return {
            "side": self.side.value,
            "usable": self.usable,
            "status": self.status.value,
            "reasons": [reason.value for reason in self.reasons],
            "bid": self.bid.to_dict() if self.bid else None,
            "ask": self.ask.to_dict() if self.ask else None,
            "checked_at": _iso(self.checked_at),
            "common_usable": self.common_usable,
        }


@dataclass(frozen=True, slots=True)
class QuoteLeg:
    """A price and its independent source/availability evidence."""

    side: QuoteSide
    price: Decimal | int | str | None
    source_timestamp: datetime | None = None
    received_at: datetime | None = None
    available_at: datetime | None = None
    quality: QuoteQuality = QuoteQuality.VALID
    timestamp_known: bool = True
    source: str = "fixture"
    sequence: int | str | None = None
    session_generation: int | str | None = None
    present: bool | None = None
    quality_reasons: tuple[QuoteReason | str, ...] = ()

    def __post_init__(self) -> None:
        side = _leg_side(self.side)
        price = _leg_price(self.price, side)
        source_timestamp = _optional_utc(self.source_timestamp, f"{side.value}.source_timestamp")
        received_at = _optional_utc(self.received_at, f"{side.value}.received_at")
        available_at = _optional_utc(self.available_at, f"{side.value}.available_at")
        _validate_leg_times(side, source_timestamp, received_at, available_at, self.timestamp_known)
        present = _present_value(self.present, price, side)
        quality_reasons = tuple(
            dict.fromkeys((*quality_reasons_from(self.quality), *quality_reasons_from(self.quality_reasons)))
        )
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "source_timestamp", source_timestamp)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "quality", quality_from(self.quality))
        object.__setattr__(self, "source", str(self.source or "fixture"))
        object.__setattr__(self, "present", present)
        object.__setattr__(self, "quality_reasons", quality_reasons)

    def assess(
        self,
        *,
        at: datetime,
        max_age_seconds: Decimal | int | str | None = None,
        expected_session: int | str | None = None,
        connected: bool = True,
        snapshot: bool = False,
        out_of_order: bool = False,
    ) -> QuoteLegAssessment:
        """Assess this leg without changing it or consulting ambient state."""

        checked = _utc(at, name="at")
        quality, reasons = _base_leg_assessment(
            self,
            expected_session=expected_session,
            connected=connected,
            snapshot=snapshot,
            out_of_order=out_of_order,
        )
        reasons.extend(quality_reasons_from(self.quality_reasons))
        age, time_reasons, time_quality = _leg_time_assessment(self, checked, max_age_seconds)
        reasons.extend(time_reasons)
        quality = time_quality or quality
        unique = tuple(dict.fromkeys(reasons))
        side = self.side if isinstance(self.side, QuoteSide) else QuoteSide(str(self.side).lower())
        return QuoteLegAssessment(side, quality, not unique, self.source_timestamp, age, unique)


def _leg_side(value: QuoteSide | str) -> QuoteSide:
    side = value if isinstance(value, QuoteSide) else QuoteSide(str(value).lower())
    if side is QuoteSide.MID:
        raise ValueError("QuoteLeg sólo admite bid o ask")
    return side


def _leg_price(value: Any, side: QuoteSide) -> Decimal | None:
    if value is None:
        return None
    price = _decimal(value, name=f"{side.value}.price")
    if price <= 0:
        raise ValueError(f"{side.value}.price debe ser positivo")
    return price


def _optional_utc(value: Any, name: str) -> datetime | None:
    return _utc(value, name=name) if value is not None else None


def _validate_leg_times(
    side: QuoteSide,
    source_timestamp: datetime | None,
    received_at: datetime | None,
    available_at: datetime | None,
    timestamp_known: bool,
) -> None:
    if not isinstance(timestamp_known, bool):
        raise ValueError(f"{side.value}.timestamp_known debe ser booleano")
    if timestamp_known and source_timestamp is None:
        raise ValueError(f"{side.value}.source_timestamp requerido cuando timestamp_known=True")
    if source_timestamp is not None and received_at is not None and received_at < source_timestamp:
        raise ValueError(f"{side.value}.received_at no puede preceder source_timestamp")
    if received_at is not None and available_at is not None and available_at < received_at:
        raise ValueError(f"{side.value}.available_at no puede preceder received_at")
    if source_timestamp is not None and available_at is not None and available_at < source_timestamp:
        raise ValueError(f"{side.value}.available_at no puede preceder source_timestamp")


def _present_value(value: bool | None, price: Decimal | None, side: QuoteSide) -> bool:
    if value is None:
        return price is not None
    if not isinstance(value, bool):
        raise ValueError(f"{side.value}.present debe ser booleano")
    return value


def _base_leg_assessment(
    leg: QuoteLeg,
    *,
    expected_session: int | str | None,
    connected: bool,
    snapshot: bool,
    out_of_order: bool,
) -> tuple[QuoteQuality, list[QuoteReason]]:
    quality, reasons = _leg_presence(leg)
    quality, operational = _leg_operational_state(
        leg,
        quality,
        expected_session=expected_session,
        connected=connected,
        snapshot=snapshot,
        out_of_order=out_of_order,
    )
    reasons.extend(operational)
    if quality not in _USABLE_QUALITY:
        reasons.append(QuoteReason.INVALID_QUALITY)
    return quality, reasons


def _leg_presence(leg: QuoteLeg) -> tuple[QuoteQuality, list[QuoteReason]]:
    if leg.present and leg.price is not None:
        quality = leg.quality if isinstance(leg.quality, QuoteQuality) else quality_from(leg.quality)
        return quality, []
    reason = QuoteReason.MISSING_BID if leg.side is QuoteSide.BID else QuoteReason.MISSING_ASK
    quality = leg.quality if isinstance(leg.quality, QuoteQuality) else quality_from(leg.quality)
    return quality, [reason]


def _leg_operational_state(
    leg: QuoteLeg,
    quality: QuoteQuality,
    *,
    expected_session: int | str | None,
    connected: bool,
    snapshot: bool,
    out_of_order: bool,
) -> tuple[QuoteQuality, list[QuoteReason]]:
    reasons: list[QuoteReason] = []
    if not connected:
        reasons.append(QuoteReason.DISCONNECTED)
        quality = QuoteQuality.DISCONNECTED
    if snapshot:
        reasons.append(QuoteReason.SNAPSHOT)
        quality = QuoteQuality.SNAPSHOT
    if out_of_order:
        reasons.append(QuoteReason.OUT_OF_ORDER)
        quality = QuoteQuality.OUT_OF_ORDER
    if _session_mismatch(leg, expected_session):
        reasons.append(QuoteReason.SESSION_MISMATCH)
        quality = QuoteQuality.SESSION_MISMATCH
    return quality, reasons


def _session_mismatch(leg: QuoteLeg, expected_session: int | str | None) -> bool:
    return (
        expected_session is not None
        and leg.session_generation is not None
        and leg.session_generation != expected_session
    )


def _leg_time_assessment(
    leg: QuoteLeg,
    checked: datetime,
    max_age_seconds: Decimal | int | str | None,
) -> tuple[Decimal | None, list[QuoteReason], QuoteQuality | None]:
    if not leg.timestamp_known or leg.source_timestamp is None:
        return None, [QuoteReason.MISSING_SOURCE_TIMESTAMP], None
    if leg.source_timestamp > checked:
        return None, [QuoteReason.FUTURE_SOURCE_TIMESTAMP], None
    if leg.received_at is not None and leg.received_at > checked:
        return None, [QuoteReason.FUTURE_AVAILABILITY], None
    if leg.available_at is not None and leg.available_at > checked:
        return None, [QuoteReason.FUTURE_AVAILABILITY], None
    age = _seconds(checked - leg.source_timestamp)
    if max_age_seconds is not None and age > _decimal(max_age_seconds, name="max_age_seconds"):
        return age, [QuoteReason.STALE], QuoteQuality.STALE
    return age, [], None


def _seconds(delta: Any) -> Decimal:
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return Decimal(micros) / Decimal(1_000_000)


def assess_pair(
    bid: QuoteLeg,
    ask: QuoteLeg,
    *,
    at: datetime,
    max_age_seconds: Decimal | int | str | None = None,
    common_quality: QuoteQuality | str = QuoteQuality.VALID,
    expected_session: int | str | None = None,
    connected: bool = True,
    snapshot: bool = False,
    out_of_order: bool = False,
    crossed: bool = False,
) -> QuoteAssessment:
    """Assess both legs; ``mid`` requires both sides to be usable."""

    common = quality_from(common_quality)
    bid_assessment = bid.assess(
        at=at,
        max_age_seconds=max_age_seconds,
        expected_session=expected_session,
        connected=connected,
        snapshot=snapshot,
        out_of_order=out_of_order,
    )
    ask_assessment = ask.assess(
        at=at,
        max_age_seconds=max_age_seconds,
        expected_session=expected_session,
        connected=connected,
        snapshot=snapshot,
        out_of_order=out_of_order,
    )
    reasons = list((*bid_assessment.reasons, *ask_assessment.reasons))
    if common not in _USABLE_QUALITY:
        reasons.append(QuoteReason.INVALID_QUALITY)
    if crossed:
        reasons.append(QuoteReason.CROSSED)
    unique = tuple(dict.fromkeys(reasons))
    return QuoteAssessment(
        QuoteSide.MID,
        not unique,
        unique,
        bid_assessment,
        ask_assessment,
        _utc(at, name="at"),
        common_usable=common in _USABLE_QUALITY,
    )


__all__ = [
    "QuoteAssessment",
    "QuoteLeg",
    "QuoteLegAssessment",
    "QuoteQuality",
    "QuoteReason",
    "QuoteSide",
    "assess_pair",
    "quality_from",
    "quality_reasons_from",
]
