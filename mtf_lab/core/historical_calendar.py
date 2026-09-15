"""Conservative model for weekly historical FX quote availability.

This is not an account, venue, holiday, or broker calendar.  It only models the
standard Friday-to-Sunday weekly closure in New York time and is opt-in for
historical quote analysis.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

CALENDAR_SCHEMA_VERSION = 1
CALENDAR_BASIS = "MODELED_WEEKLY_FX_NOT_ACCOUNT_VERIFIED"
CALENDAR_TIMEZONE = "America/New_York"
CALENDAR_PROVENANCE_URI = "https://www.oanda.com/assets/documents/402/Hours_of_Operation_bT4prdz.pdf"
CALENDAR_DOCUMENTED_AT = datetime(2026, 9, 13, tzinfo=UTC)
MAX_INTERVAL = timedelta(days=32)


class HistoricalCalendarError(ValueError):
    """The requested calendar interval or calendar identity is invalid."""


@dataclass(frozen=True, slots=True)
class HistoricalQuoteCalendar:
    """Weekly closure model for historical quotes, never for account execution."""

    timezone: str = CALENDAR_TIMEZONE
    friday_close: str = "17:00"
    sunday_open: str = "17:00"
    provenance_uri: str = CALENDAR_PROVENANCE_URI
    documented_at: datetime = CALENDAR_DOCUMENTED_AT
    basis: str = CALENDAR_BASIS
    schema_version: int = CALENDAR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != CALENDAR_SCHEMA_VERSION
        ):
            raise HistoricalCalendarError("unsupported historical calendar schema")
        if self.timezone != CALENDAR_TIMEZONE:
            raise HistoricalCalendarError("historical calendar timezone is fixed to America/New_York")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise HistoricalCalendarError(f"unknown calendar timezone: {self.timezone}") from exc
        if self.friday_close != "17:00" or self.sunday_open != "17:00":
            raise HistoricalCalendarError("historical weekly closure must be Friday 17:00 to Sunday 17:00")
        if not self.provenance_uri.strip():
            raise HistoricalCalendarError("provenance_uri must be non-empty")
        documented_at = _utc(self.documented_at, "documented_at")
        object.__setattr__(self, "documented_at", documented_at)
        if self.basis != CALENDAR_BASIS:
            raise HistoricalCalendarError("historical calendar basis is not account-verified")

    @property
    def calendar_hash(self) -> str:
        material = self.to_dict(include_hash=False)
        encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def to_dict(self, *, include_hash: bool = True) -> dict[str, object]:
        result: dict[str, object] = {
            "schema_version": self.schema_version,
            "timezone": self.timezone,
            "friday_close": self.friday_close,
            "sunday_open": self.sunday_open,
            "provenance_uri": self.provenance_uri,
            "documented_at": _iso(self.documented_at),
            "basis": self.basis,
        }
        if include_hash:
            result["calendar_hash"] = self.calendar_hash
        return result

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> HistoricalQuoteCalendar:
        if not isinstance(value, Mapping):
            raise HistoricalCalendarError("historical calendar must be a mapping")
        allowed = {
            "schema_version",
            "timezone",
            "friday_close",
            "sunday_open",
            "provenance_uri",
            "documented_at",
            "basis",
            "calendar_hash",
        }
        if set(value) - allowed:
            raise HistoricalCalendarError("unknown historical calendar fields")
        schema_version = value.get("schema_version")
        if isinstance(schema_version, bool) or not isinstance(schema_version, int):
            raise HistoricalCalendarError("schema_version must be an integer")
        documented_at = value.get("documented_at")
        if not isinstance(documented_at, str):
            raise HistoricalCalendarError("documented_at must be an ISO timestamp")
        result = cls(
            schema_version=schema_version,
            timezone=_text(value, "timezone"),
            friday_close=_text(value, "friday_close"),
            sunday_open=_text(value, "sunday_open"),
            provenance_uri=_text(value, "provenance_uri"),
            documented_at=_parse_iso(documented_at),
            basis=_text(value, "basis"),
        )
        claimed = value.get("calendar_hash")
        if claimed is not None and claimed != result.calendar_hash:
            raise HistoricalCalendarError("calendar_hash mismatch")
        return result

    def open_seconds_between(self, start: datetime, end: datetime) -> float:
        """Return UTC elapsed seconds outside the modeled weekly closure."""

        start_utc, end_utc = _interval(start, end)
        if start_utc == end_utc:
            return 0.0
        elapsed = (end_utc - start_utc).total_seconds()
        zone = ZoneInfo(self.timezone)
        local_first, local_last = start_utc.astimezone(zone), end_utc.astimezone(zone)
        if local_first.date() == local_last.date():
            return _same_day_open_seconds(start_utc, end_utc, local_first)
        closed = 0.0
        local_start = local_first.date() - timedelta(days=2)
        local_end = local_last.date() + timedelta(days=2)
        current = local_start
        while current <= local_end:
            if current.weekday() == 4:
                closed_start, closed_end = self._weekly_closure(current)
                closed += _intersection_seconds(start_utc, end_utc, closed_start, closed_end)
            current += timedelta(days=1)
        return max(0.0, elapsed - closed)

    def covers_closed(self, start: datetime, end: datetime) -> bool:
        """Return true only when every elapsed second is in the weekly closure."""

        start_utc, end_utc = _interval(start, end)
        return self.open_seconds_between(start_utc, end_utc) == 0.0

    def _weekly_closure(self, friday: date) -> tuple[datetime, datetime]:
        zone = ZoneInfo(self.timezone)
        close_local = datetime.combine(friday, time(17, 0), tzinfo=zone)
        open_local = datetime.combine(friday + timedelta(days=2), time(17, 0), tzinfo=zone)
        return close_local.astimezone(UTC), open_local.astimezone(UTC)


def _interval(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    start_utc = _utc(start, "start")
    end_utc = _utc(end, "end")
    if end_utc < start_utc:
        raise HistoricalCalendarError("end must not precede start")
    if end_utc - start_utc > MAX_INTERVAL:
        raise HistoricalCalendarError("historical calendar interval exceeds 32 days")
    return start_utc, end_utc


def _intersection_seconds(start: datetime, end: datetime, closed_start: datetime, closed_end: datetime) -> float:
    left = max(start, closed_start)
    right = min(end, closed_end)
    return max(0.0, (right - left).total_seconds())


def _same_day_open_seconds(start: datetime, end: datetime, local: datetime) -> float:
    weekday = local.weekday()
    if weekday < 4:
        return (end - start).total_seconds()
    if weekday == 5:
        return 0.0
    boundary = datetime.combine(local.date(), time(17, 0), tzinfo=local.tzinfo).astimezone(UTC)
    if weekday == 4:
        return max(0.0, (min(end, boundary) - min(start, boundary)).total_seconds())
    return max(0.0, (max(end, boundary) - max(start, boundary)).total_seconds())


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise HistoricalCalendarError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HistoricalCalendarError("invalid documented_at timestamp") from exc
    return _utc(parsed, "documented_at")


def _text(value: Mapping[str, object], key: str) -> str:
    raw = value.get(key)
    if not isinstance(raw, str) or not raw.strip():
        raise HistoricalCalendarError(f"{key} must be non-empty text")
    return raw.strip()


__all__ = [
    "CALENDAR_BASIS",
    "CALENDAR_DOCUMENTED_AT",
    "CALENDAR_PROVENANCE_URI",
    "CALENDAR_SCHEMA_VERSION",
    "CALENDAR_TIMEZONE",
    "HistoricalCalendarError",
    "HistoricalQuoteCalendar",
    "MAX_INTERVAL",
]
