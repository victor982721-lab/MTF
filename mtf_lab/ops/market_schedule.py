"""Observed cTrader market schedule helpers.

This module consumes only a previously selected catalog entry.  It never
fetches a catalog, opens a connection, changes configuration, or assumes a
broker default.  cTrader ``ProtoOASymbol.schedule`` intervals are measured
from Sunday 00:00 in the symbol's declared IANA ``scheduleTimeZone``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..data.ctrader_config import normalize_symbol_name
from ..data.ctrader_protocol import SCHEMA_REVISION, message_to_mapping

UNKNOWN = "UNKNOWN"
OPEN = "OPEN"
CLOSED_SCHEDULED = "CLOSED_SCHEDULED"

_ALLOWED_PROVIDER_NAMES = frozenset({"ctrader_open_api", "ctrader-open-api", "ctrader"})
_DAY_SECONDS = 86_400
_WEEK_SECONDS = 7 * _DAY_SECONDS
_MICROSECONDS = 1_000_000
_DAY_MICROSECONDS = _DAY_SECONDS * _MICROSECONDS
_WEEK_MICROSECONDS = _WEEK_SECONDS * _MICROSECONDS
_EPOCH_DATE = date(1970, 1, 1)


@dataclass(frozen=True, slots=True)
class _CatalogView:
    provider_name: str
    catalog: Any
    selected: Any
    metadata: Mapping[str, Any]
    full_symbol: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class _HolidayWindow:
    timezone: ZoneInfo
    holiday_date: date
    recurring: bool
    start_microseconds: int
    end_microseconds: int


def observed_market_state(provider: Any, aware_now: datetime | None) -> str:
    """Return ``OPEN``, ``CLOSED_SCHEDULED`` or ``UNKNOWN`` from observed metadata.

    ``aware_now`` must be timezone-aware.  A missing or malformed provider,
    selected catalog entry, full symbol, IANA timezone, interval, or holiday
    makes the result ``UNKNOWN`` rather than inferring a market state.
    """

    view = _catalog_view(provider)
    instant = _aware_datetime(aware_now)
    if view is None or instant is None:
        return UNKNOWN
    timezone = _schedule_timezone(view.full_symbol)
    schedule = _schedule_intervals(view.full_symbol)
    if timezone is None or schedule is None:
        return UNKNOWN
    try:
        local = instant.astimezone(timezone[1])
    except (OverflowError, ValueError):
        return UNKNOWN
    holidays = _holiday_windows(view.full_symbol)
    if holidays is None:
        return UNKNOWN
    local_microseconds = _week_microseconds(local)
    for holiday in holidays:
        try:
            holiday_local = instant.astimezone(holiday.timezone)
        except (OverflowError, ValueError):
            return UNKNOWN
        if (
            _holiday_matches(holiday, holiday_local)
            and holiday.start_microseconds <= _day_microseconds(holiday_local) < holiday.end_microseconds
        ):
            return CLOSED_SCHEDULED
    if any(start <= local_microseconds < end for start, end in schedule):
        return OPEN
    return CLOSED_SCHEDULED


def catalog_provenance(provider: Any, observed_at: datetime | None) -> dict[str, Any]:
    """Return a compact, secret-free snapshot of the selected full symbol.

    Values absent from the observed payload are represented by ``None``.  The
    function does not fill broker defaults such as price scales, volume
    limits, costs, timezone, or schedule windows.
    """

    provider_name = _provider_name(provider)
    instant = _aware_datetime(observed_at)
    view = _catalog_view(provider)
    selected = getattr(view.selected, "name", None) if view is not None else None
    selected_id = _int_value(getattr(view.selected, "symbol_id", None)) if view is not None else None
    full = view.full_symbol if view is not None else None
    timezone_raw = _value(full, "scheduleTimeZone", "schedule_time_zone") if full is not None else None
    timezone_name = timezone_raw.strip() if isinstance(timezone_raw, str) and timezone_raw.strip() else None
    resolved_timezone = _schedule_timezone(full) if full is not None else None
    timezone = resolved_timezone[0] if resolved_timezone is not None else None
    compact_schedule = _compact_schedule(full)
    compact_holidays = _compact_holidays(full)
    price_scale = _int_value(_value(full, "priceScale", "price_scale")) if full is not None else None
    result: dict[str, Any] = {
        "schema_revision": SCHEMA_REVISION,
        "provider": provider_name,
        "observed_at": _iso_utc(instant),
        "market_state": observed_market_state(provider, observed_at),
        "catalog_identity_valid": view is not None,
        "selected": {
            "symbol": str(selected).strip() if isinstance(selected, str) and selected.strip() else None,
            "symbol_id": selected_id,
        },
        "full_symbol": {
            "symbol_id": _int_value(_value(full, "symbolId", "symbol_id")) if full is not None else None,
            "digits": _int_value(_value(full, "digits")) if full is not None else None,
            "pip_position": _int_value(_value(full, "pipPosition", "pip_position")) if full is not None else None,
            "price_scale": price_scale,
            "min_volume": _int_value(_value(full, "minVolume", "min_volume")) if full is not None else None,
            "step_volume": _int_value(_value(full, "stepVolume", "step_volume")) if full is not None else None,
            "max_volume": _int_value(_value(full, "maxVolume", "max_volume")) if full is not None else None,
            "lot_size": _int_value(_value(full, "lotSize", "lot_size")) if full is not None else None,
            "schedule_timezone": timezone_name,
            "schedule_timezone_resolved": timezone,
            "schedule": compact_schedule,
            "holidays": compact_holidays,
            "costs": _compact_costs(full),
        },
    }
    return result


def _catalog_view(provider: Any) -> _CatalogView | None:
    name = _provider_name(provider)
    if name is None:
        return None
    catalog = getattr(provider, "catalog", None)
    selected = getattr(catalog, "selected", None)
    symbols = getattr(catalog, "symbols", None)
    if selected is None or not isinstance(symbols, Sequence) or isinstance(symbols, (str, bytes)):
        return None
    if not any(item is selected for item in symbols):
        return None
    selected_name = getattr(selected, "name", None)
    requested_name = getattr(catalog, "requested_symbol", None)
    if not isinstance(selected_name, str) or not selected_name.strip() or not isinstance(requested_name, str):
        return None
    try:
        if normalize_symbol_name(selected_name) != normalize_symbol_name(requested_name):
            return None
    except Exception:
        return None
    metadata = getattr(selected, "metadata", None)
    if not isinstance(metadata, Mapping):
        return None
    # CTraderProvider.resolve_symbol merges the observed ProtoOASymbol into
    # metadata directly. Also accept the explicit wrapper used by importers.
    full_symbol = _as_mapping(metadata.get("fullSymbol", metadata))
    if full_symbol is None:
        return None
    selected_id = _int_value(getattr(selected, "symbol_id", None))
    full_id = _int_value(_value(full_symbol, "symbolId", "symbol_id"))
    if selected_id is None or selected_id <= 0 or full_id != selected_id:
        return None
    return _CatalogView(name, catalog, selected, metadata, full_symbol)


def _provider_name(provider: Any) -> str | None:
    value = getattr(provider, "name", None)
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized if normalized in _ALLOWED_PROVIDER_NAMES else None


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if value is None:
        return None
    try:
        mapped = message_to_mapping(value)
    except Exception:
        return None
    return mapped if isinstance(mapped, Mapping) else None


def _value(value: Mapping[str, Any] | None, *names: str) -> Any:
    if value is None:
        return None
    for name in names:
        if name in value:
            return value[name]
    return None


def _as_sequence(value: Any) -> tuple[Any, ...] | None:
    if value is None:
        return None
    if isinstance(value, (Mapping, str, bytes)):
        return (value,)
    try:
        return tuple(value)
    except TypeError:
        return None


def _int_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text and (text.isdigit() or (text[0] in "+-" and text[1:].isdigit())):
            try:
                return int(text)
            except ValueError:
                return None
    return None


def _aware_datetime(value: datetime | None) -> datetime | None:
    if not isinstance(value, datetime) or value.tzinfo is None:
        return None
    try:
        if value.utcoffset() is None:
            return None
    except (OverflowError, ValueError):
        return None
    return value


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    try:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    except (OverflowError, ValueError):
        return None


def _schedule_timezone(full_symbol: Mapping[str, Any] | None) -> tuple[str, ZoneInfo] | None:
    raw = _value(full_symbol, "scheduleTimeZone", "schedule_time_zone")
    if not isinstance(raw, str) or not raw.strip():
        return None
    name = raw.strip()
    try:
        return name, ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return None


def _schedule_intervals(full_symbol: Mapping[str, Any]) -> tuple[tuple[int, int], ...] | None:
    raw = _value(full_symbol, "schedule", "schedules")
    items = _as_sequence(raw)
    if items is None or not items:
        return None
    intervals: list[tuple[int, int]] = []
    for item in items:
        interval = _as_mapping(item)
        if interval is None:
            return None
        start = _int_value(_value(interval, "startSecond", "start_second"))
        end = _int_value(_value(interval, "endSecond", "end_second"))
        if start is None or end is None or start < 0 or end <= start or end > _WEEK_SECONDS:
            return None
        intervals.append((start * _MICROSECONDS, end * _MICROSECONDS))
    return tuple(intervals)


def _holiday_windows(full_symbol: Mapping[str, Any]) -> tuple[_HolidayWindow, ...] | None:
    raw = _value(full_symbol, "holiday", "holidays")
    items = _as_sequence(raw)
    if items is None:
        return ()
    result: list[_HolidayWindow] = []
    for item in items:
        holiday = _as_mapping(item)
        if holiday is None:
            return None
        # ProtoOAHoliday declares its own zone, independently of ProtoOASymbol.
        holiday_timezone = _schedule_timezone(holiday)
        days = _int_value(_value(holiday, "holidayDate", "holiday_date"))
        recurring = _bool_value(_value(holiday, "isRecurring", "is_recurring"))
        if holiday_timezone is None:
            return None
        if days is None or days < 0 or recurring is None:
            return None
        try:
            holiday_date = _EPOCH_DATE + timedelta(days=days)
        except (OverflowError, ValueError):
            return None
        start_present = _value_present(holiday, "startSecond", "start_second")
        end_present = _value_present(holiday, "endSecond", "end_second")
        if start_present != end_present:
            return None
        if not start_present:
            start_microseconds, end_microseconds = 0, _DAY_MICROSECONDS
        else:
            start = _int_value(_value(holiday, "startSecond", "start_second"))
            end = _int_value(_value(holiday, "endSecond", "end_second"))
            if start is None or end is None or start < 0 or end <= start or end > _DAY_SECONDS:
                return None
            start_microseconds, end_microseconds = start * _MICROSECONDS, end * _MICROSECONDS
        result.append(
            _HolidayWindow(
                holiday_timezone[1],
                holiday_date,
                recurring,
                start_microseconds,
                end_microseconds,
            )
        )
    return tuple(result)


def _value_present(value: Mapping[str, Any], *names: str) -> bool:
    return any(name in value and value[name] is not None for name in names)


def _bool_value(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _week_microseconds(local: datetime) -> int:
    sunday_index = (local.weekday() + 1) % 7
    return (
        sunday_index * _DAY_MICROSECONDS
        + local.hour * 3_600 * _MICROSECONDS
        + local.minute * 60 * _MICROSECONDS
        + local.second * _MICROSECONDS
        + local.microsecond
    )


def _day_microseconds(local: datetime) -> int:
    return (
        local.hour * 3_600 * _MICROSECONDS
        + local.minute * 60 * _MICROSECONDS
        + local.second * _MICROSECONDS
        + local.microsecond
    )


def _holiday_matches(holiday: _HolidayWindow, local: datetime) -> bool:
    if holiday.recurring:
        return (holiday.holiday_date.month, holiday.holiday_date.day) == (local.month, local.day)
    return holiday.holiday_date == local.date()


def _compact_schedule(full_symbol: Mapping[str, Any] | None) -> list[dict[str, int | None]] | None:
    raw = _value(full_symbol, "schedule", "schedules")
    items = _as_sequence(raw)
    if items is None:
        return None
    result: list[dict[str, int | None]] = []
    for item in items:
        interval = _as_mapping(item)
        if interval is None:
            return None
        result.append(
            {
                "start_second": _int_value(_value(interval, "startSecond", "start_second")),
                "end_second": _int_value(_value(interval, "endSecond", "end_second")),
            }
        )
    return result


def _compact_holidays(full_symbol: Mapping[str, Any] | None) -> list[dict[str, Any]] | None:
    raw = _value(full_symbol, "holiday", "holidays")
    items = _as_sequence(raw)
    if items is None:
        return None
    result: list[dict[str, Any]] = []
    for item in items:
        holiday = _as_mapping(item)
        if holiday is None:
            return None
        result.append(
            {
                "holiday_id": _int_value(_value(holiday, "holidayId", "holiday_id")),
                "schedule_timezone": _text_or_none(_value(holiday, "scheduleTimeZone", "schedule_time_zone")),
                "holiday_date": _int_value(_value(holiday, "holidayDate", "holiday_date")),
                "is_recurring": _bool_value(_value(holiday, "isRecurring", "is_recurring")),
                "start_second": _int_value(_value(holiday, "startSecond", "start_second")),
                "end_second": _int_value(_value(holiday, "endSecond", "end_second")),
            }
        )
    return result


def _compact_costs(full_symbol: Mapping[str, Any] | None) -> dict[str, Any]:
    names = {
        "commission": ("commission",),
        "commission_type": ("commissionType", "commission_type"),
        "precise_trading_commission_rate": ("preciseTradingCommissionRate", "precise_trading_commission_rate"),
        "min_commission": ("minCommission", "min_commission"),
        "min_commission_type": ("minCommissionType", "min_commission_type"),
        "precise_min_commission": ("preciseMinCommission", "precise_min_commission"),
        "gsl_charge": ("gslCharge", "gsl_charge"),
        "rollover_commission": ("rolloverCommission", "rollover_commission"),
        "pnl_conversion_fee_rate": ("pnlConversionFeeRate", "pnl_conversion_fee_rate"),
    }
    result: dict[str, Any] = {}
    for output_name, source_names in names.items():
        value = _value(full_symbol, *source_names)
        result[output_name] = (
            _int_value(value)
            if output_name != "commission_type" and output_name != "min_commission_type"
            else _enum_value(value)
        )
    return result


def _enum_value(value: Any) -> int | str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, str)):
        return value
    return None


def _text_or_none(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


__all__ = ["CLOSED_SCHEDULED", "OPEN", "UNKNOWN", "catalog_provenance", "observed_market_state"]
