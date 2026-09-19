"""Offline contracts for observed cTrader catalog schedules."""

from __future__ import annotations

import unittest
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

from mtf_lab.data.ctrader_market import CTraderSymbol, SymbolCatalog
from mtf_lab.ops.market_schedule import (
    CLOSED_SCHEDULED,
    OPEN,
    UNKNOWN,
    catalog_provenance,
    observed_market_state,
)

EPOCH = date(1970, 1, 1)


def _holiday(
    when: date,
    *,
    timezone: str = "UTC",
    recurring: bool = False,
    start: int | None = None,
    end: int | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "holidayId": 1,
        "name": "fixture",
        "scheduleTimeZone": timezone,
        "holidayDate": (when - EPOCH).days,
        "isRecurring": recurring,
    }
    if start is not None:
        value["startSecond"] = start
    if end is not None:
        value["endSecond"] = end
    return value


def _full_symbol(
    *,
    timezone: str = "UTC",
    schedule: Any = ({"startSecond": 0, "endSecond": 3_600},),
    holidays: Any = (),
    **extra: Any,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "symbolId": 11,
        "digits": 5,
        "pipPosition": 4,
        "scheduleTimeZone": timezone,
        "schedule": schedule,
        "holiday": holidays,
    }
    value.update(extra)
    return value


def _provider(
    full_symbol: Any,
    *,
    provider_name: str = "ctrader_open_api",
    requested_symbol: str = "EUR/USD",
    selected_in_symbols: bool = True,
) -> Any:
    selected = CTraderSymbol(
        symbol_id=11,
        name="EUR/USD",
        digits=5,
        pip_position=4,
        enabled=True,
        metadata={"fullSymbol": full_symbol, "clientSecret": "must-not-leak"},
    )
    symbols = (selected,) if selected_in_symbols else (CTraderSymbol(12, "GBP/USD"),)
    catalog = SymbolCatalog(symbols, requested_symbol, selected)
    return SimpleNamespace(name=provider_name, catalog=catalog)


class MarketScheduleTests(unittest.TestCase):
    def test_actual_provider_flat_full_symbol_shape_is_supported(self) -> None:
        selected = CTraderSymbol(11, "EUR/USD", 5, 4, True, _full_symbol())
        provider = SimpleNamespace(name="ctrader_open_api", catalog=SymbolCatalog((selected,), "EUR/USD", selected))
        self.assertEqual(observed_market_state(provider, datetime(2026, 1, 4, 0, 30, tzinfo=UTC)), OPEN)
        self.assertTrue(catalog_provenance(provider, datetime(2026, 1, 4, tzinfo=UTC))["catalog_identity_valid"])

    def test_missing_or_naive_inputs_are_unknown(self) -> None:
        now = datetime(2026, 1, 4, 0, 30, tzinfo=UTC)
        provider = _provider(_full_symbol())
        self.assertEqual(observed_market_state(None, now), UNKNOWN)
        self.assertEqual(observed_market_state(provider, None), UNKNOWN)
        self.assertEqual(observed_market_state(provider, now.replace(tzinfo=None)), UNKNOWN)
        self.assertEqual(observed_market_state(_provider({"schedule": []}), now), UNKNOWN)
        self.assertEqual(observed_market_state(_provider(_full_symbol(timezone="Not/IANA")), now), UNKNOWN)

    def test_plain_schedule_metadata_without_full_symbol_is_not_inferred(self) -> None:
        selected = CTraderSymbol(
            symbol_id=11,
            name="EUR/USD",
            digits=5,
            pip_position=4,
            metadata={"scheduleTimeZone": "UTC", "schedule": [{"startSecond": 0, "endSecond": 3_600}]},
        )
        provider = SimpleNamespace(
            name="ctrader_open_api",
            catalog=SymbolCatalog((selected,), "EUR/USD", selected),
        )
        self.assertEqual(observed_market_state(provider, datetime(2026, 1, 4, 0, 30, tzinfo=UTC)), UNKNOWN)

    def test_catalog_identity_must_be_selected_and_provider_known(self) -> None:
        now = datetime(2026, 1, 4, 0, 30, tzinfo=UTC)
        self.assertEqual(observed_market_state(_provider(_full_symbol(), provider_name="other"), now), UNKNOWN)
        self.assertEqual(observed_market_state(_provider(_full_symbol(), requested_symbol="GBP/USD"), now), UNKNOWN)
        self.assertEqual(observed_market_state(_provider(_full_symbol(), selected_in_symbols=False), now), UNKNOWN)

    def test_week_schedule_uses_sunday_zero_not_python_monday_zero(self) -> None:
        provider = _provider(_full_symbol(schedule=({"startSecond": 0, "endSecond": 3_600},)))
        self.assertEqual(observed_market_state(provider, datetime(2026, 1, 4, 0, 30, tzinfo=UTC)), OPEN)
        self.assertEqual(observed_market_state(provider, datetime(2026, 1, 5, 0, 30, tzinfo=UTC)), CLOSED_SCHEDULED)

    def test_iana_timezone_and_dst_are_applied_before_schedule_lookup(self) -> None:
        provider = _provider(
            _full_symbol(
                timezone="America/New_York",
                schedule=({"startSecond": 3_600, "endSecond": 10_800},),
            )
        )
        # 01:30 EST on the spring-forward Sunday is inside the declared window.
        self.assertEqual(observed_market_state(provider, datetime(2026, 3, 8, 6, 30, tzinfo=UTC)), OPEN)
        # 03:30 EDT is outside the same local-wall-clock window.
        self.assertEqual(observed_market_state(provider, datetime(2026, 3, 8, 7, 30, tzinfo=UTC)), CLOSED_SCHEDULED)
        # Both folds of 01:30 on the fall-back Sunday resolve to the same
        # declared local interval, without assuming a fixed UTC offset.
        self.assertEqual(observed_market_state(provider, datetime(2026, 11, 1, 5, 30, tzinfo=UTC)), OPEN)
        self.assertEqual(observed_market_state(provider, datetime(2026, 11, 1, 6, 30, tzinfo=UTC)), OPEN)

    def test_invalid_intervals_fail_closed(self) -> None:
        now = datetime(2026, 1, 4, 0, 30, tzinfo=UTC)
        invalid = (
            ({"startSecond": -1, "endSecond": 10},),
            ({"startSecond": 10, "endSecond": 10},),
            ({"startSecond": 10, "endSecond": 604_801},),
            ({"startSecond": 10},),
            ("not-an-interval",),
        )
        for schedule in invalid:
            with self.subTest(schedule=schedule):
                self.assertEqual(observed_market_state(_provider(_full_symbol(schedule=schedule)), now), UNKNOWN)

    def test_exact_holiday_closes_full_day_and_partial_windows(self) -> None:
        full_day = _provider(
            _full_symbol(
                schedule=({"startSecond": 0, "endSecond": 604_800},),
                holidays=(_holiday(date(2026, 1, 4)),),
            )
        )
        self.assertEqual(observed_market_state(full_day, datetime(2026, 1, 4, 12, tzinfo=UTC)), CLOSED_SCHEDULED)

        partial = _provider(
            _full_symbol(
                schedule=({"startSecond": 0, "endSecond": 604_800},),
                holidays=(_holiday(date(2026, 1, 4), start=3_600, end=7_200),),
            )
        )
        self.assertEqual(observed_market_state(partial, datetime(2026, 1, 4, 1, 30, tzinfo=UTC)), CLOSED_SCHEDULED)
        self.assertEqual(observed_market_state(partial, datetime(2026, 1, 4, 3, 30, tzinfo=UTC)), OPEN)

    def test_holiday_uses_own_timezone_and_requires_complete_metadata(self) -> None:
        now = datetime(2026, 1, 4, 12, tzinfo=UTC)
        mismatch = _provider(
            _full_symbol(
                schedule=({"startSecond": 0, "endSecond": 604_800},),
                holidays=(_holiday(date(2026, 1, 4), timezone="America/New_York"),),
            )
        )
        self.assertEqual(observed_market_state(mismatch, now), CLOSED_SCHEDULED)
        incomplete = _holiday(date(2026, 1, 4))
        del incomplete["isRecurring"]
        self.assertEqual(
            observed_market_state(
                _provider(_full_symbol(schedule=({"startSecond": 0, "endSecond": 604_800},), holidays=(incomplete,))),
                now,
            ),
            UNKNOWN,
        )

    def test_holiday_timezone_is_independent_at_date_boundary(self) -> None:
        provider = _provider(
            _full_symbol(
                timezone="America/New_York",
                schedule=({"startSecond": 0, "endSecond": 604_800},),
                holidays=(_holiday(date(2026, 1, 4), timezone="UTC"),),
            )
        )
        self.assertEqual(observed_market_state(provider, datetime(2026, 1, 4, 23, 30, tzinfo=UTC)), CLOSED_SCHEDULED)
        self.assertEqual(observed_market_state(provider, datetime(2026, 1, 5, 0, 30, tzinfo=UTC)), OPEN)

    def test_partial_holiday_uses_its_own_timezone(self) -> None:
        provider = _provider(
            _full_symbol(
                timezone="America/New_York",
                schedule=({"startSecond": 0, "endSecond": 604_800},),
                holidays=(_holiday(date(2026, 1, 4), timezone="UTC", start=3_600, end=7_200),),
            )
        )
        self.assertEqual(observed_market_state(provider, datetime(2026, 1, 4, 1, 30, tzinfo=UTC)), CLOSED_SCHEDULED)
        self.assertEqual(observed_market_state(provider, datetime(2026, 1, 4, 3, 30, tzinfo=UTC)), OPEN)

    def test_recurring_holiday_uses_its_own_date_boundary(self) -> None:
        provider = _provider(
            _full_symbol(
                timezone="America/New_York",
                schedule=({"startSecond": 0, "endSecond": 604_800},),
                holidays=(_holiday(date(2026, 1, 4), timezone="UTC", recurring=True),),
            )
        )
        self.assertEqual(observed_market_state(provider, datetime(2027, 1, 4, 23, 30, tzinfo=UTC)), CLOSED_SCHEDULED)
        self.assertEqual(observed_market_state(provider, datetime(2027, 1, 5, 0, 30, tzinfo=UTC)), OPEN)

    def test_holiday_invalid_timezone_or_zero_window_remains_unknown(self) -> None:
        now = datetime(2026, 1, 4, 12, tzinfo=UTC)
        for holiday in (
            _holiday(date(2026, 1, 4), timezone=""),
            _holiday(date(2026, 1, 4), timezone="Not/IANA"),
            _holiday(date(2026, 1, 4), start=0, end=0),
        ):
            with self.subTest(holiday=holiday):
                provider = _provider(
                    _full_symbol(schedule=({"startSecond": 0, "endSecond": 604_800},), holidays=(holiday,))
                )
                self.assertEqual(observed_market_state(provider, now), UNKNOWN)

    def test_recurring_holiday_matches_month_and_day(self) -> None:
        provider = _provider(
            _full_symbol(
                schedule=({"startSecond": 0, "endSecond": 604_800},),
                holidays=(_holiday(date(2026, 1, 4), recurring=True),),
            )
        )
        self.assertEqual(observed_market_state(provider, datetime(2027, 1, 4, 12, tzinfo=UTC)), CLOSED_SCHEDULED)

    def test_catalog_provenance_is_compact_and_does_not_fill_missing_values(self) -> None:
        observed = datetime(2026, 1, 4, 0, 30, tzinfo=UTC)
        provider = _provider(
            _full_symbol(
                minVolume=100,
                stepVolume=50,
                commissionType=1,
            )
        )
        provenance = catalog_provenance(provider, observed)
        full = provenance["full_symbol"]
        self.assertEqual(provenance["market_state"], OPEN)
        self.assertEqual(provenance["observed_at"], "2026-01-04T00:30:00Z")
        self.assertEqual(full["digits"], 5)
        self.assertEqual(full["pip_position"], 4)
        self.assertIsNone(full["price_scale"])
        self.assertEqual(full["min_volume"], 100)
        self.assertEqual(full["step_volume"], 50)
        self.assertIsNone(full["max_volume"])
        self.assertEqual(full["costs"]["commission_type"], 1)
        self.assertIsNone(full["costs"]["commission"])
        self.assertNotIn("must-not-leak", str(provenance))

    def test_invalid_catalog_provenance_remains_explicitly_unknown(self) -> None:
        provenance = catalog_provenance(_provider(_full_symbol(timezone="Not/IANA")), datetime(2026, 1, 4, tzinfo=UTC))
        self.assertTrue(provenance["catalog_identity_valid"])
        self.assertEqual(provenance["market_state"], UNKNOWN)
        self.assertIsNone(provenance["full_symbol"]["schedule_timezone_resolved"])
        self.assertIsNone(provenance["full_symbol"]["price_scale"])


if __name__ == "__main__":
    unittest.main()
