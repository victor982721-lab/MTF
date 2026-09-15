"""Pruebas del contexto virtual y calendario modelado explícito."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.ops.historical_assumptions import (
    CALENDAR_BASIS,
    CALENDAR_TIMEZONE,
    EURUSD_VIRTUAL_10K_MODEL_ID,
    PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID,
    VIRTUAL_EURUSD_10K_MODEL_ID,
    HistoricalAssumptionError,
    calendar_template_for,
    historical_assumptions_for,
)


class HistoricalAssumptionsTests(unittest.TestCase):
    def test_account_model_requires_literal_and_keeps_broker_unknowns(self) -> None:
        model = historical_assumptions_for(VIRTUAL_EURUSD_10K_MODEL_ID)
        self.assertEqual(model.model_id, EURUSD_VIRTUAL_10K_MODEL_ID)
        account = model.account
        self.assertEqual(account.initial_equity, 10_000)
        self.assertEqual(account.unit_value, 1)
        self.assertEqual(account.quantity_min, 1)
        self.assertEqual(account.quantity_step, 1)
        self.assertFalse(account.fees_known)
        self.assertIsNone(account.quantity_max)
        self.assertIsNone(account.minimum_stop_distance)
        self.assertIsNone(account.margin_per_unit)
        self.assertFalse(account.contract_observed)
        self.assertFalse(account.eligible_for_demo)
        self.assertIn("No es grid/contrato observado", account.disclaimer)
        self.assertFalse(account.contract_spec["known"])
        self.assertEqual(account.contract_spec["fees_known"], False)
        self.assertIsNone(account.contract_spec["quantity_max"])
        self.assertTrue(account.contract_spec["server_side_stops"])
        self.assertEqual(
            account.contract_spec["server_protection_basis"],
            "MODELED_SERVER_PROTECTIONS_NOT_OBSERVED",
        )
        self.assertEqual(len(model.assumption_hash), 64)
        self.assertEqual(model.to_dict(), model.to_dict())
        with self.assertRaises(HistoricalAssumptionError):
            historical_assumptions_for("default")

    def test_calendar_factory_has_no_implicit_default_and_hashes_template(self) -> None:
        calendar = calendar_template_for(PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID)
        self.assertEqual(calendar.timezone, CALENDAR_TIMEZONE)
        self.assertEqual(calendar.basis, CALENDAR_BASIS)
        self.assertFalse(calendar.observed)
        self.assertFalse(calendar.exceptions_known)
        self.assertFalse(calendar.financing_amount_known)
        self.assertEqual(calendar.daily_financing_time.isoformat(), "17:00:00")
        self.assertEqual(calendar.friday_close_time.isoformat(), "16:55:00")
        self.assertEqual(calendar.sunday_open_time.isoformat(), "17:01:00")
        self.assertEqual(calendar.daily_break_close_time.isoformat(), "16:59:00")
        self.assertEqual(calendar.daily_break_open_time.isoformat(), "17:01:00")
        self.assertEqual(len(calendar.calendar_hash), 64)
        with self.assertRaises(HistoricalAssumptionError):
            calendar_template_for("default")

    def test_calendar_marks_are_dynamic_not_stale_first_day_values(self) -> None:
        calendar = calendar_template_for(PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID)
        monday = datetime(2026, 9, 14, 16, 0, tzinfo=UTC)
        following_monday = monday + timedelta(days=7)
        first = calendar.observation_for(monday)
        later = calendar.observation_for(following_monday)
        self.assertEqual(first.next_financing_at, datetime(2026, 9, 14, 21, 0, tzinfo=UTC))
        self.assertEqual(first.next_weekly_close_at, datetime(2026, 9, 18, 20, 55, tzinfo=UTC))
        self.assertEqual(first.next_weekly_open_at, datetime(2026, 9, 20, 21, 1, tzinfo=UTC))
        self.assertEqual(first.next_daily_break_close_at, datetime(2026, 9, 14, 20, 59, tzinfo=UTC))
        self.assertEqual(first.next_daily_break_open_at, datetime(2026, 9, 14, 21, 1, tzinfo=UTC))
        self.assertEqual(later.next_financing_at, datetime(2026, 9, 21, 21, 0, tzinfo=UTC))
        self.assertNotEqual(first.next_financing_at, later.next_financing_at)
        state = calendar.state_for_quote(monday)
        self.assertTrue(state["known"])
        self.assertFalse(state["financing_known"])
        self.assertFalse(state["exceptions_known"])
        self.assertFalse(state["calendar_observed"])
        self.assertEqual(state["calendar_basis"], CALENDAR_BASIS)
        self.assertIn("daily_close_at", state)
        self.assertIn("daily_cut_at", state)
        self.assertNotIn("market_cut_at", state)

    def test_calendar_gate_blocks_weekend_and_preclose_entries(self) -> None:
        calendar = calendar_template_for(PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID)
        friday_before_close = datetime(2026, 9, 18, 20, 30, tzinfo=UTC)
        friday_after_close = datetime(2026, 9, 18, 21, 0, tzinfo=UTC)
        sunday_before_open = datetime(2026, 9, 20, 20, 30, tzinfo=UTC)
        monday_open = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)
        daily_break = datetime(2026, 9, 15, 21, 0, tzinfo=UTC)
        self.assertEqual(calendar.entry_gate(friday_before_close).reason, "PRE_WEEKLY_CLOSE")
        self.assertEqual(calendar.entry_gate(friday_after_close).reason, "WEEKEND_CLOSED")
        self.assertEqual(calendar.entry_gate(sunday_before_open).reason, "WEEKEND_CLOSED")
        self.assertEqual(calendar.entry_gate(daily_break).reason, "DAILY_BREAK")
        self.assertTrue(calendar.entry_gate(monday_open).allowed)

    def test_daily_break_projection_keeps_new_york_midnight_open_and_tracks_dst(self) -> None:
        calendar = calendar_template_for(PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID)
        # The January midnight in New York is not the public server break.
        self.assertTrue(calendar.entry_gate(datetime(2016, 1, 4, 5, 0, tzinfo=UTC)).allowed)
        self.assertEqual(
            calendar.observation_for(datetime(2016, 1, 4, 5, 0, tzinfo=UTC)).next_daily_break_close_at,
            datetime(2016, 1, 4, 21, 59, tzinfo=UTC),
        )
        self.assertEqual(
            calendar.observation_for(datetime(2016, 1, 4, 5, 0, tzinfo=UTC)).next_daily_break_open_at,
            datetime(2016, 1, 4, 22, 1, tzinfo=UTC),
        )
        self.assertEqual(
            calendar.entry_gate(datetime(2016, 1, 4, 21, 59, tzinfo=UTC)).reason,
            "DAILY_BREAK",
        )
        self.assertTrue(calendar.entry_gate(datetime(2016, 1, 4, 22, 1, tzinfo=UTC)).allowed)
        # During US daylight time the same New York wall-clock window shifts
        # one hour earlier in UTC.
        self.assertEqual(
            calendar.observation_for(datetime(2026, 7, 6, 12, 0, tzinfo=UTC)).next_daily_break_close_at,
            datetime(2026, 7, 6, 20, 59, tzinfo=UTC),
        )
        self.assertEqual(
            calendar.observation_for(datetime(2026, 7, 6, 12, 0, tzinfo=UTC)).next_daily_break_open_at,
            datetime(2026, 7, 6, 21, 1, tzinfo=UTC),
        )
        self.assertEqual(
            calendar.entry_gate(datetime(2026, 7, 6, 20, 59, tzinfo=UTC)).reason,
            "DAILY_BREAK",
        )


if __name__ == "__main__":
    unittest.main()
