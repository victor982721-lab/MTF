"""Focused tests for the modeled historical weekly quote calendar."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.core.historical_calendar import (
    CALENDAR_BASIS,
    CALENDAR_PROVENANCE_URI,
    HistoricalCalendarError,
    HistoricalQuoteCalendar,
)


class HistoricalQuoteCalendarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calendar = HistoricalQuoteCalendar()

    def test_default_identity_serializes_and_round_trips(self) -> None:
        payload = self.calendar.to_dict()
        self.assertEqual(self.calendar.timezone, "America/New_York")
        self.assertEqual(self.calendar.friday_close, "17:00")
        self.assertEqual(self.calendar.sunday_open, "17:00")
        self.assertEqual(self.calendar.basis, CALENDAR_BASIS)
        self.assertEqual(self.calendar.provenance_uri, CALENDAR_PROVENANCE_URI)
        self.assertEqual(len(self.calendar.calendar_hash), 64)
        self.assertEqual(HistoricalQuoteCalendar.from_mapping(payload), self.calendar)

    def test_identity_tampering_is_rejected(self) -> None:
        payload = self.calendar.to_dict()
        payload["calendar_hash"] = "0" * 64
        with self.assertRaisesRegex(HistoricalCalendarError, "calendar_hash"):
            HistoricalQuoteCalendar.from_mapping(payload)
        payload = self.calendar.to_dict()
        payload["basis"] = "ACCOUNT_VERIFIED"
        with self.assertRaises(HistoricalCalendarError):
            HistoricalQuoteCalendar.from_mapping(payload)

    def test_weekly_closure_is_exactly_friday_to_sunday_in_new_york(self) -> None:
        close = datetime(2016, 3, 11, 22, tzinfo=UTC)
        reopen = datetime(2016, 3, 13, 21, tzinfo=UTC)
        self.assertEqual(self.calendar.open_seconds_between(close, reopen), 0.0)
        self.assertTrue(self.calendar.covers_closed(close, reopen))
        self.assertFalse(self.calendar.covers_closed(close - timedelta(seconds=1), reopen))
        self.assertAlmostEqual(
            self.calendar.open_seconds_between(
                datetime(2016, 3, 11, 21, 59, 59, tzinfo=UTC),
                datetime(2016, 3, 13, 21, 0, 0, 100_000, tzinfo=UTC),
            ),
            1.1,
        )

    def test_dst_spring_and_fall_change_the_closed_elapsed_duration(self) -> None:
        spring_close = datetime(2016, 3, 11, 22, tzinfo=UTC)
        spring_open = datetime(2016, 3, 13, 21, tzinfo=UTC)
        fall_close = datetime(2016, 11, 4, 21, tzinfo=UTC)
        fall_open = datetime(2016, 11, 6, 22, tzinfo=UTC)
        self.assertEqual((spring_open - spring_close).total_seconds(), 47 * 3600)
        self.assertEqual((fall_open - fall_close).total_seconds(), 49 * 3600)
        self.assertEqual(self.calendar.open_seconds_between(spring_close, spring_open), 0.0)
        self.assertEqual(self.calendar.open_seconds_between(fall_close, fall_open), 0.0)

    def test_friday_close_and_sunday_reopen_boundaries_are_not_rounded(self) -> None:
        before_close = datetime(2016, 3, 11, 21, 59, 59, tzinfo=UTC)
        after_reopen = datetime(2016, 3, 13, 21, 0, 0, 100_000, tzinfo=UTC)
        self.assertEqual(self.calendar.open_seconds_between(before_close, before_close + timedelta(seconds=1)), 1.0)
        self.assertEqual(self.calendar.open_seconds_between(after_reopen - timedelta(seconds=0.1), after_reopen), 0.1)
        self.assertFalse(self.calendar.covers_closed(after_reopen, after_reopen + timedelta(seconds=1)))

    def test_weekday_and_holiday_are_not_silently_marked_closed(self) -> None:
        monday_start = datetime(2016, 7, 4, 14, tzinfo=UTC)
        monday_end = monday_start + timedelta(hours=1)
        self.assertEqual(self.calendar.open_seconds_between(monday_start, monday_end), 3600.0)
        self.assertFalse(self.calendar.covers_closed(monday_start, monday_end))
        self.assertNotIn("holidays", self.calendar.to_dict())

    def test_intervals_require_aware_utc_and_are_bounded(self) -> None:
        aware = datetime(2016, 3, 7, tzinfo=UTC)
        with self.assertRaises(HistoricalCalendarError):
            self.calendar.open_seconds_between(datetime(2016, 3, 7), aware)
        with self.assertRaises(HistoricalCalendarError):
            self.calendar.open_seconds_between(aware + timedelta(seconds=1), aware)
        with self.assertRaisesRegex(HistoricalCalendarError, "32 days"):
            self.calendar.open_seconds_between(aware, aware + timedelta(days=32, seconds=1))
        self.assertEqual(self.calendar.open_seconds_between(aware, aware), 0.0)
        self.assertTrue(self.calendar.covers_closed(aware, aware))


if __name__ == "__main__":
    unittest.main()
