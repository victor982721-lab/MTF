"""Regresiones del acumulador online de cobertura explícita de cotizaciones."""

from __future__ import annotations

import copy
import json
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any

from mtf_lab.core.aggregation import CandleAggregator, interval_start
from mtf_lab.core.historical_calendar import HistoricalQuoteCalendar
from mtf_lab.core.models import Candle, EventKind, MarketEvent, PriceBase, Timeframe
from mtf_lab.core.quality import DataQuality, QualityFlag, QualityIssue

BASE = datetime(2016, 3, 7, tzinfo=UTC)


def quote(
    seconds: float,
    sequence: int,
    *,
    source: str = "stream-fixture",
    quality: DataQuality | None = None,
    received: bool = True,
    origin: datetime = BASE,
) -> MarketEvent:
    event_time = origin + timedelta(seconds=seconds)
    received_at = event_time if received else None
    bid = 1.1000 + sequence * 0.0001
    return MarketEvent(
        instrument="EUR/USD",
        event_time=event_time,
        bid=bid,
        ask=bid + 0.0002,
        quantity=1.0,
        received_at=received_at,
        available_at=event_time,
        source=source,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        source_event_id=f"{source}:row:{sequence}",
        sequence=sequence,
        quality=quality or DataQuality.good(),
    )


def continuous(
    timeframe: str = "M1",
    *,
    max_gap: float = 90.0,
    calendar: HistoricalQuoteCalendar | None = None,
) -> CandleAggregator:
    return CandleAggregator(
        timeframe,
        instrument="EUR/USD",
        price_base=PriceBase.MID,
        coverage_mode="continuous_quotes",
        max_quote_gap_seconds=max_gap,
        historical_calendar=calendar,
    )


def semantic(candle: Candle) -> tuple[Any, ...]:
    """Compare the output contract without depending on object identity."""

    return (
        candle.start,
        candle.end,
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.volume,
        candle.event_count,
        candle.closed,
        candle.quality.flags,
        candle.metadata["partial"],
    )


def reference_semantics(events: list[MarketEvent], timeframe: str, max_gap: float) -> list[tuple[Any, ...]]:
    """Small independent reference for the no-calendar online contract."""

    tf = Timeframe(timeframe.upper(), {"M1": 60, "H1": 3600}[timeframe.upper()])
    groups: list[tuple[datetime, list[MarketEvent]]] = []
    for event in events:
        start = interval_start(event.event_time, tf)
        if not groups or groups[-1][0] != start:
            groups.append((start, [event]))
        else:
            groups[-1][1].append(event)

    expected: list[tuple[Any, ...]] = []
    for index, (start, bucket_events) in enumerate(groups):
        end = start + tf.delta
        previous = groups[index - 1][1][-1] if index else None
        cross_gap = event_gap(previous, bucket_events[0]) if previous is not None else None
        contiguous = (
            previous is not None
            and groups[index - 1][0] + tf.delta == start
            and cross_gap is not None
            and cross_gap <= max_gap
        )
        partial = bucket_events[0].event_time > start and not contiguous
        prices = [event.selected_price for event in bucket_events]
        assert all(price is not None for price in prices)
        values = [float(price) for price in prices if price is not None]
        gaps = [event_gap(left, right) for left, right in zip(bucket_events, bucket_events[1:], strict=False)]
        gaps.append(event_gap(bucket_events[-1], None, end=end))
        flags: set[QualityFlag] = set()
        if any(gap > max_gap for gap in gaps):
            flags.add(QualityFlag.GAP)
        if partial:
            flags.add(QualityFlag.PARTIAL)
        expected.append(
            (
                start,
                end,
                values[0],
                max(values),
                min(values),
                values[-1],
                float(sum(event.quantity or 0.0 for event in bucket_events)),
                len(bucket_events),
                True,
                frozenset(flags),
                partial,
            )
        )
    return expected


def event_gap(left: MarketEvent, right: MarketEvent | None, *, end: datetime | None = None) -> float:
    if right is None:
        if end is None:
            raise ValueError("end is required for a trailing gap")
        right_time = end
    else:
        right_time = right.event_time
    return float((right_time - left.event_time).total_seconds())


class StreamingQuoteAggregationTests(unittest.TestCase):
    def test_dense_quotes_match_online_ohlc_and_are_not_partial_at_boundaries(self) -> None:
        events = [
            quote(seconds, index) for index, seconds in enumerate((0.0, 10.0, 30.0, 59.9, 60.0, 70.0, 119.9, 120.0))
        ]
        engine = continuous(max_gap=90.0)
        emitted: list[Candle] = []
        for event in events:
            emitted.extend(engine.add(event).emitted)

        self.assertEqual(len(emitted), 2)
        self.assertEqual([candle.event_count for candle in emitted], [4, 3])
        self.assertTrue(all(candle.quality.valid for candle in emitted))
        self.assertEqual(emitted[0].open, events[0].selected_price)
        self.assertEqual(emitted[0].close, events[3].selected_price)
        self.assertEqual(emitted[1].open, events[4].selected_price)
        self.assertEqual(emitted[1].close, events[6].selected_price)
        self.assertEqual(emitted[1].metadata["partial"], False)

    def test_sparse_intervals_remain_absent_and_boundary_continuity_is_explicit(self) -> None:
        events = [quote(seconds, index) for index, seconds in enumerate((0.0, 59.0, 60.5, 119.5, 240.5, 299.5))]
        engine = continuous()
        emitted: list[Candle] = []
        issues: list[QualityIssue] = []
        for event in events:
            result = engine.add(event)
            emitted.extend(result.emitted)
            issues.extend(result.issues)
        emitted.extend(engine.flush().emitted)

        self.assertEqual(
            [candle.start for candle in emitted], [BASE, BASE + timedelta(seconds=60), BASE + timedelta(seconds=240)]
        )
        self.assertEqual(emitted[0].metadata["partial"], False)
        self.assertEqual(emitted[1].metadata["partial"], False)
        self.assertTrue(emitted[2].quality.has(QualityFlag.PARTIAL))
        self.assertIn("gap", {issue.code for issue in issues})

    def test_long_intra_bucket_gap_sets_gap_without_filling_or_dropping_ohlc(self) -> None:
        engine = continuous("H1", max_gap=90.0)
        events = [quote(seconds, index) for index, seconds in enumerate((0.0, 1000.0, 3599.0, 3600.0))]
        result: list[Candle] = []
        for event in events:
            result.extend(engine.add(event).emitted)

        self.assertEqual(len(result), 1)
        candle = result[0]
        self.assertTrue(candle.quality.has(QualityFlag.GAP))
        self.assertEqual(candle.event_count, 3)
        self.assertEqual(candle.high, events[2].selected_price)
        self.assertEqual(candle.low, events[0].selected_price)
        self.assertEqual(candle.metadata["coverage_mode"], "continuous_quotes")

    def test_dst_weekly_closure_uses_open_seconds_and_keeps_reopen_non_partial(self) -> None:
        calendar = HistoricalQuoteCalendar()
        cases = (
            (datetime(2016, 3, 11, 21, 59, 59, tzinfo=UTC), datetime(2016, 3, 13, 21, 0, 0, 100_000, tzinfo=UTC)),
            (datetime(2016, 11, 4, 20, 59, 59, tzinfo=UTC), datetime(2016, 11, 6, 22, 0, 0, 100_000, tzinfo=UTC)),
        )
        for index, (before, after) in enumerate(cases):
            with self.subTest(index=index):
                engine = continuous("H1", max_gap=3600.0, calendar=calendar)
                engine.add(quote(0, 0, origin=before))
                result = engine.add(quote(0, 1, origin=after))
                final = engine.flush().emitted

                self.assertEqual(len(result.emitted), 1)
                self.assertIn("modeled_scheduled_closure", {issue.code for issue in result.issues})
                self.assertEqual(len(final), 1)
                self.assertFalse(final[0].quality.has(QualityFlag.PARTIAL))
                self.assertFalse(final[0].quality.has(QualityFlag.GAP))
                self.assertFalse(final[0].metadata["partial"])

    def test_equal_event_times_with_distinct_source_rows_are_retained(self) -> None:
        engine = continuous()
        first = quote(0.0, 0)
        second = quote(0.0, 1)
        boundary = quote(60.0, 2)
        engine.add(first)
        engine.add(second)
        bars = engine.add(boundary).emitted

        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].event_count, 2)
        self.assertEqual(bars[0].open, first.selected_price)
        self.assertEqual(bars[0].close, second.selected_price)
        self.assertNotEqual(first.event_id, second.event_id)

    def test_restore_round_trip_is_equivalent_to_uninterrupted_stream(self) -> None:
        events = [quote(seconds, index) for index, seconds in enumerate((0.0, 15.0, 30.0, 60.0, 75.0, 105.0, 120.0))]
        uninterrupted = continuous(max_gap=90.0)
        for event in events[:5]:
            uninterrupted.add(event)
        snapshot = uninterrupted.export_bucket_state()
        assert snapshot is not None

        resumed = continuous(max_gap=90.0)
        resumed.restore_bucket_state(json.loads(json.dumps(snapshot)))
        uninterrupted_tail: list[Candle] = []
        resumed_tail: list[Candle] = []
        for event in events[5:]:
            uninterrupted_tail.extend(uninterrupted.add(event).emitted)
            resumed_tail.extend(resumed.add(event).emitted)
        uninterrupted_tail.extend(uninterrupted.flush().emitted)
        resumed_tail.extend(resumed.flush().emitted)

        self.assertEqual(
            [semantic(candle) for candle in resumed_tail], [semantic(candle) for candle in uninterrupted_tail]
        )
        self.assertEqual(resumed.export_bucket_state(), uninterrupted.export_bucket_state())

    def test_restore_validation_is_atomic_and_rejects_inconsistent_state(self) -> None:
        engine = continuous()
        engine.add(quote(0.0, 0))
        engine.add(quote(10.0, 1))
        before = engine.export_bucket_state()
        assert before is not None
        before_last_time = engine.last_event_time

        malformed = copy.deepcopy(before)
        malformed_stats = malformed["stats"]
        assert isinstance(malformed_stats, dict)
        malformed_stats["event_count"] = 0
        with self.assertRaises(ValueError):
            engine.restore_bucket_state(malformed)
        self.assertEqual(engine.export_bucket_state(), before)
        self.assertEqual(engine.last_event_time, before_last_time)

        malformed = copy.deepcopy(before)
        malformed["timeframe"] = "H1"
        with self.assertRaises(ValueError):
            engine.restore_bucket_state(malformed)
        self.assertEqual(engine.export_bucket_state(), before)

    def test_accumulator_and_export_are_bounded_without_silent_truncation(self) -> None:
        engine = continuous("D1", max_gap=90.0)
        for index in range(1000):
            engine.add(
                quote(
                    index / 10.0,
                    index,
                    source=f"source-{index}",
                    quality=DataQuality(reasons=(f"reason-{index}",)),
                )
            )

        bucket = engine._bucket
        assert bucket is not None
        self.assertEqual(bucket.events, [])
        self.assertIsNotNone(bucket.accumulator)
        state = engine.export_bucket_state()
        assert state is not None
        stats = state["stats"]
        assert isinstance(stats, dict)
        self.assertEqual(stats["event_count"], 1000)
        self.assertLessEqual(len(stats["quality_reasons"]), 64)
        self.assertEqual(stats["quality_reason_overflow"], 936)
        self.assertLessEqual(len(stats["source_counts"]), 16)
        self.assertEqual(stats["source_overflow"], 984)
        final = engine.flush().emitted
        self.assertEqual(len(final), 1)
        self.assertIn("quality_reasons_truncated:936", final[0].quality.reasons)
        self.assertIn("source_counts_truncated:984", final[0].quality.reasons)
        self.assertNotIn("events", stats)
        self.assertLess(len(json.dumps(state, sort_keys=True)), 10_000)

    def test_stream_matches_independent_reference_for_dense_sparse_and_gap_inputs(self) -> None:
        events = [
            quote(0.0, 0),
            quote(20.0, 1),
            quote(59.0, 2),
            quote(60.5, 3),
            quote(119.5, 4),
            quote(180.5, 5),
            quote(240.5, 6),
            quote(260.5, 7),
        ]
        engine = continuous(max_gap=90.0)
        actual: list[Candle] = []
        for event in events:
            actual.extend(engine.add(event).emitted)
        actual.extend(engine.flush().emitted)

        self.assertEqual([semantic(candle) for candle in actual], reference_semantics(events, "M1", 90.0))


if __name__ == "__main__":
    unittest.main()
