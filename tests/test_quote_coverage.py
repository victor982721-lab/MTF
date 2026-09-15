"""Explicit sparse quote coverage is distinct from an unobserved tick boundary."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.core.aggregation import CandleAggregator
from mtf_lab.core.models import EventKind, MarketEvent, PriceBase
from mtf_lab.core.quality import QualityFlag

START = datetime(2016, 3, 7, tzinfo=UTC)


def quote(seconds: float, sequence: int) -> MarketEvent:
    instant = START + timedelta(seconds=seconds)
    return MarketEvent(
        instrument="EUR/USD",
        event_time=instant,
        available_at=instant,
        bid=1.1 + sequence * 0.00001,
        ask=1.1001 + sequence * 0.00001,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        source="histdata_qa",
        source_event_id=f"row:{sequence}",
        sequence=sequence,
    )


def aggregator(timeframe: str = "M1", mode: str = "continuous_quotes") -> CandleAggregator:
    return CandleAggregator(
        timeframe,
        instrument="EUR/USD",
        price_base=PriceBase.MID,
        coverage_mode=mode,
        max_quote_gap_seconds=90 if mode == "continuous_quotes" else None,
        max_seen_event_ids=32,
        max_issues=16,
    )


class QuoteCoverageTests(unittest.TestCase):
    def test_ms_quote_boundaries_do_not_make_every_interval_partial(self) -> None:
        engine = aggregator()
        bars = []
        for index, seconds in enumerate((0.1, 59.9, 60.1, 119.9, 120.1)):
            bars.extend(engine.add(quote(seconds, index)).emitted)
        self.assertEqual(len(bars), 2)
        self.assertTrue(bars[0].quality.has(QualityFlag.PARTIAL))
        self.assertTrue(bars[1].quality.valid)
        self.assertEqual(bars[1].event_count, 2)
        self.assertEqual(bars[1].available_at, START + timedelta(seconds=120.1))
        self.assertIsNone(bars[1].received_at)
        self.assertEqual(bars[1].metadata["volume_basis"], "UNKNOWN_NOT_TRADED_VOLUME")

    def test_legacy_strict_mode_is_unchanged(self) -> None:
        engine = aggregator(mode="strict")
        bars = []
        for index, seconds in enumerate((0.1, 59.9, 60.1, 119.9, 120.1)):
            bars.extend(engine.add(quote(seconds, index)).emitted)
        self.assertTrue(all(bar.quality.has(QualityFlag.PARTIAL) for bar in bars))
        self.assertTrue(all("coverage_mode" not in bar.metadata for bar in bars))

    def test_missing_interval_is_not_filled_or_reclassified(self) -> None:
        engine = aggregator()
        bars = []
        for index, seconds in enumerate((0, 59, 60.1, 119, 240.1, 299, 300.1)):
            bars.extend(engine.add(quote(seconds, index)).emitted)
        self.assertEqual([int((bar.start - START).total_seconds()) for bar in bars], [0, 60, 240])
        self.assertTrue(bars[-1].quality.has(QualityFlag.PARTIAL))

    def test_long_intra_bar_quote_gap_is_preserved(self) -> None:
        engine = aggregator("H1")
        for index, seconds in enumerate((0, 1800, 3599)):
            engine.add(quote(seconds, index))
        bars = engine.add(quote(3600.1, 3)).emitted
        self.assertTrue(bars[0].quality.has(QualityFlag.GAP))

    def test_equal_timestamps_with_different_source_rows_survive(self) -> None:
        engine = aggregator()
        engine.add(quote(0, 0))
        engine.add(quote(0, 1))
        bars = engine.add(quote(60, 2)).emitted
        self.assertEqual(bars[0].event_count, 2)
        self.assertGreater(bars[0].close, bars[0].open)

    def test_mode_requires_bound_and_quote_type(self) -> None:
        with self.assertRaises(ValueError):
            CandleAggregator("M1", coverage_mode="continuous_quotes")
        engine = aggregator()
        event = MarketEvent(instrument="EUR/USD", event_time=START, price=1.1)
        self.assertFalse(engine.add(event).accepted)


if __name__ == "__main__":
    unittest.main()
