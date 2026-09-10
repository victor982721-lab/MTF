"""Regresiones de disponibilidad causal y cobertura parcial de agregación."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from mtf_lab.core import CandleAggregator, MarketEvent, QualityFlag, aggregate_events


UTC = timezone.utc
BASE = datetime(2026, 1, 1, tzinfo=UTC)


def event(seconds: int, *, received_seconds: int | None = None, available_seconds: int | None = None) -> MarketEvent:
    timestamp = BASE + timedelta(seconds=seconds)
    received = BASE + timedelta(seconds=received_seconds if received_seconds is not None else seconds)
    available = BASE + timedelta(seconds=available_seconds if available_seconds is not None else (received_seconds if received_seconds is not None else seconds))
    return MarketEvent(
        instrument="TEST/USD",
        event_time=timestamp,
        price=100.0 + seconds,
        received_at=received,
        available_at=available,
        source="coverage-fixture",
        source_event_id=f"event-{seconds}-{received.timestamp()}",
    )


class AggregationCoverageTests(unittest.TestCase):
    def test_boundary_emission_uses_knowledge_time_not_nominal_end(self) -> None:
        aggregator = CandleAggregator("M5", instrument="TEST/USD")
        aggregator.add(event(0))
        result = aggregator.add(event(300, received_seconds=307, available_seconds=307))
        self.assertEqual(len(result.candles), 1)
        candle = result.candles[0]
        self.assertEqual(candle.start, BASE)
        self.assertEqual(candle.end, BASE + timedelta(minutes=5))
        self.assertEqual(candle.available_at, BASE + timedelta(seconds=307))
        self.assertEqual(candle.metadata["emitted_at"], (BASE + timedelta(seconds=307)).isoformat())
        self.assertFalse(candle.quality.has(QualityFlag.PARTIAL))

    def test_starting_mid_interval_never_claims_complete_coverage(self) -> None:
        candles = aggregate_events([event(127)], "M5")
        self.assertEqual(len(candles), 1)
        candle = candles[0]
        self.assertEqual(candle.start, BASE)
        self.assertEqual(candle.end, BASE + timedelta(minutes=5))
        self.assertFalse(candle.closed)
        self.assertTrue(candle.quality.has(QualityFlag.PARTIAL))
        self.assertTrue(candle.metadata["partial"])
        self.assertEqual(candle.available_at, BASE + timedelta(seconds=127))

    def test_elapsed_partial_interval_is_closed_temporally_but_quality_blocks_analysis(self) -> None:
        aggregator = CandleAggregator("M5", instrument="TEST/USD")
        aggregator.add(event(127))
        result = aggregator.close_until(BASE + timedelta(minutes=5))
        self.assertEqual(len(result.candles), 1)
        candle = result.candles[0]
        self.assertTrue(candle.closed)
        self.assertTrue(candle.quality.has(QualityFlag.PARTIAL))
        self.assertEqual(candle.available_at, BASE + timedelta(minutes=5))

    def test_gap_has_no_fabricated_candle_and_is_reported(self) -> None:
        aggregator = CandleAggregator("M5", instrument="TEST/USD")
        aggregator.add(event(0))
        result = aggregator.add(event(660))  # [00:05,00:10) queda sin eventos.
        self.assertEqual([candle.start for candle in result.candles], [BASE])
        self.assertIn("gap", {issue.code for issue in result.issues})
        self.assertIn("partial_bucket", {issue.code for issue in result.issues})
        final = aggregator.flush()
        self.assertEqual([candle.start for candle in final.candles], [BASE + timedelta(minutes=10)])
        self.assertEqual(len([candle for candle in result.candles + final.candles if candle.start == BASE + timedelta(minutes=5)]), 0)

    def test_late_boundary_event_does_not_move_availability_back_to_interval_end(self) -> None:
        aggregator = CandleAggregator("M5", instrument="TEST/USD")
        aggregator.add(event(0, received_seconds=1, available_seconds=1))
        result = aggregator.add(event(300, received_seconds=1200, available_seconds=1200))
        candle = result.candles[0]
        self.assertEqual(candle.available_at, BASE + timedelta(seconds=1200))
        self.assertGreater(candle.available_at, candle.end)


if __name__ == "__main__":
    unittest.main()
