"""Calendar-aware numerical continuity remains opt-in and checkpoint-bound."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.core.historical_calendar import HistoricalQuoteCalendar
from mtf_lab.core.indicators import IncrementalIndicatorEngine
from mtf_lab.core.models import Candle, EventKind, MarketEvent, PriceBase
from mtf_lab.runtime.processor import IncrementalProcessor


class QuoteCalendarIntegrationTests(unittest.TestCase):
    def test_weekly_closure_keeps_d1_warmup_without_inventing_saturdays(self) -> None:
        calendar = HistoricalQuoteCalendar()
        model = IncrementalIndicatorEngine(max_points=2, historical_calendar=calendar)
        strict = IncrementalIndicatorEngine(max_points=2)
        start = datetime(2016, 1, 1, tzinfo=UTC)
        points = []
        for offset in range(80):
            date = start + timedelta(days=offset)
            if date.weekday() == 5:
                continue
            price = 1.1 + offset * 0.00001
            bar = Candle(
                instrument="EUR/USD",
                timeframe="D1",
                start=date,
                end=date + timedelta(days=1),
                open=price,
                high=price + 0.0002,
                low=price - 0.0001,
                close=price + 0.0001,
                price_base=PriceBase.MID,
                closed=True,
            )
            points.append(model.update(bar))
            strict.update(bar)
        self.assertIsNotNone(points[-1].ema_slow)
        self.assertIsNone(strict.points[-1].ema_slow)
        self.assertEqual(len(points), 68)
        self.assertTrue(any(issue.code == "modeled_scheduled_closure" for issue in model.series.issues))
        clone = IncrementalIndicatorEngine(max_points=2, historical_calendar=calendar)
        clone.restore(model.snapshot())
        self.assertEqual(clone.snapshot().to_dict(), model.snapshot().to_dict())
        with self.assertRaises(ValueError):
            IncrementalIndicatorEngine(max_points=2).restore(model.snapshot())

    def test_processor_resume_preserves_explicit_partial_and_calendar_identity(self) -> None:
        start = datetime(2016, 3, 7, tzinfo=UTC)
        processor = IncrementalProcessor(
            instrument="EUR/USD",
            price_base="mid",
            max_candles=64,
            quote_coverage_mode="continuous_quotes",
            max_quote_gap_seconds=90,
            historical_calendar=HistoricalQuoteCalendar(),
            market_candidate_id="dc_m5_v1",
        )
        for index, seconds in enumerate((0.1, 59.9, 60.1, 119.9, 120.1)):
            instant = start + timedelta(seconds=seconds)
            processor.process_event(
                MarketEvent(
                    instrument="EUR/USD",
                    event_time=instant,
                    available_at=instant,
                    bid=1.1,
                    ask=1.1001,
                    price_base=PriceBase.MID,
                    event_kind=EventKind.QUOTE,
                    source_event_id=str(index),
                    sequence=index,
                )
            )
        snapshot = processor.checkpoint()
        self.assertEqual(snapshot["market_candidate_id"], "dc_m5_v1")
        self.assertIs(snapshot["aggregators"]["M1"]["bucket"]["partial"], False)
        clone = IncrementalProcessor.from_checkpoint(snapshot)
        self.assertEqual(clone.checkpoint(), snapshot)
        self.assertIsNone(clone.latest_indicator_point("M1", at=start + timedelta(seconds=60)))
        self.assertIsNotNone(clone.latest_indicator_point("M1", at=start + timedelta(seconds=60.1)))
        changed = dict(snapshot, quote_coverage_mode="strict")
        with self.assertRaises(ValueError):
            IncrementalProcessor.from_checkpoint(changed)

    def test_unregistered_tp_identity_is_rejected_before_input(self) -> None:
        with self.assertRaises(ValueError):
            IncrementalProcessor(market_candidate_id="tp_unregistered")


if __name__ == "__main__":
    unittest.main()
