"""Reauditoría focal de causalidad de estrategia y checkpoints continuos."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.core import EventKind, MarketEvent, OperationMode, PriceBase
from mtf_lab.core.indicators import IndicatorConfig, IndicatorPoint
from mtf_lab.core.quality import DataQuality
from mtf_lab.core.strategy import StrategyConfig, TrendPullbackStrategy
from mtf_lab.runtime import IncrementalProcessor

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def point(
    start_minute: int,
    end_minute: int,
    available_minute: int,
    *,
    ema_fast: float,
    ema_slow: float,
    close: float = 100.0,
    rsi: float = 60.0,
    atr: float = 1.0,
) -> IndicatorPoint:
    return IndicatorPoint(
        BASE + timedelta(minutes=start_minute),
        BASE + timedelta(minutes=end_minute),
        BASE + timedelta(minutes=available_minute),
        close,
        ema_fast,
        ema_slow,
        rsi,
        atr,
        True,
        DataQuality.good(),
        f"point:{start_minute}:{available_minute}",
        start_minute,
    )


def strategy_config() -> StrategyConfig:
    return StrategyConfig(
        context_timeframe="M15",
        preparation_timeframe="M5",
        trigger_timeframe="M1",
        context_lookback=1,
        preparation_lookback=1,
        preparation_ttl_bars=3,
        max_distance_atr=10.0,
        indicators=IndicatorConfig(ema_fast=2, ema_slow=3, rsi_period=2, atr_period=2),
    )


def quote(second: int) -> MarketEvent:
    timestamp = BASE + timedelta(seconds=second)
    return MarketEvent(
        "EUR/USD",
        timestamp,
        bid=1.1000,
        ask=1.1002,
        received_at=timestamp,
        available_at=timestamp,
        mode=OperationMode.REPLAY,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        sequence=second,
        source_event_id=f"quote:{second}",
    )


class CausalityAndCheckpointReauditTests(unittest.TestCase):
    def test_public_batch_prefix_blocks_unavailable_context_lag(self) -> None:
        config = strategy_config()
        context = [
            point(0, 15, 40, ema_fast=100, ema_slow=100),
            point(15, 30, 30, ema_fast=105, ema_slow=101),
        ]
        triggers = [
            point(30, 31, 31, ema_fast=100, ema_slow=100),
            point(40, 41, 41, ema_fast=100, ema_slow=100),
        ]
        streams = {"M15": context, "M5": [], "M1": triggers}
        strategy = TrendPullbackStrategy(config)

        prefix = strategy.evaluate({**streams, "M1": triggers[:1]})
        full = strategy.evaluate(streams)
        early = next(item for item in prefix.evaluations if item.stage == "trigger")
        late = [item for item in full.evaluations if item.stage == "trigger"][-1]

        self.assertIsNone(early.direction)
        self.assertTrue({"context_not_available", "context_indicator_not_ready"}.intersection(early.reasons))
        self.assertEqual(late.direction, "UP")
        self.assertEqual(full.evaluations[0], early)

    def test_public_batch_delays_preparation_until_its_lag_is_available(self) -> None:
        config = strategy_config()
        context = [
            point(0, 15, 15, ema_fast=100, ema_slow=100),
            point(15, 30, 30, ema_fast=105, ema_slow=101, close=105),
        ]
        # The latest preparation point is available at 29, but its required
        # lag is not available until 35.  It may register only at the later
        # trigger watermark.
        preparation = [
            point(20, 25, 35, ema_fast=100, ema_slow=100, close=105),
            point(25, 30, 30, ema_fast=100, ema_slow=100, close=100),
        ]
        triggers = [
            point(30, 31, 31, ema_fast=100, ema_slow=100),
            point(40, 41, 41, ema_fast=100, ema_slow=100),
        ]
        result = TrendPullbackStrategy(config).evaluate({"M15": context, "M5": preparation, "M1": triggers})

        early_trigger = [item for item in result.evaluations if item.stage == "trigger"][0]
        self.assertIn("preparation_not_registered", early_trigger.reasons)
        preparation_evaluations = [item for item in result.evaluations if item.stage == "preparation"]
        self.assertEqual(
            [(item.timestamp, item.available_at) for item in preparation_evaluations],
            [
                (BASE + timedelta(minutes=25), BASE + timedelta(minutes=35)),
                (BASE + timedelta(minutes=30), BASE + timedelta(minutes=35)),
            ],
        )
        self.assertTrue(result.episodes)
        self.assertEqual(result.episodes[0].registered_at, BASE + timedelta(minutes=35))

    def test_continuous_standalone_checkpoint_roundtrip_preserves_accumulator(self) -> None:
        def processor() -> IncrementalProcessor:
            return IncrementalProcessor(
                timeframes=("M1",),
                instrument="EUR/USD",
                price_base=PriceBase.MID,
                quote_coverage_mode="continuous_quotes",
                max_quote_gap_seconds=90.0,
            )

        uninterrupted = processor()
        partial = processor()
        for second in (0, 30):
            uninterrupted.process_event(quote(second), evaluate_strategy=False)
            partial.process_event(quote(second), evaluate_strategy=False)

        snapshot = partial.checkpoint()
        bucket = snapshot["aggregators"]["M1"]["bucket"]
        self.assertIn("stats", bucket)
        self.assertNotIn("events", bucket)
        self.assertEqual(bucket["stats"]["event_count"], 2)

        resumed = IncrementalProcessor.from_checkpoint(snapshot)
        uninterrupted.process_event(quote(45), evaluate_strategy=False)
        resumed.process_event(quote(45), evaluate_strategy=False)
        self.assertEqual(resumed.checkpoint(), uninterrupted.checkpoint())

    def test_continuous_legacy_event_bucket_is_rejected_fail_closed(self) -> None:
        processor = IncrementalProcessor(
            timeframes=("M1",),
            instrument="EUR/USD",
            price_base=PriceBase.MID,
            quote_coverage_mode="continuous_quotes",
            max_quote_gap_seconds=90.0,
        )
        processor.process_event(quote(0), evaluate_strategy=False)
        snapshot = processor.checkpoint()
        snapshot["aggregators"]["M1"]["bucket"] = {
            "start": snapshot["aggregators"]["M1"]["bucket"]["start"],
            "end": snapshot["aggregators"]["M1"]["bucket"]["end"],
            "events": [],
        }
        with self.assertRaisesRegex(ValueError, "checkpoint continuo legado"):
            IncrementalProcessor.from_checkpoint(snapshot)


if __name__ == "__main__":
    unittest.main()
