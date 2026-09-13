"""Pruebas focales de protocolo, features causales y challenger Donchian."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.core.indicators import IndicatorPoint
from mtf_lab.core.models import Candle
from mtf_lab.core.quality import DataQuality
from mtf_lab.core.strategy_extensions import (
    BaselineStrategyAdapter,
    Donchian20M5Strategy,
    QuoteObservation,
    StrategyProtocol,
    VolatilityRegimeThreshold,
    compute_strategy_features,
    session_causal,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def point(index: int, *, atr: float = 2.0, close: float = 100.0, available: int | None = None) -> IndicatorPoint:
    start = BASE + timedelta(minutes=index)
    end = start + timedelta(minutes=1)
    available_at = end if available is None else BASE + timedelta(minutes=available)
    return IndicatorPoint(
        start,
        end,
        available_at,
        close,
        100.0 + index,
        100.0,
        55.0,
        atr,
        True,
        DataQuality.good(),
        f"point-{index}",
        index,
    )


def m5_bar(index: int, *, close: float = 100.0, high: float | None = None, low: float | None = None) -> Candle:
    start = BASE + timedelta(minutes=5 * index)
    high_value = close + 1.0 if high is None else high
    low_value = close - 1.0 if low is None else low
    return Candle(
        "EUR/USD",
        "M5",
        start,
        start + timedelta(minutes=5),
        close,
        high_value,
        low_value,
        close,
        1,
        1,
    )


class StrategyExtensionTests(unittest.TestCase):
    def test_protocol_and_baseline_adapter_keep_existing_strategy(self) -> None:
        baseline = BaselineStrategyAdapter()
        self.assertIsInstance(baseline, StrategyProtocol)
        self.assertEqual(baseline.name, "trend_pullback_v1")
        self.assertGreater(baseline.warmup_requirements().minimum_bars["M1"], 0)
        output = baseline.evaluate_causal({"M1": (), "M5": (), "M15": ()})
        self.assertEqual(output.strategy_name, baseline.name)
        self.assertEqual(output.signal_count, 0)
        checkpoint = baseline.checkpoint().to_dict()
        baseline.restore(checkpoint)
        self.assertTrue(all("order" not in signal.to_dict() for signal in output.signals))

    def test_features_are_causal_and_do_not_infer_spread_from_ohlc(self) -> None:
        points = [point(0, atr=2.0), point(1, atr=4.0)]
        quotes = [
            QuoteObservation(BASE + timedelta(minutes=1, seconds=30), bid=99.0, ask=101.0),
            QuoteObservation(BASE + timedelta(minutes=3), bid=90.0, ask=110.0),
        ]
        features = compute_strategy_features(points, quotes=quotes)
        self.assertAlmostEqual(features[0].atr_relative_price or 0.0, 0.02)
        self.assertAlmostEqual(features[1].ema_slope_atr_normalized or 0.0, 0.25)
        self.assertAlmostEqual(features[1].spread_atr or 0.0, 0.5)
        self.assertIsNone(features[0].spread_atr)
        # A future quote cannot change a prefix result.
        prefix = compute_strategy_features(points[:1], quotes=quotes)
        self.assertEqual(prefix[0], features[0])
        self.assertEqual(features[0].session, "ASIA")
        self.assertEqual(session_causal(BASE.replace(hour=9)), "LONDON")
        self.assertEqual(session_causal(BASE.replace(hour=14)), "NEW_YORK")

    def test_volatility_threshold_is_fitted_on_training_prefix_and_frozen(self) -> None:
        training = [point(0, atr=1.0), point(1, atr=2.0), point(2, atr=3.0)]
        threshold = VolatilityRegimeThreshold.fit(training)
        self.assertEqual(threshold.sample_count, 3)
        self.assertEqual(threshold.classify(0.02), "HIGH")
        full = compute_strategy_features(training + [point(3, atr=20.0)], volatility_threshold=threshold)
        self.assertEqual(full[-1].volatility_regime, "HIGH")
        self.assertEqual(threshold, VolatilityRegimeThreshold.from_training(training))

    def test_donchian_uses_previous_twenty_m5_bars_only(self) -> None:
        bars = [m5_bar(index, close=100.0, high=101.0, low=99.0) for index in range(20)]
        bars.append(m5_bar(20, close=102.0, high=150.0, low=98.0))
        result = Donchian20M5Strategy().evaluate_causal({"M5": bars})
        self.assertEqual(result.signal_count, 1)
        explanation = result.explanations[-1]
        self.assertEqual(explanation.decision, "signal")
        self.assertEqual(explanation.direction, "UP")
        self.assertEqual(explanation.values["channel_upper"], 101.0)
        self.assertEqual(explanation.values["channel_lower"], 99.0)
        self.assertEqual(explanation.values["channel_source"], "previous_closed_bars_only")
        self.assertGreaterEqual(result.signals[0].detected_at, result.signals[0].trigger_end)

    def test_donchian_causal_prefix_and_lookahead_guard(self) -> None:
        prefix_bars = [m5_bar(index, close=100.0, high=101.0, low=99.0) for index in range(21)]
        future_a = m5_bar(21, close=103.0, high=104.0, low=102.0)
        future_b = m5_bar(21, close=103.0, high=10000.0, low=-10000.0)
        strategy = Donchian20M5Strategy()
        prefix = strategy.evaluate_causal({"M5": prefix_bars})
        full_a = strategy.evaluate_causal({"M5": prefix_bars + [future_a]})
        full_b = strategy.evaluate_causal({"M5": prefix_bars + [future_b]})
        self.assertEqual(full_a.explanations[:21], prefix.explanations)
        self.assertEqual(full_b.explanations[:21], prefix.explanations)
        self.assertEqual(full_a.explanations[20], full_b.explanations[20])

    def test_donchian_checkpoint_rejects_config_mismatch(self) -> None:
        strategy = Donchian20M5Strategy()
        checkpoint = strategy.checkpoint().to_dict()
        strategy.restore(checkpoint)
        tampered = dict(checkpoint)
        tampered["config_identity"] = "different"
        with self.assertRaises(ValueError):
            strategy.restore(tampered)


if __name__ == "__main__":
    unittest.main()
