"""Contratos estáticos/runtime de las interfaces normalizadas del core."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from typing import assert_type

from mtf_lab.core import (
    Candle,
    DataQuality,
    IndicatorConfig,
    IndicatorPoint,
    IndicatorSeries,
    QualityFlag,
    StrategyConfig,
    Timeframe,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


class DataQualityStaticContractTests(unittest.TestCase):
    def test_valid_is_a_factory_on_class_and_bool_on_instance(self) -> None:
        quality = DataQuality.valid(synthetic=True)
        assert_type(quality, DataQuality)
        self.assertIsInstance(quality, DataQuality)
        self.assertTrue(quality.has(QualityFlag.SYNTHETIC))
        valid = DataQuality.good().valid
        assert_type(valid, bool)
        self.assertTrue(valid)
        self.assertFalse(DataQuality.from_flag(QualityFlag.INVALID).valid)


class NormalizedTimeframeContractTests(unittest.TestCase):
    def test_candle_accepts_string_but_exposes_normalized_timeframe(self) -> None:
        candle = Candle(
            "TEST/USD",
            "M1",
            BASE,
            BASE + timedelta(minutes=1),
            100.0,
            101.0,
            99.0,
            100.0,
        )
        self.assertIsInstance(candle.timeframe, Timeframe)
        self.assertIs(candle.normalized_timeframe, candle.timeframe)
        self.assertEqual(candle.timeframe_name, "M1")

    def test_strategy_config_normalizes_all_timeframe_fields(self) -> None:
        config = StrategyConfig(context_timeframe="M15", preparation_timeframe="M5", trigger_timeframe="M1")
        self.assertIsInstance(config.context_timeframe, Timeframe)
        self.assertIsInstance(config.preparation_timeframe, Timeframe)
        self.assertIsInstance(config.trigger_timeframe, Timeframe)
        self.assertEqual(config.preparation_ttl, timedelta(minutes=15))


class IndicatorStaticContractTests(unittest.TestCase):
    def test_series_iteration_and_slicing_are_typed_runtime_operations(self) -> None:
        point = IndicatorPoint(
            BASE,
            BASE + timedelta(minutes=1),
            BASE + timedelta(minutes=1),
            100.0,
            99.0,
            98.0,
            55.0,
            1.0,
        )
        series = IndicatorSeries(Timeframe("M1", 60), "TEST/USD", IndicatorConfig(), (point,))
        self.assertEqual(tuple(series), (point,))
        self.assertEqual(series[0], point)
        self.assertEqual(series[:1], (point,))
        self.assertEqual(point.value("ema20"), point.ema_fast)


if __name__ == "__main__":
    unittest.main()
