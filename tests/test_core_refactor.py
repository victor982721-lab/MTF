"""Regresiones de los helpers extraídos del núcleo causal."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from mtf_lab.core import (
    Candle,
    CandleAggregator,
    DataQuality,
    MarketEvent,
    QualityFlag,
    StrategyConfig,
    Timeframe,
    aggregate_events,
    validate_candle,
)
from mtf_lab.core.strategy import _quality_from

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def make_event(second: int, *, instrument: str = "TEST/USD", event_id: str | None = None) -> MarketEvent:
    timestamp = BASE + timedelta(seconds=second)
    return MarketEvent(
        instrument,
        timestamp,
        price=100.0 + second,
        received_at=timestamp,
        available_at=timestamp,
        event_id=event_id,
    )


def make_candle(*, closed: bool = True, quality: DataQuality | None = None) -> Candle:
    return Candle(
        "TEST/USD",
        "M1",
        BASE,
        BASE + timedelta(minutes=1),
        100.0,
        101.0,
        99.0,
        100.5,
        volume=2.0,
        event_count=2,
        closed=closed,
        quality=quality or DataQuality.good(),
    )


class AggregationAdmissionTests(unittest.TestCase):
    def test_rejects_identity_errors_without_mutating_the_bucket(self) -> None:
        aggregator = CandleAggregator("M1", instrument="TEST/USD")
        first = make_event(0, event_id="first")
        self.assertTrue(aggregator.add(first).accepted)
        self.assertFalse(aggregator.add(make_event(1, instrument="OTHER/USD")).accepted)
        self.assertFalse(aggregator.add(make_event(2, event_id="first")).accepted)
        self.assertEqual([issue.code for issue in aggregator.issues], ["instrument_mismatch", "duplicate_event"])
        self.assertEqual(aggregator.flush().candles[0].event_count, 1)

    def test_non_rejecting_order_mode_records_evidence_but_keeps_insertable_event(self) -> None:
        aggregator = CandleAggregator("M1", instrument="TEST/USD", reject_out_of_order=False)
        newer = make_event(30, event_id="newer")
        older = make_event(10, event_id="older")
        aggregator.add(newer)
        result = aggregator.add(older)
        self.assertTrue(result.accepted)
        self.assertEqual(result.issues, ())
        self.assertIn("out_of_order", {issue.code for issue in aggregator.issues})
        self.assertEqual(aggregator.flush().candles[0].event_count, 2)

    def test_single_event_batch_keeps_the_short_zip_comparison_safe(self) -> None:
        candles = aggregate_events([make_event(0)], "M1")
        self.assertEqual(len(candles), 1)


class CandleValidationTests(unittest.TestCase):
    def test_candle_normalizes_defaults_and_rejects_measurement_errors(self) -> None:
        candle = make_candle()
        self.assertEqual(candle.available_at, candle.end)
        self.assertTrue(validate_candle(candle).valid)
        with self.assertRaises(ValueError):
            Candle("TEST/USD", "M1", BASE, BASE + timedelta(minutes=1), 100, 99, 98, 100)
        with self.assertRaises(ValueError):
            Candle("TEST/USD", "M1", BASE, BASE + timedelta(minutes=1), 100, 101, 99, 100, volume=-1)

    def test_validate_candle_reports_shape_numeric_and_quality_stages(self) -> None:
        common = {
            "candle_id": "fixture",
            "start": BASE,
            "end": BASE + timedelta(minutes=1),
            "timeframe": Timeframe("M1", 60),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1.0,
            "event_count": 1,
            "closed": True,
            "quality": DataQuality.good(),
        }
        malformed = SimpleNamespace(**{**common, "timeframe": object()})
        self.assertIn("candle_shape_invalid", {issue.code for issue in validate_candle(malformed).issues})
        non_finite = SimpleNamespace(**{**common, "open": float("nan")})
        self.assertIn("non_finite", {issue.code for issue in validate_candle(non_finite).issues})
        incoherent = SimpleNamespace(**{**common, "high": 99.0})
        self.assertIn("ohlc_incoherent", {issue.code for issue in validate_candle(incoherent).issues})
        open_candle = make_candle(closed=False)
        self.assertIn("candle_open", {issue.code for issue in validate_candle(open_candle, require_closed=True).issues})
        blocked = make_candle(quality=DataQuality.from_flag(QualityFlag.STALE))
        self.assertIn("quality_blocked", {issue.code for issue in validate_candle(blocked).issues})


class StrategyConfigRefactorTests(unittest.TestCase):
    def test_nested_and_grouped_configurations_are_normalized(self) -> None:
        config = StrategyConfig.from_mapping(
            {
                "strategy": {"name": "fixture"},
                "timeframes": {"context": "M15", "preparation": "M5", "trigger": "M1"},
                "lookbacks": {"context": 2, "preparation": 1},
                "conditions": {"max_distance_atr": 0.25, "rsi_threshold": 55.0, "preparation_ttl_bars": 2},
            }
        )
        self.assertEqual(config.name, "fixture")
        self.assertEqual(config.context_lookback, 2)
        self.assertEqual(config.preparation_lookback, 1)
        self.assertEqual(config.max_distance_atr, 0.25)
        self.assertEqual(config.rsi_threshold, 55.0)

    def test_sequence_alias_and_ttl_configurations_are_normalized(self) -> None:
        config = StrategyConfig.from_mapping(
            {
                "timeframes": ["M1", "M5", "M15"],
                "lookback": 2,
                "setup_max_atr": 0.75,
                "preparation_ttl_minutes": 10,
                "indicators": {"ema_fast": 2, "ema_slow": 3, "rsi_period": 2, "atr_period": 2},
            }
        )
        self.assertEqual(config.context_lookback, 2)
        self.assertEqual(config.preparation_lookback, 2)
        self.assertEqual(config.preparation_ttl_bars, 2)
        self.assertEqual(config.max_distance_atr, 0.75)
        self.assertEqual(config.indicators.ema_fast, 2)

    def test_quality_mapping_preserves_unknown_flags_as_invalid(self) -> None:
        quality = _quality_from({"flags": ["synthetic", "not-a-flag"], "status": "stale", "reasons": ["delayed"]})
        self.assertTrue(quality.has(QualityFlag.SYNTHETIC))
        self.assertTrue(quality.has(QualityFlag.INVALID))
        self.assertTrue(quality.has(QualityFlag.STALE))
        self.assertEqual(quality.reasons, ("delayed",))


if __name__ == "__main__":
    unittest.main()
