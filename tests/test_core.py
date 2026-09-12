"""Pruebas focales del núcleo causal, sin proveedores ni red."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.core import (
    Candle,
    DataQuality,
    IndicatorConfig,
    IndicatorPoint,
    MarketEvent,
    PriceBase,
    StrategyConfig,
    TrendPullbackStrategy,
    aggregate_events,
    compute_indicators,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def make_candle(index: int, *, price: float | None = None, timeframe: str = "M1") -> Candle:
    seconds = {"M1": 60, "M5": 300, "M15": 900}[timeframe]
    start = BASE + timedelta(seconds=index * seconds)
    value = 100.0 + index if price is None else price
    return Candle(
        "TEST", timeframe, start, start + timedelta(seconds=seconds), value, value + 1, value - 1, value, 1, 1
    )


class CoreModelsAndAggregationTests(unittest.TestCase):
    def test_interval_is_half_open_and_price_basis_is_not_silent(self) -> None:
        e0 = MarketEvent("TEST", BASE + timedelta(seconds=299), price=101, quantity=1)
        e1 = MarketEvent("TEST", BASE + timedelta(seconds=300), price=102, quantity=1)
        bars = aggregate_events([e1, e0], "M5")
        self.assertEqual([bar.start for bar in bars], [BASE, BASE + timedelta(minutes=5)])
        self.assertEqual(bars[0].close, 101)
        self.assertEqual(bars[1].open, 102)
        bid_only = MarketEvent("TEST", BASE, price=None, bid=99, ask=101, price_base=PriceBase.BID)
        self.assertEqual(bid_only.selected_price, 99)
        self.assertEqual(aggregate_events([bid_only], "M1", price_base=PriceBase.TRADED), [])

    def test_data_quality_preserves_synthetic_without_calling_it_invalid(self) -> None:
        quality = DataQuality.good(synthetic=True)
        self.assertTrue(quality.valid)
        self.assertTrue(quality.has("synthetic"))
        self.assertFalse(DataQuality.from_flag("invalid").valid)


class IndicatorTests(unittest.TestCase):
    def test_ema_wilder_rsi_and_atr_known_values(self) -> None:
        config = IndicatorConfig(ema_fast=3, ema_slow=4, rsi_period=3, atr_period=3)
        candles = []
        closes = [1, 2, 3, 2, 2]
        highs = [2, 3, 4, 3, 3]
        lows = [0, 1, 2, 1, 1]
        for index, (close, high, low) in enumerate(zip(closes, highs, lows, strict=False)):
            start = BASE + timedelta(minutes=index)
            candles.append(Candle("T", "M1", start, start + timedelta(minutes=1), close, high, low, close, 1, 1))
        series = compute_indicators(candles, config)
        self.assertIsNone(series[1].ema_fast)
        self.assertAlmostEqual(series[2].ema_fast, 2.0)
        self.assertAlmostEqual(series[3].ema_fast, 2.0)
        # RSI: gains [1,1,0], losses [0,0,1], Wilder seed => 66.666...
        self.assertAlmostEqual(series[3].rsi, 66.6666666667, places=8)
        # ATR seed uses the first three true ranges, each equal to 2.
        self.assertAlmostEqual(series[2].atr, 2.0)

    def test_incremental_and_batch_are_the_same_state_machine(self) -> None:
        config = IndicatorConfig(ema_fast=3, ema_slow=5, rsi_period=3, atr_period=3)
        candles = [make_candle(index) for index in range(20)]
        batch = compute_indicators(candles, config)
        from mtf_lab.core.indicators import IncrementalIndicatorEngine

        engine = IncrementalIndicatorEngine(config)
        points = [engine.update(candle) for candle in candles]
        self.assertEqual(len(points), len(batch.points))
        for expected, actual in zip(batch.points, points, strict=False):
            for field in ("ema_fast", "ema_slow", "rsi", "atr"):
                self.assertEqual(getattr(expected, field), getattr(actual, field))

    def test_gap_restarts_warmup_instead_of_bridging_unknown_data(self) -> None:
        config = IndicatorConfig(ema_fast=2, ema_slow=3, rsi_period=2, atr_period=2)
        candles = [make_candle(0), make_candle(1), make_candle(3)]
        series = compute_indicators(candles, config)
        self.assertTrue(series[2].quality.has("gap"))
        self.assertFalse(series[2].ready)


def point(
    start_minute: int,
    *,
    close: float,
    ema_fast: float,
    ema_slow: float,
    rsi: float = 60,
    atr: float = 10,
    available: int | None = None,
) -> IndicatorPoint:
    start = BASE + timedelta(minutes=start_minute)
    end = start + timedelta(minutes=1)
    available_dt = end if available is None else BASE + timedelta(minutes=available)
    return IndicatorPoint(
        start,
        end,
        available_dt,
        close,
        ema_fast,
        ema_slow,
        rsi,
        atr,
        True,
        DataQuality.good(),
        f"p-{start_minute}",
        start_minute,
    )


class StrategyCausalityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = StrategyConfig.from_mapping(
            {
                "context_timeframe": "M15",
                "preparation_timeframe": "M5",
                "trigger_timeframe": "M1",
                "context_lookback": 1,
                "preparation_lookback": 3,
                "preparation_ttl_bars": 2,
                "max_distance_atr": 0.5,
                "rsi_threshold": 50,
                "indicators": {"ema_fast": 2, "ema_slow": 3, "rsi_period": 2, "atr_period": 2},
            }
        )

    def test_m15_close_is_not_available_before_interval_end(self) -> None:
        # El contexto alcista se forma en M15 [15,30), por lo que los M1 de
        # [6,7) no pueden usarlo, aunque el lote contenga la vela futura.
        context = [
            point(0, close=100, ema_fast=99, ema_slow=100, rsi=50, available=15),
            point(15, close=105, ema_fast=105, ema_slow=101, rsi=60, available=30),
        ]
        preparation = [
            point(0, close=105, ema_fast=100, ema_slow=100, available=5),
            point(5, close=105, ema_fast=100, ema_slow=100, available=10),
            point(10, close=105, ema_fast=100, ema_slow=100, available=15),
            point(15, close=105, ema_fast=100, ema_slow=100, available=20),
            point(20, close=105, ema_fast=100, ema_slow=100, available=25),
            point(25, close=101, ema_fast=100, ema_slow=100, available=30),
        ]
        trigger = [
            point(6, close=101, ema_fast=100, ema_slow=100, rsi=60, available=7),
            point(28, close=99, ema_fast=100, ema_slow=100, rsi=40, available=29),
            point(29, close=101, ema_fast=100, ema_slow=100, rsi=60, available=30),
        ]
        result = TrendPullbackStrategy(self.config).evaluate({"M15": context, "M5": preparation, "M1": trigger})
        early = next(
            evaluation
            for evaluation in result.evaluations
            if evaluation.stage == "trigger" and evaluation.timestamp.minute == 7
        )
        self.assertEqual(early.decision.value, "blocked")
        self.assertIn("context_not_available", early.reasons)
        # El punto de 00:30 ya puede usar el cierre de M15 y la preparación.
        self.assertTrue(any(signal.detected_at == BASE + timedelta(minutes=30) for signal in result.signals))

    def test_preparation_expires_and_repeated_signal_is_blocked(self) -> None:
        context = [
            point(0, close=100, ema_fast=99, ema_slow=100, available=15),
            point(15, close=105, ema_fast=105, ema_slow=101, available=30),
            point(30, close=106, ema_fast=106, ema_slow=101, available=45),
        ]
        preparation = [
            point(
                index * 5,
                close=(105 if index < 5 else 101 if index == 5 else 105),
                ema_fast=100,
                ema_slow=100,
                available=(index + 1) * 5,
            )
            for index in range(8)
        ]
        # La preparación [25,30) se registra a 30 y TTL=2 M5, así que 00:40
        # es frontera de expiración. Dos cruces antes de esa frontera deben
        # producir como máximo una señal por episodio.
        trigger = [
            point(28, close=99, ema_fast=100, ema_slow=100, rsi=40, available=29),
            point(29, close=101, ema_fast=100, ema_slow=100, rsi=60, available=30),
            point(34, close=99, ema_fast=100, ema_slow=100, rsi=40, available=35),
            point(35, close=101, ema_fast=100, ema_slow=100, rsi=60, available=36),
            point(40, close=101, ema_fast=100, ema_slow=100, rsi=60, available=40),
        ]
        result = TrendPullbackStrategy(self.config).evaluate({"M15": context, "M5": preparation, "M1": trigger})
        self.assertLessEqual(len(result.signals), 2)
        # Al menos un bloqueo explícito por expiración o por episodio ya usado.
        self.assertTrue(
            any(
                "preparation_expired" in evaluation.reasons or "signal_duplicate" in evaluation.reasons
                for evaluation in result.evaluations
                if evaluation.stage == "trigger"
            )
        )

    def test_unknown_mandatory_condition_never_becomes_partial_signal(self) -> None:
        context = [
            point(0, close=100, ema_fast=99, ema_slow=100, available=15),
            point(15, close=105, ema_fast=105, ema_slow=101, available=30),
        ]
        preparation = [
            point(index * 5, close=105 if index < 5 else 101, ema_fast=100, ema_slow=100, available=(index + 1) * 5)
            for index in range(6)
        ]
        # El cierre/EMA anterior se marca desconocido: el cruce obligatorio no
        # puede ser compensado por RSI/contexto.
        trigger = [
            point(28, close=99, ema_fast=100, ema_slow=100, rsi=40, available=29),
            IndicatorPoint(
                BASE + timedelta(minutes=29),
                BASE + timedelta(minutes=30),
                BASE + timedelta(minutes=30),
                101,
                None,
                100,
                60,
                10,
                True,
                DataQuality.good(),
                "unknown-trigger",
                29,
            ),
        ]
        result = TrendPullbackStrategy(self.config).evaluate({"M15": context, "M5": preparation, "M1": trigger})
        self.assertEqual(result.signals, ())
        self.assertTrue(
            any(
                e.decision.value == "blocked" and "trigger_indicator_not_ready" in e.reasons
                for e in result.evaluations
                if e.stage == "trigger"
            )
        )


if __name__ == "__main__":
    unittest.main()
