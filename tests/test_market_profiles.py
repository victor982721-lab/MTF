"""Pruebas del registro único de perfiles de mercado."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.core.market_profiles import CANDIDATE_IDS, MARKET_PROFILES, MarketProfile, market_profile
from mtf_lab.core.models import Candle
from mtf_lab.core.risk_exit import RiskExitPolicy
from mtf_lab.core.strategy_extensions import Donchian20Config, Donchian20Strategy


class MarketProfileTests(unittest.TestCase):
    def test_registry_has_the_six_frozen_identities(self) -> None:
        self.assertEqual(tuple(item.candidate_id for item in MARKET_PROFILES), CANDIDATE_IDS)
        self.assertEqual(
            tuple(item.timeframes for item in MARKET_PROFILES),
            (
                ("M15", "M5", "M1"),
                ("H4", "H1", "M15"),
                ("D1", "H4", "H1"),
                ("M5",),
                ("M15",),
                ("H1",),
            ),
        )
        self.assertEqual(
            tuple(item.holding_profile for item in MARKET_PROFILES),
            ("INTRADAY", "INTRADAY", "MULTIDAY", "INTRADAY", "INTRADAY", "MULTIDAY"),
        )

    def test_policy_projection_keeps_preclose_margin_separate_from_bar_limit(self) -> None:
        base = RiskExitPolicy(intraday_max_minutes=Decimal("37"))
        intraday = market_profile("tp_fast_v1").policy_for(base)
        multiday = market_profile("tp_multiday_v1").policy_for(base)
        self.assertEqual(intraday.holding_profile, "INTRADAY")
        self.assertEqual(intraday.intraday_max_bars, 5)
        self.assertEqual(intraday.intraday_max_minutes, Decimal("37"))
        self.assertEqual(multiday.holding_profile, "MULTIDAY")
        self.assertEqual(multiday.multiday_max_hours, Decimal("72"))
        self.assertEqual(multiday.intraday_max_minutes, Decimal("37"))

    def test_profile_identity_and_shape_are_not_mutable_or_ambiguous(self) -> None:
        profile = market_profile("dc_m15_v1")
        self.assertTrue(profile.is_donchian)
        self.assertEqual(profile.trigger_timeframe, "M15")
        self.assertEqual(profile.to_dict()["candidate_id"], "dc_m15_v1")
        with self.assertRaises(ValueError):
            market_profile("optimizer_best")
        with self.assertRaises(ValueError):
            MarketProfile("dc_m5_v1", "UnknownStrategy", ("M5",), "INTRADAY", "INTRADAY", "M5", "bad")

    def test_donchian_generalization_keeps_the_configured_timeframe(self) -> None:
        strategy = Donchian20Strategy(Donchian20Config(timeframe="M15"))
        self.assertEqual(strategy.warmup_requirements().minimum_bars, {"M15": 20})
        start = datetime(2026, 1, 1, tzinfo=UTC)
        bars = tuple(
            Candle(
                "EUR/USD",
                "M15",
                start + timedelta(minutes=15 * index),
                start + timedelta(minutes=15 * (index + 1)),
                100.0,
                101.0,
                99.0,
                100.0,
                1.0,
                1,
            )
            for index in range(20)
        )
        breakout = Candle(
            "EUR/USD",
            "M15",
            start + timedelta(minutes=300),
            start + timedelta(minutes=315),
            102.0,
            103.0,
            101.0,
            102.0,
            1.0,
            1,
        )
        result = strategy.evaluate_causal({"M15": [*bars, breakout]})
        self.assertEqual(result.signal_count, 1)
        self.assertEqual(result.signals[0].direction, "UP")


if __name__ == "__main__":
    unittest.main()
