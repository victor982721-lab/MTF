"""Focused contract tests for the pure cTrader MARKET protection projection."""

from __future__ import annotations

import json
import unittest
from dataclasses import FrozenInstanceError
from decimal import Decimal

from mtf_lab.ops.ctrader_protection import (
    MarketProtection,
    MarketProtectionError,
    project_market_protection,
)


class MarketProtectionTests(unittest.TestCase):
    @staticmethod
    def _buy(**overrides: object) -> MarketProtection:
        values: dict[str, object] = {
            "direction": "BUY",
            "entry_price": "1.10000",
            "stop_loss": "1.099935",
            "take_profit": "1.100105",
            "price_quantum": "0.00003",
            "executable_bid": "1.10000",
            "executable_ask": "1.10001",
            "minimum_stop_distance": "0.00001",
        }
        values.update(overrides)
        return project_market_protection(**values)

    @staticmethod
    def _sell(**overrides: object) -> MarketProtection:
        values: dict[str, object] = {
            "direction": "SELL",
            "entry_price": "1.10000",
            "stop_loss": "1.100065",
            "take_profit": "1.099895",
            "price_quantum": "0.00003",
            "executable_bid": "1.09999",
            "executable_ask": "1.10000",
            "minimum_stop_distance": "0.00001",
        }
        values.update(overrides)
        return project_market_protection(**values)

    def test_buy_fractional_distances_floor_toward_entry(self) -> None:
        protection = self._buy()

        self.assertIsInstance(protection, MarketProtection)
        self.assertEqual(protection.direction, "BUY")
        self.assertEqual(protection.planned_stop_distance, Decimal("0.000065"))
        self.assertEqual(protection.planned_take_profit_distance, Decimal("0.000105"))
        self.assertEqual(protection.stop_rounding_delta, Decimal("0.000005"))
        self.assertEqual(protection.take_profit_rounding_delta, Decimal("0.000015"))
        self.assertEqual(protection.to_order_options(), {"relative_stop_loss": 6, "relative_take_profit": 9})
        self.assertEqual(protection.reference_stop_loss, Decimal("1.09994"))
        self.assertEqual(protection.reference_take_profit, Decimal("1.10009"))

    def test_sell_fractional_distances_floor_toward_entry(self) -> None:
        protection = self._sell()

        self.assertEqual(protection.direction, "SELL")
        self.assertEqual(protection.planned_stop_distance, Decimal("0.000065"))
        self.assertEqual(protection.planned_take_profit_distance, Decimal("0.000105"))
        self.assertEqual(protection.to_order_options(), {"relative_stop_loss": 6, "relative_take_profit": 9})
        self.assertEqual(protection.reference_stop_loss, Decimal("1.10006"))
        self.assertEqual(protection.reference_take_profit, Decimal("1.09991"))

    def test_exact_distances_and_alias_directions_are_preserved(self) -> None:
        protection = self._buy(
            direction="UP",
            stop_loss="1.09994",
            take_profit="1.10009",
        )

        self.assertEqual(protection.to_order_options(), {"relative_stop_loss": 6, "relative_take_profit": 9})
        self.assertEqual(protection.stop_rounding_delta, Decimal("0"))
        self.assertEqual(protection.take_profit_rounding_delta, Decimal("0"))
        self.assertEqual(protection.wire_stop_loss, protection.reference_stop_loss)
        self.assertEqual(protection.wire_take_profit, protection.reference_take_profit)

    def test_wire_scaling_uses_one_e_minus_five_price_units(self) -> None:
        protection = self._buy(
            price_quantum="0.00001",
            stop_loss="1.09981",
            take_profit="1.10041",
        )

        self.assertEqual(protection.to_order_options(), {"relative_stop_loss": 19, "relative_take_profit": 41})
        self.assertNotEqual(protection.relative_stop_loss, 1_900_000)
        self.assertEqual(protection.wire_stop_distance, Decimal("0.00019"))

    def test_stop_is_never_widened_away_from_entry(self) -> None:
        buy = self._buy()
        sell = self._sell()

        self.assertGreater(buy.reference_stop_loss, buy.planned_stop_loss)
        self.assertLess(sell.reference_stop_loss, sell.planned_stop_loss)
        self.assertLessEqual(buy.wire_stop_distance, buy.planned_stop_distance)
        self.assertLessEqual(sell.wire_stop_distance, sell.planned_stop_distance)

    def test_to_order_options_excludes_absolute_levels_and_audit_is_json_safe(self) -> None:
        protection = self._buy()
        options = protection.to_order_options()
        audit = protection.to_dict()

        self.assertEqual(set(options), {"relative_stop_loss", "relative_take_profit"})
        self.assertNotIn("stop_loss", options)
        self.assertNotIn("take_profit", options)
        self.assertEqual(audit["rounding_basis"], "TOWARD_ENTRY_NO_RISK_INCREASE")
        self.assertEqual(audit["relative_levels_anchored_to"], "BROKER_ACTUAL_FILL")
        self.assertFalse(audit["reference_levels_observed"])
        self.assertIsInstance(audit["planned_stop_loss"], str)
        json.dumps(audit)

    def test_dataclass_is_frozen(self) -> None:
        protection = self._buy()
        with self.assertRaises(FrozenInstanceError):
            protection.relative_stop_loss = 7  # type: ignore[misc]

    def test_missing_nan_infinity_and_boolean_inputs_reject(self) -> None:
        for field in (
            "entry_price",
            "stop_loss",
            "take_profit",
            "price_quantum",
            "executable_bid",
            "executable_ask",
            "minimum_stop_distance",
        ):
            with self.subTest(field=field, value=None), self.assertRaises(MarketProtectionError):
                self._buy(**{field: None})
            for value in ("NaN", "Infinity", True):
                with self.subTest(field=field, value=value), self.assertRaises(MarketProtectionError):
                    self._buy(**{field: value})

    def test_nonpositive_inputs_reject(self) -> None:
        for field in (
            "entry_price",
            "stop_loss",
            "take_profit",
            "price_quantum",
            "executable_bid",
            "executable_ask",
            "minimum_stop_distance",
        ):
            for value in ("0", "-0.00001"):
                with self.subTest(field=field, value=value), self.assertRaises(MarketProtectionError):
                    self._buy(**{field: value})

    def test_directional_crossing_and_zero_quantized_distance_reject(self) -> None:
        cases = (
            {"direction": "BUY", "stop_loss": "1.10001"},
            {"direction": "BUY", "take_profit": "1.09999"},
            {"direction": "SELL", "stop_loss": "1.09999"},
            {"direction": "SELL", "take_profit": "1.10001"},
            {"direction": "BUY", "stop_loss": "1.09999", "price_quantum": "0.00003"},
        )
        for overrides in cases:
            with self.subTest(overrides=overrides), self.assertRaises(MarketProtectionError):
                (self._sell if overrides["direction"] == "SELL" else self._buy)(**overrides)

    def test_non_multiple_quantum_crossed_pair_overflow_and_minimum_after_floor_reject(self) -> None:
        cases = (
            {"price_quantum": "0.000015"},
            {"executable_bid": "1.10002", "executable_ask": "1.10001"},
            {
                "stop_loss": "1.09996",
                "price_quantum": "0.00003",
                "executable_bid": "1.09998",
                "minimum_stop_distance": "0.00002",
            },
            {
                "entry_price": "1000000000000000",
                "stop_loss": "1",
                "take_profit": "1000000000000001",
                "executable_bid": "1000000000000000",
                "executable_ask": "1000000000000000",
            },
        )
        for overrides in cases:
            with self.subTest(overrides=overrides), self.assertRaises(MarketProtectionError):
                self._buy(**overrides)


if __name__ == "__main__":
    unittest.main()
