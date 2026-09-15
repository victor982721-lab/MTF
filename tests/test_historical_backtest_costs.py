"""Pruebas de la proyección económica explícita del backtest histórico."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.core.cfd_simulation import CFDQuote, CFDSignal, CFDSimulator, Direction, EconomicState, TradeState
from mtf_lab.ops.historical_backtest import (
    HistoricalBacktestConfig,
    _historical_economic_metrics,
    _legacy_cfd_config,
    _profile_state,
    _risk_cfd_config,
)
from mtf_lab.ops.market_protocol import ResearchProtocol

BASE = datetime(2016, 3, 7, 5, tzinfo=UTC)


def explicit_spec(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "known": True,
        "pip_size": "0.0001",
        "quantity_min": "1",
        "quantity_step": "1",
        "quantity_max": "100000",
        "unit_value": "1",
        "minimum_stop_distance": "0.0001",
        "margin_per_unit": "1",
        "fees_known": True,
        "spread_known": True,
        "expected_commission_fixed": "1",
        "expected_commission_per_unit": "0.001",
        "expected_exit_slippage_pips": "0.1",
        "expected_cost_currency": "USD",
        "expected_cost_source": "explicit_test_fixture",
        "financing_required": True,
        "financing_rate_per_second": "0.000001",
    }
    value.update(overrides)
    return value


def known_config(*, scenario: str = "base", **spec_overrides: object) -> HistoricalBacktestConfig:
    return HistoricalBacktestConfig.from_protocol(
        ResearchProtocol.default(),
        scenario=scenario,
        candidates=("tp_fast_v1",),
        contract_spec=explicit_spec(**spec_overrides),
        calendar_state={"known": True, "financing_known": True},
    )


class HistoricalBacktestCostTests(unittest.TestCase):
    def test_missing_or_modelled_costs_remain_unknown(self) -> None:
        for config in (
            HistoricalBacktestConfig.from_protocol(ResearchProtocol.default()),
            HistoricalBacktestConfig.from_protocol(
                ResearchProtocol.default(),
                contract_spec=explicit_spec(expected_commission_per_unit=None),
            ),
        ):
            with self.subTest(contract_spec=dict(config.contract_spec)):
                legacy = _legacy_cfd_config(config, "EUR/USD")
                risk = _risk_cfd_config(config, config.profiles[0], "EUR/USD")
                self.assertFalse(legacy.commission_known)
                self.assertEqual(legacy.commission_fixed, Decimal("0"))
                self.assertEqual(legacy.commission_per_unit, Decimal("0"))
                self.assertTrue(legacy.financing_required)
                if config.contract_spec:
                    self.assertEqual(legacy.financing_rate_per_second, Decimal("0.000001"))
                else:
                    self.assertIsNone(legacy.financing_rate_per_second)
                self.assertFalse(risk.commission_known)
                self.assertEqual(risk.financing_rate_per_second, legacy.financing_rate_per_second)

    def test_explicit_known_spec_reaches_legacy_and_risk_simulators(self) -> None:
        config = known_config()
        legacy = _legacy_cfd_config(config, "EUR/USD")
        risk = _risk_cfd_config(config, config.profiles[0], "EUR/USD")
        self.assertTrue(legacy.commission_known)
        self.assertEqual(legacy.commission_fixed, Decimal("1"))
        self.assertEqual(legacy.commission_per_unit, Decimal("0.001"))
        self.assertTrue(legacy.financing_required)
        self.assertEqual(legacy.financing_rate_per_second, Decimal("0.000001"))
        self.assertTrue(risk.commission_known)
        self.assertEqual(risk.commission_fixed, legacy.commission_fixed)
        self.assertEqual(risk.commission_per_unit, legacy.commission_per_unit)
        self.assertEqual(risk.financing_rate_per_second, legacy.financing_rate_per_second)

    def test_known_explicit_financing_exemption_is_not_inferred(self) -> None:
        config = known_config(financing_required=False)
        projected = _legacy_cfd_config(config, "EUR/USD")
        self.assertTrue(projected.commission_known)
        self.assertFalse(projected.financing_required)
        self.assertIsNone(projected.financing_rate_per_second)

    def test_unknown_spread_does_not_upgrade_economics(self) -> None:
        config = known_config(spread_known=False)
        projected = _legacy_cfd_config(config, "EUR/USD")
        self.assertFalse(projected.commission_known)

    def test_result_metrics_do_not_report_gross_only_for_known_inputs(self) -> None:
        known = known_config(financing_required=False)
        known_state = _profile_state(known, known.profiles[0], "EUR/USD")
        known_metrics = _historical_economic_metrics({known.profiles[0].candidate_id: known_state})
        self.assertTrue(known_metrics["costs_applied"])
        self.assertFalse(known_metrics["gross_only"])
        self.assertEqual(known_metrics["net_status"], "KNOWN")

        unknown = HistoricalBacktestConfig.from_protocol(ResearchProtocol.default(), candidates=("tp_fast_v1",))
        unknown_state = _profile_state(unknown, unknown.profiles[0], "EUR/USD")
        unknown_metrics = _historical_economic_metrics({unknown.profiles[0].candidate_id: unknown_state})
        self.assertFalse(unknown_metrics["costs_applied"])
        self.assertTrue(unknown_metrics["gross_only"])
        self.assertEqual(unknown_metrics["net_status"], "UNKNOWN_COSTS")

    def test_known_costs_are_settled_once_by_legacy_simulator(self) -> None:
        config = known_config()
        simulator = CFDSimulator(_legacy_cfd_config(config, "EUR/USD"))
        simulator.submit(CFDSignal("explicit-cost", "EUR/USD", Direction.LONG, BASE))
        simulator.on_quote(CFDQuote("EUR/USD", BASE, Decimal("1.1000"), Decimal("1.1002"), "before"))
        simulator.on_quote(
            CFDQuote("EUR/USD", BASE + timedelta(seconds=5), Decimal("1.1000"), Decimal("1.1002"), "entry")
        )
        changes = simulator.on_quote(
            CFDQuote("EUR/USD", BASE + timedelta(seconds=65), Decimal("1.1005"), Decimal("1.1007"), "close")
        )
        self.assertEqual(len(changes), 1)
        trade = simulator.trades[0]
        self.assertEqual(trade.state, TradeState.CLOSED)
        self.assertEqual(trade.commission_quote, Decimal("2.000"))
        self.assertIsNotNone(trade.financing_quote)
        self.assertIsNotNone(trade.costs_account)
        self.assertIsNotNone(trade.net_pnl)
        self.assertEqual(trade.economic_result.state, EconomicState.DETERMINED)

    def test_adverse_multiplier_scales_only_explicit_cost_inputs(self) -> None:
        config = known_config(scenario="adverse")
        projected = _legacy_cfd_config(config, "EUR/USD")
        self.assertEqual(projected.commission_fixed, Decimal("1.5"))
        self.assertEqual(projected.commission_per_unit, Decimal("0.0015"))
        self.assertEqual(projected.financing_rate_per_second, Decimal("0.0000015"))
        risk_spec = _risk_cfd_config(config, config.profiles[0], "EUR/USD").risk_exit_contract_spec
        assert risk_spec is not None
        self.assertEqual(risk_spec["expected_commission_fixed"], Decimal("1.5"))
        self.assertEqual(risk_spec["expected_commission_per_unit"], Decimal("0.0015"))
        self.assertEqual(risk_spec["expected_exit_slippage_pips"], Decimal("0.15"))


if __name__ == "__main__":
    unittest.main()
