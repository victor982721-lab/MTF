"""Pure RiskExitPolicy contracts and safety regressions."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.core.risk_exit import (
    EXIT_STOP_LOSS,
    EXIT_TAKE_PROFIT,
    EXIT_TIME,
    EXIT_UNKNOWN,
    RiskExitPolicy,
    evaluate_exit,
    plan_entry,
    serialize,
)

NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


def spec(**overrides):
    result = {
        "known": True,
        "pip_size": "0.0001",
        "quantity_min": "100",
        "quantity_step": "100",
        "quantity_max": "100000",
        "unit_value": "1",
        "minimum_stop_distance": "0.0001",
        "margin_per_unit": "1",
        "fees_known": True,
        "spread_known": True,
        "expected_commission_fixed": "0",
        "expected_commission_per_unit": "0",
        "expected_exit_slippage_pips": "0",
        "expected_cost_currency": "USD",
        "expected_cost_source": "explicit_test_fixture",
        "executable_bid": "1.1000",
        "executable_ask": "1.1002",
    }
    result.update(overrides)
    return result


def risk(**overrides):
    result = {
        "daily_pnl": "0",
        "daily_anchor_equity": "10000",
        "high_water_equity": "10000",
        "drawdown": "0",
        "positions": 0,
        "intents": 0,
        "costs_known": True,
        "equity_source": "VIRTUAL_PAPER_ONLY",
        "equity": "10000",
        "margin_available": "10000",
        "margin_required": "1",
        "bar_clock_known": True,
    }
    result.update(overrides)
    return result


def calendar(**overrides):
    result = {"known": True, "financing_known": True}
    result.update(overrides)
    return result


class RiskExitTests(unittest.TestCase):
    def test_policy_defaults_and_serialization_are_explicit(self) -> None:
        policy = RiskExitPolicy()
        self.assertEqual(policy.stop_atr_multiple, Decimal("1.5"))
        self.assertEqual(policy.take_profit_atr_multiple, Decimal("3"))
        self.assertEqual(policy.planned_risk_fraction, Decimal("0.0025"))
        self.assertEqual(policy.max_daily_loss_fraction, Decimal("0.01"))
        self.assertEqual(policy.max_drawdown_fraction, Decimal("0.05"))
        self.assertEqual(policy.initial_equity, Decimal("10000"))
        encoded = serialize(policy)
        self.assertEqual(encoded["policy_hash"], policy.policy_hash)
        self.assertEqual(RiskExitPolicy.from_mapping(policy.to_dict()), policy)

    def test_plan_entry_uses_floor_grid_without_increasing_quantity(self) -> None:
        policy = RiskExitPolicy()
        plan = plan_entry(
            policy,
            direction="LONG",
            entry_price="1.10000",
            atr="0.00100",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(quantity_min="100", quantity_step="100"),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        self.assertTrue(plan.allowed)
        self.assertEqual(plan.quantity, Decimal("16600"))
        self.assertEqual(plan.initial_stop, Decimal("1.09850"))
        self.assertEqual(plan.take_profit, Decimal("1.10300"))
        self.assertEqual(plan.risk_amount, Decimal("24.90000"))
        self.assertEqual(plan.risk_budget, Decimal("25.00000"))
        self.assertEqual(plan.assumptions["actual_risk_amount"], "24.900000")
        self.assertTrue(plan.assumptions["initial_r_is_immutable"])

    def test_plan_sizes_total_stop_envelope_with_fixed_variable_and_exit_slippage_costs(self) -> None:
        plan = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.10000",
            atr="0.00100",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(
                expected_commission_fixed="1",
                expected_commission_per_unit="0.0001",
                expected_exit_slippage_pips="2",
                expected_cost_source="explicit_test_fixture",
            ),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        self.assertTrue(plan.allowed)
        self.assertTrue(plan.risk_envelope_known)
        self.assertEqual(plan.quantity, Decimal("13300"))
        self.assertEqual(plan.risk_amount, Decimal("19.95000"))
        self.assertEqual(plan.risk_budget, Decimal("25.00000"))
        self.assertEqual(plan.expected_total_loss_at_stop, Decimal("24.94000"))
        self.assertLessEqual(plan.expected_total_loss_at_stop or Decimal("999"), plan.risk_budget or Decimal("0"))
        self.assertLessEqual(plan.risk_amount or Decimal("999"), Decimal("10000") * Decimal("0.0025"))
        self.assertEqual(plan.expected_cost_fixed, Decimal("1"))
        self.assertEqual(plan.expected_cost_per_unit, Decimal("0.0001"))
        self.assertEqual(plan.expected_exit_slippage_per_unit, Decimal("0.0002"))
        self.assertEqual(plan.assumptions["initial_risk_basis"], "filled_quantity * stop_distance * unit_value")
        self.assertEqual(plan.assumptions["risk_budget_currency"], "USD")

    def test_missing_cost_estimate_blocks_demo_but_diagnostic_is_price_risk_only(self) -> None:
        incomplete = spec()
        for key in (
            "expected_commission_fixed",
            "expected_commission_per_unit",
            "expected_exit_slippage_pips",
            "expected_cost_currency",
            "expected_cost_source",
        ):
            incomplete.pop(key)
        kwargs = dict(
            direction="LONG",
            entry_price="1.10000",
            atr="0.00100",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=incomplete,
            calendar_state=calendar(),
            risk_state=risk(),
        )
        blocked = plan_entry(RiskExitPolicy(), **kwargs)
        self.assertFalse(blocked.allowed)
        self.assertIn("RISK_ENVELOPE_UNKNOWN", blocked.reasons)
        diagnostic = plan_entry(RiskExitPolicy(), mode="VIRTUAL_DIAGNOSTIC", **kwargs)
        self.assertFalse(diagnostic.allowed)
        self.assertFalse(diagnostic.risk_envelope_known)
        self.assertIsNone(diagnostic.expected_total_loss_at_stop)
        self.assertEqual(diagnostic.quantity, Decimal("16600"))

    def test_stop_must_be_below_long_bid_or_above_short_ask(self) -> None:
        long_common = dict(
            entry_price="1.10500",
            atr="0.00100",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(expected_exit_slippage_per_unit="0", executable_bid="1.1000", executable_ask="1.1052"),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        short_common = {**long_common, "entry_price": "1.10000"}
        long_plan = plan_entry(
            RiskExitPolicy(), direction="LONG", **long_common, executable_bid="1.1000", executable_ask="1.1052"
        )
        short_plan = plan_entry(
            RiskExitPolicy(), direction="SHORT", **short_common, executable_bid="1.1000", executable_ask="1.1052"
        )
        self.assertFalse(long_plan.allowed)
        self.assertFalse(short_plan.allowed)
        self.assertIn("STOP_EXECUTABLE_SIDE_INVALID", long_plan.reasons)
        self.assertIn("STOP_EXECUTABLE_SIDE_INVALID", short_plan.reasons)

    def test_minimum_stop_distance_uses_executable_side_with_boundary_and_unknown(self) -> None:
        common = dict(
            direction="LONG",
            entry_price="1.10020",
            atr="0.00010",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            calendar_state=calendar(),
            risk_state=risk(),
        )
        inside = plan_entry(
            RiskExitPolicy(),
            **common,
            contract_spec=spec(
                minimum_stop_distance="0.0001",
                margin_per_unit="0.01",
                executable_bid="1.10010",
                executable_ask="1.10020",
            ),
            executable_bid="1.10010",
            executable_ask="1.10020",
        )
        self.assertFalse(inside.allowed)
        self.assertIn("STOP_DISTANCE_BELOW_MINIMUM_EXECUTABLE", inside.reasons)
        self.assertEqual(Decimal(inside.assumptions["minimum_stop_distance_observed"]), Decimal("0.00005"))
        self.assertEqual(inside.assumptions["minimum_stop_distance_basis"], "executable_bid - initial_stop (LONG)")

        boundary = plan_entry(
            RiskExitPolicy(),
            **common,
            contract_spec=spec(
                minimum_stop_distance="0.0001",
                margin_per_unit="0.01",
                executable_bid="1.10015",
                executable_ask="1.10020",
            ),
            executable_bid="1.10015",
            executable_ask="1.10020",
        )
        self.assertTrue(boundary.allowed)
        self.assertEqual(Decimal(boundary.assumptions["minimum_stop_distance_observed"]), Decimal("0.00010"))

        no_side = spec(minimum_stop_distance="0.0001", margin_per_unit="0.01")
        no_side.pop("executable_bid")
        no_side.pop("executable_ask")
        unknown = plan_entry(RiskExitPolicy(), **common, contract_spec=no_side)
        self.assertFalse(unknown.allowed)
        self.assertIn("STOP_EXECUTABLE_SIDE_UNKNOWN", unknown.reasons)

    def test_unknown_spec_calendar_or_risk_blocks_without_fabricating(self) -> None:
        policy = RiskExitPolicy()
        for kwargs, reason in (
            ({"contract_spec": None, "calendar_state": calendar()}, "CONTRACT_SPEC_UNKNOWN"),
            ({"contract_spec": spec(), "calendar_state": None}, "CALENDAR_UNKNOWN"),
            ({"contract_spec": spec(), "calendar_state": calendar(), "risk_state": None}, "RISK_STATE_UNKNOWN"),
        ):
            plan = plan_entry(
                policy,
                direction="LONG",
                entry_price="1.10000",
                atr="0.00100",
                equity="10000",
                equity_source="VIRTUAL_PAPER_ONLY",
                available_at=NOW,
                **kwargs,
            )
            self.assertFalse(plan.allowed)
            self.assertIn(reason, plan.reasons)
            self.assertIsNone(plan.quantity)

    def test_risk_limits_and_observed_equity_origin_are_fail_closed(self) -> None:
        policy = RiskExitPolicy(equity_basis="OBSERVED_DEMO")
        blocked = plan_entry(
            policy,
            direction="LONG",
            entry_price="1.1",
            atr="0.001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(daily_pnl="-100", equity_source="SERVER_OBSERVED"),
        )
        self.assertFalse(blocked.allowed)
        self.assertIn("EQUITY_PROVENANCE_UNKNOWN", blocked.reasons)

        daily_block = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr="0.001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(daily_pnl="-100"),
        )
        self.assertIn("MAX_DAILY_LOSS", daily_block.reasons)

        # Limits are measured from the persisted daily anchor/high-water
        # values, not from the current equity supplied to this call.
        dd_block = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr="0.001",
            equity="19000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(daily_pnl="0", high_water_equity="20000", drawdown="1000"),
        )
        self.assertIn("MAX_DRAWDOWN", dd_block.reasons)
        inconsistent_dd = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr="0.001",
            equity="19000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(high_water_equity="20000", drawdown="0"),
        )
        self.assertIn("DRAWDOWN_INCONSISTENT", inconsistent_dd.reasons)
        self.assertIn("MAX_DRAWDOWN", inconsistent_dd.reasons)

        missing_anchor = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr="0.001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(daily_anchor_equity=None, high_water_equity=None),
        )
        self.assertIn("DAILY_ANCHOR_UNKNOWN", missing_anchor.reasons)
        self.assertIn("HIGH_WATER_UNKNOWN", missing_anchor.reasons)

    def test_evaluate_exit_consumes_fixed_entry_plan_levels(self) -> None:
        policy = RiskExitPolicy()
        plan = plan_entry(
            policy,
            direction="LONG",
            entry_price="1.10000",
            atr="0.00100",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        stop = evaluate_exit(
            policy,
            plan,
            current_price="1.09840",
            observed_at=NOW + timedelta(minutes=1),
            entry_at=NOW,
            calendar_state=calendar(),
            executable_price="1.09840",
        )
        self.assertEqual(stop.action, EXIT_STOP_LOSS)
        self.assertEqual(stop.executable_price, Decimal("1.09840"))
        self.assertEqual(stop.planned_trigger_price, Decimal("1.09850"))

        target = evaluate_exit(
            policy,
            plan,
            current_price="1.10320",
            observed_at=NOW + timedelta(minutes=1),
            entry_at=NOW,
            calendar_state=calendar(),
            gap=True,
            executable_price="1.10320",
        )
        self.assertEqual(target.action, EXIT_TAKE_PROFIT)
        self.assertEqual(target.reason, "TAKE_PROFIT_GAP")

        # Changing the live ATR cannot move the plan's initial levels because
        # evaluate_exit receives only the immutable EntryPlan.
        self.assertEqual(plan.initial_stop, Decimal("1.09850"))

        excursion = evaluate_exit(
            policy,
            plan,
            current_price="1.10050",
            observed_at=NOW + timedelta(minutes=1),
            entry_at=NOW,
            calendar_state=calendar(),
            favorable_price="1.10050",
            adverse_price="1.09900",
            executable_price="1.10050",
        )
        self.assertEqual(excursion.mfe_r, Decimal("0.3333333333333333333333333333333333"))
        self.assertEqual(excursion.mae_r.quantize(Decimal("0.0000000001")), Decimal("0.6666666667"))
        self.assertEqual(excursion.r_multiple, excursion.mfe_r)

    def test_missing_executable_price_is_unknown_and_time_exit_has_latency(self) -> None:
        policy = RiskExitPolicy(exit_latency_seconds="2")
        plan = plan_entry(
            policy,
            direction="SHORT",
            entry_price="1.10000",
            atr="0.00100",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        unknown = evaluate_exit(
            policy,
            plan,
            current_price=None,
            observed_at=NOW + timedelta(minutes=1),
            entry_at=NOW,
            calendar_state=calendar(),
        )
        self.assertEqual(unknown.action, EXIT_UNKNOWN)
        self.assertEqual(unknown.reason, "NO_EXECUTABLE_PRICE")
        timed = evaluate_exit(
            policy,
            plan,
            current_price="1.09900",
            observed_at=NOW + timedelta(hours=2),
            entry_at=NOW,
            bars_held=5,
            calendar_state=calendar(),
            executable_price="1.09900",
        )
        self.assertEqual(timed.action, EXIT_TIME)
        self.assertEqual(timed.executable_price, Decimal("1.09900"))
        self.assertEqual(timed.latency_seconds, Decimal("2"))

    def test_server_side_stop_has_no_client_latency_but_time_exit_does(self) -> None:
        policy = RiskExitPolicy(exit_latency_seconds="2")
        plan = plan_entry(
            policy,
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        decision = evaluate_exit(
            policy,
            plan,
            current_price="1.098",
            observed_at=NOW,
            entry_at=NOW,
            calendar_state=calendar(),
            server_side_stop=True,
            executable_price="1.098",
        )
        self.assertEqual(decision.action, EXIT_STOP_LOSS)
        self.assertEqual(decision.latency_seconds, Decimal("0"))

    def test_current_price_alone_is_not_assumed_to_be_executable(self) -> None:
        plan = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        decision = evaluate_exit(
            RiskExitPolicy(),
            plan,
            current_price="1.098",
            observed_at=NOW + timedelta(minutes=1),
            entry_at=NOW,
            calendar_state=calendar(),
        )
        self.assertEqual(decision.action, EXIT_UNKNOWN)
        self.assertEqual(decision.reason, "EXECUTABLE_PRICE_REQUIRED")

    def test_multiday_does_not_exit_on_daily_financing_cut(self) -> None:
        policy = RiskExitPolicy(holding_profile="MULTIDAY")
        plan = plan_entry(
            policy,
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(financing_at=NOW + timedelta(minutes=10)),
            risk_state=risk(),
        )
        decision = evaluate_exit(
            policy,
            plan,
            current_price="1.1001",
            executable_price="1.1001",
            observed_at=NOW + timedelta(minutes=1),
            entry_at=NOW,
            calendar_state=calendar(financing_at=NOW + timedelta(minutes=9)),
        )
        self.assertEqual(decision.action, "NONE")

    def test_entry_is_blocked_inside_intraday_pre_cut_and_model_not_upgraded(self) -> None:
        policy = RiskExitPolicy()
        near_cut = plan_entry(
            policy,
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(financing_at=NOW + timedelta(minutes=10)),
            risk_state=risk(),
        )
        self.assertFalse(near_cut.allowed)
        self.assertIn("ENTRY_PRE_FINANCING_AT", near_cut.reasons)
        modeled = plan_entry(
            policy,
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(
                basis="MODELED_WEEKLY_FX_NOT_ACCOUNT_VERIFIED",
                account_verified=True,
            ),
            risk_state=risk(),
        )
        self.assertIn("CALENDAR_UNVERIFIED", modeled.reasons)
        modeled_plan = plan_entry(
            policy,
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(
                basis="MODELED_WEEKLY_FX_NOT_ACCOUNT_VERIFIED",
                account_verified=True,
            ),
            risk_state=risk(),
            mode="VIRTUAL_DIAGNOSTIC",
        )
        modeled_exit = evaluate_exit(
            policy,
            modeled_plan,
            current_price="1.1001",
            executable_price="1.1001",
            observed_at=NOW + timedelta(minutes=1),
            entry_at=NOW,
            calendar_state=calendar(
                basis="MODELED_WEEKLY_FX_NOT_ACCOUNT_VERIFIED",
                account_verified=True,
                financing_at=NOW + timedelta(minutes=10),
            ),
        )
        self.assertEqual(modeled_exit.action, EXIT_TIME)

    def test_virtual_diagnostic_retains_levels_but_is_never_demo_eligible(self) -> None:
        plan = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.10000",
            atr="0.00100",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(fees_known=False, spread_known=False, known=False),
            calendar_state=None,
            risk_state=risk(costs_known=False),
            mode="VIRTUAL_DIAGNOSTIC",
        )
        self.assertFalse(plan.allowed)
        self.assertFalse(plan.eligible_for_demo)
        self.assertEqual(plan.mode, "VIRTUAL_DIAGNOSTIC")
        self.assertEqual(plan.initial_stop, Decimal("1.09850"))
        self.assertIn("FEES_UNKNOWN", plan.reasons)
        self.assertIn("CALENDAR_UNKNOWN", plan.reasons)

        complete = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(),
            mode="VIRTUAL_DIAGNOSTIC",
        )
        self.assertTrue(complete.allowed)
        self.assertFalse(complete.eligible_for_demo)

    def test_equity_source_is_required_even_for_virtual_paper(self) -> None:
        plan = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        self.assertFalse(plan.allowed)
        self.assertIn("EQUITY_PROVENANCE_UNKNOWN", plan.reasons)

    def test_equity_provenance_is_allowlisted_and_state_coherent(self) -> None:
        unknown_source = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="LOCAL_GUESS",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        self.assertIn("EQUITY_PROVENANCE_UNKNOWN", unknown_source.reasons)

        mismatched_state = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(equity_source="OBSERVED_DEMO"),
        )
        self.assertIn("EQUITY_STATE_SOURCE_MISMATCH", mismatched_state.reasons)

    def test_protection_minimum_and_margin_are_fail_closed(self) -> None:
        protection = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(minimum_stop_distance="0.01"),
            calendar_state=calendar(),
            risk_state=risk(),
        )
        self.assertIn("STOP_DISTANCE_BELOW_MINIMUM", protection.reasons)

        margin = plan_entry(
            RiskExitPolicy(),
            direction="LONG",
            entry_price="1.1",
            atr=".001",
            equity="10000",
            equity_source="VIRTUAL_PAPER_ONLY",
            available_at=NOW,
            contract_spec=spec(),
            calendar_state=calendar(),
            risk_state=risk(margin_available="0", margin_required="1"),
        )
        self.assertIn("MARGIN_INSUFFICIENT", margin.reasons)


if __name__ == "__main__":
    unittest.main()
