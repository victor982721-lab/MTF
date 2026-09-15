"""Focused opt-in RiskExit integration tests for the local CFD state machine."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.core.cfd_simulation import (
    CFDConfig,
    CFDQuote,
    CFDSignal,
    CFDSimulationError,
    CFDSimulator,
    TradeState,
)
from mtf_lab.core.risk_exit import RiskExitPolicy

START = datetime(2026, 1, 1, 12, tzinfo=UTC)


def contract(**overrides):
    value = {
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
    value.update(overrides)
    return value


def calendar(**overrides):
    value = {"known": True, "financing_known": True}
    value.update(overrides)
    return value


def quote(at: datetime, bid: str = "1.1000", ask: str = "1.1002", quote_id: str = "q") -> CFDQuote:
    return CFDQuote("EUR/USD", at, Decimal(bid), Decimal(ask), quote_id)


_DEFAULT = object()


def config(
    *,
    horizons=("600",),
    policy=None,
    spec=_DEFAULT,
    cal=_DEFAULT,
    units="2050",
    risk_trigger_timeframe="M1",
    **kwargs,
):
    return CFDConfig(
        units=Decimal(units),
        horizons_seconds=tuple(Decimal(item) for item in horizons),
        risk_exit_policy=policy or RiskExitPolicy(),
        risk_exit_contract_spec=contract(server_side_stops=True) if spec is _DEFAULT else spec,
        risk_exit_calendar=calendar() if cal is _DEFAULT else cal,
        risk_trigger_timeframe=risk_trigger_timeframe,
        **kwargs,
    )


class CFDRiskExitTests(unittest.TestCase):
    def test_fill_uses_floor_grid_and_fixed_levels_then_server_gap_closes(self) -> None:
        sim = CFDSimulator(config())
        signal = CFDSignal("risk-long", "EUR/USD", "LONG", START, metadata={"atr": "0.001"})
        sim.submit_all(signal)
        sim.on_quote(quote(START, quote_id="entry"))
        filled = sim.trades[0]
        self.assertEqual(filled.state, TradeState.FILLED)
        self.assertEqual(filled.units, Decimal("2000"))
        self.assertEqual(filled.risk_exit_plan["initial_stop"], "1.09870")
        self.assertEqual(filled.risk_exit_plan["take_profit"], "1.10320")

        # The bid is the executable side for a long close.  The close records
        # the observed gap price, not the planned stop level.
        sim.on_quote(quote(START + timedelta(minutes=1), bid="1.0970", ask="1.0972", quote_id="gap"))
        closed = sim.trades[0]
        self.assertEqual(closed.state, TradeState.CLOSED)
        self.assertEqual(closed.close_price, Decimal("1.09700"))
        self.assertEqual(closed.risk_exit_decision["action"], "STOP_LOSS")
        self.assertEqual(closed.risk_exit_decision["executable_price"], "1.0970")
        self.assertEqual(closed.risk_exit_decision["latency_seconds"], "0")
        self.assertTrue(closed.risk_exit_decision["gap"])
        self.assertEqual(closed.risk_mfe_price, Decimal("0"))
        self.assertGreater(closed.risk_mae_price, Decimal("0"))

    def test_exact_trigger_is_tick_not_gap(self) -> None:
        sim = CFDSimulator(config())
        sim.submit(CFDSignal("exact-stop", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        sim.on_quote(quote(START + timedelta(minutes=1), bid="1.0987", ask="1.0989", quote_id="tick"))
        decision = sim.trades[0].risk_exit_decision
        self.assertEqual(decision["reason"], "STOP_LOSS_TICK")
        self.assertFalse(decision["gap"])

    def test_partial_new_executable_bid_closes_long_without_false_cross(self) -> None:
        sim = CFDSimulator(config())
        sim.submit(CFDSignal("partial-stop", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="full"))
        partial = CFDQuote(
            "EUR/USD",
            START + timedelta(minutes=1),
            Decimal("1.0970"),
            None,
            "bid-only",
            updated_sides=("bid",),
        )
        sim.on_quote(partial)
        trade = sim.trades[0]
        self.assertEqual(trade.state, TradeState.CLOSED)
        self.assertEqual(trade.close_price, Decimal("1.09700"))
        self.assertNotIn(
            "CROSSED", " ".join(str(event) for event in sim.events if event.get("event") == "quote_blocked")
        )

    def test_risk_entry_rejects_mixed_timestamp_bbo_until_coherent_pair_arrives(self) -> None:
        sim = CFDSimulator(config(max_quote_age_seconds="5"))
        sim.submit(CFDSignal("coherent-entry", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        stale_bid = CFDQuote(
            "EUR/USD",
            START,
            Decimal("1.1000"),
            Decimal("1.1002"),
            "mixed",
            bid_source_timestamp=START - timedelta(seconds=10),
            ask_source_timestamp=START,
            bid_timestamp_known=True,
            ask_timestamp_known=True,
            updated_sides=("bid", "ask"),
        )
        sim.on_quote(stale_bid)
        self.assertEqual(sim.trades[0].state, TradeState.PENDING)
        fresh = quote(START + timedelta(seconds=1), quote_id="coherent")
        sim.on_quote(fresh)
        self.assertEqual(sim.trades[0].state, TradeState.FILLED)

    def test_fresh_retained_opposite_leg_can_complete_risk_bbo(self) -> None:
        sim = CFDSimulator(config(max_quote_age_seconds="5"))
        sim.on_quote(quote(START, quote_id="baseline"))
        sim.submit(
            CFDSignal("retained-bbo", "EUR/USD", "LONG", START + timedelta(seconds=1), metadata={"atr": "0.001"})
        )
        partial = CFDQuote(
            "EUR/USD",
            START + timedelta(seconds=1),
            Decimal("1.1001"),
            None,
            "new-bid",
            updated_sides=("bid",),
        )
        sim.on_quote(partial)
        self.assertEqual(sim.trades[0].state, TradeState.FILLED)
        self.assertEqual(sim.trades[0].entry_price, Decimal("1.10020"))

    def test_unknown_spec_calendar_and_costs_reject_at_fill(self) -> None:
        cases = (
            (None, calendar(), {}, "CONTRACT_SPEC_UNKNOWN"),
            (contract(), None, {}, "CALENDAR_UNKNOWN"),
            (contract(fees_known=False), calendar(), {}, "FEES_UNKNOWN"),
            (contract(), calendar(), {"commission_known": False}, "COSTS_UNKNOWN"),
        )
        for spec, cal, kwargs, reason in cases:
            with self.subTest(reason=reason):
                sim = CFDSimulator(config(spec=spec, cal=cal, **kwargs))
                sim.submit(CFDSignal("blocked-" + reason, "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
                sim.on_quote(quote(START, quote_id="entry-" + reason))
                self.assertEqual(sim.trades[0].state, TradeState.REJECTED)
                self.assertIn(reason, sim.trades[0].reason or "")

    def test_missing_stop_cost_estimates_block_demo_but_diagnostic_stays_noneligible(self) -> None:
        missing = contract()
        for key in (
            "expected_commission_fixed",
            "expected_commission_per_unit",
            "expected_exit_slippage_pips",
            "expected_cost_currency",
            "expected_cost_source",
        ):
            missing.pop(key)
        demo = CFDSimulator(config(spec=missing))
        demo.submit(CFDSignal("missing-envelope", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        demo.on_quote(quote(START, quote_id="entry"))
        self.assertEqual(demo.trades[0].state, TradeState.REJECTED)
        self.assertIn("RISK_ENVELOPE_UNKNOWN", demo.trades[0].reason or "")

        diagnostic = CFDSimulator(config(spec=missing, risk_exit_mode="VIRTUAL_DIAGNOSTIC"))
        diagnostic.submit(CFDSignal("diagnostic-envelope", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        diagnostic.on_quote(quote(START, quote_id="entry"))
        filled = diagnostic.trades[0]
        self.assertEqual(filled.state, TradeState.FILLED)
        self.assertFalse(filled.risk_exit_plan["risk_envelope_known"])
        self.assertIsNone(filled.risk_exit_plan["expected_total_loss_at_stop"])

    def test_cfd_config_nonzero_costs_bind_explicitly_and_keep_envelope_under_budget(self) -> None:
        missing = contract(margin_per_unit="0.1")
        for key in (
            "expected_commission_fixed",
            "expected_commission_per_unit",
            "expected_exit_slippage_pips",
            "expected_cost_source",
        ):
            missing.pop(key)
        sim = CFDSimulator(
            config(
                spec=missing,
                units="100000",
                commission_fixed="1",
                commission_per_unit="0.0001",
                slippage_pips="2",
            )
        )
        sim.submit(CFDSignal("costed-envelope", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        filled = sim.trades[0]
        self.assertEqual(filled.state, TradeState.FILLED)
        plan = filled.risk_exit_plan
        self.assertTrue(plan["risk_envelope_known"])
        self.assertEqual(plan["expected_cost_fixed"], "1")
        self.assertEqual(plan["expected_cost_per_unit"], "0.0001")
        self.assertEqual(plan["expected_exit_slippage_per_unit"], "0.0002")
        self.assertLessEqual(Decimal(plan["expected_total_loss_at_stop"]), Decimal(plan["risk_budget"]))

    def test_stop_inside_high_spread_is_rejected_on_executable_side(self) -> None:
        wide = contract(executable_bid="1.1000", executable_ask="1.1052")
        sim = CFDSimulator(config(spec=wide, units="2050"))
        sim.submit(CFDSignal("wide-stop", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(CFDQuote("EUR/USD", START, Decimal("1.1000"), Decimal("1.1050"), "wide"))
        self.assertEqual(sim.trades[0].state, TradeState.REJECTED)
        self.assertIn("STOP_EXECUTABLE_SIDE_INVALID", sim.trades[0].reason or "")

    def test_one_position_and_intent_are_enforced_across_horizons(self) -> None:
        sim = CFDSimulator(config(horizons=("60", "120")))
        sim.submit_all(CFDSignal("one-intent", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        states = [trade.state for trade in sim.trades]
        self.assertEqual(states.count(TradeState.FILLED), 1)
        self.assertEqual(states.count(TradeState.REJECTED), 1)
        reasons = " ".join((trade.reason or "") for trade in sim.trades)
        self.assertTrue("MAX_POSITIONS" in reasons or "MAX_INTENTS" in reasons)

    def test_second_signal_is_rejected_before_creating_a_pending_intent(self) -> None:
        sim = CFDSimulator(config(horizons=("600",)))
        first = CFDSignal("first-intent", "EUR/USD", "LONG", START, metadata={"atr": "0.001"})
        second = CFDSignal("second-intent", "EUR/USD", "LONG", START, metadata={"atr": "0.001"})
        sim.submit_all(first)
        with self.assertRaisesRegex(CFDSimulationError, "una sola intención"):
            sim.submit_all(second)
        self.assertEqual([trade.signal_id for trade in sim.trades], ["first-intent"])

    def test_time_exit_waits_for_declared_latency_and_never_uses_legacy_horizon(self) -> None:
        sim = CFDSimulator(config(policy=RiskExitPolicy(exit_latency_seconds="2")))
        sim.submit(CFDSignal("time-exit", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="q0"))
        for index in range(1, 6):
            sim.on_quote(quote(START + timedelta(minutes=index), quote_id=f"q{index}"))
        pending = sim.trades[0]
        self.assertEqual(pending.state, TradeState.FILLED)
        self.assertEqual(pending.risk_exit_decision["action"], "TIME_EXIT")
        self.assertEqual(pending.risk_exit_decision["latency_seconds"], "2")
        self.assertIsNotNone(pending.risk_exit_due_at)
        sim.on_quote(quote(START + timedelta(minutes=6), quote_id="q6"))
        self.assertEqual(sim.trades[0].state, TradeState.CLOSED)
        self.assertEqual(sim.trades[0].reason, "MAX_INTRADAY_BARS")

    def test_pending_client_exit_preserves_trigger_and_actual_fill_evidence(self) -> None:
        sim = CFDSimulator(
            config(
                policy=RiskExitPolicy(exit_latency_seconds="2"),
                spec=contract(server_side_stops=False),
            )
        )
        sim.submit(CFDSignal("pending-stop", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        trigger_time = START + timedelta(seconds=1)
        sim.on_quote(quote(trigger_time, bid="1.0980", ask="1.0982", quote_id="trigger"))
        pending = sim.trades[0]
        self.assertEqual(pending.state, TradeState.FILLED)
        self.assertEqual(pending.risk_exit_triggered_at, trigger_time)
        self.assertEqual(pending.risk_exit_requested_latency, Decimal("2"))
        self.assertEqual(pending.risk_exit_trigger_price, Decimal("1.09870"))
        self.assertEqual(pending.risk_exit_decision["requested_latency_seconds"], "2")
        self.assertEqual(pending.risk_exit_decision["triggered_at"], "2026-01-01T12:00:01.000000Z")
        self.assertEqual(pending.risk_exit_due_at, trigger_time + timedelta(seconds=2))

        fill_time = START + timedelta(seconds=3)
        sim.on_quote(quote(fill_time, bid="1.1000", ask="1.1002", quote_id="fill"))
        closed = sim.trades[0]
        self.assertEqual(closed.state, TradeState.CLOSED)
        self.assertEqual(closed.close_available_at, fill_time)
        self.assertEqual(closed.risk_exit_decision["action"], "STOP_LOSS")
        self.assertEqual(closed.risk_exit_decision["planned_trigger_price"], "1.09870")
        self.assertEqual(closed.risk_exit_decision["requested_latency_seconds"], "2")
        self.assertEqual(closed.risk_exit_decision["filled_at"], "2026-01-01T12:00:03.000000Z")

    def test_bars_use_utc_timeframe_boundaries_not_tick_count(self) -> None:
        sim = CFDSimulator(config())
        sim.submit(CFDSignal("bar-clock", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        for index in range(1, 100):
            sim.on_quote(quote(START + timedelta(microseconds=index * 500_000), quote_id=f"tick-{index}"))
        self.assertEqual(sim.trades[0].state, TradeState.FILLED)
        self.assertEqual(sim.trades[0].risk_bars_held, 0)
        for index in range(1, 6):
            sim.on_quote(quote(START + timedelta(minutes=index), quote_id=f"bar-{index}"))
        self.assertEqual(sim.trades[0].state, TradeState.CLOSED)
        self.assertEqual(sim.trades[0].risk_exit_decision["bars_held"], 5)
        self.assertEqual(sim.trades[0].risk_bar_clock_basis, "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION")

    def test_bar_clock_mismatch_is_unknown_and_does_not_advance_time_exit(self) -> None:
        sim = CFDSimulator(config())
        sim.submit(CFDSignal("bar-regression", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        bad = CFDQuote(
            "EUR/USD",
            START + timedelta(minutes=1),
            Decimal("1.1000"),
            Decimal("1.1002"),
            "bad-clock",
            metadata={"risk_trigger_bar_count": 0},
        )
        sim.on_quote(bad)
        self.assertEqual(sim.trades[0].state, TradeState.FILLED)
        self.assertEqual(sim.trades[0].risk_exit_decision["action"], "UNKNOWN")
        self.assertEqual(sim.trades[0].risk_exit_decision["reason"], "RISK_BAR_CLOCK_UNKNOWN")
        self.assertEqual(sim.trades[0].risk_bars_held, 0)

    def test_virtual_diagnostic_can_close_gross_when_net_costs_are_unknown(self) -> None:
        sim = CFDSimulator(
            config(
                policy=RiskExitPolicy(),
                spec=contract(fees_known=False, spread_known=False, known=False, server_side_stops=True),
                cal=None,
                commission_known=False,
                risk_exit_mode="VIRTUAL_DIAGNOSTIC",
            )
        )
        sim.submit(CFDSignal("diagnostic-paper", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        filled = sim.trades[0]
        self.assertEqual(filled.state, TradeState.FILLED)
        self.assertFalse(filled.risk_exit_plan["allowed"])
        self.assertFalse(filled.risk_exit_plan["eligible_for_demo"])
        self.assertEqual(filled.risk_exit_plan["mode"], "VIRTUAL_DIAGNOSTIC")
        sim.on_quote(quote(START + timedelta(minutes=1), bid="1.0980", ask="1.0982", quote_id="stop"))
        closed = sim.trades[0]
        self.assertEqual(closed.state, TradeState.CLOSED)
        self.assertIsNone(closed.net_pnl)
        self.assertIsNotNone(closed.gross_pnl_quote)
        self.assertIn("COMMISSION_UNKNOWN", closed.reason or "")
        self.assertIsNone(sim.risk_state["realized_pnl"])
        self.assertNotEqual(sim.risk_state["realized_gross_pnl"], "0")

    def test_economic_r_uses_settled_net_not_favorable_excursion_for_long_and_short(self) -> None:
        cases = (
            ("LONG", {"bid": "1.10095", "ask": "1.10115"}, {"bid": "1.09870", "ask": "1.09890"}),
            ("SHORT", {"bid": "1.09905", "ask": "1.09925"}, {"bid": "1.10130", "ask": "1.10150"}),
        )
        for direction, favorable, stop in cases:
            with self.subTest(direction=direction):
                sim = CFDSimulator(config())
                sim.submit(CFDSignal(f"r-{direction}", "EUR/USD", direction, START, metadata={"atr": "0.001"}))
                sim.on_quote(quote(START, quote_id=f"entry-{direction}"))
                filled = sim.trades[0]
                self.assertEqual(filled.initial_risk, Decimal("3.0000"))
                sim.on_quote(quote(START + timedelta(minutes=1), quote_id=f"favorable-{direction}", **favorable))
                favorable_trade = sim.trades[0]
                self.assertEqual(favorable_trade.mfe_r, Decimal("0.5"))
                self.assertIsNone(favorable_trade.net_r)
                self.assertIsNone(favorable_trade.r_multiple)
                sim.on_quote(quote(START + timedelta(minutes=2), quote_id=f"stop-{direction}", **stop))
                closed = sim.trades[0]
                self.assertEqual(closed.net_pnl, Decimal("-3.00000"))
                self.assertEqual(closed.net_r, Decimal("-1"))
                self.assertEqual(closed.r_multiple, Decimal("-1"))
                self.assertEqual(closed.mfe_r, Decimal("0.5"))
                self.assertEqual(closed.gross_r, Decimal("-1"))
                self.assertEqual(Decimal(closed.to_dict()["r_multiple"]), Decimal("-1"))

    def test_unknown_net_r_is_none_but_diagnostic_gross_r_is_negative(self) -> None:
        sim = CFDSimulator(
            config(
                spec=contract(fees_known=False, spread_known=False, known=False, server_side_stops=True),
                cal=None,
                commission_known=False,
                risk_exit_mode="VIRTUAL_DIAGNOSTIC",
            )
        )
        sim.submit(CFDSignal("unknown-r", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        sim.on_quote(quote(START + timedelta(minutes=1), bid="1.0980", ask="1.0982", quote_id="stop"))
        closed = sim.trades[0]
        self.assertIsNone(closed.net_pnl)
        self.assertIsNone(closed.net_r)
        self.assertIsNone(closed.r_multiple)
        self.assertLess(closed.gross_r or Decimal("0"), Decimal("0"))
        self.assertIsNone(closed.to_dict()["net_r"])
        self.assertIsNone(closed.to_dict()["r_multiple"])

    def test_zero_known_net_r_is_zero_not_unknown(self) -> None:
        sim = CFDSimulator(config(policy=RiskExitPolicy(intraday_max_bars=1)))
        sim.submit(CFDSignal("zero-r", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        sim.on_quote(quote(START + timedelta(minutes=1), bid="1.1002", ask="1.1004", quote_id="flat"))
        closed = sim.trades[0]
        self.assertEqual(closed.net_pnl, Decimal("0.00000"))
        self.assertEqual(closed.net_r, Decimal("0"))
        self.assertEqual(closed.r_multiple, Decimal("0"))

    def test_snapshot_restores_frozen_r_denominator_and_decimal_metrics(self) -> None:
        sim = CFDSimulator(config())
        sim.submit(CFDSignal("decimal-r", "EUR/USD", "LONG", START, metadata={"atr": "0.001"}))
        sim.on_quote(quote(START, quote_id="entry"))
        sim.on_quote(quote(START + timedelta(minutes=1), bid="1.10095", ask="1.10115", quote_id="favorable"))
        snapshot = sim.snapshot()
        restored = CFDSimulator.from_snapshot(snapshot)
        original_trade = sim.trades[0]
        restored_trade = restored.trades[0]
        self.assertEqual(original_trade.initial_risk, Decimal("3.0000"))
        self.assertEqual(restored_trade.initial_risk, Decimal("3.0000"))
        self.assertEqual(restored_trade.mfe_r, Decimal("0.5"))
        self.assertIsNone(restored_trade.r_multiple)
        final = quote(START + timedelta(minutes=2), bid="1.09870", ask="1.09890", quote_id="stop")
        sim.on_quote(final)
        restored.on_quote(final)
        self.assertEqual(sim.trades[0].to_dict(), restored.trades[0].to_dict())

    def test_snapshot_restore_preserves_risk_state_and_future_result(self) -> None:
        config_value = config()
        signal = CFDSignal("resume-risk", "EUR/USD", "LONG", START, metadata={"atr": "0.001"})
        first = quote(START, quote_id="q0")
        second = quote(START + timedelta(minutes=1), bid="1.0995", ask="1.0997", quote_id="q1")
        final = quote(START + timedelta(minutes=2), bid="1.0970", ask="1.0972", quote_id="q2")
        original = CFDSimulator(config_value)
        original.submit(signal)
        original.on_quote(first)
        original.on_quote(second)
        restored = CFDSimulator.from_snapshot(original.snapshot())
        original.on_quote(final)
        restored.on_quote(final)
        self.assertEqual([item.to_dict() for item in original.trades], [item.to_dict() for item in restored.trades])
        self.assertEqual(original.risk_state, restored.risk_state)


if __name__ == "__main__":
    unittest.main()
