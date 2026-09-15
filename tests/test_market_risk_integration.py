"""Execution bridge regressions for the shared RiskExit contract."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from mtf_lab.core.risk_exit import RiskExitPolicy
from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DemoAccount,
    DemoTransport,
    ExecutionPolicy,
    Quote,
    RiskLimitRejected,
)
from mtf_lab.ops.supervision_demo import _callbacks_for, _observed_risk_contract_spec
from mtf_lab.ops.volume_rules import VolumeGrid

NOW = datetime(2026, 9, 13, 12, tzinfo=UTC)


def _spec(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "known": True,
        "pip_size": "0.0001",
        "quantity_min": "0.01",
        "quantity_step": "0.01",
        "quantity_max": "100000",
        "unit_value": "1",
        "minimum_stop_distance": "0.0001",
        "margin_per_unit": "1",
        "fees_known": True,
        "spread_known": True,
        "expected_cost_fixed": "0",
        "expected_cost_per_unit": "0",
        "expected_exit_slippage_pips": "0",
        "expected_cost_currency": "USD",
        "expected_cost_source": "qa_fixture",
    }
    value.update(overrides)
    return value


def _calendar(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {"known": True, "financing_known": True}
    value.update(overrides)
    return value


class MarketRiskIntegrationTests(unittest.TestCase):
    def _executor(
        self,
        *,
        contract_spec: dict[str, object] | None = None,
        calendar: dict[str, object] | None = None,
        candidate: str | None = "tp_fast_v1",
    ) -> tuple[CTraderDemoExecutor, DemoTransport]:
        account = DemoAccount(
            "demo-account",
            "DEMO",
            "demo://ctrader",
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )
        transport = DemoTransport(account_id=account.account_id, endpoint=account.endpoint, clock=lambda: NOW)
        executor = CTraderDemoExecutor(
            account,
            transport=transport,
            clock=lambda: NOW,
            policy=ExecutionPolicy(
                max_quantity=20_000,
                fixed_quantity=1,
                max_exposure=100_000,
                max_spread=1,
            ),
            risk_exit_policy=RiskExitPolicy(),
            risk_contract_spec=contract_spec,
            risk_calendar=calendar,
            market_candidate_id=candidate,
        )
        executor.update_risk_metrics(
            equity=10_000,
            daily_pnl=0,
            realized_daily_pnl=-1,
            unrealized_daily_pnl=-1,
            drawdown=0,
            high_water_equity=10_000,
            margin_available=10_000,
            margin_required=1,
            daily_anchor_equity=10_000,
            daily_anchor_verified=True,
            daily_loss_state="READY",
            daily_anchor_observed_at=NOW,
            observed_at=NOW,
            connection_generation=None,
        )
        executor.observe_runtime(
            {
                "market_candidate_id": candidate,
                "trigger_timeframe": "M1",
                "trigger_bar_count": int(NOW.timestamp() // 60),
                "risk_bar_clock_basis": "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION",
                "latest_trigger_start": "2026-09-13T11:59:00Z",
                "latest_trigger_end": "2026-09-13T12:00:00Z",
                "latest_trigger_available_at": "2026-09-13T12:00:00Z",
                "atr": "0.001",
                "last_market": NOW,
                "last_available": NOW,
                "data_mode": "LIVE",
            }
        )
        executor.activate()
        return executor, transport

    @staticmethod
    def _quote() -> Quote:
        return Quote("EUR/USD", "1.1000", "1.1002", NOW, NOW, source_identity="fixture-quote")

    def test_shared_policy_sizes_on_floor_grid_and_persists_immutable_levels(self) -> None:
        executor, transport = self._executor(contract_spec=_spec(), calendar=_calendar())
        signal = {
            "signal_id": "signal-1",
            "instrument": "EUR/USD",
            "direction": "UP",
            "atr": "0.00100",
            "market_candidate_id": "tp_fast_v1",
        }
        plan = executor.risk_entry_plan(signal, self._quote())
        assert plan is not None
        self.assertTrue(plan.allowed)
        self.assertTrue(plan.eligible_for_demo)
        self.assertEqual(plan.mode, "DEMO_GATED")
        self.assertEqual(str(plan.quantity), "16666.66")
        self.assertEqual(str(plan.initial_stop), "1.098700")
        self.assertEqual(str(plan.take_profit), "1.10320")

        result = executor.submit_signal(signal, self._quote())
        self.assertEqual(result.state.value, "FILLED")
        self.assertEqual(result.protection_state, "OBSERVED")
        self.assertEqual(result.intent.metadata["risk_exit_plan"]["policy_hash"], plan.policy_hash)
        self.assertTrue(result.intent.metadata["risk_entry_bar_count_frozen"])
        self.assertEqual(result.intent.metadata["risk_entry_bar_count_source"], "RUNTIME_UTC_ORDINAL_AT_FILL")
        position = next(iter(transport.positions_by_id.values()))
        self.assertEqual(position.stop_loss, plan.initial_stop)
        self.assertEqual(position.take_profit, plan.take_profit)

    def test_missing_contract_or_calendar_blocks_before_journaling_or_transport(self) -> None:
        for contract_spec, calendar, reason in (
            (None, _calendar(), "CONTRACT_SPEC_UNKNOWN"),
            (_spec(), None, "CALENDAR_UNKNOWN"),
            (_spec(expected_cost_fixed=None), _calendar(), "RISK_ENVELOPE_UNKNOWN"),
        ):
            with self.subTest(reason=reason):
                executor, transport = self._executor(contract_spec=contract_spec, calendar=calendar)
                signal = {
                    "signal_id": f"blocked-{reason}",
                    "instrument": "EUR/USD",
                    "direction": "UP",
                    "atr": "0.001",
                    "market_candidate_id": "tp_fast_v1",
                }
                plan = executor.risk_entry_plan(signal, self._quote())
                assert plan is not None
                self.assertFalse(plan.allowed)
                self.assertIn(reason, plan.reasons)
                with self.assertRaises(RiskLimitRejected):
                    executor.submit_signal(signal, self._quote())
                self.assertEqual(transport.orders, {})
                self.assertEqual(executor.intent_store.intents, {})

    def test_daily_anchor_is_not_replaced_by_realized_plus_unrealized(self) -> None:
        executor, _transport = self._executor(contract_spec=_spec(), calendar=_calendar())
        status = executor.risk_status(refresh=False)
        self.assertEqual(status["metrics"]["daily_pnl"], "0")
        executor.update_risk_metrics(
            daily_pnl="-7",
            realized_daily_pnl="-1",
            unrealized_daily_pnl="-1",
            daily_anchor_equity="10000",
            daily_anchor_verified=True,
            daily_loss_state="READY",
            daily_anchor_observed_at=NOW,
            observed_at=NOW,
            connection_generation=None,
        )
        self.assertEqual(executor.risk_status(refresh=False)["metrics"]["daily_pnl"], "-7")

    def test_diagnostic_mode_and_candidate_mismatch_never_send(self) -> None:
        executor, transport = self._executor(contract_spec=_spec(), calendar=_calendar())
        diagnostic = {
            "signal_id": "diagnostic",
            "instrument": "EUR/USD",
            "direction": "UP",
            "atr": "0.001",
            "market_candidate_id": "tp_fast_v1",
            "risk_exit_mode": "VIRTUAL_DIAGNOSTIC",
        }
        with self.assertRaises(RiskLimitRejected):
            executor.submit_signal(diagnostic, self._quote())
        with self.assertRaises(RiskLimitRejected):
            executor.submit_signal(
                {**diagnostic, "signal_id": "wrong", "market_candidate_id": "dc_m5_v1"}, self._quote()
            )
        self.assertEqual(transport.orders, {})
        self.assertEqual(executor.intent_store.intents, {})

    def test_manage_callback_uses_utc_ordinal_for_five_bar_time_exit(self) -> None:
        executor, transport = self._executor(contract_spec=_spec(), calendar=_calendar())
        now = [NOW]
        executor.clock = lambda: now[0]
        transport.clock = lambda: now[0]
        signal = {
            "signal_id": "managed",
            "instrument": "EUR/USD",
            "direction": "UP",
            "atr": "0.001",
            "market_candidate_id": "tp_fast_v1",
        }
        executor.submit_signal(signal, self._quote())

        class LocalBook:
            spec = SimpleNamespace(symbol="EUR/USD", symbol_id=11)

            def __init__(self) -> None:
                self.bid = "1.1000"
                self.ask = "1.1002"

            def snapshot_quote_state(self):
                at = now[0]
                return {
                    "symbols": {
                        "11": {
                            "bid": {
                                "price": self.bid,
                                "event_time": at,
                                "available_at": at,
                                "generation": "fixture-generation",
                                "sequence": int(at.timestamp()),
                            },
                            "ask": {
                                "price": self.ask,
                                "event_time": at,
                                "available_at": at,
                                "generation": "fixture-generation",
                                "sequence": int(at.timestamp()),
                            },
                        }
                    }
                }

        provider = LocalBook()
        observation = SimpleNamespace(
            environment="DEMO",
            session_id="fixture-session",
            connection_generation="fixture-generation",
        )
        callbacks = _callbacks_for(executor, provider, observation)
        provider.bid = "1.0980"
        provider.ask = "1.0982"
        now[0] = NOW + timedelta(minutes=1)
        callbacks.observe_runtime(
            {
                "market_candidate_id": "tp_fast_v1",
                "trigger_timeframe": "M1",
                "trigger_bar_count": int(now[0].timestamp() // 60),
                "risk_bar_clock_basis": "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION",
                "latest_trigger_start": now[0] - timedelta(minutes=1),
                "latest_trigger_end": now[0],
                "latest_trigger_available_at": now[0],
                "atr": "0.001",
                "last_market": now[0],
                "last_available": now[0],
                "data_mode": "LIVE",
            }
        )
        self.assertEqual(callbacks.manage(), ())
        self.assertEqual(len(transport.positions_by_id), 1)

        provider.bid = "1.1000"
        provider.ask = "1.1002"
        now[0] = NOW + timedelta(minutes=5)
        callbacks.observe_runtime(
            {
                "market_candidate_id": "tp_fast_v1",
                "trigger_timeframe": "M1",
                "trigger_bar_count": int(now[0].timestamp() // 60),
                "risk_bar_clock_basis": "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION",
                "latest_trigger_start": now[0] - timedelta(minutes=1),
                "latest_trigger_end": now[0],
                "latest_trigger_available_at": now[0],
                "atr": "0.001",
                "last_market": now[0],
                "last_available": now[0],
                "data_mode": "LIVE",
            }
        )
        managed = callbacks.manage()
        self.assertEqual(len(managed), 1)
        self.assertEqual(managed[0].state.value, "CLOSED")
        self.assertEqual(transport.positions_by_id, {})
        decisions = [event for event in executor.events() if event.event_type == "RISK_EXIT_DECISION"]
        self.assertTrue(decisions)
        self.assertEqual(decisions[-1].details["action"], "TIME_EXIT")

    def test_catalog_projection_does_not_infer_fee_values_from_fees_known(self) -> None:
        selected = SimpleNamespace(
            name="EUR/USD",
            symbol_id=11,
            metadata={
                "fullSymbol": {
                    "symbolId": 11,
                    "minVolume": 100,
                    "maxVolume": 100_000,
                    "stepVolume": 100,
                    "pipSize": "0.0001",
                    "unitValue": "1",
                    "commissionKnown": True,
                }
            },
        )
        provider = SimpleNamespace(
            name="ctrader_open_api",
            catalog=SimpleNamespace(selected=selected, symbols=(selected,), requested_symbol="EUR/USD"),
        )
        projected = _observed_risk_contract_spec(
            provider,
            NOW,
            VolumeGrid(min_volume=100, max_volume=100_000, step_volume=100),
        )
        self.assertTrue(projected["fees_known"])
        self.assertNotIn("expected_cost_fixed", projected)
        self.assertNotIn("expected_cost_per_unit", projected)
        self.assertNotIn("expected_exit_slippage_pips", projected)


if __name__ == "__main__":
    unittest.main()
