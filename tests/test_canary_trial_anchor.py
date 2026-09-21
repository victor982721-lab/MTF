"""Regression coverage for the approval-bound DEMO canary trial anchor.

The trial anchor is an opt-in exception for this bounded canary only.  These
tests deliberately keep the account's UTC-day anchor UNKNOWN in the positive
case: the canary may use its own observed trial equity, but it must not turn an
unknown daily baseline into zero or alter the existing risk gates.
"""

from __future__ import annotations

import dataclasses
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mtf_lab.core.risk_exit import CanaryTrialRiskBoundary, RiskExitError, RiskExitPolicy, plan_entry
from mtf_lab.ops.ctrader_demo_transport import ServerAccountObservation
from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DemoAccount,
    DemoTransport,
    ExecutionPolicy,
    MemoryIntentStore,
    Quote,
    RiskLimitRejected,
)
from mtf_lab.ops.supervision import SupervisorStateStore
from tests.test_demo_canary import FakeProvider, _binding, _config, _inputs
from tools.demo_canary import CanaryApproval, CanaryGateError, _approval_document_digest, run_demo_canary

NOW = datetime(2026, 9, 19, 14, 0, tzinfo=UTC)
ACCOUNT_ID = "5097"
GENERATION = "g1"
ENDPOINT = "demo.ctraderapi.com:5035"


def _approval(
    *, opt_in: bool = False, source: str | None = None, approval_id: str = "trial-anchor-1"
) -> CanaryApproval:
    return CanaryApproval(
        approved=True,
        approval_id=approval_id,
        account_id=ACCOUNT_ID,
        symbol="EUR/USD",
        window_start=NOW - timedelta(minutes=1),
        window_end=NOW + timedelta(minutes=20),
        max_holding_seconds=Decimal("300"),
        max_quantity=Decimal("1000"),
        max_risk_fraction=Decimal("0.0005"),
        max_mutation_messages=6,
        canary_trial_anchor_authorized=opt_in,
        canary_trial_anchor_authorization_source=source,
    )


def _provenance() -> dict[str, object]:
    return {
        "network_performed": True,
        "source_mode": "DEMO_OBSERVED",
        "synthetic": False,
        "environment": "DEMO",
        "account_id": ACCOUNT_ID,
        "account_selected": True,
        "account_verified": True,
        "endpoint": ENDPOINT,
        "connection_generation": GENERATION,
    }


def _risk_projection(
    *,
    equity: Decimal = Decimal("10000"),
    generation: str = GENERATION,
    cashflow_total: str | None = "0",
    cashflows_complete: bool = True,
    account_complete: bool = True,
    daily_complete: bool = False,
    high_water_equity: str | None = "10000",
    observed_at: datetime = NOW,
) -> dict[str, object]:
    daily_state = "READY" if daily_complete else "UNKNOWN"
    return {
        "state": "READY",
        "account_id": ACCOUNT_ID,
        "session_id": "s1",
        "environment": "DEMO",
        "endpoint": ENDPOINT,
        "symbol": "EUR/USD",
        "equity": str(equity),
        "equity_source": "OBSERVED_DEMO",
        "account_complete": account_complete,
        "daily_complete": daily_complete,
        "fresh": True,
        "complete": daily_complete,
        "reasons": [] if daily_complete else ["day_anchor_start_unobserved"],
        "metrics_observed_at": observed_at.isoformat(),
        "metrics_connection_generation": generation,
        "position_state": "VALID",
        "positions_fresh": True,
        "metrics": {
            "equity": str(equity),
            "high_water_equity": high_water_equity,
            "drawdown": "0",
        },
        "risk_exit": {
            "metrics": {
                "daily_loss_state": daily_state,
                "daily_loss": "0" if daily_complete else None,
                "daily_anchor_equity": "10000" if daily_complete else None,
                "daily_anchor_cashflow_total": "0" if daily_complete else None,
                "daily_anchor_verified": daily_complete,
                "daily_cashflow_total": cashflow_total,
                "cashflows_complete": cashflows_complete,
            }
        },
    }


def _trial_boundary() -> CanaryTrialRiskBoundary:
    return CanaryTrialRiskBoundary(
        approval_digest="a" * 64,
        authorization_source="Usuario: autorización DEMO trial anchor",
        account_id=ACCOUNT_ID,
        session_id="session-trial-anchor",
        connection_generation=GENERATION,
        window_start=NOW - timedelta(minutes=1),
        window_end=NOW + timedelta(minutes=20),
        trial_equity=Decimal("10000"),
        trial_cashflow_total=Decimal("0"),
        trial_loss_fraction=Decimal("0.001"),
        trial_loss_budget=Decimal("10"),
    )


def _snapshot_from_raw(raw: dict[str, object]) -> SimpleNamespace:
    metrics = raw["metrics"]
    assert isinstance(metrics, dict)
    risk_exit = raw["risk_exit"]
    assert isinstance(risk_exit, dict)
    exit_metrics = risk_exit["metrics"]
    assert isinstance(exit_metrics, dict)

    def to_dict() -> dict[str, object]:
        return dict(raw)

    return SimpleNamespace(
        fresh=raw["fresh"],
        complete=raw["complete"],
        account_complete=raw["account_complete"],
        equity=metrics["equity"],
        realized_daily_pnl=None,
        unrealized_daily_pnl=None,
        drawdown=metrics["drawdown"],
        high_water_equity=metrics["high_water_equity"],
        used_margin=None,
        margin_level=None,
        observed_at=datetime.fromisoformat(str(raw["metrics_observed_at"])),
        account_id=ACCOUNT_ID,
        session_id="s1",
        environment="DEMO",
        endpoint=ENDPOINT,
        symbol="EUR/USD",
        connection_generation=raw["metrics_connection_generation"],
        daily_loss=exit_metrics["daily_loss"],
        daily_anchor_equity=exit_metrics["daily_anchor_equity"],
        daily_anchor_cashflow_total=exit_metrics["daily_anchor_cashflow_total"],
        daily_cashflow_total=exit_metrics["daily_cashflow_total"],
        daily_anchor_day=NOW.date().isoformat() if raw["complete"] else None,
        daily_anchor_observed_at=NOW if raw["complete"] else None,
        daily_anchor_verified=exit_metrics["daily_anchor_verified"],
        daily_loss_state=exit_metrics["daily_loss_state"],
        daily_loss_reason=None if raw["complete"] else "day_anchor_start_unobserved",
        cashflows_complete=exit_metrics["cashflows_complete"],
        cashflow_fingerprint="fixture-cashflow",
        to_dict=to_dict,
    )


def _trial_loss_updater(binding: SimpleNamespace, captured: list[object]):
    def update(
        committed: object,
        observed: object | None = None,
        expected_digest: object | None = None,
    ) -> None:
        if captured and str(expected_digest) != captured[-1].approval_digest:
            raise AssertionError("trial loss update must remain bound to approval digest")
        next_committed = Decimal(str(committed))
        prior = binding.executor.trial_loss_updates[-1] if binding.executor.trial_loss_updates else Decimal("0")
        if next_committed < prior:
            raise AssertionError("trial loss committed value must be monotonic")
        binding.executor.trial_loss_updates.append(next_committed)
        binding.executor.trial_loss_observed = observed
        boundary = getattr(binding.executor, "canary_trial_risk", None)
        if isinstance(boundary, CanaryTrialRiskBoundary):
            binding.executor.canary_trial_risk = boundary.with_loss(
                committed=next_committed,
                observed=observed,
            )

    return update


def _trial_setter(binding: SimpleNamespace, captured: list[object]):
    def set_boundary(boundary: object) -> None:
        if not isinstance(boundary, CanaryTrialRiskBoundary):
            raise AssertionError("the runner must attach the typed trial boundary")
        captured.append(boundary)
        binding.executor.canary_trial_risk = boundary
        ledger_path = getattr(binding.executor, "ledger_path", None)
        if isinstance(ledger_path, Path) and ledger_path.exists():
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            record = next(iter(ledger["approvals"].values()))
            binding.executor.ledger_snapshots.append((record.get("state"), record.get("mutation_messages", 0)))

    return set_boundary


def _arm_fake_binding(binding: SimpleNamespace, snapshots: list[dict[str, object]]) -> list[object]:
    """Attach the canonical-shaped risk observer seam to the existing fake binding."""

    calls = 0
    captured: list[object] = []

    def current() -> dict[str, object]:
        nonlocal calls
        index = min(calls, len(snapshots) - 1)
        calls += 1
        return snapshots[index]

    def status(*, refresh: bool = False) -> dict[str, object]:
        del refresh
        return {"risk": current()}

    def activate() -> None:
        binding.executor.active = True
        binding.executor.activated = True

    binding.executor.status = status
    binding.executor.update_risk_metrics = lambda **_kwargs: {"state": "READY"}
    binding.executor.update_canary_trial_loss = _trial_loss_updater(binding, captured)
    binding.executor.activate = activate
    binding.executor.active = False
    binding.executor.activated = False
    binding.executor.server_observation = binding.observation
    binding.executor.clock = lambda: NOW
    binding.executor.trial_loss_updates = []
    binding.executor.trial_loss_observed = None
    binding.executor.ledger_snapshots = []
    binding.risk_observer = SimpleNamespace(
        observe=lambda: _snapshot_from_raw(current()),
        update_executor=lambda _executor: current(),
    )
    binding.executor.set_canary_trial_risk = _trial_setter(binding, captured)
    return captured


class CanaryTrialAnchorTests(unittest.TestCase):
    def test_opt_in_defaults_false_and_true_requires_human_source(self) -> None:
        default = _approval()
        self.assertFalse(default.canary_trial_anchor_authorized)
        self.assertIsNone(default.canary_trial_anchor_authorization_source)

        with self.assertRaises(ValueError):
            _approval(opt_in=True)

        approved = _approval(opt_in=True, source="Usuario: autorización DEMO trial anchor")
        self.assertTrue(approved.canary_trial_anchor_authorized)
        self.assertEqual(approved.canary_trial_anchor_authorization_source, "Usuario: autorización DEMO trial anchor")

        parsed = CanaryApproval.from_mapping(
            {
                "approved": True,
                "approval_id": "trial-anchor-from-mapping",
                "account_id": ACCOUNT_ID,
                "symbol": "EUR/USD",
                "window_start": (NOW - timedelta(minutes=1)).isoformat(),
                "window_end": (NOW + timedelta(minutes=20)).isoformat(),
                "max_holding_seconds": "300",
                "max_quantity": "1000",
                "max_risk_fraction": "0.0005",
                "max_trial_loss_fraction": "0.001",
                "max_mutation_messages": 6,
                "stop_loss_atr_multiple": "1.5",
                "take_profit_atr_multiple": "3",
                "canary_trial_anchor_authorized": True,
                "canary_trial_anchor_authorization_source": "Usuario: autorización DEMO trial anchor",
            }
        )
        self.assertTrue(parsed.canary_trial_anchor_authorized)
        self.assertEqual(parsed.canary_trial_anchor_authorization_source, "Usuario: autorización DEMO trial anchor")

    def test_generic_risk_exit_stays_fail_closed_on_daily_unknown(self) -> None:
        policy = RiskExitPolicy(
            planned_risk_fraction=Decimal("0.0005"),
            max_daily_loss_fraction=Decimal("0.01"),
            equity_basis="OBSERVED_DEMO",
        )
        risk_state = {
            "equity": "10000",
            "equity_source": "OBSERVED_DEMO",
            "daily_pnl": None,
            "drawdown": "0",
            "high_water_equity": "10000",
            "bar_clock_known": True,
            "margin_available": "100000",
            "margin_required": "10",
            "costs_known": True,
            "account_complete": True,
            "positions": 0,
            "intents": 0,
        }
        contract = {
            "known": True,
            "pip_size": "0.0001",
            "quantity_min": "1",
            "quantity_step": "1",
            "quantity_max": "1000",
            "unit_value": "1",
            "minimum_stop_distance": "0.00001",
            "margin_per_unit": "10",
            "fees_known": True,
            "spread_known": True,
            "expected_cost_fixed": "0",
            "expected_cost_per_unit": "0",
            "expected_exit_slippage_per_unit": "0.00001",
            "expected_cost_currency": "USD",
            "expected_cost_source": "fixture-observed",
        }
        plan = plan_entry(
            policy,
            direction="BUY",
            entry_price="1.1002",
            atr="0.001",
            equity="10000",
            available_at=NOW,
            contract_spec=contract,
            calendar_state={"known": True, "basis": "OBSERVED_BROKER", "financing_known": True},
            risk_state=risk_state,
            requested_quantity="1",
            equity_source="OBSERVED_DEMO",
            executable_bid="1.1000",
            executable_ask="1.1002",
        )
        self.assertFalse(plan.allowed)
        self.assertIn("DAILY_PNL_UNKNOWN", plan.reasons)

    def test_typed_trial_boundary_relaxes_only_daily_unknown(self) -> None:
        policy = RiskExitPolicy(
            planned_risk_fraction=Decimal("0.0005"),
            max_daily_loss_fraction=Decimal("0.01"),
            equity_basis="OBSERVED_DEMO",
        )
        risk_state = {
            "equity": "10000",
            "equity_source": "OBSERVED_DEMO",
            "daily_pnl": None,
            "drawdown": "0",
            "high_water_equity": "10000",
            "bar_clock_known": True,
            "margin_available": "100000",
            "margin_required": "10",
            "costs_known": True,
            "account_complete": True,
            "positions": 0,
            "intents": 0,
        }
        contract = {
            "known": True,
            "pip_size": "0.0001",
            "quantity_min": "1",
            "quantity_step": "1",
            "quantity_max": "1000",
            "unit_value": "1",
            "minimum_stop_distance": "0.00001",
            "margin_per_unit": "10",
            "fees_known": True,
            "spread_known": True,
            "expected_cost_fixed": "0",
            "expected_cost_per_unit": "0",
            "expected_exit_slippage_per_unit": "0.00001",
            "expected_cost_currency": "USD",
            "expected_cost_source": "fixture-observed",
        }
        plan = plan_entry(
            policy,
            direction="BUY",
            entry_price="1.1002",
            atr="0.001",
            equity="10000",
            available_at=NOW,
            contract_spec=contract,
            calendar_state={"known": True, "basis": "OBSERVED_BROKER", "financing_known": True},
            risk_state=risk_state,
            requested_quantity="1",
            equity_source="OBSERVED_DEMO",
            executable_bid="1.1000",
            executable_ask="1.1002",
            canary_trial=_trial_boundary(),
        )
        self.assertTrue(plan.allowed)
        self.assertNotIn("DAILY_PNL_UNKNOWN", plan.reasons)

    def test_typed_trial_boundary_does_not_relax_stops_or_highwater(self) -> None:
        policy = RiskExitPolicy(
            planned_risk_fraction=Decimal("0.0005"),
            max_daily_loss_fraction=Decimal("0.01"),
            equity_basis="OBSERVED_DEMO",
        )
        contract = {
            "known": True,
            "pip_size": "0.0001",
            "quantity_min": "1",
            "quantity_step": "1",
            "quantity_max": "1000",
            "unit_value": "1",
            "minimum_stop_distance": "0.00001",
            "margin_per_unit": "10",
            "fees_known": True,
            "spread_known": True,
            "expected_cost_fixed": "0",
            "expected_cost_per_unit": "0",
            "expected_exit_slippage_per_unit": "0.00001",
            "expected_cost_currency": "USD",
            "expected_cost_source": "fixture-observed",
        }

        def make_plan(**risk_changes: object):
            risk_state = {
                "equity": "10000",
                "equity_source": "OBSERVED_DEMO",
                "daily_pnl": None,
                "drawdown": "0",
                "high_water_equity": "10000",
                "bar_clock_known": True,
                "margin_available": "100000",
                "margin_required": "10",
                "costs_known": True,
                "account_complete": True,
                "positions": 0,
                "intents": 0,
            }
            risk_state.update(risk_changes)
            return plan_entry(
                policy,
                direction="BUY",
                entry_price="1.1002",
                atr="0.001",
                equity="10000",
                available_at=NOW,
                contract_spec=contract,
                calendar_state={"known": True, "basis": "OBSERVED_BROKER", "financing_known": True},
                risk_state=risk_state,
                requested_quantity="1",
                equity_source="OBSERVED_DEMO",
                executable_bid=risk_changes.pop("executable_bid", "1.1000"),
                executable_ask="1.1002",
                canary_trial=_trial_boundary(),
            )

        stop_blocked = make_plan(executable_bid=None)
        self.assertFalse(stop_blocked.allowed)
        self.assertIn("STOP_EXECUTABLE_SIDE_UNKNOWN", stop_blocked.reasons)
        highwater_blocked = make_plan(high_water_equity=None)
        self.assertFalse(highwater_blocked.allowed)
        self.assertIn("HIGH_WATER_UNKNOWN", highwater_blocked.reasons)

    def test_executor_rejects_raw_or_missing_typed_trial_context(self) -> None:
        account = DemoAccount(
            ACCOUNT_ID,
            "DEMO",
            "demo://ctrader",
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )
        executor = CTraderDemoExecutor(
            account,
            policy=ExecutionPolicy(max_daily_loss=Decimal("5")),
            transport=DemoTransport(account_id=ACCOUNT_ID, endpoint="demo://ctrader", scopes={"trading"}),
            intent_store=MemoryIntentStore(),
            risk_exit_policy=RiskExitPolicy(equity_basis="OBSERVED_DEMO"),
        )
        with self.assertRaises(RiskLimitRejected):
            executor.set_canary_trial_risk({"trial_equity": "10000"})  # type: ignore[arg-type]
        with self.assertRaises(RiskLimitRejected):
            executor.set_canary_trial_risk(None)  # type: ignore[arg-type]

    def test_trial_context_binds_account_session_generation_window_and_monotonic_cap(self) -> None:
        account = DemoAccount(
            ACCOUNT_ID,
            "DEMO",
            ENDPOINT,
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )
        observation = ServerAccountObservation(
            account_id=ACCOUNT_ID,
            environment="DEMO",
            endpoint=ENDPOINT,
            scopes=frozenset({"trading"}),
            observed_at=NOW,
            source="test",
            session_id="session-trial-anchor",
            connection_generation=GENERATION,
        )
        executor = CTraderDemoExecutor(
            account,
            transport=DemoTransport(account_id=ACCOUNT_ID, endpoint=ENDPOINT, scopes={"trading"}),
            intent_store=MemoryIntentStore(),
            server_observation=observation,
            risk_exit_policy=RiskExitPolicy(equity_basis="OBSERVED_DEMO"),
        )
        boundary = _trial_boundary()
        for label, mismatch in (
            ("account", dataclasses.replace(boundary, account_id="5098")),
            ("session", dataclasses.replace(boundary, session_id="other-session")),
            ("generation", dataclasses.replace(boundary, connection_generation="g2")),
        ):
            with self.subTest(label=label), self.assertRaises(RiskLimitRejected):
                executor.set_canary_trial_risk(mismatch)

        with self.assertRaises(RiskExitError):
            dataclasses.replace(boundary, window_end=boundary.window_start)

        committed = boundary.with_loss(committed=Decimal("5"))
        lowered = committed.with_loss(committed=Decimal("4"), observed=Decimal("3"))
        self.assertEqual(committed.trial_loss_committed, Decimal("5"))
        self.assertEqual(lowered.trial_loss_committed, Decimal("5"))
        self.assertEqual(lowered.loss_committed, Decimal("5"))

    def test_real_executor_fixture_uses_trial_boundary_without_daily_zero(self) -> None:
        account = DemoAccount(
            ACCOUNT_ID,
            "DEMO",
            ENDPOINT,
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )
        observation = ServerAccountObservation(
            account_id=ACCOUNT_ID,
            environment="DEMO",
            endpoint=ENDPOINT,
            scopes=frozenset({"trading"}),
            observed_at=NOW,
            source="test",
            session_id="session-trial-anchor",
            connection_generation=GENERATION,
        )
        contract = {
            "known": True,
            "pip_size": "0.0001",
            "quantity_min": "1",
            "quantity_step": "1",
            "quantity_max": "1000",
            "unit_value": "1",
            "minimum_stop_distance": "0.00001",
            "margin_per_unit": "10",
            "fees_known": True,
            "spread_known": True,
            "expected_cost_fixed": "0",
            "expected_cost_per_unit": "0",
            "expected_exit_slippage_per_unit": "0.00001",
            "expected_cost_currency": "USD",
            "expected_cost_source": "fixture-observed",
        }
        executor = CTraderDemoExecutor(
            account,
            policy=ExecutionPolicy(max_quantity=Decimal("1000"), max_daily_loss=Decimal("5")),
            transport=DemoTransport(account_id=ACCOUNT_ID, endpoint=ENDPOINT, scopes={"trading"}),
            intent_store=MemoryIntentStore(),
            clock=lambda: NOW,
            server_observation=observation,
            risk_exit_policy=RiskExitPolicy(
                planned_risk_fraction=Decimal("0.0005"),
                equity_basis="VIRTUAL_PAPER_ONLY",
            ),
            risk_contract_spec=contract,
            risk_calendar={"known": True, "basis": "OBSERVED_BROKER", "financing_known": True},
            market_candidate_id="tp_fast_v1",
        )
        runtime = dict(_inputs().buy_runtime)
        runtime["trigger_bar_count"] = int(NOW.timestamp() // 60)
        runtime["latest_trigger_start"] = (NOW - timedelta(minutes=1)).isoformat()
        normalized_runtime = executor.observe_runtime(runtime)
        self.assertEqual(normalized_runtime["runtime_state"], "VALID", normalized_runtime["reasons"])
        executor.update_risk_metrics(
            equity=Decimal("10000"),
            daily_pnl=None,
            drawdown=Decimal("0"),
            high_water_equity=Decimal("10000"),
            margin_available=Decimal("10000"),
            margin_required=Decimal("10"),
            margin_level=Decimal("1000"),
            observed_at=NOW,
            connection_generation=GENERATION,
            daily_loss=None,
            daily_cashflow_total=Decimal("0"),
            daily_anchor_verified=False,
            daily_loss_state="UNKNOWN",
            cashflows_complete=True,
        )
        boundary = _trial_boundary()
        executor.set_canary_trial_risk(boundary)
        with self.assertRaises(RiskLimitRejected):
            executor.update_canary_trial_loss(
                committed=Decimal("1"),
                expected_digest="0" * 64,
            )
        updated = executor.update_canary_trial_loss(
            committed=Decimal("5"),
            observed=Decimal("3"),
            expected_digest=boundary.approval_digest,
        )
        self.assertEqual(updated.trial_loss_committed, Decimal("5"))
        self.assertEqual(updated.trial_loss_observed, Decimal("3"))
        lowered = executor.update_canary_trial_loss(
            committed=Decimal("4"),
            observed=Decimal("2"),
            expected_digest=boundary.approval_digest,
        )
        self.assertEqual(lowered.trial_loss_committed, Decimal("5"))
        self.assertEqual(lowered.trial_loss_observed, Decimal("3"))
        executor.clock = lambda: NOW + timedelta(hours=1)
        with self.assertRaises(RiskLimitRejected):
            executor.update_canary_trial_loss(
                committed=Decimal("6"),
                expected_digest=boundary.approval_digest,
            )
        executor.clock = lambda: NOW
        quote = Quote(
            "EUR/USD",
            Decimal("1.1000"),
            Decimal("1.1002"),
            NOW,
            NOW,
            connection_generation=GENERATION,
            data_mode="LIVE",
        )
        signal = {
            "signal_id": "real-fixture-buy",
            "instrument": "EUR/USD",
            "direction": "BUY",
            "mode": "DEMO",
            "account_environment": "DEMO",
            "data_mode": "LIVE",
            "market_candidate_id": "tp_fast_v1",
            "atr": "0.001",
        }
        allowed = executor.risk_entry_plan(signal, quote, requested_quantity=Decimal("1"))
        self.assertIsNotNone(allowed)
        assert allowed is not None
        self.assertTrue(allowed.allowed, allowed.reasons)
        self.assertNotIn("DAILY_PNL_UNKNOWN", allowed.reasons)
        self.assertFalse(executor.active)

        executor.set_canary_trial_risk(boundary.with_loss(committed=Decimal("10")))
        breached = executor.risk_entry_plan(signal, quote, requested_quantity=Decimal("1"))
        self.assertIsNotNone(breached)
        assert breached is not None
        self.assertFalse(breached.allowed)
        self.assertIn("CANARY_TRIAL_LOSS_BUDGET", breached.reasons)

    def test_authorized_canary_runs_two_cycles_with_daily_unknown_and_no_reset(self) -> None:
        binding = _binding()
        snapshots = [_risk_projection() for _ in range(32)]
        captured = _arm_fake_binding(binding, snapshots)
        approval = _approval(opt_in=True, source="Usuario: autorización DEMO trial anchor")
        with tempfile.TemporaryDirectory(prefix="mtf-canary-trial-anchor-") as directory:
            root = Path(directory)
            daily_anchor = root / "account-risk.day-anchor.json"
            journal = root / "intents.jsonl"
            daily_anchor.write_text('{"scope":"UTC_DAY_EQUITY_ANCHOR","anchor_equity":"9999"}\n', encoding="utf-8")
            journal.write_text('{"journal_type":"event","event_type":"ACTIVATED"}\n', encoding="utf-8")
            before_anchor = daily_anchor.read_bytes()
            before_journal = journal.read_bytes()
            binding.executor.ledger_path = root / "canary-approval-ledger.json"
            with patch("tools.demo_canary.build_demo_execution_binding", return_value=binding):
                result = run_demo_canary(
                    FakeProvider(),
                    _provenance(),
                    _config(),
                    state_dir=root,
                    approval=approval,
                    inputs=_inputs(),
                    execute=True,
                    writer_store=SupervisorStateStore(root, "trial-anchor"),
                    market_window_state=lambda *_args: "OPEN",
                    clock=lambda: NOW,
                )
            self.assertTrue(result.ok)
            self.assertEqual(result.state, "CANARY_COMPLETED")
            self.assertEqual((binding.executor.submit_calls, binding.executor.close_calls), (2, 2))
            self.assertTrue(binding.executor.activated)
            self.assertEqual(len(captured), 1)
            boundary = captured[0]
            self.assertEqual(boundary.account_id, ACCOUNT_ID)
            self.assertEqual(boundary.connection_generation, GENERATION)
            self.assertEqual(boundary.approval_digest, _approval_document_digest(approval))
            self.assertEqual(boundary.trial_equity, Decimal("10000"))
            self.assertEqual(boundary.trial_loss_budget, Decimal("10.000"))
            self.assertEqual(binding.executor.ledger_snapshots, [("STARTED", 0)])
            self.assertEqual(daily_anchor.read_bytes(), before_anchor)
            self.assertEqual(journal.read_bytes(), before_journal)

            ledger = json.loads((root / "canary-approval-ledger.json").read_text(encoding="utf-8"))
            record = next(iter(ledger["approvals"].values()))
            self.assertEqual(record["state"], "COMPLETED")
            self.assertEqual(record["canary_trial_anchor_authorized"], True)
            self.assertEqual(
                record["canary_trial_anchor_authorization_source"],
                approval.canary_trial_anchor_authorization_source,
            )
            self.assertEqual(record["approval_digest"], boundary.approval_digest)
            self.assertEqual(record["account_id"], ACCOUNT_ID)
            self.assertEqual(record["session_id"], "s1")
            self.assertEqual(record["connection_generation"], GENERATION)
            self.assertEqual(record["endpoint"], ENDPOINT.lower())
            self.assertEqual(record["window_start"], approval.window_start.isoformat())
            self.assertEqual(record["window_end"], approval.window_end.isoformat())
            self.assertEqual(record["trial_cashflow_total"], "0")
            self.assertEqual(record["trial_loss_fraction"], "0.001")
            self.assertEqual(record["trial_loss_budget"], "10.000")
            self.assertEqual(Decimal(record["trial_equity"]), Decimal("10000"))
            self.assertEqual(Decimal(record["trial_loss_budget"]), Decimal("10.000"))
            self.assertEqual(Decimal(record["trial_loss_committed"]), Decimal("8"))
            self.assertEqual(binding.executor.trial_loss_updates[-1], Decimal("8"))
            self.assertEqual(os.stat(root / "canary-approval-ledger.json").st_mode & 0o777, 0o600)

    def test_without_opt_in_known_daily_anchor_keeps_legacy_canary_route(self) -> None:
        binding = _binding()
        captured = _arm_fake_binding(binding, [_risk_projection(daily_complete=True) for _ in range(32)])
        approval = _approval(opt_in=False, approval_id="legacy-canary-route")
        with tempfile.TemporaryDirectory(prefix="mtf-canary-legacy-route-") as directory:
            root = Path(directory)
            with patch("tools.demo_canary.build_demo_execution_binding", return_value=binding):
                result = run_demo_canary(
                    FakeProvider(),
                    _provenance(),
                    _config(),
                    state_dir=root,
                    approval=approval,
                    inputs=_inputs(),
                    execute=True,
                    writer_store=SupervisorStateStore(root, "legacy-canary-route"),
                    market_window_state=lambda *_args: "OPEN",
                    clock=lambda: NOW,
                )
            self.assertTrue(result.ok)
            self.assertEqual(result.state, "CANARY_COMPLETED")
            self.assertEqual((binding.executor.submit_calls, binding.executor.close_calls), (2, 2))
            self.assertEqual(captured, [])

    def test_opt_in_without_observer_or_account_completeness_never_activates(self) -> None:
        binding = _binding()
        activated: list[bool] = []
        binding.executor.activate = lambda: activated.append(True)
        binding.risk_observer = SimpleNamespace(
            observe=lambda: SimpleNamespace(fresh=True, account_complete=False, equity="10000")
        )
        approval = _approval(opt_in=True, source="Usuario: autorización DEMO trial anchor", approval_id="missing-risk")
        with tempfile.TemporaryDirectory(prefix="mtf-canary-missing-risk-") as directory:
            root = Path(directory)
            with (
                patch("tools.demo_canary.build_demo_execution_binding", return_value=binding),
                self.assertRaises(CanaryGateError),
            ):
                run_demo_canary(
                    FakeProvider(),
                    _provenance(),
                    _config(),
                    state_dir=root,
                    approval=approval,
                    inputs=_inputs(),
                    execute=True,
                    writer_store=SupervisorStateStore(root, "missing-risk"),
                    market_window_state=lambda *_args: "OPEN",
                    clock=lambda: NOW,
                )
            self.assertEqual(binding.executor.submit_calls, 0)
            self.assertEqual(binding.executor.close_calls, 0)
            self.assertEqual(activated, [])

    def test_trial_loss_equal_to_budget_stops_before_second_cycle(self) -> None:
        binding = _binding()
        snapshots = [_risk_projection() for _ in range(9)] + [
            _risk_projection(equity=Decimal("9990")) for _ in range(16)
        ]
        _arm_fake_binding(binding, snapshots)
        approval = _approval(
            opt_in=True,
            source="Usuario: autorización DEMO trial anchor",
            approval_id="trial-loss-eq",
        )
        with tempfile.TemporaryDirectory(prefix="mtf-canary-trial-loss-") as directory:
            root = Path(directory)
            with (
                patch("tools.demo_canary.build_demo_execution_binding", return_value=binding),
                self.assertRaises(CanaryGateError) as raised,
            ):
                run_demo_canary(
                    FakeProvider(),
                    _provenance(),
                    _config(),
                    state_dir=root,
                    approval=approval,
                    inputs=_inputs(),
                    execute=True,
                    writer_store=SupervisorStateStore(root, "trial-loss-eq"),
                    market_window_state=lambda *_args: "OPEN",
                    clock=lambda: NOW,
                )
            self.assertEqual(binding.executor.submit_calls, 1)
            self.assertEqual(binding.executor.close_calls, 1)
            self.assertIn("trial loss", str(raised.exception).lower())

    def test_existing_cashflow_generation_and_freshness_gates_remain(self) -> None:
        cases = (
            ("cashflow", {"cashflow_total": None, "cashflows_complete": False}, "cashflow"),
            ("generation", {"generation": "g2"}, "risklimitrejected"),
            ("freshness", {"observed_at": NOW - timedelta(seconds=31)}, "frescas"),
        )
        for label, changes, expected in cases:
            with self.subTest(label=label):
                binding = _binding()
                snapshot = _risk_projection(**changes)
                _arm_fake_binding(binding, [snapshot] * 8)
                approval = _approval(
                    opt_in=True,
                    source="Usuario: autorización DEMO trial anchor",
                    approval_id=f"trial-gate-{label}",
                )
                with tempfile.TemporaryDirectory(prefix=f"mtf-canary-trial-{label}-") as directory:
                    root = Path(directory)
                    with (
                        patch("tools.demo_canary.build_demo_execution_binding", return_value=binding),
                        self.assertRaises(CanaryGateError) as raised,
                    ):
                        run_demo_canary(
                            FakeProvider(),
                            _provenance(),
                            _config(),
                            state_dir=root,
                            approval=approval,
                            inputs=_inputs(),
                            execute=True,
                            writer_store=SupervisorStateStore(root, f"trial-gate-{label}"),
                            market_window_state=lambda *_args: "OPEN",
                            clock=lambda: NOW,
                        )
                    self.assertEqual(binding.executor.submit_calls, 0, label)
                    self.assertEqual(binding.executor.close_calls, 0, label)
                    self.assertIn(expected, str(raised.exception).lower(), label)

    def test_quote_freshness_gate_remains_before_any_trial_open(self) -> None:
        class StaleProvider(FakeProvider):
            def snapshot_quote_state(self):
                snapshot = super().snapshot_quote_state()
                for leg in snapshot["symbols"]["11"].values():
                    leg["event_time"] = (NOW - timedelta(seconds=31)).isoformat()
                    leg["available_at"] = (NOW - timedelta(seconds=31)).isoformat()
                return snapshot

        binding = _binding()
        _arm_fake_binding(binding, [_risk_projection() for _ in range(32)])
        approval = _approval(opt_in=True, source="Usuario: autorización DEMO trial anchor", approval_id="trial-quote")
        with tempfile.TemporaryDirectory(prefix="mtf-canary-trial-quote-") as directory:
            root = Path(directory)
            with (
                patch("tools.demo_canary.build_demo_execution_binding", return_value=binding),
                self.assertRaises(CanaryGateError) as raised,
            ):
                run_demo_canary(
                    StaleProvider(),
                    _provenance(),
                    _config(),
                    state_dir=root,
                    approval=approval,
                    inputs=_inputs(),
                    execute=True,
                    writer_store=SupervisorStateStore(root, "trial-quote"),
                    market_window_state=lambda *_args: "OPEN",
                    clock=lambda: NOW,
                )
            self.assertEqual(binding.executor.submit_calls, 0)
            self.assertEqual(binding.executor.close_calls, 0)
            self.assertIn("bbo", str(raised.exception).lower())


if __name__ == "__main__":
    unittest.main()
