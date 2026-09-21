"""Negative boundary tests for the bounded DEMO canary.

These tests deliberately stop at the canary seam.  They use a deterministic
provider, clock, and execution binding; no cTrader client, credential store,
network, SQLite database, or production state is touched.  The small fake
executor is only an internal state machine: every assertion below is tied to
the observable canary gate or to the durable approval ledger.
"""

from __future__ import annotations

import dataclasses
import hashlib
import importlib
import json
import os
import tempfile
import unittest
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from mtf_lab.ops.ctrader_executor import OrderState
from mtf_lab.ops.supervision import SupervisorStateStore
from mtf_lab.ops.volume_rules import VolumeGrid
from tools.demo_canary import (
    CanaryApproval,
    CanaryGateError,
    CanaryInputs,
    run_demo_canary,
    validate_static_config,
)

canonical_config = cast(Callable[[], Any], importlib.import_module("tests.test_demo_canary")._config)

CANARY_START = datetime(2026, 9, 21, 15, 0, tzinfo=UTC)
CANARY_END = datetime(2026, 9, 21, 15, 20, tzinfo=UTC)
ACCOUNT_ID = "5097"
ENDPOINT = "demo.ctraderapi.com:5035"
GENERATION = "g1"
SYMBOL = "EUR/USD"


@contextmanager
def _isolated_environment(root: Path) -> Any:
    """Keep HOME/XDG/TMP state out of the user's runtime during one test."""

    home = root / "home"
    xdg = root / "xdg"
    tmp = root / "tmp"
    home.mkdir(parents=True, exist_ok=True)
    xdg.mkdir(parents=True, exist_ok=True)
    tmp.mkdir(parents=True, exist_ok=True)
    with patch.dict(
        os.environ,
        {"HOME": str(home), "XDG_STATE_HOME": str(xdg), "TMPDIR": str(tmp)},
        clear=False,
    ):
        yield


class _FakeClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class _FakeProvider:
    name = "ctrader_open_api"

    def __init__(self, clock: _FakeClock, *, symbol: str = SYMBOL, generation: str = GENERATION) -> None:
        self.clock = clock
        self.generation = generation
        self.config = SimpleNamespace(host="demo.ctraderapi.com", port=5035)
        self.spec = SimpleNamespace(symbol=symbol, symbol_id=11)

    def snapshot_quote_state(self) -> dict[str, Any]:
        now = self.clock()
        event_time = now - timedelta(seconds=1)
        leg = {
            "price": "1.1000",
            "event_time": event_time.isoformat(),
            "available_at": now.isoformat(),
            "generation": self.generation,
            "sequence": 1,
        }
        ask = dict(leg)
        ask["price"] = "1.1002"
        return {"symbols": {"11": {"bid": leg, "ask": ask}}}


class _BoundaryExecutor:
    """Deterministic internal execution state machine for one test run."""

    def __init__(
        self,
        clock: _FakeClock,
        *,
        plan_totals: tuple[Decimal, ...] = (Decimal("4"), Decimal("4")),
        wrong_protection: bool = False,
        fail_submit: bool = False,
        fail_close: bool = False,
        on_submit: Callable[[], None] | None = None,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self.clock = clock
        self.policy = SimpleNamespace(max_quantity=Decimal("1000"), max_price_age_seconds=Decimal("30"))
        self.plan_totals = plan_totals
        self.wrong_protection = wrong_protection
        self.fail_submit = fail_submit
        self.fail_close = fail_close
        self.on_submit = on_submit
        self.on_close = on_close
        self.submit_calls = 0
        self.close_calls = 0
        self.plan_calls = 0
        self.open_position: str | None = None
        self.equity = Decimal("10000")
        self.mutation_hook: Callable[[str], None] | None = None
        self.mutation_snapshots: list[tuple[str, int]] = []
        self.deactivated = False
        self.active = True

    def status(self, *, refresh: bool = False) -> dict[str, Any]:
        del refresh
        return {
            "risk": {
                "position_state": "VALID",
                "positions_fresh": True,
                "metrics_observed_at": self.clock().isoformat(),
                "metrics_connection_generation": GENERATION,
                "metrics": {"equity": str(self.equity)},
                "risk_exit": {
                    "metrics": {
                        "daily_anchor_verified": True,
                        "daily_loss_state": "READY",
                        "daily_anchor_equity": "10000",
                        "daily_cashflow_total": "0",
                        "cashflows_complete": True,
                    }
                },
            }
        }

    def observe_runtime(self, snapshot: Any) -> Any:
        if not isinstance(snapshot, dict) or snapshot.get("runtime_state") != "VALID":
            raise CanaryGateError("runtime snapshot inválido")
        return snapshot

    def risk_entry_plan(self, signal: Any, quote: Any, *, requested_quantity: Decimal) -> Any:
        self.plan_calls += 1
        direction = str(signal["direction"]).upper()
        atr = Decimal(str(signal["atr"]))
        entry = quote.ask if direction == "BUY" else quote.bid
        stop_multiple = Decimal("1.4") if self.wrong_protection else Decimal("1.5")
        target_multiple = Decimal("2.5") if self.wrong_protection else Decimal("3")
        if direction == "BUY":
            stop = entry - atr * stop_multiple
            target = entry + atr * target_multiple
        else:
            stop = entry + atr * stop_multiple
            target = entry - atr * target_multiple
        total_index = min(self.plan_calls - 1, len(self.plan_totals) - 1)
        total = self.plan_totals[total_index]
        return SimpleNamespace(
            allowed=True,
            eligible_for_demo=True,
            mode="DEMO_GATED",
            quantity=Decimal(str(requested_quantity)),
            initial_stop=stop,
            take_profit=target,
            atr=atr,
            risk_envelope_known=True,
            expected_total_loss_at_stop=total,
            expected_total_loss_at_stop_basis="INITIAL_RISK_PLUS_COSTS",
            expected_cost_fixed=Decimal("2"),
            expected_cost_per_unit=Decimal("0"),
            expected_exit_slippage_per_unit=Decimal("0"),
            risk_budget=self.equity * Decimal("0.0005"),
        )

    def reconcile_positions(self) -> dict[str, Any]:
        owned = [] if self.open_position is None else [{"position_id": self.open_position}]
        return {"state": "VALID", "foreign_positions": [], "owned_positions": owned}

    def submit_signal(self, signal: Any, quote: Any) -> Any:
        del quote
        if self.mutation_hook is not None:
            self.mutation_hook("OPEN")
        self.submit_calls += 1
        self.open_position = f"position-{self.submit_calls}"
        if self.on_submit is not None:
            self.on_submit()
        if self.fail_submit:
            # Simulate the ambiguous boundary: the remote mutation may have
            # happened even though the caller received an exception.
            raise RuntimeError("transport submit lost after send")
        intent = SimpleNamespace(intent_id=str(signal["signal_id"]), quantity=Decimal("1000"))
        return SimpleNamespace(
            state=OrderState.FILLED,
            filled_quantity=Decimal("1000"),
            intent=intent,
            position_ids=(self.open_position,),
        )

    def reconcile(self, intent_id: str) -> Any:
        raise AssertionError(f"the fake must not reconcile a known FILLED result: {intent_id}")

    def close_position(self, position_id: str) -> Any:
        if self.mutation_hook is not None:
            self.mutation_hook("CLOSE")
        self.close_calls += 1
        if position_id != self.open_position:
            raise AssertionError("unexpected position id")
        if self.fail_close:
            raise RuntimeError("transport close failed after send")
        self.open_position = None
        if self.on_close is not None:
            self.on_close()
        return SimpleNamespace(
            state=OrderState.CLOSED,
            intent=SimpleNamespace(intent_id=f"close-{position_id}", quantity=Decimal("1000")),
        )

    def deactivate(self, reason: str) -> None:
        del reason
        self.deactivated = True
        self.active = False


def _inputs(clock: _FakeClock) -> CanaryInputs:
    timestamp = clock().isoformat()
    runtime = {
        "runtime_state": "VALID",
        "market_candidate_id": "tp_fast_v1",
        "trigger_timeframe": "M1",
        "data_mode": "LIVE",
        "risk_bar_clock_basis": "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION",
        "trigger_bar_count": 1,
        "latest_trigger_start": (clock() - timedelta(minutes=1)).isoformat(),
        "latest_trigger_end": timestamp,
        "latest_trigger_available_at": timestamp,
        "last_market": timestamp,
        "last_available": timestamp,
        "atr": "0.001",
    }
    return CanaryInputs.from_observed_runtime(
        runtime,
        dict(runtime),
        instrument=SYMBOL,
        buy_signal_id="boundary-buy",
        sell_signal_id="boundary-sell",
    )


def _approval(*, approved: bool = True, **changes: Any) -> CanaryApproval:
    values: dict[str, Any] = {
        "approved": approved,
        "approval_id": "boundary-approval-1",
        "account_id": ACCOUNT_ID,
        "symbol": SYMBOL,
        "window_start": CANARY_START,
        "window_end": CANARY_END,
        "max_holding_seconds": Decimal("300"),
        "max_quantity": Decimal("1000"),
        "max_risk_fraction": Decimal("0.0005"),
        "max_trial_loss_fraction": Decimal("0.001"),
        "max_mutation_messages": 6,
        "stop_loss_atr_multiple": Decimal("1.5"),
        "take_profit_atr_multiple": Decimal("3"),
    }
    values.update(changes)
    return CanaryApproval(**values)


def _provenance() -> dict[str, Any]:
    return {
        "network_performed": True,
        "source_mode": "DEMO_OBSERVED",
        "synthetic": False,
        "environment": "DEMO",
        "account_id": ACCOUNT_ID,
        "account_selected": True,
        "account_verified": True,
        "endpoint": ENDPOINT,
        "symbol": SYMBOL,
        "connection_generation": GENERATION,
    }


def _binding(executor: _BoundaryExecutor) -> SimpleNamespace:
    return SimpleNamespace(
        executor=executor,
        transport=SimpleNamespace(volume_grid=VolumeGrid(100_000, 1_000_000, 100_000)),
        observation=SimpleNamespace(
            session_id="boundary-session",
            account_id=ACCOUNT_ID,
            environment="DEMO",
            endpoint=ENDPOINT,
            symbol=SYMBOL,
            symbol_id=11,
            connection_generation=GENERATION,
            scopes=frozenset({"accounts", "trading"}),
        ),
        risk_observer=None,
        close=lambda: None,
    )


def _store(root: Path) -> SupervisorStateStore:
    return SupervisorStateStore(root / "state", "boundary", lock_root=root / "locks")


def _ledger_record(root: Path, approval: CanaryApproval) -> dict[str, Any]:
    path = root / "state" / "canary-approval-ledger.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    digest = hashlib.sha256(approval.approval_id.encode("utf-8")).hexdigest()
    record = raw["approvals"][digest]
    if not isinstance(record, dict):
        raise AssertionError("approval ledger record is not an object")
    return record


class DemoCanaryBoundaryRegressionTests(unittest.TestCase):
    def _run(
        self,
        root: Path,
        clock: _FakeClock,
        executor: _BoundaryExecutor,
        *,
        provider: _FakeProvider | None = None,
        provenance: dict[str, Any] | None = None,
        approval: CanaryApproval | None = None,
        execute: bool = True,
    ) -> Any:
        provider = provider or _FakeProvider(clock)
        approval = approval or _approval()
        binding = _binding(executor)
        with _isolated_environment(root), patch("tools.demo_canary.build_demo_execution_binding", return_value=binding):
            return run_demo_canary(
                provider,
                provenance or _provenance(),
                canonical_config(),
                state_dir=root / "state",
                approval=approval,
                inputs=_inputs(clock),
                execute=execute,
                writer_store=_store(root),
                risk_refresh=lambda _executor: {"state": "READY"},
                market_window_state=lambda *_args: "OPEN",
                clock=clock,
            )

    def test_durable_mutation_counter_advances_before_each_send(self) -> None:
        clock = _FakeClock(CANARY_START + timedelta(seconds=1))
        with tempfile.TemporaryDirectory(prefix="mtf-canary-boundary-") as directory:
            root = Path(directory)
            approval = _approval()
            executor = _BoundaryExecutor(clock)

            def observe_before_mutation(operation: str) -> None:
                record = _ledger_record(root, approval)
                self.assertEqual(record.get("state"), "STARTED")
                self.assertEqual(record.get("mutation_messages"), len(executor.mutation_snapshots) + 1)
                executor.mutation_snapshots.append((operation, int(record["mutation_messages"])))

            executor.mutation_hook = observe_before_mutation
            result = self._run(root, clock, executor, approval=approval)

            self.assertTrue(result.ok)
            self.assertEqual(executor.mutation_snapshots, [("OPEN", 1), ("CLOSE", 2), ("OPEN", 3), ("CLOSE", 4)])
            record = _ledger_record(root, approval)
            self.assertEqual(record["state"], "COMPLETED")
            self.assertEqual(record["mutation_messages"], 4)
            history = record.get("mutation_history")
            self.assertIsInstance(history, list)
            assert isinstance(history, list)
            self.assertEqual([item["sequence"] for item in history], [1, 2, 3, 4])
            self.assertEqual([item["operation"] for item in history], ["OPEN", "CLOSE", "OPEN", "CLOSE"])

    def test_close_failure_preserves_error_and_consumes_approval(self) -> None:
        clock = _FakeClock(CANARY_START + timedelta(seconds=1))
        with tempfile.TemporaryDirectory(prefix="mtf-canary-boundary-") as directory:
            root = Path(directory)
            approval = _approval()
            executor = _BoundaryExecutor(clock, fail_close=True)
            with self.assertRaises(CanaryGateError) as raised:
                self._run(root, clock, executor, approval=approval)
            self.assertTrue(any(term in str(raised.exception).lower() for term in ("close", "cierre")))
            self.assertEqual(executor.submit_calls, 1)
            self.assertEqual(executor.close_calls, 1)
            record = _ledger_record(root, approval)
            self.assertEqual(record["state"], "UNKNOWN")
            self.assertEqual(record["mutation_messages"], 2)
            self.assertIn("close", str(record.get("last_error", record.get("error", ""))).lower())
            history = record.get("mutation_history")
            self.assertIsInstance(history, list)
            assert isinstance(history, list)
            self.assertEqual(history[-1]["operation"], "CLOSE")
            self.assertIn(history[-1]["status"], {"ERROR", "UNKNOWN"})
            self.assertIn("close", str(history[-1].get("error", "")).lower())

    def test_approval_hash_is_single_use_after_ambiguous_submit(self) -> None:
        clock = _FakeClock(CANARY_START + timedelta(seconds=1))
        with tempfile.TemporaryDirectory(prefix="mtf-canary-boundary-") as directory:
            root = Path(directory)
            approval = _approval()
            first = _BoundaryExecutor(clock, fail_submit=True)
            with self.assertRaises(CanaryGateError):
                self._run(root, clock, first, approval=approval)

            ledger_path = root / "state" / "canary-approval-ledger.json"
            raw_text = ledger_path.read_text(encoding="utf-8")
            digest = hashlib.sha256(approval.approval_id.encode("utf-8")).hexdigest()
            self.assertIn(digest, raw_text)
            self.assertNotIn(approval.approval_id, raw_text)
            record = _ledger_record(root, approval)
            self.assertEqual(record["state"], "UNKNOWN")
            self.assertGreaterEqual(record["mutation_messages"], 1)

            second = _BoundaryExecutor(clock)
            with self.assertRaises(CanaryGateError):
                self._run(root, clock, second, approval=approval)
            self.assertEqual(second.submit_calls, 0)

    def test_trial_budget_is_anchored_and_first_cycle_total_includes_costs(self) -> None:
        clock = _FakeClock(CANARY_START + timedelta(seconds=1))
        with tempfile.TemporaryDirectory(prefix="mtf-canary-boundary-") as directory:
            root = Path(directory)
            approval = _approval()

            def after_first_close() -> None:
                # A later equity increase must not enlarge the trial budget
                # anchored by the first fresh DEMO snapshot.
                executor.equity = Decimal("20000")

            executor = _BoundaryExecutor(clock, plan_totals=(Decimal("5"), Decimal("6")), on_close=after_first_close)
            with self.assertRaises(CanaryGateError) as raised:
                self._run(root, clock, executor, approval=approval)
            self.assertIn("trial", str(raised.exception).lower())
            self.assertEqual(executor.submit_calls, 1)
            self.assertEqual(executor.close_calls, 1)
            record = _ledger_record(root, approval)
            self.assertEqual(record["trial_equity"], "10000")
            self.assertEqual(record["trial_loss_budget"], "10.000")
            # The plan's total is 3 risk + 2 observed cost; committing only
            # the initial risk would incorrectly admit the second cycle.
            self.assertEqual(record["trial_loss_committed"], "5")

    def test_binding_provenance_mismatch_blocks_before_any_open(self) -> None:
        cases: tuple[tuple[str, Callable[[dict[str, Any], _BoundaryExecutor], None], str], ...] = (
            (
                "account",
                lambda provenance, executor: provenance.update({"account_id": "5098"}),
                "account",
            ),
            (
                "symbol",
                lambda provenance, executor: provenance.update({"symbol": "GBP/USD"}),
                "symbol",
            ),
            (
                "generation",
                lambda provenance, executor: provenance.update({"connection_generation": "g2"}),
                "generation",
            ),
            (
                "endpoint",
                lambda provenance, executor: provenance.update({"endpoint": "demo.invalid:5035"}),
                "endpoint",
            ),
        )
        for label, mutate, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory(prefix="mtf-canary-boundary-") as directory:
                root = Path(directory)
                clock = _FakeClock(CANARY_START + timedelta(seconds=1))
                executor = _BoundaryExecutor(clock)
                provenance = _provenance()
                mutate(provenance, executor)
                with self.assertRaises(CanaryGateError) as raised:
                    self._run(root, clock, executor, provenance=provenance)
                self.assertIn(expected, str(raised.exception).lower())
                self.assertEqual(executor.submit_calls, 0)
                self.assertEqual(executor.plan_calls, 0)

    def test_sl_tp_are_exact_approval_values_and_wrong_plan_never_opens(self) -> None:
        approval = _approval()
        self.assertEqual(approval.stop_loss_atr_multiple, Decimal("1.5"))
        self.assertEqual(approval.take_profit_atr_multiple, Decimal("3"))
        validate_static_config(canonical_config(), approval)

        bad_config = canonical_config()
        bad_execution = dict(bad_config.execution)
        bad_policy = dict(bad_execution["risk_exit_policy"])
        bad_policy["stop_atr_multiple"] = "1.6"
        bad_execution["risk_exit_policy"] = bad_policy
        bad_config.execution = bad_execution
        with self.assertRaises(CanaryGateError):
            validate_static_config(bad_config, approval)
        with self.assertRaises(ValueError):
            dataclasses.replace(approval, take_profit_atr_multiple=Decimal("2.9"))

        clock = _FakeClock(CANARY_START + timedelta(seconds=1))
        with tempfile.TemporaryDirectory(prefix="mtf-canary-boundary-") as directory:
            root = Path(directory)
            executor = _BoundaryExecutor(clock, wrong_protection=True)
            with self.assertRaises(CanaryGateError) as raised:
                self._run(root, clock, executor, approval=approval)
            self.assertIn("sl", str(raised.exception).lower())
            self.assertEqual(executor.submit_calls, 0)

    def test_preflight_never_activates_or_mutates_canary_journal(self) -> None:
        clock = _FakeClock(CANARY_START + timedelta(seconds=1))
        with tempfile.TemporaryDirectory(prefix="mtf-canary-boundary-") as directory:
            root = Path(directory)
            executor = _BoundaryExecutor(clock)
            binding = _binding(executor)
            with (
                _isolated_environment(root),
                patch("tools.demo_canary.build_demo_execution_binding", return_value=binding) as build,
            ):
                result = run_demo_canary(
                    _FakeProvider(clock),
                    _provenance(),
                    canonical_config(),
                    state_dir=root / "state",
                    approval=_approval(approved=False),
                    inputs=_inputs(clock),
                    execute=False,
                    writer_store=_store(root),
                    risk_refresh=lambda _executor: {"state": "READY"},
                    market_window_state=lambda *_args: "OPEN",
                    clock=clock,
                )
            self.assertFalse(result.ok)
            self.assertEqual(result.state, "PREFLIGHT_RUNTIME_REQUIRED")
            build.assert_not_called()
            self.assertEqual(executor.submit_calls, 0)
            self.assertEqual(executor.close_calls, 0)
            self.assertFalse(executor.deactivated)
            self.assertFalse((root / "state" / "canary-approval-ledger.json").exists())
            self.assertFalse(any((root / "state").iterdir()) if (root / "state").exists() else False)

    def test_window_before_open_and_close_boundary_cannot_start_second_cycle(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-canary-boundary-") as directory:
            root = Path(directory)
            approval = _approval()
            before_clock = _FakeClock(CANARY_START - timedelta(seconds=1))
            before = _BoundaryExecutor(before_clock)
            with self.assertRaises(CanaryGateError) as raised:
                self._run(root, before_clock, before, approval=approval)
            self.assertIn("ventana", str(raised.exception).lower())
            self.assertEqual(before.submit_calls, 0)
            self.assertFalse((root / "state" / "canary-approval-ledger.json").exists())

            # Enter exactly one holding period before the frame end so the
            # holding gate passes and the closing-window gate is the one that
            # blocks the first cycle.
            close_clock = _FakeClock(CANARY_END - timedelta(seconds=300))

            def move_to_frame_end() -> None:
                close_clock.value = CANARY_END

            first = _BoundaryExecutor(close_clock, on_submit=move_to_frame_end)
            # The close check observes the frame end after the first OPEN;
            # CLOSE and the second OPEN must both be impossible.
            with self.assertRaises(CanaryGateError):
                self._run(root, close_clock, first, approval=approval)
            self.assertEqual(first.submit_calls, 1)
            self.assertEqual(first.close_calls, 0)
            self.assertIsNotNone(first.open_position)
            record = _ledger_record(root, approval)
            self.assertEqual(record["mutation_messages"], 1)

            second = _BoundaryExecutor(close_clock)
            with self.assertRaises(CanaryGateError):
                self._run(root, close_clock, second, approval=approval)
            self.assertEqual(second.submit_calls, 0)


if __name__ == "__main__":
    unittest.main()
