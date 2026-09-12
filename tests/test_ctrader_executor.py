from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.ops.ctrader_executor import (
    ActivationRequired,
    CorrelationError,
    CTraderDemoExecutor,
    DemoAccount,
    DemoAccountRequired,
    DemoTransport,
    DuplicateIntent,
    EndpointRejected,
    ExecutionPaused,
    ExecutionPolicy,
    ForeignPosition,
    JsonlIntentStore,
    MemoryIntentStore,
    OrderSnapshot,
    OrderState,
    Position,
    Quote,
    RealAccountForbidden,
    RiskLimitRejected,
    ScopeRejected,
    Side,
)


class Clock:
    def __init__(self, value: datetime):
        self.value = value

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


class CountingTransport(DemoTransport):
    def __init__(self, *args, intent_store: MemoryIntentStore | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.intent_store = intent_store
        self.submit_calls = 0

    def submit(self, intent, *, timeout_seconds):
        self.submit_calls += 1
        if self.intent_store is not None:
            # The durable intent must already exist before any transport call.
            assert intent.intent_id in self.intent_store.intents
        return super().submit(intent, timeout_seconds=timeout_seconds)


class CTraderExecutorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock(datetime(2025, 1, 1, 12, 0, tzinfo=UTC))
        self.account = DemoAccount(
            "demo-1", "DEMO", "demo://ctrader", frozenset({"trading"}), selected=True, verified=True
        )

    def make_executor(
        self,
        *,
        behavior: str = "full",
        policy: ExecutionPolicy | None = None,
        store: MemoryIntentStore | None = None,
        transport=None,
    ) -> CTraderDemoExecutor:
        store = store or MemoryIntentStore()
        transport = transport or CountingTransport(
            account_id=self.account.account_id,
            endpoint=self.account.endpoint,
            scopes=self.account.scopes,
            default_behavior=behavior,
            clock=self.clock,
            intent_store=store,
        )
        executor = CTraderDemoExecutor(
            self.account,
            policy=policy
            or ExecutionPolicy(
                max_quantity=2,
                fixed_quantity=1,
                max_positions=2,
                max_exposure=10000,
                max_spread=0.2,
                max_price_age_seconds=10,
                timeout_seconds=1,
            ),
            transport=transport,
            intent_store=store,
            clock=self.clock,
        )
        return executor

    def quote(self, *, bid: float = 99.9, ask: float = 100.1, quality: str = "VALID") -> Quote:
        return Quote("EUR/USD", bid, ask, self.clock(), available_at=self.clock(), quality=quality, source="fixture")

    @staticmethod
    def signal(signal_id: str = "s-1", direction: str = "UP") -> dict:
        return {"signal_id": signal_id, "instrument": "EUR/USD", "direction": direction, "mode": "DEMO"}

    def test_real_is_rejected_before_malformed_credentials_or_endpoint(self) -> None:
        with self.assertRaises(RealAccountForbidden):
            CTraderDemoExecutor({"environment": "REAL", "endpoint": None, "account_id": None, "token": object()})
        with self.assertRaises(RealAccountForbidden):
            CTraderDemoExecutor({"environment": "LIVE/REAL", "endpoint": "not-a-url", "scopes": "not-a-set"})

    def test_demo_account_endpoint_scope_and_activation_gates(self) -> None:
        with self.assertRaises(DemoAccountRequired):
            CTraderDemoExecutor(
                DemoAccount("d", "DEMO", "demo://ctrader", frozenset({"trading"}), selected=False, verified=True)
            )
        with self.assertRaises(EndpointRejected):
            CTraderDemoExecutor(
                DemoAccount(
                    "d", "DEMO", "https://api.ctrader.com", frozenset({"trading"}), selected=True, verified=True
                )
            )
        with self.assertRaises(ScopeRejected):
            CTraderDemoExecutor(DemoAccount("d", "DEMO", "demo://ctrader", frozenset(), selected=True, verified=True))
        executor = self.make_executor()
        with self.assertRaises(ActivationRequired):
            executor.submit_signal(self.signal(), self.quote())
        self.assertFalse(executor.active)
        self.assertTrue(executor.activate()["active"])

    def test_intent_is_recorded_before_transport_and_full_fill_is_numeric(self) -> None:
        store = MemoryIntentStore()
        transport = CountingTransport(
            account_id="demo-1", endpoint="demo://ctrader", scopes={"trading"}, clock=self.clock, intent_store=store
        )
        executor = self.make_executor(store=store, transport=transport)
        executor.activate()
        result = executor.submit_signal(self.signal(), self.quote())
        self.assertEqual(result.state, OrderState.FILLED)
        self.assertEqual(result.filled_quantity, 1.0)
        self.assertEqual(result.remaining_quantity, 0.0)
        self.assertEqual(transport.submit_calls, 1)
        self.assertIn(result.intent.intent_id, store.intents)
        self.assertEqual(store.events[0]["event_type"], "ACTIVATED")
        self.assertEqual(store.events[1]["event_type"], "INTENT_RECORDED")
        self.assertEqual(store.events[1]["details"]["before_transport"], True)
        self.assertEqual(len(executor.positions()), 1)

    def test_partial_fill_then_reconciliation_completes_without_resubmission(self) -> None:
        store = MemoryIntentStore()
        transport = CountingTransport(
            account_id="demo-1",
            endpoint="demo://ctrader",
            scopes={"trading"},
            default_behavior="partial",
            clock=self.clock,
            intent_store=store,
        )
        executor = self.make_executor(store=store, transport=transport)
        executor.activate()
        result = executor.submit_signal(self.signal("partial"), self.quote())
        self.assertEqual(result.state, OrderState.PARTIAL)
        self.assertAlmostEqual(result.filled_quantity, 0.5)
        self.assertEqual(transport.submit_calls, 1)
        transport.advance_partial(result.intent.intent_id)
        reconciled = executor.manage()[0]
        self.assertEqual(reconciled.state, OrderState.FILLED)
        self.assertEqual(transport.submit_calls, 1)
        self.assertAlmostEqual(reconciled.filled_quantity, 1.0)
        self.assertAlmostEqual(executor.positions()[0].quantity, 1.0)

    def test_rejection_and_timeout_are_distinct_and_timeout_is_not_retried(self) -> None:
        rejected = self.make_executor(behavior="reject")
        rejected.activate()
        rejection = rejected.submit_signal(self.signal("reject"), self.quote())
        self.assertEqual(rejection.state, OrderState.REJECTED)
        self.assertEqual(rejected.positions(), ())
        transport = CountingTransport(
            account_id="demo-1",
            endpoint="demo://ctrader",
            scopes={"trading"},
            default_behavior="timeout",
            clock=self.clock,
        )
        executor = self.make_executor(transport=transport)
        executor.activate()
        unknown = executor.submit_signal(self.signal("timeout"), self.quote())
        self.assertEqual(unknown.state, OrderState.UNKNOWN)
        self.assertEqual(transport.submit_calls, 1)
        self.assertTrue(unknown.unknown_reason)
        checked = executor.reconcile(unknown.intent.intent_id)
        self.assertEqual(checked.state, OrderState.UNKNOWN)
        self.assertTrue(checked.reconciled)
        self.assertEqual(transport.submit_calls, 1)
        with self.assertRaises((DuplicateIntent, RiskLimitRejected)):
            executor.submit_signal(self.signal("timeout"), self.quote())

    def test_risk_caps_quality_spread_stale_exposure_and_positions(self) -> None:
        policy = ExecutionPolicy(
            max_quantity=1, fixed_quantity=1, max_positions=1, max_exposure=50, max_spread=0.1, max_price_age_seconds=2
        )
        executor = self.make_executor(policy=policy)
        executor.activate()
        with self.assertRaises(RiskLimitRejected):
            executor.submit_signal(self.signal("spread"), self.quote(bid=99, ask=101))
        with self.assertRaises(RiskLimitRejected):
            executor.submit_signal(self.signal("bad-quality"), self.quote(quality="DISCONNECTED"))
        self.clock.advance(3)
        with self.assertRaises(RiskLimitRejected):
            executor.submit_signal(self.signal("stale"), self.quote())
        self.clock.advance(-3)
        with self.assertRaises(RiskLimitRejected):
            executor.submit_signal(self.signal("exposure"), self.quote(bid=99.9, ask=100.1))
        # Same policy with enough exposure: one open position consumes the
        # position cap and a second distinct signal is rejected.
        executor = self.make_executor(
            policy=ExecutionPolicy(
                max_quantity=1,
                fixed_quantity=1,
                max_positions=1,
                max_exposure=1000,
                max_spread=0.2,
                max_price_age_seconds=10,
            )
        )
        executor.activate()
        executor.submit_signal(self.signal("first"), self.quote())
        with self.assertRaises(RiskLimitRejected):
            executor.submit_signal(self.signal("second"), self.quote())

    def test_pause_blocks_new_intents_but_management_and_own_close_continue(self) -> None:
        executor = self.make_executor()
        executor.activate()
        opened = executor.submit_signal(self.signal("to-close"), self.quote())
        self.assertEqual(opened.state, OrderState.FILLED)
        position_id = executor.positions()[0].position_id
        foreign_id = executor.transport.add_foreign_position()
        executor.pause("manual review")
        with self.assertRaises(ExecutionPaused):
            executor.submit_signal(self.signal("paused"), self.quote())
        with self.assertRaises(ForeignPosition):
            executor.close_position(foreign_id)
        closed = executor.close_position(position_id)
        self.assertEqual(closed.state, OrderState.CLOSED)
        self.assertEqual(executor.positions(), ())
        self.assertTrue(executor.manage() == ())
        executor.resume()
        self.assertFalse(executor.paused)

    def test_no_martingale_and_deterministic_fixture(self) -> None:
        with self.assertRaises(ValueError):
            ExecutionPolicy(no_martingale=False)
        first = self.make_executor()
        first.activate()
        result_a = first.submit_signal(self.signal("same"), self.quote())
        second = self.make_executor()
        second.activate()
        result_b = second.submit_signal(self.signal("same"), self.quote())
        self.assertEqual(result_a.intent.intent_id, result_b.intent.intent_id)
        self.assertEqual(result_a.order_id, result_b.order_id)
        self.assertEqual(result_a.to_dict()["fills"], result_b.to_dict()["fills"])

    def test_persistence_failure_is_fail_closed_and_correlation_mismatch_is_rejected(self) -> None:
        class BrokenIntentStore:
            def record_intent(self, intent):
                raise OSError("journal unavailable")

            def record_event(self, event):
                return None

            def update_intent(self, intent_id, update):
                return None

        store = BrokenIntentStore()
        transport = CountingTransport(
            account_id="demo-1", endpoint="demo://ctrader", scopes={"trading"}, clock=self.clock
        )
        executor = self.make_executor(store=store, transport=transport)
        executor.activate()
        with self.assertRaises(OSError):
            executor.submit_signal(self.signal("journal-failure"), self.quote())
        self.assertEqual(transport.submit_calls, 0)

        class WrongCorrelationTransport(CountingTransport):
            def submit(self, intent, *, timeout_seconds):
                self.submit_calls += 1
                return OrderSnapshot(
                    "order-wrong",
                    "other-client-id",
                    OrderState.FILLED,
                    intent.quantity,
                    intent.quantity,
                    observed_at=self.clock(),
                )

        transport2 = WrongCorrelationTransport(
            account_id="demo-1", endpoint="demo://ctrader", scopes={"trading"}, clock=self.clock
        )
        executor2 = self.make_executor(transport=transport2)
        executor2.activate()
        with self.assertRaises(CorrelationError):
            executor2.submit_signal(self.signal("bad-correlation"), self.quote())
        self.assertEqual(executor2.positions(), ())

    def test_same_account_foreign_owner_is_not_closeable(self) -> None:
        executor = self.make_executor()
        executor.activate()
        pid = "foreign-same-account"
        executor.transport.positions_by_id[pid] = Position(
            pid, "demo-1", "EUR/USD", Side.BUY, 1, 100, "external-order", owner="other-owner"
        )
        self.assertEqual(executor.positions(), ())
        with self.assertRaises(ForeignPosition):
            executor.close_position(pid)

    def test_jsonl_intent_store_flushes_intent_and_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "intent.jsonl"
            store = JsonlIntentStore(path)
            executor = self.make_executor(store=store)
            executor.activate()
            result = executor.submit_signal(self.signal("journal"), self.quote())
            store.close()
            rows = [line for line in path.read_text(encoding="utf-8").splitlines() if line]
            self.assertGreaterEqual(len(rows), 4)
            self.assertIn('"journal_type": "intent"', rows[1])
            self.assertIn(result.intent.intent_id, path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
