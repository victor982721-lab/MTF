from __future__ import annotations

import dataclasses
import tempfile
import unittest
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path

from mtf_lab.ops.ctrader_demo_composition import recover_execution_intents
from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DecimalValue,
    DemoAccount,
    DemoTransport,
    ExecutionIntent,
    ExecutionPolicy,
    Fill,
    JsonlIntentStore,
    OrderResult,
    OrderSnapshot,
    OrderState,
    Position,
    Quote,
)


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value


class CountingTransport(DemoTransport):
    def __init__(
        self,
        *,
        account_id: str = "demo-account",
        endpoint: str = "demo://ctrader",
        scopes: Iterable[str] = ("trading",),
        default_behavior: str = "full",
        partial_ratio: float = 0.5,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(
            account_id=account_id,
            endpoint=endpoint,
            scopes=scopes,
            default_behavior=default_behavior,
            partial_ratio=partial_ratio,
            clock=clock,
        )
        self.open_calls = 0
        self.close_calls = 0

    def submit(self, intent: ExecutionIntent, *, timeout_seconds: float) -> OrderSnapshot:
        self.open_calls += 1
        return super().submit(intent, timeout_seconds=timeout_seconds)

    def close_position(
        self,
        position: Position,
        *,
        client_order_id: str,
        timeout_seconds: float,
    ) -> OrderSnapshot:
        self.close_calls += 1
        return super().close_position(
            position,
            client_order_id=client_order_id,
            timeout_seconds=timeout_seconds,
        )


class ContradictoryCloseTransport(CountingTransport):
    """Return CLOSED while the account snapshot still proves a residual."""

    def close_position(
        self,
        position: Position,
        *,
        client_order_id: str,
        timeout_seconds: float,
    ) -> OrderSnapshot:
        snapshot = super().close_position(
            position,
            client_order_id=client_order_id,
            timeout_seconds=timeout_seconds,
        )
        if snapshot.status is OrderState.CLOSED:
            with self._lock:
                self.positions_by_id[position.position_id] = dataclasses.replace(
                    position,
                    quantity=DecimalValue("0.25"),
                )
        return snapshot


class DeltaSnapshotTransport(CountingTransport):
    def __init__(
        self,
        *,
        account_id: str = "demo-account",
        endpoint: str = "demo://ctrader",
        scopes: Iterable[str] = ("trading",),
        default_behavior: str = "full",
        partial_ratio: float = 0.5,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        super().__init__(
            account_id=account_id,
            endpoint=endpoint,
            scopes=scopes,
            default_behavior=default_behavior,
            partial_ratio=partial_ratio,
            clock=clock,
        )
        self.reconcile_snapshot: OrderSnapshot | None = None

    def get_order(self, client_order_id: str) -> OrderSnapshot | None:
        if self.reconcile_snapshot is not None:
            return self.reconcile_snapshot
        return super().get_order(client_order_id)


class CancelResponseTransport(CountingTransport):
    cancel_snapshot: OrderSnapshot | None = None
    cancel_calls = 0

    def cancel_order(
        self,
        order_id: str,
        *,
        client_order_id: str | None = None,
        timeout_seconds: float,
    ) -> OrderSnapshot:
        del order_id, client_order_id, timeout_seconds
        self.cancel_calls += 1
        if self.cancel_snapshot is None:
            raise AssertionError("test cancellation snapshot was not configured")
        return self.cancel_snapshot


class ExecutorRecoveryInvariantTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.account = DemoAccount(
            "demo-1",
            "DEMO",
            "demo://ctrader",
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )

    def policy(self) -> ExecutionPolicy:
        return ExecutionPolicy(
            max_quantity=DecimalValue("2"),
            fixed_quantity=DecimalValue("1"),
            max_positions=2,
            max_exposure=DecimalValue("10000"),
            max_spread=DecimalValue("0.2"),
            max_price_age_seconds=DecimalValue("10"),
            timeout_seconds=DecimalValue("1"),
        )

    def quote(self) -> Quote:
        return Quote(
            "EUR/USD",
            DecimalValue("99.9"),
            DecimalValue("100.1"),
            self.clock(),
            available_at=self.clock(),
            quality="VALID",
            source="fixture",
        )

    def open_signal(self, signal_id: str) -> dict[str, str]:
        return {
            "signal_id": signal_id,
            "instrument": "EUR/USD",
            "direction": "UP",
            "mode": "DEMO",
        }

    def executor(self, transport: DemoTransport, store: JsonlIntentStore | None = None) -> CTraderDemoExecutor:
        return CTraderDemoExecutor(
            self.account,
            policy=self.policy(),
            transport=transport,
            intent_store=store,
            clock=self.clock,
        )

    def partial_cancel_fixture(self) -> tuple[CancelResponseTransport, CTraderDemoExecutor, OrderResult]:
        transport = CancelResponseTransport(
            account_id=self.account.account_id,
            endpoint=self.account.endpoint,
            scopes=self.account.scopes,
            default_behavior="partial",
            clock=self.clock,
        )
        executor = self.executor(transport)
        executor.activate()
        partial = executor.submit_signal(self.open_signal("cancel"), self.quote())
        return transport, executor, partial

    def test_closed_ack_with_residual_is_unknown_and_preserves_server_fill_evidence(self) -> None:
        transport = ContradictoryCloseTransport(
            account_id=self.account.account_id,
            endpoint=self.account.endpoint,
            scopes=self.account.scopes,
            clock=self.clock,
        )
        with tempfile.TemporaryDirectory() as raw_tmp:
            store = JsonlIntentStore(Path(raw_tmp) / "intents.jsonl")
            try:
                executor = self.executor(transport, store)
                executor.activate()
                opened = executor.submit_signal(self.open_signal("residual"), self.quote())
                position_id = executor.positions()[0].position_id

                result = executor.close_position(position_id)

                self.assertEqual(result.state, OrderState.UNKNOWN)
                self.assertEqual(result.filled_quantity, DecimalValue("1"))
                self.assertEqual(
                    [fill.fill_id for fill in result.fills], ["close-" + result.intent.intent_id + "-fill"]
                )
                self.assertEqual(sum((fill.quantity for fill in result.fills), DecimalValue("0")), DecimalValue("1"))
                self.assertEqual(executor.positions()[0].quantity, DecimalValue("0.25"))
                self.assertEqual(transport.close_calls, 1)
                self.assertEqual(store.updates[-1]["state"], OrderState.UNKNOWN.value)
                self.assertEqual(store.updates[-1]["filled_quantity"], "1")
                self.assertEqual(executor.events()[-1].event_type, "CLOSE_RESIDUAL_POSITION")
                self.assertEqual(executor.events()[-1].state, OrderState.UNKNOWN)

                repeated = executor.reconcile(result.intent.intent_id)
                self.assertEqual(repeated.state, OrderState.UNKNOWN)
                self.assertEqual(repeated.filled_quantity, DecimalValue("1"))
                self.assertEqual(transport.close_calls, 1)
                self.assertEqual(opened.state, OrderState.FILLED)
            finally:
                store.close()

    def test_unknown_close_restart_reconciles_late_ack_without_second_send(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            journal_path = Path(raw_tmp) / "intents.jsonl"
            transport = CountingTransport(
                account_id=self.account.account_id,
                endpoint=self.account.endpoint,
                scopes=self.account.scopes,
                clock=self.clock,
            )
            first_store = JsonlIntentStore(journal_path)
            try:
                first = self.executor(transport, first_store)
                first.activate()
                first.submit_signal(self.open_signal("restart"), self.quote())
                position_id = first.positions()[0].position_id
                transport.queue_behavior("timeout")
                unknown = first.close_position(position_id)
                self.assertEqual(unknown.state, OrderState.UNKNOWN)
                close_intent_id = unknown.intent.intent_id
                self.assertEqual(transport.close_calls, 1)
            finally:
                first_store.close()

            resumed_store = JsonlIntentStore(journal_path)
            try:
                resumed = self.executor(transport, resumed_store)
                recover_execution_intents(journal_path, resumed)

                before_ack = resumed.reconcile(close_intent_id)
                self.assertEqual(before_ack.state, OrderState.UNKNOWN)
                self.assertEqual(transport.close_calls, 1)

                transport.orders[close_intent_id] = OrderSnapshot(
                    "close-late-order",
                    "wrong-correlation",
                    OrderState.CLOSED,
                    DecimalValue("1"),
                    DecimalValue("1"),
                    (
                        Fill(
                            "late-fill",
                            DecimalValue("1"),
                            DecimalValue("100.1"),
                            self.clock(),
                        ),
                    ),
                    position_ids=(position_id,),
                    observed_at=self.clock(),
                )
                wrong = resumed.reconcile(close_intent_id)
                self.assertEqual(wrong.state, OrderState.UNKNOWN)
                self.assertEqual(transport.close_calls, 1)

                transport.orders[close_intent_id] = OrderSnapshot(
                    "close-late-order",
                    close_intent_id,
                    OrderState.CLOSED,
                    DecimalValue("1"),
                    DecimalValue("1"),
                    (
                        Fill(
                            "late-fill",
                            DecimalValue("1"),
                            DecimalValue("100.1"),
                            self.clock(),
                        ),
                    ),
                    position_ids=(position_id,),
                    observed_at=self.clock(),
                )
                transport.positions_by_id.pop(position_id)
                late = resumed.reconcile(close_intent_id)
                self.assertEqual(late.state, OrderState.CLOSED)
                self.assertEqual(late.filled_quantity, DecimalValue("1"))
                self.assertEqual(transport.close_calls, 1)
            finally:
                resumed_store.close()

    def test_delta_snapshot_is_rejected_and_prior_evidence_is_not_merged_or_relabelled(self) -> None:
        transport = DeltaSnapshotTransport(
            account_id=self.account.account_id,
            endpoint=self.account.endpoint,
            scopes=self.account.scopes,
            default_behavior="partial",
            clock=self.clock,
        )
        executor = self.executor(transport)
        executor.activate()
        partial = executor.submit_signal(self.open_signal("delta"), self.quote())
        self.assertEqual(partial.state, OrderState.PARTIAL)
        first_fill = partial.fills[0]
        transport.reconcile_snapshot = OrderSnapshot(
            partial.order_id,
            partial.intent.intent_id,
            OrderState.FILLED,
            DecimalValue("1"),
            DecimalValue("1"),
            (
                Fill(
                    "delta-only",
                    DecimalValue("0.5"),
                    first_fill.price,
                    self.clock(),
                ),
            ),
            position_ids=partial.position_ids,
            observed_at=self.clock(),
        )

        rejected = executor.reconcile(partial.intent.intent_id)

        self.assertEqual(rejected.state, OrderState.UNKNOWN)
        self.assertEqual(rejected.filled_quantity, DecimalValue("0.5"))
        self.assertEqual([fill.fill_id for fill in rejected.fills], [first_fill.fill_id])
        self.assertIn("omitted previously observed fills", rejected.unknown_reason or "")
        self.assertEqual(transport.open_calls, 1)
        self.assertEqual(executor.reconcile(partial.intent.intent_id).state, OrderState.UNKNOWN)

    def test_cancel_same_quantity_without_fills_preserves_cumulative_ledger(self) -> None:
        transport, executor, partial = self.partial_cancel_fixture()
        transport.cancel_snapshot = OrderSnapshot(
            partial.order_id,
            partial.intent.intent_id,
            OrderState.CANCELLED,
            partial.intent.quantity,
            partial.filled_quantity,
            (),
            position_ids=partial.position_ids,
            observed_at=self.clock(),
        )

        cancelled = executor.cancel_pending()[0]

        self.assertEqual(cancelled.state, OrderState.CANCELLED)
        self.assertEqual(cancelled.filled_quantity, partial.filled_quantity)
        self.assertEqual(cancelled.fills, partial.fills)
        self.assertEqual(transport.cancel_calls, 1)

    def test_cancel_higher_quantity_unions_subset_and_new_fills_deterministically(self) -> None:
        transport, executor, partial = self.partial_cancel_fixture()
        first_fill = partial.fills[0]
        new_fill = Fill("new-cancel-fill", DecimalValue("0.25"), first_fill.price, self.clock())
        transport.cancel_snapshot = OrderSnapshot(
            partial.order_id,
            partial.intent.intent_id,
            OrderState.CANCELLED,
            partial.intent.quantity,
            DecimalValue("0.75"),
            (new_fill,),
            position_ids=partial.position_ids,
            observed_at=self.clock(),
        )

        cancelled = executor.cancel_pending()[0]

        expected_fills = tuple(sorted((first_fill, new_fill), key=lambda fill: (fill.timestamp, str(fill.fill_id))))
        self.assertEqual(cancelled.state, OrderState.CANCELLED)
        self.assertEqual(cancelled.filled_quantity, DecimalValue("0.75"))
        self.assertEqual(cancelled.fills, expected_fills)
        self.assertEqual(sum((fill.quantity for fill in cancelled.fills), DecimalValue("0")), DecimalValue("0.75"))
        self.assertEqual(transport.cancel_calls, 1)

    def test_cancel_conflict_or_overfill_is_unknown_and_not_retried(self) -> None:
        for case in ("conflicting_fill", "merged_overfill"):
            with self.subTest(case=case):
                transport, executor, partial = self.partial_cancel_fixture()
                first_fill = partial.fills[0]
                if case == "conflicting_fill":
                    fills = (Fill(first_fill.fill_id, first_fill.quantity, DecimalValue("101"), first_fill.timestamp),)
                    filled_quantity = partial.filled_quantity
                else:
                    fills = (Fill("overfill", DecimalValue("0.4"), first_fill.price, self.clock()),)
                    filled_quantity = DecimalValue("0.75")
                transport.cancel_snapshot = OrderSnapshot(
                    partial.order_id,
                    partial.intent.intent_id,
                    OrderState.CANCELLED,
                    partial.intent.quantity,
                    filled_quantity,
                    fills,
                    position_ids=partial.position_ids,
                    observed_at=self.clock(),
                )

                unresolved = executor.cancel_pending()[0]

                self.assertEqual(unresolved.state, OrderState.UNKNOWN)
                self.assertEqual(unresolved.filled_quantity, partial.filled_quantity)
                self.assertEqual(unresolved.fills, partial.fills)
                self.assertEqual(transport.cancel_calls, 1)
                self.assertEqual(executor.cancel_pending(), ())

    def test_order_snapshot_rejects_duplicate_fill_ids(self) -> None:
        fill = Fill("same", DecimalValue("1"), DecimalValue("100"), self.clock())
        with self.assertRaisesRegex(ValueError, "duplicate fill_id"):
            OrderSnapshot(
                "order-1",
                "intent-1",
                OrderState.FILLED,
                DecimalValue("1"),
                DecimalValue("1"),
                (fill, fill),
                observed_at=self.clock(),
            )


if __name__ == "__main__":
    unittest.main()
