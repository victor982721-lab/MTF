from __future__ import annotations

import unittest
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DecimalValue,
    DemoAccount,
    DemoTransport,
    ExecutionPolicy,
    OrderState,
    Position,
    Quote,
)


class FixedClock:
    def __init__(self, value: datetime):
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class CountingTransport(DemoTransport):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.list_positions_calls = 0
        self.connection_generation = "generation-1"

    def list_positions(self, account_id: str) -> Sequence[Position]:
        self.list_positions_calls += 1
        return super().list_positions(account_id)


class ExecutorStatusCacheTests(unittest.TestCase):
    NOW = datetime(2025, 1, 1, 12, tzinfo=UTC)

    def setUp(self) -> None:
        self.clock = FixedClock(self.NOW)
        self.account = DemoAccount(
            "demo-cache",
            "DEMO",
            "demo://ctrader",
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )
        self.transport = CountingTransport(
            account_id=self.account.account_id,
            endpoint=self.account.endpoint,
            scopes=self.account.scopes,
            clock=self.clock,
        )
        self.executor = CTraderDemoExecutor(
            self.account,
            policy=ExecutionPolicy(
                max_quantity=DecimalValue("1"),
                fixed_quantity=DecimalValue("1"),
                max_positions=2,
                max_exposure=DecimalValue("10000"),
                max_spread=DecimalValue("1"),
                max_price_age_seconds=DecimalValue("30"),
            ),
            transport=self.transport,
            clock=self.clock,
        )

    def quote(self) -> Quote:
        return Quote(
            "EUR/USD",
            DecimalValue("99.9"),
            DecimalValue("100.0"),
            self.clock(),
            available_at=self.clock(),
            source="fixture",
        )

    def signal(self, signal_id: str = "cache-signal") -> dict[str, str]:
        return {
            "signal_id": signal_id,
            "instrument": "EUR/USD",
            "direction": "UP",
            "mode": "DEMO",
        }

    def test_native_client_generation_precedes_frozen_gateway_proof(self) -> None:
        del self.transport.connection_generation
        live_client = SimpleNamespace(generation=1)
        self.transport.client = SimpleNamespace(client=live_client, session=SimpleNamespace(connection_generation=1))
        self.executor.status()
        before = self.transport.list_positions_calls
        live_client.generation = 2
        result = self.executor.cached_status()
        self.assertEqual(result["risk"]["position_state"], "UNKNOWN")
        self.assertIsNone(result["risk"]["exposure"])
        self.assertEqual(self.transport.list_positions_calls, before)

    def test_one_fresh_status_query_then_one_hundred_cached_calls(self) -> None:
        self.executor.activate()
        baseline_before = self.transport.list_positions_calls
        fresh = self.executor.status()
        self.assertEqual(self.transport.list_positions_calls, baseline_before + 1)
        baseline_before = self.transport.list_positions_calls
        for _ in range(100):
            fresh = self.executor.status()
        self.assertEqual(self.transport.list_positions_calls, baseline_before + 100)

        cached = self.executor.cached_status()
        self.assertEqual(cached["risk"], fresh["risk"])
        self.assertTrue(cached["risk"]["positions_fresh"])

        before_cached = self.transport.list_positions_calls
        for _ in range(100):
            self.assertEqual(self.executor.cached_status()["risk"], fresh["risk"])
        self.assertEqual(self.transport.list_positions_calls, before_cached)

    def test_cache_unknown_never_reports_zero_exposure(self) -> None:
        missing = self.executor.cached_status()
        self.assertEqual(missing["risk"]["state"], "UNKNOWN")
        self.assertEqual(missing["risk"]["position_state"], "UNKNOWN")
        self.assertFalse(missing["risk"]["positions_fresh"])
        self.assertIsNone(missing["risk"]["exposure"])
        self.assertEqual(self.transport.list_positions_calls, 0)

        self.executor.activate()
        observed_calls = self.transport.list_positions_calls
        self.clock.value = self.NOW + timedelta(seconds=31)
        stale = self.executor.cached_status()
        self.assertEqual(stale["risk"]["state"], "UNKNOWN")
        self.assertEqual(stale["risk"]["position_cache_reason"], "position_snapshot_stale")
        self.assertIsNone(stale["risk"]["exposure"])
        self.assertEqual(self.transport.list_positions_calls, observed_calls)

        self.clock.value = self.NOW - timedelta(seconds=1)
        future = self.executor.cached_status()
        self.assertEqual(future["risk"]["state"], "UNKNOWN")
        self.assertEqual(future["risk"]["position_cache_reason"], "position_snapshot_future")

        self.clock.value = self.NOW
        self.transport.connection_generation = "generation-2"
        changed_generation = self.executor.cached_status()
        self.assertEqual(changed_generation["risk"]["state"], "UNKNOWN")
        self.assertEqual(changed_generation["risk"]["position_cache_reason"], "position_snapshot_generation_mismatch")

    def test_refresh_argument_keeps_status_fresh_and_open_close_gates_fresh(self) -> None:
        self.executor.activate()
        before = self.transport.list_positions_calls
        cached = self.executor.status(refresh=False)
        self.assertEqual(self.transport.list_positions_calls, before)
        self.assertTrue(cached["positions_fresh"])

        refreshed = self.executor.risk_status(refresh=True)
        self.assertEqual(self.transport.list_positions_calls, before + 1)
        self.assertTrue(refreshed["positions_fresh"])

        before_open = self.transport.list_positions_calls
        result = self.executor.submit_signal(self.signal(), self.quote())
        self.assertEqual(result.state, OrderState.FILLED)
        self.assertGreater(self.transport.list_positions_calls, before_open)

        position_id = result.position_ids[0]
        before_close = self.transport.list_positions_calls
        closed = self.executor.close_position(position_id)
        self.assertEqual(closed.state, OrderState.CLOSED)
        self.assertGreater(self.transport.list_positions_calls, before_close)

    def test_fresh_status_detects_new_foreign_position_after_cached_projection(self) -> None:
        self.executor.activate()
        before = self.transport.list_positions_calls
        self.transport.add_foreign_position(account_id=self.account.account_id)

        cached = self.executor.cached_status()
        self.assertEqual(self.transport.list_positions_calls, before)
        self.assertEqual(cached["risk"]["foreign_position_count"], 0)

        fresh = self.executor.status()
        self.assertEqual(self.transport.list_positions_calls, before + 1)
        self.assertEqual(fresh["risk"]["foreign_position_count"], 1)
        self.assertEqual(fresh["risk"]["position_count"], 1)


if __name__ == "__main__":
    unittest.main()
