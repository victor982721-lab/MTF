from __future__ import annotations

import unittest
from datetime import UTC, datetime
from decimal import localcontext
from types import SimpleNamespace
from typing import Any

from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DemoAccount,
    DemoTransport,
    ExecutionPolicy,
    Quote,
    RealAccountForbidden,
)
from mtf_lab.ops.supervision_demo import _callbacks_for
from mtf_lab.ops.volume_rules import VolumeGrid, VolumeRuleError


class ExecutionRiskIntegrationTests(unittest.TestCase):
    def test_zero_used_margin_is_distinct_from_unknown(self) -> None:
        account = DemoAccount("demo-1", "DEMO", "demo://ctrader", frozenset({"trading"}), selected=True, verified=True)
        executor = CTraderDemoExecutor(
            account,
            transport=DemoTransport(account_id="demo-1"),
            policy=ExecutionPolicy(min_margin_level=500),
            fixture_mode=True,
        )
        known = executor.update_risk_metrics(equity=1000, used_margin=0, margin_level=None)
        self.assertIsNone(known["halt_reason"])
        self.assertIsNone(known["metrics"]["margin_level"])
        unknown = executor.update_risk_metrics(used_margin=None, margin_level=None)
        self.assertEqual(unknown["halt_reason"], "margin_level_unobserved")

    def test_volume_representation_is_exact_under_low_precision(self) -> None:
        grid = VolumeGrid(min_volume=100, max_volume=1_000_000, step_volume=100)
        with localcontext() as context:
            context.prec = 2
            self.assertEqual(grid.validate_open_quantity("1001"), 100100)
            with self.assertRaises(VolumeRuleError):
                grid.validate_open_quantity("1001.000000000000000000000001")

    def test_live_data_is_not_relabelled_as_a_real_account(self) -> None:
        seen: list[dict[str, Any]] = []
        executor = SimpleNamespace(
            submit_signal=lambda data, quote: seen.append(data),
            manage=lambda: [],
            reconcile_positions=lambda: {"state": "VALID"},
            reduce_exposure=lambda: {},
        )
        callbacks = _callbacks_for(executor, object(), SimpleNamespace(environment="DEMO"))
        quote = Quote("EUR/USD", 1.1, 1.1002, datetime.now(UTC))
        callbacks.on_signal({"signal_id": "s", "mode": "LIVE", "direction": "UP"}, quote)
        self.assertEqual(seen[0]["data_mode"], "LIVE")
        self.assertEqual(seen[0]["account_environment"], "DEMO")
        with self.assertRaises(RealAccountForbidden):
            callbacks.on_signal({"signal_id": "x", "account_environment": "REAL"}, quote)
