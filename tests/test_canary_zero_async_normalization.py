"""Compose authorized equality with real cached-leg proof before the issue gate."""

from __future__ import annotations

import os
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from mtf_lab.data.ctrader import CTraderConfig, CTraderProvider, DeterministicTransport, WireMessage
from mtf_lab.data.ctrader_protocol import PAYLOAD
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.ops.ctrader_canary_inputs import collect_canary_inputs
from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor, DemoAccount, DemoTransport, ExecutionPolicy
from tests.test_canary_positive_async_quotes import _technical_config
from tests.test_canary_zero_spread_inputs import BASE, _authorization, _evidence, _QueuedDemoClient


class CanaryZeroAsyncNormalizationTests(unittest.TestCase):
    def _collect(self, *, authorized: bool, extra_issue: bool = False):
        clock_value = [BASE]
        client = _QueuedDemoClient(clock_value)
        provider = CTraderProvider(
            CTraderConfig(symbol="EUR/USD", symbol_id=99, account_id=7, quote_basis="mid"),
            client=client,
            transport=DeterministicTransport(),
            clock=lambda: clock_value[0],
            max_quote_age_seconds=30,
        )
        provider.reset_generation(1)
        end = BASE + timedelta(minutes=25)
        authorization = _authorization(window_end=end) if authorized else None
        count = 105 if authorized and not extra_issue else 8
        for index in range(count):
            at = BASE + timedelta(seconds=5 + 10 * index)
            values = {"ctidTraderAccountId": 7, "symbolId": 99, "timestamp": int(at.timestamp() * 1000)}
            if index == 0:
                values.update({"bid": 110_000, "ask": 110_000})
            elif index % 2:
                values["ask"] = 110_000 + (index + 1) // 2
            else:
                values["bid"] = 110_000 + index // 2
            client.push(
                WireMessage(
                    PAYLOAD["PROTO_OA_SPOT_EVENT"],
                    proto.ProtoOASpotEvent(**values),
                    is_event=True,
                    received_at=at + timedelta(seconds=1),
                    available_at=at + timedelta(seconds=1),
                    ingest_sequence=index,
                    connection_generation=1,
                    source_identity="zero-async-official-protobuf-fixture",
                )
            )
        normalize = provider.normalize_spot
        if extra_issue:

            def with_extra_issue(*args, **kwargs):
                result = normalize(*args, **kwargs)
                return replace(result, issues=(*result.issues, "unrelated source normalization failure"))

            provider.normalize_spot = with_extra_issue
        account = DemoAccount("diagnostic", "DEMO", "demo://ctrader", frozenset({"trading"}), True, True)
        executor = CTraderDemoExecutor(
            account,
            transport=DemoTransport(account_id="diagnostic", endpoint="demo://ctrader", clock=lambda: clock_value[0]),
            policy=ExecutionPolicy(max_quantity=1_000, fixed_quantity=1, max_exposure=10_000),
            market_candidate_id="tp_fast_v1",
        )
        try:
            with (
                tempfile.TemporaryDirectory() as state,
                tempfile.TemporaryDirectory() as home,
                patch.dict(os.environ, {"HOME": home, "XDG_STATE_HOME": str(Path(home) / "state")}),
            ):
                result = collect_canary_inputs(
                    provider,
                    _technical_config(),
                    network=True,
                    deadline=end,
                    max_events=count,
                    window_start=BASE + timedelta(minutes=5),
                    window_end=end,
                    preparation_start=BASE,
                    session_evidence=_evidence(),
                    runtime_observer=executor.observe_runtime,
                    technical_only=True,
                    zero_spread_authorization=authorization,
                    fetch_warmup=False,
                    market_window_state=lambda *_args: "OPEN",
                    isolated_state_dir=state,
                    clock=lambda: clock_value[0],
                )
        finally:
            provider.close()
        self.assertFalse(executor.active)
        self.assertFalse(result.gates["orders_attempted"])
        return result

    def test_observed_async_equality_reaches_atr14_without_normalization_latch(self) -> None:
        result = self._collect(authorized=True)
        self.assertTrue(result.ok, result.reason)
        self.assertIsNotNone(result.context)
        assert result.context is not None
        self.assertEqual(result.context.bbo.bid, result.context.bbo.ask)
        self.assertGreater(float(result.context.runtime_snapshot["atr"]), 0)
        self.assertEqual(result.context.runtime_provenance["atr_period"], 14)
        self.assertFalse(result.context.strategy_ready)
        self.assertEqual(result.gates["watch_continuity_state"], "CONTINUOUS")
        self.assertEqual(result.gates["watch_freshness_state"], "VALID")
        self.assertNotEqual(result.gates["watch_reconciliation_state"], "NEEDS_RECONCILIATION")
        self.assertNotIn("normalization_error", result.gates["watch_analysis_blocked_reasons"])
        self.assertLessEqual(
            set(result.gates["watch_bbo_rejections"]),
            {"BBO fuera de la ventana aprobada", "BBO fuera de ventana por la pierna más antigua"},
        )
        self.assertGreaterEqual(result.context.bbo.available_at, BASE + timedelta(minutes=5))

    def test_async_equality_without_authorization_remains_blocked(self) -> None:
        result = self._collect(authorized=False)
        self.assertFalse(result.ok)
        self.assertEqual(result.gates["watch_reconciliation_state"], "NEEDS_RECONCILIATION")
        self.assertIn("normalization_error", result.gates["watch_analysis_blocked_reasons"])

    def test_unrelated_normalization_issue_is_never_waived_by_the_composite(self) -> None:
        result = self._collect(authorized=True, extra_issue=True)
        self.assertFalse(result.ok)
        self.assertEqual(result.gates["watch_reconciliation_state"], "NEEDS_RECONCILIATION")
        self.assertIn("normalization_error", result.gates["watch_analysis_blocked_reasons"])


if __name__ == "__main__":
    unittest.main()
