"""Regression tests for the canonical DEMO canary CLI module identity."""

from __future__ import annotations

import argparse
import contextlib
import inspect
import io
import json
import runpy
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from mtf_lab.ops.ctrader_canary_inputs import CanaryInputContext
from tools import demo_canary


class _StopCli(BaseException):
    """Stop the runpy CLI before it can reach any backend."""


class _MinimalContext:
    """Only the adapter seam is needed; this is not a READY market context."""

    buy_signal = {}
    sell_signal = {}
    runtime_snapshot = {}
    to_canary_inputs = CanaryInputContext.to_canary_inputs


class DemoCanaryEntrypointTests(unittest.TestCase):
    def _network_error(self, error: Exception, *, execute: bool = False) -> tuple[int, dict[str, object]]:
        argv = ["--config", "unused.toml", "--network", "--state-dir", "."]
        if execute:
            argv.append("--execute")
        output = io.StringIO()
        with (
            patch.object(demo_canary, "network_cli_preflight", side_effect=error) as preflight,
            contextlib.redirect_stdout(output),
        ):
            status = demo_canary.main(argv)
        self.assertEqual(preflight.call_count, 1)
        return status, json.loads(output.getvalue())

    def test_runpy_entrypoint_uses_canonical_main_identity_for_adapter(self) -> None:
        seen: dict[str, object] = {}

        def parse_args_probe(parser: argparse.ArgumentParser, argv=None, namespace=None):
            del parser, argv, namespace
            frame = inspect.currentframe()
            try:
                caller = frame.f_back if frame is not None else None
                if caller is None:
                    raise AssertionError("parse_args caller frame missing")
                module = caller.f_globals
                seen["module"] = module
                seen["result"] = module["_coerce_inputs"](_MinimalContext())
            finally:
                del frame
            raise _StopCli()

        script = Path(__file__).resolve().parents[1] / "tools" / "demo_canary.py"
        with (
            patch.object(argparse.ArgumentParser, "parse_args", new=parse_args_probe),
            self.assertRaises(_StopCli),
        ):
            runpy.run_path(str(script), run_name="__main__")

        module = seen["module"]
        result = seen["result"]
        self.assertIsInstance(module, dict)
        self.assertEqual(module["__name__"], "tools.demo_canary")
        self.assertIs(module["CanaryInputs"], demo_canary.CanaryInputs)
        self.assertIsInstance(result, demo_canary.CanaryInputs)
        self.assertEqual(result.buy_signal, {})
        self.assertEqual(result.sell_signal, {})
        self.assertEqual(result.buy_runtime, {})
        self.assertEqual(result.sell_runtime, {})

    def test_bad_adapter_remains_a_gate_error(self) -> None:
        class BadAdapter:
            def to_canary_inputs(self):
                return {}

        with self.assertRaises(demo_canary.CanaryGateError) as raised:
            demo_canary._coerce_inputs(BadAdapter())
        self.assertEqual(str(raised.exception), "inputs no son CanaryInputs ni contexto observado adaptable")

    def test_network_gate_error_preserves_reason_and_canonical_gates(self) -> None:
        observed_at = datetime(2026, 9, 21, 15, 0, 1, tzinfo=UTC)
        error = demo_canary.CanaryGateError(
            "inputs observados ausentes",
            gates={"state": "BLOCKED", "budget": Decimal("1.25"), "observed_at": observed_at},
        )
        status, payload = self._network_error(error)
        self.assertEqual(status, 2)
        self.assertEqual(payload["reason"], "inputs observados ausentes")
        self.assertEqual(
            payload["gates"],
            {"budget": "1.25", "observed_at": "2026-09-21T15:00:01.000000Z", "state": "BLOCKED"},
        )
        self.assertEqual(payload["state"], "NETWORK_PREFLIGHT_BLOCKED")
        self.assertEqual(payload["execution_state"], "NOT_STARTED")
        self.assertEqual(payload["mutation_messages"], 0)

    def test_mutation_boundary_is_unknown_and_requires_reconciliation(self) -> None:
        error = demo_canary.CanaryGateError(
            "respuesta perdida después de mutación",
            gates={"state": "UNKNOWN"},
            mutation_messages=2,
        )
        status, payload = self._network_error(error, execute=True)
        self.assertEqual(status, 2)
        self.assertEqual(payload["state"], "CANARY_EXECUTION_UNKNOWN")
        self.assertEqual(payload["execution_state"], "UNKNOWN")
        self.assertEqual(payload["mutation_messages"], 2)
        self.assertTrue(payload["orders_attempted"])
        self.assertTrue(payload["account_reconciliation_required"])
        self.assertTrue(payload["human_review_required"])
        self.assertEqual(payload["reason"], "respuesta perdida después de mutación")

    def test_nonserializable_gate_details_do_not_leak_repr(self) -> None:
        class SecretGate:
            def __repr__(self) -> str:
                return "secret-repr-must-not-appear"

        error = demo_canary.CanaryGateError("safe reason", gates={"detail": SecretGate()})
        output = io.StringIO()
        with (
            patch.object(demo_canary, "network_cli_preflight", side_effect=error),
            contextlib.redirect_stdout(output),
        ):
            status = demo_canary.main(["--config", "unused.toml", "--network", "--state-dir", "."])
        payload = json.loads(output.getvalue())
        self.assertEqual(status, 2)
        self.assertEqual(payload["reason"], "safe reason")
        self.assertEqual(payload["gates"], {"details_unavailable": "non_serializable_gate_details"})
        self.assertNotIn("secret-repr-must-not-appear", output.getvalue())


if __name__ == "__main__":
    unittest.main()
