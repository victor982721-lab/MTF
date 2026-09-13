from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from decimal import Decimal, localcontext
from pathlib import Path

from mtf_lab.ops.cfd_backtest import build_execution_plan
from mtf_lab.ops.cli import main
from mtf_lab.ops.research import compare_research, run_research


class ResearchIntegrationTests(unittest.TestCase):
    def test_partial_execution_model_is_reachable_through_actual_cli(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "partial.json"
            output = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
                code = main(
                    [
                        "research",
                        "run",
                        "--fixture",
                        "--manifest",
                        str(path),
                        "--variants",
                        "trend_pullback_v1",
                        "--horizons-seconds",
                        "60",
                        "--execution-model",
                        "ioc_partial",
                        "--fill-fraction",
                        "0.4",
                        "--bootstrap-iterations",
                        "4",
                    ]
                )
            self.assertEqual(code, 0)
            value = json.loads(output.getvalue())
            ledger = value["results"][0]["ledger"]
            self.assertEqual(ledger[0]["filled_quantity"], "400.0")
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(main(["research", "validate", str(path)]), 0)

    def test_different_strategy_families_can_be_compared_without_aggregation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ("baseline.json", "challenger.json")]
            for path, variant in zip(paths, ("trend_pullback_v1", "donchian20_m5_v1"), strict=True):
                run_research(
                    manifest_path=path, fixture=True, variants=[variant], horizons_seconds=[60], bootstrap_iterations=4
                )
            result = compare_research(paths)
            self.assertTrue(result["ok"])
            self.assertEqual(len(result["rows"]), 2)
            self.assertEqual({row["variant"] for row in result["rows"]}, {"trend_pullback_v1", "donchian20_m5_v1"})
            self.assertNotIn("combined_pnl", result)

    def test_partial_quantity_does_not_depend_on_ambient_decimal_context(self) -> None:
        expected = build_execution_plan("1234.56", execution_model="ioc_partial", fill_fraction="0.34567")
        with localcontext() as context:
            context.prec = 2
            actual = build_execution_plan("1234.56", execution_model="ioc_partial", fill_fraction="0.34567")
            self.assertEqual(context.prec, 2)
        self.assertEqual(actual, expected)
        # Integer check: 123456 * 34567 = 4267503552, with seven decimal places.
        self.assertEqual(actual.effective_quantity, Decimal("426.7503552"))
