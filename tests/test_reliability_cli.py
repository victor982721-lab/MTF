from __future__ import annotations

import contextlib
import io
import unittest
from pathlib import Path

from mtf_lab.ops.cli import build_parser, cmd_ctrader_supervise, cmd_research


class ReliabilityCliTests(unittest.TestCase):
    def test_research_product_and_inputs_are_explicit(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["research", "run", "--fixture", "--manifest", "/tmp/new-manifest.json"])
        self.assertIs(args.func, cmd_research)
        self.assertEqual(args.research_action, "run")
        self.assertTrue(args.fixture)
        self.assertIsNone(args.input)
        self.assertEqual(args.manifest, Path("/tmp/new-manifest.json"))
        self.assertEqual(args.execution_model, "full_fill")
        partial = parser.parse_args(
            [
                "research",
                "run",
                "--fixture",
                "--manifest",
                "/tmp/partial.json",
                "--execution-model",
                "ioc_partial",
                "--fill-fraction",
                "0.4",
            ]
        )
        self.assertEqual(partial.fill_fraction, "0.4")
        self.assertEqual(partial.execution_model, "ioc_partial")
        self.assertEqual(parser.parse_args(["research", "validate", "/tmp/x.json"]).research_action, "validate")
        self.assertEqual(len(parser.parse_args(["research", "compare", "/tmp/a.json", "/tmp/b.json"]).manifests), 2)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["research", "run", "--manifest", "/tmp/x.json"])

    def test_supervision_never_selects_network_or_trading_silently(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            ["ctrader", "supervise", "--fixture", "--state-dir", "/tmp/state", "--db", "/tmp/state/capture.db"]
        )
        self.assertIs(args.func, cmd_ctrader_supervise)
        self.assertEqual(args.mode, "observe")
        self.assertFalse(args.activate)
        self.assertFalse(args.network)
        self.assertFalse(args.continuous)
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["ctrader", "supervise", "--state-dir", "/tmp/state", "--db", "/tmp/state/x.db"])

    def test_legacy_commands_remain_separate(self) -> None:
        parser = build_parser()
        self.assertEqual(parser.parse_args(["demo"]).command, "demo")
        self.assertEqual(parser.parse_args(["backtest"]).command, "backtest")
        self.assertEqual(parser.parse_args(["cfd-paper"]).command, "cfd-paper")
