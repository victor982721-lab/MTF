from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from mtf_lab.data.capture import iter_jsonl
from mtf_lab.ops import cli


class CliContractTests(unittest.TestCase):
    def test_facade_keeps_commands_and_public_callbacks(self) -> None:
        parser = cli.build_parser()
        help_text = parser.format_help()
        for command in (
            "doctor",
            "demo",
            "import",
            "replay",
            "backtest",
            "report",
            "watch",
            "ui",
            "ctrader",
            "cfd-paper",
        ):
            self.assertIn(command, help_text)

        parsed = parser.parse_args(["replay", "--input", "capture.jsonl", "--preserve-order"])
        self.assertIs(parsed.func, cli.cmd_replay)
        self.assertTrue(parsed.preserve_order)
        parsed = parser.parse_args(["ctrader", "query", "--fixture"])
        self.assertIs(parsed.func, cli.cmd_ctrader_query)
        self.assertTrue(parsed.fixture)

    def test_public_jsonl_reader_is_lazy_and_preserves_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "capture.jsonl"
            first = {"timestamp": 1_700_000_000_000, "bid": "1.1"}
            second = {"timestamp": 1_700_000_060_000, "ask": "1.2"}
            path.write_text(json.dumps(first) + "\n", encoding="utf-8")
            iterator = iter_jsonl(path)
            # The public reader opens/decodes on demand; a later row is visible
            # to the same stream without read_text().splitlines() materialising it.
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(second) + "\n")
            self.assertEqual(next(iterator), first)
            self.assertEqual(next(iterator), second)
            with self.assertRaises(StopIteration):
                next(iterator)

    def test_ctrader_demo_without_activation_is_explicitly_offline(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = cli.main(["ctrader", "demo"])
        result = json.loads(output.getvalue())
        self.assertEqual(code, 0)
        self.assertFalse(result["server_contacted"])
        self.assertFalse(result["gateway_adapter_used"])
        self.assertEqual(result["fixture_source"], "local_fixture")


if __name__ == "__main__":
    unittest.main()
