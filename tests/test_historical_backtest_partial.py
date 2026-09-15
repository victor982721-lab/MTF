"""Regresiones offline para checkpoints históricos no terminales."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_historical_backtest import manifest, quotes

from mtf_lab.core.cfd_simulation import CFDSimulator
from mtf_lab.ops.historical_backtest import (
    HistoricalBacktestConfig,
    HistoricalBacktestError,
    IncrementalProcessor,
    run_historical_backtest,
)
from mtf_lab.ops.market_protocol import ResearchProtocol


class HistoricalBacktestPartialCheckpointTests(unittest.TestCase):
    def _config(self) -> HistoricalBacktestConfig:
        return HistoricalBacktestConfig.from_protocol(ResearchProtocol.default())

    def test_partial_checkpoint_does_not_call_finalizers_and_is_durable(self) -> None:
        fixture = quotes(16)
        with tempfile.TemporaryDirectory(prefix="mtf-historical-partial-") as directory:
            root = Path(directory)
            output = root / "output"
            with (
                patch.object(IncrementalProcessor, "finalize", side_effect=AssertionError("processor finalized")),
                patch.object(CFDSimulator, "finish", side_effect=AssertionError("simulator finalized")),
            ):
                result = run_historical_backtest(
                    fixture,
                    manifest=manifest(root),
                    config=self._config(),
                    output_dir=output,
                    sink=lambda _kind, _row: None,
                    stop_after_quotes=7,
                )

            self.assertEqual(result.status, "CHECKPOINTED")
            self.assertFalse(result.finished)
            self.assertFalse(result.capture_complete)
            self.assertFalse(result.acceptance)
            self.assertEqual(result.processed_quotes, 7)
            self.assertEqual(result.checkpoint.status, "CHECKPOINTED")
            self.assertFalse(result.checkpoint.finished)
            self.assertEqual(result.metrics["status"], "CHECKPOINTED")
            self.assertFalse(result.metrics["finished"])
            self.assertFalse(result.metrics["capture_complete"])
            self.assertEqual(result.to_dict()["status"], "CHECKPOINTED")
            self.assertFalse(result.to_dict()["finished"])

            checkpoint_payload = json.loads((output / "checkpoint.json").read_text(encoding="utf-8"))
            self.assertEqual(checkpoint_payload["status"], "CHECKPOINTED")
            self.assertFalse(checkpoint_payload["finished"])
            self.assertEqual(checkpoint_payload["processed_quotes"], 7)
            for profile_id in self._config().candidate_ids:
                state = result.checkpoint.state["profiles"][profile_id]
                self.assertEqual(state["quote_count"], 7)
                self.assertFalse(state["legacy"]["finished"])
                self.assertFalse(state["risk"]["finished"])

            for kind, offset in result.checkpoint.artifact_offsets.items():
                path = output / f"{kind}.jsonl"
                self.assertTrue(path.is_file())
                self.assertEqual(path.stat().st_size, offset)
                if offset:
                    self.assertEqual(path.read_bytes()[-1:], b"\n")

    def test_stop_callback_stops_only_after_a_fully_consumed_quote(self) -> None:
        fixture = quotes(12)
        seen = 0

        def should_stop() -> bool:
            return seen >= 4

        def source():
            nonlocal seen
            for quote in fixture:
                seen += 1
                yield quote

        with tempfile.TemporaryDirectory(prefix="mtf-historical-stop-callback-") as directory:
            result = run_historical_backtest(
                source,
                manifest=manifest(Path(directory)),
                config=self._config(),
                output_dir=Path(directory) / "output",
                sink=lambda _kind, _row: None,
                stop_requested=should_stop,
            )

        self.assertEqual(seen, 4)
        self.assertEqual(result.status, "CHECKPOINTED")
        self.assertEqual(result.processed_quotes, 4)
        self.assertEqual(result.checkpoint.cursor_sequence, 3)

    def test_partial_resume_is_equivalent_to_uninterrupted_capture(self) -> None:
        fixture = quotes(80)
        with tempfile.TemporaryDirectory(prefix="mtf-historical-partial-equivalence-") as directory:
            root = Path(directory)
            config = self._config()
            full = run_historical_backtest(
                fixture,
                manifest=manifest(root),
                config=config,
                output_dir=root / "full",
                sink=lambda _kind, _row: None,
            )
            partial = run_historical_backtest(
                fixture,
                manifest=manifest(root),
                config=config,
                output_dir=root / "resumed",
                sink=lambda _kind, _row: None,
                checkpoint_after_quotes=17,
            )
            resumed = run_historical_backtest(
                fixture,
                manifest=manifest(root),
                config=config,
                output_dir=root / "resumed",
                sink=lambda _kind, _row: None,
                resume=partial.checkpoint,
            )

            self.assertEqual(partial.status, "CHECKPOINTED")
            self.assertFalse(partial.finished)
            self.assertEqual(resumed.status, "COMPLETED")
            self.assertTrue(resumed.finished)
            self.assertEqual(resumed.processed_quotes, full.processed_quotes)
            self.assertEqual(resumed.metrics, full.metrics)
            self.assertEqual(
                [variant.to_dict() for variant in resumed.variants],
                [variant.to_dict() for variant in full.variants],
            )
            self.assertEqual(resumed.checkpoint.state, full.checkpoint.state)
            self.assertEqual(resumed.checkpoint.guard_snapshot, full.checkpoint.guard_snapshot)
            for kind in ("ledger", "equity", "funnel"):
                self.assertEqual(
                    (root / "resumed" / f"{kind}.jsonl").read_bytes(),
                    (root / "full" / f"{kind}.jsonl").read_bytes(),
                )

    def test_resume_validates_partial_state_identity_and_offsets(self) -> None:
        fixture = quotes(18)
        with tempfile.TemporaryDirectory(prefix="mtf-historical-partial-identity-") as directory:
            root = Path(directory)
            config = self._config()
            partial = run_historical_backtest(
                fixture,
                manifest=manifest(root),
                config=config,
                output_dir=root / "output",
                sink=lambda _kind, _row: None,
                stop_after_quotes=8,
            )

            identity = partial.checkpoint.to_dict()
            identity["config_hash"] = "f" * 64
            with self.assertRaisesRegex(HistoricalBacktestError, "resume identity mismatch"):
                run_historical_backtest(
                    fixture,
                    manifest=manifest(root),
                    config=config,
                    output_dir=root / "output",
                    sink=lambda _kind, _row: None,
                    resume=identity,
                )

            missing_offset = partial.checkpoint.to_dict()
            offsets = missing_offset["artifact_offsets"]
            assert isinstance(offsets, dict)
            offsets.pop("equity")
            with self.assertRaisesRegex(HistoricalBacktestError, "offsets completos"):
                run_historical_backtest(
                    fixture,
                    manifest=manifest(root),
                    config=config,
                    output_dir=root / "output",
                    sink=lambda _kind, _row: None,
                    resume=missing_offset,
                )

            lifecycle = partial.checkpoint.to_dict()
            lifecycle["finished"] = True
            with self.assertRaisesRegex(HistoricalBacktestError, "lifecycle incompatible"):
                run_historical_backtest(
                    fixture,
                    manifest=manifest(root),
                    config=config,
                    output_dir=root / "output",
                    sink=lambda _kind, _row: None,
                    resume=lifecycle,
                )

    def test_terminal_prefix_behavior_is_unchanged_without_partial_control(self) -> None:
        fixture = quotes(9)
        with tempfile.TemporaryDirectory(prefix="mtf-historical-prefix-terminal-") as directory:
            result = run_historical_backtest(
                fixture,
                manifest=manifest(Path(directory)),
                config=self._config(),
                output_dir=Path(directory) / "output",
                sink=lambda _kind, _row: None,
            )

        self.assertEqual(result.status, "COMPLETED")
        self.assertTrue(result.finished)
        self.assertTrue(result.capture_complete)
        self.assertTrue(result.checkpoint.finished)
        self.assertEqual(result.checkpoint.status, "COMPLETED")
        self.assertEqual(result.checkpoint.cursor_sequence, len(fixture) - 1)

    def test_partial_control_validation_is_fail_closed(self) -> None:
        fixture = quotes(4)
        with tempfile.TemporaryDirectory(prefix="mtf-historical-partial-validation-") as directory:
            root = Path(directory)
            for kwargs, message in (
                ({"stop_after_quotes": 0}, "entero positivo"),
                ({"stop_after_quotes": True}, "entero positivo"),
                ({"stop_after_quotes": 2, "checkpoint_after_quotes": 3}, "no coinciden"),
                ({"stop_requested": lambda: 1}, "debe devolver booleano"),
            ):
                with self.subTest(kwargs=kwargs):
                    if "stop_requested" in kwargs:
                        stop_requested = kwargs["stop_requested"]
                        stop_after = None
                        alias = None
                    else:
                        stop_requested = None
                        stop_after = kwargs.get("stop_after_quotes")
                        alias = kwargs.get("checkpoint_after_quotes")
                    with self.assertRaisesRegex(HistoricalBacktestError, message):
                        run_historical_backtest(
                            fixture,
                            manifest=manifest(root / str(len(kwargs))),
                            config=self._config(),
                            output_dir=root / f"output-{len(kwargs)}-{message}",
                            sink=lambda _kind, _row: None,
                            stop_after_quotes=stop_after,
                            checkpoint_after_quotes=alias,
                            stop_requested=stop_requested,
                        )


if __name__ == "__main__":
    unittest.main()
