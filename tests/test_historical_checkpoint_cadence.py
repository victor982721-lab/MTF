"""Pruebas focales de cadencia durable y reanudación histórica."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_historical_backtest import manifest, quotes  # type: ignore[import-not-found]

import mtf_lab.ops.historical_backtest as historical_backtest
from mtf_lab.ops.historical_backtest import (
    BLOCK_SIZE,
    DEFAULT_CHECKPOINT_INTERVAL_BLOCKS,
    HistoricalBacktestConfig,
    HistoricalBacktestError,
    _ArtifactWriter,
    run_historical_backtest,
)
from mtf_lab.ops.market_protocol import ResearchProtocol


class HistoricalCheckpointCadenceTests(unittest.TestCase):
    def _config(self, *, interval_blocks: int = DEFAULT_CHECKPOINT_INTERVAL_BLOCKS) -> HistoricalBacktestConfig:
        return HistoricalBacktestConfig.from_protocol(
            ResearchProtocol.default(),
            candidates=("tp_fast_v1",),
            checkpoint_interval_blocks=interval_blocks,
        )

    def test_default_cadence_is_explicit_and_part_of_identity(self) -> None:
        config = self._config()
        self.assertEqual(config.block_size, BLOCK_SIZE)
        self.assertEqual(config.checkpoint_interval_blocks, 32)
        self.assertEqual(config.checkpoint_interval_quotes, 65_536)
        serialized = config.to_dict()
        self.assertEqual(serialized["checkpoint_interval_blocks"], 32)
        self.assertEqual(serialized["checkpoint_interval_quotes"], 65_536)

        faster = self._config(interval_blocks=1)
        self.assertNotEqual(config.config_hash, faster.config_hash)

        with self.assertRaisesRegex(HistoricalBacktestError, "entero positivo"):
            self._config(interval_blocks=0)
        with self.assertRaisesRegex(HistoricalBacktestError, "entero positivo"):
            self._config(interval_blocks=True)

    def test_stop_at_periodic_boundary_writes_one_immediate_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-historical-cadence-boundary-") as directory:
            root = Path(directory)
            config = self._config(interval_blocks=1)
            with patch.object(
                historical_backtest,
                "_write_checkpoint",
                wraps=historical_backtest._write_checkpoint,
            ) as write_checkpoint:
                result = run_historical_backtest(
                    quotes(BLOCK_SIZE),
                    manifest=manifest(root),
                    config=config,
                    output_dir=root / "output",
                    sink=lambda _kind, _row: None,
                    stop_after_quotes=BLOCK_SIZE,
                )

            self.assertEqual(result.status, "CHECKPOINTED")
            self.assertEqual(result.processed_quotes, BLOCK_SIZE)
            self.assertFalse(result.checkpoint.finished)
            self.assertEqual(result.checkpoint.checkpoint_interval_blocks, 1)
            self.assertEqual(write_checkpoint.call_count, 1)
            payload = json.loads((root / "output" / "checkpoint.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["processed_quotes"], BLOCK_SIZE)
            self.assertEqual(payload["checkpoint_interval_blocks"], 1)

    def test_non_boundary_stop_is_immediate_even_with_sparse_default_cadence(self) -> None:
        events: list[str] = []
        original_offsets = _ArtifactWriter.artifact_offsets
        original_write = historical_backtest._write_checkpoint
        ledger_size = 0

        def offsets(writer: _ArtifactWriter) -> dict[str, int]:
            events.append("offsets")
            return original_offsets(writer)

        def write(output_dir: Path, checkpoint: historical_backtest.HistoricalBacktestCheckpoint) -> None:
            events.append("checkpoint")
            original_write(output_dir, checkpoint)

        with tempfile.TemporaryDirectory(prefix="mtf-historical-cadence-stop-") as directory:
            root = Path(directory)
            with (
                patch.object(_ArtifactWriter, "artifact_offsets", offsets),
                patch.object(historical_backtest, "_write_checkpoint", write),
            ):
                result = run_historical_backtest(
                    quotes(7),
                    manifest=manifest(root),
                    config=self._config(),
                    output_dir=root / "output",
                    sink=lambda _kind, _row: None,
                    stop_after_quotes=7,
                )
            ledger_size = (root / "output" / "ledger.jsonl").stat().st_size

        self.assertEqual(result.status, "CHECKPOINTED")
        self.assertEqual(result.processed_quotes, 7)
        self.assertEqual(events, ["offsets", "checkpoint"])
        self.assertEqual(result.checkpoint.artifact_offsets["ledger"], ledger_size)

    def test_resume_with_periodic_snapshots_matches_uninterrupted_terminal_run(self) -> None:
        fixture = quotes(BLOCK_SIZE + 37)
        with tempfile.TemporaryDirectory(prefix="mtf-historical-cadence-resume-") as directory:
            root = Path(directory)
            config = self._config(interval_blocks=1)
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
                checkpoint_after_quotes=BLOCK_SIZE,
            )
            resumed = run_historical_backtest(
                fixture,
                manifest=manifest(root),
                config=config,
                output_dir=root / "resumed",
                sink=lambda _kind, _row: None,
                resume=partial.checkpoint,
            )

            self.assertEqual(partial.checkpoint.processed_quotes, BLOCK_SIZE)
            self.assertEqual(resumed.status, "COMPLETED")
            self.assertEqual(resumed.metrics, full.metrics)
            self.assertEqual(resumed.checkpoint.state, full.checkpoint.state)
            self.assertEqual(resumed.checkpoint.to_dict(), full.checkpoint.to_dict())
            for kind in ("ledger", "equity", "funnel"):
                self.assertEqual(
                    (root / "resumed" / f"{kind}.jsonl").read_bytes(),
                    (root / "full" / f"{kind}.jsonl").read_bytes(),
                )


if __name__ == "__main__":
    unittest.main()
