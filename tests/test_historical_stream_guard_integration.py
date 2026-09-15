"""Integration fences for the manifest-bound historical backtest stream."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from mtf_lab.data.historical import DatasetManifest, DatasetPartition, HistoricalQuote, SourceLocator
from mtf_lab.ops.historical_backtest import (
    HistoricalBacktestConfig,
    HistoricalBacktestError,
    _consume_quote,
    run_historical_backtest,
)
from mtf_lab.ops.market_protocol import ResearchProtocol

_DIGEST = "a" * 64
_BASE = datetime(2016, 3, 7, 5, tzinfo=UTC)


def _manifest(root: Path) -> DatasetManifest:
    partition = DatasetPartition(
        partition_id="201603",
        raw_archive="raw/fixture.zip",
        raw_sha256=_DIGEST,
        raw_size=1,
        members=("quotes.csv",),
        source_uri="local-fixture",
        terms_uri="local-fixture",
        acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
    )
    content_hash = hashlib.sha256(b"201603\0" + _DIGEST.encode() + b"\0" + b"1\0quotes.csv").hexdigest()
    return DatasetManifest(
        dataset_id=f"histdata:EUR/USD:201603:{_DIGEST[:32]}",
        provider="histdata",
        instrument="EUR/USD",
        partitions=(partition,),
        coverage_start=None,
        coverage_end=None,
        content_hash=content_hash,
        data_root=str(root),
    )


def _quote(
    index: int,
    *,
    event_time: datetime | None = None,
    sequence: int | None = None,
) -> HistoricalQuote:
    locator = SourceLocator(
        "raw/fixture.zip",
        "quotes.csv",
        _DIGEST,
        index,
        index + 1,
    )
    return HistoricalQuote(
        instrument="EUR/USD",
        event_time=event_time or (_BASE + timedelta(minutes=index)),
        available_at=event_time or (_BASE + timedelta(minutes=index)),
        bid=Decimal("1.10000"),
        ask=Decimal("1.10020"),
        locator=locator,
        sequence=index if sequence is None else sequence,
        source_event_id=f"{_DIGEST}:quotes.csv:{index + 1}:{index}",
    )


class HistoricalStreamGuardIntegrationTests(unittest.TestCase):
    def _config(self) -> HistoricalBacktestConfig:
        return HistoricalBacktestConfig.from_protocol(
            ResearchProtocol.default(),
            candidates=("tp_fast_v1",),
        )

    def test_duplicate_locator_with_new_sequence_is_rejected_before_consume(self) -> None:
        fixture = (_quote(0), _quote(0, sequence=1))
        with tempfile.TemporaryDirectory(prefix="mtf-stream-guard-duplicate-") as directory:
            root = Path(directory)
            with (
                patch("mtf_lab.ops.historical_backtest._consume_quote", wraps=_consume_quote) as consumed,
                self.assertRaises(HistoricalBacktestError),
            ):
                run_historical_backtest(
                    fixture,
                    manifest=_manifest(root),
                    config=self._config(),
                    output_dir=root / "output",
                    sink=lambda kind, row: None,
                )
            self.assertEqual(consumed.call_count, 1)
            assert consumed.call_args is not None
            raw = consumed.call_args.args[2]
            assert isinstance(raw, HistoricalQuote)
            self.assertEqual(raw.sequence, 0)

    def test_equal_timestamp_distinct_locator_is_valid_and_checkpointed(self) -> None:
        fixture = (_quote(0), _quote(1, event_time=_BASE))
        with tempfile.TemporaryDirectory(prefix="mtf-stream-guard-equal-time-") as directory:
            root = Path(directory)
            result = run_historical_backtest(
                fixture,
                manifest=_manifest(root),
                config=self._config(),
                output_dir=root / "output",
                sink=lambda kind, row: None,
            )
            snapshot = result.checkpoint.guard_snapshot
            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot["sequence"], 1)
            self.assertEqual(snapshot["event_time"], "2016-03-07T05:00:00.000000Z")
            self.assertEqual(snapshot["locator"]["row"], 2)

    def test_resume_requires_full_prefix_and_preserves_guard_identity(self) -> None:
        fixture = (_quote(0), _quote(1), _quote(2))
        with tempfile.TemporaryDirectory(prefix="mtf-stream-guard-resume-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            output = root / "output"
            config = self._config()
            result = run_historical_backtest(
                fixture,
                manifest=manifest,
                config=config,
                output_dir=output,
                sink=lambda kind, row: None,
            )
            resumed = run_historical_backtest(
                fixture,
                manifest=manifest,
                config=config,
                output_dir=output,
                sink=lambda kind, row: None,
                resume=result.checkpoint,
            )
            self.assertEqual(resumed.checkpoint.guard_snapshot, result.checkpoint.guard_snapshot)
            self.assertEqual(resumed.processed_quotes, result.processed_quotes)

            with self.assertRaisesRegex(HistoricalBacktestError, "prefix count|archive"):
                run_historical_backtest(
                    fixture[1:],
                    manifest=manifest,
                    config=config,
                    output_dir=output,
                    sink=lambda kind, row: None,
                    resume=result.checkpoint,
                )

    def test_resume_rejects_guard_snapshot_and_manifest_mismatches(self) -> None:
        fixture = (_quote(0), _quote(1))
        with tempfile.TemporaryDirectory(prefix="mtf-stream-guard-mismatch-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            output = root / "output"
            result = run_historical_backtest(
                fixture,
                manifest=manifest,
                config=self._config(),
                output_dir=output,
                sink=lambda kind, row: None,
            )
            tampered = result.checkpoint.to_dict()
            assert isinstance(tampered["guard_snapshot"], dict)
            tampered["guard_snapshot"]["manifest_hash"] = "f" * 64
            with self.assertRaisesRegex(HistoricalBacktestError, "guard snapshot incompatible"):
                run_historical_backtest(
                    fixture,
                    manifest=manifest,
                    config=self._config(),
                    output_dir=output,
                    sink=lambda kind, row: None,
                    resume=tampered,
                )

            identity_tampered = result.checkpoint.to_dict()
            identity_tampered["data_hash"] = "f" * 64
            with self.assertRaisesRegex(HistoricalBacktestError, "resume identity mismatch"):
                run_historical_backtest(
                    fixture,
                    manifest=manifest,
                    config=self._config(),
                    output_dir=output,
                    sink=lambda kind, row: None,
                    resume=identity_tampered,
                )


if __name__ == "__main__":
    unittest.main()
