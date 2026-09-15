"""Direct service contracts for the descriptive historical-data operation."""

from __future__ import annotations

import tempfile
import unittest
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from mtf_lab.data.historical import (
    HISTDATA_INSTRUMENT,
    HISTDATA_PROVIDER,
    DatasetManifest,
    HistoricalQuote,
    manifest_from_histdata_archive,
)
from mtf_lab.ops.global_trial_registry import GlobalTrialRegistry
from mtf_lab.ops.market_data_service import (
    DESCRIPTIVE_CANDIDATE_ID,
    DESCRIPTIVE_SCOPE,
    describe_market_data,
)

_LINES = b"20160307 000000000,1.10000,1.10020,0\n20160307 000100000,1.10001,1.10021,0\n"


def _manifest(root: Path) -> DatasetManifest:
    data_root = root / "market-data"
    raw_root = data_root / "raw"
    raw_root.mkdir(parents=True)
    archive = raw_root / "HISTDATA_COM_ASCII_EURUSD_T_201603.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as handle:
        handle.writestr("DAT_ASCII_EURUSD_T_201603.csv", _LINES)
    return manifest_from_histdata_archive(
        archive,
        data_root=data_root,
        acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
        source_uri="https://www.histdata.com/download-free-forex-historical-data/",
        terms_uri="https://www.histdata.com/f-a-q/data-files-detailed-specification/",
    )


class _CompatibleProvider:
    def __init__(self, manifest: DatasetManifest) -> None:
        self.manifest = manifest
        self.calls: list[tuple[datetime, datetime]] = []

    def stream(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[HistoricalQuote]:
        assert start is not None
        assert end is not None
        self.calls.append((start, end))
        return []


class MarketDataServiceTests(unittest.TestCase):
    def test_describe_registers_before_consuming_and_closes_completed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-market-service-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            registry = GlobalTrialRegistry(root / "trials.jsonl")
            start = datetime(2016, 3, 7, tzinfo=UTC)
            end = datetime(2016, 3, 8, tzinfo=UTC)
            structure = {"quote_statistics": {"quote_count": 2}}
            with patch(
                "mtf_lab.ops.market_structure.describe_market_structure",
                return_value=structure,
            ) as describe:
                result = describe_market_data(
                    manifest,
                    start=start,
                    end=end,
                    calendar_mode="strict",
                    registry=registry,
                    runtime_identity={"python": "test-runtime"},
                )
            self.assertEqual(result["market_structure"], structure)
            self.assertEqual(result["economic_conclusion"], "NOT_ASSESSED")
            self.assertEqual(result["profitability_claim"], "NONE")
            self.assertFalse(result["trading_enabled"])
            self.assertEqual(result["provenance"]["fills"], "NONE_DESCRIPTIVE_ONLY")
            self.assertEqual(describe.call_count, 1)

            records = registry.records()
            self.assertEqual(len(records), 2)
            registered, completed = records
            self.assertEqual(registered["candidate_id"], DESCRIPTIVE_CANDIDATE_ID)
            self.assertEqual(registered["mode"], "HISTORICAL")
            self.assertEqual(registered["scope"]["scope"], DESCRIPTIVE_SCOPE)
            self.assertEqual(registered["scope"]["manifest_hash"], manifest.content_hash)
            self.assertEqual(registered["scope"]["calendar_mode"], "strict")
            self.assertEqual(completed["status"], "COMPLETED")
            receipt = completed["details"]["receipt"]
            self.assertEqual(receipt["status"], "COMPLETED")
            self.assertEqual(receipt["manifest_hash"], manifest.content_hash)
            self.assertEqual(receipt["window_start"], "2016-03-07T00:00:00.000000Z")
            self.assertEqual(receipt["window_end"], "2016-03-08T00:00:00.000000Z")
            self.assertEqual(receipt["quote_count"], 2)
            self.assertFalse(receipt["network_performed"])
            self.assertEqual(registry.validate()["state"], "VALID")

    def test_describe_records_failed_receipt_and_error(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-market-service-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            registry = GlobalTrialRegistry(root / "trials.jsonl")
            with (
                patch(
                    "mtf_lab.ops.market_structure.describe_market_structure",
                    side_effect=RuntimeError("synthetic stream failure"),
                ),
                self.assertRaisesRegex(RuntimeError, "synthetic stream failure"),
            ):
                describe_market_data(
                    manifest,
                    start="2016-03-07T00:00:00Z",
                    end="2016-03-08T00:00:00Z",
                    registry=registry,
                )
            records = registry.records()
            self.assertEqual(len(records), 2)
            failed = records[-1]
            self.assertEqual(failed["status"], "FAILED")
            self.assertEqual(failed["details"]["error"], "synthetic stream failure")
            self.assertEqual(failed["details"]["receipt"]["status"], "FAILED")
            self.assertEqual(failed["details"]["receipt"]["error_type"], "RuntimeError")
            self.assertEqual(failed["details"]["receipt"]["manifest_hash"], manifest.content_hash)
            self.assertTrue(registry.validate()["ok"])

    def test_invalid_holdout_window_precedes_manifest_and_registry(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-market-service-") as directory:
            manifest_path = Path(directory) / "holdout.json"
            registry_path = Path(directory) / "trials.jsonl"
            with (
                patch("mtf_lab.data.historical.read_manifest") as read,
                patch("mtf_lab.ops.market_data_service.GlobalTrialRegistry") as registry_class,
                self.assertRaisesRegex(ValueError, "holdout cerrado"),
            ):
                describe_market_data(
                    manifest_path,
                    start="2024-01-01T00:00:00Z",
                    end="2025-01-01T00:00:00Z",
                    registry=registry_path,
                )
            read.assert_not_called()
            registry_class.assert_not_called()

    def test_compatible_provider_is_streamed_with_the_validated_window(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-market-service-") as directory:
            provider = _CompatibleProvider(_manifest(Path(directory)))
            with patch(
                "mtf_lab.ops.market_structure.describe_market_structure",
                return_value={"quote_statistics": {"quote_count": 0}},
            ):
                result = describe_market_data(
                    provider,
                    start="2016-03-07T00:00:00Z",
                    end="2016-03-08T00:00:00Z",
                    calendar_mode="modeled-fx",
                )
            self.assertEqual(result["calendar_mode"], "modeled-fx")
            self.assertEqual(
                provider.calls,
                [
                    (
                        datetime(2016, 3, 7, tzinfo=UTC),
                        datetime(2016, 3, 8, tzinfo=UTC),
                    )
                ],
            )
            self.assertEqual(result["manifest"]["provider"], HISTDATA_PROVIDER)
            self.assertEqual(result["manifest"]["instrument"], HISTDATA_INSTRUMENT)


if __name__ == "__main__":
    unittest.main()
