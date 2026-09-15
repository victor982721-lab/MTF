"""Offline contracts for composing adjacent HistData development months."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
import zipfile
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from mtf_lab.data.historical import (
    HISTDATA_INSTRUMENT,
    HISTDATA_PROVIDER,
    DatasetManifest,
    DatasetPartition,
    HistoricalDataError,
    iter_quotes,
    manifest_from_histdata_archive,
    manifest_from_histdata_archives,
    read_manifest,
    validate_dataset,
)

_ACQUIRED_AT = datetime(2026, 9, 14, tzinfo=UTC)
_SOURCE_URI = "https://www.histdata.com/download-free-forex-historical-data/"
_TERMS_URI = "https://www.histdata.com/f-a-q/data-files-detailed-specification/"


class HistoricalManifestScaleTests(unittest.TestCase):
    @staticmethod
    def _archive(root: Path, month: str, day: int, *, suffix: str = "", descending: bool = False) -> tuple[Path, Path]:
        data_root = root / "market-data"
        raw_root = data_root / "raw"
        raw_root.mkdir(parents=True, exist_ok=True)
        archive_path = raw_root / f"HISTDATA_COM_ASCII_EURUSD_T_{month}{suffix}.zip"
        rows = [
            f"{month}{day:02d} {hour:02d}0000000,{1.10000 + hour / 10000:.5f},{1.10020 + hour / 10000:.5f},0\n"
            for hour in range(2)
        ]
        if descending:
            rows.reverse()
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(f"DAT_ASCII_EURUSD_T_{month}.csv", "".join(rows).encode("ascii"))
        return archive_path, data_root

    @classmethod
    def _manifest(cls, root: Path, months: Sequence[str]) -> DatasetManifest:
        archives: list[Path] = []
        data_root: Path | None = None
        for index, month in enumerate(months):
            archive, data_root = cls._archive(root, month, 7 if month == "201603" else 1, suffix=f"-{index}")
            archives.append(archive)
        assert data_root is not None
        return manifest_from_histdata_archives(
            archives,
            data_root=data_root,
            acquired_at=_ACQUIRED_AT,
            source_uri=_SOURCE_URI,
            terms_uri=_TERMS_URI,
        )

    def test_adjacent_months_stream_with_global_identity_coverage_and_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-historical-scale-") as directory:
            root = Path(directory)
            manifest = self._manifest(root, ("201603", "201604", "201605"))
            validation = validate_dataset(manifest)
            self.assertTrue(validation.ok)
            self.assertEqual(validation.quote_count, 6)
            self.assertEqual([item.partition_id for item in manifest.partitions], ["201603", "201604", "201605"])
            self.assertEqual(len({item.raw_sha256 for item in manifest.partitions}), 3)
            self.assertEqual(validation.coverage_start, manifest.partitions[0].coverage_start)
            self.assertEqual(validation.coverage_end, manifest.partitions[-1].coverage_end)
            quotes = list(iter_quotes(manifest))
            self.assertEqual([quote.sequence for quote in quotes], list(range(6)))
            self.assertEqual(
                [quote.raw_sha256 for quote in quotes[::2]], [item.raw_sha256 for item in manifest.partitions]
            )
            self.assertEqual(quotes, list(iter_quotes(manifest, quotes[0].event_time, None)))

            manifest_path = root / "manifest.json"
            manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
            restored = read_manifest(manifest_path)
            self.assertEqual(restored.to_dict(), manifest.to_dict())
            self.assertTrue(validate_dataset(restored).ok)

    def test_partition_identity_and_month_gap_are_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-historical-scale-") as directory:
            root = Path(directory)
            march, data_root = self._archive(root, "201603", 7, suffix="-a")
            may, _ = self._archive(root, "201605", 1, suffix="-b")
            with self.assertRaisesRegex(HistoricalDataError, "gap between 201603 and 201605"):
                manifest_from_histdata_archives(
                    (march, may),
                    data_root=data_root,
                    acquired_at=_ACQUIRED_AT,
                    source_uri=_SOURCE_URI,
                    terms_uri=_TERMS_URI,
                )

            duplicate, _ = self._archive(root, "201603", 8, suffix="-duplicate")
            with self.assertRaisesRegex(HistoricalDataError, "duplicate partition identity"):
                manifest_from_histdata_archives(
                    (march, duplicate),
                    data_root=data_root,
                    acquired_at=_ACQUIRED_AT,
                    source_uri=_SOURCE_URI,
                    terms_uri=_TERMS_URI,
                )

    def test_global_source_order_is_rejected_without_sorting(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-historical-scale-") as directory:
            root = Path(directory)
            reversed_march, data_root = self._archive(root, "201603", 7, descending=True)
            april, _ = self._archive(root, "201604", 1)
            with self.assertRaisesRegex(HistoricalDataError, "source order violation"):
                manifest_from_histdata_archives(
                    (reversed_march, april),
                    data_root=data_root,
                    acquired_at=_ACQUIRED_AT,
                    source_uri=_SOURCE_URI,
                    terms_uri=_TERMS_URI,
                )

    def test_duplicate_raw_hash_and_coverage_collision_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-historical-scale-") as directory:
            root = Path(directory)
            common_hash = "a" * 64
            first = DatasetPartition(
                partition_id="201603",
                raw_archive="raw/march.zip",
                raw_sha256=common_hash,
                raw_size=1,
                members=("DAT_ASCII_EURUSD_T_201603.csv",),
                source_uri=_SOURCE_URI,
                terms_uri=_TERMS_URI,
                acquired_at=_ACQUIRED_AT,
                month="201603",
            )
            second = DatasetPartition(
                partition_id="201604",
                raw_archive="raw/april.zip",
                raw_sha256=common_hash,
                raw_size=1,
                members=("DAT_ASCII_EURUSD_T_201604.csv",),
                source_uri=_SOURCE_URI,
                terms_uri=_TERMS_URI,
                acquired_at=_ACQUIRED_AT,
                month="201604",
            )
            content_hash = hashlib.sha256(
                "\n".join(
                    f"{item.partition_id}\0{item.raw_sha256}\0{item.raw_size}\0{','.join(item.members)}"
                    for item in (first, second)
                ).encode("utf-8")
            ).hexdigest()
            with self.assertRaisesRegex(HistoricalDataError, "duplicate raw SHA-256"):
                DatasetManifest(
                    dataset_id=f"{HISTDATA_PROVIDER}:{HISTDATA_INSTRUMENT}:201603-201604:{content_hash[:32]}",
                    provider=HISTDATA_PROVIDER,
                    instrument=HISTDATA_INSTRUMENT,
                    partitions=(first, second),
                    coverage_start=None,
                    coverage_end=None,
                    content_hash=content_hash,
                    data_root=str(root / "market-data"),
                )

            manifest = self._manifest(Path(directory), ("201603", "201604"))
            previous = manifest.partitions[0]
            overlapping = manifest.partitions[1]
            overlapping = replace(overlapping, coverage_start=previous.coverage_end)
            with self.assertRaisesRegex(HistoricalDataError, "coverage collision"):
                DatasetManifest(
                    dataset_id=manifest.dataset_id,
                    provider=manifest.provider,
                    instrument=manifest.instrument,
                    partitions=(previous, overlapping),
                    coverage_start=manifest.coverage_start,
                    coverage_end=manifest.coverage_end,
                    content_hash=manifest.content_hash,
                    data_root=manifest.data_root,
                )

    def test_march_single_manifest_is_unchanged_and_holdout_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-historical-scale-") as directory:
            root = Path(directory)
            archive, data_root = self._archive(root, "201603", 7)
            single = manifest_from_histdata_archive(
                archive,
                data_root=data_root,
                acquired_at=_ACQUIRED_AT,
                source_uri=_SOURCE_URI,
                terms_uri=_TERMS_URI,
            )
            composed = manifest_from_histdata_archives(
                (archive,),
                data_root=data_root,
                acquired_at=_ACQUIRED_AT,
                source_uri=_SOURCE_URI,
                terms_uri=_TERMS_URI,
            )
            self.assertEqual(composed.to_dict(), single.to_dict())

        for month in ("202401", "202501"):
            with self.subTest(month=month), tempfile.TemporaryDirectory(prefix="mtf-historical-holdout-") as directory:
                archive, data_root = self._archive(Path(directory), month, 1)
                with self.assertRaisesRegex(HistoricalDataError, "holdout months"):
                    manifest_from_histdata_archives(
                        (archive,),
                        data_root=data_root,
                        acquired_at=_ACQUIRED_AT,
                        source_uri=_SOURCE_URI,
                        terms_uri=_TERMS_URI,
                    )


if __name__ == "__main__":
    unittest.main()
