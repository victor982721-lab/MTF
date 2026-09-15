"""Offline contracts for the bounded HistData historical quote adapter."""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import unittest
import zipfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest import mock

import mtf_lab.data.histdata_acquisition as acquisition_impl
from mtf_lab.data.historical import (
    HISTDATA_INSTRUMENT,
    HISTDATA_MONTH,
    HISTDATA_PROVIDER,
    DatasetManifest,
    DatasetPartition,
    HistoricalDataError,
    HistoricalQuote,
    HistoricalQuoteProvider,
    SourceLocator,
    iter_quotes,
    manifest_from_histdata_archive,
    read_manifest,
    stream,
    validate,
    validate_dataset,
)
from mtf_lab.data.provider import MarketDataProvider
from tools import histdata_acquire

LINES = (
    b"20160307 000000000,1.10000,1.10020,0\n"
    b"20160307 000100000,1.10001,1.10021,0\n"
    b"20160307 000100000,1.10002,1.10022,0\n"
    b"20160313 120000000,1.10003,1.10023,0\n"
)


class HistoricalFixtureMixin:
    def make_archive(self, root: Path, lines: bytes = LINES) -> tuple[Path, Path, str]:
        data_root = root / "market-data"
        raw = data_root / "raw"
        raw.mkdir(parents=True)
        archive = raw / "HISTDATA_COM_ASCII_EURUSD_T_201603.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as handle:
            handle.writestr("DAT_ASCII_EURUSD_T_201603.csv", lines)
        return archive, data_root, hashlib.sha256(archive.read_bytes()).hexdigest()

    def make_manifest(self, root: Path, lines: bytes = LINES) -> DatasetManifest:
        archive, data_root, _ = self.make_archive(root, lines)
        return manifest_from_histdata_archive(
            archive,
            data_root=data_root,
            acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
            source_uri="https://www.histdata.com/download-free-forex-historical-data/",
            terms_uri="https://www.histdata.com/f-a-q/data-files-detailed-specification/",
        )


class HistoricalDataTests(HistoricalFixtureMixin, unittest.TestCase):
    def test_manifest_and_validation_are_versioned_and_streaming(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.make_manifest(Path(directory))
            result = validate_dataset(manifest)
            self.assertTrue(result.ok)
            self.assertEqual(result.quote_count, 4)
            self.assertEqual(result.coverage_start, datetime(2016, 3, 7, 5, tzinfo=UTC))
            self.assertEqual(result.coverage_end, datetime(2016, 3, 13, 17, tzinfo=UTC))
            self.assertEqual(manifest.provider, HISTDATA_PROVIDER)
            self.assertEqual(manifest.instrument, HISTDATA_INSTRUMENT)
            self.assertEqual(manifest.partitions[0].month, HISTDATA_MONTH)
            self.assertEqual(manifest.to_dict()["partitions"][0]["raw_sha256"], manifest.partitions[0].raw_sha256)
            limited = validate_dataset(
                manifest,
                datetime(2016, 3, 7, 5, tzinfo=UTC),
                datetime(2016, 3, 7, 5, 2, tzinfo=UTC),
            )
            self.assertTrue(limited.ok)
            self.assertTrue(limited.coverage_limited)
            self.assertEqual(limited.quote_count, 3)
            self.assertEqual(limited.requested_start, datetime(2016, 3, 7, 5, tzinfo=UTC))

    def test_iter_quotes_preserves_same_timestamp_and_decimal_bbo(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.make_manifest(Path(directory))
            start = datetime(2016, 3, 7, 5, 0, tzinfo=UTC)
            end = start + timedelta(minutes=2)
            quotes = list(iter_quotes(manifest, start, end))
        self.assertEqual(len(quotes), 3)
        self.assertEqual([quote.sequence for quote in quotes], [0, 1, 2])
        self.assertEqual([quote.row for quote in quotes], [1, 2, 3])
        self.assertEqual(quotes[1].event_time, quotes[2].event_time)
        self.assertEqual(quotes[0].bid, Decimal("1.10000"))
        self.assertEqual(quotes[0].ask, Decimal("1.10020"))
        self.assertEqual(quotes[0].available_at, quotes[0].event_time)
        self.assertEqual(quotes[0].availability_basis, "historical_event_time")
        self.assertEqual(quotes[0].raw_sha256, quotes[0].partitionSHA)
        self.assertEqual(quotes[0].event_ts, quotes[0].timestamp)
        self.assertEqual(
            quotes[0].source_event_id, f"{quotes[0].raw_sha256}:DAT_ASCII_EURUSD_T_201603.csv:1:{quotes[0].offset}"
        )
        self.assertEqual(HistoricalQuote.from_mapping(quotes[0].to_dict()), quotes[0])

    def test_fixed_est_without_dst_and_provider_stream(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.make_manifest(Path(directory))
            provider = HistoricalQuoteProvider(manifest)
            self.assertIsInstance(provider, MarketDataProvider)
            quotes = list(provider.stream())
            self.assertEqual(list(stream(manifest)), quotes)
        self.assertEqual(quotes[-1].event_time, datetime(2016, 3, 13, 17, tzinfo=UTC))
        self.assertEqual(provider.name, "histdata_historical")

    def test_manifest_json_roundtrip_and_strict_integer_parsing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self.make_manifest(root)
            manifest_path = root / "manifest.json"
            manifest_path.write_text(manifest.to_json() + "\n", encoding="utf-8")
            restored = read_manifest(manifest_path)
            self.assertEqual(restored.to_dict(), manifest.to_dict())
            raw = json.loads(manifest.to_json())
            raw["partitions"][0]["raw_size"] = True
            with self.assertRaises(HistoricalDataError):
                DatasetManifest.from_mapping(raw)
            raw["partitions"][0]["raw_size"] = float(manifest.partitions[0].raw_size)
            with self.assertRaises(HistoricalDataError):
                DatasetManifest.from_mapping(raw)

            for key, value in (
                ("normalization_version", "bogus-v2"),
                ("dataset_id", "unbound"),
                ("instrument", "eur/usd"),
                ("unexpected", True),
            ):
                tampered = json.loads(manifest.to_json())
                tampered[key] = value
                with self.assertRaises(HistoricalDataError):
                    DatasetManifest.from_mapping(tampered)

    def test_volume_is_checked_but_never_attributed_as_traded_volume(self) -> None:
        for volume in ("not-a-number", "NaN", "Infinity", "-1"):
            with tempfile.TemporaryDirectory() as directory, self.assertRaises(HistoricalDataError):
                self.make_manifest(
                    Path(directory),
                    f"20160307 000000000,1.2,1.3,{volume}\n".encode(),
                )

    def test_empty_or_out_of_coverage_validation_is_not_eligible(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = self.make_manifest(Path(directory))
            result = validate_dataset(
                manifest,
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 2, tzinfo=UTC),
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.quote_count, 0)
        self.assertTrue(result.coverage_limited)
        self.assertIn("requested coverage is empty", result.issues)
        self.assertIn("requested range is outside dataset coverage", result.issues)

    def test_validate_detects_mutation_and_order_violation_without_sorting(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, data_root, digest = self.make_archive(
                root,
                b"20160307 000100000,1.10001,1.10021,0\n20160307 000000000,1.10000,1.10020,0\n",
            )
            partition = DatasetPartition(
                partition_id="201603",
                raw_archive="raw/HISTDATA_COM_ASCII_EURUSD_T_201603.zip",
                raw_sha256=digest,
                raw_size=archive.stat().st_size,
                members=("DAT_ASCII_EURUSD_T_201603.csv",),
                source_uri="https://www.histdata.com/download-free-forex-historical-data/",
                terms_uri="https://www.histdata.com/f-a-q/data-files-detailed-specification/",
                acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
            )
            manifest = DatasetManifest(
                dataset_id=f"histdata:EUR/USD:201603:{digest[:32]}",
                provider=HISTDATA_PROVIDER,
                instrument=HISTDATA_INSTRUMENT,
                partitions=(partition,),
                coverage_start=None,
                coverage_end=None,
                content_hash=hashlib.sha256(
                    f"201603\0{digest}\0{archive.stat().st_size}\0DAT_ASCII_EURUSD_T_201603.csv".encode()
                ).hexdigest(),
                data_root=str(data_root),
            )
            result = validate(manifest)
            self.assertFalse(result.ok)
            self.assertTrue(any("order violation" in issue for issue in result.issues))
            archive.write_bytes(archive.read_bytes() + b"x")
            self.assertFalse(validate_dataset(manifest).ok)

    def test_invalid_quote_contracts_and_unsafe_locator_are_rejected(self) -> None:
        with self.assertRaises(HistoricalDataError):
            SourceLocator("raw/archive.zip", "../outside.csv", "0" * 64, 0, 1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, data_root, _ = self.make_archive(root, b"20160307 000000000,1.2,1.1,0\n")
            with self.assertRaises(HistoricalDataError):
                manifest_from_histdata_archive(
                    archive,
                    data_root=data_root,
                    acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
                    source_uri="source",
                    terms_uri="terms",
                )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(HistoricalDataError):
                list(iter_quotes(self.make_manifest(root), datetime(2016, 3, 8), None))

    def test_manifest_and_raw_paths_reject_symlink_ancestors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self.make_manifest(root)
            manifest_path = root / "real" / "manifest.json"
            manifest_path.parent.mkdir()
            manifest_path.write_text(manifest.to_json(), encoding="utf-8")
            manifest_alias = root / "manifest-alias"
            manifest_alias.symlink_to(manifest_path.parent, target_is_directory=True)
            with self.assertRaises(HistoricalDataError):
                read_manifest(manifest_alias / manifest_path.name)

            raw_alias = Path(manifest.data_root) / "raw-alias"
            raw_alias.symlink_to(Path(manifest.data_root) / "raw", target_is_directory=True)
            partition = manifest.partitions[0]
            aliased_partition = DatasetPartition(
                partition_id=partition.partition_id,
                raw_archive=f"{raw_alias.name}/{Path(partition.raw_archive).name}",
                raw_sha256=partition.raw_sha256,
                raw_size=partition.raw_size,
                members=partition.members,
                source_uri=partition.source_uri,
                terms_uri=partition.terms_uri,
                acquired_at=partition.acquired_at,
                coverage_start=partition.coverage_start,
                coverage_end=partition.coverage_end,
                quote_count=partition.quote_count,
                month=partition.month,
            )
            aliased_manifest = DatasetManifest(
                dataset_id=manifest.dataset_id,
                provider=manifest.provider,
                instrument=manifest.instrument,
                partitions=(aliased_partition,),
                coverage_start=manifest.coverage_start,
                coverage_end=manifest.coverage_end,
                content_hash=manifest.content_hash,
                data_root=manifest.data_root,
            )
            self.assertFalse(validate_dataset(aliased_manifest).ok)

            data_root_alias = root / "data-root-alias"
            data_root_alias.symlink_to(Path(manifest.data_root), target_is_directory=True)
            raw = json.loads(manifest.to_json())
            raw["data_root"] = str(data_root_alias)
            with self.assertRaises(HistoricalDataError):
                DatasetManifest.from_mapping(raw)

    def test_validation_caps_issues_but_preserves_total_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lines = []
            for second in reversed(range(300)):
                local = datetime(2016, 3, 7) + timedelta(seconds=second)
                lines.append(f"{local:%Y%m%d %H%M%S}000,1.2,1.3,0\n")
            archive, data_root, digest = self.make_archive(root, "".join(lines).encode())
            partition = DatasetPartition(
                partition_id="201603",
                raw_archive="raw/HISTDATA_COM_ASCII_EURUSD_T_201603.zip",
                raw_sha256=digest,
                raw_size=archive.stat().st_size,
                members=("DAT_ASCII_EURUSD_T_201603.csv",),
                source_uri="https://www.histdata.com/download-free-forex-historical-data/",
                terms_uri="https://www.histdata.com/f-a-q/data-files-detailed-specification/",
                acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
            )
            manifest = DatasetManifest(
                dataset_id=f"histdata:EUR/USD:201603:{digest[:32]}",
                provider=HISTDATA_PROVIDER,
                instrument=HISTDATA_INSTRUMENT,
                partitions=(partition,),
                coverage_start=None,
                coverage_end=None,
                content_hash=hashlib.sha256(
                    f"201603\0{digest}\0{archive.stat().st_size}\0DAT_ASCII_EURUSD_T_201603.csv".encode()
                ).hexdigest(),
                data_root=str(data_root),
            )
            result = validate_dataset(manifest)
        self.assertFalse(result.ok)
        self.assertGreaterEqual(result.issues_total, 299)
        self.assertEqual(len(result.issues), 128)
        self.assertTrue(result.issues_truncated)


class _Response:
    def __init__(self, body: bytes, url: str, *, headers: dict[str, str] | None = None, code: int = 200) -> None:
        self.body = io.BytesIO(body)
        self.url = url
        self.headers = headers or {}
        self.code = code

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def geturl(self) -> str:
        return self.url

    def getcode(self) -> int:
        return self.code

    def read(self, size: int = -1) -> bytes:
        return self.body.read(size)


class _Opener:
    def __init__(self, archive: bytes) -> None:
        self.archive = archive
        self.requests: list[Any] = []

    def open(self, request: Any, timeout: float) -> _Response:
        self.requests.append(request)
        if request.full_url == histdata_acquire.HISTDATA_MONTH_PAGE:
            form = (
                '<form id="file_down" action="/get.php" method="POST">'
                '<input type="hidden" name="tk" value="fixture-token">'
                '<input type="hidden" name="date" value="2016">'
                '<input type="hidden" name="datemonth" value="201603">'
                '<input type="hidden" name="platform" value="ASCII">'
                '<input type="hidden" name="timeframe" value="T">'
                '<input type="hidden" name="fxpair" value="EURUSD">'
                "</form>"
            ).encode("ascii")
            return _Response(form, histdata_acquire.HISTDATA_MONTH_PAGE)
        return _Response(
            self.archive,
            histdata_acquire.HISTDATA_DOWNLOAD_ENDPOINT,
            headers={"Content-Length": str(len(self.archive)), "ETag": '"fixture-etag"'},
        )


class HistoricalAcquisitionTests(HistoricalFixtureMixin, unittest.TestCase):
    @staticmethod
    def archive_bytes() -> bytes:
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "DAT_ASCII_EURUSD_T_201603.csv",
                b"20160307 000000000,1.10000,1.10020,0\n20160307 000100000,1.10001,1.10021,0\n",
            )
        return output.getvalue()

    def test_acquire_is_scoped_to_terms_and_march_container(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "market-data"
            archive = self.archive_bytes()
            opener = _Opener(archive)
            receipt = histdata_acquire.acquire(
                provider="histdata",
                instrument="EURUSD",
                month="2016-03",
                data_root=data_root,
                terms_accepted=True,
                opener=cast(Any, opener),
            )
            self.assertEqual(receipt.status, "DOWNLOADED")
            self.assertEqual(receipt.etag, '"fixture-etag"')
            self.assertEqual(receipt.archive_path.read_bytes(), archive)
            self.assertTrue(receipt.manifest_path.is_file())
            self.assertTrue(validate_dataset(receipt.manifest_path).ok)
            self.assertEqual(len(opener.requests), 2)
            self.assertEqual(opener.requests[1].full_url, histdata_acquire.HISTDATA_DOWNLOAD_ENDPOINT)
            self.assertIn(b"fxpair=EURUSD", opener.requests[1].data or b"")
            self.assertFalse((data_root / "raw/.HISTDATA_COM_ASCII_EURUSD_T_201603.zip.part").exists())
        self.assertFalse(data_root.exists())

    def test_acquire_rejects_unapproved_scope_or_missing_terms_before_writing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            for kwargs in (
                {"month": "2016-04", "terms_accepted": True},
                {"terms_accepted": False},
                {"provider": "dukascopy", "terms_accepted": True},
            ):
                with self.assertRaises(histdata_acquire.AcquisitionError):
                    histdata_acquire.acquire(data_root=root, **kwargs)
                self.assertFalse(root.exists())

    def test_existing_partial_verifies_source_size_hash_and_offset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            part = root / "archive.part"
            state = root / "archive.state.json"
            part.write_bytes(b"abc")
            action = "https://www.histdata.com/get.php"
            digest = hashlib.sha256(part.read_bytes()).hexdigest()
            acquisition_impl._write_state(
                state,
                action=action,
                etag='"etag"',
                expected_total=6,
                received=3,
                sha256=digest,
            )
            saved = acquisition_impl._existing_partial(part, state, action)
            self.assertIsNotNone(saved)
            assert saved is not None
            self.assertEqual(saved.offset, 3)
            self.assertEqual(saved.expected_total, 6)
            raw = json.loads(state.read_text(encoding="utf-8"))
            for key, value in (("received", 2), ("sha256", "0" * 64), ("url", "other")):
                tampered = dict(raw)
                tampered[key] = value
                state.write_text(json.dumps(tampered), encoding="utf-8")
                with self.assertRaises(histdata_acquire.AcquisitionError):
                    acquisition_impl._existing_partial(part, state, action)
            acquisition_impl._write_state(
                state,
                action=action,
                etag='"etag"',
                expected_total=6,
                received=3,
                sha256=digest,
            )
            response = _Response(
                b"",
                histdata_acquire.HISTDATA_DOWNLOAD_ENDPOINT,
                headers={"Content-Range": "bytes 2-5/6", "Content-Length": "4"},
                code=206,
            )
            with self.assertRaises(histdata_acquire.AcquisitionError):
                acquisition_impl._response_total(response, 206, 3, 4)
            with self.assertRaises(histdata_acquire.AcquisitionError):
                acquisition_impl._check_resume_headers(
                    response,
                    status=206,
                    offset=3,
                    content_length=4,
                    expected_total=6,
                    prior=acquisition_impl._PartialState(3, '"old"', 6, digest),
                    response_etag='"new"',
                )

    def test_partial_download_requires_and_uses_exact_content_range(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            (root / "raw").mkdir(parents=True)
            os.chmod(root, 0o700)
            os.chmod(root / "raw", 0o700)
            source = self.archive_bytes()
            split = 17
            part = root / "raw" / ".archive.part"
            state = root / "raw" / ".archive.state.json"
            target = root / "raw" / "archive.zip"
            part.write_bytes(source[:split])
            acquisition_impl._write_state(
                state,
                action="https://www.histdata.com/get.php",
                etag='"etag"',
                expected_total=len(source),
                received=split,
                sha256=hashlib.sha256(source[:split]).hexdigest(),
            )

            class RangeOpener:
                def __init__(self) -> None:
                    self.request: Any | None = None

                def open(self, request: Any, timeout: float) -> _Response:
                    self.request = request
                    return _Response(
                        source[split:],
                        histdata_acquire.HISTDATA_DOWNLOAD_ENDPOINT,
                        headers={
                            "Content-Length": str(len(source) - split),
                            "Content-Range": f"bytes {split}-{len(source) - 1}/{len(source)}",
                            "ETag": '"etag"',
                        },
                        code=206,
                    )

            opener = RangeOpener()
            result = acquisition_impl._download(
                cast(Any, opener),
                "https://www.histdata.com/get.php",
                {"fxpair": "EURUSD"},
                part_path=part,
                state_path=state,
                archive_path=target,
                timeout_seconds=10,
                root=root,
            )
            self.assertEqual(target.read_bytes(), source)
            self.assertEqual(result[0], hashlib.sha256(source).hexdigest())
            assert opener.request is not None
            self.assertEqual(opener.request.headers.get("Range"), f"bytes={split}-")

    def test_acquisition_root_and_existing_artifacts_require_private_modes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            root.mkdir(mode=0o755)
            os.chmod(root, 0o755)
            with self.assertRaisesRegex(histdata_acquire.AcquisitionError, "mode 700"):
                acquisition_impl._prepare_root(root)
            self.assertFalse((root / "raw").exists())

    def test_existing_archive_rejects_manifest_root_or_raw_path_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, data_root, _ = self.make_archive(root)
            os.chmod(data_root, 0o700)
            os.chmod(data_root / "raw", 0o700)
            # Keep the current archive and its valid manifest in the same root.
            manifest = manifest_from_histdata_archive(
                archive,
                data_root=data_root,
                acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
                source_uri="https://www.histdata.com/download-free-forex-data/",
                terms_uri="https://www.histdata.com/f-a-q/data-files-detailed-specification/",
            )
            manifest_path = data_root / "manifests"
            manifest_path.mkdir(mode=0o700)
            manifest_file = manifest_path / "manifest.json"
            manifest_file.write_text(manifest.to_json() + "\n", encoding="utf-8")
            os.chmod(archive, 0o600)
            os.chmod(manifest_file, 0o600)
            with self.assertRaises(histdata_acquire.AcquisitionError):
                acquisition_impl._reuse_existing(archive, manifest_file, root / "other-root")

            other_archive = data_root / "raw" / "other.zip"
            other_archive.write_bytes(archive.read_bytes())
            os.chmod(other_archive, 0o600)
            raw = manifest.to_dict()
            raw["partitions"][0]["raw_archive"] = "raw/other.zip"
            manifest_file.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaises(histdata_acquire.AcquisitionError):
                acquisition_impl._reuse_existing(archive, manifest_file, data_root)

    def test_partial_state_persistence_failure_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            part = root / "archive.part"
            state = root / "archive.state.json"
            part.write_bytes(b"partial")
            with (
                mock.patch.object(acquisition_impl, "_write_state", side_effect=OSError("fixture")),
                self.assertRaisesRegex(histdata_acquire.AcquisitionError, "partial-state persistence"),
            ):
                acquisition_impl._preserve_or_raise(
                    part,
                    state,
                    "https://www.histdata.com/get.php",
                    None,
                    100,
                    OSError("download"),
                )


if __name__ == "__main__":
    unittest.main()
