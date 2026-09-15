"""Offline contracts for the bounded official Dukascopy tick adapter."""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.error import URLError

import mtf_lab.data.historical as historical
from mtf_lab.data.dukascopy import (
    DUKASCOPY_CONFIG_URI,
    DUKASCOPY_DEFAULT_DATA_ROOT,
    DUKASCOPY_FORMAT,
    DUKASCOPY_INSTRUMENT,
    DUKASCOPY_INTERVAL_CODE,
    DUKASCOPY_MAX_BYTES,
    DUKASCOPY_NORMALIZATION_VERSION,
    DUKASCOPY_PROVIDER,
    DUKASCOPY_SOURCE_TIMEZONE,
    DUKASCOPY_SOURCE_URI,
    DUKASCOPY_SYMBOL,
    DUKASCOPY_TERMS_URI,
    DUKASCOPY_WIDGET_URI,
    DUKASCOPY_WINDOW_END,
    DUKASCOPY_WINDOW_START,
    DukascopyError,
    DukascopyManifest,
    DukascopyPartition,
    decode_tick_payload,
    iter_provider_quotes,
    iter_quotes,
    manifest_content_hash,
    read_manifest,
    validate_manifest,
)
from mtf_lab.data.dukascopy_acquisition import AcquisitionError, acquire, acquire_week


def payload_for(hour: datetime) -> bytes:
    timestamp = int(hour.timestamp() * 1000)
    value = {
        "timestamp": timestamp,
        "bid": "1.10000",
        "ask": "1.10020",
        "multiplier": "0.00001",
        "times": [0, 500, 1000],
        "bids": [0, 1, -1],
        "asks": [0, 1, -1],
        "bidVolumes": [0, 0, 0],
        "askVolumes": [0, 0, 0],
    }
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


class Response:
    def __init__(self, body: bytes, url: str, *, code: int = 200) -> None:
        self.body = io.BytesIO(body)
        self.url = url
        self.code = code
        self.headers = {"Content-Length": str(len(body)), "Content-Type": "application/json"}

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def geturl(self) -> str:
        return self.url

    def getcode(self) -> int:
        return self.code

    def read(self, size: int = -1) -> bytes:
        return self.body.read(size)


class PublicOpener:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def open(self, request: Any, timeout: float) -> Response:
        self.urls.append(request.full_url)
        if request.full_url == DUKASCOPY_CONFIG_URI:
            return Response(b'{"JETTA_SERVER_URL":"https://jetta.test.dukascopy.com"}', request.full_url)
        if request.full_url.endswith("/instruments/EUR-USD"):
            body = json.dumps(
                {
                    "code": DUKASCOPY_SYMBOL,
                    "name": "EUR/USD",
                    "histories": [{"period": "1T", "from": 0}],
                },
                separators=(",", ":"),
            ).encode("utf-8")
            return Response(body, request.full_url)
        prefix = "https://jetta.test.dukascopy.com/v1/ticks/EUR-USD/"
        if request.full_url.startswith(prefix):
            parts = request.full_url[len(prefix) :].split("/")
            if len(parts) != 4:
                raise AssertionError(request.full_url)
            hour = datetime(int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3]), tzinfo=UTC)
            return Response(payload_for(hour), request.full_url)
        raise AssertionError(request.full_url)


class FailingOpener:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def open(self, request: Any, timeout: float) -> Response:
        self.urls.append(request.full_url)
        raise URLError("test network timeout")


def make_manifest(root: Path) -> DukascopyManifest:
    raw = root / "raw"
    raw.mkdir(mode=0o700)
    start = DUKASCOPY_WINDOW_START
    end = start + timedelta(hours=1)
    body = payload_for(start)
    path = raw / "ticks-20160307T00Z.json"
    path.write_bytes(body)
    os.chmod(path, 0o600)
    digest = hashlib.sha256(body).hexdigest()
    partition = DukascopyPartition(
        partition_id=start.isoformat().replace("+00:00", "Z"),
        raw_path="raw/ticks-20160307T00Z.json",
        raw_sha256=digest,
        raw_size=len(body),
        source_uri="https://jetta.test.dukascopy.com/v1/ticks/EUR-USD/2016/3/7/0",
        instrument=DUKASCOPY_INSTRUMENT,
        interval_code=DUKASCOPY_INTERVAL_CODE,
        start=start,
        end=end,
        coverage_start=start,
        coverage_end=start + timedelta(seconds=1, milliseconds=500),
        quote_count=3,
    )
    return DukascopyManifest(
        dataset_id=f"dukascopy:EUR/USD:20160307-20160314:{manifest_content_hash((partition,))[:32]}",
        provider=DUKASCOPY_PROVIDER,
        instrument=DUKASCOPY_INSTRUMENT,
        normalization_version=DUKASCOPY_NORMALIZATION_VERSION,
        format=DUKASCOPY_FORMAT,
        source_uri=DUKASCOPY_SOURCE_URI,
        widget_uri=DUKASCOPY_WIDGET_URI,
        config_uri=DUKASCOPY_CONFIG_URI,
        terms_uri=DUKASCOPY_TERMS_URI,
        api_base="https://jetta.test.dukascopy.com/v1",
        source_timezone=DUKASCOPY_SOURCE_TIMEZONE,
        availability_basis="historical_event_time",
        data_root=str(root),
        window_start=DUKASCOPY_WINDOW_START,
        window_end=DUKASCOPY_WINDOW_END,
        coverage_start=start,
        coverage_end=start + timedelta(seconds=1, milliseconds=500),
        quote_count=3,
        raw_bytes=len(body),
        content_hash=manifest_content_hash((partition,)),
        partitions=(partition,),
    )


class DukascopyDecoderTests(unittest.TestCase):
    def test_decoder_preserves_bbo_source_time_and_no_traded_volume(self) -> None:
        start = DUKASCOPY_WINDOW_START
        quotes = decode_tick_payload(
            payload_for(start),
            raw_path="raw/ticks.json",
            raw_sha256="a" * 64,
            partition_start=start,
            partition_end=start + timedelta(hours=1),
        )
        self.assertEqual(len(quotes), 3)
        self.assertEqual([item.sequence for item in quotes], [0, 1, 2])
        self.assertEqual(quotes[1].event_time, start + timedelta(milliseconds=500))
        self.assertEqual(quotes[0].bid, Decimal("1.10000"))
        self.assertEqual(quotes[1].ask, Decimal("1.10021"))
        self.assertEqual(quotes[0].provider, DUKASCOPY_PROVIDER)
        self.assertEqual(quotes[0].source_timestamp, str(int(start.timestamp() * 1000) + 0))
        self.assertEqual(quotes[0].source_timezone, DUKASCOPY_SOURCE_TIMEZONE)
        self.assertEqual(quotes[0].precision, "multiplier=0.00001")
        self.assertEqual(quotes[0].source_locator.kind, "compact_json_tick_index")
        self.assertNotIn("volume", quotes[0].to_dict())
        self.assertNotIn("quantity", quotes[0].to_dict())

    def test_decoder_rejects_malformed_compact_arrays_and_price_contract(self) -> None:
        start = DUKASCOPY_WINDOW_START
        good = json.loads(payload_for(start))
        cases = []
        bad_lengths = dict(good)
        bad_lengths["asks"] = [0]
        cases.append(bad_lengths)
        bad_order = dict(good)
        bad_order["times"] = [0, -1, 1]
        cases.append(bad_order)
        bad_price = dict(good)
        bad_price["ask"] = "1.00000"
        bad_price["bids"] = [0, 0, 0]
        bad_price["asks"] = [0, 0, 0]
        cases.append(bad_price)
        for case in cases:
            with self.subTest(case=case), self.assertRaises(DukascopyError):
                decode_tick_payload(
                    json.dumps(case).encode("utf-8"),
                    raw_path="raw/ticks.json",
                    raw_sha256="a" * 64,
                    partition_start=start,
                    partition_end=start + timedelta(hours=1),
                )

    def test_manifest_roundtrip_and_provider_dispatch_are_strict(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-dukascopy-manifest-") as directory:
            root = Path(directory)
            manifest = make_manifest(root)
            path = root / "manifest.json"
            path.write_text(manifest.to_json() + "\n", encoding="utf-8")
            os.chmod(path, 0o600)
            restored = read_manifest(path)
            self.assertEqual(restored.to_dict(), manifest.to_dict())
            self.assertTrue(validate_manifest(restored).ok)
            quotes = list(iter_quotes(restored))
            dispatched = list(iter_provider_quotes(path))
            bridge_manifest = historical.read_manifest(path)
            bridge_quotes = list(historical.iter_quotes(path))
            bridge_validation = historical.validate_dataset(path)
        self.assertEqual(quotes, dispatched)
        self.assertIsInstance(bridge_manifest, type(manifest))
        self.assertEqual(bridge_quotes, quotes)
        self.assertTrue(bridge_validation.ok)
        self.assertEqual([item.sequence for item in quotes], [0, 1, 2])


class DukascopyAcquisitionTests(unittest.TestCase):
    def test_acquire_is_exact_week_and_preserves_each_native_response(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-dukascopy-acquire-") as directory:
            root = Path(directory)
            opener = PublicOpener()
            receipt = acquire(
                provider=DUKASCOPY_PROVIDER,
                instrument="EURUSD",
                start=DUKASCOPY_WINDOW_START,
                end=DUKASCOPY_WINDOW_END,
                data_root=root,
                free_web_authorized=True,
                provider_authorization_ref="fixture:written-provider-consent",
                opener=opener,
            )
            self.assertEqual(receipt.status, "DOWNLOADED")
            self.assertEqual(receipt.completed_hours, 168)
            self.assertEqual(receipt.quote_count, 504)
            self.assertEqual(receipt.source_timezone, DUKASCOPY_SOURCE_TIMEZONE)
            self.assertEqual(receipt.manifest_path, root / "manifests" / "dukascopy-eurusd-20160307-20160314.json")
            manifest = read_manifest(receipt.manifest_path)
            self.assertTrue(validate_manifest(manifest).ok)
            raw_paths = sorted((root / "raw").glob("ticks-*.json"))
            self.assertEqual(len(raw_paths), 168)
            self.assertEqual(raw_paths[0].read_bytes(), payload_for(DUKASCOPY_WINDOW_START))
            self.assertEqual(raw_paths[-1].read_bytes(), payload_for(DUKASCOPY_WINDOW_END - timedelta(hours=1)))
            self.assertEqual(len([url for url in opener.urls if "/ticks/" in url]), 168)
            self.assertTrue(all("/v1/ticks/EUR-USD/" in url for url in opener.urls if "/ticks/" in url))
            self.assertTrue((root / "receipts" / "dukascopy-eurusd-20160307-20160314.json").is_file())
            self.assertEqual(
                (root / "receipts" / "dukascopy-eurusd-20160307-20160314.json").stat().st_mode & 0o777, 0o600
            )

    def test_scope_and_free_web_gate_reject_before_network(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-dukascopy-scope-") as directory:
            root = Path(directory)
            opener = PublicOpener()
            for kwargs in (
                {"free_web_authorized": False},
                {"provider": "histdata", "free_web_authorized": True},
                {
                    "end": DUKASCOPY_WINDOW_END + timedelta(hours=1),
                    "free_web_authorized": True,
                    "provider_authorization_ref": "fixture:written-provider-consent",
                },
            ):
                with self.assertRaises(AcquisitionError):
                    acquire(data_root=root, opener=opener, **kwargs)
            self.assertEqual(opener.urls, [])

    def test_provider_written_authorization_is_required_before_network(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-dukascopy-auth-gate-") as directory:
            root = Path(directory)
            opener = PublicOpener()
            with self.assertRaisesRegex(AcquisitionError, "provider_authorization_ref"):
                acquire_week(data_root=root, free_web_authorized=True, opener=opener)
            self.assertEqual(opener.urls, [])
            self.assertFalse((root / "raw").exists())

    def test_network_timeout_writes_blocked_receipt_and_no_manifest(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-dukascopy-blocked-") as directory:
            root = Path(directory)
            opener = FailingOpener()
            with self.assertRaises(AcquisitionError):
                acquire_week(
                    data_root=root,
                    free_web_authorized=True,
                    provider_authorization_ref="fixture:written-provider-consent",
                    opener=opener,
                    timeout_seconds=0.1,
                )
            receipt_path = root / "receipts" / "dukascopy-eurusd-20160307-20160314.json"
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            self.assertEqual(receipt["status"], "BLOCKED")
            self.assertTrue(receipt["network_performed"])
            self.assertFalse((root / "manifests" / "dukascopy-eurusd-20160307-20160314.json").exists())
            self.assertEqual(opener.urls, [DUKASCOPY_CONFIG_URI])

    def test_dedicated_root_and_size_constant_are_explicit(self) -> None:
        self.assertNotEqual(DUKASCOPY_DEFAULT_DATA_ROOT, historical.DEFAULT_DATA_ROOT)
        self.assertEqual(DUKASCOPY_MAX_BYTES, 200 * 1024**2)


if __name__ == "__main__":
    unittest.main()
