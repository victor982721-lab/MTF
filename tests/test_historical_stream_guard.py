"""Bounded source-identity and resume contracts for historical streams."""

from __future__ import annotations

import hashlib
import json
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.data.dukascopy import (
    DUKASCOPY_CONFIG_URI,
    DUKASCOPY_FORMAT,
    DUKASCOPY_INSTRUMENT,
    DUKASCOPY_INTERVAL_CODE,
    DUKASCOPY_NORMALIZATION_VERSION,
    DUKASCOPY_PROVIDER,
    DUKASCOPY_SOURCE_TIMEZONE,
    DUKASCOPY_SOURCE_URI,
    DUKASCOPY_TERMS_URI,
    DUKASCOPY_WIDGET_URI,
    DUKASCOPY_WINDOW_END,
    DUKASCOPY_WINDOW_START,
    DukascopyManifest,
    DukascopyPartition,
    manifest_content_hash,
)
from mtf_lab.data.historical import (
    HISTDATA_INSTRUMENT,
    HISTDATA_PROVIDER,
    DatasetManifest,
    DatasetPartition,
    HistoricalQuote,
    SourceLocator,
)
from mtf_lab.data.historical_stream_guard import (
    HistoricalStreamIdentityError,
    HistoricalStreamIdentityGuard,
)

DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
BASE = datetime(2016, 3, 7, 5, tzinfo=UTC)


def histdata_manifest(*, members: tuple[str, ...] = ("quotes.csv",)) -> DatasetManifest:
    partition = DatasetPartition(
        partition_id="201603",
        raw_archive="raw/fixture.zip",
        raw_sha256=DIGEST_A,
        raw_size=1,
        members=members,
        source_uri="local-fixture",
        terms_uri="local-fixture",
        acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
    )
    content_hash = hashlib.sha256(
        "\n".join(
            f"{partition.partition_id}\0{partition.raw_sha256}\0{partition.raw_size}\0{','.join(partition.members)}"
            for partition in (partition,)
        ).encode()
    ).hexdigest()
    return DatasetManifest(
        dataset_id=f"histdata:{HISTDATA_INSTRUMENT}:201603:{DIGEST_A[:32]}",
        provider=HISTDATA_PROVIDER,
        instrument=HISTDATA_INSTRUMENT,
        partitions=(partition,),
        coverage_start=None,
        coverage_end=None,
        content_hash=content_hash,
        data_root="/tmp/mtf-historical-guard",
    )


def hist_quote(
    index: int,
    *,
    member: str = "quotes.csv",
    when: datetime | None = None,
    sequence: int | None = None,
    archive: str = "raw/fixture.zip",
    sha256: str = DIGEST_A,
    provider: str = HISTDATA_PROVIDER,
) -> HistoricalQuote:
    offset = index * 32
    row = index + 1
    event_time = when or (BASE + timedelta(milliseconds=index))
    locator = SourceLocator(archive, member, sha256, offset, row)
    return HistoricalQuote(
        instrument=HISTDATA_INSTRUMENT,
        event_time=event_time,
        available_at=event_time,
        bid=Decimal("1.10000"),
        ask=Decimal("1.10020"),
        locator=locator,
        sequence=index if sequence is None else sequence,
        source_event_id=f"{sha256}:{member}:{row}:{offset}",
        provider=provider,
    )


def duka_manifest(root: str = "/tmp/mtf-duka-guard") -> DukascopyManifest:
    first = DUKASCOPY_WINDOW_START
    partitions = []
    for index, (start, digest) in enumerate(((first, DIGEST_A), (first + timedelta(hours=1), DIGEST_B))):
        end = start + timedelta(hours=1)
        partition = DukascopyPartition(
            partition_id=start.isoformat().replace("+00:00", "Z"),
            raw_path=f"raw/ticks-{index}.json",
            raw_sha256=digest,
            raw_size=1,
            source_uri=f"https://jetta.test.dukascopy.com/v1/ticks/EUR-USD/{index}",
            instrument=DUKASCOPY_INSTRUMENT,
            interval_code=DUKASCOPY_INTERVAL_CODE,
            start=start,
            end=end,
            coverage_start=None,
            coverage_end=None,
            quote_count=0,
        )
        partitions.append(partition)
    typed = tuple(partitions)
    content_hash = manifest_content_hash(typed)
    return DukascopyManifest(
        dataset_id=f"dukascopy:{DUKASCOPY_INSTRUMENT}:20160307-20160314:{content_hash[:32]}",
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
        data_root=root,
        window_start=DUKASCOPY_WINDOW_START,
        window_end=DUKASCOPY_WINDOW_END,
        coverage_start=None,
        coverage_end=None,
        quote_count=0,
        raw_bytes=2,
        content_hash=content_hash,
        partitions=typed,
    )


def duka_quote(index: int, *, when: datetime, sequence: int, partition: int = 0) -> HistoricalQuote:
    sha256 = (DIGEST_A, DIGEST_B)[partition]
    archive = f"raw/ticks-{partition}.json"
    offset = index * 11
    row = index + 1
    locator = SourceLocator(archive, "ticks", sha256, offset, row, kind="compact_json_tick_index")
    return HistoricalQuote(
        instrument=DUKASCOPY_INSTRUMENT,
        event_time=when,
        available_at=when,
        bid=Decimal("1.10000"),
        ask=Decimal("1.10020"),
        locator=locator,
        sequence=sequence,
        source_event_id=f"{sha256}:ticks:{row}:{offset}",
        provider=DUKASCOPY_PROVIDER,
        source_timezone=DUKASCOPY_SOURCE_TIMEZONE,
        precision="multiplier=0.00001",
    )


class HistoricalStreamIdentityGuardTests(unittest.TestCase):
    def test_histdata_order_and_equal_timestamps_use_source_tie_breakers(self) -> None:
        guard = HistoricalStreamIdentityGuard(histdata_manifest())
        first = hist_quote(0, when=BASE)
        same_time = hist_quote(1, when=BASE)
        guard.validate(first)
        guard.validate(same_time)
        self.assertEqual(guard.cursor.event_time if guard.cursor else None, BASE)
        self.assertEqual(guard.cursor.locator.row if guard.cursor else None, 2)

    def test_duplicate_locator_is_rejected_even_when_sequence_changes(self) -> None:
        guard = HistoricalStreamIdentityGuard(histdata_manifest())
        guard.validate(hist_quote(0))
        duplicate = hist_quote(0, sequence=100)
        with self.assertRaises(HistoricalStreamIdentityError):
            guard.validate(duplicate)
        self.assertEqual(guard.cursor.sequence if guard.cursor else None, 0)

    def test_member_order_is_strict_and_state_is_transactional(self) -> None:
        guard = HistoricalStreamIdentityGuard(histdata_manifest(members=("a.csv", "b.csv")))
        guard.validate(hist_quote(0, member="a.csv"))
        guard.validate(hist_quote(1, member="b.csv", sequence=1))
        # A return to a previous member is rejected after a valid transition.
        with self.assertRaises(HistoricalStreamIdentityError):
            guard.validate(hist_quote(2, member="a.csv", sequence=2))

    def test_wrong_provider_sha_member_and_archive_are_rejected(self) -> None:
        manifest = histdata_manifest()
        cases = (
            hist_quote(0, provider="other"),
            hist_quote(0, sha256=DIGEST_B),
            hist_quote(0, member="other.csv"),
            hist_quote(0, archive="raw/other.zip"),
        )
        for quote in cases:
            with self.subTest(quote=quote), self.assertRaises(HistoricalStreamIdentityError):
                HistoricalStreamIdentityGuard(manifest).validate(quote)

    def test_snapshot_roundtrip_binds_manifest_and_locator(self) -> None:
        manifest = histdata_manifest()
        guard = HistoricalStreamIdentityGuard(manifest)
        guard.validate(hist_quote(0))
        snapshot = guard.snapshot()
        self.assertEqual(snapshot["manifest_hash"], manifest.content_hash)
        self.assertEqual(snapshot["locator"]["row"], 1)
        restored = HistoricalStreamIdentityGuard.from_snapshot(manifest, snapshot)
        restored.validate(hist_quote(1, sequence=1))
        self.assertEqual(restored.cursor.locator.row if restored.cursor else None, 2)

        tampered = dict(snapshot)
        tampered["manifest_hash"] = "f" * 64
        with self.assertRaises(HistoricalStreamIdentityError):
            HistoricalStreamIdentityGuard.from_snapshot(manifest, tampered)
        tampered_locator = json.loads(json.dumps(snapshot))
        tampered_locator["locator"]["row"] = 9
        with self.assertRaises(HistoricalStreamIdentityError):
            HistoricalStreamIdentityGuard.from_snapshot(manifest, tampered_locator)

    def test_empty_snapshot_is_a_safe_fresh_cursor(self) -> None:
        manifest = histdata_manifest()
        guard = HistoricalStreamIdentityGuard.from_snapshot(
            manifest,
            HistoricalStreamIdentityGuard(manifest).snapshot(),
        )
        guard.validate(hist_quote(0))

    def test_dukascopy_partition_transition_and_return_are_rejected(self) -> None:
        manifest = duka_manifest()
        guard = HistoricalStreamIdentityGuard(manifest)
        guard.validate(duka_quote(0, when=DUKASCOPY_WINDOW_START, sequence=0, partition=0))
        guard.validate(duka_quote(0, when=DUKASCOPY_WINDOW_START + timedelta(hours=1), sequence=1, partition=1))
        with self.assertRaises(HistoricalStreamIdentityError):
            guard.validate(duka_quote(1, when=DUKASCOPY_WINDOW_START + timedelta(hours=1), sequence=2, partition=0))

    def test_duplicate_manifest_sha_member_is_rejected_without_quote_memory(self) -> None:
        first = DUKASCOPY_WINDOW_START
        partition_a = DukascopyPartition(
            partition_id="a",
            raw_path="raw/a.json",
            raw_sha256=DIGEST_A,
            raw_size=1,
            source_uri="https://jetta.test.dukascopy.com/a",
            instrument=DUKASCOPY_INSTRUMENT,
            interval_code=DUKASCOPY_INTERVAL_CODE,
            start=first,
            end=first + timedelta(hours=1),
            coverage_start=None,
            coverage_end=None,
            quote_count=0,
        )
        partition_b = DukascopyPartition(
            partition_id="b",
            raw_path="raw/b.json",
            raw_sha256=DIGEST_A,
            raw_size=1,
            source_uri="https://jetta.test.dukascopy.com/b",
            instrument=DUKASCOPY_INSTRUMENT,
            interval_code=DUKASCOPY_INTERVAL_CODE,
            start=first + timedelta(hours=1),
            end=first + timedelta(hours=2),
            coverage_start=None,
            coverage_end=None,
            quote_count=0,
        )
        parts = (partition_a, partition_b)
        content_hash = manifest_content_hash(parts)
        manifest = DukascopyManifest(
            dataset_id=f"dukascopy:{DUKASCOPY_INSTRUMENT}:20160307-20160314:{content_hash[:32]}",
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
            data_root="/tmp/mtf-duka-duplicate",
            window_start=DUKASCOPY_WINDOW_START,
            window_end=DUKASCOPY_WINDOW_END,
            coverage_start=None,
            coverage_end=None,
            quote_count=0,
            raw_bytes=2,
            content_hash=content_hash,
            partitions=parts,
        )
        with self.assertRaises(HistoricalStreamIdentityError):
            HistoricalStreamIdentityGuard(manifest)

    def test_no_seen_quote_set_is_retained(self) -> None:
        guard = HistoricalStreamIdentityGuard(histdata_manifest())
        self.assertNotIn("_seen", guard.__slots__)
        self.assertNotIn("_source_event_ids", guard.__slots__)
        self.assertEqual(len(guard.__slots__), 8)


if __name__ == "__main__":
    unittest.main()
