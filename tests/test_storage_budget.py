"""Offline contracts for the aggregate raw-storage guard."""

from __future__ import annotations

import os
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import mtf_lab.data.histdata_acquisition as histdata_acquisition
from mtf_lab.data.storage_budget import (
    StorageBudgetError,
    acquisition_lock,
    ceil_ratio,
    guard_budget,
    inventory_raw,
    project_budget,
    shared_lock_root,
)


def _statvfs(*, blocks: int, available: int, fragment: int = 1) -> SimpleNamespace:
    return SimpleNamespace(f_blocks=blocks, f_bavail=available, f_frsize=fragment, f_bsize=fragment)


class StorageBudgetTests(unittest.TestCase):
    def test_inventory_deduplicates_hardlinks_and_ignores_repeated_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            duka = root / "dukascopy"
            (root / "raw").mkdir(parents=True)
            (root / "manifests").mkdir()
            (duka / "raw").mkdir(parents=True)
            (duka / "manifests").mkdir()
            (root / "raw" / "one.bin").write_bytes(b"12345")
            os.link(root / "raw" / "one.bin", duka / "raw" / "hardlink.bin")
            (root / "manifests" / "one.json").write_bytes(b"manifest")
            (duka / "manifests" / "same.json").write_bytes(b"manifest")

            inventory = inventory_raw((root, duka))

            self.assertEqual(inventory.raw_bytes, 5)
            self.assertEqual(inventory.physical_file_count, 1)
            self.assertEqual(inventory.deduplicated_file_count, 1)

    def test_inventory_rejects_symlinks_without_following_them(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            raw = root / "raw"
            raw.mkdir(parents=True)
            target = raw / "target.bin"
            target.write_bytes(b"raw")
            (raw / "alias.bin").symlink_to(target)

            with self.assertRaisesRegex(StorageBudgetError, "symlink"):
                inventory_raw(root)

    def test_projection_is_aggregate_even_when_each_file_is_individually_small(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            raw = root / "raw"
            raw.mkdir(parents=True)
            (raw / "a.bin").write_bytes(b"123456")
            (raw / "b.bin").write_bytes(b"abcdef")

            projection = project_budget(root, max_raw_bytes=10)

            self.assertFalse(projection.ok)
            self.assertEqual(projection.current_raw_bytes, 12)
            self.assertEqual(projection.projected_bytes, 12)
            with self.assertRaisesRegex(StorageBudgetError, "aggregate limit"):
                guard_budget(root, max_raw_bytes=10)

    def test_reserve_uses_integer_ceil_at_the_boundary(self) -> None:
        self.assertEqual(ceil_ratio(5, 1, 5), 1)
        self.assertEqual(ceil_ratio(6, 1, 5), 2)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            (root / "raw").mkdir(parents=True)
            with mock.patch("mtf_lab.data.storage_budget.os.statvfs", return_value=_statvfs(blocks=100, available=20)):
                self.assertTrue(guard_budget(root).ok)
                with self.assertRaisesRegex(StorageBudgetError, "reserve"):
                    guard_budget(root, additional_bytes=1)

    def test_replacement_is_explicit_and_does_not_allow_an_overlarge_subtraction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            raw = root / "raw"
            raw.mkdir(parents=True)
            (raw / "payload.bin").write_bytes(b"123456")
            projection = project_budget(root, additional_bytes=4, replacing_bytes=6, max_raw_bytes=8)
            self.assertTrue(projection.ok)
            with self.assertRaises(StorageBudgetError):
                project_budget(root, replacing_bytes=7)

    def test_resume_projection_counts_only_the_remaining_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            raw = root / "raw"
            raw.mkdir(parents=True)
            (raw / ".download.part").write_bytes(b"prefix")

            projection = project_budget(root, additional_bytes=4, max_raw_bytes=10)

            self.assertEqual(projection.current_raw_bytes, 6)
            self.assertEqual(projection.projected_bytes, 10)

    def test_unknown_length_streaming_failure_preserves_the_partial(self) -> None:
        class Response:
            def __init__(self) -> None:
                self.body = BytesIO(b"abcdefgh")
                self.headers: dict[str, str] = {}

            def __enter__(self) -> Response:
                return self

            def __exit__(self, *_: object) -> None:
                return None

            def geturl(self) -> str:
                return histdata_acquisition.HISTDATA_DOWNLOAD_ENDPOINT

            def getcode(self) -> int:
                return 200

            def read(self, _size: int = -1) -> bytes:
                return self.body.read(4)

        class Opener:
            def open(self, _request: object, timeout: float) -> Response:
                del timeout
                return Response()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            raw = root / "raw"
            raw.mkdir(parents=True)
            part = raw / ".download.part"
            state = raw / ".download.state.json"
            target = raw / "download.zip"
            values = iter((28, 28, 23))

            def fake_statvfs(_path: Path) -> SimpleNamespace:
                return _statvfs(blocks=100, available=next(values))

            with (
                mock.patch("mtf_lab.data.storage_budget.os.statvfs", side_effect=fake_statvfs),
                self.assertRaises(histdata_acquisition.AcquisitionError),
            ):
                histdata_acquisition._download(
                    Opener(),
                    histdata_acquisition.HISTDATA_DOWNLOAD_ENDPOINT,
                    {},
                    part_path=part,
                    state_path=state,
                    archive_path=target,
                    timeout_seconds=1,
                    root=root,
                )

            self.assertEqual(part.read_bytes(), b"abcd")
            self.assertTrue(state.is_file())
            self.assertFalse(target.exists())

    def test_common_lock_is_shared_by_histdata_root_and_canonical_dukascopy_child(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "market-data"
            root.mkdir()
            duka = root / "dukascopy"
            self.assertEqual(shared_lock_root(root, provider="histdata"), root)
            self.assertEqual(shared_lock_root(duka, provider="dukascopy"), root)
            with acquisition_lock(root, lock_root=root) as lock_path:
                self.assertTrue(lock_path.is_file())
                with self.assertRaises(StorageBudgetError), acquisition_lock(duka, lock_root=root):
                    pass
            self.assertFalse(lock_path.exists())


if __name__ == "__main__":
    unittest.main()
