"""Focused offline tests for the independent walk-forward contract."""

from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from mtf_lab.data.historical import DatasetManifest, iter_quotes, manifest_from_histdata_archive
from mtf_lab.data.walk_forward import (
    WALK_FORWARD_END,
    WALK_FORWARD_MONTHS,
    WALK_FORWARD_START,
    WalkForwardError,
    WalkForwardIdentityError,
    WalkForwardInput,
    WalkForwardManifest,
    WalkForwardStreamGuard,
    classify_walk_forward_phase,
    iter_walk_forward_quotes,
    make_walk_forward_receipt,
    validate_walk_forward_manifest,
    validate_walk_forward_receipt,
    walk_forward_manifest_from_histdata_archives,
)

_ACQUIRED_AT = datetime(2026, 9, 14, tzinfo=UTC)
_SOURCE_URI = "https://www.histdata.com/download-free-forex-historical-data/"
_TERMS_URI = "https://www.histdata.com/f-a-q/data-files-detailed-specification/"


class WalkForwardFixtureMixin:
    @staticmethod
    def _archive(root: Path, month: str, *, hour: int = 0) -> Path:
        data_root = root / "market-data"
        raw_root = data_root / "raw"
        raw_root.mkdir(parents=True, exist_ok=True)
        path = raw_root / f"HISTDATA_COM_ASCII_EURUSD_T_{month}.zip"
        row = f"{month}01 {hour:02d}0000000,1.10000,1.10020,0\n".encode("ascii")
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(f"DAT_ASCII_EURUSD_T_{month}.csv", row)
        return path

    def make_wf(self, root: Path) -> WalkForwardManifest:
        archives = [self._archive(root, month) for month in WALK_FORWARD_MONTHS]
        return walk_forward_manifest_from_histdata_archives(
            archives,
            data_root=root / "market-data",
            acquired_at=_ACQUIRED_AT,
            source_uri=_SOURCE_URI,
            terms_uri=_TERMS_URI,
        )

    def make_development(self, root: Path) -> DatasetManifest:
        archive = self._archive(root, "201912", hour=18)
        return manifest_from_histdata_archive(
            archive,
            data_root=root / "market-data",
            acquired_at=_ACQUIRED_AT,
            source_uri=_SOURCE_URI,
            terms_uri=_TERMS_URI,
            month="201912",
        )

    @staticmethod
    def make_input(development: DatasetManifest, walk_forward: WalkForwardManifest) -> WalkForwardInput:
        return WalkForwardInput.from_manifests(
            development,
            walk_forward,
            protocol_hash="a" * 64,
            risk_hash="b" * 64,
            calendar_hash="c" * 64,
            costs_hash="d" * 64,
            code_hash="e" * 64,
            runtime_hash="f" * 64,
        )


class WalkForwardContractTests(WalkForwardFixtureMixin, unittest.TestCase):
    def test_manifest_requires_all_months_and_roundtrips_with_independent_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-wf-contract-") as directory:
            root = Path(directory)
            manifest = self.make_wf(root)
            self.assertIsInstance(manifest, WalkForwardManifest)
            self.assertEqual(len(manifest.partitions), 48)
            self.assertEqual(manifest.partitions[0].month, "202001")
            self.assertEqual(manifest.partitions[-1].month, "202312")
            self.assertTrue(validate_walk_forward_manifest(manifest).ok)
            self.assertEqual(len(list(iter_walk_forward_quotes(manifest))), 48)

            raw = manifest.to_dict()
            raw["manifest_id"] = raw.pop("walk_forward_id")
            self.assertEqual(WalkForwardManifest.from_mapping(raw).to_dict(), manifest.to_dict())

            with self.assertRaisesRegex(WalkForwardError, "exactly 48"):
                walk_forward_manifest_from_histdata_archives(
                    [self._archive(root, month) for month in WALK_FORWARD_MONTHS[:-1]],
                    data_root=root / "market-data",
                    acquired_at=_ACQUIRED_AT,
                    source_uri=_SOURCE_URI,
                    terms_uri=_TERMS_URI,
                )

    def test_manifest_rejects_raw_mutation_without_sorting_or_relabeling(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-wf-contract-") as directory:
            root = Path(directory)
            manifest = self.make_wf(root)
            archive = root / "market-data" / manifest.partitions[0].raw_archive
            archive.write_bytes(archive.read_bytes() + b"x")
            result = validate_walk_forward_manifest(manifest)
            self.assertFalse(result.ok)
            self.assertTrue(any("size mismatch" in issue for issue in result.issues))

    def test_boundary_guard_accepts_development_only_then_selected_wf_and_rejects_holdout(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-wf-contract-") as directory:
            root = Path(directory)
            development = self.make_development(root)
            walk_forward = self.make_wf(root)
            input_contract = self.make_input(development, walk_forward)
            guard = WalkForwardStreamGuard(development, walk_forward, input_contract=input_contract)

            development_quote = next(iter(iter_quotes(development)))
            wf_quote = next(iter(iter_walk_forward_quotes(walk_forward)))
            self.assertEqual(guard.validate(development_quote, phase="WARMUP_ONLY"), "WARMUP_ONLY")
            self.assertEqual(guard.validate(wf_quote, phase="WF_EVALUATION"), "WF_EVALUATION")
            self.assertEqual(classify_walk_forward_phase(WALK_FORWARD_START), "WF_EVALUATION")
            self.assertEqual(classify_walk_forward_phase(WALK_FORWARD_END), "HOLDOUT_REJECTED")

            # A quote from the next annual partition is not part of WF_2020.
            next_year_quote = list(iter_walk_forward_quotes(walk_forward))[12]
            with self.assertRaisesRegex(WalkForwardError, "outside the selected"):
                WalkForwardStreamGuard(development, walk_forward, input_contract=input_contract).validate(
                    next_year_quote
                )

            snapshot = guard.snapshot()
            resumed = WalkForwardStreamGuard.from_snapshot(
                development,
                walk_forward,
                snapshot,
                input_contract=input_contract,
            )
            self.assertEqual(resumed.cursor, guard.cursor)
            tampered = json.loads(json.dumps(snapshot))
            tampered["holdout"] = "OPEN"
            with self.assertRaises(WalkForwardError):
                WalkForwardStreamGuard.from_snapshot(development, walk_forward, tampered, input_contract=input_contract)

    def test_input_and_receipt_are_hash_bound_and_review_only(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-wf-contract-") as directory:
            root = Path(directory)
            development = self.make_development(root)
            walk_forward = self.make_wf(root)
            input_contract = self.make_input(development, walk_forward)
            restored = WalkForwardInput.from_mapping(input_contract.to_dict())
            self.assertEqual(restored.identity_hash, input_contract.identity_hash)
            receipt = make_walk_forward_receipt(input_contract, status="INSUFFICIENT", attempt_id="attempt-1")
            self.assertEqual(validate_walk_forward_receipt(receipt.to_dict(), input_contract=input_contract), receipt)

            tampered_input = input_contract.to_dict()
            tampered_input["holdout"] = "OPEN"
            with self.assertRaises(WalkForwardError):
                WalkForwardInput.from_mapping(tampered_input)

            tampered_receipt = receipt.to_dict()
            tampered_receipt["trading_enabled"] = True
            with self.assertRaises(WalkForwardError):
                validate_walk_forward_receipt(tampered_receipt)

    def test_dataset_manifest_cannot_be_used_as_walk_forward_manifest(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-wf-contract-") as directory:
            root = Path(directory)
            development = self.make_development(root)
            with self.assertRaises(WalkForwardIdentityError):
                WalkForwardStreamGuard(development, cast(Any, development))


if __name__ == "__main__":
    unittest.main()
