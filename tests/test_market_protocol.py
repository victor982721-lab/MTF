from __future__ import annotations

import hashlib
import json
import stat
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from mtf_lab.data.historical import DatasetPartition
from mtf_lab.ops.market_protocol import (
    CANDIDATE_IDS,
    DatasetManifest,
    MarketProtocolError,
    ResearchProtocol,
    read_protocol,
    write_default_protocol,
)


class MarketProtocolTests(unittest.TestCase):
    def test_default_is_exactly_the_six_candidate_protocol(self) -> None:
        protocol = ResearchProtocol.default()
        self.assertEqual(protocol.candidate_ids, CANDIDATE_IDS)
        self.assertEqual([item.candidate_id for item in protocol.candidates], list(CANDIDATE_IDS))
        self.assertEqual(protocol.candidate("tp_fast_v1").timeframes, ("M1", "M5", "M15"))
        self.assertEqual(protocol.candidate("dc_m5_v1").timeframes, ("M5",))
        self.assertEqual(protocol.walkforward_years, (2020, 2021, 2022, 2023))
        self.assertEqual(protocol.holdout_start_year, 2024)
        self.assertEqual(protocol.holdout_end_year, 2025)
        self.assertTrue(protocol.holdout_locked)
        self.assertFalse(protocol.holdout_open)
        self.assertEqual(protocol.bootstrap_iterations, 100_000)
        self.assertEqual(protocol.bootstrap_seed, 20_260_913)
        self.assertEqual(protocol.block_days, 14)
        self.assertEqual(protocol.bootstrap_block_days, 14)
        self.assertEqual(protocol.bootstrap_sensitivity, (7, 28))
        self.assertEqual(protocol.to_dict()["bootstrap"]["sensitivity_days"], [7, 28])
        self.assertEqual(protocol.to_dict()["scenarios"]["models"]["base"]["decision_latency_seconds"], "5")
        self.assertEqual(protocol.to_dict()["evidence_gates"]["drop_best_count"], 5)
        self.assertEqual(protocol.to_dict()["ranking"][-1], "candidate_id")
        self.assertEqual(str(protocol.risk_policy.planned_risk_fraction), "0.0025")

    def test_protocol_artifact_is_hashed_and_exclusive(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-market-protocol-") as directory:
            path = Path(directory) / "nested" / "protocol.json"
            written = write_default_protocol(path)
            self.assertEqual(written, path)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(read_protocol(path).protocol_hash, ResearchProtocol.default().protocol_hash)
            before = path.read_bytes()
            with self.assertRaises(MarketProtocolError):
                write_default_protocol(path)
            self.assertEqual(path.read_bytes(), before)

    def test_dataset_manifest_preserves_owner_fields_and_rejects_naive_coverage(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-market-dataset-") as directory:
            root = Path(directory)
            partition = DatasetPartition(
                partition_id="201603",
                raw_archive="raw.zip",
                raw_sha256="a" * 64,
                raw_size=123,
                members=("DAT_ASCII_EURUSD_T_201603.csv",),
                source_uri="local",
                terms_uri="local",
                acquired_at=datetime(2024, 1, 1, tzinfo=UTC),
                coverage_start=datetime(2024, 1, 1, tzinfo=UTC),
                coverage_end=datetime(2024, 1, 31, 23, 59, tzinfo=UTC),
                quote_count=10,
            )
            material = "201603\0" + "a" * 64 + "\0" + str(123) + "\0DAT_ASCII_EURUSD_T_201603.csv"
            content_hash = hashlib.sha256(material.encode("utf-8")).hexdigest()
            manifest = DatasetManifest(
                dataset_id="histdata:EUR/USD:201603:" + "a" * 32,
                provider="histdata",
                instrument="EUR/USD",
                partitions=(partition,),
                coverage_start=datetime(2024, 1, 1, tzinfo=UTC),
                coverage_end=datetime(2024, 1, 31, 23, 59, tzinfo=UTC),
                content_hash=content_hash,
                data_root=str(root),
            )
            self.assertEqual(manifest.to_dict()["schema_version"], 1)
            self.assertEqual(manifest.content_hash, content_hash)

    def test_protocol_tampering_hash_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-market-protocol-") as directory:
            path = write_default_protocol(Path(directory) / "protocol.json")
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["holdout"]["open"] = True
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(MarketProtocolError):
                read_protocol(path)


if __name__ == "__main__":
    unittest.main()
