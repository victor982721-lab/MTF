from __future__ import annotations

import hashlib
import tempfile
import unittest
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from mtf_lab.data.historical import DatasetManifest, DatasetPartition
from mtf_lab.ops.global_trial_registry import GlobalTrialRegistry, RegistryConflict
from mtf_lab.ops.market_protocol import ResearchProtocol
from mtf_lab.ops.market_research import (
    HOLDOUT_WINDOW_END,
    HOLDOUT_WINDOW_START,
    HoldoutAccessError,
    HoldoutExecutionPermission,
    open_confirmatory_access,
    register_campaign,
    run_campaign,
)


def _manifest(root: Path) -> DatasetManifest:
    partition = DatasetPartition(
        partition_id="201603",
        raw_archive="raw.zip",
        raw_sha256="a" * 64,
        raw_size=1,
        members=("EURUSD_T_201603.csv",),
        source_uri="local",
        terms_uri="local",
        acquired_at=datetime(2024, 1, 1, tzinfo=UTC),
        coverage_start=datetime(2024, 1, 1, tzinfo=UTC),
        coverage_end=datetime(2025, 12, 31, 23, 59, tzinfo=UTC),
        quote_count=0,
    )
    material = f"201603\0{'a' * 64}\0{1}\0EURUSD_T_201603.csv"
    return DatasetManifest(
        dataset_id="histdata:EUR/USD:201603:" + "a" * 32,
        provider="histdata",
        instrument="EUR/USD",
        partitions=(partition,),
        coverage_start=datetime(2024, 1, 1, tzinfo=UTC),
        coverage_end=datetime(2025, 12, 31, 23, 59, tzinfo=UTC),
        content_hash=hashlib.sha256(material.encode("utf-8")).hexdigest(),
        data_root=str(root),
    )


def _identities() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    return (
        {
            "state": "FROZEN",
            "runtime_id": "runtime-test",
            "python": "3.14",
            "version": "test",
            "code_hash": "a" * 64,
            "identity_seal": "b" * 64,
            "sha256_verified": True,
        },
        {
            "state": "KNOWN",
            "costs_complete": True,
            "known_source": "frozen-catalog",
            "currency": "USD",
            "cost_hash": "c" * 64,
        },
        {
            "coverage_start": HOLDOUT_WINDOW_START,
            "coverage_end": "2025-12-31T23:59:00Z",
            "exceptions_known": True,
            "calendar_hash": "d" * 64,
        },
    )


class MarketHoldoutGateHardeningTests(unittest.TestCase):
    def test_generic_runner_is_rejected_before_it_can_consume_or_read_holdout(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-holdout-runner-gate-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            registry = GlobalTrialRegistry(root / "registry.jsonl")
            runtime, costs, calendar = _identities()
            called = False

            def generic_runner(**_: object) -> dict[str, object]:
                nonlocal called
                called = True
                return {"scenario_applied": True}

            with self.assertRaisesRegex(HoldoutAccessError, "holdout_permission/holdout_access"):
                run_campaign(
                    protocol,
                    manifest,
                    registry,
                    output_dir=root / "output",
                    stage="holdout",
                    runtime_identity=runtime,
                    calendar_hash=calendar,
                    cost_identity=costs,
                    start=HOLDOUT_WINDOW_START,
                    end=HOLDOUT_WINDOW_END,
                    runner=generic_runner,
                )
            self.assertFalse(called)
            self.assertEqual(registry.records(), ())

    def test_access_attempt_must_be_one_of_the_selected_attempts(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-holdout-attempt-gate-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            registry = GlobalTrialRegistry(root / "registry.jsonl")
            runtime, costs, calendar = _identities()
            register_campaign(
                protocol,
                manifest,
                registry,
                runtime_identity=runtime,
                calendar_hash=calendar,
                cost_identity=costs,
                stage="holdout",
            )
            first_attempt = next(
                str(record["attempt_id"])
                for record in registry.records()
                if record.get("event") == "ATTEMPT_REGISTERED" and record.get("candidate_id") == "tp_fast_v1"
            )
            with patch("mtf_lab.ops.market_research._check_holdout_dataset"):
                access = open_confirmatory_access(
                    protocol,
                    manifest,
                    registry,
                    attempt_id=first_attempt,
                    calendar_hash=calendar,
                    runtime_identity=runtime,
                    cost_identity=costs,
                )
                with self.assertRaisesRegex(HoldoutAccessError, "no pertenece a los intentos seleccionados"):
                    run_campaign(
                        protocol,
                        manifest,
                        registry,
                        output_dir=root / "output",
                        stage="holdout",
                        runtime_identity=runtime,
                        calendar_hash=calendar,
                        cost_identity=costs,
                        candidates=("tp_intraday_slow_v1",),
                        start=HOLDOUT_WINDOW_START,
                        end=HOLDOUT_WINDOW_END,
                        runner=lambda *, holdout_permission: {"scenario_applied": True},
                        confirmatory_access=access,
                    )
            self.assertFalse(
                any(record.get("event") == "CONFIRMATORY_ACCESS_CONSUMED" for record in registry.records())
            )

    def test_explicit_runner_receives_only_non_secret_scoped_permission(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-holdout-permission-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            registry = GlobalTrialRegistry(root / "registry.jsonl")
            runtime, costs, calendar = _identities()
            register_campaign(
                protocol,
                manifest,
                registry,
                runtime_identity=runtime,
                calendar_hash=calendar,
                cost_identity=costs,
                stage="holdout",
                candidates=("tp_fast_v1",),
            )
            attempt_id = next(
                str(record["attempt_id"])
                for record in registry.records()
                if record.get("event") == "ATTEMPT_REGISTERED"
            )
            with patch("mtf_lab.ops.market_research._check_holdout_dataset"):
                access = open_confirmatory_access(
                    protocol,
                    manifest,
                    registry,
                    attempt_id=attempt_id,
                    calendar_hash=calendar,
                    runtime_identity=runtime,
                    cost_identity=costs,
                )
                received: list[HoldoutExecutionPermission] = []

                def permitted_runner(
                    *, holdout_permission: HoldoutExecutionPermission, candidate_id: str, **_: object
                ) -> dict[str, object]:
                    received.append(holdout_permission)
                    self.assertEqual(candidate_id, "tp_fast_v1")
                    return {"candidate_id": candidate_id, "scenario_applied": True, "status": "COMPLETED"}

                receipt = run_campaign(
                    protocol,
                    manifest,
                    registry,
                    output_dir=root / "output",
                    stage="holdout",
                    runtime_identity=runtime,
                    calendar_hash=calendar,
                    cost_identity=costs,
                    candidates=("tp_fast_v1",),
                    start=HOLDOUT_WINDOW_START,
                    end=HOLDOUT_WINDOW_END,
                    runner=permitted_runner,
                    confirmatory_access=access,
                )
            self.assertEqual(receipt["holdout"], "CONSUMED_SINGLE_USE")
            self.assertEqual(len(received), 1)
            permission = received[0]
            public = permission.to_dict()
            self.assertEqual(public["window_start"], HOLDOUT_WINDOW_START)
            self.assertEqual(public["window_end"], HOLDOUT_WINDOW_END)
            self.assertEqual(public["candidate_ids"], ["tp_fast_v1"])
            self.assertEqual(public["attempt_ids"], [attempt_id])
            self.assertNotIn("capability", public)
            self.assertNotIn("token", public)

    def test_current_histdata_contract_cannot_be_relabelled_as_2024_holdout(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-holdout-dataset-gate-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            registry = GlobalTrialRegistry(root / "registry.jsonl")
            runtime, costs, calendar = _identities()
            with (
                patch("mtf_lab.ops.market_research.validate_dataset") as validator,
                self.assertRaisesRegex(HoldoutAccessError, "sólo soporta HistData marzo-2016"),
            ):
                open_confirmatory_access(
                    protocol,
                    manifest,
                    registry,
                    attempt_id="unregistered",
                    calendar_hash=calendar,
                    runtime_identity=runtime,
                    cost_identity=costs,
                )
            validator.assert_not_called()
            self.assertEqual(registry.records(), ())

    def test_confirmatory_issue_conflict_is_fail_closed(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-holdout-issue-cas-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            registry = GlobalTrialRegistry(root / "registry.jsonl")
            runtime, costs, calendar = _identities()
            registration = register_campaign(
                protocol,
                manifest,
                registry,
                runtime_identity=runtime,
                calendar_hash=calendar,
                cost_identity=costs,
                stage="holdout",
                candidates=("tp_fast_v1",),
            )
            attempt_id = str(registration["records"][0]["attempt_id"])

            def conflict(record: Mapping[str, Any], *, expected_revision: int | None = None) -> dict[str, Any]:
                del record, expected_revision
                raise RegistryConflict("simulated concurrent append")

            with (
                patch("mtf_lab.ops.market_research._check_holdout_dataset"),
                patch.object(registry, "append", side_effect=conflict),
                self.assertRaisesRegex(HoldoutAccessError, "disputado"),
            ):
                open_confirmatory_access(
                    protocol,
                    manifest,
                    registry,
                    attempt_id=attempt_id,
                    calendar_hash=calendar,
                    runtime_identity=runtime,
                    cost_identity=costs,
                )
            self.assertFalse(any(item.get("event") == "CONFIRMATORY_ACCESS_ISSUED" for item in registry.records()))


if __name__ == "__main__":
    unittest.main()
