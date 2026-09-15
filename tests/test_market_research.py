from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from mtf_lab.data.historical import DatasetManifest, DatasetPartition
from mtf_lab.ops.global_trial_registry import GlobalTrialRegistry
from mtf_lab.ops.historical_assumptions import HistoricalAssumptionsModel, HistoricalCalendarTemplate
from mtf_lab.ops.market_protocol import ResearchProtocol, write_default_protocol
from mtf_lab.ops.market_research import (
    ForwardMonitorPolicy,
    HoldoutAccessError,
    evaluate_forward_monitor,
    market_research_command,
    open_confirmatory_access,
    register_campaign,
    report_campaign,
    run_campaign,
    validate_campaign,
)


def _manifest(root: Path, *, holdout: bool = False) -> DatasetManifest:
    start = datetime(2024, 1, 1, tzinfo=UTC) if holdout else datetime(2016, 3, 1, tzinfo=UTC)
    end = datetime(2025, 12, 31, 23, 59, tzinfo=UTC) if holdout else datetime(2016, 3, 31, 23, 59, tzinfo=UTC)
    partition = DatasetPartition(
        partition_id="201603",
        raw_archive="raw.zip",
        raw_sha256="a" * 64,
        raw_size=1,
        members=("EURUSD_T_201603.csv",),
        source_uri="local",
        terms_uri="local",
        acquired_at=start,
        coverage_start=start,
        coverage_end=end,
        quote_count=0,
    )
    raw_sha256 = "a" * 64
    content_material = f"201603\0{raw_sha256}\0{1}\0EURUSD_T_201603.csv"
    content_hash = hashlib.sha256(content_material.encode("utf-8")).hexdigest()
    return DatasetManifest(
        dataset_id=f"histdata:EUR/USD:201603:{raw_sha256[:32]}",
        provider="histdata",
        instrument="EUR/USD",
        partitions=(partition,),
        coverage_start=start,
        coverage_end=end,
        content_hash=content_hash,
        data_root=str(root),
    )


def _ids(protocol: ResearchProtocol) -> tuple[str, ...]:
    return tuple(protocol.candidate_ids)


class MarketResearchServiceTests(unittest.TestCase):
    def test_init_register_run_report_validate_is_local_and_review_only(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-market-service-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            protocol_path = root / "protocol.json"
            registry_path = root / "registry.jsonl"
            output_dir = root / "output"
            write_default_protocol(protocol_path)
            registry = GlobalTrialRegistry(registry_path)

            registered = register_campaign(
                protocol,
                manifest,
                registry,
                runtime_identity={"state": "FROZEN", "python": "test"},
                calendar_hash="calendar-v1",
                cost_identity={"state": "KNOWN", "catalog": "test"},
                stage="pilot-week",
            )
            self.assertEqual(registered["candidate_ids"], list(_ids(protocol)))
            self.assertTrue(registered["registered_before_results"])

            def runner(*, candidate_id: str, **_: object) -> dict[str, object]:
                return {"candidate_id": candidate_id, "status": "COMPLETED", "synthetic": True}

            receipt = run_campaign(
                protocol,
                manifest,
                registry,
                output_dir=output_dir,
                stage="pilot-week",
                runtime_identity={"state": "FROZEN", "python": "test"},
                calendar_hash="calendar-v1",
                cost_identity={"state": "KNOWN", "catalog": "test"},
                candidates=("tp_fast_v1",),
                runner=runner,
            )
            self.assertEqual(receipt["stage"], "pilot-week")
            self.assertFalse(receipt["auto_promote"])
            self.assertTrue(Path(receipt["artifact_path"]).is_file())
            self.assertTrue(
                any(
                    record["event"] == "ATTEMPT_STATUS" and record["status"] == "COMPLETED"
                    for record in registry.records()
                )
            )

            report = report_campaign(receipt, protocol=protocol)
            self.assertEqual(report["validation"]["state"], "VALID")
            self.assertFalse(report["evidence"]["auto_promote"])
            self.assertEqual(validate_campaign(receipt, protocol=protocol, registry=registry)["state"], "VALID")

    def test_holdout_remains_closed_for_the_current_histdata_contract(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-market-holdout-") as directory:
            root = Path(directory)
            manifest = _manifest(root, holdout=True)
            registry = GlobalTrialRegistry(root / "registry.jsonl")
            runtime = {
                "state": "FROZEN",
                "runtime_id": "runtime-test",
                "python": "3.14",
                "version": "test",
                "code_hash": "a" * 64,
                "identity_seal": "b" * 64,
                "sha256_verified": True,
            }
            costs = {
                "state": "KNOWN",
                "costs_complete": True,
                "known_source": "frozen-catalog",
                "currency": "USD",
                "cost_hash": "c" * 64,
            }
            calendar = {
                "coverage_start": "2024-01-01T00:00:00Z",
                "coverage_end": "2025-12-31T23:59:00Z",
                "exceptions_known": True,
                "calendar_hash": "d" * 64,
            }
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
                record["attempt_id"]
                for record in registry.records()
                if record["event"] == "ATTEMPT_REGISTERED" and record["candidate_id"] == "tp_fast_v1"
            )
            with self.assertRaisesRegex(HoldoutAccessError, "sólo soporta HistData marzo-2016"):
                open_confirmatory_access(
                    protocol,
                    manifest,
                    registry,
                    attempt_id=str(first_attempt),
                    calendar_hash=calendar,
                    runtime_identity=runtime,
                    cost_identity=costs,
                )

    def test_explicit_assumption_and_calendar_models_are_bound_to_the_attempt(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-market-models-") as directory:
            root = Path(directory)
            manifest = _manifest(root)
            registry = GlobalTrialRegistry(root / "registry.jsonl")
            assumptions_id = "eurusd_virtual_10k_unit_value_v1"
            calendar_id = "pepperstone_public_calendar_template_v1"
            contract = {"known": False, "unit_value": "1", "quantity_step": "1"}
            calendar_state = {"known": True, "exceptions_known": False, "calendar_basis": "MODELED"}
            registered = register_campaign(
                protocol,
                manifest,
                registry,
                runtime_identity={"state": "UNDECLARED"},
                calendar_hash="UNKNOWN",
                cost_identity={"state": "UNDECLARED"},
                assumptions_model_id=assumptions_id,
                calendar_model_id=calendar_id,
                contract_spec=contract,
                calendar_state=calendar_state,
                candidates=("tp_fast_v1",),
                stage="pilot-month",
            )
            record = registered["records"][0]
            self.assertEqual(record["scope"]["assumptions_model_id"], assumptions_id)
            self.assertEqual(record["scope"]["calendar_model_id"], calendar_id)
            self.assertEqual(record["parameters"]["assumption_binding"]["mode"], "MODELED_NOT_OBSERVED")
            self.assertEqual(record["parameters"]["assumption_binding"]["contract_spec"], contract)

            def runner(
                *, candidate_id: str, assumptions_model: object, calendar_model: object, **_: object
            ) -> dict[str, object]:
                assert isinstance(assumptions_model, HistoricalAssumptionsModel)
                assert isinstance(calendar_model, HistoricalCalendarTemplate)
                self.assertEqual(assumptions_model.model_id, assumptions_id)
                self.assertEqual(calendar_model.model_id, calendar_id)
                return {"candidate_id": candidate_id, "scenario_applied": True, "status": "COMPLETED"}

            receipt = run_campaign(
                protocol,
                manifest,
                registry,
                output_dir=root / "output",
                stage="pilot-month",
                runtime_identity={"state": "UNDECLARED"},
                calendar_hash="UNKNOWN",
                cost_identity={"state": "UNDECLARED"},
                assumptions_model_id=assumptions_id,
                calendar_model_id=calendar_id,
                contract_spec=contract,
                calendar_state=calendar_state,
                candidates=("tp_fast_v1",),
                runner=runner,
            )
            self.assertEqual(receipt["results"][0]["assumption_binding"]["calendar_model_id"], calendar_id)

    def test_modeled_calendar_cannot_be_used_to_escape_holdout_gate(self) -> None:
        protocol = ResearchProtocol.default()
        with tempfile.TemporaryDirectory(prefix="mtf-market-model-holdout-") as directory:
            root = Path(directory)
            manifest = _manifest(root, holdout=True)
            registry = GlobalTrialRegistry(root / "registry.jsonl")
            runtime = {
                "state": "FROZEN",
                "runtime_id": "runtime-test",
                "python": "3.14",
                "version": "test",
                "code_hash": "a" * 64,
                "identity_seal": "b" * 64,
                "sha256_verified": True,
            }
            costs = {
                "state": "KNOWN",
                "costs_complete": True,
                "known_source": "frozen-catalog",
                "currency": "USD",
                "cost_hash": "c" * 64,
            }
            calendar = {
                "coverage_start": "2024-01-01T00:00:00Z",
                "coverage_end": "2025-12-31T23:59:00Z",
                "exceptions_known": True,
                "calendar_hash": "d" * 64,
            }
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
                record["attempt_id"]
                for record in registry.records()
                if record["event"] == "ATTEMPT_REGISTERED" and record["candidate_id"] == "tp_fast_v1"
            )
            with self.assertRaisesRegex(HoldoutAccessError, "sólo soporta HistData marzo-2016"):
                open_confirmatory_access(
                    protocol,
                    manifest,
                    registry,
                    attempt_id=str(first_attempt),
                    runtime_identity=runtime,
                    calendar_hash=calendar,
                    cost_identity=costs,
                )

    def test_command_and_forward_monitor_preserve_gates(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-market-command-") as directory:
            root = Path(directory)
            code, payload = market_research_command(
                SimpleNamespace(
                    action="init",
                    protocol=str(root / "protocol.json"),
                    registry=str(root / "registry.jsonl"),
                )
            )
            self.assertEqual(code, 0)
            self.assertFalse(payload["auto_promote"])
        monitor = evaluate_forward_monitor(
            [
                {
                    "net_pnl": "-1",
                    "initial_risk": "1",
                    "r_multiple": "0.5",
                    "state": "CLOSED",
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            ],
            policy=ForwardMonitorPolicy(interim_episodes=2, final_episodes=3, threshold_r=Decimal("0.5")),
        )
        self.assertEqual(monitor["state"], "OBSERVE_INSUFFICIENT_SAMPLE")
        self.assertTrue(monitor["cusum"]["breach"])
        self.assertFalse(monitor["auto_tune"])


if __name__ == "__main__":
    unittest.main()
