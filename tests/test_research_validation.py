"""Semantic validation independent of research.py's file/DB reader."""

from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path
from typing import Any, ClassVar

from mtf_lab.core.canonical import fingerprint
from mtf_lab.ops.research import run_research
from mtf_lab.ops.research_validation import validate_manifest_contract


def _rehashed(value: dict[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(value)
    payload.pop("integrity_hash", None)
    payload["integrity_hash"] = fingerprint(payload)
    return payload


class ResearchValidationTests(unittest.TestCase):
    _temporary: ClassVar[Any]
    manifest: ClassVar[dict[str, Any]]

    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory(prefix="mtf-research-validation-")
        path = Path(cls._temporary.name) / "manifest.json"
        cls.manifest = run_research(
            manifest_path=path,
            fixture=True,
            variants=("trend_pullback_v1",),
            horizons_seconds=(60,),
            bootstrap_iterations=2,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_current_fixture_manifest_is_semantically_valid(self) -> None:
        self.assertEqual(validate_manifest_contract(self.manifest), [])

    def test_execution_scenarios_remain_quantity_reconcilable(self) -> None:
        for scenario, fraction in (("full_fill", None), ("ioc_partial", "0.25"), ("rejected", None)):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory(prefix="mtf-scenario-") as directory:
                manifest = run_research(
                    manifest_path=Path(directory) / "manifest.json",
                    fixture=True,
                    variants=("trend_pullback_v1",),
                    horizons_seconds=(60,),
                    bootstrap_iterations=1,
                    execution_model=scenario,
                    fill_fraction=fraction,
                )
                self.assertEqual(validate_manifest_contract(manifest), [])

    def test_drop_result_is_rejected_even_after_recomputing_integrity_hash(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        tampered["results"].pop()
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("trial_result_count_mismatch", errors)
        self.assertIn("trial_result_ids_not_one_to_one", errors)

    def test_economic_gross_tamper_is_rejected_without_leakage_list(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        row = tampered["results"][0]["ledger"][0]
        row["gross_pnl_quote"] = "999"
        tampered["results"][0]["leakage_violations"] = []
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("economic_gross_not_reconstructible", errors)

    def test_fee_tamper_is_rejected_even_when_gross_is_untouched(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        row = tampered["results"][0]["ledger"][0]
        row["commission_quote"] = "7"
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("economic_costs_not_reconstructible", errors)

    def test_unknown_costs_cannot_be_relabelled_as_zero_default(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        tampered["costs_contract"]["missing_is_zero"] = True
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("economic_uncertainty_default_zero", errors)

    def test_time_tamper_is_rejected_from_ledger_ordering(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        row = tampered["results"][0]["ledger"][0]
        row["entry_available_at"] = "2025-12-31T23:00:00Z"
        tampered["results"][0]["leakage_violations"] = []
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("ledger_causality_violation", errors)

    def test_fixture_broker_evidence_true_is_rejected(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        result = tampered["results"][0]
        result["broker_evidence"] = True
        result["execution_model"]["external_broker_fills"] = "OBSERVED"
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("fixture_broker_evidence_present", errors)

    def test_future_window_is_rejected(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        tampered["windows"][-1]["test_end"] = "2026-01-02T00:00:00Z"
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("holdout_range_invalid", errors)
        self.assertIn("window_future_or_overlap", errors)

    def test_explicit_unknown_economics_are_allowed_but_not_zero_defaults(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        row = tampered["results"][0]["ledger"][0]
        row.update(
            {
                "state": "UNKNOWN",
                "costs_known": False,
                "costs_unknown_reason": "COMMISSION_UNKNOWN",
                "costs_account": None,
                "net_pnl": None,
                "economic_result": {
                    "state": "INDETERMINATE",
                    "economic_state": "INDETERMINATE",
                    "gross_pnl_quote": row["gross_pnl_quote"],
                    "costs_quote": None,
                    "gross_pnl_account": row["gross_pnl_account"],
                    "costs_account": None,
                    "net_pnl": None,
                    "reason": "COMMISSION_UNKNOWN",
                },
            }
        )
        tampered["results"][0]["costs"].update(
            {"unknown_count": 1, "unknown_reasons": ["COMMISSION_UNKNOWN"], "state": "PARTIAL_UNKNOWN"}
        )
        self.assertEqual(validate_manifest_contract(_rehashed(tampered)), [])

    def test_non_aware_ledger_timestamp_is_rejected(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        tampered["results"][0]["ledger"][0]["decision_at"] = "2026-01-01T02:03:01"
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("ledger_timestamp_invalid", errors)

    def test_holdout_tuning_cannot_be_final_approval(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        tampered["policy"]["holdout_used_for_tuning"] = True
        tampered["summary"]["final_holdout_approved"] = True
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("holdout_used_for_tuning_cannot_be_final_approval", errors)

    def test_hypothesis_registration_and_decision_gate_are_required(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        tampered.pop("hypotheses")
        tampered["results"][0]["decision"]["promote"] = True
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("hypotheses_preregistration_missing", errors)
        self.assertIn("result_decision_promotion_not_prohibited", errors)

    def test_fixture_decision_cannot_be_marked_promoted(self) -> None:
        tampered = copy.deepcopy(self.manifest)
        tampered["results"][0]["decision"]["status"] = "PROMOTED"
        errors = validate_manifest_contract(_rehashed(tampered))
        self.assertIn("result_decision_status_invalid", errors)


if __name__ == "__main__":
    unittest.main()
