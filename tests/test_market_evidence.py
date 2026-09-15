from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from mtf_lab.ops.market_evidence import (
    BootstrapPolicy,
    EvidenceCriteria,
    bootstrap_mean_ci,
    derive_net_r,
    evaluate_candidate,
    evaluate_candidates,
    validate_evidence,
)
from mtf_lab.ops.market_protocol import CANDIDATE_IDS, ResearchProtocol


def _policy() -> BootstrapPolicy:
    return BootstrapPolicy(iterations=32, seed=20260913, block_days=14, sensitivity=(7, 28))


def _criteria() -> EvidenceCriteria:
    return EvidenceCriteria(
        min_holdout_episodes=5,
        min_holdout_sessions=5,
        min_holdout_blocks=2,
        min_walkforward_episodes=2,
        drop_best_count=1,
        bootstrap=_policy(),
    )


def _result(
    *, fixture: bool = False, r_value: str = "0.10", net_value: str = "1.0", drawdown: str = "0.03"
) -> dict[str, object]:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    rows = [
        {
            "episode_id": f"episode-{index}",
            "session_id": f"session-{index}",
            "block_id": f"block-{index // 2}",
            "state": "CLOSED",
            "r_multiple": r_value,
            "initial_risk": "1",
            "net_pnl": net_value,
            "close_available_at": (start + timedelta(days=index * 14)).isoformat(),
        }
        for index in range(6)
    ]
    return {
        "fixture": fixture,
        "costs": {"state": "KNOWN", "unknown_count": 0},
        "marks": [{"state": "KNOWN", "at": start.isoformat()}],
        "r_complete": True,
        "holdout": {
            "episodes": rows,
            "coverage_start": start.isoformat(),
            "coverage_end": (start + timedelta(days=119, hours=23, minutes=59)).isoformat(),
            "coverage_complete": True,
            "covered_session_count": 6,
            "gaps_known": True,
            "episode_count": 6,
            "session_count": 6,
            "block_count": 3,
            "mean_r": "0.08",
            "lcb": "0.01",
        },
        "stress": {
            "episodes": rows,
            "costs": {"state": "KNOWN", "unknown_count": 0},
            "marks": [{"state": "KNOWN", "at": start.isoformat()}],
            "coverage_start": start.isoformat(),
            "coverage_end": (start + timedelta(days=119, hours=23, minutes=59)).isoformat(),
            "coverage_complete": True,
            "covered_session_count": 6,
            "gaps_known": True,
            "mean_r": "0.10",
            "lcb": "0.02",
            "max_drawdown_fraction": drawdown,
        },
        "walkforward": [
            {
                "episodes": [
                    {
                        "r_multiple": r_value,
                        "net_pnl": r_value,
                        "initial_risk": "1",
                        "state": "CLOSED",
                        "close_available_at": (start + timedelta(days=window * 14 + offset)).isoformat(),
                    }
                    for offset in range(2)
                ]
            }
            for window in range(4)
        ],
    }


class MarketEvidenceTests(unittest.TestCase):
    def test_economic_r_comes_from_settled_net_pnl_not_excursion_r(self) -> None:
        loss: dict[str, Any] = {
            "state": "CLOSED",
            "net_pnl": "-1",
            "initial_risk": "1",
            "r_multiple": "0.5",  # Legacy/ambiguous field; economics is reconstructed below.
            "close_available_at": "2024-01-01T00:00:00Z",
        }
        derived = derive_net_r(loss)
        self.assertEqual(derived["status"], "DERIVED")
        self.assertEqual(derived["net_r"], "-1")
        self.assertEqual(derived["net_pnl"], "-1")
        self.assertEqual(derived["initial_risk"], "1")
        self.assertTrue(derived["r_multiple_not_used_as_authority"])

        opaque = dict(loss)
        opaque.pop("initial_risk")
        self.assertEqual(derive_net_r(opaque)["status"], "NOT_ASSESSED")

        mismatch = dict(loss)
        mismatch["net_r"] = "0.5"
        self.assertEqual(derive_net_r(mismatch)["status"], "INVALID")

        canonical_mismatch = dict(loss)
        canonical_mismatch["economics_version"] = "cfd-economics-v2"
        self.assertEqual(derive_net_r(canonical_mismatch)["status"], "INVALID")

        risk_mismatch = dict(loss)
        risk_mismatch["risk_exit_plan"] = {"risk_amount": "2"}
        self.assertEqual(derive_net_r(risk_mismatch)["status"], "INVALID")

    def test_unknown_fee_blocks_even_a_positive_reported_net(self) -> None:
        source = _result()
        source["costs"] = {"state": "UNKNOWN", "unknown_count": 1}
        result = evaluate_candidate("tp_fast_v1", source, protocol=ResearchProtocol.default(), criteria=_criteria())
        self.assertEqual(result["decision"], "INSUFFICIENT")
        self.assertIn("costs_incomplete_or_unknown", result["reasons"])

        holdout = source["holdout"]
        assert isinstance(holdout, dict)
        row = holdout["episodes"][0]
        assert isinstance(row, dict)
        row["commission_known"] = False
        source["costs"] = {"state": "KNOWN", "unknown_count": 0}
        result = evaluate_candidate("tp_fast_v1", source, protocol=ResearchProtocol.default(), criteria=_criteria())
        self.assertEqual(result["decision"], "INSUFFICIENT")

        row["commission_known"] = True
        row["costs"] = {"state": "KNOWN", "unknown_count": 1}
        result = evaluate_candidate("tp_fast_v1", source, protocol=ResearchProtocol.default(), criteria=_criteria())
        self.assertEqual(result["decision"], "INSUFFICIENT")

    def test_loss_is_not_rescued_by_a_positive_excursion_column(self) -> None:
        source = _result(net_value="-1", r_value="0.5")
        result = evaluate_candidate("tp_fast_v1", source, protocol=ResearchProtocol.default(), criteria=_criteria())
        self.assertNotEqual(result["decision"], "ACCEPT")
        self.assertEqual(result["stress"]["mean_r"], "-1")

    def test_bootstrap_is_deterministic_and_one_sided(self) -> None:
        policy = _policy()
        first = bootstrap_mean_ci([Decimal("1"), Decimal("2"), Decimal("3")], policy=policy)
        second = bootstrap_mean_ci([Decimal("1"), Decimal("2"), Decimal("3")], policy=policy)
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "ASSESSED")
        self.assertEqual(first["mean"], "2")
        self.assertIsNotNone(first["lcb"])

    def test_fixture_and_low_sample_are_insufficient(self) -> None:
        protocol = ResearchProtocol.default()
        fixture = evaluate_candidate("tp_fast_v1", _result(fixture=True), protocol=protocol, criteria=_criteria())
        self.assertEqual(fixture["decision"], "INSUFFICIENT")
        self.assertIn("fixture_not_external_evidence", fixture["reasons"])

        low_criteria = EvidenceCriteria(bootstrap=_policy())
        low = evaluate_candidate("tp_fast_v1", _result(), protocol=protocol, criteria=low_criteria)
        self.assertEqual(low["decision"], "INSUFFICIENT")
        self.assertIn("holdout_episodes_below_minimum", low["reasons"])

    def test_criteria_return_reject_or_human_review_only(self) -> None:
        protocol = ResearchProtocol.default()
        criteria = _criteria()
        rejected = evaluate_candidate(
            "tp_fast_v1", _result(r_value="0.01", net_value="0.01"), protocol=protocol, criteria=criteria
        )
        self.assertEqual(rejected["decision"], "REJECT")
        self.assertFalse(rejected["auto_promote"])
        self.assertFalse(rejected["trading_enabled"])

        accepted = evaluate_candidate("tp_fast_v1", _result(), protocol=protocol, criteria=criteria)
        self.assertEqual(accepted["decision"], "ACCEPT")
        self.assertEqual(accepted["eligibility"], "HUMAN_REVIEW_ONLY")
        self.assertEqual(accepted["sample"]["coverage"]["zero_episode_blocks"], 3)
        self.assertEqual(accepted["base"]["primary_bootstrap"]["block_days"], 14)
        self.assertEqual(accepted["base"]["bootstrap_sensitivity"].keys(), {"7", "28"})
        self.assertEqual(accepted["base"]["bootstrap_sensitivity"]["7"]["block_days"], 7)
        self.assertEqual(accepted["base"]["bootstrap_sensitivity"]["28"]["block_days"], 28)
        self.assertFalse(accepted["auto_promote"])
        self.assertFalse(accepted["trading_enabled"])

        covered = _result()
        covered_holdout = covered["holdout"]
        assert isinstance(covered_holdout, dict)
        covered_holdout["covered_session_count"] = 12
        covered_result = evaluate_candidate("tp_fast_v1", covered, protocol=protocol, criteria=criteria)
        self.assertEqual(covered_result["sample"]["sessions"], 12)

    def test_declared_counts_and_lcb_do_not_replace_coverage_or_sessions(self) -> None:
        protocol = ResearchProtocol.default()
        criteria = _criteria()
        missing_coverage = _result()
        holdout = missing_coverage["holdout"]
        stress = missing_coverage["stress"]
        assert isinstance(holdout, dict) and isinstance(stress, dict)
        for section in (holdout, stress):
            section.pop("coverage_start", None)
            section.pop("coverage_end", None)
            section.pop("coverage_complete", None)
            section["episode_count"] = 1000
            section["session_count"] = 1000
            section["block_count"] = 1000
            section["lcb"] = "999"
        result = evaluate_candidate("tp_fast_v1", missing_coverage, protocol=protocol, criteria=criteria)
        self.assertEqual(result["decision"], "INSUFFICIENT")
        self.assertIn("holdout_coverage_missing_or_incomplete", result["reasons"])
        self.assertIsNone(result["base"]["lcb_r"])

        same_session = _result()
        same_holdout = same_session["holdout"]
        assert isinstance(same_holdout, dict)
        for row in same_holdout["episodes"]:
            assert isinstance(row, dict)
            row["session_id"] = "one-session"
        same_holdout["covered_session_count"] = 1
        result = evaluate_candidate("tp_fast_v1", same_session, protocol=protocol, criteria=criteria)
        self.assertEqual(result["sample"]["sessions"], 1)
        self.assertIn("holdout_sessions_below_minimum", result["reasons"])

    def test_unknown_calendar_gaps_block_inference(self) -> None:
        source = _result()
        for section_name in ("holdout", "stress"):
            section = source[section_name]
            assert isinstance(section, dict)
            section["gaps_unknown"] = True
            section["gaps_known"] = None
        result = evaluate_candidate("tp_fast_v1", source, protocol=ResearchProtocol.default(), criteria=_criteria())
        self.assertEqual(result["decision"], "INSUFFICIENT")
        self.assertIn("holdout_coverage_missing_or_incomplete", result["reasons"])

    def test_joint_report_validates_and_never_promotes(self) -> None:
        protocol = ResearchProtocol.default()
        criteria = _criteria()
        results = {candidate_id: _result() for candidate_id in CANDIDATE_IDS}
        report = evaluate_candidates(results, protocol=protocol, criteria=criteria)
        self.assertEqual(report["candidate_count"], 6)
        self.assertFalse(report["auto_promote"])
        self.assertFalse(report["trading_enabled"])
        self.assertEqual(validate_evidence(report)["state"], "VALID")


if __name__ == "__main__":
    unittest.main()
