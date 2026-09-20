from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from mtf_lab.core.cfd_simulation import CFDConfig, CFDQuote, CFDSignal, CFDSimulator, Direction
from mtf_lab.ops import market_research
from mtf_lab.ops.historical_backtest import _historical_economic_metrics
from mtf_lab.ops.market_research import MarketResearchError
from mtf_lab.ops.research_reporting import build_research_report
from mtf_lab.ops.research_reporting_metrics import MAX_TRADE_SUMMARIES

BASE = datetime(2016, 3, 7, 5, tzinfo=UTC)


def _known_config() -> CFDConfig:
    return CFDConfig(
        instrument="EUR/USD",
        horizons_seconds=(Decimal("1"),),
        max_quote_age_seconds=Decimal("5"),
        terminal_retention=1,
        event_retention=16,
        quote_id_retention=16,
        commission_known=True,
        financing_required=False,
    )


def _evicted_unknown_simulator() -> CFDSimulator:
    simulator = CFDSimulator(_known_config())
    simulator.submit(CFDSignal("unknown-1", "EUR/USD", Direction.LONG, BASE))
    simulator.advance(BASE + timedelta(seconds=6), capture_complete=False)
    simulator.submit(CFDSignal("known-2", "EUR/USD", Direction.LONG, BASE + timedelta(seconds=10)))
    simulator.on_quote(CFDQuote("EUR/USD", BASE + timedelta(seconds=10), Decimal("1.1000"), Decimal("1.1002"), "entry"))
    simulator.on_quote(CFDQuote("EUR/USD", BASE + timedelta(seconds=11), Decimal("1.1005"), Decimal("1.1007"), "close"))
    return simulator


def _ledger_row(trade_id: str, value: str = "1") -> dict[str, Any]:
    return {
        "trade_id": trade_id,
        "state": "CLOSED",
        "gross_pnl_account": value,
        "costs_account": "0.1",
        "net_pnl": "0.9",
        "costs_known": True,
    }


def _write_ledger(path: Path, rows: list[dict[str, Any]]) -> tuple[str, int]:
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, separators=(",", ":")) + "\n")
    payload = path.read_bytes()
    return hashlib.sha256(payload).hexdigest(), len(rows)


def _descriptor(path: Path, digest: str, count: int) -> dict[str, Any]:
    return {
        "kind": "ledger",
        "path": str(path),
        "count": count,
        "retained": 0,
    }


def _evidence_for_ledger(path: Path, digest: str, count: int) -> dict[str, Any]:
    return {
        "results": [
            {
                "candidate_id": "tp_fast_v1",
                "ledger": _descriptor(path, digest, count),
                "snapshot": {
                    "ledger": {
                        "kind": "ledger",
                        "status": "ASSESSED",
                        "path_reference": str(path),
                        "sha256": digest,
                        "row_count": count,
                        "complete": True,
                        "pager": {"page_size": 256, "source": "complete_jsonl_artifact"},
                    }
                },
            }
        ]
    }


class _ArtifactDTO:
    def __init__(self, rows: list[dict[str, Any]], declared_count: int) -> None:
        self.ledger = SimpleNamespace(retained=rows)
        self.equity = None
        self.funnel = None
        self._rows = rows
        self._declared_count = declared_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "variants": [],
            "ledger": {
                "kind": "ledger",
                "path": "/tmp/not-opened-ledger.jsonl",
                "count": self._declared_count,
                "retained": len(self._rows),
            },
        }


class ReauditEconomicsReportingTests(unittest.TestCase):
    def test_evicted_unknown_fails_closed_and_survives_simulator_resume(self) -> None:
        simulator = _evicted_unknown_simulator()
        self.assertTrue(simulator.archive_required)
        self.assertEqual(simulator.counters["terminal_evicted"], 1)

        resumed = CFDSimulator.from_snapshot(simulator.snapshot())
        self.assertTrue(resumed.archive_required)
        self.assertEqual(resumed.counters["terminal_evicted"], 1)
        metrics = _historical_economic_metrics(cast(Any, {"fixture": SimpleNamespace(legacy=resumed, risk=resumed)}))
        self.assertFalse(metrics["costs_applied"])
        self.assertEqual(metrics["costs_status"], "UNKNOWN_NOT_ZERO")
        self.assertEqual(metrics["net_status"], "UNKNOWN_COSTS")

    def test_inline_and_verified_paged_ledger_have_equivalent_cost_bridge(self) -> None:
        row = _ledger_row("t1", "10")
        inline = build_research_report({"results": [{"candidate_id": "tp_fast_v1", "ledger": [row]}]})
        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-ledger-") as directory:
            path = Path(directory) / "ledger.jsonl"
            digest, count = _write_ledger(path, [row])
            paged = build_research_report(_evidence_for_ledger(path, digest, count))
        for key in ("status", "closed_trade_count", "gross", "costs", "net"):
            self.assertEqual(paged["results"][0]["cost_bridge"][key], inline["results"][0]["cost_bridge"][key])
        self.assertEqual(paged["results"][0]["ledger_source"]["status"], "ASSESSED")

    def test_historical_result_retained_rows_are_preserved_but_suffix_is_not_complete(self) -> None:
        row = _ledger_row("retained", "10")
        complete = build_research_report(_ArtifactDTO([row], declared_count=1))
        self.assertEqual(complete["results"][0]["cost_bridge"]["status"], "ASSESSED")

        incomplete = build_research_report(_ArtifactDTO([row], declared_count=2))
        bridge = incomplete["results"][0]["cost_bridge"]
        self.assertEqual(bridge["status"], "INSUFFICIENT")
        self.assertEqual(bridge["reason"], "ledger_retention_incomplete")
        self.assertIsNone(bridge["net"])

    def test_equity_and_funnel_descriptors_are_not_rows(self) -> None:
        descriptor = {"kind": "equity", "path": "/tmp/equity.jsonl", "count": 1, "retained": 0}
        funnel = {"kind": "funnel", "path": "/tmp/funnel.jsonl", "count": 5, "retained": 0}
        report = build_research_report(
            {"results": [{"candidate_id": "tp_fast_v1", "ledger": [], "equity": descriptor, "funnel": funnel}]}
        )
        projection = report["results"][0]
        self.assertEqual(projection["equity"]["status"], "NOT_ASSESSED")
        self.assertEqual(projection["equity"]["reason"], "equity_snapshot_not_paged")
        self.assertEqual(report["funnel"]["status"], "NOT_ASSESSED")
        self.assertEqual(report["funnel"]["reason"], "funnel_snapshot_not_paged")
        self.assertEqual(report["funnel"]["raw_row_count"], 0)

    def test_ledger_reference_corruption_path_and_count_fail_closed(self) -> None:
        row = _ledger_row("t1")
        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-ledger-") as directory:
            path = Path(directory) / "ledger.jsonl"
            digest, count = _write_ledger(path, [row])
            cases = (
                ("hash", {"sha256": "0" * 64, "path_reference": str(path), "row_count": count}),
                (
                    "path",
                    {"sha256": digest, "path_reference": str(Path(directory) / "other.jsonl"), "row_count": count},
                ),
                ("count", {"sha256": digest, "path_reference": str(path), "row_count": count + 1}),
                (
                    "metadata",
                    {
                        "kind": "funnel",
                        "sha256": digest,
                        "path_reference": str(path),
                        "row_count": count,
                    },
                ),
            )
            for name, reference in cases:
                with self.subTest(name=name):
                    evidence = _evidence_for_ledger(path, digest, count)
                    base_reference = dict(evidence["results"][0]["snapshot"]["ledger"])
                    evidence["results"][0]["snapshot"] = {"ledger": {**base_reference, **reference}}
                    report = build_research_report(evidence)
                    bridge = report["results"][0]["cost_bridge"]
                    self.assertEqual(bridge["status"], "INSUFFICIENT")
                    self.assertIsNone(bridge["net"])
                    self.assertIsNone(bridge["closed_trade_count"])
                    self.assertNotEqual(report["results"][0]["ledger_source"]["status"], "ASSESSED")

    def test_paged_ledger_keeps_display_memory_bounded(self) -> None:
        rows = [_ledger_row(f"t-{index}") for index in range(MAX_TRADE_SUMMARIES + 50)]
        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-ledger-") as directory:
            path = Path(directory) / "ledger.jsonl"
            digest, count = _write_ledger(path, rows)
            report = build_research_report(_evidence_for_ledger(path, digest, count))
        result = report["results"][0]
        self.assertEqual(result["cost_bridge"]["closed_trade_count"], count)
        self.assertEqual(result["cost_bridge"]["source_count"], count)
        self.assertTrue(result["trade_summary_truncated"])
        self.assertLessEqual(len(result["trade_summaries"]), MAX_TRADE_SUMMARIES)
        self.assertEqual(result["ledger_source"]["display_count"], MAX_TRADE_SUMMARIES)
        self.assertEqual(result["ledger_source"]["row_count"], count)
        self.assertEqual(report["privacy"]["trade_summary_limit"], MAX_TRADE_SUMMARIES)
        self.assertEqual(len(report["trade_summaries"][0]["rows"]), MAX_TRADE_SUMMARIES)
        self.assertEqual(report["counts"]["trades"], count)
        self.assertEqual(report["counts"]["closed_trades"], count)
        self.assertEqual(report["counts"]["known_net_trades"], count)

    def test_snapshot_pages_hash_the_same_fd_bytes_and_reject_later_mutation(self) -> None:
        original = {"trade_id": "original", "state": "CLOSED"}
        changed = {"trade_id": "changed", "state": "CLOSED"}
        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-ledger-") as directory:
            path = Path(directory) / "ledger.jsonl"
            path.write_text(json.dumps(original) + "\n", encoding="utf-8")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            reference = {
                "kind": "ledger",
                "status": "ASSESSED",
                "path_reference": str(path),
                "sha256": digest,
                "row_count": 1,
                "complete": True,
                "pager": {"page_size": 256, "source": "complete_jsonl_artifact"},
            }
            original_open = market_research._open_snapshot_fd

            def swap_after_open(target: Path) -> int:
                fd = original_open(target)
                replacement = path.with_suffix(".replacement")
                replacement.write_text(json.dumps(changed) + "\n", encoding="utf-8")
                os.replace(replacement, path)
                return fd

            with patch.object(market_research, "_open_snapshot_fd", swap_after_open):
                pages = list(market_research.iter_snapshot_pages(reference))
            self.assertEqual(pages[0][0]["trade_id"], "original")
            with self.assertRaises(MarketResearchError):
                list(market_research.iter_snapshot_pages(reference))


if __name__ == "__main__":
    unittest.main()
