from __future__ import annotations

import json
import os
import tempfile
import unittest
from typing import cast

from mtf_lab.ops.research_reporting import build_research_report, render_research_report


def _trade(trade_id: str, direction: str = "LONG") -> dict[str, object]:
    return {
        "trade_id": trade_id,
        "signal_id": f"signal-{trade_id}",
        "episode_id": f"episode-{trade_id}",
        "direction": direction,
        "state": "CLOSED",
        "entry_available_at": "2025-01-02T10:00:00Z",
        "close_available_at": "2025-01-02T10:02:00Z",
        "entry_price": "1.1000",
        "close_price": "1.1010" if direction == "LONG" else "1.0990",
        "gross_pnl_account": "10",
        "costs_account": "2",
        "commission_account": "1",
        "slippage_account": "1",
        "net_pnl": "8",
        "costs_known": True,
        "account_currency": "USD",
    }


def _evidence() -> dict[str, object]:
    quotes = [
        {
            "quote_id": f"q-{index}",
            "available_at": f"2025-01-02T10:0{index}:00Z",
            "timeframe": "M1",
            "bid": 1.1000 + index * 0.0001,
            "ask": 1.1002 + index * 0.0001,
            "atr": 0.0010,
            "session": "LONDON",
        }
        for index in range(3)
    ]
    funnel = [
        {"variant": "tp-fast", "timeframe": "M1", "stage": "detected", "evaluation_id": "eval-1", "event_id": "e1"},
        {"variant": "tp-fast", "timeframe": "M1", "stage": "detected", "evaluation_id": "eval-1", "event_id": "e1"},
        {"variant": "tp-fast", "timeframe": "M1", "stage": "eligible", "evaluation_id": "eval-1", "event_id": "e2"},
    ]
    return {
        "schema": "mtf-lab.research-evidence.v1",
        "run_id": "run-1",
        "instrument": "EUR/USD",
        "data_hash": "dataset-hash",
        "provenance": {"quote_source": "REAL_HISTORICAL_QUOTES", "fill_source": "MODEL_FILL"},
        "variants": ["tp-fast"],
        "quotes": quotes,
        "results": [
            {
                "trial_id": "eval-1",
                "variant": "tp-fast",
                "timeframe": "M1",
                "evaluation_id": "eval-1",
                "quotes": quotes,
                "funnel": funnel,
                "ledger": [_trade("t1")],
                "equity_mark_to_market": [
                    {"available_at": "2025-01-02T10:00:00Z", "equity": "100", "realized": "100", "state": "KNOWN"},
                    {"available_at": "2025-01-02T10:01:00Z", "equity": "98", "realized": "100", "state": "KNOWN"},
                    {"available_at": "2025-01-02T10:03:00Z", "equity": "108", "realized": "108", "state": "KNOWN"},
                ],
                "decision": {"status": "EVIDENCE_INSUFFICIENT", "reason": "fixture"},
            }
        ],
    }


def _real_market_structure() -> dict[str, object]:
    def distribution(
        n: int, mean: str | None, *, lower: str | None = None, upper: str | None = None
    ) -> dict[str, object]:
        return {
            "n": n,
            "mean": mean,
            "variance_population": None,
            "min": None,
            "max": None,
            "quantiles": {
                "p50": None if lower is None else {"lower": lower, "upper_exclusive": upper, "overflow": upper is None}
            },
            "quantile_method": "fixed_width_histogram_interval_no_interpolation",
            "histogram_width": "0.01",
            "overflow_count": 0,
        }

    quote_statistics = {
        "quote_count": 5,
        "coverage_start": "2025-01-01T00:00:00Z",
        "coverage_end": "2025-01-02T00:00:00Z",
        "spread_unit": "pip = 0.0001 USD per EUR",
        "spread_tick_weighted": distribution(5, "1.20", lower="1.00", upper="1.50"),
        "spread_by_hour_utc": {
            "00": distribution(2, "1.10", lower="1.00", upper="1.20"),
            "01": distribution(0, None),
        },
        "spread_by_session": {
            "UTC_00_08": distribution(2, "1.10"),
            "UTC_08_16": distribution(0, None),
        },
        "session_definition": "nonoverlapping fixed UTC descriptive blocks",
        "spread_time_weighted_pips": None,
        "time_weighted_covered_seconds": "0",
        "time_weighted_model": "last-known quote within explicit bound",
        "gaps_over_declared_bound": 1,
        "largest_gaps": [{"from": "2025-01-01T00:00:00Z", "to": "2025-01-01T01:00:00Z", "seconds": 3600}],
        "gap_classification": "UNKNOWN; includes scheduled closures",
        "days_with_quotes": [
            {
                "date_utc": "2025-01-01",
                "ticks": 3,
                "first_quote": "2025-01-01T00:00:00Z",
                "last_quote": "2025-01-01T00:59:00Z",
                "full_session": "UNKNOWN",
            },
            {
                "date_utc": "2025-01-02",
                "ticks": 2,
                "first_quote": "2025-01-02T00:00:00Z",
                "last_quote": "2025-01-02T00:59:00Z",
                "full_session": "UNKNOWN",
            },
        ],
    }
    return {
        "schema": "mtf-lab.market-structure.v1",
        "dataset_id": "histdata-eurusd-2025-01",
        "dataset_content_hash": "hash-real-structure",
        "source": "REAL_HISTORICAL_QUOTES_NOT_DEMO_FILLS",
        "availability": "HISTORICAL_EVENT_TIME_MODELED_NOT_RECEIPT",
        "volume": None,
        "max_quote_gap_seconds": 90.0,
        "calendar_identity": {
            "basis": "MODELED_WEEKLY_FX_NOT_ACCOUNT_VERIFIED",
            "provenance_uri": "https://example.invalid/calendar.pdf",
        },
        "normalization_version": "historical-v1",
        "quote_statistics": quote_statistics,
        "timeframes": {
            "M1": {
                "timeframe": "M1",
                "closed_bars": 2,
                "coverage_valid_bars": 2,
                "issues": {},
                "spread_atr": distribution(1, "0.50"),
                "spread_atr_by_close_hour_utc": {},
                "spread_atr_basis": "latest just closed ATR14",
                "spread_atr_unit": "dimensionless",
                "returns": {},
                "calendar_gap_policy": "STRICT_UNKNOWN_GAPS_RESET_INDICATORS",
                "last_open_bar_excluded": True,
            },
            "H1": {
                "timeframe": "H1",
                "closed_bars": 0,
                "coverage_valid_bars": 0,
                "issues": {},
                "spread_atr": distribution(0, None),
                "spread_atr_by_close_hour_utc": {},
                "spread_atr_basis": "latest just closed ATR14",
                "spread_atr_unit": "dimensionless",
                "returns": {},
                "calendar_gap_policy": "STRICT_UNKNOWN_GAPS_RESET_INDICATORS",
                "last_open_bar_excluded": True,
            },
            "D1": {
                "timeframe": "D1",
                "closed_bars": 0,
                "coverage_valid_bars": 0,
                "issues": {"partial_bucket": 1},
                "spread_atr": distribution(0, None),
                "spread_atr_by_close_hour_utc": {},
                "spread_atr_basis": "current_quote_spread_divided_by_latest_just_closed_ATR14",
                "spread_atr_unit": "dimensionless",
                "returns": {},
                "calendar_gap_policy": "STRICT_UNKNOWN_GAPS_RESET_INDICATORS",
                "last_open_bar_excluded": True,
            },
        },
        "economic_conclusion": "NOT_ASSESSED_DESCRIPTIVE_ONLY",
        "profitability_claim": "NONE",
        "provider_comparison": "NOT_ASSESSED_SECOND_PROVIDER_NOT_ACQUIRED",
    }


class ResearchReportingTests(unittest.TestCase):
    def test_projection_keeps_provenance_costs_and_unknowns_separate(self) -> None:
        report = build_research_report(_evidence())
        self.assertEqual(report["schema"], "mtf-lab.research-report.v1")
        self.assertEqual(report["provenance"]["labels"], ["MODEL_FILL", "REAL_HISTORICAL_QUOTES"])
        self.assertEqual(report["market"]["overall"]["spread"]["count"], 3)
        self.assertEqual(report["results"][0]["cost_bridge"]["net"], 8)
        self.assertTrue(report["results"][0]["cost_bridge"]["spread_and_slippage_not_subtracted_twice"])
        self.assertEqual(report["results"][0]["drawdown"]["max_drawdown"], 2)
        self.assertEqual(report["funnel"]["duplicate_row_count"], 1)
        self.assertEqual(report["funnel"]["deduplicated_row_count"], 2)
        self.assertEqual(report["decision_summary"]["status"], "INSUFFICIENT")

    def test_visual_series_is_bounded_and_html_has_no_remote_dependency(self) -> None:
        evidence = _evidence()
        source_result = cast(dict[str, object], cast(list[object], _evidence()["results"])[0])
        evidence["results"] = [
            {
                **source_result,
                "equity_mark_to_market": [
                    {
                        "available_at": f"2025-01-02T00:{index // 60:02d}:{index % 60:02d}Z",
                        "equity": str(index),
                        "state": "KNOWN",
                    }
                    for index in range(2_500)
                ],
            }
        ]
        with tempfile.TemporaryDirectory(prefix="mtf-research-report-") as directory:
            paths = render_research_report(evidence, directory)
            self.assertTrue(paths.json_path.is_file())
            self.assertTrue(paths.html_path.is_file())
            report = json.loads(paths.json_path.read_text(encoding="utf-8"))
            self.assertLessEqual(len(report["equity"][0]["mark_to_market"]["points"]), 1_000)
            html = paths.html_path.read_text(encoding="utf-8").lower()
            self.assertNotIn("plotly", html)
            self.assertNotIn("<script src", html)
            self.assertNotIn("https://", html)
            self.assertEqual(os.stat(paths.json_path).st_mode & 0o777, 0o600)
            self.assertEqual(os.stat(paths.html_path).st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                render_research_report(evidence, directory)

    def test_secret_like_values_are_redacted_without_changing_input(self) -> None:
        evidence = _evidence()
        evidence["credentials"] = {"client_secret": "do-not-publish", "account_id": "private"}
        original = json.dumps(evidence, sort_keys=True)
        report = build_research_report(evidence)
        encoded = json.dumps(report, sort_keys=True)
        self.assertNotIn("do-not-publish", encoded)
        self.assertNotIn('"private"', encoded)
        self.assertEqual(json.dumps(evidence, sort_keys=True), original)

    def test_real_market_structure_dto_projects_direct_aggregates_and_four_non_economic_svgs(self) -> None:
        dto = _real_market_structure()
        evidence = {
            "market_structure": dto,
            "measurement": {"quote_count": 499804, "elapsed_seconds": 173.88, "peak_rss_bytes": 187445248},
            "quality": [{"status": "ASSESSED", "scope": "descriptive"}],
            "insufficient_cases": [{"status": "NOT_ASSESSED", "reason": "no_economic_ledger"}],
            "evidence_gates": {"status": "DESCRIPTIVE_ONLY", "trading_enabled": False},
        }
        report = build_research_report(evidence)
        summary = report["market_structure_projection"]
        self.assertEqual(summary["source"], "REAL_HISTORICAL_QUOTES_NOT_DEMO_FILLS")
        self.assertEqual(summary["quote_count"], 5)
        self.assertEqual(summary["spread_mean_by_hour_utc"][0]["mean"], 1.1)
        self.assertIsNone(summary["spread_mean_by_hour_utc"][1]["mean"])
        self.assertEqual(summary["spread_quantile_intervals_by_hour_utc"][0]["quantiles"]["p50"]["lower"], "1.00")
        self.assertEqual(
            summary["spread_quantile_intervals_by_hour_utc"][0]["quantiles"]["p50"]["upper_exclusive"], "1.20"
        )
        timeframe_m1 = next(item for item in summary["spread_atr_by_timeframe"] if item["timeframe"] == "M1")
        self.assertEqual(timeframe_m1["n"], 1)
        self.assertEqual(timeframe_m1["unit"], "dimensionless")
        self.assertEqual(timeframe_m1["warmup_status"], "UNKNOWN")
        self.assertEqual(summary["coverage_daily"][0]["ticks"], 3)
        self.assertEqual(summary["gaps_utc"]["count"], 1)
        self.assertEqual(report["provenance"]["labels"], ["REAL_HISTORICAL_QUOTES"])
        self.assertEqual(report["measurement"]["quote_count"], 499804)
        self.assertEqual(report["quality"][0]["status"], "ASSESSED")
        self.assertEqual(report["evidence_gates"]["status"], "DESCRIPTIVE_ONLY")
        with tempfile.TemporaryDirectory(prefix="mtf-structure-report-") as directory:
            paths = render_research_report(evidence, directory)
            html = paths.html_path.read_text(encoding="utf-8")
            self.assertEqual(html.count("<svg"), 4)
            self.assertIn("Spread por hora UTC", html)
            self.assertIn("dimensionless", html)
            self.assertIn("ticks observados", html)
            self.assertIn("seconds", html)
            self.assertNotIn("Equity MTM", html)
            self.assertNotIn("Neto conocido", html)
            self.assertIn("Evidence gates", html)
            self.assertIn("https://example.invalid/calendar.pdf", html)
            self.assertNotIn("<script src", html)
            self.assertNotIn("<img ", html)
            self.assertNotIn("<link ", html)
            report_json = paths.json_path.read_text(encoding="utf-8")
            self.assertNotIn('"quotes": [', report_json)
            self.assertIn("https://example.invalid/calendar.pdf", report_json)

    def test_supplied_insufficient_messages_are_preserved_and_visible(self) -> None:
        evidence = {
            "market_structure": _real_market_structure(),
            "quality": {
                "status": "SOURCE_VALIDATED_COVERAGE_LIMITS",
                "raw_validation": "VALIDATED",
                "raw_issues_count": 0,
                "global_quality": "NOT_ASSESSED",
            },
            "insufficient_cases": ["mensaje A", "mensaje B"],
            "cautions": ["cautela visible"],
        }
        report = build_research_report(evidence)
        self.assertEqual(report["insufficient_cases"], ["mensaje A", "mensaje B"])
        self.assertEqual(report["quality"]["status"], "SOURCE_VALIDATED_COVERAGE_LIMITS")
        with tempfile.TemporaryDirectory(prefix="mtf-insufficient-report-") as directory:
            paths = render_research_report(evidence, directory)
            rendered = paths.html_path.read_text(encoding="utf-8")
            self.assertIn("<li>mensaje A</li>", rendered)
            self.assertIn("<li>mensaje B</li>", rendered)
            self.assertIn("<li>cautela visible</li>", rendered)
            self.assertIn("SOURCE_VALIDATED_COVERAGE_LIMITS", rendered)
            self.assertIn("VALIDATED · issues=0", rendered)

    def test_timeframe_chart_has_numeric_n_and_explicit_unknown_denominator(self) -> None:
        evidence = {"market_structure": _real_market_structure()}
        with tempfile.TemporaryDirectory(prefix="mtf-timeframe-report-") as directory:
            paths = render_research_report(evidence, directory)
            rendered = paths.html_path.read_text(encoding="utf-8")
            self.assertIn("M1 n=1", rendered)
            self.assertIn("D1 n=0", rendered)
            self.assertIn("D=UNKNOWN", rendered)
            self.assertNotIn("n=sample", rendered)
            self.assertNotIn("n=TF", rendered)

    def test_gap_table_marks_truncated_largest_gap_list(self) -> None:
        dto = _real_market_structure()
        quote_statistics = dict(cast(dict[str, object], dto["quote_statistics"]))
        quote_statistics["gaps_over_declared_bound"] = 3
        quote_statistics["largest_gaps"] = [
            {"from": "2025-01-01T00:00:00Z", "to": "2025-01-01T01:00:00Z", "seconds": 3600}
        ]
        dto["quote_statistics"] = quote_statistics
        with tempfile.TemporaryDirectory(prefix="mtf-gap-report-") as directory:
            paths = render_research_report({"market_structure": dto}, directory)
            rendered = paths.html_path.read_text(encoding="utf-8")
            self.assertIn("Mostrando 1 mayores de 3 gaps publicados", rendered)
            self.assertIn("2 adicionales no se muestran", rendered)

    def test_provenance_does_not_turn_networked_demo_quotes_into_server_fills(self) -> None:
        report = build_research_report(
            {
                "environment": "DEMO",
                "network_performed": True,
                "account_environment": "DEMO",
                "execution_model": {"source": "SERVER_DEMO"},
                "quotes": [{"quote_id": "demo-q", "bid": "1.0", "ask": "1.1"}],
            }
        )
        self.assertEqual(report["provenance"]["quote_sources"], ["UNKNOWN"])
        self.assertEqual(report["provenance"]["fill_sources"], ["UNKNOWN"])
        self.assertEqual(report["provenance"]["labels"], ["UNKNOWN"])

    def test_provenance_does_not_promote_synthetic_history_to_real_or_model_fill(self) -> None:
        report = build_research_report(
            {
                "source": "synthetic-history",
                "synthetic": True,
                "quotes": [{"quote_id": "synthetic-q", "bid": "1.0", "ask": "1.1"}],
            }
        )
        self.assertEqual(report["provenance"]["quote_source"], "UNKNOWN")
        self.assertEqual(report["provenance"]["fill_source"], "UNKNOWN")
        self.assertTrue(report["provenance"]["synthetic"])

    def test_unknown_historical_text_is_not_a_real_quote_enum(self) -> None:
        report = build_research_report({"source": "UNKNOWN_HISTORICAL", "quotes": []})
        self.assertEqual(report["provenance"]["quote_source"], "UNKNOWN")
        self.assertEqual(report["provenance"]["labels"], ["UNKNOWN"])

    def test_explicit_quote_and_fill_dimensions_remain_separate(self) -> None:
        model = build_research_report(
            {"provenance": {"quote_source": "REAL_HISTORICAL_QUOTES", "fill_source": "MODEL_FILL"}}
        )
        server = build_research_report({"provenance": {"quote_source": "SERVER_DEMO", "fill_source": "SERVER_DEMO"}})
        quote_only = build_research_report({"provenance": {"quote_source": "SERVER_DEMO"}})
        fill_only = build_research_report({"provenance": {"fill_source": "SERVER_DEMO"}})
        self.assertEqual(model["provenance"]["quote_source"], "REAL_HISTORICAL_QUOTES")
        self.assertEqual(model["provenance"]["fill_source"], "MODEL_FILL")
        self.assertEqual(server["provenance"]["quote_source"], "SERVER_DEMO")
        self.assertEqual(server["provenance"]["fill_source"], "SERVER_DEMO")
        self.assertEqual(quote_only["provenance"]["fill_source"], "UNKNOWN")
        self.assertEqual(fill_only["provenance"]["quote_source"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
