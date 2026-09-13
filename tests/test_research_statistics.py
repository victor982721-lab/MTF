from __future__ import annotations

import unittest
from decimal import getcontext

from mtf_lab.ops.research_statistics import result_metrics


def _result(*, unknown_mark: bool = False, unrecovered: bool = False) -> dict[str, object]:
    marks: list[dict[str, object]] = [
        {"available_at": "2026-01-01T00:00:00Z", "equity": "100", "exposure": "1000", "state": "KNOWN"},
        {"available_at": "2026-01-01T00:01:00Z", "equity": "80", "exposure": "1200", "state": "KNOWN"},
    ]
    if not unrecovered:
        marks.append({"available_at": "2026-01-01T00:02:00Z", "equity": "120", "exposure": "0", "state": "KNOWN"})
    if unknown_mark:
        marks.append({"available_at": "2026-01-01T00:03:00Z", "equity": None, "state": "UNKNOWN"})
    return {
        "observed_calendar_days": ["2026-01-01"],
        "equity_mark_to_market": marks,
        "ledger": [
            {
                "state": "CLOSED",
                "units": "100",
                "filled_quantity": "100",
                "entry_available_at": "2026-01-01T00:00:00Z",
                "close_available_at": "2026-01-01T00:02:30Z",
                "entry_price": "10",
                "close_price": "10.5",
                "net_pnl": "10",
                "costs_account": "1",
                "costs_known": True,
                "account_currency": "USD",
                "quote_currency": "USD",
            }
        ],
        "costs": {"state": "KNOWN", "known": "1", "unknown_count": 0},
    }


class ResearchStatisticsTests(unittest.TestCase):
    def test_drawdown_uses_ordered_equity_marks_and_real_recovery(self) -> None:
        metrics = result_metrics(_result())
        self.assertEqual(metrics["status"], "ASSESSED")
        self.assertEqual(metrics["max_drawdown_abs"], "20")
        self.assertEqual(metrics["max_drawdown"]["value"], "20")
        self.assertEqual(metrics["max_drawdown"]["basis"], "mark_to_market_equity")
        self.assertEqual(metrics["max_drawdown_peak_at"], "2026-01-01T00:00:00.000000Z")
        self.assertEqual(metrics["max_drawdown_trough_at"], "2026-01-01T00:01:00.000000Z")
        self.assertEqual(metrics["recovery_duration_seconds"], "120")
        self.assertEqual(metrics["unrecovered_state"], "RECOVERED")
        self.assertEqual(metrics["net_pnl"], "10")
        self.assertEqual(metrics["expectancy"], "10")
        self.assertEqual(metrics["worst_utc_observed_day"], {"date": "2026-01-01", "pnl": "10"})
        self.assertEqual(metrics["worst_session"]["session_basis"], "UTC_observed_calendar_days_only")
        self.assertEqual(metrics["exposure_peak"], "1200")
        self.assertEqual(metrics["exposure_peak_at"], "2026-01-01T00:01:00.000000Z")
        self.assertEqual(metrics["exposure_peak_quantity"], "100")
        self.assertEqual(metrics["exposure_duration_seconds"], "120")
        self.assertEqual(metrics["exposure_notional_time"], "132000")
        self.assertEqual(metrics["turnover_notional"], "2050.0")
        self.assertEqual(metrics["turnover_currency"], "USD")
        self.assertEqual(metrics["cost_total_known"], "1")
        self.assertEqual(metrics["cost_total_unknown"], "0")
        self.assertEqual(metrics["trade_concentration_hhi"], "1")
        self.assertEqual(metrics["day_concentration_hhi"], "1")
        self.assertEqual(metrics["margin"]["status"], "NOT_ASSESSED")
        self.assertIsNone(metrics["margin"]["value"])

    def test_unknown_marks_or_costs_withhold_full_metrics_but_keep_subtotals(self) -> None:
        result = _result(unknown_mark=True)
        result["ledger"] = [
            result["ledger"][0],
            {
                "state": "CLOSED",
                "units": "100",
                "entry_available_at": "2026-01-01T00:00:30Z",
                "close_available_at": "2026-01-01T00:03:00Z",
                "net_pnl": None,
                "costs_account": None,
                "costs_known": False,
            },
        ]
        result["costs"] = {"state": "PARTIAL_UNKNOWN", "known": "1", "unknown_count": 1}
        metrics = result_metrics(result)
        self.assertEqual(metrics["status"], "NOT_ASSESSED")
        self.assertIsNone(metrics["max_drawdown_abs"])
        self.assertIsNone(metrics["net_pnl"])
        self.assertEqual(metrics["partial"]["max_drawdown_abs_known"], "20")
        self.assertEqual(metrics["partial"]["known_net_pnl_subtotal"], "10")
        self.assertEqual(metrics["partial"]["unknown_mark_count"], 1)
        self.assertGreaterEqual(metrics["partial"]["unknown_cost_count"], 1)
        self.assertEqual(metrics["cost_total_known"], "1")
        self.assertIsNone(metrics["cost_total_unknown"])

    def test_known_unrecovered_drawdown_is_not_reported_as_recovered(self) -> None:
        metrics = result_metrics(_result(unrecovered=True))
        self.assertEqual(metrics["status"], "ASSESSED")
        self.assertEqual(metrics["max_drawdown_abs"], "20")
        self.assertIsNone(metrics["recovery_duration_seconds"])
        self.assertEqual(metrics["unrecovered_state"], "UNRECOVERED")
        self.assertEqual(metrics["recovery"]["unrecovered_state"], "UNRECOVERED")

    def test_mark_order_is_normalized_in_utc_without_fabricating_calendar_days(self) -> None:
        result = _result()
        result["equity_mark_to_market"] = list(reversed(result["equity_mark_to_market"]))
        metrics = result_metrics(result)
        self.assertEqual(metrics["max_drawdown_abs"], "20")
        self.assertEqual(metrics["worst_utc_observed_day"]["date"], "2026-01-01")
        self.assertFalse(metrics["daily_aggregate"]["weekends_fabricated"])

    def test_zero_cancelled_or_no_fill_has_no_division_or_fake_margin(self) -> None:
        result = {
            "observed_calendar_days": ["2026-01-01"],
            "equity_mark_to_market": [
                {"available_at": "2026-01-01T00:00:00Z", "equity": "0", "exposure": "0", "state": "KNOWN"}
            ],
            "ledger": [
                {
                    "state": "REJECTED",
                    "units": "100",
                    "requested_quantity": "100",
                    "filled_quantity": "0",
                    "cancelled_quantity": "100",
                    "entry_price": None,
                    "net_pnl": None,
                    "costs_known": True,
                }
            ],
            "costs": {"state": "KNOWN", "known": "0", "unknown_count": 0},
        }
        old_precision = getcontext().prec
        try:
            getcontext().prec = 3
            metrics = result_metrics(result)
        finally:
            getcontext().prec = old_precision
        self.assertEqual(metrics["status"], "NOT_ASSESSED")
        self.assertEqual(metrics["max_drawdown"]["value"], "0")
        self.assertEqual(metrics["turnover"]["notional"], "0")
        self.assertEqual(metrics["partial"]["turnover_notional_known"], "0")
        self.assertIsNone(metrics["trade_concentration_hhi_known"])
        self.assertEqual(metrics["turnover"]["capital_ratio"]["status"], "NOT_ASSESSED")
        self.assertEqual(metrics["margin"]["reason"], "margin_or_leverage_inputs_missing")
        self.assertIsNone(metrics["margin"]["value"])
        self.assertEqual(metrics["cost_total_known"], "0")
        self.assertEqual(metrics["cost_unknown_count"], 0)


if __name__ == "__main__":
    unittest.main()
