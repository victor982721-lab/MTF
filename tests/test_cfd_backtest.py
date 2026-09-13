from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.configuration import load_config
from mtf_lab.data.models import Bar
from mtf_lab.ops.cfd_backtest import (
    ResearchWindow,
    assign_trade_window,
    build_research_windows,
    load_cfd_capture,
    replay_interleaved,
    run_cfd_backtest,
    synthetic_cfd_capture,
)
from mtf_lab.ops.cfd_simulation import CFDConfig, CFDQuote, CFDSignal, CFDSimulator


class CFDBacktestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config("config/ctrader_pipeline_fixture.toml")
        self.capture = synthetic_cfd_capture(config=self.config, count=190)

    def test_shared_pipeline_returns_cfd_ledger_and_mark_to_market(self) -> None:
        result = run_cfd_backtest(
            self.capture,
            config=self.config,
            variant="trend_pullback_v1",
            horizon_seconds=60,
        )
        self.assertEqual(result["product"], "FOREX_CFD_LOCAL_PAPER")
        self.assertEqual(result["detector"], "TrendPullbackStrategy")
        self.assertGreaterEqual(result["signals"], 1)
        self.assertEqual(result["trades"], len(result["ledger"]))
        self.assertTrue(result["equity_mark_to_market"])
        self.assertIn("peak", result["exposure"])
        self.assertIn("known", result["costs"])
        self.assertFalse(result["leakage_violations"])
        self.assertNotIn("binary", json.dumps(result).lower())

    def test_windows_have_required_policy_and_preserve_warmup(self) -> None:
        windows = build_research_windows(self.capture)
        self.assertEqual(
            [item.name for item in windows],
            [
                "walkforward_1",
                "walkforward_2",
                "walkforward_3",
                "walkforward_4",
                "holdout",
            ],
        )
        self.assertTrue(all(item.to_dict()["warmup_preserved"] for item in windows))
        self.assertEqual(windows[0].purge_holding_seconds, 0)
        self.assertEqual(windows[-1].test_records + windows[-1].train_records, len(self.capture.quote_events))

    def test_capture_path_round_trip_uses_envelopes_without_mutating_source(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-capture-") as tmp:
            source = Path(tmp) / "capture.json"
            source.write_text(
                json.dumps({"envelopes": [item.to_dict() for item in self.capture.envelopes]}),
                encoding="utf-8",
            )
            before = source.read_bytes()
            loaded = load_cfd_capture(source, config=self.config)
            self.assertEqual(loaded.capture_hash, self.capture.capture_hash)
            self.assertEqual(source.read_bytes(), before)

    def test_actual_holding_crossing_test_window_is_purged(self) -> None:
        window = ResearchWindow(
            name="test",
            kind="walkforward",
            index=0,
            warmup_start=datetime(2026, 1, 1, tzinfo=UTC),
            train_start=datetime(2026, 1, 1, tzinfo=UTC),
            train_end=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
            test_start=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
            test_end=datetime(2026, 1, 1, 0, 2, tzinfo=UTC),
            purge_holding_seconds=60,
            embargo_seconds=60,
            warmup_records=1,
            train_records=2,
            test_records=1,
        )
        row = {
            "entry_available_at": "2026-01-01T00:01:30Z",
            "close_available_at": "2026-01-01T00:02:30Z",
        }
        self.assertEqual(assign_trade_window(row, window), ("PURGED", "actual_holding_crosses_test_boundary"))

    def test_donchian_variant_routes_real_implementation_not_baseline_label(self) -> None:
        bars = tuple(
            Bar(
                instrument="EUR/USD",
                interval_start=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=5 * index),
                interval_end=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=5 * (index + 1)),
                open=100.0,
                high=101.0 if index < 20 else 150.0,
                low=99.0,
                close=100.0 if index < 20 else 102.0,
                resolution_seconds=300,
                price_basis="native",
                source="synthetic-fixture",
                available_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=5 * (index + 1)),
            )
            for index in range(21)
        )
        capture = replace(self.capture, bars=bars)
        result = run_cfd_backtest(
            capture,
            config=self.config,
            variant="donchian20_m5_v1",
            horizon_seconds=60,
        )
        self.assertEqual(result["detector"], "Donchian20M5Strategy")
        self.assertEqual(result["strategy_route"]["route"], "StrategyProtocol")
        self.assertNotEqual(result["strategy_route"]["strategy_name"], "trend_pullback_v1")

    def test_interleaved_replay_does_not_preload_future_signals_into_capacity(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=UTC)
        config = CFDConfig(
            instrument="EUR/USD",
            units="1000",
            horizons_seconds=(60,),
            max_active_trades=1,
            max_quote_age_seconds=10,
        )
        simulator = CFDSimulator(config)
        signals = (
            CFDSignal("early", "EUR/USD", "LONG", base),
            CFDSignal("future", "EUR/USD", "LONG", base + timedelta(seconds=120)),
        )
        quotes = (
            CFDQuote("EUR/USD", base, "1.1000", "1.1002", "q0"),
            CFDQuote("EUR/USD", base + timedelta(seconds=60), "1.1005", "1.1007", "q1"),
            CFDQuote("EUR/USD", base + timedelta(seconds=120), "1.1000", "1.1002", "q2"),
            CFDQuote("EUR/USD", base + timedelta(seconds=180), "1.1005", "1.1007", "q3"),
        )
        result = replay_interleaved(
            simulator,
            signals,
            quotes,
            horizon_seconds=60,
            capture_complete=True,
        )
        self.assertEqual(len(result.trades), 2)

    def test_unknown_economics_are_visible_and_not_coerced_to_zero(self) -> None:
        result = run_cfd_backtest(
            self.capture,
            config=self.config,
            variant="trend_pullback_v1",
            horizon_seconds=60,
            fixture=False,
        )
        self.assertEqual(result["costs"]["state"], "PARTIAL_UNKNOWN")
        self.assertIsNone(result["equity_summary"]["net_pnl"])
        self.assertIsNone(result["equity_summary"]["known_net_pnl_subtotal"])
        self.assertGreaterEqual(result["equity_summary"]["closed_unknown_count"], 1)
        self.assertEqual(result["equity_summary"]["economic_state"], "INDETERMINATE")

    def test_ioc_partial_models_effective_quantity_and_first_fill_cancellation(self) -> None:
        result = run_cfd_backtest(
            self.capture,
            config=self.config,
            variant="trend_pullback_v1",
            horizon_seconds=60,
            execution_model="ioc_partial",
            fill_fraction="0.25",
        )
        self.assertEqual(result["execution_model"]["scenario"], "ioc_partial")
        self.assertEqual(result["execution_model"]["partial_fills"], "MODELED_SYNTHETIC_HYPOTHESIS")
        row = result["ledger"][0]
        self.assertEqual(row["units"], "250.00")
        self.assertEqual(row["requested_quantity"], "1000")
        self.assertEqual(row["filled_quantity"], "250.00")
        self.assertEqual(row["cancelled_quantity"], "750.00")
        self.assertEqual(row["cancel_reason"], "IOC_RESIDUAL_CANCELLED_ON_FIRST_FILL")
        self.assertTrue(row["executed"])

    def test_rejected_execution_scenario_has_no_execution_or_cost_claim(self) -> None:
        result = run_cfd_backtest(
            self.capture,
            config=self.config,
            variant="trend_pullback_v1",
            horizon_seconds=60,
            execution_model="rejected",
        )
        self.assertEqual(result["execution_model"]["scenario"], "rejected")
        self.assertFalse(result["execution_model"]["executed"])
        self.assertEqual(result["execution_model"]["filled_quantity"], "0")
        self.assertEqual(result["execution_model"]["rejected_quantity"], "1000")
        self.assertEqual(result["equity_summary"]["economic_state"], "NO_EXECUTION")
        self.assertEqual(result["costs"]["state"], "KNOWN")
        self.assertTrue(all(row["state"] == "REJECTED" for row in result["ledger"]))
        self.assertTrue(all(not row["executed"] and row["entry_price"] is None for row in result["ledger"]))

    def test_unknown_variant_is_rejected_instead_of_relabeled(self) -> None:
        with self.assertRaises(ValueError):
            run_cfd_backtest(
                self.capture,
                config=self.config,
                variant="unimplemented_challenger",
                horizon_seconds=60,
            )


if __name__ == "__main__":
    unittest.main()
