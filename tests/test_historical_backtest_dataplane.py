"""Offline acceptance tests for the historical shared data plane."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_historical_backtest import manifest, quotes

from mtf_lab.ops.historical_backtest import (
    HistoricalBacktestConfig,
    HistoricalBacktestError,
    _historical_quote_to_event,
    _new_historical_data_plane,
    _profile_state,
    run_historical_backtest,
)
from mtf_lab.ops.market_protocol import ResearchProtocol
from mtf_lab.runtime.processor import DataPlaneResult, SharedDataPlane


class HistoricalBacktestDataPlaneTests(unittest.TestCase):
    def test_default_run_has_one_plane_for_all_six_timeframes_and_one_feed_per_quote(self) -> None:
        config = HistoricalBacktestConfig.from_protocol(ResearchProtocol.default())
        fixture = quotes(32)
        calls: list[object] = []
        original = SharedDataPlane.feed_event

        def counted(self: SharedDataPlane, record: object) -> DataPlaneResult:
            calls.append(record)
            return original(self, record)

        with tempfile.TemporaryDirectory(prefix="mtf-historical-plane-") as directory:
            root = Path(directory)
            with patch("mtf_lab.ops.historical_backtest.SharedDataPlane.feed_event", counted):
                result = run_historical_backtest(
                    fixture,
                    manifest=manifest(root),
                    config=config,
                    output_dir=root / "output",
                    sink=lambda kind, row: None,
                )

        state = result.checkpoint.state
        raw_plane = state.get("data_plane")
        self.assertIsInstance(raw_plane, dict)
        assert isinstance(raw_plane, dict)
        self.assertEqual(raw_plane["timeframes"], ["M1", "M5", "M15", "H1", "H4", "D1"])
        self.assertEqual(raw_plane["events_processed"], len(fixture))
        self.assertEqual(len(calls), len(fixture))
        self.assertEqual(
            tuple(state["profiles"][candidate]["processor"]["checkpoint_scope"] for candidate in config.candidate_ids),
            ("strategy",) * 6,
        )
        self.assertTrue(
            all("data_plane" not in state["profiles"][candidate]["processor"] for candidate in config.candidate_ids)
        )

    def test_single_profile_shared_frames_match_legacy_candles_points_and_strategy_results(self) -> None:
        config = HistoricalBacktestConfig.from_protocol(
            ResearchProtocol.default(),
            candidates=("tp_fast_v1",),
        )
        profile = config.profiles[0]
        legacy = _profile_state(config, profile, "EUR/USD")
        plane = _new_historical_data_plane(config, "EUR/USD")
        shared = _profile_state(config, profile, "EUR/USD", data_plane=plane)

        for raw_quote in quotes(120):
            event = _historical_quote_to_event(raw_quote)
            legacy_result = legacy.processor.process_event(event)
            shared_result = shared.processor.feed_precomputed(plane.feed_event(event))
            self.assertEqual(shared_result.accepted, legacy_result.accepted)
            self.assertEqual(
                tuple(
                    (item.start, item.end, item.open, item.high, item.low, item.close, item.event_count)
                    for item in shared_result.candles
                ),
                tuple(
                    (item.start, item.end, item.open, item.high, item.low, item.close, item.event_count)
                    for item in legacy_result.candles
                ),
            )
            self.assertEqual(
                tuple(item.as_dict() for item in shared_result.evaluations),
                tuple(item.as_dict() for item in legacy_result.evaluations),
            )
            self.assertEqual(
                tuple(item.signal_id for item in shared_result.signals),
                tuple(item.signal_id for item in legacy_result.signals),
            )

        for timeframe in profile.timeframes:
            self.assertEqual(
                [
                    (item.start, item.end, item.open, item.high, item.low, item.close, item.event_count)
                    for item in legacy.processor.candles[timeframe]
                ],
                [
                    (item.start, item.end, item.open, item.high, item.low, item.close, item.event_count)
                    for item in shared.processor.candles[timeframe]
                ],
            )
            self.assertEqual(
                [
                    (
                        item.start,
                        item.end,
                        item.available_at,
                        item.close,
                        item.ema_fast,
                        item.ema_slow,
                        item.rsi,
                        item.atr,
                    )
                    for item in legacy.processor.indicator_points[timeframe]
                ],
                [
                    (
                        item.start,
                        item.end,
                        item.available_at,
                        item.close,
                        item.ema_fast,
                        item.ema_slow,
                        item.rsi,
                        item.atr,
                    )
                    for item in shared.processor.indicator_points[timeframe]
                ],
            )

    def test_shared_plane_checkpoint_resume_matches_uninterrupted_run(self) -> None:
        config = HistoricalBacktestConfig.from_protocol(ResearchProtocol.default())
        fixture = quotes(32)
        with tempfile.TemporaryDirectory(prefix="mtf-historical-plane-resume-") as directory:
            root = Path(directory)
            full = run_historical_backtest(
                fixture,
                manifest=manifest(root / "manifest-full"),
                config=config,
                output_dir=root / "full",
                sink=lambda kind, row: None,
            )
            prefix = run_historical_backtest(
                fixture[:10],
                manifest=manifest(root / "manifest-resume"),
                config=config,
                output_dir=root / "resume",
                sink=lambda kind, row: None,
            )
            resumed = run_historical_backtest(
                fixture,
                manifest=manifest(root / "manifest-resume"),
                config=config,
                output_dir=root / "resume",
                sink=lambda kind, row: None,
                resume=prefix.checkpoint,
            )

        self.assertEqual(resumed.processed_quotes, full.processed_quotes)
        self.assertEqual(resumed.checkpoint.state["data_plane"], full.checkpoint.state["data_plane"])
        self.assertEqual(
            [(item.candidate_id, item.metrics) for item in resumed.variants],
            [(item.candidate_id, item.metrics) for item in full.variants],
        )
        self.assertEqual(resumed.ledger.count, full.ledger.count)
        self.assertEqual(resumed.equity.count, full.equity.count)
        self.assertEqual(resumed.funnel.count, full.funnel.count)

    def test_duplicate_or_out_of_order_source_is_rejected_before_shared_feed(self) -> None:
        config = HistoricalBacktestConfig.from_protocol(
            ResearchProtocol.default(),
            candidates=("tp_fast_v1",),
        )
        fixture = (quotes(1)[0], quotes(1)[0])
        with tempfile.TemporaryDirectory(prefix="mtf-historical-plane-identity-") as directory:
            root = Path(directory)
            with self.assertRaises(HistoricalBacktestError):
                run_historical_backtest(
                    fixture,
                    manifest=manifest(root),
                    config=config,
                    output_dir=root / "duplicate",
                    sink=lambda kind, row: None,
                )

            out_of_order = (quotes(2)[1], quotes(2)[0])
            with self.assertRaises(HistoricalBacktestError):
                run_historical_backtest(
                    out_of_order,
                    manifest=manifest(root / "out-of-order"),
                    config=config,
                    output_dir=root / "out-of-order-output",
                    sink=lambda kind, row: None,
                )


if __name__ == "__main__":
    unittest.main()
