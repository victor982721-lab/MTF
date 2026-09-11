"""Integración offline cTrader -> runtime -> CFD PAPER."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from mtf_lab.configuration import load_config
from mtf_lab.core import IndicatorConfig
from mtf_lab.data.ctrader import CTraderInstrumentSpec
from mtf_lab.ops.cfd_simulation import CFDConfig, TradeState
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.query import QueryService
from mtf_lab.ops.ctrader_pipeline import (
    PAPER_PRODUCT,
    PAPER_SESSION_VERSION,
    CTraderPipeline,
    normalize_ctrader_capture,
    signal_to_cfd_signal,
    spot_event_to_cfd_quote,
    synthetic_ctrader_capture,
)


UTC = UTC
BASE = datetime(2026, 1, 1, tzinfo=UTC)


def pipeline_config():
    base = load_config("config/fixture_cfd.toml")
    indicators = IndicatorConfig(ema_fast=2, ema_slow=3, rsi_period=2, atr_period=2)
    strategy = replace(
        base.strategy,
        indicators=indicators,
        context_lookback=1,
        preparation_lookback=3,
        preparation_ttl_bars=2,
        max_distance_atr=2.0,
    )
    # cTrader spot events are the detector's explicit MID stream; native
    # trendbars remain captured evidence and are used by the TRADED variant.
    cfd = dict(base.cfd)
    cfd["max_quote_age_seconds"] = 90
    return replace(base, mode="REPLAY", price_base="mid", indicators=indicators, strategy=strategy, cfd=cfd)


def receipt(index: int, raw: dict) -> datetime:
    timestamp = datetime.fromtimestamp(float(raw["timestamp"]) / 1000.0, UTC)
    return timestamp + timedelta(seconds=1)


class CTraderPipelineTests(unittest.TestCase):
    def test_capture_normalizes_spot_and_native_bars_with_stable_provenance(self) -> None:
        capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)
        self.assertEqual(len(capture.quote_events), 190)
        self.assertEqual(len(capture.bars), 190)
        self.assertEqual(capture.snapshot_count, 1)
        self.assertEqual(capture.issues, ())
        self.assertEqual(capture.quote_events[0].bid, 1.0999)
        self.assertEqual(capture.quote_events[0].ask, 1.1001)
        self.assertEqual(capture.bars[0].resolution, "M1")
        self.assertTrue(capture.bars[0].metadata["provenance"]["provider"].startswith("ctrader"))
        self.assertTrue(capture.bars[0].metadata["no_tick_interpolation"])
        self.assertEqual(capture.provenance["mode"], "REPLAY")
        self.assertTrue(capture.provenance["synthetic"])
        self.assertEqual(capture.provenance["source_mode"], "SYNTHETIC_FIXTURE")

        reversed_capture = normalize_ctrader_capture(
            reversed(capture.envelopes),
            spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99),
            quote_basis="mid",
            mode="REPLAY",
            received_at=receipt,
        )
        self.assertEqual(capture.capture_hash, reversed_capture.capture_hash)
        self.assertEqual(capture.capture_id, reversed_capture.capture_id)
        self.assertEqual(
            [event.source_event_id for event in capture.quote_events],
            [event.source_event_id for event in reversed_capture.quote_events],
        )

    def test_replay_retains_partial_bid_ask_state_without_interpolation(self) -> None:
        payloads = (
            {"symbolId": 99, "timestamp": int(BASE.timestamp() * 1000), "bid": 110000, "sequence": 1},
            {"symbolId": 99, "timestamp": int((BASE + timedelta(seconds=2)).timestamp() * 1000), "ask": 110020, "sequence": 2},
        )
        capture = normalize_ctrader_capture(
            payloads,
            spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99),
            quote_basis="mid",
            received_at=receipt,
        )
        self.assertEqual(len(capture.quote_events), 1)
        partial = capture.quote_events[0]
        self.assertEqual((partial.bid, partial.ask), (1.1, 1.1002))
        self.assertTrue(partial.metadata["partial_update"])
        self.assertEqual(partial.metadata["bid_source_timestamp"], BASE.isoformat().replace("+00:00", "Z"))
        self.assertTrue(capture.issues, "the earlier incomplete leg observation remains auditable")

    def test_real_runtime_warmup_emits_authentic_signals_and_adapts_cfd(self) -> None:
        capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)
        config = pipeline_config()
        with TemporaryDirectory() as tmp:
            with SQLiteStore(Path(tmp) / "pipeline.sqlite3") as store:
                result = CTraderPipeline(
                    store,
                    config,
                    spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99),
                ).run(capture, session_id="ctrader-pipeline", capture_complete=True)
                self.assertGreaterEqual(len(result.signals), 1)
                self.assertEqual(len(result.signals), len(result.cfd_signals))
                self.assertTrue(all(signal.mode.value == "REPLAY" for signal in result.signals))
                self.assertTrue(all(signal.metadata["source"] == "RuntimeCoordinator" for signal in result.cfd_signals))
                self.assertTrue(all(trade.state is TradeState.CLOSED for trade in result.trades))
                self.assertTrue(all(signal.signal_id == cfd.signal_id for signal, cfd in zip(result.signals, result.cfd_signals)))
                self.assertEqual(result.analysis_basis, "mid")
                self.assertEqual(result.snapshot["schema_version"], PAPER_SESSION_VERSION)
                self.assertEqual(result.snapshot["product"], PAPER_PRODUCT)
                self.assertEqual(result.snapshot["paper"]["product"], PAPER_PRODUCT)
                self.assertEqual(result.snapshot["processor"]["warmup_pending"]["M1"], 0)
                self.assertEqual(result.snapshot["processor"]["warmup_pending"]["M5"], 0)
                self.assertEqual(result.snapshot["processor"]["warmup_pending"]["M15"], 0)
                self.assertEqual(result.runtime_result.rejected_records, 0)

    def test_native_trendbars_can_be_the_runtime_analysis_stream(self) -> None:
        capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)
        traded = replace(pipeline_config(), price_base="native")
        with TemporaryDirectory() as tmp:
            with SQLiteStore(Path(tmp) / "pipeline.sqlite3") as store:
                result = CTraderPipeline(
                    store,
                    traded,
                    spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99),
                ).run(capture, session_id="native-bars")
                self.assertTrue(result.analysis_records)
                self.assertTrue(all(record.__class__.__name__ == "Bar" for record in result.analysis_records))
                self.assertGreaterEqual(len(result.signals), 1)
                self.assertEqual(result.analysis_basis, "native")
                self.assertEqual(store.list_simulations(result.session_id), [])
                self.assertTrue(store.list_cfd_trades(result.session_id, result.paper_analysis_id))

    def test_paper_product_and_snapshot_are_visible_to_sqlite_and_query(self) -> None:
        capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)
        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "pipeline.sqlite3"
            with SQLiteStore(db) as store:
                result = CTraderPipeline(store, pipeline_config(), spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)).run(capture, session_id="paper-visible")
                paper_rows = store.list_cfd_trades(result.session_id, result.paper_analysis_id)
                self.assertEqual(store.list_simulations(result.session_id), [])
                self.assertEqual(len(paper_rows), len(result.trades))
                self.assertGreaterEqual(len(paper_rows), 1)
                self.assertTrue(all(row["product"] == PAPER_PRODUCT for row in paper_rows))
                self.assertTrue(all(isinstance(row["units"], str) for row in paper_rows))
                self.assertTrue(all("executor" not in str(row["payload"]).lower() for row in paper_rows))
                self.assertTrue(all(row["state"] == "CLOSED" for row in paper_rows))
                self.assertEqual(store.schema_version, 4)
                checkpoint = store.get_checkpoint(result.session_id, "pipeline", analysis_id=result.paper_analysis_id, allow_alternate=False)
                self.assertIsNotNone(checkpoint)
                assert checkpoint is not None
                self.assertEqual(checkpoint["state"]["schema_version"], PAPER_SESSION_VERSION)
                self.assertEqual(checkpoint["state"]["paper"]["product"], PAPER_PRODUCT)
                query_snapshot = QueryService(store).snapshot(result.session_id)
                self.assertEqual(query_snapshot["schema_version"], 4)
                self.assertGreaterEqual(query_snapshot["counts"]["cfd_trades"], len(paper_rows))

    def test_replay_order_and_second_run_are_idempotent(self) -> None:
        capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)
        config = pipeline_config()
        with TemporaryDirectory() as first_tmp, TemporaryDirectory() as second_tmp:
            with SQLiteStore(Path(first_tmp) / "first.sqlite3") as first_store, SQLiteStore(Path(second_tmp) / "second.sqlite3") as second_store:
                first = CTraderPipeline(first_store, config, spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)).run(capture, session_id="first")
                second = CTraderPipeline(second_store, config, spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)).run(
                    list(reversed(capture.envelopes)), session_id="second", received_at=receipt
                )
                self.assertEqual(first.capture.capture_hash, second.capture.capture_hash)
                self.assertEqual([item.signal_id for item in first.signals], [item.signal_id for item in second.signals])
                self.assertEqual([item.to_dict() for item in first.trades], [item.to_dict() for item in second.trades])
                self.assertEqual(first.snapshot["paper"]["trades"], second.snapshot["paper"]["trades"])
                self.assertEqual(first.snapshot_hash, second.snapshot_hash)

            with SQLiteStore(Path(first_tmp) / "first.sqlite3") as rerun_store:
                rerun = CTraderPipeline(rerun_store, config, spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)).run(capture, session_id="first")
                self.assertEqual([item.signal_id for item in rerun.signals], [item.signal_id for item in first.signals])
                rows = rerun_store.list_cfd_trades("first", rerun.paper_analysis_id)
                self.assertEqual(len(rows), len(first.trades))

    def test_only_explicit_bid_ask_quotes_and_detector_signals_cross_boundary(self) -> None:
        capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)
        quote = spot_event_to_cfd_quote(capture.quote_events[1], capture_hash=capture.capture_hash)
        self.assertEqual(quote.quality, "SYNTHETIC")
        self.assertEqual(quote.metadata["capture_hash"], capture.capture_hash)
        self.assertEqual(quote.bid + quote.spread, quote.ask)
        one_sided = spot_event_to_cfd_quote(replace(capture.quote_events[1], bid=None))
        self.assertIsNone(one_sided.bid)
        with TemporaryDirectory() as tmp:
            with SQLiteStore(Path(tmp) / "pipeline.sqlite3") as store:
                result = CTraderPipeline(store, pipeline_config(), spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)).run(capture, session_id="boundary")
                self.assertGreaterEqual(len(result.signals), 1)
                converted = signal_to_cfd_signal(result.signals[0], capture_hash=capture.capture_hash)
                self.assertEqual(converted.signal_id, result.signals[0].signal_id)
                self.assertEqual(converted.metadata["episode_id"], result.signals[0].episode_id)
                # The fixture's first quote is a snapshot and must not be a CFD fill.
                snapshot_quote = spot_event_to_cfd_quote(capture.quote_events[0], capture_hash=capture.capture_hash)
                self.assertEqual(snapshot_quote.quality, "SNAPSHOT")


if __name__ == "__main__":
    unittest.main()
