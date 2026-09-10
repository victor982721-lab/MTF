from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.core import Candle, DataQuality, OperationMode, aggregate_events, MarketEvent, assess_freshness
from mtf_lab.data import SyntheticGenerator
from mtf_lab.data.kraken import normalize_pair
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.simulation import EvaluationSpec, Outcome, VirtualContractSimulator, break_even_probability
from mtf_lab.pipeline import build_streams, run_synthetic_pipeline


class PipelineAcceptanceTests(unittest.TestCase):
    def test_synthetic_vertical_route_and_reproducible_replay(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "lab.sqlite3"
            result = run_synthetic_pipeline(db, seed=42, periods_each=600)
            self.assertEqual(result.dataset.provenance.mode, "SYNTHETIC")
            self.assertEqual({k: len(v) for k, v in result.streams.items()}, {"M1": 2400, "M5": 480, "M15": 160})
            self.assertGreater(len(result.strategy.signals), 0)
            self.assertTrue(result.report and result.report["mode_label"] == "SINTETICO")
            with SQLiteStore(db, read_only=True) as store:
                sid = result.session_id
                before = store.status(sid)["counts"]
                self.assertEqual(before["events"], 2400)
                self.assertEqual(before["signals"], len(result.strategy.signals))
                self.assertGreater(before["simulations"], 0)

    def test_half_open_boundaries_and_no_gap_fill(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=UTC)
        def e(sec: int, price: float) -> MarketEvent:
            t = base + timedelta(seconds=sec)
            return MarketEvent("T", t, price=price, received_at=t, available_at=t, mode=OperationMode.REPLAY, source_event_id=str(sec), quality=DataQuality.good())
        bars = aggregate_events([e(0, 10), e(59, 11), e(120, 13)], "M1")
        self.assertEqual([b.start for b in bars], [base, base + timedelta(minutes=2)])
        self.assertEqual(bars[0].close, 11)
        self.assertEqual(bars[1].open, 13)

    def test_virtual_outcomes_and_break_even(self) -> None:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        spec = EvaluationSpec(horizons_seconds=(60,), entry_latency_seconds=1, max_price_age_seconds=5, requested_base_price="close")
        sim = VirtualContractSimulator(spec=spec)
        signal = {"signal_id": "s", "detected_ts": start.isoformat(), "direction": "UP"}
        def p(sec: int, value: float) -> dict:
            return {"timestamp": (start + timedelta(seconds=sec)).isoformat(), "price": value, "price_base": "close", "closed": True}
        self.assertEqual(sim.evaluate(signal, [p(2, 100), p(62, 101)]).outcome, Outcome.WIN)
        self.assertEqual(sim.evaluate(signal, [p(2, 100), p(62, 99)]).outcome, Outcome.LOSS)
        self.assertEqual(sim.evaluate(signal, [p(2, 100), p(62, 100)]).outcome, Outcome.TIE)
        self.assertEqual(sim.evaluate(signal, [p(2, 100)]).outcome, Outcome.INDETERMINATE)
        self.assertAlmostEqual(break_even_probability(), 1 / 1.8)

    def test_feed_and_candle_ages_are_separate(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=UTC)
        fresh = assess_freshness(now=now, last_received_at=now - timedelta(seconds=10), last_closed_end=now - timedelta(minutes=10), max_feed_age_seconds=90, max_closed_candle_age_seconds=900)
        stale = assess_freshness(now=now, last_received_at=now - timedelta(seconds=100), last_closed_end=now - timedelta(minutes=10), max_feed_age_seconds=90, max_closed_candle_age_seconds=900)
        self.assertEqual(fresh.quality.status, "valid")
        self.assertTrue(stale.quality.has("stale"))
        self.assertFalse(stale.quality.has("late"))

    def test_config_and_pair_aliases_are_explicit(self) -> None:
        from mtf_lab.core import StrategyConfig
        with self.assertRaises(ValueError):
            StrategyConfig.from_mapping({"unknown_typo": 1})
        self.assertEqual(normalize_pair("XXBTZUSD"), "BTC/USD")


if __name__ == "__main__":
    unittest.main()
