from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.ops.backtest import BacktestRunner, VariantSpec
from mtf_lab.ops.demo import run_demo
from mtf_lab.ops.importer import ImportValidationError, LocalImporter
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.reporting import ReportBuilder
from mtf_lab.ops.simulation import EvaluationSpec, Outcome, VirtualContractSimulator, break_even_probability


def point(ts: datetime, price: float, ordinal: int, *, available_at: datetime | None = None) -> dict:
    return {"timestamp": ts.isoformat().replace("+00:00", "Z"), "price": price, "price_base": "close", "closed": True, "available_at": (available_at or ts).isoformat().replace("+00:00", "Z"), "source_ordinal": ordinal, "resolution": "M1"}


class OpsTests(unittest.TestCase):
    def test_sqlite_idempotency_revisions_and_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "lab.sqlite3"
            with SQLiteStore(db) as store:
                sid = store.create_session(mode="REPLAY", provider="fixture", instrument="TEST/USD", config={"x": 1})
                candle = {"candle_id": "c1", "instrument": "TEST/USD", "timeframe": "M1", "start_ts": "2025-01-01T00:00:00Z", "end_ts": "2025-01-01T00:01:00Z", "open": 1, "high": 2, "low": 1, "close": 1.5, "closed": True, "source": "fixture", "price_base": "close", "quality": "VALID"}
                self.assertTrue(store.save_candle(sid, candle))
                self.assertFalse(store.save_candle(sid, candle))
                revised = dict(candle, candle_id="c1-r1", revision=1, close=1.7)
                self.assertTrue(store.save_candle(sid, revised))
                store.save_checkpoint(sid, "replay", cursor={"row": 2}, events_processed=2, last_event_id="e2", state={"warm": False})
                self.assertEqual(len(store.list_candles(sid)), 2)
                self.assertEqual(store.get_checkpoint(sid, "replay")["cursor"]["row"], 2)
                # The lineage/analysis migration is applied on first open.
                self.assertEqual(store.schema_version, 2)

    def test_contract_outcomes_and_break_even(self) -> None:
        start = datetime(2025, 1, 1, tzinfo=UTC)
        spec = EvaluationSpec(horizons_seconds=(60,), entry_latency_seconds=1, max_price_age_seconds=5, requested_base_price="close")
        sim = VirtualContractSimulator(spec=spec)
        signal = {"signal_id": "s1", "detected_ts": start.isoformat(), "direction": "UP"}
        points = [point(start + timedelta(seconds=2), 100, 0), point(start + timedelta(seconds=62), 101, 1)]
        self.assertEqual(sim.evaluate(signal, points).outcome, Outcome.WIN)
        points[1]["price"] = 99
        self.assertEqual(sim.evaluate(signal, points).outcome, Outcome.LOSS)
        points[1]["price"] = 100
        self.assertEqual(sim.evaluate(signal, points).outcome, Outcome.TIE)
        self.assertEqual(sim.evaluate(signal, points[:1]).outcome, Outcome.INDETERMINATE)
        self.assertAlmostEqual(break_even_probability(), 1 / 1.8)

    def test_backtest_partition_excludes_crossing_exploration(self) -> None:
        start = datetime(2025, 1, 1, tzinfo=UTC)
        signals = [{"signal_id": "a", "detected_ts": (start + timedelta(seconds=60)).isoformat(), "direction": "UP"}, {"signal_id": "b", "detected_ts": (start + timedelta(seconds=120)).isoformat(), "direction": "UP"}]
        points = [point(start + timedelta(seconds=x), 100 + x / 60, x) for x in range(0, 600, 60)]
        runner = BacktestRunner(spec=EvaluationSpec(horizons_seconds=(60,), entry_latency_seconds=1, max_price_age_seconds=120))
        result = runner.partitioned_run(signals, points, boundary=start + timedelta(seconds=120))
        self.assertEqual(result["exploration"][0].signal_count, 0)  # horizon from signal a crosses boundary
        self.assertEqual(result["evaluation"][0].signal_count, 1)

    def test_import_rejects_naive_and_bad_ohlc(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.csv"
            path.write_text("timestamp,open,high,low,close\n2025-01-01 00:00:00,2,1,0,1\n", encoding="utf-8")
            importer = LocalImporter(instrument="TEST/USD", timeframe="M1", price_base="close")
            with self.assertRaises(ImportValidationError):
                importer.read(path)

    def test_demo_is_offline_and_generates_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_demo(Path(tmp) / "demo.sqlite3", seed=3, minutes=120)
            self.assertTrue(result["dataset"]["synthetic"])
            self.assertGreater(result["dataset"]["candles"], 0)
            self.assertTrue(Path(result["report_path"]).exists())
            with SQLiteStore(result["db_path"], read_only=True) as store:
                sid = result["session_id"]
                self.assertEqual(store.status(sid)["mode"], "SYNTHETIC")
                self.assertGreater(len(store.list_simulations(sid)), 0)
                self.assertIn("SINTETICO", Path(result["report_path"]).read_text(encoding="utf-8") or "") is False


if __name__ == "__main__":
    unittest.main()
