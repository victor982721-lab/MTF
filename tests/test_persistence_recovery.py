from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from mtf_lab.ops.persistence import IdempotencyConflict, SQLiteStore


class PersistenceRecoveryTests(unittest.TestCase):
    def _store(self, tmp: str) -> tuple[SQLiteStore, str]:
        store = SQLiteStore(Path(tmp) / "lab.sqlite3")
        sid = store.create_session(mode="REPLAY", provider="fixture", instrument="TEST/USD")
        return store, sid

    def test_checkpoint_namespace_exact_and_alternate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self._store(tmp)[0] as store:
                sid = store.sessions()[0]["session_id"]
                analysis_a = store.create_analysis(sid, dataset_hash="d", config_hash="a", variant="a")
                analysis_b = store.create_analysis(sid, dataset_hash="d", config_hash="b", variant="b")
                store.save_checkpoint(sid, "runtime", analysis_id=analysis_a, cursor={"analysis_id": analysis_a}, events_processed=1, state={"owner": "a"})
                store.save_checkpoint(sid, "runtime", analysis_id=analysis_b, cursor={"analysis_id": analysis_b}, events_processed=2, state={"owner": "b"})
                exact = store.get_checkpoint(sid, "runtime", analysis_id=analysis_a, allow_alternate=False)
                self.assertEqual(exact["analysis_id"], analysis_a)
                alternate = store.get_checkpoint(sid, "runtime", analysis_id="missing", allow_alternate=True)
                self.assertTrue(alternate["is_alternate"])
                self.assertEqual(alternate["requested_analysis_id"], "missing")
                self.assertEqual(alternate["analysis_id"], analysis_b)
                self.assertIsNone(store.get_checkpoint(sid, "runtime", analysis_id="missing", allow_alternate=False))
                self.assertEqual(len(store.list_checkpoints(sid, "runtime")), 2)

    def test_pending_simulation_can_become_terminal_but_terminal_is_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self._store(tmp)[0] as store:
                sid = store.sessions()[0]["session_id"]
                base = {
                    "simulation_id": "sim-1", "signal_id": "sig-1", "simulation_type": "VIRTUAL_CONTRACT",
                    "horizon_seconds": 60, "direction": "UP", "detected_ts": "2025-01-01T00:00:00Z",
                    "expiry_ts": "2025-01-01T00:01:00Z", "stake": 1, "price_base": "close",
                    "quality": "VALID", "resolution": "M1", "assumptions": {},
                }
                self.assertTrue(store.save_simulation(sid, {**base, "outcome": "PENDING", "net_result": None}))
                self.assertTrue(store.update_simulation(sid, {**base, "outcome": "WIN", "entry_price": 1, "final_price": 2, "net_result": .8}))
                self.assertFalse(store.update_simulation(sid, {**base, "outcome": "WIN", "entry_price": 1, "final_price": 2, "net_result": .8}))
                with self.assertRaises(IdempotencyConflict):
                    store.update_simulation(sid, {**base, "outcome": "LOSS", "entry_price": 1, "final_price": 2, "net_result": -1})
                with self.assertRaises(IdempotencyConflict):
                    store.update_simulation(sid, {**base, "outcome": "PENDING", "net_result": None})

    def test_same_signal_can_have_memberships_in_two_analyses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self._store(tmp)[0] as store:
                sid = store.sessions()[0]["session_id"]
                analysis_a = store.create_analysis(sid, dataset_hash="d", config_hash="a", variant="a")
                analysis_b = store.create_analysis(sid, dataset_hash="d", config_hash="b", variant="b")
                signal = {"signal_id": "shared", "detected_ts": "2025-01-01T00:00:00Z", "instrument": "TEST/USD", "direction": "UP", "status": "VALID"}
                self.assertTrue(store.save_signal(sid, {**signal, "values": {"variant": "a"}}, analysis_id=analysis_a, variant="a"))
                self.assertTrue(store.save_signal(sid, {**signal, "values": {"variant": "b"}}, analysis_id=analysis_b, variant="b"))
                self.assertFalse(store.save_signal(sid, {**signal, "values": {"variant": "a"}}, analysis_id=analysis_a, variant="a"))
                memberships = store.list_signal_memberships(sid, signal_id="shared")
                self.assertEqual({item["analysis_id"] for item in memberships}, {analysis_a, analysis_b})
                self.assertEqual(store.status(sid)["counts"]["signal_memberships"], 2)
                with self.assertRaises(IdempotencyConflict):
                    store.save_signal(sid, {**signal, "values": {"variant": "a", "changed": True}}, analysis_id=analysis_a, variant="a")

    def test_event_and_candle_receipt_are_separate_from_availability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self._store(tmp)[0] as store:
                sid = store.sessions()[0]["session_id"]
                self.assertTrue(store.save_event(sid, {"event_id": "e1", "instrument": "TEST/USD", "event_time": "2025-01-01T00:00:00Z", "received_at": "2025-01-01T00:00:02Z", "available_at": "2025-01-01T00:00:03Z", "price": 1, "price_basis": "traded"}))
                event = store.list_events(sid)[0]
                self.assertEqual(event["event_ts"], "2025-01-01T00:00:00.000000Z")
                self.assertEqual(event["received_ts"], "2025-01-01T00:00:02.000000Z")
                self.assertEqual(event["available_ts"], "2025-01-01T00:00:03.000000Z")
                self.assertTrue(store.save_candle(sid, {"candle_id": "c1", "instrument": "TEST/USD", "timeframe": "M1", "start_ts": "2025-01-01T00:00:00Z", "end_ts": "2025-01-01T00:01:00Z", "received_ts": "2025-01-01T00:01:02Z", "available_ts": "2025-01-01T00:01:03Z", "open": 1, "high": 2, "low": 1, "close": 1.5, "closed": True, "source": "fixture", "price_base": "close", "quality": "VALID"}))
                candle = store.list_candles(sid)[0]
                self.assertEqual(candle["received_ts"], "2025-01-01T00:01:02.000000Z")
                self.assertEqual(candle["available_ts"], "2025-01-01T00:01:03.000000Z")

    def test_v2_checkpoint_migrates_to_v3_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy.sqlite3"
            conn = sqlite3.connect(path)
            conn.executescript("""
                CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO schema_meta VALUES('schema_version','2');
                CREATE TABLE sessions(session_id TEXT PRIMARY KEY);
                INSERT INTO sessions VALUES('s');
                CREATE TABLE candles(session_id TEXT, candle_id TEXT, instrument TEXT, timeframe TEXT, start_ts TEXT, end_ts TEXT, available_ts TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL, closed INTEGER, source TEXT, price_base TEXT, quality TEXT, revision INTEGER, provenance_json TEXT, payload_json TEXT);
                CREATE TABLE checkpoints(session_id TEXT, checkpoint_name TEXT, updated_at TEXT, cursor_json TEXT, events_processed INTEGER, last_event_id TEXT, state_json TEXT, PRIMARY KEY(session_id,checkpoint_name));
                INSERT INTO checkpoints VALUES('s','runtime','2025-01-01T00:00:00Z','{"analysis_id":"old"}',1,'e','{}');
            """)
            conn.commit(); conn.close()
            with SQLiteStore(path) as store:
                self.assertEqual(store.schema_version, 3)
                checkpoint = store.get_checkpoint("s", "runtime", analysis_id="old", allow_alternate=False)
                self.assertEqual(checkpoint["analysis_id"], "old")
                self.assertIn("received_ts", [row[1] for row in store.conn.execute("PRAGMA table_info(candles)")])


if __name__ == "__main__":
    unittest.main()
