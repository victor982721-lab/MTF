from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from mtf_lab.ops.persistence import SQLiteStore


class MigrationDeliveryTests(unittest.TestCase):
    @staticmethod
    def _create_v2_fixture(path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta VALUES('schema_version', '2');
            CREATE TABLE sessions(session_id TEXT PRIMARY KEY);
            INSERT INTO sessions VALUES('session-1');
            CREATE TABLE candles(
                candle_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                candle_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT NOT NULL,
                available_ts TEXT,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL,
                closed INTEGER NOT NULL,
                source TEXT NOT NULL,
                price_base TEXT NOT NULL,
                quality TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                provenance_json TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE TABLE checkpoints(
                session_id TEXT NOT NULL,
                checkpoint_name TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                cursor_json TEXT NOT NULL,
                events_processed INTEGER NOT NULL DEFAULT 0,
                last_event_id TEXT,
                state_json TEXT NOT NULL,
                PRIMARY KEY(session_id, checkpoint_name)
            );
            INSERT INTO checkpoints VALUES(
                'session-1', 'runtime', '2025-01-01T00:00:00Z',
                '{"analysis_id":"analysis-a"}', 7, 'event-7',
                '{"processor":{"analysis_id":"analysis-a"}}'
            );
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _create_v3_fixture(path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.executescript(
            """
            CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta VALUES('schema_version', '3');
            CREATE TABLE sessions(session_id TEXT PRIMARY KEY);
            INSERT INTO sessions VALUES('session-1');
            CREATE TABLE candles(
                candle_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                candle_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT NOT NULL,
                available_ts TEXT,
                received_ts TEXT,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL,
                closed INTEGER NOT NULL,
                source TEXT NOT NULL,
                price_base TEXT NOT NULL,
                quality TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                provenance_json TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            INSERT INTO candles(
                session_id,candle_id,instrument,timeframe,start_ts,end_ts,available_ts,received_ts,
                open,high,low,close,volume,closed,source,price_base,quality,revision,provenance_json,payload_json
            ) VALUES(
                'session-1','candle-1','TEST/USD','M1','2025-01-01T00:00:00Z',
                '2025-01-01T00:01:00Z','2025-01-01T00:01:01Z','2025-01-01T00:01:02Z',
                1,2,1,1.5,10,1,'fixture','close','VALID',0,'{}','{}'
            );
            CREATE TABLE checkpoints(
                session_id TEXT NOT NULL,
                checkpoint_name TEXT NOT NULL,
                analysis_id TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                cursor_json TEXT NOT NULL,
                events_processed INTEGER NOT NULL DEFAULT 0,
                last_event_id TEXT,
                state_json TEXT NOT NULL,
                PRIMARY KEY(session_id, checkpoint_name, analysis_id)
            );
            INSERT INTO checkpoints VALUES(
                'session-1','runtime','analysis-a','2025-01-01T00:00:00Z',
                '{"analysis_id":"analysis-a"}',7,'event-7','{}'
            );
            CREATE TABLE signal_analysis_membership(
                membership_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                signal_id TEXT NOT NULL,
                analysis_id TEXT NOT NULL,
                variant TEXT,
                analysis_config_hash TEXT,
                contract_hash TEXT,
                partition TEXT,
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, signal_id, analysis_id)
            );
            """
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _stage_partial_v3(
        path: Path,
        *,
        target_row: tuple[object, ...] | None = None,
        old_index: bool = False,
    ) -> None:
        """Stage the two-table state left by the historical v3 migration."""

        conn = sqlite3.connect(path)
        if old_index:
            conn.execute(
                "CREATE INDEX checkpoints_session_name ON checkpoints(session_id, checkpoint_name, updated_at)"
            )
        conn.execute("ALTER TABLE checkpoints RENAME TO checkpoints_v2_legacy")
        conn.execute(
            """
            CREATE TABLE checkpoints(
                session_id TEXT NOT NULL,
                checkpoint_name TEXT NOT NULL,
                analysis_id TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                cursor_json TEXT NOT NULL,
                events_processed INTEGER NOT NULL DEFAULT 0,
                last_event_id TEXT,
                state_json TEXT NOT NULL,
                PRIMARY KEY(session_id, checkpoint_name, analysis_id)
            )
            """
        )
        if target_row is not None:
            conn.execute("INSERT INTO checkpoints VALUES(?,?,?,?,?,?,?,?)", target_row)
        conn.commit()
        conn.close()

    @staticmethod
    def _new_raw_store(path: Path) -> SQLiteStore:
        store = SQLiteStore.__new__(SQLiteStore)
        store.path = path
        store.read_only = False
        store._lock = threading.RLock()
        store.conn = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
        store.conn.row_factory = sqlite3.Row
        store.conn.execute("PRAGMA foreign_keys=ON")
        store.conn.execute("PRAGMA busy_timeout=30000")
        return store

    def test_v2_resume_with_legacy_and_new_checkpoint_tables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "partial.sqlite3"
            self._create_v2_fixture(path)
            self._stage_partial_v3(
                path,
                target_row=(
                    "session-1",
                    "runtime",
                    "analysis-b",
                    "2025-01-01T00:00:01Z",
                    '{"analysis_id":"analysis-b"}',
                    8,
                    "event-8",
                    "{}",
                ),
            )

            with SQLiteStore(path) as store:
                self.assertEqual(store.schema_version, 4)
                self.assertFalse(store._table_exists("checkpoints_v2_legacy"))
                rows = store.list_checkpoints("session-1", "runtime")
                self.assertEqual({row["analysis_id"] for row in rows}, {"analysis-a", "analysis-b"})
                self.assertEqual(
                    store.get_checkpoint("session-1", "runtime", analysis_id="analysis-a", allow_alternate=False)[
                        "events_processed"
                    ],
                    7,
                )
                candle_columns = {row[1] for row in store.conn.execute("PRAGMA table_info(candles)")}
                self.assertIn("received_ts", candle_columns)
                self.assertTrue(store._table_exists("signal_analysis_membership"))
                self.assertTrue(store._table_exists("cfd_trades"))
                self.assertTrue(store._table_exists("capture_envelopes"))

    def test_v2_resume_identical_collision_is_noop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "identical.sqlite3"
            self._create_v2_fixture(path)
            self._stage_partial_v3(
                path,
                target_row=(
                    "session-1",
                    "runtime",
                    "analysis-a",
                    "2025-01-01T00:00:00Z",
                    '{"analysis_id":"analysis-a"}',
                    7,
                    "event-7",
                    '{"processor":{"analysis_id":"analysis-a"}}',
                ),
            )

            with SQLiteStore(path) as store:
                row = store.conn.execute(
                    "SELECT session_id,checkpoint_name,analysis_id,updated_at,cursor_json,"
                    "events_processed,last_event_id,state_json FROM checkpoints"
                ).fetchone()
                self.assertEqual(
                    tuple(row),
                    (
                        "session-1",
                        "runtime",
                        "analysis-a",
                        "2025-01-01T00:00:00Z",
                        '{"analysis_id":"analysis-a"}',
                        7,
                        "event-7",
                        '{"processor":{"analysis_id":"analysis-a"}}',
                    ),
                )
                self.assertFalse(store._table_exists("checkpoints_v2_legacy"))

    def test_v2_resume_conflict_fails_closed_and_preserves_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "conflict.sqlite3"
            self._create_v2_fixture(path)
            self._stage_partial_v3(
                path,
                target_row=(
                    "session-1",
                    "runtime",
                    "analysis-a",
                    "2025-01-01T00:00:00Z",
                    '{"analysis_id":"analysis-a"}',
                    99,
                    "event-99",
                    '{"processor":{"analysis_id":"analysis-a"}}',
                ),
            )
            store = self._new_raw_store(path)
            try:
                with self.assertRaisesRegex(RuntimeError, "conflicto de checkpoint"):
                    store._migrate_v3()
                table_names = {
                    row[0]
                    for row in store.conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'checkpoints%'"
                    )
                }
                self.assertEqual(table_names, {"checkpoints", "checkpoints_v2_legacy"})
                legacy = store.conn.execute(
                    "SELECT updated_at,cursor_json,events_processed,last_event_id,state_json FROM checkpoints_v2_legacy"
                ).fetchone()
                target = store.conn.execute(
                    "SELECT updated_at,cursor_json,events_processed,last_event_id,state_json FROM checkpoints"
                ).fetchone()
                self.assertEqual(
                    tuple(legacy),
                    tuple(
                        (
                            "2025-01-01T00:00:00Z",
                            '{"analysis_id":"analysis-a"}',
                            7,
                            "event-7",
                            '{"processor":{"analysis_id":"analysis-a"}}',
                        )
                    ),
                )
                self.assertEqual(tuple(target)[2], 99)

                # Repair the isolated fixture to the source value and prove
                # that the failed migration left the connection retryable.
                store.conn.execute(
                    "UPDATE checkpoints SET events_processed=?,last_event_id=? "
                    "WHERE session_id=? AND checkpoint_name=? AND analysis_id=?",
                    (7, "event-7", "session-1", "runtime", "analysis-a"),
                )
                store.conn.commit()
                store._migrate_v3()
            finally:
                store.close()

            with SQLiteStore(path) as reopened:
                self.assertEqual(reopened.schema_version, 4)
                checkpoint = reopened.get_checkpoint(
                    "session-1", "runtime", analysis_id="analysis-a", allow_alternate=False
                )
                self.assertEqual(checkpoint["events_processed"], 7)
                self.assertFalse(reopened._table_exists("checkpoints_v2_legacy"))

    def test_v2_migration_recreates_lookup_index_after_legacy_drop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "legacy-index.sqlite3"
            self._create_v2_fixture(path)
            self._stage_partial_v3(path, old_index=True)

            with SQLiteStore(path) as store:
                index_names = {row[1] for row in store.conn.execute("PRAGMA index_list('checkpoints')")}
                self.assertIn("checkpoints_session_name", index_names)
                index_columns = [row[2] for row in store.conn.execute("PRAGMA index_info('checkpoints_session_name')")]
                self.assertEqual(index_columns, ["session_id", "checkpoint_name", "updated_at"])

    def test_v3_migration_rolls_back_on_failure_and_can_retry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rollback.sqlite3"
            self._create_v2_fixture(path)
            store = self._new_raw_store(path)

            def deny_legacy_drop(action: int, arg1: str | None, *_: object) -> int:
                if action == sqlite3.SQLITE_DROP_TABLE and arg1 == "checkpoints_v2_legacy":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            store.conn.set_authorizer(deny_legacy_drop)
            try:
                with self.assertRaises(sqlite3.DatabaseError):
                    store._migrate_v3()
                table_names = {
                    row[0]
                    for row in store.conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'checkpoints%'"
                    )
                }
                self.assertEqual(table_names, {"checkpoints"})
                columns = {row[1] for row in store.conn.execute("PRAGMA table_info(checkpoints)")}
                self.assertNotIn("analysis_id", columns)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0], 1)

                store.conn.set_authorizer(None)
                store._migrate_v3()
                self.assertFalse(store._table_exists("checkpoints_v2_legacy"))
                self.assertIn(
                    "analysis_id",
                    {row[1] for row in store.conn.execute("PRAGMA table_info(checkpoints)")},
                )
            finally:
                store.close()

            # The marker was intentionally still v2 when the focal method was
            # called.  Reopening must resume v3 and complete the v4 extension.
            with SQLiteStore(path) as reopened:
                self.assertEqual(reopened.schema_version, 4)
                checkpoint = reopened.get_checkpoint(
                    "session-1", "runtime", analysis_id="analysis-a", allow_alternate=False
                )
                self.assertIsNotNone(checkpoint)
                self.assertEqual(checkpoint["events_processed"], 7)

    def test_v3_to_v4_reopen_preserves_existing_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "v3.sqlite3"
            self._create_v3_fixture(path)
            with SQLiteStore(path) as store:
                self.assertEqual(store.schema_version, 4)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0], 1)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0], 1)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM signal_analysis_membership").fetchone()[0], 0)
                self.assertTrue(store._table_exists("cfd_trades"))
                self.assertTrue(store._table_exists("capture_envelopes"))

            with SQLiteStore(path) as reopened:
                self.assertEqual(reopened.schema_version, 4)
                self.assertEqual(reopened.conn.execute("SELECT COUNT(*) FROM candles").fetchone()[0], 1)
                self.assertEqual(reopened.conn.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0], 1)
                self.assertEqual(
                    reopened.get_checkpoint("session-1", "runtime", analysis_id="analysis-a", allow_alternate=False)[
                        "events_processed"
                    ],
                    7,
                )

    def test_future_schema_is_rejected_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "future.sqlite3"
            conn = sqlite3.connect(path)
            conn.execute("CREATE TABLE schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            conn.execute("INSERT INTO schema_meta VALUES('schema_version', '99')")
            conn.commit()
            conn.close()
            with self.assertRaisesRegex(RuntimeError, "newer than supported"):
                SQLiteStore(path)


if __name__ == "__main__":
    unittest.main()
