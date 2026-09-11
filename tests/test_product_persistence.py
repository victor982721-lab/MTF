from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from urllib.request import urlopen

from mtf_lab.data.capture import CaptureEnvelope, MessageClass
from mtf_lab.ops.persistence import IdempotencyConflict, SQLiteStore
from mtf_lab.ops.query import QueryService
from mtf_lab.ops.ui import create_server


T0 = datetime(2026, 1, 1, tzinfo=UTC)


def cfd_trade(state: str = "PENDING", **changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "product": "FOREX_CFD_LOCAL_PAPER",
        "trade_id": "paper-1",
        "signal_id": "signal-1",
        "instrument": "EUR/USD",
        "direction": "LONG",
        "units": "1000.00",
        "horizon_seconds": "60",
        "state": state,
        "detected_at": "2026-01-01T00:00:00Z",
        "signal_available_at": "2026-01-01T00:00:00Z",
        "decision_at": "2026-01-01T00:00:00Z",
        "entry_target_at": "2026-01-01T00:00:00Z",
        "fill_policy": "first_quote_at_or_after",
        "close_policy": "first_quote_at_or_after",
        "pip_size": "0.0001",
        "price_precision": 5,
        "account_currency": "USD",
        "quote_currency": "USD",
        "quality": "VALID",
        "lineage": {"parent_signal_id": "signal-1"},
    }
    row.update(changes)
    return row


class ProductPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.tmp.name) / "lab.sqlite3")
        self.session_id = self.store.create_session(mode="REPLAY", provider="fixture", instrument="EUR/USD")
        self.analysis_id = self.store.create_analysis(
            self.session_id, dataset_hash="dataset", config_hash="config", variant="paper"
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_atomic_batch_rolls_back_all_writes_and_nested_helpers_use_savepoints(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "abort"):
            with self.store.atomic_batch():
                self.store.save_checkpoint(self.session_id, "runtime", state={"cursor": 1})
                self.store.save_cfd_trade(self.session_id, self.analysis_id, cfd_trade())
                raise RuntimeError("abort")
        self.assertIsNone(self.store.get_checkpoint(self.session_id, "runtime"))
        self.assertEqual(self.store.list_cfd_trades(self.session_id, self.analysis_id), [])

        with self.store.atomic_batch():
            self.store.save_checkpoint(self.session_id, "runtime", state={"cursor": 2})
            with self.assertRaisesRegex(ValueError, "stake"):
                with self.store.transaction():
                    self.store.save_cfd_trade(
                        self.session_id, self.analysis_id, {**cfd_trade(), "trade_id": "inner", "stake": "1000"}
                    )
            self.store.save_cfd_trade(self.session_id, self.analysis_id, {**cfd_trade(), "trade_id": "outer"})
        self.assertIsNotNone(self.store.get_checkpoint(self.session_id, "runtime"))
        self.assertEqual(len(self.store.list_cfd_trades(self.session_id, self.analysis_id)), 1)

    def test_cfd_units_are_exact_and_lifecycle_is_not_binary_outcome(self) -> None:
        pending = cfd_trade(units="1000.00")
        self.assertTrue(self.store.save_cfd_trade(self.session_id, self.analysis_id, pending))
        filled = {**pending, "state": "FILLED", "entry_price": "1.10020", "entry_quote_id": "q-entry"}
        self.assertTrue(self.store.save_cfd_trade(self.session_id, self.analysis_id, filled))
        closed = {
            **filled,
            "state": "CLOSED",
            "close_price": "1.10050",
            "close_quote_id": "q-close",
            "close_market_at": "2026-01-01T00:01:00Z",
            "close_available_at": "2026-01-01T00:01:00Z",
            "gross_pnl_quote": "0.30000",
            "net_pnl": "0.30000",
        }
        self.assertTrue(self.store.save_cfd_trade(self.session_id, self.analysis_id, closed))
        row = self.store.list_cfd_trades(self.session_id, self.analysis_id)[0]
        self.assertEqual(row["units"], "1000.00")
        self.assertEqual(row["net_pnl"], "0.30000")
        self.assertEqual(row["state"], "CLOSED")
        self.assertEqual(row["lifecycle_state"], "CLOSED")
        self.assertEqual(row["economic_status"], "KNOWN")
        self.assertEqual(row["economic_state"], "DETERMINED")
        self.assertTrue(row["close_observed"])
        self.assertNotIn("stake", row)
        with self.assertRaises(IdempotencyConflict):
            self.store.save_cfd_trade(
                self.session_id, self.analysis_id, {**closed, "net_pnl": "0.30001"}
            )
        with self.assertRaises(IdempotencyConflict):
            self.store.save_cfd_trade(self.session_id, self.analysis_id, {**pending, "state": "PENDING"})

    def test_cfd_unknown_economy_remains_distinct_from_lifecycle(self) -> None:
        row = self.store.list_cfd_trades(self.session_id, self.analysis_id)
        self.assertEqual(row, [])
        unknown = cfd_trade(
            state="UNKNOWN",
            trade_id="unknown-1",
            reason="CONVERSION_RATE_MISSING",
            entry_price="1.10020",
            entry_quote_id="q-entry",
            close_price="1.10050",
            close_quote_id="q-close",
            gross_pnl_quote="0.30000",
            net_pnl=None,
        )
        self.store.save_cfd_trade(self.session_id, self.analysis_id, unknown)
        result = self.store.list_cfd_trades(self.session_id, self.analysis_id)[0]
        self.assertEqual(result["state"], "UNKNOWN")
        self.assertEqual(result["economic_status"], "UNKNOWN")
        self.assertEqual(result["economic_state"], "INDETERMINATE")
        self.assertIsNone(result["net_pnl"])

    def test_capture_envelopes_are_idempotent_and_stream_by_causal_cursor(self) -> None:
        envelopes = [
            CaptureEnvelope(T0, T0 + timedelta(seconds=2), T0 + timedelta(seconds=2), 2, 0, MessageClass.SPOT, {"n": 2}),
            CaptureEnvelope(T0, T0 + timedelta(seconds=1), T0 + timedelta(seconds=1), 1, 0, MessageClass.SPOT, {"n": 1}),
            CaptureEnvelope(T0, T0 + timedelta(seconds=1), T0 + timedelta(seconds=1), 3, 1, MessageClass.CONNECTION, {"n": 3}),
        ]
        for envelope in envelopes:
            self.assertTrue(self.store.save_capture_envelope(self.session_id, envelope))
            self.assertFalse(self.store.save_capture_envelope(self.session_id, envelope))
        ordered = list(self.store.iter_capture_envelopes(self.session_id))
        self.assertEqual([(row["available_at"], row["ingest_sequence"]) for row in ordered], [
            ("2026-01-01T00:00:01.000000Z", 1),
            ("2026-01-01T00:00:01.000000Z", 3),
            ("2026-01-01T00:00:02.000000Z", 2),
        ])
        after = list(self.store.iter_capture_envelopes(
            self.session_id, after_cursor=(ordered[0]["available_at"], ordered[0]["ingest_sequence"])
        ))
        self.assertEqual([row["ingest_sequence"] for row in after], [3, 2])
        with self.assertRaises(IdempotencyConflict):
            self.store.save_capture_envelope(
                self.session_id,
                CaptureEnvelope(T0, T0 + timedelta(seconds=2), T0 + timedelta(seconds=2), 2, 0, MessageClass.SPOT, {"n": 999}),
            )
        with self.assertRaises(IdempotencyConflict):
            self.store.save_capture_envelope(
                self.session_id,
                CaptureEnvelope(T0, T0 + timedelta(seconds=3), T0 + timedelta(seconds=3), 2, 1, MessageClass.SPOT, {"n": 2}),
            )

    def test_query_and_read_only_ui_keep_cfd_separate(self) -> None:
        self.store.save_cfd_trade(self.session_id, self.analysis_id, cfd_trade())
        query = QueryService(self.store, max_limit=10)
        page = query.query_cfd_trades(self.session_id, analysis_id=self.analysis_id)
        self.assertEqual(page.total, 1)
        self.assertEqual(page.items[0]["state"], "PENDING")
        self.assertEqual(page.items[0]["economic_status"], "NOT_SETTLED")
        self.assertEqual(query.query_simulations(self.session_id).total, 0)
        self.assertEqual(query.snapshot(self.session_id)["pending_cfd_trades"], 1)

        server = create_server(Path(self.tmp.name) / "lab.sqlite3", host="127.0.0.1", port=0, session_id=self.session_id)
        import threading
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"
            with urlopen(base + f"/api/cfd-trades?session={self.session_id}&analysis_id={self.analysis_id}", timeout=3) as response:
                payload = json.loads(response.read().decode())
            self.assertEqual(payload["items"][0]["state"], "PENDING")
            with urlopen(base + f"/api/simulations?session={self.session_id}", timeout=3) as response:
                binary = json.loads(response.read().decode())
            self.assertEqual(binary["items"], [])
        finally:
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()
            server.store.close()


if __name__ == "__main__":
    unittest.main()
