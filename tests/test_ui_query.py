from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.request import urlopen

from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.query import QueryService
from mtf_lab.ops.reporting import ReportBuilder
from mtf_lab.ops.ui import create_server


class QueryUITests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "lab.sqlite3"
        self.store = SQLiteStore(self.db)
        self.sid = self.store.create_session(
            mode="SYNTHETIC",
            provider="fixture",
            instrument="TEST/USD",
            config={"strategy": "trend_pullback_v1"},
            metadata={"partition": "evaluation"},
        )

        def candle(
            cid: str,
            start: str,
            end: str,
            close: float,
            *,
            revision: int = 0,
            closed: bool = True,
            indicators: dict | None = None,
        ) -> dict:
            return {
                "candle_id": cid,
                "instrument": "TEST/USD",
                "timeframe": "M1",
                "start_ts": start,
                "end_ts": end,
                "open": close,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "closed": closed,
                "revision": revision,
                "source": "fixture",
                "price_base": "close",
                "quality": "SYNTHETIC_VALIDATED",
                "provenance": {"synthetic": True, "indicators": indicators or {}},
            }

        self.store.save_candle(
            self.sid,
            candle(
                "c0",
                "2025-01-01T00:00:00Z",
                "2025-01-01T00:01:00Z",
                100,
                indicators={"ema_fast": 99, "ema_slow": 98, "rsi": 55, "atr": 1},
            ),
        )
        # Deliberate missing [00:01,00:02) interval.
        self.store.save_candle(self.sid, candle("c2", "2025-01-01T00:02:00Z", "2025-01-01T00:03:00Z", 101, revision=0))
        self.store.save_candle(
            self.sid, candle("c2-r1", "2025-01-01T00:02:00Z", "2025-01-01T00:03:00Z", 102, revision=1)
        )
        self.store.save_candle(
            self.sid, candle("c3-open", "2025-01-01T00:03:00Z", "2025-01-01T00:04:00Z", 102, closed=False)
        )
        decision_payload = {
            "mode": "SYNTHETIC",
            "conditions": [
                {
                    "name": "context",
                    "state": "fulfilled",
                    "observed": "UP",
                    "expected": "UP",
                    "reason": "",
                    "mandatory": True,
                },
                {
                    "name": "rsi",
                    "state": "failed",
                    "observed": 48,
                    "expected": ">50",
                    "reason": "threshold",
                    "mandatory": True,
                },
            ],
        }
        self.store.save_decision(
            self.sid,
            {
                "decision_id": "d1",
                "observed_ts": "2025-01-01T00:03:00Z",
                "kind": "trigger",
                "status": "DISCARDED",
                "payload": decision_payload,
            },
        )
        self.store.save_signal(
            self.sid,
            {
                "signal_id": "sig1",
                "detected_ts": "2025-01-01T00:03:00Z",
                "instrument": "TEST/USD",
                "direction": "UP",
                "episode_id": "ep1",
                "status": "VALID",
                "strategy": "trend_pullback_v1",
            },
        )
        self.store.save_simulation(
            self.sid,
            {
                "simulation_id": "variant_a:sig1:60",
                "signal_id": "sig1",
                "simulation_type": "VIRTUAL_CONTRACT",
                "horizon_seconds": 60,
                "direction": "UP",
                "detected_ts": "2025-01-01T00:03:00Z",
                "entry_ts": "2025-01-01T00:03:01Z",
                "expiry_ts": "2025-01-01T00:04:01Z",
                "entry_price": 100,
                "final_price": 101,
                "outcome": "WIN",
                "stake": 1,
                "net_result": 0.8,
                "price_base": "close",
                "quality": "SYNTHETIC_VALIDATED",
                "resolution": "M1",
                "assumptions": {
                    "partition": "evaluation",
                    "virtual_contract": {"stake": 1, "payout_net": 0.8, "loss_amount": 1},
                },
                "payload": {
                    "analysis": "trend_pullback_v1",
                    "variant": "variant_a",
                    "instrument": "TEST/USD",
                    "partition": "evaluation",
                    "contract": "illustrative",
                },
            },
        )
        self.store.finish_session(self.sid)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_pagination_ranges_recent_and_tie_break(self) -> None:
        query = QueryService(self.store, max_limit=10)
        first = query.query_candles(self.sid, timeframe="M1", revisions="all", limit=1)
        self.assertEqual(first.total, 4)
        self.assertTrue(first.has_more)
        second = query.query_candles(self.sid, timeframe="M1", revisions="all", limit=1, cursor=first.next_cursor)
        self.assertEqual(second.items[0]["start_ts"], "2025-01-01T00:02:00.000000Z")
        self.assertNotEqual(first.items[0]["candle_row_id"], second.items[0]["candle_row_id"])
        recent = query.query_candles(self.sid, timeframe="M1", recent=True, limit=1)
        self.assertEqual(recent.items[0]["candle_id"], "c3-open")
        ranged = query.query_candles(
            self.sid,
            timeframe="M1",
            start_ts="2025-01-01T00:02:00Z",
            end_ts="2025-01-01T00:03:00Z",
            revisions="all",
            limit=10,
        )
        self.assertEqual([x["candle_id"] for x in ranged.items], ["c2", "c2-r1"])
        with self.assertRaises(ValueError):
            query.query_candles(
                self.sid, timeframe="M1", limit=1, cursor=first.next_cursor, start_ts="2025-01-01T00:01:00Z"
            )

    def test_revisions_indicators_gaps_and_conditions(self) -> None:
        query = QueryService(self.store, max_limit=10)
        revisions = query.query_revisions(self.sid, timeframe="M1", limit=10)
        self.assertEqual([x["candle_id"] for x in revisions.items], ["c2-r1"])
        indicators = query.query_indicators(self.sid, timeframe="M1", limit=10)
        first = next(x for x in indicators.items if x["candle_id"] == "c0")
        self.assertEqual(first["indicator_values"]["rsi"], 55)
        gaps = query.query_gaps(self.sid, timeframe="M1")
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]["duration_seconds"], 60.0)
        self.assertFalse(gaps[0]["filled"])
        conditions = query.query_conditions(self.sid, limit=1)
        self.assertEqual(conditions.total, 2)
        self.assertTrue(conditions.has_more)
        second = query.query_conditions(self.sid, limit=1, cursor=conditions.next_cursor)
        self.assertEqual(second.items[0]["name"], "rsi")

    def test_report_contains_six_segment_dimensions_and_pending(self) -> None:
        self.store.save_simulation(
            self.sid,
            {
                "simulation_id": "variant_a:sig1:61",
                "signal_id": "sig1",
                "simulation_type": "VIRTUAL_CONTRACT",
                "horizon_seconds": 61,
                "direction": "UP",
                "detected_ts": "2025-01-01T00:03:00Z",
                "expiry_ts": "2025-01-01T00:05:00Z",
                "outcome": "PENDING",
                "stake": 1,
                "net_result": None,
                "price_base": "close",
                "quality": "SYNTHETIC_VALIDATED",
                "resolution": "M1",
                "assumptions": {
                    "partition": "evaluation",
                    "virtual_contract": {"stake": 1, "payout_net": 0.8, "loss_amount": 1},
                },
                "payload": {
                    "analysis": "trend_pullback_v1",
                    "variant": "variant_a",
                    "instrument": "TEST/USD",
                    "partition": "evaluation",
                    "contract": "illustrative",
                },
            },
        )
        data = ReportBuilder(self.store, self.sid).summary()
        self.assertEqual(data["mode_label"], "SINTETICO")
        self.assertTrue(data["segments"])
        self.assertEqual(
            set(data["segments"][0]["dimensions"]),
            {"analysis", "variant", "instrument", "horizon", "partition", "contract"},
        )
        self.assertIn("pending_count", data["aggregate"])
        self.assertIn("PENDING", data["outcomes"])
        query = QueryService(self.store, max_limit=10)
        page = query.query_simulations(self.sid, variant="variant_a", partition="evaluation", limit=1)
        self.assertEqual(page.total, 2)
        self.assertTrue(page.has_more)
        page2 = query.query_simulations(
            self.sid, variant="variant_a", partition="evaluation", limit=1, cursor=page.next_cursor
        )
        self.assertEqual(page2.total, 2)
        self.assertEqual(page2.items[0]["outcome"], "PENDING")
        markdown = ReportBuilder(self.store, self.sid).to_markdown(data)
        self.assertIn("Partición", markdown)
        self.assertIn("Contrato", markdown)
        self.assertIn("W/L/T/I/P", markdown)

    def test_local_ui_exposes_read_models_without_writes(self) -> None:
        server = create_server(self.db, host="127.0.0.1", port=0, session_id=self.sid)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"

            def get(path: str, *, parse_json: bool = True):
                with urlopen(base + path, timeout=3) as response:
                    body = response.read().decode()
                    return response.status, (json.loads(body) if parse_json else body)

            status, health = get("/api/health")
            self.assertEqual(status, 200)
            self.assertTrue(health["read_only"])
            _, poll = get(f"/api/poll?session={self.sid}&limit=1")
            self.assertIn("status", poll)
            self.assertLessEqual(len(poll["events"]["items"]), 1)
            _, candles = get(f"/api/candles?session={self.sid}&timeframe=M1&limit=1&revisions=all")
            self.assertEqual(len(candles["items"]), 1)
            self.assertIn("next_cursor", candles)
            _, indicators = get(f"/api/indicators?session={self.sid}&timeframe=M1&limit=10")
            self.assertIn("indicator_values", indicators["items"][0])
            _, gaps = get(f"/api/gaps?session={self.sid}&timeframe=M1")
            self.assertEqual(gaps["items"][0]["quality"], "GAP_OBSERVED")
            _, conditions = get(f"/api/conditions?session={self.sid}&limit=1")
            self.assertEqual(conditions["items"][0]["condition_ordinal"], 0)
            _, report = get(f"/api/report?session={self.sid}")
            self.assertEqual(report["mode_label"], "SINTETICO")
            _, page = get(f"/api/simulations?session={self.sid}&variant=variant_a&partition=evaluation")
            self.assertEqual(len(page["items"]), 1)
            self.assertEqual(page["items"][0]["dimensions"]["contract"], "illustrative")
            _, root = get("/", parse_json=False)
            self.assertIn("MTF Lab", root)
        finally:
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()
            server.store.close()


if __name__ == "__main__":
    unittest.main()
