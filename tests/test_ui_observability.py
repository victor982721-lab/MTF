from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any, cast
from urllib.request import urlopen

from mtf_lab.ops import cli
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.query import QueryService
from mtf_lab.ops.ui import INDEX_HTML, create_server


def _extract_observability_js() -> str:
    start = INDEX_HTML.index("function freshnessMaxAge")
    end = INDEX_HTML.index("function renderObservability", start)
    return INDEX_HTML[start:end]


class UIObservabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="mtf-ui-observability-")
        self.db = Path(self.tmp.name) / "observability.sqlite3"
        self.store = SQLiteStore(self.db)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def _event(self, event_id: str = "e1") -> dict[str, object]:
        return {
            "event_id": event_id,
            "source": "fixture",
            "instrument": "EUR/USD",
            "event_ts": "2026-01-01T00:01:00Z",
            "received_ts": "2026-01-01T00:01:01Z",
            "available_ts": "2026-01-01T00:01:02Z",
            "kind": "quote",
            "price_base": "mid",
            "bid": 1.1,
            "ask": 1.2,
        }

    def test_provenance_keeps_observed_demo_separate_from_account_environment(self) -> None:
        sid = self.store.create_session(
            session_id="demo-observed",
            mode="REPLAY",
            provider="ctrader-open-api",
            instrument="EUR/USD",
            dataset_ref="capture-abc",
            config={"ctrader": {"environment": "REAL", "account_id": "account-redacted"}},
            metadata={
                "source_mode": "DEMO_OBSERVED",
                "synthetic": False,
                "provider": "ctrader-open-api",
                "environment": "DEMO",
                "network_performed": True,
                "execution_enabled": False,
                "dataset_hash": "dataset-hash",
                "capture_hash": "capture-hash",
                "coverage_start": "2026-01-01T00:00:00Z",
                "coverage_end": "2026-01-01T00:02:00Z",
            },
        )
        self.store.save_event(sid, self._event())
        self.store.finish_session(sid)

        status = QueryService(self.store).snapshot(sid)

        self.assertEqual(status["provenance"]["source_class"], "DEMO_OBSERVED")
        self.assertEqual(status["provenance"]["observed_environment"], "DEMO")
        self.assertEqual(status["provenance"]["account_environment"], "REAL")
        self.assertFalse(status["provenance"]["execution_enabled"])
        self.assertEqual(status["provenance"]["dataset_ref"], "capture-abc")
        self.assertEqual(status["provenance"]["range"]["start_ts"], "2026-01-01T00:00:00Z")
        self.assertEqual(status["process_status"]["status"], "COMPLETED")
        self.assertFalse(status["process_status"]["feed_active"])
        self.assertEqual(status["freshness"]["state"], "NOT_APPLICABLE")
        self.assertFalse(status["freshness"]["stale"])
        self.assertEqual(status["last_data"]["timestamp"], "2026-01-01T00:01:00.000000Z")

    def test_running_session_without_data_is_unknown_not_stale(self) -> None:
        sid = self.store.create_session(
            session_id="empty-live",
            mode="LIVE",
            provider="provider-name-is-not-proof",
            instrument="EUR/USD",
            metadata={"source_mode": "LIVE", "network_performed": False, "execution_enabled": False},
        )

        status = QueryService(self.store).snapshot(sid)

        self.assertEqual(status["provenance"]["source_class"], "UNKNOWN")
        self.assertEqual(status["process_status"]["status"], "RUNNING")
        self.assertEqual(status["last_data"]["timestamp"], None)
        self.assertEqual(status["freshness"]["state"], "UNKNOWN")
        self.assertFalse(status["freshness"]["stale"])
        self.assertFalse(status["freshness"]["feed_active"])

    def test_paused_live_capture_is_not_reported_as_active_feed(self) -> None:
        sid = self.store.create_session(
            session_id="paused-live",
            mode="LIVE",
            provider="ctrader-open-api",
            instrument="EUR/USD",
            metadata={"source_mode": "LIVE", "synthetic": False, "environment": "DEMO", "network_performed": True},
        )
        self.store.save_event(sid, self._event())
        self.store.save_checkpoint(
            sid,
            "runtime",
            state={
                "status": {"capture_state": "PAUSED", "connection": "CONNECTED", "freshness_state": "FRESH"},
                "processor": {"errors": 0},
            },
        )

        status = QueryService(self.store).snapshot(sid)

        self.assertFalse(status["process_status"]["feed_active"])
        self.assertFalse(status["freshness"]["feed_active"])

    def test_running_replay_is_not_reported_as_active_feed(self) -> None:
        sid = self.store.create_session(
            session_id="running-replay",
            mode="REPLAY",
            provider="fixture",
            instrument="EUR/USD",
            metadata={"source_mode": "HISTORICAL_REPLAY", "synthetic": False},
        )
        self.store.save_event(sid, self._event())
        self.store.save_checkpoint(
            sid,
            "runtime",
            state={
                "status": {"capture_state": "CAPTURING", "connection": "CONNECTED", "freshness_state": "VALID"},
                "processor": {"errors": 0},
            },
        )

        status = QueryService(self.store).snapshot(sid)

        self.assertEqual(status["process_status"]["status"], "RUNNING")
        self.assertFalse(status["process_status"]["feed_active"])
        self.assertFalse(status["freshness"]["feed_active"])

    def test_demo_source_label_requires_explicit_observation_evidence(self) -> None:
        sid = self.store.create_session(
            session_id="unverified-demo-label",
            mode="LIVE",
            provider="provider",
            instrument="EUR/USD",
            metadata={"source_mode": "DEMO_OBSERVED", "synthetic": False, "network_performed": False},
        )

        status = QueryService(self.store).snapshot(sid)

        self.assertEqual(status["provenance"]["source_mode"], "DEMO_OBSERVED")
        self.assertEqual(status["provenance"]["source_class"], "UNKNOWN")
        self.assertIn("evidence_missing", " ".join(status["provenance"]["evidence"]))

    def test_checkpoint_errors_are_projected_without_raw_messages(self) -> None:
        sid = self.store.create_session(
            session_id="checkpoint-errors",
            mode="LIVE",
            provider="fixture",
            instrument="EUR/USD",
            metadata={"source_mode": "UNKNOWN"},
        )
        self.store.save_checkpoint(
            sid,
            "runtime",
            state={
                "status": {
                    "capture_state": "ERROR",
                    "connection": "DISCONNECTED",
                    "freshness_state": "UNKNOWN",
                },
                "processor": {
                    "errors": 2,
                    "issues": [
                        {
                            "code": "event_invalid",
                            "message": "secret-token-and-stack-must-not-reach-ui",
                            "timestamp": "2026-01-01T00:01:00Z",
                        },
                        {"code": "gap", "message": "second", "timestamp": "2026-01-01T00:02:00Z"},
                    ],
                },
            },
        )

        status = QueryService(self.store).snapshot(sid)

        self.assertEqual(status["errors"]["count"], 2)
        self.assertEqual(status["errors"]["codes"], ["EVENT_INVALID", "GAP"])
        self.assertNotIn("secret-token", json.dumps(status))

    def test_http_status_and_poll_share_observability_projection(self) -> None:
        sid = self.store.create_session(
            session_id="api-observability",
            mode="REPLAY",
            provider="fixture",
            instrument="TEST/USD",
            dataset_ref="historical-fixture",
            metadata={"source_mode": "HISTORICAL_REPLAY", "synthetic": False},
        )
        server = create_server(self.db, host="127.0.0.1", port=0, session_id=sid)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        thread.start()
        try:
            base = f"http://127.0.0.1:{server.server_port}"

            def get(path: str) -> dict[str, Any]:
                with urlopen(base + path, timeout=3) as response:
                    return cast(dict[str, Any], json.loads(response.read().decode()))

            status = get(f"/api/status?session={sid}")
            poll = get(f"/api/poll?session={sid}&limit=1")
            self.assertEqual(status["provenance"], poll["status"]["provenance"])
            self.assertEqual(status["freshness"], poll["status"]["freshness"])
            self.assertEqual(status["process_status"], poll["status"]["process_status"])
        finally:
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()
            server.store.close()

    def test_actual_ctrader_watch_fixture_projects_exact_paused_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-ui-cli-watch-") as directory:
            db = Path(directory) / "watch.sqlite3"
            output, errors = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
                code = cli.main(
                    [
                        "ctrader",
                        "watch",
                        "--fixture",
                        "--db",
                        str(db),
                        "--max-events",
                        "40",
                        "--duration",
                        "5",
                        "--idle-timeout",
                        "0.1",
                    ]
                )
            self.assertEqual(code, 0, errors.getvalue())
            result = json.loads(output.getvalue())
            with SQLiteStore(db) as store:
                status = QueryService(store).snapshot(result["session_id"])

        self.assertEqual(status["checkpoint_name"], "ctrader-watch")
        self.assertEqual(status["checkpoint_analysis_id"], result["analysis_id"])
        self.assertEqual(status["capture_state"], "PAUSED")
        self.assertEqual(status["connection"], "DISCONNECTED")
        self.assertEqual(status["continuity_state"], "CONTINUOUS")
        self.assertEqual(status["freshness_state"], "NOT_APPLICABLE")
        self.assertFalse(status["analysis_enabled"])
        self.assertTrue(status["analysis_blocked_reasons"])
        self.assertTrue(all(status["warmup_pending"][name] > 0 for name in ("M1", "M5", "M15")))

    def test_checkpoint_projection_does_not_use_latest_other_analysis(self) -> None:
        sid = self.store.create_session(
            session_id="checkpoint-identity",
            mode="LIVE",
            provider="fixture",
            instrument="EUR/USD",
            dataset_ref="dataset-good",
        )
        good = self.store.create_analysis(
            sid,
            dataset_hash="dataset-good",
            config_hash="config-good",
            variant="trend_pullback_v1",
            partition="all",
            analysis_id="analysis-good",
        )
        other = self.store.create_analysis(
            sid,
            dataset_hash="dataset-other",
            config_hash="config-other",
            variant="trend_pullback_v1",
            partition="all",
            analysis_id="analysis-other",
        )
        self.store.save_checkpoint(
            sid,
            "runtime",
            analysis_id=good,
            state={"status": {"capture_state": "CAPTURING", "connection": "CONNECTED"}},
        )
        self.store.save_checkpoint(
            sid,
            "runtime",
            analysis_id=other,
            state={"status": {"capture_state": "PAUSED", "connection": "DISCONNECTED"}},
        )

        status = QueryService(self.store).snapshot(sid)

        self.assertEqual(status["checkpoint_analysis_id"], good)
        self.assertEqual(status["capture_state"], "CAPTURING")
        self.assertEqual(status["connection"], "CONNECTED")

    @unittest.skipUnless(
        os.environ.get("MTF_UI_JS_DEV") == "1", "dev-only: set MTF_UI_JS_DEV=1 for the Node freshness gate"
    )
    def test_browser_freshness_ages_when_poll_has_no_new_data(self) -> None:
        node = shutil.which("node")
        self.assertIsNotNone(node, "MTF_UI_JS_DEV=1 requires Node")
        assert node is not None
        runner = (
            """
const freshnessView = (function() {
  """
            + _extract_observability_js()
            + """
  return freshnessView;
})();
const status = {
  provenance: {source_class: 'UNKNOWN'},
  freshness: {state: 'FRESH', reason: 'runtime_projection', last_data_ts: '2026-01-01T00:00:00Z', max_age_seconds: 60},
  last_data_ts: '2026-01-01T00:00:00Z'
};
const missing = { ...status, freshness: { ...status.freshness } };
delete missing.freshness.max_age_seconds;
const nullValue = { ...status, freshness: { ...status.freshness, max_age_seconds: null } };
const invalid = { ...status, freshness: { ...status.freshness, max_age_seconds: 'not-a-number' } };
const booleanValue = { ...status, freshness: { ...status.freshness, max_age_seconds: false } };
const zero = { ...status, freshness: { ...status.freshness, max_age_seconds: 0 } };
const stringLimit = { ...status, freshness: { ...status.freshness, max_age_seconds: '60' } };
const oldNow = Date.now;
Date.now = () => Date.parse('2026-01-01T00:00:30Z');
const before = freshnessView(status);
const zeroAtThirty = freshnessView(zero);
Date.now = () => Date.parse('2026-01-01T00:02:00Z');
const after = freshnessView(status);
const noLimit = freshnessView(missing);
const nullLimit = freshnessView(nullValue);
const invalidLimit = freshnessView(invalid);
const booleanLimit = freshnessView(booleanValue);
const stringLimitResult = freshnessView(stringLimit);
Date.now = oldNow;
process.stdout.write(JSON.stringify({before, after, zeroAtThirty, noLimit, nullLimit, invalidLimit, booleanLimit, stringLimitResult}));
"""
        )
        result = subprocess.run([node, "-e", runner], capture_output=True, text=True, check=False, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["before"]["state"], "FRESH")
        self.assertEqual(payload["after"]["state"], "STALE")
        self.assertEqual(payload["after"]["reason"], "data_age_exceeded")
        self.assertEqual(payload["zeroAtThirty"]["state"], "STALE")
        for key in ("noLimit", "nullLimit", "invalidLimit", "booleanLimit"):
            self.assertEqual(payload[key]["state"], "FRESH")
        self.assertEqual(payload["stringLimitResult"]["state"], "STALE")


if __name__ == "__main__":
    unittest.main()
