from __future__ import annotations

import json
import tempfile
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.request import urlopen

from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.readiness import process_identity, read_supervisor_readiness
from mtf_lab.ops.ui import create_server


class ReadinessTests(unittest.TestCase):
    def test_process_identity_ttl_and_readiness_are_distinct(self) -> None:
        now = datetime.now(UTC)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            value = {
                "schema_version": 1,
                "mode": "shadow",
                "readiness": {"ready": True, "reasons": []},
                "runtime_identity": {**process_identity(), "published_at": now.isoformat(), "valid_for_seconds": 5},
            }
            path.write_text(json.dumps(value))
            path.chmod(0o600)
            self.assertTrue(read_supervisor_readiness(path, now=now)["ready"])
            value["mode"] = "UNKNOWN"
            path.write_text(json.dumps(value))
            self.assertFalse(read_supervisor_readiness(path, now=now)["ready"])
            value["mode"] = "shadow"
            path.write_text(json.dumps(value))
            self.assertFalse(read_supervisor_readiness(path, now=now + timedelta(seconds=6))["ready"])
            value["runtime_identity"]["process_start_ticks"] = "wrong"
            path.write_text(json.dumps(value))
            self.assertFalse(read_supervisor_readiness(path, now=now)["ready"])

    def test_no_evidence_unsafe_paths_and_secrets_are_not_exposed(self) -> None:
        self.assertFalse(read_supervisor_readiness(None)["ready"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            path.write_text('{"secret":"fixture-secret", "ready":true}')
            path.chmod(0o600)
            result = read_supervisor_readiness(path)
            self.assertFalse(result["ready"])
            self.assertNotIn("fixture-secret", str(result))
            alias = Path(directory) / "alias.json"
            alias.symlink_to(path)
            self.assertFalse(read_supervisor_readiness(alias)["ready"])

    def test_http_health_is_not_runtime_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "fixture.sqlite3"
            with SQLiteStore(db):
                pass
            server = create_server(db, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
            thread.start()
            try:
                base = f"http://127.0.0.1:{server.server_port}"
                with urlopen(base + "/api/health", timeout=3) as response:
                    self.assertEqual(response.status, 200)
                with urlopen(base + "/api/readiness", timeout=3) as response:
                    value = json.load(response)
                    self.assertFalse(value["ready"])
                    self.assertEqual(value["reasons"], ["NO_RUNTIME_EVIDENCE"])
            finally:
                server.shutdown()
                thread.join(timeout=3)
                server.server_close()
                server.store.close()
