from __future__ import annotations

import json
import os
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

from mtf_lab.ops.ui import SnapshotSecurityError, create_server


class UISnapshotModeTests(unittest.TestCase):
    def _serve(self, path: Path):
        server = create_server(snapshot_path=path, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, thread

    @staticmethod
    def _get(base: str, path: str) -> dict[str, object]:
        with urlopen(base + path, timeout=3) as response:
            return json.loads(response.read().decode("utf-8"))

    def test_snapshot_mode_does_not_construct_sqlite_and_ignores_url_session(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-ui-snapshot-") as directory:
            path = Path(directory) / "state.json"
            path.write_text(
                json.dumps(
                    {
                        "schema": "mtf-lab.research-report.v1",
                        "generated_at": "1970-01-01T00:00:00Z",
                        "provenance": {"labels": ["REAL_HISTORICAL_QUOTES"], "account_id": "private"},
                        "counts": {"trades": 2},
                        "status": {"ready": True},
                        "report_secret": "never-return-this",
                    }
                ),
                encoding="utf-8",
            )
            path.chmod(0o600)
            with patch("mtf_lab.ops.ui.SQLiteStore", side_effect=AssertionError("SQLite must not open")):
                server, thread = self._serve(path)
            try:
                base = f"http://127.0.0.1:{server.server_port}"
                health = self._get(base, "/api/health?session=secret")
                status = self._get(base, "/api/status?session=secret")
                report = self._get(base, "/api/report?path=/other/user.json")
                readiness = self._get(base, "/api/readiness?ready=true")
                self.assertTrue(health["ok"])
                self.assertTrue(health["snapshot_mode"])
                self.assertEqual(status["mode"], "SNAPSHOT")
                self.assertFalse(status["readiness_verified"])
                self.assertEqual(status["staleness"]["state"], "STALE")
                self.assertNotIn("never-return-this", json.dumps(report))
                self.assertEqual(readiness["ready"], False)
            finally:
                server.shutdown()
                thread.join(timeout=3)
                server.server_close()

    def test_snapshot_file_must_be_private_regular_single_link_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-ui-snapshot-security-") as directory:
            root = Path(directory)
            valid = root / "valid.json"
            valid.write_text("{}", encoding="utf-8")
            valid.chmod(0o600)
            hardlink = root / "hardlink.json"
            os.link(valid, hardlink)
            with self.assertRaises(SnapshotSecurityError):
                create_server(snapshot_path=hardlink, port=0)
            link = root / "link.json"
            link.symlink_to(valid)
            with self.assertRaises(SnapshotSecurityError):
                create_server(snapshot_path=link, port=0)
            public = root / "public.json"
            public.write_text("{}", encoding="utf-8")
            public.chmod(0o644)
            with self.assertRaises(SnapshotSecurityError):
                create_server(snapshot_path=public, port=0)
            with self.assertRaises(SnapshotSecurityError):
                create_server(snapshot_path="https://example.invalid/state.json", port=0)

    def test_readiness_is_not_derived_from_snapshot_and_health_is_static(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-ui-snapshot-ready-") as directory:
            path = Path(directory) / "state.json"
            path.write_text(
                json.dumps({"generated_at": "2999-01-01T00:00:00Z", "readiness": {"ready": True}}), encoding="utf-8"
            )
            path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            server, thread = self._serve(path)
            try:
                base = f"http://127.0.0.1:{server.server_port}"
                status = self._get(base, "/api/status")
                health = self._get(base, "/api/health")
                readiness = self._get(base, "/api/readiness")
                self.assertEqual(status["staleness"]["state"], "UNKNOWN")
                self.assertFalse(status["ready"])
                self.assertTrue(health["ok"])
                self.assertFalse(readiness["ready"])
            finally:
                server.shutdown()
                thread.join(timeout=3)
                server.server_close()


if __name__ == "__main__":
    unittest.main()
