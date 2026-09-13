from __future__ import annotations

import unittest

from mtf_lab.ops.runtime_safety import sqlite_wal_readiness


class SQLiteRuntimeSafetyTests(unittest.TestCase):
    def test_upstream_versions_and_unknown_vendor_patches_stay_distinct(self) -> None:
        for version in ("3.51.3", "3.51.4", "3.52.0", "3.44.6", "3.50.7"):
            self.assertTrue(sqlite_wal_readiness(version)["external_continuous_ready"])
        for version in ("3.46.1", "3.51.2", "3.50.6", "unknown", "3.46.1-9ubuntu0.2"):
            status = sqlite_wal_readiness(version)
            self.assertFalse(status["external_continuous_ready"])
            self.assertEqual(status["wal_reset_patch"], "VENDOR_BACKPORT_NOT_VERIFIED")
            self.assertFalse(status["database_opened"])
