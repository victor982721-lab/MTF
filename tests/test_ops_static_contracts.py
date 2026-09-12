from __future__ import annotations

import base64
import json
import tempfile
import unittest
from pathlib import Path

import mtf_lab.ops as ops
from mtf_lab.ops import demo
from mtf_lab.ops.logging_state import ProgressState
from mtf_lab.ops.persistence import _capture_cursor
from mtf_lab.ops.query import QueryService
from mtf_lab.ops.simulation import EvaluationSpec, VirtualContractSimulator


class OpsStaticContractTests(unittest.TestCase):
    def test_package_exports_remain_bound(self) -> None:
        for name in ops.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(ops, name))

    def test_demo_module_exports_remain_bound(self) -> None:
        for name in demo.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(demo, name))

    def test_progress_snapshot_is_typed_and_independent(self) -> None:
        state = ProgressState(mode="REPLAY", instrument="TEST/USD")
        state.merge_nested("coverage", {"M1": {"count": 1}})
        snapshot = state.snapshot()
        snapshot["coverage"]["M1"]["count"] = 99
        self.assertEqual(state.snapshot()["coverage"]["M1"]["count"], 1)

    def test_cursor_decoders_reject_ambiguous_values(self) -> None:
        self.assertEqual(_capture_cursor(("2025-01-01T00:00:00Z", "7")), ("2025-01-01T00:00:00.000000Z", 7, -1))
        with self.assertRaises(ValueError):
            _capture_cursor(("2025-01-01T00:00:00Z", True))

    def test_query_cursor_non_object_is_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            from mtf_lab.ops.persistence import SQLiteStore

            with SQLiteStore(Path(tmp) / "query.sqlite3") as store:
                service = QueryService(store)
                cursor = base64.urlsafe_b64encode(json.dumps([]).encode()).decode().rstrip("=")
                with self.assertRaises(ValueError):
                    service._read_cursor(cursor, table="events", filters={}, order="asc")

    def test_spec_conflict_is_explicit(self) -> None:
        spec = EvaluationSpec()
        with self.assertRaises(ValueError):
            VirtualContractSimulator(spec, spec)


if __name__ == "__main__":
    unittest.main()
