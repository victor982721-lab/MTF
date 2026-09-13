"""Focal contracts for the isolated reliability benchmark."""

from __future__ import annotations

import json
import tracemalloc
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import tools.benchmark_reliability as benchmark
from tools.benchmark_reliability import JSON_SCHEMA, run_benchmark


class BenchmarkReliabilityTests(unittest.TestCase):
    def test_schema_and_routes_report_separate_metrics_and_equivalence(self) -> None:
        report = run_benchmark(events=6, iterations=1, seed=17, batch_size=3)

        self.assertEqual(report["schema_version"], 1)
        self.assertEqual(report["benchmark"], "mtf-reliability")
        self.assertFalse(report["measurement_contract"]["timing"]["tracemalloc"])
        self.assertTrue(report["measurement_contract"]["memory"]["tracemalloc"])
        self.assertEqual(report["input"]["seed"], 17)
        self.assertEqual(report["input"]["market_event_count"], 6)
        self.assertEqual(report["input"]["envelope_count"], 7)
        self.assertEqual(len(report["input"]["sha256"]), 64)
        self.assertEqual(
            report["input"]["sha256"],
            report["equivalence"]["input_sha256"]["atomic_batch"][0],
        )
        self.assertEqual(
            report["input"]["sha256"],
            report["equivalence"]["input_sha256"]["unbatched"][0],
        )

        for name in ("atomic_batch", "unbatched"):
            variant = report["variants"][name]
            self.assertEqual(variant["iterations"], 1)
            self.assertIn("ingest", variant["phases"])
            ingest = variant["phases"]["ingest"]
            self.assertIn("timing", ingest)
            self.assertIn("memory", ingest)
            self.assertIsNotNone(ingest["timing"]["wall_seconds"]["p50"])
            self.assertIsNotNone(ingest["timing"]["cpu_seconds"]["p95"])
            self.assertIsNotNone(ingest["memory"]["maxrss_bytes"]["p99"])
            self.assertIsNotNone(ingest["memory"]["bytes_work_new"]["p50"])
            self.assertIn("checkpoint", variant["phases"])
            self.assertIn("finish", variant["phases"])
            self.assertIn("query_snapshot", variant["phases"])
            self.assertEqual(variant["runs"][0]["input_sha256"], report["input"]["sha256"])
            self.assertIn("indicator_updates", variant["runs"][0]["result"])

        self.assertTrue(report["equivalence"]["input_identity_match"])
        self.assertTrue(report["equivalence"]["functional_match"])
        self.assertTrue(report["equivalence"]["functional_pass_match"])
        self.assertTrue(report["equivalence"]["durable_match"])
        self.assertTrue(report["equivalence"]["query_match"])

        for name in ("atomic_batch", "unbatched"):
            self.assertEqual(
                report["variants"][name]["passes"]["timing"]["functional_hashes"],
                report["variants"][name]["passes"]["memory"]["functional_hashes"],
            )

        codec_status = report["codec"]["status"]
        self.assertIn(codec_status, {"available", "not_available"})
        if codec_status == "available":
            self.assertEqual(report["codec"]["backend"], "bundled_official_generated")
            self.assertEqual(report["codec"]["decoded_envelopes"], 7)
        else:
            self.assertEqual(report["codec"]["backend"], "fixture_json_fallback")

    def test_seed_changes_input_identity_but_not_route_shape(self) -> None:
        first = run_benchmark(events=4, iterations=1, seed=1, batch_size=2)
        second = run_benchmark(events=4, iterations=1, seed=2, batch_size=2)

        self.assertNotEqual(first["input"]["sha256"], second["input"]["sha256"])
        self.assertEqual(first["input"]["market_event_count"], second["input"]["market_event_count"])
        self.assertTrue(first["equivalence"]["functional_match"])
        self.assertTrue(second["equivalence"]["functional_match"])

    def test_speed_and_memory_measurements_observe_their_required_tracing_state(self) -> None:
        tracing_before = tracemalloc.is_tracing()
        try:
            tracemalloc.stop()
            with patch.object(tracemalloc, "is_tracing", return_value=False) as speed_state:
                _result, speed_sample = benchmark._measure_speed(lambda: "speed")
            speed_state.assert_called()
            self.assertTrue(hasattr(speed_sample, "wall_ns"))
            self.assertFalse(hasattr(speed_sample, "python_peak_delta_bytes"))

            tracemalloc.start()
            with TemporaryDirectory(prefix="mtf-benchmark-memory-test-") as tmp:
                baseline = benchmark._MemoryBaseline(0)
                with patch.object(tracemalloc, "is_tracing", return_value=True) as memory_state:
                    _result, memory_sample = benchmark._measure_memory(
                        lambda: "memory",
                        work_root=Path(tmp),
                        baseline=baseline,
                    )
                memory_state.assert_called()
            self.assertTrue(hasattr(memory_sample, "python_peak_delta_bytes"))
            self.assertFalse(hasattr(memory_sample, "wall_ns"))
        finally:
            tracemalloc.stop()
            if tracing_before:
                tracemalloc.start()

    def test_arguments_are_positive(self) -> None:
        with self.assertRaises(ValueError):
            run_benchmark(events=0)
        with self.assertRaises(ValueError):
            run_benchmark(iterations=0)
        with self.assertRaises(ValueError):
            run_benchmark(batch_size=0)

    def test_json_schema_is_serializable_and_describes_top_level_contract(self) -> None:
        encoded = json.dumps(JSON_SCHEMA, sort_keys=True)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["$id"], "mtf.reliability-benchmark.v1")
        self.assertEqual(
            decoded["required"],
            [
                "schema_version",
                "benchmark",
                "measurement_contract",
                "input",
                "codec",
                "variants",
                "equivalence",
            ],
        )


if __name__ == "__main__":
    unittest.main()
