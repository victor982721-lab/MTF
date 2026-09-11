"""Focused contracts for the credential-free delivery gate."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import offline_tests
from tools import verify_offline as delivery

ROOT = Path(__file__).resolve().parents[1]


class EngineeringDeliveryTests(unittest.TestCase):
    def test_sanitization_redacts_secrets_and_absolute_paths(self) -> None:
        payload = delivery.sanitize_payload(
            {
                "access_token": "do-not-publish",
                "nested": {"client_secret": "also-private"},
                "stderr": f"authorization=Bearer abc123 at {ROOT}/.venv/bin/python",
            },
            root=ROOT,
        )
        rendered = json.dumps(payload, sort_keys=True)
        self.assertNotIn("do-not-publish", rendered)
        self.assertNotIn("also-private", rendered)
        self.assertNotIn(str(ROOT), rendered)
        self.assertIn("<redacted>", rendered)

    def test_safe_environment_drops_credentials_and_python_overrides(self) -> None:
        source = {
            "PATH": "/usr/bin",
            "PYTHONPATH": "/private/checkout",
            "CTRADER_CLIENT_SECRET": "secret",
            "CTRADER_ACCESS_TOKEN": "token",
            "MTF_UI_JS_DEV": "1",
            "MTF_NODE_BIN": "/usr/bin/node",
        }
        safe = delivery.safe_environment(source)
        self.assertEqual({"PATH", "MTF_UI_JS_DEV", "MTF_NODE_BIN"}, set(safe))

    def test_offline_child_environment_drops_ambient_credentials(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-offline-env-") as name:
            temp_root = Path(name)
            guard = temp_root / "guard"
            with patch.dict(
                os.environ,
                {
                    "CTRADER_CLIENT_SECRET": "secret",
                    "CTRADER_ACCESS_TOKEN": "token",
                    "PYTHONPATH": "/private/override",
                    "MTF_UI_JS_DEV": "1",
                },
                clear=True,
            ):
                child = offline_tests._isolated_env(ROOT, temp_root, guard, block_writes=False)
        self.assertNotIn("CTRADER_CLIENT_SECRET", child)
        self.assertNotIn("CTRADER_ACCESS_TOKEN", child)
        self.assertNotEqual("/private/override", child["PYTHONPATH"])
        self.assertIn(str(guard), child["PYTHONPATH"])
        self.assertEqual("1", child["MTF_UI_JS_DEV"])

    def test_offline_suite_failure_preserves_sanitized_runner_diagnostics(self) -> None:
        payload = {
            "success": False,
            "suite": {
                "summary": {
                    "discovered": 2,
                    "tests_run": 2,
                    "passed": 0,
                    "failed": 2,
                    "skipped": 0,
                    "expected_failures": 0,
                    "unexpected_successes": 0,
                    "failure_identifiers": ["tests.BadTests.test_summary"],
                },
                "stdout": "suite output",
                "stderr": (
                    "FAIL: test_bad (tests.BadTests.test_bad)\n"
                    "ERROR: test_broken (tests.BadTests.test_broken)\n"
                    "authorization=Bearer should-not-publish at /home/example/private\n"
                ),
            },
        }
        outcome = delivery._CommandOutcome(("python", "tools/offline_tests.py"), 1, "", "", False, 0.1)
        with tempfile.TemporaryDirectory(prefix="mtf-delivery-runner-failure-") as name:
            temporary_root = Path(name)
            (temporary_root / "offline-results.json").write_text(json.dumps(payload), encoding="utf-8")
            with (
                patch.object(delivery, "_node_version", return_value={"ok": True, "version": "v-test"}),
                patch.object(delivery, "run_command", return_value=outcome),
            ):
                result = delivery._run_offline_suite(
                    Path("runtime-python"),
                    Path("node"),
                    root=ROOT,
                    environment={},
                    temporary_root=temporary_root,
                    timeout=1,
                )
        self.assertFalse(result["ok"])
        self.assertEqual(2, result["counts"]["failed"])
        self.assertEqual("suite output", result["runner_output"]["stdout_tail"])
        self.assertEqual(
            [
                "test_bad (tests.BadTests.test_bad)",
                "test_broken (tests.BadTests.test_broken)",
                "tests.BadTests.test_summary",
            ],
            result["runner_output"]["failure_identifiers"],
        )
        self.assertNotIn("should-not-publish", result["runner_output"]["stderr_tail"])
        self.assertNotIn("/home/example/private", result["runner_output"]["stderr_tail"])
        self.assertEqual(2, result["runner_summary"]["failed"])

    def test_source_manifest_includes_runtime_and_final_documents(self) -> None:
        manifest = delivery.source_manifest(ROOT)
        files = manifest["files"]
        self.assertIn("mtf_lab/runtime/state.py", files)
        self.assertIn("reports/engineering/latest/engineering_consolidation.md", files)
        self.assertIn("reports/engineering/history/2026-09-11-refactor.json", files)
        self.assertNotIn("reports/engineering/latest/engineering_results.json", files)
        self.assertNotIn("reports/engineering/latest/engineering_tooling.json", files)

    def test_source_manifest_delta_detects_runtime_and_report_changes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-delivery-manifest-") as name:
            root = Path(name)
            runtime = root / "mtf_lab" / "runtime"
            report = root / "reports" / "engineering" / "latest"
            history = root / "reports" / "engineering" / "history"
            runtime.mkdir(parents=True)
            report.mkdir(parents=True)
            history.mkdir(parents=True)
            state = runtime / "state.py"
            human = report / "engineering_consolidation.md"
            state.write_text("STATE = 1\n", encoding="utf-8")
            human.write_text("report v1\n", encoding="utf-8")
            (history / "refactor.json").write_text("{}\n", encoding="utf-8")
            (report / "engineering_results.json").write_text("{}\n", encoding="utf-8")
            before = delivery.source_manifest(root)
            state.write_text("STATE = 2\n", encoding="utf-8")
            human.write_text("report v2\n", encoding="utf-8")
            after = delivery.source_manifest(root)
            delta = delivery._manifest_delta(before, after)
        self.assertFalse(delta["ok"])
        self.assertEqual(
            ["mtf_lab/runtime/state.py", "reports/engineering/latest/engineering_consolidation.md"], delta["changed"]
        )

    def test_source_manifest_fails_loudly_when_a_file_cannot_be_read(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-delivery-unreadable-") as name:
            root = Path(name)
            source = root / "mtf_lab" / "runtime"
            source.mkdir(parents=True)
            (source / "state.py").write_text("STATE = 1\n", encoding="utf-8")
            with (
                patch.object(Path, "open", side_effect=OSError("unreadable")),
                self.assertRaisesRegex(RuntimeError, "unable to hash source file"),
            ):
                delivery.source_manifest(root)

    def test_link_check_defers_only_generated_receipts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-delivery-links-") as name:
            root = Path(name)
            report = root / "reports" / "engineering" / "latest"
            docs = root / "docs"
            report.mkdir(parents=True)
            docs.mkdir(parents=True)
            (report / "engineering_consolidation.md").write_text(
                "[results](engineering_results.json) [tooling](engineering_tooling.json)\n",
                encoding="utf-8",
            )
            (docs / "activation_boundaries.md").write_text(
                "[results](../reports/engineering/latest/engineering_results.json) "
                "[tooling](../reports/engineering/latest/engineering_tooling.json)\n",
                encoding="utf-8",
            )
            before = delivery.document_link_check(root)
            (report / "engineering_results.json").write_text("{}\n", encoding="utf-8")
            (report / "engineering_tooling.json").write_text("{}\n", encoding="utf-8")
            after = delivery.document_link_check(root)
        self.assertTrue(before["ok"], before)
        self.assertEqual(
            [
                "reports/engineering/latest/engineering_results.json",
                "reports/engineering/latest/engineering_tooling.json",
            ],
            before["deferred_generated"],
        )
        self.assertTrue(after["ok"], after)
        self.assertEqual([], after["deferred_generated"])
        self.assertEqual([], after["broken"])

    def test_counts_are_separate_and_consistent(self) -> None:
        counts = delivery._compact_counts(
            {
                "discovered": 8,
                "tests_run": 8,
                "passed": 6,
                "failed": 1,
                "skipped": 1,
                "expected_failures": 0,
                "unexpected_successes": 0,
            }
        )
        self.assertEqual(
            {"discovered": 8, "tests_run": 8, "executed": 7, "passed": 6, "failed": 1, "skipped": 1},
            {key: counts[key] for key in ("discovered", "tests_run", "executed", "passed", "failed", "skipped")},
        )
        self.assertTrue(counts["consistent"])

    def test_build_reports_keeps_relative_references_and_redacts_input(self) -> None:
        bundle = {
            "schema_version": 2,
            "success": True,
            "source_tree": {"file_count": 1, "content_sha256": "a" * 64, "head_tree_hash": "b" * 64},
            "validation": {},
            "tests": {"discovered": 1, "executed": 1, "passed": 1, "failed": 0, "skipped": 0},
            "environment": {"path": "/home/example/private", "client_secret": "private"},
            "external_validation": {"status": "NOT_EXECUTED"},
        }
        results, tooling = delivery.build_reports(bundle)
        rendered = json.dumps((results, tooling), sort_keys=True)
        self.assertNotIn('client_secret": "private"', rendered)
        self.assertNotIn("/home/example/private", rendered)
        self.assertEqual("engineering_consolidation.md", results["references"]["report"])
        self.assertEqual("engineering_results.json", results["references"]["results"])
        self.assertEqual("engineering_tooling.json", results["references"]["tooling"])
        self.assertEqual("b" * 64, results["source_tree"]["head_tree_hash"])
        self.assertNotIn("tree_hash", results["source_tree"])
        self.assertIn("validated_working_tree_identity", results["identity_contract"])
        self.assertEqual("b" * 64, tooling["source_tree"]["head_tree_hash"])
        self.assertEqual(results["identity_contract"], tooling["identity_contract"])
        self.assertEqual("NOT_EXECUTED", tooling["external_validation"]["status"])


if __name__ == "__main__":
    unittest.main()
