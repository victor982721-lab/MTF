"""Focused contracts for the complete quality-gate orchestration.

These tests exercise command construction and isolation with tiny temporary
trees.  They do not invoke the repository-wide suite from inside the suite.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import offline_tests, quality_gate, verify_offline
from tools.quality_scope import discover_quality_scope


class QualityScopeContracts(unittest.TestCase):
    def test_scope_is_global_and_mypy_has_no_hidden_file_filter(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-scope-") as name:
            root = Path(name)
            for directory in ("mtf_lab", "tests", "tools"):
                path = root / directory
                path.mkdir()
                (path / f"{directory}_module.py").write_text("VALUE = 1\n", encoding="utf-8")
            scope = discover_quality_scope(root)
            calls: list[tuple[str, list[str]]] = []

            def fake_run(
                name: str,
                command: list[str] | tuple[str, ...],
                **_kwargs: object,
            ) -> dict[str, object]:
                calls.append((name, list(command)))
                return {"ok": True, "name": name}

            with (
                patch.object(quality_gate, "_run_check", side_effect=fake_run),
                patch.object(verify_offline, "document_link_check", return_value={"ok": True}),
            ):
                result = quality_gate._static_checks(
                    Path("runtime-python"),
                    Path("dev-python"),
                    root=root,
                    environment={},
                    temporary_root=root / "gate-tmp",
                    scope=scope,
                    timeout=1,
                )

        self.assertTrue(result["scope"]["ok"])
        ruff = next(command for name, command in calls if name == "ruff")
        self.assertEqual(["mtf_lab", "tests", "tools"], ruff[-3:])
        mypy = next(command for name, command in calls if name == "mypy")
        self.assertIn("--strict", mypy)
        self.assertIn("--explicit-package-bases", mypy)
        self.assertNotIn("--follow-imports=silent", mypy)
        self.assertNotIn("--follow-imports=skip", mypy)
        self.assertEqual(["mtf_lab", "tools"], mypy[-2:])

    def test_missing_scope_is_a_required_failure_not_an_empty_command(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-missing-scope-") as name:
            root = Path(name)
            for directory in ("mtf_lab", "tools"):
                path = root / directory
                path.mkdir()
                (path / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
            scope = discover_quality_scope(root)
            with (
                patch.object(quality_gate, "_run_check") as run_check,
                patch.object(verify_offline, "document_link_check", return_value={"ok": True}),
            ):
                result = quality_gate._static_checks(
                    Path("runtime-python"),
                    Path("dev-python"),
                    root=root,
                    environment={},
                    temporary_root=root / "gate-tmp",
                    scope=scope,
                    timeout=1,
                )
        self.assertFalse(result["scope"]["ok"])
        self.assertFalse(result["ruff"]["ok"])
        self.assertFalse(result["mypy"]["ok"])
        self.assertNotIn("ruff", [call.args[0] for call in run_check.call_args_list])
        self.assertNotIn("ruff_format", [call.args[0] for call in run_check.call_args_list])
        self.assertNotIn("mypy", [call.args[0] for call in run_check.call_args_list])

    def test_delivery_gate_uses_the_same_global_scope_without_advisory_escape(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-delivery-scope-") as name:
            root = Path(name)
            for directory in ("mtf_lab", "tests", "tools"):
                path = root / directory
                path.mkdir()
                (path / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
            scope = discover_quality_scope(root)
            commands: list[list[str]] = []

            def fake_command(command: list[str] | tuple[str, ...], **_kwargs: object) -> verify_offline._CommandOutcome:
                commands.append(list(command))
                return verify_offline._CommandOutcome(tuple(command), 0, "", "", False, 0.0)

            with (
                patch.object(verify_offline, "run_command", side_effect=fake_command),
                patch.object(verify_offline, "document_link_check", return_value={"ok": True}),
            ):
                result = verify_offline._run_quality_gates(
                    Path("runtime-python"),
                    Path("dev-python"),
                    root=root,
                    environment={},
                    temporary_root=root / "gate-tmp",
                    timeout=1,
                    scope=scope,
                )

        self.assertTrue(result["quality_scope"]["ok"])
        self.assertNotIn("ruff_full_repository_advisory", result)
        self.assertNotIn("format_full_repository_advisory", result)
        ruff = next(command for command in commands if "ruff" in command)
        self.assertEqual(["mtf_lab", "tests", "tools"], ruff[-3:])
        mypy = next(command for command in commands if "mypy" in command)
        self.assertIn("--explicit-package-bases", mypy)
        self.assertNotIn("--follow-imports=silent", mypy)
        self.assertEqual(["mtf_lab", "tools"], mypy[-2:])
        pyright = next(command for command in commands if "pyright" in command)
        self.assertIn("--project", pyright)
        self.assertIn("pyrightconfig.json", pyright)
        self.assertIn("--pythonpath", pyright)
        self.assertIn("runtime-python", pyright)


class QualityGateFailureContracts(unittest.TestCase):
    def test_relative_interpreters_and_node_are_anchored_to_requested_root(self) -> None:
        root = Path("/tmp/alternate-mtf-root")
        self.assertEqual(root / "runtime/python", quality_gate._resolve_path("runtime/python", root, root=root))
        self.assertEqual(root / "dev/python", quality_gate._resolve_path("dev/python", root, root=root))
        self.assertEqual(root / "bin/node", quality_gate._node_path("bin/node", root=root))

    def test_missing_tool_is_recorded_as_failed_command(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-tool-") as name:
            root = Path(name)
            receipt = quality_gate._run_check(
                "missing-tool",
                [str(root / "does-not-exist")],
                root=root,
                environment={"PATH": os.defpath},
                timeout=1,
                temporary_root=root,
            )
        self.assertFalse(receipt["ok"])
        self.assertNotEqual(0, receipt["returncode"])
        self.assertTrue(receipt["error"])

    def test_missing_pyright_is_a_required_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-pyright-") as name:
            root = Path(name)
            receipt = quality_gate._run_check(
                "pyright",
                [str(root / "missing-python"), "-m", "pyright"],
                root=root,
                environment={"PATH": os.defpath},
                timeout=1,
                temporary_root=root,
            )
        self.assertFalse(receipt["ok"])
        self.assertEqual("pyright", receipt["name"])

    def test_missing_coverage_json_cannot_pass_or_drop_denominators(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-coverage-") as name:
            summary = quality_gate._coverage_summary(Path(name) / "missing.json", threshold=60.0)
        self.assertFalse(summary["ok"])
        for key in (
            "covered_lines",
            "num_statements",
            "covered_branches",
            "num_branches",
            "line_percent",
            "branch_percent",
        ):
            self.assertIsNone(summary[key])

    def test_coverage_threshold_uses_line_and_branch_denominators(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-coverage-values-") as name:
            path = Path(name) / "coverage.json"
            path.write_text(
                json.dumps(
                    {
                        "totals": {
                            "covered_lines": 90,
                            "num_statements": 100,
                            "covered_branches": 6,
                            "num_branches": 10,
                        }
                    }
                ),
                encoding="utf-8",
            )
            summary = quality_gate._coverage_summary(path, threshold=60.0)
        self.assertTrue(summary["ok"])
        self.assertEqual(90, summary["covered_lines"])
        self.assertEqual(100, summary["num_statements"])
        self.assertEqual(6, summary["covered_branches"])
        self.assertEqual(10, summary["num_branches"])
        self.assertEqual(90.0, summary["line_percent"])
        self.assertEqual(60.0, summary["branch_percent"])

    def test_impossible_coverage_totals_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-impossible-coverage-") as name:
            path = Path(name) / "coverage.json"
            path.write_text(
                json.dumps(
                    {
                        "totals": {
                            "covered_lines": 101,
                            "num_statements": 100,
                            "covered_branches": 11,
                            "num_branches": 10,
                        }
                    }
                ),
                encoding="utf-8",
            )
            summary = quality_gate._coverage_summary(path, threshold=0.0)
        self.assertFalse(summary["ok"])
        self.assertIn("impossible", summary["error"])

    def test_node_absence_is_a_required_offline_suite_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-node-") as name:
            root = Path(name)
            result = verify_offline._run_offline_suite(
                Path(sys.executable),
                root / "missing-node",
                root=root,
                environment={"PATH": os.defpath},
                temporary_root=root / "gate-tmp",
                timeout=1,
            )
        self.assertFalse(result["ok"])
        self.assertEqual("node-not-executable", result["node"]["reason"])
        self.assertIsNone(result["counts"]["discovered"])

    def test_skip_coverage_is_never_a_successful_gate(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-skip-") as name:
            root = Path(name)
            for directory in ("mtf_lab", "tests", "tools"):
                path = root / directory
                path.mkdir()
                (path / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
            with (
                patch.object(quality_gate, "_static_checks", return_value={"ok": True}),
                patch.object(verify_offline, "git_snapshot", return_value={}),
            ):
                result = quality_gate.run(root, skip_coverage=True)
        self.assertFalse(result["ok"])
        self.assertTrue(result["validation"]["coverage"]["skipped"])
        self.assertFalse(result["validation"]["coverage"]["ok"])

    def test_failed_suite_exposes_diagnostic_and_test_identifiers(self) -> None:
        errors = quality_gate._error_list(
            {
                "ok": False,
                "diagnostic": "offline suite failed; inspect runner_output",
                "runner_output": {"failure_identifiers": ["tests.Bad.test_case"]},
            }
        )
        self.assertTrue(any("offline suite failed" in item for item in errors))
        self.assertTrue(any("tests.Bad.test_case" in item for item in errors))

    def test_gate_preserves_source_and_uses_ephemeral_state(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-preservation-") as name:
            root = Path(name)
            for directory in ("mtf_lab", "tests", "tools"):
                path = root / directory
                path.mkdir()
                (path / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
            before = verify_offline.source_manifest(root)
            with (
                patch.object(quality_gate, "_static_checks", return_value={"ok": True}),
                patch.object(quality_gate, "_coverage_checks", return_value={"ok": True}),
                patch.object(verify_offline, "git_snapshot", return_value={}),
            ):
                result = quality_gate.run(root, runtime_python="runtime", dev_python="dev", node="node")
            after = verify_offline.source_manifest(root)
            self.assertEqual(before["content_sha256"], after["content_sha256"])
        self.assertTrue(result["source_integrity"]["ok"])
        self.assertTrue(result["environment"]["isolated_state_dir"].startswith("<temporary>"))


class IsolationContracts(unittest.TestCase):
    def test_runner_overrides_state_and_drops_ambient_credentials(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-isolation-") as name:
            root = Path(name)
            temporary = root / "temporary"
            guard = temporary / "guard"
            with patch.dict(
                os.environ,
                {
                    "HOME": "/private/home",
                    "PYTHONPATH": "/private/pythonpath",
                    "CTRADER_CLIENT_SECRET": "private-secret",
                    "MTF_LAB_BUILD_PYTHON": "/private/dev-python",
                },
                clear=True,
            ):
                environment = offline_tests._isolated_env(root, temporary, guard, block_writes=False)
        self.assertTrue(Path(environment["HOME"]).is_relative_to(temporary))
        self.assertTrue(Path(environment["MTF_LAB_STATE_DIR"]).is_relative_to(temporary))
        self.assertTrue(Path(environment["TMPDIR"]).is_relative_to(temporary))
        self.assertNotIn("CTRADER_CLIENT_SECRET", environment)
        self.assertNotIn("/private/pythonpath", environment["PYTHONPATH"])
        self.assertEqual("/private/dev-python", environment["MTF_LAB_BUILD_PYTHON"])

    def test_runner_coverage_uses_isolated_child_without_mutating_fixture(self) -> None:
        if importlib.util.find_spec("coverage") is None:
            self.skipTest("Coverage.py no está instalado en el intérprete de esta prueba")
        with tempfile.TemporaryDirectory(prefix="mtf-quality-child-") as name:
            root = Path(name)
            (root / "mtf_lab").mkdir()
            (root / "mtf_lab" / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
            tests = root / "tests"
            tests.mkdir()
            fixture = tests / "test_tiny.py"
            fixture.write_text(
                "import unittest\n\nclass Tiny(unittest.TestCase):\n    def test_ok(self):\n        self.assertTrue(True)\n",
                encoding="utf-8",
            )
            before = fixture.read_bytes()
            coverage_file = root / "coverage" / ".coverage"
            coverage_spec = importlib.util.find_spec("coverage")
            self.assertIsNotNone(coverage_spec)
            assert coverage_spec is not None
            self.assertIsNotNone(coverage_spec.origin)
            assert coverage_spec.origin is not None
            coverage_site = Path(coverage_spec.origin).resolve().parent.parent
            result = offline_tests.run_suite(
                root,
                timeout=30,
                coverage_file=coverage_file,
                coverage_site=coverage_site,
            )
            self.assertTrue(result["summary"]["successful"], result)
            self.assertTrue(result["coverage"]["data_file_exists"], result)
            self.assertEqual(before, fixture.read_bytes())

    def test_missing_guard_log_is_not_reported_as_zero_network_attempts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-guard-evidence-") as name:
            root = Path(name)
            temporary = root / "temporary"
            completed = subprocess.CompletedProcess([], 0, "", "")
            with patch.object(offline_tests.subprocess, "run", return_value=completed):
                result = offline_tests._child_command(
                    [sys.executable, "-c", "pass"],
                    root=root,
                    temp_root=temporary,
                    block_writes=False,
                    timeout=1,
                )
        self.assertFalse(result["guard_evidence"]["runtime_log"])
        self.assertFalse(result["guard_evidence"]["write_log"])
        self.assertIsNone(result["network_attempts"])
        self.assertIn("complete diagnostic logs", result["guard_error"])


if __name__ == "__main__":
    unittest.main()
