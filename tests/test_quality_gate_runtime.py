"""Focused regressions for quality-tool interpreter separation.

The quality gate runs static tooling from the development environment while
the runtime environment remains the source of execution checks.  These tests
only inspect command construction and receipt isolation; they never launch
the repository-wide suite.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import quality_gate, verify_offline
from tools.quality_scope import discover_quality_scope


class QualityGateInterpreterContracts(unittest.TestCase):
    def test_mypy_resolves_stubs_from_dev_interpreter_not_runtime(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-runtime-contract-") as name:
            root = Path(name)
            for directory in ("mtf_lab", "tests", "tools"):
                path = root / directory
                path.mkdir()
                (path / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
            runtime = Path("/private/runtime-python")
            development = Path("/private/dev-python")
            scope = discover_quality_scope(root)
            calls: list[tuple[str, list[str], dict[str, str]]] = []

            def fake_run(
                name: str,
                command: list[str] | tuple[str, ...],
                *,
                environment: dict[str, str],
                **_kwargs: object,
            ) -> dict[str, object]:
                calls.append((name, list(command), dict(environment)))
                return {"ok": True, "name": name}

            with (
                patch.object(quality_gate, "_run_check", side_effect=fake_run),
                patch.object(verify_offline, "document_link_check", return_value={"ok": True}),
            ):
                result = quality_gate._static_checks(
                    runtime,
                    development,
                    root=root,
                    environment={"PATH": os.defpath},
                    temporary_root=root / "gate-tmp",
                    scope=scope,
                    timeout=1,
                )

        self.assertTrue(result["scope"]["ok"])
        mypy = next(command for name, command, _environment in calls if name == "mypy")
        self.assertEqual(str(development), mypy[0])
        executable_index = mypy.index("--python-executable")
        self.assertEqual(str(development), mypy[executable_index + 1])
        self.assertNotIn(str(runtime), mypy)
        self.assertIn("--strict", mypy)
        self.assertIn("--explicit-package-bases", mypy)
        self.assertIn("--no-incremental", mypy)
        self.assertEqual(["mtf_lab", "tools"], mypy[-2:])

        # The other interpreter roles remain unchanged: runtime owns compile,
        # dependency, and architecture probes; dev owns static tooling.
        compile_command = next(command for name, command, _environment in calls if name == "compilation")
        self.assertEqual(str(runtime), compile_command[0])
        pip_command = next(command for name, command, _environment in calls if name == "pip_check")
        self.assertEqual(str(runtime), pip_command[0])
        ruff_command = next(command for name, command, _environment in calls if name == "ruff")
        self.assertEqual(str(development), ruff_command[0])

    def test_quality_gate_environment_drops_ambient_paths_and_secrets(self) -> None:
        captured: dict[str, str] = {}

        def fake_static(*_args: object, environment: dict[str, str], **_kwargs: object) -> dict[str, object]:
            captured.update(environment)
            return {"ok": True}

        with tempfile.TemporaryDirectory(prefix="mtf-quality-environment-contract-") as name:
            root = Path(name)
            for directory in ("mtf_lab", "tests", "tools"):
                (root / directory).mkdir()
                (root / directory / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
            with (
                patch.dict(
                    os.environ,
                    {
                        "PYTHONPATH": "/private/ambient-source",
                        "MYPYPATH": "/private/ambient-stubs",
                        "CTRADER_CLIENT_SECRET": "do-not-copy",
                    },
                    clear=True,
                ),
                patch.object(quality_gate, "_static_checks", side_effect=fake_static),
                patch.object(quality_gate, "_coverage_checks", return_value={"ok": True}),
                patch.object(verify_offline, "git_snapshot", return_value={}),
            ):
                result = quality_gate.run(root, runtime_python="runtime", dev_python="dev", node="node")

        self.assertTrue(result["source_integrity"]["ok"])
        self.assertNotIn("PYTHONPATH", captured)
        self.assertNotIn("MYPYPATH", captured)
        self.assertNotIn("CTRADER_CLIENT_SECRET", captured)
        state_dir = Path(captured["MTF_LAB_STATE_DIR"])
        self.assertEqual(state_dir.parent / "home", Path(captured["HOME"]))

    def test_quality_gate_uses_project_dev_venv_for_nested_wheel_build(self) -> None:
        captured: dict[str, str] = {}

        def fake_static(*_args: object, environment: dict[str, str], **_kwargs: object) -> dict[str, object]:
            captured.update(environment)
            return {"ok": True}

        with tempfile.TemporaryDirectory(prefix="mtf-quality-build-python-") as name:
            root = Path(name)
            for directory in ("mtf_lab", "tests", "tools"):
                (root / directory).mkdir()
                (root / directory / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
            project_dev = root / ".venv-dev" / "bin" / "python"
            project_dev.parent.mkdir(parents=True)
            project_dev.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            project_dev.chmod(0o700)
            with (
                patch.object(quality_gate, "_static_checks", side_effect=fake_static),
                patch.object(quality_gate, "_coverage_checks", return_value={"ok": True}),
                patch.object(verify_offline, "git_snapshot", return_value={}),
            ):
                result = quality_gate.run(root, runtime_python="runtime", dev_python="dev", node="node")

        self.assertTrue(result["source_integrity"]["ok"])
        self.assertEqual(captured["MTF_LAB_BUILD_PYTHON"], str(project_dev))

    def test_failed_mypy_receipt_redacts_external_paths_and_secrets(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-quality-receipt-contract-") as name:
            root = Path(name)
            temporary = root / "temporary"
            outcome = verify_offline._CommandOutcome(
                ("/private/dev-python", "-m", "mypy", "--python-executable", "/private/dev-python"),
                1,
                "client-secret=do-not-persist\n/home/private/source/file.py",
                "MYPYPATH=/home/private/stubs\n",
                False,
                0.0,
            )
            with patch.object(verify_offline, "run_command", return_value=outcome):
                receipt = quality_gate._run_check(
                    "mypy",
                    outcome.command,
                    root=root,
                    environment={"PATH": os.defpath},
                    timeout=1,
                    temporary_root=temporary,
                )

        serialized = json.dumps(receipt, ensure_ascii=False)
        self.assertNotIn("do-not-persist", serialized)
        self.assertNotIn("/private/dev-python", serialized)
        self.assertNotIn("/home/private/source/file.py", serialized)
        self.assertFalse(receipt["ok"])


if __name__ == "__main__":
    unittest.main()
