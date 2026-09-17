"""Focused tests for the private market-runtime preparer.

These tests exercise only archive/provenance and cache guards.  They do not
download packages, touch the staged user runtime, or open a broker connection.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from types import ModuleType
from unittest import mock

from mtf_lab.ops import runtime_lifecycle


def _load_preparer() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "tools/prepare_market_runtime.py"
    spec = importlib.util.spec_from_file_location("prepare_market_runtime_under_test", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load runtime preparer")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


PREPARER = _load_preparer()


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _make_review(root: Path, name: str, *, marker_state: str | None = "REVIEW_READY") -> Path:
    parent = root / name
    runtime = parent / "runtime"
    (runtime / "base-python/bin").mkdir(parents=True)
    (runtime / "runtime-python/bin").mkdir(parents=True)
    (runtime / "dev-python/bin").mkdir(parents=True)
    for name in ("runtime-python", "dev-python"):
        (runtime / name / "pyvenv.cfg").write_text(
            "home = /old/base/bin\ninclude-system-site-packages = false\nversion = 3.12.14\n",
            encoding="utf-8",
        )
    (runtime / "payload.txt").write_text(name + "\n", encoding="utf-8")
    _write_json(
        runtime / runtime_lifecycle.STAGED_MARKER,
        {
            "project": "mtf-lab",
            "state": "STAGED_RUNTIME",
            "promotion_state": "NOT_PROMOTED",
            "current_pointer": None,
            "destination": str(runtime),
        },
    )
    if marker_state is not None:
        _write_json(
            parent / runtime_lifecycle.LIFECYCLE_MARKER,
            {
                "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
                "project": "mtf-lab",
                "role": "review",
                "state": marker_state,
                "pid": None,
                "destination": str(runtime),
            },
        )
    return runtime


def _make_active(root: Path) -> Path:
    active = root / "runtime"
    (active / "runtime-python/bin").mkdir(parents=True)
    (active / "base-python").mkdir()
    (active / "base-python/bin").mkdir()
    for name in ("runtime-python",):
        (active / name / "pyvenv.cfg").write_text(
            "home = /old/base/bin\ninclude-system-site-packages = false\nversion = 3.12.14\n",
            encoding="utf-8",
        )
    (active / "active.txt").write_text("active\n", encoding="utf-8")
    _write_json(
        active / runtime_lifecycle.STAGED_MARKER,
        {
            "project": "mtf-lab",
            "state": "ACTIVE_RUNTIME",
            "promotion_state": "ACTIVE",
            "current_pointer": str(active),
            "destination": str(active),
        },
    )
    return active


class MarketRuntimePreparationTests(unittest.TestCase):
    def test_archive_provenance_preserves_license_bytes_and_hash(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            wheel = root / "demo-1.0-py3-none-any.whl"
            license_bytes = b"demo license bytes\n"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr(
                    "demo-1.0.dist-info/METADATA",
                    "Metadata-Version: 2.1\nName: demo\nVersion: 1.0\nLicense: MIT\n",
                )
                archive.writestr("demo-1.0.dist-info/WHEEL", "Wheel-Version: 1.0\n")
                archive.writestr("demo-1.0.dist-info/LICENSE", license_bytes)
            record = PREPARER.preserve_archive(
                wheel,
                root=root,
                relative_path="dependencies/demo.whl",
                origin="test-local",
                kind="test",
            )
            self.assertEqual("demo", record["name"])
            self.assertEqual("1.0", record["version"])
            self.assertEqual(PREPARER.sha256_file(root / "dependencies/demo.whl"), record["sha256"])
            license_path = root / record["license_files"][0]
            self.assertEqual(license_bytes, license_path.read_bytes())

    def test_cache_guard_reports_clean_private_tree(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "module.py").write_text("print('ok')\n", encoding="utf-8")
            observed = PREPARER.validate_no_caches(root, source_marker="/not-a-cache/")
            self.assertEqual([], observed["pycache_dirs"])
            self.assertEqual([], observed["external_symlinks"])
            self.assertEqual([], observed["source_cache_hits"])

    def test_cache_removal_is_limited_to_regenerable_files(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            cache = root / "pkg/__pycache__"
            cache.mkdir(parents=True)
            (cache / "module.cpython-312.pyc").write_bytes(b"pyc")
            (root / "keep.txt").write_text("keep\n", encoding="utf-8")
            removed = PREPARER.remove_regenerable_caches(root)
            self.assertEqual(1, removed["removed_pycache_dirs"])
            self.assertFalse(cache.exists())
            self.assertTrue((root / "keep.txt").is_file())

    def test_source_cache_payload_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            source_marker = "/fixture/source-cache/"
            (root / "bad.txt").write_text(f"{source_marker}\n", encoding="utf-8")
            with self.assertRaises(PREPARER.PreparationError):
                PREPARER.validate_no_caches(root, source_marker=source_marker)

    def test_runtime_source_defaults_are_portable(self) -> None:
        expected_root = Path.home() / ".cache" / "codex-runtimes"
        self.assertEqual(
            PREPARER.DEFAULT_SOURCE_PYTHON,
            expected_root / "codex-primary-runtime/dependencies/python/bin/python3",
        )
        self.assertEqual(PREPARER.SOURCE_CACHE_MARKER, f"{expected_root}{os.sep}")

    def test_current_pointer_gate_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            destination = Path(raw) / "runtime"
            destination.mkdir()
            (destination / "CURRENT").write_text("not used\n", encoding="utf-8")
            with self.assertRaises(PREPARER.PreparationError):
                PREPARER._assert_no_current(destination)

    def test_source_version_gate_is_numeric(self) -> None:
        self.assertGreaterEqual(PREPARER._parse_version("3.53.1"), (3, 51, 3))
        self.assertLess(PREPARER._parse_version("3.50.9"), (3, 51, 3))

    def test_venv_config_uses_absolute_base_for_isolated_mode(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            base = root / "base-python"
            venv = root / "runtime-python"
            (base / "bin").mkdir(parents=True)
            (venv / "bin").mkdir(parents=True)
            PREPARER.retarget_venv(venv, base, python_version="3.12.14")
            config = (venv / "pyvenv.cfg").read_text(encoding="utf-8")
            self.assertIn(f"home = {base.resolve() / 'bin'}\n", config)
            self.assertIn("command = python3.12 -m venv", config)

    def test_python_wrapper_does_not_strip_pythonpath(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            bin_dir = root / "runtime-python/bin"
            bin_dir.mkdir(parents=True)
            PREPARER._python_wrapper(bin_dir / "python")
            wrapper = (bin_dir / "python").read_text(encoding="utf-8")
            self.assertNotIn("unset PYTHONPATH", wrapper)
            self.assertIn('export PYTHONHOME="$ROOT/base-python"', wrapper)

    def test_patch_entrypoints_rewrites_pip_shell_polyglot_for_relocation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            bin_dir = root / "runtime-python/bin"
            bin_dir.mkdir(parents=True)
            script = bin_dir / "mtf-lab"
            script.write_text(
                "#!/bin/sh\n"
                "'''exec' /staging/runtime-python/bin/.python3.12.bin \"$0\" \"$@\"\n"
                "' '''\n"
                "import sys\n"
                "sys.exit(0)\n",
                encoding="utf-8",
            )
            script.chmod(0o700)
            moved = PREPARER.patch_entrypoints(root / "runtime-python", root=root, venv_name="runtime-python")
            self.assertEqual(["mtf-lab"], moved)
            wrapper = script.read_text(encoding="utf-8")
            self.assertIn('exec "$SELF/.python3.12.bin" "$ROOT/libexec/runtime-python/', wrapper)
            body = root / "libexec/runtime-python/__mtf_entrypoint_mtf-lab.py"
            self.assertTrue(body.is_file())
            self.assertIn("/staging/runtime-python/bin/.python3.12.bin", body.read_text(encoding="utf-8"))

    def test_command_result_serializes_without_environment(self) -> None:
        result = PREPARER.CommandResult(("python", "-V"), 0, 0.25, "runtime.log")
        payload = json.loads(json.dumps(result.as_dict()))
        self.assertEqual(["python", "-V"], payload["argv"])
        self.assertNotIn("HOME", payload)

    def test_review_allocation_and_completion_are_managed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manager = runtime_lifecycle.RuntimeLifecycle(raw, stale_after_seconds=0)
            destination = manager.allocate_review()
            self.assertTrue(destination.parent.name.startswith("runtime-review-"))
            building = manager.inspect()["records"][0]
            self.assertEqual("temporary_in_use", building["role"])
            (destination / "runtime-python/bin").mkdir(parents=True)
            (destination / "dev-python/bin").mkdir(parents=True)
            (destination / "base-python").mkdir()
            _write_json(
                destination / runtime_lifecycle.STAGED_MARKER,
                {
                    "project": "mtf-lab",
                    "state": "STAGED_RUNTIME",
                    "promotion_state": "NOT_PROMOTED",
                    "current_pointer": None,
                    "destination": str(destination),
                },
            )
            with manager.lock():
                manager.mark_completed_unlocked(destination, {"state": "STAGED_RUNTIME"})
            record = next(item for item in manager.inspect()["records"] if item["name"] == destination.parent.name)
            self.assertEqual("review", record["role"])
            self.assertTrue(record["safe_to_delete"])

    def test_gc_deletes_only_valid_reviews_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = _make_active(root)
            first = _make_review(root, "runtime-review-20260101-0000")
            second = _make_review(root, "runtime-review-20260102-0000")
            (root / "runtime-review-unknown").mkdir()
            (root / "runtime-review-unknown" / "keep.txt").write_text("keep\n", encoding="utf-8")
            (root / "notes").mkdir()
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            result = manager.gc(max_reviews=0)
            self.assertEqual(
                {"runtime-review-20260101-0000", "runtime-review-20260102-0000"},
                {item["name"] for item in result["deleted"]},
            )
            self.assertTrue(active.is_dir())
            self.assertTrue(first.parent.exists() is False)
            self.assertTrue(second.parent.exists() is False)
            self.assertTrue((root / "runtime-review-unknown").is_dir())
            self.assertEqual([], manager.gc(max_reviews=0)["deleted"])

    def test_gc_retains_newest_review_by_policy(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            oldest = _make_review(root, "runtime-review-20260101-0000")
            newest = _make_review(root, "runtime-review-20260102-0000")
            os.utime(oldest.parent, (1, 1))
            os.utime(newest.parent, (2, 2))
            manager = runtime_lifecycle.RuntimeLifecycle(root, keep_reviews=1)
            result = manager.gc()
            self.assertEqual(["runtime-review-20260101-0000"], [item["name"] for item in result["deleted"]])
            self.assertTrue(newest.parent.is_dir())

    def test_gc_preserves_legacy_evidence_unless_explicitly_purged(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            destination = _make_review(root, "runtime-review-with-evidence")
            evidence = destination.parent / "logs"
            evidence.mkdir()
            (evidence / "validation.log").write_text("receipt\n", encoding="utf-8")
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            result = manager.gc(max_reviews=0)
            self.assertEqual("payload_deleted", result["deleted"][0]["action"])
            self.assertTrue(evidence.is_dir())
            result = manager.gc(max_reviews=0, purge_review_evidence=True)
            self.assertEqual("deleted", result["deleted"][0]["action"])
            self.assertFalse(destination.parent.exists())

    def test_gc_protects_live_process_and_deletes_after_exit(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            destination = _make_review(root, "runtime-review-live")
            child = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(1)"],
                cwd=destination,
            )
            try:
                result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0)
                preserved = next(item for item in result["preserved"] if item["name"] == "runtime-review-live")
                self.assertEqual("needs_review", preserved["action"])
                self.assertTrue(destination.parent.is_dir())
            finally:
                child.wait(timeout=10)
            result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0)
            self.assertEqual("runtime-review-live", result["deleted"][0]["name"])

    def test_unknown_and_symlink_trees_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            outside = Path(raw).parent / (Path(raw).name + "-outside")
            outside.mkdir()
            try:
                (root / "runtime-review-symlink").symlink_to(outside, target_is_directory=True)
                destination = _make_review(root, "runtime-review-escape")
                (destination / "escape").symlink_to(outside, target_is_directory=True)
                manager = runtime_lifecycle.RuntimeLifecycle(root)
                result = manager.gc(max_reviews=0)
                names = {item["name"] for item in result["preserved"]}
                self.assertIn("runtime-review-symlink", names)
                self.assertIn("runtime-review-escape", names)
                with self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                    manager.validate_build_destination(root / ".." / "outside" / "runtime-review-x" / "runtime")
            finally:
                outside.rmdir()

    def test_promotion_keeps_one_real_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = _make_active(root)
            candidate = _make_review(root, "runtime-review-promote")
            result = runtime_lifecycle.RuntimeLifecycle(root).promote(candidate)
            self.assertEqual("PROMOTED", result["state"])
            self.assertTrue((root / "runtime" / "payload.txt").is_file())
            rollback = Path(result["rollback"])
            self.assertTrue(rollback.is_dir())
            self.assertTrue((rollback / "active.txt").is_file())
            self.assertFalse(candidate.parent.exists())
            self.assertFalse((active / "active.txt").exists())
            self.assertIn(
                f"home = {root / 'runtime' / 'base-python' / 'bin'}",
                (root / "runtime/runtime-python/pyvenv.cfg").read_text(),
            )
            self.assertIn(
                f"home = {rollback / 'base-python' / 'bin'}", (rollback / "runtime-python/pyvenv.cfg").read_text()
            )
            rollback_record = next(
                item
                for item in runtime_lifecycle.RuntimeLifecycle(root).inspect()["records"]
                if item["name"] == rollback.name
            )
            self.assertEqual("rollback", rollback_record["role"])

    def test_promotion_failure_restores_active_and_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = _make_active(root)
            candidate = _make_review(root, "runtime-review-fail")
            real_rename = os.rename
            calls = 0

            def fail_second(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> None:
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("synthetic promotion failure")
                real_rename(source, target)

            with mock.patch.object(runtime_lifecycle.os, "rename", side_effect=fail_second), self.assertRaises(OSError):
                runtime_lifecycle.RuntimeLifecycle(root).promote(candidate)
            self.assertTrue(active.is_dir())
            self.assertTrue(candidate.is_dir())
            # A subsequent lifecycle operation reconciles the journal rather
            # than silently deleting either side of the failed transaction.
            runtime_lifecycle.RuntimeLifecycle(root).inspect()
            self.assertFalse((root / ".runtime-promotion.json").exists())

    def test_failed_review_without_payload_is_collectable(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            parent = root / "runtime-review-failed"
            parent.mkdir()
            _write_json(
                parent / runtime_lifecycle.LIFECYCLE_MARKER,
                {
                    "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
                    "project": "mtf-lab",
                    "role": "review",
                    "state": "FAILED",
                    "destination": str(parent / "runtime"),
                },
            )
            result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0)
            self.assertEqual("runtime-review-failed", result["deleted"][0]["name"])

    def test_prepare_wrapper_rejects_destination_outside_managed_root(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "runtime-root"
            destination = Path(raw) / "elsewhere" / "runtime-review-x" / "runtime"
            with self.assertRaises(PREPARER.PreparationError):
                PREPARER.prepare_runtime(
                    repo_root=Path(__file__).resolve().parents[1],
                    destination=destination,
                    lifecycle_root=root,
                )

    def test_prepare_wrapper_removes_failed_payload(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "runtime-root"
            destination = root / "runtime-review-failed" / "runtime"

            def fake_prepare(**kwargs: object) -> dict[str, object]:
                target = Path(str(kwargs["destination"]))
                target.mkdir(parents=True)
                (target / "partial.txt").write_text("partial\n", encoding="utf-8")
                raise RuntimeError("synthetic validation failure")

            with (
                mock.patch.object(PREPARER, "_prepare_runtime_impl", side_effect=fake_prepare),
                self.assertRaises(RuntimeError),
            ):
                PREPARER.prepare_runtime(
                    repo_root=Path(__file__).resolve().parents[1],
                    destination=destination,
                    lifecycle_root=root,
                )
            self.assertFalse(destination.parent.exists())

    def test_prepare_wrapper_reconciles_previous_review_after_success(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "runtime-root"
            old = _make_review(root, "runtime-review-old")
            os.utime(old.parent, (1, 1))
            destination = root / "runtime-review-new" / "runtime"

            def fake_prepare(**kwargs: object) -> dict[str, object]:
                target = Path(str(kwargs["destination"]))
                target.mkdir(parents=True)
                _write_json(
                    target / runtime_lifecycle.STAGED_MARKER,
                    {
                        "project": "mtf-lab",
                        "state": "STAGED_RUNTIME",
                        "promotion_state": "NOT_PROMOTED",
                        "current_pointer": None,
                        "destination": str(target),
                    },
                )
                return {"state": "STAGED_RUNTIME"}

            with mock.patch.object(PREPARER, "_prepare_runtime_impl", side_effect=fake_prepare):
                PREPARER.prepare_runtime(
                    repo_root=Path(__file__).resolve().parents[1],
                    destination=destination,
                    lifecycle_root=root,
                )
            self.assertTrue(destination.parent.is_dir())
            self.assertFalse(old.parent.exists())

    def test_lifecycle_lock_serializes_processes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "runtime-root"
            started_path = Path(raw) / "child-started"
            script = (
                "import sys,time; "
                "from mtf_lab.ops.runtime_lifecycle import RuntimeLifecycle; "
                "m=RuntimeLifecycle(sys.argv[1]); "
                "p=__import__('pathlib').Path(sys.argv[2]); "
                "\nwith m.lock(): p.write_text('ready'); time.sleep(0.45)"
            )
            child = subprocess.Popen([sys.executable, "-c", script, str(root), str(started_path)])
            deadline = time.monotonic() + 5
            while not started_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(started_path.exists())
            started = time.monotonic()
            with runtime_lifecycle.RuntimeLifecycle(root).lock():
                waited = time.monotonic() - started
            child.wait(timeout=5)
            self.assertGreaterEqual(waited, 0.25)


if __name__ == "__main__":
    unittest.main()
