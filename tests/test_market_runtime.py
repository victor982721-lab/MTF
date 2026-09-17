"""Focused tests for the private market-runtime preparer.

These tests exercise only archive/provenance and cache guards.  They do not
download packages, touch the staged user runtime, or open a broker connection.
"""

from __future__ import annotations

import errno
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile
from collections.abc import Iterator
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
        marker: dict[str, object] = {
            "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
            "project": "mtf-lab",
            "role": "review",
            "state": marker_state,
            "pid": None,
            "destination": str(runtime),
        }
        if marker_state == "BUILDING":
            marker.update({"pid": os.getpid(), "created_at": "2026-09-17T00:00:00Z"})
        elif marker_state == "REVIEW_READY":
            marker.update(
                {
                    "created_at": "2026-09-17T00:00:00Z",
                    "completed_at": "2026-09-17T00:01:00Z",
                    "manifest_sha256": runtime_lifecycle._sha256(runtime / runtime_lifecycle.STAGED_MARKER),
                    "manifest_state": "STAGED_RUNTIME",
                }
            )
        elif marker_state == "FAILED":
            marker.update(
                {
                    "failed_at": "2026-09-17T00:01:00Z",
                    "error_type": "RuntimeError",
                    "error": "synthetic fixture failure",
                }
            )
        _write_json(parent / runtime_lifecycle.LIFECYCLE_MARKER, marker)
    return runtime


def _make_active(root: Path) -> Path:
    active = root / "runtime"
    (active / "runtime-python/bin").mkdir(parents=True)
    (active / "dev-python/bin").mkdir(parents=True)
    (active / "base-python").mkdir()
    (active / "base-python/bin").mkdir()
    for name in ("runtime-python", "dev-python"):
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

    def test_gc_removes_failed_hidden_staging_payload(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            parent = root / "runtime-review-failed-staging"
            parent.mkdir()
            hidden = parent / ".runtime-staging-crashed"
            hidden.mkdir()
            (hidden / "partial.txt").write_text("partial\n", encoding="utf-8")
            _write_json(
                parent / runtime_lifecycle.LIFECYCLE_MARKER,
                {
                    "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
                    "project": "mtf-lab",
                    "role": "review",
                    "state": "FAILED",
                    "pid": None,
                    "destination": str(parent / "runtime"),
                    "failed_at": "2026-09-17T00:01:00Z",
                    "error_type": "RuntimeError",
                    "error": "synthetic fixture failure",
                },
            )
            with mock.patch.object(runtime_lifecycle, "_proc_references", return_value={}):
                result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0)
            self.assertEqual("deleted", result["deleted"][0]["action"])
            self.assertFalse(parent.exists())

    def test_sigkill_build_is_collected_after_stale_grace(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            child_code = """
import sys
import time
from pathlib import Path
from mtf_lab.ops.runtime_lifecycle import RuntimeLifecycle

destination = RuntimeLifecycle(Path(sys.argv[1]), stale_after_seconds=0).allocate_review()
print(destination, flush=True)
time.sleep(30)
"""
            child = subprocess.Popen(
                [sys.executable, "-c", child_code, str(root)],
                cwd=Path(__file__).resolve().parents[1],
                stdout=subprocess.PIPE,
                text=True,
            )
            try:
                assert child.stdout is not None
                destination = Path(child.stdout.readline().strip())
                self.assertTrue((destination.parent / runtime_lifecycle.LIFECYCLE_MARKER).is_file())
                child.kill()
                child.wait(timeout=10)
                result = runtime_lifecycle.RuntimeLifecycle(root, stale_after_seconds=0).gc(max_reviews=0)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
                if child.stdout is not None:
                    child.stdout.close()
            self.assertIn(destination.parent.name, {item["name"] for item in result["deleted"]})
            self.assertFalse(destination.parent.exists())

    def test_unmarked_unknown_review_is_not_purged_as_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            parent = root / "runtime-review-unknown"
            parent.mkdir()
            (parent / "keep.txt").write_text("unknown\n", encoding="utf-8")
            with mock.patch.object(runtime_lifecycle, "_proc_references", return_value={}):
                result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0, purge_review_evidence=True)
            preserved = next(item for item in result["preserved"] if item["name"] == parent.name)
            self.assertEqual("unknown", preserved["role"])
            self.assertEqual("preserve", preserved["action"])
            self.assertTrue(parent.exists())

    def test_corrupt_legacy_evidence_marker_is_not_purged(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            parent = root / "runtime-review-corrupt-evidence"
            parent.mkdir()
            _write_json(parent / runtime_lifecycle.STAGED_MARKER, {"project": "foreign"})
            (parent / "logs").mkdir()
            (parent / "logs" / "old.log").write_text("evidence\n", encoding="utf-8")
            with mock.patch.object(runtime_lifecycle, "_proc_references", return_value={}):
                result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0, purge_review_evidence=True)
            preserved = next(item for item in result["preserved"] if item["name"] == parent.name)
            self.assertEqual("unknown", preserved["role"])
            self.assertTrue(parent.exists())

    def test_unrecognized_evidence_file_is_not_purged(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            parent = root / "runtime-review-evidence-unknown-file"
            (parent / "logs").mkdir(parents=True)
            (parent / "logs" / "keep.bin").write_bytes(b"unknown\n")
            with mock.patch.object(runtime_lifecycle, "_proc_references", return_value={}):
                result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0, purge_review_evidence=True)
            preserved = next(item for item in result["preserved"] if item["name"] == parent.name)
            self.assertEqual("unknown", preserved["role"])
            self.assertTrue(parent.exists())

    def test_foreign_rollback_metadata_is_not_collectable(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            rollback = root / "runtime-rollback-foreign"
            rollback.mkdir()
            (rollback / "keep.bin").write_bytes(b"foreign\n")
            _write_json(
                rollback / runtime_lifecycle.STAGED_MARKER,
                {
                    "project": "mtf-lab",
                    "state": "ROLLBACK_RUNTIME",
                    "promotion_state": "ROLLBACK",
                },
            )
            with mock.patch.object(runtime_lifecycle, "_proc_references", return_value={}):
                result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_rollbacks=0)
            preserved = next(item for item in result["preserved"] if item["name"] == rollback.name)
            self.assertEqual("unknown", preserved["role"])
            self.assertTrue(rollback.exists())

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
                internal_target = root / "market-data"
                internal_target.mkdir()
                internal_destination = _make_review(root, "runtime-review-cross-tree")
                (internal_destination / "cross-tree").symlink_to(internal_target, target_is_directory=True)
                manager = runtime_lifecycle.RuntimeLifecycle(root)
                result = manager.gc(max_reviews=0)
                names = {item["name"] for item in result["preserved"]}
                self.assertIn("runtime-review-symlink", names)
                self.assertIn("runtime-review-escape", names)
                self.assertIn("runtime-review-cross-tree", names)
                with self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                    manager.validate_build_destination(root / ".." / "outside" / "runtime-review-x" / "runtime")
            finally:
                outside.rmdir()

    def test_remove_tree_rejects_top_level_symlink_without_touching_target(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            outside = root / "outside"
            outside.mkdir()
            sentinel = outside / "keep.txt"
            sentinel.write_text("keep\n", encoding="utf-8")
            link = root / "runtime-review-link"
            link.symlink_to(outside, target_is_directory=True)
            with self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                runtime_lifecycle._remove_tree(link)
            self.assertTrue(sentinel.exists())
            self.assertTrue(link.is_symlink())

    def test_shared_hardlink_payload_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            outside = root / "outside.bin"
            outside.write_bytes(b"shared\n")
            candidate = _make_review(root, "runtime-review-hardlink")
            os.link(outside, candidate / "shared.bin")
            with mock.patch.object(runtime_lifecycle, "_proc_references", return_value={}):
                result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0)
            preserved = next(item for item in result["preserved"] if item["name"] == candidate.parent.name)
            self.assertEqual("unknown", preserved["role"])
            self.assertTrue(outside.exists())
            self.assertEqual(b"shared\n", outside.read_bytes())

    def test_incomplete_managed_layout_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            parent = root / "runtime-review-incomplete"
            runtime = parent / "runtime"
            runtime.mkdir(parents=True)
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
            result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0)
            preserved = next(item for item in result["preserved"] if item["name"] == parent.name)
            self.assertEqual("unknown", preserved["role"])
            self.assertEqual("preserve", preserved["action"])
            self.assertTrue(runtime.is_dir())

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

            with (
                mock.patch("mtf_lab.ops.runtime_lifecycle.os.rename", side_effect=fail_second),
                self.assertRaises(OSError),
            ):
                runtime_lifecycle.RuntimeLifecycle(root).promote(candidate)
            self.assertTrue(active.is_dir())
            self.assertTrue(candidate.is_dir())
            # A subsequent lifecycle operation reconciles the journal rather
            # than silently deleting either side of the failed transaction.
            runtime_lifecycle.RuntimeLifecycle(root).inspect()
            self.assertFalse((root / ".runtime-promotion.json").exists())

    def test_late_promotion_cleanup_failure_recovers_committed_state(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            _make_active(root)
            candidate = _make_review(root, "runtime-review-late-cleanup")
            manager = runtime_lifecycle.RuntimeLifecycle(root)

            def fail_review_rmdir(_root: Path, path: Path) -> None:
                if path == candidate.parent:
                    raise OSError("synthetic marker cleanup failure")

            with (
                mock.patch.object(
                    runtime_lifecycle,
                    "_rmdir_child",
                    side_effect=fail_review_rmdir,
                ),
                self.assertRaises(OSError),
            ):
                manager.promote(candidate)
            self.assertTrue((root / ".runtime-promotion.json").exists())
            self.assertTrue((root / "runtime" / "payload.txt").is_file())
            rollback_dirs = list(root.glob("runtime-rollback-*"))
            self.assertEqual(1, len(rollback_dirs))
            self.assertTrue((rollback_dirs[0] / runtime_lifecycle.STAGED_MARKER).is_file())
            recovered = manager.inspect()
            self.assertFalse((root / ".runtime-promotion.json").exists())
            self.assertTrue((root / "runtime" / "payload.txt").is_file())
            self.assertFalse(candidate.parent.exists())
            rollback_records = [item for item in recovered["records"] if item["role"] == "rollback"]
            self.assertEqual(1, len(rollback_records))

    def test_failed_review_is_not_promotable(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            candidate = _make_review(root, "runtime-review-failed", marker_state="FAILED")
            with self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                runtime_lifecycle.RuntimeLifecycle(root).promote(candidate)
            self.assertTrue(candidate.parent.is_dir())

    def test_invalid_active_manifest_is_rejected_before_promotion_journal(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = _make_active(root)
            (active / runtime_lifecycle.STAGED_MARKER).unlink()
            candidate = _make_review(root, "runtime-review-invalid-active")
            with self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                runtime_lifecycle.RuntimeLifecycle(root).promote(candidate)
            self.assertTrue(active.is_dir())
            self.assertTrue(candidate.is_dir())
            self.assertFalse((root / ".runtime-promotion.json").exists())

    def test_promotion_protects_active_runtime_in_use(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = _make_active(root)
            candidate = _make_review(root, "runtime-review-active-live")
            child = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(2)"],
                cwd=active,
            )
            try:
                with self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                    runtime_lifecycle.RuntimeLifecycle(root).promote(candidate)
                self.assertTrue((active / "active.txt").is_file())
                self.assertTrue(candidate.is_dir())
            finally:
                child.wait(timeout=10)

    def test_unknown_marker_cannot_be_reused(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            parent = root / "runtime-review-unknown-marker"
            parent.mkdir()
            marker = parent / runtime_lifecycle.LIFECYCLE_MARKER
            original: dict[str, object] = {
                "schema": "other",
                "project": "other",
                "role": "review",
                "state": "UNKNOWN",
                "destination": str(parent / "runtime"),
            }
            _write_json(marker, original)
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            with manager.lock(), self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                manager.begin_unlocked(parent / "runtime")
            self.assertEqual(original, json.loads(marker.read_text(encoding="utf-8")))

    def test_incomplete_ready_marker_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            candidate = _make_review(root, "runtime-review-incomplete-marker")
            marker = candidate.parent / runtime_lifecycle.LIFECYCLE_MARKER
            malformed = json.loads(marker.read_text(encoding="utf-8"))
            malformed.pop("manifest_sha256")
            _write_json(marker, malformed)
            with mock.patch.object(runtime_lifecycle, "_proc_references", return_value={}):
                result = runtime_lifecycle.RuntimeLifecycle(root).gc(max_reviews=0)
            preserved = next(item for item in result["preserved"] if item["name"] == candidate.parent.name)
            self.assertEqual("unknown", preserved["role"])
            self.assertEqual("preserve", preserved["action"])
            self.assertTrue(candidate.parent.exists())

    def test_ready_marker_hash_mismatch_is_preserved_and_not_promotable(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            candidate = _make_review(root, "runtime-review-tampered-manifest")
            manifest_path = candidate / runtime_lifecycle.STAGED_MARKER
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["tampered"] = True
            _write_json(manifest_path, manifest)
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            with mock.patch.object(runtime_lifecycle, "_proc_references", return_value={}):
                record = next(item for item in manager.inspect()["records"] if item["name"] == candidate.parent.name)
                self.assertEqual("unknown", record["role"])
                self.assertFalse(record["safe_to_delete"])
            with self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                manager.promote(candidate)

    def test_process_descriptor_permission_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            real_iterdir = Path.iterdir
            process_path = Path(f"/proc/{os.getpid()}")

            def deny_proc_fd(path: Path) -> Iterator[Path]:
                if path == Path("/proc"):
                    return iter([process_path])
                if path == process_path / "fd":
                    raise PermissionError(errno.EACCES, "synthetic proc denial", str(path))
                return real_iterdir(path)

            with (
                mock.patch.object(Path, "iterdir", deny_proc_fd),
                mock.patch.object(runtime_lifecycle, "_proc_is_non_mtf", return_value=False),
                self.assertRaises(runtime_lifecycle.RuntimeLifecycleError),
            ):
                runtime_lifecycle._proc_references([root / "candidate"])

    def test_opaque_process_with_empty_command_line_is_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            real_iterdir = Path.iterdir
            process_path = Path(f"/proc/{os.getpid()}")

            def deny_proc_fd(path: Path) -> Iterator[Path]:
                if path == Path("/proc"):
                    return iter([process_path])
                if path == process_path / "fd":
                    raise PermissionError(errno.EACCES, "synthetic proc denial", str(path))
                return real_iterdir(path)

            with (
                mock.patch.object(Path, "iterdir", deny_proc_fd),
                mock.patch.object(runtime_lifecycle, "_proc_commandline", return_value=""),
                self.assertRaises(runtime_lifecycle.RuntimeLifecycleError),
            ):
                runtime_lifecycle._proc_references([root / "candidate"])

    def test_incomplete_process_scan_preserves_review_and_allows_inspect(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            candidate = _make_review(root, "runtime-review-proc-incomplete")
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            with mock.patch.object(
                runtime_lifecycle,
                "_proc_references",
                side_effect=runtime_lifecycle.RuntimeLifecycleError("synthetic process scan denial"),
            ):
                result = manager.gc(max_reviews=0)
                self.assertTrue(candidate.parent.exists())
                preserved = next(item for item in result["preserved"] if item["name"] == candidate.parent.name)
                self.assertEqual("needs_review", preserved["action"])
                inspected = manager.inspect()
            self.assertEqual("INCOMPLETE", inspected["process_scan"]["status"])

    def test_recovery_restores_active_after_interrupted_move(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = _make_active(root)
            staged = _make_review(root, "runtime-review-recovery")
            rollback = root / "runtime-rollback-recovery"
            os.rename(active, rollback)
            os.rename(staged, active)
            journal = root / ".runtime-promotion.json"
            runtime_lifecycle._atomic_json(
                journal,
                {
                    "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
                    "state": "APPLYING",
                    "active": str(root / "runtime"),
                    "staged": str(staged),
                    "rollback": str(rollback),
                },
            )
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            with manager.lock():
                recovered = manager.recover_unlocked()
            self.assertTrue(recovered["recovered"])
            self.assertTrue((active / "active.txt").is_file())
            self.assertTrue(staged.parent.is_dir())
            self.assertFalse(journal.exists())

    def test_recovery_restores_legacy_active_after_first_rename(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = _make_active(root)
            legacy_manifest = json.loads((active / runtime_lifecycle.STAGED_MARKER).read_text(encoding="utf-8"))
            legacy_manifest.update(
                {"state": "STAGED_RUNTIME", "promotion_state": "NOT_PROMOTED", "current_pointer": None}
            )
            _write_json(active / runtime_lifecycle.STAGED_MARKER, legacy_manifest)
            staged = _make_review(root, "runtime-review-legacy-recovery")
            rollback = root / "runtime-rollback-legacy-recovery"
            os.rename(active, rollback)
            journal = root / ".runtime-promotion.json"
            runtime_lifecycle._atomic_json(
                journal,
                {
                    "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
                    "state": "PREPARED",
                    "active": str(root / "runtime"),
                    "staged": str(staged),
                    "rollback": str(rollback),
                },
            )
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            with manager.lock():
                recovered = manager.recover_unlocked()
            self.assertTrue(recovered["recovered"])
            self.assertTrue((root / "runtime" / "active.txt").is_file())
            self.assertTrue(staged.parent.is_dir())
            self.assertFalse(journal.exists())

    def test_recovery_restores_first_promotion_candidate_without_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            staged = _make_review(root, "runtime-review-first-promotion")
            active = root / "runtime"
            os.rename(staged, active)
            journal = root / ".runtime-promotion.json"
            runtime_lifecycle._atomic_json(
                journal,
                {
                    "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
                    "state": "APPLYING",
                    "active": str(active),
                    "staged": str(staged),
                    "rollback": str(root / "runtime-rollback-first-promotion"),
                },
            )
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            with manager.lock():
                recovered = manager.recover_unlocked()
            self.assertTrue(recovered["recovered"])
            self.assertFalse(active.exists())
            self.assertTrue(staged.is_dir())
            self.assertFalse(journal.exists())

    def test_recovery_finalizes_rollback_after_candidate_activation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            active = _make_active(root)
            staged = _make_review(root, "runtime-review-finalize-rollback")
            rollback = root / "runtime-rollback-finalize-rollback"
            os.rename(active, rollback)
            os.rename(staged, root / "runtime")
            runtime_lifecycle._retarget_venv_configs(root / "runtime")
            promoted = json.loads((root / "runtime" / runtime_lifecycle.STAGED_MARKER).read_text(encoding="utf-8"))
            promoted.update(
                {
                    "state": "ACTIVE_RUNTIME",
                    "promotion_state": "ACTIVE",
                    "current_pointer": str(root / "runtime"),
                    "destination": str(root / "runtime"),
                }
            )
            _write_json(root / "runtime" / runtime_lifecycle.STAGED_MARKER, promoted)
            journal = root / ".runtime-promotion.json"
            runtime_lifecycle._atomic_json(
                journal,
                {
                    "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
                    "state": "APPLYING",
                    "active": str(root / "runtime"),
                    "staged": str(staged),
                    "rollback": str(rollback),
                },
            )
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            with manager.lock():
                recovered = manager.recover_unlocked()
            self.assertTrue(recovered["recovered"])
            rollback_manifest = json.loads((rollback / runtime_lifecycle.STAGED_MARKER).read_text(encoding="utf-8"))
            self.assertEqual("ROLLBACK_RUNTIME", rollback_manifest["state"])
            self.assertEqual("ROLLBACK", rollback_manifest["promotion_state"])
            rollback_record = next(item for item in manager.inspect()["records"] if item["name"] == rollback.name)
            self.assertEqual("rollback", rollback_record["role"])
            self.assertFalse(journal.exists())

    def test_unknown_promotion_journal_state_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            journal = root / ".runtime-promotion.json"
            runtime_lifecycle._atomic_json(
                journal,
                {
                    "schema": runtime_lifecycle.LIFECYCLE_SCHEMA,
                    "state": "UNKNOWN",
                    "active": str(root / "runtime"),
                    "staged": str(root / "runtime-review-x" / "runtime"),
                    "rollback": str(root / "runtime-rollback-x"),
                },
            )
            manager = runtime_lifecycle.RuntimeLifecycle(root)
            with manager.lock(), self.assertRaises(runtime_lifecycle.RuntimeLifecycleError):
                manager.recover_unlocked()
            self.assertTrue(journal.exists())

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
                    "pid": None,
                    "destination": str(parent / "runtime"),
                    "failed_at": "2026-09-17T00:01:00Z",
                    "error_type": "RuntimeError",
                    "error": "synthetic fixture failure",
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

    def test_prepare_wrapper_does_not_touch_preexisting_unknown_destination(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "runtime-root"
            destination = root / "runtime-review-foreign" / "runtime"
            destination.mkdir(parents=True)
            payload = destination / "keep.bin"
            payload.write_bytes(b"foreign bytes\n")
            marker = destination.parent / runtime_lifecycle.LIFECYCLE_MARKER
            marker.write_text('{"schema":"foreign","state":"UNKNOWN"}\n', encoding="utf-8")
            payload_before = payload.read_bytes()
            marker_before = marker.read_bytes()
            with self.assertRaises(PREPARER.PreparationError):
                PREPARER.prepare_runtime(
                    repo_root=Path(__file__).resolve().parents[1],
                    destination=destination,
                    lifecycle_root=root,
                )
            self.assertEqual(payload_before, payload.read_bytes())
            self.assertEqual(marker_before, marker.read_bytes())

    def test_prepare_wrapper_reconciles_previous_review_after_success(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "runtime-root"
            old = _make_review(root, "runtime-review-old")
            os.utime(old.parent, (1, 1))
            destination = root / "runtime-review-new" / "runtime"

            def fake_prepare(**kwargs: object) -> dict[str, object]:
                target = Path(str(kwargs["destination"]))
                target.mkdir(parents=True)
                for relative in ("base-python", "runtime-python", "dev-python"):
                    (target / relative).mkdir()
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
