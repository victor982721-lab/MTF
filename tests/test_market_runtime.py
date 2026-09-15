"""Focused tests for the private market-runtime preparer.

These tests exercise only archive/provenance and cache guards.  They do not
download packages, touch the staged user runtime, or open a broker connection.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import ModuleType


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


if __name__ == "__main__":
    unittest.main()
