"""Regression checks for clone-clean packaging and offline entry points."""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import tomllib
import unittest
import zipfile
from pathlib import Path
from unittest import mock

import mtf_lab.configuration as configuration
from mtf_lab.configuration import default_state_dir, load_config, packaged_config_path
from mtf_lab.ops import cli
from mtf_lab.ops.application_services import default_config, default_watch_config
from tools import wheel_installer

ROOT = Path(__file__).resolve().parents[1]
CONFIG_NAMES = {
    "default.toml",
    "kraken.toml",
    "fixture_cfd.toml",
    "ctrader_query.toml",
    "ctrader_demo.toml",
    "ctrader_pipeline_fixture.toml",
}


class PackagingDeliveryTests(unittest.TestCase):
    def test_pyproject_declares_pinned_extras_and_bundled_configs(self) -> None:
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        metadata = project["project"]
        self.assertEqual(metadata["requires-python"], ">=3.11")
        self.assertEqual(
            metadata["optional-dependencies"]["ctrader"],
            ["protobuf==7.36.1"],
        )
        self.assertEqual(
            metadata["optional-dependencies"]["dev"],
            [
                "build==1.6.1",
                "coverage==7.16.0",
                "mypy==2.3.1",
                "pyright==1.1.414",
                "ruff==0.16.7",
            ],
        )
        self.assertEqual(metadata["readme"], "README.md")
        self.assertEqual(
            project["tool"]["setuptools"]["package-data"]["mtf_lab"],
            ["resources/config/*.toml", "data/protobuf_generated/*.pyi"],
        )
        self.assertEqual(
            project["tool"]["setuptools"]["package-data"]["mtf_lab.resources"],
            ["licenses/*.txt", "provenance/*.json"],
        )

    def test_all_source_configs_have_byte_equal_wheel_resources(self) -> None:
        source_root = ROOT / "config"
        bundled_root = ROOT / "mtf_lab" / "resources" / "config"
        source_names = {path.name for path in source_root.glob("*.toml")}
        bundled_names = {path.name for path in bundled_root.glob("*.toml")}
        self.assertEqual(source_names, CONFIG_NAMES)
        self.assertEqual(bundled_names, CONFIG_NAMES)
        for name in sorted(CONFIG_NAMES):
            self.assertEqual(
                (bundled_root / name).read_bytes(),
                (source_root / name).read_bytes(),
                msg=f"recurso TOML desincronizado: {name}",
            )
            self.assertTrue(packaged_config_path(name).is_file())

    def test_config_defaults_are_not_written_beside_installed_code(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-packaging-state-") as directory:
            state = Path(directory) / "state"
            with contextlib.ExitStack() as stack:
                stack.enter_context(mock.patch.dict(os.environ, {"MTF_LAB_STATE_DIR": str(state)}, clear=False))
                config = load_config()
                self.assertEqual(default_state_dir(), state)
                self.assertEqual(Path(config.storage_db), state / "mtf_lab.sqlite3")
                self.assertNotIn("site-packages", str(Path(config.storage_db)))

    def test_foreign_config_root_cannot_shadow_packaged_defaults(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-foreign-root-") as directory:
            foreign = Path(directory)
            (foreign / "config").mkdir()
            (foreign / "config" / "default.toml").write_text("foreign = true\n", encoding="utf-8")
            (foreign / "pyproject.toml").write_text('[project]\nname = "other-project"\n', encoding="utf-8")
            xdg = foreign / "xdg-state"
            with (
                mock.patch.object(configuration, "PROJECT_ROOT", foreign),
                mock.patch.dict(
                    os.environ,
                    {"MTF_LAB_STATE_DIR": "", "XDG_STATE_HOME": str(xdg)},
                    clear=False,
                ),
            ):
                selected = configuration.packaged_config_path("default.toml")
                self.assertNotEqual(selected, foreign / "config" / "default.toml")
                self.assertEqual(selected.read_bytes(), (ROOT / "config" / "default.toml").read_bytes())
                self.assertEqual(configuration.default_state_dir(), xdg / "mtf-lab")

    def test_cli_uses_packaged_defaults_without_cwd_assumptions(self) -> None:
        self.assertTrue(default_config().is_file())
        self.assertTrue(default_watch_config().is_file())
        parser = cli.build_parser()
        ctrader = parser.parse_args(["ctrader", "query", "--fixture"])
        paper = parser.parse_args(["cfd-paper"])
        self.assertTrue(ctrader.config.is_file())
        self.assertTrue(paper.config.is_file())

    def test_launchers_resolve_root_from_an_unrelated_cwd(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-launcher-cwd-") as directory:
            env = dict(os.environ)
            env.update(
                {
                    "PYTHON": os.environ.get("PYTHON", sys.executable),
                    "PYTHONPATH": "",
                    "HOME": str(Path(directory) / "home"),
                    "XDG_STATE_HOME": str(Path(directory) / "state"),
                    "MTF_LAB_STATE_DIR": str(Path(directory) / "mtf-state"),
                }
            )
            for launcher in (ROOT / "mtf-lab", ROOT / "bin" / "mtf-lab"):
                result = subprocess.run(
                    [str(launcher), "--help"],
                    cwd=directory,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=30,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage: mtf-lab", result.stdout)

    def test_fixture_commands_are_explicitly_offline(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = cli.main(["ctrader", "query", "--fixture"])
        self.assertEqual(code, 0)
        self.assertIn('"network_performed": false', output.getvalue())

    def test_wheel_installer_builds_and_runs_outside_checkout(self) -> None:
        installer = ROOT / "install-mtf-lab-wheel.sh"
        self.assertTrue(installer.is_file())
        self.assertTrue(os.access(installer, os.X_OK))
        with tempfile.TemporaryDirectory(prefix="mtf-wheel-install-") as directory:
            isolated = Path(directory)
            target = isolated / "venv"
            env = wheel_installer._environment(isolated)
            env.update(
                {
                    "PYTHONPATH": "",
                    "HOME": str(isolated / "home"),
                    "XDG_STATE_HOME": str(isolated / "state"),
                    "MTF_LAB_STATE_DIR": str(isolated / "mtf-state"),
                    "MTF_LAB_BUILD_PYTHON": os.environ.get(
                        "MTF_LAB_BUILD_PYTHON", str(ROOT / ".venv-dev" / "bin" / "python")
                    ),
                }
            )
            source_paths = [ROOT / "pyproject.toml", ROOT / "README.md", *(ROOT / "mtf_lab").rglob("*.py")]
            before = {path: path.read_bytes() for path in source_paths}
            build_paths = [ROOT / "build", ROOT / "mtf_lab.egg-info"]
            derivatives = {
                path: path.stat().st_mtime_ns
                for directory in build_paths
                for path in directory.rglob("*")
                if path.is_file()
            }
            result = subprocess.run(
                [str(installer), "--venv", str(target), "--wheel-dir", str(isolated / "wheels")],
                cwd=isolated,
                env=env,
                capture_output=True,
                text=True,
                check=False,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("INSTALLED_WHEEL=mtf_lab-0.1.0-py3-none-any.whl", result.stdout)
            self.assertRegex(result.stdout, r"WHEEL_SHA256=[0-9a-f]{64}")
            receipt = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
            retained_wheel = Path(receipt["WHEEL_PATH"])
            self.assertTrue(retained_wheel.is_file(), "wheel bytes must survive temporary build cleanup")
            self.assertEqual(hashlib.sha256(retained_wheel.read_bytes()).hexdigest(), receipt["WHEEL_SHA256"])
            self.assertEqual(retained_wheel.parent, isolated / "wheels" / receipt["WHEEL_SHA256"])
            with zipfile.ZipFile(retained_wheel) as archive:
                for config_name in CONFIG_NAMES:
                    self.assertEqual(
                        archive.read(f"mtf_lab/resources/config/{config_name}"),
                        (ROOT / "config" / config_name).read_bytes(),
                    )
                package_data = [
                    *(ROOT / "mtf_lab/data/protobuf_generated").glob("*.pyi"),
                    *(ROOT / "mtf_lab/resources/licenses").glob("*.txt"),
                    *(ROOT / "mtf_lab/resources/provenance").glob("*.json"),
                ]
                for source in package_data:
                    archive_name = source.relative_to(ROOT).as_posix()
                    self.assertEqual(archive.read(archive_name), source.read_bytes(), msg=archive_name)
            self.assertEqual(before, {path: path.read_bytes() for path in source_paths})
            self.assertEqual(
                derivatives,
                {
                    path: path.stat().st_mtime_ns
                    for directory in build_paths
                    for path in directory.rglob("*")
                    if path.is_file()
                },
            )
            launcher = target / "bin" / "mtf-lab"
            self.assertTrue(launcher.is_file())
            smoke = subprocess.run(
                [str(launcher), "--help"],
                cwd=isolated,
                env=env,
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
            self.assertEqual(smoke.returncode, 0, smoke.stderr)
            self.assertIn("usage: mtf-lab", smoke.stdout)
            install_existing = subprocess.run(
                [
                    str(installer),
                    "--venv",
                    str(target),
                    "--wheel",
                    str(retained_wheel),
                    "--wheel-dir",
                    str(isolated / "second-wheels"),
                ],
                cwd=isolated,
                env=env,
                capture_output=True,
                text=True,
                check=False,
                timeout=90,
            )
            self.assertEqual(install_existing.returncode, 0, install_existing.stderr)
            existing_receipt = dict(line.split("=", 1) for line in install_existing.stdout.splitlines() if "=" in line)
            self.assertEqual(existing_receipt["WHEEL_SHA256"], receipt["WHEEL_SHA256"])
            self.assertEqual(Path(existing_receipt["WHEEL_PATH"]).read_bytes(), retained_wheel.read_bytes())

    def test_installer_stages_only_distribution_sources(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-stage-contract-") as name:
            base = Path(name)
            root = base / "repo"
            package = root / "mtf_lab"
            package.mkdir(parents=True)
            (root / "pyproject.toml").write_text(
                '[project]\nname="mtf-lab"\n[tool.setuptools.package-data]\nmtf_lab = ["resources/*.txt"]\n',
                encoding="utf-8",
            )
            (root / "README.md").write_text("Fixture project", encoding="utf-8")
            (package / "__init__.py").write_text("__version__ = '0.1.0'\n", encoding="utf-8")
            (package / "resources").mkdir()
            (package / "resources" / "declared.txt").write_text("declared", encoding="utf-8")
            (package / "secret.json").write_text("fixture-only-not-a-secret", encoding="utf-8")
            staged = base / "source"
            wheel_installer._copy_source(root, staged)
            self.assertEqual((package / "__init__.py").read_bytes(), (staged / "mtf_lab/__init__.py").read_bytes())
            self.assertEqual(
                (package / "resources/declared.txt").read_bytes(),
                (staged / "mtf_lab/resources/declared.txt").read_bytes(),
            )
            self.assertFalse((staged / "mtf_lab/secret.json").exists())
            (package / "bad.py").symlink_to(package / "__init__.py")
            with self.assertRaisesRegex(ValueError, "regular"):
                wheel_installer._copy_source(root, base / "second-source")

    def test_installer_rejects_declared_package_data_symlink(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-stage-data-symlink-") as name:
            base = Path(name)
            root = base / "repo"
            package = root / "mtf_lab"
            (package / "resources").mkdir(parents=True)
            (root / "pyproject.toml").write_text(
                '[project]\nname="mtf-lab"\n[tool.setuptools.package-data]\nmtf_lab = ["resources/*.txt"]\n',
                encoding="utf-8",
            )
            (root / "README.md").write_text("fixture", encoding="utf-8")
            outside = base / "outside.txt"
            outside.write_text("outside", encoding="utf-8")
            (package / "resources/linked.txt").symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "regular"):
                wheel_installer._copy_source(root, base / "staged")

    def test_installer_environment_does_not_inherit_credentials_or_pythonpath(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            mock.patch.dict(
                os.environ,
                {"CTRADER_CLIENT_SECRET": "fixture-token", "PYTHONPATH": "untrusted", "PIP_INDEX_URL": "fixture"},
            ),
        ):
            env = wheel_installer._environment(Path(directory))
            self.assertNotIn("CTRADER_CLIENT_SECRET", env)
            self.assertNotIn("PYTHONPATH", env)
            self.assertNotIn("PIP_INDEX_URL", env)
            self.assertEqual(env["PIP_NO_INDEX"], "1")
            for name in ("HOME", "XDG_STATE_HOME", "XDG_DATA_HOME", "MTF_LAB_STATE_DIR", "TMPDIR"):
                self.assertTrue(Path(env[name]).is_relative_to(directory))

    def test_installer_retains_wheel_bytes_and_rejects_identity_collision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            source = base / "mtf_lab-0.1.0-py3-none-any.whl"
            with zipfile.ZipFile(source, "w") as archive:
                archive.writestr("mtf_lab-0.1.0.dist-info/METADATA", "Name: mtf-lab\nVersion: 0.1.0\n")
            retained = wheel_installer._retain_wheel(source, base / "dist")
            self.assertEqual(retained.read_bytes(), source.read_bytes())
            self.assertEqual(retained, wheel_installer._retain_wheel(source, base / "dist"))
            retained.write_bytes(b"existing foreign bytes")
            with self.assertRaisesRegex(ValueError, "colisión"):
                wheel_installer._retain_wheel(source, base / "dist")
            self.assertEqual(retained.read_bytes(), b"existing foreign bytes")

    def test_installer_rejects_non_virtual_existing_destination(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            directory = Path(name)
            marker = directory / "preserve.txt"
            marker.write_bytes(b"original")
            with self.assertRaisesRegex(ValueError, "no es un entorno virtual"):
                wheel_installer._target_python(directory, Path(sys.executable), directory, {})
            self.assertEqual(marker.read_bytes(), b"original")

    def test_installer_rejects_symlink_directories_without_writing_outside(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            wheel = base / "mtf_lab-0.1.0-py3-none-any.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("mtf_lab-0.1.0.dist-info/METADATA", "Name: mtf-lab\nVersion: 0.1.0\n")
            outside = base / "outside"
            outside.mkdir()
            (outside / "preserved").write_bytes(b"original")
            root_alias = base / "alias"
            root_alias.symlink_to(outside, target_is_directory=True)
            with self.assertRaises(OSError):
                wheel_installer._retain_wheel(wheel, root_alias)
            store = base / "store"
            store.mkdir()
            digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
            (store / digest).symlink_to(outside, target_is_directory=True)
            with self.assertRaises(OSError):
                wheel_installer._retain_wheel(wheel, store)
            self.assertEqual(list(outside.iterdir()), [outside / "preserved"])
            self.assertEqual((outside / "preserved").read_bytes(), b"original")

    def test_installer_rejects_nested_source_directory_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            root = base / "repo"
            package = root / "mtf_lab"
            package.mkdir(parents=True)
            (root / "pyproject.toml").write_text('[project]\nname="mtf-lab"\n', encoding="utf-8")
            (root / "README.md").write_text("fixture", encoding="utf-8")
            outside = base / "outside"
            outside.mkdir()
            (outside / "module.py").write_text("VALUE=1\n", encoding="utf-8")
            (package / "nested").symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "directorio symlink"):
                wheel_installer._copy_source(root, base / "staged")

    def test_installer_rejects_fifo_artifact_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            wheel = base / "mtf_lab-0.1.0-py3-none-any.whl"
            with zipfile.ZipFile(wheel, "w") as archive:
                archive.writestr("mtf_lab-0.1.0.dist-info/METADATA", "Name: mtf-lab\nVersion: 0.1.0\n")
            digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
            bucket = base / "dist" / digest
            bucket.mkdir(parents=True)
            os.mkfifo(bucket / wheel.name)
            with self.assertRaisesRegex(ValueError, "no es un archivo regular"):
                wheel_installer._retain_wheel(wheel, base / "dist")

    def test_installer_checks_resolved_smoke_state_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            temporary = base / "temporary"
            temporary.mkdir()
            outside = base / "outside"
            outside.mkdir()
            (temporary / "state").symlink_to(outside, target_is_directory=True)
            venv = base / "venv"
            python = venv / "bin/python"
            observed = json.dumps(
                {
                    "module": str(venv / "site-packages/mtf_lab/__init__.py"),
                    "version": "0.1.0",
                    "state": str(temporary / "state"),
                }
            )
            with (
                mock.patch.object(wheel_installer, "_wheel_identity", return_value=("0.1.0", "0" * 64)),
                mock.patch.object(wheel_installer, "_target_python", return_value=python),
                mock.patch.object(wheel_installer, "_run", side_effect=["", observed]),
                self.assertRaisesRegex(ValueError, "estado no aislado"),
            ):
                wheel_installer.install_wheel(base / "fixture.whl", venv, python, temporary, {})


if __name__ == "__main__":
    unittest.main()
