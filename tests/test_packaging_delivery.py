"""Regression checks for clone-clean packaging and offline entry points."""

from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest import mock

import mtf_lab.configuration as configuration
from mtf_lab.configuration import default_state_dir, load_config, packaged_config_path
from mtf_lab.ops import cli
from mtf_lab.ops.application_services import default_config, default_watch_config

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
            ["ctrader-open-api==0.9.2", "service-identity==24.2.0"],
        )
        self.assertEqual(metadata["optional-dependencies"]["dev"], ["ruff==0.16.7", "mypy==2.3.1"])
        self.assertEqual(project["tool"]["setuptools"]["package-data"]["mtf_lab"], ["resources/config/*.toml"])

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


if __name__ == "__main__":
    unittest.main()
