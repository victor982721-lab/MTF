"""Contracts for the private, read-only cTrader query launcher."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from unittest import mock

from mtf_lab.ops import ctrader_cli_services

ROOT = Path(__file__).parents[1]
MODULE_PATH = ROOT / "tools" / "ctrader_query_launcher.py"


def load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("ctrader_query_launcher_under_test", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise AssertionError("no se pudo cargar el launcher")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CTraderQueryLauncherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.launcher = load_module()

    @staticmethod
    def private_dir(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        os.chmod(path, 0o700)

    @staticmethod
    def private_json(path: Path, value: object) -> None:
        path.write_text(json.dumps(value), encoding="utf-8")
        os.chmod(path, 0o600)

    def make_state(self, root: Path, *, token_scopes: list[str] | None = None) -> Path:
        config = root / "config"
        self.private_dir(config)
        config.joinpath("ctrader_query.toml").write_text(
            """
[project]
name = "MTF Lab cTrader consulta"
version = "0.2.0"
mode = "live"
[instrument]
symbol = "EUR/USD"
price_base = "mid"
[timeframes]
base = "1m"
values = ["1m", "5m", "15m"]
closed_only = true
[provider]
name = "ctrader_query"
[quality]
max_feed_age_seconds = 90
max_closed_candle_age_seconds = 900
max_gap_minutes = 3
require_warmup = true
[ctrader]
enabled = true
operation_mode = "query"
environment = "DEMO"
required_scopes = ["accounts"]
account_id = ""
account_selected = false
token_ref = "ctrader-query-demo"
token_store_dir = "TOKEN_DIR"
[ctrader_oauth]
client_id_env = "CTRADER_CLIENT_ID"
client_secret_env = "CTRADER_CLIENT_SECRET"
redirect_uri = "http://127.0.0.1:8767/oauth/callback"
authorization_url = "https://id.ctrader.com/my/settings/openapi/grantingaccess/"
token_url = "https://openapi.ctrader.com/apps/token"
""".replace("TOKEN_DIR", str(root / "state" / "tokens")),
            encoding="utf-8",
        )
        token_dir = root / "state" / "tokens"
        self.private_dir(root / "state")
        self.private_dir(token_dir)
        expires = (datetime.now(UTC) + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        self.private_json(
            token_dir / "ctrader-query-demo.json",
            {
                "version": 1,
                "access_token": "fixture-access",
                "refresh_token": "fixture-refresh",
                "metadata": {
                    "token_ref": "ctrader-query-demo",
                    "granted_scopes": token_scopes if token_scopes is not None else ["accounts"],
                    "expires_at": expires,
                },
            },
        )
        self.private_json(
            token_dir / "selection.json",
            {"version": 1, "token_ref": "ctrader-query-demo", "account_id": "7", "environment": "DEMO"},
        )
        self.private_json(
            token_dir / "account-discovery.json",
            {
                "version": 1,
                "token_ref": "ctrader-query-demo",
                "permissionScope": "SCOPE_VIEW",
                "accounts": [{"account_id": "7", "environment": "DEMO"}],
            },
        )
        return token_dir

    def make_credentials(self, root: Path) -> Path:
        config_dir = root / "credentials"
        self.private_dir(config_dir)
        path = config_dir / "ctrader-app-fixture.credentials.json"
        self.private_json(path, {"version": 1, "client_id": "fixture-client", "client_secret": "fixture-secret"})
        return path

    def make_runtime(self, root: Path) -> Path:
        runtime_dir = root / "runtime" / "bin"
        self.private_dir(root / "runtime")
        self.private_dir(runtime_dir)
        path = runtime_dir / "mtf-lab"
        path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        os.chmod(path, 0o700)
        return path

    @staticmethod
    def fixture_script_path(root: Path) -> Path:
        tools = root / "tools"
        tools.mkdir(parents=True, exist_ok=True)
        return tools / "ctrader_query_launcher.py"

    def test_build_exec_forces_network_query_and_scrubs_python_ambient_state(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            self.make_state(root)
            credentials = self.make_credentials(root)
            runtime = self.make_runtime(root)
            inherited = {
                "PATH": "/untrusted/bin",
                "PYTHONHOME": "/untrusted/python",
                "PYTHONPATH": "/untrusted/path",
                "MTF_LAB_CTRADER_CREDENTIALS_FILE": str(credentials),
                "CTRADER_CLIENT_SECRET": "ambient-secret",
                "LANG": "C.UTF-8",
            }
            target, command, child = self.launcher.build_exec(
                ["--report", str(root / "report.json"), "--capture", str(root / "capture.json")],
                root=root,
                inherited=inherited,
                credentials_path=credentials,
                runtime_path=runtime,
            )
            self.assertEqual(target, runtime)
            self.assertEqual(command[1:4], ["ctrader", "query", "--network"])
            self.assertIn("--config", command)
            self.assertNotIn("--fixture", command)
            self.assertNotIn("--activate", command)
            self.assertNotIn("--account-id", command)
            self.assertNotIn("PYTHONHOME", child)
            self.assertNotIn("PYTHONPATH", child)
            self.assertNotIn("MTF_LAB_CTRADER_CREDENTIALS_FILE", child)
            self.assertEqual(child["CTRADER_CLIENT_ID"], "fixture-client")
            self.assertEqual(child["CTRADER_CLIENT_SECRET"], "fixture-secret")
            self.assertEqual(child["PYTHONNOUSERSITE"], "1")
            self.assertEqual(child["PATH"], "/usr/bin:/bin")

    def test_help_does_not_read_any_private_store(self) -> None:
        with (
            mock.patch.object(self.launcher, "_preflight_selection") as preflight,
            mock.patch.object(self.launcher, "_credentials") as credentials,
            mock.patch.dict(self.launcher.os.environ, {}, clear=True),
            contextlib.redirect_stdout(io.StringIO()),
            self.assertRaises(SystemExit) as stopped,
        ):
            self.launcher.main(["--help"])
        self.assertEqual(stopped.exception.code, 0)
        preflight.assert_not_called()
        credentials.assert_not_called()

    def test_missing_credentials_reference_fails_closed_without_glob(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            self.make_state(root)
            with self.assertRaisesRegex(self.launcher.LauncherError, "ruta privada exacta"):
                self.launcher._credentials({})

    def test_configured_account_must_match_durable_selection(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            self.make_state(root)
            config = root / "config" / "ctrader_query.toml"
            original = config.read_text()
            config.write_text(
                original.replace('account_id = ""', 'account_id = "8"').replace(
                    "account_selected = false", "account_selected = true"
                )
            )
            with self.assertRaisesRegex(self.launcher.LauncherError, "difiere"):
                self.launcher._preflight_selection(root)
            config.write_text(
                original.replace('account_id = ""', 'account_id = "7"').replace(
                    "account_selected = false", "account_selected = true"
                )
            )
            self.launcher._preflight_selection(root)

    def test_output_cannot_overwrite_token_or_follow_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            tokens = self.make_state(root)
            token = tokens / "ctrader-query-demo.json"
            before = token.read_bytes()
            alias = root / "alias.json"
            alias.symlink_to(token)
            for output in (token, alias):
                with self.subTest(output=output), self.assertRaisesRegex(self.launcher.LauncherError, "ya existe"):
                    self.launcher._query_args(["--report", str(output)])
            self.assertEqual(token.read_bytes(), before)

    def test_outputs_require_private_parent_and_distinct_paths(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            output = root / "report.json"
            with self.assertRaisesRegex(self.launcher.LauncherError, "distintos"):
                self.launcher._query_args(["--report", str(output), "--capture", str(output)])
            os.chmod(root, 0o755)
            with self.assertRaisesRegex(self.launcher.LauncherError, "0700"):
                self.launcher._query_args(["--report", str(output)])

    def test_lexical_output_aliases_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            self.private_dir(root / "nested")
            with self.assertRaisesRegex(self.launcher.LauncherError, "distintos"):
                self.launcher._query_args(
                    [
                        "--report",
                        str(root / "nested" / ".." / "same.json"),
                        "--capture",
                        str(root / "same.json"),
                    ]
                )

    def test_query_report_is_private_and_never_replaces_existing_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            output = Path(name) / "report.json"
            ctrader_cli_services._write_query_report(output, {"fixture": True})
            before = output.read_bytes()
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                ctrader_cli_services._write_query_report(output, {"replacement": True})
            self.assertEqual(output.read_bytes(), before)

    def test_query_report_preserves_destination_created_after_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            output = Path(name) / "report.json"
            original_link = os.link

            def competing_writer(source: str | Path, target: str | Path, **kwargs: object) -> None:
                Path(target).write_bytes(b"existing-independent-output")
                original_link(source, target, **kwargs)

            with (
                mock.patch.object(ctrader_cli_services.os, "link", side_effect=competing_writer),
                self.assertRaises(FileExistsError),
            ):
                ctrader_cli_services._write_query_report(output, {"replacement": True})
            self.assertEqual(output.read_bytes(), b"existing-independent-output")
            self.assertEqual([item.name for item in Path(name).iterdir()], ["report.json"])

    def test_insecure_credentials_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            self.make_state(root)
            credentials = self.make_credentials(root)
            os.chmod(credentials, 0o644)
            with self.assertRaisesRegex(self.launcher.LauncherError, "0600"):
                self.launcher._credentials({self.launcher._CREDENTIALS_ENV: str(credentials)})

    def test_trading_scope_or_non_demo_selection_fails_before_exec(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            token_dir = self.make_state(root, token_scopes=["accounts", "trading"])
            credentials = self.make_credentials(root)
            runtime = self.make_runtime(root)
            with self.assertRaisesRegex(self.launcher.LauncherError, "exclusivamente accounts"):
                self.launcher.build_exec(
                    [], root=root, inherited={}, credentials_path=credentials, runtime_path=runtime
                )
            self.assertEqual((token_dir / "selection.json").stat().st_mode & 0o777, 0o600)

    def test_expired_token_fails_closed_before_exec(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            token_dir = self.make_state(root)
            token_path = token_dir / "ctrader-query-demo.json"
            token = json.loads(token_path.read_text(encoding="utf-8"))
            token["metadata"]["expires_at"] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
            self.private_json(token_path, token)
            with self.assertRaisesRegex(self.launcher.LauncherError, "expirado"):
                self.launcher.build_exec(
                    [],
                    root=root,
                    inherited={},
                    credentials_path=self.make_credentials(root),
                    runtime_path=self.make_runtime(root),
                )

    def test_real_selection_fails_closed_before_exec(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            token_dir = self.make_state(root)
            selection_path = token_dir / "selection.json"
            selection = json.loads(selection_path.read_text(encoding="utf-8"))
            selection["environment"] = "REAL"
            self.private_json(selection_path, selection)
            with self.assertRaisesRegex(self.launcher.LauncherError, "selección durable"):
                self.launcher.build_exec(
                    [],
                    root=root,
                    inherited={},
                    credentials_path=self.make_credentials(root),
                    runtime_path=self.make_runtime(root),
                )

    def test_symlink_credentials_fail_closed_before_exec(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            self.make_state(root)
            credentials = self.make_credentials(root)
            link = credentials.with_name("ctrader-app-link.credentials.json")
            link.symlink_to(credentials)
            with self.assertRaisesRegex(self.launcher.LauncherError, "symlink"):
                self.launcher.build_exec(
                    [], root=root, inherited={}, credentials_path=link, runtime_path=self.make_runtime(root)
                )

    def test_forbidden_flags_are_rejected_before_exec(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            self.make_state(root)
            credentials = self.make_credentials(root)
            runtime = self.make_runtime(root)
            fake_file = self.fixture_script_path(root)
            with (
                mock.patch.object(self.launcher, "__file__", str(fake_file)),
                mock.patch.object(self.launcher, "_runtime_path", return_value=runtime),
                mock.patch.object(self.launcher.os, "execve") as execve,
                mock.patch.dict(
                    self.launcher.os.environ,
                    {self.launcher._CREDENTIALS_ENV: str(credentials)},
                    clear=True,
                ),
            ):
                for flags in (["--activate"], ["--account-id", "7"]):
                    with (
                        self.subTest(flags=flags),
                        contextlib.redirect_stderr(io.StringIO()),
                        self.assertRaises(SystemExit),
                    ):
                        self.launcher.main(flags)
            execve.assert_not_called()

    def test_main_execs_only_after_preflight_and_never_prints_secret(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            self.make_state(root)
            credentials = self.make_credentials(root)
            runtime = self.make_runtime(root)
            fake_file = self.fixture_script_path(root)
            with (
                mock.patch.object(self.launcher, "__file__", str(fake_file)),
                mock.patch.object(self.launcher, "_runtime_path", return_value=runtime),
                mock.patch.object(self.launcher.os, "execve", side_effect=SystemExit(0)) as execve,
                mock.patch.dict(
                    self.launcher.os.environ,
                    {self.launcher._CREDENTIALS_ENV: str(credentials)},
                    clear=True,
                ),
                self.assertRaises(SystemExit),
            ):
                self.launcher.main(["--report", str(root / "report.json")])
            execve.assert_called_once()
            _, command, child = execve.call_args.args
            self.assertIn("ctrader", command)
            self.assertIn("query", command)
            self.assertIn("--network", command)
            self.assertEqual(child["CTRADER_CLIENT_SECRET"], "fixture-secret")


if __name__ == "__main__":
    unittest.main()
