from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

from mtf_lab.ops import cli
from mtf_lab.ops.ctrader_activation import SecureTokenStore


ROOT = Path(__file__).parents[1]
NOW = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)


def write_query_config(base: Path, *, selected: bool = False) -> tuple[Path, Path]:
    token_dir = base / "real-token-store"
    text = (ROOT / "config" / "ctrader_query.toml").read_text(encoding="utf-8")
    text = text.replace(
        'token_store_dir = "~/.local/state/mtf-lab/ctrader-tokens"',
        f'token_store_dir = "{token_dir}"',
    )
    if selected:
        text = text.replace('account_id = ""', 'account_id = "7"')
        text = text.replace("account_selected = false", "account_selected = true")
    target = base / "query.toml"
    target.write_text(text, encoding="utf-8")
    return target, token_dir


def run_cli(argv: list[str]) -> tuple[int, dict]:
    output = io.StringIO()
    errors = io.StringIO()
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
        code = cli.main(argv)
    raw = output.getvalue().strip()
    if not raw:
        raise AssertionError(f"CLI sin JSON; stderr={errors.getvalue()!r}")
    return code, json.loads(raw)


class CTraderCLIActivationTests(unittest.TestCase):
    def test_auth_url_persists_restart_safe_attempt_and_never_opens_browser_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config, token_dir = write_query_config(Path(tmp))
            with mock.patch.dict(os.environ, {"CTRADER_CLIENT_ID": "public-fixture-client"}, clear=False):
                code, output = run_cli(["ctrader", "auth-url", "--config", str(config)])
            self.assertEqual(code, 0)
            self.assertTrue(output["ok"])
            self.assertFalse(output["browser_opened"])
            self.assertFalse(output["network_performed"])
            self.assertTrue(output["attempt"]["attempt_id"].startswith("oauth-"))
            self.assertIn("state=", output["authorization_url"])
            persisted = token_dir / f'.oauth-attempt-{output["attempt"]["attempt_id"]}.json'
            self.assertTrue(persisted.exists())
            self.assertEqual(persisted.stat().st_mode & 0o777, 0o600)

    def test_fixture_exchange_and_refresh_never_create_profile_token_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config, real_token_dir = write_query_config(Path(tmp))
            real_store = SecureTokenStore(real_token_dir, project_root=ROOT)
            real_store.rotate(
                "ctrader-query-demo",
                access_token="preserved-real-access",
                refresh_token="preserved-real-refresh",
                granted_scopes=["accounts"],
                expires_at=NOW + timedelta(hours=1),
                now=NOW,
            )
            real_path = real_token_dir / "ctrader-query-demo.json"
            before = real_path.read_bytes()
            exchange_code, exchange = run_cli(
                ["ctrader", "token-exchange", "--config", str(config), "--fixture"]
            )
            refresh_code, refresh = run_cli(
                ["ctrader", "token-refresh", "--config", str(config), "--fixture"]
            )
            self.assertEqual((exchange_code, refresh_code), (0, 0))
            for output in (exchange, refresh):
                self.assertTrue(output["fixture"])
                self.assertFalse(output["network_performed"])
                self.assertFalse(output["real_token_store_touched"])
                self.assertTrue(output["fixture_store_removed"])
                self.assertEqual(output["secrets"], "REDACTED")
            self.assertTrue(real_token_dir.exists())
            self.assertEqual(real_path.read_bytes(), before)

    def test_select_requires_token_backed_account_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config, token_dir = write_query_config(base)
            store = SecureTokenStore(token_dir, project_root=ROOT)
            store.rotate(
                "ctrader-query-demo",
                access_token="local-test-access",
                refresh_token="local-test-refresh",
                granted_scopes=["accounts"],
                expires_at=NOW + timedelta(hours=1),
                now=NOW,
            )
            accounts_file = base / "accounts.json"
            accounts_file.write_text(
                json.dumps(
                    {
                        "token_ref": "ctrader-query-demo",
                        "observed_at": NOW.isoformat(),
                        "accounts": [
                            {"account_id": "7", "environment": "DEMO"},
                            {"account_id": "8", "environment": "REAL"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            code, output = run_cli(
                [
                    "ctrader",
                    "select",
                    "--config",
                    str(config),
                    "--accounts-file",
                    str(accounts_file),
                    "--account-id",
                    "7",
                ]
            )
            self.assertEqual(code, 0)
            self.assertTrue(output["accepted"])
            self.assertEqual(output["config_patch"]["ctrader"]["account_id"], "7")
            self.assertEqual(output["discovery"]["token_ref"], "ctrader-query-demo")
            self.assertTrue(output["selection_persisted"])
            selection = json.loads(Path(output["selection_path"]).read_text(encoding="utf-8"))
            self.assertEqual(selection["account_id"], "7")
            self.assertEqual((Path(output["selection_path"]).stat().st_mode & 0o777), 0o600)

    def test_query_calls_connect_auth_and_discovery_before_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config, token_dir = write_query_config(base, selected=True)
            store = SecureTokenStore(token_dir, project_root=ROOT)
            store.rotate(
                "ctrader-query-demo",
                access_token="local-test-access",
                refresh_token="local-test-refresh",
                granted_scopes=["accounts"],
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            calls: list[str] = []

            class Status:
                auth = "AUTHENTICATED"
                action = ""

                def to_dict(self) -> dict:
                    return {"connection": "CONNECTED", "auth": "AUTHENTICATED", "action": ""}

            class Provider:
                def __init__(self, config):
                    self.config = config
                    self.status = Status()

                def connect(self):
                    calls.append("connect")
                    return self.status

                def authenticate(self, *, secret_provider, token_provider, authorize_selected=True):
                    calls.append("application_auth")
                    self.assert_secret = secret_provider("CTRADER_CLIENT_SECRET")
                    calls.append("account_discovery")
                    self.assert_token = token_provider("ctrader-query-demo")
                    assert authorize_selected is False
                    return "ACCOUNT_REQUIRED"

                def discover_accounts(self):
                    return {"records": [{"account_id": 7, "environment": "DEMO"}], "permissionScope": "SCOPE_VIEW"}

                def authorize_account(self, account_id, *, token_provider):
                    assert account_id == 7
                    assert token_provider("ctrader-query-demo") == "local-test-access"
                    calls.append("account_auth")
                    return "AUTHENTICATED"

                def resolve_symbol(self):
                    calls.append("catalog")
                    return {"selected": 99}

                def fetch_history(self, timeframe, count, max_pages):
                    calls.append("history")
                    return {"timeframe": timeframe, "count": 0}

                def close(self):
                    calls.append("close")

            env = {
                "CTRADER_CLIENT_ID": "public-fixture-client",
                "CTRADER_CLIENT_SECRET": "runtime-secret",
            }
            with mock.patch.dict(os.environ, env, clear=False), mock.patch(
                "mtf_lab.data.ctrader.CTraderProvider", Provider
            ):
                code, output = run_cli(
                    ["ctrader", "query", "--config", str(config), "--network"]
                )
            self.assertEqual(code, 0)
            self.assertTrue(output["ok"])
            self.assertEqual(
                calls,
                ["connect", "application_auth", "account_discovery", "account_auth", "catalog", "history", "close"],
            )
            self.assertEqual(
                output["sequence"],
                ["connect", "application_auth", "account_discovery", "account_auth", "catalog", "history"],
            )
            self.assertNotIn("runtime-secret", json.dumps(output))


    def test_query_discovers_accounts_then_stops_for_explicit_selection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config, token_dir = write_query_config(base, selected=False)
            SecureTokenStore(token_dir, project_root=ROOT).rotate(
                "ctrader-query-demo",
                access_token="local-test-access",
                refresh_token="local-test-refresh",
                granted_scopes=["accounts"],
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            calls: list[str] = []

            class Status:
                action = "Seleccione una cuenta DEMO descubierta antes de autenticarla."

                def to_dict(self) -> dict:
                    return {
                        "connection": "CONNECTED",
                        "auth": "ACCOUNT_REQUIRED",
                        "action": self.action,
                    }

            class Provider:
                def __init__(self, config):
                    self.status = Status()

                def connect(self):
                    calls.append("connect")

                def authenticate(self, *, secret_provider, token_provider, authorize_selected=True):
                    calls.append("authenticate")
                    secret_provider("CTRADER_CLIENT_SECRET")
                    token_provider("ctrader-query-demo")
                    assert authorize_selected is False
                    calls.extend(("application_auth", "account_discovery"))
                    return "ACCOUNT_REQUIRED"

                def discover_accounts(self):
                    return {"records": [{"account_id": 7, "environment": "DEMO"}], "permissionScope": "SCOPE_VIEW"}

                def authorize_account(self, account_id, *, token_provider):
                    calls.append("account_auth")
                    raise AssertionError("no se debe autorizar sin selección")

                def close(self):
                    calls.append("close")

            with mock.patch.dict(
                os.environ,
                {
                    "CTRADER_CLIENT_ID": "public-fixture-client",
                    "CTRADER_CLIENT_SECRET": "runtime-secret",
                },
                clear=False,
            ), mock.patch("mtf_lab.data.ctrader.CTraderProvider", Provider):
                code, output = run_cli(
                    ["ctrader", "query", "--config", str(config), "--network"]
                )
            self.assertEqual(code, 2)
            self.assertEqual(
                output["sequence"],
                ["connect", "application_auth", "account_discovery"],
            )
            self.assertNotIn("account_auth", output["sequence"])
            self.assertIn("Seleccione una cuenta DEMO", output["next_action"])
            self.assertTrue(Path(output["discovery_path"]).exists())
            discovery = json.loads(Path(output["discovery_path"]).read_text(encoding="utf-8"))
            self.assertEqual(discovery["accounts"][0]["account_id"], 7)
            self.assertEqual(calls, ["connect", "authenticate", "application_auth", "account_discovery", "close"])


if __name__ == "__main__":
    unittest.main()
