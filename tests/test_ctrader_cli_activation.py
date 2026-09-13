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

from mtf_lab.data.capture import CaptureEnvelope, MessageClass
from mtf_lab.data.ctrader import (
    CTraderHistoryResult,
    CTraderInstrumentSpec,
    normalize_trendbar,
)
from mtf_lab.data.paper_fixture import synthetic_ctrader_payloads
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
            persisted = token_dir / f".oauth-attempt-{output['attempt']['attempt_id']}.json"
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
            exchange_code, exchange = run_cli(["ctrader", "token-exchange", "--config", str(config), "--fixture"])
            refresh_code, refresh = run_cli(["ctrader", "token-refresh", "--config", str(config), "--fixture"])
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
                generation = 1

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

                def discover_accounts(self, *, include_token: bool = False):
                    return {
                        "accessToken": "local-test-access",
                        "records": [{"account_id": 7, "environment": "DEMO"}],
                        "permissionScope": "SCOPE_VIEW",
                    }

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
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch("mtf_lab.data.ctrader.CTraderProvider", Provider),
            ):
                code, output = run_cli(["ctrader", "query", "--config", str(config), "--network"])
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
                generation = 1

                def to_dict(self) -> dict:
                    return {
                        "connection": "CONNECTED",
                        "auth": "ACCOUNT_REQUIRED",
                        "action": self.action,
                    }

            class Provider:
                def __init__(self, config):
                    self.config = config
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

                def discover_accounts(self, *, include_token: bool = False):
                    return {
                        "accessToken": "local-test-access",
                        "records": [{"account_id": 7, "environment": "DEMO"}],
                        "permissionScope": "SCOPE_VIEW",
                    }

                def authorize_account(self, account_id, *, token_provider):
                    calls.append("account_auth")
                    raise AssertionError("no se debe autorizar sin selección")

                def close(self):
                    calls.append("close")

            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "CTRADER_CLIENT_ID": "public-fixture-client",
                        "CTRADER_CLIENT_SECRET": "runtime-secret",
                    },
                    clear=False,
                ),
                mock.patch("mtf_lab.data.ctrader.CTraderProvider", Provider),
            ):
                code, output = run_cli(["ctrader", "query", "--config", str(config), "--network"])
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

    def test_query_exports_native_history_capture_and_cfd_paper_has_no_quote_fills(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            config, token_dir = write_query_config(base, selected=True)
            SecureTokenStore(token_dir, project_root=ROOT).rotate(
                "ctrader-query-demo",
                access_token="local-test-access",
                refresh_token="local-test-refresh",
                granted_scopes=["accounts"],
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )

            start = datetime(2026, 1, 1, tzinfo=UTC)
            rows = synthetic_ctrader_payloads(start=start, symbol_id=99, count=190)
            spec = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)
            raw_bars = [row["trendbar"][0] for row in rows]
            received = (
                start + timedelta(minutes=95, seconds=1),
                start + timedelta(minutes=190, seconds=1),
            )
            pages = (
                {
                    "period": 1,
                    "symbolId": 99,
                    "trendbar": raw_bars[:95],
                    "hasMore": True,
                },
                {
                    "period": 1,
                    "symbolId": 99,
                    "trendbar": raw_bars[95:],
                    "hasMore": False,
                },
            )
            bars = tuple(
                normalize_trendbar(
                    raw,
                    spec=spec,
                    received_at=received[0 if index < 95 else 1],
                    available_at=received[0 if index < 95 else 1],
                )
                for index, raw in enumerate(raw_bars)
            )
            page_metadata = tuple(
                {
                    "received_at": when,
                    "available_at": when,
                    "ingest_sequence": index,
                    "connection_generation": 1,
                    "source_identity": f"fake-history-page-{index}",
                    "request": {
                        "payload_type": "PROTO_OA_GET_TRENDBARS_REQ",
                        "payload": {"period": 1, "symbolId": 99, "count": 95},
                    },
                    "response_type": "PROTO_OA_GET_TRENDBARS_RES",
                }
                for index, when in enumerate(received)
            )
            history = CTraderHistoryResult(
                bars,
                "M1",
                2,
                True,
                False,
                (),
                pages,
                page_metadata,
            )
            capture_path = base / "history.jsonl"
            query_report = base / "query-report.json"
            calls: list[str] = []

            class Status:
                generation = 1
                action = ""

                def to_dict(self) -> dict[str, object]:
                    return {"connection": "CONNECTED", "auth": "AUTHENTICATED", "generation": 1, "action": ""}

            class Provider:
                def __init__(self, provider_config):
                    self.config = provider_config
                    self.spec = spec
                    self.status = Status()

                def connect(self):
                    calls.append("connect")

                def authenticate(self, *, secret_provider, token_provider, authorize_selected=True):
                    calls.append("authenticate")
                    secret_provider("CTRADER_CLIENT_SECRET")
                    token_provider("ctrader-query-demo")
                    assert authorize_selected is False
                    return "ACCOUNT_REQUIRED"

                def discover_accounts(self, *, include_token: bool = False):
                    calls.append("discover")
                    return {
                        "accessToken": "local-test-access",
                        "records": [{"account_id": 7, "environment": "DEMO"}],
                        "permissionScope": "SCOPE_VIEW",
                    }

                def authorize_account(self, account_id, *, token_provider):
                    calls.append("account_auth")
                    assert account_id == 7
                    assert token_provider("ctrader-query-demo") == "local-test-access"

                def resolve_symbol(self):
                    calls.append("catalog")
                    return {"selected": 99}

                def fetch_history(self, timeframe, count, max_pages):
                    calls.append("history")
                    assert (timeframe, count, max_pages) == ("M1", 500, 20)
                    return history

                def close(self):
                    calls.append("close")

            env = {
                "CTRADER_CLIENT_ID": "public-fixture-client",
                "CTRADER_CLIENT_SECRET": "runtime-secret",
            }
            with (
                mock.patch.dict(os.environ, env, clear=False),
                mock.patch("mtf_lab.data.ctrader.CTraderProvider", Provider),
            ):
                query_code, query_output = run_cli(
                    [
                        "ctrader",
                        "query",
                        "--config",
                        str(config),
                        "--network",
                        "--capture",
                        str(capture_path),
                        "--report",
                        str(query_report),
                    ]
                )
            self.assertEqual(query_code, 0)
            self.assertEqual(
                calls, ["connect", "authenticate", "discover", "account_auth", "catalog", "history", "close"]
            )
            self.assertEqual(query_output["capture"]["analysis_basis"], "native")
            self.assertEqual(query_output["capture"]["native_bars"], 190)
            self.assertEqual(query_output["capture"]["quote_events"], 0)
            self.assertEqual(query_output["capture"]["paper_fills"], 0)
            self.assertTrue(query_report.is_file())
            self.assertEqual(
                json.loads(query_report.read_text(encoding="utf-8"))["capture"]["message_class"], "trendbar"
            )

            envelopes = [
                CaptureEnvelope.from_mapping(json.loads(line))
                for line in capture_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(
                [item.message_class for item in envelopes],
                [MessageClass.TRENDBAR, MessageClass.TRENDBAR, MessageClass.END],
            )
            self.assertTrue(all("bid" not in item.payload and "ask" not in item.payload for item in envelopes[:2]))
            self.assertEqual(envelopes[0].payload["capture_kind"], "historical_trendbars")

            native_config = base / "native-pipeline.toml"
            native_text = (ROOT / "config" / "ctrader_pipeline_fixture.toml").read_text(encoding="utf-8")
            native_config.write_text(
                native_text.replace('price_base = "mid"', 'price_base = "native"'), encoding="utf-8"
            )
            paper_db = base / "native-paper.sqlite3"
            paper_report = base / "native-paper.json"
            paper_code, paper_output = run_cli(
                [
                    "cfd-paper",
                    "--config",
                    str(native_config),
                    "--input",
                    str(capture_path),
                    "--db",
                    str(paper_db),
                    "--report",
                    str(paper_report),
                    "--price-base",
                    "native",
                    "--order",
                    "market_time_corrected",
                ]
            )
            self.assertEqual(paper_code, 0)
            self.assertEqual(paper_output["analysis_basis"], "native")
            self.assertEqual(paper_output["capture"]["quote_event_count"], 0)
            self.assertEqual(paper_output["capture"]["bar_count"], 190)
            self.assertEqual(paper_output["trades"], [])
            self.assertTrue(paper_report.is_file())


if __name__ == "__main__":
    unittest.main()
