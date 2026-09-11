from __future__ import annotations

import os
import stat
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.ops.ctrader_activation import (
    ActivationError,
    ActivationMode,
    ActivationProfile,
    ActivationState,
    BrokerAccount,
    OAuthAppConfig,
    RealAccountForbidden,
    SecureTokenStore,
    TokenMetadata,
    UnsafeTokenStore,
    evaluate_activation,
    select_demo_account,
)
from mtf_lab.ops.ctrader_commands import select_account_command, status_command


NOW = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)


def app() -> OAuthAppConfig:
    return OAuthAppConfig(
        client_id_env="CTRADER_CLIENT_ID",
        client_secret_env="CTRADER_CLIENT_SECRET",
        redirect_uri="http://127.0.0.1:8767/oauth/callback",
        authorization_url="https://id.ctrader.com/authorize",
        token_url="https://openapi.ctrader.com/apps/token",
    )


def profile(mode: str = "query", **changes: object) -> ActivationProfile:
    values: dict[str, object] = {
        "enabled": True,
        "operation_mode": mode,
        "environment": "DEMO",
        "required_scopes": ["accounts"] if mode == "query" else ["accounts", "trading"],
        "account_id": "demo-123456",
        "account_selected": True,
        "token_ref": "fixture",
        "token_store_dir": "~/.local/state/mtf-lab/test",
    }
    values.update(changes)
    return ActivationProfile.from_mapping(values)


def token(*scopes: str, expired: bool = False) -> TokenMetadata:
    return TokenMetadata(
        token_ref="fixture",
        granted_scopes=frozenset(scopes),
        expires_at=NOW + (-timedelta(seconds=1) if expired else timedelta(hours=1)),
        rotated_at=NOW - timedelta(minutes=1),
        generation=1,
    )


class ActivationTests(unittest.TestCase):
    def test_oauth_rejects_direct_secrets_and_non_loopback_redirect(self) -> None:
        with self.assertRaises(ActivationError):
            OAuthAppConfig.from_mapping(
                {
                    "client_id_env": "CTRADER_CLIENT_ID",
                    "client_secret_env": "CTRADER_CLIENT_SECRET",
                    "client_secret": "never",
                    "redirect_uri": "http://127.0.0.1:1/callback",
                    "authorization_url": "https://example.invalid/auth",
                    "token_url": "https://example.invalid/token",
                }
            )
        with self.assertRaises(ActivationError):
            OAuthAppConfig(
                "CTRADER_CLIENT_ID",
                "CTRADER_CLIENT_SECRET",
                "https://remote.example/callback",
                "https://example.invalid/auth",
                "https://example.invalid/token",
            )

    def test_state_machine_query_then_demo_scopes(self) -> None:
        accounts = [BrokerAccount("demo-123456", "DEMO", "fixture")]
        env = {"CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET"}
        missing = evaluate_activation(profile(), token=None, accounts=accounts, present_env_keys=env, app=app(), now=NOW)
        self.assertEqual(missing.state, ActivationState.ACCOUNTS_SCOPE_REQUIRED)
        query = evaluate_activation(profile(), token=token("accounts"), accounts=accounts, present_env_keys=env, app=app(), now=NOW)
        self.assertEqual(query.state, ActivationState.QUERY_READY)
        needs_trading = evaluate_activation(profile("demo"), token=token("accounts"), accounts=accounts, present_env_keys=env, app=app(), now=NOW)
        self.assertEqual(needs_trading.state, ActivationState.TRADING_SCOPE_REQUIRED)
        ready = evaluate_activation(profile("demo"), token=token("accounts", "trading"), accounts=accounts, present_env_keys=env, app=app(), now=NOW)
        self.assertEqual(ready.state, ActivationState.DEMO_READY)
        self.assertTrue(ready.ready)

    def test_expiry_and_explicit_demo_selection_fail_closed(self) -> None:
        env = {"CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET"}
        expired = evaluate_activation(profile(), token=token("accounts", expired=True), accounts=[BrokerAccount("demo-123456", "DEMO")], present_env_keys=env, app=app(), now=NOW)
        self.assertEqual(expired.state, ActivationState.TOKEN_EXPIRED)
        mismatch = evaluate_activation(profile(), token=TokenMetadata("other", frozenset({"accounts"}), NOW + timedelta(hours=1), NOW, 1), accounts=[BrokerAccount("demo-123456", "DEMO")], present_env_keys=env, app=app(), now=NOW)
        self.assertEqual(mismatch.state, ActivationState.TOKEN_REFERENCE_REQUIRED)
        invalid = evaluate_activation(profile(account_id="unknown"), token=token("accounts"), accounts=[BrokerAccount("demo-123456", "DEMO")], present_env_keys=env, app=app(), now=NOW)
        self.assertEqual(invalid.state, ActivationState.ACCOUNT_SELECTION_INVALID)
        with self.assertRaises(RealAccountForbidden):
            select_demo_account([BrokerAccount("real-1", "REAL")], "real-1", environment="DEMO")
        with self.assertRaises(RealAccountForbidden):
            ActivationProfile.from_mapping(
                {
                    "enabled": True,
                    "operation_mode": "demo",
                    "environment": "REAL",
                    "required_scopes": ["accounts", "trading"],
                }
            )

    def test_external_token_store_atomic_rotation_and_redaction(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            project = base / "project"
            external = base / "private-state"
            project.mkdir()
            with self.assertRaises(UnsafeTokenStore):
                SecureTokenStore(project / "tokens", project_root=project)
            store = SecureTokenStore(external, project_root=project)
            first = store.rotate(
                "demo",
                access_token="access-one",
                refresh_token="refresh-one",
                granted_scopes=["accounts"],
                expires_at=NOW + timedelta(hours=1),
                now=NOW,
            )
            second = store.rotate(
                "demo",
                access_token="access-two",
                refresh_token="refresh-two",
                granted_scopes=["accounts", "trading"],
                expires_at=NOW + timedelta(hours=2),
                now=NOW + timedelta(minutes=1),
            )
            self.assertEqual((first.generation, second.generation), (1, 2))
            lease = store.read("demo")
            self.assertEqual(lease.access_token, "access-two")
            self.assertNotIn("access-two", repr(lease))
            self.assertEqual(stat.S_IMODE(external.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE((external / "demo.json").stat().st_mode), 0o600)
            self.assertEqual(list(external.glob("*.tmp")), [])

    def test_command_helpers_are_pure_and_redacted(self) -> None:
        raw = {
            "ctrader": {
                "enabled": True,
                "operation_mode": "query",
                "environment": "DEMO",
                "required_scopes": ["accounts"],
                "account_id": "demo-123456",
                "account_selected": True,
                "token_ref": "fixture",
                "token_store_dir": "/external",
            },
            "ctrader_oauth": {
                "client_id_env": "CTRADER_CLIENT_ID",
                "client_secret_env": "CTRADER_CLIENT_SECRET",
                "redirect_uri": "http://localhost:8767/callback",
                "authorization_url": "https://example.invalid/auth",
                "token_url": "https://example.invalid/token",
            },
        }
        before = repr(raw)
        output = status_command(
            raw,
            token_metadata=token("accounts"),
            accounts=[{"account_id": "demo-123456", "environment": "DEMO"}],
            present_env_keys={"CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET"},
            now=NOW,
        )
        self.assertEqual(output["status"]["state"], "QUERY_READY")
        self.assertEqual(before, repr(raw))
        self.assertNotIn("access_token", repr(output))
        selected = select_account_command(raw, [{"account_id": "demo-123456", "environment": "DEMO"}], account_id="demo-123456", environment="DEMO")
        self.assertEqual(selected["config_patch"]["ctrader"]["account_id"], "demo-123456")


if __name__ == "__main__":
    unittest.main()

class OAuthHelperTests(unittest.TestCase):
    def test_authorization_url_and_callback_are_loopback_and_scoped(self) -> None:
        from mtf_lab.ops.ctrader_activation import build_authorization_url, parse_callback_uri
        url = build_authorization_url(app(), client_id="client-public", scope="accounts", state="state-1")
        self.assertIn("scope=accounts", url)
        self.assertIn("state=state-1", url)
        self.assertEqual(parse_callback_uri("http://127.0.0.1:8767/oauth/callback?code=abc&state=state-1", expected_state="state-1", registered_uri=app().redirect_uri), "abc")
        with self.assertRaises(ActivationError):
            parse_callback_uri("http://127.0.0.1:8767/oauth/callback?code=abc&state=wrong", expected_state="state-1", registered_uri=app().redirect_uri)

    def test_token_exchange_redacts_secrets_and_rotation_payload(self) -> None:
        from mtf_lab.ops.ctrader_activation import exchange_authorization_code, refresh_access_token
        seen = []
        def requester(url, params, timeout):
            seen.append((url, dict(params), timeout))
            return {"accessToken": "access-secret", "refreshToken": "refresh-secret", "expiresIn": 60, "tokenType": "bearer"}
        payload = exchange_authorization_code(app(), client_id="client", client_secret="secret", code="code", requester=requester)
        self.assertNotIn("access-secret", repr(payload))
        self.assertEqual(payload.expires_in, 60)
        refreshed = refresh_access_token(app(), client_id="client", client_secret="secret", refresh_token="old", requester=requester)
        self.assertEqual(refreshed.refresh_token, "refresh-secret")
        self.assertEqual(seen[0][1]["grant_type"], "authorization_code")
        self.assertEqual(seen[1][1]["grant_type"], "refresh_token")
