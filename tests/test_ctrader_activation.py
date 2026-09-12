from __future__ import annotations

import stat
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.ops.ctrader_activation import (
    ActivationError,
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
        missing = evaluate_activation(
            profile(), token=None, accounts=accounts, present_env_keys=env, app=app(), now=NOW
        )
        self.assertEqual(missing.state, ActivationState.ACCOUNTS_SCOPE_REQUIRED)
        query = evaluate_activation(
            profile(), token=token("accounts"), accounts=accounts, present_env_keys=env, app=app(), now=NOW
        )
        self.assertEqual(query.state, ActivationState.QUERY_READY)
        needs_trading = evaluate_activation(
            profile("demo"), token=token("accounts"), accounts=accounts, present_env_keys=env, app=app(), now=NOW
        )
        self.assertEqual(needs_trading.state, ActivationState.TRADING_SCOPE_REQUIRED)
        ready = evaluate_activation(
            profile("demo"),
            token=token("accounts", "trading"),
            accounts=accounts,
            present_env_keys=env,
            app=app(),
            now=NOW,
        )
        self.assertEqual(ready.state, ActivationState.DEMO_READY)
        self.assertTrue(ready.ready)

    def test_expiry_and_explicit_demo_selection_fail_closed(self) -> None:
        env = {"CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET"}
        expired = evaluate_activation(
            profile(),
            token=token("accounts", expired=True),
            accounts=[BrokerAccount("demo-123456", "DEMO")],
            present_env_keys=env,
            app=app(),
            now=NOW,
        )
        self.assertEqual(expired.state, ActivationState.TOKEN_EXPIRED)
        mismatch = evaluate_activation(
            profile(),
            token=TokenMetadata("other", frozenset({"accounts"}), NOW + timedelta(hours=1), NOW, 1),
            accounts=[BrokerAccount("demo-123456", "DEMO")],
            present_env_keys=env,
            app=app(),
            now=NOW,
        )
        self.assertEqual(mismatch.state, ActivationState.TOKEN_REFERENCE_REQUIRED)
        invalid = evaluate_activation(
            profile(account_id="unknown"),
            token=token("accounts"),
            accounts=[BrokerAccount("demo-123456", "DEMO")],
            present_env_keys=env,
            app=app(),
            now=NOW,
        )
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
        from mtf_lab.ops.ctrader_commands import account_discovery_command

        discovery = account_discovery_command(
            token("accounts"),
            [{"account_id": "demo-123456", "environment": "DEMO"}],
            observed_at=NOW,
        )
        selected = select_account_command(raw, discovery, account_id="demo-123456", environment="DEMO")
        self.assertEqual(selected["config_patch"]["ctrader"]["account_id"], "demo-123456")
        with self.assertRaisesRegex(ValueError, "AccountDiscovery"):
            select_account_command(raw, [], account_id="demo-123456", environment="DEMO")


class OAuthHelperTests(unittest.TestCase):
    def test_authorization_url_and_callback_are_loopback_and_scoped(self) -> None:
        from mtf_lab.ops.ctrader_activation import build_authorization_url, parse_callback_uri

        url = build_authorization_url(app(), client_id="client-public", scope="accounts", state="state-1")
        self.assertIn("scope=accounts", url)
        with self.assertRaises(ActivationError):
            build_authorization_url(app(), client_id="client-public", scope=["accounts", "trading"], state="state-1")
        self.assertIn("state=state-1", url)
        self.assertEqual(
            parse_callback_uri(
                "http://127.0.0.1:8767/oauth/callback?code=abc&state=state-1",
                expected_state="state-1",
                registered_uri=app().redirect_uri,
            ),
            "abc",
        )
        with self.assertRaises(ActivationError):
            parse_callback_uri(
                "http://127.0.0.1:8767/oauth/callback?code=abc&state=wrong",
                expected_state="state-1",
                registered_uri=app().redirect_uri,
            )

    def test_token_exchange_redacts_secrets_and_rotation_payload(self) -> None:
        from mtf_lab.ops.ctrader_activation import exchange_authorization_code, refresh_access_token

        seen = []

        def requester(url, params, timeout):
            seen.append((url, dict(params), timeout))
            return {
                "accessToken": "access-secret",
                "refreshToken": "refresh-secret",
                "expiresIn": 60,
                "tokenType": "bearer",
            }

        payload = exchange_authorization_code(
            app(), client_id="client", client_secret="secret", code="code", requester=requester
        )
        self.assertNotIn("access-secret", repr(payload))
        self.assertEqual(payload.expires_in, 60)
        refreshed = refresh_access_token(
            app(), client_id="client", client_secret="secret", refresh_token="old", requester=requester
        )
        self.assertEqual(refreshed.refresh_token, "refresh-secret")
        self.assertEqual(seen[0][1]["grant_type"], "authorization_code")
        self.assertEqual(seen[1][1]["grant_type"], "refresh_token")


class ResumableLoopbackTests(unittest.TestCase):
    def test_callback_parser_is_exact_and_browser_requires_explicit_gate(self) -> None:
        from mtf_lab.ops.ctrader_activation import (
            build_authorization_url,
            open_authorization_browser,
            parse_callback_uri,
        )

        url = build_authorization_url(
            app(),
            client_id="public-client",
            scope=["accounts"],
            state="state-value",
        )
        opened: list[str] = []
        with self.assertRaises(ActivationError):
            open_authorization_browser(url, opener=lambda value: opened.append(value))
        self.assertEqual(opened, [])
        self.assertTrue(
            open_authorization_browser(
                url,
                allow_browser=True,
                opener=lambda value: opened.append(value) or True,
            )
        )
        self.assertEqual(opened, [url])

        good = "http://127.0.0.1:8767/oauth/callback?code=one&state=state-value"
        self.assertEqual(
            parse_callback_uri(
                good,
                expected_state="state-value",
                registered_uri=app().redirect_uri,
            ),
            "one",
        )
        bad_callbacks = (
            good + "&code=two",
            good + "&unknown=x",
            good + "#fragment",
            "http://127.0.0.1:8767/oauth/other?code=one&state=state-value",
            "http://127.0.0.1:8767/oauth/callback?code=one&state=wrong",
            "http://127.0.0.1:8767/oauth/callback?code=one&state=state-value&state=state-value",
        )
        for callback in bad_callbacks:
            with self.subTest(callback=callback), self.assertRaises(ActivationError):
                parse_callback_uri(
                    callback,
                    expected_state="state-value",
                    registered_uri=app().redirect_uri,
                )

    def test_restart_safe_exchange_refresh_and_account_discovery(self) -> None:
        from mtf_lab.ops.ctrader_activation import (
            LoopbackOAuthAssistant,
            OAuthAttemptPhase,
            OAuthAttemptStore,
            record_account_discovery,
            select_discovered_demo_account,
        )

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            project = base / "project"
            project.mkdir()
            attempt_store = OAuthAttemptStore.for_fixture(
                base / "attempts",
                project_root=project,
            )
            token_store = SecureTokenStore.for_fixture(
                base / "tokens",
                project_root=project,
            )
            assistant = LoopbackOAuthAssistant(
                app(),
                attempts=attempt_store,
                tokens=token_store,
                fixture_mode=True,
            )
            opened: list[str] = []
            attempt = assistant.begin(
                client_id="public-client",
                requested_scopes=["accounts"],
                open_browser=False,
                opener=lambda value: opened.append(value),
                now=NOW,
            )
            self.assertEqual(opened, [])
            self.assertEqual(attempt.phase, OAuthAttemptPhase.AWAITING_CALLBACK)
            self.assertNotIn(attempt.csrf_state, repr(attempt))
            self.assertEqual(
                assistant.resume(attempt.attempt_id, now=NOW)["phase"],
                "AWAITING_CALLBACK",
            )
            hidden_handoff = assistant.resume_authorization(attempt.attempt_id, now=NOW)
            self.assertEqual(hidden_handoff["authorization_url"], "REDACTED")
            revealed_handoff = assistant.resume_authorization(
                attempt.attempt_id,
                reveal_url=True,
                now=NOW,
            )
            self.assertEqual(revealed_handoff["authorization_url"], attempt.authorization_url)
            self.assertFalse(revealed_handoff["browser_opened"])

            callback = f"http://127.0.0.1:8767/oauth/callback?code=short-lived&state={attempt.csrf_state}"
            assistant.receive_callback(attempt.attempt_id, callback, now=NOW)

            # New object, same private stores: this proves restart/resume.
            resumed = LoopbackOAuthAssistant(
                app(),
                attempts=OAuthAttemptStore.for_fixture(
                    base / "attempts",
                    project_root=project,
                ),
                tokens=SecureTokenStore.for_fixture(
                    base / "tokens",
                    project_root=project,
                ),
                fixture_mode=True,
            )
            self.assertEqual(
                resumed.resume(attempt.attempt_id, now=NOW)["phase"],
                "CALLBACK_RECEIVED",
            )
            calls: list[tuple[str, dict[str, str], float]] = []

            def requester(url: str, params: dict[str, str], timeout: float) -> dict[str, object]:
                calls.append((url, params, timeout))
                suffix = "refresh" if params["grant_type"] == "refresh_token" else "exchange"
                return {
                    "accessToken": f"access-{suffix}",
                    "refreshToken": f"refresh-{suffix}",
                    "expiresIn": 3600,
                    "tokenType": "bearer",
                }

            metadata = resumed.exchange(
                attempt.attempt_id,
                client_id="public-client",
                client_secret="runtime-only",
                token_ref="fixture-auth",
                observed_scopes=["accounts"],
                requester=requester,
                now=NOW,
            )
            self.assertEqual(metadata.generation, 1)
            self.assertEqual(
                resumed.resume(attempt.attempt_id, now=NOW)["phase"],
                "TOKEN_STORED",
            )
            self.assertIsNone(attempt_store.load(attempt.attempt_id).authorization_code)
            discovery = record_account_discovery(
                metadata,
                [
                    {"account_id": "demo-1", "environment": "DEMO"},
                    {"account_id": "real-1", "environment": "REAL"},
                ],
                observed_at=NOW,
            )
            selected = select_discovered_demo_account(
                discovery,
                "demo-1",
                environment="DEMO",
                token_ref="fixture-auth",
            )
            self.assertEqual(selected.environment, "DEMO")
            with self.assertRaises(RealAccountForbidden):
                select_discovered_demo_account(
                    discovery,
                    "real-1",
                    environment="DEMO",
                    token_ref="fixture-auth",
                )
            refreshed = resumed.refresh(
                "fixture-auth",
                client_id="public-client",
                client_secret="runtime-only",
                observed_scopes=["accounts"],
                requester=requester,
                now=NOW + timedelta(minutes=1),
            )
            self.assertEqual(refreshed.generation, 2)
            self.assertEqual(calls[0][1]["grant_type"], "authorization_code")
            self.assertEqual(calls[1][1]["grant_type"], "refresh_token")
            self.assertNotIn("runtime-only", repr(resumed.resume(attempt.attempt_id, now=NOW)))

    def test_fixture_store_cannot_overwrite_real_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            project = base / "project"
            project.mkdir()
            real_store = SecureTokenStore(base / "real-tokens", project_root=project)
            fixture_store = SecureTokenStore.for_fixture(
                base / "fixture-tokens",
                project_root=project,
            )
            kwargs = {
                "access_token": "fixture-access",
                "refresh_token": "fixture-refresh",
                "granted_scopes": ["accounts"],
                "expires_at": NOW + timedelta(hours=1),
                "now": NOW,
            }
            with self.assertRaises(UnsafeTokenStore):
                real_store.rotate("same-ref", fixture_payload=True, **kwargs)
            self.assertFalse(real_store.root.exists())
            with self.assertRaises(UnsafeTokenStore):
                fixture_store.rotate("same-ref", **kwargs)
            self.assertFalse(fixture_store.root.exists())
            fixture_store.rotate("same-ref", fixture_payload=True, **kwargs)
            self.assertTrue((fixture_store.root / "same-ref.json").exists())
            self.assertFalse(real_store.root.exists())

    def test_no_transport_means_no_network_fallback(self) -> None:
        from mtf_lab.ops.ctrader_activation import exchange_authorization_code

        with self.assertRaisesRegex(ActivationError, "transporte explícito"):
            exchange_authorization_code(
                app(),
                client_id="public-client",
                client_secret="runtime-only",
                code="short-lived",
                requester=None,
            )

    def test_observed_scopes_are_not_inferred_from_request(self) -> None:
        from mtf_lab.ops.ctrader_activation import (
            LoopbackOAuthAssistant,
            OAuthAttemptStore,
        )

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            project = base / "project"
            project.mkdir()
            attempts = OAuthAttemptStore.for_fixture(base / "attempts", project_root=project)
            tokens = SecureTokenStore.for_fixture(base / "tokens", project_root=project)
            assistant = LoopbackOAuthAssistant(
                app(),
                attempts=attempts,
                tokens=tokens,
                fixture_mode=True,
            )
            attempt = assistant.begin(
                client_id="public-client",
                requested_scopes=["trading"],
                now=NOW,
            )
            assistant.receive_callback(
                attempt.attempt_id,
                f"http://127.0.0.1:8767/oauth/callback?code=short-lived&state={attempt.csrf_state}",
                now=NOW,
            )
            calls: list[object] = []
            with self.assertRaisesRegex(ActivationError, "scopes observados"):
                assistant.exchange(
                    attempt.attempt_id,
                    client_id="public-client",
                    client_secret="runtime-only",
                    token_ref="fixture-trading",
                    observed_scopes=["accounts"],
                    requester=lambda *args: calls.append(args),
                    now=NOW,
                )
            self.assertEqual(calls, [])
            self.assertFalse((tokens.root / "fixture-trading.json").exists())
            with self.assertRaisesRegex(ActivationError, "código OAuth caducó"):
                assistant.exchange(
                    attempt.attempt_id,
                    client_id="public-client",
                    client_secret="runtime-only",
                    token_ref="fixture-trading",
                    observed_scopes=["trading"],
                    requester=lambda *args: calls.append(args),
                    now=NOW + timedelta(seconds=60),
                )
            self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
