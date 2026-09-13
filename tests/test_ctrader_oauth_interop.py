from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import tempfile
import traceback
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlsplit

from mtf_lab.ops import cli
from mtf_lab.ops.ctrader_activation import (
    ActivationError,
    LoopbackOAuthAssistant,
    OAuthAppConfig,
    OAuthAttemptStore,
    OAuthTokenCandidate,
    OAuthTokenPayload,
    RealAccountForbidden,
    ReauthorizationRequired,
    SecureTokenStore,
    UnsafeTokenStore,
    VerifiedAccountAuthorization,
    open_authorization_browser,
    scopes_from_permission_scope,
    validate_demo_server_endpoint,
    verify_server_account_discovery,
    verify_server_demo_discovery,
)
from mtf_lab.ops.ctrader_oauth_http import (
    OAuthHTTPError,
    build_token_request,
    request_token,
)

NOW = datetime(2026, 9, 12, 12, 0, tzinfo=UTC)
ROOT = Path(__file__).parents[1]


class _Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, _limit: int) -> bytes:
        return self._body


def _app() -> OAuthAppConfig:
    return OAuthAppConfig(
        client_id_env="CTRADER_CLIENT_ID",
        client_secret_env="CTRADER_CLIENT_SECRET",
        redirect_uri="http://127.0.0.1:8767/oauth/callback",
        authorization_url="https://id.ctrader.com/my/settings/openapi/grantingaccess/",
        token_url="https://openapi.ctrader.com/apps/token",
    )


def _write_query_config(base: Path) -> tuple[Path, Path]:
    token_dir = base / "real-token-store"
    text = (ROOT / "config" / "ctrader_query.toml").read_text(encoding="utf-8")
    text = text.replace(
        'token_store_dir = "~/.local/state/mtf-lab/ctrader-tokens"',
        f'token_store_dir = "{token_dir}"',
    )
    target = base / "query.toml"
    target.write_text(text, encoding="utf-8")
    return target, token_dir


def _run_cli(argv: list[str]) -> tuple[int, dict[str, object]]:
    output = io.StringIO()
    errors = io.StringIO()
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
        code = cli.main(argv)
    raw = output.getvalue().strip()
    if not raw:
        raise AssertionError(f"CLI sin JSON; stderr={errors.getvalue()!r}")
    return code, json.loads(raw)


class CTraderOAuthInteropTests(unittest.TestCase):
    def test_official_permission_scope_values_are_exact(self) -> None:
        self.assertEqual(scopes_from_permission_scope(0), frozenset({"accounts"}))
        self.assertEqual(scopes_from_permission_scope("SCOPE_VIEW"), frozenset({"accounts"}))
        self.assertEqual(
            scopes_from_permission_scope(1),
            frozenset({"accounts", "trading"}),
        )
        self.assertEqual(
            scopes_from_permission_scope("SCOPE_TRADE"),
            frozenset({"accounts", "trading"}),
        )
        for value in (True, 2, "FULL_ACCESS", "trading", None):
            with self.subTest(value=value), self.assertRaises(ActivationError):
                scopes_from_permission_scope(value)

    def test_oauth_trading_scope_normalizes_to_accounts_and_trading(self) -> None:
        payload = OAuthTokenPayload.from_response(
            {
                "accessToken": "access-fixture",
                "refreshToken": "refresh-fixture",
                "expiresIn": 3600,
                "scope": "trading",
            }
        )
        self.assertEqual(payload.scopes, frozenset({"accounts", "trading"}))

    def test_binding_endpoint_accepts_only_canonical_demo_endpoints(self) -> None:
        self.assertEqual(validate_demo_server_endpoint("demo.ctraderapi.com:5035"), "demo.ctraderapi.com:5035")
        with self.assertRaises(RealAccountForbidden):
            validate_demo_server_endpoint("live.ctraderapi.com:5035")

    def test_candidate_separates_store_and_connection_generations(self) -> None:
        candidate = OAuthTokenCandidate(
            payload=OAuthTokenPayload(access_token="access-fixture", expires_in=3600),
            token_ref="query-fixture",
            token_url=_app().token_url,
            store_generation=4,
            server_endpoint="demo.ctraderapi.com:5035",
        )
        bound = candidate.bind_connection(9)
        self.assertEqual(bound.store_generation, 4)
        self.assertEqual(bound.connection_generation, 9)
        with self.assertRaises(ActivationError):
            candidate.bind_connection(0)

    def test_installed_official_descriptor_matches_permission_scope_contract(self) -> None:
        from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as messages

        field = messages.ProtoOAGetAccountListByAccessTokenRes.DESCRIPTOR.fields_by_name["permissionScope"]
        self.assertEqual(field.enum_type.full_name, "ProtoOAClientPermissionScope")
        self.assertEqual(
            [(item.name, item.number) for item in field.enum_type.values],
            [("SCOPE_VIEW", 0), ("SCOPE_TRADE", 1)],
        )

    def test_server_scope_requires_selected_server_observed_demo(self) -> None:
        discovery = {
            "permissionScope": "SCOPE_VIEW",
            "records": [
                {"account_id": 202, "environment": "DEMO"},
            ],
        }
        verified = verify_server_account_discovery(
            discovery,
            requested_scopes=["accounts"],
            selected_account_id=202,
        )
        self.assertIsInstance(verified, VerifiedAccountAuthorization)
        self.assertEqual(verified.selected_account_id, "202")
        self.assertEqual(verified.environment, "DEMO")
        self.assertEqual(verified.granted_scopes, frozenset({"accounts"}))
        with self.assertRaises(RealAccountForbidden):
            verify_server_account_discovery(
                {
                    "accessToken": "access-fixture",
                    "permissionScope": "SCOPE_VIEW",
                    "records": [
                        {"account_id": 101, "environment": "REAL"},
                        {"account_id": 202, "environment": "DEMO"},
                    ],
                },
                requested_scopes=["accounts"],
                selected_account_id=101,
            )

    def test_server_probe_rejects_mixed_real_and_demo_grant(self) -> None:
        with self.assertRaises(RealAccountForbidden):
            verify_server_demo_discovery(
                {
                    "accessToken": "access-a-fixture",
                    "permissionScope": "SCOPE_VIEW",
                    "records": [
                        {"account_id": 101, "environment": "REAL"},
                        {"account_id": 202, "environment": "DEMO"},
                    ],
                },
                requested_scopes=["accounts"],
            )

    def test_requested_trading_cannot_be_inferred_from_view_permission(self) -> None:
        with self.assertRaisesRegex(ActivationError, "permissionScope"):
            verify_server_account_discovery(
                {
                    "accessToken": "access-fixture",
                    "permissionScope": "SCOPE_VIEW",
                    "records": [{"account_id": 202, "environment": "DEMO"}],
                },
                requested_scopes=["accounts", "trading"],
                selected_account_id=202,
            )

    def test_view_only_request_rejects_overbroad_trade_permission(self) -> None:
        with self.assertRaisesRegex(ActivationError, "permissionScope"):
            verify_server_demo_discovery(
                {
                    "permissionScope": "SCOPE_TRADE",
                    "records": [{"account_id": 202, "environment": "DEMO"}],
                },
                requested_scopes=["accounts"],
            )

    def test_trading_request_maps_to_full_official_permission_scope(self) -> None:
        verified = verify_server_demo_discovery(
            {
                "permissionScope": "SCOPE_TRADE",
                "records": [{"account_id": 202, "environment": "DEMO"}],
            },
            requested_scopes=["trading"],
        )
        self.assertEqual(verified.granted_scopes, frozenset({"accounts", "trading"}))

    def test_authorization_code_uses_documented_get_query_shape(self) -> None:
        request = build_token_request(
            "https://openapi.ctrader.com/apps/token",
            {
                "grant_type": "authorization_code",
                "code": "code-fixture",
                "redirect_uri": "http://127.0.0.1:8767/oauth/callback",
                "client_id": "client-fixture",
                "client_secret": "secret-fixture",
            },
        )
        self.assertEqual(request.method, "GET")
        self.assertIsNone(request.data)
        query = parse_qs(urlsplit(request.full_url).query)
        self.assertEqual(query["grant_type"], ["authorization_code"])
        self.assertEqual(query["code"], ["code-fixture"])
        self.assertEqual(query["client_secret"], ["secret-fixture"])

    def test_refresh_uses_documented_post_query_shape(self) -> None:
        request = build_token_request(
            "https://openapi.ctrader.com/apps/token",
            {
                "grant_type": "refresh_token",
                "refresh_token": "refresh-fixture",
                "client_id": "client-fixture",
                "client_secret": "secret-fixture",
            },
        )
        self.assertEqual(request.method, "POST")
        self.assertIsNone(request.data)
        query = parse_qs(urlsplit(request.full_url).query)
        self.assertEqual(query["grant_type"], ["refresh_token"])
        self.assertEqual(query["refresh_token"], ["refresh-fixture"])

    def test_http_helper_returns_json_without_echoing_url_or_secret_on_failure(self) -> None:
        seen: list[tuple[str, str, float]] = []

        def opener(request: object, *, timeout: float) -> _Response:
            assert hasattr(request, "method")
            seen.append((str(request.method), str(request.full_url), timeout))
            return _Response(
                {
                    "accessToken": "access-fixture",
                    "refreshToken": "refresh-fixture",
                    "expiresIn": 3600,
                    "tokenType": "bearer",
                }
            )

        payload = request_token(
            "https://openapi.ctrader.com/apps/token",
            {
                "grant_type": "authorization_code",
                "code": "code-fixture",
                "redirect_uri": "http://127.0.0.1:8767/oauth/callback",
                "client_id": "client-fixture",
                "client_secret": "secret-fixture",
            },
            3,
            opener=opener,
        )
        self.assertIn("accessToken", payload)
        self.assertEqual(seen[0][0], "GET")
        self.assertNotIn("secret-fixture", str(OAuthHTTPError("solicitud OAuth falló")))

    def test_http_failure_traceback_does_not_carry_secret_url(self) -> None:
        def opener(_request: object, *, timeout: float) -> _Response:
            raise RuntimeError("https://openapi.ctrader.com/apps/token?client_secret=secret-fixture")

        try:
            request_token(
                "https://openapi.ctrader.com/apps/token",
                {
                    "grant_type": "authorization_code",
                    "code": "code-fixture",
                    "redirect_uri": "http://127.0.0.1:8767/oauth/callback",
                    "client_id": "client-fixture",
                    "client_secret": "secret-fixture",
                },
                3,
                opener=opener,
            )
        except OAuthHTTPError:
            trace = traceback.format_exc()
        else:
            self.fail("se esperaba OAuthHTTPError")
        self.assertNotIn("secret-fixture", trace)
        self.assertNotIn("openapi.ctrader.com/apps/token?client_secret", trace)

    def test_oauth_helpers_reject_secret_query_extensions(self) -> None:
        with self.assertRaises(OAuthHTTPError):
            build_token_request(
                "https://openapi.ctrader.com/apps/token",
                {
                    "grant_type": "refresh_token",
                    "refresh_token": "refresh-fixture",
                    "client_id": "client-fixture",
                    "client_secret": "secret-fixture",
                    "password": "unexpected-secret",
                },
            )
        with self.assertRaises(ActivationError):
            open_authorization_browser(
                "https://id.ctrader.com/my/settings/openapi/grantingaccess/?client_id=client&client_secret=secret",
                allow_browser=True,
                opener=lambda _url: self.fail("no se debe abrir una URL con secretos"),
            )
        opened: list[str] = []
        self.assertTrue(
            open_authorization_browser(
                "https://id.ctrader.com/authorize",
                allow_browser=True,
                opener=lambda url: opened.append(url) or True,
            )
        )
        self.assertEqual(opened, ["https://id.ctrader.com/authorize"])
        with self.assertRaises(OAuthHTTPError):
            request_token(
                "https://openapi.ctrader.com/apps/token",
                {
                    "grant_type": "refresh_token",
                    "refresh_token": "refresh-fixture",
                    "client_id": "client-fixture",
                    "client_secret": "secret-fixture",
                },
                "not-a-timeout",
            )

    def test_attempt_and_token_file_identity_aliases_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            project.mkdir()
            attempt_root = root / "attempts"
            attempt_store = OAuthAttemptStore.for_fixture(attempt_root, project_root=project)
            assistant = LoopbackOAuthAssistant(
                _app(),
                attempts=attempt_store,
                tokens=SecureTokenStore.for_fixture(root / "tokens", project_root=project),
                fixture_mode=True,
            )
            attempt = assistant.begin(client_id="client-fixture", requested_scopes=["accounts"], token_ref="query")
            attempt_path = attempt_root / f".oauth-attempt-{attempt.attempt_id}.json"
            hardlink = root / "attempt-hardlink.json"
            hardlink.hardlink_to(attempt_path)
            with self.assertRaises(UnsafeTokenStore):
                attempt_store.load(attempt.attempt_id)
            with self.assertRaises(UnsafeTokenStore):
                attempt_store.save(attempt)
            hardlink.unlink()

            copied_path = attempt_root / ".oauth-attempt-other.json"
            shutil.copyfile(attempt_path, copied_path)
            os.chmod(copied_path, 0o600)
            with self.assertRaises(UnsafeTokenStore):
                attempt_store.load("other")

            token_store = SecureTokenStore.for_fixture(root / "tokens-identity", project_root=project)
            token_store.rotate(
                "source",
                access_token="access-fixture",
                refresh_token="refresh-fixture",
                granted_scopes=["accounts"],
                expires_at=NOW + timedelta(hours=1),
                now=NOW,
                fixture_payload=True,
            )
            shutil.copyfile(root / "tokens-identity/source.json", root / "tokens-identity/other.json")
            os.chmod(root / "tokens-identity/other.json", 0o600)
            with self.assertRaises(UnsafeTokenStore):
                token_store.read("other")

    def test_refresh_transaction_blocks_reuse_and_requires_explicit_reauth(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            project.mkdir()
            token_root = root / "tokens"
            attempt_root = root / "attempts"
            tokens = SecureTokenStore.for_fixture(token_root, project_root=project)
            tokens.rotate(
                "query",
                access_token="old-access-fixture",
                refresh_token="one-use-refresh-fixture",
                granted_scopes=["accounts"],
                expires_at=NOW + timedelta(hours=1),
                now=NOW,
                fixture_payload=True,
            )
            first = LoopbackOAuthAssistant(
                _app(),
                attempts=OAuthAttemptStore.for_fixture(attempt_root, project_root=project),
                tokens=tokens,
                fixture_mode=True,
            )
            calls: list[str] = []

            def requester(_url: str, params: dict[str, str], _timeout: float) -> dict[str, object]:
                calls.append(params.get("refresh_token", params.get("code", "")))
                return {
                    "accessToken": "new-access-fixture",
                    "refreshToken": "new-refresh-fixture",
                    "expiresIn": 3600,
                }

            candidate = first.refresh_unpersisted(
                "query",
                client_id="client-fixture",
                client_secret="secret-fixture",
                requester=requester,
            )
            self.assertEqual(calls, ["one-use-refresh-fixture"])
            self.assertEqual(tokens.transaction_state("query"), "IN_FLIGHT")
            marker = (token_root / ".oauth-transaction-query.json").read_text(encoding="utf-8")
            self.assertNotIn("one-use-refresh-fixture", marker)
            self.assertNotIn("secret-fixture", marker)
            self.assertRaises(
                ReauthorizationRequired,
                first.refresh_unpersisted,
                "query",
                client_id="client-fixture",
                client_secret="secret-fixture",
                requester=requester,
            )
            with self.assertRaises(ReauthorizationRequired):
                tokens.read("query")
            first.abandon_candidate(candidate)
            self.assertEqual(tokens.transaction_state("query"), "UNKNOWN")
            with self.assertRaises(ReauthorizationRequired):
                first.refresh_unpersisted(
                    "query",
                    client_id="client-fixture",
                    client_secret="secret-fixture",
                    requester=requester,
                )
            reauth_attempt = first.begin(
                client_id="client-fixture",
                requested_scopes=["accounts"],
                token_ref="query",
                now=NOW,
            )
            first.prepare_reauthorization(
                reauth_attempt,
                client_id="client-fixture",
                profile_scopes=["accounts"],
            )
            self.assertEqual(tokens.transaction_state("query"), "REAUTH_PENDING")
            with self.assertRaises(ReauthorizationRequired):
                tokens.read("query")
            first.receive_callback(
                reauth_attempt.attempt_id,
                f"{_app().redirect_uri}?code=reauth-code&state={reauth_attempt.csrf_state}",
                now=NOW,
            )
            exchanged = first.exchange_unpersisted(
                reauth_attempt.attempt_id,
                token_ref="query",
                server_endpoint="demo.ctraderapi.com:5035",
                client_id="client-fixture",
                client_secret="secret-fixture",
                profile_scopes=["accounts"],
                requester=requester,
                now=NOW,
            ).bind_connection(1)
            verification = verify_server_demo_discovery(
                {
                    "accessToken": "new-access-fixture",
                    "permissionScope": "SCOPE_VIEW",
                    "records": [{"account_id": 202, "environment": "DEMO"}],
                },
                requested_scopes=["accounts"],
                token_candidate=exchanged,
            )
            refreshed = first.persist_verified_token(
                reauth_attempt.attempt_id,
                token_ref="query",
                candidate=exchanged,
                verification=verification,
                server_endpoint="demo.ctraderapi.com:5035",
                now=NOW,
            )
            self.assertEqual(refreshed.generation, 2)
            self.assertIsNone(tokens.transaction_state("query"))
            self.assertEqual(calls, ["one-use-refresh-fixture", "reauth-code"])

    def test_exchange_binds_attempt_to_current_profile_before_request(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            project.mkdir()
            attempt_store = OAuthAttemptStore.for_fixture(root / "attempts", project_root=project)
            assistant = LoopbackOAuthAssistant(
                _app(),
                attempts=attempt_store,
                tokens=SecureTokenStore.for_fixture(root / "tokens", project_root=project),
                fixture_mode=True,
            )
            attempt = assistant.begin(
                client_id="client-fixture",
                requested_scopes=["trading"],
                token_ref="execution",
                now=NOW,
            )
            assistant.receive_callback(
                attempt.attempt_id,
                f"{_app().redirect_uri}?code=code-fixture&state={attempt.csrf_state}",
                now=NOW,
            )
            calls: list[dict[str, str]] = []
            with self.assertRaises(ReauthorizationRequired):
                assistant.exchange_unpersisted(
                    attempt.attempt_id,
                    token_ref="execution",
                    client_id="client-fixture",
                    client_secret="secret-fixture",
                    profile_scopes=["accounts"],
                    requester=lambda _url, params, _timeout: calls.append(dict(params)) or {},
                    now=NOW,
                )
            self.assertEqual(calls, [])
            self.assertIsNone(assistant.tokens.transaction_state("execution"))

    def test_failed_attempt_persistence_leaves_token_outcome_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            project.mkdir()
            token_root = root / "tokens"
            attempts = OAuthAttemptStore(token_root, project_root=project)
            tokens = SecureTokenStore(token_root, project_root=project)
            assistant = LoopbackOAuthAssistant(_app(), attempts=attempts, tokens=tokens)
            attempt = assistant.begin(
                client_id="client-fixture",
                requested_scopes=["accounts"],
                token_ref="query-fixture",
                now=NOW,
            )
            assistant.receive_callback(
                attempt.attempt_id,
                f"{_app().redirect_uri}?code=code-fixture&state={attempt.csrf_state}",
                now=NOW,
            )
            candidate = assistant.exchange_unpersisted(
                attempt.attempt_id,
                token_ref="query-fixture",
                server_endpoint="demo.ctraderapi.com:5035",
                client_id="client-fixture",
                client_secret="secret-fixture",
                requester=lambda _url, _params, _timeout: {
                    "accessToken": "access-fixture",
                    "refreshToken": "refresh-fixture",
                    "expiresIn": 3600,
                },
                now=NOW,
            ).bind_connection(1)
            verification = verify_server_demo_discovery(
                {
                    "accessToken": "access-fixture",
                    "permissionScope": "SCOPE_VIEW",
                    "records": [{"account_id": 202, "environment": "DEMO"}],
                },
                requested_scopes=["accounts"],
                token_candidate=candidate,
            )
            with (
                mock.patch.object(attempts, "save", side_effect=OSError("attempt store fault")),
                self.assertRaises(OSError),
            ):
                assistant.persist_verified_token(
                    attempt.attempt_id,
                    token_ref="query-fixture",
                    candidate=candidate,
                    verification=verification,
                    server_endpoint="demo.ctraderapi.com:5035",
                    now=NOW,
                )
            self.assertEqual(tokens.transaction_state("query-fixture"), "UNKNOWN")
            with self.assertRaises(ReauthorizationRequired):
                tokens.read("query-fixture")
            self.assertEqual(attempts.load(attempt.attempt_id).phase.value, "CALLBACK_RECEIVED")

    def test_cli_real_exchange_probes_demo_permission_before_persisting(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            config, token_root = _write_query_config(base)
            attempts = OAuthAttemptStore(token_root, project_root=ROOT)
            tokens = SecureTokenStore(token_root, project_root=ROOT)
            assistant = LoopbackOAuthAssistant(_app(), attempts=attempts, tokens=tokens)
            attempt = assistant.begin(
                client_id="client-fixture",
                requested_scopes=["accounts"],
                token_ref="ctrader-query-demo",
            )
            assistant.receive_callback(
                attempt.attempt_id,
                f"{_app().redirect_uri}?code=code-fixture&state={attempt.csrf_state}",
            )
            calls: list[str] = []
            requests: list[tuple[str, dict[str, str], float]] = []

            class Provider:
                def __init__(self, _config: object) -> None:
                    self.status = type("Status", (), {"generation": 1})()

                def connect(self) -> None:
                    calls.append("connect")

                def authenticate(self, *, secret_provider, token_provider, authorize_selected=True) -> None:
                    assert authorize_selected is False
                    assert secret_provider("CTRADER_CLIENT_SECRET") == "secret-fixture"
                    assert token_provider("ctrader-query-demo") == "access-fixture"
                    calls.append("application_auth")

                def discover_accounts(self, *, include_token: bool = False) -> dict[str, object]:
                    calls.append("account_discovery")
                    return {
                        "accessToken": "access-fixture",
                        "permissionScope": "SCOPE_VIEW",
                        "records": [{"account_id": 7, "environment": "DEMO"}],
                    }

                def close(self) -> None:
                    calls.append("close")

            def fake_request(url: str, params: dict[str, str], timeout: float) -> dict[str, object]:
                requests.append((url, dict(params), timeout))
                return {
                    "accessToken": "access-fixture",
                    "refreshToken": "refresh-fixture",
                    "expiresIn": 3600,
                    "tokenType": "bearer",
                }

            with (
                mock.patch.dict(
                    os.environ,
                    {"CTRADER_CLIENT_ID": "client-fixture", "CTRADER_CLIENT_SECRET": "secret-fixture"},
                    clear=False,
                ),
                mock.patch("mtf_lab.ops.ctrader_cli_services.request_token", fake_request),
                mock.patch("mtf_lab.data.ctrader.CTraderProvider", Provider),
            ):
                code, output = _run_cli(
                    [
                        "ctrader",
                        "token-exchange",
                        "--config",
                        str(config),
                        "--attempt-id",
                        attempt.attempt_id,
                    ]
                )
            self.assertEqual(code, 0)
            self.assertEqual(calls, ["connect", "application_auth", "account_discovery", "close"])
            self.assertEqual(requests[0][1]["grant_type"], "authorization_code")
            self.assertEqual(output["permission"]["permission_scope"], "SCOPE_VIEW")
            self.assertNotIn("access-fixture", json.dumps(output))
            self.assertNotIn("refresh-fixture", json.dumps(output))
            metadata = SecureTokenStore(token_root, project_root=ROOT).metadata("ctrader-query-demo")
            self.assertEqual(metadata.granted_scopes, frozenset({"accounts"}))
            discovery = token_root / "account-discovery.json"
            self.assertTrue(discovery.exists())
            self.assertEqual(discovery.stat().st_mode & 0o777, 0o600)

    def test_cli_real_exchange_rejects_real_inventory_before_token_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            config, token_root = _write_query_config(base)
            attempts = OAuthAttemptStore(token_root, project_root=ROOT)
            tokens = SecureTokenStore(token_root, project_root=ROOT)
            assistant = LoopbackOAuthAssistant(_app(), attempts=attempts, tokens=tokens)
            attempt = assistant.begin(
                client_id="client-fixture",
                requested_scopes=["accounts"],
                token_ref="ctrader-query-demo",
            )
            assistant.receive_callback(
                attempt.attempt_id,
                f"{_app().redirect_uri}?code=code-fixture&state={attempt.csrf_state}",
            )

            class Provider:
                def __init__(self, _config: object) -> None:
                    self.status = type("Status", (), {"generation": 1})()

                def connect(self) -> None:
                    return None

                def authenticate(self, *, secret_provider, token_provider, authorize_selected=True) -> None:
                    assert authorize_selected is False
                    secret_provider("CTRADER_CLIENT_SECRET")
                    token_provider("ctrader-query-demo")

                def discover_accounts(self, *, include_token: bool = False) -> dict[str, object]:
                    return {
                        "accessToken": "access-fixture",
                        "permissionScope": "SCOPE_VIEW",
                        "records": [
                            {"account_id": 7, "environment": "DEMO"},
                            {"account_id": 8, "environment": "REAL"},
                        ],
                    }

                def close(self) -> None:
                    return None

            def fake_request(_url: str, _params: dict[str, str], _timeout: float) -> dict[str, object]:
                return {
                    "accessToken": "access-fixture",
                    "refreshToken": "refresh-fixture",
                    "expiresIn": 3600,
                    "tokenType": "bearer",
                }

            with (
                mock.patch.dict(
                    os.environ,
                    {"CTRADER_CLIENT_ID": "client-fixture", "CTRADER_CLIENT_SECRET": "secret-fixture"},
                    clear=False,
                ),
                mock.patch("mtf_lab.ops.ctrader_cli_services.request_token", fake_request),
                mock.patch("mtf_lab.data.ctrader.CTraderProvider", Provider),
            ):
                code, output = _run_cli(
                    [
                        "ctrader",
                        "token-exchange",
                        "--config",
                        str(config),
                        "--attempt-id",
                        attempt.attempt_id,
                    ]
                )
            self.assertEqual(code, 2)
            self.assertEqual(output["state"], "RealAccountForbidden")
            self.assertFalse((token_root / "ctrader-query-demo.json").exists())
            self.assertEqual(
                SecureTokenStore(token_root, project_root=ROOT).transaction_state("ctrader-query-demo"), "UNKNOWN"
            )
            self.assertNotIn("access-fixture", json.dumps(output))

    def test_cli_real_refresh_uses_post_query_transport_and_redacts_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config, token_root = _write_query_config(Path(temp))
            store = SecureTokenStore(token_root, project_root=ROOT)
            store.rotate(
                "ctrader-query-demo",
                access_token="old-access-fixture",
                refresh_token="old-refresh-fixture",
                granted_scopes=["accounts"],
                expires_at=datetime.now(UTC),
            )
            requests: list[tuple[str, dict[str, str], float]] = []

            class Provider:
                def __init__(self, _config: object) -> None:
                    self.status = type("Status", (), {"generation": 1})()

                def connect(self) -> None:
                    return None

                def authenticate(self, *, secret_provider, token_provider, authorize_selected=True) -> None:
                    assert authorize_selected is False
                    secret_provider("CTRADER_CLIENT_SECRET")
                    token_provider("ctrader-query-demo")

                def discover_accounts(self, *, include_token: bool = False) -> dict[str, object]:
                    return {
                        "accessToken": "new-access-fixture",
                        "permissionScope": "SCOPE_VIEW",
                        "records": [{"account_id": 7, "environment": "DEMO"}],
                    }

                def close(self) -> None:
                    return None

            def fake_request(url: str, params: dict[str, str], timeout: float) -> dict[str, object]:
                requests.append((url, dict(params), timeout))
                return {
                    "accessToken": "new-access-fixture",
                    "refreshToken": "new-refresh-fixture",
                    "expiresIn": 3600,
                    "tokenType": "bearer",
                }

            with (
                mock.patch.dict(
                    os.environ,
                    {"CTRADER_CLIENT_ID": "client-fixture", "CTRADER_CLIENT_SECRET": "secret-fixture"},
                    clear=False,
                ),
                mock.patch("mtf_lab.ops.ctrader_cli_services.request_token", fake_request),
                mock.patch("mtf_lab.data.ctrader.CTraderProvider", Provider),
            ):
                code, output = _run_cli(
                    [
                        "ctrader",
                        "token-refresh",
                        "--config",
                        str(config),
                    ]
                )
            self.assertEqual(code, 0)
            self.assertEqual(requests[0][1]["grant_type"], "refresh_token")
            self.assertEqual(output["permission"]["permission_scope"], "SCOPE_VIEW")
            self.assertNotIn("new-access-fixture", json.dumps(output))
            self.assertNotIn("new-refresh-fixture", json.dumps(output))
            self.assertEqual(
                SecureTokenStore(token_root, project_root=ROOT).metadata("ctrader-query-demo").granted_scopes,
                frozenset({"accounts"}),
            )

    def test_verified_persistence_purges_code_and_stores_observed_scopes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            project.mkdir()
            token_root = root / "tokens"
            attempts = OAuthAttemptStore.for_fixture(token_root, project_root=project)
            tokens = SecureTokenStore.for_fixture(token_root, project_root=project)
            assistant = LoopbackOAuthAssistant(_app(), attempts=attempts, tokens=tokens, fixture_mode=True)
            attempt = assistant.begin(client_id="client-fixture", requested_scopes=["accounts"], now=NOW)
            assistant.receive_callback(
                attempt.attempt_id,
                f"{_app().redirect_uri}?code=code-fixture&state={attempt.csrf_state}",
                now=NOW,
            )
            payload = OAuthTokenPayload(
                access_token="access-fixture",
                refresh_token="refresh-fixture",
                expires_in=3600,
            )
            candidate = OAuthTokenCandidate(
                payload=payload,
                token_ref="query-fixture",
                token_url=_app().token_url,
                store_generation=1,
                server_endpoint="demo.ctraderapi.com:5035",
                connection_generation=1,
            )
            verification = verify_server_account_discovery(
                {
                    "accessToken": "access-fixture",
                    "permissionScope": "SCOPE_VIEW",
                    "records": [{"account_id": 202, "environment": "DEMO"}],
                },
                requested_scopes=["accounts"],
                selected_account_id=202,
                token_candidate=candidate,
            )
            metadata = assistant.persist_verified_token(
                attempt.attempt_id,
                token_ref="query-fixture",
                candidate=candidate,
                verification=verification,
                server_endpoint="demo.ctraderapi.com:5035",
                now=NOW,
            )
            self.assertEqual(metadata.granted_scopes, frozenset({"accounts"}))
            self.assertIsNone(attempts.load(attempt.attempt_id).authorization_code)
            self.assertNotIn("access-fixture", repr(payload))
            self.assertNotIn("refresh-fixture", repr(payload))

    def test_verified_persistence_rejects_cross_token_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            project.mkdir()
            token_root = root / "tokens"
            attempts = OAuthAttemptStore.for_fixture(token_root, project_root=project)
            tokens = SecureTokenStore.for_fixture(token_root, project_root=project)
            assistant = LoopbackOAuthAssistant(_app(), attempts=attempts, tokens=tokens, fixture_mode=True)
            attempt = assistant.begin(client_id="client-fixture", requested_scopes=["accounts"], now=NOW)
            assistant.receive_callback(
                attempt.attempt_id,
                f"{_app().redirect_uri}?code=code-fixture&state={attempt.csrf_state}",
                now=NOW,
            )
            candidate_a = OAuthTokenCandidate(
                payload=OAuthTokenPayload(
                    access_token="access-a-fixture",
                    refresh_token="refresh-a-fixture",
                    expires_in=3600,
                ),
                token_ref="query-fixture",
                token_url=_app().token_url,
                store_generation=1,
                server_endpoint="demo.ctraderapi.com:5035",
                connection_generation=1,
            )
            candidate_b = OAuthTokenCandidate(
                payload=OAuthTokenPayload(
                    access_token="access-b-fixture",
                    refresh_token="refresh-b-fixture",
                    expires_in=3600,
                ),
                token_ref="query-fixture",
                token_url=_app().token_url,
                store_generation=1,
                server_endpoint="demo.ctraderapi.com:5035",
                connection_generation=1,
            )
            candidate_c = OAuthTokenCandidate(
                payload=candidate_a.payload,
                token_ref="query-fixture",
                token_url=_app().token_url,
                store_generation=2,
                server_endpoint="demo.ctraderapi.com:5035",
                connection_generation=1,
            )
            evidence_a = verify_server_demo_discovery(
                {
                    "accessToken": "access-a-fixture",
                    "permissionScope": "SCOPE_VIEW",
                    "records": [{"account_id": 202, "environment": "DEMO"}],
                },
                requested_scopes=["accounts"],
                token_candidate=candidate_a,
            )
            with self.assertRaisesRegex(ActivationError, "no corresponde"):
                assistant.persist_verified_token(
                    attempt.attempt_id,
                    token_ref="query-fixture",
                    candidate=candidate_b,
                    verification=evidence_a,
                    server_endpoint="demo.ctraderapi.com:5035",
                    now=NOW,
                )
            with self.assertRaisesRegex(ActivationError, "no corresponde"):
                assistant.persist_verified_token(
                    attempt.attempt_id,
                    token_ref="query-fixture",
                    candidate=candidate_c,
                    verification=evidence_a,
                    server_endpoint="demo.ctraderapi.com:5035",
                    now=NOW,
                )
            self.assertFalse((token_root / "query-fixture.json").exists())

    def test_legacy_exchange_and_refresh_cannot_persist_production_stores(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            project = root / "project"
            project.mkdir()
            store = SecureTokenStore(root / "tokens", project_root=project)
            attempts = OAuthAttemptStore(root / "tokens", project_root=project)
            assistant = LoopbackOAuthAssistant(_app(), attempts=attempts, tokens=store, fixture_mode=False)
            with self.assertRaisesRegex(ActivationError, "fixture"):
                assistant.exchange(
                    "not-an-attempt",
                    client_id="client-fixture",
                    client_secret="secret-fixture",
                    token_ref="query-fixture",
                    observed_scopes=["accounts"],
                    requester=lambda *_args: {},
                )
            with self.assertRaisesRegex(ActivationError, "fixture"):
                assistant.refresh(
                    "query-fixture",
                    client_id="client-fixture",
                    client_secret="secret-fixture",
                    observed_scopes=["accounts"],
                    requester=lambda *_args: {},
                )
            self.assertFalse((root / "tokens").exists())


class DiscoveryShapeRegressionTests(unittest.TestCase):
    def test_non_mapping_records_fail_closed_with_activation_error(self) -> None:
        for records in ("not-an-account", b"not-an-account", [42], [None]):
            with self.subTest(records=records), self.assertRaises(ActivationError):
                verify_server_demo_discovery({"permissionScope": 0, "records": records}, requested_scopes=["accounts"])


if __name__ == "__main__":
    unittest.main()
