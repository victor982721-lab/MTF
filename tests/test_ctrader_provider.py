"""Fixtures y pruebas offline del proveedor cTrader Open API."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sys
import unittest
import socket
import struct

from mtf_lab.data.ctrader import (
    AuthState,
    CTraderAuthError,
    CTraderClient,
    CTraderConfig,
    CTraderDataError,
    CTraderInstrumentSpec,
    CTraderProtocolError,
    CTraderProvider,
    CTraderRequestCancelled,
    CTraderRequestTimeout,
    CTraderRateLimiter,
    ConnectionState,
    SdkProtobufCodec,
    TcpTlsTransport,
    DeterministicTransport,
    PAYLOAD,
    WireMessage,
    dependency_report,
    normalize_spot_event,
    normalize_trendbar,
    synthetic_spot_event,
    synthetic_trendbar,
)


UTC = timezone.utc
BASE = datetime(2026, 1, 1, tzinfo=UTC)


class CTraderProviderTests(unittest.TestCase):
    def test_config_catalog_and_secret_redaction(self) -> None:
        config = CTraderConfig()
        self.assertEqual(config.host, "demo.ctraderapi.com")
        self.assertEqual(config.port, 5035)
        self.assertEqual(config.symbol, "EUR/USD")
        self.assertEqual(config.timeframes, ("M1", "M5", "M15"))
        configured = CTraderConfig(client_id="public-id", client_secret_ref="vault://ctrader/client", access_token_ref="vault://ctrader/token", account_id=7)
        public = configured.to_dict()
        self.assertNotIn("client_secret", public)
        self.assertNotIn("access_token", public)
        self.assertTrue(public["client_secret_ref_configured"])
        with self.assertRaises(ValueError):
            CTraderConfig.from_mapping({"client_secret": "never-accept-this"})

    def test_dependency_probe_is_read_only_and_no_reactor_import(self) -> None:
        import subprocess
        probe = (
            "import sys; "
            "from mtf_lab.data.ctrader import dependency_report; "
            "assert 'twisted.internet.reactor' not in sys.modules; "
            "r=dependency_report(); "
            "assert r.sdk_state.value in {'AVAILABLE','MISSING','UNIMPORTABLE'}; "
            "assert r.protobuf_state.value in {'AVAILABLE','MISSING','UNIMPORTABLE'}; "
            "assert r.codec_state.value in {'AVAILABLE','MISSING','UNIMPORTABLE','NOT_VERIFIED'}; "
            "assert 'twisted.internet.reactor' not in sys.modules"
        )
        result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def test_relative_trendbar_scaling_and_interval(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99, digits=5, pip_position=4)
        bar = normalize_trendbar(synthetic_trendbar(timestamp_minutes=100), spec=spec, received_at=BASE + timedelta(hours=1), request_id="fixture")
        self.assertEqual(bar.interval_start, datetime.fromtimestamp(100 * 60, UTC))
        self.assertEqual(bar.interval_end, datetime.fromtimestamp(101 * 60, UTC))
        self.assertEqual(bar.open, 1.1001)
        self.assertEqual(bar.close, 1.1002)
        self.assertEqual(bar.high, 1.1003)
        self.assertEqual(bar.low, 1.1)
        self.assertTrue(bar.metadata["no_tick_interpolation"])

    def test_trendbar_does_not_claim_closed_before_interval_end(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99)
        raw = synthetic_trendbar(timestamp_minutes=int(BASE.timestamp() // 60))
        bar = normalize_trendbar(
            raw,
            spec=spec,
            received_at=BASE + timedelta(seconds=30),
            request_id="live",
            mode="LIVE",
        )
        self.assertFalse(bar.closed)
        self.assertEqual(bar.available_at, BASE + timedelta(seconds=30))
        self.assertEqual(bar.metadata["closed_evidence"], "not_observed")

    def test_trendbar_missing_required_relative_field_is_rejected(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99)
        raw = synthetic_trendbar(timestamp_minutes=100)
        del raw["deltaClose"]
        with self.assertRaises(CTraderDataError):
            normalize_trendbar(raw, spec=spec, received_at=BASE)

    def test_spot_event_preserves_bid_ask_mid_and_snapshot(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99)
        payload = synthetic_spot_event(timestamp_ms=int(BASE.timestamp() * 1000), symbol_id=99, snapshot=True)
        result = normalize_spot_event(payload, spec=spec, received_at=BASE + timedelta(seconds=1))
        self.assertEqual(len(result.quote_events), 1)
        event = result.quote_events[0]
        self.assertEqual(event.price_basis, "mid")
        self.assertEqual(event.bid, 1.1)
        self.assertEqual(event.ask, 1.1002)
        self.assertEqual(event.mid, 1.1001)
        self.assertTrue(event.is_snapshot)
        self.assertEqual(len(result.bars), 0)

    def test_optional_quote_fields_do_not_become_zero_or_another_basis(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99)
        result = normalize_spot_event({"symbolId": 99, "timestamp": int(BASE.timestamp() * 1000), "bid": 110000}, spec=spec, received_at=BASE + timedelta(seconds=1), quote_basis="mid")
        self.assertEqual(result.quote_events, ())
        self.assertTrue(any("mid" in issue for issue in result.issues))
        bid_result = normalize_spot_event({"symbolId": 99, "timestamp": int(BASE.timestamp() * 1000), "bid": 110000}, spec=spec, received_at=BASE + timedelta(seconds=1), quote_basis="bid")
        self.assertEqual(bid_result.quote_events[0].price, 1.1)

    def test_provider_retains_partial_bid_ask_legs_and_their_ages(self) -> None:
        provider = CTraderProvider(
            CTraderConfig(symbol_id=99, quote_basis="mid"),
            transport=DeterministicTransport(),
        )
        first = provider.normalize_spot(
            {"symbolId": 99, "timestamp": int(BASE.timestamp() * 1000), "bid": 110000, "ask": 110020},
            received_at=BASE + timedelta(seconds=1),
            sequence=1,
        )
        second = provider.normalize_spot(
            {"symbolId": 99, "timestamp": int((BASE + timedelta(seconds=2)).timestamp() * 1000), "bid": 110010},
            received_at=BASE + timedelta(seconds=3),
            sequence=2,
        )
        self.assertEqual(len(first.quote_events), 1)
        self.assertEqual(len(second.quote_events), 1)
        event = second.quote_events[0]
        self.assertEqual((event.bid, event.ask), (1.1001, 1.1002))
        self.assertTrue(event.metadata["partial_update"])
        self.assertEqual(event.metadata["ask_source_timestamp"], (BASE + timedelta(seconds=0)).isoformat().replace("+00:00", "Z"))
        self.assertGreater(event.metadata["ask_age_seconds"], event.metadata["bid_age_seconds"])
        self.assertNotIn("no evaluable", " ".join(second.issues))

    def test_spot_event_can_contain_multiple_trendbars(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99)
        payload = synthetic_spot_event(timestamp_ms=int(BASE.timestamp() * 1000), symbol_id=99, trendbars=[synthetic_trendbar(timestamp_minutes=1, period="M1"), synthetic_trendbar(timestamp_minutes=5, period="M5")])
        result = normalize_spot_event(payload, spec=spec, received_at=BASE + timedelta(seconds=1))
        self.assertEqual([bar.resolution for bar in result.bars], ["M1", "M5"])

    def test_request_response_correlation_preserves_unrelated_events(self) -> None:
        def handler(request: WireMessage):
            return [
                WireMessage("PROTO_OA_SPOT_EVENT", {"symbolId": 99, "timestamp": int(BASE.timestamp() * 1000), "bid": 110000, "ask": 110020}, None, True),
                WireMessage("PROTO_OA_SYMBOLS_LIST_RES", {"symbol": [{"symbolId": 99, "symbolName": "EUR/USD"}]}, request.client_msg_id),
            ]

        transport = DeterministicTransport(handler)
        client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport)
        client.connect()
        response = client.request("PROTO_OA_SYMBOLS_LIST_REQ", {"ctidTraderAccountId": 7})
        self.assertEqual(response.payload_type_name, "PROTO_OA_SYMBOLS_LIST_RES")
        event = client.poll_event()
        self.assertIsNotNone(event)
        self.assertEqual(event.payload_type_id, PAYLOAD["PROTO_OA_SPOT_EVENT"])
        self.assertEqual(transport.sent[0].client_msg_id, response.client_msg_id)

    def test_timeout_and_cancellation_are_explicit(self) -> None:
        transport = DeterministicTransport()
        client = CTraderClient(CTraderConfig(symbol_id=99, request_timeout_seconds=0.01), transport=transport)
        client.connect()
        with self.assertRaises(CTraderRequestTimeout):
            client.request("PROTO_OA_SYMBOLS_LIST_REQ", {}, timeout_seconds=0.01)
        self.assertTrue(client.status.needs_reconciliation)
        token = __import__("mtf_lab.data.ctrader", fromlist=["CancellationToken"]).CancellationToken()
        token.cancel()
        with self.assertRaises(CTraderRequestCancelled):
            client.request("PROTO_OA_SYMBOLS_LIST_REQ", {}, cancel=token)

    def test_protocol_error_reads_body_and_retry_after(self) -> None:
        def handler(request: WireMessage):
            return WireMessage("PROTO_OA_ERROR_RES", {"errorCode": "BLOCKED_PAYLOAD_TYPE", "description": "rate limited", "retryAfter": 2}, request.client_msg_id)

        transport = DeterministicTransport(handler)
        client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport)
        client.connect()
        with self.assertRaises(CTraderProtocolError) as caught:
            client.request("PROTO_OA_GET_TRENDBARS_REQ", {})
        self.assertEqual(caught.exception.error_code, "BLOCKED_PAYLOAD_TYPE")
        self.assertEqual(caught.exception.retry_after, 2)

    def test_reconnect_backoff_is_injectable_and_deterministic(self) -> None:
        delays: list[float] = []
        transport = DeterministicTransport(fail_connect_times=2)
        client = CTraderClient(CTraderConfig(symbol_id=99, max_reconnects=3, reconnect_backoff_seconds=0.25, reconnect_backoff_max_seconds=1), transport=transport, sleep=delays.append)
        status = client.connect_with_retry()
        self.assertEqual(status.connection, ConnectionState.CONNECTED)
        self.assertEqual(transport.connect_calls, 3)
        self.assertEqual(delays, [0.25, 0.5])

    def test_bounded_event_queue_makes_loss_visible_and_heartbeat_is_explicit(self) -> None:
        def handler(request: WireMessage):
            return [
                WireMessage("PROTO_OA_SPOT_EVENT", {"i": 1}, None, True),
                WireMessage("PROTO_OA_SPOT_EVENT", {"i": 2}, None, True),
                WireMessage("PROTO_OA_SPOT_EVENT", {"i": 3}, None, True),
                WireMessage("PROTO_OA_SYMBOLS_LIST_RES", {}, request.client_msg_id),
            ]

        transport = DeterministicTransport(handler, inbound_maxsize=16)
        client = CTraderClient(CTraderConfig(symbol_id=99, queue_maxsize=2), transport=transport)
        client.connect()
        client.request("PROTO_OA_SYMBOLS_LIST_REQ", {})
        self.assertEqual(client.status.queue_size, 2)
        self.assertEqual(client.status.dropped_messages, 1)
        self.assertTrue(client.status.needs_reconciliation)
        client.heartbeat()
        self.assertEqual(transport.sent[-1].payload_type_id, PAYLOAD["PROTO_HEARTBEAT_EVENT"])

    def test_provider_fetch_builds_official_historical_request(self) -> None:
        def handler(request: WireMessage):
            if request.payload_type_id == PAYLOAD["PROTO_OA_GET_TRENDBARS_REQ"]:
                return WireMessage("PROTO_OA_GET_TRENDBARS_RES", {"period": 1, "symbolId": 99, "trendbar": [synthetic_trendbar(timestamp_minutes=100), synthetic_trendbar(timestamp_minutes=101)], "hasMore": False}, request.client_msg_id)
            return WireMessage("PROTO_OA_SYMBOLS_LIST_RES", {"symbol": [{"symbolId": 99, "symbolName": "EUR/USD", "digits": 5, "pipPosition": 4}]}, request.client_msg_id)

        transport = DeterministicTransport(handler)
        config = CTraderConfig(symbol="EUR/USD", symbol_id=99, account_id=7)
        client = CTraderClient(config, transport=transport)
        client.connect(); client.mark_authenticated(7)
        provider = CTraderProvider(config, client=client)
        result = provider.fetch("M1", count=2, from_timestamp=BASE, to_timestamp=BASE + timedelta(minutes=2))
        self.assertEqual(len(result.bars), 2)
        self.assertEqual(result.request.payload["period"], 1)
        self.assertEqual(result.request.payload["symbolId"], 99)
        self.assertEqual(result.request.payload["count"], 2)
        self.assertIsNotNone(result.request.client_msg_id)

    def test_provider_catalog_and_subscription_order(self) -> None:
        def handler(request: WireMessage):
            if request.payload_type_id == PAYLOAD["PROTO_OA_SYMBOLS_LIST_REQ"]:
                return WireMessage("PROTO_OA_SYMBOLS_LIST_RES", {"symbol": [{"symbolId": 42, "symbolName": "EUR/USD", "digits": 5, "pipPosition": 4}]}, request.client_msg_id)
            response_name = {PAYLOAD["PROTO_OA_SUBSCRIBE_SPOTS_REQ"]: "PROTO_OA_SUBSCRIBE_SPOTS_RES", PAYLOAD["PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ"]: "PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES"}[request.payload_type_id]
            return WireMessage(response_name, {}, request.client_msg_id)

        transport = DeterministicTransport(handler)
        config = CTraderConfig(account_id=7)
        client = CTraderClient(config, transport=transport)
        client.connect(); client.mark_authenticated(7)
        provider = CTraderProvider(config, client=client)
        catalog = provider.resolve_symbol()
        self.assertEqual(catalog.selected.symbol_id, 42)
        provider.subscribe()
        request_names = [item.payload_type_name for item in transport.sent]
        self.assertEqual(request_names[0], "PROTO_OA_SYMBOLS_LIST_REQ")
        self.assertEqual(request_names[1], "PROTO_OA_SUBSCRIBE_SPOTS_REQ")
        self.assertEqual(request_names[2:5], ["PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ"] * 3)

    def test_oauth_discovery_can_complete_before_account_selection(self) -> None:
        def handler(request: WireMessage):
            if request.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage("PROTO_OA_APPLICATION_AUTH_RES", {}, request.client_msg_id)
            if request.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                return WireMessage(
                    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES",
                    {"ctidTraderAccount": [
                        {"ctidTraderAccountId": 7, "isLive": False},
                        {"ctidTraderAccountId": 8},
                    ], "permissionScope": "SCOPE_VIEW"},
                    request.client_msg_id,
                )
            raise AssertionError(request.payload_type_name)

        config = CTraderConfig(
            client_id="public",
            client_secret_ref="secret-ref",
            access_token_ref="token-ref",
            account_id=None,
            symbol_id=99,
        )
        transport = DeterministicTransport(handler)
        client = CTraderClient(config, transport=transport)
        client.connect()
        self.assertEqual(
            client.authenticate(
                secret_provider=lambda ref: "SECRET_VALUE",
                token_provider=lambda ref: "TOKEN_VALUE",
            ),
            AuthState.ACCOUNT_REQUIRED,
        )
        self.assertEqual(client.discovered_accounts[0]["environment"], "DEMO")
        self.assertEqual(client.discovered_accounts[1]["environment"], "UNKNOWN")
        self.assertEqual(
            [item.payload_type_id for item in transport.sent],
            [PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"], PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]],
        )
        client.close()

    def test_provider_auth_state_is_actionable_without_secret_capture(self) -> None:
        config = CTraderConfig(symbol_id=99)
        transport = DeterministicTransport()
        client = CTraderClient(config, transport=transport)
        provider = CTraderProvider(config, client=client)
        self.assertEqual(client.status.auth, AuthState.NOT_CONFIGURED)
        with self.assertRaises(CTraderAuthError) as caught:
            provider.fetch("M1")
        self.assertIn("Autentique", caught.exception.action)
        self.assertNotIn("secret", str(client.status.to_dict()).lower())

    def test_oauth_request_sequence_is_official_and_values_are_redacted_from_wire_view(self) -> None:
        def handler(request: WireMessage):
            responses = {
                PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]: WireMessage("PROTO_OA_APPLICATION_AUTH_RES", {}, request.client_msg_id),
                PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]: WireMessage("PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES", {"ctidTraderAccount": [{"ctidTraderAccountId": 7, "isLive": False}]}, request.client_msg_id),
                PAYLOAD["PROTO_OA_ACCOUNT_AUTH_REQ"]: WireMessage("PROTO_OA_ACCOUNT_AUTH_RES", {"ctidTraderAccountId": 7}, request.client_msg_id),
            }
            return responses[request.payload_type_id]

        config = CTraderConfig(client_id="public", client_secret_ref="secret-ref", access_token_ref="token-ref", account_id=7, symbol_id=99)
        transport = DeterministicTransport(handler)
        client = CTraderClient(config, transport=transport)
        client.connect()
        self.assertEqual(client.authenticate(secret_provider=lambda ref: "SECRET_VALUE", token_provider=lambda ref: "TOKEN_VALUE"), AuthState.AUTHENTICATED)
        self.assertEqual([item.payload_type_id for item in transport.sent], [2100, 2149, 2102])
        self.assertNotIn("SECRET_VALUE", str(transport.sent[0].to_dict()))
        self.assertNotIn("TOKEN_VALUE", str(transport.sent[1].to_dict()))


    def test_account_auth_rejects_server_observed_live_account(self) -> None:
        def handler(request: WireMessage):
            if request.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage("PROTO_OA_APPLICATION_AUTH_RES", {}, request.client_msg_id)
            if request.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                return WireMessage("PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES", {"ctidTraderAccount": [{"ctidTraderAccountId": 7, "isLive": True}]}, request.client_msg_id)
            raise AssertionError(request.payload_type_name)

        config = CTraderConfig(
            client_id="public", client_secret_ref="secret-ref",
            access_token_ref="token-ref", account_id=7,
        )
        transport = DeterministicTransport(handler)
        client = CTraderClient(config, transport=transport)
        client.connect()
        with self.assertRaises(CTraderAuthError):
            client.authenticate(
                secret_provider=lambda ref: "secret",
                token_provider=lambda ref: "token",
            )
        self.assertEqual(
            [item.payload_type_id for item in transport.sent],
            [PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"], PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]],
        )
        client.close()

    def test_application_oauth_discovers_accounts_without_account_id_or_auto_selection(self) -> None:
        def handler(request: WireMessage):
            if request.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage("PROTO_OA_APPLICATION_AUTH_RES", {}, request.client_msg_id)
            if request.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                return WireMessage(
                    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES",
                    {
                        "permissionScope": "SCOPE_VIEW",
                        "ctidTraderAccount": [
                            {"ctidTraderAccountId": 7, "isLive": False, "traderLogin": 1234},
                            {"ctidTraderAccountId": 8},
                        ],
                    },
                    request.client_msg_id,
                )
            raise AssertionError(f"request inesperado: {request.payload_type_name}")

        config = CTraderConfig(
            client_id="public",
            client_secret_ref="secret-ref",
            access_token_ref="token-ref",
            symbol_id=99,
        )
        transport = DeterministicTransport(handler)
        client = CTraderClient(config, transport=transport)
        client.connect()
        provider = CTraderProvider(config, client=client)

        state = provider.authenticate(
            secret_provider=lambda ref: "SECRET_VALUE",
            token_provider=lambda ref: "TOKEN_VALUE",
        )

        self.assertEqual(state, AuthState.ACCOUNT_REQUIRED)
        self.assertIsNone(config.account_id)
        self.assertEqual(client.status.auth, AuthState.ACCOUNT_REQUIRED)
        discovery = provider.discover_accounts()
        self.assertEqual(discovery["permissionScope"], "SCOPE_VIEW")
        self.assertEqual(discovery["records"][0]["account_id"], 7)
        self.assertEqual(discovery["records"][0]["environment"], "DEMO")
        self.assertEqual(discovery["records"][0]["trader_login"], 1234)
        self.assertEqual(discovery["records"][1], {"account_id": 8, "environment": "UNKNOWN", "permission_scope": "SCOPE_VIEW"})
        self.assertEqual(
            [item.payload_type_name for item in transport.sent],
            [
                "PROTO_OA_APPLICATION_AUTH_REQ",
                "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ",
            ],
        )
        self.assertNotIn("TOKEN_VALUE", str(discovery))

    def test_discovery_snapshot_is_detached_and_preserves_missing_permission_scope(self) -> None:
        def handler(request: WireMessage):
            if request.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage("PROTO_OA_APPLICATION_AUTH_RES", {}, request.client_msg_id)
            return WireMessage(
                "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES",
                {"ctidTraderAccount": [{"ctidTraderAccountId": 9}]},
                request.client_msg_id,
            )

        config = CTraderConfig(client_id="public", client_secret_ref="secret-ref", access_token_ref="token-ref")
        transport = DeterministicTransport(handler)
        client = CTraderClient(config, transport=transport)
        client.connect()
        client.authenticate(secret_provider=lambda ref: "SECRET", token_provider=lambda ref: "TOKEN")
        first = client.discover_accounts()
        first["records"][0]["account_id"] = 999
        second = client.discover_accounts()
        self.assertIsNone(second["permissionScope"])
        self.assertEqual(second["records"][0]["account_id"], 9)


    def test_real_generated_account_payload_does_not_expose_token_and_requires_full_symbol_entity(self) -> None:
        report = dependency_report()
        if not report.codec_operational:
            self.skipTest("SDK cTrader opcional no instalado")
        from ctrader_open_api.messages import OpenApiMessages_pb2 as pb

        account_payload = pb.ProtoOAGetAccountListByAccessTokenRes()
        account_payload.accessToken = "secret-access-token"
        account_payload.permissionScope = 1
        account = account_payload.ctidTraderAccount.add()
        account.ctidTraderAccountId = 7
        account.isLive = False

        def handler(request: WireMessage):
            if request.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                return WireMessage("PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES", account_payload, request.client_msg_id)
            if request.payload_type_id == PAYLOAD["PROTO_OA_SYMBOLS_LIST_REQ"]:
                response = pb.ProtoOASymbolsListRes()
                light = response.symbol.add()
                light.symbolId = 99
                light.symbolName = "EUR/USD"
                light.enabled = True
                return WireMessage("PROTO_OA_SYMBOLS_LIST_RES", response, request.client_msg_id)
            if request.payload_type_id == PAYLOAD["PROTO_OA_SYMBOL_BY_ID_REQ"]:
                response = pb.ProtoOASymbolByIdRes()
                full = response.symbol.add()
                full.symbolId = 99
                full.digits = 5
                full.pipPosition = 4
                full.minVolume = 100
                full.maxVolume = 100000
                full.stepVolume = 100
                full.lotSize = 100000
                return WireMessage("PROTO_OA_SYMBOL_BY_ID_RES", response, request.client_msg_id)
            raise AssertionError(request.payload_type_name)

        transport = DeterministicTransport(handler)
        config = CTraderConfig(account_id=7)
        client = CTraderClient(config, transport=transport)
        client.connect()
        client.mark_authenticated(7)
        provider = CTraderProvider(config, client=client)
        normalized = provider.client._discover_accounts_for_token("secret-access-token")
        self.assertEqual(normalized["records"], [{"account_id": 7, "environment": "DEMO", "permission_scope": "SCOPE_TRADE"}])
        self.assertNotIn("secret-access-token", repr(normalized))
        catalog = provider.resolve_symbol()
        self.assertEqual(catalog.selected.metadata["minVolume"], 100)
        self.assertEqual([message.payload_type_id for message in transport.sent], [
            PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"],
            PAYLOAD["PROTO_OA_SYMBOLS_LIST_REQ"],
            PAYLOAD["PROTO_OA_SYMBOL_BY_ID_REQ"],
        ])
        client.close()


class CTraderHardeningTests(unittest.TestCase):
    def test_trendbar_protocol_volume_is_not_economic_volume(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99)
        bar = normalize_trendbar(synthetic_trendbar(timestamp_minutes=100, volume=42), spec=spec, received_at=BASE)
        self.assertIsNone(bar.volume)
        self.assertEqual(bar.metadata["protocol_volume"], 42)

    def test_missing_source_timestamp_is_visible_as_issue(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99)
        result = normalize_spot_event({"symbolId": 99, "bid": 110000, "ask": 110020}, spec=spec, received_at=BASE)
        self.assertTrue(any("timestamp" in issue for issue in result.issues))

class CTraderCalendarTests(unittest.TestCase):
    def test_calendar_does_not_infer_open_or_quote_without_explicit_window(self) -> None:
        from mtf_lab.data.ctrader import CTraderMarketCalendar, CTraderSessionWindow
        calendar = CTraderMarketCalendar([CTraderSessionWindow(weekday=3, open_minute=0, close_minute=1440)], holidays=["2026-01-01"])
        self.assertEqual(calendar.state(BASE), "CLOSED_SCHEDULED")
        self.assertEqual(calendar.state(BASE + timedelta(days=7)), "OPEN_NO_QUOTE")
        self.assertEqual(CTraderMarketCalendar().state(BASE + timedelta(days=7)), "UNKNOWN")


class _FragmentSocket:
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.timeouts = []
    def settimeout(self, value):
        self.timeouts.append(value)
    def recv(self, count):
        if not self.chunks:
            raise socket.timeout()
        chunk = self.chunks.pop(0)
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk
    def sendall(self, data):
        pass
    def close(self):
        pass


class _BytesCodec:
    def encode(self, message):
        return str(message.payload_type).encode()
    def decode(self, payload):
        return WireMessage("PROTO_OA_SPOT_EVENT", {"raw": payload.decode()})


class CTraderTransportHardeningTests(unittest.TestCase):
    def test_tcp_framing_preserves_partial_header_body_and_multiple_frames(self) -> None:
        first = b"one"
        second = b"two-two"
        raw_first = struct.pack("!i", len(first)) + first
        raw_second = struct.pack("!i", len(second)) + second
        fake = _FragmentSocket([raw_first[:2]])
        transport = TcpTlsTransport("fixture", codec=_BytesCodec())
        transport._socket = fake
        self.assertIsNone(transport.receive(timeout=0.001))
        fake.chunks.append(raw_first[2:] + raw_second)
        one = transport.receive(timeout=0.1)
        two = transport.receive(timeout=0.1)
        self.assertEqual(one.payload["raw"], "one")
        self.assertEqual(two.payload["raw"], "two-two")

    def test_official_codec_envelope_and_heartbeat_when_extra_is_available(self) -> None:
        report = dependency_report()
        if not report.codec_operational:
            self.skipTest(report.message or "SDK/Protobuf oficial no disponible")
        codec = SdkProtobufCodec()
        heartbeat = codec.decode(codec.encode(WireMessage("PROTO_HEARTBEAT_EVENT")))
        self.assertEqual(heartbeat.payload_type_id, PAYLOAD["PROTO_HEARTBEAT_EVENT"])
        request = WireMessage("PROTO_OA_SYMBOLS_LIST_REQ", {"ctidTraderAccountId": 7, "includeArchivedSymbols": False}, "req-1")
        decoded = codec.decode(codec.encode(request))
        self.assertEqual(decoded.payload_type_id, PAYLOAD["PROTO_OA_SYMBOLS_LIST_REQ"])
        self.assertEqual(decoded.client_msg_id, "req-1")

    def test_rate_limits_are_separate_for_historical_and_other_requests(self) -> None:
        now = [0.0]
        sleeps = []
        def clock(): return now[0]
        def sleep(value): sleeps.append(value); now[0] += value
        limiter = CTraderRateLimiter(request_rate=2, historical_rate=1, clock=clock, sleep=sleep)
        limiter.acquire(historical=False); limiter.acquire(historical=False); limiter.acquire(historical=False)
        limiter.acquire(historical=True); limiter.acquire(historical=True)
        self.assertGreaterEqual(sleeps[0], 0.49)
        self.assertGreaterEqual(sleeps[-1], 0.99)



class CTraderReconnectTests(unittest.TestCase):
    def test_reconnect_invalidates_auth_and_clears_subscriptions_until_explicit_reauth(self) -> None:
        transport = DeterministicTransport()
        config = CTraderConfig(symbol_id=99, account_id=7)
        client = CTraderClient(config, transport=transport)
        client.connect(); client.mark_authenticated(7)
        provider = CTraderProvider(config, client=client)
        provider._subscribed.update({"M1", "M5"})
        status = provider.reconnect()
        self.assertEqual(status.connection, ConnectionState.CONNECTED)
        self.assertEqual(status.auth, AuthState.REQUIRED)
        self.assertEqual(provider._subscribed, set())
        self.assertIn("Reautentique", status.action)

    def test_periodic_heartbeat_is_driven_by_poll_without_reactor(self) -> None:
        now = [0.0]
        transport = DeterministicTransport()
        client = CTraderClient(CTraderConfig(symbol_id=99, heartbeat_seconds=10), transport=transport, clock=lambda: now[0])
        client.connect()
        now[0] = 11.0
        self.assertIsNone(client.poll_event())
        self.assertEqual(transport.sent[-1].payload_type_id, PAYLOAD["PROTO_HEARTBEAT_EVENT"])

class PumpTransportTests(unittest.TestCase):
    def test_tcp_transport_receive_pump_correlates_responses_and_events(self) -> None:
        import queue
        from mtf_lab.data.ctrader import TcpTlsTransport
        class FakeTcp:
            def __init__(self):
                self.inbound = queue.Queue(); self._connected = False; self.codec = None; self.sent = []
            @property
            def connected(self): return self._connected
            def connect(self, timeout=None): self._connected = True
            def close(self): self._connected = False
            def send(self, message):
                self.sent.append(message)
                self.inbound.put(WireMessage("PROTO_OA_SPOT_EVENT", {"x": 1}, None, True))
                self.inbound.put(WireMessage("PROTO_OA_SYMBOLS_LIST_RES", {}, message.client_msg_id))
            def receive(self, timeout=None):
                try: return self.inbound.get(timeout=timeout or 0.05)
                except queue.Empty: return None
        if dependency_report().sdk_state.value != "AVAILABLE":
            self.skipTest("SDK cTrader opcional no instalado en este intérprete")
        transport = FakeTcp(); client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport); client.connect()
        response = client.request("PROTO_OA_SYMBOLS_LIST_REQ", {})
        self.assertEqual(response.payload_type_name, "PROTO_OA_SYMBOLS_LIST_RES")
        self.assertIsNotNone(client.poll_event(0.2)); client.close()

if __name__ == "__main__":
    unittest.main()
