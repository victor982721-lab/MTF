"""Fixtures y pruebas offline del proveedor cTrader Open API."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sys
import unittest

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
    ConnectionState,
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
        report = dependency_report()
        self.assertIn(report.sdk_state.value, {"AVAILABLE", "MISSING"})
        self.assertIn(report.protobuf_state.value, {"AVAILABLE", "MISSING"})
        self.assertNotIn("twisted.internet.reactor", sys.modules)

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
                PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]: WireMessage("PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES", {"ctidTraderAccount": [{"ctidTraderAccountId": 7}]}, request.client_msg_id),
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


if __name__ == "__main__":
    unittest.main()


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
