"""Contratos offline de lector único, fases, heartbeat y generaciones cTrader."""

from __future__ import annotations

import queue
import threading
import time
import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.data.ctrader import (
    PAYLOAD,
    CTraderAuthError,
    CTraderClient,
    CTraderConfig,
    CTraderConfigurationError,
    CTraderProtocolError,
    CTraderRequestCancelled,
    CTraderRequestTimeout,
    CTraderTransportError,
    RequestPhase,
    WireMessage,
)


class ControlledTransport:
    def __init__(self, handler=None):
        self.handler = handler
        self.connected = False
        self.close_calls = 0
        self.inbound: queue.Queue[WireMessage] = queue.Queue()
        self.sent: list[WireMessage] = []
        self.receive_threads: set[int] = set()
        self.send_active = 0
        self.max_send_active = 0
        self.send_lock = threading.Lock()
        self.send_started = threading.Event()
        self.fail_receive = threading.Event()

    def connect(self, timeout=None):
        self.fail_receive.clear()
        self.connected = True

    def close(self):
        self.close_calls += 1
        self.connected = False
        self.fail_receive.set()

    def send(self, message):
        if not self.connected:
            raise CTraderTransportError("transport closed")
        with self.send_lock:
            self.send_active += 1
            self.max_send_active = max(self.max_send_active, self.send_active)
        self.send_started.set()
        try:
            # Make concurrent callers overlap unless the session serializes
            # the calls correctly.
            time.sleep(0.01)
            self.sent.append(message)
            if self.handler is not None:
                response = self.handler(message)
                if response is not None:
                    if isinstance(response, WireMessage):
                        response = (response,)
                    for item in response:
                        self.inbound.put(item)
        finally:
            with self.send_lock:
                self.send_active -= 1

    def receive(self, timeout=None):
        self.receive_threads.add(threading.get_ident())
        if self.fail_receive.is_set():
            raise CTraderTransportError("EOF controlled")
        try:
            return self.inbound.get(timeout=timeout or 0.01)
        except queue.Empty as exc:
            if self.fail_receive.is_set():
                raise CTraderTransportError("EOF controlled") from exc
            return None


class ContinuousTransport(ControlledTransport):
    def __init__(self, clock_value):
        super().__init__()
        self.clock_value = clock_value

    def receive(self, timeout=None):
        self.receive_threads.add(threading.get_ident())
        self.clock_value[0] += 1.0
        return WireMessage(
            PAYLOAD["PROTO_OA_SPOT_EVENT"],
            {"symbolId": 99, "timestamp": 1767225600000, "bid": 110000, "ask": 110020},
            None,
            True,
        )


class ProtoOASymbolsListReq:
    payloadType = PAYLOAD["PROTO_OA_SYMBOLS_LIST_REQ"]


class CTraderSessionContractTests(unittest.TestCase):
    def test_request_and_heartbeat_share_one_serialized_send_gate(self):
        def handler(message):
            if message.client_msg_id:
                return WireMessage("PROTO_OA_SYMBOLS_LIST_RES", {}, message.client_msg_id)
            return None

        transport = ControlledTransport(handler)
        client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport)
        client.connect()
        failures: list[BaseException] = []

        def request():
            try:
                client.request("PROTO_OA_SYMBOLS_LIST_REQ", {}, timeout_seconds=1)
            except BaseException as exc:  # pragma: no cover - assertion below
                failures.append(exc)

        threads = [threading.Thread(target=request) for _ in range(3)]
        for thread in threads:
            thread.start()
        heartbeat = threading.Thread(target=client.heartbeat)
        heartbeat.start()
        for thread in threads:
            thread.join(timeout=2)
        heartbeat.join(timeout=2)
        try:
            self.assertFalse(failures)
            self.assertEqual(transport.max_send_active, 1)
            self.assertEqual(len(transport.receive_threads), 1)
            self.assertEqual(client.status.pending_requests, 0)
        finally:
            client.close()

    def test_eof_wakes_pending_request_with_transport_cause(self):
        transport = ControlledTransport()
        client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport)
        client.connect()
        outcome: list[BaseException] = []

        def run_request():
            try:
                client.request("PROTO_OA_SYMBOLS_LIST_REQ", {}, timeout_seconds=10)
            except BaseException as exc:
                outcome.append(exc)

        thread = threading.Thread(target=run_request)
        thread.start()
        self.assertTrue(transport.send_started.wait(1))
        transport.fail_receive.set()
        thread.join(timeout=2)
        try:
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(outcome), 1)
            self.assertIsInstance(outcome[0], CTraderTransportError)
            self.assertEqual(outcome[0].phase, RequestPhase.FAILED_AFTER_SEND)
            self.assertTrue(client.status.needs_reconciliation)
            self.assertEqual(client.status.discontinuity_reason, "CTraderTransportError")
        finally:
            client.close()

    def test_cancel_before_send_is_distinct_from_sent_timeout(self):
        transport = ControlledTransport()
        client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport)
        client.connect()
        token = __import__("mtf_lab.data.ctrader", fromlist=["CancellationToken"]).CancellationToken()
        token.cancel()
        with self.assertRaises(CTraderRequestCancelled) as cancelled:
            client.request("PROTO_OA_SYMBOLS_LIST_REQ", {}, cancel=token)
        self.assertEqual(transport.sent, [])
        self.assertEqual(cancelled.exception.phase, RequestPhase.CANCELLED_BEFORE_SEND)
        self.assertEqual(client.status.last_request_phase, RequestPhase.CANCELLED_BEFORE_SEND)

        with self.assertRaises(CTraderRequestTimeout) as timed_out:
            client.request("PROTO_OA_SYMBOLS_LIST_REQ", {}, timeout_seconds=0.01)
        self.assertEqual(timed_out.exception.phase, RequestPhase.TIMED_OUT)
        self.assertEqual(client.status.last_request_phase, RequestPhase.TIMED_OUT)
        self.assertEqual(len(transport.sent), 1)
        client.close()

    def test_request_message_uses_same_reader_and_correlator(self):
        def handler(message):
            return WireMessage("PROTO_OA_SYMBOLS_LIST_RES", {"symbol": []}, message.client_msg_id)

        transport = ControlledTransport(handler)
        client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport)
        client.connect()
        response = client.request_message(ProtoOASymbolsListReq(), client_msg_id="typed-1", timeout_seconds=1)
        try:
            self.assertEqual(response.payload_type_id, PAYLOAD["PROTO_OA_SYMBOLS_LIST_RES"])
            self.assertEqual(transport.sent[0].client_msg_id, "typed-1")
            self.assertEqual(len(transport.receive_threads), 1)
        finally:
            client.close()

    def test_heartbeat_progresses_while_market_messages_are_continuous(self):
        now = [0.0]
        transport = ContinuousTransport(now)
        client = CTraderClient(
            CTraderConfig(symbol_id=99, heartbeat_seconds=3),
            transport=transport,
            clock=lambda: now[0],
        )
        client.connect()
        time.sleep(0.03)
        client.close()
        self.assertGreaterEqual(
            sum(message.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"] for message in transport.sent),
            1,
        )

    def test_reconnect_uses_new_generation_and_old_reader_is_stopped(self):
        transport = ControlledTransport()
        client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport)
        first = client.connect()
        old_generation = first.generation
        transport.inbound.put(WireMessage("PROTO_OA_SPOT_EVENT", {"x": 1}, None, True))
        self.assertIsNotNone(client.poll_event(timeout_seconds=1))
        second = client.connect()
        try:
            self.assertGreater(second.generation, old_generation)
            self.assertEqual(client.generation, second.generation)
            self.assertGreaterEqual(transport.close_calls, 1)
            self.assertEqual(client.status.generation, second.generation)
        finally:
            client.close()

    def test_control_queue_is_not_evicted_by_market_backpressure(self):
        transport = ControlledTransport()
        client = CTraderClient(CTraderConfig(symbol_id=99, queue_maxsize=1), transport=transport)
        client.connect()
        # These are unsolicited control messages; they must remain observable
        # while quote traffic is allowed to be bounded/lossy.
        transport.inbound.put(WireMessage("PROTO_OA_ERROR_RES", {"description": "control"}, None, True))
        for index in range(5):
            transport.inbound.put(WireMessage("PROTO_OA_SPOT_EVENT", {"i": index}, None, True))
        time.sleep(0.03)
        try:
            first = client.poll_event(timeout_seconds=1)
            self.assertEqual(first.payload_type_id, PAYLOAD["PROTO_OA_ERROR_RES"])
            self.assertGreaterEqual(client.status.dropped_messages, 1)
        finally:
            client.close()


class CTraderSessionEvidenceTests(unittest.TestCase):
    def test_evidence_is_minted_only_from_server_inventory_and_account_auth(self):
        def handler(message):
            if message.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage("PROTO_OA_APPLICATION_AUTH_RES", {}, message.client_msg_id)
            if message.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                return WireMessage(
                    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES",
                    {
                        "permissionScope": "SCOPE_TRADE",
                        "ctidTraderAccount": [{"ctidTraderAccountId": 7, "isLive": False}],
                    },
                    message.client_msg_id,
                )
            if message.payload_type_id == PAYLOAD["PROTO_OA_ACCOUNT_AUTH_REQ"]:
                return WireMessage(
                    "PROTO_OA_ACCOUNT_AUTH_RES",
                    {"ctidTraderAccountId": 7},
                    message.client_msg_id,
                )
            raise AssertionError(message.payload_type_name)

        transport = ControlledTransport(handler)
        config = CTraderConfig(
            client_id="public",
            client_secret_ref="secret-ref",
            access_token_ref="token-ref",
            account_id=7,
            symbol_id=99,
        )
        client = CTraderClient(config, transport=transport)
        client.connect()
        self.assertIsNone(getattr(client, "_session_evidence", None))
        client.authenticate(
            secret_provider=lambda _: "SECRET",
            token_provider=lambda _: "TOKEN",
        )
        proof = client.authenticated_session_evidence()
        try:
            self.assertEqual(proof.account_id, 7)
            self.assertEqual(proof.environment, "DEMO")
            self.assertEqual(proof.endpoint, "demo.ctraderapi.com:5035")
            self.assertIn("trading", proof.scopes)
            self.assertEqual(proof.connection_generation, str(client.generation))
            self.assertTrue(client.validate_session_evidence(proof))
            import dataclasses

            with self.assertRaises(CTraderAuthError):
                client.validate_session_evidence(dataclasses.replace(proof))
            client.connect()
            with self.assertRaises(CTraderAuthError):
                client.authenticated_session_evidence()
        finally:
            client.close()


class CTraderWireCaptureTests(unittest.TestCase):
    def test_reader_metadata_survives_late_consumer_poll(self):
        base = datetime(2026, 1, 1, tzinfo=UTC)
        transport = ControlledTransport()
        client = CTraderClient(CTraderConfig(symbol_id=99), transport=transport)
        client.connect()
        transport.inbound.put(
            WireMessage(
                "PROTO_OA_SPOT_EVENT",
                {
                    "symbolId": 99,
                    "timestamp": int(base.timestamp() * 1000),
                    "bid": 110000,
                    "ask": 110020,
                },
                None,
                True,
                received_at=base,
                available_at=base + timedelta(seconds=1),
            )
        )
        time.sleep(0.03)
        try:
            message = client.poll_event(timeout_seconds=1)
            self.assertIsNotNone(message)
            assert message is not None
            self.assertEqual(message.received_at, base)
            self.assertEqual(message.available_at, base + timedelta(seconds=1))
            self.assertEqual(message.ingest_sequence, 0)
            self.assertEqual(message.connection_generation, client.generation)
            envelope = message.capture_envelope()
            self.assertEqual(envelope["received_at"], base)
            self.assertEqual(envelope["available_at"], message.available_at)
            self.assertEqual(envelope["ingest_sequence"], 0)
        finally:
            client.close()


class CTraderCaptureAllowlistTests(unittest.TestCase):
    def test_auth_payload_is_not_exported_as_market_capture(self):
        base = datetime(2026, 1, 1, tzinfo=UTC)
        message = WireMessage(
            "PROTO_OA_ACCOUNT_AUTH_RES",
            {"accessToken": "SECRET_TOKEN", "ctidTraderAccountId": 7},
            received_at=base,
            available_at=base,
        )
        with self.assertRaises(CTraderProtocolError) as caught:
            message.capture_envelope()
        self.assertEqual(caught.exception.error_code, "CAPTURE_UNSUPPORTED")
        self.assertNotIn("SECRET_TOKEN", str(caught.exception))

    def test_sdk_heartbeat_is_always_a_clock_envelope(self):
        base = datetime(2026, 1, 1, tzinfo=UTC)
        message = WireMessage(
            PAYLOAD["PROTO_HEARTBEAT_EVENT"],
            {},
            received_at=base,
            available_at=base,
        )
        envelope = message.capture_envelope()
        self.assertEqual(envelope["message_class"], "clock")
        self.assertEqual(envelope["payload"], {"clock_kind": "heartbeat"})

        from mtf_lab.data.ctrader import SdkProtobufCodec, dependency_report

        if dependency_report().codec_operational:
            codec = SdkProtobufCodec()
            decoded = codec.decode(codec.encode(WireMessage(PAYLOAD["PROTO_HEARTBEAT_EVENT"])))
            stamped = decoded.with_capture_metadata(
                received_at=base,
                available_at=base,
                ingest_sequence=1,
                connection_generation=2,
            )
            self.assertEqual(stamped.capture_envelope()["payload"], {"clock_kind": "heartbeat"})

    def test_account_disconnect_is_sanitized_to_connection_state(self):
        base = datetime(2026, 1, 1, tzinfo=UTC)
        message = WireMessage(
            "ACCOUNT_DISCONNECT",
            {"state": "DISCONNECTED", "reason_code": "EOF", "accessToken": "SECRET_TOKEN"},
            received_at=base,
            available_at=base,
        )
        envelope = message.capture_envelope()
        self.assertEqual(envelope["message_class"], "connection")
        self.assertEqual(
            envelope["payload"],
            {"state": "DISCONNECTED", "reason_code": "EOF"},
        )
        self.assertNotIn("SECRET_TOKEN", str(envelope))


class CTraderSessionRevocationTests(unittest.TestCase):
    def _authenticated_client(self, *, account_auth_id=7):
        def handler(message):
            if message.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                return WireMessage("PROTO_OA_APPLICATION_AUTH_RES", {}, message.client_msg_id)
            if message.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                return WireMessage(
                    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES",
                    {
                        "permissionScope": "SCOPE_TRADE",
                        "ctidTraderAccount": [{"ctidTraderAccountId": 7, "isLive": False}],
                    },
                    message.client_msg_id,
                )
            if message.payload_type_id == PAYLOAD["PROTO_OA_ACCOUNT_AUTH_REQ"]:
                return WireMessage(
                    "PROTO_OA_ACCOUNT_AUTH_RES",
                    {"ctidTraderAccountId": account_auth_id},
                    message.client_msg_id,
                )
            return None

        transport = ControlledTransport(handler)
        client = CTraderClient(
            CTraderConfig(
                client_id="public",
                client_secret_ref="secret-ref",
                access_token_ref="token-ref",
                account_id=7,
                symbol_id=99,
            ),
            transport=transport,
        )
        client.connect()
        client.authenticate(secret_provider=lambda _: "SECRET", token_provider=lambda _: "TOKEN")
        return client, transport

    def test_matching_account_disconnect_invalidates_proof_and_gateway_sends_nothing(self):
        from ctrader_open_api.messages import OpenApiMessages_pb2 as proto

        from mtf_lab.ops.ctrader_demo_transport import CTraderClientGateway, OfficialGatewayConfigurationError

        client, transport = self._authenticated_client()
        gateway = CTraderClientGateway(client)
        before = len(transport.sent)
        try:
            transport.inbound.put(
                WireMessage(
                    "PROTO_OA_ACCOUNT_DISCONNECT_EVENT",
                    {"ctidTraderAccountId": 7},
                    None,
                    True,
                )
            )
            time.sleep(0.03)
            with self.assertRaises(CTraderAuthError):
                client.authenticated_session_evidence()
            with self.assertRaises(OfficialGatewayConfigurationError):
                gateway.send(proto.ProtoOANewOrderReq(), client_msg_id="revoked", timeout_seconds=1)
            self.assertEqual(len(transport.sent), before)
            control = client.poll_event(timeout_seconds=1)
            self.assertIsNotNone(control)
            self.assertEqual(control.payload_type_id, 2164)
        finally:
            client.close()

    def test_token_invalidation_for_other_account_does_not_revoke_current_proof(self):
        client, transport = self._authenticated_client()
        proof = client.authenticated_session_evidence()
        try:
            transport.inbound.put(
                WireMessage(
                    "PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT",
                    {"ctidTraderAccountIds": [999], "reason": "TOKEN_REVOKED"},
                    None,
                    True,
                )
            )
            time.sleep(0.03)
            self.assertIs(client.authenticated_session_evidence(), proof)
            control = client.poll_event(timeout_seconds=1)
            self.assertIsNotNone(control)
            self.assertEqual(control.payload_type_id, 2147)
        finally:
            client.close()

    def test_matching_token_invalidation_invalidates_proof_and_gateway_sends_nothing(self):
        from ctrader_open_api.messages import OpenApiMessages_pb2 as proto

        from mtf_lab.ops.ctrader_demo_transport import CTraderClientGateway, OfficialGatewayConfigurationError

        client, transport = self._authenticated_client()
        gateway = CTraderClientGateway(client)
        before = len(transport.sent)
        try:
            transport.inbound.put(
                WireMessage(
                    "PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT",
                    {"ctidTraderAccountIds": [7], "reason": "TOKEN_REVOKED"},
                    None,
                    True,
                )
            )
            time.sleep(0.03)
            with self.assertRaises(OfficialGatewayConfigurationError):
                gateway.send(proto.ProtoOANewOrderReq(), client_msg_id="revoked-token", timeout_seconds=1)
            self.assertEqual(len(transport.sent), before)
        finally:
            client.close()

    def test_old_generation_disconnect_cannot_revoke_new_generation_proof(self):
        client, transport = self._authenticated_client()
        old_generation = client.generation
        client.connect()
        client.authenticate(secret_provider=lambda _: "SECRET", token_provider=lambda _: "TOKEN")
        new_proof = client.authenticated_session_evidence()
        try:
            transport.inbound.put(
                WireMessage(
                    "PROTO_OA_ACCOUNT_DISCONNECT_EVENT",
                    {"ctidTraderAccountId": 7},
                    None,
                    True,
                    connection_generation=old_generation,
                )
            )
            time.sleep(0.03)
            self.assertIs(client.authenticated_session_evidence(), new_proof)
        finally:
            client.close()

    def test_account_auth_without_or_with_wrong_account_id_fails_closed(self):
        for response_id in (None, 999):

            def handler(message, response_id=response_id):
                if message.payload_type_id == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                    return WireMessage("PROTO_OA_APPLICATION_AUTH_RES", {}, message.client_msg_id)
                if message.payload_type_id == PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]:
                    return WireMessage(
                        "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES",
                        {
                            "permissionScope": "SCOPE_TRADE",
                            "ctidTraderAccount": [{"ctidTraderAccountId": 7, "isLive": False}],
                        },
                        message.client_msg_id,
                    )
                payload = {} if response_id is None else {"ctidTraderAccountId": response_id}
                return WireMessage("PROTO_OA_ACCOUNT_AUTH_RES", payload, message.client_msg_id)

            transport = ControlledTransport(handler)
            client = CTraderClient(
                CTraderConfig(
                    client_id="public",
                    client_secret_ref="secret-ref",
                    access_token_ref="token-ref",
                    account_id=7,
                    symbol_id=99,
                ),
                transport=transport,
            )
            client.connect()
            try:
                with self.assertRaises(CTraderAuthError):
                    client.authenticate(
                        secret_provider=lambda _: "SECRET",
                        token_provider=lambda _: "TOKEN",
                    )
                self.assertEqual(client.status.auth.value, "INVALID")
            finally:
                client.close()

    def test_resume_counters_seed_first_reader_and_survive_reconnect(self):
        client = CTraderClient(
            CTraderConfig(symbol_id=99),
            transport=ControlledTransport(),
            next_ingest_sequence=100,
            initial_generation=7,
        )
        client.connect()
        try:
            self.assertEqual(client.generation, 8)
            client.transport.inbound.put(
                WireMessage(
                    "PROTO_OA_SPOT_EVENT",
                    {"symbolId": 99, "timestamp": 1767225600000, "bid": 110000, "ask": 110020},
                    None,
                    True,
                )
            )
            time.sleep(0.03)
            first = client.poll_event(timeout_seconds=1)
            self.assertIsNotNone(first)
            assert first is not None
            self.assertEqual(first.ingest_sequence, 100)
            self.assertEqual(first.connection_generation, 8)

            client.connect()
            self.assertEqual(client.generation, 9)
            client.transport.inbound.put(
                WireMessage(
                    "PROTO_OA_SPOT_EVENT",
                    {"symbolId": 99, "timestamp": 1767225660000, "bid": 110001, "ask": 110021},
                    None,
                    True,
                )
            )
            time.sleep(0.03)
            second = client.poll_event(timeout_seconds=1)
            self.assertIsNotNone(second)
            assert second is not None
            self.assertEqual(second.ingest_sequence, 101)
            self.assertEqual(second.connection_generation, 9)
        finally:
            client.close()

    def test_resume_counters_reject_bool_or_negative_values(self):
        for kwargs in (
            {"next_ingest_sequence": -1},
            {"initial_generation": -1},
            {"next_ingest_sequence": True},
            {"initial_generation": False},
        ):
            with self.assertRaises(CTraderConfigurationError):
                CTraderClient(CTraderConfig(symbol_id=99), transport=ControlledTransport(), **kwargs)


if __name__ == "__main__":
    unittest.main()
