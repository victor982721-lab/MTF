"""Offline contract regressions for the typed cTrader DEMO adapter."""

from __future__ import annotations

import queue
import unittest
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal, getcontext

from mtf_lab.data.ctrader import (
    CTraderClient,
    CTraderConfig,
    CTraderRequestCancelled,
    RequestPhase,
    SdkProtobufCodec,
    WireMessage,
)
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.data.protobuf_generated import OpenApiModelMessages_pb2 as model
from mtf_lab.ops.ctrader_demo_transport import (
    PAYLOAD_TYPES,
    AuthenticatedSession,
    CTraderClientGateway,
    CTraderDemoTransport,
    OfficialCancelledError,
    OfficialCorrelationError,
    OfficialGatewayConfigurationError,
    OfficialMessageError,
    OfficialTransportError,
    OfficialUncertainSendError,
    SendPhase,
    ServerAccountObservation,
    _field,
    _many,
    _volume_from_protocol,
    _volume_to_protocol,
)
from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DemoAccount,
    DemoAccountRequired,
    ExecutionIntent,
    ExecutionPolicy,
    OrderState,
    Quote,
    RiskLimitRejected,
    Side,
)

NOW = datetime(2025, 1, 1, 12, tzinfo=UTC)


class Gateway:
    def __init__(self, responses=()):
        self.responses = responses if callable(responses) else deque(responses)
        self.calls: list[tuple[object, str, float]] = []

    def send(self, message, *, client_msg_id, timeout_seconds):
        self.calls.append((message, client_msg_id, timeout_seconds))
        if callable(self.responses):
            return self.responses(message, client_msg_id, timeout_seconds)
        return self.responses.popleft() if self.responses else None


def account() -> DemoAccount:
    return DemoAccount("123", "DEMO", "demo.ctraderapi.com:5035", {"trading"}, True, True)


def observation() -> ServerAccountObservation:
    return ServerAccountObservation(
        "123",
        "DEMO",
        "demo.ctraderapi.com:5035",
        {"trading"},
        NOW,
        source="fixture-server",
        session_id="fixture-session",
        connection_generation="generation-1",
    )


def intent(quantity="1") -> ExecutionIntent:
    return ExecutionIntent("intent-1", "signal-1", "EUR/USD", Side.BUY, quantity, "1.1010", NOW, "123")


def external_policy(**overrides):
    values = {
        "fixed_quantity": 1,
        "max_quantity": 1,
        "max_positions": 2,
        "max_exposure": 1000,
        "max_spread": 1,
        "max_price_age_seconds": 5,
        "max_daily_loss": 100,
        "max_drawdown": 100,
        "min_margin_level": 1,
        "max_inflight_intents": 1,
        "max_holding_seconds": 3600,
        "require_protective_stops": True,
        "relative_stop_loss": 10,
        "relative_take_profit": 20,
    }
    values.update(overrides)
    return ExecutionPolicy(**values)


VOLUME_GRID = {"min_volume": 100, "max_volume": 100_000, "step_volume": 100}


def transport(gateway, *, use_model_proto=True) -> CTraderDemoTransport:
    return CTraderDemoTransport(
        account(),
        client=gateway,
        proto=proto,
        symbol_ids={"EUR/USD": 11},
        symbol_names={11: "EUR/USD"},
        clock=lambda: NOW,
        server_observation=observation(),
        volume_grid=VOLUME_GRID,
    )


def filled_event(
    client_id="intent-1",
    *,
    execution_type=3,
    volume=100,
    deal_id=8,
    stop_loss=None,
    take_profit=None,
    opened_at=None,
):
    event = proto.ProtoOAExecutionEvent(ctidTraderAccountId=123, executionType=execution_type)
    event.order.orderId = 7
    event.order.clientOrderId = client_id
    event.order.orderStatus = 2 if execution_type == 3 else 1
    event.order.executedVolume = volume
    event.order.executionPrice = 1.101
    if stop_loss is not None:
        event.order.stopLoss = stop_loss
    if take_profit is not None:
        event.order.takeProfit = take_profit
    event.deal.dealId = deal_id
    event.deal.orderId = 7
    event.deal.positionId = 9
    event.deal.volume = volume
    event.deal.filledVolume = volume
    event.deal.executionTimestamp = 1735732800000
    event.deal.executionPrice = 1.101
    event.deal.tradeSide = 1
    event.deal.dealStatus = 2 if execution_type == 3 else 3
    event.position.positionId = 9
    event.position.positionStatus = 1
    event.position.price = 1.101
    event.position.tradeData.symbolId = 11
    event.position.tradeData.volume = volume
    event.position.tradeData.tradeSide = 1
    event.position.tradeData.label = client_id
    if stop_loss is not None:
        event.position.stopLoss = stop_loss
    if take_profit is not None:
        event.position.takeProfit = take_profit
    if opened_at is not None:
        event.position.tradeData.openTimestamp = int(opened_at.timestamp() * 1000)
    return event


def external_executor_for_event(event):
    submitted = False
    current = [NOW]

    def handler(message, client_msg_id, timeout_seconds):
        nonlocal submitted
        del timeout_seconds
        if type(message).__name__ == "ProtoOANewOrderReq":
            submitted = True
            event.order.clientOrderId = client_msg_id
            event.position.tradeData.label = client_msg_id
            return event
        if type(message).__name__ == "ProtoOAReconcileReq":
            return proto.ProtoOAReconcileRes(
                ctidTraderAccountId=123,
                position=[event.position] if submitted else [],
            )
        raise AssertionError(type(message).__name__)

    gateway = Gateway(handler)
    transport_obj = CTraderDemoTransport(
        account(),
        client=gateway,
        proto=proto,
        symbol_ids={"EUR/USD": 11},
        clock=lambda: current[0],
        server_observation=observation(),
        volume_grid=VOLUME_GRID,
    )
    executor = CTraderDemoExecutor(
        account(),
        policy=external_policy(max_positions=2),
        transport=transport_obj,
        clock=lambda: current[0],
        server_observation=observation(),
    )
    executor.activate()
    executor.update_risk_metrics(
        realized_daily_pnl=0,
        unrealized_daily_pnl=0,
        drawdown=0,
        margin_level=100,
        observed_at=NOW,
        connection_generation="generation-1",
    )
    return executor, gateway, current


class CodecRoundTripTransport:
    """Local byte round-trip transport; it never opens a socket."""

    def __init__(self, codec, handler):
        self.codec = codec
        self.handler = handler
        self.connected = False
        self.inbound: queue.Queue[WireMessage] = queue.Queue()
        self.sent: list[WireMessage] = []
        self.reader_threads: set[int] = set()

    def connect(self, timeout=None):
        self.connected = True

    def close(self):
        self.connected = False

    def send(self, message):
        if not self.connected:
            raise RuntimeError("fixture transport disconnected")
        request = self.codec.decode(self.codec.encode(message))
        self.sent.append(request)
        response = self.handler(request)
        if response is not None:
            self.inbound.put(self.codec.decode(self.codec.encode(response)))

    def receive(self, timeout=None):
        import threading

        self.reader_threads.add(threading.get_ident())
        if not self.connected:
            raise RuntimeError("fixture transport disconnected")
        try:
            return self.inbound.get(timeout=timeout or 0.01)
        except queue.Empty:
            return None


def _typed_wire(payload_type: int, body, client_msg_id: str | None) -> WireMessage:
    body.payloadType = payload_type
    return WireMessage(payload_type, body, client_msg_id)


def _execution_event_for_wire(client_msg_id: str) -> object:
    event = proto.ProtoOAExecutionEvent(ctidTraderAccountId=123, executionType=3)
    event.order.orderId = 7
    event.order.clientOrderId = client_msg_id
    event.order.orderType = 1
    event.order.tradeData.symbolId = 11
    event.order.tradeData.volume = 100
    event.order.tradeData.tradeSide = 1
    event.order.orderStatus = 2
    event.order.executedVolume = 100
    event.order.executionPrice = 1.101
    event.deal.dealId = 8
    event.deal.orderId = 7
    event.deal.symbolId = 11
    event.deal.createTimestamp = 1735732800000
    event.deal.positionId = 9
    event.deal.volume = 100
    event.deal.filledVolume = 100
    event.deal.executionTimestamp = 1735732800000
    event.deal.executionPrice = 1.101
    event.deal.tradeSide = 1
    event.deal.dealStatus = 2
    event.position.positionId = 9
    event.position.positionStatus = 1
    event.position.swap = 0
    event.position.price = 1.101
    event.position.tradeData.symbolId = 11
    event.position.tradeData.volume = 100
    event.position.tradeData.tradeSide = 1
    event.position.tradeData.label = client_msg_id
    return event


def _run_local_official_e2e():
    codec = SdkProtobufCodec()

    def handler(request: WireMessage):
        payload_type = request.payload_type_id
        if payload_type == 2100:
            return _typed_wire(2101, proto.ProtoOAApplicationAuthRes(), request.client_msg_id)
        if payload_type == 2149:
            response = proto.ProtoOAGetAccountListByAccessTokenRes(permissionScope=1, accessToken="offline-token")
            response.ctidTraderAccount.add(ctidTraderAccountId=123, isLive=False)
            return _typed_wire(2150, response, request.client_msg_id)
        if payload_type == 2102:
            return _typed_wire(2103, proto.ProtoOAAccountAuthRes(ctidTraderAccountId=123), request.client_msg_id)
        if payload_type == 2124:
            return _typed_wire(2125, proto.ProtoOAReconcileRes(ctidTraderAccountId=123), request.client_msg_id)
        if payload_type == 2106:
            return _typed_wire(2126, _execution_event_for_wire(request.client_msg_id or ""), request.client_msg_id)
        raise AssertionError(f"unexpected payload type: {payload_type}")

    transport = CodecRoundTripTransport(codec, handler)
    config = CTraderConfig(
        environment="demo",
        symbol="EUR/USD",
        symbol_id=11,
        account_id=123,
        client_id="offline-client",
        client_secret_ref="offline-secret-ref",
        access_token_ref="offline-token-ref",
        request_timeout_seconds=1,
        heartbeat_seconds=60,
    )
    client = CTraderClient(config, transport=transport, codec=codec, wall_clock=lambda: NOW)
    client.connect()
    client.authenticate(
        secret_provider=lambda ref: "offline-secret",
        token_provider=lambda ref: "offline-token",
    )
    proof = client.authenticated_session_evidence()
    gateway = CTraderClientGateway(client, clock=lambda: NOW)
    account_obj = account()
    demo_transport = CTraderDemoTransport(
        account_obj,
        client=gateway,
        proto=proto,
        symbol_ids={"EUR/USD": 11},
        clock=lambda: NOW,
        volume_grid=VOLUME_GRID,
    )
    from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor

    executor = CTraderDemoExecutor(
        account_obj,
        policy=external_policy(),
        transport=demo_transport,
        clock=lambda: NOW,
        server_observation=demo_transport.server_observation,
    )
    executor.activate()
    executor.update_risk_metrics(
        realized_daily_pnl=0,
        unrealized_daily_pnl=0,
        drawdown=0,
        margin_level=100,
        observed_at=NOW,
        connection_generation=str(proof.connection_generation),
    )
    result = executor.submit_signal(
        {"signal_id": "e2e-signal", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
        Quote(
            "EUR/USD",
            "1.1009",
            "1.1010",
            NOW,
            NOW,
            source_identity="fixture-quote",
            session_id=proof.session_id,
            connection_generation=proof.connection_generation,
            data_mode="LIVE",
        ),
    )
    return client, transport, proof, result


class CTraderDemoContractTests(unittest.TestCase):
    def test_external_executor_rejects_policy_without_account_risk_gates(self):
        transport_obj = CTraderDemoTransport(
            account(),
            client=Gateway(()),
            proto=proto,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: NOW,
            server_observation=observation(),
        )
        with self.assertRaises(RiskLimitRejected):
            CTraderDemoExecutor(
                account(),
                policy=ExecutionPolicy(max_quantity=1, fixed_quantity=1),
                transport=transport_obj,
                clock=lambda: NOW,
                server_observation=observation(),
            )

    def test_external_risk_metrics_require_fresh_components_and_generation(self):
        current = [NOW]

        def handler(message, client_msg_id, timeout_seconds):
            del client_msg_id, timeout_seconds
            if type(message).__name__ == "ProtoOAReconcileReq":
                return proto.ProtoOAReconcileRes(ctidTraderAccountId=123)
            raise AssertionError(type(message).__name__)

        transport_obj = CTraderDemoTransport(
            account(),
            client=Gateway(handler),
            proto=proto,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: current[0],
            server_observation=observation(),
        )
        executor = CTraderDemoExecutor(
            account(),
            policy=external_policy(),
            transport=transport_obj,
            clock=lambda: current[0],
            server_observation=observation(),
        )
        executor.activate()
        executor.update_risk_metrics(
            realized_daily_pnl=0,
            unrealized_daily_pnl=0,
            drawdown=0,
            margin_level=100,
            observed_at=NOW,
            connection_generation="generation-1",
        )
        self.assertTrue(executor.status()["new_intents_enabled"])

        current[0] = NOW + timedelta(seconds=6)
        self.assertEqual(executor.status()["risk"]["halt_reason"], "risk_metrics_stale")
        executor.update_risk_metrics(
            realized_daily_pnl=0,
            unrealized_daily_pnl=0,
            drawdown=0,
            margin_level=100,
            observed_at=current[0],
            connection_generation="generation-2",
        )
        self.assertEqual(executor.status()["risk"]["halt_reason"], "risk_metrics_generation_unobserved")

    def test_external_risk_breach_remains_latched_until_explicit_clear(self):
        def handler(message, client_msg_id, timeout_seconds):
            del client_msg_id, timeout_seconds
            if type(message).__name__ == "ProtoOAReconcileReq":
                return proto.ProtoOAReconcileRes(ctidTraderAccountId=123)
            raise AssertionError(type(message).__name__)

        transport_obj = CTraderDemoTransport(
            account(),
            client=Gateway(handler),
            proto=proto,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: NOW,
            server_observation=observation(),
        )
        executor = CTraderDemoExecutor(
            account(),
            policy=external_policy(max_daily_loss=1),
            transport=transport_obj,
            clock=lambda: NOW,
            server_observation=observation(),
        )
        executor.activate()
        executor.update_risk_metrics(
            realized_daily_pnl=-2,
            unrealized_daily_pnl=0,
            drawdown=0,
            margin_level=100,
            observed_at=NOW,
            connection_generation="generation-1",
        )
        executor.update_risk_metrics(
            realized_daily_pnl=0,
            unrealized_daily_pnl=0,
            drawdown=0,
            margin_level=100,
            observed_at=NOW,
            connection_generation="generation-1",
        )
        self.assertEqual(executor.status()["risk"]["halt_reason"], "max_daily_loss_exceeded")
        cleared = executor.clear_risk_halt()
        self.assertIsNone(cleared["halt_reason"])
        self.assertTrue(cleared["new_intents_enabled"])

    def test_requested_protection_without_server_protection_keeps_fill_and_blocks_new_entries(self):
        event = filled_event(opened_at=NOW)
        executor, _gateway, _current = external_executor_for_event(event)
        result = executor.submit_signal(
            {
                "signal_id": "missing-server-protection",
                "instrument": "EUR/USD",
                "direction": "UP",
                "mode": "DEMO",
                "order_options": {"stop_loss": "1.09", "take_profit": "1.12"},
            },
            Quote(
                "EUR/USD",
                "1.1009",
                "1.1010",
                NOW,
                NOW,
                source_identity="fixture-quote",
                session_id="fixture-session",
                connection_generation="generation-1",
                data_mode="LIVE",
            ),
        )
        self.assertEqual(result.state, OrderState.FILLED)
        self.assertEqual(result.protection_state, "UNKNOWN")
        status = executor.status()
        self.assertEqual(status["risk"]["protection_state"], "UNKNOWN")
        self.assertEqual(status["risk"]["halt_reason"], "protection_unverified")
        self.assertFalse(status["new_intents_enabled"])

    def test_server_stop_and_take_profit_are_retained_and_mark_protection_observed(self):
        event = filled_event(stop_loss=1.09, take_profit=1.12, opened_at=NOW)
        executor, _gateway, _current = external_executor_for_event(event)
        result = executor.submit_signal(
            {"signal_id": "server-protection", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
            Quote(
                "EUR/USD",
                "1.1009",
                "1.1010",
                NOW,
                NOW,
                source_identity="fixture-quote",
                session_id="fixture-session",
                connection_generation="generation-1",
                data_mode="LIVE",
            ),
        )
        self.assertEqual(result.protection_state, "OBSERVED")
        self.assertEqual(result.stop_loss, Decimal("1.09"))
        self.assertEqual(result.take_profit, Decimal("1.12"))
        position = executor.positions()[0]
        self.assertEqual(position.protection_state, "OBSERVED")
        self.assertEqual(position.stop_loss, Decimal("1.09"))
        self.assertEqual(position.take_profit, Decimal("1.12"))
        status = executor.status()
        self.assertEqual(status["risk"]["protection_state"], "OBSERVED")
        self.assertTrue(status["new_intents_enabled"])

    def test_holding_deadline_closes_known_position_while_strategy_is_paused_once(self):
        opened_at = NOW - timedelta(seconds=3_601)
        open_event = filled_event(stop_loss=1.09, take_profit=1.12, opened_at=opened_at)
        close_event = filled_event(stop_loss=None, take_profit=None, opened_at=None)
        close_event.order.orderId = 8
        close_event.order.positionId = 9
        close_event.order.closingOrder = True
        close_event.deal.orderId = 8
        close_event.deal.positionId = 9
        close_event.position.positionStatus = 2
        close_event.order.ClearField("clientOrderId")
        current = [NOW]
        opened = False
        close_calls = 0

        def handler(message, client_msg_id, timeout_seconds):
            nonlocal opened, close_calls
            del timeout_seconds
            name = type(message).__name__
            if name == "ProtoOANewOrderReq":
                opened = True
                open_event.order.clientOrderId = client_msg_id
                open_event.position.tradeData.label = client_msg_id
                return open_event
            if name == "ProtoOAReconcileReq":
                return proto.ProtoOAReconcileRes(
                    ctidTraderAccountId=123,
                    position=[open_event.position] if opened else [],
                )
            if name == "ProtoOAClosePositionReq":
                close_calls += 1
                opened = False
                return close_event
            raise AssertionError(name)

        transport_obj = CTraderDemoTransport(
            account(),
            client=Gateway(handler),
            proto=proto,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: current[0],
            server_observation=observation(),
            volume_grid=VOLUME_GRID,
        )
        executor = CTraderDemoExecutor(
            account(),
            policy=external_policy(max_positions=2),
            transport=transport_obj,
            clock=lambda: current[0],
            server_observation=observation(),
        )
        executor.activate()
        executor.update_risk_metrics(
            realized_daily_pnl=0,
            unrealized_daily_pnl=0,
            drawdown=0,
            margin_level=100,
            observed_at=NOW,
            connection_generation="generation-1",
        )
        opened_result = executor.submit_signal(
            {"signal_id": "timed", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
            Quote(
                "EUR/USD",
                "1.1009",
                "1.1010",
                NOW,
                NOW,
                source_identity="fixture-quote",
                session_id="fixture-session",
                connection_generation="generation-1",
                data_mode="LIVE",
            ),
        )
        self.assertEqual(opened_result.state, OrderState.FILLED)
        current[0] = NOW + timedelta(seconds=3_601)
        executor.pause("strategy paused")
        managed = executor.manage()
        self.assertEqual(len(managed), 1)
        self.assertEqual(managed[0].state, OrderState.CLOSED)
        self.assertIsNone(managed[0].intent.requested_price)
        self.assertEqual(close_calls, 1)
        self.assertEqual(executor.manage(), ())
        self.assertEqual(close_calls, 1)

    def test_future_server_open_timestamp_blocks_new_entries_without_closing_early(self):
        event = filled_event(stop_loss=1.09, take_profit=1.12, opened_at=NOW + timedelta(seconds=60))
        executor, _gateway, _current = external_executor_for_event(event)
        result = executor.submit_signal(
            {"signal_id": "future-open", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
            Quote(
                "EUR/USD",
                "1.1009",
                "1.1010",
                NOW,
                NOW,
                source_identity="fixture-quote",
                session_id="fixture-session",
                connection_generation="generation-1",
                data_mode="LIVE",
            ),
        )
        self.assertEqual(result.state, OrderState.FILLED)
        status = executor.status()
        self.assertEqual(status["risk"]["halt_reason"], "opened_at_future")
        self.assertFalse(status["new_intents_enabled"])

    def test_decimal_volume_roundtrip_is_independent_of_process_context(self):
        original = getcontext().copy()
        try:
            getcontext().prec = 2
            self.assertEqual(_volume_to_protocol(Decimal("0.10"), 100), 10)
            self.assertEqual(_volume_from_protocol(123456789, 100), Decimal("1234567.89"))
        finally:
            getcontext().prec = original.prec
            getcontext().rounding = original.rounding

    def test_protocol_presence_does_not_turn_missing_fields_into_zero_or_empty(self):
        event = proto.ProtoOAExecutionEvent(ctidTraderAccountId=123, executionType=3)
        self.assertIsNone(_field(event, "deal"))
        self.assertIsNone(_field(event, "order"))
        reconcile = proto.ProtoOAReconcileRes(ctidTraderAccountId=123)
        self.assertEqual(_many(reconcile, "position"), [])
        if hasattr(reconcile, "returnProtectionOrders"):
            self.assertFalse(reconcile.returnProtectionOrders)
        with self.assertRaises(OfficialCorrelationError):
            transport(Gateway([event])).submit(intent(), timeout_seconds=1)

    def test_exact_execution_enums_distinguish_cancel_expiry_reject_and_partial(self):
        for execution_type, expected in ((5, OrderState.CANCELLED), (6, OrderState.EXPIRED), (7, OrderState.REJECTED)):
            result = transport(Gateway([filled_event(execution_type=execution_type, volume=0)])).submit(
                intent(), timeout_seconds=1
            )
            self.assertEqual(result.status, expected)
            self.assertEqual(result.filled_quantity, Decimal("0"))
        partial = transport(Gateway([filled_event(execution_type=11, volume=50)])).submit(intent(), timeout_seconds=1)
        self.assertEqual(partial.status, OrderState.PARTIAL)
        self.assertEqual(partial.remaining_quantity, Decimal("0.5"))

    def test_timeout_is_an_uncertain_sent_phase_not_a_generic_transport_error(self):
        def fail(*_):
            raise TimeoutError("no ack")

        gateway = Gateway(())
        gateway.responses = fail
        with self.assertRaises(OfficialUncertainSendError) as caught:
            transport(gateway).submit(intent(), timeout_seconds=1)
        self.assertEqual(caught.exception.phase, SendPhase.SENT_NO_RESPONSE)
        self.assertTrue(caught.exception.uncertain)
        self.assertFalse(issubclass(OfficialTransportError, TimeoutError))

    def test_no_quantity_default_and_no_filled_requested_guess(self):
        order = model.ProtoOAOrder(orderId=7, orderStatus=2, clientOrderId="intent-1")
        reconcile = proto.ProtoOAReconcileRes(ctidTraderAccountId=123, order=[order])
        t = transport(Gateway([reconcile]))
        with self.assertRaises(OfficialMessageError):
            t.get_order("intent-1")
        event = proto.ProtoOAExecutionEvent(ctidTraderAccountId=123, executionType=3)
        event.order.orderId = 7
        event.order.clientOrderId = "intent-1"
        result = transport(Gateway([event])).submit(intent(), timeout_seconds=1)
        self.assertEqual(result.status, OrderState.UNKNOWN)
        self.assertEqual(result.filled_quantity, Decimal("0"))

    def test_deals_are_deduplicated_by_account_environment_and_deal_id(self):
        first = filled_event(volume=50, deal_id=8, execution_type=11)
        second = filled_event(volume=50, deal_id=8, execution_type=11)
        third = filled_event(volume=50, deal_id=9, execution_type=11)
        gateway = Gateway([first, second])
        t = transport(gateway)
        first_snapshot = t.submit(intent(), timeout_seconds=1)
        self.assertEqual(first_snapshot.filled_quantity, Decimal("0.5"))
        t._requested_quantities["intent-1"] = intent().quantity
        second_snapshot = t._snapshot_from_response(
            second, client_order_id="intent-1", requested_quantity=Decimal("1"), closing=False
        )
        self.assertEqual(second_snapshot.filled_quantity, Decimal("0.5"))
        third_snapshot = t._snapshot_from_response(
            third, client_order_id="intent-1", requested_quantity=Decimal("1"), closing=False
        )
        self.assertEqual(third_snapshot.filled_quantity, Decimal("1"))
        self.assertEqual({fill.fill_id for fill in third_snapshot.fills}, {"8", "9"})

    def test_same_client_correlation_cannot_switch_server_order_identity(self):
        first = filled_event(volume=50, deal_id=8, execution_type=11)
        second = filled_event(volume=50, deal_id=9, execution_type=11)
        second.order.orderId = 8
        second.deal.orderId = 8
        t = transport(Gateway([first]))
        initial = t.submit(intent(), timeout_seconds=1)
        with self.assertRaises(OfficialCorrelationError):
            t._snapshot_from_response(
                second,
                client_order_id="intent-1",
                requested_quantity=Decimal("1"),
                closing=False,
            )
        cached = t._snapshots["intent-1"]
        self.assertEqual(cached.order_id, "7")
        self.assertEqual({fill.fill_id for fill in cached.fills}, {"8"})
        self.assertEqual(initial.order_id, "7")

    def test_reconcile_miss_does_not_return_stale_cache_and_history_recovers_order(self):
        rec = proto.ProtoOAReconcileRes(ctidTraderAccountId=123)
        order = model.ProtoOAOrder(
            orderId=7,
            tradeData=model.ProtoOATradeData(symbolId=11, volume=100, tradeSide=1),
            orderType=1,
            orderStatus=2,
            executedVolume=100,
            executionPrice=1.101,
            clientOrderId="intent-1",
        )
        history = proto.ProtoOAOrderListRes(ctidTraderAccountId=123, order=[order], hasMore=False)
        details = proto.ProtoOAOrderDetailsRes(ctidTraderAccountId=123, order=order)
        deals = proto.ProtoOADealListRes(ctidTraderAccountId=123, hasMore=False)
        gateway = Gateway([rec, history, details, deals])
        t = transport(gateway)
        t._requested_quantities["intent-1"] = intent().quantity
        t._intent_created_at["intent-1"] = NOW
        recovered = t.get_order("intent-1")
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered.status, OrderState.FILLED)
        self.assertEqual(recovered.filled_quantity, Decimal("1"))
        self.assertEqual(
            [type(call[0]).__name__ for call in gateway.calls],
            ["ProtoOAReconcileReq", "ProtoOAOrderListReq", "ProtoOAOrderDetailsReq", "ProtoOADealListReq"],
        )

    def test_explicit_client_gateway_requires_request_message_and_correlates_wire_response(self):
        class LocalClient:
            def __init__(self, response):
                self.response = response
                self.calls = []

            def request_message(self, message, *, client_msg_id, timeout_seconds):
                self.calls.append((message, client_msg_id, timeout_seconds))
                return self.response

            def authenticated_session_evidence(self):
                return self.session

            def validate_session_evidence(self, proof):
                return True

        session = AuthenticatedSession(
            "session-1",
            "123",
            "DEMO",
            "demo.ctraderapi.com:5035",
            {"trading"},
            NOW,
            "generation-1",
            NOW + timedelta(hours=1),
        )
        wire = WireMessage(PAYLOAD_TYPES["ProtoOAExecutionEvent"], filled_event(), "intent-1")
        local = LocalClient(wire)
        local.session = session
        gateway = CTraderClientGateway(local, clock=lambda: NOW)
        body = gateway.send(proto.ProtoOANewOrderReq(), client_msg_id="intent-1", timeout_seconds=1)
        self.assertEqual(body, wire.payload)
        wrong = LocalClient(WireMessage(PAYLOAD_TYPES["ProtoOAExecutionEvent"], filled_event(), "other"))
        wrong.session = session
        with self.assertRaises(OfficialCorrelationError):
            CTraderClientGateway(wrong, clock=lambda: NOW).send(
                proto.ProtoOANewOrderReq(), client_msg_id="intent-1", timeout_seconds=1
            )

        class SendOnly:
            def send(self, message, **kwargs):
                return None

        with self.assertRaises(OfficialGatewayConfigurationError):
            CTraderClientGateway(SendOnly(), clock=lambda: NOW)

    def test_session_bound_observation_is_required_for_c_trader_client_gateway(self):
        class LocalClient:
            def request_message(self, message, *, client_msg_id, timeout_seconds):
                return WireMessage(
                    PAYLOAD_TYPES["ProtoOAReconcileRes"],
                    proto.ProtoOAReconcileRes(ctidTraderAccountId=123),
                    client_msg_id,
                )

            def authenticated_session_evidence(self):
                return self.session

            def validate_session_evidence(self, proof):
                return True

        session = AuthenticatedSession(
            "session-1",
            "123",
            "DEMO",
            "demo.ctraderapi.com:5035",
            {"trading"},
            NOW,
            "generation-1",
            NOW + timedelta(hours=1),
        )
        local = LocalClient()
        local.session = session
        gateway = CTraderClientGateway(local, clock=lambda: NOW)
        unbound = ServerAccountObservation(
            "123", "DEMO", "demo.ctraderapi.com:5035", {"trading"}, NOW, source="fixture-server"
        )
        with self.assertRaises(DemoAccountRequired):
            CTraderDemoTransport(
                account(),
                client=gateway,
                proto=proto,
                symbol_ids={"EUR/USD": 11},
                clock=lambda: NOW,
                server_observation=unbound,
            )
        bound = ServerAccountObservation.from_session(session)
        t = CTraderDemoTransport(
            account(),
            client=gateway,
            proto=proto,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: NOW,
            server_observation=bound,
        )
        self.assertEqual(t.account_id, "123")

    def test_executor_records_local_message_error_without_marking_outcome_unknown(self):
        from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor

        reconcile = proto.ProtoOAReconcileRes(ctidTraderAccountId=123)
        t = CTraderDemoTransport(
            account(),
            client=Gateway([reconcile, reconcile, reconcile, reconcile]),
            proto=proto,
            symbol_ids={},
            clock=lambda: NOW,
            server_observation=observation(),
            volume_grid=VOLUME_GRID,
        )
        executor = CTraderDemoExecutor(
            account(),
            policy=external_policy(),
            transport=t,
            clock=lambda: NOW,
            server_observation=observation(),
        )
        executor.activate()
        executor.update_risk_metrics(
            realized_daily_pnl=0,
            unrealized_daily_pnl=0,
            drawdown=0,
            margin_level=100,
            observed_at=NOW,
            connection_generation="generation-1",
        )
        result = executor.submit_signal(
            {"signal_id": "local-bad", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
            Quote("EUR/USD", "1.1009", "1.1010", NOW, NOW),
        )
        self.assertEqual(result.state, OrderState.INTENT_RECORDED)
        self.assertEqual(result.uncertainty_reason, "LOCAL_ERROR")
        self.assertEqual(result.unknown_reason, None)

    def test_executor_marks_response_protocol_error_uncertain_without_retry(self):
        from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor

        bad = proto.ProtoOAExecutionEvent(ctidTraderAccountId=999, executionType=3)
        t = CTraderDemoTransport(
            account(),
            client=Gateway(
                [
                    proto.ProtoOAReconcileRes(ctidTraderAccountId=123),
                    proto.ProtoOAReconcileRes(ctidTraderAccountId=123),
                    proto.ProtoOAReconcileRes(ctidTraderAccountId=123),
                    proto.ProtoOAReconcileRes(ctidTraderAccountId=123),
                    bad,
                ]
            ),
            proto=proto,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: NOW,
            server_observation=observation(),
            volume_grid=VOLUME_GRID,
        )
        executor = CTraderDemoExecutor(
            account(),
            policy=external_policy(),
            transport=t,
            clock=lambda: NOW,
            server_observation=observation(),
        )
        executor.activate()
        executor.update_risk_metrics(
            realized_daily_pnl=0,
            unrealized_daily_pnl=0,
            drawdown=0,
            margin_level=100,
            observed_at=NOW,
            connection_generation="generation-1",
        )
        result = executor.submit_signal(
            {"signal_id": "bad-response", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
            Quote("EUR/USD", "1.1009", "1.1010", NOW, NOW),
        )
        self.assertEqual(result.state, OrderState.UNKNOWN)
        self.assertEqual(result.uncertainty_reason, "SENT")
        self.assertEqual(result.remaining_quantity, Decimal("1"))

    def test_gateway_preserves_cancel_before_send_phase(self):
        class LocalClient:
            def __init__(self):
                self.session = AuthenticatedSession(
                    "session-1", "123", "DEMO", "demo.ctraderapi.com:5035", {"trading"}, NOW, "generation-1"
                )
                self.status = type("Status", (), {"last_request_phase": RequestPhase.CANCELLED_BEFORE_SEND})()

            def authenticated_session_evidence(self):
                return self.session

            def validate_session_evidence(self, proof):
                return True

            def request_message(self, message, *, client_msg_id, timeout_seconds):
                error = CTraderRequestCancelled("cancelled before send")
                error.phase = RequestPhase.CANCELLED_BEFORE_SEND
                raise error

        gateway = CTraderClientGateway(LocalClient(), clock=lambda: NOW)
        with self.assertRaises(OfficialCancelledError) as caught:
            gateway.send(proto.ProtoOANewOrderReq(), client_msg_id="cancelled", timeout_seconds=1)
        self.assertEqual(caught.exception.phase, SendPhase.CANCELLED_BEFORE_SEND)
        self.assertFalse(isinstance(caught.exception, TimeoutError))

    def test_installed_codec_e2e_uses_one_local_client_and_real_generated_execution_messages(self):
        client = None
        try:
            client, transport_obj, proof, result = _run_local_official_e2e()
            self.assertTrue(proof)
            self.assertEqual(int(proof.account_id), 123)
            self.assertEqual(str(proof.environment).upper(), "DEMO")
            self.assertIn("trading", set(proof.scopes))
            self.assertTrue(str(proof.connection_generation))
            self.assertEqual(result.state, OrderState.FILLED)
            self.assertEqual(result.filled_quantity, Decimal("1"))
            self.assertEqual(result.position_ids, ("9",))
            self.assertEqual(
                {message.payload_type_id for message in transport_obj.sent}, {2100, 2149, 2102, 2124, 2106}
            )
            self.assertEqual(len(transport_obj.reader_threads), 1)
        finally:
            if client is not None:
                client.close()


if __name__ == "__main__":
    unittest.main()
