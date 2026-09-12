from __future__ import annotations

import unittest
from datetime import UTC, datetime

from mtf_lab.ops.ctrader_demo_transport import (
    CTraderDemoTransport,
    EndpointRejected,
    OfficialCorrelationError,
    ServerAccountObservation,
    load_official_proto,
)
from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DemoAccount,
    DemoAccountRequired,
    ExecutionIntent,
    ExecutionPolicy,
    MemoryIntentStore,
    OrderState,
    Position,
    Quote,
    RealAccountForbidden,
    Side,
)


class ProtoMessage:
    def __init__(self, **values):
        self.__dict__.update(values)


class EnumFixture:
    @staticmethod
    def Value(name):
        return name


class ProtoFixture:
    ProtoOANewOrderReq = type("ProtoOANewOrderReq", (ProtoMessage,), {})
    ProtoOAClosePositionReq = type("ProtoOAClosePositionReq", (ProtoMessage,), {})
    ProtoOAReconcileReq = type("ProtoOAReconcileReq", (ProtoMessage,), {})
    ProtoOAApplicationAuthReq = type("ProtoOAApplicationAuthReq", (ProtoMessage,), {})
    ProtoOAAccountAuthReq = type("ProtoOAAccountAuthReq", (ProtoMessage,), {})
    ProtoOAOrderType = EnumFixture
    ProtoOATradeSide = EnumFixture


class FakeOfficialGateway:
    def __init__(self, handler):
        self.handler = handler
        self.messages: list[tuple[object, dict]] = []

    def send(self, message, **kwargs):
        self.messages.append((message, dict(kwargs)))
        return self.handler(message, kwargs)


def _event(
    *,
    account=123,
    client_order_id=None,
    execution_type="ORDER_FILLED",
    order_id=7,
    position_id=9,
    symbol_id=11,
    volume=100,
    price=1.1010,
    timestamp=1735732800000,
    error_code=None,
):
    order_values = {
        "orderId": order_id,
        "executedVolume": volume,
        "executionPrice": price,
        "orderStatus": "ORDER_STATUS_FILLED" if execution_type == "ORDER_FILLED" else "ORDER_STATUS_ACCEPTED",
    }
    if client_order_id is not None:
        order_values["clientOrderId"] = client_order_id
    order = ProtoMessage(**order_values)
    deal = ProtoMessage(
        dealId=8,
        orderId=order_id,
        positionId=position_id,
        filledVolume=volume,
        volume=volume,
        executionPrice=price,
        executionTimestamp=timestamp,
        dealStatus="FILLED" if execution_type == "ORDER_FILLED" else "PARTIALLY_FILLED",
    )
    trade = ProtoMessage(symbolId=symbol_id, volume=volume, tradeSide="BUY", label=client_order_id or "")
    position = ProtoMessage(positionId=position_id, price=price, tradeData=trade)
    values = {
        "ctidTraderAccountId": account,
        "executionType": execution_type,
        "order": order,
        "deal": deal,
        "position": position,
    }
    if error_code is not None:
        values["errorCode"] = error_code
    return ProtoMessage(**values)


class CTraderOfficialTransportTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
        self.account = DemoAccount(
            "123", "DEMO", "demo.ctraderapi.com:5035", frozenset({"trading"}), selected=True, verified=True
        )
        self.intent = ExecutionIntent("intent-1", "signal-1", "EUR/USD", Side.BUY, 1.0, 1.1010, self.now, "123")
        self.observation = ServerAccountObservation(
            "123", "DEMO", "demo.ctraderapi.com:5035", frozenset({"trading"}), self.now, source="fixture-server"
        )

    def test_official_order_close_reconcile_messages_are_built(self):
        gateway = FakeOfficialGateway(lambda message, kwargs: None)
        transport = CTraderDemoTransport(
            self.account,
            client=gateway,
            proto=ProtoFixture,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: self.now,
            server_observation=self.observation,
        )
        new = transport.build_new_order(self.intent)
        self.assertEqual(type(new).__name__, "ProtoOANewOrderReq")
        self.assertEqual(new.ctidTraderAccountId, 123)
        self.assertEqual(new.symbolId, 11)
        self.assertEqual(new.orderType, "MARKET")
        self.assertEqual(new.tradeSide, "BUY")
        self.assertEqual(new.volume, 100)
        self.assertEqual(new.clientOrderId, "intent-1")
        close = transport.build_close_position(
            Position("9", "123", "EUR/USD", Side.BUY, 1, 1.101, "intent-1"), client_order_id="close-1"
        )
        self.assertEqual(type(close).__name__, "ProtoOAClosePositionReq")
        self.assertEqual(close.ctidTraderAccountId, 123)
        self.assertEqual(close.positionId, 9)
        self.assertEqual(close.volume, 100)
        reconcile = transport.build_reconcile()
        self.assertEqual(type(reconcile).__name__, "ProtoOAReconcileReq")
        self.assertEqual(reconcile.ctidTraderAccountId, 123)
        self.assertFalse(reconcile.returnProtectionOrders)
        app = transport.build_application_auth("client", "secret")
        self.assertEqual((app.clientId, app.clientSecret), ("client", "secret"))
        account_auth = transport.build_account_auth("access-token")
        self.assertEqual((account_auth.ctidTraderAccountId, account_auth.accessToken), (123, "access-token"))
        self.assertEqual(gateway.messages, [])

    def test_caller_verified_flag_alone_cannot_construct_external_transport(self):
        gateway = FakeOfficialGateway(lambda *_: None)
        with self.assertRaises(DemoAccountRequired):
            CTraderDemoTransport(
                self.account,
                client=gateway,
                proto=ProtoFixture,
                symbol_ids={"EUR/USD": 11},
            )

    def test_executor_can_derive_verified_gate_from_server_observation(self):
        from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor

        account = DemoAccount(
            "123",
            "DEMO",
            "demo.ctraderapi.com:5035",
            frozenset(),
            selected=True,
            verified=False,
        )
        executor = CTraderDemoExecutor(
            account,
            policy=ExecutionPolicy(fixed_quantity=1, max_quantity=1),
            transport=CTraderDemoTransport(
                account,
                client=FakeOfficialGateway(lambda *_: None),
                proto=ProtoFixture,
                symbol_ids={"EUR/USD": 11},
                server_observation=self.observation,
            ),
            server_observation=self.observation,
            clock=lambda: self.now,
        )
        executor.activate()
        self.assertTrue(executor.active)
        self.assertFalse(executor.status()["virtual_only"])

    def test_installed_generated_protobuf_accepts_order_and_reconcile_fields(self):
        from mtf_lab.data.ctrader import dependency_report

        if not dependency_report().codec_operational:
            self.skipTest("SDK cTrader opcional no instalado")
        proto = load_official_proto()
        gateway = FakeOfficialGateway(lambda *_: None)
        observation = ServerAccountObservation(
            "123",
            "DEMO",
            "demo.ctraderapi.com:5035",
            frozenset({"trading"}),
            self.now,
            source="fixture-server",
        )
        transport = CTraderDemoTransport(
            self.account,
            client=gateway,
            proto=proto,
            symbol_ids={"EUR/USD": 11},
            server_observation=observation,
        )
        new = transport.build_new_order(self.intent)
        self.assertEqual((new.orderType, new.tradeSide, new.volume), (1, 1, 100))
        reconcile = transport.build_reconcile()
        self.assertEqual(reconcile.ctidTraderAccountId, 123)
        self.assertFalse(hasattr(reconcile, "returnProtectionOrders"))

    def test_official_execution_event_maps_to_executor_fill_and_position(self):
        store = MemoryIntentStore()

        def handler(message, kwargs):
            if type(message).__name__ == "ProtoOANewOrderReq":
                return _event(client_order_id=message.clientOrderId)
            if type(message).__name__ == "ProtoOAReconcileReq":
                return ProtoMessage(
                    ctidTraderAccountId=123, position=[_event(client_order_id="intent-1").position], order=[]
                )
            raise AssertionError(type(message).__name__)

        gateway = FakeOfficialGateway(handler)
        transport = CTraderDemoTransport(
            self.account,
            client=gateway,
            proto=ProtoFixture,
            symbol_ids={"EUR/USD": 11},
            symbol_names={11: "EUR/USD"},
            clock=lambda: self.now,
            server_observation=self.observation,
        )
        executor = CTraderDemoExecutor(
            self.account,
            policy=ExecutionPolicy(
                fixed_quantity=1,
                max_quantity=1,
                max_positions=2,
                max_exposure=1000,
                max_spread=1,
                max_price_age_seconds=5,
            ),
            transport=transport,
            intent_store=store,
            clock=lambda: self.now,
            server_observation=self.observation,
        )
        executor.activate()
        result = executor.submit_signal(
            {"signal_id": "signal-1", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
            Quote("EUR/USD", 1.1009, 1.1010, self.now, self.now),
        )
        self.assertEqual(result.state, OrderState.FILLED)
        self.assertEqual(result.order_id, "7")
        self.assertEqual(result.filled_quantity, 1.0)
        self.assertEqual(result.position_ids, ("9",))
        self.assertEqual(executor.positions()[0].position_id, "9")
        sent_index, sent = next(
            (index, message)
            for index, (message, kwargs) in enumerate(gateway.messages)
            if type(message).__name__ == "ProtoOANewOrderReq"
        )
        self.assertEqual(sent.clientOrderId, result.intent.intent_id)
        self.assertEqual(gateway.messages[sent_index][1]["client_msg_id"], result.intent.intent_id)

    def test_partial_execution_then_reconcile_full_uses_official_reconcile_without_resubmit(self):
        def handler(message, kwargs):
            if type(message).__name__ == "ProtoOANewOrderReq":
                return _event(client_order_id=message.clientOrderId, execution_type="ORDER_PARTIAL_FILL", volume=50)
            if type(message).__name__ == "ProtoOAReconcileReq":
                return _event(client_order_id="intent-1", execution_type="ORDER_FILLED", volume=100)
            raise AssertionError(type(message).__name__)

        gateway = FakeOfficialGateway(handler)
        transport = CTraderDemoTransport(
            self.account,
            client=gateway,
            proto=ProtoFixture,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: self.now,
            server_observation=self.observation,
        )
        result = transport.submit(self.intent, timeout_seconds=1)
        self.assertEqual(result.status, OrderState.PARTIAL)
        self.assertEqual(result.filled_quantity, 0.5)
        reconciled = transport.get_order("intent-1")
        self.assertEqual(reconciled.status, OrderState.FILLED)
        self.assertEqual(reconciled.filled_quantity, 1.0)
        self.assertEqual(len(gateway.messages), 2)
        self.assertEqual(type(gateway.messages[1][0]).__name__, "ProtoOAReconcileReq")

    def test_timeout_is_raised_for_executor_to_mark_unknown_and_no_resend(self):
        calls = []

        def handler(message, kwargs):
            calls.append(type(message).__name__)
            if type(message).__name__ == "ProtoOANewOrderReq":
                raise TimeoutError("no ack")
            return None

        gateway = FakeOfficialGateway(handler)
        transport = CTraderDemoTransport(
            self.account,
            client=gateway,
            proto=ProtoFixture,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: self.now,
            server_observation=self.observation,
        )
        with self.assertRaises(TimeoutError):
            transport.submit(self.intent, timeout_seconds=0.1)
        self.assertEqual(calls, ["ProtoOANewOrderReq"])
        # The caller can query reconciliation; this transport never calls the
        # new-order message again as a side effect of get_order.
        self.assertIsNone(transport.get_order("intent-1"))
        self.assertEqual(calls, ["ProtoOANewOrderReq", "ProtoOAReconcileReq"])

    def test_real_scope_endpoint_and_correlation_fail_closed(self):
        with self.assertRaises(RealAccountForbidden):
            CTraderDemoTransport(
                {"environment": "REAL", "endpoint": None},
                client=FakeOfficialGateway(lambda *_: None),
                proto=ProtoFixture,
            )
        with self.assertRaises(EndpointRejected):
            CTraderDemoTransport(
                DemoAccount("123", "DEMO", "live.ctraderapi.com:5035", frozenset({"trading"}), True, True),
                client=FakeOfficialGateway(lambda *_: None),
                proto=ProtoFixture,
                server_observation=ServerAccountObservation(
                    "123", "DEMO", "live.ctraderapi.com:5035", frozenset({"trading"}), self.now, source="fixture-server"
                ),
            )
        proof_only = CTraderDemoTransport(
            DemoAccount("123", "DEMO", "demo.ctraderapi.com:5035", frozenset(), True, False),
            client=FakeOfficialGateway(lambda *_: None),
            proto=ProtoFixture,
            server_observation=self.observation,
        )
        self.assertIn("trading", proof_only.available_scopes())
        gateway = FakeOfficialGateway(lambda message, kwargs: _event(client_order_id="different"))
        transport = CTraderDemoTransport(
            self.account,
            client=gateway,
            proto=ProtoFixture,
            symbol_ids={"EUR/USD": 11},
            clock=lambda: self.now,
            server_observation=self.observation,
        )
        with self.assertRaises(OfficialCorrelationError):
            transport.submit(self.intent, timeout_seconds=1)


if __name__ == "__main__":
    unittest.main()
