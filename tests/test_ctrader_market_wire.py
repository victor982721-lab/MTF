"""Focused official MARKET protection wire contracts.

These tests deliberately use the generated Spotware protobuf rather than a
duck-typed message.  A fake gateway is present only to prove that invalid
payloads fail before any send boundary is crossed.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from decimal import Decimal

from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.ops.ctrader_demo_transport import (
    CTraderDemoTransport,
    DemoAccount,
    OfficialMessageError,
    ServerAccountObservation,
)
from mtf_lab.ops.ctrader_executor import ExecutionIntent, Side
from tests.test_ctrader_demo_transport import VOLUME_GRID
from tests.test_demo_canary import NOW


class _NoNetworkGateway:
    """OfficialGateway-shaped sink; any call means validation was too late."""

    def __init__(self) -> None:
        self.messages: list[object] = []

    def send(self, message: object, *, client_msg_id: str, timeout_seconds: float) -> object:
        del client_msg_id, timeout_seconds
        self.messages.append(message)
        raise AssertionError("wire fixture no debe cruzar el gateway")


def _wire_transport(gateway: _NoNetworkGateway) -> CTraderDemoTransport:
    observed = ServerAccountObservation(
        "123",
        "DEMO",
        "demo.ctraderapi.com:5035",
        frozenset({"trading"}),
        NOW,
        source="fixture-server",
        session_id="market-wire-fixture",
        connection_generation="market-wire-generation",
    )
    return CTraderDemoTransport(
        DemoAccount(
            "123",
            "DEMO",
            "demo.ctraderapi.com:5035",
            frozenset({"trading"}),
            selected=True,
            verified=True,
        ),
        client=gateway,
        proto=proto,
        symbol_ids={"EUR/USD": 11},
        symbol_names={11: "EUR/USD"},
        clock=lambda: NOW,
        server_observation=observed,
        volume_grid=VOLUME_GRID,
    )


def _intent(tag: str, options: dict[str, object]) -> ExecutionIntent:
    return ExecutionIntent(
        f"market-wire-{tag}",
        f"signal-{tag}",
        "EUR/USD",
        Side.BUY,
        Decimal("1"),
        Decimal("1.10100"),
        datetime(2026, 9, 19, 14, 0, tzinfo=UTC),
        "123",
        metadata={"order_options": options},
    )


class CTraderMarketWireTests(unittest.TestCase):
    def test_relative_market_wire_is_exact_integer_grid_and_preserves_identity(self) -> None:
        gateway = _NoNetworkGateway()
        transport = _wire_transport(gateway)
        intent = _intent(
            "valid",
            {"relative_stop_loss": 150, "relative_take_profit": 300},
        )

        message = transport.build_new_order(intent)
        self.assertIsInstance(message, proto.ProtoOANewOrderReq)
        self.assertEqual(message.orderType, 1)
        self.assertEqual(message.tradeSide, 1)
        self.assertEqual(message.volume, 100)
        self.assertEqual(message.clientOrderId, intent.intent_id)
        self.assertEqual(message.ctidTraderAccountId, 123)
        self.assertEqual(message.symbolId, 11)
        self.assertTrue(message.HasField("relativeStopLoss"))
        self.assertTrue(message.HasField("relativeTakeProfit"))
        self.assertEqual(message.relativeStopLoss, 150)
        self.assertEqual(message.relativeTakeProfit, 300)
        self.assertFalse(message.HasField("stopLoss"))
        self.assertFalse(message.HasField("takeProfit"))
        self.assertEqual(transport.available_scopes(), frozenset({"trading"}))
        self.assertEqual(gateway.messages, [])

        # Presence bits and integer values must survive the actual protobuf
        # bytes, not only the in-memory generated object.
        decoded = proto.ProtoOANewOrderReq()
        decoded.ParseFromString(message.SerializeToString())
        self.assertTrue(decoded.HasField("relativeStopLoss"))
        self.assertTrue(decoded.HasField("relativeTakeProfit"))
        self.assertFalse(decoded.HasField("stopLoss"))
        self.assertFalse(decoded.HasField("takeProfit"))
        self.assertEqual((decoded.relativeStopLoss, decoded.relativeTakeProfit, decoded.volume), (150, 300, 100))

    def test_absolute_market_protection_is_rejected_before_send(self) -> None:
        gateway = _NoNetworkGateway()
        transport = _wire_transport(gateway)
        with self.assertRaises(OfficialMessageError):
            transport.submit(
                _intent("absolute", {"stop_loss": "1.09950", "take_profit": "1.10400"}),
                timeout_seconds=1,
            )
        self.assertEqual(gateway.messages, [])

    def test_mixed_absolute_and_relative_market_protection_is_rejected_before_send(self) -> None:
        gateway = _NoNetworkGateway()
        transport = _wire_transport(gateway)
        with self.assertRaises(OfficialMessageError):
            transport.submit(
                _intent("mixed", {"relative_stop_loss": 150, "take_profit": "1.10400"}),
                timeout_seconds=1,
            )
        self.assertEqual(gateway.messages, [])

    def test_fractional_relative_market_protection_is_rejected_before_send(self) -> None:
        gateway = _NoNetworkGateway()
        transport = _wire_transport(gateway)
        with self.assertRaises(OfficialMessageError):
            transport.submit(
                _intent("fractional", {"relative_stop_loss": 150.5, "relative_take_profit": 300}),
                timeout_seconds=1,
            )
        self.assertEqual(gateway.messages, [])

    def test_out_of_int64_relative_market_protection_is_rejected_before_send(self) -> None:
        gateway = _NoNetworkGateway()
        transport = _wire_transport(gateway)
        with self.assertRaises(OfficialMessageError):
            transport.submit(
                _intent("overflow", {"relative_stop_loss": 150, "relative_take_profit": 2**63}),
                timeout_seconds=1,
            )
        self.assertEqual(gateway.messages, [])


if __name__ == "__main__":
    unittest.main()
