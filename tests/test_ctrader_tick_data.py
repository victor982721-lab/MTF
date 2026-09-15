"""Offline contracts for the cTrader historical tick-data reader."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from mtf_lab.data.ctrader import (
    PAYLOAD,
    CTraderClient,
    CTraderConfig,
    CTraderConfigurationError,
    CTraderDataError,
    CTraderInstrumentSpec,
    CTraderProvider,
    DeterministicTransport,
    SdkProtobufCodec,
    WireMessage,
    read_field,
    read_repeated,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)
SPEC = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99, digits=5, pip_position=4, price_scale=100_000)


def _provider(
    handler: Any,
    *,
    clock: datetime = BASE + timedelta(hours=2),
) -> tuple[CTraderProvider, CTraderClient, DeterministicTransport]:
    config = CTraderConfig(
        symbol="EUR/USD",
        symbol_id=SPEC.symbol_id,
        account_id=7,
        request_timeout_seconds=1.0,
        heartbeat_seconds=60.0,
    )
    transport = DeterministicTransport(handler)
    client = CTraderClient(config, transport=transport, wall_clock=lambda: clock)
    client.connect()
    client.mark_authenticated(7)
    provider = CTraderProvider(config, client=client, clock=lambda: clock)
    return provider, client, transport


class CTraderTickCodecTests(unittest.TestCase):
    def test_official_tick_request_and_response_round_trip(self) -> None:
        codec = SdkProtobufCodec()
        request = WireMessage(
            "PROTO_OA_GET_TICKDATA_REQ",
            {
                "ctidTraderAccountId": 7,
                "symbolId": 99,
                "type": 1,
                "fromTimestamp": int(BASE.timestamp() * 1000),
                "toTimestamp": int((BASE + timedelta(hours=1)).timestamp() * 1000),
            },
            "tick-request",
        )
        decoded_request = codec.decode(codec.encode(request))
        self.assertEqual(decoded_request.payload_type_id, PAYLOAD["PROTO_OA_GET_TICKDATA_REQ"])
        self.assertEqual(decoded_request.client_msg_id, "tick-request")
        self.assertEqual(read_field(decoded_request.payload, "type"), 1)
        self.assertEqual(read_field(decoded_request.payload, "symbolId"), 99)

        response = WireMessage(
            "PROTO_OA_GET_TICKDATA_RES",
            {
                "ctidTraderAccountId": 7,
                "tickData": [
                    {"timestamp": int((BASE + timedelta(hours=1)).timestamp() * 1000), "tick": 110020},
                    {"timestamp": -250, "tick": 110010},
                ],
                "hasMore": False,
            },
            "tick-request",
        )
        decoded_response = codec.decode(codec.encode(response))
        self.assertEqual(decoded_response.payload_type_id, PAYLOAD["PROTO_OA_GET_TICKDATA_RES"])
        rows = read_repeated(decoded_response.payload, "tickData")
        self.assertEqual(
            [read_field(row, "timestamp") for row in rows],
            [int((BASE + timedelta(hours=1)).timestamp() * 1000), -250],
        )
        self.assertEqual([read_field(row, "tick") for row in rows], [110020, 110010])


class CTraderTickProviderTests(unittest.TestCase):
    def test_fetch_reconstructs_deltas_paginates_and_keeps_receipts(self) -> None:
        start = BASE
        end = BASE + timedelta(hours=1)
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        calls: list[WireMessage] = []
        first_receipt = BASE + timedelta(hours=2)
        second_receipt = BASE + timedelta(hours=2, seconds=1)

        def handler(request: WireMessage) -> WireMessage:
            calls.append(request)
            if len(calls) == 1:
                self.assertEqual(request.payload_type_id, PAYLOAD["PROTO_OA_GET_TICKDATA_REQ"])
                self.assertEqual(request.payload["type"], 1)
                return WireMessage(
                    "PROTO_OA_GET_TICKDATA_RES",
                    {
                        "ctidTraderAccountId": 7,
                        "tickData": [
                            {"timestamp": end_ms, "tick": 115430},
                            {"timestamp": -250, "tick": 1},
                            {"timestamp": -250, "tick": 1},
                        ],
                        "hasMore": True,
                    },
                    request.client_msg_id,
                    received_at=first_receipt,
                    available_at=first_receipt,
                    source_identity="tick-page-1",
                )
            self.assertEqual(request.payload["toTimestamp"], end_ms - 500 - 1)
            return WireMessage(
                "PROTO_OA_GET_TICKDATA_RES",
                {
                    "ctidTraderAccountId": 7,
                    "tickData": [{"timestamp": start_ms, "tick": 115400}],
                    "hasMore": False,
                },
                request.client_msg_id,
                received_at=second_receipt,
                available_at=second_receipt,
                source_identity="tick-page-2",
            )

        provider, client, _transport = _provider(handler)
        try:
            result = provider.fetch_tick_data("BID", from_timestamp=start, to_timestamp=end)
        finally:
            client.close()

        self.assertTrue(result.complete)
        self.assertFalse(result.has_more)
        self.assertEqual(result.pages, 2)
        self.assertEqual(len(calls), 2)
        self.assertEqual([item.quote_type for item in result.ticks], ["BID"] * 4)
        self.assertEqual(
            [item.event_time for item in result.ticks],
            [start, end - timedelta(milliseconds=500), end - timedelta(milliseconds=250), end],
        )
        self.assertEqual(
            [item.price for item in result.ticks],
            [Decimal("1.154"), Decimal("1.15432"), Decimal("1.15431"), Decimal("1.1543")],
        )
        page_zero = {item.ordinal: item for item in result.ticks if item.page == 0}
        self.assertEqual([page_zero[index].absolute_raw_tick for index in range(3)], [115430, 115431, 115432])
        self.assertEqual([page_zero[index].raw_tick for index in range(3)], [115430, 1, 1])
        self.assertEqual([page_zero[index].raw_tick_delta for index in range(3)], [None, 1, 1])
        self.assertTrue(all(item.native and item.price_basis == "bid" for item in result.ticks))
        self.assertTrue(all(item.metadata["pair_constructed"] is False for item in result.ticks))
        self.assertTrue(all("bid" not in item.to_dict() and "ask" not in item.to_dict() for item in result.ticks))
        self.assertEqual(result.raw_pages[0]["tickData"][1]["timestamp"], -250)
        self.assertEqual(result.page_metadata[0]["source_identity"], "tick-page-1")
        self.assertEqual(result.page_metadata[1]["source_identity"], "tick-page-2")
        self.assertEqual(result.page_metadata[0]["received_at"], first_receipt)
        self.assertEqual(result.to_dict()["timestamp_encoding"], "first_absolute_ms_then_deltas_ms")
        self.assertEqual(result.to_dict()["price_encoding"], "first_absolute_relative_then_deltas")
        self.assertNotIn("ticks", result.to_dict())
        self.assertEqual(len(result.to_dict(include_ticks=True)["ticks"]), 4)

    def test_ask_type_is_explicit_and_never_joined_to_bid(self) -> None:
        seen_types: list[int] = []

        def handler(request: WireMessage) -> WireMessage:
            seen_types.append(request.payload["type"])
            return WireMessage(
                "PROTO_OA_GET_TICKDATA_RES",
                {
                    "ctidTraderAccountId": 7,
                    "tickData": [{"timestamp": int(BASE.timestamp() * 1000), "tick": 110020}],
                    "hasMore": False,
                },
                request.client_msg_id,
                received_at=BASE + timedelta(hours=1),
                available_at=BASE + timedelta(hours=1),
            )

        provider, client, _transport = _provider(handler)
        try:
            result = provider.fetch_ticks("ask", from_timestamp=BASE, to_timestamp=BASE + timedelta(minutes=1))
        finally:
            client.close()
        self.assertEqual(seen_types, [2])
        self.assertEqual(result.quote_type, "ASK")
        self.assertEqual(result.ticks[0].price, Decimal("1.1002"))
        self.assertEqual(result.ticks[0].price_basis, "ask")

    def test_window_is_explicit_and_capped_at_one_week_before_transport(self) -> None:
        provider, client, transport = _provider(lambda _request: None)
        try:
            with self.assertRaises(CTraderConfigurationError):
                provider.fetch_tick_data("BID", from_timestamp=BASE, to_timestamp=BASE + timedelta(days=8))
            with self.assertRaises(CTraderConfigurationError):
                provider.fetch_tick_data("BID", from_timestamp=None, to_timestamp=BASE)
            with self.assertRaises(CTraderConfigurationError):
                provider.fetch_tick_data(
                    "BID",
                    from_timestamp=BASE + timedelta(seconds=1),
                    to_timestamp=BASE,
                )
            self.assertEqual(transport.sent, [])
        finally:
            client.close()

    def test_has_more_without_temporal_progress_is_incomplete(self) -> None:
        end = BASE + timedelta(minutes=1)

        def handler(request: WireMessage) -> WireMessage:
            return WireMessage(
                "PROTO_OA_GET_TICKDATA_RES",
                {
                    "ctidTraderAccountId": 7,
                    "tickData": [{"timestamp": int(end.timestamp() * 1000), "tick": 110020}],
                    "hasMore": True,
                },
                request.client_msg_id,
                received_at=BASE + timedelta(hours=1),
                available_at=BASE + timedelta(hours=1),
            )

        provider, client, _transport = _provider(handler)
        try:
            result = provider.fetch_tick_data("BID", from_timestamp=BASE, to_timestamp=end)
        finally:
            client.close()
        self.assertFalse(result.complete)
        self.assertTrue(result.has_more)
        self.assertIn("sin progreso temporal", " ".join(result.issues))

    def test_invalid_newest_first_encoding_is_not_relabelled(self) -> None:
        def handler(request: WireMessage) -> WireMessage:
            return WireMessage(
                "PROTO_OA_GET_TICKDATA_RES",
                {
                    "ctidTraderAccountId": 7,
                    "tickData": [
                        {"timestamp": int((BASE + timedelta(minutes=1)).timestamp() * 1000), "tick": 110020},
                        {"timestamp": 250, "tick": 110010},
                    ],
                    "hasMore": False,
                },
                request.client_msg_id,
                received_at=BASE + timedelta(hours=1),
                available_at=BASE + timedelta(hours=1),
            )

        provider, client, _transport = _provider(handler)
        try:
            result = provider.fetch_tick_data("BID", from_timestamp=BASE, to_timestamp=BASE + timedelta(minutes=2))
        finally:
            client.close()
        self.assertFalse(result.complete)
        self.assertEqual(len(result.ticks), 1)
        self.assertIn("newest-first", " ".join(result.issues))

    def test_malformed_response_account_is_rejected_without_ticks(self) -> None:
        def handler(request: WireMessage) -> WireMessage:
            return WireMessage(
                "PROTO_OA_GET_TICKDATA_RES",
                {"ctidTraderAccountId": 8, "tickData": [], "hasMore": False},
                request.client_msg_id,
                received_at=BASE + timedelta(hours=1),
                available_at=BASE + timedelta(hours=1),
            )

        provider, client, _transport = _provider(handler)
        try:
            result = provider.fetch_tick_data("BID", from_timestamp=BASE, to_timestamp=BASE + timedelta(minutes=1))
        finally:
            client.close()
        self.assertFalse(result.complete)
        self.assertEqual(result.ticks, ())
        self.assertTrue(any("account_id" in issue for issue in result.issues))


class CTraderTickRecordTests(unittest.TestCase):
    def test_tick_rejects_receipt_before_market_time(self) -> None:
        from mtf_lab.data.ctrader import CTraderTick

        with self.assertRaises(CTraderDataError):
            CTraderTick(
                symbol="EUR/USD",
                symbol_id=99,
                quote_type="BID",
                event_time=BASE,
                price=Decimal("1.1"),
                raw_tick=110000,
                received_at=BASE - timedelta(milliseconds=1),
                available_at=BASE,
            )


if __name__ == "__main__":
    unittest.main()
