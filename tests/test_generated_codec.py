"""Focused contracts for the pinned Spotware-generated cTrader codec."""

from __future__ import annotations

import os
import unittest
from importlib.resources import files
from typing import ClassVar

from mtf_lab.data.ctrader_errors import CTraderProtocolError
from mtf_lab.data.ctrader_protocol import (
    MESSAGE_PAYLOAD_TYPES,
    PAYLOAD,
    SdkProtobufCodec,
    WireMessage,
    dependency_report,
    field_present,
    message_payload_type,
    message_to_mapping,
    read_field,
    read_repeated,
)
from mtf_lab.data.protobuf_generated import (
    OpenApiCommonMessages_pb2 as common,
)
from mtf_lab.data.protobuf_generated import (
    OpenApiMessages_pb2 as messages,
)
from mtf_lab.data.protobuf_generated import (
    OpenApiModelMessages_pb2 as models,
)


class GeneratedCodecTests(unittest.TestCase):
    codec: ClassVar[SdkProtobufCodec]

    @classmethod
    def setUpClass(cls) -> None:
        cls.codec = SdkProtobufCodec()

    def test_dependency_report_identifies_pinned_local_codec(self) -> None:
        report = dependency_report()
        self.assertTrue(report.available)
        self.assertTrue(report.codec_operational)
        self.assertEqual(report.codec_backend, "bundled_official_generated")
        self.assertEqual(report.protobuf_version, "7.36.1")
        expected_implementation = os.environ.get("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "upb")
        self.assertEqual(report.protobuf_implementation, expected_implementation)
        self.assertEqual(report.schema_revision, "openapi-proto-messages@91:017413087c1c23c1866bbf07ff24d56574047253")
        self.assertEqual(report.security_state, "PATCH_REQUIREMENT_SATISFIED")

    def test_public_generated_modules_expose_stable_message_names(self) -> None:
        self.assertTrue(hasattr(common, "ProtoMessage"))
        self.assertTrue(hasattr(common, "ProtoHeartbeatEvent"))
        self.assertTrue(hasattr(messages, "ProtoOASpotEvent"))
        self.assertTrue(hasattr(messages, "ProtoOANewOrderReq"))
        self.assertTrue(hasattr(messages, "ProtoOAReconcileReq"))
        self.assertTrue(hasattr(models, "ProtoOATrendbar"))

    def test_runtime_resources_carry_license_and_provenance(self) -> None:
        license_text = (
            files("mtf_lab.resources")
            .joinpath("licenses/spotware-openapi-proto-messages-MIT.txt")
            .read_text(encoding="utf-8")
        )
        provenance = (
            files("mtf_lab.resources").joinpath("provenance/ctrader-protobuf-91.json").read_text(encoding="utf-8")
        )
        self.assertIn("MIT License", license_text)
        self.assertIn("017413087c1c23c1866bbf07ff24d56574047253", provenance)

    def test_heartbeat_and_mapping_requests_roundtrip(self) -> None:
        heartbeat = self.codec.decode(self.codec.encode(WireMessage("PROTO_HEARTBEAT_EVENT")))
        self.assertEqual(heartbeat.payload_type_id, PAYLOAD["PROTO_HEARTBEAT_EVENT"])
        self.assertTrue(heartbeat.is_event)
        self.assertIsNone(heartbeat.payload)

        request = WireMessage(
            "PROTO_OA_SYMBOLS_LIST_REQ",
            {"ctidTraderAccountId": 7, "includeArchivedSymbols": False},
            "request-1",
        )
        decoded = self.codec.decode(self.codec.encode(request))
        self.assertEqual(decoded.payload_type_id, PAYLOAD["PROTO_OA_SYMBOLS_LIST_REQ"])
        self.assertEqual(decoded.client_msg_id, "request-1")
        self.assertEqual(type(decoded.payload).__name__, "ProtoOASymbolsListReq")
        self.assertTrue(field_present(decoded.payload, "includeArchivedSymbols"))
        self.assertFalse(read_field(decoded.payload, "includeArchivedSymbols", default=True))

    def test_representative_request_and_response_messages_roundtrip(self) -> None:
        order = messages.ProtoOANewOrderReq(
            ctidTraderAccountId=7,
            symbolId=11,
            orderType="MARKET",
            tradeSide="BUY",
            volume=100,
        )
        spot = messages.ProtoOASpotEvent(ctidTraderAccountId=7, symbolId=11)
        spot.bid = 0
        spot.timestamp = 0
        reconcile = messages.ProtoOAReconcileReq(ctidTraderAccountId=7)
        for payload_type, payload in (
            (MESSAGE_PAYLOAD_TYPES["ProtoOANewOrderReq"], order),
            (PAYLOAD["PROTO_OA_SPOT_EVENT"], spot),
            (MESSAGE_PAYLOAD_TYPES["ProtoOAReconcileReq"], reconcile),
        ):
            decoded = self.codec.decode(self.codec.encode(WireMessage(payload_type, payload, "typed")))
            self.assertEqual(decoded.payload_type_id, payload_type)
            self.assertEqual(decoded.client_msg_id, "typed")
            self.assertEqual(message_payload_type(decoded.payload), payload_type)

        spot_decoded = self.codec.decode(self.codec.encode(WireMessage(PAYLOAD["PROTO_OA_SPOT_EVENT"], spot, "typed")))
        self.assertTrue(field_present(spot_decoded.payload, "bid"))
        self.assertEqual(read_field(spot_decoded.payload, "bid", default=None), 0)
        self.assertTrue(field_present(spot_decoded.payload, "timestamp"))
        self.assertEqual(read_field(spot_decoded.payload, "timestamp", default=None), 0)

    def test_proto2_presence_does_not_materialize_defaults(self) -> None:
        spot = messages.ProtoOASpotEvent(ctidTraderAccountId=7, symbolId=11)
        self.assertFalse(field_present(spot, "bid"))
        self.assertIsNone(read_field(spot, "bid", default=None))
        self.assertTrue(field_present(spot, "trendbar"))
        self.assertEqual(read_repeated(spot, "trendbar"), ())

        spot.bid = 0
        self.assertTrue(field_present(spot, "bid"))
        self.assertEqual(read_field(spot, "bid", default=None), 0)
        spot.ClearField("bid")
        self.assertFalse(field_present(spot, "bid"))
        self.assertIsNone(read_field(spot, "bid", default=None))

        bar = spot.trendbar.add(volume=0)
        bar.low = 0
        bar.deltaOpen = 0
        bar.deltaClose = 0
        bar.deltaHigh = 0
        bar.utcTimestampInMinutes = 0
        decoded = self.codec.decode(self.codec.encode(WireMessage(PAYLOAD["PROTO_OA_SPOT_EVENT"], spot)))
        self.assertEqual(len(read_repeated(decoded.payload, "trendbar")), 1)
        self.assertTrue(field_present(decoded.payload.trendbar[0], "low"))
        self.assertEqual(read_field(decoded.payload.trendbar[0], "low", default=None), 0)

    def test_unknown_fields_survive_parse_and_reserialize(self) -> None:
        spot = messages.ProtoOASpotEvent(ctidTraderAccountId=7, symbolId=11)
        body = spot.SerializeToString() + b"\x88\x06\x01"  # unknown field 81, varint 1
        envelope = common.ProtoMessage(payloadType=PAYLOAD["PROTO_OA_SPOT_EVENT"], payload=body)
        decoded = self.codec.decode(envelope.SerializeToString())
        self.assertIn(b"\x88\x06\x01", decoded.payload.SerializeToString())
        self.assertEqual(message_to_mapping(decoded.payload)["symbolId"], 11)

    def test_payload_type_mismatch_and_unknown_payload_fail_closed(self) -> None:
        order = messages.ProtoOANewOrderReq(
            ctidTraderAccountId=7,
            symbolId=11,
            orderType="MARKET",
            tradeSide="BUY",
            volume=100,
        )
        with self.assertRaises(CTraderProtocolError):
            self.codec.encode(WireMessage(PAYLOAD["PROTO_OA_SPOT_EVENT"], order))
        with self.assertRaises(CTraderProtocolError):
            self.codec.encode(WireMessage(999999, {}))

        unknown = common.ProtoMessage(payloadType=999999, payload=b"\x00")
        with self.assertRaises(CTraderProtocolError):
            self.codec.decode(unknown.SerializeToString())

    def test_missing_payload_type_and_oversized_frames_fail_closed(self) -> None:
        with self.assertRaises(CTraderProtocolError):
            self.codec.decode(b"")
        with self.assertRaises(CTraderProtocolError):
            self.codec.decode(b"\x00" * 15_000_001)


if __name__ == "__main__":
    unittest.main()
