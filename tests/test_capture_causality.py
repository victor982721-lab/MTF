"""Causal capture regressions, not false invariance to market permutations."""

import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from mtf_lab.core.canonical import canonical_json
from mtf_lab.data.capture import (
    CaptureContractError,
    CaptureEnvelope,
    CaptureIndex,
    MessageClass,
    capture_fingerprint,
    envelope_from_raw,
    iter_capture_order,
    iter_jsonl,
    parse_instant,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def envelope(sequence, event_second, available_second, **payload):
    when = BASE + timedelta(seconds=event_second)
    available = BASE + timedelta(seconds=available_second)
    raw = {"symbolId": 99, "timestamp": int(when.timestamp() * 1000), **payload}
    return CaptureEnvelope(when, available, available, sequence, 0, MessageClass.SPOT, raw)


class CaptureEnvelopeTests(unittest.TestCase):
    def test_physical_shuffle_keeps_durable_availability_order(self):
        rows = [
            envelope(0, 0, 0, ask=110020, bid=110000),
            envelope(1, 180, 180, bid=110005),
            envelope(2, 100, 181, ask=110030),
        ]
        first = tuple(iter_capture_order(rows))
        shuffled = tuple(iter_capture_order(reversed(rows)))
        self.assertEqual([row.to_dict() for row in first], [row.to_dict() for row in shuffled])
        self.assertEqual([row.ingest_sequence for row in first], [0, 1, 2])
        self.assertEqual(
            capture_fingerprint(first, mode="as_observed"), capture_fingerprint(shuffled, mode="as_observed")
        )

    def test_corrected_market_reconstruction_is_a_distinct_identity(self):
        rows = [envelope(0, 180, 180, bid=110005), envelope(1, 100, 181, ask=110030)]
        observed = tuple(iter_capture_order(rows))
        corrected = tuple(iter_capture_order(rows, mode="market_time_corrected"))
        self.assertEqual([row.ingest_sequence for row in observed], [0, 1])
        self.assertEqual([row.ingest_sequence for row in corrected], [1, 0])
        self.assertNotEqual(
            capture_fingerprint(observed, mode="as_observed"),
            capture_fingerprint(corrected, mode="market_time_corrected"),
        )

    def test_identical_legitimate_observations_are_not_payload_deduplicated(self):
        first = envelope(0, 0, 0, bid=110000, ask=110020)
        second = replace(first, ingest_sequence=1)
        rows = tuple(iter_capture_order([second, first, first]))
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0].observation_id, rows[1].observation_id)

    def test_ingest_identity_conflict_is_not_silently_overwritten(self):
        first = envelope(0, 0, 0, bid=110000)
        with self.assertRaises(CaptureContractError):
            tuple(iter_capture_order([first, replace(first, payload={"bid": 110050})]))

    def test_receipt_unknown_is_not_zero_latency(self):
        raw = {"timestamp": 1000, "bid": 110000, "ask": 110020}
        row = envelope_from_raw(raw, 7)
        self.assertIsNone(row.received_at)
        self.assertEqual(row.availability_policy, "historical_event_time")
        self.assertEqual(row.event_time, datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=1))
        with self.assertRaises(CaptureContractError):
            parse_instant(1000)

    def test_reader_metadata_is_not_replaced_by_file_ordinal_or_poll_time(self):
        from mtf_lab.data.ctrader import WireMessage
        from mtf_lab.ops.ctrader_capture import capture_envelopes

        received = BASE + timedelta(seconds=1)
        available = BASE + timedelta(seconds=2)
        wire = WireMessage(
            "PROTO_OA_SPOT_EVENT",
            {"timestamp": int(BASE.timestamp() * 1000), "bid": 110000, "ask": 110020, "symbolId": 99},
            is_event=True,
            received_at=received,
            available_at=available,
            ingest_sequence=48,
            connection_generation=3,
        )
        item = tuple(capture_envelopes([wire]))[0]
        self.assertEqual((item.received_at, item.available_at), (received, available))
        self.assertEqual((item.ingest_sequence, item.connection_generation), (48, 3))

    def test_ingest_sequence_is_global_across_generations(self):
        first = envelope(0, 0, 0, bid=110000, ask=110020)
        next_connection = replace(first, ingest_sequence=1, connection_generation=1)
        self.assertEqual(len(tuple(iter_capture_order([next_connection, first]))), 2)
        with self.assertRaisesRegex(CaptureContractError, "must not reset"):
            tuple(iter_capture_order([first, replace(next_connection, ingest_sequence=0)]))

    def test_declared_end_cannot_hide_later_observations(self):
        end = CaptureEnvelope(BASE, BASE, BASE, 0, 0, MessageClass.END, {"continuity": "CONTINUOUS"})
        with self.assertRaisesRegex(CaptureContractError, "after.*dataset end"):
            tuple(iter_capture_order([envelope(1, 1, 1, bid=110000), end]))

    def test_payload_has_owned_immutable_bytes_and_strict_canonical_values(self):
        payload = {"bid": 110000, "nested": {"value": [1]}}
        row = envelope(0, 0, 0, **payload)
        payload["nested"]["value"].append(2)
        self.assertEqual(row.to_dict()["payload"]["nested"], {"value": [1]})
        with self.assertRaises(TypeError):
            row.payload["bid"] = 7
        with self.assertRaises(TypeError):
            canonical_json(object())

    def test_jsonl_is_lazy_and_index_has_composite_cursor(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "capture.jsonl"
            path.write_text('{"value": 1}\ninvalid-json\n')
            rows = iter_jsonl(path)
            self.assertEqual(next(rows), {"value": 1})
            with self.assertRaisesRegex(CaptureContractError, "line 2"):
                next(rows)
        rows = [envelope(0, 0, 0, bid=1), envelope(1, 0, 0, ask=2)]
        with CaptureIndex(rows) as index:
            cursor = (rows[0].to_dict()["available_at"], 0)
            self.assertEqual([row.ingest_sequence for row in index.iter_after(cursor)], [1])


class NormalizerCausalityTests(unittest.TestCase):
    def test_late_ask_cannot_participate_in_previous_bid_decision(self):
        from mtf_lab.data.ctrader import CTraderInstrumentSpec
        from mtf_lab.ops.ctrader_capture import normalize_ctrader_capture

        rows = [
            envelope(0, 0, 0, ask=110020, bid=110000),
            envelope(1, 180, 180, bid=110005),
            envelope(2, 100, 181, ask=110030),
        ]
        capture = normalize_ctrader_capture(rows, spec=CTraderInstrumentSpec(symbol_id=99))
        previous = next(row for row in capture.quote_events if row.source_sequence == 1)
        self.assertEqual(previous.ask, 1.1002, "ask available at181 must never enter the t180 quote")
        self.assertFalse(capture.is_complete, "absence of issues is not a dataset-end declaration")

    def test_historical_policy_does_not_forge_leg_receipt_or_closed_bar_availability(self):
        from mtf_lab.data.ctrader import CTraderInstrumentSpec, synthetic_trendbar
        from mtf_lab.ops.ctrader_capture import normalize_ctrader_capture

        raw = {
            "symbolId": 99,
            "timestamp": int(BASE.timestamp() * 1000),
            "bid": 110000,
            "ask": 110020,
            "trendbar": [synthetic_trendbar(timestamp_minutes=int(BASE.timestamp() / 60))],
        }
        item = CaptureEnvelope(
            BASE, None, BASE, 0, 0, MessageClass.SPOT, raw, availability_policy="historical_event_time"
        )
        capture = normalize_ctrader_capture([item], spec=CTraderInstrumentSpec(symbol_id=99))
        event = capture.quote_events[0]
        self.assertIsNone(event.received_at)
        self.assertIsNone(event.metadata["quote_quality"]["bid"]["received_at"])
        self.assertNotEqual(event.metadata["quote_quality"]["state"], "VALID")
        self.assertEqual(capture.bars[0].available_at, BASE)
        self.assertFalse(capture.bars[0].closed)

    def test_changing_observed_sequence_can_change_retained_prices(self):
        from mtf_lab.data.ctrader import CTraderInstrumentSpec
        from mtf_lab.ops.ctrader_capture import normalize_ctrader_capture

        rows = [envelope(0, 0, 0, ask=110020, bid=110000), envelope(1, 1, 1, ask=110030), envelope(2, 1, 1, bid=110005)]
        changed = [rows[0], replace(rows[2], ingest_sequence=1), replace(rows[1], ingest_sequence=2)]
        first = normalize_ctrader_capture(rows, spec=CTraderInstrumentSpec(symbol_id=99))
        second = normalize_ctrader_capture(changed, spec=CTraderInstrumentSpec(symbol_id=99))
        self.assertNotEqual(first.capture_hash, second.capture_hash)
        self.assertNotEqual(first.quote_events[1].ask, second.quote_events[1].ask)
