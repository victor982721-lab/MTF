"""Focal regressions for real cTrader timing/quote diagnostics."""

from __future__ import annotations

import unittest
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from mtf_lab.data.ctrader import (
    CTraderConfig,
    CTraderDataError,
    CTraderInstrumentSpec,
    CTraderProvider,
    DeterministicTransport,
    QuoteQualityReason,
    QuoteQualityState,
    build_spot_diagnostic,
    normalize_spot_event,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


class CTraderRootCauseTests(unittest.TestCase):
    def _payload(self, when: datetime, *, bid: int | None = None, ask: int | None = None) -> dict[str, object]:
        payload: dict[str, object] = {
            "symbolId": 99,
            "timestamp": int(when.timestamp() * 1000),
            "timestamp_unit": "ms",
        }
        if bid is not None:
            payload["bid"] = bid
        if ask is not None:
            payload["ask"] = ask
        return payload

    def test_equal_and_greater_raw_quotes_are_one_crossed_class(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99, digits=5, price_scale=100_000)
        for bid, ask in ((110000, 110000), (110001, 110000)):
            diagnostic = build_spot_diagnostic(
                event_time=BASE,
                timestamp_original=int(BASE.timestamp() * 1000),
                timestamp_unit="ms",
                received_at=BASE,
                available_at=BASE,
                bid_raw=bid,
                ask_raw=ask,
                price_scale=spec.price_scale,
                digits=spec.digits,
                fields_present=("timestamp", "bid", "ask"),
            )
            self.assertEqual(diagnostic.bid_ask_relation, "CROSSED")
            result = normalize_spot_event(
                self._payload(BASE, bid=bid, ask=ask),
                spec=spec,
                received_at=BASE,
            )
            event = result.quote_events[0]
            self.assertEqual(event.metadata["spot_diagnostic"]["bid_ask_relation"], "CROSSED")
            self.assertIn(QuoteQualityReason.CROSSED.value, event.metadata["quality_reasons"])
            self.assertEqual(event.metadata["quality_state"], QuoteQualityState.INVALID.value)
            self.assertFalse(event.metadata["quote_usable"])

    def test_partial_update_diagnostic_keeps_wire_presence_and_updated_side(self) -> None:
        provider = CTraderProvider(
            CTraderConfig(symbol_id=99, quote_basis="mid"),
            transport=DeterministicTransport(),
        )
        first = provider.normalize_spot(
            self._payload(BASE, bid=110000, ask=110020),
            received_at=BASE,
            sequence=1,
        )
        second = provider.normalize_spot(
            self._payload(BASE + timedelta(seconds=1), bid=110010),
            received_at=BASE + timedelta(seconds=1),
            sequence=2,
        )
        self.assertEqual(len(first.quote_events), 1)
        event = second.quote_events[0]
        diagnostic = event.metadata["spot_diagnostic"]
        self.assertEqual(diagnostic["bid_raw"], 110010)
        self.assertIsNone(diagnostic["ask_raw"])
        self.assertEqual(diagnostic["fields_present"], ["timestamp", "bid"])
        self.assertEqual(diagnostic["updated_sides"], ["bid"])
        self.assertTrue(diagnostic["partial_update"])
        self.assertTrue(event.metadata["partial_update"])
        self.assertEqual(event.metadata["ask_source_timestamp"], "2026-01-01T00:00:00Z")

    def test_future_availability_is_rejected_with_minimal_unchanged_diagnostic(self) -> None:
        event_time = BASE + timedelta(seconds=2)
        received = BASE
        available = BASE + timedelta(seconds=1)
        payload = self._payload(event_time, bid=110000, ask=110020)
        with self.assertRaises(CTraderDataError) as caught:
            normalize_spot_event(
                payload,
                spec=CTraderInstrumentSpec(symbol_id=99, digits=5, price_scale=100_000),
                received_at=received,
                available_at=available,
            )
        error = caught.exception
        self.assertIsInstance(error.diagnostic, Mapping)
        diagnostic = error.diagnostic
        assert isinstance(diagnostic, Mapping)
        self.assertEqual(diagnostic["timestamp_original"], payload["timestamp"])
        self.assertEqual(diagnostic["timestamp_unit"], "ms")
        self.assertEqual(diagnostic["event_time"], "2026-01-01T00:00:02.000000Z")
        self.assertEqual(diagnostic["received_at"], "2026-01-01T00:00:00.000000Z")
        self.assertEqual(diagnostic["available_at"], "2026-01-01T00:00:01.000000Z")
        self.assertEqual(diagnostic["bid_raw"], 110000)
        self.assertEqual(diagnostic["ask_raw"], 110020)
        self.assertEqual(diagnostic["price_scale"], 100_000)
        self.assertEqual(diagnostic["digits"], 5)
        self.assertEqual(diagnostic["fields_present"], ["timestamp", "bid", "ask"])
        self.assertEqual(diagnostic["updated_sides"], ["bid", "ask"])
        self.assertFalse(diagnostic["partial_update"])
        self.assertEqual(diagnostic["bid_ask_relation"], "ORDERED")
        self.assertEqual(diagnostic["timing_status"], "AVAILABLE_BEFORE_EVENT")
        self.assertEqual(diagnostic["timing_attribution"], "UNRESOLVED_SERVER_OR_LOCAL_CLOCK")
        self.assertEqual(diagnostic["available_minus_event_seconds"], -1.0)
        self.assertEqual(payload["timestamp"], int(event_time.timestamp() * 1000))

    def test_cli_diagnostic_projection_drops_unallowlisted_fields(self) -> None:
        from mtf_lab.ops.ctrader_watch_cli import _safe_spot_diagnostic

        error = CTraderDataError(
            "diagnostic",
            diagnostic={
                "event_time": "2026-01-01T00:00:00.000000Z",
                "bid_raw": 110000,
                "access_token": "must-not-leak",
            },
        )
        self.assertEqual(
            _safe_spot_diagnostic(error),
            {"event_time": "2026-01-01T00:00:00.000000Z", "bid_raw": 110000},
        )

    def test_ordered_quotes_have_no_cross_and_diagnostic_keeps_raw_relation(self) -> None:
        result = normalize_spot_event(
            self._payload(BASE, bid=110000, ask=110020),
            spec=CTraderInstrumentSpec(symbol_id=99),
            received_at=BASE,
        )
        event = result.quote_events[0]
        diagnostic = event.metadata["spot_diagnostic"]
        self.assertEqual(diagnostic["bid_ask_relation"], "ORDERED")
        self.assertEqual(diagnostic["price_scale"], 100_000)
        self.assertEqual(diagnostic["digits"], 5)
        self.assertFalse(diagnostic["partial_update"])
        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.VALID.value)


if __name__ == "__main__":
    unittest.main()
