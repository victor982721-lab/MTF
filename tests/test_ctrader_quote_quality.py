"""Regresiones de calidad causal bid/ask del adaptador cTrader."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest

from mtf_lab.data.ctrader import (
    CTraderConfig,
    CTraderInstrumentSpec,
    CTraderProvider,
    DeterministicTransport,
    QuoteQualityReason,
    QuoteQualityState,
    normalize_spot_event,
)


UTC_BASE = datetime(2026, 1, 1, tzinfo=UTC)


class CTraderQuoteQualityTests(unittest.TestCase):
    def _provider(self, *, max_age: float = 60.0) -> CTraderProvider:
        return CTraderProvider(
            CTraderConfig(symbol_id=99, quote_basis="mid"),
            transport=DeterministicTransport(),
            max_quote_age_seconds=max_age,
        )

    def _payload(self, when: datetime, *, bid: int | None = None, ask: int | None = None) -> dict:
        payload = {
            "symbolId": 99,
            "timestamp": int(when.timestamp() * 1000),
        }
        if bid is not None:
            payload["bid"] = bid
        if ask is not None:
            payload["ask"] = ask
        return payload

    def test_provider_keeps_embedded_native_trendbars_when_composing_quote(self) -> None:
        from mtf_lab.data.ctrader import synthetic_trendbar

        provider = self._provider()
        payload = self._payload(UTC_BASE, bid=110000, ask=110020)
        payload["trendbar"] = [synthetic_trendbar(timestamp_minutes=int(UTC_BASE.timestamp() // 60))]
        result = provider.normalize_spot(payload, received_at=UTC_BASE)
        self.assertEqual(len(result.quote_events), 1)
        self.assertEqual(len(result.bars), 1)
        self.assertEqual(result.bars[0].price_basis, "native")

    def test_old_ask_is_not_rejuvenated_by_new_bid(self) -> None:
        provider = self._provider(max_age=60.0)
        first = provider.normalize_spot(
            self._payload(UTC_BASE, bid=110000, ask=110020),
            received_at=UTC_BASE,
            sequence=1,
        )
        second_time = UTC_BASE + timedelta(seconds=180)
        second = provider.normalize_spot(
            self._payload(second_time, bid=110010),
            received_at=second_time,
            sequence=2,
        )

        self.assertEqual(len(first.quote_events), 1)
        self.assertEqual(len(second.quote_events), 1)
        event = second.quote_events[0]
        self.assertEqual((event.bid, event.ask), (1.1001, 1.1002))
        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.STALE.value)
        self.assertIn(QuoteQualityReason.STALE_ASK.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["quote_usable"])
        self.assertEqual(event.metadata["ask_source_timestamp"], "2026-01-01T00:00:00Z")

    def test_old_bid_is_not_rejuvenated_by_new_ask(self) -> None:
        provider = self._provider(max_age=60.0)
        provider.normalize_spot(
            self._payload(UTC_BASE, bid=110000, ask=110020),
            received_at=UTC_BASE,
            sequence=1,
        )
        second_time = UTC_BASE + timedelta(seconds=180)
        result = provider.normalize_spot(
            self._payload(second_time, ask=110030),
            received_at=second_time,
            sequence=2,
        )
        event = result.quote_events[0]
        self.assertEqual((event.bid, event.ask), (1.1, 1.1003))
        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.STALE.value)
        self.assertIn(QuoteQualityReason.STALE_BID.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["quote_usable"])

    def test_rejected_both_sides_cannot_bypass_stateful_book(self) -> None:
        provider = self._provider(max_age=600.0)
        provider.normalize_spot(
            self._payload(UTC_BASE + timedelta(seconds=100), bid=110000, ask=110020),
            received_at=UTC_BASE + timedelta(seconds=100),
            sequence=10,
        )
        # Both values are newer in the wire frame but their source timestamp
        # is older than the already accepted book.  The old implementation
        # used the stateless has_both=True result and leaked these prices.
        result = provider.normalize_spot(
            self._payload(UTC_BASE + timedelta(seconds=90), bid=110030, ask=110040),
            received_at=UTC_BASE + timedelta(seconds=101),
            sequence=11,
        )
        self.assertEqual(len(result.quote_events), 1)
        event = result.quote_events[0]
        self.assertEqual((event.bid, event.ask), (1.1, 1.1002))
        self.assertIn(QuoteQualityReason.OUT_OF_ORDER.value, event.metadata["quality_reasons"])
        self.assertIn(QuoteQualityReason.REJECTED_UPDATE.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["quote_usable"])

    def test_missing_timestamp_never_invents_receipt_or_freshness(self) -> None:
        spec = CTraderInstrumentSpec(symbol_id=99)
        with self.assertRaises(ValueError):
            normalize_spot_event(
                {"symbolId": 99, "bid": 110000, "ask": 110020},
                spec=spec,
                received_at=None,
            )
        # A pure normalizer cannot invent a receipt.  With no source timestamp
        # and no receipt it rejects the frame instead of assigning datetime.now.

        observed = normalize_spot_event(
            {"symbolId": 99, "bid": 110000, "ask": 110020},
            spec=spec,
            received_at=UTC_BASE,
        )
        event = observed.quote_events[0]
        self.assertEqual(event.available_at, UTC_BASE)
        self.assertEqual(event.metadata["available_at_policy"], "OBSERVED_RECEIPT")
        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.UNKNOWN.value)
        self.assertIn(QuoteQualityReason.MISSING_SOURCE_TIMESTAMP.value, event.metadata["quality_reasons"])
        self.assertIsNone(event.metadata["quote_quality"]["bid"]["source_timestamp"])
        self.assertIsNone(event.metadata["quote_quality"]["bid"]["age_seconds"])

    def test_crossed_quote_is_observed_but_not_operable(self) -> None:
        result = normalize_spot_event(
            {"symbolId": 99, "timestamp": int(UTC_BASE.timestamp() * 1000), "bid": 110020, "ask": 110000},
            spec=CTraderInstrumentSpec(symbol_id=99),
            received_at=UTC_BASE,
        )
        self.assertEqual(len(result.quote_events), 1)
        event = result.quote_events[0]
        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.INVALID.value)
        self.assertIn(QuoteQualityReason.CROSSED.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["quote_usable"])

    def test_partial_quote_basis_does_not_fabricate_missing_leg(self) -> None:
        result = normalize_spot_event(
            {"symbolId": 99, "timestamp": int(UTC_BASE.timestamp() * 1000), "bid": 110000},
            spec=CTraderInstrumentSpec(symbol_id=99),
            quote_basis="bid",
            received_at=UTC_BASE,
        )
        self.assertEqual(len(result.quote_events), 1)
        event = result.quote_events[0]
        self.assertEqual(event.bid, 1.1)
        self.assertIsNone(event.ask)
        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.INCOMPLETE.value)
        self.assertIn(QuoteQualityReason.MISSING_ASK.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["quote_usable"])

    def test_generation_reset_cannot_carry_old_side_into_new_session(self) -> None:
        provider = self._provider()
        provider.normalize_spot(
            self._payload(UTC_BASE, bid=110000, ask=110020),
            received_at=UTC_BASE,
            generation=1,
        )
        provider.reset_generation(2)
        result = provider.normalize_spot(
            self._payload(UTC_BASE + timedelta(seconds=1), bid=110010),
            received_at=UTC_BASE + timedelta(seconds=1),
            generation=2,
        )
        self.assertEqual(result.quote_events, ())
        snapshot = provider.snapshot_quote_state()
        self.assertEqual(snapshot["generation"], 2)
        self.assertEqual(set(snapshot["symbols"]["99"]), {"bid"})

    def test_quote_state_snapshot_roundtrip_preserves_leg_ages(self) -> None:
        provider = self._provider()
        provider.normalize_spot(
            self._payload(UTC_BASE, bid=110000, ask=110020),
            received_at=UTC_BASE,
            generation=4,
        )
        snapshot = provider.snapshot_quote_state()
        restored = self._provider()
        restored.restore_quote_state(snapshot)
        result = restored.normalize_spot(
            self._payload(UTC_BASE + timedelta(seconds=30), bid=110010),
            received_at=UTC_BASE + timedelta(seconds=30),
            generation=4,
        )
        event = result.quote_events[0]
        self.assertEqual(event.metadata["ask_source_timestamp"], "2026-01-01T00:00:00Z")
        self.assertEqual(event.metadata["ask_age_seconds"], 30.0)

    def test_timestamp_unit_is_explicit_milliseconds(self) -> None:
        with self.assertRaises(ValueError):
            normalize_spot_event(
                {"symbolId": 99, "timestamp": int(UTC_BASE.timestamp())},
                spec=CTraderInstrumentSpec(symbol_id=99),
                received_at=UTC_BASE,
                timestamp_unit="s",
            )


if __name__ == "__main__":
    unittest.main()
