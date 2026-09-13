"""Focused causal-boundary tests for the CFD PAPER state machine."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.core.cfd_quality import QuoteReason
from mtf_lab.core.cfd_simulation import (
    CFDConfig,
    CFDQuote,
    CFDSignal,
    CFDSimulator,
    Direction,
    TradeState,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def quote(
    instrument: str,
    second: int,
    bid: str | None,
    ask: str | None,
    quote_id: str,
    **kwargs: object,
) -> CFDQuote:
    when = BASE + timedelta(seconds=second)
    return CFDQuote(
        instrument,
        when,
        Decimal(bid) if bid is not None else None,
        Decimal(ask) if ask is not None else None,
        quote_id,
        **kwargs,
    )


def strict_pair(
    second: int,
    quote_id: str,
    *,
    bid: str | None = "1.1000",
    ask: str | None = "1.1002",
    **kwargs: object,
) -> CFDQuote:
    when = BASE + timedelta(seconds=second)
    return quote(
        "EUR/USD",
        second,
        bid,
        ask,
        quote_id,
        available_at=when,
        bid_source_timestamp=when,
        ask_source_timestamp=when,
        bid_timestamp_known=True,
        ask_timestamp_known=True,
        **kwargs,
    )


class CFDCausalityV2Tests(unittest.TestCase):
    def test_wrong_instrument_does_not_touch_book_clock_or_dedup(self) -> None:
        simulator = CFDSimulator(CFDConfig(instrument="EUR/USD", horizons_seconds=("60",), max_quote_age_seconds="60"))
        good = strict_pair(0, "eur-0", sequence=1)
        simulator.on_quote(good)
        before = simulator.snapshot()
        wrong = strict_pair(1, "wrong-1", sequence=2)
        wrong = CFDQuote(
            "GBP/USD",
            wrong.market_time,
            wrong.bid,
            wrong.ask,
            wrong.quote_id,
            available_at=wrong.available_at,
            sequence=wrong.sequence,
            bid_source_timestamp=wrong.bid_source_timestamp,
            ask_source_timestamp=wrong.ask_source_timestamp,
            bid_timestamp_known=True,
            ask_timestamp_known=True,
        )
        self.assertEqual(simulator.on_quote(wrong), ())
        after = simulator.snapshot()
        self.assertEqual(after["last_watermark"], before["last_watermark"])
        self.assertEqual(after["last_sequence"], before["last_sequence"])
        self.assertEqual(after["quote_book"], before["quote_book"])
        self.assertEqual(after["seen_quote_ids"], before["seen_quote_ids"])
        self.assertEqual(after["counters"]["quotes_instrument_mismatch"], 1)
        self.assertNotIn("wrong-1", after["seen_quote_ids"])

    def test_side_availability_is_promoted_and_blocks_early_assessment(self) -> None:
        when = BASE
        available = BASE + timedelta(seconds=10)
        quote_with_late_ask = quote(
            "EUR/USD",
            0,
            "1.1000",
            "1.1002",
            "late-ask",
            bid_source_timestamp=when,
            ask_source_timestamp=when,
            bid_timestamp_known=True,
            ask_timestamp_known=True,
            bid_available_at=when,
            ask_available_at=available,
        )
        self.assertEqual(quote_with_late_ask.available_ts, available)
        assessment = quote_with_late_ask.assessment_for("ask", at=when, max_age_seconds=60)
        self.assertFalse(assessment.usable)
        self.assertIn(QuoteReason.FUTURE_AVAILABILITY, assessment.reasons)

        simulator = CFDSimulator(CFDConfig(instrument="EUR/USD", horizons_seconds=("60",), max_quote_age_seconds="60"))
        simulator.submit(CFDSignal("late-ask-signal", "EUR/USD", Direction.LONG, BASE))
        changed = simulator.on_quote(quote_with_late_ask)
        self.assertEqual(changed[0].state, TradeState.FILLED)
        self.assertEqual(changed[0].entry_available_at, available)

        missing_source = quote(
            "EUR/USD",
            0,
            "1.1000",
            "1.1002",
            "late-ask-no-source",
            ask_available_at=available,
        )
        self.assertFalse(missing_source.operable_for("ask", at=available, max_age_seconds=60))
        self.assertIn(QuoteReason.MISSING_SOURCE_TIMESTAMP, missing_source.assessment_for("ask", at=available).reasons)

    def test_global_and_side_quality_flags_are_not_ignored(self) -> None:
        stale_side = strict_pair(
            0,
            "stale-side",
            bid_quality={"status": "VALID", "flags": ["STALE"]},
            ask_quality={"status": "VALID", "reasons": ["STALE"]},
        )
        bid = stale_side.assessment_for("bid", max_age_seconds=60)
        ask = stale_side.assessment_for("ask", max_age_seconds=60)
        self.assertFalse(bid.usable)
        self.assertFalse(ask.usable)
        self.assertIn(QuoteReason.STALE, bid.reasons)
        self.assertIn(QuoteReason.STALE, ask.reasons)

        invalid_common = strict_pair(
            0,
            "invalid-common",
            quality="INVALID",
            bid_quality="VALID",
            ask_quality="VALID",
        )
        self.assertFalse(invalid_common.operable_for("bid", max_age_seconds=60))
        self.assertFalse(invalid_common.operable_for("ask", max_age_seconds=60))

    def test_replay_interleaved_preserves_signal_quote_order(self) -> None:
        signal = CFDSignal("interleaved", "EUR/USD", Direction.LONG, BASE)
        records = (
            signal,
            strict_pair(0, "entry"),
            strict_pair(60, "close", bid="1.1005", ask="1.1007"),
        )
        direct = CFDSimulator(CFDConfig(instrument="EUR/USD", horizons_seconds=("60",))).replay(records)
        explicit = CFDSimulator(CFDConfig(instrument="EUR/USD", horizons_seconds=("60",))).replay_interleaved(records)
        keyword = CFDSimulator(CFDConfig(instrument="EUR/USD", horizons_seconds=("60",))).replay(records=records)
        self.assertEqual(direct.trades[0].state, TradeState.CLOSED)
        self.assertEqual(direct.trades[0].to_dict(), explicit.trades[0].to_dict())
        self.assertEqual(direct.trades[0].to_dict(), keyword.trades[0].to_dict())

        late_signal = CFDSimulator(
            CFDConfig(instrument="EUR/USD", horizons_seconds=("60",), max_quote_age_seconds="120")
        ).replay((strict_pair(0, "before-signal"), signal, strict_pair(60, "after-signal")))
        self.assertEqual(late_signal.trades[0].state, TradeState.UNKNOWN)
        self.assertEqual(late_signal.trades[0].entry_quote_id, "after-signal")


if __name__ == "__main__":
    unittest.main()
