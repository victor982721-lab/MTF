"""Focused parity contracts for the extracted CFD quote boundary."""

from __future__ import annotations

import hashlib
import json
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.core.cfd_quality import QuoteReason
from mtf_lab.core.cfd_quote_contract import (
    CFDQuote as ContractQuote,
)
from mtf_lab.core.cfd_quote_contract import (
    CFDSimulationError as ContractError,
)
from mtf_lab.core.cfd_quote_contract import (
    QuoteAssessment as ContractAssessment,
)
from mtf_lab.core.cfd_simulation import (
    CFDConfig,
    CFDQuote,
    CFDSignal,
    CFDSimulationError,
    CFDSimulator,
    Direction,
    QuoteAssessment,
    TradeState,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def strict_quote(
    second: int,
    bid: str | None,
    ask: str | None,
    quote_id: str,
    *,
    bid_source_second: int | None = None,
    ask_source_second: int | None = None,
    updated_sides: tuple[str, ...] | None = None,
    bid_available_at: datetime | None = None,
    ask_available_at: datetime | None = None,
    generation: int = 1,
) -> CFDQuote:
    when = BASE + timedelta(seconds=second)
    bid_at = BASE + timedelta(seconds=bid_source_second if bid_source_second is not None else second)
    ask_at = BASE + timedelta(seconds=ask_source_second if ask_source_second is not None else second)
    return CFDQuote(
        "EUR/USD",
        when,
        Decimal(bid) if bid is not None else None,
        Decimal(ask) if ask is not None else None,
        quote_id,
        available_at=when,
        updated_sides=updated_sides,
        session_generation=generation,
        bid_source_timestamp=bid_at if bid is not None else None,
        ask_source_timestamp=ask_at if ask is not None else None,
        bid_available_at=bid_available_at,
        ask_available_at=ask_available_at,
        bid_source_timestamp_missing=bid is None,
        ask_source_timestamp_missing=ask is None,
        bid_timestamp_known=bid is not None,
        ask_timestamp_known=ask is not None,
    )


def json_default(value: object) -> object:
    return getattr(value, "value", str(value))


class CFDQuoteContractModuleTests(unittest.TestCase):
    def test_legacy_reexports_preserve_identity_and_assessment_type(self) -> None:
        self.assertIs(CFDQuote, ContractQuote)
        self.assertIs(CFDSimulationError, ContractError)
        self.assertIs(QuoteAssessment, ContractAssessment)
        quote = strict_quote(0, "1.1000", "1.1002", "identity")
        self.assertIsInstance(quote.assessment_for("mid"), ContractAssessment)

    def test_quote_json_bytes_and_mapping_roundtrip_are_stable(self) -> None:
        quote = strict_quote(0, "1.1000", "1.1002", "quote-json", generation=7)
        payload = json.dumps(
            quote.to_dict(),
            default=json_default,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertEqual(
            hashlib.sha256(payload).hexdigest(),
            "a39a5e1104dcf06917b20dcbe7ab8395a91ca1e975bd246f237a2136962ef960",
        )
        restored = ContractQuote.from_mapping(json.loads(payload))
        self.assertEqual(quote.to_dict(), restored.to_dict())

    def test_crossed_partial_future_and_correlated_legs_remain_fail_closed(self) -> None:
        with self.assertRaisesRegex(ContractError, "ask debe ser mayor") as caught:
            ContractQuote(
                "EUR/USD",
                BASE,
                Decimal("1.1002"),
                Decimal("1.1000"),
                "crossed",
                quality="INVALID",
                quote_reasons=(QuoteReason.CROSSED,),
                bid_source_timestamp=BASE,
                ask_source_timestamp=BASE,
                bid_timestamp_known=True,
                ask_timestamp_known=True,
            )
        self.assertEqual(caught.exception.code, "CROSSED_QUOTE")

        late_ask = strict_quote(
            0,
            "1.1000",
            "1.1002",
            "future-ask",
            ask_available_at=BASE + timedelta(seconds=10),
        )
        self.assertEqual(late_ask.available_ts, BASE + timedelta(seconds=10))
        assessment = late_ask.assessment_for("ask", at=BASE, max_age_seconds=60)
        self.assertFalse(assessment.usable)
        self.assertIn(QuoteReason.FUTURE_AVAILABILITY, assessment.reasons)

        simulator = CFDSimulator(CFDConfig(horizons_seconds=(Decimal("60"),), max_quote_age_seconds=Decimal("60")))
        simulator.submit(CFDSignal("partial", "EUR/USD", Direction.LONG, BASE + timedelta(seconds=120)))
        simulator.on_quote(strict_quote(0, "1.1000", "1.1002", "q0"))
        simulator.on_quote(
            strict_quote(
                180,
                "1.1001",
                "1.1002",
                "q1",
                bid_source_second=180,
                ask_source_second=0,
                updated_sides=("bid",),
            )
        )
        current = simulator.current_quote
        self.assertIsNotNone(current)
        assert current is not None
        self.assertFalse(current.operable_for("ask", at=BASE + timedelta(seconds=180), max_age_seconds=60))
        self.assertFalse(current.operable_for("mid", at=BASE + timedelta(seconds=180), max_age_seconds=60))

        correlated = strict_quote(
            0,
            "1.1000",
            "1.1002",
            "correlated",
            bid_available_at=BASE,
            ask_available_at=BASE + timedelta(seconds=5),
        )
        self.assertFalse(correlated.operable_for("ask", at=BASE, max_age_seconds=60))
        self.assertTrue(correlated.operable_for("ask", at=BASE + timedelta(seconds=5), max_age_seconds=60))

    def test_contract_import_and_legacy_import_produce_same_replay(self) -> None:
        signal = CFDSignal("replay", "EUR/USD", Direction.LONG, BASE)
        entry_mapping = strict_quote(0, "1.1000", "1.1002", "entry").to_dict()
        close_mapping = strict_quote(60, "1.1005", "1.1007", "close").to_dict()
        legacy = CFDSimulator(CFDConfig(horizons_seconds=(Decimal("60"),))).replay(
            [signal], [CFDQuote.from_mapping(entry_mapping), CFDQuote.from_mapping(close_mapping)]
        )
        extracted = CFDSimulator(CFDConfig(horizons_seconds=(Decimal("60"),))).replay(
            [signal], [ContractQuote.from_mapping(entry_mapping), ContractQuote.from_mapping(close_mapping)]
        )
        self.assertEqual(legacy.trades[0].state, TradeState.CLOSED)
        self.assertEqual(legacy.trades[0].to_dict(), extracted.trades[0].to_dict())
        self.assertEqual(legacy.events, extracted.events)


if __name__ == "__main__":
    unittest.main()
