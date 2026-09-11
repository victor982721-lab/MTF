"""Contracts for the typed CFD quote boundary and resumable PAPER session."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal, getcontext

from mtf_lab.ops.cfd_simulation import (
    CFDConfig,
    CFDQuote,
    CFDSignal,
    CFDSimulationError,
    CFDSimulator,
    EconomicState,
    QuoteReason,
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
    quality: str = "VALID",
    snapshot: bool = False,
    generation: int = 1,
) -> CFDQuote:
    when = BASE + timedelta(seconds=second)
    bid_at = BASE + timedelta(seconds=bid_source_second if bid_source_second is not None else second)
    ask_at = BASE + timedelta(seconds=ask_source_second if ask_source_second is not None else second)
    return CFDQuote(
        "EUR/USD",
        when,
        bid,
        ask,
        quote_id,
        available_at=when,
        quality=quality,
        updated_sides=updated_sides,
        session_generation=generation,
        is_snapshot=snapshot,
        bid_source_timestamp=bid_at if bid is not None else None,
        ask_source_timestamp=ask_at if ask is not None else None,
        bid_source_timestamp_missing=bid is None,
        ask_source_timestamp_missing=ask is None,
        bid_timestamp_known=bid is not None,
        ask_timestamp_known=ask is not None,
    )


class CFDQuoteContractTests(unittest.TestCase):
    def test_old_ask_new_bid_blocks_long_entry_and_mid(self) -> None:
        simulator = CFDSimulator(CFDConfig(horizons_seconds=(60,), max_quote_age_seconds=60))
        simulator.submit(CFDSignal("old-ask", "EUR/USD", "LONG", BASE + timedelta(seconds=120)))
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
        self.assertEqual(simulator.trades[0].state, TradeState.PENDING)
        current = simulator.current_quote
        self.assertIsNotNone(current)
        assert current is not None
        self.assertFalse(
            current.operable_for("ask", at=BASE + timedelta(seconds=180), max_age_seconds=60, expected_session=1)
        )
        self.assertFalse(
            current.operable_for("mid", at=BASE + timedelta(seconds=180), max_age_seconds=60, expected_session=1)
        )
        self.assertIsNotNone(current.mid)
        self.assertIsNone(current.mid_at(at=BASE + timedelta(seconds=180), max_age_seconds=60, expected_session=1))
        self.assertEqual(
            current.assessment_for("ask", at=BASE + timedelta(seconds=180), max_age_seconds=60).status.value, "STALE"
        )

    def test_generic_stale_reason_blocks_without_side_quality_and_text_cannot_upgrade(self) -> None:
        quote = CFDQuote(
            "EUR/USD",
            BASE,
            "1.1000",
            "1.1002",
            "generic-stale",
            quote_quality={"state": "VALID", "reasons": ["STALE"]},
            bid_source_timestamp=BASE,
            ask_source_timestamp=BASE,
            bid_timestamp_known=True,
            ask_timestamp_known=True,
        )
        self.assertEqual(quote.mid, Decimal("1.1001"))
        self.assertIsNone(quote.mid_at(max_age_seconds=60))
        self.assertFalse(quote.operable_for("bid", max_age_seconds=60))
        self.assertFalse(quote.operable_for("ask", max_age_seconds=60))
        unchanged = CFDQuote(
            "EUR/USD",
            BASE,
            "1.1000",
            "1.1002",
            "text-quality",
            quality="VALID_WITH_WARNINGS",
        )
        self.assertFalse(unchanged.operable_for("bid"))

    def test_old_bid_new_ask_blocks_short_entry_symmetrically(self) -> None:
        simulator = CFDSimulator(CFDConfig(horizons_seconds=(60,), max_quote_age_seconds=60))
        simulator.submit(CFDSignal("old-bid", "EUR/USD", "SHORT", BASE + timedelta(seconds=120)))
        simulator.on_quote(strict_quote(0, "1.1000", "1.1002", "q0"))
        simulator.on_quote(
            strict_quote(
                180,
                "1.1000",
                "1.1003",
                "q1",
                bid_source_second=0,
                ask_source_second=180,
                updated_sides=("ask",),
            )
        )
        self.assertEqual(simulator.trades[0].state, TradeState.PENDING)
        current = simulator.current_quote
        assert current is not None
        self.assertFalse(
            current.operable_for("bid", at=BASE + timedelta(seconds=180), max_age_seconds=60, expected_session=1)
        )

    def test_missing_timestamp_and_snapshot_are_fail_closed(self) -> None:
        missing = CFDQuote(
            "EUR/USD",
            BASE,
            "1.1000",
            "1.1002",
            "missing-ask-ts",
            bid_source_timestamp=BASE,
            ask_source_timestamp=None,
            bid_source_timestamp_missing=False,
            ask_source_timestamp_missing=True,
            bid_timestamp_known=True,
            ask_timestamp_known=False,
        )
        self.assertFalse(missing.operable_for("ask", max_age_seconds=60))
        self.assertTrue(missing.operable_for("bid", max_age_seconds=60))
        snapshot = strict_quote(0, "1.1000", "1.1002", "snapshot", snapshot=True)
        self.assertFalse(snapshot.operable_for("bid", max_age_seconds=60))
        self.assertEqual(snapshot.assessment_for("mid").status.value, "SNAPSHOT")

    def test_disconnect_and_generation_change_do_not_fill(self) -> None:
        simulator = CFDSimulator(CFDConfig(horizons_seconds=(1,), max_quote_age_seconds=5))
        simulator.submit(CFDSignal("session", "EUR/USD", "LONG", BASE))
        simulator.disconnect()
        self.assertEqual(simulator.on_quote(strict_quote(0, "1.1", "1.1002", "offline")), ())
        simulator.reconnect(2)
        self.assertEqual(simulator.on_quote(strict_quote(1, "1.1", "1.1002", "baseline", generation=2)), ())
        self.assertEqual(simulator.trades[0].state, TradeState.PENDING)
        self.assertTrue(any(event["event"] == "quote_blocked" for event in simulator.events))

    def test_crossed_quote_is_rejected_at_the_strict_boundary(self) -> None:
        with self.assertRaisesRegex(CFDSimulationError, "ask debe ser mayor") as captured:
            CFDQuote(
                "EUR/USD",
                BASE,
                "1.1002",
                "1.1000",
                "crossed",
                quality="INVALID",
                quote_reasons=(QuoteReason.CROSSED,),
                bid_source_timestamp=BASE,
                ask_source_timestamp=BASE,
                bid_timestamp_known=True,
                ask_timestamp_known=True,
            )
        self.assertEqual(captured.exception.code, "CROSSED_QUOTE")


class CFDSessionContractTests(unittest.TestCase):
    def _config(self, **changes: object) -> CFDConfig:
        values: dict[str, object] = {
            "instrument": "EUR/USD",
            "units": "1000",
            "horizons_seconds": (Decimal("60"),),
            "max_quote_age_seconds": Decimal("5"),
            "terminal_retention": 2,
            "event_retention": 64,
        }
        values.update(changes)
        return CFDConfig(**values)

    def test_incremental_checkpoint_restore_matches_replay(self) -> None:
        config = self._config()
        signal = CFDSignal("resume", "EUR/USD", "LONG", BASE)
        entry = strict_quote(0, "1.1000", "1.1002", "entry")
        close = strict_quote(60, "1.1005", "1.1007", "close")
        live = CFDSimulator(config)
        live.submit(signal)
        live.on_quote(entry)
        checkpoint = live.snapshot()
        restored = CFDSimulator(config)
        restored.restore(checkpoint)
        restored.on_quote(close)
        resumed = restored.finish()
        replayed = CFDSimulator(config).replay([signal], [entry, close])
        self.assertEqual(resumed.trades[0].to_dict(), replayed.trades[0].to_dict())
        self.assertEqual(resumed.counters["closures"], replayed.counters["closures"])
        self.assertEqual(resumed.finished, replayed.finished)

    def test_decimal_results_ignore_ambient_context(self) -> None:
        config = self._config(units="1000", pip_size="0.0001")
        signal = CFDSignal("decimal", "EUR/USD", "LONG", BASE)
        quotes = (strict_quote(0, "1.1000", "1.1002", "entry"), strict_quote(60, "1.1005", "1.1007", "close"))
        original = getcontext().copy()
        try:
            getcontext().prec = 4
            first = CFDSimulator(config).replay([signal], quotes).trades[0].to_dict()
            getcontext().prec = 80
            second = CFDSimulator(config).replay([signal], quotes).trades[0].to_dict()
        finally:
            getcontext().prec = original.prec
            getcontext().rounding = original.rounding
        self.assertEqual(first, second)
        self.assertEqual(first["net_pnl"], "0.30000")

    def test_unknown_commission_keeps_closed_lifecycle_and_unknown_economy(self) -> None:
        config = self._config(commission_known=False)
        signal = CFDSignal("commission-unknown", "EUR/USD", "LONG", BASE)
        result = CFDSimulator(config).replay(
            [signal],
            [strict_quote(0, "1.1000", "1.1002", "entry"), strict_quote(60, "1.1005", "1.1007", "close")],
        )
        trade = result.trades[0]
        self.assertEqual(trade.state, TradeState.CLOSED)
        self.assertTrue(trade.close_observed)
        self.assertEqual(trade.gross_pnl_quote, Decimal("0.30000"))
        self.assertIsNone(trade.commission_quote)
        self.assertIsNone(trade.net_pnl)
        self.assertEqual(trade.economic_result.state, EconomicState.INDETERMINATE)
        self.assertEqual(trade.economic_result.reason, "COMMISSION_UNKNOWN")

    def test_advance_expires_unfilled_window_before_dataset_end(self) -> None:
        config = self._config(horizons_seconds=(Decimal("1"),), max_quote_age_seconds=Decimal("5"))
        simulator = CFDSimulator(config)
        simulator.submit(CFDSignal("expires", "EUR/USD", "LONG", BASE))
        changed = simulator.advance(BASE + timedelta(seconds=6), capture_complete=False)
        self.assertEqual(changed[0].state, TradeState.UNKNOWN)
        self.assertEqual(changed[0].reason, "ENTRY_QUOTE_WINDOW_EXPIRED")
        self.assertEqual(simulator.counters["expiry_unknown"], 1)
        self.assertEqual(simulator.on_quote(strict_quote(10, "1.1000", "1.1002", "too-late")), ())

    def test_active_capacity_is_explicit_backpressure_not_silent_eviction(self) -> None:
        simulator = CFDSimulator(self._config(max_active_trades=2, terminal_retention=8))
        simulator.submit(CFDSignal("active-0", "EUR/USD", "LONG", BASE))
        simulator.submit(CFDSignal("active-1", "EUR/USD", "LONG", BASE))
        with self.assertRaises(CFDSimulationError) as caught:
            simulator.submit(CFDSignal("active-2", "EUR/USD", "LONG", BASE))
        self.assertEqual(caught.exception.code, "ACTIVE_CAPACITY_EXCEEDED")
        self.assertEqual(len(simulator.trades), 2)

    def test_terminal_retention_is_bounded_but_counters_are_total(self) -> None:
        config = self._config(terminal_retention=2, horizons_seconds=(Decimal("1"),))
        simulator = CFDSimulator(config, terminal_lookup=lambda _trade_id: None)
        for index in range(5):
            start = BASE + timedelta(seconds=index * 2)
            simulator.submit(CFDSignal(f"signal-{index}", "EUR/USD", "LONG", start))
            simulator.on_quote(strict_quote(index * 2, "1.1000", "1.1002", f"entry-{index}"))
            simulator.on_quote(strict_quote(index * 2 + 1, "1.1005", "1.1007", f"close-{index}"))
        self.assertLessEqual(len(simulator.trades), 2)
        self.assertEqual(simulator.counters["closures"], 5)
        self.assertEqual(simulator.counters["terminal_evicted"], 3)
        self.assertEqual(simulator.counters["terminal_retained"], 2)

    def test_evicted_terminal_identity_fails_closed_without_lookup(self) -> None:
        config = self._config(terminal_retention=1, horizons_seconds=(Decimal("1"),))
        simulator = CFDSimulator(config)
        first = CFDSignal("evicted", "EUR/USD", "LONG", BASE)
        second = CFDSignal("retained", "EUR/USD", "LONG", BASE + timedelta(seconds=2))
        simulator.submit(first)
        simulator.on_quote(strict_quote(0, "1.1000", "1.1002", "entry-evicted"))
        simulator.on_quote(strict_quote(1, "1.1005", "1.1007", "close-evicted"))
        simulator.submit(second)
        simulator.on_quote(strict_quote(2, "1.1000", "1.1002", "entry-retained"))
        simulator.on_quote(strict_quote(3, "1.1005", "1.1007", "close-retained"))
        self.assertEqual(len(simulator.trades), 1)
        with self.assertRaises(CFDSimulationError) as caught:
            simulator.submit(first)
        self.assertEqual(caught.exception.code, "ARCHIVE_REQUIRED")

    def test_finish_and_restore_never_resurrect_terminal_trade(self) -> None:
        simulator = CFDSimulator(self._config())
        simulator.submit(CFDSignal("terminal", "EUR/USD", "LONG", BASE))
        simulator.finish()
        self.assertEqual(simulator.trades[0].state, TradeState.UNKNOWN)
        restored = CFDSimulator.from_snapshot(simulator.snapshot())
        self.assertTrue(restored.finished)
        restored.on_quote(strict_quote(10, "1.2", "1.2002", "late"))
        self.assertEqual(restored.trades[0].state, TradeState.UNKNOWN)
        self.assertEqual(restored.trades[0].economic_result.state, EconomicState.INDETERMINATE)


if __name__ == "__main__":
    unittest.main()
