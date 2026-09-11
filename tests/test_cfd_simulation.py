from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
import unittest

from mtf_lab.ops.cfd_simulation import (
    CFDConfig,
    CFDQuote,
    CFDSignal,
    CFDSimulationError,
    CFDSimulator,
    Direction,
    TradeState,
    known_fixture_eurusd_long,
)


T0 = datetime(2026, 1, 1, tzinfo=UTC)


def quote(sec: int, bid: str, ask: str, quote_id: str, *, available_sec: int | None = None, quality: str = "VALID") -> CFDQuote:
    return CFDQuote(
        "EUR/USD",
        T0 + timedelta(seconds=sec),
        Decimal(bid),
        Decimal(ask),
        quote_id,
        available_at=T0 + timedelta(seconds=available_sec if available_sec is not None else sec),
        quality=quality,
    )


class CFDSimulationTests(unittest.TestCase):
    def test_known_long_fixture_uses_ask_then_bid_and_decimal_pnl(self) -> None:
        signal, quotes = known_fixture_eurusd_long()
        sim = CFDSimulator(CFDConfig(instrument="EUR/USD", units=Decimal("1000"), pip_size=Decimal("0.0001"), price_precision=5, horizons_seconds=(Decimal("60"),)))
        result = sim.replay([signal], quotes)
        trade = result.trades[0]
        self.assertEqual(trade.state, TradeState.CLOSED)
        self.assertEqual(trade.entry_side, "ask")
        self.assertEqual(trade.entry_price, Decimal("1.10020"))
        self.assertEqual(trade.close_price, Decimal("1.10050"))
        self.assertEqual(trade.pips, Decimal("3.0"))
        self.assertEqual(trade.gross_pnl_quote, Decimal("0.30000"))
        self.assertEqual(trade.net_pnl, Decimal("0.30000"))
        self.assertIsInstance(trade.net_pnl, Decimal)
        self.assertEqual(trade.lineage["entry_quote_id"], "fixture-entry")
        self.assertEqual(trade.lineage["close_quote_id"], "fixture-close")

    def test_short_uses_bid_then_ask_and_immobile_spread_loses_spread(self) -> None:
        signal = CFDSignal("short", "EUR/USD", Direction.SHORT, T0)
        sim = CFDSimulator(CFDConfig(instrument="EUR/USD", units="1000", pip_size="0.0001", price_precision=5, horizons_seconds=(60,)))
        result = sim.replay(
            [signal],
            [quote(0, "1.1000", "1.1002", "entry"), quote(60, "1.1000", "1.1002", "close")],
        )
        trade = result.trades[0]
        self.assertEqual(trade.state, TradeState.CLOSED)
        self.assertEqual(trade.entry_side, "bid")
        self.assertEqual(trade.entry_price, Decimal("1.1000"))
        self.assertEqual(trade.close_price, Decimal("1.1002"))
        self.assertEqual(trade.gross_pnl_quote, Decimal("-0.2000"))
        self.assertEqual(trade.pips, Decimal("-2"))

    def test_commission_and_slippage_are_explicit_costs(self) -> None:
        signal = CFDSignal("costs", "EUR/USD", "LONG", T0)
        cfg = CFDConfig(
            instrument="EUR/USD",
            units="1000",
            pip_size="0.0001",
            price_precision=5,
            horizons_seconds=(60,),
            commission_fixed="0.05",
            slippage_pips="1",
        )
        trade = CFDSimulator(cfg).replay([signal], [quote(0, "1.1000", "1.1002", "entry"), quote(60, "1.1005", "1.1007", "close")]).trades[0]
        self.assertEqual(trade.entry_price, Decimal("1.10030"))
        self.assertEqual(trade.close_price, Decimal("1.10040"))
        self.assertEqual(trade.gross_pnl_quote, Decimal("0.10000"))
        self.assertEqual(trade.commission_quote, Decimal("0.05"))
        self.assertEqual(trade.slippage_quote, Decimal("0.2000"))
        self.assertEqual(trade.net_pnl, Decimal("-0.15000"))

    def test_horizon_is_from_effective_fill_after_decision_and_entry_latency(self) -> None:
        signal = CFDSignal("latency", "EUR/USD", "LONG", T0, available_at=T0 + timedelta(seconds=2))
        cfg = CFDConfig(
            instrument="EUR/USD",
            units="1000",
            pip_size="0.0001",
            price_precision=5,
            horizons_seconds=(60,),
            decision_latency_seconds=1,
            entry_latency_seconds=2,
            close_latency_seconds=3,
            max_quote_age_seconds=5,
        )
        sim = CFDSimulator(cfg)
        sim.submit(signal)
        # At t=3 the signal's effective entry target is t=4, so it remains pending.
        self.assertEqual(sim.on_quote(quote(3, "1.1000", "1.1002", "q3")), ())
        filled = sim.on_quote(quote(5, "1.1000", "1.1002", "q5"))
        self.assertEqual(filled[0].state, TradeState.FILLED)
        self.assertEqual(filled[0].entry_available_at, T0 + timedelta(seconds=5))
        self.assertEqual(filled[0].close_target_at, T0 + timedelta(seconds=68))
        self.assertEqual(sim.on_quote(quote(67, "1.1005", "1.1007", "too-early")), ())
        closed = sim.on_quote(quote(68, "1.1005", "1.1007", "q68"))
        self.assertEqual(closed[0].state, TradeState.CLOSED)

    def test_pending_then_unknown_when_capture_completes(self) -> None:
        signal = CFDSignal("missing", "EUR/USD", "LONG", T0)
        sim = CFDSimulator(CFDConfig(instrument="EUR/USD", units="1000", horizons_seconds=(60,)))
        pending = sim.submit(signal)
        self.assertEqual(pending.state, TradeState.PENDING)
        self.assertEqual(sim.advance(T0 + timedelta(seconds=600), capture_complete=False), ())
        self.assertEqual(sim.trades[0].state, TradeState.PENDING)
        unknown = sim.advance(T0 + timedelta(seconds=600), capture_complete=True)
        self.assertEqual(unknown[0].state, TradeState.UNKNOWN)
        self.assertEqual(unknown[0].reason, "ENTRY_QUOTE_NOT_AVAILABLE")
        self.assertIsNone(unknown[0].net_pnl)

    def test_missing_conversion_or_required_financing_is_unknown(self) -> None:
        signal = CFDSignal("conversion", "EUR/JPY", "LONG", T0)
        quotes = [CFDQuote("EUR/JPY", T0, "160.00", "160.02", "e"), CFDQuote("EUR/JPY", T0 + timedelta(seconds=60), "160.10", "160.12", "c")]
        conversion_missing = CFDSimulator(CFDConfig(instrument="EUR/JPY", account_currency="USD", units="1000", pip_size="0.01", price_precision=2, horizons_seconds=(60,))).replay([signal], quotes).trades[0]
        self.assertEqual(conversion_missing.state, TradeState.UNKNOWN)
        self.assertEqual(conversion_missing.reason, "CONVERSION_RATE_MISSING")
        self.assertEqual(conversion_missing.gross_pnl_quote, Decimal("80.00"))
        financing_missing = CFDSimulator(CFDConfig(instrument="EUR/JPY", account_currency="JPY", units="1000", pip_size="0.01", price_precision=2, horizons_seconds=(60,), financing_required=True)).replay([signal], quotes).trades[0]
        self.assertEqual(financing_missing.state, TradeState.UNKNOWN)
        self.assertEqual(financing_missing.reason, "FINANCING_RATE_MISSING")

    def test_stream_and_replay_share_same_state_machine(self) -> None:
        signal = CFDSignal("same", "EUR/USD", "LONG", T0)
        quotes = (quote(0, "1.1000", "1.1002", "e"), quote(60, "1.1005", "1.1007", "c"))
        cfg = CFDConfig(instrument="EUR/USD", units="1000", horizons_seconds=(60,))
        streamed = CFDSimulator(cfg)
        streamed.submit(signal)
        streamed.on_quote(quotes[0])
        streamed.on_quote(quotes[1])
        replayed = CFDSimulator(cfg).replay([signal], quotes)
        self.assertEqual(streamed.trades[0].to_dict(), replayed.trades[0].to_dict())
        self.assertEqual(streamed.trades[0].identity, replayed.trades[0].identity)

    def test_config_identity_rejection_and_no_double_leverage(self) -> None:
        with self.assertRaises(CFDSimulationError):
            CFDConfig.from_mapping({"instrument": "EUR/USD", "leverage": 10})
        with self.assertRaises(CFDSimulationError):
            CFDQuote("EUR/USD", T0, "1.1002", "1.1000", "bad")
        with self.assertRaises(CFDSimulationError):
            CFDSignal("bad-direction", "EUR/USD", "SIDEWAYS", T0)
        signal = CFDSignal("reject", "GBP/USD", "LONG", T0)
        trade = CFDSimulator(CFDConfig(instrument="EUR/USD", units="1000", horizons_seconds=(60,))).submit(signal)
        self.assertEqual(trade.state, TradeState.REJECTED)
        self.assertEqual(trade.reason, "INSTRUMENT_MISMATCH")
        self.assertNotIn("leverage", CFDConfig().to_dict())

    def test_default_horizons_and_fixture_api_are_offline(self) -> None:
        cfg = CFDConfig()
        self.assertEqual(cfg.horizons_seconds, (Decimal("60"), Decimal("180"), Decimal("300")))
        signal, quotes = known_fixture_eurusd_long()
        self.assertEqual(signal.instrument, "EUR/USD")
        self.assertEqual(len(quotes), 2)


if __name__ == "__main__":
    unittest.main()

class CFDHorizonTests(unittest.TestCase):
    def test_default_horizons_are_independent_trades(self) -> None:
        signal, quotes = known_fixture_eurusd_long()
        later = CFDQuote("EUR/USD", T0 + timedelta(seconds=301), "1.2000", "1.2002", "later")
        result = CFDSimulator(CFDConfig(instrument="EUR/USD", units="1000")).replay([signal], (*quotes, later))
        self.assertEqual({trade.horizon_seconds for trade in result.trades}, {Decimal("60"), Decimal("180"), Decimal("300")})
        self.assertEqual(len(result.trades), 3)
