"""Focused economic-contract tests for the versioned CFD PAPER model."""

from __future__ import annotations

import copy
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.core.canonical import fingerprint
from mtf_lab.core.cfd_simulation import (
    CFD_ECONOMICS_LEGACY_VERSION,
    CFD_ECONOMICS_VERSION,
    CFDConfig,
    CFDQuote,
    CFDSignal,
    CFDSimulator,
    Direction,
    TradeState,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def quote(second: int, bid: str, ask: str, quote_id: str) -> CFDQuote:
    return CFDQuote(
        "EUR/USD",
        BASE + timedelta(seconds=second),
        Decimal(bid),
        Decimal(ask),
        quote_id,
    )


class CFDEconomicsV2Tests(unittest.TestCase):
    def test_slippage_is_accounted_once_and_prices_are_quantized_before_pnl(self) -> None:
        signal = CFDSignal("v2-costs", "EUR/USD", Direction.LONG, BASE)
        config = CFDConfig(
            instrument="EUR/USD",
            units="1000",
            pip_size="0.0001",
            price_precision=5,
            horizons_seconds=("60",),
            commission_fixed="0.05",
            slippage_pips="1",
            economics_version=CFD_ECONOMICS_VERSION,
        )
        trade = (
            CFDSimulator(config)
            .replay([signal], [quote(0, "1.1000", "1.1002", "entry"), quote(60, "1.1005", "1.1007", "close")])
            .trades[0]
        )

        self.assertEqual(trade.state, TradeState.CLOSED)
        self.assertEqual(trade.economics_version, CFD_ECONOMICS_VERSION)
        self.assertEqual(trade.entry_price, Decimal("1.10030"))
        self.assertEqual(trade.close_price, Decimal("1.10040"))
        self.assertEqual(trade.entry_reference_price, Decimal("1.10020"))
        self.assertEqual(trade.close_reference_price, Decimal("1.10050"))
        self.assertEqual(trade.reference_gross_pnl_quote, Decimal("0.30000"))
        self.assertEqual(trade.gross_pnl_quote, Decimal("0.10000"))
        self.assertEqual(trade.pips, Decimal("1.0"))
        self.assertEqual(trade.slippage_quote, Decimal("0.20000"))
        self.assertEqual(trade.commission_quote, Decimal("0.05"))
        self.assertEqual(trade.net_pnl, Decimal("0.05000"))
        self.assertEqual(trade.costs_account, Decimal("0.05"))
        self.assertEqual(trade.economic_result.costs_quote, Decimal("0.05"))
        self.assertEqual(trade.net_pnl, trade.gross_pnl_quote - Decimal("0.05"))

    def test_legacy_config_keeps_previous_double_slippage_contract(self) -> None:
        signal = CFDSignal("v1-costs", "EUR/USD", Direction.LONG, BASE)
        config = CFDConfig(
            instrument="EUR/USD",
            units="1000",
            pip_size="0.0001",
            price_precision=5,
            horizons_seconds=("60",),
            commission_fixed="0.05",
            slippage_pips="1",
            economics_version=CFD_ECONOMICS_LEGACY_VERSION,
        )
        trade = (
            CFDSimulator(config)
            .replay([signal], [quote(0, "1.1000", "1.1002", "entry"), quote(60, "1.1005", "1.1007", "close")])
            .trades[0]
        )

        self.assertEqual(trade.economics_version, CFD_ECONOMICS_LEGACY_VERSION)
        self.assertEqual(trade.entry_price, Decimal("1.10030"))
        self.assertEqual(trade.close_price, Decimal("1.10040"))
        self.assertEqual(trade.gross_pnl_quote, Decimal("0.10000"))
        self.assertEqual(trade.slippage_quote, Decimal("0.2000"))
        self.assertEqual(trade.net_pnl, Decimal("-0.15000"))

    def test_v2_snapshot_round_trip_preserves_economic_identity(self) -> None:
        config = CFDConfig(
            instrument="EUR/USD",
            units="1000",
            horizons_seconds=("60",),
            slippage_pips="1",
            commission_fixed="0.05",
            economics_version=CFD_ECONOMICS_VERSION,
        )
        signal = CFDSignal("snapshot-v2", "EUR/USD", Direction.LONG, BASE)
        entry = quote(0, "1.1000", "1.1002", "entry")
        close = quote(60, "1.1005", "1.1007", "close")
        live = CFDSimulator(config)
        live.submit(signal)
        live.on_quote(entry)
        snapshot = live.snapshot()
        self.assertEqual(snapshot["economics_version"], CFD_ECONOMICS_VERSION)
        self.assertEqual(snapshot["config"]["economics_version"], CFD_ECONOMICS_VERSION)
        self.assertEqual(snapshot["trades"][0]["economics_version"], CFD_ECONOMICS_VERSION)

        restored = CFDSimulator.from_snapshot(snapshot)
        restored.on_quote(close)
        result = restored.finish()
        expected = CFDSimulator(config).replay([signal], [entry, close])
        self.assertEqual(result.trades[0].to_dict(), expected.trades[0].to_dict())

    def test_pre_h1_snapshot_is_explicitly_restored_as_legacy(self) -> None:
        config = CFDConfig(
            instrument="EUR/USD",
            units="1000",
            horizons_seconds=("60",),
            slippage_pips="1",
            commission_fixed="0.05",
            economics_version=CFD_ECONOMICS_LEGACY_VERSION,
        )
        signal = CFDSignal("legacy-snapshot", "EUR/USD", Direction.LONG, BASE)
        live = CFDSimulator(config)
        live.submit(signal)
        legacy = copy.deepcopy(live.snapshot())
        legacy.pop("economics_version", None)
        raw_config = dict(legacy["config"])
        raw_config.pop("economics_version", None)
        legacy["config"] = raw_config
        legacy["config_hash"] = fingerprint(raw_config)
        for raw_trade in legacy["trades"]:
            raw_trade.pop("economics_version", None)
            raw_trade.pop("entry_reference_price", None)
            raw_trade.pop("close_reference_price", None)
            raw_trade.pop("reference_gross_pnl_quote", None)
            lineage = raw_trade.get("lineage")
            if isinstance(lineage, dict):
                lineage.pop("economics_version", None)
        material = dict(legacy)
        material.pop("snapshot_hash", None)
        legacy["snapshot_hash"] = fingerprint(material)

        restored = CFDSimulator.from_snapshot(legacy)
        self.assertEqual(restored.config.economics_version, CFD_ECONOMICS_LEGACY_VERSION)
        self.assertEqual(restored.trades[0].economics_version, CFD_ECONOMICS_LEGACY_VERSION)
        restored.on_quote(quote(0, "1.1000", "1.1002", "entry"))
        restored.on_quote(quote(60, "1.1005", "1.1007", "close"))
        trade = restored.finish().trades[0]
        self.assertEqual(trade.gross_pnl_quote, Decimal("0.10000"))
        self.assertEqual(trade.net_pnl, Decimal("-0.15000"))

    def test_snapshot_rejects_inconsistent_terminal_order(self) -> None:
        simulator = CFDSimulator(CFDConfig(horizons_seconds=("60",)))
        simulator.submit(CFDSignal("snapshot-shape", "EUR/USD", Direction.LONG, BASE))
        snapshot = simulator.snapshot()
        snapshot["terminal_order"] = ["not-a-trade"]
        material = dict(snapshot)
        material.pop("snapshot_hash", None)
        snapshot["snapshot_hash"] = fingerprint(material)
        with self.assertRaisesRegex(ValueError, "terminal_order"):
            CFDSimulator.from_snapshot(snapshot)


if __name__ == "__main__":
    unittest.main()
