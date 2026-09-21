"""Regression coverage for high-water drawdown versus the daily anchor gate."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.ops.ctrader_account_risk import AccountRiskObserver
from tests.test_ctrader_account_risk import _deal
from tests.test_daily_equity_anchor import NOW, AnchorClient, FixedClock, Provider


class CanaryDrawdownIndependenceTests(unittest.TestCase):
    def _observer(self, root: Path, client: AnchorClient, clock: FixedClock) -> AccountRiskObserver:
        journal = root / "intents.jsonl"
        journal.write_text('{"journal_type":"event","event_type":"ACTIVATED"}\n', encoding="utf-8")
        os.chmod(journal, 0o600)
        return AccountRiskObserver(
            Provider(client),
            proto=proto,
            clock=clock,
            state_path=root / "account-risk.json",
            journal_path=journal,
            allow_new_baseline=True,
            require_cashflows=True,
        )

    def test_fresh_account_keeps_drawdown_when_daily_anchor_is_unknown(self) -> None:
        clock = FixedClock(NOW)
        client = AnchorClient(clock)
        with tempfile.TemporaryDirectory(prefix="mtf-canary-drawdown-") as directory:
            observer = self._observer(Path(directory), client, clock)
            observer.observe(now=NOW)
            client.balance = 99_000
            snapshot = observer.observe(now=NOW)

        self.assertTrue(snapshot.fresh)
        self.assertTrue(snapshot.account_complete)
        self.assertFalse(snapshot.complete)
        self.assertEqual(snapshot.drawdown, "10.00")
        self.assertEqual(snapshot.high_water_equity, "1000.00")
        self.assertEqual(snapshot.daily_loss_state, "UNKNOWN")
        self.assertIsNone(snapshot.daily_loss)
        self.assertIsNone(snapshot.daily_pnl)
        self.assertFalse(snapshot.daily_anchor_verified)

    def test_stale_account_does_not_report_high_water_drawdown(self) -> None:
        clock = FixedClock(NOW)
        client = AnchorClient(clock)
        with tempfile.TemporaryDirectory(prefix="mtf-canary-drawdown-stale-") as directory:
            observer = self._observer(Path(directory), client, clock)
            observer.observe(now=NOW)
            clock.value = NOW - timedelta(seconds=11)
            snapshot = observer.observe(now=NOW)

        self.assertFalse(snapshot.fresh)
        self.assertFalse(snapshot.account_complete)
        self.assertIsNone(snapshot.drawdown)

    def test_missing_high_water_state_does_not_report_drawdown(self) -> None:
        clock = FixedClock(NOW)
        client = AnchorClient(clock)
        with tempfile.TemporaryDirectory(prefix="mtf-canary-drawdown-high-water-") as directory:
            root = Path(directory)
            observer = self._observer(root, client, clock)
            (root / "intents.jsonl").write_text('{"journal_type":"intent"}\n', encoding="utf-8")
            snapshot = observer.observe(now=NOW)

        self.assertFalse(snapshot.account_complete)
        self.assertFalse(snapshot.risk_state_complete)
        self.assertIsNone(snapshot.drawdown)

    def test_account_incomplete_does_not_report_high_water_drawdown(self) -> None:
        clock = FixedClock(NOW)
        client = AnchorClient(clock)
        with tempfile.TemporaryDirectory(prefix="mtf-canary-drawdown-incomplete-") as directory:
            observer = self._observer(Path(directory), client, clock)
            observer.observe(now=NOW)
            client.balance = 99_000
            client.deal_pages = [[_deal(conversion_fee=None)]]
            snapshot = observer.observe(now=NOW)

        self.assertFalse(snapshot.account_complete)
        self.assertFalse(snapshot.fees_complete)
        self.assertIsNone(snapshot.drawdown)


if __name__ == "__main__":
    unittest.main()
