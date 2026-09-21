"""Focused account-completeness coverage for the canary anchor seam."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.data.protobuf_generated import OpenApiModelMessages_pb2 as model
from mtf_lab.ops.ctrader_account_risk import AccountRiskObserver
from tests.test_ctrader_account_risk import _deal, _pnl, _position
from tests.test_daily_equity_anchor import AnchorClient, FixedClock, Provider, _cashflow

NOW = datetime(2026, 9, 21, 15, tzinfo=UTC)


def _deal_at(*, conversion_fee: int | None) -> model.ProtoOADeal:
    deal = _deal(conversion_fee=conversion_fee)
    timestamp = int(NOW.timestamp() * 1000)
    deal.createTimestamp = timestamp
    deal.executionTimestamp = timestamp
    return deal


class _IncompleteDealsClient(AnchorClient):
    def request_message(self, message, *, client_msg_id: str, timeout_seconds: float):
        response = super().request_message(
            message,
            client_msg_id=client_msg_id,
            timeout_seconds=timeout_seconds,
        )
        if type(message).__name__ == "ProtoOADealListReq":
            response.payload.hasMore = True
        return response


class CanaryAccountCompletenessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FixedClock(NOW)
        self.client = AnchorClient(self.clock)
        self.provider = Provider(self.client)

    def _observer(self, root: Path, *, allow_new_baseline: bool = True) -> AccountRiskObserver:
        journal = root / "intents.jsonl"
        journal.write_text('{"journal_type":"event","event_type":"ACTIVATED"}\n', encoding="utf-8")
        os.chmod(journal, 0o600)
        return AccountRiskObserver(
            self.provider,
            proto=proto,
            clock=self.clock,
            state_path=root / "account-risk.json",
            journal_path=journal,
            allow_new_baseline=allow_new_baseline,
            require_cashflows=True,
        )

    def test_valid_current_account_is_complete_but_unverified_day_anchor_is_not(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-canary-account-complete-") as directory:
            root = Path(directory)
            self.client.balance = 110_000
            self.client.cashflows = [_cashflow(at=NOW, balance=110_000)]
            snapshot = self._observer(root).observe(now=NOW)

            self.assertTrue(snapshot.account_complete)
            self.assertFalse(snapshot.complete)
            self.assertEqual(snapshot.daily_loss_state, "UNKNOWN")
            self.assertIn("day_anchor_start_unobserved", snapshot.reasons)
            self.assertIsNone(snapshot.daily_loss)
            self.assertIsNone(snapshot.daily_pnl)
            self.assertFalse(snapshot.daily_anchor_verified)

            projection = snapshot.to_dict()
            self.assertTrue(projection["account_complete"])
            self.assertTrue(projection["completeness"]["account_complete"])

    def test_independent_account_gates_never_claim_account_complete(self) -> None:
        cases = {
            "fees": lambda: setattr(self.client, "deal_pages", [[_deal_at(conversion_fee=None)]]),
            "positions": lambda: setattr(self.client, "positions", [model.ProtoOAPosition(positionStatus=1)]),
            "unrealized": lambda: (
                setattr(self.client, "positions", [_position()]),
                setattr(self.client, "pnl", []),
            ),
            "cashflows": lambda: setattr(self.client, "cashflow_payload_missing", True),
            "margin": lambda: (
                setattr(self.client, "positions", [_position(used_margin=None)]),
                setattr(self.client, "pnl", [_pnl()]),
            ),
            "freshness": lambda: setattr(self.clock, "value", NOW - timedelta(seconds=11)),
        }

        for name, mutate in cases.items():
            with (
                self.subTest(name=name),
                tempfile.TemporaryDirectory(prefix=f"mtf-canary-account-incomplete-{name}-") as directory,
            ):
                root = Path(directory)
                self.clock = FixedClock(NOW)
                self.client = AnchorClient(self.clock)
                self.provider = Provider(self.client)
                mutate()
                snapshot = self._observer(root).observe(now=NOW)
                self.assertFalse(snapshot.account_complete)

    def test_missing_high_water_state_is_an_account_gate(self) -> None:
        # ``state_path=None`` intentionally permits an in-memory high-water
        # for generic read-only observations.  A canary must prove the
        # durable state boundary instead: an absent state with a live intent
        # cannot be treated as a fresh baseline.
        with tempfile.TemporaryDirectory(prefix="mtf-canary-account-state-missing-") as directory:
            root = Path(directory)
            observer = self._observer(root)
            journal = root / "intents.jsonl"
            journal.write_text('{"journal_type":"intent"}\n', encoding="utf-8")
            snapshot = observer.observe(now=NOW)

            self.assertFalse(snapshot.account_complete)
            self.assertFalse(snapshot.risk_state_complete)
            self.assertIn("risk_journal_nonempty", snapshot.reasons)
            self.assertFalse((root / "account-risk.json").exists())

    def test_incomplete_deal_pages_are_an_account_gate(self) -> None:
        self.client = _IncompleteDealsClient(self.clock)
        self.provider = Provider(self.client)
        with tempfile.TemporaryDirectory(prefix="mtf-canary-account-deals-") as directory:
            snapshot = self._observer(Path(directory)).observe(now=NOW)

        self.assertFalse(snapshot.account_complete)
        self.assertFalse(snapshot.deals_complete)

    def test_unverified_anchor_and_high_water_baselines_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-canary-account-baseline-") as directory:
            root = Path(directory)
            observer = self._observer(root)
            self.client.balance = 110_000
            self.client.cashflows = [_cashflow(at=NOW, balance=110_000)]
            first = observer.observe(now=NOW)
            anchor_path = root / "account-risk.day-anchor.json"
            state_path = root / "account-risk.json"
            anchor_before = json.loads(anchor_path.read_text(encoding="utf-8"))
            state_before = json.loads(state_path.read_text(encoding="utf-8"))

            self.client.balance = 109_000
            self.client.cashflows = [_cashflow(at=NOW, balance=110_000)]
            later = observer.observe(now=NOW)
            anchor_after = json.loads(anchor_path.read_text(encoding="utf-8"))
            state_after = json.loads(state_path.read_text(encoding="utf-8"))

            self.assertFalse(first.complete)
            self.assertFalse(later.complete)
            self.assertTrue(later.account_complete)
            self.assertIsNone(later.daily_loss)
            self.assertIsNone(later.daily_pnl)
            self.assertFalse(later.daily_anchor_verified)
            self.assertEqual(anchor_after["anchor_equity"], anchor_before["anchor_equity"])
            self.assertEqual(anchor_after["anchor_cashflow_total"], anchor_before["anchor_cashflow_total"])
            self.assertFalse(anchor_after["anchor_verified"])
            self.assertEqual(state_after["peak_equity"], state_before["peak_equity"])


if __name__ == "__main__":
    unittest.main()
