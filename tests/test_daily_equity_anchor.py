from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from mtf_lab.data.ctrader_protocol import WireMessage
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.data.protobuf_generated import OpenApiModelMessages_pb2 as model
from mtf_lab.ops.ctrader_account_risk import AccountRiskObserver

NOW = datetime(2026, 9, 13, 12, tzinfo=UTC)


def _position() -> model.ProtoOAPosition:
    position = model.ProtoOAPosition(positionId=7, positionStatus="POSITION_STATUS_OPEN")
    position.tradeData.symbolId = 99
    position.tradeData.volume = 100
    position.usedMargin = 2500
    position.moneyDigits = 2
    return position


def _pnl(value: int = 0) -> model.ProtoOAPositionUnrealizedPnL:
    return model.ProtoOAPositionUnrealizedPnL(
        positionId=7,
        grossUnrealizedPnL=value,
        netUnrealizedPnL=value,
    )


def _cashflow(
    *,
    flow_id: int = 1,
    delta: int = 10_000,
    balance: int = 110_000,
    at: datetime = NOW,
    operation: int = 0,
) -> model.ProtoOADepositWithdraw:
    return model.ProtoOADepositWithdraw(
        operationType="BALANCE_DEPOSIT" if operation == 0 else "BALANCE_WITHDRAW",
        balanceHistoryId=flow_id,
        balance=balance,
        delta=delta,
        changeBalanceTimestamp=int(at.timestamp() * 1000),
        moneyDigits=2,
    )


class FixedClock:
    def __init__(self, value: datetime):
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class AnchorClient:
    """Small generated-Protobuf server double for the day-anchor boundary."""

    def __init__(self, clock: FixedClock) -> None:
        self.clock = clock
        self.generation = 1
        self.response_generation: int | None = 1
        self.balance = 100_000
        self.positions: list[model.ProtoOAPosition] = []
        self.pnl: list[model.ProtoOAPositionUnrealizedPnL] = []
        self.deal_pages: list[list[model.ProtoOADeal]] = [[]]
        self.cashflows: list[model.ProtoOADepositWithdraw] = []
        self.cashflow_payload_missing = False
        self.calls: list[str] = []

    def authenticated_session_evidence(self) -> dict[str, Any]:
        return {
            "account_id": "123",
            "session_id": "anchor-session",
            "connection_generation": str(self.generation),
            "environment": "DEMO",
            "endpoint": "demo.ctraderapi.com:5035",
        }

    def validate_session_evidence(self, _proof: Any) -> bool:
        return True

    def request_message(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> WireMessage:
        del timeout_seconds
        name = type(message).__name__
        self.calls.append(name)
        if name == "ProtoOATraderReq":
            body: Any = proto.ProtoOATraderRes(
                ctidTraderAccountId=123,
                trader=model.ProtoOATrader(ctidTraderAccountId=123, balance=self.balance, moneyDigits=2),
            )
            payload_type = 2122
        elif name == "ProtoOAReconcileReq":
            body = proto.ProtoOAReconcileRes(ctidTraderAccountId=123, position=self.positions)
            payload_type = 2125
        elif name == "ProtoOAGetPositionUnrealizedPnLReq":
            body = proto.ProtoOAGetPositionUnrealizedPnLRes(
                ctidTraderAccountId=123,
                positionUnrealizedPnL=self.pnl,
                moneyDigits=2,
            )
            payload_type = 2188
        elif name == "ProtoOADealListReq":
            body = proto.ProtoOADealListRes(ctidTraderAccountId=123, deal=self.deal_pages[0], hasMore=False)
            payload_type = 2134
        elif name == "ProtoOACashFlowHistoryListReq":
            if self.cashflow_payload_missing:
                body = {"ctidTraderAccountId": 123}
            else:
                body = proto.ProtoOACashFlowHistoryListRes(
                    ctidTraderAccountId=123,
                    depositWithdraw=self.cashflows,
                )
            payload_type = 2144
        else:  # pragma: no cover - fixed observer request allowlist
            raise AssertionError(name)
        return WireMessage(
            payload_type,
            body,
            client_msg_id,
            received_at=self.clock(),
            available_at=self.clock(),
            connection_generation=self.response_generation,
        )


class Provider:
    def __init__(self, client: AnchorClient) -> None:
        self.client = client


class DailyEquityAnchorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FixedClock(NOW)
        self.client = AnchorClient(self.clock)
        self.provider = Provider(self.client)

    def _observer(self, root: Path, *, allow_new_baseline: bool) -> AccountRiskObserver:
        journal = root / "intents.jsonl"
        if not journal.exists():
            journal.write_text('{"journal_type":"event","event_type":"ACTIVATED"}\n', encoding="utf-8")
            os.chmod(journal, 0o600)
        return AccountRiskObserver(
            self.provider,
            clock=self.clock,
            state_path=root / "account-risk.json",
            journal_path=journal,
            allow_new_baseline=allow_new_baseline,
            require_cashflows=True,
        )

    def test_daily_loss_uses_equity_anchor_and_cashflow_adjustment(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-day-anchor-") as tmp:
            root = Path(tmp)
            observer = self._observer(root, allow_new_baseline=True)
            day_start = NOW.replace(hour=0)
            self.clock.value = day_start
            first = observer.observe(now=day_start)
            self.assertTrue(first.complete)
            self.assertEqual(Decimal(first.daily_pnl or "nan"), Decimal("0"))
            self.assertEqual(first.daily_loss, "0")
            self.assertEqual(Decimal(first.daily_anchor_equity or "nan"), Decimal("1000"))
            self.assertEqual(first.daily_cashflow_total, "0")

            self.clock.value = NOW
            self.client.balance = 110_000
            self.client.positions = [_position()]
            self.client.pnl = [_pnl(-5_000)]
            self.client.cashflows = [_cashflow()]
            lower = observer.observe(now=NOW)
            self.assertTrue(lower.complete)
            self.assertEqual(lower.equity, "1050.00")
            self.assertEqual(Decimal(lower.daily_pnl or "nan"), Decimal("-50"))
            self.assertEqual(Decimal(lower.daily_loss or "nan"), Decimal("50"))
            self.assertEqual(Decimal(lower.daily_anchor_equity or "nan"), Decimal("1000"))
            self.assertEqual(Decimal(lower.daily_cashflow_total or "nan"), Decimal("100"))
            self.assertNotEqual(lower.daily_pnl, lower.realized_daily_pnl)

    def test_anchor_and_cashflow_ledger_survive_restart_without_reset(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-day-anchor-restart-") as tmp:
            root = Path(tmp)
            observer = self._observer(root, allow_new_baseline=True)
            day_start = NOW.replace(hour=0)
            self.clock.value = day_start
            observer.observe(now=day_start)
            self.clock.value = NOW
            self.client.balance = 110_000
            self.client.positions = [_position()]
            self.client.pnl = [_pnl(-5_000)]
            self.client.cashflows = [_cashflow()]
            lower = observer.observe(now=NOW)
            self.assertEqual(Decimal(lower.daily_pnl or "nan"), Decimal("-50"))

            restarted = self._observer(root, allow_new_baseline=False).observe(now=NOW)
            self.assertTrue(restarted.complete)
            self.assertEqual(Decimal(restarted.daily_anchor_equity or "nan"), Decimal("1000"))
            self.assertEqual(Decimal(restarted.daily_pnl or "nan"), Decimal("-50"))
            self.assertEqual(restarted.daily_loss_state, "READY")
            anchor_path = root / "account-risk.day-anchor.json"
            self.assertTrue(anchor_path.exists())
            self.assertEqual(os.stat(anchor_path).st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(anchor_path.read_text(encoding="utf-8"))["scope"], "UTC_DAY_EQUITY_ANCHOR")

    def test_first_midday_start_is_persisted_unknown_not_treated_as_utc_day_start(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-day-anchor-midday-") as tmp:
            root = Path(tmp)
            observer = self._observer(root, allow_new_baseline=True)
            first = observer.observe(now=NOW)
            self.assertFalse(first.complete)
            self.assertIsNone(first.daily_pnl)
            self.assertEqual(first.daily_loss_state, "UNKNOWN")
            self.assertIn("day_anchor_start_unobserved", first.reasons)
            anchor_before = json.loads((root / "account-risk.day-anchor.json").read_text(encoding="utf-8"))

            self.client.balance = 99_000
            self.client.positions = [_position()]
            self.client.pnl = [_pnl(-5_000)]
            later = observer.observe(now=NOW)
            self.assertFalse(later.complete)
            self.assertIsNone(later.daily_pnl)
            anchor_after = json.loads((root / "account-risk.day-anchor.json").read_text(encoding="utf-8"))
            self.assertEqual(anchor_after["anchor_equity"], anchor_before["anchor_equity"])
            self.assertFalse(anchor_after["anchor_verified"])

    def test_continuous_boundary_uses_last_known_mark_and_survives_restart(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-day-anchor-boundary-") as tmp:
            root = Path(tmp)
            observer = self._observer(root, allow_new_baseline=True)
            before_boundary = NOW.replace(hour=23, minute=59, second=59)
            self.clock.value = before_boundary
            first = observer.observe(now=before_boundary)
            self.assertFalse(first.complete)
            self.assertEqual(first.daily_anchor_basis, "INITIAL_MARK_UNVERIFIED")

            after_boundary = before_boundary + timedelta(seconds=2)
            self.clock.value = after_boundary
            self.client.balance = 100_000
            self.client.positions = [_position()]
            self.client.pnl = [_pnl(500)]
            crossed = observer.observe(now=after_boundary)
            self.assertTrue(crossed.complete)
            self.assertEqual(crossed.daily_anchor_basis, "LAST_KNOWN_MARK_AT_UTC_BOUNDARY")
            self.assertEqual(Decimal(crossed.daily_anchor_equity or "nan"), Decimal("1000"))
            self.assertEqual(Decimal(crossed.daily_pnl or "nan"), Decimal("5"))
            self.assertEqual(Decimal(crossed.daily_anchor_mark_age_seconds or "nan"), Decimal("1"))
            self.assertEqual(crossed.daily_anchor_reference_at, before_boundary)

            restarted = self._observer(root, allow_new_baseline=False).observe(now=after_boundary)
            self.assertTrue(restarted.complete)
            self.assertEqual(restarted.daily_anchor_basis, "LAST_KNOWN_MARK_AT_UTC_BOUNDARY")
            self.assertEqual(Decimal(restarted.daily_pnl or "nan"), Decimal("5"))

    def test_boundary_reference_too_old_remains_unknown(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-day-anchor-boundary-gap-") as tmp:
            root = Path(tmp)
            observer = AccountRiskObserver(
                self.provider,
                clock=self.clock,
                max_age_seconds=5,
                state_path=root / "account-risk.json",
                journal_path=root / "intents.jsonl",
                allow_new_baseline=True,
                require_cashflows=True,
            )
            journal = root / "intents.jsonl"
            journal.write_text('{"journal_type":"event","event_type":"ACTIVATED"}\n', encoding="utf-8")
            os.chmod(journal, 0o600)
            before_boundary = NOW.replace(hour=23, minute=59, second=59)
            self.clock.value = before_boundary
            observer.observe(now=before_boundary)
            after_boundary = before_boundary + timedelta(seconds=10)
            self.clock.value = after_boundary
            snapshot = observer.observe(now=after_boundary)
            self.assertFalse(snapshot.complete)
            self.assertIsNone(snapshot.daily_pnl)
            self.assertEqual(snapshot.daily_loss_state, "UNKNOWN")
            self.assertEqual(snapshot.daily_anchor_basis, "BOUNDARY_MARK_TOO_OLD")

    def test_missing_cashflow_history_blocks_without_zero_daily_loss(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-day-anchor-missing-") as tmp:
            root = Path(tmp)
            observer = self._observer(root, allow_new_baseline=True)
            self.client.cashflow_payload_missing = True
            snapshot = observer.observe(now=NOW)
            self.assertFalse(snapshot.complete)
            self.assertFalse(snapshot.cashflows_complete)
            self.assertIsNone(snapshot.daily_pnl)
            self.assertIsNone(snapshot.daily_loss)
            self.assertIn("cashflow_list_incomplete", snapshot.reasons)

    def test_missing_anchor_without_explicit_baseline_blocks_and_does_not_reset(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-day-anchor-gate-") as tmp:
            root = Path(tmp)
            snapshot = self._observer(root, allow_new_baseline=False).observe(now=NOW)
            self.assertFalse(snapshot.complete)
            self.assertIsNone(snapshot.daily_anchor_equity)
            self.assertIsNone(snapshot.daily_loss)
            self.assertEqual(snapshot.daily_loss_state, "UNARMED")
            self.assertIn("day_anchor_baseline_required", snapshot.reasons)
            self.assertFalse((root / "account-risk.day-anchor.json").exists())

    def test_cashflow_history_regression_invalidates_existing_anchor(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-day-anchor-regression-") as tmp:
            root = Path(tmp)
            observer = self._observer(root, allow_new_baseline=True)
            observer.observe(now=NOW)
            self.client.cashflows = [_cashflow()]
            observer.observe(now=NOW)
            self.client.cashflows = []
            regressed = observer.observe(now=NOW)
            self.assertFalse(regressed.complete)
            self.assertIsNone(regressed.daily_pnl)
            self.assertIn("cashflow_history_regressed", regressed.reasons)


if __name__ == "__main__":
    unittest.main()
