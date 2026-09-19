"""Offline account-risk observations over generated cTrader Protobuf 91."""

from __future__ import annotations

import json
import os
import queue
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mtf_lab.data.ctrader_config import CTraderConfig
from mtf_lab.data.ctrader_protocol import WireMessage
from mtf_lab.data.ctrader_session import AuthenticatedSessionEvidence, CTraderClient
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.data.protobuf_generated import OpenApiModelMessages_pb2 as model
from mtf_lab.ops.ctrader_account_risk import AccountRiskObservationError, AccountRiskObserver

NOW = datetime(2026, 9, 13, 12, tzinfo=UTC)


def _position(position_id: int = 7, *, used_margin: int | None = 2500, money_digits: int | None = 2):
    position = model.ProtoOAPosition(positionId=position_id, positionStatus=1)
    position.tradeData.symbolId = 99
    position.tradeData.volume = 100
    position.tradeData.tradeSide = 1
    if used_margin is not None:
        position.usedMargin = used_margin
    if money_digits is not None:
        position.moneyDigits = money_digits
    return position


def _pnl(position_id: int = 7, *, gross: int = 1000, net: int = 900):
    return model.ProtoOAPositionUnrealizedPnL(
        positionId=position_id,
        grossUnrealizedPnL=gross,
        netUnrealizedPnL=net,
    )


def _deal(deal_id: int = 1, *, conversion_fee: int | None = 0):
    deal = model.ProtoOADeal(
        dealId=deal_id,
        orderId=10 + deal_id,
        positionId=7,
        volume=100,
        filledVolume=100,
        symbolId=99,
        createTimestamp=int(NOW.timestamp() * 1000),
        executionTimestamp=int(NOW.timestamp() * 1000),
        tradeSide=1,
        dealStatus=2,
    )
    detail = deal.closePositionDetail
    detail.entryPrice = 1.1
    detail.grossProfit = 1000
    detail.swap = -20
    detail.commission = -30
    detail.balance = 100950
    detail.moneyDigits = 2
    if conversion_fee is not None:
        detail.pnlConversionFee = conversion_fee
    return deal


class ControlledClient:
    """A synchronous same-client gateway returning generated messages."""

    def __init__(self) -> None:
        self.generation = 1
        self.response_at = NOW
        self.balance = 100000
        self.positions = [_position()]
        self.pnl = [_pnl()]
        self.deal_pages = [[_deal()]]
        self.calls: list[tuple[str, object]] = []
        self.response_generation: int | None = 1

    def authenticated_session_evidence(self):
        return {
            "account_id": "123",
            "session_id": "session-1",
            "connection_generation": str(self.generation),
            "environment": "DEMO",
            "endpoint": "demo.ctraderapi.com:5035",
        }

    def validate_session_evidence(self, _proof):
        return True

    def request_message(self, message, *, client_msg_id: str, timeout_seconds: float):
        del timeout_seconds
        name = type(message).__name__
        self.calls.append((name, message))
        if name == "ProtoOATraderReq":
            body = proto.ProtoOATraderRes(
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
            index = len([item for item in self.calls if item[0] == name]) - 1
            page = self.deal_pages[min(index, len(self.deal_pages) - 1)]
            body = proto.ProtoOADealListRes(
                ctidTraderAccountId=123,
                deal=page,
                hasMore=index < len(self.deal_pages) - 1,
            )
            payload_type = 2134
        else:  # pragma: no cover - the observer has a fixed request allowlist
            raise AssertionError(name)
        return WireMessage(
            payload_type,
            body,
            client_msg_id,
            received_at=self.response_at,
            available_at=self.response_at,
            connection_generation=self.response_generation,
        )


class SequencedResponseClient(ControlledClient):
    """Controlled gateway whose local receive metadata advances per response."""

    def __init__(self, response_times: list[datetime]) -> None:
        super().__init__()
        self.response_times = iter(response_times)

    def request_message(self, message: object, *, client_msg_id: str, timeout_seconds: float) -> WireMessage:
        self.response_at = next(self.response_times)
        return super().request_message(message, client_msg_id=client_msg_id, timeout_seconds=timeout_seconds)


class Provider:
    def __init__(self, client: ControlledClient) -> None:
        self.client = client


class LocalClientTransport:
    """Local transport that exercises CTraderClient's single reader path."""

    def __init__(self) -> None:
        self.connected = False
        self.inbound: queue.Queue[WireMessage] = queue.Queue()
        self.sent: list[WireMessage] = []
        self.server = ControlledClient()

    def connect(self, timeout=None) -> None:
        del timeout
        self.connected = True

    def close(self) -> None:
        self.connected = False

    def send(self, message: WireMessage) -> None:
        if not self.connected:
            raise RuntimeError("local risk transport disconnected")
        self.sent.append(message)
        response = self.server.request_message(
            message.payload,
            client_msg_id=str(message.client_msg_id),
            timeout_seconds=1,
        )
        self.inbound.put(response)

    def receive(self, timeout=None) -> WireMessage | None:
        if not self.connected:
            raise RuntimeError("local risk transport disconnected")
        try:
            return self.inbound.get(timeout=timeout or 0.01)
        except queue.Empty:
            return None


class ExecutorProbe:
    def __init__(self) -> None:
        self.kwargs = None

    def update_risk_metrics(self, **kwargs):
        self.kwargs = kwargs
        return {"updated": True}


class CTraderAccountRiskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ControlledClient()
        self.provider = Provider(self.client)

    def test_generated_messages_scale_account_pnl_and_margin_exactly(self) -> None:
        observer = AccountRiskObserver(self.provider, proto=proto, clock=lambda: NOW)
        snapshot = observer.observe(now=NOW)

        self.assertTrue(snapshot.fresh)
        self.assertTrue(snapshot.complete)
        self.assertEqual(snapshot.balance, "1000.00")
        self.assertEqual(snapshot.unrealized_daily_pnl, "9.00")
        self.assertEqual(snapshot.equity, "1009.00")
        self.assertEqual(snapshot.used_margin, "25.00")
        self.assertEqual(snapshot.margin_level, "4036.00")
        self.assertEqual(snapshot.realized_daily_pnl, "9.50")
        self.assertEqual(snapshot.daily_pnl, "18.50")
        self.assertEqual(
            [name for name, _message in self.client.calls],
            [
                "ProtoOATraderReq",
                "ProtoOAReconcileReq",
                "ProtoOAGetPositionUnrealizedPnLReq",
                "ProtoOADealListReq",
            ],
        )
        deal_request = self.client.calls[-1][1]
        self.assertEqual(deal_request.fromTimestamp, int(datetime(2026, 9, 13, tzinfo=UTC).timestamp() * 1000))
        self.assertEqual(deal_request.toTimestamp, int(NOW.timestamp() * 1000))
        self.assertEqual(deal_request.maxRows, 1000)

    def test_zero_used_margin_is_observed_without_infinite_ratio(self) -> None:
        self.client.positions = []
        self.client.pnl = []
        observer = AccountRiskObserver(self.provider, proto=proto, clock=lambda: NOW)
        snapshot = observer.observe(now=NOW)

        self.assertTrue(snapshot.complete)
        self.assertEqual(snapshot.used_margin, "0")
        self.assertIsNone(snapshot.margin_level)
        self.assertEqual(snapshot.margin_state, "NO_MARGIN_USED")
        self.assertIn("NO_MARGIN_USED", snapshot.reasons)
        self.assertNotIn("999", snapshot.to_dict().__repr__())
        self.assertEqual(snapshot.to_executor_kwargs()["used_margin"], Decimal("0"))

    def test_missing_conversion_fee_does_not_become_zero_realized_pnl(self) -> None:
        self.client.deal_pages = [[_deal(conversion_fee=None)]]
        observer = AccountRiskObserver(self.provider, proto=proto, clock=lambda: NOW)
        snapshot = observer.observe(now=NOW)

        self.assertFalse(snapshot.complete)
        self.assertFalse(snapshot.fees_complete)
        self.assertEqual(snapshot.realized_daily_gross_pnl, "10.00")
        self.assertIsNone(snapshot.realized_daily_pnl)
        self.assertIsNone(snapshot.daily_pnl)
        self.assertIn("realized_pnl_conversion_fee_unobserved", snapshot.reasons)

    def test_has_more_beyond_page_bound_is_incomplete(self) -> None:
        second = _deal(2)
        second.executionTimestamp -= 1_000
        self.client.deal_pages = [[_deal(1)], [second]]
        observer = AccountRiskObserver(self.provider, proto=proto, clock=lambda: NOW, max_pages=1)
        snapshot = observer.observe(now=NOW)

        self.assertFalse(snapshot.complete)
        self.assertFalse(snapshot.deals_complete)
        self.assertIn("deal_pagination_limit", snapshot.reasons)

    def test_stale_or_changed_generation_invalidates_executor_values(self) -> None:
        self.client.response_at = NOW - timedelta(seconds=11)
        observer = AccountRiskObserver(self.provider, proto=proto, clock=lambda: NOW, max_age_seconds=5)
        stale = observer.observe(now=NOW)
        self.assertFalse(stale.fresh)
        self.assertEqual(stale.freshness_state, "STALE")
        self.assertIsNone(stale.equity)
        self.assertIsNone(stale.unrealized_daily_pnl)
        self.assertIsNone(stale.to_executor_kwargs()["daily_pnl"])

        self.client.response_at = NOW
        self.client.response_generation = 2
        changed = observer.observe(now=NOW)
        self.assertFalse(changed.fresh)
        self.assertIn("connection_generation_changed", changed.reasons)
        self.assertIsNone(changed.to_executor_kwargs()["equity"])

    def test_default_clock_uses_terminal_instant_after_multiple_responses(self) -> None:
        response_times = [NOW + timedelta(seconds=offset) for offset in (0.10, 0.25, 0.50, 0.75)]
        client = SequencedResponseClient(response_times)
        clock_times = iter((NOW, NOW + timedelta(seconds=1)))
        observer = AccountRiskObserver(
            Provider(client),
            proto=proto,
            clock=lambda: next(clock_times),
            max_age_seconds=5,
        )

        snapshot = observer.observe()

        self.assertTrue(snapshot.fresh)
        self.assertTrue(snapshot.complete)
        self.assertEqual(snapshot.observed_at, response_times[-1])
        self.assertEqual(snapshot.day_end, NOW + timedelta(seconds=1))

    def test_explicit_now_rejects_a_real_future_response_and_does_not_sample_clock(self) -> None:
        self.client.response_at = NOW + timedelta(seconds=1)
        clock_calls = []

        def unexpected_clock_call():
            clock_calls.append(True)
            raise AssertionError("explicit now must bind the caller's reference")

        observer = AccountRiskObserver(
            self.provider,
            proto=proto,
            clock=unexpected_clock_call,
            max_age_seconds=5,
        )
        snapshot = observer.observe(now=NOW)

        self.assertFalse(snapshot.fresh)
        self.assertIn("response_stale:-1.000", snapshot.reasons)
        self.assertEqual(clock_calls, [])

    def test_default_clock_crossing_utc_midnight_does_not_relabel_prior_day_data(self) -> None:
        before_midnight = datetime(2026, 9, 13, 23, 59, 59, 500000, tzinfo=UTC)
        after_midnight = datetime(2026, 9, 14, 0, 0, 0, 500000, tzinfo=UTC)
        self.client.response_at = before_midnight + timedelta(milliseconds=100)
        clock_times = iter((before_midnight, after_midnight))
        observer = AccountRiskObserver(
            self.provider,
            proto=proto,
            clock=lambda: next(clock_times),
            max_age_seconds=5,
        )

        snapshot = observer.observe()

        self.assertFalse(snapshot.fresh)
        self.assertIn("utc_day_changed_during_observation", snapshot.reasons)
        self.assertEqual(snapshot.day_start, datetime(2026, 9, 13, tzinfo=UTC))
        self.assertEqual(snapshot.day_end, before_midnight)
        self.assertIsNone(snapshot.equity)

    def test_default_clock_marks_excessive_terminal_age_stale(self) -> None:
        self.client.response_at = NOW + timedelta(seconds=0.1)
        clock_times = iter((NOW, NOW + timedelta(seconds=6)))
        observer = AccountRiskObserver(
            self.provider,
            proto=proto,
            clock=lambda: next(clock_times),
            max_age_seconds=5,
        )

        snapshot = observer.observe()

        self.assertFalse(snapshot.fresh)
        self.assertIn("response_stale:5.900", snapshot.reasons)
        self.assertIsNone(snapshot.equity)

    def test_default_clock_regression_is_not_clamped_into_freshness(self) -> None:
        self.client.response_at = NOW - timedelta(seconds=0.25)
        clock_times = iter((NOW, NOW - timedelta(seconds=0.5)))
        observer = AccountRiskObserver(
            self.provider,
            proto=proto,
            clock=lambda: next(clock_times),
            max_age_seconds=5,
        )

        snapshot = observer.observe()

        self.assertFalse(snapshot.fresh)
        self.assertIn("observation_clock_regressed", snapshot.reasons)
        self.assertIn("response_stale:-0.250", snapshot.reasons)
        self.assertIsNone(snapshot.equity)

    def test_day_and_generation_are_cache_boundaries(self) -> None:
        observer = AccountRiskObserver(self.provider, proto=proto, clock=lambda: NOW)
        observer.observe(now=NOW)
        self.client.response_at = NOW + timedelta(days=1)
        next_day = NOW + timedelta(days=1)
        next_snapshot = observer.observe(now=next_day)

        self.assertTrue(next_snapshot.cache_invalidated)
        self.assertEqual(next_snapshot.cache_invalidation_reason, "utc_day_changed")

    def test_update_executor_withholds_incomplete_metrics(self) -> None:
        self.client.deal_pages = [[_deal(conversion_fee=None)]]
        observer = AccountRiskObserver(self.provider, proto=proto, clock=lambda: NOW)
        executor = ExecutorProbe()
        result = observer.update_executor(executor, now=NOW)

        self.assertTrue(result["observation"]["complete"] is False)
        self.assertIsNone(executor.kwargs["daily_pnl"])
        self.assertIsNone(executor.kwargs["realized_daily_pnl"])

    def test_real_client_single_reader_path_uses_local_controlled_transport(self) -> None:
        transport = LocalClientTransport()
        config = CTraderConfig(
            environment="demo",
            host="demo.ctraderapi.com",
            port=5035,
            symbol="EUR/USD",
            account_id=123,
            request_timeout_seconds=1,
            heartbeat_seconds=60,
        )
        client = CTraderClient(config, transport=transport, wall_clock=lambda: NOW)
        client.connect()
        client.mark_authenticated(123)
        client._session_evidence = AuthenticatedSessionEvidence(
            account_id=123,
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            scopes=frozenset({"accounts", "trading"}),
            session_id="local-session",
            connection_generation="1",
            authenticated_at=NOW,
        )
        try:
            snapshot = AccountRiskObserver(Provider(client), proto=proto, clock=lambda: NOW).observe(now=NOW)
            self.assertTrue(snapshot.fresh)
            self.assertTrue(snapshot.complete)
            self.assertEqual(snapshot.equity, "1009.00")
            self.assertEqual(
                [message.payload_type_id for message in transport.sent],
                [2121, 2124, 2187, 2133],
            )
        finally:
            client.close()

    def test_equity_high_water_is_observed_since_arm_and_survives_restart(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-risk-state-") as tmp:
            root = Path(tmp)
            state_path = root / "account-risk.json"
            journal_path = root / "intents.jsonl"
            journal_path.write_text('{"journal_type":"event","event_type":"ACTIVATED"}\n')
            os.chmod(journal_path, 0o600)
            observer = AccountRiskObserver(
                self.provider,
                proto=proto,
                clock=lambda: NOW,
                state_path=state_path,
                journal_path=journal_path,
                allow_new_baseline=True,
            )
            self.client.positions = []
            self.client.pnl = []
            armed = observer.observe(now=NOW)
            self.assertEqual(armed.equity, "1000.00")
            self.assertEqual(armed.drawdown, "0")
            self.assertTrue(armed.risk_state_complete)

            self.client.pnl = [_pnl(gross=-10000, net=-10000)]
            self.client.positions = [_position(used_margin=2500)]
            lower = observer.observe(now=NOW)
            self.assertEqual(lower.equity, "900.00")
            self.assertEqual(lower.high_water_equity, "1000.00")
            self.assertEqual(lower.drawdown, "100.00")

            restarted = AccountRiskObserver(
                self.provider,
                proto=proto,
                clock=lambda: NOW,
                state_path=state_path,
                journal_path=journal_path,
                allow_new_baseline=False,
            ).observe(now=NOW)
            self.assertEqual(restarted.high_water_equity, "1000.00")
            self.assertEqual(restarted.drawdown, "100.00")
            self.assertEqual(json.loads(state_path.read_text())["scope"], "OBSERVED_SINCE_ARM")
            self.assertEqual(os.stat(state_path).st_mode & 0o777, 0o600)

    def test_missing_or_corrupt_risk_state_with_existing_intents_never_resets(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-risk-state-invalid-") as tmp:
            root = Path(tmp)
            state_path = root / "account-risk.json"
            journal_path = root / "intents.jsonl"
            journal_path.write_text('{"journal_type":"intent"}\n')
            os.chmod(journal_path, 0o600)
            observer = AccountRiskObserver(
                self.provider,
                proto=proto,
                clock=lambda: NOW,
                state_path=state_path,
                journal_path=journal_path,
                allow_new_baseline=True,
            )
            missing = observer.observe(now=NOW)
            self.assertFalse(missing.risk_state_complete)
            self.assertIsNone(missing.drawdown)
            self.assertIn("risk_journal_nonempty", missing.reasons)
            self.assertFalse(state_path.exists())

            state_path.write_text("not-json")
            os.chmod(state_path, 0o600)
            corrupt = observer.observe(now=NOW)
            self.assertFalse(corrupt.risk_state_complete)
            self.assertIsNone(corrupt.drawdown)
            self.assertIn("risk_journal_nonempty", corrupt.reasons)
            self.assertEqual(state_path.read_text(), "not-json")

    def test_corrupt_state_with_empty_journal_requires_explicit_baseline_gate(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-risk-state-corrupt-") as tmp:
            root = Path(tmp)
            state_path = root / "account-risk.json"
            journal_path = root / "intents.jsonl"
            journal_path.touch()
            os.chmod(journal_path, 0o600)
            state_path.write_text("not-json")
            os.chmod(state_path, 0o600)
            observer = AccountRiskObserver(
                self.provider,
                proto=proto,
                clock=lambda: NOW,
                state_path=state_path,
                journal_path=journal_path,
                allow_new_baseline=False,
            )
            snapshot = observer.observe(now=NOW)
            self.assertFalse(snapshot.risk_state_complete)
            self.assertIsNone(snapshot.drawdown)
            self.assertIn("risk_state_corrupt_json", snapshot.reasons)
            self.assertEqual(state_path.read_text(), "not-json")

            state_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "scope": "OBSERVED_SINCE_ARM",
                        "peak_equity": "1000.00",
                        "armed_at": "2026-09-13T12:00:00Z",
                        "updated_at": "2026-09-13T12:00:00Z",
                    }
                )
            )
            os.chmod(state_path, 0o600)
            identity_missing = AccountRiskObserver(
                self.provider,
                proto=proto,
                clock=lambda: NOW,
                state_path=state_path,
                journal_path=journal_path,
                allow_new_baseline=False,
            ).observe(now=NOW)
            self.assertFalse(identity_missing.risk_state_complete)
            self.assertIsNone(identity_missing.drawdown)
            self.assertIn("risk_state_corrupt_fields", identity_missing.reasons)

    def test_corrupt_state_never_auto_resets_with_lifecycle_journal(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-risk-state-corrupt-lifecycle-") as tmp:
            root = Path(tmp)
            state_path = root / "account-risk.json"
            journal_path = root / "intents.jsonl"
            journal_path.write_text('{"journal_type":"event","event_type":"ACTIVATED"}\n')
            os.chmod(journal_path, 0o600)
            state_path.write_text("not-json")
            os.chmod(state_path, 0o600)
            before = state_path.read_bytes()
            snapshot = AccountRiskObserver(
                self.provider,
                proto=proto,
                clock=lambda: NOW,
                state_path=state_path,
                journal_path=journal_path,
                allow_new_baseline=True,
            ).observe(now=NOW)
            self.assertFalse(snapshot.risk_state_complete)
            self.assertIsNone(snapshot.drawdown)
            self.assertIn("risk_state_corrupt_json", snapshot.reasons)
            self.assertEqual(state_path.read_bytes(), before)

    def test_stale_observation_does_not_advance_high_water_or_report_zero_drawdown(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-risk-state-stale-") as tmp:
            root = Path(tmp)
            state_path = root / "account-risk.json"
            journal_path = root / "intents.jsonl"
            journal_path.touch()
            os.chmod(journal_path, 0o600)
            self.client.positions = []
            self.client.pnl = []
            observer = AccountRiskObserver(
                self.provider,
                proto=proto,
                clock=lambda: NOW,
                state_path=state_path,
                journal_path=journal_path,
                allow_new_baseline=True,
                max_age_seconds=5,
            )
            observer.observe(now=NOW)
            before = state_path.read_text()
            self.client.response_at = NOW - timedelta(seconds=6)
            stale = observer.observe(now=NOW)
            self.assertFalse(stale.fresh)
            self.assertIsNone(stale.drawdown)
            self.assertEqual(state_path.read_text(), before)

    def test_rearm_requires_explicit_human_confirmation(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-risk-state-rearm-") as tmp:
            root = Path(tmp)
            journal_path = root / "intents.jsonl"
            journal_path.touch()
            os.chmod(journal_path, 0o600)
            observer = AccountRiskObserver(
                self.provider,
                proto=proto,
                clock=lambda: NOW,
                state_path=root / "account-risk.json",
                journal_path=journal_path,
                allow_new_baseline=True,
            )
            with self.assertRaises(AccountRiskObservationError):
                observer.rearm_after_human()


if __name__ == "__main__":
    unittest.main()
