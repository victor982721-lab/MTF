"""Canonical synthetic DEMO canary flow for the scoped zero-spread opt-in."""

from __future__ import annotations

import dataclasses
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mtf_lab.core.canary_quote import CanaryZeroSpreadAuthorization
from mtf_lab.data.ctrader import WireMessage, synthetic_spot_event
from mtf_lab.data.ctrader_session import AuthenticatedSessionEvidence
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.data.protobuf_generated import OpenApiModelMessages_pb2 as model
from mtf_lab.ops.ctrader_canary_economics import ExitSlippageHypothesis, observe_canary_economics
from mtf_lab.ops.ctrader_canary_inputs import CanaryInputContext, collect_canary_inputs
from mtf_lab.ops.supervision import SupervisorStateStore
from mtf_lab.ops.supervision_demo import _update_executor_from_canary_economics, build_demo_execution_binding
from tests import test_ctrader_canary_economics as economics_fixtures
from tests.test_ctrader_canary_inputs import _FakeExecutor
from tests.test_demo_canary import (
    NOW,
    _CanonicalGatewayClient,
    _CanonicalProvider,
    _config,
)
from tools.demo_canary import (
    CanaryApproval,
    CanaryGateError,
    PreparedCanarySession,
    _approval_document_digest,
    run_demo_canary,
)


def _approval() -> CanaryApproval:
    return CanaryApproval(
        approved=True,
        approval_id="zero-spread-e2e",
        account_id="5097",
        symbol="EUR/USD",
        window_start=NOW - timedelta(minutes=10),
        window_end=NOW + timedelta(minutes=10),
        max_holding_seconds=Decimal("300"),
        max_quantity=Decimal("1000"),
        max_risk_fraction=Decimal("0.0005"),
        max_mutation_messages=6,
        canary_trial_anchor_authorized=True,
        canary_trial_anchor_authorization_source="Usuario: autorización DEMO trial anchor",
        canary_zero_spread_authorized=True,
        canary_zero_spread_authorization_source="Usuario: autorización DEMO zero spread",
    )


def _provenance() -> dict[str, object]:
    return {
        "network_performed": True,
        "source_mode": "DEMO_OBSERVED",
        "synthetic": False,
        "environment": "DEMO",
        "account_id": "5097",
        "account_selected": True,
        "account_verified": True,
        "endpoint": "demo.ctraderapi.com:5035",
        "symbol": "EUR/USD",
        "connection_generation": "1",
    }


class _AccountGatewayClient(_CanonicalGatewayClient):
    def __init__(self) -> None:
        super().__init__()
        self.open_requests: list[object] = []

    def _wire(self, name: str, payload: object, client_msg_id: str) -> WireMessage:
        del name
        return WireMessage(
            int(payload.payloadType),
            payload,
            client_msg_id,
            received_at=self.session.authenticated_at,
            available_at=self.session.authenticated_at,
            connection_generation=int(self.session.connection_generation),
        )

    def _filled_event(
        self,
        client_id: str,
        *,
        volume: int,
        position_id: int,
        close: bool = False,
        trade_side: int = 1,
        execution_price: float = 1.1002,
    ) -> object:
        event = super()._filled_event(
            client_id,
            volume=volume,
            position_id=position_id,
            close=close,
            trade_side=trade_side,
            execution_price=execution_price,
        )
        event.position.usedMargin = 0
        event.position.moneyDigits = 2
        if not close:
            event.position.tradeData.openTimestamp = int(self.session.authenticated_at.timestamp() * 1000)
        event.order.executionPrice = 1.1
        event.deal.executionPrice = 1.1
        event.position.price = 1.1
        return event

    def request_message(self, message: object, *, client_msg_id: str, timeout_seconds: float):
        name = type(message).__name__
        if name == "ProtoOATraderReq":
            body = proto.ProtoOATraderRes(
                ctidTraderAccountId=5097,
                trader=model.ProtoOATrader(
                    ctidTraderAccountId=5097,
                    depositAssetId=15,
                    balance=100_000,
                    moneyDigits=2,
                ),
            )
            return self._wire("ProtoOATraderRes", body, client_msg_id)
        if name == "ProtoOAAssetListReq":
            body = proto.ProtoOAAssetListRes(ctidTraderAccountId=5097)
            body.asset.add(assetId=5, name="EUR")
            body.asset.add(assetId=15, name="USD")
            return self._wire("ProtoOAAssetListRes", body, client_msg_id)
        if name == "ProtoOAExpectedMarginReq":
            body = proto.ProtoOAExpectedMarginRes(ctidTraderAccountId=5097, moneyDigits=2)
            body.margin.add(buyMargin=2000, sellMargin=2200)
            return self._wire("ProtoOAExpectedMarginRes", body, client_msg_id)
        if name == "ProtoOAGetPositionUnrealizedPnLReq":
            pnl = [
                model.ProtoOAPositionUnrealizedPnL(
                    positionId=position.positionId,
                    grossUnrealizedPnL=0,
                    netUnrealizedPnL=0,
                )
                for position in self.positions
            ]
            body = proto.ProtoOAGetPositionUnrealizedPnLRes(
                ctidTraderAccountId=5097,
                positionUnrealizedPnL=pnl,
                moneyDigits=2,
            )
            return self._wire("ProtoOAGetPositionUnrealizedPnLRes", body, client_msg_id)
        if name == "ProtoOADealListReq":
            body = proto.ProtoOADealListRes(ctidTraderAccountId=5097, deal=[], hasMore=False)
            return self._wire("ProtoOADealListRes", body, client_msg_id)
        if name == "ProtoOACashFlowHistoryListReq":
            body = proto.ProtoOACashFlowHistoryListRes(
                ctidTraderAccountId=5097,
                depositWithdraw=[],
            )
            return self._wire("ProtoOACashFlowHistoryListRes", body, client_msg_id)
        position_side = int(self.positions[0].tradeData.tradeSide) if self.positions else None
        wire = super().request_message(message, client_msg_id=client_msg_id, timeout_seconds=timeout_seconds)
        if name == "ProtoOANewOrderReq":
            self.open_requests.append(message)
            wire.payload.deal.tradeSide = message.tradeSide
            wire.payload.position.tradeData.tradeSide = message.tradeSide
        elif name == "ProtoOAClosePositionReq" and position_side is not None:
            wire.payload.deal.tradeSide = 2 if position_side == 1 else 1
            wire.payload.position.tradeData.tradeSide = position_side
        return wire


class _ZeroSpreadCanonicalProvider(_CanonicalProvider):
    def __init__(self) -> None:
        super().__init__()
        self.client = _AccountGatewayClient()
        # Reuse the complete catalog fixture rather than assuming broker
        # identity, volume grid, commission units or financing metadata.
        self.catalog.selected.metadata = dict(economics_fixtures.FakeProvider().catalog.selected.metadata)
        self.catalog.selected.metadata["minVolume"] = 100_000
        self.zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None

    def set_canary_zero_spread_authorization(self, authorization: CanaryZeroSpreadAuthorization) -> None:
        if not authorization.matches(
            account_id="5097",
            symbol="EUR/USD",
            session_id="s1",
            connection_generation="1",
            endpoint="demo.ctraderapi.com:5035",
            now=NOW,
            require_order_window=False,
        ):
            raise AssertionError("zero-spread fixture identity mismatch")
        self.zero_spread_authorization = authorization

    @property
    def canary_zero_spread_authorization(self) -> CanaryZeroSpreadAuthorization | None:
        return self.zero_spread_authorization

    def snapshot_quote_state(self) -> dict[str, object]:
        leg = {
            "price": "1.1000",
            "event_time": (NOW - timedelta(seconds=1)).isoformat(),
            "available_at": NOW.isoformat(),
            "generation": "1",
            "sequence": 1,
            "state": "VALID",
        }
        return {"symbols": {"11": {"bid": dict(leg), "ask": dict(leg)}}}


def _zero_authorization(approval: CanaryApproval) -> CanaryZeroSpreadAuthorization:
    return CanaryZeroSpreadAuthorization(
        approval_digest=_approval_document_digest(approval),
        authorization_source=str(approval.canary_zero_spread_authorization_source),
        account_id="5097",
        session_id="s1",
        connection_generation="1",
        endpoint="demo.ctraderapi.com:5035",
        symbol="EUR/USD",
        preparation_start=approval.window_start - timedelta(minutes=5),
        window_start=approval.window_start,
        window_end=approval.window_end,
    )


def _execution_time(context: CanaryInputContext) -> datetime:
    runtime = context.runtime_snapshot
    timestamps = [
        datetime.fromisoformat(str(runtime[name]).replace("Z", "+00:00"))
        for name in ("last_available", "latest_trigger_available_at")
    ]
    return max((NOW, *timestamps))


def _aligned_input_context(
    authorization: CanaryZeroSpreadAuthorization,
    *,
    crossed: bool = False,
    preparation_start: datetime | None = None,
):
    import tests.test_ctrader_canary_inputs as input_module

    fixture = input_module.CanaryInputTests("test_ready_context_uses_runtime_profile_and_manual_directions")
    with patch.object(input_module, "NOW", NOW):
        fixture.setUp()
        try:
            proof = AuthenticatedSessionEvidence(
                account_id=7,
                environment="DEMO",
                endpoint="demo.ctraderapi.com:5035",
                scopes=frozenset({"accounts", "trading"}),
                session_id="typed-session",
                connection_generation=str(fixture.provider.generation),
                authenticated_at=authorization.preparation_start,
            )
            proof = dataclasses.replace(
                proof,
                account_id=5097,
                session_id="s1",
            )
            fixture.provider.client._session_evidence = proof
            fixture.provider.client._authenticated_account_id = 5097
            fixture.provider.config = dataclasses.replace(fixture.provider.config, account_id=5097)
            fixture.evidence = dataclasses.replace(
                fixture.evidence,
                account_id="5097",
                session_id="s1",
                authenticated_at=authorization.preparation_start,
            )
            clock_value = [authorization.preparation_start + timedelta(seconds=5)]
            fixture.provider._clock = lambda: clock_value[0]
            fixture.provider.client._wall_clock = lambda: clock_value[0]
            if authorization is not None:
                fixture.provider.set_canary_zero_spread_authorization(authorization)
            capture_start = preparation_start or authorization.preparation_start
            order_start = NOW - timedelta(minutes=10)
            order_end = NOW + timedelta(minutes=10)
            for index in range(91):
                event_time = capture_start + timedelta(seconds=5 + index * 10)
                bid = 110000 + index
                ask = bid - 20 if crossed else bid
                payload = synthetic_spot_event(
                    timestamp_ms=int(event_time.timestamp() * 1000),
                    symbol_id=99,
                    bid_relative=bid,
                    ask_relative=ask,
                )
                payload.pop("synthetic_fixture", None)
                fixture.transport.push(
                    WireMessage(
                        "PROTO_OA_SPOT_EVENT",
                        payload,
                        is_event=True,
                        received_at=event_time + timedelta(seconds=1),
                        available_at=event_time + timedelta(seconds=1),
                        ingest_sequence=index,
                        connection_generation=fixture.provider.generation,
                    )
                )
            clock_value[0] = capture_start + timedelta(seconds=5)
            original_poll = fixture.provider.client.poll_event

            def poll_event(timeout_seconds: float):
                message = original_poll(timeout_seconds)
                if message is not None and message.available_at is not None:
                    clock_value[0] = message.available_at
                return message

            fixture.provider.client.poll_event = poll_event
            with tempfile.TemporaryDirectory(prefix="mtf-zero-spread-inputs-") as directory:
                root = Path(directory)
                with patch.dict(
                    os.environ,
                    {"HOME": str(root / "home"), "XDG_STATE_HOME": str(root / "xdg")},
                    clear=False,
                ):
                    (root / "home").mkdir(mode=0o700)
                    (root / "xdg").mkdir(mode=0o700)
                    result = collect_canary_inputs(
                        fixture.provider,
                        fixture._load_toml_canary_config(),
                        network=True,
                        deadline=order_end,
                        max_events=200,
                        window_start=order_start,
                        window_end=order_end,
                        preparation_start=capture_start,
                        session_evidence=fixture.evidence,
                        executor=_FakeExecutor(),
                        warmup=None,
                        fetch_warmup=False,
                        technical_only=True,
                        zero_spread_authorization=authorization,
                        market_window_state=lambda *_args: "OPEN",
                        isolated_state_dir=root,
                        clock=lambda: clock_value[0],
                    )
            return result
        finally:
            fixture.tearDown()


class CanaryZeroSpreadFlowTests(unittest.TestCase):
    def test_authorized_zero_spread_runs_observed_atr14_manual_two_cycles(self) -> None:
        approval = _approval()
        authorization = _zero_authorization(approval)
        input_result = _aligned_input_context(authorization)
        self.assertTrue(input_result.ok, input_result.reason)
        assert input_result.context is not None
        self.assertTrue(input_result.context.technical_only)
        self.assertEqual(input_result.context.bbo.bid, input_result.context.bbo.ask)
        self.assertIs(input_result.context.zero_spread_authorization, authorization)
        self.assertEqual(input_result.context.runtime_snapshot["trigger_timeframe"], "M1")
        self.assertTrue(input_result.context.runtime_snapshot["atr"])

        execution_time = _execution_time(input_result.context)
        import tests.test_demo_canary as demo_module

        with patch.object(demo_module, "NOW", execution_time), patch(f"{__name__}.NOW", execution_time):
            provider = _ZeroSpreadCanonicalProvider()
            provider.set_canary_zero_spread_authorization(authorization)
            provenance = _provenance()
            config = _config()
            with tempfile.TemporaryDirectory(prefix="mtf-zero-spread-flow-") as directory:
                root = Path(directory)
                store = SupervisorStateStore(root, "zero-spread-flow")
                binding = build_demo_execution_binding(
                    provider,
                    provenance,
                    config=config,
                    state_dir=root,
                    resume=True,
                    canary_economics=None,
                    proto=proto,
                    clock=lambda: execution_time,
                    defer_activation=True,
                    zero_spread_authorization=authorization,
                )
                try:
                    snapshot = binding.risk_observer.observe()
                    self.assertTrue(snapshot.account_complete, snapshot.reasons)
                    projection = observe_canary_economics(
                        provider,
                        snapshot,
                        window_start=approval.window_start,
                        window_end=approval.window_end,
                        now=execution_time,
                        exit_slippage_hypothesis=ExitSlippageHypothesis(),
                        zero_spread_authorization=authorization,
                        proto=proto,
                    )
                    binding.canary_economics = projection
                    _update_executor_from_canary_economics(binding.executor, projection, "CONSERVATIVE")
                    session = PreparedCanarySession(
                        provider,
                        config,
                        provenance,
                        binding,
                        SimpleNamespace(close=lambda: None),
                        store,
                        defer_activation=True,
                        zero_spread_authorization=authorization,
                    )

                    result = run_demo_canary(
                        provider,
                        provenance,
                        config,
                        state_dir=root,
                        approval=approval,
                        inputs=input_result.context,
                        execute=True,
                        writer_store=store,
                        economics_refresh=lambda _executor, _observed: observe_canary_economics(
                            provider,
                            binding.risk_observer.last_observation,
                            window_start=approval.window_start,
                            window_end=approval.window_end,
                            now=execution_time,
                            exit_slippage_hypothesis=ExitSlippageHypothesis(),
                            zero_spread_authorization=authorization,
                            proto=proto,
                        ),
                        market_window_state=lambda *_args: "OPEN",
                        clock=lambda: execution_time,
                        prepared_session=session,
                    )
                    self.assertTrue(result.ok)
                    self.assertEqual(result.state, "CANARY_COMPLETED")
                    self.assertEqual(result.mutation_messages, 4)
                    self.assertEqual(provider.client.order_count, 2)
                    self.assertEqual([request.tradeSide for request in provider.client.open_requests], [1, 2])
                    for request in provider.client.open_requests:
                        self.assertEqual(request.volume, 100_000)
                        self.assertTrue(request.clientOrderId)
                        self.assertFalse(request.HasField("stopLoss"))
                        self.assertFalse(request.HasField("takeProfit"))
                        self.assertTrue(request.HasField("relativeStopLoss"))
                        self.assertTrue(request.HasField("relativeTakeProfit"))
                        self.assertGreater(request.relativeStopLoss, 0)
                        self.assertGreater(request.relativeTakeProfit, 0)
                    self.assertEqual(len({request.clientOrderId for request in provider.client.open_requests}), 2)
                    self.assertEqual([request.volume for request in provider.client.open_requests], [100_000, 100_000])
                    self.assertEqual(
                        [
                            (request.relativeStopLoss, request.relativeTakeProfit)
                            for request in provider.client.open_requests
                        ],
                        [
                            (
                                provider.client.open_requests[0].relativeStopLoss,
                                provider.client.open_requests[0].relativeTakeProfit,
                            )
                        ]
                        * 2,
                    )
                    open_events = [event for event in provider.client.filled_events if not event.order.closingOrder]
                    self.assertEqual(len(open_events), 2)
                    self.assertEqual([event.deal.tradeSide for event in open_events], [1, 2])
                    events_by_client_id = {event.order.clientOrderId: event for event in open_events}
                    for request in provider.client.open_requests:
                        event = events_by_client_id[request.clientOrderId]
                        fill_price = Decimal(str(event.order.executionPrice))
                        stop_delta = Decimal(request.relativeStopLoss) / Decimal(100_000)
                        target_delta = Decimal(request.relativeTakeProfit) / Decimal(100_000)
                        if event.deal.tradeSide == 1:
                            self.assertEqual(Decimal(str(event.order.stopLoss)), fill_price - stop_delta)
                            self.assertEqual(Decimal(str(event.order.takeProfit)), fill_price + target_delta)
                        else:
                            self.assertEqual(Decimal(str(event.order.stopLoss)), fill_price + stop_delta)
                            self.assertEqual(Decimal(str(event.order.takeProfit)), fill_price - target_delta)
                        self.assertEqual(event.position.stopLoss, event.order.stopLoss)
                        self.assertEqual(event.position.takeProfit, event.order.takeProfit)
                    self.assertEqual(len(provider.client.positions), 0)
                    self.assertIs(binding.executor.canary_zero_spread_authorization, authorization)
                    self.assertFalse(binding.risk_observer.last_observation.complete)
                    self.assertIsNone(binding.risk_observer.last_observation.daily_loss)
                finally:
                    binding.close()

                ledger = json.loads((root / "canary-approval-ledger.json").read_text(encoding="utf-8"))
                record = next(iter(ledger["approvals"].values()))
                self.assertEqual(record["state"], "COMPLETED")
                self.assertEqual(record["mutation_messages"], 4)
                self.assertEqual(record["canary_zero_spread_authorized"], True)
                self.assertEqual(record["canary_trial_anchor_authorized"], True)

    def test_missing_opt_in_crossed_and_context_mismatch_never_reach_orders(self) -> None:
        approval = _approval()
        authorization = _zero_authorization(approval)
        crossed = _aligned_input_context(authorization, crossed=True)
        self.assertFalse(crossed.ok)
        self.assertFalse(crossed.gates["orders_attempted"])
        mismatched = _aligned_input_context(
            authorization,
            preparation_start=authorization.window_start,
        )
        self.assertFalse(mismatched.ok)
        self.assertFalse(mismatched.gates["orders_attempted"])

        missing = _aligned_input_context(authorization)
        self.assertTrue(missing.ok)
        provider = _ZeroSpreadCanonicalProvider()
        provenance = _provenance()
        config = _config()
        with tempfile.TemporaryDirectory(prefix="mtf-zero-spread-negative-") as directory:
            root = Path(directory)
            store = SupervisorStateStore(root, "zero-spread-negative")
            binding = build_demo_execution_binding(
                provider,
                provenance,
                config=config,
                state_dir=root,
                proto=proto,
                clock=lambda: NOW,
                defer_activation=True,
            )
            try:
                with self.assertRaises(CanaryGateError):
                    run_demo_canary(
                        provider,
                        provenance,
                        config,
                        state_dir=root,
                        approval=dataclasses.replace(
                            approval,
                            canary_zero_spread_authorized=False,
                            canary_zero_spread_authorization_source=None,
                        ),
                        inputs=missing.context,
                        execute=True,
                        writer_store=store,
                        market_window_state=lambda *_args: "OPEN",
                        clock=lambda: NOW,
                        prepared_session=PreparedCanarySession(
                            provider,
                            config,
                            provenance,
                            binding,
                            SimpleNamespace(close=lambda: None),
                            store,
                            defer_activation=True,
                        ),
                    )
                self.assertEqual(provider.client.order_count, 0)
                self.assertEqual(len(provider.client.positions), 0)
            finally:
                binding.close()


if __name__ == "__main__":
    unittest.main()
