from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from mtf_lab.data.ctrader import WireMessage
from mtf_lab.ops.ctrader_demo_transport import PAYLOAD_TYPES, AuthenticatedSession
from mtf_lab.ops.ctrader_executor import OrderState
from mtf_lab.ops.supervision import SupervisorStateStore
from mtf_lab.ops.supervision_demo import build_demo_execution_binding
from mtf_lab.ops.volume_rules import VolumeGrid
from tools.demo_canary import (
    CanaryApproval,
    CanaryGateError,
    CanaryInputs,
    PreparedCanarySession,
    derive_canary_quantity,
    main,
    run_demo_canary,
    static_cli_preflight,
)

try:
    from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
except Exception:  # pragma: no cover - optional generated runtime
    proto = None

NOW = datetime(2026, 9, 19, 14, 0, tzinfo=UTC)


def _policy() -> SimpleNamespace:
    return SimpleNamespace(max_quantity=Decimal("1000"), max_price_age_seconds=Decimal("30"))


def _config() -> SimpleNamespace:
    from mtf_lab.core.risk_exit import RiskExitPolicy

    return SimpleNamespace(
        instrument="EUR/USD",
        ctrader={
            "enabled": True,
            "environment": "DEMO",
            "account_id": "5097",
            "account_selected": True,
            "required_scopes": ["accounts", "trading"],
        },
        execution={
            "enabled": True,
            "environment": "DEMO",
            "endpoint": "demo.ctraderapi.com:5035",
            "account_id": "5097",
            "scope": "trading",
            "max_quantity": "1000",
            "fixed_quantity": "1000",
            "max_exposure": "2000",
            "max_positions": 1,
            "max_inflight_intents": 1,
            "max_daily_loss": "5",
            "max_drawdown": "10",
            "min_margin_level": "100",
            "max_holding_seconds": "300",
            "require_protective_stops": True,
            "market_candidate_id": "tp_fast_v1",
            "risk_exit_policy": RiskExitPolicy(
                planned_risk_fraction=Decimal("0.0005"), equity_basis="OBSERVED_DEMO"
            ).to_dict(),
            "risk_calendar": {"known": True, "basis": "OBSERVED_BROKER"},
        },
    )


def _approval(*, approved: bool = False) -> CanaryApproval:
    return CanaryApproval(
        approved=approved,
        approval_id="approval-test-1",
        account_id="5097",
        symbol="EUR/USD",
        window_start=NOW - timedelta(minutes=1),
        window_end=NOW + timedelta(minutes=10),
        max_holding_seconds=Decimal("300"),
        max_quantity=Decimal("1000"),
        max_risk_fraction=Decimal("0.0005"),
        max_mutation_messages=6,
    )


def _inputs() -> CanaryInputs:
    runtime = {
        "runtime_state": "VALID",
        "market_candidate_id": "tp_fast_v1",
        "trigger_timeframe": "M1",
        "data_mode": "LIVE",
        "risk_bar_clock_basis": "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION",
        "trigger_bar_count": 1,
        "latest_trigger_end": NOW.isoformat(),
        "latest_trigger_available_at": NOW.isoformat(),
        "last_market": NOW.isoformat(),
        "last_available": NOW.isoformat(),
        "atr": "0.001",
    }
    return CanaryInputs.from_observed_runtime(
        runtime,
        runtime,
        instrument="EUR/USD",
        buy_signal_id="buy-1",
        sell_signal_id="sell-1",
    )


class FakeProvider:
    spec = SimpleNamespace(symbol="EUR/USD", symbol_id=11)
    generation = "g1"
    config = SimpleNamespace(host="demo.ctraderapi.com", port=5035)

    def snapshot_quote_state(self):
        return {
            "symbols": {
                "11": {
                    "bid": {
                        "price": "1.1000",
                        "event_time": (NOW - timedelta(seconds=1)).isoformat(),
                        "available_at": NOW.isoformat(),
                        "generation": "g1",
                        "sequence": 1,
                    },
                    "ask": {
                        "price": "1.1002",
                        "event_time": (NOW - timedelta(seconds=1)).isoformat(),
                        "available_at": NOW.isoformat(),
                        "generation": "g1",
                        "sequence": 1,
                    },
                }
            }
        }


class _CanonicalGatewayClient:
    """In-process CTraderClient-shaped gateway for the canonical E2E test."""

    def __init__(self) -> None:
        assert proto is not None
        self.session = AuthenticatedSession(
            "s1",
            "5097",
            "DEMO",
            "demo.ctraderapi.com:5035",
            {"accounts", "trading"},
            NOW,
            "1",
            NOW + timedelta(hours=1),
        )
        self.positions: list[Any] = []
        self.order_count = 0
        self.calls: list[str] = []
        self.filled_events: list[Any] = []

    def authenticated_session_evidence(self) -> AuthenticatedSession:
        return self.session

    def validate_session_evidence(self, proof: Any) -> bool:
        return proof is self.session

    def _wire(self, name: str, payload: Any, client_msg_id: str) -> WireMessage:
        return WireMessage(
            PAYLOAD_TYPES[name],
            payload,
            client_msg_id,
            received_at=NOW,
            available_at=NOW,
            connection_generation=1,
        )

    @staticmethod
    def _filled_event(
        client_id: str,
        *,
        volume: int,
        position_id: int,
        close: bool = False,
        trade_side: int = 1,
        execution_price: float = 1.1002,
    ) -> Any:
        assert proto is not None
        event = proto.ProtoOAExecutionEvent(ctidTraderAccountId=5097, executionType=3)
        event.order.orderId = 7 + position_id
        event.order.clientOrderId = client_id
        event.order.orderStatus = 2
        event.order.executedVolume = volume
        event.order.executionPrice = execution_price
        event.order.tradeData.symbolId = 11
        event.order.tradeData.volume = volume
        event.order.tradeData.tradeSide = trade_side
        event.order.tradeData.label = client_id
        event.deal.dealId = 8 + position_id
        event.deal.orderId = 7 + position_id
        event.deal.positionId = position_id
        event.deal.volume = volume
        event.deal.filledVolume = volume
        event.deal.executionTimestamp = int(NOW.timestamp() * 1000)
        event.deal.executionPrice = execution_price
        event.deal.tradeSide = trade_side
        event.deal.dealStatus = 2
        event.position.positionId = position_id
        event.position.positionStatus = 2 if close else 1
        event.position.price = execution_price
        event.position.tradeData.symbolId = 11
        event.position.tradeData.volume = volume
        event.position.tradeData.tradeSide = trade_side
        event.position.tradeData.label = client_id
        if not close:
            event.position.tradeData.openTimestamp = int(NOW.timestamp() * 1000)
        if close:
            event.order.closingOrder = True
            event.order.ClearField("clientOrderId")
        return event

    def request_message(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> WireMessage:
        del timeout_seconds
        name = type(message).__name__
        self.calls.append(name)
        if name == "ProtoOAReconcileReq":
            assert proto is not None
            return self._wire(
                "ProtoOAReconcileRes",
                proto.ProtoOAReconcileRes(ctidTraderAccountId=5097, position=list(self.positions)),
                client_msg_id,
            )
        if name == "ProtoOANewOrderReq":
            self.order_count += 1
            position_id = 100 + self.order_count
            if int(message.orderType) == 1:
                if message.HasField("stopLoss") or message.HasField("takeProfit"):
                    raise AssertionError("el servidor MARKET no acepta stopLoss/takeProfit absolutos")
                if not message.HasField("relativeStopLoss") or not message.HasField("relativeTakeProfit"):
                    raise AssertionError("el servidor MARKET requiere protecciones relativas")
                relative_stop_loss = int(message.relativeStopLoss)
                relative_take_profit = int(message.relativeTakeProfit)
                if relative_stop_loss <= 0 or relative_take_profit <= 0:
                    raise AssertionError("el servidor MARKET requiere distancias relativas positivas")
                trade_side = int(message.tradeSide)
                execution_price = 1.1002 if trade_side == 1 else 1.1000
                event = self._filled_event(
                    client_msg_id,
                    volume=int(message.volume),
                    position_id=position_id,
                    trade_side=trade_side,
                    execution_price=execution_price,
                )
                # The server reports the protections as absolute prices on the
                # filled order/position, derived from the observed fill, not
                # from the caller's theoretical entry quote.
                fill_price = Decimal(str(event.order.executionPrice))
                stop_delta = Decimal(relative_stop_loss) / Decimal(100_000)
                target_delta = Decimal(relative_take_profit) / Decimal(100_000)
                if trade_side == 1:
                    stop_loss = fill_price - stop_delta
                    take_profit = fill_price + target_delta
                else:
                    stop_loss = fill_price + stop_delta
                    take_profit = fill_price - target_delta
                event.order.relativeStopLoss = relative_stop_loss
                event.order.relativeTakeProfit = relative_take_profit
                event.order.stopLoss = float(stop_loss)
                event.order.takeProfit = float(take_profit)
                event.position.stopLoss = float(stop_loss)
                event.position.takeProfit = float(take_profit)
            else:
                event = self._filled_event(client_msg_id, volume=int(message.volume), position_id=position_id)
            self.positions = [event.position]
            self.filled_events.append(event)
            return self._wire("ProtoOAExecutionEvent", event, client_msg_id)
        if name == "ProtoOAClosePositionReq":
            position_id = int(message.positionId)
            event = self._filled_event(client_msg_id, volume=int(message.volume), position_id=position_id, close=True)
            self.positions = []
            self.filled_events.append(event)
            return self._wire("ProtoOAExecutionEvent", event, client_msg_id)
        raise AssertionError(f"gateway E2E recibió mensaje inesperado: {name}")


class _CanonicalProvider:
    name = "ctrader_open_api"
    generation = "1"

    def __init__(self) -> None:
        self.client = _CanonicalGatewayClient()
        self.spec = SimpleNamespace(symbol="EUR/USD", symbol_id=11)
        selected = SimpleNamespace(
            name="EUR/USD",
            symbol_id=11,
            metadata={
                "fullSymbol": {
                    "symbolId": 11,
                    "minVolume": 100,
                    "maxVolume": 100_000,
                    "stepVolume": 100,
                    "digits": 5,
                    "pipPosition": 4,
                    "priceScale": 100_000,
                }
            },
        )
        self.catalog = SimpleNamespace(selected=selected, symbols=(selected,), requested_symbol="EUR/USD")

    def snapshot_quote_state(self) -> dict[str, Any]:
        leg = {
            "price": "1.1000",
            "event_time": NOW.isoformat(),
            "available_at": NOW.isoformat(),
            "generation": "1",
            "sequence": 1,
        }
        ask = dict(leg)
        ask["price"] = "1.1002"
        return {"symbols": {"11": {"bid": leg, "ask": ask}}}


class _CanonicalEconomics:
    quantity = Decimal("1")
    connection_generation = "1"
    observed_at = NOW
    calendar_state = {"known": True, "basis": "OBSERVED_BROKER", "financing_known": True}
    provenance = {"connection_generation": "1"}
    buy_margin_required = Decimal("10")
    sell_margin_required = Decimal("10")
    buy_margin_level = Decimal("1000")
    sell_margin_level = Decimal("1000")

    def required_margin(self, side: str) -> Decimal:
        del side
        return Decimal("10")

    def projected_margin_level(self, side: str) -> Decimal:
        del side
        return Decimal("1000")

    def to_update_risk_kwargs(self, side: str) -> dict[str, Any]:
        del side
        return {
            "equity": Decimal("10000"),
            "margin_available": Decimal("10000"),
            "margin_required": Decimal("10"),
            "margin_level": Decimal("1000"),
            "used_margin": Decimal("0"),
            "observed_at": NOW,
            "connection_generation": "1",
        }

    def to_contract_spec(self, side: str) -> dict[str, Any]:
        del side
        return {
            "known": True,
            "pip_size": "0.0001",
            "quantity_min": "1",
            "quantity_step": "1",
            "quantity_max": "1000",
            "unit_value": "1",
            "minimum_stop_distance": "0.00001",
            "margin_per_unit": "10",
            "fees_known": True,
            "spread_known": True,
            "expected_commission_fixed": "0",
            "expected_commission_per_unit": "0",
            "expected_exit_slippage_pips": "0.1",
            "expected_cost_currency": "USD",
            "expected_cost_source": "IMPLEMENTATION_HYPOTHESIS:baseline-0.1pip",
            "executable_bid": "1.1000",
            "executable_ask": "1.1002",
            "price_quantum": "0.00001",
        }


def _binding() -> SimpleNamespace:  # noqa: C901
    class Executor:
        def __init__(self) -> None:
            self.policy = _policy()
            self.submit_calls = 0
            self.close_calls = 0
            self.open_position: str | None = None
            self.deactivated = False
            self.active = True

        def status(self, *, refresh: bool = False):
            del refresh
            return {
                "risk": {
                    "position_state": "VALID",
                    "positions_fresh": True,
                    "metrics_observed_at": NOW.isoformat(),
                    "metrics_connection_generation": "g1",
                    "metrics": {"equity": "10000"},
                    "risk_exit": {
                        "metrics": {
                            "daily_anchor_verified": True,
                            "daily_loss_state": "READY",
                            "daily_anchor_equity": "10000",
                            "daily_cashflow_total": "0",
                            "cashflows_complete": True,
                        }
                    },
                }
            }

        def observe_runtime(self, snapshot):
            if snapshot.get("runtime_state") != "VALID":
                raise CanaryGateError("runtime snapshot inválido")
            return snapshot

        def risk_entry_plan(self, signal, quote, *, requested_quantity):
            direction = str(signal.get("direction", "")).upper()
            atr = Decimal(str(signal["atr"]))
            entry = quote.ask if direction == "BUY" else quote.bid
            stop = entry - atr * Decimal("1.5") if direction == "BUY" else entry + atr * Decimal("1.5")
            target = entry + atr * Decimal("3") if direction == "BUY" else entry - atr * Decimal("3")
            return SimpleNamespace(
                allowed=True,
                eligible_for_demo=True,
                mode="DEMO_GATED",
                quantity=Decimal(str(requested_quantity)),
                initial_stop=stop,
                take_profit=target,
                risk_envelope_known=True,
                expected_total_loss_at_stop=Decimal("4"),
                risk_budget=Decimal("5"),
            )

        def reconcile_positions(self):
            owned = []
            if self.open_position is not None:
                owned = [{"position_id": self.open_position}]
            return {"state": "VALID", "foreign_positions": [], "owned_positions": owned}

        def submit_signal(self, signal, quote):
            del quote
            self.submit_calls += 1
            self.open_position = f"position-{self.submit_calls}"
            intent = SimpleNamespace(intent_id=str(signal["signal_id"]), quantity=Decimal("1000"))
            return SimpleNamespace(
                state=OrderState.FILLED,
                filled_quantity=Decimal("1000"),
                intent=intent,
                position_ids=(self.open_position,),
            )

        def reconcile(self, intent_id):
            del intent_id
            raise AssertionError("known FILLED result must not reconcile again")

        def close_position(self, position_id):
            self.close_calls += 1
            if position_id != self.open_position:
                raise AssertionError("unexpected position")
            self.open_position = None
            intent = SimpleNamespace(intent_id=f"close-{position_id}", quantity=Decimal("1000"))
            return SimpleNamespace(state=OrderState.CLOSED, intent=intent)

        def deactivate(self, reason):
            del reason
            self.deactivated = True
            self.active = False

    return SimpleNamespace(
        executor=Executor(),
        transport=SimpleNamespace(volume_grid=VolumeGrid(100_000, 1_000_000, 100_000)),
        observation=SimpleNamespace(
            session_id="s1",
            account_id="5097",
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            symbol="EUR/USD",
            connection_generation="g1",
            scopes=frozenset({"accounts", "trading"}),
        ),
        risk_observer=None,
        close=lambda: None,
    )


class DemoCanaryTests(unittest.TestCase):
    def test_quantity_is_derived_from_observed_grid_not_lot_literal(self) -> None:
        grid = VolumeGrid(100_000, 1_000_000, 100_000)
        self.assertEqual(derive_canary_quantity(grid, Decimal("1000")), Decimal("1000"))
        with self.assertRaises(CanaryGateError):
            derive_canary_quantity(VolumeGrid(200_000, 1_000_000, 100_000), Decimal("1000"))

    def test_default_preflight_never_submits_or_closes(self) -> None:
        binding = _binding()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("tools.demo_canary.build_demo_execution_binding", return_value=binding),
        ):
            result = run_demo_canary(
                FakeProvider(),
                {
                    "network_performed": True,
                    "source_mode": "DEMO_OBSERVED",
                    "synthetic": False,
                    "environment": "DEMO",
                    "account_id": "5097",
                    "account_selected": True,
                    "account_verified": True,
                    "endpoint": "demo.ctraderapi.com:5035",
                },
                _config(),
                state_dir=Path(directory),
                approval=_approval(),
                inputs=_inputs(),
                execute=False,
                risk_refresh=lambda _executor: {"state": "READY"},
                market_window_state=lambda *_args: "OPEN",
                clock=lambda: NOW,
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.state, "PREFLIGHT_RUNTIME_REQUIRED")
        self.assertFalse(binding.executor.submit_calls)
        self.assertFalse(binding.executor.close_calls)
        self.assertFalse(binding.executor.deactivated)

    def test_canary_requires_approval_and_writer_lock(self) -> None:
        with self.assertRaises(CanaryGateError):
            run_demo_canary(
                FakeProvider(),
                {},
                _config(),
                state_dir=Path("/tmp/unused"),
                approval=_approval(approved=False),
                inputs=_inputs(),
                execute=True,
            )

    def test_approved_canary_runs_exactly_two_open_close_cycles(self) -> None:
        binding = _binding()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("tools.demo_canary.build_demo_execution_binding", return_value=binding),
        ):
            result = run_demo_canary(
                FakeProvider(),
                {
                    "network_performed": True,
                    "source_mode": "DEMO_OBSERVED",
                    "synthetic": False,
                    "environment": "DEMO",
                    "account_id": "5097",
                    "account_selected": True,
                    "account_verified": True,
                    "endpoint": "demo.ctraderapi.com:5035",
                },
                _config(),
                state_dir=Path(directory),
                approval=_approval(approved=True),
                inputs=_inputs(),
                execute=True,
                writer_store=SupervisorStateStore(Path(directory), "demo-test"),
                risk_refresh=lambda _executor: {"state": "READY"},
                market_window_state=lambda *_args: "OPEN",
                clock=lambda: NOW,
            )
        self.assertTrue(result.ok)
        self.assertEqual(result.state, "CANARY_COMPLETED")
        self.assertEqual(result.mutation_messages, 4)
        self.assertEqual(binding.executor.submit_calls, 2)
        self.assertEqual(binding.executor.close_calls, 2)

    @unittest.skipIf(proto is None, "protobuf runtime no instalado")
    def test_canonical_binding_and_gateway_fake_complete_two_cycles(self) -> None:
        provider = _CanonicalProvider()
        provenance = {
            "network_performed": True,
            "source_mode": "DEMO_OBSERVED",
            "synthetic": False,
            "environment": "DEMO",
            "account_id": "5097",
            "account_selected": True,
            "account_verified": True,
            "endpoint": "demo.ctraderapi.com:5035",
            "connection_generation": "1",
        }
        runtime = {
            "runtime_state": "VALID",
            "market_candidate_id": "tp_fast_v1",
            "trigger_timeframe": "M1",
            "data_mode": "LIVE",
            "risk_bar_clock_basis": "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION",
            "trigger_bar_count": int(NOW.timestamp() // 60),
            "latest_trigger_start": (NOW - timedelta(minutes=1)).isoformat(),
            "latest_trigger_end": NOW.isoformat(),
            "latest_trigger_available_at": NOW.isoformat(),
            "last_market": NOW.isoformat(),
            "last_available": NOW.isoformat(),
            "atr": "0.001",
        }
        inputs = CanaryInputs.from_observed_runtime(
            runtime,
            runtime,
            instrument="EUR/USD",
            buy_signal_id="canonical-buy",
            sell_signal_id="canonical-sell",
        )
        approval = _approval(approved=True)
        with tempfile.TemporaryDirectory() as directory:
            state_dir = Path(directory)
            config = _config()
            binding = build_demo_execution_binding(
                provider,
                provenance,
                config=config,
                state_dir=state_dir,
                canary_economics=_CanonicalEconomics(),
                proto=proto,
                clock=lambda: NOW,
            )
            try:
                binding.executor.update_risk_metrics(
                    equity=Decimal("10000"),
                    daily_pnl=Decimal("0"),
                    realized_daily_pnl=Decimal("0"),
                    unrealized_daily_pnl=Decimal("0"),
                    drawdown=Decimal("0"),
                    high_water_equity=Decimal("10000"),
                    margin_available=Decimal("10000"),
                    margin_required=Decimal("10"),
                    margin_level=Decimal("1000"),
                    used_margin=Decimal("0"),
                    observed_at=NOW,
                    connection_generation="1",
                    daily_loss=Decimal("0"),
                    daily_anchor_equity=Decimal("10000"),
                    daily_anchor_cashflow_total=Decimal("0"),
                    daily_cashflow_total=Decimal("0"),
                    daily_anchor_day=NOW.date().isoformat(),
                    daily_anchor_observed_at=NOW,
                    daily_anchor_verified=True,
                    daily_loss_state="READY",
                    daily_loss_reason=None,
                    cashflows_complete=True,
                    cashflow_fingerprint="canonical-e2e",
                )
                store = SupervisorStateStore(state_dir, "canonical-e2e")
                session = PreparedCanarySession(
                    provider,
                    config,
                    provenance,
                    binding,
                    SimpleNamespace(close=lambda: None),
                    store,
                )
                refreshes: list[Any] = []

                def risk_refresh(executor: Any) -> dict[str, Any]:
                    return executor.status(refresh=False)["risk"]

                def economics_refresh(executor: Any, observed: Any) -> _CanonicalEconomics:
                    del executor, observed
                    projection = _CanonicalEconomics()
                    refreshes.append(projection)
                    return projection

                result = run_demo_canary(
                    provider,
                    provenance,
                    config,
                    state_dir=state_dir,
                    approval=approval,
                    inputs=inputs,
                    execute=True,
                    writer_store=store,
                    risk_refresh=risk_refresh,
                    economics_refresh=economics_refresh,
                    market_window_state=lambda *_args: "OPEN",
                    clock=lambda: NOW,
                    prepared_session=session,
                )
                self.assertTrue(result.ok)
                self.assertEqual(result.state, "CANARY_COMPLETED")
                self.assertEqual(result.mutation_messages, 4)
                self.assertGreaterEqual(len(refreshes), 3)
                self.assertEqual(provider.client.calls.count("ProtoOANewOrderReq"), 2)
                self.assertEqual(provider.client.calls.count("ProtoOAClosePositionReq"), 2)
            finally:
                binding.close()

    def test_observed_trial_loss_stops_before_second_cycle(self) -> None:
        binding = _binding()
        refresh_calls = 0

        def status(*, refresh: bool = False) -> dict[str, Any]:
            del refresh
            equity = Decimal("9990") if refresh_calls >= 3 else Decimal("10000")
            return {
                "risk": {
                    "position_state": "VALID",
                    "positions_fresh": True,
                    "metrics_observed_at": NOW.isoformat(),
                    "metrics_connection_generation": "g1",
                    "metrics": {"equity": str(equity)},
                    "risk_exit": {
                        "metrics": {
                            "daily_anchor_verified": True,
                            "daily_loss_state": "READY",
                            "daily_anchor_equity": "10000",
                            "daily_cashflow_total": "0",
                            "cashflows_complete": True,
                        }
                    },
                }
            }

        binding.executor.status = status
        binding.risk_observer = SimpleNamespace(update_executor=lambda _executor: {"state": "READY"})

        def risk_refresh(_executor: Any) -> dict[str, Any]:
            nonlocal refresh_calls
            refresh_calls += 1
            return {"state": "READY"}

        with (
            tempfile.TemporaryDirectory() as directory,
            patch("tools.demo_canary.build_demo_execution_binding", return_value=binding),
            self.assertRaises(CanaryGateError),
        ):
            run_demo_canary(
                FakeProvider(),
                {
                    "network_performed": True,
                    "source_mode": "DEMO_OBSERVED",
                    "synthetic": False,
                    "environment": "DEMO",
                    "account_id": "5097",
                    "account_selected": True,
                    "account_verified": True,
                    "endpoint": "demo.ctraderapi.com:5035",
                },
                _config(),
                state_dir=Path(directory),
                approval=_approval(approved=True),
                inputs=_inputs(),
                execute=True,
                writer_store=SupervisorStateStore(Path(directory), "trial-loss-observed"),
                risk_refresh=risk_refresh,
                market_window_state=lambda *_args: "OPEN",
                clock=lambda: NOW,
            )
        self.assertEqual(binding.executor.submit_calls, 1)
        self.assertEqual(binding.executor.close_calls, 1)

    def test_final_post_close_trial_loss_breaches_budget_after_second_close(self) -> None:
        binding = _binding()
        refresh_calls = 0

        def status(*, refresh: bool = False) -> dict[str, Any]:
            del refresh
            equity = Decimal("9990") if refresh_calls >= 5 else Decimal("10000")
            return {
                "risk": {
                    "position_state": "VALID",
                    "positions_fresh": True,
                    "metrics_observed_at": NOW.isoformat(),
                    "metrics_connection_generation": "g1",
                    "metrics": {"equity": str(equity)},
                    "risk_exit": {
                        "metrics": {
                            "daily_anchor_verified": True,
                            "daily_loss_state": "READY",
                            "daily_anchor_equity": "10000",
                            "daily_cashflow_total": "0",
                            "cashflows_complete": True,
                        }
                    },
                }
            }

        binding.executor.status = status
        binding.risk_observer = SimpleNamespace(update_executor=lambda _executor: {"state": "READY"})

        def risk_refresh(_executor: Any) -> dict[str, Any]:
            nonlocal refresh_calls
            refresh_calls += 1
            return {"state": "READY"}

        with (
            tempfile.TemporaryDirectory() as directory,
            patch("tools.demo_canary.build_demo_execution_binding", return_value=binding),
            self.assertRaises(CanaryGateError) as raised,
        ):
            run_demo_canary(
                FakeProvider(),
                {
                    "network_performed": True,
                    "source_mode": "DEMO_OBSERVED",
                    "synthetic": False,
                    "environment": "DEMO",
                    "account_id": "5097",
                    "account_selected": True,
                    "account_verified": True,
                    "endpoint": "demo.ctraderapi.com:5035",
                },
                _config(),
                state_dir=Path(directory),
                approval=_approval(approved=True),
                inputs=_inputs(),
                execute=True,
                writer_store=SupervisorStateStore(Path(directory), "trial-loss-final"),
                risk_refresh=risk_refresh,
                market_window_state=lambda *_args: "OPEN",
                clock=lambda: NOW,
            )
            ledger = json.loads((Path(directory) / "canary-approval-ledger.json").read_text(encoding="utf-8"))
            record = next(iter(ledger["approvals"].values()))
            self.assertEqual(record["state"], "RISK_BUDGET_BREACHED")
            self.assertEqual(Decimal(record["trial_loss_committed"]), Decimal("10"))
            self.assertEqual(Decimal(record["trial_loss_observed"]), Decimal("10"))
        self.assertEqual(binding.executor.submit_calls, 2)
        self.assertEqual(binding.executor.close_calls, 2)
        self.assertEqual(raised.exception.gates["state"], "RISK_BUDGET_BREACHED")
        self.assertEqual(raised.exception.mutation_messages, 4)

    def test_approval_is_single_use_and_ledger_survives_second_attempt(self) -> None:
        first = _binding()
        second = _binding()
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("tools.demo_canary.build_demo_execution_binding", side_effect=[first, second]),
        ):
            kwargs = {
                "provider": FakeProvider(),
                "provenance": {
                    "network_performed": True,
                    "source_mode": "DEMO_OBSERVED",
                    "synthetic": False,
                    "environment": "DEMO",
                    "account_id": "5097",
                    "account_selected": True,
                    "account_verified": True,
                    "endpoint": "demo.ctraderapi.com:5035",
                },
                "config": _config(),
                "state_dir": Path(directory),
                "approval": _approval(approved=True),
                "inputs": _inputs(),
                "execute": True,
                "writer_store": SupervisorStateStore(Path(directory), "demo-test"),
                "risk_refresh": lambda _executor: {"state": "READY"},
                "market_window_state": lambda *_args: "OPEN",
                "clock": lambda: NOW,
            }
            run_demo_canary(**kwargs)
            with self.assertRaises(CanaryGateError):
                run_demo_canary(**kwargs)
        self.assertEqual(second.executor.submit_calls, 0)

    def test_static_cli_current_demo_config_is_blocked_without_side_effects(self) -> None:
        with tempfile.TemporaryDirectory() as home, tempfile.TemporaryDirectory() as xdg:
            old_home, old_xdg = __import__("os").environ.get("HOME"), __import__("os").environ.get("XDG_STATE_HOME")
            __import__("os").environ["HOME"] = home
            __import__("os").environ["XDG_STATE_HOME"] = xdg
            try:
                result = static_cli_preflight(Path(__file__).parents[1] / "config" / "ctrader_demo.toml")
            finally:
                if old_home is None:
                    __import__("os").environ.pop("HOME", None)
                else:
                    __import__("os").environ["HOME"] = old_home
                if old_xdg is None:
                    __import__("os").environ.pop("XDG_STATE_HOME", None)
                else:
                    __import__("os").environ["XDG_STATE_HOME"] = old_xdg
        self.assertFalse(result["ok"])
        self.assertFalse(result["network_performed"])
        self.assertFalse(result["orders_attempted"])

    def test_cli_unknown_exception_preserves_post_send_mutation_state(self) -> None:
        with (
            tempfile.TemporaryDirectory() as directory,
            patch(
                "tools.demo_canary.network_cli_preflight",
                side_effect=CanaryGateError("cierre no confirmado", mutation_messages=1),
            ),
            patch("sys.stdout", new_callable=io.StringIO) as output,
        ):
            code = main(
                [
                    "--config",
                    str(Path(__file__).parents[1] / "config" / "ctrader_demo.toml"),
                    "--network",
                    "--execute",
                    "--max-events",
                    "1",
                    "--state-dir",
                    directory,
                ]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(code, 2)
        self.assertEqual(payload["state"], "CANARY_EXECUTION_UNKNOWN")
        self.assertTrue(payload["orders_attempted"])
        self.assertEqual(payload["mutation_messages"], 1)
        self.assertTrue(payload["account_reconciliation_required"])

    def test_cli_session_adapter_uses_server_identity_for_writer_namespace(self) -> None:
        binding = _binding()
        prepared = SimpleNamespace(
            provider=FakeProvider(),
            binding=binding,
            context=SimpleNamespace(config=_config()),
            provenance={
                "account_id": "5097",
                "environment": "DEMO",
                "account_selected": True,
                "account_verified": True,
                "endpoint": "demo.ctraderapi.com:5035",
            },
            close=lambda: None,
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("mtf_lab.ops.supervision_composition.prepare_network_supervision", return_value=prepared),
        ):
            from tools.demo_canary import prepare_cli_session

            session = prepare_cli_session("config/ctrader_demo.toml", directory)
            try:
                self.assertTrue(session.writer_store.account_key.startswith("demo-"))
                self.assertNotEqual(session.writer_store.account_key, "5097")
            finally:
                session.close()


if __name__ == "__main__":
    unittest.main()
