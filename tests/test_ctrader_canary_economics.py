"""Focused offline tests for the bounded read-only canary economics."""

from __future__ import annotations

import dataclasses
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from mtf_lab.data.ctrader_protocol import WireMessage
from mtf_lab.data.ctrader_session import AuthenticatedSessionEvidence
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.data.protobuf_generated import OpenApiModelMessages_pb2 as model
from mtf_lab.ops.ctrader_account_risk import AccountRiskSnapshot
from mtf_lab.ops.ctrader_canary_economics import (
    CanaryEconomicsError,
    CanaryEconomicsProjection,
    ExitSlippageHypothesis,
    observe_canary_economics,
)

NOW = datetime(2026, 9, 19, 14, 0, tzinfo=UTC)


def _snapshot(
    *, equity: str = "10000", positions: tuple[dict[str, object], ...] = (), used_margin: str = "0"
) -> AccountRiskSnapshot:
    return AccountRiskSnapshot(
        account_id="123",
        session_id="session-1",
        connection_generation="1",
        observed_at=NOW,
        day_start=datetime(2026, 9, 19, tzinfo=UTC),
        day_end=NOW,
        balance=equity,
        equity=equity,
        used_margin=used_margin,
        margin_level=None,
        realized_daily_pnl="0",
        realized_daily_gross_pnl="0",
        realized_daily_costs="0",
        unrealized_daily_pnl="0",
        unrealized_gross_pnl="0",
        daily_pnl="0",
        positions=positions,
        freshness_state="FRESH",
        fresh=True,
        complete=True,
        account_complete=True,
        margin_state="NO_MARGIN_USED",
        positions_complete=True,
        unrealized_complete=True,
        deals_complete=True,
        fees_complete=True,
    )


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[Any] = []
        self.session = AuthenticatedSessionEvidence(
            account_id=123,
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            scopes=frozenset({"accounts"}),
            session_id="session-1",
            connection_generation="1",
            authenticated_at=NOW - timedelta(minutes=1),
        )
        self.margin_buy = 2000
        self.margin_sell = 2200
        self.response_at = NOW

    def authenticated_session_evidence(self) -> AuthenticatedSessionEvidence:
        return self.session

    def validate_session_evidence(self, proof: Any) -> bool:
        return proof is self.session

    def request_message(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> WireMessage:
        del timeout_seconds
        self.calls.append(message)
        name = type(message).__name__
        if name == "ProtoOATraderReq":
            body: Any = proto.ProtoOATraderRes(
                ctidTraderAccountId=123,
                trader=model.ProtoOATrader(ctidTraderAccountId=123, depositAssetId=15, moneyDigits=2),
            )
            payload_type = 2122
        elif name == "ProtoOAAssetListReq":
            body = proto.ProtoOAAssetListRes(ctidTraderAccountId=123)
            body.asset.add(assetId=5, name="EUR")
            body.asset.add(assetId=15, name="USD")
            payload_type = 2113
        elif name == "ProtoOAExpectedMarginReq":
            body = proto.ProtoOAExpectedMarginRes(ctidTraderAccountId=123, moneyDigits=2)
            body.margin.add(buyMargin=self.margin_buy, sellMargin=self.margin_sell)
            payload_type = 2140
        else:  # pragma: no cover - the economics adapter has a fixed read allowlist
            raise AssertionError(name)
        return WireMessage(
            payload_type,
            body,
            client_msg_id,
            received_at=self.response_at,
            available_at=self.response_at,
            connection_generation=1,
        )


class FakeProvider:
    name = "ctrader_open_api"

    def __init__(self) -> None:
        self.client = FakeClient()
        self.spec = SimpleNamespace(symbol="EUR/USD", symbol_id=11)
        full = {
            "symbolId": 11,
            "digits": 5,
            "pipPosition": 4,
            "baseAssetId": 5,
            "quoteAssetId": 15,
            "minVolume": 0,
            "maxVolume": 1_000_000,
            "stepVolume": 100_000,
            "commissionType": 2,
            "preciseTradingCommissionRate": 300_000_000,
            "preciseMinCommission": 0,
            "lotSize": 10_000_000,
            "distanceSetIn": 1,
            "slDistance": 0,
            "tpDistance": 0,
            "swapTime": 1260,
            "swapPeriod": 24,
            "scheduleTimeZone": "UTC",
        }
        selected = SimpleNamespace(name="EUR/USD", symbol_id=11, metadata=full)
        self.catalog = SimpleNamespace(selected=selected, symbols=(selected,), requested_symbol="EUR/USD")

    def snapshot_quote_state(self) -> dict[str, Any]:
        return {
            "symbols": {
                "11": {
                    "bid": {
                        "price": "1.10000",
                        "event_time": (NOW - timedelta(seconds=1)).isoformat(),
                        "available_at": NOW.isoformat(),
                        "generation": "1",
                        "sequence": 7,
                        "state": "VALID",
                    },
                    "ask": {
                        "price": "1.10020",
                        "event_time": (NOW - timedelta(seconds=1)).isoformat(),
                        "available_at": NOW.isoformat(),
                        "generation": "1",
                        "sequence": 7,
                        "state": "VALID",
                    },
                }
            }
        }


def _observe(provider: FakeProvider, **kwargs: Any) -> CanaryEconomicsProjection:
    parameters: dict[str, Any] = {
        "window_start": NOW,
        "window_end": NOW + timedelta(minutes=10),
        "now": NOW,
        "exit_slippage_pips": "0.1",
        "exit_slippage_approved": True,
        "exit_slippage_approval_source": "manual-canary-plan-v1",
        "proto": proto,
    }
    if "exit_slippage_hypothesis" in kwargs or "exit_slippage_basis" in kwargs or "exit_slippage_source" in kwargs:
        parameters.pop("exit_slippage_pips")
        parameters.pop("exit_slippage_approved")
        parameters.pop("exit_slippage_approval_source")
    parameters.update(kwargs)
    return observe_canary_economics(
        provider,
        _snapshot(),
        **parameters,
    )


class CanaryEconomicsTests(unittest.TestCase):
    def test_compact_selected_symbol_preserves_canonical_eurusd_identity(self) -> None:
        provider = FakeProvider()
        selected = SimpleNamespace(
            name="EURUSD",
            symbol_id=11,
            metadata=provider.catalog.selected.metadata,
        )
        provider.catalog.selected = selected
        provider.catalog.symbols = (selected,)

        projection = _observe(provider)

        self.assertEqual(projection.symbol, "EUR/USD")
        self.assertEqual(projection.symbol_id, 11)
        self.assertEqual(projection.quote.symbol, "EUR/USD")
        self.assertEqual(provider.spec.symbol, "EUR/USD")
        self.assertEqual(provider.catalog.requested_symbol, "EUR/USD")
        self.assertEqual(projection.provenance["raw_catalog_fields"]["symbol_id"], 11)

    def test_other_pair_suffix_identity_and_catalog_list_fail_closed(self) -> None:
        cases = (
            ("other_pair", "GBPUSD", "GBPUSD", 11, True),
            ("suffix", "EUR/USD.pro", "EUR/USD.pro", 11, True),
            ("identity", "EURUSD", "EUR/USD", 12, True),
            ("catalog_list", "EURUSD", "EUR/USD", 11, False),
        )
        for label, selected_name, requested_symbol, selected_id, listed in cases:
            with self.subTest(label=label):
                provider = FakeProvider()
                selected = SimpleNamespace(
                    name=selected_name,
                    symbol_id=selected_id,
                    metadata=provider.catalog.selected.metadata,
                )
                provider.catalog.selected = selected
                provider.catalog.requested_symbol = requested_symbol
                provider.catalog.symbols = (selected,) if listed else ()
                with self.assertRaises(CanaryEconomicsError):
                    _observe(provider)

    def test_asymmetric_quote_uses_joint_availability_and_retains_oldest_leg(self) -> None:
        provider = FakeProvider()
        quote_state = provider.snapshot_quote_state()
        bid = quote_state["symbols"]["11"]["bid"]
        ask = quote_state["symbols"]["11"]["ask"]
        bid["event_time"] = (NOW - timedelta(seconds=0.2)).isoformat()
        bid["available_at"] = (NOW - timedelta(seconds=0.1)).isoformat()
        ask["event_time"] = (NOW - timedelta(seconds=2)).isoformat()
        ask["available_at"] = (NOW - timedelta(seconds=1.9)).isoformat()
        provider.snapshot_quote_state = lambda: quote_state  # type: ignore[method-assign]

        projection = _observe(provider)

        self.assertEqual(projection.quote.event_time, NOW - timedelta(seconds=0.2))
        self.assertEqual(projection.quote.available_at, NOW - timedelta(seconds=0.1))
        self.assertEqual(projection.quote.earliest_available_at, NOW - timedelta(seconds=1.9))
        self.assertEqual(
            projection.quote.to_dict()["available_at"],
            (NOW - timedelta(seconds=0.1)).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        )
        self.assertEqual(
            projection.quote.to_dict()["earliest_available_at"],
            (NOW - timedelta(seconds=1.9)).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        )
        self.assertEqual(projection.symbol, "EUR/USD")
        self.assertEqual(projection.symbol_id, 11)

    def test_asymmetric_quote_stale_future_causal_generation_and_spread_gates_fail_closed(self) -> None:
        cases = ("stale_old_leg", "future_leg", "available_before_event", "generation", "zero_spread", "crossed")
        for case in cases:
            with self.subTest(case=case):
                provider = FakeProvider()
                quote_state = provider.snapshot_quote_state()
                bid = quote_state["symbols"]["11"]["bid"]
                ask = quote_state["symbols"]["11"]["ask"]
                if case == "stale_old_leg":
                    ask["event_time"] = (NOW - timedelta(seconds=11.2)).isoformat()
                    ask["available_at"] = (NOW - timedelta(seconds=11)).isoformat()
                    bid["event_time"] = (NOW - timedelta(seconds=0.2)).isoformat()
                    bid["available_at"] = (NOW - timedelta(seconds=0.1)).isoformat()
                elif case == "future_leg":
                    bid["event_time"] = (NOW + timedelta(seconds=1)).isoformat()
                    bid["available_at"] = (NOW + timedelta(seconds=1.1)).isoformat()
                elif case == "available_before_event":
                    bid["event_time"] = (NOW - timedelta(seconds=0.2)).isoformat()
                    bid["available_at"] = (NOW - timedelta(seconds=0.3)).isoformat()
                elif case == "generation":
                    ask["generation"] = "generation-2"
                elif case == "zero_spread":
                    ask["price"] = bid["price"]
                else:
                    ask["price"] = "1.09990"
                provider.snapshot_quote_state = lambda state=quote_state: state  # type: ignore[method-assign]
                with self.assertRaises(CanaryEconomicsError):
                    _observe(provider)

    def test_typed_slippage_hypothesis_is_not_human_approval_or_observation(self) -> None:
        projection = _observe(FakeProvider(), exit_slippage_hypothesis=ExitSlippageHypothesis())

        self.assertEqual(projection.assumptions["exit_slippage_basis"], "IMPLEMENTATION_HYPOTHESIS")
        self.assertEqual(projection.assumptions["exit_slippage_source"], "baseline-0.1pip")
        self.assertNotIn("exit_slippage_approval_source", projection.assumptions)
        self.assertEqual(projection.contract_spec["exit_slippage_basis"], "IMPLEMENTATION_HYPOTHESIS")
        self.assertEqual(projection.contract_spec["exit_slippage_source"], "baseline-0.1pip")
        self.assertTrue(projection.contract_spec["risk_envelope_known"])
        self.assertFalse(projection.contract_spec["risk_envelope_guarantee"])

    def test_direct_typed_slippage_seam_requires_no_boolean_approval(self) -> None:
        projection = _observe(
            FakeProvider(),
            exit_slippage_pips=Decimal("0.1"),
            exit_slippage_basis="IMPLEMENTATION_HYPOTHESIS",
            exit_slippage_source="baseline-0.1pip",
        )
        self.assertEqual(projection.assumptions["exit_slippage_source"], "baseline-0.1pip")

    def test_typed_hypothesis_cannot_be_mixed_with_legacy_approval_fields(self) -> None:
        with self.assertRaises(CanaryEconomicsError):
            _observe(
                FakeProvider(),
                exit_slippage_hypothesis=ExitSlippageHypothesis(),
                exit_slippage_approved=True,
            )

    def test_projection_uses_exact_protocol_volume_and_observed_margin(self) -> None:
        provider = FakeProvider()
        projection = _observe(provider)

        self.assertEqual(projection.quantity, Decimal("1000"))
        self.assertEqual(projection.quantity_protocol, 100_000)
        self.assertEqual(projection.buy_margin_required, Decimal("20.00"))
        self.assertEqual(projection.sell_margin_required, Decimal("22.00"))
        self.assertEqual(projection.margin_available, Decimal("10000"))
        self.assertEqual(projection.to_update_risk_kwargs("BUY")["used_margin"], Decimal("0"))
        self.assertEqual(projection.to_contract_spec("BUY")["pip_size"], "0.0001")
        self.assertEqual(projection.to_contract_spec("BUY")["price_quantum"], "0.00001")
        self.assertEqual(projection.to_contract_spec("BUY")["unit_value"], "1")
        self.assertEqual(projection.to_contract_spec("BUY")["expected_cost_per_unit"], "0.00006")
        self.assertEqual(projection.to_contract_spec("BUY")["expected_exit_slippage_per_unit"], "0.00001")
        self.assertEqual(projection.to_contract_spec("BUY")["minimum_stop_distance"], "0.00001")
        self.assertEqual(projection.calendar_state["financing_known"], True)
        self.assertEqual(
            [type(item).__name__ for item in provider.client.calls],
            [
                "ProtoOATraderReq",
                "ProtoOAAssetListReq",
                "ProtoOAExpectedMarginReq",
            ],
        )
        margin_request = provider.client.calls[-1]
        self.assertEqual(margin_request.ctidTraderAccountId, 123)
        self.assertEqual(margin_request.symbolId, 11)
        self.assertEqual(tuple(margin_request.volume), (100_000,))

    def test_default_clock_uses_terminal_reference_for_response_freshness(self) -> None:
        provider = FakeProvider()
        provider.client.response_at = NOW + timedelta(seconds=0.5)
        clock_values = iter((NOW, NOW + timedelta(seconds=1)))
        projection = observe_canary_economics(
            provider,
            _snapshot(),
            window_start=NOW,
            window_end=NOW + timedelta(minutes=10),
            clock=lambda: next(clock_values),
            exit_slippage_pips="0.1",
            exit_slippage_approved=True,
            exit_slippage_approval_source="plan",
            proto=proto,
        )
        self.assertEqual(projection.observed_at, NOW + timedelta(seconds=0.5))

    def test_missing_slippage_approval_fails_closed(self) -> None:
        with self.assertRaises(CanaryEconomicsError):
            observe_canary_economics(
                FakeProvider(),
                _snapshot(),
                window_start=NOW,
                window_end=NOW + timedelta(minutes=10),
                now=NOW,
                proto=proto,
            )

    def test_partial_margin_response_fails_closed(self) -> None:
        provider = FakeProvider()
        provider.client.margin_sell = 0
        with self.assertRaises(CanaryEconomicsError):
            _observe(provider)

    def test_future_or_changed_generation_margin_response_fails_closed(self) -> None:
        provider = FakeProvider()
        provider.client.response_at = NOW + timedelta(seconds=1)
        with self.assertRaises(CanaryEconomicsError):
            _observe(provider)

        provider = FakeProvider()
        original = provider.client.request_message

        def changed_generation(message: Any, *, client_msg_id: str, timeout_seconds: float) -> WireMessage:
            result = original(message, client_msg_id=client_msg_id, timeout_seconds=timeout_seconds)
            if type(message).__name__ == "ProtoOAExpectedMarginReq":
                return dataclasses.replace(result, connection_generation=2)
            return result

        provider.client.request_message = changed_generation  # type: ignore[method-assign]
        with self.assertRaises(CanaryEconomicsError):
            _observe(provider)

    def test_nonflat_or_unknown_margin_snapshot_fails_closed(self) -> None:
        provider = FakeProvider()
        snapshot = _snapshot(positions=({"position_id": "p1"},))
        with self.assertRaises(CanaryEconomicsError):
            observe_canary_economics(
                provider,
                snapshot,
                window_start=NOW,
                window_end=NOW + timedelta(minutes=10),
                now=NOW,
                exit_slippage_pips="0.1",
                exit_slippage_approved=True,
                exit_slippage_approval_source="plan",
                proto=proto,
            )

    def test_asset_mapping_must_prove_usd_deposit_and_quote(self) -> None:
        provider = FakeProvider()
        provider.client.session = dataclasses.replace(provider.client.session, account_id=124)
        with self.assertRaises(CanaryEconomicsError):
            _observe(provider)

    def test_unsupported_minimum_commission_fails_closed(self) -> None:
        provider = FakeProvider()
        provider.catalog.selected.metadata["preciseMinCommission"] = 1
        with self.assertRaises(CanaryEconomicsError):
            _observe(provider)

    def test_rollover_window_is_not_financing_safe(self) -> None:
        provider = FakeProvider()
        with self.assertRaises(CanaryEconomicsError):
            observe_canary_economics(
                provider,
                _snapshot(),
                window_start=NOW,
                window_end=NOW + timedelta(minutes=10),
                now=NOW.replace(hour=20, minute=59, second=59),
                exit_slippage_pips="0.1",
                exit_slippage_approved=True,
                exit_slippage_approval_source="plan",
                proto=proto,
            )

    def test_quote_causality_and_scope_are_gates(self) -> None:
        provider = FakeProvider()
        quote_state = provider.snapshot_quote_state()
        quote_state["symbols"]["11"]["ask"]["available_at"] = (NOW - timedelta(seconds=2)).isoformat()
        provider.snapshot_quote_state = lambda: quote_state  # type: ignore[method-assign]
        with self.assertRaises(CanaryEconomicsError):
            _observe(provider)

        provider = FakeProvider()
        provider.client.session = dataclasses.replace(provider.client.session, scopes=frozenset())
        with self.assertRaises(CanaryEconomicsError):
            _observe(provider)


if __name__ == "__main__":
    unittest.main()
