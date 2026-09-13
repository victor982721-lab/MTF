from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from mtf_lab.data.ctrader import WireMessage
from mtf_lab.ops.ctrader_demo_transport import PAYLOAD_TYPES, AuthenticatedSession, OfficialMessageError
from mtf_lab.ops.ctrader_executor import DemoAccountRequired, ExecutionPolicy, Quote, RiskLimitRejected
from mtf_lab.ops.supervision_demo import build_demo_execution_binding

try:
    from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
except Exception:  # pragma: no cover - optional runtime dependency
    proto = None


NOW = datetime(2025, 1, 1, 12, tzinfo=UTC)


class LocalClient:
    def __init__(self) -> None:
        self.session = AuthenticatedSession(
            "session-1",
            "123",
            "DEMO",
            "demo.ctraderapi.com:5035",
            {"trading"},
            NOW,
            "generation-1",
            NOW + timedelta(hours=1),
        )
        self.calls: list[str] = []

    def authenticated_session_evidence(self):
        return self.session

    def validate_session_evidence(self, proof):
        return proof is self.session

    def request_message(self, message, *, client_msg_id, timeout_seconds):
        del timeout_seconds
        self.calls.append(type(message).__name__)
        if type(message).__name__ != "ProtoOAReconcileReq":
            raise AssertionError(type(message).__name__)
        response = proto.ProtoOAReconcileRes(ctidTraderAccountId=123)
        return WireMessage(PAYLOAD_TYPES["ProtoOAReconcileRes"], response, client_msg_id)


class Provider:
    name = "ctrader_open_api"

    def __init__(self) -> None:
        self.client = LocalClient()
        self.spec = type("Spec", (), {"symbol": "EUR/USD", "symbol_id": 11})()
        selected = SimpleNamespace(
            name="EUR/USD",
            symbol_id=11,
            metadata={
                "fullSymbol": {
                    "symbolId": 11,
                    "minVolume": 100,
                    "maxVolume": 100000,
                    "stepVolume": 100,
                }
            },
        )
        self.catalog = SimpleNamespace(
            selected=selected,
            symbols=(selected,),
            requested_symbol="EUR/USD",
        )

    def snapshot_quote_state(self):
        return {
            "symbols": {
                "11": {
                    "bid": {
                        "price": "1.1000",
                        "event_time": "2025-01-01T12:00:00Z",
                        "available_at": "2025-01-01T12:00:01Z",
                        "generation": "generation-1",
                        "sequence": 2,
                    },
                    "ask": {
                        "price": "1.1002",
                        "event_time": "2025-01-01T12:00:00Z",
                        "available_at": "2025-01-01T12:00:01Z",
                        "generation": "generation-1",
                        "sequence": 2,
                    },
                }
            }
        }


def config(*, enabled: bool = True):
    return type(
        "Config",
        (),
        {
            "instrument": "EUR/USD",
            "execution": {
                "enabled": enabled,
                "environment": "DEMO",
                "endpoint": "demo.ctraderapi.com:5035",
                "account_id": "123",
                "scope": "trading",
                "max_quantity": 1,
                "fixed_quantity": 1,
                "max_exposure": 1000,
                "max_positions": 1,
                "max_inflight_intents": 1,
                "max_holding_seconds": 3600,
                "max_daily_loss": 100,
                "max_drawdown": 100,
                "min_margin_level": 1,
                "max_spread": 1,
                "max_price_age_seconds": 30,
                "require_protective_stops": True,
                "relative_stop_loss": 10,
                "relative_take_profit": 20,
            },
        },
    )()


def external_policy() -> ExecutionPolicy:
    return ExecutionPolicy(
        max_quantity=1,
        fixed_quantity=1,
        max_positions=1,
        max_exposure=1000,
        max_spread=1,
        max_price_age_seconds=30,
        max_daily_loss=100,
        max_drawdown=100,
        min_margin_level=1,
        max_inflight_intents=1,
        max_holding_seconds=3600,
        require_protective_stops=True,
        relative_stop_loss=10,
        relative_take_profit=20,
    )


PROVENANCE = {
    "network_performed": True,
    "source_mode": "DEMO_OBSERVED",
    "synthetic": False,
    "environment": "DEMO",
    "account_id": 123,
    "account_selected": True,
    "account_verified": True,
    "endpoint": "demo.ctraderapi.com:5035",
}


@unittest.skipIf(proto is None, "protobuf runtime no instalado")
class SupervisionDemoCompositionTests(unittest.TestCase):
    def test_binding_reuses_one_local_client_and_real_generated_reconcile(self):
        provider = Provider()
        with tempfile.TemporaryDirectory() as raw_dir:
            binding = build_demo_execution_binding(
                provider,
                PROVENANCE,
                config=config(),
                state_dir=Path(raw_dir),
                proto=proto,
                policy=external_policy(),
                clock=lambda: NOW,
            )
            try:
                self.assertIs(binding.callbacks.executor, binding.executor)
                self.assertTrue(binding.executor.active)
                self.assertEqual(provider.client.calls, ["ProtoOAReconcileReq", "ProtoOAReconcileReq"])
                self.assertTrue(callable(binding.callbacks.on_signal))
                self.assertFalse(binding.executor.status()["new_intents_enabled"])
            finally:
                binding.close()

    def test_disabled_execution_config_is_rejected_before_client_use(self):
        provider = Provider()
        with tempfile.TemporaryDirectory() as raw_dir, self.assertRaises(DemoAccountRequired):
            build_demo_execution_binding(
                provider,
                PROVENANCE,
                config=config(enabled=False),
                state_dir=Path(raw_dir),
                proto=proto,
                clock=lambda: NOW,
            )
        self.assertEqual(provider.client.calls, [])

    def test_external_open_rejects_off_grid_before_intent_or_wire_send(self):
        provider = Provider()
        current = [NOW]
        policy = ExecutionPolicy(
            fixed_quantity=None,
            max_quantity=2,
            max_positions=1,
            max_exposure=1000,
            max_spread=1,
            max_price_age_seconds=30,
            max_daily_loss=100,
            max_drawdown=100,
            min_margin_level=1,
            max_inflight_intents=1,
            max_holding_seconds=3600,
            require_protective_stops=True,
            relative_stop_loss=10,
            relative_take_profit=20,
        )
        with tempfile.TemporaryDirectory() as raw_dir:
            binding = build_demo_execution_binding(
                provider,
                PROVENANCE,
                config=config(),
                state_dir=Path(raw_dir),
                proto=proto,
                policy=policy,
                clock=lambda: current[0],
            )
            try:
                binding.executor.update_risk_metrics(
                    realized_daily_pnl=0,
                    unrealized_daily_pnl=0,
                    drawdown=0,
                    margin_level=100,
                    observed_at=NOW,
                    connection_generation="generation-1",
                )
                quote = Quote(
                    "EUR/USD",
                    "1.1000",
                    "1.1002",
                    NOW,
                    NOW,
                    source_identity="observed-quote",
                    session_id="session-1",
                    connection_generation="generation-1",
                    data_mode="LIVE",
                )
                with self.assertRaises((RiskLimitRejected, OfficialMessageError)):
                    binding.executor.submit_signal(
                        {"signal_id": "off-grid", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
                        quote,
                        quantity="1.01",
                    )
                self.assertEqual(binding.intent_store.intents, {})
                self.assertNotIn("ProtoOANewOrderReq", provider.client.calls)
            finally:
                binding.close()

    def test_external_open_requires_observed_volume_grid(self):
        provider = Provider()
        provider.catalog.selected.metadata = {"fullSymbol": {"symbolId": 11}}
        with tempfile.TemporaryDirectory() as raw_dir, self.assertRaises(RiskLimitRejected):
            build_demo_execution_binding(
                provider,
                PROVENANCE,
                config=config(),
                state_dir=Path(raw_dir),
                proto=proto,
                clock=lambda: NOW,
            )
        self.assertEqual(provider.client.calls, [])


if __name__ == "__main__":
    unittest.main()
