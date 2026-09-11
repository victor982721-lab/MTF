"""Offline cTrader/CFD/demo-executor demonstration."""
from __future__ import annotations
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from typing import Any
from ..data.ctrader import CTraderClient, CTraderConfig, CTraderProvider, DeterministicTransport, PAYLOAD, WireMessage, synthetic_spot_event, synthetic_trendbar
from .cfd_simulation import CFDConfig, CFDSimulator, known_fixture_eurusd_long
from .ctrader_executor import CTraderDemoExecutor, DemoAccount, DemoTransport, ExecutionPolicy, Quote

def _handler(request: WireMessage) -> WireMessage:
    if request.payload_type_id == PAYLOAD["PROTO_OA_SYMBOLS_LIST_REQ"]:
        return WireMessage("PROTO_OA_SYMBOLS_LIST_RES", {"symbol": [{"symbolId": 99, "symbolName": "EUR/USD", "digits": 5, "pipPosition": 4}]}, request.client_msg_id)
    if request.payload_type_id == PAYLOAD["PROTO_OA_GET_TRENDBARS_REQ"]:
        period = int((request.payload or {}).get("period", 1))
        return WireMessage("PROTO_OA_GET_TRENDBARS_RES", {"period": period, "symbolId": 99, "trendbar": [synthetic_trendbar(timestamp_minutes=1), synthetic_trendbar(timestamp_minutes=2)], "hasMore": False}, request.client_msg_id)
    if request.payload_type_id == PAYLOAD["PROTO_OA_SUBSCRIBE_SPOTS_REQ"]:
        return WireMessage("PROTO_OA_SUBSCRIBE_SPOTS_RES", {}, request.client_msg_id)
    if request.payload_type_id == PAYLOAD["PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ"]:
        return WireMessage("PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES", {}, request.client_msg_id)
    return WireMessage("PROTO_ERROR_RES", {"errorCode": "UNSUPPORTED_FIXTURE_REQUEST"}, request.client_msg_id)

def run_ctrader_fixture(report_path: str | Path | None = None) -> dict[str, Any]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    transport = DeterministicTransport(_handler)
    config = CTraderConfig(symbol="EUR/USD", symbol_id=99, account_id=7, quote_basis="mid")
    client = CTraderClient(config, transport=transport)
    client.connect(); client.mark_authenticated(7)
    provider = CTraderProvider(config, client=client)
    catalog = provider.resolve_symbol(); provider.subscribe()
    history = provider.fetch("M1", count=2, from_timestamp=start, to_timestamp=start + timedelta(minutes=2))
    quote_result = provider.normalize_spot(synthetic_spot_event(timestamp_ms=int(start.timestamp() * 1000), symbol_id=99), received_at=start + timedelta(seconds=1), snapshot=True)
    signal, quotes = known_fixture_eurusd_long()
    cfd = CFDSimulator(CFDConfig(instrument="EUR/USD", units="1000", horizons_seconds=("60",)))
    cfd_result = cfd.replay([signal], quotes)
    account = DemoAccount("fixture-demo", "DEMO", "demo://ctrader", frozenset({"trading"}), selected=True, verified=True)
    demo_transport = DemoTransport(account_id=account.account_id, endpoint=account.endpoint, scopes=account.scopes, clock=lambda: start)
    executor = CTraderDemoExecutor(account, transport=demo_transport, policy=ExecutionPolicy(max_quantity=1, fixed_quantity=1, max_exposure=10_000, max_positions=1, max_spread=0.01, max_price_age_seconds=30, timeout_seconds=1), clock=lambda: start)
    executor.activate()
    execution = executor.submit_signal({"signal_id": "fixture-demo-signal", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"}, Quote("EUR/USD", 1.1000, 1.1002, start, available_at=start, source="fixture"))
    provider_status = provider.status.to_dict()
    # Receipt-time telemetry is intentionally omitted from the reproducible
    # fixture artifact; source timestamps remain fixed in the records.
    provider_status["last_message_at"] = None
    result: dict[str, Any] = {"ok": True, "provider": {"name": provider.name, "status": provider_status, "catalog": catalog.to_dict(), "history": history.to_dict(), "quote_events": len(quote_result.quote_events), "bars": len(quote_result.bars), "network_performed": False, "credentials_used": False}, "cfd_paper": {"product": "FOREX_CFD_LOCAL_PAPER", "capture_complete": cfd_result.capture_complete, "trades": [item.to_dict() for item in cfd_result.trades], "events": list(cfd_result.events)}, "demo_executor_fixture": {"product": "CTRADER_DEMO_EXECUTOR_FIXTURE", "virtual_only": True, "server_contacted": False, "execution": execution.to_dict(), "status": executor.status()}, "limitations": ["cTrader SDK/TCP real no probado: dependencia opcional ausente en este host.", "Pepperstone, aplicación OAuth, cuenta DEMO y permisos no verificados.", "Los precios/órdenes son fixtures sintéticos; no representan fills ni rentabilidad externa."]}
    if report_path is not None:
        target = Path(report_path).expanduser(); target.parent.mkdir(parents=True, exist_ok=True); target.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"); result["report_path"] = str(target)
    client.close()
    return result

__all__ = ["run_ctrader_fixture"]
