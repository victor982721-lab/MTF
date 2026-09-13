"""Offline cTrader/CFD/demo-executor demonstration."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from ..data.ctrader import (
    PAYLOAD,
    CTraderClient,
    CTraderConfig,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
    dependency_report,
    synthetic_spot_event,
    synthetic_trendbar,
)
from .cfd_simulation import CFDConfig, CFDSimulator, known_fixture_eurusd_long
from .ctrader_demo_transport import (
    AuthenticatedSession,
    CTraderClientGateway,
    CTraderDemoTransport,
    CTraderDemoTransportConfig,
    load_official_proto,
)
from .ctrader_executor import (
    CTraderDemoExecutor,
    DecimalValue,
    DemoAccount,
    DemoTransport,
    ExecutionIntent,
    ExecutionPolicy,
    Quote,
    Side,
)


def _handler(request: WireMessage) -> WireMessage:
    if request.payload_type_id == PAYLOAD["PROTO_OA_SYMBOLS_LIST_REQ"]:
        return WireMessage(
            "PROTO_OA_SYMBOLS_LIST_RES",
            {"symbol": [{"symbolId": 99, "symbolName": "EUR/USD", "digits": 5, "pipPosition": 4}]},
            request.client_msg_id,
        )
    if request.payload_type_id == PAYLOAD["PROTO_OA_GET_TRENDBARS_REQ"]:
        period = int((request.payload or {}).get("period", 1))
        return WireMessage(
            "PROTO_OA_GET_TRENDBARS_RES",
            {
                "period": period,
                "symbolId": 99,
                "trendbar": [synthetic_trendbar(timestamp_minutes=1), synthetic_trendbar(timestamp_minutes=2)],
                "hasMore": False,
            },
            request.client_msg_id,
        )
    if request.payload_type_id == PAYLOAD["PROTO_OA_SUBSCRIBE_SPOTS_REQ"]:
        return WireMessage("PROTO_OA_SUBSCRIBE_SPOTS_RES", {}, request.client_msg_id)
    if request.payload_type_id == PAYLOAD["PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ"]:
        return WireMessage("PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES", {}, request.client_msg_id)
    return WireMessage("PROTO_ERROR_RES", {"errorCode": "UNSUPPORTED_FIXTURE_REQUEST"}, request.client_msg_id)


def _official_demo_probe(start: datetime) -> dict[str, Any]:
    """Exercise message construction with the installed official descriptors."""

    report = dependency_report()
    result: dict[str, Any] = {
        "endpoint": "demo.ctraderapi.com:5035",
        "network_performed": False,
        "credentials_used": False,
        "sdk_state": report.sdk_state.value,
        "codec_operational": report.codec_operational,
    }
    if not report.codec_operational:
        result["state"] = "SDK_UNAVAILABLE"
        result["next_action"] = "Instale el extra oficial en .venv antes de validar mensajes Protobuf reales."
        return result

    class LocalRequestClient:
        """Synchronous typed client used only to prove offline composition."""

        def __init__(self, session: AuthenticatedSession) -> None:
            self.session = session

        def authenticated_session_evidence(self) -> AuthenticatedSession:
            return self.session

        def validate_session_evidence(self, proof: Any) -> bool:
            return True

        def request_message(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> WireMessage:
            raise AssertionError("el probe de construcción no debe enviar mensajes")

    try:
        account = DemoAccount(
            "7",
            "DEMO",
            "demo.ctraderapi.com:5035",
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )
        session = AuthenticatedSession(
            "fixture-session",
            "7",
            "DEMO",
            "demo.ctraderapi.com:5035",
            frozenset({"trading"}),
            start,
            "fixture-generation",
        )
        gateway = CTraderClientGateway(cast(CTraderClient, LocalRequestClient(session)), clock=lambda: start)
        transport = CTraderDemoTransport(
            account,
            client=gateway,
            proto=load_official_proto(),
            symbol_ids={"EUR/USD": 99},
            config=CTraderDemoTransportConfig(),
            clock=lambda: start,
        )
        intent = ExecutionIntent(
            "fixture-official-intent",
            "fixture-official-signal",
            "EUR/USD",
            Side.BUY,
            DecimalValue("1.0"),
            DecimalValue("1.1002"),
            start,
            "7",
        )
        message = transport.build_new_order(intent)
        result.update(
            {
                "state": "MESSAGE_CONSTRUCTION_OK",
                "gateway": "CTraderClientGateway",
                "session_provenance": transport.server_observation.to_dict(),
                "message_type": type(message).__name__,
                "payload_fields": {
                    "ctidTraderAccountId": int(message.ctidTraderAccountId),
                    "symbolId": int(message.symbolId),
                    "volume": int(message.volume),
                    "clientOrderId": str(message.clientOrderId),
                },
            }
        )
    except Exception as exc:
        result.update(
            {
                "state": "MESSAGE_CONSTRUCTION_ERROR",
                "error": type(exc).__name__,
                "next_action": "Revise los descriptores instalados; no se abrió red.",
            }
        )
    return result


def run_ctrader_fixture(report_path: str | Path | None = None) -> dict[str, Any]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    transport = DeterministicTransport(_handler)
    config = CTraderConfig(symbol="EUR/USD", symbol_id=99, account_id=7, quote_basis="mid")
    client = CTraderClient(config, transport=transport)
    client.connect()
    client.mark_authenticated(7)
    provider = CTraderProvider(config, client=client)
    catalog = provider.resolve_symbol()
    provider.subscribe()
    history = provider.fetch("M1", count=2, from_timestamp=start, to_timestamp=start + timedelta(minutes=2))
    quote_result = provider.normalize_spot(
        synthetic_spot_event(timestamp_ms=int(start.timestamp() * 1000), symbol_id=99),
        received_at=start + timedelta(seconds=1),
        snapshot=True,
    )
    signal, quotes = known_fixture_eurusd_long()
    cfd = CFDSimulator(CFDConfig(instrument="EUR/USD", units=Decimal("1000"), horizons_seconds=(Decimal("60"),)))
    cfd_result = cfd.replay([signal], quotes)
    account = DemoAccount(
        "fixture-demo", "DEMO", "demo://ctrader", frozenset({"trading"}), selected=True, verified=True
    )
    demo_transport = DemoTransport(
        account_id=account.account_id, endpoint=account.endpoint, scopes=account.scopes, clock=lambda: start
    )
    executor = CTraderDemoExecutor(
        account,
        transport=demo_transport,
        policy=ExecutionPolicy(
            max_quantity=DecimalValue("1"),
            fixed_quantity=DecimalValue("1"),
            max_exposure=DecimalValue("10000"),
            max_positions=1,
            max_spread=DecimalValue("0.01"),
            max_price_age_seconds=DecimalValue("30"),
            timeout_seconds=DecimalValue("1"),
        ),
        clock=lambda: start,
    )
    executor.activate()
    execution = executor.submit_signal(
        {"signal_id": "fixture-demo-signal", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
        Quote(
            "EUR/USD",
            DecimalValue("1.1000"),
            DecimalValue("1.1002"),
            start,
            available_at=start,
            source="fixture",
        ),
    )
    provider_status = provider.status.to_dict()
    official_probe = _official_demo_probe(start)
    dep = dependency_report()
    limitation_sdk = (
        "Codec Protobuf generado utilizable y serialización local comprobada; TCP/TLS/OAuth real no probado."
        if dep.codec_operational
        else "SDK/codec cTrader no disponible en el intérprete de esta corrida; TCP/TLS/OAuth real no probado."
    )
    # Receipt-time telemetry is intentionally omitted from the reproducible
    # fixture artifact; source timestamps remain fixed in the records.
    provider_status["last_message_at"] = None
    result: dict[str, Any] = {
        "ok": True,
        "provider": {
            "name": provider.name,
            "status": provider_status,
            "catalog": catalog.to_dict(),
            "history": history.to_dict(),
            "quote_events": len(quote_result.quote_events),
            "bars": len(quote_result.bars),
            "network_performed": False,
            "credentials_used": False,
        },
        "cfd_paper": {
            "product": "FOREX_CFD_LOCAL_PAPER",
            "capture_complete": cfd_result.capture_complete,
            "trades": [item.to_dict() for item in cfd_result.trades],
            "events": list(cfd_result.events),
        },
        "demo_executor_fixture": {
            "product": "CTRADER_DEMO_EXECUTOR_FIXTURE",
            "virtual_only": True,
            "server_contacted": False,
            "execution": execution.to_dict(),
            "status": executor.status(),
        },
        "official_demo_adapter": official_probe,
        "limitations": [
            limitation_sdk,
            "Pepperstone, aplicación OAuth, cuenta DEMO y permisos no verificados.",
            "Los precios/órdenes son fixtures sintéticos; no representan fills ni rentabilidad externa.",
        ],
    }
    if report_path is not None:
        target = Path(report_path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
        )
        result["report_path"] = str(target)
    client.close()
    return result


__all__ = ["run_ctrader_fixture"]
