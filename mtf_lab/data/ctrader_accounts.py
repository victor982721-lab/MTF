"""Account-list response normalization for the cTrader auth boundary.

The functions here only consume generated Protobuf/mapping payloads and keep
non-secret account metadata. They intentionally do not import the session or
market provider, preventing an authentication/market dependency cycle.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .ctrader_errors import CTraderDataError
from .ctrader_protocol import enum_name, read_field


def normalize_account_payload(payload: Any) -> dict[str, Any]:
    raw_records = read_field(
        payload,
        "ctidTraderAccount",
        "ctidTraderAccounts",
        "accounts",
        "records",
        default=(),
    )
    if isinstance(payload, (list, tuple)):
        raw_records = payload
    records = [_normalize_account_record(item) for item in _as_sequence(raw_records)]
    ids = [record["account_id"] for record in records]
    if len(ids) != len(set(ids)):
        raise CTraderDataError("respuesta OAuth contiene account_id duplicados")
    permission_scope = enum_name(
        payload,
        "permissionScope",
        read_field(payload, "permissionScope", "permission_scope", default=None),
    )
    if permission_scope is not None:
        for record in records:
            record["permission_scope"] = permission_scope
    return {"records": records, "permissionScope": permission_scope}


def _normalize_account_record(item: Any) -> dict[str, Any]:
    value = read_field(
        item,
        "ctidTraderAccountId",
        "ctid_trader_account_id",
        "account_id",
        "id",
        default=item if isinstance(item, (int, str)) else None,
    )
    try:
        account_id = int(value)
    except (TypeError, ValueError) as exc:
        raise CTraderDataError("respuesta OAuth contiene una cuenta sin account_id válido") from exc
    if account_id <= 0:
        raise CTraderDataError("respuesta OAuth contiene un account_id no positivo")
    is_live = read_field(item, "isLive", "is_live", default=None)
    environment = _account_environment(is_live)
    record: dict[str, Any] = {"account_id": account_id, "environment": environment}
    for source_name, output_name, converter in (
        ("traderLogin", "trader_login", int),
        ("lastClosingDealTimestamp", "last_closing_deal_timestamp", int),
        ("lastBalanceUpdateTimestamp", "last_balance_update_timestamp", int),
        ("brokerTitleShort", "broker_title_short", str),
    ):
        raw_value = read_field(item, source_name, output_name, default=None)
        if raw_value is None:
            continue
        try:
            record[output_name] = converter(raw_value)
        except (TypeError, ValueError):
            continue
    return record


def _account_environment(is_live: Any) -> str:
    if is_live is None:
        return "UNKNOWN"
    if isinstance(is_live, str):
        return "LIVE" if is_live.strip().lower() in {"1", "true", "yes", "live"} else "DEMO"
    return "LIVE" if bool(is_live) else "DEMO"


def _as_sequence(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes, Mapping)):
        return (value,)
    try:
        return tuple(value)
    except TypeError:
        return (value,)


__all__ = ["normalize_account_payload"]
