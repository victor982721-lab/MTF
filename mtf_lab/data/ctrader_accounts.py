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

_ACCOUNT_ID_FIELDS = (
    "ctidTraderAccountId",
    "ctid_trader_account_id",
    "account_id",
    "id",
)
_MAX_ACCOUNT_ID = (1 << 63) - 1


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
    account_id = _read_account_id(item)
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


def _read_account_id(item: Any) -> int:
    """Read one positive account id without lossy coercion.

    Mapping payloads may expose compatibility aliases, so every populated
    alias is checked for the same identity.  Generated Protobuf messages do
    not expose those aliases; ``read_field`` retains its presence-aware
    behavior for their repeated/default fields.
    """

    if isinstance(item, Mapping):
        values = [item[name] for name in _ACCOUNT_ID_FIELDS if name in item and item[name] is not None]
        if not values:
            raise CTraderDataError("respuesta OAuth contiene una cuenta sin account_id válido")
        account_ids = tuple(_coerce_account_id(value) for value in values)
        if len(set(account_ids)) != 1:
            raise CTraderDataError("respuesta OAuth contiene aliases de account_id conflictivos")
        return account_ids[0]
    value = read_field(
        item,
        *_ACCOUNT_ID_FIELDS,
        default=item if isinstance(item, (int, str)) else None,
    )
    return _coerce_account_id(value)


def _coerce_account_id(value: Any) -> int:
    """Accept only integer account ids or decimal strings."""

    if isinstance(value, bool):
        raise CTraderDataError("respuesta OAuth contiene una cuenta sin account_id válido")
    if isinstance(value, int):
        account_id = value
    elif isinstance(value, str):
        text = value.strip()
        if not text or not text.isascii() or not text.isdecimal():
            raise CTraderDataError("respuesta OAuth contiene una cuenta sin account_id válido")
        account_id = int(text)
    else:
        raise CTraderDataError("respuesta OAuth contiene una cuenta sin account_id válido")
    if account_id <= 0:
        raise CTraderDataError("respuesta OAuth contiene un account_id no positivo")
    if account_id > _MAX_ACCOUNT_ID:
        raise CTraderDataError("respuesta OAuth contiene un account_id fuera de rango int64")
    return account_id


def _account_environment(is_live: Any) -> str:
    if is_live is None:
        return "UNKNOWN"
    if isinstance(is_live, bool):
        return "LIVE" if is_live else "DEMO"
    if isinstance(is_live, str):
        value = is_live.strip().lower()
        if value in {"1", "true", "yes", "live"}:
            return "LIVE"
        if value in {"0", "false", "no", "demo"}:
            return "DEMO"
        return "UNKNOWN"
    if isinstance(is_live, int) and is_live in {0, 1}:
        return "LIVE" if is_live else "DEMO"
    # A malformed server/mapping value must not be promoted to DEMO.  The
    # selector and execution gates intentionally reject UNKNOWN.
    return "UNKNOWN"


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
