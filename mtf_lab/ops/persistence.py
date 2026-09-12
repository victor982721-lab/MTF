"""Durable, idempotent SQLite storage for MTF Lab runs.

The database is an audit log rather than a cache of only the latest values.
Events and candles therefore have stable identities, while corrected candles
are stored as revisions.  A correction never updates a previously recorded
decision or signal.  All mutating operations are transactional and use
``INSERT ... ON CONFLICT DO NOTHING`` so a restarted replay can safely submit
the same input again.

Only the Python standard library is required.  The helpers intentionally
accept mappings *and* dataclass/object records; this is the integration seam
between this operational layer and the core/data packages.
"""

from __future__ import annotations

import base64
import contextlib
import dataclasses
import json
import os
import sqlite3
import threading
import uuid
from collections.abc import Iterable, Iterator, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol, cast

from ..core.canonical import canonical_json as _strict_canonical_json
from ..core.canonical import fingerprint as _strict_fingerprint

# Version 4 is an additive persistence migration.  The original binary
# projections remain v3-compatible: existing rows are not rewritten and the
# legacy ``simulations`` table keeps its stake/WIN/LOSS meaning.
SCHEMA_VERSION = 4
PERSISTENCE_EXTENSION_VERSION = 1

TERMINAL_SIMULATION_OUTCOMES = frozenset({"WIN", "LOSS", "TIE", "INDETERMINATE"})
CFD_TERMINAL_STATES = frozenset({"CLOSED", "REJECTED", "UNKNOWN"})
_CFD_STATE_RANK = {"PENDING": 0, "FILLED": 1, "CLOSED": 2, "REJECTED": 2, "UNKNOWN": 2}
_CFD_TRANSITIONS = {
    "PENDING": frozenset({"FILLED", "REJECTED", "UNKNOWN"}),
    "FILLED": frozenset({"CLOSED", "UNKNOWN"}),
}
_CFD_DECIMAL_FIELDS = (
    "units",
    "horizon_seconds",
    "pip_size",
    "entry_price",
    "close_price",
    "pips",
    "gross_pnl_quote",
    "costs_quote",
    "commission_quote",
    "slippage_quote",
    "financing_quote",
    "gross_pnl_account",
    "costs_account",
    "net_pnl",
    "conversion_rate",
)
_CFD_TIME_FIELDS = (
    "detected_at",
    "signal_available_at",
    "decision_at",
    "entry_target_at",
    "entry_market_at",
    "entry_available_at",
    "close_target_at",
    "close_market_at",
    "close_available_at",
)
_CFD_SEMANTIC_FIELDS = (
    "product",
    "trade_id",
    "signal_id",
    "instrument",
    "direction",
    "units",
    "horizon_seconds",
    "state",
    "detected_at",
    "signal_available_at",
    "decision_at",
    "entry_target_at",
    "fill_policy",
    "close_policy",
    "pip_size",
    "price_precision",
    "account_currency",
    "quote_currency",
    "entry_market_at",
    "entry_available_at",
    "entry_quote_id",
    "entry_price",
    "entry_side",
    "close_target_at",
    "close_market_at",
    "close_available_at",
    "close_quote_id",
    "close_price",
    "pips",
    "gross_pnl_quote",
    "commission_quote",
    "slippage_quote",
    "financing_quote",
    "gross_pnl_account",
    "costs_account",
    "net_pnl",
    "conversion_rate",
    "close_observed",
    "economic_state",
    "costs_quote",
    "economic_reason",
    "quality",
    "reason",
)
_CFD_STATIC_FIELDS = (
    "product",
    "trade_id",
    "signal_id",
    "instrument",
    "direction",
    "units",
    "horizon_seconds",
    "detected_at",
    "signal_available_at",
    "decision_at",
    "entry_target_at",
    "fill_policy",
    "close_policy",
    "pip_size",
    "price_precision",
    "account_currency",
    "quote_currency",
    "variant",
    "partition",
    "analysis_config_hash",
    "contract_hash",
)
_CFD_ROW_COLUMNS = (
    "session_id",
    "analysis_id",
    "trade_id",
    "signal_id",
    "product",
    "variant",
    "partition",
    "analysis_config_hash",
    "contract_hash",
    "instrument",
    "direction",
    "units",
    "horizon_seconds",
    "state",
    "detected_at",
    "signal_available_at",
    "decision_at",
    "entry_target_at",
    "fill_policy",
    "close_policy",
    "pip_size",
    "price_precision",
    "account_currency",
    "quote_currency",
    "entry_market_at",
    "entry_available_at",
    "entry_quote_id",
    "entry_price",
    "entry_side",
    "close_target_at",
    "close_market_at",
    "close_available_at",
    "close_quote_id",
    "close_price",
    "pips",
    "gross_pnl_quote",
    "commission_quote",
    "slippage_quote",
    "financing_quote",
    "gross_pnl_account",
    "costs_account",
    "net_pnl",
    "conversion_rate",
    "close_observed",
    "economic_state",
    "costs_quote",
    "economic_reason",
    "quality",
    "reason",
    "lineage_json",
    "semantic_hash",
    "payload_json",
    "terminal",
    "created_at",
    "updated_at",
)
_CFD_UPDATE_COLUMNS = tuple(column for column in _CFD_ROW_COLUMNS[3:] if column != "created_at")


class IdempotencyConflict(RuntimeError):
    """Misma identidad persistente con contenido/configuración distinta."""


class _ModelDumpRecord(Protocol):
    def model_dump(self) -> Mapping[str, Any]: ...


def _mapping(value: Any) -> dict[str, Any]:
    """Return a shallow JSON-friendly mapping for a record-like value."""

    if isinstance(value, Mapping):
        return dict(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    if hasattr(value, "model_dump"):
        try:
            result = cast(_ModelDumpRecord, value).model_dump()
            if isinstance(result, Mapping):
                return dict(result)
        except Exception:  # pragma: no cover - defensive adapter boundary
            pass
    if hasattr(value, "__dict__"):
        return {key: val for key, val in vars(value).items() if not key.startswith("_")}
    raise TypeError(f"record must be a mapping/dataclass/object, got {type(value)!r}")


def _get(value: Any, *names: str, default: Any = None) -> Any:
    record = _mapping(value)
    for name in names:
        if name in record and record[name] is not None:
            return record[name]
    return default


def canonical_json(value: object) -> str:
    """Serialize using the strict project-wide canonical value contract.

    Persistence identities must reject unsupported objects instead of silently
    turning them into process-dependent ``str`` representations.
    """

    return _strict_canonical_json(value)


def payload_hash(value: object) -> str:
    """Return the project canonical fingerprint for an identity payload."""

    return _strict_fingerprint(value)


def utc_iso(value: Any = None) -> str:
    """Normalize timestamps to an explicit UTC ISO-8601 string.

    Naive datetimes are treated as UTC only at this low-level storage seam;
    importers should reject a naive source timestamp unless a timezone was
    explicitly supplied in their configuration.
    """

    if value is None:
        dt = datetime.now(UTC)
    elif isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), UTC)
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            # Keep a clear, stable failure instead of silently storing a
            # provider-local or malformed timestamp.
            raise ValueError(f"invalid timestamp: {value!r}") from None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json_load(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _decimal_text(value: object, *, name: str, positive: bool = False, allow_none: bool = True) -> str | None:
    """Validate a product amount and retain its exact decimal spelling."""

    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{name} es obligatorio")
    if isinstance(value, bool):
        raise ValueError(f"{name} no puede ser booleano")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} no es Decimal válido") from exc
    if not number.is_finite():
        raise ValueError(f"{name} debe ser finito")
    if positive and number <= 0:
        raise ValueError(f"{name} debe ser positivo")
    return str(number)


def _cfd_time_text(value: object, *, name: str, allow_none: bool = True) -> str | None:
    if value is None:
        if allow_none:
            return None
        raise ValueError(f"{name} es obligatorio")
    return utc_iso(value)


def _cfd_source(trade: Any) -> dict[str, Any]:
    if isinstance(trade, Mapping):
        source = dict(trade)
    elif hasattr(trade, "to_dict"):
        converted = trade.to_dict()
        if not isinstance(converted, Mapping):
            raise TypeError("CFD trade.to_dict() debe devolver un mapping")
        source = dict(converted)
    else:
        source = _mapping(trade)
    if "stake" in source:
        raise ValueError("un CFD usa units; stake pertenece al producto binario")
    return source


def _cfd_aliases(source: Mapping[str, Any]) -> dict[str, Any]:
    aliases = {
        "id": "trade_id",
        "identity": "trade_id",
        "detected_ts": "detected_at",
        "signal_available_ts": "signal_available_at",
        "decision_ts": "decision_at",
        "entry_target_ts": "entry_target_at",
        "entry_market_ts": "entry_market_at",
        "entry_available_ts": "entry_available_at",
        "close_target_ts": "close_target_at",
        "close_market_ts": "close_market_at",
        "close_available_ts": "close_available_at",
    }
    record = dict(source)
    for alias, canonical in aliases.items():
        if canonical not in record and alias in record:
            record[canonical] = record[alias]
        record.pop(alias, None)
    return record


def _cfd_require_identity(record: Mapping[str, Any]) -> None:
    fields = ("trade_id", "signal_id", "instrument", "direction", "units", "horizon_seconds", "state", "detected_at")
    for name in fields:
        value = record.get(name)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValueError(f"CFD trade requiere {name}")


def _cfd_extract_economic(record: dict[str, Any]) -> None:
    economic = record.get("economic_result")
    if not isinstance(economic, Mapping):
        return
    fields = (
        "economic_state",
        "gross_pnl_quote",
        "costs_quote",
        "gross_pnl_account",
        "costs_account",
        "net_pnl",
        "economic_reason",
    )
    for field in fields:
        if field not in record and field in economic:
            record[field] = economic[field]


def _cfd_normalize_base(record: dict[str, Any]) -> None:
    record["trade_id"] = str(record["trade_id"])
    record["signal_id"] = str(record["signal_id"])
    record["instrument"] = str(record["instrument"]).upper()
    record["direction"] = str(getattr(record["direction"], "value", record["direction"])).upper()
    record["state"] = str(getattr(record["state"], "value", record["state"])).upper()
    if record["state"] not in _CFD_STATE_RANK:
        raise ValueError(f"estado CFD no soportado: {record['state']!r}")
    if record["direction"] not in {"LONG", "SHORT"}:
        raise ValueError(f"dirección CFD no soportada: {record['direction']!r}")


def _cfd_normalize_numbers(record: dict[str, Any]) -> None:
    for name in _CFD_DECIMAL_FIELDS:
        record[name] = _decimal_text(record.get(name), name=name, positive=name in {"units", "horizon_seconds"})
    precision = record.get("price_precision")
    if precision is not None and (isinstance(precision, bool) or not isinstance(precision, int) or precision < 0):
        raise ValueError("price_precision CFD inválido")
    record.setdefault("price_precision", None)


def _cfd_normalize_times(record: dict[str, Any]) -> None:
    for name in _CFD_TIME_FIELDS:
        record[name] = _cfd_time_text(record.get(name), name=name, allow_none=name != "detected_at")


def _cfd_close_observed(record: Mapping[str, Any]) -> bool:
    explicit = record.get("close_observed")
    if explicit is not None:
        if not isinstance(explicit, bool):
            raise ValueError("close_observed CFD debe ser booleano")
        return explicit
    return record.get("close_market_at") is not None and record.get("close_price") is not None


def _cfd_economic_state(record: Mapping[str, Any]) -> str:
    value = record.get("economic_state")
    if value is None:
        return (
            "NOT_SETTLED"
            if record.get("state") in {"PENDING", "FILLED"}
            else ("DETERMINED" if record.get("net_pnl") is not None else "INDETERMINATE")
        )
    return str(getattr(value, "value", value)).upper()


def _validate_cfd_economic_state(record: Mapping[str, Any], state: str) -> None:
    if state not in {"NOT_SETTLED", "DETERMINED", "INDETERMINATE"}:
        raise ValueError(f"economic_state CFD no soportado: {state!r}")
    if record.get("state") in {"PENDING", "FILLED"} and state != "NOT_SETTLED":
        raise ValueError("un CFD no cerrado debe conservar economic_state=NOT_SETTLED")
    if state == "DETERMINED" and record.get("net_pnl") is None:
        raise ValueError("economic_state=DETERMINED requiere net_pnl exacto")


def _cfd_normalize_economic_state(record: dict[str, Any]) -> None:
    state = _cfd_economic_state(record)
    _validate_cfd_economic_state(record, state)
    record["close_observed"] = _cfd_close_observed(record)
    record["economic_state"] = state


def _cfd_normalize_metadata(record: dict[str, Any]) -> None:
    record.setdefault("fill_policy", "unknown")
    record.setdefault("close_policy", "unknown")
    record["fill_policy"] = str(record["fill_policy"])
    record["close_policy"] = str(record["close_policy"])
    record["account_currency"] = str(record.get("account_currency") or "UNKNOWN").upper()
    quote_currency = record.get("quote_currency")
    record["quote_currency"] = str(quote_currency).upper() if quote_currency else None
    quality_value = record.get("quality") or "UNKNOWN"
    record["quality"] = str(getattr(quality_value, "value", quality_value)).upper()
    record["reason"] = str(record["reason"]) if record.get("reason") is not None else None
    record["economic_reason"] = (
        str(record["economic_reason"]) if record.get("economic_reason") is not None else record["reason"]
    )
    lineage = record.get("lineage")
    if lineage is None:
        record["lineage"] = {}
    elif isinstance(lineage, Mapping):
        record["lineage"] = dict(lineage)
    else:
        raise ValueError("lineage CFD debe ser un mapping")
    for field in _CFD_SEMANTIC_FIELDS:
        record.setdefault(field, None)


def _normalise_cfd_trade(trade: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a validated CFD row and its immutable semantic projection."""

    record = _cfd_aliases(_cfd_source(trade))
    _cfd_require_identity(record)
    product = str(record.get("product") or "FOREX_CFD_LOCAL_PAPER")
    if product != "FOREX_CFD_LOCAL_PAPER":
        raise ValueError(f"producto CFD no soportado: {product!r}")
    record["product"] = product
    _cfd_extract_economic(record)
    _cfd_normalize_base(record)
    _cfd_normalize_numbers(record)
    _cfd_normalize_times(record)
    _cfd_normalize_economic_state(record)
    _cfd_normalize_metadata(record)
    semantic = {field: record.get(field) for field in _CFD_SEMANTIC_FIELDS}
    canonical_json(record)
    canonical_json(semantic)
    return record, semantic


def _capture_mapping(envelope: Any, *, sequence: int | None = None) -> dict[str, Any]:
    """Validate/coerce one explicit capture envelope without inventing receipt."""

    from ..data.capture import CaptureEnvelope, envelope_from_raw

    if isinstance(envelope, CaptureEnvelope):
        return envelope.to_dict()
    if not isinstance(envelope, Mapping):
        if hasattr(envelope, "to_dict"):
            converted = envelope.to_dict()
            if isinstance(converted, Mapping):
                envelope = converted
        if not isinstance(envelope, Mapping):
            raise TypeError("capture envelope debe ser CaptureEnvelope o mapping")
    source = dict(envelope)
    if "capture_schema" in source or {"ingest_sequence", "connection_generation", "message_class", "payload"}.issubset(
        source
    ):
        source.setdefault("capture_schema", 1)
        return CaptureEnvelope.from_mapping(source).to_dict()
    if sequence is None:
        raise ValueError("capture envelope legacy requiere sequence explícita")
    return envelope_from_raw(source, int(sequence)).to_dict()


def _capture_cursor_pair(value: tuple[object, ...] | list[object]) -> tuple[str | None, int, int]:
    if len(value) not in {2, 3}:
        raise ValueError("cursor de captura requiere (available_at, sequence)")
    available = value[0]
    available_text = None if available is None else utc_iso(available)
    try:
        sequence = _coerce_cursor_int(value[1])
        row_id = _coerce_cursor_int(value[2]) if len(value) == 3 else -1
    except (TypeError, ValueError) as exc:
        raise ValueError("cursor de captura inválido") from exc
    return (available_text, sequence, row_id)


def _coerce_cursor_int(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("cursor de captura inválido")
    if isinstance(value, (int, float, str, bytes, bytearray)):
        return int(value)
    raise ValueError("cursor de captura inválido")


def _capture_cursor_mapping(value: Mapping[str, Any]) -> tuple[str | None, int, int]:
    nested = value.get("cursor")
    if isinstance(nested, Mapping):
        value = nested
    available = value.get("available_at", value.get("available_ts"))
    sequence = value.get("ingest_sequence", value.get("sequence"))
    row_id = value.get("capture_row_id", value.get("row_id", -1))
    if sequence is None:
        raise ValueError("cursor de captura requiere ingest_sequence")
    try:
        available_text = None if available is None else utc_iso(available)
        return (available_text, int(sequence), int(row_id))
    except (TypeError, ValueError) as exc:
        raise ValueError("cursor de captura inválido") from exc


def _capture_cursor_text(value: str) -> tuple[str | None, int, int]:
    raw = value.strip()
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        try:
            padded = raw + "=" * (-len(raw) % 4)
            decoded = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("cursor de captura inválido") from exc
    return _capture_cursor(decoded)


def _capture_cursor(value: object) -> tuple[str | None, int, int]:
    """Decode a causal capture cursor ``(available_at, ingest_sequence)``."""

    if isinstance(value, bool):
        raise ValueError("cursor de captura inválido")
    if isinstance(value, int):
        return (None, int(value), -1)
    if isinstance(value, (tuple, list)):
        return _capture_cursor_pair(value)
    if isinstance(value, Mapping):
        return _capture_cursor_mapping(value)
    if isinstance(value, str):
        return _capture_cursor_text(value)
    raise ValueError("cursor de captura inválido")


def _capture_cursor_token(available_at: str | None, sequence: int, row_id: int) -> str:
    raw = {"v": 1, "available_at": available_at, "ingest_sequence": sequence, "capture_row_id": row_id}
    return base64.urlsafe_b64encode(canonical_json(raw).encode()).decode().rstrip("=")


def _trade_cursor_pair(value: tuple[object, ...] | list[object]) -> tuple[str, int]:
    if len(value) != 2:
        raise ValueError("cursor CFD requiere (detected_at, row_id)")
    try:
        return utc_iso(value[0]), _coerce_cursor_int(value[1])
    except (TypeError, ValueError) as exc:
        raise ValueError("cursor CFD inválido") from exc


def _trade_cursor_mapping(value: Mapping[str, Any]) -> tuple[str, int]:
    timestamp = value.get("detected_at", value.get("detected_ts"))
    row_id = value.get("cfd_trade_row_id", value.get("row_id"))
    if timestamp is None or row_id is None:
        raise ValueError("cursor CFD inválido")
    try:
        return utc_iso(timestamp), int(row_id)
    except (TypeError, ValueError) as exc:
        raise ValueError("cursor CFD inválido") from exc


def _trade_cursor_text(value: str) -> tuple[str, int]:
    raw = value.strip()
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        try:
            padded = raw + "=" * (-len(raw) % 4)
            decoded = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("cursor CFD inválido") from exc
    return _trade_cursor(decoded)


def _trade_cursor(value: object) -> tuple[str, int]:
    """Decode the bounded ``(detected_at, row_id)`` trade cursor."""

    if isinstance(value, (tuple, list)):
        return _trade_cursor_pair(value)
    if isinstance(value, Mapping):
        return _trade_cursor_mapping(value)
    if isinstance(value, str):
        return _trade_cursor_text(value)
    raise ValueError("cursor CFD inválido")


def _optional_text(value: object | None) -> str | None:
    return str(value) if value is not None else None


def _cfd_apply_scope(
    record: dict[str, Any],
    semantic: dict[str, Any],
    analysis_id: str,
    variant: str | None,
    partition: str | None,
    analysis_config_hash: str | None,
    contract_hash: str | None,
    config_hash: str | None,
    contract: str | None,
) -> None:
    resolved_config = analysis_config_hash if analysis_config_hash is not None else config_hash
    resolved_config = resolved_config if resolved_config is not None else record.get("analysis_config_hash")
    resolved_contract = contract_hash if contract_hash is not None else contract
    resolved_contract = resolved_contract if resolved_contract is not None else record.get("contract_hash")
    record.update(
        {
            "variant": _optional_text(variant if variant is not None else record.get("variant")),
            "partition": _optional_text(partition if partition is not None else record.get("partition")),
            "analysis_config_hash": _optional_text(resolved_config),
            "contract_hash": _optional_text(resolved_contract),
        }
    )
    semantic.update(
        {
            "analysis_id": analysis_id,
            "variant": record["variant"],
            "partition": record["partition"],
            "analysis_config_hash": record["analysis_config_hash"],
            "contract_hash": record["contract_hash"],
        }
    )


def _cfd_row_values(
    session_id: str,
    analysis_id: str,
    record: Mapping[str, Any],
    semantic_hash: str,
    payload_text: str,
    now: str,
) -> tuple[Any, ...]:
    state = str(record["state"])
    return (
        session_id,
        analysis_id,
        record["trade_id"],
        record["signal_id"],
        record["product"],
        record["variant"],
        record["partition"],
        record["analysis_config_hash"],
        record["contract_hash"],
        record["instrument"],
        record["direction"],
        record["units"],
        record["horizon_seconds"],
        state,
        record["detected_at"],
        record["signal_available_at"],
        record["decision_at"],
        record["entry_target_at"],
        record["fill_policy"],
        record["close_policy"],
        record["pip_size"],
        record["price_precision"],
        record["account_currency"],
        record["quote_currency"],
        record["entry_market_at"],
        record["entry_available_at"],
        record["entry_quote_id"],
        record["entry_price"],
        record["entry_side"],
        record["close_target_at"],
        record["close_market_at"],
        record["close_available_at"],
        record["close_quote_id"],
        record["close_price"],
        record["pips"],
        record["gross_pnl_quote"],
        record["commission_quote"],
        record["slippage_quote"],
        record["financing_quote"],
        record["gross_pnl_account"],
        record["costs_account"],
        record["net_pnl"],
        record["conversion_rate"],
        int(record["close_observed"]),
        record["economic_state"],
        record["costs_quote"],
        record["economic_reason"],
        record["quality"],
        record["reason"],
        canonical_json(record["lineage"]),
        semantic_hash,
        payload_text,
        int(state in CFD_TERMINAL_STATES),
        now,
        now,
    )


def _cfd_existing_action(
    existing: sqlite3.Row | None,
    record: Mapping[str, Any],
    semantic_hash: str,
    session_id: str,
    analysis_id: str,
) -> bool | None:
    """Return False for an idempotent duplicate, True for an update."""

    if existing is None:
        return None
    if str(existing[1]) == semantic_hash:
        return False
    previous_state = str(existing[0]).upper()
    existing_record = _json_load(existing[2], {}) or {}
    if isinstance(existing_record, Mapping):
        changed_static = [field for field in _CFD_STATIC_FIELDS if existing_record.get(field) != record.get(field)]
        if changed_static:
            raise IdempotencyConflict(
                f"conflicto semántico CFD en {changed_static}: identidad={(session_id, analysis_id, record['trade_id'])!r}"
            )
    if previous_state in CFD_TERMINAL_STATES:
        raise IdempotencyConflict(
            f"operación CFD terminal inmutable: identidad={(session_id, analysis_id, record['trade_id'])!r} ya está en {previous_state}"
        )
    state = str(record["state"])
    if state not in _CFD_TRANSITIONS.get(previous_state, frozenset()):
        raise IdempotencyConflict(
            f"transición CFD no permitida: {previous_state} -> {state} para {(session_id, analysis_id, record['trade_id'])!r}"
        )
    return True


def _cfd_update_row(
    conn: sqlite3.Connection,
    row_values: tuple[Any, ...],
    session_id: str,
    analysis_id: str,
    trade_id: str,
) -> None:
    assignments = ",".join(f"{column}=?" for column in _CFD_UPDATE_COLUMNS)
    values = tuple(row_values[_CFD_ROW_COLUMNS.index(column)] for column in _CFD_UPDATE_COLUMNS)
    conn.execute(
        f"UPDATE cfd_trades SET {assignments} WHERE session_id=? AND analysis_id=? AND trade_id=?",
        (*values, session_id, analysis_id, trade_id),
    )


def _persist_cfd_row(
    store: SQLiteStore,
    session_id: str,
    analysis_id: str,
    record: Mapping[str, Any],
    semantic_hash: str,
    payload_text: str,
    row_values: tuple[Any, ...],
) -> bool:
    with store.transaction() as conn:
        existing = conn.execute(
            "SELECT state,semantic_hash,payload_json FROM cfd_trades WHERE session_id=? AND analysis_id=? AND trade_id=?",
            (session_id, analysis_id, record["trade_id"]),
        ).fetchone()
        action = _cfd_existing_action(existing, record, semantic_hash, session_id, analysis_id)
        if action is False:
            return False
        if action is True:
            _cfd_update_row(conn, row_values, session_id, analysis_id, record["trade_id"])
            return True
        placeholders = ",".join("?" for _ in _CFD_ROW_COLUMNS)
        conn.execute(
            f"INSERT INTO cfd_trades({','.join(_CFD_ROW_COLUMNS)}) VALUES({placeholders})",
            row_values,
        )
    return True


def _cfd_resolve_state(state: str | None, lifecycle_state: str | None) -> str | None:
    if lifecycle_state is None:
        return state
    if (
        state is not None
        and str(getattr(state, "value", state)).upper()
        != str(getattr(lifecycle_state, "value", lifecycle_state)).upper()
    ):
        raise ValueError("state y lifecycle_state no pueden diferir")
    return lifecycle_state


def _append_cfd_equals(
    clauses: list[str],
    params: list[Any],
    pairs: tuple[tuple[str, object | None], ...],
) -> None:
    for column, value in pairs:
        if value is not None:
            normalized = getattr(value, "value", value)
            clauses.append(f"{column}=?")
            params.append(str(normalized))


def _append_cfd_time_ranges(
    clauses: list[str],
    params: list[Any],
    start_ts: Any | None,
    end_ts: Any | None,
) -> None:
    if start_ts is not None:
        clauses.append("detected_at>=?")
        params.append(utc_iso(start_ts))
    if end_ts is not None:
        clauses.append("detected_at<?")
        params.append(utc_iso(end_ts))


def _cfd_list_where(
    session_id: str,
    analysis_id: str | None,
    trade_id: str | None,
    signal_id: str | None,
    state: str | None,
    lifecycle_state: str | None,
    terminal: bool | None,
    instrument: str | None,
    product: str | None,
    variant: str | None,
    partition: str | None,
    start_ts: Any | None,
    end_ts: Any | None,
) -> tuple[list[str], list[Any]]:
    state = _cfd_resolve_state(state, lifecycle_state)
    clauses = ["session_id=?"]
    params: list[Any] = [session_id]
    _append_cfd_equals(
        clauses,
        params,
        (
            ("analysis_id", analysis_id),
            ("trade_id", trade_id),
            ("signal_id", signal_id),
            ("instrument", instrument),
            ("product", product),
            ("variant", variant),
            ("partition", partition),
        ),
    )
    if state is not None:
        clauses.append("state=?")
        params.append(str(getattr(state, "value", state)).upper())
    if terminal is not None:
        if not isinstance(terminal, bool):
            raise ValueError("terminal debe ser booleano")
        clauses.append("terminal=?")
        params.append(int(terminal))
    _append_cfd_time_ranges(clauses, params, start_ts, end_ts)
    return clauses, params


def _append_capture_time_range(
    clauses: list[str],
    params: list[Any],
    column: str,
    operator: str,
    value: Any | None,
) -> None:
    if value is not None:
        clauses.append(f"{column}{operator}?")
        params.append(utc_iso(value))


def _capture_base_where(
    session_id: str,
    after_sequence: int | None,
    before_sequence: int | None,
    connection_generation: int | None,
    message_class: str | None,
    start_ts: Any | None,
    end_ts: Any | None,
    available_start: Any | None,
    available_end: Any | None,
) -> tuple[list[str], list[Any]]:
    clauses = ["session_id=?"]
    params: list[Any] = [session_id]
    _append_cfd_equals(
        clauses, params, (("connection_generation", connection_generation), ("message_class", message_class))
    )
    if after_sequence is not None:
        if isinstance(after_sequence, bool) or int(after_sequence) < 0:
            raise ValueError("after_sequence inválida")
        clauses.append("ingest_sequence>?")
        params.append(int(after_sequence))
    if before_sequence is not None:
        if isinstance(before_sequence, bool) or int(before_sequence) < 0:
            raise ValueError("before_sequence inválida")
        clauses.append("ingest_sequence<?")
        params.append(int(before_sequence))
    _append_capture_time_range(clauses, params, "event_time", ">=", start_ts)
    _append_capture_time_range(clauses, params, "event_time", "<", end_ts)
    _append_capture_time_range(clauses, params, "available_at", ">=", available_start)
    _append_capture_time_range(clauses, params, "available_at", "<", available_end)
    return clauses, params


def _capture_after_clause(after_cursor: object) -> tuple[str, list[Any]]:
    available, sequence_value, row_id = _capture_cursor(after_cursor)
    if available is None:
        return "ingest_sequence>?", [sequence_value]
    if row_id < 0:
        return "(available_at>? OR (available_at=? AND ingest_sequence>?) OR available_at IS NULL)", [
            available,
            available,
            sequence_value,
        ]
    return (
        "(available_at>? OR (available_at=? AND ingest_sequence>?) OR (available_at=? AND ingest_sequence=? AND capture_row_id>?) OR available_at IS NULL)",
        [available, available, sequence_value, available, sequence_value, row_id],
    )


def _decode_capture_row(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    record = _json_load(result.pop("envelope_json"), {}) or {}
    result["envelope_hash"] = str(result.get("envelope_hash"))
    result["payload"] = record.get("payload")
    fields = (
        "capture_schema",
        "event_time",
        "received_at",
        "available_at",
        "ingest_sequence",
        "connection_generation",
        "source_identity",
        "message_class",
        "availability_policy",
    )
    for field in fields:
        if field in record:
            result[field] = record[field]
    result["cursor"] = {
        "available_at": result.get("available_at"),
        "ingest_sequence": int(result["ingest_sequence"]),
        "capture_row_id": int(result["capture_row_id"]),
    }
    result["cursor_token"] = _capture_cursor_token(
        result.get("available_at"), int(result["ingest_sequence"]), int(result["capture_row_id"])
    )
    return result


class SQLiteStore:
    """Versioned SQLite store used by replay, observation and backtests."""

    def __init__(self, path: str | os.PathLike[str], *, read_only: bool = False, timeout: float = 30.0):
        self.path = Path(path).expanduser()
        self.read_only = read_only
        self._lock = threading.RLock()
        if read_only:
            uri = f"file:{self.path.resolve()}?mode=ro"
            self.conn = sqlite3.connect(uri, uri=True, timeout=timeout, check_same_thread=False)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(self.path, timeout=timeout, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")
        if not read_only:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self._migrate()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def __enter__(self) -> SQLiteStore:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    @contextlib.contextmanager
    def atomic_batch(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """Group detector, product and cursor writes in one outer commit.

        A nested call creates a SQLite savepoint instead of beginning/committing
        a second transaction.  Consequently a failure in any write rolls the
        complete batch back, while an inner helper cannot publish half a
        checkpoint.  The re-entrant lock serializes writers that share this
        connection; separate connections remain governed by SQLite locking.
        """

        if self.read_only:
            raise RuntimeError("read-only SQLiteStore cannot start a write transaction")
        with self._lock:
            nested = self.conn.in_transaction
            savepoint = f"sp_{uuid.uuid4().hex}" if nested else None
            if savepoint is None:
                self.conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            else:
                self.conn.execute(f"SAVEPOINT {savepoint}")
            try:
                yield self.conn
            except BaseException:
                if savepoint is None:
                    self.conn.rollback()
                else:
                    self.conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    self.conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                raise
            else:
                if savepoint is None:
                    self.conn.commit()
                else:
                    self.conn.execute(f"RELEASE SAVEPOINT {savepoint}")

    @contextlib.contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """Backward-compatible transaction helper backed by ``atomic_batch``."""

        with self.atomic_batch(immediate=immediate) as conn:
            yield conn

    def _migrate(self) -> None:
        with self._lock:
            self.conn.execute("CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            current = self.conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
            version = int(current[0]) if current else 0
            if version > SCHEMA_VERSION:
                raise RuntimeError(f"database schema {version} is newer than supported {SCHEMA_VERSION}")
            if version < 1:
                self._create_v1()
                version = 1
                self.conn.execute(
                    "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(version),),
                )
                self.conn.commit()
            if version < 2:
                self._migrate_v2()
                self.conn.execute(
                    "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("2",),
                )
                self.conn.commit()
                version = 2
            if version < 3:
                self._migrate_v3()
                self.conn.execute(
                    "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("3",),
                )
                self.conn.commit()
                version = 3
            if version < 4:
                self._migrate_v4()
                self.conn.execute(
                    "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(SCHEMA_VERSION),),
                )
                version = 4
            # CREATE IF NOT EXISTS makes recovery from an interrupted extension
            # migration safe, while the marker records the additive contract.
            self._migrate_v4()
            self.conn.execute(
                "INSERT INTO schema_meta(key,value) VALUES('persistence_extension_version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(PERSISTENCE_EXTENSION_VERSION),),
            )
            self.conn.commit()

    def _create_v1(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                status TEXT NOT NULL,
                mode TEXT NOT NULL CHECK(mode IN ('LIVE','REPLAY','SYNTHETIC','BACKTEST')),
                provider TEXT NOT NULL,
                instrument TEXT NOT NULL,
                code_version TEXT,
                seed INTEGER,
                dataset_ref TEXT,
                config_hash TEXT,
                config_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS configs (
                config_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                name TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                config_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(session_id, name, config_hash)
            );
            CREATE TABLE IF NOT EXISTS events (
                event_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                event_id TEXT NOT NULL,
                source TEXT NOT NULL,
                instrument TEXT NOT NULL,
                event_ts TEXT NOT NULL,
                received_ts TEXT,
                available_ts TEXT,
                kind TEXT NOT NULL,
                price_base TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                is_correction INTEGER NOT NULL DEFAULT 0,
                UNIQUE(session_id, event_id)
            );
            CREATE INDEX IF NOT EXISTS events_session_time ON events(session_id, event_ts);
            CREATE TABLE IF NOT EXISTS candles (
                candle_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                candle_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT NOT NULL,
                available_ts TEXT,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL,
                closed INTEGER NOT NULL,
                source TEXT NOT NULL,
                price_base TEXT NOT NULL,
                quality TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                provenance_json TEXT NOT NULL,
                UNIQUE(session_id, candle_id)
            );
            CREATE INDEX IF NOT EXISTS candles_lookup ON candles(session_id, instrument, timeframe, start_ts, revision);
            CREATE TABLE IF NOT EXISTS decisions (
                decision_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                decision_id TEXT NOT NULL,
                observed_ts TEXT NOT NULL,
                available_ts TEXT,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, decision_id)
            );
            CREATE INDEX IF NOT EXISTS decisions_session_time ON decisions(session_id, observed_ts);
            CREATE TABLE IF NOT EXISTS signals (
                signal_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                signal_id TEXT NOT NULL,
                episode_id TEXT,
                detected_ts TEXT NOT NULL,
                available_ts TEXT,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, signal_id)
            );
            CREATE INDEX IF NOT EXISTS signals_lookup ON signals(session_id, detected_ts);
            CREATE TABLE IF NOT EXISTS discards (
                discard_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                discard_id TEXT NOT NULL,
                decision_id TEXT,
                observed_ts TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                required INTEGER NOT NULL,
                condition_status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, discard_id)
            );
            CREATE INDEX IF NOT EXISTS discards_reason ON discards(session_id, reason_code);
            CREATE TABLE IF NOT EXISTS simulations (
                simulation_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                simulation_id TEXT NOT NULL,
                signal_id TEXT,
                simulation_type TEXT NOT NULL,
                horizon_seconds REAL NOT NULL,
                direction TEXT NOT NULL,
                detected_ts TEXT NOT NULL,
                entry_ts TEXT,
                expiry_ts TEXT NOT NULL,
                entry_price REAL,
                final_price REAL,
                outcome TEXT NOT NULL,
                stake REAL NOT NULL,
                net_result REAL,
                price_base TEXT NOT NULL,
                quality TEXT NOT NULL,
                resolution TEXT NOT NULL,
                assumptions_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, simulation_id)
            );
            CREATE INDEX IF NOT EXISTS simulations_lookup ON simulations(session_id, detected_ts);
            CREATE TABLE IF NOT EXISTS checkpoints (
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                checkpoint_name TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                cursor_json TEXT NOT NULL,
                events_processed INTEGER NOT NULL DEFAULT 0,
                last_event_id TEXT,
                state_json TEXT NOT NULL,
                PRIMARY KEY(session_id, checkpoint_name)
            );
            CREATE TABLE IF NOT EXISTS metrics (
                metric_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                metric_name TEXT NOT NULL,
                segment_json TEXT NOT NULL,
                value REAL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(session_id, metric_name, segment_json)
            );
            """
        )

    def _migrate_v2(self) -> None:
        """Add analysis/variant lineage without rewriting existing captures."""
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS analyses (
                analysis_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                dataset_hash TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                code_version TEXT,
                variant TEXT NOT NULL,
                contract_hash TEXT,
                partition TEXT NOT NULL DEFAULT 'all',
                status TEXT NOT NULL DEFAULT 'COMPLETED',
                created_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                UNIQUE(session_id,dataset_hash,config_hash,variant,contract_hash,partition)
            );
            CREATE INDEX IF NOT EXISTS analyses_session ON analyses(session_id,created_at);
            """
        )
        existing_candles = {str(row[1]) for row in self.conn.execute("PRAGMA table_info(candles)")}
        if "payload_json" not in existing_candles:
            self.conn.execute("ALTER TABLE candles ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}'")
        for table in ("decisions", "discards", "signals", "simulations"):
            existing = {str(row[1]) for row in self.conn.execute(f"PRAGMA table_info({table})")}
            for column in ("analysis_id", "variant", "analysis_config_hash", "contract_hash", "partition"):
                if column not in existing:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")
            self.conn.execute(f"CREATE INDEX IF NOT EXISTS {table}_analysis ON {table}(session_id,analysis_id)")
        self.conn.commit()

    def _ensure_v3_checkpoint_table(self, conn: sqlite3.Connection) -> bool:
        """Prepare the v3 checkpoint table and return whether legacy rows exist."""

        legacy_name = "checkpoints_v2_legacy"
        checkpoints_exists = self._table_exists("checkpoints")
        legacy_exists = self._table_exists(legacy_name)
        if checkpoints_exists:
            checkpoint_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(checkpoints)")}
            if "analysis_id" not in checkpoint_columns:
                if legacy_exists:
                    raise RuntimeError("migración v3 ambigua: existen checkpoints v2 y checkpoints_v2_legacy")
                conn.execute("ALTER TABLE checkpoints RENAME TO checkpoints_v2_legacy")
                legacy_exists = True
                checkpoints_exists = False
        if not checkpoints_exists:
            if not legacy_exists:
                raise RuntimeError("migración v3 no encuentra checkpoints ni su respaldo legacy")
            conn.execute(
                """
                CREATE TABLE checkpoints (
                    session_id TEXT NOT NULL REFERENCES sessions(session_id),
                    checkpoint_name TEXT NOT NULL,
                    analysis_id TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    cursor_json TEXT NOT NULL,
                    events_processed INTEGER NOT NULL DEFAULT 0,
                    last_event_id TEXT,
                    state_json TEXT NOT NULL,
                    PRIMARY KEY(session_id, checkpoint_name, analysis_id)
                )
                """
            )
        checkpoint_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(checkpoints)")}
        required_checkpoint_columns = {
            "session_id",
            "checkpoint_name",
            "analysis_id",
            "updated_at",
            "cursor_json",
            "events_processed",
            "last_event_id",
            "state_json",
        }
        missing_checkpoint_columns = required_checkpoint_columns - checkpoint_columns
        if missing_checkpoint_columns:
            missing = ", ".join(sorted(missing_checkpoint_columns))
            raise RuntimeError(f"migración v3: checkpoints nuevo incompleto ({missing})")
        return legacy_exists

    @staticmethod
    def _ensure_v3_checkpoint_index(conn: sqlite3.Connection) -> None:
        """Create the v3 lookup index after any legacy table is removed."""

        conn.execute(
            "CREATE INDEX IF NOT EXISTS checkpoints_session_name "
            "ON checkpoints(session_id, checkpoint_name, updated_at)"
        )

    @staticmethod
    def _copy_v2_checkpoints(conn: sqlite3.Connection, legacy_exists: bool) -> None:
        """Copy legacy rows once and drop the source only after a full copy."""

        if not legacy_exists:
            return
        legacy_name = "checkpoints_v2_legacy"
        legacy_columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({legacy_name})")}
        required_legacy_columns = {
            "session_id",
            "checkpoint_name",
            "updated_at",
            "cursor_json",
            "events_processed",
            "last_event_id",
            "state_json",
        }
        missing_legacy_columns = required_legacy_columns - legacy_columns
        if missing_legacy_columns:
            missing = ", ".join(sorted(missing_legacy_columns))
            raise RuntimeError(f"migración v3: checkpoints legacy incompleto ({missing})")
        legacy = conn.execute(
            "SELECT session_id,checkpoint_name,updated_at,cursor_json,events_processed,"
            "last_event_id,state_json FROM checkpoints_v2_legacy"
        ).fetchall()
        for row in legacy:
            cursor = _json_load(row[3], {}) or {}
            state = _json_load(row[6], {}) or {}
            analysis_id = cursor.get("analysis_id") or state.get("analysis_id")
            if not analysis_id and isinstance(state.get("processor"), Mapping):
                analysis_id = state["processor"].get("analysis_id")
            candidate = (
                row[0],
                row[1],
                str(analysis_id or ""),
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
            )
            existing = conn.execute(
                "SELECT session_id,checkpoint_name,analysis_id,updated_at,cursor_json,"
                "events_processed,last_event_id,state_json FROM checkpoints "
                "WHERE session_id=? AND checkpoint_name=? AND analysis_id=?",
                candidate[:3],
            ).fetchone()
            if existing is not None:
                if tuple(existing) != candidate:
                    identity = (row[0], row[1], str(analysis_id or ""))
                    raise RuntimeError(
                        f"migración v3: conflicto de checkpoint para {identity!r}; "
                        "se conservan ambas fuentes para recuperación"
                    )
                continue
            conn.execute(
                """INSERT INTO checkpoints(session_id,checkpoint_name,analysis_id,updated_at,cursor_json,events_processed,last_event_id,state_json)
                VALUES(?,?,?,?,?,?,?,?)""",
                candidate,
            )
        conn.execute(f"DROP TABLE {legacy_name}")

    @staticmethod
    def _ensure_v3_projection_tables(conn: sqlite3.Connection) -> None:
        """Complete v3 receipt and signal-membership projections idempotently."""

        existing_candles = {str(row[1]) for row in conn.execute("PRAGMA table_info(candles)")}
        if "received_ts" not in existing_candles:
            conn.execute("ALTER TABLE candles ADD COLUMN received_ts TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_analysis_membership (
                membership_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                signal_id TEXT NOT NULL,
                analysis_id TEXT NOT NULL,
                variant TEXT,
                analysis_config_hash TEXT,
                contract_hash TEXT,
                partition TEXT,
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, signal_id, analysis_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS signal_membership_session_analysis "
            "ON signal_analysis_membership(session_id, analysis_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS signal_membership_session_signal "
            "ON signal_analysis_membership(session_id, signal_id)"
        )

    def _migrate_v3(self) -> None:
        """Namespace checkpoints by analysis and preserve receipt time.

        SQLite cannot add a column to a composite primary key in place.  The
        legacy checkpoint table is therefore copied into a v3 table, deriving
        the old row's analysis id from its cursor/state when available.  A
        blank id is the explicit legacy/capture namespace and is never
        confused with a real analysis id.

        This migration must also be safe to resume after a process stopped
        between the table rename and the marker update.  In particular,
        ``executescript`` is deliberately avoided here: it commits any active
        transaction before running the script, which can leave
        ``checkpoints_v2_legacy`` and a partially populated v3 table behind.
        All DDL and row copying therefore run in one explicit transaction and
        the table-shape checks make both the old and the new partial states
        idempotent.
        """
        with self.atomic_batch(immediate=True) as conn:
            legacy_exists = self._ensure_v3_checkpoint_table(conn)
            self._copy_v2_checkpoints(conn, legacy_exists)
            self._ensure_v3_checkpoint_index(conn)
            self._ensure_v3_projection_tables(conn)

    def _migrate_v4(self) -> None:
        """Add isolated CFD and capture projections without rewriting v3 rows."""

        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS cfd_trades (
                cfd_trade_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                analysis_id TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                signal_id TEXT NOT NULL,
                product TEXT NOT NULL,
                variant TEXT,
                partition TEXT,
                analysis_config_hash TEXT,
                contract_hash TEXT,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                units TEXT NOT NULL,
                horizon_seconds TEXT NOT NULL,
                state TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                signal_available_at TEXT,
                decision_at TEXT,
                entry_target_at TEXT,
                fill_policy TEXT,
                close_policy TEXT,
                pip_size TEXT,
                price_precision INTEGER,
                account_currency TEXT,
                quote_currency TEXT,
                entry_market_at TEXT,
                entry_available_at TEXT,
                entry_quote_id TEXT,
                entry_price TEXT,
                entry_side TEXT,
                close_target_at TEXT,
                close_market_at TEXT,
                close_available_at TEXT,
                close_quote_id TEXT,
                close_price TEXT,
                pips TEXT,
                gross_pnl_quote TEXT,
                commission_quote TEXT,
                slippage_quote TEXT,
                financing_quote TEXT,
                gross_pnl_account TEXT,
                costs_account TEXT,
                net_pnl TEXT,
                conversion_rate TEXT,
                close_observed INTEGER NOT NULL DEFAULT 0,
                economic_state TEXT NOT NULL DEFAULT 'NOT_SETTLED',
                costs_quote TEXT,
                economic_reason TEXT,
                quality TEXT NOT NULL,
                reason TEXT,
                lineage_json TEXT NOT NULL,
                semantic_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                terminal INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(session_id, analysis_id, trade_id)
            );
            CREATE INDEX IF NOT EXISTS cfd_trades_session_analysis ON cfd_trades(session_id, analysis_id, detected_at, cfd_trade_row_id);
            CREATE INDEX IF NOT EXISTS cfd_trades_state ON cfd_trades(session_id, state, updated_at);
            CREATE INDEX IF NOT EXISTS cfd_trades_signal ON cfd_trades(session_id, signal_id);
            CREATE TABLE IF NOT EXISTS capture_envelopes (
                capture_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                ingest_sequence INTEGER NOT NULL,
                connection_generation INTEGER NOT NULL,
                event_time TEXT,
                received_at TEXT,
                available_at TEXT,
                source_identity TEXT,
                message_class TEXT NOT NULL,
                availability_policy TEXT NOT NULL,
                envelope_hash TEXT NOT NULL,
                envelope_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(session_id, connection_generation, ingest_sequence)
            );
            CREATE INDEX IF NOT EXISTS capture_envelopes_order ON capture_envelopes(session_id, available_at, ingest_sequence, capture_row_id);
            CREATE INDEX IF NOT EXISTS capture_envelopes_event ON capture_envelopes(session_id, event_time, ingest_sequence, capture_row_id);
            CREATE UNIQUE INDEX IF NOT EXISTS capture_envelopes_session_sequence ON capture_envelopes(session_id, ingest_sequence);
            """
        )
        existing_cfd = {str(row[1]) for row in self.conn.execute("PRAGMA table_info(cfd_trades)")}
        for column, definition in (
            ("close_observed", "INTEGER NOT NULL DEFAULT 0"),
            ("economic_state", "TEXT NOT NULL DEFAULT 'NOT_SETTLED'"),
            ("costs_quote", "TEXT"),
            ("economic_reason", "TEXT"),
        ):
            if column not in existing_cfd:
                self.conn.execute(f"ALTER TABLE cfd_trades ADD COLUMN {column} {definition}")

    @property
    def schema_version(self) -> int:
        if not self._table_exists("schema_meta"):
            return 0
        row = self.conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        return int(row[0]) if row else 0

    def create_session(
        self,
        *,
        session_id: str | None = None,
        mode: str,
        provider: str,
        instrument: str,
        config: Mapping[str, Any] | None = None,
        code_version: str | None = None,
        seed: int | None = None,
        dataset_ref: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        status: str = "RUNNING",
        started_at: Any = None,
    ) -> str:
        mode = str(mode).upper()
        if mode not in {"LIVE", "REPLAY", "SYNTHETIC", "BACKTEST"}:
            raise ValueError(f"unsupported session mode: {mode}")
        session_id = session_id or str(uuid.uuid4())
        config = dict(config or {})
        metadata = dict(metadata or {})
        created = utc_iso()
        config_text = canonical_json(config)
        with self.transaction(immediate=True) as conn:
            conn.execute(
                """INSERT INTO sessions(
                    session_id,created_at,started_at,status,mode,provider,instrument,
                    code_version,seed,dataset_ref,config_hash,config_json,metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id) DO NOTHING""",
                (
                    session_id,
                    created,
                    utc_iso(started_at) if started_at else created,
                    status,
                    mode,
                    str(provider),
                    str(instrument),
                    code_version,
                    seed,
                    dataset_ref,
                    payload_hash(config),
                    config_text,
                    canonical_json(metadata),
                ),
            )
        return session_id

    def finish_session(self, session_id: str, *, status: str = "COMPLETED", ended_at: Any = None) -> None:
        with self.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE sessions SET status=?, ended_at=? WHERE session_id=?",
                (status, utc_iso(ended_at), session_id),
            )

    def save_config(self, session_id: str, name: str, config: Mapping[str, Any]) -> str:
        config_id = f"{session_id}:{name}:{payload_hash(config)[:16]}"
        text = canonical_json(config)
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO configs(config_id,session_id,name,config_hash,config_json,created_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(session_id,name,config_hash) DO NOTHING""",
                (config_id, session_id, name, payload_hash(config), text, utc_iso()),
            )
        return config_id

    def _event_identity(self, event: Any, *, ordinal: int | None = None) -> tuple[str, dict[str, Any]]:
        record = _mapping(event)
        explicit = _get(record, "event_id", "id", "trade_id", "uid", "source_event_id", "data_id")
        if explicit is None:
            # Importers should provide source_ordinal/source_seq.  Including
            # the full payload and ordinal prevents accidental de-duplication
            # of two equal-price trades at the same source timestamp.
            explicit = f"generated:{ordinal if ordinal is not None else record.get('source_ordinal', '')}:{payload_hash(record)}"
        return str(explicit), record

    def _check_idempotency(
        self, table: str, identity_columns: tuple[str, ...], identity_values: tuple[Any, ...], payload: str
    ) -> bool:
        where = " AND ".join(f"{column}=?" for column in identity_columns)
        row = self.conn.execute(f"SELECT payload_json FROM {table} WHERE {where}", identity_values).fetchone()
        if row is None:
            return False
        if str(row[0]) != payload:
            raise IdempotencyConflict(
                f"conflicto de idempotencia en {table}: identidad={identity_values!r} ya tiene contenido distinto"
            )
        return True

    def create_analysis(
        self,
        session_id: str,
        *,
        dataset_hash: str,
        config_hash: str,
        variant: str,
        contract_hash: str | None = None,
        partition: str = "all",
        code_version: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        identity_extra: Mapping[str, Any] | None = None,
        analysis_id: str | None = None,
        status: str = "COMPLETED",
    ) -> str:
        """Create/reuse one immutable analysis identity for a capture."""
        if not dataset_hash or not config_hash or not variant:
            raise ValueError("dataset_hash, config_hash y variant son obligatorios")
        partition = str(partition or "all")
        identity = {
            "session_id": session_id,
            "dataset_hash": str(dataset_hash),
            "config_hash": str(config_hash),
            "variant": str(variant),
            "contract_hash": str(contract_hash or ""),
            "partition": partition,
            "code_version": str(code_version or ""),
            "identity_extra": dict(identity_extra or {}),
        }
        analysis_id = analysis_id or "an_" + payload_hash(identity)[:32]
        metadata_text = canonical_json(metadata or {})
        with self.transaction(immediate=True) as conn:
            existing = conn.execute(
                "SELECT session_id,dataset_hash,config_hash,variant,contract_hash,partition,metadata_json FROM analyses WHERE analysis_id=?",
                (analysis_id,),
            ).fetchone()
            if existing is not None:
                expected = (session_id, str(dataset_hash), str(config_hash), str(variant), contract_hash, partition)
                actual = tuple(existing[:6])
                if actual != expected:
                    raise IdempotencyConflict(f"analysis_id {analysis_id} ya existe con identidad distinta")
                return analysis_id
            conn.execute(
                """INSERT INTO analyses(analysis_id,session_id,dataset_hash,config_hash,code_version,variant,contract_hash,partition,status,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    analysis_id,
                    session_id,
                    str(dataset_hash),
                    str(config_hash),
                    code_version,
                    str(variant),
                    contract_hash,
                    partition,
                    status,
                    utc_iso(),
                    metadata_text,
                ),
            )
        return analysis_id

    def get_analysis(self, analysis_id: str) -> dict[str, Any] | None:
        if not self._table_exists("analyses"):
            return None
        rows = self._rows("SELECT * FROM analyses WHERE analysis_id=?", (analysis_id,))
        if not rows:
            return None
        row = rows[0]
        row["metadata"] = _json_load(row.pop("metadata_json"), {})
        return row

    def analyses(self, session_id: str | None = None, *, limit: int = 100) -> list[dict[str, Any]]:
        if not self._table_exists("analyses"):
            return []
        if session_id is None:
            rows = self._rows("SELECT * FROM analyses ORDER BY created_at DESC LIMIT ?", (int(limit),))
        else:
            rows = self._rows(
                "SELECT * FROM analyses WHERE session_id=? ORDER BY created_at DESC LIMIT ?", (session_id, int(limit))
            )
        for row in rows:
            row["metadata"] = _json_load(row.pop("metadata_json", None), {})
        return rows

    def save_event(self, session_id: str, event: Any, *, ordinal: int | None = None) -> bool:
        event_id, record = self._event_identity(event, ordinal=ordinal)
        payload = canonical_json(record)
        values = (
            session_id,
            event_id,
            str(_get(record, "source", "provider", default="unknown")),
            str(_get(record, "instrument", "symbol", default="unknown")),
            utc_iso(_get(record, "event_ts", "event_time", "timestamp", "ts", "time")),
            utc_iso(_get(record, "received_ts", "received_at", "receipt_ts", "receipt_at", default=None))
            if _get(record, "received_ts", "received_at", "receipt_ts", "receipt_at") is not None
            else None,
            utc_iso(_get(record, "available_ts", "available_at", default=None))
            if _get(record, "available_ts", "available_at") is not None
            else None,
            str(_get(record, "kind", "event_kind", "type", default="event")),
            str(_get(record, "price_base", "price_basis", "price_type", "base_price", default="unknown")),
            payload,
            int(bool(_get(record, "is_correction", "correction", default=False))),
        )
        with self.transaction() as conn:
            if self._check_idempotency("events", ("session_id", "event_id"), (session_id, event_id), payload):
                return False
            cur = conn.execute(
                """INSERT INTO events(
                session_id,event_id,source,instrument,event_ts,received_ts,available_ts,
                kind,price_base,payload_json,is_correction
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id,event_id) DO NOTHING""",
                values,
            )
        return cur.rowcount == 1

    def save_candle(self, session_id: str, candle: Any, *, ordinal: int | None = None) -> bool:
        record = _mapping(candle)
        explicit = _get(record, "candle_id", "data_id", "source_record_id", "id", "uid")
        start = utc_iso(_get(record, "start_ts", "interval_start", "start", "open_time", "timestamp", "ts"))
        end = utc_iso(_get(record, "end_ts", "interval_end", "end", "close_time"))
        timeframe_value = _get(record, "timeframe", "resolution", "resolution_seconds", "interval")
        if (
            isinstance(timeframe_value, (int, float))
            and float(timeframe_value) >= 60
            and float(timeframe_value) % 60 == 0
        ):
            timeframe = f"M{int(float(timeframe_value) // 60)}"
        else:
            timeframe = str(timeframe_value)
        revision = int(_get(record, "revision", default=0))
        if explicit is None:
            explicit = f"candle:{_get(record, 'instrument', 'symbol', default='unknown')}:{timeframe}:{start}:r{revision}:{ordinal if ordinal is not None else ''}"
        values = (
            session_id,
            str(explicit),
            str(_get(record, "instrument", "symbol", default="unknown")),
            timeframe,
            start,
            end,
            utc_iso(_get(record, "available_ts", "available_at"))
            if _get(record, "available_ts", "available_at") is not None
            else None,
            utc_iso(_get(record, "received_ts", "received_at", "receipt_ts", "receipt_at"))
            if _get(record, "received_ts", "received_at", "receipt_ts", "receipt_at") is not None
            else None,
            float(_get(record, "open", "o")),
            float(_get(record, "high", "h")),
            float(_get(record, "low", "l")),
            float(_get(record, "close", "c")),
            float(_get(record, "volume", "v")) if _get(record, "volume", "v") is not None else None,
            int(bool(_get(record, "closed", "is_closed", default=True))),
            str(_get(record, "source", "provider", default="unknown")),
            str(_get(record, "price_base", "price_basis", "price_type", default="unknown")),
            str(_get(record, "quality", "data_quality", default="UNKNOWN")),
            revision,
            canonical_json(_get(record, "provenance", "provenance_json", default=record)),
            canonical_json(record),
        )
        payload = canonical_json(record)
        with self.transaction() as conn:
            if self._check_idempotency("candles", ("session_id", "candle_id"), (session_id, str(explicit)), payload):
                return False
            cur = conn.execute(
                """INSERT INTO candles(
                session_id,candle_id,instrument,timeframe,start_ts,end_ts,available_ts,received_ts,
                open,high,low,close,volume,closed,source,price_base,quality,revision,provenance_json,payload_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id,candle_id) DO NOTHING""",
                values,
            )
        return cur.rowcount == 1

    def save_decision(
        self,
        session_id: str,
        decision: Any,
        *,
        ordinal: int | None = None,
        analysis_id: str | None = None,
        variant: str | None = None,
        analysis_config_hash: str | None = None,
        contract_hash: str | None = None,
        partition: str | None = None,
    ) -> bool:
        record = _mapping(decision)
        analysis_id = analysis_id or _get(record, "analysis_id")
        variant = variant or _get(record, "variant", "variant_name")
        analysis_config_hash = analysis_config_hash or _get(record, "analysis_config_hash", "config_hash")
        contract_hash = contract_hash or _get(record, "contract_hash")
        partition = partition or _get(record, "partition")
        decision_id = _get(record, "decision_id", "id", "uid")
        if decision_id is None:
            decision_id = f"decision:{_get(record, 'observed_ts', 'timestamp', 'ts')}:{ordinal if ordinal is not None else payload_hash(record)[:16]}"
        observed = utc_iso(_get(record, "observed_ts", "observed_at", "timestamp", "available_at", "ts"))
        values = (
            session_id,
            str(decision_id),
            observed,
            utc_iso(_get(record, "available_ts", "available_at"))
            if _get(record, "available_ts", "available_at") is not None
            else None,
            str(_get(record, "kind", "type", "stage", default="strategy_evaluation")),
            str(_get(record, "status", "decision", default="UNKNOWN")),
            canonical_json(record),
        )
        payload = canonical_json(record)
        with self.transaction() as conn:
            if self._check_idempotency(
                "decisions", ("session_id", "decision_id"), (session_id, str(decision_id)), payload
            ):
                return False
            cur = conn.execute(
                """INSERT INTO decisions(session_id,decision_id,observed_ts,available_ts,kind,status,payload_json,analysis_id,variant,analysis_config_hash,contract_hash,partition)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (*values, analysis_id, variant, analysis_config_hash, contract_hash, partition),
            )
        return cur.rowcount == 1

    def save_signal(
        self,
        session_id: str,
        signal: Any,
        *,
        ordinal: int | None = None,
        analysis_id: str | None = None,
        variant: str | None = None,
        analysis_config_hash: str | None = None,
        contract_hash: str | None = None,
        partition: str | None = None,
    ) -> bool:
        """Persist a signal and its analysis membership independently.

        ``signals`` remains the capture-level canonical row for backwards
        compatible queries.  ``signal_analysis_membership`` is the lineage
        relation: the same source signal id may belong to multiple analyses
        without a false idempotency conflict, while a changed payload inside
        one analysis is still rejected.
        """
        record = _mapping(signal)
        analysis_id = analysis_id or _get(record, "analysis_id")
        analysis_id = str(analysis_id) if analysis_id is not None else None
        variant = variant or _get(record, "variant", "variant_name")
        analysis_config_hash = analysis_config_hash or _get(record, "analysis_config_hash", "config_hash")
        contract_hash = contract_hash or _get(record, "contract_hash")
        partition = partition or _get(record, "partition")
        signal_id = _get(record, "signal_id", "id", "uid")
        if signal_id is None:
            signal_id = f"signal:{_get(record, 'detected_ts', 'detected_at', 'timestamp', 'ts')}:{_get(record, 'direction', 'side', default='UNKNOWN')}:{ordinal if ordinal is not None else payload_hash(record)[:16]}"
        signal_id = str(signal_id)
        detected = utc_iso(_get(record, "detected_ts", "detected_at", "timestamp", "ts"))
        values = (
            session_id,
            signal_id,
            _get(record, "episode_id", "episode"),
            detected,
            utc_iso(_get(record, "available_ts", "available_at"))
            if _get(record, "available_ts", "available_at") is not None
            else None,
            str(_get(record, "instrument", "symbol", default="unknown")),
            str(_get(record, "direction", "side", default="UNKNOWN")).upper(),
            str(_get(record, "status", default="VALID")),
            canonical_json(record),
        )
        payload = canonical_json(record)
        inserted_signal = False
        inserted_membership = False
        membership_analysis = analysis_id or ""
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT payload_json,analysis_id FROM signals WHERE session_id=? AND signal_id=?",
                (session_id, signal_id),
            ).fetchone()
            if existing is None:
                cur = conn.execute(
                    """INSERT INTO signals(session_id,signal_id,episode_id,detected_ts,available_ts,instrument,direction,status,payload_json,analysis_id,variant,analysis_config_hash,contract_hash,partition)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (*values, analysis_id, variant, analysis_config_hash, contract_hash, partition),
                )
                inserted_signal = cur.rowcount == 1
            elif str(existing[0]) != payload:
                # Different analyses may intentionally emit the same signal id
                # with distinct annotations/configuration.  The membership row
                # is the lineage-specific payload; only one analysis may revise
                # its own identity.
                existing_analysis = str(existing[1] or "")
                if not membership_analysis or existing_analysis == membership_analysis:
                    raise IdempotencyConflict(
                        f"conflicto de idempotencia en signals: identidad={(session_id, signal_id)!r} ya tiene contenido distinto dentro del mismo namespace"
                    )
            if membership_analysis:
                membership_id = (
                    "sm_"
                    + payload_hash(
                        {"session_id": session_id, "signal_id": signal_id, "analysis_id": membership_analysis}
                    )[:32]
                )
                member = conn.execute(
                    "SELECT payload_json,variant,analysis_config_hash,contract_hash,partition FROM signal_analysis_membership WHERE session_id=? AND signal_id=? AND analysis_id=?",
                    (session_id, signal_id, membership_analysis),
                ).fetchone()
                if member is not None:
                    expected_meta = (variant, analysis_config_hash, contract_hash, partition)
                    actual_meta = tuple(member[index] for index in range(1, 5))
                    if str(member[0]) != payload or actual_meta != expected_meta:
                        raise IdempotencyConflict(
                            f"conflicto de idempotencia en signal membership: identidad={(session_id, signal_id, membership_analysis)!r} ya tiene contenido distinto"
                        )
                else:
                    conn.execute(
                        """INSERT INTO signal_analysis_membership(membership_id,session_id,signal_id,analysis_id,variant,analysis_config_hash,contract_hash,partition,created_at,payload_json)
                        VALUES(?,?,?,?,?,?,?,?,?,?)""",
                        (
                            membership_id,
                            session_id,
                            signal_id,
                            membership_analysis,
                            variant,
                            analysis_config_hash,
                            contract_hash,
                            partition,
                            utc_iso(),
                            payload,
                        ),
                    )
                    inserted_membership = True
        return inserted_signal or inserted_membership

    def list_signal_memberships(
        self, session_id: str, *, signal_id: str | None = None, analysis_id: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Return lineage memberships, preserving capture signal rows."""
        clauses = ["session_id=?"]
        params: list[Any] = [session_id]
        if signal_id is not None:
            clauses.append("signal_id=?")
            params.append(str(signal_id))
        if analysis_id is not None:
            clauses.append("analysis_id=?")
            params.append(str(analysis_id))
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(
            f"SELECT * FROM signal_analysis_membership WHERE {' AND '.join(clauses)} ORDER BY created_at,membership_id{lim}",
            params,
        )
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def save_discard(
        self,
        session_id: str,
        discard: Any,
        *,
        ordinal: int | None = None,
        analysis_id: str | None = None,
        variant: str | None = None,
        analysis_config_hash: str | None = None,
        contract_hash: str | None = None,
        partition: str | None = None,
    ) -> bool:
        record = _mapping(discard)
        analysis_id = analysis_id or _get(record, "analysis_id")
        variant = variant or _get(record, "variant", "variant_name")
        analysis_config_hash = analysis_config_hash or _get(record, "analysis_config_hash", "config_hash")
        contract_hash = contract_hash or _get(record, "contract_hash")
        partition = partition or _get(record, "partition")
        discard_id = _get(record, "discard_id", "id", "uid")
        if discard_id is None:
            discard_id = f"discard:{_get(record, 'observed_ts', 'timestamp', 'ts')}:{_get(record, 'reason_code', 'reason', default='UNKNOWN')}:{ordinal if ordinal is not None else payload_hash(record)[:16]}"
        observed = utc_iso(_get(record, "observed_ts", "observed_at", "timestamp", "available_at", "ts"))
        values = (
            session_id,
            str(discard_id),
            _get(record, "decision_id"),
            observed,
            str(_get(record, "reason_code", "reason", default="UNKNOWN")),
            int(bool(_get(record, "required", default=True))),
            str(_get(record, "condition_status", "status", default="UNSATISFIED")),
            canonical_json(record),
        )
        payload = canonical_json(record)
        with self.transaction() as conn:
            if self._check_idempotency(
                "discards", ("session_id", "discard_id"), (session_id, str(discard_id)), payload
            ):
                return False
            cur = conn.execute(
                """INSERT INTO discards(session_id,discard_id,decision_id,observed_ts,reason_code,required,condition_status,payload_json,analysis_id,variant,analysis_config_hash,contract_hash,partition)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (*values, analysis_id, variant, analysis_config_hash, contract_hash, partition),
            )
        return cur.rowcount == 1

    def save_simulation(
        self,
        session_id: str,
        simulation: Any,
        *,
        ordinal: int | None = None,
        analysis_id: str | None = None,
        variant: str | None = None,
        analysis_config_hash: str | None = None,
        contract_hash: str | None = None,
        partition: str | None = None,
        allow_update: bool = False,
        ignore_pending_terminal: bool = False,
    ) -> bool:
        record = _mapping(simulation)
        analysis_id = analysis_id or _get(record, "analysis_id")
        variant = variant or _get(record, "variant", "variant_name")
        analysis_config_hash = analysis_config_hash or _get(
            record, "analysis_config_hash", "config_hash", "variant_config_hash"
        )
        contract_hash = contract_hash or _get(record, "contract_hash")
        partition = partition or _get(record, "partition")
        sim_id = _get(record, "simulation_id", "id", "uid")
        if sim_id is None:
            sim_id = f"simulation:{_get(record, 'signal_id', default='none')}:{_get(record, 'horizon_seconds', 'horizon', default=0)}:{ordinal if ordinal is not None else payload_hash(record)[:16]}"
        detected = utc_iso(_get(record, "detected_ts", "detected_at", "timestamp", "ts"))
        expiry = utc_iso(_get(record, "expiry_ts", "expiry_at", "expires_at", "expiry", default=detected))
        values = (
            session_id,
            str(sim_id),
            _get(record, "signal_id"),
            str(_get(record, "simulation_type", "type", default="DIRECTIONAL")),
            float(_get(record, "horizon_seconds", "horizon", default=0)),
            str(_get(record, "direction", "side", default="UNKNOWN")).upper(),
            detected,
            utc_iso(_get(record, "entry_ts", "entry_at")) if _get(record, "entry_ts", "entry_at") is not None else None,
            expiry,
            float(_get(record, "entry_price")) if _get(record, "entry_price") is not None else None,
            float(_get(record, "final_price")) if _get(record, "final_price") is not None else None,
            str(
                getattr(
                    _get(record, "outcome", default="INDETERMINATE"),
                    "value",
                    _get(record, "outcome", default="INDETERMINATE"),
                )
            ).upper(),
            float(_get(record, "stake", default=0)),
            float(_get(record, "net_result")) if _get(record, "net_result") is not None else None,
            str(_get(record, "price_base", "base_price", default="unknown")),
            str(_get(record, "quality", default="UNKNOWN")),
            str(_get(record, "resolution", default="UNKNOWN")),
            canonical_json(_get(record, "assumptions", "assumptions_json", default={})),
            canonical_json(record),
        )
        payload = canonical_json(record)
        incoming_outcome = str(values[11]).upper()
        if incoming_outcome not in TERMINAL_SIMULATION_OUTCOMES and incoming_outcome != "PENDING":
            raise ValueError(f"outcome de simulación no soportado: {incoming_outcome!r}")
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT payload_json,outcome,signal_id,simulation_type,horizon_seconds,direction,detected_ts,entry_ts,expiry_ts,entry_price,final_price,stake,net_result,price_base,resolution FROM simulations WHERE session_id=? AND simulation_id=?",
                (session_id, str(sim_id)),
            ).fetchone()
            if existing is not None:
                if str(existing[0]) == payload:
                    return False
                existing_outcome = str(existing[1]).upper()
                if existing_outcome in TERMINAL_SIMULATION_OUTCOMES:
                    # A replay may revisit an already terminal identity before
                    # its later observations arrive. Never downgrade durable
                    # terminal state to an intermediate PENDING row.
                    if incoming_outcome == "PENDING":
                        if ignore_pending_terminal:
                            return False
                        raise IdempotencyConflict(
                            f"simulación terminal inmutable: identidad={(session_id, str(sim_id))!r} no admite volver a PENDING"
                        )
                    incoming_semantic = (
                        incoming_outcome,
                        _get(record, "signal_id"),
                        str(_get(record, "simulation_type", "type", default="DIRECTIONAL")),
                        float(_get(record, "horizon_seconds", "horizon", default=0)),
                        str(_get(record, "direction", "side", default="UNKNOWN")).upper(),
                        detected,
                        utc_iso(_get(record, "entry_ts", "entry_at"))
                        if _get(record, "entry_ts", "entry_at") is not None
                        else None,
                        expiry,
                        float(_get(record, "entry_price")) if _get(record, "entry_price") is not None else None,
                        float(_get(record, "final_price")) if _get(record, "final_price") is not None else None,
                        float(_get(record, "stake", default=0)),
                        float(_get(record, "net_result")) if _get(record, "net_result") is not None else None,
                        str(_get(record, "price_base", "base_price", default="unknown")),
                        str(_get(record, "resolution", default="UNKNOWN")),
                    )
                    existing_semantic = (existing_outcome, *tuple(existing[index] for index in range(2, 15)))
                    if existing_semantic == incoming_semantic:
                        return False
                    raise IdempotencyConflict(
                        f"simulación terminal inmutable: identidad={(session_id, str(sim_id))!r} ya está en {existing_outcome}"
                    )
                if not allow_update:
                    raise IdempotencyConflict(
                        f"conflicto de idempotencia en simulations: identidad={(session_id, str(sim_id))!r} ya tiene contenido distinto"
                    )
                (
                    _sid,
                    _sim_id,
                    signal_id,
                    simulation_type,
                    horizon,
                    direction,
                    detected_ts,
                    entry_ts,
                    expiry_ts,
                    entry_price,
                    final_price,
                    outcome,
                    stake,
                    net_result,
                    price_base,
                    quality,
                    resolution,
                    assumptions_json,
                    payload_json,
                ) = values
                conn.execute(
                    """UPDATE simulations SET signal_id=?,simulation_type=?,horizon_seconds=?,direction=?,detected_ts=?,entry_ts=?,expiry_ts=?,entry_price=?,final_price=?,outcome=?,stake=?,net_result=?,price_base=?,quality=?,resolution=?,assumptions_json=?,payload_json=?,analysis_id=?,variant=?,analysis_config_hash=?,contract_hash=?,partition=? WHERE session_id=? AND simulation_id=?""",
                    (
                        signal_id,
                        simulation_type,
                        horizon,
                        direction,
                        detected_ts,
                        entry_ts,
                        expiry_ts,
                        entry_price,
                        final_price,
                        outcome,
                        stake,
                        net_result,
                        price_base,
                        quality,
                        resolution,
                        assumptions_json,
                        payload_json,
                        analysis_id,
                        variant,
                        analysis_config_hash,
                        contract_hash,
                        partition,
                        session_id,
                        str(sim_id),
                    ),
                )
                return True
            cur = conn.execute(
                """INSERT INTO simulations(
                session_id,simulation_id,signal_id,simulation_type,horizon_seconds,direction,detected_ts,entry_ts,expiry_ts,
                entry_price,final_price,outcome,stake,net_result,price_base,quality,resolution,assumptions_json,payload_json,analysis_id,variant,analysis_config_hash,contract_hash,partition
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (*values, analysis_id, variant, analysis_config_hash, contract_hash, partition),
            )
        return cur.rowcount == 1

    def _decode_cfd_trade(
        self, row: sqlite3.Row | Mapping[str, Any], *, include_payload: bool = True
    ) -> dict[str, Any]:
        result = dict(row)
        result["terminal"] = bool(int(result.get("terminal", 0)))
        result["close_observed"] = bool(int(result.get("close_observed", 0)))
        result["lineage"] = _json_load(result.pop("lineage_json", None), {}) or {}
        if include_payload:
            result["payload"] = _json_load(result.pop("payload_json", None), {}) or {}
        else:
            result.pop("payload_json", None)
        result["lifecycle_state"] = result.get("state")
        economic_state = str(
            result.get("economic_state")
            or (
                "NOT_SETTLED"
                if result.get("state") in {"PENDING", "FILLED"}
                else ("DETERMINED" if result.get("net_pnl") is not None else "INDETERMINATE")
            )
        ).upper()
        result["economic_state"] = economic_state
        result["economic_result_state"] = economic_state
        result["economic_status"] = {
            "NOT_SETTLED": "NOT_SETTLED",
            "DETERMINED": "KNOWN",
            "INDETERMINATE": "UNKNOWN",
        }.get(economic_state, "UNKNOWN")
        result["economic_result"] = {
            "state": economic_state,
            "gross_pnl_quote": result.get("gross_pnl_quote"),
            "costs_quote": result.get("costs_quote"),
            "gross_pnl_account": result.get("gross_pnl_account"),
            "costs_account": result.get("costs_account"),
            "net_pnl": result.get("net_pnl"),
            "reason": result.get("economic_reason"),
        }
        return result

    def save_cfd_trade(
        self,
        session_id: str,
        analysis_id: str,
        trade: Any,
        *,
        variant: str | None = None,
        partition: str | None = None,
        analysis_config_hash: str | None = None,
        contract_hash: str | None = None,
        config_hash: str | None = None,
        contract: str | None = None,
    ) -> bool:
        """Persist one exact CFD PAPER lifecycle row."""

        if not self._table_exists("cfd_trades"):
            raise RuntimeError("la extensión de persistencia CFD no está disponible")
        namespace = str(analysis_id or "").strip()
        if not namespace:
            raise ValueError("analysis_id es obligatorio para un CFD")
        record, semantic = _normalise_cfd_trade(trade)
        _cfd_apply_scope(
            record,
            semantic,
            namespace,
            variant,
            partition,
            analysis_config_hash,
            contract_hash,
            config_hash,
            contract,
        )
        semantic_hash = payload_hash(semantic)
        payload_text = canonical_json(record)
        row_values = _cfd_row_values(session_id, namespace, record, semantic_hash, payload_text, utc_iso())
        return _persist_cfd_row(self, session_id, namespace, record, semantic_hash, payload_text, row_values)

    def list_cfd_trades(
        self,
        session_id: str,
        analysis_id: str | None = None,
        *,
        trade_id: str | None = None,
        signal_id: str | None = None,
        state: str | None = None,
        lifecycle_state: str | None = None,
        terminal: bool | None = None,
        instrument: str | None = None,
        product: str | None = None,
        variant: str | None = None,
        partition: str | None = None,
        start_ts: Any | None = None,
        end_ts: Any | None = None,
        after_cursor: object | None = None,
        limit: int | None = None,
        include_payload: bool = True,
    ) -> list[dict[str, Any]]:
        """Return a bounded deterministic CFD lifecycle projection."""

        if not self._table_exists("cfd_trades"):
            return []
        clauses, params = _cfd_list_where(
            session_id,
            analysis_id,
            trade_id,
            signal_id,
            state,
            lifecycle_state,
            terminal,
            instrument,
            product,
            variant,
            partition,
            start_ts,
            end_ts,
        )
        if after_cursor is not None:
            detected, row_id = _trade_cursor(after_cursor)
            clauses.append("(detected_at>? OR (detected_at=? AND cfd_trade_row_id>?))")
            params.extend([detected, detected, row_id])
        sql = f"SELECT * FROM cfd_trades WHERE {' AND '.join(clauses)} ORDER BY detected_at ASC,cfd_trade_row_id ASC"
        if limit is not None:
            bounded = max(1, int(limit))
            sql += " LIMIT ?"
            params.append(bounded)
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [self._decode_cfd_trade(row, include_payload=include_payload) for row in rows]

    def get_cfd_trade(self, session_id: str, analysis_id: str, trade_id: str) -> dict[str, Any] | None:
        rows = self.list_cfd_trades(session_id, analysis_id, trade_id=trade_id, limit=1)
        return rows[0] if rows else None

    def save_capture_envelope(
        self,
        session_id: str,
        envelope: Any,
        *,
        sequence: int | None = None,
    ) -> bool:
        """Append one versioned envelope, rejecting identity conflicts."""

        if not self._table_exists("capture_envelopes"):
            raise RuntimeError("la extensión de persistencia de capturas no está disponible")
        record = _capture_mapping(envelope, sequence=sequence)
        try:
            ingest_sequence = int(record["ingest_sequence"])
            generation = int(record["connection_generation"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("capture envelope requiere secuencia y generación enteras") from exc
        if ingest_sequence < 0 or generation < 0:
            raise ValueError("secuencia/generación de captura no pueden ser negativas")
        envelope_text = canonical_json(record)
        envelope_hash = payload_hash(record)
        values = (
            session_id,
            ingest_sequence,
            generation,
            record.get("event_time"),
            record.get("received_at"),
            record.get("available_at"),
            record.get("source_identity"),
            str(record["message_class"]),
            str(record["availability_policy"]),
            envelope_hash,
            envelope_text,
            utc_iso(),
        )
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT connection_generation,envelope_json FROM capture_envelopes WHERE session_id=? AND ingest_sequence=?",
                (session_id, ingest_sequence),
            ).fetchone()
            if existing is not None:
                if int(existing[0]) == generation and str(existing[1]) == envelope_text:
                    return False
                raise IdempotencyConflict(
                    f"conflicto de idempotencia en capture_envelopes: identidad={(session_id, generation, ingest_sequence)!r}"
                )
            conn.execute(
                """INSERT INTO capture_envelopes(
                    session_id,ingest_sequence,connection_generation,event_time,received_at,available_at,
                    source_identity,message_class,availability_policy,envelope_hash,envelope_json,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                values,
            )
        return True

    def iter_capture_envelopes(
        self,
        session_id: str,
        *,
        after_cursor: object | None = None,
        after_sequence: int | None = None,
        before_sequence: int | None = None,
        connection_generation: int | None = None,
        message_class: str | None = None,
        start_ts: Any | None = None,
        end_ts: Any | None = None,
        available_start: Any | None = None,
        available_end: Any | None = None,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream causal envelopes without materialising capture history."""

        if not self._table_exists("capture_envelopes"):
            return
        if after_cursor is not None and after_sequence is not None:
            raise ValueError("use after_cursor o after_sequence, no ambos")
        clauses, params = _capture_base_where(
            session_id,
            after_sequence,
            before_sequence,
            connection_generation,
            message_class,
            start_ts,
            end_ts,
            available_start,
            available_end,
        )
        if after_cursor is not None:
            cursor_sql, cursor_params = _capture_after_clause(after_cursor)
            clauses.append(cursor_sql)
            params.extend(cursor_params)
        sql = f"SELECT * FROM capture_envelopes WHERE {' AND '.join(clauses)} ORDER BY available_at IS NULL ASC,available_at ASC,ingest_sequence ASC,capture_row_id ASC"
        if limit is not None:
            bounded = max(1, int(limit))
            sql += " LIMIT ?"
            params.append(bounded)
        for row in self.conn.execute(sql, tuple(params)):
            yield _decode_capture_row(row)

    def list_capture_envelopes(self, session_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        """Materialised convenience wrapper; use the iterator for large captures."""

        return list(self.iter_capture_envelopes(session_id, **kwargs))

    def update_simulation(self, session_id: str, simulation: Any, **kwargs: Any) -> bool:
        """Actualiza el ciclo de vida PENDING -> resuelto de una simulación."""
        kwargs["allow_update"] = True
        return self.save_simulation(session_id, simulation, **kwargs)

    def save_checkpoint(
        self,
        session_id: str,
        checkpoint_name: str,
        *,
        cursor: Mapping[str, Any] | None = None,
        events_processed: int = 0,
        last_event_id: str | None = None,
        state: Mapping[str, Any] | None = None,
        analysis_id: str | None = None,
    ) -> None:
        """Atomically save a checkpoint in an analysis-specific namespace.

        Existing callers may omit ``analysis_id``: the value is recovered from
        ``cursor.analysis_id`` or ``state.analysis_id`` (including the nested
        processor state used by :class:`RuntimeCoordinator`).  A blank id is a
        deliberate capture/legacy namespace, not an accidental wildcard.
        """
        cursor_payload = dict(cursor or {})
        state_payload = dict(state or {})
        resolved_analysis = analysis_id or cursor_payload.get("analysis_id") or state_payload.get("analysis_id")
        if not resolved_analysis and isinstance(state_payload.get("processor"), Mapping):
            resolved_analysis = state_payload["processor"].get("analysis_id")
        resolved_analysis = str(resolved_analysis or "")
        if resolved_analysis:
            cursor_payload.setdefault("analysis_id", resolved_analysis)
        with self.transaction(immediate=True) as conn:
            conn.execute(
                """INSERT INTO checkpoints(session_id,checkpoint_name,analysis_id,updated_at,cursor_json,events_processed,last_event_id,state_json)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(session_id,checkpoint_name,analysis_id) DO UPDATE SET
                updated_at=excluded.updated_at,cursor_json=excluded.cursor_json,events_processed=excluded.events_processed,
                last_event_id=excluded.last_event_id,state_json=excluded.state_json""",
                (
                    session_id,
                    str(checkpoint_name),
                    resolved_analysis,
                    utc_iso(),
                    canonical_json(cursor_payload),
                    int(events_processed),
                    last_event_id,
                    canonical_json(state_payload),
                ),
            )

    def save_metric(
        self,
        session_id: str,
        metric_name: str,
        segment: Mapping[str, Any],
        value: float | None,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        segment_text = canonical_json(segment)
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO metrics(session_id,metric_name,segment_json,value,payload_json,created_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(session_id,metric_name,segment_json) DO UPDATE SET value=excluded.value,payload_json=excluded.payload_json,created_at=excluded.created_at""",
                (session_id, metric_name, segment_text, value, canonical_json(payload or {}), utc_iso()),
            )

    def _rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def _table_exists(self, table: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return row is not None

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM sessions WHERE session_id=?", (session_id,))
        if not rows:
            return None
        row = rows[0]
        row["config"] = _json_load(row.pop("config_json"), {})
        row["metadata"] = _json_load(row.pop("metadata_json"), {})
        return row

    @staticmethod
    def _decode_checkpoint(row: sqlite3.Row, *, requested_analysis_id: str | None = None) -> dict[str, Any]:
        result = dict(row)
        stored_analysis = str(result.get("analysis_id") or "")
        result["analysis_id"] = stored_analysis or None
        result["cursor"] = _json_load(result.pop("cursor_json", None), {}) or {}
        result["state"] = _json_load(result.pop("state_json", None), {}) or {}
        inferred = stored_analysis or result["cursor"].get("analysis_id") or result["state"].get("analysis_id")
        if not inferred and isinstance(result["state"].get("processor"), Mapping):
            inferred = result["state"]["processor"].get("analysis_id")
        result["analysis_id"] = str(inferred) if inferred else None
        if requested_analysis_id is not None:
            result["requested_analysis_id"] = str(requested_analysis_id)
            result["is_alternate"] = bool(result["analysis_id"] and result["analysis_id"] != str(requested_analysis_id))
        else:
            result["is_alternate"] = False
        return result

    def get_checkpoint(
        self,
        session_id: str,
        checkpoint_name: str = "default",
        *,
        analysis_id: str | None = None,
        allow_alternate: bool = True,
    ) -> dict[str, Any] | None:
        """Load a checkpoint, preferring an exact analysis namespace.

        If an exact namespace is absent and ``allow_alternate`` is true, the
        newest checkpoint for the same logical name is returned with
        ``is_alternate=True`` and ``requested_analysis_id`` metadata.  This
        makes a resume decision explicit instead of silently loading another
        analysis's state.
        """
        logical_name = str(checkpoint_name)
        requested = str(analysis_id) if analysis_id is not None else None
        if requested is not None:
            row = self.conn.execute(
                "SELECT * FROM checkpoints WHERE session_id=? AND checkpoint_name=? AND analysis_id=?",
                (session_id, logical_name, requested),
            ).fetchone()
            if row is not None:
                return self._decode_checkpoint(row, requested_analysis_id=requested)
            if not allow_alternate:
                return None
            row = self.conn.execute(
                "SELECT * FROM checkpoints WHERE session_id=? AND checkpoint_name=? AND analysis_id<>'' ORDER BY updated_at DESC, analysis_id DESC LIMIT 1",
                (session_id, logical_name),
            ).fetchone()
            return self._decode_checkpoint(row, requested_analysis_id=requested) if row is not None else None
        if allow_alternate:
            row = self.conn.execute(
                "SELECT * FROM checkpoints WHERE session_id=? AND checkpoint_name=? ORDER BY (analysis_id='') ASC, updated_at DESC, analysis_id DESC LIMIT 1",
                (session_id, logical_name),
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT * FROM checkpoints WHERE session_id=? AND checkpoint_name=? AND analysis_id='' ORDER BY updated_at DESC LIMIT 1",
                (session_id, logical_name),
            ).fetchone()
        return self._decode_checkpoint(row) if row is not None else None

    def load_alternate_checkpoint(
        self, session_id: str, checkpoint_name: str = "default", *, analysis_id: str
    ) -> dict[str, Any] | None:
        """Load the newest *other* analysis checkpoint explicitly."""
        target = str(analysis_id)
        row = self.conn.execute(
            "SELECT * FROM checkpoints WHERE session_id=? AND checkpoint_name=? AND analysis_id<>'' AND analysis_id<>? ORDER BY updated_at DESC, analysis_id DESC LIMIT 1",
            (session_id, str(checkpoint_name), target),
        ).fetchone()
        return self._decode_checkpoint(row, requested_analysis_id=target) if row is not None else None

    def list_checkpoints(
        self, session_id: str, checkpoint_name: str | None = None, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        params: list[Any] = [session_id]
        where = "session_id=?"
        if checkpoint_name is not None:
            where += " AND checkpoint_name=?"
            params.append(str(checkpoint_name))
        params.append(max(1, int(limit)))
        rows = self.conn.execute(
            f"SELECT * FROM checkpoints WHERE {where} ORDER BY updated_at DESC, checkpoint_name, analysis_id LIMIT ?",
            tuple(params),
        ).fetchall()
        return [self._decode_checkpoint(row) for row in rows]

    def list_candles(
        self,
        session_id: str,
        *,
        instrument: str | None = None,
        timeframe: str | None = None,
        closed_only: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["session_id=?"]
        params: list[Any] = [session_id]
        if instrument is not None:
            clauses.append("instrument=?")
            params.append(instrument)
        if timeframe is not None:
            clauses.append("timeframe=?")
            params.append(timeframe)
        if closed_only:
            clauses.append("closed=1")
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(
            f"SELECT * FROM candles WHERE {' AND '.join(clauses)} ORDER BY start_ts, timeframe, revision{lim}", params
        )
        for row in rows:
            row["provenance"] = _json_load(row.pop("provenance_json"), {})
        return rows

    def list_events(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(
            f"SELECT * FROM events WHERE session_id=? ORDER BY event_ts, event_row_id{lim}", (session_id,)
        )
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def list_decisions(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(
            f"SELECT * FROM decisions WHERE session_id=? ORDER BY observed_ts, decision_row_id{lim}", (session_id,)
        )
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def list_signals(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(
            f"SELECT * FROM signals WHERE session_id=? ORDER BY detected_ts, signal_row_id{lim}", (session_id,)
        )
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def list_discards(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(
            f"SELECT * FROM discards WHERE session_id=? ORDER BY observed_ts, discard_row_id{lim}", (session_id,)
        )
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def list_simulations(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(
            f"SELECT * FROM simulations WHERE session_id=? ORDER BY detected_ts, simulation_row_id{lim}", (session_id,)
        )
        for row in rows:
            row["assumptions"] = _json_load(row.pop("assumptions_json"), {})
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def status(self, session_id: str) -> dict[str, Any]:
        session = self.get_session(session_id) or {"session_id": session_id, "status": "UNKNOWN"}
        counts = {}
        for table in (
            "events",
            "candles",
            "decisions",
            "signals",
            "discards",
            "simulations",
            "analyses",
            "cfd_trades",
            "capture_envelopes",
        ):
            if not self._table_exists(table):
                counts[table] = 0
                continue
            row = self.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE session_id=?", (session_id,)).fetchone()
            counts[table] = int(row[0])
        if self._table_exists("signal_analysis_membership"):
            counts["signal_memberships"] = int(
                self.conn.execute(
                    "SELECT COUNT(*) FROM signal_analysis_membership WHERE session_id=?", (session_id,)
                ).fetchone()[0]
            )
        # ``counts.signals`` is the primary MTF detector count for terminal
        # compatibility; the complete capture count (including the independent
        # M1 reference variant) remains explicit as ``signals_total``.
        if self._table_exists("signals") and "variant" in {
            str(row[1]) for row in self.conn.execute("PRAGMA table_info(signals)")
        }:
            total_signals = counts.get("signals", 0)
            primary = self.conn.execute(
                "SELECT COUNT(*) FROM signals WHERE session_id=? AND (variant IS NULL OR variant <> 'm1_trigger_reference')",
                (session_id,),
            ).fetchone()[0]
            counts["signals_total"] = total_signals
            counts["signals"] = int(primary)
        latest = self.conn.execute("SELECT MAX(event_ts) FROM events WHERE session_id=?", (session_id,)).fetchone()[0]
        session.update({"counts": counts, "last_event_ts": latest, "schema_version": self.schema_version})
        return session

    def sessions(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return self._rows(
            "SELECT session_id,created_at,started_at,ended_at,status,mode,provider,instrument,code_version,seed,dataset_ref FROM sessions ORDER BY created_at DESC LIMIT ?",
            (int(limit),),
        )
