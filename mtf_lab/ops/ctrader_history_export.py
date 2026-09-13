"""Export cTrader historical trendbars as an explicit native replay capture.

This module is deliberately independent from OAuth and CLI orchestration.  It
keeps the server response page order and receipt timestamps, while marking the
replay order as ``market_time_corrected``.  The capture contains native
trendbars only: it never manufactures bid/ask quotes or PAPER fills.
"""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..core.canonical import canonical_json
from ..data.capture import CAPTURE_VERSION, CaptureEnvelope, MessageClass, parse_instant
from ..data.ctrader import CTraderInstrumentSpec, normalize_trendbar

CAPTURE_KIND = "historical_trendbars"
CAPTURE_ORDER = "market_time_corrected"


class HistoryCaptureError(ValueError):
    """A historical export cannot claim a complete native capture."""

    def __init__(self, message: str, *, issues: Sequence[str] = (), state: str = "PARTIAL") -> None:
        super().__init__(message)
        self.state = str(state)
        self.issues = tuple(str(item) for item in issues)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value is not None else None


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return _iso(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    if hasattr(value, "value") and not isinstance(value, (str, bytes, int, float, bool)):
        return _jsonable(value.value)
    return value


def _metadata_fields(value: Any, allowed: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {
        str(key): item
        for key, item in value.items()
        if str(key) in allowed and (item is None or isinstance(item, (str, int, float, bool)))
    }


def _metadata_records(value: Any, allowed: frozenset[str]) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [_metadata_fields(item, allowed) for item in value if isinstance(item, Mapping)]


def _safe_discovery(value: Any) -> dict[str, Any]:
    """Keep only identity/permission observations, never arbitrary server fields."""

    if not isinstance(value, Mapping):
        return {}
    result = _metadata_fields(
        value,
        frozenset({"payloadType", "permissionScope", "permission_scope", "connection_generation", "generation"}),
    )
    account_fields = frozenset(
        {
            "ctidTraderAccountId",
            "ctid_trader_account_id",
            "account_id",
            "traderLogin",
            "trader_login",
            "environment",
            "isLive",
            "is_live",
        }
    )
    for key in ("ctidTraderAccount", "accounts", "records"):
        if key in value:
            result[key] = _metadata_records(value[key], account_fields)
    return result


def _safe_catalog(value: Any) -> dict[str, Any]:
    value = _jsonable(value)
    result = _metadata_fields(value, frozenset({"requested_symbol", "selected"}))
    if isinstance(value, Mapping):
        result["symbols"] = _metadata_records(
            value.get("symbols"), frozenset({"symbol_id", "name", "digits", "pip_position", "enabled"})
        )
    return result


def _safe_history_request(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    result = _metadata_fields(value, frozenset({"payload_type", "payloadType", "client_msg_id"}))
    result["payload"] = _metadata_fields(
        value.get("payload"),
        frozenset({"ctidTraderAccountId", "symbolId", "period", "fromTimestamp", "toTimestamp", "count"}),
    )
    return result


def _write_text_atomic(path: str | Path, text: str, *, overwrite: bool = False) -> Path:
    target = Path(path).expanduser()
    if not overwrite and (target.exists() or target.is_symlink()):
        raise FileExistsError(f"el artefacto ya existe: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=target.parent,
            prefix=f".{target.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(text)
            temporary.flush()
            os.fsync(temporary.fileno())
        assert temporary_path is not None
        os.chmod(temporary_path, 0o600)
        if overwrite:
            os.replace(temporary_path, target)
        else:
            # Both names are on the destination filesystem. Linking publishes
            # complete bytes atomically and fails if *any* destination appeared
            # after the preflight, including a dangling symlink.
            os.link(temporary_path, target, follow_symlinks=False)
    finally:
        if temporary_path is not None:
            with suppress(FileNotFoundError):
                temporary_path.unlink()
    return target


def _raw_trendbars(page: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw = page.get("trendbar", page.get("trendbars", ()))
    if isinstance(raw, Mapping):
        return (raw,)
    if not isinstance(raw, (list, tuple)):
        return ()
    return tuple(item for item in raw if isinstance(item, Mapping))


def _page_window(
    raw_page: Mapping[str, Any],
    *,
    spec: CTraderInstrumentSpec,
    received_at: datetime,
    page_index: int,
) -> tuple[datetime, datetime, int, tuple[str, ...]]:
    bars = _raw_trendbars(raw_page)
    if not bars:
        raise HistoryCaptureError(
            f"página histórica {page_index} vacía",
            issues=(f"page_{page_index}:sin trendbars",),
        )
    starts: list[datetime] = []
    ends: list[datetime] = []
    issues: list[str] = []
    valid = 0
    for bar_index, raw_bar in enumerate(bars):
        try:
            normalized = normalize_trendbar(
                raw_bar,
                spec=spec,
                received_at=received_at,
                available_at=received_at,
                request_id=f"history-page-{page_index}-bar-{bar_index}",
                mode="REPLAY",
            )
        except Exception as exc:
            issues.append(f"page_{page_index}:bar_{bar_index}:{type(exc).__name__}")
            continue
        valid += 1
        starts.append(normalized.interval_start)
        ends.append(normalized.interval_end)
    if not starts:
        raise HistoryCaptureError(
            f"página histórica {page_index} no contiene barras válidas",
            issues=issues or (f"page_{page_index}:sin barras válidas",),
        )
    return min(starts), max(ends), valid, tuple(issues)


def _history_status(
    history: Any, *, raw_bar_count: int, valid_bar_count: int, issues: Sequence[str]
) -> tuple[str, bool, tuple[str, ...]]:
    combined = list(str(item) for item in getattr(history, "issues", ()))
    combined.extend(str(item) for item in issues)
    if bool(getattr(history, "has_more", False)):
        combined.append("history_has_more=true")
    if not bool(getattr(history, "complete", False)):
        combined.append("history_complete=false")
    if raw_bar_count != valid_bar_count:
        combined.append(f"raw_bars={raw_bar_count} valid_bars={valid_bar_count}")
    unique = tuple(dict.fromkeys(combined))
    complete = bool(getattr(history, "complete", False)) and not unique and raw_bar_count > 0
    return ("COMPLETE" if complete else "PARTIAL"), complete, unique


def _build_page_envelopes(
    raw_pages: Sequence[Mapping[str, Any]],
    page_metadata: Sequence[Mapping[str, Any]],
    *,
    spec: CTraderInstrumentSpec,
    common_provenance: Mapping[str, Any],
    timeframe: str,
) -> tuple[list[CaptureEnvelope], list[tuple[datetime, datetime]], int, int, list[str]]:
    envelopes: list[CaptureEnvelope] = []
    page_windows: list[tuple[datetime, datetime]] = []
    raw_bar_count = 0
    valid_bar_count = 0
    page_issues: list[str] = []
    for page_index, (raw_page, metadata) in enumerate(zip(raw_pages, page_metadata, strict=True)):
        if not isinstance(raw_page, Mapping) or not isinstance(metadata, Mapping):
            raise HistoryCaptureError("página histórica sin mapping JSON válido", issues=("page_mapping_invalid",))
        if "bid" in raw_page or "ask" in raw_page:
            raise HistoryCaptureError(
                "respuesta histórica contiene bid/ask inesperado; se rechaza sin relabelar quotes",
                state="INVALID",
            )
        received_at = parse_instant(metadata.get("received_at"))
        response_available_at = parse_instant(metadata.get("available_at"))
        if received_at is None or response_available_at is None:
            raise HistoryCaptureError(
                "página histórica sin evidencia de recepción/disponibilidad",
                issues=(f"page_{page_index}:receipt_missing",),
            )
        page_bars = _raw_trendbars(raw_page)
        raw_bar_count += len(page_bars)
        start, end, valid_count, local_issues = _page_window(
            raw_page, spec=spec, received_at=received_at, page_index=page_index
        )
        valid_bar_count += valid_count
        page_issues.extend(local_issues)
        page_windows.append((start, end))
        payload = dict(raw_page)
        payload["capture_kind"] = CAPTURE_KIND
        payload["capture_source"] = "ctrader-open-api"
        payload["synthetic_fixture"] = False
        payload["capture_provenance"] = {
            **dict(common_provenance),
            "response_type": metadata.get("response_type", "PROTO_OA_GET_TRENDBARS_RES"),
            "timeframe": timeframe,
            "original_page_order": page_index,
            "source_ingest_sequence": metadata.get("ingest_sequence"),
            "request": _safe_history_request(metadata.get("request")),
            "response_received_at": _iso(received_at),
            "response_available_at": _iso(response_available_at),
            "replay_order": CAPTURE_ORDER,
            "availability_policy": "historical_event_time",
            "market_time_start": _iso(start),
            "market_time_end": _iso(end),
            "valid_bar_count": valid_count,
        }
        envelopes.append(
            CaptureEnvelope(
                event_time=start,
                received_at=received_at,
                available_at=end,
                ingest_sequence=page_index,
                connection_generation=int(metadata.get("connection_generation", 0) or 0),
                message_class=MessageClass.TRENDBAR,
                payload=payload,
                source_identity=str(metadata.get("source_identity") or f"ctrader-history:page-{page_index}"),
                availability_policy="historical_event_time",
            )
        )
    return envelopes, page_windows, raw_bar_count, valid_bar_count, page_issues


def export_history_capture(
    path: str | Path,
    *,
    history: Any,
    spec: CTraderInstrumentSpec,
    catalog: Any,
    environment: str,
    account_id: str | int,
    endpoint: str,
    permission_scope: str | int | None,
    discovery: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Export raw historical pages and an explicit corrected replay contract."""

    if str(environment).strip().upper() != "DEMO":
        raise HistoryCaptureError("la exportación histórica sólo admite environment=DEMO", state="REAL_FORBIDDEN")
    raw_pages = tuple(getattr(history, "raw_pages", ()))
    page_metadata = tuple(getattr(history, "page_metadata", ()))
    if not raw_pages or len(raw_pages) != len(page_metadata):
        raise HistoryCaptureError(
            "el histórico no conserva payloads y metadata de recepción para exportar captura",
            issues=("raw_pages_missing",),
        )
    if len(raw_pages) != int(getattr(history, "pages", len(raw_pages))):
        raise HistoryCaptureError(
            "el conteo de páginas no coincide con el payload histórico", issues=("page_count_mismatch",)
        )
    observed_catalog = _safe_catalog(catalog)
    observed_discovery = _safe_discovery(discovery or {})
    observed_spec = _jsonable(spec.to_dict())
    common_provenance = {
        "environment": "DEMO",
        "account_id": str(account_id),
        "endpoint": str(endpoint),
        "permission_scope": permission_scope,
        "discovery": observed_discovery,
        "instrument_spec": observed_spec,
        "catalog": observed_catalog,
    }
    envelopes, page_windows, raw_bar_count, valid_bar_count, page_issues = _build_page_envelopes(
        raw_pages,
        page_metadata,
        spec=spec,
        common_provenance=common_provenance,
        timeframe=str(getattr(history, "timeframe", "M1")),
    )
    status, complete, issues = _history_status(
        history,
        raw_bar_count=raw_bar_count,
        valid_bar_count=valid_bar_count,
        issues=page_issues,
    )
    if not envelopes or not valid_bar_count:
        raise HistoryCaptureError("captura histórica vacía o sin barras válidas", issues=issues, state="PARTIAL")
    market_start = min(item[0] for item in page_windows)
    market_end = max(item[1] for item in page_windows)
    received_values = [
        parsed
        for metadata in page_metadata
        if isinstance(metadata, Mapping)
        for parsed in (parse_instant(metadata.get("received_at")),)
        if parsed is not None
    ]
    if not received_values:
        raise HistoryCaptureError("captura histórica sin recepción observable", issues=("receipt_missing",))
    last_received = max(received_values)
    envelopes.append(
        CaptureEnvelope(
            event_time=market_end,
            received_at=last_received,
            available_at=market_end,
            ingest_sequence=len(envelopes),
            connection_generation=envelopes[-1].connection_generation,
            message_class=MessageClass.END,
            payload={
                "state": "END",
                "continuity": "UNKNOWN",
                "capture_kind": CAPTURE_KIND,
                "capture_order": CAPTURE_ORDER,
                "capture_status": status,
                "complete": complete,
                "has_more": bool(getattr(history, "has_more", False)),
                "issues": list(issues),
                "history_complete": bool(getattr(history, "complete", False)),
                "native_bars": valid_bar_count,
                "requested_start": _iso(market_start),
                "requested_end": _iso(market_end),
            },
            source_identity="ctrader-history:end",
            availability_policy="historical_event_time",
        )
    )
    serialized = [item.to_dict() for item in envelopes]
    document_meta = {
        "capture_schema": CAPTURE_VERSION,
        "capture_kind": CAPTURE_KIND,
        "capture_source": "ctrader-open-api",
        "capture_order": CAPTURE_ORDER,
        "capture_status": status,
        "complete": complete,
        "has_more": bool(getattr(history, "has_more", False)),
        "issues": list(issues),
        "environment": "DEMO",
        "account_id": str(account_id),
        "endpoint": str(endpoint),
        "permission_scope": permission_scope,
        "discovery": observed_discovery,
        "instrument_spec": observed_spec,
        "catalog": observed_catalog,
        "analysis_basis": "native",
        "synthetic_fixture": False,
        "native_bars": valid_bar_count,
        "raw_bars": raw_bar_count,
        "market_time_start": _iso(market_start),
        "market_time_end": _iso(market_end),
    }
    target = Path(path).expanduser()
    if target.suffix.lower() in {".jsonl", ".ndjson"}:
        text = "".join(canonical_json(item) + "\n" for item in serialized)
        output_format = "jsonl"
    else:
        text = canonical_json({**document_meta, "envelopes": serialized}) + "\n"
        output_format = "json"
    target = _write_text_atomic(target, text)
    return {
        **document_meta,
        "path": str(target),
        "format": output_format,
        "envelopes": len(envelopes),
        "message_class": "trendbar",
        "quote_events": 0,
        "paper_fills": 0,
    }


def _read_capture_markers(path: Path) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
    if path.suffix.lower() not in {".jsonl", ".ndjson"}:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return (raw, raw) if isinstance(raw, Mapping) else (None, None)
    first: Mapping[str, Any] | None = None
    last: Mapping[str, Any] | None = None
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                raw = json.loads(line)
                if isinstance(raw, Mapping):
                    first = first or raw
                    last = raw
    return first, last


def inspect_historical_capture(path: str | Path) -> dict[str, Any] | None:
    """Read capture metadata for local pipeline gating without opening SQLite."""

    first, last = _read_capture_markers(Path(path).expanduser())
    if first is None:
        return None
    if first.get("capture_kind") == CAPTURE_KIND:
        return dict(first)
    payload = first.get("payload")
    if not isinstance(payload, Mapping) or payload.get("capture_kind") != CAPTURE_KIND:
        return None
    provenance = payload.get("capture_provenance")
    result = dict(provenance) if isinstance(provenance, Mapping) else {}
    result["capture_kind"] = CAPTURE_KIND
    if isinstance(last, Mapping) and last.get("message_class") == MessageClass.END.value:
        end_payload = last.get("payload")
        if isinstance(end_payload, Mapping):
            for key in ("capture_status", "complete", "has_more", "issues", "native_bars", "capture_order"):
                if key in end_payload:
                    result[key] = end_payload[key]
    return result


__all__ = [
    "CAPTURE_KIND",
    "CAPTURE_ORDER",
    "HistoryCaptureError",
    "export_history_capture",
    "inspect_historical_capture",
]
