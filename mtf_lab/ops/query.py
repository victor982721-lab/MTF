"""Read-only, deterministic queries for the local MTF Lab UI.

The persistence layer intentionally remains a small append-only SQLite API.
This module adds the query concerns that should not leak into the strategy:
UTC half-open ranges, stable composite cursors, recent/paginated views,
indicator extraction, candle revisions, explicit gaps, and condition details.
No query fills gaps or computes a different trading result.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .persistence import SQLiteStore, canonical_json, utc_iso


@dataclasses.dataclass(frozen=True, slots=True)
class QueryPage:
    """Page with opaque cursors and a deterministic tie-break."""

    items: list[dict[str, Any]]
    limit: int
    order: str
    total: int | None
    next_cursor: str | None = None
    prev_cursor: str | None = None
    has_more: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": self.items,
            "limit": self.limit,
            "order": self.order,
            "total": self.total,
            "next_cursor": self.next_cursor,
            "prev_cursor": self.prev_cursor,
            "has_more": self.has_more,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class _SimulationQueryOptions:
    start_ts: Any | None
    end_ts: Any | None
    limit: int
    recent: bool
    cursor: Any | None
    revisions: str


_TABLES: dict[str, tuple[str, str, str]] = {
    "events": ("events", "event_ts", "event_row_id"),
    "candles": ("candles", "start_ts", "candle_row_id"),
    "decisions": ("decisions", "observed_ts", "decision_row_id"),
    "signals": ("signals", "detected_ts", "signal_row_id"),
    "discards": ("discards", "observed_ts", "discard_row_id"),
    "simulations": ("simulations", "detected_ts", "simulation_row_id"),
    "cfd_trades": ("cfd_trades", "detected_at", "cfd_trade_row_id"),
    "capture_envelopes": ("capture_envelopes", "available_at", "capture_row_id"),
}


def _parse_time(value: Any | None) -> str | None:
    if value is None or value == "":
        return None
    return utc_iso(value)


def _decode_json(value: Any, default: Any = None) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return value
    try:
        return json.loads(value) if value is not None else default
    except (TypeError, json.JSONDecodeError):
        return default


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value) if value is not None else ""


def _normalized_text(value: Any, *, default: str = "UNKNOWN") -> str:
    """Return a bounded, display-safe uppercase label.

    Session metadata is input data, not an instruction.  Keeping labels short
    and flat here also prevents the UI from accidentally rendering a nested
    provider payload or a traceback as an operational state.
    """

    text = str(value).strip().upper() if value is not None else ""
    return text or default


def _explicit_bool(*values: Any) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
    return None


def _safe_reference(value: Any) -> str | None:
    """Keep a dataset reference useful without exposing credential material."""

    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    lowered = text.lower()
    if ("://" in text and "@" in text) or any(
        marker in lowered for marker in ("token", "secret", "password", "authorization", "access_key")
    ):
        return "[REDACTED]"
    return text[:256] + ("…" if len(text) > 256 else "")


def _resolve_query_alias(primary: str | None, alias: str | None, label: str) -> str | None:
    if alias is not None:
        if primary is not None and str(primary) != str(alias):
            raise ValueError(f"{label} y su alias no pueden diferir")
        return alias
    return primary


def _resolve_query_state(state: object | None, alias: object | None) -> object | None:
    if alias is not None:
        state_text = str(getattr(state, "value", state)).upper() if state is not None else None
        alias_text = str(getattr(alias, "value", alias)).upper()
        if state_text is not None and state_text != alias_text:
            raise ValueError("state y lifecycle_state no pueden diferir")
        return alias
    return state


def _append_query_equals(
    clauses: list[str],
    params: list[Any],
    pairs: tuple[tuple[str, object | None], ...],
) -> None:
    for column, value in pairs:
        if value is not None:
            clauses.append(f"{column}=?")
            params.append(str(getattr(value, "value", value)))


def _append_query_cfd_options(
    clauses: list[str],
    params: list[Any],
    state: object | None,
    horizon_seconds: object | None,
    terminal: bool | None,
) -> None:
    if state is not None:
        clauses.append("state=?")
        params.append(str(getattr(state, "value", state)).upper())
    if horizon_seconds is not None:
        clauses.append("horizon_seconds=?")
        params.append(str(horizon_seconds))
    if terminal is not None:
        if not isinstance(terminal, bool):
            raise ValueError("terminal debe ser booleano")
        clauses.append("terminal=?")
        params.append(int(terminal))


def _append_query_cfd_ranges(
    clauses: list[str],
    params: list[Any],
    start_ts: Any | None,
    end_ts: Any | None,
) -> None:
    if start_ts is not None:
        clauses.append("detected_at>=?")
        params.append(_parse_time(start_ts))
    if end_ts is not None:
        clauses.append("detected_at<?")
        params.append(_parse_time(end_ts))


def _cfd_query_where(
    session_id: str,
    analysis_id: str | None,
    trade_id: str | None,
    signal_id: str | None,
    state: object | None,
    instrument: str | None,
    product: str | None,
    variant: str | None,
    partition: str | None,
    horizon_seconds: object | None,
    terminal: bool | None,
    start_ts: Any | None,
    end_ts: Any | None,
) -> tuple[list[str], list[Any], dict[str, Any]]:
    filters: dict[str, Any] = {
        "session_id": session_id,
        "analysis_id": analysis_id,
        "trade_id": trade_id,
        "signal_id": signal_id,
        "state": str(getattr(state, "value", state)).upper() if state is not None else None,
        "instrument": instrument,
        "product": product,
        "variant": variant,
        "partition": partition,
        "horizon_seconds": str(horizon_seconds) if horizon_seconds is not None else None,
        "terminal": terminal,
        "start_ts": _parse_time(start_ts),
        "end_ts": _parse_time(end_ts),
    }
    clauses = ["session_id=?"]
    params: list[Any] = [session_id]
    _append_query_equals(
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
    _append_query_cfd_options(clauses, params, state, horizon_seconds, terminal)
    _append_query_cfd_ranges(clauses, params, start_ts, end_ts)
    return clauses, params, filters


def _query_cursor_condition(decoded: Mapping[str, Any] | None, order: str) -> tuple[str, list[Any], bool]:
    if not decoded:
        return "", [], False
    op = str(decoded["op"])
    operator = _cursor_operator(order, op)
    sql = f" AND (detected_at {operator} ? OR (detected_at=? AND cfd_trade_row_id {operator} ?))"
    return sql, [decoded["ts"], decoded["ts"], int(decoded["row_id"])], op == "before"


def _cursor_operator(order: str, op: str) -> str:
    return ">" if ((order == "asc" and op == "after") or (order == "desc" and op == "before")) else "<"


def _query_cfd_cursor_pair(
    service: QueryService,
    items: list[dict[str, Any]],
    filters: Mapping[str, Any],
    order: str,
    decoded: Mapping[str, Any] | None,
    has_more: bool,
) -> tuple[str | None, str | None]:
    if not items:
        return None, None
    first, last = items[0], items[-1]
    next_cursor = (
        service._cursor(
            table="cfd_trades",
            filters=filters,
            order=order,
            op="after",
            ts=str(last["detected_at"]),
            row_id=int(last["cfd_trade_row_id"]),
        )
        if has_more or (decoded and decoded.get("op") == "before")
        else None
    )
    prev_cursor = (
        service._cursor(
            table="cfd_trades",
            filters=filters,
            order=order,
            op="before",
            ts=str(first["detected_at"]),
            row_id=int(first["cfd_trade_row_id"]),
        )
        if decoded
        else None
    )
    return next_cursor, prev_cursor


def _snapshot_context(status: dict[str, Any], session: Mapping[str, Any]) -> None:
    session_config = _as_mapping(session.get("config"))
    ctrader = _as_mapping(session_config.get("ctrader"))
    execution = _as_mapping(session_config.get("execution"))
    pipeline = _as_mapping(session_config.get("pipeline"))
    watch = _as_mapping(session_config.get("ctrader_watch"))
    metadata = _as_mapping(session.get("metadata"))
    provenance = _as_mapping(metadata.get("provenance"))
    watch_metadata = _as_mapping(metadata.get("ctrader_watch"))
    account_environment = ctrader.get("environment") or metadata.get("account_environment")
    execution_enabled = _explicit_bool(
        execution.get("enabled"),
        metadata.get("execution_enabled"),
        pipeline.get("execution_enabled"),
        watch.get("execution_enabled"),
        watch_metadata.get("execution_enabled"),
    )
    status.update(
        {
            "provider_environment": ctrader.get("environment") or session.get("mode"),
            "provider_account_id": ctrader.get("account_id") or None,
            "provider_symbol": ctrader.get("symbol") or session.get("instrument"),
            "execution_environment": execution.get("environment") or None,
            "execution_destination": _safe_reference(execution.get("endpoint")),
            # Account/execution facts are intentionally kept separate from the
            # source of the observed prices.  A REAL account record must not
            # turn a DEMO observation into a REAL market-data claim.
            "account_environment": account_environment,
            "observation_environment": metadata.get("environment")
            or metadata.get("observed_environment")
            or provenance.get("environment")
            or watch_metadata.get("environment")
            or watch.get("environment"),
            "execution_enabled": execution_enabled,
            "permissions": {
                "scopes": ctrader.get("required_scopes", []),
                "account_selected": ctrader.get("account_selected", False),
                "executor_enabled": execution_enabled is True,
            },
        }
    )
    if str(status.get("mode", "")).upper() in {"SYNTHETIC", "REPLAY"}:
        status.setdefault("connection", "OFFLINE")
        status.setdefault("analysis_enabled", False)


def _source_class(
    *,
    mode: Any,
    source_mode: Any,
    synthetic: bool | None,
    environment: Any,
    network_performed: bool | None,
) -> tuple[str, list[str]]:
    """Classify provenance only from explicit evidence.

    ``LIVE`` and a provider name are deliberately not enough to claim that a
    source is real.  Conversely, an explicit DEMO observation remains a data
    provenance fact even when an account record elsewhere says REAL.
    """

    mode_text = _normalized_text(mode)
    source_text = _normalized_text(source_mode)
    environment_text = _normalized_text(environment)
    evidence: list[str] = []
    fixture_values = {"FIXTURE", "SYNTHETIC", "SYNTHETIC_FIXTURE", "SYNTHETIC_DATA"}
    replay_values = {"REPLAY", "HISTORICAL", "HISTORICAL_REPLAY", "HISTORIC"}
    demo_values = {"DEMO", "DEMO_OBSERVED", "OBSERVED_DEMO", "DEMO_OBSERVATION"}
    if source_text in fixture_values:
        evidence.append(f"metadata.source_mode={source_text}")
        return "FIXTURE", evidence
    if synthetic is True:
        evidence.append("metadata.synthetic=true")
        return "FIXTURE", evidence
    if source_text in demo_values:
        evidence.append(f"metadata.source_mode={source_text}")
        if environment_text == "DEMO" and network_performed is True and synthetic is False:
            evidence.extend(["metadata.environment=DEMO", "metadata.network_performed=true"])
            return "DEMO_OBSERVED", evidence
        evidence.append("demo_observation_evidence_missing_or_contradictory")
        return "UNKNOWN", evidence
    if source_text in replay_values:
        evidence.append(f"metadata.source_mode={source_text}")
        return "HISTORICAL_REPLAY", evidence
    if mode_text == "SYNTHETIC":
        evidence.append("session.mode=SYNTHETIC")
        return "FIXTURE", evidence
    if mode_text == "REPLAY":
        evidence.append("session.mode=REPLAY")
        return "HISTORICAL_REPLAY", evidence
    if environment_text == "DEMO" and network_performed is True and synthetic is False:
        evidence.extend(["metadata.environment=DEMO", "metadata.network_performed=true"])
        return "DEMO_OBSERVED", evidence
    return "UNKNOWN", ["no_explicit_source_evidence"]


def _snapshot_provenance(
    status: Mapping[str, Any],
    session: Mapping[str, Any],
    coverage: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Project a small, allowlisted provenance read model for the UI."""

    metadata = _as_mapping(session.get("metadata"))
    nested = _as_mapping(metadata.get("provenance"))
    watch_metadata = _as_mapping(metadata.get("ctrader_watch"))
    config = _as_mapping(session.get("config"))
    pipeline = _as_mapping(config.get("pipeline"))
    watch_config = _as_mapping(config.get("ctrader_watch"))
    source_mode = (
        metadata.get("source_mode")
        or nested.get("source_mode")
        or pipeline.get("source_mode")
        or watch_metadata.get("source_mode")
        or watch_config.get("source_mode")
        or "UNKNOWN"
    )
    synthetic = _explicit_bool(
        metadata.get("synthetic"),
        metadata.get("synthetic_fixture"),
        nested.get("synthetic"),
        pipeline.get("synthetic"),
        watch_metadata.get("synthetic"),
        watch_config.get("synthetic"),
    )
    environment = (
        metadata.get("environment")
        or metadata.get("observed_environment")
        or nested.get("environment")
        or pipeline.get("environment")
        or watch_metadata.get("environment")
        or watch_config.get("environment")
    )
    network_performed = _explicit_bool(
        metadata.get("network_performed"),
        nested.get("network_performed"),
        pipeline.get("network_performed"),
        watch_metadata.get("network_performed"),
        watch_config.get("network_performed"),
    )
    execution_enabled = _explicit_bool(
        metadata.get("execution_enabled"),
        nested.get("execution_enabled"),
        pipeline.get("execution_enabled"),
        watch_metadata.get("execution_enabled"),
        watch_config.get("execution_enabled"),
        status.get("execution_enabled"),
    )
    source_class, evidence = _source_class(
        mode=session.get("mode"),
        source_mode=source_mode,
        synthetic=synthetic,
        environment=environment,
        network_performed=network_performed,
    )
    ranges = [
        (str(value.get("start_ts")), str(value.get("end_ts")))
        for value in coverage.values()
        if value.get("start_ts") and value.get("end_ts")
    ]
    metadata_start = metadata.get("coverage_start") or nested.get("coverage_start")
    metadata_end = metadata.get("coverage_end") or nested.get("coverage_end")
    start_values = [item[0] for item in ranges]
    end_values = [item[1] for item in ranges]
    if metadata_start:
        start_values.append(str(metadata_start))
    if metadata_end:
        end_values.append(str(metadata_end))
    dataset_ref = session.get("dataset_ref") or metadata.get("dataset_ref") or nested.get("dataset_ref")
    dataset_hash = (
        metadata.get("dataset_hash")
        or metadata.get("data_identity_hash")
        or nested.get("dataset_hash")
        or nested.get("data_identity_hash")
        or pipeline.get("dataset_hash")
        or pipeline.get("data_identity_hash")
        or watch_metadata.get("data_identity_hash")
        or watch_config.get("data_identity_hash")
        or config.get("dataset_hash")
    )
    capture_hash = metadata.get("capture_hash") or nested.get("capture_hash") or config.get("capture_hash")
    account_environment = status.get("account_environment")
    observation_environment = status.get("observation_environment") or environment
    return {
        "source_class": source_class,
        "source_class_label": {
            "FIXTURE": "FIXTURE",
            "HISTORICAL_REPLAY": "HISTORICAL / REPLAY",
            "DEMO_OBSERVED": "DEMO observado",
            "UNKNOWN": "UNKNOWN",
        }.get(source_class, "UNKNOWN"),
        "source_mode": _normalized_text(source_mode),
        "synthetic": synthetic if synthetic is not None else "UNKNOWN",
        "provider": session.get("provider") or nested.get("provider") or watch_metadata.get("provider") or "UNKNOWN",
        "environment": observation_environment or "UNKNOWN",
        "observed_environment": observation_environment or "UNKNOWN",
        "account_environment": account_environment or "UNKNOWN",
        "network_performed": network_performed if network_performed is not None else "UNKNOWN",
        "execution_enabled": execution_enabled if execution_enabled is not None else "UNKNOWN",
        "instrument": session.get("instrument") or nested.get("instrument") or "UNKNOWN",
        "dataset_ref": _safe_reference(dataset_ref),
        "dataset_hash": dataset_hash,
        "capture_hash": capture_hash,
        "range": {
            "start_ts": min(start_values) if start_values else None,
            "end_ts": max(end_values) if end_values else None,
        },
        "evidence": evidence,
    }


def _snapshot_last_data(store: SQLiteStore, session_id: str) -> dict[str, Any]:
    """Return the newest persisted market-data point without touching payloads."""

    event = store.conn.execute(
        "SELECT event_ts,received_ts,available_ts,instrument,source,kind FROM events "
        "WHERE session_id=? ORDER BY event_ts DESC,event_row_id DESC LIMIT 1",
        (session_id,),
    ).fetchone()
    candle = store.conn.execute(
        "SELECT end_ts,start_ts,available_ts,instrument,timeframe,source,quality FROM candles "
        "WHERE session_id=? ORDER BY end_ts DESC,candle_row_id DESC LIMIT 1",
        (session_id,),
    ).fetchone()
    event_ts = str(event[0]) if event is not None else None
    candle_ts = str(candle[0]) if candle is not None else None
    if event_ts is None and candle_ts is None:
        return {
            "timestamp": None,
            "event_ts": None,
            "available_at": None,
            "kind": None,
            "instrument": None,
            "timeframe": None,
            "source": None,
            "quality": None,
        }
    if candle_ts is not None and (event_ts is None or candle_ts >= event_ts):
        assert candle is not None
        return {
            "timestamp": candle_ts,
            "event_ts": None,
            "available_at": candle[2],
            "kind": "candle",
            "instrument": candle[3],
            "timeframe": candle[4],
            "source": candle[5],
            "quality": candle[6],
        }
    assert event is not None
    return {
        "timestamp": event_ts,
        "event_ts": event_ts,
        "available_at": event[2] or event[1],
        "kind": "event",
        "instrument": event[3],
        "timeframe": None,
        "source": event[4],
        "quality": None,
    }


def _snapshot_errors(status: Mapping[str, Any]) -> dict[str, Any]:
    processor = _as_mapping(status.get("runtime_processor"))
    raw_issues = status.get("runtime_issues", ())
    issues = [item for item in raw_issues if isinstance(item, Mapping)] if isinstance(raw_issues, list) else []
    codes = [_normalized_text(item.get("code"), default="UNKNOWN") for item in issues]
    latest = issues[-1] if issues else None
    return {
        "count": int(processor.get("errors", 0) or 0),
        "codes": codes[-20:],
        "last": {
            "code": _normalized_text(latest.get("code"), default="UNKNOWN"),
            "timestamp": latest.get("timestamp"),
        }
        if latest
        else None,
    }


def _snapshot_process(status: Mapping[str, Any], session: Mapping[str, Any]) -> dict[str, Any]:
    session_status = _normalized_text(session.get("status"), default="UNKNOWN")
    terminal = session_status in {"COMPLETED", "STOPPED", "FAILED", "ERROR"}
    capture_state = _normalized_text(status.get("capture_state"), default="UNKNOWN")
    mode = _normalized_text(session.get("mode"))
    active_capture_states = {"CAPTURING", "RUNNING", "ACTIVE", "OBSERVING"}
    return {
        "status": session_status,
        "running": session_status == "RUNNING",
        "terminal": terminal,
        "mode": mode,
        "capture_state": capture_state,
        "connection": _normalized_text(status.get("connection")),
        "reconciliation": _normalized_text(status.get("reconciliation_state")),
        "continuity": _normalized_text(status.get("continuity_state")),
        "analysis_enabled": bool(status.get("analysis_enabled", False)),
        # A running process is not proof of a live feed.  Only an explicitly
        # active capture in LIVE mode may be presented as feed-active; replay,
        # fixtures, paused slices and unknown states stay observational.
        "feed_active": bool(mode == "LIVE" and session_status == "RUNNING" and capture_state in active_capture_states),
        "pending_simulations": int(status.get("pending_simulations", 0) or 0),
        "errors": int(_as_mapping(status.get("runtime_processor")).get("errors", 0) or 0),
    }


def _feed_age_limit(session: Mapping[str, Any]) -> float | None:
    config = _as_mapping(session.get("config"))
    quality = _as_mapping(config.get("quality"))
    metadata = _as_mapping(session.get("metadata"))
    candidates = (
        metadata.get("max_feed_age_seconds"),
        quality.get("max_feed_age_seconds"),
    )
    for value in candidates:
        if value is None or isinstance(value, bool):
            continue
        try:
            number = float(str(value))
        except (TypeError, ValueError):
            continue
        if number >= 0:
            return number
    return None


def _snapshot_coverage(store: SQLiteStore, session_id: str) -> dict[str, dict[str, Any]]:
    coverage: dict[str, dict[str, Any]] = {}
    rows = store.conn.execute(
        "SELECT timeframe, MIN(start_ts), MAX(end_ts), SUM(closed=0), COUNT(*) FROM candles WHERE session_id=? GROUP BY timeframe",
        (session_id,),
    )
    for row in rows:
        coverage[str(row[0])] = {
            "start_ts": row[1],
            "end_ts": row[2],
            "open_count": int(row[3] or 0),
            "count": int(row[4] or 0),
        }
    return coverage


def _snapshot_cfd_lifecycle(store: SQLiteStore, session_id: str) -> dict[str, Any]:
    empty = {
        "cfd_lifecycle": {"pending_or_filled": 0, "closed": 0, "terminal_unknown_or_rejected": 0},
        "pending_cfd_trades": 0,
    }
    if not store._table_exists("cfd_trades"):
        return empty
    counts = store.conn.execute(
        "SELECT SUM(state IN ('PENDING','FILLED')), SUM(state='CLOSED'), SUM(state IN ('REJECTED','UNKNOWN')) FROM cfd_trades WHERE session_id=?",
        (session_id,),
    ).fetchone()
    pending = int(counts[0] or 0)
    return {
        "cfd_lifecycle": {
            "pending_or_filled": pending,
            "closed": int(counts[1] or 0),
            "terminal_unknown_or_rejected": int(counts[2] or 0),
        },
        "pending_cfd_trades": pending,
    }


def _checkpoint_names(session: Mapping[str, Any]) -> list[str]:
    config = _as_mapping(session.get("config"))
    metadata = _as_mapping(session.get("metadata"))
    names: list[str] = []
    for source in (
        config.get("ctrader_watch"),
        metadata.get("ctrader_watch"),
        config.get("watch"),
        metadata.get("watch"),
    ):
        checkpoint_name = _as_mapping(source).get("checkpoint_name")
        if checkpoint_name is not None and str(checkpoint_name).strip():
            name = str(checkpoint_name).strip()
            if name not in names:
                names.append(name)
    for name in ("ctrader-watch", "runtime", "pipeline"):
        if name not in names:
            names.append(name)
    return names


def _session_analysis_ids(store: SQLiteStore, session_id: str, session: Mapping[str, Any]) -> set[str]:
    """Return analysis identities evidenced by this session, never by recency alone."""

    ids: set[str] = set()
    config = _as_mapping(session.get("config"))
    metadata = _as_mapping(session.get("metadata"))
    for source in (session, config, metadata, config.get("ctrader_watch"), metadata.get("ctrader_watch")):
        mapping = _as_mapping(source)
        for key in ("analysis_id", "active_analysis_id"):
            value = mapping.get(key)
            if value is not None and str(value).strip():
                ids.add(str(value).strip())
    dataset_ref = session.get("dataset_ref")
    if dataset_ref is None:
        semantic_identity = metadata.get("semantic_identity")
        if semantic_identity:
            dataset_ref = f"ctrader-watch:{semantic_identity}"
    if dataset_ref is not None:
        dataset_text = str(dataset_ref)
        for analysis in store.analyses(session_id, limit=1000):
            if str(analysis.get("dataset_hash")) == dataset_text and analysis.get("analysis_id"):
                ids.add(str(analysis["analysis_id"]))
    return ids


def _checkpoint_analysis_key(checkpoint: Mapping[str, Any]) -> str:
    value = checkpoint.get("analysis_id")
    if value is None:
        cursor = _as_mapping(checkpoint.get("cursor"))
        state = _as_mapping(checkpoint.get("state"))
        value = cursor.get("analysis_id") or state.get("analysis_id")
    return str(value).strip() if value is not None else ""


def _checkpoint_warmup_pending(processor: Mapping[str, Any]) -> dict[str, int]:
    raw = processor.get("warmup_pending")
    if isinstance(raw, Mapping):
        return {str(key): int(value or 0) for key, value in raw.items()}
    strategy = _as_mapping(processor.get("strategy"))
    indicators = _as_mapping(strategy.get("indicators"))
    try:
        required = max(
            int(indicators.get("ema_slow", 0) or 0),
            int(indicators.get("rsi_period", 0) or 0) + 1,
            int(indicators.get("atr_period", 0) or 0),
        )
    except (TypeError, ValueError):
        return {}
    if required <= 0:
        return {}
    point_groups = _as_mapping(processor.get("indicator_points"))
    timeframes = processor.get("timeframes")
    names = [str(value) for value in timeframes] if isinstance(timeframes, list) else list(point_groups)
    result: dict[str, int] = {}
    for timeframe in names:
        points = point_groups.get(timeframe, [])
        if not isinstance(points, list):
            points = []
        last = _as_mapping(points[-1]) if points else {}
        ready = last.get("ready") is True or all(last.get(key) is not None for key in ("ema_slow", "rsi", "atr"))
        result[timeframe] = 0 if ready else max(1, required - len(points))
    return result


def _select_checkpoint(
    store: SQLiteStore, session_id: str, session: Mapping[str, Any], name: str
) -> Mapping[str, Any] | None:
    """Select only an evidenced checkpoint; never fall back to newest alternate."""

    checkpoints = store.list_checkpoints(session_id, name, limit=1000)
    if not checkpoints:
        return None
    evidenced_ids = _session_analysis_ids(store, session_id, session)
    if evidenced_ids:
        matches = [item for item in checkpoints if _checkpoint_analysis_key(item) in evidenced_ids]
        match_ids = {_checkpoint_analysis_key(item) for item in matches}
        if len(match_ids) == 1:
            return matches[0]
        # Multiple evidenced analyses are ambiguous for a single read model;
        # do not make a latest-row decision that could project another run.
        return None
    analysis_ids = {_checkpoint_analysis_key(item) for item in checkpoints if _checkpoint_analysis_key(item)}
    if len(analysis_ids) == 1:
        return next(item for item in checkpoints if _checkpoint_analysis_key(item) in analysis_ids)
    if analysis_ids:
        return None
    # A single blank legacy namespace is safe; multiple exact identities are
    # not.  ``list_checkpoints`` is already ordered by updated_at.
    blank = [item for item in checkpoints if not _checkpoint_analysis_key(item)]
    return blank[0] if len(blank) == 1 else None


def _apply_checkpoint_projection(
    store: SQLiteStore, status: dict[str, Any], session_id: str, session: Mapping[str, Any]
) -> None:
    for name in _checkpoint_names(session):
        checkpoint = _select_checkpoint(store, session_id, session, name)
        if not checkpoint:
            continue
        status["checkpoint_name"] = name
        status["checkpoint_analysis_id"] = _checkpoint_analysis_key(checkpoint) or None
        state = _as_mapping(checkpoint.get("state"))
        runtime_status = _as_mapping(state.get("status"))
        processor = _as_mapping(state.get("processor"))
        if runtime_status:
            status.update(
                {
                    key: runtime_status[key]
                    for key in (
                        "connection",
                        "analysis_enabled",
                        "analysis_blocked_reasons",
                        "pending_simulations",
                        "completed_simulations",
                        "capture_state",
                        "reconciliation_state",
                        "freshness_state",
                        "continuity_state",
                        "last_market_time",
                        "last_processed_at",
                        "last_heartbeat_at",
                    )
                    if key in runtime_status
                }
            )
        if processor:
            status["warmup_pending"] = _checkpoint_warmup_pending(processor)
            status["runtime_processor"] = {
                key: processor.get(key)
                for key in (
                    "events_processed",
                    "candles_processed",
                    "signals",
                    "evaluations",
                    "errors",
                    "last_event_time",
                    "last_available_at",
                )
            }
            raw_issues = processor.get("issues", [])
            status["runtime_issues"] = (
                [
                    {
                        "code": item.get("code"),
                        "timestamp": item.get("timestamp"),
                    }
                    for item in raw_issues[-20:]
                    if isinstance(item, Mapping)
                ]
                if isinstance(raw_issues, list)
                else []
            )
        break


class QueryService:
    """Bounded read model over an existing :class:`SQLiteStore`.

    Cursors include table, order and a hash of all filters.  Reusing a cursor
    with different filters is rejected instead of silently skipping or
    duplicating rows.  Every page is ordered by ``(timestamp, row_id)`` or its
    descending equivalent, so equal timestamps remain reproducible.
    """

    def __init__(self, store: SQLiteStore, *, max_limit: int = 1000):
        self.store = store
        self.max_limit = max(1, int(max_limit))

    def _filter_hash(self, table: str, filters: Mapping[str, Any]) -> str:
        payload = canonical_json({"table": table, **dict(filters)})
        return hashlib.sha256(payload.encode()).hexdigest()[:24]

    def _cursor(
        self,
        *,
        table: str,
        filters: Mapping[str, Any],
        order: str,
        op: str,
        ts: str,
        row_id: int,
        condition_ordinal: int | None = None,
    ) -> str:
        raw = {
            "v": 1,
            "table": table,
            "filter_hash": self._filter_hash(table, filters),
            "order": order,
            "op": op,
            "ts": ts,
            "row_id": int(row_id),
        }
        if condition_ordinal is not None:
            raw["condition_ordinal"] = int(condition_ordinal)
        encoded = (
            base64.urlsafe_b64encode(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode())
            .decode()
            .rstrip("=")
        )
        return encoded

    def _read_cursor(
        self, cursor: str | None, *, table: str, filters: Mapping[str, Any], order: str
    ) -> dict[str, Any] | None:
        if not cursor:
            return None
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            decoded = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        except Exception as exc:
            raise ValueError("cursor inválido") from exc
        if not isinstance(decoded, dict):
            raise ValueError("cursor inválido")
        data: dict[str, Any] = decoded
        if (
            data.get("v") != 1
            or data.get("table") != table
            or data.get("order") != order
            or data.get("filter_hash") != self._filter_hash(table, filters)
        ):
            raise ValueError("cursor no corresponde a la consulta actual")
        if data.get("op") not in {"after", "before"} or not isinstance(data.get("row_id"), int):
            raise ValueError("cursor inválido")
        # Validate/normalize instead of allowing arbitrary SQL literals through.
        data["ts"] = utc_iso(data["ts"])
        return data

    @staticmethod
    def _row_dict(row: Any) -> dict[str, Any]:
        return dict(row)

    def _decode_candle(self, result: dict[str, Any]) -> dict[str, Any]:
        result["provenance"] = _decode_json(result.pop("provenance_json", None), {})
        latest = self.store.conn.execute(
            "SELECT MAX(revision) FROM candles WHERE session_id=? AND instrument=? AND timeframe=? AND start_ts=?",
            (result["session_id"], result["instrument"], result["timeframe"], result["start_ts"]),
        ).fetchone()[0]
        result["is_latest_revision"] = int(result.get("revision", 0)) == int(latest or 0)
        provenance = result.get("provenance")
        indicators: Any = {}
        if isinstance(provenance, Mapping):
            for key in ("indicators", "indicator_values", "indicator", "values"):
                if isinstance(provenance.get(key), Mapping):
                    indicators = dict(provenance[key])
                    break
        result["indicator_values"] = indicators
        return result

    @staticmethod
    def _decode_payload(result: dict[str, Any]) -> dict[str, Any]:
        result["payload"] = _decode_json(result.pop("payload_json", None), {})
        return result

    @staticmethod
    def _decode_simulation(result: dict[str, Any]) -> dict[str, Any]:
        result["assumptions"] = _decode_json(result.pop("assumptions_json", None), {})
        result["payload"] = _decode_json(result.pop("payload_json", None), {})
        return result

    @staticmethod
    def _economic_state(result: Mapping[str, Any]) -> str:
        if result.get("economic_state"):
            return str(result["economic_state"]).upper()
        if result.get("state") in {"PENDING", "FILLED"}:
            return "NOT_SETTLED"
        return "DETERMINED" if result.get("net_pnl") is not None else "INDETERMINATE"

    @classmethod
    def _decode_cfd_trade(cls, result: dict[str, Any]) -> dict[str, Any]:
        result["lineage"] = _decode_json(result.pop("lineage_json", None), {}) or {}
        result["payload"] = _decode_json(result.pop("payload_json", None), {}) or {}
        result["terminal"] = bool(int(result.get("terminal", 0)))
        result["close_observed"] = bool(int(result.get("close_observed", 0)))
        result["lifecycle_state"] = result.get("state")
        # CFD lifecycle and economic knowledge are separate dimensions;
        # never project them to binary WIN/LOSS/TIE labels.
        economic_state = cls._economic_state(result)
        result["economic_state"] = economic_state
        result["economic_result_state"] = economic_state
        result["economic_status"] = {
            "NOT_SETTLED": "NOT_SETTLED",
            "DETERMINED": "KNOWN",
            "INDETERMINATE": "UNKNOWN",
        }.get(economic_state, "UNKNOWN")
        result["economic_result"] = {
            "state": economic_state,
            "net_pnl": result.get("net_pnl"),
            "gross_pnl_quote": result.get("gross_pnl_quote"),
            "costs_quote": result.get("costs_quote"),
            "gross_pnl_account": result.get("gross_pnl_account"),
            "costs_account": result.get("costs_account"),
            "reason": result.get("economic_reason"),
        }
        return result

    @staticmethod
    def _decode_capture_envelope(result: dict[str, Any]) -> dict[str, Any]:
        envelope = _decode_json(result.pop("envelope_json", None), {}) or {}
        result["payload"] = envelope.get("payload")
        for field in (
            "capture_schema",
            "event_time",
            "received_at",
            "available_at",
            "ingest_sequence",
            "connection_generation",
            "source_identity",
            "message_class",
            "availability_policy",
        ):
            if field in envelope:
                result[field] = envelope[field]
        return result

    def _decode_row(self, table: str, row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        if table == "candles":
            return self._decode_candle(result)
        if table in {"events", "decisions", "signals", "discards"}:
            return self._decode_payload(result)
        if table == "simulations":
            return self._decode_simulation(result)
        if table == "cfd_trades":
            return self._decode_cfd_trade(result)
        if table == "capture_envelopes":
            return self._decode_capture_envelope(result)
        return result

    @staticmethod
    def _append_time_filters(
        clauses: list[str], params: list[Any], time_col: str, start_ts: Any | None, end_ts: Any | None
    ) -> tuple[str | None, str | None]:
        start = _parse_time(start_ts)
        end = _parse_time(end_ts)
        if start is not None:
            clauses.append(f"{time_col}>=?")
            params.append(start)
        if end is not None:
            clauses.append(f"{time_col}<?")
            params.append(end)
        return start, end

    @staticmethod
    def _append_dimension_filters(
        clauses: list[str],
        params: list[Any],
        table: str,
        *,
        instrument: str | None,
        timeframe: str | None,
        closed: bool | None,
    ) -> None:
        if instrument is not None and table in {"candles", "signals", "events"}:
            clauses.append("instrument=?")
            params.append(str(instrument))
        if timeframe is not None and table == "candles":
            clauses.append("timeframe=?")
            params.append(str(timeframe).upper())
        if closed is not None and table == "candles":
            clauses.append("closed=?")
            params.append(int(bool(closed)))

    @staticmethod
    def _append_revision_filter(clauses: list[str], table: str, revisions: str) -> None:
        if table != "candles":
            return
        if revisions not in {"latest", "all"}:
            raise ValueError("revisions debe ser latest o all")
        if revisions == "latest":
            clauses.append(
                "revision=(SELECT MAX(c2.revision) FROM candles c2 WHERE c2.session_id=candles.session_id AND c2.instrument=candles.instrument AND c2.timeframe=candles.timeframe AND c2.start_ts=candles.start_ts)"
            )

    def _base_where(
        self,
        table: str,
        session_id: str,
        *,
        start_ts: Any | None = None,
        end_ts: Any | None = None,
        instrument: str | None = None,
        timeframe: str | None = None,
        closed: bool | None = None,
        revisions: str = "latest",
    ) -> tuple[str, list[Any], dict[str, Any]]:
        if table not in _TABLES:
            raise ValueError(f"tabla no consultable: {table}")
        if not session_id:
            raise ValueError("session_id es requerido")
        time_col = _TABLES[table][1]
        clauses = ["session_id=?"]
        params: list[Any] = [session_id]
        start, end = self._append_time_filters(clauses, params, time_col, start_ts, end_ts)
        self._append_dimension_filters(
            clauses,
            params,
            table,
            instrument=instrument,
            timeframe=timeframe,
            closed=closed,
        )
        self._append_revision_filter(clauses, table, revisions)
        filters = {
            "session_id": session_id,
            "start_ts": start,
            "end_ts": end,
            "instrument": instrument,
            "timeframe": timeframe,
            "closed": closed,
            "revisions": revisions,
        }
        return " AND ".join(clauses), params, filters

    def _page_table(
        self,
        table: str,
        session_id: str,
        *,
        start_ts: Any | None = None,
        end_ts: Any | None = None,
        instrument: str | None = None,
        timeframe: str | None = None,
        closed: bool | None = None,
        revisions: str = "latest",
        limit: int = 100,
        recent: bool = False,
        cursor: str | None = None,
        revision_only: bool = False,
    ) -> QueryPage:
        limit = min(self.max_limit, max(1, int(limit)))
        where, params, filters = self._base_where(
            table,
            session_id,
            start_ts=start_ts,
            end_ts=end_ts,
            instrument=instrument,
            timeframe=timeframe,
            closed=closed,
            revisions=revisions,
        )
        if revision_only:
            if table != "candles":
                raise ValueError("revision_only sólo aplica a candles")
            where += " AND revision>0"
            filters["revision_only"] = True
        time_col, row_col = _TABLES[table][1], _TABLES[table][2]
        order = "desc" if recent else "asc"
        decoded = self._read_cursor(cursor, table=table, filters=filters, order=order)
        query_params = list(params)
        reverse_page = False
        if decoded:
            op = decoded["op"]
            ts, row_id = decoded["ts"], int(decoded["row_id"])
            operator = _cursor_operator(order, op)
            where += f" AND ({time_col} {operator} ? OR ({time_col}=? AND {row_col} {operator} ?))"
            query_params.extend([ts, ts, row_id])
            if op == "before":
                reverse_page = True
        sql_order = "DESC" if ((order == "desc") != reverse_page) else "ASC"
        rows = self.store.conn.execute(
            f"SELECT * FROM {table} WHERE {where} ORDER BY {time_col} {sql_order}, {row_col} {sql_order} LIMIT ?",
            (*query_params, limit + 1),
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        if reverse_page:
            rows.reverse()
        items = [self._decode_row(table, self._row_dict(row)) for row in rows]
        base_where, _base_params, _ = self._base_where(
            table,
            session_id,
            start_ts=start_ts,
            end_ts=end_ts,
            instrument=instrument,
            timeframe=timeframe,
            closed=closed,
            revisions=revisions,
        )
        if revision_only:
            base_where += " AND revision>0"
        total = int(
            self.store.conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {base_where}", tuple(params[: len(_base_params)])
            ).fetchone()[0]
        )
        next_cursor = prev_cursor = None
        if items:
            first, last = items[0], items[-1]
            next_cursor = (
                self._cursor(
                    table=table,
                    filters=filters,
                    order=order,
                    op="after",
                    ts=str(last[time_col]),
                    row_id=int(last[row_col]),
                )
                if has_more or not decoded or decoded.get("op") == "before"
                else None
            )
            prev_cursor = (
                self._cursor(
                    table=table,
                    filters=filters,
                    order=order,
                    op="before",
                    ts=str(first[time_col]),
                    row_id=int(first[row_col]),
                )
                if decoded or (has_more and len(items) == limit)
                else None
            )
        return QueryPage(items, limit, order, total, next_cursor, prev_cursor, has_more)

    def query_events(self, session_id: str, **kwargs: Any) -> QueryPage:
        return self._page_table("events", session_id, **kwargs)

    def query_candles(self, session_id: str, **kwargs: Any) -> QueryPage:
        return self._page_table("candles", session_id, **kwargs)

    def query_signals(self, session_id: str, **kwargs: Any) -> QueryPage:
        return self._page_table("signals", session_id, **kwargs)

    def query_decisions(self, session_id: str, **kwargs: Any) -> QueryPage:
        return self._page_table("decisions", session_id, **kwargs)

    def query_discards(self, session_id: str, **kwargs: Any) -> QueryPage:
        return self._page_table("discards", session_id, **kwargs)

    def _simulation_dimensions(
        self, row: Mapping[str, Any], session: Mapping[str, Any], signal_instruments: Mapping[str, str]
    ) -> dict[str, str]:
        payload = _as_mapping(row.get("payload"))
        if (
            isinstance(payload, Mapping)
            and not any(
                key in payload
                for key in (
                    "analysis",
                    "analysis_name",
                    "variant",
                    "variant_name",
                    "instrument",
                    "partition",
                    "contract",
                )
            )
            and isinstance(payload.get("payload"), Mapping)
        ):
            payload = payload["payload"]
        assumptions = _as_mapping(row.get("assumptions"))
        session_config = _as_mapping(session.get("config"))
        metadata = _as_mapping(session.get("metadata"))
        virtual = _as_mapping(assumptions.get("virtual_contract"))
        variant = (
            payload.get("variant")
            or payload.get("variant_name")
            or row.get("variant")
            or str(row.get("simulation_id", "UNKNOWN")).split(":", 1)[0]
        )
        analysis = (
            payload.get("analysis")
            or payload.get("analysis_name")
            or payload.get("strategy")
            or session_config.get("strategy")
            or "UNKNOWN"
        )
        instrument = payload.get("instrument") or signal_instruments.get(
            str(row.get("signal_id")), session.get("instrument", "UNKNOWN")
        )
        partition = payload.get("partition") or assumptions.get("partition") or metadata.get("partition") or "UNKNOWN"
        contract = (
            payload.get("contract")
            or assumptions.get("contract")
            or (
                "VIRTUAL_CONTRACT"
                if str(row.get("simulation_type", "")).upper() == "VIRTUAL_CONTRACT"
                else row.get("simulation_type", "UNKNOWN")
            )
        )
        if isinstance(contract, Mapping):
            contract = contract.get("name") or contract.get("type") or "VIRTUAL_CONTRACT"
        return {
            "analysis": str(analysis),
            "variant": str(variant),
            "instrument": str(instrument),
            "horizon_seconds": str(row.get("horizon_seconds", "UNKNOWN")),
            "partition": str(partition),
            "contract": str(contract),
            "contract_stake": str(virtual.get("stake", row.get("stake", ""))),
            "contract_payout_net": str(virtual.get("payout_net", "")),
        }

    def _simulation_options(self, kwargs: dict[str, Any]) -> _SimulationQueryOptions:
        start_ts = kwargs.pop("start_ts", None)
        end_ts = kwargs.pop("end_ts", None)
        limit = min(self.max_limit, max(1, int(kwargs.pop("limit", 100))))
        recent = bool(kwargs.pop("recent", False))
        cursor = kwargs.pop("cursor", None)
        revisions = kwargs.pop("revisions", "latest")
        if kwargs:
            raise TypeError(f"unknown simulation query options: {sorted(kwargs)}")
        return _SimulationQueryOptions(start_ts, end_ts, limit, recent, cursor, revisions)

    def _simulation_where(
        self,
        session_id: str,
        *,
        analysis: str | None,
        variant: str | None,
        partition: str | None,
        contract: str | None,
        horizon_seconds: float | None,
        instrument: str | None,
        start_ts: Any | None,
        end_ts: Any | None,
        revisions: str,
    ) -> tuple[str, list[Any], dict[str, Any]]:
        where, params, filters = self._base_where(
            "simulations",
            session_id,
            start_ts=start_ts,
            end_ts=end_ts,
            instrument=None,
            timeframe=None,
            revisions=revisions,
        )
        filters.update(
            {
                "analysis": analysis,
                "variant": variant,
                "partition": partition,
                "contract": contract,
                "instrument_dimension": instrument,
                "horizon_seconds": None,
            }
        )
        if horizon_seconds is not None:
            normalized_horizon = float(horizon_seconds)
            where += " AND horizon_seconds=?"
            params.append(normalized_horizon)
            filters["horizon_seconds"] = normalized_horizon
        return where, params, filters

    def _match_simulation(
        self,
        raw: Any,
        *,
        session: Mapping[str, Any],
        signal_instruments: Mapping[str, str],
        wanted: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        item = self._decode_row("simulations", self._row_dict(raw))
        dimensions = self._simulation_dimensions(item, session, signal_instruments)
        item["dimensions"] = dimensions
        if any(value is not None and dimensions[key] != str(value) for key, value in wanted.items()):
            return None
        return item

    def _scan_simulations(
        self,
        rows: Any,
        *,
        session: Mapping[str, Any],
        signal_instruments: Mapping[str, str],
        wanted: Mapping[str, Any],
        max_items: int | None,
    ) -> tuple[list[dict[str, Any]], int]:
        matched: list[dict[str, Any]] = []
        total = 0
        for raw in rows:
            item = self._match_simulation(
                raw,
                session=session,
                signal_instruments=signal_instruments,
                wanted=wanted,
            )
            if item is None:
                continue
            total += 1
            if max_items is None or len(matched) < max_items:
                matched.append(item)
        return matched, total

    def query_simulations(
        self,
        session_id: str,
        *,
        analysis: str | None = None,
        variant: str | None = None,
        partition: str | None = None,
        contract: str | None = None,
        horizon_seconds: float | None = None,
        instrument: str | None = None,
        **kwargs: Any,
    ) -> QueryPage:
        """Query simulations with report dimensions extracted from payloads.

        Dimension extraction is intentionally read-only and uses bounded page
        scans.  The stored simulation fields remain the sole source of numeric
        outcomes; this method does not recalculate settlement.
        """
        # The SQLite schema stores the numeric horizon directly, so retain a
        # SQL filter before the payload dimension scan.
        table = "simulations"
        options = self._simulation_options(kwargs)
        where, params, filters = self._simulation_where(
            session_id,
            analysis=analysis,
            variant=variant,
            partition=partition,
            contract=contract,
            horizon_seconds=horizon_seconds,
            instrument=instrument,
            start_ts=options.start_ts,
            end_ts=options.end_ts,
            revisions=options.revisions,
        )
        base_where = where
        base_params = tuple(params)
        order = "desc" if options.recent else "asc"
        decoded = self._read_cursor(options.cursor, table=table, filters=filters, order=order)
        time_col, row_col = _TABLES[table][1], _TABLES[table][2]
        query_params = list(params)
        if decoded:
            operator = _cursor_operator(order, str(decoded["op"]))
            where += f" AND ({time_col}{operator}? OR ({time_col}=? AND {row_col}{operator}?))"
            query_params.extend([decoded["ts"], decoded["ts"], decoded["row_id"]])
        sql_order = "DESC" if order == "desc" else "ASC"
        rows = self.store.conn.execute(
            f"SELECT * FROM {table} WHERE {where} ORDER BY {time_col} {sql_order}, {row_col} {sql_order}",
            tuple(query_params),
        )
        session = self.store.get_session(session_id) or {}
        signal_rows = self.store.list_signals(session_id)
        signal_instruments = {str(x.get("signal_id")): str(x.get("instrument", "UNKNOWN")) for x in signal_rows}
        wanted = {
            "analysis": analysis,
            "variant": variant,
            "partition": partition,
            "contract": contract,
            "instrument": instrument,
        }
        matched, total_after_cursor = self._scan_simulations(
            rows,
            session=session,
            signal_instruments=signal_instruments,
            wanted=wanted,
            max_items=options.limit + 1,
        )
        has_more = len(matched) > options.limit
        matched = matched[: options.limit]
        # ``total`` is the count for the complete filtered query, not the
        # remaining suffix after a cursor.  This second streaming scan runs
        # only when a cursor is supplied.
        total = total_after_cursor
        if decoded:
            base_rows = self.store.conn.execute(
                f"SELECT * FROM {table} WHERE {base_where} ORDER BY {time_col} {sql_order}, {row_col} {sql_order}",
                base_params,
            )
            _unused, total = self._scan_simulations(
                base_rows,
                session=session,
                signal_instruments=signal_instruments,
                wanted=wanted,
                max_items=0,
            )
        next_cursor = prev_cursor = None
        if matched:
            next_cursor = (
                self._cursor(
                    table=table,
                    filters=filters,
                    order=order,
                    op="after",
                    ts=str(matched[-1][time_col]),
                    row_id=int(matched[-1][row_col]),
                )
                if has_more
                else None
            )
            prev_cursor = (
                self._cursor(
                    table=table,
                    filters=filters,
                    order=order,
                    op="before",
                    ts=str(matched[0][time_col]),
                    row_id=int(matched[0][row_col]),
                )
                if decoded
                else None
            )
        return QueryPage(matched, options.limit, order, total, next_cursor, prev_cursor, has_more)

    def query_cfd_trades(
        self,
        session_id: str,
        *,
        analysis_id: str | None = None,
        analysis: str | None = None,
        trade_id: str | None = None,
        signal_id: str | None = None,
        state: str | None = None,
        lifecycle_state: str | None = None,
        instrument: str | None = None,
        product: str | None = None,
        variant: str | None = None,
        partition: str | None = None,
        horizon_seconds: object | None = None,
        terminal: bool | None = None,
        start_ts: Any | None = None,
        end_ts: Any | None = None,
        recent: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> QueryPage:
        """Read CFD lifecycle rows without projecting binary outcomes."""

        bounded = min(self.max_limit, max(1, int(limit)))
        analysis_id = _resolve_query_alias(analysis_id, analysis, "analysis")
        resolved_state = _resolve_query_state(state, lifecycle_state)
        clauses, params, filters = _cfd_query_where(
            session_id,
            analysis_id,
            trade_id,
            signal_id,
            resolved_state,
            instrument,
            product,
            variant,
            partition,
            horizon_seconds,
            terminal,
            start_ts,
            end_ts,
        )
        if not self.store._table_exists("cfd_trades"):
            return QueryPage([], bounded, "desc" if recent else "asc", 0)
        order = "desc" if recent else "asc"
        decoded = self._read_cursor(cursor, table="cfd_trades", filters=filters, order=order)
        cursor_sql, cursor_params, reverse_page = _query_cursor_condition(decoded, order)
        table_order = "DESC" if ((order == "desc") != reverse_page) else "ASC"
        where = " AND ".join(clauses) + cursor_sql
        rows = self.store.conn.execute(
            f"SELECT * FROM cfd_trades WHERE {where} ORDER BY detected_at {table_order}, cfd_trade_row_id {table_order} LIMIT ?",
            (*params, *cursor_params, bounded + 1),
        ).fetchall()
        has_more = len(rows) > bounded
        items = [self._decode_row("cfd_trades", dict(row)) for row in rows[:bounded]]
        if reverse_page:
            items.reverse()
        total = int(
            self.store.conn.execute(
                f"SELECT COUNT(*) FROM cfd_trades WHERE {' AND '.join(clauses)}", tuple(params)
            ).fetchone()[0]
        )
        next_cursor, prev_cursor = _query_cfd_cursor_pair(self, items, filters, order, decoded, has_more)
        return QueryPage(items, bounded, order, total, next_cursor, prev_cursor, has_more)

    def query_capture_envelopes(
        self,
        session_id: str,
        *,
        after_cursor: object | None = None,
        after_sequence: int | None = None,
        start_ts: Any | None = None,
        end_ts: Any | None = None,
        available_start: Any | None = None,
        available_end: Any | None = None,
        connection_generation: int | None = None,
        message_class: str | None = None,
        limit: int = 100,
        cursor: object | None = None,
    ) -> QueryPage:
        """Bound one page of durable causal envelopes using the store iterator."""

        if cursor is not None:
            if after_cursor is not None:
                raise ValueError("use cursor o after_cursor, no ambos")
            after_cursor = cursor
        bounded = min(self.max_limit, max(1, int(limit)))
        rows = list(
            self.store.iter_capture_envelopes(
                session_id,
                after_cursor=after_cursor,
                after_sequence=after_sequence,
                start_ts=start_ts,
                end_ts=end_ts,
                available_start=available_start,
                available_end=available_end,
                connection_generation=connection_generation,
                message_class=message_class,
                limit=bounded + 1,
            )
        )
        has_more = len(rows) > bounded
        rows = rows[:bounded]
        next_cursor = rows[-1].get("cursor_token") if has_more and rows else None
        return QueryPage(rows, bounded, "causal", None, next_cursor, None, has_more)

    def query_revisions(self, session_id: str, **kwargs: Any) -> QueryPage:
        kwargs["revisions"] = "all"
        kwargs["revision_only"] = True
        return self._page_table("candles", session_id, **kwargs)

    def query_indicators(self, session_id: str, **kwargs: Any) -> QueryPage:
        return self.query_candles(session_id, **kwargs)

    def query_gaps(
        self,
        session_id: str,
        *,
        timeframe: str | None = None,
        instrument: str | None = None,
        start_ts: Any | None = None,
        end_ts: Any | None = None,
        revisions: str = "latest",
        include_open: bool = True,
    ) -> list[dict[str, Any]]:
        """Return observed missing intervals; never inserts a synthetic bar."""
        where, params, _ = self._base_where(
            "candles",
            session_id,
            timeframe=timeframe,
            instrument=instrument,
            start_ts=start_ts,
            end_ts=end_ts,
            revisions=revisions,
            closed=None if include_open else True,
        )
        raw_rows = self.store.conn.execute(
            f"SELECT * FROM candles WHERE {where} ORDER BY timeframe ASC, start_ts ASC, candle_row_id ASC",
            tuple(params),
        ).fetchall()
        candles = [self._decode_row("candles", self._row_dict(row)) for row in raw_rows]
        gaps: list[dict[str, Any]] = []
        for previous, current in zip(candles, candles[1:], strict=False):
            if previous.get("timeframe") != current.get("timeframe") or previous.get("instrument") != current.get(
                "instrument"
            ):
                continue
            prev_end = datetime.fromisoformat(str(previous["end_ts"]).replace("Z", "+00:00"))
            next_start = datetime.fromisoformat(str(current["start_ts"]).replace("Z", "+00:00"))
            if next_start > prev_end:
                gaps.append(
                    {
                        "session_id": session_id,
                        "instrument": current.get("instrument"),
                        "timeframe": current.get("timeframe"),
                        "gap_start": previous.get("end_ts"),
                        "gap_end": current.get("start_ts"),
                        "duration_seconds": (next_start - prev_end).total_seconds(),
                        "previous_candle_id": previous.get("candle_id"),
                        "next_candle_id": current.get("candle_id"),
                        "quality": "GAP_OBSERVED",
                        "filled": False,
                    }
                )
        return gaps

    def query_conditions(
        self,
        session_id: str,
        *,
        start_ts: Any | None = None,
        end_ts: Any | None = None,
        recent: bool = False,
        limit: int = 100,
        cursor: str | None = None,
    ) -> QueryPage:
        """Flatten persisted decision conditions with decision/ordinal tie-breaks."""
        # Decisions are already bounded by the query; the condition rows are a
        # transparent projection of their payload and retain the parent ID.
        where, params, _filters = self._base_where("decisions", session_id, start_ts=start_ts, end_ts=end_ts)
        raw_rows = self.store.conn.execute(
            f"SELECT * FROM decisions WHERE {where} ORDER BY observed_ts ASC, decision_row_id ASC", tuple(params)
        ).fetchall()
        items: list[dict[str, Any]] = []
        for raw in raw_rows:
            decision = self._decode_row("decisions", self._row_dict(raw))
            payload = decision.get("payload") or {}
            # SQLite stores the complete decision record in payload_json.  A
            # caller may itself have put the core decision payload under a
            # nested ``payload`` key; unwrap that transparently while keeping
            # the parent decision id/timestamp as the audit tie-break.
            if (
                isinstance(payload, Mapping)
                and not payload.get("conditions")
                and isinstance(payload.get("payload"), Mapping)
            ):
                payload = payload["payload"]
            conditions = payload.get("conditions", []) if isinstance(payload, Mapping) else []
            if not conditions and isinstance(payload, Mapping):
                conditions = payload.get("condition_results", []) or []
            for ordinal, condition in enumerate(conditions):
                if not isinstance(condition, Mapping):
                    continue
                items.append(
                    {
                        "decision_row_id": decision.get("decision_row_id"),
                        "decision_id": decision.get("decision_id"),
                        "observed_ts": decision.get("observed_ts"),
                        "decision": decision.get("status"),
                        "condition_ordinal": ordinal,
                        "name": condition.get("name"),
                        "state": condition.get("state", condition.get("status")),
                        "observed": condition.get("observed"),
                        "expected": condition.get("expected"),
                        "reason": condition.get("reason"),
                        "mandatory": condition.get("mandatory", True),
                        "mode": payload.get("mode") if isinstance(payload, Mapping) else None,
                    }
                )
        items.sort(
            key=lambda row: (row["observed_ts"], int(row["decision_row_id"]), int(row["condition_ordinal"])),
            reverse=bool(recent),
        )
        total_count = len(items)
        order = "desc" if recent else "asc"
        filt = {"session_id": session_id, "start_ts": _parse_time(start_ts), "end_ts": _parse_time(end_ts)}
        decoded = self._read_cursor(cursor, table="conditions", filters=filt, order=order) if cursor else None
        if decoded:
            key = (decoded["ts"], int(decoded["row_id"]), int(decoded.get("condition_ordinal", 0)))
            keep_after = []
            for row in items:
                row_key = (row["observed_ts"], int(row["decision_row_id"]), int(row["condition_ordinal"]))
                keep_after.append(row_key < key if recent else row_key > key)
            items = [row for row, keep in zip(items, keep_after, strict=True) if keep]
        limit = min(self.max_limit, max(1, int(limit)))
        has_more = len(items) > limit
        items = items[:limit]
        next_cursor = prev_cursor = None
        if items:
            next_cursor = (
                self._cursor(
                    table="conditions",
                    filters=filt,
                    order=order,
                    op="after",
                    ts=str(items[-1]["observed_ts"]),
                    row_id=int(items[-1]["decision_row_id"]),
                    condition_ordinal=int(items[-1]["condition_ordinal"]),
                )
                if has_more
                else None
            )
            prev_cursor = (
                self._cursor(
                    table="conditions",
                    filters=filt,
                    order=order,
                    op="before",
                    ts=str(items[0]["observed_ts"]),
                    row_id=int(items[0]["decision_row_id"]),
                    condition_ordinal=int(items[0]["condition_ordinal"]),
                )
                if decoded
                else None
            )
        return QueryPage(items, limit, order, total_count, next_cursor, prev_cursor, has_more)

    def poll(self, session_id: str, *, limit: int = 100) -> dict[str, Any]:
        """Return one bounded status poll; never starts a background loop."""
        bounded = min(self.max_limit, max(1, int(limit)))
        return {
            "status": self.snapshot(session_id),
            "events": self.query_events(session_id, recent=True, limit=bounded).to_dict(),
            "signals": self.query_signals(session_id, recent=True, limit=bounded).to_dict(),
            "discards": self.query_discards(session_id, recent=True, limit=bounded).to_dict(),
            "simulations": self.query_simulations(session_id, recent=True, limit=bounded).to_dict(),
            "cfd_trades": self.query_cfd_trades(session_id, recent=True, limit=bounded).to_dict(),
        }

    def snapshot(self, session_id: str) -> dict[str, Any]:
        status = self.store.status(session_id)
        session = self.store.get_session(session_id) or {}
        _snapshot_context(status, session)
        coverage = _snapshot_coverage(self.store, session_id)
        status["coverage"] = coverage
        status["revisions_count"] = int(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM candles WHERE session_id=? AND revision>0", (session_id,)
            ).fetchone()[0]
        )
        status.update(_snapshot_cfd_lifecycle(self.store, session_id))
        status["gaps"] = self.query_gaps(session_id)
        _apply_checkpoint_projection(self.store, status, session_id, session)
        last_data = _snapshot_last_data(self.store, session_id)
        status["last_data"] = last_data
        status["last_data_ts"] = last_data["timestamp"]
        status["process_status"] = _snapshot_process(status, session)
        status["process"] = status["process_status"]
        source = _snapshot_provenance(status, session, coverage)
        status["provenance"] = source
        # These allowlisted aliases make the contract convenient for a simple
        # client while the nested objects remain the canonical projection.
        status["source_mode"] = source["source_mode"]
        status["synthetic"] = source["synthetic"]
        status["network_performed"] = source["network_performed"]
        status["execution_enabled"] = source["execution_enabled"]
        historical = str(session.get("mode", "")).upper() in {"REPLAY", "BACKTEST", "SYNTHETIC"}
        raw_freshness = _normalized_text(status.get("freshness_state"))
        if historical:
            freshness_state = "NOT_APPLICABLE"
            freshness_reason = "historical_or_offline_data"
        elif last_data["timestamp"] is None:
            freshness_state = "UNKNOWN"
            freshness_reason = "no_data_observed"
        elif raw_freshness in {"FRESH", "VALID", "STALE", "UNKNOWN", "DISCONNECTED", "BLOCKED"}:
            freshness_state = raw_freshness
            freshness_reason = "runtime_projection"
        else:
            freshness_state = "UNKNOWN"
            freshness_reason = "freshness_not_observed"
        status["freshness"] = {
            "state": freshness_state,
            "reason": freshness_reason,
            "last_data_ts": last_data["timestamp"],
            "last_observed_at": last_data["available_at"] or last_data["timestamp"],
            "max_age_seconds": _feed_age_limit(session),
            "stale": freshness_state == "STALE",
            "feed_active": bool(
                not historical
                and status["process_status"]["feed_active"]
                and last_data["timestamp"] is not None
                and status["process_status"]["connection"] in {"CONNECTED", "HEALTHY"}
            ),
        }
        status["coverage_summary"] = {
            "timeframes": sorted(coverage),
            "start_ts": source["range"]["start_ts"],
            "end_ts": source["range"]["end_ts"],
            "gaps": len(status["gaps"]),
            "gap_state": "GAPS_OBSERVED" if status["gaps"] else "NO_GAPS_OBSERVED",
        }
        status["errors"] = _snapshot_errors(status)
        return status


__all__ = ["QueryPage", "QueryService"]
