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


def _text(value: Any) -> str:
    return str(value) if value is not None else ""


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
    clauses: list[str], params: list[Any], pairs: tuple[tuple[str, object | None], ...],
) -> None:
    for column, value in pairs:
        if value is not None:
            clauses.append(f"{column}=?")
            params.append(str(getattr(value, "value", value)))


def _append_query_cfd_options(
    clauses: list[str], params: list[Any], state: object | None,
    horizon_seconds: object | None, terminal: bool | None,
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
    clauses: list[str], params: list[Any], start_ts: Any | None, end_ts: Any | None,
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
        "session_id": session_id, "analysis_id": analysis_id, "trade_id": trade_id,
        "signal_id": signal_id, "state": str(getattr(state, "value", state)).upper() if state is not None else None,
        "instrument": instrument, "product": product, "variant": variant,
        "partition": partition, "horizon_seconds": str(horizon_seconds) if horizon_seconds is not None else None,
        "terminal": terminal, "start_ts": _parse_time(start_ts), "end_ts": _parse_time(end_ts),
    }
    clauses = ["session_id=?"]
    params: list[Any] = [session_id]
    _append_query_equals(clauses, params, (("analysis_id", analysis_id), ("trade_id", trade_id), ("signal_id", signal_id), ("instrument", instrument), ("product", product), ("variant", variant), ("partition", partition)))
    _append_query_cfd_options(clauses, params, state, horizon_seconds, terminal)
    _append_query_cfd_ranges(clauses, params, start_ts, end_ts)
    return clauses, params, filters


def _query_cursor_condition(decoded: Mapping[str, Any] | None, order: str) -> tuple[str, list[Any], bool]:
    if not decoded:
        return "", [], False
    op = str(decoded["op"])
    operator = ">" if ((order == "asc" and op == "after") or (order == "desc" and op == "before")) else "<"
    sql = f" AND (detected_at {operator} ? OR (detected_at=? AND cfd_trade_row_id {operator} ?))"
    return sql, [decoded["ts"], decoded["ts"], int(decoded["row_id"])], op == "before"


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
    next_cursor = service._cursor(table="cfd_trades", filters=filters, order=order, op="after", ts=str(last["detected_at"]), row_id=int(last["cfd_trade_row_id"])) if has_more or (decoded and decoded.get("op") == "before") else None
    prev_cursor = service._cursor(table="cfd_trades", filters=filters, order=order, op="before", ts=str(first["detected_at"]), row_id=int(first["cfd_trade_row_id"])) if decoded else None
    return next_cursor, prev_cursor


def _snapshot_context(status: dict[str, Any], session: Mapping[str, Any]) -> None:
    session_config = session.get("config") if isinstance(session.get("config"), Mapping) else {}
    ctrader = session_config.get("ctrader") if isinstance(session_config.get("ctrader"), Mapping) else {}
    execution = session_config.get("execution") if isinstance(session_config.get("execution"), Mapping) else {}
    status.update({
        "provider_environment": ctrader.get("environment") or session.get("mode"),
        "provider_account_id": ctrader.get("account_id") or None,
        "provider_symbol": ctrader.get("symbol") or session.get("instrument"),
        "execution_environment": execution.get("environment") or None,
        "execution_destination": execution.get("endpoint") or None,
        "permissions": {"scopes": ctrader.get("required_scopes", []), "account_selected": ctrader.get("account_selected", False), "executor_enabled": execution.get("enabled", False)},
    })
    if str(status.get("mode", "")).upper() in {"SYNTHETIC", "REPLAY"}:
        status.setdefault("connection", "OFFLINE")
        status.setdefault("analysis_enabled", False)


def _snapshot_coverage(store: SQLiteStore, session_id: str) -> dict[str, dict[str, Any]]:
    coverage: dict[str, dict[str, Any]] = {}
    rows = store.conn.execute(
        "SELECT timeframe, MIN(start_ts), MAX(end_ts), SUM(closed=0), COUNT(*) FROM candles WHERE session_id=? GROUP BY timeframe",
        (session_id,),
    )
    for row in rows:
        coverage[str(row[0])] = {"start_ts": row[1], "end_ts": row[2], "open_count": int(row[3] or 0), "count": int(row[4] or 0)}
    return coverage


def _snapshot_cfd_lifecycle(store: SQLiteStore, session_id: str) -> dict[str, Any]:
    empty = {"cfd_lifecycle": {"pending_or_filled": 0, "closed": 0, "terminal_unknown_or_rejected": 0}, "pending_cfd_trades": 0}
    if not store._table_exists("cfd_trades"):
        return empty
    counts = store.conn.execute(
        "SELECT SUM(state IN ('PENDING','FILLED')), SUM(state='CLOSED'), SUM(state IN ('REJECTED','UNKNOWN')) FROM cfd_trades WHERE session_id=?",
        (session_id,),
    ).fetchone()
    pending = int(counts[0] or 0)
    return {"cfd_lifecycle": {"pending_or_filled": pending, "closed": int(counts[1] or 0), "terminal_unknown_or_rejected": int(counts[2] or 0)}, "pending_cfd_trades": pending}


def _apply_checkpoint_projection(store: SQLiteStore, status: dict[str, Any], session_id: str) -> None:
    for name in ("runtime", "pipeline"):
        checkpoint = store.get_checkpoint(session_id, name)
        if not checkpoint:
            continue
        state = checkpoint.get("state") if isinstance(checkpoint.get("state"), Mapping) else {}
        runtime_status = state.get("status") if isinstance(state.get("status"), Mapping) else {}
        processor = state.get("processor") if isinstance(state.get("processor"), Mapping) else {}
        if runtime_status:
            status.update({key: runtime_status[key] for key in ("connection", "analysis_enabled", "analysis_blocked_reasons", "pending_simulations", "completed_simulations") if key in runtime_status})
        if processor:
            status["warmup_pending"] = processor.get("warmup_pending", {})
            status["runtime_processor"] = {key: processor.get(key) for key in ("events_processed", "candles_processed", "signals", "evaluations", "errors", "last_event_time", "last_available_at")}
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

    def _cursor(self, *, table: str, filters: Mapping[str, Any], order: str, op: str, ts: str, row_id: int, condition_ordinal: int | None = None) -> str:
        raw = {"v": 1, "table": table, "filter_hash": self._filter_hash(table, filters), "order": order, "op": op, "ts": ts, "row_id": int(row_id)}
        if condition_ordinal is not None:
            raw["condition_ordinal"] = int(condition_ordinal)
        encoded = base64.urlsafe_b64encode(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).decode().rstrip("=")
        return encoded

    def _read_cursor(self, cursor: str | None, *, table: str, filters: Mapping[str, Any], order: str) -> dict[str, Any] | None:
        if not cursor:
            return None
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            data = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        except Exception as exc:
            raise ValueError("cursor inválido") from exc
        if data.get("v") != 1 or data.get("table") != table or data.get("order") != order or data.get("filter_hash") != self._filter_hash(table, filters):
            raise ValueError("cursor no corresponde a la consulta actual")
        if data.get("op") not in {"after", "before"} or not isinstance(data.get("row_id"), int):
            raise ValueError("cursor inválido")
        # Validate/normalize instead of allowing arbitrary SQL literals through.
        data["ts"] = utc_iso(data["ts"])
        return data

    @staticmethod
    def _row_dict(row: Any) -> dict[str, Any]:
        return dict(row)

    def _decode_row(self, table: str, row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        if table == "candles":
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
                        indicators = dict(provenance[key]); break
            result["indicator_values"] = indicators
        elif table in {"events", "decisions", "signals", "discards"}:
            result["payload"] = _decode_json(result.pop("payload_json", None), {})
        elif table == "simulations":
            result["assumptions"] = _decode_json(result.pop("assumptions_json", None), {})
            result["payload"] = _decode_json(result.pop("payload_json", None), {})
        elif table == "cfd_trades":
            result["lineage"] = _decode_json(result.pop("lineage_json", None), {}) or {}
            result["payload"] = _decode_json(result.pop("payload_json", None), {}) or {}
            result["terminal"] = bool(int(result.get("terminal", 0)))
            result["close_observed"] = bool(int(result.get("close_observed", 0)))
            result["lifecycle_state"] = result.get("state")
            # CFD lifecycle and economic knowledge are separate dimensions;
            # never project them to binary WIN/LOSS/TIE labels.
            economic_state = str(result.get("economic_state") or ("NOT_SETTLED" if result.get("state") in {"PENDING", "FILLED"} else ("DETERMINED" if result.get("net_pnl") is not None else "INDETERMINATE"))).upper()
            result["economic_state"] = economic_state
            result["economic_result_state"] = economic_state
            result["economic_status"] = {"NOT_SETTLED": "NOT_SETTLED", "DETERMINED": "KNOWN", "INDETERMINATE": "UNKNOWN"}.get(economic_state, "UNKNOWN")
            result["economic_result"] = {"state": economic_state, "net_pnl": result.get("net_pnl"), "gross_pnl_quote": result.get("gross_pnl_quote"), "costs_quote": result.get("costs_quote"), "gross_pnl_account": result.get("gross_pnl_account"), "costs_account": result.get("costs_account"), "reason": result.get("economic_reason")}
        elif table == "capture_envelopes":
            envelope = _decode_json(result.pop("envelope_json", None), {}) or {}
            result["payload"] = envelope.get("payload")
            for field in ("capture_schema", "event_time", "received_at", "available_at", "ingest_sequence", "connection_generation", "source_identity", "message_class", "availability_policy"):
                if field in envelope:
                    result[field] = envelope[field]
        return result

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
        start = _parse_time(start_ts); end = _parse_time(end_ts)
        if start is not None:
            clauses.append(f"{time_col}>=?"); params.append(start)
        if end is not None:
            clauses.append(f"{time_col}<?"); params.append(end)
        if instrument is not None and table in {"candles", "signals", "events"}:
            clauses.append("instrument=?"); params.append(str(instrument))
        if timeframe is not None and table == "candles":
            clauses.append("timeframe=?"); params.append(str(timeframe).upper())
        if closed is not None and table == "candles":
            clauses.append("closed=?"); params.append(int(bool(closed)))
        if table == "candles":
            if revisions not in {"latest", "all"}:
                raise ValueError("revisions debe ser latest o all")
            if revisions == "latest":
                clauses.append("revision=(SELECT MAX(c2.revision) FROM candles c2 WHERE c2.session_id=candles.session_id AND c2.instrument=candles.instrument AND c2.timeframe=candles.timeframe AND c2.start_ts=candles.start_ts)")
        filters = {"session_id": session_id, "start_ts": start, "end_ts": end, "instrument": instrument, "timeframe": timeframe, "closed": closed, "revisions": revisions}
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
        where, params, filters = self._base_where(table, session_id, start_ts=start_ts, end_ts=end_ts, instrument=instrument, timeframe=timeframe, closed=closed, revisions=revisions)
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
            if (order == "asc" and op == "after") or (order == "desc" and op == "before"):
                operator = ">"
            else:
                operator = "<"
            where += f" AND ({time_col} {operator} ? OR ({time_col}=? AND {row_col} {operator} ?))"
            query_params.extend([ts, ts, row_id])
            if op == "before":
                reverse_page = True
        sql_order = "DESC" if ((order == "desc") != reverse_page) else "ASC"
        rows = self.store.conn.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY {time_col} {sql_order}, {row_col} {sql_order} LIMIT ?", (*query_params, limit + 1)).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        if reverse_page:
            rows.reverse()
        items = [self._decode_row(table, self._row_dict(row)) for row in rows]
        base_where, _base_params, _ = self._base_where(table, session_id, start_ts=start_ts, end_ts=end_ts, instrument=instrument, timeframe=timeframe, closed=closed, revisions=revisions)
        if revision_only:
            base_where += " AND revision>0"
        total = int(self.store.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {base_where}", tuple(params[:len(_base_params)])).fetchone()[0])
        next_cursor = prev_cursor = None
        if items:
            first, last = items[0], items[-1]
            next_cursor = self._cursor(table=table, filters=filters, order=order, op="after", ts=str(last[time_col]), row_id=int(last[row_col])) if has_more or not decoded or decoded.get("op") == "before" else None
            prev_cursor = self._cursor(table=table, filters=filters, order=order, op="before", ts=str(first[time_col]), row_id=int(first[row_col])) if decoded or (has_more and len(items) == limit) else None
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

    def _simulation_dimensions(self, row: Mapping[str, Any], session: Mapping[str, Any], signal_instruments: Mapping[str, str]) -> dict[str, str]:
        payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
        if isinstance(payload, Mapping) and not any(key in payload for key in ("analysis", "analysis_name", "variant", "variant_name", "instrument", "partition", "contract")) and isinstance(payload.get("payload"), Mapping):
            payload = payload["payload"]
        assumptions = row.get("assumptions") if isinstance(row.get("assumptions"), Mapping) else {}
        session_config = session.get("config") if isinstance(session.get("config"), Mapping) else {}
        metadata = session.get("metadata") if isinstance(session.get("metadata"), Mapping) else {}
        virtual = assumptions.get("virtual_contract") if isinstance(assumptions.get("virtual_contract"), Mapping) else {}
        variant = payload.get("variant") or payload.get("variant_name") or row.get("variant") or str(row.get("simulation_id", "UNKNOWN")).split(":", 1)[0]
        analysis = payload.get("analysis") or payload.get("analysis_name") or payload.get("strategy") or session_config.get("strategy") or "UNKNOWN"
        instrument = payload.get("instrument") or signal_instruments.get(str(row.get("signal_id")), session.get("instrument", "UNKNOWN"))
        partition = payload.get("partition") or assumptions.get("partition") or metadata.get("partition") or "UNKNOWN"
        contract = payload.get("contract") or assumptions.get("contract") or ("VIRTUAL_CONTRACT" if str(row.get("simulation_type", "")).upper() == "VIRTUAL_CONTRACT" else row.get("simulation_type", "UNKNOWN"))
        if isinstance(contract, Mapping):
            contract = contract.get("name") or contract.get("type") or "VIRTUAL_CONTRACT"
        return {"analysis": str(analysis), "variant": str(variant), "instrument": str(instrument), "horizon_seconds": str(row.get("horizon_seconds", "UNKNOWN")), "partition": str(partition), "contract": str(contract), "contract_stake": str(virtual.get("stake", row.get("stake", ""))), "contract_payout_net": str(virtual.get("payout_net", ""))}

    def query_simulations(self, session_id: str, *, analysis: str | None = None, variant: str | None = None, partition: str | None = None, contract: str | None = None, horizon_seconds: float | None = None, instrument: str | None = None, **kwargs: Any) -> QueryPage:
        """Query simulations with report dimensions extracted from payloads.

        Dimension extraction is intentionally read-only and uses bounded page
        scans.  The stored simulation fields remain the sole source of numeric
        outcomes; this method does not recalculate settlement.
        """
        # The SQLite schema stores the numeric horizon directly, so retain a
        # SQL filter before the payload dimension scan.
        table = "simulations"
        start_ts = kwargs.pop("start_ts", None); end_ts = kwargs.pop("end_ts", None)
        limit = min(self.max_limit, max(1, int(kwargs.pop("limit", 100))))
        recent = bool(kwargs.pop("recent", False)); cursor = kwargs.pop("cursor", None)
        revisions = kwargs.pop("revisions", "latest")
        if kwargs:
            raise TypeError(f"unknown simulation query options: {sorted(kwargs)}")
        where, params, filters = self._base_where(table, session_id, start_ts=start_ts, end_ts=end_ts, instrument=None, timeframe=None, revisions=revisions)
        filters.update({"analysis": analysis, "variant": variant, "partition": partition, "contract": contract, "instrument_dimension": instrument})
        if horizon_seconds is not None:
            where += " AND horizon_seconds=?"; params.append(float(horizon_seconds)); filters["horizon_seconds"] = float(horizon_seconds)
        else:
            filters["horizon_seconds"] = None
        base_where = where
        base_params = tuple(params)
        order = "desc" if recent else "asc"
        decoded = self._read_cursor(cursor, table=table, filters=filters, order=order)
        time_col, row_col = _TABLES[table][1], _TABLES[table][2]
        query_params = list(params)
        if decoded:
            op = decoded["op"]; operator = ">" if ((order == "asc" and op == "after") or (order == "desc" and op == "before")) else "<"
            where += f" AND ({time_col}{operator}? OR ({time_col}=? AND {row_col}{operator}?))"; query_params.extend([decoded["ts"], decoded["ts"], decoded["row_id"]])
        sql_order = "DESC" if order == "desc" else "ASC"
        # Scan in chunks to avoid loading an unbounded history into memory.
        rows = self.store.conn.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY {time_col} {sql_order}, {row_col} {sql_order}", tuple(query_params))
        session = self.store.get_session(session_id) or {}
        signal_rows = self.store.list_signals(session_id)
        signal_instruments = {str(x.get("signal_id")): str(x.get("instrument", "UNKNOWN")) for x in signal_rows}
        matched: list[dict[str, Any]] = []; total_after_cursor = 0
        wanted = {"analysis": analysis, "variant": variant, "partition": partition, "contract": contract, "instrument": instrument}
        def matches(raw: Any) -> dict[str, Any] | None:
            item = self._decode_row(table, self._row_dict(raw)); dimensions = self._simulation_dimensions(item, session, signal_instruments); item["dimensions"] = dimensions
            if any(value is not None and dimensions[key] != str(value) for key, value in wanted.items()): return None
            return item
        for raw in rows:
            item = matches(raw)
            if item is None: continue
            total_after_cursor += 1
            if len(matched) < limit + 1: matched.append(item)
        has_more = len(matched) > limit; matched = matched[:limit]
        # ``total`` is the count for the complete filtered query, not the
        # remaining suffix after a cursor.  This is a second bounded streaming
        # scan only when a cursor is supplied; it keeps pagination metadata
        # honest without loading history into a list.
        total = total_after_cursor
        if decoded:
            base_rows = self.store.conn.execute(f"SELECT * FROM {table} WHERE {base_where} ORDER BY {time_col} {sql_order}, {row_col} {sql_order}", base_params)
            total = 0
            for raw in base_rows:
                if matches(raw) is not None: total += 1
        next_cursor = prev_cursor = None
        if matched:
            next_cursor = self._cursor(table=table, filters=filters, order=order, op="after", ts=str(matched[-1][time_col]), row_id=int(matched[-1][row_col])) if has_more else None
            prev_cursor = self._cursor(table=table, filters=filters, order=order, op="before", ts=str(matched[0][time_col]), row_id=int(matched[0][row_col])) if decoded else None
        return QueryPage(matched, limit, order, total, next_cursor, prev_cursor, has_more)

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
        state = _resolve_query_state(state, lifecycle_state)
        clauses, params, filters = _cfd_query_where(
            session_id, analysis_id, trade_id, signal_id, state, instrument, product,
            variant, partition, horizon_seconds, terminal, start_ts, end_ts,
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
        total = int(self.store.conn.execute(f"SELECT COUNT(*) FROM cfd_trades WHERE {' AND '.join(clauses)}", tuple(params)).fetchone()[0])
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
        rows = list(self.store.iter_capture_envelopes(
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
        ))
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

    def query_gaps(self, session_id: str, *, timeframe: str | None = None, instrument: str | None = None, start_ts: Any | None = None, end_ts: Any | None = None, revisions: str = "latest", include_open: bool = True) -> list[dict[str, Any]]:
        """Return observed missing intervals; never inserts a synthetic bar."""
        where, params, _ = self._base_where("candles", session_id, timeframe=timeframe, instrument=instrument, start_ts=start_ts, end_ts=end_ts, revisions=revisions, closed=None if include_open else True)
        raw_rows = self.store.conn.execute(f"SELECT * FROM candles WHERE {where} ORDER BY timeframe ASC, start_ts ASC, candle_row_id ASC", tuple(params)).fetchall()
        candles = [self._decode_row("candles", self._row_dict(row)) for row in raw_rows]
        gaps: list[dict[str, Any]] = []
        for previous, current in zip(candles, candles[1:]):
            if previous.get("timeframe") != current.get("timeframe") or previous.get("instrument") != current.get("instrument"): continue
            prev_end = datetime.fromisoformat(str(previous["end_ts"]).replace("Z", "+00:00")); next_start = datetime.fromisoformat(str(current["start_ts"]).replace("Z", "+00:00"))
            if next_start > prev_end:
                gaps.append({"session_id": session_id, "instrument": current.get("instrument"), "timeframe": current.get("timeframe"), "gap_start": previous.get("end_ts"), "gap_end": current.get("start_ts"), "duration_seconds": (next_start - prev_end).total_seconds(), "previous_candle_id": previous.get("candle_id"), "next_candle_id": current.get("candle_id"), "quality": "GAP_OBSERVED", "filled": False})
        return gaps

    def query_conditions(self, session_id: str, *, start_ts: Any | None = None, end_ts: Any | None = None, recent: bool = False, limit: int = 100, cursor: str | None = None) -> QueryPage:
        """Flatten persisted decision conditions with decision/ordinal tie-breaks."""
        # Decisions are already bounded by the query; the condition rows are a
        # transparent projection of their payload and retain the parent ID.
        where, params, _filters = self._base_where("decisions", session_id, start_ts=start_ts, end_ts=end_ts)
        raw_rows = self.store.conn.execute(f"SELECT * FROM decisions WHERE {where} ORDER BY observed_ts ASC, decision_row_id ASC", tuple(params)).fetchall()
        items: list[dict[str, Any]] = []
        for raw in raw_rows:
            decision = self._decode_row("decisions", self._row_dict(raw)); payload = decision.get("payload") or {}
            # SQLite stores the complete decision record in payload_json.  A
            # caller may itself have put the core decision payload under a
            # nested ``payload`` key; unwrap that transparently while keeping
            # the parent decision id/timestamp as the audit tie-break.
            if isinstance(payload, Mapping) and not payload.get("conditions") and isinstance(payload.get("payload"), Mapping):
                payload = payload["payload"]
            conditions = payload.get("conditions", []) if isinstance(payload, Mapping) else []
            if not conditions and isinstance(payload, Mapping): conditions = payload.get("condition_results", []) or []
            for ordinal, condition in enumerate(conditions):
                if not isinstance(condition, Mapping): continue
                items.append({"decision_row_id": decision.get("decision_row_id"), "decision_id": decision.get("decision_id"), "observed_ts": decision.get("observed_ts"), "decision": decision.get("status"), "condition_ordinal": ordinal, "name": condition.get("name"), "state": condition.get("state", condition.get("status")), "observed": condition.get("observed"), "expected": condition.get("expected"), "reason": condition.get("reason"), "mandatory": condition.get("mandatory", True), "mode": payload.get("mode") if isinstance(payload, Mapping) else None})
        items.sort(key=lambda row: (row["observed_ts"], int(row["decision_row_id"]), int(row["condition_ordinal"])), reverse=bool(recent))
        total_count = len(items)
        order = "desc" if recent else "asc"; filt = {"session_id": session_id, "start_ts": _parse_time(start_ts), "end_ts": _parse_time(end_ts)}; decoded = self._read_cursor(cursor, table="conditions", filters=filt, order=order) if cursor else None
        if decoded:
            key = (decoded["ts"], int(decoded["row_id"]), int(decoded.get("condition_ordinal", 0))); keep_after = []
            for row in items:
                row_key = (row["observed_ts"], int(row["decision_row_id"]), int(row["condition_ordinal"]))
                keep_after.append(row_key < key if recent else row_key > key)
            items = [row for row, keep in zip(items, keep_after) if keep]
        limit = min(self.max_limit, max(1, int(limit))); has_more = len(items) > limit; items = items[:limit]
        next_cursor = prev_cursor = None
        if items:
            next_cursor = self._cursor(table="conditions", filters=filt, order=order, op="after", ts=str(items[-1]["observed_ts"]), row_id=int(items[-1]["decision_row_id"]), condition_ordinal=int(items[-1]["condition_ordinal"])) if has_more else None
            prev_cursor = self._cursor(table="conditions", filters=filt, order=order, op="before", ts=str(items[0]["observed_ts"]), row_id=int(items[0]["decision_row_id"]), condition_ordinal=int(items[0]["condition_ordinal"])) if decoded else None
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
        status["coverage"] = _snapshot_coverage(self.store, session_id)
        status["revisions_count"] = int(self.store.conn.execute("SELECT COUNT(*) FROM candles WHERE session_id=? AND revision>0", (session_id,)).fetchone()[0])
        status.update(_snapshot_cfd_lifecycle(self.store, session_id))
        status["gaps"] = self.query_gaps(session_id)
        _apply_checkpoint_projection(self.store, status, session_id)
        return status



__all__ = ["QueryPage", "QueryService"]
