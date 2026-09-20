"""Offline JSON/HTML projections for historical research evidence.

``render_research_report`` accepts the mapping returned by the research
protocol (or a mapping-compatible ``ResearchEvidenceReport`` object).  The
renderer is intentionally a sink: it does not read a dataset, open SQLite,
run a strategy, contact a provider, or infer a positive result from a fixture.
It emits a bounded, self-contained JSON/HTML bundle suitable for local review.

The report keeps quote provenance, modeled fills, and server DEMO observations
separate.  Missing marks, costs, samples, and provenance remain ``None`` or
``UNKNOWN`` with an explicit status; a missing value is never represented by
zero.  Bid/ask and slippage are shown once in the cost bridge and are not
subtracted again by chart code.
"""

from __future__ import annotations

import contextlib
import dataclasses
import errno
import html
import json
import os
import stat
from collections import Counter, defaultdict
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, ClassVar, cast

from .research_reporting_metrics import (
    MAX_TRADE_SUMMARIES,
    MAX_VISUAL_POINTS,
    aggregate_sessions_episodes,
    as_mapping,
    break_even_metrics,
    closed_trade,
    confidence_for_result,
    cost_bridge,
    cost_bridge_from_rows,
    dimensions,
    drawdown_equity,
    funnel_report,
    is_artifact_descriptor,
    ledger_rows,
    market_structure_svgs,
    mfe_mae,
    number,
    project_market_structure,
    provenance,
    regime_session_report,
    render_svg,
    safe_value,
    spread_report,
    trade_amounts,
)

EvidenceReportMapping = Mapping[str, Any]

JSON_FILENAME = "report.json"
HTML_FILENAME = "report.html"
REPORT_SCHEMA = "mtf-lab.research-report.v1"
_CURRENT_UID = os.getuid()
_SENSITIVE_KEYWORDS = ("token", "secret", "password", "authorization", "api_key", "refresh")


@dataclasses.dataclass(frozen=True, slots=True)
class ResearchReportPaths(Mapping[str, Path]):
    """Paths written by :func:`render_research_report`.

    The mapping interface keeps the return value convenient for CLI adapters
    while the named properties make the contract unambiguous for callers.
    """

    json_path: Path
    html_path: Path

    _KEYS: ClassVar[tuple[str, str]] = ("json", "html")

    @property
    def json(self) -> Path:
        return self.json_path

    @property
    def html(self) -> Path:
        return self.html_path

    def __getitem__(self, key: str) -> Path:
        if key in {"json", "json_path", "report_json"}:
            return self.json_path
        if key in {"html", "html_path", "report_html"}:
            return self.html_path
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._KEYS)

    def __len__(self) -> int:
        return len(self._KEYS)

    def as_dict(self) -> dict[str, Path]:
        return {"json": self.json_path, "html": self.html_path}


def _rows(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        if is_artifact_descriptor(value):
            return []
        return [value]
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    result: list[Mapping[str, Any]] = []
    for item in value:
        row = as_mapping(item)
        if row:
            result.append(row)
    return result


def _preserved_list(value: Any) -> list[Any]:
    """Keep supplied list items, including scalar explanatory messages.

    ``_rows`` is intentionally mapping-only because most report sections are
    tabular.  ``insufficient_cases`` is also used for human-readable reasons,
    however, so applying ``_rows`` to that field would silently discard a
    perfectly valid ``list[str]``.  Keep the value bounded and pass it through
    the normal redaction projection without changing the source object.
    """

    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [safe_value(item) for item in list(value)[:MAX_TRADE_SUMMARIES]]
    return [safe_value(value)]


def _retention_metadata(descriptor: Any, retained_count: int) -> dict[str, Any]:
    declared: int | None = None
    if is_artifact_descriptor(descriptor):
        raw_count = descriptor.get("count", descriptor.get("row_count"))
        if isinstance(raw_count, int) and not isinstance(raw_count, bool) and raw_count >= 0:
            declared = raw_count
    else:
        declared = retained_count
    complete = declared is not None and declared == retained_count
    return {
        "status": "ASSESSED" if complete else "INSUFFICIENT",
        "complete": complete,
        "row_count": declared,
        "display_count": retained_count,
        "truncated": declared is None or retained_count < declared,
        "reason": None if complete else "retained_suffix_incomplete",
    }


def _as_payload(value: Any) -> Mapping[str, Any]:
    payload = dict(as_mapping(value))
    # HistoricalBacktestResult exposes bounded ``ArtifactRows.retained`` data
    # while its DTO representation intentionally contains only artifact
    # metadata.  Consume the already-retained rows if the caller passed the
    # object itself; never open an artifact path from a report renderer.
    for name in ("ledger", "equity", "funnel"):
        retained = getattr(getattr(value, name, None), "retained", None)
        if isinstance(retained, Sequence) and not isinstance(retained, (str, bytes, bytearray)):
            payload[f"_{name}_retained"] = list(retained)
            payload[f"_{name}_retention"] = _retention_metadata(payload.get(name), len(retained))
    if not payload:
        raise TypeError("evidence report debe ser mapping-compatible")
    return payload


def _has_result_content(payload: Mapping[str, Any]) -> bool:
    for key in ("ledger", "trades", "equity_mark_to_market", "equity"):
        value = payload.get(key)
        if isinstance(value, Mapping) and value:
            return True
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and value:
            return True
    return False


def _raw_results(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = payload.get(
        "results",
        payload.get("result", payload.get("reports", payload.get("evidence_results"))),
    )
    result: list[Mapping[str, Any]]
    if isinstance(raw, Mapping):
        # Accept either {"items": [...]} or an individual result mapping.
        items = raw.get("items", raw.get("results"))
        if items is not None:
            result = _rows(items)
        elif raw and all(isinstance(item, Mapping) for item in raw.values()):
            result = [{**dict(as_mapping(item)), "variant": str(key)} for key, item in raw.items()]
        else:
            result = [raw]
    else:
        result = _rows(raw)
    return result


def _inherited_artifacts(payload: Mapping[str, Any]) -> dict[str, Any]:
    inherited: dict[str, Any] = {}
    for name in ("ledger", "equity", "funnel"):
        retained = payload.get(f"_{name}_retained")
        if isinstance(retained, Sequence) and not isinstance(retained, (str, bytes, bytearray)):
            inherited[name] = list(retained)
            retention = payload.get(f"_{name}_retention")
            if isinstance(retention, Mapping):
                inherited[f"_{name}_retention"] = dict(retention)
    return inherited


def _apply_inherited(item: Mapping[str, Any], inherited: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(item)
    for name in ("ledger", "equity", "funnel"):
        if name not in inherited:
            continue
        if name not in merged or is_artifact_descriptor(merged.get(name)):
            merged[name] = inherited[name]
            retention_key = f"_{name}_retention"
            if retention_key in inherited:
                merged[retention_key] = inherited[retention_key]
    return merged


def _results(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    result = _raw_results(payload)
    inherited = _inherited_artifacts(payload)

    if result:
        if inherited:
            result = [cast(Mapping[str, Any], _apply_inherited(item, inherited)) for item in result]
        return result
    decisions = payload.get("decisions")
    if isinstance(decisions, Mapping):
        return [
            {**dict(as_mapping(item)), "candidate_id": str(candidate_id), "variant": str(candidate_id)}
            for candidate_id, item in decisions.items()
            if as_mapping(item)
        ]
    variants = _rows(payload.get("variants"))
    if variants:
        if inherited:
            return [_apply_inherited(item, inherited) for item in variants]
        return [dict(item) for item in variants]
    if _has_result_content(payload):
        return [_apply_inherited(payload, inherited) if inherited else payload]
    return []


def _descriptor_count(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("ledger descriptor count must be a non-negative integer")
    return value


def _validate_ledger_reference(descriptor: Mapping[str, Any], reference: Mapping[str, Any]) -> int:
    expected_count = _descriptor_count(descriptor.get("count", descriptor.get("row_count")))
    path_reference = reference.get("path_reference")
    if path_reference is None or str(path_reference) != str(descriptor.get("path")):
        raise ValueError("ledger descriptor/reference path mismatch")
    if reference.get("kind") != "ledger":
        raise ValueError("ledger snapshot kind mismatch")
    if reference.get("complete") is not True:
        raise ValueError("ledger snapshot is not complete")
    pager = as_mapping(reference.get("pager"))
    if pager.get("source") != "complete_jsonl_artifact":
        raise ValueError("ledger snapshot pager source is not verified")
    return expected_count


def _prepare_ledger_snapshot(result: Mapping[str, Any]) -> Mapping[str, Any]:
    """Load one hashed JSONL ledger through the bounded market-research seam.

    Serialized historical results intentionally contain only an artifact
    descriptor.  The renderer may consume that descriptor, but only through
    ``iter_snapshot_pages`` so path, owner, hash and row-count checks remain
    centralized.  Rows kept for display are bounded; economic aggregation is
    streamed over every page.
    """

    descriptor = result.get("ledger")
    if not isinstance(descriptor, Mapping) or descriptor.get("path") is None:
        return result
    prepared = dict(result)
    reference = as_mapping(as_mapping(result.get("snapshot")).get("ledger"))
    expected_count: int | None = None
    display_rows: list[Mapping[str, Any]] = []
    stream_meta: dict[str, Any] = {
        "status": "NOT_ASSESSED",
        "complete": False,
        "row_count": None,
        "display_count": 0,
        "truncated": False,
    }
    try:
        expected_count = _validate_ledger_reference(descriptor, reference)

        # Import locally to keep the renderer's pure inline-data path free of
        # campaign-service initialization and to use the existing verified
        # paging seam as the sole file-backed entry point.
        from .market_research import iter_snapshot_pages

        observed = 0

        def stream_rows() -> Iterator[Mapping[str, Any]]:
            nonlocal observed
            for page in iter_snapshot_pages(reference, page_size=256):
                for row in page:
                    observed += 1
                    if len(display_rows) < MAX_TRADE_SUMMARIES:
                        display_rows.append(dict(row))
                    yield row

        bridge = cost_bridge_from_rows(
            result,
            stream_rows(),
            expected_source_count=expected_count,
        )
        prepared["ledger"] = display_rows
        stream_meta.update(
            {
                "status": "ASSESSED" if bridge.get("source_complete") is True else "INSUFFICIENT",
                "complete": bridge.get("source_complete") is True,
                "row_count": observed,
                "display_count": len(display_rows),
                "truncated": observed > len(display_rows),
                "kind": "ledger",
                "source": "complete_jsonl_artifact",
                "sha256": reference.get("sha256"),
            }
        )
    except (OSError, TypeError, ValueError) as exc:
        # Do not expose paths or exception text in the public report.  The
        # bridge remains fail-closed and never becomes an empty/known ledger.
        bridge = cost_bridge_from_rows(
            result,
            (),
            expected_source_count=expected_count,
            source_error=f"ledger_snapshot_invalid:{type(exc).__name__}",
        )
        prepared["ledger"] = []
        stream_meta.update(
            {
                "status": "INVALID",
                "error": f"ledger_snapshot_invalid:{type(exc).__name__}",
            }
        )
    prepared["_verified_ledger_cost_bridge"] = bridge
    prepared["_ledger_stream"] = stream_meta
    return prepared


def _first(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return None


def _text(value: Any, default: str = "UNKNOWN") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _result_id(result: Mapping[str, Any], index: int) -> str:
    value = _first(result, "evaluation_id", "eval_id", "trial_id", "result_id", "analysis_id", "id")
    return _text(value, f"result-{index + 1}")


def _raw_provenance(payload: Mapping[str, Any], result: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return provenance(payload, result)


def _find_in_sources(
    payload: Mapping[str, Any], data: Mapping[str, Any], dataset: Mapping[str, Any], keys: Sequence[str]
) -> Any:
    for source in (payload, data, dataset):
        value = _first(source, *keys)
        if value is not None:
            return value
    return None


def _select_public_identity(payload: Mapping[str, Any]) -> dict[str, Any]:
    data = as_mapping(payload.get("data"))
    dataset = as_mapping(payload.get("dataset"))
    identity: dict[str, Any] = {}
    fields = {
        "run_id": ("run_id", "report_id", "evaluation_id"),
        "manifest_version": ("manifest_version", "schema_version"),
        "dataset_ref": ("dataset_ref", "dataset_id", "capture_id"),
        "dataset_hash": ("dataset_hash", "data_hash", "capture_hash"),
        "capture_hash": ("capture_hash", "data_hash"),
        "config_hash": ("config_hash",),
        "code_hash": ("code_hash",),
        "instrument": ("instrument",),
    }
    for output, keys in fields.items():
        value = _find_in_sources(payload, data, dataset, keys)
        if value is not None:
            identity[output] = safe_value(value, key=output)
    coverage = _find_in_sources(payload, data, dataset, ("coverage", "coverage_summary"))
    if coverage is not None:
        identity["coverage"] = safe_value(coverage, key="coverage")
    extras = {
        "provider": ("provider", "source_provider"),
        "mode": ("mode", "source_mode"),
        "partition": ("partition", "dataset_partition"),
        "periods": ("periods", "windows"),
    }
    for output, keys in extras.items():
        value = _find_in_sources(payload, data, dataset, keys)
        if value is not None:
            identity[output] = safe_value(value, key=output)
    # Paths are deliberately excluded.  The output is local, but a path can
    # still disclose a user's home layout or a private dataset name.
    return identity


def _public_market_structure(value: Any) -> Any:
    """Keep chart inputs and public provenance URLs as inert report data."""

    raw = dict(as_mapping(value))
    if not raw:
        return safe_value(value)
    return safe_value(raw)


def _decision(result: Mapping[str, Any]) -> dict[str, Any]:
    raw = result.get("decision")
    value = as_mapping(raw)
    if not value and isinstance(raw, str):
        value = {"status": raw, "decision": raw}
    if not value:
        return {"status": "UNKNOWN", "reason": "decision_not_present"}
    selected = {
        key: value.get(key) for key in ("status", "decision", "reason", "basis", "promote", "holdout") if key in value
    }
    if "status" not in selected and "decision" in selected:
        selected["status"] = selected["decision"]
    return cast(dict[str, Any], safe_value(selected))


def _holdout(result: Mapping[str, Any]) -> dict[str, Any]:
    for key in ("holdout", "window", "split", "partition"):
        value = result.get(key)
        if isinstance(value, Mapping):
            return cast(dict[str, Any], safe_value(value, key=key))
        if value is not None:
            return {key: safe_value(value, key=key)}
    return {"status": "UNKNOWN", "reason": "holdout_membership_not_present"}


def _evidence_gate_projection(result: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "sample",
        "walkforward",
        "base",
        "stress",
        "drop_best",
        "criteria",
        "reasons",
        "eligibility",
        "auto_promote",
        "trading_enabled",
        "no_operation_reference",
    )
    selected = {key: result.get(key) for key in keys if key in result}
    return cast(dict[str, Any], safe_value(selected))


def _computed_statistics(result: Mapping[str, Any]) -> dict[str, Any]:
    """Reuse the canonical statistics projection when the result is compatible."""

    try:
        from .research_statistics import result_metrics

        computed = result_metrics(result)
    except Exception as exc:  # noqa: BLE001 - one malformed result must not hide other evidence
        return {"status": "NOT_ASSESSED", "reason": f"statistics_unavailable:{type(exc).__name__}"}
    return cast(dict[str, Any], safe_value(computed))


def _trade_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    amounts = trade_amounts(row)
    return {
        "trade_id": _text(_first(row, "trade_id", "id", "identity")),
        "signal_id": _text(row.get("signal_id")),
        "episode_id": _text(_first(row, "episode_id", "episode")),
        "direction": _text(_first(row, "direction", "side", "order_side")),
        "state": _text(_first(row, "state", "status")),
        "entry_at": _text(_first(row, "entry_available_at", "entry_at", "entry_ts")),
        "close_at": _text(_first(row, "close_available_at", "close_at", "exit_at", "close_ts")),
        "gross": number(amounts["gross"]),
        "costs": number(amounts["cost"]),
        "net": number(amounts["net"]),
        "costs_known": row.get("costs_known") is True or amounts["cost"] is not None,
        "provenance": safe_value(_first(row, "provenance", "lineage"))
        if _first(row, "provenance", "lineage") is not None
        else None,
    }


def _result_projection(payload: Mapping[str, Any], result: Mapping[str, Any], index: int) -> dict[str, Any]:
    result_id = _result_id(result, index)
    dims = dimensions({}, result, payload)
    bridge = cost_bridge(result)
    break_even = break_even_metrics(result, bridge)
    equity = drawdown_equity(result)
    mfe = mfe_mae(payload, result)
    confidence = confidence_for_result(result)
    sessions = aggregate_sessions_episodes(result, bridge)
    trades = ledger_rows(result)
    known_closed = [row for row in trades if closed_trade(row)]
    display_trades = [_trade_summary(row) for row in known_closed[:MAX_TRADE_SUMMARIES]]
    ledger_stream = as_mapping(result.get("_ledger_stream")) or as_mapping(result.get("_ledger_retention"))
    full_trade_count = ledger_stream.get("row_count", len(trades))
    full_closed_count = bridge.get("closed_trade_count")
    if full_closed_count is None and not ledger_stream:
        full_closed_count = len(known_closed)
    result_provenance = _raw_provenance(payload, result)
    return {
        "result_id": result_id,
        "dimensions": dims,
        "provenance": result_provenance,
        "trade_count": full_trade_count,
        "closed_trade_count": full_closed_count,
        "signal_count": sessions["signals"],
        "episode_count": sessions["episodes"],
        "session_count": sessions["sessions"],
        "trade_summaries": display_trades,
        "trade_summary_truncated": bool(ledger_stream.get("truncated")) or len(known_closed) > len(display_trades),
        "ledger_source": safe_value(ledger_stream) if ledger_stream else {"status": "INLINE"},
        "cost_bridge": bridge,
        "gross_pnl": bridge.get("gross"),
        "costs": bridge.get("costs"),
        "net_pnl": bridge.get("net"),
        "break_even": break_even,
        "equity": equity,
        "equity_mark_to_market": equity.get("mark_to_market"),
        "equity_realized": equity.get("realized"),
        "drawdown": {
            "status": equity.get("status"),
            "max_drawdown": equity.get("max_drawdown"),
            "known_max_drawdown": equity.get("known_max_drawdown"),
            "peak_at": equity.get("max_drawdown_peak_at"),
            "trough_at": equity.get("max_drawdown_trough_at"),
            "recovery_duration_seconds": equity.get("recovery_duration_seconds"),
            "unrecovered_state": equity.get("unrecovered_state"),
        },
        "mfe_mae": mfe,
        "exposure": sessions["exposure"],
        "confidence_intervals": confidence,
        "decision": _decision(result),
        "holdout": _holdout(result),
        "quality": safe_value(_first(result, "quality", "validation"))
        if _first(result, "quality", "validation") is not None
        else {"status": "UNKNOWN"},
        "runner_metrics": safe_value(result.get("metrics")) if result.get("metrics") is not None else {},
        "evidence_gates": _evidence_gate_projection(result),
        "statistics": _computed_statistics(result),
        "supplied_statistics": safe_value(result.get("statistics")) if result.get("statistics") is not None else {},
        "status": "ASSESSED"
        if bridge.get("status") == "ASSESSED"
        and equity.get("status") == "ASSESSED"
        and confidence.get("net_expectancy", {}).get("status") == "ASSESSED"
        else "INSUFFICIENT",
    }


def _variant_groups(results: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for result in results:
        variant = _text(as_mapping(result.get("dimensions")).get("variant"))
        groups[variant].append(result)
    output: list[dict[str, Any]] = []
    for variant in sorted(groups):
        values = [result.get("cost_bridge", {}).get("net") for result in groups[variant]]
        known = [value for value in values if isinstance(value, (int, float)) and not isinstance(value, bool)]
        unknown = len(values) - len(known)
        output.append(
            {
                "variant": variant,
                "result_count": len(groups[variant]),
                "closed_trade_count": sum(int(result.get("closed_trade_count", 0) or 0) for result in groups[variant]),
                "net_known_subtotal": sum(known) if known else None,
                "net": sum(known) if known and unknown == 0 else None,
                "unknown_result_count": unknown,
                "status": "ASSESSED" if known and unknown == 0 else "INSUFFICIENT",
                "reason": None if known and unknown == 0 else "unknown_result_net_or_cost",
            }
        )
    return output


def _provenance_summary(payload: Mapping[str, Any], results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    per_result = [_raw_provenance(payload, result) for result in results]
    if not per_result:
        per_result = [_raw_provenance(payload)]
    labels = sorted({label for item in per_result for label in item.get("labels", [])})
    if not labels:
        labels = _raw_provenance(payload).get("labels", [])
    if not labels:
        labels = ["UNKNOWN"]
    quote_sources = sorted({item.get("quote_source", "UNKNOWN") for item in per_result}) or ["UNKNOWN"]
    fill_sources = sorted({item.get("fill_source", "UNKNOWN") for item in per_result}) or ["UNKNOWN"]
    quote_source = quote_sources[0] if len(quote_sources) == 1 else "UNKNOWN"
    fill_source = fill_sources[0] if len(fill_sources) == 1 else "UNKNOWN"
    return {
        "quote_source": quote_source,
        "fill_source": fill_source,
        "labels": labels,
        "quote_sources": quote_sources,
        "fill_sources": fill_sources,
        "status": "UNKNOWN" if labels == ["UNKNOWN"] else "ASSESSED",
        "synthetic": any(item.get("synthetic") is True for item in per_result),
        "network_observed": any(item.get("network_observed") is True for item in per_result),
        "rules": {
            "REAL_HISTORICAL_QUOTES": "cotización histórica bid/ask explícitamente identificada",
            "MODEL_FILL": "fill o ejecución modelada; no es un fill de servidor",
            "SERVER_DEMO": "observación/fill de servidor DEMO explícitamente respaldado",
            "UNKNOWN": "no se eleva la etiqueta sin procedencia explícita",
        },
    }


def _candidate_summary(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get(
        "variants",
        payload.get("candidates", payload.get("candidate_registry", payload.get("trials"))),
    )
    if raw is None and isinstance(payload.get("decisions"), Mapping):
        raw = [{"candidate_id": key, **dict(as_mapping(value))} for key, value in payload["decisions"].items()]
    output: list[dict[str, Any]] = []
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        for item in raw:
            if isinstance(item, str):
                output.append({"id": item, "variant": item})
    for item in _rows(raw):
        variant = _text(_first(item, "variant", "candidate_id", "id", "name"))
        selected = {
            key: item.get(key)
            for key in (
                "variant",
                "candidate_id",
                "id",
                "name",
                "role",
                "hypothesis",
                "timeframes",
                "profile",
                "horizon_seconds",
                "decision",
                "reasons",
                "sample",
                "walkforward",
            )
            if key in item
        }
        selected["id"] = variant
        output.append(cast(dict[str, Any], safe_value(selected)))
    return output[:MAX_TRADE_SUMMARIES]


def build_research_report(
    evidence: EvidenceReportMapping | Any, *, max_visual_points: int = MAX_VISUAL_POINTS
) -> dict[str, Any]:
    """Build the bounded machine-readable projection without writing files."""

    payload = _as_payload(evidence)
    results = [_prepare_ledger_snapshot(result) for result in _results(payload)]
    projections = [_result_projection(payload, result, index) for index, result in enumerate(results)]
    if max_visual_points != MAX_VISUAL_POINTS:
        # Rebuild only the visual series with the requested bound.  The source
        # ledger and all aggregate counts remain untouched.
        for result, source in zip(projections, results, strict=True):
            result["equity"] = drawdown_equity(
                source, max_points=max(1, min(MAX_VISUAL_POINTS, int(max_visual_points)))
            )
            result["drawdown"]["max_drawdown"] = result["equity"].get("max_drawdown")
    bridge = [item["cost_bridge"] for item in projections]
    equity = [
        {
            "result_id": item["result_id"],
            "dimensions": item["dimensions"],
            "mark_to_market": item["equity"]["mark_to_market"],
            "realized": item["equity"]["realized"],
            "unrealized": item["equity"]["unrealized"],
        }
        for item in projections
    ]
    total_trade_rows: int | None = 0
    total_closed_rows: int | None = 0
    known_net_trades = 0
    known_net_subtotal = Decimal("0")
    known_net_seen = False
    for projection in projections:
        trade_count = projection.get("trade_count")
        closed_count = projection.get("closed_trade_count")
        if isinstance(trade_count, int) and not isinstance(trade_count, bool) and total_trade_rows is not None:
            total_trade_rows += trade_count
        else:
            total_trade_rows = None
        if isinstance(closed_count, int) and not isinstance(closed_count, bool) and total_closed_rows is not None:
            total_closed_rows += closed_count
        else:
            total_closed_rows = None
        bridge_value = projection["cost_bridge"].get("known_net_count")
        if isinstance(bridge_value, int) and not isinstance(bridge_value, bool):
            known_net_trades += bridge_value
        subtotal = projection["cost_bridge"].get("net_known_subtotal")
        if subtotal is not None:
            try:
                known_net_subtotal += Decimal(str(subtotal))
                known_net_seen = True
            except (InvalidOperation, TypeError, ValueError):
                pass
    decision_counts = Counter(_text(as_mapping(item.get("decision")).get("status")) for item in projections)
    source = _select_public_identity(payload)
    generated_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
    market = spread_report(payload, results)
    market["spread_by_timeframe"] = market["by_timeframe"]
    market["spread_by_hour_utc"] = market["by_hour_utc"]
    market["spread_by_session"] = market["by_session"]
    structure_input = _first(payload, "market_structure", "market_structure_report")
    structure_projection = (
        project_market_structure(structure_input)
        if structure_input is not None
        else {"status": "NOT_ASSESSED", "reason": "market_structure_not_present"}
    )
    if structure_input is not None:
        market = {
            "status": "NOT_ASSESSED",
            "reason": "legacy_tick_projection_disabled_in_market_structure_mode",
            "source": "market_structure.v1",
            "unknown_values_are_not_zero": True,
        }
    generated_quality = [
        {"result_id": item["result_id"], "dimensions": item["dimensions"], "quality": item["quality"]}
        for item in projections
    ]
    supplied_quality = payload.get("quality")
    quality_output = (
        generated_quality if projections else safe_value(supplied_quality) if supplied_quality is not None else []
    )
    generated_insufficient = [
        {
            "result_id": item["result_id"],
            "dimensions": item["dimensions"],
            "status": item["status"],
            "cost_status": item["cost_bridge"].get("status"),
            "equity_status": item["equity"].get("status"),
            "confidence_status": item["confidence_intervals"].get("net_expectancy", {}).get("status"),
            "conditional_on_costs": item["cost_bridge"].get("conditional_on_costs"),
        }
        for item in projections
        if item["status"] != "ASSESSED"
        or item["cost_bridge"].get("conditional_on_costs")
        or item["confidence_intervals"].get("net_expectancy", {}).get("status") != "ASSESSED"
    ]
    supplied_insufficient = payload.get("insufficient_cases")
    supplied_insufficient_output = _preserved_list(supplied_insufficient)
    insufficient_output = [*supplied_insufficient_output, *generated_insufficient]
    measurement_output = (
        safe_value(payload.get("measurement"))
        if payload.get("measurement") is not None
        else structure_projection.get("measurement", {"status": "UNKNOWN"})
    )
    evidence_gates_output = (
        safe_value(payload.get("evidence_gates"))
        if payload.get("evidence_gates") is not None
        else {"status": "UNKNOWN", "reason": "evidence_gates_not_provided"}
    )
    cautions_output = _preserved_list(payload.get("cautions", payload.get("caveats")))
    supplied_limitations = _preserved_list(payload.get("limitations"))
    protocol_output = (
        safe_value(payload.get("protocol")) if payload.get("protocol") is not None else {"status": "UNKNOWN"}
    )
    validation_output = (
        safe_value(payload.get("validation")) if payload.get("validation") is not None else {"status": "UNKNOWN"}
    )
    replay_output = (
        safe_value(payload.get("replay_state"))
        if payload.get("replay_state") is not None
        else {"status": "UNKNOWN", "reason": "replay_state_not_provided"}
    )
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "report_schema": REPORT_SCHEMA,
        "input_schema": safe_value(payload.get("schema"), key="input_schema")
        if payload.get("schema") is not None
        else "UNKNOWN",
        "schema_version": 1,
        "generated_at": generated_at,
        "source": source,
        "provenance": _provenance_summary(payload, results),
        "privacy": {
            "scope": "local_private_review",
            "raw_quotes_embedded": False,
            "raw_ticks_embedded": False,
            "raw_trades_embedded": False,
            "trade_summary_limit": MAX_TRADE_SUMMARIES,
            "redaction": "secret_and_account_identifier_keys_redacted",
            "network_used": False,
        },
        "methodology": {
            "economic": "net = gross - total_costs_once; bid/ask and recorded slippage are not deducted twice",
            "unknowns": "NONE/UNKNOWN/insufficient values remain explicit and are never replaced by zero",
            "visuals": "series are downsampled only for display; aggregate calculations use supplied evidence",
            "provenance": "quote source, modeled fill, and server DEMO evidence are separate labels",
            "inference": "confidence intervals are nominal and conditional on observed marks, costs, and dependence assumptions",
            "holdout": "holdout and walk-forward flags are reported, not re-opened or retuned by this renderer",
        },
        "counts": {
            "results": len(results),
            "trades": total_trade_rows,
            "closed_trades": total_closed_rows,
            "known_net_trades": known_net_trades,
            "signals": sum(int(item.get("signal_count", 0) or 0) for item in projections),
            "episodes_known": sum(
                int(item.get("episode_count", 0) or 0) for item in projections if item.get("episode_count") is not None
            ),
        },
        "metrics": {
            "known_net_subtotal": number(known_net_subtotal) if known_net_seen else None,
            "status": "ASSESSED"
            if projections and all(item.get("status") == "ASSESSED" for item in projections)
            else "INSUFFICIENT",
            "unknown_values_are_not_zero": True,
        },
        "market": market,
        "market_structure": _public_market_structure(structure_input)
        if structure_input is not None
        else {"status": "NOT_ASSESSED", "reason": "market_structure_not_present"},
        "market_structure_projection": structure_projection,
        "measurement": measurement_output,
        "protocol": protocol_output,
        "validation": validation_output,
        "replay_state": replay_output,
        "evidence_gates": evidence_gates_output,
        "funnel": funnel_report(payload, results),
        "candidates": _candidate_summary(payload),
        "results": projections,
        "variants": _variant_groups(projections),
        "trade_summaries": [
            {
                "result_id": item["result_id"],
                "dimensions": item["dimensions"],
                "rows": item["trade_summaries"],
                "truncated": item["trade_summary_truncated"],
            }
            for item in projections
        ],
        "quality": quality_output,
        "cost_bridge": bridge,
        "equity": equity,
        "drawdown": [
            item["drawdown"] | {"result_id": item["result_id"], "dimensions": item["dimensions"]}
            for item in projections
        ],
        "mfe_mae": [
            item["mfe_mae"] | {"result_id": item["result_id"], "dimensions": item["dimensions"]} for item in projections
        ],
        "exposure": [
            item["exposure"] | {"result_id": item["result_id"], "dimensions": item["dimensions"]}
            for item in projections
        ],
        "sessions": [
            {"result_id": item["result_id"], "count": item["session_count"], "dimensions": item["dimensions"]}
            for item in projections
        ],
        "episodes": [
            {"result_id": item["result_id"], "count": item["episode_count"], "dimensions": item["dimensions"]}
            for item in projections
        ],
        "regimes_and_sessions": regime_session_report(results),
        "confidence_intervals": [
            item["confidence_intervals"] | {"result_id": item["result_id"], "dimensions": item["dimensions"]}
            for item in projections
        ],
        "decision_summary": {
            "counts": dict(sorted(decision_counts.items())),
            "status": "INSUFFICIENT"
            if not projections
            or any(key in decision_counts for key in ("INSUFFICIENT", "EVIDENCE_INSUFFICIENT", "UNKNOWN"))
            else "ASSESSED",
            "auto_promotion": "PROHIBITED",
        },
        "insufficient_cases": insufficient_output,
        "break_even_fees": [
            {
                "result_id": item["result_id"],
                "value": item["break_even"].get("fee_per_closed_trade"),
                "status": item["break_even"].get("fee_status"),
                "reason": item["break_even"].get("reason"),
                "break_even_probability": item["break_even"].get("break_even_probability"),
                "basis": "additional_average_cost_until_aggregate_gross_reaches_zero",
                "conditional_on_known_gross": True,
            }
            for item in projections
        ],
        "limitations": [
            "El informe es una proyección local; no prueba permisos, fills o rentabilidad futura.",
            "Los resultados sintéticos o MODEL_FILL no son REAL_HISTORICAL_QUOTES ni SERVER_DEMO.",
            "Los intervalos son nominales y condicionados; la dependencia entre episodios puede hacerlos insuficientes.",
            "Una etiqueta favorable no autoriza cuentas, órdenes, publicación ni promoción.",
            *supplied_limitations,
        ],
        "cautions": cautions_output,
    }
    # Keep the published trade-summary cap aligned with the declared report
    # contract while retaining the same redaction/unsafe-value handling.
    return cast(dict[str, Any], safe_value(report, max_items=MAX_TRADE_SUMMARIES))


def _json_text(report: Mapping[str, Any]) -> str:
    return (
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True, separators=(",", ": "), allow_nan=False) + "\n"
    )


def _metric_card(label: str, value: Any) -> str:
    shown = "UNKNOWN" if value is None else str(value)
    return (
        f'<div class="card"><span class="label">{html.escape(label)}</span><strong>{html.escape(shown)}</strong></div>'
    )


def _structure_text(value: Any, default: str = "UNKNOWN") -> str:
    if value is None or value == "":
        return default
    return str(value)


def _structure_status(value: Any, default: str = "NOT_ASSESSED") -> str:
    """Read a displayed status without treating a missing status as PASS."""

    if isinstance(value, Mapping):
        for key in ("status", "state", "quality_status"):
            candidate = value.get(key)
            if candidate is not None and str(candidate).strip():
                return str(candidate)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        statuses = [_structure_status(item, "") for item in value]
        for candidate in statuses:
            if candidate:
                return candidate
    return default


def _structure_bullets(value: Any, *, empty: str = "NOT_ASSESSED") -> str:
    """Render bounded scalar or mapping cases as visible HTML bullets."""

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        cases = list(value)
    elif value is None:
        cases = []
    else:
        cases = [value]
    if not cases:
        return f'<p class="muted">{html.escape(empty)}</p>'
    rendered: list[str] = []
    for item in cases[:MAX_TRADE_SUMMARIES]:
        if isinstance(item, Mapping):
            status = item.get("status")
            reason = item.get("reason")
            result_id = item.get("result_id")
            parts = [str(part) for part in (status, reason) if part not in (None, "")]
            if result_id not in (None, ""):
                parts.insert(0, f"{result_id}")
            text = " · ".join(parts)
            if not text:
                text = json.dumps(safe_value(item), ensure_ascii=False, sort_keys=True)
        else:
            text = _structure_text(item)
        rendered.append(f"<li>{html.escape(text)}</li>")
    return "<ul>" + "".join(rendered) + "</ul>"


def _structure_number(value: Any, decimals: int) -> str:
    if value is None or value == "":
        return "UNKNOWN"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError, OverflowError):
        return "UNKNOWN"


def _structure_instrument(source: Mapping[str, Any], projection: Mapping[str, Any]) -> str:
    instrument = _structure_text(source.get("instrument"))
    if instrument == "UNKNOWN":
        instrument = _structure_text(projection.get("instrument"))
    if instrument == "UNKNOWN" and projection.get("schema") == "mtf-lab.market-structure.v1":
        return "EUR/USD"
    return instrument


def _structure_measurement_counts(measurement: Mapping[str, Any], projection: Mapping[str, Any]) -> dict[str, Any]:
    quote_count = measurement.get("quote_count")
    if quote_count is None:
        quote_count = projection.get("quote_count")
    selected_quote_count = measurement.get("selected_quote_count")
    if selected_quote_count is None:
        selected_quote_count = quote_count
    full_decode_quote_count = measurement.get("full_decode_quote_count")
    selected_over_full = measurement.get("selected_over_full")
    if selected_over_full is None:
        selected_over_full = measurement.get("selected_of_full")
    full_decode_display = measurement.get("full_decode_quote_count_display")
    if full_decode_display is None:
        full_decode_display = full_decode_quote_count
    selected_over_full_display = measurement.get("selected_over_full_display")
    if selected_over_full_display is None:
        selected_over_full_display = selected_over_full
    return {
        "quote_count": quote_count,
        "selected_quote_count": selected_quote_count,
        "full_decode_quote_count": full_decode_quote_count,
        "full_decode_display": full_decode_display,
        "selected_over_full": selected_over_full_display,
    }


def _structure_rss_display(value: Any) -> str:
    try:
        return f"{float(value) / 1024 / 1024:.1f} MiB" if value is not None else "UNKNOWN"
    except (TypeError, ValueError, OverflowError):
        return "UNKNOWN"


def _structure_measurement_view(report: Mapping[str, Any]) -> dict[str, Any]:
    projection = as_mapping(report.get("market_structure_projection"))
    source = as_mapping(report.get("source"))
    measurement = as_mapping(report.get("measurement"))
    if not measurement:
        measurement = as_mapping(projection.get("measurement"))
    instrument = _structure_instrument(source, projection)
    coverage_start = _structure_text(projection.get("coverage_start"))
    coverage_end = _structure_text(projection.get("coverage_end"))
    window = (
        f"{coverage_start[:10]} → {coverage_end[:10]}"
        if coverage_start != "UNKNOWN" and coverage_end != "UNKNOWN"
        else "UNKNOWN"
    )
    spread = as_mapping(projection.get("spread_tick_weighted"))
    mean = spread.get("mean_raw") if spread.get("mean_raw") is not None else spread.get("mean")
    measurement_counts = _structure_measurement_counts(measurement, projection)
    wall_time_ratio = measurement.get("wall_time_ratio")
    wall_time_ratio_display = measurement.get("wall_time_ratio_display")
    if wall_time_ratio_display is None and wall_time_ratio is not None:
        wall_time_ratio_display = f"{_structure_number(wall_time_ratio, 3)}x · sólo relación wall"
    days = projection.get("coverage_daily")
    day_count = len(days) if isinstance(days, list) else None
    elapsed = measurement.get("elapsed_seconds")
    rss_display = _structure_rss_display(measurement.get("peak_rss_bytes"))
    source_label = _structure_text(projection.get("provenance"))
    if source_label == "REAL_HISTORICAL_QUOTES_NOT_DEMO_FILLS":
        source_label = "REAL_HISTORICAL_QUOTES"
    spread_unit = _structure_text(projection.get("spread_unit"))
    return {
        "instrument": instrument,
        "window": window,
        "source": source_label,
        "measurement": measurement,
        **measurement_counts,
        "wall_time_ratio": wall_time_ratio,
        "wall_time_ratio_display": wall_time_ratio_display or "UNKNOWN",
        "spread_mean": mean,
        "spread_unit": spread_unit,
        "spread_mean_display": f"{_structure_number(mean, 6)} pip" if mean is not None else "UNKNOWN",
        "days": day_count,
        "elapsed_seconds": elapsed,
        "elapsed_display": f"{_structure_number(elapsed, 2)} s" if elapsed is not None else "UNKNOWN",
        "rss": rss_display,
        "coverage_daily": days if isinstance(days, list) else [],
        "gaps": as_mapping(projection.get("gaps_utc")),
    }


def _structure_gap_table(gaps: Mapping[str, Any]) -> str:
    rows_gap = gaps.get("largest")
    if not isinstance(rows_gap, list) or not rows_gap:
        count = gaps.get("count")
        return f'<p class="muted">Gaps publicados: {html.escape(_structure_text(count))}; no se ocultan puntos faltantes.</p>'
    displayed_count = sum(1 for item in rows_gap if isinstance(item, Mapping))
    body = "".join(
        "<tr>"
        + "".join(f"<td>{html.escape(_structure_text(item.get(key)))}</td>" for key in ("from", "to", "seconds"))
        + "</tr>"
        for item in rows_gap
        if isinstance(item, Mapping)
    )
    count = gaps.get("count")
    classification = _structure_text(gaps.get("classification"))
    count_number = number(count)
    if isinstance(count_number, int) and count_number >= displayed_count:
        if count_number > displayed_count:
            count_label = (
                f"Mostrando {displayed_count} mayores de {count_number} gaps publicados; "
                f"{count_number - displayed_count} adicionales no se muestran."
            )
        else:
            count_label = f"Mostrando los {displayed_count} gaps publicados."
    else:
        count_label = f"Mostrando {displayed_count} gaps; total {_structure_text(count)}."
    return (
        f'<p class="muted">{html.escape(count_label)} · clasificación: '
        f"{html.escape(classification)}</p>"
        '<div class="table-scroll"><table><thead><tr><th>Desde UTC</th><th>Hasta UTC</th><th>Duración (seconds)</th></tr></thead>'
        f"<tbody>{body}</tbody></table></div>"
    )


def _structure_quality_view(report: Mapping[str, Any], measurement: Mapping[str, Any]) -> str:
    quality = report.get("quality", [])
    insufficient = report.get("insufficient_cases", [])
    evidence = report.get("evidence_gates")
    if evidence is None:
        evidence = {"status": "UNKNOWN", "reason": "evidence_gates_not_provided"}
    details = {
        "quality": quality,
        "insufficient_cases": insufficient,
        "evidence_gates": evidence,
        "measurement": measurement,
    }
    quality_state = _structure_status(quality)
    insufficient_state = str(len(insufficient)) if isinstance(insufficient, list) and insufficient else "NOT_ASSESSED"
    evidence_state = (
        "PRESENT"
        if evidence and evidence != {"status": "UNKNOWN", "reason": "evidence_gates_not_provided"}
        else "UNKNOWN"
    )
    measurement_present = bool(measurement) and measurement != {"status": "UNKNOWN"}
    raw_validation = quality.get("raw_validation") if isinstance(quality, Mapping) else None
    if raw_validation is None and isinstance(quality, Mapping):
        raw_validation = quality.get("dataset_validation")
    raw_issues = quality.get("raw_issues_count") if isinstance(quality, Mapping) else None
    if raw_issues is None and isinstance(quality, Mapping):
        raw_issues = quality.get("issues_total")
    raw_state = _structure_text(raw_validation, "UNKNOWN")
    if raw_issues is not None:
        raw_state = f"{raw_state} · issues={raw_issues}"
    return (
        '<section><h2>Calidad, insuficiencia y medición</h2><div class="grid">'
        f"{_metric_card('Calidad', quality_state)}"
        f"{_metric_card('Validación raw', raw_state)}"
        f"{_metric_card('Casos insuficientes', insufficient_state)}"
        f"{_metric_card('Evidence gates', evidence_state)}"
        f"{_metric_card('Measurement', 'PRESENT' if measurement_present else 'UNKNOWN')}"
        "</div><h3>Casos insuficientes y límites</h3>"
        f"{_structure_bullets(insufficient, empty='Sin casos insuficientes suministrados; el alcance sigue siendo NOT_ASSESSED.')}"
        "<details><summary>Detalles de calidad/evidence/measurement</summary>"
        f"<pre>{html.escape(json.dumps(details, ensure_ascii=False, indent=2, sort_keys=True))}</pre></details></section>"
    )


def _structure_view(report: Mapping[str, Any], charts: Sequence[str]) -> dict[str, str]:
    view = _structure_measurement_view(report)
    chart_html = "".join(f'<div class="chart">{chart}</div>' for chart in charts)
    coverage = view["coverage_daily"]
    days_with_quotes = _structure_text(view["days"])
    gaps = view["gaps"]
    elapsed_display = view["elapsed_display"]
    spread_display = view["spread_mean_display"]
    details = {
        "measurement": view["measurement"],
        "market_structure_projection": report.get("market_structure_projection", {}),
        "coverage_daily": coverage,
        "gaps_utc": gaps,
    }
    return {
        "header": (
            f"<header><h1>MTF Lab · estructura de mercado · {html.escape(view['instrument'])}</h1>"
            f'<p class="muted">Ventana UTC: {html.escape(view["window"])} · procedencia: '
            f"{html.escape(view['source'])} · sólo diagnóstico descriptivo; no es fill ni rentabilidad.</p></header>"
        ),
        "summary": (
            '<section><h2>Resumen descriptivo</h2><div class="grid">'
            f"{_metric_card('Quotes observadas', view['quote_count'])}"
            f"{_metric_card('Decodificación completa', view['full_decode_display'])}"
            f"{_metric_card('Selección / total', view['selected_over_full'])}"
            f"{_metric_card('Spread medio', spread_display)}"
            f"{_metric_card('Días con quotes', days_with_quotes)}"
            f"{_metric_card('Tiempo transcurrido', elapsed_display)}"
            f"{_metric_card('Pico RSS', view['rss'])}"
            f"{_metric_card('Relación wall', view['wall_time_ratio_display'])}"
            '</div><p class="muted">Economía/operaciones/trading: NOT_ASSESSED; este DTO no contiene ledger económico.</p></section>'
        ),
        "legacy_market": "<section><h2>Mercado legacy</h2><p>Oculto: el informe usa agregados de market_structure.v1.</p></section>",
        "structure": (
            "<section><h2>Estructura agregada del mercado</h2>"
            f'<div class="grid charts">{chart_html}</div>'
            f"<h3>Gaps UTC publicados</h3>{_structure_gap_table(gaps)}"
            "<details><summary>Metadatos y agregados completos</summary>"
            f"<pre>{html.escape(json.dumps(details, ensure_ascii=False, indent=2, sort_keys=True))}</pre></details></section>"
        ),
        "quality": _structure_quality_view(report, view["measurement"]),
        "economic": '<section><h2>Economía</h2><p class="muted">NOT_ASSESSED · sin ledger/operaciones/equity económica en este diagnóstico.</p></section>',
        "evaluation": '<section><h2>Evaluación</h2><p class="muted">NOT_ASSESSED · no se ejecutaron variantes ni operaciones en este diagnóstico descriptivo.</p></section>',
        "cautions": (
            "<section><h2>Cautelas y estado de revalidación</h2>"
            f"{_structure_bullets(report.get('cautions'), empty='Sin cautelas suministradas; revisar los límites del diagnóstico.')}"
            f"<details><summary>Validación y replay</summary><pre>{html.escape(json.dumps({'validation': report.get('validation', {}), 'replay_state': report.get('replay_state', {})}, ensure_ascii=False, indent=2, sort_keys=True))}</pre></details></section>"
        ),
    }


def _html_report(report: Mapping[str, Any]) -> str:
    counts = as_mapping(report.get("counts"))
    provenance_data = as_mapping(report.get("provenance"))
    market = as_mapping(report.get("market"))
    structure_source = report.get("market_structure")
    structure_charts = market_structure_svgs(structure_source)
    structure_view = _structure_view(report, structure_charts) if structure_charts else {}
    charts: list[str] = list(structure_charts)
    if not charts:
        for item in _rows(report.get("equity"))[:32]:
            result_id = _text(item.get("result_id"))
            series = as_mapping(item.get("mark_to_market"))
            charts.append(render_svg(f"Equity MTM · {result_id}", series, stroke="#2563eb", value_key="equity"))
            realized = as_mapping(item.get("realized"))
            if realized.get("points"):
                charts.append(
                    render_svg(f"Equity realizada · {result_id}", realized, stroke="#059669", value_key="realized")
                )
        spread_overall = as_mapping(market.get("overall"))
        spread_summary = as_mapping(spread_overall.get("spread"))
        spread_series = {
            "points": [
                {"at": "min", "value": spread_summary.get("min")},
                {"at": "mean", "value": spread_summary.get("mean")},
                {"at": "max", "value": spread_summary.get("max")},
            ],
            "basis": "spread_summary_display_only",
        }
        charts.append(render_svg("Spread observado", spread_series, stroke="#b45309"))
        price_series = as_mapping(market.get("price_series"))
        if price_series.get("points"):
            charts.append(render_svg("Precio midpoint (visual)", price_series, stroke="#7c3aed"))
    charts = charts[:4]
    spread_overall = as_mapping(market.get("overall"))
    economic_chart_html = "" if structure_charts else "".join(f'<div class="chart">{chart}</div>' for chart in charts)
    report_json = _json_text(report).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    labels = provenance_data.get("labels", [])
    label_html = (
        " ".join(f'<span class="pill">{html.escape(str(label))}</span>' for label in labels)
        or '<span class="pill">UNKNOWN</span>'
    )
    variant_rows = _rows(report.get("variants"))
    variant_table = "".join(
        "<tr>"
        + "".join(
            f"<td>{html.escape(str(row.get(key) if row.get(key) is not None else 'UNKNOWN'))}</td>"
            for key in ("variant", "result_count", "closed_trade_count", "net", "status")
        )
        + "</tr>"
        for row in variant_rows[:256]
    )
    if structure_charts:
        header_html = structure_view["header"]
        summary_html = structure_view["summary"]
        evaluation_html = structure_view["evaluation"]
        cautions_html = structure_view["cautions"]
        market_html = ""
        structure_html = structure_view["structure"]
        economic_html = structure_view["economic"]
        break_even_html = '<section><h2>Break-even y decisiones</h2><p class="muted">NOT_ASSESSED · no hay evaluación económica en este diagnóstico.</p></section>'
        quality_html = structure_view["quality"]
        regimes_html = '<section><h2>Regímenes y sesiones</h2><p class="muted">NOT_ASSESSED · no hay operaciones económicas para segmentar.</p></section>'
    else:
        header_html = '<header><h1>MTF Lab — evidencia histórica</h1><p class="muted">Proyección local de sólo lectura; no autoriza cuentas, órdenes ni promoción.</p></header>'
        summary_html = (
            '<section><h2>Resumen</h2><div class="grid">'
            f"{_metric_card('Resultados', counts.get('results'))}{_metric_card('Operaciones', counts.get('trades'))}"
            f"{_metric_card('Cerradas', counts.get('closed_trades'))}{_metric_card('Neto conocido', counts.get('known_net_trades'))}"
            f"{_metric_card('Señales', counts.get('signals'))}</div></section>"
        )
        evaluation_html = (
            "<section><h2>Variantes y embudo</h2><table><thead><tr><th>Variante</th><th>Resultados</th><th>Cerradas</th><th>Neto</th><th>Estado</th></tr></thead><tbody>"
            + (variant_table or '<tr><td colspan="5">UNKNOWN / INSUFFICIENT</td></tr>')
            + f'</tbody></table><p class="muted">Funnel: {html.escape(str(as_mapping(report.get("funnel")).get("deduplicated_row_count", 0)))} filas únicas; clave de deduplicación conservada en JSON.</p></section>'
        )
        market_html = (
            "<section><h2>Mercado: spread y spread/ATR</h2><pre>"
            + html.escape(
                json.dumps(
                    {
                        "overall": spread_overall,
                        "by_timeframe": market.get("by_timeframe", []),
                        "by_hour_utc": market.get("by_hour_utc", []),
                        "by_session": market.get("by_session", []),
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            + "</pre></section>"
        )
        structure_html = (
            "<section><h2>Estructura descriptiva del mercado</h2><pre>"
            + html.escape(
                json.dumps(
                    report.get("market_structure_projection", report.get("market_structure", {})),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            + "</pre></section>"
        )
        economic_html = (
            '<section><h2>Equity realizada/MTM · cost bridge · drawdown/recovery · MFE/MAE</h2><div class="grid charts">'
            + economic_chart_html
            + "</div><pre>"
            + html.escape(
                json.dumps(
                    {
                        "cost_bridge": report.get("cost_bridge", []),
                        "drawdown": report.get("drawdown", []),
                        "mfe_mae": report.get("mfe_mae", []),
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            + "</pre></section>"
        )
        break_even_html = (
            "<section><h2>Break-even y decisiones</h2><pre>"
            + html.escape(
                json.dumps(
                    {
                        "break_even_fees": report.get("break_even_fees", []),
                        "confidence_intervals": report.get("confidence_intervals", []),
                        "decision_summary": report.get("decision_summary", {}),
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            + "</pre></section>"
        )
        quality_html = (
            "<section><h2>Calidad y casos insuficientes</h2><pre>"
            + html.escape(
                json.dumps(
                    {
                        "quality": report.get("quality", []),
                        "insufficient_cases": report.get("insufficient_cases", []),
                        "evidence_gates": report.get("evidence_gates", {}),
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            + "</pre></section>"
        )
        cautions_html = (
            "<section><h2>Cautelas y estado de revalidación</h2>"
            + _structure_bullets(report.get("cautions"), empty="Sin cautelas suministradas.")
            + "</section>"
        )
        regimes_html = (
            "<section><h2>Regímenes y sesiones</h2><pre>"
            + html.escape(
                json.dumps(
                    {
                        "regimes_and_sessions": report.get("regimes_and_sessions", {}),
                        "sessions": report.get("sessions", []),
                        "episodes": report.get("episodes", []),
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            + "</pre></section>"
        )
    return f"""<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MTF Lab — informe de evidencia</title>
<style>
:root{{color-scheme:light dark}}body{{font-family:system-ui,sans-serif;max-width:1240px;margin:1rem auto;padding:0 1rem;line-height:1.4}}h1,h2{{margin:.4rem 0}}.muted{{opacity:.75}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.65rem}}.charts{{grid-template-columns:repeat(auto-fit,minmax(420px,1fr))}}.card{{border:1px solid #aab4c0;border-radius:.5rem;padding:.65rem;min-width:0;overflow-wrap:anywhere;background:color-mix(in srgb,Canvas 92%,#3b82f6 8%)}}.label{{display:block;font-size:.8rem;opacity:.75}}strong{{font-size:1.15rem}}.pill{{display:inline-block;border:1px solid #64748b;border-radius:1rem;padding:.15rem .5rem;margin:.15rem;font-size:.82rem}}.chart{{border:1px solid #aab4c0;margin:.35rem 0;overflow:auto}}svg{{width:100%;min-height:220px}}svg text{{font-size:12px}}svg text.footer{{font-size:10px}}table{{border-collapse:collapse;width:100%;margin:.5rem 0}}th,td{{border:1px solid #aab4c0;padding:.35rem;text-align:left;vertical-align:top}}th{{background:#64748b22}}.table-scroll{{overflow:auto;max-width:100%}}pre{{white-space:pre-wrap;overflow:auto;max-height:22rem;background:#64748b18;padding:.65rem}}details{{margin:.7rem 0}}section{{margin-top:1.2rem}}
</style></head><body>
{header_html}
<section><h2>Procedencia</h2><div>{label_html}</div><p class="muted">Las etiquetas se muestran sólo con evidencia explícita; UNKNOWN no se convierte en cero.</p></section>
{summary_html}
{cautions_html}
{evaluation_html}
{market_html}
{structure_html}
{economic_html}
{break_even_html}
{quality_html}
{regimes_html}
<section><h2>Metodología y límites</h2><pre>{html.escape(json.dumps(report.get("methodology", {}), ensure_ascii=False, indent=2, sort_keys=True))}</pre><ul>{"".join(f"<li>{html.escape(str(item))}</li>" for item in report.get("limitations", []))}</ul></section>
<script type="application/json" id="report-data">{report_json}</script>
<script>(function(){{"use strict";const node=document.getElementById("report-data");const data=JSON.parse(node.textContent||"{{}}");document.documentElement.dataset.reportSchema=String(data.schema||"UNKNOWN");}})();</script>
</body></html>
"""


def _ensure_output_dir(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    if any(component in {".", ".."} for component in path.parts):
        raise ValueError("output_dir debe usar una ruta sin componentes relativos")
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            parent_info = os.lstat(current)
        except FileNotFoundError:
            break
        if stat.S_ISLNK(parent_info.st_mode):
            raise ValueError("output_dir no puede atravesar symlinks")
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise ValueError("output_dir debe ser un directorio real")
    if info.st_uid != _CURRENT_UID:
        raise PermissionError("output_dir debe pertenecer al usuario actual")
    return path


def _write_exclusive(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags | nofollow, 0o600)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            raise FileExistsError(f"no se sobrescribe el artefacto existente: {path}") from exc
        raise
    try:
        offset = 0
        while offset < len(content):
            offset += os.write(fd, content[offset:])
        os.fsync(fd)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != _CURRENT_UID:
            raise PermissionError("artefacto de reporte perdió su identidad privada")
    finally:
        os.close(fd)


def render_research_report(evidence: EvidenceReportMapping | Any, output_dir: str | Path) -> ResearchReportPaths:
    """Write an exclusive local JSON+HTML evidence bundle.

    Existing ``report.json``/``report.html`` files are never replaced.  Both
    files are mode ``0600`` and contain only the bounded local projection.
    """

    report = build_research_report(evidence)
    directory = _ensure_output_dir(output_dir)
    json_path = directory / JSON_FILENAME
    html_path = directory / HTML_FILENAME
    json_bytes = _json_text(report).encode("utf-8")
    html_bytes = _html_report(report).encode("utf-8")
    _write_exclusive(json_path, json_bytes)
    try:
        _write_exclusive(html_path, html_bytes)
    except Exception:
        # Remove only the file created by this invocation.  If the caller lost
        # the race, the existing file is preserved and the original exception
        # remains the useful result.
        with contextlib.suppress(OSError):
            json_path.unlink()
        raise
    return ResearchReportPaths(json_path=json_path, html_path=html_path)


# Small compatibility aliases for adapters that use a verb-oriented name.
render_report = render_research_report
write_research_report = render_research_report


__all__ = [
    "EvidenceReportMapping",
    "HTML_FILENAME",
    "JSON_FILENAME",
    "MAX_TRADE_SUMMARIES",
    "MAX_VISUAL_POINTS",
    "REPORT_SCHEMA",
    "ResearchReportPaths",
    "build_research_report",
    "market_structure_svgs",
    "project_market_structure",
    "render_report",
    "render_research_report",
    "write_research_report",
]
