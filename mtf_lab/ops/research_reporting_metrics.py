"""Pure helpers for the local historical-evidence report.

The reporting path deliberately consumes already materialized evidence.  It
does not open a dataset, query SQLite, contact a provider, or run a strategy.
This module keeps the expensive/high-cardinality inputs out of the published
projection: observations are reduced to bounded aggregates and visual series.
Unknown values are represented by ``None`` plus a status/reason; they are never
silently converted to zero.
"""

from __future__ import annotations

import dataclasses
import hashlib
import html
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

MAX_VISUAL_POINTS = 1_000
MAX_QUANTILE_SAMPLE = 2_048
MAX_TRADE_SUMMARIES = 10_000

_SECRET_KEY = (
    "token",
    "secret",
    "password",
    "passphrase",
    "authorization",
    "api_key",
    "apikey",
    "refresh",
    "access",
)
_PRIVATE_KEY = (
    "account_id",
    "accountid",
    "account_key",
    "session_id",
    "email",
    "phone",
    "client_id",
    "path",
    "filename",
    "file_name",
    "executable",
)
_D0 = Decimal("0")
_KNOWN_PROVENANCE = {"REAL_HISTORICAL_QUOTES", "MODEL_FILL", "SERVER_DEMO", "UNKNOWN"}
_UNSAFE_SCALAR = object()


def as_mapping(value: Any) -> Mapping[str, Any]:
    """Best-effort adapter for mapping, dataclass, and evidence DTO objects."""

    if isinstance(value, Mapping):
        return value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        # Keep this adapter shallow.  A report DTO may carry a large quote
        # iterator/list; ``dataclasses.asdict`` would eagerly deep-copy it.
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    for method_name in ("to_mapping", "to_dict", "as_dict"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                result = method()
            except TypeError:
                try:
                    result = method(include_private=False)
                except Exception:  # noqa: BLE001 - a report must remain projection-only
                    continue
            if isinstance(result, Mapping):
                return result
    values = getattr(value, "__dict__", None)
    if isinstance(values, Mapping):
        return {key: item for key, item in values.items() if not str(key).startswith("_")}
    return {}


def is_artifact_descriptor(value: Any) -> bool:
    """Identify a path/count descriptor, never a row of evidence."""

    return bool(
        isinstance(value, Mapping) and value.get("path") is not None and ("count" in value or "row_count" in value)
    )


def rows(value: Any) -> list[Mapping[str, Any]]:
    """Return mapping rows without copying or mutating the source sequence."""

    if isinstance(value, Mapping):
        if is_artifact_descriptor(value):
            return []
        return [value]
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping) or as_mapping(item)]


def _text(value: Any, default: str | None = None) -> str | None:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _number(value: Any) -> Decimal | None:
    return _decimal(value)


def number(value: Any) -> int | float | None:
    """Convert a finite numeric value to a JSON number while retaining zero."""

    parsed = _decimal(value)
    if parsed is None:
        return None
    result = float(parsed)
    if not math.isfinite(result):
        return None
    return int(result) if result.is_integer() else result


def _aware(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError, OverflowError):
            return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z") if value is not None else None


def _first(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _field(row: Mapping[str, Any], parent: Mapping[str, Any] | None, *keys: str) -> Any:
    value = _first(row, *keys)
    return value if value is not None or parent is None else _first(parent, *keys)


def _key_is_private(key: str) -> bool:
    lowered = key.lower().replace("-", "_")
    return any(part in lowered for part in _SECRET_KEY) or any(part in lowered for part in _PRIVATE_KEY)


def _safe_scalar(value: Any) -> Any:
    if isinstance(value, str):
        lowered = value.lower()
        if any(
            marker in lowered
            for marker in ("secret-token", "client_secret=", "access_token=", "refresh_token=", "bearer ")
        ):
            return "[REDACTED]"
        return value
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return _UNSAFE_SCALAR


def safe_value(value: Any, *, key: str = "", max_items: int = 256) -> Any:
    """Redact secrets/identifiers for local report projection and HTML embedding."""

    if _key_is_private(key):
        return "[REDACTED]"
    if isinstance(value, Decimal):
        return number(value)
    if isinstance(value, datetime):
        return iso(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        shallow = {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
        return safe_value(shallow, key=key, max_items=max_items)
    if isinstance(value, Mapping):
        return {
            str(name): safe_value(item, key=str(name), max_items=max_items)
            for name, item in list(value.items())[:max_items]
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [safe_value(item, max_items=max_items) for item in list(value)[:max_items]]
    scalar = _safe_scalar(value)
    return scalar if scalar is not _UNSAFE_SCALAR else "UNKNOWN"


def canonical_digest(value: Any) -> str:
    payload = json.dumps(safe_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class OnlineStats:
    """Bounded exact-prefix statistics for a stream of finite Decimals."""

    __slots__ = ("count", "minimum", "maximum", "total", "sample", "unknown")

    def __init__(self) -> None:
        self.count = 0
        self.minimum: Decimal | None = None
        self.maximum: Decimal | None = None
        self.total = _D0
        self.sample: list[float] = []
        self.unknown = 0

    def add(self, value: Any) -> None:
        parsed = _number(value)
        if parsed is None:
            self.unknown += 1
            return
        self.count += 1
        self.minimum = parsed if self.minimum is None else min(self.minimum, parsed)
        self.maximum = parsed if self.maximum is None else max(self.maximum, parsed)
        self.total += parsed
        if len(self.sample) < MAX_QUANTILE_SAMPLE:
            self.sample.append(float(parsed))
        else:
            # Deterministic bounded reservoir.  It is for display only, not an
            # inferential estimator or a substitute for a full source ledger.
            index = (self.count * 2_654_435_761) % MAX_QUANTILE_SAMPLE
            self.sample[index] = float(parsed)

    def summary(self, *, unknown: int | None = None) -> dict[str, Any]:
        unknown_count = self.unknown if unknown is None else max(self.unknown, unknown)
        ordered = sorted(self.sample)
        return {
            "status": "ASSESSED" if self.count else "NOT_ASSESSED",
            "count": self.count,
            "unknown_count": unknown_count,
            "min": number(self.minimum),
            "max": number(self.maximum),
            "mean": number(self.total / Decimal(self.count)) if self.count else None,
            "p50": statistics.median(ordered) if ordered else None,
            "p95": _quantile(ordered, 0.95),
        }


def _quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    position = max(0.0, min(1.0, probability)) * (len(values) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    fraction = position - lower
    return values[lower] + (values[upper] - values[lower]) * fraction


def bounded_series(
    values: Sequence[Mapping[str, Any]], *, value_key: str, max_points: int = MAX_VISUAL_POINTS
) -> dict[str, Any]:
    """Downsample a visual series only; the source count remains explicit."""

    normalized = [dict(item) for item in values if item.get("at") is not None and item.get(value_key) is not None]
    if len(normalized) <= max_points:
        selected = normalized
    elif max_points <= 1:
        selected = normalized[:1]
    else:
        selected = [normalized[round(i * (len(normalized) - 1) / (max_points - 1))] for i in range(max_points)]
    return {
        "points": selected,
        "source_count": len(normalized),
        "display_count": len(selected),
        "downsampled": len(selected) < len(normalized),
        "basis": "visual_only_even_index_selection",
    }


def _dimension_value(row: Mapping[str, Any], result: Mapping[str, Any], root: Mapping[str, Any], *keys: str) -> str:
    for source in (row, result, root):
        value = _first(source, *keys)
        if value is not None and str(value).strip():
            return str(value).strip()
    return "UNKNOWN"


def dimensions(row: Mapping[str, Any], result: Mapping[str, Any], root: Mapping[str, Any]) -> dict[str, str]:
    payload = as_mapping(_first(row, "payload", "metadata"))
    result_payload = as_mapping(_first(result, "payload", "metadata"))
    merged_row = {**payload, **result, **result_payload, **row}
    timeframes = _first(merged_row, "timeframes", "time_frames")
    if isinstance(timeframes, Sequence) and not isinstance(timeframes, (str, bytes, bytearray)):
        merged_row = {**merged_row, "timeframe": ",".join(str(item) for item in timeframes)}
    if _first(merged_row, "profile", "operation_profile", "horizon_profile") is None:
        holding_profile = _first(merged_row, "holding_profile", "holding_period")
        if holding_profile is not None:
            merged_row = {**merged_row, "profile": holding_profile}
    return {
        "variant": _dimension_value(merged_row, result, root, "variant", "variant_name", "candidate_id", "strategy"),
        "timeframe": _dimension_value(
            merged_row, result, root, "timeframe", "tf", "trigger_timeframe", "resolution", "time_frame"
        ),
        "stage": _dimension_value(merged_row, result, root, "stage", "phase", "evaluation_stage", "kind"),
        "evaluation_id": _dimension_value(
            merged_row, result, root, "evaluation_id", "eval_id", "evaluation", "analysis_id", "trial_id"
        ),
        "profile": _dimension_value(merged_row, result, root, "profile", "operation_profile", "horizon_profile"),
        "partition": _dimension_value(merged_row, result, root, "partition", "split", "dataset_partition"),
    }


_QUOTE_PROVENANCE_KEYS = frozenset({"quote_source", "quotes_source", "quote_provenance", "quote_label"})
_FILL_PROVENANCE_KEYS = frozenset({"fill_source", "fills_source", "fill_provenance", "fill_label", "fill_mode"})
_PROVENANCE_NESTED_KEYS = frozenset(
    {
        "provenance",
        "data",
        "metadata",
        "market_structure",
        "dataset",
        "manifest",
        "quote",
        "quotes",
        "quote_evidence",
        "fill",
        "fills",
        "fill_records",
        "orders_with_fills",
    }
)
_QUOTE_ENUMS = {
    "REAL_HISTORICAL_QUOTES": "REAL_HISTORICAL_QUOTES",
    "REAL_HISTORICAL_QUOTES_NOT_DEMO_FILLS": "REAL_HISTORICAL_QUOTES",
    "SERVER_DEMO": "SERVER_DEMO",
}
_FILL_ENUMS = {"MODEL_FILL": "MODEL_FILL", "SERVER_DEMO": "SERVER_DEMO"}


def _enum_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return value.strip().upper().replace("-", "_").replace(" ", "_") or None


def _provenance_sources(payload: Mapping[str, Any], result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    sources: list[Mapping[str, Any]] = [payload, result]
    for root in (payload, result):
        for key in ("provenance", "data", "metadata", "market_structure", "dataset", "manifest"):
            value = as_mapping(root.get(key))
            if value:
                sources.append(value)
                nested = as_mapping(value.get("provenance"))
                if nested:
                    sources.append(nested)
    return sources


def _record_values(value: Any) -> list[Any]:
    if isinstance(value, Mapping):
        return [value] if value else []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _add_provenance_candidate(
    candidate: Any,
    candidates: list[str],
    allowed: Mapping[str, str],
    evidence: list[str],
    evidence_name: str,
) -> None:
    normalized = _enum_text(candidate)
    if normalized is not None and normalized in allowed:
        candidates.append(normalized)
        evidence.append(evidence_name)


def _collect_label_values(
    value: Any,
    quote_candidates: list[str],
    fill_candidates: list[str],
    evidence: list[str],
    context: str | None,
) -> None:
    items = [value] if isinstance(value, str) else _record_values(value)
    for item in items:
        candidate = _enum_text(item)
        if candidate == "SERVER_DEMO" and context is None:
            continue
        if candidate in _QUOTE_ENUMS and context in {None, "quote"}:
            quote_candidates.append(candidate)
            evidence.append("explicit_quote_label")
        elif candidate in _FILL_ENUMS and context in {None, "fill"}:
            fill_candidates.append(candidate)
            evidence.append("explicit_fill_label")


def _collect_source_label_field(
    lowered: str,
    value: Any,
    quote_candidates: list[str],
    fill_candidates: list[str],
    evidence: list[str],
    context: str | None,
) -> bool:
    if lowered in _QUOTE_PROVENANCE_KEYS:
        _add_provenance_candidate(value, quote_candidates, _QUOTE_ENUMS, evidence, "explicit_quote_source")
        return True
    if lowered in _FILL_PROVENANCE_KEYS:
        _add_provenance_candidate(value, fill_candidates, _FILL_ENUMS, evidence, "explicit_fill_source")
        return True
    if lowered == "source":
        if context == "fill":
            _add_provenance_candidate(value, fill_candidates, _FILL_ENUMS, evidence, "explicit_fill_source_field")
        elif context == "quote":
            _add_provenance_candidate(value, quote_candidates, _QUOTE_ENUMS, evidence, "explicit_quote_source_field")
        else:
            candidate = _enum_text(value)
            if candidate in {"REAL_HISTORICAL_QUOTES", "REAL_HISTORICAL_QUOTES_NOT_DEMO_FILLS"}:
                quote_candidates.append(candidate)
                evidence.append("explicit_quote_source_field")
        return True
    return False


def _collect_record_label_field(
    lowered: str,
    value: Any,
    fill_candidates: list[str],
    evidence: list[str],
) -> bool:
    if lowered in {"model_fill_records", "model_fills"}:
        if _record_values(value):
            fill_candidates.append("MODEL_FILL")
            evidence.append("explicit_model_fill_records")
        return True
    if lowered in {"server_fill_records", "server_fills", "observed_server_fills"}:
        if _record_values(value):
            fill_candidates.append("SERVER_DEMO")
            evidence.append("explicit_server_fill_records")
        return True
    return False


def _collect_label_field(
    lowered: str,
    value: Any,
    quote_candidates: list[str],
    fill_candidates: list[str],
    evidence: list[str],
    context: str | None,
) -> bool:
    if _collect_source_label_field(lowered, value, quote_candidates, fill_candidates, evidence, context):
        return True
    if _collect_record_label_field(lowered, value, fill_candidates, evidence):
        return True
    if lowered in {"label", "labels"}:
        _collect_label_values(value, quote_candidates, fill_candidates, evidence, context)
        return True
    return False


def _nested_provenance_context(lowered: str, context: str | None) -> str | None:
    if lowered in {"quote", "quotes", "quote_evidence"}:
        return "quote"
    if lowered in {"fill", "fills", "fill_records", "orders_with_fills"}:
        return "fill"
    if lowered in {"market_structure", "dataset", "manifest"}:
        return "quote"
    return context


def _collect_provenance_labels(
    source: Mapping[str, Any],
    quote_candidates: list[str],
    fill_candidates: list[str],
    evidence: list[str],
    *,
    context: str | None = None,
) -> None:
    for key, value in source.items():
        lowered = str(key).strip().lower().replace("-", "_")
        if _collect_label_field(lowered, value, quote_candidates, fill_candidates, evidence, context):
            continue
        if lowered not in _PROVENANCE_NESTED_KEYS:
            continue
        nested_context = _nested_provenance_context(lowered, context)
        nested = as_mapping(value)
        if nested:
            _collect_provenance_labels(
                nested,
                quote_candidates,
                fill_candidates,
                evidence,
                context=nested_context,
            )
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) and nested_context:
            for item in value:
                nested_item = as_mapping(item)
                if nested_item:
                    _collect_provenance_labels(
                        nested_item,
                        quote_candidates,
                        fill_candidates,
                        evidence,
                        context=nested_context,
                    )


def _resolve_provenance_dimension(
    candidates: Sequence[str], aliases: Mapping[str, str], evidence: list[str], dimension: str
) -> str:
    resolved = {aliases[item] for item in candidates if item in aliases}
    unknown = "UNKNOWN" in candidates
    if len(resolved) == 1 and not unknown:
        return next(iter(resolved))
    if len(resolved) > 1 or (resolved and unknown):
        evidence.append(f"conflicting_{dimension}_provenance")
    return "UNKNOWN"


def provenance(payload: Mapping[str, Any], result: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Resolve producer-declared quote/fill enums without cross-dimension inference."""

    quote_candidates: list[str] = []
    fill_candidates: list[str] = []
    evidence: list[str] = []
    sources = _provenance_sources(payload, result or {})
    for source in sources:
        _collect_provenance_labels(source, quote_candidates, fill_candidates, evidence)
    synthetic = any(
        source.get("synthetic") is True or source.get("fixture") is True or source.get("fixture_flag") is True
        for source in sources
    )
    network = any(source.get("network_performed") is True for source in sources)
    quote = _resolve_provenance_dimension(quote_candidates, _QUOTE_ENUMS, evidence, "quote")
    fill = _resolve_provenance_dimension(fill_candidates, _FILL_ENUMS, evidence, "fill")
    if synthetic and quote == "REAL_HISTORICAL_QUOTES":
        quote = "UNKNOWN"
        evidence.append("synthetic_conflicts_with_real_quote_label")
    labels = sorted({item for item in (quote, fill) if item != "UNKNOWN"}) or ["UNKNOWN"]
    return {
        "quote_source": quote,
        "fill_source": fill,
        "labels": labels,
        "evidence": sorted(set(evidence)) or ["no_explicit_provenance"],
        "status": "ASSESSED" if labels != ["UNKNOWN"] else "UNKNOWN",
        "synthetic": synthetic,
        "network_observed": network,
    }


def _nested_quote(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return as_mapping(_first(row, "quote", "bbo", "market_quote"))


def iter_observations(root: Mapping[str, Any], result: Mapping[str, Any] | None = None) -> Iterable[Mapping[str, Any]]:
    """Yield quote-like observations from common evidence DTO spellings."""

    result = result or {}
    seen: set[int] = set()
    for source in (
        root,
        result,
        as_mapping(_first(root, "dataset", "market_data")),
        as_mapping(_first(result, "dataset", "market_data")),
    ):
        for key in ("quotes", "ticks", "observations", "market_observations", "prices", "events"):
            raw = source.get(key)
            if id(raw) in seen:
                continue
            seen.add(id(raw))
            for item in rows(raw):
                row = as_mapping(item)
                if row:
                    nested = _nested_quote(row)
                    yield {**nested, **row} if nested else row


def _session_key(row: Mapping[str, Any], at: datetime | None) -> str:
    value = _first(row, "session", "market_session", "session_name")
    if value is not None and str(value).strip():
        return str(value).strip()
    return f"UTC_{at.hour:02d}" if at is not None else "UNKNOWN"


class _SpreadAccumulator:
    __slots__ = ("spread", "ratio", "unknown_spread", "unknown_ratio")

    def __init__(self) -> None:
        self.spread = OnlineStats()
        self.ratio = OnlineStats()
        self.unknown_spread = 0
        self.unknown_ratio = 0

    def add(self, row: Mapping[str, Any]) -> None:
        bid = _number(_first(row, "bid", "best_bid"))
        ask = _number(_first(row, "ask", "best_ask"))
        if bid is None or ask is None or ask < bid:
            self.unknown_spread += 1
            return
        spread = ask - bid
        self.spread.add(spread)
        atr = _number(_first(row, "atr", "atr_value", "atr_points"))
        if atr is None:
            atr = _number(as_mapping(_first(row, "indicator_values", "indicators")).get("atr"))
        if atr is None or atr <= _D0:
            self.unknown_ratio += 1
        else:
            self.ratio.add(spread / atr)

    def to_dict(self) -> dict[str, Any]:
        spread = self.spread.summary(unknown=self.unknown_spread)
        ratio = self.ratio.summary(unknown=self.unknown_ratio)
        ratio["reason"] = None if self.ratio.count else "atr_missing_or_nonpositive"
        return {
            "count": spread["count"],
            "unknown_count": spread["unknown_count"],
            "spread": spread,
            "spread_to_atr": ratio,
        }


def spread_report(payload: Mapping[str, Any], results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    groups: dict[str, dict[str, _SpreadAccumulator]] = {
        "overall": {"ALL": _SpreadAccumulator()},
        "timeframe": {},
        "hour_utc": {},
        "session": {},
    }
    observed = 0
    seen_observation_ids: set[int] = set()
    seen_observation_keys: set[str] = set()
    visual_prices: list[dict[str, Any]] = []
    visual_price_count = 0
    for result in (None, *results):
        root = payload if result is None else result
        result_mapping = {} if result is None else result
        dims = dimensions({}, result_mapping, payload)
        for row in iter_observations(root, result_mapping):
            identity = _text(_first(row, "quote_id", "event_id", "tick_id", "observation_id"))
            if id(row) in seen_observation_ids or (identity is not None and identity in seen_observation_keys):
                continue
            seen_observation_ids.add(id(row))
            if identity is not None:
                seen_observation_keys.add(identity)
            observed += 1
            at = _aware(_first(row, "available_at", "available_ts", "timestamp", "event_ts", "ts", "at"))
            bid = _number(_first(row, "bid", "best_bid"))
            ask = _number(_first(row, "ask", "best_ask"))
            if at is not None and bid is not None and ask is not None and ask >= bid:
                visual_price_count += 1
                midpoint = number((bid + ask) / Decimal("2"))
                visual_row = {"at": iso(at), "value": midpoint, "bid": number(bid), "ask": number(ask)}
                if len(visual_prices) < MAX_VISUAL_POINTS * 2:
                    visual_prices.append(visual_row)
                else:
                    index = (visual_price_count * 2_654_435_761) % (MAX_VISUAL_POINTS * 2)
                    visual_prices[index] = visual_row
            keys = {
                "overall": "ALL",
                "timeframe": _text(_first(row, "timeframe", "tf", "resolution")) or dims["timeframe"],
                "hour_utc": f"{at.hour:02d}" if at is not None else "UNKNOWN",
                "session": _session_key(row, at),
            }
            for kind, key in keys.items():
                bucket = groups[kind].setdefault(key, _SpreadAccumulator())
                bucket.add(row)

    def render(source: dict[str, _SpreadAccumulator]) -> list[dict[str, Any]]:
        return [{"group": key, **source[key].to_dict()} for key in sorted(source)]

    overall = groups["overall"]["ALL"].to_dict()
    visual_prices.sort(key=lambda item: str(item.get("at", "")))
    price_series = bounded_series(visual_prices, value_key="value", max_points=MAX_VISUAL_POINTS)
    price_series["source_count"] = visual_price_count
    price_series["visual_sample_bound"] = MAX_VISUAL_POINTS * 2
    price_series["basis"] = "visual_only_midpoint_from_observed_bid_ask"
    return {
        "status": "ASSESSED" if overall["count"] else "NOT_ASSESSED",
        "observations": observed,
        "unknown_spread_count": overall["unknown_count"],
        "overall": overall,
        "price_series": price_series,
        "by_timeframe": render(groups["timeframe"]),
        "by_hour_utc": render(groups["hour_utc"]),
        "by_session": render(groups["session"]),
        "basis": "observed_bid_ask_and_explicit_atr_only",
        "unknown_values_are_not_zero": True,
    }


def ledger_rows(result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = result.get("ledger")
    if raw is None:
        raw = result.get("trades")
    parsed = [as_mapping(item) for item in rows(raw) if as_mapping(item)]
    variant = _text(_first(result, "variant", "candidate_id"))
    if variant:
        labeled = [item for item in parsed if _text(_first(item, "variant", "candidate_id")) is not None]
        if labeled:
            parsed = [item for item in labeled if _text(_first(item, "variant", "candidate_id")) == variant]
    return parsed


def _trade_state(row: Mapping[str, Any]) -> str:
    return str(row.get("state", row.get("status", "UNKNOWN"))).strip().upper()


def closed_trade(row: Mapping[str, Any]) -> bool:
    state = _trade_state(row)
    if state in {"REJECTED", "PENDING", "CANCELLED", "UNKNOWN", "OPEN"}:
        return False
    if state in {"CLOSED", "CLOSE_OBSERVED", "SETTLED", "FILLED_CLOSED"}:
        return True
    return (
        _number(_first(row, "net_pnl", "net", "pnl_net")) is not None
        and _aware(_first(row, "close_available_at", "close_at", "exit_at", "closed_at")) is not None
    )


def _trade_amount(row: Mapping[str, Any], *keys: str) -> Decimal | None:
    value = _first(row, *keys)
    if value is None:
        economic = as_mapping(row.get("economic_result"))
        value = _first(economic, *keys)
    return _number(value)


def trade_amounts(row: Mapping[str, Any]) -> dict[str, Decimal | None]:
    gross = _trade_amount(
        row,
        "gross_pnl_account",
        "gross_pnl",
        "gross",
        "gross_pnl_quote",
        "reference_gross_pnl_account",
        "reference_gross_pnl_quote",
    )
    cost = _trade_amount(row, "costs_account", "total_cost_account", "costs", "cost_total", "costs_quote")
    if row.get("costs_known") is False:
        cost = None
    if cost is None and row.get("costs_known") is True:
        component_values = [
            _trade_amount(row, "spread_cost_account", "spread_account", "spread_cost"),
            _trade_amount(row, "slippage_account", "slippage_cost_account", "slippage"),
            _trade_amount(row, "commission_account", "commission_cost_account", "commission"),
            _trade_amount(row, "financing_account", "financing_cost_account", "swap", "financing"),
        ]
        if all(value is not None for value in component_values):
            cost = sum((value for value in component_values if value is not None), _D0)
    economic = as_mapping(row.get("economic_result"))
    if str(economic.get("state", "KNOWN")).upper() in {"UNKNOWN", "INDETERMINATE", "NOT_ASSESSED"}:
        cost = None
    net = _trade_amount(row, "net_pnl", "net", "pnl_net", "net_result")
    if net is None and gross is not None and cost is not None:
        net = gross - cost
    if gross is None and net is not None and cost is not None:
        gross = net + cost
    return {"gross": gross, "cost": cost, "net": net}


def _component(row: Mapping[str, Any], *keys: str) -> Decimal | None:
    value = _trade_amount(row, *keys)
    return value


def _invalid_cost_bridge(
    reason: str,
    *,
    source_count: int | None = None,
    expected_source_count: int | None = None,
) -> dict[str, Any]:
    """Return a fail-closed bridge when a complete ledger cannot be read."""

    unknown_components = {
        name: {"known_subtotal": None, "known_count": None, "unknown_count": None}
        for name in ("spread", "slippage", "commission", "financing", "other")
    }
    return {
        "status": "INSUFFICIENT",
        "reason": reason,
        "closed_trade_count": None,
        "gross": None,
        "costs": None,
        "net": None,
        "gross_known_subtotal": None,
        "costs_known_subtotal": None,
        "net_known_subtotal": None,
        "known_gross_count": None,
        "known_cost_count": None,
        "known_net_count": None,
        "unknown_gross_count": None,
        "unknown_cost_count": None,
        "unknown_net_count": None,
        "components": unknown_components,
        "accounting_equation": "net = gross - total_costs_once",
        "spread_and_slippage_not_subtracted_twice": True,
        "gross_basis": "executed_bid_ask_when_fill_sides_are_present_else_recorded_gross",
        "conditional_on_costs": True,
        "source_complete": False,
        "source_count": source_count,
        "expected_source_count": expected_source_count,
    }


def _accumulate_cost_row(
    row: Mapping[str, Any],
    *,
    totals: dict[str, Decimal],
    known_counts: dict[str, int],
    components: Mapping[str, OnlineStats],
) -> None:
    amounts = trade_amounts(row)
    for name, value in amounts.items():
        if value is not None:
            totals[name] += value
            known_counts[name] += 1
    component_values = {
        "spread": _component(row, "spread_cost_account", "spread_account", "spread_cost"),
        "slippage": _component(row, "slippage_account", "slippage_cost_account", "slippage"),
        "commission": _component(row, "commission_account", "commission_cost_account", "commission"),
        "financing": _component(row, "financing_account", "financing_cost_account", "swap", "financing"),
    }
    total_cost = amounts["cost"]
    explicit = sum((item for item in component_values.values() if item is not None), _D0)
    if total_cost is not None:
        component_values["other"] = (
            total_cost - explicit if any(item is not None for item in component_values.values()) else None
        )
    for name, value in component_values.items():
        components[name].add(value)


def _cost_bridge_from_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    declared_costs: Mapping[str, Any] | None = None,
    expected_source_count: int | None = None,
    source_error: str | None = None,
) -> dict[str, Any]:
    """Aggregate a complete or paged ledger without retaining all rows."""

    if source_error is not None:
        return _invalid_cost_bridge(
            source_error,
            expected_source_count=expected_source_count,
        )
    totals = {name: _D0 for name in ("gross", "cost", "net")}
    known_counts = {name: 0 for name in totals}
    components: dict[str, OnlineStats] = {
        name: OnlineStats() for name in ("spread", "slippage", "commission", "financing", "other")
    }
    source_count = 0
    count = 0
    for row in rows:
        source_count += 1
        if not closed_trade(row):
            continue
        count += 1
        _accumulate_cost_row(row, totals=totals, known_counts=known_counts, components=components)
    declared_costs = declared_costs or {}
    declared_unknown = _number(declared_costs.get("unknown_count"))
    declared_unknown_count = int(declared_unknown) if declared_unknown is not None and declared_unknown >= 0 else 0
    if str(declared_costs.get("state", "KNOWN")).upper() not in {"KNOWN", ""}:
        declared_unknown_count = max(1, declared_unknown_count)
    unknown_cost_count = max(count - known_counts["cost"], declared_unknown_count)
    source_complete = expected_source_count is None or source_count == expected_source_count
    complete = (
        source_complete
        and count > 0
        and all(known_counts[name] == count for name in totals)
        and unknown_cost_count == 0
    )
    status = "ASSESSED" if complete else ("INSUFFICIENT" if count else "NOT_ASSESSED")
    if not source_complete:
        status = "INSUFFICIENT"
    reason = None
    if not source_complete:
        reason = "ledger_row_count_mismatch"
    elif not complete:
        reason = "closed_trade_gross_net_or_cost_unknown" if count else "no_closed_trades"
    return {
        "status": status,
        "reason": reason,
        "closed_trade_count": count,
        "gross": number(totals["gross"]) if known_counts["gross"] == count and count else None,
        "costs": number(totals["cost"]) if known_counts["cost"] == count and count else None,
        "net": number(totals["net"]) if known_counts["net"] == count and complete else None,
        "gross_known_subtotal": number(totals["gross"]) if known_counts["gross"] else None,
        "costs_known_subtotal": number(totals["cost"]) if known_counts["cost"] else None,
        "net_known_subtotal": number(totals["net"]) if known_counts["net"] else None,
        "known_gross_count": known_counts["gross"],
        "known_cost_count": known_counts["cost"],
        "known_net_count": known_counts["net"],
        "unknown_gross_count": count - known_counts["gross"],
        "unknown_cost_count": unknown_cost_count,
        "unknown_net_count": count - known_counts["net"],
        "components": {
            name: {
                "known_subtotal": number(stat.total) if stat.count else None,
                "known_count": stat.count,
                "unknown_count": stat.unknown,
            }
            for name, stat in components.items()
        },
        "accounting_equation": "net = gross - total_costs_once",
        "spread_and_slippage_not_subtracted_twice": True,
        "gross_basis": "executed_bid_ask_when_fill_sides_are_present_else_recorded_gross",
        "conditional_on_costs": not complete,
        "source_complete": source_complete,
        "source_count": source_count,
        "expected_source_count": expected_source_count,
    }


def cost_bridge_from_rows(
    result: Mapping[str, Any],
    rows: Iterable[Mapping[str, Any]],
    *,
    expected_source_count: int | None = None,
    source_error: str | None = None,
) -> dict[str, Any]:
    """Build a cost bridge from an already verified inline or paged ledger."""

    return _cost_bridge_from_rows(
        rows,
        declared_costs=as_mapping(result.get("costs")),
        expected_source_count=expected_source_count,
        source_error=source_error,
    )


def cost_bridge(result: Mapping[str, Any]) -> dict[str, Any]:
    verified = result.get("_verified_ledger_cost_bridge")
    if isinstance(verified, Mapping):
        return dict(verified)
    retention = as_mapping(result.get("_ledger_retention"))
    if retention and retention.get("complete") is not True:
        return _invalid_cost_bridge(
            "ledger_retention_incomplete",
            source_count=retention.get("display_count") if isinstance(retention.get("display_count"), int) else None,
            expected_source_count=retention.get("row_count") if isinstance(retention.get("row_count"), int) else None,
        )
    return cost_bridge_from_rows(result, ledger_rows(result))


def break_even_metrics(result: Mapping[str, Any], bridge: Mapping[str, Any]) -> dict[str, Any]:
    """Expose fee and binary-contract break-even points conditionally."""

    gross = _number(bridge.get("gross"))
    count = _number(bridge.get("closed_trade_count"))
    fee = gross / count if gross is not None and count is not None and count > _D0 else None
    assumptions = as_mapping(_first(result, "contract", "virtual_contract", "assumptions"))
    if isinstance(assumptions.get("virtual_contract"), Mapping):
        assumptions = as_mapping(assumptions.get("virtual_contract"))
    if not assumptions:
        assumptions = as_mapping(result.get("simulation"))
    payout = _number(_first(assumptions, "payout_net", "payout"))
    loss = _number(_first(assumptions, "loss_amount", "loss"))
    stake = _number(_first(assumptions, "stake", "amount")) or _D0
    costs = _number(_first(assumptions, "costs", "cost_total"))
    probability = None
    if payout is not None and loss is not None and payout + loss > _D0:
        probability = loss / (payout + loss)
        if costs is not None and stake > _D0:
            probability = (loss * stake + costs) / (stake * (payout + loss))
    return {
        "fee_per_closed_trade": number(fee),
        "fee_status": "ASSESSED" if fee is not None else "INSUFFICIENT",
        "break_even_probability": number(probability),
        "probability_status": "ASSESSED" if probability is not None else "NOT_ASSESSED",
        "reason": None if fee is not None or probability is not None else "gross_or_contract_assumptions_missing",
        "conditional_on_known_gross_and_contract_costs": True,
    }


def _artifact_source_reason(result: Mapping[str, Any], key: str) -> str | None:
    raw = (
        result.get("equity_mark_to_market", result.get("equity_marks", result.get("equity")))
        if key == "equity"
        else result.get(key)
    )
    if is_artifact_descriptor(raw):
        return f"{key}_snapshot_not_paged"
    retention = as_mapping(result.get(f"_{key}_retention"))
    if retention and retention.get("complete") is not True:
        return f"{key}_retention_incomplete"
    return None


def _marks(result: Mapping[str, Any]) -> tuple[list[dict[str, Any]], int]:
    raw = result.get("equity_mark_to_market", result.get("equity_marks", result.get("equity")))
    if is_artifact_descriptor(raw) or (
        as_mapping(result.get("_equity_retention"))
        and as_mapping(result.get("_equity_retention")).get("complete") is not True
    ):
        return [], 1
    known: list[dict[str, Any]] = []
    unknown = 0
    for ordinal, item in enumerate(rows(raw)):
        row = as_mapping(item)
        at = _aware(_first(row, "available_at", "available_ts", "timestamp", "at", "ts"))
        equity = _number(_first(row, "equity", "mark_to_market", "mtm_equity"))
        if (
            at is None
            or equity is None
            or str(row.get("state", "KNOWN")).upper() in {"UNKNOWN", "INDETERMINATE", "NOT_ASSESSED"}
        ):
            unknown += 1
            continue
        known.append(
            {
                "at": iso(at),
                "_at": at,
                "equity": number(equity),
                "realized": number(_first(row, "realized", "realized_equity", "equity_realized")),
                "unrealized": number(_first(row, "unrealized", "unrealized_pnl", "equity_unrealized")),
                "exposure": number(_first(row, "exposure", "notional_exposure", "gross_exposure")),
                "quantity": number(_first(row, "quantity", "exposure_quantity", "filled_quantity")),
                "currency": _text(_first(row, "currency", "account_currency", "exposure_currency")),
                "ordinal": ordinal,
            }
        )
    known.sort(key=lambda item: (item["_at"], item["ordinal"]))
    summary = as_mapping(result.get("equity_summary"))
    reported = _number(summary.get("unknown_mark_count"))
    if reported is not None:
        unknown = max(unknown, int(reported))
    return known, unknown


def drawdown_equity(result: Mapping[str, Any], *, max_points: int = MAX_VISUAL_POINTS) -> dict[str, Any]:
    source_reason = _artifact_source_reason(result, "equity")
    if source_reason is not None:
        return {
            "status": "NOT_ASSESSED",
            "reason": source_reason,
            "unknown_mark_count": 1,
            "max_drawdown": None,
            "max_drawdown_peak_at": None,
            "max_drawdown_trough_at": None,
            "recovery_duration_seconds": None,
            "unrecovered_duration_seconds": None,
            "unrecovered_state": "NOT_ASSESSED",
            "mark_to_market": bounded_series([], value_key="equity", max_points=max_points),
            "realized": bounded_series([], value_key="realized", max_points=max_points),
            "unrealized": bounded_series([], value_key="unrealized", max_points=max_points),
        }
    marks, unknown = _marks(result)
    series = [{key: value for key, value in item.items() if not key.startswith("_")} for item in marks]
    mtm = bounded_series(series, value_key="equity", max_points=max_points)
    if not marks:
        return {
            "status": "NOT_ASSESSED",
            "reason": "equity_marks_missing_or_unusable",
            "unknown_mark_count": unknown,
            "max_drawdown": None,
            "max_drawdown_peak_at": None,
            "max_drawdown_trough_at": None,
            "recovery_duration_seconds": None,
            "unrecovered_duration_seconds": None,
            "unrecovered_state": "NOT_ASSESSED",
            "mark_to_market": mtm,
            "realized": bounded_series(series, value_key="realized", max_points=max_points),
            "unrealized": bounded_series(series, value_key="unrealized", max_points=max_points),
        }
    peak = _decimal(marks[0]["equity"])
    peak_at = marks[0]["_at"]
    maximum = _D0
    maximum_peak: datetime | None = None
    maximum_trough: datetime | None = None
    underwater_since: datetime | None = None
    recoveries: list[float] = []
    for mark in marks:
        equity = _decimal(mark["equity"])
        assert equity is not None and peak is not None
        if equity < peak:
            underwater_since = underwater_since or peak_at
            drawdown = peak - equity
            if drawdown > maximum:
                maximum = drawdown
                maximum_peak = peak_at
                maximum_trough = mark["_at"]
        else:
            if underwater_since is not None:
                recoveries.append((mark["_at"] - underwater_since).total_seconds())
                underwater_since = None
            if equity > peak:
                peak, peak_at = equity, mark["_at"]
    complete = unknown == 0
    return {
        "status": "ASSESSED" if complete else "INSUFFICIENT",
        "reason": None if complete else "unknown_equity_marks",
        "unknown_mark_count": unknown,
        "max_drawdown": number(maximum) if complete else None,
        "known_max_drawdown": number(maximum),
        "max_drawdown_peak_at": iso(maximum_peak),
        "max_drawdown_trough_at": iso(maximum_trough),
        "recovery_duration_seconds": max(recoveries) if recoveries and complete else None,
        "unrecovered_duration_seconds": (marks[-1]["_at"] - underwater_since).total_seconds()
        if underwater_since
        else 0,
        "unrecovered_state": "UNRECOVERED" if underwater_since else "RECOVERED",
        "mark_to_market": mtm,
        "realized": bounded_series(series, value_key="realized", max_points=max_points),
        "unrealized": bounded_series(series, value_key="unrealized", max_points=max_points),
        "basis": "ordered_mark_to_market_equity",
    }


def exposure_metrics(result: Mapping[str, Any], marks: Mapping[str, Any] | None = None) -> dict[str, Any]:
    source_reason = _artifact_source_reason(result, "equity")
    if source_reason is not None:
        return {
            "status": "NOT_ASSESSED",
            "reason": source_reason,
            "peak": None,
            "unknown_mark_count": 1,
        }
    parsed_marks, unknown = _marks(result)
    observations = [item for item in parsed_marks if item.get("exposure") is not None]
    if not observations:
        return {
            "status": "NOT_ASSESSED",
            "reason": "exposure_marks_missing",
            "peak": None,
            "unknown_mark_count": unknown,
        }
    peak = max(observations, key=lambda item: float(item["exposure"] or 0))
    duration = 0.0
    notional_time = Decimal("0")
    for current, following in zip(parsed_marks, parsed_marks[1:], strict=False):
        exposure = _number(current.get("exposure"))
        seconds = (following["_at"] - current["_at"]).total_seconds()
        if exposure is None or exposure <= _D0 or seconds <= 0:
            continue
        duration += seconds
        notional_time += exposure * Decimal(str(seconds))
    complete = unknown == 0
    return {
        "status": "ASSESSED" if complete else "INSUFFICIENT",
        "reason": None if complete else "unknown_equity_marks",
        "peak": peak["exposure"] if complete else None,
        "known_peak": peak["exposure"],
        "peak_at": peak["at"],
        "duration_seconds": duration if complete else None,
        "known_duration_seconds": duration,
        "notional_time": number(notional_time) if complete else None,
        "known_notional_time": number(notional_time),
        "currency": peak.get("currency"),
        "basis": "known_mark_to_market_exposure",
    }


def _quote_rows_for_trade(
    root: Mapping[str, Any], result: Mapping[str, Any], trade: Mapping[str, Any]
) -> Iterable[Mapping[str, Any]]:
    trade_id = _text(_first(trade, "trade_id", "id", "identity"))
    entry = _aware(_first(trade, "entry_available_at", "entry_at", "entry_ts"))
    close = _aware(_first(trade, "close_available_at", "close_at", "exit_at", "close_ts"))
    for row in iter_observations(root, result):
        row_trade = _text(_first(row, "trade_id", "position_id", "id"))
        at = _aware(_first(row, "available_at", "available_ts", "timestamp", "event_ts", "ts", "at"))
        if trade_id and row_trade and row_trade != trade_id:
            continue
        if at is None or (entry is not None and at < entry) or (close is not None and at > close):
            continue
        yield row


def mfe_mae(root: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    trades = [row for row in ledger_rows(result) if closed_trade(row)]
    records: list[dict[str, Any]] = []
    for trade in trades[:MAX_TRADE_SUMMARIES]:
        direction = str(_first(trade, "direction", "side", "order_side") or "UNKNOWN").strip().upper()
        long_side = direction in {"LONG", "BUY", "UP"}
        short_side = direction in {"SHORT", "SELL", "DOWN"}
        entry = _number(_first(trade, "entry_price", "entry_fill_price"))
        mfe = _trade_amount(trade, "mfe_quote", "mfe_pnl", "mfe")
        mae = _trade_amount(trade, "mae_quote", "mae_pnl", "mae")
        mfe_price = _number(_first(trade, "mfe_price", "max_favorable_price"))
        mae_price = _number(_first(trade, "mae_price", "max_adverse_price"))
        if entry is not None and long_side:
            mfe = mfe if mfe is not None else (mfe_price - entry if mfe_price is not None else None)
            mae = mae if mae is not None else (mae_price - entry if mae_price is not None else None)
        elif entry is not None and short_side:
            mfe = mfe if mfe is not None else (entry - mfe_price if mfe_price is not None else None)
            mae = mae if mae is not None else (entry - mae_price if mae_price is not None else None)
        for quote in _quote_rows_for_trade(root, result, trade):
            liquidation = _number(quote.get("bid" if long_side else "ask" if short_side else "close"))
            if entry is None or liquidation is None:
                continue
            move = liquidation - entry if long_side else entry - liquidation if short_side else None
            if move is None:
                continue
            mfe = move if mfe is None else max(mfe, move)
            mae = move if mae is None else min(mae, move)
        records.append(
            {
                "trade_id": _text(_first(trade, "trade_id", "id", "identity")) or "UNKNOWN",
                "direction": direction,
                "liquidation_side": "bid" if long_side else "ask" if short_side else "UNKNOWN",
                "mfe": number(mfe),
                "mae": number(mae),
                "status": "ASSESSED"
                if (long_side or short_side) and mfe is not None and mae is not None
                else "NOT_ASSESSED",
                "reason": None
                if (long_side or short_side) and mfe is not None and mae is not None
                else "liquidation_side_or_intratrade_marks_missing",
            }
        )
    known_mfe: list[Decimal] = []
    known_mae: list[Decimal] = []
    for item in records:
        mfe_value = _number(item["mfe"])
        mae_value = _number(item["mae"])
        if mfe_value is not None:
            known_mfe.append(mfe_value)
        if mae_value is not None:
            known_mae.append(mae_value)
    complete = bool(records) and all(item.get("status") == "ASSESSED" for item in records)
    return {
        "status": "ASSESSED" if complete else "NOT_ASSESSED",
        "reason": None if complete else "mfe_mae_marks_missing_or_unknown",
        "trade_count": len(trades),
        "displayed_trade_count": len(records),
        "truncated": len(trades) > len(records),
        "mfe_mean": number(sum(known_mfe, _D0) / Decimal(len(known_mfe))) if known_mfe else None,
        "mae_mean": number(sum(known_mae, _D0) / Decimal(len(known_mae))) if known_mae else None,
        "records": records,
        "basis": "liquidation_side_bid_for_long_ask_for_short",
    }


def confidence_interval(values: Sequence[Decimal], *, alpha: float = 0.05) -> dict[str, Any]:
    clean = [float(value) for value in values if value.is_finite()]
    if len(clean) < 2:
        return {
            "status": "INSUFFICIENT",
            "reason": "fewer_than_two_known_observations",
            "n": len(clean),
            "mean": number(sum(values, _D0) / Decimal(len(values))) if values else None,
            "lower": None,
            "upper": None,
            "ci_lower": None,
            "ci_upper": None,
            "interval": "nominal_conditional_on_observed_independent_values",
        }
    mean = statistics.fmean(clean)
    stdev = statistics.stdev(clean)
    margin = 1.96 * stdev / math.sqrt(len(clean))
    return {
        "status": "ASSESSED",
        "reason": None,
        "n": len(clean),
        "mean": mean,
        "lower": mean - margin,
        "upper": mean + margin,
        "ci_lower": mean - margin,
        "ci_upper": mean + margin,
        "alpha": alpha,
        "interval": "nominal_conditional_on_observed_independent_values",
    }


def confidence_for_result(result: Mapping[str, Any]) -> dict[str, Any]:
    values: list[Decimal] = []
    unknown_costs = False
    for row in ledger_rows(result):
        if closed_trade(row):
            amounts = trade_amounts(row)
            if amounts["cost"] is None:
                unknown_costs = True
            value = amounts["net"]
            if value is not None:
                values.append(value)
    supplied = as_mapping(_first(result, "confidence_intervals", "confidence_interval", "ci", "statistics"))
    result_ci = {
        str(key): safe_value(value, key=str(key))
        for key, value in supplied.items()
        if any(token in str(key).lower() for token in ("ci", "interval", "lcb", "confidence"))
    }
    interval = confidence_interval(values)
    if unknown_costs:
        interval = {
            **interval,
            "status": "INSUFFICIENT",
            "reason": "costs_unknown_or_conditional",
            "conditional_costs": True,
        }
    return {
        "net_expectancy": interval,
        "supplied": result_ci,
        "values_are_conditional_on_costs": True,
        "costs_unknown": unknown_costs,
    }


def _funnel_source_rows(
    source: Mapping[str, Any], root: Mapping[str, Any], result: Mapping[str, Any]
) -> Iterable[Mapping[str, Any]]:
    for key in ("funnel", "stages", "pipeline", "evaluations", "evaluation_funnel"):
        raw = source.get(key)
        if is_artifact_descriptor(raw):
            continue
        if isinstance(raw, Mapping):
            for stage, count in raw.items():
                if _number(count) is not None:
                    yield {"stage": stage, "count": count}
                elif isinstance(count, Mapping):
                    yield {"stage": stage, **count}
            continue
        for item in rows(raw):
            row = as_mapping(item)
            if row:
                yield row


def funnel_report(payload: Mapping[str, Any], results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    seen: set[tuple[str, str, str, str, str]] = set()
    raw_count = 0
    funnel_keys = ("funnel", "stages", "pipeline", "evaluations", "evaluation_funnel")
    descriptor_blocked = any(is_artifact_descriptor(payload.get(key)) for key in funnel_keys)
    for result in results:
        if any(
            is_artifact_descriptor(result.get(key))
            or (
                as_mapping(result.get(f"_{key}_retention"))
                and as_mapping(result.get(f"_{key}_retention")).get("complete") is not True
            )
            for key in funnel_keys
        ):
            descriptor_blocked = True
            continue
        for row in _funnel_source_rows(result, payload, result):
            raw_count += 1
            dims = dimensions(row, result, payload)
            identity = _text(
                _first(row, "event_id", "evaluation_event_id", "signal_id", "episode_id", "trade_id", "id")
            )
            if identity is None:
                identity = dims["evaluation_id"] if dims["evaluation_id"] != "UNKNOWN" else canonical_digest(row)
            key = (dims["variant"], dims["timeframe"], dims["stage"], dims["evaluation_id"], identity)
            group_key: tuple[str, str, str, str] = key[:4]
            group = groups.setdefault(
                group_key,
                {"count": 0, "raw_count": 0, "duplicate_count": 0, "identities": set(), "observed_counts": []},
            )
            group["raw_count"] += 1
            if key in seen:
                group["duplicate_count"] += 1
                continue
            seen.add(key)
            group["count"] += 1
            group["identities"].add(identity)
            count = _number(row.get("count"))
            if count is not None:
                group["observed_counts"].append(count)
    output: list[dict[str, Any]] = []
    for group_key in sorted(groups):
        value = groups[group_key]
        output.append(
            {
                "variant": group_key[0],
                "timeframe": group_key[1],
                "stage": group_key[2],
                "evaluation_id": group_key[3],
                "unique_count": value["count"],
                "raw_count": value["raw_count"],
                "count": number(sum(value["observed_counts"], _D0)) if value["observed_counts"] else value["count"],
                "duplicate_count": value["duplicate_count"],
            }
        )
    return {
        "status": "ASSESSED" if output and not descriptor_blocked else "NOT_ASSESSED",
        "reason": "funnel_snapshot_not_paged" if descriptor_blocked else None,
        "rows": output,
        "raw_row_count": raw_count,
        "deduplicated_row_count": len(seen),
        "duplicate_row_count": raw_count - len(seen),
        "dedupe_key": "variant,timeframe,stage,evaluation_id,event_or_signal_or_stable_digest",
        "unknown_dimensions_are_explicit": True,
    }


def _group_net(result_metrics: Sequence[Mapping[str, Any]], key: str) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for item in result_metrics:
        groups[str(item.get("dimensions", {}).get(key, "UNKNOWN"))].append(item)
    output: list[dict[str, Any]] = []
    for name in sorted(groups):
        values = [_number(item.get("net")) for item in groups[name]]
        known = [value for value in values if value is not None]
        output.append(
            {
                key: name,
                "result_count": len(groups[name]),
                "known_result_count": len(known),
                "net_known_subtotal": number(sum(known, _D0)) if known else None,
                "net": number(sum(known, _D0)) if known and len(known) == len(values) else None,
                "status": "ASSESSED" if known and len(known) == len(values) else "INSUFFICIENT",
                "reason": None if known and len(known) == len(values) else "unknown_result_net_or_cost",
            }
        )
    return output


def _declared_signal_count(result: Mapping[str, Any]) -> int:
    signals_raw = result.get("signals")
    if isinstance(signals_raw, (int, float, str)) and _number(signals_raw) is not None:
        return int(_number(signals_raw) or 0)
    if "signals" not in result:
        declared_signals = _number(as_mapping(result.get("metrics")).get("signals"))
        if declared_signals is not None:
            return int(declared_signals)
    return len(rows(signals_raw))


def aggregate_sessions_episodes(result: Mapping[str, Any], bridge: Mapping[str, Any]) -> dict[str, Any]:
    signals_raw = result.get("signals")
    episodes: set[str] = set()
    sessions: set[str] = set()
    signal_count = _declared_signal_count(result)
    for item in rows(signals_raw):
        row = as_mapping(item)
        episode = _text(_first(row, "episode_id", "episode"))
        session = _text(_first(row, "session", "market_session", "session_name"))
        if episode:
            episodes.add(episode)
        if session:
            sessions.add(session)
    for row in ledger_rows(result):
        episode = _text(_first(row, "episode_id", "episode"))
        session = _text(_first(row, "session", "market_session", "session_name"))
        if episode:
            episodes.add(episode)
        if session:
            sessions.add(session)
    declared_sessions = rows(result.get("sessions"))
    for item in declared_sessions:
        row = as_mapping(item)
        session = _text(_first(row, "session", "market_session", "name", "id"))
        if session:
            sessions.add(session)
    return {
        "signals": signal_count,
        "episodes": len(episodes) if episodes else None,
        "episode_ids_known": sorted(episodes)[:MAX_TRADE_SUMMARIES],
        "sessions": len(sessions) if sessions else None,
        "session_names_known": sorted(sessions),
        "exposure": exposure_metrics(result),
        "basis": "explicit_signal_episode_session_ids_only",
    }


def regime_session_report(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str], list[Decimal]] = defaultdict(list)
    group_counts: defaultdict[tuple[str, str], int] = defaultdict(int)
    group_unknown: defaultdict[tuple[str, str], int] = defaultdict(int)
    for result in results:
        for row in ledger_rows(result):
            if not closed_trade(row):
                continue
            regime = _text(_first(row, "regime", "market_regime")) or "UNKNOWN"
            session = _text(_first(row, "session", "market_session", "session_name")) or "UNKNOWN"
            group_key = (regime, session)
            group_counts[group_key] += 1
            net = trade_amounts(row)["net"]
            if net is None:
                group_unknown[group_key] += 1
            else:
                groups[group_key].append(net)
    rows_out = []
    for group_key in sorted(group_counts):
        regime, session = group_key
        values = groups[group_key]
        unknown_count = group_unknown[group_key]
        rows_out.append(
            {
                "regime": regime,
                "session": session,
                "count": group_counts[group_key],
                "known_count": len(values),
                "unknown_count": unknown_count,
                "net": number(sum(values, _D0)) if values and not unknown_count else None,
                "expectancy": number(sum(values, _D0) / Decimal(len(values))) if values and not unknown_count else None,
                "status": "ASSESSED" if values and not unknown_count else "INSUFFICIENT",
            }
        )
    return {
        "status": "ASSESSED" if rows_out else "NOT_ASSESSED",
        "rows": rows_out,
        "unknown_net_count": sum(group_unknown.values()),
        "weekends_fabricated": False,
    }


def render_svg(title: str, series: Mapping[str, Any], *, stroke: str = "#2563eb", value_key: str = "value") -> str:
    points = rows(series.get("points"))
    values = [_number(item.get(value_key)) for item in points]
    known = [float(value) for value in values if value is not None]
    escaped_title = html.escape(title)
    if not known:
        return f'<svg viewBox="0 0 720 220" role="img" aria-label="{escaped_title}"><text x="16" y="34">{escaped_title}</text><text x="16" y="112">UNKNOWN / INSUFFICIENT</text></svg>'
    low, high = min(known), max(known)
    if low == high:
        high = low + 1.0
    path: list[str] = []
    for index, value in enumerate(values):
        if value is None:
            continue
        x = 16.0 + (688.0 * index / max(1, len(values) - 1))
        y = 196.0 - (160.0 * (float(value) - low) / (high - low))
        path.append(("L" if path else "M") + f"{x:.1f} {y:.1f}")
    return (
        f'<svg viewBox="0 0 720 220" role="img" aria-label="{escaped_title}">'
        f'<text x="16" y="18">{escaped_title}</text>'
        f'<path d="{" ".join(path)}" fill="none" stroke="{stroke}" stroke-width="2"/>'
        f'<text x="16" y="216">n={len(points)} · {series.get("basis", "visual")}</text></svg>'
    )


def _distribution_projection(value: Any, *, unit: str | None = None) -> dict[str, Any]:
    raw = as_mapping(value)
    n = _number(raw.get("n"))
    n_value = int(n) if n is not None and n >= _D0 and n == n.to_integral_value() else None
    return {
        "status": "ASSESSED" if n_value is not None and n_value > 0 and raw.get("mean") is not None else "NOT_ASSESSED",
        "n": n_value,
        "mean": number(raw.get("mean")),
        "mean_raw": safe_value(raw.get("mean")) if raw.get("mean") is not None else None,
        "variance_population": number(raw.get("variance_population")),
        "variance_population_raw": safe_value(raw.get("variance_population"))
        if raw.get("variance_population") is not None
        else None,
        "min": number(raw.get("min")),
        "max": number(raw.get("max")),
        "quantiles": safe_value(raw.get("quantiles", {})),
        "unit": unit,
        "quantile_method": safe_value(raw.get("quantile_method"))
        if raw.get("quantile_method") is not None
        else "UNKNOWN",
        "overflow_count": number(raw.get("overflow_count")),
    }


def _hour_distribution_rows(quote_statistics: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = quote_statistics.get("spread_by_hour_utc")
    if not isinstance(raw, Mapping):
        return []
    unit = _text(quote_statistics.get("spread_unit")) or "UNKNOWN"
    output: list[dict[str, Any]] = []

    def sort_key(item: tuple[Any, Any]) -> tuple[int, str]:
        try:
            return int(str(item[0])), str(item[0])
        except ValueError:
            return 24, str(item[0])

    for hour, distribution in sorted(raw.items(), key=sort_key):
        row = _distribution_projection(distribution, unit=unit)
        try:
            row["hour_utc"] = f"{int(str(hour)):02d}"
        except ValueError:
            row["hour_utc"] = str(hour)
        output.append(row)
    return output


def _session_distribution_rows(quote_statistics: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = quote_statistics.get("spread_by_session")
    if not isinstance(raw, Mapping):
        return []
    unit = _text(quote_statistics.get("spread_unit")) or "UNKNOWN"
    output: list[dict[str, Any]] = []
    for session, distribution in sorted(raw.items(), key=lambda item: str(item[0])):
        row = _distribution_projection(distribution, unit=unit)
        row["session"] = str(session)
        output.append(row)
    return output


def _timeframe_distribution_rows(value: Any) -> list[dict[str, Any]]:
    raw = value if isinstance(value, Mapping) else {}
    output: list[dict[str, Any]] = []
    order = {name: index for index, name in enumerate(("M1", "M5", "M15", "H1", "H4", "D1"))}
    for timeframe, frame_value in sorted(raw.items(), key=lambda item: (order.get(str(item[0]), 99), str(item[0]))):
        frame = as_mapping(frame_value)
        unit = _text(frame.get("spread_atr_unit"), "dimensionless") or "dimensionless"
        row = _distribution_projection(frame.get("spread_atr"), unit=unit)
        row.update(
            {
                "timeframe": str(frame.get("timeframe", timeframe)),
                "warmup": _first(frame, "warmup", "warmup_bars", "warmup_records"),
                "warmup_status": "ASSESSED"
                if _first(frame, "warmup", "warmup_bars", "warmup_records") is not None
                else "UNKNOWN",
                "closed_bars": number(frame.get("closed_bars")),
                "coverage_valid_bars": number(frame.get("coverage_valid_bars")),
                "spread_atr_basis": _text(frame.get("spread_atr_basis")),
            }
        )
        output.append(row)
    return output


def _coverage_rows(quote_statistics: Mapping[str, Any]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in rows(quote_statistics.get("days_with_quotes")):
        output.append(
            {
                "date_utc": _text(_first(item, "date_utc", "date")),
                "ticks": number(item.get("ticks")),
                "first_quote": _text(item.get("first_quote")),
                "last_quote": _text(item.get("last_quote")),
                "full_session": _text(item.get("full_session")),
            }
        )
    return output


def _gap_projection(quote_statistics: Mapping[str, Any]) -> dict[str, Any]:
    count = number(quote_statistics.get("gaps_over_declared_bound"))
    largest = [
        {
            "from": _text(_first(item, "from", "gap_start")),
            "to": _text(_first(item, "to", "gap_end")),
            "seconds": number(_first(item, "seconds", "duration_seconds")),
        }
        for item in rows(quote_statistics.get("largest_gaps"))
    ]
    return {
        "status": "ASSESSED" if count is not None else "UNKNOWN",
        "count": count,
        "largest": largest,
        "max_quote_gap_seconds": number(quote_statistics.get("max_quote_gap_seconds")),
        "classification": _text(quote_statistics.get("gap_classification")),
        "covered_seconds": number(quote_statistics.get("time_weighted_covered_seconds")),
    }


def project_market_structure(value: Any) -> dict[str, Any]:
    """Project the real ``market_structure.v1`` DTO without reading raw quotes."""

    raw = as_mapping(value)
    quote_statistics = as_mapping(raw.get("quote_statistics"))
    if not quote_statistics:
        return {"status": "NOT_ASSESSED", "reason": "market_structure_quote_statistics_missing"}
    source = _text(raw.get("source"))
    quote_count = number(quote_statistics.get("quote_count"))
    spread_rows = _hour_distribution_rows(quote_statistics)
    timeframe_rows = _timeframe_distribution_rows(raw.get("timeframes"))
    gap_source = {**quote_statistics, "max_quote_gap_seconds": raw.get("max_quote_gap_seconds")}
    measurement = as_mapping(raw.get("measurement"))
    return {
        "status": "ASSESSED" if raw.get("schema") == "mtf-lab.market-structure.v1" else "UNKNOWN",
        "schema": _text(raw.get("schema")),
        "dataset_id": _text(raw.get("dataset_id")),
        "dataset_content_hash": _text(raw.get("dataset_content_hash")),
        "source": source,
        "provenance": source or "UNKNOWN",
        "availability": _text(raw.get("availability")),
        "quote_count": quote_count,
        "coverage_start": _text(quote_statistics.get("coverage_start")),
        "coverage_end": _text(quote_statistics.get("coverage_end")),
        "spread_unit": _text(quote_statistics.get("spread_unit")),
        "spread_tick_weighted": _distribution_projection(
            quote_statistics.get("spread_tick_weighted"), unit=_text(quote_statistics.get("spread_unit"))
        ),
        "spread_mean_by_hour_utc": [
            {
                "hour_utc": row["hour_utc"],
                "mean": row["mean"],
                "mean_raw": row["mean_raw"],
                "n": row["n"],
                "unit": row["unit"],
                "status": row["status"],
            }
            for row in spread_rows
        ],
        "spread_quantile_intervals_by_hour_utc": [
            {
                "hour_utc": row["hour_utc"],
                "n": row["n"],
                "unit": row["unit"],
                "quantiles": row["quantiles"],
                "status": row["status"],
            }
            for row in spread_rows
        ],
        "spread_by_session": _session_distribution_rows(quote_statistics),
        "session_definition": _text(quote_statistics.get("session_definition")),
        "spread_time_weighted_pips": number(quote_statistics.get("spread_time_weighted_pips")),
        "time_weighted_covered_seconds": number(quote_statistics.get("time_weighted_covered_seconds")),
        "time_weighted_model": _text(quote_statistics.get("time_weighted_model")),
        "coverage_daily": _coverage_rows(quote_statistics),
        "gaps_utc": _gap_projection(gap_source),
        "spread_atr_by_timeframe": timeframe_rows,
        "warmup": {
            "status": "UNKNOWN",
            "reason": "market_structure_dto_does_not_declare_warmup",
            "values": [
                {"timeframe": row["timeframe"], "warmup": row["warmup"], "status": row["warmup_status"]}
                for row in timeframe_rows
            ],
        },
        "economic_conclusion": _text(raw.get("economic_conclusion")),
        "profitability_claim": _text(raw.get("profitability_claim")),
        "provider_comparison": _text(raw.get("provider_comparison")),
        "measurement": safe_value(measurement) if measurement else {"status": "UNKNOWN"},
        "raw_quotes_embedded": False,
        "raw_ticks_embedded": False,
    }


def _svg_footer(*, unit: str, count: Any, source: str, note: str, include_count: bool = True) -> str:
    source_label = {
        "REAL_HISTORICAL_QUOTES_NOT_DEMO_FILLS": "REAL_HISTORICAL_QUOTES",
    }.get(source, source)
    if len(source_label) > 48:
        source_label = f"{source_label[:45]}..."
    unit_label = "pips" if "pip" in unit.lower() else unit
    count_label = f"n={count if count is not None else 'UNKNOWN'} · " if include_count else ""
    return html.escape(f"{unit_label} · {count_label}{note} · provenance={source_label}")


def _svg_unknown(title: str, *, unit: str, count: Any, source: str, reason: str) -> str:
    title_text = html.escape(title)
    return (
        f'<svg viewBox="0 0 720 260" role="img" aria-label="{title_text}">'
        f'<text x="16" y="22">{title_text}</text>'
        f'<text x="16" y="100">UNKNOWN · {html.escape(reason)}</text>'
        f'<text class="footer" x="16" y="248">{_svg_footer(unit=unit, count=count, source=source, note="sin puntos agregados")}</text></svg>'
    )


def _interval_value(value: Any, key: str) -> float | None:
    interval = as_mapping(value)
    parsed = _number(interval.get(key))
    return float(parsed) if parsed is not None else None


def render_market_structure_spread_svg(summary: Mapping[str, Any]) -> str:
    """Render direct hourly means and p50 interval bounds from the DTO."""

    rows_hour = rows(summary.get("spread_mean_by_hour_utc"))
    intervals = {str(item.get("hour_utc")): item for item in rows(summary.get("spread_quantile_intervals_by_hour_utc"))}
    source = _text(summary.get("provenance")) or "UNKNOWN"
    unit = _text(summary.get("spread_unit")) or "UNKNOWN"
    count = summary.get("quote_count")
    values: list[tuple[int, float, float | None, float | None, str, Any]] = []
    for index, item in enumerate(rows_hour):
        mean = _number(item.get("mean"))
        if mean is None:
            continue
        interval = as_mapping(intervals.get(str(item.get("hour_utc")), {}).get("quantiles"))
        p50 = as_mapping(interval.get("p50"))
        values.append(
            (
                index,
                float(mean),
                _interval_value(p50, "lower"),
                _interval_value(p50, "upper_exclusive"),
                str(item.get("hour_utc")),
                item.get("n"),
            )
        )
    scale_values: list[float] = [
        value for _, mean, lower, upper, _, _ in values for value in (mean, lower, upper) if value is not None
    ]
    title = "Spread por hora UTC · media y límites p50"
    if not scale_values:
        return _svg_unknown(
            title, unit=unit, count=count, source=source, reason="no hay medias o intervalos p50 observados"
        )
    data_low: float = min(scale_values)
    data_high: float = max(scale_values)
    low: float = data_low
    high: float = data_high
    if low == high:
        low -= 0.5
        high += 0.5
    marks: list[str] = []
    labels: list[str] = []
    axis_labels: list[str] = []
    for axis_index, axis_item in enumerate(rows_hour):
        axis_x = 28.0 + (664.0 * axis_index / max(1, len(rows_hour) - 1))
        axis_labels.append(
            f'<text x="{axis_x:.1f}" y="207" text-anchor="middle">{html.escape(str(axis_item.get("hour_utc", "UNKNOWN")))}</text>'
        )
    for value_index, mean_value, lower_value, upper_value, hour, n in values:
        x = 28.0 + (664.0 * value_index / max(1, len(rows_hour) - 1))
        y_mean = 188.0 - 150.0 * (mean_value - low) / (high - low)
        if lower_value is not None:
            y_lower = 188.0 - 150.0 * (lower_value - low) / (high - low)
            if upper_value is not None:
                y_upper = 188.0 - 150.0 * (upper_value - low) / (high - low)
                marks.append(
                    f'<line x1="{x:.1f}" y1="{y_lower:.1f}" x2="{x:.1f}" y2="{y_upper:.1f}" stroke="#b45309" stroke-width="4"/>'
                )
            else:
                marks.append(
                    f'<circle cx="{x:.1f}" cy="{y_lower:.1f}" r="4" fill="none" stroke="#b45309" stroke-width="2"/>'
                )
        marks.append(f'<circle cx="{x:.1f}" cy="{y_mean:.1f}" r="4" fill="#2563eb"/>')
        labels.append(f"{hour}:00 n={n if n is not None else 'UNKNOWN'}")
    aria = html.escape(f"{title}; unit={unit}; n={count if count is not None else 'UNKNOWN'}; provenance={source}")
    return (
        f'<svg viewBox="0 0 720 260" role="img" aria-label="{aria}">'
        f'<text x="16" y="18">{html.escape(title)}</text>'
        + "".join(marks)
        + "".join(axis_labels)
        + f'<text x="4" y="40">{data_high:g}</text><text x="4" y="190">{data_low:g}</text><text x="16" y="218">eje Y: pips</text>'
        + '<text x="16" y="235" fill="#2563eb">● media</text><line x1="90" y1="231" x2="118" y2="231" stroke="#b45309" stroke-width="4"/><text x="124" y="235">p50 lower/upper_exclusive</text>'
        + f'<text class="footer" x="16" y="253">{_svg_footer(unit=unit, count=count, source=source, note="p50 bounds directos; overflow sin cola")}</text>'
        f"<title>{html.escape(' · '.join(labels[:24]))}</title></svg>"
    )


def render_market_structure_timeframe_svg(summary: Mapping[str, Any]) -> str:
    values: list[tuple[int, float, str | None, Any, Any]] = []
    source = _text(summary.get("provenance")) or "UNKNOWN"
    rows_tf = rows(summary.get("spread_atr_by_timeframe"))
    for index, item in enumerate(rows_tf):
        mean = _number(item.get("mean"))
        if mean is not None:
            values.append((index, float(mean), _text(item.get("timeframe")), item.get("n"), item.get("warmup")))
    title = "Spread/ATR por temporalidad · dimensionless"
    if not values:
        return _svg_unknown(title, unit="dimensionless", count=None, source=source, reason="ATR o medias no observadas")
    data_low: float = min(value[1] for value in values)
    data_high: float = max(value[1] for value in values)
    low: float = data_low
    high: float = data_high
    if low == high:
        low -= 0.5
        high += 0.5
    marks = []
    labels = []
    axis_labels: list[str] = []
    for axis_index, axis_item in enumerate(rows_tf):
        axis_x = 60.0 + 600.0 * axis_index / max(1, len(rows_tf) - 1)
        n_label = axis_item.get("n") if axis_item.get("n") is not None else "UNKNOWN"
        axis_labels.append(
            f'<text x="{axis_x:.1f}" y="204" text-anchor="middle">{html.escape(str(axis_item.get("timeframe", "UNKNOWN")))}</text>'
            f'<text class="footer" x="{axis_x:.1f}" y="218" text-anchor="middle">n={html.escape(str(n_label))}</text>'
        )
    for value_index, mean_value, timeframe, n, warmup in values:
        x = 60.0 + 600.0 * value_index / max(1, len(rows_tf) - 1)
        y = 188.0 - 150.0 * (mean_value - low) / (high - low)
        marks.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="#7c3aed"/>')
        labels.append(
            f"{timeframe} n={n if n is not None else 'UNKNOWN'} warmup={warmup if warmup is not None else 'UNKNOWN'}"
        )
    n_labels = ", ".join(
        f"{item.get('timeframe', 'UNKNOWN')} n={item.get('n') if item.get('n') is not None else 'UNKNOWN'}"
        for item in rows_tf
    )
    aria = html.escape(f"{title}; {n_labels}; provenance={source}")
    return (
        f'<svg viewBox="0 0 720 280" role="img" aria-label="{aria}">'
        f'<text x="16" y="18">{html.escape(title)}</text>'
        + "".join(marks)
        + "".join(axis_labels)
        + f'<text x="4" y="40">{data_high:g}</text><text x="4" y="190">{data_low:g}</text><text x="16" y="238">eje Y: dimensionless ratio</text>'
        + '<text x="16" y="255" fill="#7c3aed">● ratio medio</text><text x="150" y="255">D=UNKNOWN (denominador ATR no declarado) · warmup=UNKNOWN</text>'
        + f'<text class="footer" x="16" y="273">{_svg_footer(unit="dimensionless", count=None, source=source, note="n directo por TF; D UNKNOWN", include_count=False)}</text>'
        f"<title>{html.escape(' · '.join(labels))}</title></svg>"
    )


def render_market_structure_coverage_svg(summary: Mapping[str, Any]) -> str:
    daily = rows(summary.get("coverage_daily"))
    known: list[tuple[int, float, str]] = []
    for index, item in enumerate(daily):
        ticks = number(item.get("ticks"))
        if ticks is not None:
            known.append((index, float(ticks), str(item.get("date_utc"))))
    source = _text(summary.get("provenance")) or "UNKNOWN"
    count = summary.get("quote_count")
    title = "Cobertura diaria · ticks observados"
    if not known:
        return _svg_unknown(
            title, unit="ticks", count=count, source=source, reason="days_with_quotes sin ticks conocidos"
        )
    maximum: float = max(value for _, value, _ in known) or 1.0
    bars = []
    labels = []
    axis_labels: list[str] = []
    width = 664.0 / max(1, len(daily))
    label_stride = max(1, math.ceil(len(daily) / 12))
    for index, value, day in known[:MAX_VISUAL_POINTS]:
        x = 28.0 + width * index
        height = 150.0 * value / maximum
        bars.append(
            f'<rect x="{x:.1f}" y="{188.0 - height:.1f}" width="{max(1.0, width - 1):.1f}" height="{height:.1f}" fill="#059669"/>'
        )
        if index % label_stride == 0 or index == len(daily) - 1:
            axis_labels.append(
                f'<text x="{x + width / 2:.1f}" y="207" text-anchor="middle">{html.escape(day[5:] if len(day) >= 10 else day)}</text>'
            )
        labels.append(f"{day} ticks={int(value)}")
    return (
        f'<svg viewBox="0 0 720 260" role="img" aria-label="{html.escape(title)}; unit=ticks; n={count if count is not None else "UNKNOWN"}; provenance={source}">'
        f'<text x="16" y="18">{html.escape(title)}</text>'
        + "".join(bars)
        + "".join(axis_labels)
        + f'<text x="4" y="40">{maximum:g}</text><text x="4" y="190">0</text><text x="16" y="228">eje Y: ticks</text>'
        + f'<text class="footer" x="16" y="248">{_svg_footer(unit="ticks", count=count, source=source, note="días directos; calendario no fabricado")}</text>'
        f"<title>{html.escape(' · '.join(labels[:24]))}</title></svg>"
    )


def render_market_structure_gaps_svg(summary: Mapping[str, Any]) -> str:
    gaps = as_mapping(summary.get("gaps_utc"))
    largest = rows(gaps.get("largest"))
    known: list[tuple[int, float, str | None]] = []
    for index, item in enumerate(largest):
        seconds = number(item.get("seconds"))
        if seconds is not None:
            known.append((index, float(seconds), _text(item.get("from"))))
    source = _text(summary.get("provenance")) or "UNKNOWN"
    count = gaps.get("count")
    title = "Gaps UTC sobre el umbral declarado"
    if not known:
        reason = "sin gaps publicados" if count == 0 else "gaps o segundos UNKNOWN"
        return _svg_unknown(title, unit="seconds", count=count, source=source, reason=reason)
    maximum: float = max(value for _, value, _ in known) or 1.0
    bars = []
    labels = []
    axis_labels: list[str] = []
    width = 664.0 / max(1, len(known))
    for index, value, start in known[:MAX_VISUAL_POINTS]:
        x = 28.0 + width * index
        height = 150.0 * value / maximum
        bars.append(
            f'<rect x="{x:.1f}" y="{188.0 - height:.1f}" width="{max(1.0, width - 1):.1f}" height="{height:.1f}" fill="#dc2626"/>'
        )
        axis_labels.append(f'<text x="{x + width / 2:.1f}" y="207" text-anchor="middle">gap {index + 1}</text>')
        labels.append(f"gap {index + 1} {start} seconds={value:g}")
    return (
        f'<svg viewBox="0 0 720 260" role="img" aria-label="{html.escape(title)}; unit=seconds; n={count if count is not None else "UNKNOWN"}; provenance={source}">'
        f'<text x="16" y="18">{html.escape(title)}</text>'
        + "".join(bars)
        + "".join(axis_labels)
        + f'<text x="4" y="40">{maximum:g}</text><text x="4" y="190">0</text><text x="16" y="228">eje Y: seconds</text>'
        + f'<text class="footer" x="16" y="248">{_svg_footer(unit="seconds", count=count, source=source, note="clasificación UNKNOWN; tabla completa abajo")}</text>'
        f"<title>{html.escape(' · '.join(labels[:24]))}</title></svg>"
    )


def market_structure_svgs(value: Any) -> list[str]:
    summary = project_market_structure(value)
    if summary.get("status") not in {"ASSESSED", "UNKNOWN"}:
        return []
    return [
        render_market_structure_spread_svg(summary),
        render_market_structure_timeframe_svg(summary),
        render_market_structure_coverage_svg(summary),
        render_market_structure_gaps_svg(summary),
    ]


__all__ = [
    "MAX_TRADE_SUMMARIES",
    "MAX_VISUAL_POINTS",
    "as_mapping",
    "aggregate_sessions_episodes",
    "bounded_series",
    "break_even_metrics",
    "canonical_digest",
    "closed_trade",
    "confidence_for_result",
    "cost_bridge",
    "cost_bridge_from_rows",
    "dimensions",
    "drawdown_equity",
    "funnel_report",
    "iso",
    "iter_observations",
    "is_artifact_descriptor",
    "ledger_rows",
    "mfe_mae",
    "number",
    "project_market_structure",
    "provenance",
    "regime_session_report",
    "render_svg",
    "market_structure_svgs",
    "render_market_structure_coverage_svg",
    "render_market_structure_gaps_svg",
    "render_market_structure_spread_svg",
    "render_market_structure_timeframe_svg",
    "rows",
    "safe_value",
    "spread_report",
    "trade_amounts",
]
