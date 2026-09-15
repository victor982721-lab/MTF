"""Bounded statistical evidence gates for the frozen market protocol.

This module consumes runner summaries or small focal-test mappings.  It never
reads market data, opens a provider, changes a strategy, or turns a positive
number into a trading authorization.  ``ACCEPT`` means only eligible for a
human review under the declared protocol.
"""

from __future__ import annotations

import dataclasses
import random
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from ..core.numeric import decimal_context
from ..core.risk_exit import RiskExitPolicy
from .market_protocol import CANDIDATE_IDS, ResearchProtocol, quant_audit

EVIDENCE_SCHEMA = "mtf-lab.market-evidence.v1"
Decision = Literal["ACCEPT", "REJECT", "INSUFFICIENT", "INVALID"]
_D0 = Decimal("0")
_CALENDAR_ANCHOR = datetime(1970, 1, 1, tzinfo=UTC)
_SAMPLE_BLOCK_DAYS = 14


class MarketEvidenceError(ValueError):
    """Malformed result, protocol, or evidence input."""


class ResearchEvidenceReport(Mapping[str, Any]):
    """Immutable mapping wrapper for a machine-readable review report."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        if not isinstance(payload, Mapping):
            raise MarketEvidenceError("report debe ser mapping")
        self._payload = dict(payload)

    @classmethod
    def from_candidates(
        cls,
        results: Mapping[str, Mapping[str, Any] | Any],
        *,
        protocol: ResearchProtocol | None = None,
        criteria: EvidenceCriteria | None = None,
    ) -> ResearchEvidenceReport:
        return cls(evaluate_candidates(results, protocol=protocol, criteria=criteria))

    def __getitem__(self, key: str) -> Any:
        return self._payload[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._payload)

    def __len__(self) -> int:
        return len(self._payload)

    def to_dict(self) -> dict[str, Any]:
        return dict(self._payload)

    as_dict = to_dict


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        result = dataclasses.asdict(value)
        return result if isinstance(result, Mapping) else {}
    for method_name in ("to_mapping", "to_dict", "as_dict"):
        method = getattr(value, method_name, None)
        if callable(method):
            try:
                result = method()
            except TypeError:
                continue
            if isinstance(result, Mapping):
                return result
    return {}


def _rows(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return [value]
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [_mapping(item) for item in value if _mapping(item)]


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _decimal_text(value: Any) -> str | None:
    parsed = _decimal(value)
    if parsed is None:
        return None
    with decimal_context():
        return format(parsed, "f")


def _aware(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def _value(source: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in source and source[key] is not None:
            return source[key]
    return None


@dataclasses.dataclass(frozen=True, slots=True)
class BootstrapPolicy:
    """Pre-registered block-bootstrap settings."""

    iterations: int = 100_000
    seed: int = 20_260_913
    # The primary inference schedule is the protocol's two-week block.  The
    # two preregistered sensitivity widths are explicit calendar widths; they
    # are not multipliers (7 and 28 days, not 14 and 56 days).
    block_days: int = 14
    sensitivity: tuple[int, ...] = (7, 28)
    alpha: Decimal = Decimal("0.05")
    candidates: int = 6
    looks: int = 2

    def __post_init__(self) -> None:
        for name in ("iterations", "seed", "block_days", "candidates", "looks"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise MarketEvidenceError(f"{name} debe ser entero positivo")
        if self.sensitivity != (7, 28):
            raise MarketEvidenceError("bootstrap sensitivity requiere bloques explícitos de 7 y 28 días")
        alpha = _decimal(self.alpha)
        if alpha is None or not _D0 < alpha < Decimal("1"):
            raise MarketEvidenceError("alpha debe estar entre 0 y 1")
        object.__setattr__(self, "alpha", alpha)

    @property
    def per_look_candidate_alpha(self) -> Decimal:
        with decimal_context():
            return self.alpha / Decimal(self.candidates * self.looks)

    @property
    def sensitivity_days(self) -> tuple[int, ...]:
        return self.sensitivity

    def to_dict(self) -> dict[str, Any]:
        return {
            "iterations": self.iterations,
            "seed": self.seed,
            "block_days": self.block_days,
            "sensitivity": list(self.sensitivity),
            "sensitivity_days": list(self.sensitivity),
            "alpha": _decimal_text(self.alpha),
            "candidates": self.candidates,
            "looks": self.looks,
            "per_look_candidate_alpha": _decimal_text(self.per_look_candidate_alpha),
        }


@dataclasses.dataclass(frozen=True, slots=True)
class EvidenceCriteria:
    """Absolute candidate-versus-no-op gate; baseline is a candidate too."""

    stress_mean_r_min: Decimal = Decimal("0.05")
    max_drawdown_fraction: Decimal = Decimal("0.05")
    min_holdout_episodes: int = 120
    min_holdout_sessions: int = 100
    min_holdout_blocks: int = 20
    sample_block_days: int = _SAMPLE_BLOCK_DAYS
    min_walkforward_episodes: int = 20
    drop_best_count: int = 5
    bootstrap: BootstrapPolicy = dataclasses.field(default_factory=BootstrapPolicy)

    def __post_init__(self) -> None:
        for name in (
            "stress_mean_r_min",
            "max_drawdown_fraction",
        ):
            value = _decimal(getattr(self, name))
            if value is None or value < _D0:
                raise MarketEvidenceError(f"{name} debe ser Decimal no negativo")
            object.__setattr__(self, name, value)
        for name in (
            "min_holdout_episodes",
            "min_holdout_sessions",
            "min_holdout_blocks",
            "min_walkforward_episodes",
            "drop_best_count",
            "sample_block_days",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise MarketEvidenceError(f"{name} debe ser entero positivo")

    @classmethod
    def from_protocol(cls, protocol: ResearchProtocol) -> EvidenceCriteria:
        return cls(
            stress_mean_r_min=protocol.stress_mean_r_min,
            max_drawdown_fraction=protocol.max_drawdown_fraction,
            min_holdout_episodes=protocol.min_holdout_episodes,
            min_holdout_sessions=protocol.min_holdout_sessions,
            min_holdout_blocks=protocol.min_holdout_blocks,
            sample_block_days=protocol.block_days,
            min_walkforward_episodes=protocol.min_walkforward_episodes,
            drop_best_count=protocol.drop_best_count,
            bootstrap=BootstrapPolicy(
                iterations=protocol.bootstrap_iterations,
                seed=protocol.bootstrap_seed,
                block_days=getattr(protocol, "bootstrap_block_days", protocol.block_days),
                sensitivity=tuple(getattr(protocol, "bootstrap_sensitivity", (7, 28))),
                alpha=protocol.alpha,
                candidates=protocol.alpha_candidates,
                looks=protocol.alpha_looks,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "stress_mean_r_min": _decimal_text(self.stress_mean_r_min),
            "max_drawdown_fraction": _decimal_text(self.max_drawdown_fraction),
            "min_holdout_episodes": self.min_holdout_episodes,
            "min_holdout_sessions": self.min_holdout_sessions,
            "min_holdout_blocks": self.min_holdout_blocks,
            "sample_block_days": self.sample_block_days,
            "min_walkforward_episodes": self.min_walkforward_episodes,
            "drop_best_count": self.drop_best_count,
            "bootstrap": self.bootstrap.to_dict(),
        }


def _holdout(source: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("holdout", "evaluation_holdout", "locked_holdout"):
        value = _mapping(source.get(key))
        if value:
            return value
    splits = _mapping(source.get("splits"))
    return _mapping(splits.get("holdout")) if splits else source


def _stress(source: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("stress", "adverse", "cost_stress", "stress_case"):
        value = _mapping(source.get(key))
        if value:
            return value
    return {}


def _episodes(source: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    holdout = _holdout(source)
    for key in ("episodes", "trades", "ledger", "observations", "oos_episodes", "test_episodes"):
        rows = _rows(holdout.get(key))
        if rows:
            return rows
    return _rows(source.get("episodes"))


@dataclasses.dataclass(frozen=True, slots=True)
class _NetRDetail:
    value: Decimal | None
    reason: str | None = None
    invalid: bool = False
    source: str = "net_pnl_divided_by_initial_risk"
    net_pnl: Decimal | None = None
    initial_risk: Decimal | None = None


def _closed_row(row: Mapping[str, Any]) -> bool:
    state = _value(row, "state", "status", "lifecycle_state")
    if state is None:
        return True
    return str(state).strip().upper() in {"CLOSED", "SETTLED", "TERMINAL", "CLOSED_VALID"}


def _risk_mapping(row: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("risk_exit_plan", "entry_plan", "risk_plan", "plan", "initial_risk"):
        value = _mapping(row.get(key))
        if value:
            return value
    return {}


def _initial_risk_detail(row: Mapping[str, Any]) -> tuple[Decimal | None, str | None, bool]:
    direct = _value(
        row,
        "initial_risk",
        "initial_risk_amount",
        "initial_risk_account",
        "risk_amount",
        "risk_amount_account",
    )
    plan = _risk_mapping(row)
    plan_value = _value(plan, "initial_risk", "risk_amount", "risk_amount_account")
    direct_parsed = _decimal(direct)
    plan_parsed = _decimal(plan_value)
    if (
        direct is not None
        and plan_value is not None
        and (direct_parsed is None or plan_parsed is None or direct_parsed != plan_parsed)
    ):
        return None, "initial_risk_mismatch", True
    value = direct_parsed if direct is not None else plan_parsed
    if value is not None:
        return (value, None, False) if value > _D0 else (None, "initial_risk_nonpositive", True)
    per_unit = _value(row, "initial_risk_per_unit", "risk_per_unit", "risk_per_unit_account")
    if per_unit is None:
        per_unit = _value(plan, "risk_per_unit", "risk_per_unit_account")
    quantity = _value(row, "filled_quantity", "quantity", "units")
    if quantity is None:
        quantity = _value(plan, "quantity", "units")
    parsed_per_unit = _decimal(per_unit)
    parsed_quantity = _decimal(quantity)
    if parsed_per_unit is None or parsed_quantity is None or parsed_per_unit <= _D0 or parsed_quantity <= _D0:
        return None, "initial_risk_unknown", False
    with decimal_context():
        derived = parsed_per_unit * parsed_quantity
    if value is not None and value != derived:
        return None, "initial_risk_mismatch", True
    return derived, None, False


def _initial_risk(row: Mapping[str, Any]) -> Decimal | None:
    return _initial_risk_detail(row)[0]


def _net_pnl(row: Mapping[str, Any]) -> Decimal | None:
    direct = _value(row, "net_pnl", "net_pnl_account", "net_result", "net")
    if direct is not None:
        return _decimal(direct)
    economic = _mapping(row.get("economic_result"))
    nested = _value(economic, "net_pnl", "net_pnl_account", "net_result")
    if nested is not None:
        return _decimal(nested)
    gross = _value(row, "gross_pnl_account", "gross_pnl", "gross")
    costs = _value(row, "costs_account", "costs", "total_costs")
    if gross is None or costs is None:
        return None
    parsed_gross = _decimal(gross)
    parsed_costs = _decimal(costs)
    if parsed_gross is None or parsed_costs is None:
        return None
    with decimal_context():
        return parsed_gross - parsed_costs


def _row_costs_known(row: Mapping[str, Any]) -> bool:
    for key in ("costs_known", "economics_complete", "costs_complete"):
        value = row.get(key)
        if value is False:
            return False
    if row.get("commission_known") is False:
        return False
    if row.get("financing_required") is True and row.get("financing_quote") is None:
        return False
    economic = _mapping(row.get("economic_result"))
    if str(economic.get("state", "DETERMINED")).upper() in {"INDETERMINATE", "UNKNOWN", "NOT_SETTLED"}:
        return False
    costs = _mapping(row.get("costs"))
    if costs and str(costs.get("state", "KNOWN")).upper() not in {"KNOWN", "DETERMINED", "SETTLED"}:
        return False
    unknown_raw = costs.get("unknown_count") if costs else None
    unknown = _int(unknown_raw) if unknown_raw is not None else 0
    return unknown is not None and unknown == 0


def _explicit_net_r(row: Mapping[str, Any]) -> Decimal | None:
    return _decimal(_value(row, "net_r", "net_r_multiple", "net_return_r"))


def _episode_r_detail(row: Mapping[str, Any]) -> _NetRDetail:
    if not _closed_row(row):
        return _NetRDetail(None, "episode_not_closed")
    net = _net_pnl(row)
    risk, risk_reason, risk_invalid = _initial_risk_detail(row)
    explicit = _explicit_net_r(row)
    if risk_invalid:
        return _NetRDetail(None, risk_reason, True)
    if net is None or risk is None:
        return _NetRDetail(None, risk_reason or "net_pnl_or_initial_risk_unknown")
    with decimal_context():
        derived = net / risk
    if explicit is not None and explicit != derived:
        return _NetRDetail(None, "net_r_mismatch_with_net_pnl_and_initial_risk", True)
    semantics = (
        str(_value(row, "r_multiple_semantics", "r_semantics", "return_semantics", "risk_multiple_semantics") or "")
        .strip()
        .upper()
    )
    economics_version = str(row.get("economics_version", "")).strip().lower().replace("_", "-")
    canonical_net_field = semantics in {"NET", "NET_R", "NET_RETURN"} or economics_version in {
        "2",
        "v2",
        "cfd-economics-v2",
    }
    if canonical_net_field:
        opaque = _decimal(_value(row, "r_multiple", "risk_r_multiple"))
        if opaque is not None and opaque != derived:
            return _NetRDetail(None, "r_multiple_net_crosscheck_mismatch", True)
    if not _row_costs_known(row):
        return _NetRDetail(None, "costs_unknown", net_pnl=net, initial_risk=risk)
    # Legacy or ambiguous r_multiple fields may describe an excursion.  They
    # are never authoritative for economics; a positive excursion cannot
    # override a negative settled net PnL.  Canonical v2 fields are only
    # cross-checked above, while the value is still reconstructed here.
    return _NetRDetail(derived, net_pnl=net, initial_risk=risk)


def derive_net_r(row: Mapping[str, Any]) -> dict[str, Any]:
    """Derive the economic R multiple from settled net PnL and initial risk."""

    detail = _episode_r_detail(row)
    return {
        "status": "INVALID" if detail.invalid else "DERIVED" if detail.value is not None else "NOT_ASSESSED",
        "net_r": _decimal_text(detail.value),
        "net_pnl": _decimal_text(detail.net_pnl),
        "initial_risk": _decimal_text(detail.initial_risk),
        "reason": detail.reason,
        "source": detail.source,
        "r_multiple_not_used_as_authority": row.get("r_multiple") is not None,
    }


def _episode_r(row: Mapping[str, Any]) -> Decimal | None:
    return _episode_r_detail(row).value


def _episode_time(row: Mapping[str, Any]) -> datetime | None:
    return _aware(_value(row, "close_available_at", "close_market_at", "settled_at", "available_at", "timestamp", "at"))


def _sample_fallback(row: Mapping[str, Any]) -> str:
    when = _episode_time(row)
    return when.date().isoformat() if when is not None else ""


def _coverage_session_count(candidate: Mapping[str, Any], *, complete: bool | None) -> int | None:
    if complete is not True:
        return None
    raw = _value(candidate, "covered_sessions", "covered_session_ids", "session_dates", "covered_days", "sessions")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        return len(raw)
    if raw is not None:
        parsed = _int(raw)
        if parsed is not None:
            return parsed
    return _int(_value(candidate, "covered_session_count", "session_count", "n_sessions"))


def _coverage_gap_state(candidate: Mapping[str, Any]) -> bool | None:
    unknown = _value(candidate, "gaps_unknown", "gap_unknown")
    if unknown is True:
        return False
    known = _value(candidate, "gaps_known", "gap_known")
    if isinstance(known, bool):
        return known
    status = str(_value(candidate, "gaps_status", "gap_status") or "").upper()
    if status in {"KNOWN", "ASSESSED", "COMPLETE", "NONE"}:
        return True
    if status in {"UNKNOWN", "INCOMPLETE", "NOT_ASSESSED"}:
        return False
    gaps = _value(candidate, "gaps", "coverage_gaps")
    if isinstance(gaps, Sequence) and not isinstance(gaps, (str, bytes, bytearray)):
        return True
    return None


def _coverage_flags(candidate: Mapping[str, Any], container: Mapping[str, Any]) -> tuple[bool | None, bool | None]:
    complete = _value(candidate, "complete", "coverage_complete", "is_complete")
    complete_flag = complete if isinstance(complete, bool) else None
    if complete_flag is None:
        status = str(_value(candidate, "status", "coverage_status") or "").upper()
        if status in {"COMPLETE", "ASSESSED", "VALID"}:
            complete_flag = True
        elif status in {"INCOMPLETE", "UNKNOWN", "NOT_ASSESSED"}:
            complete_flag = False
    if complete_flag is None and candidate is not container:
        parent_complete = _value(container, "complete", "coverage_complete", "is_complete")
        complete_flag = parent_complete if isinstance(parent_complete, bool) else None
    gap_state = _coverage_gap_state(candidate)
    if gap_state is None and candidate is not container:
        gap_state = _coverage_gap_state(container)
    return complete_flag, gap_state


def _coverage_bounds(
    source: Mapping[str, Any],
) -> tuple[datetime | None, datetime | None, bool | None, int | None, bool | None]:
    """Read explicit UTC coverage, never inventing it from trade timestamps."""

    containers: list[Mapping[str, Any]] = []
    holdout = _holdout(source)
    if holdout is not source:
        containers.append(holdout)
    containers.append(source)
    dataset = _mapping(source.get("dataset"))
    if dataset:
        containers.append(dataset)
    for container in containers:
        nested = _mapping(_value(container, "coverage", "data_coverage", "observed_coverage", "calendar_coverage"))
        search = (nested, container) if nested else (container,)
        for candidate in search:
            start = _value(candidate, "coverage_start", "start", "from", "start_at")
            end = _value(candidate, "coverage_end", "end", "to", "end_at")
            if start is None or end is None:
                continue
            parsed_start = _aware(start)
            parsed_end = _aware(end)
            complete_flag, gaps_known = _coverage_flags(candidate, container)
            if parsed_start is None or parsed_end is None or parsed_end < parsed_start:
                return None, None, False, None, False
            return (
                parsed_start,
                parsed_end,
                complete_flag,
                _coverage_session_count(candidate, complete=complete_flag),
                gaps_known,
            )
    return None, None, None, None, None


def _valid_episode_rows(rows: Sequence[Mapping[str, Any]]) -> list[tuple[Mapping[str, Any], datetime, Decimal]]:
    valid: list[tuple[Mapping[str, Any], datetime, Decimal]] = []
    for row in rows:
        when = _episode_time(row)
        value = _episode_r(row)
        if when is not None and value is not None:
            valid.append((row, when, value))
    return valid


def _block_start(when: datetime, block_days: int) -> datetime:
    elapsed_days = (when.date() - _CALENDAR_ANCHOR.date()).days
    index = elapsed_days // block_days
    return _CALENDAR_ANCHOR + timedelta(days=index * block_days)


def _calendar_block_keys(start: datetime, end: datetime, block_days: int) -> tuple[datetime, ...]:
    """Return complete fixed-anchor calendar blocks in an explicit interval."""

    start_date = datetime(start.year, start.month, start.day, tzinfo=UTC)
    end_exclusive = datetime(end.year, end.month, end.day, tzinfo=UTC) + timedelta(days=1)
    first = _block_start(start_date, block_days)
    last = _block_start(end_exclusive - timedelta(microseconds=1), block_days)
    keys: list[datetime] = []
    key = first
    while key <= last:
        block_end = key + timedelta(days=block_days)
        if key >= start_date and block_end <= end_exclusive:
            keys.append(key)
        key += timedelta(days=block_days)
    return tuple(keys)


def _calendar_blocks(
    source: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    *,
    block_days: int,
) -> tuple[tuple[datetime, ...], tuple[tuple[Decimal, int], ...], int, int, dict[str, Any]]:
    """Aggregate ``(sum R, episode count)`` by complete calendar block.

    Blocks are generated from the declared coverage interval before trade rows
    are assigned.  Consequently a week with no episodes remains a zero block,
    while a missing coverage interval cannot be mistaken for a complete
    sample.  The returned counts are derived from rows and observed timestamps;
    declared summary counts are intentionally ignored.
    """

    coverage_start, coverage_end, coverage_complete, covered_sessions, gaps_known = _coverage_bounds(source)
    if coverage_start is None or coverage_end is None:
        return (), (), 0, 0, {"status": "NOT_ASSESSED", "reason": "coverage_missing"}
    keys = _calendar_block_keys(coverage_start, coverage_end, block_days)
    aggregates: dict[datetime, tuple[Decimal, int]] = {key: (_D0, 0) for key in keys}
    valid_rows = _valid_episode_rows(rows)
    sessions: set[str] = set()
    outside = 0
    for row, when, value in valid_rows:
        key = _block_start(when, block_days)
        if key not in aggregates:
            outside += 1
            continue
        aggregate_sum, aggregate_count = aggregates[key]
        aggregates[key] = (aggregate_sum + value, aggregate_count + 1)
        session = _value(row, "session_id", "session", "day") or when.date().isoformat()
        sessions.add(str(session))
    pairs = tuple(aggregates.values())
    session_count = max(len(sessions), covered_sessions or 0) if coverage_complete is True else len(sessions)
    coverage_status = "ASSESSED" if keys and coverage_complete is True and gaps_known is True else "NOT_ASSESSED"
    return (
        keys,
        pairs,
        sum(values[1] for values in aggregates.values()),
        session_count,
        {
            "status": coverage_status,
            "coverage_start": _iso(coverage_start),
            "coverage_end": _iso(coverage_end),
            "coverage_complete": coverage_complete,
            "covered_sessions": covered_sessions,
            "gaps_known": gaps_known,
            "block_days": block_days,
            "zero_episode_blocks": sum(values[1] == 0 for values in aggregates.values()),
            "rows_outside_complete_blocks": outside,
        },
    )


def _sample_facts(
    source: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], *, block_days: int = _SAMPLE_BLOCK_DAYS
) -> dict[str, Any]:
    valid_rows = _valid_episode_rows(rows)
    _, pairs, episodes, sessions, coverage = _calendar_blocks(source, rows, block_days=block_days)
    return {
        "episodes": episodes,
        "sessions": sessions,
        "blocks": len(pairs),
        "block_days": block_days,
        "coverage": coverage,
        "rows_with_valid_r_and_time": len(valid_rows),
    }


def _walkforward_counts(source: Mapping[str, Any]) -> tuple[int, tuple[int, ...]]:
    raw = _value(source, "walk_forward", "walkforward", "walk_forward_windows", "windows")
    windows = _rows(raw)
    if isinstance(raw, Mapping):
        windows = [_mapping(value) for value in raw.values() if _mapping(value)]
    counts = tuple(len(_valid_episode_rows(_episodes(item))) for item in windows)
    return len(windows), counts


def _metric_decimal(source: Mapping[str, Any], *keys: str) -> Decimal | None:
    value = _value(source, *keys)
    if value is not None:
        return _decimal(value)
    confidence = _mapping(source.get("confidence"))
    return _decimal(_value(confidence, *keys)) if confidence else None


def _complete_flag(source: Mapping[str, Any], *keys: str) -> bool | None:
    value = _value(source, *keys)
    return value if isinstance(value, bool) else None


def _complete_economics(source: Mapping[str, Any]) -> bool:
    costs = _mapping(source.get("costs"))
    flag = _complete_flag(source, "costs_complete", "costs_known", "economics_complete")
    if flag is False:
        return False
    if not costs or str(costs.get("state", "")).upper() != "KNOWN":
        return False
    unknown_raw = costs.get("unknown_count")
    unknown = _int(unknown_raw) if unknown_raw is not None else 0
    if unknown is None or unknown != 0:
        return False
    rows = _episodes(source)
    return all(_row_costs_known(row) for row in rows) and all(
        str(_mapping(row.get("economic_result")).get("state", "DETERMINED")).upper()
        in {"DETERMINED", "KNOWN", "SETTLED"}
        for row in rows
    )


def _complete_marks(source: Mapping[str, Any]) -> bool:
    flag = _complete_flag(source, "marks_complete", "mark_to_market_complete", "equity_complete")
    marks = _value(source, "equity_mark_to_market", "marks", "equity")
    if flag is not None:
        return flag and bool(marks)
    return bool(marks) and not any(
        _mapping(row).get("state") in {"UNKNOWN", "INDETERMINATE", "NOT_ASSESSED"} for row in _rows(marks)
    )


def _complete_r(rows: Sequence[Mapping[str, Any]], source: Mapping[str, Any]) -> bool:
    flag = _complete_flag(source, "r_complete", "risk_multiple_complete")
    valid = _valid_episode_rows(rows)
    return bool(valid) and len(valid) == len(rows) and flag is not False and not _invalid_r_rows(rows)


def _invalid_r_rows(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    for row in rows:
        detail = _episode_r_detail(row)
        if detail.invalid and detail.reason is not None:
            reasons.append(detail.reason)
    return reasons


def _net_values(rows: Sequence[Mapping[str, Any]]) -> list[Decimal]:
    values: list[Decimal] = []
    for row in rows:
        value = _net_pnl(row)
        if value is not None:
            values.append(value)
    return values


def _complete_net(rows: Sequence[Mapping[str, Any]]) -> bool:
    return bool(rows) and len(_net_values(rows)) == len(rows)


def _r_values(rows: Sequence[Mapping[str, Any]]) -> list[Decimal]:
    return [value for row in rows if (value := _episode_r(row)) is not None]


def _bootstrap_ratio_sample(blocks: Sequence[tuple[Decimal, int]], *, iterations: int, seed: int) -> list[Decimal]:
    """Resample bounded calendar aggregates, not the underlying trade rows."""

    if not blocks or sum(count for _, count in blocks) <= 0:
        return []
    rng = random.Random(seed)
    # Convert only the already aggregated block totals.  The inner loop is
    # bounded by calendar blocks, not by the number of episodes, and avoids
    # repeated Decimal arithmetic over raw rows at the 100k-iteration gate.
    numeric_blocks = tuple((float(block_sum), block_count) for block_sum, block_count in blocks)
    means: list[Decimal] = []
    for _ in range(iterations):
        total = 0.0
        count = 0
        indices = rng.choices(range(len(numeric_blocks)), k=len(numeric_blocks))
        total = sum(numeric_blocks[index][0] for index in indices)
        count = sum(numeric_blocks[index][1] for index in indices)
        if count <= 0:
            continue
        means.append(Decimal(str(total / count)))
    return means


def _quantile(values: Sequence[Decimal], probability: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    with decimal_context():
        position = probability * Decimal(len(ordered) - 1)
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    if lower == upper:
        return ordered[lower]
    with decimal_context():
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - Decimal(lower))


def bootstrap_mean_ci(
    values: Sequence[Decimal | int | float | str],
    *,
    policy: BootstrapPolicy | None = None,
    block_multiplier: int = 1,
) -> dict[str, Any]:
    """Return a deterministic one-sided block-bootstrap interval."""

    selected = policy or BootstrapPolicy()
    parsed = tuple(value for item in values if (value := _decimal(item)) is not None)
    if len(parsed) < 2:
        return {"status": "NOT_ASSESSED", "reason": "insufficient_observations", "observations": len(parsed)}
    samples = _bootstrap_ratio_sample(
        tuple((value, 1) for value in parsed),
        iterations=selected.iterations,
        seed=selected.seed + block_multiplier,
    )
    alpha = selected.per_look_candidate_alpha
    mean_value = sum(parsed, _D0) / Decimal(len(parsed))
    lower = _quantile(samples, alpha)
    return {
        "status": "ASSESSED" if lower is not None else "NOT_ASSESSED",
        "mean": _decimal_text(mean_value),
        "lcb": _decimal_text(lower),
        "alpha": _decimal_text(alpha),
        "observations": len(parsed),
        "iterations": selected.iterations,
        "seed": selected.seed + block_multiplier,
        "block_days": selected.block_days * block_multiplier,
    }


def _calendar_bootstrap_ci(
    source: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], *, policy: BootstrapPolicy, block_days: int
) -> dict[str, Any]:
    width = block_days
    _, blocks, episodes, _, coverage = _calendar_blocks(source, rows, block_days=width)
    if coverage.get("status") != "ASSESSED" or len(blocks) < 2 or episodes <= 0:
        return {
            "status": "NOT_ASSESSED",
            "reason": coverage.get("reason", "calendar_blocks_insufficient"),
            "observations": episodes,
            "blocks": len(blocks),
            "block_days": width,
        }
    samples = _bootstrap_ratio_sample(blocks, iterations=policy.iterations, seed=policy.seed + width)
    if not samples:
        return {
            "status": "NOT_ASSESSED",
            "reason": "bootstrap_zero_episode_resamples",
            "observations": episodes,
            "blocks": len(blocks),
            "block_days": width,
        }
    alpha = policy.per_look_candidate_alpha
    total = sum(value for value, _ in blocks)
    count = sum(count for _, count in blocks)
    mean_value = total / Decimal(count)
    lower = _quantile(samples, alpha)
    return {
        "status": "ASSESSED" if lower is not None else "NOT_ASSESSED",
        "mean": _decimal_text(mean_value),
        "lcb": _decimal_text(lower),
        "alpha": _decimal_text(alpha),
        "observations": count,
        "blocks": len(blocks),
        "iterations": policy.iterations,
        "seed": policy.seed + width,
        "block_days": width,
        "zero_episode_blocks": sum(block_count == 0 for _, block_count in blocks),
    }


def _metric_from_bootstrap(
    source: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], criteria: EvidenceCriteria
) -> dict[str, Any]:
    sensitivity: dict[str, dict[str, Any]] = {}
    primary = _calendar_bootstrap_ci(source, rows, policy=criteria.bootstrap, block_days=criteria.bootstrap.block_days)
    for width in criteria.bootstrap.sensitivity:
        sensitivity[str(width)] = _calendar_bootstrap_ci(source, rows, policy=criteria.bootstrap, block_days=width)
    mean_r = _decimal(primary.get("mean"))
    lower_bounds = [_decimal(primary.get("lcb"))] + [_decimal(item.get("lcb")) for item in sensitivity.values()]
    available = [item for item in lower_bounds if item is not None]
    lcb = min(available) if len(available) == len(lower_bounds) and available else None
    return {"mean_r": mean_r, "lcb": lcb, "primary": primary, "sensitivity": sensitivity}


def _ratio_ci_from_blocks(
    blocks: Sequence[tuple[Decimal, int]], *, policy: BootstrapPolicy, width: int
) -> dict[str, Any]:
    episodes = sum(count for _, count in blocks)
    if len(blocks) < 2 or episodes <= 0:
        return {
            "status": "NOT_ASSESSED",
            "reason": "calendar_blocks_insufficient",
            "observations": episodes,
            "blocks": len(blocks),
            "block_days": width,
        }
    samples = _bootstrap_ratio_sample(blocks, iterations=policy.iterations, seed=policy.seed + width)
    lower = _quantile(samples, policy.per_look_candidate_alpha)
    total = sum(value for value, _ in blocks)
    mean_value = total / Decimal(episodes)
    return {
        "status": "ASSESSED" if lower is not None else "NOT_ASSESSED",
        "mean": _decimal_text(mean_value),
        "lcb": _decimal_text(lower),
        "alpha": _decimal_text(policy.per_look_candidate_alpha),
        "observations": episodes,
        "blocks": len(blocks),
        "iterations": policy.iterations,
        "seed": policy.seed + width,
        "block_days": width,
        "zero_episode_blocks": sum(count == 0 for _, count in blocks),
    }


def _aligned_ratio_cis(
    candidates: Mapping[str, Sequence[tuple[Decimal, int]]], *, policy: BootstrapPolicy, width: int
) -> dict[str, dict[str, Any]]:
    """Use one block-index resample schedule for every candidate."""

    if not candidates:
        return {}
    ordered_candidates = tuple(sorted(candidates))
    block_count = len(candidates[ordered_candidates[0]])
    rng = random.Random(policy.seed + width)
    numeric: dict[str, tuple[tuple[float, int], ...]] = {
        candidate: tuple((float(block_sum), block_count) for block_sum, block_count in candidates[candidate])
        for candidate in ordered_candidates
    }
    samples: dict[str, list[Decimal]] = {candidate: [] for candidate in ordered_candidates}
    for _ in range(policy.iterations):
        indices = rng.choices(range(block_count), k=block_count)
        for candidate in ordered_candidates:
            totals = sum(numeric[candidate][index][0] for index in indices)
            counts = sum(numeric[candidate][index][1] for index in indices)
            if counts > 0:
                samples[candidate].append(Decimal(str(totals / counts)))
    result: dict[str, dict[str, Any]] = {}
    for candidate in ordered_candidates:
        blocks = candidates[candidate]
        episodes = sum(count for _, count in blocks)
        lower = _quantile(samples[candidate], policy.per_look_candidate_alpha)
        mean_value = sum(value for value, _ in blocks) / Decimal(episodes) if episodes > 0 else None
        result[candidate] = {
            "status": "ASSESSED" if lower is not None else "NOT_ASSESSED",
            "mean": _decimal_text(mean_value),
            "lcb": _decimal_text(lower),
            "alpha": _decimal_text(policy.per_look_candidate_alpha),
            "observations": episodes,
            "blocks": len(blocks),
            "iterations": policy.iterations,
            "seed": policy.seed + width,
            "block_days": width,
            "zero_episode_blocks": sum(count == 0 for _, count in blocks),
        }
    return result


def joint_aligned_bootstrap(
    candidates: Mapping[str, Mapping[str, Any]],
    *,
    policy: BootstrapPolicy | None = None,
) -> dict[str, Any]:
    """Bootstrap aligned candidate blocks with one shared resample schedule."""

    selected = policy or BootstrapPolicy()
    block_maps: dict[str, dict[datetime, tuple[Decimal, int]]] = {}
    for candidate_id, source in candidates.items():
        rows = _episodes(source)
        keys_for_candidate, pairs, _, _, coverage = _calendar_blocks(source, rows, block_days=selected.block_days)
        if coverage.get("status") != "ASSESSED":
            continue
        block_maps[candidate_id] = dict(zip(keys_for_candidate, pairs, strict=True))
    keys = set.intersection(*(set(item) for item in block_maps.values())) if block_maps else set()
    if len(block_maps) != selected.candidates or len(keys) < 2:
        return {
            "status": "NOT_ASSESSED",
            "reason": "aligned_candidate_blocks_insufficient",
            "candidate_count": len(block_maps),
            "blocks": len(keys),
        }
    ordered = tuple(sorted(keys))
    shared = {candidate: [mapping[key] for key in ordered] for candidate, mapping in block_maps.items()}
    result: dict[str, Any] = {
        "status": "ASSESSED",
        "aligned": True,
        "candidate_count": len(shared),
        "blocks": len(ordered),
        "candidates": {},
        "sensitivity": {},
    }
    result["candidates"] = _aligned_ratio_cis(shared, policy=selected, width=selected.block_days)
    for width in selected.sensitivity:
        sensitivity_maps: dict[str, dict[datetime, tuple[Decimal, int]]] = {}
        for candidate_id, source in candidates.items():
            rows = _episodes(source)
            keys_for_width, pairs_for_width, _, _, coverage = _calendar_blocks(source, rows, block_days=width)
            if coverage.get("status") != "ASSESSED":
                sensitivity_maps = {}
                break
            sensitivity_maps[candidate_id] = dict(zip(keys_for_width, pairs_for_width, strict=True))
        common = (
            set.intersection(*(set(item) for item in sensitivity_maps.values()))
            if len(sensitivity_maps) == selected.candidates
            else set()
        )
        if len(common) < 2:
            result["sensitivity"][str(width)] = {
                "status": "NOT_ASSESSED",
                "reason": "aligned_calendar_blocks_insufficient",
                "blocks": len(common),
                "block_days": width,
            }
            continue
        ordered_width = tuple(sorted(common))
        result["sensitivity"][str(width)] = {
            "status": "ASSESSED",
            "block_days": width,
            "blocks": len(ordered_width),
            "candidates": _aligned_ratio_cis(
                {
                    candidate_id: [sensitivity_maps[candidate_id][key] for key in ordered_width]
                    for candidate_id in sensitivity_maps
                },
                policy=selected,
                width=width,
            ),
        }
    return result


def _fixture(source: Mapping[str, Any]) -> bool:
    provenance = _mapping(source.get("provenance"))
    return bool(
        source.get("fixture") is True
        or source.get("synthetic") is True
        or source.get("mode") in {"FIXTURE", "SYNTHETIC"}
        or provenance.get("synthetic") is True
        or provenance.get("fixture") is True
    )


def _check(status: str, value: Any, *, reason: str | None = None) -> dict[str, Any]:
    return {"status": status, "value": value, "reason": reason}


def _drop_best(values: Sequence[Decimal], count: int) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values, reverse=True)
    kept = ordered[min(count, len(ordered)) :]
    return sum(kept, _D0)


def _completeness_reasons(source: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> list[str]:
    checks = (
        ("fixture_not_external_evidence", not _fixture(source)),
        ("costs_incomplete_or_unknown", _complete_economics(source)),
        ("marks_incomplete_or_unknown", _complete_marks(source)),
        ("risk_multiple_incomplete", _complete_r(rows, source)),
        ("net_pnl_incomplete", _complete_net(rows)),
    )
    return [reason for reason, complete in checks if not complete]


def _sample_reasons(
    sample: Mapping[str, Any],
    walkforward_count: int,
    walkforward_values: Sequence[int],
    criteria: EvidenceCriteria,
) -> list[str]:
    coverage = sample.get("coverage")
    coverage_ok = isinstance(coverage, Mapping) and coverage.get("status") == "ASSESSED"
    checks = (
        ("holdout_coverage_missing_or_incomplete", coverage_ok),
        ("holdout_episodes_below_minimum", sample["episodes"] >= criteria.min_holdout_episodes),
        ("holdout_sessions_below_minimum", sample["sessions"] >= criteria.min_holdout_sessions),
        ("holdout_blocks_below_minimum", sample["blocks"] >= criteria.min_holdout_blocks),
        (
            "walkforward_sample_below_minimum",
            walkforward_count >= 4 and all(value >= criteria.min_walkforward_episodes for value in walkforward_values),
        ),
    )
    return [reason for reason, sufficient in checks if not sufficient]


def _metric_reasons(
    stress_metrics: Mapping[str, Decimal | None],
    base_metrics: Mapping[str, Decimal | None],
    drawdown: Decimal | None,
    drop_best: Decimal | None,
) -> list[str]:
    checks = (
        ("stress_inference_missing", stress_metrics["mean_r"] is not None and stress_metrics["lcb"] is not None),
        ("base_inference_missing", base_metrics["lcb"] is not None),
        ("stress_drawdown_missing", drawdown is not None),
        ("drop_best_sample_missing", drop_best is not None),
    )
    return [reason for reason, available in checks if not available]


def _candidate_violations(
    stress_metrics: Mapping[str, Decimal | None],
    base_metrics: Mapping[str, Decimal | None],
    drawdown: Decimal | None,
    drop_best: Decimal | None,
    criteria: EvidenceCriteria,
) -> list[str]:
    stress_mean_r = stress_metrics["mean_r"]
    stress_lcb = stress_metrics["lcb"]
    base_lcb = base_metrics["lcb"]
    if stress_mean_r is None or stress_lcb is None or base_lcb is None or drawdown is None or drop_best is None:
        return ["required_metric_missing"]
    checks = (
        ("stress_mean_r_below_minimum", stress_mean_r >= criteria.stress_mean_r_min),
        ("lcb_not_positive_base_or_stress", stress_lcb > _D0 and base_lcb > _D0),
        ("stress_drawdown_above_limit", drawdown <= criteria.max_drawdown_fraction),
        ("drop_best_net_negative", drop_best >= _D0),
    )
    return [reason for reason, passes in checks if not passes]


def _required_metric(value: Decimal | None, name: str) -> Decimal:
    if value is None:
        raise MarketEvidenceError(f"métrica requerida ausente: {name}")
    return value


def evaluate_candidate(
    candidate_id: str,
    result: Mapping[str, Any] | Any,
    *,
    protocol: ResearchProtocol | None = None,
    criteria: EvidenceCriteria | None = None,
    risk_policy: RiskExitPolicy | None = None,
) -> dict[str, Any]:
    """Evaluate one candidate against no-operation, never against rank alone."""

    source = _mapping(result)
    selected_protocol = protocol or ResearchProtocol.default()
    selected_criteria = criteria or EvidenceCriteria.from_protocol(selected_protocol)
    if candidate_id not in selected_protocol.candidate_ids:
        raise MarketEvidenceError(f"candidate no registrado: {candidate_id}")
    rows = _episodes(source)
    holdout = _holdout(source)
    stress = _stress(source)
    sample = _sample_facts(source, rows, block_days=selected_criteria.sample_block_days)
    walkforward_count, walkforward_values = _walkforward_counts(source)
    stress_metrics = _metric_from_bootstrap(stress, _episodes(stress) or rows, selected_criteria)
    base_metrics = _metric_from_bootstrap(holdout, rows, selected_criteria)
    drawdown = _metric_decimal(stress, "max_drawdown_fraction", "drawdown_fraction", "max_dd_fraction")
    drop_best = _drop_best(_net_values(rows), selected_criteria.drop_best_count)
    risk_audit = quant_audit(risk_policy or selected_protocol.risk_policy)
    invalid_r_reasons = _invalid_r_rows(rows)
    reasons = _completeness_reasons(source, rows)
    if stress and not _complete_economics(stress):
        reasons.append("stress_costs_incomplete_or_unknown")
    reasons.extend(invalid_r_reasons)
    reasons.extend(_sample_reasons(sample, walkforward_count, walkforward_values, selected_criteria))
    reasons.extend(_metric_reasons(stress_metrics, base_metrics, drawdown, drop_best))
    if invalid_r_reasons:
        decision: Decision = "INVALID"
    elif reasons:
        decision = "INSUFFICIENT"
    else:
        violations = _candidate_violations(stress_metrics, base_metrics, drawdown, drop_best, selected_criteria)
        decision = "REJECT" if violations else "ACCEPT"
        reasons.extend(violations)
    return {
        "schema": EVIDENCE_SCHEMA,
        "candidate_id": candidate_id,
        "decision": decision,
        "eligibility": "HUMAN_REVIEW_ONLY" if decision == "ACCEPT" else "NOT_ELIGIBLE",
        "auto_promote": False,
        "trading_enabled": False,
        "no_operation_reference": "ZERO_NET_AFTER_EXPLICIT_COSTS",
        "economic_r_definition": "settled_net_pnl_divided_by_immutable_initial_risk",
        "opaque_r_multiple_is_not_economic_authority": True,
        "invalid_r_reasons": sorted(set(invalid_r_reasons)),
        "reasons": sorted(set(reasons)),
        "sample": sample,
        "walkforward": {"window_count": walkforward_count, "test_episodes": list(walkforward_values)},
        "base": {
            "lcb_r": _decimal_text(base_metrics["lcb"]),
            "primary_bootstrap": base_metrics["primary"],
            "bootstrap_sensitivity": base_metrics["sensitivity"],
        },
        "stress": {
            "mean_r": _decimal_text(stress_metrics["mean_r"]),
            "lcb_r": _decimal_text(stress_metrics["lcb"]),
            "max_drawdown_fraction": _decimal_text(drawdown),
            "primary_bootstrap": stress_metrics["primary"],
            "bootstrap_sensitivity": stress_metrics["sensitivity"],
        },
        "drop_best": {
            "count": selected_criteria.drop_best_count,
            "remaining_net": _decimal_text(drop_best),
        },
        "costs_complete": _complete_economics(source),
        "marks_complete": _complete_marks(source),
        "risk_multiple_complete": _complete_r(rows, source),
        "risk_policy_audit": risk_audit,
        "criteria": selected_criteria.to_dict(),
    }


def rank_evidence(evidence: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Rank only as a deterministic review queue, never as approval."""

    def key(item: Mapping[str, Any]) -> tuple[Any, ...]:
        stress = _mapping(item.get("stress"))
        dd = _decimal(stress.get("max_drawdown_fraction"))
        lcb = _decimal(stress.get("lcb_r"))
        costs = item.get("costs_complete") is True
        return (
            0
            if item.get("decision") == "ACCEPT"
            else 1
            if item.get("decision") == "REJECT"
            else 2
            if item.get("decision") == "INVALID"
            else 3,
            dd is None,
            dd if dd is not None else Decimal("Infinity"),
            not costs,
            -(lcb if lcb is not None else Decimal("-Infinity")),
            str(item.get("candidate_id", "")),
        )

    return [dict(item) for item in sorted(evidence, key=key)]


def evaluate_candidates(
    results: Mapping[str, Mapping[str, Any] | Any],
    *,
    protocol: ResearchProtocol | None = None,
    criteria: EvidenceCriteria | None = None,
) -> dict[str, Any]:
    """Evaluate the six pre-registered candidates and return a review report."""

    selected_protocol = protocol or ResearchProtocol.default()
    selected_criteria = criteria or EvidenceCriteria.from_protocol(selected_protocol)
    evidence = [
        evaluate_candidate(candidate_id, results[candidate_id], protocol=selected_protocol, criteria=selected_criteria)
        for candidate_id in selected_protocol.candidate_ids
        if candidate_id in results
    ]
    joint = joint_aligned_bootstrap(
        {
            candidate_id: _mapping(results[candidate_id])
            for candidate_id in selected_protocol.candidate_ids
            if candidate_id in results
        },
        policy=selected_criteria.bootstrap,
    )
    return {
        "schema": EVIDENCE_SCHEMA,
        "protocol_hash": selected_protocol.protocol_hash,
        "criteria": selected_criteria.to_dict(),
        "candidate_count": len(evidence),
        "decisions": {str(item["candidate_id"]): item for item in evidence},
        "rank": [item["candidate_id"] for item in rank_evidence(evidence)],
        "joint_aligned_bootstrap": joint,
        "auto_promote": False,
        "trading_enabled": False,
        "state": (
            "REVIEW_REQUIRED"
            if any(item["decision"] == "ACCEPT" for item in evidence)
            else "INVALID_INPUT"
            if any(item["decision"] == "INVALID" for item in evidence)
            else "NO_ELIGIBLE_CANDIDATE"
        ),
    }


def validate_evidence(report: Mapping[str, Any]) -> dict[str, Any]:
    """Validate report shape without asserting market profitability."""

    if not isinstance(report, Mapping):
        return {"ok": False, "state": "INVALID", "errors": ["report_not_mapping"]}
    errors: list[str] = []
    if report.get("schema") != EVIDENCE_SCHEMA:
        errors.append("schema_incompatible")
    decisions = report.get("decisions")
    if not isinstance(decisions, Mapping):
        errors.append("decisions_missing")
    else:
        for candidate_id, evidence in decisions.items():
            if candidate_id not in CANDIDATE_IDS:
                errors.append(f"candidate_unknown:{candidate_id}")
            if not isinstance(evidence, Mapping) or evidence.get("decision") not in {
                "ACCEPT",
                "REJECT",
                "INSUFFICIENT",
                "INVALID",
            }:
                errors.append(f"decision_invalid:{candidate_id}")
            if isinstance(evidence, Mapping) and (
                evidence.get("auto_promote") is not False or evidence.get("trading_enabled") is not False
            ):
                errors.append(f"promotion_gate_invalid:{candidate_id}")
    return {"ok": not errors, "state": "VALID" if not errors else "INVALID", "errors": errors}


__all__ = [
    "BootstrapPolicy",
    "Decision",
    "EVIDENCE_SCHEMA",
    "EvidenceCriteria",
    "MarketEvidenceError",
    "ResearchEvidenceReport",
    "bootstrap_mean_ci",
    "derive_net_r",
    "evaluate_candidate",
    "evaluate_candidates",
    "joint_aligned_bootstrap",
    "rank_evidence",
    "validate_evidence",
]
