"""Auditable risk and performance metrics for one CFD research result.

The module consumes the already materialized result mapping from the local
research service.  It does not read a capture, query a provider, or rebuild a
simulation.  Values that cannot be established from known, timezone-aware
observations remain ``None`` and carry an explicit status/reason instead of
being replaced with zero.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ..core.numeric import decimal_context, seconds_decimal

_D0 = Decimal("0")
_KNOWN_STATES = {"CLOSED", "FILLED", "OPEN", "CLOSE_OBSERVED"}
_TERMINAL_NO_EXECUTION = {"REJECTED", "PENDING"}


@dataclass(frozen=True, slots=True)
class _Mark:
    """One known mark-to-market observation in UTC order."""

    at: datetime
    equity: Decimal
    exposure: Decimal | None
    quantity: Decimal | None
    currency: str | None
    ordinal: int


@dataclass(frozen=True, slots=True)
class _TradeFacts:
    """Known and unknown economic facts extracted without mutation."""

    known_net: tuple[tuple[Mapping[str, Any], Decimal, datetime | None], ...]
    unknown_net_count: int
    known_cost_total: Decimal | None
    unknown_cost_count: int
    currencies: tuple[str, ...]


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _rows(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _nonnegative_decimal(value: Any) -> Decimal | None:
    parsed = _decimal(value)
    return parsed if parsed is not None and parsed >= _D0 else None


def _decimal_text(value: Any) -> str | None:
    parsed = value if isinstance(value, Decimal) else _decimal(value)
    if parsed is None or not parsed.is_finite():
        return None
    with decimal_context():
        return format(parsed, "f")


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
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z") if value is not None else None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _state(row: Mapping[str, Any]) -> str:
    return str(row.get("state", "CLOSED")).strip().upper()


def _currency(row: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _text(row.get(key))
        if value:
            return value.upper()
    return None


def _parse_marks(result: Mapping[str, Any]) -> tuple[list[_Mark], int, list[str]]:
    known: list[_Mark] = []
    unknown = 0
    reasons: list[str] = []
    for ordinal, row in enumerate(_rows(result.get("equity_mark_to_market"))):
        at = _aware(row.get("available_at", row.get("timestamp", row.get("at"))))
        equity = _decimal(row.get("equity"))
        state = str(row.get("state", "KNOWN")).strip().upper()
        unknown_components = _nonnegative_decimal(row.get("unknown_marks"))
        if at is None or equity is None or state in {"UNKNOWN", "INDETERMINATE", "NOT_ASSESSED"}:
            unknown += 1
            reasons.append("equity_mark_unknown_or_unordered")
            continue
        if unknown_components is not None and unknown_components > 0:
            unknown += 1
            reasons.append("equity_mark_contains_unknown_components")
            continue
        exposure = _nonnegative_decimal(row.get("exposure"))
        quantity = _nonnegative_decimal(row.get("quantity", row.get("exposure_quantity")))
        currency = _currency(row, "currency", "exposure_currency")
        known.append(_Mark(at, equity, exposure, quantity, currency, ordinal))
    known.sort(key=lambda item: (item.at, item.ordinal))
    summary = _mapping(result.get("equity_summary"))
    reported_unknown = _nonnegative_decimal(summary.get("unknown_mark_count"))
    if reported_unknown is not None and reported_unknown > 0:
        unknown = max(unknown, int(reported_unknown))
        reasons.append("equity_summary_unknown_marks")
    return known, unknown, sorted(set(reasons))


def _closed_trade(row: Mapping[str, Any]) -> bool:
    state = _state(row)
    if state in _TERMINAL_NO_EXECUTION:
        return False
    if state in {"CLOSED", "CLOSE_OBSERVED"}:
        return True
    return row.get("net_pnl") is not None and row.get("close_available_at", row.get("close_price")) is not None


def _trade_facts(result: Mapping[str, Any]) -> _TradeFacts:
    ledger = _rows(result.get("ledger"))
    known_net: list[tuple[Mapping[str, Any], Decimal, datetime | None]] = []
    unknown_net = 0
    known_costs: list[Decimal] = []
    unknown_costs = 0
    currencies: set[str] = set()
    for row in ledger:
        state = _state(row)
        if state == "REJECTED":
            continue
        if _closed_trade(row):
            net = _decimal(row.get("net_pnl"))
            close_at = _aware(row.get("close_available_at", row.get("close_target_at")))
            if net is None or close_at is None:
                unknown_net += 1
            else:
                known_net.append((row, net, close_at))
            cost = _decimal(row.get("costs_account"))
            costs_known = row.get("costs_known") is True or cost is not None
            if cost is not None and costs_known:
                known_costs.append(cost)
            else:
                unknown_costs += 1
            currency = _currency(row, "account_currency", "currency")
            if currency:
                currencies.add(currency)
    summary = _mapping(result.get("costs"))
    summary_known = _decimal(summary.get("known"))
    reported_unknown = _nonnegative_decimal(summary.get("unknown_count"))
    if reported_unknown is not None:
        unknown_costs = max(unknown_costs, int(reported_unknown))
    with decimal_context():
        known_cost_total = sum(known_costs, _D0) if known_costs else summary_known
    if summary.get("state") not in {None, "KNOWN"}:
        unknown_costs = max(unknown_costs, 1)
    return _TradeFacts(tuple(known_net), unknown_net, known_cost_total, unknown_costs, tuple(sorted(currencies)))


def _observed_days(result: Mapping[str, Any]) -> tuple[str, ...]:
    raw = result.get("observed_calendar_days", ())
    days: set[str] = set()
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        for value in raw:
            try:
                days.add(date.fromisoformat(str(value)).isoformat())
            except (TypeError, ValueError):
                continue
    return tuple(sorted(days))


def _daily_net(facts: _TradeFacts, result: Mapping[str, Any]) -> tuple[dict[str, Decimal], int]:
    grouped: defaultdict[str, Decimal] = defaultdict(lambda: _D0)
    unknown_dates = 0
    with decimal_context():
        for day in _observed_days(result):
            grouped[day] += _D0
        for _row, net, close_at in facts.known_net:
            if close_at is None:
                unknown_dates += 1
                continue
            grouped[close_at.date().isoformat()] += net
        for row in _rows(result.get("ledger")):
            if _state(row) in _TERMINAL_NO_EXECUTION or not _closed_trade(row):
                continue
            if _decimal(row.get("net_pnl")) is not None:
                continue
            if _aware(row.get("close_available_at", row.get("close_target_at"))) is not None:
                unknown_dates += 1
    return dict(sorted(grouped.items())), unknown_dates


def _drawdown_metrics(marks: Sequence[_Mark]) -> dict[str, Any]:
    if not marks:
        return {
            "status": "NOT_ASSESSED",
            "reason": "equity_marks_missing_or_unusable",
            "max_drawdown": None,
            "max_drawdown_peak_at": None,
            "max_drawdown_trough_at": None,
            "recovery_duration_seconds": None,
            "unrecovered_duration_seconds": None,
            "unrecovered_state": "NOT_ASSESSED",
            "known_mark_count": 0,
        }
    peak = marks[0].equity
    peak_at = marks[0].at
    maximum = _D0
    maximum_peak_at: datetime | None = None
    maximum_trough_at: datetime | None = None
    underwater_since: datetime | None = None
    completed_recoveries: list[Decimal] = []
    with decimal_context():
        for mark in marks:
            if mark.equity < peak:
                if underwater_since is None:
                    underwater_since = peak_at
                drawdown = peak - mark.equity
                if drawdown > maximum:
                    maximum = drawdown
                    maximum_peak_at = peak_at
                    maximum_trough_at = mark.at
                continue
            if underwater_since is not None:
                completed_recoveries.append(seconds_decimal(mark.at - underwater_since))
                underwater_since = None
            if mark.equity > peak:
                peak = mark.equity
                peak_at = mark.at
    unrecovered = underwater_since is not None
    maximum_recovery = None if underwater_since is not None else max(completed_recoveries, default=Decimal("0"))
    with decimal_context():
        unrecovered_duration = (
            seconds_decimal(marks[-1].at - underwater_since) if underwater_since is not None else Decimal("0")
        )
    return {
        "status": "ASSESSED",
        "reason": None,
        "max_drawdown": maximum,
        "max_drawdown_peak_at": maximum_peak_at,
        "max_drawdown_trough_at": maximum_trough_at,
        "recovery_duration_seconds": maximum_recovery,
        "unrecovered_state": "UNRECOVERED" if unrecovered else "RECOVERED",
        "unrecovered_since": underwater_since,
        "unrecovered_duration_seconds": unrecovered_duration if unrecovered else Decimal("0"),
        "known_mark_count": len(marks),
    }


def _known_quantity(row: Mapping[str, Any]) -> Decimal | None:
    quantity = _nonnegative_decimal(row.get("filled_quantity"))
    if quantity is not None and quantity > _D0:
        return quantity
    if row.get("entry_price") is None and row.get("entry_available_at") is None:
        return None
    units = _nonnegative_decimal(row.get("units"))
    return units if units is not None and units > _D0 else None


def _quantity_at(trades: Sequence[Mapping[str, Any]], at: datetime) -> Decimal | None:
    total = _D0
    found = False
    uncertain = False
    for row in trades:
        if _state(row) in _TERMINAL_NO_EXECUTION:
            continue
        entry = _aware(row.get("entry_available_at"))
        if entry is None:
            if _state(row) in {"FILLED", "CLOSED", "CLOSE_OBSERVED"}:
                uncertain = True
            continue
        close = _aware(row.get("close_available_at"))
        if at < entry or (close is not None and at >= close):
            continue
        quantity = _known_quantity(row)
        if quantity is None:
            uncertain = True
            continue
        with decimal_context():
            total += quantity
        found = True
    if uncertain and not found:
        return None
    return total


def _exposure_metrics(marks: Sequence[_Mark], result: Mapping[str, Any]) -> dict[str, Any]:
    candidates = [mark for mark in marks if mark.exposure is not None]
    if not candidates:
        return {"status": "NOT_ASSESSED", "reason": "exposure_marks_missing", "peak": None}
    peak = max(candidates, key=lambda mark: (mark.exposure or _D0, -mark.ordinal))
    quantity = peak.quantity
    if quantity is None:
        quantity = _quantity_at(_rows(result.get("ledger")), peak.at)
    currency = peak.currency or _currency(result, "exposure_currency", "account_currency")
    if currency is None:
        currencies = {
            value
            for row in _rows(result.get("ledger"))
            if (value := _currency(row, "quote_currency", "exposure_currency")) is not None
        }
        currency = next(iter(currencies)) if len(currencies) == 1 else None
    duration = _D0
    notional_time = _D0
    for current, following in zip(marks, marks[1:], strict=False):
        if current.exposure is None or current.exposure <= _D0:
            continue
        with decimal_context():
            seconds = seconds_decimal(following.at - current.at)
        if seconds <= _D0:
            continue
        with decimal_context():
            duration += seconds
            notional_time += current.exposure * seconds
    return {
        "status": "ASSESSED",
        "reason": None,
        "peak": peak.exposure,
        "peak_at": peak.at,
        "peak_quantity": quantity,
        "quantity_status": "ASSESSED" if quantity is not None else "NOT_ASSESSED",
        "currency": currency,
        "duration_seconds": duration,
        "notional_time": notional_time,
        "basis": "known_equity_mark_to_market_exposure",
    }


def _net_metrics(facts: _TradeFacts) -> dict[str, Any]:
    if not facts.known_net:
        return {
            "status": "NOT_ASSESSED",
            "reason": "no_known_closed_trade_net_pnl",
            "net": None,
            "expectancy": None,
            "known_subtotal": None,
            "observations": 0,
        }
    with decimal_context():
        subtotal = sum((item[1] for item in facts.known_net), _D0)
        expectancy = subtotal / Decimal(len(facts.known_net))
    complete = facts.unknown_net_count == 0 and facts.unknown_cost_count == 0
    return {
        "status": "ASSESSED" if complete else "PARTIAL_NOT_ASSESSED",
        "reason": None if complete else "unknown_trade_economics",
        "net": subtotal if complete else None,
        "expectancy": expectancy if complete else None,
        "known_subtotal": subtotal,
        "known_expectancy": expectancy,
        "observations": len(facts.known_net),
        "unknown_trade_count": facts.unknown_net_count,
    }


def _turnover_row(row: Mapping[str, Any]) -> tuple[Decimal, int, int, str | None]:
    state = _state(row)
    if state in _TERMINAL_NO_EXECUTION:
        return _D0, 0, 0, None
    quantity = _known_quantity(row)
    entry = _decimal(row.get("entry_price"))
    close = _decimal(row.get("close_price"))
    if quantity is None:
        return _D0, int(state in _KNOWN_STATES), 0, None
    if entry is None or entry <= _D0:
        return _D0, 1, 0, None
    unknown = 0
    with decimal_context():
        total = abs(entry * quantity)
        if close is None:
            unknown = int(_closed_trade(row) or state in {"FILLED", "OPEN", "UNKNOWN"})
        elif close <= _D0:
            unknown = 1
        else:
            total += abs(close * quantity)
    return total, unknown, 1, _currency(row, "quote_currency", "notional_currency")


def _turnover_metrics(facts: _TradeFacts, result: Mapping[str, Any]) -> dict[str, Any]:
    total = _D0
    unknown = 0
    executions = 0
    currencies: set[str] = set()
    for row in _rows(result.get("ledger")):
        row_total, row_unknown, row_executions, currency = _turnover_row(row)
        with decimal_context():
            total += row_total
        unknown += row_unknown
        executions += row_executions
        if currency:
            currencies.add(currency)
    complete = unknown == 0 and facts.unknown_cost_count == 0
    currency = next(iter(currencies)) if len(currencies) == 1 else None
    if executions == 0 and unknown == 0:
        return {
            "status": "NO_EXECUTION",
            "reason": "no_known_filled_quantity",
            "notional": _D0,
            "known_subtotal": _D0,
            "unknown_count": 0,
            "currency": currency,
            "basis": "executed_entry_and_close_price_times_filled_quantity",
        }
    return {
        "status": "ASSESSED" if complete else "PARTIAL_NOT_ASSESSED",
        "reason": None if complete else "turnover_or_cost_evidence_incomplete",
        "notional": total if complete else None,
        "known_subtotal": total,
        "unknown_count": unknown,
        "currency": currency,
        "basis": "executed_entry_and_close_price_times_filled_quantity",
    }


def _hhi(values: Sequence[Decimal]) -> Decimal | None:
    absolute = [abs(value) for value in values]
    with decimal_context():
        total = sum(absolute, _D0)
    if not absolute or total == _D0:
        return None
    with decimal_context():
        return sum(((value / total) ** 2 for value in absolute), _D0)


def _concentration_metrics(facts: _TradeFacts, result: Mapping[str, Any]) -> dict[str, Any]:
    trade_values = [item[1] for item in facts.known_net]
    daily, _unknown_dates = _daily_net(facts, result)
    day_values = list(daily.values())
    trade_hhi = _hhi(trade_values)
    day_hhi = _hhi(day_values)
    complete = facts.unknown_net_count == 0 and facts.unknown_cost_count == 0
    reason = None if complete else "unknown_trade_economics"
    trade_reason: str | None
    day_reason: str | None
    if trade_hhi is None:
        trade_status = "NOT_ASSESSED"
        trade_reason = "zero_absolute_net_pnl" if trade_values else "no_known_closed_trade_net_pnl"
    else:
        trade_status = "ASSESSED" if complete else "PARTIAL_NOT_ASSESSED"
        trade_reason = reason
    if day_hhi is None:
        day_status = "NOT_ASSESSED"
        day_reason = "zero_absolute_net_pnl" if day_values else "no_observed_net_pnl_day"
    else:
        day_status = "ASSESSED" if complete else "PARTIAL_NOT_ASSESSED"
        day_reason = reason
    basis = "HHI_of_absolute_net_pnl_shares"
    return {
        "trade": {
            "status": trade_status,
            "value": trade_hhi if complete else None,
            "known_subtotal": trade_hhi,
            "reason": trade_reason,
            "observations": len(trade_values),
            "basis": basis,
        },
        "day": {
            "status": day_status,
            "value": day_hhi if complete else None,
            "known_subtotal": day_hhi,
            "reason": day_reason,
            "observations": len(day_values),
            "basis": basis,
        },
        "basis": basis,
    }


def _concentration_json(concentration: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"basis": concentration.get("basis")}
    for name in ("trade", "day"):
        raw = _mapping(concentration.get(name))
        result[name] = {
            **dict(raw),
            "value": _decimal_text(raw.get("value")),
            "known_subtotal": _decimal_text(raw.get("known_subtotal")),
        }
    result["trade_hhi"] = result["trade"]["value"]
    result["day_hhi"] = result["day"]["value"]
    return result


def _worst_day_metrics(facts: _TradeFacts, result: Mapping[str, Any]) -> dict[str, Any]:
    if not facts.known_net:
        return {
            "status": "NOT_ASSESSED",
            "reason": "no_observed_closed_trade_day",
            "day": None,
            "pnl": None,
            "known_subtotal": {},
            "basis": "UTC_close_available_at_and_explicit_observed_calendar_days",
            "weekends_fabricated": False,
        }
    daily, unknown_dates = _daily_net(facts, result)
    if not daily:
        return {
            "status": "NOT_ASSESSED",
            "reason": "no_observed_closed_trade_day",
            "day": None,
            "pnl": None,
            "basis": "UTC_close_available_at_and_explicit_observed_calendar_days",
            "weekends_fabricated": False,
        }
    day, pnl = min(daily.items(), key=lambda item: (item[1], item[0]))
    complete = facts.unknown_net_count == 0 and facts.unknown_cost_count == 0 and unknown_dates == 0
    session_basis = "UTC_observed_calendar_days_only" if _observed_days(result) else "UTC_close_available_at_only"
    return {
        "status": "ASSESSED" if complete else "PARTIAL_NOT_ASSESSED",
        "reason": None if complete else "unknown_trade_economics_or_day",
        "day": day if complete else None,
        "pnl": pnl if complete else None,
        "known_subtotal": {key: _decimal_text(value) for key, value in daily.items()},
        "basis": "UTC_close_available_at_and_explicit_observed_calendar_days",
        "weekends_fabricated": False,
        "session_basis": session_basis,
    }


def _margin_metrics(result: Mapping[str, Any]) -> dict[str, Any]:
    raw = _mapping(result.get("margin"))
    source = {**_mapping(result), **raw}
    explicit = {
        key: _nonnegative_decimal(source.get(key)) for key in ("margin_level", "used_margin", "equity", "leverage")
    }
    if explicit["margin_level"] is not None:
        return {
            "status": "ASSESSED",
            "value": _decimal_text(explicit["margin_level"]),
            "margin_level": _decimal_text(explicit["margin_level"]),
            "used_margin": _decimal_text(explicit["used_margin"]),
            "leverage": _decimal_text(explicit["leverage"]),
            "reason": None,
            "basis": "explicit_margin_level_input",
        }
    if explicit["used_margin"] is not None and explicit["equity"] is not None and explicit["used_margin"] > _D0:
        with decimal_context():
            level = explicit["equity"] / explicit["used_margin"] * Decimal("100")
        return {
            "status": "ASSESSED",
            "value": _decimal_text(level),
            "margin_level": _decimal_text(level),
            "used_margin": _decimal_text(explicit["used_margin"]),
            "leverage": _decimal_text(explicit["leverage"]),
            "reason": None,
            "basis": "explicit_equity_divided_by_used_margin",
        }
    return {
        "status": "NOT_ASSESSED",
        "value": None,
        "margin_level": None,
        "used_margin": _decimal_text(explicit["used_margin"]),
        "leverage": _decimal_text(explicit["leverage"]),
        "reason": "margin_or_leverage_inputs_missing",
        "basis": "no_explicit_margin_or_leverage",
    }


def _partial_payload(
    drawdown: Mapping[str, Any],
    facts: _TradeFacts,
    net: Mapping[str, Any],
    worst: Mapping[str, Any],
    exposure: Mapping[str, Any],
    turnover: Mapping[str, Any],
    concentration: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "status": "KNOWN_SUBTOTAL_ONLY",
        "basis": "known_equity_marks_and_known_trade_economics_only",
        "max_drawdown_basis": "mark_to_market_equity",
        "unknown_mark_count": drawdown.get("unknown_mark_count", 0),
        "unknown_cost_count": facts.unknown_cost_count,
        "max_drawdown_abs_known": _decimal_text(drawdown.get("max_drawdown")),
        "max_drawdown_known": _decimal_text(drawdown.get("max_drawdown")),
        "recovery_duration_seconds_known": _decimal_text(drawdown.get("recovery_duration_seconds")),
        "unrecovered_duration_seconds_known": _decimal_text(drawdown.get("unrecovered_duration_seconds")),
        "unrecovered_state_known": drawdown.get("unrecovered_state"),
        "known_net_pnl_subtotal": _decimal_text(net.get("known_subtotal")),
        "known_expectancy": _decimal_text(net.get("known_expectancy")),
        "cost_total_known": _decimal_text(facts.known_cost_total),
        "cost_total_unknown": None if facts.unknown_cost_count else _decimal_text(_D0),
        "worst_observed_day_known": worst.get("known_subtotal"),
        "exposure_peak_known": _decimal_text(exposure.get("peak")),
        "exposure_duration_seconds_known": _decimal_text(exposure.get("duration_seconds")),
        "exposure_notional_time_known": _decimal_text(exposure.get("notional_time")),
        "turnover_notional_known": _decimal_text(turnover.get("known_subtotal")),
        "turnover_unknown_count": turnover.get("unknown_count", 0),
        "trade_concentration_hhi_known": _decimal_text(_mapping(concentration.get("trade")).get("known_subtotal")),
        "day_concentration_hhi_known": _decimal_text(_mapping(concentration.get("day")).get("known_subtotal")),
    }


def result_metrics(result: Mapping[str, Any]) -> dict[str, Any]:
    """Return explicit risk/performance metrics for one CFD result mapping.

    The primary fields are withheld whenever the result contains unknown mark
    or cost evidence.  ``partial`` retains known subtotals and labels the
    narrower evidence basis; it is never presented as a complete estimate.
    """

    if not isinstance(result, Mapping):
        raise TypeError("result_metrics requiere un Mapping")
    marks, unknown_marks, mark_reasons = _parse_marks(result)
    drawdown = _drawdown_metrics(marks)
    drawdown["unknown_mark_count"] = unknown_marks
    facts = _trade_facts(result)
    net = _net_metrics(facts)
    worst = _worst_day_metrics(facts, result)
    exposure = _exposure_metrics(marks, result)
    turnover = _turnover_metrics(facts, result)
    concentration = _concentration_metrics(facts, result)
    margin = _margin_metrics(result)
    unknown_costs = facts.unknown_cost_count
    evidence_complete = not unknown_marks and not unknown_costs
    drawdown_complete = evidence_complete and bool(marks)
    net_complete = evidence_complete and net.get("status") == "ASSESSED" and bool(facts.known_net)
    worst_complete = net_complete and worst.get("status") == "ASSESSED"
    exposure_complete = evidence_complete and exposure.get("status") == "ASSESSED"
    turnover_complete = evidence_complete and turnover.get("status") in {"ASSESSED", "NO_EXECUTION"}
    concentration_complete = net_complete and concentration.get("trade", {}).get("status") == "ASSESSED"
    complete = evidence_complete and drawdown_complete and net_complete
    reasons = list(mark_reasons)
    if unknown_costs:
        reasons.append("cost_evidence_unknown")
    if not marks:
        reasons.append("equity_marks_missing_or_unusable")
    if not facts.known_net:
        reasons.append("no_known_closed_trade_net_pnl")
    reasons = sorted(set(reasons))
    main = {
        "max_drawdown_abs": _decimal_text(drawdown.get("max_drawdown")) if drawdown_complete else None,
        "max_drawdown_peak_at": _iso(drawdown.get("max_drawdown_peak_at")) if drawdown_complete else None,
        "max_drawdown_trough_at": _iso(drawdown.get("max_drawdown_trough_at")) if drawdown_complete else None,
        "recovery_duration_seconds": (
            _decimal_text(drawdown.get("recovery_duration_seconds")) if drawdown_complete else None
        ),
        "max_recovery_duration_seconds": (
            _decimal_text(drawdown.get("recovery_duration_seconds")) if drawdown_complete else None
        ),
        "recovery_duration": (_decimal_text(drawdown.get("recovery_duration_seconds")) if drawdown_complete else None),
        "unrecovered_state": drawdown.get("unrecovered_state") if drawdown_complete else "NOT_ASSESSED",
        "unrecovered_since": _iso(drawdown.get("unrecovered_since")) if drawdown_complete else None,
        "unrecovered_duration_seconds": (
            _decimal_text(drawdown.get("unrecovered_duration_seconds")) if drawdown_complete else None
        ),
        "net_pnl": _decimal_text(net.get("net")) if net_complete else None,
        "expectancy": _decimal_text(net.get("expectancy")) if net_complete else None,
        "known_net_pnl_subtotal": _decimal_text(net.get("known_subtotal")),
        "known_expectancy": _decimal_text(net.get("known_expectancy")),
        "worst_utc_observed_day": (
            {"date": worst.get("day"), "pnl": _decimal_text(worst.get("pnl"))} if worst_complete else None
        ),
        "worst_utc_observed_day_basis": worst.get("basis"),
        "worst_session_basis": worst.get("session_basis"),
        "exposure_peak": _decimal_text(exposure.get("peak")) if exposure_complete else None,
        "exposure_peak_at": _iso(exposure.get("peak_at")) if exposure_complete else None,
        "exposure_peak_quantity": _decimal_text(exposure.get("peak_quantity")) if exposure_complete else None,
        "peak_exposure": _decimal_text(exposure.get("peak")) if exposure_complete else None,
        "peak_exposure_at": _iso(exposure.get("peak_at")) if exposure_complete else None,
        "peak_exposure_quantity": _decimal_text(exposure.get("peak_quantity")) if exposure_complete else None,
        "exposure_currency": exposure.get("currency"),
        "exposure_duration_seconds": (_decimal_text(exposure.get("duration_seconds")) if exposure_complete else None),
        "exposure_duration": _decimal_text(exposure.get("duration_seconds")) if exposure_complete else None,
        "exposure_notional_time": _decimal_text(exposure.get("notional_time")) if exposure_complete else None,
        "turnover_notional": _decimal_text(turnover.get("notional")) if turnover_complete else None,
        "turnover_notional_value": _decimal_text(turnover.get("notional")) if turnover_complete else None,
        "turnover_currency": turnover.get("currency"),
        "turnover_basis": turnover.get("basis"),
        "turnover_unknown_count": turnover.get("unknown_count", 0),
        "turnover_capital_ratio": None,
        "trade_concentration_hhi": (
            _decimal_text(_mapping(concentration.get("trade")).get("value")) if concentration_complete else None
        ),
        "concentration_trade_hhi": (
            _decimal_text(_mapping(concentration.get("trade")).get("value")) if concentration_complete else None
        ),
        "day_concentration_hhi": (
            _decimal_text(_mapping(concentration.get("day")).get("value")) if concentration_complete else None
        ),
        "concentration_day_hhi": (
            _decimal_text(_mapping(concentration.get("day")).get("value")) if concentration_complete else None
        ),
        "trade_concentration_hhi_known": _decimal_text(_mapping(concentration.get("trade")).get("known_subtotal")),
        "day_concentration_hhi_known": _decimal_text(_mapping(concentration.get("day")).get("known_subtotal")),
        "cost_total_known": _decimal_text(facts.known_cost_total),
        "cost_total_unknown": None if unknown_costs else _decimal_text(_D0),
        "cost_unknown_count": unknown_costs,
        "cost_total": {
            "known": _decimal_text(facts.known_cost_total),
            "unknown": None if unknown_costs else _decimal_text(_D0),
            "unknown_count": unknown_costs,
            "status": "ASSESSED" if not unknown_costs else "NOT_ASSESSED",
        },
    }
    partial = _partial_payload(drawdown, facts, net, worst, exposure, turnover, concentration)
    metrics: dict[str, Any] = {
        "metrics_version": 1,
        "status": "ASSESSED" if complete else "NOT_ASSESSED",
        "reason": None if complete else (";".join(reasons) or "insufficient_metric_evidence"),
        "reasons": reasons,
        **main,
        "max_drawdown_basis": "mark_to_market_equity",
        "drawdown": {
            "status": "ASSESSED" if drawdown_complete else "NOT_ASSESSED",
            "value": main["max_drawdown_abs"],
            "basis": "mark_to_market_equity",
            "calculation_basis": "ordered_aware_equity_mark_to_market_absolute",
            "peak_at": main["max_drawdown_peak_at"],
            "trough_at": main["max_drawdown_trough_at"],
        },
        "max_drawdown": {
            "status": "ASSESSED" if drawdown_complete else "NOT_ASSESSED",
            "value": main["max_drawdown_abs"],
            "basis": "mark_to_market_equity",
            "calculation_basis": "ordered_aware_equity_mark_to_market_absolute",
            "peak_at": main["max_drawdown_peak_at"],
            "trough_at": main["max_drawdown_trough_at"],
        },
        "partial_drawdown": {
            "status": "KNOWN_SUBTOTAL_ONLY",
            "value": partial["max_drawdown_abs_known"],
            "basis": "known_equity_marks_only",
            "unknown_mark_count": partial["unknown_mark_count"],
        },
        "recovery": {
            "duration_seconds": main["recovery_duration_seconds"],
            "unrecovered_duration_seconds": (
                _decimal_text(drawdown.get("unrecovered_duration_seconds")) if drawdown_complete else None
            ),
            "unrecovered_state": main["unrecovered_state"],
            "status": "ASSESSED" if drawdown_complete else "NOT_ASSESSED",
        },
        "net": {
            "value": main["net_pnl"],
            "expectancy": main["expectancy"],
            "known_subtotal": main["known_net_pnl_subtotal"],
            "status": net.get("status"),
        },
        "worst_day": {
            "value": main["worst_utc_observed_day"],
            "status": worst.get("status"),
            "basis": worst.get("basis"),
            "weekends_fabricated": False,
        },
        "worst_session": {
            "value": main["worst_utc_observed_day"],
            "status": worst.get("status"),
            "basis": worst.get("basis"),
            "session_basis": worst.get("session_basis"),
            "weekends_fabricated": False,
        },
        "daily_aggregate": {
            "status": worst.get("status"),
            "pnl_by_utc_day": worst.get("known_subtotal"),
            "worst_utc_observed_day": main["worst_utc_observed_day"],
            "basis": worst.get("basis"),
            "weekends_fabricated": False,
        },
        "exposure": {
            "peak": main["exposure_peak"],
            "at": main["exposure_peak_at"],
            "quantity": main["exposure_peak_quantity"],
            "duration_seconds": main["exposure_duration_seconds"],
            "notional_time": main["exposure_notional_time"],
            "quantity_status": exposure.get("quantity_status"),
            "currency": main["exposure_currency"],
            "status": exposure.get("status") if exposure_complete else "NOT_ASSESSED",
        },
        "turnover": {
            "notional": main["turnover_notional"],
            "currency": main["turnover_currency"],
            "basis": main["turnover_basis"],
            "unknown_count": main["turnover_unknown_count"],
            "capital_ratio": {
                "status": "NOT_ASSESSED",
                "value": None,
                "reason": "capital_or_margin_inputs_missing",
            },
            "status": turnover.get("status") if turnover_complete else "NOT_ASSESSED",
        },
        "costs": {
            "known_total": main["cost_total_known"],
            "unknown_total": main["cost_total_unknown"],
            "unknown_count": unknown_costs,
            "status": "ASSESSED" if not unknown_costs else "NOT_ASSESSED",
        },
        "concentration": _concentration_json(concentration),
        "margin": margin,
        "partial": partial,
        "basis": {
            "drawdown": "mark_to_market_equity",
            "drawdown_calculation": "ordered_aware_equity_mark_to_market_absolute",
            "worst_day": worst.get("basis"),
            "turnover": turnover.get("basis"),
            "concentration": "HHI_of_absolute_net_pnl_shares",
            "weekends_fabricated": False,
        },
    }
    return metrics


__all__ = ["result_metrics"]
