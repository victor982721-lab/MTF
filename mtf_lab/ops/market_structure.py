"""Bounded descriptive statistics for real historical Bid/Ask quotes.

This is not a strategy selector. All quote statistics are tick-weighted unless
explicitly named otherwise; spread/ATR is sampled only when a causal bar closes.
Unknown session completeness, calendar coverage and volume stay unknown.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_FLOOR, Decimal
from typing import Any

from ..core.aggregation import CandleAggregator
from ..core.historical_calendar import HistoricalQuoteCalendar
from ..core.indicators import IncrementalIndicatorEngine
from ..core.models import Candle, EventKind, MarketEvent, OperationMode, PriceBase
from ..core.numeric import decimal_context
from ..data.historical import DatasetManifest, HistoricalQuote

STRUCTURE_SCHEMA = "mtf-lab.market-structure.v1"
TIMEFRAMES = ("M1", "M5", "M15", "H1", "H4", "D1")
MAX_DAYS = 8192
_ZERO = Decimal("0")
_PIP = Decimal("0.0001")


def _instant(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


@dataclass(slots=True)
class BoundedDistribution:
    """Exact moments and a fixed-width, explicitly bounded quantile histogram."""

    width: Decimal = Decimal("0.01")
    bins: int = 10000
    count: int = 0
    total: Decimal = _ZERO
    square_total: Decimal = _ZERO
    minimum: Decimal | None = None
    maximum: Decimal | None = None
    histogram: Counter[int] = field(default_factory=Counter)

    def add(self, value: Decimal) -> None:
        if not value.is_finite() or value < 0:
            raise ValueError("distribution requires a finite nonnegative value")
        with decimal_context():
            self.total += value
            self.square_total += value * value
            index = min(self.bins, int((value / self.width).to_integral_value(rounding=ROUND_FLOOR)))
        self.count += 1
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        self.histogram[index] += 1

    def _quantile(self, probability: Decimal) -> dict[str, Any] | None:
        if not self.count:
            return None
        rank = max(1, math.ceil(probability * self.count))
        cumulative = 0
        for index, count in sorted(self.histogram.items()):
            cumulative += count
            if cumulative >= rank:
                return {
                    "lower": str(index * self.width),
                    "upper_exclusive": str((index + 1) * self.width) if index < self.bins else None,
                    "overflow": index == self.bins,
                }
        raise AssertionError("histogram count mismatch")

    def to_dict(self) -> dict[str, Any]:
        with decimal_context():
            mean = self.total / self.count if self.count else None
            variance = max(_ZERO, self.square_total / self.count - mean * mean) if mean is not None else None
        return {
            "n": self.count,
            "mean": str(mean) if mean is not None else None,
            "variance_population": str(variance) if variance is not None else None,
            "min": str(self.minimum) if self.minimum is not None else None,
            "max": str(self.maximum) if self.maximum is not None else None,
            "quantiles": {
                name: self._quantile(Decimal(value)) for name, value in (("p50", ".5"), ("p90", ".9"), ("p99", ".99"))
            },
            "quantile_method": "fixed_width_histogram_interval_no_interpolation",
            "histogram_width": str(self.width),
            "overflow_count": self.histogram[self.bins],
        }


@dataclass(slots=True)
class _Returns:
    count: int = 0
    square_sum: float = 0.0
    previous_return: float | None = None
    paired: int = 0
    x: float = 0.0
    y: float = 0.0
    xx: float = 0.0
    yy: float = 0.0
    xy: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.square_sum += value * value
        previous = self.previous_return
        if previous is not None:
            self.paired += 1
            self.x += previous
            self.y += value
            self.xx += previous * previous
            self.yy += value * value
            self.xy += previous * value
        self.previous_return = value

    def to_dict(self) -> dict[str, Any]:
        denominator = (self.paired * self.xx - self.x * self.x) * (self.paired * self.yy - self.y * self.y)
        correlation = (self.paired * self.xy - self.x * self.y) / math.sqrt(denominator) if denominator > 0 else None
        return {
            "contiguous_log_returns": self.count,
            "sqrt_sum_squared_log_returns": math.sqrt(self.square_sum) if self.count else None,
            "lag_one_correlation": correlation,
            "paired_observations": self.paired,
            "interpretation": "DESCRIPTIVE_NOT_ENTRY_RULE_OR_EDGE",
        }


class _TimeframeStructure:
    def __init__(self, timeframe: str, max_quote_gap_seconds: float, calendar: HistoricalQuoteCalendar | None) -> None:
        self.timeframe = timeframe
        self.calendar = calendar
        self.aggregator = CandleAggregator(
            timeframe,
            instrument="EUR/USD",
            price_base=PriceBase.MID,
            mode=OperationMode.REPLAY,
            coverage_mode="continuous_quotes",
            max_quote_gap_seconds=max_quote_gap_seconds,
            max_seen_event_ids=2048,
            max_issues=32,
            historical_calendar=calendar,
        )
        self.indicators = IncrementalIndicatorEngine(max_points=2, max_issues=32, historical_calendar=calendar)
        self.bars = 0
        self.valid_bars = 0
        self.issues: Counter[str] = Counter()
        self.ratio = BoundedDistribution()
        self.ratio_by_hour = {str(hour): BoundedDistribution() for hour in range(24)}
        self.returns = _Returns()
        self.previous_bar: Candle | None = None

    def add(self, event: MarketEvent, spread: Decimal) -> None:
        result = self.aggregator.add(event)
        for issue in result.issues:
            self.issues[issue.code] += 1
        for bar in result.emitted:
            self._bar(bar, spread)

    def _bar(self, bar: Candle, spread: Decimal) -> None:
        self.bars += 1
        self.valid_bars += int(bar.quality.valid)
        point = self.indicators.update(bar)
        if point.atr is not None and point.atr > 0 and point.quality.valid:
            with decimal_context():
                ratio = spread / Decimal(str(point.atr))
            self.ratio.add(ratio)
            self.ratio_by_hour[str(bar.end.hour)].add(ratio)
        previous = self.previous_bar
        if bar.quality.valid and previous is not None and previous.quality.valid and previous.end == bar.start:
            self.returns.add(math.log(bar.close / previous.close))
        else:
            self.returns.previous_return = None
        self.previous_bar = bar

    def to_dict(self) -> dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "closed_bars": self.bars,
            "coverage_valid_bars": self.valid_bars,
            "issues": dict(sorted(self.issues.items())),
            "spread_atr": self.ratio.to_dict(),
            "spread_atr_by_close_hour_utc": {hour: value.to_dict() for hour, value in self.ratio_by_hour.items()},
            "spread_atr_basis": "current_quote_spread_divided_by_latest_just_closed_ATR14",
            "spread_atr_unit": "dimensionless",
            "returns": self.returns.to_dict(),
            "calendar_gap_policy": self.calendar.basis if self.calendar else "STRICT_UNKNOWN_GAPS_RESET_INDICATORS",
            "last_open_bar_excluded": True,
        }


def quote_event(quote: HistoricalQuote) -> MarketEvent:
    """Preserve causal availability and explicitly unknown receipt/volume."""
    return MarketEvent(
        instrument=quote.instrument,
        event_time=quote.event_time,
        available_at=quote.available_at,
        received_at=None,
        bid=float(quote.bid),
        ask=float(quote.ask),
        quantity=None,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        source=quote.source,
        mode=OperationMode.REPLAY,
        sequence=quote.sequence,
        source_event_id=quote.source_event_id,
    )


class _QuoteStructure:
    def __init__(self, max_quote_gap_seconds: float) -> None:
        self.bound = max_quote_gap_seconds
        self.count = 0
        self.first: datetime | None = None
        self.previous: HistoricalQuote | None = None
        self.equal_time_count = 0
        self.gaps_over_bound = 0
        self.largest_gaps: list[dict[str, Any]] = []
        self.spreads = BoundedDistribution()
        self.by_hour = {str(hour): BoundedDistribution() for hour in range(24)}
        self.by_session = {label: BoundedDistribution() for label in ("UTC_00_08", "UTC_08_16", "UTC_16_24")}
        self.days: dict[str, dict[str, Any]] = {}
        self.time_weighted_total = _ZERO
        self.covered_seconds = _ZERO

    def add(self, quote: HistoricalQuote) -> None:
        previous = self.previous
        if previous is not None:
            self._gap(previous, quote)
        self.count += 1
        self.first = self.first or quote.event_time
        with decimal_context():
            spread_pips = (quote.ask - quote.bid) / _PIP
        self.spreads.add(spread_pips)
        self.by_hour[str(quote.event_time.hour)].add(spread_pips)
        session = ("UTC_00_08", "UTC_08_16", "UTC_16_24")[quote.event_time.hour // 8]
        self.by_session[session].add(spread_pips)
        self._day(quote)
        self.previous = quote

    def _gap(self, previous: HistoricalQuote, quote: HistoricalQuote) -> None:
        if quote.event_time < previous.event_time or quote.sequence <= previous.sequence:
            raise ValueError("market structure requires source-order quotes")
        seconds = (quote.event_time - previous.event_time).total_seconds()
        self.equal_time_count += int(seconds == 0)
        if seconds > self.bound:
            self.gaps_over_bound += 1
            self.largest_gaps.append(
                {"from": _instant(previous.event_time), "to": _instant(quote.event_time), "seconds": seconds}
            )
            self.largest_gaps.sort(key=lambda row: -float(row["seconds"]))
            del self.largest_gaps[16:]
        else:
            with decimal_context():
                duration = Decimal(str(seconds))
                self.time_weighted_total += duration * (previous.ask - previous.bid) / _PIP
                self.covered_seconds += duration

    def _day(self, quote: HistoricalQuote) -> None:
        day = quote.event_time.date().isoformat()
        if day not in self.days:
            if len(self.days) >= MAX_DAYS:
                raise ValueError("descriptive session budget exceeded; split the authorized campaign")
            self.days[day] = {
                "date_utc": day,
                "ticks": 0,
                "first_quote": _instant(quote.event_time),
                "full_session": "UNKNOWN",
            }
        self.days[day]["ticks"] += 1
        self.days[day]["last_quote"] = _instant(quote.event_time)

    def to_dict(self) -> dict[str, Any]:
        with decimal_context():
            weighted = self.time_weighted_total / self.covered_seconds if self.covered_seconds else None
        return {
            "quote_count": self.count,
            "coverage_start": _instant(self.first),
            "coverage_end": _instant(self.previous.event_time) if self.previous else None,
            "equal_timestamp_successors_preserved": self.equal_time_count,
            "spread_unit": "pip = 0.0001 USD per EUR",
            "spread_tick_weighted": self.spreads.to_dict(),
            "spread_by_hour_utc": {hour: value.to_dict() for hour, value in self.by_hour.items()},
            "spread_by_session": {name: value.to_dict() for name, value in self.by_session.items()},
            "session_definition": "nonoverlapping fixed UTC descriptive blocks, not observed venue sessions",
            "spread_time_weighted_pips": str(weighted) if weighted is not None else None,
            "time_weighted_covered_seconds": str(self.covered_seconds),
            "time_weighted_model": "last-known quote, only consecutive gaps within explicit bound",
            "gaps_over_declared_bound": self.gaps_over_bound,
            "largest_gaps": self.largest_gaps,
            "gap_classification": "UNKNOWN; includes scheduled closures until calendar is accredited",
            "days_with_quotes": list(self.days.values()),
        }


def describe_market_structure(
    quotes: Iterable[HistoricalQuote],
    *,
    manifest: DatasetManifest,
    max_quote_gap_seconds: float = 90.0,
    historical_calendar: HistoricalQuoteCalendar | None = None,
) -> dict[str, Any]:
    """Consume quotes once with bounded tick retention and no output side effects."""
    if not math.isfinite(max_quote_gap_seconds) or max_quote_gap_seconds <= 0:
        raise ValueError("max_quote_gap_seconds must be finite and positive")
    stats = _QuoteStructure(max_quote_gap_seconds)
    frames = [_TimeframeStructure(name, max_quote_gap_seconds, historical_calendar) for name in TIMEFRAMES]
    for quote in quotes:
        stats.add(quote)
        event = quote_event(quote)
        with decimal_context():
            spread = quote.ask - quote.bid
        for timeframe in frames:
            timeframe.add(event, spread)
    return {
        "schema": STRUCTURE_SCHEMA,
        "dataset_id": manifest.dataset_id,
        "dataset_content_hash": manifest.content_hash,
        "source": "REAL_HISTORICAL_QUOTES_NOT_DEMO_FILLS",
        "availability": "HISTORICAL_EVENT_TIME_MODELED_NOT_RECEIPT",
        "volume": None,
        "max_quote_gap_seconds": max_quote_gap_seconds,
        "normalization_version": manifest.normalization_version,
        "calendar_identity": historical_calendar.to_dict() if historical_calendar else None,
        "quote_statistics": stats.to_dict(),
        "timeframes": {frame.timeframe: frame.to_dict() for frame in frames},
        "economic_conclusion": "NOT_ASSESSED_DESCRIPTIVE_ONLY",
        "profitability_claim": "NONE",
        "provider_comparison": "NOT_ASSESSED_SECOND_PROVIDER_NOT_ACQUIRED",
    }
