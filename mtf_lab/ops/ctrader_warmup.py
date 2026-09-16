"""Causal cTrader history bootstrap for the read-only watch path.

The Open API history endpoint returns a bounded page rather than a proof that
the whole instrument history was downloaded.  Warmup therefore validates only
the suffix needed by the configured strategy: closed native bars, one fixed
cutoff, and an adjacent interval sequence.  ``has_more`` remains provenance
about the source query and is never relabeled as complete history.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..configuration import EffectiveConfig
from ..core.models import parse_timeframe
from ..data.ctrader_market import CTraderHistoryResult, CTraderProvider
from ..data.models import Bar, ensure_utc


class CTraderWarmupError(RuntimeError):
    """History cannot establish a causal warmup boundary."""


@dataclass(frozen=True, slots=True)
class CTraderWarmupResult:
    """Validated native-bar suffixes and source-query provenance."""

    cutoff: datetime
    required: Mapping[str, int]
    bars: Mapping[str, tuple[Bar, ...]]
    source: Mapping[str, Mapping[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "cutoff": self.cutoff.isoformat().replace("+00:00", "Z"),
            "required": dict(self.required),
            "timeframes": {
                timeframe: {
                    "bars": len(values),
                    "first": values[0].interval_start.isoformat().replace("+00:00", "Z") if values else None,
                    "last": values[-1].interval_start.isoformat().replace("+00:00", "Z") if values else None,
                    **dict(self.source.get(timeframe, {})),
                }
                for timeframe, values in self.bars.items()
            },
        }


def warmup_requirements(config: EffectiveConfig) -> dict[str, int]:
    """Derive the minimum closed bars needed by the configured strategy."""

    indicator_bars = max(
        config.indicators.ema_slow,
        config.indicators.rsi_period + 1,
        config.indicators.atr_period,
    )
    strategy = config.strategy
    return {
        parse_timeframe(strategy.context_timeframe).name: indicator_bars + strategy.context_lookback,
        parse_timeframe(strategy.preparation_timeframe).name: indicator_bars + strategy.preparation_lookback,
        parse_timeframe(strategy.trigger_timeframe).name: indicator_bars + 2,
    }


def _contiguous_suffix(bars: tuple[Bar, ...], *, timeframe: str, cutoff: datetime) -> tuple[Bar, ...]:
    candidates = [
        bar
        for bar in bars
        if bar.instrument
        and bar.resolution == timeframe
        and bar.price_basis == "native"
        and bar.closed
        and bar.available_at is not None
        and bar.available_at >= bar.interval_end
        and bar.interval_end <= cutoff
    ]
    candidates.sort(key=lambda item: (item.interval_start, item.revision))
    unique: list[Bar] = []
    seen: set[tuple[datetime, int]] = set()
    for bar in candidates:
        key = (bar.interval_start, int(bar.revision))
        if key in seen:
            continue
        seen.add(key)
        unique.append(bar)
    if not unique:
        return ()
    runs: list[list[Bar]] = [[unique[0]]]
    for bar in unique[1:]:
        previous = runs[-1][-1]
        if bar.interval_start == previous.interval_end:
            runs[-1].append(bar)
        else:
            runs.append([bar])
    return tuple(runs[-1])


def validate_history_result(
    result: CTraderHistoryResult,
    *,
    timeframe: str,
    required: int,
    instrument: str,
    cutoff: datetime,
) -> tuple[Bar, ...]:
    """Return a contiguous suffix or fail closed with an actionable reason."""

    cutoff = ensure_utc(cutoff, field_name="cutoff")
    normalized = str(timeframe).strip().upper()
    if result.timeframe != normalized:
        raise CTraderWarmupError(f"warmup {normalized}: respuesta timeframe={result.timeframe!r}")
    if result.issues:
        raise CTraderWarmupError(f"warmup {normalized}: historia con incidencias: {result.issues[0]}")
    bars = tuple(bar for bar in result.bars if bar.instrument == instrument)
    if len(bars) != len(result.bars):
        raise CTraderWarmupError(f"warmup {normalized}: instrumento incompatible")
    if any(bar.interval_end > cutoff for bar in bars):
        raise CTraderWarmupError(f"warmup {normalized}: historia posterior al cutoff causal")
    suffix = _contiguous_suffix(bars, timeframe=normalized, cutoff=cutoff)
    if len(suffix) < required:
        raise CTraderWarmupError(
            f"warmup {normalized}: {len(suffix)} barras contiguas cerradas; se requieren {required}"
        )
    # Keep the complete validated suffix, not only the minimum.  This leaves
    # an explicit source boundary for diagnostics while the coordinator owns
    # its configured candle retention.
    return suffix


def fetch_causal_warmup(
    provider: CTraderProvider,
    config: EffectiveConfig,
    *,
    cutoff: datetime | None = None,
    page_count: int = 2,
    max_pages: int = 20,
) -> CTraderWarmupResult:
    """Fetch and validate one causal native-bar suffix per strategy timeframe."""

    if isinstance(page_count, bool) or not isinstance(page_count, int) or page_count <= 0:
        raise CTraderWarmupError("warmup page_count debe ser entero positivo")
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages <= 0:
        raise CTraderWarmupError("warmup max_pages debe ser entero positivo")
    selected_cutoff = ensure_utc(cutoff or datetime.now(UTC), field_name="cutoff")
    required = warmup_requirements(config)
    bars_by_tf: dict[str, tuple[Bar, ...]] = {}
    source: dict[str, Mapping[str, Any]] = {}
    instrument = str(config.instrument).strip().upper().replace("-", "/")
    for timeframe, minimum in required.items():
        result = provider.fetch_history(
            timeframe,
            count=minimum + page_count,
            to_timestamp=selected_cutoff,
            max_pages=max_pages,
            stop_after_bars=minimum + page_count,
        )
        bars = validate_history_result(
            result,
            timeframe=timeframe,
            required=minimum,
            instrument=instrument,
            cutoff=selected_cutoff,
        )
        bars_by_tf[timeframe] = bars
        source[timeframe] = {
            "pages": result.pages,
            "source_has_more": result.has_more,
            "source_complete": result.complete,
            "issues": list(result.issues),
            "selected_contiguous": len(bars),
        }
    return CTraderWarmupResult(selected_cutoff, required, bars_by_tf, source)


__all__ = [
    "CTraderWarmupError",
    "CTraderWarmupResult",
    "fetch_causal_warmup",
    "validate_history_result",
    "warmup_requirements",
]
