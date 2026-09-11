"""Deterministic cTrader protocol fixtures for offline tests and demos."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .ctrader_protocol import TREND_PERIODS


def synthetic_trendbar(
    *,
    timestamp_minutes: int,
    period: str = "M1",
    low_relative: int = 110_000,
    delta_open: int = 10,
    delta_close: int = 20,
    delta_high: int = 30,
    volume: int = 42,
) -> dict[str, Any]:
    """Return a protocol-shaped trendbar; never represents live market data."""

    period_name = period.upper()
    if period_name not in TREND_PERIODS:
        raise ValueError(f"period fixture no soportado: {period!r}")
    return {
        "period": TREND_PERIODS[period_name],
        "low": low_relative,
        "deltaOpen": delta_open,
        "deltaClose": delta_close,
        "deltaHigh": delta_high,
        "utcTimestampInMinutes": timestamp_minutes,
        "volume": volume,
        "synthetic_fixture": True,
    }


def synthetic_spot_event(
    *,
    timestamp_ms: int,
    symbol_id: int,
    bid_relative: int = 110_000,
    ask_relative: int = 110_020,
    trendbars: Sequence[Mapping[str, Any]] = (),
    snapshot: bool = False,
) -> dict[str, Any]:
    """Return a complete synthetic SpotEvent with explicit millisecond time."""

    return {
        "symbolId": symbol_id,
        "timestamp": timestamp_ms,
        "timestamp_unit": "ms",
        "bid": bid_relative,
        "ask": ask_relative,
        "trendbar": list(trendbars),
        "synthetic_fixture": True,
        "snapshot": snapshot,
    }


__all__ = ["synthetic_spot_event", "synthetic_trendbar"]
