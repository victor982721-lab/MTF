"""Deterministic cTrader PAPER scenario; never a market-data claim."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from .capture import CaptureContractError as CTraderPipelineError
from .ctrader import CTraderInstrumentSpec, synthetic_spot_event, synthetic_trendbar


def _utc(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise CTraderPipelineError(f"{name} requires timezone")
    return value.astimezone(UTC)


def _fixture_close_series(count: int) -> list[float]:
    if count < 130:
        raise CTraderPipelineError("el fixture cTrader requiere al menos 130 barras M1")
    closes: list[float] = []
    for index in range(100):
        closes.append(1.1000 + index * 0.0001)
    for _ in range(15):
        closes.append(closes[-1] + 0.0002)
    # Con indicadores pequeños este tramo forma contexto alcista, una
    # preparación M5 de retroceso y un cruce M1 auténtico en 02:02 UTC.
    closes.extend([1.1125, 1.1118, 1.1110, 1.1106, 1.1105, 1.1104, 1.1120, 1.1125, 1.1128, 1.1130])
    while len(closes) < count:
        closes.append(closes[-1] + 0.00015)
    return closes


def synthetic_ctrader_payloads(
    *,
    start: datetime = datetime(2026, 1, 1, tzinfo=UTC),
    symbol_id: int = 99,
    count: int = 190,
) -> tuple[Mapping[str, Any], ...]:
    """Fixture offline de SpotEvent + trendbar; no es mercado real."""

    start = _utc(start, name="start")
    spec = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=symbol_id)
    closes = _fixture_close_series(count)
    payloads: list[Mapping[str, Any]] = []
    scale = spec.price_scale
    for index, close in enumerate(closes):
        bar_start = start + timedelta(minutes=index)
        bar_end = bar_start + timedelta(minutes=1)
        previous = closes[index - 1] if index else close
        open_price = previous
        low_price = min(open_price, close) - 0.0001
        high_price = max(open_price, close) + 0.0001
        low_relative = int(round(low_price * scale))
        raw_bar = synthetic_trendbar(
            timestamp_minutes=int(bar_start.timestamp() // 60),
            period="M1",
            low_relative=low_relative,
            delta_open=int(round(open_price * scale)) - low_relative,
            delta_close=int(round(close * scale)) - low_relative,
            delta_high=int(round(high_price * scale)) - low_relative,
            volume=42,
        )
        bid_relative = int(round((close - 0.0001) * scale))
        ask_relative = int(round((close + 0.0001) * scale))
        payload = dict(
            synthetic_spot_event(
                timestamp_ms=int(bar_end.timestamp() * 1000),
                symbol_id=symbol_id,
                bid_relative=bid_relative,
                ask_relative=ask_relative,
                trendbars=(raw_bar,),
                snapshot=index == 0,
            )
        )
        # Source sequence is fixture provenance. Durable observation order lives
        # in CaptureEnvelope, not in this provider-specific payload field.
        payload["sequence"] = index
        payloads.append(payload)
    return tuple(payloads)
