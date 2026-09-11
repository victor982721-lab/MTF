"""Reference projections built from the canonical indicator series."""

from __future__ import annotations

import hashlib
from typing import Any

from .indicators import IndicatorPoint, IndicatorSeries


def _crosses_up(
    previous_close: float,
    previous_ema: float,
    point_close: float,
    point_ema: float,
    point_rsi: float,
    rsi_threshold: float,
) -> bool:
    return previous_close <= previous_ema and point_close > point_ema and point_rsi > rsi_threshold


def _crosses_down(
    previous_close: float,
    previous_ema: float,
    point_close: float,
    point_ema: float,
    point_rsi: float,
    rsi_threshold: float,
) -> bool:
    return previous_close >= previous_ema and point_close < point_ema and point_rsi < rsi_threshold


def _reference_direction(
    previous: IndicatorPoint,
    point: IndicatorPoint,
    rsi_threshold: float,
) -> str | None:
    previous_close = previous.close
    previous_ema = previous.ema_fast
    point_close = point.close
    point_ema = point.ema_fast
    point_rsi = point.rsi
    if previous_close is None:
        return None
    if previous_ema is None:
        return None
    if point_close is None:
        return None
    if point_ema is None:
        return None
    if point_rsi is None:
        return None
    if _crosses_up(previous_close, previous_ema, point_close, point_ema, point_rsi, rsi_threshold):
        return "UP"
    if _crosses_down(previous_close, previous_ema, point_close, point_ema, point_rsi, rsi_threshold):
        return "DOWN"
    return None


def _reference_candidate(
    series: IndicatorSeries,
    index: int,
    rsi_threshold: float,
) -> tuple[IndicatorPoint, str] | None:
    if index == 0:
        return None
    point = series.points[index]
    previous = series.points[index - 1]
    if not point.ready or not previous.ready:
        return None
    if previous.close is None or previous.ema_fast is None:
        return None
    if point.close is None or point.ema_fast is None or point.rsi is None:
        return None
    direction = _reference_direction(previous, point, rsi_threshold)
    return (point, direction) if direction is not None else None


def _reference_row(
    series: IndicatorSeries,
    point: IndicatorPoint,
    direction: str,
    *,
    rsi_threshold: float,
    mode: str,
    identity_salt: str,
) -> dict[str, Any]:
    detected = point.available_at or point.end
    token = (
        f"m1-reference|{series.instrument}|{point.start.isoformat()}|"
        f"{direction}|rsi>{rsi_threshold:g}|mode={mode}|salt={identity_salt}"
    )
    return {
        "signal_id": "m1ref_" + hashlib.sha256(token.encode()).hexdigest()[:32],
        "instrument": series.instrument,
        "direction": direction,
        "detected_at": detected,
        "timestamp": detected,
        "mode": mode,
        "status": "REFERENCE_M1",
        "available_at": detected,
        "values": {
            "close": point.close,
            "ema_fast": point.ema_fast,
            "rsi": point.rsi,
            "rsi_threshold": rsi_threshold,
            "variant": "m1_trigger_reference",
        },
        "quality": point.quality.status,
    }


def m1_reference_signals(
    series: IndicatorSeries,
    *,
    rsi_threshold: float = 50.0,
    mode: str = "REPLAY",
    identity_salt: str = "",
) -> list[dict[str, Any]]:
    """Return the controlled M1 EMA20/RSI reference projection.

    This is explicitly a reference variant, not a second implementation of
    ``trend_pullback_v1``. It consumes already-calculated indicator points and
    keeps its identity namespace independent from the detector and session.
    """

    rows: list[dict[str, Any]] = []
    for index, _point in enumerate(series.points):
        candidate = _reference_candidate(series, index, rsi_threshold)
        if candidate is None:
            continue
        point, direction = candidate
        rows.append(
            _reference_row(
                series,
                point,
                direction,
                rsi_threshold=rsi_threshold,
                mode=mode,
                identity_salt=identity_salt,
            )
        )
    return rows


__all__ = ["m1_reference_signals"]
