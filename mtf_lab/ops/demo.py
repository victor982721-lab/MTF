"""Offline deterministic end-to-end demonstration for MTF Lab."""

from __future__ import annotations

import dataclasses
import math
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from .backtest import BacktestRunner, VariantSpec
from .logging_state import OperationTelemetry
from .persistence import SQLiteStore
from .reporting import ReportBuilder
from .simulation import EvaluationSpec, VirtualContractSimulator


@dataclasses.dataclass(slots=True)
class DemoDataset:
    instrument: str
    seed: int
    m1: list[dict[str, Any]]
    m5: list[dict[str, Any]]
    m15: list[dict[str, Any]]
    signals: list[dict[str, Any]]
    issues: list[dict[str, Any]]

    @property
    def all_candles(self) -> list[dict[str, Any]]:
        return self.m1 + self.m5 + self.m15


def _aggregate(m1: list[dict[str, Any]], minutes: int) -> list[dict[str, Any]]:
    grouped: list[dict[str, Any]] = []
    for offset in range(0, len(m1), minutes):
        block = m1[offset:offset + minutes]
        if len(block) < minutes:
            break
        start = block[0]["start_ts"]
        end = block[-1]["end_ts"]
        grouped.append({
            "candle_id": f"demo-{minutes}m-{start}", "instrument": block[0]["instrument"], "timeframe": f"M{minutes}",
            "start_ts": start, "end_ts": end, "open": block[0]["open"], "high": max(x["high"] for x in block),
            "low": min(x["low"] for x in block), "close": block[-1]["close"], "volume": sum(x["volume"] for x in block),
            "closed": True, "source": "synthetic-demo", "price_base": "close", "quality": "SYNTHETIC_VALIDATED",
            "provenance": {"synthetic": True, "aggregation": f"[start,end), {minutes} x M1", "input": "demo-m1"},
        })
    return grouped


def generate_demo(seed: int = 42, *, minutes: int = 720, instrument: str = "DEMO/LOCAL") -> DemoDataset:
    """Generate trend, pullback, lateral and volatility-change segments.

    Values are deliberately synthetic and are never labelled as EUR/USD or a
    tradable quote.  Signals are deterministic demonstration records; the
    production detector can replace them without changing the simulator or
    persistence APIs.
    """
    rng = random.Random(seed)
    start = datetime(2025, 1, 2, 12, 0, tzinfo=UTC)
    price = 100.0
    m1: list[dict[str, Any]] = []
    for i in range(minutes):
        if i < minutes * 0.30:
            drift = 0.020
            wave = 0.10 * math.sin(i / 8)
        elif i < minutes * 0.55:
            drift = -0.028  # broad pullback/downward regime
            wave = 0.14 * math.sin(i / 6)
        elif i < minutes * 0.78:
            drift = 0.002  # lateral regime
            wave = 0.18 * math.sin(i / 4)
        else:
            drift = 0.018
            wave = 0.34 * math.sin(i / 3)  # volatility change
        noise = rng.gauss(0.0, 0.015 if i < minutes * 0.78 else 0.06)
        open_price = price
        close_price = max(0.01, price + drift + wave * 0.08 + noise)
        spread = abs(rng.gauss(0.035 if i < minutes * 0.78 else 0.12, 0.01))
        high = max(open_price, close_price) + spread
        low = min(open_price, close_price) - spread
        begin = start + timedelta(minutes=i)
        end = begin + timedelta(minutes=1)
        m1.append({
            "candle_id": f"demo-m1-{i:06d}", "instrument": instrument, "timeframe": "M1", "start_ts": begin.isoformat().replace("+00:00", "Z"), "end_ts": end.isoformat().replace("+00:00", "Z"),
            "open": round(open_price, 8), "high": round(high, 8), "low": round(low, 8), "close": round(close_price, 8), "volume": round(1 + rng.random(), 6),
            "closed": True, "source": "synthetic-demo", "price_base": "close", "quality": "SYNTHETIC_VALIDATED", "source_ordinal": i,
            "provenance": {"synthetic": True, "seed": seed, "scenario": "trend/pullback/lateral/volatility_change"},
        })
        price = close_price
    m5 = _aggregate(m1, 5); m15 = _aggregate(m1, 15)
    points = m1
    # These are reproducible candidate signal records used only to exercise
    # persistence and virtual evaluation.  They are not a claim that the
    # trend_pullback_v1 detector accepted these candidates.
    signals: list[dict[str, Any]] = []
    for ordinal, idx in enumerate(range(90, minutes - 10, 37)):
        direction = "UP" if ordinal % 2 == 0 else "DOWN"
        detection = points[idx]["end_ts"]
        signals.append({
            "signal_id": f"demo-signal-{ordinal:04d}", "episode_id": f"demo-episode-{idx // 37:04d}", "detected_ts": detection,
            "available_ts": detection, "instrument": instrument, "direction": direction, "status": "VALID_DEMO",
            "strategy": "trend_pullback_v1", "mode": "SYNTHETIC", "payload": {"synthetic": True, "candidate": True, "source_ordinal": idx},
        })
    issues = [
        {"row": minutes // 3, "code": "DUPLICATE_FIXTURE", "message": "fixture problemático disponible en metadata; no se usa para señales", "synthetic": True},
        {"row": minutes // 2, "code": "OUT_OF_ORDER_FIXTURE", "message": "fixture problemático disponible en metadata; no se usa para señales", "synthetic": True},
    ]
    return DemoDataset(instrument, seed, m1, m5, m15, signals, issues)


def run_demo(
    db_path: str | Path,
    *,
    seed: int = 42,
    minutes: int = 720,
    report_path: str | Path | None = None,
    log_path: str | Path | None = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Ejecuta el recorrido real: sintético -> temporalidades -> detector -> SQLite -> simulación.

    ``minutes`` es la duración virtual solicitada. Se usa al menos un bloque de
    600 M1 por escenario para que EMA50/RSI/ATR y el contexto M15 puedan
    calentarse sin esperar tiempo real. No se fabrican ticks desde OHLC.
    """
    # Importación local evita que el núcleo operativo tenga una dependencia
    # circular en tiempo de importación y mantiene este adaptador reemplazable.
    from ..pipeline import run_synthetic_pipeline

    requested_minutes = max(1, int(minutes))
    periods_each = max(600, (requested_minutes + 3) // 4)
    db = Path(db_path).expanduser()
    result = run_synthetic_pipeline(db, seed=int(seed), periods_each=periods_each, config=config, log_path=log_path)
    generated_report = db.with_name(f"{db.stem}-report.md")
    target_report = Path(report_path).expanduser() if report_path is not None else generated_report
    if target_report != generated_report:
        target_report.parent.mkdir(parents=True, exist_ok=True)
        target_report.write_text(generated_report.read_text(encoding="utf-8"), encoding="utf-8")
    return {
        "session_id": result.session_id,
        "db_path": str(db),
        "report_path": str(target_report),
        "log_path": str(Path(log_path).expanduser() if log_path is not None else db.with_suffix(".jsonl")),
        "dataset": {
            "seed": int(seed),
            "requested_virtual_minutes": requested_minutes,
            "periods_each_scenario": periods_each,
            "synthetic": True,
            "scenarios": ["trend", "pullback", "sideways", "volatility_change"],
            "candles": sum(len(x) for x in result.streams.values()),
            "m1": len(result.streams.get("M1", [])),
            "m5": len(result.streams.get("M5", [])),
            "m15": len(result.streams.get("M15", [])),
            "signals": len(result.strategy.signals),
            "evaluations": len(result.strategy.evaluations),
            "quality": result.dataset.quality.to_dict(),
            "provenance": result.dataset.provenance.to_dict(),
        },
        "results": result.simulations or [],
        "report": result.report or {},
    }
