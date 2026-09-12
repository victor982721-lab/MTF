"""Deterministic synthetic market data for offline demos and tests.

Synthetic records are intentionally tagged at both record and provenance level.
They are useful for exercising causal plumbing and quality handling only; they
must never be presented as observations of a real instrument.
"""

from __future__ import annotations

import random
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from .models import (
    Bar,
    DataSet,
    Provenance,
    ValidationIssue,
    infer_quality,
    resolution_name,
    resolution_to_seconds,
)

ScenarioName = Literal["trend", "pullback", "sideways", "volatility_change"]
_SUPPORTED_SCENARIOS = {"trend", "pullback", "sideways", "volatility_change"}
_SUPPORTED_ANOMALIES = {"gap", "duplicate", "out_of_order"}


@dataclass(frozen=True, slots=True)
class SyntheticConfig:
    """Configuration for one reproducible synthetic series."""

    seed: int = 7
    instrument: str = "SYNTH/USD"
    start: datetime = datetime(2024, 1, 2, tzinfo=UTC)
    resolution: int | str = "M1"
    periods: int = 240
    scenario: ScenarioName = "pullback"
    base_price: float = 100.0
    anomalies: tuple[str, ...] = ()
    gap_positions: tuple[int, ...] = ()
    duplicate_position: int | None = None
    out_of_order_positions: tuple[int, int] | None = None

    def __post_init__(self) -> None:
        if self.seed < 0:
            raise ValueError("seed must be non-negative")
        if self.start.tzinfo is None or self.start.utcoffset() is None:
            raise ValueError("start must include a timezone")
        if self.periods < 1:
            raise ValueError("periods must be positive")
        if self.scenario not in _SUPPORTED_SCENARIOS:
            raise ValueError(f"unsupported scenario: {self.scenario!r}")
        resolution_to_seconds(self.resolution)
        if self.base_price <= 0:
            raise ValueError("base_price must be positive")
        unknown = set(self.anomalies) - _SUPPORTED_ANOMALIES
        if unknown:
            raise ValueError(f"unknown synthetic anomalies: {sorted(unknown)}")


class SyntheticGenerator:
    """Generate reproducible bars with explicit scenario and anomaly labels.

    ``random.Random`` is local to a generator call, so generation does not
    mutate Python's process-global RNG.  Given the same configuration and
    Python's documented ``Random`` algorithm, output records and source IDs are
    deterministic.  The generated series is not a market-data fixture.
    """

    def __init__(
        self,
        seed: int = 7,
        *,
        instrument: str = "SYNTH/USD",
        start: datetime = datetime(2024, 1, 2, tzinfo=UTC),
        resolution: int | str = "M1",
    ) -> None:
        self.seed = seed
        self.instrument = instrument
        self.start = start
        self.resolution = resolution

    def generate(
        self,
        periods: int = 240,
        *,
        count: int | None = None,
        scenario: ScenarioName = "pullback",
        anomalies: Iterable[str] = (),
        gap_positions: Sequence[int] = (),
        duplicate_position: int | None = None,
        out_of_order_positions: tuple[int, int] | None = None,
        base_price: float = 100.0,
    ) -> DataSet:
        """Return a sequence-like :class:`DataSet` of synthetic ``Bar`` s.

        ``count`` is accepted as an alias for callers that use that common
        spelling.  Anomaly requests are explicit and are recorded in
        provenance; no gaps are filled with fabricated bars.
        """

        if count is not None:
            periods = count
        config = SyntheticConfig(
            seed=self.seed,
            instrument=self.instrument,
            start=self.start,
            resolution=self.resolution,
            periods=periods,
            scenario=scenario,
            base_price=base_price,
            anomalies=tuple(anomalies),
            gap_positions=tuple(gap_positions),
            duplicate_position=duplicate_position,
            out_of_order_positions=out_of_order_positions,
        )
        return self.generate_dataset(config)

    def generate_bars(self, *args: Any, **kwargs: Any) -> list[Bar]:
        """Convenience API returning only bars."""

        return list(self.generate(*args, **kwargs).bars)

    def generate_dataset(self, config: SyntheticConfig) -> DataSet:
        resolution_seconds = resolution_to_seconds(config.resolution)
        start = config.start.astimezone(UTC)
        rng = random.Random(config.seed)
        bars: list[Bar] = []
        close = float(config.base_price)
        for index in range(config.periods):
            drift, volatility, mean_reversion = self._scenario_parameters(
                config.scenario, index, config.periods, close, config.base_price
            )
            open_price = close
            shock = rng.gauss(0.0, volatility)
            if mean_reversion:
                shock += (config.base_price - close) * mean_reversion
            close = max(0.000001, open_price + drift + shock)
            wick = abs(rng.gauss(volatility * 0.65, volatility * 0.25))
            high = max(open_price, close) + wick
            low = max(0.000001, min(open_price, close) - abs(rng.gauss(volatility * 0.65, volatility * 0.25)))
            interval_start = start + timedelta(seconds=index * resolution_seconds)
            interval_end = interval_start + timedelta(seconds=resolution_seconds)
            volume = max(0.0, rng.lognormvariate(2.1, 0.28))
            trade_count = max(1, int(round(volume * 2.7)))
            bars.append(
                Bar(
                    instrument=config.instrument,
                    interval_start=interval_start,
                    interval_end=interval_end,
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    resolution_seconds=resolution_seconds,
                    volume=volume,
                    trade_count=trade_count,
                    price_basis="traded",
                    source="synthetic",
                    source_record_id=f"synthetic:{config.seed}:{config.scenario}:{index}",
                    received_at=interval_end,
                    available_at=interval_end,
                    closed=True,
                    synthetic=True,
                    metadata={
                        "scenario": config.scenario,
                        "seed": config.seed,
                        "mode": "SINTETICO",
                    },
                )
            )

        bars, anomaly_notes = self._apply_anomalies(bars, config)
        issues: list[ValidationIssue] = []
        for note in anomaly_notes:
            code = {
                "gap": "GAP",
                "duplicate": "DUPLICATE",
                "out_of_order": "OUT_OF_ORDER",
            }.get(note.split(":", 1)[0], "SYNTHETIC_ANOMALY")
            issues.append(ValidationIssue(code=code, message=note, severity="ERROR"))
        quality = infer_quality(bars, issues)
        times = [bar.interval_start for bar in bars]
        provenance = Provenance(
            provider="synthetic",
            mode="SYNTHETIC",
            instrument=config.instrument,
            resolutions=(resolution_name(config.resolution),),
            price_basis="traded",
            source_uri=f"synthetic://{config.scenario}?seed={config.seed}",
            coverage_start=min(times) if times else None,
            coverage_end=(max(bar.interval_end for bar in bars) if bars else None),
            generated_seed=config.seed,
            synthetic=True,
            retrieved_at=None,
            notes=(
                "SINTETICO: no representa cotizaciones reales ni evidencia de rentabilidad",
                f"scenario={config.scenario}",
                *anomaly_notes,
            ),
        )
        return DataSet(records=tuple(bars), provenance=provenance, quality=quality, issues=tuple(issues))

    def generate_scenarios(
        self,
        scenarios: Sequence[ScenarioName] = ("trend", "pullback", "sideways", "volatility_change"),
        *,
        periods_each: int = 240,
        gap: bool = False,
        duplicate: bool = False,
        out_of_order: bool = False,
    ) -> DataSet:
        """Generate a deterministic concatenated fixture for all scenarios.

        Scenarios are placed on disjoint, contiguous dates so their identity is
        unambiguous.  This helper is intended for demos, not for a composite
        market series or performance claims.
        """

        records: list[Bar] = []
        all_notes: list[str] = []
        for offset, scenario in enumerate(scenarios):
            scenario_seed = self.seed + offset * 1009
            scenario_start = self.start + timedelta(
                seconds=offset * periods_each * resolution_to_seconds(self.resolution)
            )
            generator = SyntheticGenerator(
                scenario_seed,
                instrument=self.instrument,
                start=scenario_start,
                resolution=self.resolution,
            )
            anomalies = tuple(
                name
                for name, enabled in (("gap", gap), ("duplicate", duplicate), ("out_of_order", out_of_order))
                if enabled
            )
            data = generator.generate(periods_each, scenario=scenario, anomalies=anomalies)
            records.extend(data.bars)
            all_notes.extend(
                [
                    f"{scenario}: {note}"
                    for note in data.provenance.notes
                    if note.startswith(("gap:", "duplicate:", "out_of_order:"))
                ]
            )
        issues = tuple(ValidationIssue(code="SYNTHETIC_SCENARIO", message=note, severity="ERROR") for note in all_notes)
        times = [bar.interval_start for bar in records]
        quality = infer_quality(records, issues)
        provenance = Provenance(
            provider="synthetic",
            mode="SYNTHETIC",
            instrument=self.instrument,
            resolutions=(resolution_name(self.resolution),),
            price_basis="traded",
            source_uri=f"synthetic://scenarios?seed={self.seed}",
            coverage_start=min(times) if times else None,
            coverage_end=max((bar.interval_end for bar in records), default=None),
            generated_seed=self.seed,
            synthetic=True,
            notes=("SINTETICO: conjunto compuesto para pruebas; no son cotizaciones reales", *all_notes),
        )
        return DataSet(tuple(records), provenance, quality, issues)

    @staticmethod
    def _scenario_parameters(
        scenario: str, index: int, periods: int, close: float, base: float
    ) -> tuple[float, float, float]:
        """Return drift, volatility and optional mean-reversion strength."""

        if scenario == "trend":
            return base * 0.00055, base * 0.00085, 0.0
        if scenario == "pullback":
            fraction = index / max(1, periods - 1)
            if fraction < 0.42:
                return base * 0.00065, base * 0.00065, 0.0
            if fraction < 0.67:
                return -base * 0.00085, base * 0.00075, 0.0
            return base * 0.00075, base * 0.00070, 0.0
        if scenario == "sideways":
            return 0.0, base * 0.00065, 0.025
        if scenario == "volatility_change":
            return base * 0.00025, base * (0.00035 if index < periods / 2 else 0.0018), 0.0
        raise ValueError(f"unsupported scenario: {scenario!r}")

    @staticmethod
    def _apply_anomalies(bars: list[Bar], config: SyntheticConfig) -> tuple[list[Bar], list[str]]:
        result = list(bars)
        notes: list[str] = []
        requested = set(config.anomalies)
        if "gap" in requested:
            positions = config.gap_positions or (max(1, len(result) // 3), max(2, len(result) // 3 + 1))
            removed = sorted({position for position in positions if 0 <= position < len(result)}, reverse=True)
            for position in removed:
                result.pop(position)
            notes.append(f"gap: removed interval positions {list(reversed(removed))}")
        if "duplicate" in requested and result:
            position = config.duplicate_position if config.duplicate_position is not None else len(result) // 2
            if 0 <= position < len(result):
                result.insert(position + 1, result[position])
                notes.append(f"duplicate: repeated source record at position {position}")
        if "out_of_order" in requested and len(result) >= 2:
            # If a duplicate was inserted at the default midpoint, skip it so
            # the anomaly really swaps two different intervals.
            default_left = len(result) // 2 + (
                1 if "duplicate" in requested and config.out_of_order_positions is None else 0
            )
            if default_left >= len(result) - 1:
                default_left = len(result) - 2
            pair = config.out_of_order_positions or (default_left, default_left + 1)
            left, right = pair
            if 0 <= left < len(result) and 0 <= right < len(result) and left != right:
                result[left], result[right] = result[right], result[left]
                notes.append(f"out_of_order: swapped positions {left} and {right}")
        return result, notes


# Name used by a few callers that prefer the longer spelling.
SyntheticDataGenerator = SyntheticGenerator
