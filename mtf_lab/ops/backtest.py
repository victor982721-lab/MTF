"""Backtest orchestration using the same causal simulation core as replay."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from .simulation import (
    DirectionalEvaluator,
    EvaluationSpec,
    Outcome,
    PricePoint,
    SimulationResult,
    VirtualContractSimulator,
    normalize_points,
    parse_ts,
)


def _row(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    if hasattr(value, "model_dump"):
        return dict(value.model_dump())
    if hasattr(value, "__dict__"):
        return {k: v for k, v in vars(value).items() if not k.startswith("_")}
    raise TypeError(f"expected record-like signal, got {type(value)!r}")


def _signal_ts(signal: Any) -> datetime:
    item = _row(signal)
    value = item.get("detected_ts", item.get("detected_at", item.get("timestamp", item.get("ts"))))
    if value is None:
        raise ValueError("signal has no detected timestamp")
    return parse_ts(value)


@dataclasses.dataclass(frozen=True, slots=True)
class VariantSpec:
    """A named, controlled strategy variant.

    ``signal_filter`` is deliberately the only optional hook here.  A caller
    can supply signals produced by the core strategy, or use ``signal_factory``
    on ``BacktestRunner.run_from_engine``.  The operational layer never
    reimplements indicator or strategy rules.
    """

    name: str
    description: str
    config: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    signal_filter: Callable[[Mapping[str, Any]], bool] | None = None
    mode: str = "MULTITIMEFRAME"

    @property
    def config_hash(self) -> str:
        text = json.dumps(dict(self.config), sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))
        return hashlib.sha256(text.encode()).hexdigest()

    def accepts(self, signal: Any) -> bool:
        return bool(self.signal_filter(_row(signal))) if self.signal_filter else True

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "description": self.description, "config": dict(self.config), "config_hash": self.config_hash, "mode": self.mode}


@dataclasses.dataclass(slots=True)
class BacktestResult:
    variant: str
    config_hash: str
    mode: str
    start_ts: str | None
    end_ts: str | None
    signal_count: int
    discarded_count: int
    coverage_count: int
    outcomes: dict[str, int]
    net_result: float
    gross_wins: float
    gross_losses: float
    max_drawdown: float | None
    unresolved_count: int
    independent_sample_count: int
    dependency_window_seconds: float
    simulations: list[SimulationResult] = dataclasses.field(default_factory=list)
    notes: list[str] = dataclasses.field(default_factory=list)

    @property
    def resolved_count(self) -> int:
        return self.outcomes.get(Outcome.WIN.value, 0) + self.outcomes.get(Outcome.LOSS.value, 0) + self.outcomes.get(Outcome.TIE.value, 0)

    @property
    def win_rate_on_resolved(self) -> float | None:
        return self.outcomes.get(Outcome.WIN.value, 0) / self.resolved_count if self.resolved_count else None

    def to_dict(self, *, include_simulations: bool = True) -> dict[str, Any]:
        data = {
            "variant": self.variant,
            "config_hash": self.config_hash,
            "mode": self.mode,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "signal_count": self.signal_count,
            "discarded_count": self.discarded_count,
            "coverage_count": self.coverage_count,
            "outcomes": dict(self.outcomes),
            "resolved_count": self.resolved_count,
            "unresolved_count": self.unresolved_count,
            "net_result": self.net_result,
            "gross_wins": self.gross_wins,
            "gross_losses": self.gross_losses,
            "max_drawdown": self.max_drawdown,
            "independent_sample_count": self.independent_sample_count,
            "dependency_window_seconds": self.dependency_window_seconds,
            "win_rate_on_resolved": self.win_rate_on_resolved,
            "notes": list(self.notes),
        }
        if include_simulations:
            data["simulations"] = [x.to_dict() for x in self.simulations]
        return data


def _fingerprint_points(points: Sequence[PricePoint]) -> str:
    h = hashlib.sha256()
    for p in points:
        h.update(f"{p.timestamp.isoformat()}|{p.price:.17g}|{p.base_price}|{p.source_ordinal}\n".encode())
    return h.hexdigest()


@dataclasses.dataclass(slots=True)
class PortfolioResult:
    """Cartera virtual opcional, separada de la evaluación independiente."""
    mode: str
    max_positions: int
    accepted_count: int
    skipped_overlap: int
    balance: float
    max_drawdown: float
    simulations: list[SimulationResult] = dataclasses.field(default_factory=list)
    notes: list[str] = dataclasses.field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode, "max_positions": self.max_positions,
            "accepted_count": self.accepted_count, "skipped_overlap": self.skipped_overlap,
            "balance": self.balance, "max_drawdown": self.max_drawdown,
            "simulations": [item.to_dict() for item in self.simulations], "notes": list(self.notes),
        }


class BacktestRunner:
    """Run controlled variants over one immutable data snapshot.

    The runner accepts precomputed core signals.  ``run_from_engine`` is an
    adapter for cores exposing ``process``/``update``/``on_event``; all
    resulting signals then flow through exactly the same evaluator used by
    live/replay.
    """

    def __init__(
        self,
        *,
        spec: EvaluationSpec | None = None,
        simulator: DirectionalEvaluator | VirtualContractSimulator | None = None,
        store: Any | None = None,
        session_id: str | None = None,
        dependency_window_seconds: float | None = None,
        logger: Any | None = None,
    ):
        self.spec = spec or EvaluationSpec()
        self.simulator = simulator or DirectionalEvaluator(self.spec)
        self.store = store
        self.session_id = session_id
        self.dependency_window_seconds = float(dependency_window_seconds if dependency_window_seconds is not None else max(self.spec.horizons_seconds))
        self.logger = logger

    def _simulate(self, signal: Any, points: Sequence[Any], *, horizon: float, simulation_id: str) -> SimulationResult:
        return self.simulator.evaluate(signal, points, horizon_seconds=horizon, simulation_id=simulation_id)

    @staticmethod
    def _boundary_filter(signals: Sequence[Any], boundary: datetime | None, *, partition: str, horizons: Sequence[float], latency: float) -> tuple[list[Any], int]:
        if boundary is None:
            return list(signals), 0
        selected: list[Any] = []
        excluded = 0
        max_horizon = max(horizons)
        for signal in signals:
            detected = _signal_ts(signal)
            # Exploration must finish before the boundary, including the
            # largest outcome horizon. Evaluation starts at/after boundary.
            if partition == "exploration":
                if detected + timedelta(seconds=latency + max_horizon) <= boundary:
                    selected.append(signal)
                else:
                    excluded += 1
            elif partition == "evaluation":
                if detected >= boundary:
                    selected.append(signal)
                else:
                    excluded += 1
            else:
                raise ValueError("partition must be exploration, evaluation, or all")
        return selected, excluded

    def run(
        self,
        signals: Iterable[Any],
        points: Iterable[Any],
        *,
        variants: Sequence[VariantSpec] | None = None,
        boundary: Any | None = None,
        partition: str = "all",
        discarded_count: int = 0,
        data_quality: str = "UNKNOWN",
        resolution: str = "UNKNOWN",
        persist: bool = True,
    ) -> list[BacktestResult]:
        signal_rows = list(signals)
        point_rows = normalize_points(points)
        variants = list(variants or [VariantSpec("trend_pullback_v1", "Hipótesis multitemporal configurable")])
        boundary_dt = parse_ts(boundary) if boundary is not None else None
        all_results: list[BacktestResult] = []
        for variant in variants:
            filtered = [signal for signal in signal_rows if variant.accepts(signal)]
            selected, excluded = self._boundary_filter(filtered, boundary_dt, partition=partition, horizons=self.spec.horizons_seconds, latency=self.spec.entry_latency_seconds)
            results: list[SimulationResult] = []
            for ordinal, signal in enumerate(sorted(selected, key=_signal_ts)):
                row = _row(signal)
                base_id = str(row.get("signal_id", row.get("id", f"signal-{ordinal}")))
                # Each variant gets an isolated simulation identity, allowing
                # the same source signal to be compared without collisions.
                for horizon in self.spec.horizons_seconds:
                    result = self._simulate(row, point_rows, horizon=horizon, simulation_id=f"{variant.name}:{base_id}:{horizon:g}")
                    results.append(result)
                    if self.store is not None and self.session_id and persist:
                        sim_row = result.to_dict()
                        # Conserva la variante y su hash junto a cada resultado
                        # para segmentar por configuración sin depender sólo del
                        # nombre de un archivo de reporte.
                        sim_row["variant"] = variant.name
                        sim_row["variant_config_hash"] = variant.config_hash
                        self.store.save_simulation(self.session_id, sim_row)
            counts = Counter(result.outcome.value for result in results)
            equity = 0.0; peak = 0.0; drawdown = 0.0
            for result in sorted((r for r in results if r.net_result is not None), key=lambda x: x.detected_ts):
                equity += float(result.net_result or 0)
                peak = max(peak, equity)
                drawdown = max(drawdown, peak - equity)
            groups: list[datetime] = []
            for signal in sorted(selected, key=_signal_ts):
                ts = _signal_ts(signal)
                if not groups or (ts - groups[-1]).total_seconds() > self.dependency_window_seconds:
                    groups.append(ts)
            start = min((_signal_ts(s) for s in selected), default=None)
            end = max((_signal_ts(s) for s in selected), default=None)
            notes = [
                "Resultados virtuales; no son órdenes ni validación de rentabilidad.",
                f"datos={_fingerprint_points(point_rows)[:16]} calidad={data_quality} resolución={resolution}",
                "La muestra bruta puede contener dependencia entre señales cercanas; se informa una agrupación conservadora.",
            ]
            if boundary_dt is not None:
                notes.append(f"partición={partition}; frontera={boundary_dt.isoformat()}; señales excluidas={excluded}")
            if not point_rows:
                notes.append("Sin puntos de precio: todos los resultados quedan INDETERMINATE.")
            result = BacktestResult(
                variant=variant.name,
                config_hash=variant.config_hash,
                mode="BACKTEST",
                start_ts=start.isoformat().replace("+00:00", "Z") if start else None,
                end_ts=end.isoformat().replace("+00:00", "Z") if end else None,
                signal_count=len(selected),
                discarded_count=int(discarded_count) + excluded,
                coverage_count=len(point_rows),
                outcomes={key: counts.get(key, 0) for key in ("WIN", "LOSS", "TIE", "INDETERMINATE")},
                net_result=equity,
                gross_wins=sum(max(0.0, float(r.net_result or 0)) for r in results),
                gross_losses=sum(min(0.0, float(r.net_result or 0)) for r in results),
                max_drawdown=drawdown if results else None,
                unresolved_count=counts.get("INDETERMINATE", 0),
                independent_sample_count=len(groups),
                dependency_window_seconds=self.dependency_window_seconds,
                simulations=results,
                notes=notes,
            )
            all_results.append(result)
            if self.logger:
                self.logger.info("backtest_variant_complete", variant=variant.name, metrics=result.to_dict(include_simulations=False))
        return all_results


    def run_portfolio(
        self,
        signals: Iterable[Any],
        points: Iterable[Any],
        *,
        horizon_seconds: float | None = None,
        initial_balance: float = 0.0,
        max_positions: int = 1,
    ) -> PortfolioResult:
        """Liquida una posición por señal con saldo fijo y solapamiento explícito.

        Esta cartera es sólo virtual; una señal que se solapa con el máximo de
        posiciones se cuenta como ``skipped_overlap`` y no se convierte en
        pérdida/ganancia. La evaluación independiente de :meth:`run` no cambia.
        """
        if max_positions < 1:
            raise ValueError("max_positions must be positive")
        balance = float(initial_balance)
        if not math.isfinite(balance):
            raise ValueError("initial_balance must be finite")
        normalized_points = normalize_points(points)
        active: list[datetime] = []
        accepted: list[SimulationResult] = []
        skipped = 0
        peak = balance
        drawdown = 0.0
        for ordinal, signal in enumerate(sorted(list(signals), key=_signal_ts)):
            result = self._simulate(signal, normalized_points, horizon=horizon_seconds if horizon_seconds is not None else self.spec.horizons_seconds[0], simulation_id=f"portfolio:{ordinal}")
            entry = parse_ts(result.entry_ts) if result.entry_ts else _signal_ts(signal)
            active = [expiry for expiry in active if expiry > entry]
            if len(active) >= max_positions:
                skipped += 1
                continue
            accepted.append(result)
            if result.net_result is not None:
                balance += float(result.net_result)
                peak = max(peak, balance)
                drawdown = max(drawdown, peak - balance)
            active.append(parse_ts(result.expiry_ts))
        return PortfolioResult("VIRTUAL_PORTFOLIO", max_positions, len(accepted), skipped, balance, drawdown, accepted, ["saldo y stake fijos; sin martingala", "las señales solapadas se omiten explícitamente", "no es ejecución ni conexión a broker"])

    def run_from_engine(
        self,
        events: Iterable[Any],
        engine: Any,
        points: Iterable[Any],
        *,
        variants: Sequence[VariantSpec] | None = None,
        persist_decisions: bool = True,
    ) -> list[BacktestResult]:
        """Feed events to a core engine without duplicating strategy logic.

        Supported engine method names are intentionally small and explicit:
        ``process_event``, ``update`` or ``on_event``.  The method may return
        one signal, an iterable of signals, a mapping with ``signals``, or
        ``None``.  A pre-existing ``engine.signals`` collection is also
        accepted after all events.  Unknown engines fail loudly.
        """
        method = next((getattr(engine, name, None) for name in ("process_event", "update", "on_event") if callable(getattr(engine, name, None))), None)
        if method is None:
            raise TypeError("engine must expose process_event, update or on_event")
        signals: list[Any] = []
        for ordinal, event in enumerate(events):
            returned = method(event)
            if returned is None:
                continue
            if isinstance(returned, Mapping) and "signals" in returned:
                returned = returned["signals"]
            if isinstance(returned, (str, bytes)):
                returned = [returned]
            try:
                signals.extend(list(returned))
            except TypeError:
                signals.append(returned)
            if self.store is not None and self.session_id and persist_decisions:
                decision = returned if isinstance(returned, Mapping) else {"observed_ts": _row(event).get("event_ts", _row(event).get("timestamp")), "status": "PROCESSED", "signals_emitted": len(signals), "ordinal": ordinal}
                self.store.save_decision(self.session_id, decision, ordinal=ordinal)
        if not signals and hasattr(engine, "signals"):
            signals = list(getattr(engine, "signals"))
        return self.run(signals, points, variants=variants)

    def partitioned_run(
        self,
        signals: Iterable[Any],
        points: Iterable[Any],
        *,
        boundary: Any,
        variants: Sequence[VariantSpec] | None = None,
    ) -> dict[str, list[BacktestResult]]:
        """Return leakage-safe exploration/evaluation result sets."""
        signal_rows = list(signals); point_rows = list(points)
        return {
            "exploration": self.run(signal_rows, point_rows, variants=variants, boundary=boundary, partition="exploration"),
            "evaluation": self.run(signal_rows, point_rows, variants=variants, boundary=boundary, partition="evaluation"),
        }
