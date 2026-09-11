"""Consumidores explícitos para las señales del detector incremental.

El detector sólo decide. Cada consumidor recibe las señales y observaciones
causales que la aplicación le entrega, y devuelve deltas observables junto con
un checkpoint versionado. El consumidor binario es el comportamiento histórico
por defecto; los consumidores de registro y CFD no construyen el libro binario.

Las ventanas de eventos y señales son acotadas. Los contadores, el último
watermark y los deltas devueltos permiten a la aplicación persistir lo que
necesite sin depender de historiales ocultos en memoria.
"""

from __future__ import annotations

import hashlib
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

from ..core import Signal
from ..ops.simulation import Selection, select_price_point
from .state import (
    PendingSimulation,
    PriceObservation,
    SimulationConfig,
    iso,
    quality_label_is_usable,
    signal_dict,
    signal_from_dict,
)

CONSUMER_CHECKPOINT_VERSION = 1
_DEFAULT_MAX_RECORDS = 4096


def _bounded_limit(value: int, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} debe ser entero positivo")
    return value


def _aware_utc(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} debe ser datetime aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class SignalConsumerEvent:
    """Delta auditable de una transición de consumidor."""

    kind: str
    identity: str
    sequence: int
    timestamp: datetime | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        kind = str(self.kind).strip()
        identity = str(self.identity).strip()
        if not kind:
            raise ValueError("SignalConsumerEvent.kind no puede estar vacío")
        if not identity:
            raise ValueError("SignalConsumerEvent.identity no puede estar vacío")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 1:
            raise ValueError("SignalConsumerEvent.sequence debe ser entero positivo")
        timestamp = self.timestamp
        if timestamp is not None:
            timestamp = _aware_utc(timestamp, name="SignalConsumerEvent.timestamp")
        if not isinstance(self.payload, Mapping):
            raise ValueError("SignalConsumerEvent.payload debe ser mapping")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "identity", identity)
        object.__setattr__(self, "timestamp", timestamp)
        object.__setattr__(self, "payload", dict(self.payload))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "identity": self.identity,
            "sequence": self.sequence,
            "timestamp": iso(self.timestamp),
            "payload": dict(self.payload),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SignalConsumerEvent:
        if not isinstance(value, Mapping):
            raise TypeError("evento de consumidor debe ser mapping")
        timestamp = value.get("timestamp")
        return cls(
            kind=str(value.get("kind", "")),
            identity=str(value.get("identity", "")),
            sequence=int(value.get("sequence", 0)),
            timestamp=(datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")) if timestamp else None),
            payload=value.get("payload", {}),
        )


@dataclass(frozen=True, slots=True)
class SignalConsumerCheckpoint:
    """Snapshot versionado de un consumidor, separado del detector."""

    consumer_type: str
    state: Mapping[str, Any] = field(default_factory=dict)
    schema_version: int = CONSUMER_CHECKPOINT_VERSION

    def __post_init__(self) -> None:
        consumer_type = str(self.consumer_type).strip()
        if not consumer_type:
            raise ValueError("consumer_type no puede estar vacío")
        if self.schema_version != CONSUMER_CHECKPOINT_VERSION:
            raise ValueError("versión de checkpoint de consumidor no soportada")
        if not isinstance(self.state, Mapping):
            raise ValueError("state de consumidor debe ser mapping")
        object.__setattr__(self, "consumer_type", consumer_type)
        object.__setattr__(self, "state", dict(self.state))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "consumer_type": self.consumer_type,
            "state": dict(self.state),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SignalConsumerCheckpoint:
        if not isinstance(value, Mapping):
            raise TypeError("checkpoint de consumidor debe ser mapping")
        return cls(
            consumer_type=str(value.get("consumer_type", value.get("type", ""))),
            state=value.get("state", {}),
            schema_version=int(value.get("schema_version", 0)),
        )


@dataclass(frozen=True, slots=True)
class SignalConsumerResult:
    """Cambios de producto producidos por una transición."""

    events: tuple[SignalConsumerEvent, ...] = ()
    completed_simulations: tuple[PendingSimulation, ...] = ()
    pending_simulations: tuple[PendingSimulation, ...] = ()

    @property
    def simulations(self) -> tuple[PendingSimulation, ...]:
        return self.completed_simulations


@runtime_checkable
class SignalConsumer(Protocol):
    """Frontera mínima entre el detector y un producto de señales."""

    @property
    def consumer_type(self) -> str: ...

    def on_signal(self, signal: Signal) -> SignalConsumerResult: ...

    def on_observation(
        self,
        observation: PriceObservation,
        *,
        watermark: datetime,
    ) -> SignalConsumerResult: ...

    def advance(
        self,
        watermark: datetime,
        *,
        capture_complete: bool = False,
    ) -> SignalConsumerResult: ...

    def checkpoint(self) -> SignalConsumerCheckpoint: ...

    def restore(self, checkpoint: SignalConsumerCheckpoint | Mapping[str, Any]) -> None: ...


class RecordingSignalConsumer:
    """Registra señales y transiciones sin calcular ningún producto."""

    consumer_type = "recording"
    product = "SIGNAL_RECORDING"

    def __init__(
        self,
        *,
        max_events: int = _DEFAULT_MAX_RECORDS,
        max_signals: int = _DEFAULT_MAX_RECORDS,
    ) -> None:
        self.max_events = _bounded_limit(max_events, name="max_events")
        self.max_signals = _bounded_limit(max_signals, name="max_signals")
        self._events: deque[SignalConsumerEvent] = deque(maxlen=self.max_events)
        self._signals: deque[Signal] = deque(maxlen=self.max_signals)
        self._signal_ids: set[str] = set()
        self._signal_order: deque[str] = deque()
        self._sequence = 0
        self.signal_count = 0
        self.observation_count = 0
        self.advance_count = 0
        self._last_watermark: datetime | None = None

    @property
    def signals(self) -> tuple[Signal, ...]:
        return tuple(self._signals)

    @property
    def events(self) -> tuple[SignalConsumerEvent, ...]:
        return tuple(self._events)

    @property
    def pending_simulations(self) -> tuple[PendingSimulation, ...]:
        return ()

    @property
    def completed_simulations(self) -> tuple[PendingSimulation, ...]:
        return ()

    def _emit(
        self,
        kind: str,
        identity: str,
        *,
        timestamp: datetime | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> SignalConsumerEvent:
        self._sequence += 1
        event = SignalConsumerEvent(
            kind=kind,
            identity=identity,
            sequence=self._sequence,
            timestamp=timestamp,
            payload=payload or {},
        )
        self._events.append(event)
        return event

    def _remember_signal(self, signal: Signal) -> None:
        self._signals.append(signal)
        self._signal_ids.add(signal.signal_id)
        self._signal_order.append(signal.signal_id)
        while len(self._signal_order) > self.max_signals:
            self._signal_ids.discard(self._signal_order.popleft())

    def on_signal(self, signal: Signal) -> SignalConsumerResult:
        if not isinstance(signal, Signal):
            raise TypeError("on_signal requiere core.Signal")
        if signal.signal_id in self._signal_ids:
            return SignalConsumerResult()
        self._remember_signal(signal)
        self.signal_count += 1
        event = self._emit(
            "signal",
            signal.signal_id,
            timestamp=signal.detected_at,
            payload={
                "product": self.product,
                "instrument": signal.instrument,
                "direction": signal.direction,
                "detected_at": iso(signal.detected_at),
            },
        )
        return SignalConsumerResult(events=(event,))

    def on_observation(
        self,
        observation: PriceObservation,
        *,
        watermark: datetime,
    ) -> SignalConsumerResult:
        if not isinstance(observation, PriceObservation):
            raise TypeError("on_observation requiere PriceObservation")
        watermark = _aware_utc(watermark, name="watermark")
        self.observation_count += 1
        self._last_watermark = watermark
        event = self._emit(
            "observation",
            observation.identity,
            timestamp=observation.available_at,
            payload={
                "product": self.product,
                "instrument": observation.instrument,
                "quality": observation.quality,
                "available_at": iso(observation.available_at),
            },
        )
        return SignalConsumerResult(events=(event,))

    def advance(
        self,
        watermark: datetime,
        *,
        capture_complete: bool = False,
    ) -> SignalConsumerResult:
        watermark = _aware_utc(watermark, name="watermark")
        if not isinstance(capture_complete, bool):
            raise ValueError("capture_complete debe ser booleano")
        self.advance_count += 1
        self._last_watermark = watermark
        event = self._emit(
            "advance",
            f"advance:{self.advance_count}",
            timestamp=watermark,
            payload={"product": self.product, "capture_complete": capture_complete},
        )
        return SignalConsumerResult(events=(event,))

    def checkpoint(self) -> SignalConsumerCheckpoint:
        state = {
            "max_events": self.max_events,
            "max_signals": self.max_signals,
            "events": [event.to_dict() for event in self._events],
            "signals": [signal_dict(signal) for signal in self._signals],
            "signal_ids": list(self._signal_order),
            "sequence": self._sequence,
            "signal_count": self.signal_count,
            "observation_count": self.observation_count,
            "advance_count": self.advance_count,
            "last_watermark": iso(self._last_watermark),
        }
        return SignalConsumerCheckpoint(self.consumer_type, state)

    def _restore_signals(self, state: Mapping[str, Any]) -> None:
        for raw in state.get("signals", ()):
            if isinstance(raw, Mapping):
                signal = signal_from_dict(raw)
                self._signals.append(signal)
                self._signal_ids.add(signal.signal_id)
                self._signal_order.append(signal.signal_id)
        while len(self._signals) > self.max_signals:
            evicted = self._signals.popleft()
            self._signal_ids.discard(evicted.signal_id)
        restored_order = [str(item) for item in state.get("signal_ids", ()) if str(item) in self._signal_ids]
        self._signal_order = deque(restored_order or (signal.signal_id for signal in self._signals))
        while len(self._signal_order) > self.max_signals:
            self._signal_ids.discard(self._signal_order.popleft())

    def _restore_events(self, state: Mapping[str, Any]) -> None:
        for raw in state.get("events", ()):
            if isinstance(raw, Mapping):
                self._events.append(SignalConsumerEvent.from_mapping(raw))

    def _restore_counters(self, state: Mapping[str, Any]) -> None:
        self._sequence = int(state.get("sequence", 0))
        self.signal_count = int(state.get("signal_count", len(self._signals)))
        self.observation_count = int(state.get("observation_count", 0))
        self.advance_count = int(state.get("advance_count", 0))
        raw_watermark = state.get("last_watermark")
        self._last_watermark = (
            datetime.fromisoformat(str(raw_watermark).replace("Z", "+00:00")) if raw_watermark else None
        )

    def restore(
        self,
        checkpoint: SignalConsumerCheckpoint | Mapping[str, Any],
    ) -> None:
        snapshot = (
            checkpoint
            if isinstance(checkpoint, SignalConsumerCheckpoint)
            else SignalConsumerCheckpoint.from_mapping(checkpoint)
        )
        if snapshot.consumer_type != self.consumer_type:
            raise ValueError(f"checkpoint de {snapshot.consumer_type!r} incompatible con {self.consumer_type!r}")
        self._events.clear()
        self._signals.clear()
        self._signal_ids.clear()
        self._signal_order.clear()
        self._restore_signals(snapshot.state)
        self._restore_events(snapshot.state)
        self._restore_counters(snapshot.state)


class CFDSignalConsumer:
    """Sink estrecho de señales core para una aplicación CFD externa.

    La conversión de producto, las cotizaciones y la liquidación viven en el
    adaptador CFD que compone la aplicación. Este objeto sólo entrega cada
    señal nueva al sink explícito y mantiene contadores/eventos acotados para
    diagnóstico; nunca crea un simulador ni retiene un historial de señales.
    """

    consumer_type = "cfd_signal"
    product = "FOREX_CFD_LOCAL_PAPER"

    def __init__(
        self,
        *,
        max_events: int = _DEFAULT_MAX_RECORDS,
        signal_sink: Callable[[Signal], None] | None = None,
    ) -> None:
        self.max_events = _bounded_limit(max_events, name="max_events")
        self.signal_sink = signal_sink
        self._events: deque[SignalConsumerEvent] = deque(maxlen=self.max_events)
        self._sequence = 0
        self.signal_count = 0
        self.observation_count = 0
        self.advance_count = 0
        self._last_watermark: datetime | None = None

    @property
    def events(self) -> tuple[SignalConsumerEvent, ...]:
        return tuple(self._events)

    @property
    def pending_simulations(self) -> tuple[PendingSimulation, ...]:
        return ()

    @property
    def completed_simulations(self) -> tuple[PendingSimulation, ...]:
        return ()

    def _emit(
        self,
        kind: str,
        identity: str,
        *,
        timestamp: datetime | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> SignalConsumerEvent:
        self._sequence += 1
        event = SignalConsumerEvent(
            kind=kind,
            identity=identity,
            sequence=self._sequence,
            timestamp=timestamp,
            payload={"product": self.product, **dict(payload or {})},
        )
        self._events.append(event)
        return event

    def on_signal(self, signal: Signal) -> SignalConsumerResult:
        if not isinstance(signal, Signal):
            raise TypeError("on_signal requiere core.Signal")
        self.signal_count += 1
        if self.signal_sink is not None:
            self.signal_sink(signal)
        event = self._emit(
            "signal",
            signal.signal_id,
            timestamp=signal.detected_at,
            payload={
                "instrument": signal.instrument,
                "direction": signal.direction,
                "detected_at": iso(signal.detected_at),
            },
        )
        return SignalConsumerResult(events=(event,))

    def on_observation(
        self,
        observation: PriceObservation,
        *,
        watermark: datetime,
    ) -> SignalConsumerResult:
        if not isinstance(observation, PriceObservation):
            raise TypeError("on_observation requiere PriceObservation")
        watermark = _aware_utc(watermark, name="watermark")
        self.observation_count += 1
        self._last_watermark = watermark
        event = self._emit(
            "observation",
            observation.identity,
            timestamp=observation.available_at,
            payload={
                "instrument": observation.instrument,
                "quality": observation.quality,
                "available_at": iso(observation.available_at),
            },
        )
        return SignalConsumerResult(events=(event,))

    def advance(
        self,
        watermark: datetime,
        *,
        capture_complete: bool = False,
    ) -> SignalConsumerResult:
        watermark = _aware_utc(watermark, name="watermark")
        if not isinstance(capture_complete, bool):
            raise ValueError("capture_complete debe ser booleano")
        self.advance_count += 1
        self._last_watermark = watermark
        event = self._emit(
            "advance",
            f"advance:{self.advance_count}",
            timestamp=watermark,
            payload={"capture_complete": capture_complete},
        )
        return SignalConsumerResult(events=(event,))

    def checkpoint(self) -> SignalConsumerCheckpoint:
        state = {
            "max_events": self.max_events,
            "events": [event.to_dict() for event in self._events],
            "sequence": self._sequence,
            "signal_count": self.signal_count,
            "observation_count": self.observation_count,
            "advance_count": self.advance_count,
            "last_watermark": iso(self._last_watermark),
        }
        return SignalConsumerCheckpoint(self.consumer_type, state)

    def restore(
        self,
        checkpoint: SignalConsumerCheckpoint | Mapping[str, Any],
    ) -> None:
        snapshot = (
            checkpoint
            if isinstance(checkpoint, SignalConsumerCheckpoint)
            else SignalConsumerCheckpoint.from_mapping(checkpoint)
        )
        if snapshot.consumer_type != self.consumer_type:
            raise ValueError(f"checkpoint de {snapshot.consumer_type!r} incompatible con {self.consumer_type!r}")
        self._events.clear()
        for raw in snapshot.state.get("events", ()):
            if isinstance(raw, Mapping):
                self._events.append(SignalConsumerEvent.from_mapping(raw))
        state = snapshot.state
        self._sequence = int(state.get("sequence", 0))
        self.signal_count = int(state.get("signal_count", 0))
        self.observation_count = int(state.get("observation_count", 0))
        self.advance_count = int(state.get("advance_count", 0))
        raw_watermark = state.get("last_watermark")
        self._last_watermark = (
            datetime.fromisoformat(str(raw_watermark).replace("Z", "+00:00")) if raw_watermark else None
        )


class _SimulationBook:
    """Libro causal de evaluaciones virtuales aún no vencidas.

    The book shares the batch selector's availability/base/quality policy.  A
    live watermark only observes what is known so far; it never converts a
    missing observation into ``INDETERMINATE`` until the caller explicitly
    declares the capture complete.
    """

    def __init__(self, config: SimulationConfig, *, max_observations: int = 4096) -> None:
        self.config = config
        self.pending: dict[str, PendingSimulation] = {}
        self.completed: dict[str, PendingSimulation] = {}
        self.completed_ids: set[str] = set()
        self.completed_order: deque[str] = deque()
        self.observations: list[PriceObservation] = []
        self._observation_ids: set[str] = set()
        self.max_observations = max(256, int(max_observations))

    def add_signal(self, signal: Signal) -> tuple[PendingSimulation, ...]:
        created: list[PendingSimulation] = []
        for horizon in self.config.horizons_seconds:
            sim_id = "sim_" + hashlib.sha256(f"{signal.signal_id}|{horizon:g}".encode()).hexdigest()[:32]
            if sim_id in self.pending or sim_id in self.completed or sim_id in self.completed_ids:
                continue
            detected = signal.detected_at
            entry_due = detected + timedelta(seconds=self.config.entry_latency_seconds)
            expiry_origin = entry_due if self.config.horizon_from == "entry" else detected
            expiry = expiry_origin + timedelta(seconds=horizon)
            item = PendingSimulation(
                simulation_id=sim_id,
                signal_id=signal.signal_id,
                instrument=signal.instrument,
                direction=signal.direction,
                horizon_seconds=horizon,
                detected_at=detected,
                detection_available_at=detected,
                entry_due_at=entry_due,
                expiry_at=expiry,
                entry_rule=self.config.entry_rule,
                exit_rule=self.config.exit_rule,
                price_base=self.config.requested_base_price,
                resolution=self.config.resolution,
                quality=signal.quality.status,
                capture_complete=False,
            )
            self.pending[sim_id] = item
            created.append(item)
        return tuple(created)

    def _eligible(self, observation: PriceObservation) -> bool:
        return bool(observation.closed and quality_label_is_usable(observation.quality))

    def _select_entry(
        self, item: PendingSimulation, watermark: datetime
    ) -> tuple[Selection[PriceObservation] | None, str | None]:
        return select_price_point(
            self.observations,
            item.entry_due_at,
            rule="first_observation_at_or_after",
            requested_base_price=item.price_base,
            require_closed=True,
            max_price_age_seconds=self.config.max_price_age_seconds,
            as_of=watermark,
            instrument=item.instrument,
        )

    def _select_final(
        self, item: PendingSimulation, watermark: datetime
    ) -> tuple[Selection[PriceObservation] | None, str | None]:
        cutoff = watermark
        if item.exit_rule == "last_observation_at_or_before":
            cutoff = min(watermark, item.expiry_at + timedelta(seconds=self.config.max_price_age_seconds))
        return select_price_point(
            self.observations,
            item.expiry_at,
            rule=item.exit_rule,
            requested_base_price=item.price_base,
            require_closed=True,
            max_price_age_seconds=self.config.max_price_age_seconds,
            as_of=cutoff,
            exclude_identity=(item.final_observation.identity if item.final_observation else None),
            exclude_market_time=item.entry_at if item.entry_at is not None else None,
            instrument=item.instrument,
        )

    def _resolve(
        self,
        item: PendingSimulation,
        *,
        outcome: str,
        net_result: float | None,
        final: PriceObservation | None = None,
        reason: str | None = None,
        capture_complete: bool = False,
    ) -> PendingSimulation:
        updated = replace(
            item,
            status="RESOLVED" if outcome in {"WIN", "LOSS", "TIE"} else "INDETERMINATE",
            final_at=final.available_at if final else item.final_at,
            final_price=final.price if final else item.final_price,
            outcome=outcome,
            net_result=net_result,
            quality=final.quality if final else item.quality,
            reason=reason,
            final_observation=final,
            capture_complete=capture_complete,
        )
        self.pending.pop(item.simulation_id, None)
        self.completed[item.simulation_id] = updated
        self.completed_ids.add(item.simulation_id)
        self.completed_order.append(item.simulation_id)
        while len(self.completed_order) > self.max_observations:
            evicted_id = self.completed_order.popleft()
            self.completed_ids.discard(evicted_id)
            self.completed.pop(evicted_id, None)
        return updated

    def _settle_with_final(self, item: PendingSimulation, observation: PriceObservation) -> PendingSimulation:
        assert item.entry_price is not None
        delta = observation.price - item.entry_price
        if abs(delta) <= self.config.tie_tolerance:
            outcome = "TIE"
            net = self.config.tie_net - self.config.costs
        else:
            won = delta > 0 if item.direction.upper() == "UP" else delta < 0
            outcome = "WIN" if won else "LOSS"
            net = (
                (self.config.stake * self.config.payout_net - self.config.costs)
                if won
                else (-self.config.stake * self.config.loss_amount - self.config.costs)
            )
        return self._resolve(
            item, outcome=outcome, net_result=net, final=observation, reason=None, capture_complete=False
        )

    def _refresh_item(self, item: PendingSimulation, watermark: datetime) -> PendingSimulation | None:
        """Update entry/final candidates using only observations available now."""
        if item.entry_price is None:
            selection, _reason = self._select_entry(item, watermark)
            if selection is not None:
                entry = selection.point
                expiry = (selection.use_time if self.config.horizon_from == "entry" else item.detected_at) + timedelta(
                    seconds=item.horizon_seconds
                )
                item = replace(
                    item,
                    entry_at=selection.use_time,
                    entry_price=float(entry.price),
                    expiry_at=expiry,
                    quality=str(entry.quality),
                )
                self.pending[item.simulation_id] = item
        if item.entry_price is not None:
            selection, _reason = self._select_final(item, watermark)
            if selection is not None:
                final = selection.point
                # For `last_observation_at_or_before`, the selector is already
                # latest-at-cutoff. For `first...after`, the first market point
                # is stable once its availability is visible.
                item = replace(
                    item,
                    final_observation=final,
                    final_price=float(final.price),
                    final_at=final.available_at,
                    quality=str(final.quality),
                )
                self.pending[item.simulation_id] = item
        return item

    def _final_deadline_reached(self, item: PendingSimulation, watermark: datetime) -> bool:
        return watermark >= item.expiry_at + timedelta(seconds=self.config.max_price_age_seconds)

    def _remember_observation(self, observation: PriceObservation) -> None:
        if not self._eligible(observation) or observation.identity in self._observation_ids:
            return
        self._observation_ids.add(observation.identity)
        self.observations.append(observation)
        if len(self.observations) <= self.max_observations:
            return
        removed = self.observations[: -self.max_observations]
        self.observations = self.observations[-self.max_observations :]
        for item in removed:
            self._observation_ids.discard(item.identity)

    def _observed_completion(
        self,
        item: PendingSimulation,
        watermark: datetime,
    ) -> PendingSimulation | None:
        item = self._refresh_item(item, watermark) or item
        if item.entry_price is None:
            return None
        final = item.final_observation
        if final is not None and item.exit_rule == "first_observation_at_or_after":
            if final.timestamp >= item.expiry_at:
                return self._settle_with_final(item, final)
            return None
        if item.exit_rule != "last_observation_at_or_before":
            return None
        if final is None or not self._final_deadline_reached(item, watermark):
            return None
        return self._settle_with_final(item, final)

    def observe(
        self, observation: PriceObservation, *, watermark: datetime | None = None
    ) -> tuple[PendingSimulation, ...]:
        """Consume one observation; never terminalize an open capture."""
        watermark = (watermark or observation.available_at).astimezone(UTC)
        self._remember_observation(observation)
        completed = []
        for item in tuple(self.pending.values()):
            resolved = self._observed_completion(item, watermark)
            if resolved is not None:
                completed.append(resolved)
        return tuple(completed)

    def _missing_entry(
        self,
        item: PendingSimulation,
        deadline: bool,
        capture_complete: bool,
    ) -> PendingSimulation | None:
        if not deadline or not capture_complete:
            return None
        outcome, reason = self.config.missing_outcome("ENTRY_PRICE_NOT_AVAILABLE", capture_complete=True)
        return self._resolve(
            item,
            outcome=outcome,
            net_result=None,
            reason=reason,
            capture_complete=True,
        )

    def _final_completion(
        self,
        item: PendingSimulation,
        deadline: bool,
        capture_complete: bool,
    ) -> PendingSimulation | None:
        final = item.final_observation
        if final is None:
            return None
        if item.exit_rule == "first_observation_at_or_after":
            if final.timestamp >= item.expiry_at:
                return self._settle_with_final(item, final)
            return None
        if item.exit_rule == "last_observation_at_or_before" and (deadline or capture_complete):
            return self._settle_with_final(item, final)
        return None

    def _advanced_completion(
        self,
        item: PendingSimulation,
        watermark: datetime,
        *,
        capture_complete: bool,
    ) -> PendingSimulation | None:
        item = self._refresh_item(item, watermark) or item
        deadline = self._final_deadline_reached(item, watermark)
        if item.entry_price is None:
            return self._missing_entry(item, deadline, capture_complete)
        completed = self._final_completion(item, deadline, capture_complete)
        if completed is not None:
            return completed
        if not capture_complete:
            return None
        outcome, reason = self.config.missing_outcome("FINAL_PRICE_NOT_AVAILABLE_WITHIN_MAX_AGE", capture_complete=True)
        return self._resolve(
            item,
            outcome=outcome,
            net_result=None,
            reason=reason,
            capture_complete=True,
        )

    def advance(self, watermark: datetime, *, capture_complete: bool = True) -> tuple[PendingSimulation, ...]:
        """Advance virtual time without inventing observations."""
        watermark = watermark.astimezone(UTC)
        completed = []
        for item in tuple(self.pending.values()):
            resolved = self._advanced_completion(item, watermark, capture_complete=capture_complete)
            if resolved is not None:
                completed.append(resolved)
        return tuple(completed)


class BinarySimulationConsumer:
    """Consumidor que conserva el comportamiento binario histórico."""

    consumer_type = "binary_simulation"
    product = "BINARY_VIRTUAL_CONTRACT"

    def __init__(
        self,
        config: SimulationConfig,
        *,
        max_observations: int = _DEFAULT_MAX_RECORDS,
    ) -> None:
        if not isinstance(config, SimulationConfig):
            raise TypeError("config debe ser SimulationConfig")
        self.config = config
        self.book = _SimulationBook(config, max_observations=max_observations)
        self._sequence = 0
        self._events: deque[SignalConsumerEvent] = deque(maxlen=max(256, int(max_observations)))

    @property
    def pending_simulations(self) -> tuple[PendingSimulation, ...]:
        return tuple(self.book.pending.values())

    @property
    def completed_simulations(self) -> tuple[PendingSimulation, ...]:
        return tuple(self.book.completed.values())

    @property
    def observations(self) -> tuple[PriceObservation, ...]:
        return tuple(self.book.observations)

    @property
    def events(self) -> tuple[SignalConsumerEvent, ...]:
        return tuple(self._events)

    def _emit(
        self,
        kind: str,
        identity: str,
        *,
        timestamp: datetime | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> SignalConsumerEvent:
        self._sequence += 1
        event = SignalConsumerEvent(
            kind=kind,
            identity=identity,
            sequence=self._sequence,
            timestamp=timestamp,
            payload={"product": self.product, **dict(payload or {})},
        )
        self._events.append(event)
        return event

    def on_signal(self, signal: Signal) -> SignalConsumerResult:
        if not isinstance(signal, Signal):
            raise TypeError("on_signal requiere core.Signal")
        pending = self.book.add_signal(signal)
        event = self._emit(
            "signal",
            signal.signal_id,
            timestamp=signal.detected_at,
            payload={"created_simulations": len(pending)},
        )
        return SignalConsumerResult(events=(event,), pending_simulations=self.pending_simulations)

    def on_observation(
        self,
        observation: PriceObservation,
        *,
        watermark: datetime,
    ) -> SignalConsumerResult:
        if not isinstance(observation, PriceObservation):
            raise TypeError("on_observation requiere PriceObservation")
        watermark = _aware_utc(watermark, name="watermark")
        completed = self.book.observe(observation, watermark=watermark)
        event = self._emit(
            "observation",
            observation.identity,
            timestamp=observation.available_at,
            payload={"completed_simulations": len(completed)},
        )
        return SignalConsumerResult(
            events=(event,),
            completed_simulations=tuple(completed),
            pending_simulations=self.pending_simulations,
        )

    def advance(
        self,
        watermark: datetime,
        *,
        capture_complete: bool = False,
    ) -> SignalConsumerResult:
        watermark = _aware_utc(watermark, name="watermark")
        if not isinstance(capture_complete, bool):
            raise ValueError("capture_complete debe ser booleano")
        completed = self.book.advance(watermark, capture_complete=capture_complete)
        event = self._emit(
            "advance",
            f"advance:{self._sequence + 1}",
            timestamp=watermark,
            payload={
                "capture_complete": capture_complete,
                "completed_simulations": len(completed),
            },
        )
        return SignalConsumerResult(
            events=(event,),
            completed_simulations=tuple(completed),
            pending_simulations=self.pending_simulations,
        )

    def checkpoint(self) -> SignalConsumerCheckpoint:
        state = {
            "config": self.config.to_dict(),
            "max_observations": self.book.max_observations,
            "pending_simulations": [item.to_dict() for item in self.book.pending.values()],
            "completed_simulations": [item.to_dict() for item in self.book.completed.values()],
            "completed_simulation_ids": list(self.book.completed_order),
            "simulation_observations": [item.to_dict() for item in self.book.observations],
            "sequence": self._sequence,
            "events": [event.to_dict() for event in self._events],
        }
        return SignalConsumerCheckpoint(self.consumer_type, state)

    def _restore_book(self, state: Mapping[str, Any]) -> None:
        self.book.pending = {
            item.simulation_id: item
            for item in (PendingSimulation.from_dict(raw) for raw in state.get("pending_simulations", ()))
        }
        completed = [PendingSimulation.from_dict(raw) for raw in state.get("completed_simulations", ())]
        self.book.completed = {item.simulation_id: item for item in completed}
        self.book.completed_ids = {str(item) for item in state.get("completed_simulation_ids", self.book.completed)}
        self.book.completed_ids.update(self.book.completed)
        order = state.get("completed_simulation_ids", self.book.completed)
        self.book.completed_order = deque(str(item) for item in order if str(item) in self.book.completed)
        self.book.observations = [PriceObservation.from_dict(raw) for raw in state.get("simulation_observations", ())][
            -self.book.max_observations :
        ]
        self.book._observation_ids = {item.identity for item in self.book.observations}

    def _restore_events(self, state: Mapping[str, Any]) -> None:
        self._sequence = int(state.get("sequence", 0))
        self._events.clear()
        for raw in state.get("events", ()):
            if isinstance(raw, Mapping):
                self._events.append(SignalConsumerEvent.from_mapping(raw))

    def restore(
        self,
        checkpoint: SignalConsumerCheckpoint | Mapping[str, Any],
    ) -> None:
        snapshot = (
            checkpoint
            if isinstance(checkpoint, SignalConsumerCheckpoint)
            else SignalConsumerCheckpoint.from_mapping(checkpoint)
        )
        if snapshot.consumer_type != self.consumer_type:
            raise ValueError(f"checkpoint de {snapshot.consumer_type!r} incompatible con {self.consumer_type!r}")
        self._restore_book(snapshot.state)
        self._restore_events(snapshot.state)


def _consumer_limits(state: Mapping[str, Any]) -> tuple[int, int]:
    max_events = state.get("max_events", _DEFAULT_MAX_RECORDS)
    max_signals = state.get("max_signals", _DEFAULT_MAX_RECORDS)
    if not isinstance(max_events, int) or isinstance(max_events, bool) or max_events <= 0:
        max_events = _DEFAULT_MAX_RECORDS
    if not isinstance(max_signals, int) or isinstance(max_signals, bool) or max_signals <= 0:
        max_signals = _DEFAULT_MAX_RECORDS
    return max_events, max_signals


def _new_consumer(
    consumer_type: str,
    *,
    simulation: SimulationConfig,
    max_observations: int,
    max_events: int,
    max_signals: int,
) -> SignalConsumer:
    if consumer_type == "binary_simulation":
        return BinarySimulationConsumer(simulation, max_observations=max_observations)
    if consumer_type == "recording":
        return RecordingSignalConsumer(max_events=max_events, max_signals=max_signals)
    if consumer_type == "cfd_signal":
        return CFDSignalConsumer(max_events=max_events)
    raise ValueError(f"tipo de consumidor de checkpoint desconocido: {consumer_type!r}")


def consumer_from_checkpoint(
    checkpoint: SignalConsumerCheckpoint | Mapping[str, Any] | None,
    *,
    simulation: SimulationConfig,
    max_observations: int = _DEFAULT_MAX_RECORDS,
) -> SignalConsumer:
    """Construye el consumidor declarado por un checkpoint, sin adivinar."""

    if checkpoint is None:
        return BinarySimulationConsumer(simulation, max_observations=max_observations)
    snapshot = (
        checkpoint
        if isinstance(checkpoint, SignalConsumerCheckpoint)
        else SignalConsumerCheckpoint.from_mapping(checkpoint)
    )
    max_events, max_signals = _consumer_limits(snapshot.state)
    consumer = _new_consumer(
        snapshot.consumer_type,
        simulation=simulation,
        max_observations=max_observations,
        max_events=max_events,
        max_signals=max_signals,
    )
    consumer.restore(snapshot)
    return consumer


__all__ = [
    "BinarySimulationConsumer",
    "CFDSignalConsumer",
    "CONSUMER_CHECKPOINT_VERSION",
    "RecordingSignalConsumer",
    "SignalConsumer",
    "SignalConsumerCheckpoint",
    "SignalConsumerEvent",
    "SignalConsumerResult",
    "consumer_from_checkpoint",
]
