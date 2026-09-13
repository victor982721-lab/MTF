"""Stable callback contracts shared by supervision and DEMO safety wiring."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol


class ExecutorPort(Protocol):
    """Genuine DEMO executor seam; OMS and journal stay outside H5."""

    def manage(self) -> Any: ...

    def reconcile(self) -> Any: ...

    def reduce_exposure(self, reason: str) -> Any: ...


class RiskPort(Protocol):
    """Risk callback seam without importing the supervisor implementation."""

    def allow_entry(self, signal: Any) -> bool: ...


@dataclass(frozen=True, slots=True)
class ExecutionCallbacks:
    """Callbacks supplied by the execution/safety composition root.

    Explicit callbacks take precedence over methods on ``executor``.  Keeping
    this dataclass in a leaf contracts module avoids the lazy import cycle
    between the lifecycle supervisor and DEMO composition.
    """

    executor: Any | None = None
    on_signal: Callable[[Any], Any] | None = None
    manage: Callable[[], Any] | None = None
    reconcile: Callable[[], Any] | None = None
    reduce: Callable[..., Any] | None = None
    risk: Any | None = None
    quote_resolver: Callable[[Any], Any] | None = None


__all__ = ["ExecutionCallbacks", "ExecutorPort", "RiskPort"]
