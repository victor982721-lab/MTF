"""Fail-closed local supervisor for the cTrader observation/shadow boundary.

This module is intentionally a composition root, not another order manager.
Market normalisation and the deterministic runtime remain in
``ctrader_watch``/``RuntimeCoordinator``.  A future safety adapter supplies an
executor and risk callbacks through :class:`ExecutionCallbacks`; this module
never constructs an executor, writes an execution journal, or talks to a
broker on its own.

The command-facing entry point is :class:`CommandService`.  It can create the
existing offline cTrader fixture, but a live provider must be prepared by the
caller with an explicit ``network=True`` gate.  Consequently importing this
module, constructing a service, or running it without ``--fixture``/network
authority cannot open a socket or create an account session.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import hashlib
import inspect
import json
import logging
import math
import os
import re
import socket
import stat
import subprocess
import tempfile
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, TypeAlias, cast

from ..configuration import EffectiveConfig, load_config, packaged_config_path
from ..core.canonical import canonical_json
from .application_services import CommandResult
from .ctrader_watch import (
    CTraderWatchContext,
    CTraderWatchOptions,
    CTraderWatchResult,
    CTraderWatchRunner,
    WatchStopReason,
)
from .market_schedule import observed_market_state
from .persistence import SQLiteStore
from .runtime_safety import sqlite_wal_readiness
from .supervision_contracts import ExecutionCallbacks, ExecutorPort, RiskPort

SUPERVISION_SCHEMA_VERSION = 1
_DEFAULT_ACCOUNT_KEY = "demo-account"
_UNSPECIFIED_ACCOUNT_KEYS = frozenset({"demo-account", "fixture"})
_TERMINAL_RECONCILIATIONS = frozenset({"VERIFIED", "RECOVERED_BOUNDED", "NOT_APPLICABLE"})
_CONNECTED_STATES = frozenset({"CONNECTED", "HEALTHY", "OPEN"})
_CLOSED_STATES = frozenset({"CLOSED", "CLOSED_MARKET", "CLOSED_SCHEDULED", "MARKET_CLOSED"})
_ACTIVE_LIFECYCLES = frozenset(
    {
        "STARTING",
        "OBSERVING",
        "SHADOW",
        "DEMO",
        "RECONNECTING",
        "RECONCILING",
    }
)
_LATCHED_RISK_BREACHES = frozenset({"max_daily_loss_exceeded", "max_drawdown_exceeded", "min_margin_level_breached"})

JsonObject: TypeAlias = dict[str, Any]
Clock: TypeAlias = Callable[[], datetime]
MonoClock: TypeAlias = Callable[[], float]
AlertCallback: TypeAlias = Callable[[str, Mapping[str, Any]], Any]
ProviderFactory: TypeAlias = Callable[[], Any]
WatchRunnerFactory: TypeAlias = Callable[[CTraderWatchContext, CTraderWatchOptions], Any]
CallbacksFactory: TypeAlias = Callable[[Any, Mapping[str, Any]], "ExecutionCallbacks"]


class SupervisionError(RuntimeError):
    """Base error for a supervisor that refuses to weaken a safety gate."""


class SingleWriterBusy(SupervisionError):
    """Another process owns the account supervisor lock."""


class NetworkActivationRequired(SupervisionError):
    """A non-fixture provider was requested without an explicit network gate."""


class StateCorrupt(SupervisionError):
    """Persistent state is malformed or belongs to another account."""


class StatePersistenceFailure(SupervisionError):
    """The durable supervisor state could not be written."""


class HumanLatchRequired(SupervisionError):
    """A latched supervisor requires an explicit human reset."""


class SupervisorMode(StrEnum):
    """Product mode; values are CLI-friendly lower-case strings."""

    OBSERVE = "observe"
    SHADOW = "shadow"
    DEMO = "demo"


class SupervisorLifecycle(StrEnum):
    CREATED = "CREATED"
    STARTING = "STARTING"
    OBSERVING = "OBSERVING"
    SHADOW = "SHADOW"
    DEMO = "DEMO"
    RECONNECTING = "RECONNECTING"
    RECONCILING = "RECONCILING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    FATAL_LATCHED = "FATAL_LATCHED"
    ERROR = "ERROR"


def _as_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} debe ser booleano")
    return value


def _finite_number(value: Any, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} debe ser numérico")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser numérico") from exc
    if not math.isfinite(result) or result < 0 or (positive and result <= 0):
        qualifier = "positivo" if positive else "finito y no negativo"
        raise ValueError(f"{name} debe ser {qualifier}")
    return result


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} debe ser entero positivo")
    return value


def _mode(value: Any) -> SupervisorMode:
    text = str(getattr(value, "value", value) or "observe").strip().lower()
    aliases = {
        "observation": SupervisorMode.OBSERVE,
        "observacion": SupervisorMode.OBSERVE,
        "observación": SupervisorMode.OBSERVE,
        "shadowing": SupervisorMode.SHADOW,
        "demo-only": SupervisorMode.DEMO,
    }
    try:
        return SupervisorMode(aliases.get(text, text))
    except ValueError as exc:
        raise ValueError(f"mode no soportado: {value!r}") from exc


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat() if value is not None else None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if result.tzinfo is None or result.utcoffset() is None:
        return None
    return result.astimezone(UTC)


def _now_utc(clock: Clock) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock debe devolver datetime con zona horaria")
    return value.astimezone(UTC)


def _boot_id() -> str | None:
    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
    except OSError:
        return None
    return value or None


def _process_start_ticks(pid: int) -> int | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except (OSError, ValueError):
        return None
    closing = raw.rfind(")")
    if closing < 0:
        return None
    fields = raw[closing + 2 :].split()
    # ``fields[0]`` is proc stat field 3 (state); field 22 is index 19.
    if len(fields) <= 19:
        return None
    try:
        return int(fields[19])
    except ValueError:
        return None


def _runtime_identity() -> dict[str, Any]:
    pid = os.getpid()
    return {
        "pid": pid,
        "boot_id": _boot_id(),
        "process_start_ticks": _process_start_ticks(pid),
    }


def _safe_component(value: str) -> str:
    text = str(value).strip()
    if (
        not text
        or text in {".", ".."}
        or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in text)
    ):
        raise ValueError("account_key sólo admite letras, números, '_' y '-'")
    return text


def _public_reason(value: str) -> str:
    text = str(value).strip().upper().replace("-", "_").replace(" ", "_")
    return text[:80] if text else "UNKNOWN_REASON"


def _safe_error_text(error: BaseException) -> str:
    """Keep state/alerts useful without persisting adapter exception bodies."""
    return f"{type(error).__name__}: detail_redacted"


def _normalise_option_numbers(options: SupervisorOptions) -> None:
    for name in (
        "duration_seconds",
        "idle_timeout_seconds",
        "poll_timeout_seconds",
        "reconnect_backoff_seconds",
        "reconnect_backoff_max_seconds",
        "stale_after_seconds",
        "suspension_threshold_seconds",
        "clock_backwards_tolerance_seconds",
        "watchdog_interval_seconds",
    ):
        value = getattr(options, name)
        if value is not None:
            object.__setattr__(options, name, _finite_number(value, name))
    if options.poll_timeout_seconds <= 0 or options.watchdog_interval_seconds <= 0:
        raise ValueError("poll_timeout_seconds y watchdog_interval_seconds deben ser positivos")
    if options.reconnect_backoff_max_seconds < options.reconnect_backoff_seconds:
        raise ValueError("reconnect_backoff_max_seconds debe ser >= reconnect_backoff_seconds")
    for name in ("max_events", "checkpoint_every", "max_candles", "max_reconnect_attempts"):
        value = getattr(options, name)
        if value is not None:
            object.__setattr__(options, name, _positive_int(value, name))


def _normalise_option_paths(options: SupervisorOptions) -> None:
    object.__setattr__(options, "account_key", _safe_component(options.account_key))
    if options.state_dir is not None:
        object.__setattr__(options, "state_dir", Path(options.state_dir).expanduser())
    if options.db_path is not None:
        object.__setattr__(options, "db_path", Path(options.db_path).expanduser())
    if options.notify_socket is not None:
        path = str(options.notify_socket)
        if not path or path.startswith("@"):  # abstract sockets are not a controlled filesystem path
            raise ValueError("notify_socket debe ser una ruta UNIX controlada")
        object.__setattr__(options, "notify_socket", path)
    if options.watchdog_enabled and options.notify_socket is None:
        raise ValueError("watchdog_enabled requiere notify_socket explícito")


@dataclass(frozen=True, slots=True)
class SupervisorOptions:
    """Bounds and explicit gates for one supervisor invocation."""

    mode: SupervisorMode | str = SupervisorMode.OBSERVE
    fixture: bool = False
    network: bool = False
    activate: bool = False
    duration_seconds: float | None = 30.0
    max_events: int | None = 100
    idle_timeout_seconds: float | None = 5.0
    poll_timeout_seconds: float = 0.25
    checkpoint_every: int = 100
    max_candles: int | None = 5_000
    max_reconnect_attempts: int = 3
    reconnect_backoff_seconds: float = 0.25
    reconnect_backoff_max_seconds: float = 5.0
    stale_after_seconds: float = 90.0
    suspension_threshold_seconds: float = 30.0
    clock_backwards_tolerance_seconds: float = 1.0
    account_key: str = _DEFAULT_ACCOUNT_KEY
    state_dir: Path | None = None
    db_path: Path | None = None
    notify_socket: str | Path | None = None
    watchdog_enabled: bool = False
    watchdog_interval_seconds: float = 30.0
    dbus_alerts: bool = False
    reduce_on_failure: bool = True
    resume: bool = True
    continuous: bool = False

    def __post_init__(self) -> None:
        selected_mode = _mode(self.mode)
        object.__setattr__(self, "mode", selected_mode)
        _validate_option_flags(self, selected_mode)
        _normalise_option_numbers(self)
        _normalise_option_paths(self)

    @property
    def execution_requested(self) -> bool:
        return self.mode is SupervisorMode.DEMO and self.activate


def _validate_option_flags(options: SupervisorOptions, selected_mode: SupervisorMode) -> None:
    for name in (
        "fixture",
        "network",
        "activate",
        "watchdog_enabled",
        "dbus_alerts",
        "reduce_on_failure",
        "resume",
        "continuous",
    ):
        _as_bool(getattr(options, name), name)
    if options.fixture and options.network:
        raise ValueError("fixture y network son incompatibles")
    if options.activate and selected_mode is not SupervisorMode.DEMO:
        raise ValueError("activate sólo está permitido en mode=demo")
    if (
        options.duration_seconds is None
        and options.max_events is None
        and options.idle_timeout_seconds is None
        and not options.continuous
    ):
        raise ValueError("el supervisor requiere duration, max_events o idle_timeout_seconds")


class ProviderPort(Protocol):
    """Small provider surface consumed by the supervisor."""

    @property
    def status(self) -> Any: ...

    def close(self) -> Any: ...


class CooperativeStop:
    """Signal-to-event bridge used by the command boundary."""

    def __init__(self) -> None:
        self.event = threading.Event()
        self._previous: dict[int, Any] = {}

    def __enter__(self) -> threading.Event:
        import signal

        if threading.current_thread() is threading.main_thread():
            for number in (signal.SIGINT, signal.SIGTERM):
                self._previous[number] = signal.getsignal(number)
                signal.signal(number, self._request)
        return self.event

    def _request(self, _number: int, _frame: Any) -> None:
        self.event.set()

    def __exit__(self, *_args: Any) -> None:
        import signal

        for number, previous in self._previous.items():
            signal.signal(number, previous)


@dataclass(slots=True)
class SupervisorContext:
    """Prepared local runtime and provider boundary."""

    provider: ProviderPort | Any | None
    config: EffectiveConfig
    store: SQLiteStore
    provenance: Mapping[str, Any] = field(default_factory=dict)
    provider_factory: ProviderFactory | None = None
    callbacks: ExecutionCallbacks = field(default_factory=ExecutionCallbacks)
    market_state: Callable[..., str] | None = None
    clock: Clock | None = None
    monotonic: MonoClock | None = None
    sleep: Callable[[float], None] | None = None
    watch_runner_factory: WatchRunnerFactory | None = None
    stop_event: threading.Event | None = None
    reconnect: Callable[[], Any] | None = None


@dataclass(slots=True)
class PersistentSupervisorState:
    """JSON-safe operational state; no credentials or raw provider payloads."""

    schema_version: int = SUPERVISION_SCHEMA_VERSION
    account_key: str = _DEFAULT_ACCOUNT_KEY
    mode: str = SupervisorMode.OBSERVE.value
    source: str = "UNKNOWN"
    instrument: str = "EUR/USD"
    execution_enabled: bool = False
    backlog: int = 0
    connection_generation: int | None = None
    exposure_state: str = "UNKNOWN"
    protection_state: str = "UNKNOWN"
    economic_state: str = "UNKNOWN"
    lifecycle: str = SupervisorLifecycle.CREATED.value
    connection_state: str = "UNKNOWN"
    market_state: str = "UNKNOWN"
    feed_state: str = "UNKNOWN"
    freshness_state: str = "UNKNOWN"
    reconciliation_state: str = "PENDING"
    risk_state: str = "BLOCKED"
    risk_blocked_reasons: tuple[str, ...] = ()
    session_id: str | None = None
    analysis_id: str | None = None
    started_at: str | None = None
    stopped_at: str | None = None
    last_progress_at: str | None = None
    last_market_at: str | None = None
    previous_wall_at: str | None = None
    previous_mono: float | None = None
    runtime_identity: Mapping[str, Any] = field(default_factory=dict)
    published_at: str | None = None
    valid_for_seconds: int = 60
    messages: int = 0
    events: int = 0
    signals: int = 0
    reconnects: int = 0
    reconciliations: int = 0
    progress_counter: int = 0
    stop_reason: str | None = None
    error: str | None = None
    fatal_latched: bool = False
    latch_reason: str | None = None
    stop_requested: bool = False
    suspension_detected: bool = False
    clock_anomaly: bool = False
    persistence_ok: bool = True
    state_persisted: bool = True
    watchdog_error: str | None = None

    def readiness_mapping(self) -> JsonObject:
        reasons: list[str] = []
        active_lifecycle = self.lifecycle in _ACTIVE_LIFECYCLES
        if not active_lifecycle:
            reasons.append("process_not_running")
        if self.fatal_latched:
            reasons.append("fatal_latched")
        if not self.persistence_ok:
            reasons.append("state_persistence_failed")
        if self.stop_requested:
            reasons.append("stop_requested")
        if self.feed_state not in {"VALID", "NOT_APPLICABLE"}:
            reasons.append("feed_not_ready")
        if self.feed_state == "CLOSED_MARKET":
            reasons.append("market_closed")
        if self.reconciliation_state not in _TERMINAL_RECONCILIATIONS:
            reasons.append("reconciliation_pending")
        if self.mode == SupervisorMode.DEMO.value and not self.execution_enabled:
            reasons.append("execution_not_activated")
        # In observe/shadow, readiness means the observation path is safe; it
        # is intentionally not a claim that trading is ready.
        return {
            "ready": not reasons,
            "reasons": list(dict.fromkeys(_public_reason(item) for item in reasons)),
            "mode": self.mode,
            "source": self.source,
            "execution_enabled": self.execution_enabled,
            "backlog": self.backlog,
            "connection_generation": self.connection_generation,
            "exposure_state": self.exposure_state,
            "protection_state": self.protection_state,
            "economic_state": self.economic_state,
            "connection": self.connection_state,
            "feed": self.feed_state,
            "freshness": self.freshness_state,
            "reconciliation": self.reconciliation_state,
        }

    def to_dict(self) -> JsonObject:
        identity = dict(self.runtime_identity)
        if self.published_at is not None:
            identity.setdefault("published_at", self.published_at)
        identity.setdefault("valid_for_seconds", self.valid_for_seconds)
        return {
            "schema_version": self.schema_version,
            "account_key": self.account_key,
            "mode": self.mode,
            "source": self.source,
            "instrument": self.instrument,
            "execution_enabled": self.execution_enabled,
            "backlog": self.backlog,
            "connection_generation": self.connection_generation,
            "exposure_state": self.exposure_state,
            "protection_state": self.protection_state,
            "economic_state": self.economic_state,
            "lifecycle": self.lifecycle,
            "connection_state": self.connection_state,
            "market_state": self.market_state,
            "feed_state": self.feed_state,
            "freshness_state": self.freshness_state,
            "freshness": self.freshness_state,
            "reconciliation_state": self.reconciliation_state,
            "risk_state": self.risk_state,
            "risk_blocked_reasons": list(self.risk_blocked_reasons),
            "session_id": self.session_id,
            "analysis_id": self.analysis_id,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "last_progress_at": self.last_progress_at,
            "last_market_at": self.last_market_at,
            "previous_wall_at": self.previous_wall_at,
            "previous_mono": self.previous_mono,
            "runtime_identity": identity,
            "readiness": self.readiness_mapping(),
            "published_at": self.published_at,
            "valid_for_seconds": self.valid_for_seconds,
            "messages": self.messages,
            "events": self.events,
            "signals": self.signals,
            "reconnects": self.reconnects,
            "reconciliations": self.reconciliations,
            "progress_counter": self.progress_counter,
            "stop_reason": self.stop_reason,
            "error": self.error,
            "fatal_latched": self.fatal_latched,
            "latch_reason": self.latch_reason,
            "stop_requested": self.stop_requested,
            "suspension_detected": self.suspension_detected,
            "clock_anomaly": self.clock_anomaly,
            "persistence_ok": self.persistence_ok,
            "state_persisted": self.state_persisted,
            "watchdog_error": self.watchdog_error,
        }

    @classmethod
    def from_mapping(
        cls, raw: Mapping[str, Any], *, account_key: str, mode: SupervisorMode, instrument: str
    ) -> PersistentSupervisorState:
        values: JsonObject = dict(raw)
        _validate_state_identity(values, account_key=account_key, mode=mode, instrument=instrument)
        _validate_state_fields(values)
        allowed = {field.name for field in cls.__dataclass_fields__.values()}
        values = {key: value for key, value in values.items() if key in allowed}
        values["account_key"] = account_key
        values["mode"] = mode.value
        values["source"] = str(values.get("source", "UNKNOWN"))
        values["instrument"] = instrument
        values["runtime_identity"] = dict(cast(Mapping[str, Any], values.get("runtime_identity", {})))
        values["risk_blocked_reasons"] = tuple(str(item) for item in values.get("risk_blocked_reasons", ()))
        return cls(**values)


def _validate_state_identity(
    values: Mapping[str, Any], *, account_key: str, mode: SupervisorMode, instrument: str
) -> None:
    if values.get("schema_version") != SUPERVISION_SCHEMA_VERSION:
        raise StateCorrupt("schema de estado de supervisión desconocido")
    if str(values.get("account_key", "")) != account_key:
        raise StateCorrupt("estado pertenece a otra cuenta")
    if str(values.get("mode", "")) != mode.value:
        raise StateCorrupt("estado pertenece a otro modo")
    if str(values.get("instrument", "")).upper() != instrument.upper():
        raise StateCorrupt("estado pertenece a otro instrumento")


def _validate_state_fields(values: Mapping[str, Any]) -> None:
    integer_fields = (
        "messages",
        "events",
        "signals",
        "reconnects",
        "reconciliations",
        "progress_counter",
        "backlog",
    )
    for name in integer_fields:
        value = values.get(name, 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise StateCorrupt(f"campo de estado inválido: {name}")
    previous_mono = values.get("previous_mono")
    if previous_mono is not None:
        try:
            valid_previous_mono = not isinstance(previous_mono, bool) and math.isfinite(float(previous_mono))
        except (TypeError, ValueError):
            valid_previous_mono = False
        if not valid_previous_mono:
            raise StateCorrupt("previous_mono inválido")
    valid_for = values.get("valid_for_seconds", 60)
    if isinstance(valid_for, bool) or not isinstance(valid_for, int) or not 0 < valid_for <= 60:
        raise StateCorrupt("valid_for_seconds inválido")
    identity = values.get("runtime_identity", {})
    if not isinstance(identity, Mapping):
        raise StateCorrupt("runtime_identity inválido")
    if not isinstance(values.get("execution_enabled", False), bool):
        raise StateCorrupt("execution_enabled inválido")
    generation = values.get("connection_generation")
    if generation is not None and (isinstance(generation, bool) or not isinstance(generation, int) or generation < 0):
        raise StateCorrupt("connection_generation inválido")


class _AccountLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def __enter__(self) -> _AccountLock:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        parent_mode = self.path.parent.lstat().st_mode
        if stat.S_ISLNK(parent_mode) or not stat.S_ISDIR(parent_mode):
            raise StateCorrupt("directorio de lock no es una carpeta regular")
        if self.path.is_symlink():
            raise StateCorrupt("lock de cuenta es un symlink")
        fd = os.open(
            self.path,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise StateCorrupt("lock de cuenta no es archivo regular")
            os.fchmod(fd, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                    raise SingleWriterBusy("otra instancia posee el lock de la cuenta") from exc
                raise
            self._fd = fd
            return self
        except BaseException:
            os.close(fd)
            raise

    def __exit__(self, *_args: object) -> None:
        fd, self._fd = self._fd, None
        if fd is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


class SupervisorStateStore:
    """Private, atomic, fsync-backed state and one-writer lock."""

    def __init__(self, root: str | Path, account_key: str, *, lock_root: str | Path | None = None) -> None:
        self.root = Path(root).expanduser()
        self.account_key = _safe_component(account_key)
        self.state_path = self.root / f"{self.account_key}.json"
        configured_lock_root = lock_root or os.environ.get("MTF_LAB_SUPERVISION_LOCK_DIR")
        if configured_lock_root is None:
            xdg = os.environ.get("XDG_STATE_HOME")
            configured_lock_root = (
                Path(xdg).expanduser() / "mtf-lab" / "supervision-locks"
                if xdg
                else Path.home() / ".local" / "state" / "mtf-lab" / "supervision-locks"
            )
        self.lock_root = Path(configured_lock_root).expanduser()
        self.lock_path = self.lock_root / f"{self.account_key}.lock"

    def lock(self) -> _AccountLock:
        return _AccountLock(self.lock_path)

    def _ensure_regular_target(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            mode = self.root.lstat().st_mode
        except OSError as exc:
            raise StatePersistenceFailure("no se pudo inspeccionar el directorio de estado") from exc
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise StateCorrupt("directorio de estado no es una carpeta regular")
        if self.state_path.is_symlink():
            raise StateCorrupt("estado de supervisión es un symlink")

    def load(self, *, mode: SupervisorMode, instrument: str) -> PersistentSupervisorState | None:
        self._ensure_regular_target()
        if not self.state_path.exists():
            return None
        if self.state_path.is_symlink():
            raise StateCorrupt("estado de supervisión es un symlink")
        try:
            with self.state_path.open("r", encoding="utf-8") as stream:
                raw = json.load(stream)
        except (OSError, ValueError) as exc:
            raise StateCorrupt("estado de supervisión ilegible") from exc
        if not isinstance(raw, Mapping):
            raise StateCorrupt("estado de supervisión no es un objeto")
        return PersistentSupervisorState.from_mapping(
            raw, account_key=self.account_key, mode=mode, instrument=instrument
        )

    def save(self, state: PersistentSupervisorState) -> None:
        self._ensure_regular_target()
        payload = canonical_json(state.to_dict()).encode("utf-8") + b"\n"
        tmp_fd: int | None = None
        tmp_name: str | None = None
        try:
            tmp_fd, tmp_name = tempfile.mkstemp(prefix=f".{self.account_key}.", suffix=".tmp", dir=self.root)
            os.fchmod(tmp_fd, 0o600)
            with os.fdopen(tmp_fd, "wb") as stream:
                tmp_fd = None
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp_name, self.state_path)
            tmp_name = None
            directory_fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except (OSError, ValueError, TypeError) as exc:
            raise StatePersistenceFailure("no se pudo persistir estado de supervisión") from exc
        finally:
            if tmp_fd is not None:
                with contextlib.suppress(OSError):
                    os.close(tmp_fd)
            if tmp_name is not None:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_name)


def read_supervisor_readiness(path: str | Path, *, now: datetime | None = None) -> ReadinessStatus:
    """Adapt the canonical readiness reader to this module's dataclass."""
    from .readiness import read_supervisor_readiness as read_canonical_readiness

    current = read_canonical_readiness(path, now=now)
    mode = str(current.get("mode", "unknown"))
    reasons_raw = current.get("reasons", [])
    reasons = tuple(str(item) for item in reasons_raw) if isinstance(reasons_raw, list) else ("INVALID_READINESS",)
    execution = mode.lower() == SupervisorMode.DEMO.value
    return ReadinessStatus(
        bool(current.get("ready", False)),
        mode,
        "UNKNOWN",
        execution,
        reasons,
        "UNKNOWN",
        "UNKNOWN",
        "UNKNOWN",
        "UNKNOWN",
    )


@dataclass(frozen=True, slots=True)
class RiskStatus:
    """Risk view kept separate from UI/health and delegated to the executor."""

    state: str
    entries_allowed: bool
    management_allowed: bool
    reduction_allowed: bool
    exposure_known: bool
    own_exposure: int | None
    blocked_reasons: tuple[str, ...] = ()
    foreign_exposure: bool = False

    def to_dict(self) -> JsonObject:
        return {
            "state": self.state,
            "entries_allowed": self.entries_allowed,
            "management_allowed": self.management_allowed,
            "reduction_allowed": self.reduction_allowed,
            "exposure_known": self.exposure_known,
            "own_exposure": self.own_exposure,
            "blocked_reasons": list(self.blocked_reasons),
            "foreign_exposure": self.foreign_exposure,
        }


@dataclass(frozen=True, slots=True)
class HealthStatus:
    """Operational health mapping; no UI dependencies."""

    healthy: bool
    lifecycle: str
    connection: str
    market: str
    feed: str
    freshness: str
    reconciliation: str
    persistence_ok: bool
    lock_held: bool
    fatal_latched: bool
    stop_requested: bool
    clock_anomaly: bool
    suspension_detected: bool
    last_progress_at: str | None
    last_market_at: str | None
    progress_age_seconds: float | None
    market_age_seconds: float | None
    blocked_reasons: tuple[str, ...] = ()
    error: str | None = None
    live_process_proof: bool = False

    def to_dict(self) -> JsonObject:
        return {
            "healthy": self.healthy,
            "lifecycle": self.lifecycle,
            "connection": self.connection,
            "market": self.market,
            "feed": self.feed,
            "freshness": self.freshness,
            "reconciliation": self.reconciliation,
            "persistence_ok": self.persistence_ok,
            "lock_held": self.lock_held,
            "fatal_latched": self.fatal_latched,
            "stop_requested": self.stop_requested,
            "clock_anomaly": self.clock_anomaly,
            "suspension_detected": self.suspension_detected,
            "last_progress_at": self.last_progress_at,
            "last_market_at": self.last_market_at,
            "progress_age_seconds": self.progress_age_seconds,
            "market_age_seconds": self.market_age_seconds,
            "blocked_reasons": list(self.blocked_reasons),
            "error": self.error,
            # A state file is an observation, not proof of a currently live
            # process.  Only an in-memory lock held by this process is proof.
            "live_process_proof": self.live_process_proof,
        }


@dataclass(frozen=True, slots=True)
class ReadinessStatus:
    """Execution/analysis readiness mapping, deliberately not a UI model."""

    ready: bool
    mode: str
    source: str
    execution_enabled: bool
    reasons: tuple[str, ...] = ()
    connection: str = "UNKNOWN"
    feed: str = "UNKNOWN"
    freshness: str = "UNKNOWN"
    reconciliation: str = "PENDING"

    def to_dict(self) -> JsonObject:
        return {
            "ready": self.ready,
            "mode": self.mode,
            "source": self.source,
            "execution_enabled": self.execution_enabled,
            "reasons": list(self.reasons),
            "connection": self.connection,
            "feed": self.feed,
            "freshness": self.freshness,
            "reconciliation": self.reconciliation,
        }


@dataclass(frozen=True, slots=True)
class SupervisorResult:
    """Structured summary returned by the command/API composition root."""

    ok: bool
    state: str
    mode: str
    source: str
    session_id: str | None
    analysis_id: str | None
    messages: int
    events: int
    signals: int
    reconciliations: int
    reconnects: int
    stop_reason: str | None
    clean_stop: bool
    execution_enabled: bool
    network_attempted: bool
    fatal_latched: bool
    reconciled_before_summary: bool
    health: HealthStatus
    readiness: ReadinessStatus
    risk: RiskStatus
    state_path: str
    error: str | None = None
    provenance: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> JsonObject:
        return {
            "schema_version": SUPERVISION_SCHEMA_VERSION,
            "ok": self.ok,
            "state": self.state,
            "mode": self.mode,
            "source": self.source,
            "session_id": self.session_id,
            "analysis_id": self.analysis_id,
            "messages": self.messages,
            "events": self.events,
            "signals": self.signals,
            "reconciliations": self.reconciliations,
            "reconnects": self.reconnects,
            "stop_reason": self.stop_reason,
            "clean_stop": self.clean_stop,
            "execution_enabled": self.execution_enabled,
            "network_attempted": self.network_attempted,
            "fatal_latched": self.fatal_latched,
            "reconciled_before_summary": self.reconciled_before_summary,
            "health": self.health.to_dict(),
            "readiness": self.readiness.to_dict(),
            "risk": self.risk.to_dict(),
            "state_path": self.state_path,
            "error": self.error,
            "provenance": dict(self.provenance),
        }


class AlertSink(Protocol):
    def alert(self, event: str, details: Mapping[str, Any]) -> Any: ...


class JournalAlertSink:
    """Local journal alert sink; it never creates a second execution log."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger("mtf_lab.supervision")

    def alert(self, event: str, details: Mapping[str, Any]) -> None:
        self.logger.warning("mtf_supervision_alert event=%s details=%s", event, canonical_json(dict(details)))


class DbusAlertSink:
    """Opt-in D-Bus seam requiring an injected, tested sender.

    No D-Bus package is imported and no system bus is contacted by default.
    A production composition root must deliberately provide a sender after it
    has verified the desired local policy.
    """

    def __init__(self, sender: AlertCallback | None = None, *, enabled: bool = False) -> None:
        if enabled and not callable(sender):
            raise ValueError("dbus alerts requieren un sender probado explícitamente")
        self.sender = sender
        self.enabled = enabled

    def alert(self, event: str, details: Mapping[str, Any]) -> None:
        if self.enabled and self.sender is not None:
            self.sender(event, dict(details))


def _send_desktop_alert(event: str, details: Mapping[str, Any]) -> None:
    """Explicit local desktop notification; no shell, secrets or remote channel."""

    def code(value: Any) -> str:
        text = str(value).upper()
        return text if re.fullmatch(r"[A-Z][A-Z0-9_]{0,79}", text) else "DETAIL_REDACTED"

    body = " · ".join(code(details[key]) for key in ("reason", "error_type") if key in details)
    subprocess.run(
        ["/usr/bin/notify-send", "--app-name=MTF Lab", "--", f"MTF Lab: {code(event)}", body],
        check=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=3,
    )


class CompositeAlertSink:
    def __init__(self, sinks: Iterable[AlertSink] = ()) -> None:
        self.sinks = tuple(sinks)

    def alert(self, event: str, details: Mapping[str, Any]) -> None:
        for sink in self.sinks:
            sink.alert(event, details)


class WatchdogNotifier:
    """Optional systemd watchdog notifier based only on useful progress."""

    def __init__(
        self,
        socket_path: str | Path | None,
        *,
        enabled: bool = False,
        interval_seconds: float = 30.0,
        monotonic: MonoClock | None = None,
        sender: Callable[[str, bytes], Any] | None = None,
    ) -> None:
        self.socket_path = str(socket_path) if socket_path is not None else None
        self.enabled = enabled
        self.interval_seconds = _finite_number(interval_seconds, "watchdog interval", positive=True)
        self._monotonic = monotonic or time.monotonic
        self._sender = sender or self._send
        self._last_ping = float("-inf")
        self._last_progress = -1
        self._ready_sent = False
        if enabled and not self.socket_path:
            raise ValueError("watchdog requiere NOTIFY_SOCKET controlado")
        if self.socket_path and ("\x00" in self.socket_path or not self.socket_path.startswith(("/", "@"))):
            raise ValueError("watchdog requiere una dirección Unix local absoluta o abstracta")

    @classmethod
    def from_environment(
        cls,
        *,
        enabled: bool = False,
        interval_seconds: float = 30.0,
        monotonic: MonoClock | None = None,
        sender: Callable[[str, bytes], Any] | None = None,
    ) -> WatchdogNotifier:
        return cls(
            os.environ.get("NOTIFY_SOCKET") if enabled else None,
            enabled=enabled,
            interval_seconds=interval_seconds,
            monotonic=monotonic,
            sender=sender,
        )

    @staticmethod
    def _send(path: str, payload: bytes) -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.settimeout(2)
            sock.connect("\0" + path[1:] if path.startswith("@") else path)
            sock.send(payload)

    def notify_progress(self, progress_counter: int, status: str) -> bool:
        """Send at most once per interval, and only after new progress."""
        if not self.enabled or self.socket_path is None:
            return False
        now = self._monotonic()
        useful = progress_counter > self._last_progress
        due = now - self._last_ping >= self.interval_seconds
        if not useful or not due:
            return False
        payload = f"WATCHDOG=1\nSTATUS=MTF {status} progress={progress_counter}\n".encode()
        self._sender(self.socket_path, payload)
        self._last_ping = now
        self._last_progress = progress_counter
        return True

    def notify_ready(self, status: str = "STARTING") -> bool:
        """Publish readiness to a local systemd notifier when explicitly enabled."""
        if not self.enabled or self.socket_path is None or self._ready_sent:
            return False
        payload = f"READY=1\nSTATUS=MTF {status}\n".encode()
        self._sender(self.socket_path, payload)
        self._ready_sent = True
        return True


def _invoke_callback(callback: Callable[..., Any], *args: Any) -> Any:
    """Call a callback using its declared arity without hiding its failures."""
    try:
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        return callback(*args)
    parameters = tuple(signature.parameters.values())
    if any(parameter.kind is parameter.VAR_POSITIONAL for parameter in parameters):
        return callback(*args)
    positional = tuple(
        parameter
        for parameter in parameters
        if parameter.kind in {parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD}
    )
    required = sum(parameter.default is parameter.empty for parameter in positional)
    if required > len(args):
        raise TypeError("callback requiere más argumentos que el contrato disponible")
    return callback(*args[: len(positional)])


def _provider_value(provider: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(provider, name)
    except (AttributeError, RuntimeError):
        return default


def _connection_from_provider(provider: Any) -> str:
    status = _provider_value(provider, "status")
    if isinstance(status, Mapping):
        value = status.get("connection", status.get("state", "UNKNOWN"))
    else:
        value = getattr(status, "connection", getattr(status, "state", status))
    return str(getattr(value, "value", value)).strip().upper() or "UNKNOWN"


def _source_from_provenance(provenance: Mapping[str, Any], fixture: bool) -> str:
    value = str(provenance.get("source_mode", "") or "").strip().upper()
    if fixture or bool(provenance.get("synthetic", False)):
        return "SYNTHETIC_FIXTURE"
    return value if value in {"LIVE", "DEMO_OBSERVED"} else "UNKNOWN"


def _is_synthetic(provenance: Mapping[str, Any], fixture: bool) -> bool:
    return (
        fixture
        or bool(provenance.get("synthetic", False))
        or _source_from_provenance(provenance, fixture) == "SYNTHETIC_FIXTURE"
    )


def _network_account_key(
    provenance: Mapping[str, Any], config: EffectiveConfig, requested: str, provider: Any | None = None
) -> str:
    observed_id = provenance.get("account_id", provenance.get("selected_account_id"))
    if isinstance(observed_id, bool) or observed_id is None or not str(observed_id).strip():
        raise NetworkActivationRequired("network requiere account_id observado y seleccionado")
    environment = str(provenance.get("environment", "")).strip().upper()
    if environment != "DEMO":
        raise SupervisionError("network supervisor sólo admite entorno DEMO observado")
    if provenance.get("account_selected") is not True or provenance.get("account_verified") is not True:
        raise NetworkActivationRequired("network requiere cuenta DEMO seleccionada y verificada")
    endpoint = str(provenance.get("endpoint", "")).strip().lower()
    if not endpoint and provider is not None:
        provider_config = getattr(provider, "config", None)
        host = str(getattr(provider_config, "host", "")).strip().lower()
        port = getattr(provider_config, "port", None)
        if host and isinstance(port, int) and not isinstance(port, bool):
            endpoint = f"{host}:{port}"
    if not endpoint:
        raise NetworkActivationRequired("network requiere endpoint DEMO observado")
    identity = {
        "account_id": str(observed_id),
        "environment": environment,
        "endpoint": endpoint,
    }
    digest = hashlib.sha256(canonical_json(identity).encode()).hexdigest()[:24]
    derived = f"demo-{digest}"
    if requested not in _UNSPECIFIED_ACCOUNT_KEYS and requested != derived:
        raise SupervisionError("account_key libre no coincide con la identidad DEMO observada")
    return derived


class _SupervisedWatchRunner(CTraderWatchRunner):
    """Existing watch runner plus a durable supervisor heartbeat hook."""

    def __init__(
        self,
        context: CTraderWatchContext,
        options: CTraderWatchOptions,
        progress_callback: Callable[[], None],
        signal_callback: Callable[[Any], None],
        record_callback: Callable[[Any], None],
        status_callback: Callable[[Any], None],
        idle_callback: Callable[[], None],
    ) -> None:
        super().__init__(context, options)
        self._progress_callback = progress_callback
        self._signal_callback = signal_callback
        self._record_callback = record_callback
        self._status_callback = status_callback
        self._idle_callback = idle_callback

    def _checkpoint(self) -> None:
        super()._checkpoint()
        self._progress_callback()

    def _process_record(self, record: Any, raw_synthetic: bool) -> bool:
        before = len(self.coordinator.signals)
        accepted = super()._process_record(record, raw_synthetic)
        self._record_callback(record)
        for signal in self.coordinator.signals[before:]:
            self._signal_callback(signal)
        return accepted

    def _process_message(self, message: Any) -> None:
        super()._process_message(message)
        self._status_callback(self.coordinator)

    def _poll_stop_reason(self, started: float) -> WatchStopReason | None:
        self._idle_callback()
        return super()._poll_stop_reason(started)


class CTraderSupervisor:
    """Bounded, persistent supervisor around the existing cTrader runtime."""

    def __init__(
        self,
        context: SupervisorContext,
        options: SupervisorOptions | None = None,
        *,
        alert_sink: AlertSink | None = None,
        watchdog: WatchdogNotifier | None = None,
    ) -> None:
        self.context = context
        self.options = options or SupervisorOptions()
        self._clock = context.clock or (lambda: datetime.now(UTC))
        self._monotonic = context.monotonic or time.monotonic
        self._sleep = context.sleep or time.sleep
        self._stop_event = context.stop_event or threading.Event()
        self._alert = alert_sink or (
            CompositeAlertSink((JournalAlertSink(), DbusAlertSink(_send_desktop_alert, enabled=True)))
            if self.options.dbus_alerts
            else JournalAlertSink()
        )
        self._watchdog = watchdog or WatchdogNotifier(
            self.options.notify_socket,
            enabled=self.options.watchdog_enabled,
            interval_seconds=self.options.watchdog_interval_seconds,
            monotonic=self._monotonic,
        )
        self._account_key = (
            _network_account_key(context.provenance, context.config, self.options.account_key, context.provider)
            if self.options.network
            else self.options.account_key
        )
        state_root = self._state_root()
        self.state_store = SupervisorStateStore(state_root, self._account_key)
        self._source = _source_from_provenance(context.provenance, self.options.fixture)
        self._synthetic = _is_synthetic(context.provenance, self.options.fixture)
        self.state = PersistentSupervisorState(
            account_key=self._account_key,
            mode=cast(SupervisorMode, self.options.mode).value,
            source=self._source,
            instrument=context.config.instrument,
        )
        self._provider: Any | None = context.provider
        self._lock_held = False
        self._primary_error: str | None = None
        self._reconciled_before_summary = False
        self._network_attempted = bool(self.options.network)
        self._last_result: CTraderWatchResult | None = None
        self._active_watch_runner: Any | None = None
        self._watch_started_monotonic: float | None = None
        self._watch_messages_consumed = 0
        self._last_signals: tuple[Any, ...] = ()
        self._delivered_signal_ids: set[str] = set()
        self._delivered_signal_order: deque[str] = deque(maxlen=8192)
        self._last_published_progress = 0
        self._last_published_monotonic = float("-inf")
        self._runtime_connection_state = "UNKNOWN"
        self._runtime_recovery_attempted = False
        self._last_available_at: datetime | None = None
        self._last_record_generation: int | None = None
        self._last_manage_monotonic = float("-inf")
        self._manage_interval_seconds = 5.0
        self._publish_interval_seconds = 15.0
        self._reduction_attempted = False
        self._shutdown_clean = True
        self._validate_boundary()

    def _state_root(self) -> Path:
        if self.options.state_dir is not None:
            return self.options.state_dir
        configured = os.environ.get("MTF_LAB_STATE_DIR")
        if configured:
            return Path(configured).expanduser() / "supervision"
        xdg = os.environ.get("XDG_STATE_HOME")
        return (Path(xdg).expanduser() if xdg else Path.home() / ".local" / "state") / "mtf-lab" / "supervision"

    def _validate_boundary(self) -> None:
        if self.options.fixture and self.options.network:
            raise NetworkActivationRequired("fixture y network son incompatibles")
        config_environment = str(self.context.config.ctrader.get("environment", "DEMO")).upper()
        if config_environment in {"REAL", "LIVE", "PRODUCTION"}:
            raise SupervisionError("supervisor sólo admite configuración DEMO")
        if self.options.mode is SupervisorMode.DEMO and self.options.execution_requested and self._synthetic:
            # A fixture can exercise the callback seam, but it can never be
            # considered a broker execution environment.
            self._network_attempted = False

    def _load_state(self) -> None:
        existing = self.state_store.load(
            mode=cast(SupervisorMode, self.options.mode),
            instrument=self.context.config.instrument,
        )
        if existing is not None:
            self.state = existing

    def _persist(self, *, best_effort: bool = False) -> bool:
        try:
            self.state.state_persisted = True
            self.state.persistence_ok = True
            self.state.runtime_identity = _runtime_identity()
            self.state.published_at = _iso(_now_utc(self._clock))
            self.state.valid_for_seconds = 60
            self.state_store.save(self.state)
            return True
        except Exception as exc:
            self.state.persistence_ok = False
            self.state.state_persisted = False
            if not best_effort:
                self._latch_in_memory("state_persistence_failed", exc)
                raise StatePersistenceFailure("no se pudo persistir estado de supervisión") from exc
            return False

    def _latch_in_memory(self, reason: str, error: BaseException) -> None:
        self.state.fatal_latched = True
        self.state.lifecycle = SupervisorLifecycle.FATAL_LATCHED.value
        self.state.latch_reason = str(reason)
        self.state.error = _safe_error_text(error)
        self._primary_error = self.state.error
        with contextlib.suppress(Exception):
            self._alert.alert("fatal_latch", {"reason": reason, "error_type": type(error).__name__})

    def _set_error(self, error: BaseException, *, latch: bool = False, reason: str = "failure") -> None:
        self.state.error = _safe_error_text(error)
        self._primary_error = self.state.error
        self.state.lifecycle = SupervisorLifecycle.ERROR.value
        with contextlib.suppress(Exception):
            self._alert.alert("supervision_error", {"reason": reason, "error_type": type(error).__name__})
        if latch:
            self._latch_in_memory(reason, error)

    def _now(self) -> datetime:
        now = _now_utc(self._clock)
        mono = self._monotonic()
        previous_wall = _parse_datetime(self.state.previous_wall_at)
        previous_mono = self.state.previous_mono
        if previous_wall is not None and previous_mono is not None:
            wall_delta = (now - previous_wall).total_seconds()
            mono_delta = mono - previous_mono
            if wall_delta < -self.options.clock_backwards_tolerance_seconds:
                self.state.clock_anomaly = True
                self.state.stop_requested = True
                self._stop_event.set()
                self._latch_in_memory("clock_went_backwards", ValueError("reloj de pared retrocedió"))
            elif mono_delta < -self.options.clock_backwards_tolerance_seconds:
                self.state.clock_anomaly = True
                self.state.stop_requested = True
                self._stop_event.set()
                self._latch_in_memory("monotonic_went_backwards", ValueError("reloj monotónico retrocedió"))
            elif wall_delta - mono_delta > self.options.suspension_threshold_seconds:
                self.state.suspension_detected = True
                self.state.lifecycle = SupervisorLifecycle.PAUSED.value
                self.state.reconciliation_state = "NEEDS_RECONCILIATION"
                self._stop_event.set()
                self.state.risk_blocked_reasons = tuple(
                    dict.fromkeys((*self.state.risk_blocked_reasons, "suspension_detected"))
                )
        self.state.previous_wall_at = _iso(now)
        self.state.previous_mono = mono
        return now

    def request_stop(self, reason: str = "STOP_REQUESTED") -> None:
        self.state.stop_requested = True
        safe_reason = str(reason).strip().upper()
        self.state.stop_reason = (
            safe_reason
            if safe_reason in {"STOP_REQUESTED", "PROVIDER_DISCONNECTED", "RECONCILIATION_REQUIRED", "RISK_REDUCTION"}
            else "STOP_REQUESTED"
        )
        self._stop_event.set()

    def _market_state(self, now: datetime) -> str:
        callback = self.context.market_state
        if callback is not None:
            value = _invoke_callback(callback, now)
            return str(getattr(value, "value", value)).strip().upper() or "UNKNOWN"
        provider_value = _provider_value(self._provider, "market_state")
        if callable(provider_value):
            value = _invoke_callback(provider_value, now)
            return str(getattr(value, "value", value)).strip().upper() or "UNKNOWN"
        return "OFFLINE" if self._synthetic else observed_market_state(self._provider, now)

    def _refresh_market(self, now: datetime | None = None) -> None:
        current = now or self._now()
        market = self._market_state(current)
        self.state.market_state = market
        last_market = _parse_datetime(self.state.last_market_at)
        age = (current - last_market).total_seconds() if last_market is not None else None
        if market in _CLOSED_STATES:
            self.state.feed_state = "CLOSED_MARKET"
            self.state.freshness_state = "CLOSED_MARKET"
            self.state.lifecycle = SupervisorLifecycle.PAUSED.value
        elif self._synthetic:
            self.state.feed_state = "NOT_APPLICABLE"
            self.state.freshness_state = "NOT_APPLICABLE"
        elif last_market is None or age is None or age > self.options.stale_after_seconds:
            self.state.feed_state = "STALE"
            self.state.freshness_state = "STALE"
            if self.state.lifecycle not in {SupervisorLifecycle.FATAL_LATCHED.value, SupervisorLifecycle.ERROR.value}:
                self.state.lifecycle = SupervisorLifecycle.PAUSED.value
        else:
            self.state.feed_state = "VALID"
            self.state.freshness_state = "VALID"

    def mark_progress(self, *, messages: int = 0, events: int = 0, market_at: datetime | None = None) -> None:
        if messages < 0 or events < 0:
            raise ValueError("progress counters must be non-negative")
        now = self._now()
        self.state.messages += int(messages)
        self.state.events += int(events)
        self.state.progress_counter += int(messages) + int(events)
        self.state.last_progress_at = _iso(now)
        if market_at is not None:
            if market_at.tzinfo is None or market_at.utcoffset() is None:
                raise ValueError("market_at requiere zona horaria")
            self.state.last_market_at = _iso(market_at)
        self._refresh_market(now)
        with contextlib.suppress(Exception):
            self._watchdog.notify_progress(self.state.progress_counter, self.state.lifecycle)

    def _status_from_executor(self) -> Mapping[str, Any]:
        executor = self.context.callbacks.executor
        status_method = getattr(executor, "cached_status", None) if executor is not None else None
        if not callable(status_method):
            status_method = getattr(executor, "status", None) if executor is not None else None
        if not callable(status_method):
            return {}
        try:
            result = status_method()
        except Exception as exc:
            self.state.risk_blocked_reasons = tuple(
                dict.fromkeys((*self.state.risk_blocked_reasons, "risk_status_error"))
            )
            return {"status_error": type(exc).__name__}
        return result if isinstance(result, Mapping) else {}

    @staticmethod
    def _risk_exposure(raw: Mapping[str, Any], risk_raw: Mapping[str, Any]) -> tuple[int | None, bool]:
        if risk_raw.get("position_state") not in {None, "VALID"} or risk_raw.get("position_error"):
            return None, False
        exposure = raw.get("own_positions", risk_raw.get("own_exposure"))
        if isinstance(exposure, (list, tuple)):
            return len(exposure), True
        if isinstance(exposure, bool):
            return None, False
        if isinstance(exposure, int):
            return exposure, True
        return None, bool(risk_raw.get("exposure_known", False))

    def _risk_reasons(self, raw: Mapping[str, Any], risk_raw: Mapping[str, Any]) -> list[str]:
        reasons = [str(item) for item in self.state.risk_blocked_reasons]
        extra = risk_raw.get("blocked_reasons", ())
        if isinstance(extra, (list, tuple, set)):
            reasons.extend(str(item) for item in extra)
        if bool(
            risk_raw.get(
                "foreign_exposure",
                risk_raw.get("foreign_positions", raw.get("foreign_exposure", raw.get("foreign_positions", False))),
            )
        ):
            reasons.append("foreign_exposure")
        return reasons

    def _risk_entries_allowed(self, execution: bool, foreign: bool, exposure_known: bool) -> bool:
        entries = execution and exposure_known and self.options.mode is SupervisorMode.DEMO
        entries = entries and not self.state.fatal_latched and not self.state.stop_requested
        entries = entries and self.state.feed_state in {"VALID", "NOT_APPLICABLE"}
        entries = entries and self.state.reconciliation_state in _TERMINAL_RECONCILIATIONS
        return entries and not foreign

    def risk_status(self) -> RiskStatus:
        raw = dict(self._status_from_executor())
        risk_raw = raw.get("risk") if isinstance(raw.get("risk"), Mapping) else raw
        if not isinstance(risk_raw, Mapping):
            risk_raw = {}
        own_exposure, exposure_known = self._risk_exposure(raw, risk_raw)
        foreign = bool(
            risk_raw.get(
                "foreign_exposure",
                risk_raw.get("foreign_positions", raw.get("foreign_exposure", raw.get("foreign_positions", False))),
            )
        )
        raw_execution = raw.get("new_intents_enabled", raw.get("execution_enabled", False))
        active = raw.get("active", True) is True
        paused = raw.get("paused", False) is True
        halt = raw.get("halt_reason", risk_raw.get("halt_reason"))
        halted = bool(halt) if isinstance(halt, (str, bool)) else halt is not None
        management = self.options.execution_requested and self.context.callbacks.executor is not None
        execution = management and raw_execution is True and active and not paused and not halted
        entries = self._risk_entries_allowed(execution, foreign, exposure_known)
        blocked = self._risk_reasons(raw, risk_raw)
        if not execution and self.options.mode is SupervisorMode.DEMO:
            blocked.append("execution_not_activated")
        if self.state.fatal_latched:
            blocked.append("fatal_latched")
        if not exposure_known and self.options.mode is SupervisorMode.DEMO:
            blocked.append("exposure_unknown")
        if raw_execution is not True and self.options.mode is SupervisorMode.DEMO:
            blocked.append("execution_disabled")
        if paused:
            blocked.append("risk_paused")
        if halted:
            blocked.append("risk_halt")
        if management and not self._synthetic and not sqlite_wal_readiness()["external_continuous_ready"]:
            blocked.append("sqlite_wal_patch_not_verified")
        if management and not self._synthetic and self._source not in {"LIVE", "DEMO_OBSERVED"}:
            blocked.append("runtime_provenance_unknown")
        unique = tuple(dict.fromkeys(blocked))
        entries = entries and not unique
        management = management and not self.state.fatal_latched and self.state.persistence_ok
        reduction = (
            management
            and exposure_known
            and not foreign
            and self.state.reconciliation_state in _TERMINAL_RECONCILIATIONS
        )
        state = "READY" if entries else ("MANAGE_ONLY" if management else "BLOCKED")
        return RiskStatus(
            state,
            entries,
            management,
            reduction,
            exposure_known,
            own_exposure,
            unique,
            foreign,
        )

    def health_status(self) -> HealthStatus:
        now = self._now()
        last_progress = _parse_datetime(self.state.last_progress_at)
        last_market = _parse_datetime(self.state.last_market_at)
        progress_age = (now - last_progress).total_seconds() if last_progress is not None else None
        market_age = (now - last_market).total_seconds() if last_market is not None else None
        reasons: list[str] = []
        if self.state.fatal_latched:
            reasons.append("fatal_latched")
        if not self.state.persistence_ok:
            reasons.append("state_persistence_failed")
        if self.state.clock_anomaly:
            reasons.append("clock_anomaly")
        if self.state.suspension_detected:
            reasons.append("suspension_detected")
        if self.state.feed_state not in {"VALID", "NOT_APPLICABLE"} and self.state.feed_state != "CLOSED_MARKET":
            reasons.append("stale_feed")
        if self.state.feed_state == "CLOSED_MARKET":
            reasons.append("market_closed")
        lifecycle_good = self.state.lifecycle in {
            SupervisorLifecycle.STARTING.value,
            SupervisorLifecycle.OBSERVING.value,
            SupervisorLifecycle.SHADOW.value,
            SupervisorLifecycle.DEMO.value,
        }
        healthy = lifecycle_good and not reasons and self.state.persistence_ok
        return HealthStatus(
            healthy,
            self.state.lifecycle,
            self.state.connection_state,
            self.state.market_state,
            self.state.feed_state,
            self.state.freshness_state,
            self.state.reconciliation_state,
            self.state.persistence_ok,
            self._lock_held,
            self.state.fatal_latched,
            self.state.stop_requested,
            self.state.clock_anomaly,
            self.state.suspension_detected,
            self.state.last_progress_at,
            self.state.last_market_at,
            progress_age,
            market_age,
            tuple(dict.fromkeys(reasons)),
            self.state.error,
            self._lock_held
            and self.state.lifecycle
            in {
                SupervisorLifecycle.STARTING.value,
                SupervisorLifecycle.OBSERVING.value,
                SupervisorLifecycle.SHADOW.value,
                SupervisorLifecycle.DEMO.value,
                SupervisorLifecycle.RECONNECTING.value,
                SupervisorLifecycle.RECONCILING.value,
            },
        )

    def readiness_status(self) -> ReadinessStatus:
        risk = self.risk_status()
        reasons: list[str] = list(risk.blocked_reasons)
        if self.state.fatal_latched:
            reasons.append("fatal_latched")
        if self.state.stop_requested:
            reasons.append("stop_requested")
        if self.state.lifecycle not in _ACTIVE_LIFECYCLES:
            reasons.append("process_not_running")
        if not self.state.persistence_ok:
            reasons.append("state_persistence_failed")
        if self.state.connection_state not in _CONNECTED_STATES and not self._synthetic:
            reasons.append("connection_not_ready")
        if self.state.feed_state not in {"VALID", "NOT_APPLICABLE"} and not self._synthetic:
            reasons.append("feed_not_ready")
        if self.state.feed_state == "CLOSED_MARKET":
            reasons.append("market_closed")
        if self.state.reconciliation_state not in _TERMINAL_RECONCILIATIONS:
            reasons.append("reconciliation_pending")
        if self.options.mode is SupervisorMode.DEMO and not self.options.execution_requested:
            reasons.append("execution_not_activated")
        ready = not reasons and (self.options.mode is not SupervisorMode.DEMO or risk.entries_allowed)
        return ReadinessStatus(
            ready,
            cast(SupervisorMode, self.options.mode).value,
            self._source,
            self.options.execution_requested and self.context.callbacks.executor is not None,
            tuple(dict.fromkeys(reasons)),
            self.state.connection_state,
            self.state.feed_state,
            self.state.freshness_state,
            self.state.reconciliation_state,
        )

    def health(self) -> HealthStatus:
        return self.health_status()

    def readiness(self) -> ReadinessStatus:
        return self.readiness_status()

    def _resolve_hook(self, explicit: str, methods: Sequence[str]) -> Callable[..., Any] | None:
        candidate = getattr(self.context.callbacks, explicit, None)
        if callable(candidate):
            return cast(Callable[..., Any], candidate)
        executor = self.context.callbacks.executor
        for name in methods:
            candidate = getattr(executor, name, None) if executor is not None else None
            if callable(candidate):
                return cast(Callable[..., Any], candidate)
        return None

    def _reconcile(self) -> bool:
        callback = self._resolve_hook("reconcile", ("reconcile", "reconcile_order", "reconcile_account"))
        if callback is None:
            self.state.reconciliation_state = (
                "PENDING" if self.options.mode is SupervisorMode.DEMO else "NOT_APPLICABLE"
            )
            return self.state.reconciliation_state in _TERMINAL_RECONCILIATIONS
        self.state.lifecycle = SupervisorLifecycle.RECONCILING.value
        try:
            value = _invoke_callback(callback)
            self.state.reconciliations += 1
            self.state.progress_counter += 1
            verified = self._reconcile_value_verified(value)
            self.state.reconciliation_state = "VERIFIED" if verified else "UNKNOWN"
            return verified
        except Exception as exc:
            self.state.reconciliation_state = "UNKNOWN"
            self.state.risk_blocked_reasons = tuple(
                dict.fromkeys((*self.state.risk_blocked_reasons, "reconciliation_failed"))
            )
            self._set_error(exc, reason="reconciliation_failed")
            return False

    @staticmethod
    def _reconcile_value_verified(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, Mapping):
            if value.get("ok") is False or value.get("verified") is False:
                return False
            if str(value.get("state", "")).upper() in {"UNKNOWN", "UNRESOLVED", "PENDING"}:
                return False
            return value.get("ok") is True or value.get("verified") is True
        if isinstance(value, (list, tuple, set)):
            return bool(value) and all(CTraderSupervisor._reconcile_value_verified(item) for item in value)
        if hasattr(value, "reconciled"):
            state = str(getattr(getattr(value, "state", None), "value", getattr(value, "state", "UNKNOWN"))).upper()
            return value.reconciled is True and state not in {"UNKNOWN", "UNRESOLVED", "PENDING"}
        return False

    def _manage(self) -> bool:
        callback = self._resolve_hook("manage", ("manage", "manage_orders", "manage_exposure"))
        if callback is None:
            return True
        try:
            _invoke_callback(callback)
            self.state.progress_counter += 1
            return True
        except Exception as exc:
            self.state.risk_blocked_reasons = tuple(
                dict.fromkeys((*self.state.risk_blocked_reasons, "management_failed"))
            )
            self._set_error(exc, reason="management_failed")
            return False

    def _reduce_if_safe(self, reason: str) -> bool:
        callback = self._resolve_hook("reduce", ("reduce_exposure", "reduce", "close_own_positions"))
        risk = self.risk_status()
        if callback is None or not self.options.reduce_on_failure:
            return False
        if self.state.connection_state not in _CONNECTED_STATES or self.state.feed_state == "CLOSED_MARKET":
            return False
        if not risk.exposure_known or not risk.reduction_allowed:
            return False
        try:
            outcome = _invoke_callback(callback, reason)
            self.state.progress_counter += 1
            verified = isinstance(outcome, Mapping) and outcome.get("state") == "COMPLETED"
            if not verified:
                self.state.reconciliation_state = "UNKNOWN"
            with contextlib.suppress(Exception):
                self._alert.alert(
                    "exposure_reduction_requested" if verified else "reduction_outcome_unknown", {"reason": reason}
                )
            return verified
        except Exception as exc:
            self._set_error(exc, latch=True, reason="reduction_failed")
            return False

    def _risk_reduction_reason(self) -> str | None:
        if not self.state.execution_enabled:
            return None
        if self._reduction_attempted:
            return None
        raw = self._status_from_executor()
        risk_raw = raw.get("risk") if isinstance(raw.get("risk"), Mapping) else raw
        if not isinstance(risk_raw, Mapping):
            return None
        return self._risk_reason_from_snapshot(raw, risk_raw)

    def _risk_reason_from_snapshot(self, raw: Mapping[str, Any], risk_raw: Mapping[str, Any]) -> str | None:
        halt = risk_raw.get("halt_reason")
        foreign = risk_raw.get("foreign_positions")
        protection = str(risk_raw.get("protection_state", ""))
        position_state = str(risk_raw.get("position_state", ""))
        count, known = self._risk_exposure(raw, risk_raw)
        has_exposure = known and (count or 0) > 0
        if foreign:
            return "foreign_positions_present"
        if halt:
            return str(halt)
        if risk_raw.get("recovery_required") is True:
            return "execution_recovery_required"
        if protection in {"UNKNOWN", "UNVERIFIED"} and has_exposure:
            return "protection_unverified"
        if position_state == "UNKNOWN":
            return "position_reconciliation_failed"
        if self.state.feed_state in {"STALE", "DISCONNECTED"} and has_exposure:
            return "feed_stale"
        if self.state.stop_requested or (self._last_result is not None and self._last_result.clean_stop):
            return "supervisor_shutdown"
        return None

    def _apply_risk_reduction_cycle(self, reason: str) -> bool:
        self._reduction_attempted = True
        reconciled = self._reconcile()
        risk = self.risk_status()
        if not reconciled or risk.foreign_exposure:
            self._shutdown_clean = False
            self.state.reconciliation_state = "UNKNOWN"
            return False
        if not risk.exposure_known or (risk.own_exposure or 0) <= 0:
            # A known empty account needs no close operation; unknown exposure
            # is never treated as empty.
            if not risk.exposure_known:
                self._shutdown_clean = False
                self.state.reconciliation_state = "UNKNOWN"
                return False
            return True
        reduced = self._reduce_if_safe(reason)
        confirmed = self._reconcile()
        if not reduced or not confirmed:
            self._shutdown_clean = False
            self.state.reconciliation_state = "UNKNOWN"
            return False
        return True

    def _latch_confirmed_risk_breach(self, reason: str, reduced: bool) -> None:
        if reduced and reason in _LATCHED_RISK_BREACHES and not self.state.fatal_latched:
            self._latch_in_memory(reason, RuntimeError("risk breach latched"))

    def _apply_reconnect_result(self, refreshed: Any) -> None:
        if not isinstance(refreshed, Mapping):
            return
        if refreshed.get("ok") is False:
            raise SupervisionError("reconexión no dejó una sesión DEMO verificada")
        callbacks = refreshed.get("callbacks")
        if isinstance(callbacks, ExecutionCallbacks):
            self.context.callbacks = callbacks
        provenance = refreshed.get("provenance")
        if isinstance(provenance, Mapping):
            self.context.provenance = dict(provenance)
            self._source = _source_from_provenance(self.context.provenance, self.options.fixture)
            self._synthetic = _is_synthetic(self.context.provenance, self.options.fixture)
            self.state.source = self._source

    def _reconnect_once(self, reconnect: Callable[..., Any], provider: Any) -> bool:
        if self.context.reconnect is not None:
            # Old gateway/session evidence is invalid at a generation boundary.
            self.context.callbacks = ExecutionCallbacks()
        refreshed = _invoke_callback(reconnect)
        self._apply_reconnect_result(refreshed)
        self.state.connection_state = _connection_from_provider(provider)
        if self.state.connection_state not in _CONNECTED_STATES:
            raise SupervisionError("reconnect no dejó provider conectado")
        generation = getattr(provider, "generation", None)
        if isinstance(generation, int) and not isinstance(generation, bool) and generation >= 0:
            self.state.connection_generation = generation
            self._last_record_generation = generation
        self._refresh_market()
        if not self._reconcile():
            raise SupervisionError("reconnect requiere reconciliación verificable")
        self.state.connection_state = "CONNECTED"
        self.state.feed_state = "VALID" if self._synthetic else self.state.feed_state
        self.state.lifecycle = SupervisorLifecycle.PAUSED.value
        return True

    def _attempt_reconnect(self) -> bool:
        provider = self._provider
        reconnect = self.context.reconnect
        if reconnect is None:
            reconnect = getattr(provider, "reconnect", None) if provider is not None else None
        if not callable(reconnect) or provider is None:
            return False
        if self.state.fatal_latched or not self.state.persistence_ok:
            return False
        if not (self.options.network or self._synthetic):
            return False
        for attempt in range(self.options.max_reconnect_attempts):
            if self._stop_event.is_set():
                return False
            self.state.lifecycle = SupervisorLifecycle.RECONNECTING.value
            self.state.reconnects += 1
            self._persist(best_effort=True)
            try:
                return self._reconnect_once(cast(Callable[..., Any], reconnect), provider)
            except Exception as exc:
                self._set_error(exc, reason="reconnect_failed")
                if attempt + 1 < self.options.max_reconnect_attempts:
                    delay = min(
                        self.options.reconnect_backoff_max_seconds,
                        self.options.reconnect_backoff_seconds * (2**attempt),
                    )
                    self._sleep(delay)
        return False

    def _watch_context(self, config: EffectiveConfig) -> CTraderWatchContext:
        provenance = dict(self.context.provenance)
        provenance.setdefault("provider", "ctrader_open_api_fixture" if self._synthetic else "ctrader_open_api")
        provenance.setdefault("source_mode", self._source)
        provenance.setdefault("synthetic", self._synthetic)
        provenance.setdefault("environment", "OFFLINE" if self._synthetic else "DEMO")
        provenance.setdefault("network_performed", bool(self.options.network and not self._synthetic))
        # The existing watch normalizer is a read-only market boundary.  DEMO
        # execution remains a separate callback path owned by safety_audit.
        provenance["execution_enabled"] = False
        return CTraderWatchContext(
            cast(Any, self._provider),
            config,
            self.context.store,
            provenance=provenance,
            session_id=self.state.session_id,
            stop_event=self._stop_event,
            clock=self._clock,
            monotonic=self._monotonic,
            # H5 owns shutdown ordering: the watch borrows the provider until
            # manage/reduce/reconcile have completed.
            close_provider=False,
            on_initialized=self._watch_initialized,
        )

    def _watch_initialized(self, session_id: str, analysis_id: str) -> None:
        if self.state.session_id not in {None, session_id}:
            raise SupervisionError("watch session identity changed during supervision")
        if self.state.analysis_id not in {None, analysis_id}:
            raise SupervisionError("watch analysis identity changed during supervision")
        self.state.session_id = session_id
        self.state.analysis_id = analysis_id
        self.state.last_progress_at = _iso(_now_utc(self._clock))
        self._persist()

    def _watch_options(self) -> CTraderWatchOptions:
        duration = self.options.duration_seconds
        if duration is not None and self._watch_started_monotonic is not None:
            duration = max(0.0, duration - (self._monotonic() - self._watch_started_monotonic))
        max_events = self.options.max_events
        if max_events is not None:
            max_events = max_events - self._watch_messages_consumed
            if max_events <= 0:
                raise SupervisionError("watch budget de mensajes agotado")
        return CTraderWatchOptions(
            duration_seconds=duration,
            max_events=max_events,
            idle_timeout_seconds=self.options.idle_timeout_seconds,
            poll_timeout_seconds=self.options.poll_timeout_seconds,
            checkpoint_every=self.options.checkpoint_every,
            max_candles=self.options.max_candles,
            resume=self.options.resume,
            mode="SYNTHETIC" if self._synthetic else "LIVE",
        )

    def _run_watch_slice(self) -> tuple[CTraderWatchResult, tuple[Any, ...]]:
        if self._provider is None:
            raise SupervisionError("no hay provider preparado")
        config = self.context.config
        if self._synthetic and config.mode != "SYNTHETIC":
            data = dict(config.data)
            data["mode"] = "SYNTHETIC"
            config = replace(config, mode="SYNTHETIC", data=data)
        watch_context = self._watch_context(config)
        options = self._watch_options()
        factory = self.context.watch_runner_factory
        runner: Any
        runner = (
            _SupervisedWatchRunner(
                watch_context,
                options,
                self._publish_progress_snapshot,
                self._dispatch_signal,
                self._observe_record,
                self._observe_runtime_status,
                self._service_tick,
            )
            if factory is None
            else factory(watch_context, options)
        )
        self._active_watch_runner = runner
        result = (
            runner
            if isinstance(runner, CTraderWatchResult)
            else runner.run()
            if hasattr(runner, "run")
            else _invoke_callback(cast(Callable[..., Any], runner))
        )
        if not isinstance(result, CTraderWatchResult):
            raise SupervisionError("watch runner no devolvió CTraderWatchResult")
        self._watch_messages_consumed += result.messages_this_run
        coordinator = getattr(runner, "coordinator", None)
        signals_value = getattr(coordinator, "signals", ()) if coordinator is not None else ()
        signals = tuple(signals_value) if isinstance(signals_value, Iterable) else ()
        return result, signals

    def _capture_failed_watch_progress(self) -> None:
        runner = self._active_watch_runner
        stats = getattr(runner, "stats", None)
        consumed = getattr(stats, "messages", None)
        start = getattr(runner, "_run_messages_start", 0)
        if start == 0 and bool(getattr(runner, "_resumed", False)):
            return
        if isinstance(consumed, int) and isinstance(start, int):
            self._watch_messages_consumed += max(0, consumed - start)

    def _watch_failure_retryable(self, error: BaseException) -> bool:
        if self._stop_event.is_set() or self.state.fatal_latched:
            return False
        if not (self.options.network or self._synthetic):
            return False
        markers = (
            "identity",
            "checkpoint",
            "config",
            "provenance",
            "resume",
            "schema",
            "permission",
            "corrupt",
            "instrument",
            "generation",
            "discontinuity",
            "incompatible",
            "unknown",
            "scope",
            "permiso",
            "cuenta",
            "endpoint",
            "entorno",
            "token",
            "lease",
        )
        detail = f"{type(error).__name__} {error}".lower()
        return not any(marker in detail for marker in markers)

    def _publish_progress_snapshot(self) -> None:
        self.state.progress_counter += 1
        now = _now_utc(self._clock)
        self.state.last_progress_at = _iso(now)
        self._refresh_market(now)
        self._update_state_projections()
        self._last_published_progress = self.state.progress_counter
        self._last_published_monotonic = self._monotonic()
        self._persist()

    def _observe_record(self, record: Any) -> None:
        now = self._now()
        self._runtime_connection_state = "CONNECTED"
        self._runtime_recovery_attempted = False
        self.state.events += 1
        self.state.progress_counter += 1
        self.state.last_progress_at = _iso(now)
        available_at = getattr(record, "available_at", None)
        if not isinstance(available_at, datetime):
            available_at = getattr(record, "effective_available_at", None)
        if isinstance(available_at, datetime) and available_at.tzinfo is not None:
            self._last_available_at = available_at.astimezone(UTC)
        market_time = getattr(record, "event_time", getattr(record, "interval_end", None))
        if isinstance(market_time, datetime) and market_time.tzinfo is not None:
            self.state.last_market_at = _iso(market_time)
        metadata = getattr(record, "metadata", {})
        if isinstance(metadata, Mapping):
            generation = metadata.get("generation", metadata.get("connection_generation"))
            if isinstance(generation, int) and not isinstance(generation, bool) and generation >= 0:
                self.state.connection_generation = generation
                self._last_record_generation = generation
            quality = str(metadata.get("quality_state", "")).upper()
            if not self._synthetic:
                if metadata.get("quote_usable") is True or quality == "VALID":
                    self.state.feed_state = "VALID"
                    self.state.freshness_state = "VALID"
                    if self.state.lifecycle == SupervisorLifecycle.PAUSED.value:
                        self.state.lifecycle = {
                            SupervisorMode.OBSERVE: SupervisorLifecycle.OBSERVING.value,
                            SupervisorMode.SHADOW: SupervisorLifecycle.SHADOW.value,
                            SupervisorMode.DEMO: SupervisorLifecycle.DEMO.value,
                        }[cast(SupervisorMode, self.options.mode)]
                elif quality in {"STALE", "UNKNOWN", "INVALID", "INCOMPLETE"}:
                    self.state.feed_state = "STALE" if quality == "STALE" else quality
                    self.state.freshness_state = self.state.feed_state

    def _observe_runtime_status(self, coordinator: Any) -> None:
        status = coordinator.status()
        connection = str(getattr(status, "connection", "UNKNOWN")).upper()
        self._runtime_connection_state = connection
        if connection in {"DISCONNECTED", "ERROR"}:
            self.state.connection_state = connection
            self.state.feed_state = "DISCONNECTED"
            self.state.freshness_state = "DISCONNECTED"
            self.state.reconciliation_state = "NEEDS_RECONCILIATION"
            if not self._runtime_recovery_attempted and not self.state.stop_requested:
                self._runtime_recovery_attempted = True
                if not self._attempt_reconnect():
                    self.request_stop("RECONCILIATION_REQUIRED")

    def _risk_allows(self, signal: Any) -> bool:
        risk = self.context.callbacks.risk
        if risk is None:
            return True
        for name in ("allow_entry", "can_open", "check_entry"):
            callback = getattr(risk, name, None)
            if callable(callback):
                value = _invoke_callback(callback, signal)
                if isinstance(value, Mapping):
                    return bool(value.get("allowed", value.get("ok", False)))
                return bool(value)
        return bool(getattr(risk, "entries_allowed", False))

    @staticmethod
    def _signal_key(signal: Any) -> str:
        value = signal.get("signal_id") if isinstance(signal, Mapping) else getattr(signal, "signal_id", None)
        return str(value) if value is not None else "MISSING_SIGNAL_ID"

    def _remember_signal(self, key: str) -> None:
        if key in self._delivered_signal_ids:
            return
        if len(self._delivered_signal_order) == self._delivered_signal_order.maxlen:
            self._delivered_signal_ids.discard(self._delivered_signal_order.popleft())
        self._delivered_signal_order.append(key)
        self._delivered_signal_ids.add(key)

    def _dispatch_signal(self, signal: Any) -> None:
        key = self._signal_key(signal)
        if key in self._delivered_signal_ids:
            return
        self._remember_signal(key)
        self.state.signals += 1
        self._process_signals((signal,), allow_delivered=True)
        if self.state.progress_counter > self._last_published_progress:
            self._publish_progress_snapshot()

    def _service_tick(self) -> None:
        now = self._now()
        if self.state.fatal_latched or self.state.stop_requested:
            return
        provider_connection = _connection_from_provider(self._provider)
        if self._runtime_connection_state not in {"DISCONNECTED", "ERROR"}:
            self.state.connection_state = provider_connection
        self._refresh_market(now)
        current_mono = self._monotonic()
        if current_mono - self._last_manage_monotonic >= self._manage_interval_seconds:
            self._last_manage_monotonic = current_mono
            if not self._manage():
                self.request_stop("STOP_REQUESTED")
        if (
            self.state.progress_counter > self._last_published_progress
            and current_mono - self._last_published_monotonic >= self._publish_interval_seconds
        ):
            self._publish_progress_snapshot()
        reduction_reason = self._risk_reduction_reason()
        if reduction_reason is not None:
            reduced = self._apply_risk_reduction_cycle(reduction_reason)
            self._latch_confirmed_risk_breach(reduction_reason, reduced)
            self.request_stop("RISK_REDUCTION")
        if provider_connection not in _CONNECTED_STATES and not self._attempt_reconnect():
            self.request_stop("PROVIDER_DISCONNECTED")

    def _submit_signal(self, signal: Any) -> bool:
        transient = {
            "signal_availability_unknown",
            "signal_availability_future",
            "signal_expired",
            "signal_generation_unknown",
            "risk_rejected",
        }
        self.state.risk_blocked_reasons = tuple(
            item for item in self.state.risk_blocked_reasons if item not in transient
        )
        if self._signal_key(signal) == "MISSING_SIGNAL_ID":
            return False
        callback = self._resolve_hook("on_signal", ("submit_signal", "execute_signal", "execute", "submit"))
        if callback is None:
            self.state.risk_blocked_reasons = tuple(
                dict.fromkeys((*self.state.risk_blocked_reasons, "executor_not_configured"))
            )
            return False
        if not self._signal_is_fresh(signal):
            return False
        if not self.risk_status().entries_allowed or not self._risk_allows(signal):
            self.state.risk_blocked_reasons = tuple(dict.fromkeys((*self.state.risk_blocked_reasons, "risk_rejected")))
            return False
        try:
            values = getattr(signal, "values", {})
            quote = values.get("quote") if isinstance(values, Mapping) else None
            if quote is None and self.context.callbacks.quote_resolver is not None:
                quote = self.context.callbacks.quote_resolver(signal)
            if quote is None:
                _invoke_callback(callback, signal)
            else:
                _invoke_callback(callback, signal, quote)
            self.state.progress_counter += 1
            return True
        except Exception as exc:
            self._set_error(exc, reason="signal_submission_failed")
            return False

    @staticmethod
    def _mapping_field(value: Any, names: Sequence[str]) -> Any:
        if not isinstance(value, Mapping):
            return None
        for name in names:
            if name in value:
                return value[name]
        return None

    @classmethod
    def _signal_field(cls, signal: Any, *names: str) -> Any:
        if isinstance(signal, Mapping):
            found = cls._mapping_field(signal, names)
            if found is not None:
                return found
            found = cls._mapping_field(signal.get("values"), names)
            if found is not None:
                return found
        values = getattr(signal, "values", None)
        found = cls._mapping_field(values, names)
        if found is not None:
            return found
        for name in names:
            value = getattr(signal, name, None)
            if value is not None:
                return value
        return None

    def _signal_is_fresh(self, signal: Any) -> bool:
        now = _now_utc(self._clock)
        available_raw = self._signal_field(signal, "signal_available_at", "available_at", "available_ts")
        available = _parse_datetime(available_raw) if available_raw is not None else self._last_available_at
        detected = _parse_datetime(self._signal_field(signal, "detected_at", "timestamp"))
        if available is None or detected is None:
            self.state.risk_blocked_reasons = tuple(
                dict.fromkeys((*self.state.risk_blocked_reasons, "signal_availability_unknown"))
            )
            return False
        if available > now or detected > now:
            self.state.risk_blocked_reasons = tuple(
                dict.fromkeys((*self.state.risk_blocked_reasons, "signal_availability_future"))
            )
            return False
        limit = self.options.stale_after_seconds
        raw = self._status_from_executor()
        policy = raw.get("risk_policy")
        if isinstance(policy, Mapping):
            candidate = policy.get("max_price_age_seconds")
            if candidate is not None:
                try:
                    limit = min(limit, _finite_number(candidate, "max_price_age_seconds"))
                except ValueError:
                    limit = 0.0
        if (now - available).total_seconds() > limit or (now - detected).total_seconds() > limit:
            self.state.risk_blocked_reasons = tuple(dict.fromkeys((*self.state.risk_blocked_reasons, "signal_expired")))
            return False
        generation = self._signal_field(signal, "connection_generation", "generation")
        if generation is None:
            generation = self._last_record_generation
        current_generation = getattr(self._provider, "generation", self.state.connection_generation)
        if generation is None or current_generation is None or str(generation) != str(current_generation):
            self.state.risk_blocked_reasons = tuple(
                dict.fromkeys((*self.state.risk_blocked_reasons, "signal_generation_unknown"))
            )
            return False
        return True

    def _process_signals(self, signals: Sequence[Any], *, allow_delivered: bool = False) -> int:
        processed = 0
        for signal_value in signals:
            processed += 1
            key = self._signal_key(signal_value)
            if key in self._delivered_signal_ids and not allow_delivered:
                continue
            self._remember_signal(key)
            if self.state.stop_requested or self.state.fatal_latched:
                continue
            if self.options.mode is SupervisorMode.OBSERVE:
                continue
            if self.options.mode is SupervisorMode.SHADOW:
                callback = self.context.callbacks.on_signal
                if callback is not None:
                    with contextlib.suppress(Exception):
                        _invoke_callback(callback, signal_value)
                continue
            if self.options.execution_requested and not self.state.fatal_latched:
                self._submit_signal(signal_value)
            else:
                self.state.risk_blocked_reasons = tuple(
                    dict.fromkeys((*self.state.risk_blocked_reasons, "execution_not_activated"))
                )
        return processed

    def _apply_watch_result(self, result: CTraderWatchResult) -> None:
        self._last_result = result
        self.state.session_id = result.session_id
        self.state.analysis_id = result.analysis_id
        self.state.messages = max(self.state.messages, result.messages)
        self.state.events = max(self.state.events, result.events)
        self.state.signals = len(self._last_signals)
        status = result.status
        self.state.connection_state = str(status.get("connection", "DISCONNECTED")).upper()
        self.state.reconciliation_state = str(status.get("reconciliation_state", "PENDING")).upper()
        self.state.freshness_state = str(status.get("freshness_state", "UNKNOWN")).upper()
        self.state.last_market_at = _iso(_parse_datetime(status.get("last_market_time")))
        self.state.last_progress_at = _iso(_now_utc(self._clock))
        if self._synthetic:
            self.state.feed_state = "NOT_APPLICABLE"
            self.state.market_state = "OFFLINE"
            self.state.freshness_state = "NOT_APPLICABLE"
        else:
            self._refresh_market()
        if result.clean_stop:
            self.state.stop_reason = result.stop_reason

    def _begin_run(self) -> None:
        self.state.lifecycle = SupervisorLifecycle.STARTING.value
        self.state.execution_enabled = self.options.execution_requested and self.context.callbacks.executor is not None
        self.state.source = self._source
        self.state.started_at = _iso(self._now())
        self.state.stop_requested = False
        self._persist()

    def _prepare_provider_for_run(self) -> Any:
        provider = self._provider
        if provider is None and self.context.provider_factory is not None:
            if not (self.options.fixture or self.options.network):
                raise NetworkActivationRequired("provider_factory requiere fixture o network explícito")
            produced = self.context.provider_factory()
            if isinstance(produced, tuple) and len(produced) == 2:
                provider, extra = produced
                if isinstance(extra, Mapping):
                    self.context.provenance = {**dict(self.context.provenance), **dict(extra)}
                    self._source = _source_from_provenance(self.context.provenance, self.options.fixture)
                    self._synthetic = _is_synthetic(self.context.provenance, self.options.fixture)
                    self.state.source = self._source
            else:
                provider = produced
            self._provider = provider
        if provider is None:
            raise SupervisionError("no hay provider; use fixture o entregue una sesión preparada")
        if not self.options.fixture and not self.options.network and not self._synthetic:
            raise NetworkActivationRequired("provider no sintético requiere network=True explícito")
        self.state.connection_state = _connection_from_provider(provider)
        self._update_state_projections()
        self._refresh_market()
        with contextlib.suppress(Exception):
            self._watchdog.notify_ready(self.state.lifecycle)
        self._persist()
        if self.state.market_state in _CLOSED_STATES:
            self.state.stop_reason = "MARKET_CLOSED"
            self.state.lifecycle = SupervisorLifecycle.PAUSED.value
            self._persist()
            return provider
        if self.state.execution_enabled and not self._reconcile():
            raise SupervisionError("la ejecución DEMO requiere reconciliación inicial verificable")
        if self.state.connection_state not in _CONNECTED_STATES and not self._attempt_reconnect():
            raise SupervisionError("provider no está conectado y no pudo recuperarse")
        self.state.lifecycle = {
            SupervisorMode.OBSERVE: SupervisorLifecycle.OBSERVING.value,
            SupervisorMode.SHADOW: SupervisorLifecycle.SHADOW.value,
            SupervisorMode.DEMO: SupervisorLifecycle.DEMO.value,
        }[cast(SupervisorMode, self.options.mode)]
        self._persist()
        return provider

    def _run_watch_and_recover(self) -> None:
        self._watch_started_monotonic = self._monotonic()
        attempts = 0
        while True:
            try:
                result, signals = self._run_watch_slice()
                self._last_signals = signals
                self._apply_watch_result(result)
                self.state.signals = self._process_signals(signals)
                if not self.state.fatal_latched:
                    self.state.error = None
                    self._primary_error = None
                return
            except Exception as exc:
                self._capture_failed_watch_progress()
                self._set_error(exc, reason="watch_failed")
                retry_allowed = self._watch_failure_retryable(exc)
                retry_allowed = retry_allowed and attempts < self.options.max_reconnect_attempts
                if not retry_allowed:
                    self._latch_in_memory("watch_failed_unrecovered", exc)
                    return
                attempts += 1
                if not self._attempt_reconnect():
                    self._latch_in_memory("watch_failed_unrecovered", exc)
                    return
                self.state.error = None
                self._primary_error = None
                self.state.lifecycle = {
                    SupervisorMode.OBSERVE: SupervisorLifecycle.OBSERVING.value,
                    SupervisorMode.SHADOW: SupervisorLifecycle.SHADOW.value,
                    SupervisorMode.DEMO: SupervisorLifecycle.DEMO.value,
                }[cast(SupervisorMode, self.options.mode)]

    def _update_state_projections(self) -> None:
        generation = getattr(self._provider, "generation", None)
        self.state.connection_generation = (
            generation if isinstance(generation, int) and not isinstance(generation, bool) and generation >= 0 else None
        )
        raw = self._status_from_executor()
        pending = raw.get("pending_management", raw.get("backlog", 0))
        if isinstance(pending, int) and not isinstance(pending, bool) and pending >= 0:
            self.state.backlog = pending
        risk = self.risk_status()
        risk_raw = raw.get("risk") if isinstance(raw.get("risk"), Mapping) else raw
        if not isinstance(risk_raw, Mapping):
            risk_raw = {}
        self.state.risk_state = risk.state
        self.state.exposure_state = "KNOWN" if risk.exposure_known else "UNKNOWN"
        protection = raw.get("protection_state", raw.get("protections", risk_raw.get("protection_state", "UNKNOWN")))
        economic = raw.get("economic_state", raw.get("economic", risk_raw.get("economic_state", "UNKNOWN")))
        self.state.protection_state = str(protection).upper()
        self.state.economic_state = str(economic).upper()

    def _shutdown_needs_reduction(self) -> str | None:
        if not self.state.execution_enabled:
            return None
        if self._reduction_attempted:
            return None
        return self._risk_reduction_reason() or "supervisor_shutdown"

    def _close_provider_after_management(self) -> None:
        provider = self._provider
        close = getattr(provider, "close", None) if provider is not None else None
        if not callable(close):
            return
        try:
            close()
            self.state.connection_state = "DISCONNECTED"
        except Exception as exc:
            self._shutdown_clean = False
            self._set_error(exc, latch=True, reason="provider_close_failed")

    def _finish_run(self) -> None:
        if not self._manage():
            self._shutdown_clean = False
        reduction_reason = self._shutdown_needs_reduction()
        if reduction_reason is not None:
            self._reconciled_before_summary = self._apply_risk_reduction_cycle(reduction_reason)
            self._latch_confirmed_risk_breach(reduction_reason, self._reconciled_before_summary)
            if not self._reconciled_before_summary:
                self._shutdown_clean = False
        else:
            self._reconciled_before_summary = self._reconcile()
        self._reconciled_before_summary = self._reconciled_before_summary and self._shutdown_clean
        self._update_state_projections()
        self._close_provider_after_management()
        if self.state.fatal_latched:
            self.state.lifecycle = SupervisorLifecycle.FATAL_LATCHED.value
        elif self.state.stop_requested:
            self.state.lifecycle = SupervisorLifecycle.STOPPED.value
        elif self.state.error is not None and self._last_result is None:
            self.state.lifecycle = SupervisorLifecycle.ERROR.value
        elif not self.state.stop_requested and self.state.lifecycle != SupervisorLifecycle.PAUSED.value:
            self.state.lifecycle = SupervisorLifecycle.STOPPED.value
        self.state.stopped_at = _iso(_now_utc(self._clock))
        self._persist(best_effort=True)

    def _run_locked(self) -> SupervisorResult:
        self._load_state()
        if self.state.fatal_latched:
            self._close_provider_after_management()
            return self._result(clean_stop=False, stop_reason="FATAL_LATCHED")
        self._begin_run()
        early_stop = False
        try:
            self._prepare_provider_for_run()
            if self.state.market_state in _CLOSED_STATES:
                early_stop = True
            else:
                self._run_watch_and_recover()
        except Exception as exc:
            self._set_error(exc, reason="supervision_prepare_failed")
        finally:
            self._finish_run()
        if early_stop:
            return self._result(
                clean_stop=self._shutdown_clean and self._reconciled_before_summary and self.state.error is None,
                stop_reason="MARKET_CLOSED",
            )
        if (
            self._last_result is not None
            and self._last_result.clean_stop
            and not self.state.fatal_latched
            and self.state.error is None
            and self._shutdown_clean
            and self._reconciled_before_summary
        ):
            return self._result(clean_stop=True, stop_reason=self._last_result.stop_reason)
        return self._result(clean_stop=False, stop_reason=self.state.stop_reason or "ERROR")

    def _result(self, *, clean_stop: bool, stop_reason: str) -> SupervisorResult:
        try:
            self._refresh_market()
        except Exception as exc:
            self._set_error(exc, latch=True, reason="health_refresh_failed")
        health = self.health_status()
        readiness = self.readiness_status()
        risk = self.risk_status()
        ok = clean_stop and not self.state.fatal_latched and self.state.error is None and self.state.persistence_ok
        return SupervisorResult(
            ok,
            self.state.lifecycle,
            cast(SupervisorMode, self.options.mode).value,
            self._source,
            self.state.session_id,
            self.state.analysis_id,
            self.state.messages,
            self.state.events,
            self.state.signals,
            self.state.reconciliations,
            self.state.reconnects,
            stop_reason,
            clean_stop,
            self.options.execution_requested and self.context.callbacks.executor is not None,
            self._network_attempted,
            self.state.fatal_latched,
            self._reconciled_before_summary,
            health,
            readiness,
            risk,
            str(self.state_store.state_path),
            self.state.error or self._primary_error,
            {**dict(self.context.provenance), "source_mode": self._source, "synthetic": self._synthetic},
        )

    def run(self) -> SupervisorResult:
        """Run one bounded slice while holding the per-account writer lock."""
        try:
            with self.state_store.lock():
                self._lock_held = True
                try:
                    return self._run_locked()
                finally:
                    self._lock_held = False
        except SingleWriterBusy as exc:
            self._set_error(exc, reason="single_writer_busy")
            return self._result(clean_stop=False, stop_reason="SINGLE_WRITER_BUSY")
        except (StateCorrupt, NetworkActivationRequired, SupervisionError) as exc:
            self._set_error(exc, latch=isinstance(exc, StatePersistenceFailure), reason=type(exc).__name__)
            return self._result(clean_stop=False, stop_reason=type(exc).__name__.upper())
        except Exception as exc:
            self._set_error(exc, latch=True, reason="unhandled_supervision_failure")
            return self._result(clean_stop=False, stop_reason="ERROR")

    def clear_latch(self) -> None:
        """Explicit human re-arm; never called automatically by recovery."""
        with self.state_store.lock():
            self._load_state()
            if not self.state.fatal_latched:
                return
            self.state.fatal_latched = False
            self.state.latch_reason = None
            self.state.error = None
            self.state.lifecycle = SupervisorLifecycle.CREATED.value
            self.state.persistence_ok = True
            self.state.state_persisted = True
            self.state.stop_requested = False
            self.state.risk_blocked_reasons = ()
            self.state_store.save(self.state)


def _source_required_result() -> CommandResult:
    return CommandResult.json(
        {
            "ok": False,
            "state": "SOURCE_REQUIRED",
            "network_attempted": False,
            "execution_enabled": False,
        },
        code=2,
        stderr=True,
    )


def run_ctrader_supervise(
    context: SupervisorContext,
    options: SupervisorOptions | None = None,
    *,
    alert_sink: AlertSink | None = None,
    watchdog: WatchdogNotifier | None = None,
) -> SupervisorResult:
    """Public functional entry point for CLI/API composition roots."""
    return CTraderSupervisor(context, options, alert_sink=alert_sink, watchdog=watchdog).run()


def _safe_database_path(value: Any) -> Path:
    if value is None:
        raise SupervisionError("supervise requiere --db explícito")
    path = Path(value).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise SupervisionError("--db no puede ser symlink")
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise SupervisionError("--db debe ser archivo regular")
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)
    return path


def _options_from_args(args: Any) -> SupervisorOptions:
    mode = getattr(args, "mode", getattr(args, "supervise_mode", "observe"))
    fixture = bool(getattr(args, "fixture", False))
    network = bool(getattr(args, "network", False))
    activate = bool(getattr(args, "activate", getattr(args, "execute", False)))
    continuous = bool(getattr(args, "continuous", False))
    duration = None if continuous else getattr(args, "duration", 30.0)
    max_events = None if continuous else getattr(args, "max_events", getattr(args, "max_messages", 100))
    idle_timeout = getattr(args, "idle_timeout", 5.0)
    return SupervisorOptions(
        mode=mode,
        fixture=fixture,
        network=network,
        activate=activate,
        duration_seconds=duration,
        max_events=max_events,
        idle_timeout_seconds=idle_timeout,
        poll_timeout_seconds=getattr(args, "poll_timeout", 0.25),
        checkpoint_every=getattr(args, "checkpoint_every", 100),
        max_candles=getattr(args, "max_candles", 5000),
        max_reconnect_attempts=getattr(args, "max_reconnect_attempts", 3),
        reconnect_backoff_seconds=getattr(args, "reconnect_backoff_seconds", 0.25),
        stale_after_seconds=getattr(args, "stale_after_seconds", 90.0),
        account_key=getattr(args, "account_key", _DEFAULT_ACCOUNT_KEY),
        state_dir=getattr(args, "state_dir", None),
        db_path=getattr(args, "db", None),
        notify_socket=getattr(args, "notify_socket", None)
        or (os.environ.get("NOTIFY_SOCKET") if bool(getattr(args, "watchdog", False)) else None),
        watchdog_enabled=bool(getattr(args, "watchdog", False)),
        dbus_alerts=bool(getattr(args, "dbus_alerts", False)),
        reduce_on_failure=not bool(getattr(args, "no_reduce", False)),
        resume=bool(getattr(args, "resume", True)),
        continuous=continuous,
    )


class CommandService:
    """Public command/API service; current CLI integration remains upstream."""

    def __init__(
        self,
        *,
        provider_factory: ProviderFactory | None = None,
        watch_runner_factory: WatchRunnerFactory | None = None,
        alert_sink: AlertSink | None = None,
        watchdog_factory: Callable[[SupervisorOptions, MonoClock], WatchdogNotifier] | None = None,
        callbacks_factory: CallbacksFactory | None = None,
        callbacks: ExecutionCallbacks | None = None,
    ) -> None:
        self.provider_factory = provider_factory
        self.watch_runner_factory = watch_runner_factory
        self.alert_sink = alert_sink
        self.watchdog_factory = watchdog_factory
        self.callbacks_factory = callbacks_factory
        self.callbacks = callbacks or ExecutionCallbacks()

    def _fixture_provider(self, config: EffectiveConfig, count: int) -> tuple[Any, Clock]:
        # Reuse the existing real provider, deterministic transport and runtime
        # fixture.  There is deliberately no parallel mock-only engine here.
        from .ctrader_watch_cli import _fixture_provider

        return cast(tuple[Any, Clock], _fixture_provider(config, 0, max(130, count)))

    def _prepare_source(
        self, args: Any, options: SupervisorOptions, config: EffectiveConfig
    ) -> tuple[Any | None, Mapping[str, Any], Any | None, EffectiveConfig] | CommandResult:
        if options.fixture:
            provider, _fixture_clock = self._fixture_provider(config, options.max_events or 130)
            data = dict(config.data)
            data["mode"] = "SYNTHETIC"
            fixture_config = replace(config, mode="SYNTHETIC", data=data)
            provenance = {
                "provider": "ctrader_open_api_fixture",
                "source_mode": "SYNTHETIC_FIXTURE",
                "synthetic": True,
                "environment": "OFFLINE",
                "network_performed": False,
                "execution_enabled": False,
                "data_identity": "mtf-ctrader-supervise-fixture-v1",
            }
            return provider, provenance, None, fixture_config
        if self.provider_factory is not None:
            produced = self.provider_factory()
            if isinstance(produced, tuple) and len(produced) == 2:
                provider, extra = produced
                provenance = dict(extra) if isinstance(extra, Mapping) else {}
            else:
                provider, provenance = produced, {}
            return provider, provenance, None, config
        from .supervision_composition import prepare_network_supervision

        prepared = prepare_network_supervision(args)
        if isinstance(prepared, CommandResult):
            return prepared
        return prepared.provider, prepared.provenance, prepared, config

    def run(self, args: Any) -> CommandResult:
        network_preparation: Any | None = None
        provider: Any | None = None
        supervisor_completed = False
        try:
            options = _options_from_args(args)
            if not options.fixture and not options.network:
                return _source_required_result()
            config_path = getattr(args, "config", None) or packaged_config_path("ctrader_query.toml")
            config = load_config(config_path)
            prepared = self._prepare_source(args, options, config)
            if isinstance(prepared, CommandResult):
                return prepared
            provider, provenance, network_preparation, config = prepared
            db = _safe_database_path(options.db_path)
            callbacks = self.callbacks
            prepared_callbacks = getattr(network_preparation, "callbacks", None)
            if callable(getattr(prepared_callbacks, "on_signal", None)) or prepared_callbacks is not None:
                callbacks = cast(ExecutionCallbacks, prepared_callbacks)
            if self.callbacks_factory is not None:
                callbacks = self.callbacks_factory(provider, provenance)
            with SQLiteStore(db) as store, CooperativeStop() as stop:
                context = SupervisorContext(
                    provider,
                    config,
                    store,
                    provenance=provenance,
                    provider_factory=None,
                    callbacks=callbacks,
                    watch_runner_factory=self.watch_runner_factory,
                    stop_event=stop,
                    reconnect=(network_preparation.reconnect if network_preparation is not None else None),
                )
                watchdog = (
                    self.watchdog_factory(options, context.monotonic or time.monotonic)
                    if self.watchdog_factory is not None
                    else None
                )
                result = run_ctrader_supervise(context, options, alert_sink=self.alert_sink, watchdog=watchdog)
                supervisor_completed = True
            report = getattr(args, "report", None)
            if report is not None:
                self._write_report(Path(report), result.to_dict())
            return CommandResult.json(result.to_dict(), code=0 if result.ok else 2, stderr=not result.ok)
        except Exception as exc:
            return CommandResult.json(
                {
                    "ok": False,
                    "state": type(exc).__name__,
                    "error": _safe_error_text(exc),
                    "network_attempted": False,
                },
                code=2,
                stderr=True,
            )
        finally:
            if network_preparation is not None:
                with contextlib.suppress(Exception):
                    network_preparation.close()
            elif provider is not None and not supervisor_completed:
                with contextlib.suppress(Exception):
                    provider.close()

    @staticmethod
    def _write_report(path: Path, payload: Mapping[str, Any]) -> None:
        if path.is_symlink():
            raise SupervisionError("report no puede ser symlink")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        encoded = canonical_json(dict(payload)).encode("utf-8") + b"\n"
        fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as stream:
                fd = -1
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, path)
            temp_name = ""
        finally:
            if fd >= 0:
                with contextlib.suppress(OSError):
                    os.close(fd)
            if temp_name:
                with contextlib.suppress(OSError):
                    os.unlink(temp_name)

    def health(self, supervisor: CTraderSupervisor) -> HealthStatus:
        return supervisor.health_status()

    def readiness(self, supervisor: CTraderSupervisor) -> ReadinessStatus:
        return supervisor.readiness_status()

    def risk(self, supervisor: CTraderSupervisor) -> RiskStatus:
        return supervisor.risk_status()


def supervise_command(args: Any, *, service: CommandService | None = None) -> tuple[int, JsonObject]:
    """CLI-friendly adapter without coupling this module to argparse.

    The root CLI can render the returned payload directly.  No source flag
    means no provider construction; a live source is still the responsibility
    of an explicitly authorised composition root.
    """
    result = (service or CommandService()).run(args)
    if isinstance(result.payload, Mapping):
        payload = dict(result.payload)
    else:
        payload = {"ok": result.code == 0, "text": result.text}
    return int(result.code), payload


# Names kept as stable aliases while the root CLI selects its preferred label.
SupervisionCommandService = CommandService
CTraderSupervisionService = CommandService
Supervisor = CTraderSupervisor


__all__ = [
    "AlertSink",
    "CTraderSupervisionService",
    "CTraderSupervisor",
    "CommandService",
    "CompositeAlertSink",
    "CooperativeStop",
    "DbusAlertSink",
    "ExecutionCallbacks",
    "ExecutorPort",
    "HealthStatus",
    "HumanLatchRequired",
    "JournalAlertSink",
    "NetworkActivationRequired",
    "PersistentSupervisorState",
    "ProviderPort",
    "ReadinessStatus",
    "RiskPort",
    "RiskStatus",
    "SUPERVISION_SCHEMA_VERSION",
    "SingleWriterBusy",
    "StateCorrupt",
    "StatePersistenceFailure",
    "Supervisor",
    "SupervisorContext",
    "SupervisorLifecycle",
    "SupervisorMode",
    "SupervisorOptions",
    "SupervisorResult",
    "SupervisorStateStore",
    "SupervisionCommandService",
    "SupervisionError",
    "WatchdogNotifier",
    "read_supervisor_readiness",
    "run_ctrader_supervise",
    "supervise_command",
]
