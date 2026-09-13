#!/usr/bin/env python3
"""Local, resumable reliability soak harness for MTF Lab.

The harness is deliberately conservative:

* the default runner reuses the validated ``CommandService`` cTrader fixture;
  it never enables network, OAuth, an account or execution;
* a normal run requests at least 72 hours (259200 seconds), while
  ``--smoke`` is explicitly short and can never be accepted as a 72-hour
  result;
* wall-clock and monotonic time are both recorded, suspension/clock gaps are
  discounted and fail the acceptance gate;
* checkpoints are private, bounded, atomic and fenced to the current code and
  input identities; resume time is recorded as downtime rather than useful
  soak time.

This file is a tool/API only.  It does not install a unit, alter systemd,
create an automation, contact a broker or create a goal.  The command defaults
to ``--dry-run``; use ``--smoke`` for a bounded local fixture exercise or
``--execute`` for the explicitly requested long local run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mtf_lab.core.canonical import canonical_json, fingerprint, instant_text

MIN_SOAK_SECONDS = 72 * 60 * 60
SOAK_SCHEMA_VERSION = 1
MAX_CHECKPOINT_BYTES = 128 * 1024
_ACCOUNT_RE = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
_REDUCED_CODE_PATHS = (
    "mtf_lab/core/canonical.py",
    "mtf_lab/core/indicators.py",
    "mtf_lab/core/strategy.py",
    "mtf_lab/runtime/processor.py",
    "mtf_lab/ops/ctrader_watch.py",
    "mtf_lab/ops/ctrader_watch_cli.py",
    "mtf_lab/ops/supervision.py",
    "tools/soak_reliability.py",
)
_DEFAULT_FIXTURE_CONFIG = "config/ctrader_pipeline_fixture.toml"

# The default fence follows executable/package inputs rather than a hand
# curated list.  The top-level runtime/data/reports trees are operational or
# generated state; the similarly named ``mtf_lab/runtime`` and
# ``mtf_lab/data`` packages remain in scope because they are source code.
_FENCE_ROOTS = (
    "mtf_lab",
    "tools",
    "config",
    "bin",
    "deployment",
    "schemas",
    "manifests",
    "licencias",
    "metadata",
)
_ROOT_METADATA_NAMES = frozenset(
    {
        ".gitignore",
        ".gitattributes",
        "MANIFEST.in",
        "Makefile",
        "Pipfile",
        "Pipfile.lock",
        "Taskfile.yml",
        "justfile",
        "noxfile.py",
        "package.json",
        "package-lock.json",
        "pnpm-lock.yaml",
        "poetry.lock",
        "pytest.ini",
        "pyproject.toml",
        "pyrightconfig.json",
        "setup.cfg",
        "setup.py",
        "tox.ini",
        "uv.lock",
    }
)
_DERIVED_DIR_NAMES = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "build",
        "dist",
    }
)
_DERIVED_FILE_NAMES = frozenset({".coverage"})
_DERIVED_FILE_SUFFIXES = frozenset(
    {
        ".db",
        ".jsonl",
        ".log",
        ".ndjson",
        ".pyc",
        ".pyo",
        ".pyd",
        ".so",
        ".sqlite",
        ".sqlite3",
    }
)
_FENCE_METADATA_SUFFIXES = frozenset({".cfg", ".conf", ".ini", ".json", ".lock", ".toml", ".xml", ".yaml", ".yml"})


def _finite(value: Any, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} debe ser numérico")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser numérico") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        raise ValueError(f"{name} debe ser finito y {'positivo' if positive else 'no negativo'}")
    return result


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} debe ser entero positivo")
    return value


def _utc_now(value: datetime, name: str = "clock") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} debe devolver datetime con zona horaria")
    return value.astimezone(UTC)


def _json_object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} debe ser un objeto JSON")
    return {str(key): item for key, item in value.items()}


def _safe_component(value: str, name: str) -> str:
    text = str(value).strip()
    if not text or any(char not in _ACCOUNT_RE for char in text):
        raise ValueError(f"{name} sólo admite letras, números, '_' y '-'")
    return text


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _private_path(value: Path | str, *, repo_root: Path, name: str) -> Path:
    target = Path(value).expanduser()
    absolute = target.absolute()
    if target.is_symlink() or any(parent.is_symlink() for parent in absolute.parents):
        raise ValueError(f"{name} no puede atravesar symlinks")
    resolved = absolute.resolve(strict=False)
    if _is_under(resolved, repo_root) or resolved == repo_root:
        raise ValueError(f"{name} debe estar fuera del checkout")
    return resolved


def _ensure_private_dir(path: Path, name: str) -> None:
    if path.exists():
        info = path.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError(f"{name} debe ser una carpeta privada 0700 del usuario actual")
        return
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError(f"{name} no pudo quedar como carpeta privada 0700")


def _read_private_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.exists():
        raise ValueError("checkpoint inexistente o symlink")
    info = path.stat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_nlink != 1
        or stat.S_IMODE(info.st_mode) != 0o600
    ):
        raise ValueError("checkpoint debe ser archivo privado 0600")
    raw = path.read_bytes()
    if len(raw) > MAX_CHECKPOINT_BYTES:
        raise ValueError("checkpoint excede el límite de tamaño")
    try:
        return _json_object(json.loads(raw.decode("utf-8")), "checkpoint")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("checkpoint JSON ilegible") from exc


def _atomic_private_json(path: Path, value: Mapping[str, Any]) -> None:
    parent = path.parent
    _ensure_private_dir(parent, "directorio del checkpoint")
    if path.is_symlink():
        raise ValueError("checkpoint no puede ser symlink")
    payload = (canonical_json(dict(value)) + "\n").encode("utf-8")
    if len(payload) > MAX_CHECKPOINT_BYTES:
        raise ValueError("checkpoint excede el límite de tamaño")
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=parent)
    temporary: Path | None = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        assert temporary is not None
        os.replace(temporary, path)
        temporary = None
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary is not None:
            with suppress(FileNotFoundError):
                temporary.unlink()


def _file_hash(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"fuente de hash ausente o symlink: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_head(repo_root: Path) -> str | None:
    head_path = repo_root / ".git" / "HEAD"
    try:
        value = head_path.read_text(encoding="ascii").strip()
        if value.startswith("ref: "):
            ref = repo_root / ".git" / value[5:]
            return ref.read_text(encoding="ascii").strip()
        return value
    except (OSError, UnicodeDecodeError):
        return None


def _checkout_root(value: Path | str | None) -> Path:
    supplied = Path(value or Path(__file__).resolve().parents[1]).expanduser()
    absolute = supplied.absolute()
    if supplied.is_symlink() or any(parent.is_symlink() for parent in absolute.parents):
        raise ValueError("repo_root no puede ser un alias o atravesar symlinks")
    root = absolute.resolve(strict=False)
    if not root.is_dir():
        raise ValueError("repo_root debe ser un directorio")
    return root


def _relative_fence_path(value: str | Path) -> str:
    raw = os.fspath(value)
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("code_paths debe usar rutas UTF-8") from exc
    text = str(raw)
    path = Path(text)
    if (
        not text
        or path.is_absolute()
        or text.startswith("\\")
        or any(part in {".", ".."} for part in path.parts)
        or path.as_posix() != text
    ):
        raise ValueError("code_paths debe contener rutas relativas canónicas")
    return path.as_posix()


def _reject_symlink_components(path: Path, *, root: Path, name: str) -> None:
    current = path
    while True:
        if current.is_symlink():
            raise ValueError(f"{name} no puede ser un alias o atravesar symlinks")
        if current == root:
            return
        parent = current.parent
        if parent == current:
            raise ValueError(f"{name} está fuera del checkout")
        current = parent


def _derived_component(part: str) -> bool:
    return part in _DERIVED_DIR_NAMES or part.endswith(".egg-info")


def _derived_fence_path(relative: str) -> bool:
    path = Path(relative)
    if any(_derived_component(part) for part in path.parts):
        return True
    return path.name in _DERIVED_FILE_NAMES or path.suffix.lower() in _DERIVED_FILE_SUFFIXES


def _root_metadata_candidate(relative: str, *, executable: bool = False) -> bool:
    path = Path(relative)
    name = path.name
    lower = name.lower()
    if name in _ROOT_METADATA_NAMES or lower.startswith("requirements"):
        return True
    if path.suffix.lower() in _FENCE_METADATA_SUFFIXES:
        return True
    return executable


def _default_fence_path(relative: str) -> bool:
    path = Path(relative)
    if _derived_fence_path(relative):
        return False
    if path.parts and path.parts[0] in _FENCE_ROOTS:
        return True
    return len(path.parts) == 1 and _root_metadata_candidate(relative)


def _git_path_set(root: Path, args: Sequence[str]) -> set[str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), *args],
            check=True,
            capture_output=True,
            timeout=10.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ValueError("no se pudo leer el inventario Git del checkout") from exc
    paths: set[str] = set()
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        try:
            relative = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("el checkout contiene una ruta no UTF-8") from exc
        paths.add(relative)
    return paths


def _default_git_paths(root: Path) -> set[str]:
    # The index covers dirty/staged paths and untracked non-ignored additions;
    # HEAD supplies paths whose deletion was staged out of the index.
    return _git_path_set(root, ("ls-files", "-z", "--cached", "--others", "--exclude-standard")) | _git_path_set(
        root, ("ls-tree", "-r", "-z", "--name-only", "HEAD")
    )


def _scan_fence_child(
    root: Path,
    child: Path,
    *,
    pending: list[Path],
    found: set[str],
) -> None:
    relative = child.relative_to(root).as_posix()
    # Check aliases before any filtering or resolution.  A symlink in
    # a derived directory is still an unexpected checkout alias.
    if child.is_symlink():
        raise ValueError(f"fence contiene symlink: {relative}")
    if child.is_dir():
        if not _derived_fence_path(relative):
            pending.append(child)
        return
    if child.is_file():
        if not _derived_fence_path(relative):
            found.add(relative)
        return
    raise ValueError(f"fence contiene un tipo de archivo no regular: {relative}")


def _scan_fence_tree(root: Path, relative_root: str) -> set[str]:
    start = root / relative_root
    _reject_symlink_components(start, root=root, name=relative_root)
    if not start.exists():
        return set()
    if not start.is_dir():
        raise ValueError(f"fence root no es un directorio: {relative_root}")
    found: set[str] = set()
    pending = [start]
    while pending:
        directory = pending.pop()
        _reject_symlink_components(directory, root=root, name=relative_root)
        try:
            children = sorted(directory.iterdir(), key=lambda item: item.as_posix())
        except OSError as exc:
            raise ValueError(f"no se pudo leer el fence: {directory}") from exc
        for child in children:
            _scan_fence_child(root, child, pending=pending, found=found)
    return found


def _scan_root_metadata(root: Path) -> set[str]:
    found: set[str] = set()
    try:
        children = sorted(root.iterdir(), key=lambda item: item.as_posix())
    except OSError as exc:
        raise ValueError("no se pudo leer la metadata del checkout") from exc
    for child in children:
        relative = child.relative_to(root).as_posix()
        if child.is_symlink():
            if _root_metadata_candidate(relative):
                raise ValueError(f"metadata contiene symlink: {relative}")
            continue
        if not child.is_file():
            continue
        if _root_metadata_candidate(
            relative, executable=bool(child.stat().st_mode & 0o111)
        ) and not _derived_fence_path(relative):
            found.add(relative)
    return found


def _default_fence_paths(root: Path) -> tuple[str, ...]:
    paths = _default_git_paths(root)
    paths = {relative for relative in paths if _default_fence_path(relative)}
    for relative_root in _FENCE_ROOTS:
        paths.update(_scan_fence_tree(root, relative_root))
    paths.update(_scan_root_metadata(root))
    return tuple(sorted(paths, key=lambda item: item.encode("utf-8")))


def _fence_entry(root: Path, relative: str) -> dict[str, Any]:
    source = root / relative
    _reject_symlink_components(source, root=root, name=relative)
    if source.is_symlink():
        raise ValueError(f"fence contiene symlink: {relative}")
    if source.is_file():
        info = source.stat()
        return {
            "path": relative,
            "status": "PRESENT",
            "mode": stat.S_IMODE(info.st_mode),
            "sha256": _file_hash(source),
        }
    if source.exists():
        raise ValueError(f"fence contiene un tipo de archivo no regular: {relative}")
    return {"path": relative, "status": "DELETED", "mode": None, "sha256": None}


def compute_code_hash(repo_root: Path | str | None = None, paths: Sequence[str] | None = None) -> str:
    """Hash all executable checkout inputs, including dirty/add/delete state.

    With no explicit ``paths`` the manifest is recursive and includes source,
    launchers, configs, schemas, locks and packaging metadata while excluding
    state, runtime artifacts, logs, databases and generated caches.  Passing
    ``paths`` is intentionally a reduced test fence; callers must not treat it
    as complete acceptance evidence.
    """

    root = _checkout_root(repo_root)
    complete = paths is None
    if paths is None:
        selected = _default_fence_paths(root)
    else:
        raw_paths = tuple(_relative_fence_path(item) for item in paths)
        if len(set(raw_paths)) != len(raw_paths):
            raise ValueError("code_paths no puede contener rutas duplicadas")
        selected = tuple(raw_paths)
    entries = [_fence_entry(root, relative) for relative in selected]
    return fingerprint(
        {
            "schema_version": SOAK_SCHEMA_VERSION,
            "fence": "checkout-inputs-v2",
            "complete": complete,
            "git_head": _git_head(root),
            "sources": entries,
        }
    )


def compute_input_hash(
    *,
    repo_root: Path | str | None = None,
    config_path: Path | str | None = None,
    input_identity: str,
    max_events_per_cycle: int,
    account_key: str,
) -> str:
    """Hash fixture/config identity without hashing generated observations."""

    root = Path(repo_root or Path(__file__).resolve().parents[1]).resolve()
    selected = Path(config_path) if config_path is not None else root / _DEFAULT_FIXTURE_CONFIG
    selected = selected.expanduser().resolve(strict=False)
    if not _is_under(selected, root):
        raise ValueError("config_path debe pertenecer al checkout")
    return fingerprint(
        {
            "schema_version": SOAK_SCHEMA_VERSION,
            "fixture": "ctrader_supervisor_fixture_v1",
            "input_identity": str(input_identity),
            "config_path": selected.relative_to(root).as_posix(),
            "config_sha256": _file_hash(selected),
            "max_events_per_cycle": max_events_per_cycle,
            "account_key": account_key,
            "mode": "observe",
            "network": False,
            "execution": False,
        }
    )


@dataclass(frozen=True, slots=True)
class SoakConfig:
    """Immutable policy for one soak invocation."""

    duration_seconds: float = MIN_SOAK_SECONDS
    cycle_interval_seconds: float = 30.0
    max_events_per_cycle: int = 64
    checkpoint_every_cycles: int = 1
    state_dir: Path | None = None
    data_dir: Path | None = None
    checkpoint_path: Path | None = None
    output_path: Path | None = None
    config_path: Path | None = None
    account_key: str = "mtf-soak"
    input_identity: str = "ctrader-supervisor-fixture-v1"
    # ``None`` selects the complete checkout fence.  A supplied tuple is a
    # reduced test fence and is never eligible for 72-hour acceptance.
    code_paths: tuple[str, ...] | None = None
    smoke: bool = False
    dry_run: bool = False
    min_free_bytes: int = 64 * 1024 * 1024
    suspension_threshold_seconds: float = 60.0
    max_cycles: int | None = None
    watchdog_enabled: bool = False

    def __post_init__(self) -> None:
        duration = _finite(self.duration_seconds, "duration_seconds", positive=True)
        if not self.smoke and duration < MIN_SOAK_SECONDS:
            raise ValueError(f"duration_seconds debe ser al menos {MIN_SOAK_SECONDS} fuera de smoke")
        object.__setattr__(self, "duration_seconds", duration)
        object.__setattr__(
            self,
            "cycle_interval_seconds",
            _finite(self.cycle_interval_seconds, "cycle_interval_seconds", positive=True),
        )
        object.__setattr__(
            self,
            "suspension_threshold_seconds",
            _finite(self.suspension_threshold_seconds, "suspension_threshold_seconds", positive=True),
        )
        object.__setattr__(
            self, "max_events_per_cycle", _positive_int(self.max_events_per_cycle, "max_events_per_cycle")
        )
        object.__setattr__(
            self, "checkpoint_every_cycles", _positive_int(self.checkpoint_every_cycles, "checkpoint_every_cycles")
        )
        if isinstance(self.min_free_bytes, bool) or not isinstance(self.min_free_bytes, int) or self.min_free_bytes < 0:
            raise ValueError("min_free_bytes debe ser entero no negativo")
        if self.max_cycles is not None:
            object.__setattr__(self, "max_cycles", _positive_int(self.max_cycles, "max_cycles"))
        if (
            not isinstance(self.smoke, bool)
            or not isinstance(self.dry_run, bool)
            or not isinstance(self.watchdog_enabled, bool)
        ):
            raise ValueError("smoke, dry_run y watchdog_enabled deben ser booleanos")
        object.__setattr__(self, "account_key", _safe_component(self.account_key, "account_key"))
        identity = str(self.input_identity).strip()
        if not identity:
            raise ValueError("input_identity es obligatorio")
        object.__setattr__(self, "input_identity", identity)
        if self.code_paths is not None:
            if isinstance(self.code_paths, (str, bytes)):
                raise ValueError("code_paths debe ser una secuencia de rutas")
            paths = tuple(str(item) for item in self.code_paths)
            if not paths:
                raise ValueError("code_paths no puede estar vacío")
            object.__setattr__(self, "code_paths", paths)


@dataclass(frozen=True, slots=True)
class CycleResult:
    """Redacted facts returned by one replay/supervisor cycle."""

    ok: bool
    status: str
    messages: int = 0
    events: int = 0
    progress: bool = False
    replay_hash: str = ""
    supervisor_hash: str = ""
    errors: tuple[str, ...] = ()
    disk_ok: bool = True
    watchdog_ok: bool = True
    suspension_detected: bool = False
    gap_detected: bool = False
    work_units: int = 0
    model_restore: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.ok, bool):
            raise ValueError("CycleResult.ok debe ser booleano")
        for name in ("messages", "events"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} debe ser entero no negativo")
        if isinstance(self.work_units, bool) or not isinstance(self.work_units, int) or self.work_units < 0:
            raise ValueError("work_units debe ser entero no negativo")
        object.__setattr__(self, "errors", tuple(str(item) for item in self.errors))
        object.__setattr__(self, "model_restore", MappingProxyType(dict(self.model_restore)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "messages": self.messages,
            "events": self.events,
            "progress": self.progress,
            "replay_hash": self.replay_hash,
            "supervisor_hash": self.supervisor_hash,
            "errors": list(self.errors),
            "disk_ok": self.disk_ok,
            "watchdog_ok": self.watchdog_ok,
            "suspension_detected": self.suspension_detected,
            "gap_detected": self.gap_detected,
            "work_units": self.work_units,
            "model_restore": dict(self.model_restore),
        }


class CycleRunner(Protocol):
    def run_cycle(self, cycle_index: int, config: SoakConfig) -> CycleResult: ...


@dataclass(frozen=True, slots=True)
class SoakCheckpoint:
    """Bounded durable progress, fenced to input and source identities."""

    schema_version: int
    run_id: str
    code_hash: str
    input_hash: str
    requested_duration_seconds: float
    smoke: bool
    active_wallclock_seconds: float
    active_monotonic_seconds: float
    downtime_seconds: float
    cycles: int
    progress_counter: int
    last_wallclock_at: str
    last_status: str
    last_replay_hash: str
    last_supervisor_hash: str
    last_restore_hash: str
    recent_cycles: tuple[Mapping[str, Any], ...] = ()
    failed_reasons: tuple[str, ...] = ()
    restore_scope: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != SOAK_SCHEMA_VERSION:
            raise ValueError("schema_version de checkpoint no soportada")
        for name in ("run_id", "code_hash", "input_hash", "last_wallclock_at"):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"{name} del checkpoint es obligatorio")
        for name in (
            "requested_duration_seconds",
            "active_wallclock_seconds",
            "active_monotonic_seconds",
            "downtime_seconds",
        ):
            object.__setattr__(
                self,
                name,
                _finite(getattr(self, name), name, positive=name == "requested_duration_seconds"),
            )
            if getattr(self, name) < 0.0:
                raise ValueError(f"{name} debe ser no negativo")
        for name in ("cycles", "progress_counter"):
            object.__setattr__(self, name, _positive_int(getattr(self, name), name) if getattr(self, name) else 0)
        if not isinstance(self.smoke, bool):
            raise ValueError("smoke del checkpoint debe ser booleano")
        _utc_now(datetime.fromisoformat(self.last_wallclock_at.replace("Z", "+00:00")), "last_wallclock_at")
        object.__setattr__(self, "recent_cycles", tuple(dict(item) for item in self.recent_cycles))
        object.__setattr__(self, "failed_reasons", tuple(str(item) for item in self.failed_reasons))
        object.__setattr__(self, "restore_scope", MappingProxyType(dict(self.restore_scope)))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "code_hash": self.code_hash,
            "input_hash": self.input_hash,
            "requested_duration_seconds": self.requested_duration_seconds,
            "smoke": self.smoke,
            "active_wallclock_seconds": self.active_wallclock_seconds,
            "active_monotonic_seconds": self.active_monotonic_seconds,
            "downtime_seconds": self.downtime_seconds,
            "cycles": self.cycles,
            "progress_counter": self.progress_counter,
            "last_wallclock_at": self.last_wallclock_at,
            "last_status": self.last_status,
            "last_replay_hash": self.last_replay_hash,
            "last_supervisor_hash": self.last_supervisor_hash,
            "last_restore_hash": self.last_restore_hash,
            "recent_cycles": [dict(item) for item in self.recent_cycles],
            "failed_reasons": list(self.failed_reasons),
            "restore_scope": dict(self.restore_scope),
        }

    @property
    def restore_hash(self) -> str:
        return fingerprint(self.to_dict())

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SoakCheckpoint:
        if not isinstance(value, Mapping):
            raise ValueError("checkpoint debe ser mapping")
        required = {
            "schema_version",
            "run_id",
            "code_hash",
            "input_hash",
            "requested_duration_seconds",
            "smoke",
            "active_wallclock_seconds",
            "active_monotonic_seconds",
            "downtime_seconds",
            "cycles",
            "progress_counter",
            "last_wallclock_at",
            "last_status",
            "last_replay_hash",
            "last_supervisor_hash",
            "last_restore_hash",
            "recent_cycles",
            "failed_reasons",
        }
        missing = required - set(value)
        if missing:
            raise ValueError(f"checkpoint incompleto: {sorted(missing)}")
        recent = value["recent_cycles"]
        if not isinstance(recent, Sequence) or isinstance(recent, (str, bytes)):
            raise ValueError("recent_cycles debe ser secuencia")
        rows = tuple(_json_object(item, "recent_cycle") for item in recent)
        failures = value["failed_reasons"]
        if not isinstance(failures, Sequence) or isinstance(failures, (str, bytes)):
            raise ValueError("failed_reasons debe ser secuencia")
        return cls(
            schema_version=int(value["schema_version"]),
            run_id=str(value["run_id"]),
            code_hash=str(value["code_hash"]),
            input_hash=str(value["input_hash"]),
            requested_duration_seconds=float(value["requested_duration_seconds"]),
            smoke=value["smoke"],
            active_wallclock_seconds=float(value["active_wallclock_seconds"]),
            active_monotonic_seconds=float(value["active_monotonic_seconds"]),
            downtime_seconds=float(value["downtime_seconds"]),
            cycles=int(value["cycles"]),
            progress_counter=int(value["progress_counter"]),
            last_wallclock_at=str(value["last_wallclock_at"]),
            last_status=str(value["last_status"]),
            last_replay_hash=str(value["last_replay_hash"]),
            last_supervisor_hash=str(value["last_supervisor_hash"]),
            last_restore_hash=str(value["last_restore_hash"]),
            recent_cycles=rows,
            failed_reasons=tuple(str(item) for item in failures),
            restore_scope=_json_object(value.get("restore_scope", {}), "restore_scope"),
        )


@dataclass(frozen=True, slots=True)
class SoakReceipt:
    """Secret-free result receipt emitted by the harness."""

    schema_version: int
    run_id: str
    status: str
    acceptance: bool
    smoke: bool
    requested_duration_seconds: float
    wallclock_elapsed_seconds: float
    monotonic_elapsed_seconds: float
    downtime_seconds: float
    cycles: int
    progress_counter: int
    input_hash: str
    code_hash: str
    checkpoint_path: str | None
    state_dir: str | None
    data_dir: str | None
    clock_mode: str
    runner_mode: str
    observation_source: str
    os: Mapping[str, Any]
    watchdog: Mapping[str, Any]
    disk: Mapping[str, Any]
    last_replay_hash: str
    last_supervisor_hash: str
    last_restore_hash: str
    compare_hash: str
    code_fence_complete: bool = True
    restore_scope: Mapping[str, Any] = field(default_factory=dict)
    failed_reasons: tuple[str, ...] = ()
    progress: tuple[Mapping[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "status": self.status,
            "acceptance": self.acceptance,
            "smoke": self.smoke,
            "acceptance_basis": "real_wallclock_and_monotonic_duration" if self.acceptance else "not_accepted",
            "duration_policy": {
                "minimum_seconds": MIN_SOAK_SECONDS,
                "requested_seconds": self.requested_duration_seconds,
                "smoke_can_never_be_accepted": True,
            },
            "requested_duration_seconds": self.requested_duration_seconds,
            "wallclock_elapsed_seconds": self.wallclock_elapsed_seconds,
            "wallclock_elapsed_real_seconds": self.wallclock_elapsed_seconds,
            "monotonic_elapsed_seconds": self.monotonic_elapsed_seconds,
            "downtime_seconds": self.downtime_seconds,
            "cycles": self.cycles,
            "progress_counter": self.progress_counter,
            "input_hash": self.input_hash,
            "code_hash": self.code_hash,
            "checkpoint_path": self.checkpoint_path,
            "state_dir": self.state_dir,
            "data_dir": self.data_dir,
            "clock_mode": self.clock_mode,
            "runner_mode": self.runner_mode,
            "observation_source": self.observation_source,
            "fixture": True,
            "synthetic": True,
            "network_attempted": False,
            "execution_enabled": False,
            "os": dict(self.os),
            "watchdog": dict(self.watchdog),
            "disk": dict(self.disk),
            "last_replay_hash": self.last_replay_hash,
            "last_supervisor_hash": self.last_supervisor_hash,
            "last_restore_hash": self.last_restore_hash,
            "compare_hash": self.compare_hash,
            "code_fence_complete": self.code_fence_complete,
            "restore_scope": dict(self.restore_scope),
            "hashes": {
                "input": self.input_hash,
                "code": self.code_hash,
                "replay": self.last_replay_hash,
                "supervisor": self.last_supervisor_hash,
                "restore": self.last_restore_hash,
                "compare": self.compare_hash,
            },
            "failed_reasons": list(self.failed_reasons),
            "progress": [dict(item) for item in self.progress],
        }


def _default_state_dir() -> Path:
    root = os.environ.get("XDG_STATE_HOME")
    return (Path(root).expanduser() if root else Path.home() / ".local" / "state") / "mtf-lab" / "soak"


def _default_data_dir() -> Path:
    root = os.environ.get("XDG_DATA_HOME")
    return (Path(root).expanduser() if root else Path.home() / ".local" / "share") / "mtf-lab" / "soak"


def _resolve_paths(config: SoakConfig, repo_root: Path) -> tuple[Path, Path, Path, Path | None]:
    state_dir = _private_path(config.state_dir or _default_state_dir(), repo_root=repo_root, name="state_dir")
    data_dir = _private_path(config.data_dir or _default_data_dir(), repo_root=repo_root, name="data_dir")
    checkpoint = _private_path(
        config.checkpoint_path or state_dir / "checkpoint.json", repo_root=repo_root, name="checkpoint_path"
    )
    output = (
        _private_path(config.output_path, repo_root=repo_root, name="output_path")
        if config.output_path is not None
        else None
    )
    return state_dir, data_dir, checkpoint, output


def _disk_status(path: Path, minimum: int) -> dict[str, Any]:
    try:
        usage = shutil.disk_usage(path)
    except OSError as exc:
        return {"ok": False, "reason": f"DISK_USAGE_ERROR:{type(exc).__name__}"}
    return {"ok": usage.free >= minimum, "free_bytes": usage.free, "required_bytes": minimum}


def _progress_row(
    state: str,
    *,
    cycle: int,
    active_wall: float,
    active_mono: float,
    downtime: float,
    message: str = "",
) -> dict[str, Any]:
    return {
        "state": state,
        "cycle": cycle,
        "active_wallclock_seconds": round(active_wall, 6),
        "active_monotonic_seconds": round(active_mono, 6),
        "downtime_seconds": round(downtime, 6),
        "message": message,
    }


class _SupervisorFixtureRunner:
    """Reuse the supervisor fixture and verify the real model resume path."""

    def __init__(
        self, state_dir: Path, data_dir: Path, account_key: str, max_events: int, config_path: Path | None
    ) -> None:
        self.state_dir = state_dir
        self.data_dir = data_dir
        self.account_key = account_key
        self.max_events = max_events
        self.config_path = config_path

    def _supervisor_args(self, cycle_dir: Path) -> argparse.Namespace:
        return argparse.Namespace(
            mode="observe",
            fixture=True,
            network=False,
            activate=False,
            duration=None,
            max_events=self.max_events,
            idle_timeout=0.5,
            poll_timeout=0.01,
            checkpoint_every=16,
            max_candles=5000,
            max_reconnect_attempts=1,
            account_key=self.account_key,
            state_dir=cycle_dir / "supervisor-state",
            db=cycle_dir / "supervisor.sqlite3",
            config=self.config_path,
            report=None,
            continuous=False,
            resume=False,
        )

    @staticmethod
    def _supervisor_hash(payload: Mapping[str, Any]) -> str:
        stable = {
            key: payload.get(key)
            for key in (
                "ok",
                "state",
                "mode",
                "source",
                "messages",
                "events",
                "signals",
                "reconciliations",
                "reconnects",
                "clean_stop",
                "execution_enabled",
                "network_attempted",
                "fatal_latched",
                "reconciled_before_summary",
            )
        }
        return fingerprint(stable)

    def _run_supervisor(self, cycle_dir: Path) -> tuple[bool, Mapping[str, Any], int, int, str, tuple[str, ...]]:
        from mtf_lab.ops.supervision import CommandService

        result = CommandService().run(self._supervisor_args(cycle_dir))
        payload = dict(result.payload) if isinstance(result.payload, Mapping) else {}
        ok = int(result.code) == 0 and payload.get("ok") is True
        messages = payload.get("messages", 0)
        events = payload.get("events", 0)
        safe_messages = messages if isinstance(messages, int) and not isinstance(messages, bool) else 0
        safe_events = events if isinstance(events, int) and not isinstance(events, bool) else 0
        errors = () if ok else (f"SUPERVISOR:{payload.get('state', 'UNKNOWN')}",)
        return ok, payload, safe_messages, safe_events, self._supervisor_hash(payload), errors

    @staticmethod
    def _pipeline_spec(config: Any) -> Any:
        from mtf_lab.data.ctrader import CTraderInstrumentSpec

        return CTraderInstrumentSpec(
            symbol=config.instrument,
            symbol_id=int(config.ctrader.get("symbol_id", 99)),
            digits=int(config.ctrader.get("digits", 5)),
            pip_position=int(config.ctrader.get("pip_position", 4)),
            price_scale=int(config.ctrader.get("price_scale", 100_000)),
        )

    @staticmethod
    def _model_hash(result: Any) -> str:
        return fingerprint(
            {
                "snapshot_hash": result.snapshot_hash,
                "signals": [item.as_dict() for item in result.signals],
                "trades": [item.to_dict() for item in result.trades],
            }
        )

    def _model_restore(self, cycle_dir: Path) -> tuple[str, Mapping[str, Any]]:
        from mtf_lab.configuration import load_config
        from mtf_lab.ops.cfd_backtest import synthetic_cfd_capture
        from mtf_lab.ops.ctrader_pipeline import CTraderPipeline
        from mtf_lab.ops.persistence import SQLiteStore

        config = load_config(self.config_path)
        capture = synthetic_cfd_capture(config=config, count=190)
        if not capture.quote_events:
            raise RuntimeError("MODEL_FIXTURE_NO_QUOTES")
        spec = self._pipeline_spec(config)
        with SQLiteStore(cycle_dir / "model-full.sqlite3") as full_store:
            full = CTraderPipeline(full_store, config, spec=spec, mode="REPLAY", max_candles=5000).run(
                capture, session_id="model"
            )
        with SQLiteStore(cycle_dir / "model-resume.sqlite3") as resume_store:
            pipeline = CTraderPipeline(resume_store, config, spec=spec, mode="REPLAY", max_candles=5000)
            session = pipeline.open_session(
                dataset_id=capture.capture_hash,
                session_id="model",
                coverage=capture.coverage,
                provenance=capture.provenance,
                resume=False,
                order="as_observed",
            )
            split = len(capture.envelopes) // 2
            session.ingest_many(capture.envelopes[:split], chunk_size=16)
            session.checkpoint()
            restored_session = pipeline.open_session(
                dataset_id=capture.capture_hash,
                session_id="model",
                coverage=capture.coverage,
                provenance=capture.provenance,
                resume=True,
                order="as_observed",
            )
            corrupted = dict(restored_session.snapshot())
            corrupted["integrity_hash"] = "corrupted-model-checkpoint"
            try:
                restored_session.restore(corrupted)
            except Exception:
                corrupt_rejected = True
            else:
                corrupt_rejected = False
            restored_session.ingest_many(capture.envelopes[split:], chunk_size=16)
            restored = restored_session.finish(capture_complete=capture.is_complete, finish_session=True)
        full_hash = self._model_hash(full)
        restored_hash = self._model_hash(restored)
        compare_hash = fingerprint({"full": full_hash, "restored": restored_hash})
        details = {
            "scope": "restore_model",
            "status": "VERIFIED" if full_hash == restored_hash and corrupt_rejected else "FAILED",
            "capture_hash": capture.capture_hash,
            "quotes": len(capture.quote_events),
            "full_signals": len(full.signals),
            "restored_signals": len(restored.signals),
            "full_hash": full_hash,
            "restored_hash": restored_hash,
            "compare_hash": compare_hash,
            "corrupt_checkpoint_rejected": corrupt_rejected,
        }
        if not full.signals:
            raise RuntimeError("MODEL_FIXTURE_NO_SIGNALS")
        if full_hash != restored_hash:
            raise RuntimeError("MODEL_RESTORE_OUTPUT_MISMATCH")
        if not corrupt_rejected:
            raise RuntimeError("MODEL_CORRUPT_RESTORE_ACCEPTED")
        return full_hash, details

    def run_cycle(self, cycle_index: int, config: SoakConfig) -> CycleResult:
        del config
        cycle_root = Path(tempfile.mkdtemp(prefix=f"cycle-{cycle_index}-", dir=self.data_dir))
        try:
            os.chmod(cycle_root, 0o700)
            supervisor_ok, payload, messages, events, supervisor_hash, supervisor_errors = self._run_supervisor(
                cycle_root
            )
            model_hash, model_restore = self._model_restore(cycle_root)
            ok = supervisor_ok and bool(model_restore.get("status") == "VERIFIED")
            errors = supervisor_errors if not supervisor_ok else ()
            if not ok and not errors:
                errors = ("MODEL_RESTORE_FAILED",)
            progress = messages > 0 or events > 0 or bool(model_restore.get("quotes", 0))
            return CycleResult(
                ok=ok,
                status=str(payload.get("state", "UNKNOWN")),
                messages=messages,
                events=events,
                progress=progress,
                replay_hash=model_hash,
                supervisor_hash=supervisor_hash,
                errors=errors,
                work_units=messages + events + int(model_restore.get("quotes", 0)),
                model_restore=model_restore,
            )
        except Exception as exc:
            return CycleResult(
                ok=False,
                status="FAILED",
                errors=(f"MODEL_CYCLE_EXCEPTION:{type(exc).__name__}",),
            )
        finally:
            # Cycle directories are disposable QA state; retain only hashes in
            # the outer receipt/checkpoint.  TemporaryDirectory-like cleanup
            # is explicit and bounded to our own directory.
            import shutil as _shutil

            _shutil.rmtree(cycle_root, ignore_errors=False)


def _coerce_cycle_result(value: Any) -> CycleResult:
    if isinstance(value, CycleResult):
        return value
    if isinstance(value, Mapping):
        return CycleResult(
            ok=value.get("ok") is True,
            status=str(value.get("status", "UNKNOWN")),
            messages=int(value.get("messages", 0)),
            events=int(value.get("events", 0)),
            progress=value.get("progress") is True,
            replay_hash=str(value.get("replay_hash", "")),
            supervisor_hash=str(value.get("supervisor_hash", "")),
            errors=tuple(str(item) for item in value.get("errors", ())),
            disk_ok=value.get("disk_ok") is not False,
            watchdog_ok=value.get("watchdog_ok") is not False,
            suspension_detected=value.get("suspension_detected") is True,
            gap_detected=value.get("gap_detected") is True,
            work_units=int(value.get("work_units", 0)),
            model_restore=value.get("model_restore", {}) if isinstance(value.get("model_restore", {}), Mapping) else {},
        )
    raise TypeError("cycle_runner debe devolver CycleResult o mapping")


def _call_runner(runner: Any, cycle_index: int, config: SoakConfig) -> CycleResult:
    if hasattr(runner, "run_cycle") and callable(runner.run_cycle):
        return _coerce_cycle_result(runner.run_cycle(cycle_index, config))
    if callable(runner):
        return _coerce_cycle_result(runner(cycle_index, config))
    raise TypeError("cycle_runner no cumple el contrato")


def _call_watchdog(watchdog: Any, progress_counter: int, status: str) -> bool:
    notify = getattr(watchdog, "notify_progress", None)
    if not callable(notify):
        raise TypeError("watchdog requiere notify_progress")
    result = notify(progress_counter, status)
    return result is True


def _checkpoint_from_state(
    *,
    run_id: str,
    code_hash: str,
    input_hash: str,
    config: SoakConfig,
    active_wall: float,
    active_mono: float,
    downtime: float,
    cycles: int,
    progress_counter: int,
    now_wall: datetime,
    last_status: str,
    last_replay_hash: str,
    last_supervisor_hash: str,
    last_restore_hash: str,
    recent_cycles: Sequence[Mapping[str, Any]],
    failed_reasons: Sequence[str],
    restore_scope: Mapping[str, Any],
) -> SoakCheckpoint:
    return SoakCheckpoint(
        schema_version=SOAK_SCHEMA_VERSION,
        run_id=run_id,
        code_hash=code_hash,
        input_hash=input_hash,
        requested_duration_seconds=config.duration_seconds,
        smoke=config.smoke,
        active_wallclock_seconds=active_wall,
        active_monotonic_seconds=active_mono,
        downtime_seconds=downtime,
        cycles=cycles,
        progress_counter=progress_counter,
        last_wallclock_at=instant_text(now_wall),
        last_status=last_status,
        last_replay_hash=last_replay_hash,
        last_supervisor_hash=last_supervisor_hash,
        last_restore_hash=last_restore_hash,
        recent_cycles=tuple(recent_cycles[-32:]),
        failed_reasons=tuple(dict.fromkeys(str(item) for item in failed_reasons)),
        restore_scope=dict(restore_scope),
    )


def _receipt(
    *,
    run_id: str,
    status: str,
    acceptance: bool,
    config: SoakConfig,
    active_wall: float,
    active_mono: float,
    downtime: float,
    cycles: int,
    progress_counter: int,
    input_hash: str,
    code_hash: str,
    checkpoint: Path | None,
    state_dir: Path | None,
    data_dir: Path | None,
    clock_mode: str,
    runner_mode: str,
    failed_reasons: Sequence[str],
    progress: Sequence[Mapping[str, Any]],
    last_replay_hash: str = "",
    last_supervisor_hash: str = "",
    last_restore_hash: str = "",
    compare_hash: str = "",
    watchdog: Mapping[str, Any] | None = None,
    disk: Mapping[str, Any] | None = None,
    restore_scope: Mapping[str, Any] | None = None,
) -> SoakReceipt:
    resolved_compare_hash = compare_hash or fingerprint(
        {
            "input_hash": input_hash,
            "code_hash": code_hash,
            "replay_hash": last_replay_hash,
            "supervisor_hash": last_supervisor_hash,
            "restore_hash": last_restore_hash,
        }
    )
    return SoakReceipt(
        schema_version=SOAK_SCHEMA_VERSION,
        run_id=run_id,
        status=status,
        acceptance=acceptance,
        smoke=config.smoke,
        requested_duration_seconds=config.duration_seconds,
        wallclock_elapsed_seconds=max(0.0, active_wall),
        monotonic_elapsed_seconds=max(0.0, active_mono),
        downtime_seconds=max(0.0, downtime),
        cycles=cycles,
        progress_counter=progress_counter,
        input_hash=input_hash,
        code_hash=code_hash,
        checkpoint_path=str(checkpoint) if checkpoint is not None else None,
        state_dir=str(state_dir) if state_dir is not None else None,
        data_dir=str(data_dir) if data_dir is not None else None,
        clock_mode=clock_mode,
        runner_mode=runner_mode,
        observation_source="local_supervisor_fixture_synthetic;not_market_evidence",
        os={"system": platform.system(), "name": os.name, "python": platform.python_version()},
        watchdog=MappingProxyType(dict(watchdog or {})),
        disk=MappingProxyType(dict(disk or {})),
        last_replay_hash=last_replay_hash,
        last_supervisor_hash=last_supervisor_hash,
        last_restore_hash=last_restore_hash,
        compare_hash=resolved_compare_hash,
        code_fence_complete=config.code_paths is None,
        restore_scope=MappingProxyType(dict(restore_scope or {})),
        failed_reasons=tuple(dict.fromkeys(str(item) for item in failed_reasons)),
        progress=tuple(dict(item) for item in progress[-64:]),
    )


@dataclass(slots=True)
class _SoakState:
    run_id: str
    code_hash: str
    input_hash: str
    active_wall: float = 0.0
    active_mono: float = 0.0
    downtime: float = 0.0
    cycles: int = 0
    progress_counter: int = 0
    last_status: str = "CREATED"
    last_replay_hash: str = ""
    last_supervisor_hash: str = ""
    last_restore_hash: str = ""
    recent_cycles: list[Mapping[str, Any]] = field(default_factory=list)
    progress_rows: list[dict[str, Any]] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    watchdog_state: dict[str, Any] = field(default_factory=dict)
    disk_state: dict[str, Any] = field(default_factory=dict)
    restore_scope: dict[str, Any] = field(default_factory=dict)


def _append_failure(state: _SoakState, reason: str) -> None:
    if reason not in state.failures:
        state.failures.append(reason)


def _emit_progress(
    state: _SoakState,
    value: Mapping[str, Any],
    sink: Callable[[Mapping[str, Any]], Any] | None,
) -> None:
    row = dict(value)
    state.progress_rows.append(row)
    if sink is not None:
        sink(row)


def _elapsed_delta(
    before_wall: datetime,
    after_wall: datetime,
    before_mono: float,
    after_mono: float,
    threshold: float,
) -> tuple[float, float, float, str | None]:
    wall_delta = (after_wall - before_wall).total_seconds()
    mono_delta = after_mono - before_mono
    if wall_delta < 0.0 or mono_delta < 0.0:
        return 0.0, 0.0, 0.0, "CLOCK_BACKWARDS"
    gap = wall_delta - mono_delta
    if gap > threshold:
        return min(wall_delta, mono_delta), mono_delta, gap, "SUSPENSION_OR_CLOCK_GAP"
    if wall_delta > threshold and mono_delta > threshold:
        return 0.0, 0.0, wall_delta, "SUSPENSION_OR_CLOCK_GAP"
    if mono_delta - wall_delta > threshold:
        return 0.0, 0.0, 0.0, "CLOCK_DOMAIN_MISMATCH"
    return wall_delta, mono_delta, 0.0, None


def _state_from_checkpoint(
    checkpoint: SoakCheckpoint,
    *,
    now_wall: Callable[[], datetime],
) -> _SoakState:
    resumed_at = _utc_now(now_wall())
    saved_at = _utc_now(
        datetime.fromisoformat(checkpoint.last_wallclock_at.replace("Z", "+00:00")),
        "checkpoint.last_wallclock_at",
    )
    resume_gap = (resumed_at - saved_at).total_seconds()
    if resume_gap < 0.0:
        raise ValueError("CLOCK_BACKWARDS_ON_RESUME")
    return _SoakState(
        run_id=checkpoint.run_id,
        code_hash=checkpoint.code_hash,
        input_hash=checkpoint.input_hash,
        active_wall=checkpoint.active_wallclock_seconds,
        active_mono=checkpoint.active_monotonic_seconds,
        downtime=checkpoint.downtime_seconds + resume_gap,
        cycles=checkpoint.cycles,
        progress_counter=checkpoint.progress_counter,
        last_status="RESUMED",
        last_replay_hash=checkpoint.last_replay_hash,
        last_supervisor_hash=checkpoint.last_supervisor_hash,
        last_restore_hash=checkpoint.restore_hash,
        recent_cycles=[dict(item) for item in checkpoint.recent_cycles],
        watchdog_state={"requested": False, "status": "PENDING", "pings": 0, "ok": True},
        disk_state={"ok": True},
        restore_scope=dict(checkpoint.restore_scope),
    )


def _new_soak_state(
    config: SoakConfig,
    *,
    code_hash: str,
    input_hash: str,
    clock: Callable[[], datetime],
) -> _SoakState:
    started = _utc_now(clock())
    run_id = "soak_" + fingerprint({"input_identity": config.input_identity, "started": instant_text(started)})[:32]
    return _SoakState(
        run_id=run_id,
        code_hash=code_hash,
        input_hash=input_hash,
        watchdog_state={
            "requested": config.watchdog_enabled,
            "status": "DISABLED" if not config.watchdog_enabled else "PENDING",
            "pings": 0,
            "ok": not config.watchdog_enabled,
        },
        disk_state={"ok": True},
    )


def _identity_for_run(config: SoakConfig, repo_root: Path) -> tuple[str, str, Path]:
    config_path = (config.config_path or repo_root / _DEFAULT_FIXTURE_CONFIG).resolve(strict=False)
    code_hash = compute_code_hash(repo_root, config.code_paths)
    input_hash = compute_input_hash(
        repo_root=repo_root,
        config_path=config_path,
        input_identity=config.input_identity,
        max_events_per_cycle=config.max_events_per_cycle,
        account_key=config.account_key,
    )
    return code_hash, input_hash, config_path


def _checkpoint_roundtrip(
    path: Path,
    checkpoint: SoakCheckpoint,
) -> str:
    _atomic_private_json(path, checkpoint.to_dict())
    restored = SoakCheckpoint.from_mapping(_read_private_json(path))
    if restored.restore_hash != checkpoint.restore_hash:
        raise ValueError("checkpoint roundtrip hash mismatch")
    return restored.restore_hash


def _save_soak_checkpoint(
    state: _SoakState,
    config: SoakConfig,
    checkpoint_path: Path,
    now_wall: datetime,
) -> None:
    checkpoint = _checkpoint_from_state(
        run_id=state.run_id,
        code_hash=state.code_hash,
        input_hash=state.input_hash,
        config=config,
        active_wall=state.active_wall,
        active_mono=state.active_mono,
        downtime=state.downtime,
        cycles=state.cycles,
        progress_counter=state.progress_counter,
        now_wall=now_wall,
        last_status=state.last_status,
        last_replay_hash=state.last_replay_hash,
        last_supervisor_hash=state.last_supervisor_hash,
        last_restore_hash=state.last_restore_hash,
        recent_cycles=state.recent_cycles,
        failed_reasons=state.failures,
        restore_scope=state.restore_scope,
    )
    state.last_restore_hash = _checkpoint_roundtrip(checkpoint_path, checkpoint)
    state.restore_scope = {
        **state.restore_scope,
        "bookkeeping": {
            "scope": "bookkeeping",
            "status": "VERIFIED",
            "hash": state.last_restore_hash,
        },
    }


def _record_cycle(
    state: _SoakState,
    cycle: CycleResult,
    *,
    wall_increment: float,
    mono_increment: float,
    gap: float,
) -> None:
    state.active_wall += wall_increment
    state.active_mono += mono_increment
    if gap > 0.0:
        state.downtime += gap
    state.cycles += 1
    state.last_status = cycle.status
    state.last_replay_hash = cycle.replay_hash
    state.last_supervisor_hash = cycle.supervisor_hash
    # The watchdog cursor must advance only when the cycle reports observable
    # new work.  A hash, timestamp, or merely completed cycle is not progress.
    work_units = cycle.work_units or (cycle.messages + cycle.events)
    if cycle.progress and work_units > 0:
        state.progress_counter += work_units
    if not cycle.ok:
        if cycle.errors:
            for reason in cycle.errors:
                _append_failure(state, reason)
        else:
            _append_failure(state, "CYCLE_FAILED")
    if not cycle.disk_ok:
        _append_failure(state, "DISK_SPACE_FAILED")
    if not cycle.watchdog_ok:
        _append_failure(state, "WATCHDOG_FAILED")
    if cycle.suspension_detected or cycle.gap_detected:
        _append_failure(state, "CYCLE_REPORTED_SUSPENSION_OR_GAP")
    if cycle.model_restore:
        state.restore_scope["restore_model"] = dict(cycle.model_restore)
    state.recent_cycles.append({"cycle": state.cycles, **cycle.to_dict()})
    del state.recent_cycles[:-32]


def _watchdog_cycle(
    state: _SoakState,
    config: SoakConfig,
    watchdog: Any | None,
) -> None:
    if not config.watchdog_enabled or watchdog is None:
        return
    try:
        if _call_watchdog(watchdog, state.progress_counter, state.last_status):
            state.watchdog_state["pings"] = int(state.watchdog_state.get("pings", 0)) + 1
        state.watchdog_state["status"] = "ACTIVE"
        state.watchdog_state["ok"] = True
    except Exception as exc:
        state.watchdog_state["status"] = "FAILED"
        state.watchdog_state["error"] = type(exc).__name__
        state.watchdog_state["ok"] = False
        _append_failure(state, "WATCHDOG_FAILED")


def _sleep_and_account(
    state: _SoakState,
    config: SoakConfig,
    *,
    sleep_fn: Callable[[float], None],
    clock: Callable[[], datetime],
    monotonic: Callable[[], float],
) -> bool:
    before_wall = _utc_now(clock())
    before_mono = _finite(monotonic(), "monotonic")
    sleep_fn(config.cycle_interval_seconds)
    after_wall = _utc_now(clock())
    after_mono = _finite(monotonic(), "monotonic")
    wall, mono, gap, reason = _elapsed_delta(
        before_wall, after_wall, before_mono, after_mono, config.suspension_threshold_seconds
    )
    state.active_wall += wall
    state.active_mono += mono
    state.downtime += gap
    if reason is not None:
        _append_failure(state, reason)
    return wall != 0.0 or mono != 0.0


def _run_one_cycle(
    state: _SoakState,
    config: SoakConfig,
    *,
    runner: Any,
    clock: Callable[[], datetime],
    monotonic: Callable[[], float],
    previous_wall: datetime,
    previous_mono: float,
) -> tuple[CycleResult, float, float, float, datetime, float] | None:
    before_wall = _utc_now(clock())
    before_mono = _finite(monotonic(), "monotonic")
    if before_wall < previous_wall or before_mono < previous_mono:
        _append_failure(state, "CLOCK_BACKWARDS")
        return None
    try:
        cycle = _call_runner(runner, state.cycles + 1, config)
    except Exception as exc:
        _append_failure(state, f"CYCLE_EXCEPTION:{type(exc).__name__}")
        return None
    after_wall = _utc_now(clock())
    after_mono = _finite(monotonic(), "monotonic")
    wall, mono, gap, reason = _elapsed_delta(
        before_wall, after_wall, before_mono, after_mono, config.suspension_threshold_seconds
    )
    if reason is not None:
        _append_failure(state, reason)
    return cycle, wall, mono, gap, after_wall, after_mono


def _refresh_disk_state(
    state: _SoakState,
    config: SoakConfig,
    *,
    disk_probe: Callable[[Path, int], Mapping[str, Any] | bool] | None,
    state_dir: Path,
) -> None:
    value = (
        disk_probe(state_dir, config.min_free_bytes) if disk_probe else _disk_status(state_dir, config.min_free_bytes)
    )
    state.disk_state = dict(value) if isinstance(value, Mapping) else {"ok": value is True}
    if state.disk_state.get("ok") is not True:
        _append_failure(state, "DISK_SPACE_FAILED")


def _checkpoint_cycle(
    state: _SoakState,
    config: SoakConfig,
    *,
    checkpoint_path: Path,
    after_wall: datetime,
) -> bool:
    if state.cycles % config.checkpoint_every_cycles != 0 and not state.failures:
        return True
    if state.disk_state.get("ok") is not True:
        return False
    try:
        _save_soak_checkpoint(state, config, checkpoint_path, after_wall)
    except Exception as exc:
        _append_failure(state, f"CHECKPOINT_WRITE_FAILED:{type(exc).__name__}")
        return False
    return True


def _cycle_preflight(
    state: _SoakState,
    config: SoakConfig,
    *,
    stop_after_cycles: int | None,
    repo_root: Path,
) -> bool:
    if stop_after_cycles is not None and state.cycles >= stop_after_cycles:
        state.last_status = "PAUSED"
        return False
    current_code, current_input, _ = _identity_for_run(config, repo_root)
    if current_code != state.code_hash or current_input != state.input_hash:
        _append_failure(state, "HASH_FENCE_CHANGED_DURING_RUN")
        return False
    return True


def _cycle_postflight(state: _SoakState, config: SoakConfig) -> bool:
    if state.failures:
        return False
    if config.max_cycles is not None and state.cycles >= config.max_cycles:
        _append_failure(state, "CYCLE_LIMIT_BEFORE_DURATION")
        return False
    return True


def _pace_cycle(
    state: _SoakState,
    config: SoakConfig,
    *,
    sleep_fn: Callable[[float], None],
    clock: Callable[[], datetime],
    monotonic: Callable[[], float],
    no_advance: int,
) -> tuple[bool, int]:
    if not (state.active_mono < config.duration_seconds or state.active_wall < config.duration_seconds):
        return True, no_advance
    advanced = _sleep_and_account(state, config, sleep_fn=sleep_fn, clock=clock, monotonic=monotonic)
    if state.failures:
        return False, no_advance
    next_no_advance = 0 if advanced else no_advance + 1
    if next_no_advance >= 3:
        _append_failure(state, "CLOCK_NOT_ADVANCING")
        return False, next_no_advance
    return True, next_no_advance


def _run_cycles(
    state: _SoakState,
    config: SoakConfig,
    *,
    runner: Any,
    checkpoint_path: Path,
    disk_probe: Callable[[Path, int], Mapping[str, Any] | bool] | None,
    state_dir: Path,
    clock: Callable[[], datetime],
    monotonic: Callable[[], float],
    sleep_fn: Callable[[float], None],
    progress_sink: Callable[[Mapping[str, Any]], Any] | None,
    watchdog: Any | None,
    stop_after_cycles: int | None,
    repo_root: Path,
) -> None:
    last_wall = _utc_now(clock())
    last_mono = _finite(monotonic(), "monotonic")
    no_advance = 0
    while state.active_mono < config.duration_seconds or state.active_wall < config.duration_seconds:
        if not _cycle_preflight(state, config, stop_after_cycles=stop_after_cycles, repo_root=repo_root):
            return
        result = _run_one_cycle(
            state,
            config,
            runner=runner,
            clock=clock,
            monotonic=monotonic,
            previous_wall=last_wall,
            previous_mono=last_mono,
        )
        if result is None:
            return
        cycle, wall, mono, gap, after_wall, after_mono = result
        _record_cycle(state, cycle, wall_increment=wall, mono_increment=mono, gap=gap)
        last_wall, last_mono = after_wall, after_mono
        _watchdog_cycle(state, config, watchdog)
        _emit_progress(
            state,
            _progress_row(
                "PROGRESS",
                cycle=state.cycles,
                active_wall=state.active_wall,
                active_mono=state.active_mono,
                downtime=state.downtime,
                message=state.last_status,
            ),
            progress_sink,
        )
        _refresh_disk_state(state, config, disk_probe=disk_probe, state_dir=state_dir)
        if not _checkpoint_cycle(state, config, checkpoint_path=checkpoint_path, after_wall=after_wall):
            return
        if not _cycle_postflight(state, config):
            return
        keep_going, no_advance = _pace_cycle(
            state,
            config,
            sleep_fn=sleep_fn,
            clock=clock,
            monotonic=monotonic,
            no_advance=no_advance,
        )
        if not keep_going:
            return


def _acceptance_allowed(
    state: _SoakState,
    config: SoakConfig,
    *,
    status: str,
    control_injected: bool,
) -> bool:
    return (
        status == "PASS"
        and not config.smoke
        and not control_injected
        and config.code_paths is None
        and state.active_mono >= config.duration_seconds
        and state.active_wall >= config.duration_seconds
        and not state.failures
        and state.watchdog_state.get("ok") is True
        and state.disk_state.get("ok") is True
        and platform.system() == "Linux"
    )


def _finish_soak(
    state: _SoakState,
    config: SoakConfig,
    *,
    checkpoint_path: Path,
    state_dir: Path,
    data_dir: Path,
    output_path: Path | None,
    clock: Callable[[], datetime],
    progress_sink: Callable[[Mapping[str, Any]], Any] | None,
    control_injected: bool,
    clock_injected: bool,
    runner_injected: bool,
) -> SoakReceipt:
    complete = state.active_mono >= config.duration_seconds and state.active_wall >= config.duration_seconds
    if state.failures:
        status = "FAILED"
    elif state.last_status == "PAUSED":
        status = "PAUSED"
    elif complete:
        status = "PASS"
    else:
        status = "INCOMPLETE"
        _append_failure(state, "DURATION_NOT_REACHED")
    try:
        _save_soak_checkpoint(state, config, checkpoint_path, _utc_now(clock()))
    except Exception as exc:
        _append_failure(state, f"CHECKPOINT_WRITE_FAILED:{type(exc).__name__}")
        status = "FAILED"
    acceptance = _acceptance_allowed(state, config, status=status, control_injected=control_injected)
    _emit_progress(
        state,
        _progress_row(
            status,
            cycle=state.cycles,
            active_wall=state.active_wall,
            active_mono=state.active_mono,
            downtime=state.downtime,
        ),
        progress_sink,
    )
    receipt = _receipt(
        run_id=state.run_id,
        status=status,
        acceptance=acceptance,
        config=config,
        active_wall=state.active_wall,
        active_mono=state.active_mono,
        downtime=state.downtime,
        cycles=state.cycles,
        progress_counter=state.progress_counter,
        input_hash=state.input_hash,
        code_hash=state.code_hash,
        checkpoint=checkpoint_path,
        state_dir=state_dir,
        data_dir=data_dir,
        clock_mode="injected" if clock_injected else "real",
        runner_mode="injected_test_double" if runner_injected else "local_supervisor_fixture",
        failed_reasons=state.failures,
        progress=state.progress_rows,
        last_replay_hash=state.last_replay_hash,
        last_supervisor_hash=state.last_supervisor_hash,
        last_restore_hash=state.last_restore_hash,
        watchdog=state.watchdog_state,
        disk=state.disk_state,
        restore_scope=state.restore_scope,
    )
    if output_path is not None:
        try:
            _atomic_private_json(output_path, receipt.to_dict())
        except Exception as exc:
            _append_failure(state, f"OUTPUT_WRITE_FAILED:{type(exc).__name__}")
            receipt = replace(
                receipt,
                status="FAILED",
                acceptance=False,
                failed_reasons=tuple(state.failures),
            )
    return receipt


def _failed_receipt(
    state: _SoakState,
    config: SoakConfig,
    *,
    reason: str,
    checkpoint_path: Path | None,
    state_dir: Path | None,
    data_dir: Path | None,
    control_injected: bool,
    clock_injected: bool = False,
    runner_injected: bool = False,
) -> SoakReceipt:
    _append_failure(state, reason)
    state.progress_rows.append(
        _progress_row(
            "FAILED",
            cycle=state.cycles,
            active_wall=state.active_wall,
            active_mono=state.active_mono,
            downtime=state.downtime,
            message=reason,
        )
    )
    return _receipt(
        run_id=state.run_id,
        status="FAILED",
        acceptance=False,
        config=config,
        active_wall=state.active_wall,
        active_mono=state.active_mono,
        downtime=state.downtime,
        cycles=state.cycles,
        progress_counter=state.progress_counter,
        input_hash=state.input_hash,
        code_hash=state.code_hash,
        checkpoint=checkpoint_path,
        state_dir=state_dir,
        data_dir=data_dir,
        clock_mode="injected" if clock_injected else "real",
        runner_mode="injected_test_double" if runner_injected else "local_supervisor_fixture",
        failed_reasons=state.failures,
        progress=state.progress_rows,
        last_replay_hash=state.last_replay_hash,
        last_supervisor_hash=state.last_supervisor_hash,
        last_restore_hash=state.last_restore_hash,
        watchdog=state.watchdog_state,
        disk=state.disk_state,
        restore_scope=state.restore_scope,
    )


def _exception_reason(exc: Exception) -> str:
    message = str(exc).strip()
    return (
        message
        if message and message.replace("_", "").isalnum() and message.isupper()
        else f"{type(exc).__name__}:{message}"
    )


def _load_checkpoint_for_run(
    path: Path,
    *,
    resume: bool,
    code_hash: str,
    input_hash: str,
    config: SoakConfig,
) -> SoakCheckpoint | None:
    if not path.exists():
        return None
    if not resume:
        raise ValueError("CHECKPOINT_EXISTS_USE_RESUME")
    checkpoint = SoakCheckpoint.from_mapping(_read_private_json(path))
    if (
        checkpoint.code_hash != code_hash
        or checkpoint.input_hash != input_hash
        or checkpoint.requested_duration_seconds != config.duration_seconds
        or checkpoint.smoke != config.smoke
    ):
        raise ValueError("HASH_FENCE_MISMATCH")
    if checkpoint.failed_reasons:
        raise ValueError("CHECKPOINT_HAS_FAILURES")
    return checkpoint


def _initial_disk_state(
    path: Path,
    config: SoakConfig,
    disk_probe: Callable[[Path, int], Mapping[str, Any] | bool] | None,
) -> dict[str, Any]:
    value = disk_probe(path, config.min_free_bytes) if disk_probe else _disk_status(path, config.min_free_bytes)
    return dict(value) if isinstance(value, Mapping) else {"ok": value is True}


def _dry_receipt(
    state: _SoakState,
    config: SoakConfig,
    *,
    checkpoint_path: Path,
    state_dir: Path,
    data_dir: Path,
    injected: bool,
) -> SoakReceipt:
    return _receipt(
        run_id=state.run_id,
        status="DRY_RUN",
        acceptance=False,
        config=config,
        active_wall=state.active_wall,
        active_mono=state.active_mono,
        downtime=state.downtime,
        cycles=state.cycles,
        progress_counter=state.progress_counter,
        input_hash=state.input_hash,
        code_hash=state.code_hash,
        checkpoint=checkpoint_path,
        state_dir=state_dir,
        data_dir=data_dir,
        clock_mode="injected" if injected else "real",
        runner_mode="injected_test_double" if injected else "local_supervisor_fixture",
        failed_reasons=(),
        progress=state.progress_rows,
        watchdog=state.watchdog_state,
        disk=state.disk_state,
        restore_scope=state.restore_scope,
    )


def run_soak(
    config: SoakConfig | None = None,
    *,
    cycle_runner: CycleRunner | Callable[[int, SoakConfig], CycleResult] | None = None,
    clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
    sleep: Callable[[float], None] | None = None,
    progress_sink: Callable[[Mapping[str, Any]], Any] | None = None,
    disk_probe: Callable[[Path, int], Mapping[str, Any] | bool] | None = None,
    watchdog: Any | None = None,
    resume: bool = True,
    stop_after_cycles: int | None = None,
) -> SoakReceipt:
    """Run or resume a local soak; injected controls are never accepted."""

    selected = config or SoakConfig()
    repo_root = Path(__file__).resolve().parents[1]
    control_injected = any(item is not None for item in (cycle_runner, clock, monotonic, sleep, disk_probe, watchdog))
    clock_injected = clock is not None or monotonic is not None
    runner_injected = cycle_runner is not None
    clock_fn = clock or (lambda: datetime.now(UTC))
    monotonic_fn = monotonic or time.monotonic
    sleep_fn = sleep or time.sleep
    code_hash = ""
    input_hash = ""
    state_dir: Path | None = None
    data_dir: Path | None = None
    checkpoint_path: Path | None = None
    output_path: Path | None = None
    state = _new_soak_state(selected, code_hash="", input_hash="", clock=clock_fn)
    try:
        state_dir, data_dir, checkpoint_path, output_path = _resolve_paths(selected, repo_root)
        code_hash, input_hash, config_path = _identity_for_run(selected, repo_root)
        state.code_hash = code_hash
        state.input_hash = input_hash
        if selected.dry_run:
            _emit_progress(
                state,
                _progress_row("STARTED", cycle=0, active_wall=0.0, active_mono=0.0, downtime=0.0),
                progress_sink,
            )
            state.disk_state = {"ok": True, "checked": False, "dry_run": True}
            return _dry_receipt(
                state,
                selected,
                checkpoint_path=checkpoint_path,
                state_dir=state_dir,
                data_dir=data_dir,
                injected=control_injected,
            )
        _ensure_private_dir(state_dir, "state_dir")
        _ensure_private_dir(data_dir, "data_dir")
        state.disk_state = _initial_disk_state(state_dir, selected, disk_probe)
        if state.disk_state.get("ok") is not True:
            return _failed_receipt(
                state,
                selected,
                reason="DISK_SPACE_FAILED",
                checkpoint_path=checkpoint_path,
                state_dir=state_dir,
                data_dir=data_dir,
                control_injected=control_injected,
                clock_injected=clock_injected,
                runner_injected=runner_injected,
            )
        _emit_progress(
            state, _progress_row("STARTED", cycle=0, active_wall=0.0, active_mono=0.0, downtime=0.0), progress_sink
        )
        if platform.system() != "Linux":
            return _failed_receipt(
                state,
                selected,
                reason="OS_UNSUPPORTED",
                checkpoint_path=checkpoint_path,
                state_dir=state_dir,
                data_dir=data_dir,
                control_injected=control_injected,
                clock_injected=clock_injected,
                runner_injected=runner_injected,
            )
        checkpoint = _load_checkpoint_for_run(
            checkpoint_path,
            resume=resume,
            code_hash=code_hash,
            input_hash=input_hash,
            config=selected,
        )
        if checkpoint is not None:
            state = _state_from_checkpoint(checkpoint, now_wall=clock_fn)
            state.code_hash = code_hash
            state.input_hash = input_hash
            state.watchdog_state = {
                "requested": selected.watchdog_enabled,
                "status": "DISABLED" if not selected.watchdog_enabled else "PENDING",
                "pings": 0,
                "ok": not selected.watchdog_enabled,
            }
            state.disk_state = {"ok": True}
        if selected.watchdog_enabled and watchdog is None:
            return _failed_receipt(
                state,
                selected,
                reason="WATCHDOG_NOT_CONFIGURED",
                checkpoint_path=checkpoint_path,
                state_dir=state_dir,
                data_dir=data_dir,
                control_injected=control_injected,
                clock_injected=clock_injected,
                runner_injected=runner_injected,
            )
        runner: Any = cycle_runner or _SupervisorFixtureRunner(
            state_dir, data_dir, selected.account_key, selected.max_events_per_cycle, config_path
        )
        _run_cycles(
            state,
            selected,
            runner=runner,
            checkpoint_path=checkpoint_path,
            disk_probe=disk_probe,
            state_dir=state_dir,
            clock=clock_fn,
            monotonic=monotonic_fn,
            sleep_fn=sleep_fn,
            progress_sink=progress_sink,
            watchdog=watchdog,
            stop_after_cycles=stop_after_cycles,
            repo_root=repo_root,
        )
        return _finish_soak(
            state,
            selected,
            checkpoint_path=checkpoint_path,
            state_dir=state_dir,
            data_dir=data_dir,
            output_path=output_path,
            clock=clock_fn,
            progress_sink=progress_sink,
            control_injected=control_injected,
            clock_injected=clock_injected,
            runner_injected=runner_injected,
        )
    except Exception as exc:
        receipt = _failed_receipt(
            state,
            selected,
            reason=_exception_reason(exc),
            checkpoint_path=checkpoint_path,
            state_dir=state_dir,
            data_dir=data_dir,
            control_injected=control_injected,
            clock_injected=clock_injected,
            runner_injected=runner_injected,
        )
        if output_path is not None:
            with suppress(Exception):
                _atomic_private_json(output_path, receipt.to_dict())
        return receipt


def _cli_config(args: argparse.Namespace, repo_root: Path) -> SoakConfig:
    smoke = bool(args.smoke)
    duration = args.duration_seconds
    if duration is None:
        duration = 2.0 if smoke else MIN_SOAK_SECONDS
    return SoakConfig(
        duration_seconds=duration,
        cycle_interval_seconds=args.cycle_interval_seconds,
        max_events_per_cycle=args.max_events_per_cycle,
        checkpoint_every_cycles=args.checkpoint_every_cycles,
        state_dir=Path(args.state_dir).expanduser() if args.state_dir else None,
        data_dir=Path(args.data_dir).expanduser() if args.data_dir else None,
        checkpoint_path=Path(args.checkpoint).expanduser() if args.checkpoint else None,
        output_path=Path(args.output).expanduser() if args.output else None,
        config_path=Path(args.config).expanduser() if args.config else repo_root / _DEFAULT_FIXTURE_CONFIG,
        account_key=args.account_key,
        smoke=smoke,
        dry_run=bool(args.dry_run),
        max_cycles=args.max_cycles,
        watchdog_enabled=bool(args.watchdog),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Harness local de resistencia MTF; fixture offline, checkpoint privado y hash fences.",
        epilog=(
            "Pasos: 1) --dry-run (predeterminado); 2) --smoke para una prueba corta local "
            "que nunca es aceptación; 3) --execute para la corrida >=72h. "
            "No instala units systemd, no abre broker/OAuth y no crea automations."
        ),
    )
    parser.add_argument("--execute", action="store_true", help="autoriza la corrida larga local; no habilita red")
    parser.add_argument(
        "--dry-run", action="store_true", help="muestra/valida el plan sin ejecutar ciclos (predeterminado)"
    )
    parser.add_argument("--smoke", action="store_true", help="fixture corto; acceptance=false siempre")
    parser.add_argument("--duration-seconds", type=float, default=None, help=f"duración; normal >= {MIN_SOAK_SECONDS}s")
    parser.add_argument("--cycle-interval-seconds", type=float, default=30.0)
    parser.add_argument("--max-events-per-cycle", type=int, default=64)
    parser.add_argument("--checkpoint-every-cycles", type=int, default=1)
    parser.add_argument(
        "--max-cycles", type=int, default=None, help="límite explícito; si corta antes de duración falla"
    )
    parser.add_argument("--state-dir", default=None, help="carpeta privada fuera del checkout")
    parser.add_argument("--data-dir", default=None, help="carpeta privada de datos/SQLite fuera del checkout")
    parser.add_argument("--checkpoint", default=None, help="checkpoint privado explícito")
    parser.add_argument("--output", default=None, help="receipt JSON privado opcional")
    parser.add_argument("--config", default=None, help="TOML local de configuración; sólo se hashea, no se modifica")
    parser.add_argument("--account-key", default="mtf-soak")
    parser.add_argument(
        "--watchdog", action="store_true", help="requiere un watchdog inyectado; CLI no instala NOTIFY_SOCKET"
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    if not args.dry_run and not args.smoke and not args.execute:
        args.dry_run = True
    try:
        config = _cli_config(args, repo_root)
        receipt = run_soak(config)
        print(canonical_json(receipt.to_dict()))
        return 0 if receipt.status in {"DRY_RUN", "PASS", "PAUSED"} else 2
    except (TypeError, ValueError, OSError) as exc:
        print(canonical_json({"status": "FAILED", "acceptance": False, "error": type(exc).__name__}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CycleRunner",
    "CycleResult",
    "MIN_SOAK_SECONDS",
    "SOAK_SCHEMA_VERSION",
    "SoakCheckpoint",
    "SoakConfig",
    "SoakReceipt",
    "compute_code_hash",
    "compute_input_hash",
    "main",
    "run_soak",
]
