"""Aggregate storage budget and acquisition locking for market-data raws.

The market-data providers keep their manifests, receipts, and caches outside
this budget.  Only physical regular files below each provider's ``raw/``
directory count.  This module is intentionally provider-neutral so HistData
and Dukascopy cannot accidentally maintain independent 40 GiB limits.

The inventory never follows symlinks.  Hard links are counted once by their
``(st_dev, st_ino)`` identity, including when the same inode is visible from
more than one configured raw root.  Callers must hold :func:`acquisition_lock`
from their preflight through publication of the provider manifest; the helper
itself does not try to coordinate a lock implicitly around an individual
inventory call.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias

GIB = 1024**3
MAX_RAW_BYTES = 40 * GIB
MAX_CORPUS_BYTES = MAX_RAW_BYTES  # descriptive compatibility alias
MAX_DATASET_BYTES = MAX_RAW_BYTES  # compatibility with provider budget terminology
MAX_TOTAL_RAW_BYTES = MAX_RAW_BYTES
MIN_FREE_RATIO_NUMERATOR = 1
MIN_FREE_RATIO_DENOMINATOR = 5
MIN_FREE_PERCENT = 20
MIN_FREE_RATIO = MIN_FREE_RATIO_NUMERATOR / MIN_FREE_RATIO_DENOMINATOR
DEFAULT_LOCK_NAME = ".mtf-market-data-acquire.lock"

PathLike: TypeAlias = str | os.PathLike[str]
RootsInput: TypeAlias = PathLike | Iterable[PathLike]


class StorageBudgetError(RuntimeError):
    """The aggregate raw budget or common acquisition lock was violated."""


class AcquisitionLockError(StorageBudgetError):
    """The common acquisition lock cannot be acquired safely."""


@dataclass(frozen=True, slots=True)
class PhysicalRawFile:
    """One regular physical file included in the raw-byte inventory."""

    path: Path
    size: int
    st_dev: int
    st_ino: int

    @property
    def identity(self) -> tuple[int, int]:
        return self.st_dev, self.st_ino

    @property
    def device(self) -> int:
        return self.st_dev

    @property
    def inode(self) -> int:
        return self.st_ino

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "size": self.size,
            "st_dev": self.st_dev,
            "st_ino": self.st_ino,
        }


@dataclass(frozen=True, slots=True)
class RawInventory:
    """Snapshot of physical raw bytes and the filesystem reserve inputs."""

    roots: tuple[Path, ...]
    files: tuple[PhysicalRawFile, ...]
    raw_bytes: int
    filesystem_bytes: int
    available_bytes: int
    filesystem_path: Path

    @property
    def current_raw_bytes(self) -> int:
        return self.raw_bytes

    @property
    def physical_file_count(self) -> int:
        return len(self.files)

    @property
    def deduplicated_file_count(self) -> int:
        return len(self.files)

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def required_reserve_bytes(self) -> int:
        return ceil_ratio(self.filesystem_bytes, MIN_FREE_RATIO_NUMERATOR, MIN_FREE_RATIO_DENOMINATOR)

    @property
    def reserve_bytes(self) -> int:
        return self.required_reserve_bytes

    def to_dict(self) -> dict[str, Any]:
        return {
            "roots": [str(root) for root in self.roots],
            "files": [item.to_dict() for item in self.files],
            "raw_bytes": self.raw_bytes,
            "filesystem_bytes": self.filesystem_bytes,
            "available_bytes": self.available_bytes,
            "filesystem_path": str(self.filesystem_path),
            "required_reserve_bytes": self.required_reserve_bytes,
        }


@dataclass(frozen=True, slots=True)
class BudgetProjection:
    """Result of applying an incoming/replacement projection to an inventory."""

    inventory: RawInventory
    additional_bytes: int
    replacing_bytes: int
    projected_bytes: int
    max_raw_bytes: int
    required_reserve_bytes: int
    available_after_additional_bytes: int
    ok: bool
    reasons: tuple[str, ...]

    @property
    def current_raw_bytes(self) -> int:
        return self.inventory.raw_bytes

    @property
    def current_raw(self) -> int:
        return self.inventory.raw_bytes

    @property
    def projected_raw_bytes(self) -> int:
        return self.projected_bytes

    @property
    def projected_raw(self) -> int:
        return self.projected_bytes

    @property
    def available_bytes(self) -> int:
        return self.inventory.available_bytes

    @property
    def available_after_additional(self) -> int:
        return self.available_after_additional_bytes

    @property
    def filesystem_bytes(self) -> int:
        return self.inventory.filesystem_bytes

    @property
    def reserve_bytes(self) -> int:
        return self.required_reserve_bytes

    @property
    def within_budget(self) -> bool:
        return self.ok

    @property
    def failed_reasons(self) -> tuple[str, ...]:
        return self.reasons

    def to_dict(self) -> dict[str, Any]:
        return {
            "current_raw_bytes": self.current_raw_bytes,
            "additional_bytes": self.additional_bytes,
            "replacing_bytes": self.replacing_bytes,
            "projected_bytes": self.projected_bytes,
            "max_raw_bytes": self.max_raw_bytes,
            "filesystem_bytes": self.filesystem_bytes,
            "available_bytes": self.available_bytes,
            "available_after_additional_bytes": self.available_after_additional_bytes,
            "required_reserve_bytes": self.required_reserve_bytes,
            "ok": self.ok,
            "reasons": list(self.reasons),
            "inventory": self.inventory.to_dict(),
        }


def ceil_ratio(value: int, numerator: int, denominator: int) -> int:
    """Return ``ceil(value * numerator / denominator)`` using integer math."""

    _nonnegative_int(value, "value")
    _nonnegative_int(numerator, "numerator")
    if isinstance(denominator, bool) or not isinstance(denominator, int) or denominator <= 0:
        raise ValueError("denominator must be a positive integer")
    return (value * numerator + denominator - 1) // denominator


def inventory_raw(
    roots: RootsInput,
    *,
    filesystem_path: PathLike | None = None,
) -> RawInventory:
    """Inventory physical files below provider ``raw/`` roots.

    ``roots`` accepts either provider data roots (``root/raw`` is scanned) or
    explicit paths ending in ``raw``.  Missing raw directories are treated as
    empty, while an existing symlink or non-directory is rejected.  A symlink
    anywhere below an existing raw directory is rejected rather than followed
    or silently omitted, so an alias cannot evade the aggregate guard.
    """

    normalized_roots = _normalize_roots(roots)
    scan_roots = tuple(_raw_directory(root) for root in normalized_roots)
    files: list[PhysicalRawFile] = []
    seen: set[tuple[int, int]] = set()
    for raw_root in sorted(scan_roots, key=lambda item: str(item)):
        _reject_symlink_ancestors(raw_root)
        if not raw_root.exists():
            continue
        info = _lstat(raw_root, "raw root")
        if stat.S_ISLNK(info.st_mode):
            raise StorageBudgetError(f"raw root is a symlink: {raw_root}")
        if not stat.S_ISDIR(info.st_mode):
            raise StorageBudgetError(f"raw root is not a directory: {raw_root}")
        _walk_raw(raw_root, files, seen)

    selected_filesystem_path = _filesystem_path(normalized_roots, filesystem_path)
    filesystem_bytes, available_bytes = _statvfs_bytes(selected_filesystem_path)
    ordered_files = tuple(sorted(files, key=lambda item: str(item.path)))
    return RawInventory(
        roots=normalized_roots,
        files=ordered_files,
        raw_bytes=sum(item.size for item in ordered_files),
        filesystem_bytes=filesystem_bytes,
        available_bytes=available_bytes,
        filesystem_path=selected_filesystem_path,
    )


def inventory_physical_raw(
    roots: RootsInput,
    *,
    filesystem_path: PathLike | None = None,
) -> RawInventory:
    """Descriptive alias for :func:`inventory_raw`."""

    return inventory_raw(roots, filesystem_path=filesystem_path)


def inventory_physical(
    roots: RootsInput,
    *,
    filesystem_path: PathLike | None = None,
) -> RawInventory:
    """Short alias for the physical raw inventory helper."""

    return inventory_raw(roots, filesystem_path=filesystem_path)


def project_budget(
    roots: RootsInput,
    *,
    additional_bytes: int = 0,
    replacing_bytes: int = 0,
    filesystem_path: PathLike | None = None,
    max_raw_bytes: int | None = None,
) -> BudgetProjection:
    """Calculate the aggregate raw and filesystem-reserve projection.

    The projection itself does not raise when either gate is false; this makes
    it possible to record a bounded diagnostic before :func:`guard_budget`
    raises.  Invalid arguments and an over-large replacement are rejected.
    """

    additional = _nonnegative_int(additional_bytes, "additional_bytes")
    replacing = _nonnegative_int(replacing_bytes, "replacing_bytes")
    maximum = MAX_RAW_BYTES if max_raw_bytes is None else _positive_int(max_raw_bytes, "max_raw_bytes")
    inventory = inventory_raw(roots, filesystem_path=filesystem_path)
    if replacing > inventory.raw_bytes:
        raise StorageBudgetError(
            f"replacing_bytes ({replacing}) exceeds the current physical raw inventory ({inventory.raw_bytes})"
        )
    projected = inventory.raw_bytes - replacing + additional
    reserve = ceil_ratio(inventory.filesystem_bytes, MIN_FREE_RATIO_NUMERATOR, MIN_FREE_RATIO_DENOMINATOR)
    available_after = inventory.available_bytes - additional
    reasons: list[str] = []
    if projected > maximum:
        reasons.append(f"projected raw bytes {projected} exceed aggregate limit {maximum}")
    if available_after < reserve:
        reasons.append(f"available bytes after incoming data {available_after} fall below reserve {reserve}")
    return BudgetProjection(
        inventory=inventory,
        additional_bytes=additional,
        replacing_bytes=replacing,
        projected_bytes=projected,
        max_raw_bytes=maximum,
        required_reserve_bytes=reserve,
        available_after_additional_bytes=available_after,
        ok=not reasons,
        reasons=tuple(reasons),
    )


evaluate_budget = project_budget


def guard_budget(
    roots: RootsInput,
    *,
    additional_bytes: int = 0,
    replacing_bytes: int = 0,
    filesystem_path: PathLike | None = None,
    max_raw_bytes: int | None = None,
) -> BudgetProjection:
    """Raise unless aggregate raw and 20% reserve gates both pass."""

    projection = project_budget(
        roots,
        additional_bytes=additional_bytes,
        replacing_bytes=replacing_bytes,
        filesystem_path=filesystem_path,
        max_raw_bytes=max_raw_bytes,
    )
    if not projection.ok:
        raise StorageBudgetError("; ".join(projection.reasons))
    return projection


def assert_budget(
    roots: RootsInput,
    *,
    additional_bytes: int = 0,
    replacing_bytes: int = 0,
    filesystem_path: PathLike | None = None,
    max_raw_bytes: int | None = None,
) -> BudgetProjection:
    """Descriptive alias for the raising aggregate guard."""

    return guard_budget(
        roots,
        additional_bytes=additional_bytes,
        replacing_bytes=replacing_bytes,
        filesystem_path=filesystem_path,
        max_raw_bytes=max_raw_bytes,
    )


def check_budget(
    roots: RootsInput,
    *,
    additional_bytes: int = 0,
    replacing_bytes: int = 0,
    filesystem_path: PathLike | None = None,
    max_raw_bytes: int | None = None,
) -> BudgetProjection:
    """Compatibility alias for the raising aggregate guard."""

    return guard_budget(
        roots,
        additional_bytes=additional_bytes,
        replacing_bytes=replacing_bytes,
        filesystem_path=filesystem_path,
        max_raw_bytes=max_raw_bytes,
    )


def shared_lock_root(root: PathLike, *, provider: str | None = None) -> Path:
    """Return the namespace root used by both provider writers.

    The canonical Dukascopy root is ``<market-data>/dukascopy``; its lock must
    live beside the HistData root so both writers serialize against one file.
    An explicit provider is preferred by integrations, while the directory
    name remains a compatibility fallback for callers using the helper alone.
    """

    path = _absolute_path(Path(root))
    normalized_provider = provider.strip().lower() if isinstance(provider, str) else None
    if normalized_provider == "dukascopy" or (normalized_provider is None and path.name.lower() == "dukascopy"):
        return path.parent
    return path


@contextmanager
def acquisition_lock(
    root: PathLike,
    *,
    lock_root: PathLike | None = None,
    lock_name: str = DEFAULT_LOCK_NAME,
) -> Iterator[Path]:
    """Acquire an exclusive common lock and release it on every exit path."""

    if not isinstance(lock_name, str) or not lock_name or Path(lock_name).name != lock_name:
        raise ValueError("lock_name must be a non-empty file name")
    namespace = shared_lock_root(root) if lock_root is None else _absolute_path(Path(lock_root))
    _reject_symlink_ancestors(namespace)
    if namespace.exists():
        info = _lstat(namespace, "acquisition lock root")
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise AcquisitionLockError(f"acquisition lock root is not a directory: {namespace}")
    else:
        raise AcquisitionLockError(f"acquisition lock root does not exist: {namespace}")
    lock_path = namespace / lock_name
    _reject_symlink_ancestors(lock_path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except FileExistsError as exc:
        raise AcquisitionLockError(f"another market-data acquisition is already running: {lock_path}") from exc
    except OSError as exc:
        raise AcquisitionLockError(f"cannot acquire market-data lock: {lock_path}") from exc
    try:
        os.close(descriptor)
        yield lock_path
    finally:
        with suppress(FileNotFoundError, OSError):
            lock_path.unlink()


def _normalize_roots(roots: RootsInput) -> tuple[Path, ...]:
    values = (roots,) if isinstance(roots, (str, os.PathLike)) else tuple(roots)
    if not values:
        raise ValueError("at least one raw root is required")
    normalized: list[Path] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, (str, os.PathLike)):
            raise TypeError("raw roots must be paths")
        path = _absolute_path(Path(value))
        key = str(path)
        if key not in seen:
            normalized.append(path)
            seen.add(key)
    return tuple(normalized)


def _raw_directory(root: Path) -> Path:
    return root if root.name == "raw" else root / "raw"


def _walk_raw(path: Path, files: list[PhysicalRawFile], seen: set[tuple[int, int]]) -> None:
    try:
        with os.scandir(path) as directory:
            entries = sorted(directory, key=lambda item: item.name)
            for entry in entries:
                entry_path = Path(entry.path)
                try:
                    info = entry.stat(follow_symlinks=False)
                except OSError as exc:
                    raise StorageBudgetError(f"cannot inspect raw entry: {entry_path}") from exc
                if stat.S_ISLNK(info.st_mode):
                    raise StorageBudgetError(f"symlink is not allowed below raw/: {entry_path}")
                if stat.S_ISDIR(info.st_mode):
                    _walk_raw(entry_path, files, seen)
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise StorageBudgetError(f"non-regular entry is not allowed below raw/: {entry_path}")
                identity = (int(info.st_dev), int(info.st_ino))
                if identity in seen:
                    continue
                seen.add(identity)
                files.append(PhysicalRawFile(entry_path, int(info.st_size), identity[0], identity[1]))
    except OSError as exc:
        raise StorageBudgetError(f"cannot inventory raw directory: {path}") from exc


def _filesystem_path(roots: tuple[Path, ...], explicit: PathLike | None) -> Path:
    if explicit is not None:
        return _absolute_path(Path(explicit))
    return roots[0]


def _statvfs_bytes(path: Path) -> tuple[int, int]:
    try:
        usage = os.statvfs(path)
    except OSError as exc:
        raise StorageBudgetError(f"cannot inspect filesystem capacity: {path}") from exc
    fragment = int(getattr(usage, "f_frsize", 0) or getattr(usage, "f_bsize", 0))
    if fragment <= 0:
        raise StorageBudgetError(f"filesystem block size is invalid: {path}")
    total = int(usage.f_blocks) * fragment
    available = int(usage.f_bavail) * fragment
    if total <= 0 or available < 0:
        raise StorageBudgetError(f"filesystem capacity is invalid: {path}")
    return total, available


def _lstat(path: Path, label: str) -> os.stat_result:
    try:
        return os.lstat(path)
    except OSError as exc:
        raise StorageBudgetError(f"cannot inspect {label}: {path}") from exc


def _reject_symlink_ancestors(path: Path) -> None:
    absolute = _absolute_path(path)
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise StorageBudgetError(f"cannot inspect path component: {current}") from exc
        if stat.S_ISLNK(info.st_mode):
            raise StorageBudgetError(f"path contains a symlink component: {current}")


def _absolute_path(path: Path) -> Path:
    return Path(os.path.abspath(path))


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


__all__ = [
    "AcquisitionLockError",
    "BudgetProjection",
    "DEFAULT_LOCK_NAME",
    "GIB",
    "MAX_CORPUS_BYTES",
    "MAX_DATASET_BYTES",
    "MAX_RAW_BYTES",
    "MAX_TOTAL_RAW_BYTES",
    "MIN_FREE_PERCENT",
    "MIN_FREE_RATIO_DENOMINATOR",
    "MIN_FREE_RATIO_NUMERATOR",
    "PhysicalRawFile",
    "RawInventory",
    "StorageBudgetError",
    "acquisition_lock",
    "assert_budget",
    "ceil_ratio",
    "check_budget",
    "guard_budget",
    "inventory_physical_raw",
    "inventory_physical",
    "inventory_raw",
    "evaluate_budget",
    "project_budget",
    "shared_lock_root",
]
