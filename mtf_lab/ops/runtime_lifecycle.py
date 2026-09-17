"""Safe lifecycle management for private MTF Lab runtimes.

The runtime preparer creates a complete, relocatable environment, but the
environment itself is an implementation detail.  This module owns the small
state machine around that detail: one canonical ``runtime`` directory,
short-lived review directories, an optional single rollback, and a locked,
fail-closed garbage collector.  It never touches market data, research state,
databases, credentials, or files outside the MTF runtime root.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import stat
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_RUNTIME_ROOT = Path.home() / ".local" / "share" / "mtf-lab"
ACTIVE_RUNTIME_NAME = "runtime"
REVIEW_PREFIX = "runtime-review-"
ROLLBACK_PREFIX = "runtime-rollback-"
BUILD_PREFIXES = (".runtime-staging-", ".runtime-relocated-", ".runtime-build-")
LIFECYCLE_MARKER = "RUNTIME_LIFECYCLE.json"
STAGED_MARKER = "STAGED_RUNTIME.json"
LIFECYCLE_SCHEMA = "mtf-lab.runtime-lifecycle.v1"
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class RuntimeLifecycleError(RuntimeError):
    """A lifecycle operation cannot prove its safety contract."""


@dataclass(frozen=True, slots=True)
class RuntimeRecord:
    """Bounded classification returned by :class:`RuntimeLifecycle`."""

    name: str
    path: Path
    role: str
    state: str
    safe_to_delete: bool
    reason: str
    apparent_bytes: int
    allocated_bytes: int
    files: int
    directories: int
    symlinks: int
    in_use: bool = False
    manifest_sha256: str | None = None

    def to_dict(self, *, root: Path | None = None) -> dict[str, Any]:
        path = self.path
        path_value = str(path)
        if root is not None:
            with contextlib.suppress(ValueError):
                path_value = path.relative_to(root).as_posix()
        return {
            "name": self.name,
            "path": path_value,
            "role": self.role,
            "state": self.state,
            "safe_to_delete": self.safe_to_delete,
            "reason": self.reason,
            "apparent_bytes": self.apparent_bytes,
            "allocated_bytes": self.allocated_bytes,
            "files": self.files,
            "directories": self.directories,
            "symlinks": self.symlinks,
            "in_use": self.in_use,
            "manifest_sha256": self.manifest_sha256,
        }


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def _safe_json(path: Path) -> dict[str, Any] | None:
    try:
        info = os.lstat(path)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return dict(value) if isinstance(value, Mapping) else None


def _marker_pid(marker: Mapping[str, Any] | None) -> int:
    if marker is None:
        return 0
    value = marker.get("pid", 0)
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _atomic_json(path: Path, value: Mapping[str, Any], *, replace: bool = True) -> None:
    if os.path.lexists(path.parent):
        parent_info = os.lstat(path.parent)
        if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
            raise RuntimeLifecycleError(f"lifecycle marker parent is unsafe: {path.parent}")
    else:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    payload = (json.dumps(dict(value), ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    fd = os.open(temporary, flags, 0o600)
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(fd, payload[offset:])
            if written <= 0:
                raise OSError("runtime lifecycle marker write made no progress")
            offset += written
        os.fchmod(fd, 0o600)
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        if not replace and os.path.lexists(path):
            raise RuntimeLifecycleError(f"lifecycle marker already exists: {path}")
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | os.O_NOFOLLOW)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        raise


def _retarget_venv_configs(runtime: Path) -> None:  # noqa: C901 - one post-move venv repair gate
    """Repair absolute ``pyvenv.cfg`` references after a directory move."""

    base_bin = (runtime / "base-python" / "bin").resolve(strict=False)
    if not _is_within(base_bin, runtime) or not base_bin.is_dir():
        raise RuntimeLifecycleError(f"runtime base is missing or escapes candidate: {runtime}")
    for name in ("runtime-python", "dev-python"):
        config = runtime / name / "pyvenv.cfg"
        if not os.path.lexists(config):
            continue
        info = os.lstat(config)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeLifecycleError(f"venv config is unsafe: {config}")
        lines = config.read_text(encoding="utf-8").splitlines(keepends=True)
        replaced: list[str] = []
        seen_home = seen_executable = False
        for line in lines:
            if line.startswith("home = "):
                replaced.append(f"home = {base_bin}\n")
                seen_home = True
            elif line.startswith("executable = "):
                replaced.append(f"executable = {base_bin / 'python3.12'}\n")
                seen_executable = True
            else:
                replaced.append(line)
        if not seen_home:
            replaced.insert(0, f"home = {base_bin}\n")
        if not seen_executable:
            replaced.insert(1, f"executable = {base_bin / 'python3.12'}\n")
        payload = "".join(replaced).encode("utf-8")
        temporary = config.with_name(f".{config.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            os.replace(temporary, config)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()
            raise


def _tree_stats(path: Path) -> tuple[int, int, int, int, int, set[tuple[int, int]]]:
    apparent = allocated = files = directories = symlinks = 0
    seen: set[tuple[int, int]] = set()
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            entries = list(os.scandir(current))
        except OSError as exc:
            raise RuntimeLifecycleError(f"runtime tree unreadable: {current}") from exc
        directories += 1
        for entry in entries:
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise RuntimeLifecycleError(f"runtime entry unreadable: {entry.path}") from exc
            key = (info.st_dev, info.st_ino)
            if stat.S_ISLNK(info.st_mode):
                symlinks += 1
                continue
            if key in seen:
                continue
            seen.add(key)
            if stat.S_ISDIR(info.st_mode):
                stack.append(Path(entry.path))
            elif stat.S_ISREG(info.st_mode):
                files += 1
                apparent += info.st_size
                allocated += info.st_blocks * 512
            else:
                raise RuntimeLifecycleError(f"unsupported runtime entry type: {entry.path}")
    return apparent, allocated, files, directories, symlinks, seen


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _proc_reference(path: Path) -> tuple[int, str] | None:  # noqa: C901 - bounded process-reference safety gate
    """Return a live process reference to *path*, if observable."""

    target_root = path.resolve(strict=False)
    proc = Path("/proc")
    try:
        processes = list(proc.iterdir())
    except OSError:
        return None
    numeric = [process for process in processes if process.name.isdigit()]

    def read_target(link: Path) -> Path | None:
        try:
            value = os.readlink(link)
        except OSError:
            return None
        if not value.startswith("/"):
            return None
        return Path(value.removesuffix(" (deleted)"))

    # Cwd/exe/root are cheap and cover normal runtime processes.  Check them
    # for every PID before walking thousands of file descriptors; this keeps a
    # short-lived build from disappearing while an unrelated process's FDs are
    # being inspected.
    for process in numeric:
        if not process.name.isdigit():
            continue
        pid = int(process.name)
        for name in ("cwd", "exe", "root"):
            target = read_target(process / name)
            if target is not None:
                with contextlib.suppress(ValueError):
                    target.relative_to(target_root)
                    return pid, name
    for process in numeric:
        pid = int(process.name)
        try:
            descriptors = list((process / "fd").iterdir())
        except OSError:
            continue
        for descriptor in descriptors:
            target = read_target(descriptor)
            if target is not None:
                with contextlib.suppress(ValueError):
                    target.relative_to(target_root)
                    return pid, f"fd/{descriptor.name}"
    return None


def _proc_references(paths: Sequence[Path]) -> dict[Path, tuple[int, str]]:  # noqa: C901 - shared bounded scan
    """Scan ``/proc`` once and map observable process references to candidates."""

    roots = {path.resolve(strict=False): path for path in paths}
    found: dict[Path, tuple[int, str]] = {}
    try:
        processes = [item for item in Path("/proc").iterdir() if item.name.isdigit()]
    except OSError:
        return found

    def remember(target: Path, pid: int, reference: str) -> None:
        for candidate_root, candidate in roots.items():
            if candidate in found:
                continue
            with contextlib.suppress(ValueError):
                target.relative_to(candidate_root)
                found[candidate] = (pid, reference)

    def read_target(link: Path) -> Path | None:
        try:
            value = os.readlink(link)
        except OSError:
            return None
        if not value.startswith("/"):
            return None
        return Path(value.removesuffix(" (deleted)"))

    for process in processes:
        pid = int(process.name)
        for name in ("cwd", "exe", "root"):
            target = read_target(process / name)
            if target is not None:
                remember(target, pid, name)
        if len(found) == len(roots):
            return found
    # File descriptors are more expensive, so only inspect candidates that did
    # not already have a cwd/executable/root reference.
    for process in processes:
        pid = int(process.name)
        try:
            descriptors = list((process / "fd").iterdir())
        except OSError:
            continue
        for descriptor in descriptors:
            target = read_target(descriptor)
            if target is not None:
                remember(target, pid, f"fd/{descriptor.name}")
            if len(found) == len(roots):
                return found
    return found


def _validate_confined_tree(path: Path, root: Path) -> None:  # noqa: C901 - one immutable tree-safety gate
    """Validate ownership, entry types, and internal-only symlinks."""

    uid = os.getuid()
    root_resolved = path.resolve(strict=False)
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            current_info = os.lstat(current)
        except OSError as exc:
            raise RuntimeLifecycleError(f"runtime entry disappeared: {current}") from exc
        if current_info.st_uid != uid:
            raise RuntimeLifecycleError(f"runtime entry has unexpected owner: {current}")
        if stat.S_ISLNK(current_info.st_mode):
            target = (current.parent / os.readlink(current)).resolve(strict=False)
            if not _is_within(target, root_resolved):
                raise RuntimeLifecycleError(f"runtime symlink escapes candidate: {current}")
            continue
        if not stat.S_ISDIR(current_info.st_mode):
            continue
        try:
            entries = list(os.scandir(current))
        except OSError as exc:
            raise RuntimeLifecycleError(f"runtime tree unreadable: {current}") from exc
        for entry in entries:
            child = Path(entry.path)
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                target = (child.parent / os.readlink(child)).resolve(strict=False)
                if not _is_within(target, root_resolved):
                    raise RuntimeLifecycleError(f"runtime symlink escapes candidate: {child}")
            elif stat.S_ISDIR(info.st_mode):
                stack.append(child)
            elif stat.S_ISREG(info.st_mode):
                if info.st_uid != uid:
                    raise RuntimeLifecycleError(f"runtime file has unexpected owner: {child}")
            else:
                raise RuntimeLifecycleError(f"unsupported runtime entry type: {child}")


def _validate_runtime_layout(path: Path) -> None:
    """Require the minimum venv layout before a review can be collected."""

    for relative in ("base-python", "runtime-python", "dev-python"):
        target = path / relative
        try:
            info = os.lstat(target)
        except OSError as exc:
            raise RuntimeLifecycleError(f"runtime layout is incomplete: {target}") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise RuntimeLifecycleError(f"runtime layout entry is unsafe: {target}")


def _remove_tree(path: Path) -> None:
    """Remove a previously validated tree without following symlinks."""

    directories: list[Path] = []
    stack = [path]
    while stack:
        current = stack.pop()
        directories.append(current)
        for entry in os.scandir(current):
            child = Path(entry.path)
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode) and not stat.S_ISLNK(info.st_mode):
                stack.append(child)
            else:
                os.unlink(child)
    for directory in reversed(directories):
        os.rmdir(directory)


class RuntimeLifecycle:
    """Manage one MTF runtime root with an inter-process lock."""

    def __init__(
        self,
        root: str | Path | None = None,
        *,
        keep_reviews: int = 1,
        keep_rollbacks: int = 1,
        stale_after_seconds: float = 3600.0,
    ) -> None:
        selected = Path(root or DEFAULT_RUNTIME_ROOT).expanduser()
        if not selected.is_absolute():
            raise RuntimeLifecycleError("runtime root must be absolute")
        self.root = Path(os.path.abspath(selected))
        self.keep_reviews = max(0, int(keep_reviews))
        self.keep_rollbacks = max(0, int(keep_rollbacks))
        self.stale_after_seconds = max(0.0, float(stale_after_seconds))

    @property
    def active(self) -> Path:
        return self.root / ACTIVE_RUNTIME_NAME

    @property
    def lock_path(self) -> Path:
        return self.root / ".runtime-manager.lock"

    def _ensure_root(self) -> Path:
        current = Path(self.root.anchor)
        for component in self.root.parts:
            if component == self.root.anchor:
                continue
            current /= component
            if not os.path.lexists(current):
                current.mkdir(mode=0o700)
            info = os.lstat(current)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise RuntimeLifecycleError(f"runtime root ancestor is unsafe: {current}")
        os.chmod(self.root, 0o700)
        if os.stat(self.root).st_uid != os.getuid():
            raise RuntimeLifecycleError(f"runtime root owner mismatch: {self.root}")
        return self.root

    @contextlib.contextmanager
    def lock(self) -> Iterator[None]:
        self._ensure_root()
        flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
        fd = os.open(self.lock_path, flags, 0o600)
        try:
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _assert_direct_child(self, path: str | Path) -> Path:
        target = Path(os.path.abspath(Path(path).expanduser()))
        if not target.is_absolute():
            raise RuntimeLifecycleError("runtime path must be absolute")
        if target.parent != self.root:
            raise RuntimeLifecycleError(f"runtime path must be a direct child of {self.root}")
        if not _NAME_RE.fullmatch(target.name):
            raise RuntimeLifecycleError("runtime path contains unsafe characters")
        if not _is_within(target, self.root):
            raise RuntimeLifecycleError("runtime path escapes runtime root")
        if os.path.lexists(target) and stat.S_ISLNK(os.lstat(target).st_mode):
            raise RuntimeLifecycleError("runtime path must not be a symlink")
        return target

    def validate_build_destination(self, destination: str | Path) -> Path:
        target = Path(destination).expanduser()
        if not target.is_absolute():
            raise RuntimeLifecycleError("runtime destination must be absolute")
        if target.name != "runtime" or target.parent.parent != self.root:
            raise RuntimeLifecycleError(
                f"runtime reviews must be generated under {self.root}/{REVIEW_PREFIX}<id>/runtime"
            )
        parent = self._assert_direct_child(target.parent)
        if not parent.name.startswith(REVIEW_PREFIX):
            raise RuntimeLifecycleError("caller-selected runtime review name is not managed")
        if os.path.lexists(parent) and (
            stat.S_ISLNK(os.lstat(parent).st_mode) or not stat.S_ISDIR(os.lstat(parent).st_mode)
        ):
            raise RuntimeLifecycleError("review parent is not a regular directory")
        return target

    def allocate_review(self) -> Path:
        with self.lock():
            self.recover_unlocked()
            self.gc_unlocked(max_reviews=self.keep_reviews)
            for _ in range(10):
                name = f"{REVIEW_PREFIX}{dt.datetime.now(dt.UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:8]}"
                parent = self.root / name
                try:
                    parent.mkdir(mode=0o700)
                except FileExistsError:
                    continue
                destination = parent / "runtime"
                self._write_build_marker_unlocked(destination)
                return destination
            raise RuntimeLifecycleError("could not allocate a unique runtime review")

    def _write_build_marker_unlocked(self, destination: Path) -> None:
        destination = self.validate_build_destination(destination)
        parent = destination.parent
        parent.mkdir(mode=0o700, parents=False, exist_ok=True)
        marker = {
            "schema": LIFECYCLE_SCHEMA,
            "project": "mtf-lab",
            "role": "review",
            "state": "BUILDING",
            "pid": os.getpid(),
            "destination": str(destination),
            "created_at": _now(),
        }
        existing = _safe_json(parent / LIFECYCLE_MARKER)
        if existing is not None and existing.get("state") == "BUILDING":
            existing_pid = _marker_pid(existing)
            if _pid_alive(existing_pid):
                if existing_pid == os.getpid():
                    return
                raise RuntimeLifecycleError("runtime review is already being built")
        _atomic_json(parent / LIFECYCLE_MARKER, marker)

    def begin_unlocked(self, destination: str | Path) -> Path:
        target = self.validate_build_destination(destination)
        self._write_build_marker_unlocked(target)
        return target

    def mark_completed_unlocked(self, destination: str | Path, manifest: Mapping[str, Any]) -> None:
        target = self.validate_build_destination(destination)
        previous = _safe_json(target.parent / LIFECYCLE_MARKER)
        marker = {
            "schema": LIFECYCLE_SCHEMA,
            "project": "mtf-lab",
            "role": "review",
            "state": "REVIEW_READY",
            "pid": None,
            "destination": str(target),
            "created_at": previous.get("created_at") if previous else _now(),
            "completed_at": _now(),
            "manifest_sha256": _sha256(target / STAGED_MARKER),
            "manifest_state": manifest.get("state"),
        }
        _atomic_json(target.parent / LIFECYCLE_MARKER, marker)

    def mark_failed_unlocked(self, destination: str | Path, error: BaseException) -> None:
        target = self.validate_build_destination(destination)
        marker = {
            "schema": LIFECYCLE_SCHEMA,
            "project": "mtf-lab",
            "role": "review",
            "state": "FAILED",
            "pid": None,
            "destination": str(target),
            "failed_at": _now(),
            "error_type": type(error).__name__,
            "error": str(error)[:1000],
        }
        _atomic_json(target.parent / LIFECYCLE_MARKER, marker)

    def recover_unlocked(self) -> dict[str, Any]:
        journal = self.root / ".runtime-promotion.json"
        if not os.path.lexists(journal):
            return {"state": "CLEAN", "recovered": False}
        record = _safe_json(journal)
        if record is None or record.get("schema") != LIFECYCLE_SCHEMA:
            raise RuntimeLifecycleError("promotion journal is malformed; recovery required")
        active = self.active
        rollback = Path(str(record.get("rollback", "")))
        staged = Path(str(record.get("staged", "")))
        for candidate in (rollback, staged):
            if not candidate.is_absolute() or not _is_within(candidate, self.root):
                raise RuntimeLifecycleError("promotion journal path escapes runtime root")
        # The only automatic recovery is restoring a missing canonical runtime
        # from the known rollback.  Ambiguous states remain fail-closed.
        active_info = os.lstat(active) if os.path.lexists(active) else None
        rollback_info = os.lstat(rollback) if os.path.lexists(rollback) else None
        staged_info = os.lstat(staged) if os.path.lexists(staged) else None
        if (
            active_info is None
            and rollback_info is not None
            and stat.S_ISDIR(rollback_info.st_mode)
            and staged_info is None
        ):
            os.rename(rollback, active)
            active_info = os.lstat(active)
        if active_info is not None and stat.S_ISDIR(active_info.st_mode) and not stat.S_ISLNK(active_info.st_mode):
            journal.unlink()
            return {"state": "RECOVERED", "recovered": True}
        raise RuntimeLifecycleError("promotion journal requires manual review")

    def _record_review_unlocked(  # noqa: C901 - one immutable review-classification gate
        self, parent: Path, process_references: Mapping[Path, tuple[int, str]] | None = None
    ) -> RuntimeRecord:
        destination = parent / "runtime"
        apparent = allocated = files = directories = symlinks = 0
        manifest_sha: str | None = None
        marker = _safe_json(parent / LIFECYCLE_MARKER)
        state = str(marker.get("state")) if marker else "LEGACY"
        if marker is not None and (
            marker.get("schema") != LIFECYCLE_SCHEMA
            or marker.get("project") != "mtf-lab"
            or marker.get("role") != "review"
            or marker.get("destination") != str(destination)
            or state not in {"BUILDING", "REVIEW_READY", "FAILED"}
        ):
            return RuntimeRecord(
                parent.name,
                parent,
                "unknown",
                state,
                False,
                "lifecycle marker is missing or ambiguous",
                0,
                0,
                0,
                1,
                0,
            )
        reason = "legacy review metadata"
        safe = False
        in_use = False
        manifest: dict[str, Any] | None = None
        valid = False
        try:
            info = os.lstat(destination)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise RuntimeLifecycleError("review runtime is not a regular directory")
            manifest = _safe_json(destination / STAGED_MARKER)
            valid = (
                manifest is not None
                and manifest.get("project") == "mtf-lab"
                and manifest.get("state") == "STAGED_RUNTIME"
                and manifest.get("promotion_state") == "NOT_PROMOTED"
                and manifest.get("current_pointer") is None
                and manifest.get("destination") == str(destination)
            )
            if valid:
                _validate_runtime_layout(destination)
            _validate_confined_tree(destination, self.root)
            apparent, allocated, files, directories, symlinks, _ = _tree_stats(destination)
            manifest_path = destination / STAGED_MARKER
            if manifest_path.is_file():
                manifest_sha = _sha256(manifest_path)
        except (OSError, RuntimeLifecycleError) as exc:
            if state == "FAILED" and not os.path.lexists(destination):
                return RuntimeRecord(
                    parent.name, parent, "review", state, True, "failed review without payload", 0, 0, 0, 1, 0
                )
            if state == "BUILDING" and not os.path.lexists(destination):
                pid = _marker_pid(marker)
                if _pid_alive(pid):
                    return RuntimeRecord(
                        parent.name, parent, "temporary_in_use", state, False, "build is live", 0, 0, 0, 1, 0, True
                    )
                age = 0.0
                created = marker.get("created_at") if marker else None
                if isinstance(created, str):
                    with contextlib.suppress(ValueError):
                        age = (
                            dt.datetime.now(dt.UTC) - dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
                        ).total_seconds()
                if age >= self.stale_after_seconds:
                    return RuntimeRecord(
                        parent.name, parent, "review", state, True, "stale incomplete build", 0, 0, 0, 1, 0
                    )
                return RuntimeRecord(
                    parent.name, parent, "temporary_stale", state, False, "build payload is incomplete", 0, 0, 0, 1, 0
                )
            if not os.path.lexists(destination) and marker is None:
                try:
                    evidence_stats = _tree_stats(parent)
                except RuntimeLifecycleError:
                    evidence_stats = (0, 0, 0, 1, 0, set())
                evidence_reference = (
                    process_references.get(parent.resolve(strict=False))
                    if process_references is not None
                    else _proc_reference(parent)
                )
                return RuntimeRecord(
                    parent.name,
                    parent,
                    "review_evidence",
                    "LEGACY_EVIDENCE",
                    evidence_reference is None,
                    "runtime payload removed; evidence preserved",
                    evidence_stats[0],
                    evidence_stats[1],
                    evidence_stats[2],
                    evidence_stats[3],
                    evidence_stats[4],
                    evidence_reference is not None,
                    None,
                )
            reason = str(exc)
            return RuntimeRecord(
                parent.name,
                parent,
                "unknown",
                state,
                False,
                reason,
                apparent,
                allocated,
                files,
                directories,
                symlinks,
                False,
                manifest_sha,
            )
        reference = None
        if process_references is not None:
            reference = process_references.get(destination.resolve(strict=False)) or process_references.get(
                parent.resolve(strict=False)
            )
        else:
            reference = _proc_reference(destination)
        in_use = reference is not None
        if state == "BUILDING":
            pid = _marker_pid(marker)
            if _pid_alive(pid) or in_use:
                return RuntimeRecord(
                    parent.name,
                    parent,
                    "temporary_in_use",
                    state,
                    False,
                    "build is live",
                    apparent,
                    allocated,
                    files,
                    directories,
                    symlinks,
                    True,
                    manifest_sha,
                )
            created = marker.get("created_at") if marker else None
            age = 0.0
            if isinstance(created, str):
                with contextlib.suppress(ValueError):
                    age = (
                        dt.datetime.now(dt.UTC) - dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
                    ).total_seconds()
            if age < self.stale_after_seconds:
                return RuntimeRecord(
                    parent.name,
                    parent,
                    "temporary_stale",
                    state,
                    False,
                    "stale build grace period",
                    apparent,
                    allocated,
                    files,
                    directories,
                    symlinks,
                    False,
                    manifest_sha,
                )
            safe = True
            reason = "stale managed build"
        elif state == "FAILED":
            safe = not in_use
            reason = "failed review" if safe else "failed review is in use"
        elif valid:
            safe = not in_use
            reason = "unpromoted review" if safe else "review is in use"
        else:
            return RuntimeRecord(
                parent.name,
                parent,
                "unknown",
                state,
                False,
                "missing or ambiguous managed manifest",
                apparent,
                allocated,
                files,
                directories,
                symlinks,
                in_use,
                manifest_sha,
            )
        return RuntimeRecord(
            parent.name,
            parent,
            "review",
            state,
            safe,
            reason,
            apparent,
            allocated,
            files,
            directories,
            symlinks,
            in_use,
            manifest_sha,
        )

    def _record_rollback_unlocked(
        self, path: Path, process_references: Mapping[Path, tuple[int, str]] | None = None
    ) -> RuntimeRecord:
        try:
            info = os.lstat(path)
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise RuntimeLifecycleError("rollback is not a regular directory")
            _validate_confined_tree(path, self.root)
            apparent, allocated, files, directories, symlinks, _ = _tree_stats(path)
        except (OSError, RuntimeLifecycleError) as exc:
            return RuntimeRecord(path.name, path, "unknown", "UNKNOWN", False, str(exc), 0, 0, 0, 0, 0)
        manifest = _safe_json(path / STAGED_MARKER)
        valid = (
            manifest is not None
            and manifest.get("project") == "mtf-lab"
            and manifest.get("promotion_state") == "ROLLBACK"
            and manifest.get("state") == "ROLLBACK_RUNTIME"
        )
        if not valid:
            return RuntimeRecord(
                path.name,
                path,
                "unknown",
                "UNKNOWN",
                False,
                "rollback metadata is missing or ambiguous",
                apparent,
                allocated,
                files,
                directories,
                symlinks,
            )
        reference = (
            process_references.get(path.resolve(strict=False))
            if process_references is not None
            else _proc_reference(path)
        )
        return RuntimeRecord(
            path.name,
            path,
            "rollback",
            "ROLLBACK",
            reference is None,
            "rollback retained" if reference is None else "rollback is in use",
            apparent,
            allocated,
            files,
            directories,
            symlinks,
            reference is not None,
            _sha256(path / STAGED_MARKER),
        )

    def scan_unlocked(self) -> list[RuntimeRecord]:  # noqa: C901 - one locked classification pass
        self._ensure_root()
        records: list[RuntimeRecord] = []
        entries = sorted(self.root.iterdir(), key=lambda item: item.name)
        process_references = _proc_references(
            [item for entry in entries if entry.name.startswith(REVIEW_PREFIX) for item in (entry, entry / "runtime")]
            + [entry for entry in entries if entry.name.startswith(ROLLBACK_PREFIX)]
        )
        for entry in entries:
            if entry.name.startswith("."):
                continue
            if entry.name == ACTIVE_RUNTIME_NAME:
                try:
                    info = os.lstat(entry)
                    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                        raise RuntimeLifecycleError("canonical runtime path is not a regular directory")
                    apparent, allocated, files, directories, symlinks, _ = _tree_stats(entry)
                except (OSError, RuntimeLifecycleError):
                    apparent = allocated = files = directories = symlinks = 0
                records.append(
                    RuntimeRecord(
                        entry.name,
                        entry,
                        "active",
                        "ACTIVE_CANONICAL",
                        False,
                        "canonical runtime is always protected",
                        apparent,
                        allocated,
                        files,
                        directories,
                        symlinks,
                    )
                )
            elif entry.name.startswith(REVIEW_PREFIX):
                try:
                    info = os.lstat(entry)
                except OSError:
                    continue
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    records.append(
                        RuntimeRecord(
                            entry.name,
                            entry,
                            "unknown",
                            "UNKNOWN",
                            False,
                            "review root is not a regular directory",
                            0,
                            0,
                            0,
                            0,
                            0,
                        )
                    )
                else:
                    records.append(self._record_review_unlocked(entry, process_references))
            elif entry.name.startswith(ROLLBACK_PREFIX):
                try:
                    info = os.lstat(entry)
                except OSError:
                    continue
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    records.append(
                        RuntimeRecord(
                            entry.name,
                            entry,
                            "unknown",
                            "UNKNOWN",
                            False,
                            "rollback root is not a regular directory",
                            0,
                            0,
                            0,
                            0,
                            0,
                        )
                    )
                else:
                    records.append(self._record_rollback_unlocked(entry, process_references))
            elif any(entry.name.startswith(prefix) for prefix in BUILD_PREFIXES):
                records.append(
                    RuntimeRecord(
                        entry.name, entry, "unknown", "UNKNOWN", False, "unmarked temporary preserved", 0, 0, 0, 0, 0
                    )
                )
            else:
                records.append(
                    RuntimeRecord(
                        entry.name, entry, "unknown", "UNKNOWN", False, "not an MTF runtime artifact", 0, 0, 0, 0, 0
                    )
                )
        return records

    def inspect_unlocked(self) -> dict[str, Any]:
        records = self.scan_unlocked()
        return {
            "schema": LIFECYCLE_SCHEMA,
            "root": str(self.root),
            "active": str(self.active),
            "records": [record.to_dict(root=self.root) for record in records],
            "totals": {
                "apparent_bytes": sum(record.apparent_bytes for record in records),
                "allocated_bytes": sum(record.allocated_bytes for record in records),
                "runtime_records": len(records),
            },
        }

    def inspect(self) -> dict[str, Any]:
        with self.lock():
            self.recover_unlocked()
            return self.inspect_unlocked()

    def _delete_record_unlocked(self, record: RuntimeRecord, *, purge_review_evidence: bool = False) -> None:
        evidence_only = record.role == "review_evidence"
        if (not record.safe_to_delete and not (evidence_only and purge_review_evidence)) or record.role not in {
            "review",
            "review_evidence",
            "rollback",
        }:
            raise RuntimeLifecycleError(f"refusing to delete {record.name}: {record.reason}")
        if record.in_use or _proc_reference(record.path) is not None:
            raise RuntimeLifecycleError(f"runtime became busy: {record.path}")
        _validate_confined_tree(record.path, self.root)
        if record.role in {"review", "review_evidence"}:
            runtime = record.path / "runtime"
            if record.role == "review" and os.path.lexists(runtime):
                _remove_tree(runtime)
            marker = record.path / LIFECYCLE_MARKER
            if os.path.lexists(marker):
                os.unlink(marker)
            if purge_review_evidence or record.role == "review_evidence":
                # This mode is an explicit, audited migration of legacy
                # review roots.  New lifecycle runs never place evidence next
                # to the payload, so the automatic GC keeps this conservative.
                if record.path.exists():
                    _validate_confined_tree(record.path, self.root)
                    _remove_tree(record.path)
                return
            if not any(record.path.iterdir()):
                os.rmdir(record.path)
        else:
            _remove_tree(record.path)

    def gc_unlocked(
        self,
        *,
        dry_run: bool = False,
        max_reviews: int | None = None,
        max_rollbacks: int | None = None,
        preserve: Sequence[str | Path] = (),
        purge_review_evidence: bool = False,
    ) -> dict[str, Any]:
        records = self.scan_unlocked()
        preserve_paths = {Path(item).expanduser().resolve(strict=False) for item in preserve}
        review_records = sorted(
            (r for r in records if r.role == "review"), key=lambda r: r.path.stat().st_mtime, reverse=True
        )
        rollback_records = sorted(
            (r for r in records if r.role == "rollback"), key=lambda r: r.path.stat().st_mtime, reverse=True
        )
        keep_reviews = max(0, self.keep_reviews if max_reviews is None else int(max_reviews))
        keep_rollbacks = max(0, self.keep_rollbacks if max_rollbacks is None else int(max_rollbacks))
        keep_review_names = {r.name for r in review_records[:keep_reviews]}
        keep_rollback_names = {r.name for r in rollback_records[:keep_rollbacks]}
        deleted: list[dict[str, Any]] = []
        preserved: list[dict[str, Any]] = []
        for record in records:
            if record.role == "review_evidence" and not purge_review_evidence:
                item = record.to_dict(root=self.root)
                item["action"] = "evidence_preserved"
                preserved.append(item)
                continue
            if record.role not in {"review", "review_evidence", "rollback"}:
                item = record.to_dict(root=self.root)
                item["action"] = "preserve"
                preserved.append(item)
                continue
            keep = record.name in (keep_review_names if record.role == "review" else keep_rollback_names)
            if record.path.resolve(strict=False) in preserve_paths:
                keep = True
            if keep or not record.safe_to_delete:
                item = record.to_dict(root=self.root)
                item["action"] = "preserve" if keep else "needs_review"
                preserved.append(item)
                continue
            if dry_run:
                item = record.to_dict(root=self.root)
                item["action"] = "would_delete"
                deleted.append(item)
                continue
            try:
                self._delete_record_unlocked(record, purge_review_evidence=purge_review_evidence)
            except (OSError, RuntimeLifecycleError) as exc:
                item = record.to_dict(root=self.root)
                item["action"] = "needs_review"
                item["error"] = str(exc)
                preserved.append(item)
            else:
                item = record.to_dict(root=self.root)
                item["action"] = "deleted" if not record.path.exists() else "payload_deleted"
                deleted.append(item)
        return {
            "schema": LIFECYCLE_SCHEMA,
            "root": str(self.root),
            "dry_run": dry_run,
            "deleted": deleted,
            "preserved": preserved,
        }

    def gc(self, **kwargs: Any) -> dict[str, Any]:
        with self.lock():
            self.recover_unlocked()
            return self.gc_unlocked(**kwargs)

    def promote(self, staged: str | Path, *, keep_rollback: int = 1) -> dict[str, Any]:  # noqa: C901 - atomic promotion gate
        candidate = Path(staged).expanduser()
        with self.lock():
            self.recover_unlocked()
            candidate = self.validate_build_destination(candidate)
            parent = candidate.parent
            record = self._record_review_unlocked(parent)
            if record.role != "review" or not record.safe_to_delete:
                raise RuntimeLifecycleError(f"staged runtime is not promotable: {record.reason}")
            if not candidate.is_dir() or candidate.resolve(strict=False) == self.active.resolve(strict=False):
                raise RuntimeLifecycleError("staged runtime is not a distinct review")
            rollback = self.root / f"{ROLLBACK_PREFIX}{dt.datetime.now(dt.UTC):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:8]}"
            journal = self.root / ".runtime-promotion.json"
            _atomic_json(
                journal,
                {
                    "schema": LIFECYCLE_SCHEMA,
                    "state": "PREPARED",
                    "active": str(self.active),
                    "staged": str(candidate),
                    "rollback": str(rollback),
                    "created_at": _now(),
                },
            )
            active_moved = False
            staged_moved = False
            try:
                if os.path.lexists(self.active):
                    active_info = os.lstat(self.active)
                    if stat.S_ISLNK(active_info.st_mode) or not stat.S_ISDIR(active_info.st_mode):
                        raise RuntimeLifecycleError("canonical runtime is not a regular directory")
                    _validate_confined_tree(self.active, self.root)
                    os.rename(self.active, rollback)
                    active_moved = True
                    _retarget_venv_configs(rollback)
                os.rename(candidate, self.active)
                staged_moved = True
                _retarget_venv_configs(self.active)
                _atomic_json(
                    journal,
                    {
                        "schema": LIFECYCLE_SCHEMA,
                        "state": "APPLYING",
                        "active": str(self.active),
                        "staged": str(candidate),
                        "rollback": str(rollback),
                        "created_at": _now(),
                    },
                )
                active_manifest = _safe_json(self.active / STAGED_MARKER)
                if active_manifest is None:
                    raise RuntimeLifecycleError("promoted runtime manifest is unreadable")
                active_manifest.update(
                    {
                        "state": "ACTIVE_RUNTIME",
                        "promotion_state": "ACTIVE",
                        "current_pointer": str(self.active),
                        "destination": str(self.active),
                        "promoted_at": _now(),
                    }
                )
                _atomic_json(self.active / STAGED_MARKER, active_manifest)
                if active_moved:
                    rollback_manifest = _safe_json(rollback / STAGED_MARKER)
                    if rollback_manifest is not None:
                        rollback_manifest.update(
                            {
                                "state": "ROLLBACK_RUNTIME",
                                "promotion_state": "ROLLBACK",
                                "rollback_of": str(self.active),
                                "destination": str(rollback),
                                "rollback_created_at": _now(),
                            }
                        )
                        _atomic_json(rollback / STAGED_MARKER, rollback_manifest)
                with contextlib.suppress(FileNotFoundError):
                    (parent / LIFECYCLE_MARKER).unlink()
                if parent.exists() and not any(parent.iterdir()):
                    parent.rmdir()
                journal.unlink()
            except BaseException:
                with contextlib.suppress(OSError):
                    if staged_moved and self.active.exists() and not candidate.exists():
                        os.rename(self.active, candidate)
                        with contextlib.suppress(RuntimeLifecycleError, OSError):
                            _retarget_venv_configs(candidate)
                with contextlib.suppress(OSError):
                    if active_moved and rollback.exists() and not self.active.exists():
                        os.rename(rollback, self.active)
                        with contextlib.suppress(RuntimeLifecycleError, OSError):
                            _retarget_venv_configs(self.active)
                raise
            self.gc_unlocked(max_reviews=0, max_rollbacks=keep_rollback, preserve=[self.active])
            result = {
                "schema": LIFECYCLE_SCHEMA,
                "state": "PROMOTED",
                "active": str(self.active),
                "rollback": str(rollback) if active_moved and rollback.exists() else None,
            }
            return result


__all__ = ["DEFAULT_RUNTIME_ROOT", "RuntimeLifecycle", "RuntimeLifecycleError", "RuntimeRecord", "LIFECYCLE_SCHEMA"]
