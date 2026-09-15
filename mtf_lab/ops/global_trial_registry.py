"""Durable append-only registry for every market-research attempt.

The registry records historical, QA, forward, and market-labelled attempts;
it never contacts a market, rewrites a prior line, or derives evidence from an
unregistered run.  It is intentionally a small local JSONL log with a hash
chain and an inter-process CAS lock.
"""

from __future__ import annotations

import fcntl
import json
import os
import stat
import threading
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..core.canonical import canonical_json, fingerprint, instant_text

REGISTRY_SCHEMA = "mtf-lab.global-trial-registry.v1"
_REPOSITORY = Path(__file__).resolve().parents[2]
_ALLOWED_MODES = {"HISTORICAL", "QA", "FORWARD", "MARKET"}
_ALLOWED_STATUSES = {"REGISTERED", "RUNNING", "COMPLETED", "FAILED", "ABANDONED", "INSUFFICIENT"}
_SECRET_KEYS = {"token", "access_token", "refresh_token", "client_secret", "secret", "password"}
_PROCESS_LOCK = threading.RLock()


class RegistryError(ValueError):
    """Registry input, identity, or persistence error."""


class RegistryConflict(RegistryError):
    """The caller's expected registry revision is stale."""


def _safe_path(path: str | Path) -> Path:
    target = Path(path).expanduser()
    if not target.is_absolute():
        raise RegistryError("registry_path debe ser absoluto")
    target = Path(os.path.abspath(os.fspath(target)))
    if not target.name:
        raise RegistryError("registry_path debe apuntar a un archivo")
    try:
        target.relative_to(_REPOSITORY)
    except ValueError:
        return target
    raise RegistryError("el registry debe estar fuera del checkout")


def _check_parent(target: Path) -> None:
    current = Path(target.anchor)
    for part in target.parent.parts:
        if part == target.anchor:
            continue
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            break
        except OSError as exc:
            raise RegistryError(f"padre de registry ilegible: {current}") from exc
        if stat.S_ISLNK(info.st_mode):
            raise RegistryError(f"padre de registry no puede ser symlink: {current}")
        if not stat.S_ISDIR(info.st_mode):
            raise RegistryError(f"padre de registry no es directorio: {current}")


def _validate_regular(info: os.stat_result, path: Path) -> None:
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise RegistryError(f"registry debe ser archivo regular: {path}")
    if info.st_nlink != 1:
        raise RegistryError(f"registry no puede ser hardlink compartido: {path}")


def _ensure_file(target: Path) -> None:
    _check_parent(target)
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        info = os.lstat(target)
    except FileNotFoundError:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(target, flags, 0o600)
        try:
            os.fchmod(fd, 0o600)
            os.fsync(fd)
        finally:
            os.close(fd)
        parent_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return
    _validate_regular(info, target)
    os.chmod(target, 0o600)


def _lock_path(target: Path) -> Path:
    return target.with_name(f".{target.name}.lock")


def _ensure_lock(target: Path) -> Path:
    lock = _lock_path(target)
    try:
        info = os.lstat(lock)
    except FileNotFoundError:
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(lock, flags, 0o600)
        os.close(fd)
        return lock
    _validate_regular(info, lock)
    return lock


def _json_safe(value: Any, *, key: str = "") -> Any:
    if key.lower() in _SECRET_KEYS or any(secret in key.lower() for secret in ("token", "secret", "password")):
        raise RegistryError(f"campo sensible no permitido en registry: {key}")
    if isinstance(value, Mapping):
        return {str(name): _json_safe(item, key=str(name)) for name, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item, key=key) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (datetime, Path)):
        return instant_text(value) if isinstance(value, datetime) else str(value)
    raise RegistryError(f"valor no serializable en registry: {type(value).__name__}")


def _record_hash(record: Mapping[str, Any]) -> str:
    material = dict(record)
    material.pop("record_hash", None)
    return str(fingerprint(material))


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


class GlobalTrialRegistry:
    """Append-only local registry with deterministic revision/CAS semantics."""

    def __init__(self, path: str | Path) -> None:
        self.path = _safe_path(path)
        _ensure_file(self.path)
        self.lock_path = _ensure_lock(self.path)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        with _PROCESS_LOCK:
            fd = os.open(self.lock_path, os.O_RDWR | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0))
            try:
                info = os.fstat(fd)
                _validate_regular(info, self.lock_path)
                fcntl.flock(fd, fcntl.LOCK_EX)
                yield
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _read_records(self) -> list[dict[str, Any]]:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
        fd = os.open(self.path, flags)
        try:
            info = os.fstat(fd)
            _validate_regular(info, self.path)
            with os.fdopen(fd, "r", encoding="utf-8") as stream:
                lines = stream.readlines()
            fd = -1
        finally:
            if fd >= 0:
                with suppress(OSError):
                    os.close(fd)
        records: list[dict[str, Any]] = []
        previous_hash: str | None = None
        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                raise RegistryError(f"línea vacía en registry: {line_number}")
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RegistryError(f"JSON inválido en registry: línea {line_number}") from exc
            if not isinstance(raw, Mapping):
                raise RegistryError(f"registro no es objeto: línea {line_number}")
            record = dict(raw)
            revision = record.get("registry_revision")
            if not isinstance(revision, int) or isinstance(revision, bool) or revision != line_number:
                raise RegistryError(f"revisión inválida en registry: línea {line_number}")
            if record.get("previous_record_hash") != previous_hash:
                raise RegistryError(f"cadena de registry rota: línea {line_number}")
            expected = record.get("record_hash")
            if not isinstance(expected, str) or expected != _record_hash(record):
                raise RegistryError(f"hash de registry inválido: línea {line_number}")
            records.append(record)
            previous_hash = expected
        return records

    def records(self) -> tuple[dict[str, Any], ...]:
        with self._locked():
            return tuple(self._read_records())

    @property
    def revision(self) -> int:
        return len(self.records())

    @property
    def registry_hash(self) -> str:
        return str(fingerprint(list(self.records())))

    def append(self, record: Mapping[str, Any], *, expected_revision: int | None = None) -> dict[str, Any]:
        if not isinstance(record, Mapping):
            raise RegistryError("record debe ser mapping")
        material = _json_safe(dict(record))
        if not isinstance(material, dict):
            raise RegistryError("record no se pudo normalizar")
        if "record_hash" in material or "registry_revision" in material or "previous_record_hash" in material:
            raise RegistryError("campos de cadena son reservados")
        with self._locked():
            records = self._read_records()
            current_revision = len(records)
            if expected_revision is not None and expected_revision != current_revision:
                raise RegistryConflict(f"registry_revision esperada={expected_revision}, actual={current_revision}")
            previous_hash = records[-1]["record_hash"] if records else None
            material["schema"] = REGISTRY_SCHEMA
            material["registry_revision"] = current_revision + 1
            material["previous_record_hash"] = previous_hash
            material["record_hash"] = _record_hash(material)
            line = (canonical_json(material) + "\n").encode("utf-8")
            flags = os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
            fd = os.open(self.path, flags)
            try:
                _validate_regular(os.fstat(fd), self.path)
                offset = 0
                while offset < len(line):
                    offset += os.write(fd, line[offset:])
                os.fsync(fd)
            finally:
                os.close(fd)
            parent_fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
            return dict(material)

    def register_attempt(
        self,
        *,
        candidate_id: str,
        protocol_hash: str,
        dataset_hash: str,
        runtime_identity: Mapping[str, Any],
        data_identity: Mapping[str, Any] | None = None,
        scope: Mapping[str, Any] | None = None,
        mode: str = "HISTORICAL",
        parameters: Mapping[str, Any] | None = None,
        attempt_id: str | None = None,
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        normalized_mode = str(mode).strip().upper()
        if normalized_mode not in _ALLOWED_MODES:
            raise RegistryError(f"mode de registry no soportado: {mode!r}")
        identifier = str(attempt_id or uuid.uuid4().hex).strip()
        if not identifier:
            raise RegistryError("attempt_id no puede estar vacío")
        return self.append(
            {
                "event": "ATTEMPT_REGISTERED",
                "attempt_id": identifier,
                "status": "REGISTERED",
                "mode": normalized_mode,
                "candidate_id": str(candidate_id),
                "protocol_hash": str(protocol_hash),
                "dataset_hash": str(dataset_hash),
                "runtime_identity": dict(runtime_identity),
                "data_identity": dict(data_identity or {}),
                "scope": dict(scope or {}),
                "parameters": dict(parameters or {}),
                "registered_at": _now(),
            },
            expected_revision=expected_revision,
        )

    register = register_attempt

    def update_status(
        self,
        attempt_id: str,
        status: str,
        *,
        details: Mapping[str, Any] | None = None,
        expected_revision: int | None = None,
    ) -> dict[str, Any]:
        normalized_status = str(status).strip().upper()
        if normalized_status not in _ALLOWED_STATUSES:
            raise RegistryError(f"status de registry no soportado: {status!r}")
        records = self.records()
        known = [item for item in records if item.get("attempt_id") == attempt_id]
        if not known:
            raise RegistryError(f"attempt_id no registrado: {attempt_id}")
        return self.append(
            {
                "event": "ATTEMPT_STATUS",
                "attempt_id": str(attempt_id),
                "status": normalized_status,
                "details": dict(details or {}),
                "updated_at": _now(),
            },
            expected_revision=expected_revision,
        )

    close_attempt = update_status

    def validate(self) -> dict[str, Any]:
        try:
            records = self.records()
        except RegistryError as exc:
            return {"ok": False, "state": "INVALID", "errors": [str(exc)], "revision": None}
        errors: list[str] = []
        attempts: dict[str, str] = {}
        for record in records:
            attempt_id = record.get("attempt_id")
            status = record.get("status")
            if not isinstance(attempt_id, str) or not attempt_id:
                errors.append("attempt_id_missing")
                continue
            if status not in _ALLOWED_STATUSES:
                errors.append(f"status_invalid:{attempt_id}")
            if record.get("event") == "ATTEMPT_REGISTERED":
                if attempt_id in attempts:
                    errors.append(f"attempt_id_duplicate:{attempt_id}")
                attempts[attempt_id] = str(status)
            elif attempt_id not in attempts:
                errors.append(f"status_without_registration:{attempt_id}")
            else:
                attempts[attempt_id] = str(status)
        return {
            "ok": not errors,
            "state": "VALID" if not errors else "INVALID",
            "errors": errors,
            "revision": len(records),
            "record_count": len(records),
            "attempt_count": len(attempts),
            "registry_hash": fingerprint(list(records)),
        }


__all__ = [
    "GlobalTrialRegistry",
    "REGISTRY_SCHEMA",
    "RegistryConflict",
    "RegistryError",
]
