"""Read bounded supervisor evidence without opening its working database."""

from __future__ import annotations

import json
import math
import os
import re
import stat
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_MAX_BYTES = 128 * 1024


def process_identity(pid: int | None = None) -> dict[str, Any]:
    """Linux identity includes start ticks and boot ID, not only a reusable PID."""
    selected = os.getpid() if pid is None else pid
    if isinstance(selected, bool) or not isinstance(selected, int) or selected < 1:
        raise ValueError("invalid process id")
    fields = Path(f"/proc/{selected}/stat").read_text().rpartition(")")[2].split()
    if len(fields) < 20 or fields[0] in {"Z", "X"}:
        raise ValueError("process is not live")
    return {
        "pid": selected,
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "process_start_ticks": fields[19],
    }


def _blocked(reason: str) -> dict[str, Any]:
    return {"schema_version": 1, "ready": False, "liveness": "UNVERIFIED", "reasons": [reason]}


def _read_private(path: Path) -> Mapping[str, Any]:
    if any(item.is_symlink() for item in (path, *path.parents)):
        raise ValueError("unsafe state path")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ValueError("unsafe state file")
        raw = handle.read(_MAX_BYTES + 1)
    if len(raw) > _MAX_BYTES:
        raise ValueError("oversize state")
    value = json.loads(raw)
    if not isinstance(value, Mapping):
        raise ValueError("invalid state")
    return value


def _verify_identity(proof: Mapping[str, Any], now: datetime) -> None:
    observed = process_identity(proof.get("pid"))
    if any(str(proof.get(key)) != str(value) for key, value in observed.items()):
        raise ValueError("process identity changed")
    published = datetime.fromisoformat(str(proof.get("published_at", "")).replace("Z", "+00:00"))
    if published.tzinfo is None:
        raise ValueError("naive timestamp")
    ttl = float(proof.get("valid_for_seconds", 0))
    age = (now - published).total_seconds()
    if not math.isfinite(ttl) or not 0 < ttl <= 60 or not 0 <= age <= ttl:
        raise ValueError("stale state")


def _public_operational(value: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for name in (
        "connection_state",
        "feed_state",
        "freshness_state",
        "reconciliation_state",
        "exposure_state",
        "protection_state",
        "economic_state",
    ):
        raw = value.get(name)
        output[name] = raw if isinstance(raw, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,59}", raw) else "UNKNOWN"
    for name in ("backlog", "connection_generation"):
        raw = value.get(name)
        output[name] = raw if type(raw) is int and 0 <= raw < 2**63 else None
    output["execution_enabled"] = value.get("execution_enabled") is True and value.get("mode") == "demo"
    return output


def read_supervisor_readiness(path: str | Path | None, *, now: datetime | None = None) -> dict[str, Any]:
    if path is None:
        return _blocked("NO_RUNTIME_EVIDENCE")
    try:
        value = _read_private(Path(path).expanduser().absolute())
        if value.get("schema_version") != 1 or value.get("mode") not in {"observe", "shadow", "demo"}:
            return _blocked("INVALID_RUNTIME_SCHEMA_OR_MODE")
        proof, readiness = value.get("runtime_identity"), value.get("readiness")
        if not isinstance(proof, Mapping) or not isinstance(readiness, Mapping):
            return _blocked("NO_RUNTIME_IDENTITY")
        _verify_identity(proof, now or datetime.now(UTC))
        raw_reasons = readiness.get("reasons", readiness.get("blocked_reasons", []))
        if not isinstance(raw_reasons, list):
            return _blocked("INVALID_READINESS")
        reasons = [
            item if isinstance(item, str) and re.fullmatch(r"[A-Z][A-Z0-9_:]{0,79}", item) else "UNRECOGNIZED_REASON"
            for item in raw_reasons[:32]
        ]
        return {
            "schema_version": 1,
            "ready": readiness.get("ready") is True and not reasons,
            "liveness": "VERIFIED",
            "reasons": reasons,
            "mode": value["mode"],
            "operational": _public_operational(value),
        }
    except (OSError, ValueError, TypeError, OverflowError):
        return _blocked("UNVERIFIED_OR_STALE_RUNTIME")
