"""Conservative, local dependency gates; never query an advisory service."""

from __future__ import annotations

import sqlite3
from typing import Any

SQLITE_WAL_REFERENCE = "https://sqlite.org/wal.html#the_wal_reset_bug"


def sqlite_wal_readiness(version: str | None = None) -> dict[str, Any]:
    """Upstream versions establish a fix; vendor backports need separate proof.

    A negative result means the patch is not demonstrated, not that a database
    is corrupt. This helper never opens a database or changes the host.
    """
    selected = sqlite3.sqlite_version if version is None else version
    try:
        parts = tuple(int(part) for part in selected.split("."))
        if len(parts) != 3 or any(part < 0 for part in parts):
            raise ValueError("invalid version")
    except (ValueError, AttributeError):
        parts = ()
    fixed = bool(parts) and (
        parts >= (3, 51, 3) or ((3, 44, 6) <= parts < (3, 45, 0)) or ((3, 50, 7) <= parts < (3, 51, 0))
    )
    return {
        "version": selected,
        "external_continuous_ready": fixed,
        "wal_reset_patch": "UPSTREAM_FIXED" if fixed else "VENDOR_BACKPORT_NOT_VERIFIED",
        "reason": None if fixed else "SQLITE_WAL_PATCH_NOT_VERIFIED",
        "reference": SQLITE_WAL_REFERENCE,
        "network_performed": False,
        "database_opened": False,
    }
