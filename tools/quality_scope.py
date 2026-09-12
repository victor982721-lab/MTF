"""Canonical Python-file scope for the local MTF Lab quality gates.

The source tree is intentionally discovered at gate time instead of keeping a
second hand-maintained list in every runner.  Both Ruff and format inspect the
complete ``mtf_lab``/``tests``/``tools`` tree; mypy inspects the complete
``mtf_lab``/``tools`` tree.  A missing or empty root is a gate error, never a
reason to silently shrink the command line.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

RUFF_ROOTS: tuple[str, ...] = ("mtf_lab", "tests", "tools")
MYPY_ROOTS: tuple[str, ...] = ("mtf_lab", "tools")


def _discover_files(root: Path, roots: tuple[str, ...]) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    files: list[str] = []
    missing_roots: list[str] = []
    empty_roots: list[str] = []
    for name in roots:
        directory = root / name
        if not directory.is_dir():
            missing_roots.append(name)
            continue
        discovered = sorted(path.relative_to(root).as_posix() for path in directory.rglob("*.py") if path.is_file())
        if not discovered:
            empty_roots.append(name)
        files.extend(discovered)
    return tuple(sorted(set(files))), tuple(sorted(set(missing_roots))), tuple(sorted(set(empty_roots)))


@dataclass(frozen=True)
class QualityScope:
    """One immutable snapshot of every Python file a gate must inspect."""

    ruff_roots: tuple[str, ...]
    mypy_roots: tuple[str, ...]
    ruff_files: tuple[str, ...]
    mypy_files: tuple[str, ...]
    missing_roots: tuple[str, ...]
    empty_roots: tuple[str, ...]
    identity_sha256: str

    @property
    def ok(self) -> bool:
        return not self.missing_roots and not self.empty_roots and bool(self.ruff_files) and bool(self.mypy_files)

    def missing_files(self, root: Path) -> tuple[str, ...]:
        """Return snapshot files that disappeared before command execution."""

        return tuple(
            path for path in sorted(set(self.ruff_files) | set(self.mypy_files)) if not (root / path).is_file()
        )

    def as_dict(self, root: Path | None = None) -> dict[str, Any]:
        missing_files = self.missing_files(root) if root is not None else ()
        return {
            "ruff_roots": list(self.ruff_roots),
            "mypy_roots": list(self.mypy_roots),
            "ruff_files": list(self.ruff_files),
            "mypy_files": list(self.mypy_files),
            "ruff_file_count": len(self.ruff_files),
            "mypy_file_count": len(self.mypy_files),
            "missing_roots": list(self.missing_roots),
            "empty_roots": list(self.empty_roots),
            "missing_files": list(missing_files),
            "identity_sha256": self.identity_sha256,
            "ok": self.ok and not missing_files,
        }


def discover_quality_scope(root: Path) -> QualityScope:
    """Discover the complete maintained quality scope below ``root``."""

    root_path = root.resolve()
    ruff_files, ruff_missing, ruff_empty = _discover_files(root_path, RUFF_ROOTS)
    mypy_files, mypy_missing, mypy_empty = _discover_files(root_path, MYPY_ROOTS)
    missing_roots = tuple(sorted(set(ruff_missing) | set(mypy_missing)))
    empty_roots = tuple(sorted(set(ruff_empty) | set(mypy_empty)))
    identity_value = {
        "ruff_roots": RUFF_ROOTS,
        "mypy_roots": MYPY_ROOTS,
        "ruff_files": ruff_files,
        "mypy_files": mypy_files,
        "missing_roots": missing_roots,
        "empty_roots": empty_roots,
    }
    identity_sha256 = hashlib.sha256(
        json.dumps(identity_value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return QualityScope(
        ruff_roots=RUFF_ROOTS,
        mypy_roots=MYPY_ROOTS,
        ruff_files=ruff_files,
        mypy_files=mypy_files,
        missing_roots=missing_roots,
        empty_roots=empty_roots,
        identity_sha256=identity_sha256,
    )


__all__ = ["MYPY_ROOTS", "RUFF_ROOTS", "QualityScope", "discover_quality_scope"]
