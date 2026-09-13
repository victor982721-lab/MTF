#!/usr/bin/env python3
"""Run and record MTF Lab's reproducible, credential-free delivery gates.

The command is intentionally a *local* delivery gate.  It never performs an
OAuth flow, contacts a broker, discovers a real account, or sends an order.
All subprocesses receive an allow-listed environment and temporary HOME/XDG
directories.  The optional report writer stores portable, sanitised JSON; it
does not include the report files in their own source fingerprint.

The default command prints a diagnostic bundle and exits non-zero when a gate
is missing or fails.  Writing the canonical reports is explicit::

    .venv/bin/python tools/verify_offline.py --write-reports

Use explicit ``--runtime-python``, ``--dev-python`` and ``--node`` paths in
automation when the checkout is not using the conventional ``.venv`` and
``.venv-dev`` directories.  No dependency installation is attempted here.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from urllib.parse import unquote

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.quality_scope import QualityScope, discover_quality_scope

BASE_COMMIT = "ea288085690ccfc617e6bcd5ba44f20925633f8f"
REPORT_DIRECTORY = Path("reports/engineering/latest")
RESULTS_FILENAME = "engineering_results.json"
TOOLING_FILENAME = "engineering_tooling.json"
DEFAULT_TIMEOUT = 900.0
DEFAULT_BENCHMARK_EVENTS = 7500

RUNTIME_DISTRIBUTIONS: tuple[str, ...] = (
    "mtf-lab",
    "protobuf",
    "pip",
)
DEV_DISTRIBUTIONS: tuple[str, ...] = ("ruff", "mypy", "pip")

_SAFE_ENV_NAMES = frozenset(
    {
        "CI",
        "COLORTERM",
        "FORCE_COLOR",
        "LANG",
        "NO_COLOR",
        "PATH",
        "PYTHONIOENCODING",
        "PYTHONUTF8",
        "PYTHONHASHSEED",
        "TERM",
        "TZ",
        # These are non-secret delivery controls consumed by the UI tests.
        "MTF_NODE_BIN",
        "MTF_UI_JS_DEV",
    }
)
_SENSITIVE_KEY = re.compile(
    r"(?i)(?:access[_ -]?token|refresh[_ -]?token|client[_ -]?secret|secret|password|passwd|credential|cookie|authorization|oauth[_ -]?code)"
)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(\b(?:access[_ -]?token|refresh[_ -]?token|client[_ -]?secret|secret|password|passwd|credential|cookie|authorization|oauth[_ -]?code)\b\s*[:=]\s*)([\"']?)[^\s,;\"']+\2"
)
_BEARER_VALUE = re.compile(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/=-]+")
_ABSOLUTE_PATH = re.compile(
    r"(?<![A-Za-z0-9:/])/(?:home|tmp|workspace|opt|usr|var|mnt|root|run|etc)/(?:[^\s'\"`,;\)\]]*)"
)
_FAILURE_IDENTIFIER = re.compile(r"(?m)^(?:FAIL|ERROR):\s+(.+?)\s*$")
_SKIP_DIRECTORY_NAMES = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        ".venv-dev",
        "__pycache__",
        "logs",
    }
)


@dataclass(frozen=True)
class _CommandOutcome:
    """A compact command receipt before it is converted to JSON."""

    command: tuple[str, ...]
    returncode: int | None
    stdout: str
    stderr: str
    timed_out: bool
    duration_seconds: float

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out

    def to_dict(self, *, root: Path, temporary_roots: Iterable[Path] = ()) -> dict[str, Any]:
        return {
            "command": [_portable_text(item, root=root, temporary_roots=temporary_roots) for item in self.command],
            "returncode": self.returncode,
            "ok": self.ok,
            "timed_out": self.timed_out,
            "duration_seconds": round(self.duration_seconds, 3),
            "stdout_tail": _sanitize_text(self.stdout, root=root, temporary_roots=temporary_roots),
            "stderr_tail": _sanitize_text(self.stderr, root=root, temporary_roots=temporary_roots),
        }


def _tail(value: str | None, limit: int = 3000) -> str:
    text = value or ""
    return text if len(text) <= limit else text[-limit:]


def _sanitize_text(value: str, *, root: Path | None = None, temporary_roots: Iterable[Path] = ()) -> str:
    """Remove secrets and replace paths without pretending a result passed."""

    text = str(value)
    replacements: list[tuple[str, str]] = []
    if root is not None:
        with suppress(OSError):
            replacements.append((str(root.resolve()), "<repo>"))
    for temporary in temporary_roots:
        with suppress(OSError):
            replacements.append((str(temporary.resolve()), "<temporary>"))
    with suppress(OSError):
        replacements.append((str(Path.home().resolve()), "<home>"))
    for source, target in sorted(replacements, key=lambda pair: len(pair[0]), reverse=True):
        text = text.replace(source, target)
    text = _BEARER_VALUE.sub(r"\1<redacted>", text)
    text = _SECRET_ASSIGNMENT.sub(r"\1<redacted>", text)
    text = _ABSOLUTE_PATH.sub("<absolute-path>", text)
    return text


def _portable_text(value: Any, *, root: Path | None = None, temporary_roots: Iterable[Path] = ()) -> str:
    raw = os.fspath(value)
    if root is not None and os.path.isabs(raw):
        with suppress(ValueError):
            return Path(raw).relative_to(root).as_posix() or "."
    text = _sanitize_text(raw, root=root, temporary_roots=temporary_roots)
    # An externally selected interpreter/node path is an execution detail, not
    # a durable location.  Keep commands portable even when it is absolute.
    if text.startswith("/"):
        return "<external-path>"
    return text


def sanitize_payload(
    value: Any,
    *,
    root: Path | None = None,
    temporary_roots: Iterable[Path] = (),
    key: str | None = None,
) -> Any:
    """Return JSON-safe metadata with key-aware secret and path redaction.

    This function is intentionally public for focused regression tests and for
    callers that need to sanitize a command/tool receipt before persisting it.
    It is not a claim that arbitrary untrusted output is safe to publish; the
    gate only feeds allow-listed structured values through this function.
    """

    if key is not None and _SENSITIVE_KEY.search(key):
        # A false/empty policy marker such as ``credentials_used: false`` is
        # safe and useful evidence; redact only an actual secret value.
        if value is None or value is False or value == "" or value == []:
            return value
        return "<redacted>"
    if isinstance(value, Mapping):
        return {
            str(name): sanitize_payload(
                item,
                root=root,
                temporary_roots=temporary_roots,
                key=str(name),
            )
            for name, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [sanitize_payload(item, root=root, temporary_roots=temporary_roots) for item in value]
    if isinstance(value, Path):
        return _portable_text(value, root=root, temporary_roots=temporary_roots)
    if value is None or isinstance(value, (str, int, float, bool)):
        return _sanitize_text(value, root=root, temporary_roots=temporary_roots) if isinstance(value, str) else value
    return _sanitize_text(repr(value), root=root, temporary_roots=temporary_roots)


def safe_environment(source: Mapping[str, str] | None = None) -> dict[str, str]:
    """Copy only benign process controls; never inherit login material."""

    current = os.environ if source is None else source
    result: dict[str, str] = {}
    for name, value in current.items():
        if name in _SAFE_ENV_NAMES or name.startswith("LC_"):
            result[name] = value
    result.setdefault("PATH", os.defpath)
    return result


def _command_environment(temp_root: Path, extras: Mapping[str, str] | None = None) -> dict[str, str]:
    directories = {
        "HOME": temp_root / "home",
        "XDG_CONFIG_HOME": temp_root / "config",
        "XDG_CACHE_HOME": temp_root / "cache",
        "XDG_DATA_HOME": temp_root / "data",
        "XDG_STATE_HOME": temp_root / "state",
        "MTF_LAB_STATE_DIR": temp_root / "state",
        "TMPDIR": temp_root / "tmp",
        "MTF_LAB_DB": temp_root / "data" / "mtf_lab.sqlite3",
        "PYTHONPYCACHEPREFIX": temp_root / "pycache",
    }
    for directory in directories.values():
        directory.parent.mkdir(parents=True, exist_ok=True)
        if directory.suffix == "" or directory.name in {"home", "config", "cache", "data", "state", "tmp", "pycache"}:
            directory.mkdir(parents=True, exist_ok=True)
    env = safe_environment()
    env.update({name: str(path) for name, path in directories.items()})
    env.update(
        {
            "MTF_LAB_OFFLINE": "1",
            "PIP_CONFIG_FILE": os.devnull,
            "PIP_DISABLE_PIP_VERSION_CHECK": "1",
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    if extras:
        for name, value in extras.items():
            # Extra controls are selected by this tool, not copied wholesale
            # from the ambient environment.  Secret-shaped names are refused.
            if _SENSITIVE_KEY.search(name):
                continue
            env[name] = value
    return env


def run_command(
    command: Sequence[str | os.PathLike[str]],
    *,
    root: Path,
    environment: Mapping[str, str],
    timeout: float,
) -> _CommandOutcome:
    """Run one bounded command without a shell or inherited stdin."""

    argv = tuple(os.fspath(item) for item in command)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            list(argv),
            cwd=str(root),
            env=dict(environment),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout.decode(errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = exc.stderr.decode(errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        return _CommandOutcome(argv, None, _tail(stdout), _tail(stderr), True, time.monotonic() - started)
    except OSError as exc:
        return _CommandOutcome(
            argv,
            None,
            "",
            f"{type(exc).__name__}: command could not start",
            False,
            time.monotonic() - started,
        )
    return _CommandOutcome(
        argv,
        completed.returncode,
        _tail(completed.stdout),
        _tail(completed.stderr),
        False,
        time.monotonic() - started,
    )


def _git_output(root: Path, arguments: Sequence[str]) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=str(root),
            env=safe_environment(),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    value = completed.stdout.strip()
    return value or None


def git_snapshot(root: Path) -> dict[str, Any]:
    """Collect read-only git identity without contacting a remote."""

    status = _git_output(root, ["status", "--short"])
    status_paths = [] if status is None else [line[:3].strip() + line[3:] for line in status.splitlines()]
    return {
        "base_commit": BASE_COMMIT,
        "head": _git_output(root, ["rev-parse", "HEAD"]),
        "branch": _git_output(root, ["branch", "--show-current"]),
        "main": _git_output(root, ["rev-parse", "--verify", "main"]),
        "origin_main": _git_output(root, ["rev-parse", "--verify", "origin/main"]),
        "head_tree_hash": _git_output(root, ["rev-parse", "--verify", "HEAD^{tree}"]),
        "status_short": status_paths,
        "dirty": bool(status_paths),
        "remote_contacted": False,
    }


def _excluded_source_path(relative: Path) -> bool:
    parts = relative.parts
    if any(part in _SKIP_DIRECTORY_NAMES for part in parts):
        return True
    # ``runtime/`` at the repository root is generated state;
    # ``mtf_lab/runtime/`` is production source and must be fingerprinted.
    if parts and parts[0] == "runtime":
        return True
    # Only the two self-referential latest outputs are excluded.  The human
    # report and history are inputs to the final delivery fingerprint.
    output_paths = {
        REPORT_DIRECTORY / RESULTS_FILENAME,
        REPORT_DIRECTORY / TOOLING_FILENAME,
        Path(str(REPORT_DIRECTORY / RESULTS_FILENAME) + ".tmp"),
        Path(str(REPORT_DIRECTORY / TOOLING_FILENAME) + ".tmp"),
    }
    if relative in output_paths:
        return True
    if relative.as_posix().startswith("reports/generated/"):
        return True
    return relative.name.endswith((".pyc", ".pyo", ".sqlite", ".sqlite3", ".db"))


def _source_paths(root: Path) -> list[Path]:
    listed = _git_output(root, ["ls-files", "--cached", "--others", "--exclude-standard", "-z"])
    if listed is not None:
        # Git's -z output is safe for spaces and other ordinary path bytes;
        # Python's Path accepts the UTF-8 repository path convention here.
        candidates = [root / item for item in listed.split("\0") if item]
    else:
        candidates = [item for item in root.rglob("*") if item.is_file()]
    paths: list[Path] = []
    for path in candidates:
        try:
            relative = path.relative_to(root)
        except ValueError:
            continue
        if _excluded_source_path(relative) or path.is_symlink() or not path.is_file():
            continue
        paths.append(path)
    return sorted(set(paths), key=lambda item: item.relative_to(root).as_posix())


def source_manifest(root: Path) -> dict[str, Any]:
    """Hash the reproducible source view, excluding generated receipts."""

    files: dict[str, str] = {}
    for path in _source_paths(root):
        relative = path.relative_to(root).as_posix()
        digest = hashlib.sha256()
        try:
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(block)
        except OSError as exc:
            raise RuntimeError(f"unable to hash source file: {relative}") from exc
        files[relative] = digest.hexdigest()
    canonical = json.dumps(files, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    content_hash = hashlib.sha256(canonical).hexdigest()
    return {
        "file_count": len(files),
        "files": files,
        "manifest_algorithm": "sha256(path -> bytes)",
        "manifest_sha256": content_hash,
        "content_sha256": content_hash,
    }


def _manifest_delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    old = before.get("files", {}) if isinstance(before.get("files"), Mapping) else {}
    new = after.get("files", {}) if isinstance(after.get("files"), Mapping) else {}
    old_files = {str(key): str(value) for key, value in old.items()}
    new_files = {str(key): str(value) for key, value in new.items()}
    return {
        "before_content_sha256": before.get("content_sha256"),
        "after_content_sha256": after.get("content_sha256"),
        "added": sorted(set(new_files) - set(old_files)),
        "removed": sorted(set(old_files) - set(new_files)),
        "changed": sorted(key for key in set(old_files) & set(new_files) if old_files[key] != new_files[key]),
        "ok": old_files == new_files,
    }


def _last_json(text: str) -> Any:
    for line in reversed((text or "").splitlines()):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


def _version_probe(
    runtime_python: Path,
    *,
    root: Path,
    environment: Mapping[str, str],
    timeout: float,
    distribution_names: Sequence[str] = RUNTIME_DISTRIBUTIONS,
) -> dict[str, Any]:
    names_literal = repr(tuple(distribution_names))
    code = """
import importlib.metadata as metadata
import json
import platform
import sqlite3

names = __NAMES__
versions = {}
for name in names:
    try:
        versions[name] = metadata.version(name)
    except metadata.PackageNotFoundError:
        versions[name] = None
print(json.dumps({
    "implementation": platform.python_implementation(),
    "python_version": platform.python_version(),
    "sqlite": sqlite3.sqlite_version,
    "distributions": versions,
}, sort_keys=True))
"""
    code = code.replace("__NAMES__", names_literal)
    outcome = run_command([str(runtime_python), "-c", code], root=root, environment=environment, timeout=timeout)
    result = outcome.to_dict(root=root)
    parsed = _last_json(outcome.stdout)
    result["metadata"] = parsed if isinstance(parsed, Mapping) else None
    return result


def _probe_metadata_value(probe: Mapping[str, Any], key: str, default: Any = None) -> Any:
    metadata = probe.get("metadata")
    return metadata.get(key, default) if isinstance(metadata, Mapping) else default


def site_packages_path(
    python: Path,
    *,
    root: Path,
    environment: Mapping[str, str],
    timeout: float,
) -> dict[str, Any]:
    """Resolve the selected interpreter's import root without ambient state."""

    code = "import json, site; print(json.dumps(site.getsitepackages()))"
    outcome = run_command([str(python), "-c", code], root=root, environment=environment, timeout=timeout)
    parsed = _last_json(outcome.stdout)
    candidates = parsed if isinstance(parsed, list) else []
    selected = next(
        (Path(item) for item in candidates if isinstance(item, str) and Path(item).is_dir()),
        None,
    )
    result = outcome.to_dict(root=root)
    result.update(
        {
            "available": selected is not None,
            "path": str(selected) if selected is not None else None,
            "candidates": [item for item in candidates if isinstance(item, str)],
        }
    )
    return result


def _node_version(node: Path | None, *, root: Path, environment: Mapping[str, str], timeout: float) -> dict[str, Any]:
    if node is None:
        return {"path_available": False, "version": None, "ok": False, "reason": "node-not-found"}
    if not node.is_file() or not os.access(node, os.X_OK):
        return {"path_available": False, "version": None, "ok": False, "reason": "node-not-executable"}
    outcome = run_command([str(node), "--version"], root=root, environment=environment, timeout=timeout)
    result = outcome.to_dict(root=root)
    version = outcome.stdout.strip().splitlines()[-1] if outcome.stdout.strip() else None
    result.update({"path_available": True, "version": _sanitize_text(version or "", root=root) or None})
    return result


def _compact_counts(summary: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(summary, Mapping):
        return {
            "discovered": None,
            "tests_run": None,
            "executed": None,
            "passed": None,
            "failed": None,
            "skipped": None,
            "expected_failures": None,
            "unexpected_successes": None,
            "consistent": False,
        }

    def number(name: str) -> int | None:
        value = summary.get(name)
        return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None

    discovered = number("discovered")
    tests_run = number("tests_run")
    skipped = number("skipped")
    executed = tests_run - skipped if tests_run is not None and skipped is not None else None
    passed = number("passed")
    failed = number("failed")
    expected = number("expected_failures")
    unexpected = number("unexpected_successes")
    values = (discovered, tests_run, executed, passed, failed, skipped, expected, unexpected)
    if any(item is None for item in values):
        consistent = False
    else:
        assert discovered is not None
        assert tests_run is not None
        assert executed is not None
        assert passed is not None
        assert failed is not None
        assert skipped is not None
        assert expected is not None
        assert unexpected is not None
        consistent = (
            tests_run >= skipped and discovered >= tests_run and executed == passed + failed + expected + unexpected
        )
    return {
        "discovered": discovered,
        "tests_run": tests_run,
        "executed": executed,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "expected_failures": expected,
        "unexpected_successes": unexpected,
        "consistent": consistent,
    }


def _count_nested_records(value: Any, key: str) -> int:
    if isinstance(value, Mapping):
        return sum(_count_nested_records(item, key) for item in value.values()) + (
            len(value.get(key, [])) if isinstance(value.get(key), list) else 0
        )
    if isinstance(value, list):
        return sum(_count_nested_records(item, key) for item in value)
    return 0


def _failure_identifiers(value: str, *, root: Path, temporary_roots: Iterable[Path] = ()) -> list[str]:
    identifiers = (
        _sanitize_text(match.group(1).strip(), root=root, temporary_roots=temporary_roots)
        for match in _FAILURE_IDENTIFIER.finditer(value or "")
    )
    return sorted({item for item in identifiers if item})


def _run_offline_suite(
    runtime_python: Path,
    node: Path | None,
    *,
    root: Path,
    environment: Mapping[str, str],
    temporary_root: Path,
    timeout: float,
    coverage_file: Path | None = None,
    coverage_site: Path | None = None,
    build_python: Path | None = None,
) -> dict[str, Any]:
    coverage_requested = coverage_file is not None or coverage_site is not None
    if (coverage_file is None) != (coverage_site is None):
        return {
            "ok": False,
            "required_ui_javascript": True,
            "node": {"ok": False, "reason": "coverage_file y coverage_site deben proporcionarse juntos"},
            "counts": _compact_counts(None),
            "reason": "cobertura mal configurada; no se ejecutó la suite",
            "coverage": {"enabled": coverage_requested, "ok": False, "error": "coverage configuration mismatch"},
        }
    node_receipt = _node_version(node, root=root, environment=environment, timeout=min(timeout, 30.0))
    if not node_receipt.get("ok"):
        return {
            "ok": False,
            "required_ui_javascript": True,
            "node": node_receipt,
            "counts": _compact_counts(None),
            "reason": "Node real no disponible; MTF_UI_JS_DEV=1 es un gate obligatorio",
            "guard_evidence": None,
            "guard_error": "offline runner was not started",
            "coverage": {
                "enabled": coverage_requested,
                "ok": False if coverage_requested else None,
                "error": "suite no ejecutada porque falta Node",
            },
        }
    result_path = temporary_root / "offline-results.json"
    command = [
        str(runtime_python),
        "tools/offline_tests.py",
        "--root",
        ".",
        "--timeout",
        str(timeout),
        "--json",
        str(result_path),
    ]
    child_environment = dict(environment)
    child_environment.update({"MTF_UI_JS_DEV": "1", "MTF_NODE_BIN": str(node)})
    if build_python is not None:
        child_environment["MTF_LAB_BUILD_PYTHON"] = str(build_python)
    if coverage_file is not None:
        assert coverage_site is not None
        command.extend(["--coverage-file", str(coverage_file), "--coverage-site", str(coverage_site)])
    outcome = run_command(command, root=root, environment=child_environment, timeout=timeout + 30.0)
    payload: Any = None
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        payload = None
    suite = payload.get("suite") if isinstance(payload, Mapping) else None
    summary = suite.get("summary") if isinstance(suite, Mapping) else None
    counts = _compact_counts(summary if isinstance(summary, Mapping) else None)
    raw_stdout = suite.get("stdout", "") if isinstance(suite, Mapping) else ""
    raw_stderr = suite.get("stderr", "") if isinstance(suite, Mapping) else ""
    failure_identifiers = set(
        _failure_identifiers(f"{raw_stdout}\n{raw_stderr}", root=root, temporary_roots=(temporary_root,))
    )
    if isinstance(summary, Mapping):
        for identifier in summary.get("failure_identifiers", []):
            if isinstance(identifier, str) and identifier.strip():
                failure_identifiers.add(
                    _sanitize_text(identifier.strip(), root=root, temporary_roots=(temporary_root,))
                )
    runner_output = {
        "stdout_tail": _sanitize_text(_tail(str(raw_stdout), 6000), root=root, temporary_roots=(temporary_root,)),
        "stderr_tail": _sanitize_text(_tail(str(raw_stderr), 6000), root=root, temporary_roots=(temporary_root,)),
        "failure_identifiers": sorted(failure_identifiers),
    }
    network_attempts = _count_nested_records(suite, "network_attempts")
    write_attempts = _count_nested_records(suite, "write_attempts")
    guard_evidence = suite.get("guard_evidence") if isinstance(suite, Mapping) else None
    guard_ok = bool(
        isinstance(guard_evidence, Mapping)
        and guard_evidence.get("runtime_log") is True
        and guard_evidence.get("write_log") is True
    )
    runner_success = bool(payload.get("success")) if isinstance(payload, Mapping) else False
    closure = {
        "discovered_positive": isinstance(counts["discovered"], int) and counts["discovered"] > 0,
        "all_discovered_attended": counts["discovered"] == counts["tests_run"]
        if counts["discovered"] is not None
        else False,
        "zero_failed": counts["failed"] == 0,
        "zero_skipped": counts["skipped"] == 0,
        "zero_expected_failures": counts["expected_failures"] == 0,
        "zero_unexpected_successes": counts["unexpected_successes"] == 0,
    }
    ok = (
        outcome.ok
        and runner_success
        and counts["consistent"]
        and all(closure.values())
        and network_attempts == 0
        and write_attempts == 0
        and guard_ok
    )
    return {
        "ok": ok,
        "required_ui_javascript": True,
        "node": node_receipt,
        "command": outcome.to_dict(root=root, temporary_roots=(temporary_root,)),
        "counts": counts,
        "runner_summary": sanitize_payload(
            summary if isinstance(summary, Mapping) else None,
            root=root,
            temporary_roots=(temporary_root,),
        ),
        "runner_output": runner_output,
        "closure": closure,
        "network_attempts": network_attempts,
        "write_attempts": write_attempts,
        "guard_evidence": sanitize_payload(
            guard_evidence,
            root=root,
            temporary_roots=(temporary_root,),
        ),
        "guard_error": (
            None
            if guard_ok
            else (suite.get("guard_error") if isinstance(suite, Mapping) else "offline guard evidence missing")
        ),
        "runner_success": runner_success,
        "coverage": sanitize_payload(
            payload.get("coverage") if isinstance(payload, Mapping) else None,
            root=root,
            temporary_roots=(temporary_root,),
        ),
        "diagnostic": ("offline suite failed; inspect runner_output" if not ok else None),
        "smoke": sanitize_payload(
            payload.get("smoke") if isinstance(payload, Mapping) else None,
            root=root,
            temporary_roots=(temporary_root,),
        ),
    }


def _run_sdk_codec_probe(
    runtime_python: Path, *, root: Path, environment: Mapping[str, str], timeout: float
) -> dict[str, Any]:
    # This is deliberately a local generated-message round-trip.  It creates
    # no client, socket, account, token, or transport connection.
    code = """
import importlib.metadata as metadata
import json
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as messages
from mtf_lab.data.ctrader_protocol import SdkProtobufCodec, WireMessage, dependency_report

report = dependency_report()
if not report.available:
    raise RuntimeError("Generated codec unavailable")
codec = SdkProtobufCodec()
heartbeat = codec.decode(codec.encode(WireMessage("PROTO_HEARTBEAT_EVENT")))
order = messages.ProtoOANewOrderReq(
    ctidTraderAccountId=1, symbolId=1, orderType=1, tradeSide=1, volume=1
)
decoded = codec.decode(codec.encode(WireMessage("ProtoOANewOrderReq", order, "offline-codec")))
assert heartbeat.payload_type_id == 51
assert decoded.payload_type_id == 2106
assert decoded.client_msg_id == "offline-codec"
print(json.dumps({
    "codec_backend": report.codec_backend,
    "schema_revision": report.schema_revision,
    "protobuf_version": metadata.version("protobuf"),
    "codec_operational": report.codec_operational,
    "heartbeat_payload_type": heartbeat.payload_type_id,
    "order_payload_type": decoded.payload_type_id,
    "order_message": type(decoded.payload).__name__,
}, sort_keys=True))
"""
    outcome = run_command([str(runtime_python), "-c", code], root=root, environment=environment, timeout=timeout)
    result = outcome.to_dict(root=root)
    parsed = _last_json(outcome.stdout)
    result["metadata"] = parsed if isinstance(parsed, Mapping) else None
    result["ok"] = bool(result["ok"] and isinstance(parsed, Mapping) and parsed.get("codec_operational"))
    return result


def _run_benchmark(
    runtime_python: Path,
    *,
    root: Path,
    environment: Mapping[str, str],
    timeout: float,
    events: int,
) -> dict[str, Any]:
    outcome = run_command(
        [str(runtime_python), "-m", "tools.benchmark_paper", "--events", str(events)],
        root=root,
        environment=environment,
        timeout=timeout,
    )
    payload = _last_json(outcome.stdout)
    valid = isinstance(payload, Mapping) and payload.get("network_blocked") is True
    valid = bool(valid and payload.get("events_durable") == events and "synthetic" in str(payload.get("fixture")))
    return {
        "ok": bool(outcome.ok and valid),
        "command": outcome.to_dict(root=root),
        "events_requested": events,
        "result": sanitize_payload(payload if isinstance(payload, Mapping) else None, root=root),
        "verified_synthetic": valid,
    }


_MARKDOWN_LINK = re.compile(r"(?<!!)\[[^]]+\]\(([^)\s]+)(?:\s+[^)]*)?\)")


def document_link_check(root: Path) -> dict[str, Any]:
    """Resolve required final-report links from the checkout, not from GitHub."""

    required = (REPORT_DIRECTORY / "engineering_consolidation.md", Path("docs/activation_boundaries.md"))
    broken: list[str] = []
    deferred: list[str] = []
    checked = 0
    generated = {REPORT_DIRECTORY / RESULTS_FILENAME, REPORT_DIRECTORY / TOOLING_FILENAME}
    for relative in required:
        path = root / relative
        if not path.is_file():
            broken.append(relative.as_posix())
            continue
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            broken.append(relative.as_posix())
            continue
        for target in _MARKDOWN_LINK.findall(source):
            target = unquote(target).split("#", 1)[0]
            if not target or target.startswith(("http:", "https:", "mailto:", "codex:")):
                continue
            checked += 1
            candidate = (path.parent / target).resolve()
            try:
                candidate_relative = candidate.relative_to(root.resolve())
            except ValueError:
                broken.append(f"{relative.as_posix()} -> external-path")
                continue
            if candidate_relative in generated and not candidate.is_file():
                # The two JSON receipts are written after this pre-write gate;
                # retain the link contract and verify their bytes in clone QA.
                deferred.append(candidate_relative.as_posix())
            elif not candidate.is_file():
                broken.append(f"{relative.as_posix()} -> {target}")
    return {
        "ok": not broken,
        "required_documents": [item.as_posix() for item in required],
        "links_checked": checked,
        "deferred_generated": sorted(set(deferred)),
        "broken": broken,
    }


def _failed_quality_command(
    command: Sequence[str | os.PathLike[str]],
    reason: str,
    *,
    root: Path,
    temporary_root: Path,
) -> dict[str, Any]:
    outcome = _CommandOutcome(
        tuple(os.fspath(item) for item in command),
        2,
        "",
        reason,
        False,
        0.0,
    )
    return outcome.to_dict(root=root, temporary_roots=(temporary_root,))


def _run_quality_gates(
    runtime_python: Path,
    dev_python: Path,
    *,
    root: Path,
    environment: Mapping[str, str],
    temporary_root: Path,
    timeout: float,
    scope: QualityScope,
) -> dict[str, Any]:
    """Run required quality checks over one complete, immutable scope."""

    scope_receipt = scope.as_dict(root)
    scope_ok = bool(scope_receipt["ok"])
    scope_reason = json.dumps(
        {
            "missing_roots": scope_receipt["missing_roots"],
            "empty_roots": scope_receipt["empty_roots"],
            "missing_files": scope_receipt["missing_files"],
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    reason = f"quality scope invalid: {scope_reason}"
    compile_environment = dict(environment)
    compile_environment["PYTHONPYCACHEPREFIX"] = str(temporary_root / "compile-cache")
    compile_outcome = run_command(
        [str(runtime_python), "-m", "compileall", "-q", *scope.ruff_roots],
        root=root,
        environment=compile_environment,
        timeout=min(timeout, 180.0),
    )

    diff_outcome = run_command(
        ["git", "diff", "--check", "HEAD", "--"],
        root=root,
        environment=environment,
        timeout=min(timeout, 60.0),
    )
    cached_diff_outcome = run_command(
        ["git", "diff", "--cached", "--check", "--"],
        root=root,
        environment=environment,
        timeout=min(timeout, 60.0),
    )
    diff_receipt = {
        "ok": diff_outcome.ok and cached_diff_outcome.ok,
        "unstaged": diff_outcome.to_dict(root=root),
        "staged": cached_diff_outcome.to_dict(root=root),
    }

    pip_outcome = run_command(
        [str(runtime_python), "-m", "pip", "check"],
        root=root,
        environment=environment,
        timeout=min(timeout, 60.0),
    )
    if scope_ok:
        ruff_outcome = run_command(
            [str(dev_python), "-m", "ruff", "check", "--no-cache", "--output-format", "concise", *scope.ruff_roots],
            root=root,
            environment=environment,
            timeout=min(timeout, 180.0),
        )
        format_outcome = run_command(
            [str(dev_python), "-m", "ruff", "format", "--check", "--no-cache", *scope.ruff_roots],
            root=root,
            environment=environment,
            timeout=min(timeout, 180.0),
        )
        mypy_cache = temporary_root / "mypy-cache"
        mypy_outcome = run_command(
            [
                str(dev_python),
                "-m",
                "mypy",
                "--strict",
                "--explicit-package-bases",
                "--no-incremental",
                "--python-executable",
                str(runtime_python),
                "--cache-dir",
                str(mypy_cache),
                "--show-error-codes",
                *scope.mypy_roots,
            ],
            root=root,
            environment=environment,
            timeout=min(timeout, 300.0),
        )
    else:
        ruff_outcome = None
        format_outcome = None
        mypy_outcome = None

    architecture_json = temporary_root / "architecture.json"
    architecture_outcome = run_command(
        [str(runtime_python), "tools/engineering_audit.py", "--strict", "--json", str(architecture_json)],
        root=root,
        environment=environment,
        timeout=min(timeout, 180.0),
    )
    try:
        architecture_payload = json.loads(architecture_json.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        architecture_payload = None
    architecture_summary = architecture_payload.get("summary") if isinstance(architecture_payload, Mapping) else None
    architecture_ok = bool(
        architecture_outcome.ok
        and isinstance(architecture_summary, Mapping)
        and int(architecture_summary.get("strict_violations", 1)) == 0
    )
    ruff_receipt = (
        ruff_outcome.to_dict(root=root)
        if ruff_outcome is not None
        else _failed_quality_command(
            ["ruff", "check", *scope.ruff_roots], reason, root=root, temporary_root=temporary_root
        )
    )
    format_receipt = (
        format_outcome.to_dict(root=root)
        if format_outcome is not None
        else _failed_quality_command(
            ["ruff", "format", *scope.ruff_roots], reason, root=root, temporary_root=temporary_root
        )
    )
    mypy_receipt = (
        mypy_outcome.to_dict(root=root, temporary_roots=(temporary_root,))
        if mypy_outcome is not None
        else _failed_quality_command(
            ["mypy", "--strict", "--explicit-package-bases", *scope.mypy_roots],
            reason,
            root=root,
            temporary_root=temporary_root,
        )
    )
    pyright_outcome = run_command(
        [str(dev_python), "-m", "pyright", "--project", "pyrightconfig.json", "--pythonpath", str(runtime_python)],
        root=root,
        environment=environment,
        timeout=min(timeout, 420.0),
    )
    return {
        "quality_scope": scope_receipt,
        "compilation": {"ok": compile_outcome.ok, "command": compile_outcome.to_dict(root=root)},
        "git_diff_check": diff_receipt,
        "pip_check": {"ok": pip_outcome.ok, "command": pip_outcome.to_dict(root=root)},
        "ruff": {
            "ok": bool(ruff_outcome is not None and ruff_outcome.ok and scope_ok),
            "roots": list(scope.ruff_roots),
            "files": list(scope.ruff_files),
            "command": ruff_receipt,
        },
        "format": {
            "ok": bool(format_outcome is not None and format_outcome.ok and scope_ok),
            "roots": list(scope.ruff_roots),
            "files": list(scope.ruff_files),
            "command": format_receipt,
        },
        "mypy": {
            "ok": bool(mypy_outcome is not None and mypy_outcome.ok and scope_ok),
            "roots": list(scope.mypy_roots),
            "files": list(scope.mypy_files),
            "files_checked": len(scope.mypy_files),
            "command": mypy_receipt,
        },
        "pyright": {
            "ok": pyright_outcome.ok,
            "command": pyright_outcome.to_dict(root=root, temporary_roots=(temporary_root,)),
        },
        "architecture": {
            "ok": architecture_ok,
            "command": architecture_outcome.to_dict(root=root, temporary_roots=(temporary_root,)),
            "summary": sanitize_payload(architecture_summary, root=root, temporary_roots=(temporary_root,)),
            "cycles": sanitize_payload(
                architecture_payload.get("cycles") if isinstance(architecture_payload, Mapping) else None,
                root=root,
                temporary_roots=(temporary_root,),
            ),
            "forbidden_core_imports": sanitize_payload(
                architecture_payload.get("forbidden_core_imports")
                if isinstance(architecture_payload, Mapping)
                else None,
                root=root,
                temporary_roots=(temporary_root,),
            ),
            "import_time_side_effects": sanitize_payload(
                architecture_payload.get("import_time_side_effects")
                if isinstance(architecture_payload, Mapping)
                else None,
                root=root,
                temporary_roots=(temporary_root,),
            ),
            "parse_errors": sanitize_payload(
                architecture_payload.get("parse_errors") if isinstance(architecture_payload, Mapping) else None,
                root=root,
                temporary_roots=(temporary_root,),
            ),
        },
        "document_links": document_link_check(root),
    }


def _all_gates_pass(validation: Mapping[str, Any]) -> bool:
    def walk(value: Any) -> bool:
        if isinstance(value, Mapping):
            if "ok" in value and value["ok"] is False:
                return False
            return all(walk(item) for item in value.values())
        if isinstance(value, list):
            return all(walk(item) for item in value)
        return True

    return walk(validation)


def run(
    root: str | os.PathLike[str] = ".",
    *,
    runtime_python: str | os.PathLike[str] | None = None,
    dev_python: str | os.PathLike[str] | None = None,
    node: str | os.PathLike[str] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    benchmark_events: int = DEFAULT_BENCHMARK_EVENTS,
    skip_benchmark: bool = False,
) -> dict[str, Any]:
    """Execute all offline gates and return a portable diagnostic bundle."""

    root_path = Path(root).resolve()
    runtime_path = Path(runtime_python or root_path / ".venv/bin/python")
    dev_path = Path(dev_python or root_path / ".venv-dev/bin/python")
    if not runtime_path.is_absolute():
        runtime_path = root_path / runtime_path
    if not dev_path.is_absolute():
        dev_path = root_path / dev_path
    configured_node = os.environ.get("MTF_NODE_BIN")
    discovered_node = shutil.which("node")
    node_candidate = node or configured_node or discovered_node
    node_path = Path(node_candidate) if node_candidate is not None else None
    if node_path is not None and not node_path.is_absolute():
        node_path = root_path / node_path
    with tempfile.TemporaryDirectory(prefix="mtf-delivery-") as name:
        temporary_root = Path(name)
        environment = _command_environment(temporary_root)
        scope = discover_quality_scope(root_path)
        before = source_manifest(root_path)
        git = git_snapshot(root_path)
        runtime_exists = runtime_path.is_file() and os.access(runtime_path, os.X_OK)
        dev_exists = dev_path.is_file() and os.access(dev_path, os.X_OK)
        environment_probe = (
            _version_probe(runtime_path, root=root_path, environment=environment, timeout=min(timeout, 60.0))
            if runtime_exists
            else {"ok": False, "reason": "runtime-python-not-found"}
        )
        dev_environment_probe = (
            _version_probe(
                dev_path,
                root=root_path,
                environment=environment,
                timeout=min(timeout, 60.0),
                distribution_names=DEV_DISTRIBUTIONS,
            )
            if dev_exists
            else {"ok": False, "reason": "dev-python-not-found"}
        )
        quality = (
            _run_quality_gates(
                runtime_path,
                dev_path,
                root=root_path,
                environment=environment,
                temporary_root=temporary_root,
                timeout=timeout,
                scope=scope,
            )
            if runtime_exists and dev_exists
            else {
                "quality_scope": scope.as_dict(root_path),
                "compilation": {
                    "ok": False,
                    "reason": "runtime-python-not-found" if not runtime_exists else "dev-python-not-found",
                },
                "git_diff_check": {"ok": False, "reason": "tooling-interpreter-not-found"},
                "pip_check": {"ok": False, "reason": "runtime-python-not-found"},
                "ruff": {"ok": False, "reason": "dev-python-not-found"},
                "format": {"ok": False, "reason": "dev-python-not-found"},
                "mypy": {"ok": False, "reason": "dev-python-not-found"},
                "pyright": {"ok": False, "reason": "dev-python-not-found"},
                "architecture": {"ok": False, "reason": "runtime-python-not-found"},
            }
        )
        offline = (
            _run_offline_suite(
                runtime_path,
                node_path,
                root=root_path,
                environment=environment,
                temporary_root=temporary_root,
                timeout=timeout,
                build_python=dev_path if dev_exists else None,
            )
            if runtime_exists
            else {"ok": False, "reason": "runtime-python-not-found"}
        )
        sdk_codec = (
            _run_sdk_codec_probe(runtime_path, root=root_path, environment=environment, timeout=min(timeout, 120.0))
            if runtime_exists
            else {"ok": False, "reason": "runtime-python-not-found"}
        )
        benchmark = (
            {
                "ok": False,
                "status": "not_run",
                "events_requested": benchmark_events,
                "reason": "benchmark was explicitly skipped",
            }
            if skip_benchmark
            else (
                _run_benchmark(
                    runtime_path,
                    root=root_path,
                    environment=environment,
                    timeout=timeout,
                    events=benchmark_events,
                )
                if runtime_exists
                else {"ok": False, "reason": "runtime-python-not-found"}
            )
        )
        after = source_manifest(root_path)
        integrity = _manifest_delta(before, after)
        validation = {
            **quality,
            "environment": {
                "runtime": sanitize_payload(environment_probe, root=root_path, temporary_roots=(temporary_root,)),
                "dev": sanitize_payload(dev_environment_probe, root=root_path, temporary_roots=(temporary_root,)),
            },
            "offline_suite": offline,
            "sdk_codec": sdk_codec,
            "benchmark": benchmark,
            "source_integrity": integrity,
        }
        validation_ok = _all_gates_pass(validation) and integrity["ok"]
        bundle = {
            "schema_version": 2,
            "scope": "final local offline delivery gate; no OAuth, account access, broker or order execution",
            "verified_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "git": git,
            "source_tree": {
                "base_commit": BASE_COMMIT,
                "head_commit": git.get("head"),
                "head_tree_hash": git.get("head_tree_hash"),
                "branch": git.get("branch"),
                "origin_main": git.get("origin_main"),
                **after,
            },
            "source_integrity": integrity,
            "environment": {
                "runtime_python": {
                    "path": ".venv/bin/python"
                    if runtime_path == root_path / ".venv/bin/python"
                    else "<external-python>",
                    "available": runtime_exists,
                    "probe": sanitize_payload(environment_probe, root=root_path, temporary_roots=(temporary_root,)),
                },
                "dev_python": {
                    "path": ".venv-dev/bin/python"
                    if dev_path == root_path / ".venv-dev/bin/python"
                    else "<external-python>",
                    "available": dev_exists,
                    "probe": sanitize_payload(dev_environment_probe, root=root_path, temporary_roots=(temporary_root,)),
                },
                "python": {
                    "runtime": _probe_metadata_value(environment_probe, "python_version"),
                    "dev": _probe_metadata_value(dev_environment_probe, "python_version"),
                },
                "sqlite": _probe_metadata_value(environment_probe, "sqlite"),
                "distributions": _probe_metadata_value(environment_probe, "distributions", {}),
                "dev_distributions": _probe_metadata_value(dev_environment_probe, "distributions", {}),
                "node": sanitize_payload(offline.get("node"), root=root_path, temporary_roots=(temporary_root,)),
            },
            "validation": validation,
            "tests": offline.get("counts", _compact_counts(None)),
            "external_validation": {
                "status": "NOT_EXECUTED",
                "oauth": "NOT_EXECUTED",
                "account_discovery": "NOT_EXECUTED",
                "demo_orders": "NOT_EXECUTED",
                "real_live_execution": "REJECTED",
                "network_policy": "external network blocked by offline child guard",
                "credentials_used": False,
            },
            "success": bool(validation_ok and runtime_exists and dev_exists),
        }
        return cast(dict[str, Any], sanitize_payload(bundle, root=root_path, temporary_roots=(temporary_root,)))


def _identity_contract() -> dict[str, Any]:
    return {
        "checkout_provenance": (
            "head_commit and head_tree_hash describe HEAD before this gate; they do not identify uncommitted working-tree content"
        ),
        "validated_working_tree_identity": ["content_sha256", "files"],
        "self_referential_outputs_excluded": [
            (REPORT_DIRECTORY / RESULTS_FILENAME).as_posix(),
            (REPORT_DIRECTORY / TOOLING_FILENAME).as_posix(),
        ],
        "mutation_check": "source_integrity compares the pre-gate and post-gate content_sha256 and must be ok",
    }


def build_reports(bundle: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create the two canonical, portable JSON documents from one run."""

    results = sanitize_payload(dict(bundle))
    results["identity_contract"] = _identity_contract()
    results["references"] = {
        "report": "engineering_consolidation.md",
        "results": RESULTS_FILENAME,
        "tooling": TOOLING_FILENAME,
        "activation_boundaries": "../../../docs/activation_boundaries.md",
    }
    results["report_contract"] = {
        "results": RESULTS_FILENAME,
        "tooling": TOOLING_FILENAME,
        "report_directory": REPORT_DIRECTORY.as_posix(),
        "source_fingerprint_excludes": [
            (REPORT_DIRECTORY / RESULTS_FILENAME).as_posix(),
            (REPORT_DIRECTORY / TOOLING_FILENAME).as_posix(),
            "reports/generated/",
            ".venv/",
            ".venv-dev/",
            "*cache/",
        ],
    }
    validation = results.get("validation", {})
    quality_scope = validation.get("quality_scope", {}) if isinstance(validation, Mapping) else {}
    tooling = {
        "schema_version": 2,
        "scope": {
            "runtime": "stdlib/offline; optional cTrader SDK checked locally",
            "network": "No OAuth, account, broker or order access",
            "credentials_used": False,
            "source_fingerprint": "tracked and non-ignored working-tree files, excluding generated engineering receipts",
        },
        "configuration": {
            "quality_scope": quality_scope,
            "mypy_roots": quality_scope.get("mypy_roots", []) if isinstance(quality_scope, Mapping) else [],
            "ruff_format_roots": quality_scope.get("ruff_roots", []) if isinstance(quality_scope, Mapping) else [],
            "mypy_files": quality_scope.get("mypy_files", []) if isinstance(quality_scope, Mapping) else [],
            "ruff_format_files": quality_scope.get("ruff_files", []) if isinstance(quality_scope, Mapping) else [],
            "scope_identity_sha256": (
                quality_scope.get("identity_sha256") if isinstance(quality_scope, Mapping) else None
            ),
            "report_directory": REPORT_DIRECTORY.as_posix(),
        },
        "interpreters": results.get("environment", {}),
        "quality_checks": {
            "final_gates": validation,
            "test_counts": results.get("tests"),
        },
        "source_tree": {
            "base_commit": results.get("source_tree", {}).get("base_commit"),
            "head_commit": results.get("source_tree", {}).get("head_commit"),
            "head_tree_hash": results.get("source_tree", {}).get("head_tree_hash"),
            "file_count": results.get("source_tree", {}).get("file_count"),
            "content_sha256": results.get("source_tree", {}).get("content_sha256"),
        },
        "identity_contract": _identity_contract(),
        "external_validation": results.get("external_validation"),
        "validation": {
            "final_receipt": RESULTS_FILENAME,
            "offline_external_status": "NOT_EXECUTED",
        },
    }
    return results, tooling


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gate reproducible offline de entrega de MTF Lab")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument(
        "--runtime-python", default=None, help="intérprete runtime explícito; por defecto root/.venv/bin/python"
    )
    parser.add_argument(
        "--dev-python", default=None, help="intérprete dev explícito; por defecto root/.venv-dev/bin/python"
    )
    parser.add_argument("--node", default=None, help="binario Node explícito; por defecto se busca en PATH")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--benchmark-events", type=int, default=DEFAULT_BENCHMARK_EVENTS)
    parser.add_argument(
        "--skip-benchmark", action="store_true", help="diagnóstico solamente; el resultado no puede pasar"
    )
    parser.add_argument("--write-reports", action="store_true", help=f"escribe {REPORT_DIRECTORY.as_posix()}/*.json")
    parser.add_argument("--json", dest="json_path", default="-", help="bundle JSON o '-' para stdout")
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout debe ser positivo")
    if args.benchmark_events < 1:
        parser.error("--benchmark-events debe ser positivo")
    bundle = run(
        args.root,
        runtime_python=args.runtime_python,
        dev_python=args.dev_python,
        node=args.node,
        timeout=args.timeout,
        benchmark_events=args.benchmark_events,
        skip_benchmark=args.skip_benchmark,
    )
    if args.write_reports:
        results, tooling = build_reports(bundle)
        report_root = Path(args.root).resolve() / REPORT_DIRECTORY
        _write_json(report_root / RESULTS_FILENAME, results)
        _write_json(report_root / TOOLING_FILENAME, tooling)
    rendered = json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.json_path == "-":
        sys.stdout.write(rendered)
    else:
        _write_json(Path(args.json_path), bundle)
    return 0 if bundle.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
