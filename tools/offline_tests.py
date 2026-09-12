#!/usr/bin/env python3
"""Run MTF Lab's local validation without broker/network side effects.

The runner uses a child process with isolated HOME/XDG/TMP directories and a
small socket guard.  Loopback is permitted because the existing UI tests use a
local HTTP server; all non-loopback DNS, connects, and URL opens are rejected.
It never installs packages, logs in, opens OAuth, or contacts a broker.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from importlib import metadata

from tools.engineering_audit import analyze_repository

SUMMARY_MARKER = "__MTF_SUMMARY__"
RUNTIME_LOG_ENV = "MTF_OFFLINE_RUNTIME_LOG"
WRITE_LOG_ENV = "MTF_OFFLINE_WRITE_LOG"

_SAFE_INHERITED_ENV = frozenset(
    {
        "CI",
        "COLORTERM",
        "FORCE_COLOR",
        "LANG",
        "NO_COLOR",
        "PATH",
        "PYTHONHASHSEED",
        "PYTHONIOENCODING",
        "PYTHONUTF8",
        "TERM",
        "TZ",
        # Non-secret controls for the opt-in UI gate.
        "MTF_NODE_BIN",
        "MTF_UI_JS_DEV",
        # The wheel smoke uses the explicitly selected tooling interpreter;
        # this path is not a credential and is never copied from an arbitrary
        # environment by the delivery gate.
        "MTF_LAB_BUILD_PYTHON",
    }
)


def _safe_parent_environment() -> dict[str, str]:
    """Do not let credentials or ambient Python overrides enter the child."""

    result = {
        name: value for name, value in os.environ.items() if name in _SAFE_INHERITED_ENV or name.startswith("LC_")
    }
    result.setdefault("PATH", os.defpath)
    return result


# sitecustomize is deliberately tiny and stdlib-only.  It is loaded before
# project modules because the runner places its temporary directory first in
# PYTHONPATH.  The child writes only diagnostic JSON in that temporary dir.
_NETWORK_GUARD = r"""
import atexit
import builtins
import io
import json
import os
from pathlib import Path
import socket
import sys
import urllib.parse
import urllib.request

_ORIGINAL_OPEN = builtins.open
_ORIGINAL_IO_OPEN = io.open
_ORIGINAL_OS_OPEN = os.open
_ORIGINAL_SOCKET = socket.socket
_ORIGINAL_CREATE_CONNECTION = socket.create_connection
_RUNTIME_LOG = os.environ.get("MTF_OFFLINE_RUNTIME_LOG")
_WRITE_LOG = os.environ.get("MTF_OFFLINE_WRITE_LOG")
_BLOCK_WRITES = os.environ.get("MTF_OFFLINE_BLOCK_WRITES", "0") == "1"
_network_attempts = []
_write_attempts = []


def _record(path, collection, kind):
    item = {"kind": kind, "target": str(path)}
    collection.append(item)
    return item


def _host(address):
    if address is None:
        return None
    if isinstance(address, (tuple, list)):
        return address[0] if address else None
    if isinstance(address, str):
        parsed = urllib.parse.urlsplit(address)
        return parsed.hostname or address
    return str(address)


def _local_host(host):
    if host is None:
        return True
    text = str(host).strip().lower().strip("[]")
    return text in {"", "localhost", "::1", "0:0:0:0:0:0:0:1"} or text.startswith("127.")


def _check_host(host):
    if _local_host(host):
        return
    _record(host, _network_attempts, "external_network_blocked")
    raise RuntimeError("offline network blocked: " + str(host))


def _guarded_getaddrinfo(host, *args, **kwargs):
    _check_host(host)
    return _ORIGINAL_GETADDRINFO(host, *args, **kwargs)


_ORIGINAL_GETADDRINFO = socket.getaddrinfo
socket.getaddrinfo = _guarded_getaddrinfo


def _guarded_create_connection(address, *args, **kwargs):
    _check_host(_host(address))
    return _ORIGINAL_CREATE_CONNECTION(address, *args, **kwargs)


socket.create_connection = _guarded_create_connection


class _GuardedSocket(_ORIGINAL_SOCKET):
    def connect(self, address):
        _check_host(_host(address))
        return super().connect(address)

    def connect_ex(self, address):
        _check_host(_host(address))
        return super().connect_ex(address)


socket.socket = _GuardedSocket

_ORIGINAL_URL_OPEN = urllib.request.urlopen


def _guarded_urlopen(url, *args, **kwargs):
    value = url if isinstance(url, str) else getattr(url, "full_url", url)
    parsed = urllib.parse.urlsplit(str(value))
    _check_host(parsed.hostname)
    return _ORIGINAL_URL_OPEN(url, *args, **kwargs)


urllib.request.urlopen = _guarded_urlopen


def _write_mode(mode):
    return isinstance(mode, str) and any(flag in mode for flag in "wax+")


def _guarded_open(file, mode="r", *args, **kwargs):
    if _BLOCK_WRITES and _write_mode(mode):
        _record(file, _write_attempts, "filesystem_write_blocked")
        raise RuntimeError("offline import smoke blocked filesystem write: " + str(file))
    return _ORIGINAL_OPEN(file, mode, *args, **kwargs)


def _guarded_io_open(file, mode="r", *args, **kwargs):
    if _BLOCK_WRITES and _write_mode(mode):
        _record(file, _write_attempts, "filesystem_write_blocked")
        raise RuntimeError("offline import smoke blocked filesystem write: " + str(file))
    return _ORIGINAL_IO_OPEN(file, mode, *args, **kwargs)


builtins.open = _guarded_open
io.open = _guarded_io_open


def _guarded_os_open(file, flags, *args, **kwargs):
    write_flags = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC
    if _BLOCK_WRITES and flags & write_flags:
        _record(file, _write_attempts, "filesystem_write_blocked")
        raise RuntimeError("offline import smoke blocked os.open: " + str(file))
    return _ORIGINAL_OS_OPEN(file, flags, *args, **kwargs)


os.open = _guarded_os_open


def _blocked_fs_call(name):
    def call(*args, **kwargs):
        if _BLOCK_WRITES:
            target = args[0] if args else name
            _record(target, _write_attempts, "filesystem_write_blocked")
            raise RuntimeError("offline import smoke blocked " + name)
        return _ORIGINAL_FS[name](*args, **kwargs)
    return call


_ORIGINAL_FS = {}
for _name in ("mkdir", "makedirs", "remove", "unlink", "rename", "replace", "rmdir"):
    if hasattr(os, _name):
        _ORIGINAL_FS[_name] = getattr(os, _name)
        setattr(os, _name, _blocked_fs_call(_name))


def _write_diagnostics():
    payload = {
        "executable": sys.executable,
        "version": sys.version,
        "loaded_optional": sorted(
            name for name in sys.modules
            if name == "ctrader_open_api" or name.startswith(("ctrader_open_api.", "twisted", "google.protobuf", "requests"))
        ),
        "network_attempts": _network_attempts,
        "write_attempts": _write_attempts,
    }
    if _RUNTIME_LOG:
        try:
            with _ORIGINAL_OPEN(_RUNTIME_LOG, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, sort_keys=True)
        except Exception:
            pass
    if _WRITE_LOG:
        try:
            with _ORIGINAL_OPEN(_WRITE_LOG, "w", encoding="utf-8") as handle:
                json.dump(_write_attempts, handle, sort_keys=True)
        except Exception:
            pass


atexit.register(_write_diagnostics)
"""

_TEST_DRIVER = r"""
import json
import os
import sys
import unittest


def flatten(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from flatten(item)
        else:
            yield item

start = sys.argv[1]
pattern = sys.argv[2]
loader = unittest.defaultTestLoader
# Keep the relative start directory: Python's unittest accepts a directory
# without __init__.py in this form, while an absolute path plus top_level_dir
# is rejected by recent Python versions.
coverage_instance = None
coverage_file = os.environ.get("MTF_OFFLINE_COVERAGE_FILE")
if coverage_file:
    try:
        import coverage

        coverage_instance = coverage.Coverage(
            data_file=coverage_file,
            branch=True,
            source=[os.environ["MTF_OFFLINE_COVERAGE_SOURCE"]],
            config_file=False,
        )
        coverage_instance.start()
    except Exception as exc:
        print(
            "__MTF_COVERAGE_ERROR__" + json.dumps(
                {"type": type(exc).__name__, "message": str(exc)}, sort_keys=True
            ),
            file=sys.stderr,
        )
        sys.exit(2)

try:
    suite = loader.discover(start_dir=start, pattern=pattern)
    discovered = list(flatten(suite))
    result = unittest.TextTestRunner(verbosity=1, stream=sys.stderr).run(suite)
finally:
    if coverage_instance is not None:
        coverage_instance.stop()
        coverage_instance.save()
failed = len(result.failures) + len(result.errors)
failure_identifiers = sorted(
    test.id() for test, _traceback in [*result.failures, *result.errors] if hasattr(test, "id")
)
summary = {
    "discovered": len(discovered),
    "tests_run": result.testsRun,
    "passed": result.testsRun - failed - len(result.skipped) - len(result.expectedFailures) - len(result.unexpectedSuccesses),
    "failed": failed,
    "failures": len(result.failures),
    "errors": len(result.errors),
    "skipped": len(result.skipped),
    "expected_failures": len(result.expectedFailures),
    "unexpected_successes": len(result.unexpectedSuccesses),
    "failure_identifiers": failure_identifiers,
    "successful": result.wasSuccessful(),
}
print("__MTF_SUMMARY__" + json.dumps(summary, sort_keys=True))
sys.exit(0 if result.wasSuccessful() else 1)
"""

_CORE_SMOKE = r"""
import json
import mtf_lab.core
print("__MTF_CORE_SMOKE__" + json.dumps({"imported": "mtf_lab.core"}, sort_keys=True))
"""


def _tail(value: str, limit: int = 3000) -> str:
    value = value or ""
    return value if len(value) <= limit else value[-limit:]


def _distribution_versions(names: Iterable[str]) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for name in names:
        try:
            result[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            result[name] = None
    return result


def _read_json(path: Path) -> dict[str, Any] | list[Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, (dict, list)) else None


def _isolated_env(
    root: Path,
    temp_root: Path,
    guard: Path,
    *,
    block_writes: bool,
    pythonpath_entries: Iterable[Path] = (),
    coverage_file: Path | None = None,
    coverage_source: Path | None = None,
) -> dict[str, str]:
    env = _safe_parent_environment()
    # The launcher honours PYTHON; remove an ambient override so the recorded
    # interpreter is the launcher's effective default.
    env.pop("PYTHON", None)
    env.update(
        {
            "HOME": str(temp_root / "home"),
            "XDG_CONFIG_HOME": str(temp_root / "config"),
            "XDG_CACHE_HOME": str(temp_root / "cache"),
            "XDG_DATA_HOME": str(temp_root / "data"),
            "XDG_STATE_HOME": str(temp_root / "state"),
            "MTF_LAB_STATE_DIR": str(temp_root / "state"),
            "TMPDIR": str(temp_root / "tmp"),
            "MTF_LAB_DB": str(temp_root / "data" / "mtf_lab.sqlite3"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPYCACHEPREFIX": str(temp_root / "pycache"),
            "MTF_LAB_OFFLINE": "1",
            "MTF_OFFLINE_BLOCK_WRITES": "1" if block_writes else "0",
            "PYTHONNOUSERSITE": "1",
        }
    )
    current = [str(guard), str(root)]
    current.extend(str(item) for item in pythonpath_entries)
    if env.get("PYTHONPATH"):
        current.extend(item for item in env["PYTHONPATH"].split(os.pathsep) if item)
    env["PYTHONPATH"] = os.pathsep.join(current)
    if coverage_file is not None:
        if coverage_source is None:
            raise ValueError("coverage_source is required when coverage_file is set")
        coverage_file.parent.mkdir(parents=True, exist_ok=True)
        env["MTF_OFFLINE_COVERAGE_FILE"] = str(coverage_file)
        env["MTF_OFFLINE_COVERAGE_SOURCE"] = str(coverage_source)
    for directory in ("home", "config", "cache", "data", "state", "tmp", "pycache"):
        (temp_root / directory).mkdir(parents=True, exist_ok=True)
    return env


def _child_command(
    command: list[str],
    *,
    root: Path,
    temp_root: Path,
    block_writes: bool,
    timeout: float,
    pythonpath_entries: Iterable[Path] = (),
    coverage_file: Path | None = None,
    coverage_source: Path | None = None,
) -> dict[str, Any]:
    guard = temp_root / "guard"
    guard.mkdir(parents=True, exist_ok=True)
    (guard / "sitecustomize.py").write_text(_NETWORK_GUARD, encoding="utf-8")
    runtime_log = temp_root / "runtime.json"
    write_log = temp_root / "writes.json"
    env = _isolated_env(
        root,
        temp_root,
        guard,
        block_writes=block_writes,
        pythonpath_entries=pythonpath_entries,
        coverage_file=coverage_file,
        coverage_source=coverage_source,
    )
    env[RUNTIME_LOG_ENV] = str(runtime_log)
    env[WRITE_LOG_ENV] = str(write_log)
    try:
        completed = subprocess.run(
            command,
            cwd=str(root),
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
        result: dict[str, Any] = {
            "command": command,
            "returncode": completed.returncode,
            "stdout": _tail(completed.stdout),
            "stderr": _tail(completed.stderr),
            "timed_out": False,
        }
    except subprocess.TimeoutExpired as exc:
        result = {
            "command": command,
            "returncode": None,
            "stdout": _tail(exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")),
            "stderr": _tail(exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")),
            "timed_out": True,
        }
    except OSError as exc:
        result = {
            "command": command,
            "returncode": None,
            "stdout": "",
            "stderr": f"{type(exc).__name__}: {exc}",
            "timed_out": False,
        }
    runtime = _read_json(runtime_log)
    writes = _read_json(write_log)
    runtime_log_ok = isinstance(runtime, dict)
    write_log_ok = isinstance(writes, list)
    result["runtime"] = runtime if isinstance(runtime, dict) else None
    result["write_attempts"] = writes if isinstance(writes, list) else []
    result["guard_evidence"] = {"runtime_log": runtime_log_ok, "write_log": write_log_ok}
    if not runtime_log_ok or not write_log_ok:
        result["guard_error"] = "offline guard did not produce complete diagnostic logs"
    if isinstance(runtime, dict):
        result["network_attempts"] = runtime.get("network_attempts", [])
        result["loaded_optional"] = runtime.get("loaded_optional", [])
    else:
        result["network_attempts"] = None
        result["loaded_optional"] = []
    return result


def _parse_summary(stdout: str) -> dict[str, Any] | None:
    for line in reversed((stdout or "").splitlines()):
        if line.startswith(SUMMARY_MARKER):
            try:
                value = json.loads(line[len(SUMMARY_MARKER) :])
            except json.JSONDecodeError:
                return None
            return value if isinstance(value, dict) else None
    return None


def run_suite(
    root: Path,
    *,
    timeout: float = 300.0,
    start_directory: str = "tests",
    pattern: str = "test*.py",
    coverage_file: Path | None = None,
    coverage_site: Path | None = None,
) -> dict[str, Any]:
    if (coverage_file is None) != (coverage_site is None):
        raise ValueError("coverage_file y coverage_site deben proporcionarse juntos")
    with tempfile.TemporaryDirectory(prefix="mtf-offline-suite-") as name:
        temp_root = Path(name)
        command = [sys.executable, "-c", _TEST_DRIVER, start_directory, pattern]
        result = _child_command(
            command,
            root=root,
            temp_root=temp_root,
            block_writes=False,
            timeout=timeout,
            pythonpath_entries=(coverage_site,) if coverage_site is not None else (),
            coverage_file=coverage_file,
            coverage_source=root / "mtf_lab" if coverage_file is not None else None,
        )
    summary = _parse_summary(str(result.get("stdout", "")))
    result["summary"] = summary
    result["guard_evidence"] = result.get("guard_evidence", {"runtime_log": False, "write_log": False})
    result["guard_error"] = result.get("guard_error")
    if coverage_file is not None:
        result["coverage"] = {
            "enabled": True,
            "data_file": str(coverage_file),
            "data_file_exists": coverage_file.is_file(),
            "site": str(coverage_site),
            "error": next(
                (
                    line[len("__MTF_COVERAGE_ERROR__") :]
                    for line in str(result.get("stderr", "")).splitlines()
                    if line.startswith("__MTF_COVERAGE_ERROR__")
                ),
                None,
            ),
        }
    else:
        result["coverage"] = {"enabled": False, "data_file_exists": False, "error": None}
    return result


def run_smoke(root: Path, *, timeout: float = 30.0) -> dict[str, Any]:
    launcher = root / "mtf-lab"
    with tempfile.TemporaryDirectory(prefix="mtf-offline-smoke-") as name:
        temp_root = Path(name)
        import_result = _child_command(
            [sys.executable, "-c", _CORE_SMOKE],
            root=root,
            temp_root=temp_root / "core",
            block_writes=True,
            timeout=timeout,
        )
        help_result = _child_command(
            [str(launcher), "--help"],
            root=root,
            temp_root=temp_root / "help",
            block_writes=True,
            timeout=timeout,
        )
    core_policy_ok = (
        import_result.get("guard_evidence", {}).get("runtime_log")
        and import_result.get("guard_evidence", {}).get("write_log")
        and not import_result.get("loaded_optional")
        and not import_result.get("network_attempts")
        and not import_result.get("write_attempts")
    )
    help_policy_ok = (
        help_result.get("guard_evidence", {}).get("runtime_log")
        and help_result.get("guard_evidence", {}).get("write_log")
        and not help_result.get("loaded_optional")
        and not help_result.get("network_attempts")
        and not help_result.get("write_attempts")
    )
    return {
        "core_import": {
            "returncode": import_result["returncode"],
            "ok": import_result["returncode"] == 0
            and "__MTF_CORE_SMOKE__" in str(import_result["stdout"])
            and core_policy_ok,
            "runtime": import_result.get("runtime"),
            "loaded_optional": import_result.get("loaded_optional", []),
            "network_attempts": import_result.get("network_attempts", []),
            "write_attempts": import_result.get("write_attempts", []),
            "guard_evidence": import_result.get("guard_evidence"),
            "guard_error": import_result.get("guard_error"),
            "stderr": import_result.get("stderr", ""),
        },
        "help": {
            "command": [str(launcher), "--help"],
            "returncode": help_result["returncode"],
            "ok": help_result["returncode"] == 0 and "usage:" in str(help_result["stdout"]) and help_policy_ok,
            "runtime": help_result.get("runtime"),
            "loaded_optional": help_result.get("loaded_optional", []),
            "network_attempts": help_result.get("network_attempts", []),
            "write_attempts": help_result.get("write_attempts", []),
            "guard_evidence": help_result.get("guard_evidence"),
            "guard_error": help_result.get("guard_error"),
            "stdout": help_result.get("stdout", ""),
            "stderr": help_result.get("stderr", ""),
        },
    }


def run_pip_check(root: Path, *, timeout: float = 30.0) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="mtf-offline-pip-") as name:
        temp_root = Path(name)
        result = _child_command(
            [sys.executable, "-m", "pip", "check"],
            root=root,
            temp_root=temp_root,
            block_writes=False,
            timeout=timeout,
        )
    return {
        "returncode": result["returncode"],
        "ok": result["returncode"] == 0
        and bool(result.get("guard_evidence", {}).get("runtime_log"))
        and bool(result.get("guard_evidence", {}).get("write_log")),
        "stdout": result.get("stdout", ""),
        "stderr": result.get("stderr", ""),
        "network_attempts": result.get("network_attempts", []),
        "guard_evidence": result.get("guard_evidence"),
        "guard_error": result.get("guard_error"),
    }


def environment_snapshot() -> dict[str, Any]:
    return {
        "python": {
            "executable": sys.executable,
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "sys_version": sys.version,
        },
        "sqlite": sqlite3.sqlite_version,
        "distributions": _distribution_versions(
            [
                "mtf-lab",
                "ctrader-open-api",
                "protobuf",
                "Twisted",
                "service-identity",
                "requests",
                "websockets",
                "pyOpenSSL",
                "pip",
            ]
        ),
    }


def run(
    root: str | os.PathLike[str] = ".",
    *,
    timeout: float = 300.0,
    skip_smoke: bool = False,
    coverage_file: Path | None = None,
    coverage_site: Path | None = None,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    result: dict[str, Any] = {
        "schema_version": 1,
        "root": str(root_path),
        "environment": environment_snapshot(),
        "audit": analyze_repository(root_path),
        "pip_check": run_pip_check(root_path, timeout=min(timeout, 60.0)),
        "suite": run_suite(
            root_path,
            timeout=timeout,
            coverage_file=coverage_file,
            coverage_site=coverage_site,
        ),
    }
    if not skip_smoke:
        result["smoke"] = run_smoke(root_path, timeout=min(timeout, 60.0))
    else:
        result["smoke"] = {"skipped": True}
    suite_summary = result["suite"].get("summary")
    suite_ok = bool(
        isinstance(suite_summary, dict)
        and suite_summary.get("successful")
        and not result["suite"].get("network_attempts")
        and result["suite"].get("guard_evidence", {}).get("runtime_log")
        and result["suite"].get("guard_evidence", {}).get("write_log")
    )
    smoke_ok = bool(result["smoke"].get("skipped")) or all(
        bool(value.get("ok")) for value in result["smoke"].values() if isinstance(value, dict) and "ok" in value
    )
    coverage_result = result["suite"].get("coverage", {})
    coverage_ok = not coverage_result.get("enabled") or bool(
        coverage_result.get("data_file_exists") and not coverage_result.get("error")
    )
    result["coverage"] = coverage_result
    result["success"] = bool(result["pip_check"].get("ok") and suite_ok and smoke_ok and coverage_ok)
    return result


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Suite y smoke offline de MTF Lab con red externa bloqueada")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--skip-smoke", action="store_true")
    parser.add_argument("--coverage-file", type=Path, help="archivo de datos Coverage.py fuera del checkout")
    parser.add_argument("--coverage-site", type=Path, help="site-packages del intérprete dev con Coverage.py")
    parser.add_argument("--json", dest="json_path", default="-", help="salida JSON o '-' para stdout")
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout debe ser positivo")
    if (args.coverage_file is None) != (args.coverage_site is None):
        parser.error("--coverage-file y --coverage-site deben proporcionarse juntos")
    result = run(
        args.root,
        timeout=args.timeout,
        skip_smoke=args.skip_smoke,
        coverage_file=args.coverage_file,
        coverage_site=args.coverage_site,
    )
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.json_path == "-":
        sys.stdout.write(rendered)
    else:
        _write_json(Path(args.json_path), result)
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
