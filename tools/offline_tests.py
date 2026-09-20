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
WRITE_AUDIT_ENV = "MTF_OFFLINE_WRITE_AUDIT"
WRITE_ROOTS_ENV = "MTF_OFFLINE_WRITE_ROOTS"
WRITE_PATHS_ENV = "MTF_OFFLINE_WRITE_PATHS"
WRITE_DIRS_ENV = "MTF_OFFLINE_WRITE_DIRS"
EXECUTION_ROOT_ENV = "MTF_OFFLINE_EXECUTION_ROOT"

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
import stat
import subprocess
import sys
import urllib.parse
import urllib.request

_ORIGINAL_OPEN = builtins.open
_ORIGINAL_IO_OPEN = io.open
_ORIGINAL_OS_OPEN = os.open
_ORIGINAL_OS_FSTAT = os.fstat
_ORIGINAL_OS_SYSTEM = os.system
_ORIGINAL_SUBPROCESS_POPEN = subprocess.Popen
_ORIGINAL_SOCKET = socket.socket
_ORIGINAL_CREATE_CONNECTION = socket.create_connection
_RUNTIME_LOG = os.environ.get("MTF_OFFLINE_RUNTIME_LOG")
_WRITE_LOG = os.environ.get("MTF_OFFLINE_WRITE_LOG")
_BLOCK_WRITES = os.environ.get("MTF_OFFLINE_BLOCK_WRITES", "0") == "1"
_WRITE_AUDIT = os.environ.get("MTF_OFFLINE_WRITE_AUDIT", "0") == "1"
_network_attempts = []
_write_attempts = []
_write_events = []
_subprocess_events = []
_allowed_write_count = 0
_blocked_write_count = 0
_read_only_probe_count = 0
_device_open_count = 0
_non_filesystem_fd_count = 0
_write_event_count = 0
_write_events_truncated = False
_WRITE_EVENT_LIMIT = 512
_child_log_counter = 0


def _paths_from_environment(name):
    raw = os.environ.get(name, "[]")
    try:
        values = json.loads(raw)
    except (TypeError, ValueError):
        values = []
    if not isinstance(values, list):
        return []
    result = []
    for value in values:
        if not isinstance(value, str) or not value:
            continue
        try:
            result.append(Path(value).expanduser().resolve(strict=False))
        except (OSError, RuntimeError, ValueError):
            continue
    return result


_WRITE_ROOTS = _paths_from_environment("MTF_OFFLINE_WRITE_ROOTS")
_WRITE_PATHS = _paths_from_environment("MTF_OFFLINE_WRITE_PATHS")
_WRITE_DIRS = _paths_from_environment("MTF_OFFLINE_WRITE_DIRS")
_EXECUTION_ROOT = None
_execution_root_value = os.environ.get("MTF_OFFLINE_EXECUTION_ROOT")
if _execution_root_value:
    try:
        _EXECUTION_ROOT = Path(_execution_root_value).expanduser().resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        _EXECUTION_ROOT = None


def _record(path, collection, kind):
    item = {"kind": kind, "target": str(path)}
    collection.append(item)
    return item


def _path_text(value):
    if isinstance(value, bytes):
        return os.fsdecode(value)
    if isinstance(value, int):
        return _fd_path(value) or "<fd:%d>" % value
    try:
        return os.fspath(value)
    except TypeError:
        return str(value)


def _fd_path(value):
    target = _fd_target(value)
    return target if target is not None and target.startswith("/") else None


def _fd_target(value):
    if not isinstance(value, int) or value < 0:
        return None
    try:
        return os.readlink("/proc/self/fd/%d" % value)
    except OSError:
        return None


def _anonymous_pipe_fd(value):
    target = _fd_target(value)
    if target is None or not target.startswith("pipe:["):
        return False
    try:
        return stat.S_ISFIFO(_ORIGINAL_OS_FSTAT(value).st_mode)
    except OSError:
        return False


def _base_for_dir_fd(dir_fd):
    if dir_fd is None or dir_fd == getattr(os, "AT_FDCWD", -100):
        return Path.cwd()
    target = _fd_path(dir_fd)
    return Path(target) if target is not None else None


def _resolved_path(value, dir_fd=None, *, follow_final=True):
    raw = _path_text(value)
    if isinstance(raw, bytes):
        raw = os.fsdecode(raw)
    if not isinstance(raw, str):
        raw = str(raw)
    if raw.startswith("<fd:"):
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        base = _base_for_dir_fd(dir_fd)
        if base is None:
            return None
        path = base / path
    try:
        if follow_final:
            return path.resolve(strict=False)
        return path.parent.resolve(strict=False) / path.name
    except (OSError, RuntimeError, ValueError):
        return None


def _within(path, root):
    if path is None:
        return False
    return path == root or root in path.parents


def _write_allowed(path, kind):
    if path is None:
        return False
    if any(_within(path, root) for root in _WRITE_ROOTS) or any(path == item for item in _WRITE_PATHS):
        return True
    return kind in {"os.mkdir", "os.makedirs"} and any(path == item for item in _WRITE_DIRS)


def _record_write_event(kind, target, resolved, allowed, action):
    global _allowed_write_count, _blocked_write_count, _read_only_probe_count, _device_open_count
    global _non_filesystem_fd_count
    global _write_event_count, _write_events_truncated
    _write_event_count += 1
    if action == "allow":
        _allowed_write_count += 1
    elif action == "probe_allow":
        _read_only_probe_count += 1
    elif action == "device_allow":
        _device_open_count += 1
    elif action == "nonfilesystem_allow":
        _non_filesystem_fd_count += 1
    else:
        _blocked_write_count += 1
    if not _WRITE_AUDIT:
        return
    if len(_write_events) >= _WRITE_EVENT_LIMIT:
        _write_events_truncated = True
        return
    _write_events.append(
        {
            "action": action,
            "allowed": allowed,
            "kind": kind,
            "resolved": str(resolved) if resolved is not None else None,
            "target": str(target),
        }
    )


def _allow_existing_directory_probe(kind, target, resolved, *, dir_fd=None):
    if kind not in {"os.mkdir", "os.makedirs"} or resolved is None or _write_allowed(resolved, kind):
        return False
    try:
        if not resolved.is_dir():
            return False
    except OSError:
        return False
    _record_write_event(kind + ".existing_directory_probe", target, resolved, False, "probe_allow")
    return True


def _audit_write(kind, target, *, dir_fd=None, follow_final=True):
    if isinstance(target, int) and _anonymous_pipe_fd(target):
        _record_write_event(kind + ".anonymous_pipe", target, None, True, "nonfilesystem_allow")
        return None
    resolved = _resolved_path(target, dir_fd, follow_final=follow_final)
    if isinstance(target, int) and resolved == Path(os.devnull).resolve():
        _record_write_event(kind + ".device", target, resolved, False, "device_allow")
        return resolved
    allowed = _write_allowed(resolved, kind)
    action = "allow" if allowed and not _BLOCK_WRITES else "block"
    _record_write_event(kind, target, resolved, allowed, action)
    if action == "block":
        item = {
            "action": "block",
            "allowed": allowed,
            "kind": kind,
            "resolved": str(resolved) if resolved is not None else None,
            "target": str(target),
        }
        _write_attempts.append(item)
        reason = "strict smoke" if _BLOCK_WRITES else "outside authorized temporary roots"
        raise RuntimeError("offline write audit blocked %s (%s): %s" % (kind, reason, target))
    return resolved


def _record_subprocess(command, cwd, instrumented):
    if not _WRITE_AUDIT:
        return
    _subprocess_events.append(
        {
            "command": str(command),
            "cwd": str(cwd) if cwd is not None else None,
            "instrumented": instrumented,
        }
    )


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
    if _write_mode(mode):
        _audit_write("builtins.open", file)
    return _ORIGINAL_OPEN(file, mode, *args, **kwargs)


def _guarded_io_open(file, mode="r", *args, **kwargs):
    if _write_mode(mode):
        _audit_write("io.open", file)
    return _ORIGINAL_IO_OPEN(file, mode, *args, **kwargs)


builtins.open = _guarded_open
io.open = _guarded_io_open


def _guarded_os_open(file, flags, *args, **kwargs):
    write_flags = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC
    if flags & write_flags:
        resolved = _resolved_path(file, kwargs.get("dir_fd"))
        if resolved == Path(os.devnull).resolve():
            _record_write_event("os.open.device", file, resolved, False, "device_allow")
        else:
            _audit_write("os.open", file, dir_fd=kwargs.get("dir_fd"))
    return _ORIGINAL_OS_OPEN(file, flags, *args, **kwargs)


os.open = _guarded_os_open


def _guarded_single_path_call(name):
    def call(*args, **kwargs):
        target = args[0] if args else name
        directory_fd = kwargs.get("dir_fd")
        if name in {"mkdir", "makedirs"}:
            resolved = _resolved_path(target, directory_fd)
            if _allow_existing_directory_probe("os." + name, target, resolved, dir_fd=directory_fd):
                return _ORIGINAL_FS[name](*args, **kwargs)
        _audit_write(
            "os." + name,
            target,
            dir_fd=kwargs.get("dir_fd"),
            follow_final=name not in {"remove", "unlink", "rmdir"},
        )
        return _ORIGINAL_FS[name](*args, **kwargs)
    return call


def _guarded_move_call(name):
    def call(*args, **kwargs):
        source = args[0] if args else name
        destination = args[1] if len(args) > 1 else name
        _audit_write(
            "os." + name + ".source",
            source,
            dir_fd=kwargs.get("src_dir_fd"),
            follow_final=False,
        )
        _audit_write(
            "os." + name + ".destination",
            destination,
            dir_fd=kwargs.get("dst_dir_fd"),
            follow_final=False,
        )
        return _ORIGINAL_FS[name](*args, **kwargs)
    return call


def _guarded_link_call(name):
    def call(*args, **kwargs):
        destination = args[1] if len(args) > 1 else name
        directory_fd = kwargs.get("dir_fd") if name == "symlink" else kwargs.get("dst_dir_fd")
        _audit_write("os." + name + ".destination", destination, dir_fd=directory_fd, follow_final=False)
        return _ORIGINAL_FS[name](*args, **kwargs)
    return call


def _child_path_values(raw, name):
    try:
        values = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("offline child policy is not valid JSON: " + name) from exc
    if not isinstance(values, list):
        raise RuntimeError("offline child policy must be a JSON list: " + name)
    result = []
    for value in values:
        if not isinstance(value, str) or not value:
            raise RuntimeError("offline child policy contains an invalid path: " + name)
        try:
            result.append(Path(value).expanduser().resolve(strict=False))
        except (OSError, RuntimeError, ValueError) as exc:
            raise RuntimeError("offline child policy path cannot be resolved: " + name) from exc
    return result


def _child_path_policy(name, raw, parent_values):
    values = _child_path_values(raw, name)
    for value in values:
        in_root = any(_within(value, root) for root in _WRITE_ROOTS)
        in_exact = any(value == path for path in parent_values)
        if not in_root and not in_exact:
            raise RuntimeError("offline child policy broadens " + name)
    return values


def _child_log_path(base, label):
    global _child_log_counter
    if not base:
        return ""
    path = Path(base)
    parent = path.parent.resolve(strict=False)
    if not any(_within(parent, root) for root in _WRITE_ROOTS):
        return ""
    _child_log_counter += 1
    return str(parent / (path.name + ".child-%d-%d-%s" % (os.getpid(), _child_log_counter, label)))


def _explicit_child_log_path(value, parent_value):
    if not value:
        return None
    path = Path(value)
    resolved = path.resolve(strict=False)
    if not any(_within(resolved.parent, root) for root in _WRITE_ROOTS):
        return None
    if parent_value:
        parent = Path(parent_value).resolve(strict=False)
        if resolved == parent:
            return None
    return str(path)


def _guarded_popen(*args, **kwargs):
    command = args[0] if args else kwargs.get("args")
    executable = kwargs.get("executable")
    command_text = command if command is not None else executable
    if isinstance(command, (list, tuple)) and command:
        command_text = command[0]
    command_value = _path_text(command_text) if command_text is not None else "<unknown>"
    cwd = kwargs.get("cwd")
    resolved_cwd = _resolved_path(cwd if cwd is not None else Path.cwd())
    cwd_allowed = _within(resolved_cwd, _EXECUTION_ROOT) if _EXECUTION_ROOT is not None else False
    cwd_allowed = cwd_allowed or any(_within(resolved_cwd, root) for root in _WRITE_ROOTS)
    if not cwd_allowed:
        _record_subprocess(command_value, resolved_cwd, False)
        raise RuntimeError("offline subprocess cwd outside authorized roots: " + str(cwd))
    command_name = str(command_value).lower().rsplit("/", 1)[-1]
    instrumented = command_name.startswith("python") or command_value == sys.executable
    _record_subprocess(command_value, resolved_cwd, instrumented)
    child_environment = kwargs.get("env")
    if child_environment is None:
        child_environment = dict(os.environ)
    else:
        child_environment = dict(child_environment)
    if child_environment is not None:
        child_environment.pop("PYTHONHOME", None)
        child_environment.pop("PYTHONUSERBASE", None)
        child_environment.pop("PYTHONSTARTUP", None)
        child_environment["MTF_OFFLINE_WRITE_ROOTS"] = json.dumps(
            [str(path) for path in _child_path_policy(
                "MTF_OFFLINE_WRITE_ROOTS",
                child_environment.get("MTF_OFFLINE_WRITE_ROOTS", os.environ.get("MTF_OFFLINE_WRITE_ROOTS", "[]")),
                _WRITE_ROOTS,
            )]
        )
        child_environment["MTF_OFFLINE_WRITE_PATHS"] = json.dumps(
            [str(path) for path in _child_path_policy(
                "MTF_OFFLINE_WRITE_PATHS",
                child_environment.get("MTF_OFFLINE_WRITE_PATHS", os.environ.get("MTF_OFFLINE_WRITE_PATHS", "[]")),
                _WRITE_PATHS,
            )]
        )
        child_environment["MTF_OFFLINE_WRITE_DIRS"] = json.dumps(
            [str(path) for path in _child_path_policy(
                "MTF_OFFLINE_WRITE_DIRS",
                child_environment.get("MTF_OFFLINE_WRITE_DIRS", os.environ.get("MTF_OFFLINE_WRITE_DIRS", "[]")),
                _WRITE_DIRS,
            )]
        )
        requested_audit = child_environment.get("MTF_OFFLINE_WRITE_AUDIT", "")
        child_environment["MTF_OFFLINE_WRITE_AUDIT"] = "1" if _WRITE_AUDIT or requested_audit == "1" else "0"
        requested_block = child_environment.get("MTF_OFFLINE_BLOCK_WRITES", "")
        if requested_block not in {"", "0", "1"}:
            raise RuntimeError("offline child policy has invalid block mode")
        child_environment["MTF_OFFLINE_BLOCK_WRITES"] = "1" if _BLOCK_WRITES or requested_block == "1" else "0"
        parent_execution = os.environ.get("MTF_OFFLINE_EXECUTION_ROOT", "")
        child_execution = child_environment.get("MTF_OFFLINE_EXECUTION_ROOT", parent_execution)
        if parent_execution and child_execution:
            child_execution_path = _resolved_path(child_execution)
            under_execution_root = _within(child_execution_path, _EXECUTION_ROOT)
            under_write_root = any(_within(child_execution_path, root) for root in _WRITE_ROOTS)
            if not under_execution_root and not under_write_root:
                raise RuntimeError("offline child policy broadens execution root")
            child_environment["MTF_OFFLINE_EXECUTION_ROOT"] = str(child_execution_path)
        else:
            child_environment["MTF_OFFLINE_EXECUTION_ROOT"] = parent_execution
        runtime_log = _explicit_child_log_path(
            child_environment.get("MTF_OFFLINE_RUNTIME_LOG", ""), _RUNTIME_LOG
        )
        write_log = _explicit_child_log_path(child_environment.get("MTF_OFFLINE_WRITE_LOG", ""), _WRITE_LOG)
        child_environment["MTF_OFFLINE_RUNTIME_LOG"] = runtime_log or _child_log_path(_RUNTIME_LOG, command_name)
        child_environment["MTF_OFFLINE_WRITE_LOG"] = write_log or _child_log_path(_WRITE_LOG, command_name)
        requested_pythonpath = child_environment.get("PYTHONPATH", os.environ.get("PYTHONPATH", ""))
        if instrumented:
            guard_path = os.environ.get("PYTHONPATH", "").split(os.pathsep)[0]
            child_environment["PYTHONPATH"] = (
                guard_path + os.pathsep + requested_pythonpath
                if guard_path and requested_pythonpath
                else guard_path or requested_pythonpath
            )
        else:
            child_environment["PYTHONPATH"] = requested_pythonpath
        kwargs["env"] = child_environment
    return _ORIGINAL_SUBPROCESS_POPEN(*args, **kwargs)


def _guarded_system(command):
    _record_subprocess("os.system", Path.cwd(), False)
    return _ORIGINAL_OS_SYSTEM(command)


_ORIGINAL_FS = {}
for _name in (
    "mkdir",
    "makedirs",
    "remove",
    "unlink",
    "rmdir",
    "mkfifo",
    "mknod",
    "truncate",
    "chmod",
    "utime",
    "chown",
):
    if hasattr(os, _name):
        _ORIGINAL_FS[_name] = getattr(os, _name)
        setattr(os, _name, _guarded_single_path_call(_name))
for _name in ("rename", "replace"):
    if hasattr(os, _name):
        _ORIGINAL_FS[_name] = getattr(os, _name)
        setattr(os, _name, _guarded_move_call(_name))
for _name in ("link", "symlink"):
    if hasattr(os, _name):
        _ORIGINAL_FS[_name] = getattr(os, _name)
        setattr(os, _name, _guarded_link_call(_name))
for _name in ("ftruncate", "fchmod", "fchown"):
    if hasattr(os, _name):
        _ORIGINAL_FS[_name] = getattr(os, _name)
        setattr(os, _name, _guarded_single_path_call(_name))

subprocess.Popen = _guarded_popen
os.system = _guarded_system


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
        "write_audit": {
            "allowed_roots": [str(path) for path in _WRITE_ROOTS],
            "authorized_directories": [str(path) for path in _WRITE_DIRS],
            "authorized_paths": [str(path) for path in _WRITE_PATHS],
            "allowed_writes": _allowed_write_count,
            "blocked_writes": _blocked_write_count,
            "device_opens": _device_open_count,
            "anonymous_pipe_fds": _non_filesystem_fd_count,
            "read_only_probes": _read_only_probe_count,
            "enabled": _WRITE_AUDIT,
            "event_count": _write_event_count,
            "events": _write_events,
            "events_truncated": _write_events_truncated,
            "scope": "Python filesystem hooks only; native filesystem I/O and child diagnostics are not aggregated",
            "hook_scope": [
                "builtins.open",
                "io.open",
                "os.open",
                "os.open /dev/null device handles (not filesystem writes)",
                "anonymous pipe file descriptors (fstat+procfd; regular/named FIFO remain fenced)",
                "os.mkdir/makedirs/remove/unlink/rmdir/mkfifo/mknod",
                "os.rename/replace",
                "os.link/symlink",
                "os.truncate/chmod/utime/chown",
                "os.ftruncate/fchmod/fchown",
                "subprocess.Popen cwd and Python-child propagation",
                "os.system (recorded as native/unverified)",
            ],
            "native_subprocesses_unverified": [
                event for event in _subprocess_events if not event["instrumented"]
            ],
            "subprocesses": _subprocess_events,
            "enforcement": "block_all_writes" if _BLOCK_WRITES else "block_outside_authorized_paths",
        },
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
    write_paths: Iterable[Path] = (),
    write_dirs: Iterable[Path] = (),
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
            WRITE_AUDIT_ENV: "1",
            WRITE_ROOTS_ENV: json.dumps([str(temp_root.resolve())]),
            WRITE_PATHS_ENV: json.dumps([str(path.resolve(strict=False)) for path in write_paths]),
            WRITE_DIRS_ENV: json.dumps([str(path.resolve(strict=False)) for path in write_dirs]),
            EXECUTION_ROOT_ENV: str(root.resolve()),
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
    write_paths: Iterable[Path] = (),
    write_dirs: Iterable[Path] = (),
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
        write_paths=write_paths,
        write_dirs=write_dirs,
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
    write_audit = runtime.get("write_audit") if isinstance(runtime, dict) else None
    audit_log_ok = isinstance(write_audit, dict) and write_audit.get("enabled") is True
    result["write_audit"] = write_audit if isinstance(write_audit, dict) else None
    result["guard_evidence"] = {
        "runtime_log": runtime_log_ok,
        "write_log": write_log_ok,
        "write_audit": audit_log_ok,
    }
    if not runtime_log_ok or not write_log_ok or not audit_log_ok:
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


def _reject_symlink_components(path: Path) -> None:
    """Reject a coverage output path that could escape through a symlink."""

    absolute = path.absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        try:
            if current.is_symlink():
                raise ValueError(f"coverage output path contains symlink: {path}")
        except OSError as exc:
            raise ValueError(f"coverage output path cannot be inspected: {path}") from exc


def _coverage_write_paths(root: Path, coverage_file: Path) -> tuple[Path, ...]:
    """Return the exact coverage file and SQLite sidecars the child may write."""

    candidate = coverage_file if coverage_file.is_absolute() else root / coverage_file
    candidate = candidate.absolute()
    _reject_symlink_components(candidate.parent)
    _reject_symlink_components(candidate)
    candidate.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink_components(candidate.parent)
    _reject_symlink_components(candidate)
    sidecars = tuple(candidate.with_name(candidate.name + suffix) for suffix in ("-wal", "-shm", "-journal"))
    for sidecar in sidecars:
        _reject_symlink_components(sidecar)
    return (candidate, *sidecars)


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
    coverage_paths = _coverage_write_paths(root, coverage_file) if coverage_file is not None else ()
    coverage_target = coverage_paths[0] if coverage_paths else None
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
            coverage_file=coverage_target,
            coverage_source=root / "mtf_lab" if coverage_target is not None else None,
            write_paths=coverage_paths,
            write_dirs=(coverage_target.parent,) if coverage_target is not None else (),
        )
    summary = _parse_summary(str(result.get("stdout", "")))
    result["summary"] = summary
    result["guard_evidence"] = result.get("guard_evidence", {"runtime_log": False, "write_log": False})
    result["guard_error"] = result.get("guard_error")
    if coverage_file is not None:
        result["coverage"] = {
            "enabled": True,
            "data_file": str(coverage_target),
            "data_file_exists": bool(coverage_target and coverage_target.is_file()),
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
        and import_result.get("guard_evidence", {}).get("write_audit")
        and not import_result.get("loaded_optional")
        and not import_result.get("network_attempts")
        and not import_result.get("write_attempts")
    )
    help_policy_ok = (
        help_result.get("guard_evidence", {}).get("runtime_log")
        and help_result.get("guard_evidence", {}).get("write_log")
        and help_result.get("guard_evidence", {}).get("write_audit")
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
            "write_audit": import_result.get("write_audit"),
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
            "write_audit": help_result.get("write_audit"),
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
        and bool(result.get("guard_evidence", {}).get("write_log"))
        and bool(result.get("guard_evidence", {}).get("write_audit"))
        and isinstance(result.get("write_audit"), dict)
        and result["write_audit"].get("blocked_writes") == 0
        and not result.get("write_attempts"),
        "stdout": result.get("stdout", ""),
        "stderr": result.get("stderr", ""),
        "network_attempts": result.get("network_attempts", []),
        "write_audit": result.get("write_audit"),
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
        and not result["suite"].get("write_attempts")
        and result["suite"].get("guard_evidence", {}).get("runtime_log")
        and result["suite"].get("guard_evidence", {}).get("write_log")
        and result["suite"].get("guard_evidence", {}).get("write_audit")
        and isinstance(result["suite"].get("write_audit"), dict)
        and result["suite"]["write_audit"].get("blocked_writes") == 0
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
