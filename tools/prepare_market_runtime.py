#!/usr/bin/env python3
"""Prepare a private, relocatable MTF Lab runtime as STAGED_RUNTIME.

The source Python is copied without the unrelated packages in the Codex bundle.
The observed interpreter embeds SQLite, so this tool does not preload or copy a
system libsqlite3.  It only prepares local/offline MTF cTrader artifacts and
development tooling; it never installs a launcher, enables systemd, touches a
user database, or contacts a broker.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import email.parser
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_DEFAULT_RUNTIME_CACHE_ROOT = Path.home() / ".cache" / "codex-runtimes"
DEFAULT_SOURCE_PYTHON = _DEFAULT_RUNTIME_CACHE_ROOT / "codex-primary-runtime/dependencies/python/bin/python3"
DEFAULT_DESTINATION = Path("~/.local/share/mtf-lab/runtime")
DEFAULT_PROTOBUF_WHEEL = Path("runtime/implementation/dependencies/protobuf-7.36.1-cp310-abi3-manylinux2014_x86_64.whl")
DEFAULT_TYPES_PROTOBUF_WHEEL = Path(
    "runtime/implementation/dependencies/types_protobuf-7.35.1.20260906-py3-none-any.whl"
)
EXPECTED_PROTOBUF_SHA256 = "97198b77e369a0abd8e262b8f6c7266c55ddb796a3a12c76d7b8881188ed83aa"
MINIMUM_SAFE_SQLITE = (3, 51, 3)
SOURCE_CACHE_MARKER = f"{_DEFAULT_RUNTIME_CACHE_ROOT}{os.sep}"
PYTHON_LICENSE_NAME = "python-stdlib-LICENSE.txt"


class PreparationError(RuntimeError):
    """A concrete safety or compatibility gate prevented preparation."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    argv: tuple[str, ...]
    returncode: int
    elapsed_seconds: float
    log: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "argv": list(self.argv),
            "returncode": self.returncode,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "log": self.log,
        }


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def _lexists(path: Path) -> bool:
    return os.path.lexists(path)


def _private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def _fsync_dir(path: Path) -> None:
    with contextlib.suppress(OSError):
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _atomic_write(path: Path, payload: bytes, mode: int = 0o600) -> None:
    _private_dir(path.parent)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_dir(path.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        raise


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_write(path, (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _append_log(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)


def _clean_env(private: Path) -> dict[str, str]:
    home = private / "home"
    tmp = private / "tmp"
    cache = private / "xdg-cache"
    for directory in (home, tmp, cache):
        _private_dir(directory)
    inherited_path = os.environ.get("PATH", "/usr/bin:/bin")
    env = {
        "PATH": inherited_path,
        "HOME": str(home),
        "TMPDIR": str(tmp),
        "XDG_CACHE_HOME": str(cache),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONHASHSEED": "0",
        "PIP_CONFIG_FILE": os.devnull,
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PIP_NO_INPUT": "1",
    }
    for key in ("LANG", "LC_ALL"):
        if value := os.environ.get(key):
            env[key] = value
    return env


def run_logged(
    argv: Sequence[str | Path],
    *,
    log_path: Path,
    cwd: Path,
    env: Mapping[str, str],
    timeout: int = 900,
    check: bool = True,
) -> CommandResult:
    command = tuple(str(item) for item in argv)
    started = time.monotonic()
    _append_log(log_path, f"\n$ {shlex.join(command)}\n")
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=dict(env),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed = time.monotonic() - started
        _append_log(log_path, f"TIMEOUT after {elapsed:.3f}s\n{exc}\n")
        raise PreparationError(f"command timed out: {shlex.join(command)}") from exc
    elapsed = time.monotonic() - started
    _append_log(log_path, completed.stdout or "")
    _append_log(log_path, f"[returncode={completed.returncode} elapsed={elapsed:.3f}s]\n")
    result = CommandResult(command, completed.returncode, elapsed, str(log_path))
    if check and completed.returncode:
        raise PreparationError(f"command failed ({completed.returncode}): {shlex.join(command)}")
    return result


def run_json(
    argv: Sequence[str | Path],
    *,
    log_path: Path,
    cwd: Path,
    env: Mapping[str, str],
    timeout: int = 120,
) -> Any:
    command = tuple(str(item) for item in argv)
    started = time.monotonic()
    _append_log(log_path, f"\n$ {shlex.join(command)}\n")
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=dict(env),
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )
    elapsed = time.monotonic() - started
    _append_log(log_path, completed.stdout)
    if completed.stderr:
        _append_log(log_path, "\n[stderr]\n" + completed.stderr)
    _append_log(log_path, f"[returncode={completed.returncode} elapsed={elapsed:.3f}s]\n")
    if completed.returncode:
        raise PreparationError(f"JSON probe failed ({completed.returncode}); see {log_path}")
    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        raise PreparationError(f"JSON probe emitted no output; see {log_path}")
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise PreparationError(f"JSON probe emitted invalid output; see {log_path}") from exc


def _parse_version(value: str) -> tuple[int, int, int]:
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", value.strip())
    if match is None:
        raise PreparationError(f"cannot parse version {value!r}")
    return tuple(int(part) for part in match.groups())  # type: ignore[return-value]


def inspect_source_symlinks(source_root: Path) -> dict[str, Any]:
    total = broken = outside = 0
    examples: list[dict[str, str]] = []
    for path in source_root.rglob("*"):
        if not path.is_symlink():
            continue
        total += 1
        target_text = os.readlink(path)
        target = path.resolve(strict=False)
        state = "ok"
        if not target.exists():
            broken += 1
            state = "broken"
        elif not target.is_relative_to(source_root):
            outside += 1
            state = "outside"
        if len(examples) < 80:
            examples.append({"path": str(path.relative_to(source_root)), "target": target_text, "state": state})
    return {"total": total, "broken": broken, "outside": outside, "examples": examples}


def inspect_source_runtime(
    source_python: Path,
    *,
    source_root: Path,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> dict[str, Any]:
    if not source_python.is_file() or not os.access(source_python, os.X_OK):
        raise PreparationError(f"source Python is not an executable regular file: {source_python}")
    code = r"""
import importlib.util, json, sqlite3, sys, sysconfig
connection = sqlite3.connect(":memory:")
try:
    sqlite_version, sqlite_source_id = connection.execute(
        "select sqlite_version(), sqlite_source_id()"
    ).fetchone()
    compile_options = [row[0] for row in connection.execute("pragma compile_options")]
finally:
    connection.close()
spec = importlib.util.find_spec("_sqlite3")
print(json.dumps({
    "python_version": sys.version.split()[0],
    "executable": sys.executable,
    "prefix": sys.prefix,
    "base_prefix": sys.base_prefix,
    "sqlite_version": sqlite_version,
    "sqlite_source_id": sqlite_source_id,
    "sqlite_module_origin": None if spec is None else spec.origin,
    "sqlite_module_is_builtin": "_sqlite3" in sys.builtin_module_names,
    "compile_options": compile_options,
    "config_prefix": sysconfig.get_config_var("prefix"),
    "config_args": sysconfig.get_config_var("CONFIG_ARGS"),
}, sort_keys=True))
"""
    observed = run_json([source_python, "-c", code], cwd=cwd, env=env, log_path=log_path)
    if not isinstance(observed, dict):
        raise PreparationError("source runtime probe did not return an object")
    observed["source_python"] = str(source_python)
    observed["source_symlinks"] = inspect_source_symlinks(source_root)
    ldd = shutil.which("ldd")
    if ldd:
        observed["ldd"] = run_logged([ldd, source_python], cwd=cwd, env=env, log_path=log_path, check=False).as_dict()
    else:
        observed["ldd"] = {"available": False}
    sqlite_version = _parse_version(str(observed.get("sqlite_version", "")))
    if sqlite_version < MINIMUM_SAFE_SQLITE:
        raise PreparationError(
            f"gate sqlite-version-below-wal-fix: {observed.get('sqlite_version')} "
            f"< {'.'.join(map(str, MINIMUM_SAFE_SQLITE))}"
        )
    if not observed.get("sqlite_module_is_builtin") or observed.get("sqlite_module_origin") != "built-in":
        raise PreparationError("gate sqlite-linkage-not-embedded: _sqlite3 is not built-in")
    return observed


def _copy_entry(source: Path, destination: Path, *, source_root: Path) -> None:
    if source.is_symlink():
        target = source.resolve(strict=False)
        if not target.exists():
            raise PreparationError(f"broken source symlink: {source} -> {os.readlink(source)}")
        if not target.is_relative_to(source_root):
            raise PreparationError(f"source symlink escapes source root: {source} -> {target}")
        _copy_entry(target, destination, source_root=source_root)
    elif source.is_dir():
        _copy_tree(source, destination, source_root=source_root)
    elif source.is_file():
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        shutil.copy2(source, destination)
    else:
        raise PreparationError(f"unsupported source entry: {source}")


def _copy_tree(
    source: Path,
    destination: Path,
    *,
    source_root: Path,
    excluded: frozenset[str] = frozenset(),
) -> None:
    if destination.exists() and not destination.is_dir():
        raise PreparationError(f"destination collision: {destination}")
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(destination, 0o700)
    for entry in sorted(source.iterdir(), key=lambda item: item.name):
        if entry.name == "__pycache__" or entry.name in excluded:
            continue
        _copy_entry(entry, destination / entry.name, source_root=source_root)


def copy_python_base(source_root: Path, destination: Path, *, python_version: str) -> dict[str, Any]:
    _private_dir(destination)
    (destination / "bin").mkdir(mode=0o700)
    (destination / "lib").mkdir(mode=0o700)
    source_python = source_root / "bin/python3.12"
    if not source_python.is_file():
        raise PreparationError(f"source binary missing: {source_python}")
    shutil.copy2(source_python, destination / "bin/python3.12")
    os.chmod(destination / "bin/python3.12", 0o700)
    candidates = sorted((source_root / "lib").glob(f"python{_parse_version(python_version)[0]}.*"))
    stdlib = next((item for item in candidates if item.is_dir()), None)
    if stdlib is None:
        raise PreparationError(f"standard library missing under {source_root / 'lib'}")
    _copy_tree(
        stdlib, destination / "lib" / stdlib.name, source_root=source_root, excluded=frozenset({"site-packages"})
    )
    _private_dir(destination / "lib" / stdlib.name / "site-packages")
    for name in ("libpython3.12.so.1.0", "libpython3.so"):
        source_lib = source_root / "lib" / name
        if not source_lib.is_file():
            raise PreparationError(f"Python shared library missing: {source_lib}")
        shutil.copy2(source_lib, destination / "lib" / name)
    (destination / "lib/libpython3.12.so").symlink_to("libpython3.12.so.1.0")
    with zipfile.ZipFile(destination / "lib/python312.zip", "w"):
        pass
    os.chmod(destination / "lib/python312.zip", 0o600)
    return {
        "python_version": python_version,
        "source_stdlib": str(stdlib),
        "copied_libraries": ["libpython3.12.so.1.0", "libpython3.so"],
        "repaired_symlink": "lib/libpython3.12.so -> libpython3.12.so.1.0",
        "stdlib_zip": "lib/python312.zip",
    }


def _python_minor(version: str) -> str:
    parts = version.split(".")
    if len(parts) < 2:
        raise PreparationError(f"invalid Python version: {version}")
    return f"{parts[0]}.{parts[1]}"


def _python_wrapper(path: Path, *, entrypoint: str | None = None) -> None:
    command = 'exec "$SELF/.python3.12.bin" "$@"'
    if entrypoint:
        command = f'exec "$SELF/.python3.12.bin" "$ROOT/libexec/{entrypoint}" "$@"'
    payload = (
        "#!/bin/sh\n"
        "set -eu\n"
        'SELF=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)\n'
        'ROOT=$(CDPATH= cd -- "$SELF/../.." && pwd -P)\n'
        'export PYTHONHOME="$ROOT/base-python"\n'
        "export PYTHONNOUSERSITE=1\n"
        "export PYTHONDONTWRITEBYTECODE=1\n"
        f"{command}\n"
    )
    _atomic_write(path, payload.encode(), mode=0o700)


def _pip_wrapper(path: Path) -> None:
    payload = (
        "#!/bin/sh\n"
        "set -eu\n"
        'SELF=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)\n'
        'ROOT=$(CDPATH= cd -- "$SELF/../.." && pwd -P)\n'
        'export PYTHONHOME="$ROOT/base-python"\n'
        "export PYTHONNOUSERSITE=1\n"
        "export PYTHONDONTWRITEBYTECODE=1\n"
        'exec "$SELF/.python3.12.bin" -m pip "$@"\n'
    )
    _atomic_write(path, payload.encode(), mode=0o700)


def _write_activation_files(venv: Path) -> None:
    bin_dir = venv / "bin"
    _atomic_write(
        bin_dir / "activate",
        (
            b"# Relocatable MTF Lab environment\n"
            b'VIRTUAL_ENV=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)\n'
            b"export VIRTUAL_ENV\n"
            b'if [ -n "${_MTF_OLD_VIRTUAL_PATH+x}" ]; then\n'
            b'    PATH="${_MTF_OLD_VIRTUAL_PATH}"\n'
            b"else\n"
            b'    _MTF_OLD_VIRTUAL_PATH="${PATH:-}"\n'
            b"fi\n"
            b"export _MTF_OLD_VIRTUAL_PATH\n"
            b'PATH="$VIRTUAL_ENV/bin:${PATH:-}"\n'
            b"export PATH\n"
        ),
    )
    _atomic_write(
        bin_dir / "activate.fish",
        (
            b"# Relocatable MTF Lab environment\n"
            b"set -l _mtf_script_dir (dirname (status --current-filename))\n"
            b'set -gx VIRTUAL_ENV (realpath "$_mtf_script_dir/..")\n'
            b'set -gx PATH "$VIRTUAL_ENV/bin" $PATH\n'
        ),
    )
    _atomic_write(
        bin_dir / "activate.csh",
        (
            b"# Relocatable MTF Lab environment\n"
            b"set _mtf_script_dir = $0:h\n"
            b'setenv VIRTUAL_ENV `cd "$_mtf_script_dir/.." && pwd`\n'
            b'setenv PATH "$VIRTUAL_ENV/bin:$PATH"\n'
        ),
    )
    _atomic_write(
        bin_dir / "Activate.ps1",
        (
            b"# Relocatable MTF Lab environment\n"
            b"$env:VIRTUAL_ENV = Split-Path -Parent $PSScriptRoot\n"
            b'$env:Path = "$env:VIRTUAL_ENV/bin;" + $env:Path\n'
        ),
    )


def _seed_pip(venv: Path, source_root: Path, *, python_minor: str) -> list[str]:
    source_site = source_root / "lib" / f"python{python_minor}" / "site-packages"
    destination_site = venv / "lib" / f"python{python_minor}" / "site-packages"
    _private_dir(destination_site)
    copied: list[str] = []
    for entry in sorted(source_site.iterdir(), key=lambda item: item.name):
        if entry.name == "pip" or entry.name.startswith("pip-") and entry.name.endswith(".dist-info"):
            _copy_entry(entry, destination_site / entry.name, source_root=source_root)
            copied.append(entry.name)
    if "pip" not in copied:
        raise PreparationError("source bundle does not contain pip")
    for name in ("pip", "pip3", f"pip{python_minor}"):
        _pip_wrapper(venv / "bin" / name)
    return copied


def _write_venv_cfg(venv: Path, base_root: Path, python_version: str) -> None:
    base_bin = base_root.resolve() / "bin"
    _atomic_write(
        venv / "pyvenv.cfg",
        (
            f"home = {base_bin}\n"
            "include-system-site-packages = false\n"
            f"version = {python_version}\n"
            f"executable = {base_bin / 'python3.12'}\n"
            "command = python3.12 -m venv --copies --without-pip\n"
        ).encode(),
    )


def retarget_venv(venv: Path, base_root: Path, *, python_version: str) -> None:
    """Materialize the canonical absolute base path needed by isolated mode."""

    _write_venv_cfg(venv, base_root, python_version)


def create_venv(
    base_python: Path,
    base_root: Path,
    venv: Path,
    *,
    python_version: str,
    source_root: Path,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> dict[str, Any]:
    if _lexists(venv):
        raise PreparationError(f"venv collision: {venv}")
    result = run_logged(
        [base_python, "-m", "venv", "--copies", "--without-pip", venv],
        cwd=cwd,
        env=env,
        log_path=log_path,
    )
    cfg = venv / "pyvenv.cfg"
    if not cfg.is_file():
        raise PreparationError(f"pyvenv.cfg missing: {venv}")
    _write_venv_cfg(venv, base_root, python_version)
    hidden = venv / "bin/.python3.12.bin"
    shutil.copy2(venv / "bin/python3.12", hidden)
    os.chmod(hidden, 0o700)
    for name in ("python", "python3", "python3.12"):
        _python_wrapper(venv / "bin" / name)
    seeded = _seed_pip(venv, source_root, python_minor=_python_minor(python_version))
    _write_activation_files(venv)
    return {"command": result.as_dict(), "seeded_pip_entries": seeded}


def patch_entrypoints(venv: Path, *, root: Path, venv_name: str) -> list[str]:
    libexec = root / "libexec" / venv_name
    _private_dir(libexec)
    moved: list[str] = []
    for path in sorted((venv / "bin").iterdir(), key=lambda item: item.name):
        if not path.is_file() or path.name.startswith(".") or path.name.startswith("pip"):
            continue
        if path.name in {
            "python",
            "python3",
            "python3.12",
            "activate",
            "activate.csh",
            "activate.fish",
            "Activate.ps1",
        }:
            continue
        try:
            data = path.read_bytes()
            first, separator, remainder = data.partition(b"\n")
            shebang = first.decode("utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        shell_polyglot = (
            shebang == "#!/bin/sh"
            and remainder.startswith(b"'''exec'")
            and b".python3.12.bin" in remainder.split(b"\n", 1)[0]
        )
        if not separator or not shebang.startswith("#!") or ("python" not in shebang.lower() and not shell_polyglot):
            continue
        # A body named ``mypy.py`` or ``pyright.py`` would shadow the imported
        # package because Python puts the script directory at sys.path[0].
        body = libexec / f"__mtf_entrypoint_{path.name}.py"
        _atomic_write(body, remainder)
        _python_wrapper(path, entrypoint=f"{venv_name}/{body.name}")
        moved.append(path.name)
    return moved


def _archive_metadata(path: Path) -> tuple[dict[str, Any], list[tuple[str, bytes]]]:
    if path.suffix == ".whl" or path.suffix == ".zip":
        try:
            with zipfile.ZipFile(path) as archive:
                names = archive.namelist()
                metadata_names = sorted(name for name in names if name.endswith(".dist-info/METADATA"))
                if not metadata_names:
                    raise PreparationError(f"archive metadata missing: {path}")
                metadata_name = metadata_names[0]
                headers = email.parser.Parser().parsestr(archive.read(metadata_name).decode("utf-8"))
                dist_info = metadata_name.rsplit("/", 1)[0]
                license_names = sorted(
                    {
                        name
                        for name in names
                        if not name.endswith("/")
                        and (name.startswith(dist_info + "/licenses/") or _is_license_name(Path(name).name))
                    }
                )
                metadata = {
                    "name": headers.get("Name", ""),
                    "version": headers.get("Version", ""),
                    "license_declared": headers.get("License-Expression") or headers.get("License"),
                }
                return metadata, [(name, archive.read(name)) for name in license_names]
        except zipfile.BadZipFile as exc:
            raise PreparationError(f"invalid wheel/archive: {path}") from exc
    if path.name.endswith((".tar.gz", ".tar.bz2", ".tar.xz", ".tgz")):
        with tarfile.open(path, "r:*") as archive:
            pkg_info = next((item for item in archive.getmembers() if item.name.endswith("/PKG-INFO")), None)
            if pkg_info is None:
                return {"name": path.name, "version": "", "license_declared": None}, []
            stream = archive.extractfile(pkg_info)
            headers = email.parser.Parser().parsestr("" if stream is None else stream.read().decode("utf-8", "replace"))
            licenses: list[tuple[str, bytes]] = []
            for member in archive.getmembers():
                if not member.isfile() or not _is_license_name(Path(member.name).name):
                    continue
                stream = archive.extractfile(member)
                if stream is not None:
                    licenses.append((member.name, stream.read()))
            return {
                "name": headers.get("Name", path.name),
                "version": headers.get("Version", ""),
                "license_declared": headers.get("License-Expression") or headers.get("License"),
            }, licenses
    return {"name": path.name, "version": "", "license_declared": None}, []


def _is_license_name(name: str) -> bool:
    return name.lower().startswith(("license", "copying", "notice", "copyright"))


def _archive_label(metadata: Mapping[str, Any], fallback: str) -> str:
    value = f"{metadata.get('name', '')}-{metadata.get('version', '')}".strip("-") or fallback
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def preserve_archive(
    archive: Path,
    *,
    root: Path,
    relative_path: str,
    origin: str,
    kind: str,
) -> dict[str, Any]:
    metadata, licenses = _archive_metadata(archive)
    target = root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if archive.resolve() != target.resolve():
        shutil.copy2(archive, target)
    os.chmod(target, 0o600)
    label = _archive_label(metadata, archive.stem)
    license_paths: list[str] = []
    for member_name, payload in licenses:
        member = Path(member_name)
        if any(part in {"", ".", ".."} for part in member.parts):
            raise PreparationError(f"unsafe license member: {member_name}")
        license_target = root / "licenses" / label / member
        _atomic_write(license_target, payload)
        license_paths.append(str(license_target.relative_to(root)))
    return {
        **metadata,
        "kind": kind,
        "path": str(target.relative_to(root)),
        "sha256": sha256_file(target),
        "size": target.stat().st_size,
        "origin": origin,
        "license_files": sorted(license_paths),
    }


def _copy_source_license(source_root: Path, root: Path, python_version: str) -> dict[str, Any]:
    source = source_root / "lib" / f"python{_python_minor(python_version)}" / "LICENSE.txt"
    if not source.is_file():
        raise PreparationError(f"Python license missing: {source}")
    target = root / "licenses/python-stdlib-LICENSE.txt"
    _atomic_write(target, source.read_bytes())
    return {
        "name": "Python",
        "version": python_version,
        "path": str(target.relative_to(root)),
        "sha256": sha256_file(target),
        "size": target.stat().st_size,
        "origin": str(source),
        "license_declared": "Python Software Foundation License",
    }


def _repo_file(repo_root: Path, value: Path, label: str) -> Path:
    candidate = value if value.is_absolute() else repo_root / value
    resolved = candidate.resolve(strict=True)
    if not resolved.is_file() or resolved.is_symlink() or not resolved.is_relative_to(repo_root):
        raise PreparationError(f"{label} must be a regular file under the repository: {resolved}")
    return resolved


def _copy_provenance(source: Path, root: Path, relative: str) -> str:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    shutil.copy2(source, target)
    os.chmod(target, 0o600)
    return str(target.relative_to(root))


def _copy_project(repo_root: Path, target: Path) -> None:
    target.mkdir(mode=0o700)
    for name in ("pyproject.toml", "README.md"):
        shutil.copy2(repo_root / name, target / name)
    _copy_tree(repo_root / "mtf_lab", target / "mtf_lab", source_root=repo_root)


def build_mtf_wheel(
    repo_root: Path,
    *,
    dev_python: Path,
    output: Path,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> Path:
    source_copy = output.parent / "build-source"
    _copy_project(repo_root, source_copy)
    output.mkdir(mode=0o700)
    run_logged(
        [dev_python, "-m", "build", "--wheel", "--no-isolation", "--outdir", output],
        cwd=source_copy,
        env=env,
        log_path=log_path,
        timeout=900,
    )
    wheels = sorted(output.glob("*.whl"))
    if len(wheels) != 1:
        raise PreparationError(f"expected one MTF wheel, found {wheels}")
    return wheels[0]


def download_dev_wheels(
    *,
    dev_python: Path,
    lock_file: Path,
    root: Path,
    log_dir: Path,
    cwd: Path,
    env: Mapping[str, str],
) -> tuple[Path, dict[str, Any]]:
    work = root / "download-work"
    binary = work / "binary"
    fallback = work / "public-fallback"
    for directory in (work, binary, fallback):
        _private_dir(directory)
    common: list[str | Path] = [
        dev_python,
        "-m",
        "pip",
        "download",
        "--isolated",
        "--disable-pip-version-check",
        "--dest",
    ]
    binary_result = run_logged(
        [
            *common,
            binary,
            "--only-binary=:all:",
            "--index-url",
            "https://pypi.org/simple",
            "--requirement",
            lock_file,
        ],
        cwd=cwd,
        env=env,
        log_path=log_dir / "pip-download-binary.log",
        check=False,
        timeout=900,
    )
    selected = binary
    method = "pypi-public-binary"
    fallback_result: CommandResult | None = None
    if binary_result.returncode:
        fallback_result = run_logged(
            [
                *common,
                fallback,
                "--index-url",
                "https://pypi.org/simple",
                "--requirement",
                lock_file,
            ],
            cwd=cwd,
            env=env,
            log_path=log_dir / "pip-download-fallback.log",
            check=False,
            timeout=900,
        )
        if fallback_result.returncode:
            raise PreparationError("gate dev-artifacts-unavailable: public PyPI attempts failed")
        selected = fallback
        method = "pypi-public-fallback"
    archives = sorted(
        item
        for item in selected.iterdir()
        if item.is_file()
        and (item.suffix in {".whl", ".zip"} or item.name.endswith((".tar.gz", ".tar.bz2", ".tar.xz", ".tgz")))
    )
    if not archives:
        raise PreparationError(f"PyPI returned no artifacts in {selected}")
    artifact_dir = root / "dependencies/dev"
    _private_dir(artifact_dir)
    records: list[dict[str, Any]] = []
    for archive in archives:
        record = preserve_archive(
            archive,
            root=root,
            relative_path=f"dependencies/dev/{archive.name}",
            origin="https://pypi.org/simple/ (public package index; no advisory service)",
            kind="development",
        )
        records.append(record)
    shutil.rmtree(work, ignore_errors=True)
    return artifact_dir, {
        "method": method,
        "binary_attempt": binary_result.as_dict(),
        "fallback_attempt": None if fallback_result is None else fallback_result.as_dict(),
        "archives": records,
        "lock": str(lock_file),
    }


def _install_named(
    python: Path,
    requirement: str,
    *,
    find_links: Path,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> CommandResult:
    return run_logged(
        [
            python,
            "-m",
            "pip",
            "install",
            "--isolated",
            "--disable-pip-version-check",
            "--no-index",
            "--find-links",
            find_links,
            requirement,
        ],
        cwd=cwd,
        env=env,
        log_path=log_path,
        timeout=900,
    )


def _install_lock(
    python: Path,
    lock_file: Path,
    *,
    find_links: Path,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> CommandResult:
    return run_logged(
        [
            python,
            "-m",
            "pip",
            "install",
            "--isolated",
            "--disable-pip-version-check",
            "--no-index",
            "--find-links",
            find_links,
            "--requirement",
            lock_file,
        ],
        cwd=cwd,
        env=env,
        log_path=log_path,
        timeout=900,
    )


def _pip_check(python: Path, *, cwd: Path, env: Mapping[str, str], log_path: Path) -> CommandResult:
    return run_logged(
        [python, "-m", "pip", "check"],
        cwd=cwd,
        env=env,
        log_path=log_path,
        timeout=120,
    )


def _relocated_import_code(source_marker: str) -> str:
    return f"""
import importlib, json, sqlite3, sys
import mtf_lab
import google.protobuf
generated = importlib.import_module("mtf_lab.data.protobuf_generated.OpenApiMessages_pb2")
connection = sqlite3.connect(":memory:")
try:
    version, source_id = connection.execute("select sqlite_version(), sqlite_source_id()").fetchone()
finally:
    connection.close()
print(json.dumps({{
    "executable": sys.executable,
    "prefix": sys.prefix,
    "base_prefix": sys.base_prefix,
    "path": sys.path,
    "mtf_version": mtf_lab.__version__,
    "protobuf_version": google.protobuf.__version__,
    "generated_module": generated.__name__,
    "sqlite_version": version,
    "sqlite_source_id": source_id,
    "source_cache_in_path": any({source_marker!r} in str(item) for item in sys.path),
}}, sort_keys=True))
"""


def validate_imports(
    python: Path,
    *,
    expected_prefix: Path,
    expected_base: Path,
    source_marker: str,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> dict[str, Any]:
    value = run_json(
        [python, "-c", _relocated_import_code(source_marker)],
        cwd=cwd,
        env=env,
        log_path=log_path,
    )
    if not isinstance(value, dict):
        raise PreparationError("import probe did not return an object")
    prefix = Path(str(value.get("prefix", ""))).resolve()
    base = Path(str(value.get("base_prefix", ""))).resolve()
    if prefix != expected_prefix.resolve() or base != expected_base.resolve():
        raise PreparationError(f"gate venv-prefix-mismatch: {prefix=} {base=}")
    if value.get("source_cache_in_path"):
        raise PreparationError("gate source-cache-in-sys-path")
    if _parse_version(str(value.get("sqlite_version", ""))) < MINIMUM_SAFE_SQLITE:
        raise PreparationError("gate relocated-sqlite-version")
    return value


def validate_isolated(
    python: Path,
    *,
    expected_prefix: Path,
    expected_base: Path,
    source_marker: str,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> dict[str, Any]:
    """Run the same import/SQLite probe with CPython isolated mode enabled."""

    value = run_json(
        [python, "-I", "-B", "-c", _relocated_import_code(source_marker)],
        cwd=cwd,
        env=env,
        log_path=log_path,
    )
    if not isinstance(value, dict):
        raise PreparationError("isolated import probe did not return an object")
    prefix = Path(str(value.get("prefix", ""))).resolve()
    base = Path(str(value.get("base_prefix", ""))).resolve()
    if prefix != expected_prefix.resolve() or base != expected_base.resolve():
        raise PreparationError(f"gate isolated-venv-prefix-mismatch: {prefix=} {base=}")
    if value.get("source_cache_in_path"):
        raise PreparationError("gate isolated-source-cache-in-sys-path")
    if _parse_version(str(value.get("sqlite_version", ""))) < MINIMUM_SAFE_SQLITE:
        raise PreparationError("gate isolated-sqlite-version")
    return value


def validate_pythonpath_guard(
    python: Path,
    *,
    parent: Path,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> dict[str, Any]:
    """Prove an intentional PYTHONPATH sitecustomize guard is not stripped."""

    guard = Path(tempfile.mkdtemp(prefix="mtf-runtime-guard-", dir=parent))
    os.chmod(guard, 0o700)
    sentinel = guard / "loaded"
    try:
        (guard / "sitecustomize.py").write_text(
            "import os\nfrom pathlib import Path\n"
            "Path(os.environ['MTF_GUARD_SENTINEL']).write_text('loaded', encoding='ascii')\n",
            encoding="utf-8",
        )
        probe_env = dict(env)
        probe_env["PYTHONPATH"] = str(guard)
        probe_env["MTF_GUARD_SENTINEL"] = str(sentinel)
        value = run_json(
            [
                python,
                "-B",
                "-c",
                "import json, sys; print(json.dumps({'path': sys.path}, sort_keys=True))",
            ],
            cwd=cwd,
            env=probe_env,
            log_path=log_path,
        )
        if not isinstance(value, dict) or str(guard) not in [str(item) for item in value.get("path", [])]:
            raise PreparationError("gate pythonpath-guard-not-visible")
        if not sentinel.is_file() or sentinel.read_text(encoding="ascii") != "loaded":
            raise PreparationError("gate pythonpath-sitecustomize-not-loaded")
        return {"guard_path": str(guard), "sitecustomize_loaded": True, "path_visible": True}
    finally:
        shutil.rmtree(guard, ignore_errors=True)


def validate_proc_maps(
    python: Path,
    *,
    source_marker: str,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> dict[str, Any]:
    code = """
import importlib, os, sqlite3, time
import mtf_lab, google.protobuf
importlib.import_module("mtf_lab.data.protobuf_generated.OpenApiMessages_pb2")
sqlite3.connect(":memory:").close()
print(os.getpid(), flush=True)
time.sleep(2.0)
"""
    command = [str(python), "-c", code]
    started = time.monotonic()
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=dict(env),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        if process.stdout is None:
            raise PreparationError("gate proc-maps-no-stdout")
        pid_line = process.stdout.readline().strip()
        if not pid_line.isdigit():
            stderr = "" if process.stderr is None else process.stderr.read()
            raise PreparationError(f"gate proc-maps-probe-failed: {stderr[:400]}")
        pid = int(pid_line)
        maps_path = Path(f"/proc/{pid}/maps")
        if not maps_path.is_file():
            raise PreparationError("gate proc-maps-unavailable")
        maps = maps_path.read_text(encoding="utf-8", errors="replace")
        hits = [line for line in maps.splitlines() if source_marker in line]
        if hits:
            raise PreparationError("gate source-cache-in-proc-maps")
        return {"pid": pid, "elapsed_seconds": round(time.monotonic() - started, 3), "cache_hits": []}
    finally:
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=5)
        if process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            process.wait(timeout=5)
        _append_log(log_path, f"\n$ {shlex.join(command)}\nproc_maps_pid={process.pid}\n")


def validate_sqlite_scratch(
    python: Path,
    *,
    parent: Path,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> dict[str, Any]:
    scratch = Path(tempfile.mkdtemp(prefix="mtf-sqlite-scratch-", dir=parent))
    os.chmod(scratch, 0o700)
    code = """
import json, pathlib, sqlite3, sys
root = pathlib.Path(sys.argv[1])
database = root / "scratch.sqlite3"
connection = sqlite3.connect(database)
try:
    journal = connection.execute("pragma journal_mode=wal").fetchone()[0]
    connection.execute("create table t (id integer primary key, value text)")
    connection.execute("insert into t(value) values ('private-scratch')")
    connection.commit()
    count = connection.execute("select count(*) from t").fetchone()[0]
    version, source_id = connection.execute("select sqlite_version(), sqlite_source_id()").fetchone()
finally:
    connection.close()
print(json.dumps({"database": str(database), "journal_mode": journal, "count": count, "sqlite_version": version, "sqlite_source_id": source_id}, sort_keys=True))
"""
    try:
        value = run_json([python, "-c", code, scratch], cwd=cwd, env=env, log_path=log_path)
        if (
            not isinstance(value, dict)
            or str(value.get("journal_mode", "")).lower() != "wal"
            or value.get("count") != 1
        ):
            raise PreparationError(f"gate sqlite-scratch-wal: {value}")
        return value
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def validate_no_caches(root: Path, *, source_marker: str) -> dict[str, Any]:
    pycaches: list[str] = []
    external_symlinks: list[str] = []
    source_hits: list[str] = []
    for path in root.rglob("*"):
        if path == root / "STAGED_RUNTIME.json":
            continue
        if path.name == "__pycache__":
            pycaches.append(str(path.relative_to(root)))
        if path.is_symlink():
            target = path.resolve(strict=False)
            if not target.exists() or not target.is_relative_to(root):
                external_symlinks.append(str(path.relative_to(root)))
        if path.is_file() and path.stat().st_size <= 32 * 1024 * 1024 and source_marker.encode() in path.read_bytes():
            source_hits.append(str(path.relative_to(root)))
    if pycaches:
        raise PreparationError(f"gate pycache-present: {pycaches[:10]}")
    if external_symlinks:
        raise PreparationError(f"gate external-symlink-present: {external_symlinks[:10]}")
    if source_hits:
        raise PreparationError(f"gate source-cache-bytes-present: {source_hits[:10]}")
    return {"pycache_dirs": [], "external_symlinks": [], "source_cache_hits": []}


def remove_regenerable_caches(root: Path) -> dict[str, int]:
    removed_dirs = 0
    removed_files = 0
    for path in sorted(root.rglob("__pycache__"), key=lambda item: len(item.parts), reverse=True):
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path, ignore_errors=False)
            removed_dirs += 1
    for path in root.rglob("*.pyc"):
        if path.is_file() and not path.is_symlink():
            path.unlink()
            removed_files += 1
    return {"removed_pycache_dirs": removed_dirs, "removed_pyc_files": removed_files}


def _assert_no_current(destination: Path) -> None:
    if _lexists(destination / "CURRENT"):
        raise PreparationError(f"gate current-pointer-present: {destination / 'CURRENT'}")


def installed_distributions(
    python: Path,
    *,
    cwd: Path,
    env: Mapping[str, str],
    log_path: Path,
) -> list[dict[str, Any]]:
    code = r"""
import hashlib, importlib.metadata as metadata, json, pathlib

def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            value.update(chunk)
    return value.hexdigest()

items = []
for distribution in sorted(
    metadata.distributions(),
    key=lambda item: (item.metadata.get("Name") or "").lower(),
):
    hashes = []
    for relative in distribution.files or ():
        text = str(relative)
        if not text.endswith(("/METADATA", "/WHEEL", "/RECORD", "/direct_url.json")):
            continue
        path = pathlib.Path(distribution.locate_file(relative))
        if path.is_file():
            hashes.append({"path": text, "sha256": digest(path)})
    items.append({
        "name": distribution.metadata.get("Name"),
        "version": distribution.version,
        "license": distribution.metadata.get("License-Expression") or distribution.metadata.get("License"),
        "location": str(distribution.locate_file("")),
        "metadata_hashes": hashes,
    })
print(json.dumps(items, sort_keys=True))
"""
    value = run_json([python, "-c", code], cwd=cwd, env=env, log_path=log_path)
    if not isinstance(value, list):
        raise PreparationError("installed distribution probe did not return a list")
    return value


def _install_payload(
    *,
    repo_root: Path,
    source_root: Path,
    stage: Path,
    source: Mapping[str, Any],
    protobuf_source: Path,
    types_source: Path,
    lock_ctrader: Path,
    lock_dev: Path,
    runtime: Path,
    development: Path,
    scratch: Path,
    env: Mapping[str, str],
    log_dir: Path,
    manifest: dict[str, Any],
) -> None:
    python_version = str(source["python_version"])
    _copy_provenance(lock_ctrader, stage, "provenance/requirements-ctrader.lock")
    _copy_provenance(lock_dev, stage, "provenance/requirements-dev.lock")
    for source_file, relative in (
        (repo_root / "manifests/ctrader-protobuf-91.json", "provenance/ctrader-protobuf-91.json"),
        (repo_root / "manifests/sqlite-wal-review-20260913.json", "provenance/sqlite-wal-review-20260913.json"),
    ):
        if source_file.is_file():
            _copy_provenance(source_file, stage, relative)
    manifest["artifacts"].append(_copy_source_license(source_root, stage, python_version))

    if sha256_file(protobuf_source) != EXPECTED_PROTOBUF_SHA256:
        raise PreparationError(f"gate protobuf-wheel-hash for {protobuf_source}")
    local_links = stage / "dependencies/ctrader"
    _private_dir(local_links)
    manifest["artifacts"].append(
        preserve_archive(
            protobuf_source,
            root=stage,
            relative_path=f"dependencies/ctrader/{protobuf_source.name}",
            origin="repository-local runtime/implementation/dependencies (hash verified)",
            kind="runtime",
        )
    )
    if types_source.is_file():
        manifest["artifacts"].append(
            preserve_archive(
                types_source,
                root=stage,
                relative_path=f"dependencies/ctrader/{types_source.name}",
                origin="repository-local runtime/implementation/dependencies (optional typing artifact)",
                kind="development-optional",
            )
        )
        manifest["types_protobuf"] = {"available": True, "version": "7.35.1.20260906"}
    else:
        manifest["types_protobuf"] = {
            "available": False,
            "reason": "optional local types-protobuf wheel is absent; dev type stubs not installed",
        }

    dev_links, download = download_dev_wheels(
        dev_python=development / "bin/python",
        lock_file=lock_dev,
        root=stage,
        log_dir=log_dir,
        cwd=scratch,
        env=env,
    )
    manifest["artifacts"].extend(download["archives"])
    manifest["development_download"] = {key: value for key, value in download.items() if key != "archives"}
    _install_lock(
        development / "bin/python",
        lock_dev,
        find_links=dev_links,
        cwd=scratch,
        env=env,
        log_path=log_dir / "pip-install-dev.log",
    )
    _install_named(
        development / "bin/python",
        "protobuf==7.36.1",
        find_links=local_links,
        cwd=scratch,
        env=env,
        log_path=log_dir / "pip-install-dev-protobuf.log",
    )
    if types_source.is_file():
        _install_named(
            development / "bin/python",
            "types-protobuf==7.35.1.20260906",
            find_links=local_links,
            cwd=scratch,
            env=env,
            log_path=log_dir / "pip-install-types-protobuf.log",
        )

    mtf_wheel = build_mtf_wheel(
        repo_root,
        dev_python=development / "bin/python",
        output=stage / "dependencies/mtf",
        cwd=scratch,
        env=env,
        log_path=log_dir / "build-mtf-wheel.log",
    )
    manifest["artifacts"].append(
        preserve_archive(
            mtf_wheel,
            root=stage,
            relative_path=f"dependencies/mtf/{mtf_wheel.name}",
            origin="local MTF checkout copied to private build source",
            kind="project",
        )
    )
    shutil.rmtree(stage / "dependencies/build-source", ignore_errors=True)

    _install_named(
        runtime / "bin/python",
        "protobuf==7.36.1",
        find_links=local_links,
        cwd=scratch,
        env=env,
        log_path=log_dir / "pip-install-runtime-protobuf.log",
    )
    mtf_links = stage / "dependencies/mtf"
    for venv, log_name in (
        (runtime, "pip-install-runtime-mtf.log"),
        (development, "pip-install-dev-mtf.log"),
    ):
        _install_named(
            venv / "bin/python",
            "mtf-lab==0.1.0",
            find_links=mtf_links,
            cwd=scratch,
            env=env,
            log_path=log_dir / log_name,
        )
    runtime_info = manifest["python"]["runtime"]
    dev_info = manifest["python"]["development"]
    runtime_info["entrypoints_relocated"] = patch_entrypoints(runtime, root=stage, venv_name="runtime-python")
    dev_info["entrypoints_relocated"] = patch_entrypoints(development, root=stage, venv_name="dev-python")


def _validate_and_relocate(
    *,
    stage: Path,
    relocated: Path,
    base: Path,
    runtime: Path,
    development: Path,
    python_version: str,
    scratch: Path,
    env: Mapping[str, str],
    log_dir: Path,
    manifest: dict[str, Any],
) -> Path:
    checks = manifest["validations"]
    checks["removed_regenerable_caches"] = remove_regenerable_caches(stage)
    checks["initial_no_caches"] = validate_no_caches(stage, source_marker=SOURCE_CACHE_MARKER)
    checks["runtime_pip_check"] = _pip_check(
        runtime / "bin/python",
        cwd=scratch,
        env=env,
        log_path=log_dir / "pip-check-runtime.log",
    ).as_dict()
    checks["dev_pip_check"] = _pip_check(
        development / "bin/python",
        cwd=scratch,
        env=env,
        log_path=log_dir / "pip-check-dev.log",
    ).as_dict()
    checks["runtime_import"] = validate_imports(
        runtime / "bin/python",
        expected_prefix=runtime,
        expected_base=base,
        source_marker=SOURCE_CACHE_MARKER,
        cwd=scratch,
        env=env,
        log_path=log_dir / "runtime-import.log",
    )
    checks["dev_import"] = validate_imports(
        development / "bin/python",
        expected_prefix=development,
        expected_base=base,
        source_marker=SOURCE_CACHE_MARKER,
        cwd=scratch,
        env=env,
        log_path=log_dir / "dev-import.log",
    )
    checks["sqlite_scratch"] = validate_sqlite_scratch(
        runtime / "bin/python",
        parent=scratch,
        cwd=scratch,
        env=env,
        log_path=log_dir / "sqlite-scratch.log",
    )
    checks["proc_maps"] = validate_proc_maps(
        runtime / "bin/python",
        source_marker=SOURCE_CACHE_MARKER,
        cwd=scratch,
        env=env,
        log_path=log_dir / "proc-maps.log",
    )
    os.replace(stage, relocated)
    stage = relocated
    retarget_venv(stage / "runtime-python", stage / "base-python", python_version=python_version)
    retarget_venv(stage / "dev-python", stage / "base-python", python_version=python_version)
    checks["relocated_no_caches"] = validate_no_caches(stage, source_marker=SOURCE_CACHE_MARKER)
    checks["relocated_runtime_import"] = validate_imports(
        stage / "runtime-python/bin/python",
        expected_prefix=stage / "runtime-python",
        expected_base=stage / "base-python",
        source_marker=SOURCE_CACHE_MARKER,
        cwd=scratch,
        env=env,
        log_path=log_dir / "relocated-runtime-import.log",
    )
    checks["relocated_dev_import"] = validate_imports(
        stage / "dev-python/bin/python",
        expected_prefix=stage / "dev-python",
        expected_base=stage / "base-python",
        source_marker=SOURCE_CACHE_MARKER,
        cwd=scratch,
        env=env,
        log_path=log_dir / "relocated-dev-import.log",
    )
    checks["relocated_proc_maps"] = validate_proc_maps(
        stage / "runtime-python/bin/python",
        source_marker=SOURCE_CACHE_MARKER,
        cwd=scratch,
        env=env,
        log_path=log_dir / "relocated-proc-maps.log",
    )
    checks["relocated_sqlite_scratch"] = validate_sqlite_scratch(
        stage / "runtime-python/bin/python",
        parent=scratch,
        cwd=scratch,
        env=env,
        log_path=log_dir / "relocated-sqlite-scratch.log",
    )
    checks["installed_runtime"] = installed_distributions(
        stage / "runtime-python/bin/python",
        cwd=scratch,
        env=env,
        log_path=log_dir / "installed-runtime.log",
    )
    checks["installed_development"] = installed_distributions(
        stage / "dev-python/bin/python",
        cwd=scratch,
        env=env,
        log_path=log_dir / "installed-development.log",
    )
    return stage


def prepare_runtime(
    *,
    repo_root: Path,
    source_python: Path = DEFAULT_SOURCE_PYTHON,
    destination: Path = DEFAULT_DESTINATION,
    log_dir: Path | None = None,
) -> dict[str, Any]:
    """Build, relocate, validate, and publish a new private staged runtime."""

    repo_root = repo_root.resolve(strict=True)
    source_python = source_python.expanduser().resolve(strict=True)
    source_root = source_python.parent.parent
    destination = destination.expanduser().resolve(strict=False)
    log_dir = (log_dir or repo_root / "runtime/market-evidence/runtime-preparation").expanduser().resolve()
    if destination.name != "runtime":
        raise PreparationError(f"destination must end in runtime: {destination}")
    if _lexists(destination):
        raise PreparationError(f"staged destination already exists; refusing overwrite: {destination}")
    if not (repo_root / "mtf_lab").is_dir():
        raise PreparationError(f"MTF package root is missing: {repo_root / 'mtf_lab'}")
    lock_dev = _repo_file(repo_root, Path("requirements-dev.lock"), "development lock")
    lock_ctrader = _repo_file(repo_root, Path("requirements-ctrader.lock"), "cTrader lock")
    protobuf_source = _repo_file(repo_root, DEFAULT_PROTOBUF_WHEEL, "protobuf wheel")
    types_source = repo_root / DEFAULT_TYPES_PROTOBUF_WHEEL
    if types_source.exists():
        types_source = _repo_file(repo_root, DEFAULT_TYPES_PROTOBUF_WHEEL, "types-protobuf wheel")
    _private_dir(log_dir)
    _private_dir(destination.parent)

    run_id = f"{dt.datetime.now(dt.UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    scratch = Path(tempfile.mkdtemp(prefix="mtf-runtime-prep-"))
    os.chmod(scratch, 0o700)
    env = _clean_env(scratch)
    stage = destination.parent / f".runtime-staging-{run_id}"
    relocated = destination.parent / f".runtime-relocated-{run_id}"
    if _lexists(stage) or _lexists(relocated):
        raise PreparationError(f"temporary staging collision: {run_id}")
    _private_dir(stage)
    started = time.monotonic()
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "state": "STAGED_RUNTIME",
        "promotion_state": "NOT_PROMOTED",
        "current_pointer": None,
        "run_id": run_id,
        "prepared_at": _now(),
        "project": "mtf-lab",
        "destination": str(destination),
        "entrypoints": {
            "runtime_python": "runtime-python/bin/python",
            "dev_python": "dev-python/bin/python",
        },
        "safety": {
            "host_changes": False,
            "public_launcher_installed": False,
            "systemd_installed": False,
            "oauth_started": False,
            "broker_contacted": False,
            "user_database_touched": False,
        },
        "source": {},
        "artifacts": [],
        "validations": {},
        "logs": {},
    }
    try:
        source = inspect_source_runtime(
            source_python,
            source_root=source_root,
            cwd=scratch,
            env=env,
            log_path=log_dir / "source-inspection.log",
        )
        _write_json(log_dir / "source-inspection.json", source)
        manifest["source"] = source
        python_version = str(source["python_version"])
        manifest["sqlite"] = {
            "version": source["sqlite_version"],
            "source_id": source["sqlite_source_id"],
            "module_origin": source["sqlite_module_origin"],
            "linkage": "static-embedded-in-python-interpreter",
            "minimum_safe_version": ".".join(map(str, MINIMUM_SAFE_SQLITE)),
            "system_library_copied": False,
        }

        base = stage / "base-python"
        base_info = copy_python_base(source_root, base, python_version=python_version)
        runtime = stage / "runtime-python"
        development = stage / "dev-python"
        runtime_info = create_venv(
            base / "bin/python3.12",
            base,
            runtime,
            python_version=python_version,
            source_root=source_root,
            cwd=scratch,
            env=env,
            log_path=log_dir / "venv-runtime.log",
        )
        dev_info = create_venv(
            base / "bin/python3.12",
            base,
            development,
            python_version=python_version,
            source_root=source_root,
            cwd=scratch,
            env=env,
            log_path=log_dir / "venv-dev.log",
        )
        manifest["python"] = {
            "version": python_version,
            "base": base_info,
            "runtime": runtime_info,
            "development": dev_info,
        }
        _install_payload(
            repo_root=repo_root,
            source_root=source_root,
            stage=stage,
            source=source,
            protobuf_source=protobuf_source,
            types_source=types_source,
            lock_ctrader=lock_ctrader,
            lock_dev=lock_dev,
            runtime=runtime,
            development=development,
            scratch=scratch,
            env=env,
            log_dir=log_dir,
            manifest=manifest,
        )
        manifest["validations"]["sqlite_gate"] = manifest["sqlite"]
        manifest["elapsed_seconds_before_validation"] = round(time.monotonic() - started, 3)
        stage = _validate_and_relocate(
            stage=stage,
            relocated=relocated,
            base=base,
            runtime=runtime,
            development=development,
            python_version=python_version,
            scratch=scratch,
            env=env,
            log_dir=log_dir,
            manifest=manifest,
        )
        manifest["elapsed_seconds_before_publish"] = round(time.monotonic() - started, 3)
        manifest["logs"] = {"directory": str(log_dir)}
        _assert_no_current(destination)
        _write_json(stage / "STAGED_RUNTIME.json", manifest)
        os.replace(stage, destination)
        stage = destination
        retarget_venv(destination / "runtime-python", destination / "base-python", python_version=python_version)
        retarget_venv(destination / "dev-python", destination / "base-python", python_version=python_version)
        _assert_no_current(destination)
        manifest["validations"]["final_no_caches"] = validate_no_caches(destination, source_marker=SOURCE_CACHE_MARKER)
        manifest["validations"]["final_isolated_runtime"] = validate_isolated(
            destination / "runtime-python/bin/python",
            expected_prefix=destination / "runtime-python",
            expected_base=destination / "base-python",
            source_marker=SOURCE_CACHE_MARKER,
            cwd=scratch,
            env=env,
            log_path=log_dir / "final-isolated-runtime.log",
        )
        manifest["validations"]["final_isolated_dev"] = validate_isolated(
            destination / "dev-python/bin/python",
            expected_prefix=destination / "dev-python",
            expected_base=destination / "base-python",
            source_marker=SOURCE_CACHE_MARKER,
            cwd=scratch,
            env=env,
            log_path=log_dir / "final-isolated-dev.log",
        )
        manifest["validations"]["pythonpath_guard"] = validate_pythonpath_guard(
            destination / "runtime-python/bin/python",
            parent=scratch,
            cwd=scratch,
            env=env,
            log_path=log_dir / "pythonpath-guard.log",
        )
        manifest["published_at"] = _now()
        manifest["elapsed_seconds"] = round(time.monotonic() - started, 3)
        _write_json(destination / "STAGED_RUNTIME.json", manifest)
        _write_json(log_dir / "STAGED_RUNTIME.json", manifest)
        return manifest
    except BaseException as exc:
        _write_json(
            log_dir / f"failure-{run_id}.json",
            {
                "schema_version": 1,
                "state": "PREPARATION_FAILED",
                "run_id": run_id,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "stage": str(stage) if _lexists(stage) else None,
                "destination": str(destination),
            },
        )
        raise
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
        if _lexists(stage) and stage != destination:
            shutil.rmtree(stage, ignore_errors=True)
        if _lexists(relocated) and relocated != destination:
            shutil.rmtree(relocated, ignore_errors=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-python", type=Path, default=DEFAULT_SOURCE_PYTHON)
    parser.add_argument("--destination", type=Path, default=DEFAULT_DESTINATION)
    parser.add_argument("--log-dir", type=Path, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        manifest = prepare_runtime(
            repo_root=args.repo_root,
            source_python=args.source_python,
            destination=args.destination,
            log_dir=args.log_dir,
        )
    except PreparationError as exc:
        print(f"STAGED_RUNTIME blocked: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - defensive boundary
        print(f"STAGED_RUNTIME failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "state": manifest["state"],
                "runtime_python": manifest["entrypoints"]["runtime_python"],
                "dev_python": manifest["entrypoints"]["dev_python"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
