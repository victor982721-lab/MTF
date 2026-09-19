#!/usr/bin/env python3
"""Secure, read-only launcher for the canonical cTrader query runtime.

The project CLI intentionally resolves application credentials from environment
references.  This small stdlib-only bridge is the one supported local route for
the already-authorized query profile: it reads an explicitly named private
credential file, validates the durable DEMO selection, injects the two values
only into the child environment, and replaces itself with the active runtime.

It never accepts credentials or account identifiers on argv, never discovers a
credential by wildcard, never starts OAuth, and never permits fixture/trading
flags.  The child still performs fresh account discovery and revalidates the
selection against the DEMO server.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import tomllib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class LauncherError(RuntimeError):
    """A local safety or activation preflight failed."""


_TOKEN_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_PYTHON_ENV_KEYS = frozenset(
    {
        "PYTHONHOME",
        "PYTHONPATH",
        "PYTHONSTARTUP",
        "PYTHONWARNINGS",
        "PYTHONINSPECT",
        "PYTHONUSERBASE",
        "PYTHONEXECUTABLE",
    }
)
_CREDENTIALS_ENV = "MTF_LAB_CTRADER_CREDENTIALS_FILE"
_CLIENT_ID_ENV = "CTRADER_CLIENT_ID"
_CLIENT_SECRET_ENV = "CTRADER_CLIENT_SECRET"


def _uid() -> int:
    getter = getattr(os, "getuid", None)
    if getter is None:  # pragma: no cover - MTF is Linux/Kubuntu-only.
        raise LauncherError("no se pudo verificar el owner del archivo privado")
    return int(getter())


def _reject_symlink_chain(path: Path) -> None:
    """Reject a symlink at any component before opening a private path."""

    current = path
    chain: list[Path] = []
    while True:
        chain.append(current)
        if current.parent == current:
            break
        current = current.parent
    for item in reversed(chain):
        try:
            if item.is_symlink():
                raise LauncherError(f"ruta insegura (symlink): {item}")
        except OSError as exc:
            raise LauncherError(f"no se pudo inspeccionar la ruta privada: {item}") from exc


def _private_directory(path: Path, *, label: str) -> Path:
    path = path.expanduser()
    _reject_symlink_chain(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise LauncherError(f"{label} no está disponible") from exc
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != _uid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise LauncherError(f"{label} debe ser un directorio privado 0700 del usuario actual")
    return path


def _private_file(path: Path, *, label: str) -> Path:
    path = path.expanduser()
    _reject_symlink_chain(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise LauncherError(f"{label} no está disponible") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_uid != _uid() or stat.S_IMODE(info.st_mode) != 0o600:
        raise LauncherError(f"{label} debe ser un archivo regular privado 0600 del usuario actual")
    return path


def _private_executable(path: Path, *, label: str) -> Path:
    path = path.expanduser()
    _reject_symlink_chain(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise LauncherError(f"{label} no está disponible") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_uid != _uid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise LauncherError(f"{label} debe ser un archivo regular privado 0700 del usuario actual")
    if not os.access(path, os.X_OK):
        raise LauncherError(f"{label} no es ejecutable")
    return path


def _json_file(path: Path, *, label: str) -> Mapping[str, Any]:
    _private_file(path, label=label)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LauncherError(f"{label} no contiene JSON válido") from exc
    if not isinstance(value, Mapping):
        raise LauncherError(f"{label} debe ser un objeto JSON")
    return value


def _source_file(path: Path, *, label: str) -> Path:
    """Validate a repository input without imposing secret-file permissions."""

    path = path.expanduser()
    _reject_symlink_chain(path)
    try:
        info = path.lstat()
    except OSError as exc:
        raise LauncherError(f"{label} no está disponible") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_uid != _uid() or stat.S_IMODE(info.st_mode) & 0o002:
        raise LauncherError(f"{label} debe ser un archivo regular del usuario y no ser world-writable")
    return path


def _credentials_path(environ: Mapping[str, str]) -> Path:
    """Resolve one explicit reference; wildcard discovery is forbidden."""

    raw = str(environ.get(_CREDENTIALS_ENV, "")).strip()
    if not raw:
        raise LauncherError(f"define {_CREDENTIALS_ENV} con la ruta privada exacta; no se usa glob")
    path = Path(raw).expanduser()
    if path.name.startswith(".") or not path.name.startswith("ctrader-app-"):
        raise LauncherError("la referencia de credenciales no tiene el nombre canónico")
    if not path.name.endswith(".credentials.json"):
        raise LauncherError("la referencia de credenciales no tiene sufijo canónico")
    _private_directory(path.parent, label="directorio de credenciales")
    return _private_file(path, label="credenciales de aplicación")


def _credentials(environ: Mapping[str, str]) -> tuple[str, str]:
    value = _json_file(_credentials_path(environ), label="credenciales de aplicación")
    client_id = value.get("client_id")
    client_secret = value.get("client_secret")
    if not isinstance(client_id, str) or not client_id.strip() or any(char in client_id for char in "\x00\r\n"):
        raise LauncherError("credenciales de aplicación sin client_id válido")
    if (
        not isinstance(client_secret, str)
        or not client_secret.strip()
        or any(char in client_secret for char in "\x00\r\n")
    ):
        raise LauncherError("credenciales de aplicación sin client_secret válido")
    return client_id, client_secret


def _utc(value: Any, *, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise LauncherError(f"{label} no tiene timestamp UTC")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise LauncherError(f"{label} tiene timestamp inválido") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LauncherError(f"{label} requiere zona horaria")
    return parsed.astimezone(UTC)


def _account_id(value: Any) -> str:
    text = str(value).strip()
    if not text.isdigit() or int(text) <= 0:
        raise LauncherError("selección DEMO sin account_id válido")
    return text


def _account_records(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        raise LauncherError("discovery de cuentas sin lista accounts")
    records = tuple(item for item in value if isinstance(item, Mapping))
    if len(records) != len(value):
        raise LauncherError("discovery de cuentas contiene registros inválidos")
    return records


def _preflight_selection(root: Path) -> None:  # noqa: C901 - fail-closed activation boundary
    """Require an unexpired accounts token and a durable DEMO selection."""

    config_path = root / "config" / "ctrader_query.toml"
    _source_file(config_path, label="configuración cTrader")
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise LauncherError("no se pudo leer config/ctrader_query.toml") from exc
    if not isinstance(raw, Mapping):
        raise LauncherError("config cTrader inválida")
    profile = raw.get("ctrader")
    if not isinstance(profile, Mapping):
        raise LauncherError("config cTrader sin perfil ctrader")
    if str(profile.get("environment", "")).upper() != "DEMO":
        raise LauncherError("el launcher sólo admite environment=DEMO")
    if str(profile.get("operation_mode", "")).lower() != "query":
        raise LauncherError("el launcher sólo admite operation_mode=query")
    required_scopes = {str(item).strip().lower() for item in profile.get("required_scopes", ())}
    if required_scopes != {"accounts"}:
        raise LauncherError("el launcher exige exactamente scope accounts")
    token_ref = str(profile.get("token_ref", "")).strip()
    if not _TOKEN_REF.fullmatch(token_ref):
        raise LauncherError("token_ref de consulta inválido")
    token_dir_raw = profile.get("token_store_dir")
    if not isinstance(token_dir_raw, str) or not token_dir_raw.strip():
        raise LauncherError("token_store_dir no está configurado")
    token_dir = Path(token_dir_raw).expanduser()
    _private_directory(token_dir, label="token store")
    token = _json_file(token_dir / f"{token_ref}.json", label="token DEMO")
    metadata = token.get("metadata")
    if not isinstance(metadata, Mapping) or str(metadata.get("token_ref", "")) != token_ref:
        raise LauncherError("token DEMO no corresponde al token_ref del perfil")
    scopes = {str(item).strip().lower() for item in metadata.get("granted_scopes", ())}
    if scopes != {"accounts"}:
        raise LauncherError("el token DEMO no es exclusivamente accounts")
    if not isinstance(token.get("access_token"), str) or not str(token.get("access_token")).strip():
        raise LauncherError("token DEMO sin access_token almacenado")
    if _utc(metadata.get("expires_at"), label="token DEMO") <= datetime.now(UTC):
        raise LauncherError("token DEMO expirado")

    selection = _json_file(token_dir / "selection.json", label="selección DEMO")
    if str(selection.get("token_ref", "")) != token_ref or str(selection.get("environment", "")).upper() != "DEMO":
        raise LauncherError("selección durable no corresponde al perfil DEMO")
    selected_id = _account_id(selection.get("account_id"))
    config_selected = profile.get("account_selected", False)
    if not isinstance(config_selected, bool):
        raise LauncherError("account_selected debe ser booleano")
    if config_selected and _account_id(profile.get("account_id")) != selected_id:
        raise LauncherError("la cuenta configurada difiere de la selección durable DEMO")

    discovery = _json_file(token_dir / "account-discovery.json", label="discovery DEMO")
    if str(discovery.get("token_ref", "")) != token_ref:
        raise LauncherError("discovery durable no corresponde al token DEMO")
    if str(discovery.get("permissionScope", discovery.get("permission_scope", ""))).upper() != "SCOPE_VIEW":
        raise LauncherError("discovery durable no conserva scope de sólo lectura")
    records = _account_records(discovery.get("accounts"))
    matches = []
    for record in records:
        record_id = record.get("account_id", record.get("ctidTraderAccountId"))
        environment = str(record.get("environment", "")).upper()
        if record_id is not None and _account_id(record_id) == selected_id and environment == "DEMO":
            matches.append(record)
    if len(matches) != 1:
        raise LauncherError("la selección durable no coincide con exactamente una cuenta DEMO observada")


def _runtime_path() -> Path:
    return Path.home() / ".local" / "share" / "mtf-lab" / "runtime" / "runtime-python" / "bin" / "mtf-lab"


def _new_output_path(path: Path) -> Path:
    target = path.expanduser().absolute()
    _private_directory(target.parent, label="directorio de salida")
    if target.exists() or target.is_symlink():
        raise LauncherError("la salida ya existe; el launcher no sobrescribe archivos")
    # Normalize lexical aliases only after rejecting every symlink component.
    return target.resolve(strict=False)


def _query_args(argv: Sequence[str]) -> list[str]:
    parser = argparse.ArgumentParser(
        prog="mtf-lab-ctrader-query",
        description="consulta cTrader DEMO/accounts en sólo lectura con estado durable",
    )
    parser.add_argument("--report", type=Path)
    parser.add_argument("--capture", "--capture-output", dest="capture", type=Path)
    args = parser.parse_args(list(argv))
    command = ["ctrader", "query", "--network"]
    targets: set[Path] = set()
    if args.report is not None:
        report = _new_output_path(args.report)
        targets.add(report)
        command.extend(["--report", str(report)])
    if args.capture is not None:
        capture = _new_output_path(args.capture)
        if capture in targets:
            raise LauncherError("reporte y captura requieren destinos distintos")
        command.extend(["--capture", str(capture)])
    return command


def build_exec(
    argv: Sequence[str],
    *,
    root: Path,
    inherited: Mapping[str, str],
    credentials_path: Path,
    runtime_path: Path,
) -> tuple[Path, list[str], dict[str, str]]:
    """Build the exec tuple for tests without starting the child."""

    query_args = _query_args(argv)
    _preflight_selection(root)
    _private_directory(credentials_path.parent, label="directorio de credenciales")
    client_id, client_secret = _credentials({_CREDENTIALS_ENV: str(credentials_path)})
    runtime = _private_executable(runtime_path, label="runtime cTrader")
    config_path = root / "config" / "ctrader_query.toml"
    _reject_symlink_chain(config_path)
    if not config_path.is_file():
        raise LauncherError("configuración cTrader ausente")
    command = [str(runtime), *query_args, "--config", str(config_path)]
    child_env = dict(inherited)
    for key in _PYTHON_ENV_KEYS | {_CREDENTIALS_ENV, _CLIENT_ID_ENV, _CLIENT_SECRET_ENV}:
        child_env.pop(key, None)
    child_env["PYTHONNOUSERSITE"] = "1"
    # The canonical runtime wrapper invokes standard Unix utilities.  Do not
    # resolve them through a caller's project-local PATH while holding secrets.
    child_env["PATH"] = "/usr/bin:/bin"
    child_env[_CLIENT_ID_ENV] = client_id
    child_env[_CLIENT_SECRET_ENV] = client_secret
    return runtime, command, child_env


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv if argv is not None else sys.argv[1:])
    # Help and unsupported flags must not read the user's credential/token store.
    try:
        _query_args(arguments)
    except LauncherError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    root = Path(__file__).resolve().parents[1]
    inherited = dict(os.environ)
    raw_credentials = inherited.get(_CREDENTIALS_ENV, "").strip()
    if not raw_credentials:
        print(f"ERROR: define {_CREDENTIALS_ENV} con la ruta privada exacta; no se usa glob", file=sys.stderr)
        return 2
    try:
        runtime, command, child_env = build_exec(
            arguments,
            root=root,
            inherited=inherited,
            credentials_path=Path(raw_credentials).expanduser(),
            runtime_path=_runtime_path(),
        )
        os.execve(str(runtime), command, child_env)
    except LauncherError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"ERROR: no se pudo iniciar runtime cTrader ({type(exc).__name__})", file=sys.stderr)
        return 2
    return 0  # pragma: no cover - os.execve replaces the process.


if __name__ == "__main__":  # pragma: no cover - exercised through the launcher.
    raise SystemExit(main())
