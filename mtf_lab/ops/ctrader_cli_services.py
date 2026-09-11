"""Application services for the cTrader-specific CLI commands.

The optional SDK is imported only inside the query path.  Local fixtures use
isolated temporary stores and the local demo transport; they never turn a
fixture into a gateway or account authorization.
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import socket
import ssl
import sys
import tempfile
import urllib.parse
import urllib.request
from typing import Any

from .application_services import (
    CommandResult,
    PROJECT_ROOT,
    _config_for,
)


def _json_error(payload: Mapping[str, Any], *, code: int = 2) -> CommandResult:
    return CommandResult.json(dict(payload), code=code, stderr=False)


def _ctrader_activation_payload(config: Any) -> dict[str, Any]:
    return {"ctrader": dict(config.ctrader), "ctrader_oauth": dict(config.ctrader_oauth)}


def _selection_state_path(config: Any) -> Path:
    from .ctrader_activation import ActivationProfile, SecureTokenStore

    profile = ActivationProfile.from_mapping(dict(config.ctrader))
    return SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT).root / "selection.json"


def _discovery_state_path(config: Any) -> Path:
    return _selection_state_path(config).with_name("account-discovery.json")


def _secret_free(value: Any) -> Any:
    forbidden = {"access_token", "accesstoken", "refresh_token", "refreshtoken", "client_secret", "clientsecret", "password"}
    if isinstance(value, Mapping):
        return {str(key): _secret_free(item) for key, item in value.items() if str(key).lower() not in forbidden}
    if isinstance(value, (list, tuple)):
        return [_secret_free(item) for item in value]
    return value


def _persist_discovery(config: Any, *, token_ref: str, observed: Mapping[str, Any], observed_at: datetime | None = None) -> Path:
    """Persist private account metadata, never access or refresh tokens."""
    from .ctrader_activation import SecureTokenStore

    guard = SecureTokenStore(_discovery_state_path(config).parent, project_root=PROJECT_ROOT)
    guard._prepare()
    target = guard.root / "account-discovery.json"
    temporary = guard.root / f".account-discovery.{os.getpid()}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temporary, flags, 0o600)
    try:
        raw_records = observed.get("records", ()) if isinstance(observed, Mapping) else ()
        payload = {
            "version": 1,
            "source": "ctrader-open-api",
            "token_ref": str(token_ref),
            "observed_at": (observed_at or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "permissionScope": observed.get("permissionScope") if isinstance(observed, Mapping) else None,
            "accounts": [_secret_free(dict(item)) for item in raw_records if isinstance(item, Mapping)],
        }
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        directory_fd = os.open(guard.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary.exists():
            temporary.unlink()
    return target


def _load_persisted_selection(config: Any) -> dict[str, Any] | None:
    """Read the explicit DEMO account selection from the private store."""
    try:
        profile_token_ref = str(config.ctrader.get("token_ref", ""))
        path = _selection_state_path(config)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        try:
            if (os.fstat(fd).st_mode & 0o777) != 0o600:
                return None
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                fd = -1
                raw = json.load(handle)
        finally:
            if fd >= 0:
                os.close(fd)
        if not isinstance(raw, Mapping) or raw.get("version") != 1:
            return None
        if str(raw.get("token_ref", "")) != profile_token_ref or str(raw.get("environment", "")).upper() != "DEMO":
            return None
        account_id = str(raw.get("account_id", "")).strip()
        if not account_id:
            return None
        return {"account_id": account_id, "account_selected": True, "environment": "DEMO"}
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _activation_payload_with_selection(config: Any) -> dict[str, Any]:
    payload = _ctrader_activation_payload(config)
    selected = _load_persisted_selection(config)
    if selected and not bool(payload["ctrader"].get("account_selected", False)):
        payload["ctrader"].update(selected)
    return payload


def _persist_selection(config: Any, *, account_id: str, token_ref: str, observed_at: datetime | None = None) -> Path:
    """Persist only explicit DEMO selection atomically outside the repo."""
    from .ctrader_activation import SecureTokenStore

    guard = SecureTokenStore(_selection_state_path(config).parent, project_root=PROJECT_ROOT)
    guard._prepare()
    target = guard.root / "selection.json"
    temporary = guard.root / f".selection.{os.getpid()}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temporary, flags, 0o600)
    try:
        payload = {
            "version": 1,
            "token_ref": str(token_ref),
            "account_id": str(account_id),
            "environment": "DEMO",
            "observed_at": (observed_at or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        directory_fd = os.open(guard.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary.exists():
            temporary.unlink()
    return target


def _ctrader_config(config: Any) -> Any:
    """Map the profile section to the provider's strict, secret-free config."""
    from ..data.ctrader import CTraderConfig

    raw = dict(config.ctrader)
    allowed = {
        "environment", "host", "port", "symbol", "symbol_id", "account_id", "client_id", "client_secret_ref",
        "access_token_ref", "refresh_token_ref", "quote_basis", "timeframes", "digits", "price_scale",
        "pip_position", "request_timeout_seconds", "max_reconnects", "reconnect_backoff_seconds",
        "reconnect_backoff_max_seconds", "heartbeat_seconds", "queue_maxsize", "historical_count",
        "request_rate_limit", "historical_rate_limit",
    }
    values = {key: value for key, value in raw.items() if key in allowed}
    values.setdefault("environment", "demo")
    values.setdefault("symbol", config.instrument)
    values.setdefault("quote_basis", "mid")
    values.setdefault("timeframes", tuple(tf.name for tf in config.timeframes))
    if values.get("account_id") in {"", None}:
        values.pop("account_id", None)
    elif isinstance(values.get("account_id"), str) and values["account_id"].isdigit():
        values["account_id"] = int(values["account_id"])
    oauth = dict(config.ctrader_oauth)
    values.setdefault("client_id", os.environ.get(str(oauth.get("client_id_env", ""))) if oauth.get("client_id_env") else None)
    values.setdefault("client_secret_ref", str(oauth.get("client_secret_env", "")) if oauth.get("client_secret_env") else None)
    if raw.get("token_ref"):
        values.setdefault("access_token_ref", str(raw.get("token_ref")))
    return CTraderConfig.from_mapping(values)


def _token_metadata_for_config(config: Any) -> Any:
    from .ctrader_activation import SecureTokenStore

    raw = dict(config.ctrader)
    token_ref = str(raw.get("token_ref", ""))
    token_dir = raw.get("token_store_dir")
    if not token_ref or not token_dir:
        return None
    try:
        root = SecureTokenStore(token_dir, project_root=PROJECT_ROOT)
        return root.metadata(token_ref)
    except Exception:
        return None


def _check_ctrader_connectivity(config: Any, *, timeout: float) -> dict[str, Any]:
    """Perform only an explicitly requested bounded DNS/TCP/TLS probe."""
    provider_config = _ctrader_config(config)
    host = str(provider_config.host)
    port = int(provider_config.port)
    result: dict[str, Any] = {
        "requested": True, "endpoint": f"{host}:{port}", "dns": "NOT_CHECKED", "tcp": "NOT_CHECKED",
        "tls": "NOT_CHECKED", "account_authorized": False,
    }
    addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    result["dns"] = "OK"
    result["address_count"] = len(addresses)
    raw = socket.create_connection((host, port), timeout=float(timeout))
    result["tcp"] = "OK"
    context = ssl.create_default_context()
    try:
        tls = context.wrap_socket(raw, server_hostname=host)
        try:
            result["tls"] = "OK"
            result["tls_version"] = tls.version()
        finally:
            tls.close()
    finally:
        try:
            raw.close()
        except OSError:
            pass
    result["note"] = "TLS de transporte solamente; no prueba OAuth, cuenta ni permisos."
    return result


def _fixture_oauth_response(*, refreshed: bool = False) -> dict[str, Any]:
    suffix = "refreshed" if refreshed else "exchange"
    return {
        "accessToken": f"fixture-access-{suffix}",
        "refreshToken": f"fixture-refresh-{suffix}",
        "expiresIn": 3600,
        "tokenType": "bearer",
    }


def _oauth_http_request(url: str, params: Mapping[str, str], timeout: float) -> Mapping[str, Any]:
    """POST form data to a validated OAuth endpoint without echoing secrets."""
    encoded = urllib.parse.urlencode(dict(params)).encode("utf-8")
    request = urllib.request.Request(str(url), data=encoded, method="POST", headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(request, timeout=float(timeout)) as response:
            body = response.read(128 * 1024)
    except Exception as exc:
        raise RuntimeError("solicitud OAuth falló; revise conectividad y estado de la aplicación") from exc
    try:
        parsed = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError("respuesta OAuth no es JSON válido") from exc
    if not isinstance(parsed, Mapping):
        raise RuntimeError("respuesta OAuth no es un objeto")
    return parsed


def _read_callback_input(args: argparse.Namespace) -> str:
    """Read a callback URI from protected local input, never a process arg."""
    callback_file = getattr(args, "callback_file", None)
    use_stdin = bool(getattr(args, "callback_stdin", False))
    if bool(callback_file) == use_stdin:
        raise ValueError("proporcione exactamente --callback-file (0600) o --callback-stdin")
    if use_stdin:
        value = sys.stdin.read(16_384)
    else:
        path = Path(callback_file).expanduser()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
        except OSError as exc:
            raise ValueError("no se pudo abrir callback-file de forma segura") from exc
        try:
            info = os.fstat(fd)
            if (info.st_mode & 0o777) != 0o600 or info.st_uid != os.getuid():
                raise ValueError("callback-file debe ser privado, del usuario actual y tener permisos exactos 0600")
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                fd = -1
                value = handle.read(16_384)
        finally:
            if fd >= 0:
                os.close(fd)
    value = value.strip()
    if not value or len(value) > 16_384:
        raise ValueError("callback local vacío o demasiado grande")
    return value


@dataclass
class QueryContext:
    config: Any
    metadata: Any
    status_before: Mapping[str, Any]
    profile: Any
    app: Any
    lease: Any
    client_secret: str
    sequence: list[str] | None = None
    network_performed: bool = False

    def __post_init__(self) -> None:
        if self.sequence is None:
            self.sequence = []



class CTraderCliService:
    """Compose cTrader CLI use cases while keeping optional SDK imports lazy."""

    def doctor(self, args: argparse.Namespace) -> CommandResult:
        from ..data.ctrader import dependency_report
        from .ctrader_commands import status_command

        config = _config_for(args)
        report = dependency_report()
        output: dict[str, Any] = {
            "ok": True, "diagnostic_ok": True, "provider": "ctrader_open_api", "dependency": report.to_dict(),
            "config_path": config.path, "network_performed": False, "browser_opened": False,
            "connector_available": report.available, "connector_usable": False, "configuration_pending": False,
            "account_authorized": False, "connectivity": {"requested": False, "state": "NOT_CHECKED", "account_authorized": False},
        }
        try:
            output["activation"] = self._activation_status(config, status_command)
            activation = output["activation"]
            state = str(activation.get("status", activation).get("state", "NOT_CONFIGURED"))
            output["configuration_pending"] = state in {"NOT_CONFIGURED", "APP_CREDENTIALS_REQUIRED", "ACCOUNTS_SCOPE_REQUIRED", "TOKEN_REFERENCE_REQUIRED", "TOKEN_EXPIRED"}
        except Exception as exc:
            output["activation"] = {"state": "INVALID_PROFILE", "ready": False, "next_action": "Corrija el perfil cTrader; no se modificó nada.", "error": type(exc).__name__}
            output["diagnostic_ok"] = False
        if getattr(args, "network", False):
            output["network_performed"] = True
            try:
                output["connectivity"] = _check_ctrader_connectivity(config, timeout=float(getattr(args, "timeout", 5.0)))
            except Exception as exc:
                output["connectivity"] = {"requested": True, "state": "ERROR", "account_authorized": False, "error": f"{type(exc).__name__}: {exc}", "note": "No se probó OAuth ni una cuenta; revise DNS/TCP/TLS."}
                output["diagnostic_ok"] = False
        output["connector_usable"] = bool(report.available and output.get("activation", {}).get("status", {}).get("ready", False) and output["connectivity"].get("tls") == "OK")
        output["ok"] = bool(output["diagnostic_ok"])
        return CommandResult.json(output, code=0 if output["diagnostic_ok"] else 2)

    @staticmethod
    def _activation_status(config: Any, status_command: Any) -> Mapping[str, Any]:
        if config.ctrader and config.ctrader_oauth:
            return status_command(_activation_payload_with_selection(config), token_metadata=_token_metadata_for_config(config), present_env_keys=set(os.environ), accounts=(), now=datetime.now(UTC))
        return {"state": "NOT_CONFIGURED", "ready": False, "next_action": "Use config/ctrader_query.toml o registre la aplicación; no se abrió navegador."}

    def auth_url(self, args: argparse.Namespace) -> CommandResult:
        from .ctrader_activation import ActivationProfile, LoopbackOAuthAssistant, OAuthAppConfig, OAuthAttemptStore, SecureTokenStore

        config = _config_for(args)
        if not config.ctrader_oauth or not config.ctrader:
            return _json_error({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "next_action": "Seleccione un perfil cTrader completo.", "browser_opened": False})
        app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
        profile = ActivationProfile.from_mapping(dict(config.ctrader))
        if not profile.enabled:
            return _json_error({"ok": False, "state": "DISABLED", "network_performed": False, "browser_opened": False, "next_action": "Habilite explícitamente el perfil antes de iniciar OAuth."})
        requested_scope = str(getattr(args, "scope", "") or ("trading" if profile.operation_mode.value == "DEMO" else "accounts")).strip().lower()
        if requested_scope not in profile.required_scopes:
            return _json_error({"ok": False, "state": "SCOPE_NOT_ALLOWED", "network_performed": False, "browser_opened": False, "next_action": "Solicite sólo un scope permitido por el perfil y autorícelo en una fase separada."})
        client_id = os.environ.get(app.client_id_env, "")
        if not client_id:
            return _json_error({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "missing": [app.client_id_env], "next_action": "Defina client_id localmente; nunca lo guarde en TOML.", "browser_opened": False})
        tokens = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
        attempts = OAuthAttemptStore(profile.token_store_dir, project_root=PROJECT_ROOT)
        assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens)
        attempt = assistant.begin(client_id=client_id, requested_scopes=[requested_scope], open_browser=False)
        handoff = assistant.resume_authorization(attempt.attempt_id, reveal_url=True, open_browser=bool(getattr(args, "open_browser", False)))
        return CommandResult.json({"ok": True, "attempt": attempt.to_public_dict(), "authorization_url": handoff["authorization_url"], "browser_opened": handoff["browser_opened"], "network_performed": False, "requested_scope": requested_scope, "next_action": "Complete OAuth y ejecute callback-listen/token-exchange; accounts y trading se autorizan en fases separadas."})

    def callback_listen(self, args: argparse.Namespace) -> CommandResult:
        from .ctrader_activation import ActivationProfile, LoopbackOAuthAssistant, OAuthAppConfig, OAuthAttemptStore, SecureTokenStore

        config = _config_for(args)
        if not config.ctrader_oauth or not config.ctrader:
            return _json_error({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False})
        if not getattr(args, "attempt_id", None):
            return _json_error({"ok": False, "state": "CALLBACK_REQUIRED", "network_performed": False, "next_action": "Proporcione --attempt-id devuelto por auth-url."})
        app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
        profile = ActivationProfile.from_mapping(dict(config.ctrader))
        try:
            tokens = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
            attempts = OAuthAttemptStore(profile.token_store_dir, project_root=PROJECT_ROOT)
            assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens)
            attempt = assistant.listen_callback(args.attempt_id, open_browser=bool(getattr(args, "open_browser", False)), timeout_seconds=float(getattr(args, "timeout", 600.0)))
        except Exception as exc:
            return _json_error({"ok": False, "state": type(exc).__name__, "network_performed": False, "next_action": "No se recibió un callback válido; reanude el intento sin exponer el código.", "error": str(exc)})
        return CommandResult.json({"ok": True, "attempt": attempt.to_public_dict(), "network_performed": False, "browser_opened": bool(getattr(args, "open_browser", False)), "next_action": "Ejecute token-exchange --attempt-id para intercambiar el código persistido; no se imprimió el código."})

    def token_exchange(self, args: argparse.Namespace) -> CommandResult:
        if getattr(args, "fixture", False):
            return self._fixture_exchange(args)
        return self._real_exchange(args)

    @staticmethod
    def _fixture_exchange(args: argparse.Namespace) -> CommandResult:
        from .ctrader_activation import ActivationProfile, LoopbackOAuthAssistant, OAuthAppConfig, OAuthAttemptStore, SecureTokenStore

        config = _config_for(args)
        app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
        profile = ActivationProfile.from_mapping(dict(config.ctrader))
        with tempfile.TemporaryDirectory(prefix="mtf-lab-ctrader-fixture-") as tmp:
            root = Path(tmp)
            tokens = SecureTokenStore.for_fixture(root / "tokens", project_root=PROJECT_ROOT)
            attempts = OAuthAttemptStore.for_fixture(root / "attempts", project_root=PROJECT_ROOT)
            assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens, fixture_mode=True)
            scopes = ("trading",) if "trading" in profile.required_scopes else ("accounts",)
            attempt = assistant.begin(client_id="fixture-client", requested_scopes=scopes)
            assistant.receive_callback(attempt.attempt_id, f"{app.redirect_uri}?code=fixture-code&state={attempt.csrf_state}")
            metadata = assistant.exchange(attempt.attempt_id, client_id="fixture-client", client_secret="fixture-runtime-only", token_ref=profile.token_ref, observed_scopes=profile.required_scopes, requester=lambda url, params, timeout: _fixture_oauth_response())
            output = {"ok": True, "fixture": True, "network_performed": False, "token": metadata.to_dict(), "secrets": "REDACTED", "real_token_store_touched": False, "fixture_source": "local_oauth_fixture", "gateway_adapter_used": False}
        output["fixture_store_removed"] = not root.exists()
        return CommandResult.json(output)

    @staticmethod
    def _real_exchange(args: argparse.Namespace) -> CommandResult:
        from .ctrader_activation import ActivationProfile, LoopbackOAuthAssistant, OAuthAppConfig, OAuthAttemptPhase, OAuthAttemptStore, SecureTokenStore

        config = _config_for(args)
        if not config.ctrader_oauth or not config.ctrader:
            return _json_error({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False})
        app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
        profile = ActivationProfile.from_mapping(dict(config.ctrader))
        if not getattr(args, "attempt_id", None):
            return _json_error({"ok": False, "state": "CALLBACK_REQUIRED", "network_performed": False, "next_action": "Use auth-url y entregue --attempt-id más --callback-file 0600 o --callback-stdin; no pase el código como argumento."})
        if getattr(args, "callback_uri", None):
            return _json_error({"ok": False, "state": "CALLBACK_INPUT_UNSAFE", "network_performed": False, "next_action": "No pase el callback con código en argumentos; use --callback-file 0600 o --callback-stdin."})
        client_id = os.environ.get(app.client_id_env, "")
        client_secret = os.environ.get(app.client_secret_env, "")
        if not client_id or not client_secret:
            return _json_error({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False, "next_action": "Defina las variables locales de aplicación; no se guardaron secretos."})
        network_performed = False
        try:
            tokens = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
            attempts = OAuthAttemptStore(profile.token_store_dir, project_root=PROJECT_ROOT)
            assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens)
            pending = attempts.load(args.attempt_id)
            if pending.phase is not OAuthAttemptPhase.CALLBACK_RECEIVED:
                assistant.receive_callback(args.attempt_id, _read_callback_input(args))
            network_performed = True
            metadata = assistant.exchange(args.attempt_id, client_id=client_id, client_secret=client_secret, token_ref=profile.token_ref, observed_scopes=None, requester=_oauth_http_request)
        except Exception as exc:
            return _json_error({"ok": False, "state": type(exc).__name__, "network_performed": network_performed, "next_action": "No se guardó un token incompleto; revise callback, aplicación y código vigente.", "error": str(exc)})
        finally:
            client_secret = ""
        return CommandResult.json({"ok": True, "network_performed": network_performed, "token": metadata.to_dict(), "secrets": "REDACTED", "next_action": "Descubra las cuentas autorizadas antes de seleccionar una cuenta DEMO."})

    def token_refresh(self, args: argparse.Namespace) -> CommandResult:
        if getattr(args, "fixture", False):
            return self._fixture_refresh(args)
        return self._real_refresh(args)

    @staticmethod
    def _fixture_refresh(args: argparse.Namespace) -> CommandResult:
        from .ctrader_activation import ActivationProfile, LoopbackOAuthAssistant, OAuthAppConfig, OAuthAttemptStore, SecureTokenStore

        config = _config_for(args)
        app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
        profile = ActivationProfile.from_mapping(dict(config.ctrader))
        with tempfile.TemporaryDirectory(prefix="mtf-lab-ctrader-fixture-") as tmp:
            root = Path(tmp)
            tokens = SecureTokenStore.for_fixture(root / "tokens", project_root=PROJECT_ROOT)
            attempts = OAuthAttemptStore.for_fixture(root / "attempts", project_root=PROJECT_ROOT)
            tokens.rotate(profile.token_ref, access_token="fixture-access-before-refresh", refresh_token="fixture-refresh-before-refresh", granted_scopes=profile.required_scopes, expires_at=datetime.now(UTC) + timedelta(minutes=5), fixture_payload=True)
            assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens, fixture_mode=True)
            metadata = assistant.refresh(profile.token_ref, client_id="fixture-client", client_secret="fixture-runtime-only", observed_scopes=profile.required_scopes, requester=lambda url, params, timeout: _fixture_oauth_response(refreshed=True))
            output = {"ok": True, "fixture": True, "network_performed": False, "token": metadata.to_dict(), "secrets": "REDACTED", "real_token_store_touched": False, "fixture_source": "local_oauth_fixture", "gateway_adapter_used": False}
        output["fixture_store_removed"] = not root.exists()
        return CommandResult.json(output)

    @staticmethod
    def _real_refresh(args: argparse.Namespace) -> CommandResult:
        from .ctrader_activation import ActivationProfile, LoopbackOAuthAssistant, OAuthAppConfig, OAuthAttemptStore, SecureTokenStore

        config = _config_for(args)
        app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
        profile = ActivationProfile.from_mapping(dict(config.ctrader))
        client_id = os.environ.get(app.client_id_env, "")
        client_secret = os.environ.get(app.client_secret_env, "")
        if not client_id or not client_secret:
            return _json_error({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False, "next_action": "Defina las variables locales de aplicación; no se guardaron secretos."})
        network_performed = False
        try:
            tokens = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
            attempts = OAuthAttemptStore(profile.token_store_dir, project_root=PROJECT_ROOT)
            assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens)
            existing = tokens.read(profile.token_ref)
            network_performed = True
            metadata = assistant.refresh(profile.token_ref, client_id=client_id, client_secret=client_secret, observed_scopes=existing.metadata.granted_scopes, requester=_oauth_http_request)
        except Exception as exc:
            return _json_error({"ok": False, "state": type(exc).__name__, "network_performed": network_performed, "next_action": "No se modificó el token previo; revise conectividad y la autorización vigente.", "error": str(exc)})
        finally:
            client_secret = ""
        return CommandResult.json({"ok": True, "network_performed": network_performed, "token": metadata.to_dict(), "secrets": "REDACTED"})

    def _selection_data(self, args: argparse.Namespace, config: Any) -> tuple[Any, Any, list[Any]] | CommandResult:
        accounts_path = Path(args.accounts_file).expanduser() if args.accounts_file else _discovery_state_path(config)
        if not accounts_path.exists():
            return _json_error({"ok": False, "state": "ACCOUNT_DISCOVERY_REQUIRED", "next_action": "Ejecute query --network o proporcione --accounts-file con una respuesta observada; no se selecciona automáticamente."})
        metadata = _token_metadata_for_config(config)
        if metadata is None:
            return _json_error({"ok": False, "state": "TOKEN_REFERENCE_REQUIRED", "network_performed": False, "next_action": "Active primero un token externo con scope accounts."})
        raw = json.loads(accounts_path.read_text(encoding="utf-8"))
        accounts = raw.get("accounts", raw.get("records")) if isinstance(raw, Mapping) else raw
        if not isinstance(accounts, list):
            return _json_error({"ok": False, "state": "ACCOUNT_DISCOVERY_INVALID", "network_performed": False, "next_action": "El archivo debe contener una lista accounts observada."})
        if isinstance(raw, Mapping) and raw.get("token_ref") not in {None, metadata.token_ref}:
            return _json_error({"ok": False, "state": "ACCOUNT_DISCOVERY_INVALID", "network_performed": False, "next_action": "El token_ref del descubrimiento no coincide con el perfil."})
        return metadata, raw, accounts

    @staticmethod
    def _make_discovery(metadata: Any, raw: Any, accounts: list[Any]) -> Any | CommandResult:
        from .ctrader_commands import account_discovery_command

        try:
            return account_discovery_command(metadata, accounts, observed_at=CTraderCliService._parse_observed_at(raw), permission_scope=(raw.get("permissionScope", raw.get("permission_scope")) if isinstance(raw, Mapping) else None))
        except Exception as exc:
            return _json_error({"ok": False, "state": "ACCOUNT_DISCOVERY_INVALID", "network_performed": False, "next_action": "La respuesta no contiene cuentas observadas válidas; repita discovery desde el servidor.", "error": type(exc).__name__})

    def select(self, args: argparse.Namespace) -> CommandResult:
        from .ctrader_commands import select_account_command

        config = _config_for(args)
        selection = self._selection_data(args, config)
        if isinstance(selection, CommandResult):
            return selection
        metadata, raw, accounts = selection
        discovery = self._make_discovery(metadata, raw, accounts)
        if isinstance(discovery, CommandResult):
            return discovery
        output = select_account_command(_ctrader_activation_payload(config), discovery, account_id=args.account_id, environment="DEMO")
        try:
            selection_path = _persist_selection(config, account_id=args.account_id, token_ref=metadata.token_ref, observed_at=discovery.observed_at)
        except Exception as exc:
            return _json_error({"ok": False, "state": "SELECTION_PERSISTENCE_FAILED", "network_performed": False, "next_action": "No se modificó TOML; corrija el store externo y repita la selección.", "error": type(exc).__name__})
        output["selection_persisted"] = True
        output["selection_path"] = str(selection_path)
        output["next_action"] = "Selección DEMO guardada fuera del repo; al conectar se volverá a validar contra el inventario del servidor."
        return CommandResult.json(output)


    @staticmethod
    def _parse_observed_at(raw: Any) -> datetime | None:
        if isinstance(raw, Mapping) and raw.get("observed_at"):
            return datetime.fromisoformat(str(raw["observed_at"]).replace("Z", "+00:00"))
        return None

    def query(self, args: argparse.Namespace) -> CommandResult:
        if getattr(args, "fixture", False):
            return self.fixture(args)
        return self._query_network(args) if getattr(args, "network", False) else self._query_preflight(args)

    def _query_preflight(self, args: argparse.Namespace) -> CommandResult:
        from .ctrader_commands import status_command

        config = _config_for(args)
        status = status_command(_activation_payload_with_selection(config), token_metadata=_token_metadata_for_config(config), present_env_keys=set(os.environ), accounts=(), now=datetime.now(UTC))
        return _json_error({"ok": False, "state": status["status"]["state"], "network_performed": False, "activation": status, "next_action": "Use --fixture o --network sólo tras completar OAuth externo."})

    def _prepare_query(self, args: argparse.Namespace) -> "QueryContext" | CommandResult:
        from .ctrader_activation import ActivationProfile, OAuthAppConfig, SecureTokenStore
        from .ctrader_commands import status_command

        config = _config_for(args)
        metadata = _token_metadata_for_config(config)
        status_before = status_command(_activation_payload_with_selection(config), token_metadata=metadata, present_env_keys=set(os.environ), accounts=(), now=datetime.now(UTC))
        activation_payload = _activation_payload_with_selection(config)
        profile = ActivationProfile.from_mapping(dict(activation_payload["ctrader"]))
        app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
        if profile.operation_mode.value != "QUERY" or metadata is None or metadata.is_expired(datetime.now(UTC)) or "accounts" not in metadata.granted_scopes:
            return _json_error({"ok": False, "state": status_before["status"]["state"], "network_performed": False, "activation": status_before, "next_action": "Complete un token accounts vigente antes de conectar."})
        client_id = os.environ.get(app.client_id_env, "")
        client_secret = os.environ.get(app.client_secret_env, "")
        if not client_id or not client_secret:
            return _json_error({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False, "next_action": "Defina las referencias de aplicación en variables de entorno."})
        lease = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT).read(profile.token_ref)
        return QueryContext(config, metadata, status_before, profile, app, lease, client_secret)

    def _connect_and_discover(self, context: "QueryContext") -> tuple[Any, Any]:
        from ..data.ctrader import CTraderProvider

        provider = CTraderProvider(_ctrader_config(context.config))
        provider.connect()
        context.network_performed = True
        context.sequence.append("connect")
        provider.authenticate(secret_provider=self._secret_provider(context.app, context.client_secret, context.sequence), token_provider=self._token_provider(context.profile, context.lease, context.sequence), authorize_selected=False)
        return provider, provider.discover_accounts()

    def _query_observation(self, context: "QueryContext", provider: Any, observed: Any) -> CommandResult:
        from .ctrader_commands import status_command

        accounts = self._accounts(observed)
        discovery_path = self._try_persist_discovery(context.config, context.profile, observed)
        status_after = status_command(_activation_payload_with_selection(context.config), token_metadata=context.metadata, present_env_keys=set(os.environ), accounts=accounts, now=datetime.now(UTC))
        if not status_after["status"].get("ready"):
            output = {"ok": False, "network_performed": True, "sequence": context.sequence, "state": status_after["status"]["state"], "accounts": [account.redacted() for account in accounts], "permission_scope": observed.get("permissionScope") if isinstance(observed, Mapping) else None, "discovery_path": str(discovery_path) if discovery_path else None, "activation": status_after, "status": provider.status.to_dict(), "next_action": status_after["status"]["next_action"]}
            return CommandResult.json(output, code=2)
        provider.authorize_account(int(context.profile.account_id), token_provider=lambda ref: context.lease.access_token if ref == context.profile.token_ref else "")
        context.sequence.append("account_auth")
        catalog = provider.resolve_symbol()
        context.sequence.append("catalog")
        history = provider.fetch_history("M1", count=int(context.config.ctrader.get("historical_count", 500)), max_pages=20)
        context.sequence.append("history")
        output = {"ok": True, "network_performed": True, "sequence": context.sequence, "accounts": [account.redacted() for account in accounts], "permission_scope": observed.get("permissionScope") if isinstance(observed, Mapping) else None, "discovery_path": str(discovery_path) if discovery_path else None, "activation": status_after, "catalog": catalog.to_dict() if hasattr(catalog, "to_dict") else catalog, "history": history.to_dict() if hasattr(history, "to_dict") else history, "status": provider.status.to_dict(), "next_action": "Cuenta DEMO observada, catálogo e histórico obtenidos; las cotizaciones siguen siendo de consulta."}
        return CommandResult.json(output)

    def _query_network(self, args: argparse.Namespace) -> CommandResult:
        context = self._prepare_query(args)
        if isinstance(context, CommandResult):
            return context
        provider = None
        try:
            provider, observed = self._connect_and_discover(context)
            return self._query_observation(context, provider, observed)
        except Exception as exc:
            provider_status = provider.status.to_dict() if provider is not None else {}
            action = provider_status.get("action") if isinstance(provider_status, Mapping) else None
            return _json_error({"ok": False, "network_performed": context.network_performed, "sequence": context.sequence, "state": type(exc).__name__, "status": provider_status, "next_action": action or "Conecte, autentique la aplicación y descubra cuentas antes de seleccionar una cuenta DEMO."})
        finally:
            context.client_secret = ""
            if provider is not None:
                try:
                    provider.close()
                except Exception:
                    pass


    @staticmethod
    def _secret_provider(app: Any, client_secret: str, sequence: list[str]):
        def provide(ref: str) -> str:
            if ref == app.client_secret_env:
                sequence.append("application_auth")
                return client_secret
            return ""

        return provide

    @staticmethod
    def _token_provider(profile: Any, lease: Any, sequence: list[str]):
        def provide(ref: str) -> str:
            if ref == profile.token_ref:
                sequence.append("account_discovery")
                return lease.access_token
            return ""

        return provide

    @staticmethod
    def _accounts(observed: Any) -> list[Any]:
        from .ctrader_activation import BrokerAccount

        result: list[Any] = []
        raw_records = observed.get("records", ()) if isinstance(observed, Mapping) else ()
        for raw in raw_records:
            if not isinstance(raw, Mapping):
                continue
            account_id = raw.get("account_id", raw.get("ctidTraderAccountId"))
            if account_id is None:
                continue
            label = str(raw.get("brokerTitleShort", raw.get("label", "")))
            permissions = raw.get("permissions", raw.get("permission_scope", raw.get("permissionScope", ())))
            result.append(BrokerAccount(str(account_id), str(raw.get("environment", "UNKNOWN")), label, permissions))
        return result

    @staticmethod
    def _try_persist_discovery(config: Any, profile: Any, observed: Any) -> Path | None:
        try:
            return _persist_discovery(config, token_ref=profile.token_ref, observed=observed, observed_at=datetime.now(UTC))
        except Exception:
            return None

    def demo(self, args: argparse.Namespace) -> CommandResult:
        if not getattr(args, "activate", False):
            return CommandResult.json({"ok": True, "enabled": False, "environment": "DEMO", "server_contacted": False, "fixture_source": "local_fixture", "gateway_adapter_used": False, "next_action": "Use --activate sólo para el fixture local determinista; no se activa una cuenta externa ni se contacta gateway."})
        return self.fixture(args)

    @staticmethod
    def fixture(args: argparse.Namespace) -> CommandResult:
        from .ctrader_demo import run_ctrader_fixture

        output = dict(run_ctrader_fixture(args.report))
        output.setdefault("fixture_source", "local_fixture")
        output.setdefault("gateway_adapter_used", False)
        output.setdefault("network_performed", False)
        output.setdefault("credentials_used", False)
        output.setdefault("next_action", "Fixture local offline; no gateway ni cuenta externa fueron contactados.")
        return CommandResult.json(output)


__all__ = [
    "CTraderCliService",
    "_ctrader_activation_payload",
    "_selection_state_path",
    "_discovery_state_path",
    "_secret_free",
    "_persist_discovery",
    "_load_persisted_selection",
    "_activation_payload_with_selection",
    "_persist_selection",
    "_ctrader_config",
    "_token_metadata_for_config",
    "_check_ctrader_connectivity",
    "_fixture_oauth_response",
    "_oauth_http_request",
    "_read_callback_input",
]
