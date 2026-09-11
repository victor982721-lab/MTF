"""Fail-closed, local activation state for a future cTrader connector.

This module deliberately performs no HTTP requests, opens no browser and never
accepts an account password.  It models the human activation gates and keeps
OAuth tokens outside the project tree in a small atomically-rotated store.

Only token *references* and redacted metadata are suitable for configuration,
logs or SQLite.  :class:`TokenLease` is the sole object carrying secret bytes;
its string representations are always redacted.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import os
import re
import secrets
import stat
from collections.abc import Mapping, Sequence, Set
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


SUPPORTED_SCOPES = frozenset({"accounts", "trading"})
_TOKEN_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_FORBIDDEN_CONFIG_KEYS = frozenset(
    {"password", "passphrase", "client_secret", "access_token", "refresh_token"}
)


class ActivationError(RuntimeError):
    """Invalid or unsafe activation input."""


class UnsafeTokenStore(ActivationError):
    """The selected token store is not external or is not private."""


class RealAccountForbidden(ActivationError):
    """MTF Lab never activates execution against a real account."""


class ActivationMode(str, enum.Enum):
    QUERY = "QUERY"
    DEMO = "DEMO"


class ActivationState(str, enum.Enum):
    DISABLED = "DISABLED"
    APP_CREDENTIALS_REQUIRED = "APP_CREDENTIALS_REQUIRED"
    ACCOUNTS_SCOPE_REQUIRED = "ACCOUNTS_SCOPE_REQUIRED"
    TOKEN_REFERENCE_REQUIRED = "TOKEN_REFERENCE_REQUIRED"
    TOKEN_EXPIRED = "TOKEN_EXPIRED"
    ACCOUNT_DISCOVERY_REQUIRED = "ACCOUNT_DISCOVERY_REQUIRED"
    DEMO_ACCOUNT_SELECTION_REQUIRED = "DEMO_ACCOUNT_SELECTION_REQUIRED"
    ACCOUNT_SELECTION_INVALID = "ACCOUNT_SELECTION_INVALID"
    REAL_ACCOUNT_FORBIDDEN = "REAL_ACCOUNT_FORBIDDEN"
    QUERY_READY = "QUERY_READY"
    TRADING_SCOPE_REQUIRED = "TRADING_SCOPE_REQUIRED"
    DEMO_READY = "DEMO_READY"


def _utc(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip().replace("Z", "+00:00")
        result = datetime.fromisoformat(text)
    if result.tzinfo is None:
        raise ActivationError("timestamps de activación requieren zona horaria")
    return result.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _scope_set(values: Sequence[str] | Set[str]) -> frozenset[str]:
    result = frozenset(str(value).strip().lower() for value in values)
    unsupported = sorted(result - SUPPORTED_SCOPES)
    if unsupported:
        raise ActivationError(f"scopes cTrader no soportados: {unsupported}")
    return result


def _is_loopback_redirect(uri: str) -> bool:
    parsed = urlparse(uri)
    return (
        parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        and parsed.port is not None
        and bool(parsed.path)
    )


@dataclasses.dataclass(frozen=True, slots=True)
class OAuthAppConfig:
    """OAuth application references; never contains credential values."""

    client_id_env: str
    client_secret_env: str
    redirect_uri: str
    authorization_url: str
    token_url: str

    def __post_init__(self) -> None:
        for name in ("client_id_env", "client_secret_env"):
            value = str(getattr(self, name)).strip()
            if not value or not value.replace("_", "A").isalnum() or value.upper() != value:
                raise ActivationError(f"{name} debe ser un nombre de variable de entorno")
            object.__setattr__(self, name, value)
        if not _is_loopback_redirect(self.redirect_uri):
            raise ActivationError("redirect_uri debe ser un callback HTTP de loopback con puerto y ruta")
        for name in ("authorization_url", "token_url"):
            value = str(getattr(self, name)).strip()
            parsed = urlparse(value)
            if parsed.scheme != "https" or not parsed.hostname:
                raise ActivationError(f"{name} debe ser una URL HTTPS explícita")
            object.__setattr__(self, name, value)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "OAuthAppConfig":
        forbidden = sorted(set(value) & _FORBIDDEN_CONFIG_KEYS)
        if forbidden:
            raise ActivationError(f"secretos directos prohibidos en configuración: {forbidden}")
        allowed = {
            "client_id_env",
            "client_secret_env",
            "redirect_uri",
            "authorization_url",
            "token_url",
        }
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ActivationError(f"claves OAuth desconocidas: {unknown}")
        try:
            return cls(**{name: str(value[name]) for name in allowed})
        except KeyError as exc:
            raise ActivationError(f"falta configuración OAuth: {exc.args[0]}") from None

    def redacted(self, present_env_keys: Set[str] = frozenset()) -> dict[str, Any]:
        return {
            "client_id_env": self.client_id_env,
            "client_id_present": self.client_id_env in present_env_keys,
            "client_secret_env": self.client_secret_env,
            "client_secret_present": self.client_secret_env in present_env_keys,
            "redirect_uri": self.redirect_uri,
            "authorization_url": self.authorization_url,
            "token_url": self.token_url,
            "secrets": "REDACTED",
        }


@dataclasses.dataclass(frozen=True, slots=True, repr=False)
class OAuthTokenPayload:
    """Short-lived exchange result; secret fields never appear in repr/str."""

    access_token: str = dataclasses.field(repr=False)
    refresh_token: str | None = dataclasses.field(default=None, repr=False)
    expires_in: int = 0
    token_type: str = "bearer"

    def __repr__(self) -> str:
        return f"OAuthTokenPayload(expires_in={self.expires_in}, token_type={self.token_type!r}, secrets='REDACTED')"

    __str__ = __repr__

    def __post_init__(self) -> None:
        if not isinstance(self.access_token, str) or not self.access_token:
            raise ActivationError("respuesta OAuth sin access_token válido")
        if self.refresh_token is not None and (not isinstance(self.refresh_token, str) or not self.refresh_token):
            raise ActivationError("respuesta OAuth con refresh_token inválido")
        if isinstance(self.expires_in, bool) or int(self.expires_in) <= 0:
            raise ActivationError("expires_in OAuth debe ser entero positivo")
        object.__setattr__(self, "expires_in", int(self.expires_in))
        object.__setattr__(self, "token_type", str(self.token_type or "bearer").lower())

    @classmethod
    def from_response(cls, payload: Mapping[str, Any]) -> "OAuthTokenPayload":
        if not isinstance(payload, Mapping):
            raise ActivationError("respuesta OAuth no es un objeto")
        error = payload.get("errorCode") or payload.get("error")
        if error:
            # Never echo response bodies, codes or token-like values to logs.
            raise ActivationError("intercambio OAuth rechazado; revise el estado de la aplicación y el código vigente")
        try:
            return cls(
                access_token=str(payload["accessToken"]),
                refresh_token=(str(payload["refreshToken"]) if payload.get("refreshToken") is not None else None),
                expires_in=int(payload.get("expiresIn", 0)),
                token_type=str(payload.get("tokenType", "bearer")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ActivationError("respuesta OAuth incompleta") from exc


def build_authorization_url(app: OAuthAppConfig, *, client_id: str, scope: str, state: str | None = None) -> str:
    """Build the official cTrader OAuth URL without opening a browser."""
    client_id = str(client_id).strip()
    if not client_id:
        raise ActivationError("client_id no puede estar vacío")
    scopes = _scope_set([item for item in str(scope).replace(",", " ").split() if item])
    if not scopes:
        raise ActivationError("scope OAuth no puede estar vacío")
    parsed = urlparse(app.authorization_url)
    query = {"client_id": client_id, "redirect_uri": app.redirect_uri, "scope": " ".join(sorted(scopes)), "product": "web"}
    if state is not None:
        state_value = str(state).strip()
        if not state_value:
            raise ActivationError("state no puede estar vacío")
        query["state"] = state_value
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query), parsed.fragment))


def parse_callback_uri(uri: str, *, expected_state: str | None = None, registered_uri: str | None = None) -> str:
    """Extract one authorization code from a registered loopback callback."""
    parsed = urlparse(str(uri))
    callback_base = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, "", ""))
    if not _is_loopback_redirect(callback_base):
        raise ActivationError("callback OAuth fuera de la URI loopback registrada")
    if registered_uri is not None:
        registered = urlparse(str(registered_uri))
        registered_base = urlunparse((registered.scheme, registered.netloc, registered.path, registered.params, "", ""))
        if callback_base != registered_base:
            raise ActivationError("callback OAuth no coincide con la URI registrada")
    query = parse_qs(parsed.query, keep_blank_values=False)
    if query.get("error"):
        raise ActivationError("el usuario o el proveedor rechazó la autorización OAuth")
    codes = query.get("code", [])
    if len(codes) != 1 or not codes[0].strip():
        raise ActivationError("callback OAuth sin un código único")
    if expected_state is not None and query.get("state", [None])[0] != str(expected_state):
        raise ActivationError("state OAuth no coincide")
    return codes[0]


def _safe_token_request(app: OAuthAppConfig, params: Mapping[str, str], *, requester: Any = None, timeout: float = 10.0) -> OAuthTokenPayload:
    """Execute an injected or stdlib token request without exposing secrets."""
    if requester is not None:
        try:
            response = requester(app.token_url, dict(params), timeout)
        except Exception as exc:
            raise ActivationError("falló la solicitud OAuth; revise conectividad y estado de la aplicación") from exc
    else:
        from urllib.request import Request, urlopen
        try:
            query = urlencode(dict(params)).encode("utf-8")
            request = Request(f"{app.token_url}?{urlencode(dict(params))}", headers={"Accept": "application/json"})
            with urlopen(request, timeout=timeout) as stream:
                response = json.loads(stream.read().decode("utf-8"))
        except Exception as exc:
            raise ActivationError("falló la solicitud OAuth; revise conectividad y estado de la aplicación") from exc
    return OAuthTokenPayload.from_response(response)


def exchange_authorization_code(app: OAuthAppConfig, *, client_id: str, client_secret: str, code: str, requester: Any = None, timeout: float = 10.0) -> OAuthTokenPayload:
    """Exchange a one-minute authorization code; caller decides where to store tokens."""
    if not isinstance(client_secret, str) or not client_secret:
        raise ActivationError("client_secret debe llegar por el proveedor seguro, no por configuración")
    code = str(code).strip()
    if not code:
        raise ActivationError("authorization code vacío")
    return _safe_token_request(app, {"grant_type": "authorization_code", "code": code, "redirect_uri": app.redirect_uri, "client_id": str(client_id), "client_secret": client_secret}, requester=requester, timeout=timeout)


def refresh_access_token(app: OAuthAppConfig, *, client_id: str, client_secret: str, refresh_token: str, requester: Any = None, timeout: float = 10.0) -> OAuthTokenPayload:
    """Rotate access/refresh atomically; never assume the old refresh token survives."""
    if not isinstance(client_secret, str) or not client_secret or not isinstance(refresh_token, str) or not refresh_token:
        raise ActivationError("credenciales OAuth de renovación incompletas")
    return _safe_token_request(app, {"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": str(client_id), "client_secret": client_secret}, requester=requester, timeout=timeout)


@dataclasses.dataclass(frozen=True, slots=True)
class ActivationProfile:
    enabled: bool
    operation_mode: ActivationMode
    environment: str
    required_scopes: frozenset[str]
    account_id: str = ""
    account_selected: bool = False
    token_ref: str = ""
    token_store_dir: str = "~/.local/state/mtf-lab/ctrader-tokens"

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool) or not isinstance(self.account_selected, bool):
            raise ActivationError("enabled y account_selected deben ser booleanos")
        mode = self.operation_mode
        if not isinstance(mode, ActivationMode):
            try:
                mode = ActivationMode(str(mode).strip().upper())
            except ValueError:
                raise ActivationError("operation_mode debe ser query o demo") from None
            object.__setattr__(self, "operation_mode", mode)
        environment = str(self.environment).strip().upper()
        if environment != "DEMO":
            raise RealAccountForbidden("sólo se admite environment=DEMO")
        object.__setattr__(self, "environment", environment)
        scopes = _scope_set(self.required_scopes)
        minimum = {"accounts"} if mode is ActivationMode.QUERY else {"accounts", "trading"}
        if scopes != minimum:
            raise ActivationError(
                f"required_scopes para {mode.value.lower()} debe ser exactamente {sorted(minimum)}"
            )
        object.__setattr__(self, "required_scopes", scopes)
        account_id = str(self.account_id).strip()
        token_ref = str(self.token_ref).strip()
        if self.account_selected and not account_id:
            raise ActivationError("account_selected=true requiere account_id explícito")
        if token_ref and not _TOKEN_REF.fullmatch(token_ref):
            raise ActivationError("token_ref contiene caracteres no permitidos")
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "token_ref", token_ref)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ActivationProfile":
        allowed = {
            "enabled",
            "operation_mode",
            "environment",
            "required_scopes",
            "account_id",
            "account_selected",
            "token_ref",
            "token_store_dir",
        }
        forbidden = sorted(set(value) & _FORBIDDEN_CONFIG_KEYS)
        if forbidden:
            raise ActivationError(f"secretos directos prohibidos en configuración: {forbidden}")
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ActivationError(f"claves de activación desconocidas: {unknown}")
        try:
            return cls(
                enabled=value["enabled"],
                operation_mode=ActivationMode(str(value["operation_mode"]).upper()),
                environment=str(value["environment"]),
                required_scopes=frozenset(value["required_scopes"]),
                account_id=str(value.get("account_id", "")),
                account_selected=value.get("account_selected", False),
                token_ref=str(value.get("token_ref", "")),
                token_store_dir=str(value.get("token_store_dir", "~/.local/state/mtf-lab/ctrader-tokens")),
            )
        except KeyError as exc:
            raise ActivationError(f"falta clave de activación: {exc.args[0]}") from None
        except ValueError as exc:
            raise ActivationError(str(exc)) from None

    def redacted(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "operation_mode": self.operation_mode.value,
            "environment": self.environment,
            "required_scopes": sorted(self.required_scopes),
            "account_id": redact_identifier(self.account_id),
            "account_selected": self.account_selected,
            "token_ref": self.token_ref,
            "token_store_dir": self.token_store_dir,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class BrokerAccount:
    account_id: str
    environment: str
    label: str = ""

    def __post_init__(self) -> None:
        account_id = str(self.account_id).strip()
        if not account_id:
            raise ActivationError("account_id no puede estar vacío")
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "environment", str(self.environment).strip().upper())

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "BrokerAccount":
        return cls(
            account_id=str(value.get("account_id", value.get("id", ""))),
            environment=str(value.get("environment", value.get("account_type", ""))),
            label=str(value.get("label", "")),
        )

    def redacted(self) -> dict[str, str]:
        return {
            "account_id": redact_identifier(self.account_id),
            "environment": self.environment,
            "label": self.label,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class TokenMetadata:
    token_ref: str
    granted_scopes: frozenset[str]
    expires_at: datetime
    rotated_at: datetime
    generation: int

    def __post_init__(self) -> None:
        if not _TOKEN_REF.fullmatch(str(self.token_ref)):
            raise ActivationError("token_ref inválido")
        object.__setattr__(self, "granted_scopes", _scope_set(self.granted_scopes))
        object.__setattr__(self, "expires_at", _utc(self.expires_at))
        object.__setattr__(self, "rotated_at", _utc(self.rotated_at))
        if isinstance(self.generation, bool) or int(self.generation) < 1:
            raise ActivationError("generation debe ser entero positivo")
        object.__setattr__(self, "generation", int(self.generation))

    @property
    def expired(self) -> bool:
        return self.expires_at <= datetime.now(UTC)

    def is_expired(self, now: datetime) -> bool:
        return self.expires_at <= _utc(now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "token_ref": self.token_ref,
            "granted_scopes": sorted(self.granted_scopes),
            "expires_at": _iso(self.expires_at),
            "rotated_at": _iso(self.rotated_at),
            "generation": self.generation,
            "secret": "REDACTED",
        }


@dataclasses.dataclass(frozen=True, slots=True, repr=False)
class TokenLease:
    metadata: TokenMetadata
    access_token: str = dataclasses.field(repr=False)
    refresh_token: str | None = dataclasses.field(default=None, repr=False)

    def __repr__(self) -> str:
        return f"TokenLease(token_ref={self.metadata.token_ref!r}, secret='REDACTED')"

    __str__ = __repr__


@dataclasses.dataclass(frozen=True, slots=True)
class ActivationStatus:
    state: ActivationState
    ready: bool
    operation_mode: ActivationMode
    missing_scopes: tuple[str, ...]
    next_action: str
    selected_account: Mapping[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "ready": self.ready,
            "operation_mode": self.operation_mode.value,
            "missing_scopes": list(self.missing_scopes),
            "next_action": self.next_action,
            "selected_account": dict(self.selected_account) if self.selected_account else None,
        }


class SecureTokenStore:
    """A private external directory with atomic, same-filesystem rotation."""

    def __init__(self, root: str | Path, *, project_root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve(strict=False)
        self.project_root = Path(project_root).expanduser().resolve(strict=False)
        if self.root == self.project_root or self.root.is_relative_to(self.project_root):
            raise UnsafeTokenStore("token_store_dir debe estar fuera del árbol del proyecto")

    def _prepare(self) -> None:
        if self.root.exists() and self.root.is_symlink():
            raise UnsafeTokenStore("token_store_dir no puede ser symlink")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        if stat.S_IMODE(self.root.stat().st_mode) != 0o700:
            raise UnsafeTokenStore("token_store_dir debe tener permisos 0700")

    def _path(self, token_ref: str) -> Path:
        if not _TOKEN_REF.fullmatch(str(token_ref)):
            raise ActivationError("token_ref inválido")
        return self.root / f"{token_ref}.json"

    def rotate(
        self,
        token_ref: str,
        *,
        access_token: str,
        refresh_token: str | None,
        granted_scopes: Sequence[str] | Set[str],
        expires_at: datetime | str,
        now: datetime | None = None,
    ) -> TokenMetadata:
        """Atomically replace one token envelope and return redacted metadata."""

        if not isinstance(access_token, str) or not access_token:
            raise ActivationError("access_token no puede estar vacío")
        if refresh_token is not None and (not isinstance(refresh_token, str) or not refresh_token):
            raise ActivationError("refresh_token debe ser texto no vacío o None")
        self._prepare()
        target = self._path(token_ref)
        previous = self.metadata(token_ref) if target.exists() else None
        rotated_at = _utc(now or datetime.now(UTC))
        metadata = TokenMetadata(
            token_ref=token_ref,
            granted_scopes=_scope_set(granted_scopes),
            expires_at=_utc(expires_at),
            rotated_at=rotated_at,
            generation=(previous.generation + 1 if previous else 1),
        )
        payload = {
            "version": 1,
            "metadata": {key: value for key, value in metadata.to_dict().items() if key != "secret"},
            "access_token": access_token,
            "refresh_token": refresh_token,
        }
        temporary = self.root / f".{token_ref}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            os.chmod(target, 0o600)
            directory_fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary.exists():
                temporary.unlink()
        return metadata

    def read(self, token_ref: str) -> TokenLease:
        raw = self._load(token_ref)
        metadata = self._metadata_from_payload(raw)
        access_token = raw.get("access_token")
        refresh_token = raw.get("refresh_token")
        if not isinstance(access_token, str) or not access_token:
            raise ActivationError("sobre de token inválido")
        return TokenLease(metadata, access_token, refresh_token)

    def metadata(self, token_ref: str) -> TokenMetadata:
        return self._metadata_from_payload(self._load(token_ref))

    def _load(self, token_ref: str) -> Mapping[str, Any]:
        target = self._path(token_ref)
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(target, flags)
        except OSError as exc:
            raise UnsafeTokenStore(f"no se pudo abrir el token de forma segura: {exc}") from exc
        try:
            if stat.S_IMODE(os.fstat(fd).st_mode) != 0o600:
                raise UnsafeTokenStore("el archivo de token debe tener permisos exactos 0600")
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                fd = -1
                raw = json.load(handle)
        finally:
            if fd >= 0:
                os.close(fd)
        if not isinstance(raw, Mapping):
            raise ActivationError("sobre de token inválido")
        return raw

    @staticmethod
    def _metadata_from_payload(raw: Mapping[str, Any]) -> TokenMetadata:
        if raw.get("version") != 1 or not isinstance(raw.get("metadata"), Mapping):
            raise ActivationError("versión o metadatos de token inválidos")
        meta = raw["metadata"]
        return TokenMetadata(
            token_ref=str(meta["token_ref"]),
            granted_scopes=frozenset(meta["granted_scopes"]),
            expires_at=_utc(meta["expires_at"]),
            rotated_at=_utc(meta["rotated_at"]),
            generation=int(meta["generation"]),
        )


def redact_identifier(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    if len(text) <= 4:
        return "*" * len(text)
    return f"{text[:2]}{'*' * min(8, len(text) - 4)}{text[-2:]}"


def select_demo_account(
    accounts: Sequence[BrokerAccount | Mapping[str, Any]],
    account_id: str,
    *,
    environment: str,
) -> BrokerAccount:
    """Return the explicitly named demo account; never chooses a default."""

    requested_env = str(environment).strip().upper()
    if requested_env != "DEMO":
        raise RealAccountForbidden("la selección requiere environment=DEMO explícito")
    matches = [
        item if isinstance(item, BrokerAccount) else BrokerAccount.from_mapping(item)
        for item in accounts
        if str(item.account_id if isinstance(item, BrokerAccount) else item.get("account_id", item.get("id", ""))).strip()
        == str(account_id).strip()
    ]
    if len(matches) != 1:
        raise ActivationError("account_id debe coincidir exactamente con una cuenta descubierta")
    selected = matches[0]
    if selected.environment != "DEMO":
        raise RealAccountForbidden("la cuenta seleccionada no está marcada como DEMO")
    return selected


def evaluate_activation(
    profile: ActivationProfile,
    *,
    token: TokenMetadata | None,
    accounts: Sequence[BrokerAccount | Mapping[str, Any]],
    present_env_keys: Set[str],
    app: OAuthAppConfig,
    now: datetime | None = None,
) -> ActivationStatus:
    """Evaluate activation from supplied facts without performing any I/O."""

    if not profile.enabled:
        return ActivationStatus(ActivationState.DISABLED, False, profile.operation_mode, (), "Habilita el perfil explícitamente.")
    missing_app = [name for name in (app.client_id_env, app.client_secret_env) if name not in present_env_keys]
    if missing_app:
        return ActivationStatus(
            ActivationState.APP_CREDENTIALS_REQUIRED,
            False,
            profile.operation_mode,
            (),
            f"Define localmente las variables requeridas: {', '.join(missing_app)}; no las guardes en TOML.",
        )
    if token is None:
        return ActivationStatus(
            ActivationState.ACCOUNTS_SCOPE_REQUIRED,
            False,
            profile.operation_mode,
            ("accounts",),
            "Completa OAuth local solicitando primero scope=accounts y guarda sólo el token_ref.",
        )
    if not profile.token_ref or token.token_ref != profile.token_ref:
        return ActivationStatus(
            ActivationState.TOKEN_REFERENCE_REQUIRED,
            False,
            profile.operation_mode,
            (),
            "Configura un token_ref que coincida exactamente con el sobre externo seleccionado.",
        )
    instant = _utc(now or datetime.now(UTC))
    if token.is_expired(instant):
        return ActivationStatus(
            ActivationState.TOKEN_EXPIRED,
            False,
            profile.operation_mode,
            tuple(sorted(profile.required_scopes - token.granted_scopes)),
            "Rota el token de forma atómica antes de consultar o ejecutar.",
        )
    if "accounts" not in token.granted_scopes:
        return ActivationStatus(
            ActivationState.ACCOUNTS_SCOPE_REQUIRED,
            False,
            profile.operation_mode,
            ("accounts",),
            "Reautoriza con scope=accounts; un token de trading no sustituye el descubrimiento de cuentas.",
        )
    normalized = tuple(item if isinstance(item, BrokerAccount) else BrokerAccount.from_mapping(item) for item in accounts)
    if not normalized:
        return ActivationStatus(
            ActivationState.ACCOUNT_DISCOVERY_REQUIRED,
            False,
            profile.operation_mode,
            (),
            "Consulta las cuentas autorizadas y vuelve a evaluar; no selecciones por inferencia.",
        )
    if not profile.account_selected or not profile.account_id:
        return ActivationStatus(
            ActivationState.DEMO_ACCOUNT_SELECTION_REQUIRED,
            False,
            profile.operation_mode,
            (),
            "Selecciona explícitamente un account_id cuyo environment observado sea DEMO.",
        )
    try:
        selected = select_demo_account(normalized, profile.account_id, environment=profile.environment)
    except RealAccountForbidden:
        return ActivationStatus(
            ActivationState.REAL_ACCOUNT_FORBIDDEN,
            False,
            profile.operation_mode,
            (),
            "Elimina la selección real y elige una cuenta DEMO; MTF Lab no admite cuentas reales.",
        )
    except ActivationError:
        return ActivationStatus(
            ActivationState.ACCOUNT_SELECTION_INVALID,
            False,
            profile.operation_mode,
            (),
            "El account_id seleccionado no coincide exactamente con una cuenta descubierta; selecciona de nuevo.",
        )
    if profile.operation_mode is ActivationMode.QUERY:
        return ActivationStatus(
            ActivationState.QUERY_READY,
            True,
            profile.operation_mode,
            (),
            "Consulta habilitada; trading permanece fuera de alcance de este perfil.",
            selected.redacted(),
        )
    if "trading" not in token.granted_scopes:
        return ActivationStatus(
            ActivationState.TRADING_SCOPE_REQUIRED,
            False,
            profile.operation_mode,
            ("trading",),
            "Autoriza scope=trading sólo después de confirmar nuevamente la cuenta DEMO seleccionada.",
            selected.redacted(),
        )
    return ActivationStatus(
        ActivationState.DEMO_READY,
        True,
        profile.operation_mode,
        (),
        "Activación DEMO lista; el ejecutor aún debe aplicar sus gates y límites de riesgo.",
        selected.redacted(),
    )


__all__ = [
    "ActivationError",
    "OAuthTokenPayload",
    "build_authorization_url",
    "parse_callback_uri",
    "exchange_authorization_code",
    "refresh_access_token",
    "ActivationMode",
    "ActivationProfile",
    "ActivationState",
    "ActivationStatus",
    "BrokerAccount",
    "OAuthAppConfig",
    "RealAccountForbidden",
    "SecureTokenStore",
    "SUPPORTED_SCOPES",
    "TokenLease",
    "TokenMetadata",
    "UnsafeTokenStore",
    "evaluate_activation",
    "redact_identifier",
    "select_demo_account",
]
