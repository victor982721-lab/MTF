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
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import tempfile
from collections.abc import Iterator, Mapping, Sequence, Set
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .oauth_policy import approved_oauth_endpoint

SUPPORTED_SCOPES = frozenset({"accounts", "trading"})
_TOKEN_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_FORBIDDEN_CONFIG_KEYS = frozenset({"password", "passphrase", "client_secret", "access_token", "refresh_token"})


class ActivationError(RuntimeError):
    """Invalid or unsafe activation input."""


class UnsafeTokenStore(ActivationError):
    """The selected token store is not external or is not private."""


class ReauthorizationRequired(ActivationError):
    """A prior one-use OAuth transaction has an unknown outcome."""


class RealAccountForbidden(ActivationError):
    """MTF Lab never activates execution against a real account."""


class ActivationMode(str, enum.Enum):  # noqa: UP042 - preserve public string enum behavior
    QUERY = "QUERY"
    DEMO = "DEMO"


class ActivationState(str, enum.Enum):  # noqa: UP042 - preserve public string enum behavior
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


def _oauth_scope_set(values: Sequence[str] | Set[str]) -> frozenset[str]:
    scopes = _scope_set(values)
    return frozenset({"accounts", "trading"}) if scopes == frozenset({"trading"}) else scopes


_AUTHORIZATION_QUERY_KEYS = frozenset({"client_id", "redirect_uri", "scope", "product", "state"})
_TRANSACTION_ID = re.compile(r"^oauth-txn-[0-9a-f]{32}$")
_FINGERPRINT = re.compile(r"^[0-9a-f]{64}$")


def _candidate_proof(
    *,
    kind: str,
    token_ref: str,
    current_generation: int,
    attempt_id: str,
    client_id: str,
    token_url: str,
    redirect_uri: str,
    requested_scopes: Sequence[str] | Set[str],
) -> str:
    """Derive a non-secret transaction proof from the intended request."""

    material = {
        "attempt_id": str(attempt_id),
        "client_id": str(client_id).strip(),
        "current_generation": int(current_generation),
        "kind": str(kind),
        "redirect_uri": str(redirect_uri),
        "requested_scopes": sorted(_scope_set(requested_scopes)),
        "token_ref": str(token_ref).strip(),
        "token_url": str(token_url),
    }
    encoded = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _parse_authorization_handoff(url: str) -> tuple[Any, dict[str, str]]:
    """Validate an authorization URL and return its non-secret fields."""

    parsed = urlparse(str(url).strip())
    if not approved_oauth_endpoint(parsed._replace(query="").geturl(), token=False):
        raise ActivationError("authorization_url no corresponde a un endpoint aprobado")
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ActivationError("authorization_url inválida")
    values = _unique_query_values(
        parsed.query,
        allowed=_AUTHORIZATION_QUERY_KEYS,
        invalid_message="query de authorization_url inválido",
        unknown_message="authorization_url contiene parámetros no reconocidos",
    )
    required = {"client_id", "redirect_uri", "scope", "state"}
    if not required.issubset(values) or any(not values[key].strip() for key in required):
        raise ActivationError("authorization_url requiere client_id, redirect_uri, scope y state")
    redirect_uri = values["redirect_uri"]
    if not _is_loopback_redirect(redirect_uri):
        raise ActivationError("authorization_url contiene un redirect_uri no loopback")
    scopes = _scope_set(str(values["scope"]).replace(",", " ").split())
    if len(scopes) != 1:
        raise ActivationError("authorization_url requiere exactamente un scope")
    if "product" in values and values["product"] != "web":
        raise ActivationError("authorization_url contiene un product no soportado")
    values["scope"] = next(iter(scopes))
    return parsed, values


def _unique_query_values(
    query: str,
    *,
    allowed: Set[str],
    invalid_message: str,
    unknown_message: str,
) -> dict[str, str]:
    try:
        pairs = parse_qsl(query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise ActivationError(invalid_message) from exc
    values: dict[str, str] = {}
    for key, value in pairs:
        if key not in allowed:
            raise ActivationError(unknown_message)
        if key in values:
            raise ActivationError(f"parámetro OAuth {key} duplicado")
        values[key] = value
    return values


def _validate_transaction_marker(raw: Mapping[str, Any], token_ref: str) -> Mapping[str, Any]:
    """Validate marker identity and keep the marker content non-secret."""

    if raw.get("version") != 1:
        raise UnsafeTokenStore("versión de marker OAuth inválida")
    if str(raw.get("token_ref", "")) != str(token_ref):
        raise UnsafeTokenStore("marker OAuth no corresponde al token_ref")
    state = str(raw.get("state", ""))
    if state not in {"IN_FLIGHT", "UNKNOWN", "REAUTH_PENDING"}:
        raise UnsafeTokenStore("estado de marker OAuth inválido")
    if not _TOKEN_REF.fullmatch(str(raw.get("attempt_id", ""))):
        raise UnsafeTokenStore("attempt_id de marker OAuth inválido")
    if not _TRANSACTION_ID.fullmatch(str(raw.get("transaction_id", ""))):
        raise UnsafeTokenStore("transaction_id de marker OAuth inválido")
    if not _FINGERPRINT.fullmatch(str(raw.get("candidate_proof", ""))):
        raise UnsafeTokenStore("candidate_proof de marker OAuth inválido")
    if str(raw.get("kind", "")) not in {"exchange", "refresh"}:
        raise UnsafeTokenStore("tipo de marker OAuth inválido")
    generation = raw.get("current_generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise UnsafeTokenStore("current_generation de marker OAuth inválida")
    _utc(raw.get("created_at", ""))
    return raw


def _is_loopback_redirect(uri: str) -> bool:
    try:
        parsed = urlparse(uri)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        and port is not None
        and 1 <= port <= 65535
        and bool(parsed.path)
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )


def _attempt_client_fingerprint(
    authorization_url: str,
    csrf_state: str,
    requested_scopes: frozenset[str],
    configured_fingerprint: str,
) -> str:
    _, authorization = _parse_authorization_handoff(authorization_url)
    if not secrets.compare_digest(authorization["state"], csrf_state):
        raise ActivationError("state del authorization_url no coincide con el intento")
    if _scope_set((authorization["scope"],)) != requested_scopes:
        raise ActivationError("scope del authorization_url no coincide con el intento")
    derived = hashlib.sha256(authorization["client_id"].encode("utf-8")).hexdigest()
    if configured_fingerprint and configured_fingerprint != derived:
        raise ActivationError("client_id del intento no coincide con authorization_url")
    return derived


def _validate_attempt_phase(
    phase: OAuthAttemptPhase,
    authorization_code: str | None,
    callback_received_at: datetime | None,
    token_ref: str | None,
) -> None:
    if phase is OAuthAttemptPhase.AWAITING_CALLBACK and (
        authorization_code is not None or callback_received_at is not None
    ):
        raise ActivationError("un intento pendiente no puede contener callback")
    if phase is OAuthAttemptPhase.CALLBACK_RECEIVED and (not authorization_code or callback_received_at is None):
        raise ActivationError("CALLBACK_RECEIVED requiere código y momento observado")
    if phase is OAuthAttemptPhase.TOKEN_STORED:
        if not token_ref:
            raise ActivationError("TOKEN_STORED requiere token_ref")
        if authorization_code is not None:
            raise ActivationError("TOKEN_STORED debe purgar authorization_code")


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
            if not approved_oauth_endpoint(value, token=name == "token_url"):
                raise ActivationError(f"{name} no es un endpoint cTrader aprobado")
            parsed = urlparse(value)
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise ActivationError(f"{name} debe ser una URL HTTPS base, sin credenciales, query ni fragmento")
            object.__setattr__(self, name, value)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> OAuthAppConfig:
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
    scopes: frozenset[str] | None = None

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
        if self.scopes is not None:
            object.__setattr__(self, "scopes", _scope_set(self.scopes))

    @classmethod
    def from_response(cls, payload: Mapping[str, Any]) -> OAuthTokenPayload:
        if not isinstance(payload, Mapping):
            raise ActivationError("respuesta OAuth no es un objeto")
        error = payload.get("errorCode") or payload.get("error")
        if error:
            # Never echo response bodies, codes or token-like values to logs.
            raise ActivationError("intercambio OAuth rechazado; revise el estado de la aplicación y el código vigente")
        try:
            access = payload.get("accessToken", payload.get("access_token"))
            refresh = payload.get("refreshToken", payload.get("refresh_token"))
            expires = payload.get("expiresIn", payload.get("expires_in", 0))
            token_type = payload.get("tokenType", payload.get("token_type", "bearer"))
            raw_scopes = payload.get("scope", payload.get("scopes", payload.get("grantedScopes")))
            parsed_scopes = None
            if raw_scopes is not None:
                if isinstance(raw_scopes, str):
                    parsed_scopes = _oauth_scope_set(raw_scopes.replace(",", " ").split())
                else:
                    parsed_scopes = _oauth_scope_set(raw_scopes)
            return cls(
                access_token=str(access) if access is not None else "",
                refresh_token=(str(refresh) if refresh is not None else None),
                expires_in=int(expires),
                token_type=str(token_type),
                scopes=parsed_scopes,
            )
        except (TypeError, ValueError):
            raise ActivationError("respuesta OAuth incompleta") from None


_DEMO_SERVER_ENDPOINTS = frozenset({"demo.ctraderapi.com:5035", "demo.ctraderapi.com:5036"})


def validate_demo_server_endpoint(value: str) -> str:
    endpoint = str(value).strip().lower()
    if endpoint not in _DEMO_SERVER_ENDPOINTS:
        raise RealAccountForbidden("el endpoint de activación no es DEMO")
    return endpoint


@dataclasses.dataclass(frozen=True, slots=True, repr=False)
class OAuthTokenCandidate:
    """In-memory token bound to one OAuth endpoint, ref and next generation."""

    payload: OAuthTokenPayload = dataclasses.field(repr=False)
    token_ref: str
    token_url: str
    store_generation: int
    server_endpoint: str = ""
    connection_generation: int = 0
    transaction_id: str = ""
    candidate_proof: str = ""
    transaction_attempt_id: str = ""
    _transaction: Any = dataclasses.field(default=None, repr=False, compare=False)
    token_fingerprint: str = dataclasses.field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.payload, OAuthTokenPayload):
            raise ActivationError("candidate OAuth inválido")
        if not _TOKEN_REF.fullmatch(str(self.token_ref).strip()):
            raise ActivationError("token_ref de candidate inválido")
        parsed = urlparse(str(self.token_url).strip())
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ActivationError("token_url del candidate no es HTTPS base")
        if isinstance(self.store_generation, bool) or int(self.store_generation) < 1:
            raise ActivationError("store_generation de candidate inválida")
        object.__setattr__(self, "token_ref", str(self.token_ref).strip())
        object.__setattr__(self, "token_url", str(self.token_url).strip())
        object.__setattr__(self, "store_generation", int(self.store_generation))
        endpoint = str(self.server_endpoint).strip()
        if endpoint:
            endpoint = validate_demo_server_endpoint(endpoint)
        object.__setattr__(self, "server_endpoint", endpoint)
        if isinstance(self.connection_generation, bool) or int(self.connection_generation) < 0:
            raise ActivationError("connection_generation de candidate inválida")
        object.__setattr__(self, "connection_generation", int(self.connection_generation))
        transaction_fields = (
            str(self.transaction_id).strip(),
            str(self.candidate_proof).strip(),
            str(self.transaction_attempt_id).strip(),
        )
        if any(transaction_fields):
            if (
                not all(transaction_fields)
                or not _TRANSACTION_ID.fullmatch(transaction_fields[0])
                or not _FINGERPRINT.fullmatch(transaction_fields[1])
                or not _TOKEN_REF.fullmatch(transaction_fields[2])
            ):
                raise ActivationError("transaction binding de candidate inválido")
            object.__setattr__(self, "transaction_id", transaction_fields[0])
            object.__setattr__(self, "candidate_proof", transaction_fields[1])
            object.__setattr__(self, "transaction_attempt_id", transaction_fields[2])
        object.__setattr__(self, "token_fingerprint", hashlib.sha256(self.payload.access_token.encode()).hexdigest())

    @property
    def access_token(self) -> str:
        return self.payload.access_token

    def bind_connection(self, generation: int) -> OAuthTokenCandidate:
        if isinstance(generation, bool) or int(generation) < 1:
            raise ActivationError("connection_generation inválida")
        return dataclasses.replace(self, connection_generation=int(generation))

    def __repr__(self) -> str:
        return (
            f"OAuthTokenCandidate(token_ref={self.token_ref!r}, store_generation={self.store_generation}, "
            f"connection_generation={self.connection_generation}, secrets='REDACTED')"
        )

    __str__ = __repr__


def build_authorization_url(
    app: OAuthAppConfig,
    *,
    client_id: str,
    scope: str | Sequence[str],
    state: str,
) -> str:
    """Build one deterministic cTrader OAuth URL without opening a browser."""

    client_id = str(client_id).strip()
    state_value = str(state).strip()
    if not client_id:
        raise ActivationError("client_id no puede estar vacío")
    if not state_value:
        raise ActivationError("state OAuth es obligatorio")
    raw_scopes = (
        [item for item in str(scope).replace(",", " ").split() if item]
        if isinstance(scope, str)
        else [str(item) for item in scope]
    )
    scopes = _scope_set(raw_scopes)
    if not scopes:
        raise ActivationError("scope OAuth no puede estar vacío")
    if len(scopes) != 1:
        raise ActivationError(
            "solicite un solo scope OAuth por intento; accounts y trading se autorizan en fases separadas"
        )
    parsed = urlparse(app.authorization_url)
    query = (
        ("client_id", client_id),
        ("redirect_uri", app.redirect_uri),
        ("scope", " ".join(sorted(scopes))),
        ("product", "web"),
        ("state", state_value),
    )
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(query), ""))


def parse_callback_uri(
    uri: str,
    *,
    expected_state: str,
    registered_uri: str,
) -> str:
    """Extract exactly one code from the exact registered loopback callback."""

    parsed = urlparse(str(uri))
    registered = urlparse(str(registered_uri))
    if not _is_loopback_redirect(registered_uri):
        raise ActivationError("registered_uri no es un callback loopback válido")
    callback_base = (
        parsed.scheme,
        parsed.hostname,
        parsed.port,
        parsed.path,
        parsed.params,
    )
    registered_base = (
        registered.scheme,
        registered.hostname,
        registered.port,
        registered.path,
        registered.params,
    )
    if (
        callback_base != registered_base
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ActivationError("callback OAuth no coincide exactamente con la URI registrada")
    try:
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
    except ValueError as exc:
        raise ActivationError("query del callback OAuth inválido") from exc
    allowed = {"code", "state", "error", "error_description"}
    if any(key not in allowed for key, _ in pairs):
        raise ActivationError("callback OAuth contiene parámetros no reconocidos")
    values: dict[str, str] = {}
    for key, value in pairs:
        if key in values:
            raise ActivationError(f"callback OAuth contiene {key} duplicado")
        values[key] = value
    received_state = values.get("state", "")
    if not received_state or not secrets.compare_digest(received_state, str(expected_state)):
        raise ActivationError("state OAuth no coincide")
    has_code = bool(values.get("code", "").strip())
    has_error = bool(values.get("error", "").strip())
    if has_code == has_error:
        raise ActivationError("callback OAuth requiere exactamente code o error")
    if has_error:
        raise ActivationError("el usuario o el proveedor rechazó la autorización OAuth")
    return values["code"].strip()


def open_authorization_browser(
    authorization_url: str,
    *,
    allow_browser: bool = False,
    opener: Any = None,
) -> bool:
    """Open the browser only after an explicit API flag.

    Tests should inject an opener; webbrowser is imported only after the
    explicit gate has passed.
    """

    if allow_browser is not True:
        raise ActivationError("abrir navegador requiere allow_browser=True explícito")
    parsed = urlparse(str(authorization_url).strip())
    if parsed.query:
        _parse_authorization_handoff(authorization_url)
    elif not approved_oauth_endpoint(parsed.geturl(), token=False):
        raise ActivationError("authorization_url no corresponde a un endpoint aprobado")
    if opener is None:
        import webbrowser

        opener = webbrowser.open
    return bool(opener(str(authorization_url)))


def _safe_token_request(
    app: OAuthAppConfig,
    params: Mapping[str, str],
    *,
    requester: Any,
    timeout: float = 10.0,
) -> OAuthTokenPayload:
    """Execute only an explicitly injected transport; never fall back to network."""

    if requester is None or not callable(requester):
        raise ActivationError("intercambio OAuth requiere un transporte explícito")
    try:
        response = requester(app.token_url, dict(params), timeout)
    except Exception:
        raise ActivationError("falló la solicitud OAuth; revise conectividad y estado de la aplicación") from None
    return OAuthTokenPayload.from_response(cast(Mapping[str, Any], response))


def exchange_authorization_code(
    app: OAuthAppConfig, *, client_id: str, client_secret: str, code: str, requester: Any = None, timeout: float = 10.0
) -> OAuthTokenPayload:
    """Exchange a one-minute authorization code; caller decides where to store tokens."""
    if not isinstance(client_secret, str) or not client_secret:
        raise ActivationError("client_secret debe llegar por el proveedor seguro, no por configuración")
    code = str(code).strip()
    if not code:
        raise ActivationError("authorization code vacío")
    return _safe_token_request(
        app,
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": app.redirect_uri,
            "client_id": str(client_id),
            "client_secret": client_secret,
        },
        requester=requester,
        timeout=timeout,
    )


def refresh_access_token(
    app: OAuthAppConfig,
    *,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    requester: Any = None,
    timeout: float = 10.0,
) -> OAuthTokenPayload:
    """Rotate access/refresh atomically; never assume the old refresh token survives."""
    if (
        not isinstance(client_secret, str)
        or not client_secret
        or not isinstance(refresh_token, str)
        or not refresh_token
    ):
        raise ActivationError("credenciales OAuth de renovación incompletas")
    return _safe_token_request(
        app,
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": str(client_id),
            "client_secret": client_secret,
        },
        requester=requester,
        timeout=timeout,
    )


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
            raise ActivationError(f"required_scopes para {mode.value.lower()} debe ser exactamente {sorted(minimum)}")
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
    def from_mapping(cls, value: Mapping[str, Any]) -> ActivationProfile:
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
    permissions: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        account_id = str(self.account_id).strip()
        if not account_id:
            raise ActivationError("account_id no puede estar vacío")
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "environment", str(self.environment).strip().upper() or "UNKNOWN")
        if self.permissions is None:
            permissions = frozenset()
        elif isinstance(self.permissions, str):
            permissions = frozenset(
                item.strip().lower() for item in self.permissions.replace(",", " ").split() if item.strip()
            )
        elif isinstance(self.permissions, (set, frozenset, list, tuple)):
            permissions = frozenset(str(item).strip().lower() for item in self.permissions)
        else:
            permissions = frozenset({str(self.permissions).strip().lower()})
        object.__setattr__(self, "permissions", permissions)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> BrokerAccount:
        return cls(
            account_id=str(value.get("account_id", value.get("id", ""))),
            environment=str(value.get("environment", value.get("account_type", ""))),
            label=str(value.get("label", "")),
            permissions=value.get("permissions", value.get("permission_scope", value.get("permissionScope", ()))),
        )

    def redacted(self) -> dict[str, Any]:
        return {
            "account_id": redact_identifier(self.account_id),
            "environment": self.environment,
            "label": self.label,
            "permissions": sorted(self.permissions),
        }


# These are the exact values documented by Spotware for
# ProtoOAGetAccountListByAccessTokenRes.permissionScope.  Do not replace this
# map with substring matching: a requested OAuth scope is not evidence that the
# server granted it.
_PERMISSION_SCOPE_VALUES = {
    0: "SCOPE_VIEW",
    1: "SCOPE_TRADE",
}
_PERMISSION_SCOPE_SCOPES = {
    "SCOPE_VIEW": frozenset({"accounts"}),
    "SCOPE_TRADE": frozenset({"accounts", "trading"}),
}


def _permission_scope_name(value: Any) -> str:
    """Resolve only the official cTrader permission-scope enum values."""

    named = getattr(value, "name", None)
    if named is not None:
        value = named
    if isinstance(value, bool) or value is None:
        raise ActivationError("permissionScope cTrader ausente o inválido")
    if isinstance(value, int):
        name = _PERMISSION_SCOPE_VALUES.get(value)
        if name is None:
            raise ActivationError("permissionScope cTrader desconocido")
        return name
    text = str(value).strip().upper()
    if text.isdigit():
        name = _PERMISSION_SCOPE_VALUES.get(int(text))
        if name is None:
            raise ActivationError("permissionScope cTrader desconocido")
        return name
    if text not in _PERMISSION_SCOPE_SCOPES:
        raise ActivationError("permissionScope cTrader desconocido")
    return text


def scopes_from_permission_scope(value: Any) -> frozenset[str]:
    """Map server-observed ``permissionScope`` to the exact granted scopes."""

    return _PERMISSION_SCOPE_SCOPES[_permission_scope_name(value)]


def _validate_binding_fields(
    *,
    token_fingerprint: str,
    token_ref: str,
    token_url: str,
    store_generation: int,
    server_endpoint: str,
    connection_generation: int,
) -> None:
    fields = (
        str(token_fingerprint),
        str(token_ref),
        str(token_url),
        int(store_generation),
        str(server_endpoint),
        int(connection_generation),
    )
    if not any(fields):
        return
    if not all(fields):
        raise ActivationError("evidencia de token incompleta")
    if len(fields[0]) != 64 or any(char not in "0123456789abcdef" for char in fields[0].lower()):
        raise ActivationError("fingerprint de token inválido")
    if not approved_oauth_endpoint(fields[2], token=True):
        raise ActivationError("endpoint de evidencia no está aprobado")
    if fields[3] < 1 or fields[5] < 1:
        raise ActivationError("generation de evidencia inválida")
    validate_demo_server_endpoint(fields[4])


@dataclasses.dataclass(frozen=True, slots=True)
class VerifiedDemoAuthorization:
    """Server proof that the OAuth grant exposes DEMO accounts only."""

    authorized_account_ids: tuple[str, ...]
    environment: str
    permission_scope: str
    granted_scopes: frozenset[str]
    token_fingerprint: str = ""
    token_ref: str = ""
    token_url: str = ""
    store_generation: int = 0
    server_endpoint: str = ""
    connection_generation: int = 0

    def __post_init__(self) -> None:
        account_ids = tuple(str(item).strip() for item in self.authorized_account_ids if str(item).strip())
        if not account_ids or len(set(account_ids)) != len(account_ids):
            raise ActivationError("la evidencia DEMO requiere cuentas autorizadas únicas")
        environment = str(self.environment).strip().upper()
        if environment != "DEMO":
            raise RealAccountForbidden("la evidencia de activación debe ser exclusivamente DEMO")
        scope_name = _permission_scope_name(self.permission_scope)
        scopes = _scope_set(self.granted_scopes)
        if scopes != _PERMISSION_SCOPE_SCOPES[scope_name]:
            raise ActivationError("scopes no coinciden con permissionScope observado")
        object.__setattr__(self, "authorized_account_ids", account_ids)
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "permission_scope", scope_name)
        object.__setattr__(self, "granted_scopes", scopes)
        _validate_binding_fields(
            token_fingerprint=self.token_fingerprint,
            token_ref=self.token_ref,
            token_url=self.token_url,
            store_generation=self.store_generation,
            server_endpoint=self.server_endpoint,
            connection_generation=self.connection_generation,
        )
        object.__setattr__(self, "token_ref", str(self.token_ref).strip())
        object.__setattr__(self, "token_url", str(self.token_url).strip())
        object.__setattr__(self, "store_generation", int(self.store_generation))
        object.__setattr__(self, "server_endpoint", str(self.server_endpoint).strip())

    def redacted(self) -> dict[str, Any]:
        return {
            "authorized_account_ids": [redact_identifier(item) for item in self.authorized_account_ids],
            "environment": self.environment,
            "permission_scope": self.permission_scope,
            "granted_scopes": sorted(self.granted_scopes),
            "token_bound": bool(self.token_fingerprint),
            "source": "ProtoOAGetAccountListByAccessTokenRes.permissionScope",
        }


@dataclasses.dataclass(frozen=True, slots=True)
class VerifiedAccountAuthorization:
    """Server evidence required before persisting an OAuth token.

    The evidence must come from ``ProtoOAGetAccountListByAccessTokenRes`` (or
    the lossless normalized mapping produced from it).  It is deliberately
    separate from an OAuth request, login page, or requested scope.
    """

    selected_account_id: str
    environment: str
    permission_scope: str
    granted_scopes: frozenset[str]
    token_fingerprint: str = ""
    token_ref: str = ""
    token_url: str = ""
    store_generation: int = 0
    server_endpoint: str = ""
    connection_generation: int = 0

    def __post_init__(self) -> None:
        account_id = str(self.selected_account_id).strip()
        if not account_id:
            raise ActivationError("la evidencia DEMO requiere account_id")
        environment = str(self.environment).strip().upper()
        if environment != "DEMO":
            raise RealAccountForbidden("la evidencia de activación debe ser una cuenta DEMO")
        scope_name = _permission_scope_name(self.permission_scope)
        scopes = _scope_set(self.granted_scopes)
        if scopes != _PERMISSION_SCOPE_SCOPES[scope_name]:
            raise ActivationError("scopes no coinciden con permissionScope observado")
        object.__setattr__(self, "selected_account_id", account_id)
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "permission_scope", scope_name)
        object.__setattr__(self, "granted_scopes", scopes)
        _validate_binding_fields(
            token_fingerprint=self.token_fingerprint,
            token_ref=self.token_ref,
            token_url=self.token_url,
            store_generation=self.store_generation,
            server_endpoint=self.server_endpoint,
            connection_generation=self.connection_generation,
        )
        object.__setattr__(self, "token_ref", str(self.token_ref).strip())
        object.__setattr__(self, "token_url", str(self.token_url).strip())
        object.__setattr__(self, "store_generation", int(self.store_generation))
        object.__setattr__(self, "server_endpoint", str(self.server_endpoint).strip())

    def redacted(self) -> dict[str, Any]:
        return {
            "selected_account_id": redact_identifier(self.selected_account_id),
            "environment": self.environment,
            "permission_scope": self.permission_scope,
            "granted_scopes": sorted(self.granted_scopes),
            "token_bound": bool(self.token_fingerprint),
            "source": "ProtoOAGetAccountListByAccessTokenRes.permissionScope",
        }


def verify_server_account_discovery(
    discovery: Mapping[str, Any],
    *,
    requested_scopes: Sequence[str] | Set[str],
    selected_account_id: str | int,
    expected_environment: str = "DEMO",
    token_candidate: OAuthTokenCandidate | None = None,
) -> VerifiedAccountAuthorization:
    """Verify permission and DEMO identity from a server account response.

    ``permissionScope`` is the exact cTrader enum field.  Account metadata or
    the OAuth request alone cannot satisfy this function.  The grant must be
    limited to server-observed DEMO accounts before an account is selected.
    """

    demo = verify_server_demo_discovery(
        discovery,
        requested_scopes=requested_scopes,
        expected_environment=expected_environment,
        token_candidate=token_candidate,
    )
    selected_text = str(selected_account_id).strip()
    if selected_text not in demo.authorized_account_ids:
        raise ActivationError("la cuenta seleccionada no coincide exactamente con el descubrimiento")
    return VerifiedAccountAuthorization(
        selected_account_id=selected_text,
        environment=demo.environment,
        permission_scope=demo.permission_scope,
        granted_scopes=demo.granted_scopes,
        token_fingerprint=demo.token_fingerprint,
        token_ref=demo.token_ref,
        token_url=demo.token_url,
        store_generation=demo.store_generation,
        server_endpoint=demo.server_endpoint,
        connection_generation=demo.connection_generation,
    )


def _normalized_server_accounts(discovery: Mapping[str, Any]) -> tuple[BrokerAccount, ...]:
    raw_records = discovery.get("records", discovery.get("accounts", ()))
    if isinstance(raw_records, (str, bytes, Mapping)):
        raw_records = (raw_records,)
    try:
        records = tuple(raw_records)
    except TypeError as exc:
        raise ActivationError("descubrimiento cTrader sin cuentas") from exc
    normalized: list[BrokerAccount] = []
    for item in records:
        if isinstance(item, BrokerAccount):
            normalized.append(item)
        elif isinstance(item, Mapping):
            normalized.append(BrokerAccount.from_mapping(item))
        else:
            raise ActivationError("registro de cuenta cTrader inválido")
    if not normalized:
        raise ActivationError("descubrimiento cTrader sin cuentas")
    return tuple(normalized)


def _verify_access_token_echo(discovery: Mapping[str, Any], candidate: OAuthTokenCandidate | None) -> None:
    if candidate is None:
        return
    observed = discovery.get("accessToken", discovery.get("access_token"))
    if not isinstance(observed, str) or not observed:
        raise ActivationError("la respuesta de discovery no contiene accessToken observado")
    if not secrets.compare_digest(observed, candidate.access_token):
        raise ActivationError("el accessToken de discovery no corresponde al candidate")


def verify_server_demo_discovery(
    discovery: Mapping[str, Any],
    *,
    requested_scopes: Sequence[str] | Set[str],
    expected_environment: str = "DEMO",
    token_candidate: OAuthTokenCandidate | None = None,
) -> VerifiedDemoAuthorization:
    """Verify that the OAuth grant exposes only server-observed DEMO accounts."""

    if not isinstance(discovery, Mapping):
        raise ActivationError("descubrimiento cTrader inválido")
    if str(expected_environment).strip().upper() != "DEMO":
        raise RealAccountForbidden("la verificación sólo admite environment=DEMO")
    requested = _scope_set(requested_scopes)
    if not requested:
        raise ActivationError("requested_scopes no puede estar vacío")
    # cTrader's single ``trading`` OAuth value is full account+trading access;
    # normalize it before comparing with the server enum.
    if requested == frozenset({"trading"}):
        requested = frozenset({"accounts", "trading"})
    permission = discovery.get("permissionScope", discovery.get("permission_scope"))
    granted = scopes_from_permission_scope(permission)
    if granted != requested:
        raise ActivationError("permissionScope observado no coincide con el alcance solicitado")
    _verify_access_token_echo(discovery, token_candidate)
    normalized = _normalized_server_accounts(discovery)
    environments = {item.environment for item in normalized}
    if "LIVE" in environments or "REAL" in environments:
        raise RealAccountForbidden("el consentimiento OAuth incluye una cuenta REAL/LIVE")
    if environments != {"DEMO"}:
        raise ActivationError("el entorno de todas las cuentas autorizadas no está observado como DEMO")
    return VerifiedDemoAuthorization(
        authorized_account_ids=tuple(item.account_id for item in normalized),
        environment="DEMO",
        permission_scope=_permission_scope_name(permission),
        granted_scopes=granted,
        token_fingerprint=token_candidate.token_fingerprint if token_candidate else "",
        token_ref=token_candidate.token_ref if token_candidate else "",
        token_url=token_candidate.token_url if token_candidate else "",
        store_generation=token_candidate.store_generation if token_candidate else 0,
        server_endpoint=token_candidate.server_endpoint if token_candidate else "",
        connection_generation=token_candidate.connection_generation if token_candidate else 0,
    )


def _require_candidate_binding(
    candidate: OAuthTokenCandidate,
    verification: VerifiedDemoAuthorization | VerifiedAccountAuthorization,
    *,
    token_ref: str,
    token_url: str,
    server_endpoint: str,
) -> None:
    if not isinstance(candidate, OAuthTokenCandidate):
        raise ActivationError("se requiere candidate OAuth no persistido")
    if not verification.token_fingerprint:
        raise ActivationError("la evidencia de permiso no está ligada al token")
    if (
        verification.token_fingerprint != candidate.token_fingerprint
        or verification.token_ref != candidate.token_ref
        or verification.token_url != candidate.token_url
        or verification.store_generation != candidate.store_generation
        or not candidate.server_endpoint
        or not candidate.connection_generation
        or verification.server_endpoint != candidate.server_endpoint
        or verification.connection_generation != candidate.connection_generation
        or candidate.token_ref != str(token_ref).strip()
        or candidate.token_url != str(token_url).strip()
        or candidate.server_endpoint != validate_demo_server_endpoint(server_endpoint)
    ):
        raise ActivationError("la evidencia de permiso no corresponde al token y generación activos")


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
    selected_account: Mapping[str, Any] | None = None

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

    def __init__(
        self,
        root: str | Path,
        *,
        project_root: str | Path,
        fixture: bool = False,
    ) -> None:
        raw_root = Path(root).expanduser().absolute()
        if any(item.is_symlink() for item in (raw_root, *raw_root.parents)):
            raise UnsafeTokenStore("token_store_dir y sus padres no pueden ser symlinks")
        self.root = raw_root.resolve(strict=False)
        self.project_root = Path(project_root).expanduser().resolve(strict=False)
        self.is_fixture = bool(fixture)
        if self.root == self.project_root or self.root.is_relative_to(self.project_root):
            raise UnsafeTokenStore("token_store_dir debe estar fuera del árbol del proyecto")
        temp_root = Path(tempfile.gettempdir()).resolve()
        if self.is_fixture and not self.root.is_relative_to(temp_root):
            raise UnsafeTokenStore("los tokens fixture sólo pueden usar un directorio temporal aislado")

    @classmethod
    def for_fixture(cls, root: str | Path, *, project_root: str | Path) -> SecureTokenStore:
        """Create a fixture-only store that can never target the real token path."""

        return cls(root, project_root=project_root, fixture=True)

    def _check_root(self, *, strict_mode: bool = False) -> None:
        if any(item.is_symlink() for item in (self.root, *self.root.parents)):
            raise UnsafeTokenStore("token_store_dir no puede ser symlink")
        if not self.root.exists():
            return
        info = self.root.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            raise UnsafeTokenStore("token_store_dir debe ser un directorio privado del usuario actual")
        if strict_mode and stat.S_IMODE(info.st_mode) != 0o700:
            raise UnsafeTokenStore("token_store_dir debe tener permisos exactos 0700")

    def _prepare(self) -> None:
        self._check_root()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        root_info = self.root.stat()
        if (
            not stat.S_ISDIR(root_info.st_mode)
            or stat.S_IMODE(root_info.st_mode) != 0o700
            or root_info.st_uid != os.getuid()
        ):
            raise UnsafeTokenStore("token_store_dir debe ser del usuario actual y tener permisos 0700")

    def _path(self, token_ref: str) -> Path:
        if not _TOKEN_REF.fullmatch(str(token_ref)):
            raise ActivationError("token_ref inválido")
        return self.root / f"{token_ref}.json"

    def _marker_path(self, token_ref: str) -> Path:
        self._path(token_ref)
        return self.root / f".oauth-transaction-{token_ref}.json"

    def _open_rotation_fd(self, token_ref: str) -> int:
        self._path(token_ref)
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(self.root / f".{token_ref}.lock", flags, 0o600)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise UnsafeTokenStore("lock OAuth inseguro")
            return fd
        except BaseException:
            os.close(fd)
            raise

    @staticmethod
    def _unlock_fd(fd: int) -> None:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _read_private_json(self, target: Path, *, description: str) -> Mapping[str, Any]:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(target, flags)
        except OSError as exc:
            raise UnsafeTokenStore(f"no se pudo abrir {description} de forma segura") from exc
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()
            ):
                raise UnsafeTokenStore(f"{description} debe ser un archivo privado regular 0600")
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                fd = -1
                raw = json.load(handle)
        finally:
            if fd >= 0:
                os.close(fd)
        if not isinstance(raw, Mapping):
            raise ActivationError(f"{description} inválido")
        return raw

    def _read_transaction_marker(self, token_ref: str) -> Mapping[str, Any] | None:
        self._check_root(strict_mode=True)
        target = self._marker_path(token_ref)
        if not os.path.lexists(target):
            return None
        if target.is_symlink():
            raise UnsafeTokenStore("marker OAuth no puede ser symlink")
        return _validate_transaction_marker(self._read_private_json(target, description="marker OAuth"), token_ref)

    def transaction_state(self, token_ref: str) -> str | None:
        """Return a blocking transaction state without exposing marker values."""

        self._check_root(strict_mode=True)
        marker = self._read_transaction_marker(token_ref)
        return str(marker["state"]) if marker is not None else None

    def _write_private_json(self, target: Path, payload: Mapping[str, Any], *, prefix: str) -> None:
        temporary = self.root / f".{prefix}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                fd = -1
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
            if fd >= 0:
                os.close(fd)
            if temporary.exists():
                temporary.unlink()

    @staticmethod
    def _marker_matches(marker: Mapping[str, Any], transaction: _OAuthTransaction) -> bool:
        return (
            str(marker.get("token_ref", "")) == transaction.token_ref
            and str(marker.get("transaction_id", "")) == transaction.transaction_id
            and str(marker.get("candidate_proof", "")) == transaction.candidate_proof
            and str(marker.get("attempt_id", "")) == transaction.attempt_id
            and int(marker.get("current_generation", -1)) == transaction.current_generation
        )

    def _assert_transaction_marker_locked(self, transaction: _OAuthTransaction) -> Mapping[str, Any]:
        marker = self._read_transaction_marker(transaction.token_ref)
        if marker is None or marker.get("state") != "IN_FLIGHT" or not self._marker_matches(marker, transaction):
            raise ReauthorizationRequired("la transacción OAuth ya no coincide; requiere reautorización")
        return marker

    def _set_transaction_state_locked(self, transaction: _OAuthTransaction, state: str) -> None:
        marker = self._assert_transaction_marker_locked(transaction)
        if state not in {"IN_FLIGHT", "UNKNOWN"}:
            raise ActivationError("estado de transacción OAuth inválido")
        self._write_private_json(
            self._marker_path(transaction.token_ref),
            {**dict(marker), "state": state},
            prefix=f"oauth-transaction-{transaction.token_ref}",
        )

    def _remove_transaction_marker_locked(
        self,
        token_ref: str,
        *,
        transaction_id: str,
        candidate_proof: str,
        attempt_id: str,
    ) -> None:
        marker = self._read_transaction_marker(token_ref)
        if (
            marker is None
            or marker.get("state") != "IN_FLIGHT"
            or (
                str(marker.get("transaction_id", "")) != transaction_id
                or str(marker.get("candidate_proof", "")) != candidate_proof
                or str(marker.get("attempt_id", "")) != attempt_id
            )
        ):
            raise ReauthorizationRequired("la transacción OAuth no coincide; requiere reautorización")
        target = self._marker_path(token_ref)
        target.unlink()
        directory_fd = os.open(self.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def _current_generation_locked(self, token_ref: str) -> int:
        target = self._path(token_ref)
        if not os.path.lexists(target):
            return 0
        if target.is_symlink():
            raise UnsafeTokenStore("el token no puede ser symlink")
        return self._metadata_from_payload(self._load(token_ref)).generation

    def current_generation(self, token_ref: str, *, allow_reauth_pending: bool = False) -> int:
        """Read the generation used to bind a new transaction proof."""

        self._check_root(strict_mode=True)
        marker = self._read_transaction_marker(token_ref)
        if marker is not None and not (allow_reauth_pending and marker.get("state") in {"UNKNOWN", "REAUTH_PENDING"}):
            raise ReauthorizationRequired("la transacción OAuth no tiene resultado confirmado; requiere reautorización")
        return self._current_generation_locked(token_ref)

    def begin_transaction(
        self,
        token_ref: str,
        *,
        current_generation: int | None,
        attempt_id: str,
        candidate_proof: str,
        kind: str,
    ) -> _OAuthTransaction:
        """Claim a one-use OAuth operation before sending its HTTP request."""

        if kind not in {"exchange", "refresh"}:
            raise ActivationError("tipo de transacción OAuth inválido")
        if not _TOKEN_REF.fullmatch(str(token_ref)) or not _TOKEN_REF.fullmatch(str(attempt_id)):
            raise ActivationError("identidad de transacción OAuth inválida")
        if not _FINGERPRINT.fullmatch(str(candidate_proof)):
            raise ActivationError("candidate_proof de transacción OAuth inválido")
        if current_generation is not None and (
            isinstance(current_generation, bool) or not isinstance(current_generation, int) or current_generation < 0
        ):
            raise ActivationError("current_generation de transacción OAuth inválida")
        self._prepare()
        fd = self._open_rotation_fd(token_ref)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ReauthorizationRequired("hay una transacción OAuth activa; espere su resultado") from exc
            actual_generation = self._current_generation_locked(token_ref)
            if current_generation is not None and actual_generation != current_generation:
                raise ReauthorizationRequired("la generación OAuth cambió; requiere reautorización")
            marker = self._read_transaction_marker(token_ref)
            if marker is not None:
                if not (
                    marker.get("state") == "REAUTH_PENDING"
                    and marker.get("kind") == kind == "exchange"
                    and str(marker.get("attempt_id")) == str(attempt_id)
                    and str(marker.get("candidate_proof")) == str(candidate_proof)
                    and int(marker.get("current_generation", -1)) == actual_generation
                ):
                    raise ReauthorizationRequired("hay una transacción OAuth sin resultado; requiere reautorización")
                transaction_id = str(marker["transaction_id"])
                marker = {**dict(marker), "state": "IN_FLIGHT"}
            else:
                transaction_id = f"oauth-txn-{secrets.token_hex(16)}"
                marker = {
                    "version": 1,
                    "token_ref": str(token_ref),
                    "current_generation": actual_generation,
                    "attempt_id": str(attempt_id),
                    "candidate_proof": str(candidate_proof),
                    "transaction_id": transaction_id,
                    "kind": kind,
                    "state": "IN_FLIGHT",
                    "created_at": _iso(datetime.now(UTC)),
                }
            self._write_private_json(
                self._marker_path(token_ref),
                marker,
                prefix=f"oauth-transaction-{token_ref}",
            )
            return _OAuthTransaction(
                store=self,
                token_ref=str(token_ref),
                current_generation=actual_generation,
                attempt_id=str(attempt_id),
                candidate_proof=str(candidate_proof),
                transaction_id=transaction_id,
                fd=fd,
                kind=kind,
            )
        except BaseException:
            self._unlock_fd(fd)
            raise

    def prepare_reauthorization(self, token_ref: str, *, attempt_id: str, candidate_proof: str) -> None:
        """Bind a stale outcome to a new explicit authorization attempt."""

        if not _TOKEN_REF.fullmatch(str(attempt_id)):
            raise ActivationError("attempt_id inválido")
        if not _FINGERPRINT.fullmatch(str(candidate_proof)):
            raise ActivationError("candidate_proof de reautorización inválido")
        self._prepare()
        fd = self._open_rotation_fd(token_ref)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ReauthorizationRequired("hay una transacción OAuth activa; espere su resultado") from exc
            marker = self._read_transaction_marker(token_ref)
            if marker is not None:
                current_generation = self._current_generation_locked(token_ref)
                pending = {
                    **dict(marker),
                    "state": "REAUTH_PENDING",
                    "kind": "exchange",
                    "current_generation": current_generation,
                    "attempt_id": str(attempt_id),
                    "candidate_proof": str(candidate_proof),
                    "transaction_id": f"oauth-txn-{secrets.token_hex(16)}",
                    "created_at": _iso(datetime.now(UTC)),
                }
                self._write_private_json(
                    self._marker_path(token_ref),
                    pending,
                    prefix=f"oauth-transaction-{token_ref}",
                )
        finally:
            self._unlock_fd(fd)

    def rotate(
        self,
        token_ref: str,
        *,
        access_token: str,
        refresh_token: str | None,
        granted_scopes: Sequence[str] | Set[str],
        expires_at: datetime | str,
        now: datetime | None = None,
        fixture_payload: bool = False,
        expected_generation: int | None = None,
    ) -> TokenMetadata:
        """Serialize rotations and optionally reject a stale verified token."""
        if bool(fixture_payload) != self.is_fixture:
            raise UnsafeTokenStore(
                "fixture_payload y tipo de token store deben coincidir; nunca mezcle fixtures con tokens reales"
            )
        if not isinstance(access_token, str) or not access_token:
            raise ActivationError("access_token no puede estar vacío")
        if refresh_token is not None and (not isinstance(refresh_token, str) or not refresh_token):
            raise ActivationError("refresh_token debe ser texto no vacío o None")
        self._prepare()
        with self._rotation_lock(token_ref):
            target = self._path(token_ref)
            if target.is_symlink():
                raise UnsafeTokenStore("el token no puede ser symlink, incluso sin target")
            if self._read_transaction_marker(token_ref) is not None:
                raise ReauthorizationRequired("hay una transacción OAuth sin resultado; requiere reautorización")
            previous = self._current_generation_locked(token_ref)
            if expected_generation is not None and previous != expected_generation:
                raise ActivationError("store_generation OAuth cambió; revalidación requerida")
            return self._rotate_locked(
                token_ref,
                access_token=access_token,
                refresh_token=refresh_token,
                granted_scopes=granted_scopes,
                expires_at=expires_at,
                now=now,
                fixture_payload=fixture_payload,
            )

    @contextmanager
    def _rotation_lock(self, token_ref: str) -> Iterator[None]:
        self._path(token_ref)  # Validate before constructing the stable lock path.
        fd = self._open_rotation_fd(token_ref)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def _rotate_locked(
        self,
        token_ref: str,
        *,
        access_token: str,
        refresh_token: str | None,
        granted_scopes: Sequence[str] | Set[str],
        expires_at: datetime | str,
        now: datetime | None = None,
        fixture_payload: bool = False,
    ) -> TokenMetadata:
        """Atomically replace one token envelope and return redacted metadata."""

        self._prepare()
        target = self._path(token_ref)
        if target.is_symlink():
            raise UnsafeTokenStore("el token no puede ser symlink")
        previous = self._metadata_from_payload(self._load(token_ref)) if target.exists() else None
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
        self._reject_blocking_transaction(token_ref)
        raw = self._load(token_ref)
        metadata = self._metadata_from_payload(raw)
        access_token = raw.get("access_token")
        refresh_token = raw.get("refresh_token")
        if not isinstance(access_token, str) or not access_token:
            raise ActivationError("sobre de token inválido")
        return TokenLease(metadata, access_token, refresh_token)

    def metadata(self, token_ref: str) -> TokenMetadata:
        self._reject_blocking_transaction(token_ref)
        return self._metadata_from_payload(self._load(token_ref))

    def _reject_blocking_transaction(self, token_ref: str) -> None:
        if self._read_transaction_marker(token_ref) is not None:
            raise ReauthorizationRequired("la transacción OAuth no tiene resultado confirmado; requiere reautorización")

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
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()
            ):
                raise UnsafeTokenStore("el archivo de token debe ser del usuario actual y tener permisos exactos 0600")
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                fd = -1
                raw = json.load(handle)
        finally:
            if fd >= 0:
                os.close(fd)
        if not isinstance(raw, Mapping):
            raise ActivationError("sobre de token inválido")
        metadata = self._metadata_from_payload(raw)
        if metadata.token_ref != str(token_ref):
            raise UnsafeTokenStore("el archivo de token no corresponde al token_ref solicitado")
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


@dataclasses.dataclass(slots=True)
class _OAuthTransaction:
    """A held per-token lock plus a secret-free durable transaction marker."""

    store: SecureTokenStore
    token_ref: str
    current_generation: int
    attempt_id: str
    candidate_proof: str
    transaction_id: str
    fd: int
    kind: str
    active: bool = True

    def rotate(
        self,
        *,
        access_token: str,
        refresh_token: str | None,
        granted_scopes: Sequence[str] | Set[str],
        expires_at: datetime | str,
        now: datetime | None = None,
        fixture_payload: bool = False,
    ) -> TokenMetadata:
        self._assert_active()
        self.store._assert_transaction_marker_locked(self)
        actual_generation = self.store._current_generation_locked(self.token_ref)
        if actual_generation != self.current_generation:
            raise ReauthorizationRequired("la generación OAuth cambió durante la transacción")
        return self.store._rotate_locked(
            self.token_ref,
            access_token=access_token,
            refresh_token=refresh_token,
            granted_scopes=granted_scopes,
            expires_at=expires_at,
            now=now,
            fixture_payload=fixture_payload,
        )

    def commit(self) -> None:
        self._assert_active()
        try:
            self.store._remove_transaction_marker_locked(
                self.token_ref,
                transaction_id=self.transaction_id,
                candidate_proof=self.candidate_proof,
                attempt_id=self.attempt_id,
            )
        finally:
            self._release()

    def mark_unknown(self) -> None:
        if not self.active:
            return
        try:
            self.store._set_transaction_state_locked(self, "UNKNOWN")
        finally:
            self._release()

    def _assert_active(self) -> None:
        if not self.active:
            raise ReauthorizationRequired("la transacción OAuth ya terminó; requiere reautorización")

    def _release(self) -> None:
        if self.active:
            self.active = False
            self.store._unlock_fd(self.fd)

    def __del__(self) -> None:
        if getattr(self, "active", False):
            try:
                self.mark_unknown()
            except Exception:
                with suppress(Exception):
                    self._release()


class OAuthAttemptPhase(str, enum.Enum):  # noqa: UP042 - preserve public string enum behavior
    AWAITING_CALLBACK = "AWAITING_CALLBACK"
    CALLBACK_RECEIVED = "CALLBACK_RECEIVED"
    TOKEN_STORED = "TOKEN_STORED"


@dataclasses.dataclass(frozen=True, slots=True, repr=False)
class OAuthAttempt:
    """Persistable OAuth state; repr and public output hide state/code values."""

    attempt_id: str
    csrf_state: str = dataclasses.field(repr=False)
    requested_scopes: frozenset[str]
    authorization_url: str = dataclasses.field(repr=False)
    created_at: datetime
    expires_at: datetime
    phase: OAuthAttemptPhase = OAuthAttemptPhase.AWAITING_CALLBACK
    authorization_code: str | None = dataclasses.field(default=None, repr=False)
    callback_received_at: datetime | None = None
    token_ref: str | None = None
    client_id_fingerprint: str = dataclasses.field(default="", repr=False)

    def __post_init__(self) -> None:
        if not _TOKEN_REF.fullmatch(str(self.attempt_id)):
            raise ActivationError("attempt_id inválido")
        if not isinstance(self.csrf_state, str) or len(self.csrf_state) < 32:
            raise ActivationError("csrf_state insuficiente")
        requested_scopes = _scope_set(self.requested_scopes)
        object.__setattr__(self, "requested_scopes", requested_scopes)
        client_id_fingerprint = str(self.client_id_fingerprint).strip().lower()
        object.__setattr__(
            self,
            "client_id_fingerprint",
            _attempt_client_fingerprint(
                self.authorization_url, self.csrf_state, requested_scopes, client_id_fingerprint
            ),
        )
        created = _utc(self.created_at)
        expires = _utc(self.expires_at)
        if expires <= created:
            raise ActivationError("expires_at del intento debe ser posterior a created_at")
        object.__setattr__(self, "created_at", created)
        object.__setattr__(self, "expires_at", expires)
        phase = self.phase if isinstance(self.phase, OAuthAttemptPhase) else OAuthAttemptPhase(str(self.phase))
        object.__setattr__(self, "phase", phase)
        callback_received = _utc(self.callback_received_at) if self.callback_received_at is not None else None
        object.__setattr__(self, "callback_received_at", callback_received)
        _validate_attempt_phase(phase, self.authorization_code, callback_received, self.token_ref)
        if self.token_ref is not None:
            token_ref = str(self.token_ref).strip()
            if not _TOKEN_REF.fullmatch(token_ref):
                raise ActivationError("token_ref del intento inválido")
            object.__setattr__(self, "token_ref", token_ref)

    def __repr__(self) -> str:
        return f"OAuthAttempt(attempt_id={self.attempt_id!r}, phase={self.phase.value!r}, secrets='REDACTED')"

    __str__ = __repr__

    def is_expired(self, now: datetime) -> bool:
        return self.expires_at <= _utc(now)

    def validate_binding(
        self,
        *,
        app: OAuthAppConfig,
        client_id: str,
        token_ref: str,
        profile_scopes: Sequence[str] | Set[str] | None = None,
        require_token_ref: bool = True,
    ) -> None:
        """Bind a persisted attempt to its current application/profile."""

        parsed, authorization = _parse_authorization_handoff(self.authorization_url)
        if parsed._replace(query="", fragment="").geturl().rstrip("/") != app.authorization_url.rstrip("/"):
            raise ReauthorizationRequired("endpoint de autorización del intento cambió; requiere reautorización")
        if authorization["redirect_uri"] != app.redirect_uri:
            raise ReauthorizationRequired("redirect_uri del intento cambió; requiere reautorización")
        if _scope_set((authorization["scope"],)) != self.requested_scopes:
            raise ReauthorizationRequired("scope del intento cambió; requiere reautorización")
        if require_token_ref and (not self.token_ref or self.token_ref != str(token_ref).strip()):
            raise ReauthorizationRequired("token_ref del intento cambió; requiere reautorización")
        if self.token_ref is not None and self.token_ref != str(token_ref).strip():
            raise ReauthorizationRequired("token_ref del intento cambió; requiere reautorización")
        if not secrets.compare_digest(
            self.client_id_fingerprint,
            hashlib.sha256(str(client_id).strip().encode("utf-8")).hexdigest(),
        ):
            raise ReauthorizationRequired("client_id del intento cambió; requiere reautorización")
        if profile_scopes is not None and _oauth_scope_set(self.requested_scopes) != _oauth_scope_set(profile_scopes):
            raise ReauthorizationRequired("el intento no coincide con el perfil OAuth vigente")

    def to_public_dict(self, *, now: datetime | None = None) -> dict[str, Any]:
        instant = _utc(now or datetime.now(UTC))
        return {
            "attempt_id": self.attempt_id,
            "phase": self.phase.value,
            "requested_scopes": sorted(self.requested_scopes),
            "created_at": _iso(self.created_at),
            "expires_at": _iso(self.expires_at),
            "expired": self.is_expired(instant),
            "state_fingerprint": hashlib.sha256(self.csrf_state.encode("utf-8")).hexdigest()[:16],
            "authorization_code": "REDACTED" if self.authorization_code else None,
            "callback_received_at": (
                _iso(self.callback_received_at) if self.callback_received_at is not None else None
            ),
            "token_ref": self.token_ref,
            "resumable": self.phase is not OAuthAttemptPhase.TOKEN_STORED and not self.is_expired(instant),
            "next_action": (
                "Inicia un intento OAuth nuevo."
                if self.is_expired(instant)
                else {
                    OAuthAttemptPhase.AWAITING_CALLBACK: "Completa la autorización y entrega el callback loopback exacto.",
                    OAuthAttemptPhase.CALLBACK_RECEIVED: "Intercambia el código vigente mediante un transporte explícito.",
                    OAuthAttemptPhase.TOKEN_STORED: "Descubre las cuentas autorizadas antes de seleccionar una cuenta DEMO.",
                }[self.phase]
            ),
        }

    def _to_private_dict(self) -> dict[str, Any]:
        return {
            "version": 1,
            "attempt_id": self.attempt_id,
            "csrf_state": self.csrf_state,
            "requested_scopes": sorted(self.requested_scopes),
            "authorization_url": self.authorization_url,
            "created_at": _iso(self.created_at),
            "expires_at": _iso(self.expires_at),
            "phase": self.phase.value,
            "authorization_code": self.authorization_code,
            "callback_received_at": (
                _iso(self.callback_received_at) if self.callback_received_at is not None else None
            ),
            "token_ref": self.token_ref,
            "client_id_fingerprint": self.client_id_fingerprint,
        }

    @classmethod
    def _from_private_dict(cls, raw: Mapping[str, Any]) -> OAuthAttempt:
        if raw.get("version") != 1:
            raise ActivationError("versión del intento OAuth inválida")
        try:
            return cls(
                attempt_id=str(raw["attempt_id"]),
                csrf_state=str(raw["csrf_state"]),
                requested_scopes=frozenset(raw["requested_scopes"]),
                authorization_url=str(raw["authorization_url"]),
                created_at=_utc(raw["created_at"]),
                expires_at=_utc(raw["expires_at"]),
                phase=OAuthAttemptPhase(str(raw["phase"])),
                authorization_code=(
                    str(raw["authorization_code"]) if raw.get("authorization_code") is not None else None
                ),
                callback_received_at=(
                    _utc(raw["callback_received_at"]) if raw.get("callback_received_at") is not None else None
                ),
                token_ref=(str(raw["token_ref"]) if raw.get("token_ref") is not None else None),
                client_id_fingerprint=str(raw.get("client_id_fingerprint", "")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ActivationError("intento OAuth persistido inválido") from exc


class OAuthAttemptStore:
    """Private resumable state, isolated from token envelope filenames."""

    def __init__(
        self,
        root: str | Path,
        *,
        project_root: str | Path,
        fixture: bool = False,
    ) -> None:
        self._guard = SecureTokenStore(root, project_root=project_root, fixture=fixture)
        self.root = self._guard.root
        self.is_fixture = self._guard.is_fixture

    @classmethod
    def for_fixture(cls, root: str | Path, *, project_root: str | Path) -> OAuthAttemptStore:
        return cls(root, project_root=project_root, fixture=True)

    def _path(self, attempt_id: str) -> Path:
        if not _TOKEN_REF.fullmatch(str(attempt_id)):
            raise ActivationError("attempt_id inválido")
        return self.root / f".oauth-attempt-{attempt_id}.json"

    def save(self, attempt: OAuthAttempt) -> None:
        self._guard._prepare()
        target = self._path(attempt.attempt_id)
        if target.exists():
            previous = self.load(attempt.attempt_id)
            ranks = {
                OAuthAttemptPhase.AWAITING_CALLBACK: 0,
                OAuthAttemptPhase.CALLBACK_RECEIVED: 1,
                OAuthAttemptPhase.TOKEN_STORED: 2,
            }
            if ranks[attempt.phase] < ranks[previous.phase]:
                raise ActivationError("no se permite retroceder la fase OAuth")
            if not secrets.compare_digest(previous.csrf_state, attempt.csrf_state):
                raise ActivationError("csrf_state del intento persistido no coincide")
            if (
                previous.authorization_code
                and attempt.authorization_code
                and not secrets.compare_digest(previous.authorization_code, attempt.authorization_code)
            ):
                raise ActivationError("authorization_code del intento persistido no coincide")
        temporary = self.root / f".{attempt.attempt_id}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(
                    attempt._to_private_dict(),
                    handle,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
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

    def load(self, attempt_id: str) -> OAuthAttempt:
        self._guard._check_root(strict_mode=True)
        target = self._path(attempt_id)
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            fd = os.open(target, flags)
        except OSError as exc:
            raise ActivationError("intento OAuth no encontrado o inseguro") from exc
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()
            ):
                raise UnsafeTokenStore("el intento OAuth debe ser un archivo regular privado 0600")
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                fd = -1
                raw = json.load(handle)
        finally:
            if fd >= 0:
                os.close(fd)
        if not isinstance(raw, Mapping):
            raise ActivationError("intento OAuth persistido inválido")
        attempt = OAuthAttempt._from_private_dict(raw)
        if attempt.attempt_id != str(attempt_id):
            raise UnsafeTokenStore("el intento OAuth no corresponde al nombre solicitado")
        return attempt


@dataclasses.dataclass(frozen=True, slots=True)
class AccountDiscovery:
    """Observed account inventory; a selection must reference this snapshot."""

    token_ref: str
    observed_scopes: frozenset[str]
    observed_at: datetime
    accounts: tuple[BrokerAccount, ...]
    permission_scope: str | int | None = None

    def __post_init__(self) -> None:
        if not _TOKEN_REF.fullmatch(str(self.token_ref)):
            raise ActivationError("token_ref del descubrimiento inválido")
        scopes = _scope_set(self.observed_scopes)
        if "accounts" not in scopes:
            raise ActivationError("descubrimiento de cuentas requiere scope accounts observado")
        normalized = tuple(
            item if isinstance(item, BrokerAccount) else BrokerAccount.from_mapping(item) for item in self.accounts
        )
        if not normalized:
            raise ActivationError("el descubrimiento no contiene cuentas")
        ids = [item.account_id for item in normalized]
        if len(set(ids)) != len(ids):
            raise ActivationError("el descubrimiento contiene account_id duplicados")
        permission = self.permission_scope
        if permission is not None:
            permission = str(getattr(permission, "name", permission)).upper()
        object.__setattr__(self, "permission_scope", permission)
        object.__setattr__(self, "observed_scopes", scopes)
        object.__setattr__(self, "observed_at", _utc(self.observed_at))
        object.__setattr__(self, "accounts", normalized)

    def redacted(self) -> dict[str, Any]:
        return {
            "token_ref": self.token_ref,
            "observed_scopes": sorted(self.observed_scopes),
            "observed_at": _iso(self.observed_at),
            "permission_scope": self.permission_scope,
            "accounts": [account.redacted() for account in self.accounts],
        }


def record_account_discovery(
    token: TokenMetadata,
    accounts: Sequence[BrokerAccount | Mapping[str, Any]],
    *,
    observed_at: datetime | None = None,
    permission_scope: str | int | None = None,
) -> AccountDiscovery:
    """Record caller-observed accounts; this function performs no discovery I/O."""

    return AccountDiscovery(
        token_ref=token.token_ref,
        observed_scopes=token.granted_scopes,
        observed_at=observed_at or datetime.now(UTC),
        accounts=tuple(
            item if isinstance(item, BrokerAccount) else BrokerAccount.from_mapping(item) for item in accounts
        ),
        permission_scope=permission_scope,
    )


def select_discovered_demo_account(
    discovery: AccountDiscovery,
    account_id: str,
    *,
    environment: str,
    token_ref: str,
) -> BrokerAccount:
    if discovery.token_ref != str(token_ref):
        raise ActivationError("el inventario de cuentas no corresponde al token_ref activo")
    return select_demo_account(discovery.accounts, account_id, environment=environment)


def _callback_handler(
    assistant: Any,
    attempt_id: str,
    registered: Any,
    outcome: dict[str, Any],
) -> type[Any]:
    """Build the request handler without exposing the callback query."""

    import http.server

    class CallbackHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requested = urlparse(self.path)
            callback_uri = urlunparse((registered.scheme, registered.netloc, requested.path, "", requested.query, ""))
            try:
                outcome["attempt"] = assistant.receive_callback(
                    attempt_id,
                    callback_uri,
                    now=datetime.now(UTC),
                )
                body = b"Autorizacion recibida. Puede cerrar esta ventana."
                self.send_response(200)
            except Exception:
                outcome["error"] = ActivationError("callback OAuth rechazado")
                body = b"Callback OAuth rechazado. Puede cerrar esta ventana."
                self.send_response(400)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            return

    return CallbackHandler


def _serve_callback(
    assistant: Any,
    attempt_id: str,
    attempt: OAuthAttempt,
    registered: Any,
    outcome: dict[str, Any],
    *,
    open_browser: bool,
    opener: Any,
    timeout_seconds: float,
) -> None:
    """Serve exactly one local callback, closing the socket on every path."""

    import http.server
    import socket
    import time

    hostname = registered.hostname
    port = registered.port
    if hostname is None or port is None:
        raise ActivationError("redirect_uri no contiene endpoint loopback completo")
    handler = _callback_handler(assistant, attempt_id, registered, outcome)
    if hostname == "::1":

        class IPv6HTTPServer(http.server.HTTPServer):
            address_family = socket.AF_INET6

        server: http.server.HTTPServer = IPv6HTTPServer((hostname, port, 0, 0), handler)
    else:
        server = http.server.HTTPServer((hostname, port), handler)
    server.timeout = min(1.0, float(timeout_seconds))
    deadline = time.monotonic() + float(timeout_seconds)
    try:
        if open_browser:
            open_authorization_browser(
                attempt.authorization_url,
                allow_browser=True,
                opener=opener,
            )
        while "attempt" not in outcome and "error" not in outcome and time.monotonic() < deadline:
            server.handle_request()
    finally:
        server.server_close()


def _callback_result(outcome: Mapping[str, Any]) -> OAuthAttempt:
    attempt = outcome.get("attempt")
    if isinstance(attempt, OAuthAttempt):
        return attempt
    error = outcome.get("error")
    if isinstance(error, BaseException):
        raise error
    raise ActivationError("callback OAuth no recibido dentro del tiempo indicado")


def _activation_preflight(
    profile: ActivationProfile,
    *,
    token: TokenMetadata | None,
    present_env_keys: Set[str],
    app: OAuthAppConfig,
    now: datetime | None,
) -> ActivationStatus | None:
    """Return a blocking status before account inventory is considered."""

    if not profile.enabled:
        return ActivationStatus(
            ActivationState.DISABLED,
            False,
            profile.operation_mode,
            (),
            "Habilita el perfil explícitamente.",
        )
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
    return None


def _normalize_accounts(
    accounts: Sequence[BrokerAccount | Mapping[str, Any]],
) -> tuple[BrokerAccount, ...]:
    return tuple(item if isinstance(item, BrokerAccount) else BrokerAccount.from_mapping(item) for item in accounts)


def _account_selection_status(
    profile: ActivationProfile,
    accounts: Sequence[BrokerAccount],
) -> BrokerAccount | ActivationStatus:
    try:
        return select_demo_account(accounts, profile.account_id, environment=profile.environment)
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


class LoopbackOAuthAssistant:
    """Restart-safe orchestration around pure OAuth helpers and external stores."""

    def __init__(
        self,
        app: OAuthAppConfig,
        *,
        attempts: OAuthAttemptStore,
        tokens: SecureTokenStore,
        fixture_mode: bool = False,
        code_ttl_seconds: int = 60,
    ) -> None:
        if attempts.is_fixture != tokens.is_fixture or bool(fixture_mode) != tokens.is_fixture:
            raise UnsafeTokenStore("los stores de intentos/tokens y fixture_mode deben pertenecer al mismo aislamiento")
        self.app = app
        self.attempts = attempts
        if isinstance(code_ttl_seconds, bool) or not 1 <= int(code_ttl_seconds) <= 300:
            raise ActivationError("code_ttl_seconds debe estar entre 1 y 300")
        self.tokens = tokens
        self.fixture_mode = bool(fixture_mode)
        self.code_ttl_seconds = int(code_ttl_seconds)

    def _transaction_for_candidate(self, candidate: OAuthTokenCandidate) -> _OAuthTransaction | None:
        transaction = getattr(candidate, "_transaction", None)
        if not isinstance(transaction, _OAuthTransaction):
            return None
        if transaction.store is not self.tokens or not transaction.active:
            raise ReauthorizationRequired("candidate OAuth sin transacción activa; requiere reautorización")
        if (
            candidate.transaction_id != transaction.transaction_id
            or candidate.candidate_proof != transaction.candidate_proof
            or candidate.transaction_attempt_id != transaction.attempt_id
            or candidate.token_ref != transaction.token_ref
        ):
            raise ReauthorizationRequired("candidate OAuth no corresponde a su transacción")
        return transaction

    def abandon_candidate(self, candidate: OAuthTokenCandidate | None) -> None:
        """Leave a durable UNKNOWN marker when external outcome is uncertain."""

        if candidate is None:
            return
        transaction = self._transaction_for_candidate(candidate)
        if transaction is not None:
            transaction.mark_unknown()

    def _candidate_transaction_or_fixture(self, candidate: OAuthTokenCandidate) -> _OAuthTransaction | None:
        transaction = self._transaction_for_candidate(candidate)
        if transaction is None and not self.fixture_mode:
            raise ReauthorizationRequired("candidate OAuth no está ligado a una transacción; requiere reautorización")
        return transaction

    def _next_generation(self, token_ref: str) -> int:
        if not _TOKEN_REF.fullmatch(str(token_ref).strip()):
            raise ActivationError("token_ref inválido")
        target = self.tokens.root / f"{str(token_ref).strip()}.json"
        if not target.exists():
            return 1
        return self.tokens.metadata(str(token_ref).strip()).generation + 1

    def begin(
        self,
        *,
        client_id: str,
        requested_scopes: Sequence[str] | Set[str],
        token_ref: str | None = None,
        open_browser: bool = False,
        opener: Any = None,
        now: datetime | None = None,
        ttl_seconds: int = 600,
    ) -> OAuthAttempt:
        if isinstance(ttl_seconds, bool) or int(ttl_seconds) < 30:
            raise ActivationError("ttl_seconds debe ser entero >= 30")
        instant = _utc(now or datetime.now(UTC))
        client_id_value = str(client_id).strip()
        if not client_id_value:
            raise ActivationError("client_id no puede estar vacío")
        if token_ref is not None and not _TOKEN_REF.fullmatch(str(token_ref).strip()):
            raise ActivationError("token_ref del intento inválido")
        attempt_id = f"oauth-{secrets.token_hex(12)}"
        csrf_state = secrets.token_urlsafe(32)
        scopes = _scope_set(requested_scopes)
        url = build_authorization_url(
            self.app,
            client_id=client_id_value,
            scope=sorted(scopes),
            state=csrf_state,
        )
        attempt = OAuthAttempt(
            attempt_id=attempt_id,
            csrf_state=csrf_state,
            requested_scopes=scopes,
            authorization_url=url,
            created_at=instant,
            expires_at=instant + timedelta(seconds=int(ttl_seconds)),
            token_ref=str(token_ref).strip() if token_ref is not None else None,
            client_id_fingerprint=hashlib.sha256(client_id_value.encode("utf-8")).hexdigest(),
        )
        self.attempts.save(attempt)
        if open_browser:
            open_authorization_browser(url, allow_browser=True, opener=opener)
        return attempt

    def prepare_reauthorization(
        self,
        attempt: OAuthAttempt,
        *,
        client_id: str,
        profile_scopes: Sequence[str] | Set[str],
    ) -> None:
        """Keep old-token use blocked while a new authorization is pending."""

        if not attempt.token_ref:
            raise ReauthorizationRequired("el intento no tiene token_ref; requiere reautorización")
        attempt.validate_binding(
            app=self.app,
            client_id=client_id,
            token_ref=attempt.token_ref,
            profile_scopes=profile_scopes,
        )
        current_generation = self.tokens.current_generation(attempt.token_ref, allow_reauth_pending=True)
        proof = _candidate_proof(
            kind="exchange",
            token_ref=attempt.token_ref,
            current_generation=current_generation,
            attempt_id=attempt.attempt_id,
            client_id=client_id,
            token_url=self.app.token_url,
            redirect_uri=self.app.redirect_uri,
            requested_scopes=attempt.requested_scopes,
        )
        self.tokens.prepare_reauthorization(
            attempt.token_ref,
            attempt_id=attempt.attempt_id,
            candidate_proof=proof,
        )

    def resume(self, attempt_id: str, *, now: datetime | None = None) -> dict[str, Any]:
        return self.attempts.load(attempt_id).to_public_dict(now=now)

    def resume_authorization(
        self,
        attempt_id: str,
        *,
        reveal_url: bool = False,
        open_browser: bool = False,
        opener: Any = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Resume a pending handoff; URL/browser both require explicit choices."""

        instant = _utc(now or datetime.now(UTC))
        attempt = self.attempts.load(attempt_id)
        status = attempt.to_public_dict(now=instant)
        if attempt.phase is not OAuthAttemptPhase.AWAITING_CALLBACK or attempt.is_expired(instant):
            return {**status, "authorization_url": None, "browser_opened": False}
        browser_opened = False
        if open_browser:
            browser_opened = open_authorization_browser(
                attempt.authorization_url,
                allow_browser=True,
                opener=opener,
            )
        return {
            **status,
            "authorization_url": attempt.authorization_url if reveal_url else "REDACTED",
            "browser_opened": browser_opened,
        }

    def listen_callback(
        self,
        attempt_id: str,
        *,
        open_browser: bool = False,
        opener: Any = None,
        timeout_seconds: float = 600.0,
        now: datetime | None = None,
    ) -> OAuthAttempt:
        """Serve one validated callback on the registered loopback URI.

        The handler never logs the request target (which contains the
        authorization code), binds only to the registered loopback address,
        and returns a generic browser response.
        """
        if isinstance(timeout_seconds, bool) or not 1 <= float(timeout_seconds) <= 1800:
            raise ActivationError("timeout_seconds debe estar entre 1 y 1800")
        instant = _utc(now or datetime.now(UTC))
        attempt = self.attempts.load(attempt_id)
        if attempt.phase is not OAuthAttemptPhase.AWAITING_CALLBACK:
            raise ActivationError("el intento OAuth ya no espera callback")
        if attempt.is_expired(instant):
            raise ActivationError("el intento OAuth caducó; inicia uno nuevo")
        registered = urlparse(self.app.redirect_uri)
        if not _is_loopback_redirect(self.app.redirect_uri):
            raise ActivationError("redirect_uri no es loopback")
        outcome: dict[str, Any] = {}
        _serve_callback(
            self,
            attempt_id,
            attempt,
            registered,
            outcome,
            open_browser=open_browser,
            opener=opener,
            timeout_seconds=float(timeout_seconds),
        )
        return _callback_result(outcome)

    def receive_callback(
        self,
        attempt_id: str,
        callback_uri: str,
        *,
        now: datetime | None = None,
    ) -> OAuthAttempt:
        instant = _utc(now or datetime.now(UTC))
        attempt = self.attempts.load(attempt_id)
        if attempt.phase is not OAuthAttemptPhase.AWAITING_CALLBACK:
            raise ActivationError("el callback OAuth ya fue recibido")
        if attempt.is_expired(instant):
            raise ActivationError("el intento OAuth caducó; inicia uno nuevo")
        code = parse_callback_uri(
            callback_uri,
            expected_state=attempt.csrf_state,
            registered_uri=self.app.redirect_uri,
        )
        updated = dataclasses.replace(
            attempt,
            phase=OAuthAttemptPhase.CALLBACK_RECEIVED,
            authorization_code=code,
            callback_received_at=instant,
        )
        self.attempts.save(updated)
        return updated

    def _exchange_transaction_inputs(
        self,
        attempt: OAuthAttempt,
        *,
        attempt_id: str,
        token_ref: str,
        client_id: str,
        client_secret: str,
        requester: Any,
        profile_scopes: Sequence[str] | Set[str] | None,
    ) -> tuple[str, int, str]:
        token_ref_value = str(token_ref).strip()
        if not _TOKEN_REF.fullmatch(token_ref_value):
            raise ActivationError("token_ref inválido")
        if attempt.token_ref is not None and attempt.token_ref != token_ref_value:
            raise ReauthorizationRequired("token_ref del intento no coincide; requiere reautorización")
        attempt.validate_binding(
            app=self.app,
            client_id=client_id,
            token_ref=token_ref_value,
            profile_scopes=profile_scopes,
            require_token_ref=profile_scopes is not None,
        )
        if not isinstance(client_secret, str) or not client_secret:
            raise ActivationError("client_secret debe llegar por el proveedor seguro, no por configuración")
        if requester is None or not callable(requester):
            raise ActivationError("intercambio OAuth requiere un transporte explícito")
        current_generation = self.tokens.current_generation(
            token_ref_value, allow_reauth_pending=profile_scopes is not None
        )
        proof = _candidate_proof(
            kind="exchange",
            token_ref=token_ref_value,
            current_generation=current_generation,
            attempt_id=attempt_id,
            client_id=client_id,
            token_url=self.app.token_url,
            redirect_uri=self.app.redirect_uri,
            requested_scopes=attempt.requested_scopes,
        )
        return token_ref_value, current_generation, proof

    def _validated_payload_for_persistence(
        self,
        attempt: OAuthAttempt,
        *,
        token_ref: str,
        candidate: OAuthTokenCandidate,
        observed_scopes: Sequence[str] | Set[str],
        transaction: _OAuthTransaction | None,
    ) -> tuple[OAuthTokenPayload, frozenset[str]]:
        if transaction is not None and (transaction.kind != "exchange" or transaction.attempt_id != attempt.attempt_id):
            raise ReauthorizationRequired("candidate OAuth no corresponde al intento activo")
        if candidate.token_ref != str(token_ref).strip() or candidate.token_url != self.app.token_url:
            raise ActivationError("candidate OAuth no corresponde al destino de persistencia")
        expected_generation = (
            transaction.current_generation + 1 if transaction is not None else self._next_generation(token_ref)
        )
        if candidate.store_generation != expected_generation:
            raise ActivationError("store_generation OAuth cambió antes de persistir")
        payload = candidate.payload
        scopes = _scope_set(observed_scopes)
        if payload.scopes is not None and payload.scopes != scopes:
            raise ActivationError("los scopes del token no coinciden con la evidencia observada")
        if not attempt.requested_scopes.issubset(scopes):
            raise ActivationError("los scopes observados no cubren los solicitados")
        if payload.refresh_token is None:
            raise ActivationError("el proveedor no devolvió refresh_token; no se persistió el intercambio")
        return payload, scopes

    def _rotate_candidate_payload(
        self,
        token_ref: str,
        *,
        candidate: OAuthTokenCandidate,
        payload: OAuthTokenPayload,
        scopes: frozenset[str],
        now: datetime,
        transaction: _OAuthTransaction | None,
    ) -> TokenMetadata:
        if transaction is not None:
            return transaction.rotate(
                access_token=payload.access_token,
                refresh_token=payload.refresh_token,
                granted_scopes=scopes,
                expires_at=now + timedelta(seconds=payload.expires_in),
                now=now,
                fixture_payload=self.fixture_mode,
            )
        return self.tokens.rotate(
            token_ref,
            access_token=payload.access_token,
            refresh_token=payload.refresh_token,
            granted_scopes=scopes,
            expires_at=now + timedelta(seconds=payload.expires_in),
            now=now,
            fixture_payload=self.fixture_mode,
            expected_generation=candidate.store_generation - 1,
        )

    def exchange_unpersisted(
        self,
        attempt_id: str,
        *,
        token_ref: str,
        server_endpoint: str = "",
        client_id: str,
        client_secret: str,
        requester: Any,
        profile_scopes: Sequence[str] | Set[str] | None = None,
        now: datetime | None = None,
        timeout: float = 10.0,
    ) -> OAuthTokenCandidate:
        """Exchange the callback code without persisting an unverified token.

        The caller must use the returned token immediately to obtain the
        server account response and then call :meth:`persist_verified_token`.
        The payload is kept in memory only; its repr/str is redacted.
        """

        instant = _utc(now or datetime.now(UTC))
        attempt = self.attempts.load(attempt_id)
        if attempt.phase is not OAuthAttemptPhase.CALLBACK_RECEIVED:
            raise ActivationError("el intento no está listo para intercambio")
        if attempt.is_expired(instant):
            raise ActivationError("el intento OAuth caducó; inicia un intento nuevo")
        if attempt.callback_received_at is None or instant >= attempt.callback_received_at + timedelta(
            seconds=self.code_ttl_seconds
        ):
            raise ActivationError("el código OAuth caducó; inicia un intento nuevo")
        token_ref_value, current_generation, proof = self._exchange_transaction_inputs(
            attempt,
            attempt_id=attempt_id,
            token_ref=token_ref,
            client_id=client_id,
            client_secret=client_secret,
            requester=requester,
            profile_scopes=profile_scopes,
        )
        transaction = self.tokens.begin_transaction(
            token_ref_value,
            current_generation=current_generation,
            attempt_id=attempt_id,
            candidate_proof=proof,
            kind="exchange",
        )
        try:
            payload = exchange_authorization_code(
                self.app,
                client_id=client_id,
                client_secret=client_secret,
                code=str(attempt.authorization_code),
                requester=requester,
                timeout=timeout,
            )
            return OAuthTokenCandidate(
                payload=payload,
                token_ref=token_ref_value,
                token_url=self.app.token_url,
                store_generation=transaction.current_generation + 1,
                server_endpoint=server_endpoint,
                transaction_id=transaction.transaction_id,
                candidate_proof=transaction.candidate_proof,
                transaction_attempt_id=transaction.attempt_id,
                _transaction=transaction,
            )
        except BaseException:
            transaction.mark_unknown()
            raise

    def _persist_payload(
        self,
        attempt_id: str,
        *,
        token_ref: str,
        candidate: OAuthTokenCandidate,
        observed_scopes: Sequence[str] | Set[str],
        now: datetime,
    ) -> TokenMetadata:
        """Persist a payload after a caller supplied observed scopes."""

        transaction = self._candidate_transaction_or_fixture(candidate)
        try:
            attempt = self.attempts.load(attempt_id)
            if attempt.phase is not OAuthAttemptPhase.CALLBACK_RECEIVED:
                raise ActivationError("el intento no está listo para persistencia")
            payload, scopes = self._validated_payload_for_persistence(
                attempt,
                token_ref=token_ref,
                candidate=candidate,
                observed_scopes=observed_scopes,
                transaction=transaction,
            )
            metadata = self._rotate_candidate_payload(
                token_ref,
                candidate=candidate,
                payload=payload,
                scopes=scopes,
                now=now,
                transaction=transaction,
            )
            self.attempts.save(
                dataclasses.replace(
                    attempt,
                    phase=OAuthAttemptPhase.TOKEN_STORED,
                    authorization_code=None,
                    token_ref=token_ref,
                )
            )
            if transaction is not None:
                transaction.commit()
            return metadata
        except BaseException:
            if transaction is not None and transaction.active:
                transaction.mark_unknown()
            raise

    def persist_verified_token(
        self,
        attempt_id: str,
        *,
        token_ref: str,
        candidate: OAuthTokenCandidate,
        verification: VerifiedDemoAuthorization | VerifiedAccountAuthorization,
        server_endpoint: str,
        now: datetime | None = None,
    ) -> TokenMetadata:
        """Persist a token only after exact server DEMO/scope verification."""

        transaction = self._transaction_for_candidate(candidate)
        try:
            instant = _utc(now or datetime.now(UTC))
            attempt = self.attempts.load(attempt_id)
            if attempt.phase is not OAuthAttemptPhase.CALLBACK_RECEIVED:
                raise ActivationError("el intento no está listo para persistencia")
            # The authorization code was already exchanged by exchange_unpersisted;
            # do not reapply its one-minute lifetime to the in-memory access token.
            if not isinstance(verification, (VerifiedDemoAuthorization, VerifiedAccountAuthorization)):
                raise ActivationError("se requiere evidencia de cuentas DEMO del servidor")
            _require_candidate_binding(
                candidate,
                verification,
                token_ref=token_ref,
                token_url=self.app.token_url,
                server_endpoint=server_endpoint,
            )
            if not attempt.requested_scopes.issubset(verification.granted_scopes):
                raise ActivationError("la evidencia de permiso no cubre los scopes solicitados")
            return self._persist_payload(
                attempt_id,
                token_ref=token_ref,
                candidate=candidate,
                observed_scopes=verification.granted_scopes,
                now=instant,
            )
        except BaseException:
            if transaction is not None and transaction.active:
                transaction.mark_unknown()
            raise

    def refresh_unpersisted(
        self,
        token_ref: str,
        *,
        server_endpoint: str = "",
        client_id: str,
        client_secret: str,
        requester: Any,
        timeout: float = 10.0,
    ) -> OAuthTokenCandidate:
        """Rotate access credentials in memory without writing an unverified token."""

        token_ref_value = str(token_ref).strip()
        if not _TOKEN_REF.fullmatch(token_ref_value):
            raise ActivationError("token_ref inválido")
        if not str(client_id).strip():
            raise ActivationError("client_id no puede estar vacío")
        if not isinstance(client_secret, str) or not client_secret:
            raise ActivationError("client_secret debe llegar por el proveedor seguro, no por configuración")
        if requester is None or not callable(requester):
            raise ActivationError("renovación OAuth requiere un transporte explícito")
        lease = self.tokens.read(token_ref_value)
        if not lease.refresh_token:
            raise ActivationError("el sobre activo no contiene refresh_token")
        transaction_attempt_id = f"refresh-{secrets.token_hex(12)}"
        proof = _candidate_proof(
            kind="refresh",
            token_ref=token_ref_value,
            current_generation=lease.metadata.generation,
            attempt_id=transaction_attempt_id,
            client_id=client_id,
            token_url=self.app.token_url,
            redirect_uri=self.app.redirect_uri,
            requested_scopes=lease.metadata.granted_scopes,
        )
        transaction = self.tokens.begin_transaction(
            token_ref_value,
            current_generation=lease.metadata.generation,
            attempt_id=transaction_attempt_id,
            candidate_proof=proof,
            kind="refresh",
        )
        try:
            payload = refresh_access_token(
                self.app,
                client_id=client_id,
                client_secret=client_secret,
                refresh_token=lease.refresh_token,
                requester=requester,
                timeout=timeout,
            )
            return OAuthTokenCandidate(
                payload=payload,
                token_ref=token_ref_value,
                token_url=self.app.token_url,
                store_generation=transaction.current_generation + 1,
                server_endpoint=server_endpoint,
                transaction_id=transaction.transaction_id,
                candidate_proof=transaction.candidate_proof,
                transaction_attempt_id=transaction.attempt_id,
                _transaction=transaction,
            )
        except BaseException:
            transaction.mark_unknown()
            raise

    def _rotate_payload(
        self,
        token_ref: str,
        *,
        candidate: OAuthTokenCandidate,
        observed_scopes: Sequence[str] | Set[str],
        now: datetime,
    ) -> TokenMetadata:
        transaction = self._candidate_transaction_or_fixture(candidate)
        try:
            token_ref_value = str(token_ref).strip()
            if transaction is not None and transaction.kind != "refresh":
                raise ReauthorizationRequired("candidate OAuth no corresponde a renovación")
            if candidate.token_ref != token_ref_value or candidate.token_url != self.app.token_url:
                raise ActivationError("candidate OAuth no corresponde al destino de rotación")
            expected_generation = (
                transaction.current_generation + 1
                if transaction is not None
                else self._next_generation(token_ref_value)
            )
            if candidate.store_generation != expected_generation:
                raise ActivationError("store_generation OAuth cambió antes de rotar")
            payload = candidate.payload
            scopes = _scope_set(observed_scopes)
            if payload.scopes is not None and payload.scopes != scopes:
                raise ActivationError("los scopes del token no coinciden con la evidencia observada")
            if payload.refresh_token is None:
                raise ActivationError("refresh sin token de rotación; se conserva intacto el sobre previo")
            if transaction is not None:
                metadata = transaction.rotate(
                    access_token=payload.access_token,
                    refresh_token=payload.refresh_token,
                    granted_scopes=scopes,
                    expires_at=now + timedelta(seconds=payload.expires_in),
                    now=now,
                    fixture_payload=self.fixture_mode,
                )
                transaction.commit()
                return metadata
            return self.tokens.rotate(
                token_ref_value,
                access_token=payload.access_token,
                refresh_token=payload.refresh_token,
                granted_scopes=scopes,
                expires_at=now + timedelta(seconds=payload.expires_in),
                now=now,
                fixture_payload=self.fixture_mode,
                expected_generation=candidate.store_generation - 1,
            )
        except BaseException:
            if transaction is not None and transaction.active:
                transaction.mark_unknown()
            raise

    def persist_verified_refresh(
        self,
        token_ref: str,
        *,
        candidate: OAuthTokenCandidate,
        verification: VerifiedDemoAuthorization | VerifiedAccountAuthorization,
        server_endpoint: str,
        now: datetime | None = None,
    ) -> TokenMetadata:
        """Persist a refreshed token only after a fresh server DEMO probe."""

        transaction = self._transaction_for_candidate(candidate)
        try:
            if not isinstance(verification, (VerifiedDemoAuthorization, VerifiedAccountAuthorization)):
                raise ActivationError("se requiere evidencia de cuentas DEMO del servidor")
            _require_candidate_binding(
                candidate,
                verification,
                token_ref=token_ref,
                token_url=self.app.token_url,
                server_endpoint=server_endpoint,
            )
            instant = _utc(now or datetime.now(UTC))
            return self._rotate_payload(
                token_ref,
                candidate=candidate,
                observed_scopes=verification.granted_scopes,
                now=instant,
            )
        except BaseException:
            if transaction is not None and transaction.active:
                transaction.mark_unknown()
            raise

    def exchange(
        self,
        attempt_id: str,
        *,
        client_id: str,
        client_secret: str,
        token_ref: str,
        observed_scopes: Sequence[str] | Set[str] | None = None,
        requester: Any,
        now: datetime | None = None,
        timeout: float = 10.0,
    ) -> TokenMetadata:
        if not self.fixture_mode:
            raise ActivationError("exchange legado sólo está permitido en stores fixture aislados")
        instant = _utc(now or datetime.now(UTC))
        attempt = self.attempts.load(attempt_id)
        scopes = _scope_set(observed_scopes) if observed_scopes is not None else None
        if scopes is not None and not attempt.requested_scopes.issubset(scopes):
            raise ActivationError("los scopes observados no cubren los solicitados")
        candidate = self.exchange_unpersisted(
            attempt_id,
            token_ref=token_ref,
            client_id=client_id,
            client_secret=client_secret,
            requester=requester,
            now=instant,
            timeout=timeout,
        )
        scopes = scopes if scopes is not None else candidate.payload.scopes
        if scopes is None:
            raise ActivationError("requiere permissionScope observado por la API antes de persistir")
        if candidate.payload.scopes is not None and _scope_set(scopes) != candidate.payload.scopes:
            raise ActivationError("los scopes declarados por OAuth no coinciden con los observados")
        if not attempt.requested_scopes.issubset(_scope_set(scopes)):
            raise ActivationError("los scopes observados no cubren los solicitados")
        return self._persist_payload(
            attempt_id,
            token_ref=token_ref,
            candidate=candidate,
            observed_scopes=scopes,
            now=instant,
        )

    def refresh(
        self,
        token_ref: str,
        *,
        client_id: str,
        client_secret: str,
        observed_scopes: Sequence[str] | Set[str],
        requester: Any,
        now: datetime | None = None,
        timeout: float = 10.0,
    ) -> TokenMetadata:
        if not self.fixture_mode:
            raise ActivationError("refresh legado sólo está permitido en stores fixture aislados")
        instant = _utc(now or datetime.now(UTC))
        scopes = _scope_set(observed_scopes)
        if not scopes:
            raise ActivationError("refresh requiere scopes observados")
        candidate = self.refresh_unpersisted(
            token_ref,
            client_id=client_id,
            client_secret=client_secret,
            requester=requester,
            timeout=timeout,
        )
        payload = candidate.payload
        if payload.scopes is not None:
            if not payload.scopes.issubset(scopes):
                raise ActivationError("los scopes declarados por refresh no coinciden con los observados")
            scopes = payload.scopes
        return self._rotate_payload(
            token_ref,
            candidate=candidate,
            observed_scopes=scopes,
            now=instant,
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
        if str(
            item.account_id if isinstance(item, BrokerAccount) else item.get("account_id", item.get("id", ""))
        ).strip()
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

    preflight = _activation_preflight(
        profile,
        token=token,
        present_env_keys=present_env_keys,
        app=app,
        now=now,
    )
    if preflight is not None:
        return preflight
    assert token is not None
    normalized = _normalize_accounts(accounts)
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
            "Seleccione una cuenta DEMO explícitamente, cuyo environment haya sido observado en el servidor.",
        )
    selected_or_status = _account_selection_status(profile, normalized)
    if isinstance(selected_or_status, ActivationStatus):
        return selected_or_status
    selected = selected_or_status
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
    "AccountDiscovery",
    "ActivationError",
    "LoopbackOAuthAssistant",
    "OAuthAttempt",
    "OAuthAttemptPhase",
    "OAuthAttemptStore",
    "OAuthTokenPayload",
    "build_authorization_url",
    "open_authorization_browser",
    "parse_callback_uri",
    "exchange_authorization_code",
    "refresh_access_token",
    "record_account_discovery",
    "select_discovered_demo_account",
    "ActivationMode",
    "ActivationProfile",
    "ActivationState",
    "ActivationStatus",
    "BrokerAccount",
    "OAuthAppConfig",
    "OAuthTokenCandidate",
    "ReauthorizationRequired",
    "VerifiedDemoAuthorization",
    "VerifiedAccountAuthorization",
    "RealAccountForbidden",
    "SecureTokenStore",
    "SUPPORTED_SCOPES",
    "TokenLease",
    "TokenMetadata",
    "UnsafeTokenStore",
    "evaluate_activation",
    "redact_identifier",
    "select_demo_account",
    "scopes_from_permission_scope",
    "validate_demo_server_endpoint",
    "verify_server_demo_discovery",
    "verify_server_account_discovery",
]
