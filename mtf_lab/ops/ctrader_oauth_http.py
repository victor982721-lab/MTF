"""Small, redaction-safe HTTP adapter for the cTrader OAuth token endpoint.

The cTrader documentation currently describes two HTTP shapes for the token
endpoint: an authorization-code exchange as ``GET`` with query parameters and
refresh as ``POST`` with query parameters.  This module keeps that transport
separate from the OAuth state machine so it can be tested with an injected
opener and cannot accidentally print credentials or response bodies.

The caller remains responsible for deciding when a returned token has enough
server-observed permission evidence to be persisted.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any


class OAuthHTTPError(RuntimeError):
    """A bounded OAuth HTTP or JSON-protocol failure without secret details."""


_GRANT_TYPES = frozenset({"authorization_code", "refresh_token"})
_MAX_RESPONSE_BYTES = 128 * 1024


def _validate_endpoint(url: str) -> urllib.parse.SplitResult:
    parsed = urllib.parse.urlsplit(str(url).strip())
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise OAuthHTTPError("endpoint OAuth inválido; se requiere una URL HTTPS base")
    return parsed


def _validate_params(grant_type: str, params: Mapping[str, str]) -> dict[str, str]:
    grant = str(grant_type).strip()
    if grant not in _GRANT_TYPES:
        raise OAuthHTTPError("grant_type OAuth no soportado")
    values = {str(key): str(value) for key, value in params.items()}
    values["grant_type"] = grant
    required = {
        "authorization_code": {"grant_type", "code", "redirect_uri", "client_id", "client_secret"},
        "refresh_token": {"grant_type", "refresh_token", "client_id", "client_secret"},
    }[grant]
    if any(not values.get(key, "") for key in required):
        raise OAuthHTTPError("parámetros OAuth incompletos")
    return values


def build_token_request(
    url: str,
    params: Mapping[str, str],
) -> urllib.request.Request:
    """Build the documented cTrader token request without performing I/O.

    The returned request necessarily contains OAuth credentials in its URL,
    because the documented cTrader contract places the parameters in the
    query.  This function never logs, renders or includes that URL in an
    exception.  Callers must keep the request object local and short-lived.
    """

    parsed = _validate_endpoint(url)
    values = _validate_params(str(params.get("grant_type", "")), params)
    query = urllib.parse.urlencode(values)
    request_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ""))
    grant_type = values["grant_type"]
    method = "GET" if grant_type == "authorization_code" else "POST"
    return urllib.request.Request(
        request_url,
        data=None,
        method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    )


def request_token(
    url: str,
    params: Mapping[str, str],
    timeout: float,
    *,
    opener: Callable[..., Any] | None = None,
) -> Mapping[str, Any]:
    """Perform one bounded token request through an injected/explicit opener.

    ``opener`` is injectable for offline tests.  The default is the standard
    library opener; no response body, URL or credential is echoed on failure.
    """

    if isinstance(timeout, bool) or float(timeout) <= 0:
        raise OAuthHTTPError("timeout OAuth inválido")
    request = build_token_request(url, params)
    open_request = opener or urllib.request.urlopen
    try:
        with open_request(request, timeout=float(timeout)) as response:
            body = response.read(_MAX_RESPONSE_BYTES)
    except Exception:
        raise OAuthHTTPError("solicitud OAuth falló; revise conectividad y autorización") from None
    if not isinstance(body, bytes):
        raise OAuthHTTPError("respuesta OAuth inválida")
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        raise OAuthHTTPError("respuesta OAuth no es JSON válido") from None
    if not isinstance(payload, Mapping):
        raise OAuthHTTPError("respuesta OAuth no es un objeto")
    return payload


__all__ = ["OAuthHTTPError", "build_token_request", "request_token"]
