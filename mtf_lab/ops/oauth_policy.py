"""Pinned public OAuth endpoints; no credentials or network operations."""

from urllib.parse import urlsplit

_AUTH_PATHS = frozenset({"/my/settings/openapi/grantingaccess", "/authorize"})


def approved_oauth_endpoint(url: str, *, token: bool) -> bool:
    """Accept only the documented origin/path (and legacy auth path)."""
    try:
        parsed = urlsplit(str(url).strip())
        port = parsed.port
    except ValueError:
        return False
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.query
        or parsed.fragment
    ):
        return False
    host = "openapi.ctrader.com" if token else "id.ctrader.com"
    paths = {"/apps/token"} if token else _AUTH_PATHS
    return parsed.hostname == host and parsed.path.rstrip("/") in paths
