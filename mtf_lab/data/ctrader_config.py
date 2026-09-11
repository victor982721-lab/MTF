"""Immutable cTrader configuration and instrument-facing static metadata."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

from .ctrader_errors import CTraderConfigurationError
from .ctrader_protocol import TREND_PERIODS


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CTraderConfigurationError(f"{name} debe ser entero positivo")
    return value


def _finite(value: Any, name: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise CTraderConfigurationError(f"{name} debe ser numérico")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise CTraderConfigurationError(f"{name} debe ser numérico") from exc
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise CTraderConfigurationError(f"{name} debe ser finito y >= {minimum}")
    return result


def _validated_connection(config: Any) -> tuple[str, str, int]:
    environment = str(config.environment).strip().lower()
    if environment not in {"demo", "live"}:
        raise CTraderConfigurationError("environment debe ser demo o live")
    host = config.host or ("demo.ctraderapi.com" if environment == "demo" else "live.ctraderapi.com")
    if not str(host).strip():
        raise CTraderConfigurationError("host no puede estar vacío")
    port = _positive_int(config.port, "port")
    if port > 65_535:
        raise CTraderConfigurationError("port fuera de rango")
    return environment, str(host).strip(), port


def _validated_symbol(config: Any) -> tuple[str, int | None, int | None]:
    symbol = normalize_symbol_name(config.symbol)
    if not symbol:
        raise CTraderConfigurationError("symbol no puede estar vacío")
    symbol_id = _optional_positive(config.symbol_id, "symbol_id")
    account_id = _optional_positive(config.account_id, "account_id")
    return symbol, symbol_id, account_id


def _optional_positive(value: Any, name: str) -> int | None:
    if value is None:
        return None
    return _positive_int(value, name)


def _validated_quote_basis(value: Any) -> str:
    quote_basis = str(value).strip().lower()
    if quote_basis not in {"mid", "bid", "ask"}:
        raise CTraderConfigurationError("quote_basis debe ser mid, bid o ask")
    return quote_basis


def _validated_periods(values: Any) -> tuple[str, ...]:
    periods = tuple(str(value).strip().upper() for value in values)
    if not periods or any(value not in TREND_PERIODS for value in periods):
        raise CTraderConfigurationError(f"timeframes sólo admite {sorted(TREND_PERIODS)}")
    if len(set(periods)) != len(periods):
        raise CTraderConfigurationError("timeframes no puede repetir periodos")
    return periods


def _validated_scale(config: Any) -> tuple[int, int, int]:
    digits = _positive_int(config.digits, "digits")
    scale = _positive_int(config.price_scale, "price_scale")
    pip_position = _positive_int(config.pip_position, "pip_position")
    if pip_position > digits:
        raise CTraderConfigurationError("pip_position no puede exceder digits")
    return digits, scale, pip_position


def _normalize_limits(config: Any) -> None:
    for name in (
        "request_timeout_seconds",
        "reconnect_backoff_seconds",
        "reconnect_backoff_max_seconds",
        "heartbeat_seconds",
    ):
        object.__setattr__(config, name, _finite(getattr(config, name), name, minimum=0.001))
    object.__setattr__(config, "max_reconnects", _positive_int(config.max_reconnects, "max_reconnects"))
    object.__setattr__(config, "queue_maxsize", _positive_int(config.queue_maxsize, "queue_maxsize"))
    object.__setattr__(config, "historical_count", _positive_int(config.historical_count, "historical_count"))
    for name in ("request_rate_limit", "historical_rate_limit"):
        object.__setattr__(config, name, _finite(getattr(config, name), name, minimum=0.001))


def _validated_mapping(mapping: Mapping[str, Any], config_fields: Any) -> dict[str, Any]:
    if not isinstance(mapping, Mapping):
        raise TypeError("ctrader config debe ser un mapping")
    allowed = {item.name for item in config_fields} | {"credentials"}
    unknown = set(mapping) - allowed
    if unknown:
        raise CTraderConfigurationError(f"claves desconocidas en ctrader: {sorted(unknown)}")
    return dict(mapping)


def _merge_credentials(raw: dict[str, Any], credentials: Any) -> None:
    if credentials is None:
        return
    if not isinstance(credentials, Mapping):
        raise CTraderConfigurationError("credentials debe ser tabla")
    allowed = {"client_id", "client_secret_ref", "access_token_ref", "refresh_token_ref"}
    unknown = set(credentials) - allowed
    if unknown:
        raise CTraderConfigurationError(f"claves desconocidas en ctrader.credentials: {sorted(unknown)}")
    _reject_plaintext_secrets(credentials)
    for key, value in credentials.items():
        if key in raw:
            raise CTraderConfigurationError(f"credencial duplicada: {key}")
        raw[key] = value


def _reject_plaintext_secrets(raw: Mapping[str, Any]) -> None:
    for forbidden in ("client_secret", "access_token", "refresh_token"):
        if forbidden in raw:
            raise CTraderConfigurationError(f"{forbidden} no se acepta; no se capturan secretos")


def normalize_symbol_name(value: str) -> str:
    text = str(value).strip().upper().replace("-", "/")
    if text in {"EURUSD", "EUR/USD"}:
        return "EUR/USD"
    if "/" not in text and len(text) == 6:
        return f"{text[:3]}/{text[3:]}"
    return text


@dataclass(frozen=True, slots=True)
class CTraderConfig:
    """Secret-free configuration for a cTrader data session.

    The ``*_ref`` fields are names resolved by an external secret provider;
    plaintext secrets are rejected and never enter this object.
    """

    environment: str = "demo"
    host: str | None = None
    port: int = 5035
    symbol: str = "EUR/USD"
    symbol_id: int | None = None
    account_id: int | None = None
    client_id: str | None = None
    client_secret_ref: str | None = None
    access_token_ref: str | None = None
    refresh_token_ref: str | None = None
    quote_basis: str = "mid"
    timeframes: tuple[str, ...] = ("M1", "M5", "M15")
    digits: int = 5
    price_scale: int = 100_000
    pip_position: int = 4
    request_timeout_seconds: float = 5.0
    max_reconnects: int = 3
    reconnect_backoff_seconds: float = 1.0
    reconnect_backoff_max_seconds: float = 30.0
    heartbeat_seconds: float = 10.0
    queue_maxsize: int = 256
    historical_count: int = 500
    request_rate_limit: float = 50.0
    historical_rate_limit: float = 5.0

    def __post_init__(self) -> None:
        environment, host, port = _validated_connection(self)
        symbol, symbol_id, account_id = _validated_symbol(self)
        quote_basis = _validated_quote_basis(self.quote_basis)
        periods = _validated_periods(self.timeframes)
        digits, scale, pip_position = _validated_scale(self)
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "symbol_id", symbol_id)
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "quote_basis", quote_basis)
        object.__setattr__(self, "timeframes", periods)
        object.__setattr__(self, "digits", digits)
        object.__setattr__(self, "price_scale", scale)
        object.__setattr__(self, "pip_position", pip_position)
        _normalize_limits(self)

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> CTraderConfig:
        if mapping is None:
            return cls()
        raw = _validated_mapping(mapping, fields(cls))
        credentials = raw.pop("credentials", None)
        _merge_credentials(raw, credentials)
        _reject_plaintext_secrets(raw)
        return cls(**raw)

    @property
    def auth_configured(self) -> bool:
        return bool(self.client_id and self.client_secret_ref and self.access_token_ref)

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "host": self.host,
            "port": self.port,
            "symbol": self.symbol,
            "symbol_id": self.symbol_id,
            "account_id": self.account_id,
            "client_id_configured": bool(self.client_id),
            "client_secret_ref_configured": bool(self.client_secret_ref),
            "access_token_ref_configured": bool(self.access_token_ref),
            "refresh_token_ref_configured": bool(self.refresh_token_ref),
            "quote_basis": self.quote_basis,
            "timeframes": list(self.timeframes),
            "digits": self.digits,
            "price_scale": self.price_scale,
            "pip_position": self.pip_position,
            "request_timeout_seconds": self.request_timeout_seconds,
            "max_reconnects": self.max_reconnects,
            "reconnect_backoff_seconds": self.reconnect_backoff_seconds,
            "reconnect_backoff_max_seconds": self.reconnect_backoff_max_seconds,
            "heartbeat_seconds": self.heartbeat_seconds,
            "queue_maxsize": self.queue_maxsize,
            "historical_count": self.historical_count,
            "request_rate_limit": self.request_rate_limit,
            "historical_rate_limit": self.historical_rate_limit,
        }


__all__ = ["CTraderConfig", "normalize_symbol_name"]
