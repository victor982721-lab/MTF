"""Typed errors shared by the cTrader data adapter.

The cTrader integration has several failure domains that must not be collapsed
into ``TimeoutError``.  In particular, a local validation failure is known to
have happened before a request is sent, while a transport failure after a
successful send may leave the remote outcome unknown.  The session layer
attaches the request phase to those transitions; this module only owns the
stable exception taxonomy.
"""

from __future__ import annotations

from enum import StrEnum


class CTraderError(RuntimeError):
    """Base de errores del proveedor cTrader."""


class CTraderConfigurationError(CTraderError, ValueError):
    """Configuración local inválida antes de abrir una conexión."""


class CTraderDependencyError(CTraderError):
    """SDK/codec requerido no está disponible o no es importable."""


class CTraderTransportError(CTraderError):
    """Fallo de transporte, con fase sólo cuando el request la estableció."""

    def __init__(self, message: str, *, phase: RequestPhase | None = None) -> None:
        self.phase = phase
        super().__init__(message)


class CTraderProtocolError(CTraderError):
    """Error protocolario devuelto por cTrader."""

    def __init__(
        self,
        error_code: str,
        description: str = "",
        *,
        retry_after: int | None = None,
    ) -> None:
        self.error_code = str(error_code)
        self.description = str(description or "")
        self.retry_after = retry_after
        suffix = f": {self.description}" if self.description else ""
        super().__init__(f"cTrader error {self.error_code}{suffix}")


class CTraderAuthError(CTraderError):
    """La sesión carece de la autenticación o autorización requerida."""

    def __init__(self, message: str, *, action: str = "") -> None:
        self.action = action
        super().__init__(message)


class CTraderRequestTimeout(CTraderError):
    """Una solicitud no recibió respuesta dentro del plazo."""

    def __init__(self, message: str, *, phase: RequestPhase | None = None) -> None:
        self.phase = phase
        super().__init__(message)


class CTraderRequestCancelled(CTraderError):
    """Una solicitud se canceló, con fase sólo si la sesión la estableció."""

    def __init__(self, message: str, *, phase: RequestPhase | None = None) -> None:
        self.phase = phase
        super().__init__(message)


class CTraderDataError(CTraderError, ValueError):
    """Payload o dato de mercado incompatible con el contrato."""


class DependencyState(StrEnum):
    AVAILABLE = "AVAILABLE"
    MISSING = "MISSING"
    UNIMPORTABLE = "UNIMPORTABLE"
    NOT_VERIFIED = "NOT_VERIFIED"
    OPTIONAL = "OPTIONAL"


class ConnectionState(StrEnum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    FAILED = "FAILED"
    CLOSED = "CLOSED"


class AuthState(StrEnum):
    NOT_CONFIGURED = "NOT_CONFIGURED"
    REQUIRED = "REQUIRED"
    READY = "READY"
    AUTHENTICATED = "AUTHENTICATED"
    ACCOUNT_REQUIRED = "ACCOUNT_REQUIRED"
    EXPIRED = "EXPIRED"
    INVALID = "INVALID"


class RequestPhase(StrEnum):
    """Observable phases for one request attempt."""

    CREATED = "CREATED"
    WAITING_TO_SEND = "WAITING_TO_SEND"
    SENT = "SENT"
    COMPLETED = "COMPLETED"
    CANCELLED_BEFORE_SEND = "CANCELLED_BEFORE_SEND"
    CANCELLED_AFTER_SEND = "CANCELLED_AFTER_SEND"
    TIMED_OUT = "TIMED_OUT"
    FAILED_BEFORE_SEND = "FAILED_BEFORE_SEND"
    FAILED_AFTER_SEND = "FAILED_AFTER_SEND"


__all__ = [
    "AuthState",
    "ConnectionState",
    "CTraderAuthError",
    "CTraderConfigurationError",
    "CTraderDataError",
    "CTraderDependencyError",
    "CTraderError",
    "CTraderProtocolError",
    "CTraderRequestCancelled",
    "CTraderRequestTimeout",
    "CTraderTransportError",
    "DependencyState",
    "RequestPhase",
]
