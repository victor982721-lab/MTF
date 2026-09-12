"""Contrato mínimo para proveedores intercambiables.

Los adaptadores sólo normalizan datos y procedencia; no implementan EMA,
estrategia ni ejecución. Cualquier proveedor futuro puede cumplir este Protocol
sin cambiar el detector.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class MarketDataProvider(Protocol):
    name: str
    instrument: str

    def fetch(self, *args: Any, **kwargs: Any) -> Any:
        """Obtiene una instantánea finita para calentamiento/replay."""
        ...

    def stream(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        """Entrega eventos normalizados; el consumidor controla su duración."""
        ...


def provider_name(provider: Any) -> str:
    return str(getattr(provider, "name", getattr(provider, "provider", type(provider).__name__)))


__all__ = ["MarketDataProvider", "provider_name"]
