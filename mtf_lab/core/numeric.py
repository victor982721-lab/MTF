"""Small, versioned numeric policy used by the CFD domain.

The rest of MTF Lab deliberately keeps its historical float based market
records.  CFD accounting is different: quantities and monetary results are
``Decimal`` values and must not inherit a context left behind by an indicator
or a caller.  This module is intentionally small instead of being a generic
units framework.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from decimal import ROUND_HALF_UP, Context, Decimal, localcontext

DECIMAL_POLICY_VERSION = "cfd-decimal-v1"


@dataclass(frozen=True, slots=True)
class DecimalPolicy:
    """Explicit arithmetic settings for reproducible CFD calculations."""

    version: str = DECIMAL_POLICY_VERSION
    precision: int = 34
    rounding: str = ROUND_HALF_UP

    def __post_init__(self) -> None:
        if self.version != DECIMAL_POLICY_VERSION:
            raise ValueError(f"política Decimal no soportada: {self.version!r}")
        if isinstance(self.precision, bool) or not isinstance(self.precision, int) or self.precision < 16:
            raise ValueError("precision Decimal debe ser un entero >= 16")
        # ``Context`` validates the rounding name and makes this check useful
        # at construction time rather than halfway through a simulation.
        Context(prec=self.precision, rounding=self.rounding)

    def context(self) -> Context:
        """Return a detached context; callers may safely mutate the copy."""

        return Context(prec=self.precision, rounding=self.rounding)


DEFAULT_DECIMAL_POLICY = DecimalPolicy()


@contextmanager
def decimal_context(policy: DecimalPolicy = DEFAULT_DECIMAL_POLICY) -> Iterator[None]:
    """Run a block under *policy*, independent of the ambient context."""

    with localcontext(policy.context()):
        yield


def quantize_decimal(value: Decimal, exponent: Decimal, *, policy: DecimalPolicy = DEFAULT_DECIMAL_POLICY) -> Decimal:
    """Quantize using the local versioned policy, never the caller context."""

    with decimal_context(policy):
        return value.quantize(exponent, rounding=policy.rounding)


def seconds_decimal(delta: timedelta) -> Decimal:
    """Convert a timedelta to exact decimal seconds (microsecond resolution)."""

    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return Decimal(micros) / Decimal(1_000_000)


__all__ = [
    "DECIMAL_POLICY_VERSION",
    "DEFAULT_DECIMAL_POLICY",
    "DecimalPolicy",
    "decimal_context",
    "quantize_decimal",
    "seconds_decimal",
]
