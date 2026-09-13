"""Strict cTrader protocol-volume rules derived from observed symbol metadata."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

PROTOCOL_VOLUME_SCALE = 100


class VolumeRuleError(ValueError):
    """The observed symbol volume grid is absent or a quantity is invalid."""


def _protocol_integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise VolumeRuleError(f"{name} must be an integer protocol value")
    if isinstance(value, int):
        result = value
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise VolumeRuleError(f"{name} must be an integer protocol value")
        result = int(value)
    else:
        try:
            decimal = Decimal(str(value).strip())
            if not decimal.is_finite() or decimal != decimal.to_integral_value():
                raise VolumeRuleError(f"{name} must be an integer protocol value")
            result = int(decimal)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise VolumeRuleError(f"{name} must be an integer protocol value") from exc
    if result <= 0:
        raise VolumeRuleError(f"{name} must be positive")
    return result


def _field(value: Any, *names: str) -> Any:
    if not isinstance(value, Mapping):
        try:
            return next(getattr(value, name) for name in names if hasattr(value, name))
        except StopIteration:
            return None
    for name in names:
        if name in value:
            return value[name]
    return None


@dataclass(frozen=True, slots=True)
class VolumeGrid:
    """Observed protocol-unit range and step for one selected symbol."""

    min_volume: int
    max_volume: int
    step_volume: int
    volume_scale: int = PROTOCOL_VOLUME_SCALE

    def __post_init__(self) -> None:
        minimum = _protocol_integer(self.min_volume, "min_volume")
        maximum = _protocol_integer(self.max_volume, "max_volume")
        step = _protocol_integer(self.step_volume, "step_volume")
        scale = _protocol_integer(self.volume_scale, "volume_scale")
        if scale != PROTOCOL_VOLUME_SCALE:
            raise VolumeRuleError(f"volume_scale must be exactly {PROTOCOL_VOLUME_SCALE}")
        if maximum < minimum:
            raise VolumeRuleError("max_volume must be >= min_volume")
        object.__setattr__(self, "min_volume", minimum)
        object.__setattr__(self, "max_volume", maximum)
        object.__setattr__(self, "step_volume", step)
        object.__setattr__(self, "volume_scale", scale)

    @classmethod
    def from_mapping(cls, value: Any, *, volume_scale: int = PROTOCOL_VOLUME_SCALE) -> VolumeGrid:
        full = value
        if isinstance(value, Mapping):
            full = value.get("full_symbol", value.get("fullSymbol", value))
        values = {
            "min_volume": _field(full, "min_volume", "minVolume"),
            "max_volume": _field(full, "max_volume", "maxVolume"),
            "step_volume": _field(full, "step_volume", "stepVolume"),
        }
        missing = sorted(name for name, raw in values.items() if raw is None)
        if missing:
            raise VolumeRuleError("observed symbol volume grid is incomplete: " + ", ".join(missing))
        return cls(
            _protocol_integer(values["min_volume"], "min_volume"),
            _protocol_integer(values["max_volume"], "max_volume"),
            _protocol_integer(values["step_volume"], "step_volume"),
            _protocol_integer(volume_scale, "volume_scale"),
        )

    def protocol_units(self, quantity: Any, *, require_grid: bool = True) -> int:
        if isinstance(quantity, bool):
            raise VolumeRuleError("quantity must be numeric, not bool")
        try:
            decimal = Decimal(str(quantity).strip())
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise VolumeRuleError("quantity cannot be represented in protocol units") from exc
        if not decimal.is_finite() or decimal <= 0:
            raise VolumeRuleError("quantity must be a positive exact protocol multiple")
        numerator, denominator = decimal.as_integer_ratio()
        scaled = numerator * self.volume_scale
        if scaled % denominator:
            raise VolumeRuleError("quantity must be a positive exact protocol multiple")
        units = scaled // denominator
        if units <= 0:
            raise VolumeRuleError("quantity must produce positive protocol units")
        if require_grid and (units < self.min_volume or units > self.max_volume):
            raise VolumeRuleError(
                f"quantity protocol units {units} outside observed range [{self.min_volume}, {self.max_volume}]"
            )
        if require_grid and (units - self.min_volume) % self.step_volume != 0:
            raise VolumeRuleError(
                f"quantity protocol units {units} is off observed step {self.step_volume} "
                f"from minimum {self.min_volume}"
            )
        return units

    def validate_open_quantity(self, quantity: Any) -> int:
        return self.protocol_units(quantity, require_grid=True)

    def validate_close_quantity(self, quantity: Any) -> int:
        # A residual close may be below minVolume or off the opening lot grid;
        # only exact positive protocol representation is required.
        return self.protocol_units(quantity, require_grid=False)

    def to_dict(self) -> dict[str, int]:
        return {
            "min_volume": self.min_volume,
            "max_volume": self.max_volume,
            "step_volume": self.step_volume,
            "volume_scale": self.volume_scale,
        }


__all__ = ["PROTOCOL_VOLUME_SCALE", "VolumeGrid", "VolumeRuleError"]
