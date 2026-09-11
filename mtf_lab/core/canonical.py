"""Canonical value contract used for identities, not arbitrary object reprs.

Version 1 uses UTF-8, sorted string keys, finite JSON numbers and Decimal text.
UTC instants retain microseconds. Diagnostic/session fields must be excluded by
the owning identity contract before calling ``fingerprint``.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TypeAlias

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]
CANONICAL_VERSION = 1


def instant_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("canonical instants require an explicit timezone")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _scalar(value: object) -> JSONValue:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite numbers are not canonical values")
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_value(value: object) -> JSONValue:
    if isinstance(value, Enum):
        return canonical_value(value.value)
    if isinstance(value, datetime):
        return instant_text(value)
    if isinstance(value, (Decimal, Path)):
        if isinstance(value, Decimal) and not value.is_finite():
            raise ValueError("non-finite Decimal")
        return str(value)
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise TypeError("canonical mapping keys must be strings")
        return {key: canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: canonical_value(getattr(value, field.name)) for field in fields(value)}
    return _scalar(value)


def canonical_json(value: object) -> str:
    return json.dumps(
        canonical_value(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def fingerprint(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
