"""Versioned capture envelopes and bounded, explicit capture ordering.

The durable ingestion sequence identifies an observation, even when its bytes
equal another observation. A provider sequence or payload hash does not replace
it. Raw legacy rows get file-order sequences and a named historical policy;
their original reception is never invented.
"""

from __future__ import annotations

import json
import math
import sqlite3
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import Any, Literal

from ..core.canonical import canonical_json, fingerprint, instant_text

CAPTURE_VERSION = 1
CaptureOrder = Literal["as_observed", "market_time_corrected"]
AvailabilityPolicy = Literal["observed", "historical_event_time", "unknown"]


class CaptureContractError(ValueError):
    """A capture cannot establish the claimed availability/identity contract."""


class MessageClass(StrEnum):
    SPOT = "spot"
    CLOCK = "clock"
    CONNECTION = "connection"
    REVISION = "revision"
    END = "end"


def parse_instant(value: object, *, unit: str | None = None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and unit is None:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = _numeric_instant(value, unit)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CaptureContractError("timestamp requires timezone")
    return parsed.astimezone(UTC)


def _numeric_instant(value: object, unit: str | None) -> datetime:
    if unit not in {"s", "ms", "us"} or isinstance(value, bool):
        raise CaptureContractError("numeric local timestamps require declared s/ms/us units")
    if not isinstance(value, (str, int, float)):
        raise CaptureContractError("timestamp must be an ISO string or declared JSON number")
    number = float(value)  # temporal projection only; never monetary accounting
    if not math.isfinite(number):
        raise CaptureContractError("timestamp must be finite")
    factor = {"s": 1.0, "ms": 1000.0, "us": 1_000_000.0}[unit]
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=number / factor)


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (tuple, list)):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class CaptureEnvelope:
    """An observation with a globally increasing durable ingest sequence.

    ``ingest_sequence`` is global to the capture and MUST NOT reset when the
    connection generation changes. A provider-local sequence belongs in payload.
    """

    event_time: datetime | None
    received_at: datetime | None
    available_at: datetime | None
    ingest_sequence: int
    connection_generation: int
    message_class: MessageClass
    payload: Mapping[str, Any]
    source_identity: str | None = None
    availability_policy: AvailabilityPolicy = "observed"
    schema_version: int = CAPTURE_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CAPTURE_VERSION:
            raise CaptureContractError("unsupported capture envelope version")
        for name in ("ingest_sequence", "connection_generation"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise CaptureContractError(f"{name} must be a nonnegative integer")
        for name in ("event_time", "received_at", "available_at"):
            object.__setattr__(self, name, parse_instant(getattr(self, name)))
        object.__setattr__(self, "message_class", MessageClass(self.message_class))
        self._validate_availability()
        # Validate JSON types and take ownership; callers cannot mutate a hash.
        payload = json.loads(canonical_json(self.payload))
        object.__setattr__(self, "payload", _freeze(payload))

    def _validate_availability(self) -> None:
        if self.availability_policy not in {"observed", "historical_event_time", "unknown"}:
            raise CaptureContractError("unknown availability policy")
        if self.availability_policy == "observed" and self.received_at is None:
            raise CaptureContractError("observed capture requires original received_at")
        if self.received_at and self.available_at and self.available_at < self.received_at:
            raise CaptureContractError("available_at cannot precede receipt")

    @property
    def observation_id(self) -> str:
        return f"capture-v{self.schema_version}:{self.connection_generation}:{self.ingest_sequence}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "capture_schema": self.schema_version,
            "event_time": instant_text(self.event_time) if self.event_time else None,
            "received_at": instant_text(self.received_at) if self.received_at else None,
            "available_at": instant_text(self.available_at) if self.available_at else None,
            "ingest_sequence": self.ingest_sequence,
            "connection_generation": self.connection_generation,
            "source_identity": self.source_identity,
            "message_class": self.message_class.value,
            "availability_policy": self.availability_policy,
            "payload": json.loads(canonical_json(self.payload)),
        }

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> CaptureEnvelope:
        if not isinstance(row.get("payload"), Mapping):
            raise CaptureContractError("capture envelope payload must be a mapping")
        unit = row.get("time_unit")
        return cls(
            event_time=parse_instant(row.get("event_time"), unit=unit),
            received_at=parse_instant(row.get("received_at"), unit=unit),
            available_at=parse_instant(row.get("available_at"), unit=unit),
            ingest_sequence=row["ingest_sequence"],
            connection_generation=row["connection_generation"],
            source_identity=row.get("source_identity"),
            message_class=MessageClass(row["message_class"]),
            payload=row["payload"],
            availability_policy=row.get("availability_policy", "observed"),
            schema_version=row["capture_schema"],
        )


def envelope_from_raw(
    row: Mapping[str, Any],
    sequence: int,
    *,
    received_at: datetime | None = None,
    historical_policy: AvailabilityPolicy = "historical_event_time",
) -> CaptureEnvelope:
    if "capture_schema" in row:
        return CaptureEnvelope.from_mapping(row)
    event_time = parse_instant(row.get("timestamp", row.get("timestamp_ms")), unit="ms")
    receipt = received_at or parse_instant(row.get("received_at"), unit=row.get("time_unit"))
    available = parse_instant(row.get("available_at"), unit=row.get("time_unit")) or receipt
    policy: AvailabilityPolicy = "observed" if receipt else historical_policy
    if available is None and policy == "historical_event_time":
        available = event_time
    return CaptureEnvelope(
        event_time, receipt, available, sequence, 0, MessageClass.SPOT, row, availability_policy=policy
    )


def iter_jsonl(path: str | Path) -> Iterator[Mapping[str, Any]]:
    """One decoded row at a time; diagnostics identify the failing line."""
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise CaptureContractError(f"invalid JSONL line {line_number}: {exc.msg}") from exc
            if not isinstance(row, Mapping):
                raise CaptureContractError(f"JSONL line {line_number} must be an object")
            yield row


def ordering_key(envelope: CaptureEnvelope, mode: CaptureOrder = "as_observed") -> tuple[str, int]:
    if mode not in {"as_observed", "market_time_corrected"}:
        raise CaptureContractError(f"unsupported replay order: {mode}")
    instant = envelope.event_time if mode == "market_time_corrected" else envelope.available_at
    if instant is None:
        raise CaptureContractError("unknown availability needs an explicit historical reconstruction policy")
    return instant_text(instant), envelope.ingest_sequence


def _index_envelope(conn: sqlite3.Connection, envelope: CaptureEnvelope, mode: CaptureOrder) -> None:
    order_time, sequence = ordering_key(envelope, mode)
    encoded = canonical_json(envelope.to_dict())
    previous = conn.execute("SELECT payload FROM capture WHERE sequence=?", (sequence,)).fetchone()
    if previous:
        if previous[0] != encoded:
            raise CaptureContractError(
                f"conflicting global ingest_sequence {sequence}; it must not reset across connection generations"
            )
        return
    conn.execute("INSERT INTO capture VALUES (?,?,?)", (sequence, order_time, encoded))


def iter_capture_order(
    envelopes: Iterable[CaptureEnvelope], *, mode: CaptureOrder = "as_observed"
) -> Iterator[CaptureEnvelope]:
    """Explicit external ordering stage with bounded memory and a disk index.

    Reordered files preserving the envelope are harmless. ``market_time_corrected``
    is a different research identity, never evidence of decisions as observed.
    A session already receiving ordered envelopes does not need this stage.
    """
    with CaptureIndex(envelopes, mode=mode) as index:
        yield from index


class CaptureIndex:
    """Reusable disk index; source is decoded once, ranges use bounded memory."""

    def __init__(self, envelopes: Iterable[CaptureEnvelope], *, mode: CaptureOrder = "as_observed") -> None:
        self.mode = mode
        self._directory = TemporaryDirectory(prefix="mtf-capture-order-")
        self._conn = sqlite3.connect(str(Path(self._directory.name) / "capture.sqlite3"))
        try:
            self._build(envelopes)
        except BaseException:
            self.close()
            raise

    def _build(self, envelopes: Iterable[CaptureEnvelope]) -> None:
        self._conn.execute("PRAGMA cache_size=-2048")
        self._conn.execute("PRAGMA temp_store=FILE")
        self._conn.execute("CREATE TABLE capture(sequence INTEGER PRIMARY KEY, time TEXT, payload TEXT)")
        for envelope in envelopes:
            _index_envelope(self._conn, envelope, self.mode)
        self._conn.execute("CREATE INDEX capture_order ON capture(time, sequence)")
        self._conn.commit()
        self._validate_end()

    def _validate_end(self) -> None:
        ended = False
        for envelope in self:
            if ended:
                raise CaptureContractError("capture contains observations after its declared dataset end")
            ended = envelope.message_class is MessageClass.END

    def __enter__(self) -> CaptureIndex:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._conn.close()
        self._directory.cleanup()

    def __iter__(self) -> Iterator[CaptureEnvelope]:
        return self.iter_after()

    def iter_after(self, cursor: tuple[str, int] | None = None) -> Iterator[CaptureEnvelope]:
        if cursor is None:
            rows = self._conn.execute("SELECT payload FROM capture ORDER BY time,sequence")
        else:
            rows = self._conn.execute(
                "SELECT payload FROM capture WHERE (time,sequence)>(?,?) ORDER BY time,sequence", cursor
            )
        for (payload,) in rows:
            yield CaptureEnvelope.from_mapping(json.loads(payload))

    @property
    def last_envelope(self) -> CaptureEnvelope | None:
        row = self._conn.execute("SELECT payload FROM capture ORDER BY time DESC,sequence DESC LIMIT 1").fetchone()
        return CaptureEnvelope.from_mapping(json.loads(row[0])) if row else None

    @property
    def capture_hash(self) -> str:
        return capture_fingerprint(self, mode=self.mode)


def capture_fingerprint(envelopes: Iterable[CaptureEnvelope], *, mode: CaptureOrder) -> str:
    """Hash chain is streaming and includes the replay semantics version."""
    digest = fingerprint({"capture_version": CAPTURE_VERSION, "order": mode})
    for envelope in envelopes:
        digest = fingerprint({"previous": digest, "envelope": envelope.to_dict()})
    return digest
