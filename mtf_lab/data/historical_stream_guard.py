"""Bounded source-identity guard for historical quote streams.

The historical readers already validate individual :class:`HistoricalQuote`
records.  This module validates the *stream* around those records: a stream is
bound to one immutable manifest, moves through the manifest partitions and
members exactly once, and advances its source cursor without retaining the
quotes it has seen.  The guard is deliberately independent from the readers so
it can be put in front of a backtest, a resume loop, or another consumer.

The cursor is O(1).  The only retained collection is the manifest-derived tuple
of partition/member identities (O(partitions), rather than O(quotes)).  A
manifest with the same ``sha256 + member`` identity in two partitions is
rejected up front because ``HistoricalQuote.source_event_id`` intentionally
does not include the archive path; accepting that alias would make two raw
positions indistinguishable after a restart.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeAlias

from .dukascopy import DukascopyManifest, DukascopyPartition
from .historical import (
    DatasetManifest,
    DatasetPartition,
    HistoricalDataError,
    HistoricalQuote,
    SourceLocator,
    read_manifest,
)

STREAM_GUARD_SCHEMA_VERSION = 1
"""Version of the durable stream cursor payload."""


HistoricalManifest: TypeAlias = DatasetManifest | DukascopyManifest
ManifestInput: TypeAlias = HistoricalManifest | str | Path


class HistoricalStreamIdentityError(HistoricalDataError):
    """A historical quote cannot be accepted by the bound source stream."""


# Keep a short alias for callers that do not need to distinguish the identity
# aspect from the stream contract.  Both names intentionally share the same
# exception type for ``except HistoricalDataError`` compatibility.
HistoricalStreamGuardError = HistoricalStreamIdentityError


@dataclass(frozen=True, slots=True)
class HistoricalStreamCursor:
    """The last accepted source position of a historical stream."""

    partition_index: int
    member_index: int
    locator: SourceLocator
    sequence: int
    event_time: datetime
    source_event_id: str

    def __post_init__(self) -> None:
        _strict_nonnegative_int(self.partition_index, "cursor partition_index")
        _strict_nonnegative_int(self.member_index, "cursor member_index")
        _strict_nonnegative_int(self.sequence, "cursor sequence")
        if not isinstance(self.locator, SourceLocator):
            raise HistoricalStreamIdentityError("cursor locator must be SourceLocator")
        event_time = _utc(self.event_time, "cursor event_time")
        object.__setattr__(self, "event_time", event_time)
        if not isinstance(self.source_event_id, str) or not self.source_event_id.strip():
            raise HistoricalStreamIdentityError("cursor source_event_id must be non-empty")
        expected = _source_event_id(self.locator)
        if self.source_event_id != expected:
            raise HistoricalStreamIdentityError("cursor source_event_id is not bound to its locator")

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-compatible cursor portion of a guard snapshot."""

        return {
            "partition_index": self.partition_index,
            "member_index": self.member_index,
            "locator": self.locator.to_dict(),
            "sequence": self.sequence,
            "event_time": _iso(self.event_time),
            "source_event_id": self.source_event_id,
        }


@dataclass(frozen=True, slots=True)
class _PartitionIdentity:
    """Manifest-derived position metadata retained by the guard."""

    partition_id: str
    archive: str
    sha256: str
    members: tuple[str, ...]
    locator_kind: str


class HistoricalStreamIdentityGuard:
    """Validate ordered, manifest-bound :class:`HistoricalQuote` records.

    A fresh guard starts at the first partition and first member.  A resumed
    guard must be created with :meth:`from_snapshot`; this prevents a caller
    from silently treating a suffix as a new stream.  The first and subsequent
    records are checked against the manifest, their source locator, sequence,
    and event-time watermark.  Equal timestamps are valid; source row/offset
    and sequence are the tie-breakers.
    """

    __slots__ = (
        "_manifest",
        "_manifest_hash",
        "_provider",
        "_instrument",
        "_partitions",
        "_current_partition",
        "_current_member",
        "_cursor",
    )

    def __init__(self, manifest: ManifestInput) -> None:
        resolved = _coerce_manifest(manifest)
        partitions = _partition_identities(resolved)
        if not partitions:
            raise HistoricalStreamIdentityError("historical manifest must contain partitions")
        self._manifest = resolved
        self._manifest_hash = _manifest_hash(resolved)
        self._provider = _required_text(getattr(resolved, "provider", None), "manifest provider")
        self._instrument = _required_text(getattr(resolved, "instrument", None), "manifest instrument")
        self._partitions = partitions
        self._current_partition: int | None = None
        self._current_member: int | None = None
        self._cursor: HistoricalStreamCursor | None = None

    @property
    def manifest(self) -> HistoricalManifest:
        """The exact manifest object to which this guard is bound."""

        return self._manifest

    @property
    def manifest_hash(self) -> str:
        """The manifest ``content_hash`` used for resume binding."""

        return self._manifest_hash

    @property
    def provider(self) -> str:
        return self._provider

    @property
    def instrument(self) -> str:
        return self._instrument

    @property
    def cursor(self) -> HistoricalStreamCursor | None:
        """The last accepted cursor, or ``None`` before the first quote."""

        return self._cursor

    @property
    def position(self) -> HistoricalStreamCursor | None:
        """Compatibility/readability alias for :attr:`cursor`."""

        return self._cursor

    def validate(self, quote: HistoricalQuote) -> None:
        """Accept one quote or raise without changing state.

        Validation is transactional: malformed or out-of-order input leaves
        the previous cursor intact, so a caller cannot accidentally advance a
        checkpoint on a rejected record.
        """

        if not isinstance(quote, HistoricalQuote):
            raise HistoricalStreamIdentityError("historical stream requires HistoricalQuote")
        if quote.provider != self._provider:
            raise HistoricalStreamIdentityError(
                f"quote provider {quote.provider!r} does not match manifest provider {self._provider!r}"
            )
        if quote.instrument != self._instrument:
            raise HistoricalStreamIdentityError(
                f"quote instrument {quote.instrument!r} does not match manifest instrument {self._instrument!r}"
            )
        locator = quote.locator
        if not isinstance(locator, SourceLocator):
            raise HistoricalStreamIdentityError("quote locator must be SourceLocator")
        expected_source_id = _source_event_id(locator)
        if quote.source_event_id != expected_source_id:
            raise HistoricalStreamIdentityError("quote source_event_id is not bound to its locator")

        partition_index, member_index = self._locate(locator)
        self._validate_global_progress(quote)
        if self._cursor is not None and partition_index == self._current_partition:
            assert member_index is not None
            if member_index == self._current_member:
                self._validate_member_progress(locator)

        # All checks above are side-effect free.  Only commit the new cursor
        # after the entire quote has passed.
        self._current_partition = partition_index
        self._current_member = member_index
        self._cursor = HistoricalStreamCursor(
            partition_index=partition_index,
            member_index=member_index,
            locator=locator,
            sequence=quote.sequence,
            event_time=quote.event_time,
            source_event_id=quote.source_event_id,
        )

    def accept(self, quote: HistoricalQuote) -> HistoricalQuote:
        """Validate and return ``quote`` for iterator/pipeline composition."""

        self.validate(quote)
        return quote

    def validate_many(self, quotes: Iterable[HistoricalQuote]) -> Iterable[HistoricalQuote]:
        """Yield a stream after validating each quote in source order."""

        for quote in quotes:
            self.validate(quote)
            yield quote

    def snapshot(self) -> dict[str, Any]:
        """Return a small, JSON-compatible checkpoint bound to this manifest."""

        cursor = self._cursor
        if cursor is None:
            partition_index: int | None = None
            member_index: int | None = None
            locator: dict[str, Any] | None = None
            sequence: int | None = None
            event_time: str | None = None
            source_event_id: str | None = None
        else:
            partition_index = cursor.partition_index
            member_index = cursor.member_index
            locator = cursor.locator.to_dict()
            sequence = cursor.sequence
            event_time = _iso(cursor.event_time)
            source_event_id = cursor.source_event_id
        return {
            "schema_version": STREAM_GUARD_SCHEMA_VERSION,
            "manifest_hash": self._manifest_hash,
            "provider": self._provider,
            "instrument": self._instrument,
            "partition_index": partition_index,
            "member_index": member_index,
            "locator": locator,
            "sequence": sequence,
            "event_time": event_time,
            "source_event_id": source_event_id,
        }

    @classmethod
    def from_snapshot(
        cls,
        manifest: ManifestInput,
        snapshot: Mapping[str, Any] | str,
    ) -> HistoricalStreamIdentityGuard:
        """Restore a guard only when the snapshot still matches ``manifest``."""

        guard = cls(manifest)
        raw = _snapshot_mapping(snapshot)
        _strict_snapshot_keys(raw)
        version = raw.get("schema_version")
        if version != STREAM_GUARD_SCHEMA_VERSION:
            raise HistoricalStreamIdentityError("unsupported historical stream snapshot schema_version")
        snapshot_hash = raw.get("manifest_hash")
        if not isinstance(snapshot_hash, str) or snapshot_hash.strip().lower() != guard.manifest_hash.lower():
            raise HistoricalStreamIdentityError("historical stream snapshot manifest_hash mismatch")
        provider = raw.get("provider")
        if provider != guard.provider:
            raise HistoricalStreamIdentityError("historical stream snapshot provider mismatch")
        instrument = raw.get("instrument")
        if instrument != guard.instrument:
            raise HistoricalStreamIdentityError("historical stream snapshot instrument mismatch")

        locator_raw = raw.get("locator")
        if locator_raw is None:
            if any(
                raw.get(key) is not None
                for key in ("partition_index", "member_index", "sequence", "event_time", "source_event_id")
            ):
                raise HistoricalStreamIdentityError("empty historical stream snapshot has cursor fields")
            return guard
        if not isinstance(locator_raw, Mapping):
            raise HistoricalStreamIdentityError("historical stream snapshot locator must be an object")
        locator = SourceLocator.from_mapping(locator_raw)
        partition_index = _strict_nonnegative_int(raw.get("partition_index"), "snapshot partition_index")
        member_index = _strict_nonnegative_int(raw.get("member_index"), "snapshot member_index")
        sequence = _strict_nonnegative_int(raw.get("sequence"), "snapshot sequence")
        event_time = _utc(raw.get("event_time"), "snapshot event_time")
        source_event_id = raw.get("source_event_id")
        if not isinstance(source_event_id, str) or not source_event_id.strip():
            raise HistoricalStreamIdentityError("snapshot source_event_id must be non-empty")
        if source_event_id != _source_event_id(locator):
            raise HistoricalStreamIdentityError("snapshot source_event_id is not bound to locator")

        guard._validate_snapshot_position(partition_index, member_index, locator)
        guard._current_partition = partition_index
        guard._current_member = member_index
        guard._cursor = HistoricalStreamCursor(
            partition_index=partition_index,
            member_index=member_index,
            locator=locator,
            sequence=sequence,
            event_time=event_time,
            source_event_id=source_event_id,
        )
        return guard

    def _locate(self, locator: SourceLocator) -> tuple[int, int]:
        """Resolve the current or next source position without a seen-set."""

        current_partition = self._current_partition
        if current_partition is None:
            return self._locate_initial(locator)

        current = self._partitions[current_partition]
        if locator.archive == current.archive:
            return self._locate_current_partition(current_partition, current, locator)
        return self._locate_next_partition(current_partition, locator)

    def _locate_initial(self, locator: SourceLocator) -> tuple[int, int]:
        partition_index = self._find_partition(locator)
        if partition_index != 0:
            raise HistoricalStreamIdentityError("historical stream must start at manifest partition zero")
        member_index = self._find_member(self._partitions[partition_index], locator)
        if member_index != 0:
            raise HistoricalStreamIdentityError("historical stream must start at manifest member zero")
        return partition_index, member_index

    def _locate_current_partition(
        self,
        partition_index: int,
        partition: _PartitionIdentity,
        locator: SourceLocator,
    ) -> tuple[int, int]:
        if locator.sha256 != partition.sha256:
            raise HistoricalStreamIdentityError("quote raw SHA-256 does not match current partition")
        member_index = self._find_member(partition, locator)
        assert self._current_member is not None
        if member_index < self._current_member:
            raise HistoricalStreamIdentityError("historical stream member order regressed")
        if member_index > self._current_member + 1:
            raise HistoricalStreamIdentityError("historical stream skipped a manifest member")
        return partition_index, member_index

    def _locate_next_partition(self, current_partition: int, locator: SourceLocator) -> tuple[int, int]:
        next_partition = current_partition + 1
        if next_partition >= len(self._partitions):
            self._reject_unknown_or_past_partition(locator)
        expected = self._partitions[next_partition]
        if locator.archive != expected.archive:
            self._reject_unknown_or_past_partition(locator)
        if locator.sha256 != expected.sha256:
            raise HistoricalStreamIdentityError("quote raw SHA-256 does not match next partition")
        member_index = self._find_member(expected, locator)
        if member_index != 0:
            raise HistoricalStreamIdentityError("partition transition must start at manifest member zero")
        return next_partition, member_index

    def _find_partition(self, locator: SourceLocator) -> int:
        for index, partition in enumerate(self._partitions):
            if locator.archive == partition.archive:
                if locator.sha256 != partition.sha256:
                    raise HistoricalStreamIdentityError("quote raw SHA-256 does not match manifest partition")
                return index
        raise HistoricalStreamIdentityError("quote raw archive is not listed in manifest")

    @staticmethod
    def _find_member(partition: _PartitionIdentity, locator: SourceLocator) -> int:
        if locator.kind != partition.locator_kind:
            raise HistoricalStreamIdentityError(
                f"quote locator kind {locator.kind!r} does not match partition kind {partition.locator_kind!r}"
            )
        try:
            return partition.members.index(locator.member)
        except ValueError as exc:
            raise HistoricalStreamIdentityError(
                f"quote member {locator.member!r} is not listed in manifest partition {partition.partition_id!r}"
            ) from exc

    def _reject_unknown_or_past_partition(self, locator: SourceLocator) -> None:
        for index, partition in enumerate(self._partitions):
            if locator.archive != partition.archive:
                continue
            if locator.sha256 != partition.sha256:
                raise HistoricalStreamIdentityError("quote raw SHA-256 does not match manifest partition")
            if self._current_partition is not None and index <= self._current_partition:
                raise HistoricalStreamIdentityError("historical stream partition order regressed")
            raise HistoricalStreamIdentityError("historical stream skipped a manifest partition")
        raise HistoricalStreamIdentityError("quote raw archive is not listed in manifest")

    def _validate_global_progress(self, quote: HistoricalQuote) -> None:
        cursor = self._cursor
        if cursor is None:
            return
        if quote.sequence <= cursor.sequence:
            if quote.source_event_id == cursor.source_event_id or quote.locator == cursor.locator:
                raise HistoricalStreamIdentityError("duplicate historical raw locator/source_event_id")
            raise HistoricalStreamIdentityError("historical quote sequence is not strictly increasing")
        if quote.event_time < cursor.event_time:
            raise HistoricalStreamIdentityError("historical quote event_time regressed")
        if quote.source_event_id == cursor.source_event_id:
            raise HistoricalStreamIdentityError("duplicate historical source_event_id")
        if quote.locator == cursor.locator:
            raise HistoricalStreamIdentityError("duplicate historical raw locator")

    def _validate_member_progress(self, locator: SourceLocator) -> None:
        cursor = self._cursor
        assert cursor is not None
        last = cursor.locator
        if locator.offset <= last.offset or locator.row <= last.row:
            raise HistoricalStreamIdentityError("historical raw locator offset/row is not strictly increasing")

    def _validate_snapshot_position(self, partition_index: int, member_index: int, locator: SourceLocator) -> None:
        if partition_index >= len(self._partitions):
            raise HistoricalStreamIdentityError("snapshot partition_index is outside manifest")
        partition = self._partitions[partition_index]
        if locator.archive != partition.archive:
            raise HistoricalStreamIdentityError("snapshot locator archive does not match partition_index")
        if locator.sha256 != partition.sha256:
            raise HistoricalStreamIdentityError("snapshot locator SHA-256 does not match partition")
        if member_index >= len(partition.members):
            raise HistoricalStreamIdentityError("snapshot member_index is outside manifest")
        if locator.member != partition.members[member_index]:
            raise HistoricalStreamIdentityError("snapshot locator member does not match member_index")


# Name used by a few callers that prefer the shorter contract label.
HistoricalStreamGuard = HistoricalStreamIdentityGuard


def _coerce_manifest(value: ManifestInput) -> HistoricalManifest:
    if isinstance(value, (str, Path)):
        value = read_manifest(value)
    if not isinstance(value, (DatasetManifest, DukascopyManifest)):
        raise HistoricalStreamIdentityError("manifest must be a supported HistData or Dukascopy manifest")
    return value


def _manifest_hash(manifest: HistoricalManifest) -> str:
    value = getattr(manifest, "content_hash", None)
    if not isinstance(value, str) or not value.strip():
        raise HistoricalStreamIdentityError("manifest content_hash is required for a stream guard")
    return value.strip()


def _partition_identities(manifest: HistoricalManifest) -> tuple[_PartitionIdentity, ...]:
    identities: list[_PartitionIdentity] = []
    source_keys: set[tuple[str, str]] = set()
    for partition in manifest.partitions:
        if isinstance(partition, DatasetPartition):
            archive = partition.raw_archive
            sha256 = partition.raw_sha256
            members = tuple(partition.members)
        elif isinstance(partition, DukascopyPartition):
            archive = partition.raw_path
            sha256 = partition.raw_sha256
            # Dukascopy's compact endpoint is one logical source member per
            # hourly JSON partition; the decoder uses this exact member name.
            members = ("ticks",)
        else:
            raise HistoricalStreamIdentityError("manifest contains an unsupported partition type")
        if not members or len(set(members)) != len(members):
            raise HistoricalStreamIdentityError("manifest partition members must be unique and non-empty")
        for member in members:
            key = (sha256, member)
            if key in source_keys:
                raise HistoricalStreamIdentityError(
                    "manifest repeats a sha256/member source identity; source_event_id would be ambiguous"
                )
            source_keys.add(key)
        identities.append(
            _PartitionIdentity(
                partition_id=str(partition.partition_id),
                archive=archive,
                sha256=sha256,
                members=members,
                locator_kind=(
                    "archive_member_byte" if isinstance(partition, DatasetPartition) else "compact_json_tick_index"
                ),
            )
        )
    return tuple(identities)


def _snapshot_mapping(value: Mapping[str, Any] | str) -> Mapping[str, Any]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise HistoricalStreamIdentityError("historical stream snapshot is not valid JSON") from exc
    else:
        decoded = value
    if not isinstance(decoded, Mapping):
        raise HistoricalStreamIdentityError("historical stream snapshot must be an object")
    return decoded


def _strict_snapshot_keys(value: Mapping[str, Any]) -> None:
    allowed = {
        "schema_version",
        "manifest_hash",
        "provider",
        "instrument",
        "partition_index",
        "member_index",
        "locator",
        "sequence",
        "event_time",
        "source_event_id",
    }
    unknown = set(value) - allowed
    if unknown:
        raise HistoricalStreamIdentityError(f"historical stream snapshot has unknown keys: {sorted(unknown)!r}")


def _source_event_id(locator: SourceLocator) -> str:
    return f"{locator.sha256}:{locator.member}:{locator.row}:{locator.offset}"


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HistoricalStreamIdentityError(f"{name} must be non-empty text")
    return value.strip()


def _strict_nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise HistoricalStreamIdentityError(f"{name} must be a non-negative integer")
    return value


def _utc(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise HistoricalStreamIdentityError(f"{name} must be an ISO timestamp") from exc
    else:
        raise HistoricalStreamIdentityError(f"{name} must be an ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise HistoricalStreamIdentityError(f"{name} must include timezone")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


__all__ = [
    "HistoricalManifest",
    "HistoricalStreamCursor",
    "HistoricalStreamGuard",
    "HistoricalStreamGuardError",
    "HistoricalStreamIdentityError",
    "HistoricalStreamIdentityGuard",
    "ManifestInput",
    "STREAM_GUARD_SCHEMA_VERSION",
]
