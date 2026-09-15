"""Lossless, quote-only historical data contracts for the local MTF runner.

The first supported source is HistData's Generic ASCII Tick export.  Raw ZIP
bytes stay outside the repository; this module only reads them and exposes a
stream of immutable bid/ask observations.  Historical availability is an
explicit reconstruction policy, never an observation of broker delivery.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import stat
import zipfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast

DATASET_SCHEMA_VERSION = 1
NORMALIZATION_VERSION = "histdata-generic-ascii-tick-v1"
HISTDATA_PROVIDER = "histdata"
HISTDATA_INSTRUMENT = "EUR/USD"
HISTDATA_MONTH = "201603"
# The March-2016 pilot remains the first acquired container.  The manifest
# reader may represent the development span once additional, already-acquired
# monthly archives are available.  Calendar-month bounds prevent a future
# holdout container from being relabelled through coverage metadata alone.
HISTDATA_DEVELOPMENT_START_MONTH = "201601"
HISTDATA_DEVELOPMENT_END_MONTH = "201912"
HISTDATA_HOLDOUT_START_MONTH = "202001"
HISTDATA_SOURCE_TIMEZONE = "EST-05:00-no-dst"
HISTDATA_SOURCE_OFFSET = timezone(timedelta(hours=-5), name="EST")
# Keep the default private data root portable across local users.  Manifests
# still persist the resolved absolute root, so this only changes the default
# selected before a caller supplies an explicit ``data_root``.
DEFAULT_DATA_ROOT = Path.home() / ".local" / "share" / "mtf-lab" / "market-data"
MAX_DATASET_BYTES = 40 * 1024**3
MAX_VALIDATION_ISSUES = 128
_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "dataset_id",
        "provider",
        "instrument",
        "normalization_version",
        "data_root",
        "coverage_start",
        "coverage_end",
        "content_hash",
        "partitions",
    }
)
_LEGACY_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "dataset_id",
        "provider",
        "instrument",
        "month",
        "format",
        "normalization_version",
        "source_uri",
        "terms_uri",
        "source_timezone",
        "availability_policy",
        "data_root",
        "raw_archive",
        "raw_sha256",
        "raw_size",
        "members",
        "acquired_at",
        "coverage_start",
        "coverage_end",
        "quote_count",
        "etag",
        "last_modified",
    }
)
_PARTITION_KEYS = frozenset(
    {
        "partition_id",
        "raw_archive",
        "raw_sha256",
        "raw_size",
        "members",
        "source_uri",
        "terms_uri",
        "acquired_at",
        "coverage_start",
        "coverage_end",
        "quote_count",
        "etag",
        "last_modified",
        "month",
    }
)
_LOCATOR_KEYS = frozenset({"archive", "member", "sha256", "offset", "row", "kind"})
_QUOTE_KEYS = frozenset(
    {
        "record_type",
        "instrument",
        "event_time",
        "event_ts",
        "available_at",
        "availability_basis",
        "source_availability",
        "source_timestamp",
        "provider",
        "source_timezone",
        "precision",
        "bid",
        "ask",
        "sequence",
        "source_event_id",
        "raw_sha256",
        "locator",
        "row",
        "offset",
        "quality",
    }
)


class HistoricalDataError(ValueError):
    """The historical dataset cannot satisfy the lossless quote contract."""


@dataclass(slots=True)
class _IssueAccumulator:
    items: list[str]
    total: int = 0

    def add(self, issue: str) -> None:
        self.total += 1
        if len(self.items) < MAX_VALIDATION_ISSUES:
            self.items.append(issue)

    def extend(self, issues: Sequence[str], total: int | None = None) -> None:
        self.total += len(issues) if total is None else total
        remaining = MAX_VALIDATION_ISSUES - len(self.items)
        if remaining > 0:
            self.items.extend(issues[:remaining])


@dataclass(frozen=True, slots=True)
class _ScanResult:
    count: int
    first: datetime | None
    last: datetime | None
    selected_count: int
    selected_first: datetime | None
    selected_last: datetime | None
    issues: tuple[str, ...]
    issues_total: int


@dataclass(frozen=True, slots=True)
class SourceLocator:
    """Stable source position for one quote in an immutable raw archive."""

    archive: str
    member: str
    sha256: str
    offset: int
    row: int
    kind: str = "archive_member_byte"

    def __post_init__(self) -> None:
        _relative_path(self.archive, "locator archive")
        _relative_path(self.member, "locator member")
        if not _is_sha256(self.sha256):
            raise HistoricalDataError("locator sha256 must be a hexadecimal SHA-256")
        _strict_nonnegative_int(self.offset, "locator offset")
        _strict_positive_int(self.row, "locator row")
        if not isinstance(self.kind, str) or not self.kind.strip():
            raise HistoricalDataError("locator kind must be non-empty")

    @property
    def raw_sha256(self) -> str:
        return self.sha256

    def to_dict(self) -> dict[str, Any]:
        return {
            "archive": self.archive,
            "member": self.member,
            "sha256": self.sha256,
            "offset": self.offset,
            "row": self.row,
            "kind": self.kind,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> SourceLocator:
        _strict_keys(value, _LOCATOR_KEYS, "locator")
        kind = value.get("kind", "archive_member_byte")
        if not isinstance(kind, str) or not kind.strip():
            raise HistoricalDataError("locator kind must be non-empty text")
        return cls(
            archive=_required_text(value, "archive"),
            member=_required_text(value, "member"),
            sha256=_required_text(value, "sha256"),
            offset=_strict_nonnegative_int(value.get("offset"), "offset"),
            row=_strict_positive_int(value.get("row"), "row"),
            kind=kind,
        )


@dataclass(frozen=True, slots=True)
class HistoricalQuote:
    """One immutable historical BBO tick; it is never a trade or a fill."""

    instrument: str
    event_time: datetime
    available_at: datetime
    bid: Decimal
    ask: Decimal
    locator: SourceLocator
    sequence: int
    source_event_id: str
    availability_basis: str = "historical_event_time"
    quality: str = "VALID"
    source_timestamp: str | None = None
    provider: str = HISTDATA_PROVIDER
    source_timezone: str = HISTDATA_SOURCE_TIMEZONE
    precision: str = "source_native"
    source_availability: str | None = None

    def __post_init__(self) -> None:
        _validate_quote_identity(self)
        _normalize_quote_times(self)
        _normalize_quote_source(self)
        _normalize_quote_prices(self)
        _validate_quote_locator(self)

    @property
    def timestamp(self) -> datetime:
        return self.event_time

    @property
    def event_ts(self) -> datetime:
        return self.event_time

    @property
    def raw_sha256(self) -> str:
        return self.locator.sha256

    @property
    def partitionSHA(self) -> str:  # noqa: N802 - external DTO compatibility
        return self.locator.sha256

    @property
    def partition_sha256(self) -> str:
        return self.locator.sha256

    @property
    def row(self) -> int:
        return self.locator.row

    @property
    def offset(self) -> int:
        return self.locator.offset

    @property
    def availability_policy(self) -> str:
        return self.availability_basis

    @property
    def source(self) -> str:
        return self.provider

    @property
    def source_locator(self) -> SourceLocator:
        """Provider-neutral alias for the immutable source position."""

        return self.locator

    @property
    def utc_timestamp(self) -> datetime:
        return self.event_time

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_type": "historical_quote",
            "instrument": self.instrument,
            "event_time": _iso(self.event_time),
            "event_ts": _iso(self.event_time),
            "available_at": _iso(self.available_at),
            "availability_basis": self.availability_basis,
            "source_availability": self.source_availability,
            "source_timestamp": self.source_timestamp,
            "provider": self.provider,
            "source_timezone": self.source_timezone,
            "precision": self.precision,
            "bid": str(self.bid),
            "ask": str(self.ask),
            "sequence": self.sequence,
            "source_event_id": self.source_event_id,
            "raw_sha256": self.raw_sha256,
            "locator": self.locator.to_dict(),
            "row": self.locator.row,
            "offset": self.locator.offset,
            "quality": self.quality,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> HistoricalQuote:
        _strict_keys(value, _QUOTE_KEYS, "quote")
        if value.get("record_type") is not None and value.get("record_type") != "historical_quote":
            raise HistoricalDataError("quote record_type is invalid")
        locator_raw = value.get("locator")
        if not isinstance(locator_raw, Mapping):
            raise HistoricalDataError("quote locator must be an object")
        locator = SourceLocator.from_mapping(locator_raw)
        if value.get("raw_sha256") is not None and value.get("raw_sha256") != locator.sha256:
            raise HistoricalDataError("quote raw_sha256 does not match locator")
        if value.get("row") is not None and value.get("row") != locator.row:
            raise HistoricalDataError("quote row does not match locator")
        if value.get("offset") is not None and value.get("offset") != locator.offset:
            raise HistoricalDataError("quote offset does not match locator")
        event_raw = value.get("event_time", value.get("event_ts"))
        event_time = _parse_iso(event_raw, "event_time")
        if (
            value.get("event_time") is not None
            and value.get("event_ts") is not None
            and _parse_iso(value["event_ts"], "event_ts") != event_time
        ):
            raise HistoricalDataError("quote event_time/event_ts mismatch")
        return cls(
            instrument=_required_text(value, "instrument"),
            event_time=event_time,
            available_at=_parse_iso(value.get("available_at"), "available_at"),
            bid=_positive_decimal(value.get("bid"), "bid"),
            ask=_positive_decimal(value.get("ask"), "ask"),
            locator=locator,
            sequence=_strict_nonnegative_int(value.get("sequence"), "sequence"),
            source_event_id=_required_text(value, "source_event_id"),
            availability_basis=str(value.get("availability_basis", "historical_event_time")),
            quality=str(value.get("quality", "VALID")),
            source_timestamp=(str(value["source_timestamp"]) if value.get("source_timestamp") is not None else None),
            provider=_optional_text(value, "provider", HISTDATA_PROVIDER),
            source_timezone=_optional_text(value, "source_timezone", HISTDATA_SOURCE_TIMEZONE),
            precision=_optional_text(value, "precision", "source_native"),
            source_availability=(
                _optional_text(value, "source_availability", "")
                if value.get("source_availability") is not None
                else None
            ),
        )


def _validate_quote_identity(quote: HistoricalQuote) -> None:
    if not isinstance(quote.instrument, str) or quote.instrument != HISTDATA_INSTRUMENT:
        raise HistoricalDataError(f"unsupported historical instrument: {quote.instrument!r}")
    _strict_nonnegative_int(quote.sequence, "sequence")
    if not isinstance(quote.source_event_id, str) or not quote.source_event_id.strip():
        raise HistoricalDataError("quote source_event_id must be non-empty")
    if quote.availability_basis != "historical_event_time":
        raise HistoricalDataError("historical quote availability_basis is fixed")
    if quote.quality != "VALID":
        raise HistoricalDataError("historical quote quality must be VALID")


def _normalize_quote_times(quote: HistoricalQuote) -> None:
    event_time = _utc(quote.event_time, "event_time")
    available_at = _utc(quote.available_at, "available_at")
    if available_at != event_time:
        raise HistoricalDataError("historical availability must equal event_time")
    object.__setattr__(quote, "event_time", event_time)
    object.__setattr__(quote, "available_at", available_at)


def _normalize_quote_source(quote: HistoricalQuote) -> None:
    if quote.source_timestamp is None:
        object.__setattr__(quote, "source_timestamp", _iso(quote.event_time))
    elif not isinstance(quote.source_timestamp, str) or not quote.source_timestamp.strip():
        raise HistoricalDataError("source_timestamp must be non-empty text")
    for value, name in (
        (quote.provider, "quote provider"),
        (quote.source_timezone, "quote source_timezone"),
        (quote.precision, "quote precision"),
    ):
        if not isinstance(value, str) or not value.strip():
            raise HistoricalDataError(f"{name} must be non-empty")
    source_availability = (
        quote.source_availability if quote.source_availability is not None else quote.availability_basis
    )
    if not isinstance(source_availability, str) or not source_availability.strip():
        raise HistoricalDataError("quote source_availability must be non-empty")
    if source_availability != quote.availability_basis:
        raise HistoricalDataError("quote source_availability/availability_basis mismatch")
    object.__setattr__(quote, "source_availability", source_availability)


def _normalize_quote_prices(quote: HistoricalQuote) -> None:
    bid = _positive_decimal(quote.bid, "bid")
    ask = _positive_decimal(quote.ask, "ask")
    if ask < bid:
        raise HistoricalDataError("ask must not be below bid")
    object.__setattr__(quote, "bid", bid)
    object.__setattr__(quote, "ask", ask)


def _validate_quote_locator(quote: HistoricalQuote) -> None:
    if not isinstance(quote.locator, SourceLocator):
        raise HistoricalDataError("quote locator must be SourceLocator")
    expected_id = f"{quote.locator.sha256}:{quote.locator.member}:{quote.locator.row}:{quote.locator.offset}"
    if quote.source_event_id != expected_id:
        raise HistoricalDataError("quote source_event_id is not bound to its locator")


@dataclass(frozen=True, slots=True)
class DatasetPartition:
    """One immutable raw archive and its source-local coverage."""

    partition_id: str
    raw_archive: str
    raw_sha256: str
    raw_size: int
    members: tuple[str, ...]
    source_uri: str
    terms_uri: str
    acquired_at: datetime
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    quote_count: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    month: str = HISTDATA_MONTH

    def __post_init__(self) -> None:
        if not isinstance(self.partition_id, str) or not self.partition_id.strip():
            raise HistoricalDataError("partition_id must be non-empty")
        month = _normalize_histdata_month(self.month)
        if self.partition_id != month:
            raise HistoricalDataError("partition_id must equal the canonical YYYYMM month")
        _relative_path(self.raw_archive, "raw archive")
        if not _is_sha256(self.raw_sha256):
            raise HistoricalDataError("partition raw_sha256 must be a hexadecimal SHA-256")
        _strict_positive_int(self.raw_size, "raw_size")
        if self.raw_size > MAX_DATASET_BYTES:
            raise HistoricalDataError("partition raw_size exceeds the dataset limit")
        if not self.members or len(set(self.members)) != len(self.members):
            raise HistoricalDataError("partition must list unique CSV members")
        for member in self.members:
            _relative_path(member, "archive member")
        if not HISTDATA_DEVELOPMENT_START_MONTH <= month <= HISTDATA_DEVELOPMENT_END_MONTH:
            if month >= HISTDATA_HOLDOUT_START_MONTH:
                raise HistoricalDataError("HistData holdout months are not allowed in DatasetManifest")
            raise HistoricalDataError("HistData month is outside the approved development window")
        object.__setattr__(self, "month", month)
        object.__setattr__(self, "acquired_at", _utc(self.acquired_at, "acquired_at"))
        _normalize_coverage(self)
        if self.quote_count is not None:
            _strict_nonnegative_int(self.quote_count, "quote_count")

    @property
    def sha256(self) -> str:
        return self.raw_sha256

    @property
    def size(self) -> int:
        return self.raw_size

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "raw_archive": self.raw_archive,
            "raw_sha256": self.raw_sha256,
            "raw_size": self.raw_size,
            "members": list(self.members),
            "source_uri": self.source_uri,
            "terms_uri": self.terms_uri,
            "acquired_at": _iso(self.acquired_at),
            "coverage_start": _iso(self.coverage_start),
            "coverage_end": _iso(self.coverage_end),
            "quote_count": self.quote_count,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "month": self.month,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DatasetPartition:
        _strict_keys(value, _PARTITION_KEYS, "partition")
        raw_members = value.get("members")
        if not isinstance(raw_members, list) or not all(isinstance(item, str) for item in raw_members):
            raise HistoricalDataError("partition members must be a list of strings")
        return cls(
            partition_id=_required_text(value, "partition_id"),
            raw_archive=_required_text(value, "raw_archive"),
            raw_sha256=_required_text(value, "raw_sha256"),
            raw_size=_strict_positive_int(value.get("raw_size"), "raw_size"),
            members=tuple(raw_members),
            source_uri=_required_text(value, "source_uri"),
            terms_uri=_required_text(value, "terms_uri"),
            acquired_at=_parse_iso(value.get("acquired_at"), "acquired_at"),
            coverage_start=_parse_optional_iso(value.get("coverage_start"), "coverage_start"),
            coverage_end=_parse_optional_iso(value.get("coverage_end"), "coverage_end"),
            quote_count=(
                _strict_nonnegative_int(value.get("quote_count"), "quote_count")
                if value.get("quote_count") is not None
                else None
            ),
            etag=str(value["etag"]) if value.get("etag") is not None else None,
            last_modified=str(value["last_modified"]) if value.get("last_modified") is not None else None,
            month=_required_text(value, "month"),
        )


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    """Versioned dataset identity; research splits live in another contract."""

    dataset_id: str
    provider: str
    instrument: str
    partitions: tuple[DatasetPartition, ...]
    coverage_start: datetime | None
    coverage_end: datetime | None
    content_hash: str
    data_root: str = str(DEFAULT_DATA_ROOT)
    normalization_version: str = NORMALIZATION_VERSION
    schema_version: int = DATASET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_manifest_core(self)
        _validate_manifest_partitions(self)
        _validate_manifest_root(self)
        _normalize_coverage(self)

    @property
    def version(self) -> int:
        return self.schema_version

    @property
    def manifest_version(self) -> int:
        return self.schema_version

    @property
    def partition_paths(self) -> tuple[Path, ...]:
        root = Path(self.data_root).expanduser()
        return tuple(root / partition.raw_archive for partition in self.partitions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "provider": self.provider,
            "instrument": self.instrument,
            "normalization_version": self.normalization_version,
            "data_root": self.data_root,
            "coverage_start": _iso(self.coverage_start),
            "coverage_end": _iso(self.coverage_end),
            "content_hash": self.content_hash,
            "partitions": [partition.to_dict() for partition in self.partitions],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DatasetManifest:
        raw_partitions = value.get("partitions")
        if raw_partitions is None and "raw_archive" in value:
            _strict_keys(value, _LEGACY_MANIFEST_KEYS, "legacy manifest")
            return cls._from_legacy_mapping(value)
        _strict_keys(value, _MANIFEST_KEYS, "manifest")
        if not isinstance(raw_partitions, list):
            raise HistoricalDataError("manifest partitions must be a list")
        if any(not isinstance(item, Mapping) for item in raw_partitions):
            raise HistoricalDataError("manifest partitions must contain objects")
        return cls(
            schema_version=_strict_positive_int(value.get("schema_version"), "schema_version"),
            dataset_id=_required_text(value, "dataset_id"),
            provider=_required_text(value, "provider"),
            instrument=_required_text(value, "instrument"),
            partitions=tuple(DatasetPartition.from_mapping(item) for item in raw_partitions),
            coverage_start=_parse_optional_iso(value.get("coverage_start"), "coverage_start"),
            coverage_end=_parse_optional_iso(value.get("coverage_end"), "coverage_end"),
            content_hash=_required_text(value, "content_hash"),
            data_root=str(value.get("data_root", DEFAULT_DATA_ROOT)),
            normalization_version=str(value.get("normalization_version", NORMALIZATION_VERSION)),
        )

    @classmethod
    def _from_legacy_mapping(cls, value: Mapping[str, Any]) -> DatasetManifest:
        """Read the pre-partition March manifest without rewriting the artifact."""

        members = value.get("members")
        if not isinstance(members, list) or not all(isinstance(item, str) for item in members):
            raise HistoricalDataError("legacy manifest members must be a list of strings")
        raw_archive = _required_text(value, "raw_archive")
        raw_sha256 = _required_text(value, "raw_sha256")
        raw_size = _strict_positive_int(value.get("raw_size"), "raw_size")
        partition = DatasetPartition(
            partition_id=_required_text(value, "month"),
            raw_archive=raw_archive,
            raw_sha256=raw_sha256,
            raw_size=raw_size,
            members=tuple(members),
            source_uri=_required_text(value, "source_uri"),
            terms_uri=_required_text(value, "terms_uri"),
            acquired_at=_parse_iso(value.get("acquired_at"), "acquired_at"),
            coverage_start=_parse_optional_iso(value.get("coverage_start"), "coverage_start"),
            coverage_end=_parse_optional_iso(value.get("coverage_end"), "coverage_end"),
            quote_count=(
                _strict_nonnegative_int(value.get("quote_count"), "quote_count")
                if value.get("quote_count") is not None
                else None
            ),
            etag=str(value["etag"]) if value.get("etag") is not None else None,
            last_modified=str(value["last_modified"]) if value.get("last_modified") is not None else None,
            month=_required_text(value, "month"),
        )
        return cls(
            schema_version=_strict_positive_int(value.get("schema_version"), "schema_version"),
            dataset_id=_required_text(value, "dataset_id"),
            provider=_required_text(value, "provider"),
            instrument=_required_text(value, "instrument"),
            partitions=(partition,),
            coverage_start=_parse_optional_iso(value.get("coverage_start"), "coverage_start"),
            coverage_end=_parse_optional_iso(value.get("coverage_end"), "coverage_end"),
            content_hash=_content_hash((partition,)),
            data_root=str(value.get("data_root", DEFAULT_DATA_ROOT)),
            normalization_version=str(value.get("normalization_version", NORMALIZATION_VERSION)),
        )

    @classmethod
    def from_path(cls, path: str | Path) -> DatasetManifest:
        target = Path(path).expanduser()
        _reject_symlink_ancestors(target)
        if target.is_symlink() or not target.is_file():
            raise HistoricalDataError("dataset manifest is not a regular file")
        try:
            value = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HistoricalDataError(f"cannot read dataset manifest: {target}") from exc
        if not isinstance(value, Mapping):
            raise HistoricalDataError("dataset manifest must be a JSON object")
        return cls.from_mapping(value)


@dataclass(frozen=True, slots=True)
class DatasetValidation:
    """Bounded validation result; truthiness is the ``ok`` field."""

    ok: bool
    quote_count: int
    coverage_start: datetime | None
    coverage_end: datetime | None
    content_hash: str | None
    issues: tuple[str, ...] = ()
    issues_total: int = 0
    coverage_limited: bool = False
    requested_start: datetime | None = None
    requested_end: datetime | None = None

    def __bool__(self) -> bool:
        return self.ok

    @property
    def raw_sha256(self) -> str | None:
        return self.content_hash

    @property
    def issues_truncated(self) -> bool:
        return self.issues_total > len(self.issues)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "quote_count": self.quote_count,
            "coverage_start": _iso(self.coverage_start),
            "coverage_end": _iso(self.coverage_end),
            "content_hash": self.content_hash,
            "issues": list(self.issues),
            "issues_total": self.issues_total,
            "issues_truncated": self.issues_truncated,
            "coverage_limited": self.coverage_limited,
            "requested_start": _iso(self.requested_start),
            "requested_end": _iso(self.requested_end),
        }


if TYPE_CHECKING:
    from .dukascopy import DukascopyManifest, DukascopyValidation

    DukascopyManifestType: TypeAlias = DukascopyManifest
    DukascopyValidationType: TypeAlias = DukascopyValidation
    HistoricalManifest: TypeAlias = DatasetManifest | DukascopyManifest
    HistoricalValidation: TypeAlias = DatasetValidation | DukascopyValidation
else:
    HistoricalManifest: TypeAlias = DatasetManifest
    HistoricalValidation: TypeAlias = DatasetValidation


class _DukascopyModule(Protocol):
    """Typed boundary for the dynamically loaded provider adapter.

    The provider-neutral module must not statically import a provider module:
    the provider imports this module for the shared quote DTOs.  The protocol
    keeps the dispatch surface typed while the runtime import remains one-way
    from the completed historical module into the selected provider.
    """

    DukascopyManifest: type[DukascopyManifestType]
    read_manifest: Callable[[str | Path], DukascopyManifestType]
    iter_quotes: Callable[
        [DukascopyManifestType | str | Path, datetime | None, datetime | None],
        Iterator[HistoricalQuote],
    ]
    validate_manifest: Callable[[DukascopyManifestType | str | Path], DukascopyValidationType]


def _load_dukascopy_adapter() -> _DukascopyModule:
    """Load the optional provider only after this module has initialized."""

    return cast(_DukascopyModule, importlib.import_module("mtf_lab.data.dukascopy"))


@dataclass(frozen=True, slots=True)
class HistoricalQuoteProvider:
    """Provider facade that streams an already acquired HistData manifest."""

    manifest: DatasetManifest

    @property
    def name(self) -> str:
        return f"{HISTDATA_PROVIDER}_historical"

    @property
    def instrument(self) -> str:
        return self.manifest.instrument

    def stream(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[HistoricalQuote]:
        return iter_quotes(self.manifest, start, end)

    def fetch(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[HistoricalQuote]:
        return self.stream(start=start, end=end)

    def validate(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> DatasetValidation:
        return cast(DatasetValidation, validate_dataset(self.manifest, start, end))


def iter_quotes(
    manifest: HistoricalManifest | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[HistoricalQuote]:
    """Stream quotes in source order over the half-open UTC interval ``[start,end)``."""

    resolved = _resolve_provider_manifest(manifest)
    if not isinstance(resolved, DatasetManifest):
        adapter = _load_dukascopy_adapter()
        yield from adapter.iter_quotes(resolved, start, end)
        return
    start_utc = _bound(start, "start")
    end_utc = _bound(end, "end")
    if start_utc and end_utc and end_utc <= start_utc:
        raise HistoricalDataError("end must be after start")
    _verify_manifest_partitions(resolved)
    yield from _iter_manifest_quotes(resolved, start=start_utc, end=end_utc)


def stream(
    manifest: HistoricalManifest | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[HistoricalQuote]:
    """Public provider-neutral alias for :func:`iter_quotes`."""

    return iter_quotes(manifest, start, end)


def read_manifest(path: str | Path) -> HistoricalManifest:
    """Read a known provider manifest without rewriting its source artifacts."""

    target = Path(path).expanduser()
    _reject_symlink_ancestors(target)
    if target.is_symlink() or not target.is_file():
        raise HistoricalDataError("dataset manifest is not a regular file")
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HistoricalDataError(f"cannot read dataset manifest: {target}") from exc
    if not isinstance(raw, Mapping):
        raise HistoricalDataError("dataset manifest must be a JSON object")
    provider = raw.get("provider")
    if provider == "dukascopy":
        return _load_dukascopy_adapter().read_manifest(target)
    if provider not in (None, HISTDATA_PROVIDER):
        raise HistoricalDataError(f"unsupported historical provider: {provider!r}")
    return DatasetManifest.from_mapping(raw)


def validate(
    manifest: HistoricalManifest | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> HistoricalValidation:
    """Public short alias for :func:`validate_dataset`."""

    return validate_dataset(manifest, start, end)


def validate_dataset(
    manifest: HistoricalManifest | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> HistoricalValidation:
    """Verify raw bytes, ZIP members, row order, identities, and coverage."""

    try:
        resolved = _resolve_provider_manifest(manifest)
        if not isinstance(resolved, DatasetManifest):
            return _load_dukascopy_adapter().validate_manifest(resolved)
        start_utc = _bound(start, "start")
        end_utc = _bound(end, "end")
        if start_utc and end_utc and end_utc <= start_utc:
            raise HistoricalDataError("end must be after start")
        _verify_manifest_partitions(resolved)
        summary = _scan_manifest(resolved, start=start_utc, end=end_utc)
        issues = _IssueAccumulator(list(summary.issues), summary.issues_total)
        if _content_hash(resolved.partitions) != resolved.content_hash:
            issues.add("manifest content_hash mismatch")
        return replace(summary, ok=issues.total == 0, issues=tuple(issues.items), issues_total=issues.total)
    except (HistoricalDataError, OSError, TypeError, ValueError, zipfile.BadZipFile) as exc:
        return DatasetValidation(False, 0, None, None, None, (str(exc),), issues_total=1)


def manifest_from_histdata_archive(
    archive: str | Path,
    *,
    data_root: str | Path,
    acquired_at: datetime,
    source_uri: str,
    terms_uri: str,
    month: str = HISTDATA_MONTH,
    etag: str | None = None,
    last_modified: str | None = None,
) -> DatasetManifest:
    """Create a complete monthly manifest from one completed raw ZIP.

    The default remains the exact March-2016 pilot contract.  The month is an
    explicit identity input for later development partitions; it is checked
    against the CSV member name and every decoded row rather than inferred
    from a caller-controlled coverage timestamp.
    """

    archive_path = Path(archive).expanduser()
    root = Path(data_root).expanduser()
    month = _normalize_histdata_month(month)
    _reject_symlink_ancestors(root)
    _reject_symlink_ancestors(archive_path)
    try:
        raw_archive = archive_path.relative_to(root).as_posix()
    except ValueError as exc:
        raise HistoricalDataError("raw archive must be under data_root") from exc
    if archive_path.is_symlink() or not archive_path.is_file():
        raise HistoricalDataError("raw archive is not a regular file")
    raw_size = archive_path.stat().st_size
    if raw_size < 1 or raw_size > MAX_DATASET_BYTES:
        raise HistoricalDataError("raw archive exceeds the dataset limit")
    raw_sha256 = _hash_file(archive_path)
    members = _histdata_members(archive_path, month=month)
    partition = DatasetPartition(
        partition_id=month,
        raw_archive=raw_archive,
        raw_sha256=raw_sha256,
        raw_size=raw_size,
        members=tuple(members),
        source_uri=source_uri,
        terms_uri=terms_uri,
        acquired_at=acquired_at,
        etag=etag,
        last_modified=last_modified,
        month=month,
    )
    content_hash = _content_hash((partition,))
    manifest = DatasetManifest(
        dataset_id=_dataset_id_for_partitions((partition,), content_hash),
        provider=HISTDATA_PROVIDER,
        instrument=HISTDATA_INSTRUMENT,
        partitions=(partition,),
        coverage_start=None,
        coverage_end=None,
        content_hash=content_hash,
        data_root=str(root),
    )
    summary = _scan_manifest(manifest)
    if not summary.ok:
        raise HistoricalDataError("raw archive failed quote validation: " + "; ".join(summary.issues))
    partition = replace(
        partition,
        coverage_start=summary.coverage_start,
        coverage_end=summary.coverage_end,
        quote_count=summary.quote_count,
    )
    return replace(
        manifest,
        partitions=(partition,),
        coverage_start=summary.coverage_start,
        coverage_end=summary.coverage_end,
    )


def manifest_from_histdata_archives(
    archives: Sequence[str | Path],
    *,
    data_root: str | Path,
    acquired_at: datetime | Sequence[datetime],
    source_uri: str | Sequence[str],
    terms_uri: str | Sequence[str],
    months: Sequence[str] | None = None,
    etags: str | Sequence[str | None] | None = None,
    last_modified: str | Sequence[str | None] | None = None,
) -> DatasetManifest:
    """Build a read-only, contiguous development manifest from ZIP archives.

    The function never downloads, rewrites, or combines raw bytes.  It only
    hashes and validates already-acquired archives under data_root and records
    one immutable partition per calendar month.  Archives must be supplied in
    chronological order; repeated identity, a missing month, overlapping
    coverage, or any holdout month is rejected.

    When months is omitted, every archive must contain exactly one canonical
    HistData member whose name identifies its YYYYMM month.  Explicit months
    are checked against those member names as an additional provenance guard.
    """

    if isinstance(archives, (str, Path)):
        raise HistoricalDataError("archives must be a non-empty sequence of monthly paths")
    archive_list = tuple(Path(item).expanduser() for item in archives)
    if not archive_list:
        raise HistoricalDataError("archives must contain at least one monthly archive")
    root = Path(data_root).expanduser()
    if not root.is_absolute():
        raise HistoricalDataError("data_root must be an absolute path")
    _reject_symlink_ancestors(root)

    selected_months = _select_histdata_months(archive_list, months)

    acquired_values = _expand_partition_metadata(acquired_at, len(archive_list), "acquired_at")
    source_values = _expand_partition_metadata(source_uri, len(archive_list), "source_uri")
    terms_values = _expand_partition_metadata(terms_uri, len(archive_list), "terms_uri")
    etag_values = _expand_partition_metadata(etags, len(archive_list), "etag", allow_none=True)
    modified_values = _expand_partition_metadata(last_modified, len(archive_list), "last_modified", allow_none=True)
    partitions = tuple(
        _partition_from_archive(
            archive_path,
            month=month,
            data_root=root,
            acquired_at=acquired_values[index],
            source_uri=source_values[index],
            terms_uri=terms_values[index],
            etag=etag_values[index],
            last_modified=modified_values[index],
        )
        for index, (archive_path, month) in enumerate(zip(archive_list, selected_months, strict=True))
    )

    content_hash = _content_hash(tuple(partitions))
    manifest = DatasetManifest(
        dataset_id=_dataset_id_for_partitions(tuple(partitions), content_hash),
        provider=HISTDATA_PROVIDER,
        instrument=HISTDATA_INSTRUMENT,
        partitions=tuple(partitions),
        coverage_start=None,
        coverage_end=None,
        content_hash=content_hash,
        data_root=str(root),
    )
    # Verify bytes before deriving coverage/count metadata.  Nothing in this
    # path writes the source archive or an external manifest file.
    _verify_manifest_partitions(manifest)
    summary = _scan_manifest(manifest)
    if not summary.ok:
        raise HistoricalDataError("raw archives failed quote validation: " + "; ".join(summary.issues))
    covered_partitions = tuple(_partition_with_coverage(manifest, partition) for partition in manifest.partitions)
    return replace(
        manifest,
        partitions=covered_partitions,
        coverage_start=summary.coverage_start,
        coverage_end=summary.coverage_end,
    )


def _coerce_manifest(value: DatasetManifest | str | Path) -> DatasetManifest:
    if isinstance(value, DatasetManifest):
        return value
    return DatasetManifest.from_path(value)


def _resolve_provider_manifest(value: HistoricalManifest | str | Path) -> HistoricalManifest:
    if isinstance(value, (str, Path)):
        return read_manifest(value)
    if isinstance(value, DatasetManifest):
        return value
    adapter = _load_dukascopy_adapter()
    if isinstance(value, adapter.DukascopyManifest):
        return value
    raise HistoricalDataError("unsupported historical manifest object")


def _iter_manifest_quotes(
    manifest: DatasetManifest,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[HistoricalQuote]:
    previous_time: datetime | None = None
    sequence = 0
    for partition in manifest.partitions:
        for quote in _iter_partition_quotes(manifest, partition, sequence_start=sequence):
            if previous_time is not None and quote.event_time < previous_time:
                raise HistoricalDataError(f"source order violation at sequence {quote.sequence}")
            previous_time = quote.event_time
            sequence = quote.sequence + 1
            if (start is None or quote.event_time >= start) and (end is None or quote.event_time < end):
                yield quote
    if sequence == 0:
        raise HistoricalDataError("dataset contains no historical quotes")


def _iter_partition_quotes(
    manifest: DatasetManifest,
    partition: DatasetPartition,
    *,
    sequence_start: int,
) -> Iterator[HistoricalQuote]:
    archive_path = _regular_archive_path(manifest.data_root, partition.raw_archive)
    with zipfile.ZipFile(archive_path, "r") as archive:
        for member in partition.members:
            info = archive.getinfo(member)
            if info.is_dir() or _zipinfo_is_symlink(info):
                raise HistoricalDataError(f"archive member is not a regular file: {member}")
            with archive.open(info, "r") as stream:
                row = 0
                while True:
                    offset = int(stream.tell())
                    line = stream.readline()
                    if not line:
                        break
                    row += 1
                    quote = _parse_histdata_line(
                        line,
                        archive=partition.raw_archive,
                        member=member,
                        sha256=partition.raw_sha256,
                        row=row,
                        offset=offset,
                        sequence=sequence_start,
                    )
                    sequence_start += 1
                    local_month = quote.event_time.astimezone(HISTDATA_SOURCE_OFFSET).strftime("%Y%m")
                    if local_month != partition.month:
                        raise HistoricalDataError(f"row outside partition month at {member}:{row}")
                    yield quote


def _parse_histdata_line(
    line: bytes,
    *,
    archive: str,
    member: str,
    sha256: str,
    row: int,
    offset: int,
    sequence: int,
) -> HistoricalQuote:
    try:
        text = line.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise HistoricalDataError(f"non-ASCII HistData row at {member}:{row}") from exc
    fields = text.split(",")
    if len(fields) != 4 or any(not field.strip() for field in fields):
        raise HistoricalDataError(f"expected timestamp,bid,ask,volume at {member}:{row}")
    timestamp = _parse_histdata_timestamp(fields[0].strip(), member, row)
    _nonnegative_decimal(fields[3].strip(), "volume")
    locator = SourceLocator(archive, member, sha256, offset, row)
    return HistoricalQuote(
        instrument=HISTDATA_INSTRUMENT,
        event_time=timestamp,
        available_at=timestamp,
        bid=_positive_decimal(fields[1].strip(), "bid"),
        ask=_positive_decimal(fields[2].strip(), "ask"),
        locator=locator,
        sequence=sequence,
        source_event_id=f"{sha256}:{member}:{row}:{offset}",
        source_timestamp=fields[0].strip(),
        provider=HISTDATA_PROVIDER,
        source_timezone=HISTDATA_SOURCE_TIMEZONE,
        precision="decimal_ascii_millisecond",
        source_availability="historical_event_time",
    )


def _parse_histdata_timestamp(value: str, member: str, row: int) -> datetime:
    if len(value) != 18 or value[8] != " " or not value[:8].isdigit() or not value[9:].isdigit():
        raise HistoricalDataError(f"invalid HistData timestamp at {member}:{row}")
    try:
        local = datetime.strptime(value[:8] + value[9:15], "%Y%m%d%H%M%S").replace(
            tzinfo=HISTDATA_SOURCE_OFFSET,
            microsecond=int(value[15:]) * 1000,
        )
    except (TypeError, ValueError) as exc:
        raise HistoricalDataError(f"invalid HistData timestamp at {member}:{row}") from exc
    return local.astimezone(UTC)


def _scan_manifest(
    manifest: DatasetManifest,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
) -> DatasetValidation:
    try:
        scan = _scan_partitions(manifest, start=start, end=end)
    except (HistoricalDataError, OSError, TypeError, ValueError, zipfile.BadZipFile) as exc:
        return DatasetValidation(False, 0, None, None, manifest.content_hash, (str(exc),), issues_total=1)
    issues = _IssueAccumulator(list(scan.issues), scan.issues_total)
    if scan.count == 0:
        issues.add("dataset contains no historical quotes")
    if manifest.coverage_start is not None and manifest.coverage_start != scan.first:
        issues.add("manifest coverage_start mismatch")
    if manifest.coverage_end is not None and manifest.coverage_end != scan.last:
        issues.add("manifest coverage_end mismatch")
    if start is not None or end is not None:
        _add_range_issues(issues, manifest, scan, start=start, end=end)
        return DatasetValidation(
            issues.total == 0,
            scan.selected_count,
            scan.selected_first,
            scan.selected_last,
            manifest.content_hash,
            tuple(issues.items),
            issues.total,
            coverage_limited=True,
            requested_start=start,
            requested_end=end,
        )
    return DatasetValidation(
        issues.total == 0, scan.count, scan.first, scan.last, manifest.content_hash, tuple(issues.items), issues.total
    )


def _scan_partitions(
    manifest: DatasetManifest,
    *,
    start: datetime | None,
    end: datetime | None,
) -> _ScanResult:
    count = 0
    first: datetime | None = None
    last: datetime | None = None
    selected_count = 0
    selected_first: datetime | None = None
    selected_last: datetime | None = None
    issues = _IssueAccumulator([])
    sequence = 0
    for partition in manifest.partitions:
        (
            part_count,
            part_first,
            part_last,
            part_selected,
            part_selected_first,
            part_selected_last,
            sequence,
            part_issues,
            part_issue_total,
        ) = _scan_partition(
            manifest,
            partition,
            sequence_start=sequence,
            previous_time=last,
            start=start,
            end=end,
        )
        count += part_count
        first = first or part_first
        last = part_last or last
        selected_count += part_selected
        selected_first = selected_first or part_selected_first
        selected_last = part_selected_last or selected_last
        issues.extend(part_issues, part_issue_total)
    return _ScanResult(
        count, first, last, selected_count, selected_first, selected_last, tuple(issues.items), issues.total
    )


def _add_range_issues(
    issues: _IssueAccumulator,
    manifest: DatasetManifest,
    scan: _ScanResult,
    *,
    start: datetime | None,
    end: datetime | None,
) -> None:
    if scan.selected_count == 0:
        issues.add("requested coverage is empty")
    if start is not None and manifest.coverage_end is not None and start >= manifest.coverage_end:
        issues.add("requested range is outside dataset coverage")
    if end is not None and manifest.coverage_start is not None and end <= manifest.coverage_start:
        issues.add("requested range is outside dataset coverage")
    if start is not None and manifest.coverage_start is not None and start < manifest.coverage_start:
        issues.add("requested range starts before dataset coverage")
    if end is not None and manifest.coverage_end is not None and end > manifest.coverage_end:
        issues.add("requested range ends after dataset coverage")


def _scan_partition(
    manifest: DatasetManifest,
    partition: DatasetPartition,
    *,
    sequence_start: int,
    previous_time: datetime | None,
    start: datetime | None,
    end: datetime | None,
) -> tuple[int, datetime | None, datetime | None, int, datetime | None, datetime | None, int, tuple[str, ...], int]:
    count = 0
    first: datetime | None = None
    last = previous_time
    selected_count = 0
    selected_first: datetime | None = None
    selected_last: datetime | None = None
    issues = _IssueAccumulator([])
    try:
        for quote in _iter_partition_quotes(manifest, partition, sequence_start=sequence_start):
            if previous_time is not None and quote.event_time < previous_time:
                issues.add(f"source order violation at sequence {quote.sequence}")
            previous_time = quote.event_time
            first = first or quote.event_time
            last = quote.event_time
            sequence_start = quote.sequence + 1
            count += 1
            if (start is None or quote.event_time >= start) and (end is None or quote.event_time < end):
                selected_count += 1
                selected_first = selected_first or quote.event_time
                selected_last = quote.event_time
    except (HistoricalDataError, OSError, TypeError, ValueError, zipfile.BadZipFile) as exc:
        issues.add(str(exc))
    if partition.quote_count is not None and partition.quote_count != count:
        issues.add(f"partition quote_count mismatch: {partition.partition_id}")
    if first is not None and partition.coverage_start is not None and partition.coverage_start != first:
        issues.add(f"partition coverage_start mismatch: {partition.partition_id}")
    if last is not None and partition.coverage_end is not None and partition.coverage_end != last:
        issues.add(f"partition coverage_end mismatch: {partition.partition_id}")
    return (
        count,
        first,
        last,
        selected_count,
        selected_first,
        selected_last,
        sequence_start,
        tuple(issues.items),
        issues.total,
    )


def _verify_manifest_partitions(manifest: DatasetManifest) -> None:
    for partition in manifest.partitions:
        _verify_partition_archive(manifest.data_root, partition)


def _verify_partition_archive(data_root: str, partition: DatasetPartition) -> None:
    archive_path = _regular_archive_path(data_root, partition.raw_archive)
    if archive_path.stat().st_size != partition.raw_size:
        raise HistoricalDataError(f"raw archive size mismatch: {partition.partition_id}")
    if _hash_file(archive_path) != partition.raw_sha256:
        raise HistoricalDataError(f"raw archive SHA-256 mismatch: {partition.partition_id}")
    with zipfile.ZipFile(archive_path, "r") as archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        if len(names) != len(set(names)):
            raise HistoricalDataError("ZIP contains duplicate member names")
        for info in infos:
            _validate_zip_name(info)
        actual = set(_histdata_members_from_infos(infos, month=partition.month))
        expected = set(partition.members)
        if actual != expected:
            raise HistoricalDataError(f"partition CSV members mismatch: {partition.partition_id}")


def _regular_archive_path(data_root: str, raw_archive: str) -> Path:
    root = Path(data_root).expanduser()
    _reject_symlink_ancestors(root)
    if root.is_symlink() or not root.is_dir():
        raise HistoricalDataError("manifest data_root is not a regular directory")
    target = root / _relative_path(raw_archive, "raw archive")
    _reject_symlink_ancestors(target)
    if target.is_symlink() or not target.is_file():
        raise HistoricalDataError("raw archive is not a regular file")
    return target


def _reject_symlink_ancestors(path: Path) -> None:
    absolute = Path(os.path.abspath(path))
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            raise HistoricalDataError(f"path contains a symlink component: {current}")


def _histdata_members(archive: Path, *, month: str = HISTDATA_MONTH) -> list[str]:
    with zipfile.ZipFile(archive, "r") as handle:
        infos = handle.infolist()
        for info in infos:
            _validate_zip_name(info)
        return _histdata_members_from_infos(infos, month=month)


def _histdata_members_from_infos(
    infos: Sequence[zipfile.ZipInfo],
    *,
    month: str = HISTDATA_MONTH,
) -> list[str]:
    month = _normalize_histdata_month(month)
    marker = f"_t_{month}".lower()
    members = [
        info.filename
        for info in infos
        if not info.is_dir()
        and info.filename.lower().endswith(".csv")
        and "eurusd" in info.filename.lower()
        and marker in info.filename.lower()
    ]
    if not members:
        raise HistoricalDataError(f"raw ZIP has no EURUSD {month} tick CSV")
    return members


def _validate_manifest_core(manifest: DatasetManifest) -> None:
    _strict_positive_int(manifest.schema_version, "schema_version")
    if manifest.schema_version != DATASET_SCHEMA_VERSION:
        raise HistoricalDataError("unsupported dataset manifest version")
    if not isinstance(manifest.dataset_id, str) or not manifest.dataset_id.strip():
        raise HistoricalDataError("dataset_id must be non-empty")
    if manifest.provider != HISTDATA_PROVIDER or manifest.instrument != HISTDATA_INSTRUMENT:
        raise HistoricalDataError("manifest provider/instrument is not supported")
    if not isinstance(manifest.normalization_version, str) or manifest.normalization_version != NORMALIZATION_VERSION:
        raise HistoricalDataError("unsupported normalization_version")
    if not _is_sha256(manifest.content_hash):
        raise HistoricalDataError("manifest content_hash must be a hexadecimal SHA-256")


def _normalize_histdata_month(value: Any) -> str:
    if not isinstance(value, str):
        raise HistoricalDataError("HistData month must be YYYYMM text")
    month = value.strip()
    if len(month) == 7 and month[4] == "-":
        month = month[:4] + month[5:]
    if len(month) != 6 or not month.isdigit():
        raise HistoricalDataError("HistData month must be YYYYMM text")
    month_number = int(month[4:6])
    if not 1 <= month_number <= 12:
        raise HistoricalDataError("HistData month has an invalid calendar month")
    return month


def _next_histdata_month(month: str) -> str:
    normalized = _normalize_histdata_month(month)
    year = int(normalized[:4])
    month_number = int(normalized[4:6])
    if month_number == 12:
        year += 1
        month_number = 1
    else:
        month_number += 1
    return f"{year:04d}{month_number:02d}"


def _dataset_id_for_partitions(
    partitions: Sequence[DatasetPartition],
    content_hash: str,
) -> str:
    if not partitions:
        raise HistoricalDataError("manifest must contain at least one partition")
    if len(partitions) == 1:
        partition = partitions[0]
        return f"{HISTDATA_PROVIDER}:{HISTDATA_INSTRUMENT}:{partition.month}:{partition.raw_sha256[:32]}"
    return f"{HISTDATA_PROVIDER}:{HISTDATA_INSTRUMENT}:{partitions[0].month}-{partitions[-1].month}:{content_hash[:32]}"


def _infer_histdata_month(archive: Path) -> str:
    _reject_symlink_ancestors(archive)
    if archive.is_symlink() or not archive.is_file():
        raise HistoricalDataError("raw archive is not a regular file")
    months: set[str] = set()
    pattern = re.compile(r"_t_(\d{6})\.csv$", re.IGNORECASE)
    try:
        with zipfile.ZipFile(archive, "r") as handle:
            infos = handle.infolist()
            for info in infos:
                _validate_zip_name(info)
                if info.is_dir():
                    continue
                match = pattern.search(info.filename)
                if match is not None and "eurusd" in info.filename.lower():
                    months.add(_normalize_histdata_month(match.group(1)))
    except (OSError, zipfile.BadZipFile) as exc:
        raise HistoricalDataError(f"cannot inspect HistData archive: {archive}") from exc
    if not months:
        raise HistoricalDataError("raw ZIP has no canonical EURUSD monthly tick CSV")
    if len(months) != 1:
        raise HistoricalDataError("raw ZIP must contain exactly one EURUSD calendar month")
    return next(iter(months))


def _select_histdata_months(
    archives: Sequence[Path],
    months: Sequence[str] | None,
) -> tuple[str, ...]:
    inferred = tuple(_infer_histdata_month(path) for path in archives)
    if months is None:
        return inferred
    selected = tuple(_normalize_histdata_month(item) for item in months)
    if len(selected) != len(archives):
        raise HistoricalDataError("months length must match archives")
    if selected != inferred:
        raise HistoricalDataError("explicit months do not match HistData archive members")
    return selected


def _partition_from_archive(
    archive_path: Path,
    *,
    month: str,
    data_root: Path,
    acquired_at: Any,
    source_uri: Any,
    terms_uri: Any,
    etag: Any,
    last_modified: Any,
) -> DatasetPartition:
    try:
        raw_archive = archive_path.relative_to(data_root).as_posix()
    except ValueError as exc:
        raise HistoricalDataError("every raw archive must be under data_root") from exc
    if archive_path.is_symlink() or not archive_path.is_file():
        raise HistoricalDataError("raw archive is not a regular file")
    raw_size = archive_path.stat().st_size
    if raw_size < 1 or raw_size > MAX_DATASET_BYTES:
        raise HistoricalDataError("raw archive exceeds the dataset limit")
    return DatasetPartition(
        partition_id=month,
        raw_archive=raw_archive,
        raw_sha256=_hash_file(archive_path),
        raw_size=raw_size,
        members=tuple(_histdata_members(archive_path, month=month)),
        source_uri=_metadata_text(source_uri, "source_uri"),
        terms_uri=_metadata_text(terms_uri, "terms_uri"),
        acquired_at=_metadata_datetime(acquired_at, "acquired_at"),
        etag=_metadata_optional_text(etag, "etag"),
        last_modified=_metadata_optional_text(last_modified, "last_modified"),
        month=month,
    )


def _partition_with_coverage(
    manifest: DatasetManifest,
    partition: DatasetPartition,
) -> DatasetPartition:
    summary = _scan_partition(
        manifest,
        partition,
        sequence_start=0,
        previous_time=None,
        start=None,
        end=None,
    )
    return replace(
        partition,
        coverage_start=summary[1],
        coverage_end=summary[2],
        quote_count=summary[0],
    )


def _expand_partition_metadata(
    value: Any,
    count: int,
    name: str,
    *,
    allow_none: bool = False,
) -> tuple[Any, ...]:
    if value is None:
        if allow_none:
            return (None,) * count
        raise HistoricalDataError(f"{name} is required")
    if isinstance(value, (str, bytes, bytearray, datetime, Path)):
        return (value,) * count
    if isinstance(value, Sequence):
        expanded = tuple(value)
        if len(expanded) != count:
            raise HistoricalDataError(f"{name} length must match archives")
        return expanded
    raise HistoricalDataError(f"{name} must be a scalar or sequence")


def _metadata_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HistoricalDataError(f"{name} must be non-empty text")
    return value


def _metadata_optional_text(value: Any, name: str) -> str | None:
    if value is None:
        return None
    return _metadata_text(value, name)


def _metadata_datetime(value: Any, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise HistoricalDataError(f"{name} must be timezone-aware")
    return _utc(value, name)


def _validate_manifest_partitions(manifest: DatasetManifest) -> None:
    if not manifest.partitions:
        raise HistoricalDataError("manifest must contain at least one partition")
    if any(not isinstance(item, DatasetPartition) for item in manifest.partitions):
        raise HistoricalDataError("manifest partitions must be DatasetPartition objects")
    _validate_partition_identities(manifest.partitions)
    _validate_partition_sequence(manifest.partitions)
    expected = _dataset_id_for_partitions(manifest.partitions, manifest.content_hash)
    if manifest.dataset_id != expected:
        raise HistoricalDataError(
            "dataset_id is not bound to the manifest provider, instrument, month range, and content hash"
        )
    if manifest.content_hash != _content_hash(manifest.partitions):
        raise HistoricalDataError("manifest content_hash does not match partitions")


def _validate_partition_identities(partitions: Sequence[DatasetPartition]) -> None:
    partition_ids = [item.partition_id for item in partitions]
    raw_paths = [item.raw_archive for item in partitions]
    raw_hashes = [item.raw_sha256 for item in partitions]
    months = [item.month for item in partitions]
    if len(set(partition_ids)) != len(partition_ids):
        raise HistoricalDataError("manifest partition collision: duplicate partition identity")
    if len(set(raw_paths)) != len(raw_paths):
        raise HistoricalDataError("manifest partition collision: duplicate raw path")
    if len(set(raw_hashes)) != len(raw_hashes):
        raise HistoricalDataError("manifest partition collision: duplicate raw SHA-256")
    if len(set(months)) != len(months):
        raise HistoricalDataError("manifest partition collision: duplicate calendar month")


def _validate_partition_sequence(partitions: Sequence[DatasetPartition]) -> None:
    months = [item.month for item in partitions]
    if months != sorted(months):
        raise HistoricalDataError("manifest partitions must be in chronological month order")
    for previous, current in zip(partitions, partitions[1:], strict=False):
        expected_month = _next_histdata_month(previous.month)
        if current.month != expected_month:
            raise HistoricalDataError(
                f"manifest partition gap between {previous.month} and {current.month}; monthly coverage must be contiguous"
            )
        if (
            previous.coverage_end is not None
            and current.coverage_start is not None
            and current.coverage_start <= previous.coverage_end
        ):
            raise HistoricalDataError(
                f"manifest partition coverage collision between {previous.month} and {current.month}"
            )


def _validate_manifest_root(manifest: DatasetManifest) -> None:
    root = Path(manifest.data_root).expanduser()
    _reject_symlink_ancestors(root)
    if not root.is_absolute() or root.is_symlink():
        raise HistoricalDataError("manifest data_root must be an absolute non-symlink path")


def _validate_zip_name(info: zipfile.ZipInfo) -> None:
    name = info.filename
    if not name or Path(name).is_absolute() or ".." in Path(name).parts:
        raise HistoricalDataError(f"unsafe ZIP member name: {name!r}")
    if _zipinfo_is_symlink(info):
        raise HistoricalDataError(f"ZIP symlink member is forbidden: {name!r}")


def _zipinfo_is_symlink(info: zipfile.ZipInfo) -> bool:
    mode = (info.external_attr >> 16) & 0xFFFF
    return stat.S_ISLNK(mode)


def _content_hash(partitions: Sequence[DatasetPartition]) -> str:
    material = "\n".join(
        f"{partition.partition_id}\0{partition.raw_sha256}\0{partition.raw_size}\0{','.join(partition.members)}"
        for partition in partitions
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _normalize_coverage(value: Any) -> None:
    start = getattr(value, "coverage_start", None)
    end = getattr(value, "coverage_end", None)
    if start is not None:
        start = _utc(start, "coverage_start")
        object.__setattr__(value, "coverage_start", start)
    if end is not None:
        end = _utc(end, "coverage_end")
        object.__setattr__(value, "coverage_end", end)
    if start is not None and end is not None and end < start:
        raise HistoricalDataError("coverage_end must not precede coverage_start")


def _relative_path(value: str, label: str) -> Path:
    path = Path(value)
    if not isinstance(value, str) or not value or path.is_absolute() or ".." in path.parts:
        raise HistoricalDataError(f"{label} must be a safe relative path")
    return path


def _required_text(value: Mapping[str, Any], key: str) -> str:
    raw = value.get(key)
    if not isinstance(raw, str) or not raw.strip():
        raise HistoricalDataError(f"{key} must be non-empty text")
    return raw.strip()


def _optional_text(value: Mapping[str, Any], key: str, default: str) -> str:
    raw = value.get(key, default)
    if not isinstance(raw, str) or not raw.strip():
        raise HistoricalDataError(f"{key} must be non-empty text")
    return raw.strip()


def _strict_positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise HistoricalDataError(f"{name} must be a positive integer")
    return value


def _strict_nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise HistoricalDataError(f"{name} must be a non-negative integer")
    return value


def _positive_decimal(value: Decimal | str | Any, name: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise HistoricalDataError(f"{name} must be a decimal") from exc
    if not result.is_finite() or result <= 0:
        raise HistoricalDataError(f"{name} must be finite and positive")
    return result


def _nonnegative_decimal(value: Decimal | str | Any, name: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise HistoricalDataError(f"{name} must be a decimal") from exc
    if not result.is_finite() or result < 0:
        raise HistoricalDataError(f"{name} must be finite and non-negative")
    return result


def _strict_keys(value: Mapping[str, Any], allowed: frozenset[str], label: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise HistoricalDataError(f"{label} has unknown keys: {sorted(str(item) for item in unknown)}")


def _is_sha256(value: str) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdefABCDEF" for char in value)


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise HistoricalDataError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _bound(value: datetime | None, name: str) -> datetime | None:
    return _utc(value, name) if value is not None else None


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value is not None else None


def _parse_iso(value: Any, name: str) -> datetime:
    parsed = _parse_optional_iso(value, name)
    if parsed is None:
        raise HistoricalDataError(f"{name} is required")
    return parsed


def _parse_optional_iso(value: Any, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise HistoricalDataError(f"{name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HistoricalDataError(f"{name} is not a valid ISO timestamp") from exc
    return _utc(parsed, name)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


HistoricalDataProvider = HistoricalQuoteProvider

__all__ = [
    "DATASET_SCHEMA_VERSION",
    "DEFAULT_DATA_ROOT",
    "DatasetManifest",
    "DatasetPartition",
    "DatasetValidation",
    "HISTDATA_DEVELOPMENT_END_MONTH",
    "HISTDATA_DEVELOPMENT_START_MONTH",
    "HISTDATA_HOLDOUT_START_MONTH",
    "HISTDATA_INSTRUMENT",
    "HISTDATA_MONTH",
    "HISTDATA_PROVIDER",
    "HISTDATA_SOURCE_TIMEZONE",
    "HistoricalManifest",
    "HistoricalDataError",
    "HistoricalDataProvider",
    "HistoricalQuote",
    "HistoricalQuoteProvider",
    "HistoricalValidation",
    "MAX_DATASET_BYTES",
    "MAX_VALIDATION_ISSUES",
    "NORMALIZATION_VERSION",
    "SourceLocator",
    "iter_quotes",
    "manifest_from_histdata_archive",
    "manifest_from_histdata_archives",
    "read_manifest",
    "stream",
    "validate",
    "validate_dataset",
]
