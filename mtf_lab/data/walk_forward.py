"""Contracts for the isolated 2020--2023 walk-forward data stage.

This module is intentionally separate from :mod:`mtf_lab.data.historical`.
``DatasetManifest`` is the immutable HistData development contract and must
not be relabelled as walk-forward data (or as the 2024--2025 holdout).  The
walk-forward manifest below therefore has its own partition type, schema and
identity.  It only reads already acquired raw ZIPs; acquisition is owned by a
different layer and is never started by this module.

The public contracts are useful before a runner exists:

* ``WalkForwardManifest`` binds exactly the 48 HistData months 2020--2023;
* ``WalkForwardInput`` binds both manifests and every frozen input hash;
* ``WalkForwardStreamGuard`` makes the development-to-window boundary
  explicit and rejects holdout events; and
* ``WalkForwardReceipt`` is a small, fail-closed receipt for validation or a
  future review-only run.

All intervals are UTC and half-open.  A validation or stream operation never
sorts, clamps, fills, downloads, writes a manifest, or opens the holdout.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast

from .historical import (
    DEFAULT_DATA_ROOT,
    HISTDATA_INSTRUMENT,
    HISTDATA_PROVIDER,
    HISTDATA_SOURCE_OFFSET,
    MAX_DATASET_BYTES,
    NORMALIZATION_VERSION,
    DatasetManifest,
    DatasetPartition,
    HistoricalDataError,
    HistoricalQuote,
    SourceLocator,
    _histdata_members,
    _histdata_members_from_infos,
    _infer_histdata_month,
    _is_sha256,
    _iso,
    _parse_histdata_line,
    _parse_iso,
    _parse_optional_iso,
    _reject_symlink_ancestors,
    _relative_path,
    _strict_keys,
    _strict_nonnegative_int,
    _strict_positive_int,
    _utc,
    _validate_zip_name,
    _zipinfo_is_symlink,
    read_manifest,
)

WALK_FORWARD_MANIFEST_SCHEMA = "mtf-lab.walk-forward-manifest.v1"
WALK_FORWARD_INPUT_SCHEMA = "mtf-lab.walk-forward-input.v1"
WALK_FORWARD_RECEIPT_SCHEMA = "mtf-lab.walk-forward-receipt.v1"
WALK_FORWARD_SCHEMA_VERSION = 1

WALK_FORWARD_PROVIDER = HISTDATA_PROVIDER
WALK_FORWARD_INSTRUMENT = HISTDATA_INSTRUMENT
WALK_FORWARD_NORMALIZATION_VERSION = NORMALIZATION_VERSION
WALK_FORWARD_START_MONTH = "202001"
WALK_FORWARD_END_MONTH = "202312"
WALK_FORWARD_HOLDOUT_START_MONTH = "202401"
WALK_FORWARD_START = datetime(2020, 1, 1, tzinfo=UTC)
WALK_FORWARD_END = datetime(2024, 1, 1, tzinfo=UTC)
WALK_FORWARD_HOLDOUT_END = datetime(2026, 1, 1, tzinfo=UTC)
DEVELOPMENT_START = datetime(2016, 1, 1, tzinfo=UTC)
WARMUP_START = datetime(2015, 1, 1, tzinfo=UTC)
WALK_FORWARD_WINDOW_IDS = ("WF_2020", "WF_2021", "WF_2022", "WF_2023")

WalkForwardPhase: TypeAlias = Literal["WARMUP_ONLY", "WF_EVALUATION"]

_MANIFEST_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "walk_forward_id",
        "manifest_id",
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
_PARTITION_KEYS = frozenset(
    {
        "partition_id",
        "month",
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
    }
)
_INPUT_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "stage",
        "development_manifest_hash",
        "walk_forward_manifest_hash",
        "protocol_hash",
        "risk_hash",
        "calendar_hash",
        "costs_hash",
        "code_hash",
        "runtime_hash",
        "holdout",
        "holdout_start",
        "holdout_end",
        "input_hash",
    }
)
_RECEIPT_KEYS = frozenset(
    {
        "schema",
        "schema_version",
        "receipt_type",
        "stage",
        "status",
        "attempt_id",
        "window_id",
        "development_manifest_hash",
        "walk_forward_manifest_hash",
        "input_hash",
        "holdout",
        "selection_performed",
        "promotion_performed",
        "trading_enabled",
        "auto_promote",
        "network_performed",
        "data_acquisition",
        "costs_status",
        "conclusion",
        "error",
    }
)


class WalkForwardError(HistoricalDataError):
    """The walk-forward contract, identity or boundary is invalid."""


class WalkForwardIdentityError(WalkForwardError):
    """A source, input or receipt is not bound to the declared identity."""


@dataclass(frozen=True, slots=True)
class WalkForwardPartition:
    """One immutable HistData raw archive in the 2020--2023 WF corpus."""

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
    month: str = WALK_FORWARD_START_MONTH

    def __post_init__(self) -> None:
        month = _month(self.month)
        _validate_partition_identity(self, month)
        _validate_partition_storage(self)
        if not isinstance(self.members, tuple):
            object.__setattr__(self, "members", tuple(self.members))
        _validate_partition_members(self)
        _validate_partition_month(self, month)
        _validate_partition_metadata(self)
        object.__setattr__(self, "month", month)
        object.__setattr__(self, "raw_sha256", self.raw_sha256.lower())
        object.__setattr__(self, "acquired_at", _utc(self.acquired_at, "acquired_at"))
        _normalize_partition_coverage(self)

    @property
    def sha256(self) -> str:
        return self.raw_sha256

    @property
    def size(self) -> int:
        return self.raw_size

    @property
    def raw_path(self) -> str:
        """Provider-neutral alias; no path is resolved or followed here."""

        return self.raw_archive

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "month": self.month,
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
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WalkForwardPartition:
        _strict_keys(value, _PARTITION_KEYS, "walk-forward partition")
        members = value.get("members")
        if not isinstance(members, list) or not all(isinstance(item, str) for item in members):
            raise WalkForwardError("walk-forward partition members must be a list of strings")
        month = _required_mapping_text(value, "month")
        partition_id = _required_mapping_text(value, "partition_id")
        if partition_id != month:
            raise WalkForwardError("partition_id and month must be equal")
        return cls(
            partition_id=partition_id,
            month=month,
            raw_archive=_required_mapping_text(value, "raw_archive"),
            raw_sha256=_required_mapping_text(value, "raw_sha256"),
            raw_size=_strict_positive_int(value.get("raw_size"), "raw_size"),
            members=tuple(members),
            source_uri=_required_mapping_text(value, "source_uri"),
            terms_uri=_required_mapping_text(value, "terms_uri"),
            acquired_at=_parse_iso(value.get("acquired_at"), "acquired_at"),
            coverage_start=_parse_optional_iso(value.get("coverage_start"), "coverage_start"),
            coverage_end=_parse_optional_iso(value.get("coverage_end"), "coverage_end"),
            quote_count=(
                _strict_nonnegative_int(value.get("quote_count"), "quote_count")
                if value.get("quote_count") is not None
                else None
            ),
            etag=_optional_mapping_text(value, "etag"),
            last_modified=_optional_mapping_text(value, "last_modified"),
        )


@dataclass(frozen=True, slots=True)
class WalkForwardManifest:
    """Independent, complete manifest for HistData WF 2020--2023.

    Exactly 48 chronological monthly partitions are required.  The manifest
    can be constructed before the raw files are scanned (coverage/counts may
    be ``None``), but ``validate_walk_forward_manifest`` is the gate that
    proves the bytes and decoded rows.
    """

    walk_forward_id: str
    provider: str
    instrument: str
    partitions: tuple[WalkForwardPartition, ...]
    coverage_start: datetime | None
    coverage_end: datetime | None
    content_hash: str
    data_root: str = str(DEFAULT_DATA_ROOT)
    normalization_version: str = WALK_FORWARD_NORMALIZATION_VERSION
    schema_version: int = WALK_FORWARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _strict_positive_int(self.schema_version, "schema_version")
        if self.schema_version != WALK_FORWARD_SCHEMA_VERSION:
            raise WalkForwardError("unsupported walk-forward manifest schema_version")
        if not isinstance(self.walk_forward_id, str) or not self.walk_forward_id.strip():
            raise WalkForwardError("walk_forward_id must be non-empty")
        if self.provider != WALK_FORWARD_PROVIDER or self.instrument != WALK_FORWARD_INSTRUMENT:
            raise WalkForwardError("walk-forward provider/instrument mismatch")
        if self.normalization_version != WALK_FORWARD_NORMALIZATION_VERSION:
            raise WalkForwardError("unsupported walk-forward normalization_version")
        if not _is_sha256(self.content_hash):
            raise WalkForwardError("content_hash must be a hexadecimal SHA-256")
        object.__setattr__(self, "content_hash", self.content_hash.lower())
        if not isinstance(self.partitions, tuple):
            object.__setattr__(self, "partitions", tuple(self.partitions))
        if any(not isinstance(item, WalkForwardPartition) for item in self.partitions):
            raise WalkForwardError("walk-forward partitions must be WalkForwardPartition objects")
        _validate_walk_forward_partitions(self.partitions)
        _validate_walk_forward_root(self.data_root)
        _normalize_manifest_coverage(self)
        expected_hash = walk_forward_content_hash(self.partitions)
        if self.content_hash.lower() != expected_hash:
            raise WalkForwardError("walk-forward content_hash does not match partitions")
        expected_id = _walk_forward_id(self.partitions, expected_hash)
        if self.walk_forward_id != expected_id:
            raise WalkForwardError("walk_forward_id is not bound to the partitions/content_hash")

    @property
    def manifest_id(self) -> str:
        return self.walk_forward_id

    @property
    def dataset_id(self) -> str:
        """Read-only compatibility label; it is not a DatasetManifest."""

        return self.walk_forward_id

    @property
    def version(self) -> int:
        return self.schema_version

    @property
    def partition_paths(self) -> tuple[Path, ...]:
        root = Path(self.data_root).expanduser()
        return tuple(root / item.raw_archive for item in self.partitions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": WALK_FORWARD_MANIFEST_SCHEMA,
            "schema_version": self.schema_version,
            "walk_forward_id": self.walk_forward_id,
            "provider": self.provider,
            "instrument": self.instrument,
            "normalization_version": self.normalization_version,
            "data_root": self.data_root,
            "coverage_start": _iso(self.coverage_start),
            "coverage_end": _iso(self.coverage_end),
            "content_hash": self.content_hash,
            "partitions": [item.to_dict() for item in self.partitions],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WalkForwardManifest:
        _strict_keys(value, _MANIFEST_KEYS, "walk-forward manifest")
        if value.get("schema") not in (None, WALK_FORWARD_MANIFEST_SCHEMA):
            raise WalkForwardError("walk-forward manifest schema is invalid")
        raw_id = value.get("walk_forward_id", value.get("manifest_id"))
        walk_forward_id = _required_text(raw_id, "walk_forward_id")
        if (
            value.get("walk_forward_id") is not None
            and value.get("manifest_id") is not None
            and value.get("walk_forward_id") != value.get("manifest_id")
        ):
            raise WalkForwardError("walk_forward_id and manifest_id mismatch")
        raw_partitions = value.get("partitions")
        if not isinstance(raw_partitions, list) or any(not isinstance(item, Mapping) for item in raw_partitions):
            raise WalkForwardError("walk-forward partitions must be a list of objects")
        return cls(
            schema_version=_strict_positive_int(value.get("schema_version"), "schema_version"),
            walk_forward_id=walk_forward_id,
            provider=_required_text(value.get("provider"), "provider"),
            instrument=_required_text(value.get("instrument"), "instrument"),
            normalization_version=_required_text(value.get("normalization_version"), "normalization_version"),
            data_root=_required_text(value.get("data_root"), "data_root"),
            coverage_start=_parse_optional_iso(value.get("coverage_start"), "coverage_start"),
            coverage_end=_parse_optional_iso(value.get("coverage_end"), "coverage_end"),
            content_hash=_required_text(value.get("content_hash"), "content_hash"),
            partitions=tuple(WalkForwardPartition.from_mapping(item) for item in raw_partitions),
        )

    @classmethod
    def from_path(cls, path: str | Path) -> WalkForwardManifest:
        target = Path(path).expanduser()
        _reject_symlink_ancestors(target)
        if target.is_symlink() or not target.is_file():
            raise WalkForwardError("walk-forward manifest is not a regular file")
        try:
            raw = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WalkForwardError(f"cannot read walk-forward manifest: {target}") from exc
        if not isinstance(raw, Mapping):
            raise WalkForwardError("walk-forward manifest must be a JSON object")
        return cls.from_mapping(raw)


@dataclass(frozen=True, slots=True)
class WalkForwardValidation:
    """Bounded byte/row validation result for a WF manifest."""

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
    partition_counts: tuple[tuple[int, datetime | None, datetime | None], ...] = ()

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


@dataclass(frozen=True, slots=True)
class WalkForwardWindow:
    """One exact annual WF window and its preceding warmup boundary."""

    window_id: str
    warmup_start: datetime
    warmup_end: datetime
    test_start: datetime
    test_end: datetime

    def __post_init__(self) -> None:
        if self.window_id not in WALK_FORWARD_WINDOW_IDS:
            raise WalkForwardError(f"unknown walk-forward window: {self.window_id!r}")
        warmup_start = _utc(self.warmup_start, "warmup_start")
        warmup_end = _utc(self.warmup_end, "warmup_end")
        test_start = _utc(self.test_start, "test_start")
        test_end = _utc(self.test_end, "test_end")
        if warmup_start >= warmup_end or warmup_end != test_start or test_start >= test_end:
            raise WalkForwardError("warmup and WF boundaries must be adjacent half-open intervals")
        if warmup_start < WARMUP_START:
            raise WalkForwardError("warmup_start precedes the approved warmup boundary")
        if test_start < WALK_FORWARD_START or test_end > WALK_FORWARD_END:
            raise WalkForwardError("WF window lies outside 2020--2023")
        year = int(self.window_id[-4:])
        expected_start = datetime(year, 1, 1, tzinfo=UTC)
        expected_end = datetime(year + 1, 1, 1, tzinfo=UTC)
        if test_start != expected_start or test_end != expected_end:
            raise WalkForwardError("WF window must be the exact calendar year named by window_id")
        object.__setattr__(self, "warmup_start", warmup_start)
        object.__setattr__(self, "warmup_end", warmup_end)
        object.__setattr__(self, "test_start", test_start)
        object.__setattr__(self, "test_end", test_end)

    @property
    def start(self) -> datetime:
        return self.test_start

    @property
    def end(self) -> datetime:
        return self.test_end

    @property
    def year(self) -> int:
        return int(self.window_id[-4:])

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_id": self.window_id,
            "warmup_start": _iso(self.warmup_start),
            "warmup_end": _iso(self.warmup_end),
            "test_start": _iso(self.test_start),
            "test_end": _iso(self.test_end),
            "warmup_preserved": True,
        }


@dataclass(frozen=True, slots=True)
class WalkForwardBoundary:
    """Classify timestamps without allowing the holdout to become a phase."""

    window: WalkForwardWindow

    def classify(self, event_time: datetime) -> WalkForwardPhase | Literal["OUTSIDE", "HOLDOUT_REJECTED"]:
        timestamp = _utc(event_time, "event_time")
        if timestamp < self.window.warmup_start:
            return "OUTSIDE"
        if timestamp < self.window.test_start:
            return "WARMUP_ONLY"
        if timestamp < self.window.test_end:
            return "WF_EVALUATION"
        if timestamp >= WALK_FORWARD_END:
            return "HOLDOUT_REJECTED"
        return "OUTSIDE"

    def to_dict(self) -> dict[str, Any]:
        return {"window": self.window.to_dict(), "holdout": "CLOSED"}


def canonical_walk_forward_windows(*, warmup_start: datetime = DEVELOPMENT_START) -> tuple[WalkForwardWindow, ...]:
    """Return the four exact windows with an explicit causal warmup prefix."""

    start = _utc(warmup_start, "warmup_start")
    windows: list[WalkForwardWindow] = []
    for index, window_id in enumerate(WALK_FORWARD_WINDOW_IDS):
        year = 2020 + index
        test_start = datetime(year, 1, 1, tzinfo=UTC)
        windows.append(
            WalkForwardWindow(
                window_id=window_id,
                warmup_start=start,
                warmup_end=test_start,
                test_start=test_start,
                test_end=datetime(year + 1, 1, 1, tzinfo=UTC),
            )
        )
    return tuple(windows)


WALK_FORWARD_WINDOWS = canonical_walk_forward_windows()


@dataclass(frozen=True, slots=True)
class WalkForwardInput:
    """Hash-only composite input for one future WF campaign."""

    development_manifest_hash: str
    walk_forward_manifest_hash: str
    protocol_hash: str
    risk_hash: str
    calendar_hash: str
    costs_hash: str
    code_hash: str
    runtime_hash: str
    holdout_start: datetime = WALK_FORWARD_END
    holdout_end: datetime = WALK_FORWARD_HOLDOUT_END
    holdout: str = "CLOSED"
    schema: str = WALK_FORWARD_INPUT_SCHEMA
    schema_version: int = WALK_FORWARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema != WALK_FORWARD_INPUT_SCHEMA or self.schema_version != WALK_FORWARD_SCHEMA_VERSION:
            raise WalkForwardError("walk-forward input schema/version mismatch")
        for value, name in (
            (self.development_manifest_hash, "development_manifest_hash"),
            (self.walk_forward_manifest_hash, "walk_forward_manifest_hash"),
            (self.protocol_hash, "protocol_hash"),
            (self.risk_hash, "risk_hash"),
            (self.calendar_hash, "calendar_hash"),
            (self.costs_hash, "costs_hash"),
            (self.code_hash, "code_hash"),
            (self.runtime_hash, "runtime_hash"),
        ):
            if not _is_sha256(value):
                raise WalkForwardError(f"{name} must be a hexadecimal SHA-256")
        start = _utc(self.holdout_start, "holdout_start")
        end = _utc(self.holdout_end, "holdout_end")
        if start != WALK_FORWARD_END or end != WALK_FORWARD_HOLDOUT_END:
            raise WalkForwardError("holdout exclusion must be [2024-01-01, 2026-01-01)")
        if self.holdout != "CLOSED":
            raise WalkForwardError("walk-forward input holdout must be CLOSED")
        object.__setattr__(self, "holdout_start", start)
        object.__setattr__(self, "holdout_end", end)
        for name in (
            "development_manifest_hash",
            "walk_forward_manifest_hash",
            "protocol_hash",
            "risk_hash",
            "calendar_hash",
            "costs_hash",
            "code_hash",
            "runtime_hash",
        ):
            object.__setattr__(self, name, str(getattr(self, name)).lower())

    @classmethod
    def from_manifests(
        cls,
        development_manifest: DatasetManifest | str | Path,
        walk_forward_manifest: WalkForwardManifest | str | Path,
        *,
        protocol_hash: str,
        risk_hash: str,
        calendar_hash: str,
        costs_hash: str,
        code_hash: str,
        runtime_hash: str,
    ) -> WalkForwardInput:
        development = _coerce_development_manifest(development_manifest)
        walk_forward = _coerce_walk_forward_manifest(walk_forward_manifest)
        if development.provider != HISTDATA_PROVIDER or development.instrument != HISTDATA_INSTRUMENT:
            raise WalkForwardError("development manifest provider/instrument mismatch")
        if any(item.month >= WALK_FORWARD_START_MONTH for item in development.partitions):
            raise WalkForwardError("development manifest cannot contain WF or holdout months")
        if development.coverage_end is not None and development.coverage_end > WALK_FORWARD_START:
            raise WalkForwardError("development manifest overlaps the WF boundary")
        return cls(
            development_manifest_hash=_manifest_content_hash(development),
            walk_forward_manifest_hash=walk_forward.content_hash,
            protocol_hash=protocol_hash,
            risk_hash=risk_hash,
            calendar_hash=calendar_hash,
            costs_hash=costs_hash,
            code_hash=code_hash,
            runtime_hash=runtime_hash,
        )

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "schema": self.schema,
            "schema_version": self.schema_version,
            "stage": "walk-forward",
            "development_manifest_hash": self.development_manifest_hash,
            "walk_forward_manifest_hash": self.walk_forward_manifest_hash,
            "protocol_hash": self.protocol_hash,
            "risk_hash": self.risk_hash,
            "calendar_hash": self.calendar_hash,
            "costs_hash": self.costs_hash,
            "code_hash": self.code_hash,
            "runtime_hash": self.runtime_hash,
            "holdout": self.holdout,
            "holdout_start": _iso(self.holdout_start),
            "holdout_end": _iso(self.holdout_end),
        }
        if include_hash:
            value["input_hash"] = self.identity_hash
        return value

    @property
    def identity_hash(self) -> str:
        encoded = json.dumps(
            self.to_dict(include_hash=False), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WalkForwardInput:
        _strict_keys(value, _INPUT_KEYS, "walk-forward input")
        if value.get("schema") != WALK_FORWARD_INPUT_SCHEMA:
            raise WalkForwardError("walk-forward input schema is invalid")
        if value.get("stage") != "walk-forward":
            raise WalkForwardError("walk-forward input stage is invalid")
        result = cls(
            schema=str(value.get("schema")),
            schema_version=_strict_positive_int(value.get("schema_version"), "schema_version"),
            development_manifest_hash=_required_text(
                value.get("development_manifest_hash"), "development_manifest_hash"
            ),
            walk_forward_manifest_hash=_required_text(
                value.get("walk_forward_manifest_hash"), "walk_forward_manifest_hash"
            ),
            protocol_hash=_required_text(value.get("protocol_hash"), "protocol_hash"),
            risk_hash=_required_text(value.get("risk_hash"), "risk_hash"),
            calendar_hash=_required_text(value.get("calendar_hash"), "calendar_hash"),
            costs_hash=_required_text(value.get("costs_hash"), "costs_hash"),
            code_hash=_required_text(value.get("code_hash"), "code_hash"),
            runtime_hash=_required_text(value.get("runtime_hash"), "runtime_hash"),
            holdout=str(value.get("holdout")),
            holdout_start=_parse_iso(value.get("holdout_start"), "holdout_start"),
            holdout_end=_parse_iso(value.get("holdout_end"), "holdout_end"),
        )
        if value.get("input_hash") is not None and value.get("input_hash") != result.identity_hash:
            raise WalkForwardIdentityError("walk-forward input_hash does not match frozen inputs")
        return result

    @classmethod
    def from_json(cls, value: str) -> WalkForwardInput:
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise WalkForwardError("walk-forward input is not valid JSON") from exc
        if not isinstance(decoded, Mapping):
            raise WalkForwardError("walk-forward input must be a JSON object")
        return cls.from_mapping(decoded)


@dataclass(frozen=True, slots=True)
class WalkForwardCursor:
    """Small source cursor stored by ``WalkForwardStreamGuard``."""

    phase: WalkForwardPhase
    manifest_role: Literal["development", "walk-forward"]
    partition_index: int
    member_index: int
    locator: SourceLocator
    event_time: datetime
    source_event_id: str
    accepted_count: int

    def __post_init__(self) -> None:
        if self.phase not in ("WARMUP_ONLY", "WF_EVALUATION"):
            raise WalkForwardIdentityError("cursor phase is invalid")
        if self.manifest_role not in ("development", "walk-forward"):
            raise WalkForwardIdentityError("cursor manifest_role is invalid")
        _strict_nonnegative_int(self.partition_index, "cursor partition_index")
        _strict_nonnegative_int(self.member_index, "cursor member_index")
        _strict_positive_int(self.accepted_count, "cursor accepted_count")
        if not isinstance(self.locator, SourceLocator):
            raise WalkForwardIdentityError("cursor locator must be SourceLocator")
        event_time = _utc(self.event_time, "cursor event_time")
        object.__setattr__(self, "event_time", event_time)
        expected = _source_event_id(self.locator)
        if self.source_event_id != expected:
            raise WalkForwardIdentityError("cursor source_event_id is not bound to locator")

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "manifest_role": self.manifest_role,
            "partition_index": self.partition_index,
            "member_index": self.member_index,
            "locator": self.locator.to_dict(),
            "event_time": _iso(self.event_time),
            "source_event_id": self.source_event_id,
            "accepted_count": self.accepted_count,
        }


class WalkForwardStreamGuard:
    """Causal, manifest-bound guard for development warmup then one WF year.

    The guard does not evaluate a strategy.  It only verifies that the caller
    feeds a development prefix as ``WARMUP_ONLY`` and then the selected exact
    calendar-year window as ``WF_EVALUATION``.  Quotes from another year or
    from 2024+ are rejected, as are source skips, regressions and duplicates.
    """

    __slots__ = (
        "_development_manifest",
        "_walk_forward_manifest",
        "_window",
        "_boundary",
        "_input",
        "_manifest_hashes",
        "_development_identities",
        "_walk_forward_identities",
        "_last_rank",
        "_last_event_time",
        "_cursor",
    )

    def __init__(
        self,
        development_manifest: DatasetManifest | str | Path,
        walk_forward_manifest: WalkForwardManifest | str | Path,
        *,
        window: WalkForwardWindow | str = "WF_2020",
        input_contract: WalkForwardInput | None = None,
    ) -> None:
        development = _coerce_development_manifest(development_manifest)
        walk_forward = _coerce_walk_forward_manifest(walk_forward_manifest)
        selected_window = _coerce_window(window)
        if any(item.month >= WALK_FORWARD_START_MONTH for item in development.partitions):
            raise WalkForwardIdentityError("development manifest contains WF/holdout data")
        if development.coverage_end is not None and development.coverage_end > selected_window.test_start:
            raise WalkForwardIdentityError("development manifest crosses selected WF boundary")
        development_identities = _source_identities(development.partitions, role="development")
        walk_forward_identities = _source_identities(walk_forward.partitions, role="walk-forward")
        development_paths = {key[0] for key in development_identities}
        walk_forward_paths = {key[0] for key in walk_forward_identities}
        development_hash_members = {(key[1], key[2]) for key in development_identities}
        walk_forward_hash_members = {(key[1], key[2]) for key in walk_forward_identities}
        if (
            set(development_identities).intersection(walk_forward_identities)
            or development_paths.intersection(walk_forward_paths)
            or development_hash_members.intersection(walk_forward_hash_members)
        ):
            raise WalkForwardIdentityError("development and WF manifests share a source identity")
        if input_contract is not None:
            if input_contract.development_manifest_hash != _manifest_content_hash(development):
                raise WalkForwardIdentityError("input development manifest hash mismatch")
            if input_contract.walk_forward_manifest_hash != walk_forward.content_hash:
                raise WalkForwardIdentityError("input walk-forward manifest hash mismatch")
        self._development_manifest = development
        self._walk_forward_manifest = walk_forward
        self._window = selected_window
        self._boundary = WalkForwardBoundary(selected_window)
        self._input = input_contract
        self._manifest_hashes = {
            "development": _manifest_content_hash(development),
            "walk-forward": walk_forward.content_hash,
        }
        self._development_identities = development_identities
        self._walk_forward_identities = walk_forward_identities
        self._last_rank: tuple[int, int, int, int, int] | None = None
        self._last_event_time: datetime | None = None
        self._cursor: WalkForwardCursor | None = None

    @property
    def development_manifest(self) -> DatasetManifest:
        return self._development_manifest

    @property
    def walk_forward_manifest(self) -> WalkForwardManifest:
        return self._walk_forward_manifest

    @property
    def window(self) -> WalkForwardWindow:
        return self._window

    @property
    def boundary(self) -> WalkForwardBoundary:
        return self._boundary

    @property
    def input_contract(self) -> WalkForwardInput | None:
        return self._input

    @property
    def cursor(self) -> WalkForwardCursor | None:
        return self._cursor

    @property
    def manifest_hashes(self) -> Mapping[str, str]:
        return dict(self._manifest_hashes)

    def validate(self, quote: HistoricalQuote, *, phase: WalkForwardPhase | None = None) -> WalkForwardPhase:
        """Validate one quote transactionally and return its required phase."""

        expected_phase = self._validate_phase(quote, phase)
        role, partition_index, member_index = self._locate_quote(quote)
        self._validate_role(expected_phase, role, partition_index, quote)
        rank = self._rank(expected_phase, partition_index, member_index, quote)
        self._validate_source_position(expected_phase, role, partition_index, member_index)
        self._validate_progress(expected_phase, rank, quote)
        self._commit(expected_phase, role, partition_index, member_index, rank, quote)
        return expected_phase

    def _validate_phase(self, quote: HistoricalQuote, phase: WalkForwardPhase | None) -> WalkForwardPhase:
        if not isinstance(quote, HistoricalQuote):
            raise WalkForwardIdentityError("walk-forward stream requires HistoricalQuote")
        expected = self._boundary.classify(quote.event_time)
        if expected == "OUTSIDE":
            raise WalkForwardError("quote lies outside the selected warmup/WF window")
        if expected == "HOLDOUT_REJECTED":
            raise WalkForwardError("holdout is CLOSED: 2024+ quote rejected")
        if phase is not None and phase != expected:
            raise WalkForwardError(f"quote phase {phase!r} does not match boundary {expected!r}")
        return expected

    def _validate_role(
        self,
        phase: WalkForwardPhase,
        role: Literal["development", "walk-forward"],
        partition_index: int,
        quote: HistoricalQuote,
    ) -> None:
        if phase == "WARMUP_ONLY" and role != "development":
            raise WalkForwardIdentityError("WARMUP_ONLY quote must come from development manifest")
        if phase == "WF_EVALUATION" and role != "walk-forward":
            raise WalkForwardIdentityError("WF quote must come from WalkForwardManifest")
        partitions = (
            self._development_manifest.partitions if role == "development" else self._walk_forward_manifest.partitions
        )
        partition = partitions[partition_index]
        if phase == "WARMUP_ONLY" and partition.month >= WALK_FORWARD_START_MONTH:
            raise WalkForwardIdentityError("WARMUP_ONLY partition is not development data")
        if phase == "WF_EVALUATION" and partition.month[:4] != str(self._window.year):
            raise WalkForwardIdentityError("WF quote belongs to another calendar-year partition")
        if quote.provider != HISTDATA_PROVIDER or quote.instrument != HISTDATA_INSTRUMENT:
            raise WalkForwardIdentityError("quote provider/instrument mismatch")

    def _validate_source_position(
        self,
        phase: WalkForwardPhase,
        role: Literal["development", "walk-forward"],
        partition_index: int,
        member_index: int,
    ) -> None:
        cursor = self._cursor
        if cursor is None:
            _require_development_start(phase, role, partition_index, member_index)
            return
        _validate_phase_transition(self._window, cursor, phase, role, partition_index, member_index)
        _validate_same_manifest_position(cursor, role, partition_index, member_index)

    @staticmethod
    def _rank(
        phase: WalkForwardPhase,
        partition_index: int,
        member_index: int,
        quote: HistoricalQuote,
    ) -> tuple[int, int, int, int, int]:
        return (
            0 if phase == "WARMUP_ONLY" else 1,
            partition_index,
            member_index,
            quote.locator.offset,
            quote.locator.row,
        )

    def _validate_progress(
        self,
        phase: WalkForwardPhase,
        rank: tuple[int, int, int, int, int],
        quote: HistoricalQuote,
    ) -> None:
        if self._last_rank is not None and rank <= self._last_rank:
            raise WalkForwardIdentityError("walk-forward source order regressed or duplicated")
        if self._last_event_time is not None and quote.event_time < self._last_event_time:
            raise WalkForwardIdentityError("walk-forward event_time regressed")
        if self._cursor is not None and phase == "WARMUP_ONLY" and self._cursor.phase != "WARMUP_ONLY":
            raise WalkForwardError("WARMUP_ONLY cannot resume after WF evaluation")

    def _commit(
        self,
        phase: WalkForwardPhase,
        role: Literal["development", "walk-forward"],
        partition_index: int,
        member_index: int,
        rank: tuple[int, int, int, int, int],
        quote: HistoricalQuote,
    ) -> None:
        accepted_count = 1 if self._cursor is None else self._cursor.accepted_count + 1
        self._cursor = WalkForwardCursor(
            phase=phase,
            manifest_role=role,
            partition_index=partition_index,
            member_index=member_index,
            locator=quote.locator,
            event_time=quote.event_time,
            source_event_id=quote.source_event_id,
            accepted_count=accepted_count,
        )
        self._last_rank = rank
        self._last_event_time = quote.event_time

    def accept(self, quote: HistoricalQuote, *, phase: WalkForwardPhase | None = None) -> HistoricalQuote:
        self.validate(quote, phase=phase)
        return quote

    def validate_many(
        self,
        quotes: Iterable[HistoricalQuote],
        *,
        phase: WalkForwardPhase | None = None,
    ) -> Iterable[HistoricalQuote]:
        for quote in quotes:
            yield self.accept(quote, phase=phase)

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema": "mtf-lab.walk-forward-stream-guard.v1",
            "schema_version": WALK_FORWARD_SCHEMA_VERSION,
            "development_manifest_hash": self._manifest_hashes["development"],
            "walk_forward_manifest_hash": self._manifest_hashes["walk-forward"],
            "window_id": self._window.window_id,
            "input_hash": self._input.identity_hash if self._input is not None else None,
            "holdout": "CLOSED",
            "cursor": self._cursor.to_dict() if self._cursor is not None else None,
        }

    @classmethod
    def from_snapshot(
        cls,
        development_manifest: DatasetManifest | str | Path,
        walk_forward_manifest: WalkForwardManifest | str | Path,
        snapshot: Mapping[str, Any] | str,
        *,
        input_contract: WalkForwardInput | None = None,
    ) -> WalkForwardStreamGuard:
        raw = _snapshot_mapping(snapshot)
        window_id = _snapshot_window_id(raw)
        guard = cls(
            development_manifest,
            walk_forward_manifest,
            window=window_id,
            input_contract=input_contract,
        )
        _validate_snapshot_identity(guard, raw, input_contract)
        _restore_snapshot_cursor(guard, raw.get("cursor"))
        return guard

    def _locate_quote(self, quote: HistoricalQuote) -> tuple[Literal["development", "walk-forward"], int, int]:
        key = _locator_identity(quote.locator)
        for role, identities in (
            ("development", self._development_identities),
            ("walk-forward", self._walk_forward_identities),
        ):
            position = identities.get(key)
            if position is not None:
                if quote.provider != HISTDATA_PROVIDER or quote.instrument != HISTDATA_INSTRUMENT:
                    raise WalkForwardIdentityError("quote provider/instrument mismatch")
                if quote.source_event_id != _source_event_id(quote.locator):
                    raise WalkForwardIdentityError("quote source_event_id is not bound to locator")
                return cast(Literal["development", "walk-forward"], role), position[0], position[1]
        raise WalkForwardIdentityError("quote locator is not listed in either manifest")


@dataclass(frozen=True, slots=True)
class WalkForwardReceipt:
    """Minimum secret-free receipt; no flag may imply promotion/trading."""

    status: str
    window_id: str
    development_manifest_hash: str
    walk_forward_manifest_hash: str
    input_hash: str
    attempt_id: str | None = None
    holdout: str = "CLOSED"
    selection_performed: bool = False
    promotion_performed: bool = False
    trading_enabled: bool = False
    auto_promote: bool = False
    network_performed: bool = False
    data_acquisition: bool = False
    costs_status: str = "UNKNOWN_NOT_ZERO"
    conclusion: str = "NOT_ASSESSED"
    error: str | None = None
    schema: str = WALK_FORWARD_RECEIPT_SCHEMA
    schema_version: int = WALK_FORWARD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_receipt_header(self)
        _validate_receipt_flags(self)
        _validate_receipt_payload(self)
        object.__setattr__(self, "development_manifest_hash", self.development_manifest_hash.lower())
        object.__setattr__(self, "walk_forward_manifest_hash", self.walk_forward_manifest_hash.lower())
        object.__setattr__(self, "input_hash", self.input_hash.lower())

    @classmethod
    def minimum(
        cls,
        input_contract: WalkForwardInput,
        *,
        window: WalkForwardWindow | str = "WF_2020",
        status: str = "VALIDATED",
        attempt_id: str | None = None,
        error: str | None = None,
    ) -> WalkForwardReceipt:
        selected = _coerce_window(window)
        return cls(
            schema=WALK_FORWARD_RECEIPT_SCHEMA,
            schema_version=WALK_FORWARD_SCHEMA_VERSION,
            status=status,
            window_id=selected.window_id,
            development_manifest_hash=input_contract.development_manifest_hash,
            walk_forward_manifest_hash=input_contract.walk_forward_manifest_hash,
            input_hash=input_contract.identity_hash,
            attempt_id=attempt_id,
            error=error,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "schema_version": self.schema_version,
            "receipt_type": "walk-forward",
            "stage": "walk-forward",
            "status": self.status,
            "attempt_id": self.attempt_id,
            "window_id": self.window_id,
            "development_manifest_hash": self.development_manifest_hash,
            "walk_forward_manifest_hash": self.walk_forward_manifest_hash,
            "input_hash": self.input_hash,
            "holdout": self.holdout,
            "selection_performed": self.selection_performed,
            "promotion_performed": self.promotion_performed,
            "trading_enabled": self.trading_enabled,
            "auto_promote": self.auto_promote,
            "network_performed": self.network_performed,
            "data_acquisition": self.data_acquisition,
            "costs_status": self.costs_status,
            "conclusion": self.conclusion,
            "error": self.error,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> WalkForwardReceipt:
        _strict_keys(value, _RECEIPT_KEYS, "walk-forward receipt")
        if value.get("schema") != WALK_FORWARD_RECEIPT_SCHEMA or value.get("receipt_type") != "walk-forward":
            raise WalkForwardError("walk-forward receipt schema/type is invalid")
        if value.get("stage") != "walk-forward":
            raise WalkForwardError("walk-forward receipt stage is invalid")
        return cls(
            schema=str(value.get("schema")),
            schema_version=_strict_positive_int(value.get("schema_version"), "schema_version"),
            status=_required_text(value.get("status"), "status"),
            attempt_id=(str(value["attempt_id"]) if value.get("attempt_id") is not None else None),
            window_id=_required_text(value.get("window_id"), "window_id"),
            development_manifest_hash=_required_text(
                value.get("development_manifest_hash"), "development_manifest_hash"
            ),
            walk_forward_manifest_hash=_required_text(
                value.get("walk_forward_manifest_hash"), "walk_forward_manifest_hash"
            ),
            input_hash=_required_text(value.get("input_hash"), "input_hash"),
            holdout=_required_text(value.get("holdout"), "holdout"),
            selection_performed=_strict_false(value.get("selection_performed"), "selection_performed"),
            promotion_performed=_strict_false(value.get("promotion_performed"), "promotion_performed"),
            trading_enabled=_strict_false(value.get("trading_enabled"), "trading_enabled"),
            auto_promote=_strict_false(value.get("auto_promote"), "auto_promote"),
            network_performed=_strict_false(value.get("network_performed"), "network_performed"),
            data_acquisition=_strict_false(value.get("data_acquisition"), "data_acquisition"),
            costs_status=_required_text(value.get("costs_status"), "costs_status"),
            conclusion=_required_text(value.get("conclusion"), "conclusion"),
            error=(str(value["error"]) if value.get("error") is not None else None),
        )


def make_walk_forward_receipt(
    input_contract: WalkForwardInput,
    *,
    window: WalkForwardWindow | str = "WF_2020",
    status: str = "VALIDATED",
    attempt_id: str | None = None,
    error: str | None = None,
) -> WalkForwardReceipt:
    """Construct the minimum review-only receipt without side effects."""

    return WalkForwardReceipt.minimum(
        input_contract,
        window=window,
        status=status,
        attempt_id=attempt_id,
        error=error,
    )


def validate_walk_forward_receipt(
    value: WalkForwardReceipt | Mapping[str, Any],
    *,
    input_contract: WalkForwardInput | None = None,
) -> WalkForwardReceipt:
    """Parse and validate a receipt, optionally against its composite input."""

    receipt = value if isinstance(value, WalkForwardReceipt) else WalkForwardReceipt.from_mapping(value)
    if input_contract is not None:
        if receipt.input_hash != input_contract.identity_hash:
            raise WalkForwardIdentityError("receipt input_hash mismatch")
        if receipt.development_manifest_hash != input_contract.development_manifest_hash:
            raise WalkForwardIdentityError("receipt development manifest hash mismatch")
        if receipt.walk_forward_manifest_hash != input_contract.walk_forward_manifest_hash:
            raise WalkForwardIdentityError("receipt walk-forward manifest hash mismatch")
    return receipt


def walk_forward_content_hash(partitions: Sequence[WalkForwardPartition]) -> str:
    """Hash raw source identity, including path, member, size and month."""

    material = "\n".join(
        f"{item.month}\0{item.raw_archive}\0{item.raw_sha256.lower()}\0{item.raw_size}\0{','.join(item.members)}"
        for item in partitions
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def walk_forward_manifest_from_histdata_archives(
    archives: Sequence[str | Path],
    *,
    data_root: str | Path,
    acquired_at: datetime | Sequence[datetime],
    source_uri: str | Sequence[str],
    terms_uri: str | Sequence[str],
    months: Sequence[str] | None = None,
    etags: str | Sequence[str | None] | None = None,
    last_modified: str | Sequence[str | None] | None = None,
) -> WalkForwardManifest:
    """Build and validate the complete 48-month WF manifest from local ZIPs.

    This function never downloads or writes any input.  A partial list,
    missing month, holdout month, or malformed raw archive is rejected before
    a manifest is returned.
    """

    if isinstance(archives, (str, Path)):
        raise WalkForwardError("archives must be a non-empty sequence of monthly paths")
    archive_list = tuple(Path(item).expanduser() for item in archives)
    if len(archive_list) != len(WALK_FORWARD_MONTHS):
        raise WalkForwardError("WalkForwardManifest requires exactly 48 monthly archives")
    root = Path(data_root).expanduser()
    if not root.is_absolute():
        raise WalkForwardError("data_root must be absolute")
    _reject_symlink_ancestors(root)
    inferred = tuple(_infer_histdata_month(path) for path in archive_list)
    selected = tuple(_month(item) for item in months) if months is not None else inferred
    if selected != inferred:
        raise WalkForwardError("explicit months do not match HistData archive members")
    if selected != WALK_FORWARD_MONTHS:
        raise WalkForwardError("WalkForwardManifest requires the contiguous months 202001..202312")
    acquired_values = _expand_metadata(acquired_at, len(archive_list), "acquired_at")
    source_values = _expand_metadata(source_uri, len(archive_list), "source_uri")
    terms_values = _expand_metadata(terms_uri, len(archive_list), "terms_uri")
    etag_values = _expand_metadata(etags, len(archive_list), "etag", allow_none=True)
    modified_values = _expand_metadata(last_modified, len(archive_list), "last_modified", allow_none=True)
    partitions = tuple(
        _walk_forward_partition_from_archive(
            path,
            month=selected[index],
            data_root=root,
            acquired_at=acquired_values[index],
            source_uri=source_values[index],
            terms_uri=terms_values[index],
            etag=etag_values[index],
            last_modified=modified_values[index],
        )
        for index, path in enumerate(archive_list)
    )
    content_hash = walk_forward_content_hash(partitions)
    manifest = WalkForwardManifest(
        walk_forward_id=_walk_forward_id(partitions, content_hash),
        provider=WALK_FORWARD_PROVIDER,
        instrument=WALK_FORWARD_INSTRUMENT,
        normalization_version=WALK_FORWARD_NORMALIZATION_VERSION,
        data_root=str(root),
        coverage_start=None,
        coverage_end=None,
        content_hash=content_hash,
        partitions=partitions,
    )
    validation = validate_walk_forward_manifest(manifest)
    if not validation.ok:
        raise WalkForwardError("raw archives failed walk-forward validation: " + "; ".join(validation.issues))
    details = validation.partition_counts
    if len(details) != len(partitions):
        raise WalkForwardError("walk-forward validation did not return partition coverage")
    covered = tuple(
        replace(
            partition,
            coverage_start=details[index][1],
            coverage_end=details[index][2],
            quote_count=details[index][0],
        )
        for index, partition in enumerate(partitions)
    )
    # Recompute only metadata; raw identity and content_hash stay unchanged.
    return replace(
        manifest,
        partitions=covered,
        coverage_start=validation.coverage_start,
        coverage_end=validation.coverage_end,
    )


def manifest_from_histdata_archives(*args: Any, **kwargs: Any) -> WalkForwardManifest:
    """Explicit alias for callers that import this module as a provider."""

    return walk_forward_manifest_from_histdata_archives(*args, **kwargs)


def read_walk_forward_manifest(path: str | Path) -> WalkForwardManifest:
    return WalkForwardManifest.from_path(path)


def iter_walk_forward_quotes(
    manifest: WalkForwardManifest | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterable[HistoricalQuote]:
    """Stream raw quotes in source order over the WF half-open interval."""

    resolved = _coerce_walk_forward_manifest(manifest)
    start_utc = _utc(start, "start") if start is not None else None
    end_utc = _utc(end, "end") if end is not None else None
    if start_utc is not None and start_utc < WALK_FORWARD_START:
        raise WalkForwardError("WF start precedes 2020-01-01")
    if end_utc is not None and end_utc > WALK_FORWARD_END:
        raise WalkForwardError("WF end reaches the closed holdout")
    if start_utc is not None and end_utc is not None and end_utc <= start_utc:
        raise WalkForwardError("end must be after start")
    _verify_walk_forward_partitions(resolved)
    previous: datetime | None = None
    sequence = 0
    for partition in resolved.partitions:
        for quote in _iter_walk_forward_partition_quotes(resolved, partition, sequence_start=sequence):
            if previous is not None and quote.event_time < previous:
                raise WalkForwardError("source order violation")
            previous = quote.event_time
            sequence = quote.sequence + 1
            if (start_utc is None or quote.event_time >= start_utc) and (end_utc is None or quote.event_time < end_utc):
                yield quote
    if sequence == 0:
        raise WalkForwardError("walk-forward manifest contains no historical quotes")


@dataclass(frozen=True, slots=True)
class _WalkForwardScan:
    total: int
    selected: int
    first: datetime | None
    last: datetime | None
    selected_first: datetime | None
    selected_last: datetime | None
    partition_counts: tuple[tuple[int, datetime | None, datetime | None], ...]
    issues: tuple[str, ...]


def _requested_bounds(
    start: datetime | None,
    end: datetime | None,
) -> tuple[datetime | None, datetime | None]:
    start_utc = _utc(start, "start") if start is not None else None
    end_utc = _utc(end, "end") if end is not None else None
    if start_utc is not None and start_utc < WALK_FORWARD_START:
        raise WalkForwardError("requested WF range starts before 2020-01-01")
    if end_utc is not None and end_utc > WALK_FORWARD_END:
        raise WalkForwardError("requested WF range reaches the closed holdout")
    if start_utc is not None and end_utc is not None and end_utc <= start_utc:
        raise WalkForwardError("end must be after start")
    return start_utc, end_utc


def _scan_walk_forward_manifest(
    manifest: WalkForwardManifest,
    *,
    start: datetime | None,
    end: datetime | None,
) -> _WalkForwardScan:
    total = 0
    selected = 0
    first: datetime | None = None
    last: datetime | None = None
    selected_first: datetime | None = None
    selected_last: datetime | None = None
    partition_counts: list[tuple[int, datetime | None, datetime | None]] = []
    issues: list[str] = []
    previous: datetime | None = None
    sequence = 0
    for partition in manifest.partitions:
        (
            count,
            part_first,
            part_last,
            previous,
            sequence,
            selected_count,
            range_first,
            range_last,
            partition_issues,
        ) = _scan_wf_partition(
            manifest,
            partition,
            sequence=sequence,
            previous=previous,
            start=start,
            end=end,
        )
        total += count
        selected += selected_count
        first = first or part_first
        last = part_last or last
        selected_first = selected_first or range_first
        selected_last = range_last or selected_last
        partition_counts.append((count, part_first, part_last))
        issues.extend(partition_issues)
    if total == 0:
        issues.append("walk-forward manifest contains no historical quotes")
    return _WalkForwardScan(
        total,
        selected,
        first,
        last,
        selected_first,
        selected_last,
        tuple(partition_counts),
        tuple(issues),
    )


def _scan_wf_partition(
    manifest: WalkForwardManifest,
    partition: WalkForwardPartition,
    *,
    sequence: int,
    previous: datetime | None,
    start: datetime | None,
    end: datetime | None,
) -> tuple[
    int,
    datetime | None,
    datetime | None,
    datetime | None,
    int,
    int,
    datetime | None,
    datetime | None,
    tuple[str, ...],
]:
    count = 0
    first: datetime | None = None
    last: datetime | None = None
    selected = 0
    selected_first: datetime | None = None
    selected_last: datetime | None = None
    issues: list[str] = []
    for quote in _iter_walk_forward_partition_quotes(manifest, partition, sequence_start=sequence):
        if previous is not None and quote.event_time < previous:
            issues.append(f"source order violation at sequence {quote.sequence}")
        previous = quote.event_time
        sequence = quote.sequence + 1
        count += 1
        first = first or quote.event_time
        last = quote.event_time
        if (start is None or quote.event_time >= start) and (end is None or quote.event_time < end):
            selected += 1
            selected_first = selected_first or quote.event_time
            selected_last = quote.event_time
    if count == 0:
        issues.append(f"partition contains no historical quotes: {partition.partition_id}")
    return count, first, last, previous, sequence, selected, selected_first, selected_last, tuple(issues)


def _walk_forward_validation_issues(
    manifest: WalkForwardManifest,
    scan: _WalkForwardScan,
    *,
    start: datetime | None,
    end: datetime | None,
) -> list[str]:
    issues = list(scan.issues)
    if manifest.coverage_start is not None and manifest.coverage_start != scan.first:
        issues.append("manifest coverage_start mismatch")
    if manifest.coverage_end is not None and manifest.coverage_end != scan.last:
        issues.append("manifest coverage_end mismatch")
    issues.extend(_partition_validation_issues(manifest, scan.partition_counts))
    limited = start is not None or end is not None
    if limited and scan.selected == 0:
        issues.append("requested coverage is empty")
    if limited and start is not None and manifest.coverage_start is not None and start < manifest.coverage_start:
        issues.append("requested range starts before dataset coverage")
    if limited and end is not None and manifest.coverage_end is not None and end > manifest.coverage_end:
        issues.append("requested range ends after dataset coverage")
    if walk_forward_content_hash(manifest.partitions) != manifest.content_hash:
        issues.append("manifest content_hash mismatch")
    return issues


def _partition_validation_issues(
    manifest: WalkForwardManifest,
    counts: Sequence[tuple[int, datetime | None, datetime | None]],
) -> list[str]:
    issues: list[str] = []
    for index, (count, part_first, part_last) in enumerate(counts):
        partition = manifest.partitions[index]
        if partition.quote_count is not None and partition.quote_count != count:
            issues.append(f"partition quote_count mismatch: {partition.partition_id}")
        if partition.coverage_start is not None and partition.coverage_start != part_first:
            issues.append(f"partition coverage_start mismatch: {partition.partition_id}")
        if partition.coverage_end is not None and partition.coverage_end != part_last:
            issues.append(f"partition coverage_end mismatch: {partition.partition_id}")
    return issues


def validate_walk_forward_manifest(
    manifest: WalkForwardManifest | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> WalkForwardValidation:
    """Verify bytes, members, row order, month identity and coverage."""

    try:
        resolved = _coerce_walk_forward_manifest(manifest)
        start_utc, end_utc = _requested_bounds(start, end)
        _verify_walk_forward_partitions(resolved)
        scan = _scan_walk_forward_manifest(resolved, start=start_utc, end=end_utc)
        issues = _walk_forward_validation_issues(resolved, scan, start=start_utc, end=end_utc)
        limited = start_utc is not None or end_utc is not None
        return WalkForwardValidation(
            not issues,
            scan.selected if limited else scan.total,
            scan.selected_first if limited else scan.first,
            scan.selected_last if limited else scan.last,
            resolved.content_hash,
            tuple(issues),
            len(issues),
            limited,
            start_utc,
            end_utc,
            scan.partition_counts,
        )
    except (WalkForwardError, HistoricalDataError, OSError, TypeError, ValueError, zipfile.BadZipFile) as exc:
        return WalkForwardValidation(False, 0, None, None, None, (str(exc),), 1)


def classify_walk_forward_phase(
    event_time: datetime,
    window: WalkForwardWindow | str = "WF_2020",
) -> WalkForwardPhase | Literal["OUTSIDE", "HOLDOUT_REJECTED"]:
    return WalkForwardBoundary(_coerce_window(window)).classify(event_time)


def _month(value: Any) -> str:
    if not isinstance(value, str):
        raise WalkForwardError("month must be YYYYMM text")
    text = value.strip().replace("-", "")
    if len(text) != 6 or not text.isdigit() or not 1 <= int(text[4:]) <= 12:
        raise WalkForwardError("month must be valid YYYYMM text")
    return text


def _next_month(value: str) -> str:
    month = _month(value)
    year = int(month[:4])
    number = int(month[4:])
    if number == 12:
        return f"{year + 1:04d}01"
    return f"{year:04d}{number + 1:02d}"


def _month_bounds(month: str) -> tuple[datetime, datetime]:
    value = _month(month)
    local_start = datetime(int(value[:4]), int(value[4:]), 1, tzinfo=HISTDATA_SOURCE_OFFSET)
    next_value = _next_month(value)
    local_end = datetime(int(next_value[:4]), int(next_value[4:]), 1, tzinfo=HISTDATA_SOURCE_OFFSET)
    return local_start.astimezone(UTC), local_end.astimezone(UTC)


def _validate_partition_identity(partition: WalkForwardPartition, month: str) -> None:
    if not isinstance(partition.partition_id, str) or not partition.partition_id.strip():
        raise WalkForwardError("partition_id must be non-empty")
    if partition.partition_id != month:
        raise WalkForwardError("partition_id must equal the canonical YYYYMM month")


def _validate_partition_storage(partition: WalkForwardPartition) -> None:
    _relative_path(partition.raw_archive, "raw archive")
    if not _is_sha256(partition.raw_sha256):
        raise WalkForwardError("partition raw_sha256 must be a hexadecimal SHA-256")
    _strict_positive_int(partition.raw_size, "raw_size")
    if partition.raw_size > MAX_DATASET_BYTES:
        raise WalkForwardError("partition raw_size exceeds the dataset limit")


def _validate_partition_members(partition: WalkForwardPartition) -> None:
    if not partition.members or len(set(partition.members)) != len(partition.members):
        raise WalkForwardError("partition must list unique CSV members")
    for member in partition.members:
        _relative_path(member, "archive member")


def _validate_partition_month(partition: WalkForwardPartition, month: str) -> None:
    if WALK_FORWARD_START_MONTH <= month <= WALK_FORWARD_END_MONTH:
        return
    if month >= WALK_FORWARD_HOLDOUT_START_MONTH:
        raise WalkForwardError("holdout months are not allowed in WalkForwardManifest")
    raise WalkForwardError("walk-forward month is outside the approved 2020--2023 window")


def _validate_partition_metadata(partition: WalkForwardPartition) -> None:
    for value, name in ((partition.source_uri, "source_uri"), (partition.terms_uri, "terms_uri")):
        if not isinstance(value, str) or not value.strip():
            raise WalkForwardError(f"{name} must be non-empty text")
    for metadata_value, name in ((partition.etag, "etag"), (partition.last_modified, "last_modified")):
        if metadata_value is not None and (not isinstance(metadata_value, str) or not metadata_value.strip()):
            raise WalkForwardError(f"{name} must be non-empty text when present")


def _normalize_partition_coverage(partition: WalkForwardPartition) -> None:
    start = partition.coverage_start
    end = partition.coverage_end
    if start is not None:
        start = _utc(start, "coverage_start")
        object.__setattr__(partition, "coverage_start", start)
    if end is not None:
        end = _utc(end, "coverage_end")
        object.__setattr__(partition, "coverage_end", end)
    if start is not None and end is not None and end < start:
        raise WalkForwardError("coverage_end must not precede coverage_start")
    lower, upper = _month_bounds(partition.month)
    for value, name in ((start, "coverage_start"), (end, "coverage_end")):
        if value is not None and not lower <= value < upper:
            raise WalkForwardError(f"{name} lies outside partition month {partition.month}")


def _normalize_manifest_coverage(manifest: WalkForwardManifest) -> None:
    start = manifest.coverage_start
    end = manifest.coverage_end
    if start is not None:
        start = _utc(start, "coverage_start")
        object.__setattr__(manifest, "coverage_start", start)
    if end is not None:
        end = _utc(end, "coverage_end")
        object.__setattr__(manifest, "coverage_end", end)
    if start is not None and end is not None and end < start:
        raise WalkForwardError("coverage_end must not precede coverage_start")
    if start is not None and start < WALK_FORWARD_START:
        raise WalkForwardError("manifest coverage starts before WF 2020")
    if end is not None and end >= WALK_FORWARD_END:
        raise WalkForwardError("manifest coverage reaches the closed holdout")
    if (
        manifest.partitions
        and start is not None
        and manifest.partitions[0].coverage_start is not None
        and start != manifest.partitions[0].coverage_start
    ):
        raise WalkForwardError("manifest coverage_start does not match first partition")
    if (
        manifest.partitions
        and end is not None
        and manifest.partitions[-1].coverage_end is not None
        and end != manifest.partitions[-1].coverage_end
    ):
        raise WalkForwardError("manifest coverage_end does not match last partition")


def _validate_walk_forward_partitions(partitions: Sequence[WalkForwardPartition]) -> None:
    if len(partitions) != len(WALK_FORWARD_MONTHS):
        raise WalkForwardError("WalkForwardManifest requires exactly 48 partitions")
    months = [item.month for item in partitions]
    if tuple(months) != WALK_FORWARD_MONTHS:
        raise WalkForwardError("walk-forward partitions must be exactly chronological 202001..202312")
    _validate_partition_collisions(partitions)
    _validate_partition_adjacency(partitions)


def _validate_partition_collisions(partitions: Sequence[WalkForwardPartition]) -> None:
    identities: set[tuple[str, str, str]] = set()
    paths: set[str] = set()
    hashes: set[str] = set()
    for item in partitions:
        if item.raw_archive in paths:
            raise WalkForwardError("manifest partition collision: duplicate raw path")
        if item.raw_sha256 in hashes:
            raise WalkForwardError("manifest partition collision: duplicate raw SHA-256")
        paths.add(item.raw_archive)
        hashes.add(item.raw_sha256)
        for member in item.members:
            key = (item.raw_archive, item.raw_sha256, member)
            if key in identities:
                raise WalkForwardError("manifest partition collision: duplicate source identity")
            identities.add(key)


def _validate_partition_adjacency(partitions: Sequence[WalkForwardPartition]) -> None:
    for previous, current in zip(partitions, partitions[1:], strict=False):
        if current.month != _next_month(previous.month):
            raise WalkForwardError(f"manifest partition gap between {previous.month} and {current.month}")
        if (
            previous.coverage_end is not None
            and current.coverage_start is not None
            and current.coverage_start <= previous.coverage_end
        ):
            raise WalkForwardError(f"manifest coverage collision between {previous.month} and {current.month}")


def _validate_walk_forward_root(data_root: str) -> None:
    if not isinstance(data_root, str) or not data_root.strip():
        raise WalkForwardError("data_root must be non-empty text")
    root = Path(data_root).expanduser()
    _reject_symlink_ancestors(root)
    if not root.is_absolute() or root.is_symlink():
        raise WalkForwardError("walk-forward data_root must be an absolute non-symlink path")


def _walk_forward_id(partitions: Sequence[WalkForwardPartition], content_hash: str) -> str:
    return f"{WALK_FORWARD_PROVIDER}:{WALK_FORWARD_INSTRUMENT}:{WALK_FORWARD_START_MONTH}-{WALK_FORWARD_END_MONTH}:{content_hash[:32]}"


def _manifest_content_hash(manifest: DatasetManifest) -> str:
    value = getattr(manifest, "content_hash", None)
    if not isinstance(value, str) or not _is_sha256(value):
        raise WalkForwardIdentityError("development manifest content_hash is invalid")
    return value.lower()


def _coerce_development_manifest(value: DatasetManifest | str | Path) -> DatasetManifest:
    candidate: DatasetManifest | Any = value
    if isinstance(value, (str, Path)):
        candidate = read_manifest(value)
    if not isinstance(candidate, DatasetManifest):
        raise WalkForwardIdentityError("development manifest must be DatasetManifest")
    return candidate


def _coerce_walk_forward_manifest(value: WalkForwardManifest | str | Path) -> WalkForwardManifest:
    if isinstance(value, (str, Path)):
        value = read_walk_forward_manifest(value)
    if not isinstance(value, WalkForwardManifest):
        raise WalkForwardIdentityError("manifest must be WalkForwardManifest, not DatasetManifest/holdout")
    return value


def _coerce_window(value: WalkForwardWindow | str) -> WalkForwardWindow:
    if isinstance(value, WalkForwardWindow):
        return value
    if not isinstance(value, str) or value not in WALK_FORWARD_WINDOW_IDS:
        raise WalkForwardError("window must be one of WF_2020..WF_2023")
    return WALK_FORWARD_WINDOWS[WALK_FORWARD_WINDOW_IDS.index(value)]


def _source_identities(
    partitions: Sequence[DatasetPartition | WalkForwardPartition],
    *,
    role: Literal["development", "walk-forward"],
) -> dict[tuple[str, str, str], tuple[int, int]]:
    identities: dict[tuple[str, str, str], tuple[int, int]] = {}
    for partition_index, partition in enumerate(partitions):
        archive = partition.raw_archive
        raw_hash = partition.raw_sha256.lower()
        for member_index, member in enumerate(partition.members):
            key = (archive, raw_hash, member)
            if key in identities:
                raise WalkForwardIdentityError(f"{role} manifest repeats a source identity")
            identities[key] = (partition_index, member_index)
    return identities


def _require_development_start(
    phase: WalkForwardPhase,
    role: Literal["development", "walk-forward"],
    partition_index: int,
    member_index: int,
) -> None:
    if phase != "WARMUP_ONLY" or role != "development" or partition_index != 0 or member_index != 0:
        raise WalkForwardIdentityError("stream must start at development partition/member zero")


def _validate_phase_transition(
    window: WalkForwardWindow,
    cursor: WalkForwardCursor,
    phase: WalkForwardPhase,
    role: Literal["development", "walk-forward"],
    partition_index: int,
    member_index: int,
) -> None:
    if phase == "WARMUP_ONLY":
        if cursor.phase != "WARMUP_ONLY" or role != "development":
            raise WalkForwardError("WARMUP_ONLY cannot resume after WF evaluation")
        return
    if cursor.phase == "WARMUP_ONLY":
        expected_partition = (window.year - 2020) * 12
        if role != "walk-forward" or partition_index != expected_partition or member_index != 0:
            raise WalkForwardIdentityError("WF phase must start at the selected annual partition/member zero")
    elif role != "walk-forward":
        raise WalkForwardIdentityError("WF phase requires WalkForwardManifest source")


def _validate_same_manifest_position(
    cursor: WalkForwardCursor,
    role: Literal["development", "walk-forward"],
    partition_index: int,
    member_index: int,
) -> None:
    if role != cursor.manifest_role:
        return
    if partition_index > cursor.partition_index + 1:
        raise WalkForwardIdentityError("walk-forward source skipped a partition")
    if partition_index == cursor.partition_index and member_index > cursor.member_index + 1:
        raise WalkForwardIdentityError("walk-forward source skipped a member")
    if partition_index > cursor.partition_index and member_index != 0:
        raise WalkForwardIdentityError("partition transition must start at member zero")


def _locator_identity(locator: SourceLocator) -> tuple[str, str, str]:
    return (locator.archive, locator.sha256.lower(), locator.member)


def _source_event_id(locator: SourceLocator) -> str:
    return f"{locator.sha256}:{locator.member}:{locator.row}:{locator.offset}"


def _required_mapping_text(value: Mapping[str, Any], key: str) -> str:
    return _required_text(value.get(key), key)


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WalkForwardError(f"{name} must be non-empty text")
    return value.strip()


def _optional_mapping_text(value: Mapping[str, Any], key: str) -> str | None:
    raw = value.get(key)
    if raw is None:
        return None
    return _required_text(raw, key)


def _strict_false(value: Any, name: str) -> bool:
    if value is not False:
        raise WalkForwardError(f"{name} must be false")
    return False


def _validate_receipt_header(receipt: WalkForwardReceipt) -> None:
    if receipt.schema != WALK_FORWARD_RECEIPT_SCHEMA or receipt.schema_version != WALK_FORWARD_SCHEMA_VERSION:
        raise WalkForwardError("walk-forward receipt schema/version mismatch")
    if receipt.status not in {"VALIDATED", "RUNNING", "COMPLETED", "FAILED", "INSUFFICIENT"}:
        raise WalkForwardError("walk-forward receipt status is invalid")
    if receipt.window_id not in WALK_FORWARD_WINDOW_IDS:
        raise WalkForwardError("walk-forward receipt window_id is invalid")
    for value, name in (
        (receipt.development_manifest_hash, "development_manifest_hash"),
        (receipt.walk_forward_manifest_hash, "walk_forward_manifest_hash"),
        (receipt.input_hash, "input_hash"),
    ):
        if not _is_sha256(value):
            raise WalkForwardError(f"{name} must be a hexadecimal SHA-256")
    if receipt.holdout != "CLOSED":
        raise WalkForwardError("walk-forward receipt holdout must be CLOSED")


def _validate_receipt_flags(receipt: WalkForwardReceipt) -> None:
    for value, name in (
        (receipt.selection_performed, "selection_performed"),
        (receipt.promotion_performed, "promotion_performed"),
        (receipt.trading_enabled, "trading_enabled"),
        (receipt.auto_promote, "auto_promote"),
        (receipt.network_performed, "network_performed"),
        (receipt.data_acquisition, "data_acquisition"),
    ):
        if value is not False:
            raise WalkForwardError(f"{name} must be false in a review-only receipt")


def _validate_receipt_payload(receipt: WalkForwardReceipt) -> None:
    if receipt.costs_status not in {"UNKNOWN_NOT_ZERO", "ASSESSED"}:
        raise WalkForwardError("costs_status is invalid")
    if not isinstance(receipt.conclusion, str) or not receipt.conclusion.strip():
        raise WalkForwardError("conclusion must be non-empty text")
    if receipt.error is not None and (not isinstance(receipt.error, str) or not receipt.error.strip()):
        raise WalkForwardError("error must be non-empty text when present")
    if receipt.status == "FAILED" and receipt.error is None:
        raise WalkForwardError("FAILED receipt requires error")
    if receipt.attempt_id is not None and (not isinstance(receipt.attempt_id, str) or not receipt.attempt_id.strip()):
        raise WalkForwardError("attempt_id must be non-empty text when present")


def _expand_metadata(value: Any, count: int, name: str, *, allow_none: bool = False) -> tuple[Any, ...]:
    if value is None:
        if allow_none:
            return (None,) * count
        raise WalkForwardError(f"{name} is required")
    if isinstance(value, (str, bytes, bytearray, datetime, Path)):
        return (value,) * count
    if isinstance(value, Sequence):
        expanded = tuple(value)
        if len(expanded) != count:
            raise WalkForwardError(f"{name} length must match archives")
        return expanded
    raise WalkForwardError(f"{name} must be a scalar or sequence")


def _walk_forward_partition_from_archive(
    archive_path: Path,
    *,
    month: str,
    data_root: Path,
    acquired_at: Any,
    source_uri: Any,
    terms_uri: Any,
    etag: Any,
    last_modified: Any,
) -> WalkForwardPartition:
    try:
        raw_archive = archive_path.relative_to(data_root).as_posix()
    except ValueError as exc:
        raise WalkForwardError("raw archive must be under data_root") from exc
    _reject_symlink_ancestors(archive_path)
    if archive_path.is_symlink() or not archive_path.is_file():
        raise WalkForwardError("raw archive is not a regular file")
    raw_size = archive_path.stat().st_size
    if raw_size < 1 or raw_size > MAX_DATASET_BYTES:
        raise WalkForwardError("raw archive exceeds the dataset limit")
    return WalkForwardPartition(
        partition_id=month,
        month=month,
        raw_archive=raw_archive,
        raw_sha256=_hash_file(archive_path),
        raw_size=raw_size,
        members=tuple(_histdata_members(archive_path, month=month)),
        source_uri=_required_text(source_uri, "source_uri"),
        terms_uri=_required_text(terms_uri, "terms_uri"),
        acquired_at=acquired_at if isinstance(acquired_at, datetime) else _parse_iso(acquired_at, "acquired_at"),
        etag=_optional_value_text(etag, "etag"),
        last_modified=_optional_value_text(last_modified, "last_modified"),
    )


def _optional_value_text(value: Any, name: str) -> str | None:
    return None if value is None else _required_text(value, name)


def _verify_walk_forward_partitions(manifest: WalkForwardManifest) -> None:
    for partition in manifest.partitions:
        path = _regular_walk_forward_archive(manifest.data_root, partition.raw_archive)
        if path.stat().st_size != partition.raw_size:
            raise WalkForwardError(f"raw archive size mismatch: {partition.partition_id}")
        if _hash_file(path) != partition.raw_sha256:
            raise WalkForwardError(f"raw archive SHA-256 mismatch: {partition.partition_id}")
        with zipfile.ZipFile(path, "r") as archive:
            infos = archive.infolist()
            names = [info.filename for info in infos]
            if len(names) != len(set(names)):
                raise WalkForwardError("ZIP contains duplicate member names")
            for info in infos:
                _validate_zip_name(info)
            actual = set(_histdata_members_from_infos(infos, month=partition.month))
            if actual != set(partition.members):
                raise WalkForwardError(f"partition CSV members mismatch: {partition.partition_id}")


def _regular_walk_forward_archive(data_root: str, raw_archive: str) -> Path:
    root = Path(data_root).expanduser()
    _reject_symlink_ancestors(root)
    if root.is_symlink() or not root.is_dir():
        raise WalkForwardError("walk-forward data_root is not a regular directory")
    target = root / _relative_path(raw_archive, "raw archive")
    _reject_symlink_ancestors(target)
    if target.is_symlink() or not target.is_file():
        raise WalkForwardError("raw archive is not a regular file")
    return target


def _iter_walk_forward_partition_quotes(
    manifest: WalkForwardManifest,
    partition: WalkForwardPartition,
    *,
    sequence_start: int,
) -> Iterable[HistoricalQuote]:
    path = _regular_walk_forward_archive(manifest.data_root, partition.raw_archive)
    with zipfile.ZipFile(path, "r") as archive:
        for member in partition.members:
            info = archive.getinfo(member)
            if info.is_dir() or _zipinfo_is_symlink(info):
                raise WalkForwardError(f"archive member is not a regular file: {member}")
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
                        raise WalkForwardError(f"row outside partition month at {member}:{row}")
                    if not WALK_FORWARD_START <= quote.event_time < WALK_FORWARD_END:
                        raise WalkForwardError("walk-forward raw contains 2024+ or pre-2020 data")
                    yield quote


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _snapshot_mapping(value: Mapping[str, Any] | str) -> Mapping[str, Any]:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise WalkForwardIdentityError("walk-forward snapshot is not valid JSON") from exc
    else:
        decoded = value
    if not isinstance(decoded, Mapping):
        raise WalkForwardIdentityError("walk-forward snapshot must be an object")
    return decoded


def _snapshot_window_id(value: Mapping[str, Any]) -> str:
    allowed = {
        "schema",
        "schema_version",
        "development_manifest_hash",
        "walk_forward_manifest_hash",
        "window_id",
        "input_hash",
        "holdout",
        "cursor",
    }
    unknown = set(value) - allowed
    if unknown:
        raise WalkForwardIdentityError(f"walk-forward snapshot has unknown keys: {sorted(unknown)!r}")
    if value.get("schema") != "mtf-lab.walk-forward-stream-guard.v1" or value.get("schema_version") != 1:
        raise WalkForwardIdentityError("unsupported walk-forward snapshot schema")
    if value.get("holdout") != "CLOSED":
        raise WalkForwardError("walk-forward snapshot cannot open holdout")
    window_id = value.get("window_id")
    if not isinstance(window_id, str):
        raise WalkForwardIdentityError("snapshot window_id is required")
    return window_id


def _validate_snapshot_identity(
    guard: WalkForwardStreamGuard,
    value: Mapping[str, Any],
    input_contract: WalkForwardInput | None,
) -> None:
    if value.get("development_manifest_hash") != guard._manifest_hashes["development"]:
        raise WalkForwardIdentityError("snapshot development manifest hash mismatch")
    if value.get("walk_forward_manifest_hash") != guard._manifest_hashes["walk-forward"]:
        raise WalkForwardIdentityError("snapshot walk-forward manifest hash mismatch")
    expected_input = input_contract.identity_hash if input_contract is not None else None
    if value.get("input_hash") != expected_input:
        raise WalkForwardIdentityError("snapshot input_hash mismatch")


def _restore_snapshot_cursor(guard: WalkForwardStreamGuard, value: Any) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping):
        raise WalkForwardIdentityError("snapshot cursor must be an object")
    cursor = _cursor_from_mapping(value)
    role_map = (
        guard._development_identities if cursor.manifest_role == "development" else guard._walk_forward_identities
    )
    locator_key = _locator_identity(cursor.locator)
    identity = role_map.get(locator_key)
    if identity is None or identity[:2] != (cursor.partition_index, cursor.member_index):
        raise WalkForwardIdentityError("snapshot cursor is not in its declared manifest position")
    if cursor.phase == "WARMUP_ONLY" and cursor.event_time >= guard.window.test_start:
        raise WalkForwardError("snapshot warmup cursor crosses WF boundary")
    if cursor.phase == "WF_EVALUATION" and not guard.window.test_start <= cursor.event_time < guard.window.test_end:
        raise WalkForwardError("snapshot WF cursor lies outside selected window")
    guard._cursor = cursor
    guard._last_event_time = cursor.event_time
    guard._last_rank = (
        0 if cursor.phase == "WARMUP_ONLY" else 1,
        cursor.partition_index,
        cursor.member_index,
        cursor.locator.offset,
        cursor.locator.row,
    )


def _cursor_from_mapping(value: Mapping[str, Any]) -> WalkForwardCursor:
    allowed = {
        "phase",
        "manifest_role",
        "partition_index",
        "member_index",
        "locator",
        "event_time",
        "source_event_id",
        "accepted_count",
    }
    unknown = set(value) - allowed
    if unknown:
        raise WalkForwardIdentityError(f"walk-forward cursor has unknown keys: {sorted(unknown)!r}")
    locator_raw = value.get("locator")
    if not isinstance(locator_raw, Mapping):
        raise WalkForwardIdentityError("walk-forward cursor locator must be an object")
    locator = SourceLocator.from_mapping(locator_raw)
    phase = value.get("phase")
    role = value.get("manifest_role")
    if phase not in ("WARMUP_ONLY", "WF_EVALUATION") or role not in ("development", "walk-forward"):
        raise WalkForwardIdentityError("walk-forward cursor phase/role is invalid")
    return WalkForwardCursor(
        phase=phase,
        manifest_role=role,
        partition_index=_strict_nonnegative_int(value.get("partition_index"), "partition_index"),
        member_index=_strict_nonnegative_int(value.get("member_index"), "member_index"),
        locator=locator,
        event_time=_parse_iso(value.get("event_time"), "event_time"),
        source_event_id=_required_text(value.get("source_event_id"), "source_event_id"),
        accepted_count=_strict_positive_int(value.get("accepted_count"), "accepted_count"),
    )


WALK_FORWARD_MONTHS = tuple(f"{year:04d}{month:02d}" for year in range(2020, 2024) for month in range(1, 13))


__all__ = [
    "DEVELOPMENT_START",
    "WALK_FORWARD_END",
    "WALK_FORWARD_HOLDOUT_END",
    "WALK_FORWARD_HOLDOUT_START_MONTH",
    "WALK_FORWARD_INPUT_SCHEMA",
    "WALK_FORWARD_INSTRUMENT",
    "WALK_FORWARD_MANIFEST_SCHEMA",
    "WALK_FORWARD_MONTHS",
    "WALK_FORWARD_NORMALIZATION_VERSION",
    "WALK_FORWARD_PROVIDER",
    "WALK_FORWARD_RECEIPT_SCHEMA",
    "WALK_FORWARD_SCHEMA_VERSION",
    "WALK_FORWARD_START",
    "WALK_FORWARD_START_MONTH",
    "WALK_FORWARD_WINDOW_IDS",
    "WALK_FORWARD_WINDOWS",
    "WARMUP_START",
    "WalkForwardBoundary",
    "WalkForwardCursor",
    "WalkForwardError",
    "WalkForwardIdentityError",
    "WalkForwardInput",
    "WalkForwardManifest",
    "WalkForwardPartition",
    "WalkForwardPhase",
    "WalkForwardReceipt",
    "WalkForwardStreamGuard",
    "WalkForwardValidation",
    "WalkForwardWindow",
    "canonical_walk_forward_windows",
    "classify_walk_forward_phase",
    "iter_walk_forward_quotes",
    "make_walk_forward_receipt",
    "manifest_from_histdata_archives",
    "read_walk_forward_manifest",
    "validate_walk_forward_manifest",
    "validate_walk_forward_receipt",
    "walk_forward_content_hash",
    "walk_forward_manifest_from_histdata_archives",
]
