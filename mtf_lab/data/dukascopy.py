"""Provider-neutral, lossless reader for the official Dukascopy tick export.

The public Historical Data Export currently obtains compact JSON tick blocks
from the JETTA service.  This module deliberately contains no network client:
``dukascopy_acquisition`` owns bounded HTTPS acquisition and this module owns
the immutable manifest/DTO/reader contract.  The response's native JSON bytes
remain the source of truth; ``bidVolumes`` and ``askVolumes`` are validated
only as payload shape and are never exposed as traded volume.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .historical import (
    DEFAULT_DATA_ROOT,
    HistoricalDataError,
    HistoricalQuote,
    SourceLocator,
    _iso,
    _parse_optional_iso,
    _utc,
)

DUKASCOPY_PROVIDER = "dukascopy"
DUKASCOPY_INSTRUMENT = "EUR/USD"
DUKASCOPY_SYMBOL = "EUR-USD"
DUKASCOPY_SCHEMA_VERSION = 1
DUKASCOPY_NORMALIZATION_VERSION = "dukascopy-jetta-ticks-v1"
DUKASCOPY_INTERVAL_CODE = "1T"
DUKASCOPY_SOURCE_TIMEZONE = "UTC_epoch_milliseconds"
DUKASCOPY_FORMAT = "jetta_compact_json_ticks_v1"
DUKASCOPY_SOURCE_URI = "https://www.dukascopy.com/swiss/english/marketwatch/historical/"
DUKASCOPY_WIDGET_URI = "https://widgets.dukascopy.com/en/historical-data-export"
DUKASCOPY_CONFIG_URI = "https://widgets.dukascopy.com/en/config.json"
DUKASCOPY_TERMS_URI = "https://www.dukascopy.com/swiss/english/legal-pages/terms-of-use/"
DUKASCOPY_TICKS_PATH_TEMPLATE = "/v1/ticks/EUR-USD/{year}/{month}/{day}/{hour}"
DUKASCOPY_WINDOW_START = datetime(2016, 3, 7, tzinfo=UTC)
DUKASCOPY_WINDOW_END = datetime(2016, 3, 14, tzinfo=UTC)
DUKASCOPY_MAX_BYTES = 200 * 1024**2
DUKASCOPY_DEFAULT_DATA_ROOT = DEFAULT_DATA_ROOT / DUKASCOPY_PROVIDER
DUKASCOPY_MAX_ISSUES = 128

_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "dataset_id",
        "provider",
        "instrument",
        "normalization_version",
        "format",
        "source_uri",
        "widget_uri",
        "config_uri",
        "terms_uri",
        "api_base",
        "source_timezone",
        "availability_basis",
        "data_root",
        "window_start",
        "window_end",
        "coverage_start",
        "coverage_end",
        "quote_count",
        "raw_bytes",
        "content_hash",
        "partitions",
    }
)
_PARTITION_KEYS = frozenset(
    {
        "partition_id",
        "raw_path",
        "raw_sha256",
        "raw_size",
        "source_uri",
        "instrument",
        "interval_code",
        "start",
        "end",
        "coverage_start",
        "coverage_end",
        "quote_count",
        "source_timezone",
        "precision",
        "format",
        "availability_basis",
    }
)


class DukascopyError(HistoricalDataError):
    """The Dukascopy compact tick contract or manifest is invalid."""


@dataclass(frozen=True, slots=True)
class DukascopyPartition:
    """One native JSON response covering one UTC hour of the pilot window."""

    partition_id: str
    raw_path: str
    raw_sha256: str
    raw_size: int
    source_uri: str
    instrument: str
    interval_code: str
    start: datetime
    end: datetime
    coverage_start: datetime | None
    coverage_end: datetime | None
    quote_count: int
    source_timezone: str = DUKASCOPY_SOURCE_TIMEZONE
    precision: str = "official_widget_multiplier"
    format: str = DUKASCOPY_FORMAT
    availability_basis: str = "historical_event_time"

    def __post_init__(self) -> None:
        _text(self.partition_id, "partition_id")
        _relative_path(self.raw_path, "raw_path")
        _sha256(self.raw_sha256, "raw_sha256")
        _positive_int(self.raw_size, "raw_size")
        if self.raw_size > DUKASCOPY_MAX_BYTES:
            raise DukascopyError("partition raw_size exceeds the 200 MiB pilot limit")
        if self.instrument != DUKASCOPY_INSTRUMENT:
            raise DukascopyError("Dukascopy partition instrument must be EUR/USD")
        if self.interval_code != DUKASCOPY_INTERVAL_CODE:
            raise DukascopyError("Dukascopy partition interval_code must be 1T")
        start = _utc(self.start, "partition start")
        end = _utc(self.end, "partition end")
        if end <= start:
            raise DukascopyError("partition end must be after start")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        _check_optional_coverage(self.coverage_start, self.coverage_end)
        if self.coverage_start is not None:
            object.__setattr__(self, "coverage_start", _utc(self.coverage_start, "coverage_start"))
        if self.coverage_end is not None:
            object.__setattr__(self, "coverage_end", _utc(self.coverage_end, "coverage_end"))
        if self.coverage_start is not None and not start <= self.coverage_start < end:
            raise DukascopyError("partition coverage_start lies outside its interval")
        if self.coverage_end is not None and not start <= self.coverage_end < end:
            raise DukascopyError("partition coverage_end lies outside its interval")
        _nonnegative_int(self.quote_count, "quote_count")
        _text(self.source_timezone, "source_timezone")
        _text(self.precision, "precision")
        _text(self.format, "format")
        if self.availability_basis != "historical_event_time":
            raise DukascopyError("Dukascopy availability_basis is fixed")

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "raw_path": self.raw_path,
            "raw_sha256": self.raw_sha256,
            "raw_size": self.raw_size,
            "source_uri": self.source_uri,
            "instrument": self.instrument,
            "interval_code": self.interval_code,
            "start": _iso(self.start),
            "end": _iso(self.end),
            "coverage_start": _iso(self.coverage_start),
            "coverage_end": _iso(self.coverage_end),
            "quote_count": self.quote_count,
            "source_timezone": self.source_timezone,
            "precision": self.precision,
            "format": self.format,
            "availability_basis": self.availability_basis,
        }

    @property
    def raw_archive(self) -> str:
        """Compatibility alias used by the HistData manifest seam."""

        return self.raw_path

    @property
    def sha256(self) -> str:
        return self.raw_sha256

    @property
    def size(self) -> int:
        return self.raw_size

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DukascopyPartition:
        _strict_keys(value, _PARTITION_KEYS, "Dukascopy partition")
        return cls(
            partition_id=_required_text(value, "partition_id"),
            raw_path=_required_text(value, "raw_path"),
            raw_sha256=_required_text(value, "raw_sha256"),
            raw_size=_positive_int(value.get("raw_size"), "raw_size"),
            source_uri=_required_text(value, "source_uri"),
            instrument=_required_text(value, "instrument"),
            interval_code=_required_text(value, "interval_code"),
            start=_required_time(value.get("start"), "start"),
            end=_required_time(value.get("end"), "end"),
            coverage_start=_parse_optional_iso(value.get("coverage_start"), "coverage_start"),
            coverage_end=_parse_optional_iso(value.get("coverage_end"), "coverage_end"),
            quote_count=_nonnegative_int(value.get("quote_count"), "quote_count"),
            source_timezone=_optional_text(value, "source_timezone", DUKASCOPY_SOURCE_TIMEZONE),
            precision=_optional_text(value, "precision", "official_widget_multiplier"),
            format=_optional_text(value, "format", DUKASCOPY_FORMAT),
            availability_basis=_optional_text(value, "availability_basis", "historical_event_time"),
        )


def _validate_manifest_header(manifest: DukascopyManifest) -> None:
    if manifest.schema_version != DUKASCOPY_SCHEMA_VERSION:
        raise DukascopyError("unsupported Dukascopy manifest schema_version")
    if manifest.provider != DUKASCOPY_PROVIDER or manifest.instrument != DUKASCOPY_INSTRUMENT:
        raise DukascopyError("Dukascopy manifest provider/instrument mismatch")
    if manifest.normalization_version != DUKASCOPY_NORMALIZATION_VERSION:
        raise DukascopyError("unsupported Dukascopy normalization_version")
    if manifest.format != DUKASCOPY_FORMAT:
        raise DukascopyError("unsupported Dukascopy format")
    if manifest.source_timezone != DUKASCOPY_SOURCE_TIMEZONE:
        raise DukascopyError("Dukascopy source_timezone must be UTC epoch milliseconds")
    if manifest.availability_basis != "historical_event_time":
        raise DukascopyError("Dukascopy availability_basis is fixed")
    for name in ("dataset_id", "source_uri", "widget_uri", "config_uri", "terms_uri", "api_base", "data_root"):
        _text(getattr(manifest, name), name)
    for name in ("source_uri", "widget_uri", "config_uri", "terms_uri", "api_base"):
        _https_dukascopy_url(getattr(manifest, name), name, allow_path=True)


def _normalize_manifest_window(manifest: DukascopyManifest) -> None:
    root = Path(manifest.data_root).expanduser()
    if not root.is_absolute():
        raise DukascopyError("Dukascopy data_root must be absolute")
    start = _utc(manifest.window_start, "window_start")
    end = _utc(manifest.window_end, "window_end")
    if start != DUKASCOPY_WINDOW_START or end != DUKASCOPY_WINDOW_END:
        raise DukascopyError("Dukascopy manifest window must be the authorized 2016-03-07 UTC week")
    object.__setattr__(manifest, "window_start", start)
    object.__setattr__(manifest, "window_end", end)
    _check_optional_coverage(manifest.coverage_start, manifest.coverage_end)
    if manifest.coverage_start is not None:
        object.__setattr__(manifest, "coverage_start", _utc(manifest.coverage_start, "coverage_start"))
    if manifest.coverage_end is not None:
        object.__setattr__(manifest, "coverage_end", _utc(manifest.coverage_end, "coverage_end"))
    _nonnegative_int(manifest.quote_count, "quote_count")
    _nonnegative_int(manifest.raw_bytes, "raw_bytes")
    if manifest.raw_bytes > DUKASCOPY_MAX_BYTES:
        raise DukascopyError("Dukascopy manifest exceeds the 200 MiB pilot limit")
    _sha256(manifest.content_hash, "content_hash")


def _validate_manifest_partitions(manifest: DukascopyManifest) -> None:
    if not manifest.partitions:
        raise DukascopyError("Dukascopy manifest must contain partitions")
    ids = [partition.partition_id for partition in manifest.partitions]
    paths = [partition.raw_path for partition in manifest.partitions]
    if len(set(ids)) != len(ids) or len(set(paths)) != len(paths):
        raise DukascopyError("Dukascopy partitions must have unique identities and paths")
    previous_end: datetime | None = None
    total_size = 0
    total_quotes = 0
    for partition in manifest.partitions:
        if partition.start < manifest.window_start or partition.end > manifest.window_end:
            raise DukascopyError("Dukascopy partition is outside the authorized window")
        if previous_end is not None and partition.start < previous_end:
            raise DukascopyError("Dukascopy partition order overlaps")
        previous_end = partition.end
        total_size += partition.raw_size
        total_quotes += partition.quote_count
    if total_size != manifest.raw_bytes:
        raise DukascopyError("Dukascopy raw_bytes does not match partitions")
    if total_quotes != manifest.quote_count:
        raise DukascopyError("Dukascopy quote_count does not match partitions")
    expected_id = f"{DUKASCOPY_PROVIDER}:{DUKASCOPY_INSTRUMENT}:20160307-20160314:{manifest.content_hash[:32]}"
    if manifest.dataset_id != expected_id:
        raise DukascopyError("Dukascopy dataset_id is not bound to the manifest content hash")
    if manifest.content_hash != manifest_content_hash(manifest.partitions):
        raise DukascopyError("Dukascopy content_hash does not match partitions")


@dataclass(frozen=True, slots=True)
class DukascopyManifest:
    """Strict manifest for the exact EUR/USD 2016-03-07 UTC pilot."""

    dataset_id: str
    provider: str
    instrument: str
    normalization_version: str
    format: str
    source_uri: str
    widget_uri: str
    config_uri: str
    terms_uri: str
    api_base: str
    source_timezone: str
    availability_basis: str
    data_root: str
    window_start: datetime
    window_end: datetime
    coverage_start: datetime | None
    coverage_end: datetime | None
    quote_count: int
    raw_bytes: int
    content_hash: str
    partitions: tuple[DukascopyPartition, ...]
    schema_version: int = DUKASCOPY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _validate_manifest_header(self)
        _normalize_manifest_window(self)
        _validate_manifest_partitions(self)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "provider": self.provider,
            "instrument": self.instrument,
            "normalization_version": self.normalization_version,
            "format": self.format,
            "source_uri": self.source_uri,
            "widget_uri": self.widget_uri,
            "config_uri": self.config_uri,
            "terms_uri": self.terms_uri,
            "api_base": self.api_base,
            "source_timezone": self.source_timezone,
            "availability_basis": self.availability_basis,
            "data_root": self.data_root,
            "window_start": _iso(self.window_start),
            "window_end": _iso(self.window_end),
            "coverage_start": _iso(self.coverage_start),
            "coverage_end": _iso(self.coverage_end),
            "quote_count": self.quote_count,
            "raw_bytes": self.raw_bytes,
            "content_hash": self.content_hash,
            "partitions": [partition.to_dict() for partition in self.partitions],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> DukascopyManifest:
        _strict_keys(value, _MANIFEST_KEYS, "Dukascopy manifest")
        raw_partitions = value.get("partitions")
        if not isinstance(raw_partitions, list) or any(not isinstance(item, Mapping) for item in raw_partitions):
            raise DukascopyError("Dukascopy manifest partitions must be a list of objects")
        return cls(
            schema_version=_positive_int(value.get("schema_version"), "schema_version"),
            dataset_id=_required_text(value, "dataset_id"),
            provider=_required_text(value, "provider"),
            instrument=_required_text(value, "instrument"),
            normalization_version=_required_text(value, "normalization_version"),
            format=_required_text(value, "format"),
            source_uri=_required_text(value, "source_uri"),
            widget_uri=_required_text(value, "widget_uri"),
            config_uri=_required_text(value, "config_uri"),
            terms_uri=_required_text(value, "terms_uri"),
            api_base=_required_text(value, "api_base"),
            source_timezone=_required_text(value, "source_timezone"),
            availability_basis=_required_text(value, "availability_basis"),
            data_root=_required_text(value, "data_root"),
            window_start=_required_time(value.get("window_start"), "window_start"),
            window_end=_required_time(value.get("window_end"), "window_end"),
            coverage_start=_parse_optional_iso(value.get("coverage_start"), "coverage_start"),
            coverage_end=_parse_optional_iso(value.get("coverage_end"), "coverage_end"),
            quote_count=_nonnegative_int(value.get("quote_count"), "quote_count"),
            raw_bytes=_nonnegative_int(value.get("raw_bytes"), "raw_bytes"),
            content_hash=_required_text(value, "content_hash"),
            partitions=tuple(DukascopyPartition.from_mapping(item) for item in raw_partitions),
        )

    @classmethod
    def from_path(cls, path: str | Path) -> DukascopyManifest:
        target = Path(path).expanduser()
        _reject_symlink_ancestors(target)
        if target.is_symlink() or not target.is_file():
            raise DukascopyError("Dukascopy manifest is not a regular file")
        try:
            raw = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DukascopyError(f"cannot read Dukascopy manifest: {target}") from exc
        if not isinstance(raw, Mapping):
            raise DukascopyError("Dukascopy manifest must be a JSON object")
        return cls.from_mapping(raw)


@dataclass(frozen=True, slots=True)
class DukascopyQuoteProvider:
    """Small provider facade over a validated, already acquired manifest."""

    manifest: DukascopyManifest

    @property
    def name(self) -> str:
        return f"{DUKASCOPY_PROVIDER}_historical"

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

    def validate(self) -> DukascopyValidation:
        return validate_manifest(self.manifest)


@dataclass(frozen=True, slots=True)
class DukascopyValidation:
    """Bounded validation result for a manifest and its native JSON blocks."""

    ok: bool
    quote_count: int
    raw_bytes: int
    coverage_start: datetime | None
    coverage_end: datetime | None
    content_hash: str | None
    issues: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return self.ok

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "quote_count": self.quote_count,
            "raw_bytes": self.raw_bytes,
            "coverage_start": _iso(self.coverage_start),
            "coverage_end": _iso(self.coverage_end),
            "content_hash": self.content_hash,
            "issues": list(self.issues),
        }


def decode_tick_payload(
    payload: bytes,
    *,
    raw_path: str,
    raw_sha256: str,
    partition_start: datetime,
    partition_end: datetime,
    sequence_start: int = 0,
) -> tuple[HistoricalQuote, ...]:
    """Decode one official compact JSON block without sorting or imputing.

    The official widget accumulates ``times`` from ``timestamp`` and applies
    ``multiplier`` to price deltas.  This mirrors that documented first-party
    decoder using ``Decimal``; no hard-coded pip/price scale is introduced.
    """

    value = _load_tick_payload(payload)
    times, bids, asks = _tick_arrays(value)
    if not times:
        return ()
    timestamp_ms, bid_base, ask_base, multiplier, precision_factor = _tick_header(value)
    return _decode_tick_rows(
        times,
        bids,
        asks,
        timestamp_ms=timestamp_ms,
        bid_base=bid_base,
        ask_base=ask_base,
        multiplier=multiplier,
        precision_factor=precision_factor,
        raw_path=raw_path,
        raw_sha256=raw_sha256,
        partition_start=partition_start,
        partition_end=partition_end,
        sequence_start=sequence_start,
    )


def _load_tick_payload(payload: bytes) -> Mapping[str, Any]:
    if not isinstance(payload, bytes) or not payload:
        raise DukascopyError("Dukascopy tick response must contain non-empty JSON bytes")
    try:
        value = json.loads(payload.decode("utf-8"), parse_float=Decimal, parse_int=int)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DukascopyError("Dukascopy tick response is not valid UTF-8 JSON") from exc
    if not isinstance(value, Mapping):
        raise DukascopyError("Dukascopy tick response must be a JSON object")
    if "error" in value:
        raise DukascopyError("Dukascopy tick response contains an error")
    return value


def _tick_arrays(value: Mapping[str, Any]) -> tuple[list[Any], list[Any], list[Any]]:
    times = _array(value.get("times"), "times")
    bids = _array(value.get("bids"), "bids")
    asks = _array(value.get("asks"), "asks")
    for name, array in (("bids", bids), ("asks", asks)):
        if len(array) != len(times):
            raise DukascopyError(f"Dukascopy {name} length does not match times")
    _validate_volume_arrays(value, len(times))
    return times, bids, asks


def _validate_volume_arrays(value: Mapping[str, Any], expected_length: int) -> None:
    for name in ("bidVolumes", "askVolumes"):
        volume_array = value.get(name)
        if volume_array is None:
            continue
        if not isinstance(volume_array, list) or len(volume_array) != expected_length:
            raise DukascopyError(f"Dukascopy {name} length does not match times")
        for item in volume_array:
            _nonnegative_decimal(item, name)


def _tick_header(value: Mapping[str, Any]) -> tuple[int, Decimal, Decimal, Decimal, Decimal]:
    timestamp_ms = _nonnegative_int(value.get("timestamp"), "timestamp")
    bid_base = _decimal(value.get("bid"), "bid")
    ask_base = _decimal(value.get("ask"), "ask")
    multiplier = _positive_decimal(value.get("multiplier"), "multiplier")
    return timestamp_ms, bid_base, ask_base, multiplier, _round_factor(multiplier)


def _decode_tick_rows(
    times: list[Any],
    bids: list[Any],
    asks: list[Any],
    *,
    timestamp_ms: int,
    bid_base: Decimal,
    ask_base: Decimal,
    multiplier: Decimal,
    precision_factor: Decimal,
    raw_path: str,
    raw_sha256: str,
    partition_start: datetime,
    partition_end: datetime,
    sequence_start: int,
) -> tuple[HistoricalQuote, ...]:
    current_ms = timestamp_ms
    current_bid = bid_base
    current_ask = ask_base
    start = _utc(partition_start, "partition_start")
    end = _utc(partition_end, "partition_end")
    quotes: list[HistoricalQuote] = []
    previous_ms: int | None = None
    for index, (delta_ms, bid_delta, ask_delta) in enumerate(zip(times, bids, asks, strict=True)):
        if isinstance(delta_ms, bool) or not isinstance(delta_ms, int) or delta_ms < 0:
            raise DukascopyError("Dukascopy times must be non-negative integer millisecond deltas")
        current_ms += delta_ms
        if previous_ms is not None and current_ms < previous_ms:
            raise DukascopyError("Dukascopy tick timestamps are not source-ordered")
        previous_ms = current_ms
        current_bid = _round_price(current_bid + _decimal(bid_delta, "bid delta") * multiplier, precision_factor)
        current_ask = _round_price(current_ask + _decimal(ask_delta, "ask delta") * multiplier, precision_factor)
        if current_bid <= 0 or current_ask <= 0 or current_ask < current_bid:
            raise DukascopyError("Dukascopy tick has invalid bid/ask after official multiplier decoding")
        event_time = _epoch_ms(current_ms)
        if event_time < start or event_time >= end:
            raise DukascopyError("Dukascopy tick lies outside its requested hourly partition")
        locator = SourceLocator(
            raw_path,
            "ticks",
            raw_sha256,
            index,
            index + 1,
            kind="compact_json_tick_index",
        )
        quotes.append(
            HistoricalQuote(
                instrument=DUKASCOPY_INSTRUMENT,
                event_time=event_time,
                available_at=event_time,
                bid=current_bid,
                ask=current_ask,
                locator=locator,
                sequence=sequence_start + index,
                source_event_id=f"{raw_sha256}:ticks:{index + 1}:{index}",
                source_timestamp=str(current_ms),
                provider=DUKASCOPY_PROVIDER,
                source_timezone=DUKASCOPY_SOURCE_TIMEZONE,
                precision=f"multiplier={multiplier}",
                source_availability="historical_event_time",
            )
        )
    return tuple(quotes)


def read_manifest(path: str | Path) -> DukascopyManifest:
    """Read and validate one Dukascopy manifest."""

    return DukascopyManifest.from_path(path)


def iter_quotes(
    manifest: DukascopyManifest | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[HistoricalQuote]:
    """Stream native Dukascopy quotes in causal source order over ``[start,end)``."""

    resolved = manifest if isinstance(manifest, DukascopyManifest) else read_manifest(manifest)
    validation = validate_manifest(resolved)
    if not validation.ok:
        raise DukascopyError("Dukascopy manifest/raw validation failed: " + "; ".join(validation.issues))
    start_utc = _utc(start, "start") if start is not None else None
    end_utc = _utc(end, "end") if end is not None else None
    if start_utc is not None and end_utc is not None and end_utc <= start_utc:
        raise DukascopyError("end must be after start")
    sequence = 0
    previous: datetime | None = None
    for partition in resolved.partitions:
        raw_path = _regular_raw_path(resolved.data_root, partition.raw_path)
        raw = raw_path.read_bytes()
        quotes = decode_tick_payload(
            raw,
            raw_path=partition.raw_path,
            raw_sha256=partition.raw_sha256,
            partition_start=partition.start,
            partition_end=partition.end,
            sequence_start=sequence,
        )
        if len(quotes) != partition.quote_count:
            raise DukascopyError(f"Dukascopy partition quote_count mismatch: {partition.partition_id}")
        for quote in quotes:
            if previous is not None and quote.event_time < previous:
                raise DukascopyError("Dukascopy source order violation")
            previous = quote.event_time
            sequence = quote.sequence + 1
            if (start_utc is None or quote.event_time >= start_utc) and (end_utc is None or quote.event_time < end_utc):
                yield quote


def stream(
    manifest: DukascopyManifest | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[HistoricalQuote]:
    """Provider-neutral alias for :func:`iter_quotes`."""

    return iter_quotes(manifest, start, end)


def validate_manifest(manifest: DukascopyManifest | str | Path) -> DukascopyValidation:
    """Verify manifest identity, raw bytes and compact payloads without mutation."""

    try:
        resolved = manifest if isinstance(manifest, DukascopyManifest) else read_manifest(manifest)
        count, raw_bytes, first, last, issues = _scan_manifest_raw(resolved)
        if count != resolved.quote_count:
            issues.append("manifest quote_count does not match decoded quotes")
        if raw_bytes != resolved.raw_bytes:
            issues.append("manifest raw_bytes does not match partitions")
        if first != resolved.coverage_start or last != resolved.coverage_end:
            issues.append("manifest coverage does not match decoded quotes")
        return DukascopyValidation(not issues, count, raw_bytes, first, last, resolved.content_hash, tuple(issues))
    except (DukascopyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return DukascopyValidation(False, 0, 0, None, None, None, (str(exc),))


def _scan_manifest_raw(
    manifest: DukascopyManifest,
) -> tuple[int, int, datetime | None, datetime | None, list[str]]:
    sequence = 0
    count = 0
    raw_bytes = 0
    first: datetime | None = None
    last: datetime | None = None
    previous: datetime | None = None
    issues: list[str] = []
    for partition in manifest.partitions:
        raw_path = _regular_raw_path(manifest.data_root, partition.raw_path)
        if raw_path.stat().st_size != partition.raw_size:
            raise DukascopyError(f"raw size mismatch: {partition.partition_id}")
        if _hash_file(raw_path) != partition.raw_sha256:
            raise DukascopyError(f"raw SHA-256 mismatch: {partition.partition_id}")
        quotes = decode_tick_payload(
            raw_path.read_bytes(),
            raw_path=partition.raw_path,
            raw_sha256=partition.raw_sha256,
            partition_start=partition.start,
            partition_end=partition.end,
            sequence_start=sequence,
        )
        if len(quotes) != partition.quote_count:
            raise DukascopyError(f"quote_count mismatch: {partition.partition_id}")
        first, last, previous = _merge_partition_scan(partition, quotes, first=first, previous=previous, issues=issues)
        sequence += len(quotes)
        count += len(quotes)
        raw_bytes += partition.raw_size
    return count, raw_bytes, first, last, issues


def _merge_partition_scan(
    partition: DukascopyPartition,
    quotes: tuple[HistoricalQuote, ...],
    *,
    first: datetime | None,
    previous: datetime | None,
    issues: list[str],
) -> tuple[datetime | None, datetime | None, datetime | None]:
    if not quotes:
        if partition.coverage_start is not None or partition.coverage_end is not None:
            issues.append(f"empty partition has coverage: {partition.partition_id}")
        return first, previous, previous
    if partition.coverage_start != quotes[0].event_time:
        issues.append(f"coverage_start mismatch: {partition.partition_id}")
    if partition.coverage_end != quotes[-1].event_time:
        issues.append(f"coverage_end mismatch: {partition.partition_id}")
    if previous is not None and quotes[0].event_time < previous:
        issues.append(f"source order violation: {partition.partition_id}")
    return first or quotes[0].event_time, quotes[-1].event_time, quotes[-1].event_time


def manifest_content_hash(partitions: Sequence[DukascopyPartition]) -> str:
    """Hash partition identity, not a rewritten copy of raw response bytes."""

    material = json.dumps(
        [partition.to_dict() for partition in partitions],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def load_provider_manifest(path: str | Path) -> DukascopyManifest | Any:
    """Dispatch only the two known historical providers by manifest provider."""

    target = Path(path).expanduser()
    _reject_symlink_ancestors(target)
    if target.is_symlink() or not target.is_file():
        raise DukascopyError("provider manifest is not a regular file")
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DukascopyError("provider manifest is unreadable") from exc
    if not isinstance(raw, Mapping):
        raise DukascopyError("provider manifest must be an object")
    provider = raw.get("provider")
    if provider == DUKASCOPY_PROVIDER:
        return DukascopyManifest.from_mapping(raw)
    if provider == "histdata":
        from .historical import read_manifest as read_histdata_manifest

        return read_histdata_manifest(target)
    raise DukascopyError(f"unsupported historical provider: {provider!r}")


def iter_provider_quotes(
    manifest: DukascopyManifest | Any | str | Path,
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[HistoricalQuote]:
    """Dispatch a known manifest to its provider reader without fallback guessing."""

    if isinstance(manifest, DukascopyManifest):
        yield from iter_quotes(manifest, start, end)
        return
    resolved = load_provider_manifest(manifest) if isinstance(manifest, (str, Path)) else manifest
    if isinstance(resolved, DukascopyManifest):
        yield from iter_quotes(resolved, start, end)
        return
    from .historical import DatasetManifest
    from .historical import iter_quotes as iter_histdata_quotes

    if isinstance(resolved, DatasetManifest):
        yield from iter_histdata_quotes(resolved, start, end)
        return
    raise DukascopyError("unsupported provider manifest object")


def _array(value: Any, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise DukascopyError(f"Dukascopy payload field {name} must be an array")
    return value


def _epoch_ms(value: int) -> datetime:
    try:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)
    except (OverflowError, ValueError) as exc:
        raise DukascopyError("Dukascopy timestamp is outside datetime range") from exc


def _round_factor(multiplier: Decimal) -> Decimal:
    adjusted = multiplier.adjusted()
    return Decimal(10) ** (-adjusted) if adjusted < 0 else Decimal(1)


def _round_price(value: Decimal, factor: Decimal) -> Decimal:
    quantum = Decimal(1) / factor
    try:
        return value.quantize(quantum, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError) as exc:
        raise DukascopyError("Dukascopy price cannot be represented at official precision") from exc


def _regular_raw_path(data_root: str, raw_path: str) -> Path:
    root = Path(data_root).expanduser()
    _reject_symlink_ancestors(root)
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise DukascopyError("Dukascopy data_root is not a regular directory")
    target = root / _relative_path(raw_path, "raw_path")
    _reject_symlink_ancestors(target)
    if target.is_symlink() or not target.is_file():
        raise DukascopyError("Dukascopy raw path is not a regular file")
    mode = stat.S_IMODE(os.lstat(target).st_mode)
    if mode != 0o600:
        raise DukascopyError("Dukascopy raw file must be mode 600")
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
            raise DukascopyError(f"path contains symlink component: {current}")


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DukascopyError(f"{name} must be non-empty text")
    return value.strip()


def _required_text(value: Mapping[str, Any], key: str) -> str:
    return _text(value.get(key), key)


def _optional_text(value: Mapping[str, Any], key: str, default: str) -> str:
    raw = value.get(key, default)
    return _text(raw, key)


def _required_time(value: Any, name: str) -> datetime:
    parsed = _parse_optional_iso(value, name)
    if parsed is None:
        raise DukascopyError(f"{name} is required")
    return parsed


def _relative_path(value: str, name: str) -> Path:
    path = Path(value)
    if not isinstance(value, str) or not value or path.is_absolute() or ".." in path.parts:
        raise DukascopyError(f"{name} must be a safe relative path")
    return path


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise DukascopyError(f"{name} must be a positive integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DukascopyError(f"{name} must be a non-negative integer")
    return value


def _sha256(value: Any, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(char not in "0123456789abcdefABCDEF" for char in value):
        raise DukascopyError(f"{name} must be a hexadecimal SHA-256")
    return value


def _decimal(value: Any, name: str) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise DukascopyError(f"{name} must be a decimal") from exc
    if not result.is_finite():
        raise DukascopyError(f"{name} must be finite")
    return result


def _positive_decimal(value: Any, name: str) -> Decimal:
    result = _decimal(value, name)
    if result <= 0:
        raise DukascopyError(f"{name} must be positive")
    return result


def _nonnegative_decimal(value: Any, name: str) -> Decimal:
    result = _decimal(value, name)
    if result < 0:
        raise DukascopyError(f"{name} must be non-negative")
    return result


def _check_optional_coverage(start: datetime | None, end: datetime | None) -> None:
    if start is not None:
        _utc(start, "coverage_start")
    if end is not None:
        _utc(end, "coverage_end")
    if start is not None and end is not None and _utc(end, "coverage_end") < _utc(start, "coverage_start"):
        raise DukascopyError("coverage_end must not precede coverage_start")


def _strict_keys(value: Mapping[str, Any], allowed: frozenset[str], name: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise DukascopyError(f"{name} has unknown keys: {sorted(str(item) for item in unknown)}")


def _https_dukascopy_url(value: str, name: str, *, allow_path: bool) -> None:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(".dukascopy.com"):
        raise DukascopyError(f"{name} must be an official Dukascopy HTTPS URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise DukascopyError(f"{name} must not contain credentials, query, or fragment")
    if not allow_path and parsed.path not in ("", "/"):
        raise DukascopyError(f"{name} must not contain a path")


DukascopyProvider = DukascopyQuoteProvider
DukascopyDataProvider = DukascopyQuoteProvider


__all__ = [
    "DUKASCOPY_CONFIG_URI",
    "DUKASCOPY_DEFAULT_DATA_ROOT",
    "DUKASCOPY_FORMAT",
    "DUKASCOPY_INSTRUMENT",
    "DUKASCOPY_INTERVAL_CODE",
    "DUKASCOPY_MAX_BYTES",
    "DUKASCOPY_NORMALIZATION_VERSION",
    "DUKASCOPY_PROVIDER",
    "DUKASCOPY_SCHEMA_VERSION",
    "DUKASCOPY_SOURCE_URI",
    "DUKASCOPY_SOURCE_TIMEZONE",
    "DUKASCOPY_SYMBOL",
    "DUKASCOPY_TERMS_URI",
    "DUKASCOPY_TICKS_PATH_TEMPLATE",
    "DUKASCOPY_WIDGET_URI",
    "DUKASCOPY_WINDOW_END",
    "DUKASCOPY_WINDOW_START",
    "DukascopyError",
    "DukascopyDataProvider",
    "DukascopyManifest",
    "DukascopyPartition",
    "DukascopyProvider",
    "DukascopyQuoteProvider",
    "DukascopyValidation",
    "decode_tick_payload",
    "iter_provider_quotes",
    "iter_quotes",
    "load_provider_manifest",
    "manifest_content_hash",
    "read_manifest",
    "stream",
    "validate_manifest",
]
