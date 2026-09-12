"""Strict local CSV/JSONL ingestion with explicit column and unit mapping.

No field is guessed from a neighbouring price type.  In particular, a quote
``bid``/``ask``/``mid`` basis must be selected explicitly and events never get
invented from a candle.  Invalid rows are rejected in strict mode; lenient mode
returns valid rows plus a complete issue list for inspection.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import (
    Bar,
    DataSet,
    DataValidationError,
    Event,
    PriceBasis,
    Provenance,
    ValidationIssue,
    infer_quality,
    resolution_to_seconds,
)

RecordKind = Literal["bar", "candle", "event"]
_TIMESTAMP_UNITS = {"iso8601", "s", "ms", "us", "ns"}
_PRICE_BASES = {"traded", "bid", "ask", "mid"}


class ImportConfigurationError(ValueError):
    """Raised before reading rows when mapping/configuration is ambiguous."""


@dataclass(frozen=True, slots=True)
class ColumnMapping:
    """Names and timestamp units for a row-oriented local file.

    A field set to ``None`` is not read from the row and must be supplied by
    :class:`ImportConfig` when it is required.  The mapping is intentionally
    explicit even when its conventional defaults are used.
    """

    record_kind: RecordKind = "candle"
    timestamp: str = "timestamp"
    timestamp_unit: str = "iso8601"
    assume_timezone: str | None = None
    instrument: str | None = "instrument"
    resolution: str | None = "resolution"
    open: str | None = "open"
    high: str | None = "high"
    low: str | None = "low"
    close: str | None = "close"
    volume: str | None = "volume"
    trade_count: str | None = "trade_count"
    price: str | None = "price"
    bid: str | None = "bid"
    ask: str | None = "ask"
    mid: str | None = "mid"
    quantity: str | None = "quantity"
    received_at: str | None = "received_at"
    available_at: str | None = "available_at"
    event_id: str | None = "event_id"
    sequence: str | None = "sequence"
    side: str | None = "side"

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ColumnMapping:
        allowed = {item.name for item in fields(cls)}
        unknown = set(value) - allowed
        if unknown:
            raise ImportConfigurationError(f"unknown column mapping keys: {sorted(unknown)}")
        data = dict(value)
        if "record_kind" in data and data["record_kind"] == "bar":
            data["record_kind"] = "candle"
        result = cls(**data)
        result.validate()
        return result

    def validate(self) -> None:
        if self.record_kind not in {"bar", "candle", "event"}:
            raise ImportConfigurationError(f"record_kind must be candle/bar/event, got {self.record_kind!r}")
        if self.timestamp_unit not in _TIMESTAMP_UNITS:
            raise ImportConfigurationError(
                f"timestamp_unit must be one of {sorted(_TIMESTAMP_UNITS)}, got {self.timestamp_unit!r}"
            )
        if not self.timestamp:
            raise ImportConfigurationError("timestamp column must not be empty")
        if self.assume_timezone:
            try:
                ZoneInfo(self.assume_timezone)
            except ZoneInfoNotFoundError as exc:
                raise ImportConfigurationError(f"unknown assume_timezone: {self.assume_timezone!r}") from exc


@dataclass(frozen=True, slots=True)
class ImportConfig:
    """File-level defaults and validation policy."""

    instrument: str | None = None
    resolution: int | str | None = None
    price_basis: PriceBasis = "traded"
    source: str = "local-file"
    strict: bool = True
    reorder: bool = False
    drop_duplicates: bool = False

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ImportConfig:
        allowed = {item.name for item in fields(cls)}
        unknown = set(value) - allowed
        if unknown:
            raise ImportConfigurationError(f"unknown import config keys: {sorted(unknown)}")
        result = cls(**dict(value))
        result.validate()
        return result

    def validate(self) -> None:
        if self.price_basis not in _PRICE_BASES:
            raise ImportConfigurationError(f"price_basis must be one of {sorted(_PRICE_BASES)}")
        if self.resolution is not None:
            try:
                resolution_to_seconds(self.resolution)
            except (TypeError, ValueError) as exc:
                raise ImportConfigurationError(f"invalid resolution: {self.resolution!r}") from exc
        if not self.source or not self.source.strip():
            raise ImportConfigurationError("source must not be empty")


@dataclass(frozen=True, slots=True)
class _Parsed:
    records: tuple[Event | Bar, ...]
    issues: tuple[ValidationIssue, ...]
    source_hash: str


@dataclass(frozen=True, slots=True)
class _ParsedRow:
    record: Event | Bar
    timestamp: datetime
    identity_key: tuple[Any, ...]


def _nonempty(
    row: Mapping[str, Any], column: str | None, *, row_number: int, field_name: str, required: bool = False
) -> Any:
    if column is None:
        if required:
            raise ValueError(f"no column mapping for {field_name}")
        return None
    if column not in row:
        if required:
            raise KeyError(f"mapped column {column!r} is absent")
        return None
    value = row[column]
    if value is None or (isinstance(value, str) and not value.strip()):
        if required:
            raise ValueError(f"{field_name} is empty")
        return None
    return value


def _parse_number(value: Any, *, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _parse_int(value: Any, *, name: str) -> int:
    result = _parse_number(value, name=name)
    if int(result) != result:
        raise ValueError(f"{name} must be an integer")
    return int(result)


def parse_timestamp(value: Any, *, unit: str, assume_timezone: str | None = None) -> datetime:
    """Parse one source timestamp and normalize it to UTC."""

    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError("timestamp is empty")
    if unit == "iso8601":
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            raise ValueError("numeric timestamp requires explicit s/ms/us/ns unit")
        text = str(value).strip()
        if text.endswith("Z") or text.endswith("z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        numeric = _parse_number(value, name="timestamp")
        scale = {"s": 1.0, "ms": 1e-3, "us": 1e-6, "ns": 1e-9}[unit]
        try:
            parsed = datetime.fromtimestamp(numeric * scale, tz=UTC)
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError(f"timestamp fuera de rango: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        if not assume_timezone:
            raise ValueError("timestamp has no timezone; set assume_timezone explicitly")
        parsed = parsed.replace(tzinfo=ZoneInfo(assume_timezone))
    return parsed.astimezone(UTC)


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_headers(
    rows: Sequence[Mapping[str, Any]], mapping: ColumnMapping, config: ImportConfig
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not rows:
        return issues
    keys = set(rows[0])
    required = [mapping.timestamp]
    if mapping.instrument is not None and config.instrument is None:
        required.append(mapping.instrument)
    if mapping.record_kind in {"candle", "bar"}:
        required += [name for name in (mapping.open, mapping.high, mapping.low, mapping.close) if name is not None]
        if config.resolution is None and mapping.resolution is not None:
            required.append(mapping.resolution)
    else:
        source_column = {"traded": mapping.price, "bid": mapping.bid, "ask": mapping.ask, "mid": mapping.mid}[
            config.price_basis
        ]
        if source_column is not None:
            required.append(source_column)
    for column in required:
        if column is None:
            continue
        if column not in keys:
            issues.append(ValidationIssue("MISSING_COLUMN", f"mapped column {column!r} is absent", field=column))
    if mapping.record_kind == "event" and config.price_basis != "traded" and mapping.price is not None:
        # The existence of a traded price must not silently change the selected basis.
        issues.append(
            ValidationIssue(
                "EXPLICIT_PRICE_BASIS",
                f"event importer will use {config.price_basis!r}; mapped price column {mapping.price!r} is not used",
                severity="WARNING",
            )
        )
    return issues


def _row_instrument(row: Mapping[str, Any], mapping: ColumnMapping, config: ImportConfig, *, row_number: int) -> str:
    value = config.instrument
    if value is None:
        value = _nonempty(row, mapping.instrument, row_number=row_number, field_name="instrument", required=True)
    instrument = str(value).strip()
    if not instrument:
        raise ValueError("instrument is empty")
    return instrument


def _row_resolution(
    row: Mapping[str, Any],
    mapping: ColumnMapping,
    config: ImportConfig,
    *,
    row_number: int,
) -> int | None:
    if mapping.record_kind not in {"candle", "bar"}:
        return None
    value = config.resolution
    if value is None:
        value = _nonempty(row, mapping.resolution, row_number=row_number, field_name="resolution", required=True)
    return resolution_to_seconds(value) if value is not None else None


def _row_times(
    row: Mapping[str, Any],
    mapping: ColumnMapping,
    *,
    row_number: int,
    timestamp_unit: str,
) -> tuple[datetime | None, datetime | None]:
    received_raw = _nonempty(row, mapping.received_at, row_number=row_number, field_name="received_at")
    available_raw = _nonempty(row, mapping.available_at, row_number=row_number, field_name="available_at")
    return (
        parse_timestamp(received_raw, unit=timestamp_unit, assume_timezone=mapping.assume_timezone)
        if received_raw is not None
        else None,
        parse_timestamp(available_raw, unit=timestamp_unit, assume_timezone=mapping.assume_timezone)
        if available_raw is not None
        else None,
    )


def _row_identity(
    row: Mapping[str, Any],
    mapping: ColumnMapping,
    *,
    row_number: int,
) -> tuple[str, int | str | None, bool]:
    source_id_raw = _nonempty(row, mapping.event_id, row_number=row_number, field_name="event_id")
    source_id = str(source_id_raw) if source_id_raw is not None else f"line:{row_number}"
    sequence_raw = _nonempty(row, mapping.sequence, row_number=row_number, field_name="sequence")
    sequence: int | str | None = None
    if sequence_raw is not None:
        try:
            sequence = _parse_int(sequence_raw, name="sequence")
        except ValueError:
            sequence = str(sequence_raw)
    return source_id, sequence, source_id_raw is not None


def _parse_bar_row(
    row: Mapping[str, Any],
    mapping: ColumnMapping,
    config: ImportConfig,
    *,
    timestamp: datetime,
    instrument: str,
    resolution_seconds: int,
    received_at: datetime | None,
    available_at: datetime | None,
    source_id: str,
    source_uri: str,
    row_number: int,
) -> Bar:
    prices = {
        name: _parse_number(
            _nonempty(row, getattr(mapping, name), row_number=row_number, field_name=name, required=True),
            name=name,
        )
        for name in ("open", "high", "low", "close")
    }
    volume_raw = _nonempty(row, mapping.volume, row_number=row_number, field_name="volume")
    count_raw = _nonempty(row, mapping.trade_count, row_number=row_number, field_name="trade_count")
    return Bar(
        instrument=instrument,
        interval_start=timestamp,
        interval_end=timestamp + timedelta(seconds=resolution_seconds),
        open=prices["open"],
        high=prices["high"],
        low=prices["low"],
        close=prices["close"],
        resolution_seconds=resolution_seconds,
        volume=_parse_number(volume_raw, name="volume") if volume_raw is not None else None,
        trade_count=_parse_int(count_raw, name="trade_count") if count_raw is not None else None,
        price_basis=config.price_basis,
        source=config.source,
        source_record_id=source_id,
        received_at=received_at,
        available_at=available_at,
        closed=True,
        synthetic=False,
        metadata={"source_uri": source_uri, "source_row": row_number},
    )


def _parse_event_row(
    row: Mapping[str, Any],
    mapping: ColumnMapping,
    config: ImportConfig,
    *,
    timestamp: datetime,
    instrument: str,
    received_at: datetime | None,
    available_at: datetime | None,
    source_id: str,
    sequence: int | str | None,
    source_uri: str,
    row_number: int,
) -> Event:
    basis_column = {"traded": mapping.price, "bid": mapping.bid, "ask": mapping.ask, "mid": mapping.mid}[
        config.price_basis
    ]
    price = _parse_number(
        _nonempty(row, basis_column, row_number=row_number, field_name=config.price_basis, required=True),
        name=config.price_basis,
    )
    optional_prices: dict[str, float | None] = {}
    for name, column in (("bid", mapping.bid), ("ask", mapping.ask), ("mid", mapping.mid)):
        raw = _nonempty(row, column, row_number=row_number, field_name=name)
        optional_prices[name] = _parse_number(raw, name=name) if raw is not None else None
    quantity_raw = _nonempty(row, mapping.quantity, row_number=row_number, field_name="quantity")
    side_raw = _nonempty(row, mapping.side, row_number=row_number, field_name="side")
    side = str(side_raw).strip().lower() if side_raw is not None else None
    if side is not None and side not in {"buy", "sell", "unknown"}:
        raise ValueError("side must be buy, sell or unknown")
    return Event(
        instrument=instrument,
        event_time=timestamp,
        price=price,
        bid=optional_prices["bid"],
        ask=optional_prices["ask"],
        mid=optional_prices["mid"],
        price_basis=config.price_basis,
        quantity=_parse_number(quantity_raw, name="quantity") if quantity_raw is not None else None,
        received_at=received_at,
        available_at=available_at,
        source=config.source,
        source_event_id=source_id,
        source_sequence=sequence,
        side=side,
        synthetic=False,
        metadata={"source_uri": source_uri, "source_row": row_number},
    )


def _parse_row(
    row: Mapping[str, Any],
    mapping: ColumnMapping,
    config: ImportConfig,
    *,
    source_uri: str,
    row_number: int,
) -> _ParsedRow:
    timestamp = parse_timestamp(
        _nonempty(row, mapping.timestamp, row_number=row_number, field_name="timestamp", required=True),
        unit=mapping.timestamp_unit,
        assume_timezone=mapping.assume_timezone,
    )
    instrument = _row_instrument(row, mapping, config, row_number=row_number)
    resolution_seconds = _row_resolution(row, mapping, config, row_number=row_number)
    received_at, available_at = _row_times(
        row,
        mapping,
        row_number=row_number,
        timestamp_unit=mapping.timestamp_unit,
    )
    source_id, sequence, has_explicit_source_id = _row_identity(row, mapping, row_number=row_number)
    if mapping.record_kind in {"candle", "bar"}:
        assert resolution_seconds is not None
        record: Event | Bar = _parse_bar_row(
            row,
            mapping,
            config,
            timestamp=timestamp,
            instrument=instrument,
            resolution_seconds=resolution_seconds,
            received_at=received_at,
            available_at=available_at,
            source_id=source_id,
            source_uri=source_uri,
            row_number=row_number,
        )
        identity_key: tuple[Any, ...] = ("bar", instrument, resolution_seconds, timestamp)
    else:
        record = _parse_event_row(
            row,
            mapping,
            config,
            timestamp=timestamp,
            instrument=instrument,
            received_at=received_at,
            available_at=available_at,
            source_id=source_id,
            sequence=sequence,
            source_uri=source_uri,
            row_number=row_number,
        )
        identity_key = (
            "event",
            instrument,
            source_id if mapping.event_id is not None and has_explicit_source_id else timestamp,
            sequence,
        )
    return _ParsedRow(record=record, timestamp=timestamp, identity_key=identity_key)


def _row_issues(
    parsed_row: _ParsedRow,
    *,
    mapping: ColumnMapping,
    config: ImportConfig,
    row_number: int,
    previous_time: datetime | None,
    identity_keys: set[tuple[Any, ...]],
) -> tuple[list[ValidationIssue], bool]:
    issues: list[ValidationIssue] = []
    if previous_time is not None and parsed_row.timestamp < previous_time:
        issues.append(
            ValidationIssue(
                "OUT_OF_ORDER",
                f"timestamp {parsed_row.timestamp.isoformat()} precedes prior row",
                severity="ERROR" if not config.reorder else "WARNING",
                row=row_number,
                field=mapping.timestamp,
            )
        )
    if parsed_row.identity_key in identity_keys:
        instrument = parsed_row.record.instrument
        issues.append(
            ValidationIssue(
                "DUPLICATE",
                f"duplicate identity for {instrument} at {parsed_row.timestamp.isoformat()}",
                severity="ERROR" if not config.drop_duplicates else "WARNING",
                row=row_number,
            )
        )
        if config.drop_duplicates:
            return issues, True
    return issues, False


def _gap_issues(records: Sequence[Event | Bar]) -> list[ValidationIssue]:
    bars = sorted((record for record in records if isinstance(record, Bar)), key=lambda item: item.interval_start)
    issues: list[ValidationIssue] = []
    for previous, current in zip(bars, bars[1:], strict=False):
        if current.instrument != previous.instrument or current.resolution_seconds != previous.resolution_seconds:
            continue
        if current.interval_start > previous.interval_end:
            issues.append(
                ValidationIssue(
                    "GAP",
                    f"sin datos entre {previous.interval_end.isoformat()} y {current.interval_start.isoformat()}; no se interpoló y la continuidad queda impedida",
                    severity="WARNING",
                )
            )
    return issues


def _parse_rows(
    rows: Sequence[Mapping[str, Any]],
    raw_bytes: bytes,
    mapping: ColumnMapping,
    config: ImportConfig,
    *,
    source_uri: str,
) -> _Parsed:
    issues: list[ValidationIssue] = _validate_headers(rows, mapping, config)
    if any(issue.severity == "ERROR" for issue in issues):
        return _Parsed((), tuple(issues), hashlib.sha256(raw_bytes).hexdigest())

    records: list[Event | Bar] = []
    identity_keys: set[tuple[Any, ...]] = set()
    previous_time: datetime | None = None
    for row_number, row in enumerate(rows, start=1):
        try:
            parsed_row = _parse_row(row, mapping, config, source_uri=source_uri, row_number=row_number)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            issues.append(ValidationIssue("PARSE_ERROR", str(exc), row=row_number))
            continue
        row_issues, dropped = _row_issues(
            parsed_row,
            mapping=mapping,
            config=config,
            row_number=row_number,
            previous_time=previous_time,
            identity_keys=identity_keys,
        )
        issues.extend(row_issues)
        previous_time = parsed_row.timestamp
        if dropped:
            continue
        identity_keys.add(parsed_row.identity_key)
        records.append(parsed_row.record)

    if config.reorder:
        records.sort(key=lambda item: item.event_time if isinstance(item, Event) else item.interval_start)
    # A gap is a coverage fact, not proof that the venue was inactive.  Keep it
    # as a warning so strict ingestion does not fabricate continuity; the core
    # can block any analysis that requires a continuous window.
    issues.extend(_gap_issues(records))
    return _Parsed(tuple(records), tuple(issues), hashlib.sha256(raw_bytes).hexdigest())


def _result(parsed: _Parsed, *, config: ImportConfig, source_uri: str, path: Path, mapping: ColumnMapping) -> DataSet:
    if config.strict and any(issue.severity == "ERROR" for issue in parsed.issues):
        raise DataValidationError(f"invalid local data in {path}", parsed.issues)
    records = parsed.records
    times = [record.event_time if isinstance(record, Event) else record.interval_start for record in records]
    coverage_end = None
    if records:
        coverage_end = max(
            record.event_time if isinstance(record, Event) else record.interval_end for record in records
        )
    provenance = Provenance(
        provider=config.source,
        mode="IMPORT",
        instrument=config.instrument or (records[0].instrument if records else "unknown"),
        resolutions=tuple(sorted({record.resolution for record in records if isinstance(record, Bar)})),
        price_basis=config.price_basis,
        source_uri=source_uri,
        source_hash=parsed.source_hash,
        coverage_start=min(times) if times else None,
        coverage_end=coverage_end,
        synthetic=False,
        notes=(
            "timestamps normalizados a UTC",
            "event_time/interval_start se conserva separado de received_at y available_at",
            "no se interpolaron eventos intrabar a partir de velas",
            *tuple(issue.message for issue in parsed.issues if issue.severity == "WARNING"),
        ),
    )
    quality = infer_quality(records, parsed.issues)
    return DataSet(records=records, provenance=provenance, quality=quality, issues=parsed.issues)


def import_csv(
    path: str | Path,
    mapping: ColumnMapping | Mapping[str, Any] | None = None,
    *,
    config: ImportConfig | None = None,
    encoding: str = "utf-8",
) -> DataSet:
    """Import a headered CSV file with strict validation by default."""

    return _import_delimited(Path(path), mapping, config=config, encoding=encoding, delimiter=",")


def _import_delimited(
    path: Path,
    mapping: ColumnMapping | Mapping[str, Any] | None,
    *,
    config: ImportConfig | None,
    encoding: str,
    delimiter: str,
) -> DataSet:
    mapping_obj = _coerce_mapping(mapping)
    config_obj = config or ImportConfig()
    mapping_obj.validate()
    config_obj.validate()
    raw_bytes = path.read_bytes()
    with path.open("r", encoding=encoding, newline="") as stream:
        reader = csv.DictReader(stream, delimiter=delimiter)
        if reader.fieldnames is None:
            raise DataValidationError(f"CSV has no header: {path}")
        rows = [dict(row) for row in reader]
    parsed = _parse_rows(rows, raw_bytes, mapping_obj, config_obj, source_uri=path.resolve().as_uri())
    return _result(parsed, config=config_obj, source_uri=path.resolve().as_uri(), path=path, mapping=mapping_obj)


def import_jsonl(
    path: str | Path,
    mapping: ColumnMapping | Mapping[str, Any] | None = None,
    *,
    config: ImportConfig | None = None,
    encoding: str = "utf-8",
) -> DataSet:
    """Import one JSON object per line; arrays and malformed lines are errors."""

    path_obj = Path(path)
    mapping_obj = _coerce_mapping(mapping)
    config_obj = config or ImportConfig()
    mapping_obj.validate()
    config_obj.validate()
    raw_bytes = path_obj.read_bytes()
    rows: list[Mapping[str, Any]] = []
    structural_issues: list[ValidationIssue] = []
    with path_obj.open("r", encoding=encoding) as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
                if not isinstance(item, dict):
                    raise ValueError("JSONL line must be an object")
                rows.append(item)
            except (json.JSONDecodeError, ValueError) as exc:
                structural_issues.append(ValidationIssue("PARSE_ERROR", str(exc), row=line_number))
    parsed = _parse_rows(rows, raw_bytes, mapping_obj, config_obj, source_uri=path_obj.resolve().as_uri())
    parsed = _Parsed(parsed.records, tuple(structural_issues) + parsed.issues, parsed.source_hash)
    return _result(
        parsed, config=config_obj, source_uri=path_obj.resolve().as_uri(), path=path_obj, mapping=mapping_obj
    )


def _coerce_mapping(mapping: ColumnMapping | Mapping[str, Any] | None) -> ColumnMapping:
    if mapping is None:
        result = ColumnMapping()
    elif isinstance(mapping, ColumnMapping):
        result = mapping
    elif isinstance(mapping, Mapping):
        result = ColumnMapping.from_dict(mapping)
    else:
        raise ImportConfigurationError("mapping must be ColumnMapping, mapping dict, or None")
    result.validate()
    return result


# Short aliases useful to a CLI or integration tests.
load_csv = import_csv
load_jsonl = import_jsonl
