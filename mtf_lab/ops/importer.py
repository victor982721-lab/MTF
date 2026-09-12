"""Strict local CSV/JSONL importer used by ``mtf-lab import``.

The importer does not infer ticks from OHLC candles and does not interpolate
missing observations.  A column mapping and time unit are explicit, and a
naive timestamp is rejected unless the caller supplies a timezone.
"""

from __future__ import annotations

import csv
import dataclasses
import json
import math
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnMapping:
    timestamp: str | None = "timestamp"
    end_timestamp: str | None = None
    event_timestamp: str | None = None
    open: str | None = "open"
    high: str | None = "high"
    low: str | None = "low"
    close: str | None = "close"
    volume: str | None = "volume"
    price: str | None = "price"
    bid: str | None = "bid"
    ask: str | None = "ask"
    mid: str | None = "mid"
    event_id: str | None = "event_id"
    received_at: str | None = "received_at"
    available_at: str | None = "available_at"

    @classmethod
    def from_text(cls, text: str | None) -> ColumnMapping:
        if not text:
            return cls()
        known = {field.name for field in dataclasses.fields(cls)}
        data: dict[str, str | None] = {}
        for chunk in text.split(","):
            if not chunk.strip() or "=" not in chunk:
                raise ValueError(f"mapping entry must be key=column: {chunk!r}")
            key, column = (part.strip() for part in chunk.split("=", 1))
            if key not in known:
                raise ValueError(f"unknown mapping key: {key!r}")
            data[key] = column or None
        return cls(**data)


@dataclasses.dataclass(frozen=True, slots=True)
class ImportIssue:
    row: int
    code: str
    message: str
    severity: str = "ERROR"

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


class ImportValidationError(ValueError):
    def __init__(self, issues: Iterable[ImportIssue]):
        self.issues = list(issues)
        super().__init__("; ".join(f"fila {x.row}: {x.code}: {x.message}" for x in self.issues))


@dataclasses.dataclass(slots=True)
class ImportResult:
    records: list[dict[str, Any]]
    issues: list[ImportIssue]
    format: str
    instrument: str
    timeframe: str
    price_base: str
    coverage_start: str | None
    coverage_end: str | None
    source_path: str

    @property
    def errors(self) -> list[ImportIssue]:
        return [x for x in self.issues if x.severity == "ERROR"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "instrument": self.instrument,
            "timeframe": self.timeframe,
            "price_base": self.price_base,
            "coverage_start": self.coverage_start,
            "coverage_end": self.coverage_end,
            "source_path": self.source_path,
            "record_count": len(self.records),
            "issues": [x.to_dict() for x in self.issues],
        }


@dataclasses.dataclass(frozen=True, slots=True)
class _ParsedValues:
    open_value: float | None
    high_value: float | None
    low_value: float | None
    close_value: float | None
    volume: float | None
    event_price: float | None
    bid: float | None
    ask: float | None
    mid: float | None

    @property
    def is_candle(self) -> bool:
        return all(value is not None for value in (self.open_value, self.high_value, self.low_value, self.close_value))


def _parse_ts(value: Any, timezone: str | None, unit: str = "iso8601") -> datetime:
    if value is None or str(value).strip() == "":
        raise ValueError("timestamp vacío")
    unit = str(unit).lower()
    try:
        if unit == "iso8601":
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                raise ValueError("timestamp numérico requiere --timestamp-unit explícito")
            text = str(value).strip()
            if text.endswith(("Z", "z")):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
        else:
            numeric = float(value)
            scale = {"s": 1.0, "ms": 1e-3, "us": 1e-6, "ns": 1e-9}[unit]
            dt = datetime.fromtimestamp(numeric * scale, UTC)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise ValueError(f"timestamp inválido ({unit}): {value!r}") from exc
    if dt.tzinfo is None:
        if not timezone:
            raise ValueError("timestamp sin zona; indique --timezone")
        dt = dt.replace(tzinfo=ZoneInfo(timezone))
    return dt.astimezone(UTC)


def _iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _float(row: Mapping[str, Any], column: str | None, *, name: str) -> float | None:
    if not column or row.get(column) in {None, ""}:
        return None
    value = float(row[column])
    if not math.isfinite(value):
        raise ValueError(f"{name} no finito")
    return value


def _strict_bool(value: Any, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "si", "sí"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    raise ValueError(f"{name} debe ser true/false")


def _source_value(source: Mapping[str, Any], column: str | None) -> Any:
    return source.get(column) if column is not None else None


class LocalImporter:
    def __init__(
        self,
        *,
        instrument: str,
        timeframe: str,
        price_base: str,
        mapping: ColumnMapping | None = None,
        timezone: str | None = None,
        timestamp_unit: str = "iso8601",
        interval_seconds: float | None = None,
        strict: bool = True,
        source: str | None = None,
        allow_out_of_order: bool = False,
        allow_duplicates: bool = False,
    ):
        self.instrument = instrument
        self.timeframe = timeframe
        self.price_base = price_base.lower()
        self.mapping = mapping or ColumnMapping()
        self.timezone = timezone
        self.timestamp_unit = str(timestamp_unit).lower()
        if self.timestamp_unit not in {"iso8601", "s", "ms", "us", "ns"}:
            raise ValueError("timestamp_unit must be iso8601, s, ms, us or ns")
        self.interval_seconds = (
            float(interval_seconds) if interval_seconds is not None else self._interval_from_timeframe(timeframe)
        )
        self.strict = strict
        self.source = str(source).strip() if source else None
        self.allow_out_of_order = allow_out_of_order
        self.allow_duplicates = allow_duplicates
        if self.price_base not in {"trade", "close", "mid", "bid", "ask"}:
            raise ValueError("price_base must be trade, close, mid, bid or ask")

    @staticmethod
    def _interval_from_timeframe(value: str) -> float | None:
        text = str(value).upper()
        if text.startswith("M") and text[1:].isdigit():
            return float(int(text[1:]) * 60)
        if text.startswith("H") and text[1:].isdigit():
            return float(int(text[1:]) * 3600)
        return None

    def _read_rows(self, path: Path, fmt: str | None = None) -> tuple[str, list[dict[str, Any]]]:
        fmt = (fmt or path.suffix.lstrip(".")).lower()
        if fmt == "jsonl":
            rows = []
            with path.open(encoding="utf-8") as stream:
                for line_no, line in enumerate(stream, 1):
                    if not line.strip():
                        continue
                    value = json.loads(line)
                    if not isinstance(value, Mapping):
                        raise ValueError(f"JSONL fila {line_no} no es objeto")
                    rows.append(dict(value))
            return fmt, rows
        if fmt == "json":
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, list):
                raise ValueError("JSON debe contener una lista de objetos")
            return fmt, [dict(x) for x in value]
        if fmt == "csv":
            with path.open(newline="", encoding="utf-8-sig") as stream:
                return fmt, [dict(row) for row in csv.DictReader(stream)]
        raise ValueError("format must be csv, jsonl or json")

    def _row_times(self, source: Mapping[str, Any]) -> tuple[datetime, datetime]:
        mapping = self.mapping
        ts_col = (
            mapping.event_timestamp
            if (source.get("kind") or source.get("type")) and mapping.event_timestamp
            else mapping.timestamp
        )
        timestamp = _parse_ts(_source_value(source, ts_col), self.timezone, self.timestamp_unit)
        end_value = _source_value(source, mapping.end_timestamp)
        end = (
            _parse_ts(end_value, self.timezone, self.timestamp_unit)
            if end_value not in {None, ""}
            else (timestamp + timedelta(seconds=self.interval_seconds) if self.interval_seconds else None)
        )
        if end is None or end <= timestamp:
            raise ValueError("intervalo final ausente o no positivo")
        return timestamp, end

    def _check_order(
        self,
        row_number: int,
        timestamp: datetime,
        previous: datetime | None,
        issues: list[ImportIssue],
    ) -> None:
        if previous is not None and timestamp < previous:
            issue = ImportIssue(row_number, "OUT_OF_ORDER", "timestamp fuera de orden")
            issues.append(issue)
            if self.strict and not self.allow_out_of_order:
                raise ImportValidationError([issue])

    def _row_key(self, source: Mapping[str, Any], timestamp: datetime) -> tuple[str, str]:
        mapping = self.mapping
        supplied_id = source.get(mapping.event_id) if mapping.event_id else None
        source_is_candle = all(
            _source_value(source, column) not in {None, ""}
            for column in (mapping.open, mapping.high, mapping.low, mapping.close)
        )
        key_token = str(supplied_id) if supplied_id not in {None, ""} and not source_is_candle else _iso(timestamp)
        return self.instrument, key_token

    def _check_duplicate(
        self,
        row_number: int,
        key: tuple[str, str],
        seen: set[tuple[str, str]],
        issues: list[ImportIssue],
    ) -> None:
        if key in seen:
            issue = ImportIssue(row_number, "DUPLICATE", "timestamp duplicado para el instrumento")
            issues.append(issue)
            if self.strict and not self.allow_duplicates:
                raise ImportValidationError([issue])
        seen.add(key)

    def _numeric_values(self, source: Mapping[str, Any]) -> _ParsedValues:
        mapping = self.mapping
        values = _ParsedValues(
            open_value=_float(source, mapping.open, name="open"),
            high_value=_float(source, mapping.high, name="high"),
            low_value=_float(source, mapping.low, name="low"),
            close_value=_float(source, mapping.close, name="close"),
            volume=_float(source, mapping.volume, name="volume"),
            event_price=_float(source, mapping.price, name="price"),
            bid=_float(source, mapping.bid, name="bid"),
            ask=_float(source, mapping.ask, name="ask"),
            mid=_float(source, mapping.mid, name="mid"),
        )
        if values.volume is not None and values.volume < 0:
            raise ValueError("volume no puede ser negativo")
        return values

    def _availability_values(
        self,
        source: Mapping[str, Any],
        timestamp: datetime,
        end: datetime,
        values: _ParsedValues,
    ) -> tuple[datetime | None, datetime | None, bool]:
        mapping = self.mapping
        received_raw = source.get(mapping.received_at) if mapping.received_at else None
        available_raw = source.get(mapping.available_at) if mapping.available_at else None
        received_at = (
            _parse_ts(received_raw, self.timezone, self.timestamp_unit) if received_raw not in {None, ""} else None
        )
        available_at = (
            _parse_ts(available_raw, self.timezone, self.timestamp_unit) if available_raw not in {None, ""} else None
        )
        if received_at is not None and received_at < timestamp:
            raise ValueError("received_at no puede preceder al timestamp del registro")
        if available_at is not None and available_at < timestamp:
            raise ValueError("available_at no puede preceder al timestamp del registro")
        closed_flag = _strict_bool(source.get("closed", True), name="closed")
        if values.is_candle and available_at is not None and closed_flag and available_at < end:
            raise ValueError("available_at no puede preceder al cierre de una vela cerrada")
        return received_at, available_at, closed_flag

    @staticmethod
    def _provenance(
        path: Path, file_format: str, row_number: int, mapping: ColumnMapping, timezone: str | None, unit: str
    ) -> dict[str, Any]:
        return {
            "path": str(path),
            "format": file_format,
            "row": row_number,
            "mapping": dataclasses.asdict(mapping),
            "timezone": timezone,
            "timestamp_unit": unit,
            "synthetic": False,
        }

    def _build_record(
        self,
        source: Mapping[str, Any],
        *,
        row_number: int,
        path: Path,
        file_format: str,
        timestamp: datetime,
        end: datetime,
        values: _ParsedValues,
        received_at: datetime | None,
        available_at: datetime | None,
        closed_flag: bool,
    ) -> dict[str, Any]:
        mapping = self.mapping
        provenance = self._provenance(path, file_format, row_number, mapping, self.timezone, self.timestamp_unit)
        if values.is_candle:
            if not self._ohlc_is_coherent(values):
                raise ValueError("OHLC incoherente: low <= open/close <= high requerido")
            if self.price_base not in {"close", "trade"}:
                raise ValueError("una vela OHLC requiere price_base=close o trade; no se intercambia con bid/ask/mid")
            open_value, high_value, low_value, close_value = self._ohlc_values(values)
            return {
                "candle_id": str(source.get(mapping.event_id))
                if mapping.event_id and source.get(mapping.event_id)
                else f"local:{row_number}:{_iso(timestamp)}",
                "instrument": self.instrument,
                "timeframe": self.timeframe,
                "start_ts": _iso(timestamp),
                "end_ts": _iso(end),
                "open": open_value,
                "high": high_value,
                "low": low_value,
                "close": close_value,
                "volume": values.volume,
                "closed": closed_flag,
                "source": self.source or f"local:{path.name}",
                "price_base": "close",
                "quality": "VALIDATED_LOCAL",
                "received_ts": _iso(received_at) if received_at is not None else None,
                "available_ts": _iso(available_at) if available_at is not None else None,
                "source_ordinal": row_number,
                "provenance": provenance,
            }
        choices = {
            "trade": values.event_price,
            "bid": values.bid,
            "ask": values.ask,
            "mid": values.mid,
            "close": values.close_value,
        }
        selected = choices.get(self.price_base)
        if selected is None:
            raise ValueError(f"falta columna de precio explícita para base {self.price_base}")
        return {
            "event_id": str(source.get(mapping.event_id))
            if mapping.event_id and source.get(mapping.event_id)
            else f"local:{row_number}:{_iso(timestamp)}",
            "instrument": self.instrument,
            "event_ts": _iso(timestamp),
            "received_ts": _iso(received_at) if received_at is not None else None,
            "available_ts": _iso(available_at) if available_at is not None else None,
            "kind": str(source.get("kind", "trade")),
            "price": selected,
            "bid": values.bid,
            "ask": values.ask,
            "mid": values.mid,
            "price_base": self.price_base,
            "source": self.source or f"local:{path.name}",
            "source_ordinal": row_number,
            "quality": "VALIDATED_LOCAL",
            "resolution": self.timeframe,
            "provenance": provenance,
        }

    @staticmethod
    def _ohlc_is_coherent(values: _ParsedValues) -> bool:
        open_value, high_value, low_value, close_value = LocalImporter._ohlc_values(values)
        return low_value <= open_value <= high_value and low_value <= close_value <= high_value

    @staticmethod
    def _ohlc_values(values: _ParsedValues) -> tuple[float, float, float, float]:
        if not values.is_candle:
            raise ValueError("OHLC incompleto")
        assert values.open_value is not None
        assert values.high_value is not None
        assert values.low_value is not None
        assert values.close_value is not None
        return values.open_value, values.high_value, values.low_value, values.close_value

    def read(self, path: str | Path, *, fmt: str | None = None) -> ImportResult:
        path = Path(path).expanduser().resolve()
        file_format, rows = self._read_rows(path, fmt)
        issues: list[ImportIssue] = []
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        previous: datetime | None = None
        for row_number, source in enumerate(rows, 1):
            try:
                timestamp, end = self._row_times(source)
                self._check_order(row_number, timestamp, previous, issues)
                previous = timestamp
                # Trades distintos pueden compartir timestamp; para velas,
                # el intervalo sigue siendo la identidad lógica.
                key = self._row_key(source, timestamp)
                self._check_duplicate(row_number, key, seen, issues)
                values = self._numeric_values(source)
                received_at, available_at, closed_flag = self._availability_values(source, timestamp, end, values)
                record = self._build_record(
                    source,
                    row_number=row_number,
                    path=path,
                    file_format=file_format,
                    timestamp=timestamp,
                    end=end,
                    values=values,
                    received_at=received_at,
                    available_at=available_at,
                    closed_flag=closed_flag,
                )
                records.append(record)
            except ImportValidationError:
                raise
            except Exception as exc:
                issue = ImportIssue(row_number, "INVALID_RECORD", str(exc))
                issues.append(issue)
                if self.strict:
                    raise ImportValidationError([issue]) from exc
        coverage_start: str | None
        coverage_end: str | None
        if records:
            starts = [
                datetime.fromisoformat(str(row.get("start_ts", row.get("event_ts"))).replace("Z", "+00:00"))
                for row in records
            ]
            ends = [
                datetime.fromisoformat(str(row.get("end_ts", row.get("event_ts"))).replace("Z", "+00:00"))
                for row in records
            ]
            coverage_start, coverage_end = _iso(min(starts)), _iso(max(ends))
        else:
            coverage_start = coverage_end = None
        return ImportResult(
            records,
            issues,
            file_format,
            self.instrument,
            self.timeframe,
            self.price_base,
            coverage_start,
            coverage_end,
            str(path),
        )
