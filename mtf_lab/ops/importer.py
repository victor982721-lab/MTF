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
from zoneinfo import ZoneInfo
from typing import Any


@dataclasses.dataclass(frozen=True, slots=True)
class ColumnMapping:
    timestamp: str = "timestamp"
    end_timestamp: str | None = None
    event_timestamp: str | None = None
    open: str = "open"
    high: str = "high"
    low: str = "low"
    close: str = "close"
    volume: str | None = "volume"
    price: str | None = "price"
    bid: str | None = "bid"
    ask: str | None = "ask"
    mid: str | None = "mid"
    event_id: str | None = "event_id"
    received_at: str | None = "received_at"
    available_at: str | None = "available_at"

    @classmethod
    def from_text(cls, text: str | None) -> "ColumnMapping":
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
        self.interval_seconds = float(interval_seconds) if interval_seconds is not None else self._interval_from_timeframe(timeframe)
        self.strict = strict
        self.source = str(source).strip() if source else None
        self.allow_out_of_order = allow_out_of_order
        self.allow_duplicates = allow_duplicates
        if self.price_base not in {"trade", "close", "mid", "bid", "ask"}:
            raise ValueError("price_base must be trade, close, mid, bid or ask")

    @staticmethod
    def _interval_from_timeframe(value: str) -> float | None:
        text = str(value).upper()
        if text.startswith("M") and text[1:].isdigit(): return float(int(text[1:]) * 60)
        if text.startswith("H") and text[1:].isdigit(): return float(int(text[1:]) * 3600)
        return None

    def _read_rows(self, path: Path, fmt: str | None = None) -> tuple[str, list[dict[str, Any]]]:
        fmt = (fmt or path.suffix.lstrip(".")).lower()
        if fmt == "jsonl":
            rows = []
            with path.open(encoding="utf-8") as stream:
                for line_no, line in enumerate(stream, 1):
                    if not line.strip(): continue
                    value = json.loads(line)
                    if not isinstance(value, Mapping): raise ValueError(f"JSONL fila {line_no} no es objeto")
                    rows.append(dict(value))
            return fmt, rows
        if fmt == "json":
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, list): raise ValueError("JSON debe contener una lista de objetos")
            return fmt, [dict(x) for x in value]
        if fmt == "csv":
            with path.open(newline="", encoding="utf-8-sig") as stream:
                return fmt, [dict(row) for row in csv.DictReader(stream)]
        raise ValueError("format must be csv, jsonl or json")

    def read(self, path: str | Path, *, fmt: str | None = None) -> ImportResult:
        path = Path(path).expanduser().resolve()
        file_format, rows = self._read_rows(path, fmt)
        issues: list[ImportIssue] = []
        records: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        previous: datetime | None = None
        mapping = self.mapping
        for row_number, source in enumerate(rows, 1):
            try:
                ts_col = mapping.event_timestamp if (source.get("kind") or source.get("type")) and mapping.event_timestamp else mapping.timestamp
                timestamp = _parse_ts(source.get(ts_col), self.timezone, self.timestamp_unit)
                end_value = source.get(mapping.end_timestamp) if mapping.end_timestamp else None
                end = _parse_ts(end_value, self.timezone, self.timestamp_unit) if end_value not in {None, ""} else (timestamp + timedelta(seconds=self.interval_seconds) if self.interval_seconds else None)
                if end is None or end <= timestamp:
                    raise ValueError("intervalo final ausente o no positivo")
                if previous is not None and timestamp < previous:
                    issue = ImportIssue(row_number, "OUT_OF_ORDER", "timestamp fuera de orden")
                    issues.append(issue)
                    if self.strict and not self.allow_out_of_order: raise ImportValidationError([issue])
                previous = timestamp
                # Trades distintos pueden compartir timestamp; para velas,
                # el intervalo sigue siendo la identidad lógica.
                supplied_id = source.get(mapping.event_id) if mapping.event_id else None
                key_token = (str(supplied_id) if supplied_id not in {None, ""} and not all(source.get(column) not in {None, ""} for column in (mapping.open, mapping.high, mapping.low, mapping.close)) else _iso(timestamp))
                key = (self.instrument, key_token)
                if key in seen:
                    issue = ImportIssue(row_number, "DUPLICATE", "timestamp duplicado para el instrumento")
                    issues.append(issue)
                    if self.strict and not self.allow_duplicates: raise ImportValidationError([issue])
                seen.add(key)
                open_value = _float(source, mapping.open, name="open")
                high_value = _float(source, mapping.high, name="high")
                low_value = _float(source, mapping.low, name="low")
                close_value = _float(source, mapping.close, name="close")
                volume = _float(source, mapping.volume, name="volume")
                if volume is not None and volume < 0:
                    raise ValueError("volume no puede ser negativo")
                event_price = _float(source, mapping.price, name="price")
                bid = _float(source, mapping.bid, name="bid")
                ask = _float(source, mapping.ask, name="ask")
                mid = _float(source, mapping.mid, name="mid")
                received_raw = source.get(mapping.received_at) if mapping.received_at else None
                available_raw = source.get(mapping.available_at) if mapping.available_at else None
                received_at = _parse_ts(received_raw, self.timezone, self.timestamp_unit) if received_raw not in {None, ""} else None
                available_at = _parse_ts(available_raw, self.timezone, self.timestamp_unit) if available_raw not in {None, ""} else None
                if received_at is not None and received_at < timestamp:
                    raise ValueError("received_at no puede preceder al timestamp del registro")
                if available_at is not None and available_at < timestamp:
                    raise ValueError("available_at no puede preceder al timestamp del registro")
                is_candle = all(x is not None for x in (open_value, high_value, low_value, close_value))
                closed_flag = _strict_bool(source.get("closed", True), name="closed")
                if is_candle and available_at is not None and closed_flag and available_at < end:
                    raise ValueError("available_at no puede preceder al cierre de una vela cerrada")
                if is_candle:
                    if not (low_value <= open_value <= high_value and low_value <= close_value <= high_value):
                        raise ValueError("OHLC incoherente: low <= open/close <= high requerido")
                    if self.price_base not in {"close", "trade"}:
                        raise ValueError("una vela OHLC requiere price_base=close o trade; no se intercambia con bid/ask/mid")
                    record: dict[str, Any] = {
                        "candle_id": str(source.get(mapping.event_id)) if mapping.event_id and source.get(mapping.event_id) else f"local:{row_number}:{_iso(timestamp)}",
                        "instrument": self.instrument, "timeframe": self.timeframe, "start_ts": _iso(timestamp), "end_ts": _iso(end),
                        "open": open_value, "high": high_value, "low": low_value, "close": close_value, "volume": volume,
                        "closed": closed_flag, "source": self.source or f"local:{path.name}", "price_base": "close", "quality": "VALIDATED_LOCAL", "received_ts": _iso(received_at) if received_at is not None else None, "available_ts": _iso(available_at) if available_at is not None else None,
                        "source_ordinal": row_number,
                        "provenance": {"path": str(path), "format": file_format, "row": row_number, "mapping": dataclasses.asdict(mapping), "timezone": self.timezone, "timestamp_unit": self.timestamp_unit, "synthetic": False},
                    }
                else:
                    choices = {"trade": event_price, "bid": bid, "ask": ask, "mid": mid, "close": close_value}
                    selected = choices.get(self.price_base)
                    if selected is None:
                        raise ValueError(f"falta columna de precio explícita para base {self.price_base}")
                    record = {
                        "event_id": str(source.get(mapping.event_id)) if mapping.event_id and source.get(mapping.event_id) else f"local:{row_number}:{_iso(timestamp)}",
                        "instrument": self.instrument, "event_ts": _iso(timestamp), "received_ts": _iso(received_at) if received_at is not None else None, "available_ts": _iso(available_at) if available_at is not None else None, "kind": str(source.get("kind", "trade")),
                        "price": selected, "bid": bid, "ask": ask, "mid": mid, "price_base": self.price_base, "source": self.source or f"local:{path.name}", "source_ordinal": row_number,
                        "quality": "VALIDATED_LOCAL", "resolution": self.timeframe,
                        "provenance": {"path": str(path), "format": file_format, "row": row_number, "mapping": dataclasses.asdict(mapping), "timezone": self.timezone, "timestamp_unit": self.timestamp_unit, "synthetic": False},
                    }
                records.append(record)
            except ImportValidationError:
                raise
            except Exception as exc:
                issue = ImportIssue(row_number, "INVALID_RECORD", str(exc))
                issues.append(issue)
                if self.strict:
                    raise ImportValidationError([issue]) from exc
        if records:
            starts = [datetime.fromisoformat(str(row.get("start_ts", row.get("event_ts"))).replace("Z", "+00:00")) for row in records]
            ends = [datetime.fromisoformat(str(row.get("end_ts", row.get("event_ts"))).replace("Z", "+00:00")) for row in records]
            coverage_start, coverage_end = _iso(min(starts)), _iso(max(ends))
        else:
            coverage_start = coverage_end = None
        return ImportResult(records, issues, file_format, self.instrument, self.timeframe, self.price_base, coverage_start, coverage_end, str(path))
