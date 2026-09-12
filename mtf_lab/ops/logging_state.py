"""Structured JSONL logging and bounded progress state."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class JsonlLogger:
    """Append-only structured logger with an explicit mode marker.

    ``mode`` is always one of LIVE, REPLAY, SYNTHETIC or BACKTEST in normal
    operation.  The logger does not attempt to mask arbitrary user payloads;
    callers should pass only audit-safe fields and never secrets.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        session_id: str | None = None,
        mode: str = "REPLAY",
        component: str = "mtf-lab",
        flush: bool = True,
    ):
        self.path = Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.session_id = session_id
        self.mode = str(mode).upper()
        self.component = component
        self.flush = flush
        self._lock = threading.RLock()
        self._stream: TextIO | None = None
        self._closed = False

    def _ensure(self) -> TextIO:
        if self._closed:
            raise RuntimeError("logger is closed")
        if self._stream is None:
            self._stream = self.path.open("a", encoding="utf-8")
        return self._stream

    def log(self, level: str, event: str, **fields: Any) -> dict[str, Any]:
        record: dict[str, Any] = {
            "ts": _now(),
            "level": str(level).upper(),
            "event": str(event),
            "mode": self.mode,
            "component": self.component,
        }
        if self.session_id is not None:
            record["session_id"] = self.session_id
        # Avoid accidental mutation after serialization and provide a stable
        # local event identifier for correlating progress output.
        record["log_id"] = uuid.uuid4().hex
        record.update(fields)
        line = json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)
        with self._lock:
            stream = self._ensure()
            stream.write(line + "\n")
            if self.flush:
                stream.flush()
        return record

    def info(self, event: str, **fields: Any) -> dict[str, Any]:
        return self.log("INFO", event, **fields)

    def warning(self, event: str, **fields: Any) -> dict[str, Any]:
        return self.log("WARNING", event, **fields)

    def error(self, event: str, **fields: Any) -> dict[str, Any]:
        return self.log("ERROR", event, **fields)

    def exception(self, event: str, exc: BaseException, **fields: Any) -> dict[str, Any]:
        return self.error(event, error_type=type(exc).__name__, error=str(exc), **fields)

    def close(self) -> None:
        with self._lock:
            if self._stream is not None:
                self._stream.flush()
                self._stream.close()
                self._stream = None
            self._closed = True

    def __enter__(self) -> JsonlLogger:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


class ProgressState:
    """Small mutable status snapshot suitable for terminal and UI display."""

    def __init__(self, *, mode: str, session_id: str | None = None, instrument: str | None = None):
        self._lock = threading.RLock()
        self._values: dict[str, Any] = {
            "mode": str(mode).upper(),
            "session_id": session_id,
            "instrument": instrument,
            "connection": "UNKNOWN",
            "last_received_ts": None,
            "last_available_ts": None,
            "feed_delay_ms": None,
            "warmup_pending": {},
            "coverage": {},
            "events_processed": 0,
            "candles_processed": 0,
            "signals": 0,
            "discards": 0,
            "errors": 0,
            "compute_ms": 0.0,
            "continuity": "UNKNOWN",
            "data_quality": "UNKNOWN",
            "started_at": _now(),
            "updated_at": _now(),
        }

    def update(self, **values: Any) -> dict[str, Any]:
        with self._lock:
            self._values.update(values)
            self._values["updated_at"] = _now()
            return dict(self._values)

    def increment(self, field: str, amount: int | float = 1) -> dict[str, Any]:
        with self._lock:
            current = self._values.get(field, 0)
            if not isinstance(current, (int, float)):
                raise TypeError(f"progress field {field!r} is not numeric")
            self._values[field] = current + amount
            self._values["updated_at"] = _now()
            return dict(self._values)

    def merge_nested(self, field: str, values: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            current = self._values.setdefault(field, {})
            if not isinstance(current, dict):
                current = {}
                self._values[field] = current
            current.update(values)
            self._values["updated_at"] = _now()
            return dict(self._values)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            # JSON round-trip gives callers an independent nested snapshot.
            decoded: object = json.loads(json.dumps(self._values, default=str))
            if not isinstance(decoded, dict):
                raise TypeError("progress snapshot must decode to an object")
            return {str(key): value for key, value in decoded.items()}

    def as_line(self) -> str:
        snap = self.snapshot()
        warm = snap.get("warmup_pending") or {}
        cov = snap.get("coverage") or {}
        return (
            f"[{snap.get('mode')}] conexión={snap.get('connection')} "
            f"eventos={snap.get('events_processed', 0)} velas={snap.get('candles_processed', 0)} "
            f"señales={snap.get('signals', 0)} descartes={snap.get('discards', 0)} "
            f"errores={snap.get('errors', 0)} calentamiento={warm} cobertura={cov}"
        )


class ProgressReporter:
    """Rate-limited visible terminal progress; never replaces JSONL logs."""

    def __init__(self, state: ProgressState, *, stream: TextIO | None = None, interval: float = 1.0):
        self.state = state
        self.stream = stream or sys.stderr
        self.interval = max(0.0, float(interval))
        self._last = 0.0
        self._lock = threading.RLock()

    def emit(self, *, force: bool = False) -> bool:
        now = time.monotonic()
        with self._lock:
            if not force and now - self._last < self.interval:
                return False
            self._last = now
            self.stream.write(self.state.as_line() + "\n")
            self.stream.flush()
            return True


class OperationTelemetry:
    """Convenience bundle for logs plus status snapshots."""

    def __init__(
        self,
        *,
        log_path: str | os.PathLike[str],
        mode: str,
        session_id: str | None = None,
        instrument: str | None = None,
        stream: TextIO | None = None,
    ):
        self.state = ProgressState(mode=mode, session_id=session_id, instrument=instrument)
        self.logger = JsonlLogger(log_path, session_id=session_id, mode=mode)
        self.reporter = ProgressReporter(self.state, stream=stream)

    def event(self, name: str, *, level: str = "INFO", progress: bool = True, **fields: Any) -> dict[str, Any]:
        record = self.logger.log(level, name, **fields)
        if progress:
            self.reporter.emit()
        return record

    def close(self) -> None:
        self.reporter.emit(force=True)
        self.logger.close()

    def __enter__(self) -> OperationTelemetry:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
