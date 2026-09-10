"""Durable, idempotent SQLite storage for MTF Lab runs.

The database is an audit log rather than a cache of only the latest values.
Events and candles therefore have stable identities, while corrected candles
are stored as revisions.  A correction never updates a previously recorded
decision or signal.  All mutating operations are transactional and use
``INSERT ... ON CONFLICT DO NOTHING`` so a restarted replay can safely submit
the same input again.

Only the Python standard library is required.  The helpers intentionally
accept mappings *and* dataclass/object records; this is the integration seam
between this operational layer and the core/data packages.
"""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import json
import os
import sqlite3
import threading
import uuid
from collections.abc import Iterable, Iterator, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 2


class IdempotencyConflict(RuntimeError):
    """Misma identidad persistente con contenido/configuración distinta."""


def _mapping(value: Any) -> dict[str, Any]:
    """Return a shallow JSON-friendly mapping for a record-like value."""

    if isinstance(value, Mapping):
        return dict(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    if hasattr(value, "model_dump"):
        try:
            result = value.model_dump()
            if isinstance(result, Mapping):
                return dict(result)
        except Exception:  # pragma: no cover - defensive adapter boundary
            pass
    if hasattr(value, "__dict__"):
        return {
            key: val
            for key, val in vars(value).items()
            if not key.startswith("_")
        }
    raise TypeError(f"record must be a mapping/dataclass/object, got {type(value)!r}")


def _get(value: Any, *names: str, default: Any = None) -> Any:
    record = _mapping(value)
    for name in names:
        if name in record and record[name] is not None:
            return record[name]
    return default


def canonical_json(value: Any) -> str:
    """Serialize a value deterministically for hashes and audit payloads."""

    def default(obj: Any) -> Any:
        if isinstance(obj, datetime):
            return utc_iso(obj)
        if isinstance(obj, Path):
            return str(obj)
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return {field.name: getattr(obj, field.name) for field in dataclasses.fields(obj)}
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return str(obj)

    return json.dumps(value, default=default, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def utc_iso(value: Any = None) -> str:
    """Normalize timestamps to an explicit UTC ISO-8601 string.

    Naive datetimes are treated as UTC only at this low-level storage seam;
    importers should reject a naive source timestamp unless a timezone was
    explicitly supplied in their configuration.
    """

    if value is None:
        dt = datetime.now(UTC)
    elif isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), UTC)
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            # Keep a clear, stable failure instead of silently storing a
            # provider-local or malformed timestamp.
            raise ValueError(f"invalid timestamp: {value!r}") from None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _json_load(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


class SQLiteStore:
    """Versioned SQLite store used by replay, observation and backtests."""

    def __init__(self, path: str | os.PathLike[str], *, read_only: bool = False, timeout: float = 30.0):
        self.path = Path(path).expanduser()
        self.read_only = read_only
        self._lock = threading.RLock()
        if read_only:
            uri = f"file:{self.path.resolve()}?mode=ro"
            self.conn = sqlite3.connect(uri, uri=True, timeout=timeout, check_same_thread=False)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(self.path, timeout=timeout, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")
        if not read_only:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self._migrate()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def __enter__(self) -> "SQLiteStore":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    @contextlib.contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        if self.read_only:
            raise RuntimeError("read-only SQLiteStore cannot start a write transaction")
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield self.conn
            except BaseException:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()

    def _migrate(self) -> None:
        with self._lock:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            current = self.conn.execute(
                "SELECT value FROM schema_meta WHERE key='schema_version'"
            ).fetchone()
            version = int(current[0]) if current else 0
            if version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"database schema {version} is newer than supported {SCHEMA_VERSION}"
                )
            if version < 1:
                self._create_v1()
                version = 1
                self.conn.execute(
                    "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(version),),
                )
                self.conn.commit()
            if version < 2:
                self._migrate_v2()
                self.conn.execute(
                    "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(SCHEMA_VERSION),),
                )
                self.conn.commit()

    def _create_v1(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                started_at TEXT,
                ended_at TEXT,
                status TEXT NOT NULL,
                mode TEXT NOT NULL CHECK(mode IN ('LIVE','REPLAY','SYNTHETIC','BACKTEST')),
                provider TEXT NOT NULL,
                instrument TEXT NOT NULL,
                code_version TEXT,
                seed INTEGER,
                dataset_ref TEXT,
                config_hash TEXT,
                config_json TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS configs (
                config_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                name TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                config_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(session_id, name, config_hash)
            );
            CREATE TABLE IF NOT EXISTS events (
                event_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                event_id TEXT NOT NULL,
                source TEXT NOT NULL,
                instrument TEXT NOT NULL,
                event_ts TEXT NOT NULL,
                received_ts TEXT,
                available_ts TEXT,
                kind TEXT NOT NULL,
                price_base TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                is_correction INTEGER NOT NULL DEFAULT 0,
                UNIQUE(session_id, event_id)
            );
            CREATE INDEX IF NOT EXISTS events_session_time ON events(session_id, event_ts);
            CREATE TABLE IF NOT EXISTS candles (
                candle_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                candle_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT NOT NULL,
                available_ts TEXT,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL,
                closed INTEGER NOT NULL,
                source TEXT NOT NULL,
                price_base TEXT NOT NULL,
                quality TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                provenance_json TEXT NOT NULL,
                UNIQUE(session_id, candle_id)
            );
            CREATE INDEX IF NOT EXISTS candles_lookup ON candles(session_id, instrument, timeframe, start_ts, revision);
            CREATE TABLE IF NOT EXISTS decisions (
                decision_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                decision_id TEXT NOT NULL,
                observed_ts TEXT NOT NULL,
                available_ts TEXT,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, decision_id)
            );
            CREATE INDEX IF NOT EXISTS decisions_session_time ON decisions(session_id, observed_ts);
            CREATE TABLE IF NOT EXISTS signals (
                signal_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                signal_id TEXT NOT NULL,
                episode_id TEXT,
                detected_ts TEXT NOT NULL,
                available_ts TEXT,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, signal_id)
            );
            CREATE INDEX IF NOT EXISTS signals_lookup ON signals(session_id, detected_ts);
            CREATE TABLE IF NOT EXISTS discards (
                discard_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                discard_id TEXT NOT NULL,
                decision_id TEXT,
                observed_ts TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                required INTEGER NOT NULL,
                condition_status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, discard_id)
            );
            CREATE INDEX IF NOT EXISTS discards_reason ON discards(session_id, reason_code);
            CREATE TABLE IF NOT EXISTS simulations (
                simulation_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                simulation_id TEXT NOT NULL,
                signal_id TEXT,
                simulation_type TEXT NOT NULL,
                horizon_seconds REAL NOT NULL,
                direction TEXT NOT NULL,
                detected_ts TEXT NOT NULL,
                entry_ts TEXT,
                expiry_ts TEXT NOT NULL,
                entry_price REAL,
                final_price REAL,
                outcome TEXT NOT NULL,
                stake REAL NOT NULL,
                net_result REAL,
                price_base TEXT NOT NULL,
                quality TEXT NOT NULL,
                resolution TEXT NOT NULL,
                assumptions_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(session_id, simulation_id)
            );
            CREATE INDEX IF NOT EXISTS simulations_lookup ON simulations(session_id, detected_ts);
            CREATE TABLE IF NOT EXISTS checkpoints (
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                checkpoint_name TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                cursor_json TEXT NOT NULL,
                events_processed INTEGER NOT NULL DEFAULT 0,
                last_event_id TEXT,
                state_json TEXT NOT NULL,
                PRIMARY KEY(session_id, checkpoint_name)
            );
            CREATE TABLE IF NOT EXISTS metrics (
                metric_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                metric_name TEXT NOT NULL,
                segment_json TEXT NOT NULL,
                value REAL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(session_id, metric_name, segment_json)
            );
            """
        )


    def _migrate_v2(self) -> None:
        """Add analysis/variant lineage without rewriting existing captures."""
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS analyses (
                analysis_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(session_id),
                dataset_hash TEXT NOT NULL,
                config_hash TEXT NOT NULL,
                code_version TEXT,
                variant TEXT NOT NULL,
                contract_hash TEXT,
                partition TEXT NOT NULL DEFAULT 'all',
                status TEXT NOT NULL DEFAULT 'COMPLETED',
                created_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                UNIQUE(session_id,dataset_hash,config_hash,variant,contract_hash,partition)
            );
            CREATE INDEX IF NOT EXISTS analyses_session ON analyses(session_id,created_at);
            """
        )
        existing_candles = {str(row[1]) for row in self.conn.execute("PRAGMA table_info(candles)")}
        if "payload_json" not in existing_candles:
            self.conn.execute("ALTER TABLE candles ADD COLUMN payload_json TEXT NOT NULL DEFAULT '{}'")
        for table in ("decisions", "discards", "signals", "simulations"):
            existing = {str(row[1]) for row in self.conn.execute(f"PRAGMA table_info({table})")}
            for column in ("analysis_id", "variant", "analysis_config_hash", "contract_hash", "partition"):
                if column not in existing:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} TEXT")
            self.conn.execute(f"CREATE INDEX IF NOT EXISTS {table}_analysis ON {table}(session_id,analysis_id)")
        self.conn.commit()

    @property
    def schema_version(self) -> int:
        if not self._table_exists("schema_meta"):
            return 0
        row = self.conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        return int(row[0]) if row else 0

    def create_session(
        self,
        *,
        session_id: str | None = None,
        mode: str,
        provider: str,
        instrument: str,
        config: Mapping[str, Any] | None = None,
        code_version: str | None = None,
        seed: int | None = None,
        dataset_ref: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        status: str = "RUNNING",
        started_at: Any = None,
    ) -> str:
        mode = str(mode).upper()
        if mode not in {"LIVE", "REPLAY", "SYNTHETIC", "BACKTEST"}:
            raise ValueError(f"unsupported session mode: {mode}")
        session_id = session_id or str(uuid.uuid4())
        config = dict(config or {})
        metadata = dict(metadata or {})
        created = utc_iso()
        config_text = canonical_json(config)
        with self.transaction(immediate=True) as conn:
            conn.execute(
                """INSERT INTO sessions(
                    session_id,created_at,started_at,status,mode,provider,instrument,
                    code_version,seed,dataset_ref,config_hash,config_json,metadata_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id) DO NOTHING""",
                (
                    session_id, created, utc_iso(started_at) if started_at else created,
                    status, mode, str(provider), str(instrument), code_version, seed,
                    dataset_ref, payload_hash(config), config_text, canonical_json(metadata),
                ),
            )
        return session_id

    def finish_session(self, session_id: str, *, status: str = "COMPLETED", ended_at: Any = None) -> None:
        with self.transaction(immediate=True) as conn:
            conn.execute(
                "UPDATE sessions SET status=?, ended_at=? WHERE session_id=?",
                (status, utc_iso(ended_at), session_id),
            )

    def save_config(self, session_id: str, name: str, config: Mapping[str, Any]) -> str:
        config_id = f"{session_id}:{name}:{payload_hash(config)[:16]}"
        text = canonical_json(config)
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO configs(config_id,session_id,name,config_hash,config_json,created_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(session_id,name,config_hash) DO NOTHING""",
                (config_id, session_id, name, payload_hash(config), text, utc_iso()),
            )
        return config_id

    def _event_identity(self, event: Any, *, ordinal: int | None = None) -> tuple[str, dict[str, Any]]:
        record = _mapping(event)
        explicit = _get(record, "event_id", "id", "trade_id", "uid", "source_event_id", "data_id")
        if explicit is None:
            # Importers should provide source_ordinal/source_seq.  Including
            # the full payload and ordinal prevents accidental de-duplication
            # of two equal-price trades at the same source timestamp.
            explicit = f"generated:{ordinal if ordinal is not None else record.get('source_ordinal', '')}:{payload_hash(record)}"
        return str(explicit), record


    def _check_idempotency(self, table: str, identity_columns: tuple[str, ...], identity_values: tuple[Any, ...], payload: str) -> bool:
        where = " AND ".join(f"{column}=?" for column in identity_columns)
        row = self.conn.execute(f"SELECT payload_json FROM {table} WHERE {where}", identity_values).fetchone()
        if row is None:
            return False
        if str(row[0]) != payload:
            raise IdempotencyConflict(f"conflicto de idempotencia en {table}: identidad={identity_values!r} ya tiene contenido distinto")
        return True

    def create_analysis(
        self,
        session_id: str,
        *,
        dataset_hash: str,
        config_hash: str,
        variant: str,
        contract_hash: str | None = None,
        partition: str = "all",
        code_version: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        analysis_id: str | None = None,
        status: str = "COMPLETED",
    ) -> str:
        """Create/reuse one immutable analysis identity for a capture."""
        if not dataset_hash or not config_hash or not variant:
            raise ValueError("dataset_hash, config_hash y variant son obligatorios")
        partition = str(partition or "all")
        identity = {"session_id": session_id, "dataset_hash": str(dataset_hash), "config_hash": str(config_hash), "variant": str(variant), "contract_hash": str(contract_hash or ""), "partition": partition}
        analysis_id = analysis_id or "an_" + payload_hash(identity)[:32]
        metadata_text = canonical_json(metadata or {})
        with self.transaction(immediate=True) as conn:
            existing = conn.execute("SELECT session_id,dataset_hash,config_hash,variant,contract_hash,partition,metadata_json FROM analyses WHERE analysis_id=?", (analysis_id,)).fetchone()
            if existing is not None:
                expected = (session_id, str(dataset_hash), str(config_hash), str(variant), contract_hash, partition)
                actual = tuple(existing[:6])
                if actual != expected:
                    raise IdempotencyConflict(f"analysis_id {analysis_id} ya existe con identidad distinta")
                return analysis_id
            conn.execute("""INSERT INTO analyses(analysis_id,session_id,dataset_hash,config_hash,code_version,variant,contract_hash,partition,status,created_at,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", (analysis_id, session_id, str(dataset_hash), str(config_hash), code_version, str(variant), contract_hash, partition, status, utc_iso(), metadata_text))
        return analysis_id

    def get_analysis(self, analysis_id: str) -> dict[str, Any] | None:
        if not self._table_exists("analyses"):
            return None
        rows = self._rows("SELECT * FROM analyses WHERE analysis_id=?", (analysis_id,))
        if not rows: return None
        row = rows[0]; row["metadata"] = _json_load(row.pop("metadata_json"), {}); return row

    def analyses(self, session_id: str | None = None, *, limit: int = 100) -> list[dict[str, Any]]:
        if not self._table_exists("analyses"):
            return []
        if session_id is None:
            rows = self._rows("SELECT * FROM analyses ORDER BY created_at DESC LIMIT ?", (int(limit),))
        else:
            rows = self._rows("SELECT * FROM analyses WHERE session_id=? ORDER BY created_at DESC LIMIT ?", (session_id, int(limit)))
        for row in rows:
            row["metadata"] = _json_load(row.pop("metadata_json", None), {})
        return rows

    def save_event(self, session_id: str, event: Any, *, ordinal: int | None = None) -> bool:
        event_id, record = self._event_identity(event, ordinal=ordinal)
        payload = canonical_json(record)
        values = (
            session_id,
            event_id,
            str(_get(record, "source", "provider", default="unknown")),
            str(_get(record, "instrument", "symbol", default="unknown")),
            utc_iso(_get(record, "event_ts", "event_time", "timestamp", "ts", "time")),
            utc_iso(_get(record, "received_ts", "received_at", default=None)) if _get(record, "received_ts", "received_at") is not None else None,
            utc_iso(_get(record, "available_ts", "available_at", default=None)) if _get(record, "available_ts", "available_at") is not None else None,
            str(_get(record, "kind", "event_kind", "type", default="event")),
            str(_get(record, "price_base", "price_basis", "price_type", "base_price", default="unknown")),
            payload,
            int(bool(_get(record, "is_correction", "correction", default=False))),
        )
        with self.transaction() as conn:
            if self._check_idempotency("events", ("session_id", "event_id"), (session_id, event_id), payload):
                return False
            cur = conn.execute(
                """INSERT INTO events(
                session_id,event_id,source,instrument,event_ts,received_ts,available_ts,
                kind,price_base,payload_json,is_correction
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id,event_id) DO NOTHING""",
                values,
            )
        return cur.rowcount == 1

    def save_candle(self, session_id: str, candle: Any, *, ordinal: int | None = None) -> bool:
        record = _mapping(candle)
        explicit = _get(record, "candle_id", "data_id", "source_record_id", "id", "uid")
        start = utc_iso(_get(record, "start_ts", "interval_start", "start", "open_time", "timestamp", "ts"))
        end = utc_iso(_get(record, "end_ts", "interval_end", "end", "close_time"))
        timeframe_value = _get(record, "timeframe", "resolution", "resolution_seconds", "interval")
        if isinstance(timeframe_value, (int, float)) and float(timeframe_value) >= 60 and float(timeframe_value) % 60 == 0:
            timeframe = f"M{int(float(timeframe_value) // 60)}"
        else:
            timeframe = str(timeframe_value)
        revision = int(_get(record, "revision", default=0))
        if explicit is None:
            explicit = f"candle:{_get(record, 'instrument','symbol',default='unknown')}:{timeframe}:{start}:r{revision}:{ordinal if ordinal is not None else ''}"
        values = (
            session_id,
            str(explicit),
            str(_get(record, "instrument", "symbol", default="unknown")),
            timeframe,
            start,
            end,
            utc_iso(_get(record, "available_ts", "available_at")) if _get(record, "available_ts", "available_at") is not None else None,
            float(_get(record, "open", "o")),
            float(_get(record, "high", "h")),
            float(_get(record, "low", "l")),
            float(_get(record, "close", "c")),
            float(_get(record, "volume", "v")) if _get(record, "volume", "v") is not None else None,
            int(bool(_get(record, "closed", "is_closed", default=True))),
            str(_get(record, "source", "provider", default="unknown")),
            str(_get(record, "price_base", "price_basis", "price_type", default="unknown")),
            str(_get(record, "quality", "data_quality", default="UNKNOWN")),
            revision,
            canonical_json(_get(record, "provenance", "provenance_json", default=record)),
            canonical_json(record),
        )
        payload = canonical_json(record)
        with self.transaction() as conn:
            if self._check_idempotency("candles", ("session_id", "candle_id"), (session_id, str(explicit)), payload):
                return False
            cur = conn.execute(
                """INSERT INTO candles(
                session_id,candle_id,instrument,timeframe,start_ts,end_ts,available_ts,
                open,high,low,close,volume,closed,source,price_base,quality,revision,provenance_json,payload_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id,candle_id) DO NOTHING""",
                values,
            )
        return cur.rowcount == 1

    def save_decision(self, session_id: str, decision: Any, *, ordinal: int | None = None, analysis_id: str | None = None, variant: str | None = None, analysis_config_hash: str | None = None, contract_hash: str | None = None, partition: str | None = None) -> bool:
        record = _mapping(decision)
        analysis_id = analysis_id or _get(record, "analysis_id")
        variant = variant or _get(record, "variant", "variant_name")
        analysis_config_hash = analysis_config_hash or _get(record, "analysis_config_hash", "config_hash")
        contract_hash = contract_hash or _get(record, "contract_hash")
        partition = partition or _get(record, "partition")
        decision_id = _get(record, "decision_id", "id", "uid")
        if decision_id is None:
            decision_id = f"decision:{_get(record,'observed_ts','timestamp','ts')}:{ordinal if ordinal is not None else payload_hash(record)[:16]}"
        observed = utc_iso(_get(record, "observed_ts", "observed_at", "timestamp", "available_at", "ts"))
        values = (
            session_id, str(decision_id), observed,
            utc_iso(_get(record, "available_ts", "available_at")) if _get(record, "available_ts", "available_at") is not None else None,
            str(_get(record, "kind", "type", "stage", default="strategy_evaluation")),
            str(_get(record, "status", "decision", default="UNKNOWN")), canonical_json(record),
        )
        payload = canonical_json(record)
        with self.transaction() as conn:
            if self._check_idempotency("decisions", ("session_id", "decision_id"), (session_id, str(decision_id)), payload):
                return False
            cur = conn.execute(
                """INSERT INTO decisions(session_id,decision_id,observed_ts,available_ts,kind,status,payload_json,analysis_id,variant,analysis_config_hash,contract_hash,partition)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                (*values, analysis_id, variant, analysis_config_hash, contract_hash, partition),
            )
        return cur.rowcount == 1

    def save_signal(self, session_id: str, signal: Any, *, ordinal: int | None = None, analysis_id: str | None = None, variant: str | None = None, analysis_config_hash: str | None = None, contract_hash: str | None = None, partition: str | None = None) -> bool:
        record = _mapping(signal)
        analysis_id = analysis_id or _get(record, "analysis_id")
        variant = variant or _get(record, "variant", "variant_name")
        analysis_config_hash = analysis_config_hash or _get(record, "analysis_config_hash", "config_hash")
        contract_hash = contract_hash or _get(record, "contract_hash")
        partition = partition or _get(record, "partition")
        signal_id = _get(record, "signal_id", "id", "uid")
        if signal_id is None:
            signal_id = f"signal:{_get(record,'detected_ts','detected_at','timestamp','ts')}:{_get(record,'direction','side',default='UNKNOWN')}:{ordinal if ordinal is not None else payload_hash(record)[:16]}"
        detected = utc_iso(_get(record, "detected_ts", "detected_at", "timestamp", "ts"))
        values = (
            session_id, str(signal_id), _get(record, "episode_id", "episode"), detected,
            utc_iso(_get(record, "available_ts", "available_at")) if _get(record, "available_ts", "available_at") is not None else None,
            str(_get(record, "instrument", "symbol", default="unknown")),
            str(_get(record, "direction", "side", default="UNKNOWN")).upper(),
            str(_get(record, "status", default="VALID")), canonical_json(record),
        )
        payload = canonical_json(record)
        with self.transaction() as conn:
            if self._check_idempotency("signals", ("session_id", "signal_id"), (session_id, str(signal_id)), payload):
                return False
            cur = conn.execute(
                """INSERT INTO signals(session_id,signal_id,episode_id,detected_ts,available_ts,instrument,direction,status,payload_json,analysis_id,variant,analysis_config_hash,contract_hash,partition)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (*values, analysis_id, variant, analysis_config_hash, contract_hash, partition),
            )
        return cur.rowcount == 1

    def save_discard(self, session_id: str, discard: Any, *, ordinal: int | None = None, analysis_id: str | None = None, variant: str | None = None, analysis_config_hash: str | None = None, contract_hash: str | None = None, partition: str | None = None) -> bool:
        record = _mapping(discard)
        analysis_id = analysis_id or _get(record, "analysis_id")
        variant = variant or _get(record, "variant", "variant_name")
        analysis_config_hash = analysis_config_hash or _get(record, "analysis_config_hash", "config_hash")
        contract_hash = contract_hash or _get(record, "contract_hash")
        partition = partition or _get(record, "partition")
        discard_id = _get(record, "discard_id", "id", "uid")
        if discard_id is None:
            discard_id = f"discard:{_get(record,'observed_ts','timestamp','ts')}:{_get(record,'reason_code','reason',default='UNKNOWN')}:{ordinal if ordinal is not None else payload_hash(record)[:16]}"
        observed = utc_iso(_get(record, "observed_ts", "observed_at", "timestamp", "available_at", "ts"))
        values = (
            session_id, str(discard_id), _get(record, "decision_id"), observed,
            str(_get(record, "reason_code", "reason", default="UNKNOWN")),
            int(bool(_get(record, "required", default=True))),
            str(_get(record, "condition_status", "status", default="UNSATISFIED")),
            canonical_json(record),
        )
        payload = canonical_json(record)
        with self.transaction() as conn:
            if self._check_idempotency("discards", ("session_id", "discard_id"), (session_id, str(discard_id)), payload):
                return False
            cur = conn.execute(
                """INSERT INTO discards(session_id,discard_id,decision_id,observed_ts,reason_code,required,condition_status,payload_json,analysis_id,variant,analysis_config_hash,contract_hash,partition)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (*values, analysis_id, variant, analysis_config_hash, contract_hash, partition),
            )
        return cur.rowcount == 1

    def save_simulation(self, session_id: str, simulation: Any, *, ordinal: int | None = None, analysis_id: str | None = None, variant: str | None = None, analysis_config_hash: str | None = None, contract_hash: str | None = None, partition: str | None = None, allow_update: bool = False) -> bool:
        record = _mapping(simulation)
        analysis_id = analysis_id or _get(record, "analysis_id")
        variant = variant or _get(record, "variant", "variant_name")
        analysis_config_hash = analysis_config_hash or _get(record, "analysis_config_hash", "config_hash", "variant_config_hash")
        contract_hash = contract_hash or _get(record, "contract_hash")
        partition = partition or _get(record, "partition")
        sim_id = _get(record, "simulation_id", "id", "uid")
        if sim_id is None:
            sim_id = f"simulation:{_get(record,'signal_id',default='none')}:{_get(record,'horizon_seconds','horizon',default=0)}:{ordinal if ordinal is not None else payload_hash(record)[:16]}"
        detected = utc_iso(_get(record, "detected_ts", "detected_at", "timestamp", "ts"))
        expiry = utc_iso(_get(record, "expiry_ts", "expiry_at", "expires_at", "expiry", default=detected))
        values = (
            session_id, str(sim_id), _get(record, "signal_id"), str(_get(record, "simulation_type", "type", default="DIRECTIONAL")),
            float(_get(record, "horizon_seconds", "horizon", default=0)), str(_get(record, "direction", "side", default="UNKNOWN")).upper(),
            detected, utc_iso(_get(record, "entry_ts", "entry_at")) if _get(record, "entry_ts", "entry_at") is not None else None,
            expiry, float(_get(record, "entry_price")) if _get(record, "entry_price") is not None else None,
            float(_get(record, "final_price")) if _get(record, "final_price") is not None else None,
            str(_get(record, "outcome", default="INDETERMINATE")).upper(), float(_get(record, "stake", default=0)),
            float(_get(record, "net_result")) if _get(record, "net_result") is not None else None,
            str(_get(record, "price_base", "base_price", default="unknown")), str(_get(record, "quality", default="UNKNOWN")),
            str(_get(record, "resolution", default="UNKNOWN")), canonical_json(_get(record, "assumptions", "assumptions_json", default={})), canonical_json(record),
        )
        payload = canonical_json(record)
        with self.transaction() as conn:
            existing = conn.execute("SELECT payload_json FROM simulations WHERE session_id=? AND simulation_id=?", (session_id, str(sim_id))).fetchone()
            if existing is not None:
                if str(existing[0]) == payload:
                    return False
                if not allow_update:
                    raise IdempotencyConflict(f"conflicto de idempotencia en simulations: identidad={(session_id, str(sim_id))!r} ya tiene contenido distinto")
                (_sid, _sim_id, signal_id, simulation_type, horizon, direction, detected_ts, entry_ts, expiry_ts, entry_price, final_price, outcome, stake, net_result, price_base, quality, resolution, assumptions_json, payload_json) = values
                conn.execute("""UPDATE simulations SET signal_id=?,simulation_type=?,horizon_seconds=?,direction=?,detected_ts=?,entry_ts=?,expiry_ts=?,entry_price=?,final_price=?,outcome=?,stake=?,net_result=?,price_base=?,quality=?,resolution=?,assumptions_json=?,payload_json=?,analysis_id=?,variant=?,analysis_config_hash=?,contract_hash=?,partition=? WHERE session_id=? AND simulation_id=?""", (signal_id, simulation_type, horizon, direction, detected_ts, entry_ts, expiry_ts, entry_price, final_price, outcome, stake, net_result, price_base, quality, resolution, assumptions_json, payload_json, analysis_id, variant, analysis_config_hash, contract_hash, partition, session_id, str(sim_id)))
                return True
            cur = conn.execute(
                """INSERT INTO simulations(
                session_id,simulation_id,signal_id,simulation_type,horizon_seconds,direction,detected_ts,entry_ts,expiry_ts,
                entry_price,final_price,outcome,stake,net_result,price_base,quality,resolution,assumptions_json,payload_json,analysis_id,variant,analysis_config_hash,contract_hash,partition
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (*values, analysis_id, variant, analysis_config_hash, contract_hash, partition),
            )
        return cur.rowcount == 1

    def update_simulation(self, session_id: str, simulation: Any, **kwargs: Any) -> bool:
        """Actualiza el ciclo de vida PENDING -> resuelto de una simulación."""
        kwargs["allow_update"] = True
        return self.save_simulation(session_id, simulation, **kwargs)

    def save_checkpoint(
        self,
        session_id: str,
        checkpoint_name: str,
        *,
        cursor: Mapping[str, Any] | None = None,
        events_processed: int = 0,
        last_event_id: str | None = None,
        state: Mapping[str, Any] | None = None,
    ) -> None:
        with self.transaction(immediate=True) as conn:
            conn.execute(
                """INSERT INTO checkpoints(session_id,checkpoint_name,updated_at,cursor_json,events_processed,last_event_id,state_json)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(session_id,checkpoint_name) DO UPDATE SET
                updated_at=excluded.updated_at,cursor_json=excluded.cursor_json,events_processed=excluded.events_processed,
                last_event_id=excluded.last_event_id,state_json=excluded.state_json""",
                (session_id, checkpoint_name, utc_iso(), canonical_json(cursor or {}), int(events_processed), last_event_id, canonical_json(state or {})),
            )

    def save_metric(
        self, session_id: str, metric_name: str, segment: Mapping[str, Any], value: float | None, payload: Mapping[str, Any] | None = None
    ) -> None:
        segment_text = canonical_json(segment)
        with self.transaction() as conn:
            conn.execute(
                """INSERT INTO metrics(session_id,metric_name,segment_json,value,payload_json,created_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(session_id,metric_name,segment_json) DO UPDATE SET value=excluded.value,payload_json=excluded.payload_json,created_at=excluded.created_at""",
                (session_id, metric_name, segment_text, value, canonical_json(payload or {}), utc_iso()),
            )

    def _rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def _table_exists(self, table: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return row is not None

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM sessions WHERE session_id=?", (session_id,))
        if not rows:
            return None
        row = rows[0]
        row["config"] = _json_load(row.pop("config_json"), {})
        row["metadata"] = _json_load(row.pop("metadata_json"), {})
        return row

    def get_checkpoint(self, session_id: str, checkpoint_name: str = "default") -> dict[str, Any] | None:
        rows = self._rows("SELECT * FROM checkpoints WHERE session_id=? AND checkpoint_name=?", (session_id, checkpoint_name))
        if not rows:
            return None
        row = rows[0]
        row["cursor"] = _json_load(row.pop("cursor_json"), {})
        row["state"] = _json_load(row.pop("state_json"), {})
        return row

    def list_candles(self, session_id: str, *, instrument: str | None = None, timeframe: str | None = None, closed_only: bool = False, limit: int | None = None) -> list[dict[str, Any]]:
        clauses = ["session_id=?"]
        params: list[Any] = [session_id]
        if instrument is not None:
            clauses.append("instrument=?"); params.append(instrument)
        if timeframe is not None:
            clauses.append("timeframe=?"); params.append(timeframe)
        if closed_only:
            clauses.append("closed=1")
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(f"SELECT * FROM candles WHERE {' AND '.join(clauses)} ORDER BY start_ts, timeframe, revision{lim}", params)
        for row in rows:
            row["provenance"] = _json_load(row.pop("provenance_json"), {})
        return rows

    def list_events(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(f"SELECT * FROM events WHERE session_id=? ORDER BY event_ts, event_row_id{lim}", (session_id,))
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def list_decisions(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(f"SELECT * FROM decisions WHERE session_id=? ORDER BY observed_ts, decision_row_id{lim}", (session_id,))
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def list_signals(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(f"SELECT * FROM signals WHERE session_id=? ORDER BY detected_ts, signal_row_id{lim}", (session_id,))
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def list_discards(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(f"SELECT * FROM discards WHERE session_id=? ORDER BY observed_ts, discard_row_id{lim}", (session_id,))
        for row in rows:
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def list_simulations(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
        lim = f" LIMIT {int(limit)}" if limit is not None else ""
        rows = self._rows(f"SELECT * FROM simulations WHERE session_id=? ORDER BY detected_ts, simulation_row_id{lim}", (session_id,))
        for row in rows:
            row["assumptions"] = _json_load(row.pop("assumptions_json"), {})
            row["payload"] = _json_load(row.pop("payload_json"), {})
        return rows

    def status(self, session_id: str) -> dict[str, Any]:
        session = self.get_session(session_id) or {"session_id": session_id, "status": "UNKNOWN"}
        counts = {}
        for table in ("events", "candles", "decisions", "signals", "discards", "simulations", "analyses"):
            if not self._table_exists(table):
                counts[table] = 0
                continue
            row = self.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE session_id=?", (session_id,)).fetchone()
            counts[table] = int(row[0])
        # ``counts.signals`` is the primary MTF detector count for terminal
        # compatibility; the complete capture count (including the independent
        # M1 reference variant) remains explicit as ``signals_total``.
        if self._table_exists("signals") and "variant" in {str(row[1]) for row in self.conn.execute("PRAGMA table_info(signals)")}:
            total_signals = counts.get("signals", 0)
            primary = self.conn.execute("SELECT COUNT(*) FROM signals WHERE session_id=? AND (variant IS NULL OR variant <> 'm1_trigger_reference')", (session_id,)).fetchone()[0]
            counts["signals_total"] = total_signals
            counts["signals"] = int(primary)
        latest = self.conn.execute(
            "SELECT MAX(event_ts) FROM events WHERE session_id=?", (session_id,)
        ).fetchone()[0]
        session.update({"counts": counts, "last_event_ts": latest, "schema_version": self.schema_version})
        return session

    def sessions(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return self._rows("SELECT session_id,created_at,started_at,ended_at,status,mode,provider,instrument,code_version,seed,dataset_ref FROM sessions ORDER BY created_at DESC LIMIT ?", (int(limit),))
