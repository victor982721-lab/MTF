"""Bounded free-web acquisition for the exact Dukascopy EUR/USD pilot week.

Only the public Historical Data Export configuration and the first-party JETTA
JSON paths observed in that widget are used.  No login, cookies, proxy,
account, OAuth, payment, Java runtime, or third-party downloader is involved.
The reader and manifest contract live in :mod:`mtf_lab.data.dukascopy`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import OpenerDirector, ProxyHandler, Request, build_opener

from .dukascopy import (
    DUKASCOPY_CONFIG_URI,
    DUKASCOPY_DEFAULT_DATA_ROOT,
    DUKASCOPY_FORMAT,
    DUKASCOPY_INSTRUMENT,
    DUKASCOPY_INTERVAL_CODE,
    DUKASCOPY_MAX_BYTES,
    DUKASCOPY_NORMALIZATION_VERSION,
    DUKASCOPY_PROVIDER,
    DUKASCOPY_SOURCE_TIMEZONE,
    DUKASCOPY_SOURCE_URI,
    DUKASCOPY_SYMBOL,
    DUKASCOPY_TERMS_URI,
    DUKASCOPY_WIDGET_URI,
    DUKASCOPY_WINDOW_END,
    DUKASCOPY_WINDOW_START,
    DukascopyError,
    DukascopyManifest,
    DukascopyPartition,
    decode_tick_payload,
    manifest_content_hash,
    validate_manifest,
)
from .historical import DEFAULT_DATA_ROOT
from .storage_budget import StorageBudgetError, acquisition_lock, guard_budget, shared_lock_root

MANIFEST_NAME = "dukascopy-eurusd-20160307-20160314.json"
RECEIPT_NAME = "dukascopy-eurusd-20160307-20160314.json"
MAX_RESPONSE_BYTES = DUKASCOPY_MAX_BYTES
PAGE_LIMIT = 2 * 1024 * 1024
CHUNK_SIZE = 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 30.0
USER_AGENT = "mtf-lab-dukascopy/1"
TICK_PATH = "/ticks/EUR-USD/{year}/{month}/{day}/{hour}"


class AcquisitionError(DukascopyError):
    """The bounded public Dukascopy acquisition cannot continue safely."""


@dataclass(frozen=True, slots=True)
class AcquisitionReceipt:
    """Durable outcome, including partial-access evidence without raw secrets."""

    status: str
    provider: str
    instrument: str
    window_start: datetime
    window_end: datetime
    data_root: Path
    manifest_path: Path | None
    receipt_path: Path
    api_base: str | None
    metadata_uri: str | None
    attempted_hours: int
    completed_hours: int
    quote_count: int
    raw_bytes: int
    coverage_start: datetime | None
    coverage_end: datetime | None
    source_timezone: str
    network_performed: bool
    failures: tuple[str, ...] = ()
    provider_authorization_ref: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "provider": self.provider,
            "instrument": self.instrument,
            "window_start": _iso(self.window_start),
            "window_end": _iso(self.window_end),
            "data_root": str(self.data_root),
            "manifest_path": str(self.manifest_path) if self.manifest_path is not None else None,
            "receipt_path": str(self.receipt_path),
            "api_base": self.api_base,
            "metadata_uri": self.metadata_uri,
            "interval_code": DUKASCOPY_INTERVAL_CODE,
            "format": DUKASCOPY_FORMAT,
            "source_timezone": self.source_timezone,
            "availability_basis": "historical_event_time",
            "provider_authorization_ref": self.provider_authorization_ref,
            "automation_authorization_required": True,
            "attempted_hours": self.attempted_hours,
            "completed_hours": self.completed_hours,
            "quote_count": self.quote_count,
            "raw_bytes": self.raw_bytes,
            "coverage_start": _iso(self.coverage_start),
            "coverage_end": _iso(self.coverage_end),
            "network_performed": self.network_performed,
            "failures": list(self.failures),
        }


@dataclass(frozen=True, slots=True)
class _WeekDownload:
    partitions: tuple[DukascopyPartition, ...]
    attempted_hours: int
    completed_hours: int
    quote_count: int
    raw_bytes: int
    coverage_start: datetime | None
    coverage_end: datetime | None
    failures: tuple[str, ...]


@contextmanager
def _writer_lock(root: Path) -> Iterator[None]:
    try:
        with acquisition_lock(root, lock_root=shared_lock_root(root, provider=DUKASCOPY_PROVIDER)):
            yield
    except StorageBudgetError as exc:
        raise AcquisitionError(str(exc)) from exc


def acquire_week(
    *,
    data_root: str | Path = DUKASCOPY_DEFAULT_DATA_ROOT,
    free_web_authorized: bool = False,
    provider_authorization_ref: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    opener: OpenerDirector | None = None,
) -> AcquisitionReceipt:
    """Acquire only EUR/USD ticks for ``[2016-03-07, 2016-03-14)`` UTC.

    ``free_web_authorized`` is an explicit local gate.  It does not represent
    account consent or acceptance of broker terms; it only confirms that the
    caller authorized the public, free web export described by the official
    page.  ``provider_authorization_ref`` must separately point to the
    provider's prior written consent for automated access/acquisition.  A
    network failure writes a bounded receipt and leaves any partial native
    bytes under ``raw`` without publishing a manifest.
    """

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if not isinstance(free_web_authorized, bool) or not free_web_authorized:
        raise AcquisitionError("free_web_authorized=True is required before public acquisition")
    provider_authorization_ref = _require_provider_authorization(provider_authorization_ref)
    root = Path(data_root).expanduser()
    _prepare_root(root)
    manifest_path = root / "manifests" / MANIFEST_NAME
    with _writer_lock(root):
        _reject_histdata_collision(root)
        _check_budget(root)
        receipt_path = _next_receipt_path(root)
        if manifest_path.exists():
            return _reuse_existing(manifest_path, receipt_path, root, provider_authorization_ref)
        return _acquire_new(root, manifest_path, receipt_path, timeout_seconds, opener, provider_authorization_ref)


def acquire(
    *,
    provider: str = DUKASCOPY_PROVIDER,
    instrument: str = DUKASCOPY_INSTRUMENT,
    start: datetime = DUKASCOPY_WINDOW_START,
    end: datetime = DUKASCOPY_WINDOW_END,
    data_root: str | Path = DUKASCOPY_DEFAULT_DATA_ROOT,
    free_web_authorized: bool = False,
    provider_authorization_ref: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    opener: OpenerDirector | None = None,
) -> AcquisitionReceipt:
    """Strict dispatcher that cannot expand beyond the approved pilot."""

    if provider != DUKASCOPY_PROVIDER:
        raise AcquisitionError("only provider=dukascopy is supported")
    normalized_instrument = str(instrument).strip().upper().replace("/", "").replace("-", "")
    if normalized_instrument != DUKASCOPY_SYMBOL.replace("-", ""):
        raise AcquisitionError("only instrument=EUR/USD is supported")
    if _utc(start, "start") != DUKASCOPY_WINDOW_START or _utc(end, "end") != DUKASCOPY_WINDOW_END:
        raise AcquisitionError("only the exact 2016-03-07T00Z to 2016-03-14T00Z window is authorized")
    return acquire_week(
        data_root=data_root,
        free_web_authorized=free_web_authorized,
        provider_authorization_ref=provider_authorization_ref,
        timeout_seconds=timeout_seconds,
        opener=opener,
    )


def _require_provider_authorization(reference: str | None) -> str:
    if not isinstance(reference, str) or not reference.strip():
        raise AcquisitionError(
            "provider_authorization_ref is required: automated acquisition needs prior written provider consent"
        )
    return reference.strip()


def _reuse_existing(
    manifest_path: Path,
    receipt_path: Path,
    root: Path,
    provider_authorization_ref: str,
) -> AcquisitionReceipt:
    manifest = _existing_manifest(manifest_path, root)
    validation = validate_manifest(manifest)
    if not validation.ok:
        raise AcquisitionError("existing Dukascopy manifest failed validation: " + "; ".join(validation.issues))
    receipt = AcquisitionReceipt(
        "EXISTING",
        DUKASCOPY_PROVIDER,
        DUKASCOPY_INSTRUMENT,
        DUKASCOPY_WINDOW_START,
        DUKASCOPY_WINDOW_END,
        root,
        manifest_path,
        receipt_path,
        manifest.api_base,
        manifest.api_base + "/instruments/EUR-USD",
        len(manifest.partitions),
        len(manifest.partitions),
        manifest.quote_count,
        manifest.raw_bytes,
        manifest.coverage_start,
        manifest.coverage_end,
        manifest.source_timezone,
        False,
        (),
        provider_authorization_ref,
    )
    _write_receipt(receipt_path, receipt)
    return receipt


def _acquire_new(
    root: Path,
    manifest_path: Path,
    receipt_path: Path,
    timeout_seconds: float,
    opener: OpenerDirector | None,
    provider_authorization_ref: str,
) -> AcquisitionReceipt:
    client = opener or build_opener(ProxyHandler({}))
    failures: list[str] = []
    api_base: str | None = None
    metadata_uri: str | None = None
    run = _WeekDownload((), 0, 0, 0, 0, None, None, ())
    try:
        api_base = _discover_api_base(client, timeout_seconds)
        metadata_uri = api_base + "/instruments/EUR-USD"
        metadata = _get_json(client, metadata_uri, timeout_seconds, limit=PAGE_LIMIT)
        _validate_instrument_metadata(metadata)
        run = _download_week(client, api_base, root, timeout_seconds)
        failures.extend(run.failures)
        if failures:
            raise AcquisitionError("Dukascopy pilot acquisition stopped: " + failures[-1])
        if run.completed_hours != 168:
            raise AcquisitionError(f"Dukascopy pilot is incomplete: {run.completed_hours}/168 hourly partitions")
        if run.quote_count == 0:
            raise AcquisitionError("Dukascopy pilot contains no quotes")
        manifest = _build_manifest(root, api_base, run)
        validation = validate_manifest(manifest)
        if not validation.ok:
            raise AcquisitionError("new Dukascopy manifest failed validation: " + "; ".join(validation.issues))
        _check_budget(root)
        _write_manifest(manifest_path, manifest)
        receipt = _receipt_from_run(
            "DOWNLOADED",
            root,
            manifest_path,
            receipt_path,
            api_base,
            metadata_uri,
            run,
            failures,
            provider_authorization_ref,
        )
        _write_receipt(receipt_path, receipt)
        return receipt
    except (HTTPError, URLError, TimeoutError, OSError, DukascopyError, ValueError) as exc:
        if not failures:
            failures.append(f"{type(exc).__name__}: {str(exc)}")
        receipt = _receipt_from_run(
            "BLOCKED",
            root,
            None,
            receipt_path,
            api_base,
            metadata_uri,
            run,
            failures,
            provider_authorization_ref,
        )
        _write_receipt(receipt_path, receipt)
        raise AcquisitionError(str(exc)) from exc


def _download_week(
    client: OpenerDirector,
    api_base: str,
    root: Path,
    timeout_seconds: float,
) -> _WeekDownload:
    partitions: list[DukascopyPartition] = []
    failures: list[str] = []
    attempted = 0
    raw_bytes = 0
    quote_count = 0
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    for hour in _hours(DUKASCOPY_WINDOW_START, DUKASCOPY_WINDOW_END):
        attempted += 1
        try:
            partition, quotes = _download_hour(client, api_base, root, hour, timeout_seconds, raw_bytes)
        except (HTTPError, URLError, TimeoutError, OSError, DukascopyError) as exc:
            failures.append(f"{hour.isoformat()} {type(exc).__name__}: {str(exc)}")
            break
        partitions.append(_with_partition_coverage(partition, quotes))
        raw_bytes += partition.raw_size
        quote_count += len(quotes)
        if quotes:
            coverage_start = coverage_start or quotes[0].event_time
            coverage_end = quotes[-1].event_time
        print(f"PROGRESS hour={hour.isoformat()} raw_bytes={raw_bytes} quotes={quote_count}", file=sys.stderr)
    return _WeekDownload(
        tuple(partitions),
        attempted,
        len(partitions),
        quote_count,
        raw_bytes,
        coverage_start,
        coverage_end,
        tuple(failures),
    )


def _download_hour(
    client: OpenerDirector,
    api_base: str,
    root: Path,
    hour: datetime,
    timeout_seconds: float,
    prior_bytes: int,
) -> tuple[DukascopyPartition, tuple[Any, ...]]:
    endpoint = api_base + TICK_PATH.format(year=hour.year, month=hour.month, day=hour.day, hour=hour.hour)
    body = _get_bytes(client, endpoint, timeout_seconds, limit=MAX_RESPONSE_BYTES)
    if prior_bytes + len(body) > MAX_RESPONSE_BYTES:
        raise AcquisitionError("Dukascopy pilot exceeds the 200 MiB raw-byte limit")
    _check_budget(root, additional_bytes=len(body))
    partition = _store_partition(root, hour, hour + timedelta(hours=1), endpoint, body)
    quotes = decode_tick_payload(
        body,
        raw_path=partition.raw_path,
        raw_sha256=partition.raw_sha256,
        partition_start=partition.start,
        partition_end=partition.end,
    )
    return partition, quotes


def _build_manifest(root: Path, api_base: str, run: _WeekDownload) -> DukascopyManifest:
    content_hash = manifest_content_hash(run.partitions)
    return DukascopyManifest(
        dataset_id=f"dukascopy:{DUKASCOPY_INSTRUMENT}:20160307-20160314:{content_hash[:32]}",
        provider=DUKASCOPY_PROVIDER,
        instrument=DUKASCOPY_INSTRUMENT,
        normalization_version=DUKASCOPY_NORMALIZATION_VERSION,
        format=DUKASCOPY_FORMAT,
        source_uri=DUKASCOPY_SOURCE_URI,
        widget_uri=DUKASCOPY_WIDGET_URI,
        config_uri=DUKASCOPY_CONFIG_URI,
        terms_uri=DUKASCOPY_TERMS_URI,
        api_base=api_base,
        source_timezone=DUKASCOPY_SOURCE_TIMEZONE,
        availability_basis="historical_event_time",
        data_root=str(root),
        window_start=DUKASCOPY_WINDOW_START,
        window_end=DUKASCOPY_WINDOW_END,
        coverage_start=run.coverage_start,
        coverage_end=run.coverage_end,
        quote_count=run.quote_count,
        raw_bytes=run.raw_bytes,
        content_hash=content_hash,
        partitions=run.partitions,
    )


def _receipt_from_run(
    status: str,
    root: Path,
    manifest_path: Path | None,
    receipt_path: Path,
    api_base: str | None,
    metadata_uri: str | None,
    run: _WeekDownload,
    failures: list[str],
    provider_authorization_ref: str,
) -> AcquisitionReceipt:
    return AcquisitionReceipt(
        status,
        DUKASCOPY_PROVIDER,
        DUKASCOPY_INSTRUMENT,
        DUKASCOPY_WINDOW_START,
        DUKASCOPY_WINDOW_END,
        root,
        manifest_path,
        receipt_path,
        api_base,
        metadata_uri,
        run.attempted_hours,
        run.completed_hours,
        run.quote_count,
        run.raw_bytes,
        run.coverage_start,
        run.coverage_end,
        DUKASCOPY_SOURCE_TIMEZONE,
        True,
        tuple(failures[:16]),
        provider_authorization_ref,
    )


def _discover_api_base(client: OpenerDirector, timeout_seconds: float) -> str:
    config = _get_json(client, DUKASCOPY_CONFIG_URI, timeout_seconds, limit=PAGE_LIMIT)
    if not isinstance(config, dict):
        raise AcquisitionError("official Dukascopy config is not an object")
    raw = config.get("JETTA_SERVER_URL")
    if not isinstance(raw, str) or not raw.strip():
        raise AcquisitionError("official Dukascopy config lacks JETTA_SERVER_URL")
    parsed = urlsplit(raw.strip())
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or not parsed.hostname.endswith(".dukascopy.com")
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise AcquisitionError("official JETTA_SERVER_URL is not an official HTTPS host")
    base = raw.strip().rstrip("/")
    return base if base.endswith("/v1") else base + "/v1"


def _get_json(client: OpenerDirector, url: str, timeout_seconds: float, *, limit: int) -> Any:
    body = _get_bytes(client, url, timeout_seconds, limit=limit)
    try:
        return json.loads(body.decode("utf-8"), parse_float=str, parse_int=int)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcquisitionError(f"official Dukascopy response is not UTF-8 JSON: {url}") from exc


def _get_bytes(client: OpenerDirector, url: str, timeout_seconds: float, *, limit: int) -> bytes:
    _assert_official_url(url)
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Referer": DUKASCOPY_WIDGET_URI,
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )
    try:
        with client.open(request, timeout=timeout_seconds) as response:
            _assert_official_url(response.geturl())
            status = int(response.getcode() or 200)
            if status != 200:
                raise AcquisitionError(f"official Dukascopy response returned HTTP {status}")
            length = _content_length(response)
            if length is not None and length > limit:
                raise AcquisitionError(f"official Dukascopy response exceeds {limit} bytes")
            return _read_bounded(response, limit)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise AcquisitionError(f"official Dukascopy URL unavailable ({type(exc).__name__}): {url}") from exc


def _validate_instrument_metadata(value: Any) -> None:
    if not isinstance(value, dict):
        raise AcquisitionError("official Dukascopy instrument response is not an object")
    code = value.get("code")
    if code not in (DUKASCOPY_SYMBOL, DUKASCOPY_INSTRUMENT):
        raise AcquisitionError("official Dukascopy instrument response is not EUR/USD")
    histories = value.get("histories")
    if not isinstance(histories, list) or not any(
        isinstance(item, dict) and item.get("period") == "1T" for item in histories
    ):
        raise AcquisitionError("official Dukascopy instrument has no documented 1T history")


def _store_partition(
    root: Path,
    start: datetime,
    end: datetime,
    source_uri: str,
    body: bytes,
) -> DukascopyPartition:
    filename = f"ticks-{start:%Y%m%dT%H}Z.json"
    relative = Path("raw") / filename
    target = root / relative
    digest = hashlib.sha256(body).hexdigest()
    if target.exists():
        _require_private_file(target, "Dukascopy raw collision")
        if target.read_bytes() != body:
            raise AcquisitionError(f"Dukascopy raw destination collision: {relative}")
    else:
        _atomic_private_write(target, body)
    return DukascopyPartition(
        partition_id=f"{start:%Y-%m-%dT%H:%M:%SZ}",
        raw_path=relative.as_posix(),
        raw_sha256=digest,
        raw_size=len(body),
        source_uri=source_uri,
        instrument=DUKASCOPY_INSTRUMENT,
        interval_code=DUKASCOPY_INTERVAL_CODE,
        start=start,
        end=end,
        coverage_start=None,
        coverage_end=None,
        quote_count=0,
    )


def _with_partition_coverage(partition: DukascopyPartition, quotes: tuple[Any, ...]) -> DukascopyPartition:
    return DukascopyPartition(
        partition_id=partition.partition_id,
        raw_path=partition.raw_path,
        raw_sha256=partition.raw_sha256,
        raw_size=partition.raw_size,
        source_uri=partition.source_uri,
        instrument=partition.instrument,
        interval_code=partition.interval_code,
        start=partition.start,
        end=partition.end,
        coverage_start=quotes[0].event_time if quotes else None,
        coverage_end=quotes[-1].event_time if quotes else None,
        quote_count=len(quotes),
    )


def _existing_manifest(path: Path, root: Path) -> DukascopyManifest:
    if path.is_symlink() or not path.is_file():
        raise AcquisitionError("existing Dukascopy manifest is not a regular file")
    _require_private_file(path, "Dukascopy manifest")
    manifest = DukascopyManifest.from_path(path)
    if Path(os.path.abspath(manifest.data_root)) != Path(os.path.abspath(root)):
        raise AcquisitionError("existing Dukascopy manifest data_root mismatch")
    return manifest


def _next_receipt_path(root: Path) -> Path:
    base = root / "receipts" / RECEIPT_NAME
    if not base.exists():
        return base
    for attempt in range(1, 1000):
        candidate = base.with_name(f"{base.stem}-attempt-{attempt}{base.suffix}")
        if not candidate.exists():
            return candidate
    raise AcquisitionError("Dukascopy receipt version space is exhausted")


def _prepare_root(root: Path) -> None:
    _reject_symlink_ancestors(root)
    if Path(os.path.abspath(root)) == Path(os.path.abspath(DEFAULT_DATA_ROOT)):
        raise AcquisitionError("Dukascopy requires a dedicated provider data root")
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise AcquisitionError("Dukascopy data root is not a regular directory")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    _require_private_dir(root)
    for name in ("raw", "manifests", "receipts"):
        child = root / name
        _reject_symlink_ancestors(child)
        if child.exists() and (child.is_symlink() or not child.is_dir()):
            raise AcquisitionError(f"Dukascopy data child is not a regular directory: {name}")
        child.mkdir(mode=0o700, exist_ok=True)
        _require_private_dir(child)


def _check_budget(root: Path, *, additional_bytes: int = 0, replacing_bytes: int = 0) -> None:
    """Apply one aggregate HistData+Dukascopy raw budget under the lock."""

    sibling_histdata_raw = (root.parent / "raw").is_dir()
    roots = (root.parent, root) if root.name.lower() == DUKASCOPY_PROVIDER or sibling_histdata_raw else (root,)
    try:
        guard_budget(roots, additional_bytes=additional_bytes, replacing_bytes=replacing_bytes)
    except StorageBudgetError as exc:
        raise AcquisitionError(str(exc)) from exc


def _reject_histdata_collision(root: Path) -> None:
    for candidate in (
        *(root / "raw").glob("HISTDATA_COM_ASCII_EURUSD_T_201603.zip"),
        *(root / "manifests").glob("histdata*.json"),
    ):
        if candidate.exists():
            raise AcquisitionError("Dukascopy root collides with canonical HistData artifacts")


def _write_manifest(path: Path, manifest: DukascopyManifest) -> None:
    encoded = (manifest.to_json() + "\n").encode("utf-8")
    if path.exists():
        _require_private_file(path, "Dukascopy manifest")
        if path.read_bytes() != encoded:
            raise AcquisitionError("Dukascopy manifest destination collision")
        return
    _atomic_private_write(path, encoded)


def _write_receipt(path: Path, receipt: AcquisitionReceipt) -> None:
    encoded = (json.dumps(receipt.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    if path.exists():
        _require_private_file(path, "Dukascopy receipt")
    _atomic_private_write(path, encoded, replace_existing=True)


def _atomic_private_write(path: Path, data: bytes, *, replace_existing: bool = False) -> None:
    _reject_symlink_ancestors(path)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(temporary, flags, 0o600)
    except FileExistsError as exc:
        raise AcquisitionError(f"atomic temporary collision: {temporary}") from exc
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        if replace_existing:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
            temporary.unlink()
        _fsync_directory(path.parent)
    except BaseException:
        with suppress(OSError):
            temporary.unlink()
        raise


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_bounded(response: Any, limit: int) -> bytes:
    parts: list[bytes] = []
    received = 0
    while True:
        block = response.read(min(CHUNK_SIZE, limit - received + 1))
        if not block:
            break
        received += len(block)
        if received > limit:
            raise AcquisitionError(f"Dukascopy response exceeded {limit} bytes")
        parts.append(block)
    return b"".join(parts)


def _content_length(response: Any) -> int | None:
    headers = getattr(response, "headers", {})
    value = headers.get("Content-Length")
    if value is None:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise AcquisitionError("Dukascopy response has invalid Content-Length") from exc
    if result < 0:
        raise AcquisitionError("Dukascopy response has invalid Content-Length")
    return result


def _assert_official_url(url: str) -> None:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or not parsed.hostname.endswith(".dukascopy.com")
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise AcquisitionError("Dukascopy request redirected away from official HTTPS host")


def _hours(start: datetime, end: datetime) -> Iterator[datetime]:
    current = start
    while current < end:
        yield current
        current += timedelta(hours=1)


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
            raise AcquisitionError(f"path contains a symlink component: {current}")


def _require_private_dir(path: Path) -> None:
    try:
        mode = stat.S_IMODE(os.lstat(path).st_mode)
    except OSError as exc:
        raise AcquisitionError(f"cannot inspect private directory: {path}") from exc
    if mode != 0o700:
        raise AcquisitionError(f"private Dukascopy directory must be mode 700: {path}")


def _require_private_file(path: Path, label: str) -> None:
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise AcquisitionError(f"cannot inspect {label}: {path}") from exc
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
        raise AcquisitionError(f"{label} must be a regular mode-600 file: {path}")


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value is not None else None


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AcquisitionError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=DUKASCOPY_DEFAULT_DATA_ROOT)
    parser.add_argument("--allow-free-web", action="store_true")
    parser.add_argument("--provider-authorization-ref")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    args = parser.parse_args(argv)
    try:
        receipt = acquire_week(
            data_root=args.data_root,
            free_web_authorized=args.allow_free_web,
            provider_authorization_ref=args.provider_authorization_ref,
            timeout_seconds=args.timeout,
        )
    except (AcquisitionError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    for key, value in receipt.to_dict().items():
        print(f"{key.upper()}={value}")
    return 0


__all__ = [
    "AcquisitionError",
    "AcquisitionReceipt",
    "DEFAULT_TIMEOUT_SECONDS",
    "MANIFEST_NAME",
    "RECEIPT_NAME",
    "acquire",
    "acquire_week",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
