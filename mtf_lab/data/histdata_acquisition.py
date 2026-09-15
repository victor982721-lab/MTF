#!/usr/bin/env python3
"""Acquire authorized HistData EURUSD Generic ASCII tick archives.

Only the public HTTPS free-download form is used.  FTP/SFTP, paid services,
accounts, proxies, and selector arguments are intentionally absent.  The
completed ZIP is kept byte-for-byte under the private market-data root; a
versioned manifest is written only after the archive passes validation.  The
legacy March-2016 entry points remain fixed while :func:`acquire_month`
supports an explicit development month.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import HTTPCookieProcessor, OpenerDirector, ProxyHandler, Request, build_opener

from mtf_lab.data.historical import (
    DEFAULT_DATA_ROOT,
    HISTDATA_DEVELOPMENT_END_MONTH,
    HISTDATA_DEVELOPMENT_START_MONTH,
    HISTDATA_INSTRUMENT,
    HISTDATA_MONTH,
    HISTDATA_PROVIDER,
    MAX_DATASET_BYTES,
    DatasetManifest,
    HistoricalDataError,
    manifest_from_histdata_archive,
    validate_dataset,
)
from mtf_lab.data.storage_budget import StorageBudgetError, acquisition_lock, guard_budget, shared_lock_root

HISTDATA_MONTH_PAGE = (
    "https://www.histdata.com/download-free-forex-historical-data/?/ascii/tick-data-quotes/eurusd/2016/3"
)
HISTDATA_MONTH_PAGE_PREFIX = (
    "https://www.histdata.com/download-free-forex-historical-data/?/ascii/tick-data-quotes/eurusd"
)
HISTDATA_DOWNLOAD_ENDPOINT = "https://www.histdata.com/get.php"
HISTDATA_TERMS_URI = "https://www.histdata.com/f-a-q/data-files-detailed-specification/"
ARCHIVE_NAME = "HISTDATA_COM_ASCII_EURUSD_T_201603.zip"
MANIFEST_NAME = "histdata-eurusd-201603.json"
MIN_FREE_RATIO = 0.20
CHUNK_SIZE = 1024 * 1024
PAGE_LIMIT = 4 * 1024 * 1024
STATE_INTERVAL_BYTES = 8 * 1024 * 1024


class AcquisitionError(RuntimeError):
    """The bounded free HistData acquisition cannot continue safely."""


class _DownloadFormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.action: str | None = None
        self.fields: dict[str, str] = {}
        self.duplicate_fields: set[str] = set()
        self._inside = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "form" and attributes.get("id") == "file_down":
            self._inside = True
            self.action = attributes.get("action")
        elif tag == "input" and self._inside and (attributes.get("type") or "").lower() == "hidden":
            name = attributes.get("name")
            value = attributes.get("value")
            if name and value is not None:
                if name in self.fields:
                    self.duplicate_fields.add(name)
                self.fields[name] = value

    def handle_endtag(self, tag: str) -> None:
        if tag == "form" and self._inside:
            self._inside = False


@dataclass(frozen=True, slots=True)
class AcquisitionReceipt:
    archive_path: Path
    manifest_path: Path
    raw_sha256: str
    raw_size: int
    status: str
    resumed: bool
    etag: str | None
    month: str = HISTDATA_MONTH

    def to_dict(self) -> dict[str, Any]:
        return {
            "archive_path": str(self.archive_path),
            "manifest_path": str(self.manifest_path),
            "raw_sha256": self.raw_sha256,
            "raw_size": self.raw_size,
            "status": self.status,
            "resumed": self.resumed,
            "etag": self.etag,
            "network_performed": self.status == "DOWNLOADED",
            "provider": HISTDATA_PROVIDER,
            "instrument": "EUR/USD",
            "month": self.month,
        }


@dataclass(frozen=True, slots=True)
class _PartialState:
    offset: int
    etag: str | None
    expected_total: int | None
    sha256: str


@contextmanager
def _writer_lock(root: Path) -> Iterator[None]:
    try:
        with acquisition_lock(root, lock_root=shared_lock_root(root, provider=HISTDATA_PROVIDER)):
            yield
    except StorageBudgetError as exc:
        raise AcquisitionError(str(exc)) from exc


def acquire_march_2016(
    *,
    data_root: str | Path = DEFAULT_DATA_ROOT,
    timeout_seconds: float = 120.0,
    opener: OpenerDirector | None = None,
) -> AcquisitionReceipt:
    """Download/validate only the EURUSD Generic ASCII Tick March-2016 ZIP."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    root = Path(data_root).expanduser()
    _prepare_root(root)
    with _writer_lock(root):
        _check_budget(root)
        raw_dir = root / "raw"
        manifest_dir = root / "manifests"
        archive_path = raw_dir / ARCHIVE_NAME
        manifest_path = manifest_dir / MANIFEST_NAME
        if archive_path.exists():
            receipt = _reuse_existing(archive_path, manifest_path, root)
            return receipt
        client = opener or build_opener(ProxyHandler({}), HTTPCookieProcessor(CookieJar()))
        action, fields = _free_form(client, timeout_seconds)
        part_path = raw_dir / f".{ARCHIVE_NAME}.part"
        state_path = raw_dir / f".{ARCHIVE_NAME}.state.json"
        raw_sha256, raw_size, etag, resumed = _download(
            client,
            action,
            fields,
            part_path=part_path,
            state_path=state_path,
            archive_path=archive_path,
            timeout_seconds=timeout_seconds,
            root=root,
        )
        manifest = manifest_from_histdata_archive(
            archive_path,
            data_root=root,
            acquired_at=_now_utc(),
            source_uri=HISTDATA_MONTH_PAGE,
            terms_uri=HISTDATA_TERMS_URI,
            etag=etag,
        )
        validation = validate_dataset(manifest)
        if not validation.ok:
            raise AcquisitionError("downloaded archive failed validation: " + "; ".join(validation.issues))
        _check_budget(root)
        _write_manifest(manifest_path, manifest)
        with suppress(FileNotFoundError):
            state_path.unlink()
        return AcquisitionReceipt(archive_path, manifest_path, raw_sha256, raw_size, "DOWNLOADED", resumed, etag)


def acquire_month(
    year: int,
    month: int,
    *,
    data_root: str | Path = DEFAULT_DATA_ROOT,
    terms_accepted: bool = False,
    timeout_seconds: float = 120.0,
    opener: OpenerDirector | None = None,
) -> AcquisitionReceipt:
    """Acquire one explicit EURUSD development month from HistData.

    ``year`` and ``month`` are deliberately integer-only and limited to the
    approved 2016-01 through 2019-12 development span.  The month page, POST
    form fields, archive name, archive member identity, and manifest are all
    bound to that same ``YYYYMM`` value.  Existing raw bytes or manifests are
    never replaced.
    """

    month_key = _validate_development_month(year, month)
    if not isinstance(terms_accepted, bool) or not terms_accepted:
        raise AcquisitionError("terms_accepted=True is required before acquisition")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    page_uri = _month_page(month_key)
    archive_name = _archive_name(month_key)
    manifest_name = _manifest_name(month_key)
    root = Path(data_root).expanduser()
    _prepare_root(root)
    with _writer_lock(root):
        _check_budget(root)
        raw_dir = root / "raw"
        manifest_dir = root / "manifests"
        archive_path = raw_dir / archive_name
        manifest_path = manifest_dir / manifest_name
        if archive_path.exists() or archive_path.is_symlink():
            return _reuse_existing(
                archive_path,
                manifest_path,
                root,
                expected_month=month_key,
                expected_archive_name=archive_name,
                source_uri=page_uri,
                receipt_month=month_key,
            )
        if manifest_path.exists() or manifest_path.is_symlink():
            raise AcquisitionError("manifest destination exists without its archive; original bytes preserved")
        client = opener or build_opener(ProxyHandler({}), HTTPCookieProcessor(CookieJar()))
        action, fields = _free_form_for_month(client, timeout_seconds, page_uri, month_key, strict=True)
        part_path = raw_dir / f".{archive_name}.part"
        state_path = raw_dir / f".{archive_name}.state.json"
        raw_sha256, raw_size, etag, resumed = _download(
            client,
            action,
            fields,
            part_path=part_path,
            state_path=state_path,
            archive_path=archive_path,
            timeout_seconds=timeout_seconds,
            root=root,
            referer=page_uri,
        )
        _assert_archive_for_month(archive_path, month_key, archive_name)
        manifest = manifest_from_histdata_archive(
            archive_path,
            data_root=root,
            acquired_at=_now_utc(),
            source_uri=page_uri,
            terms_uri=HISTDATA_TERMS_URI,
            month=month_key,
            etag=etag,
        )
        _assert_manifest_matches_archive(
            manifest,
            root,
            archive_path,
            raw_sha256,
            raw_size,
            expected_month=month_key,
            expected_archive_name=archive_name,
        )
        validation = validate_dataset(manifest)
        if not validation.ok:
            raise AcquisitionError("downloaded archive failed validation: " + "; ".join(validation.issues))
        _check_budget(root)
        _write_manifest(manifest_path, manifest)
        with suppress(FileNotFoundError):
            state_path.unlink()
        return AcquisitionReceipt(
            archive_path,
            manifest_path,
            raw_sha256,
            raw_size,
            "DOWNLOADED",
            resumed,
            etag,
            month_key,
        )


def acquire(
    provider: str = HISTDATA_PROVIDER,
    instrument: str = "EURUSD",
    month: str = "2016-03",
    data_root: str | Path = DEFAULT_DATA_ROOT,
    terms_accepted: bool = False,
    timeout_seconds: float = 120.0,
    opener: OpenerDirector | None = None,
) -> AcquisitionReceipt:
    """Acquire only the approved HistData EURUSD March-2016 container."""

    normalized_instrument = str(instrument).strip().upper().replace("/", "")
    normalized_month = str(month).strip().replace("/", "-")
    if provider != HISTDATA_PROVIDER or normalized_instrument != "EURUSD" or normalized_month != "2016-03":
        raise AcquisitionError("only provider=histdata, instrument=EURUSD, month=2016-03 is authorized")
    if not isinstance(terms_accepted, bool) or not terms_accepted:
        raise AcquisitionError("terms_accepted=True is required before acquisition")
    return acquire_march_2016(data_root=data_root, timeout_seconds=timeout_seconds, opener=opener)


def _prepare_root(root: Path) -> None:
    _reject_symlink_parent(root)
    if root.exists() and (root.is_symlink() or not root.is_dir()):
        raise AcquisitionError("market-data root is not a regular directory")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    _reject_symlink_parent(root)
    _require_private_dir(root)
    for child in (root / "raw", root / "manifests"):
        _reject_symlink_parent(child)
        if child.exists() and (child.is_symlink() or not child.is_dir()):
            raise AcquisitionError(f"market-data child is not a regular directory: {child.name}")
        child.mkdir(mode=0o700, exist_ok=True)
        _require_private_dir(child)


def _check_budget(root: Path, *, additional_bytes: int = 0, replacing_bytes: int = 0) -> None:
    """Translate the provider-neutral aggregate guard into this API's error."""

    roots = (root, root / "dukascopy")
    try:
        guard_budget(roots, additional_bytes=additional_bytes, replacing_bytes=replacing_bytes)
    except StorageBudgetError as exc:
        raise AcquisitionError(str(exc)) from exc


def _reject_symlink_parent(target: Path) -> None:
    absolute = Path(os.path.abspath(target))
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        if current.is_symlink():
            raise AcquisitionError(f"market-data path contains symlink directory: {current}")


def _require_private_dir(path: Path) -> None:
    try:
        mode = stat.S_IMODE(os.lstat(path).st_mode)
    except OSError as exc:
        raise AcquisitionError(f"cannot inspect private directory: {path}") from exc
    if mode != 0o700:
        raise AcquisitionError(f"private market-data directory must be mode 700: {path}")


def _require_private_file(path: Path) -> None:
    try:
        info = os.lstat(path)
    except OSError as exc:
        raise AcquisitionError(f"cannot inspect private file: {path}") from exc
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
        raise AcquisitionError(f"private market-data file must be regular mode 600: {path}")


def _validate_development_month(year: int, month: int) -> str:
    if isinstance(year, bool) or not isinstance(year, int):
        raise AcquisitionError("year must be an integer")
    if isinstance(month, bool) or not isinstance(month, int):
        raise AcquisitionError("month must be an integer")
    start_year = int(HISTDATA_DEVELOPMENT_START_MONTH[:4])
    end_year = int(HISTDATA_DEVELOPMENT_END_MONTH[:4])
    if year < start_year or year > end_year:
        raise AcquisitionError("HistData development acquisition is limited to years 2016-2019")
    if month < 1 or month > 12:
        raise AcquisitionError("month must be between 1 and 12")
    month_key = f"{year:04d}{month:02d}"
    if not HISTDATA_DEVELOPMENT_START_MONTH <= month_key <= HISTDATA_DEVELOPMENT_END_MONTH:
        raise AcquisitionError("HistData month is outside the approved development window")
    return month_key


def _month_page(month: str) -> str:
    return f"{HISTDATA_MONTH_PAGE_PREFIX}/{int(month[:4])}/{int(month[4:])}"


def _archive_name(month: str) -> str:
    return f"HISTDATA_COM_ASCII_EURUSD_T_{month}.zip"


def _manifest_name(month: str) -> str:
    return f"histdata-eurusd-{month}.json"


def _expected_form_fields(month: str) -> dict[str, str]:
    return {
        "date": month[:4],
        "datemonth": month,
        "platform": "ASCII",
        "timeframe": "T",
        "fxpair": HISTDATA_INSTRUMENT.replace("/", ""),
    }


def _free_form(client: OpenerDirector, timeout_seconds: float) -> tuple[str, dict[str, str]]:
    """Read and validate the unchanged March-2016 download form."""

    return _free_form_for_month(client, timeout_seconds, HISTDATA_MONTH_PAGE, HISTDATA_MONTH, strict=False)


def _free_form_for_month(
    client: OpenerDirector,
    timeout_seconds: float,
    page_uri: str,
    month: str,
    *,
    strict: bool,
) -> tuple[str, dict[str, str]]:
    request = Request(page_uri, headers={"Accept": "text/html", "User-Agent": "mtf-lab-histdata/1"})
    try:
        with client.open(request, timeout=timeout_seconds) as response:
            _assert_official_url(response.geturl())
            body = response.read(PAGE_LIMIT + 1)
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise AcquisitionError(f"official HistData month page unavailable: {type(exc).__name__}") from exc
    if len(body) > PAGE_LIMIT:
        raise AcquisitionError("official HistData form page exceeded the bounded page limit")
    parser = _DownloadFormParser()
    try:
        parser.feed(body.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise AcquisitionError("official HistData form is not UTF-8") from exc
    if not parser.action:
        raise AcquisitionError("official HistData download form is absent")
    action = urljoin(page_uri, parser.action)
    _assert_official_url(action)
    if strict and action != HISTDATA_DOWNLOAD_ENDPOINT:
        raise AcquisitionError("official HistData form action is not the approved download endpoint")
    expected = _expected_form_fields(month)
    if (strict and parser.duplicate_fields.intersection(expected)) or any(
        parser.fields.get(key) != value for key, value in expected.items()
    ):
        raise AcquisitionError(f"official form does not describe the authorized EURUSD {month} tick ZIP")
    return action, parser.fields


def _download(
    client: OpenerDirector,
    action: str,
    fields: dict[str, str],
    *,
    part_path: Path,
    state_path: Path,
    archive_path: Path,
    timeout_seconds: float,
    root: Path,
    referer: str = HISTDATA_MONTH_PAGE,
) -> tuple[str, int, str | None, bool]:
    existing = _existing_partial(part_path, state_path, action)
    if existing is not None and existing.offset == 0:
        # No payload prefix exists to resume.  Drop only the empty marker so a
        # fresh HTTP 200 response is not mistaken for a Range continuation.
        with suppress(FileNotFoundError):
            part_path.unlink()
        with suppress(FileNotFoundError):
            state_path.unlink()
        existing = None
    offset = existing.offset if existing else 0
    prior_etag = existing.etag if existing else None
    resumed = offset > 0
    response_etag: str | None = None
    expected_total: int | None = None
    _check_budget(root)
    if existing and existing.expected_total == offset:
        _assert_zip(part_path)
        _check_budget(root)
        _link_without_overwrite(part_path, archive_path)
        _check_budget(root)
        return existing.sha256, archive_path.stat().st_size, existing.etag, True
    try:
        request = _download_request(action, fields, offset=offset, etag=prior_etag, referer=referer)
        with client.open(request, timeout=timeout_seconds) as response:
            _assert_official_url(response.geturl())
            status = int(response.getcode() or 200)
            _check_response_status(status, resumed)
            response_etag = _header(response, "ETag") or prior_etag
            content_length = _positive_header_int(response, "Content-Length")
            expected_total = _response_total(response, status, offset, content_length)
            _check_resume_headers(
                response,
                status=status,
                offset=offset,
                content_length=content_length,
                expected_total=expected_total,
                prior=existing,
                response_etag=response_etag,
            )
            _check_download_size(expected_total)
            if expected_total is not None:
                _check_budget(root, additional_bytes=max(expected_total - offset, 0))
            raw_sha256, received = _stream_response(
                response,
                part_path=part_path,
                state_path=state_path,
                action=action,
                offset=offset,
                resumed=resumed,
                expected_total=expected_total,
                etag=response_etag,
                root=root,
            )
            if expected_total is not None and received != expected_total:
                raise AcquisitionError("download ended before the advertised Content-Length")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        _preserve_or_raise(part_path, state_path, action, prior_etag, expected_total, exc)
        raise AcquisitionError(f"official HistData download failed: {type(exc).__name__}") from exc
    except BaseException as exc:
        _preserve_or_raise(part_path, state_path, action, response_etag or prior_etag, expected_total, exc)
        raise
    _assert_zip(part_path)
    _check_budget(root)
    _link_without_overwrite(part_path, archive_path)
    _check_budget(root)
    return raw_sha256, archive_path.stat().st_size, response_etag, resumed


def _download_request(
    action: str,
    fields: dict[str, str],
    *,
    offset: int,
    etag: str | None,
    referer: str = HISTDATA_MONTH_PAGE,
) -> Request:
    headers = {
        "Accept": "application/zip, application/octet-stream",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.histdata.com",
        "Referer": referer,
        "User-Agent": "mtf-lab-histdata/1",
    }
    if offset:
        headers["Range"] = f"bytes={offset}-"
        if etag:
            headers["If-Range"] = etag
    return Request(action, data=urlencode(fields).encode("ascii"), headers=headers, method="POST")


def _check_response_status(status: int, resumed: bool) -> None:
    if resumed and status != 206:
        raise AcquisitionError("server did not honor the partial-download Range; partial bytes preserved")
    if not resumed and status != 200:
        raise AcquisitionError(f"official download returned HTTP {status}")


def _check_download_size(expected_total: int | None) -> None:
    if expected_total is not None and expected_total > MAX_DATASET_BYTES:
        raise AcquisitionError("archive exceeds the 40 GiB limit")


def _stream_response(
    response: Any,
    *,
    part_path: Path,
    state_path: Path,
    action: str,
    offset: int,
    resumed: bool,
    expected_total: int | None,
    etag: str | None,
    root: Path,
) -> tuple[str, int]:
    digest = hashlib.sha256()
    if resumed:
        _update_digest_from_file(part_path, digest)
    received = offset
    # A failed first block can leave a validated zero-byte partial plus state;
    # append mode safely resumes that prefix without an ``xb`` collision.
    mode = "ab" if resumed or part_path.exists() else "xb"
    with part_path.open(mode) as output:
        while True:
            block = response.read(CHUNK_SIZE)
            if not block:
                break
            received += len(block)
            if received > MAX_DATASET_BYTES:
                raise AcquisitionError("download exceeded the 40 GiB limit")
            # The current partial is already in the inventory.  Check only
            # the next block so a resumable stream is never double-counted.
            _check_budget(root, additional_bytes=len(block))
            output.write(block)
            output.flush()
            digest.update(block)
            if received == len(block) or received % STATE_INTERVAL_BYTES < len(block):
                _write_state(
                    state_path,
                    action=action,
                    etag=etag,
                    expected_total=expected_total,
                    received=received,
                    sha256=digest.hexdigest(),
                )
                total = str(expected_total) if expected_total is not None else "unknown"
                print(f"PROGRESS bytes={received} total={total}", file=sys.stderr)
        output.flush()
        os.fsync(output.fileno())
    os.chmod(part_path, 0o600)
    _write_state(
        state_path,
        action=action,
        etag=etag,
        expected_total=expected_total,
        received=received,
        sha256=digest.hexdigest(),
    )
    return digest.hexdigest(), received


def _update_digest_from_file(path: Path, digest: Any) -> None:
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(CHUNK_SIZE), b""):
            digest.update(block)


def _existing_partial(part_path: Path, state_path: Path, action: str) -> _PartialState | None:
    if not part_path.exists():
        if state_path.exists():
            raise AcquisitionError("partial state exists without its partial archive")
        return None
    if part_path.is_symlink() or not part_path.is_file():
        raise AcquisitionError("partial archive is not a regular file")
    if not state_path.exists() and part_path.stat().st_size == 0:
        part_path.unlink()
        return None
    if state_path.is_symlink() or not state_path.is_file():
        raise AcquisitionError("partial state is not a regular file")
    state = _load_partial_state(state_path)
    return _partial_state_from_mapping(state, part_path, action)


def _load_partial_state(path: Path) -> dict[str, Any]:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcquisitionError("partial state is unreadable; bytes preserved") from exc
    if not isinstance(state, dict):
        raise AcquisitionError("partial state is not an object; bytes preserved")
    expected_keys = {"url", "etag", "expected_total", "received", "sha256", "updated_at"}
    if set(state) != expected_keys:
        raise AcquisitionError("partial state schema mismatch; bytes preserved")
    return state


def _partial_state_from_mapping(state: dict[str, Any], part_path: Path, action: str) -> _PartialState:
    if not isinstance(state.get("url"), str) or state.get("url") != action:
        raise AcquisitionError("partial state belongs to another official download; bytes preserved")
    if not isinstance(state.get("updated_at"), str) or not state["updated_at"].strip():
        raise AcquisitionError("partial state timestamp is invalid; bytes preserved")
    received = _strict_nonnegative_int(state.get("received"), "partial received")
    actual_size = part_path.stat().st_size
    if received != actual_size:
        raise AcquisitionError("partial state byte count mismatch; bytes preserved")
    expected_total = _partial_expected_total(state.get("expected_total"), received)
    etag = _partial_etag(state.get("etag"))
    sha256 = _partial_sha256(state.get("sha256"), part_path)
    return _PartialState(received, etag, expected_total, sha256)


def _partial_expected_total(value: Any, received: int) -> int | None:
    if value is None:
        return None
    expected = _strict_positive_int(value, "partial expected_total")
    if expected < received or expected > MAX_DATASET_BYTES:
        raise AcquisitionError("partial expected_total is invalid; bytes preserved")
    return expected


def _partial_etag(value: Any) -> str | None:
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise AcquisitionError("partial ETag is invalid; bytes preserved")
    return value


def _partial_sha256(value: Any, path: Path) -> str:
    if not _is_sha256(value) or _hash_file(path) != value:
        raise AcquisitionError("partial SHA-256 mismatch; bytes preserved")
    assert isinstance(value, str)
    return value


def _write_state(
    path: Path,
    *,
    action: str,
    etag: str | None,
    expected_total: int | None,
    received: int,
    sha256: str,
) -> None:
    payload = {
        "url": action,
        "etag": etag,
        "expected_total": expected_total,
        "received": received,
        "sha256": sha256,
        "updated_at": _now_utc().isoformat().replace("+00:00", "Z"),
    }
    _reject_symlink_parent(path)
    if path.exists() and path.is_symlink():
        raise AcquisitionError("partial state path is a symlink")
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    _atomic_private_write(path, encoded, replace_existing=True)


def _preserve_or_raise(
    part_path: Path,
    state_path: Path,
    action: str,
    etag: str | None,
    expected_total: int | None,
    original_error: BaseException,
) -> None:
    if not part_path.is_file() or part_path.is_symlink():
        return
    try:
        _write_state(
            state_path,
            action=action,
            etag=etag,
            expected_total=expected_total,
            received=part_path.stat().st_size,
            sha256=_hash_file(part_path),
        )
    except (AcquisitionError, OSError):
        raise AcquisitionError("partial-state persistence failed; bytes preserved") from original_error


def _assert_zip(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise AcquisitionError("completed archive is not a regular file")
    try:
        with zipfile.ZipFile(path, "r") as archive:
            if archive.testzip() is not None:
                raise AcquisitionError("completed archive contains a corrupt member")
    except zipfile.BadZipFile as exc:
        raise AcquisitionError("completed response is not a ZIP archive; partial bytes preserved") from exc


def _assert_archive_for_month(path: Path, month: str, expected_name: str) -> None:
    if path.name != expected_name:
        raise AcquisitionError("archive name does not match the requested month")
    _assert_zip(path)
    monthly_members: set[str] = set()
    pattern = re.compile(r"_t_(\d{6})\.csv$", re.IGNORECASE)
    try:
        with zipfile.ZipFile(path, "r") as archive:
            for info in archive.infolist():
                match = pattern.search(info.filename)
                if not info.is_dir() and match is not None and "eurusd" in info.filename.lower():
                    monthly_members.add(match.group(1))
    except zipfile.BadZipFile as exc:
        raise AcquisitionError("completed response is not a ZIP archive; partial bytes preserved") from exc
    if monthly_members != {month}:
        raise AcquisitionError("completed archive member month does not match the requested month")


def _link_without_overwrite(source: Path, target: Path) -> None:
    if target.exists():
        if target.is_symlink() or not target.is_file() or _hash_file(target) != _hash_file(source):
            raise AcquisitionError("archive destination collision; original bytes preserved")
        source.unlink()
        _fsync_directory(target.parent)
        return
    os.link(source, target)
    source.unlink()
    _fsync_directory(target.parent)


def _reuse_existing(
    archive_path: Path,
    manifest_path: Path,
    root: Path,
    *,
    expected_month: str | None = None,
    expected_archive_name: str | None = None,
    source_uri: str = HISTDATA_MONTH_PAGE,
    receipt_month: str = HISTDATA_MONTH,
) -> AcquisitionReceipt:
    _require_private_dir(root)
    _reject_symlink_parent(archive_path)
    _reject_symlink_parent(manifest_path)
    if expected_archive_name is not None and archive_path.name != expected_archive_name:
        raise AcquisitionError("existing archive name does not match the requested month")
    if archive_path.is_symlink() or not archive_path.is_file():
        raise AcquisitionError("existing archive is not a regular file")
    _require_private_file(archive_path)
    _assert_zip(archive_path)
    archive_size = archive_path.stat().st_size
    if archive_size > MAX_DATASET_BYTES:
        raise AcquisitionError("existing archive exceeds the 40 GiB limit")
    archive_sha256 = _hash_file(archive_path)
    if expected_month is not None:
        _assert_archive_for_month(archive_path, expected_month, expected_archive_name or archive_path.name)
    if manifest_path.exists():
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise AcquisitionError("existing manifest is not a regular file")
        _require_private_file(manifest_path)
        manifest = DatasetManifest.from_path(manifest_path)
    else:
        manifest = manifest_from_histdata_archive(
            archive_path,
            data_root=root,
            acquired_at=_now_utc(),
            source_uri=source_uri,
            terms_uri=HISTDATA_TERMS_URI,
            month=expected_month or HISTDATA_MONTH,
        )
        _check_budget(root)
        _write_manifest(manifest_path, manifest)
    _assert_manifest_matches_archive(
        manifest,
        root,
        archive_path,
        archive_sha256,
        archive_size,
        expected_month=expected_month,
        expected_archive_name=expected_archive_name,
    )
    validation = validate_dataset(manifest)
    if not validation.ok:
        raise AcquisitionError("existing archive failed validation: " + "; ".join(validation.issues))
    _check_budget(root)
    return AcquisitionReceipt(
        archive_path,
        manifest_path,
        archive_sha256,
        archive_size,
        "EXISTING",
        False,
        manifest.partitions[0].etag,
        receipt_month,
    )


def _assert_manifest_matches_archive(
    manifest: DatasetManifest,
    root: Path,
    archive_path: Path,
    archive_sha256: str,
    archive_size: int,
    *,
    expected_month: str | None = None,
    expected_archive_name: str | None = None,
) -> None:
    if Path(os.path.abspath(manifest.data_root)) != Path(os.path.abspath(root)):
        raise AcquisitionError("manifest data_root does not match the acquisition root")
    if len(manifest.partitions) != 1:
        raise AcquisitionError("manifest partition count does not match the authorized archive")
    partition = manifest.partitions[0]
    expected_path = Path(os.path.abspath(root / partition.raw_archive))
    if expected_path != Path(os.path.abspath(archive_path)):
        raise AcquisitionError("manifest raw path does not match the current archive")
    if partition.raw_sha256 != archive_sha256 or partition.raw_size != archive_size:
        raise AcquisitionError("manifest raw hash/size does not match the current archive")
    if expected_month is not None and (partition.partition_id != expected_month or partition.month != expected_month):
        raise AcquisitionError("manifest month does not match the requested month")
    if expected_archive_name is not None and Path(partition.raw_archive).name != expected_archive_name:
        raise AcquisitionError("manifest archive name does not match the requested month")


def _write_manifest(path: Path, manifest: DatasetManifest) -> None:
    _reject_symlink_parent(path)
    if path.exists():
        if path.is_symlink() or not path.is_file():
            raise AcquisitionError("manifest destination is not a regular file")
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise AcquisitionError("manifest destination must be mode 600")
        if path.read_bytes() != (manifest.to_json() + "\n").encode("utf-8"):
            raise AcquisitionError("manifest destination collision; original bytes preserved")
        return
    _atomic_private_write(path, (manifest.to_json() + "\n").encode("utf-8"))


def _atomic_private_write(path: Path, data: bytes, *, replace_existing: bool = False) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(temporary, flags, 0o600)
    except FileExistsError as exc:
        raise AcquisitionError("atomic temporary path collision; original bytes preserved") from exc
    try:
        with os.fdopen(fd, "wb") as output:
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


def _check_free_space(root: Path) -> None:
    _check_budget(root)


def _response_total(response: Any, status: int, offset: int, content_length: int | None) -> int | None:
    if status == 206:
        start, end, total = _content_range(response)
        if start != offset or end < start or total <= end:
            raise AcquisitionError("partial Content-Range does not match the saved offset")
        if content_length is not None and content_length != end - start + 1:
            raise AcquisitionError("partial Content-Length does not match Content-Range")
        return total
    return content_length


def _content_range(response: Any) -> tuple[int, int, int]:
    value = _header(response, "Content-Range")
    match = re.fullmatch(r"bytes\s+(\d+)-(\d+)/(\d+)", value or "")
    if match is None:
        raise AcquisitionError("partial response lacks a valid Content-Range")
    return tuple(int(item) for item in match.groups())  # type: ignore[return-value]


def _check_resume_headers(
    response: Any,
    *,
    status: int,
    offset: int,
    content_length: int | None,
    expected_total: int | None,
    prior: _PartialState | None,
    response_etag: str | None,
) -> None:
    if prior is None:
        return
    if prior.etag is not None and response_etag != prior.etag:
        raise AcquisitionError("resume ETag mismatch; partial bytes preserved")
    if prior.expected_total is not None and expected_total != prior.expected_total:
        raise AcquisitionError("resume total size mismatch; partial bytes preserved")
    if status != 206:
        raise AcquisitionError("resume response is not partial content; partial bytes preserved")
    _response_total(response, status, offset, content_length)


def _positive_header_int(response: Any, name: str) -> int | None:
    value = _header(response, name)
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise AcquisitionError(f"official response has invalid {name}") from exc
    if parsed < 0:
        raise AcquisitionError(f"official response has invalid {name}")
    return parsed


def _header(response: Any, name: str) -> str | None:
    value = response.headers.get(name)
    return str(value) if value is not None else None


def _strict_positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AcquisitionError(f"{name} must be a positive integer")
    return value


def _strict_nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AcquisitionError(f"{name} must be a non-negative integer")
    return value


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdefABCDEF" for char in value)


def _assert_official_url(url: str) -> None:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "www.histdata.com":
        raise AcquisitionError("download redirected away from the official HistData HTTPS host")


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(CHUNK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def _now_utc() -> datetime:
    return datetime.now(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", default=HISTDATA_PROVIDER)
    parser.add_argument("--instrument", default="EURUSD")
    parser.add_argument("--month", default="2016-03")
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--terms-accepted", action="store_true")
    parser.add_argument("--timeout", type=float, default=120.0)
    args = parser.parse_args(argv)
    try:
        receipt = acquire(
            provider=args.provider,
            instrument=args.instrument,
            month=args.month,
            data_root=args.data_root,
            terms_accepted=args.terms_accepted,
            timeout_seconds=args.timeout,
        )
    except (AcquisitionError, HistoricalDataError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    for key, value in receipt.to_dict().items():
        print(f"{key.upper()}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["AcquisitionError", "AcquisitionReceipt", "acquire", "acquire_month", "acquire_march_2016", "main"]
