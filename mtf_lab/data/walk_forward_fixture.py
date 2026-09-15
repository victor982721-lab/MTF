"""Small, deterministic offline fixture for the isolated WF stream contract.

The fixture is intentionally an identity-only source.  It builds synthetic
``DatasetManifest``/``WalkForwardManifest`` objects and ``HistoricalQuote``
records in memory; it does not inspect a raw archive, acquire data, use the
network, open SQLite, or make the closed holdout available.  The resulting
stream is only a regression surface for the causal ``WARMUP_ONLY`` to
``WF_EVALUATION`` transition and for restoring ``WalkForwardStreamGuard``
from its durable snapshot.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, TypeAlias

from ..core.canonical import canonical_json
from .historical import (
    HISTDATA_INSTRUMENT,
    HISTDATA_PROVIDER,
    DatasetManifest,
    DatasetPartition,
    HistoricalQuote,
    SourceLocator,
)
from .walk_forward import (
    WALK_FORWARD_MONTHS,
    WALK_FORWARD_NORMALIZATION_VERSION,
    WALK_FORWARD_PROVIDER,
    WALK_FORWARD_WINDOW_IDS,
    WalkForwardBoundary,
    WalkForwardCursor,
    WalkForwardError,
    WalkForwardInput,
    WalkForwardManifest,
    WalkForwardPartition,
    WalkForwardPhase,
    WalkForwardStreamGuard,
    WalkForwardWindow,
    canonical_walk_forward_windows,
    walk_forward_content_hash,
)

WALK_FORWARD_FIXTURE_SCHEMA = "mtf-lab.walk-forward-fixture.v1"
WALK_FORWARD_FIXTURE_DATA_ROOT = "/tmp/mtf-lab-walk-forward-fixture"
WALK_FORWARD_FIXTURE_SOURCE_URI = "fixture://mtf-lab/walk-forward"
WALK_FORWARD_FIXTURE_TERMS_URI = "fixture://mtf-lab/walk-forward-terms"
WALK_FORWARD_FIXTURE_ACQUIRED_AT = datetime(2026, 1, 1, tzinfo=UTC)

FixtureRunStatus: TypeAlias = Literal["CHECKPOINTED", "COMPLETED"]


class WalkForwardFixtureError(WalkForwardError):
    """The deterministic fixture or its local resume controls are invalid."""


@dataclass(frozen=True, slots=True)
class WalkForwardFixture:
    """Synthetic inputs and one annual stream for a causal WF regression."""

    window: WalkForwardWindow
    development_manifest: DatasetManifest
    walk_forward_manifest: WalkForwardManifest
    input_contract: WalkForwardInput
    quotes: tuple[HistoricalQuote, ...]

    def __post_init__(self) -> None:
        if len(self.quotes) != 13:
            raise WalkForwardFixtureError("fixture must contain exactly 13 quotes")
        boundary = WalkForwardBoundary(self.window)
        phases = tuple(boundary.classify(quote.event_time) for quote in self.quotes)
        if phases != ("WARMUP_ONLY",) + ("WF_EVALUATION",) * 12:
            raise WalkForwardFixtureError("fixture must contain one warmup and twelve selected WF quotes")
        if any(quote.event_time.year != self.window.year for quote in self.quotes[1:]):
            raise WalkForwardFixtureError("fixture WF quotes do not match the selected window")

    @property
    def fixture_id(self) -> str:
        return f"{WALK_FORWARD_FIXTURE_SCHEMA}:{self.window.window_id}"

    def new_guard(self) -> WalkForwardStreamGuard:
        """Create a fresh guard bound to this fixture's two manifests."""

        return WalkForwardStreamGuard(
            self.development_manifest,
            self.walk_forward_manifest,
            window=self.window,
            input_contract=self.input_contract,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return secret-free provenance for the synthetic input only."""

        return {
            "schema": WALK_FORWARD_FIXTURE_SCHEMA,
            "fixture_id": self.fixture_id,
            "window": self.window.to_dict(),
            "development_manifest_hash": self.input_contract.development_manifest_hash,
            "walk_forward_manifest_hash": self.input_contract.walk_forward_manifest_hash,
            "input_hash": self.input_contract.identity_hash,
            "quote_count": len(self.quotes),
            "holdout": "CLOSED",
            "network_performed": False,
            "data_acquisition": False,
            "trading_enabled": False,
        }


@dataclass(frozen=True, slots=True)
class WalkForwardFixtureRun:
    """Deterministic stream output plus the guard snapshot for resume."""

    status: FixtureRunStatus
    fixture_id: str
    window_id: str
    processed_quotes: int
    phases: tuple[WalkForwardPhase, ...]
    output: bytes
    checkpoint: dict[str, Any]

    @property
    def finished(self) -> bool:
        return self.status == "COMPLETED"

    @property
    def output_sha256(self) -> str:
        return hashlib.sha256(self.output).hexdigest()

    @property
    def checkpoint_bytes(self) -> bytes:
        """Canonical checkpoint bytes suitable for a temporary fixture file."""

        return (canonical_json(self.checkpoint) + "\n").encode("utf-8")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": WALK_FORWARD_FIXTURE_SCHEMA,
            "status": self.status,
            "fixture_id": self.fixture_id,
            "window_id": self.window_id,
            "processed_quotes": self.processed_quotes,
            "finished": self.finished,
            "phases": list(self.phases),
            "output_sha256": self.output_sha256,
            "checkpoint": self.checkpoint,
        }


def make_walk_forward_fixture(
    *,
    window: WalkForwardWindow | str = "WF_2020",
) -> WalkForwardFixture:
    """Build the same in-memory warmup→one-year WF stream on every call.

    The manifest still has all 48 canonical 2020–2023 partitions, while the
    selected stream contains one synthetic quote per month of the requested
    annual window.  This exercises the manifest position and annual-window
    checks without pretending that fixture rows are acquired market data.
    """

    selected_window = _coerce_window(window)
    development = _development_manifest()
    walk_forward = _walk_forward_manifest()
    input_contract = WalkForwardInput.from_manifests(
        development,
        walk_forward,
        protocol_hash=_fixture_hash("protocol"),
        risk_hash=_fixture_hash("risk"),
        calendar_hash=_fixture_hash("calendar"),
        costs_hash=_fixture_hash("costs"),
        code_hash=_fixture_hash("code"),
        runtime_hash=_fixture_hash("runtime"),
    )
    quotes = (_development_quote(development),) + tuple(
        _walk_forward_quote(walk_forward, year=selected_window.year, month=month) for month in range(1, 13)
    )
    return WalkForwardFixture(
        window=selected_window,
        development_manifest=development,
        walk_forward_manifest=walk_forward,
        input_contract=input_contract,
        quotes=quotes,
    )


def run_walk_forward_fixture(
    fixture: WalkForwardFixture,
    *,
    stop_after_quotes: int | None = None,
    resume: Mapping[str, Any] | str | None = None,
    prefix: bytes = b"",
) -> WalkForwardFixtureRun:
    """Consume the fixture, optionally stopping and restoring its guard.

    ``prefix`` is the already durable output from a partial run.  On resume,
    only the suffix after the checkpoint cursor is validated and appended.
    Thus ``resumed.output`` is byte-identical to an uninterrupted run when
    called with ``prefix=partial.output``.  The checkpoint itself is only the
    existing ``WalkForwardStreamGuard`` snapshot; no strategy, risk, trade,
    registry, database, raw source, or holdout consumer is involved.
    """

    if not isinstance(fixture, WalkForwardFixture):
        raise WalkForwardFixtureError("fixture must be WalkForwardFixture")
    target = _stop_target(stop_after_quotes, len(fixture.quotes))
    guard = _fixture_guard(fixture, resume)
    start_index = _resume_index(fixture.quotes, guard)
    processed_before = guard.cursor.accepted_count if guard.cursor is not None else 0
    _validate_resume_prefix(fixture, guard, resume, prefix, start_index, processed_before)
    if target is not None and target < processed_before:
        raise WalkForwardFixtureError("stop_after_quotes precedes the resume cursor")

    output = bytearray(prefix)
    phases: list[WalkForwardPhase] = []
    for quote in fixture.quotes[start_index:]:
        if target is not None and processed_before + len(phases) >= target:
            break
        phase = guard.validate(quote)
        output.extend(_render_record(quote, phase))
        phases.append(phase)

    processed = guard.cursor.accepted_count if guard.cursor is not None else 0
    if processed != start_index + len(phases):
        raise WalkForwardFixtureError("cursor accepted_count diverged from consumed fixture quotes")
    finished = processed == len(fixture.quotes)
    if target is not None and not finished and processed < target:
        raise WalkForwardFixtureError("fixture stream ended before stop_after_quotes")
    return WalkForwardFixtureRun(
        status="COMPLETED" if finished else "CHECKPOINTED",
        fixture_id=fixture.fixture_id,
        window_id=fixture.window.window_id,
        processed_quotes=processed,
        phases=tuple(phases),
        output=bytes(output),
        checkpoint=guard.snapshot(),
    )


def _coerce_window(value: WalkForwardWindow | str) -> WalkForwardWindow:
    if isinstance(value, WalkForwardWindow):
        return value
    if not isinstance(value, str) or value not in WALK_FORWARD_WINDOW_IDS:
        raise WalkForwardFixtureError("window must be one of WF_2020..WF_2023")
    return canonical_walk_forward_windows()[WALK_FORWARD_WINDOW_IDS.index(value)]


def _stop_target(value: int | None, quote_count: int) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WalkForwardFixtureError("stop_after_quotes must be a positive integer")
    if value > quote_count:
        raise WalkForwardFixtureError("stop_after_quotes exceeds the fixture quote count")
    return value


def _resume_index(quotes: tuple[HistoricalQuote, ...], guard: WalkForwardStreamGuard) -> int:
    cursor = guard.cursor
    if cursor is None:
        return 0
    for index, quote in enumerate(quotes):
        if quote.source_event_id == cursor.source_event_id:
            if cursor.accepted_count != index + 1:
                raise WalkForwardFixtureError("resume cursor accepted_count is not bound to its source record")
            return index + 1
    raise WalkForwardFixtureError("resume cursor is not present in the fixture stream")


def _fixture_guard(
    fixture: WalkForwardFixture,
    resume: Mapping[str, Any] | str | None,
) -> WalkForwardStreamGuard:
    if resume is None:
        return fixture.new_guard()
    return WalkForwardStreamGuard.from_snapshot(
        fixture.development_manifest,
        fixture.walk_forward_manifest,
        resume,
        input_contract=fixture.input_contract,
    )


def _validate_resume_prefix(
    fixture: WalkForwardFixture,
    guard: WalkForwardStreamGuard,
    resume: Mapping[str, Any] | str | None,
    prefix: bytes,
    end_index: int,
    processed_before: int,
) -> None:
    if not isinstance(prefix, bytes):
        raise WalkForwardFixtureError("prefix must be bytes")
    if prefix and not prefix.endswith(b"\n"):
        raise WalkForwardFixtureError("prefix must contain complete newline-delimited records")
    expected_prefix, expected_cursor = _durable_prefix(fixture, end_index)
    if resume is None:
        if prefix:
            raise WalkForwardFixtureError("prefix requires a resume checkpoint")
    elif prefix != expected_prefix:
        raise WalkForwardFixtureError("prefix does not match the resume checkpoint records")
    if guard.cursor != expected_cursor:
        raise WalkForwardFixtureError("resume cursor does not match the durable fixture prefix")
    if processed_before != end_index:
        raise WalkForwardFixtureError("cursor accepted_count does not match the fixture prefix")


def _durable_prefix(
    fixture: WalkForwardFixture,
    end_index: int,
) -> tuple[bytes, WalkForwardCursor | None]:
    """Render and validate the exact records represented by a checkpoint."""

    if not 0 <= end_index <= len(fixture.quotes):
        raise WalkForwardFixtureError("fixture prefix index is outside the stream")
    guard = fixture.new_guard()
    output = bytearray()
    for quote in fixture.quotes[:end_index]:
        phase = guard.validate(quote)
        output.extend(_render_record(quote, phase))
    return bytes(output), guard.cursor


def _render_record(quote: HistoricalQuote, phase: WalkForwardPhase) -> bytes:
    return (
        canonical_json(
            {
                "phase": phase,
                "event_time": quote.event_time,
                "sequence": quote.sequence,
                "source_event_id": quote.source_event_id,
            }
        )
        + "\n"
    ).encode("utf-8")


def _fixture_hash(label: str) -> str:
    return hashlib.sha256(f"{WALK_FORWARD_FIXTURE_SCHEMA}:{label}".encode()).hexdigest()


def _development_manifest() -> DatasetManifest:
    event_time = datetime(2019, 12, 15, 12, tzinfo=UTC)
    digest = _fixture_hash("development-201912")
    partition = DatasetPartition(
        partition_id="201912",
        raw_archive="raw/fixture-development-201912.zip",
        raw_sha256=digest,
        raw_size=1,
        members=("DAT_ASCII_EURUSD_T_201912.csv",),
        source_uri=WALK_FORWARD_FIXTURE_SOURCE_URI,
        terms_uri=WALK_FORWARD_FIXTURE_TERMS_URI,
        acquired_at=WALK_FORWARD_FIXTURE_ACQUIRED_AT,
        coverage_start=event_time,
        coverage_end=event_time,
        quote_count=1,
        month="201912",
    )
    content_hash = _dataset_content_hash((partition,))
    return DatasetManifest(
        dataset_id=f"{HISTDATA_PROVIDER}:{HISTDATA_INSTRUMENT}:201912:{digest[:32]}",
        provider=HISTDATA_PROVIDER,
        instrument=HISTDATA_INSTRUMENT,
        normalization_version=WALK_FORWARD_NORMALIZATION_VERSION,
        data_root=WALK_FORWARD_FIXTURE_DATA_ROOT,
        partitions=(partition,),
        coverage_start=event_time,
        coverage_end=event_time,
        content_hash=content_hash,
    )


def _walk_forward_manifest() -> WalkForwardManifest:
    partitions = tuple(_walk_forward_partition(month) for month in WALK_FORWARD_MONTHS)
    content_hash = walk_forward_content_hash(partitions)
    return WalkForwardManifest(
        walk_forward_id=f"{WALK_FORWARD_PROVIDER}:{HISTDATA_INSTRUMENT}:202001-202312:{content_hash[:32]}",
        provider=WALK_FORWARD_PROVIDER,
        instrument=HISTDATA_INSTRUMENT,
        normalization_version=WALK_FORWARD_NORMALIZATION_VERSION,
        data_root=WALK_FORWARD_FIXTURE_DATA_ROOT,
        partitions=partitions,
        coverage_start=datetime(2020, 1, 15, 12, tzinfo=UTC),
        coverage_end=datetime(2023, 12, 15, 12, tzinfo=UTC),
        content_hash=content_hash,
    )


def _walk_forward_partition(month: str) -> WalkForwardPartition:
    event_time = _month_time(month)
    digest = _fixture_hash(f"walk-forward-{month}")
    member = f"DAT_ASCII_EURUSD_T_{month}.csv"
    return WalkForwardPartition(
        partition_id=month,
        month=month,
        raw_archive=f"raw/fixture-walk-forward-{month}.zip",
        raw_sha256=digest,
        raw_size=1,
        members=(member,),
        source_uri=WALK_FORWARD_FIXTURE_SOURCE_URI,
        terms_uri=WALK_FORWARD_FIXTURE_TERMS_URI,
        acquired_at=WALK_FORWARD_FIXTURE_ACQUIRED_AT,
        coverage_start=event_time,
        coverage_end=event_time,
        quote_count=1,
    )


def _development_quote(manifest: DatasetManifest) -> HistoricalQuote:
    partition = manifest.partitions[0]
    return _quote(
        archive=partition.raw_archive,
        member=partition.members[0],
        digest=partition.raw_sha256,
        event_time=datetime(2019, 12, 15, 12, tzinfo=UTC),
        sequence=0,
    )


def _walk_forward_quote(manifest: WalkForwardManifest, *, year: int, month: int) -> HistoricalQuote:
    month_id = f"{year:04d}{month:02d}"
    partition = manifest.partitions[WALK_FORWARD_MONTHS.index(month_id)]
    return _quote(
        archive=partition.raw_archive,
        member=partition.members[0],
        digest=partition.raw_sha256,
        event_time=_month_time(month_id),
        sequence=month,
    )


def _quote(
    *,
    archive: str,
    member: str,
    digest: str,
    event_time: datetime,
    sequence: int,
) -> HistoricalQuote:
    locator = SourceLocator(archive, member, digest, offset=0, row=1)
    return HistoricalQuote(
        instrument=HISTDATA_INSTRUMENT,
        event_time=event_time,
        available_at=event_time,
        bid=Decimal("1.10000") + Decimal(sequence) / Decimal("1000000"),
        ask=Decimal("1.10020") + Decimal(sequence) / Decimal("1000000"),
        locator=locator,
        sequence=sequence,
        source_event_id=f"{digest}:{member}:1:0",
        provider=HISTDATA_PROVIDER,
    )


def _month_time(month: str) -> datetime:
    year = int(month[:4])
    number = int(month[4:])
    return datetime(year, number, 15, 12, tzinfo=UTC)


def _dataset_content_hash(partitions: tuple[DatasetPartition, ...]) -> str:
    material = "\n".join(
        f"{partition.partition_id}\0{partition.raw_sha256}\0{partition.raw_size}\0{','.join(partition.members)}"
        for partition in partitions
    )
    return hashlib.sha256(material.encode()).hexdigest()


__all__ = [
    "WALK_FORWARD_FIXTURE_ACQUIRED_AT",
    "WALK_FORWARD_FIXTURE_DATA_ROOT",
    "WALK_FORWARD_FIXTURE_SCHEMA",
    "WALK_FORWARD_FIXTURE_SOURCE_URI",
    "WALK_FORWARD_FIXTURE_TERMS_URI",
    "FixtureRunStatus",
    "WalkForwardFixture",
    "WalkForwardFixtureError",
    "WalkForwardFixtureRun",
    "make_walk_forward_fixture",
    "run_walk_forward_fixture",
]
