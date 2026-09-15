"""Dispatch for the explicitly requested historical-data operations.

The descriptive operation is deliberately narrower than the campaign runner:
it only consumes immutable historical quotes, writes an optional descriptive
report, and never evaluates a strategy or creates a fill. When the caller
provides a registry, the descriptive attempt is registered before the quote
stream is consumed and is closed with a bounded receipt (or an error).

Acquisition has a distinct action and consent flag. Merely validating or
describing a local manifest never imports or constructs a downloader or a
broker provider.
"""

from __future__ import annotations

import argparse
import platform
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, TypeAlias, cast

from ..core.canonical import fingerprint, instant_text
from ..core.historical_calendar import HistoricalQuoteCalendar
from ..data.dukascopy import DukascopyManifest
from ..data.historical import (
    DatasetManifest,
    HistoricalManifest,
    HistoricalQuote,
)
from .global_trial_registry import GlobalTrialRegistry

DESCRIPTIVE_CANDIDATE_ID = "NO_TRADE_DESCRIPTIVE"
DESCRIPTIVE_SCOPE = "DESCRIPTIVE_NO_STRATEGY"
DESCRIPTIVE_SCHEMA = "mtf-lab.market-data-description.v1"
_DEVELOPMENT_START = datetime(2016, 1, 1, tzinfo=UTC)
_DEVELOPMENT_END = datetime(2020, 1, 1, tzinfo=UTC)


class HistoricalProvider(Protocol):
    """Compatible provider facade for an already-acquired manifest."""

    @property
    def manifest(self) -> Any:
        """Return the provider's canonical manifest DTO."""
        ...

    def stream(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterable[HistoricalQuote]: ...


HistoricalManifestInput: TypeAlias = HistoricalManifest | HistoricalProvider
_ManifestObject: TypeAlias = DatasetManifest | DukascopyManifest
_StreamFactory: TypeAlias = Callable[[datetime, datetime], Iterable[HistoricalQuote]]


def _time(value: datetime | str | None) -> datetime | None:
    """Parse an explicit timezone-bearing UTC instant."""

    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("la ventana temporal debe ser ISO-8601 o datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("la ventana temporal debe incluir zona horaria")
    return parsed.astimezone(UTC)


def _validated_window(
    start_value: datetime | str | None,
    end_value: datetime | str | None,
) -> tuple[datetime, datetime]:
    """Validate the development-only window before touching any manifest."""

    start, end = _time(start_value), _time(end_value)
    if start is None or end is None or end <= start or start < _DEVELOPMENT_START or end > _DEVELOPMENT_END:
        raise ValueError("descripción permitida sólo en desarrollo 2016–2019; holdout cerrado")
    return start, end


def _calendar_mode(value: str) -> str:
    mode = str(value).strip().lower()
    if mode not in {"strict", "modeled-fx"}:
        raise ValueError("weekly_calendar debe ser strict o modeled-fx")
    return mode


def _default_runtime_identity() -> dict[str, Any]:
    """Return a deliberately small, secret-free local runtime identity."""

    return {
        "component": "mtf_lab.ops.market_data_service",
        "operation": "market_data_describe",
        "execution": "local_offline",
        "python": platform.python_version(),
    }


def _runtime_identity(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return _default_runtime_identity()
    if not isinstance(value, Mapping):
        raise ValueError("runtime_identity debe ser un mapping")
    # GlobalTrialRegistry applies the final secret/type guard. Keep this
    # identity bounded so a caller cannot accidentally register an environment
    # dump as part of a descriptive attempt.
    if len(value) > 16:
        raise ValueError("runtime_identity excede el límite de campos")
    return {str(key): item for key, item in value.items()}


def _manifest_metadata(manifest: _ManifestObject) -> dict[str, str | None]:
    """Extract only stable, bounded metadata common to both providers."""

    dataset_id = getattr(manifest, "dataset_id", None)
    provider = getattr(manifest, "provider", None)
    instrument = getattr(manifest, "instrument", None)
    content_hash = getattr(manifest, "content_hash", None)
    normalization = getattr(manifest, "normalization_version", None)
    if not all(
        isinstance(item, str) and item for item in (dataset_id, provider, instrument, content_hash, normalization)
    ):
        raise ValueError("manifest histórico carece de identidad canónica")
    return {
        "dataset_id": dataset_id,
        "provider": provider,
        "instrument": instrument,
        "manifest_hash": content_hash,
        "normalization_version": normalization,
    }


def _resolve_manifest(
    value: HistoricalManifestInput | str | Path,
) -> tuple[_ManifestObject, _StreamFactory]:
    """Resolve a path, manifest, or compatible provider without guessing."""

    # Keep these imports lazy: importing the dispatch module must not resolve a
    # dataset path, and tests/integrators can observe the canonical reader at
    # mtf_lab.data.historical.read_manifest.
    from ..data.historical import iter_quotes as historical_iter_quotes
    from ..data.historical import read_manifest as historical_read_manifest

    if isinstance(value, (str, Path)):
        resolved = historical_read_manifest(value)
        if not isinstance(resolved, (DatasetManifest, DukascopyManifest)):
            raise ValueError("manifest histórico no soportado")
        return resolved, lambda start, end: historical_iter_quotes(resolved, start, end)
    if isinstance(value, (DatasetManifest, DukascopyManifest)):
        return value, lambda start, end: historical_iter_quotes(value, start, end)

    provider_manifest = getattr(value, "manifest", None)
    if not isinstance(provider_manifest, (DatasetManifest, DukascopyManifest)):
        raise ValueError("provider histórico debe exponer un DatasetManifest o DukascopyManifest")
    stream_method = getattr(value, "stream", None)
    if not callable(stream_method):
        raise ValueError("provider histórico debe exponer stream()")

    def stream_from_provider(start: datetime, end: datetime) -> Iterable[HistoricalQuote]:
        return cast(Iterable[HistoricalQuote], stream_method(start=start, end=end))

    return provider_manifest, stream_from_provider


def _registry(value: GlobalTrialRegistry | str | Path | None) -> GlobalTrialRegistry | None:
    if value is None:
        return None
    if isinstance(value, GlobalTrialRegistry):
        return value
    return GlobalTrialRegistry(value)


def _scope(
    metadata: Mapping[str, str | None],
    *,
    start: datetime,
    end: datetime,
    calendar_mode: str,
) -> dict[str, Any]:
    return {
        "scope": DESCRIPTIVE_SCOPE,
        "operation": "market-data.describe",
        "provider": metadata["provider"],
        "instrument": metadata["instrument"],
        "dataset_id": metadata["dataset_id"],
        "manifest_hash": metadata["manifest_hash"],
        "window_start": instant_text(start),
        "window_end": instant_text(end),
        "calendar_mode": calendar_mode,
    }


def _receipt_base(
    metadata: Mapping[str, str | None],
    scope: Mapping[str, Any],
    runtime: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema": DESCRIPTIVE_SCHEMA,
        "candidate_id": DESCRIPTIVE_CANDIDATE_ID,
        "scope": DESCRIPTIVE_SCOPE,
        "manifest_hash": metadata["manifest_hash"],
        "dataset_id": metadata["dataset_id"],
        "window_start": scope["window_start"],
        "window_end": scope["window_end"],
        "calendar_mode": scope["calendar_mode"],
        "runtime_identity": dict(runtime),
        "network_performed": False,
        "economic_conclusion": "NOT_ASSESSED",
        "profitability_claim": "NONE",
        "fills": "NONE_DESCRIPTIVE_ONLY",
    }


def _protocol_hash(scope: Mapping[str, Any], runtime: Mapping[str, Any]) -> str:
    return fingerprint(
        {
            "schema": DESCRIPTIVE_SCHEMA,
            "candidate_id": DESCRIPTIVE_CANDIDATE_ID,
            "scope": dict(scope),
            "runtime_identity": dict(runtime),
        }
    )


def _close_failed(
    registry: GlobalTrialRegistry,
    attempt_id: str,
    receipt: Mapping[str, Any],
    error: Exception,
) -> None:
    failure_receipt = dict(receipt)
    failure_receipt["status"] = "FAILED"
    failure_receipt["error_type"] = type(error).__name__
    failure_receipt["error"] = str(error)
    try:
        registry.update_status(
            attempt_id,
            "FAILED",
            details={
                "receipt": failure_receipt,
                "error": str(error),
                "error_type": type(error).__name__,
            },
        )
    except Exception:
        # The original operation error remains authoritative. A broken
        # registry close must not be mistaken for a successful description.
        return


def describe_market_data(
    manifest: HistoricalManifestInput | str | Path,
    *,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    calendar_mode: str = "strict",
    weekly_calendar: str | None = None,
    registry: GlobalTrialRegistry | str | Path | None = None,
    output_dir: str | Path | None = None,
    runtime_identity: Mapping[str, Any] | None = None,
    max_quote_gap_seconds: float = 90.0,
) -> dict[str, Any]:
    """Describe historical quotes, optionally registering the attempt.

    Temporal and calendar gates execute first. Consequently a holdout or
    malformed window cannot read a manifest, instantiate a registry, invoke a
    provider, or create a registry record. The manifest argument may be a
    local path, either provider's manifest DTO, or a compatible provider facade
    exposing manifest and keyword-only stream(start=..., end=...).
    """

    # Keep this preflight before _resolve_manifest and _registry: callers use it
    # as the hard holdout barrier.
    selected_calendar_mode = _calendar_mode(weekly_calendar if weekly_calendar is not None else calendar_mode)
    if (
        weekly_calendar is not None
        and calendar_mode != "strict"
        and _calendar_mode(calendar_mode) != selected_calendar_mode
    ):
        raise ValueError("calendar_mode y weekly_calendar no pueden discrepar")
    selected_start, selected_end = _validated_window(start, end)
    if max_quote_gap_seconds <= 0:
        raise ValueError("max_quote_gap_seconds debe ser positivo")

    selected_manifest, stream_factory = _resolve_manifest(manifest)
    metadata = _manifest_metadata(selected_manifest)
    scope = _scope(
        metadata,
        start=selected_start,
        end=selected_end,
        calendar_mode=selected_calendar_mode,
    )
    runtime = _runtime_identity(runtime_identity)
    selected_registry = _registry(registry)
    protocol_hash = _protocol_hash(scope, runtime)
    receipt: dict[str, Any] = _receipt_base(metadata, scope, runtime)
    registered_attempt: dict[str, Any] | None = None
    attempt_id: str | None = None

    if selected_registry is not None:
        registered_attempt = selected_registry.register_attempt(
            candidate_id=DESCRIPTIVE_CANDIDATE_ID,
            protocol_hash=protocol_hash,
            dataset_hash=str(metadata["manifest_hash"]),
            runtime_identity=runtime,
            data_identity={
                "provider": metadata["provider"],
                "instrument": metadata["instrument"],
                "dataset_id": metadata["dataset_id"],
                "manifest_hash": metadata["manifest_hash"],
            },
            scope=scope,
            mode="HISTORICAL",
            parameters={
                "calendar_mode": selected_calendar_mode,
                "max_quote_gap_seconds": max_quote_gap_seconds,
            },
        )
        attempt_id = str(registered_attempt["attempt_id"])

    try:
        from .market_structure import describe_market_structure

        calendar = HistoricalQuoteCalendar() if selected_calendar_mode == "modeled-fx" else None
        structure = describe_market_structure(
            stream_factory(selected_start, selected_end),
            # describe_market_structure predates the provider-neutral manifest
            # union; both DTOs expose this common identity contract.
            manifest=cast(Any, selected_manifest),
            max_quote_gap_seconds=max_quote_gap_seconds,
            historical_calendar=calendar,
        )
        generated_at = datetime.now(UTC).isoformat()
        payload: dict[str, Any] = {
            "schema": DESCRIPTIVE_SCHEMA,
            "market_structure": structure,
            "manifest": dict(metadata),
            "window": {
                "start": instant_text(selected_start),
                "end": instant_text(selected_end),
            },
            "calendar_mode": selected_calendar_mode,
            "runtime_identity": dict(runtime),
            "provenance": {
                "provider": metadata["provider"],
                "dataset_id": metadata["dataset_id"],
                "data_hash": metadata["manifest_hash"],
                "manifest_hash": metadata["manifest_hash"],
                "source_mode": "HISTORICAL",
                "received_at": "UNKNOWN_NOT_OBSERVED",
                "fills": "NONE_DESCRIPTIVE_ONLY",
            },
            "generated_at": generated_at,
            "network_performed": False,
            "economic_conclusion": "NOT_ASSESSED",
            "profitability_claim": "NONE",
            "strategy_evaluation": "NOT_REQUESTED",
            "trading_enabled": False,
        }
        receipt.update(
            {
                "status": "COMPLETED",
                "generated_at": generated_at,
                "quote_count": structure["quote_statistics"]["quote_count"],
            }
        )
        if attempt_id is not None:
            payload["registry"] = {
                "attempt_id": attempt_id,
                "candidate_id": DESCRIPTIVE_CANDIDATE_ID,
                "status": "COMPLETED",
                "scope": DESCRIPTIVE_SCOPE,
            }

        output = Path(output_dir) if output_dir is not None else None
        if output is not None:
            from .research_reporting import render_research_report

            paths = render_research_report(payload, output)
            receipt.update({"report_json": str(paths.json_path), "report_html": str(paths.html_path)})
            result: dict[str, Any] = {
                "ok": True,
                "quote_count": structure["quote_statistics"]["quote_count"],
                "report_json": str(paths.json_path),
                "report_html": str(paths.html_path),
                "network_performed": False,
                "economic_conclusion": "NOT_ASSESSED",
                "profitability_claim": "NONE",
            }
            if attempt_id is not None:
                result["registry"] = payload["registry"]
        else:
            result = payload

        if selected_registry is not None and attempt_id is not None:
            selected_registry.update_status(
                attempt_id,
                "COMPLETED",
                details={"receipt": receipt},
            )
        return result
    except Exception as error:
        if selected_registry is not None and attempt_id is not None:
            _close_failed(selected_registry, attempt_id, receipt, error)
        raise


# A descriptive name for callers that do not use the CLI-bound term.
describe_historical_data = describe_market_data


def _describe(args: argparse.Namespace) -> dict[str, Any]:
    """Adapt the CLI namespace without adding or owning CLI flags."""

    return describe_market_data(
        args.manifest,
        start=getattr(args, "start", None),
        end=getattr(args, "end", None),
        weekly_calendar=getattr(args, "weekly_calendar", "strict"),
        registry=getattr(args, "registry", None),
        output_dir=getattr(args, "output_dir", None),
        runtime_identity=getattr(args, "runtime_identity", None),
    )


def market_data_command(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    if args.market_data_action == "acquire":
        if not getattr(args, "terms_accepted", False):
            return 2, {
                "ok": False,
                "state": "TERMS_GATE",
                "error": "Resuelve las condiciones de acceso y uso antes de --terms-accepted.",
                "network_performed": False,
            }
        if str(args.month) == "2016-03":
            from ..data.histdata_acquisition import acquire

            receipt = acquire(
                provider=args.provider,
                instrument=args.instrument,
                month=args.month,
                data_root=args.data_root,
                terms_accepted=args.terms_accepted,
                timeout_seconds=args.timeout,
            )
        else:
            from ..data.histdata_acquisition import acquire_month

            year_text, month_text = str(args.month).split("-", 1)
            receipt = acquire_month(
                int(year_text),
                int(month_text),
                data_root=args.data_root,
                terms_accepted=args.terms_accepted,
                timeout_seconds=args.timeout,
            )
        return 0, {"ok": True, **receipt.to_dict()}
    if args.market_data_action == "validate":
        from ..data.historical import validate_dataset

        result = validate_dataset(
            args.manifest,
            _time(getattr(args, "start", None)),
            _time(getattr(args, "end", None)),
        )
        return (0 if result.ok else 2), {**result.to_dict(), "network_performed": False}
    if args.market_data_action == "describe":
        return 0, _describe(args)
    raise ValueError("acción market-data no soportada")


__all__ = [
    "DESCRIPTIVE_CANDIDATE_ID",
    "DESCRIPTIVE_SCOPE",
    "HistoricalProvider",
    "describe_historical_data",
    "describe_market_data",
    "market_data_command",
]
