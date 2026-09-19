"""Focused contracts for the opt-in LIVE technical canary quote coverage."""

from __future__ import annotations

import copy
import hashlib
import json
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from mtf_lab.configuration import load_config
from mtf_lab.core import DataQuality, EventKind, IndicatorConfig, MarketEvent, OperationMode, PriceBase, QualityFlag
from mtf_lab.runtime import RuntimeCoordinator

BASE = datetime(2026, 9, 21, 14, 55, 5, tzinfo=UTC)


class _MemoryStore:
    """Small persistence double; these tests never open SQLite."""

    def __init__(self) -> None:
        self.identities: list[dict[str, Any]] = []
        self.checkpoints: dict[tuple[str, str, str], dict[str, Any]] = {}

    def create_analysis(self, session_id: str, **kwargs: Any) -> str:
        identity_extra = dict(kwargs.get("identity_extra", {}))
        identity = {"session_id": session_id, **kwargs, "identity_extra": identity_extra}
        self.identities.append(identity)
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()[:12]
        return f"analysis-{digest}"

    def get_checkpoint(
        self,
        session_id: str,
        checkpoint_name: str,
        *,
        analysis_id: str | None = None,
        allow_alternate: bool = False,
    ) -> dict[str, Any] | None:
        del allow_alternate
        key = (session_id, checkpoint_name, str(analysis_id))
        value = self.checkpoints.get(key)
        return copy.deepcopy(value) if value is not None else None

    def save_checkpoint(
        self,
        session_id: str,
        checkpoint_name: str,
        *,
        cursor: dict[str, Any],
        events_processed: int,
        last_event_id: str | None,
        state: dict[str, Any],
        analysis_id: str | None = None,
    ) -> None:
        self.checkpoints[(session_id, checkpoint_name, str(analysis_id))] = {
            "cursor": dict(cursor),
            "events_processed": events_processed,
            "last_event_id": last_event_id,
            "state": copy.deepcopy(state),
        }

    def save_event(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    save_candle = save_event
    save_decision = save_event
    save_signal = save_event
    save_discard = save_event
    update_simulation = save_event


def _technical_config(**changes: Any) -> Any:
    base = load_config("config/fixture_cfd.toml")
    mode = changes.pop("mode", "LIVE")
    execution = {
        "market_candidate_id": "tp_fast_v1",
        "max_price_age_seconds": 30.0,
    }
    execution.update(changes.pop("execution", {}))
    return replace(base, mode=mode, execution=execution, **changes)


def _quote(at: datetime, index: int, *, quality: DataQuality | None = None) -> MarketEvent:
    value = 1.1000 + index * 0.00001
    return MarketEvent(
        instrument="EUR/USD",
        event_time=at,
        bid=value,
        ask=value + 0.0002,
        received_at=at,
        available_at=at,
        source="technical-canary-fixture",
        mode=OperationMode.LIVE,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        sequence=index,
        source_event_id=f"canary-quote-{index}",
        quality=quality or DataQuality.good(source="technical-canary-fixture"),
    )


def _stream(count: int = 104) -> list[MarketEvent]:
    return [_quote(BASE + timedelta(seconds=10 * index), index) for index in range(count)]


def _coordinator(
    *,
    technical_gap: float | None,
    store: _MemoryStore | None = None,
    config: Any = None,
    resume: bool = False,
) -> tuple[RuntimeCoordinator, _MemoryStore]:
    memory = store or _MemoryStore()
    effective_config = config or _technical_config()
    coordinator = RuntimeCoordinator(
        memory,
        "technical-canary-session",
        effective_config,
        mode=effective_config.mode,
        dataset_hash="technical-canary-fixture",
        checkpoint_every=10_000,
        resume=resume,
        technical_canary_quote_gap_seconds=technical_gap,
    )
    return coordinator, memory


class TechnicalCanaryRuntimeTests(unittest.TestCase):
    def test_opt_in_selects_continuous_quotes_and_never_emits_signals(self) -> None:
        coordinator, memory = _coordinator(technical_gap=10.0)

        self.assertEqual(coordinator.processor.quote_coverage_mode, "continuous_quotes")
        self.assertEqual(coordinator.processor.max_quote_gap_seconds, 10.0)
        self.assertFalse(coordinator.can_emit_signals())
        self.assertEqual(memory.identities[-1]["identity_extra"]["technical_canary_quote_gap_seconds"], 10.0)

        results = [coordinator.process(record) for record in _stream()]
        self.assertTrue(all(result.accepted for result in results))
        self.assertFalse(coordinator.can_emit_signals())
        self.assertEqual(coordinator.signals, ())
        self.assertEqual(coordinator.processor.pending_simulations, ())
        self.assertEqual(coordinator.processor.quote_coverage_mode, "continuous_quotes")

    def test_first_partial_is_localized_but_atr_uses_fourteen_valid_m1_candles(self) -> None:
        coordinator, _ = _coordinator(technical_gap=10.0)
        results = [coordinator.process(record) for record in _stream()]

        self.assertTrue(any(issue.code == "partial_bucket" for result in results for issue in result.issues))
        partial_candles = [
            candle for result in results for candle in result.candles if candle.metadata.get("partial") is True
        ]
        self.assertTrue(partial_candles)
        partial = partial_candles[0]
        self.assertFalse(partial.quality.valid)

        points = tuple(coordinator.processor.indicator_points["M1"])
        self.assertTrue(points)
        self.assertIsNone(points[0].atr)
        atr_points = [point for point in points if point.quality.valid and point.atr is not None]
        self.assertTrue(atr_points)
        first_atr = atr_points[0]
        valid_before_atr = [point for point in points if point.quality.valid and point.end <= first_atr.end]
        self.assertGreaterEqual(len(valid_before_atr), 14)

    def test_default_live_path_keeps_startup_partial_broken(self) -> None:
        coordinator, _ = _coordinator(technical_gap=None)
        results = [coordinator.process(record) for record in _stream(count=8)]

        self.assertEqual(coordinator.processor.quote_coverage_mode, "strict")
        self.assertTrue(any(issue.code == "partial_bucket" for result in results for issue in result.issues))
        self.assertEqual(coordinator.continuity_state, "BROKEN")
        self.assertFalse(coordinator.can_emit_signals())

    def test_gap_quality_timestamp_and_source_issues_never_promote_continuity(self) -> None:
        coordinator, _ = _coordinator(technical_gap=10.0)
        coordinator.process(_quote(BASE, 0))
        gap_results = [
            coordinator.process(_quote(BASE + timedelta(seconds=31), 1)),
            coordinator.process(_quote(BASE + timedelta(seconds=62), 2)),
        ]
        self.assertTrue(
            any(issue.code in {"gap", "quality_blocked"} for result in gap_results for issue in result.issues)
        )
        self.assertEqual(coordinator.continuity_state, "BROKEN")
        self.assertFalse(coordinator.can_emit_signals())

        bad_timestamp, _ = _coordinator(technical_gap=10.0)
        malformed = {
            "instrument": "EUR/USD",
            "event_time": BASE.isoformat().replace("+00:00", "Z"),
            "available_at": (BASE - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            "bid": 1.1,
            "ask": 1.1002,
            "price_base": "mid",
            "event_kind": "quote",
            "mode": "LIVE",
            "source": "technical-canary-fixture",
            "source_event_id": "future-availability",
        }
        bad_timestamp.process(malformed)
        self.assertEqual(bad_timestamp.continuity_state, "BROKEN")
        self.assertFalse(bad_timestamp.can_emit_signals())

        source_bad, _ = _coordinator(technical_gap=10.0)
        source_bad.process(_quote(BASE, 0, quality=DataQuality.from_flag(QualityFlag.INVALID, "source_issue")))
        self.assertEqual(source_bad.continuity_state, "BROKEN")
        self.assertFalse(source_bad.can_emit_signals())

    def test_opt_in_checkpoint_and_resume_reject_mismatch(self) -> None:
        coordinator, store = _coordinator(technical_gap=10.0)
        snapshot = coordinator.export_state()
        self.assertEqual(snapshot["technical_canary_quote_gap_seconds"], 10.0)

        coordinator.restore_state(snapshot)
        resumed, _ = _coordinator(technical_gap=10.0, store=store, resume=True)
        self.assertEqual(resumed.processor.quote_coverage_mode, "continuous_quotes")
        self.assertEqual(resumed.processor.max_quote_gap_seconds, 10.0)

        tampered = dict(snapshot)
        tampered["technical_canary_quote_gap_seconds"] = 11.0
        with self.assertRaisesRegex(ValueError, "technical_canary_quote_gap_seconds"):
            coordinator.restore_state(tampered)

    def test_opt_in_validation_is_narrow_and_default_identity_has_no_extra(self) -> None:
        strict_store = _MemoryStore()
        _coordinator(technical_gap=None, store=strict_store)
        self.assertNotIn("technical_canary_quote_gap_seconds", strict_store.identities[-1]["identity_extra"])

        invalid = (
            {"mode": "REPLAY"},
            {"price_base": "traded"},
            {"execution": {"market_candidate_id": "dc_m5_v1", "max_price_age_seconds": 30.0}},
            {"indicators": IndicatorConfig(atr_period=13)},
        )
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                config = _technical_config(**changes)
                _coordinator(technical_gap=10.0, config=config)
        for gap in (0.0, 31.0, 31.0):
            with self.subTest(gap=gap), self.assertRaises(ValueError):
                _coordinator(technical_gap=gap)


if __name__ == "__main__":
    unittest.main()
