"""Contract tests for the explicit data <-> core translation boundary."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest

from mtf_lab.core.models import Candle as CoreCandle
from mtf_lab.core.models import EventKind, MarketEvent as CoreEvent, OperationMode, PriceBase, parse_timeframe
from mtf_lab.core.quality import DataQuality as CoreQuality, QualityFlag
from mtf_lab.data.models import Bar as DataBar
from mtf_lab.data.models import Event as DataEvent
from mtf_lab.data.translation import (
    TranslationError,
    bootstrap_reconcile,
    core_bar_to_data,
    core_event_to_data,
    data_bar_to_core,
    data_event_to_core,
    reconcile_native_candles,
)


UTC = timezone.utc
T0 = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)


def provenance(*, mode: str = "REPLAY", synthetic: bool = False, basis: str = "traded") -> dict[str, object]:
    return {
        "provider": "fixture",
        "mode": mode,
        "instrument": "TEST/USD",
        "resolutions": ["M1"],
        "price_basis": basis,
        "source_uri": "file:///tmp/fixture.jsonl",
        "source_hash": "abc123",
        "generated_seed": 11 if synthetic else None,
        "synthetic": synthetic,
        "notes": ["fixture provenance"],
    }


class TranslationTests(unittest.TestCase):
    def test_event_roundtrip_preserves_identity_quality_and_provenance(self) -> None:
        original = DataEvent(
            instrument="TEST/USD",
            event_time=T0,
            price=101.25,
            quantity=2.0,
            source="fixture",
            source_event_id="trade-01",
            source_sequence=7,
            received_at=T0 + timedelta(seconds=1),
            available_at=T0 + timedelta(seconds=2),
            synthetic=True,
            metadata={
                "quality_flags": ["synthetic", "partial"],
                "quality_reasons": ["fixture only"],
                "provenance": provenance(mode="SYNTHETIC", synthetic=True),
            },
        )
        core = data_event_to_core(original)
        self.assertEqual(core.event_id, original.data_id)
        self.assertEqual(core.mode, OperationMode.SYNTHETIC)
        self.assertEqual(core.price_base, PriceBase.TRADED)
        self.assertEqual(core.quality.flags, frozenset({QualityFlag.SYNTHETIC, QualityFlag.PARTIAL}))
        self.assertEqual(core.quality.reasons, ("fixture only",))
        restored = core_event_to_data(core)
        self.assertEqual(restored.data_id, original.data_id)
        self.assertEqual(restored.source_event_id, original.source_event_id)
        self.assertEqual(restored.source_sequence, original.source_sequence)
        self.assertEqual(restored.received_at, original.received_at)
        self.assertEqual(restored.available_at, original.available_at)
        self.assertEqual(restored.metadata["provenance"], original.metadata["provenance"])
        self.assertEqual(set(restored.metadata["quality_flags"]), {"synthetic", "partial"})

    def test_quote_bases_are_explicit_and_not_silently_substituted(self) -> None:
        event = DataEvent(
            instrument="TEST/USD",
            event_time=T0,
            price=100.5,
            bid=100.0,
            ask=101.0,
            mid=100.5,
            price_basis="mid",
            source="fixture",
            metadata={"event_kind": "quote"},
        )
        core = data_event_to_core(event)
        self.assertIsNone(core.price)
        self.assertEqual(core.price_base, PriceBase.MID)
        self.assertEqual(core.event_kind, EventKind.QUOTE)
        self.assertEqual(core.selected_price, 100.5)
        restored = core_event_to_data(core)
        self.assertEqual(restored.price_basis, "mid")
        self.assertEqual(restored.mid, 100.5)
        self.assertEqual(restored.bid, 100.0)
        self.assertEqual(restored.ask, 101.0)
        with self.assertRaises(TranslationError):
            data_event_to_core(
                DataEvent(
                    instrument="TEST/USD",
                    event_time=T0,
                    price=100.5,
                    mid=100.5,
                    price_basis="mid",
                )
            )

    def test_unknown_mode_base_quality_and_identity_are_blocking(self) -> None:
        with self.assertRaises(TranslationError) as mode_error:
            data_event_to_core(
                DataEvent(
                    instrument="TEST/USD",
                    event_time=T0,
                    price=100,
                    metadata={"provenance": {"mode": "MAYBE"}},
                )
            )
        self.assertTrue(mode_error.exception.blocking)
        with self.assertRaises(TranslationError) as quality_error:
            data_event_to_core(
                DataEvent(
                    instrument="TEST/USD",
                    event_time=T0,
                    price=100,
                    metadata={"quality_flags": ["not-a-quality-state"]},
                )
            )
        self.assertEqual(quality_error.exception.code, "QUALITY_UNKNOWN")
        # Dataclass construction normally rejects this already.  A malformed
        # object at an integration boundary must still be rejected by the
        # translator rather than defaulting to traded.
        malformed = object.__new__(DataEvent)
        object.__setattr__(malformed, "instrument", "TEST/USD")
        object.__setattr__(malformed, "event_time", T0)
        object.__setattr__(malformed, "price", 100.0)
        object.__setattr__(malformed, "bid", None)
        object.__setattr__(malformed, "ask", None)
        object.__setattr__(malformed, "mid", None)
        object.__setattr__(malformed, "price_basis", "future-basis")
        object.__setattr__(malformed, "quantity", None)
        object.__setattr__(malformed, "received_at", None)
        object.__setattr__(malformed, "available_at", None)
        object.__setattr__(malformed, "source", "fixture")
        object.__setattr__(malformed, "source_event_id", None)
        object.__setattr__(malformed, "source_sequence", None)
        object.__setattr__(malformed, "side", None)
        object.__setattr__(malformed, "is_snapshot", False)
        object.__setattr__(malformed, "synthetic", False)
        object.__setattr__(malformed, "metadata", {})
        with self.assertRaises(TranslationError):
            data_event_to_core(malformed)
        with self.assertRaises(TranslationError):
            data_event_to_core(
                DataEvent(
                    instrument="TEST/USD",
                    event_time=T0,
                    price=100,
                    metadata={"mode": "UNKNOWN"},
                )
            )
        with self.assertRaises(TranslationError):
            data_event_to_core(
                DataEvent(
                    instrument="TEST/USD",
                    event_time=T0,
                    price=100,
                    metadata={"_mtf_data_identity": {"core_event_id": []}},
                )
            )

    def test_bar_roundtrip_preserves_revision_identity_quality_and_times(self) -> None:
        original = DataBar(
            instrument="TEST/USD",
            interval_start=T0,
            interval_end=T0 + timedelta(minutes=1),
            open=100,
            high=102,
            low=99,
            close=101,
            resolution_seconds=60,
            volume=None,
            trade_count=None,
            source="fixture",
            source_record_id="bar-01",
            received_at=T0 + timedelta(minutes=1, seconds=1),
            available_at=T0 + timedelta(minutes=1, seconds=2),
            closed=True,
            synthetic=True,
            revision=4,
            metadata={
                "quality_flags": ["synthetic", "gap"],
                "quality_reasons": ["native/event check pending"],
                "provenance": provenance(mode="SYNTHETIC", synthetic=True),
                "origin": "native",
            },
        )
        core = data_bar_to_core(original)
        self.assertEqual(core.candle_id, original.data_id)
        self.assertEqual(core.mode, OperationMode.SYNTHETIC)
        self.assertEqual(core.quality.flags, frozenset({QualityFlag.SYNTHETIC, QualityFlag.GAP}))
        self.assertEqual(core.metadata["revision"], 4)
        restored = core_bar_to_data(core)
        self.assertEqual(restored.data_id, original.data_id)
        self.assertEqual(restored.source_record_id, "bar-01")
        self.assertEqual(restored.revision, 4)
        self.assertIsNone(restored.volume)
        self.assertIsNone(restored.trade_count)
        self.assertEqual(restored.metadata["provenance"], original.metadata["provenance"])
        self.assertEqual(set(restored.metadata["quality_flags"]), {"synthetic", "gap"})

    def test_core_to_data_to_core_keeps_core_ids_and_live_mode(self) -> None:
        quality = CoreQuality(frozenset({QualityFlag.LATE}), ("feed delayed",), "kraken")
        event = CoreEvent(
            instrument="TEST/USD",
            event_time=T0,
            price=100,
            source="kraken",
            mode=OperationMode.LIVE,
            price_base=PriceBase.TRADED,
            sequence=9,
            event_id="core-event-id",
            quality=quality,
            metadata={"provenance": {"provider": "kraken", "mode": "LIVE", "instrument": "TEST/USD", "price_basis": "traded"}},
        )
        data_event = core_event_to_data(event)
        restored_event = data_event_to_core(data_event)
        self.assertEqual(data_event.metadata["provenance"]["mode"], "LIVE")
        self.assertEqual(restored_event.event_id, "core-event-id")
        self.assertEqual(restored_event.mode, OperationMode.LIVE)
        self.assertEqual(restored_event.quality.flags, frozenset({QualityFlag.LATE}))

        candle = CoreCandle(
            instrument="TEST/USD",
            timeframe="M1",
            start=T0,
            end=T0 + timedelta(minutes=1),
            open=100,
            high=102,
            low=99,
            close=101,
            mode=OperationMode.LIVE,
            price_base=PriceBase.TRADED,
            candle_id="core-candle-id",
            metadata={"revision": 6, "provenance": {"provider": "kraken", "mode": "LIVE", "instrument": "TEST/USD", "price_basis": "traded"}},
        )
        data_bar = core_bar_to_data(candle)
        restored_bar = data_bar_to_core(data_bar)
        self.assertEqual(data_bar.metadata["provenance"]["mode"], "LIVE")
        self.assertEqual(restored_bar.candle_id, "core-candle-id")
        self.assertEqual(restored_bar.metadata["revision"], 6)
        self.assertEqual(restored_bar.mode, OperationMode.LIVE)

    def test_core_record_unknown_states_are_not_defaulted(self) -> None:
        quality = CoreQuality(frozenset({QualityFlag.GAP}), ("missing interval",), "fixture")
        core = CoreEvent(
            instrument="TEST/USD",
            event_time=T0,
            price=100,
            source="fixture",
            mode=OperationMode.REPLAY,
            price_base=PriceBase.TRADED,
            quality=quality,
            metadata={"provenance": {"mode": "UNKNOWN"}},
        )
        with self.assertRaises(TranslationError):
            core_event_to_data(core)
        core_bar = CoreCandle(
            instrument="TEST/USD",
            timeframe="M1",
            start=T0,
            end=T0 + timedelta(minutes=1),
            open=100,
            high=102,
            low=99,
            close=101,
            mode=OperationMode.REPLAY,
            price_base=PriceBase.TRADED,
            quality=quality,
            candle_id="core-bar",
            metadata={"revision": 2, "provenance": {"mode": "UNKNOWN"}},
        )
        with self.assertRaises(TranslationError):
            core_bar_to_data(core_bar)


class ReconciliationTests(unittest.TestCase):
    def _events(self) -> list[DataEvent]:
        return [
            DataEvent("TEST/USD", T0 + timedelta(seconds=5), 100, quantity=1, source="fixture", source_event_id="e1", available_at=T0 + timedelta(minutes=1)),
            DataEvent("TEST/USD", T0 + timedelta(seconds=25), 102, quantity=2, source="fixture", source_event_id="e2", available_at=T0 + timedelta(minutes=1)),
            DataEvent("TEST/USD", T0 + timedelta(seconds=45), 101, quantity=3, source="fixture", source_event_id="e3", available_at=T0 + timedelta(minutes=1)),
        ]

    def _native(self, *, high: float = 102) -> DataBar:
        return DataBar(
            "TEST/USD",
            T0,
            T0 + timedelta(minutes=1),
            100,
            high,
            100,
            101,
            60,
            volume=6,
            trade_count=3,
            source="native",
            source_record_id="n1",
            available_at=T0 + timedelta(minutes=1),
        )

    def test_equal_native_and_actual_events_match_without_invented_ticks(self) -> None:
        result = reconcile_native_candles([self._native()], self._events(), "M1", instrument="TEST/USD", compare_volume=True)
        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.mismatch_count, 0)
        self.assertFalse(result.blocked)
        self.assertEqual(result.invented_event_count, 0)
        self.assertEqual(result.event_bars[0].metadata["no_ticks_invented"], True)
        self.assertEqual(result.event_bars[0].metadata["reconciled_from_event_ids"], ["e1", "e2", "e3"])
        self.assertIs(bootstrap_reconcile, reconcile_native_candles)

    def test_mismatch_and_missing_side_are_blocking(self) -> None:
        result = reconcile_native_candles([self._native(high=103)], self._events(), "M1", instrument="TEST/USD")
        self.assertEqual(result.mismatch_count, 1)
        self.assertTrue(result.blocked)
        native_only = reconcile_native_candles([self._native()], [], "M1", instrument="TEST/USD")
        self.assertEqual(native_only.missing_events_count, 1)
        self.assertTrue(native_only.blocked)
        events_only = reconcile_native_candles([], self._events(), "M1", instrument="TEST/USD")
        self.assertEqual(events_only.missing_native_count, 1)
        self.assertTrue(events_only.blocked)

    def test_native_ohlc_does_not_generate_event_records(self) -> None:
        result = reconcile_native_candles([self._native()], [], "M1", instrument="TEST/USD")
        self.assertEqual(result.event_bars, ())
        self.assertEqual(result.invented_event_count, 0)

    def test_open_event_derived_bar_blocks_reconciliation(self) -> None:
        events = [
            DataEvent("TEST/USD", T0 + timedelta(seconds=5), 100, source="fixture", source_event_id="open-event")
        ]
        result = reconcile_native_candles([self._native()], events, "M1", instrument="TEST/USD")
        self.assertTrue(result.blocked)
        self.assertTrue(any("remains open" in reason for reason in result.blocking_reasons))


if __name__ == "__main__":
    unittest.main()
