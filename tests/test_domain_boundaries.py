"""Regresiones de límites de dominio para bases nativas y calidad tipada."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from mtf_lab.configuration import load_config
from mtf_lab.core import (
    Candle,
    CandleAggregator,
    DataQuality,
    IndicatorConfig,
    MarketEvent,
    PriceBase,
    QualityFlag,
    QualityReason,
    QualityState,
    compute_indicators,
)
from mtf_lab.data.models import Bar, Event, _canonical_json
from mtf_lab.data.translation import TranslationError, core_bar_to_data, data_bar_to_core, reconcile_native_candles


UTC = timezone.utc
BASE = datetime(2026, 1, 1, tzinfo=UTC)


def native_bar(*, start: datetime = BASE, source: str = "ctrader-open-api", basis: str = "native") -> Bar:
    return Bar(
        instrument="EUR/USD",
        interval_start=start,
        interval_end=start + timedelta(minutes=1),
        open=1.1000,
        high=1.1010,
        low=1.0990,
        close=1.1005,
        resolution_seconds=60,
        source=source,
        source_record_id="bar-1",
        price_basis=basis,
        received_at=start + timedelta(minutes=1),
        available_at=start + timedelta(minutes=1),
    )


class NativePriceBoundaryTests(unittest.TestCase):
    def test_provider_native_bar_roundtrip_never_becomes_traded(self) -> None:
        original = native_bar()
        core = data_bar_to_core(original)
        self.assertIs(core.price_base, PriceBase.NATIVE)
        self.assertTrue(core.is_native)
        restored = core_bar_to_data(core)
        self.assertEqual(restored.price_basis, "native")
        self.assertEqual(restored.data_id, original.data_id)

    def test_legacy_ctrader_traded_bar_is_blocked_but_generic_traded_survives(self) -> None:
        with self.assertRaises(TranslationError) as caught:
            data_bar_to_core(native_bar(basis="traded"))
        self.assertEqual(caught.exception.code, "PRICE_BASE_AMBIGUOUS")

        generic = native_bar(source="kraken-rest", basis="traded")
        self.assertIs(data_bar_to_core(generic).price_base, PriceBase.TRADED)

    def test_native_and_traded_events_do_not_mix_in_aggregation(self) -> None:
        aggregator = CandleAggregator("M1", instrument="EUR/USD", price_base=PriceBase.NATIVE)
        native_event = MarketEvent("EUR/USD", BASE, price=1.1, price_base=PriceBase.NATIVE)
        traded_event = MarketEvent("EUR/USD", BASE + timedelta(seconds=1), price=1.1, price_base=PriceBase.TRADED)
        self.assertTrue(aggregator.add(native_event).accepted)
        rejected = aggregator.add(traded_event)
        self.assertFalse(rejected.accepted)
        self.assertEqual(rejected.issues[0].code, "price_base_mismatch")

    def test_reconciliation_exposes_native_traded_mismatch(self) -> None:
        observed = Event(
            "EUR/USD",
            BASE + timedelta(seconds=5),
            price=1.1005,
            quantity=1,
            source="kraken-websocket-v2",
            source_event_id="trade-1",
            available_at=BASE + timedelta(minutes=1),
            price_basis="traded",
        )
        result = reconcile_native_candles([native_bar()], [observed], "M1", instrument="EUR/USD")
        self.assertEqual(result.mismatch_count, 1)
        self.assertTrue(result.blocked)
        self.assertTrue(any("price_base" in reason for reason in result.items[0].reasons))

    def test_native_candles_keep_indicator_warmup_contract(self) -> None:
        candles = [
            Candle(
                "EUR/USD",
                "M1",
                BASE + timedelta(minutes=index),
                BASE + timedelta(minutes=index + 1),
                1.1000 + index * 0.0001,
                1.1010 + index * 0.0001,
                1.0990 + index * 0.0001,
                1.1005 + index * 0.0001,
                price_base=PriceBase.NATIVE,
            )
            for index in range(6)
        ]
        series = compute_indicators(candles, IndicatorConfig(ema_fast=2, ema_slow=3, rsi_period=2, atr_period=2))
        self.assertTrue(all(point.quality.valid for point in series.points))
        self.assertTrue(series.points[-1].ready)


class DomainContractTests(unittest.TestCase):
    def test_configuration_accepts_native_without_reinterpreting_it(self) -> None:
        source = Path("config/default.toml").read_text(encoding="utf-8")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "native.toml"
            path.write_text(source.replace('price_base = "trade"', 'price_base = "native"', 1), encoding="utf-8")
            config = load_config(path)
        self.assertEqual(config.price_base, "native")
        self.assertEqual(config.data["price_base"], "native")
        self.assertEqual(config.to_dict()["instrument"]["price_base"], "native")

    def test_nested_metadata_is_detached_and_canonical_is_strict(self) -> None:
        nested = {"legs": [{"bid": 1.1}]}
        event = Event("EUR/USD", BASE, 1.1, price_basis="native", metadata=nested)
        nested["legs"][0]["bid"] = 9.9
        self.assertEqual(event.metadata["legs"][0]["bid"], 1.1)
        with self.assertRaises(TypeError):
            _canonical_json({"unsupported": object()})

    def test_quality_state_and_reason_are_exactly_typed(self) -> None:
        quality = DataQuality.from_flag(QualityFlag.STALE, QualityReason.STALE, source="fixture")
        self.assertIs(quality.state, QualityState.STALE)
        self.assertEqual(quality.reason_codes, (QualityReason.STALE,))
        presentation_only = DataQuality.from_flag(QualityFlag.STALE, "not-stale-but-text")
        self.assertEqual(presentation_only.reason_codes, ())
        self.assertEqual(quality.to_dict()["state"], "stale")

    def test_invalid_sequence_cannot_fall_back_to_string_identity(self) -> None:
        with self.assertRaises(TypeError):
            Event("EUR/USD", BASE, 1.1, source_sequence=object())


if __name__ == "__main__":
    unittest.main()
