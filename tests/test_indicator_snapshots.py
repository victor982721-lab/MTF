"""Contratos públicos de snapshot/restauración del engine de indicadores."""

from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.core.indicators import (
    IncrementalIndicatorEngine,
    IndicatorConfig,
    IndicatorEngineSnapshot,
    compute_indicators,
)
from mtf_lab.core.models import Candle
from mtf_lab.runtime import IncrementalProcessor

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def candle(index: int, *, timeframe: str = "M1", close: float | None = None) -> Candle:
    seconds = 60 if timeframe == "M1" else 300
    start = BASE + timedelta(seconds=index * seconds)
    value = 100.0 + index if close is None else close
    return Candle(
        "EUR/USD", timeframe, start, start + timedelta(seconds=seconds), value, value + 1, value - 1, value, 1, 1
    )


class IndicatorSnapshotTests(unittest.TestCase):
    def test_typed_snapshot_is_golden_batch_stream_and_json_roundtrip(self) -> None:
        config = IndicatorConfig(ema_fast=3, ema_slow=5, rsi_period=3, atr_period=3)
        candles = [candle(index) for index in range(20)]
        batch = compute_indicators(candles, config)
        engine = IncrementalIndicatorEngine(config)
        streamed = [engine.update(item) for item in candles]
        snapshot = engine.snapshot()

        self.assertIsInstance(snapshot, IndicatorEngineSnapshot)
        self.assertEqual(snapshot.to_dict(), json.loads(engine.snapshot_json()))
        self.assertEqual(
            [(item.ema_fast, item.ema_slow, item.rsi, item.atr) for item in batch],
            [(item.ema_fast, item.ema_slow, item.rsi, item.atr) for item in streamed],
        )
        restored = IncrementalIndicatorEngine(config)
        restored.restore(snapshot.to_dict())
        self.assertEqual(restored.snapshot(), snapshot)
        self.assertEqual(restored.config_hash, snapshot.config_identity.fingerprint)

    def test_restore_continuation_matches_uninterrupted_stream(self) -> None:
        config = IndicatorConfig(ema_fast=2, ema_slow=4, rsi_period=2, atr_period=2)
        candles = [candle(index) for index in range(24)]
        uninterrupted = IncrementalIndicatorEngine(config)
        for item in candles:
            uninterrupted.update(item)

        partial = IncrementalIndicatorEngine(config)
        for item in candles[:11]:
            partial.update(item)
        restored = IncrementalIndicatorEngine(config)
        restored.restore(partial.snapshot())
        for item in candles[11:]:
            restored.update(item)
        self.assertEqual(restored.snapshot(), uninterrupted.snapshot())

    def test_config_identity_mismatch_is_rejected_before_mutating_engine(self) -> None:
        first = IncrementalIndicatorEngine(IndicatorConfig(ema_fast=2, ema_slow=4, rsi_period=2, atr_period=2))
        for index in range(8):
            first.update(candle(index))
        snapshot = first.snapshot()
        other = IncrementalIndicatorEngine(IndicatorConfig(ema_fast=2, ema_slow=5, rsi_period=2, atr_period=2))
        before = other.snapshot()
        with self.assertRaises(ValueError):
            other.restore(snapshot)
        self.assertEqual(other.snapshot(), before)

        tampered = snapshot.to_dict()
        identity = dict(tampered["config_identity"])
        identity["ema_slow"] = 99
        tampered["config_identity"] = identity
        with self.assertRaises(ValueError):
            first.restore(tampered)

    def test_runtime_v1_checkpoint_uses_public_snapshot_and_legacy_restore(self) -> None:
        processor = IncrementalProcessor(instrument="EUR/USD", max_candles=16)
        for index in range(4):
            # Provider conversion is outside this focused contract; native
            # Candle records exercise the processor's indicator engines.
            processor.process_bar(candle(index))
        checkpoint = processor.checkpoint()
        engine_state = checkpoint["indicator_engines"]["M1"]
        self.assertEqual(engine_state["schema_version"], 1)
        self.assertIn("config_identity", engine_state)
        resumed = IncrementalProcessor.from_checkpoint(checkpoint)
        self.assertEqual(resumed.checkpoint(), checkpoint)
        historical = dict(engine_state)
        historical.pop("schema_version")
        historical.pop("config_identity")
        historical.pop("config_hash")
        fresh = IncrementalIndicatorEngine(processor.strategy_config.indicators, max_points=16)
        fresh.restore(historical, allow_legacy=True)
        self.assertEqual(fresh.snapshot().index, engine_state["index"])


if __name__ == "__main__":
    unittest.main()
