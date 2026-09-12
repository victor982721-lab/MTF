from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from mtf_lab.configuration import ConfigError, SimulationConfig, load_config
from mtf_lab.core import Candle, MarketEvent, OperationMode
from mtf_lab.data.models import Event
from mtf_lab.runtime.integration import RuntimeCoordinator
from mtf_lab.runtime.processor import ProcessResult, RuntimeIssue

BASE = datetime(2026, 1, 1, tzinfo=UTC)


class _RecordingStore:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.candles: list[dict[str, object]] = []
        self.decisions: list[dict[str, object]] = []
        self.discards: list[dict[str, object]] = []
        self.signals: list[dict[str, object]] = []
        self.simulations: list[dict[str, object]] = []

    def save_event(self, _session_id: str, event: dict[str, object], **_kwargs: object) -> bool:
        self.events.append(event)
        return True

    def save_candle(self, _session_id: str, candle: dict[str, object], **_kwargs: object) -> bool:
        self.candles.append(candle)
        return True

    def save_decision(self, _session_id: str, decision: dict[str, object], **_kwargs: object) -> bool:
        self.decisions.append(decision)
        return True

    def save_discard(self, _session_id: str, discard: dict[str, object], **_kwargs: object) -> bool:
        self.discards.append(discard)
        return True

    def save_signal(self, _session_id: str, signal: dict[str, object], **_kwargs: object) -> bool:
        self.signals.append(signal)
        return True

    def update_simulation(self, _session_id: str, simulation: dict[str, object], **_kwargs: object) -> bool:
        self.simulations.append(simulation)
        return True


def _coordinator(config: object, store: _RecordingStore) -> RuntimeCoordinator:
    coordinator = object.__new__(RuntimeCoordinator)
    coordinator.store = store
    coordinator.session_id = "session"
    coordinator.capture_id = "session"
    coordinator.config = config
    coordinator.mode = OperationMode.REPLAY
    coordinator.analysis_id = "analysis"
    coordinator.analysis_config_hash = "config-hash"
    coordinator.contract_hash = "contract-hash"
    coordinator.variant = "trend_pullback_v1"
    coordinator.partition = "all"
    coordinator._input_ordinal = 4
    coordinator.processor = SimpleNamespace(
        _point_index_by_start={},
        _point_base_index={},
        indicator_points={},
        status={"warmup_pending": {"M1": 0, "M5": 0, "M15": 0}},
        events_processed=0,
        issues=(),
        pending_simulations=(),
        completed_simulations=(),
        last_event_time=None,
    )
    coordinator.external_blocked_reasons = []
    coordinator._external_block_details = {}
    coordinator._runtime_block_details = {}
    coordinator._block_history = []
    coordinator.capture_state = "CAPTURING"
    coordinator.connection_state = "OFFLINE"
    coordinator.reconciliation_state = "NOT_APPLICABLE"
    coordinator.freshness_state = "NOT_APPLICABLE"
    coordinator.continuity_state = "CONTINUOUS"
    coordinator.last_received_at = None
    coordinator.last_processed_at = None
    coordinator.last_heartbeat_at = None
    coordinator._last_checkpoint_events = 0
    return coordinator


class ConfigurationRefactorTests(unittest.TestCase):
    def test_load_config_normalizes_sections_without_changing_alias_contract(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "aliases.toml"
            path.write_text(
                """
[project]
mode = "offline"
[instrument]
symbol = "TEST/USD"
price_base = "trade"
[timeframes]
values = ["M15", "M1", "M5"]
base = "M1"
[simulation]
horizons_minutes = [1, 3]
net_payout = 0.7
tie_return = 0.1
""",
                encoding="utf-8",
            )

            config = load_config(path)

        self.assertEqual(config.mode, "SYNTHETIC")
        self.assertEqual(config.instrument, "TEST/USD")
        self.assertEqual(config.price_base, "traded")
        self.assertEqual(tuple(tf.name for tf in config.timeframes), ("M1", "M5", "M15"))
        self.assertEqual(config.simulation.horizons_seconds, (60.0, 180.0))
        self.assertEqual(config.simulation.payout_net, 0.7)
        self.assertEqual(config.simulation.tie_net, 0.1)
        self.assertEqual(config.data["mode"], "SYNTHETIC")

    def test_load_config_rejects_unknown_and_conflicting_sections(self) -> None:
        cases = (
            ("unknown = true\n", "claves raíz desconocidas"),
            ("[simulation]\nhorizons_seconds = [60]\nhorizons_minutes = [1]\n", "incompatibles"),
            ('[instrument]\nprice_base = "bad"\n', "price_base"),
        )
        for body, expected in cases:
            with self.subTest(body=body), TemporaryDirectory() as directory:
                path = Path(directory) / "invalid.toml"
                path.write_text(body, encoding="utf-8")
                with self.assertRaisesRegex(ConfigError, expected):
                    load_config(path)

    def test_dataclass_post_init_helpers_normalize_and_validate(self) -> None:
        config = SimulationConfig(horizons_seconds=(60,), requested_base_price="trade")
        self.assertEqual(config.requested_base_price, "traded")
        self.assertEqual(config.horizons_seconds, (60.0,))
        with self.assertRaisesRegex(ConfigError, "stake"):
            SimulationConfig(horizons_seconds=(60,), stake=0)
        with self.assertRaisesRegex(ConfigError, "require_closed"):
            SimulationConfig(horizons_seconds=(60,), require_closed=1)  # type: ignore[arg-type]

    def test_effective_config_freezes_mappings_and_keeps_close_alias(self) -> None:
        config = load_config()
        with self.assertRaises(TypeError):
            config.provider["name"] = "changed"  # type: ignore[index]
        self.assertEqual(replace(config, price_base="close").price_base, "traded")


class RuntimePersistenceRefactorTests(unittest.TestCase):
    def test_persist_input_dispatches_candle_event_and_data_event(self) -> None:
        config = load_config()
        store = _RecordingStore()
        coordinator = _coordinator(config, store)
        candle = {
            "candle_id": "input-candle",
            "instrument": config.instrument,
            "timeframe": "M1",
            "start": BASE.isoformat(),
            "end": (BASE + timedelta(minutes=1)).isoformat(),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1.0,
            "closed": True,
            "source": "fixture",
            "price_base": "traded",
            "quality": {"status": "VALID"},
            "metadata": {"revision": 2},
        }
        coordinator._persist_input(candle, accepted=True)
        coordinator._persist_input(candle, accepted=True)
        coordinator._persist_input({"_persisted_capture": True, **candle}, accepted=True)
        coordinator._persist_input(candle, accepted=False)
        self.assertEqual(len(store.candles), 2)
        self.assertEqual(store.candles[0]["revision"], 2)
        self.assertEqual(store.candles[0]["quality"], "VALID")
        self.assertEqual(store.candles[0]["mode"], "REPLAY")

        event = MarketEvent(
            instrument=config.instrument,
            event_time=BASE,
            price=100.0,
            mode=OperationMode.REPLAY,
            source="fixture",
        )
        coordinator._persist_input(event, accepted=True)
        data_event = Event(
            instrument=config.instrument,
            event_time=BASE + timedelta(seconds=1),
            price=100.1,
            source="fixture",
            source_event_id="data-event",
            received_at=BASE + timedelta(seconds=1),
            available_at=BASE + timedelta(seconds=1),
        )
        coordinator._persist_input(data_event, accepted=True)
        self.assertEqual(len(store.events), 2)
        self.assertEqual(store.events[0]["source"], "fixture")
        self.assertEqual(store.events[1]["event_id"], data_event.data_id)

    def test_persist_result_routes_all_outputs_and_discard_conditions(self) -> None:
        config = load_config()
        store = _RecordingStore()
        coordinator = _coordinator(config, store)
        event = MarketEvent(instrument=config.instrument, event_time=BASE, price=100.0, source="fixture")
        candle = Candle(
            instrument=config.instrument,
            timeframe="M1",
            start=BASE,
            end=BASE + timedelta(minutes=1),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            event_count=1,
            source="fixture",
        )
        evaluation = {
            "decision": "blocked",
            "stage": "trigger",
            "timestamp": BASE.isoformat(),
            "available_at": BASE.isoformat(),
            "conditions": [{"mandatory": True, "state": "failed", "reason": "warmup"}],
        }
        result = ProcessResult(
            accepted=True,
            events=(event,),
            candles=(candle,),
            evaluations=(evaluation,),
            signals=({"signal_id": "signal-1", "direction": "UP"},),
            simulations=({"simulation_id": "resolved", "status": "RESOLVED", "outcome": "WIN"},),
            pending_simulations=({"simulation_id": "pending", "status": "PENDING"},),
        )

        coordinator._persist_result({}, result)

        self.assertEqual(len(store.events), 1)
        self.assertEqual(len(store.candles), 1)
        self.assertEqual(len(store.decisions), 1)
        self.assertEqual(len(store.discards), 1)
        self.assertEqual(len(store.signals), 1)
        self.assertEqual(len(store.simulations), 2)
        self.assertTrue(store.simulations[0]["simulation_id"].startswith("analysis:"))
        self.assertEqual(store.simulations[1]["outcome"], "PENDING")

    def test_status_composes_operational_and_processor_gates(self) -> None:
        config = load_config()
        store = _RecordingStore()
        coordinator = _coordinator(config, store)
        coordinator.mode = OperationMode.LIVE
        coordinator.connection_state = "CONNECTING"
        coordinator.reconciliation_state = "PENDING"
        coordinator.freshness_state = "UNKNOWN"
        coordinator.processor.status = {"warmup_pending": {"M1": 2, "M5": 0, "M15": 0}}
        coordinator.processor.events_processed = 1
        coordinator.processor.issues = (RuntimeIssue("gap", "gap detected"),)
        coordinator.continuity_state = "UNKNOWN"

        status = coordinator.status()

        self.assertFalse(status.analysis_enabled)
        self.assertIn("connection:connecting", status.analysis_blocked_reasons)
        self.assertIn("reconciliation:pending", status.analysis_blocked_reasons)
        self.assertIn("freshness:unknown", status.analysis_blocked_reasons)
        self.assertIn("warmup:M1", status.analysis_blocked_reasons)
        self.assertIn("continuity_or_quality", status.analysis_blocked_reasons)


if __name__ == "__main__":
    unittest.main()
