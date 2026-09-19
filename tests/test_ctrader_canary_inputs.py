from __future__ import annotations

import os
import tempfile
import unittest
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from mtf_lab.configuration import load_config
from mtf_lab.core import IndicatorConfig, OperationMode
from mtf_lab.data.ctrader import (
    CTraderClient,
    CTraderConfig,
    CTraderInstrumentSpec,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
    normalize_spot_event,
    synthetic_spot_event,
)
from mtf_lab.data.models import Bar
from mtf_lab.ops.ctrader_canary_inputs import (
    CanaryInputError,
    CanaryInputState,
    CanarySessionEvidence,
    _validate_runtime_producer,
    _validate_runtime_snapshot,
    collect_canary_inputs,
)
from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor, DemoAccount, DemoTransport, ExecutionPolicy
from mtf_lab.ops.ctrader_warmup import CTraderWarmupResult
from mtf_lab.runtime import RuntimeCoordinator, runtime_simulation_config
from mtf_lab.runtime.processor import IncrementalProcessor

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class _FakeProof:
    def __init__(self, generation: int) -> None:
        self.session_id = "typed-session"
        self.account_id = 7
        self.environment = "DEMO"
        self.endpoint = "demo.ctraderapi.com:5035"
        self.scopes = frozenset({"accounts", "trading"})
        self.connection_generation = str(generation)
        self.authenticated_at = NOW
        self.evidence_source = "test-gateway"


class _FakeExecutor:
    def __init__(self) -> None:
        self.observe_calls = 0
        self.activate_calls = 0

    def activate(self) -> None:
        self.activate_calls += 1

    def observe_runtime(self, snapshot: Mapping[str, Any]) -> dict[str, Any]:
        self.observe_calls += 1
        return {**snapshot, "runtime_state": "VALID"}


class CanaryInputTests(unittest.TestCase):
    def setUp(self) -> None:
        base = load_config("config/fixture_cfd.toml")
        indicators = IndicatorConfig(ema_fast=2, ema_slow=3, rsi_period=2, atr_period=2)
        strategy = replace(
            base.strategy,
            context_lookback=1,
            preparation_lookback=1,
            preparation_ttl_bars=2,
            indicators=indicators,
        )
        self.config = replace(
            base,
            mode="LIVE",
            price_base="native",
            indicators=indicators,
            strategy=strategy,
            execution={"market_candidate_id": "tp_fast_v1", "max_price_age_seconds": "30"},
        )
        self.transport = DeterministicTransport()
        cconfig = CTraderConfig(
            symbol="EUR/USD",
            symbol_id=99,
            account_id=7,
            quote_basis="mid",
            heartbeat_seconds=60,
        )
        client = CTraderClient(cconfig, transport=self.transport, wall_clock=lambda: NOW)
        self.provider = CTraderProvider(cconfig, client=client, clock=lambda: NOW, max_quote_age_seconds=3_600)
        self.provider.connect()
        client.mark_authenticated(7)
        self.evidence = CanarySessionEvidence(
            provider="ctrader_open_api",
            session_id="typed-session",
            account_id="7",
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            scopes=frozenset({"accounts", "trading"}),
            connection_generation=str(self.provider.generation),
            authenticated_at=NOW,
            evidence_source="test-gateway",
        )
        self.warmup = self._warmup()

    def tearDown(self) -> None:
        self.provider.close()

    def _warmup(self) -> CTraderWarmupResult:
        bars: dict[str, tuple[Bar, ...]] = {}
        counts = {"M15": 30, "M5": 30, "M1": 30}
        minutes = {"M15": 15, "M5": 5, "M1": 1}
        for timeframe, count in counts.items():
            size = minutes[timeframe]
            first = NOW - timedelta(minutes=size * count)
            values: list[Bar] = []
            for index in range(count):
                start = first + timedelta(minutes=size * index)
                end = start + timedelta(minutes=size)
                close = 1.10 + index / 100_000
                values.append(
                    Bar(
                        "EUR/USD",
                        start,
                        end,
                        close,
                        close + 0.0002,
                        close - 0.0002,
                        close,
                        size * 60,
                        price_basis="native",
                        source="ctrader-open-api",
                        received_at=NOW,
                        available_at=NOW,
                        closed=True,
                    )
                )
            bars[timeframe] = tuple(values)
        return CTraderWarmupResult(NOW, {key: len(value) for key, value in bars.items()}, bars, {})

    @staticmethod
    def _load_toml_canary_config():
        source = Path("config/ctrader_query.toml").read_text(encoding="utf-8")
        source += '\n[execution]\nenabled = false\nmarket_candidate_id = "tp_fast_v1"\nmax_price_age_seconds = 30\n'
        with tempfile.NamedTemporaryFile("w", suffix=".toml", encoding="utf-8", delete=False) as handle:
            handle.write(source)
            path = Path(handle.name)
        try:
            return load_config(path)
        finally:
            path.unlink()

    def _push_quote(self, *, bid: int = 110000, ask: int = 110020) -> None:
        payload = synthetic_spot_event(
            timestamp_ms=int(NOW.timestamp() * 1000),
            symbol_id=99,
            bid_relative=bid,
            ask_relative=ask,
        )
        payload.pop("synthetic_fixture", None)
        self.transport.push(
            WireMessage(
                "PROTO_OA_SPOT_EVENT",
                payload,
                is_event=True,
                received_at=NOW,
                available_at=NOW,
                ingest_sequence=0,
                connection_generation=self.provider.generation,
            )
        )

    def _collect(self, *, config: Any = None, executor: Any = None, **kwargs: Any) -> Any:
        self._push_quote(**kwargs.pop("quote", {}))
        executor = executor or _FakeExecutor()
        with (
            tempfile.TemporaryDirectory() as state_dir,
            tempfile.TemporaryDirectory() as home,
            patch.dict(os.environ, {"HOME": home, "XDG_STATE_HOME": str(Path(home) / "xdg")}, clear=False),
        ):
            return collect_canary_inputs(
                self.provider,
                config or self.config,
                network=True,
                deadline=NOW + timedelta(minutes=5),
                max_events=1,
                window_start=NOW - timedelta(minutes=1),
                window_end=NOW + timedelta(minutes=5),
                session_evidence=self.evidence,
                executor=executor,
                warmup=self.warmup,
                fetch_warmup=False,
                market_window_state=lambda *_args: "OPEN",
                isolated_state_dir=state_dir,
                clock=lambda: NOW,
                **kwargs,
            )

    def test_network_gate_is_explicit_and_side_effect_free(self) -> None:
        result = collect_canary_inputs(
            self.provider,
            self.config,
            network=False,
            deadline=NOW + timedelta(minutes=5),
            max_events=1,
            window_start=NOW - timedelta(minutes=1),
            window_end=NOW + timedelta(minutes=5),
        )
        self.assertEqual(result.state, CanaryInputState.BLOCKED)
        self.assertEqual(result.reason, "EXPLICIT_NETWORK_GATE_REQUIRED")
        self.assertFalse(result.gates["orders_attempted"])

    def test_json_session_payload_is_not_attestation(self) -> None:
        class Client:
            def authenticated_session_evidence(self) -> dict[str, str]:
                return {"session_id": "forged", "connection_generation": "1"}

            def validate_session_evidence(self, _proof: object) -> bool:
                return True

        provider = SimpleNamespace(client=Client(), generation=1, name="ctrader_open_api")
        with self.assertRaisesRegex(CanaryInputError, "JSON"):
            CanarySessionEvidence.from_provider(provider)

    def test_ready_context_uses_runtime_profile_and_manual_directions(self) -> None:
        executor = _FakeExecutor()
        result = self._collect(executor=executor)
        self.assertTrue(result.ok)
        assert result.context is not None
        self.assertEqual(result.state, CanaryInputState.READY)
        self.assertEqual(result.context.bbo.quality_state, "VALID")
        self.assertFalse(result.context.bbo.synthetic)
        self.assertEqual(result.context.buy_signal["direction"], "BUY")
        self.assertEqual(result.context.sell_signal["direction"], "SELL")
        self.assertTrue(result.context.buy_signal["manual_canary"])
        self.assertFalse(result.context.buy_signal["detected"])
        self.assertEqual(result.context.runtime_snapshot["runtime_state"], "VALID")
        self.assertEqual(result.context.quote.connection_generation, str(self.provider.generation))
        self.assertEqual(executor.activate_calls, 0)
        self.assertGreaterEqual(executor.observe_calls, 1)
        self.assertEqual(result.context.watch_result.status["paper"]["enabled"], False)

    def test_crossed_quote_returns_blocked_without_fabricating_bbo(self) -> None:
        result = self._collect(quote={"bid": 110020, "ask": 110020})
        self.assertFalse(result.ok)
        self.assertEqual(result.state, CanaryInputState.BLOCKED)
        self.assertIn("BBO", result.reason or "")

    def test_native_warmup_is_not_relabelled_as_mid_profile(self) -> None:
        mid_config = replace(self.config, price_base="mid")
        result = self._collect(config=mid_config)
        self.assertFalse(result.ok)
        self.assertEqual(result.state, CanaryInputState.BLOCKED)
        self.assertIn("warmup", (result.reason or "").lower())

    def test_toml_mid_snapshot_accepts_atr14_before_strategy_warmup(self) -> None:
        """Technical manual context is distinct from detector readiness."""

        config = self._load_toml_canary_config()
        self.assertEqual(config.price_base, "mid")
        self.assertEqual(config.execution["market_candidate_id"], "tp_fast_v1")
        self.assertEqual(config.indicators.atr_period, 14)
        self.assertEqual(config.strategy.trigger_timeframe.name, "M1")
        processor = IncrementalProcessor(
            strategy=config.strategy,
            simulation=runtime_simulation_config(config),
            timeframes=[item.name for item in config.timeframes],
            mode=OperationMode.LIVE,
            instrument=config.instrument,
            source="fake_observed_spot_stream",
            price_base="mid",
            max_candles=5_000,
            market_candidate_id="tp_fast_v1",
        )
        coordinator = object.__new__(RuntimeCoordinator)
        coordinator.config = config
        coordinator.processor = processor
        coordinator.mode = OperationMode.LIVE
        spec = CTraderInstrumentSpec("EUR/USD", 99, 5, 4, 100_000)
        start = datetime(2026, 9, 21, 13, 0, tzinfo=UTC)
        for index in range(18):
            event_time = start + timedelta(minutes=index)
            available = event_time + timedelta(seconds=1)
            normalized = normalize_spot_event(
                {
                    "symbolId": 99,
                    "timestamp": int(event_time.timestamp() * 1000),
                    "bid": 110_000 + index,
                    "ask": 110_020 + index,
                },
                spec=spec,
                quote_basis="mid",
                received_at=available,
                available_at=available,
                sequence=index,
                generation=1,
                max_quote_age_seconds=90,
            )
            event = normalized.quote_events[0]
            metadata = dict(event.metadata)
            metadata["source_quality"] = metadata.pop("quality")
            result = processor.process_event(replace(event, metadata=metadata))
            self.assertTrue(result.accepted)
        snapshot = coordinator.market_profile_snapshot()
        assert snapshot is not None
        self.assertEqual(snapshot["trigger_timeframe"], "M1")
        self.assertIsNotNone(snapshot["atr"])
        self.assertEqual(len(processor.signals), 0)
        self.assertGreater(processor.status["warmup_pending"]["M5"], 0)
        self.assertGreater(processor.status["warmup_pending"]["M15"], 0)

        account = DemoAccount("diagnostic", "DEMO", "demo://ctrader", frozenset({"trading"}), True, True)
        executor = CTraderDemoExecutor(
            account,
            transport=DemoTransport(account_id="diagnostic", endpoint="demo://ctrader", clock=lambda: NOW),
            policy=ExecutionPolicy(max_quantity=1_000, fixed_quantity=1, max_exposure=10_000),
            market_candidate_id="tp_fast_v1",
        )
        observed = executor.observe_runtime(snapshot)
        provenance = _validate_runtime_producer(coordinator, config, snapshot, technical_only=True)
        validated = _validate_runtime_snapshot(
            observed,
            instrument="EUR/USD",
            expected_candidate="tp_fast_v1",
            technical_only=True,
            producer_metadata=provenance,
        )
        self.assertEqual(validated["runtime_state"], "VALID")
        self.assertEqual(provenance["atr_period"], 14)
        with self.assertRaises(CanaryInputError):
            _validate_runtime_snapshot(
                {**observed, "trigger_timeframe": "M5"},
                instrument="EUR/USD",
                expected_candidate="tp_fast_v1",
                technical_only=True,
                producer_metadata=provenance,
            )

    def test_technical_collector_early_stops_on_atr_and_valid_bbo(self) -> None:
        config = self._load_toml_canary_config()
        preparation_start = NOW - timedelta(minutes=15)
        order_start = NOW - timedelta(minutes=10)
        stream_start = preparation_start + timedelta(seconds=5)
        clock_value = [stream_start]
        for index in range(91):
            event_time = stream_start + timedelta(seconds=10 * index)
            available = event_time + timedelta(seconds=1)
            payload = synthetic_spot_event(
                timestamp_ms=int(event_time.timestamp() * 1000),
                symbol_id=99,
                bid_relative=110_000 + index,
                ask_relative=110_020 + index,
            )
            payload.pop("synthetic_fixture", None)
            self.transport.push(
                WireMessage(
                    "PROTO_OA_SPOT_EVENT",
                    payload,
                    is_event=True,
                    received_at=available,
                    available_at=available,
                    ingest_sequence=index,
                    connection_generation=self.provider.generation,
                )
            )
        original_poll = self.provider.client.poll_event

        def poll_event(timeout_seconds):
            message = original_poll(timeout_seconds)
            if message is not None and message.available_at is not None:
                clock_value[0] = message.available_at
            return message

        self.provider.client.poll_event = poll_event
        executor = _FakeExecutor()
        evidence = replace(self.evidence, authenticated_at=stream_start)
        with (
            tempfile.TemporaryDirectory() as state_dir,
            tempfile.TemporaryDirectory() as home,
            patch.dict(
                os.environ,
                {"HOME": home, "XDG_STATE_HOME": str(Path(home) / "xdg")},
                clear=False,
            ),
        ):
            result = collect_canary_inputs(
                self.provider,
                config,
                network=True,
                deadline=NOW + timedelta(minutes=10),
                max_events=200,
                window_start=order_start,
                window_end=NOW + timedelta(minutes=10),
                preparation_start=preparation_start,
                session_evidence=evidence,
                executor=executor,
                technical_only=True,
                fetch_warmup=False,
                market_window_state=lambda *_args: "OPEN",
                isolated_state_dir=state_dir,
                clock=lambda: clock_value[0],
            )
        self.assertTrue(result.ok, result.reason)
        assert result.context is not None
        self.assertTrue(result.context.technical_only)
        self.assertFalse(result.context.strategy_ready)
        self.assertTrue(result.context.missing_warmups)
        self.assertLess(result.messages, 200)
        self.assertGreaterEqual(result.context.bbo.available_at, order_start)
        self.assertGreaterEqual(result.gates["execution_window_remaining_seconds"], 300)
        self.assertEqual(result.context.runtime_snapshot["runtime_state"], "VALID")

    def test_technical_collector_blocks_realistic_mid_interval_hot_start(self) -> None:
        """A non-boundary hot start cannot be promoted to continuous ATR input."""

        config = self._load_toml_canary_config()
        first = NOW - timedelta(minutes=19) + timedelta(seconds=5)
        for index in range(19):
            event_time = first + timedelta(minutes=index)
            if index == 1:
                event_time = first + timedelta(seconds=10)
            payload = synthetic_spot_event(
                timestamp_ms=int(event_time.timestamp() * 1000),
                symbol_id=99,
                bid_relative=110_000 + index,
                ask_relative=110_020 + index,
            )
            payload.pop("synthetic_fixture", None)
            self.transport.push(
                WireMessage(
                    "PROTO_OA_SPOT_EVENT",
                    payload,
                    is_event=True,
                    received_at=NOW,
                    available_at=NOW,
                    ingest_sequence=index,
                    connection_generation=self.provider.generation,
                )
            )
        with (
            tempfile.TemporaryDirectory() as state_dir,
            tempfile.TemporaryDirectory() as home,
            patch.dict(
                os.environ,
                {"HOME": home, "XDG_STATE_HOME": str(Path(home) / "xdg")},
                clear=False,
            ),
        ):
            result = collect_canary_inputs(
                self.provider,
                config,
                network=True,
                deadline=NOW + timedelta(minutes=10),
                max_events=19,
                window_start=NOW - timedelta(minutes=20),
                window_end=NOW + timedelta(minutes=10),
                session_evidence=self.evidence,
                executor=_FakeExecutor(),
                technical_only=True,
                fetch_warmup=False,
                market_window_state=lambda *_args: "OPEN",
                isolated_state_dir=state_dir,
                clock=lambda: NOW,
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "WATCH_CONTINUITY_BROKEN")

    def test_canonical_runtime_is_never_used_for_canary_state(self) -> None:
        result = collect_canary_inputs(
            self.provider,
            self.config,
            network=True,
            deadline=NOW + timedelta(minutes=5),
            max_events=1,
            window_start=NOW - timedelta(minutes=1),
            window_end=NOW + timedelta(minutes=5),
            session_evidence=self.evidence,
            runtime_observer=lambda snapshot: {**snapshot, "runtime_state": "VALID"},
            warmup=self.warmup,
            fetch_warmup=False,
            market_window_state=lambda *_args: "OPEN",
            isolated_state_dir=Path.home() / ".local" / "share" / "mtf-lab" / "runtime",
            clock=lambda: NOW,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "CANONICAL_RUNTIME_FORBIDDEN")


if __name__ == "__main__":
    unittest.main()
