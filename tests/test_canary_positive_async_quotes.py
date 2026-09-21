"""Positive technical-canary coverage for asynchronous provider BBO updates."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from mtf_lab.configuration import load_config
from mtf_lab.core import OperationMode
from mtf_lab.data.ctrader import (
    CTraderClient,
    CTraderConfig,
    CTraderInstrumentSpec,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
    normalize_spot_event,
)
from mtf_lab.data.ctrader_protocol import PAYLOAD
from mtf_lab.data.models import Event
from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as proto
from mtf_lab.ops import ctrader_canary_inputs
from mtf_lab.ops.ctrader_canary_inputs import (
    CanaryInputError,
    CanaryInputState,
    CanarySessionEvidence,
    ObservedBBO,
    collect_canary_inputs,
)
from mtf_lab.ops.ctrader_watch import _decorate_record, _WatchMetadata
from mtf_lab.runtime import to_core_event

BASE = datetime(2026, 9, 21, 17, 2, 43, tzinfo=UTC)
SYMBOL_ID = 99
ACCOUNT_ID = 7
GENERATION = 1
WINDOW_END = BASE + timedelta(minutes=25)


class _ProviderFixture:
    """One prepared DEMO-shaped provider with a deterministic single reader."""

    def __init__(self, start: datetime = BASE) -> None:
        self.clock_value = [start]
        self.transport = DeterministicTransport()
        cconfig = CTraderConfig(
            symbol="EUR/USD",
            symbol_id=SYMBOL_ID,
            account_id=ACCOUNT_ID,
            quote_basis="mid",
            heartbeat_seconds=60,
        )
        self.client = CTraderClient(
            cconfig,
            transport=self.transport,
            wall_clock=lambda: self.clock_value[0],
        )
        self.provider = CTraderProvider(
            cconfig,
            client=self.client,
            clock=lambda: self.clock_value[0],
            max_quote_age_seconds=30,
        )
        self.provider.connect()
        self.client.mark_authenticated(ACCOUNT_ID)
        self.evidence = CanarySessionEvidence(
            provider="ctrader_open_api",
            session_id="async-quote-session",
            account_id=str(ACCOUNT_ID),
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            scopes=frozenset({"accounts", "trading"}),
            connection_generation=str(self.provider.generation),
            authenticated_at=start - timedelta(seconds=1),
            evidence_source="official-protobuf-fixture",
        )

    def close(self) -> None:
        self.provider.close()


def _technical_config() -> Any:
    source_path = Path(__file__).resolve().parents[1] / "config" / "ctrader_query.toml"
    source = source_path.read_text(encoding="utf-8")
    source += '\n[execution]\nenabled = false\nmarket_candidate_id = "tp_fast_v1"\nmax_price_age_seconds = 30\n'
    with tempfile.NamedTemporaryFile("w", suffix=".toml", encoding="utf-8", delete=False) as handle:
        handle.write(source)
        path = Path(handle.name)
    try:
        return load_config(path)
    finally:
        path.unlink()


def _spot_messages(start: datetime, *, count: int) -> list[WireMessage]:
    """Create official ProtoOASpotEvent frames: one full BBO, then one leg."""

    messages: list[WireMessage] = []
    for index in range(count):
        event_time = start + timedelta(seconds=10 * index)
        available_at = event_time + timedelta(seconds=1)
        values: dict[str, Any] = {
            "ctidTraderAccountId": ACCOUNT_ID,
            "symbolId": SYMBOL_ID,
            "timestamp": int(event_time.timestamp() * 1000),
        }
        if index == 0:
            values.update({"bid": 110_000, "ask": 110_003})
        elif index % 2:
            values["bid"] = 110_000 + (index + 1) // 2
        else:
            values["ask"] = 110_003 + index // 2
        payload = proto.ProtoOASpotEvent(**values)
        messages.append(
            WireMessage(
                PAYLOAD["PROTO_OA_SPOT_EVENT"],
                payload,
                is_event=True,
                received_at=available_at,
                available_at=available_at,
                ingest_sequence=index,
                connection_generation=GENERATION,
                source_identity="official-protobuf-fixture",
            )
        )
    return messages


def _queue_messages(fixture: _ProviderFixture, messages: list[WireMessage]) -> None:
    for message in messages:
        fixture.transport.push(message)
    original_poll = fixture.client.poll_event

    def poll_event(timeout_seconds: float | None = None) -> WireMessage | None:
        message = original_poll(timeout_seconds)
        if message is not None and message.available_at is not None:
            fixture.clock_value[0] = message.available_at
        return message

    fixture.client.poll_event = poll_event  # type: ignore[method-assign]


def _collect(
    fixture: _ProviderFixture,
    config: Any,
    messages: list[WireMessage],
    *,
    technical_only: bool,
) -> Any:
    _queue_messages(fixture, messages)
    order_start = BASE + timedelta(minutes=5)
    with (
        tempfile.TemporaryDirectory() as state_dir,
        tempfile.TemporaryDirectory() as home,
        patch.dict(
            os.environ,
            {"HOME": home, "XDG_STATE_HOME": str(Path(home) / "xdg")},
            clear=False,
        ),
    ):
        return collect_canary_inputs(
            fixture.provider,
            config,
            network=True,
            deadline=WINDOW_END,
            max_events=len(messages),
            window_start=order_start,
            window_end=WINDOW_END,
            preparation_start=BASE,
            session_evidence=fixture.evidence,
            runtime_observer=lambda snapshot: {**snapshot, "runtime_state": "VALID"},
            technical_only=technical_only,
            fetch_warmup=False,
            market_window_state=lambda *_args: "OPEN",
            isolated_state_dir=state_dir,
            clock=lambda: fixture.clock_value[0],
            close_provider=False,
        )


def _provider_event(
    *,
    metadata: dict[str, Any] | None = None,
    bid: float | None = 1.1,
    ask: float | None = 1.10003,
    event_time: datetime = BASE + timedelta(minutes=6),
    available_at: datetime = BASE + timedelta(minutes=6),
) -> Event:
    raw_metadata: dict[str, Any] = {
        "symbol_id": SYMBOL_ID,
        "quality_state": "VALID",
        "quote_usable": True,
        "partial_update": False,
        "connection_generation": str(GENERATION),
        "bid_available_at": available_at.isoformat().replace("+00:00", "Z"),
        "ask_available_at": available_at.isoformat().replace("+00:00", "Z"),
    }
    if metadata:
        raw_metadata.update(metadata)
    price_basis = "mid" if bid is not None and ask is not None else "bid"
    mid = (bid + ask) / 2 if bid is not None and ask is not None else None
    return Event(
        "EUR/USD",
        event_time,
        price=mid if mid is not None else bid,
        bid=bid,
        ask=ask,
        mid=mid,
        price_basis=price_basis,
        received_at=available_at,
        available_at=available_at,
        source="ctrader-open-api",
        source_event_id="negative-quote",
        source_sequence=1,
        synthetic=False,
        metadata=raw_metadata,
    )


def _observe_provider_event(event: Event, *, now: datetime = BASE + timedelta(minutes=6, seconds=5)) -> ObservedBBO:
    provider = SimpleNamespace(spec=SimpleNamespace(symbol="EUR/USD", symbol_id=SYMBOL_ID))
    evidence = CanarySessionEvidence(
        provider="ctrader_open_api",
        session_id="async-quote-session",
        account_id=str(ACCOUNT_ID),
        environment="DEMO",
        endpoint="demo.ctraderapi.com:5035",
        scopes=frozenset({"accounts", "trading"}),
        connection_generation=str(GENERATION),
        authenticated_at=BASE,
        evidence_source="negative-quote-fixture",
    )
    return ObservedBBO.from_provider_event(
        event,
        provider=provider,
        evidence=evidence,
        now=now,
        window_start=BASE,
        window_end=WINDOW_END,
        max_age_seconds=Decimal("30"),
    )


class CanaryPositiveAsyncQuoteTests(unittest.TestCase):
    def test_odd_relative_spread_mid_survives_normalization_and_core_translation(self) -> None:
        event_time = BASE
        result = normalize_spot_event(
            {
                "symbolId": SYMBOL_ID,
                "timestamp": int(event_time.timestamp() * 1000),
                "bid": 110_000,
                "ask": 110_001,
            },
            spec=CTraderInstrumentSpec("EUR/USD", SYMBOL_ID, 5, 4, 100_000),
            quote_basis="mid",
            received_at=event_time,
            available_at=event_time,
            sequence=1,
            generation=GENERATION,
            max_quote_age_seconds=30,
        )

        self.assertEqual(len(result.quote_events), 1)
        event = result.quote_events[0]
        self.assertEqual((event.bid, event.ask), (1.1, 1.10001))
        self.assertAlmostEqual(event.mid or 0.0, 1.100005, places=12)
        self.assertEqual(event.metadata["bid_relative"], 110_000)
        self.assertEqual(event.metadata["ask_relative"], 110_001)
        self.assertEqual(event.event_time, event_time)
        self.assertEqual(event.available_at, event_time)

        decorated = _decorate_record(
            event,
            _WatchMetadata("ctrader_open_api", "demo", False, "DEMO", False),
            False,
            technical_quote_mode=True,
        )
        core = to_core_event(decorated, mode=OperationMode.LIVE)
        self.assertEqual((core.bid, core.ask), (1.1, 1.10001))
        self.assertAlmostEqual(core.selected_price or 0.0, 1.100005, places=12)
        self.assertEqual(core.selected_price, (event.bid + event.ask) / 2)

    def test_technical_canary_accepts_fresh_async_legs_without_bootstrap(self) -> None:
        fixture = _ProviderFixture()
        config = _technical_config()
        messages = _spot_messages(BASE + timedelta(seconds=5), count=105)
        runners: list[Any] = []
        runner_type = ctrader_canary_inputs._InputWatchRunner

        class SpyInputWatchRunner(runner_type):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                runners.append(self)

        try:
            with patch.object(ctrader_canary_inputs, "_InputWatchRunner", SpyInputWatchRunner):
                result = _collect(fixture, config, messages, technical_only=True)
        finally:
            fixture.close()

        self.assertTrue(result.ok, result.reason)
        assert result.context is not None
        context = result.context
        self.assertTrue(context.technical_only)
        self.assertFalse(context.strategy_ready)
        self.assertIsNone(context.warmup)
        self.assertFalse(context.bbo.synthetic)
        self.assertLess(context.bbo.bid, context.bbo.ask)
        self.assertIn(context.bbo.ask - context.bbo.bid, {Decimal("0.00002"), Decimal("0.00003")})
        self.assertEqual(context.runtime_snapshot["trigger_timeframe"], "M1")
        self.assertGreater(Decimal(str(context.runtime_snapshot["atr"])), Decimal("0"))
        self.assertEqual(context.runtime_provenance["atr_period"], 14)
        self.assertEqual(context.runtime_provenance["trigger_timeframe"], "M1")
        self.assertFalse(context.buy_signal["detected"])
        self.assertTrue(context.buy_signal["manual_technical"])
        missing = {item.split(":", 1)[0] for item in context.missing_warmups}
        self.assertTrue({"M5", "M15"}.issubset(missing), context.missing_warmups)
        self.assertEqual(context.watch_result.status["bootstrap_bars"], 0)
        self.assertFalse(context.watch_result.status["paper"]["enabled"])
        self.assertEqual(context.watch_result.status["freshness_state"], "VALID")
        self.assertFalse(result.gates["orders_attempted"])
        self.assertGreater(result.events, 0)
        self.assertLessEqual(result.events, len(messages))

        self.assertTrue(runners)
        final_record = runners[0]._last_decorated_record
        self.assertIsInstance(final_record, Event)
        effective_metadata = dict(final_record.metadata)
        self.assertTrue(effective_metadata["raw_partial_update"])
        self.assertFalse(effective_metadata["partial_update"])
        self.assertTrue(effective_metadata["canary_complete_bbo_from_partial"])

    def test_default_watch_rejects_fresh_composite_after_partial_update(self) -> None:
        fixture = _ProviderFixture()
        try:
            result = _collect(
                fixture,
                _technical_config(),
                _spot_messages(BASE + timedelta(seconds=5), count=2),
                technical_only=False,
            )
        finally:
            fixture.close()

        self.assertFalse(result.ok)
        self.assertEqual(result.state, CanaryInputState.BLOCKED)
        self.assertIn("BBO", result.reason or "")
        self.assertFalse(result.gates["orders_attempted"])

    def test_provider_composite_requires_the_older_leg_to_be_fresh(self) -> None:
        event = _provider_event(
            metadata={
                "ask_available_at": (BASE + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
            }
        )
        with self.assertRaisesRegex(CanaryInputError, "pierna más antigua"):
            _observe_provider_event(event)

    def test_future_composite_is_rejected(self) -> None:
        future = BASE + timedelta(minutes=7)
        event = _provider_event(event_time=future, available_at=future)
        with self.assertRaisesRegex(CanaryInputError, "futura"):
            _observe_provider_event(event, now=BASE + timedelta(minutes=6))

    def test_generation_mismatch_is_rejected(self) -> None:
        event = _provider_event(metadata={"connection_generation": "2"})
        with self.assertRaisesRegex(CanaryInputError, "otra generación"):
            _observe_provider_event(event)

    def test_crossed_composite_is_rejected(self) -> None:
        event = _provider_event(bid=1.10003, ask=1.10002)
        with self.assertRaisesRegex(CanaryInputError, "incompleto o cruzado"):
            _observe_provider_event(event)

    def test_missing_other_leg_is_rejected(self) -> None:
        event = _provider_event(ask=None)
        with self.assertRaisesRegex(CanaryInputError, "incompleto o cruzado"):
            _observe_provider_event(event)


if __name__ == "__main__":
    unittest.main()
