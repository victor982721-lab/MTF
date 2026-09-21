"""Offline input-boundary tests for the scoped DEMO zero-spread canary."""

from __future__ import annotations

import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mtf_lab.configuration import load_config
from mtf_lab.core.canary_quote import CanaryZeroSpreadAuthorization
from mtf_lab.data.ctrader import (
    CTraderConfig,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
    synthetic_spot_event,
)
from mtf_lab.data.ctrader_session import AuthenticatedSessionEvidence
from mtf_lab.data.models import Event
from mtf_lab.ops.ctrader_canary_inputs import (
    CanaryInputError,
    CanarySessionEvidence,
    ObservedBBO,
    collect_canary_inputs,
)

BASE = datetime(2026, 9, 21, 15, 0, tzinfo=UTC)
DIGEST = "a" * 64
OBSERVE_NOW = BASE + timedelta(minutes=6, seconds=5)


def _authorization(**changes: object) -> CanaryZeroSpreadAuthorization:
    values: dict[str, object] = {
        "approval_digest": DIGEST,
        "authorization_source": "Usuario: zero-spread DEMO canary",
        "account_id": "7",
        "session_id": "typed-session",
        "connection_generation": "1",
        "preparation_start": BASE,
        "window_start": BASE + timedelta(minutes=5),
        "window_end": BASE + timedelta(minutes=20),
    }
    values.update(changes)
    return CanaryZeroSpreadAuthorization(**values)


def _evidence(**changes: object) -> CanarySessionEvidence:
    values: dict[str, object] = {
        "provider": "ctrader_open_api",
        "session_id": "typed-session",
        "account_id": "7",
        "environment": "DEMO",
        "endpoint": "demo.ctraderapi.com:5035",
        "scopes": frozenset({"accounts", "trading"}),
        "connection_generation": "1",
        "authenticated_at": BASE,
        "evidence_source": "test-gateway",
    }
    values.update(changes)
    return CanarySessionEvidence(**values)


class _QueuedDemoClient:
    """Typed DEMO session proof plus a deterministic single-reader queue."""

    def __init__(self, clock_value: list[datetime]) -> None:
        self._clock_value = clock_value
        self._queue: list[WireMessage] = []
        self.proof = AuthenticatedSessionEvidence(
            account_id=7,
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            scopes=frozenset({"accounts", "trading"}),
            session_id="typed-session",
            connection_generation="1",
            authenticated_at=BASE - timedelta(minutes=1),
        )
        self.status = SimpleNamespace(
            connection="CONNECTED",
            auth="AUTHENTICATED",
            needs_reconciliation=False,
            dropped_messages=0,
            last_error=None,
        )

    def authenticated_session_evidence(self) -> AuthenticatedSessionEvidence:
        return self.proof

    def validate_session_evidence(self, proof: object) -> bool:
        return proof is self.proof

    def push(self, message: WireMessage) -> None:
        self._queue.append(message)

    def poll_event(self, _timeout_seconds: float) -> WireMessage | None:
        if not self._queue:
            return None
        message = self._queue.pop(0)
        if message.available_at is not None:
            self._clock_value[0] = message.available_at
        return message

    def close(self) -> None:
        self.status.connection = "DISCONNECTED"


def _event(
    *,
    event_time: datetime = BASE + timedelta(minutes=6),
    available_at: datetime = BASE + timedelta(minutes=6),
    bid: float = 1.1,
    ask: float = 1.1,
    metadata: dict[str, object] | None = None,
) -> Event:
    raw_metadata: dict[str, object] = {
        "symbol": "EUR/USD",
        "symbol_id": 99,
        "connection_generation": "1",
        "quality_state": "VALID",
        "quote_usable": True,
        "partial_update": False,
        "canary_zero_spread_authorized": True,
        "canary_zero_spread_approval_digest": DIGEST,
        "canary_zero_spread_raw_relation": "BID_EQUALS_ASK",
    }
    if metadata:
        raw_metadata.update(metadata)
    return Event(
        "EUR/USD",
        event_time,
        price=bid,
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2,
        price_basis="mid",
        received_at=available_at,
        available_at=available_at,
        source="ctrader_open_api",
        source_event_id=f"quote-{event_time.timestamp()}",
        source_sequence=1,
        synthetic=False,
        metadata=raw_metadata,
    )


def _observe(event: Event, authorization: CanaryZeroSpreadAuthorization | None = None) -> ObservedBBO:
    return ObservedBBO.from_event(
        event,
        evidence=_evidence(),
        now=OBSERVE_NOW,
        window_start=BASE,
        window_end=BASE + timedelta(minutes=20),
        max_age_seconds=Decimal("30"),
        zero_spread_authorization=authorization,
    )


def _observe_provider_event(
    event: Event,
    authorization: CanaryZeroSpreadAuthorization | None = None,
) -> ObservedBBO:
    provider = SimpleNamespace(spec=SimpleNamespace(symbol="EUR/USD", symbol_id=99))
    return ObservedBBO.from_provider_event(
        event,
        provider=provider,
        evidence=_evidence(),
        now=OBSERVE_NOW,
        window_start=BASE,
        window_end=BASE + timedelta(minutes=20),
        max_age_seconds=Decimal("30"),
        zero_spread_authorization=authorization,
    )


class CanaryZeroSpreadInputTests(unittest.TestCase):
    def test_fourteen_closed_m1_bars_plus_observed_zero_spread_are_read_only(self) -> None:
        config_source = Path("config/ctrader_query.toml").read_text(encoding="utf-8")
        config_source += (
            '\n[execution]\nenabled = false\nmarket_candidate_id = "tp_fast_v1"\nmax_price_age_seconds = 30\n'
        )
        with tempfile.NamedTemporaryFile("w", suffix=".toml", encoding="utf-8", delete=False) as handle:
            handle.write(config_source)
            config_path = Path(handle.name)
        try:
            config = load_config(config_path)
        finally:
            config_path.unlink()

        preparation_start = BASE - timedelta(minutes=15)
        order_start = BASE - timedelta(minutes=10)
        window_end = BASE + timedelta(minutes=10)
        stream_start = preparation_start + timedelta(seconds=5)
        clock_value = [stream_start]
        client = _QueuedDemoClient(clock_value)
        provider = CTraderProvider(
            CTraderConfig(symbol="EUR/USD", symbol_id=99, account_id=7, quote_basis="mid"),
            client=client,
            transport=DeterministicTransport(),
            clock=lambda: clock_value[0],
            max_quote_age_seconds=30,
        )
        provider.reset_generation(1)
        evidence = CanarySessionEvidence(
            provider="ctrader_open_api",
            session_id="typed-session",
            account_id="7",
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            scopes=frozenset({"accounts", "trading"}),
            connection_generation="1",
            authenticated_at=preparation_start,
            evidence_source="typed-fixture-session",
        )
        authorization = _authorization(
            preparation_start=preparation_start,
            window_start=order_start,
            window_end=window_end,
        )
        for index in range(91):
            event_time = stream_start + timedelta(seconds=10 * index)
            available_at = event_time + timedelta(seconds=1)
            payload = synthetic_spot_event(
                timestamp_ms=int(event_time.timestamp() * 1000),
                symbol_id=99,
                bid_relative=110_000 + index,
                ask_relative=110_000 + index,
            )
            payload.pop("synthetic_fixture", None)
            client.push(
                WireMessage(
                    "PROTO_OA_SPOT_EVENT",
                    payload,
                    is_event=True,
                    received_at=available_at,
                    available_at=available_at,
                    ingest_sequence=index,
                    connection_generation=1,
                )
            )

        try:
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
                    provider,
                    config,
                    network=True,
                    deadline=window_end,
                    max_events=200,
                    window_start=order_start,
                    window_end=window_end,
                    preparation_start=preparation_start,
                    session_evidence=evidence,
                    runtime_observer=lambda snapshot: {**snapshot, "runtime_state": "VALID"},
                    technical_only=True,
                    zero_spread_authorization=authorization,
                    fetch_warmup=False,
                    market_window_state=lambda *_args: "OPEN",
                    isolated_state_dir=state_dir,
                    clock=lambda: clock_value[0],
                )
        finally:
            provider.close()

        self.assertTrue(result.ok, result.reason)
        assert result.context is not None
        self.assertEqual(result.context.bbo.bid, result.context.bbo.ask)
        self.assertFalse(result.context.bbo.synthetic)
        self.assertEqual(result.context.bbo.quality_state, "VALID")
        self.assertFalse(result.context.strategy_ready)
        self.assertGreaterEqual(int(result.context.runtime_snapshot["trigger_bar_count"]), 14)
        self.assertGreater(Decimal(str(result.context.runtime_snapshot["atr"])), Decimal("0"))
        self.assertFalse(result.gates["orders_attempted"])
        self.assertTrue(result.gates["zero_spread_authorization_bound"])
        self.assertFalse(result.context.watch_result.status["paper"]["enabled"])

    def test_zero_spread_requires_typed_context_even_with_effective_metadata(self) -> None:
        with self.assertRaisesRegex(CanaryInputError, "spread cero"):
            _observe(_event())

    def test_zero_spread_requires_provider_effective_metadata(self) -> None:
        authorization = _authorization()
        with self.assertRaisesRegex(CanaryInputError, "metadata efectiva"):
            _observe(_event(metadata={"canary_zero_spread_authorized": False}), authorization)
        with self.assertRaisesRegex(CanaryInputError, "metadata efectiva"):
            _observe(_event(metadata={"canary_zero_spread_approval_digest": "b" * 64}), authorization)

    def test_generation_mismatch_is_rejected(self) -> None:
        authorization = _authorization()
        with self.assertRaisesRegex(CanaryInputError, "otra generación"):
            _observe(_event(metadata={"connection_generation": "2"}), authorization)

    def test_oldest_provider_leg_controls_freshness_and_quote_provenance(self) -> None:
        authorization = _authorization()
        event = _event(
            metadata={
                "bid_available_at": (BASE + timedelta(minutes=6)).isoformat().replace("+00:00", "Z"),
                "ask_available_at": (BASE + timedelta(minutes=6)).isoformat().replace("+00:00", "Z"),
            }
        )
        observed = _observe_provider_event(event, authorization)
        self.assertEqual(observed.earliest_available_at, BASE + timedelta(minutes=6))
        self.assertEqual(
            observed.to_dict()["earliest_available_at"],
            (BASE + timedelta(minutes=6)).isoformat().replace("+00:00", "Z"),
        )

    def test_stale_oldest_provider_leg_is_rejected_even_when_combined_bbo_is_fresh(self) -> None:
        authorization = _authorization()
        event = _event(
            metadata={
                "bid_available_at": (BASE + timedelta(minutes=5, seconds=20)).isoformat().replace("+00:00", "Z"),
                "ask_available_at": (BASE + timedelta(minutes=6)).isoformat().replace("+00:00", "Z"),
            }
        )
        with self.assertRaisesRegex(CanaryInputError, "pierna más antigua"):
            _observe_provider_event(event, authorization)

    def test_window_mismatch_is_rejected(self) -> None:
        authorization = _authorization()
        with self.assertRaisesRegex(CanaryInputError, "ventana"):
            _observe(_event(available_at=BASE + timedelta(minutes=21)), authorization)

    def test_crossed_bid_ask_remains_rejected_with_authorization(self) -> None:
        authorization = _authorization()
        with self.assertRaisesRegex(CanaryInputError, "incompleto o cruzado"):
            _observe(_event(bid=1.1002, ask=1.1), authorization)

    def test_positive_spread_remains_valid_without_zero_spread_scope(self) -> None:
        observed = _observe(_event(ask=1.1002))
        self.assertLess(observed.bid, observed.ask)
        self.assertFalse(observed.to_dict()["zero_spread_effective"])


if __name__ == "__main__":
    unittest.main()
