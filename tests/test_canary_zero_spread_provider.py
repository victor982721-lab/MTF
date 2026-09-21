"""Offline contract tests for the scoped DEMO zero-spread provider seam."""

from __future__ import annotations

import dataclasses
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from mtf_lab.core.canary_quote import CanaryZeroSpreadAuthorization
from mtf_lab.data.ctrader import (
    CTraderConfig,
    CTraderDataError,
    CTraderProvider,
    DeterministicTransport,
    QuoteQualityReason,
    QuoteQualityState,
)
from mtf_lab.data.ctrader_session import AuthenticatedSessionEvidence
from mtf_lab.ops.ctrader_canary_inputs import CanarySessionEvidence, ObservedBBO

BASE = datetime(2026, 9, 21, 17, 5, tzinfo=UTC)
APPROVAL_DIGEST = "a" * 64


class FakeDemoClient:
    def __init__(self) -> None:
        self.proof = AuthenticatedSessionEvidence(
            account_id=7,
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            scopes=frozenset({"accounts", "trading"}),
            session_id="session-1",
            connection_generation="1",
            authenticated_at=BASE,
        )

    def authenticated_session_evidence(self) -> AuthenticatedSessionEvidence:
        return self.proof

    def validate_session_evidence(self, proof: AuthenticatedSessionEvidence) -> bool:
        return proof is self.proof


def authorization(*, account_id: str = "7", session_id: str = "session-1") -> CanaryZeroSpreadAuthorization:
    return CanaryZeroSpreadAuthorization(
        approval_digest=APPROVAL_DIGEST,
        authorization_source="Usuario",
        account_id=account_id,
        session_id=session_id,
        connection_generation="1",
        preparation_start=BASE,
        window_start=BASE + timedelta(minutes=1),
        window_end=BASE + timedelta(minutes=20),
    )


def provider(
    *, client: FakeDemoClient | None = None, clock: datetime = BASE + timedelta(minutes=2)
) -> tuple[CTraderProvider, FakeDemoClient]:
    selected_client = client or FakeDemoClient()
    result = CTraderProvider(
        CTraderConfig(symbol="EUR/USD", symbol_id=99, account_id=7, quote_basis="mid"),
        client=selected_client,
        transport=DeterministicTransport(),
        clock=lambda: clock,
    )
    result.reset_generation(1)
    return result, selected_client


def payload(when: datetime, *, bid: int | None = 110000, ask: int | None = 110000) -> dict[str, object]:
    result: dict[str, object] = {
        "symbolId": 99,
        "timestamp": int(when.timestamp() * 1000),
    }
    if bid is not None:
        result["bid"] = bid
    if ask is not None:
        result["ask"] = ask
    return result


class CanaryZeroSpreadProviderTests(unittest.TestCase):
    def test_real_provider_event_bridges_to_observed_bbo_with_raw_causal_provenance(self) -> None:
        current, _ = provider()
        scoped = authorization()
        current.set_canary_zero_spread_authorization(scoped)
        event_time = BASE + timedelta(seconds=1)
        available_at = BASE + timedelta(seconds=2)
        event = current.normalize_spot(
            payload(event_time),
            received_at=available_at,
            available_at=available_at,
            generation=1,
        ).quote_events[0]
        evidence = CanarySessionEvidence(
            provider="ctrader_open_api",
            session_id="session-1",
            account_id="7",
            environment="DEMO",
            endpoint="demo.ctraderapi.com:5035",
            scopes=frozenset({"accounts", "trading"}),
            connection_generation="1",
            authenticated_at=BASE,
            evidence_source="provider-test",
        )

        observed = ObservedBBO.from_provider_event(
            event,
            provider=current,
            evidence=evidence,
            now=BASE + timedelta(seconds=3),
            window_start=BASE,
            window_end=BASE + timedelta(minutes=20),
            max_age_seconds=Decimal("30"),
            zero_spread_authorization=scoped,
        )

        self.assertEqual((observed.bid, observed.ask), (Decimal("1.1"), Decimal("1.1")))
        self.assertEqual(observed.quality_state, "VALID")
        self.assertFalse(observed.synthetic)
        self.assertEqual(observed.earliest_available_at, available_at)
        self.assertEqual(event.metadata["canary_zero_spread_raw_relation"], "BID_EQUALS_ASK")
        self.assertEqual(event.metadata["bid_available_at"], available_at.isoformat().replace("+00:00", "Z"))
        self.assertEqual(event.metadata["ask_available_at"], available_at.isoformat().replace("+00:00", "Z"))

    def test_authorized_equal_quote_is_effective_but_preserves_default_invalid_crossed(self) -> None:
        current, _ = provider()
        current.set_canary_zero_spread_authorization(authorization())

        result = current.normalize_spot(
            payload(BASE + timedelta(seconds=1)),
            received_at=BASE + timedelta(seconds=2),
            available_at=BASE + timedelta(seconds=2),
            generation=1,
        )
        event = result.quote_events[0]

        self.assertEqual((event.bid, event.ask), (1.1, 1.1))
        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.VALID.value)
        self.assertNotIn(QuoteQualityReason.CROSSED.value, event.metadata["quality_reasons"])
        self.assertTrue(event.metadata["quote_usable"])
        self.assertEqual(event.metadata["default_quality_state"], QuoteQualityState.INVALID.value)
        self.assertEqual(event.metadata["default_quality_reasons"], [QuoteQualityReason.CROSSED.value])
        self.assertTrue(event.metadata["canary_zero_spread_authorized"])
        self.assertEqual(event.metadata["canary_zero_spread_approval_digest"], APPROVAL_DIGEST)
        self.assertEqual(event.metadata["canary_zero_spread_provenance"]["authorization_source"], "Usuario")
        self.assertEqual(event.metadata["raw_timestamp_ms"], int((BASE + timedelta(seconds=1)).timestamp() * 1000))

    def test_without_context_equal_quote_remains_invalid_crossed(self) -> None:
        current = CTraderProvider(
            CTraderConfig(symbol="EUR/USD", symbol_id=99, quote_basis="mid"),
            transport=DeterministicTransport(),
        )

        result = current.normalize_spot(
            payload(BASE),
            received_at=BASE,
            available_at=BASE,
        )
        event = result.quote_events[0]

        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.INVALID.value)
        self.assertIn(QuoteQualityReason.CROSSED.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["canary_zero_spread_authorized"])
        self.assertIsNone(event.metadata.get("canary_zero_spread_raw_relation"))

    def test_setter_requires_current_typed_demo_identity(self) -> None:
        current, _ = provider()

        with self.assertRaises(CTraderDataError):
            current.set_canary_zero_spread_authorization(authorization(account_id="8"))
        with self.assertRaises(CTraderDataError):
            current.set_canary_zero_spread_authorization(object())  # type: ignore[arg-type]

    def test_generation_revalidation_revokes_context_for_new_session(self) -> None:
        current, client = provider()
        current.set_canary_zero_spread_authorization(authorization())
        client.proof = dataclasses.replace(client.proof, session_id="session-2", connection_generation="2")
        current.reset_generation(2)

        result = current.normalize_spot(
            payload(BASE + timedelta(seconds=1)),
            received_at=BASE + timedelta(seconds=2),
            available_at=BASE + timedelta(seconds=2),
            generation=2,
        )
        event = result.quote_events[0]

        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.INVALID.value)
        self.assertIn(QuoteQualityReason.CROSSED.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["canary_zero_spread_authorized"])

    def test_greater_than_quote_never_uses_zero_spread_exception(self) -> None:
        current, _ = provider()
        current.set_canary_zero_spread_authorization(authorization())

        result = current.normalize_spot(
            payload(BASE + timedelta(seconds=1), bid=110001, ask=110000),
            received_at=BASE + timedelta(seconds=2),
            available_at=BASE + timedelta(seconds=2),
            generation=1,
        )
        event = result.quote_events[0]

        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.INVALID.value)
        self.assertIn(QuoteQualityReason.CROSSED.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["canary_zero_spread_authorized"])

    def test_stale_leg_cannot_use_zero_spread_exception(self) -> None:
        current, _ = provider()
        current.max_quote_age_seconds = 1.0
        current.set_canary_zero_spread_authorization(authorization())
        current.normalize_spot(
            payload(BASE),
            received_at=BASE,
            available_at=BASE,
            generation=1,
        )

        result = current.normalize_spot(
            payload(BASE + timedelta(seconds=2), bid=110000, ask=None),
            received_at=BASE + timedelta(seconds=2),
            available_at=BASE + timedelta(seconds=2),
            generation=1,
        )
        event = result.quote_events[0]

        self.assertEqual(event.metadata["quality_state"], QuoteQualityState.INVALID.value)
        self.assertIn(QuoteQualityReason.CROSSED.value, event.metadata["quality_reasons"])
        self.assertIn(QuoteQualityReason.STALE_ASK.value, event.metadata["quality_reasons"])
        self.assertFalse(event.metadata["canary_zero_spread_authorized"])


if __name__ == "__main__":
    unittest.main()
