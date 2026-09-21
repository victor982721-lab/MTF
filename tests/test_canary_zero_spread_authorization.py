"""Offline scope tests for the bounded DEMO zero-spread authorization."""

from __future__ import annotations

import dataclasses
import json
import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.core.canary_quote import (
    CANARY_ZERO_SPREAD_ENDPOINT,
    CANARY_ZERO_SPREAD_SYMBOL,
    CanaryQuoteAuthorizationError,
    CanaryZeroSpreadAuthorization,
)

BASE = datetime(2026, 9, 21, 15, 0, tzinfo=UTC)
DIGEST = "a" * 64


def _authorization(**changes: object) -> CanaryZeroSpreadAuthorization:
    values: dict[str, object] = {
        "approval_digest": DIGEST,
        "authorization_source": "Usuario: zero-spread DEMO canary",
        "account_id": "5097",
        "session_id": "session-1",
        "connection_generation": "generation-1",
        "preparation_start": BASE,
        "window_start": BASE + timedelta(minutes=5),
        "window_end": BASE + timedelta(minutes=20),
    }
    values.update(changes)
    return CanaryZeroSpreadAuthorization(**values)


class CanaryZeroSpreadAuthorizationTests(unittest.TestCase):
    def test_preparation_and_order_window_matching_are_distinct(self) -> None:
        authorization = _authorization()
        identity = {
            "account_id": "5097",
            "session_id": "session-1",
            "connection_generation": "generation-1",
            "endpoint": CANARY_ZERO_SPREAD_ENDPOINT,
            "symbol": CANARY_ZERO_SPREAD_SYMBOL,
        }
        self.assertTrue(authorization.matches(**identity, now=BASE, require_order_window=False))
        self.assertFalse(authorization.matches(**identity, now=BASE, require_order_window=True))
        self.assertTrue(
            authorization.matches(
                **identity,
                now=BASE + timedelta(minutes=5),
                require_order_window=True,
            )
        )

    def test_missing_or_mismatched_scope_fails_closed(self) -> None:
        authorization = _authorization()
        common = {
            "account_id": "5097",
            "session_id": "session-1",
            "connection_generation": "generation-1",
            "endpoint": CANARY_ZERO_SPREAD_ENDPOINT,
            "symbol": CANARY_ZERO_SPREAD_SYMBOL,
            "now": BASE + timedelta(minutes=5),
            "require_order_window": True,
        }
        for field, value in (
            ("account_id", "other-account"),
            ("session_id", "other-session"),
            ("connection_generation", "other-generation"),
            ("endpoint", "demo.ctraderapi.com:5036"),
            ("symbol", "GBP/USD"),
            ("account_id", None),
        ):
            with self.subTest(field=field, value=value):
                candidate = dict(common)
                candidate[field] = value
                self.assertFalse(authorization.matches(**candidate))

    def test_clock_boundaries_and_naive_clock_fail_closed(self) -> None:
        authorization = _authorization()
        identity = {
            "account_id": "5097",
            "session_id": "session-1",
            "connection_generation": "generation-1",
            "endpoint": CANARY_ZERO_SPREAD_ENDPOINT,
            "symbol": CANARY_ZERO_SPREAD_SYMBOL,
            "require_order_window": False,
        }
        self.assertFalse(authorization.matches(**identity, now=BASE - timedelta(microseconds=1)))
        self.assertTrue(authorization.matches(**identity, now=BASE))
        self.assertFalse(authorization.matches(**identity, now=BASE + timedelta(minutes=20)))
        self.assertFalse(authorization.matches(**identity, now=BASE.replace(tzinfo=None)))

    def test_constructor_rejects_invalid_window_and_fixed_symbol_scope(self) -> None:
        invalid = (
            {"preparation_start": BASE - timedelta(minutes=1), "window_start": BASE + timedelta(minutes=5)},
            {"preparation_start": BASE, "window_start": BASE + timedelta(minutes=6)},
            {"window_end": BASE + timedelta(minutes=26)},
            {"window_end": BASE + timedelta(days=1)},
            {"symbol": "GBP/USD"},
            {"endpoint": "demo.ctraderapi.com:5036"},
            {"preparation_start": BASE.replace(tzinfo=None)},
        )
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(CanaryQuoteAuthorizationError):
                _authorization(**changes)

    def test_constructor_is_frozen_and_audit_projection_has_no_secrets(self) -> None:
        authorization = _authorization()
        self.assertTrue(dataclasses.is_dataclass(authorization))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            authorization.account_id = "other"  # type: ignore[misc]
        projection = authorization.to_dict()
        self.assertEqual(json.loads(json.dumps(projection, sort_keys=True)), projection)
        self.assertEqual(
            set(projection),
            {
                "approval_digest",
                "authorization_source",
                "account_id",
                "symbol",
                "session_id",
                "connection_generation",
                "endpoint",
                "preparation_start",
                "window_start",
                "window_end",
            },
        )
        self.assertNotIn("token", json.dumps(projection).lower())


if __name__ == "__main__":
    unittest.main()
