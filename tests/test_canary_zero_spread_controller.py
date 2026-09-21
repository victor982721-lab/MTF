"""Approval and durable-ledger regressions for the bounded quote exception."""

from __future__ import annotations

import dataclasses
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from tools.demo_canary import CanaryApproval, CanaryGateError, _ApprovalLedger, _trial_ledger_context

NOW = datetime(2026, 9, 21, 15, tzinfo=UTC)


def _approval() -> CanaryApproval:
    return CanaryApproval(
        approved=True,
        approval_id="zero-controller-fixture",
        account_id="5097",
        symbol="EUR/USD",
        window_start=NOW,
        window_end=NOW + timedelta(minutes=20),
        max_holding_seconds=Decimal("300"),
        max_quantity=Decimal("1000"),
        max_risk_fraction=Decimal("0.0005"),
        max_mutation_messages=6,
        canary_trial_anchor_authorized=True,
        canary_trial_anchor_authorization_source="fixture-start-equity-approval",
    )


class CanaryZeroSpreadControllerTests(unittest.TestCase):
    def test_quote_exception_defaults_disabled_and_requires_explicit_source(self) -> None:
        approval = _approval()
        self.assertFalse(approval.canary_zero_spread_authorized)
        self.assertIsNone(approval.canary_zero_spread_authorization_source)
        with self.assertRaises(ValueError):
            dataclasses.replace(approval, canary_zero_spread_authorized=True)
        with self.assertRaises(ValueError):
            dataclasses.replace(approval, canary_zero_spread_authorization_source="unapproved")
        with self.assertRaises(ValueError):
            dataclasses.replace(approval, canary_zero_spread_authorized="true")

    def test_ledger_rejects_changed_or_missing_zero_scope_on_update_and_finish(self) -> None:
        approval = dataclasses.replace(
            _approval(),
            canary_zero_spread_authorized=True,
            canary_zero_spread_authorization_source="fixture-zero-spread-approval",
        )
        binding = SimpleNamespace(
            observation=SimpleNamespace(
                account_id="5097",
                session_id="fixture-session",
                connection_generation="1",
                endpoint="demo.ctraderapi.com:5035",
            )
        )
        context = _trial_ledger_context(
            approval,
            binding,
            trial_equity=Decimal("1000"),
            trial_loss_budget=Decimal("1"),
            trial_cashflow_total=Decimal("0"),
        )
        for key in ("canary_zero_spread_authorized", "canary_zero_spread_authorization_source"):
            for missing in (False, True):
                for operation in ("update", "finish"):
                    with (
                        self.subTest(key=key, missing=missing, operation=operation),
                        tempfile.TemporaryDirectory(prefix="mtf-zero-ledger-") as directory,
                    ):
                        ledger = _ApprovalLedger(Path(directory))
                        digest = ledger.reserve(
                            approval,
                            trial_equity=Decimal("1000"),
                            trial_loss_budget=Decimal("1"),
                            context=context,
                        )
                        document = json.loads(ledger.path.read_text(encoding="utf-8"))
                        record = document["approvals"][digest]
                        if missing:
                            record.pop(key)
                        else:
                            record[key] = False if key.endswith("authorized") else "different-source"
                        ledger.path.write_text(json.dumps(document), encoding="utf-8")
                        before = ledger.path.read_bytes()
                        with self.assertRaisesRegex(CanaryGateError, key):
                            if operation == "update":
                                ledger.update_trial(
                                    digest,
                                    mutation_messages=0,
                                    trial_loss_committed=Decimal("0"),
                                    context=context,
                                )
                            else:
                                ledger.finish(
                                    digest,
                                    state="COMPLETED",
                                    mutation_messages=0,
                                    context=context,
                                )
                        self.assertEqual(ledger.path.read_bytes(), before)
