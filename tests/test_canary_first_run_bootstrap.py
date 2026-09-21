"""Canonical first-run bootstrap checks for the deferred DEMO canary path."""

from __future__ import annotations

import os
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from mtf_lab.configuration import load_config
from mtf_lab.ops.supervision import SupervisorStateStore
from mtf_lab.ops.supervision_demo import _journal_path
from tests.test_canary_trial_anchor import ACCOUNT_ID, ENDPOINT
from tests.test_demo_canary import _CanonicalProvider, _config
from tools.demo_canary import CanaryApproval, CanaryGateError, network_cli_preflight


def _approval(*, opt_in: bool = True, source: str | None = "Usuario: autorización DEMO trial anchor") -> CanaryApproval:
    now = datetime.now(UTC).replace(microsecond=0)
    return CanaryApproval(
        approved=True,
        approval_id="first-run-bootstrap",
        account_id=ACCOUNT_ID,
        symbol="EUR/USD",
        window_start=now - timedelta(minutes=1),
        window_end=now + timedelta(minutes=20),
        max_holding_seconds="300",
        max_quantity="1000",
        max_risk_fraction="0.0005",
        max_mutation_messages=6,
        canary_trial_anchor_authorized=opt_in,
        canary_trial_anchor_authorization_source=source,
    )


def _provenance() -> dict[str, object]:
    return {
        "network_performed": True,
        "source_mode": "DEMO_OBSERVED",
        "synthetic": False,
        "environment": "DEMO",
        "account_id": ACCOUNT_ID,
        "account_selected": True,
        "account_verified": True,
        "endpoint": ENDPOINT,
        "connection_generation": "1",
    }


class _DeferredSession:
    """Authenticated CLI result with the binding deliberately still deferred."""

    def __init__(self, state_dir: Path) -> None:
        self.provider = _CanonicalProvider()
        canonical = load_config(Path("config/ctrader_demo.toml"))
        fixture = _config()
        self.config = replace(canonical, ctrader=fixture.ctrader, execution=fixture.execution)
        self.provenance = _provenance()
        self.binding = None
        self.writer_store = SupervisorStateStore(state_dir, "first-run-bootstrap", lock_root=state_dir / "locks")
        self.observe_and_arm_calls = 0
        self.closed = False

    def observe_and_arm(self, *args: object, **_kwargs: object) -> None:
        self.observe_and_arm_calls += 1
        projection_factory = args[0]
        if not callable(projection_factory):
            raise AssertionError("canonical CLI must pass a projection factory")
        # Exercise the real AccountRiskObserver created by the deferred CLI
        # path.  The fixture client is intentionally incomplete for account
        # requests; the expected stop is therefore the normal observed-risk
        # gate, not a missing-parent/journal initialization failure.
        projection_factory()
        raise CanaryGateError("bootstrap test stops after observer construction")

    def close(self) -> None:
        self.closed = True


class CanaryFirstRunBootstrapTests(unittest.TestCase):
    def test_execute_cli_bootstraps_missing_parent_before_real_observer_without_reset(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-canary-bootstrap-") as directory:
            root = Path(directory)
            state_dir = root / "missing-state"
            self.assertFalse(state_dir.exists())
            session = _DeferredSession(state_dir)
            with patch("tools.demo_canary.prepare_cli_session", return_value=session):
                result = network_cli_preflight(
                    Path("config/fixture_cfd.toml"),
                    state_dir,
                    execute=True,
                    approval=_approval(),
                    max_events=1,
                )

            canonical = state_dir / "execution-intents"
            self.assertEqual(result["state"], "ACCOUNT_RISK_BLOCKED")
            self.assertTrue(session.closed)
            self.assertEqual(session.observe_and_arm_calls, 1)
            self.assertTrue(canonical.is_dir())
            self.assertEqual(os.stat(canonical).st_mode & 0o777, 0o700)
            canonical_journal = _journal_path(state_dir, ACCOUNT_ID, ENDPOINT, account_key=None)
            self.assertEqual(canonical_journal.parent, canonical)
            # The canonical writer owns an empty 0600 journal; no intent or
            # risk baseline is fabricated by the deferred observer.
            self.assertEqual(tuple(canonical.iterdir()), (canonical_journal,))
            self.assertEqual(canonical_journal.read_bytes(), b"")
            self.assertEqual(os.stat(canonical_journal).st_mode & 0o777, 0o600)
            self.assertNotIn("directorio de estado inexistente", str(result.get("error", "")))
            self.assertNotIn("risk_journal_missing", str(result.get("error", "")))

    def test_read_only_preflight_does_not_create_state_or_journal(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-canary-read-only-") as directory:
            root = Path(directory)
            state_dir = root / "read-only-state"
            session = _DeferredSession(state_dir)
            with patch("tools.demo_canary.prepare_cli_session", return_value=session):
                result = network_cli_preflight(Path("config/fixture_cfd.toml"), state_dir, execute=False)

            self.assertEqual(result["state"], "NETWORK_PREFLIGHT_INPUTS_REQUIRED")
            self.assertTrue(session.closed)
            self.assertFalse(state_dir.exists())

    def test_invalid_opt_in_approval_is_rejected_without_state(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-canary-invalid-approval-") as directory:
            root = Path(directory)
            state_dir = root / "invalid-state"
            values = {
                "approved": True,
                "approval_id": "invalid-trial-anchor",
                "account_id": ACCOUNT_ID,
                "symbol": "EUR/USD",
                "window_start": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
                "window_end": (datetime.now(UTC) + timedelta(minutes=20)).isoformat(),
                "max_holding_seconds": "300",
                "max_quantity": "1000",
                "max_risk_fraction": "0.0005",
                "max_trial_loss_fraction": "0.001",
                "max_mutation_messages": 6,
                "stop_loss_atr_multiple": "1.5",
                "take_profit_atr_multiple": "3",
                "canary_trial_anchor_authorized": True,
                # Human authorization source deliberately absent.
            }
            with self.assertRaises(CanaryGateError):
                CanaryApproval.from_mapping(values)
            self.assertFalse(state_dir.exists())


if __name__ == "__main__":
    unittest.main()
