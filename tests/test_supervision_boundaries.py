from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from mtf_lab.configuration import load_config, packaged_config_path
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.supervision import (
    CTraderSupervisor,
    ExecutionCallbacks,
    SupervisorContext,
    SupervisorOptions,
    WatchdogNotifier,
    _send_desktop_alert,
)


class SupervisorBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.store = SQLiteStore(root / "fixture.sqlite3")
        self.addCleanup(self.store.close)
        self.raw: dict[str, Any] = {
            "active": True,
            "new_intents_enabled": True,
            "own_positions": [],
            "risk": {"position_state": "VALID"},
        }
        self.requests: list[str] = []
        self.reduce_result: dict[str, Any] = {"state": "COMPLETED"}

        def reduce(reason: str) -> dict[str, Any]:
            self.requests.append(reason)
            return self.reduce_result

        self.supervisor = CTraderSupervisor(
            SupervisorContext(
                SimpleNamespace(status={"connection": "CONNECTED"}, generation=1),
                load_config(packaged_config_path("ctrader_pipeline_fixture.toml")),
                self.store,
                provenance={"source_mode": "DEMO_OBSERVED", "synthetic": False},
                callbacks=ExecutionCallbacks(executor=SimpleNamespace(status=lambda: self.raw), reduce=reduce),
            ),
            SupervisorOptions(mode="demo", activate=True, state_dir=root / "state"),
        )
        state = self.supervisor.state
        state.connection_state = "CONNECTED"
        state.feed_state = "VALID"
        state.reconciliation_state = "VERIFIED"

    def test_missing_sqlite_patch_blocks_entries_not_safe_management(self) -> None:
        with patch("mtf_lab.ops.supervision.sqlite_wal_readiness", return_value={"external_continuous_ready": False}):
            risk = self.supervisor.risk_status()
        self.assertFalse(risk.entries_allowed)
        self.assertTrue(risk.management_allowed)
        self.assertIn("sqlite_wal_patch_not_verified", risk.blocked_reasons)

    def test_any_blocked_reason_prevents_entry(self) -> None:
        self.supervisor.state.risk_blocked_reasons = ("management_failed",)
        with patch("mtf_lab.ops.supervision.sqlite_wal_readiness", return_value={"external_continuous_ready": True}):
            self.assertFalse(self.supervisor.risk_status().entries_allowed)

    def test_unknown_positions_are_not_zero_known_exposure(self) -> None:
        self.raw["risk"] = {"position_state": "UNKNOWN", "position_error": "TimeoutError"}
        self.assertFalse(self.supervisor.risk_status().exposure_known)
        self.assertFalse(self.supervisor._reduce_if_safe("risk_limit"))
        self.assertEqual(self.requests, [])

    def test_stale_price_and_entry_halt_do_not_block_known_identity_reduction(self) -> None:
        self.raw["new_intents_enabled"] = False
        self.raw["halt_reason"] = "max_daily_loss_exceeded"
        self.supervisor.state.feed_state = "STALE"
        self.assertFalse(self.supervisor.risk_status().entries_allowed)
        self.assertTrue(self.supervisor._reduce_if_safe("risk_limit"))
        self.assertEqual(self.requests, ["risk_limit"])
        self.reduce_result = {"state": "UNKNOWN"}
        self.assertFalse(self.supervisor._reduce_if_safe("risk_limit"))
        self.assertEqual(self.supervisor.state.reconciliation_state, "UNKNOWN")

    def test_reconciliation_requires_positive_evidence(self) -> None:
        for value in (None, {}, [], object(), {"ok": False}, {"state": "UNKNOWN", "ok": True}):
            self.assertFalse(CTraderSupervisor._reconcile_value_verified(value))
        self.assertTrue(CTraderSupervisor._reconcile_value_verified({"ok": True, "state": "VALID"}))

    def test_signal_memory_is_bounded_and_mapping_ids_are_stable(self) -> None:
        self.assertEqual(CTraderSupervisor._signal_key({"signal_id": "s"}), "s")
        for number in range(9000):
            self.supervisor._remember_signal(str(number))
        self.assertEqual(len(self.supervisor._delivered_signal_ids), 8192)
        self.assertIn("8999", self.supervisor._delivered_signal_ids)
        self.assertNotIn("0", self.supervisor._delivered_signal_ids)

    def test_native_local_alert_and_abstract_socket_adapters(self) -> None:
        with patch("mtf_lab.ops.supervision.subprocess.run") as run:
            _send_desktop_alert("risk_halt", {"reason": "access_token=fixture-secret", "secret": "fixture-secret"})
            self.assertTrue(run.called)
            self.assertNotIn("fixture-secret", str(run.call_args))
            self.assertEqual(run.call_args.args[0][0], "/usr/bin/notify-send")
        with patch("mtf_lab.ops.supervision.socket.socket") as socket:
            WatchdogNotifier._send("@mtf-fixture", b"READY=1")
            socket.return_value.__enter__.return_value.connect.assert_called_once_with("\0mtf-fixture")
