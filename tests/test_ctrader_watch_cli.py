"""Offline acceptance of the actual CLI/provider/runtime observation path."""

from __future__ import annotations

import contextlib
import io
import json
import os
import signal
import subprocess
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import mock

from mtf_lab.configuration import load_config
from mtf_lab.ops import cli
from mtf_lab.ops.ctrader_cli_services import CTraderCliService
from mtf_lab.ops.ctrader_watch_cli import CTraderWatchCliService, _fixture_provider
from mtf_lab.ops.persistence import SQLiteStore

ROOT = Path(__file__).parents[1]


class CTraderWatchCliTests(unittest.TestCase):
    def invoke(self, db: Path, *extra: str) -> tuple[int, dict]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli.main(
                [
                    "ctrader",
                    "watch",
                    "--fixture",
                    "--db",
                    str(db),
                    "--max-events",
                    "40",
                    "--duration",
                    "5",
                    "--idle-timeout",
                    "0.1",
                    *extra,
                ]
            )
        payload = out.getvalue() if code == 0 else err.getvalue()
        start = payload.find("{")
        return code, json.loads(payload[start:])

    def test_fixture_cli_runs_real_provider_and_builds_mtf_candles_reproducibly(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            first_code, first = self.invoke(root / "a.sqlite3")
            second_code, second = self.invoke(root / "b.sqlite3")
            self.assertEqual((first_code, second_code), (0, 0))
            self.assertEqual(first["messages_this_run"], 40)
            self.assertEqual(first["data_semantic_hash"], second["data_semantic_hash"])
            self.assertEqual(first["result_semantic_hash"], second["result_semantic_hash"])
            self.assertTrue(first["provenance"]["synthetic"])
            self.assertFalse(first["provenance"]["network_performed"])
            self.assertFalse(first["provenance"]["execution_enabled"])
            self.assertEqual((root / "a.sqlite3").stat().st_mode & 0o777, 0o600)
            with SQLiteStore(root / "a.sqlite3") as store:
                candles = store.list_candles(first["session_id"])
                captures = store.list_capture_envelopes(first["session_id"])
            self.assertEqual(len(captures), 40)
            self.assertGreater(len(candles), 30)
            self.assertEqual(first["status"]["capture_state"], "PAUSED")

    def test_resume_same_slice_limit_consumes_the_next_fixture_frontier(self) -> None:
        with TemporaryDirectory() as name:
            db = Path(name) / "resume.sqlite3"
            code, first = self.invoke(db, "--max-events", "10")
            self.assertEqual(code, 0)
            code, second = self.invoke(db, "--max-events", "10", "--session", first["session_id"], "--resume")
            self.assertEqual(code, 0, second)
            self.assertTrue(second["resumed"])
            self.assertEqual(second["messages_this_run"], 10)
            self.assertEqual(second["messages"], 20)
            with SQLiteStore(db) as store:
                rows = store.list_capture_envelopes(first["session_id"])
            self.assertEqual(len(rows), 20)
            self.assertEqual([row["payload"]["sequence"] for row in rows], list(range(20)))

    def test_real_config_rejected_before_database_or_provider(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            config = root / "real.toml"
            config.write_text(
                (ROOT / "config/ctrader_query.toml").read_text().replace('environment = "DEMO"', 'environment = "REAL"')
            )
            with mock.patch("mtf_lab.data.ctrader.CTraderProvider") as provider:
                code, result = self.invoke(root / "untouched.sqlite3", "--config", str(config))
            self.assertEqual(code, 2)
            provider.assert_not_called()
            self.assertFalse((root / "untouched.sqlite3").exists())
            self.assertFalse(result["network_attempted"])

    def test_network_without_credentials_never_constructs_provider(self) -> None:
        with TemporaryDirectory() as name:
            db = Path(name) / "untouched.sqlite3"
            out, err = io.StringIO(), io.StringIO()
            with (
                mock.patch("mtf_lab.data.ctrader.CTraderProvider") as provider,
                mock.patch("mtf_lab.ops.ctrader_cli_services._token_metadata_for_config", return_value=None),
                contextlib.redirect_stdout(out),
                contextlib.redirect_stderr(err),
            ):
                code = cli.main(["ctrader", "watch", "--network", "--db", str(db)])
            self.assertEqual(code, 2)
            provider.assert_not_called()
            self.assertFalse(db.exists())
            self.assertFalse(json.loads(out.getvalue() or err.getvalue())["network_performed"])

    def test_fixture_subscribes_only_spots_and_never_orders_or_native_bars(self) -> None:
        provider, _clock = _fixture_provider(load_config(ROOT / "config/ctrader_query.toml"), 0, 1)
        try:
            names = [message.payload_type_name for message in provider.client.transport.sent]
            self.assertEqual(names, ["PROTO_OA_SUBSCRIBE_SPOTS_REQ"])
        finally:
            provider.close()

    def test_provider_is_closed_when_preparation_authentication_fails(self) -> None:
        provider = mock.Mock()
        provider.authenticate.side_effect = RuntimeError("controlled auth failure")
        config = load_config(ROOT / "config/ctrader_query.toml")
        context = SimpleNamespace(
            config=config, network_performed=False, sequence=[], app=None, client_secret="", profile=None, lease=None
        )
        with (
            mock.patch("mtf_lab.data.ctrader.CTraderProvider", return_value=provider),
            mock.patch.object(CTraderCliService, "_secret_provider", return_value=lambda _ref: ""),
            mock.patch.object(CTraderCliService, "_token_provider", return_value=lambda _ref: ""),
            self.assertRaises(RuntimeError),
        ):
            CTraderCliService()._connect_and_discover(context)
        provider.close.assert_called_once()
        self.assertTrue(context.network_performed)

    def test_non_demo_endpoint_is_rejected_before_transport_or_secret_resolution(self) -> None:
        from mtf_lab.ops.ctrader_activation import RealAccountForbidden

        original = load_config(ROOT / "config/ctrader_query.toml")
        candidates = [
            {"host": "live.ctraderapi.com"},
            {"host": "untrusted.invalid"},
            {"host": "127.0.0.1"},
            {"port": 443},
            {"environment": "live", "host": "demo.ctraderapi.com"},
        ]
        for fields in candidates:
            with self.subTest(fields=fields):
                context = SimpleNamespace(
                    config=replace(original, ctrader={**original.ctrader, **fields}),
                    network_performed=False,
                )
                with (
                    mock.patch("mtf_lab.data.ctrader.CTraderProvider") as provider,
                    mock.patch.object(CTraderCliService, "_secret_provider") as secrets,
                    mock.patch.object(CTraderCliService, "_token_provider") as tokens,
                    self.assertRaises(RealAccountForbidden),
                ):
                    CTraderCliService()._connect_and_discover(context)
                provider.assert_not_called()
                secrets.assert_not_called()
                tokens.assert_not_called()
                self.assertFalse(context.network_performed)

    def test_external_composition_stops_at_shared_readonly_gate_before_subscription(self) -> None:
        # Composition-only contract: no transport, token store or broker is used.
        from mtf_lab.ops.application_services import CommandResult
        from mtf_lab.ops.ctrader_watch import CTraderWatchOptions

        provider = mock.Mock()
        query = mock.Mock(spec=CTraderCliService)
        query._connect_and_discover.return_value = (provider, {"controlled": True})
        rejected = CommandResult.json({"ok": False, "state": "SCOPE_REJECTED"}, code=2)
        query._authorize_readonly_provider.return_value = rejected
        context = SimpleNamespace(client_secret="fixture-only-sentinel")
        with TemporaryDirectory() as name:
            db = Path(name) / "untouched.sqlite3"
            result = CTraderWatchCliService(query)._network(
                SimpleNamespace(db=db, session=None), context, CTraderWatchOptions(max_events=1)
            )
            self.assertIs(result, rejected)
            self.assertFalse(db.exists())
        query._authorize_readonly_provider.assert_called_once_with(context, provider, {"controlled": True})
        provider.subscribe.assert_not_called()
        provider.close.assert_called_once()
        self.assertEqual(context.client_secret, "")

    def test_cli_sigint_stops_its_own_observation_cleanly(self) -> None:
        with TemporaryDirectory() as name:
            db = Path(name) / "stop.sqlite3"
            command = [
                str(ROOT / "mtf-lab"),
                "ctrader",
                "watch",
                "--fixture",
                "--db",
                str(db),
                "--max-events",
                "4000",
                "--duration",
                "60",
                "--idle-timeout",
                "30",
            ]
            process = subprocess.Popen(
                command,
                cwd=ROOT,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=dict(os.environ),
            )
            try:
                marker = process.stderr.readline()
                self.assertIn("CTRADER_WATCH_STARTED", marker)
                process.send_signal(signal.SIGINT)
                output, error = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 0, error)
                result = json.loads(output)
                self.assertEqual(result["stop_reason"], "STOP_REQUESTED")
                self.assertTrue(result["clean_stop"])
                self.assertLess(result["messages_this_run"], 4000)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
