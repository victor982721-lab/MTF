"""Presentation-boundary regressions for the historical research interfaces."""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mtf_lab.ops.cli import build_parser
from mtf_lab.ops.cli_handlers import cmd_market_data, cmd_market_research, cmd_research, cmd_ui


class MarketCliTests(unittest.TestCase):
    def test_campaign_init_creates_only_protocol_and_empty_registry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            protocol, registry = Path(directory) / "protocol.json", Path(directory) / "registry.jsonl"
            args = build_parser().parse_args(
                [
                    "research",
                    "campaign",
                    "init",
                    "--protocol",
                    str(protocol),
                    "--registry",
                    str(registry),
                ]
            )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(cmd_market_research(args), 0)
            response = json.loads(output.getvalue())
            self.assertEqual(response["operation"], "INIT")
            self.assertIs(response["trading_enabled"], False)
            self.assertTrue(protocol.is_file())
            self.assertTrue(registry.is_file())
            self.assertEqual(registry.read_bytes(), b"")
            self.assertEqual(protocol.stat().st_mode & 0o777, 0o600)

    def test_legacy_research_dispatch_is_unchanged(self) -> None:
        args = build_parser().parse_args(["research", "run", "--fixture", "--manifest", "/private/attempt.json"])
        self.assertIs(args.func, cmd_research)
        self.assertEqual(args.research_action, "run")

    def test_historical_models_are_explicit_and_registered_identifiers(self) -> None:
        from mtf_lab.ops.historical_assumptions import (
            PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID,
            VIRTUAL_EURUSD_10K_MODEL_ID,
        )

        base = [
            "research",
            "campaign",
            "register",
            "--protocol",
            "/private/protocol.json",
            "--registry",
            "/private/trials.jsonl",
            "--dataset-manifest",
            "/private/data.json",
            "--stage",
            "pilot-week",
        ]
        implicit = build_parser().parse_args(base)
        self.assertIsNone(implicit.assumptions_model)
        self.assertIsNone(implicit.calendar_model)
        explicit = build_parser().parse_args(
            [
                *base,
                "--assumptions-model",
                VIRTUAL_EURUSD_10K_MODEL_ID,
                "--calendar-model",
                PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID,
                "--contract-spec",
                "/private/contract.json",
                "--calendar-state",
                "/private/calendar.json",
            ]
        )
        self.assertEqual(explicit.assumptions_model, VIRTUAL_EURUSD_10K_MODEL_ID)
        self.assertEqual(explicit.calendar_model, PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID)
        self.assertEqual(explicit.contract_spec, Path("/private/contract.json"))
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            build_parser().parse_args([*base, "--assumptions-model", "account_verified"])

    def test_market_data_describe_accepts_explicit_registry_without_enabling_trading(self) -> None:
        args = build_parser().parse_args(
            [
                "market-data",
                "describe",
                "/private/data.json",
                "--start",
                "2016-03-07T00:00:00Z",
                "--end",
                "2016-03-14T00:00:00Z",
                "--registry",
                "/private/trials.jsonl",
            ]
        )
        self.assertEqual(args.registry, Path("/private/trials.jsonl"))
        self.assertFalse(getattr(args, "trading_enabled", False))

    def test_calendar_identity_does_not_silently_conflict_with_digest(self) -> None:
        base = [
            "research",
            "campaign",
            "register",
            "--protocol",
            "/private/protocol.json",
            "--registry",
            "/private/trials.jsonl",
            "--dataset-manifest",
            "/private/data.json",
            "--stage",
            "pilot-week",
        ]
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            build_parser().parse_args(
                [
                    *base,
                    "--calendar-hash",
                    "a" * 64,
                    "--calendar-identity",
                    "/private/calendar.json",
                ]
            )

    def test_describe_rejects_holdout_before_reading_manifest_or_quotes(self) -> None:
        args = build_parser().parse_args(
            [
                "market-data",
                "describe",
                "/private/holdout.json",
                "--start",
                "2024-01-01T00:00:00Z",
                "--end",
                "2025-01-01T00:00:00Z",
            ]
        )
        with (
            patch("mtf_lab.data.historical.read_manifest", side_effect=AssertionError("holdout manifest read")),
            self.assertRaisesRegex(ValueError, "holdout cerrado"),
        ):
            cmd_market_data(args)

    def test_acquisition_requires_terms_before_downloader_import(self) -> None:
        args = build_parser().parse_args(["market-data", "acquire"])
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertEqual(cmd_market_data(args), 2)
        self.assertIn("TERMS_GATE", output.getvalue())
        self.assertIs(json.loads(output.getvalue())["network_performed"], False)

    def test_acquisition_cannot_expand_pilot_with_a_cli_month(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            build_parser().parse_args(["market-data", "acquire", "--month", "2024-01", "--terms-accepted"])

    def test_acquisition_dispatches_explicit_development_month(self) -> None:
        args = build_parser().parse_args(
            ["market-data", "acquire", "--month", "2017-04", "--data-root", "/private/market-data", "--terms-accepted"]
        )
        fake_receipt = SimpleNamespace(to_dict=lambda: {"month": "201704", "status": "DOWNLOADED"})
        output = io.StringIO()
        with (
            patch("mtf_lab.data.histdata_acquisition.acquire_month", return_value=fake_receipt) as acquire_month,
            contextlib.redirect_stdout(output),
        ):
            self.assertEqual(cmd_market_data(args), 0)
        acquire_month.assert_called_once_with(
            2017,
            4,
            data_root=Path("/private/market-data"),
            terms_accepted=True,
            timeout_seconds=120.0,
        )
        self.assertEqual(json.loads(output.getvalue())["month"], "201704")

    def test_snapshot_and_database_are_mutually_exclusive(self) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            build_parser().parse_args(["ui", "--snapshot", "/private/view.json", "--db", "/private/store.db"])

    def test_snapshot_skips_configuration_and_database_resolution(self) -> None:
        args = build_parser().parse_args(["ui", "--snapshot", "/private/view.json", "--duration", "0.01"])
        with (
            patch("mtf_lab.ops.application_services._config_for", side_effect=AssertionError("config opened")),
            patch("mtf_lab.ops.application_services._db_for", side_effect=AssertionError("db resolved")),
            patch("mtf_lab.ops.ui.serve") as serve,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(cmd_ui(args), 0)
        self.assertEqual(serve.call_args.args, (None,))
        self.assertEqual(serve.call_args.kwargs["snapshot_path"], Path("/private/view.json"))


if __name__ == "__main__":
    unittest.main()
