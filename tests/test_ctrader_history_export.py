from __future__ import annotations

import argparse
import json
import os
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from mtf_lab.configuration import load_config
from mtf_lab.data.capture import CaptureEnvelope, MessageClass
from mtf_lab.data.ctrader import CTraderHistoryResult, CTraderInstrumentSpec, normalize_trendbar, synthetic_trendbar
from mtf_lab.ops.application_services import CfdPaperService
from mtf_lab.ops.ctrader_history_export import (
    CAPTURE_KIND,
    CAPTURE_ORDER,
    HistoryCaptureError,
    _write_text_atomic,
    export_history_capture,
    inspect_historical_capture,
)
from mtf_lab.ops.ctrader_pipeline import CTraderPipeline, CTraderPipelineError
from mtf_lab.ops.persistence import SQLiteStore

BASE = datetime(2026, 1, 1, tzinfo=UTC)
BAR_BASE_MINUTES = int(BASE.timestamp() // 60)
FIXTURE_ACCOUNT_ID = "fixture-demo-account"


def raw_bar(timestamp_minutes: int) -> dict[str, int]:
    value = synthetic_trendbar(
        timestamp_minutes=BAR_BASE_MINUTES + timestamp_minutes,
        low_relative=1100,
        delta_open=1,
        delta_close=2,
        delta_high=3,
    )
    value.pop("synthetic_fixture", None)
    return value


def history_fixture(
    *, complete: bool = True, has_more: bool = False, issues: tuple[str, ...] = ()
) -> tuple[CTraderHistoryResult, CTraderInstrumentSpec, dict[str, object]]:
    spec = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=314, digits=3, pip_position=2, price_scale=1_000)
    recent = {"period": 1, "symbolId": 314, "trendbar": [raw_bar(10), raw_bar(11), raw_bar(12)], "hasMore": True}
    old = {"period": 1, "symbolId": 314, "trendbar": [raw_bar(0), raw_bar(1), raw_bar(2)], "hasMore": has_more}
    receipts = (BASE + timedelta(hours=3), BASE + timedelta(hours=4))
    bars = tuple(
        normalize_trendbar(item, spec=spec, received_at=receipts[0 if index >= 3 else 1], available_at=receipts[0])
        for index, item in enumerate([*recent["trendbar"], *old["trendbar"]])
    )
    metadata = tuple(
        {
            "received_at": receipt,
            "available_at": receipt,
            "ingest_sequence": index,
            "connection_generation": 1,
            "source_identity": f"response-page-{index}",
            "response_type": "PROTO_OA_GET_TRENDBARS_RES",
            "request": {"payload_type": "PROTO_OA_GET_TRENDBARS_REQ", "payload": {"symbolId": 314}},
        }
        for index, receipt in enumerate(receipts)
    )
    history = CTraderHistoryResult(
        bars=bars,
        timeframe="M1",
        pages=2,
        complete=complete,
        has_more=has_more,
        issues=issues,
        raw_pages=(recent, old),
        page_metadata=metadata,
    )
    catalog = {
        "requested_symbol": "EUR/USD",
        "selected": 314,
        "symbols": [{"symbol_id": 314, "name": "EUR/USD", "digits": 3, "pip_position": 2, "enabled": True}],
    }
    return history, spec, catalog


class CTraderHistoryExportTests(unittest.TestCase):
    def test_no_replace_is_atomic_when_destination_appears_during_publish(self) -> None:
        original_link = os.link
        for symlink in (False, True):
            with self.subTest(symlink=symlink), tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "capture.jsonl"
                canonical = Path(tmp) / "canonical.txt"
                canonical.write_bytes(b"original unrelated bytes")

                def concurrent_publish(source, destination, *, symlink=symlink, canonical=canonical, **kwargs):
                    if symlink:
                        Path(destination).symlink_to(canonical)
                    else:
                        Path(destination).write_bytes(b"concurrent original bytes")
                    return original_link(source, destination, **kwargs)

                with (
                    patch("mtf_lab.ops.ctrader_history_export.os.link", side_effect=concurrent_publish),
                    self.assertRaises(FileExistsError),
                ):
                    _write_text_atomic(target, "replacement must not be published")
                self.assertEqual(canonical.read_bytes(), b"original unrelated bytes")
                self.assertEqual(target.is_symlink(), symlink)
                expected = b"original unrelated bytes" if symlink else b"concurrent original bytes"
                self.assertEqual(target.read_bytes(), expected)
                self.assertFalse(list(Path(tmp).glob(".capture.jsonl.*.tmp")))

    def test_provenance_allowlists_exclude_credentials_and_unknown_metadata(self) -> None:
        history, spec, catalog = history_fixture()
        marker = "fixture-sensitive-value-never-export"
        metadata = tuple(
            {
                **item,
                "request": {
                    "payload_type": "PROTO_OA_GET_TRENDBARS_REQ",
                    "authorization": marker,
                    "payload": {"symbolId": 314, "clientSecret": marker},
                },
            }
            for item in history.page_metadata
        )
        catalog = {
            **catalog,
            "clientSecret": marker,
            "symbols": [{"symbol_id": 314, "name": "EUR/USD", "metadata": {"token": marker}}],
        }
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "history.jsonl"
            result = export_history_capture(
                target,
                history=replace(history, page_metadata=metadata),
                spec=spec,
                catalog=catalog,
                environment="DEMO",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope=0,
                discovery={
                    "permissionScope": 0,
                    "accessToken": marker,
                    "clientSecret": marker,
                    "authorization": {"unexpected": marker},
                    "records": [{"account_id": FIXTURE_ACCOUNT_ID, "environment": "DEMO", "token": marker}],
                },
            )
            self.assertNotIn(marker, target.read_text())
            self.assertNotIn(marker, json.dumps(result))
            first = json.loads(target.read_text().splitlines()[0])
            provenance = first["payload"]["capture_provenance"]
            self.assertEqual(provenance["discovery"]["permissionScope"], 0)
            self.assertEqual(provenance["discovery"]["records"][0]["account_id"], FIXTURE_ACCOUNT_ID)
            self.assertEqual(provenance["request"]["payload"], {"symbolId": 314})

    def test_recent_then_old_pages_export_corrected_replay_with_observed_receipts(self) -> None:
        history, spec, catalog = history_fixture()
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "history.jsonl"
            result = export_history_capture(
                target,
                history=history,
                spec=spec,
                catalog=catalog,
                environment="DEMO",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope="SCOPE_VIEW",
                discovery={
                    "permissionScope": "SCOPE_VIEW",
                    "records": [{"account_id": FIXTURE_ACCOUNT_ID, "environment": "DEMO"}],
                },
            )
            self.assertEqual(result["capture_kind"], CAPTURE_KIND)
            self.assertEqual(result["capture_order"], CAPTURE_ORDER)
            self.assertTrue(result["complete"])
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)
            self.assertEqual(inspect_historical_capture(target)["capture_order"], CAPTURE_ORDER)

            envelopes = [CaptureEnvelope.from_mapping(json.loads(line)) for line in target.read_text().splitlines()]
            self.assertEqual(
                [item.message_class for item in envelopes],
                [MessageClass.TRENDBAR, MessageClass.TRENDBAR, MessageClass.END],
            )
            self.assertEqual(envelopes[0].event_time, BASE + timedelta(minutes=10))
            self.assertEqual(envelopes[1].event_time, BASE)
            self.assertGreater(envelopes[0].received_at, envelopes[0].available_at)
            self.assertEqual(envelopes[0].availability_policy, "historical_event_time")
            provenance = envelopes[0].payload["capture_provenance"]
            self.assertEqual(provenance["environment"], "DEMO")
            self.assertEqual(provenance["account_id"], FIXTURE_ACCOUNT_ID)
            self.assertEqual(provenance["permission_scope"], "SCOPE_VIEW")
            self.assertEqual(provenance["discovery"]["records"][0]["environment"], "DEMO")
            self.assertEqual(provenance["instrument_spec"]["symbol_id"], 314)
            self.assertEqual(provenance["instrument_spec"]["digits"], 3)
            self.assertNotIn("bid", envelopes[0].payload)
            self.assertNotIn("ask", envelopes[0].payload)

            config = replace(load_config("config/fixture_cfd.toml"), price_base="native")
            with SQLiteStore(Path(tmp) / "native.sqlite3") as store:
                result = CTraderPipeline(store, config, spec=spec).run(
                    envelopes,
                    session_id="corrected-history",
                    order="market_time_corrected",
                )
            self.assertEqual(result.analysis_basis, "native")
            self.assertEqual(len(result.analysis_records), 6)
            self.assertEqual(result.trades, ())

            with (
                SQLiteStore(Path(tmp) / "mid.sqlite3") as store,
                self.assertRaisesRegex(CTraderPipelineError, "price_base='native'"),
            ):
                CTraderPipeline(store, replace(config, price_base="mid"), spec=spec).run(
                    envelopes,
                    session_id="wrong-basis",
                    order="market_time_corrected",
                )

    def test_partial_history_is_explicit_and_invalid_empty_history_cannot_export_complete(self) -> None:
        history, spec, catalog = history_fixture(complete=False, has_more=True, issues=("invalid_bar",))
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "partial.jsonl"
            result = export_history_capture(
                target,
                history=history,
                spec=spec,
                catalog=catalog,
                environment="DEMO",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope="SCOPE_VIEW",
            )
            self.assertEqual(result["capture_status"], "PARTIAL")
            self.assertFalse(result["complete"])
            self.assertTrue(result["issues"])
            inspected = inspect_historical_capture(target)
            self.assertEqual(inspected["capture_status"], "PARTIAL")
            self.assertFalse(inspected["complete"])
            preflight = CfdPaperService()._historical_capture_preflight(
                load_config("config/fixture_cfd.toml"),
                argparse.Namespace(
                    input=target,
                    price_base="native",
                    order="market_time_corrected",
                ),
            )
            self.assertEqual(preflight.code, 2)
            self.assertEqual(preflight.payload["state"], "PARTIAL_HISTORY_CAPTURE")

            empty = CTraderHistoryResult((), "M1", 0, False, False, (), (), ())
            with self.assertRaisesRegex(HistoryCaptureError, "payloads"):
                export_history_capture(
                    Path(tmp) / "empty.jsonl",
                    history=empty,
                    spec=spec,
                    catalog=catalog,
                    environment="DEMO",
                    account_id=FIXTURE_ACCOUNT_ID,
                    endpoint="demo.ctraderapi.com:5035",
                    permission_scope="SCOPE_VIEW",
                )

    def test_real_environment_is_rejected_before_writing_capture(self) -> None:
        history, spec, catalog = history_fixture()
        with tempfile.TemporaryDirectory() as tmp, self.assertRaisesRegex(HistoryCaptureError, "environment=DEMO"):
            export_history_capture(
                Path(tmp) / "real.jsonl",
                history=history,
                spec=spec,
                catalog=catalog,
                environment="REAL",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="live.ctraderapi.com:5035",
                permission_scope="SCOPE_TRADE",
            )


if __name__ == "__main__":
    unittest.main()
