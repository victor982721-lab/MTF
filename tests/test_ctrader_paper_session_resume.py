"""Session-only cTrader PAPER resume must preserve durable instrument identity."""

from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from mtf_lab.configuration import load_config
from mtf_lab.data.capture import CaptureEnvelope, MessageClass
from mtf_lab.ops.application_services import CfdPaperService
from mtf_lab.ops.ctrader_history_export import export_history_capture
from mtf_lab.ops.persistence import SQLiteStore
from tests.test_ctrader_history_export import history_fixture


def _args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "config": Path("config/fixture_cfd.toml"),
        "input": None,
        "count": 190,
        "session": None,
        "db": None,
        "max_candles": 256,
        "chunk_size": 128,
        "price_base": "native",
        "order": "market_time_corrected",
        "incomplete": False,
        "include_payloads": False,
        "report": None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


FIXTURE_ACCOUNT_ID = "fixture-demo-account"


class CTraderPaperSessionResumeTests(unittest.TestCase):
    def test_historical_session_resume_recovers_observed_symbol_id(self) -> None:
        history, spec, catalog = history_fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture_path = root / "history.jsonl"
            db = root / "paper.sqlite3"
            export_history_capture(
                capture_path,
                history=history,
                spec=spec,
                catalog=catalog,
                environment="DEMO",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope="SCOPE_VIEW",
            )
            config_path = root / "native.toml"
            config_path.write_text(
                Path("config/fixture_cfd.toml")
                .read_text(encoding="utf-8")
                .replace('price_base = "mid"', 'price_base = "native"'),
                encoding="utf-8",
            )
            first = CfdPaperService().run(
                _args(config=config_path, input=capture_path, db=db, session="historical-session")
            )
            self.assertEqual(first.code, 0)
            self.assertEqual(first.payload["capture_source"], "local_file")

            # The fixture config has no cTrader symbol_id.  The resume must
            # recover 314 from the durable page provenance rather than use the
            # old fallback (99), otherwise create_analysis would fork IDs.
            resumed = CfdPaperService().run(_args(config=config_path, db=db, session="historical-session"))
            self.assertEqual(resumed.code, 0)
            self.assertEqual(resumed.payload["capture_source"], "sqlite_capture_envelopes")
            self.assertEqual(resumed.payload["paper_analysis_id"], first.payload["paper_analysis_id"])
            self.assertEqual(resumed.payload["runtime_analysis_id"], first.payload["runtime_analysis_id"])
            self.assertEqual(resumed.payload["capture"]["capture_hash"], first.payload["capture"]["capture_hash"])
            self.assertEqual(resumed.payload["historical_capture"]["observed_spec"]["symbol_id"], 314)
            with SQLiteStore(db) as store:
                self.assertEqual(len(store.analyses("historical-session")), 2)
                self.assertEqual(len(store.list_capture_envelopes("historical-session")), 3)

    def test_historical_session_with_changed_defaults_rejects_before_forking_identity(self) -> None:
        history, spec, catalog = history_fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capture_path = root / "history.jsonl"
            db = root / "paper.sqlite3"
            export_history_capture(
                capture_path,
                history=history,
                spec=spec,
                catalog=catalog,
                environment="DEMO",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope="SCOPE_VIEW",
            )
            config_path = root / "native.toml"
            config_path.write_text(
                Path("config/fixture_cfd.toml")
                .read_text(encoding="utf-8")
                .replace('price_base = "mid"', 'price_base = "native"'),
                encoding="utf-8",
            )
            first = CfdPaperService().run(
                _args(config=config_path, input=capture_path, db=db, session="historical-session")
            )
            self.assertEqual(first.code, 0)

            # Omitting the historical flags would otherwise change the
            # analysis basis/order and create a second identity (or crash on
            # conflicting durable candle rows).  It must fail before run().
            rejected = CfdPaperService().run(
                _args(
                    config=Path("config/fixture_cfd.toml"),
                    db=db,
                    session="historical-session",
                    price_base=None,
                    order="as_observed",
                )
            )
            self.assertEqual(rejected.code, 2)
            self.assertEqual(rejected.payload["state"], "HISTORICAL_NATIVE_REQUIRED")
            with SQLiteStore(db) as store:
                self.assertEqual(len(store.analyses("historical-session")), 2)

    def test_historical_session_without_observed_spec_rejects_before_analysis(self) -> None:
        history, spec, catalog = history_fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "history.jsonl"
            export_history_capture(
                source,
                history=history,
                spec=spec,
                catalog=catalog,
                environment="DEMO",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope="SCOPE_VIEW",
            )
            envelopes = []
            for line in source.read_text(encoding="utf-8").splitlines():
                envelope = CaptureEnvelope.from_mapping(json.loads(line))
                if envelope.message_class is MessageClass.TRENDBAR:
                    payload = dict(envelope.payload)
                    provenance = dict(payload["capture_provenance"])
                    provenance.pop("instrument_spec", None)
                    payload["capture_provenance"] = provenance
                    envelope = replace(envelope, payload=payload)
                envelopes.append(envelope)

            db = root / "paper.sqlite3"
            with SQLiteStore(db) as store:
                config = load_config("config/fixture_cfd.toml")
                store.create_session(
                    session_id="missing-spec",
                    mode="REPLAY",
                    provider="ctrader-open-api",
                    instrument="EUR/USD",
                    config=config.to_dict(),
                    dataset_ref="durable-identity",
                )
                for envelope in envelopes:
                    store.save_capture_envelope("missing-spec", envelope)

            result = CfdPaperService().run(_args(db=db, session="missing-spec"))
            self.assertEqual(result.code, 2)
            self.assertEqual(result.payload["state"], "SESSION_CAPTURE_SPEC_REQUIRED")
            with SQLiteStore(db) as store:
                self.assertEqual(store.analyses("missing-spec"), [])


if __name__ == "__main__":
    unittest.main()
