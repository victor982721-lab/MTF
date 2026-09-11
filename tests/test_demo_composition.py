"""Full offline cTrader DEMO composition without OAuth or network."""

from __future__ import annotations

import inspect
import json
import os
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from mtf_lab.configuration import load_config
from mtf_lab.data.capture import CaptureEnvelope, MessageClass
from mtf_lab.data.ctrader import (
    CTraderClient,
    CTraderConfig,
    DeterministicTransport,
    dependency_report,
    synthetic_spot_event,
)
from mtf_lab.ops.ctrader_demo_composition import (
    CTraderDemoApplication,
    CTraderDemoOfflineComposition,
    LoopbackProtobufTransport,
    OfflineCompositionError,
    _source_payload,
    recover_execution_intents,
    synthetic_demo_market_fixture,
)
from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DecimalValue,
    DemoAccount,
    DemoTransport,
    ExecutionIntent,
    ExecutionPolicy,
    JsonlIntentStore,
    MemoryIntentStore,
    OrderState,
    Quote,
    RealAccountForbidden,
    RiskLimitRejected,
    Side,
)
from mtf_lab.ops.persistence import SQLiteStore

SDK_AVAILABLE = dependency_report().codec_operational
fixture_time = datetime(2026, 1, 1, tzinfo=UTC)


@unittest.skipUnless(SDK_AVAILABLE, "cTrader OpenApiPy/Protobuf extra is not installed")
class DemoCompositionTests(unittest.TestCase):
    def _isolated_environment(self, root: Path) -> dict[str, str]:
        paths = {
            "HOME": root / "home",
            "XDG_CONFIG_HOME": root / "config",
            "XDG_CACHE_HOME": root / "cache",
            "XDG_DATA_HOME": root / "data",
            "XDG_STATE_HOME": root / "state",
            "TMPDIR": root / "tmp",
        }
        for path in paths.values():
            path.mkdir(parents=True, exist_ok=True)
        return {name: str(path) for name, path in paths.items()}

    def test_fixture_is_complete_and_explicitly_synthetic(self) -> None:
        fixture = synthetic_demo_market_fixture()
        self.assertEqual(len(fixture.rows), 1020)
        self.assertEqual(len(fixture.trendbars), len(fixture.rows))
        self.assertTrue(fixture.capture.coverage.complete)
        self.assertEqual(fixture.capture.provenance["source_mode"], "SYNTHETIC_FIXTURE")
        self.assertTrue(fixture.capture.provenance["synthetic"])
        self.assertTrue(all(row["synthetic_fixture"] is True for row in fixture.rows))
        self.assertEqual(len(fixture.capture.capture_hash), 64)

    def test_same_client_codec_and_server_proof_cover_the_full_vertical_slice(self) -> None:
        config = load_config("config/fixture_cfd.toml")
        with TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            env = self._isolated_environment(root)
            db = root / "run.sqlite3"
            journal = root / "intent-journal.jsonl"
            with patch.dict(os.environ, env, clear=False), SQLiteStore(db) as store:
                with CTraderDemoOfflineComposition(
                    store,
                    config,
                    intent_journal_path=journal,
                ) as composition:
                    result = composition.run()
                    self.assertIs(result.gateway.client, result.client)
                    self.assertIs(result.demo_transport.client, result.gateway)
                    self.assertIs(result.application.provider.client, result.client)
                    self.assertIs(result.application.coordinator.signal_consumer, result.application.consumer)
                    self.assertIsInstance(result.client.transport, LoopbackProtobufTransport)
                    self.assertEqual(result.client.status.auth.value, "AUTHENTICATED")
                    self.assertEqual(result.observation.environment, "DEMO")
                    self.assertEqual(result.observation.account_id, "123")
                    self.assertIsNotNone(result.provider.catalog)
                    catalog = result.provider.catalog
                    assert catalog is not None
                    self.assertIsNotNone(catalog.selected)
                    assert catalog.selected is not None
                    self.assertEqual(catalog.selected.symbol_id, 99)
                    self.assertEqual(len(result.application.signals), 1)
                    self.assertEqual(result.open_result.state, OrderState.PARTIAL)
                    self.assertEqual(result.reconciled_result.state, OrderState.FILLED)
                    self.assertEqual(result.close_result.state, OrderState.CLOSED)
                    self.assertEqual(str(result.reconciled_result.filled_quantity), "1")
                    self.assertEqual(result.close_result.position_ids, ("9001",))
                    self.assertEqual(result.executor.positions(), ())
                    self.assertEqual(result.application.consumer.signals, result.application.signals)
                    self.assertEqual(
                        result.application.execution_results[0].intent.signal_id,
                        result.application.signals[0].signal_id,
                    )

                    report = result.report
                    self.assertFalse(report["network_performed"])
                    self.assertFalse(report["oauth_real_executed"])
                    self.assertFalse(report["external_account_consulted"])
                    self.assertEqual(report["stages"]["runtime_coordinator"]["state"], "A")
                    self.assertEqual(report["stages"]["runtime_coordinator"]["signals"], 1)
                    self.assertEqual(report["stages"]["market_data"]["subscriptions"], 4)
                    self.assertEqual(report["stages"]["durable_intent"]["state"], "A")
                    self.assertEqual(report["stages"]["reconcile"]["results"], ["FILLED"])
                    self.assertEqual(report["stages"]["close_position"]["results"], ["CLOSED"])
                    self.assertEqual(report["external_validation"]["state"], "B")
                    self.assertEqual(report["user_intervention"]["state"], "C")

                    wire = report["wire"]
                    self.assertEqual(wire["codec"], "SdkProtobufCodec")
                    self.assertTrue(wire["loopback_only"])
                    self.assertEqual(wire["new_order_count"], 1)
                    self.assertEqual(wire["close_count"], 1)
                    self.assertGreaterEqual(wire["reconcile_count"], 1)
                    self.assertIn("ProtoOANewOrderReq", wire["payload_names"])
                    self.assertIn("ProtoOAExecutionEvent", wire["response_payload_names"])
                    self.assertTrue(
                        any(
                            type(message.payload).__name__ == "ProtoOAExecutionEvent"
                            for message in result.wire_transport.received
                        )
                    )
                    self.assertTrue(
                        any(
                            type(message.payload).__name__ == "ProtoOANewOrderReq"
                            for message in result.wire_transport.sent
                        )
                    )
                    self.assertTrue(
                        any(
                            type(message.payload).__name__ == "ProtoOAClosePositionReq"
                            for message in result.wire_transport.sent
                        )
                    )

                    sqlite_signals = store.list_signals(result.application.session_id)
                    sqlite_trades = store.list_cfd_trades(
                        result.application.session_id,
                        result.application.analysis_id,
                    )
                    self.assertGreaterEqual(len(sqlite_signals), 1)
                    self.assertEqual(sqlite_trades, [])

                # The composition owns the JSONL journal when a path is
                # supplied; close() must leave a durable, inspectable record.
                journal_lines = [
                    json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines() if line.strip()
                ]
                self.assertGreaterEqual(len(journal_lines), 1)
                self.assertNotIn("offline-fixture-value", journal.read_text(encoding="utf-8"))
                self.assertNotIn("fixture-access-token-ref", journal.read_text(encoding="utf-8"))

    def test_resume_uses_persisted_identity_and_does_not_skip_suffix(self) -> None:
        config = load_config("config/fixture_cfd.toml")
        with TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            env = self._isolated_environment(root)
            with (
                patch.dict(os.environ, env, clear=False),
                SQLiteStore(root / "resume.sqlite3") as store,
                CTraderDemoOfflineComposition(
                    store,
                    config,
                    intent_journal_path=root / "resume-intents.jsonl",
                ) as composition,
            ):
                first = composition.run()
                before = first.application.coordinator.processor.events_processed
                server = composition.server
                assert server is not None
                extra = synthetic_spot_event(
                    timestamp_ms=int(first.fixture.end.timestamp() * 1000),
                    symbol_id=first.fixture.symbol_id,
                    bid_relative=112485,
                    ask_relative=112505,
                )
                application = composition.application
                assert application is not None
                resumed = application.run(
                    first.fixture.rows + (extra,),
                    session_id=first.application.session_id,
                    history_count=len(first.fixture.trendbars),
                    history_from=first.fixture.start,
                    history_to=first.fixture.end,
                    capture_complete=False,
                    reconcile=True,
                    close_positions=False,
                    resume=True,
                    stream_identity=first.report["stream_identity"],
                )
                self.assertEqual(resumed.coordinator.processor.events_processed, before + 1)
                self.assertEqual(
                    first.server.message_names.count("ProtoOANewOrderReq"),
                    server.message_names.count("ProtoOANewOrderReq"),
                )
                self.assertEqual(resumed.report["resume"]["input_mode"], "full_prefix")
                self.assertTrue(resumed.report["resume"]["recovered_intent_ids"])

                with self.assertRaises(OfflineCompositionError):
                    application.run(
                        (extra,),
                        session_id=first.application.session_id,
                        history_count=len(first.fixture.trendbars),
                        history_from=first.fixture.start,
                        history_to=first.fixture.end,
                        capture_complete=False,
                        reconcile=False,
                        close_positions=False,
                        resume=True,
                        stream_identity=first.report["stream_identity"],
                    )

    def test_resume_wrong_identity_requires_compatible_checkpoint(self) -> None:
        config = load_config("config/fixture_cfd.toml")
        with TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            env = self._isolated_environment(root)
            with (
                patch.dict(os.environ, env, clear=False),
                SQLiteStore(root / "wrong-identity.sqlite3") as store,
                CTraderDemoOfflineComposition(
                    store,
                    config,
                    intent_journal_path=root / "wrong-identity.jsonl",
                ) as composition,
            ):
                first = composition.run()
                server = composition.server
                assert server is not None
                application = composition.application
                assert application is not None
                with self.assertRaisesRegex(OfflineCompositionError, "compatible persisted checkpoint"):
                    application.run(
                        first.fixture.rows,
                        session_id=first.application.session_id,
                        history_count=len(first.fixture.trendbars),
                        history_from=first.fixture.start,
                        history_to=first.fixture.end,
                        capture_complete=False,
                        reconcile=False,
                        close_positions=False,
                        resume=True,
                        stream_identity="wrong-stream-identity",
                    )
                self.assertEqual(server.message_names.count("ProtoOANewOrderReq"), 1)

    def test_resume_unknown_intent_blocks_second_signal_before_new_order(self) -> None:
        config = load_config("config/fixture_cfd.toml")
        with TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            env = self._isolated_environment(root)
            journal_path = root / "resume-unknown.jsonl"
            with (
                patch.dict(os.environ, env, clear=False),
                SQLiteStore(root / "resume-unknown.sqlite3") as store,
                CTraderDemoOfflineComposition(
                    store,
                    config,
                    intent_journal_path=journal_path,
                ) as composition,
            ):
                first = composition.run()
                server = composition.server
                assert server is not None
                unresolved = ExecutionIntent(
                    "intent-recovered-unknown",
                    "signal-recovered-unknown",
                    "EUR/USD",
                    Side.BUY,
                    DecimalValue("1"),
                    DecimalValue("1.12"),
                    fixture_time,
                    "123",
                ).to_dict()
                with journal_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps({"journal_type": "intent", **unresolved}) + "\n")
                application = composition.application
                assert application is not None
                resumed = application.run(
                    first.fixture.rows,
                    session_id=first.application.session_id,
                    history_count=len(first.fixture.trendbars),
                    history_from=first.fixture.start,
                    history_to=first.fixture.end,
                    capture_complete=False,
                    reconcile=True,
                    close_positions=False,
                    resume=True,
                    stream_identity=first.report["stream_identity"],
                )
                recovered = resumed.executor.result("intent-recovered-unknown")
                self.assertIsNotNone(recovered)
                assert recovered is not None
                self.assertEqual(recovered.state, OrderState.UNKNOWN)
                with (
                    patch.object(
                        resumed.demo_transport,
                        "submit",
                        side_effect=AssertionError("second signal must be blocked before transport"),
                    ),
                    self.assertRaisesRegex(RiskLimitRejected, "unresolved order"),
                ):
                    resumed.executor.submit_signal(
                        {
                            "signal_id": "second-signal",
                            "instrument": "EUR/USD",
                            "direction": "UP",
                            "mode": "DEMO",
                        },
                        Quote(
                            "EUR/USD",
                            DecimalValue("1.119"),
                            DecimalValue("1.120"),
                            first.fixture.end - timedelta(minutes=1, seconds=-1),
                            first.fixture.end - timedelta(minutes=1, seconds=-1),
                        ),
                    )
                self.assertEqual(server.message_names.count("ProtoOANewOrderReq"), 1)

    def test_completion_and_source_generation_are_explicit(self) -> None:
        parameter = inspect.signature(CTraderDemoApplication.run).parameters["capture_complete"]
        self.assertFalse(parameter.default)
        with self.assertRaises(OfflineCompositionError):
            CTraderDemoApplication._validate_run_options(True, True, False, False, False, "full_prefix", None, None)
        received = fixture_time + __import__("datetime").timedelta(seconds=1)
        envelope = CaptureEnvelope(
            fixture_time,
            received,
            received,
            42,
            7,
            MessageClass.SPOT,
            {
                "timestamp": int(fixture_time.timestamp() * 1000),
                "symbolId": 99,
                "bid": 110000,
                "ask": 110020,
            },
        )
        _payload, got_received, got_available, _snapshot, sequence, generation = _source_payload(
            envelope, 0, lambda: fixture_time
        )
        self.assertEqual((got_received, got_available, sequence, generation), (received, received, 42, 7))

    def test_unknown_journal_survives_restart_and_reconcile_does_not_resend(self) -> None:
        with TemporaryDirectory() as raw_tmp:
            path = Path(raw_tmp) / "intents.jsonl"
            account = DemoAccount("demo-account", "DEMO", "demo://ctrader", frozenset({"trading"}), True, True)
            transport = DemoTransport(account_id=account.account_id, endpoint=account.endpoint, scopes=account.scopes)
            transport.queue_behavior("timeout")
            policy = ExecutionPolicy(
                max_quantity=DecimalValue("1"),
                fixed_quantity=DecimalValue("1"),
                max_positions=1,
                max_exposure=DecimalValue("1000"),
                max_spread=DecimalValue("1"),
                max_price_age_seconds=DecimalValue("10"),
                timeout_seconds=DecimalValue("1"),
            )

            def clock() -> datetime:
                return fixture_time

            journal = JsonlIntentStore(path)
            try:
                first = CTraderDemoExecutor(
                    account, policy=policy, transport=transport, intent_store=journal, clock=clock
                )
                first.activate()
                outcome = first.submit_signal(
                    {"signal_id": "restart-signal", "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"},
                    Quote("EUR/USD", DecimalValue("1.1"), DecimalValue("1.1002"), fixture_time, fixture_time),
                )
                self.assertEqual(outcome.state, OrderState.UNKNOWN)
                intent_id = outcome.intent.intent_id
            finally:
                journal.close()

            resumed_journal = JsonlIntentStore(path)
            try:
                resumed = CTraderDemoExecutor(
                    account,
                    policy=policy,
                    transport=transport,
                    intent_store=resumed_journal,
                    clock=clock,
                )
                recovered = recover_execution_intents(path, resumed)
                self.assertEqual([item.intent_id for item in recovered], [intent_id])
                restored = resumed.result(intent_id)
                self.assertIsNotNone(restored)
                assert restored is not None
                self.assertEqual(restored.state, OrderState.UNKNOWN)
                self.assertEqual(restored.unknown_reason, outcome.unknown_reason)
                with patch.object(transport, "submit", side_effect=AssertionError("recovery must not submit")):
                    reconciled = resumed.reconcile(intent_id)
                self.assertEqual(reconciled.state, OrderState.UNKNOWN)
            finally:
                resumed_journal.close()
            self.assertIn(intent_id, path.read_text(encoding="utf-8"))

    def test_conflicting_duplicate_journal_payload_fails_closed(self) -> None:
        with TemporaryDirectory() as raw_tmp:
            path = Path(raw_tmp) / "conflict.jsonl"
            intent = ExecutionIntent(
                "intent-conflict",
                "signal-conflict",
                "EUR/USD",
                Side.BUY,
                DecimalValue("1"),
                DecimalValue("1.1"),
                fixture_time,
                "demo-account",
            ).to_dict()
            conflicting = dict(intent)
            conflicting["quantity"] = "2"
            path.write_text(
                json.dumps({"journal_type": "intent", **intent})
                + "\n"
                + json.dumps({"journal_type": "intent", **conflicting})
                + "\n",
                encoding="utf-8",
            )
            account = DemoAccount("demo-account", "DEMO", "demo://ctrader", frozenset({"trading"}), True, True)
            executor = CTraderDemoExecutor(
                account,
                policy=ExecutionPolicy(
                    max_quantity=DecimalValue("2"),
                    fixed_quantity=None,
                    max_positions=1,
                    max_exposure=DecimalValue("1000"),
                    max_spread=DecimalValue("1"),
                    max_price_age_seconds=DecimalValue("10"),
                    timeout_seconds=DecimalValue("1"),
                ),
                transport=DemoTransport(
                    account_id=account.account_id,
                    endpoint=account.endpoint,
                    scopes=account.scopes,
                ),
            )
            with self.assertRaisesRegex(OfflineCompositionError, "conflicting duplicate"):
                recover_execution_intents(path, executor)
            self.assertIsNone(executor.result("intent-conflict"))

    def test_activation_requires_durable_intent_journal_before_any_send(self) -> None:
        config = load_config("config/fixture_cfd.toml")
        with TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            transport = DeterministicTransport()
            client = CTraderClient(CTraderConfig(), transport=transport)
            with (
                SQLiteStore(root / "activation-guard.sqlite3") as store,
                self.assertRaisesRegex(OfflineCompositionError, "JsonlIntentStore"),
            ):
                CTraderDemoApplication(
                    store,
                    config,
                    client=client,
                    secret_provider=lambda _reference: "unused",
                    token_provider=lambda _reference: "unused",
                    selected_account_id=123,
                    intent_store=MemoryIntentStore(),
                    policy=ExecutionPolicy(),
                    activate=True,
                )
            self.assertEqual(transport.sent, [])

    def test_real_live_route_remains_fail_closed(self) -> None:
        with self.assertRaises(RealAccountForbidden):
            # This gate is checked before endpoint/client validation and thus
            # cannot accidentally reach a real gateway.
            from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor

            CTraderDemoExecutor(
                {
                    "account_id": "999",
                    "environment": "LIVE",
                    "endpoint": "live.ctraderapi.com:5035",
                    "scopes": ["trading"],
                    "selected": True,
                    "verified": True,
                }
            )


if __name__ == "__main__":
    unittest.main()
