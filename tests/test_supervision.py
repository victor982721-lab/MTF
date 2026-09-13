"""Offline contracts for the H5 cTrader supervisor."""

from __future__ import annotations

import json
import unittest
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, cast

from mtf_lab.configuration import EffectiveConfig, load_config
from mtf_lab.ops.ctrader_watch import (
    CTraderWatchContext,
    CTraderWatchOptions,
    CTraderWatchResult,
    run_ctrader_watch,
)
from mtf_lab.ops.ctrader_watch_cli import _fixture_provider
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.supervision import (
    CommandService,
    CTraderSupervisor,
    DbusAlertSink,
    ExecutionCallbacks,
    PersistentSupervisorState,
    ReadinessStatus,
    SingleWriterBusy,
    StateCorrupt,
    SupervisorContext,
    SupervisorMode,
    SupervisorOptions,
    SupervisorStateStore,
    WatchdogNotifier,
    read_supervisor_readiness,
    supervise_command,
)
from mtf_lab.ops.supervision_composition import NetworkPreparation, _reconnect_preparation

ROOT = Path(__file__).parents[1]
BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _config() -> EffectiveConfig:
    config = load_config(ROOT / "config" / "ctrader_query.toml")
    data = dict(config.data)
    data["mode"] = "SYNTHETIC"
    return replace(config, mode="SYNTHETIC", data=data)


class _Provider:
    def __init__(self, connection: str = "CONNECTED") -> None:
        self.connection = connection
        self.reconnect_calls = 0
        self.generation = 1

    @property
    def status(self) -> dict[str, str]:
        return {"connection": self.connection}

    def reconnect(self) -> None:
        self.reconnect_calls += 1
        self.connection = "CONNECTED"

    def close(self) -> None:
        self.connection = "DISCONNECTED"


class _ResultRunner:
    def __init__(self, result: CTraderWatchResult | None = None, failure: Exception | None = None) -> None:
        self.result = result
        self.failure = failure
        self.coordinator = SimpleNamespace(signals=())

    def run(self) -> CTraderWatchResult:
        if self.failure is not None:
            raise self.failure
        assert self.result is not None
        return self.result


def _watch_result(*, connection: str = "CONNECTED", stop_reason: str = "MAX_EVENTS") -> CTraderWatchResult:
    return CTraderWatchResult(
        session_id="session-supervise",
        analysis_id="analysis-supervise",
        semantic_identity="semantic-supervise",
        data_semantic_hash="data-hash",
        result_semantic_hash="result-hash",
        messages=2,
        messages_this_run=2,
        events=2,
        events_this_run=2,
        bars=0,
        snapshots=0,
        heartbeats=0,
        ignored_messages=0,
        duplicates=0,
        generation_changes=0,
        stop_reason=stop_reason,
        elapsed_seconds=0.1,
        idle_seconds=0.0,
        clean_stop=True,
        resumed=False,
        status={
            "connection": connection,
            "reconciliation_state": "NOT_APPLICABLE",
            "freshness_state": "NOT_APPLICABLE",
            "last_market_time": None,
        },
        provenance={"source_mode": "SYNTHETIC_FIXTURE", "synthetic": True},
    )


class SupervisorTests(unittest.TestCase):
    def _context(
        self,
        root: Path,
        provider: Any,
        *,
        callbacks: ExecutionCallbacks | None = None,
        runner: Any | None = None,
        clock: Any | None = None,
        market_state: Any | None = None,
    ) -> SupervisorContext:
        db = root / "runtime.sqlite3"
        store = SQLiteStore(db)
        self.addCleanup(store.close)
        return SupervisorContext(
            provider,
            _config(),
            store,
            provenance={
                "provider": "fixture-provider",
                "source_mode": "SYNTHETIC_FIXTURE",
                "synthetic": True,
                "environment": "OFFLINE",
                "network_performed": False,
                "execution_enabled": False,
            },
            callbacks=callbacks or ExecutionCallbacks(),
            clock=clock,
            market_state=market_state,
            watch_runner_factory=runner,
        )

    def test_command_without_source_does_not_create_database_or_provider(self) -> None:
        with TemporaryDirectory() as name:
            db = Path(name) / "not-created.sqlite3"
            args = SimpleNamespace(mode="observe", fixture=False, network=False, db=db)
            code, payload = supervise_command(args)
            self.assertEqual(code, 2)
            self.assertEqual(payload["state"], "SOURCE_REQUIRED")
            self.assertFalse(db.exists())

    def test_fixture_command_reuses_provider_runtime_and_persists_safe_envelope(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            args = SimpleNamespace(
                mode="observe",
                fixture=True,
                network=False,
                activate=False,
                duration=5.0,
                max_events=12,
                idle_timeout=0.1,
                poll_timeout=0.01,
                checkpoint_every=4,
                max_candles=5000,
                max_reconnect_attempts=1,
                account_key="demo-account",
                state_dir=root / "state",
                db=root / "capture.sqlite3",
                config=ROOT / "config" / "ctrader_query.toml",
                report=None,
                continuous=False,
            )
            command = CommandService()
            result = command.run(args)
            self.assertEqual(result.code, 0, result.rendered())
            payload = result.payload
            self.assertEqual(payload["messages"], 12)
            self.assertFalse(payload["network_attempted"])
            self.assertFalse(payload["execution_enabled"])
            state_path = root / "state" / "demo-account.json"
            raw = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(raw["schema_version"], 1)
            self.assertEqual(raw["mode"], "observe")
            self.assertIn("readiness", raw)
            self.assertIn("freshness_state", raw)
            self.assertIn("runtime_identity", raw)
            self.assertLessEqual(raw["runtime_identity"]["valid_for_seconds"], 60)
            for key in (
                "backlog",
                "connection_generation",
                "exposure_state",
                "protection_state",
                "economic_state",
            ):
                self.assertIn(key, raw)
            self.assertEqual(state_path.stat().st_mode & 0o777, 0o600)
            with SQLiteStore(root / "capture.sqlite3") as store:
                session_id = payload["session_id"]
                self.assertEqual(len(store.list_capture_envelopes(session_id)), 12)

    def test_fixture_supervisor_services_management_before_watch_finishes(self) -> None:
        managed: list[str] = []
        with TemporaryDirectory() as name:
            root = Path(name)
            args = SimpleNamespace(
                mode="observe",
                fixture=True,
                network=False,
                activate=False,
                duration=5.0,
                max_events=4,
                idle_timeout=0.1,
                poll_timeout=0.01,
                checkpoint_every=100,
                max_candles=5000,
                max_reconnect_attempts=1,
                account_key="fixture",
                state_dir=root / "state",
                db=root / "capture.sqlite3",
                config=ROOT / "config" / "ctrader_query.toml",
                report=None,
                continuous=False,
            )
            result = CommandService(callbacks=ExecutionCallbacks(manage=lambda: managed.append("manage"))).run(args)
            self.assertEqual(result.code, 0, result.rendered())
            self.assertTrue(managed)

    def test_shadow_fixture_dispatches_signals_before_the_final_summary(self) -> None:
        seen: list[str] = []
        with TemporaryDirectory() as name:
            root = Path(name)
            args = SimpleNamespace(
                mode="shadow",
                fixture=True,
                network=False,
                activate=False,
                duration=5.0,
                max_events=190,
                idle_timeout=0.2,
                poll_timeout=0.01,
                checkpoint_every=100,
                max_candles=5000,
                max_reconnect_attempts=1,
                account_key="fixture",
                state_dir=root / "state",
                db=root / "capture.sqlite3",
                config=ROOT / "config" / "ctrader_pipeline_fixture.toml",
                report=None,
                continuous=False,
            )
            result = CommandService(
                callbacks=ExecutionCallbacks(on_signal=lambda signal: seen.append(signal.signal_id))
            ).run(args)
            self.assertEqual(result.code, 0, result.rendered())
            self.assertGreaterEqual(result.payload["signals"], 1)
            self.assertEqual(len(seen), result.payload["signals"])

    def test_borrowed_watch_keeps_provider_for_owner_shutdown_and_publishes_ids_early(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            config = _config()
            provider, clock = _fixture_provider(config, 0, 130)
            store = SQLiteStore(root / "capture.sqlite3")
            self.addCleanup(store.close)
            ids: list[tuple[str, str]] = []
            context = CTraderWatchContext(
                provider,
                config,
                store,
                provenance={
                    "provider": "ctrader_open_api_fixture",
                    "source_mode": "SYNTHETIC_FIXTURE",
                    "synthetic": True,
                    "environment": "OFFLINE",
                    "network_performed": False,
                    "execution_enabled": False,
                },
                clock=clock,
                close_provider=False,
                on_initialized=lambda session, analysis: ids.append((session, analysis)),
            )
            result = run_ctrader_watch(
                context,
                CTraderWatchOptions(max_events=1, idle_timeout_seconds=0.1, poll_timeout_seconds=0.01),
            )
            self.assertTrue(result.clean_stop)
            self.assertEqual(ids, [(result.session_id, result.analysis_id)])
            self.assertTrue(provider.status.connection in {"CONNECTED", "HEALTHY"})
            provider.close()

    def test_failed_watch_reconnects_once_and_retries_same_session_without_budget_reset(self) -> None:
        attempts: list[str | None] = []
        result = replace(_watch_result(), session_id="session-retry", analysis_id="analysis-retry")

        def factory(ctx: CTraderWatchContext, _options: CTraderWatchOptions) -> _ResultRunner:
            attempts.append(ctx.session_id)
            if len(attempts) == 1:
                assert ctx.on_initialized is not None
                ctx.on_initialized("session-retry", "analysis-retry")
                return _ResultRunner(failure=RuntimeError("temporary socket reset"))
            return _ResultRunner(result=result)

        with TemporaryDirectory() as name:
            root = Path(name)
            provider = _Provider()
            context = self._context(root, provider, runner=factory)
            supervisor = CTraderSupervisor(
                context,
                SupervisorOptions(
                    mode="observe",
                    fixture=True,
                    state_dir=root / "state",
                    max_events=5,
                    max_reconnect_attempts=1,
                ),
            )
            summary = supervisor.run()
            self.assertTrue(summary.ok, summary.to_dict())
            self.assertEqual(attempts, [None, "session-retry"])
            self.assertEqual(summary.reconnects, 1)
            self.assertEqual(summary.session_id, "session-retry")

    def test_shutdown_manages_reduces_reconciles_and_closes_provider_last(self) -> None:
        calls: list[str] = []

        class Executor:
            def status(self) -> dict[str, Any]:
                return {
                    "active": True,
                    "paused": False,
                    "new_intents_enabled": True,
                    "own_positions": [{"position_id": "owned-1"}],
                    "risk": {"position_state": "VALID", "protection_state": "OBSERVED"},
                }

        class Provider(_Provider):
            def close(self) -> None:
                calls.append("close")
                super().close()

        with TemporaryDirectory() as name:
            root = Path(name)
            callbacks = ExecutionCallbacks(
                executor=Executor(),
                manage=lambda: calls.append("manage"),
                reconcile=lambda: calls.append("reconcile") or {"ok": True, "state": "VALID"},
                reduce=lambda _reason: calls.append("reduce") or {"state": "COMPLETED"},
            )
            context = self._context(
                root,
                Provider(),
                callbacks=callbacks,
                runner=lambda _ctx, _options: _ResultRunner(result=_watch_result()),
            )
            supervisor = CTraderSupervisor(
                context,
                SupervisorOptions(mode="demo", fixture=True, activate=True, state_dir=root / "state", max_events=1),
            )
            summary = supervisor.run()
            self.assertTrue(summary.ok, summary.to_dict())
            self.assertIn("reduce", calls)
            self.assertGreater(calls.index("close"), calls.index("reduce"))
            self.assertGreater(calls.index("reconcile", calls.index("reduce")), calls.index("reduce"))

    def test_risk_halt_during_poll_manages_reduces_known_exposure_and_stops(self) -> None:
        calls: list[str] = []

        class Executor:
            def status(self) -> dict[str, Any]:
                return {
                    "active": True,
                    "paused": True,
                    "new_intents_enabled": False,
                    "own_positions": [{"position_id": "owned-1"}],
                    "risk": {
                        "position_state": "VALID",
                        "foreign_positions": [],
                        "protection_state": "OBSERVED",
                        "halt_reason": "max_daily_loss_exceeded",
                    },
                }

        with TemporaryDirectory() as name:
            root = Path(name)
            context = self._context(
                root,
                _Provider(),
                callbacks=ExecutionCallbacks(
                    executor=Executor(),
                    manage=lambda: calls.append("manage"),
                    reconcile=lambda: calls.append("reconcile") or {"ok": True, "state": "VALID"},
                    reduce=lambda reason: calls.append(f"reduce:{reason}") or {"state": "COMPLETED"},
                ),
            )
            supervisor = CTraderSupervisor(
                context,
                SupervisorOptions(mode="demo", fixture=True, activate=True, state_dir=root / "state"),
            )
            supervisor.state.execution_enabled = True
            supervisor.state.feed_state = "NOT_APPLICABLE"
            supervisor.state.reconciliation_state = "NOT_APPLICABLE"
            supervisor._service_tick()
            self.assertTrue(supervisor.state.stop_requested)
            self.assertIn("reduce:max_daily_loss_exceeded", calls)

    def test_latched_daily_loss_persists_and_restart_never_reopens(self) -> None:
        class Executor:
            def status(self) -> dict[str, Any]:
                return {
                    "active": True,
                    "paused": True,
                    "new_intents_enabled": False,
                    "own_positions": [{"position_id": "owned-1"}],
                    "risk": {
                        "position_state": "VALID",
                        "foreign_positions": [],
                        "protection_state": "OBSERVED",
                        "halt_reason": "max_daily_loss_exceeded",
                    },
                }

        with TemporaryDirectory() as name:
            root = Path(name)
            callbacks = ExecutionCallbacks(
                executor=Executor(),
                manage=lambda: None,
                reconcile=lambda: {"ok": True, "state": "VALID"},
                reduce=lambda _reason: {"state": "COMPLETED"},
            )
            first = CTraderSupervisor(
                self._context(root, _Provider(), callbacks=callbacks),
                SupervisorOptions(mode="demo", fixture=True, activate=True, state_dir=root / "state", max_events=1),
            )
            first_result = first.run()
            self.assertTrue(first_result.fatal_latched)
            raw = json.loads((root / "state" / "demo-account.json").read_text())
            self.assertTrue(raw["fatal_latched"])
            second_provider = _Provider()
            second = CTraderSupervisor(
                self._context(root, second_provider, callbacks=callbacks),
                SupervisorOptions(mode="demo", fixture=True, activate=True, state_dir=root / "state", max_events=1),
            )
            second_result = second.run()
            self.assertFalse(second_result.ok)
            self.assertEqual(second_result.stop_reason, "FATAL_LATCHED")
            self.assertEqual(second_provider.connection, "DISCONNECTED")

    def test_network_reconnect_reauthenticates_same_provider_and_rejects_scope_change(self) -> None:  # noqa: C901
        class Provider:
            name = "ctrader_open_api"

            def __init__(self) -> None:
                self.generation = 1
                self.config = SimpleNamespace(host="demo.ctraderapi.com", port=5035)
                self.client = SimpleNamespace(authenticated_account_id=7)
                self._status = SimpleNamespace(generation=1, auth=SimpleNamespace(value="AUTHENTICATED"))
                self.calls: list[str] = []
                self.scope_change = False

            @property
            def status(self) -> Any:
                return self._status

            def reconnect(self) -> Any:
                self.calls.append("reconnect")
                self.generation = 2
                self._status = SimpleNamespace(generation=2, auth=SimpleNamespace(value="REQUIRED"))
                return self._status

            def authenticate(self, **kwargs: Any) -> None:
                self.calls.append(f"authenticate:{kwargs['authorize_selected']}")
                self._status.auth = SimpleNamespace(value="AUTHENTICATED")

            def discover_accounts(self, *, include_token: bool = False) -> dict[str, Any]:
                self.calls.append(f"discover:{include_token}")
                environment = "LIVE" if self.scope_change else "DEMO"
                return {"records": [{"account_id": 7, "environment": environment}]}

            def subscribe(self, *, timeframes: tuple[str, ...]) -> None:
                self.calls.append(f"subscribe:{timeframes}")

        class Metadata:
            token_ref = "token-ref"

            @staticmethod
            def is_expired(_now: datetime) -> bool:
                return False

        profile = SimpleNamespace(
            environment="DEMO",
            operation_mode=SimpleNamespace(value="QUERY"),
            required_scopes=frozenset({"accounts"}),
            token_ref="token-ref",
            account_id="7",
        )
        metadata = Metadata()
        lease = SimpleNamespace(metadata=metadata, access_token="opaque-token")
        query_context = SimpleNamespace(
            profile=profile,
            metadata=metadata,
            lease=lease,
            app=SimpleNamespace(),
            client_secret="opaque-secret",
            sequence=[],
            config=_config(),
        )
        provider = Provider()
        old_binding = SimpleNamespace(close=lambda: provider.calls.append("old_binding_close"))
        preparation = NetworkPreparation(
            provider,
            cast(Any, query_context),
            {
                "source_mode": "DEMO_OBSERVED",
                "endpoint": "demo.ctraderapi.com:5035",
                "network_performed": True,
            },
            {},
            None,
            old_binding,
        )

        class Service:
            @staticmethod
            def _secret_provider(*_args: Any) -> Any:
                return lambda _ref: "opaque-secret"

            @staticmethod
            def _token_provider(*_args: Any) -> Any:
                return lambda _ref: "opaque-token"

            @staticmethod
            def _authorize_readonly_provider(_context: Any, _provider: Any, _observed: Any) -> Any:
                return object(), {"ok": True}

        output = _reconnect_preparation(preparation, cast(Any, Service()), SimpleNamespace(state_dir="/tmp"), False)
        self.assertTrue(output["ok"])
        self.assertEqual(preparation.provider, provider)
        self.assertIsNone(preparation.callbacks)
        self.assertIn("old_binding_close", provider.calls)
        self.assertIn("authenticate:False", provider.calls)
        self.assertIn("discover:True", provider.calls)
        self.assertIn("subscribe:()", provider.calls)
        self.assertEqual(output["connection_generation"], 2)

        changed = Provider()
        changed.scope_change = True
        changed_preparation = NetworkPreparation(
            changed,
            cast(Any, query_context),
            {"source_mode": "DEMO_OBSERVED", "endpoint": "demo.ctraderapi.com:5035", "network_performed": True},
            {},
            None,
            SimpleNamespace(close=lambda: changed.calls.append("old_binding_close")),
        )
        with self.assertRaises(RuntimeError):
            _reconnect_preparation(changed_preparation, cast(Any, Service()), SimpleNamespace(state_dir="/tmp"), False)
        self.assertIsNone(changed_preparation.callbacks)
        self.assertIsNone(changed_preparation.binding)

    def test_demo_mode_without_activation_never_calls_execution_callbacks(self) -> None:
        calls: list[str] = []
        callbacks = ExecutionCallbacks(on_signal=lambda _signal: calls.append("signal"))
        with TemporaryDirectory() as name:
            root = Path(name)
            provider = _Provider()
            context = self._context(root, provider, callbacks=callbacks)
            # A runner with no signals is sufficient to verify the default gate;
            # the actual fixture provider/runtime path is covered above.
            supervisor = CTraderSupervisor(
                context,
                SupervisorOptions(
                    mode="demo",
                    fixture=True,
                    state_dir=root / "state",
                    db_path=root / "runtime.sqlite3",
                    max_events=1,
                ),
                alert_sink=DbusAlertSink(),
            )
            supervisor._last_signals = (object(),)
            processed = supervisor._process_signals(supervisor._last_signals)
            self.assertEqual(processed, 1)
            self.assertEqual(calls, [])
            self.assertFalse(supervisor.risk_status().entries_allowed)

    def test_demo_signal_dispatch_requires_fresh_feed_known_exposure_and_no_halt(self) -> None:
        class Executor:
            def __init__(self, **values: Any) -> None:
                self.values = values

            def status(self) -> dict[str, Any]:
                return {
                    "active": True,
                    "paused": False,
                    "new_intents_enabled": True,
                    "own_positions": [],
                    **self.values,
                }

        with TemporaryDirectory() as name:
            root = Path(name)
            delivered: list[Any] = []
            executor = Executor()
            supervisor = CTraderSupervisor(
                self._context(
                    root, _Provider(), callbacks=ExecutionCallbacks(executor=executor, on_signal=delivered.append)
                ),
                SupervisorOptions(mode="demo", fixture=True, activate=True, state_dir=root / "state"),
            )
            supervisor.state.execution_enabled = True
            supervisor.state.feed_state = "NOT_APPLICABLE"
            supervisor.state.reconciliation_state = "NOT_APPLICABLE"
            fresh_at = datetime.now(UTC)
            fresh_values = {"available_at": fresh_at, "connection_generation": 1}
            supervisor._dispatch_signal(SimpleNamespace(signal_id="fresh", detected_at=fresh_at, values=fresh_values))
            self.assertEqual(len(delivered), 1)

            for values, feed in (
                ({}, "STALE"),
                ({"foreign_positions": True}, "NOT_APPLICABLE"),
                ({"halt_reason": "manual"}, "NOT_APPLICABLE"),
                ({"own_positions": None}, "NOT_APPLICABLE"),
            ):
                delivered.clear()
                supervisor.state.feed_state = feed
                supervisor._delivered_signal_ids.clear()
                executor.values = values
                supervisor._dispatch_signal(
                    SimpleNamespace(
                        signal_id=f"blocked-{feed}",
                        detected_at=fresh_at,
                        values=fresh_values,
                    )
                )
                self.assertEqual(delivered, [])

            delivered.clear()
            supervisor.state.feed_state = "NOT_APPLICABLE"
            supervisor._delivered_signal_ids.clear()
            stale_at = fresh_at - timedelta(minutes=5)
            supervisor._dispatch_signal(
                SimpleNamespace(
                    signal_id="expired",
                    detected_at=stale_at,
                    values={"available_at": stale_at, "connection_generation": 1},
                )
            )
            self.assertEqual(delivered, [])
            supervisor._delivered_signal_ids.clear()
            supervisor._dispatch_signal(
                SimpleNamespace(
                    signal_id="future",
                    detected_at=fresh_at,
                    values={"available_at": fresh_at + timedelta(seconds=10), "connection_generation": 1},
                )
            )
            self.assertEqual(delivered, [])

    def test_single_writer_lock_is_global_for_same_account_namespace(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            first = SupervisorStateStore(root / "one", "account", lock_root=root / "locks")
            second = SupervisorStateStore(root / "two", "account", lock_root=root / "locks")
            with first.lock(), self.assertRaises(SingleWriterBusy), second.lock():
                pass

    def test_network_lock_identity_ignores_strategy_and_instrument_variants(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            provenance = {
                "source_mode": "DEMO_OBSERVED",
                "synthetic": False,
                "environment": "DEMO",
                "network_performed": True,
                "account_id": 7,
                "account_selected": True,
                "account_verified": True,
                "endpoint": "demo.ctraderapi.com:5035",
            }
            first_context = SupervisorContext(
                _Provider(),
                _config(),
                SQLiteStore(root / "one.sqlite3"),
                provenance=provenance,
            )
            self.addCleanup(first_context.store.close)
            second_config = replace(_config(), instrument="GBP/USD")
            second_context = SupervisorContext(
                _Provider(),
                second_config,
                SQLiteStore(root / "two.sqlite3"),
                provenance=provenance,
            )
            self.addCleanup(second_context.store.close)
            first = CTraderSupervisor(
                first_context,
                SupervisorOptions(mode="observe", network=True, state_dir=root / "one"),
            )
            second = CTraderSupervisor(
                second_context,
                SupervisorOptions(mode="observe", network=True, state_dir=root / "two"),
            )
            self.assertEqual(first.state_store.account_key, second.state_store.account_key)
            with self.assertRaises((ValueError, RuntimeError)):
                CTraderSupervisor(
                    first_context,
                    SupervisorOptions(
                        mode="observe",
                        network=True,
                        account_key="unrelated-account",
                        state_dir=root / "three",
                    ),
                )

    def test_state_store_rejects_symlink_and_writes_private_atomic_state(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            store = SupervisorStateStore(root / "state", "account", lock_root=root / "locks")
            state = PersistentSupervisorState(account_key="account", source="SYNTHETIC_FIXTURE")
            with store.lock():
                store.save(state)
            self.assertEqual(store.state_path.stat().st_mode & 0o777, 0o600)
            loaded = store.load(mode=SupervisorMode.OBSERVE, instrument="EUR/USD")
            self.assertIsNotNone(loaded)
            state_path = root / "evil.json"
            state_path.symlink_to(store.state_path)
            unsafe_root = root / "evil"
            unsafe_root.mkdir()
            unsafe = SupervisorStateStore(unsafe_root, "evil", lock_root=root / "locks")
            unsafe.state_path.symlink_to(store.state_path)
            with self.assertRaises(StateCorrupt):
                unsafe.load(mode=SupervisorMode.OBSERVE, instrument="EUR/USD")

    def test_watchdog_sends_only_for_new_useful_progress(self) -> None:
        sent: list[bytes] = []
        values = iter((0.0, 0.1, 2.0))
        watchdog = WatchdogNotifier(
            "/tmp/controlled-notify.sock",
            enabled=True,
            interval_seconds=1.0,
            monotonic=lambda: next(values),
            sender=lambda _path, payload: sent.append(payload),
        )
        self.assertTrue(watchdog.notify_progress(1, "OBSERVING"))
        self.assertFalse(watchdog.notify_progress(1, "OBSERVING"))
        self.assertTrue(watchdog.notify_progress(2, "OBSERVING"))
        self.assertEqual(len(sent), 2)
        self.assertIn(b"WATCHDOG=1", sent[0])

    def test_dbus_alerts_require_opt_in_sender(self) -> None:
        with self.assertRaises(ValueError):
            DbusAlertSink(enabled=True)
        calls: list[tuple[str, dict[str, Any]]] = []
        sink = DbusAlertSink(lambda event, details: calls.append((event, dict(details))), enabled=True)
        sink.alert("test", {"safe": True})
        self.assertEqual(calls, [("test", {"safe": True})])

    def test_market_closed_and_stale_are_distinct(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            provider = _Provider()
            state = {"value": "OPEN"}
            live_context = SupervisorContext(
                provider,
                _config(),
                SQLiteStore(root / "live.sqlite3"),
                provenance={
                    "source_mode": "DEMO_OBSERVED",
                    "synthetic": False,
                    "environment": "DEMO",
                    "network_performed": True,
                    "account_id": 7,
                    "account_selected": True,
                    "account_verified": True,
                    "endpoint": "demo.ctraderapi.com:5035",
                },
                market_state=lambda _now: state["value"],
            )
            self.addCleanup(live_context.store.close)
            supervisor = CTraderSupervisor(
                live_context,
                SupervisorOptions(mode="observe", fixture=False, network=True, state_dir=root / "state"),
            )
            supervisor.state.last_market_at = (BASE - timedelta(minutes=10)).isoformat()
            supervisor._refresh_market(BASE)
            self.assertEqual(supervisor.state.feed_state, "STALE")
            state["value"] = "CLOSED_SCHEDULED"
            supervisor._refresh_market(BASE)
            self.assertEqual(supervisor.state.feed_state, "CLOSED_MARKET")

    def test_reconnect_is_bounded_and_reconciles_before_summary(self) -> None:
        calls: list[str] = []
        provider = _Provider("DISCONNECTED")
        result = _watch_result()

        def reconcile() -> dict[str, Any]:
            calls.append("reconcile")
            return {"ok": True, "state": "VALID"}

        with TemporaryDirectory() as name:
            root = Path(name)
            config = _config()
            context = SupervisorContext(
                provider,
                config,
                SQLiteStore(root / "runtime.sqlite3"),
                provenance={
                    "provider": "prepared-demo",
                    "source_mode": "DEMO_OBSERVED",
                    "synthetic": False,
                    "environment": "DEMO",
                    "network_performed": True,
                    "account_id": 7,
                    "account_selected": True,
                    "account_verified": True,
                    "endpoint": "demo.ctraderapi.com:5035",
                },
                callbacks=ExecutionCallbacks(reconcile=reconcile),
                watch_runner_factory=lambda _ctx, _options: _ResultRunner(result),
                market_state=lambda _now: "OPEN",
            )
            self.addCleanup(context.store.close)
            supervisor = CTraderSupervisor(
                context,
                SupervisorOptions(
                    mode="observe",
                    network=True,
                    state_dir=root / "state",
                    max_events=1,
                    max_reconnect_attempts=2,
                ),
            )
            summary = supervisor.run()
            self.assertTrue(summary.ok, summary.to_dict())
            self.assertEqual(summary.reconnects, 1)
            self.assertGreaterEqual(summary.reconciliations, 2)
            self.assertTrue(summary.reconciled_before_summary)
            self.assertGreaterEqual(len(calls), 2)

    def test_failed_watch_latches_until_human_clear(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            provider = SimpleNamespace(
                connection="CONNECTED",
                generation=1,
                status={"connection": "CONNECTED"},
                close=lambda: None,
            )
            context = self._context(
                root,
                provider,
                runner=lambda _ctx, _options: _ResultRunner(failure=RuntimeError("feed failed")),
            )
            supervisor = CTraderSupervisor(
                context,
                SupervisorOptions(mode="observe", fixture=True, state_dir=root / "state", max_events=1),
            )
            result = supervisor.run()
            self.assertFalse(result.ok)
            self.assertTrue(result.fatal_latched)
            with SQLiteStore(root / "runtime.sqlite3"):
                pass
            supervisor.clear_latch()
            self.assertFalse(supervisor.state.fatal_latched)

    def test_failure_state_and_alerts_redact_exception_bodies(self) -> None:
        class Sink:
            def __init__(self) -> None:
                self.records: list[dict[str, Any]] = []

            def alert(self, event: str, details: Mapping[str, Any]) -> None:
                self.records.append({"event": event, **dict(details)})

        with TemporaryDirectory() as name:
            root = Path(name)
            secret_error = RuntimeError("access_token=fixture-secret https://example.invalid/?code=secret")
            sink = Sink()
            context = self._context(
                root,
                SimpleNamespace(status={"connection": "CONNECTED"}, generation=1, close=lambda: None),
                runner=lambda _ctx, _options: _ResultRunner(failure=secret_error),
            )
            supervisor = CTraderSupervisor(
                context,
                SupervisorOptions(mode="observe", fixture=True, state_dir=root / "state", max_events=1),
                alert_sink=sink,
            )
            result = supervisor.run()
            serialized = json.dumps(result.to_dict(), ensure_ascii=False)
            serialized += json.dumps(sink.records, ensure_ascii=False)
            self.assertNotIn("fixture-secret", serialized)
            self.assertNotIn("example.invalid", serialized)

    def test_canonical_readiness_reader_does_not_treat_stopped_json_as_live(self) -> None:
        with TemporaryDirectory() as name:
            root = Path(name)
            store = SupervisorStateStore(root / "state", "account", lock_root=root / "locks")
            state = PersistentSupervisorState(
                account_key="account",
                mode="observe",
                source="SYNTHETIC_FIXTURE",
                lifecycle="STOPPED",
                freshness_state="NOT_APPLICABLE",
                reconciliation_state="NOT_APPLICABLE",
                runtime_identity={
                    "pid": __import__("os").getpid(),
                    "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                    "process_start_ticks": int(
                        Path(f"/proc/{__import__('os').getpid()}/stat").read_text().rpartition(")")[2].split()[19]
                    ),
                },
                published_at=datetime.now(UTC).isoformat(),
            )
            with store.lock():
                store.save(state)
            readiness = read_supervisor_readiness(store.state_path)
            self.assertIsInstance(readiness, ReadinessStatus)
            self.assertFalse(readiness.ready)
            self.assertIn("PROCESS_NOT_RUNNING", readiness.reasons)


if __name__ == "__main__":
    unittest.main()
