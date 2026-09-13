"""Focal offline tests for the bounded cTrader observation runner."""

from __future__ import annotations

import threading
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest import mock

from mtf_lab.configuration import EffectiveConfig, load_config
from mtf_lab.data.ctrader import (
    CTraderClient,
    CTraderConfig,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
    synthetic_spot_event,
    synthetic_trendbar,
)
from mtf_lab.ops.ctrader_watch import (
    CTraderWatchContext,
    CTraderWatchError,
    CTraderWatchOptions,
    CTraderWatchResult,
    CTraderWatchRunner,
    WatchStopReason,
    run_ctrader_watch,
)
from mtf_lab.ops.persistence import SQLiteStore

BASE = datetime(2026, 1, 1, tzinfo=UTC)


class CTraderWatchTests(unittest.TestCase):
    def test_combined_checkpoint_rolls_back_if_watch_write_fails(self) -> None:
        provider, _transport = self._provider()
        try:
            with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "atomic.sqlite3") as store:
                runner = CTraderWatchRunner(self._context(provider, store), self._options())
                runner._prepare()
                before = store.get_checkpoint(
                    runner.coordinator.session_id,
                    "ctrader-watch",
                    analysis_id=runner.coordinator.analysis_id,
                    allow_alternate=False,
                )
                original = store.save_checkpoint
                calls = 0

                def fail_watch_write(*args: Any, **kwargs: Any) -> Any:
                    nonlocal calls
                    calls += 1
                    if calls == 2:
                        raise OSError("controlled checkpoint failure")
                    return original(*args, **kwargs)

                with (
                    mock.patch.object(store, "save_checkpoint", side_effect=fail_watch_write),
                    self.assertRaises(OSError),
                ):
                    runner._checkpoint()
                after = store.get_checkpoint(
                    runner.coordinator.session_id,
                    "ctrader-watch",
                    analysis_id=runner.coordinator.analysis_id,
                    allow_alternate=False,
                )
                self.assertEqual(after, before)
        finally:
            provider.close()

    def setUp(self) -> None:
        self.config = replace(load_config("config/fixture_cfd.toml"), mode="SYNTHETIC")
        self.provenance = {
            "source_mode": "SYNTHETIC_FIXTURE",
            "synthetic": True,
            "provider": "ctrader_open_api_fixture",
            "environment": "OFFLINE",
            "network_performed": False,
            "execution_enabled": False,
            "data_identity": "ctrader-watch-fixture-v1",
        }

    def _provider(
        self,
        *,
        when: datetime = BASE,
        initial_generation: int = 0,
    ) -> tuple[CTraderProvider, DeterministicTransport]:
        transport = DeterministicTransport()
        config = CTraderConfig(
            symbol="EUR/USD",
            symbol_id=99,
            account_id=7,
            quote_basis="mid",
            heartbeat_seconds=60.0,
        )
        client = CTraderClient(
            config,
            transport=transport,
            wall_clock=lambda: when,
            initial_generation=initial_generation,
        )
        client.connect()
        client.mark_authenticated(7)
        provider = CTraderProvider(config, client=client, clock=lambda: when)
        return provider, transport

    @staticmethod
    def _message(payload: dict[str, object]) -> WireMessage:
        return WireMessage("PROTO_OA_SPOT_EVENT", payload, None, True)

    def _full_payload(self, minute: int = 0, *, trendbar: bool = False) -> dict[str, object]:
        bars: tuple[dict[str, Any], ...] = ()
        if trendbar:
            bars = (
                synthetic_trendbar(
                    timestamp_minutes=int((BASE + timedelta(minutes=minute)).timestamp() // 60),
                    period="M1",
                ),
            )
        return synthetic_spot_event(
            timestamp_ms=int((BASE + timedelta(minutes=minute)).timestamp() * 1000),
            symbol_id=99,
            trendbars=bars,
        )

    def _context(
        self,
        provider: CTraderProvider,
        store: SQLiteStore,
        *,
        config: EffectiveConfig | None = None,
        session_id: str | None = None,
        provenance: dict[str, object] | None = None,
        stop_event: Any | None = None,
        monotonic: Any | None = None,
    ) -> CTraderWatchContext:
        return CTraderWatchContext(
            provider,
            config or self.config,
            store,
            provenance=provenance or self.provenance,
            session_id=session_id,
            stop_event=stop_event,
            clock=lambda: BASE,
            monotonic=monotonic or time.monotonic,
        )

    def _options(self, **changes: object) -> CTraderWatchOptions:
        values: dict[str, Any] = {
            "max_events": 1,
            "idle_timeout_seconds": 0.05,
            "poll_timeout_seconds": 0.01,
            "checkpoint_every": 1,
        }
        values.update(changes)
        return CTraderWatchOptions(**values)

    def test_protocol_fixture_uses_real_provider_normalization_and_metadata(self) -> None:
        provider, transport = self._provider()
        transport.push(self._message(self._full_payload(trendbar=True)))
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            result = run_ctrader_watch(self._context(provider, store), self._options())
            events = store.list_events(result.session_id)
            candles = store.list_candles(result.session_id)
            captures = store.list_capture_envelopes(result.session_id)

        self.assertEqual(result.stop_reason, WatchStopReason.MAX_EVENTS.value)
        self.assertEqual(result.messages, 1)
        # One protocol SpotEvent is atomic: the provider emits one quote and
        # one native trendbar, both through CTraderProvider.normalize_spot.
        self.assertEqual(result.events, 2)
        self.assertEqual(result.bars, 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(len(candles), 1)
        self.assertEqual(len(captures), 1)
        self.assertEqual(result.provenance["source_mode"], "SYNTHETIC_FIXTURE")
        self.assertTrue(result.provenance["synthetic"])
        self.assertFalse(result.provenance["network_performed"])
        self.assertFalse(result.provenance["execution_enabled"])
        self.assertEqual(events[0]["payload"]["quality"]["status"], "synthetic")
        self.assertTrue(events[0]["payload"]["metadata"]["synthetic"])
        self.assertEqual(events[0]["payload"]["metadata"]["source_mode"], "SYNTHETIC_FIXTURE")
        self.assertFalse(transport.connected)
        self.assertEqual(transport.sent, [])

    def test_max_events_is_atomic_message_limit_and_idle_is_bounded(self) -> None:
        provider, transport = self._provider()
        transport.push(self._message(self._full_payload(trendbar=True)))
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            result = run_ctrader_watch(self._context(provider, store), self._options(max_events=1))
            self.assertEqual(result.stop_reason, "MAX_EVENTS")
            self.assertEqual(result.messages, 1)
            self.assertEqual(result.events, 2)

            idle_provider, idle_transport = self._provider()
            idle = run_ctrader_watch(
                self._context(idle_provider, store),
                self._options(max_events=None, idle_timeout_seconds=0.03, poll_timeout_seconds=0.005),
            )
            self.assertEqual(idle.stop_reason, WatchStopReason.IDLE_TIMEOUT.value)
            self.assertEqual(idle.messages, 0)
            self.assertFalse(idle_transport.connected)

            stopped_provider, stopped_transport = self._provider()
            stop_event = threading.Event()
            stop_event.set()
            stopped = run_ctrader_watch(
                self._context(stopped_provider, store, stop_event=stop_event),
                self._options(max_events=None),
            )
            self.assertEqual(stopped.stop_reason, WatchStopReason.STOP_REQUESTED.value)
            self.assertFalse(stopped_transport.connected)

    def test_resume_invalidates_quote_state_and_does_not_skip_reset_source_sequence(self) -> None:
        first_provider, first_transport = self._provider()
        first_transport.push(self._message(self._full_payload(0)))
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            first = run_ctrader_watch(self._context(first_provider, store), self._options())

            second_provider, second_transport = self._provider(when=BASE + timedelta(minutes=1, seconds=1))
            # New process: its WireMessage ingest counter starts at zero again.
            # The runner must not use that reset value as a replay filter.
            second_transport.push(self._message(self._full_payload(1)))
            second_provenance = {**self.provenance, "continuity_verified": True}
            resumed = run_ctrader_watch(
                self._context(
                    second_provider,
                    store,
                    session_id=first.session_id,
                    provenance=second_provenance,
                ),
                self._options(max_events=1),
            )
            captures = store.list_capture_envelopes(first.session_id)

        self.assertTrue(resumed.resumed)
        self.assertEqual(resumed.events, 2)
        self.assertEqual(resumed.messages_this_run, 1)
        self.assertEqual(len(captures), 2)
        self.assertEqual(resumed.status["continuity_state"], "RECOVERED_BOUNDED")

    def test_resume_does_not_reuse_one_sided_quote_from_previous_process(self) -> None:
        first_provider, first_transport = self._provider()
        first_transport.push(self._message(self._full_payload(0)))
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            first = run_ctrader_watch(self._context(first_provider, store), self._options())
            second_provider, second_transport = self._provider()
            second_transport.push(
                self._message(
                    {
                        "symbolId": 99,
                        "timestamp": int((BASE + timedelta(minutes=1)).timestamp() * 1000),
                        "timestamp_unit": "ms",
                        "ask": 110020,
                        "synthetic_fixture": True,
                    }
                )
            )
            resumed = run_ctrader_watch(
                self._context(second_provider, store, session_id=first.session_id),
                self._options(max_events=1),
            )

        self.assertTrue(resumed.resumed)
        # If the previous bid/ask book had been restored, the one-sided ask
        # would have produced a quote.  A new process must observe both legs.
        self.assertEqual(resumed.events, first.events)
        self.assertEqual(resumed.status["continuity_state"], "BROKEN")

    def test_resume_rejects_alternate_checkpoint_after_config_change(self) -> None:
        first_provider, first_transport = self._provider()
        first_transport.push(self._message(self._full_payload(0)))
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            first = run_ctrader_watch(self._context(first_provider, store), self._options())
            second_provider, _second_transport = self._provider(when=BASE + timedelta(seconds=2))
            changed_config = replace(self.config, price_base="bid")
            with self.assertRaises(CTraderWatchError):
                run_ctrader_watch(
                    self._context(
                        second_provider,
                        store,
                        config=changed_config,
                        session_id=first.session_id,
                    ),
                    self._options(max_events=1),
                )

    def test_resume_existing_session_without_exact_checkpoint_fails_closed(self) -> None:
        provider, transport = self._provider()
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            session_id = store.create_session(
                mode="SYNTHETIC",
                provider="ctrader_open_api_fixture",
                instrument=self.config.instrument,
                config=self.config.to_dict(),
            )
            with self.assertRaises(CTraderWatchError):
                run_ctrader_watch(
                    self._context(provider, store, session_id=session_id),
                    self._options(),
                )
            self.assertEqual(store.analyses(session_id), [])
        self.assertFalse(transport.connected)

    def test_resume_rejects_capture_rows_after_checkpoint_without_truncating(self) -> None:
        first_provider, first_transport = self._provider()
        first_transport.push(self._message(self._full_payload(0)))
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            first = run_ctrader_watch(self._context(first_provider, store), self._options())
            extra = self._message(self._full_payload(1)).with_capture_metadata(
                received_at=BASE + timedelta(minutes=1, seconds=1),
                available_at=BASE + timedelta(minutes=1, seconds=1),
                ingest_sequence=999,
                connection_generation=1,
            )
            store.save_capture_envelope(first.session_id, extra.capture_envelope())
            captures_before = len(store.list_capture_envelopes(first.session_id))
            second_provider, second_transport = self._provider(when=BASE + timedelta(minutes=1, seconds=1))
            with self.assertRaises(CTraderWatchError):
                run_ctrader_watch(
                    self._context(second_provider, store, session_id=first.session_id),
                    self._options(),
                )
            self.assertEqual(len(store.list_capture_envelopes(first.session_id)), captures_before)
        self.assertFalse(second_transport.connected)

    def test_message_arriving_after_duration_or_idle_limit_is_not_processed(self) -> None:
        class SequenceClock:
            def __init__(self) -> None:
                self.values = iter((0.0, 0.0, 0.0, 2.0, 2.0))
                self.last = 2.0

            def __call__(self) -> float:
                self.last = next(self.values, self.last)
                return self.last

        for duration, idle, expected in ((1.0, None, "DURATION"), (None, 1.0, "IDLE_TIMEOUT")):
            provider, transport = self._provider()
            transport.push(self._message(self._full_payload(0)))
            clock = SequenceClock()
            with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
                result = run_ctrader_watch(
                    self._context(provider, store, monotonic=clock),
                    self._options(
                        max_events=1,
                        duration_seconds=duration,
                        idle_timeout_seconds=idle,
                    ),
                )
                self.assertEqual(result.stop_reason, expected)
                self.assertEqual(result.messages_this_run, 0)
                self.assertEqual(store.list_events(result.session_id), [])
            self.assertFalse(transport.connected)

    def test_resume_accepts_lower_first_generation_as_blocked_boundary(self) -> None:
        first_provider, first_transport = self._provider(initial_generation=1)
        first_transport.push(self._message(self._full_payload(0)))
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            first = run_ctrader_watch(self._context(first_provider, store), self._options())
            second_provider, second_transport = self._provider(
                when=BASE + timedelta(minutes=1, seconds=1),
                initial_generation=0,
            )
            second_transport.push(self._message(self._full_payload(1)))
            resumed = run_ctrader_watch(
                self._context(
                    second_provider,
                    store,
                    session_id=first.session_id,
                    provenance={**self.provenance, "continuity_verified": True},
                ),
                self._options(max_events=1),
            )

        self.assertEqual(resumed.generation_changes, 1)
        self.assertEqual(resumed.events, 2)
        self.assertEqual(resumed.status["continuity_state"], "RECOVERED_BOUNDED")

    def test_recovery_proof_is_consumed_and_invalidated_by_control_disconnect(self) -> None:
        provider, transport = self._provider(when=BASE + timedelta(minutes=1, seconds=2))
        transport.push(self._message(self._full_payload(0)))
        transport.push(
            WireMessage(
                "PROTO_OA_ACCOUNT_DISCONNECT_EVENT",
                {"ctidTraderAccountId": 999},
                None,
                True,
            )
        )
        transport.push(self._message(self._full_payload(1)))
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            result = run_ctrader_watch(
                self._context(
                    provider,
                    store,
                    provenance={**self.provenance, "continuity_verified": True},
                ),
                self._options(max_events=3),
            )

        self.assertEqual(result.messages_this_run, 3)
        self.assertEqual(result.status["continuity_state"], "BROKEN")

    def test_stop_after_poll_does_not_process_returned_message(self) -> None:
        provider, transport = self._provider()
        transport.push(self._message(self._full_payload(0)))

        class StopAfterPoll:
            checks = 0

            def is_set(self) -> bool:
                self.checks += 1
                return self.checks >= 2

        stop = StopAfterPoll()
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            result = run_ctrader_watch(
                self._context(provider, store, stop_event=stop),
                self._options(max_events=1),
            )
            self.assertEqual(store.list_events(result.session_id), [])
        self.assertEqual(result.stop_reason, "STOP_REQUESTED")
        self.assertEqual(result.messages_this_run, 0)

    def test_constructor_validation_closes_provider_and_rejects_string_boolean(self) -> None:
        provider, transport = self._provider()
        provenance = {**self.provenance, "continuity_verified": "false"}
        with (
            TemporaryDirectory() as directory,
            SQLiteStore(Path(directory) / "watch.sqlite3") as store,
            self.assertRaises(CTraderWatchError),
        ):
            run_ctrader_watch(self._context(provider, store, provenance=provenance), self._options())
        self.assertFalse(transport.connected)

    def test_new_live_session_has_no_reconciliation_debt_without_checkpoint(self) -> None:
        provider, transport = self._provider()
        live_config = replace(self.config, mode="LIVE")
        provenance = {
            "source_mode": "DEMO_OBSERVED",
            "synthetic": False,
            "provider": "ctrader_open_api",
            "environment": "DEMO",
            "network_performed": True,
            "execution_enabled": False,
            "data_identity": "demo-v1",
        }
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            result = run_ctrader_watch(
                self._context(provider, store, config=live_config, provenance=provenance),
                self._options(max_events=None, idle_timeout_seconds=0.03, poll_timeout_seconds=0.005),
            )
        self.assertEqual(result.status["reconciliation_state"], "NOT_APPLICABLE")
        self.assertFalse(transport.connected)

    def test_cleanup_failure_is_not_reported_as_clean_stop(self) -> None:
        provider, transport = self._provider()
        transport.push(self._message(self._full_payload(0)))
        original_close = provider.close

        def failing_close() -> None:
            original_close()
            raise RuntimeError("close fixture failure")

        object.__setattr__(provider, "close", failing_close)
        with (
            TemporaryDirectory() as directory,
            SQLiteStore(Path(directory) / "watch.sqlite3") as store,
            self.assertRaises(CTraderWatchError),
        ):
            run_ctrader_watch(self._context(provider, store), self._options())
        self.assertFalse(transport.connected)

    def test_fixture_semantic_identity_ignores_session_uuid_and_execution_is_never_enabled(self) -> None:
        results: list[CTraderWatchResult] = []
        for index in range(2):
            provider, transport = self._provider(when=BASE + timedelta(seconds=index * 10))
            transport.push(self._message(self._full_payload(0)))
            with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
                result = run_ctrader_watch(self._context(provider, store), self._options())
                results.append(result)
        first, second = results
        self.assertNotEqual(first.session_id, second.session_id)
        self.assertEqual(first.semantic_identity, second.semantic_identity)
        self.assertEqual(first.data_semantic_hash, second.data_semantic_hash)
        self.assertEqual(first.result_semantic_hash, second.result_semantic_hash)
        self.assertFalse(first.provenance["execution_enabled"])


if __name__ == "__main__":
    unittest.main()
