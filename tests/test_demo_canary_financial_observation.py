"""Unit-scope checks for the network canary's financial-observation seam.

No broker, credentials, user state, SQLite, or real network is used here.  The
test replaces the canonical session, account observer, economics projection,
input collector, and runner with typed local fakes.  It checks composition only:
the observer made by the binding must be the one used after ``observe_and_arm``.
"""

from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

from mtf_lab.configuration import EffectiveConfig, load_config
from mtf_lab.ops.ctrader_canary_inputs import CanarySessionEvidence
from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor
from tools.demo_canary import CanaryApproval, network_cli_preflight

NOW = datetime.now(UTC).replace(microsecond=0)


@contextmanager
def _isolated_environment(root: Path) -> Any:
    home = root / "home"
    xdg = root / "xdg"
    tmp = root / "tmp"
    for path in (home, xdg, tmp):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    with patch.dict(
        os.environ,
        {"HOME": str(home), "XDG_STATE_HOME": str(xdg), "TMPDIR": str(tmp)},
        clear=False,
    ):
        yield


@dataclasses.dataclass(frozen=True, slots=True)
class _Snapshot:
    name: str
    fresh: bool = True
    complete: bool = True
    account_complete: bool = True
    reasons: tuple[str, ...] = ()


@dataclasses.dataclass(frozen=True, slots=True)
class _Projection:
    snapshot_name: str
    calendar_state: Mapping[str, Any] = dataclasses.field(
        default_factory=lambda: {"known": True, "basis": "OBSERVED_BROKER"}
    )


class _Observer:
    def __init__(self, snapshots: tuple[_Snapshot, ...]) -> None:
        self.snapshots = snapshots
        self.observe_calls = 0
        self.update_calls = 0
        self.last_observation: _Snapshot | None = None

    def observe(self) -> _Snapshot:
        snapshot = self.snapshots[0]
        self.observe_calls += 1
        self.last_observation = snapshot
        return snapshot

    def update_executor(self, _executor: CTraderDemoExecutor) -> Mapping[str, Any]:
        index = min(self.update_calls + 1, len(self.snapshots) - 1)
        snapshot = self.snapshots[index]
        self.update_calls += 1
        self.last_observation = snapshot
        return {"state": "READY", "snapshot_name": snapshot.name}


class _Provider:
    def __init__(self) -> None:
        self.stream_calls = 0

    def stream(self, **kwargs: Any) -> tuple[object, ...]:
        self.stream_calls += 1
        if kwargs["max_events"] <= 0:
            raise AssertionError("the bounded reader must receive a positive event limit")
        return ()


class _Executor:
    def observe_runtime(self, snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
        return snapshot


class _WriterStore:
    """Minimal account-lock seam for the composition-only fake session."""

    root = Path(".")

    @contextmanager
    def lock(self) -> Any:
        yield


@dataclasses.dataclass
class _Binding:
    executor: _Executor
    risk_observer: _Observer | None
    canary_economics: _Projection | None = None


@dataclasses.dataclass
class _Session:
    provider: _Provider
    config: EffectiveConfig
    provenance: Mapping[str, Any]
    binding: _Binding | None
    binding_observer: _Observer | None
    writer_store: _WriterStore = dataclasses.field(default_factory=_WriterStore)
    closed: bool = False
    projections: list[_Projection] = dataclasses.field(default_factory=list)

    def observe_and_arm(
        self,
        projection_factory: Callable[[], _Projection],
        *,
        config_factory: Callable[[_Projection], EffectiveConfig] | None = None,
    ) -> _Projection:
        projection = projection_factory()
        self.projections.append(projection)
        if config_factory is not None:
            self.config = config_factory(projection)
        self.binding = _Binding(_Executor(), self.binding_observer)
        return projection

    def close(self) -> None:
        self.closed = True


@dataclasses.dataclass(frozen=True, slots=True)
class _InputResult:
    ok: bool
    context: object | None
    gates: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    reason: str | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class _RunnerResult:
    payload: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)


class DemoCanaryFinancialObservationTests(unittest.TestCase):
    def _approval(self) -> CanaryApproval:
        return CanaryApproval(
            approved=True,
            approval_id="financial-observation-test",
            account_id="5097",
            symbol="EUR/USD",
            window_start=NOW - timedelta(minutes=1),
            window_end=NOW + timedelta(minutes=20),
            max_holding_seconds=Decimal("300"),
            max_quantity=Decimal("1000"),
            max_risk_fraction=Decimal("0.0005"),
            max_mutation_messages=6,
        )

    def _config(self) -> EffectiveConfig:
        return load_config("config/fixture_cfd.toml")

    def _session(self, observer: _Observer | None) -> _Session:
        config = self._config()
        return _Session(
            provider=_Provider(),
            config=config,
            provenance={
                "network_performed": True,
                "source_mode": "DEMO_OBSERVED",
                "synthetic": False,
                "environment": "DEMO",
                "account_id": "5097",
                "account_selected": True,
                "account_verified": True,
                "endpoint": "demo.ctraderapi.com:5035",
            },
            binding=None,
            binding_observer=observer,
        )

    def test_binding_observer_drives_financial_callbacks_and_close_skips_quote_poll(self) -> None:
        initial = _Observer((_Snapshot("T0"),))
        bound = _Observer((_Snapshot("T0"), _Snapshot("T1"), _Snapshot("T2"), _Snapshot("T3")))
        session = self._session(bound)
        collector_kwargs: dict[str, Any] = {}
        projection_snapshots: list[str] = []
        callback_snapshots: dict[str, str] = {}
        quote_polls_before_close: list[int] = []

        def observe_economics(_provider: _Provider, snapshot: _Snapshot, **_kwargs: Any) -> _Projection:
            projection_snapshots.append(snapshot.name)
            return _Projection(snapshot.name)

        def collect(_provider: _Provider, config: EffectiveConfig, **kwargs: Any) -> _InputResult:
            self.assertIs(config, session.config)
            collector_kwargs.update(kwargs)
            return _InputResult(ok=True, context=object())

        def run_runner(_session: _Session, **kwargs: Any) -> _RunnerResult:
            executor = session.binding.executor if session.binding is not None else None
            assert executor is not None
            risk = kwargs["risk_refresh"](executor)
            callback_snapshots["risk"] = str(risk["snapshot_name"])
            economics = kwargs["economics_refresh"](executor, risk)
            callback_snapshots["economics"] = economics.snapshot_name
            before_close = session.provider.stream_calls
            post_close = kwargs["post_close_risk_refresh"](executor)
            quote_polls_before_close.append(before_close)
            callback_snapshots["post_close"] = str(post_close["snapshot_name"])
            self.assertEqual(session.provider.stream_calls, before_close)
            return _RunnerResult({"ok": True, "state": "CANARY_COMPLETED"})

        with (
            tempfile.TemporaryDirectory(prefix="mtf-financial-observation-") as directory,
            _isolated_environment(Path(directory)),
        ):
            root = Path(directory)
            journal = root / "journal.jsonl"
            with (
                patch("tools.demo_canary.prepare_cli_session", return_value=session),
                patch(
                    "mtf_lab.ops.ctrader_account_risk.AccountRiskObserver",
                    return_value=initial,
                ),
                patch.object(CanarySessionEvidence, "from_provider", return_value=object()),
                patch(
                    "mtf_lab.ops.ctrader_canary_economics.observe_canary_economics",
                    side_effect=observe_economics,
                ),
                patch("mtf_lab.ops.supervision_demo._journal_path", return_value=journal),
                patch("tools.demo_canary.validate_static_config", return_value={}),
                patch(
                    "mtf_lab.ops.supervision_demo._update_executor_from_canary_economics",
                    return_value=None,
                ),
                patch(
                    "mtf_lab.ops.ctrader_canary_inputs.collect_canary_inputs",
                    side_effect=collect,
                ),
                patch("tools.demo_canary.run_prepared_canary", side_effect=run_runner),
            ):
                result = network_cli_preflight(
                    Path("config/fixture_cfd.toml"),
                    root / "state",
                    execute=True,
                    approval=self._approval(),
                    max_events=7,
                )

        self.assertTrue(result["ok"])
        self.assertEqual(result["state"], "CANARY_COMPLETED")
        self.assertTrue(session.closed)
        self.assertEqual(initial.observe_calls, 1)
        self.assertEqual(initial.update_calls, 0)
        self.assertEqual(bound.update_calls, 3)
        self.assertEqual(projection_snapshots, ["T0", "T1", "T2"])
        self.assertEqual(callback_snapshots, {"risk": "T2", "economics": "T2", "post_close": "T3"})
        self.assertEqual(quote_polls_before_close, [3])
        self.assertEqual(session.provider.stream_calls, 3)
        self.assertEqual(
            tuple(collector_kwargs),
            (
                "network",
                "deadline",
                "max_events",
                "window_start",
                "window_end",
                "preparation_start",
                "session_evidence",
                "runtime_observer",
                "executor",
                "risk_planner",
                "requested_quantity",
                "fetch_warmup",
                "technical_only",
                "zero_spread_authorization",
                "minimum_execution_margin_seconds",
                "market_window_state",
                "isolated_state_dir",
                "close_provider",
            ),
        )
        self.assertTrue(collector_kwargs["technical_only"])
        self.assertIsNone(collector_kwargs["zero_spread_authorization"])
        self.assertFalse(collector_kwargs["fetch_warmup"])
        self.assertIsNone(collector_kwargs["executor"])
        self.assertIsNone(collector_kwargs["risk_planner"])
        self.assertEqual(collector_kwargs["minimum_execution_margin_seconds"], Decimal("300"))

    def test_missing_binding_observer_fails_closed_before_collector(self) -> None:
        initial = _Observer((_Snapshot("T0"),))
        session = self._session(None)
        collector_calls = 0
        with (
            tempfile.TemporaryDirectory(prefix="mtf-financial-observation-") as directory,
            _isolated_environment(Path(directory)),
        ):
            root = Path(directory)

            def collect(*_args: Any, **_kwargs: Any) -> _InputResult:
                nonlocal collector_calls
                collector_calls += 1
                return _InputResult(ok=True, context=object())

            with (
                patch("tools.demo_canary.prepare_cli_session", return_value=session),
                patch(
                    "mtf_lab.ops.ctrader_account_risk.AccountRiskObserver",
                    return_value=initial,
                ),
                patch.object(CanarySessionEvidence, "from_provider", return_value=object()),
                patch("tools.demo_canary.validate_static_config", return_value={}),
                patch("mtf_lab.ops.supervision_demo._journal_path", return_value=root / "journal.jsonl"),
                patch(
                    "mtf_lab.ops.ctrader_canary_economics.observe_canary_economics",
                    return_value=_Projection("T0"),
                ),
                patch(
                    "mtf_lab.ops.ctrader_canary_inputs.collect_canary_inputs",
                    side_effect=collect,
                ),
                patch("tools.demo_canary.run_prepared_canary") as runner,
            ):
                result = network_cli_preflight(
                    Path("config/fixture_cfd.toml"),
                    root / "state",
                    execute=True,
                    approval=self._approval(),
                    max_events=7,
                )

        self.assertFalse(result["ok"])
        self.assertEqual(result["state"], "RISK_OBSERVER_REQUIRED")
        self.assertEqual(collector_calls, 0)
        runner.assert_not_called()
        self.assertTrue(session.closed)


if __name__ == "__main__":
    unittest.main()
