"""Frozen economic policy opt-in must not expand legacy activation gates."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, cast

from mtf_lab.configuration import ConfigError, EffectiveConfig, load_config, packaged_config_path
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.runtime import RuntimeCoordinator


class MarketConfigTests(unittest.TestCase):
    def _config(self, appendix: str) -> EffectiveConfig:
        source = packaged_config_path("ctrader_demo.toml").read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as name:
            target = Path(name) / "config.toml"
            target.write_text(source + "\n" + appendix, encoding="utf-8")
            return load_config(target)

    def _donchian_m5_config(self, *, timeframes: str = 'base = "5m"\nvalues = ["5m"]') -> EffectiveConfig:
        """Build a candidate from the checked-in cTrader TOML, not a toy dict."""
        source = packaged_config_path("ctrader_demo.toml").read_text(encoding="utf-8")
        source = source.replace('base = "1m"\nvalues = ["1m", "5m", "15m"]', timeframes, 1)
        source = source.replace('name = "trend_pullback_v1"', 'name = "dc_m5_v1"', 1)
        for line in (
            'context_timeframe = "15m"\n',
            'setup_timeframe = "5m"\n',
            'trigger_timeframe = "1m"\n',
        ):
            source = source.replace(line, "", 1)
        source += '\nmarket_candidate_id = "dc_m5_v1"\n'
        with tempfile.TemporaryDirectory() as name:
            target = Path(name) / "dc_m5.toml"
            target.write_text(source, encoding="utf-8")
            return load_config(target)

    def test_legacy_config_does_not_opt_in(self) -> None:
        config = self._config("")
        self.assertNotIn("market_candidate_id", config.execution)
        self.assertNotIn("risk_exit_policy", config.execution)
        self.assertIs(config.execution["enabled"], False)

    def test_frozen_profile_remains_disabled(self) -> None:
        config = self._config('market_candidate_id = "tp_fast_v1"')
        policy = config.execution["risk_exit_policy"]
        self.assertEqual(policy["planned_risk_fraction"], "0.0025")
        self.assertEqual(policy["intraday_max_minutes"], "30")
        self.assertIs(config.execution["enabled"], False)
        self.assertIs(config.ctrader["account_selected"], False)

    def test_unknown_candidate_and_unbound_policy_are_rejected(self) -> None:
        for appendix in ('market_candidate_id = "optimizer_best"', "[execution.risk_exit_policy]\nmax_positions = 1"):
            with self.subTest(appendix=appendix), self.assertRaises(ConfigError):
                self._config(appendix)

    def test_risk_expansion_and_parameter_changes_are_rejected(self) -> None:
        for field in (
            'planned_risk_fraction = "0.01"',
            'max_daily_loss_fraction = "0.02"',
            'max_drawdown_fraction = "0.1"',
            "max_positions = 2",
            "intraday_max_bars = 50",
            'intraday_max_minutes = "75"',
            'stop_atr_multiple = "2"',
            'holding_profile = "MULTIDAY"',
        ):
            with self.subTest(field=field), self.assertRaises(ConfigError):
                self._config('market_candidate_id = "tp_fast_v1"\n[execution.risk_exit_policy]\n' + field)

    def test_donchian_m5_loads_from_real_toml_with_one_timeframe(self) -> None:
        config = self._donchian_m5_config()

        self.assertEqual(config.strategy.name, "dc_m5_v1")
        self.assertEqual(tuple(tf.name for tf in config.timeframes), ("M5",))
        self.assertEqual(config.execution["market_candidate_id"], "dc_m5_v1")
        self.assertEqual(config.execution["risk_exit_policy"]["intraday_max_bars"], 5)
        self.assertFalse(config.execution["enabled"])
        # The legacy StrategyConfig remains only as an adapter for callers
        # that require its old triple; the market profile itself is the single
        # M5 timeframe above and is never inferred from the adapter.
        self.assertEqual(config.execution["environment"], "DEMO")

    def test_donchian_candidate_requires_its_registered_timeframe(self) -> None:
        with self.assertRaisesRegex(ConfigError, "market_candidate_id=dc_m5_v1"):
            self._donchian_m5_config(timeframes='base = "M15"\nvalues = ["M15"]')

    def test_unknown_or_modelled_calendar_cannot_enable_demo(self) -> None:
        source = packaged_config_path("ctrader_demo.toml").read_text(encoding="utf-8")
        source = source.replace(
            "[execution]\nenabled = false",
            "[execution]\nenabled = true",
            1,
        )
        for calendar in (
            'known = false\nbasis = "UNKNOWN_NOT_OBSERVED"',
            'known = true\nbasis = "MODELED_CURRENT_PUBLIC_PEPPERSTONE_NOT_HISTORICAL_ACCOUNT"',
        ):
            appendix = f'\nmarket_candidate_id = "tp_fast_v1"\n[execution.risk_calendar]\n{calendar}\n'
            with self.subTest(calendar=calendar), tempfile.TemporaryDirectory() as name:
                target = Path(name) / "unsafe.toml"
                target.write_text(source + appendix, encoding="utf-8")
                with self.assertRaisesRegex(ConfigError, "risk_calendar"):
                    load_config(target)

    def test_runtime_coordinator_uses_candidate_and_configured_single_timeframe(self) -> None:
        config = self._donchian_m5_config()

        class MemoryStore:
            def create_analysis(self, *_args: Any, **_kwargs: Any) -> str:
                return "analysis-memory"

            def get_checkpoint(self, *_args: Any, **_kwargs: Any) -> None:
                return None

        coordinator = RuntimeCoordinator(
            cast(SQLiteStore, MemoryStore()),
            "session-memory",
            config,
            mode="REPLAY",
            dataset_hash="fixture-memory",
            resume=False,
        )

        self.assertEqual(coordinator.processor.market_candidate_id, "dc_m5_v1")
        self.assertEqual(tuple(tf.name for tf in coordinator.processor.timeframes), ("M5",))


if __name__ == "__main__":
    unittest.main()
