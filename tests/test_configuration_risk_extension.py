from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from mtf_lab.configuration import ConfigError, load_config, packaged_config_path
from mtf_lab.core.cfd_simulation import CFD_ECONOMICS_LEGACY_VERSION, CFDConfig


class ConfigurationRiskExtensionTests(unittest.TestCase):
    def test_operational_limits_are_loadable_but_do_not_enable_execution(self) -> None:
        source = packaged_config_path("ctrader_pipeline_fixture.toml").read_text()
        extension = """
[execution]
enabled = false
environment = "DEMO"
max_exposure = "100"
max_positions = 1
max_inflight_intents = 1
max_daily_loss = "20"
max_drawdown = "30"
min_margin_level = "500"
require_protective_stops = true
relative_stop_loss = 100
relative_take_profit = 200
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.toml"
            path.write_text(source + extension)
            config = load_config(path)
            self.assertFalse(config.execution["enabled"])
            self.assertEqual(config.execution["max_daily_loss"], "20")
            path.write_text(source + extension + "made_up_risk_gate = true\n")
            with self.assertRaises(ConfigError):
                load_config(path)

    def test_explicit_legacy_economics_configuration_is_not_reinterpreted(self) -> None:
        source = packaged_config_path("ctrader_pipeline_fixture.toml").read_text()
        source = source.replace("[cfd]", '[cfd]\neconomics_version = "cfd-economics-v1"')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.toml"
            path.write_text(source)
            config = load_config(path)
            self.assertEqual(CFDConfig.from_mapping(config.cfd).economics_version, CFD_ECONOMICS_LEGACY_VERSION)
