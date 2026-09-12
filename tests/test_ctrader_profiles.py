from __future__ import annotations

import unittest
from pathlib import Path

from mtf_lab.configuration import load_config


class CTraderProfileTests(unittest.TestCase):
    def test_profiles_are_strict_and_fail_closed(self) -> None:
        fixture = load_config(Path(__file__).parents[1] / "config" / "fixture_cfd.toml")
        query = load_config(Path(__file__).parents[1] / "config" / "ctrader_query.toml")
        demo = load_config(Path(__file__).parents[1] / "config" / "ctrader_demo.toml")
        self.assertEqual(fixture.mode, "REPLAY")
        self.assertEqual(query.ctrader["required_scopes"], ["accounts"])
        self.assertFalse(demo.execution["enabled"])
        self.assertEqual(demo.ctrader["environment"], "DEMO")


if __name__ == "__main__":
    unittest.main()
