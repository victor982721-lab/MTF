from __future__ import annotations

import io
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.ops.ctrader_activation import (
    ActivationError,
    SecureTokenStore,
    UnsafeTokenStore,
    open_authorization_browser,
)
from mtf_lab.ops.ctrader_oauth_http import OAuthHTTPError, _NoRedirect, build_token_request, request_token
from mtf_lab.ops.reporting import _aggregate
from mtf_lab.ops.simulation import EvaluationSpec, VirtualContractSimulator, quality_label_is_usable

NOW = datetime(2026, 1, 1, tzinfo=UTC)
TOKEN_URL = "https://openapi.ctrader.com/apps/token"
PARAMS = {"grant_type": "refresh_token", "refresh_token": "fixture", "client_id": "fixture", "client_secret": "fixture"}


class ReliabilityBoundariesTests(unittest.TestCase):
    def test_signal_availability_is_the_entry_floor(self) -> None:
        signal = {
            "signal_id": "late",
            "detected_ts": NOW,
            "available_at": NOW + timedelta(seconds=10),
            "direction": "UP",
        }
        points = [
            {
                "timestamp": NOW + timedelta(seconds=t),
                "price": price,
                "quality": "VALID",
                "price_base": "close",
                "closed": True,
            }
            for t, price in [(1, 100), (11, 110), (21, 111)]
        ]
        sim = VirtualContractSimulator(spec=EvaluationSpec(horizons_seconds=(10,), entry_latency_seconds=1))
        result = sim.evaluate(signal, points)
        self.assertEqual(result.entry_price, 110)
        self.assertEqual(result.assumptions["signal_available_ts"], "2026-01-01T00:00:10.000000Z")

    def test_structured_quality_cannot_hide_blocking_flags(self) -> None:
        self.assertFalse(quality_label_is_usable({"status": "VALID", "flags": ["STALE"]}))
        self.assertFalse(quality_label_is_usable("VALID_MADE_UP"))
        self.assertTrue(quality_label_is_usable("VALID"))

    def test_reporting_uses_settlement_and_gross_cost_bridge(self) -> None:
        rows = [
            {
                "detected_ts": "2026-01-01T00:00:00Z",
                "final_available_ts": "2026-01-01T00:03:00Z",
                "net_result": -2,
                "outcome": "LOSS",
            },
            {
                "detected_ts": "2026-01-01T00:00:01Z",
                "final_available_ts": "2026-01-01T00:01:00Z",
                "net_result": 1,
                "outcome": "WIN",
            },
            {
                "detected_ts": "2026-01-01T00:00:02Z",
                "final_available_ts": "2026-01-01T00:02:00Z",
                "net_result": 1,
                "outcome": "WIN",
            },
        ]
        self.assertEqual(_aggregate(rows)["max_drawdown"], 2)
        cost_row = {"net_result": -0.1, "outcome": "WIN", "assumptions": {"costs": 0.2}}
        self.assertAlmostEqual(_aggregate([cost_row])["gross_wins"], 0.1)
        self.assertEqual(_aggregate([cost_row])["net_losses"], -0.1)
        cost_row = {"net_result": -0.1, "outcome": "WIN", "assumptions_json": '{"costs":0.2}'}
        self.assertAlmostEqual(_aggregate([cost_row])["gross_wins"], 0.1)
        cost_row["final_available_ts"] = "2026-01-01T00:00:00"
        self.assertIsNone(_aggregate([cost_row])["max_drawdown"])

    def test_oauth_endpoints_redirects_and_body_are_bounded(self) -> None:
        with self.assertRaises(ActivationError):
            open_authorization_browser(
                "https://evil.invalid/?client_id=fixture",
                allow_browser=True,
                opener=lambda value: self.fail("unapproved browser endpoint called"),
            )
        for url in [
            "https://evil.invalid/apps/token",
            "https://openapi.ctrader.com.evil.invalid/apps/token",
            "https://openapi.ctrader.com:444/apps/token",
        ]:
            with self.assertRaises(OAuthHTTPError):
                build_token_request(url, PARAMS)
        request = build_token_request(TOKEN_URL, PARAMS)
        self.assertEqual(request.get_header("Cache-control"), "no-store")
        with self.assertRaises(OAuthHTTPError):
            _NoRedirect().redirect_request(request, None, 302, "move", {}, "https://evil.invalid")
        with self.assertRaises(OAuthHTTPError):
            request_token(TOKEN_URL, PARAMS, 1, opener=lambda *a, **k: io.BytesIO(b" " * (128 * 1024 + 1)))

    def test_rotations_are_serialized_and_stale_generation_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            store = SecureTokenStore(base / "tokens", project_root=base / "project")

            def rotate() -> int | None:
                try:
                    return store.rotate(
                        "test",
                        access_token="fixture",
                        refresh_token="fixture",
                        granted_scopes=["accounts"],
                        expires_at=NOW + timedelta(hours=1),
                        now=NOW,
                        expected_generation=0,
                    ).generation
                except ActivationError:
                    return None

            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: rotate(), range(2)))
            self.assertCountEqual(results, [1, None])
            self.assertEqual(store.metadata("test").generation, 1)

    def test_token_directory_alias_is_not_followed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "target").mkdir()
            (base / "alias").symlink_to(base / "target", target_is_directory=True)
            with self.assertRaises(UnsafeTokenStore):
                SecureTokenStore(base / "alias", project_root=base / "project")
            self.assertEqual(list((base / "target").iterdir()), [])

    def test_dangling_token_alias_is_preserved_and_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            store = SecureTokenStore(base / "tokens", project_root=base / "project")
            store.root.mkdir(mode=0o700)
            alias = store.root / "test.json"
            alias.symlink_to(base / "missing")
            with self.assertRaises(UnsafeTokenStore):
                store.rotate(
                    "test",
                    access_token="fixture",
                    refresh_token=None,
                    granted_scopes=["accounts"],
                    expires_at=NOW + timedelta(hours=1),
                    now=NOW,
                )
            self.assertTrue(alias.is_symlink())
            self.assertFalse((base / "missing").exists())
