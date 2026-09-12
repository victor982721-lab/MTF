"""Offline steady-state PAPER benchmark; all durable state lives in a temp dir."""

from __future__ import annotations

import argparse
import gc
import json
import os
import socket
import time
import tracemalloc
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from mtf_lab.core.canonical import canonical_json
from mtf_lab.data.capture import CaptureEnvelope, MessageClass
from mtf_lab.data.ctrader import CTraderInstrumentSpec
from mtf_lab.data.paper_fixture import synthetic_ctrader_payloads
from mtf_lab.ops.ctrader_pipeline import CaptureCoverage, CTraderPaperSession, CTraderPipeline
from mtf_lab.ops.persistence import SQLiteStore
from tests.test_ctrader_pipeline import BASE, pipeline_config


def events(count: int) -> Iterator[CaptureEnvelope]:
    templates = synthetic_ctrader_payloads(count=190)
    for index in range(count):
        raw = dict(templates[index % len(templates)])
        market = BASE + timedelta(minutes=index + 1)
        available = market + timedelta(seconds=1)
        raw.update(timestamp=int(market.timestamp() * 1000), sequence=index, snapshot=index == 0)
        raw["trendbar"] = [
            dict(raw["trendbar"][0], utcTimestampInMinutes=int((market - timedelta(minutes=1)).timestamp() / 60))
        ]
        yield CaptureEnvelope(market, available, available, index, 0, MessageClass.SPOT, raw)


def sample(session: CTraderPaperSession, events_processed: int, elapsed: float) -> dict[str, Any]:
    state = session.snapshot()
    checkpoint_bytes = len(canonical_json(state).encode())
    processor = state["runtime"]["processor"]
    sizes = {
        "events": len(processor["events"]),
        "seen_event_ids": len(processor["seen_event_ids"]),
        "signals": len(processor["signals"]),
        "evaluations": len(processor["evaluations"]),
        "episodes": len(processor["episodes"]),
        "candles": {key: len(value) for key, value in processor["candles"].items()},
        "indicator_points": {key: len(value) for key, value in processor["indicator_points"].items()},
        "cfd_trades": len(state["paper"]["trades"]),
        "cfd_quote_ids": len(state["paper"]["seen_quote_ids"]),
        "cfd_events": len(state["paper"]["events"]),
    }
    del state, processor
    gc.collect()
    current, peak = tracemalloc.get_traced_memory()
    return {
        "events": events_processed,
        "phase_seconds": elapsed,
        "checkpoint_bytes": checkpoint_bytes,
        "python_bytes_current": current,
        "python_bytes_peak": peak,
        "active_sizes": sizes,
    }


def run(count: int) -> dict[str, Any]:
    with TemporaryDirectory(prefix="mtf-paper-benchmark-") as directory:
        root = Path(directory)
        env = {
            "HOME": directory,
            "XDG_CONFIG_HOME": str(root / "config"),
            "XDG_DATA_HOME": str(root / "data"),
            "XDG_CACHE_HOME": str(root / "cache"),
            "XDG_STATE_HOME": str(root / "state"),
        }
        with (
            patch.dict(os.environ, env),
            patch.object(socket.socket, "connect", side_effect=AssertionError("offline network fence")),
            SQLiteStore(root / "paper.sqlite3") as store,
        ):
            pipeline = CTraderPipeline(
                store, pipeline_config(), spec=CTraderInstrumentSpec(symbol_id=99), max_candles=256
            )
            session = pipeline.open_session(
                dataset_id="benchmark-synthetic-v1",
                session_id="benchmark",
                coverage=CaptureCoverage(continuity="CONTINUOUS"),
            )
            tracemalloc.start()
            iterator = iter(events(count))
            samples: list[dict[str, Any]] = []
            start = time.perf_counter()
            for offset in range(0, count, 250):
                session.ingest_many(next(iterator) for _ in range(min(250, count - offset)))
                done = min(offset + 250, count)
                if done % 1500 == 0 or done == count:
                    elapsed = time.perf_counter() - start
                    samples.append(sample(session, done, elapsed))
                    start = time.perf_counter()
            tracemalloc.stop()
            return {
                "schema_version": 1,
                "fixture": "synthetic repeated scenario v1; not market results",
                "max_candles_per_timeframe": 256,
                "warmup_retention_events": 3840,
                "network_blocked": True,
                "stores": "temporary",
                "samples": samples,
                "events_durable": sum(1 for _ in store.iter_capture_envelopes("benchmark")),
            }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=7500)
    args = parser.parse_args()
    if args.events < 1:
        parser.error("events must be positive")
    print(json.dumps(run(args.events), sort_keys=True))
