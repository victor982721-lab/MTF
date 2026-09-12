#!/usr/bin/env python3
"""Run the local MTF Lab quality gates without network or credentials.

The gate intentionally keeps the repository-wide legacy advisory separate from
the maintained refactor surface.  It runs Ruff/format on the refactored modules
and tooling, the existing strict mypy contract, the checked-in Pyright project,
the architecture audit, and an optional branch-coverage threshold.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RUFF_TARGETS = (
    "mtf_lab/configuration.py",
    "mtf_lab/core/aggregation.py",
    "mtf_lab/core/models.py",
    "mtf_lab/core/quality.py",
    "mtf_lab/core/strategy.py",
    "mtf_lab/data/capture.py",
    "mtf_lab/data/importer.py",
    "mtf_lab/data/kraken.py",
    "mtf_lab/data/models.py",
    "mtf_lab/data/translation.py",
    "mtf_lab/ops/backtest.py",
    "mtf_lab/ops/importer.py",
    "mtf_lab/ops/query.py",
    "mtf_lab/ops/reporting.py",
    "mtf_lab/ops/simulation.py",
    "mtf_lab/pipeline.py",
    "mtf_lab/runtime/integration.py",
    "tools/quality_gate.py",
    "tests/test_config_runtime_refactor.py",
    "tests/test_data_refactor.py",
    "tests/test_ops_refactor.py",
    "tests/test_pipeline_refactor.py",
    "tests/test_packaging_delivery.py",
)
MYPY_TARGETS = (
    "mtf_lab/core/canonical.py",
    "mtf_lab/core/cfd_simulation.py",
    "mtf_lab/core/cfd_quality.py",
    "mtf_lab/core/numeric.py",
    "mtf_lab/core/reference.py",
    "mtf_lab/data/capture.py",
    "mtf_lab/data/ctrader_accounts.py",
    "mtf_lab/data/ctrader_config.py",
    "mtf_lab/data/ctrader_errors.py",
    "mtf_lab/data/ctrader_fixtures.py",
    "mtf_lab/data/ctrader_market.py",
    "mtf_lab/data/ctrader_protocol.py",
    "mtf_lab/data/ctrader_session.py",
    "mtf_lab/data/ctrader_transport.py",
    "mtf_lab/ops/ctrader_capture.py",
    "mtf_lab/ops/ctrader_paper_adapters.py",
    "mtf_lab/ops/ctrader_pipeline.py",
    "mtf_lab/ops/ctrader_demo_transport.py",
    "mtf_lab/ops/ctrader_demo_composition.py",
    "mtf_lab/runtime/consumers.py",
)


@dataclass(frozen=True)
class Outcome:
    name: str
    command: tuple[str, ...]
    returncode: int
    output: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "returncode": self.returncode,
            "ok": self.ok,
            "output_tail": self.output[-3000:],
        }


def _command(python: Path, *args: str) -> tuple[str, ...]:
    return (str(python), *args)


def _run(name: str, command: tuple[str, ...], *, env: dict[str, str] | None = None) -> Outcome:
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=900,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return Outcome(name, command, 124, f"{type(exc).__name__}: {exc}")
    output = (result.stdout or "") + (result.stderr or "")
    return Outcome(name, command, result.returncode, output)


def _runtime_site(runtime_python: Path) -> str | None:
    probe = _run("runtime-site", _command(runtime_python, "-c", "import site; print(site.getsitepackages()[0])"))
    if not probe.ok:
        return None
    site = probe.output.strip().splitlines()[-1] if probe.output.strip() else ""
    return site or None


def _coverage_env(runtime_python: Path) -> dict[str, str] | None:
    site = _runtime_site(runtime_python)
    if site is None:
        return None
    env = {
        name: os.environ[name] for name in ("LANG", "PATH", "PYTHONHASHSEED", "PYTHONUTF8", "TZ") if name in os.environ
    }
    entries = [str(ROOT), site]
    if os.environ.get("PYTHONPATH"):
        entries.append(os.environ["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(entries)
    env["HOME"] = str(ROOT / "runtime" / "quality-home")
    env["XDG_STATE_HOME"] = str(ROOT / "runtime" / "quality-state")
    env["XDG_CONFIG_HOME"] = str(ROOT / "runtime" / "quality-config")
    env["XDG_CACHE_HOME"] = str(ROOT / "runtime" / "quality-cache")
    env["MTF_UI_JS_DEV"] = "1"
    env["MTF_NODE_BIN"] = os.environ.get("MTF_NODE_BIN") or shutil.which("node") or ""
    return env


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-python", type=Path, default=ROOT / ".venv" / "bin" / "python")
    parser.add_argument("--dev-python", type=Path, default=ROOT / ".venv-dev" / "bin" / "python")
    parser.add_argument("--coverage-threshold", type=float, default=60.0)
    parser.add_argument("--skip-coverage", action="store_true")
    parser.add_argument("--json", type=Path, help="escribe el resultado JSON en esta ruta")
    return parser


def _outcome_list(runtime_python: Path, dev_python: Path, *, skip_coverage: bool, threshold: float) -> list[Outcome]:
    targets = tuple(str(ROOT / target) for target in RUFF_TARGETS if (ROOT / target).is_file())
    outcomes = [
        _run("ruff", _command(dev_python, "-m", "ruff", "check", "--no-cache", *targets)),
        _run("ruff-format", _command(dev_python, "-m", "ruff", "format", "--check", "--no-cache", *targets)),
        _run("mypy", _command(dev_python, "-m", "mypy", "--strict", "--follow-imports=silent", *MYPY_TARGETS)),
        _run("pyright", _command(dev_python, "-m", "pyright", "--project", "pyrightconfig.json")),
        _run(
            "architecture",
            _command(
                runtime_python,
                "tools/engineering_audit.py",
                "--strict",
                "--json",
                "runtime/quality-architecture.json",
            ),
        ),
    ]
    if not skip_coverage:
        env = _coverage_env(runtime_python)
        if env is None:
            outcomes.append(
                Outcome(
                    "coverage",
                    (str(dev_python), "-m", "coverage"),
                    1,
                    "no se pudo resolver site-packages del runtime",
                )
            )
        else:
            outcomes.extend(
                (
                    _run("coverage-run", _command(dev_python, "-m", "coverage", "erase"), env=env),
                    _run(
                        "coverage-tests",
                        _command(
                            dev_python,
                            "-m",
                            "coverage",
                            "run",
                            "--branch",
                            "--source=mtf_lab",
                            "-m",
                            "unittest",
                            "discover",
                            "-s",
                            "tests",
                            "-q",
                        ),
                        env=env,
                    ),
                    _run(
                        "coverage-report",
                        _command(dev_python, "-m", "coverage", "report", "--fail-under", str(threshold)),
                        env=env,
                    ),
                )
            )
    return outcomes


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    (ROOT / "runtime").mkdir(parents=True, exist_ok=True)
    outcomes = _outcome_list(
        args.runtime_python,
        args.dev_python,
        skip_coverage=args.skip_coverage,
        threshold=args.coverage_threshold,
    )
    payload = {
        "root": str(ROOT),
        "coverage_threshold": args.coverage_threshold,
        "skipped_coverage": args.skip_coverage,
        "ok": all(item.ok for item in outcomes),
        "checks": {item.name: item.as_dict() for item in outcomes},
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(rendered + "\n", encoding="utf-8")
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
