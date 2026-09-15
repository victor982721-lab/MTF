#!/usr/bin/env python3
"""Run the complete local MTF Lab quality gate without network or credentials.

Ruff and its formatter cover every Python file below ``mtf_lab``, ``tests``
and ``tools``.  Mypy covers every Python file below ``mtf_lab`` and ``tools``
with explicit package bases.  The test/coverage leg deliberately delegates to
``offline_tests`` so its HOME/XDG/TMP isolation and network/write guard remain
the single runner contract.  Coverage gates line and branch percentages
independently and records each numerator and denominator.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import verify_offline as delivery
from tools.quality_scope import QualityScope, discover_quality_scope

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COVERAGE_THRESHOLD = 60.0


def _resolve_path(value: str | os.PathLike[str] | None, default: Path, *, root: Path) -> Path:
    candidate = Path(value) if value is not None else default
    return candidate if candidate.is_absolute() else root / candidate


def _node_path(value: str | os.PathLike[str] | None, *, root: Path) -> Path | None:
    configured = value or os.environ.get("MTF_NODE_BIN") or shutil.which("node")
    if configured is None:
        return None
    candidate = Path(configured)
    return candidate if candidate.is_absolute() else root / candidate


def _failed_receipt(
    command: Sequence[str | os.PathLike[str]],
    reason: str,
    *,
    root: Path,
    temporary_root: Path,
) -> dict[str, Any]:
    outcome = delivery._CommandOutcome(
        tuple(os.fspath(item) for item in command),
        2,
        "",
        reason,
        False,
        0.0,
    )
    receipt = outcome.to_dict(root=root, temporary_roots=(temporary_root,))
    receipt["error"] = reason
    return receipt


def _run_check(
    name: str,
    command: Sequence[str | os.PathLike[str]],
    *,
    root: Path,
    environment: Mapping[str, str],
    timeout: float,
    temporary_root: Path,
) -> dict[str, Any]:
    outcome = delivery.run_command(command, root=root, environment=environment, timeout=timeout)
    receipt = outcome.to_dict(root=root, temporary_roots=(temporary_root,))
    receipt["name"] = name
    if not outcome.ok:
        diagnostic = receipt.get("stderr_tail") or receipt.get("stdout_tail") or "command failed"
        receipt["error"] = str(diagnostic)
    else:
        receipt["error"] = None
    return receipt


def _scope_error(scope: QualityScope, root: Path) -> str:
    return json.dumps(
        {
            "missing_roots": list(scope.missing_roots),
            "empty_roots": list(scope.empty_roots),
            "missing_files": list(scope.missing_files(root)),
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def _static_checks(
    runtime_python: Path,
    dev_python: Path,
    *,
    root: Path,
    environment: Mapping[str, str],
    temporary_root: Path,
    scope: QualityScope,
    timeout: float,
) -> dict[str, Any]:
    """Run required static checks and retain the complete scope in receipts."""

    scope_receipt = scope.as_dict(root)
    scope_ok = bool(scope_receipt["ok"])
    invalid_reason = f"quality scope invalid: {_scope_error(scope, root)}"
    compile_environment = dict(environment)
    compile_environment["PYTHONPYCACHEPREFIX"] = str(temporary_root / "compile-cache")
    checks: dict[str, Any] = {
        "scope": scope_receipt,
        "compilation": _run_check(
            "compilation",
            [str(runtime_python), "-m", "compileall", "-q", *scope.ruff_roots],
            root=root,
            environment=compile_environment,
            timeout=min(timeout, 180.0),
            temporary_root=temporary_root,
        ),
        "pip_check": _run_check(
            "pip_check",
            [str(runtime_python), "-m", "pip", "check"],
            root=root,
            environment=environment,
            timeout=min(timeout, 60.0),
            temporary_root=temporary_root,
        ),
        "git_diff_check": _run_check(
            "git_diff_check",
            ["git", "diff", "--check", "HEAD", "--"],
            root=root,
            environment=environment,
            timeout=min(timeout, 60.0),
            temporary_root=temporary_root,
        ),
        "git_cached_diff_check": _run_check(
            "git_cached_diff_check",
            ["git", "diff", "--cached", "--check", "--"],
            root=root,
            environment=environment,
            timeout=min(timeout, 60.0),
            temporary_root=temporary_root,
        ),
    }

    if scope_ok:
        checks["ruff"] = _run_check(
            "ruff",
            [
                str(dev_python),
                "-m",
                "ruff",
                "check",
                "--no-cache",
                "--output-format",
                "concise",
                *scope.ruff_roots,
            ],
            root=root,
            environment=environment,
            timeout=min(timeout, 300.0),
            temporary_root=temporary_root,
        )
        checks["ruff_format"] = _run_check(
            "ruff_format",
            [str(dev_python), "-m", "ruff", "format", "--check", "--no-cache", *scope.ruff_roots],
            root=root,
            environment=environment,
            timeout=min(timeout, 300.0),
            temporary_root=temporary_root,
        )
        checks["mypy"] = _run_check(
            "mypy",
            [
                str(dev_python),
                "-m",
                "mypy",
                "--strict",
                "--explicit-package-bases",
                "--no-incremental",
                "--python-executable",
                # Mypy resolves installed package stubs through this
                # interpreter.  Keep that lookup in the development
                # environment: the runtime intentionally contains only
                # execution dependencies and therefore must not be used as
                # the typing environment for generated protobuf stubs.
                str(dev_python),
                "--cache-dir",
                str(temporary_root / "mypy-cache"),
                "--show-error-codes",
                *scope.mypy_roots,
            ],
            root=root,
            environment=environment,
            timeout=min(timeout, 420.0),
            temporary_root=temporary_root,
        )
    else:
        checks["ruff"] = _failed_receipt(
            ["ruff", "check", *scope.ruff_roots], invalid_reason, root=root, temporary_root=temporary_root
        )
        checks["ruff_format"] = _failed_receipt(
            ["ruff", "format", *scope.ruff_roots], invalid_reason, root=root, temporary_root=temporary_root
        )
        checks["mypy"] = _failed_receipt(
            ["mypy", "--strict", "--explicit-package-bases", *scope.mypy_roots],
            invalid_reason,
            root=root,
            temporary_root=temporary_root,
        )

    checks["pyright"] = _run_check(
        "pyright",
        [str(dev_python), "-m", "pyright", "--project", "pyrightconfig.json", "--pythonpath", str(runtime_python)],
        root=root,
        environment=environment,
        timeout=min(timeout, 420.0),
        temporary_root=temporary_root,
    )
    architecture_json = temporary_root / "architecture.json"
    checks["architecture"] = _run_check(
        "architecture",
        [str(runtime_python), "tools/engineering_audit.py", "--strict", "--json", str(architecture_json)],
        root=root,
        environment=environment,
        timeout=min(timeout, 240.0),
        temporary_root=temporary_root,
    )
    checks["document_links"] = delivery.document_link_check(root)
    return checks


def _coverage_int(totals: Mapping[str, Any], key: str) -> int | None:
    value = totals.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _coverage_summary(path: Path, *, threshold: float) -> dict[str, Any]:
    """Read Coverage.py JSON and gate lines and branches independently."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return {
            "ok": False,
            "threshold_percent": threshold,
            "error": f"coverage JSON unavailable: {type(exc).__name__}",
            "covered_lines": None,
            "num_statements": None,
            "line_percent": None,
            "covered_branches": None,
            "num_branches": None,
            "branch_percent": None,
        }
    totals = raw.get("totals") if isinstance(raw, Mapping) else None
    if not isinstance(totals, Mapping):
        return {
            "ok": False,
            "threshold_percent": threshold,
            "error": "coverage JSON lacks totals",
            "covered_lines": None,
            "num_statements": None,
            "line_percent": None,
            "covered_branches": None,
            "num_branches": None,
            "branch_percent": None,
        }
    covered_lines = _coverage_int(totals, "covered_lines")
    num_statements = _coverage_int(totals, "num_statements")
    covered_branches = _coverage_int(totals, "covered_branches")
    num_branches = _coverage_int(totals, "num_branches")
    line_percent = 100.0 * covered_lines / num_statements if covered_lines is not None and num_statements else None
    branch_percent = 100.0 * covered_branches / num_branches if covered_branches is not None and num_branches else None
    bounds_ok = bool(
        covered_lines is not None
        and num_statements is not None
        and covered_lines <= num_statements
        and covered_branches is not None
        and num_branches is not None
        and covered_branches <= num_branches
    )
    valid = (
        covered_lines is not None
        and num_statements is not None
        and num_statements > 0
        and covered_branches is not None
        and num_branches is not None
        and num_branches > 0
        and line_percent is not None
        and branch_percent is not None
        and bounds_ok
    )
    if valid:
        assert line_percent is not None
        assert branch_percent is not None
    return {
        "ok": bool(
            valid
            and line_percent is not None
            and branch_percent is not None
            and line_percent >= threshold
            and branch_percent >= threshold
        ),
        "threshold_percent": threshold,
        "error": None if valid else "coverage totals are incomplete or impossible",
        "covered_lines": covered_lines,
        "num_statements": num_statements,
        "line_percent": round(line_percent, 3) if line_percent is not None else None,
        "covered_branches": covered_branches,
        "num_branches": num_branches,
        "branch_percent": round(branch_percent, 3) if branch_percent is not None else None,
    }


def _coverage_checks(
    runtime_python: Path,
    dev_python: Path,
    node: Path | None,
    *,
    root: Path,
    environment: Mapping[str, str],
    temporary_root: Path,
    timeout: float,
    threshold: float,
    build_python: Path | None = None,
) -> dict[str, Any]:
    coverage_data = temporary_root / "coverage" / ".coverage"
    coverage_json = temporary_root / "coverage" / "coverage.json"
    coverage_environment = dict(environment)
    coverage_environment["COVERAGE_FILE"] = str(coverage_data)
    tool = _run_check(
        "coverage_tool",
        [str(dev_python), "-c", "import coverage; print(coverage.__version__)"],
        root=root,
        environment=environment,
        timeout=min(timeout, 60.0),
        temporary_root=temporary_root,
    )
    site = delivery.site_packages_path(
        dev_python,
        root=root,
        environment=environment,
        timeout=min(timeout, 60.0),
    )
    site_value = site.get("path")
    coverage_site = Path(site_value) if isinstance(site_value, str) and site_value else None
    coverage_ready = bool(tool.get("ok") and site.get("ok") and site.get("available") and coverage_site is not None)
    if not coverage_ready:
        node_receipt = delivery._node_version(node, root=root, environment=environment, timeout=min(timeout, 30.0))
        reason = "Coverage.py no disponible en el intérprete dev; no se ejecutó la suite"
        return {
            "ok": False,
            "required": True,
            "tool": tool,
            "site": site,
            "erase": _failed_receipt(
                [str(dev_python), "-m", "coverage", "erase"], reason, root=root, temporary_root=temporary_root
            ),
            "offline_suite": {
                "ok": False,
                "required": True,
                "reason": reason,
                "node": node_receipt,
            },
            "json": _failed_receipt(
                [str(dev_python), "-m", "coverage", "json", "--data-file", str(coverage_data)],
                reason,
                root=root,
                temporary_root=temporary_root,
            ),
            "summary": _coverage_summary(coverage_json, threshold=threshold),
            "data_file": str(coverage_data),
            "json_file": str(coverage_json),
        }

    erase = _run_check(
        "coverage_erase",
        [str(dev_python), "-m", "coverage", "erase"],
        root=root,
        environment=coverage_environment,
        timeout=min(timeout, 60.0),
        temporary_root=temporary_root,
    )
    offline = delivery._run_offline_suite(
        runtime_python,
        node,
        root=root,
        environment=environment,
        temporary_root=temporary_root,
        timeout=timeout,
        coverage_file=coverage_data,
        coverage_site=coverage_site,
        build_python=build_python or dev_python,
    )
    json_receipt = _run_check(
        "coverage_json",
        [
            str(dev_python),
            "-m",
            "coverage",
            "json",
            "--data-file",
            str(coverage_data),
            "--pretty-print",
            "-o",
            str(coverage_json),
        ],
        root=root,
        environment=coverage_environment,
        timeout=min(timeout, 120.0),
        temporary_root=temporary_root,
    )
    summary = _coverage_summary(coverage_json, threshold=threshold)
    ok = bool(erase.get("ok") and offline.get("ok") and json_receipt.get("ok") and summary.get("ok"))
    return {
        "ok": ok,
        "required": True,
        "tool": tool,
        "site": site,
        "erase": erase,
        "offline_suite": offline,
        "json": json_receipt,
        "summary": summary,
        "data_file": str(coverage_data),
        "json_file": str(coverage_json),
        "test_interpreter": str(runtime_python),
        "coverage_interpreter": str(dev_python),
    }


def _mapping_error_messages(value: Mapping[str, Any], location: str) -> list[str]:
    errors: list[str] = []
    failed_gate = value.get("ok") is False
    for key in ("error", "reason", "diagnostic", "guard_error"):
        current = value.get(key)
        if current and (key != "reason" or failed_gate):
            errors.append(f"{location}.{key}: {current}")
    runner_output = value.get("runner_output")
    if isinstance(runner_output, Mapping):
        identifiers = runner_output.get("failure_identifiers")
        if isinstance(identifiers, list) and identifiers:
            errors.append(f"{location}.runner_output: failed tests: {', '.join(map(str, identifiers[:20]))}")
    evidence_fields = ("missing_roots", "empty_roots", "missing_files", "changed", "added", "removed")
    if failed_gate and any(value.get(key) for key in evidence_fields):
        details = {key: value.get(key) for key in evidence_fields if value.get(key)}
        errors.append(f"{location}: required evidence is missing or changed: {details}")
    counts = value.get("counts")
    if failed_gate and isinstance(counts, Mapping):
        failed = counts.get("failed")
        skipped = counts.get("skipped")
        if isinstance(failed, int) and failed > 0:
            errors.append(f"{location}.counts: {failed} test failures/errors")
        if isinstance(skipped, int) and skipped > 0:
            errors.append(f"{location}.counts: {skipped} tests skipped")
    return errors


def _error_list(value: Any, path: str = "") -> list[str]:
    errors: list[str] = []
    if isinstance(value, Mapping):
        location = path or "gate"
        errors.extend(_mapping_error_messages(value, location))
        for key, item in value.items():
            if key not in {"error", "reason", "diagnostic", "guard_error"}:
                errors.extend(_error_list(item, f"{path}.{key}" if path else str(key)))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            errors.extend(_error_list(item, f"{path}[{index}]"))
    return errors


def run(
    root: str | os.PathLike[str] = ROOT,
    *,
    runtime_python: str | os.PathLike[str] | None = None,
    dev_python: str | os.PathLike[str] | None = None,
    node: str | os.PathLike[str] | None = None,
    coverage_threshold: float = DEFAULT_COVERAGE_THRESHOLD,
    skip_coverage: bool = False,
    timeout: float = 900.0,
) -> dict[str, Any]:
    """Run all required gates and return a portable diagnostic receipt."""

    root_path = Path(root).resolve()
    runtime_path = _resolve_path(runtime_python, root_path / ".venv" / "bin" / "python", root=root_path)
    dev_path = _resolve_path(dev_python, root_path / ".venv-dev" / "bin" / "python", root=root_path)
    node_path = _node_path(node, root=root_path)
    with tempfile.TemporaryDirectory(prefix="mtf-quality-gate-") as name:
        temporary_root = Path(name)
        environment = delivery._command_environment(temporary_root)
        environment["MTF_UI_JS_DEV"] = "1"
        if node_path is not None:
            environment["MTF_NODE_BIN"] = str(node_path)
        # The V2 dev wrapper is suitable for tooling but its nested virtualenv
        # inherits a base that intentionally omits ``ensurepip``.  Wheel
        # delivery needs a local build interpreter that can create that nested
        # environment; prefer the canonical project dev venv when present and
        # retain the explicit dev path as a fail-closed fallback for clones
        # without it.  This does not change the runtime or static-tool roles.
        build_path = root_path / ".venv-dev" / "bin" / "python"
        if not build_path.is_file() or not os.access(build_path, os.X_OK):
            build_path = dev_path
        environment["MTF_LAB_BUILD_PYTHON"] = str(build_path)
        before = delivery.source_manifest(root_path)
        git = delivery.git_snapshot(root_path)
        scope = discover_quality_scope(root_path)
        static = _static_checks(
            runtime_path,
            dev_path,
            root=root_path,
            environment=environment,
            temporary_root=temporary_root,
            scope=scope,
            timeout=timeout,
        )
        coverage = (
            {
                "ok": False,
                "required": True,
                "skipped": True,
                "reason": "coverage no puede omitirse en el gate requerido",
            }
            if skip_coverage
            else _coverage_checks(
                runtime_path,
                dev_path,
                node_path,
                root=root_path,
                environment=environment,
                temporary_root=temporary_root,
                timeout=timeout,
                threshold=coverage_threshold,
                build_python=build_path,
            )
        )
        after = delivery.source_manifest(root_path)
        integrity = delivery._manifest_delta(before, after)
        identity = {
            "baseline_commit": delivery.BASE_COMMIT,
            "head": git.get("head"),
            "branch": git.get("branch"),
            "main": git.get("main"),
            "origin_main": git.get("origin_main"),
            "head_tree_hash": git.get("head_tree_hash"),
            "source_content_sha256_before": before.get("content_sha256"),
            "source_content_sha256_after": after.get("content_sha256"),
            "scope_identity_sha256": scope.identity_sha256,
        }
        validation = {
            "static": static,
            "coverage": coverage,
            "source_integrity": integrity,
        }
        payload = {
            "schema_version": 2,
            "scope": {
                "ruff_roots": list(scope.ruff_roots),
                "mypy_roots": list(scope.mypy_roots),
                "ruff_files": list(scope.ruff_files),
                "mypy_files": list(scope.mypy_files),
                "identity_sha256": scope.identity_sha256,
            },
            "identity": identity,
            "environment": {
                "runtime_python": str(runtime_path),
                "dev_python": str(dev_path),
                "build_python": str(build_path),
                "node": str(node_path) if node_path is not None else None,
                "isolated_state_dir": environment.get("MTF_LAB_STATE_DIR"),
            },
            "validation": validation,
            "errors": _error_list(validation),
            "source_integrity": integrity,
            "ok": bool(delivery._all_gates_pass(validation) and integrity.get("ok")),
        }
        return cast(
            dict[str, Any], delivery.sanitize_payload(payload, root=root_path, temporary_roots=(temporary_root,))
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-python", type=Path, default=None)
    parser.add_argument("--dev-python", type=Path, default=None)
    parser.add_argument("--node", type=Path, default=None)
    parser.add_argument(
        "--coverage-threshold",
        type=float,
        default=DEFAULT_COVERAGE_THRESHOLD,
        help="umbral mínimo independiente para líneas y ramas (0-100)",
    )
    parser.add_argument("--skip-coverage", action="store_true", help="diagnóstico fallido; nunca permite pasar el gate")
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--json", type=Path, help="escribe la recepción JSON; use runtime/ o una ruta externa")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if not 0.0 <= args.coverage_threshold <= 100.0:
        parser.error("--coverage-threshold debe estar entre 0 y 100")
    if args.timeout <= 0:
        parser.error("--timeout debe ser positivo")
    if args.json is not None:
        output = args.json.resolve()
        if output.is_relative_to(ROOT) and output.relative_to(ROOT).parts[:1] != ("runtime",):
            parser.error("--json sólo puede escribir dentro de runtime/ o fuera del checkout")
    payload = run(
        ROOT,
        runtime_python=args.runtime_python,
        dev_python=args.dev_python,
        node=args.node,
        coverage_threshold=args.coverage_threshold,
        skip_coverage=args.skip_coverage,
        timeout=args.timeout,
    )
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.json is None:
        print(rendered, end="")
    else:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(rendered, encoding="utf-8")
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
