#!/usr/bin/env python3
"""Private-credential bridge to the bounded, explicitly approved DEMO canary.

Only the child receives credentials, and only with --network.  This bridge
does not authenticate, mutate an account, or grant approval.  The canonical
controller rechecks the approval, server identity, time window and all gates.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.ctrader_query_launcher import (
    _CLIENT_ID_ENV,
    _CLIENT_SECRET_ENV,
    _CREDENTIALS_ENV,
    _PYTHON_ENV_KEYS,
    LauncherError,
    _credentials,
    _private_directory,
    _private_executable,
    _private_file,
    _source_file,
)


def _arguments(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--approval-file", type=Path)
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--network", action="store_true")
    parser.add_argument("--execute", action="store_true", help="requiere aprobación privada y ventana vigente")
    parser.add_argument("--max-events", type=int)
    args = parser.parse_args(list(argv))
    if args.execute and (not args.network or args.approval_file is None):
        parser.error("--execute requiere --network y --approval-file")
    if args.network and args.state_dir is None:
        parser.error("--network requiere --state-dir privado explícito")
    if args.execute and (args.max_events is None or args.max_events <= 0):
        parser.error("--execute requiere --max-events positivo")
    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events debe ser positivo")
    return args


def build_exec(
    argv: Sequence[str], *, root: Path, runtime_path: Path, inherited: Mapping[str, str]
) -> tuple[Path, list[str], dict[str, str]]:
    """Build a fixed child command without starting it or contacting a broker."""

    args = _arguments(argv)
    config = _private_file(args.config, label="configuración de canaria")
    runtime = _private_executable(runtime_path, label="Python canónico")
    controller = _source_file(root / "tools" / "demo_canary.py", label="controlador de canaria")
    command = [str(runtime), "-I", "-B", str(controller), "--config", str(config)]
    if args.approval_file is not None:
        approval = _private_file(args.approval_file, label="aprobación de canaria")
        command.extend(["--approval-file", str(approval)])
    if args.state_dir is not None:
        state = _private_directory(args.state_dir, label="estado de canaria")
        command.extend(["--state-dir", str(state)])
    if args.max_events is not None:
        command.extend(["--max-events", str(args.max_events)])
    if args.network:
        command.append("--network")
    if args.execute:
        command.append("--execute")
    elif not args.network:
        command.append("--preflight")
    child = dict(inherited)
    for key in _PYTHON_ENV_KEYS | {_CLIENT_ID_ENV, _CLIENT_SECRET_ENV, _CREDENTIALS_ENV}:
        child.pop(key, None)
    child["PATH"] = "/usr/bin:/bin"
    child["PYTHONNOUSERSITE"] = "1"
    if args.network:
        client_id, client_secret = _credentials(inherited)
        child[_CLIENT_ID_ENV] = client_id
        child[_CLIENT_SECRET_ENV] = client_secret
    return runtime, command, child


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv if argv is not None else sys.argv[1:])
    # Help/invalid flags exit before reading config, tokens or credentials.
    _arguments(arguments)
    root = Path(__file__).resolve().parents[1]
    runtime = Path.home() / ".local/share/mtf-lab/runtime/runtime-python/bin/python"
    try:
        executable, command, child = build_exec(arguments, root=root, runtime_path=runtime, inherited=os.environ)
        os.execve(str(executable), command, child)
    except LauncherError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"ERROR: no se pudo iniciar la canaria ({type(exc).__name__})", file=sys.stderr)
        return 2
    return 0  # pragma: no cover - exec replaces the process.


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
