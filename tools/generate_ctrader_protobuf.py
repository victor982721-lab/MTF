#!/usr/bin/env python3
"""Regenerate the pinned Spotware cTrader Python protobuf modules.

The repository carries the four upstream ``.proto`` sources and generated
modules so runtime operation never downloads schemas or invokes ``protoc``.
This tool is deliberately offline: it verifies the pinned source bytes,
requires the exact compiler version, generates into a temporary directory,
applies the deterministic relative-import compatibility rewrite required by
the upstream schemas (which have no Python package declarations), and only
creates missing destination files.  Existing files with different bytes are
never overwritten.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_RELATIVE = Path("schemas/ctrader/openapi-proto-messages/91")
OUTPUT_RELATIVE = Path("mtf_lab/data/protobuf_generated")
MANIFEST_RELATIVE = Path("manifests/ctrader-protobuf-91.json")
PROVENANCE_RELATIVE = Path("mtf_lab/resources/provenance/ctrader-protobuf-91.json")
SCHEMA_TAG = "91"
SCHEMA_COMMIT = "017413087c1c23c1866bbf07ff24d56574047253"
PROTOC_VERSION = "36.1"
PROTOBUF_RUNTIME_VERSION = "7.36.1"

SOURCE_FILES = (
    "OpenApiCommonMessages.proto",
    "OpenApiCommonModelMessages.proto",
    "OpenApiMessages.proto",
    "OpenApiModelMessages.proto",
)
SOURCE_SHA256 = {
    "OpenApiCommonMessages.proto": "9816cd24b340dcc4eb28548eb4dd16735995d2a61889337591e5c4d8021652a2",
    "OpenApiCommonModelMessages.proto": "b95d7df670a7e890a53ec08f676198ace7bb0a074a4b07ff0b493c4be00a0dea",
    "OpenApiMessages.proto": "a342d27a5298d9b58b6011843f267fe1e1e71af189bcfa5d1dc99abaf9c9d34f",
    "OpenApiModelMessages.proto": "c2d34d07e663f9b43d6b5806ae2b706f460aa55afd847aa83343d3d9955201ee",
}
SOURCE_GIT_BLOB_SHA1 = {
    "OpenApiCommonMessages.proto": "a21699b17c0101f4fc230710743449aab92e9685",
    "OpenApiCommonModelMessages.proto": "a420d2bea7a92c9e31a97c3a67d3553f97b1e21d",
    "OpenApiMessages.proto": "959bfb2e101485e266afdf48d69c38d73c947e2c",
    "OpenApiModelMessages.proto": "79db8354d04cb0c78d20410078737170a7212f58",
}
GENERATED_FILES = (
    "OpenApiCommonMessages_pb2.py",
    "OpenApiCommonModelMessages_pb2.py",
    "OpenApiMessages_pb2.py",
    "OpenApiModelMessages_pb2.py",
)
STUB_FILES = tuple(name.removesuffix(".py") + ".pyi" for name in GENERATED_FILES)
ALL_GENERATED_FILES = GENERATED_FILES + STUB_FILES
RELATIVE_IMPORTS = {
    "OpenApiCommonMessages_pb2.py": "OpenApiCommonModelMessages_pb2",
    "OpenApiMessages_pb2.py": "OpenApiModelMessages_pb2",
    "OpenApiCommonMessages_pb2.pyi": "OpenApiCommonModelMessages_pb2",
    "OpenApiMessages_pb2.pyi": "OpenApiModelMessages_pb2",
}
PACKAGED_LICENSES = {
    "mtf_lab/resources/licenses/spotware-openapi-proto-messages-MIT.txt": "licencias/spotware-openapi-proto-messages-MIT.txt",
    "mtf_lab/resources/licenses/protobuf-7.36.1-BSD-3-Clause.txt": "licencias/protobuf-7.36.1-BSD-3-Clause.txt",
}
LICENSE_SHA256 = {
    "licencias/spotware-openapi-proto-messages-MIT.txt": "d3fc4cfe3604a0d96b55f7808c530e428aa74f349364eaee195339dfa95d5213",
    "licencias/protobuf-7.36.1-BSD-3-Clause.txt": "6e5e117324afd944dcf67f36cf329843bc1a92229a8cd9bb573d7a83130fea7d",
}
INIT_BYTES = (
    '"""Pinned Spotware Open API protobuf generated modules.\n'
    "\n"
    "Generated from openapi-proto-messages release 91; do not edit manually.\n"
    '"""\n\n'
    f'SCHEMA_TAG = "{SCHEMA_TAG}"\n'
    f'SCHEMA_COMMIT = "{SCHEMA_COMMIT}"\n'
    f'PROTOC_VERSION = "{PROTOC_VERSION}"\n'
    f'PROTOBUF_RUNTIME_VERSION = "{PROTOBUF_RUNTIME_VERSION}"\n'
    'SCHEMA_REVISION = f"openapi-proto-messages@{SCHEMA_TAG}:{SCHEMA_COMMIT}"\n'
).encode()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _resolve(value: str | Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else ROOT / path


def _verify_sources(schema_dir: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name in SOURCE_FILES:
        path = schema_dir / name
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(f"schema source missing or symlink: {path}")
        data = path.read_bytes()
        digest = _sha256(data)
        expected = SOURCE_SHA256[name]
        if digest != expected:
            raise RuntimeError(f"schema source hash mismatch for {name}: {digest} != {expected}")
        result[name] = {
            "path": str(SCHEMA_RELATIVE / name),
            "size": len(data),
            "sha256": digest,
            "git_blob_sha1": SOURCE_GIT_BLOB_SHA1[name],
            "url": f"https://raw.githubusercontent.com/spotware/openapi-proto-messages/{SCHEMA_TAG}/{name}",
        }
    return result


def _verify_protoc(protoc: Path) -> None:
    if not protoc.is_file() or protoc.is_symlink():
        raise RuntimeError(f"protoc missing or symlink: {protoc}")
    result = subprocess.run(
        [str(protoc), "--version"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    observed = (result.stdout or result.stderr).strip()
    expected = f"libprotoc {PROTOC_VERSION}"
    if result.returncode != 0 or observed != expected:
        raise RuntimeError(f"protoc version mismatch: {observed!r} != {expected!r}")


def _rewrite_relative_imports(path: Path, module_name: str) -> None:
    text = path.read_text(encoding="utf-8")
    old = f"import {module_name} as "
    new = f"from . import {module_name} as "
    if text.count(old) != 1:
        raise RuntimeError(f"expected one generated import in {path.name}: {old!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8", newline="")


def _rewrite_stub_mapping(path: Path) -> None:
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines(keepends=True):
        if line.startswith("from collections.abc import") and "Mapping as _Mapping" in line:
            lines.append(line)
            continue
        if "_Mapping" in line:
            line = line.replace("_Mapping", "_Mapping[str, _Any]")
        if line.startswith("from typing import") and "Any as _Any" not in line:
            line = line.replace("from typing import ", "from typing import Any as _Any, ", 1)
        lines.append(line)
    path.write_text("".join(lines), encoding="utf-8", newline="")


def _generate(protoc: Path, schema_dir: Path) -> dict[str, bytes]:
    with tempfile.TemporaryDirectory(prefix="mtf-ctrader-protobuf-") as temporary:
        temporary_root = Path(temporary)
        command = [
            str(protoc),
            f"--proto_path={schema_dir}",
            f"--python_out={temporary_root}",
            f"--pyi_out={temporary_root}",
            *(str(schema_dir / name) for name in SOURCE_FILES),
        ]
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()[-1000:]
            raise RuntimeError(f"protoc failed: {detail}")
        for name, module_name in RELATIVE_IMPORTS.items():
            _rewrite_relative_imports(temporary_root / name, module_name)
        for name in STUB_FILES:
            _rewrite_stub_mapping(temporary_root / name)
        generated = {name: (temporary_root / name).read_bytes() for name in ALL_GENERATED_FILES}
        generated["__init__.py"] = INIT_BYTES
        return generated


def _artifact_records(output: Path, generated: dict[str, bytes]) -> list[dict[str, Any]]:
    return [
        {
            "path": str(OUTPUT_RELATIVE / name),
            "size": len(data),
            "sha256": _sha256(data),
        }
        for name, data in sorted(generated.items())
    ]


def _manifest(source_records: dict[str, dict[str, Any]], generated: dict[str, bytes]) -> bytes:
    payload: dict[str, Any] = {
        "schema": {
            "repository": "https://github.com/spotware/openapi-proto-messages",
            "tag": SCHEMA_TAG,
            "commit": SCHEMA_COMMIT,
            "files": [source_records[name] for name in SOURCE_FILES],
        },
        "generator": {
            "protoc_version": PROTOC_VERSION,
            "python_runtime_version": PROTOBUF_RUNTIME_VERSION,
            "relative_import_rewrite": RELATIVE_IMPORTS,
            "stub_mapping_rewrite": "bare _Mapping annotations -> _Mapping[str, _Any]",
            "generated_files": list(ALL_GENERATED_FILES),
        },
        "quality_scope": {
            "ruff_exclusion": "mtf_lab/data/protobuf_generated",
            "ruff_exclusion_reason": "protoc-owned generated code and stubs; regenerated and hash-verified here",
            "mypy_policy": "generated .pyi stubs are consumed; generated implementation is not hand-maintained",
            "pyright_policy": "generated .pyi stubs are consumed; runtime import warnings remain observable",
        },
        "runtime_artifacts": {
            "protobuf_wheel": {
                "filename": "protobuf-7.36.1-cp310-abi3-manylinux2014_x86_64.whl",
                "sha256": "97198b77e369a0abd8e262b8f6c7266c55ddb796a3a12c76d7b8881188ed83aa",
                "url": "https://files.pythonhosted.org/packages/22/df/c799fe7a05ef16ba853a59db01f3a2c5f7d0676469589ccc4874f76a2a88/protobuf-7.36.1-cp310-abi3-manylinux2014_x86_64.whl",
            },
            "types_protobuf_wheel": {
                "filename": "types_protobuf-7.35.1.20260906-py3-none-any.whl",
                "sha256": "5155e48569e0dabff303fdf578db96cd31ea9a4a63b18018a4ceac6b0ae17462",
                "url": "https://files.pythonhosted.org/packages/44/4e/f63e826c68f77ef875506d72f225918800346545ee99847bc28f3394f18d/types_protobuf-7.35.1.20260906-py3-none-any.whl",
            },
            "protoc_asset": {
                "filename": "protoc-36.1-linux-x86_64.zip",
                "sha256": "c4bc672d9d49214dc8cafdceadf4df92182d6ca8e3ec65a56b2d7de5602669b4",
                "url": "https://github.com/protocolbuffers/protobuf/releases/download/v36.1/protoc-36.1-linux-x86_64.zip",
            },
        },
        "licenses": [
            {
                "path": "licencias/spotware-openapi-proto-messages-MIT.txt",
                "sha256": "d3fc4cfe3604a0d96b55f7808c530e428aa74f349364eaee195339dfa95d5213",
                "spdx": "MIT",
            },
            {
                "path": "licencias/protobuf-7.36.1-BSD-3-Clause.txt",
                "sha256": "6e5e117324afd944dcf67f36cf329843bc1a92229a8cd9bb573d7a83130fea7d",
                "spdx": "BSD-3-Clause",
            },
            {
                "path": "licencias/types-protobuf-7.35.1.20260906-Apache-2.0.txt",
                "sha256": "295f8538c94ae5c3043301cf7cff1c852dab6a786a8ddee471e061b40d5ecabe",
                "spdx": "Apache-2.0",
            },
        ],
        "packaged_resources": [
            {
                "path": "mtf_lab/resources/licenses/spotware-openapi-proto-messages-MIT.txt",
                "sha256": "d3fc4cfe3604a0d96b55f7808c530e428aa74f349364eaee195339dfa95d5213",
            },
            {
                "path": "mtf_lab/resources/licenses/protobuf-7.36.1-BSD-3-Clause.txt",
                "sha256": "6e5e117324afd944dcf67f36cf329843bc1a92229a8cd9bb573d7a83130fea7d",
            },
            {
                "path": str(PROVENANCE_RELATIVE),
                "sha256": _sha256(_provenance(source_records, generated)),
            },
        ],
        "generated": _artifact_records(ROOT / OUTPUT_RELATIVE, generated),
    }
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _provenance(source_records: dict[str, dict[str, Any]], generated: dict[str, bytes]) -> bytes:
    payload: dict[str, Any] = {
        "schema": {
            "repository": "https://github.com/spotware/openapi-proto-messages",
            "tag": SCHEMA_TAG,
            "commit": SCHEMA_COMMIT,
            "files": [{"name": name, "sha256": source_records[name]["sha256"]} for name in SOURCE_FILES],
        },
        "generated": [{"name": name, "sha256": _sha256(data)} for name, data in sorted(generated.items())],
        "toolchain": {"protoc": PROTOC_VERSION, "protobuf": PROTOBUF_RUNTIME_VERSION},
        "licenses": [
            {
                "path": "licenses/spotware-openapi-proto-messages-MIT.txt",
                "spdx": "MIT",
                "sha256": "d3fc4cfe3604a0d96b55f7808c530e428aa74f349364eaee195339dfa95d5213",
            },
            {
                "path": "licenses/protobuf-7.36.1-BSD-3-Clause.txt",
                "spdx": "BSD-3-Clause",
                "sha256": "6e5e117324afd944dcf67f36cf329843bc1a92229a8cd9bb573d7a83130fea7d",
            },
        ],
    }
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _create_if_absent(path: Path, data: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise RuntimeError(f"refusing symlink destination: {path}")
    if path.exists():
        if path.read_bytes() != data:
            raise RuntimeError(f"destination exists with different bytes: {path}")
        return "unchanged"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o644)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        raise
    return "created"


def _check(path: Path, data: bytes) -> None:
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f"generated artifact missing or symlink: {path}")
    if path.read_bytes() != data:
        raise RuntimeError(f"generated artifact differs: {path}")


def _ensure_packaged_licenses(*, check: bool) -> list[str]:
    actions: list[str] = []
    for packaged_name, source_name in PACKAGED_LICENSES.items():
        packaged = _resolve(packaged_name)
        source = _resolve(source_name)
        if not source.is_file() or source.is_symlink():
            raise RuntimeError(f"license source missing or symlink: {source}")
        data = source.read_bytes()
        digest = _sha256(data)
        if digest != LICENSE_SHA256[source_name]:
            raise RuntimeError(f"license source hash mismatch for {source_name}: {digest}")
        if check:
            _check(packaged, data)
            actions.append("verified")
        else:
            actions.append(_create_if_absent(packaged, data))
    return actions


def _replace_atomically(path: Path, data: bytes) -> None:
    if path.is_symlink():
        raise RuntimeError(f"refusing symlink destination: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--protoc", help="exact protoc 36.1 executable")
    parser.add_argument("--schema-dir", default=str(SCHEMA_RELATIVE))
    parser.add_argument("--output", default=str(OUTPUT_RELATIVE))
    parser.add_argument("--manifest", default=str(MANIFEST_RELATIVE))
    parser.add_argument("--check", action="store_true", help="verify without creating files")
    parser.add_argument(
        "--refresh-manifest",
        action="store_true",
        help="replace an existing manifest after preserving its prior bytes externally",
    )
    parser.add_argument(
        "--refresh-generated",
        action="store_true",
        help="replace existing generated files after preserving their prior bytes externally",
    )
    args = parser.parse_args(argv)

    protoc = _resolve(args.protoc) if args.protoc else Path(shutil.which("protoc") or "")
    schema_dir = _resolve(args.schema_dir)
    output = _resolve(args.output)
    manifest_path = _resolve(args.manifest)
    source_records = _verify_sources(schema_dir)
    _verify_protoc(protoc)
    generated = _generate(protoc, schema_dir)
    manifest = _manifest(source_records, generated)
    provenance = _provenance(source_records, generated)

    destinations = {name: output / name for name in generated}
    destinations["manifest"] = manifest_path
    provenance_path = _resolve(PROVENANCE_RELATIVE)
    if args.check:
        _ensure_packaged_licenses(check=True)
        for name, data in generated.items():
            _check(destinations[name], data)
        _check(manifest_path, manifest)
        _check(provenance_path, provenance)
        action = "verified"
    else:
        actions = _ensure_packaged_licenses(check=False)
        for name, data in generated.items():
            if destinations[name].exists() and args.refresh_generated:
                _replace_atomically(destinations[name], data)
                actions.append("refreshed")
            else:
                actions.append(_create_if_absent(destinations[name], data))
        if manifest_path.exists() and args.refresh_manifest:
            _replace_atomically(manifest_path, manifest)
            actions.append("refreshed")
        else:
            actions.append(_create_if_absent(manifest_path, manifest))
        if provenance_path.exists() and args.refresh_manifest:
            _replace_atomically(provenance_path, provenance)
            actions.append("refreshed")
        else:
            actions.append(_create_if_absent(provenance_path, provenance))
        action = "created" if "created" in actions else "unchanged"
        if "refreshed" in actions:
            action = "refreshed"

    print(
        json.dumps(
            {
                "action": action,
                "schema_revision": f"openapi-proto-messages@{SCHEMA_TAG}:{SCHEMA_COMMIT}",
                "protoc_version": PROTOC_VERSION,
                "protobuf_runtime_version": PROTOBUF_RUNTIME_VERSION,
                "generated_count": len(generated),
                "manifest": str(MANIFEST_RELATIVE),
                "provenance": str(PROVENANCE_RELATIVE),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
