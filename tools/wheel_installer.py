#!/usr/bin/env python3
"""Stage sources and install only the local MTF wheel, without a package index.

Wheel bytes are retained in content-addressed directories. Build and smoke
have their own temporary HOME/XDG/state, and never modify the source package.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from collections.abc import Mapping, Sequence
from contextlib import suppress
from email.parser import BytesParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _environment(directory: Path) -> dict[str, str]:
    env = {name: value for name, value in os.environ.items() if name in {"PATH", "LANG", "TZ"}}
    env.setdefault("PATH", os.defpath)
    for variable, relative in (
        ("HOME", "home"),
        ("XDG_CONFIG_HOME", "config"),
        ("XDG_CACHE_HOME", "cache"),
        ("XDG_DATA_HOME", "data"),
        ("XDG_STATE_HOME", "state"),
        ("TMPDIR", "tmp"),
        ("MTF_LAB_STATE_DIR", "mtf-state"),
    ):
        target = directory / relative
        target.mkdir(parents=True, exist_ok=True)
        env[variable] = str(target)
    env.update(
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
        PIP_NO_INDEX="1",
        PIP_CONFIG_FILE=os.devnull,
        PIP_DISABLE_PIP_VERSION_CHECK="1",
        SOURCE_DATE_EPOCH=os.environ.get("SOURCE_DATE_EPOCH", "0"),
    )
    return env


def _run(command: Sequence[str], *, cwd: Path, env: Mapping[str, str]) -> str:
    result = subprocess.run(command, cwd=cwd, env=dict(env), capture_output=True, text=True, check=False, timeout=180)
    if result.returncode:
        raise RuntimeError(f"{Path(command[0]).name} falló ({result.returncode}): {result.stderr[-2000:]}")
    return result.stdout


def _python_executable(value: str) -> Path:
    # Resolving a venv python symlink would select the system interpreter.
    executable = shutil.which(value)
    if executable is None:
        raise ValueError(f"intérprete no ejecutable: {value}")
    return Path(executable).absolute()


def _validate_source_path(root: Path, source: Path) -> None:
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"fuente no regular: {source.relative_to(root)}")
    if any(parent.is_symlink() for parent in source.parents if parent != root and root in parent.parents):
        raise ValueError("el árbol de fuentes contiene un directorio symlink")


def _package_data_root(root: Path, package: Path, package_name: object) -> Path:
    if not isinstance(package_name, str):
        raise ValueError("nombre de paquete-data inválido")
    segments = package_name.split(".")
    if not segments or any(not segment.isidentifier() for segment in segments):
        raise ValueError(f"nombre de paquete-data inválido: {package_name}")
    package_root = root.joinpath(*segments)
    if package_root.is_symlink() or not package_root.is_dir() or not package_root.is_relative_to(package):
        raise ValueError(f"paquete-data no regular: {package_name}")
    return package_root


def _package_data_matches(root: Path, package_root: Path, package_name: str, patterns: object) -> list[Path]:
    if not isinstance(patterns, list) or not all(isinstance(pattern, str) for pattern in patterns):
        raise ValueError(f"patrones package-data inválidos: {package_name}")
    sources: list[Path] = []
    for pattern in patterns:
        relative_pattern = Path(pattern)
        if not pattern or relative_pattern.is_absolute() or ".." in relative_pattern.parts:
            raise ValueError(f"patrón package-data fuera del paquete: {pattern}")
        for source in sorted(package_root.glob(pattern)):
            _validate_source_path(root, source)
            sources.append(source)
    return sources


def _declared_package_data(root: Path, package: Path, metadata: Mapping[str, object]) -> list[Path]:
    tool = metadata.get("tool")
    setuptools = tool.get("setuptools") if isinstance(tool, Mapping) else None
    package_data = setuptools.get("package-data") if isinstance(setuptools, Mapping) else None
    if package_data is None:
        return []
    if not isinstance(package_data, Mapping):
        raise ValueError("package-data inválido")
    sources: list[Path] = []
    for package_name, patterns in package_data.items():
        package_root = _package_data_root(root, package, package_name)
        sources.extend(_package_data_matches(root, package_root, str(package_name), patterns))
    return sources


def _copy_source(root: Path, destination: Path) -> None:
    """Copy only Python sources and package-data declared by pyproject.toml."""
    package = root / "mtf_lab"
    if package.is_symlink() or not package.is_dir():
        raise ValueError("el paquete fuente no es un directorio regular")
    sources = [root / "pyproject.toml", root / "README.md"]
    for source in sources:
        _validate_source_path(root, source)
    metadata = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    for directory, subdirectories, filenames in os.walk(package, followlinks=False):
        parent = Path(directory)
        if any((parent / name).is_symlink() for name in subdirectories):
            raise ValueError("el árbol de fuentes contiene un directorio symlink")
        sources.extend(parent / name for name in filenames if Path(name).suffix in {".py", ".toml"})
    sources.extend(_declared_package_data(root, package, metadata))
    unique_sources: dict[Path, Path] = {}
    for source in sources:
        _validate_source_path(root, source)
        relative = source.relative_to(root)
        previous = unique_sources.setdefault(relative, source)
        if previous != source:
            raise ValueError(f"colisión de fuente: {relative}")

    destination.mkdir()
    for source in unique_sources.values():
        target = destination / source.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())


def _wheel_identity(wheel: Path) -> tuple[str, str]:
    if wheel.is_symlink() or not wheel.is_file() or wheel.suffix != ".whl":
        raise ValueError("se requiere un archivo wheel regular")
    with zipfile.ZipFile(wheel) as archive:
        names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
        if len(names) != 1:
            raise ValueError("metadata wheel ambigua o ausente")
        metadata = BytesParser().parsebytes(archive.read(names[0]))
        name = str(metadata.get("Name", "")).lower().replace("_", "-")
        version = str(metadata.get("Version", ""))
        if name != "mtf-lab" or not version or not wheel.name.startswith(f"mtf_lab-{version}-"):
            raise ValueError("el wheel no corresponde a la distribución mtf-lab")
    return version, hashlib.sha256(wheel.read_bytes()).hexdigest()


def _open_directory(directory: Path) -> int:
    """Create/open each Linux path component without traversing symlinks."""
    absolute = directory.absolute()
    if ".." in absolute.parts:
        raise ValueError("el destino de artefactos no puede contener '..'")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    descriptor = os.open(absolute.anchor, flags)
    try:
        for part in absolute.parts[1:]:
            with suppress(FileExistsError):
                os.mkdir(part, mode=0o755, dir_fd=descriptor)
            child = os.open(part, flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _retain_wheel(wheel: Path, directory: Path) -> Path:
    _, digest = _wheel_identity(wheel)
    output = directory / digest
    target = output / wheel.name
    content = wheel.read_bytes()
    directory_fd = _open_directory(output)
    try:
        try:
            descriptor = os.open(
                wheel.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode=0o644, dir_fd=directory_fd
            )
        except FileExistsError:
            descriptor = os.open(wheel.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
            with os.fdopen(descriptor, "rb") as handle:
                if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                    raise ValueError("el artefacto existente no es un archivo regular") from None
                if handle.read() != content:
                    raise ValueError("colisión de artefacto; se conservaron los bytes existentes") from None
        else:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return target


def build_wheel(root: Path, python: Path, wheel_dir: Path, temporary: Path, env: Mapping[str, str]) -> Path:
    source = temporary / "source"
    _copy_source(root, source)
    metadata = tomllib.loads((source / "pyproject.toml").read_text(encoding="utf-8"))
    if metadata.get("project", {}).get("name") != "mtf-lab":
        raise ValueError("el proyecto fuente no es MTF Lab")
    _run([str(python), "-c", "import build, setuptools.build_meta"], cwd=temporary, env=env)
    output = temporary / "wheels"
    _run(
        [str(python), "-m", "build", "--wheel", "--no-isolation", "--outdir", str(output), str(source)],
        cwd=temporary,
        env=env,
    )
    wheels = list(output.glob("*.whl"))
    if len(wheels) != 1:
        raise ValueError("el build debe producir exactamente un wheel")
    return _retain_wheel(wheels[0], wheel_dir)


def _target_python(venv: Path, python: Path, temporary: Path, env: Mapping[str, str]) -> Path:
    if venv.is_symlink():
        raise ValueError("el destino venv no puede ser symlink")
    if venv.exists() and not (venv / "pyvenv.cfg").is_file():
        raise ValueError("destino existente no es un entorno virtual; no se modificó")
    if not venv.exists():
        _run([str(python), "-m", "venv", str(venv)], cwd=temporary, env=env)
    target = venv / "bin" / "python"
    observation = json.loads(
        _run(
            [str(target), "-c", "import json,sys; print(json.dumps([sys.prefix,sys.base_prefix]))"],
            cwd=temporary,
            env=env,
        )
    )
    if len(observation) != 2 or Path(observation[0]).resolve() != venv.resolve() or observation[0] == observation[1]:
        raise ValueError("el intérprete destino no pertenece al venv solicitado")
    return target


_INSTALLED_SMOKE = """
import importlib.metadata, json
from pathlib import Path
import mtf_lab
from mtf_lab.configuration import default_state_dir, load_config
print(json.dumps({"module":str(Path(mtf_lab.__file__).resolve()),
                  "version":importlib.metadata.version("mtf-lab"),
                  "state":str(default_state_dir().resolve()),
                  "configuration":load_config().price_base}))
"""


def install_wheel(wheel: Path, venv: Path, python: Path, temporary: Path, env: Mapping[str, str]) -> dict[str, str]:
    version, digest = _wheel_identity(wheel)
    target = _target_python(venv, python, temporary, env)
    _run(
        [str(target), "-m", "pip", "install", "--no-index", "--no-deps", "--force-reinstall", str(wheel)],
        cwd=temporary,
        env=env,
    )
    observed = json.loads(_run([str(target), "-c", _INSTALLED_SMOKE], cwd=temporary, env=env))
    if not Path(observed["module"]).resolve().is_relative_to(venv.resolve()) or observed["version"] != version:
        raise ValueError("el smoke no cargó el wheel desde el venv destino")
    if not Path(observed["state"]).resolve().is_relative_to(temporary.resolve()):
        raise ValueError("el smoke utilizó estado no aislado")
    _run([str(venv / "bin" / "mtf-lab"), "--help"], cwd=temporary, env=env)
    return {
        "INSTALLED_WHEEL": wheel.name,
        "WHEEL_SHA256": digest,
        "WHEEL_PATH": str(wheel),
        "TARGET_PYTHON": str(target),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--venv", default=os.environ.get("MTF_LAB_VENV", str(ROOT / ".venv")), type=Path)
    parser.add_argument("--python", default=os.environ.get("MTF_LAB_BUILD_PYTHON", sys.executable))
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--wheel-dir", type=Path, default=ROOT / "dist")
    args = parser.parse_args(argv)
    try:
        python = _python_executable(args.python)
        with tempfile.TemporaryDirectory(prefix="mtf-wheel-install-") as name:
            temporary = Path(name)
            env = _environment(temporary)
            _run(
                [str(python), "-c", "import sys; assert sys.version_info >= (3,11), 'Python 3.11+ requerido'"],
                cwd=temporary,
                env=env,
            )
            wheel = (
                _retain_wheel(args.wheel.absolute(), args.wheel_dir.absolute())
                if args.wheel is not None
                else build_wheel(ROOT, python, args.wheel_dir.absolute(), temporary, env)
            )
            receipt = install_wheel(wheel, args.venv.absolute(), python, temporary, env)
        for key, value in receipt.items():
            print(f"{key}={value}")
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired, zipfile.BadZipFile) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
