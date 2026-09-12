#!/usr/bin/env bash
set -euo pipefail

# Build and install the project wheel without reaching a package index.
# The build frontend is intentionally supplied by the caller's tooling
# environment (normally .venv-dev); the target venv only receives the wheel.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_PYTHON="${MTF_LAB_BUILD_PYTHON:-${PYTHON:-python3}}"
TARGET_VENV="${MTF_LAB_VENV:-${ROOT}/.venv}"
WHEEL=""

usage() {
    cat <<'EOF'
Uso: install-mtf-lab-wheel.sh [--venv PATH] [--python PATH] [--wheel PATH]

Construye (o recibe) un wheel PEP 517 y lo instala en un entorno virtual.
No publica el artefacto ni consulta un índice de paquetes.

Variables equivalentes: MTF_LAB_VENV y MTF_LAB_BUILD_PYTHON.
EOF
}

while (($#)); do
    case "$1" in
        --venv)
            [[ $# -ge 2 ]] || { echo "--venv requiere PATH" >&2; exit 2; }
            TARGET_VENV="$2"
            shift 2
            ;;
        --python)
            [[ $# -ge 2 ]] || { echo "--python requiere PATH" >&2; exit 2; }
            BUILD_PYTHON="$2"
            shift 2
            ;;
        --wheel)
            [[ $# -ge 2 ]] || { echo "--wheel requiere PATH" >&2; exit 2; }
            WHEEL="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "opción no reconocida: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

[[ -x "$BUILD_PYTHON" || "$(command -v "$BUILD_PYTHON" 2>/dev/null || true)" ]] || {
    echo "No se encontró el intérprete de build: $BUILD_PYTHON" >&2
    exit 1
}

"$BUILD_PYTHON" - <<'PY'
import sys

if sys.version_info < (3, 11):
    raise SystemExit("MTF Lab requiere Python 3.11 o posterior")
PY

if [[ -z "$WHEEL" ]]; then
    command -v "$BUILD_PYTHON" >/dev/null 2>&1 || [[ -x "$BUILD_PYTHON" ]] || {
        echo "No se encontró el intérprete de build: $BUILD_PYTHON" >&2
        exit 1
    }
    if ! "$BUILD_PYTHON" -c 'import build' >/dev/null 2>&1; then
        echo "Falta el frontend oficial 'build'; instala requirements-dev.lock en el entorno de build" >&2
        exit 1
    fi
    BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mtf-lab-wheel.XXXXXX")"
    cleanup() { rm -rf "$BUILD_DIR"; }
    trap cleanup EXIT
    SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-0}" "$BUILD_PYTHON" -m build --wheel --no-isolation --outdir "$BUILD_DIR" "$ROOT"
    WHEEL="$(find "$BUILD_DIR" -maxdepth 1 -type f -name 'mtf_lab-*.whl' -print -quit)"
fi

[[ -f "$WHEEL" ]] || { echo "Wheel no encontrado: $WHEEL" >&2; exit 1; }
case "$(basename "$WHEEL")" in
    mtf_lab-*.whl) ;;
    *) echo "Nombre de wheel inesperado: $WHEEL" >&2; exit 1 ;;
esac

if [[ ! -x "$TARGET_VENV/bin/python" ]]; then
    "$BUILD_PYTHON" -m venv "$TARGET_VENV"
fi

TARGET_PYTHON="$TARGET_VENV/bin/python"
"$TARGET_PYTHON" -m pip install --no-deps --disable-pip-version-check --force-reinstall "$WHEEL"
"$TARGET_PYTHON" -m mtf_lab --help >/dev/null

printf 'INSTALLED_WHEEL=%s\n' "$(basename "$WHEEL")"
printf 'WHEEL_SHA256=%s\n' "$(sha256sum "$WHEEL" | awk '{print $1}')"
printf 'TARGET_PYTHON=%s\n' "$TARGET_PYTHON"
