#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# --python selects the separate build frontend; this stdlib helper needs 3.11+.
exec "${PYTHON:-python3}" "$ROOT/tools/wheel_installer.py" "$@"
