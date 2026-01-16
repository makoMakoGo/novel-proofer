#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[novel-proofer] Working dir: $(pwd)"

MODE="serve"
if [[ "${1:-}" == "--smoke" ]]; then
  MODE="smoke"
fi

VENV_DIR=".venv"
PY_LINUX="$VENV_DIR/bin/python"

PY_CMD=()

# If a Windows venv was copied into WSL, it will not be executable.
if [[ -d "$VENV_DIR" && ! -x "$PY_LINUX" && -f "$VENV_DIR/Scripts/python.exe" ]]; then
  BACKUP_DIR="${VENV_DIR}.win"
  if [[ -e "$BACKUP_DIR" ]]; then
    BACKUP_DIR="${VENV_DIR}.win.$(date +%Y%m%d%H%M%S)"
  fi
  echo "[novel-proofer] Detected Windows venv in $VENV_DIR, moving to $BACKUP_DIR ..."
  mv "$VENV_DIR" "$BACKUP_DIR"
fi

# Prefer uv when available; fall back to pip + requirements.lock.txt.
if command -v uv >/dev/null 2>&1; then
  echo "[novel-proofer] Using: $(uv --version 2>&1)"

  SYNC_ARGS=(sync --frozen --no-install-project)
  if [[ "$MODE" == "serve" ]]; then
    SYNC_ARGS+=(--no-dev)
  else
    SYNC_ARGS+=(--group dev)
  fi

  uv "${SYNC_ARGS[@]}"

  PY_CMD=(uv run --frozen --no-sync python)
  echo "[novel-proofer] Using: $("${PY_CMD[@]}" --version 2>&1)"

  if [[ "$MODE" == "smoke" ]]; then
    echo "[novel-proofer] Running tests..."
    "${PY_CMD[@]}" -m pytest -q
    echo "[novel-proofer] Tests OK."
    exit 0
  fi
else
  NEED_VENV_CREATE=0

  if [[ ! -x "$PY_LINUX" ]]; then
    NEED_VENV_CREATE=1
  elif [[ ! -f "$VENV_DIR/bin/activate" ]]; then
    NEED_VENV_CREATE=1
  elif ! "$PY_LINUX" -m pip --version >/dev/null 2>&1; then
    NEED_VENV_CREATE=1
  fi

  if [[ "$NEED_VENV_CREATE" == "1" ]]; then
    rm -rf "$VENV_DIR"
    BASE_PYTHON=""
    if command -v python3 >/dev/null 2>&1; then
      BASE_PYTHON="python3"
    elif command -v python >/dev/null 2>&1; then
      BASE_PYTHON="python"
    else
      echo "[novel-proofer] Python not found. Please install Python 3.12+."
      exit 1
    fi

    echo "[novel-proofer] .venv not found, creating..."
    "$BASE_PYTHON" -m venv "$VENV_DIR"
  fi

  source "$VENV_DIR/bin/activate"

  PY="$PY_LINUX"
  PY_CMD=("$PY")
  echo "[novel-proofer] Using: $("${PY_CMD[@]}" --version 2>&1)"

  requirements_satisfied() {
    local req_file="$1"
    "$PY" - "$req_file" <<PY
import re
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

try:
    from packaging.requirements import InvalidRequirement, Requirement
except Exception:
    from pip._vendor.packaging.requirements import InvalidRequirement, Requirement

path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

def normalize(raw: str) -> str:
    raw = raw.lstrip("\ufeff").strip()
    raw = re.split(r"\s+#", raw, 1)[0].strip()
    return raw

def is_satisfied(line: str) -> bool:
    if not line or line.startswith("#"):
        return True
    if line.startswith("-"):
        return False
    try:
        req = Requirement(line)
    except InvalidRequirement:
        return False
    if req.marker is not None and not req.marker.evaluate():
        return True
    if getattr(req, "url", None):
        return False
    try:
        v = version(req.name)
    except PackageNotFoundError:
        return False
    if not req.specifier:
        return True
    return req.specifier.contains(v, prereleases=True)

for raw in lines:
    if not is_satisfied(normalize(raw)):
        sys.exit(1)
sys.exit(0)
PY
  }

  requirements_has_entries() {
    local req_file="$1"
    "$PY" - "$req_file" <<PY
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

for raw in lines:
    line = raw.lstrip("\ufeff").strip()
    line = re.split(r"\s+#", line, 1)[0].strip()
    if not line:
        continue
    if line.startswith("#"):
        continue
    sys.exit(0)
sys.exit(1)
PY
  }

  install_requirements() {
    if [[ ! -f "requirements.lock.txt" ]]; then
      echo "[novel-proofer] No requirements.lock.txt, skipping dependency install."
      return 0
    fi
    if ! requirements_has_entries "requirements.lock.txt"; then
      echo "[novel-proofer] requirements.lock.txt has no dependencies, skipping install."
      return 0
    fi
    if requirements_satisfied "requirements.lock.txt"; then
      echo "[novel-proofer] Dependencies already installed."
      return 0
    fi
    echo "[novel-proofer] Installing dependencies from requirements.lock.txt..."
    "$PY" -m pip --disable-pip-version-check install -r requirements.lock.txt
  }

  install_requirements

  if [[ "$MODE" == "smoke" ]]; then
    echo "[novel-proofer] Running tests..."
    "$PY" -m pytest -q
    echo "[novel-proofer] Tests OK."
    exit 0
  fi
fi

HOST="${NP_HOST:-127.0.0.1}"
PORT="${NP_PORT:-18080}"

is_port_free() {
  local candidate="$1"
  "${PY_CMD[@]}" - "$candidate" "$HOST" <<PY
import socket
import sys

port = int(sys.argv[1])
host = sys.argv[2]
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind((host, port))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}

pick_port() {
  local start_port="$1"
  local end_port=$((start_port + 30))
  local p
  for ((p=start_port; p<=end_port; p++)); do
    if is_port_free "$p"; then
      PORT="$p"
      return 0
    fi
  done
  echo "[novel-proofer] No free port found in range ${start_port}..${end_port}."
  return 1
}

pick_port "$PORT"

echo "[novel-proofer] Starting server..."
echo "[novel-proofer] URL: http://${HOST}:${PORT}/"
exec "${PY_CMD[@]}" -m novel_proofer.server --host "$HOST" --port "$PORT"
