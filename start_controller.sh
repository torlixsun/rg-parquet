#!/bin/bash
# ============================================================
# Start RG Export Controller (deploy on tools-1)
# ============================================================
# Starts the Flask API + background thread (timeout/finalize).
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ---- Prevent duplicate controller instances ----
PID_FILE="${SCRIPT_DIR}/rg_controller.pid"
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE" 2>/dev/null)
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Controller is already running (PID $OLD_PID). Stop it first or remove $PID_FILE."
        exit 1
    fi
    echo "Removing stale PID file."
    rm -f "$PID_FILE"
fi
echo $$ > "$PID_FILE"
trap 'rm -f "$PID_FILE"' EXIT

# Install dependencies
pip3 install -r requirements_polling.txt --break-system-packages --quiet 2>/dev/null || true

# Read config from .env
if [ -f "${SCRIPT_DIR}/.env" ]; then
    set -a; source "${SCRIPT_DIR}/.env"; set +a
fi
PORT="${PORT:-5000}"

echo "=== Starting RG Export Controller ==="
echo "  Port:      ${PORT}"
echo "  Dashboard: http://0.0.0.0:${PORT}/dashboard"
echo "  API:       http://0.0.0.0:${PORT}/api/status"
echo ""

exec python3 rg_controller.py
