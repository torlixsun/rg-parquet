#!/bin/bash
# ============================================================
# Start RG Export Controller (deploy on tools-1)
# ============================================================
# Starts the Flask API + background thread (timeout/finalize).
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

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
