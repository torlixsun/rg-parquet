#!/bin/bash
# ============================================================
# Start Coordinator components: Celery Worker + Flask API
# Run on the coordinator machine (needs Redis + ClickHouse access).
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

pip3 install -r requirements_celery.txt --break-system-packages --quiet 2>/dev/null || true

# ---- Read PORT from .env ----
if [ -f "${SCRIPT_DIR}/.env" ]; then
    # shellcheck disable=SC1090
    set -a; source "${SCRIPT_DIR}/.env"; set +a
fi
PORT="${PORT:-5000}"

# ---- Auto-detect available port ----
port_in_use() {
    ss -tlnp 2>/dev/null | grep -q ":${1} " || lsof -i :"${1}" &>/dev/null
}

if port_in_use "$PORT"; then
    echo "Port $PORT is in use, searching for next available..."
    for p in $(seq 5001 5099); do
        if ! port_in_use "$p"; then
            PORT="$p"
            break
        fi
    done
    echo "Using port $PORT"
fi
export PORT

echo "=== Starting Coordinator Celery Worker ==="
celery -A rg_celery_app worker -Q coordinator -n coordinator@%h -l info --concurrency=1 &
WORKER_PID=$!

echo "=== Starting Flask Status API ==="
python3 rg_celery_coordinator.py &
API_PID=$!

trap "kill $WORKER_PID $API_PID 2>/dev/null; exit" INT TERM

echo ""
echo "Coordinator running:"
echo "  Worker PID: $WORKER_PID"
echo "  API PID:    $API_PID (port $PORT)"
echo "  Dashboard:  http://$(hostname -I 2>/dev/null | awk '{print $1}'):${PORT}/dashboard"
echo ""
wait
