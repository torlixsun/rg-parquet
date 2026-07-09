#!/bin/bash
# ============================================================
# Start Coordinator components: Celery Worker + Flask API
# Run on the coordinator machine (needs Redis + ClickHouse access).
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

pip3 install -r requirements_celery.txt --break-system-packages --quiet 2>/dev/null || true

echo "=== Starting Coordinator Celery Worker ==="
celery -A rg_celery_app worker -Q coordinator -n coordinator@%h -l info --concurrency=1 &
WORKER_PID=$!

echo "=== Starting Flask Status API ==="
export PORT="${PORT:-5000}"
python3 rg_celery_coordinator.py &
API_PID=$!

trap "kill $WORKER_PID $API_PID 2>/dev/null; exit" INT TERM

echo ""
echo "Coordinator running:"
echo "  Worker PID: $WORKER_PID"
echo "  API PID:    $API_PID (port $PORT)"
echo ""
wait
