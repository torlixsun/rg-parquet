#!/bin/bash
# ============================================================
# Start Master components: Celery Beat + Flask Status API
#
# Run on the coordinator/master machine.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Install deps if needed
pip3 install -r requirements_celery.txt --break-system-packages --quiet 2>/dev/null || true

echo "=== Starting Celery Beat (scheduler) ==="
celery -A rg_celery_app beat -l info &
BEAT_PID=$!

echo "=== Starting Flask Status API ==="
export PORT="${PORT:-5000}"
python3 rg_celery_coordinator.py &
API_PID=$!

trap "kill $BEAT_PID $API_PID 2>/dev/null; exit" INT TERM

echo ""
echo "Master running:"
echo "  Beat PID:  $BEAT_PID"
echo "  API PID:   $API_PID (port $PORT)"
echo ""
wait
