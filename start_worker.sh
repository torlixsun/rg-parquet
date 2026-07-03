#!/bin/bash
# ============================================================
# Start Celery Worker on one RG export server
#
# Usage (on each of the 12 servers):
#   ./start_worker.sh lweb-rg-001
# ============================================================
set -euo pipefail

HOSTNAME="${1:-}"
if [ -z "$HOSTNAME" ]; then
    echo "Usage: $0 <hostname>   (e.g. lweb-rg-001)"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Install deps if needed
pip3 install -r requirements_celery.txt --break-system-packages --quiet 2>/dev/null || true

echo "Starting worker: ${HOSTNAME} (queue: ${HOSTNAME})"
exec celery -A rg_celery_app worker \
    -Q "${HOSTNAME}" \
    -n "${HOSTNAME}@%h" \
    -l info \
    --concurrency=1 \
    --max-tasks-per-child=1
