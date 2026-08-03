#!/bin/bash
# ============================================================
# RG Parquet Export — Trigger Script (deploy on b80)
# ============================================================
# Cron: 0 1 1 * *  (1st of each month at 01:00)
#   crontab -e
#   0 1 1 * * /path/to/rg_trigger.sh
#
# Idempotent — controller skips if tasks already exist for the month.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load .env
if [ -f "${SCRIPT_DIR}/.env" ]; then
    set -a; source "${SCRIPT_DIR}/.env"; set +a
fi

CONTROLLER_URL="${CONTROLLER_URL:-http://127.0.0.1:5000}"
API_TOKEN="${API_TOKEN:-}"

# Logging
LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/trigger_$(date +%Y%m%d_%H%M%S).log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# Calculate target month (last month)
TARGET_MONTH=$(date +%Y%m -d "$(date +%Y-%m-01) - 1 month")
log "Trigger dispatching for month: ${TARGET_MONTH}"

# POST to controller (idempotent — returns existing if already created)
RESPONSE=$(curl -sf -X POST "${CONTROLLER_URL}/api/tasks" \
    -H "Content-Type: application/json" \
    -H "X-API-Token: ${API_TOKEN}" \
    -d "{\"month\": \"${TARGET_MONTH}\"}" 2>&1) || {
    log "ERROR: Failed to reach controller at ${CONTROLLER_URL}"
    log "Response: ${RESPONSE}"
    exit 1
}

log "Response: ${RESPONSE}"

# Parse result
if command -v jq >/dev/null 2>&1; then
    CREATED=$(echo "$RESPONSE" | jq -r '.created // 0' 2>/dev/null || echo 0)
    if [ "$CREATED" -gt 0 ]; then
        log "Successfully created ${CREATED} tasks for ${TARGET_MONTH}"
    else
        log "Tasks already exist for ${TARGET_MONTH} (skipped)"
    fi
else
    log "jq not found — cannot parse response, check controller dashboard manually"
fi

log "Trigger done."
