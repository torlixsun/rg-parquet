#!/bin/bash
# ============================================================
# RG Parquet Export — Trigger Script (deploy on b80)
# ============================================================
# Cron: 30 1 * * *  (daily at 01:30 — idempotent, so a missed/failed run
#                    retries the next day; tasks for a month are created once)
#   crontab -e
#   30 1 * * * /path/to/rg_trigger.sh
#
# Readiness gate: queries MySQL solr_info and only dispatches the target
# month once US_D / US_M / INTL_D / INTL_M all have solr_month == target.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load .env
if [ -f "${SCRIPT_DIR}/.env" ]; then
    set -a; source "${SCRIPT_DIR}/.env"; set +a
fi

CONTROLLER_URL="${CONTROLLER_URL:-http://127.0.0.1:5000}"
API_TOKEN="${API_TOKEN:-}"

# MySQL — readiness check (deploy on the MySQL host or with access to it)
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
MYSQL_DATABASE="${MYSQL_DATABASE:-actonia}"

# Logging
LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/trigger_$(date +%Y%m%d_%H%M%S).log"

# Clean up logs older than 30 days
find "$LOG_DIR" -name 'trigger_*.log' -mtime +30 -delete 2>/dev/null || true

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# ---- Calculate target month (last month) ----
TARGET_MONTH=$(date +%Y%m -d "$(date +%Y-%m-01) - 1 month")
log "Target month: ${TARGET_MONTH}"

# ---- Readiness check: MySQL solr_info ----
command -v mysql >/dev/null 2>&1 || {
    log "ERROR: mysql client not installed"
    exit 1
}

rows=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u "${MYSQL_USER}" -p"${MYSQL_PASSWORD}" \
    -D "${MYSQL_DATABASE}" -N -B -e \
    "SELECT country_code FROM solr_info
     WHERE solr_type = 19 AND solr_month = '${TARGET_MONTH}'" 2>>"$LOG_FILE") || {
    log "ERROR: MySQL query failed (host=${MYSQL_HOST} db=${MYSQL_DATABASE})"
    exit 1
}

expected=("US_D" "US_M" "INTL_D" "INTL_M")
ready=true
for code in "${expected[@]}"; do
    if grep -qx "$code" <<< "$rows"; then
        log "  ${code}: ready"
    else
        log "  ${code}: MISSING"
        ready=false
    fi
done

if [ "$ready" = false ]; then
    log "Not ready — some entries not at ${TARGET_MONTH} yet, skipping dispatch"
    exit 0
fi
log "All 4 entries ready for ${TARGET_MONTH}, dispatching"

# ---- POST to controller (idempotent — returns existing if already created) ----
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
