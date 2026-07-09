#!/bin/bash
# ============================================================
# MySQL Trigger Script — run on MySQL server
#
# Usage (via crontab, every 10 minutes):
#   */10 * * * * /path/to/trigger_dispatch.sh >> /var/log/rg_trigger.log 2>&1
#
# Queries MySQL solr_info. When all 4 required rows have
# solr_month == current YYYYMM, calls coordinator to dispatch.
# Tracks last dispatched month locally to avoid duplicate triggers.
# ============================================================
set -euo pipefail

# ---- Config ----
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-}"
MYSQL_DATABASE="${MYSQL_DATABASE:-actonia}"
COORDINATOR_URL="${COORDINATOR_URL:-http://127.0.0.1:5000}"
STATE_FILE="${STATE_FILE:-/tmp/rg_trigger_state.txt}"

current_month=$(date +%Y%m)

echo "=== $(date) ==="
echo "Current month: ${current_month}"

# ---- Query MySQL ----
rows=$(mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u "${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -D "${MYSQL_DATABASE}" -N -B -e \
    "SELECT id, country_code, solr_month FROM solr_info WHERE solr_month >= 201901 AND solr_type = 19 ORDER BY id")

if [ -z "$rows" ]; then
    echo "No solr_info rows found, not ready"
    exit 0
fi

echo "solr_info rows:"
echo "$rows"

# ---- Check: US_D, US_M, INTL_D, INTL_M all have solr_month == current ----
declare -A solr_months
while IFS=$'\t' read -r id code month; do
    solr_months["$code"]="$month"
done <<< "$rows"

expected=("US_D" "US_M" "INTL_D" "INTL_M")
ready=true
for code in "${expected[@]}"; do
    m="${solr_months[$code]:-}"
    echo "  ${code}: ${m:-MISSING}"
    if [ "$m" != "$current_month" ]; then
        ready=false
    fi
done

if [ "$ready" = false ]; then
    echo "Not ready — some entries not yet at ${current_month}"
    exit 0
fi

# ---- Local idempotency: don't re-trigger same month ----
last_triggered=$(cat "$STATE_FILE" 2>/dev/null || echo "")
if [ "$last_triggered" = "$current_month" ]; then
    echo "Already triggered for ${current_month}, skipping (local state: $STATE_FILE)"
    exit 0
fi

# ---- Trigger dispatch ----
echo "Triggering dispatch for ${current_month} ..."
resp=$(curl -sS -X POST "${COORDINATOR_URL}/api/dispatch?month=${current_month}" 2>&1)
echo "Response: ${resp}"

# Coordinator also checks idempotency — if "rejected", don't save state
if echo "$resp" | grep -q '"dispatched"'; then
    echo "$current_month" > "$STATE_FILE"
    echo "Dispatch triggered successfully for ${current_month}"
elif echo "$resp" | grep -q '"rejected"'; then
    # Still save state to avoid hammering the API
    echo "$current_month" > "$STATE_FILE"
    echo "Rejected by coordinator (already dispatched/completed)"
else
    echo "Unexpected response from coordinator — state NOT saved, will retry next run"
fi
