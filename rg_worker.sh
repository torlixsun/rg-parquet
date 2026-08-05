#!/bin/bash
# ============================================================
# RG Parquet Export — Worker Script (deploy on each lweb-rg)
# ============================================================
# Cron: */5 * * * *  (every 5 minutes)
#   crontab -e
#   */5 * * * * /path/to/rg_worker.sh
#
# Flow:
#   1. Send heartbeat to controller
#   2. Poll for new task (status=new, server=<hostname>)
#   3. Atomically claim task (new → progress)
#   4. Export 16 tables → Parquet → upload to Seagate
#   5. Report table-level status to controller
#   6. Mark task complete or failed
#
# If a task is already in progress (e.g., previous cron still running),
# this script exits early — only one task per server at a time.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Load .env
if [ -f "${SCRIPT_DIR}/.env" ]; then
    set -a; source "${SCRIPT_DIR}/.env"; set +a
fi

CONTROLLER_URL="${CONTROLLER_URL:-http://127.0.0.1:5000}"
SERVER_NAME=$(hostname -s)
CH_LOCAL_PASSWORD="${CH_LOCAL_PASSWORD:-}"
CH_LOCAL_DB="${CH_LOCAL_DB:-monthly_ranking}"
AWS_PROFILE="${AWS_PROFILE:-seagate}"
SEAGATE_ENDPOINT="${SEAGATE_ENDPOINT:-https://s3.us-east-1.clarity1.lyve.seagate.com}"
API_TOKEN="${API_TOKEN:-}"
# Where temporary parquet files are written on this worker.
# Empty = <ClickHouse user_files_path>/exports (default, always works).
# Set e.g. EXPORT_DIR=/data/rg-exports for a larger disk; if that path is
# outside user_files_path, the clickhouse user must be able to write it and
# the server must allow the path for the file() function.
EXPORT_DIR="${EXPORT_DIR:-}"

# Logging
LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/worker_$(date +%Y%m%d_%H%M%S).log"

# Clean up logs older than 30 days
find "$LOG_DIR" -name 'worker_*.log' -mtime +30 -delete 2>/dev/null || true
find "$LOG_DIR" -name 'trigger_*.log' -mtime +30 -delete 2>/dev/null || true

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# ---- Check dependencies ----
for cmd in curl jq aws clickhouse-client; do
    command -v "$cmd" >/dev/null 2>&1 || { log "ERROR: Missing dependency: $cmd"; exit 1; }
done

# ---- Prevent concurrent execution ----
LOCK_DIR="/tmp/rg_worker_${SERVER_NAME}.lock"
if [ -d "$LOCK_DIR" ]; then
    # The lock owner is recorded as a PID file, so long-running exports (up to
    # 48h) are never mistaken for stale locks. Only a dead/missing owner is stale.
    LOCK_PID="$(cat "$LOCK_DIR/pid" 2>/dev/null || echo '')"
    if [ -n "$LOCK_PID" ] && kill -0 "$LOCK_PID" 2>/dev/null; then
        log "Another worker instance is running (PID ${LOCK_PID}), exiting."
        exit 0
    fi
    log "Removing stale lock (owner PID ${LOCK_PID:-unknown} is not running)"
    rm -f "$LOCK_DIR/pid"
    rmdir "$LOCK_DIR" 2>/dev/null || true
fi
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    log "Race: another worker claimed the lock, exiting."
    exit 0
fi
echo $$ > "$LOCK_DIR/pid"
trap 'rm -f "$LOCK_DIR/pid"; rmdir "$LOCK_DIR" 2>/dev/null || true' EXIT

# ---- 16 table templates ----
TABLES=(
    "d_ranking_detail_{m}_intl"
    "d_ranking_detail_{m}_us"
    "d_ranking_info_{m}_intl"
    "d_ranking_info_{m}_us"
    "d_ranking_subrank_{m}_intl"
    "d_ranking_subrank_{m}_us"
    "d_ranking_url_{m}_intl"
    "d_ranking_url_{m}_us"
    "m_ranking_detail_{m}_intl"
    "m_ranking_detail_{m}_us"
    "m_ranking_info_{m}_intl"
    "m_ranking_info_{m}_us"
    "m_ranking_subrank_{m}_intl"
    "m_ranking_subrank_{m}_us"
    "m_ranking_url_{m}_intl"
    "m_ranking_url_{m}_us"
)

# ---- Get ClickHouse user_files path ----
USER_FILES_DIR=$(clickhouse-client --password "${CH_LOCAL_PASSWORD}" \
    -q "SELECT value FROM system.settings WHERE name = 'user_files_path'" 2>/dev/null | head -1 | tr -d '\n' || true)
USER_FILES_DIR="${USER_FILES_DIR:-/var/lib/clickhouse/user_files}"
USER_FILES_DIR="${USER_FILES_DIR%/}"
log "ClickHouse user_files_path: ${USER_FILES_DIR}"

# Resolve the export base directory + the path ClickHouse's file() should use.
if [ -n "${EXPORT_DIR}" ]; then
    EXPORT_DIR="${EXPORT_DIR%/}"
    case "${EXPORT_DIR}" in
        "${USER_FILES_DIR}"|"${USER_FILES_DIR}/"*)
            FILE_BASE_DIR="${EXPORT_DIR}"
            FILE_BASE_REL="${EXPORT_DIR#${USER_FILES_DIR}/}"
            ;;
        *)
            FILE_BASE_DIR="${EXPORT_DIR}"
            FILE_BASE_REL=""
            log "EXPORT_DIR (${EXPORT_DIR}) is outside user_files_path (${USER_FILES_DIR}); ClickHouse file() must allow this path"
            ;;
    esac
else
    FILE_BASE_DIR="${USER_FILES_DIR}/exports"
    FILE_BASE_REL="exports"
fi
log "Parquet export dir: ${FILE_BASE_DIR}"

# Run a ClickHouse count query. Prints the count on success, nothing on
# failure (caller decides how to treat an unreadable count — never fake 0).
ch_count() {
    local sql="$1"
    local out
    out=$(clickhouse-client --password "${CH_LOCAL_PASSWORD}" -q "$sql" 2>>"$LOG_FILE") || return 1
    printf '%s' "$out"
}

# ============================================================
# Step 1: Send heartbeat
# ============================================================
MY_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "")
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${CONTROLLER_URL}/api/heartbeat" \
    -H "Content-Type: application/json" \
    -H "X-API-Token: ${API_TOKEN}" \
    -d "{\"server\": \"${SERVER_NAME}\", \"hostname\": \"$(hostname)\", \"ip\": \"${MY_IP}\"}" \
    2>/dev/null || echo "000")
if [ "$HTTP_CODE" = "200" ]; then
    log "Heartbeat OK (${SERVER_NAME})"
else
    log "WARNING: Heartbeat failed (HTTP ${HTTP_CODE}) to ${CONTROLLER_URL} — worker may appear offline"
fi

# ============================================================
# Step 2: Poll for new task
# ============================================================
TASKS_JSON=$(curl -sf "${CONTROLLER_URL}/api/tasks?server=${SERVER_NAME}&status=new" 2>/dev/null || echo '{"tasks":[]}')
TASK_ID=$(echo "$TASKS_JSON" | jq -r '.tasks[0].id // empty' 2>/dev/null)

if [ -z "$TASK_ID" ]; then
    log "No new tasks for ${SERVER_NAME}"
    exit 0
fi

log "Found new task: ${TASK_ID}"

# ============================================================
# Step 3: Atomically claim task (new → progress)
# ============================================================
CLAIM_JSON=$(curl -sf -X PATCH "${CONTROLLER_URL}/api/tasks/${TASK_ID}" \
    -H "Content-Type: application/json" \
    -H "X-API-Token: ${API_TOKEN}" \
    -d "{\"status\": \"progress\", \"server\": \"${SERVER_NAME}\"}" 2>/dev/null || echo '{"claimed":false}')

CLAIMED=$(echo "$CLAIM_JSON" | jq -r '.claimed // false' 2>/dev/null)

if [ "$CLAIMED" != "true" ]; then
    log "Could not claim task ${TASK_ID} (already claimed or not in 'new' state)"
    exit 0
fi

MONTH=$(echo "$CLAIM_JSON" | jq -r '.task.month')
YEAR="${MONTH:0:4}"
log "Claimed task ${TASK_ID} for month ${MONTH}"

# ============================================================
# Step 4: Export 16 tables
# ============================================================
FAILED_TABLES=0

for template in "${TABLES[@]}"; do
    TABLE_NAME="${template/\{m\}/$MONTH}"
    FILE_DISK="${FILE_BASE_DIR}/${TABLE_NAME}/${SERVER_NAME}.parquet"
    if [ -n "${FILE_BASE_REL}" ]; then
        CH_FILE_PATH="${FILE_BASE_REL}/${TABLE_NAME}/${SERVER_NAME}.parquet"
    else
        CH_FILE_PATH="${FILE_DISK}"
    fi
    S3_PATH="s3://rg-datalake-${YEAR}/${TABLE_NAME}/${SERVER_NAME}.parquet"

    log "Processing ${TABLE_NAME}..."

    # ---- Idempotency: skip if already on Seagate ----
    if aws s3 ls "$S3_PATH" --profile "${AWS_PROFILE}" --endpoint-url "${SEAGATE_ENDPOINT}" 2>/dev/null | grep -qF ".parquet"; then
        log "  Already on Seagate, skipping"
        LOCAL_COUNT=$(ch_count "SELECT count() FROM ${CH_LOCAL_DB}.local_${TABLE_NAME}") || LOCAL_COUNT=0
        curl -sf -X POST "${CONTROLLER_URL}/api/tasks/${TASK_ID}/tables" \
            -H "Content-Type: application/json" \
            -H "X-API-Token: ${API_TOKEN}" \
            -d "{\"table_name\": \"${TABLE_NAME}\", \"server\": \"${SERVER_NAME}\", \"status\": \"skipped\", \"rows_local\": ${LOCAL_COUNT}, \"rows_s3\": ${LOCAL_COUNT}}" \
            >/dev/null 2>&1 || true
        continue
    fi

    # ---- Prepare directory ----
    mkdir -pv "$(dirname "$FILE_DISK")" 2>/dev/null || true
    chown -R clickhouse:clickhouse "$(dirname "$FILE_DISK")" 2>/dev/null || true

    # ---- Export to Parquet (5 retries) ----
    EXPORT_OK=0
    for attempt in $(seq 1 5); do
        if clickhouse-client --password "${CH_LOCAL_PASSWORD}" \
            --query "INSERT INTO FUNCTION file('${CH_FILE_PATH}', Parquet) \
                SELECT * FROM ${CH_LOCAL_DB}.local_${TABLE_NAME} \
                SETTINGS output_format_parquet_row_group_size = 1000000, \
                         output_format_parquet_compression_method = 'zstd', \
                         max_memory_usage = 40000000000" 2>>"$LOG_FILE"; then
            EXPORT_OK=1
            break
        fi
        log "  Export attempt ${attempt}/5 failed, retrying in 10s..."
        sleep 10
    done

    if [ $EXPORT_OK -eq 0 ]; then
        log "  Export FAILED after 5 retries"
        curl -sf -X POST "${CONTROLLER_URL}/api/tasks/${TASK_ID}/tables" \
            -H "Content-Type: application/json" \
            -H "X-API-Token: ${API_TOKEN}" \
            -d "{\"table_name\": \"${TABLE_NAME}\", \"server\": \"${SERVER_NAME}\", \"status\": \"failed\", \"error_msg\": \"export failed after 5 retries\"}" \
            >/dev/null 2>&1 || true
        FAILED_TABLES=$((FAILED_TABLES + 1))
        rm -rf "$(dirname "$FILE_DISK")" 2>/dev/null || true
        continue
    fi
    log "  Export OK"

    # ---- Upload to Seagate (5 retries) ----
    UPLOAD_OK=0
    for attempt in $(seq 1 5); do
        if aws s3 cp "${FILE_DISK}" "$S3_PATH" \
            --profile "${AWS_PROFILE}" --endpoint-url "${SEAGATE_ENDPOINT}" 2>>"$LOG_FILE"; then
            UPLOAD_OK=1
            break
        fi
        log "  Upload attempt ${attempt}/5 failed, retrying in 10s..."
        sleep 10
    done

    if [ $UPLOAD_OK -eq 0 ]; then
        log "  Upload FAILED after 5 retries"
        curl -sf -X POST "${CONTROLLER_URL}/api/tasks/${TASK_ID}/tables" \
            -H "Content-Type: application/json" \
            -H "X-API-Token: ${API_TOKEN}" \
            -d "{\"table_name\": \"${TABLE_NAME}\", \"server\": \"${SERVER_NAME}\", \"status\": \"failed\", \"error_msg\": \"upload failed after 5 retries\"}" \
            >/dev/null 2>&1 || true
        FAILED_TABLES=$((FAILED_TABLES + 1))
        rm -rf "$(dirname "$FILE_DISK")" 2>/dev/null || true
        continue
    fi
    log "  Upload OK"

    # ---- Row count validation ----
    LOCAL_COUNT=$(ch_count "SELECT count() FROM ${CH_LOCAL_DB}.local_${TABLE_NAME}") || LOCAL_COUNT=""
    EXPORT_COUNT=$(ch_count "SELECT count() FROM file('${CH_FILE_PATH}', Parquet)") || EXPORT_COUNT=""

    if [ -n "$LOCAL_COUNT" ] && [ -n "$EXPORT_COUNT" ] && [ "$LOCAL_COUNT" = "$EXPORT_COUNT" ]; then
        log "  Validation OK: ${LOCAL_COUNT} rows"
        curl -sf -X POST "${CONTROLLER_URL}/api/tasks/${TASK_ID}/tables" \
            -H "Content-Type: application/json" \
            -H "X-API-Token: ${API_TOKEN}" \
            -d "{\"table_name\": \"${TABLE_NAME}\", \"server\": \"${SERVER_NAME}\", \"status\": \"complete\", \"rows_local\": ${LOCAL_COUNT}, \"rows_s3\": ${EXPORT_COUNT}}" \
            >/dev/null 2>&1 || true
    else
        log "  Validation FAILED: local=${LOCAL_COUNT:-?} export=${EXPORT_COUNT:-?}"
        curl -sf -X POST "${CONTROLLER_URL}/api/tasks/${TASK_ID}/tables" \
            -H "Content-Type: application/json" \
            -H "X-API-Token: ${API_TOKEN}" \
            -d "{\"table_name\": \"${TABLE_NAME}\", \"server\": \"${SERVER_NAME}\", \"status\": \"failed\", \"rows_local\": ${LOCAL_COUNT:-0}, \"rows_s3\": ${EXPORT_COUNT:-0}, \"error_msg\": \"row count mismatch or count query failed\"}" \
            >/dev/null 2>&1 || true
        FAILED_TABLES=$((FAILED_TABLES + 1))
    fi

    # ---- Cleanup local file ----
    rm -rf "$(dirname "$FILE_DISK")" 2>/dev/null || true

done

# ============================================================
# Step 5: Mark task complete or failed
# ============================================================
if [ $FAILED_TABLES -eq 0 ]; then
    log "All 16 tables exported successfully"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PATCH "${CONTROLLER_URL}/api/tasks/${TASK_ID}" \
        -H "Content-Type: application/json" \
        -H "X-API-Token: ${API_TOKEN}" \
        -d "{\"status\": \"complete\", \"server\": \"${SERVER_NAME}\"}" 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" -ge 400 ] 2>/dev/null || [ "$HTTP_CODE" = "000" ]; then
        log "  WARNING: Final complete PATCH returned HTTP ${HTTP_CODE} — task may hang in 'progress'"
    fi
else
    log "Task completed with ${FAILED_TABLES} failed tables"
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PATCH "${CONTROLLER_URL}/api/tasks/${TASK_ID}" \
        -H "Content-Type: application/json" \
        -H "X-API-Token: ${API_TOKEN}" \
        -d "{\"status\": \"failed\", \"server\": \"${SERVER_NAME}\", \"error_msg\": \"${FAILED_TABLES} tables failed\"}" 2>/dev/null || echo "000")
    if [ "$HTTP_CODE" -ge 400 ] 2>/dev/null || [ "$HTTP_CODE" = "000" ]; then
        log "  WARNING: Final failed PATCH returned HTTP ${HTTP_CODE} — task may hang in 'progress'"
    fi
fi

log "Worker done."
