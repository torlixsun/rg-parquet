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

# Logging
LOG_DIR="${SCRIPT_DIR}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/worker_$(date +%Y%m%d_%H%M%S).log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

# ---- Check dependencies ----
for cmd in curl jq aws clickhouse-client; do
    command -v "$cmd" >/dev/null 2>&1 || { log "ERROR: Missing dependency: $cmd"; exit 1; }
done

# ---- Prevent concurrent execution ----
LOCK_FILE="/tmp/rg_worker_${SERVER_NAME}.lock"
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
        log "Another worker instance is running (PID $PID), exiting."
        exit 0
    fi
    log "Stale lock found, removing."
    rm -f "$LOCK_FILE"
fi
echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

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
    -q "SELECT value FROM system.settings WHERE name = 'user_files_path'" 2>/dev/null | head -1 | tr -d '\n')
USER_FILES_DIR="${USER_FILES_DIR:-/var/lib/clickhouse/user_files}"
USER_FILES_DIR="${USER_FILES_DIR%/}"
log "ClickHouse user_files_path: ${USER_FILES_DIR}"

# ============================================================
# Step 1: Send heartbeat
# ============================================================
MY_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "")
curl -sf -X POST "${CONTROLLER_URL}/api/heartbeat" \
    -H "Content-Type: application/json" \
    -H "X-API-Token: ${API_TOKEN}" \
    -d "{\"server\": \"${SERVER_NAME}\", \"hostname\": \"$(hostname)\", \"ip\": \"${MY_IP}\"}" \
    >/dev/null 2>&1 || true

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
    FILE_REL="exports/${TABLE_NAME}/${SERVER_NAME}.parquet"
    FILE_DISK="${USER_FILES_DIR}/${FILE_REL}"
    S3_PATH="s3://rg-datalake-${YEAR}/${TABLE_NAME}/${SERVER_NAME}.parquet"

    log "Processing ${TABLE_NAME}..."

    # ---- Idempotency: skip if already on Seagate ----
    if aws s3 ls "$S3_PATH" --profile "${AWS_PROFILE}" --endpoint-url "${SEAGATE_ENDPOINT}" 2>/dev/null | grep -qF ".parquet"; then
        log "  Already on Seagate, skipping"
        LOCAL_COUNT=$(clickhouse-client --password "${CH_LOCAL_PASSWORD}" \
            -q "SELECT count() FROM ${CH_LOCAL_DB}.local_${TABLE_NAME}" 2>/dev/null | tr -d '\n' || echo 0)
        curl -sf -X POST "${CONTROLLER_URL}/api/tasks/${TASK_ID}/tables" \
            -H "Content-Type: application/json" \
            -H "X-API-Token: ${API_TOKEN}" \
            -d "{\"table_name\": \"${TABLE_NAME}\", \"status\": \"skipped\", \"rows_local\": ${LOCAL_COUNT}, \"rows_s3\": ${LOCAL_COUNT}}" \
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
            --query "INSERT INTO FUNCTION file('${FILE_REL}', Parquet) \
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
            -d "{\"table_name\": \"${TABLE_NAME}\", \"status\": \"failed\", \"error_msg\": \"export failed after 5 retries\"}" \
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
            -d "{\"table_name\": \"${TABLE_NAME}\", \"status\": \"failed\", \"error_msg\": \"upload failed after 5 retries\"}" \
            >/dev/null 2>&1 || true
        FAILED_TABLES=$((FAILED_TABLES + 1))
        rm -rf "$(dirname "$FILE_DISK")" 2>/dev/null || true
        continue
    fi
    log "  Upload OK"

    # ---- Row count validation ----
    LOCAL_COUNT=$(clickhouse-client --password "${CH_LOCAL_PASSWORD}" \
        -q "SELECT count() FROM ${CH_LOCAL_DB}.local_${TABLE_NAME}" 2>/dev/null | tr -d '\n' || echo 0)
    EXPORT_COUNT=$(clickhouse-client --password "${CH_LOCAL_PASSWORD}" \
        --query "SELECT count() FROM file('${FILE_REL}', Parquet)" 2>/dev/null | tr -d '\n' || echo 0)

    if [ "$LOCAL_COUNT" = "$EXPORT_COUNT" ]; then
        log "  Validation OK: ${LOCAL_COUNT} rows"
        curl -sf -X POST "${CONTROLLER_URL}/api/tasks/${TASK_ID}/tables" \
            -H "Content-Type: application/json" \
            -H "X-API-Token: ${API_TOKEN}" \
            -d "{\"table_name\": \"${TABLE_NAME}\", \"status\": \"complete\", \"rows_local\": ${LOCAL_COUNT}, \"rows_s3\": ${EXPORT_COUNT}}" \
            >/dev/null 2>&1 || true
    else
        log "  Validation MISMATCH: local=${LOCAL_COUNT} export=${EXPORT_COUNT}"
        curl -sf -X POST "${CONTROLLER_URL}/api/tasks/${TASK_ID}/tables" \
            -H "Content-Type: application/json" \
            -H "X-API-Token: ${API_TOKEN}" \
            -d "{\"table_name\": \"${TABLE_NAME}\", \"status\": \"failed\", \"rows_local\": ${LOCAL_COUNT}, \"rows_s3\": ${EXPORT_COUNT}, \"error_msg\": \"row count mismatch\"}" \
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
    curl -sf -X PATCH "${CONTROLLER_URL}/api/tasks/${TASK_ID}" \
        -H "Content-Type: application/json" \
        -H "X-API-Token: ${API_TOKEN}" \
        -d "{\"status\": \"complete\", \"server\": \"${SERVER_NAME}\"}" >/dev/null 2>&1 || true
else
    log "Task completed with ${FAILED_TABLES} failed tables"
    curl -sf -X PATCH "${CONTROLLER_URL}/api/tasks/${TASK_ID}" \
        -H "Content-Type: application/json" \
        -H "X-API-Token: ${API_TOKEN}" \
        -d "{\"status\": \"failed\", \"server\": \"${SERVER_NAME}\", \"error_msg\": \"${FAILED_TABLES} tables failed\"}" >/dev/null 2>&1 || true
fi

log "Worker done."
