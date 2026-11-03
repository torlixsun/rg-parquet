# RG Export Orchestrator

Polling-based distributed export pipeline for ResearchGrid (RG) monthly ranking data.

Exports 16 ClickHouse tables per server (× 12 servers `lweb-rg-001` ~ `012`) to Parquet → uploads to Seagate S3 → validates row counts → cross-checks local vs cloud → sends alerts.

Optionally mirrors the exported Parquet files from Seagate S3 into a second S3-compatible cloud (MinIO / Cloudflare R2 / Aliyun OSS / Tencent COS …), with a WHERE-filtered `count()` check on either side — triggered manually from the dashboard or the API.

> **Architecture**: Flask + SQLite controller with worker cron scripts. No Celery, no Redis.

## Architecture

```
┌──────────────┐   daily cron (01:30)          ┌────────────────────────────┐
│  b80 trigger │ ── MySQL solr_info ready? ──▶ │         Controller           │
│ rg_trigger.sh│   └ yes → POST /api/tasks     │  Flask API + SQLite          │
└──────────────┘        (idempotent)           │  rg_export.db                │
                                               │  state machine + finalize    │
                                               │  + dashboard at /dashboard   │
                                               └──────────────┬───────────────┘
        ┌──────────────────────────────────────────────────────┼───────────────────────────┐
        │  workers poll /api/tasks?server=<me>&status=new every 5 min                       │
        ▼              ▼              ▼                       ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐        ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ lweb-rg-001 │ │ lweb-rg-002 │ │ lweb-rg-003 │   ...  │ lweb-rg-011 │ │ lweb-rg-012 │ │ ...         │
│ rg_worker.sh│ │             │ │             │        │             │ │             │ │             │
│ export 16   │ │ export 16   │ │ export 16   │        │ export 16   │ │ export 16   │ │             │
│ tables→S3   │ │ tables→S3   │ │ tables→S3   │        │ tables→S3   │ │ tables→S3   │ │             │
└──────┬──────┘ └──────┬──────┘ └──────┬──────┘        └──────┬──────┘ └──────┬──────┘ └─────────────┘
       │               │               │                     │               │
       └───────────────┴───────┬───────┴─────────────────────┴───────────────┘
                               ▼
                 ┌──────────────────────────┐
                 │       Seagate S3         │
                 │ s3://rg-datalake-{year}/ │
                 │  {table}/{server}.parquet│
                 └──────────────────────────┘
                               ▲
         all 12 done ──▶ controller auto-finalize (background thread)
                               │
                 comparison: local CH vs S3 via s3()  ──▶ alert
```

## Task lifecycle

Each month creates 12 tasks (one per server), tracked in SQLite:

```
new ──▶ progress ──▶ complete
          │  └─────▶ failed
          └────────▶ timeout (48h, needs manual /api/tasks/<id>/reset)
```

- **Trigger** (`rg_trigger.sh`): daily cron. Before dispatching, it queries MySQL `solr_info` and only creates tasks for the target month once `US_D` / `US_M` / `INTL_D` / `INTL_M` all have `solr_month == target`. Idempotent — if tasks already exist, the controller returns them instead of duplicating, so a missed or failed run retries the next day.
- **Controller** (`rg_controller.py`): serves the API, auto-finalizes when all 12 tasks are done, compares local ClickHouse vs Seagate S3 row counts, alerts on success/mismatch/failure, and marks long-running tasks as `timeout` after `TASK_TIMEOUT_HOURS`.
- **Worker** (`rg_worker.sh`): cron every 5 min on each server. Heartbeat → poll for its own `new` task → atomically claim (`WHERE status='new'`) → export 16 tables to Parquet (`FUNCTION file()`, zstd) → upload to Seagate (5× retry) → validate row counts → report per-table status → mark task complete/failed.

## Anti-duplication

| Layer | Mechanism |
|-------|-----------|
| Controller | `UNIQUE(month, server)` — tasks for a month are created exactly once |
| Worker | Skips tables already present on Seagate (`aws s3 ls`) |
| Trigger | Re-POSTing an existing month returns the existing tasks |

## Sync: mirror to a second cloud (manual)

Copies every `rg-datalake-{year}/<table>/*.parquet` object from Seagate S3 into a second S3-compatible bucket, then optionally verifies each file with a WHERE-filtered `count()` on ClickHouse `s3()`.

```
Seagate S3                                    Target cloud (S3-compatible)
s3://rg-datalake-2026/<table>/<file>.parquet ──copy_object──▶ s3://<SYNC_TARGET_BUCKET>/[prefix/]<table>/<file>.parquet
                                                              │
                                     ClickHouse s3() count() WHERE <SYNC_VALIDATE_FILTER>
                                     src (Seagate)  vs  dst (target)  ──▶ verified / mismatch
```

Triggered manually — nothing runs automatically after finalize.

### Configuration

```env
# Target cloud (source side is the existing SEAGATE_* credentials)
SYNC_TARGET_ENDPOINT=https://s3.example.com
SYNC_TARGET_KEY_ID=change-me
SYNC_TARGET_SECRET=change-me
SYNC_TARGET_BUCKET=rg-backup        # or rg-datalake-{year} to keep the source bucket name
SYNC_TARGET_PREFIX=                 # optional path prefix inside the target bucket

# Which side(s) to count after copying: none | src | dst | both
SYNC_VALIDATE_QUERY=both
# SQL fragment used as   SELECT count() FROM s3('<url>', ...) WHERE <filter>
SYNC_VALIDATE_FILTER=root_domain_reverse = 'com.amazon'

# ClickHouse that runs the validation queries; empty = reuse CH_COMPARE_CLOUD_HOST
CH_SYNC_VALIDATE_HOST=
CH_SYNC_VALIDATE_USER=
CH_SYNC_VALIDATE_PASSWORD=
```

`SYNC_TARGET_BUCKET` accepts a `{year}` template, so `rg-datalake-{year}` mirrors bucket-for-bucket and month `202609` lands in `rg-datalake-2026`.

Both credentials are passed to boto3 explicitly, and AWS profile environment variables (`AWS_PROFILE`, `AWS_CONFIG_FILE`, …) are temporarily removed while the client is built — the controller never picks up ambient AWS profiles (e.g. the `wasabi` profile used by `rg_worker.sh`).

### Run

From the dashboard: enter the month, then click **Sync** (copy + validate) or **Validate** (validate existing target files only, no copy). The `validate:` dropdown selects `none` / `src` / `dst` / `both`.

From the API:

```bash
# Full mirror: copy each parquet, then count on both sides
curl -X POST http://<controller>:5000/api/sync/202609 \
  -H "X-API-Token: $API_TOKEN" -H "Content-Type: application/json" \
  -d '{"action": "sync", "validate_query": "both"}'

# Validate only — no copy, just count on already-synced files
curl -X POST http://<controller>:5000/api/sync/202609 \
  -H "X-API-Token: $API_TOKEN" -H "Content-Type: application/json" \
  -d '{"action": "validate_only", "validate_query": "dst"}'

# Progress (records accumulate while running)
curl http://<controller>:5000/api/sync/202609

# CSV export — same data, Excel-friendly UTF-8 BOM
curl -OJ http://<controller>:5000/api/sync/202609/export.csv
```

Both POSTs are asynchronous: they return immediately and a background thread does the work. Triggering the same month again while `status=running` returns `already_running` instead of starting a second run.

### Per-file statuses

| Status | Meaning |
|--------|---------|
| `verified` | copy (if any) succeeded and the count query returned a value |
| `skipped` | object already existed on the target with the same size |
| `mismatch` | `both` mode, `src_count != dst_count` |
| `verify_failed` | the ClickHouse `s3()` query itself failed (bad creds, unreachable endpoint, timeout …) |
| `copy_failed` | `copy_object` failed, or the post-copy HEAD size did not match |
| `missing_on_target` | `validate_only` run, but the object is not in the target bucket |

Run-level status: `running` → `success` (everything verified/skipped) / `partial_failure` (any mismatch or failure) / `failed` (thread crashed — the crash message is stored in `sync_log.details`).

Each record keeps `src_count`/`src_ms`, `dst_count`/`dst_ms`, `copy_ms`, and the raw error text, so a slow or failing file is identifiable from the dashboard or the CSV.

### Notes

- Validation runs on one ClickHouse server for both sides. If the target cloud is a non-AWS endpoint, that server must be able to reach it — ClickHouse restricts `s3()` to allowlisted hosts, so add the target host to `remote_host_allowlist` (or `remote_host_denylist_exclude`) in `config.xml` and restart ClickHouse. Otherwise every record becomes `verify_failed`. Point `CH_SYNC_VALIDATE_HOST` at another server if needed.
- Validation cost is one full scan of the filtered column per file per side (~60 GB per parquet, tens of minutes each). `none` skips SQL entirely and only compares object sizes.
- `validate_only` never writes to the target bucket, so it is safe to run any time.

## Quick Start

Prerequisites: Python 3.10+, ClickHouse on each worker, AWS CLI with the `seagate` profile on each worker, `jq` on workers.

### 1. Configure

```bash
cp .env.example .env
# Fill in real values: API_TOKEN, ClickHouse passwords, Seagate credentials, CONTROLLER_URL
# Optional (only for the sync/validate feature): SYNC_TARGET_* and SYNC_VALIDATE_*
```

`API_TOKEN` must be identical on the controller and all workers/trigger — the controller rejects mutating API calls without a matching `X-API-Token` header.

### 2. Start Controller (1 machine)

```bash
./start_controller.sh
# Dashboard: http://<controller-ip>:5000/dashboard
```

`start_controller.sh` uses **uv** automatically when it is installed (recommended), and falls back to `pip3` otherwise:

```bash
# With uv (recommended)
uv sync               # create .venv and install dependencies
uv lock               # generate / refresh uv.lock (first run, or after dependency changes)
./start_controller.sh # or: uv run python rg_controller.py

# Without uv
pip3 install -r requirements_polling.txt --break-system-packages
python3 rg_controller.py
```

### 3. Run as a systemd service (recommended for production)

`deploy/rg-controller.service` runs the controller under systemd (auto-restart, logs via journald):

```bash
sudo cp deploy/rg-controller.service /etc/systemd/system/
# edit /etc/systemd/system/rg-controller.service: fix WorkingDirectory / ExecStart / User

sudo uv sync                      # create .venv (or pip3 install -r requirements_polling.txt)
sudo systemctl daemon-reload
sudo systemctl enable --now rg-controller

systemctl status rg-controller    # check status
journalctl -u rg-controller -f    # follow logs
```

The service runs `rg_controller.py` directly, so `start_controller.sh` and its PID-file guard are not used under systemd.

### 4. Deploy Workers (12 machines)

Copy `rg_worker.sh` (and `.env`) to each server, then add to crontab:

```bash
*/5 * * * * /path/to/rg_worker.sh >> /var/log/rg_worker.log 2>&1
```

The server short hostname must match a task name (`lweb-rg-001` … `lweb-rg-012`).

Parquet files are written to `<ClickHouse user_files_path>/exports` by default. To use a different (e.g. larger) disk, set `EXPORT_DIR=/data/rg-exports` in the worker's `.env`. If the directory is outside ClickHouse's `user_files_path`, the `clickhouse` user must be able to write it and the server must allow the path for the `file()` function; after upload the worker deletes the local files.

### 5. Deploy Trigger (b80)

Requires the `mysql` client and access to MySQL (`MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_USER` / `MYSQL_PASSWORD` / `MYSQL_DATABASE` in `.env`). The script skips dispatch until the readiness check passes, so it can run daily.

```bash
30 1 * * * /path/to/rg_trigger.sh >> /var/log/rg_trigger.log 2>&1
```

## API Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/ping` | GET | no | Health check |
| `/api/status` | GET | no | Month, cycle state, tasks, workers, finalize result |
| `/api/workers` | GET | no | Worker heartbeats with online status |
| `/api/tasks` | POST | yes | Create 12 tasks for a month (idempotent) |
| `/api/tasks` | GET | no | List tasks (`?month=&server=&status=`) |
| `/api/tasks/<id>` | GET | no | Task detail |
| `/api/tasks/<id>` | PATCH | yes | Claim (`progress`) / complete / failed — server must match task |
| `/api/tasks/<id>/tables` | GET | no | Per-table statuses |
| `/api/tasks/<id>/tables` | POST | yes | Report a table's export/validation result |
| `/api/tasks/<id>/reset` | POST | yes | Reset a task to `new` (retry after timeout/failure) |
| `/api/heartbeat` | POST | yes | Worker liveness ping |
| `/api/finalize/<month>` | POST | yes | Manually trigger finalize/comparison |
| `/api/finalize/<month>/acknowledge` | POST | yes | Mark a `mismatch` finalize as OK after manual review (status becomes `acknowledged`, diffs kept) |
| `/api/sync/<month>` | POST | yes | Start a background mirror (`action=sync`) or validate-only pass; body `{"action": "sync"\|"validate_only", "validate_query": "none"\|"src"\|"dst"\|"both"}` |
| `/api/sync/<month>` | GET | no | Latest sync run for a month: status, run-level counts, per-file records |
| `/api/sync/<month>/export.csv` | GET | no | Same records as UTF-8-BOM CSV (`rg-sync-<month>-<status>.csv`); no secrets, so no token needed |

Mutating endpoints require header `X-API-Token: <API_TOKEN>`. GET endpoints stay open so the dashboard works in the browser. The dashboard's Dispatch / Finalize buttons need the token entered in the "API Token" field (kept only in the browser tab, never embedded in the page).

## Safety

- **No MySQL or ClickHouse writes** — export uses `FUNCTION file()` to write Parquet to local disk only; comparison and sync validation are SELECT-only.
- **Idempotent** — a month's tasks are created once; workers skip tables already on Seagate; sync skips objects already present on the target bucket with a matching size.
- **Retries** — 5× retry on export and upload; per-table row-count validation before marking complete.
- **Timeouts** — tasks stuck in `progress` for 48h are flagged `timeout` and must be reset manually.
- **Credential isolation** — sync credentials come only from `.env`; AWS profile variables are stripped while boto3 clients are built, so ambient profiles on the controller host are never used.

## Files

```
├── .env.example              # Config template (placeholders — never commit real secrets)
├── deploy/
│   └── rg-controller.service # systemd unit for the controller
├── pyproject.toml            # uv / pip project manifest (dependencies)
├── requirements_polling.txt  # Controller dependencies
├── rg_controller.py          # Flask API + SQLite + background finalize/timeout thread + sync engine + dashboard
├── rg_worker.sh              # Worker poll/claim/export/upload script (cron on each server)
├── rg_trigger.sh             # Monthly trigger script (cron on b80)
├── start_controller.sh       # Controller startup
└── README.md                 # This file
```
