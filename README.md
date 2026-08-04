# RG Export Orchestrator

Polling-based distributed export pipeline for ResearchGrid (RG) monthly ranking data.

Exports 16 ClickHouse tables per server (× 12 servers `lweb-rg-001` ~ `012`) to Parquet → uploads to Seagate S3 → validates row counts → cross-checks local vs cloud → sends alerts.

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

## Quick Start

Prerequisites: Python 3.10+, ClickHouse on each worker, AWS CLI with the `seagate` profile on each worker, `jq` on workers.

### 1. Configure

```bash
cp .env.example .env
# Fill in real values: API_TOKEN, ClickHouse passwords, Seagate credentials, CONTROLLER_URL
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

Mutating endpoints require header `X-API-Token: <API_TOKEN>`. GET endpoints stay open so the dashboard works in the browser. The dashboard's Dispatch / Finalize buttons need the token entered in the "API Token" field (kept only in the browser tab, never embedded in the page).

## Safety

- **No MySQL or ClickHouse writes** — export uses `FUNCTION file()` to write Parquet to local disk only; comparison is SELECT-only.
- **Idempotent** — a month's tasks are created once; workers skip tables already on Seagate.
- **Retries** — 5× retry on export and upload; per-table row-count validation before marking complete.
- **Timeouts** — tasks stuck in `progress` for 48h are flagged `timeout` and must be reset manually.

## Files

```
├── .env.example              # Config template (placeholders — never commit real secrets)
├── deploy/
│   └── rg-controller.service # systemd unit for the controller
├── pyproject.toml            # uv / pip project manifest (dependencies)
├── requirements_polling.txt  # Controller dependencies
├── rg_controller.py          # Flask API + SQLite + background finalize/timeout thread + dashboard
├── rg_worker.sh              # Worker poll/claim/export/upload script (cron on each server)
├── rg_trigger.sh             # Monthly trigger script (cron on b80)
├── start_controller.sh       # Controller startup
└── README.md                 # This file
```

## Legacy (Celery + Redis)

The previous Celery-based architecture (`rg_celery_app.py`, `rg_celery_coordinator.py`, `start_master.sh`, `start_worker.sh`, `docker-compose.yml`, `redis.conf`, `trigger_dispatch.sh`, `requirements_celery.txt`, `architecture.md`) has been removed from the repository. See git history for the legacy code.
