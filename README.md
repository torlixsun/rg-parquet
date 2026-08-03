# RG Export Orchestrator

Polling-based distributed export pipeline for ResearchGrid (RG) monthly ranking data.

Exports 16 ClickHouse tables to Parquet → uploads to Seagate S3 → validates row counts → cross-checks all 12 servers.

## Architecture

```
┌──────────────────────┐
│  rg_trigger.sh       │  Cron: monthly (1st, 01:00)
│  (deploy on trigger  │  Computes last month → POST /api/tasks
│   server)            │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────────────────┐
│              rg_controller.py                     │
│  Flask API (port 5000) + SQLite + bg thread       │
│  (deploy on tools-1)                              │
│                                                   │
│  POST /api/tasks           create 12 tasks        │
│  GET  /api/tasks           list/poll tasks        │
│  PATCH /api/tasks/:id      claim/complete/fail    │
│  POST /api/tasks/:id/tables  table-level status   │
│  POST /api/heartbeat       worker liveness        │
│  GET  /api/status          overall status         │
│  GET  /api/workers         worker list + online   │
│  POST /api/finalize/:month manual finalize        │
│  GET  /dashboard           web UI                 │
│                                                   │
│  Background thread:                               │
│    • Every 60s: detect all-12-done → finalize     │
│    • Every 5min: timeout check (48h)              │
└──────────┬───────────────────────────────────────┘
           │  Workers poll via HTTP
           ▼
┌──────────────────┐  ┌──────────────────┐
│  lweb-rg-001     │  │  lweb-rg-002..012 │
│  rg_worker.sh    │  │  (12 servers)     │
│  Cron: */5 min   │  │                   │
│                  │  │                   │
│  1. Heartbeat    │  │                   │
│  2. Poll tasks   │  │                   │
│  3. Claim task   │  │                   │
│  4. Export 16    │  │                   │
│     tables →     │  │                   │
│     Parquet      │  │                   │
│  5. Upload to    │  │                   │
│     Seagate S3   │  │                   │
│  6. Validate     │  │                   │
│     row counts   │  │                   │
│  7. Report       │  │                   │
└──────────────────┘  └──────────────────┘
           │                   │
           ▼                   ▼
┌──────────────────────────────────────────────────┐
│              Seagate S3                           │
│  s3://rg-datalake-{year}/{table}/{server}.parquet │
│  16 tables × 12 servers = 192 parquet files       │
└──────────────────────────────────────────────────┘

                    All 12 complete → auto-finalize
                           │
                           ▼
            ┌──────────────────────────┐
            │  Cross-Validation         │
            │  Local CH vs Seagate S3   │
            │  16 tables compared       │
            └──────────┬───────────────┘
                       │
                       ▼
            ┌──────────────────────────┐
            │  Alert Notification       │
            │  actoniaalerts channel    │
            └──────────────────────────┘
```

## Anti-duplication (4 layers)

| Layer | Mechanism | Scope |
|-------|-----------|-------|
| Trigger | Monthly cron — natural de-dup | Time-based |
| Controller | `UNIQUE(month, server)` on `task_status` table | Per server |
| Worker claim | Atomic UPDATE `WHERE status='new'` | Per task |
| Per-table | `aws s3 ls` check — skips if parquet already on Seagate | Per table |

## Quick Start

### Prerequisites

- Python 3.10+
- SQLite3 (built-in)
- ClickHouse on each worker (for querying source tables)
- AWS CLI with `seagate` profile (on each worker)
- `curl`, `jq` (on each worker)

### 1. Configure

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 2. Install

```bash
pip3 install -r requirements_polling.txt --break-system-packages
```

### 3. Start Controller (1 machine)

```bash
./start_controller.sh
```

Runs Flask API on port 5000 with SQLite (`rg_export.db`) and a background thread for auto-finalize and timeout detection.

Dashboard: `http://<controller-ip>:5000/dashboard`

### 4. Deploy Workers (12 machines)

```bash
# Add crontab on each lweb-rg server:
*/5 * * * * /path/to/rg_worker.sh >> /var/log/rg_worker.log 2>&1
```

Each worker runs every 5 minutes: heartbeat → poll → claim → export → upload → validate → report.

### 5. Deploy Trigger Script

```bash
# Add crontab on the trigger server:
0 1 1 * * /path/to/rg_trigger.sh >> /var/log/rg_trigger.log 2>&1
```

Runs on the 1st of each month at 01:00. Computes the previous month and calls `POST /api/tasks`. Idempotent — controller returns existing tasks if already created.

Configure via `.env`:

```bash
CONTROLLER_URL=http://<controller-ip>:5000
```

## How It Works

1. **Trigger** (`rg_trigger.sh`): Runs monthly, POSTs `{"month": "YYYYMM"}` to controller
2. **Controller** creates 12 tasks in SQLite (status=`new`), one per server
3. **Workers** (`rg_worker.sh`) poll every 5 min:
   - Send heartbeat to controller
   - Query `GET /api/tasks?server=<hostname>&status=new`
   - Atomically claim task via `PATCH /api/tasks/:id` (status=`new` → `progress`)
   - For each of 16 tables:
     - Skip if already on Seagate (`aws s3 ls`)
     - Export via `clickhouse-client` + `FUNCTION file()` → Parquet (zstd, 1M row groups)
     - Upload to Seagate S3 (`rg-datalake-{year}/{table}/`)
     - Validate row counts (local vs exported)
     - Report table-level status to controller
     - Clean up local files
   - Mark task `complete` or `failed`
4. **Background thread** detects when all 12 tasks are done:
   - Queries local ClickHouse vs Seagate S3 row counts
   - Compares all 16 tables
   - Sends alert (success or mismatch)
   - Records result in `finalize_log`

## 16 Tables

```
d_ranking_detail_{month}_intl    m_ranking_detail_{month}_intl
d_ranking_detail_{month}_us      m_ranking_detail_{month}_us
d_ranking_info_{month}_intl      m_ranking_info_{month}_intl
d_ranking_info_{month}_us        m_ranking_info_{month}_us
d_ranking_subrank_{month}_intl   m_ranking_subrank_{month}_intl
d_ranking_subrank_{month}_us     m_ranking_subrank_{month}_us
d_ranking_url_{month}_intl       m_ranking_url_{month}_intl
d_ranking_url_{month}_us         m_ranking_url_{month}_us
```

(8 daily + 8 monthly) × (intl + us) = 16 tables

## API Endpoints (Controller)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ping` | GET | Health check |
| `/api/tasks` | POST | Create 12 tasks for a month (idempotent) |
| `/api/tasks` | GET | List tasks (filters: `month`, `server`, `status`) |
| `/api/tasks/:id` | GET | Task detail + table statuses |
| `/api/tasks/:id` | PATCH | Claim/complete/fail a task |
| `/api/tasks/:id/tables` | GET | Table-level statuses for a task |
| `/api/tasks/:id/tables` | POST | Update table-level status |
| `/api/tasks/:id/reset` | POST | Reset task to `new` (retry) |
| `/api/heartbeat` | POST | Worker heartbeat |
| `/api/status` | GET | Overall status (tasks + workers + finalize) |
| `/api/workers` | GET | Worker list with online/offline status |
| `/api/finalize/:month` | POST | Manually trigger cross-validation |
| `/dashboard` | GET | Web dashboard (dark theme, auto-refresh) |

## Task States

```
new ──→ progress ──→ complete
  │                    │
  └── (timeout)        └── failed
```

- `new`: Waiting for a worker to claim
- `progress`: Worker is exporting
- `complete`: All 16 tables exported and uploaded successfully
- `failed`: One or more tables failed
- `timeout`: Exceeded 48h (auto-set by background thread)

## Safety

- **No MySQL or ClickHouse writes** — Export uses `FUNCTION file()` to write Parquet to local filesystem only. Comparison is SELECT-only.
- **Idempotent** — Same month cannot be dispatched twice. Workers skip already-uploaded tables.
- Long-running tasks supported (no time limits, 48h timeout with auto-detection).

## Files

```
.
├── .env.example                # Config template
├── .gitignore
├── README.md
├── requirements_polling.txt    # Python dependencies
├── rg_controller.py            # Flask API + SQLite + dashboard
├── rg_trigger.sh               # Monthly dispatch trigger
├── rg_worker.sh                # Per-server export worker
└── start_controller.sh         # Controller startup
```
