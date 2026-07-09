# RG Export Orchestrator

Celery-based distributed export pipeline for ResearchGrid (RG) monthly ranking data.

Exports 16 ClickHouse tables to Parquet → uploads to Seagate S3 → validates row counts → cross-checks all 12 servers.

## Architecture

```
┌───────────────────┐
│  MySQL Server     │  crontab runs trigger_dispatch.sh
│  (solr_info)      │  every 10 min, checks solr_info
└────────┬──────────┘  → POST /api/dispatch?month=YYYYMM when ready
         │
         ▼
┌─────────────────────────────────────────────┐
│              Coordinator                    │
│  Flask API (dispatch/stats)                 │
│  Celery Worker (-Q coordinator)             │
│  .dispatch_state.json (idempotency)         │
└──────────┬──────────┬───────────────────────┘
           │          │
           ▼          ▼ (via Redis broker)
┌─────────────────────────────────────────────┐
│              Celery Chord                   │
│   group(12 × export_all_tables)             │
│          | finalize_export                  │
│          |   (comparison + alert)            │
└──────┬──────────────────────────────────────┘
       │
       ▼
┌──────────────┐        ┌──────────────┐
│ Worker 001   │  ...   │ Worker 012   │
│ Q: lweb-001  │        │ Q: lweb-012  │
│              │        │              │
│ export 16    │        │ export 16    │
│ tables →     │        │ tables →     │
│ Parquet →    │        │ Parquet →    │
│ Seagate →    │        │ Seagate →    │
│ validate     │        │ validate     │
└──────────────┘        └──────────────┘
```

## Anti-duplication (3 layers)

| Layer | Mechanism | Scope |
|-------|-----------|-------|
| Trigger script | `/tmp/rg_trigger_state.txt` tracks last triggered month | Per MySQL server |
| Coordinator API | Rejects if `dispatched_month == target_month` before even queuing | Global |
| Celery task | `.dispatch_state.json` + idempotency guard in `dispatch_export` | Global |
| Worker per-table | `aws s3 ls` check — skips if parquet already on Seagate | Per table |

## Quick Start

### Prerequisites

- Python 3.10+
- Redis (Celery broker/backend)
- ClickHouse on each worker (for querying source tables)
- AWS CLI with `seagate` profile (on each worker)

### 1. Configure

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 2. Install

```bash
pip3 install -r requirements_celery.txt --break-system-packages
```

### 3. Start Coordinator (1 machine)

```bash
./start_master.sh
```

Runs Celery Worker (`-Q coordinator`) + Flask API on port 5000.

### 4. Start Workers (12 machines)

```bash
./start_worker.sh lweb-rg-001   # on server 1
./start_worker.sh lweb-rg-002   # on server 2
# ... etc through lweb-rg-012
```

### 5. Deploy Trigger Script (MySQL server)

```bash
cp trigger_dispatch.sh /usr/local/bin/
chmod +x /usr/local/bin/trigger_dispatch.sh
```

Configure environment variables in the script or via export:

```bash
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=root
export MYSQL_PASSWORD=your_password
export COORDINATOR_URL=http://<coordinator-ip>:5000
```

Add crontab (every 10 minutes):

```bash
*/10 * * * * /usr/local/bin/trigger_dispatch.sh >> /var/log/rg_trigger.log 2>&1
```

## How It Works

1. **MySQL server**: `trigger_dispatch.sh` queries `solr_info` every 10 min
2. When 4 rows (`US_D`, `US_M`, `INTL_D`, `INTL_M`) all have `solr_month == current YYYYMM`, calls `POST /api/dispatch?month=YYYYMM`
3. **Coordinator** validates the request (idempotency check) then queues a Celery **chord**:
   - 12 parallel `export_all_tables` tasks (one per server queue)
   - Chord callback `finalize_export` (comparison + alert)
4. Each **Worker**:
   - Checks if table already on Seagate (`aws s3 ls`) → skip if exists
   - Exports table to Parquet via `clickhouse-client` + `FUNCTION file()`
   - Uploads Parquet to Seagate S3 (`rg-datalake-{year}/{table}/`)
   - Validates row counts (local vs exported)
   - Cleans up local files
5. When all 12 complete, **chord callback** (`finalize_export`):
   - Queries local ClickHouse vs Seagate S3 row counts
   - Compares all 16 tables
   - Sends alert (success or mismatch)
   - Marks cycle as completed

## API Endpoints (Coordinator)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ping` | GET | Health check |
| `/api/status` | GET | Dispatch state + worker stats |
| `/api/dispatch?month=YYYYMM` | POST | Trigger export for a month (idempotent) |
| `/api/reset` | POST | Reset dispatch state (emergency re-run) |

## Monthly Reset

No manual action needed. When `trigger_dispatch.sh` sees a new month in MySQL, it calls the coordinator. The coordinator's `.dispatch_state.json` detects the new month and allows dispatch. The trigger script's local state file also auto-rolls.

## Safety

- **No MySQL or ClickHouse writes** — Export uses `FUNCTION file()` to write Parquet to local filesystem only, not ClickHouse tables. Comparison is SELECT-only.
- **Idempotent** — Same month cannot be dispatched twice. Workers skip already-uploaded tables.
- Long-running tasks supported (no time limits, 48h visibility timeout).

## Files

```
.
├── .env.example              # Config template
├── .gitignore
├── requirements_celery.txt    # Python dependencies
├── rg_celery_app.py          # Celery app, tasks, chord logic
├── rg_celery_coordinator.py  # Flask API + dispatch endpoint
├── start_master.sh           # Coordinator startup
├── start_worker.sh           # Worker startup
└── trigger_dispatch.sh       # MySQL-side trigger script
```
