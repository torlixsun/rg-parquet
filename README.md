# RG Export Orchestrator

Celery-based distributed export pipeline for ResearchGrid (RG) monthly ranking data.

Exports 16 ClickHouse tables to Parquet → uploads to Seagate S3 → validates row counts → cross-checks all 12 servers.

## Architecture

```
┌─────────────────┐      ┌──────────────┐
│   Celery Beat   │─────▶│ Redis Broker │
│  (10min check)  │      └──────┬───────┘
└─────────────────┘             │
                                ▼
┌─────────────────────────────────────────────┐
│                Celery Chord                  │
│  group(12 × export_all_tables) | comparison  │
└──────┬──────────────────────────────┬────────┘
       │                              │
       ▼                              ▼
┌──────────────┐              ┌──────────────┐
│ Worker 001   │   ...        │ Worker 012   │
│ Q: lweb-001  │              │ Q: lweb-012  │
│              │              │              │
│ export 16    │              │ export 16    │
│ tables →     │              │ tables →     │
│ Parquet →    │              │ Parquet →    │
│ Seagate →    │              │ Seagate →    │
│ validate     │              │ validate     │
└──────┬───────┘              └──────┬───────┘
       │ 12 results                  │
       └──────────┬──────────────────┘
                  ▼
┌──────────────────────────────────┐
│     finalize_export (chord cb)   │
│  comparison + alert notification │
└──────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- Redis (for Celery broker/backend)
- ClickHouse (on each worker, for querying source tables)
- MySQL (for solr_info trigger check, read-only)
- AWS CLI with `seagate` profile (on each worker)

### 1. Configure

```bash
cp .env.example .env
# Edit .env with your credentials
```

Key settings:

| Variable | Description |
|----------|-------------|
| `REDIS_URL` | Redis connection (Celery broker + backend) |
| `MYSQL_*` | MySQL solr_info source (read-only) |
| `CH_LOCAL_*` | ClickHouse on each worker |
| `CH_COMPARE_*` | ClickHouse for cross-validation |
| `SEAGATE_*` | Seagate S3 credentials |
| `ALERT_*` | Alert notification endpoint |

### 2. Install

```bash
pip3 install -r requirements_celery.txt --break-system-packages
```

### 3. Start Master (1 machine)

```bash
./start_master.sh
```

Runs Celery Beat (10-min periodic check) + Flask status API on port 5000.

### 4. Start Workers (12 machines)

```bash
./start_worker.sh lweb-rg-001   # on server 1
./start_worker.sh lweb-rg-002   # on server 2
# ... etc through lweb-rg-012
```

Each worker listens on its own queue and processes tasks locally.

## How It Works

1. **Beat** checks MySQL `solr_info` every 10 minutes
2. When 4 rows (`US_D`, `US_M`, `INTL_D`, `INTL_M`) all have `solr_month == current YYYYMM`, dispatch triggers
3. **Chord** sends 12 parallel `export_all_tables` tasks (one per server)
4. Each **Worker**:
   - Exports 16 tables to Parquet via `clickhouse-client` + `FUNCTION file()`
   - Uploads Parquet files to Seagate S3 (`rg-datalake-{year}/{table}/`)
   - Validates row counts (local vs exported)
   - Cleans up local export files
   - Returns status
5. When all 12 complete, **chord callback** runs cross-validation:
   - Queries local ClickHouse row counts
   - Queries cloud Seagate S3 row counts via `s3()` table function
   - Compares all 16 tables
   - Sends alert (success or mismatch)

## Tables

```text
d_ranking_detail_{YYYYMM}_intl/us   d_ranking_info_{YYYYMM}_intl/us
d_ranking_subrank_{YYYYMM}_intl/us  d_ranking_url_{YYYYMM}_intl/us
m_ranking_detail_{YYYYMM}_intl/us   m_ranking_info_{YYYYMM}_intl/us
m_ranking_subrank_{YYYYMM}_intl/us  m_ranking_url_{YYYYMM}_intl/us
```

## API Endpoints (Master)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ping` | GET | Health check |
| `/api/status` | GET | Dispatch state + worker stats |
| `/api/ready` | GET | Whether solr_info trigger is ready |
| `/api/dispatch` | POST | Manually trigger dispatch |
| `/api/reset` | POST | Reset dispatch state for new cycle |

## Safety

- **Read-only** on MySQL and ClickHouse — no writes to source databases
- Parquet export writes to local filesystem only, not to ClickHouse tables
- Dispatch state tracked in `.dispatch_state.json` to prevent duplicate runs
- Tasks have `soft_time_limit` of 2 hours, `--max-tasks-per-child=1` prevents memory leaks

## Files

```
.
├── .env.example              # Config template
├── .gitignore
├── requirements_celery.txt    # Python dependencies
├── rg_celery_app.py          # Celery app, tasks, beat schedule
├── rg_celery_coordinator.py  # Flask status API
├── start_master.sh           # Master startup (beat + API)
└── start_worker.sh           # Worker startup
```
