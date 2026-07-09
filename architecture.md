# RG Export Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          db80 (MySQL)                               │
│                                                                     │
│  trigger_dispatch.sh (crontab */10)                                 │
│    ├─ SELECT solr_info FROM MySQL                                   │
│    │   check US_D/US_M/INTL_D/INTL_M solr_month == current YYYYMM  │
│    │                                                                │
│    └─ POST /api/dispatch?month=YYYYMM ─────────────────────┐       │
│                                                              │      │
└──────────────────────────────────────────────────────────────┼──────┘
                                                               │
                                                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        tools-1 (Coordinator)                         │
│                                                                      │
│  ┌─────────────────────┐    ┌──────────────────────────────┐        │
│  │ Celery Worker       │    │ Flask API (port 5000)         │        │
│  │ -Q coordinator      │    │                              │        │
│  │                     │    │ GET  /api/status              │        │
│  │ dispatch_export()   │◄───│ POST /api/dispatch?month=     │        │
│  │ finalize_export()   │    │ POST /api/reset               │        │
│  │ comparison()        │    │ GET  /dashboard               │        │
│  └─────────┬───────────┘    └──────────────────────────────┘        │
│            │                                                         │
│            │  .dispatch_state.json (idempotency)                     │
│            │                                                         │
│            │  Chord: group(12 × export) | finalize(compare+alert)    │
│            │                                                         │
└────────────┼─────────────────────────────────────────────────────────┘
             │
             │  Publish tasks via Redis
             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     Redis (Docker on tools-1)                        │
│                                                                      │
│  Broker + Result Backend  (redis://127.0.0.1:6379/0)                │
│                                                                      │
│  ┌────────┬────────┬────────┬────────┬──────────┬────────────┐      │
│  │lweb-001│lweb-002│lweb-003│  ...   │lweb-011  │lweb-012    │      │
│  │ queue  │ queue  │ queue  │        │ queue    │ queue       │      │
│  └───┬────┴───┬────┴───┬────┴────────┴────┬─────┴─────┬──────┘      │
└──────┼────────┼────────┼──────────────────┼───────────┼─────────────┘
       │        │        │                  │           │
       ▼        ▼        ▼                  ▼           ▼
┌──────────┬──────────┬──────────┬─────┬──────────┬──────────┐
│lweb-rg-  │lweb-rg-  │lweb-rg-  │ ... │lweb-rg-  │lweb-rg-  │
│  001     │  002     │  003     │     │  011     │  012     │
│          │          │          │     │          │          │
│celery    │celery    │celery    │     │celery    │celery    │
│worker    │worker    │worker    │     │worker    │worker    │
│          │          │          │     │          │          │
│export 16 │export 16 │export 16 │     │export 16 │export 16 │
│tables →  │tables →  │tables →  │     │tables →  │tables →  │
│Parquet   │Parquet   │Parquet   │     │Parquet   │Parquet   │
│    ↓     │    ↓     │    ↓     │     │    ↓     │    ↓     │
│ validate │ validate │ validate │     │ validate │ validate │
│    ↓     │    ↓     │    ↓     │     │    ↓     │    ↓     │
│ upload   │ upload   │ upload   │     │ upload   │ upload   │
└────┬─────┴────┬─────┴────┬─────┴─────┴────┬─────┴────┬─────┘
     │          │          │                │          │
     │          │          │  Seagate S3    │          │
     │          │          ├────────────────┤          │
     │          │          │  per-server    │          │
     │          │          │  .parquet      │          │
     │          │          │                │          │
     ▼          ▼          ▼                ▼          ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      Seagate S3                                      │
│                                                                      │
│  s3://rg-datalake-{year}/                                            │
│    d_ranking_detail_{month}_intl/lweb-rg-001.parquet                 │
│    d_ranking_detail_{month}_intl/lweb-rg-002.parquet                 │
│    ... (16 tables × 12 servers = 192 files)                          │
│                                                                      │
│  s3://rankintelligence-{year}/                                       │
│    (RI dataset, separate backup pipeline)                            │
└──────────────────────────────────────────────────────────────────────┘

                    All 12 complete → chord callback on tools-1
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
                    │  POST 69.175.99.218:8090 │
                    │  channel: actoniaalerts  │
                    └──────────────────────────┘
```

## Data Flow

```
db80 ──trigger──▶ tools-1 ──dispatch──▶ Redis ──routes──▶ lweb-rg-001..012
                                                              │
                                                     export → validate → upload
                                                              │
                                              Seagate S3 ◄────┘
                                                              │
                            tools-1 ◄── chord callback ──────┘
                               │
                        comparison (CH + S3)
                               │
                          alert ◄──┘
```

## Component Summary

| Host | Role | Components |
|------|------|------------|
| `db80` | Trigger | `trigger_dispatch.sh` (crontab), MySQL solr_info |
| `tools-1` | Coordinator | Celery Worker (`-Q coordinator`), Flask API, Redis (Docker) |
| `lweb-rg-001` ~ `012` | Workers | Celery Worker (`-Q lweb-rg-00X`), ClickHouse, AWS CLI |

## Network Requirements

| From | To | Protocol | Purpose |
|------|----|----------|---------|
| db80 | tools-1:5000 | HTTP | POST /api/dispatch |
| tools-1 | Redis :6379 | TCP | Celery broker |
| tools-1 | ClickHouse compare hosts | TCP 9000 | Comparison SELECT |
| lweb-rg-* | Redis :6379 | TCP | Celery worker |
| lweb-rg-* | Seagate S3 | HTTPS | Parquet upload |
| tools-1 | 69.175.99.218:8090 | HTTP | Alert notifications |
