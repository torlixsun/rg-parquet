"""
RG Export — Celery Application
===============================
- Celery Beat: periodically checks MySQL solr_info, dispatches export when ready
- Workers:  12 servers (lweb-rg-001 ~ 012), each runs export_all_tables on its own queue
- Master:   chord → comparison → alert

Deployment:
  Master (1 machine):
    celery -A rg_celery_app beat -l info
    python3 rg_celery_coordinator.py    # Flask status API (optional)

  Worker (12 machines, one each):
    celery -A rg_celery_app worker -Q {hostname} -n {hostname}@%h -l info --concurrency=1
"""

import json
import os
import re
import subprocess
import tempfile
from datetime import datetime

import clickhouse_driver
import mysql.connector
import requests
from celery import Celery, chord, group
from celery.schedules import crontab
from celery.signals import beat_init, worker_ready
from celery.utils.log import get_task_logger
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

logger = get_task_logger(__name__)

# ============================================================
# Config from .env
# ============================================================
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")

# MySQL
MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE", "actonia")

# Worker ClickHouse
CH_LOCAL_HOST = os.environ.get("CH_LOCAL_HOST", "127.0.0.1")
CH_LOCAL_PASSWORD = os.environ.get("CH_LOCAL_PASSWORD", "clarity99!")
CH_LOCAL_DB = os.environ.get("CH_LOCAL_DB", "monthly_ranking")

# Comparison ClickHouse
CH_COMPARE_LOCAL_HOST = os.environ.get("CH_COMPARE_LOCAL_HOST", "23.105.14.193")
CH_COMPARE_LOCAL_DB = os.environ.get("CH_COMPARE_LOCAL_DB", "monthly_ranking")
CH_COMPARE_CLOUD_HOST = os.environ.get("CH_COMPARE_CLOUD_HOST", "173.236.65.154")
CH_COMPARE_CLOUD_USER = os.environ.get("CH_COMPARE_CLOUD_USER", "default")
CH_COMPARE_CLOUD_PASSWORD = os.environ.get("CH_COMPARE_CLOUD_PASSWORD", "clarity99!")

# Alert
ALERT_URL = os.environ.get("ALERT_URL", "http://69.175.99.218:8090/api/v1/alert")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY", "")
ALERT_CHANNEL = os.environ.get("ALERT_CHANNEL", "actonia-alerts")

# Seagate
SEAGATE_KEY_ID = os.environ.get("SEAGATE_KEY_ID", "")
SEAGATE_SECRET = os.environ.get("SEAGATE_SECRET", "")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "seagate")
SEAGATE_ENDPOINT = os.environ.get("SEAGATE_ENDPOINT", "https://s3.clarity1.lyve.seagate.com")

# Dispatch state file (tracks which months have been dispatched)
DISPATCH_STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".dispatch_state.json"
)

# ============================================================
# Celery App
# ============================================================
app = Celery("rg_export", broker=REDIS_URL, backend=REDIS_URL)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Shanghai",
    enable_utc=False,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=7200,   # 2 hr per server
    task_time_limit=7500,
    beat_schedule={
        "check-solr-info-every-10min": {
            "task": "rg_celery_app.check_and_dispatch",
            "schedule": crontab(minute="*/10"),
        },
    },
)

# 12 servers
RG_SERVERS = [f"lweb-rg-{i:03d}" for i in range(1, 13)]

# 16 tables
RG_TABLES = [
    "d_ranking_detail_{m}_intl",
    "d_ranking_detail_{m}_us",
    "d_ranking_info_{m}_intl",
    "d_ranking_info_{m}_us",
    "d_ranking_subrank_{m}_intl",
    "d_ranking_subrank_{m}_us",
    "d_ranking_url_{m}_intl",
    "d_ranking_url_{m}_us",
    "m_ranking_detail_{m}_intl",
    "m_ranking_detail_{m}_us",
    "m_ranking_info_{m}_intl",
    "m_ranking_info_{m}_us",
    "m_ranking_subrank_{m}_intl",
    "m_ranking_subrank_{m}_us",
    "m_ranking_url_{m}_intl",
    "m_ranking_url_{m}_us",
]


# ============================================================
# Helpers
# ============================================================
def _dispatch_state_load():
    try:
        with open(DISPATCH_STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _dispatch_state_save(state):
    with open(DISPATCH_STATE_FILE, "w") as f:
        json.dump(state, f)


def _get_mysql_solr_info():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, country_code, solr_month, solr_url "
            "FROM solr_info "
            "WHERE solr_month >= 201901 AND solr_type = 19 "
            "ORDER BY id"
        )
        return cursor.fetchall()
    finally:
        conn.close()


def _is_ready():
    """
    Ready when 4 rows exist (US_D, US_M, INTL_D, INTL_M)
    AND all solr_month == current YYYYMM.
    """
    current_month = int(datetime.now().strftime("%Y%m"))
    rows = _get_mysql_solr_info()

    if not rows:
        return False, current_month, "no rows"

    expected = {"US_D", "US_M", "INTL_D", "INTL_M"}
    found = {r[1] for r in rows}
    if found != expected:
        return False, current_month, f"missing: {expected-found}, extra: {found-expected}"

    if all(r[2] == current_month for r in rows):
        return True, current_month, "all ready"
    months = {r[1]: r[2] for r in rows}
    return False, current_month, f"months: {months}"


def _send_alert(title, message, level, tags):
    try:
        requests.post(
            ALERT_URL,
            json={
                "title": title,
                "message": message,
                "level": level,
                "source": "rg-celery-coordinator",
                "tags": tags,
                "channel": ALERT_CHANNEL,
            },
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": ALERT_API_KEY,
            },
            timeout=15,
        )
    except Exception as exc:
        logger.error("Alert failed: %s", exc)


# ============================================================
# Beat Task — check solr_info and dispatch
# ============================================================
@app.task(name="rg_celery_app.check_and_dispatch")
def check_and_dispatch():
    """Periodically called by Celery Beat. When solr_info ready, dispatch export chord."""
    ready, current_month, reason = _is_ready()
    state = _dispatch_state_load()
    dispatched_month = state.get("dispatched_month")
    target_month = str(current_month)

    logger.info("check_and_dispatch: ready=%s month=%s reason=%s", ready, current_month, reason)

    if not ready:
        return {"ready": False, "month": target_month, "reason": reason}

    if dispatched_month == target_month:
        if state.get("completed"):
            return {"ready": True, "month": target_month, "status": "already_completed"}
        still_running = state.get("running_month") == target_month
        return {
            "ready": True,
            "month": target_month,
            "status": "already_dispatched" if not still_running else "still_running",
        }

    # ---- Dispatch! ----
    logger.info("Dispatching export for %s on %d servers", target_month, len(RG_SERVERS))

    state["dispatched_month"] = target_month
    state["running_month"] = target_month
    state["completed"] = False
    _dispatch_state_save(state)

    # chord: group(12 server tasks) | finalize
    header = group(
        export_all_tables.s(hostname, target_month).set(queue=hostname)
        for hostname in RG_SERVERS
    )
    workflow = chord(header)(finalize_export.s(target_month))

    return {"ready": True, "month": target_month, "status": "dispatched"}


# ============================================================
# Worker Task — export all 16 tables on one server
# ============================================================
@app.task(bind=True, name="rg_celery_app.export_all_tables")
def export_all_tables(self, hostname: str, target_month: str):
    """
    Run on a specific worker (lweb-rg-00X).
    Export 16 tables → Parquet → upload to Seagate → validate → cleanup.
    Returns: {"hostname":..., "status":"done"|"failed", "failed_tables":[...]}
    """
    year = target_month[:4]
    tables = [t.format(m=target_month) for t in RG_TABLES]
    host_short = hostname  # or use socket.gethostname()

    failed_tables = []

    logger.info("[%s] Export started, %d tables", hostname, len(tables))

    for tb in tables:
        logger.info("[%s] Processing %s", hostname, tb)
        export_dir = f"/data/exports/{tb}"
        parquet_file = f"{export_dir}/{hostname}.parquet"
        user_files_path = f"/etc/clickhouse-server/user_files/exports/{tb}/{hostname}.parquet"
        s3_path = f"s3://rg-datalake-{year}/{tb}/{hostname}.parquet"

        try:
            # mkdir + permissions
            subprocess.run(["mkdir", "-pv", export_dir], check=True)
            subprocess.run(["chown", "-R", "clickhouse:clickhouse", export_dir], check=True)

            # Export to Parquet (retry)
            for attempt in range(5):
                proc = subprocess.run(
                    [
                        "clickhouse-client",
                        "--password", CH_LOCAL_PASSWORD,
                        "--query",
                        f"INSERT INTO FUNCTION file('{user_files_path}', Parquet) "
                        f"SELECT * FROM {CH_LOCAL_DB}.local_{tb} "
                        f"SETTINGS output_format_parquet_row_group_size = 1000000, "
                        f"output_format_parquet_compression_method = 'zstd', "
                        f"max_memory_usage = 40000000000",
                    ],
                    capture_output=True, text=True, timeout=1800,
                )
                if proc.returncode == 0:
                    break
                logger.warning("[%s] Export %s attempt %d failed: %s", hostname, tb, attempt + 1, proc.stderr[-200:])
            else:
                raise RuntimeError(f"Export failed after 5 retries")

            # Upload to Seagate (retry)
            for attempt in range(5):
                proc = subprocess.run(
                    [
                        "aws", "s3", "cp", f"{tb}/{hostname}.parquet",
                        s3_path,
                        "--profile", AWS_PROFILE,
                        "--endpoint-url", SEAGATE_ENDPOINT,
                    ],
                    capture_output=True, text=True, timeout=600,
                )
                if proc.returncode == 0:
                    break
                logger.warning("[%s] Upload %s attempt %d failed: %s", hostname, tb, attempt + 1, proc.stderr[-200:])
            else:
                raise RuntimeError("Upload failed after 5 retries")

            # Row count validation
            local_count = int(
                subprocess.check_output(
                    [
                        "clickhouse-client", "-m",
                        "--password", CH_LOCAL_PASSWORD,
                        "-q", f"SELECT count() FROM {CH_LOCAL_DB}.local_{tb}",
                    ],
                    text=True, timeout=120,
                ).strip()
            )
            export_count = int(
                subprocess.check_output(
                    [
                        "clickhouse-client",
                        "--password", CH_LOCAL_PASSWORD,
                        "--query",
                        f"SELECT count() FROM file('{user_files_path}', Parquet)",
                    ],
                    text=True, timeout=120,
                ).strip()
            )

            if local_count != export_count:
                logger.error(
                    "[%s] %s row mismatch: local=%d export=%d",
                    hostname, tb, local_count, export_count,
                )
                failed_tables.append(tb)

            # Cleanup local export dir
            subprocess.run(["rm", "-rf", export_dir], check=False)

        except Exception as exc:
            logger.error("[%s] %s ERROR: %s", hostname, tb, exc)
            failed_tables.append(tb)
            # Cleanup attempt
            subprocess.run(["rm", "-rf", export_dir], check=False)

    status = "done" if not failed_tables else "failed"
    logger.info("[%s] Export finished. status=%s, failed=%d", hostname, status, len(failed_tables))

    return {
        "hostname": hostname,
        "status": status,
        "failed_tables": failed_tables,
    }


# ============================================================
# Chord Callback — all 12 servers done → compare + alert
# ============================================================
@app.task(name="rg_celery_app.finalize_export")
def finalize_export(results, target_month):
    """
    Called when all 12 export_all_tables tasks complete.
    Runs comparison (local CH vs Seagate) then sends alert.
    """
    logger.info("All 12 servers finished. Results: %s", results)

    # Check if any server failed
    failed_servers = [r for r in results if r.get("status") != "done"]
    if failed_servers:
        names = [r["hostname"] for r in failed_servers]
        _send_alert(
            "RG export completed with failures",
            f"Servers with failures: {names}",
            "WARNING",
            ["rg-parquet", "export", "partial-failure"],
        )
        _mark_completed(target_month)
        return {"status": "partial_failure", "failed_servers": names}

    # ---- Run comparison ----
    logger.info("Running comparison for %s", target_month)
    all_ok, details = _run_comparison(target_month)

    # Build summary
    lines = []
    mismatch_count = 0
    for d in details:
        diff = d.get("diff")
        if diff == 0:
            icon = "OK"
        elif diff == "-" or diff == "ERROR":
            icon = "ERROR"
        else:
            icon = "MISMATCH"
            mismatch_count += 1
        lines.append(
            f"  {icon:8s} {d['table']}  local={d.get('local','-')}  cloud={d.get('cloud','-')}  diff={diff}"
        )

    if all_ok:
        _send_alert(
            "RG export & validation completed",
            f"All 12 servers finished. {len(details)} tables validated OK.\n"
            + "\n".join(lines),
            "INFO",
            ["rg-parquet", "export", "validation", "success"],
        )
    else:
        _send_alert(
            "RG validation MISMATCH",
            f"All 12 servers finished but {mismatch_count}/{len(details)} tables have mismatches!\n"
            + "\n".join(lines),
            "CRITICAL",
            ["rg-parquet", "export", "validation", "failure", "p0"],
        )

    _mark_completed(target_month)
    return {"status": "completed", "comparison_ok": all_ok, "details": details}


def _run_comparison(target_month):
    """Compare local ClickHouse vs Seagate S3 parquet row counts."""
    year = target_month[:4]
    tables = [t.format(m=target_month) for t in RG_TABLES]

    local_client = clickhouse_driver.Client(host=CH_COMPARE_LOCAL_HOST)
    cloud_client = clickhouse_driver.Client(
        host=CH_COMPARE_CLOUD_HOST,
        user=CH_COMPARE_CLOUD_USER,
        password=CH_COMPARE_CLOUD_PASSWORD,
    )

    details = []
    all_ok = True

    for tb in tables:
        try:
            local_count = local_client.execute(
                f"SELECT count() FROM {CH_COMPARE_LOCAL_DB}.{tb}"
            )[0][0]
        except Exception as exc:
            logger.error("Local query failed %s: %s", tb, exc)
            details.append({"table": tb, "local": "ERROR", "cloud": "-", "diff": "-"})
            all_ok = False
            continue

        s3_path = (
            f"https://s3.us-east-1.clarity1.lyve.seagate.com/"
            f"rg-datalake-{year}/{tb}/*.parquet"
        )
        try:
            cloud_count = cloud_client.execute(
                f"SELECT count() FROM s3('{s3_path}', '{SEAGATE_KEY_ID}', '{SEAGATE_SECRET}', 'Parquet')"
            )[0][0]
        except Exception as exc:
            logger.error("Cloud query failed %s: %s", tb, exc)
            details.append({"table": tb, "local": local_count, "cloud": "ERROR", "diff": "-"})
            all_ok = False
            continue

        diff = cloud_count - local_count
        if diff != 0:
            all_ok = False

        logger.info("  %s  local=%s  cloud=%s  diff=%s", tb, local_count, cloud_count, diff)
        details.append({"table": tb, "local": local_count, "cloud": cloud_count, "diff": diff})

    return all_ok, details


def _mark_completed(target_month):
    state = _dispatch_state_load()
    state["completed"] = True
    state["running_month"] = None
    _dispatch_state_save(state)
    logger.info("Cycle %s marked completed", target_month)


# ============================================================
# Signal: worker_ready (for debugging)
# ============================================================
@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info("Worker ready: %s", sender.hostname)


@beat_init.connect
def on_beat_init(sender, **kwargs):
    logger.info("Beat scheduler started")
