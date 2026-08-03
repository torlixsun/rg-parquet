#!/usr/bin/env python3
"""
RG Parquet Export Controller
=============================
Flask API + SQLite.  No Redis, no Celery.
Workers poll this API via cron (every 5 min).

Deploy on tools-1:
    python3 rg_controller.py
    # or: ./start_controller.sh
"""

import json
import hmac
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta

import clickhouse_driver
import requests
from dotenv import load_dotenv
from flask import Flask, g, jsonify, render_template_string, request

# ============================================================
# Config
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

PORT = int(os.environ.get("PORT", 5000))
DB_PATH = os.path.join(SCRIPT_DIR, "rg_export.db")
TASK_TIMEOUT_HOURS = int(os.environ.get("TASK_TIMEOUT_HOURS", 48))

# Shared secret for API auth (mutating endpoints). Set a real value in .env!
API_TOKEN = os.environ.get("API_TOKEN", "")

# ClickHouse — cross-validation
CH_COMPARE_LOCAL_HOST = os.environ.get("CH_COMPARE_LOCAL_HOST", "23.105.14.193")
CH_COMPARE_LOCAL_DB = os.environ.get("CH_COMPARE_LOCAL_DB", "monthly_ranking")
CH_COMPARE_CLOUD_HOST = os.environ.get("CH_COMPARE_CLOUD_HOST", "173.236.65.154")
CH_COMPARE_CLOUD_USER = os.environ.get("CH_COMPARE_CLOUD_USER", "default")
CH_COMPARE_CLOUD_PASSWORD = os.environ.get("CH_COMPARE_CLOUD_PASSWORD", "")

# Alert
ALERT_URL = os.environ.get("ALERT_URL", "http://69.175.99.218:8090/api/v1/alert")
ALERT_API_KEY = os.environ.get("ALERT_API_KEY", "")
ALERT_CHANNEL = os.environ.get("ALERT_CHANNEL", "actoniaalerts")

# Seagate
SEAGATE_KEY_ID = os.environ.get("SEAGATE_KEY_ID", "")
SEAGATE_SECRET = os.environ.get("SEAGATE_SECRET", "")
SEAGATE_ENDPOINT = os.environ.get(
    "SEAGATE_ENDPOINT", "https://s3.us-east-1.clarity1.lyve.seagate.com"
)

# 12 servers
RG_SERVERS = [f"lweb-rg-{i:03d}" for i in range(1, 13)]

# 16 table templates
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
# SQLite
# ============================================================
app = Flask(__name__)


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA busy_timeout=5000")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def _get_db_thread():
    """Get a DB connection for background thread (no Flask context)."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS task_status (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            month       TEXT NOT NULL,
            server      TEXT NOT NULL,
            status      TEXT DEFAULT 'new',
            created_at  TEXT,
            started_at  TEXT,
            completed_at TEXT,
            error_msg   TEXT DEFAULT '',
            UNIQUE(month, server)
        );

        CREATE TABLE IF NOT EXISTS task_table_status (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id     INTEGER NOT NULL,
            table_name  TEXT NOT NULL,
            status      TEXT DEFAULT 'pending',
            rows_local  INTEGER DEFAULT 0,
            rows_s3     INTEGER DEFAULT 0,
            error_msg   TEXT DEFAULT '',
            updated_at  TEXT,
            UNIQUE(task_id, table_name),
            FOREIGN KEY(task_id) REFERENCES task_status(id)
        );

        CREATE TABLE IF NOT EXISTS worker_heartbeat (
            server      TEXT PRIMARY KEY,
            last_seen   TEXT,
            hostname    TEXT,
            ip          TEXT
        );

        CREATE TABLE IF NOT EXISTS finalize_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            month       TEXT UNIQUE,
            status      TEXT,
            details     TEXT,
            created_at  TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_task_month   ON task_status(month);
        CREATE INDEX IF NOT EXISTS idx_task_status  ON task_status(status);
        CREATE INDEX IF NOT EXISTS idx_tts_task     ON task_table_status(task_id);
        """
    )
    conn.commit()
    conn.close()


# ============================================================
# Helpers
# ============================================================
def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _valid_month(month):
    """Accept a real YYYYMM month (e.g. 202608), reject nonsense like 999999."""
    return bool(re.fullmatch(r"(19|20)\d{2}(0[1-9]|1[0-2])", month or ""))


def _check_auth():
    """Require X-API-Token on mutating endpoints when API_TOKEN is configured."""
    if not API_TOKEN:
        return None
    token = request.headers.get("X-API-Token", "")
    if not hmac.compare_digest(token, API_TOKEN):
        return jsonify({"error": "unauthorized"}), 401
    return None


def send_alert(title, message, level, tags):
    try:
        requests.post(
            ALERT_URL,
            json={
                "title": title,
                "message": message,
                "level": level,
                "source": "rg-controller",
                "tags": tags,
                "channel": ALERT_CHANNEL,
            },
            headers={"Content-Type": "application/json", "X-Api-Key": ALERT_API_KEY},
            timeout=15,
        )
    except Exception as exc:
        print(f"[ALERT ERROR] {exc}")


def run_comparison(target_month):
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
        # Local count
        try:
            local_count = local_client.execute(
                f"SELECT count() FROM {CH_COMPARE_LOCAL_DB}.{tb}"
            )[0][0]
        except Exception as exc:
            print(f"[COMPARE] Local query failed {tb}: {exc}")
            details.append(
                {"table": tb, "local": "ERROR", "cloud": "-", "diff": "-"}
            )
            all_ok = False
            continue

        # Cloud S3 count (all 12 servers' parquet files)
        s3_url = f"{SEAGATE_ENDPOINT}/rg-datalake-{year}/{tb}/*.parquet"
        try:
            cloud_count = cloud_client.execute(
                f"SELECT count() FROM s3('{s3_url}', '{SEAGATE_KEY_ID}', '{SEAGATE_SECRET}', 'Parquet')"
            )[0][0]
        except Exception as exc:
            print(f"[COMPARE] Cloud query failed {tb}: {exc}")
            details.append(
                {"table": tb, "local": local_count, "cloud": "ERROR", "diff": "-"}
            )
            all_ok = False
            continue

        diff = cloud_count - local_count
        if diff != 0:
            all_ok = False

        print(f"  {tb}  local={local_count}  cloud={cloud_count}  diff={diff}")
        details.append(
            {"table": tb, "local": local_count, "cloud": cloud_count, "diff": diff}
        )

    return all_ok, details


# ============================================================
# Flask Routes
# ============================================================
@app.route("/api/ping")
def ping():
    return jsonify({"status": "ok", "timestamp": _now()})


# ---- Task CRUD ----

@app.route("/api/tasks", methods=["POST"])
def create_tasks():
    """
    Create 12 tasks (one per server) for a given month.
    Idempotent — if tasks already exist for this month, returns existing.
    Body: {"month": "202607"}
    """
    data = request.get_json(silent=True) or {}
    month = data.get("month", "").strip()

    auth_err = _check_auth()
    if auth_err:
        return auth_err

    if not _valid_month(month):
        return jsonify({"error": "Invalid 'month'. Use a real YYYYMM (e.g. 202608)."}), 400

    db = get_db()

    # Check if already exists
    existing = db.execute(
        "SELECT * FROM task_status WHERE month=? ORDER BY server", (month,)
    ).fetchall()
    if existing:
        return jsonify(
            {
                "month": month,
                "created": 0,
                "message": "Tasks already exist",
                "tasks": [dict(r) for r in existing],
            }
        )

    # Create 12 tasks (handle concurrent creation races via UNIQUE(month, server))
    now = _now()
    try:
        for server in RG_SERVERS:
            db.execute(
                "INSERT INTO task_status (month, server, status, created_at) VALUES (?, ?, 'new', ?)",
                (month, server, now),
            )
        db.commit()
    except sqlite3.IntegrityError:
        db.rollback()
        existing = db.execute(
            "SELECT * FROM task_status WHERE month=? ORDER BY server", (month,)
        ).fetchall()
        return jsonify(
            {
                "month": month,
                "created": 0,
                "message": "Tasks already exist (concurrent creation)",
                "tasks": [dict(r) for r in existing],
            }
        )

    tasks = db.execute(
        "SELECT * FROM task_status WHERE month=? ORDER BY server", (month,)
    ).fetchall()

    return jsonify(
        {"month": month, "created": len(tasks), "tasks": [dict(r) for r in tasks]}
    )


@app.route("/api/tasks", methods=["GET"])
def list_tasks():
    """List tasks. Filters: month, server, status."""
    month = request.args.get("month")
    server = request.args.get("server")
    status = request.args.get("status")

    query = "SELECT * FROM task_status WHERE 1=1"
    params = []
    if month:
        query += " AND month=?"
        params.append(month)
    if server:
        query += " AND server=?"
        params.append(server)
    if status:
        query += " AND status=?"
        params.append(status)
    query += " ORDER BY server"

    db = get_db()
    rows = db.execute(query, params).fetchall()
    return jsonify({"tasks": [dict(r) for r in rows]})


@app.route("/api/tasks/<int:task_id>", methods=["GET"])
def get_task(task_id):
    """Get task detail + table statuses."""
    db = get_db()
    task = db.execute("SELECT * FROM task_status WHERE id=?", (task_id,)).fetchone()
    if not task:
        return jsonify({"error": "Task not found"}), 404

    tables = db.execute(
        "SELECT * FROM task_table_status WHERE task_id=? ORDER BY table_name",
        (task_id,),
    ).fetchall()

    return jsonify({"task": dict(task), "tables": [dict(r) for r in tables]})


@app.route("/api/tasks/<int:task_id>", methods=["PATCH"])
def update_task(task_id):
    """
    Update task status.
    - status='progress': atomic claim (WHERE status='new')
    - status='complete'/'failed': update (WHERE status='progress')
    Body: {"status": "progress", "server": "lweb-rg-001"}
    """
    auth_err = _check_auth()
    if auth_err:
        return auth_err

    data = request.get_json(silent=True) or {}
    new_status = data.get("status", "").strip()
    server = data.get("server", "").strip()
    error_msg = data.get("error_msg", "")

    if new_status not in ("progress", "complete", "failed"):
        return jsonify({"error": "Invalid status"}), 400

    db = get_db()
    now = _now()

    # A task can only be claimed/updated by its own server.
    task = db.execute("SELECT * FROM task_status WHERE id=?", (task_id,)).fetchone()
    if not task:
        return jsonify({"error": "Task not found"}), 404
    if not server:
        return jsonify({"error": "server is required"}), 400
    if server != task["server"]:
        return jsonify(
            {
                "updated": False,
                "message": f"Server mismatch: task {task_id} belongs to {task['server']}, not {server}",
            }
        )

    if new_status == "progress":
        # Atomic claim — only succeeds if current status is 'new'
        cur = db.execute(
            "UPDATE task_status SET status='progress', started_at=? "
            "WHERE id=? AND status='new'",
            (now, task_id),
        )
        db.commit()
        if cur.rowcount == 0:
            return jsonify({"claimed": False, "message": "Task not in 'new' state"})

        # Create table_status entries if not exist
        month = task["month"]
        tables = [t.format(m=month) for t in RG_TABLES]
        for tb in tables:
            db.execute(
                "INSERT OR IGNORE INTO task_table_status (task_id, table_name, status, updated_at) "
                "VALUES (?, ?, 'pending', ?)",
                (task_id, tb, now),
            )
        db.commit()

        return jsonify({"claimed": True, "task": dict(task)})

    elif new_status == "complete":
        cur = db.execute(
            "UPDATE task_status SET status='complete', completed_at=? "
            "WHERE id=? AND status='progress'",
            (now, task_id),
        )
        db.commit()
        if cur.rowcount == 0:
            return jsonify({"updated": False, "message": "Task not in 'progress' state"})
        return jsonify({"updated": True})

    elif new_status == "failed":
        cur = db.execute(
            "UPDATE task_status SET status='failed', completed_at=?, error_msg=? "
            "WHERE id=? AND status='progress'",
            (now, error_msg, task_id),
        )
        db.commit()
        if cur.rowcount == 0:
            return jsonify({"updated": False, "message": "Task not in 'progress' state"})
        return jsonify({"updated": True})


@app.route("/api/tasks/<int:task_id>/tables", methods=["POST"])
def update_table_status(task_id):
    """
    Update table-level status.
    Body: {"table_name": "...", "status": "complete", "rows_local": 123, "rows_s3": 123, "error_msg": ""}
    """
    auth_err = _check_auth()
    if auth_err:
        return auth_err

    data = request.get_json(silent=True) or {}
    table_name = data.get("table_name", "")
    status = data.get("status", "")
    rows_local = data.get("rows_local", 0)
    rows_s3 = data.get("rows_s3", 0)
    error_msg = data.get("error_msg", "")

    if not table_name or not status:
        return jsonify({"error": "table_name and status required"}), 400

    db = get_db()
    task = db.execute("SELECT * FROM task_status WHERE id=?", (task_id,)).fetchone()
    if not task:
        return jsonify({"error": "Task not found"}), 404
    now = _now()
    db.execute(
        "INSERT INTO task_table_status (task_id, table_name, status, rows_local, rows_s3, error_msg, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(task_id, table_name) DO UPDATE SET "
        "status=excluded.status, rows_local=excluded.rows_local, "
        "rows_s3=excluded.rows_s3, error_msg=excluded.error_msg, updated_at=excluded.updated_at",
        (task_id, table_name, status, rows_local, rows_s3, error_msg, now),
    )
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/tasks/<int:task_id>/tables", methods=["GET"])
def get_table_statuses(task_id):
    db = get_db()
    rows = db.execute(
        "SELECT * FROM task_table_status WHERE task_id=? ORDER BY table_name",
        (task_id,),
    ).fetchall()
    return jsonify({"tables": [dict(r) for r in rows]})


@app.route("/api/tasks/<int:task_id>/reset", methods=["POST"])
def reset_task(task_id):
    """Reset task to 'new' (for retry after failure/timeout)."""
    auth_err = _check_auth()
    if auth_err:
        return auth_err

    db = get_db()
    now = _now()
    db.execute(
        "UPDATE task_status SET status='new', started_at=NULL, completed_at=NULL, error_msg='' "
        "WHERE id=?",
        (task_id,),
    )
    db.execute(
        "UPDATE task_table_status SET status='pending', rows_local=0, rows_s3=0, error_msg='' "
        "WHERE task_id=?",
        (task_id,),
    )
    db.commit()
    return jsonify({"ok": True, "message": "Task reset to 'new'"})


# ---- Heartbeat ----

@app.route("/api/heartbeat", methods=["POST"])
def heartbeat():
    auth_err = _check_auth()
    if auth_err:
        return auth_err

    data = request.get_json(silent=True) or {}
    server = data.get("server", "")
    hostname = data.get("hostname", "")
    ip = data.get("ip", "")

    if not server:
        return jsonify({"error": "server required"}), 400

    db = get_db()
    db.execute(
        "INSERT INTO worker_heartbeat (server, last_seen, hostname, ip) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(server) DO UPDATE SET last_seen=excluded.last_seen, "
        "hostname=excluded.hostname, ip=excluded.ip",
        (server, _now(), hostname, ip),
    )
    db.commit()
    return jsonify({"ok": True})


# ---- Status ----

@app.route("/api/status")
def status():
    """Overall status summary."""
    month = request.args.get("month")
    db = get_db()

    if not month:
        row = db.execute(
            "SELECT month FROM task_status ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        month = row["month"] if row else None

    tasks = []
    finalize = None
    if month:
        tasks = [
            dict(r)
            for r in db.execute(
                "SELECT * FROM task_status WHERE month=? ORDER BY server", (month,)
            ).fetchall()
        ]
        fin = db.execute(
            "SELECT * FROM finalize_log WHERE month=?", (month,)
        ).fetchone()
        if fin:
            finalize = dict(fin)
            finalize["details"] = json.loads(fin["details"]) if fin["details"] else []

    # Workers
    workers = [
        dict(r)
        for r in db.execute(
            "SELECT * FROM worker_heartbeat ORDER BY server"
        ).fetchall()
    ]

    # Determine cycle state
    cycle_state = "idle"
    if tasks:
        statuses = [t["status"] for t in tasks]
        if all(s in ("complete", "failed") for s in statuses):
            cycle_state = "finalized" if finalize else "ready_for_finalize"
        elif any(s == "progress" for s in statuses):
            cycle_state = "running"
        elif any(s == "new" for s in statuses):
            cycle_state = "dispatched"

    return jsonify(
        {
            "month": month,
            "cycle_state": cycle_state,
            "tasks": tasks,
            "workers": workers,
            "finalize": finalize,
        }
    )


@app.route("/api/workers")
def workers():
    db = get_db()
    rows = db.execute("SELECT * FROM worker_heartbeat ORDER BY server").fetchall()
    now = datetime.now()
    result = []
    for r in rows:
        d = dict(r)
        try:
            last = datetime.strptime(r["last_seen"], "%Y-%m-%d %H:%M:%S")
            d["online"] = (now - last).total_seconds() < 900  # 15 min
        except (ValueError, TypeError):
            d["online"] = False
        result.append(d)
    return jsonify({"workers": result})


@app.route("/api/finalize/<month>", methods=["POST"])
def manual_finalize(month):
    """Manually trigger finalize for a month."""
    auth_err = _check_auth()
    if auth_err:
        return auth_err

    if not _valid_month(month):
        return jsonify({"error": "Invalid month. Use YYYYMM (e.g. 202608)."}), 400

    result = do_finalize(month)
    return jsonify(result)


# ============================================================
# Finalize logic
# ============================================================
def do_finalize(target_month):
    """Check all 12 tasks done → comparison → alert."""
    conn = _get_db_thread()
    try:
        # Check not already finalized
        fin = conn.execute(
            "SELECT * FROM finalize_log WHERE month=?", (target_month,)
        ).fetchone()
        if fin:
            return {"status": "already_finalized", "month": target_month}

        tasks = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM task_status WHERE month=? ORDER BY server",
                (target_month,),
            ).fetchall()
        ]

        if len(tasks) != 12:
            return {"status": "not_all_created", "month": target_month}

        # Check all done
        not_done = [t for t in tasks if t["status"] not in ("complete", "failed")]
        if not_done:
            return {
                "status": "not_all_done",
                "month": target_month,
                "pending": [t["server"] for t in not_done],
            }

        failed_servers = [t for t in tasks if t["status"] == "failed"]

        if failed_servers:
            names = [t["server"] for t in failed_servers]
            send_alert(
                "RG export completed with failures",
                f"Month: {target_month}\nFailed servers: {names}",
                "WARNING",
                ["rg-parquet", "export", "partial-failure"],
            )
            conn.execute(
                "INSERT INTO finalize_log (month, status, details, created_at) VALUES (?, ?, ?, ?)",
                (
                    target_month,
                    "partial_failure",
                    json.dumps({"failed_servers": names}),
                    _now(),
                ),
            )
            conn.commit()
            return {"status": "partial_failure", "failed_servers": names}

        # All complete → run comparison
        print(f"[FINALIZE] Running comparison for {target_month}")
        all_ok, details = run_comparison(target_month)

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
            send_alert(
                "RG export & validation completed",
                f"Month: {target_month}\nAll 12 servers finished. {len(details)} tables validated OK.\n"
                + "\n".join(lines),
                "INFO",
                ["rg-parquet", "export", "validation", "success"],
            )
            fin_status = "success"
        else:
            send_alert(
                "RG validation MISMATCH",
                f"Month: {target_month}\n{mismatch_count}/{len(details)} tables have mismatches!\n"
                + "\n".join(lines),
                "CRITICAL",
                ["rg-parquet", "export", "validation", "failure", "p0"],
            )
            fin_status = "mismatch"

        conn.execute(
            "INSERT INTO finalize_log (month, status, details, created_at) VALUES (?, ?, ?, ?)",
            (target_month, fin_status, json.dumps(details), _now()),
        )
        conn.commit()

        return {"status": fin_status, "comparison_ok": all_ok, "details": details}

    finally:
        conn.close()


# ============================================================
# Background Thread — timeout + finalize detection
# ============================================================
def background_loop():
    """Daemon thread: timeout check every 5 min, finalize check every 60s."""
    finalize_check_interval = 60
    timeout_check_interval = 300
    last_timeout_check = 0

    while True:
        now = time.time()

        # ---- Finalize check ----
        try:
            conn = _get_db_thread()
            # Find months where all 12 tasks are done but not finalized
            months = [
                r["month"]
                for r in conn.execute(
                    "SELECT DISTINCT month FROM task_status "
                    "WHERE month NOT IN (SELECT month FROM finalize_log)"
                ).fetchall()
            ]
            for m in months:
                tasks = conn.execute(
                    "SELECT status FROM task_status WHERE month=?", (m,)
                ).fetchall()
                if len(tasks) == 12 and all(
                    t["status"] in ("complete", "failed") for t in tasks
                ):
                    print(f"[BG] All 12 done for {m}, triggering finalize")
                    conn.close()
                    do_finalize(m)
                    conn = _get_db_thread()
            conn.close()
        except Exception as exc:
            print(f"[BG] Finalize check error: {exc}")

        # ---- Timeout check ----
        if now - last_timeout_check > timeout_check_interval:
            last_timeout_check = now
            try:
                conn = _get_db_thread()
                cutoff = (datetime.now() - timedelta(hours=TASK_TIMEOUT_HOURS)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                rows = conn.execute(
                    "SELECT id, server, month, started_at FROM task_status "
                    "WHERE status='progress' AND started_at < ?",
                    (cutoff,),
                ).fetchall()
                for r in rows:
                    print(f"[BG] Timeout: task {r['id']} ({r['server']}) for {r['month']}")
                    conn.execute(
                        "UPDATE task_status SET status='timeout', error_msg='Auto-timeout: started at "
                        + str(r["started_at"])
                        + "' WHERE id=?",
                        (r["id"],),
                    )
                    send_alert(
                        "RG export task TIMEOUT",
                        f"Server: {r['server']}\nMonth: {r['month']}\n"
                        f"Started: {r['started_at']}\n"
                        f"Exceeded {TASK_TIMEOUT_HOURS}h. Task marked as 'timeout'.\n"
                        f"Use /api/tasks/{r['id']}/reset to retry.",
                        "WARNING",
                        ["rg-parquet", "export", "timeout"],
                    )
                conn.commit()
                conn.close()
            except Exception as exc:
                print(f"[BG] Timeout check error: {exc}")

        time.sleep(finalize_check_interval)


# ============================================================
# Dashboard
# ============================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RG Export Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500&display=swap" rel="stylesheet">
<style>
:root {
  --bg:#09090b;--surface:#18181b;--surface2:#27272a;--border:#3f3f46;
  --muted:#a1a1aa;--fg:#fafafa;--accent:#6366f1;--accent-glow:rgba(99,102,241,.25);
  --green:#22c55e;--green-bg:rgba(34,197,94,.12);
  --yellow:#eab308;--yellow-bg:rgba(234,179,8,.12);
  --red:#ef4444;--red-bg:rgba(239,68,68,.12);
  --blue:#3b82f6;--blue-bg:rgba(59,130,246,.12);
  --purple:#a855f7;--purple-bg:rgba(168,85,247,.12);
  --radius:14px;--radius-sm:8px;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Inter',-apple-system,sans-serif;background:var(--bg);color:var(--fg);min-height:100vh;padding:32px 40px}
@media(max-width:768px){body{padding:16px}}
header{display:flex;align-items:center;justify-content:space-between;margin-bottom:28px;flex-wrap:wrap;gap:12px}
header h1{font-size:1.4rem;font-weight:700;letter-spacing:-.02em}
header h1 span{color:var(--accent)}
.refresh-badge{display:flex;align-items:center;gap:6px;font-size:.78rem;color:var(--muted);background:var(--surface);padding:5px 12px;border-radius:20px;border:1px solid var(--border)}
.refresh-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px}
@media(max-width:900px){.stats{grid-template-columns:repeat(2,1fr)}}
.stat-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px 22px;position:relative;overflow:hidden}
.stat-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.stat-card.s-blue::before{background:var(--blue)}
.stat-card.s-green::before{background:var(--green)}
.stat-card.s-yellow::before{background:var(--yellow)}
.stat-card.s-red::before{background:var(--red)}
.stat-label{font-size:.72rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-bottom:6px}
.stat-value{font-size:1.6rem;font-weight:700;font-family:'JetBrains Mono','Inter',monospace}
.stat-sub{font-size:.72rem;color:var(--muted);margin-top:3px}

.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:22px;margin-bottom:20px}
.card-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px}
.card-header h2{font-size:.88rem;font-weight:600;text-transform:uppercase;letter-spacing:.04em;color:var(--muted)}
.card-header .count{font-size:.76rem;color:var(--muted);background:var(--surface2);padding:3px 10px;border-radius:12px}

.server-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:10px}
.server-card{border-radius:var(--radius-sm);padding:14px;border:1px solid var(--border);cursor:pointer;transition:all .2s;position:relative}
.server-card:hover{border-color:var(--accent);transform:translateY(-2px)}
.server-card.s-new{background:linear-gradient(135deg,var(--blue-bg),transparent)}
.server-card.s-progress{background:linear-gradient(135deg,var(--yellow-bg),transparent)}
.server-card.s-complete{background:linear-gradient(135deg,var(--green-bg),transparent)}
.server-card.s-failed{background:linear-gradient(135deg,var(--red-bg),transparent)}
.server-card.s-timeout{background:linear-gradient(135deg,var(--purple-bg),transparent)}
.server-name{font-size:.78rem;font-weight:600;font-family:'JetBrains Mono',monospace;margin-bottom:4px}
.server-status{display:flex;align-items:center;gap:5px;font-size:.7rem}
.server-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.server-dot.new{background:var(--blue)}
.server-dot.progress{background:var(--yellow);animation:pulse 1.5s infinite}
.server-dot.complete{background:var(--green)}
.server-dot.failed{background:var(--red)}
.server-dot.timeout{background:var(--purple)}
.server-progress{font-size:.65rem;color:var(--muted);margin-top:4px;font-family:'JetBrains Mono',monospace}

.pill{display:inline-flex;align-items:center;gap:5px;padding:3px 12px;border-radius:20px;font-size:.76rem;font-weight:600}
.pill-ok{background:var(--green-bg);color:var(--green)}
.pill-warn{background:var(--yellow-bg);color:var(--yellow)}
.pill-err{background:var(--red-bg);color:var(--red)}
.pill-info{background:var(--blue-bg);color:var(--blue)}
.pill-idle{background:var(--surface2);color:var(--muted)}

.actions{display:flex;gap:8px;align-items:center;margin-top:14px}
.actions input{flex:1;background:var(--surface2);border:1px solid var(--border);color:var(--fg);padding:8px 14px;border-radius:var(--radius-sm);font-size:.82rem;font-family:'JetBrains Mono',monospace;outline:none}
.actions input:focus{border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-glow)}
.actions button{padding:8px 18px;border-radius:var(--radius-sm);font-size:.8rem;font-weight:600;cursor:pointer;border:none;transition:all .15s}
.btn-primary{background:var(--accent);color:#fff}
.btn-primary:hover{filter:brightness(1.15);box-shadow:0 0 14px var(--accent-glow)}
.btn-danger{background:var(--red-bg);color:var(--red);border:1px solid rgba(239,68,68,.3)}
.btn-danger:hover{background:rgba(239,68,68,.2)}
#msg{font-size:.76rem;margin-top:8px;min-height:16px;color:var(--muted)}

.table-detail{display:none;margin-top:12px}
.table-detail.show{display:block}
table.tbl{width:100%;border-collapse:collapse;font-size:.74rem}
table.tbl th{text-align:left;padding:6px 10px;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border);text-transform:uppercase;font-size:.66rem;letter-spacing:.04em}
table.tbl td{padding:6px 10px;border-bottom:1px solid var(--surface2);font-family:'JetBrains Mono',monospace}
table.tbl td.ok{color:var(--green)}
table.tbl td.err{color:var(--red)}
table.tbl td.warn{color:var(--yellow)}

.fin-detail{margin-top:10px;max-height:400px;overflow-y:auto}
.fin-detail table{width:100%;border-collapse:collapse;font-size:.74rem}
.fin-detail th{text-align:left;padding:6px 10px;color:var(--muted);font-weight:600;border-bottom:1px solid var(--border);text-transform:uppercase;font-size:.66rem}
.fin-detail td{padding:6px 10px;border-bottom:1px solid var(--surface2);font-family:'JetBrains Mono',monospace}

.footer{text-align:center;color:var(--muted);font-size:.7rem;margin-top:10px;opacity:.6}
</style>
</head>
<body>
<header>
  <div>
    <h1>RG Export <span>Dashboard</span></h1>
    <div style="font-size:.76rem;color:var(--muted);margin-top:2px">Polling Architecture &middot; No Celery &middot; No Redis</div>
  </div>
  <div class="refresh-badge"><span class="refresh-dot"></span> Live &middot; 30s</div>
</header>

<div class="stats">
  <div class="stat-card s-blue">
    <div class="stat-label">Target Month</div>
    <div class="stat-value" style="color:var(--blue)" id="sv-month">&mdash;</div>
    <div class="stat-sub" id="sv-month-sub">&mdash;</div>
  </div>
  <div class="stat-card" id="stat-cycle">
    <div class="stat-label">Cycle State</div>
    <div class="stat-value" id="sv-cycle">&mdash;</div>
    <div class="stat-sub" id="sv-cycle-sub">&mdash;</div>
  </div>
  <div class="stat-card s-green">
    <div class="stat-label">Complete</div>
    <div class="stat-value" style="color:var(--green)" id="sv-done">0/12</div>
    <div class="stat-sub">servers finished</div>
  </div>
  <div class="stat-card" id="stat-workers">
    <div class="stat-label">Workers Online</div>
    <div class="stat-value" id="sv-workers">0/12</div>
    <div class="stat-sub">active heartbeats</div>
  </div>
</div>

<div class="card">
  <div class="card-header">
    <h2>Servers</h2>
    <span class="count" id="server-count">0 / 12</span>
  </div>
  <div class="server-grid" id="servers"></div>
  <div class="actions">
    <input id="dispatch-month" placeholder="202608" maxlength="6">
    <button class="btn-primary" onclick="doDispatch()">Dispatch</button>
    <button class="btn-danger" onclick="doFinalize()">Finalize</button>
  </div>
  <div id="msg"></div>
</div>

<div id="finalize-section"></div>

<div class="footer">Last update: <span id="last-update">&mdash;</span></div>

<script>
const SERVERS=[...Array(12)].map((_,i)=>'lweb-rg-'+String(i+1).padStart(3,'0'));

function pill(cls,txt){return `<span class="pill pill-${cls}">${txt}</span>`}

async function fetchStatus(){
  try{
    const r=await fetch('/api/status');
    const d=await r.json();
    const tasks=d.tasks||[];
    const workers=d.workers||[];

    // Stats
    document.getElementById('sv-month').textContent=d.month||'—';
    document.getElementById('sv-month-sub').textContent=d.cycle_state||'—';

    const done=tasks.filter(t=>['complete','failed'].includes(t.status)).length;
    const inProg=tasks.filter(t=>t.status==='progress').length;
    const failed=tasks.filter(t=>t.status==='failed').length;
    document.getElementById('sv-done').textContent=done+'/12';

    // Cycle state
    const cycleEl=document.getElementById('sv-cycle');
    const cycleSub=document.getElementById('sv-cycle-sub');
    const cycleMap={
      idle:['idle','No tasks dispatched'],
      dispatched:['info','Waiting for workers to pick up'],
      running:['warn',`${inProg} in progress`],
      ready_for_finalize:['warn','All done, awaiting finalize'],
      finalized: failed>0?['err',`${failed} servers failed`]:['ok','Validation complete']
    };
    const [cls,sub]=cycleMap[d.cycle_state]||['idle','—'];
    cycleEl.innerHTML=pill(cls,d.cycle_state||'—');
    cycleSub.textContent=sub;

    // Workers online
    const now=new Date();
    let online=0;
    const workerMap={};
    workers.forEach(w=>{
      let last=null;
      if(w.last_seen){last=new Date(w.last_seen.replace(' ','T'))}
      w.online = last && (now-last)<900000;
      if(w.online) online++;
      workerMap[w.server]=w;
    });
    document.getElementById('sv-workers').textContent=online+'/12';

    // Server grid
    const grid=document.getElementById('servers');
    let html='';
    const taskMap={};
    tasks.forEach(t=>{taskMap[t.server]=t});

    SERVERS.forEach(srv=>{
      const t=taskMap[srv];
      const w=workerMap[srv];
      let status='new',progressText='';
      if(t){
        status=t.status;
        if(t.status==='complete') progressText='16/16 tables';
        else if(t.status==='progress') progressText='in progress...';
        else if(t.status==='failed') progressText=t.error_msg||'failed';
        else if(t.status==='timeout') progressText='timed out';
        else if(t.status==='new') progressText='waiting...';
      } else {
        status='idle';
        progressText='no task';
      }
      const onlineDot = w && w.online;
      const sCls = status==='idle'?'':`s-${status}`;
      html+=`<div class="server-card ${sCls}" onclick="toggleDetail('${srv}',${t?t.id:'null'})">
        <div class="server-name">${srv} ${onlineDot?'<span style="color:var(--green);font-size:.6rem">●</span>':'<span style="color:var(--red);font-size:.6rem">○</span>'}</div>
        <div class="server-status"><span class="server-dot ${status==='idle'?'':status}"></span>${status}</div>
        <div class="server-progress">${progressText}</div>
        <div class="table-detail" id="detail-${srv}"></div>
      </div>`;
    });
    grid.innerHTML=html;
    document.getElementById('server-count').textContent=done+' / 12';

    // Finalize section
    const finSection=document.getElementById('finalize-section');
    if(d.finalize){
      const fin=d.finalize;
      const details=fin.details||[];
      let finHtml='<div class="card"><div class="card-header"><h2>Finalize Results</h2>';
      finHtml+=pill(fin.status==='success'?'ok':fin.status==='mismatch'?'err':'warn',fin.status);
      finHtml+='</div><div class="fin-detail"><table><tr><th>Table</th><th>Local</th><th>Cloud</th><th>Diff</th></tr>';
      details.forEach(d=>{
        const diff=d.diff;
        let cls='';
        if(diff===0) cls='ok';
        else if(diff==='-'||diff==='ERROR') cls='err';
        else cls='warn';
        finHtml+=`<tr><td>${d.table}</td><td>${d.local||'—'}</td><td>${d.cloud||'—'}</td><td class="${cls}">${diff}</td></tr>`;
      });
      finHtml+='</table></div></div>';
      finSection.innerHTML=finHtml;
    } else {
      finSection.innerHTML='';
    }

    document.getElementById('last-update').textContent=new Date().toLocaleTimeString();
  }catch(e){
    document.getElementById('msg').textContent='Error: '+e.message;
  }
}

async function toggleDetail(srv,taskId){
  const el=document.getElementById('detail-'+srv);
  if(el.classList.contains('show')){el.classList.remove('show');return}
  if(!taskId){el.innerHTML='<span style="color:var(--muted);font-size:.72rem">No task</span>';el.classList.add('show');return}
  try{
    const r=await fetch(`/api/tasks/${taskId}/tables`);
    const d=await r.json();
    const tables=d.tables||[];
    if(!tables.length){el.innerHTML='<span style="color:var(--muted);font-size:.72rem">No table data yet</span>';el.classList.add('show');return}
    let h='<table class="tbl"><tr><th>Table</th><th>Status</th><th>Local</th><th>S3</th></tr>';
    tables.forEach(t=>{
      let cls='';
      if(t.status==='complete') cls='ok';
      else if(t.status==='failed') cls='err';
      else if(t.status==='skipped') cls='';
      else cls='warn';
      h+=`<tr><td style="font-size:.66rem">${t.table_name.replace(/_\d{6}_/,'_XX_')}</td><td class="${cls}">${t.status}</td><td>${t.rows_local||0}</td><td>${t.rows_s3||0}</td></tr>`;
    });
    h+='</table>';
    el.innerHTML=h;
    el.classList.add('show');
  }catch(e){el.innerHTML='Error: '+e.message;el.classList.add('show')}
}

async function doDispatch(){
  const m=document.getElementById('dispatch-month').value.trim();
  if(!m){showMsg('Enter a month (YYYYMM)');return}
  try{
    const r=await fetch('/api/tasks',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({month:m})});
    const d=await r.json();
    if(d.created>0) showMsg(`Created ${d.created} tasks for ${m}`);
    else showMsg(`Tasks already exist for ${m}`);
    fetchStatus();
  }catch(e){showMsg('Error: '+e.message)}
}

async function doFinalize(){
  const m=document.getElementById('dispatch-month').value.trim();
  if(!m){showMsg('Enter a month (YYYYMM) to finalize');return}
  showMsg('Triggering finalize...');
  try{
    const r=await fetch(`/api/finalize/${m}`,{method:'POST'});
    const d=await r.json();
    showMsg(`Finalize: ${d.status}`);
    fetchStatus();
  }catch(e){showMsg('Error: '+e.message)}
}

function showMsg(t){document.getElementById('msg').textContent=t;setTimeout(()=>{document.getElementById('msg').textContent=''},5000)}

fetchStatus();
setInterval(fetchStatus,30000);
</script>
</body>
</html>"""


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    init_db()
    print(f"[Controller] DB: {DB_PATH}")
    print(f"[Controller] Port: {PORT}")
    print(f"[Controller] Timeout: {TASK_TIMEOUT_HOURS}h")

    t = threading.Thread(target=background_loop, daemon=True)
    t.start()

    app.run(host="0.0.0.0", port=PORT, debug=False)
