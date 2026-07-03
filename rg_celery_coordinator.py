"""
RG Export Coordinator — Flask Status API (thin wrapper)
========================================================
Provides read-only status endpoints for monitoring.
The real orchestration is done by Celery Beat + Workers.

Usage:
    python3 rg_celery_coordinator.py
    # listens on 0.0.0.0:{PORT}
"""

import json
import os
from datetime import datetime

from celery import Celery
from dotenv import load_dotenv
from flask import Flask, jsonify

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
DISPATCH_STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".dispatch_state.json"
)

app = Flask(__name__)

# Celery inspect (read-only)
celery_app = Celery("rg_export", broker=REDIS_URL, backend=REDIS_URL)


def _load_dispatch_state():
    try:
        with open(DISPATCH_STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


@app.route("/api/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


@app.route("/api/status", methods=["GET"])
def status():
    """Return dispatch state + worker ping info."""
    state = _load_dispatch_state()

    # Try to get worker stats from Celery
    workers_info = {}
    try:
        inspect = celery_app.control.inspect(timeout=5)
        active = inspect.active() or {}
        reserved = inspect.reserved() or {}
        stats = inspect.stats() or {}

        for worker_name in stats:
            w = {
                "active_tasks": len(active.get(worker_name, [])),
                "reserved_tasks": len(reserved.get(worker_name, [])),
            }
            ws = stats[worker_name]
            if ws:
                w["pool"] = ws.get("pool", {}).get("max-concurrency", "?")
            workers_info[worker_name] = w
    except Exception as exc:
        workers_info = {"error": str(exc)}

    return jsonify({
        "dispatch_state": state,
        "workers": workers_info,
    })


@app.route("/api/ready", methods=["GET"])
def ready():
    """Check if export is ready (same logic as beat, but manual)."""
    from rg_celery_app import _is_ready
    ready, current_month, reason = _is_ready()
    state = _load_dispatch_state()
    return jsonify({
        "ready": ready,
        "current_month": current_month,
        "reason": reason,
        "dispatched_month": state.get("dispatched_month"),
        "completed": state.get("completed", False),
    })


@app.route("/api/dispatch", methods=["POST"])
def manual_dispatch():
    """Manually trigger dispatch (bypasses beat schedule)."""
    from rg_celery_app import check_and_dispatch
    result = check_and_dispatch.delay()
    return jsonify({"task_id": result.id, "message": "Dispatch check queued"})


@app.route("/api/reset", methods=["POST"])
def reset_cycle():
    """Reset dispatch state for a new month."""
    try:
        os.remove(DISPATCH_STATE_FILE)
    except FileNotFoundError:
        pass
    return jsonify({"reset": True, "message": "Dispatch state cleared"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting Coordinator status API on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
