"""
RG Export Coordinator — Flask API + Dispatch Trigger
=====================================================
Thin status API. The real orchestration is done by Celery workers.

Usage:
    python3 rg_celery_coordinator.py
    # listens on 0.0.0.0:{PORT}
"""

import json
import os
from datetime import datetime

from celery import Celery
from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
DISPATCH_STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".dispatch_state.json"
)

app = Flask(__name__)

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
    """Return dispatch state + worker stats."""
    state = _load_dispatch_state()

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


@app.route("/api/dispatch", methods=["POST"])
def manual_dispatch():
    """
    Trigger export dispatch for a given month.
    Query: ?month=YYYYMM  (required)
    Idempotent — same month only dispatched once (via .dispatch_state.json).
    """
    target_month = request.args.get("month", "").strip()
    if not target_month or len(target_month) != 6 or not target_month.isdigit():
        return jsonify({"error": "Missing or invalid 'month' parameter. Use YYYYMM."}), 400

    state = _load_dispatch_state()
    dispatched_month = state.get("dispatched_month")

    if dispatched_month == target_month:
        if state.get("completed"):
            return jsonify({"status": "rejected", "reason": "already_completed", "month": target_month})
        if state.get("running_month") == target_month:
            return jsonify({"status": "rejected", "reason": "still_running", "month": target_month})
        return jsonify({"status": "rejected", "reason": "already_dispatched", "month": target_month})

    from rg_celery_app import dispatch_export
    result = dispatch_export.delay(target_month)

    return jsonify({
        "status": "dispatched",
        "month": target_month,
        "task_id": result.id,
    })


@app.route("/api/reset", methods=["POST"])
def reset_cycle():
    """Reset dispatch state for re-run (emergency use)."""
    try:
        os.remove(DISPATCH_STATE_FILE)
    except FileNotFoundError:
        pass
    return jsonify({"reset": True, "message": "Dispatch state cleared"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting Coordinator API on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
