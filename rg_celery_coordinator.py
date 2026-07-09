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
from flask import Flask, jsonify, render_template_string, request

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


# ---- Dashboard ----
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>RG Export Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px 32px}
h1{font-size:1.6rem;margin-bottom:4px}
.sub{color:#94a3b8;font-size:.85rem;margin-bottom:24px}
.grid{display:grid;grid-template-columns:1fr 2fr;gap:24px}
@media(max-width:900px){.grid{grid-template-columns:1fr}}
.card{background:#1e293b;border-radius:10px;padding:20px;box-shadow:0 2px 8px rgba(0,0,0,.3)}
.card h2{font-size:1rem;text-transform:uppercase;letter-spacing:.05em;color:#64748b;margin-bottom:14px}
.row{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #334155}
.row:last-child{border-bottom:none}
.label{color:#94a3b8;font-size:.9rem}
.value{color:#e2e8f0;font-weight:600;font-size:.9rem}
.badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:.78rem;font-weight:600}
.badge-green{background:#166534;color:#4ade80}
.badge-yellow{background:#713f12;color:#facc15}
.badge-red{background:#7f1d1d;color:#f87171}
.badge-gray{background:#334155;color:#94a3b8}
.workers{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.worker-dot{width:12px;height:12px;border-radius:50%;display:inline-block;margin-right:6px;flex-shrink:0}
.worker-dot.online{background:#4ade80}
.worker-dot.offline{background:#f87171}
.worker-item{display:flex;align-items:center;padding:5px 12px;background:#0f172a;border-radius:8px;font-size:.82rem;gap:4px}
.pulse{animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.update{color:#64748b;font-size:.75rem;margin-top:20px;text-align:center}
.btn{border:none;padding:6px 18px;border-radius:6px;font-size:.82rem;cursor:pointer;font-weight:600;margin-right:8px}
.btn-red{background:#7f1d1d;color:#f87171}
.btn-red:hover{background:#991b1b}
.form-inline{display:flex;align-items:center;gap:8px;margin-bottom:12px}
.form-inline input{background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:5px 10px;border-radius:6px;font-size:.82rem;width:100px}
.form-inline button{background:#1e3a5f;color:#60a5fa;border:none;padding:5px 14px;border-radius:6px;cursor:pointer;font-weight:600;font-size:.82rem}
.form-inline button:hover{background:#1e40af}
#msg{font-size:.8rem;margin-top:8px;min-height:18px}
</style>
</head>
<body>
<h1>📊 RG Export Dashboard</h1>
<div class="sub">Coordinator — auto-refresh every 30s</div>
<div class="grid">
  <div class="card">
    <h2>Dispatch State</h2>
    <div class="row"><span class="label">Dispatched Month</span><span class="value" id="ds-month">—</span></div>
    <div class="row"><span class="label">Running Month</span><span class="value" id="ds-running">—</span></div>
    <div class="row"><span class="label">Status</span><span class="value" id="ds-status">—</span></div>
    <div class="row"><span class="label">Completed</span><span class="value" id="ds-completed">—</span></div>
    <hr style="border-color:#334155;margin:14px 0">
    <div class="form-inline">
      <label style="font-size:.85rem;color:#94a3b8">Trigger:</label>
      <input id="dispatch-month" placeholder="YYYYMM" maxlength="6">
      <button onclick="doDispatch()">Dispatch</button>
      <button class="btn btn-red" onclick="doReset()">Reset</button>
    </div>
    <div id="msg"></div>
  </div>
  <div class="card">
    <h2>Workers (<span id="worker-count">0</span> online)</h2>
    <div class="workers" id="workers"></div>
  </div>
</div>
<div class="update">Last update: <span id="last-update">—</span></div>
<script>
const RG_SERVERS=[...Array(12)].map((_,i)=>'lweb-rg-'+String(i+1).padStart(3,'0'));
async function fetchStatus(){
  try{
    const r=await fetch('/api/status');
    const d=await r.json();
    const s=d.dispatch_state||{};
    document.getElementById('ds-month').textContent=s.dispatched_month||'—';
    document.getElementById('ds-running').textContent=s.running_month||'—';
    const c=s.completed;
    const el=document.getElementById('ds-completed');
    el.innerHTML=c===true?'<span class="badge badge-green">YES</span>':'<span class="badge badge-gray">NO</span>';
    const st=document.getElementById('ds-status');
    if(c===true) st.innerHTML='<span class="badge badge-green">completed</span>';
    else if(s.running_month) st.innerHTML='<span class="badge badge-yellow pulse">running</span>';
    else if(s.dispatched_month) st.innerHTML='<span class="badge badge-gray">dispatched</span>';
    else st.innerHTML='<span class="badge badge-gray">idle</span>';
    const wk=d.workers||{};
    const online=new Set(Object.keys(wk).filter(w=>!w.includes('coordinator')).map(w=>w.split('@')[0]));
    let n=0;
    const container=document.getElementById('workers');
    container.innerHTML=RG_SERVERS.map(s=>{
      const isOnline=online.has(s);
      if(isOnline)n++;
      const dot=isOnline?'online':'offline';
      const tasks=wk[s+'@']||wk[Object.keys(wk).find(k=>k.startsWith(s))||'']||{};
      const active=tasks.active_tasks||0;
      const reserved=tasks.reserved_tasks||0;
      let extra='';
      if(active) extra=`<span style="color:#facc15">act:${active}</span>`;
      else if(reserved) extra=`<span style="color:#94a3b8">res:${reserved}</span>`;
      return `<div class="worker-item"><span class="worker-dot ${dot}"></span>${s} ${extra}</div>`;
    }).join('');
    document.getElementById('worker-count').textContent=n;
    document.getElementById('last-update').textContent=new Date().toLocaleTimeString();
  }catch(e){console.error(e)}
}
async function doDispatch(){
  const m=document.getElementById('dispatch-month').value.trim();
  if(!/^\d{6}$/.test(m)){document.getElementById('msg').innerHTML='<span style="color:#f87171">Invalid month format</span>';return}
  const r=await fetch('/api/dispatch?month='+m,{method:'POST'});
  const d=await r.json();
  const el=document.getElementById('msg');
  if(d.status==='dispatched') el.innerHTML='<span style="color:#4ade80">Dispatched '+m+' ✓</span>';
  else if(d.status==='rejected') el.innerHTML='<span style="color:#facc15">Rejected: '+d.reason+'</span>';
  else el.innerHTML='<span style="color:#f87171">'+JSON.stringify(d)+'</span>';
  setTimeout(fetchStatus,1500);
}
async function doReset(){
  if(!confirm('Reset dispatch state? This allows re-dispatch for the current month.'))return;
  await fetch('/api/reset',{method:'POST'});
  document.getElementById('msg').innerHTML='<span style="color:#4ade80">State reset ✓</span>';
  setTimeout(fetchStatus,1500);
}
fetchStatus();
setInterval(fetchStatus,30000);
</script>
</body>
</html>"""


@app.route("/dashboard", methods=["GET"])
def dashboard():
    return render_template_string(DASHBOARD_HTML)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting Coordinator API on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
