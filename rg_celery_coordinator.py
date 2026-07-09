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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #09090b;
  --surface: #18181b;
  --surface2: #27272a;
  --border: #3f3f46;
  --muted: #a1a1aa;
  --fg: #fafafa;
  --accent: #6366f1;
  --accent-glow: rgba(99,102,241,.25);
  --green: #22c55e;
  --green-bg: rgba(34,197,94,.12);
  --yellow: #eab308;
  --yellow-bg: rgba(234,179,8,.12);
  --red: #ef4444;
  --red-bg: rgba(239,68,68,.12);
  --blue: #3b82f6;
  --radius: 14px;
  --radius-sm: 8px;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Inter',-apple-system,sans-serif;background:var(--bg);color:var(--fg);min-height:100vh;padding:40px 48px}
@media(max-width:768px){body{padding:20px 16px}}
header{display:flex;align-items:center;justify-content:space-between;margin-bottom:36px;flex-wrap:wrap;gap:16px}
header h1{font-size:1.5rem;font-weight:700;letter-spacing:-.02em}
header h1 span{color:var(--accent)}
.header-right{display:flex;align-items:center;gap:16px}
.refresh-badge{display:flex;align-items:center;gap:6px;font-size:.8rem;color:var(--muted);background:var(--surface);padding:6px 14px;border-radius:20px;border:1px solid var(--border)}
.refresh-dot{width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* Stat cards */
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:28px}
@media(max-width:900px){.stats{grid-template-columns:repeat(2,1fr)}}
.stat-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px 24px;position:relative;overflow:hidden}
.stat-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.stat-card.p-ending::before{background:var(--yellow)}
.stat-card.p-ok::before{background:var(--green)}
.stat-card.p-info::before{background:var(--blue)}
.stat-card.p-danger::before{background:var(--red)}
.stat-label{font-size:.75rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-bottom:8px}
.stat-value{font-size:1.8rem;font-weight:700;font-family:'JetBrains Mono','Inter',monospace;letter-spacing:-.03em}
.stat-value.green{color:var(--green)}
.stat-value.yellow{color:var(--yellow)}
.stat-value.red{color:var(--red)}
.stat-value.blue{color:var(--blue)}
.stat-value.muted{color:var(--muted)}
.stat-sub{font-size:.75rem;color:var(--muted);margin-top:4px}

/* Main grid */
.main-grid{display:grid;grid-template-columns:1fr 1.6fr;gap:24px;margin-bottom:28px}
@media(max-width:1000px){.main-grid{grid-template-columns:1fr}}
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:24px}
.card-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px}
.card-header h2{font-size:.9rem;font-weight:600;text-transform:uppercase;letter-spacing:.04em;color:var(--muted)}
.card-header .count{font-size:.8rem;color:var(--muted);background:var(--surface2);padding:3px 10px;border-radius:12px}

/* Status rows */
.status-rows{display:flex;flex-direction:column;gap:1px;background:var(--border);border-radius:var(--radius-sm);overflow:hidden}
.status-row{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;background:var(--surface2)}
.status-row .label{font-size:.85rem;color:var(--muted)}
.status-row .value{font-size:.85rem;font-weight:600}
.pill{display:inline-flex;align-items:center;gap:5px;padding:3px 12px;border-radius:20px;font-size:.78rem;font-weight:600}
.pill-ok{background:var(--green-bg);color:var(--green)}
.pill-warn{background:var(--yellow-bg);color:var(--yellow)}
.pill-err{background:var(--red-bg);color:var(--red)}
.pill-idle{background:var(--surface2);color:var(--muted)}
.pill-dot{width:6px;height:6px;border-radius:50%}
.pill-dot.green{background:var(--green)}
.pill-dot.yellow{background:var(--yellow);animation:pulse 1.5s infinite}
.pill-dot.red{background:var(--red)}
.pill-dot.gray{background:var(--muted)}

/* Actions */
.actions{margin-top:20px;display:flex;gap:8px;align-items:center}
.actions input{flex:1;background:var(--surface2);border:1px solid var(--border);color:var(--fg);padding:8px 14px;border-radius:var(--radius-sm);font-size:.85rem;font-family:'JetBrains Mono',monospace;outline:none;transition:border .2s}
.actions input:focus{border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-glow)}
.actions button{padding:8px 18px;border-radius:var(--radius-sm);font-size:.82rem;font-weight:600;cursor:pointer;border:none;transition:all .15s}
.btn-primary{background:var(--accent);color:#fff}
.btn-primary:hover{filter:brightness(1.15);box-shadow:0 0 16px var(--accent-glow)}
.btn-danger{background:var(--red-bg);color:var(--red);border:1px solid rgba(239,68,68,.3)}
.btn-danger:hover{background:rgba(239,68,68,.2)}
#msg{font-size:.78rem;margin-top:10px;min-height:18px}

/* Workers */
.worker-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(155px,1fr));gap:10px}
.worker-cell{border-radius:var(--radius-sm);padding:14px 16px;border:1px solid var(--border);transition:all .2s;position:relative;overflow:hidden}
.worker-cell::after{content:'';position:absolute;inset:0;border-radius:var(--radius-sm);opacity:0;transition:opacity .2s}
.worker-cell.online{background:linear-gradient(135deg,rgba(34,197,94,.06),transparent)}
.worker-cell.offline{background:linear-gradient(135deg,rgba(239,68,68,.05),transparent)}
.worker-cell.busy{background:linear-gradient(135deg,rgba(234,179,8,.06),transparent)}
.worker-name{font-size:.78rem;font-weight:600;font-family:'JetBrains Mono',monospace;margin-bottom:4px}
.worker-status{display:flex;align-items:center;gap:5px;font-size:.7rem;font-weight:500}
.worker-dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.worker-dot.online{background:var(--green);box-shadow:0 0 6px rgba(34,197,94,.4)}
.worker-dot.offline{background:var(--red)}
.worker-dot.busy{background:var(--yellow);box-shadow:0 0 6px rgba(234,179,8,.4)}
.worker-badge{font-size:.65rem;font-weight:600;padding:2px 8px;border-radius:10px;margin-top:6px;display:inline-block}
.worker-badge.busy{background:var(--yellow-bg);color:var(--yellow)}

.footer{text-align:center;color:var(--muted);font-size:.72rem;margin-top:12px;opacity:.6}
</style>
</head>
<body>
<header>
  <div>
    <h1>RG Export <span>Dashboard</span></h1>
    <div style="font-size:.8rem;color:var(--muted);margin-top:2px">Distributed Parquet Export Orchestrator</div>
  </div>
  <div class="header-right">
    <div class="refresh-badge"><span class="refresh-dot"></span> Live · 30s</div>
    <div style="font-size:.75rem;color:var(--muted)"><span id="last-update">—</span></div>
  </div>
</header>

<div class="stats">
  <div class="stat-card p-ok" id="stat-month">
    <div class="stat-label">Target Month</div>
    <div class="stat-value blue" id="sv-month">—</div>
    <div class="stat-sub">Dispatched</div>
  </div>
  <div class="stat-card" id="stat-status">
    <div class="stat-label">Status</div>
    <div class="stat-value muted" id="sv-status">Idle</div>
    <div class="stat-sub">—</div>
  </div>
  <div class="stat-card p-info">
    <div class="stat-label">Workers Online</div>
    <div class="stat-value green" id="sv-online">0</div>
    <div class="stat-sub">of 12</div>
  </div>
  <div class="stat-card" id="stat-tasks">
    <div class="stat-label">Active Tasks</div>
    <div class="stat-value muted" id="sv-tasks">0</div>
    <div class="stat-sub">across all workers</div>
  </div>
</div>

<div class="main-grid">
  <div class="card">
    <div class="card-header"><h2>Dispatch</h2></div>
    <div class="status-rows">
      <div class="status-row"><span class="label">Target Month</span><span class="value" id="ds-month">—</span></div>
      <div class="status-row"><span class="label">Running Month</span><span class="value" id="ds-running">—</span></div>
      <div class="status-row"><span class="label">Cycle State</span><span class="value" id="ds-state">—</span></div>
      <div class="status-row"><span class="label">Completed</span><span class="value" id="ds-completed">—</span></div>
    </div>
    <div class="actions">
      <input id="dispatch-month" placeholder="202608" maxlength="6">
      <button class="btn-primary" onclick="doDispatch()">Dispatch</button>
      <button class="btn-danger" onclick="doReset()">Reset</button>
    </div>
    <div id="msg"></div>
  </div>

  <div class="card">
    <div class="card-header"><h2>Workers</h2><span class="count" id="worker-count-label">0 / 12</span></div>
    <div class="worker-grid" id="workers"></div>
  </div>
</div>

<div class="footer">Last update: <span id="last-update">—</span></div>

<script>
const RG_SERVERS=[...Array(12)].map((_,i)=>'lweb-rg-'+String(i+1).padStart(3,'0'));

function pill(icon,cls,txt){return `<span class="pill pill-${cls}"><span class="pill-dot ${icon}"></span>${txt}</span>`}

async function fetchStatus(){
  try{
    const r=await fetch('/api/status');
    const d=await r.json();
    const s=d.dispatch_state||{};
    const dm=s.dispatched_month||'—'; const rm=s.running_month||'—';
    const completed=s.completed===true;

    // Details
    document.getElementById('ds-month').textContent=dm;
    document.getElementById('ds-running').textContent=rm;
    document.getElementById('ds-completed').innerHTML=completed ? pill('green','ok','Done') : pill('gray','idle','No');
    const stEl=document.getElementById('ds-state');
    if(completed) stEl.innerHTML=pill('green','ok','Completed');
    else if(rm) stEl.innerHTML=pill('yellow','warn','Running');
    else if(dm && dm!=='—') stEl.innerHTML=pill('gray','idle','Dispatched');
    else stEl.innerHTML=pill('gray','idle','Idle');

    // Stat cards
    document.getElementById('sv-month').textContent=dm;
    const statusCard=document.getElementById('stat-status');
    const svStatus=document.getElementById('sv-status');
    statusCard.className='stat-card';
    if(completed){svStatus.textContent='Completed';svStatus.className='stat-value green';statusCard.classList.add('p-ok')}
    else if(rm){svStatus.textContent='Running';svStatus.className='stat-value yellow';statusCard.classList.add('p-ending')}
    else{svStatus.textContent='Idle';svStatus.className='stat-value muted'}

    // Workers
    const wk=d.workers||{};
    const workerKeys=Object.keys(wk).filter(w=>!w.includes('coordinator'));
    const onlineSet=new Set(workerKeys.map(w=>w.split('@')[0]));

    let onlineCount=0, totalActive=0;
    const container=document.getElementById('workers');
    container.innerHTML=RG_SERVERS.map(s=>{
      const isOnline=onlineSet.has(s); if(isOnline)onlineCount++;
      const info=wk[Object.keys(wk).find(k=>k.startsWith(s))||'']||{};
      const active=info.active_tasks||0; totalActive+=active;
      let cls='offline', statusTxt='offline', dot='offline';
      if(isOnline && active>0){cls='busy';statusTxt='exporting';dot='busy'}
      else if(isOnline){cls='online';statusTxt='idle';dot='online'}
      let badge='';
      if(active>0) badge=`<div class="worker-badge busy">act:${active}</div>`;
      return `<div class="worker-cell ${cls}"><div class="worker-name">${s}</div><div class="worker-status"><span class="worker-dot ${dot}"></span>${statusTxt}</div>${badge}</div>`;
    }).join('');

    document.getElementById('worker-count-label').textContent=`${onlineCount} / 12`;
    document.getElementById('sv-online').textContent=onlineCount;
    document.getElementById('sv-tasks').textContent=totalActive;
    const tasksCard=document.getElementById('stat-tasks');
    document.getElementById('sv-tasks').className='stat-value '+(totalActive>0?'yellow':'muted');
    tasksCard.className='stat-card'+(totalActive>0?' p-ending':'');

    document.getElementById('last-update').textContent=new Date().toLocaleTimeString();
  }catch(e){console.error(e)}
}
async function doDispatch(){
  const m=document.getElementById('dispatch-month').value.trim();
  if(!/^\d{6}$/.test(m)){document.getElementById('msg').innerHTML='<span style="color:var(--red)">Invalid YYYYMM</span>';return}
  const r=await fetch('/api/dispatch?month='+m,{method:'POST'});
  const d=await r.json();
  const el=document.getElementById('msg');
  if(d.status==='dispatched') el.innerHTML='<span style="color:var(--green)">✓ Dispatched '+m+'</span>';
  else if(d.status==='rejected') el.innerHTML='<span style="color:var(--yellow)">⚠ '+d.reason+'</span>';
  else el.innerHTML='<span style="color:var(--red)">'+JSON.stringify(d)+'</span>';
  setTimeout(fetchStatus,1500);
}
async function doReset(){
  if(!confirm('Reset dispatch state? This allows re-dispatch for the current month.'))return;
  await fetch('/api/reset',{method:'POST'});
  document.getElementById('msg').innerHTML='<span style="color:var(--green)">State reset</span>';
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
