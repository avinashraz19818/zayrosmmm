"""Authenticated Heroku web dashboard for the Telegram worker fleet.

The web dyno never opens Telegram sessions. It reads MongoDB and writes durable
fan-out tasks; account workers claim the child task for their shard. This keeps
web requests fast and prevents a second process from opening a Telegram session.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from html import escape

from bson import ObjectId
from flask import Flask, jsonify, redirect, render_template_string, request, session, url_for
from gridfs import GridFS
from pymongo import MongoClient, ReturnDocument


MONGO_URI = os.getenv("MONGO_URI", "").strip()
DB_NAME = os.getenv("MONGO_DB_NAME", "tg_manager_bot").strip() or "tg_manager_bot"
BUCKET = os.getenv("MONGO_GRIDFS_BUCKET", "tg_manager_storage").strip() or "tg_manager_storage"
APP_NAME = os.getenv("HEROKU_APP_NAME", "").strip()
SECRET = os.getenv("DASHBOARD_SESSION_SECRET", "").strip() or os.urandom(32)

app = Flask(__name__)
app.secret_key = SECRET

mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000) if MONGO_URI else None
db = mongo[DB_NAME] if mongo is not None else None


HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>VC Bot Dashboard</title>
<style>
:root{--bg:#0d111b;--panel:#151d2b;--panel2:#1b2638;--line:#29364a;--text:#edf3fb;--muted:#92a1b6;--blue:#2e9bff;--green:#27c878;--red:#ff5b5b;--yellow:#e9b34f}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px Inter,system-ui,-apple-system,Segoe UI,sans-serif}a{color:inherit;text-decoration:none}.top{height:58px;border-bottom:1px solid var(--line);display:flex;align-items:center;padding:0 28px;gap:24px}.brand{font-weight:800;font-size:17px;margin-right:auto}.brand b{color:#ffb52d}.nav{display:flex;gap:8px}.nav a{padding:10px 13px;color:var(--muted);border-radius:7px}.nav a.active,.nav a:hover{background:#202d43;color:#fff}.wrap{max-width:1120px;margin:34px auto;padding:0 20px}h1{font-size:25px;margin:0 0 24px}h2{font-size:17px;margin:28px 0 14px}.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}.card,.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px}.card{padding:18px;text-align:center}.card .num{font-size:25px;font-weight:800}.card .label{color:var(--muted);font-size:11px;text-transform:uppercase;margin-top:7px}.green{color:var(--green)}.red{color:var(--red)}.yellow{color:var(--yellow)}.blue{color:var(--blue)}.panel{padding:18px;overflow:auto}.workers{color:#c8d4e5;line-height:1.7}.workers strong{color:var(--green)}table{width:100%;border-collapse:collapse;white-space:nowrap}th,td{text-align:left;padding:12px 10px;border-bottom:1px solid var(--line)}th{color:var(--muted);font-size:11px;text-transform:uppercase}td{color:#dfe8f4}.pill{border-radius:20px;padding:4px 9px;background:#203148;color:#b7d5f3;font-size:12px}.pill.ok{background:#143b2b;color:#7de3ac}.pill.bad{background:#41232a;color:#ff9c9c}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.form{padding:16px;background:var(--panel);border:1px solid var(--line);border-radius:10px}.form h3{margin:0 0 12px}.form input,.form select{width:100%;padding:10px;margin:5px 0 9px;background:#202c3d;border:1px solid #34455c;border-radius:6px;color:#fff}.btn{border:0;background:var(--blue);color:#fff;border-radius:6px;padding:10px 14px;font-weight:700;cursor:pointer}.btn.danger{background:var(--red)}.btn.secondary{background:#2a394e}.result{margin-top:18px;padding:14px;border:1px solid var(--line);background:var(--panel);border-radius:8px;color:var(--muted)}.login{max-width:420px;margin:13vh auto;padding:26px}.login input{width:100%;padding:12px;margin:8px 0 15px;background:#202c3d;border:1px solid #34455c;border-radius:6px;color:white}.error{color:#ff8888;margin-bottom:10px}@media(max-width:800px){.cards{grid-template-columns:repeat(2,1fr)}.grid{grid-template-columns:1fr 1fr}.top{padding:0 12px}.nav a{padding:8px 5px;font-size:12px}.wrap{margin-top:22px}}@media(max-width:520px){.grid{grid-template-columns:1fr}.nav{gap:0}.nav a:nth-child(n+4){display:none}}
</style>
</head>
<body>
<div class="top"><div class="brand"><b>⚡</b> VC Bot</div><div class="nav"><a href="#" data-page="dashboard" class="active">Dashboard</a><a href="#" data-page="actions">Actions</a><a href="#" data-page="sessions">Sessions</a><a href="#" data-page="accounts">Accounts</a><a href="#" data-page="plans">Plans</a><a href="#" data-page="activity">Activity</a><a href="/logout">Logout</a></div></div>
<main class="wrap"><div id="app"></div></main>
<script>
const app=document.getElementById('app');let timer;
const esc=x=>String(x??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
async function get(u,o){let r=await fetch(u,o);if(r.status===401){location.href='/';return null}return r.json()}
function nav(page){document.querySelectorAll('[data-page]').forEach(a=>a.classList.toggle('active',a.dataset.page===page));clearInterval(timer);({dashboard:dashboard,actions:actions,sessions:sessions,accounts:accounts,plans:plans,activity:activity}[page]||dashboard)()}
document.querySelectorAll('[data-page]').forEach(a=>a.onclick=e=>{e.preventDefault();nav(a.dataset.page)});
async function dashboard(){let d=await get('/api/summary');if(!d)return;app.innerHTML=`<h1>Dashboard</h1><div class="cards"><div class="card"><div class="num">${d.total}</div><div class="label">Total accounts</div></div><div class="card"><div class="num green">${d.leased}</div><div class="label">Accounts online</div></div><div class="card"><div class="num yellow">${d.flood}</div><div class="label">Flood</div></div><div class="card"><div class="num red">${d.dead}</div><div class="label">Dead</div></div><div class="card"><div class="num red">${d.banned}</div><div class="label">Banned</div></div></div><div class="panel" style="margin-top:14px"><div class="workers"><strong>Workers: ${d.live_workers}/${d.workers} online</strong> &nbsp; ${d.slots.join(', ')||'none'}</div></div><h2>Voice chats</h2><div class="cards" style="grid-template-columns:repeat(4,1fr)"><div class="card"><div class="num">${d.voice.active}</div><div class="label">Active VCs</div></div><div class="card"><div class="num green">${d.voice.accounts}</div><div class="label">Accounts in use</div></div><div class="card"><div class="num">${d.voice.memberships}</div><div class="label">VC memberships</div></div><div class="card"><div class="num yellow">${d.voice.max_per_account}</div><div class="label">Max calls/account</div></div></div><h2>Recent tasks</h2><div class="panel"><table><thead><tr><th>When</th><th>Action</th><th>Progress</th><th>OK</th><th>Fail</th><th>Status</th></tr></thead><tbody>${d.tasks.map(t=>`<tr><td>${esc(t.when)}</td><td>${esc(t.action)}</td><td>${t.progress}%</td><td class="green">${t.ok}</td><td class="red">${t.fail}</td><td><span class="pill ${t.status==='done'?'ok':t.status==='failed'?'bad':''}">${esc(t.status)}</span></td></tr>`).join('')}</tbody></table></div>`;timer=setInterval(dashboard,10000)}
function actionForm(title,action,fields){return `<div class="form"><h3>${title}</h3>${fields.map(f=>`<input id="${action}-${f[0]}" placeholder="${f[1]}" ${f[2]||''}>`).join('')}<button class="btn" onclick="runAction('${action}')">${title}</button></div>`}
async function runAction(action){let p={};document.querySelectorAll(`[id^="${action}-"]`).forEach(x=>{p[x.id.slice(action.length+1)]=x.value});let d=await get('/api/actions',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,params:p})});document.getElementById('result').innerHTML=d?`Queued task <b>${esc(d.id)}</b>`:''}
async function uploadPhoto(){let f=document.getElementById('photo-file').files[0];if(!f){document.getElementById('result').innerText='Choose a photo first';return}let fd=new FormData();fd.append('photo',f);let d=await get('/api/profile-photo',{method:'POST',body:fd});document.getElementById('result').innerText=d?`Queued task ${d.id}`:''}
async function actions(){app.innerHTML=`<h1>Actions</h1><div class="grid">${actionForm('Join','join',[['target','https://t.me/group']])}${actionForm('Leave','leave',[['target','chat id or link']])}${actionForm('Set name','set_name',[['name','John Doe']])}${actionForm('Random names','random_names',[['names','Name One, Name Two, Name Three']])}${actionForm('React','react',[['target','post link'],['emoji','emoji (optional)']])}${actionForm('Views','views',[['target','post link']])}${actionForm('Live start','live_start',[['target','channel link']])}<div class="form"><h3>Profile photo</h3><input id="photo-file" type="file" accept="image/*"><button class="btn" onclick="uploadPhoto()">Set DP</button></div><div class="form"><h3>Live controls</h3><button class="btn danger" onclick="runAction('live_stop')">Stop all live calls</button><br><br><button class="btn secondary" onclick="runAction('live_rotate')">Rotate live accounts</button></div></div><div id="result" class="result">Choose an action.</div>`}
async function sessions(){let d=await get('/api/sessions');app.innerHTML=`<h1>Sessions</h1><div class="panel"><table><thead><tr><th>Account</th><th>Worker</th><th>Status</th><th>Last update</th></tr></thead><tbody>${d.rows.map(x=>`<tr><td>${esc(x.account)}</td><td>worker.${x.slot+1}</td><td><span class="pill ${x.online?'ok':''}">${x.online?'online':'offline'}</span></td><td>${esc(x.updated)}</td></tr>`).join('')}</tbody></table></div>`}
async function accounts(){return sessions()}
async function plans(){let d=await get('/api/plans');app.innerHTML=`<h1>Plans</h1><div class="panel"><table><thead><tr><th>Client</th><th>Channel</th><th>Accounts</th><th>React/post</th><th>Views/post</th><th>Live</th><th>Status</th><th>Expiry</th></tr></thead><tbody>${d.rows.map(x=>`<tr><td>${esc(x.name)}</td><td>${esc(x.channel)}</td><td>${x.accounts}</td><td>${x.reactions}</td><td>${x.views}</td><td>${x.live}</td><td>${esc(x.status)}</td><td>${esc(x.expiry)}</td></tr>`).join('')}</tbody></table></div>`}
async function activity(){let d=await get('/api/activity');app.innerHTML=`<h1>Activity</h1><div class="panel"><table><thead><tr><th>Time</th><th>Account</th><th>Action</th><th>Target</th><th>Status</th></tr></thead><tbody>${d.rows.map(x=>`<tr><td>${esc(x.time)}</td><td>${esc(x.account)}</td><td>${esc(x.action)}</td><td>${esc(x.target)}</td><td>${esc(x.status)}</td></tr>`).join('')}</tbody></table></div>`}
nav('dashboard');
</script></body></html>
"""

LOGIN_HTML = r"""
<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>VC Bot Login</title><style>body{background:#0d111b;color:#fff;font:15px system-ui}.login{max-width:420px;margin:15vh auto;padding:28px;background:#151d2b;border:1px solid #29364a;border-radius:12px}input{width:100%;padding:12px;box-sizing:border-box;margin:12px 0;background:#202c3d;border:1px solid #34455c;border-radius:6px;color:#fff}button{width:100%;padding:12px;background:#2e9bff;color:white;border:0;border-radius:6px;font-weight:bold}</style></head><body><div class="login"><h2>VC Bot Dashboard</h2><p>Use <code>/dashboard</code> in the owner Telegram bot to get a secure one-time login link.</p></div></body></html>
"""


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def auth_required():
    return bool(session.get("owner_id"))


@app.get("/health")
def health():
    try:
        db.command("ping")
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)[:100]}), 503


@app.get("/auth/<token>")
def auth(token):
    if db is None:
        return "MongoDB is not configured", 503
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    doc = db["dashboard_tokens"].find_one_and_update(
        {"_id": digest, "used": False, "expires_at": {"$gt": now()}},
        {"$set": {"used": True, "used_at": now()}},
        return_document=ReturnDocument.AFTER,
    )
    if not doc:
        return "This dashboard link is expired or already used.", 401
    session["owner_id"] = int(doc["owner_id"])
    return redirect("/")


@app.get("/logout")
def logout():
    session.clear()
    return redirect("/")


@app.get("/")
def index():
    return render_template_string(HTML if auth_required() else LOGIN_HTML)


def guard():
    if not auth_required():
        return jsonify({"error": "dashboard login required"}), 401
    return None


def worker_slots():
    docs = list(db["account_shards"].find({}, {"slot": 1}))
    return sorted(set(int(d.get("slot", 0) or 0) for d in docs)) or [0]


@app.get("/api/summary")
def api_summary():
    if (err := guard()): return err
    shards = db["account_shards"]
    total = shards.count_documents({})
    leased = shards.count_documents({"lease_until": {"$gt": now()}})
    docs = list(shards.find({}, {"slot": 1, "owner": 1, "lease_until": 1}))
    slots = sorted(set(int(d.get("slot", 0) or 0) for d in docs))
    owners = {d.get("owner") for d in docs if d.get("owner") and d.get("lease_until") and d["lease_until"] > now()}
    clients = db["clients"]
    live_rows = list(db["live_state"].find({}))
    voice_accounts = sum(len(row.get("keys", [])) for row in live_rows)
    voice_memberships = sum(int(row.get("target", 0) or 0) for row in live_rows)
    tasks = []
    for parent in db["dashboard_tasks"].find({"kind": "parent"}).sort("created_at", -1).limit(12):
        children = list(db["dashboard_tasks"].find({"parent_id": parent["_id"]}))
        done = sum(1 for c in children if c.get("status") in {"done", "failed", "partial"})
        ok = sum(int(c.get("result", {}).get("ok", 0) or 0) for c in children)
        fail = sum(int(c.get("result", {}).get("failed", 0) or 0) for c in children)
        tasks.append({"when": parent.get("created_at", now()).strftime("%d %b %H:%M"), "action": parent.get("action"), "progress": int(done / len(children) * 100) if children else 0, "ok": ok, "fail": fail, "status": "running" if done < len(children) else ("failed" if any(c.get("status") == "failed" for c in children) else "done")})
    return jsonify({"total": total, "leased": leased, "flood": 0, "dead": 0, "banned": 0, "workers": len(slots), "live_workers": len(owners), "slots": slots, "clients": clients.count_documents({"status":"active"}), "voice": {"active": len(live_rows), "accounts": voice_accounts, "memberships": voice_memberships, "max_per_account": 1}, "tasks": tasks})


@app.get("/api/sessions")
def api_sessions():
    if (err := guard()): return err
    rows=[]
    for d in db["account_shards"].find({}).sort("_id", 1):
        rows.append({"account": d.get("account_key") or d.get("_id"), "slot": int(d.get("slot",0) or 0), "online": bool(d.get("lease_until") and d["lease_until"] > now()), "updated": d.get("updated_at", now()).strftime("%d %b %H:%M") if hasattr(d.get("updated_at"), "strftime") else "-"})
    return jsonify({"rows": rows})


@app.get("/api/plans")
def api_plans():
    if (err := guard()): return err
    rows=[]
    for d in db["clients"].find({}).sort("created_at", -1):
        rows.append({"name": d.get("client_name","Unknown"), "channel": d.get("channel_link",""), "accounts": d.get("accounts_count",0), "reactions": d.get("reactions_per_post",0), "views": d.get("views_per_post",0), "live": d.get("livestream_accounts",0), "status": d.get("status",""), "expiry": d.get("expires_at", now()).strftime("%d %b %Y %H:%M") if hasattr(d.get("expires_at"),"strftime") else "-"})
    return jsonify({"rows": rows})


@app.get("/api/activity")
def api_activity():
    if (err := guard()): return err
    rows=[]
    for d in db["history"].find({}).sort("timestamp", -1).limit(100):
        rows.append({"time": d.get("timestamp", now()).strftime("%d %b %H:%M") if hasattr(d.get("timestamp"),"strftime") else "-", "account": d.get("phone",""), "action": d.get("action",""), "target": d.get("target",""), "status": d.get("status","")})
    return jsonify({"rows": rows})


@app.post("/api/actions")
def api_actions():
    if (err := guard()): return err
    body=request.get_json(silent=True) or {}
    action=str(body.get("action","")).strip()
    if action not in {"join","leave","react","views","set_name","random_names","live_start","live_stop","live_rotate"}:
        return jsonify({"error":"unsupported action"}),400
    params=body.get("params") or {}
    parent_id=ObjectId()
    slots=worker_slots()
    db["dashboard_tasks"].insert_one({"_id":parent_id,"kind":"parent","action":action,"params":params,"status":"queued","created_at":now()})
    children=[]
    for slot in slots:
        children.append({"_id":ObjectId(),"kind":"child","parent_id":parent_id,"action":action,"params":params,"worker_slot":slot,"status":"queued","created_at":now()})
    if children: db["dashboard_tasks"].insert_many(children)
    return jsonify({"id":str(parent_id),"workers":len(slots)})


@app.post("/api/profile-photo")
def api_profile_photo():
    if (err := guard()): return err
    upload = request.files.get("photo")
    if upload is None or not upload.filename:
        return jsonify({"error":"photo is required"}),400
    name = f"dashboard/uploads/{ObjectId()}.jpg"
    data = upload.read()
    fs = GridFS(db, collection=BUCKET)
    file_id = fs.put(data, filename=name, metadata={"kind":"dashboard_upload", "size":len(data)})
    db["storage_manifest"].replace_one(
        {"_id": name},
        {"_id": name, "gridfs_id": file_id, "kind":"dashboard_upload", "size":len(data), "updated_at":now()},
        upsert=True,
    )
    parent_id=ObjectId()
    slots=worker_slots()
    db["dashboard_tasks"].insert_one({"_id":parent_id,"kind":"parent","action":"profile_photo","params":{"file_name":name},"status":"queued","created_at":now()})
    db["dashboard_tasks"].insert_many([{"_id":ObjectId(),"kind":"child","parent_id":parent_id,"action":"profile_photo","params":{"file_name":name},"worker_slot":slot,"status":"queued","created_at":now()} for slot in slots])
    return jsonify({"id":str(parent_id),"workers":len(slots)})


if __name__ == "__main__":

    app.run(host="0.0.0.0", port=int(os.getenv("PORT","5000")))
