"""Authenticated Heroku web dashboard for the Telegram worker fleet.

The web dyno never opens Telegram sessions. It reads MongoDB and writes durable
fan-out tasks; account workers claim the child task for their shard. This keeps
web requests fast and prevents a second process from opening a Telegram session.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
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
SUMMARY_CACHE = {"at": 0.0, "data": None}


HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>VC Fleet Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #090d16;
  --bg-subtle: #0e1424;
  --panel: rgba(19, 27, 44, 0.75);
  --panel-solid: #131b2c;
  --panel-hover: #192338;
  --line: rgba(255, 255, 255, 0.08);
  --line-strong: rgba(255, 255, 255, 0.15);
  --text: #f1f5f9;
  --muted: #8e9fb5;
  --blue: #3b82f6;
  --blue-glow: rgba(59, 130, 246, 0.25);
  --green: #10b981;
  --green-glow: rgba(16, 185, 129, 0.2);
  --red: #ef4444;
  --red-glow: rgba(239, 68, 68, 0.2);
  --yellow: #f59e0b;
  --yellow-glow: rgba(245, 158, 11, 0.2);
  --radius: 14px;
}

* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg);
  background-image:
    radial-gradient(at 10% 10%, rgba(59, 130, 246, 0.08) 0px, transparent 50%),
    radial-gradient(at 90% 90%, rgba(16, 185, 129, 0.05) 0px, transparent 50%);
  color: var(--text);
  font-family: 'Plus Jakarta Sans', system-ui, -apple-system, sans-serif;
  font-size: 14px;
  line-height: 1.5;
  min-height: 100vh;
}

a { color: inherit; text-decoration: none; }

/* Top Header */
.top {
  position: sticky;
  top: 0;
  z-index: 100;
  backdrop-filter: blur(16px);
  background: rgba(9, 13, 22, 0.85);
  border-bottom: 1px solid var(--line);
  height: 64px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
}
.brand {
  display: flex;
  align-items: center;
  gap: 10px;
  font-weight: 800;
  font-size: 18px;
  letter-spacing: -0.5px;
}
.brand .logo-icon {
  width: 32px;
  height: 32px;
  background: linear-gradient(135deg, #ffb52d, #ff5b5b);
  border-radius: 8px;
  display: grid;
  place-items: center;
  font-size: 16px;
  box-shadow: 0 0 16px rgba(255, 181, 45, 0.3);
}

.nav {
  display: flex;
  gap: 6px;
  background: rgba(255, 255, 255, 0.03);
  padding: 4px;
  border-radius: 10px;
  border: 1px solid var(--line);
}
.nav a {
  padding: 7px 14px;
  color: var(--muted);
  font-weight: 600;
  border-radius: 7px;
  transition: all 0.2s ease;
  font-size: 13px;
}
.nav a:hover { color: #fff; background: rgba(255, 255, 255, 0.05); }
.nav a.active {
  background: var(--blue);
  color: #fff;
  box-shadow: 0 2px 10px var(--blue-glow);
}
.nav a.logout {
  color: var(--red);
}
.nav a.logout:hover {
  background: var(--red-glow);
}

/* Layout */
.wrap {
  max-width: 1200px;
  margin: 0 auto;
  padding: 28px 20px 60px;
}
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 24px;
}
h1 { font-size: 24px; font-weight: 800; letter-spacing: -0.5px; }
h2 { font-size: 16px; font-weight: 700; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin: 32px 0 14px; }

/* Cards & Stats */
.cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: 14px;
}
.card {
  background: var(--panel);
  border: 1px solid var(--line);
  backdrop-filter: blur(12px);
  border-radius: var(--radius);
  padding: 18px 20px;
  position: relative;
  overflow: hidden;
  transition: transform 0.2s, border-color 0.2s;
}
.card:hover {
  transform: translateY(-2px);
  border-color: var(--line-strong);
}
.card .label {
  color: var(--muted);
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 6px;
}
.card .num {
  font-size: 28px;
  font-weight: 800;
  letter-spacing: -1px;
}

.green { color: var(--green); }
.red { color: var(--red); }
.yellow { color: var(--yellow); }
.blue { color: var(--blue); }

/* Panels */
.panel {
  background: var(--panel);
  border: 1px solid var(--line);
  backdrop-filter: blur(12px);
  border-radius: var(--radius);
  padding: 20px;
  overflow-x: auto;
  -webkit-overflow-scrolling: touch;
}

.workers-badge {
  display: flex;
  align-items: center;
  gap: 10px;
  color: #cbd5e1;
  font-weight: 500;
}
.pulse-dot {
  width: 9px;
  height: 9px;
  background: var(--green);
  border-radius: 50%;
  box-shadow: 0 0 10px var(--green);
  animation: pulse 2s infinite;
}
@keyframes pulse {
  0% { transform: scale(0.95); opacity: 0.8; }
  50% { transform: scale(1.3); opacity: 1; }
  100% { transform: scale(0.95); opacity: 0.8; }
}

/* Tables */
table {
  width: 100%;
  border-collapse: collapse;
  white-space: nowrap;
  font-size: 13px;
}
th {
  text-align: left;
  padding: 12px 14px;
  color: var(--muted);
  font-weight: 700;
  text-transform: uppercase;
  font-size: 11px;
  letter-spacing: 0.6px;
  border-bottom: 1px solid var(--line);
}
td {
  padding: 14px;
  border-bottom: 1px solid var(--line);
  color: #e2e8f0;
}
tr:last-child td { border-bottom: none; }
tbody tr:hover { background: rgba(255, 255, 255, 0.02); }

/* Badges */
.pill {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  border-radius: 30px;
  padding: 4px 10px;
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  background: rgba(255, 255, 255, 0.06);
  color: var(--muted);
}
.pill.ok { background: var(--green-glow); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.3); }
.pill.bad { background: var(--red-glow); color: var(--red); border: 1px solid rgba(239, 68, 68, 0.3); }
.pill.running { background: var(--blue-glow); color: var(--blue); border: 1px solid rgba(59, 130, 246, 0.3); }

/* Forms & Grids */
.grid-forms {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 16px;
}
.form {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: var(--radius);
  padding: 20px;
  display: flex;
  flex-direction: column;
}
.form h3 {
  font-size: 15px;
  font-weight: 700;
  margin-bottom: 14px;
  color: #fff;
  display: flex;
  align-items: center;
  gap: 8px;
}
.form input, .form select {
  width: 100%;
  padding: 11px 14px;
  margin-bottom: 12px;
  background: rgba(10, 15, 26, 0.6);
  border: 1px solid var(--line);
  border-radius: 8px;
  color: #fff;
  font-family: inherit;
  font-size: 13px;
  outline: none;
  transition: border-color 0.2s, box-shadow 0.2s;
}
.form input:focus, .form select:focus {
  border-color: var(--blue);
  box-shadow: 0 0 0 3px var(--blue-glow);
}
.form input[type="file"] {
  padding: 8px;
  cursor: pointer;
}

/* Buttons */
.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  border: none;
  background: var(--blue);
  color: #fff;
  border-radius: 8px;
  padding: 10px 16px;
  font-weight: 700;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.2s ease;
  box-shadow: 0 2px 8px var(--blue-glow);
}
.btn:hover {
  filter: brightness(1.1);
  transform: translateY(-1px);
}
.btn:active { transform: translateY(0); }
.btn.danger { background: var(--red); box-shadow: 0 2px 8px var(--red-glow); }
.btn.secondary { background: rgba(255, 255, 255, 0.08); box-shadow: none; border: 1px solid var(--line); }
.btn.secondary:hover { background: rgba(255, 255, 255, 0.12); }
.btn.sm { padding: 6px 10px; font-size: 11px; border-radius: 6px; }

.result-box {
  margin-top: 20px;
  padding: 14px 18px;
  border: 1px solid var(--line);
  background: var(--panel);
  border-radius: 10px;
  color: var(--muted);
  font-size: 13px;
  display: flex;
  align-items: center;
  gap: 10px;
}
.result-box strong { color: var(--blue); }

/* Mobile Navbar & Responsive Adjustments */
@media (max-width: 860px) {
  .top {
    flex-direction: column;
    height: auto;
    padding: 12px 16px;
    gap: 12px;
  }
  .brand { margin-right: 0; }
  .nav {
    width: 100%;
    overflow-x: auto;
    justify-content: flex-start;
    -webkit-overflow-scrolling: touch;
  }
  .nav a { white-space: nowrap; font-size: 12px; padding: 6px 12px; }
  .wrap { padding: 18px 12px; }
  .cards { grid-template-columns: repeat(2, 1fr); }
  .grid-forms { grid-template-columns: 1fr; }
}

@media (max-width: 480px) {
  .cards { grid-template-columns: 1fr; }
}
</style>
</head>
<body>

<div class="top">
  <div class="brand">
    <div class="logo-icon">⚡</div>
    <span>VC Bot Fleet</span>
  </div>
  <div class="nav">
    <a href="#" data-page="dashboard" class="active">Dashboard</a>
    <a href="#" data-page="actions">Actions</a>
    <a href="#" data-page="accounts">Accounts</a>
    <a href="#" data-page="plans">Plans</a>
    <a href="#" data-page="activity">Activity</a>
    <a href="/logout" class="logout">Logout</a>
  </div>
</div>

<main class="wrap">
  <div id="app"></div>
</main>

<script>
const app = document.getElementById('app');
let timer = null;

const esc = x => String(x ?? '').replace(/[&<>"']/g, m => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[m]));

async function get(u, o) {
  try {
    let r = await fetch(u, o);
    if (r.status === 401) { location.href = '/'; return null; }
    return await r.json();
  } catch(e) {
    console.error(e);
    return null;
  }
}

function nav(page) {
  document.querySelectorAll('[data-page]').forEach(a => a.classList.toggle('active', a.dataset.page === page));
  clearInterval(timer);
  timer = null;
  const routes = { dashboard, actions, accounts, plans, activity };
  (routes[page] || dashboard)();
}

document.querySelectorAll('[data-page]').forEach(a => {
  a.onclick = e => {
    e.preventDefault();
    nav(a.dataset.page);
  };
});

/* --- DASHBOARD PAGE --- */
async function dashboard() {
  let d = await get('/api/summary');
  if (!d) return;

  app.innerHTML = `
    <div class="page-header">
      <h1>System Overview</h1>
      <span class="pill ${d.live_workers > 0 ? 'ok' : 'bad'}">${d.live_workers}/${d.workers} Workers Live</span>
    </div>

    <div class="cards">
      <div class="card">
        <div class="label">Total Accounts</div>
        <div class="num">${d.total}</div>
      </div>
      <div class="card">
        <div class="label">Online / Leased</div>
        <div class="num green">${d.leased}</div>
      </div>
      <div class="card">
        <div class="label">Flood Wait</div>
        <div class="num yellow">${d.flood}</div>
      </div>
      <div class="card">
        <div class="label">Dead / Banned</div>
        <div class="num red">${d.dead + d.banned}</div>
      </div>
    </div>

    <div class="panel" style="margin-top: 16px;">
      <div class="workers-badge">
        <div class="pulse-dot"></div>
        <div><strong>Worker Shards:</strong> ${d.slots.map(s => `worker.${s+1}`).join(', ') || 'None connected'}</div>
      </div>
    </div>

    <h2>Voice Chat Status</h2>
    <div class="cards">
      <div class="card">
        <div class="label">Active Calls</div>
        <div class="num blue">${d.voice.active}</div>
      </div>
      <div class="card">
        <div class="label">Accounts in VC</div>
        <div class="num green">${d.voice.accounts}</div>
      </div>
      <div class="card">
        <div class="label">Total Memberships</div>
        <div class="num">${d.voice.memberships}</div>
      </div>
      <div class="card">
        <div class="label">Limit / Account</div>
        <div class="num yellow">${d.voice.max_per_account}</div>
      </div>
    </div>

    <h2>Recent Fleet Tasks</h2>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Action</th>
            <th>Target</th>
            <th>Progress</th>
            <th>Success</th>
            <th>Failed</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          ${d.tasks.length ? d.tasks.map(t => `
            <tr>
              <td>${esc(t.when)}</td>
              <td><strong>${esc(t.action)}</strong></td>
              <td>${esc(t.target || '-')}</td>
              <td>${t.progress}%</td>
              <td class="green">${t.ok}</td>
              <td class="red">${t.fail}</td>
              <td><span class="pill ${t.status === 'done' ? 'ok' : t.status === 'failed' ? 'bad' : 'running'}">${esc(t.status)}</span></td>
            </tr>
          `).join('') : '<tr><td colspan="7" style="text-align:center;color:var(--muted)">No recent tasks</td></tr>'}
        </tbody>
      </table>
    </div>
  `;
  if (!timer) timer = setInterval(dashboard, 10000);
}

/* --- ACTIONS PAGE --- */
function actionForm(title, action, fields, btnText="Execute") {
  return `
    <div class="form">
      <h3>${title}</h3>
      ${fields.map(f => `<input id="${action}-${f[0]}" placeholder="${f[1]}" ${f[2] || ''}>`).join('')}
      <button class="btn" style="margin-top:auto" onclick="runAction('${action}')">${btnText}</button>
    </div>
  `;
}

async function runAction(action) {
  let p = {};
  document.querySelectorAll(`[id^="${action}-"]`).forEach(x => {
    p[x.id.slice(action.length + 1)] = x.value;
  });
  let d = await get('/api/actions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action, params: p })
  });
  const res = document.getElementById('result');
  if (res) res.innerHTML = d?.id ? `Task queued successfully! Task ID: <strong>${esc(d.id)}</strong> (${d.workers} worker shard claimed)` : `<span class="red">Error executing task</span>`;
}

async function uploadPhoto() {
  let f = document.getElementById('photo-file').files[0];
  if (!f) { alert('Please select a photo first'); return; }
  let fd = new FormData();
  fd.append('photo', f);
  let d = await get('/api/profile-photo', { method: 'POST', body: fd });
  const res = document.getElementById('result');
  if (res) res.innerHTML = d?.id ? `Photo task queued! Task ID: <strong>${esc(d.id)}</strong>` : `<span class="red">Upload failed</span>`;
}

async function actions() {
  app.innerHTML = `
    <div class="page-header">
      <h1>Fleet Actions</h1>
    </div>
    <div class="grid-forms">
      ${actionForm('Join Channel/Group', 'join', [['target', 'https://t.me/group or @username']], 'Join Target')}
      ${actionForm('Leave Chat', 'leave', [['target', 'Target Link or Chat ID']], 'Leave Target')}
      ${actionForm('Set Profile Name', 'set_name', [['name', 'e.g. Rahul Sharma']], 'Update Name')}
      ${actionForm('Random Names', 'random_names', [['names', 'Blank = Indian 500 names pool']], 'Assign Random')}
      ${actionForm('Post Reactions', 'react', [['target', 'Post link (t.me/c/...)'], ['emoji', 'Emoji (optional)']])}
      ${actionForm('Post Views', 'views', [['target', 'Post link (t.me/c/...)']])}
      ${actionForm('Start Live VC', 'live_start', [['target', 'Channel / Group link']], 'Start Live')}

      <div class="form">
        <h3>Update Profile Photo</h3>
        <input id="photo-file" type="file" accept="image/*">
        <button class="btn" style="margin-top:auto" onclick="uploadPhoto()">Upload & Set DP</button>
      </div>

      <div class="form">
        <h3>Live Voice Controls</h3>
        <div style="display:flex; flex-direction:column; gap:10px; margin-top:auto;">
          <button class="btn secondary" onclick="runAction('live_rotate')">🔄 Rotate Live Accounts</button>
          <button class="btn danger" onclick="runAction('live_stop')">🛑 Stop All Live Calls</button>
        </div>
      </div>
    </div>
    <div id="result" class="result-box">Select or configure an action above to trigger worker tasks.</div>
  `;
}

/* --- ACCOUNTS & SESSIONS (CONSOLIDATED) --- */
async function uploadZip() {
  let f = document.getElementById('zip-file').files[0];
  if (!f) { alert('Choose a .ZIP file first'); return; }
  let fd = new FormData();
  fd.append('zip', f);
  let resText = document.getElementById('zip-result');
  resText.innerText = 'Uploading...';
  let d = await get('/api/session-import', { method: 'POST', body: fd });
  resText.innerText = d?.error ? `❌ ${d.error}` : `✅ Queued Import: Task ${d?.id || ''}`;
  setTimeout(accounts, 1500);
}

async function accounts() {
  let d = await get('/api/sessions');
  if (!d) return;

  app.innerHTML = `
    <div class="page-header">
      <h1>Accounts & Sessions</h1>
      <span class="pill">${d.rows.length} Total Loaded</span>
    </div>

    <div class="form" style="margin-bottom: 20px;">
      <h3>Bulk Session Import</h3>
      <div style="display:flex; gap:12px; flex-wrap:wrap; align-items:center;">
        <input id="zip-file" type="file" accept=".zip" style="margin-bottom:0; max-width:320px;">
        <button class="btn" onclick="uploadZip()">Upload ZIP Archive</button>
        <span id="zip-result" style="color:var(--muted); font-size:13px;"></span>
      </div>
    </div>

    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>Account Identifier</th>
            <th>Assigned Shard</th>
            <th>Status</th>
            <th>Last Seen / Leased</th>
          </tr>
        </thead>
        <tbody>
          ${d.rows.length ? d.rows.map(x => `
            <tr>
              <td><strong>${esc(x.account)}</strong></td>
              <td><span class="pill">worker.${x.slot + 1}</span></td>
              <td><span class="pill ${x.online ? 'ok' : ''}">${x.online ? 'Online' : 'Offline'}</span></td>
              <td>${esc(x.updated)}</td>
            </tr>
          `).join('') : '<tr><td colspan="4" style="text-align:center;color:var(--muted)">No accounts found in storage</td></tr>'}
        </tbody>
      </table>
    </div>
  `;
}

/* --- CLIENT PLANS --- */
async function createClient() {
  let p = {
    client_user_id: Number(document.getElementById('client-user').value),
    client_name: document.getElementById('client-name').value,
    channel_link: document.getElementById('client-channel').value,
    accounts_count: Number(document.getElementById('client-accounts').value || 1),
    reactions_per_post: Number(document.getElementById('client-react').value || 0),
    views_per_post: Number(document.getElementById('client-views').value || 0),
    livestream_accounts: Number(document.getElementById('client-live').value || 0),
    subscription_days: Number(document.getElementById('client-days').value || 30)
  };
  let resText = document.getElementById('client-result');
  resText.innerText = 'Creating client...';
  let d = await get('/api/clients', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(p)
  });
  resText.innerText = d?.error ? `❌ ${d.error}` : `✅ Client created (Task ${d?.task_id || ''})`;
  if (d && !d.error) setTimeout(plans, 1200);
}

async function clientPatch(id, data) {
  await get('/api/clients/' + id, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data)
  });
  plans();
}

async function deleteClient(id) {
  if (confirm('Are you sure you want to delete this subscription? Accounts will leave the target.')) {
    await get('/api/clients/' + id, { method: 'DELETE' });
    plans();
  }
}

async function plans() {
  let d = await get('/api/plans');
  if (!d) return;

  app.innerHTML = `
    <div class="page-header">
      <h1>Subscription Plans</h1>
    </div>

    <div class="form" style="margin-bottom: 24px;">
      <h3>Add New Client Subscription</h3>
      <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(180px, 1fr)); gap:12px;">
        <input id="client-user" placeholder="Client Telegram ID">
        <input id="client-name" placeholder="Client Name">
        <input id="client-channel" placeholder="Target Channel Link">
        <input id="client-accounts" type="number" placeholder="Accounts Count (1-10)">
        <input id="client-react" type="number" placeholder="Reactions / Post">
        <input id="client-views" type="number" placeholder="Views / Post">
        <input id="client-live" type="number" placeholder="Live VC Accounts">
        <input id="client-days" type="number" placeholder="Validity Days (30)">
      </div>
      <div style="display:flex; align-items:center; gap:14px; margin-top:14px;">
        <button class="btn" onclick="createClient()">Create Subscription</button>
        <span id="client-result" style="font-size:13px; color:var(--muted)"></span>
      </div>
    </div>

    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>Client Name</th>
            <th>Channel Link</th>
            <th>Accs</th>
            <th>Reacts</th>
            <th>Views</th>
            <th>Live</th>
            <th>Status</th>
            <th>Expires On</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          ${d.rows.length ? d.rows.map(x => `
            <tr>
              <td><strong>${esc(x.name)}</strong></td>
              <td>${esc(x.channel)}</td>
              <td>${x.accounts}</td>
              <td>${x.reactions}</td>
              <td>${x.views}</td>
              <td>${x.live}</td>
              <td><span class="pill ${x.status === 'active' ? 'ok' : 'bad'}">${esc(x.status)}</span></td>
              <td>${esc(x.expiry)}</td>
              <td>
                <div style="display:flex; gap:6px;">
                  <button class="btn secondary sm" onclick="clientPatch('${x.id}', {status:'${x.status === 'active' ? 'stopped' : 'active'}'})">
                    ${x.status === 'active' ? 'Pause' : 'Resume'}
                  </button>
                  <button class="btn secondary sm" onclick="clientPatch('${x.id}', {extend_days:30})">+30d</button>
                  <button class="btn danger sm" onclick="deleteClient('${x.id}')">Del</button>
                </div>
              </td>
            </tr>
          `).join('') : '<tr><td colspan="9" style="text-align:center;color:var(--muted)">No active subscriptions found</td></tr>'}
        </tbody>
      </table>
    </div>
  `;
}

/* --- ACTIVITY LOGS --- */
async function activity() {
  let d = await get('/api/activity');
  if (!d) return;

  app.innerHTML = `
    <div class="page-header">
      <h1>System Activity Logs</h1>
      <span class="pill">Last 100 entries</span>
    </div>
    <div class="panel">
      <table>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Account / Worker</th>
            <th>Action Performed</th>
            <th>Target</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          ${d.rows.length ? d.rows.map(x => `
            <tr>
              <td>${esc(x.time)}</td>
              <td><strong>${esc(x.account || 'System')}</strong></td>
              <td>${esc(x.action)}</td>
              <td>${esc(x.target || '-')}</td>
              <td><span class="pill ${x.status === 'success' || x.status === 'ok' ? 'ok' : x.status === 'failed' ? 'bad' : ''}">${esc(x.status)}</span></td>
            </tr>
          `).join('') : '<tr><td colspan="5" style="text-align:center;color:var(--muted)">No activity logged yet</td></tr>'}
        </tbody>
      </table>
    </div>
  `;
}

// Initial View
nav('dashboard');
</script>
</body>
</html>
"""

LOGIN_HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>VC Bot Authentication</title>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #090d16;
  background-image: radial-gradient(circle at 50% 30%, rgba(59, 130, 246, 0.15), transparent 60%);
  color: #f1f5f9;
  font-family: 'Plus Jakarta Sans', system-ui, sans-serif;
  min-height: 100vh;
  display: grid;
  place-items: center;
  padding: 20px;
}
.login-card {
  width: 100%;
  max-width: 400px;
  background: rgba(19, 27, 44, 0.8);
  backdrop-filter: blur(16px);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 16px;
  padding: 32px 28px;
  text-align: center;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.5);
}
.logo-icon {
  width: 48px;
  height: 48px;
  background: linear-gradient(135deg, #ffb52d, #ff5b5b);
  border-radius: 12px;
  display: grid;
  place-items: center;
  font-size: 24px;
  margin: 0 auto 18px;
  box-shadow: 0 0 20px rgba(255, 181, 45, 0.4);
}
h2 { font-size: 22px; font-weight: 800; margin-bottom: 8px; }
p { color: #8e9fb5; font-size: 13px; line-height: 1.6; margin-bottom: 20px; }
code {
  background: rgba(59, 130, 246, 0.15);
  color: #60a5fa;
  padding: 3px 8px;
  border-radius: 6px;
  font-size: 13px;
  border: 1px solid rgba(59, 130, 246, 0.2);
}
</style>
</head>
<body>
<div class="login-card">
  <div class="logo-icon">⚡</div>
  <h2>VC Bot Fleet</h2>
  <p>Dashboard access is restricted. Please use <code>/dashboard</code> command inside your Telegram Bot to generate a one-time secure session link.</p>
</div>
</body>
</html>
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
    if SUMMARY_CACHE["data"] is not None and time.monotonic() - SUMMARY_CACHE["at"] < 3:
        return jsonify(SUMMARY_CACHE["data"])
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
        tasks.append({
            "when": parent.get("created_at", now()).strftime("%d %b %H:%M"),
            "action": parent.get("action"),
            "target": (parent.get("params") or {}).get("target", ""),
            "progress": int(done / len(children) * 100) if children else 0,
            "ok": ok,
            "fail": fail,
            "status": "running" if done < len(children) else ("failed" if any(c.get("status") == "failed" for c in children) else "done")
        })
    data = {
        "total": total,
        "leased": leased,
        "flood": 0,
        "dead": 0,
        "banned": 0,
        "workers": len(slots),
        "live_workers": len(owners),
        "slots": slots,
        "clients": clients.count_documents({"status": "active"}),
        "voice": {
            "active": len(live_rows),
            "accounts": voice_accounts,
            "memberships": voice_memberships,
            "max_per_account": 1
        },
        "tasks": tasks
    }
    SUMMARY_CACHE.update({"at": time.monotonic(), "data": data})
    return jsonify(data)


@app.get("/api/sessions")
def api_sessions():
    if (err := guard()): return err
    rows = []
    for d in db["account_shards"].find({}).sort("_id", 1):
        rows.append({
            "account": d.get("account_key") or d.get("_id"),
            "slot": int(d.get("slot", 0) or 0),
            "online": bool(d.get("lease_until") and d["lease_until"] > now()),
            "updated": d.get("updated_at", now()).strftime("%d %b %H:%M") if hasattr(d.get("updated_at"), "strftime") else "-"
        })
    return jsonify({"rows": rows})


@app.get("/api/plans")
def api_plans():
    if (err := guard()): return err
    rows = []
    for d in db["clients"].find({}).sort("created_at", -1):
        rows.append({
            "id": str(d.get("_id")),
            "name": d.get("client_name", "Unknown"),
            "channel": d.get("channel_link", ""),
            "accounts": d.get("accounts_count", 0),
            "reactions": d.get("reactions_per_post", 0),
            "views": d.get("views_per_post", 0),
            "live": d.get("livestream_accounts", 0),
            "status": d.get("status", ""),
            "expiry": d.get("expires_at", now()).strftime("%d %b %Y %H:%M") if hasattr(d.get("expires_at"), "strftime") else "-"
        })
    return jsonify({"rows": rows})


@app.get("/api/activity")
def api_activity():
    if (err := guard()): return err
    rows = []
    for d in db["history"].find({}).sort("timestamp", -1).limit(100):
        rows.append({
            "time": d.get("timestamp", now()).strftime("%d %b %H:%M") if hasattr(d.get("timestamp"), "strftime") else "-",
            "account": d.get("phone", ""),
            "action": d.get("action", ""),
            "target": d.get("target", ""),
            "status": d.get("status", "")
        })
    return jsonify({"rows": rows})


def queue_fanout(action, params):
    parent_id = ObjectId()
    slots = worker_slots()
    db["dashboard_tasks"].insert_one({
        "_id": parent_id, "kind": "parent", "action": action,
        "params": params, "status": "queued", "created_at": now(),
    })
    children = [{
        "_id": ObjectId(), "kind": "child", "parent_id": parent_id,
        "action": action, "params": params, "worker_slot": slot,
        "status": "queued", "created_at": now(),
    } for slot in slots]
    if children:
        db["dashboard_tasks"].insert_many(children)
    return parent_id, len(slots)


@app.post("/api/actions")
def api_actions():
    if (err := guard()): return err
    body = request.get_json(silent=True) or {}
    action = str(body.get("action", "")).strip()
    if action not in {"join", "leave", "react", "views", "set_name", "random_names", "live_start", "live_stop", "live_rotate"}:
        return jsonify({"error": "unsupported action"}), 400
    params = body.get("params") or {}
    parent_id, slots = queue_fanout(action, params)
    return jsonify({"id": str(parent_id), "workers": slots})


@app.post("/api/clients")
def api_create_client():
    if (err := guard()): return err
    body = request.get_json(silent=True) or {}
    try:
        user_id = int(body.get("client_user_id"))
        accounts = max(1, int(body.get("accounts_count", 1)))
        reactions = max(0, int(body.get("reactions_per_post", 0)))
        views = max(0, int(body.get("views_per_post", 0)))
        live = max(0, int(body.get("livestream_accounts", 0)))
        days = max(1, int(body.get("subscription_days", 30)))
    except (TypeError, ValueError):
        return jsonify({"error": "numeric package fields are invalid"}), 400
    link = str(body.get("channel_link", "")).strip()
    if not link or (reactions == 0 and views == 0):
        return jsonify({"error": "channel_link and reactions/views are required"}), 400
    private = bool(re.search(r"t\.me/(?:\+|joinchat/)", link))
    target_match = re.search(r"t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)", link)
    public_match = re.search(r"(?:t\.me/|@)([A-Za-z0-9_]{4,32})", link)
    target = (target_match.group(1) if private else public_match.group(1)
              if public_match else None)
    if not target:
        return jsonify({"error": "invalid channel link"}), 400
    doc = {
        "client_user_id": user_id, "client_name": str(body.get("client_name") or "Unknown"),
        "channel_link": link, "channel_id": None,
        "channel_username": None if private else target,
        "channel_type": "private" if private else "public",
        "accounts_count": accounts, "reactions_per_post": reactions,
        "views_per_post": views, "livestream_accounts": live,
        "subscription_days": days, "created_at": now(),
        "expires_at": now() + __import__("datetime").timedelta(days=days),
        "status": "active", "joined_accounts": [],
        "last_reminder_sent": None, "total_posts_processed": 0,
        "updated_at": now(),
    }
    inserted = db["clients"].insert_one(doc)
    parent_id, slots = queue_fanout("join", {"target": link, "client_id": str(inserted.inserted_id)})
    return jsonify({"id": str(inserted.inserted_id), "task_id": str(parent_id), "workers": slots})


@app.patch("/api/clients/<client_id>")
def api_update_client(client_id):
    if (err := guard()): return err
    body = request.get_json(silent=True) or {}
    try:
        oid = ObjectId(client_id)
    except Exception:
        return jsonify({"error": "invalid client id"}), 400
    update = {k: body[k] for k in ("status", "reactions_per_post", "views_per_post", "livestream_accounts") if k in body}
    if "extend_days" in body:
        try:
            update["expires_at"] = now() + __import__("datetime").timedelta(days=int(body["extend_days"]))
        except (TypeError, ValueError):
            return jsonify({"error": "extend_days is invalid"}), 400
    update["updated_at"] = now()
    result = db["clients"].update_one({"_id": oid}, {"$set": update})
    return jsonify({"updated": result.modified_count})


@app.delete("/api/clients/<client_id>")
def api_delete_client(client_id):
    if (err := guard()): return err
    try:
        oid = ObjectId(client_id)
    except Exception:
        return jsonify({"error": "invalid client id"}), 400
    doc = db["clients"].find_one({"_id": oid})
    if not doc:
        return jsonify({"error": "client not found"}), 404
    parent_id, slots = queue_fanout("leave", {"target": doc.get("channel_link", "")})
    db["clients"].delete_one({"_id": oid})
    return jsonify({"deleted": True, "task_id": str(parent_id), "workers": slots})


@app.post("/api/session-import")
def api_session_import():
    if (err := guard()): return err
    upload = request.files.get("zip")
    if upload is None or not upload.filename:
        return jsonify({"error": "ZIP file is required"}), 400
    name = f"dashboard/uploads/{ObjectId()}.zip"
    data = upload.read()
    fs = GridFS(db, collection=BUCKET)
    file_id = fs.put(data, filename=name, metadata={"kind": "dashboard_upload", "size": len(data)})
    db["storage_manifest"].replace_one(
        {"_id": name},
        {"_id": name, "gridfs_id": file_id, "kind": "dashboard_upload", "size": len(data), "updated_at": now()},
        upsert=True,
    )
    parent_id = ObjectId()
    db["dashboard_tasks"].insert_one({
        "_id": parent_id, "kind": "parent", "action": "session_import",
        "params": {"file_name": name}, "status": "queued", "created_at": now()
    })
    db["dashboard_tasks"].insert_one({
        "_id": ObjectId(), "kind": "child", "parent_id": parent_id,
        "action": "session_import", "params": {"file_name": name},
        "worker_slot": 0, "status": "queued", "created_at": now()
    })
    return jsonify({"id": str(parent_id), "workers": 1})


@app.post("/api/profile-photo")
def api_profile_photo():
    if (err := guard()): return err
    upload = request.files.get("photo")
    if upload is None or not upload.filename:
        return jsonify({"error": "photo is required"}), 400
    name = f"dashboard/uploads/{ObjectId()}.jpg"
    data = upload.read()
    fs = GridFS(db, collection=BUCKET)
    file_id = fs.put(data, filename=name, metadata={"kind": "dashboard_upload", "size": len(data)})
    db["storage_manifest"].replace_one(
        {"_id": name},
        {"_id": name, "gridfs_id": file_id, "kind": "dashboard_upload", "size": len(data), "updated_at": now()},
        upsert=True,
    )
    parent_id = ObjectId()
    slots = worker_slots()
    db["dashboard_tasks"].insert_one({
        "_id": parent_id, "kind": "parent", "action": "profile_photo",
        "params": {"file_name": name}, "status": "queued", "created_at": now()
    })
    db["dashboard_tasks"].insert_many([{
        "_id": ObjectId(), "kind": "child", "parent_id": parent_id,
        "action": "profile_photo", "params": {"file_name": name},
        "worker_slot": slot, "status": "queued", "created_at": now()
    } for slot in slots])
    return jsonify({"id": str(parent_id), "workers": len(slots)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
