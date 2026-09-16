# Heroku par naya code + restart — step by step (bilkul shuru se)

Ye sab **browser me** ho jaata hai, koi command nahi chahiye.
(Commands wala tarika neeche "Tarika B" me hai.)

## Pehle 30 second me samajh lo

- Bot Heroku par **worker dyno** par chalta hai (`Procfile` me `worker: python view.py`).
- Heroku par code pahunchta hai **Deploy** tab se (GitHub branch se), aur
  **restart** hota hai **More → Restart all dynos** se.
- Fix waala code abhi `arena/01a0a8f2-zayrosmmm` branch par hai. Usse `main`
  me laane ke liye GitHub par ek PR khula hai (PR #2) — merge karna 1 click hai.

---

## Tarika A — GitHub + Heroku Dashboard (recommended)

### Step 1 — GitHub par PR merge karo
1. Ye link kholo: https://github.com/avinashraz19818/zayrosmmm/pull/2
2. **"Merge pull request"** dabao → phir **"Confirm merge"**.
3. Ab `main` branch par fixed code aa gaya. (Agar "Merge" button dikhe hi nahi
   to "Conversation" tab me neeche scroll karo ya mujhe batao.)

### Step 2 — Heroku Dashboard kholo
1. https://dashboard.heroku.com/apps kholo.
2. Apne bot waale app par click karo (jo app aap chalate ho).

### Step 3 — Deploy tab → naya code bhejo
1. Upar **Deploy** tab kholo.
2. "Deployment method" me **GitHub** juda hua hai? (Connect hoga to repo ka naam
   dikhega: `avinashraz19818/zayrosmmm`)
   - Haan → "App connected to GitHub" ke neeche **branch `main`** chuno →
     **Deploy Branch** dabao → 2-5 min me **"Build succeeded"** aayega.
   - "Enable Automatic Deploys" ON hai → Step 1 ke baad deploy khud shuru ho
     jayega; **Activity** tab me "Deploy" dikhega.
   - GitHub juda hi nahi hai → **Tarika B** (neeche) use karo.
3. Build me `Procfile`, `worker: python view.py` dikhna chahiye.
   Agar build **fail** ho jaye → build log copy karke mujhe bhej do.

### Step 4 — Resources tab → worker dyno ON karo (ye zaroori hai)
1. **Resources** tab kholo.
2. *Dynos* section me:
   - **worker** → toggle **ON** → "Confirm".
   - purana **web** dyno chalu dikhe to usko **OFF** kar do (bot web dyno par
     nahi chal sakta — web dyno 60 second me `$PORT` maangta hai).
3. Aapka plan Eco/Basic ho to bhi 1 worker dyno chalta hai.

### Step 5 — Restart
1. Upar daaye **More** menu kholo → **Restart all dynos** → "Restart all dynos".

### Step 6 — Check karo ki chalu hua
1. **More → View logs** (ya `heroku logs --tail -a APP_NAME`).
2. Log me ye dikhna chahiye:
   - `Accounts loaded` / `status  : ready` lines
   - koi **Traceback** nahi
3. Telegram me apne bot ko `/start` bhejo → **Accounts** → **Scan Accounts**
   dabao. Jo account dead/frozen hai, wo list me aayega, aur
   **Trash Dead** / **Trash Frozen** se file trash me jaayegi
   (file delete nahi hoti).

---

## Tarika B — Heroku CLI (agar app GitHub se juda nahi hai)

Apne computer par (ya Heroku ke "Run console" se):

```bash
# ek hi baar: CLI install + login
heroku login

# apne app ka naam daalo (dashboard me URL me dikhta hai: /apps/<APP_NAME>)
heroku git:remote -a APP_NAME

# naya code deploy + restart
git push heroku HEAD:main

# worker dyno on karo (band ho to)
heroku ps:scale worker=1 --app APP_NAME

# sirf restart / logs
heroku ps:restart --app APP_NAME
heroku logs --tail --app APP_NAME
```

Deploy ke baad github/CLI dono me **worker dyno ON** hona chahiye.

---

## Kuch galat lage to mujhe ye bhejo

- Heroku logs: dashboard → **More → View logs** (ya `heroku logs -n 200 -a APP_NAME`)
- Telegram me bot kuch reply na kare → logs me `Traceback` waali lines
- Build fail → Deploy tab / Activity tab ka error text

Mujhe bas app ka naam + log ka text bhej do, main aage dekh dunga.

---

## Yaad rakhne wali 2 baatein

1. **Heroku ka disk ephemeral hai.** Restart / redeploy / ~24 ghante ke dyno
   cycle par `sessions/` folder khaali ho jaata hai, aur `sessions_trash/` bhi.
   Aapne bola sessions aap manage karte ho — theek hai. Lekin agar kabhi
   restart ke baad accounts gayab lagein, to bolo: main session files ka
   **MongoDB me backup + boot par auto-restore** add kar dunga, phir restart
   safe ho jayega.
2. **Trash Heroku par permanent ban sakta hai.** `sessions_trash/` bhi dyno ke
   andar hi hota hai, isliye dyno cycle hone par trash bhi ud jaata hai.
   Zaroori session file ko trash karne se pehle soch lo.

## Ye 3 files Heroku ke liye repo me add ki gayi hain

| File | Kaam |
|---|---|
| `Procfile` | `worker: python view.py` — isi se Heroku bot start karta hai |
| `.python-version` | `3.11` — Heroku ka default Python ab 3.14 hai, aur `tgcrypto` ke wheels sirf 3.11 tak hain (warna build fail) |
| `Aptfile` | `ffmpeg` — live audio ke liye; iske liye buildpack `heroku-buildpack-apt` chahiye (optional) |
