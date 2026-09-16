# Heroku par chalane ke liye (short guide)

Repo me jo files Heroku ke liye chahiye thi, wo missing thi:

| File | Kaam |
|---|---|
| `Procfile` | `worker: python view.py` — bot ek worker dyno par chalta hai (web dyno nahi: usme `$PORT` bind karna padta hai, jo polling bot nahi karta) |
| `.python-version` | `3.11` — Heroku ka default Python ab 3.14 hai, aur `tgcrypto` ke wheels sirf 3.11 tak hain, isliye 3.11 pin kiya |
| `Aptfile` | live audio ke liye `ffmpeg` (iske liye `heroku-buildpack-apt` buildpack add karna hoga) |

## Deploy / restart

Sabse pehle naya code Heroku par pahuchana zaroori hai, tabhi restart ka matlab hai.

**Agar Heroku app GitHub se juda hai** (Dashboard → Deploy → GitHub → automatic deploys):
1. Branch `arena/01a0a8f2-zayrosmmm` se `main` me PR merge karo → Heroku khud redeploy (aur restart) karega.
2. Ya Dashboard → Deploy → GitHub → "Deploy Branch" (branch chuno).

**Agar Heroku CLI use karte ho:**
```bash
heroku git:remote -a <APP_NAME>          # ek hi baar
git push heroku HEAD:main                # naya code deploy + restart
```

**Sirf restart (naya code push kiye bina):**
```bash
heroku ps:restart -a <APP_NAME>          # saare dynos restart
heroku ps -a <APP_NAME>                  # dekhne ke liye ki worker chalu hai
heroku logs --tail -a <APP_NAME>         # live log
```
Dashboard se: app kholo → **More → Restart all dynos**.
Ya dyno band dikhe to: **Resources → worker → dyno on karo** (`heroku ps:scale worker=1 -a <APP_NAME>`).

## ⚠️ Heroku ka disk ephemeral hai

Har restart / deploy / dyno cycle par `sessions/` khaali ho jaata hai. Heroku khud bhi har ~24 ghante me dyno cycle karta hai. Matlab:

- session files (aapke accounts) restart par udd jaate hain — dobara ZIP/string se import karna padta hai;
- `sessions_trash/` bhi ephemeral hai, isliye Heroku par "trash" ka matlab practically **permanent delete** hai.

Iska pakka solution: session files ko MongoDB (jo bot pehle se use karta hai) me backup karke boot par wapas restore karna. Wo feature add karna ho to bolo.
