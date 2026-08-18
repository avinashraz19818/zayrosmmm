# Telegram Views & Reactions Bot

Telegram SMM bot — views/reactions booster, multi-account session management, live audio streaming.
Accounts `sessions/` folder me rehte hain; MongoDB me sirf subscriptions, approved users aur activity log.
VPS pe pehle se chalta hai (`./start`), ab **Heroku-ready** bhi hai.

---

## 🚀 Heroku pe deploy kaise karein

> ⚠️ **Sabse pehle:** Heroku ka **free tier 2022 se band hai**. Deploy karne ke liye koi bhi paid plan
> chahiye (sabse sasta **Eco ≈ $5/month**). Account verify hona chahiye (credit card).
>
> ⚠️ **Do jagah ek saath mat chalao:** Heroku deploy karne se pehle VPS wala bot **band** kar do
> (`pkill -f view.py` ya `./start` ka stop process). Agar dono ek saath chale to same accounts
> Telegram pe duplicate connection banayenge (`AUTH_KEY_DUPLICATED` / flood wait) aur accounts ban ho sakte hain.

### Option 1 — Heroku CLI (recommended)

```bash
# 1. Heroku CLI install karo (apne laptop pe): https://devcenter.heroku.com/articles/heroku-cli
heroku login

# 2. Is repo ke folder me aao, app banao
heroku create telegram-smm-bot

# 3. Buildpacks — ORDER zaroori hai: ffmpeg PEHLE, python BAAD me
heroku buildpacks:add --index 1 https://github.com/jonathanong/heroku-buildpack-ffmpeg-latest.git
heroku buildpacks:add --index 2 heroku/python

# 4. (Optional) Config Vars — code me hardcoded defaults hain, isliye zaroori nahi,
#    lekin apni values dalna better hai
heroku config:set API_ID=21538384 API_HASH=your_hash BOT_TOKEN=your_token MONGO_URI=mongodb+srv://... OWNER_IDS=8015937475

# 5. Deploy (main branch ko push karo)
git push heroku main:master

# 6. Worker dyno ON karo (yehi bot ko chalata hai)
heroku ps:scale worker=1

# 7. Logs dekho
heroku logs --tail
```

Bot start hote hi `sessions/` folder se accounts load honge. Log me dikhega:

```
  mongodb      : connected (clients only)
  bot          : @YourBotUsername
  accounts     : 12 online (dead 0, unclear 0)
  live audio   : ready (83.4 MB, loops, max 50 streams)
```

### Option 2 — GitHub se (dashboard / Deploy button)

1. Repo GitHub pe push kar do (default branch `main`).
2. [heroku.com](https://heroku.com) → **New → Create new app** → app ka naam do.
3. **Deploy** tab → **GitHub** se connect karo → repo select karo → **Enable Automatic Deploys** (optional).
4. **Settings → Buildpacks** me confirm karo: `ffmpeg-latest` **pehle**, `heroku/python` **baad me**
   (repo me `app.json` hai, isliye ye auto-set bhi ho sakta hai).
5. **Resources** tab me `worker` dyno ko ON karo (pencil icon → toggle).
6. **Deploy Branch** dabao.

### Heroku ke baad useful commands

```bash
heroku logs --tail                 # live logs
heroku restart                     # restart (sessions MongoDB se auto-restore honge)
heroku ps                          # dyno status
heroku config                      # config vars dekho
heroku ps:scale worker=0           # band karna
```

---

## 🧠 Heroku pe sessions/accounts kaise survive karte hain

Heroku ka filesystem **ephemeral** hai — har deploy/restart pe `sessions/` folder wipe ho jata hai.
Isliye ye repo ab **`session_store.py`** ke saath aata hai:

- Har **5 minute** me saare `.session` files (aur unke `.json` meta) **MongoDB** me backup ho jaate hain
  (collection: `session_backup`).
- Bot jab bhi **khaali sessions folder** ke saath boot hota hai (naya dyno), to backup se
  automatically saare accounts restore kar leta hai, phir normal load hota hai.
- Account delete karoge bot se → backup me bhi `deleted` mark ho jata hai → restart pe wapas nahi aayega.
- Folder runtime me hi source of truth hai; MongoDB sirf recovery copy hai.

> Backup me session bytes hi jate hain (sqlite backup API se consistent snapshot), password ya code nahi.

## ⚙️ Requirements / versions

| Package | Version | Kyun |
|---|---|---|
| telethon | 1.36.0 | Bot + accounts (core) |
| motor | 3.4.0 | MongoDB (Atlas) |
| py-tgcalls | 2.2.11 | Live audio streaming |
| ntgcalls | 2.1.0 | py-tgcalls ka native engine |

- Python **3.11.12** (`runtime.txt` + `.python-version`).
- `ffmpeg` binary Heroku pe **ffmpeg buildpack** se aata hai — live audio ke liye zaroori hai.
- `audio/live.mp3` (87 MB) repo me committed hai — slug limit (500 MB) ke andar hai.
- MongoDB Atlas ke **Network Access** me `0.0.0.0/0` allow hona chahiye (VPS wale pe pehle se hoga).

## 🛠 Troubleshooting

- **Build fail — `ntgcalls` error:** purane `requirements.txt` me `ntgcalls==1.2.5` tha jo PyPI pe
  exist hi nahi karta (isliye build toot jaata tha). Ab `ntgcalls==2.1.0` + `py-tgcalls==2.2.11`
  pinned hai — dono verified manylinux wheels hain.
- **Accounts sab "dead" dikh rahe restart ke baad:** pehla deploy pe backup nahi tha (backup tabhi
  start hota hai jab bot pehli baar chala). Deploy se pehle **VPS wale bot ko 1-2 minute chala ke**
  backup bharne do, ya sessions ZIP ke through dobara upload karo (Accounts → Import ZIP). Uske baad
  har restart pe auto-restore hoga.
- **Live audio nahi chalta:** ffmpeg buildpack add karna bhool gaye? Check karo:
  `heroku buildpacks` me ffmpeg pehle hona chahiye. Log me `live audio : UNAVAILABLE` dikhega.
- **Bot start nahi ho raha:** `heroku logs --tail` dekho — sabse common reason: Mongo unreachable
  (Atlas whitelist) ya duplicate instance (VPS + Heroku dono chal rahe).
- **`heroku ps:scale worker=1` me error:** account pe koi paid plan nahi hai — Eco/Hobby plan chahiye.

## 🖥 VPS wala setup (unchanged)

`./start` script pehle jaisa hi kaam karta hai (venv + PID + background). Heroku files ne kuch nahi toda:
`Procfile`/`runtime.txt`/`app.json` sirf Heroku buildpack dekhta hai, `start` script unhe ignore karta hai.
