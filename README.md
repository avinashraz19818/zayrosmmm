# Telegram SMM Bot (Zayros)

[![Deploy to Heroku](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/avinashraz19818/zayrosmmm)

Heroku Direct One-Click Deploy URL:  
**https://heroku.com/deploy?template=https://github.com/avinashraz19818/zayrosmmm**

Alternative Heroku New App Template URL:  
**https://dashboard.heroku.com/new?template=https://github.com/avinashraz19818/zayrosmmm**

---

## 🚀 Features

- **Automated Post Reactions & Views**: Deterministic, natural emoji distributions across active client channels.
- **Voice Chat & Livestream**: Auto-join and live audio streaming via `py-tgcalls` and `ffmpeg`.
- **Durable Persistence**: Backed by MongoDB Atlas & GridFS — sessions survive dyno restarts and ephemeral filesystem wipes.
- **Worker Sharding**: Automatically balances account sessions across multiple dynos or runs smoothly on a single dyno.
- **Web Management Dashboard**: Beautiful, secure web UI with one-time login links to manage sessions, tasks, and client plans.
- **Dead & Frozen Account Detection**: Automatically detects, tags, and isolates dead or frozen accounts without crashing or stalling post tasks.

---

## 🛠️ Heroku Deployment Guide (1-Click Direct Deploy)

1. Click the **Deploy to Heroku** button above (or open the link).
2. Enter your app name (or let Heroku choose one).
3. Fill in the required **Config Vars**:
   - `API_ID`: Your Telegram API ID from [my.telegram.org](https://my.telegram.org).
   - `API_HASH`: Your Telegram API Hash from [my.telegram.org](https://my.telegram.org).
   - `BOT_TOKEN`: Telegram Bot Token created via [@BotFather](https://t.me/BotFather).
   - `MONGO_URI`: Your MongoDB connection string (e.g., `mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority`).
   - `OWNER_IDS`: Comma-separated Telegram User IDs of administrators (e.g. `123456789,987654321`). Get yours with `/id` on [@userinfobot](https://t.me/userinfobot).
   - `HEROKU_APP_NAME`: Set to your Heroku app name for automatic web dashboard URLs and worker scaling.
4. Click **Deploy App**.
5. Once deployment completes, both `web` and `worker` dynos will run automatically.

---

## 🤖 Bot Commands

- `/start` — Open interactive bot menu.
- `/dashboard` — Generate an authenticated, one-time link to open the web management dashboard.
- `/id` — Display your Telegram user ID.
- `/add <user_id>` — Approve a user to access the bot (Owner only).
- `/remove <user_id>` — Revoke access from a user (Owner only).
- `/list` — View approved users.
- `/reload` — Reload accounts and sync sessions from MongoDB.

For migration from VPS and advanced sharded scaling, see [`DEPLOY_HEROKU.md`](DEPLOY_HEROKU.md).
