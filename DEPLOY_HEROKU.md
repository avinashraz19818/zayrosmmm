# Heroku deployment (MongoDB-backed)

The bot is a Telegram long-polling worker, not an HTTP website. The correct
Heroku process is therefore **one worker dyno**:

```text
worker: python view.py
```

Heroku dyno storage is ephemeral. This project now treats MongoDB as the source
of truth for:

- client subscriptions, approvals, settings, counters and activity history;
- every Telegram `.session` SQLite file and its `.json` device/API sidecar;
- the bot's own `bot_session.session`;
- session trash and the live audio file.

Telethon receives temporary local copies because it needs SQLite paths. On every
boot the copies are restored from MongoDB/GridFS, and changed files are synced
back during the worker lifetime.

## 0. Rotate the old credentials first

The old VPS version contained Telegram, bot and MongoDB credentials directly in
source code. They have been removed from `view.py`, but any credentials that
were ever committed or shared should be considered exposed:

1. create a new MongoDB database user/password and revoke the old user;
2. use BotFather to revoke/regenerate the bot token;
3. rotate Telegram API credentials if they were shared publicly;
4. never commit `.env` or a session file.

## 1. Prepare MongoDB Atlas

Create an Atlas cluster and a database user with access only to the bot database.
Add a Network Access rule that permits Heroku's dynamic egress addresses. For a
small private bot this is commonly `0.0.0.0/0` **combined with** a strong,
least-privilege database user and TLS (`mongodb+srv://`); do not use an admin
user. Enable Atlas backups/point-in-time recovery if the account tier supports
it.

The default database and GridFS bucket are:

```text
database: tg_manager_bot
bucket:   tg_manager_storage
```

They can be changed with `MONGO_DB_NAME` and `MONGO_GRIDFS_BUCKET`.

## 2. Copy the current VPS data to MongoDB

Stop the old VPS bot first so SQLite files do not change during the copy. On the
VPS, from this repository (or copy `migrate_to_mongodb.py` there):

```bash
export MONGO_URI='mongodb+srv://DB_USER:DB_PASSWORD@.../?retryWrites=true&w=majority&appName=tg-manager'
export MONGO_DB_NAME='tg_manager_bot'
export MONGO_GRIDFS_BUCKET='tg_manager_storage'

python -m pip install -r requirements.txt
python migrate_to_mongodb.py --source /path/to/zayrosmmm
```

The migration uploads session files, JSON sidecars, `sessions_trash/`,
`audio/live.mp3` and `bot_session.session`. It does not overwrite an existing
MongoDB copy unless `--force` is supplied. Check the receipt and, if necessary,
run:

```bash
python migrate_to_mongodb.py --source /path/to/zayrosmmm --dry-run
```

Client/subscription data already lives in the MongoDB database used by the old
bot. Point `MONGO_URI` at that same database during migration. If the old VPS
used a different database, migrate that database with MongoDB/Atlas tools before
switching the app.

## 3. Create and configure the Heroku app

Install the Heroku CLI and run:

```bash
heroku login
heroku create YOUR-APP-NAME
heroku buildpacks:clear -a YOUR-APP-NAME
heroku buildpacks:add --index 1 https://github.com/heroku/heroku-buildpack-apt -a YOUR-APP-NAME
heroku buildpacks:add heroku/python -a YOUR-APP-NAME
```

The Apt buildpack reads `Aptfile` and installs `ffmpeg` for live audio. If the
Audio/Go Live feature is not needed, the Python worker can still run without
that optional dependency.

Set secrets as Heroku Config Vars, not in files:

```bash
heroku config:set \
  API_ID='YOUR_TELEGRAM_API_ID' \
  API_HASH='YOUR_TELEGRAM_API_HASH' \
  BOT_TOKEN='YOUR_BOTFATHER_TOKEN' \
  MONGO_URI='mongodb+srv://DB_USER:DB_PASSWORD@.../?retryWrites=true&w=majority&appName=tg-manager' \
  MONGO_DB_NAME='tg_manager_bot' \
  MONGO_GRIDFS_BUCKET='tg_manager_storage' \
  OWNER_IDS='8015937475' \
  -a YOUR-APP-NAME
```

Use your real owner Telegram ID(s), not the example in `.env.example`.

## 4. Deploy this branch and scale one worker

This session is on branch `arena/01a015ce-zayrosmmm`. If `heroku` is the
app's git remote:

```bash
git push heroku arena/01a015ce-zayrosmmm:main
heroku ps:scale worker=1 -a YOUR-APP-NAME
heroku logs --tail -a YOUR-APP-NAME
```

Do **not** scale this bot to two workers. Two Telegram long-pollers can both
receive updates, and two processes can open the same account SQLite sessions;
that can cause duplicate actions or locked sessions. Heroku automatically
restarts a crashed worker, then the boot restore loads the same MongoDB data.

The `start` file is a VPS helper that expects `.venv` and backgrounds a process;
it is intentionally not used by Heroku. Heroku uses `Procfile`.

## Purging account sessions after a duplicate-IP incident

Never run the old VPS worker and the Heroku worker with the same Telegram
sessions. If Telegram has invalidated those auth keys and you want a clean
Heroku start, scale the worker down, deploy this repository version, and run:

```bash
heroku ps:scale worker=0 -a YOUR-APP-NAME
heroku run python purge_mongodb_sessions.py --yes -a YOUR-APP-NAME
```

That command removes only `sessions/`, `trash/` and the old `bot/` GridFS
objects. It preserves clients, subscriptions, approvals, history, statistics,
settings and `audio/live.mp3`. Add new accounts later through the bot's normal
login/import flow. Do not use the bot UI's many `prob_rm_*` buttons at once;
callback queries expire and are not a bulk-delete mechanism.

## 5. First-boot checks

In the logs, confirm these lines (wording may include timestamps):

```text
mongodb      : connected (clients + GridFS runtime storage)
status       : ready; MongoDB is the persistence layer
```

Then message the bot with `/start`. The owner should see the existing accounts
and clients. Open **Accounts** and **Audio** to verify the restored session count
and audio file. A dyno restart test is useful:

```bash
heroku restart -a YOUR-APP-NAME
heroku logs --tail -a YOUR-APP-NAME
```

The local `/tmp` or dyno working files may disappear; that is expected. The
accounts and client data must come back from MongoDB/GridFS.

## Security and operational notes

- MongoDB Atlas TLS/encryption-at-rest protects the database in transit/at rest,
  but Telegram session files are still sensitive credentials. Restrict the DB
  user, rotate credentials, and enable Atlas backups.
- Keep `OWNER_IDS` set. An empty owner list makes all owner actions inaccessible.
- Use one worker dyno. Heroku's worker dyno is a paid process on current Heroku
  plans; this bot cannot be kept alive by a static web dyno alone.
- Monitor Atlas storage: GridFS retains the current object only because replaced
  versions are deleted from the bucket. Atlas backups have their own retention.
- A failed upload never replaces the MongoDB manifest, and audio uploads use a
  `.part` file before replacement, so an interrupted dyno transfer does not
  intentionally publish a truncated runtime file.
