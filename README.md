# Telegram SMM bot

This repository contains the Telegram account/client manager. It is ready for a
single Heroku **worker** dyno and uses MongoDB Atlas plus GridFS for persistent
runtime data. See [`DEPLOY_HEROKU.md`](DEPLOY_HEROKU.md) for the VPS migration,
Heroku Config Vars, buildpacks and restart checks.

Never commit `.env`, Telegram session files, bot tokens or MongoDB credentials.
Use `.env.example` only as a template.
