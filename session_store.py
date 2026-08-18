"""
Session persistence for hosts with an ephemeral filesystem (Heroku).

Telegram account sessions live in the sessions/ folder as sqlite files, and on
Heroku that folder is wiped on every dyno restart or deploy. This module
mirrors every .session file (plus its .json meta sidecar) into MongoDB, and
re-hydrates the folder from MongoDB when the bot boots on a fresh dyno.

The sessions/ folder stays the single source of truth at runtime — MongoDB is
only a recovery copy. A periodic task keeps it in sync, so a restarted dyno
picks up exactly the accounts that existed before the restart.
"""

import asyncio
import datetime
import hashlib
import logging
import os
import sqlite3
import tempfile

logger = logging.getLogger("viewbot.sessions")

BACKUP_COLLECTION = "session_backup"
BACKUP_INTERVAL = 300  # seconds between syncs


def _session_bytes(path: str) -> bytes:
    """Consistent snapshot of a sqlite .session file.

    Uses sqlite's online backup so the copy is valid even while Telethon has
    the file open and is writing to it. Falls back to a raw file copy if the
    file is not a readable sqlite database.
    """
    try:
        src = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
        fd, tmp = tempfile.mkstemp(prefix=".sessbak-", suffix=".sqlite")
        os.close(fd)
        try:
            dst = sqlite3.connect(tmp)
            try:
                with dst:
                    src.backup(dst)
            finally:
                dst.close()
            with open(tmp, "rb") as f:
                return f.read()
        finally:
            src.close()
            try:
                os.unlink(tmp)
            except OSError:
                pass
    except Exception:
        # Not a sqlite file (or unreadable) — take the raw bytes as-is.
        with open(path, "rb") as f:
            return f.read()


async def restore_sessions(mdb, sessions_dir: str) -> int:
    """Re-create session files from MongoDB when the folder is empty.

    Only runs when sessions/ has no .session files at all (i.e. a fresh dyno
    after Heroku wiped the disk). Never overwrites files that already exist.
    Returns the number of accounts restored.
    """
    try:
        existing = [f for f in os.listdir(sessions_dir) if f.endswith(".session")]
    except OSError:
        return 0
    if existing:
        return 0

    docs = await mdb[BACKUP_COLLECTION].find(
        {"deleted": {"$ne": True}}
    ).sort("updated_at", 1).to_list(length=None)

    restored = 0
    for doc in docs:
        stem = doc.get("_id")
        data = doc.get("session")
        if not isinstance(stem, str) or not stem or not data:
            continue
        # never let a malicious/mangled _id escape the sessions folder
        if "/" in stem or "\\" in stem or ".." in stem:
            continue
        try:
            with open(os.path.join(sessions_dir, stem + ".session"), "wb") as f:
                f.write(bytes(data))
            meta = doc.get("meta")
            if meta:
                with open(os.path.join(sessions_dir, stem + ".json"), "wb") as f:
                    f.write(bytes(meta))
            restored += 1
        except OSError as e:
            logger.warning("could not restore session %s: %s", stem, e)

    if restored:
        logger.info("restored %d session(s) from MongoDB backup", restored)
    return restored


async def backup_sessions(mdb, sessions_dir: str) -> int:
    """Mirror every .session file (+ .json meta) into MongoDB.

    Files whose content changed since the last sync are re-uploaded; files that
    disappeared from the folder (deleted by the admin through the bot) are
    marked deleted so a restore never resurrects them. Content is compared by
    sha256 of the raw file, because mtime granularity is unreliable on some
    filesystems (Docker/overlayfs, Heroku dynos). Returns the number of
    documents changed.
    """
    now = datetime.datetime.utcnow()
    docs = {}
    async for d in mdb[BACKUP_COLLECTION].find({}):
        docs[str(d.get("_id") or "")] = d

    changed = 0
    seen = set()
    try:
        entries = sorted(os.listdir(sessions_dir))
    except OSError:
        entries = []

    for f in entries:
        if not f.endswith(".session"):
            continue
        stem = f[: -len(".session")]
        if not stem or "/" in stem or "\\" in stem:
            continue
        seen.add(stem)
        path = os.path.join(sessions_dir, f)
        try:
            mtime = os.path.getmtime(path)
            size = os.path.getsize(path)
            with open(path, "rb") as fh:
                raw_hash = hashlib.sha256(fh.read()).hexdigest()
        except OSError:
            continue
        old = docs.get(stem)
        if (old is not None
                and old.get("deleted") is False
                and old.get("session") is not None
                and old.get("raw_sha256") == raw_hash):
            continue  # content unchanged since last sync

        data = _session_bytes(path)
        meta = None
        meta_path = os.path.splitext(path)[0] + ".json"
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "rb") as fh:
                    meta = fh.read()
            except OSError:
                meta = None

        await mdb[BACKUP_COLLECTION].replace_one(
            {"_id": stem},
            {"_id": stem, "session": data, "meta": meta, "mtime": mtime,
             "size": size, "raw_sha256": raw_hash, "deleted": False,
             "updated_at": now},
            upsert=True,
        )
        changed += 1

    for stem, doc in docs.items():
        if not stem or stem in seen or doc.get("deleted"):
            continue
        await mdb[BACKUP_COLLECTION].update_one(
            {"_id": stem},
            {"$set": {"deleted": True, "updated_at": now}},
        )
        changed += 1

    if changed:
        logger.info("session backup: %d change(s) synced", changed)
    return changed


async def backup_task(mdb, sessions_dir: str,
                      interval: int = BACKUP_INTERVAL) -> None:
    """Background loop: keep the MongoDB copy of sessions/ up to date."""
    while True:
        try:
            await backup_sessions(mdb, sessions_dir)
        except Exception as e:
            logger.warning("session backup failed: %s", e)
        await asyncio.sleep(interval)
