#!/usr/bin/env python3
"""One-time VPS -> MongoDB/GridFS migration.

Run this on the old VPS while the bot is stopped. The application also performs
an automatic "only if missing" import at startup, but this script gives a clear
receipt before the VPS is switched off and supports a deliberate --force retry.

Required environment variables:
  MONGO_URI, optionally MONGO_DB_NAME and MONGO_GRIDFS_BUCKET

Example:
  export MONGO_URI='mongodb+srv://...'
  python migrate_to_mongodb.py --source /opt/zayrosmmm
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket


DEFAULT_DB = "tg_manager_bot"
DEFAULT_BUCKET = "tg_manager_storage"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def remote_name(prefix: str, relative: Path) -> str:
    clean = "/".join(part for part in relative.as_posix().split("/")
                     if part not in ("", ".", ".."))
    return f"{prefix.strip('/')}/{clean}" if clean else prefix.strip("/")


def files_to_migrate(source: Path) -> Iterable[tuple[str, Path, str]]:
    sessions = source / "sessions"
    if sessions.is_dir():
        for path in sorted(p for p in sessions.rglob("*") if p.is_file()):
            if path.suffix in {".session", ".json", ".txt", ".imported"}:
                yield remote_name("sessions", path.relative_to(sessions)), path, "session"

    trash = source / "sessions_trash"
    if trash.is_dir():
        for path in sorted(p for p in trash.rglob("*") if p.is_file()):
            yield remote_name("trash", path.relative_to(trash)), path, "trash"

    audio = source / "audio" / "live.mp3"
    if audio.is_file():
        yield "audio/live.mp3", audio, "audio"

    bot_base = source / "bot_session"
    for suffix in (".session", ".session-journal", ".session-wal", ".session-shm"):
        path = Path(str(bot_base) + suffix)
        if path.is_file():
            yield f"bot/bot_session{suffix}", path, "bot_session"


async def migrate(source: Path, force: bool, dry_run: bool) -> int:
    uri = os.getenv("MONGO_URI", "").strip()
    if not uri:
        raise SystemExit("MONGO_URI is required; no credentials are read from source code")
    db_name = os.getenv("MONGO_DB_NAME", DEFAULT_DB).strip() or DEFAULT_DB
    bucket_name = os.getenv("MONGO_GRIDFS_BUCKET", DEFAULT_BUCKET).strip() or DEFAULT_BUCKET

    client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=20000,
                                connectTimeoutMS=20000, retryWrites=True)
    db = client[db_name]
    manifest = db["storage_manifest"]
    bucket = AsyncIOMotorGridFSBucket(db, bucket_name=bucket_name)
    await client.admin.command("ping")
    await manifest.create_index("gridfs_id")

    copied = skipped = failed = 0
    try:
        for name, path, kind in files_to_migrate(source):
            digest = await asyncio.to_thread(sha256, path)
            old = await manifest.find_one({"_id": name})
            if old and not force and old.get("sha256") == digest:
                print(f"SKIP   {name} ({path.stat().st_size} bytes, already present)")
                skipped += 1
                continue
            if dry_run:
                action = "REPLACE" if old else "UPLOAD"
                print(f"DRYRUN {action} {name} ({path.stat().st_size} bytes)")
                copied += 1
                continue
            try:
                with path.open("rb") as source_file:
                    gridfs_id = await bucket.upload_from_stream(
                        name,
                        source_file,
                        metadata={"kind": kind, "sha256": digest,
                                  "size": path.stat().st_size},
                    )
                await manifest.replace_one(
                    {"_id": name},
                    {"_id": name, "gridfs_id": gridfs_id, "kind": kind,
                     "sha256": digest, "size": path.stat().st_size,
                     "updated_at": datetime.now(timezone.utc)},
                    upsert=True,
                )
                if old and old.get("gridfs_id") != gridfs_id:
                    try:
                        await bucket.delete(old["gridfs_id"])
                    except Exception:
                        pass
                print(f"UPLOAD {name} ({path.stat().st_size} bytes)")
                copied += 1
            except Exception as exc:
                print(f"FAIL   {name}: {exc}")
                failed += 1
    finally:
        client.close()

    print(f"Done: uploaded={copied}, skipped={skipped}, failed={failed}")
    return failed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=".",
                        help="old VPS project directory (default: current directory)")
    parser.add_argument("--force", action="store_true",
                        help="replace existing MongoDB copies; use only after checking the source")
    parser.add_argument("--dry-run", action="store_true",
                        help="list what would be uploaded without changing MongoDB")
    args = parser.parse_args()
    failed = asyncio.run(migrate(Path(args.source).expanduser().resolve(),
                                 args.force, args.dry_run))
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
