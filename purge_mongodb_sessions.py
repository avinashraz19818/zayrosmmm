#!/usr/bin/env python3
"""Remove Telegram account session objects from MongoDB/GridFS.

This deliberately does NOT touch clients, subscriptions, approvals, history,
statistics, settings or audio. It removes only the user-account sessions,
trash copies and the old bot session object. Use it when switching to a clean
Heroku bot after a duplicate-IP session incident.

The default is a preview. Add --yes to delete.
"""

from __future__ import annotations

import argparse
import os
import re

from gridfs import GridFS
from pymongo import MongoClient


PREFIX_RE = re.compile(r"^(sessions|trash|bot)/")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true",
                        help="permanently delete session objects")
    args = parser.parse_args()

    uri = os.getenv("MONGO_URI", "").strip()
    if not uri:
        raise SystemExit("MONGO_URI Config Var is missing")
    db_name = os.getenv("MONGO_DB_NAME", "tg_manager_bot").strip() or "tg_manager_bot"
    bucket_name = os.getenv("MONGO_GRIDFS_BUCKET", "tg_manager_storage").strip() or "tg_manager_storage"

    client = MongoClient(uri, serverSelectionTimeoutMS=20000,
                         connectTimeoutMS=20000)
    try:
        client.admin.command("ping")
        db = client[db_name]
        manifest = db["storage_manifest"]
        fs = GridFS(db, collection=bucket_name)

        manifest_docs = list(manifest.find(
            {"_id": {"$regex": r"^(sessions|trash|bot)/"}},
            {"_id": 1, "gridfs_id": 1, "size": 1},
        ))
        gridfs_files = list(fs.find({"filename": {"$regex": r"^(sessions|trash|bot)/"}}))
        total_bytes = sum(int(d.get("size", 0) or 0) for d in manifest_docs)
        print(f"Session manifests: {len(manifest_docs)}")
        print(f"GridFS objects: {len(gridfs_files)}")
        print(f"Manifest bytes: {total_bytes}")
        print("Preserved collections: clients, history, approved_users, bot_stats, settings")
        print("Preserved file: audio/live.mp3")

        if not args.yes:
            print("Preview only. Run again with --yes to permanently delete these objects.")
            return

        deleted_files = set()
        for file_doc in gridfs_files:
            try:
                fs.delete(file_doc._id)
                deleted_files.add(file_doc._id)
            except Exception as exc:
                print(f"GridFS delete warning for {file_doc._id}: {exc}")

        result = manifest.delete_many({"_id": {"$regex": r"^(sessions|trash|bot)/"}})
        # The session keys are no longer valid after the purge. Clear only the
        # membership references; package limits, prices, expiry and every other
        # client field remain untouched. New accounts can then be onboarded
        # without old invalid keys inflating the joined_accounts list.
        membership_result = db["clients"].update_many(
            {"joined_accounts": {"$exists": True}},
            {"$set": {"joined_accounts": []}},
        )
        print(f"Deleted GridFS objects: {len(deleted_files)}")
        print(f"Deleted manifests: {result.deleted_count}")
        print(f"Cleared client membership references: {membership_result.modified_count}")
        print("Account session purge complete; client packages were preserved.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
