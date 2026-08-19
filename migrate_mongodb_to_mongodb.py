#!/usr/bin/env python3
"""Copy the bot database, including GridFS, to a second MongoDB cluster.

The current app keeps the source URI in MONGO_URI. Set MONGO_TARGET_URI as a
Heroku Config Var for a one-off migration dyno. The target database is dropped
collection-by-collection only when --yes is supplied; the source is never
modified.

Run a preview first:
  python migrate_mongodb_to_mongodb.py
Then run the copy:
  python migrate_mongodb_to_mongodb.py --yes
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from pymongo import MongoClient


BATCH_SIZE = 500


def utcnow():
    return datetime.now(timezone.utc)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes", action="store_true",
                        help="drop target collections and copy data")
    args = parser.parse_args()

    source_uri = os.getenv("MONGO_URI", "").strip()
    target_uri = os.getenv("MONGO_TARGET_URI", "").strip()
    db_name = os.getenv("MONGO_DB_NAME", "tg_manager_bot").strip() or "tg_manager_bot"
    if not source_uri or not target_uri:
        raise SystemExit("MONGO_URI and MONGO_TARGET_URI Config Vars are required")
    if source_uri == target_uri:
        raise SystemExit("Source and target MongoDB URIs must be different")

    source = MongoClient(source_uri, serverSelectionTimeoutMS=20000,
                         connectTimeoutMS=20000)
    target = MongoClient(target_uri, serverSelectionTimeoutMS=20000,
                         connectTimeoutMS=20000)
    try:
        source.admin.command("ping")
        target.admin.command("ping")
        source_db = source[db_name]
        target_db = target[db_name]
        collections = sorted(
            name for name in source_db.list_collection_names()
            if not name.startswith("system.")
        )
        print(f"Source database: {db_name}")
        print(f"Collections: {len(collections)}")
        for name in collections:
            print(f"  {name}: {source_db[name].estimated_document_count()} document(s)")
        if not args.yes:
            print("Preview only. Nothing was changed on the target.")
            print("Run again with --yes to copy all listed collections, including GridFS files/chunks.")
            return

        copied = 0
        for name in collections:
            source_collection = source_db[name]
            target_collection = target_db[name]
            target_collection.drop()
            batch = []
            for document in source_collection.find({}):
                batch.append(document)
                if len(batch) >= BATCH_SIZE:
                    target_collection.insert_many(batch, ordered=False)
                    copied += len(batch)
                    batch = []
            if batch:
                target_collection.insert_many(batch, ordered=False)
                copied += len(batch)
            # Recreate ordinary indexes after the documents. GridFS indexes are
            # recreated by the driver on first use if needed.
            for index in source_collection.list_indexes():
                if index["name"] == "_id_":
                    continue
                keys = list(index["key"].items())
                options = {k: v for k, v in index.items()
                           if k not in {"v", "key", "name", "ns"}}
                try:
                    target_collection.create_index(keys, name=index["name"], **options)
                except Exception as exc:
                    print(f"Index warning {name}/{index['name']}: {exc}")
            print(f"COPIED {name}")
        print(f"Migration complete: {copied} document(s) copied")
        print("Source database was not modified.")
    finally:
        source.close()
        target.close()


if __name__ == "__main__":
    main()
