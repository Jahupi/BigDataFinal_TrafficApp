"""
Watch the MongoDB speeds collection and refresh Tableau CSV files whenever it changes.

Primary mode uses MongoDB change streams for near-real-time updates.
If change streams are unavailable, the script falls back to a lightweight polling loop.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime

from pymongo.errors import PyMongoError

from export_speed_data import export_speed_data, latest_segment_documents, load_collection


POLL_INTERVAL_SECONDS = 60


def current_signature(collection) -> str:
    """
    Build a polling signature from the latest version of each street segment.

    This catches:
    - new inserts
    - updates to existing documents
    - deletes that change the effective latest segment view
    """
    documents = latest_segment_documents(collection)
    digest = hashlib.sha256()

    for doc in documents:
        parts = [
            str(doc.get("link_id", "")),
            str(doc.get("id", "")),
            str(doc.get("link_name", "")),
            str(doc.get("borough", "")),
            str(doc.get("speed", "")),
            str(doc.get("travel_time", "")),
            str(doc.get("data_as_of", "")),
            str(doc.get("status", "")),
            str(doc.get("link_points", "")),
        ]
        digest.update("|".join(parts).encode("utf-8"))
        digest.update(b"\n")

    return digest.hexdigest()


def run_export(reason: str) -> None:
    counts = export_speed_data()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"[{now}] Refreshed Tableau files ({reason}) | "
        f"segments={counts['segments']} path_points={counts['path_points']} summaries={counts['summaries']}"
    )


def watch_with_change_stream(collection) -> None:
    print("Watching MongoDB changes with change streams...")
    with collection.watch(
        full_document="updateLookup",
        max_await_time_ms=1000,
    ) as stream:
        for change in stream:
            operation_type = change.get("operationType", "unknown")
            run_export(f"change stream: {operation_type}")


def watch_with_polling(collection) -> None:
    print("Change streams unavailable. Falling back to polling every 60 seconds...")
    last_signature = current_signature(collection)
    while True:
        time.sleep(POLL_INTERVAL_SECONDS)
        new_signature = current_signature(collection)
        if new_signature != last_signature:
            last_signature = new_signature
            run_export("polling detected a collection change")


def main() -> None:
    client, collection = load_collection()
    try:
        run_export("initial startup")
        try:
            watch_with_change_stream(collection)
        except PyMongoError as error:
            print(f"Change stream error: {error}")
            watch_with_polling(collection)
    finally:
        client.close()


if __name__ == "__main__":
    main()
