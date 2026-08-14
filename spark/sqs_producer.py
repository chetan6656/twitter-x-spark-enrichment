"""Idempotent query-plan producer for SQS.

The producer uses a local SQLite journal by default.  In production place the
journal on durable storage or use the same schema in DynamoDB.  A message is
marked sent only after SQS accepts it, so a restart never skips a query; the
deduplication key prevents already-sent query IDs from being published again.
"""

import argparse
import csv
import hashlib
import json
import sqlite3
import time


def stable_query_id(contact_id, query_number, query):
    raw = f"{contact_id}|{query_number}|{query}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class QueryJournal:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute("CREATE TABLE IF NOT EXISTS sent (query_id TEXT PRIMARY KEY, sent_at REAL NOT NULL)")
        self.db.commit()

    def already_sent(self, query_id):
        return self.db.execute("SELECT 1 FROM sent WHERE query_id = ?", (query_id,)).fetchone() is not None

    def mark_sent(self, query_id):
        self.db.execute("INSERT OR IGNORE INTO sent VALUES (?, ?)", (query_id, time.time()))
        self.db.commit()


def publish_rows(rows, sqs_client, queue_url, journal, batch_size=10):
    sent = skipped = 0
    batch = []
    for row in rows:
        query_id = row.get("query_id") or stable_query_id(row["contact_id"], row["query_number"], row["query"])
        if journal.already_sent(query_id):
            skipped += 1
            continue
        body = {"query_id": query_id, "contact_id": str(row["contact_id"]),
                "query_number": int(row["query_number"]), "query": row["query"]}
        batch.append((query_id, body))
        if len(batch) == batch_size:
            sent += _send_batch(batch, sqs_client, queue_url, journal)
            batch = []
    if batch:
        sent += _send_batch(batch, sqs_client, queue_url, journal)
    return sent, skipped


def _send_batch(batch, sqs_client, queue_url, journal):
    response = sqs_client.send_message_batch(
        QueueUrl=queue_url,
        Entries=[{"Id": str(i), "MessageBody": json.dumps(body)}
                 for i, (_, body) in enumerate(batch)],
    )
    failed = {item["Id"] for item in response.get("Failed", [])}
    for i, (query_id, _) in enumerate(batch):
        if str(i) not in failed:
            journal.mark_sent(query_id)
    if failed:
        raise RuntimeError(f"SQS rejected {len(failed)} messages: {sorted(failed)}")
    return len(batch)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--queue-url", required=True)
    parser.add_argument("--journal", default="query-journal.sqlite3")
    args = parser.parse_args()
    import boto3
    with open(args.input, newline="", encoding="utf-8-sig") as handle:
        rows = csv.DictReader(handle)
        sent, skipped = publish_rows(rows, boto3.client("sqs"), args.queue_url, QueryJournal(args.journal))
    print(json.dumps({"sent": sent, "skipped": skipped}))


if __name__ == "__main__":
    main()
