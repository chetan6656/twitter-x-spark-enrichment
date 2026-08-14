"""Score S3 Zyte responses and write a contact-level final deliverable.

The job accepts JSON response objects written by the centralized worker.  The
normal path is a distributed Spark join and UDF; ``--local`` provides a
dependency-free CSV/JSONL mode for contract tests.
"""

import argparse
import csv
import hashlib
import json
from pathlib import Path

from scoring import best_candidate, clean, final_fields


REQUIRED_FIELDS = [
    "twitter_username_final", "twitter_handle_final", "twitter_profile_url_final",
    "twitter_confidence", "twitter_verified", "twitter_match_evidence",
    "twitter_matched_query", "twitter_display_name", "twitter_followers",
]


def load_contacts(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def load_responses(path):
    responses = {}
    root = Path(path)
    files = [root] if root.is_file() else sorted(
        list(root.glob("*.json")) + list(root.glob("*.jsonl"))
    )
    for file_path in files:
        with file_path.open(encoding="utf-8") as handle:
            for line in handle if file_path.suffix == ".jsonl" else [handle.read()]:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("status") == "success":
                    responses.setdefault(record["contact_id"], {}).setdefault(
                        record.get("query", record.get("query_id", "")), record.get("payload", {})
                    )
    return responses


def contact_id(row):
    value = clean(row.get("ic_cntuid"))
    if value:
        return value
    raw = "|".join(clean(row.get(name)) for name in ("ic_fname", "ic_lname", "email", "ic_company"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def score_contacts(contacts, responses):
    output = []
    for contact in contacts:
        result = dict(contact)
        candidate = best_candidate(contact, responses.get(contact_id(contact), {}))
        result.update(final_fields(candidate))
        output.append(result)
    return output


def write_csv(rows, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(dict.fromkeys((list(rows[0]) if rows else []) + REQUIRED_FIELDS))
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_spark(contacts_path, responses_path, parquet_output, csv_output=None,
              input_format="parquet"):
    """Run the same scorer as a distributed Spark transformation."""
    from pyspark.sql import SparkSession, functions as F, types as T

    spark = SparkSession.builder.appName("TwitterXScoreResults").getOrCreate()
    contacts = (spark.read.parquet(contacts_path) if input_format == "parquet"
                else spark.read.option("header", True).option("inferSchema", True).csv(contacts_path))
    responses = (spark.read.json(responses_path)
                 .where(F.col("status") == "success")
                 .select("contact_id", "query", F.to_json("payload").alias("payload_json")))
    grouped = responses.groupBy("contact_id").agg(
        F.map_from_entries(F.collect_list(F.struct("query", "payload_json"))).alias("payloads")
    )
    def col_or_empty(name):
        return F.col(name) if name in contacts.columns else F.lit("")

    joined = contacts.withColumn("_contact_id", F.when(
        col_or_empty("ic_cntuid").isNotNull() & (col_or_empty("ic_cntuid") != ""),
        col_or_empty("ic_cntuid").cast("string")
    ).otherwise(F.sha2(F.concat_ws("|", col_or_empty("ic_fname"), col_or_empty("ic_lname"),
                        col_or_empty("email"), col_or_empty("ic_company")), 256))) \
        .join(grouped, F.col("_contact_id") == grouped.contact_id, "left")

    result_type = T.StructType([T.StructField(name, T.StringType(), True) for name in REQUIRED_FIELDS])

    def score_row(row, payloads):
        payload_map = {}
        for query, raw in (payloads or {}).items():
            try:
                payload_map[query] = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                continue
        return tuple(final_fields(best_candidate(row.asDict(), payload_map)).get(name, "") for name in REQUIRED_FIELDS)

    score_udf = F.udf(score_row, result_type)
    result = joined.withColumn("_score", score_udf(F.struct(*contacts.columns), "payloads"))
    for name in REQUIRED_FIELDS:
        result = result.withColumn(name, F.col(f"_score.{name}"))
    result.drop("_score", "_contact_id", "contact_id", "payloads").write.mode("overwrite").parquet(parquet_output)
    if csv_output:
        result.drop("_score", "_contact_id", "contact_id", "payloads").coalesce(1).write.mode("overwrite").option("header", True).csv(csv_output)
    spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--contacts", required=True)
    parser.add_argument("--responses", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--csv-output")
    parser.add_argument("--input-format", choices=["csv", "parquet"], default="csv")
    parser.add_argument("--local", action="store_true", help="use the dependency-free local runner")
    args = parser.parse_args()
    if args.local:
        write_csv(score_contacts(load_contacts(args.contacts), load_responses(args.responses)), args.output)
    else:
        run_spark(args.contacts, args.responses, args.output, args.csv_output, args.input_format)


if __name__ == "__main__":
    main()
