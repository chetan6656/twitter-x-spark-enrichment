# AWS Runbook

This workflow is not production-ready until the 10,000-contact acceptance test
passes. The application owns the query plan, producer, worker, scoring, tests,
and metrics. Infrastructure owns EMR, S3, SQS/DLQ, Redis, IAM, networking, and
Secrets Manager.

## First test

1. Upload contacts to the input S3 prefix without committing or sharing the
   file through GitHub.
2. Confirm the Zyte account-approved QPS. Start with `50`; set `200` only after
   written approval.
3. Run Spark query preparation. Validate that the output count is exactly
   `2 * valid_contact_count` and that each `contact_id` has query numbers 1 and 2.
4. Run `spark/sqs_producer.py` with a durable journal. Re-run it and confirm
   that the second run publishes zero new query IDs.
5. Deploy at least two worker processes, all pointing to the same Redis URL.
   Confirm CloudWatch request counts divided by elapsed seconds never exceed
   the configured global QPS.
6. Wait for the queue and DLQ to settle. Score the S3 responses with
   `spark/score_results.py` and inspect coverage, confidence, and failures.
7. Repeat the scoring job and confirm completed responses are not lost or
   refetched.

## PowerShell examples

```powershell
$env:AWS_REGION = "us-east-1"
$env:GLOBAL_QPS = "50"
py -3 spark\prepare_queries.py `
  --input s3://BUCKET/input/contacts/ `
  --output s3://BUCKET/query-plan/run-001/ `
  --input-format parquet

py -3 spark\sqs_producer.py `
  --input query-plan.csv `
  --queue-url https://sqs.REGION.amazonaws.com/ACCOUNT/twitter-x-queries `
  --journal query-journal.sqlite3
```

## Acceptance evidence

Capture the run ID, valid contact count, query count, queue messages sent,
successful and failed responses, retry count, HTTP status histogram, actual
QPS, runtime, coverage, trusted-handle count, and DLQ count. Redact API keys
and contact data from logs and tickets.

## Operational rules

- A successful `query_id` is immutable and is never submitted again.
- Retryable failures remain observable and are retried by SQS visibility/DLQ
  policy; permanent failures are written to the failure prefix.
- 401/403 and credit/account errors require stopping the deployment, not
  retrying indefinitely.
- The worker is the only component allowed to call Zyte.
