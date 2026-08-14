import argparse
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone

from global_rate_limiter import GlobalRateLimiter


ZYTE_URL = "https://api.zyte.com/v1/search"


def load_zyte_key():
    import boto3
    secret_name = os.environ["ZYTE_SECRET_NAME"]
    region = os.environ.get("AWS_REGION", "us-east-1")

    client = boto3.client("secretsmanager", region_name=region)
    secret = client.get_secret_value(SecretId=secret_name)

    value = secret.get("SecretString", "")
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        data = {}

    return data.get("ZYTE_API_KEY") or data.get("api_key") or value


def search_zyte(client, api_key, query):
    response = client.post(
        ZYTE_URL,
        auth=(api_key, ""),
        json={
            "domain": "google.com",
            "query": query,
            "include": ["organic"],
            "maxResults": 10,
        },
    )

    response.raise_for_status()
    return response.json()


RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504, 520, 521}


def request_with_retry(client, api_key, query, max_retries=3):
    """Return payload and telemetry; do not retry credential or request errors."""
    attempts = 0
    statuses = []
    last_error = ""
    while attempts < max_retries:
        attempts += 1
        try:
            response = client.post(
                ZYTE_URL, auth=(api_key, ""),
                json={"domain": "google.com", "query": query,
                      "include": ["organic"], "maxResults": 10},
            )
            statuses.append(response.status_code)
            if response.status_code == 200:
                return response.json(), attempts, "success", statuses, ""
            last_error = response.text[:500]
            if response.status_code not in RETRYABLE_STATUS:
                return None, attempts, "permanent_failure", statuses, last_error
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        if attempts < max_retries:
            time.sleep(min(2 ** (attempts - 1), 30))
    return None, attempts, "retryable_failure", statuses, last_error


def main():
    import httpx
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-url", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--response-prefix", default="responses/twitter-x")
    parser.add_argument("--failure-prefix", default="responses/twitter-x/failures")
    parser.add_argument("--qps", type=float, default=float(os.environ.get("GLOBAL_QPS", "200")))
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--visibility-timeout", type=int, default=900)
    parser.add_argument("--metrics-namespace", default="TwitterXEnrichment")
    parser.add_argument("--metrics-interval", type=int, default=60)
    args = parser.parse_args()

    import boto3
    region = os.environ.get("AWS_REGION", "us-east-1")
    sqs = boto3.client("sqs", region_name=region)
    s3 = boto3.client("s3", region_name=region)
    cloudwatch = boto3.client("cloudwatch", region_name=region)

    api_key = load_zyte_key()
    limiter = GlobalRateLimiter(qps=args.qps)
    metrics = Counter()
    started_at = time.time()
    last_metrics_at = started_at

    def publish_metrics(force=False):
        nonlocal last_metrics_at
        now = time.time()
        if not force and now - last_metrics_at < args.metrics_interval:
            return
        elapsed = max(now - started_at, 1.0)
        values = {
            "Requests": metrics["requests"],
            "Successes": metrics["success"],
            "Retries": metrics["retries"],
            "RetryableFailures": metrics["retryable_failure"],
            "PermanentFailures": metrics["permanent_failure"],
            "ActualQPS": metrics["requests"] / elapsed,
        }
        cloudwatch.put_metric_data(
            Namespace=args.metrics_namespace,
            MetricData=[{"MetricName": name, "Value": value, "Unit": "Count" if name != "ActualQPS" else "Count/Second"}
                        for name, value in values.items()],
        )
        last_metrics_at = now

    timeout = httpx.Timeout(50.0, connect=20.0)

    with httpx.Client(timeout=timeout, http2=False) as http:
        while True:
            result = sqs.receive_message(
                QueueUrl=args.queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=args.visibility_timeout,
            )

            messages = result.get("Messages", [])

            for message in messages:
                body = json.loads(message["Body"])
                query_id = body["query_id"]
                query = body["query"]

                try:
                    limiter.acquire()
                    payload, attempts, result_status, statuses, error = request_with_retry(
                        http, api_key, query, args.max_retries
                    )
                    metrics["requests"] += 1
                    metrics["retries"] += max(0, attempts - 1)
                    for status in statuses:
                        metrics[f"http_{status}"] += 1
                    if result_status == "success":
                        metrics["success"] += 1
                    elif result_status == "permanent_failure":
                        metrics["permanent_failure"] += 1
                    else:
                        metrics["retryable_failure"] += 1

                    response = {
                        "query_id": query_id,
                        "contact_id": body["contact_id"],
                        "query_number": body["query_number"],
                        "query": query,
                        "status": result_status,
                        "attempt_count": attempts,
                        "http_statuses": statuses,
                        "error": error,
                        "payload": payload,
                        "processed_at": datetime.now(timezone.utc).isoformat(),
                    }

                    prefix = args.response_prefix if result_status == "success" else args.failure_prefix
                    s3.put_object(
                        Bucket=args.bucket,
                        Key=f"{prefix}/{query_id}.json",
                        Body=json.dumps(response).encode("utf-8"),
                        ContentType="application/json",
                    )

                    # Successes and permanent failures are complete. A
                    # retryable failure stays invisible only until the SQS
                    # visibility timeout, then returns for another attempt or
                    # moves to the configured DLQ.
                    if result_status != "retryable_failure":
                        sqs.delete_message(QueueUrl=args.queue_url,
                                           ReceiptHandle=message["ReceiptHandle"])
                    publish_metrics()
                    print(json.dumps({"metric": "worker_progress", **metrics}), flush=True)

                except Exception:
                    # Leave the message in SQS so it can be retried or sent
                    # to a dead-letter queue after the configured retry limit.
                    continue


if __name__ == "__main__":
    main()
