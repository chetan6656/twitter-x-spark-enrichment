import argparse
import json
import os
import time
from datetime import datetime, timezone

import boto3
import httpx

from global_rate_limiter import GlobalRateLimiter


ZYTE_URL = "https://api.zyte.com/v1/search"


def load_zyte_key():
    secret_name = os.environ["ZYTE_SECRET_NAME"]
    region = os.environ.get("AWS_REGION", "us-east-1")

    client = boto3.client("secretsmanager", region_name=region)
    secret = client.get_secret_value(SecretId=secret_name)

    value = secret.get("SecretString", "")
    data = json.loads(value)

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue-url", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--response-prefix", default="responses/twitter-x")
    parser.add_argument("--qps", type=int, default=200)
    parser.add_argument("--visibility-timeout", type=int, default=900)
    args = parser.parse_args()

    region = os.environ.get("AWS_REGION", "us-east-1")
    sqs = boto3.client("sqs", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    api_key = load_zyte_key()
    limiter = GlobalRateLimiter(qps=args.qps)

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

                    payload = None
                    last_error = None

                    for attempt in range(3):
                        try:
                            payload = search_zyte(http, api_key, query)
                            break
                        except Exception as exc:
                            last_error = str(exc)
                            if attempt < 2:
                                time.sleep(2 ** attempt)

                    response = {
                        "query_id": query_id,
                        "contact_id": body["contact_id"],
                        "query_number": body["query_number"],
                        "query": query,
                        "status": "success" if payload is not None else "failed",
                        "error": last_error,
                        "payload": payload,
                        "processed_at": datetime.now(timezone.utc).isoformat(),
                    }

                    s3.put_object(
                        Bucket=args.bucket,
                        Key=f"{args.response_prefix}/{query_id}.json",
                        Body=json.dumps(response).encode("utf-8"),
                        ContentType="application/json",
                    )

                    sqs.delete_message(
                        QueueUrl=args.queue_url,
                        ReceiptHandle=message["ReceiptHandle"],
                    )

                except Exception:
                    # Leave the message in SQS so it can be retried or sent
                    # to a dead-letter queue after the configured retry limit.
                    continue


if __name__ == "__main__":
    main()
