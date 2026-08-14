# Zyte Worker

This worker must be deployed as a centralized service. It must enforce one global QPS limit across the complete Spark cluster.

Spark executors must not call Zyte independently.

Required behavior:
- Read query records from a shared queue
- Enforce the global QPS limit
- Send requests to Zyte
- Save successful responses to S3
- Record failures and retries
- Resume from checkpoints
