# Spark Jobs

Spark prepares exactly two physical Twitter/X queries per valid contact and
later scores the cached Zyte responses. `score_results.py` supports Spark
Parquet/JSON execution and a `--local` CSV mode for contract tests.

Spark should not directly send uncontrolled requests to Zyte. All external API calls must pass through the centralized worker and global limiter.

The producer journal is part of the restart contract: mark a `query_id` sent
only after SQS accepts it, and never publish it again on restart.
