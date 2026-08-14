import os
import time
import uuid
import redis


class GlobalRateLimiter:
    """
    Cluster-wide limiter.

    Every worker uses the same Redis key. This limits the complete AWS
    deployment, not each Spark executor independently.
    """

    def __init__(self, redis_url=None, qps=200, key="twitter-x:global-rate"):
        self.redis = redis.Redis.from_url(
            redis_url or os.environ["REDIS_URL"],
            decode_responses=True,
        )
        self.interval_seconds = 1.0 / float(qps)
        self.key = key

    def acquire(self):
        token = str(uuid.uuid4())

        while True:
            acquired = self.redis.set(
                self.key,
                token,
                nx=True,
                px=max(1, int(self.interval_seconds * 1000)),
            )

            if acquired:
                return

            time.sleep(min(self.interval_seconds / 4, 0.05))
