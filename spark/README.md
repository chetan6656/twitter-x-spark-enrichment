# Spark Jobs

Spark prepares exactly two Twitter/X queries per contact and later scores the cached Zyte responses.

Spark should not directly send uncontrolled requests to Zyte. All external API calls must pass through the centralized worker and global limiter.
