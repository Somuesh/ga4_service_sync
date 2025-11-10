# services/queue/setup.py
import os
import redis
from rq import Queue

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_conn = redis.from_url(REDIS_URL)

# single queue for GA tasks
ga_queue = Queue("ga_queue", connection=redis_conn)
