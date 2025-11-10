from config import GA_JOBS
from rq import Worker
from services.queue.setup import ga_queue, redis_conn
import logging
import sys

# Optional: configure logging so you can see this in the console or redirect to a file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

if __name__ == "__main__":
    worker = Worker([ga_queue], connection=redis_conn)
    logging.info("🚀 RQ worker started and listening for GA queue jobs...")
    worker.work(with_scheduler=True)
