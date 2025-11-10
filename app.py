from flask import Flask, request, jsonify
from services.ga4.runner import run_ga, get_mode_counts
from datetime import datetime
from db.mongo import init_mongo, get_db
from dotenv import load_dotenv
from services.queue.setup import ga_queue
from services.queue.task_wrapper import enqueueable_run
from uuid import uuid4
import os

from config import GA_JOBS
from services.queue.setup import redis_conn
from rq.job import Job, NoSuchJobError

import logging, sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

load_dotenv()  # loads .env if present

app = Flask(__name__)

db = get_db()
jobs_collection = db[GA_JOBS]

# Initialize Mongo (will raise if MONGO_URI not set or unreachable)
init_mongo()

@app.route('/health/', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'time': datetime.now()})

@app.route('/ga/status/<job_id>', methods=['GET'])
def ga_status(job_id):
    # Try to get the job from Redis first
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except NoSuchJobError:
        job = None
    except Exception as e:
        # Log unexpected errors for debugging
        logging.exception(f"Error fetching job {job_id} from Redis: {e}")
        job = None

    if job:
        resp = {
            "job_id": job.id,
            "status": job.get_status(),
            "enqueued_at": str(job.enqueued_at) if job.enqueued_at else None,
            "started_at": str(job.started_at) if job.started_at else None,
            "ended_at": str(job.ended_at) if job.ended_at else None,
        }

        if job.is_finished:
            resp["result"] = job.result
        elif job.is_failed:
            resp["error_info"] = str(job.exc_info)

        return jsonify(resp)

    # Fallback to MongoDB if not found in Redis
    job_db = jobs_collection.find_one({"_id": job_id})
    if not job_db:
        return jsonify({"error": "job_not_found"}), 404

    resp = {
        "job_id": job_db["_id"],
        "status": job_db.get("status"),
        "date_range": f'{job_db.get("start_date")} to {job_db.get("end_date")}',
        "enqueued_at": str(job_db.get("created_at")),
        "started_at": str(job_db.get("started_at")),
        "ended_at": str(job_db.get("completed_at")),
    }
    return jsonify(resp)

@app.route('/ga/run', methods=['POST'])
def run():
    data = request.get_json() or {}
    mode = data.get('mode', 'combined')
    start_date = data.get('start_date')
    end_date = data.get('end_date')

    job_id = str(uuid4())

    # store job metadata immediately
    jobs_collection.insert_one({
        "_id": job_id,
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
        "status": "queued",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    })

    # Enqueue the wrapper (it will dynamically call your real runner)
    ga_queue.enqueue(enqueueable_run, mode, start_date, end_date, queue_job_id=job_id, job_timeout=1000)

    # immediate response, non-blocking
    return jsonify({
        "message": "GA run enqueued",
        "job_id": job_id,
        "status": "queued"
    }), 202

@app.route('/ga/counts', methods=['GET'])
def counts():
    """Return the dimensions/metrics counts for each mode and allow triggering count retrieval programmatically."""
    mode = request.args.get('mode', 'both')
    if mode not in ('combined', 'mapped', 'both'):
        return jsonify({'error': 'mode must be combined, mapped, or both'}), 400
    return jsonify(get_mode_counts(mode))

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
