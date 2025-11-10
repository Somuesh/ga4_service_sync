# services/queue/task_wrapper.py
import importlib
import inspect
import traceback
from datetime import datetime
from config import GA_JOBS
from db.mongo import init_mongo, get_db

init_mongo()
db = get_db()
jobs_collection = db[GA_JOBS]

# Configure the module path where your GA runner lives.
# If your GA functions live at services.ga4.runner, that's default.
GA_RUNNER_MODULE = "services.ga4.runner"

# candidate function names to try — we will find whatever exists in your module
CANDIDATE_FNAMES = ["run_ga"]

def _get_callable():
    """
    Dynamically import the GA runner module and return the first callable
    function matching a candidate name. Raises ImportError/AttributeError if none found.
    """
    mod = importlib.import_module(GA_RUNNER_MODULE)
    for name in CANDIDATE_FNAMES:
        if hasattr(mod, name) and callable(getattr(mod, name)):
            return getattr(mod, name)
    # As fallback, attempt to find any callable in module that looks like a runner (heuristic)
    for attr in dir(mod):
        obj = getattr(mod, attr)
        if callable(obj) and attr.startswith("run"):
            return obj
    raise AttributeError(f"No callable GA runner found in {GA_RUNNER_MODULE}. Checked: {CANDIDATE_FNAMES}")

def enqueueable_run(mode="combined", start_date=None, end_date=None, queue_job_id=None, job_timeout=None):
    """
    The function meant to be enqueued by RQ. This wrapper is careful:
      - looks up your real GA function dynamically
      - inspects its signature
      - passes only the params that function accepts (non-breaking)
      - attaches job metadata in result
    """
    fn = _get_callable()
    sig = inspect.signature(fn)
    call_kwargs = {}

    # Try to pass common names if accepted
    if "mode" in sig.parameters:
        call_kwargs["mode"] = mode
    if "start_date" in sig.parameters:
        call_kwargs["start_date"] = start_date
    if "end_date" in sig.parameters:
        call_kwargs["end_date"] = end_date
    if "job_id" in sig.parameters:
        call_kwargs["job_id"] = queue_job_id

    # If the function accepts *args/**kwargs, just call with these kw; otherwise safe mapping above
    try:
        result = fn(**call_kwargs) if call_kwargs else fn()
    except TypeError:
        # fallback: try calling with positional args (mode, start_date, end_date) if that might work
        try:
            result = fn(mode, start_date, end_date)
        except Exception as e:
            if queue_job_id:
                jobs_collection.update_one(
                    {"_id": queue_job_id},
                    {"$set": {
                        "status": f"Job Failed with error: {e}",
                        "completed_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow()
                    }}
                )
            # return a structured error so the job result is helpful
            return {
                "error": "failed_call",
                "exception": str(e),
                "traceback": traceback.format_exc(),
                "called_with": call_kwargs,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }

    # attach job_id and timestamp if provided
    if isinstance(result, dict):
        if queue_job_id:
            result.setdefault("queue_job_id", queue_job_id)
            jobs_collection.update_one(
                {"_id": queue_job_id},
                {"$set": {
                    "status": "Job Processed",
                    "completed_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }}
            )
        result.setdefault("finished_at", datetime.utcnow().isoformat() + "Z")
    return result
