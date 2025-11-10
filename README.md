# ga4_service_sync
Fetch and sync data from GA4 to our MongoDB

# GA4 Service (extracted)

This is a standalone Flask application that contains **only the GA4-related functionality** extracted from the original repository. It supports two run modes:

- **combined**: the combined report that uses 4 dimensions and 6 metrics.
- **mapped**: the mapped reports (dimension groups) that include 6 dimension keys and 20 unique metrics.

Endpoints:
- `GET /health` - health check
- `POST /ga/run` - run reports. JSON body:{"mode": "combined|mapped|both"}
- `GET /ga/counts` - returns counts of dimensions and metrics for modes.

Notes:
- The app will attempt to use the Google Analytics Data API if `CLIENT_SECRETS_FILE` and `GA4_PROPERTY_ID` are set. If missing or unavailable, the app will run a safe simulated response so you can validate DB writes.
- This app writes to MongoDB using `pymongo`.


Runnig the server:
- start the server using `gunivorn app:app` on which ever port you want 
- start redis and configure port and update the value in .env `REDIS_URL=redis://localhost:6379/0`
- then start worker.py to enable queuing of jobs `python3 worker.py`

