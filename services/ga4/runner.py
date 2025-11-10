import os
from config import COMBINED_DIMENSIONS, COMBINED_METRICS, DIMENSION_METRIC_MAP, CLIENT_SECRETS_FILE, GA_JOBS
from services.ga4.processor import process_response
from services.ga4.loader import save_rows_to_collection
from datetime import datetime, timedelta
import uuid

# Try to import GA client; if not available we'll simulate
try:
    from google.oauth2 import service_account
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
    _HAS_GA = True
except Exception:
    _HAS_GA = False

from db.mongo import init_mongo, get_db
init_mongo()
db = get_db()

def get_ga4_client():
    creds = service_account.Credentials.from_service_account_file(CLIENT_SECRETS_FILE)
    client = BetaAnalyticsDataClient(credentials=creds)
    return client

def get_mode_counts(mode='both'):
    if mode == 'combined':
        return {'dimensions': len(COMBINED_DIMENSIONS), 'metrics': len(COMBINED_METRICS)}
    if mode == 'mapped':
        unique_metrics = set()
        for metrics in DIMENSION_METRIC_MAP.values():
            unique_metrics.update(metrics)
        return {'dimension_groups': len(DIMENSION_METRIC_MAP), 'unique_metrics': len(unique_metrics)}
    return {
        'combined': {'dimensions': len(COMBINED_DIMENSIONS), 'metrics': len(COMBINED_METRICS)},
        'mapped': {'dimension_groups': len(DIMENSION_METRIC_MAP), 'unique_metrics': len({m for metrics in DIMENSION_METRIC_MAP.values() for m in metrics})}
    }


def _build_run_report_request(property_id, dimensions, metrics, start_date='7daysAgo', end_date='today'):
    # Build request for GA4 Data API with explicit date range
    dims = [Dimension(name=d) for d in dimensions]
    mets = [Metric(name=m) for m in metrics]
    date_ranges = [DateRange(start_date=start_date, end_date=end_date)]
    return RunReportRequest(property=f"properties/{property_id}", dimensions=dims, metrics=mets, date_ranges=date_ranges)


def _run_real_report(property_id, dimensions, metrics, start_date='7daysAgo', end_date='today'):
    """
    Real GA4 call. Caller can pass start_date and end_date strings (YYYY-MM-DD or relative like '7daysAgo').
    """
    if not _HAS_GA:
        raise RuntimeError('google-analytics-data library not available')
    client = get_ga4_client()
    req = _build_run_report_request(property_id, dimensions, metrics, start_date=start_date, end_date=end_date)
    resp = client.run_report(req)
    return resp


def _simulate_report(dimensions, metrics, date_str=None, rows=5):
    """
    Produce simulated rows matching requested dimensions/metrics.
    date_str: if provided, it's injected into generated values to make them unique per-date.
    """
    out = []
    for i in range(rows):
        row = {}
        for d in dimensions:
            suffix = f"{date_str.replace('-', '')}_{i}" if date_str else str(i)
            row[d] = f"{d}_val_{suffix}"
        for m in metrics:
            # numeric dummy values
            row[m] = i * 10 + len(m)
        # add a synthetic id and created_at as before
        row['id'] = str(uuid.uuid4())
        row['created_at'] = datetime.utcnow().isoformat() + 'Z'
        if date_str:
            row['date'] = date_str
        out.append(row)
    return out


def run_ga(mode='combined', start_date=None, end_date=None, job_id=None):
    """
    Backwards-compatible entrypoint.

    Signature:
        run_ga(mode='combined', start_date=None, end_date=None)

    - If called with only mode (or no args), behaves exactly as before.
    - If mode == 'combined' and start_date/end_date are provided (YYYY-MM-DD),
      it will iterate each date in the inclusive range, call GA for that date
      (start_date=end_date=current_date) and save the 'date' key into each row.
    - If GA client or GA4 property is not configured, simulation is used (same as before).
    """
    property_id = os.getenv('GA4_PROPERTY_ID')
    results = {}

    jobs_collection = db[GA_JOBS]

    if job_id:
        jobs_collection.update_one(
            {"_id": job_id},
            {"$set": {"status": "in_progress", "started_at": datetime.now()}}
        )

    # Combined mode supports optional date-range per-day iteration
    if mode == 'combined':
        dims = COMBINED_DIMENSIONS
        mets = COMBINED_METRICS

        # If date range provided, validate and iterate per date
        if start_date and end_date:
            try:
                start = datetime.strptime(start_date, "%Y-%m-%d")
                end = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError as ve:
                raise ValueError("start_date and end_date must be in YYYY-MM-DD format") from ve

            if start > end:
                raise ValueError("start_date must be <= end_date")

            all_rows = []
            per_date_results = []
            current = start
            while current <= end:
                date_str = current.strftime("%Y-%m-%d")
                try:
                    # request filtered to the specific date (start==end)
                    if property_id and _HAS_GA:
                        resp = _run_real_report(property_id, dims, mets, start_date=date_str, end_date=date_str)
                        rows = process_response(resp)
                    else:
                        rows = _simulate_report(dims, mets, date_str=date_str)
                except Exception as e:
                    # On any exception fallback to simulation for that date and include warning
                    rows = _simulate_report(dims, mets, date_str=date_str)
                    results.setdefault('warnings', []).append(f'{date_str}: real GA call failed: {str(e)} - simulation used.')

                # Add the date field to every row (important: we don't add 'date' as GA dimension)
                for r in rows:
                    r['date'] = date_str

                # Save rows for this date
                inserted = save_rows_to_collection('combined_dimensions', rows)
                per_date_results.append({'date': date_str, 'inserted': inserted, 'rows_count': len(rows)})
                all_rows.extend(rows)
                current += timedelta(days=1)

            results['per_date'] = per_date_results
            results['inserted'] = {'inserted': len(all_rows), 'modified': 0}
            results['rows_sample'] = all_rows[:2]
            results['counts'] = get_mode_counts('combined')
            return results

        # If no date range was provided, keep old behavior (single run)
        try:
            if property_id and _HAS_GA:
                resp = _run_real_report(property_id, dims, mets)
                rows = process_response(resp)
            else:
                rows = _simulate_report(dims, mets)
        except Exception as e:
            rows = _simulate_report(dims, mets)
            results['warning'] = f'Real GA call failed: {e} - simulation used.'

        inserted = save_rows_to_collection('combined_dimensions', rows)
        results['inserted'] = inserted
        results['rows_sample'] = rows[:2]
        results['counts'] = get_mode_counts('combined')
        return results

    # Mapped mode: unchanged behavior (no date iteration)
    if mode == 'mapped':
        all_inserted = {}
        for dim, mets in DIMENSION_METRIC_MAP.items():
            try:
                if property_id and _HAS_GA:
                    resp = _run_real_report(property_id, [dim], mets)
                    rrows = process_response(resp)
                else:
                    rrows = _simulate_report([dim], mets)
            except Exception as e:
                rrows = _simulate_report([dim], mets)
                results.setdefault('warnings', []).append(f'{dim}: real GA call failed: {e} - simulation used.')
            colname = f'ga_{dim}'
            inserted = save_rows_to_collection(colname, rrows)
            all_inserted[dim] = {'collection': colname, 'inserted': inserted, 'sample': rrows[:1]}
        results['mapped'] = all_inserted
        results['counts'] = get_mode_counts('mapped')
        return results

    raise ValueError('mode must be combined or mapped')
