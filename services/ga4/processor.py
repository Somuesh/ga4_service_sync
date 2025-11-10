# processor: converts GA run_report response into list of dict rows
try:
    from google.analytics.data_v1beta.types import RunReportResponse, Row
    _HAS_GA = True
except Exception:
    _HAS_GA = False

def process_response(response):
    """Convert a RunReportResponse (real GA client) into list of dict rows.
    If a simulated object (list) is passed, it's returned unchanged.
    """
    # If response already a list (simulation), return directly
    if isinstance(response, list):
        return response
    if response is None:
        return []
    rows = []
    dh = [h.name for h in response.dimension_headers]
    mh = [h.name for h in response.metric_headers]
    for r in response.rows:
        row = {}
        for i, dv in enumerate(r.dimension_values):
            row[dh[i]] = dv.value
        for j, mv in enumerate(r.metric_values):
            try:
                row[mh[j]] = float(mv.value)
            except:
                row[mh[j]] = mv.value
        rows.append(row)
    return rows
