from db.mongo import get_db
from pymongo import UpdateOne
from datetime import datetime
import uuid

def _build_filter_for_row(collection_name, row, unique_keys_map):
    if collection_name not in unique_keys_map:
        return None
    keys = unique_keys_map[collection_name]
    f = {}
    for k in keys:
        if k in row:
            f[k] = row[k]
        else:
            return None
    return f if f else None

def save_rows_to_collection(collection_name, rows):
    db = get_db()
    db['combined_dimensions'].create_index('date')
    col = db[collection_name]
    ops = []
    for r in rows:
        r = dict(r)
        r['updated_at'] = datetime.utcnow()
        filt = _build_filter_for_row(collection_name, r, {'combined_dimensions':['id']})
        if filt:
            ops.append(UpdateOne(filt, {'$set': r}, upsert=True))
        else:
            if '_id' not in r:
                r['_id'] = str(uuid.uuid4())
            ops.append(UpdateOne({'_id': r['_id']}, {'$setOnInsert': r}, upsert=True))
    if not ops:
        return {'inserted': 0, 'modified': 0}
    res = col.bulk_write(ops, ordered=False)
    return {'inserted': res.upserted_count, 'modified': res.modified_count}
