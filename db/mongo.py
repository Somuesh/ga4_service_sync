from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB
from pymongo.errors import ConnectionFailure
import os

_client = None
_db = None

def init_mongo():
    global _client, _db
    if _client:
        return
    uri = MONGO_URI
    if not uri:
        raise RuntimeError('MONGO_URI not set in environment')
    _client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        # trigger connection
        _client.admin.command('ping')
    except ConnectionFailure as e:
        raise RuntimeError(f'Could not connect to MongoDB: {e}')
    _db = _client[MONGO_DB]

def get_db():
    if _db is None:
        raise RuntimeError('Mongo not initialized. Call init_mongo() first.')
    return _db
