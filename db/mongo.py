import os
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ServerSelectionTimeoutError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("MONGO_DB", "ufro_master")

_client = None

def get_client():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return _client

def get_db():
    return get_client()[DB_NAME]

def is_healthy():
    try:
        get_client().admin.command("ping")
        return True
    except ServerSelectionTimeoutError:
        return False

def ensure_indexes():
    """Create MongoDB indexes. Uses same index names as ensure_indexes.py script for consistency."""
    db = get_db()
    try:
        # access_logs indexes
        db.access_logs.create_index([("ts", DESCENDING)], name="idx_access_ts")
        db.access_logs.create_index([("user.type", ASCENDING), ("ts", DESCENDING)], name="idx_access_user_type_ts")
        db.access_logs.create_index([("route", ASCENDING), ("ts", DESCENDING)], name="idx_access_route_ts")
        db.access_logs.create_index([("decision", ASCENDING), ("ts", DESCENDING)], name="idx_access_decision_ts")
        # service_logs indexes
        db.service_logs.create_index([("service_name", ASCENDING), ("ts", DESCENDING)], name="idx_service_name_ts")
        db.service_logs.create_index([("service_type", ASCENDING), ("ts", DESCENDING)], name="idx_service_type_ts")
        db.service_logs.create_index([("status_code", ASCENDING), ("ts", DESCENDING)], name="idx_service_status_ts")
    except Exception:
        # Silently fail to avoid breaking startup if MongoDB is temporarily unavailable
        # The script ensure_indexes.py should be run separately for proper setup
        pass
