#!/usr/bin/env python3
"""
Ensure MongoDB indexes for the UFRO Orchestrator project.

Usage:
  python db/ensure_indexes.py --mongo-uri mongodb://localhost:27017 --db ufro_master --ttl-days 30

This will create the recommended indexes and (optionally) a TTL index on
`input.image_hash_ts` which should be set to a datetime when storing image hashes.
"""

import argparse
import os
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure


def verify_connection(client, db_name):
    """Verify MongoDB connection and database accessibility."""
    try:
        # Test connection
        client.admin.command("ping")
        # Verify database exists or can be created
        db = client[db_name]
        # List collections to verify access
        db.list_collection_names()
        return True
    except ServerSelectionTimeoutError:
        print(f"ERROR: Cannot connect to MongoDB at {client.address}. Server is not responding.")
        return False
    except ConnectionFailure as e:
        print(f"ERROR: Connection failure: {e}")
        return False
    except Exception as e:
        print(f"WARNING: Could not verify database access: {e}")
        # Continue anyway, might be a new database
        return True


def create_index_safe(collection, index_spec, name=None, **kwargs):
    """Safely create an index, handling existing indexes gracefully."""
    try:
        result = collection.create_index(index_spec, name=name, **kwargs)
        if isinstance(result, str):
            print(f"  ✓ Created index '{name or result}'")
        else:
            print(f"  ✓ Index '{name or 'unnamed'}' ensured")
        return True
    except Exception as e:
        print(f"  ✗ Failed to create index '{name}': {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Create MongoDB indexes for UFRO Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
                        help="MongoDB connection URI")
    parser.add_argument("--db", default=os.environ.get("MONGO_DB", "ufro_master"),
                        help="Database name")
    parser.add_argument("--ttl-days", type=int, default=None,
                        help="If provided, create a TTL index for image hashes that expires after given days")
    parser.add_argument("--skip-verify", action="store_true",
                        help="Skip connection verification (not recommended)")
    args = parser.parse_args()

    print(f"Connecting to MongoDB: {args.mongo_uri}")
    print(f"Database: {args.db}")
    print("-" * 60)

    try:
        client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    except Exception as e:
        print(f"ERROR: Failed to create MongoDB client: {e}")
        sys.exit(1)

    # Verify connection
    if not args.skip_verify:
        if not verify_connection(client, args.db):
            print("\nConnection verification failed. Use --skip-verify to bypass (not recommended).")
            sys.exit(1)
        print("✓ Connection verified\n")

    db = client[args.db]
    success_count = 0
    total_count = 0

    # access_logs indexes
    print("Creating access_logs indexes...")
    indexes_access = [
        ([("ts", DESCENDING)], "idx_access_ts", {}),
        ([("user.type", ASCENDING), ("ts", DESCENDING)], "idx_access_user_type_ts", {}),
        ([("route", ASCENDING), ("ts", DESCENDING)], "idx_access_route_ts", {}),
        ([("decision", ASCENDING), ("ts", DESCENDING)], "idx_access_decision_ts", {}),
    ]
    
    for index_spec, name, kwargs in indexes_access:
        total_count += 1
        if create_index_safe(db.access_logs, index_spec, name=name, **kwargs):
            success_count += 1

    # service_logs indexes
    print("\nCreating service_logs indexes...")
    indexes_service = [
        ([("service_name", ASCENDING), ("ts", DESCENDING)], "idx_service_name_ts", {}),
        ([("service_type", ASCENDING), ("ts", DESCENDING)], "idx_service_type_ts", {}),
        ([("status_code", ASCENDING), ("ts", DESCENDING)], "idx_service_status_ts", {}),
    ]
    
    for index_spec, name, kwargs in indexes_service:
        total_count += 1
        if create_index_safe(db.service_logs, index_spec, name=name, **kwargs):
            success_count += 1

    # Optional TTL for image hashes. TTL index must be on a date field.
    if args.ttl_days is not None:
        seconds = args.ttl_days * 24 * 3600
        print(f"\nCreating TTL index on access_logs.input.image_hash_ts (expires after {args.ttl_days} days)...")
        total_count += 1
        if create_index_safe(
            db.access_logs,
            [("input.image_hash_ts", ASCENDING)],
            name="ttl_image_hash",
            expireAfterSeconds=seconds
        ):
            success_count += 1

    # Summary
    print("\n" + "-" * 60)
    print(f"Summary: {success_count}/{total_count} indexes created/verified successfully")
    
    if success_count == total_count:
        print("✓ All indexes ensured successfully!")
        return 0
    else:
        print(f"⚠ Warning: {total_count - success_count} index(es) failed to create")
        return 1


if __name__ == "__main__":
    sys.exit(main())
