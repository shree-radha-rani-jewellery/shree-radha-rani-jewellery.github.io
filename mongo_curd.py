from pymongo import MongoClient
from pymongo.errors import PyMongoError


# ─── Connection ───────────────────────────────────────────────────────────────

def get_collection(uri: str = "mongodb://localhost:27017", db_name: str = "test_db", collection_name: str = "users"):
    """Connect to MongoDB and return the collection."""
    client = MongoClient(uri)
    db = client[db_name]
    print("[MongoDB] Connected successfully.")
    return db[collection_name]


# ─── INSERT ───────────────────────────────────────────────────────────────────

def insert_one_document(collection, document: dict):
    """Insert a single document into the collection."""
    try:
        result = collection.insert_one(document)
        print(f"[MongoDB][INSERT ONE] Inserted document with ID: {result.inserted_id}")
        return result.inserted_id
    except PyMongoError as e:
        print(f"[MongoDB][INSERT ONE] Error: {e}")
        return None


def insert_many_documents(collection, documents: list):
    """Insert multiple documents into the collection."""
    try:
        result = collection.insert_many(documents)
        print(f"[MongoDB][INSERT MANY] Inserted {len(result.inserted_ids)} documents.")
        return result.inserted_ids
    except PyMongoError as e:
        print(f"[MongoDB][INSERT MANY] Error: {e}")
        return []


# ─── READ ─────────────────────────────────────────────────────────────────────

def find_one_document(collection, query: dict):
    """Find a single document matching the query."""
    try:
        doc = collection.find_one(query)
        print(f"[MongoDB][FIND ONE] Result: {doc}")
        return doc
    except PyMongoError as e:
        print(f"[MongoDB][FIND ONE] Error: {e}")
        return None


def find_all_documents(collection, query: dict = {}):
    """Find all documents matching the query."""
    try:
        docs = list(collection.find(query))
        print(f"[MongoDB][FIND ALL] Found {len(docs)} document(s):")
        for doc in docs:
            print(f"  {doc}")
        return docs
    except PyMongoError as e:
        print(f"[MongoDB][FIND ALL] Error: {e}")
        return []


# ─── UPDATE ───────────────────────────────────────────────────────────────────

def update_one_document(collection, query: dict, update_fields: dict):
    """Update a single document matching the query."""
    try:
        result = collection.update_one(query, {"$set": update_fields})
        print(f"[MongoDB][UPDATE ONE] Matched: {result.matched_count}, Modified: {result.modified_count}")
        return result.modified_count
    except PyMongoError as e:
        print(f"[MongoDB][UPDATE ONE] Error: {e}")
        return 0


def update_many_documents(collection, query: dict, update_fields: dict):
    """Update all documents matching the query."""
    try:
        result = collection.update_many(query, {"$set": update_fields})
        print(f"[MongoDB][UPDATE MANY] Matched: {result.matched_count}, Modified: {result.modified_count}")
        return result.modified_count
    except PyMongoError as e:
        print(f"[MongoDB][UPDATE MANY] Error: {e}")
        return 0


# ─── DELETE ───────────────────────────────────────────────────────────────────

def delete_one_document(collection, query: dict):
    """Delete a single document matching the query."""
    try:
        result = collection.delete_one(query)
        print(f"[MongoDB][DELETE ONE] Deleted {result.deleted_count} document(s).")
        return result.deleted_count
    except PyMongoError as e:
        print(f"[MongoDB][DELETE ONE] Error: {e}")
        return 0


def delete_many_documents(collection, query: dict):
    """Delete all documents matching the query."""
    try:
        result = collection.delete_many(query)
        print(f"[MongoDB][DELETE MANY] Deleted {result.deleted_count} document(s).")
        return result.deleted_count
    except PyMongoError as e:
        print(f"[MongoDB][DELETE MANY] Error: {e}")
        return 0
