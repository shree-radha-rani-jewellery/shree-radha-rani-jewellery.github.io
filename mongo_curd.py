from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import ObjectId


# ─── Connection ───────────────────────────────────────────────────────────────

def get_collection(uri: str = "mongodb+srv://admin:Mn9rZDhbSBk8m@cluster0.9cfopy0.mongodb.net/", db_name: str = "test_db", collection_name: str = "users"):
    """Connect to MongoDB and return the collection."""
    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]


# ─── INSERT ───────────────────────────────────────────────────────────────────

def insert_one_document(collection, document: dict):
    """Insert a single document into the collection."""
    try:
        result = collection.insert_one(document)
        print(f"[INSERT ONE] Inserted document with ID: {result.inserted_id}")
        return result.inserted_id
    except PyMongoError as e:
        print(f"[INSERT ONE] Error: {e}")
        return None


def insert_many_documents(collection, documents: list):
    """Insert multiple documents into the collection."""
    try:
        result = collection.insert_many(documents)
        print(f"[INSERT MANY] Inserted {len(result.inserted_ids)} documents: {result.inserted_ids}")
        return result.inserted_ids
    except PyMongoError as e:
        print(f"[INSERT MANY] Error: {e}")
        return []


# ─── READ ─────────────────────────────────────────────────────────────────────

def find_one_document(collection, query: dict):
    """Find a single document matching the query."""
    try:
        doc = collection.find_one(query)
        print(f"[FIND ONE] Result: {doc}")
        return doc
    except PyMongoError as e:
        print(f"[FIND ONE] Error: {e}")
        return None


def find_all_documents(collection, query: dict = {}):
    """Find all documents matching the query."""
    try:
        docs = list(collection.find(query))
        print(f"[FIND ALL] Found {len(docs)} document(s):")
        for doc in docs:
            print(f"  {doc}")
        return docs
    except PyMongoError as e:
        print(f"[FIND ALL] Error: {e}")
        return []


# ─── UPDATE ───────────────────────────────────────────────────────────────────

def update_one_document(collection, query: dict, update_fields: dict):
    """Update a single document matching the query."""
    try:
        result = collection.update_one(query, {"$set": update_fields})
        print(f"[UPDATE ONE] Matched: {result.matched_count}, Modified: {result.modified_count}")
        return result.modified_count
    except PyMongoError as e:
        print(f"[UPDATE ONE] Error: {e}")
        return 0


def update_many_documents(collection, query: dict, update_fields: dict):
    """Update all documents matching the query."""
    try:
        result = collection.update_many(query, {"$set": update_fields})
        print(f"[UPDATE MANY] Matched: {result.matched_count}, Modified: {result.modified_count}")
        return result.modified_count
    except PyMongoError as e:
        print(f"[UPDATE MANY] Error: {e}")
        return 0


# ─── DELETE ───────────────────────────────────────────────────────────────────

def delete_one_document(collection, query: dict):
    """Delete a single document matching the query."""
    try:
        result = collection.delete_one(query)
        print(f"[DELETE ONE] Deleted {result.deleted_count} document(s)")
        return result.deleted_count
    except PyMongoError as e:
        print(f"[DELETE ONE] Error: {e}")
        return 0


def delete_many_documents(collection, query: dict):
    """Delete all documents matching the query."""
    try:
        result = collection.delete_many(query)
        print(f"[DELETE MANY] Deleted {result.deleted_count} document(s)")
        return result.deleted_count
    except PyMongoError as e:
        print(f"[DELETE MANY] Error: {e}")
        return 0


# ─── MAIN (Test Runner) ───────────────────────────────────────────────────────

if __name__ == "__main__":
    collection = get_collection()

    print("\n========== INSERT ==========")
    inserted_id = insert_one_document(collection, {"name": "Alice", "age": 30, "city": "Jaipur"})

    insert_many_documents(collection, [
        {"name": "Bob",     "age": 25, "city": "Delhi"},
        {"name": "Charlie", "age": 35, "city": "Mumbai"},
        {"name": "Diana",   "age": 28, "city": "Jaipur"},
    ])

    print("\n========== READ ==========")
    find_one_document(collection, {"name": "Alice"})
    find_all_documents(collection, {"city": "Jaipur"})

    print("\n========== UPDATE ==========")
    update_one_document(collection, {"name": "Alice"}, {"age": 31, "city": "Pune"})
    update_many_documents(collection, {"city": "Jaipur"}, {"city": "Jodhpur"})

    print("\n========== READ AFTER UPDATE ==========")
    find_all_documents(collection)

    print("\n========== DELETE ==========")
    delete_one_document(collection, {"name": "Bob"})
    delete_many_documents(collection, {"city": "Jodhpur"})

    print("\n========== FINAL STATE ==========")
    find_all_documents(collection)
