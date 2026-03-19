# ─── Imports ──────────────────────────────────────────────────────────────────

import mongo_db   as mongo
import mysql_db   as mysql


# ══════════════════════════════════════════════════════════════════════════════
#  MONGODB TEST
# ══════════════════════════════════════════════════════════════════════════════

def run_mongo():
    print("\n" + "═" * 50)
    print("         MONGODB OPERATIONS")
    print("═" * 50)

    collection = mongo.get_collection(
        uri             = "mongodb://localhost:27017",
        db_name         = "test_db",
        collection_name = "users"
    )

    print("\n── INSERT ──────────────────────────────────────")
    alice_id = mongo.insert_one_document(collection, {"name": "Alice", "age": 30, "city": "Jaipur"})
    mongo.insert_many_documents(collection, [
        {"name": "Bob",     "age": 25, "city": "Delhi"},
        {"name": "Charlie", "age": 35, "city": "Mumbai"},
        {"name": "Diana",   "age": 28, "city": "Jaipur"},
    ])

    print("\n── READ ────────────────────────────────────────")
    mongo.find_one_document(collection, {"name": "Alice"})
    mongo.find_all_documents(collection, {"city": "Jaipur"})

    print("\n── UPDATE ──────────────────────────────────────")
    mongo.update_one_document(collection, {"name": "Alice"}, {"age": 31, "city": "Pune"})
    mongo.update_many_documents(collection, {"city": "Jaipur"}, {"city": "Jodhpur"})

    print("\n── READ AFTER UPDATE ───────────────────────────")
    mongo.find_all_documents(collection)

    print("\n── DELETE ──────────────────────────────────────")
    mongo.delete_one_document(collection, {"name": "Bob"})
    mongo.delete_many_documents(collection, {"city": "Jodhpur"})

    print("\n── FINAL STATE ─────────────────────────────────")
    mongo.find_all_documents(collection)


# ══════════════════════════════════════════════════════════════════════════════
#  MYSQL TEST
# ══════════════════════════════════════════════════════════════════════════════

def run_mysql():
    print("\n" + "═" * 50)
    print("         MYSQL OPERATIONS")
    print("═" * 50)

    conn = mysql.get_connection(
        host     = "localhost",
        user     = "root",
        password = "yourpassword",   # ← update this
        database = "test_db"
    )

    if not conn:
        print("[MYSQL] Skipping — could not connect.")
        return

    print("\n── SETUP ───────────────────────────────────────")
    mysql.create_table(conn)

    print("\n── INSERT ──────────────────────────────────────")
    alice_id = mysql.insert_one(conn, "Alice", 30, "Jaipur")
    mysql.insert_many(conn, [
        ("Bob",     25, "Delhi"),
        ("Charlie", 35, "Mumbai"),
        ("Diana",   28, "Jaipur"),
    ])

    print("\n── READ ────────────────────────────────────────")
    mysql.find_by_id(conn, alice_id)
    mysql.find_all(conn, city="Jaipur")

    print("\n── UPDATE ──────────────────────────────────────")
    mysql.update_by_id(conn, alice_id, age=31, city="Pune")
    mysql.update_city_by_name(conn, "Diana", "Jodhpur")

    print("\n── READ AFTER UPDATE ───────────────────────────")
    mysql.find_all(conn)

    print("\n── DELETE ──────────────────────────────────────")
    mysql.delete_by_id(conn, alice_id)
    mysql.delete_by_city(conn, "Jodhpur")

    print("\n── FINAL STATE ─────────────────────────────────")
    mysql.find_all(conn)

    conn.close()
    print("\n[MySQL] Connection closed.")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_mongo()
    run_mysql()
