import mysql.connector
from mysql.connector import Error


# ─── Connection ───────────────────────────────────────────────────────────────

def get_connection(host: str = "localhost", user: str = "root", password: str = "", database: str = "test_db"):
    """Create and return a MySQL connection."""
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if conn.is_connected():
            print("[MySQL] Connected successfully.")
        return conn
    except Error as e:
        print(f"[MySQL][CONNECTION] Error: {e}")
        return None


# ─── SETUP ────────────────────────────────────────────────────────────────────

def create_table(conn):
    """Create the users table if it doesn't already exist."""
    query = """
        CREATE TABLE IF NOT EXISTS users (
            id   INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age  INT          NOT NULL,
            city VARCHAR(100) NOT NULL
        )
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        print("[MySQL][SETUP] Table 'users' is ready.")
    except Error as e:
        print(f"[MySQL][SETUP] Error: {e}")
    finally:
        cursor.close()


# ─── INSERT ───────────────────────────────────────────────────────────────────

def insert_one(conn, name: str, age: int, city: str):
    """Insert a single row into the users table."""
    query = "INSERT INTO users (name, age, city) VALUES (%s, %s, %s)"
    try:
        cursor = conn.cursor()
        cursor.execute(query, (name, age, city))
        conn.commit()
        print(f"[MySQL][INSERT ONE] Inserted row with ID: {cursor.lastrowid}")
        return cursor.lastrowid
    except Error as e:
        print(f"[MySQL][INSERT ONE] Error: {e}")
        return None
    finally:
        cursor.close()


def insert_many(conn, records: list):
    """Insert multiple rows into the users table.
    records: list of (name, age, city) tuples
    """
    query = "INSERT INTO users (name, age, city) VALUES (%s, %s, %s)"
    try:
        cursor = conn.cursor()
        cursor.executemany(query, records)
        conn.commit()
        print(f"[MySQL][INSERT MANY] Inserted {cursor.rowcount} row(s).")
        return cursor.rowcount
    except Error as e:
        print(f"[MySQL][INSERT MANY] Error: {e}")
        return 0
    finally:
        cursor.close()


# ─── READ ─────────────────────────────────────────────────────────────────────

def find_by_id(conn, user_id: int):
    """Fetch a single row by primary key."""
    query = "SELECT * FROM users WHERE id = %s"
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, (user_id,))
        row = cursor.fetchone()
        print(f"[MySQL][FIND BY ID] Result: {row}")
        return row
    except Error as e:
        print(f"[MySQL][FIND BY ID] Error: {e}")
        return None
    finally:
        cursor.close()


def find_all(conn, city: str = None):
    """Fetch all rows, optionally filtered by city."""
    if city:
        query, params = "SELECT * FROM users WHERE city = %s", (city,)
    else:
        query, params = "SELECT * FROM users", ()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        print(f"[MySQL][FIND ALL] Found {len(rows)} row(s):")
        for row in rows:
            print(f"  {row}")
        return rows
    except Error as e:
        print(f"[MySQL][FIND ALL] Error: {e}")
        return []
    finally:
        cursor.close()


# ─── UPDATE ───────────────────────────────────────────────────────────────────

def update_by_id(conn, user_id: int, name: str = None, age: int = None, city: str = None):
    """Update a row by primary key. Only provided fields are changed."""
    fields, params = [], []
    if name is not None: fields.append("name = %s"); params.append(name)
    if age  is not None: fields.append("age = %s");  params.append(age)
    if city is not None: fields.append("city = %s"); params.append(city)

    if not fields:
        print("[MySQL][UPDATE BY ID] Nothing to update.")
        return 0

    query = f"UPDATE users SET {', '.join(fields)} WHERE id = %s"
    params.append(user_id)
    try:
        cursor = conn.cursor()
        cursor.execute(query, tuple(params))
        conn.commit()
        print(f"[MySQL][UPDATE BY ID] Modified {cursor.rowcount} row(s).")
        return cursor.rowcount
    except Error as e:
        print(f"[MySQL][UPDATE BY ID] Error: {e}")
        return 0
    finally:
        cursor.close()


def update_city_by_name(conn, name: str, new_city: str):
    """Update city for all users with the given name."""
    query = "UPDATE users SET city = %s WHERE name = %s"
    try:
        cursor = conn.cursor()
        cursor.execute(query, (new_city, name))
        conn.commit()
        print(f"[MySQL][UPDATE BY NAME] Modified {cursor.rowcount} row(s).")
        return cursor.rowcount
    except Error as e:
        print(f"[MySQL][UPDATE BY NAME] Error: {e}")
        return 0
    finally:
        cursor.close()


# ─── DELETE ───────────────────────────────────────────────────────────────────

def delete_by_id(conn, user_id: int):
    """Delete a single row by primary key."""
    query = "DELETE FROM users WHERE id = %s"
    try:
        cursor = conn.cursor()
        cursor.execute(query, (user_id,))
        conn.commit()
        print(f"[MySQL][DELETE BY ID] Deleted {cursor.rowcount} row(s).")
        return cursor.rowcount
    except Error as e:
        print(f"[MySQL][DELETE BY ID] Error: {e}")
        return 0
    finally:
        cursor.close()


def delete_by_city(conn, city: str):
    """Delete all rows where city matches."""
    query = "DELETE FROM users WHERE city = %s"
    try:
        cursor = conn.cursor()
        cursor.execute(query, (city,))
        conn.commit()
        print(f"[MySQL][DELETE BY CITY] Deleted {cursor.rowcount} row(s).")
        return cursor.rowcount
    except Error as e:
        print(f"[MySQL][DELETE BY CITY] Error: {e}")
        return 0
    finally:
        cursor.close()


def delete_all(conn):
    """Truncate the users table (remove all rows)."""
    try:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE users")
        conn.commit()
        print("[MySQL][DELETE ALL] Table truncated.")
    except Error as e:
        print(f"[MySQL][DELETE ALL] Error: {e}")
    finally:
        cursor.close()
