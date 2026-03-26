"""
seed_test_user.py
-----------------
Run ONCE at Docker build time (inside the container) to create a fresh
users.sqlite and insert the default tester account.

The password is hashed with bcrypt so the real hash sits in the DB,
not the image layers.
"""

import os
import sqlite3
import bcrypt

USERS_DB_PATH = os.path.join(os.path.dirname(__file__), "data", "users.sqlite")

TESTER_USERNAME = "tester"
TESTER_PASSWORD = "spts-test"
TESTER_ROLE = "researcher"  # researcher can run queries; cannot self-register admin

os.makedirs(os.path.dirname(USERS_DB_PATH), exist_ok=True)

conn = sqlite3.connect(USERS_DB_PATH)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        hashed_password TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'analyst'
    )
""")

hashed = bcrypt.hashpw(TESTER_PASSWORD.encode("utf-8"), bcrypt.gensalt()).decode(
    "utf-8"
)

try:
    cursor.execute(
        "INSERT INTO users (username, hashed_password, role) VALUES (?, ?, ?)",
        (TESTER_USERNAME, hashed, TESTER_ROLE),
    )
    conn.commit()
    print(f"[seed] Created tester account: '{TESTER_USERNAME}' (role: {TESTER_ROLE})")
except sqlite3.IntegrityError:
    print(f"[seed] Tester account '{TESTER_USERNAME}' already exists – skipping.")

conn.close()
