import sqlite3
import os

try:
    from .config import get_env_path
except ImportError:
    from config import get_env_path

# Separate DB for users to avoid messing with the main dataset db
USERS_DB_PATH = get_env_path("SPTS_USERS_DB_PATH", os.path.join("data", "users.sqlite"))

DEFAULT_ROLE = "analyst"
ALLOWED_ROLES = {"admin", "developer", "analyst", "manager", "researcher", "qa", "engineer"}


def normalize_role(role: str | None) -> str:
    normalized = (role or DEFAULT_ROLE).strip().lower()
    if normalized not in ALLOWED_ROLES:
        return DEFAULT_ROLE
    return normalized

def init_users_db():
    os.makedirs(os.path.dirname(USERS_DB_PATH), exist_ok=True)
    with sqlite3.connect(USERS_DB_PATH, timeout=10) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                hashed_password TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'analyst'
            )
        ''')

        cursor.execute("PRAGMA table_info(users)")
        columns = {row[1] for row in cursor.fetchall()}
        if "role" not in columns:
            cursor.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'analyst'")

        conn.commit()

def get_user_by_username(username: str):
    with sqlite3.connect(USERS_DB_PATH, timeout=10) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, hashed_password, role FROM users WHERE username = ?", (username,))
        row = cursor.fetchone()
    if row:
        return {"id": row[0], "username": row[1], "hashed_password": row[2], "role": normalize_role(row[3])}
    return None

def create_user(username: str, hashed_password: str, role: str | None = None):
    try:
        normalized_role = normalize_role(role)
        with sqlite3.connect(USERS_DB_PATH, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (username, hashed_password, role) VALUES (?, ?, ?)",
                (username, hashed_password, normalized_role),
            )
            conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False

init_users_db()
