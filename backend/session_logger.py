"""
session_logger.py
-----------------
Appends each query + results to /app/sessions/session_<username>.json.
After writing, makes the file read-only for the owner.
so testers cannot accidentally edit the log before sharing it back.
"""

import json
import os
import stat
from datetime import datetime, timezone

try:
    from .config import SESSIONS_DIR
except ImportError:
    from config import SESSIONS_DIR


def get_session_file_path(username: str) -> str:
    safe_username = (username or "").strip()
    return os.path.join(SESSIONS_DIR, f"session_{safe_username}.json")


def log_query(username: str, role: str, query: str, payload: dict) -> int:
    """
    Append one structured entry to the user's session file.

    Parameters
    ----------
    username : str
        The logged-in tester's username.
    role     : str
        The logged-in tester's role.
    query    : str
        The natural-language question the user sent.
    payload  : dict
        The full /query response dict (baseline_sql, spts_sql, results …).

    Returns the 0-based index of the newly appended entry.
    """
    os.makedirs(SESSIONS_DIR, exist_ok=True)

    session_file = get_session_file_path(username)

    # ── Load existing entries (or start fresh) ──────────────
    # The file might be read-only from a previous write; temporarily
    # make it writable so we can append.
    if os.path.exists(session_file):
        _make_writable(session_file)
        with open(session_file, "r", encoding="utf-8") as f:
            try:
                entries = json.load(f)
            except json.JSONDecodeError:
                entries = []
    else:
        entries = []

    # ── Build the new entry ──────────────────────────────────
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "username": username,
        "role": (role or "analyst").strip().lower(),
        "query": query,
        "baseline_sql": payload.get("baseline_sql"),
        "spts_sql": payload.get("spts_sql"),
        "baseline_result": _truncate(payload.get("baseline_result")),
        "spts_result": _truncate(payload.get("spts_result")),
        "mappings": payload.get("mappings"),
        "baseline_rationale": payload.get("baseline_rationale"),
        "spts_rationale": payload.get("spts_rationale"),
    }
    entries.append(entry)

    # ── Write back + lock as read-only ───────────────────────
    with open(session_file, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2, default=str)

    _make_readonly(session_file)
    print(f"[session_logger] Logged query #{len(entries)} for '{username}' → {session_file}")
    return len(entries) - 1


def update_feedback(
    username: str,
    query_index: int,
    baseline_rating: str | None,
    spts_rating: str | None,
) -> bool:
    """
    Adds/overwrites rating fields on an existing session entry.
    Returns True on success, False if the file or index is invalid.
    """
    session_file = get_session_file_path(username)
    if not os.path.exists(session_file):
        return False

    _make_writable(session_file)
    with open(session_file, "r", encoding="utf-8") as f:
        try:
            entries = json.load(f)
        except json.JSONDecodeError:
            _make_readonly(session_file)
            return False

    if not (0 <= query_index < len(entries)):
        _make_readonly(session_file)
        return False

    if baseline_rating is not None:
        entries[query_index]["baseline_rating"] = baseline_rating
    if spts_rating is not None:
        entries[query_index]["spts_rating"] = spts_rating

    with open(session_file, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2, default=str)
    _make_readonly(session_file)
    return True

# ── Helpers ──────────────────────────────────────────────────────────────────

def _truncate(result, max_rows: int = 200):
    """Keep at most max_rows rows to avoid huge files."""
    if isinstance(result, list) and len(result) > max_rows:
        return result[:max_rows]
    return result


def _make_readonly(path: str) -> None:
    try:
        if os.name == "nt":
            os.chmod(path, stat.S_IREAD)
        else:
            os.chmod(path, stat.S_IRUSR)
    except OSError:
        pass  # Windows inside WSL may not support all permission bits


def _make_writable(path: str) -> None:
    try:
        if os.name == "nt":
            os.chmod(path, stat.S_IREAD | stat.S_IWRITE)
        else:
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
