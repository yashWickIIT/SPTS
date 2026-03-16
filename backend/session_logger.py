"""
session_logger.py
-----------------
Appends each query + results to /app/sessions/session_<username>.json.
After writing, makes the file read-only inside the container (chmod 444)
so testers cannot accidentally edit the log before sharing it back.
"""

import json
import os
import stat
from datetime import datetime, timezone

SESSIONS_DIR = os.environ.get("SPTS_SESSIONS_DIR", "/app/sessions")


def log_query(username: str, query: str, payload: dict) -> None:
    """
    Append one structured entry to the user's session file.

    Parameters
    ----------
    username : str
        The logged-in tester's username.
    query    : str
        The natural-language question the user sent.
    payload  : dict
        The full /query response dict (baseline_sql, spts_sql, results …).
    """
    os.makedirs(SESSIONS_DIR, exist_ok=True)

    session_file = os.path.join(SESSIONS_DIR, f"session_{username}.json")

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


# ── Helpers ──────────────────────────────────────────────────────────────────

def _truncate(result, max_rows: int = 200):
    """Keep at most max_rows rows to avoid huge files."""
    if isinstance(result, list) and len(result) > max_rows:
        return result[:max_rows]
    return result


def _make_readonly(path: str) -> None:
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)  # 444
    except OSError:
        pass  # Windows inside WSL may not support all permission bits


def _make_writable(path: str) -> None:
    try:
        current = stat.S_IMODE(os.lstat(path).st_mode)
        os.chmod(path, current | stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
