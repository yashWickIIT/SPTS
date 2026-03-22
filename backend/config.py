import os
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit

from dotenv import load_dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

# Load workspace-level .env once for all backend/KG modules.
load_dotenv(ENV_PATH)


def get_env_path(env_key: str, default_relative_path: str) -> str:
    raw = os.getenv(env_key, "").strip()

    if raw:
        path = os.path.expandvars(os.path.expanduser(raw))
        if not os.path.isabs(path):
            path = os.path.abspath(os.path.join(PROJECT_ROOT, path))
        return path

    return os.path.abspath(os.path.join(PROJECT_ROOT, default_relative_path))


def get_main_database_url() -> str:
    """Return strict read-only DB URL for main query database."""
    explicit_url = os.getenv("SPTS_DATABASE_URL", "").strip()
    if explicit_url:
        return _ensure_read_only_database_url(explicit_url)

    sqlite_path = Path(
        get_env_path("SPTS_MAIN_DB_PATH", os.path.join("data", "bird_mini_dev.sqlite"))
    )
    sqlite_file_uri = f"sqlite:///file:{sqlite_path.as_posix()}?mode=ro&uri=true"
    return _ensure_read_only_database_url(sqlite_file_uri)


def _ensure_read_only_database_url(url: str) -> str:
    """Force read-only mode for SQLite URLs used by the main query engine."""
    if not url.lower().startswith("sqlite://"):
        return url

    parsed = urlsplit(url)
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_params["mode"] = "ro"
    query_params["uri"] = "true"

    if parsed.netloc:
        sqlite_target = f"{parsed.netloc}{parsed.path}"
    else:
        sqlite_target = parsed.path.lstrip("/")

    if not sqlite_target.lower().startswith("file:"):
        sqlite_target = f"file:{sqlite_target}"

    return f"sqlite:///{sqlite_target}?{urlencode(query_params)}"


# ============================================================================
# Centralized environment variable exports for all backend/KG modules
# ============================================================================
# This is the single source of truth for all env configuration in SPTS.
# All modules should import from this file instead of calling os.getenv() directly.


# API Key (required for LLM features like SQL generation and grounding)
# Checks both API_KEY and GROQ_API_KEY for backward compatibility
API_KEY = os.getenv("API_KEY") or os.getenv("GROQ_API_KEY") or ""


def get_allowed_origins() -> list[str]:
    raw = os.getenv("SPTS_ALLOWED_ORIGINS", "").strip()
    if not raw:
        return ["http://localhost", "http://127.0.0.1", "http://localhost:8000", "http://127.0.0.1:8000"]
    return [origin.strip() for origin in raw.split(",") if origin.strip()]

# JWT secret key for session tokens
SECRET_KEY = os.getenv("SECRET_KEY") or "spts-super-secret-key-12345"

# Embedding model configuration
EMBEDDING_MODEL = os.getenv("SPTS_EMBEDDING_MODEL") or "BAAI/bge-small-en-v1.5"
FALLBACK_EMBEDDING_DIM = int(os.getenv("SPTS_FALLBACK_EMBEDDING_DIM") or "384")

# Sessions directory (where user query logs are stored)
SESSIONS_DIR = os.getenv("SPTS_SESSIONS_DIR") or "/app/sessions"

# Vector database paths
CHROMA_PATH = get_env_path("SPTS_CHROMA_PATH", os.path.join("kg", "chroma_db"))
MAIN_DB_PATH = get_env_path("SPTS_MAIN_DB_PATH", os.path.join("data", "bird_mini_dev.sqlite"))
MAIN_DATABASE_URL = get_main_database_url()
ALLOWED_ORIGINS = get_allowed_origins()
MAX_REQUEST_BODY_BYTES = int(os.getenv("SPTS_MAX_REQUEST_BODY_BYTES") or "16384")
MAX_QUERY_LENGTH = int(os.getenv("SPTS_MAX_QUERY_LENGTH") or "1000")
