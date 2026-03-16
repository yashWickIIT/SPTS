import os
from pathlib import Path

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
    """Return DB URL from env, or fall back to local sqlite prototype path."""
    explicit_url = os.getenv("SPTS_DATABASE_URL", "").strip()
    if explicit_url:
        return explicit_url

    sqlite_path = Path(
        get_env_path("SPTS_MAIN_DB_PATH", os.path.join("data", "bird_mini_dev.sqlite"))
    )
    return f"sqlite:///{sqlite_path.as_posix()}"
