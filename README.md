# SPTS: Semantic Profiler for Text-to-SQL

Research prototype for value-level grounding in Text-to-SQL workflows.

## Overview

SPTS is a FastAPI-based system that compares a baseline Text-to-SQL path with a graph-enhanced path.
It builds and uses a Value-Level Knowledge Graph (VLKG) to resolve noisy or abbreviated query entities into canonical database values before SQL generation.

## Current Project Structure

```text
SPTS/
├── backend/
│   ├── app.py
│   ├── auth.py
│   ├── config.py
│   ├── database.py
│   ├── db_client.py
│   ├── db_users.py
│   ├── embedding_util.py
│   ├── grounding.py
│   ├── sanitizer.py
│   ├── session_logger.py
│   └── text_to_sql.py
├── frontend/
│   ├── index.html
│   ├── app.js
│   ├── admin.html
│   ├── login.html
│   ├── register.html
│   └── S.png
├── kg/
│   ├── __init__.py
│   ├── build_vlkg.py
│   ├── update_vlkg.py
│   └── chroma_db/
├── data/
│   ├── bird_mini_dev.sqlite
│   └── users.sqlite
├── sessions/
├── Dockerfile
├── Dockerfile.test
├── docker-compose.yml
├── docker-compose.test.yml
├── evaluate.py
├── metrics_calculator.py
├── requirements.txt
├── requirements.docker.txt
├── README.md
└── TESTERS_README.md
```

## Environment Configuration

SPTS uses a single workspace-level `.env` file loaded by `backend/config.py`.

### Required

```env
API_KEY=your_groq_api_key_here
```

### Common Optional Settings

```env
SPTS_MAIN_DB_PATH=data/bird_mini_dev.sqlite
SPTS_CHROMA_PATH=kg/chroma_db
SPTS_SESSIONS_DIR=/app/sessions
SPTS_USERS_DB_PATH=/app/sessions/users.sqlite
SECRET_KEY=spts-super-secret-key-12345
```

### Database URL Precedence

1. If `SPTS_DATABASE_URL` is set, it is used.
2. Otherwise, `SPTS_MAIN_DB_PATH` is used.

For SQLite, the main query database is enforced to read-only mode (`mode=ro`).

## Run Locally

From the repository root:

```bash
uvicorn backend.app:app --reload
```

Open `http://localhost:8000`.

## Run with Docker (Developer)

```bash
docker compose -f docker-compose.yml up --build
```

This profile mounts local `./data` and `./sessions` into the container.

## Run with Docker (Tester)

```bash
docker compose -f docker-compose.test.yml up
```

Tester-specific setup and usage instructions are documented in `TESTERS_README.md`.

## Evaluation

Run:

```bash
python evaluate.py
```

Artifacts:
- `evaluation_log.json`
- `final_thesis_metrics.json`

## Notes

- Do not commit `.env` files or real API keys.
- Ensure Docker images used by testers include `data/` and `kg/chroma_db/` so the default workflow is functional.