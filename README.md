# SPTS: Domain-Adaptive Semantic Profiler for Text-to-SQL

**Research Prototype for Value-Level Knowledge Graph Grounding**

## Overview
SPTS (Semantic Profiler for Text-to-SQL) addresses the critical research gap of **Value Linking** in Natural Language Interfaces to Databases (NLIDB). 

While modern Large Language Models (LLMs) excel at generating correct SQL syntax (*Schema Linking*), they frequently fail when user queries contain ambiguous, abbreviated, or "dirty" data values that do not strictly match database entries (e.g., searching for "LAUSD" when the database contains "Los Angeles Unified").

This prototype demonstrates a novel **Offline Semantic Profiling** approach that constructs a **Value-Level Knowledge Graph (VLKG)** to resolve these ambiguities *before* SQL generation, significantly improving execution accuracy. For this project publicly available bird mini dev data set is used.
---

## Project Structure

```text
SPTS/
├── backend/               # FastAPI Inference Engine
│   ├── app.py             # Main API & Server
│   ├── grounding.py       # Entity Resolution Logic 
│   ├── text_to_sql.py     # LLM Interaction Layer
│   └── database.py        # SQL Execution Engine
│
├── frontend/              # Dashboard
│   ├── index.html         # Main User Interface
│   ├── app.js             # Client Logic & Visualization
│   └── styles.css         # Styling
│
├── kg/                    # Offline Profiler Module
│   ├── build_vlkg.py      # The Semantic Profiler Script
│   └── vlkg.json          # The Generated Knowledge Graph
│
├── data/
│   └── bird_mini_dev.sqlite  # The Target Database (California Schools)
│
├── requirements.txt       # Python Dependencies
└── README.md              # Documentation

## Environment Configuration

**Single `.env` file at workspace root** — All components (backend, KG updater, tester CLI) load configuration from this one file via `backend/config.py`. This ensures consistent behavior across local dev, Docker, and utilities.

### Setup Instructions

1. **Verify `.env` exists** at the project root with your API key:
   ```bash
   cat .env
   ```

2. **Required variables** (already configured in `.env`):
   ```env
   # LLM API key (get from https://console.groq.com)
   API_KEY=your_groq_api_key_here

   # Database paths
   SPTS_MAIN_DB_PATH=data/bird_mini_dev.sqlite
   SPTS_CHROMA_PATH=kg/chroma_db

   # Authentication & sessions
   SECRET_KEY=spts-super-secret-key-12345
   SPTS_SESSIONS_DIR=/app/sessions
   ```

3. **Optional variables** (advanced database/embedding config):
   - Add them directly to `.env` as needed

### Database Connection Precedence

1. If `SPTS_DATABASE_URL` is set in `.env` → Use external database
   - PostgreSQL: `postgresql+psycopg2://user:pass@host:5432/dbname`
   - MySQL: `mysql+pymysql://user:pass@host:3306/dbname`
   - SQL Server: `mssql+pyodbc://user:pass@host:1433/dbname?driver=...`
2. Otherwise → Use local SQLite at `SPTS_MAIN_DB_PATH`

### Important Notes

- **Do not commit `.env` to version control** (contains sensitive keys)
- Old files (`.env.example`, `.env.test`) were removed; use only `.env`
- All modules import from `backend/config.py` — changes to `.env` take effect immediately at next startup

## Storage Path Configuration

Decision for this prototype:
- Keep the `data/` folder in the repository workflow.
- Avoid hardcoded paths in source code by configuring DB/vector storage via `.env`.
- Support external enterprise databases with zero Python code changes.