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

## Storage Path Configuration

Decision for this prototype:
- Keep the `data/` folder in the repository workflow.
- Avoid hardcoded paths in source code by configuring DB/vector storage via `.env`.
- Support external enterprise databases with zero Python code changes.

1. Create `.env` from `.env.example`.
2. Set these variables (relative paths are resolved from project root):

```env
API_KEY=your_groq_api_key_here
SECRET_KEY=replace_with_a_strong_random_secret
SPTS_DATABASE_URL=
SPTS_MAIN_DB_PATH=data/bird_mini_dev.sqlite
SPTS_USERS_DB_PATH=data/users.sqlite
SPTS_CHROMA_PATH=kg/chroma_db
```

Connection precedence:
1. If `SPTS_DATABASE_URL` is set, SPTS connects to that external database.
2. If `SPTS_DATABASE_URL` is empty, SPTS falls back to `SPTS_MAIN_DB_PATH` (prototype SQLite).

External DB URL examples:
- PostgreSQL: `postgresql+psycopg2://user:pass@host:5432/dbname`
- MySQL: `mysql+pymysql://user:pass@host:3306/dbname`
- SQL Server: `mssql+pyodbc://user:pass@host:1433/dbname?driver=ODBC+Driver+18+for+SQL+Server`

Note: install the matching SQLAlchemy driver package in your environment (for example `psycopg2-binary`, `pymysql`, or `pyodbc`).