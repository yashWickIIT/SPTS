# SPTS: Domain-Adaptive Semantic Profiler for Text-to-SQL

**Research Prototype for Value-Level Knowledge Graph Grounding**

## 📖 Overview
SPTS (Semantic Profiler for Text-to-SQL) addresses the critical research gap of **Value Linking** in Natural Language Interfaces to Databases (NLIDB). 

While modern Large Language Models (LLMs) excel at generating correct SQL syntax (*Schema Linking*), they frequently fail when user queries contain ambiguous, abbreviated, or "dirty" data values that do not strictly match database entries (e.g., searching for "LAUSD" when the database contains "Los Angeles Unified").

This prototype demonstrates a novel **Offline Semantic Profiling** approach that constructs a **Value-Level Knowledge Graph (VLKG)** to resolve these ambiguities *before* SQL generation, significantly improving execution accuracy on real-world datasets like BIRD.

---

## 🔬 Research Gap & Contribution

| Feature | Standard RAG / Baseline | SPTS (Proposed) |
| :--- | :--- | :--- |
| **Ambiguity Handling** | Fails on abbreviations ("Frisco", "LA") | **Resolves entities** via VLKG ("San Francisco Unified") |
| **Knowledge Source** | Generic LLM Training Data | **Domain-Specific Database Profiling** |
| **Mechanism** | Hallucination / Guesses | **Deterministic Grounding** + LLM Reasoning |
| **Evaluation** | Execution Accuracy = 0% | **Execution Accuracy = 100%** (on test cases) |



---

## 📂 Project Structure

```text
SPTS/
├── backend/               # FastAPI Inference Engine
│   ├── app.py             # Main API & Server
│   ├── grounding.py       # Entity Resolution Logic 
│   ├── text_to_sql.py     # LLM Interaction Layer
│   └── database.py        # SQLite Execution Engine
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