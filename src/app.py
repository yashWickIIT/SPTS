import streamlit as st
import chromadb
from chromadb.utils import embedding_functions
from groq import Groq
import os
import sqlite3
import sqlglot
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
DB_PATH = DATA_DIR / "c.sqlite"
VECTOR_DB_PATH = BASE_DIR / "chroma_db_store"
SCHEMA_PATH = BASE_DIR / "bird_schema.txt"

API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)
chroma_client = chromadb.PersistentClient(path=str(VECTOR_DB_PATH))
ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2", device="cpu")
collection = chroma_client.get_collection(name="value_level_kg")

GOLD_CATALOG = {
    "lausd": "SELECT COUNT(*) FROM schools WHERE District = 'Los Angeles Unified'",
    "frisco": "SELECT School FROM schools WHERE District = 'San Francisco Unified'",
    "sd": "SELECT School FROM schools WHERE District = 'San Diego Unified'",
    "ny": "SELECT COUNT(*) FROM schools WHERE County = 'New York County'",
    "alameda": "SELECT School FROM schools WHERE District = 'Alameda Unified'"
}

def get_gold_sql(user_query):
    """Finds Ground Truth SQL based on user input keywords."""
    q_lower = user_query.lower()
    for key, sql in GOLD_CATALOG.items():
        if key in q_lower:
            return sql
    return None


def retrieve_knowledge_graph(query):
    """Traverses the Knowledge Graph to find canonical entities."""
    try:
        results = collection.query(query_texts=[query], n_results=3)
        context_lines = []
        if results['metadatas']:
            metas = results['metadatas'][0]
            dists = results['distances'][0]
            for i, meta in enumerate(metas):
                if dists[i] < 1.4: 
                    context_lines.append(f"- User term maps to Graph Node: '{meta['canonical_value']}'")
        return list(set(context_lines))
    except:
        return []

def generate_sql(question, use_spts=False):
    schema_info = SCHEMA_PATH.read_text() if SCHEMA_PATH.exists() else "Table schools (District text)"
    
    system_msg = "You are a SQL expert. Output ONLY valid SQLite code. No markdown."
    user_msg = f"Schema: {schema_info}\nQuestion: {question}"

    if use_spts:
        kg_context = retrieve_knowledge_graph(question)
        if kg_context:
            user_msg += f"\n\n🧠 KNOWLEDGE GRAPH CONTEXT:\n" + "\n".join(kg_context)
            user_msg += "\n(CRITICAL: Use these canonical values in your WHERE clause)"

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
            temperature=0
        )
        return completion.choices[0].message.content.replace("```sql", "").replace("```", "").strip()
    except Exception as e:
        return f"Error: {e}"

def execute_sql(sql):
    """Executes SQL on the BIRD database."""
    if not DB_PATH.exists(): return [], []
    try:
        conn = sqlite3.connect(str(DB_PATH))
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        conn.close()
        return cols, rows
    except:
        return [], []

def calculate_structural_score(pred_sql, gold_sql):
    """
    INTERNAL ETM METRIC (Thesis-Valid Structural Matching).
    Uses sqlglot to compare Abstract Syntax Trees (AST).
    """
    if not gold_sql: return 0.0
    try:
        pred_tree = sqlglot.transpile(pred_sql, read=None, write="sqlite")[0]
        gold_tree = sqlglot.transpile(gold_sql, read=None, write="sqlite")[0]
        
        if pred_tree.lower() == gold_tree.lower():
            return 1.0
        return 0.0
    except:
        return 0.0

st.set_page_config(page_title="SPTS Demo", layout="wide")
st.title("SPTS: Value-Level Knowledge Graph")
st.markdown("**Research Gap:** Bridging Natural Language Ambiguity with Database-Centric Knowledge Graphs.")

question = st.text_input("Ask a question:", placeholder="e.g., Count schools in LAUSD")

if st.button("🚀 Run Experiment"):
    if not question:
        st.warning("Please type a question first.")
    else:
        gold_sql = get_gold_sql(question)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Standard (Baseline)")
            sql_base = generate_sql(question, use_spts=False)
            st.code(sql_base, language="sql")
            c, r = execute_sql(sql_base)
            
            st.metric("Rows Returned", len(r))
            if len(r) == 0: st.error("❌ Retrieval Failed")
            
            if gold_sql:
                score_b = calculate_structural_score(sql_base, gold_sql)
                st.metric("Structural Match Score", f"{score_b:.1f}")

        with col2:
            st.subheader("SPTS (Ours)")
            sql_spts = generate_sql(question, use_spts=True)
            st.code(sql_spts, language="sql")
            c, r = execute_sql(sql_spts)
            
            st.metric("Rows Returned", len(r))
            if len(r) > 0: st.success("✅ Retrieval Success")
            else: st.warning("No data found")
            
            if gold_sql:
                score_s = calculate_structural_score(sql_spts, gold_sql)
                st.metric("Structural Match Score", f"{score_s:.1f}", delta=f"{score_s - score_b:.1f}")

            with st.expander("🧠 View Knowledge Graph Activation"):
                ctx = retrieve_knowledge_graph(question)
                if ctx:
                    for line in ctx: st.write(line)
                else:
                    st.write("No semantic edges activated.")
