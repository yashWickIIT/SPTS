import streamlit as st
import chromadb
from chromadb.utils import embedding_functions
from groq import Groq
import os
from dotenv import load_dotenv

load_dotenv()

# --- SETUP ---
API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)
VECTOR_DB_PATH = "./chroma_db_store"

# Connect to Vector DB
chroma_client = chromadb.PersistentClient(path=VECTOR_DB_PATH)
# ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
# Force device="cpu" to fix the "meta tensor" error
ef = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2",
    device="cpu"
)
collection = chroma_client.get_or_create_collection(name="semantic_map", embedding_function=ef)

def retrieve_semantic_context(query):
    """
    Performs Vector Search to find relevant domain terms.
    """
    results = collection.query(
        query_texts=[query],
        n_results=3  # Fetch top 3 closest matches
    )
    
    context_lines = []
    if results['metadatas']:
        metas = results['metadatas'][0]
        distances = results['distances'][0]
        
        for i, meta in enumerate(metas):
            # Only use matches that are somewhat close (distance threshold)
            # Lower distance = better match. Threshold is adjustable.
            if distances[i] < 1.5: 
                line = f"- User term similar to '{meta['original']}' should map to Canonical Value: '{meta['canonical']}'"
                context_lines.append(line)
                
    return "\n".join(context_lines)

def generate_sql(question, use_spts=False):
    schema_info = "Table: schools, Columns: County, SchoolName, Students"
    
    system_msg = "You are a SQLite expert. Output ONLY valid SQL code. No markdown."
    user_msg = f"Question: {question}\nSchema: {schema_info}"

    if use_spts:
        # 1. RETRIEVE (The "R" in RAG)
        semantic_context = retrieve_semantic_context(question)
        
        if semantic_context:
            user_msg += f"\n\n🔍 SEMANTIC PROFILER KNOWLEDGE:\n{semantic_context}\n\n(Use the 'Canonical Value' for WHERE clauses)"
        else:
            user_msg += "\n\n(No specific semantic mappings found, use standard SQL matching)"

    # 2. GENERATE (The "G" in RAG)
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}
            ],
            temperature=0
        )
        return completion.choices[0].message.content.replace("```sql", "").replace("```", "").strip()
    except Exception as e:
        return f"Error: {str(e)}"

# --- UI ---
st.title("SPTS: Domain-Adaptive Text-to-SQL")
st.caption("Final Year Project: Hybrid Retrieval Architecture")

question = st.text_input("Ask a question (e.g., 'Show schools in NY'):", "Count schools in NY")

if st.button("Run Query"):
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Standard (Baseline)")
        # Baseline: Just raw GPT-3/4 guessing
        sql_base = generate_sql(question, use_spts=False)
        st.code(sql_base, language="sql")
        
    with col2:
        st.subheader("SPTS (Ours)")
        # Ours: Vector Search -> Context Injection -> SQL
        sql_spts = generate_sql(question, use_spts=True)
        st.code(sql_spts, language="sql")
        
        # Debug: Show what the retrieval found (Great for viva demo!)
        with st.expander("See Retrieved Context"):
            ctx = retrieve_semantic_context(question)
            st.text(ctx if ctx else "No relevant tags found.")