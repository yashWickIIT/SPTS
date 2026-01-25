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
ef = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2",
    device="cpu"
)
collection = chroma_client.get_or_create_collection(name="semantic_map", embedding_function=ef)

def retrieve_knowledge_graph(query):
    """
    Traverses the Value-Level Knowledge Graph to find canonical entities.
    """
    results = collection.query(
        query_texts=[query],
        n_results=3
    )
    
    context_lines = []
    if results['metadatas']:
        metas = results['metadatas'][0]
        distances = results['distances'][0]
        
        for i, meta in enumerate(metas):
            # Distance < 1.0 is a decent semantic match
            if distances[i] < 1.0: 
                line = f"- User Term maps to Graph Node: '{meta['canonical_value']}'"
                context_lines.append(line)
                
    # Deduplicate lines
    return "\n".join(list(set(context_lines)))

def generate_sql(question, use_spts=False):
    # Schema matches BIRD California Schools
    schema_info = "Table: schools, Columns: School, District, County"
    
    system_msg = "You are a SQL expert. Output ONLY valid SQLite code."
    user_msg = f"Question: {question}\nSchema: {schema_info}"

    if use_spts:
        # RAG Step
        kg_context = retrieve_knowledge_graph(question)
        
        if kg_context:
            user_msg += f"\n\n KNOWLEDGE GRAPH CONTEXT:\n{kg_context}\n(Use these exact values in WHERE clauses)"
        else:
            user_msg += "\n\n(No graph mappings found, use standard matching)"

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
st.title("SPTS: Value-Level Knowledge Graph")
st.caption("Demonstrating Robustness on BIRD-Mini (California Schools)")

# A perfect natural gap query
question = st.text_input("Ask a question:", "Count schools in LAUSD")

if st.button("Run Query"):
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Standard (Baseline)")
        sql_base = generate_sql(question, use_spts=False)
        st.code(sql_base, language="sql")
        
    with col2:
        st.subheader("SPTS (Ours)")
        sql_spts = generate_sql(question, use_spts=True)
        st.code(sql_spts, language="sql")
        
        with st.expander("View Graph Traversal"):
            ctx = retrieve_knowledge_graph(question)
            st.text(ctx if ctx else "No semantic edge found.")