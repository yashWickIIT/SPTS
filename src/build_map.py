import pandas as pd
import chromadb
from chromadb.utils import embedding_functions
from groq import Groq
import os
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "db_values.csv"
VECTOR_DB_PATH = BASE_DIR / "chroma_db_store"

API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)

# Initialize ChromaDB (CPU Mode for stability)
chroma_client = chromadb.PersistentClient(path=str(VECTOR_DB_PATH))
ef = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2",
    device="cpu" 
)
collection = chroma_client.get_or_create_collection(name="value_level_kg", embedding_function=ef)

def generate_synonyms_with_llm(value):
    """
    Uses LLM to bridge the gap between 'User Speak' and 'Database Speak'.
    """
    prompt = f"""
    Context: Building a Semantic Knowledge Graph for California Schools.
    Database Value: "{value}" (School District).
    Task: Generate 3 likely abbreviations, acronyms, or variations a user might search for.
    Example: "Los Angeles Unified" -> ["LAUSD", "L.A.", "LA Schools"]
    Output: JSON object with a key "synonyms" containing a list of strings.
    """
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        result = json.loads(completion.choices[0].message.content)
        
        # Robust JSON parsing
        if "synonyms" in result: return result["synonyms"]
        if isinstance(result, dict): return list(result.values())[0]
        return result
    except Exception as e:
        print(f"⚠️ LLM Error for {value}: {e}")
        return []

def build_knowledge_graph():
    if not CSV_PATH.exists():
        print(f"❌ Error: {CSV_PATH.name} not found. Run scan.py first.")
        return

    df = pd.read_csv(CSV_PATH)
    col_name = df.columns[0]
    
    # Reset collection for a clean build
    try:
        chroma_client.delete_collection("value_level_kg")
        collection = chroma_client.get_or_create_collection("value_level_kg", embedding_function=ef)
    except:
        pass

    # Process top 20 values (Sufficient for Demo)
    clean_values = df[col_name].head(20).tolist()
    print(f"🚀 Constructing Value-Level Knowledge Graph for {len(clean_values)} entities...")
    
    ids, documents, metadatas = [], [], []

    for idx, clean_val in enumerate(clean_values):
        # 1. Generate Edges (Synonyms)
        synonyms = generate_synonyms_with_llm(clean_val)
        # 2. Add the Node itself
        synonyms.append(clean_val)
        
        print(f"   Node: '{clean_val}' ← Edges: {synonyms}")

        for i, syn in enumerate(synonyms):
            ids.append(f"kg_{idx}_{i}")
            documents.append(syn) # Index the synonym
            metadatas.append({
                "canonical_value": clean_val, # Point to the truth
                "source": "BIRD_DB"
            })

    if ids:
        collection.add(ids=ids, documents=documents, metadatas=metadatas)
        print(f"✅ Knowledge Graph successfully built at '{VECTOR_DB_PATH}'")

if __name__ == "__main__":
    build_knowledge_graph()