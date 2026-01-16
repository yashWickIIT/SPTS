import pandas as pd
import chromadb
from chromadb.utils import embedding_functions
from groq import Groq
import os
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)
VECTOR_DB_PATH = "./chroma_db_store"

chroma_client = chromadb.PersistentClient(path=VECTOR_DB_PATH)
ef = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2",
    device="cpu"
)
collection = chroma_client.get_or_create_collection(name="semantic_map", embedding_function=ef)

def get_groq_enrichment(value):
    """
    Uses Groq (Llama-3) to "Profile" a single data value.
    It identifies what the value represents (Canonical form).
    """
    prompt = f"""
    Analyze this specific database value: "{value}" from a column named 'County' or 'Location'.
    
    1. Identify the 'Canonical' (clean/standard) full name.
    2. Identify if it is a synonym, abbreviation, or misspelling.
    
    Output ONLY a JSON object:
    {{
        "canonical": "Clean Name",
        "type": "Abbreviation/Synonym/Misspelling/Standard"
    }}
    """
    
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        return json.loads(completion.choices[0].message.content)
    except Exception as e:
        print(f"⚠️ Groq Error for {value}: {e}")
        return {"canonical": value, "type": "Unknown"}

def build_semantic_vector_map():
    if not os.path.exists("dirty_values.csv"):
        print("❌ Error: 'dirty_values.csv' not found. Run scan.py first.")
        return

    df = pd.read_csv("dirty_values.csv")
    col_name = "County" if "County" in df.columns else df.columns[0]
    
    dirty_values = df[col_name].head(20).tolist()
    
    print(f"🚀 Profiling {len(dirty_values)} values with Groq & ChromaDB...")
    
    ids = []
    documents = []
    metadatas = []

    for idx, val in enumerate(dirty_values):
        profile_data = get_groq_enrichment(val)
        ids.append(f"id_{idx}")
        documents.append(str(val))
        meta = {
            "original": str(val),
            "canonical": profile_data.get("canonical", str(val)),
            "enrichment_type": profile_data.get("type", "Standard")
        }
        metadatas.append(meta)
        print(f"   Processed: '{val}' -> {meta['canonical']}")
    collection.upsert(
        ids=ids,
        documents=documents,
        metadatas=metadatas
    )
    print(f"✅ Semantic Map built in '{VECTOR_DB_PATH}'")

if __name__ == "__main__":
    build_semantic_vector_map()