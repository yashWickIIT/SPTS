import pandas as pd
import chromadb
from chromadb.utils import embedding_functions
from groq import Groq
import os
import json
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)
VECTOR_DB_PATH = "./chroma_db_store"

# Initialize ChromaDB
chroma_client = chromadb.PersistentClient(path=VECTOR_DB_PATH)
# Use CPU to avoid "meta tensor" errors on student hardware
ef = embedding_functions.SentenceTransformerEmbeddingFunction(
    model_name="all-MiniLM-L6-v2",
    device="cpu" 
)
collection = chroma_client.get_or_create_collection(name="semantic_map", embedding_function=ef)

def generate_synonyms_with_llm(value):
    """
    The Core Research Innovation:
    Uses LLM to predict 'User Speak' for a given 'Database Speak' value.
    This constructs the 'Knowledge Graph' edges.
    """
    prompt = f"""
    You are building a Semantic Knowledge Graph for a database.
    
    Database Value: "{value}" (School District)
    
    Task: Generate 3-4 likely abbreviations, acronyms, or variations a user might type when looking for this.
    Example: If input is "Los Angeles Unified", output ["LAUSD", "L.A. Unified", "LA Schools"].
    
    Output ONLY a JSON list of strings.
    """
    
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        # Parse JSON output
        result = json.loads(completion.choices[0].message.content)
        # Handle cases where LLM returns {"synonyms": [...]} or just the list
        if isinstance(result, dict):
            return list(result.values())[0] # Grab the first list found
        return result
    except Exception as e:
        print(f"LLM Error for {value}: {e}")
        return []

def build_knowledge_graph():
    if not os.path.exists("db_values.csv"):
        print("Error: 'db_values.csv' not found. Run scan.py first.")
        return

    df = pd.read_csv("db_values.csv")
    # Determine the column name dynamically
    col_name = df.columns[0] 
    
    # Process top 15 values to save time/tokens for the demo
    clean_values = df[col_name].head(15).tolist()
    
    print(f"🚀 Constructing Value-Level Knowledge Graph for {len(clean_values)} entities...")
    
    ids = []
    documents = []
    metadatas = []

    for idx, clean_val in enumerate(clean_values):
        # 1. Generate Synonyms (The "Edges" of the graph)
        synonyms = generate_synonyms_with_llm(clean_val)
        
        # 2. Add the Canonical Value itself to the index
        synonyms.append(clean_val)
        
        print(f"   Node: '{clean_val}' <--> Edges: {synonyms}")

        # 3. Store EACH synonym as a vector pointing to the SAME canonical value
        for i, syn in enumerate(synonyms):
            ids.append(f"id_{idx}_{i}")
            documents.append(syn) # Vectorize the SYNONYM
            
            # Metadata points back to the CANONICAL value
            metadatas.append({
                "canonical_value": clean_val, 
                "source_node": "BIRD_DB"
            })

    # 4. Write to Vector Store
    collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
    print(f"Knowledge Graph built in '{VECTOR_DB_PATH}'")

if __name__ == "__main__":
    build_knowledge_graph()