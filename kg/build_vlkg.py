import sqlite3
import os
import sys
import uuid
import chromadb
from groq import Groq

# 1. Add the backend directory to sys.path to import the embedding utility
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "..", "backend"))
from embedding_util import get_embeddings_batch
from config import get_env_path

DB_PATH = get_env_path("SPTS_MAIN_DB_PATH", os.path.join("data", "bird_mini_dev.sqlite"))
# 2. Define where the Vector DB will be saved locally
CHROMA_PATH = get_env_path("SPTS_CHROMA_PATH", os.path.join("kg", "chroma_db"))

API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)

MAX_DISTINCT_VALUES = 100

def generate_synonyms(value, column_context):
    """
    Context-Aware Synonym Generation.
    """
    prompt = f"""
    Context: Database Column '{column_context}'.
    Value: "{value}".
    Task: Generate 3 likely user abbreviations, slang, or variations for this value.
    Example: "Los Angeles Unified" -> ["LAUSD", "LA Unified", "L.A. Schools"]
    Output ONLY JSON object with key 'synonyms'. Do not add markdown blocks.
    """
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        import json
        content = resp.choices[0].message.content
        data = json.loads(content)
        return data.get("synonyms", [])
    except Exception as e:
        print(f"      [!] Error generating synonyms for '{value}': {e}")
        return []

def build_graph():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    print("Initializing ChromaDB Persistent Client...")
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    
    # 3. Reset the collection for a fresh build
    collection_name = "spts_vlkg"
    try:
        chroma_client.delete_collection(name=collection_name)
    except Exception:
        pass # Collection might not exist yet, which is fine
        
    collection = chroma_client.create_collection(name=collection_name)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r[0] for r in cursor.fetchall() if r[0] != 'sqlite_sequence']

    for table in tables:
        print(f"\nAnalyzing table: {table}")
        cursor.execute(f"PRAGMA table_info({table})")
        columns_info = cursor.fetchall()
        
        text_cols = [c[1] for c in columns_info if "TEXT" in c[2].upper() or "VARCHAR" in c[2].upper() or "CHAR" in c[2].upper()]

        if not text_cols:
            print(f"   -> No text columns found in {table}. Skipping.")
            continue

        for col in text_cols:
            print(f"   Profiling {table}.{col}...")

            try:
                cursor.execute(f'SELECT COUNT(DISTINCT "{col}") FROM "{table}" WHERE "{col}" IS NOT NULL')
                distinct_count = cursor.fetchone()[0]

                if distinct_count == 0 or distinct_count > MAX_DISTINCT_VALUES:
                    print(f"      -> Skipping (Count: {distinct_count})")
                    continue

                cursor.execute(f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT {MAX_DISTINCT_VALUES}')
                values = [row[0] for row in cursor.fetchall() if isinstance(row[0], str) and row[0].strip()]

                # Setup batch arrays for ChromaDB insertion
                batch_docs = []
                batch_metadatas = []
                batch_ids = []

                for canonical in values:
                    aliases = generate_synonyms(canonical, col)
                    
                    clean_words = [w for w in canonical.split() if w.isalnum()]
                    if len(clean_words) > 1:
                        initials = "".join(w[0] for w in clean_words).lower()
                        if len(initials) > 1:
                            aliases.append(initials)

                    # Include the original canonical value in the search space
                    aliases.append(canonical)
                    
                    # Deduplicate any overlap
                    unique_aliases = list(set([a.strip() for a in aliases if a.strip()]))

                    # 4. Prepare the documents and metadata for embedding
                    for alias in unique_aliases:
                        batch_docs.append(alias)
                        batch_metadatas.append({
                            "canonical": canonical,
                            "table": table,
                            "column": col
                        })
                        # ChromaDB requires a unique ID for every single document
                        batch_ids.append(str(uuid.uuid4()))
                
                # 5. Generate Vectors and Insert into Database
                if batch_docs:
                    print(f"      -> Generating embeddings for {len(batch_docs)} textual variants...")
                    batch_embeddings = get_embeddings_batch(batch_docs)
                    
                    print(f"      -> Storing in ChromaDB...")
                    collection.add(
                        documents=batch_docs,
                        embeddings=batch_embeddings,
                        metadatas=batch_metadatas,
                        ids=batch_ids
                    )

            except Exception as e:
                print(f"      [!] Error profiling {table}.{col}: {e}")

    conn.close()
    
    # Clean up the deprecated json file
    json_path = os.path.join(BASE_DIR, "vlkg.json")
    if os.path.exists(json_path):
        os.remove(json_path)
        
    print(f"\nSuccessfully generated and saved Vector Database to {CHROMA_PATH}")

if __name__ == "__main__":
    build_graph()