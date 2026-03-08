import sqlite3
import os
import sys
import uuid
import chromadb
from groq import Groq
from dotenv import load_dotenv

# Add the backend directory to sys.path to access the embedding utility
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "..", "backend"))
from embedding_util import get_embeddings_batch

load_dotenv()

DB_PATH = os.path.join(BASE_DIR, "..", "data", "bird_mini_dev.sqlite")
CHROMA_PATH = os.path.join(BASE_DIR, "chroma_db")

API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)

# Settings matching your build script
MAX_DISTINCT_VALUES = 100
SKIP_KEYWORDS = ['id', 'code', 'url', 'zip', 'phone', 'email', 'date', 'time', 'website']

def generate_synonyms(value, column_context):
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

def delta_update():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    print("Initializing ChromaDB for Delta Update...")
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    
    try:
        # Connect to the existing collection instead of creating/deleting it
        collection = chroma_client.get_collection(name="spts_vlkg")
    except Exception as e:
        print("Error: ChromaDB collection 'spts_vlkg' not found. Please run build_vlkg.py first.")
        return

    # 1. Load existing state: Fetch all current metadata to know what we already profiled
    print("Loading existing knowledge graph vectors to identify delta...")
    existing_data = collection.get(include=["metadatas"])
    
    # Create a fast-lookup set of signatures: e.g., "schools.District.Los Angeles Unified"
    processed_signatures = set()
    if existing_data and existing_data["metadatas"]:
        for meta in existing_data["metadatas"]:
            if meta:
                sig = f"{meta['table']}.{meta['column']}.{meta['canonical']}"
                processed_signatures.add(sig)
                
    print(f"-> Found {len(processed_signatures)} unique semantic records already in Vector DB.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r[0] for r in cursor.fetchall() if r[0] != 'sqlite_sequence']

    total_new_values_added = 0

    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns_info = cursor.fetchall()
        
        text_cols = [c[1] for c in columns_info if "TEXT" in c[2].upper() or "VARCHAR" in c[2].upper() or "CHAR" in c[2].upper()]

        for col in text_cols:
            if any(keyword in col.lower() for keyword in SKIP_KEYWORDS):
                continue

            try:
                # Fetch distinct values from the live database
                cursor.execute(f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT {MAX_DISTINCT_VALUES}')
                current_values = [row[0] for row in cursor.fetchall() if isinstance(row[0], str) and row[0].strip()]

                # 2. Delta Comparison: Filter out values we've already profiled
                new_values = []
                for val in current_values:
                    sig = f"{table}.{col}.{val}"
                    if sig not in processed_signatures:
                        new_values.append(val)

                if not new_values:
                    # Silent skip if no new data exists for this column
                    continue 
                    
                print(f"\n   -> Detected {len(new_values)} NEW distinct values in {table}.{col}. Processing Delta...")

                batch_docs = []
                batch_metadatas = []
                batch_ids = []

                for canonical in new_values:
                    aliases = generate_synonyms(canonical, col)
                    
                    clean_words = [w for w in canonical.split() if w.isalnum()]
                    if len(clean_words) > 1:
                        initials = "".join(w[0] for w in clean_words).lower()
                        if len(initials) > 1:
                            aliases.append(initials)

                    aliases.append(canonical)
                    unique_aliases = list(set([a.strip() for a in aliases if a.strip()]))

                    for alias in unique_aliases:
                        batch_docs.append(alias)
                        batch_metadatas.append({
                            "canonical": canonical,
                            "table": table,
                            "column": col
                        })
                        batch_ids.append(str(uuid.uuid4()))
                
                # 3. Append to Vector Database
                if batch_docs:
                    batch_embeddings = get_embeddings_batch(batch_docs)
                    collection.add(
                        documents=batch_docs,
                        embeddings=batch_embeddings,
                        metadatas=batch_metadatas,
                        ids=batch_ids
                    )
                    total_new_values_added += len(new_values)

            except Exception as e:
                print(f"      [!] Error processing delta for {table}.{col}: {e}")

    conn.close()
    print(f"\n✅ Delta Update Complete! Embedded and appended {total_new_values_added} new unique database values.")

if __name__ == "__main__":
    delta_update()