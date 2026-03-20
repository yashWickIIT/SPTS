import os
import sys
import uuid
import chromadb
from groq import Groq

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    # Package import path (used when loaded via backend.app)
    from backend.embedding_util import get_embeddings_batch
    from backend.config import CHROMA_PATH, API_KEY
    from backend.db_client import (
        fetch_distinct_non_null_values,
        get_table_columns,
        is_textual_column_type,
        list_user_tables,
    )
except ImportError:
    # Standalone script path (python update_vlkg.py)
    sys.path.insert(0, os.path.abspath(os.path.join(BASE_DIR, "..", "backend")))
    from embedding_util import get_embeddings_batch
    from config import CHROMA_PATH, API_KEY
    from db_client import (
        fetch_distinct_non_null_values,
        get_table_columns,
        is_textual_column_type,
        list_user_tables,
    )

_groq_client = None


def _get_groq_client():
    global _groq_client
    if _groq_client is not None:
        return _groq_client

    if not API_KEY:
        return None

    try:
        _groq_client = Groq(api_key=API_KEY)
        return _groq_client
    except Exception as e:
        print(f"      [!] Failed to initialize Groq client: {e}")
        return None

# Settings matching your build script
MAX_DISTINCT_VALUES = 100
SKIP_KEYWORDS = ['id', 'code', 'url', 'zip', 'phone', 'email', 'date', 'time', 'website']

def generate_synonyms(value, column_context):
    client = _get_groq_client()
    if client is None:
        return []

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

    tables = list_user_tables()

    total_new_values_added = 0

    for table in tables:
        columns_info = get_table_columns(table)

        text_cols = [c["name"] for c in columns_info if is_textual_column_type(c.get("type"))]

        for col in text_cols:
            if any(keyword in col.lower() for keyword in SKIP_KEYWORDS):
                continue

            try:
                # Fetch distinct values from the live database
                current_values = [
                    value
                    for value in fetch_distinct_non_null_values(table, col, MAX_DISTINCT_VALUES)
                    if isinstance(value, str) and value.strip()
                ]

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

    print(f"\n✅ Delta Update Complete! Embedded and appended {total_new_values_added} new unique database values.")

if __name__ == "__main__":
    delta_update()