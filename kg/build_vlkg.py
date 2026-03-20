import os
import sys
import uuid
import chromadb
from groq import Groq

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    # Package import path (used when loaded via backend modules)
    from backend.embedding_util import get_embeddings_batch
    from backend.config import CHROMA_PATH, API_KEY
    from backend.db_client import (
        count_distinct_non_null,
        fetch_distinct_non_null_values,
        get_table_columns,
        is_textual_column_type,
        list_user_tables,
    )
except ImportError:
    # Standalone script path (python build_vlkg.py)
    sys.path.insert(0, os.path.abspath(os.path.join(BASE_DIR, "..", "backend")))
    from embedding_util import get_embeddings_batch
    from config import CHROMA_PATH, API_KEY
    from db_client import (
        count_distinct_non_null,
        fetch_distinct_non_null_values,
        get_table_columns,
        is_textual_column_type,
        list_user_tables,
    )

client = Groq(api_key=API_KEY) if API_KEY else None

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
    print("Initializing ChromaDB Persistent Client...")
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    
    # 3. Reset the collection for a fresh build
    collection_name = "spts_vlkg"
    try:
        chroma_client.delete_collection(name=collection_name)
    except Exception:
        pass # Collection might not exist yet, which is fine
        
    collection = chroma_client.create_collection(name=collection_name)

    tables = list_user_tables()

    for table in tables:
        print(f"\nAnalyzing table: {table}")
        columns_info = get_table_columns(table)

        text_cols = [c["name"] for c in columns_info if is_textual_column_type(c.get("type"))]

        if not text_cols:
            print(f"   -> No text columns found in {table}. Skipping.")
            continue

        for col in text_cols:
            print(f"   Profiling {table}.{col}...")

            try:
                distinct_count = count_distinct_non_null(table, col)

                if distinct_count == 0 or distinct_count > MAX_DISTINCT_VALUES:
                    print(f"      -> Skipping (Count: {distinct_count})")
                    continue

                values = [
                    value
                    for value in fetch_distinct_non_null_values(table, col, MAX_DISTINCT_VALUES)
                    if isinstance(value, str) and value.strip()
                ]

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
    
    # Clean up the deprecated json file
    json_path = os.path.join(BASE_DIR, "vlkg.json")
    if os.path.exists(json_path):
        os.remove(json_path)
        
    print(f"\nSuccessfully generated and saved Vector Database to {CHROMA_PATH}")

if __name__ == "__main__":
    build_graph()