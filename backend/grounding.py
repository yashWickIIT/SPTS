import os
import json
import uuid
import chromadb
from groq import Groq
try:
    from .embedding_util import get_embedding
    from .config import get_env_path
    from .db_client import get_main_dialect_name, list_user_tables, get_table_columns
except ImportError:
    from embedding_util import get_embedding
    from config import get_env_path
    from db_client import get_main_dialect_name, list_user_tables, get_table_columns

CHROMA_PATH = get_env_path("SPTS_CHROMA_PATH", os.path.join("kg", "chroma_db"))

print("Initializing ChromaDB connection in grounding...")
chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = None


def _connect_collection():
    global collection
    try:
        collection = chroma_client.get_collection(name="spts_vlkg")
        return True
    except Exception as e:
        print(f"Warning: ChromaDB collection 'spts_vlkg' not found. Error: {e}")
        collection = None
        return False


def _bootstrap_collection_if_missing():
    global collection
    if collection:
        return True

    if _connect_collection():
        return True

    print("VLKG collection missing. Running one-time bootstrap build...")
    try:
        from kg.build_vlkg import build_graph
        build_graph()
    except Exception as e:
        print(f"VLKG bootstrap failed: {e}")
        return False

    return _connect_collection()


_connect_collection()

client = Groq(api_key=os.getenv("API_KEY"))

def get_mini_schema():
    """Fetches a lightweight schema for the fallback LLM."""
    schema_str = ""
    tables = list_user_tables()
    for table in tables:
        cols = [c['name'] for c in get_table_columns(table)]
        schema_str += f"Table: {table} | Columns: {', '.join(cols)}\n"
    return schema_str

def extract_entities(query: str):
    prompt = f"""
    Extract the key specific entities, names, locations, or categorical values from the following user query. 
    Ignore general words like 'how many', 'show me', 'count', 'database'.
    Query: "{query}"
    Output ONLY a JSON object with a list of strings under the key 'entities'. Do not include markdown.
    """
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        return json.loads(resp.choices[0].message.content).get("entities", [])
    except Exception:
        return []

def lightweight_fallback_search(entity: str):
    """Uses a fast, lightweight model to guess the canonical value if vector search fails."""
    schema = get_mini_schema()
    prompt = f"""
    You are a database mapping assistant. The user searched for the noisy entity: "{entity}".
    Here is the database schema:
    {schema}
    
    Guess the exact canonical database value this maps to, and identify the likely table and column.
    Output ONLY a JSON object with keys: 'canonical', 'table', 'column', 'confidence_score' (1-100).
    """
    try:
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant", # Lightweight, fast, cost-effective model
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        data = json.loads(resp.choices[0].message.content)
        if data.get('confidence_score', 0) > 75:
            return data.get('canonical'), data.get('table'), data.get('column')
        return None, None, None
    except Exception:
        return None, None, None

def ground_query(query: str):
    if not collection and not _bootstrap_collection_if_missing():
        return query, []

    applied_mappings = []
    entities = extract_entities(query)
    
    for entity in entities:
        if len(entity) < 2:
            continue
            
        query_embedding = get_embedding(entity)
        if not query_embedding:
            continue

        results = collection.query(query_embeddings=[query_embedding], n_results=1)
        
        distance = results['distances'][0][0] if results['distances'] and len(results['distances'][0]) > 0 else 999
        THRESHOLD = 1.0 
        
        if distance < THRESHOLD:
            metadata = results['metadatas'][0][0]
            if metadata['canonical'].lower() != entity.lower():
                applied_mappings.append({
                    "original": entity,
                    "grounded": metadata['canonical'],
                    "table": metadata['table'],
                    "column": metadata['column'],
                    "distance": round(distance, 4),
                    "type": "Vector Semantic Match"
                })
        else:
            # DYNAMIC FALLBACK: Vector failed, trigger lightweight LLM
            print(f"Vector search failed for '{entity}'. Triggering LLM Fallback...")
            canonical, table, column = lightweight_fallback_search(entity)
            
            if canonical and table and column:
                # 1. Use the value for the current query
                applied_mappings.append({
                    "original": entity,
                    "grounded": canonical,
                    "table": table,
                    "column": column,
                    "distance": 0.0,
                    "type": "Real-time LLM Fallback"
                })
                # 2. Update the Vector DB dynamically so it doesn't need the LLM next time
                collection.add(
                    documents=[entity],
                    embeddings=[query_embedding],
                    metadatas=[{"canonical": canonical, "table": table, "column": column}],
                    ids=[str(uuid.uuid4())]
                )
                print(f"Dynamically updated VLKG with new mapping: {entity} -> {canonical}")
                
    return query, applied_mappings