import json
import uuid
import re
import time
from threading import Lock
import chromadb
from groq import Groq, RateLimitError, APITimeoutError

try:
    from .embedding_util import get_embedding
    from .config import CHROMA_PATH, API_KEY, GROQ_API_KEYS
    from .db_client import (
        list_user_tables,
        get_table_columns,
        table_has_column,
        value_exists_in_column,
    )
except ImportError:
    from embedding_util import get_embedding
    from config import CHROMA_PATH, API_KEY, GROQ_API_KEYS
    from db_client import (
        list_user_tables,
        get_table_columns,
        table_has_column,
        value_exists_in_column,
    )

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


def ensure_vlkg_ready():
    return _bootstrap_collection_if_missing()


def get_vlkg_status() -> dict:
    """Returns lightweight runtime status details for the VLKG collection."""
    status = {
        "chroma_path": CHROMA_PATH,
        "collection_name": "spts_vlkg",
        "collection_ready": False,
        "mapping_count": 0,
        "error": None,
    }

    try:
        if not collection and not _connect_collection():
            status["error"] = "Collection not found"
            return status

        status["collection_ready"] = True
        try:
            status["mapping_count"] = int(collection.count())
        except Exception as count_error:
            status["error"] = str(count_error)
        return status
    except Exception as error:
        status["error"] = str(error)
        return status


def _norm_text(value):
    if value is None:
        return ""
    return str(value).strip()


def _mapping_id(entity: str, canonical: str, table: str, column: str) -> str:
    seed = f"{entity.lower()}|{canonical.lower()}|{table.lower()}|{column.lower()}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


_GENERIC_ENTITY_TOKENS = {
    "school",
    "schools",
    "district",
    "districts",
    "county",
    "city",
    "state",
    "database",
    "data",
    "status",
    "active",
    "closed",
    "students",
    "student",
    "sat",
    "score",
    "scores",
    "math",
    "reading",
    "average",
    "count",
    "number",
    "record",
    "records",
    "year",
    "academic",
    "grade",
}


def _tokenize(value: str):
    return re.findall(r"[a-z0-9]+", _norm_text(value).lower())


def _is_generic_entity(entity: str) -> bool:
    tokens = _tokenize(entity)
    if not tokens:
        return True
    return all(token in _GENERIC_ENTITY_TOKENS for token in tokens)


def _acronym_of(text: str) -> str:
    words = re.findall(r"[A-Za-z]+", _norm_text(text))
    return "".join(w[0].lower() for w in words if w)


def _has_token_overlap(entity: str, canonical: str) -> bool:
    ent_tokens = set(_tokenize(entity))
    can_tokens = set(_tokenize(canonical))
    if not ent_tokens or not can_tokens:
        return False
    return len(ent_tokens.intersection(can_tokens)) > 0


def _is_plausible_vector_mapping(entity: str, canonical: str, distance: float) -> bool:
    if distance > 0.45:
        return False

    if _is_generic_entity(entity):
        return False

    entity_norm = _norm_text(entity).lower()
    canonical_norm = _norm_text(canonical).lower()

    if entity_norm in canonical_norm or canonical_norm in entity_norm:
        return True

    if _has_token_overlap(entity, canonical):
        return True

    canonical_acronym = _acronym_of(canonical)
    entity_tokens = set(_tokenize(entity))
    if canonical_acronym and canonical_acronym in entity_tokens:
        return True

    return False


_connect_collection()

_groq_clients = []
_groq_rotation_index = 0
_groq_client_lock = Lock()


def _configured_api_keys():
    keys = [key.strip() for key in (GROQ_API_KEYS or []) if key and key.strip()]
    if keys:
        return keys
    if API_KEY and API_KEY.strip():
        return [API_KEY.strip()]
    return []


def _ensure_groq_clients():
    global _groq_clients
    if _groq_clients:
        return _groq_clients

    for key in _configured_api_keys():
        try:
            _groq_clients.append(Groq(api_key=key))
        except Exception as e:
            print(f"Warning: failed to initialize Groq client in grounding: {e}")
            continue

    return _groq_clients


def _acquire_next_client():
    global _groq_rotation_index
    with _groq_client_lock:
        clients = _ensure_groq_clients()
        if not clients:
            return None, -1

        current_index = _groq_rotation_index % len(clients)
        _groq_rotation_index = (_groq_rotation_index + 1) % len(clients)
        return clients[current_index], current_index


def _is_retryable_groq_error(error: Exception) -> bool:
    if isinstance(error, (RateLimitError, APITimeoutError)):
        return True

    error_text = str(error).lower()
    retryable_markers = (
        "rate limit",
        "too many requests",
        "429",
        "quota",
        "timeout",
        "timed out",
        "overloaded",
    )
    return any(marker in error_text for marker in retryable_markers)


def _groq_completion_with_failover(create_completion, attempts_per_key=2):
    clients = _ensure_groq_clients()
    if not clients:
        return None

    total_attempts = max(1, len(clients) * max(1, attempts_per_key))

    for attempt in range(total_attempts):
        client, _ = _acquire_next_client()
        if client is None:
            break

        try:
            return create_completion(client)
        except Exception as error:
            if not _is_retryable_groq_error(error):
                return None
            time.sleep(min(0.3 * (attempt + 1), 1.5))

    return None


def get_mini_schema():
    """Fetches a lightweight schema for the fallback LLM."""
    schema_str = ""
    tables = list_user_tables()
    for table in tables:
        cols = [c["name"] for c in get_table_columns(table)]
        schema_str += f"Table: {table} | Columns: {', '.join(cols)}\n"
    return schema_str


def extract_entities(query: str):
    if not _configured_api_keys():
        return []

    prompt = f"""
    Extract ONLY the specific, categorical data values or proper nouns from this user query that need to be matched against database rows.
    
    CRITICAL INSTRUCTIONS - DO NOT EXTRACT:
    1. Numbers or digits (e.g., '400', '10th', '30'). These are thresholds, not row values.
    2. Database concepts or column names (e.g., 'average score', 'Math', 'Writing').
    3. General context words that describe the whole database (e.g., 'SAT test', 'schools', 'chartered').
    
    ONLY extract specific instances like city names ('Riverside'), exact states ('CA'), or specific proper noun categories ('Kindergarten', 'Virtual').
    
    Query: "{query}"
    Output ONLY a JSON object with a list of strings under the key 'entities'.
    """
    try:
        resp = _groq_completion_with_failover(
            lambda client: client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"},
            )
        )
        if resp is None:
            return []
        return json.loads(resp.choices[0].message.content).get("entities", [])
    except Exception:
        return []


def lightweight_fallback_search(entity: str):
    """Uses a fast, lightweight model to guess the canonical value if vector search fails."""
    if not _configured_api_keys():
        return None, None, None

    schema = get_mini_schema()
    prompt = f"""
    You are a database mapping assistant. The user searched for the noisy entity: "{entity}".
    Here is the database schema:
    {schema}
    
    Guess the exact canonical database value this maps to, and identify the likely table and column.
    Output ONLY a JSON object with keys: 'canonical', 'table', 'column', 'confidence_score' (1-100).
    """
    try:
        resp = _groq_completion_with_failover(
            lambda client: client.chat.completions.create(
                model="llama-3.1-8b-instant",  # Lightweight, fast, cost-effective model
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"},
            )
        )
        if resp is None:
            return None, None, None
        data = json.loads(resp.choices[0].message.content)
        if data.get("confidence_score", 0) > 75:
            return data.get("canonical"), data.get("table"), data.get("column")
        return None, None, None
    except Exception:
        return None, None, None


def ground_query(query: str):
    if not collection and not _bootstrap_collection_if_missing():
        return query, []

    applied_mappings = []
    entities = extract_entities(query)
    known_tables = set(list_user_tables())

    for entity in entities:
        if len(entity) < 2:
            continue

        query_embedding = get_embedding(entity)
        if not query_embedding:
            continue

        results = collection.query(query_embeddings=[query_embedding], n_results=1)

        distance = (
            results["distances"][0][0]
            if results["distances"] and len(results["distances"][0]) > 0
            else 999
        )
        THRESHOLD = 0.45

        if distance < THRESHOLD:
            metadata = results["metadatas"][0][0]
            canonical_value = metadata["canonical"]
            if (
                canonical_value.lower() != entity.lower()
                and _is_plausible_vector_mapping(entity, canonical_value, distance)
            ):
                applied_mappings.append(
                    {
                        "original": entity,
                        "grounded": canonical_value,
                        "table": metadata["table"],
                        "column": metadata["column"],
                        "distance": round(distance, 4),
                        "type": "Vector Semantic Match",
                    }
                )
        else:
            # DYNAMIC FALLBACK: Vector failed, trigger lightweight LLM
            print(f"Vector search failed for '{entity}'. Triggering LLM Fallback...")
            canonical, table, column = lightweight_fallback_search(entity)

            if canonical and table and column:
                canonical = _norm_text(canonical)
                table = _norm_text(table)
                column = _norm_text(column)

                if table not in known_tables:
                    print(
                        f"Skipped fallback mapping for '{entity}': unknown table '{table}'."
                    )
                    continue

                if not table_has_column(table, column):
                    print(
                        f"Skipped fallback mapping for '{entity}': unknown column '{table}.{column}'."
                    )
                    continue

                is_exact_match = value_exists_in_column(table, column, canonical)

                if not is_exact_match:
                    print(
                        f"Soft Mapping: '{canonical}' not exactly in {table}.{column}, but keeping as a hint for SPTS."
                    )
                else:
                    print(f"Exact Mapping: Found '{canonical}' in {table}.{column}.")

                applied_mappings.append(
                    {
                        "original": entity,
                        "grounded": canonical,
                        "table": table,
                        "column": column,
                        "distance": 0.0,
                        "type": "LLM Fallback (Exact)"
                        if is_exact_match
                        else "LLM Fallback (Hint)",
                    }
                )

                if is_exact_match:
                    collection.upsert(
                        documents=[entity],
                        embeddings=[query_embedding],
                        metadatas=[
                            {"canonical": canonical, "table": table, "column": column}
                        ],
                        ids=[_mapping_id(entity, canonical, table, column)],
                    )
                    print(
                        f"Dynamically updated VLKG with new exact mapping: {entity} -> {canonical}"
                    )

    return query, applied_mappings
