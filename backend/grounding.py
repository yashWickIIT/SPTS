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
        is_textual_column_type,
    )
except ImportError:
    from embedding_util import get_embedding
    from config import CHROMA_PATH, API_KEY, GROQ_API_KEYS
    from db_client import (
        list_user_tables,
        get_table_columns,
        table_has_column,
        value_exists_in_column,
        is_textual_column_type,
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

_ENTITY_ACTION_WORDS = {
    "list",
    "give",
    "show",
    "tell",
    "provide",
    "indicate",
    "among",
    "between",
    "what",
    "which",
    "where",
    "how",
    "please",
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
    if distance > 0.50:
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


def _normalize_entity_candidate(candidate: str) -> str:
    entity = _norm_text(candidate).strip("'\"`.,:;!?()[]{}")
    if not entity:
        return ""

    lowered = entity.lower()

    # Remove leading question/action tokens.
    for prefix in (
        "between ",
        "among ",
        "list ",
        "give ",
        "show ",
        "provide ",
        "indicate ",
    ):
        if lowered.startswith(prefix):
            entity = entity[len(prefix):].strip()
            lowered = entity.lower()

    # Remove single-token action words.
    if lowered in _ENTITY_ACTION_WORDS:
        return ""

    # Skip pure numbers and threshold-like fragments.
    if re.fullmatch(r"\d+", entity):
        return ""

    return entity


def _clean_entities(raw_entities: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()

    for raw in raw_entities:
        normalized = _normalize_entity_candidate(raw)
        if not normalized:
            continue

        # Split "A and B" composite entities into separate candidates.
        parts = [normalized]
        if " and " in normalized.lower() and len(normalized.split()) >= 3:
            parts = [part.strip() for part in re.split(r"\band\b", normalized, flags=re.IGNORECASE)]

        for part in parts:
            part = _normalize_entity_candidate(part)
            if not part:
                continue

            key = part.lower()
            if key in seen:
                continue

            seen.add(key)
            cleaned.append(part)

    return cleaned


def _is_plausible_fallback_mapping(entity: str, canonical: str) -> bool:
    if not canonical:
        return False

    entity_norm = _norm_text(entity).lower()
    canonical_norm = _norm_text(canonical).lower()
    if not entity_norm or not canonical_norm:
        return False

    if _is_generic_entity(entity):
        return False

    if entity_norm in canonical_norm or canonical_norm in entity_norm:
        return True

    if _has_token_overlap(entity, canonical):
        return True

    canonical_acronym = _acronym_of(canonical)
    entity_tokens = set(_tokenize(entity))
    if canonical_acronym and canonical_acronym in entity_tokens:
        return True

    return False


def _context_keywords(query: str) -> set[str]:
    query_lower = _norm_text(query).lower()
    keywords = set()
    for key in (
        "county",
        "city",
        "district",
        "state",
        "grade",
        "k-12",
        "kindergarten",
        "virtual",
        "charter",
        "frpm",
        "sat",
        "school",
    ):
        if key in query_lower:
            keywords.add(key)
    return keywords


def _column_context_score(query: str, column: str) -> float:
    col = _norm_text(column).lower()
    score = 0.0
    keys = _context_keywords(query)

    if "county" in keys and "county" in col:
        score += 0.20
    if "city" in keys and "city" in col:
        score += 0.20
    if "district" in keys and "district" in col:
        score += 0.20
    if "state" in keys and "state" in col:
        score += 0.20
    if ("grade" in keys or "k-12" in keys or "kindergarten" in keys) and (
        "grade" in col or "gsserved" in col or "low grade" in col or "high grade" in col
    ):
        score += 0.25
    if "virtual" in keys and "virtual" in col:
        score += 0.25
    if "charter" in keys and "charter" in col:
        score += 0.20
    if "frpm" in keys and "frpm" in col:
        score += 0.20
    if "sat" in keys and "sat" in col:
        score += 0.20
    if "school" in keys and "school" in col:
        score += 0.10
    if "administrator" in keys and "admfname" in col:
        score += 0.30
    if "ownership" in keys and ("soc" in col or "edopscode" in col):
        score += 0.30
    if "physical building" in keys and "virtual" in col:
        score += 0.30
    if "grade span" in keys and "gsserved" in col:
        score += 0.30

    return score


def _column_context_compatible(query: str, column: str) -> bool:
    col = _norm_text(column).lower()
    keys = _context_keywords(query)

    # Strong constraints only when explicit cues are present.
    if "county" in keys and any(word in col for word in ("city", "district")):
        return False
    if "city" in keys and "county" in col:
        return False

    return True


def _make_exact_mapping(original: str, grounded: str, table: str, column: str, mapping_type: str = "Rule Exact Match") -> dict:
    return {
        "original": original,
        "grounded": grounded,
        "table": table,
        "column": column,
        "distance": 0.0,
        "type": mapping_type,
    }


def _dedupe_mappings(mappings: list[dict]) -> list[dict]:
    unique = []
    seen = set()
    for mapping in mappings:
        key = (
            str(mapping.get("original", "")).lower(),
            str(mapping.get("grounded", "")).lower(),
            str(mapping.get("table", "")).lower(),
            str(mapping.get("column", "")).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(mapping)
    return unique


def _rule_based_query_mappings(query: str) -> list[dict]:
    rules: list[dict] = []
    query_text = _norm_text(query)
    query_lower = query_text.lower()

    county_matches = re.findall(r"county(?:\s+of)?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", query_text)
    for county_name in county_matches:
        if value_exists_in_column("schools", "County", county_name):
            rules.append(_make_exact_mapping(county_name, county_name, "schools", "County"))

    if "between" in query_lower and "county" in query_lower:
        place_matches = re.findall(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", query_text)
        for place in place_matches:
            if place.lower() in {"between", "which", "indicate"}:
                continue
            if value_exists_in_column("schools", "County", place):
                rules.append(_make_exact_mapping(place, place, "schools", "County"))

    grade_span_match = re.search(r"kindergarten\s+to\s+(\d+)(?:st|nd|rd|th)\s+grade", query_lower)
    if grade_span_match:
        canonical = f"K-{grade_span_match.group(1)}"
        if value_exists_in_column("schools", "GSserved", canonical):
            rules.append(_make_exact_mapping("Kindergarten to grade span", canonical, "schools", "GSserved"))

    ownership_match = re.search(r"ownership code\s+(\d+)", query_lower)
    if ownership_match:
        ownership_code = ownership_match.group(1)
        if value_exists_in_column("schools", "SOC", ownership_code):
            rules.append(_make_exact_mapping(f"ownership code {ownership_code}", ownership_code, "schools", "SOC"))

    admin_match = re.search(r"first name is\s+([A-Za-z]+)", query_text, re.IGNORECASE)
    if admin_match:
        admin_name = admin_match.group(1)
        if value_exists_in_column("schools", "AdmFName1", admin_name):
            rules.append(_make_exact_mapping(admin_name, admin_name, "schools", "AdmFName1"))

    if "does not offer physical building" in query_lower and value_exists_in_column("schools", "Virtual", "F"):
        rules.append(_make_exact_mapping("does not offer physical building", "F", "schools", "Virtual"))

    return _dedupe_mappings(rules)


def _direct_exact_schema_mapping(query: str, entity: str) -> dict | None:
    candidates = []
    for table in list_user_tables():
        for column in get_table_columns(table):
            column_name = str(column.get("name", "") or "")
            column_type = column.get("type")
            if not column_name:
                continue
            if column_type is not None and not is_textual_column_type(column_type):
                continue
            if not _column_context_compatible(query, column_name):
                continue
            if value_exists_in_column(table, column_name, entity):
                score = 1.0 + _column_context_score(query, column_name)
                candidates.append((score, table, column_name))

    if not candidates:
        return None

    candidates.sort(reverse=True)
    _, table, column_name = candidates[0]
    return _make_exact_mapping(entity, entity, table, column_name, "Schema Exact Match")


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
        return _clean_entities(_extract_domain_patterns(query))

    # First, extract domain-specific patterns (grade ranges, etc.) to avoid LLM false negatives
    domain_entities = _extract_domain_patterns(query)
    
    prompt = f"""
    Extract ONLY the specific, categorical data values or proper nouns from this user query that need to be matched against database rows.
    
    CRITICAL INSTRUCTIONS - DO NOT EXTRACT:
    1. Numeric thresholds used for comparison (e.g., '400', '30' in 'more than 30' or 'greater than 400').
       → BUT DO extract grade/year ranges (e.g., 'K-12', '9th grade', '10th', 'Kindergarten').
    2. Database concepts or column names (e.g., 'average score', 'Math', 'Writing').
    3. General context words that describe the whole database (e.g., 'SAT test', 'schools', 'chartered').
    
    EXAMPLE EXTRACTIONS:
    - "schools in Los Angeles" → ['Los Angeles']
    - "grade K-9" → ['K-9']
    - "Kindergarten to 9th grade" → ['Kindergarten', '9th', 'K-9']
    - "enrollment > 30" → [] (30 is a threshold, skip)
    - "San Diego or Santa Barbara" → ['San Diego', 'Santa Barbara']
    - "Virtual schools" → ['Virtual']
    
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
            return _clean_entities(domain_entities)
        llm_entities = json.loads(resp.choices[0].message.content).get("entities", [])
        combined = domain_entities + llm_entities
        return _clean_entities(combined)
    except Exception:
        return _clean_entities(domain_entities)


def _extract_domain_patterns(query: str):
    """Extract domain-specific entities: grade ranges, school types, county names via regex."""
    patterns = []
    import re
    
    # Grade/grade-span patterns: K-12, 9th, Kindergarten, grade 5, etc.
    grade_patterns = [
        r'\bK-12\b', r'\bK-\d+\b', r'\d+-\d+.*grade', r'\bKindergarten\b',
        r'\b\d+(?:st|nd|rd|th)\s+grade\b', r'\b\d+(?:st|nd|rd|th)\b',
    ]
    for pattern in grade_patterns:
        matches = re.findall(pattern, query, re.IGNORECASE)
        patterns.extend(matches)
    
    # School type patterns: Virtual, Traditional, Charter, etc.
    school_types = ['Virtual', 'Traditional', 'Charter', 'Public', 'Private']
    for st in school_types:
        if st.lower() in query.lower():
            patterns.append(st)
    
    # County/City patterns (simple: Capitalized words that likely are proper nouns)
    # Extract capitalized multi-word phrases likely to be place names
    multi_word_places = re.findall(r'\b([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b', query)
    patterns.extend(multi_word_places)
    
    # Single-word places that precede "County", "City", etc.
    place_keywords = re.findall(r'\b([A-Z][a-z]+)\s+(?:County|City|District|State)\b', query)
    patterns.extend(place_keywords)
    
    # Filter: remove obvious non-entities (stop words masquerading as entities)
    non_entities = {'Given', 'List', 'Give', 'Among', 'Please', 'Indicate', 'What', 'Which', 'Where', 'How', 'Tell', 'Provide', 'Show', 'Return', 'Between'}
    patterns = [p for p in patterns if p not in non_entities]
    
    return list(set(patterns))  # unique

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

    applied_mappings = _rule_based_query_mappings(query)
    entities = extract_entities(query)
    known_tables = set(list_user_tables())

    pre_mapped_entities = {str(mapping.get("original", "")).lower() for mapping in applied_mappings}

    for entity in entities:
        if entity.lower() in pre_mapped_entities:
            continue

        if len(entity) < 1:  # Allow single-char entities (e.g., 'F' for 'Virtual=F')
            continue

        direct_exact = _direct_exact_schema_mapping(query, entity)
        if direct_exact is not None:
            applied_mappings.append(direct_exact)
            continue

        query_embedding = get_embedding(entity)
        if not query_embedding:
            continue

        # Query top-k candidates and select the first plausible semantic match.
        results = collection.query(query_embeddings=[query_embedding], n_results=3)

        candidates = []
        if results.get("distances") and results.get("metadatas"):
            candidates = list(zip(results["distances"][0], results["metadatas"][0]))

        selected = None
        for candidate_distance, metadata in candidates:
            canonical_value = _norm_text(metadata.get("canonical"))
            candidate_column = _norm_text(metadata.get("column"))
            if not canonical_value:
                continue

            if canonical_value.lower() == entity.lower():
                continue

            if not _column_context_compatible(query, candidate_column):
                continue

            if _is_plausible_vector_mapping(entity, canonical_value, candidate_distance):
                adjusted_distance = candidate_distance - _column_context_score(query, candidate_column)
                if selected is None or adjusted_distance < selected[0]:
                    selected = (adjusted_distance, candidate_distance, metadata)

        if selected is not None:
            _, distance, metadata = selected
            applied_mappings.append(
                {
                    "original": entity,
                    "grounded": metadata["canonical"],
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

                if not _column_context_compatible(query, column):
                    print(
                        f"Skipped fallback mapping for '{entity}': column '{table}.{column}' conflicts with query context."
                    )
                    continue

                if not _is_plausible_fallback_mapping(entity, canonical):
                    print(
                        f"Skipped fallback mapping for '{entity}': canonical '{canonical}' is weakly related."
                    )
                    continue

                is_exact_match = value_exists_in_column(table, column, canonical)

                if not is_exact_match:
                    print(
                        f"Skipped fallback hint for '{entity}': '{canonical}' is not an exact value in {table}.{column}."
                    )
                    continue
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

    return query, _dedupe_mappings(applied_mappings)
