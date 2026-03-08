import json
import os
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KG_PATH = os.path.join(BASE_DIR, "..", "kg", "vlkg.json")

if os.path.exists(KG_PATH):
    with open(KG_PATH) as f:
        VLKG = json.load(f)
else:
    print(f"Warning: Knowledge Graph not found at {KG_PATH}")
    VLKG = {}


def ground_query(query: str):
    grounded_query = query
    applied_mappings = []
    # Simple tokenization
    query_lower = query.lower()
    sorted_keys = sorted(VLKG.keys(), key=len, reverse=True)

    for token in sorted_keys:
        pattern = r"\b" + re.escape(token) + r"\b"

        if re.search(pattern, query_lower):
            mapping = VLKG[token][0]
            canonical_val = mapping["canonical"]

            grounded_query = re.sub(
                pattern, f"'{canonical_val}'", grounded_query, flags=re.IGNORECASE
            )

            applied_mappings.append(
                {
                    "original": token,
                    "grounded": canonical_val,
                    "type": "Entity Resolution",
                }
            )

            query_lower = re.sub(pattern, "---", query_lower)

    return grounded_query, applied_mappings
