import json
import os

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
    tokens = query.lower().replace("?", "").replace(",", "").split()

    for token in tokens:
        # Check if this token exists in Value-Level Knowledge Graph
        if token in VLKG:
            mapping = VLKG[token][0] 
            canonical_val = mapping["canonical"]
            
            grounded_query = grounded_query.replace(token, f"'{canonical_val}'")
            
            applied_mappings.append({
                "original": token,
                "grounded": canonical_val,
                "type": "Entity Resolution"
            })

    return grounded_query, applied_mappings