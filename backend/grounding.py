import os
import re
import json
import chromadb
from groq import Groq
from dotenv import load_dotenv
from embedding_util import get_embedding

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHROMA_PATH = os.path.join(BASE_DIR, "..", "kg", "chroma_db")

print("Initializing ChromaDB connection in grounding...")
try:
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    collection = chroma_client.get_collection(name="spts_vlkg")
    print("Successfully connected to Vector DB.")
except Exception as e:
    print(f"Warning: ChromaDB collection 'spts_vlkg' not found. Error: {e}")
    collection = None

client = Groq(api_key=os.getenv("API_KEY"))

def extract_entities(query: str):
    prompt = f"""
    Extract the key specific entities, names, locations, or categorical values from the following user query. 
    Ignore general words like 'how many', 'show me', 'count', 'database', etc.
    Query: "{query}"
    Output ONLY a JSON object with a list of strings under the key 'entities'. Do not include markdown formatting.
    """
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content
        data = json.loads(content)
        return data.get("entities", [])
    except Exception as e:
        print(f"Error extracting entities: {e}")
        return []

def ground_query(query: str):
    if not collection:
        return query, []

    applied_mappings = []
    entities = extract_entities(query)
    
    for entity in entities:
        if len(entity) < 2:
            continue
            
        query_embedding = get_embedding(entity)
        if not query_embedding:
            continue

        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=1
        )
        
        if results['distances'] and len(results['distances'][0]) > 0:
            distance = results['distances'][0][0]
            metadata = results['metadatas'][0][0]
            
            THRESHOLD = 1.0 
            
            if distance < THRESHOLD:
                canonical_val = metadata['canonical']
                table = metadata['table']
                column = metadata['column']
                
                if canonical_val.lower() != entity.lower():
                    # NO MORE REGEX REPLACEMENT HERE!
                    # We simply record the reasoning to pass to the LLM as a hint.
                    applied_mappings.append({
                        "original": entity,
                        "grounded": canonical_val,
                        "table": table,
                        "column": column,
                        "distance": round(distance, 4),
                        "type": "Vector Semantic Match"
                    })
                    
    # Return the untouched user query and the list of hints
    return query, applied_mappings