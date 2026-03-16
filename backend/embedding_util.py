import os
from sentence_transformers import SentenceTransformer

# Initialize the model globally so it only loads into memory once.
# 'all-MiniLM-L6-v2' is highly optimized: it's incredibly fast, lightweight (~80MB), 
# and provides excellent semantic representations for general text.
print("Loading Embedding Model (all-MiniLM-L6-v2)...")
model = SentenceTransformer('all-MiniLM-L6-v2')
print("Model loaded successfully!")

def get_embedding(text: str):
    """
    Takes a string and converts it into a 384-dimensional dense vector.
    
    Args:
        text (str): The text value from the database or user query.
        
    Returns:
        list: A list of floats representing the semantic vector.
    """
    try:
        # Generate the embedding
        embedding = model.encode(text)
        
        # Convert the numpy array to a standard Python list 
        # (This makes it compatible with JSON and Vector Databases like Chroma/FAISS)
        return embedding.tolist()
    except Exception as e:
        print(f"Error generating embedding for '{text}': {e}")
        return None

def get_embeddings_batch(texts: list):
    """
    Efficiently generates embeddings for a list of strings at once.
    Highly recommended when profiling large databases.
    """
    try:
        embeddings = model.encode(texts)
        return embeddings.tolist()
    except Exception as e:
        print(f"Error generating batch embeddings: {e}")
        return []

# --- Quick Test Block ---
if __name__ == "__main__":
    # Test the embedding generation
    sample_text = "Los Angeles Unified School District"
    vector = get_embedding(sample_text)
    
    print(f"\nTest String: '{sample_text}'")
    print(f"Generated Vector Length: {len(vector)} dimensions")
    print(f"Preview of first 5 values: {vector[:5]}") 