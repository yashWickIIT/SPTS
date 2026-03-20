import os
import hashlib
import math
import threading

from fastembed import TextEmbedding

DEFAULT_EMBEDDING_MODEL = os.getenv("SPTS_EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
FALLBACK_EMBEDDING_DIM = int(os.getenv("SPTS_FALLBACK_EMBEDDING_DIM", "384"))

_model = None
_model_error = None
_model_lock = threading.Lock()


def _load_model():
    global _model, _model_error

    if _model is not None:
        return _model

    if _model_error is not None:
        return None

    with _model_lock:
        if _model is not None:
            return _model
        if _model_error is not None:
            return None

        try:
            print(f"Loading Embedding Model ({DEFAULT_EMBEDDING_MODEL})...")
            _model = TextEmbedding(model_name=DEFAULT_EMBEDDING_MODEL)
            print("Model loaded successfully!")
        except Exception as exc:
            _model_error = exc
            print(
                "Warning: could not load fastembed model "
                f"'{DEFAULT_EMBEDDING_MODEL}'. Falling back to deterministic local embeddings. "
                f"Reason: {exc}"
            )

    return _model


def _fallback_embedding(text: str, dim: int = FALLBACK_EMBEDDING_DIM):
    if not text:
        return None

    values = []
    counter = 0
    while len(values) < dim:
        digest = hashlib.sha256(f"{text}|{counter}".encode("utf-8")).digest()
        for index in range(0, len(digest), 4):
            chunk = digest[index:index + 4]
            if len(chunk) < 4:
                continue
            integer = int.from_bytes(chunk, byteorder="big", signed=False)
            values.append((integer / 4294967295.0) * 2.0 - 1.0)
            if len(values) >= dim:
                break
        counter += 1

    norm = math.sqrt(sum(value * value for value in values))
    if norm > 0:
        values = [value / norm for value in values]

    return values

def get_embedding(text: str):
    """
    Takes a string and converts it into a 384-dimensional dense vector.
    
    Args:
        text (str): The text value from the database or user query.
        
    Returns:
        list: A list of floats representing the semantic vector.
    """
    text_value = (text or "").strip()
    if not text_value:
        return None

    model = _load_model()
    if model is None:
        return _fallback_embedding(text_value)

    try:
        embedding = next(model.embed([text_value]))
        return embedding.tolist()
    except Exception as e:
        print(f"Error generating embedding for '{text}': {e}")
        return _fallback_embedding(text_value)

def get_embeddings_batch(texts: list):
    """
    Efficiently generates embeddings for a list of strings at once.
    Highly recommended when profiling large databases.
    """
    cleaned_texts = [str(item).strip() for item in texts if str(item).strip()]
    if not cleaned_texts:
        return []

    model = _load_model()
    if model is None:
        return [_fallback_embedding(item) for item in cleaned_texts]

    try:
        embeddings = list(model.embed(cleaned_texts))
        return [embedding.tolist() for embedding in embeddings]
    except Exception as e:
        print(f"Error generating batch embeddings: {e}")
        return [_fallback_embedding(item) for item in cleaned_texts]

# --- Quick Test Block ---
if __name__ == "__main__":
    # Test the embedding generation
    sample_text = "Los Angeles Unified School District"
    vector = get_embedding(sample_text)
    
    print(f"\nTest String: '{sample_text}'")
    if vector is None:
        print("No vector generated.")
    else:
        print(f"Generated Vector Length: {len(vector)} dimensions")
        print(f"Preview of first 5 values: {vector[:5]}")