import argparse
import json
import os
import random

INPUT_JSON = os.path.join("data", "mini_dev_sqlite.json")
OUTPUT_JSON = os.path.join("data", "bird_dev_sample.json")
DEFAULT_SOURCE_DB_ID = "california_schools"
DEFAULT_TARGET_DB_ID = "bird_mini_dev"
DEFAULT_SAMPLE_SIZE = 50
DEFAULT_SEED = 42

# Hugging Face dataset IDs to try in order (streaming — no local cache written)
HF_DATASET_CANDIDATES = [
    ("xlangai/BIRD", "dev"),
    ("premai-io/birdbench", "dev"),
]


def _normalize_records(records, source_db_id: str, target_db_id: str):
    cleaned = []
    for item in records:
        db_id = item.get("db_id")
        question = item.get("question") or item.get("Question")
        sql = item.get("SQL") or item.get("sql") or item.get("query")
        evidence = item.get("evidence", "")
        difficulty = item.get("difficulty", "simple")

        if db_id != source_db_id or not question or not sql:
            continue

        cleaned.append(
            {
                "question_id": item.get("question_id"),
                "db_id": target_db_id,
                "question": question,
                "evidence": evidence,
                "SQL": sql,
                "difficulty": difficulty,
            }
        )
    return cleaned


def _load_records_from_local_json(input_json: str):
    if not os.path.exists(input_json):
        print(f"Error: source JSON not found: {input_json}")
        return []

    try:
        with open(input_json, "r", encoding="utf-8") as file_obj:
            return json.load(file_obj)
    except Exception as error:
        print(f"Error reading source JSON: {error}")
        return []


def _load_records_from_hf(source_db_id: str) -> list:
    """Stream records from Hugging Face — nothing is saved to disk."""
    try:
        from datasets import load_dataset  # type: ignore[import-untyped]
    except ImportError:
        print("Warning: 'datasets' package not installed. Run: pip install datasets>=2.19.0")
        return []

    for hf_id, split in HF_DATASET_CANDIDATES:
        try:
            print(f"  Trying HF dataset '{hf_id}' split='{split}' (streaming)…")
            ds = load_dataset(hf_id, split=split, streaming=True, trust_remote_code=False)
            records = [row for row in ds if row.get("db_id") == source_db_id]  # type: ignore[union-attr]
            if records:
                print(f"  Found {len(records)} records for '{source_db_id}' in '{hf_id}'.")
                return records
        except Exception as err:
            print(f"  HF dataset '{hf_id}' unavailable: {err}")

    return []


def get_official_data(
    input_json: str = INPUT_JSON,
    source_db_id: str = DEFAULT_SOURCE_DB_ID,
    target_db_id: str = DEFAULT_TARGET_DB_ID,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    seed: int = DEFAULT_SEED,
    output_json: str = OUTPUT_JSON,
) -> bool:
    """Build bird_dev_sample.json.

    Load order:
    1. Hugging Face (streaming — no local cache written).
    2. Local JSON fallback (``input_json``).
    """
    os.makedirs(os.path.dirname(output_json), exist_ok=True)

    # --- 1. Try HF streaming first ---
    records = _load_records_from_hf(source_db_id)

    # --- 2. Fall back to local JSON ---
    if not records:
        print(f"  Falling back to local JSON: {input_json}")
        records = _load_records_from_local_json(input_json)

    if not records:
        print("Error: failed to load BIRD records from HF streaming or local JSON.")
        return False

    cleaned = _normalize_records(records, source_db_id=source_db_id, target_db_id=target_db_id)
    if not cleaned:
        print(f"Error: could not find queries for db_id='{source_db_id}'.")
        return False

    effective_sample_size = max(1, min(sample_size, len(cleaned)))
    rng = random.Random(seed)
    sample = rng.sample(cleaned, effective_sample_size)

    with open(output_json, "w", encoding="utf-8") as file_obj:
        json.dump(sample, file_obj, indent=4)

    print(
        f"Success! Saved {effective_sample_size} queries for '{source_db_id}' to {output_json}"
    )
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Build bird_dev_sample.json from an existing local BIRD mini-dev JSON file."
    )
    parser.add_argument(
        "--input",
        default=INPUT_JSON,
        help="Path to a local mini_dev_sqlite.json file.",
    )
    parser.add_argument(
        "--source-db-id",
        default=DEFAULT_SOURCE_DB_ID,
        help="Original db_id in official BIRD records (e.g., california_schools).",
    )
    parser.add_argument(
        "--target-db-id",
        default=DEFAULT_TARGET_DB_ID,
        help="db_id written into bird_dev_sample.json (matches your local sqlite name).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help="Number of records to sample.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Random seed for deterministic sampling.",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_JSON,
        help="Output JSON path.",
    )
    args = parser.parse_args()

    success = get_official_data(
        input_json=args.input,
        source_db_id=args.source_db_id,
        target_db_id=args.target_db_id,
        sample_size=args.sample_size,
        seed=args.seed,
        output_json=args.output,
    )
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
