import json
import os
import urllib.request
import zipfile
import random

OFFICIAL_MINIDEV_ZIP_URL = "https://bird-bench.oss-cn-beijing.aliyuncs.com/minidev.zip"
ZIP_PATH = os.path.join("data", "minidev.zip")
INNER_JSON = "minidev/MINIDEV/mini_dev_sqlite.json"
OUTPUT_JSON = os.path.join("data", "bird_dev_sample.json")
TARGET_DB_NAME = "bird_mini_dev"


def get_official_data():
    if not os.path.exists(ZIP_PATH):
        os.makedirs(os.path.dirname(ZIP_PATH), exist_ok=True)
        print("Downloading official BIRD benchmark questions (this takes a moment)...")
        try:
            urllib.request.urlretrieve(OFFICIAL_MINIDEV_ZIP_URL, ZIP_PATH)
        except Exception as e:
            print(f"Error downloading file: {e}")
            return

    print("Extracting test questions...")
    try:
        with zipfile.ZipFile(ZIP_PATH, "r") as archive:
            with archive.open(INNER_JSON) as f:
                records = json.load(f)
    except Exception as e:
        print(f"Error reading zip file: {e}")
        return

    cleaned = []
    for item in records:
        if (
            item.get("db_id") == "california_schools"
            and item.get("question")
            and item.get("SQL")
        ):
            cleaned.append(
                {
                    "question_id": item.get("question_id"),
                    "db_id": TARGET_DB_NAME,
                    "question": item.get("question"),
                    "evidence": item.get("evidence", ""),
                    "SQL": item.get("SQL"),
                    "difficulty": item.get("difficulty", "simple"),
                }
            )

    if not cleaned:
        print(
            "Error: Could not find 'california_schools' questions in the downloaded data."
        )
        return

    sample_size = min(50, len(cleaned))
    sample = random.sample(cleaned, sample_size)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(sample, f, indent=4)

    print(f"\nSuccess! Saved {sample_size} official questions to {OUTPUT_JSON}")
    print("Your data is clean. You are officially ready to evaluate.")


if __name__ == "__main__":
    get_official_data()
