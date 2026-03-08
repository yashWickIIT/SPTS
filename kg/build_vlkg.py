import sqlite3
import json
import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "bird_mini_dev.sqlite")
OUTPUT_PATH = os.path.join(BASE_DIR, "vlkg.json")

API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)


def generate_synonyms(value, column_context):
    """
    Context-Aware Synonym Generation.
    """
    prompt = f"""
    Context: Database Column '{column_context}'.
    Value: "{value}".
    Task: Generate 3 likely user abbreviations, slang, or variations for this value.
    Example: "Los Angeles Unified" -> ["LAUSD", "LA Unified", "L.A. Schools"]
    Output ONLY JSON object with key 'synonyms'. Do not add markdown blocks.
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
        return data.get("synonyms", [])
    except Exception as e:
        print(f"   [!] Error generating synonyms for '{value}': {e}")
        return []


def build_vlkg():
    if not os.path.exists(os.path.dirname(OUTPUT_PATH)):
        os.makedirs(os.path.dirname(OUTPUT_PATH))

    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schools';"
    )
    tables = [r[0] for r in cursor.fetchall()]

    graph = {}
    print(f"SPTS Profiler: Scanning {len(tables)} tables for text columns...")

    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        text_cols = ["District", "City", "County"]

        for col in text_cols:
            print(f"   Profiling {table}.{col}...")

            cursor.execute(
                f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT 15'
            )
            values = [row[0] for row in cursor.fetchall()]

            for canonical in values:
                aliases = generate_synonyms(canonical, col)

                clean_words = [w for w in canonical.split() if w.isalnum()]
                if len(clean_words) > 1:
                    initials = "".join(w[0] for w in clean_words).lower()
                    if len(initials) > 1:
                        aliases.append(initials)

                for alias in aliases:
                    clean_alias = alias.lower().strip()
                    if clean_alias not in graph:
                        graph[clean_alias] = []

                    if not any(
                        entry["canonical"] == canonical for entry in graph[clean_alias]
                    ):
                        graph[clean_alias].append(
                            {"canonical": canonical, "table": table, "column": col}
                        )

    conn.close()

    with open(OUTPUT_PATH, "w") as f:
        json.dump(graph, f, indent=2)

    print(f"Value-Level Knowledge Graph saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    build_vlkg()
