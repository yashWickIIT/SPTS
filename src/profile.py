import pandas as pd
from groq import Groq
import json
import time
import os

API_KEY = "your_key"
client = Groq(api_key=API_KEY)


def build_semantic_map():
    if not os.path.exists("dirty_values.csv"):
        print("Error: 'dirty_values.csv' not found.")
        return

    df = pd.read_csv("dirty_values.csv")
    col_name = "County"
    if "County" not in df.columns:
        col_name = df.columns[0]
    dirty_list = df[col_name].tolist()
    system_prompt = "You are a data cleaning assistant. Output ONLY valid JSON."
    user_prompt = f"""
    I have a list of database values: {dirty_list}
    
    Identify synonyms and group them under a single canonical (standard) name.
    The keys should be the dirty value, and the value should be the clean standard value.
    
    Example output format:
    {{
      "NY": "New York",
      "Calif": "California"
    }}
    """

    print("Asking Groq (Llama 3) to normalize data...")

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            response_format={"type": "json_object"},
        )

        clean_json = completion.choices[0].message.content

        # Save results
        with open("semantic_map.json", "w") as f:
            f.write(clean_json)

        print("✅ Success! Semantic Map saved using Groq.")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    build_semantic_map()
