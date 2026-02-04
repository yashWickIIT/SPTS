import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("API_KEY"))

SCHEMA_CONTEXT = """
Table: schools
Columns: School (text), District (text), County (text), City (text), Magnet (boolean)
"""


def generate_sql_with_llm(user_query, mode="Baseline"):
    system_prompt = "You are a SQL expert. Output ONLY valid SQLite code. No Markdown."
    user_prompt = f"Schema: {SCHEMA_CONTEXT}\nQuestion: {user_query}"

    if mode == "SPTS":
        user_prompt += "\nNOTE: Use the exact string values from the question for WHERE clauses. Do not hallucinates abbreviations."

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )
        return (
            completion.choices[0]
            .message.content.replace("```sql", "")
            .replace("```", "")
            .strip()
        )
    except Exception as e:
        return f"SELECT * FROM schools WHERE 1=0; -- Error: {e}"


def baseline_text_to_sql(query: str):
    return generate_sql_with_llm(query, mode="Baseline")


def spts_text_to_sql(grounded_query: str):
    return generate_sql_with_llm(grounded_query, mode="SPTS")
