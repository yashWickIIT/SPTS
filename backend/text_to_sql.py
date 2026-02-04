import os
import sqlite3
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "bird_mini_dev.sqlite")

client = Groq(api_key=os.getenv("API_KEY"))


def get_schema_summary():
    """
    Dynamically fetches table and column info from the database.
    This ensures the LLM always knows the actual DB structure.
    """
    if not os.path.exists(DB_PATH):
        return "Error: Database not found."

    schema_str = ""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schools';"
        )
        tables = [r[0] for r in cursor.fetchall()]

        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            col_desc = ", ".join([f"{c[1]} ({c[2]})" for c in columns])
            schema_str += f"Table: {table}\nColumns: {col_desc}\n\n"

        conn.close()
    except Exception as e:
        schema_str = f"Error reading schema: {str(e)}"

    return schema_str


def generate_sql_with_llm(user_query, mode="Baseline"):
    schema_context = get_schema_summary()

    system_prompt = """
    You are a SQL expert. Output ONLY valid SQLite code. No Markdown.
    RULES:
    1. Always use parenthesis for aggregation functions, e.g., COUNT(*), not COUNT School.
    2. Do not explain your answer. Just output the SQL.
    """

    user_prompt = f"Schema:\n{schema_context}\nQuestion: {user_query}"

    if mode == "SPTS":
        user_prompt += "\nNOTE: Use the exact string values provided in the question for WHERE clauses. Do not infer abbreviations."

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
