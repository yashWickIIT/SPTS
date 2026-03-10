import os
import sqlite3
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "bird_mini_dev.sqlite")

client = Groq(api_key=os.getenv("API_KEY"))

def get_schema_summary():
    if not os.path.exists(DB_PATH):
        return "Error: Database not found."

    schema_str = ""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cursor.fetchall() if r[0] != 'sqlite_sequence']

        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            col_desc = ", ".join([f"{c[1]} ({c[2]})" for c in columns])
            schema_str += f"Table: {table}\nColumns: {col_desc}\n"
            
            cursor.execute(f"PRAGMA foreign_key_list({table})")
            fks = cursor.fetchall()
            if fks:
                schema_str += "Foreign Keys:\n"
                for fk in fks:
                    target_table = fk[2]
                    source_column = fk[3]
                    target_column = fk[4]
                    schema_str += f"  - {table}.{source_column} references {target_table}.{target_column}\n"
            
            schema_str += "\n"

        conn.close()
    except Exception as e:
        schema_str = f"Error reading schema: {str(e)}"

    return schema_str

import time

def generate_sql_with_llm(user_query, mode="Baseline", mappings=None):
    schema_context = get_schema_summary()

    system_prompt = """
    You are a SQL expert. Output ONLY valid SQLite code. No Markdown.
    RULES:
    1. Always use parenthesis for aggregation functions, e.g., COUNT(*).
    2. Do not explain your answer. Just output the SQL.
    """

    user_prompt = f"Schema:\n{schema_context}\nQuestion: {user_query}"
    
    injected_context_str = "None"

    if mode == "SPTS":
        injected_context_str = "DATABASE HINTS:\n"
        user_prompt += "\n\nDATABASE HINTS (Use these canonical values for exact matching in your WHERE clauses):"
        if mappings and len(mappings) > 0:
            for mapping in mappings:
                hint = f"- The user's term '{mapping['original']}' maps to the exact database value '{mapping['grounded']}' in the `{mapping['table']}.{mapping['column']}` column."
                user_prompt += f"\n{hint}"
                injected_context_str += f"{hint}\n"
        else:
            hint = "- No semantic hints found. Rely strictly on the schema."
            user_prompt += f"\n{hint}"
            injected_context_str += f"{hint}\n"

    try:
        start_time = time.time()
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )
        latency = (time.time() - start_time) * 1000  # in ms
        
        sql = completion.choices[0].message.content.replace("```sql", "").replace("```", "").strip()
        tokens = {
            "prompt_tokens": completion.usage.prompt_tokens,
            "completion_tokens": completion.usage.completion_tokens,
            "total_tokens": completion.usage.total_tokens
        }
        
        return {
            "sql": sql,
            "rationale": {
                "system_prompt": system_prompt.strip(),
                "injected_context": injected_context_str.strip(),
                "latency_ms": round(latency, 2),
                "token_usage": tokens
            }
        }
    except Exception as e:
        return {
            "sql": f"SELECT * FROM error; -- API Error: {str(e)}",
            "rationale": {
                "system_prompt": system_prompt.strip(),
                "injected_context": injected_context_str.strip(),
                "latency_ms": 0,
                "token_usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            }
        }

def baseline_text_to_sql(user_query):
    return generate_sql_with_llm(user_query, mode="Baseline")

def spts_text_to_sql(user_query, mappings=None):
    # Pass the mappings through to the LLM generator
    return generate_sql_with_llm(user_query, mode="SPTS", mappings=mappings)

def fix_sql_with_llm(user_query, bad_sql, error_message, mappings=None):
    schema_context = get_schema_summary()

    system_prompt = """
    You are a SQL debugging expert. The user's previous SQL query failed. 
    Output ONLY the corrected, valid SQLite code. No Markdown.
    RULES:
    1. Always use parenthesis for aggregation functions, e.g., COUNT(*).
    2. Do not explain your answer or include any text other than the SQL.
    3. Fix the specific SQLite error provided.
    """

    user_prompt = f"Schema:\n{schema_context}\n\nQuestion: {user_query}\n\nFailed SQL:\n{bad_sql}\n\nSQLite Error:\n{error_message}\n\nPlease provide the corrected SQL."

    if mappings and len(mappings) > 0:
        user_prompt += "\n\nDATABASE HINTS (Use these canonical values for exact matching in your WHERE clauses):"
        for mapping in mappings:
            user_prompt += f"\n- The user's term '{mapping['original']}' maps to the exact database value '{mapping['grounded']}' in the `{mapping['table']}.{mapping['column']}` column."

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
        return f"SELECT * FROM error; -- API Error: {str(e)}"