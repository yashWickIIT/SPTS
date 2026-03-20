import time

from groq import Groq
try:
    from .config import API_KEY
    from .db_client import (
        get_main_dialect_name,
        get_table_columns,
        get_table_foreign_keys,
        list_user_tables,
    )
except ImportError:
    from config import API_KEY
    from db_client import (
        get_main_dialect_name,
        get_table_columns,
        get_table_foreign_keys,
        list_user_tables,
    )

_groq_client = None


def _get_groq_client():
    global _groq_client
    if _groq_client is not None:
        return _groq_client

    if not API_KEY:
        return None

    try:
        _groq_client = Groq(api_key=API_KEY)
        return _groq_client
    except Exception:
        return None

def get_schema_summary():
    schema_str = ""
    try:
        tables = list_user_tables()

        for table in tables:
            columns = get_table_columns(table)
            col_desc = ", ".join([f"{c['name']} ({c['type']})" for c in columns])
            schema_str += f"Table: {table}\nColumns: {col_desc}\n"

            fks = get_table_foreign_keys(table)
            if fks:
                schema_str += "Foreign Keys:\n"
                for fk in fks:
                    target_table = fk.get("referred_table", "unknown_table")
                    source_cols = fk.get("constrained_columns", [])
                    target_cols = fk.get("referred_columns", [])
                    for source_col, target_col in zip(source_cols, target_cols):
                        schema_str += f"  - {table}.{source_col} references {target_table}.{target_col}\n"

            schema_str += "\n"
    except Exception as e:
        schema_str = f"Error reading schema: {str(e)}"

    return schema_str

def generate_sql_with_llm(user_query, mode="Baseline", mappings=None):
    client = _get_groq_client()
    if client is None:
        return {
            "sql": "SELECT * FROM error; -- API Error: Missing API_KEY/GROQ_API_KEY",
            "rationale": {
                "system_prompt": "",
                "injected_context": "",
                "latency_ms": 0,
                "token_usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            }
        }

    schema_context = get_schema_summary()
    sql_dialect = get_main_dialect_name()

    system_prompt = """
    You are a SQL expert. Output ONLY valid SQL code for the configured database dialect: {sql_dialect}. No Markdown.
    RULES:
    1. Always use parenthesis for aggregation functions, e.g., COUNT(*).
    2. Do not explain your answer. Just output the SQL.
    3. Never invent literal filter values (e.g., city/county/year/status names) that are not explicitly present in the question.
    4. If the question asks for global totals/averages (e.g., "across all", "in the database", no specific entity), do not add WHERE filters.
    5. In SPTS mode, use mapping hints only when they correspond to explicit user-mentioned entities; otherwise ignore them.
    """.format(sql_dialect=sql_dialect)

    user_prompt = f"Schema:\n{schema_context}\nQuestion: {user_query}"
    
    injected_context_str = "None"

    if mode == "SPTS":
        injected_context_str = "DATABASE HINTS:\n"
        user_prompt += "\n\nDATABASE HINTS (Use these only for explicit entities in the question; do NOT add extra constraints):"
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
    client = _get_groq_client()
    if client is None:
        return "SELECT * FROM error; -- API Error: Missing API_KEY/GROQ_API_KEY"

    schema_context = get_schema_summary()
    sql_dialect = get_main_dialect_name()

    system_prompt = """
    You are a SQL debugging expert. The user's previous SQL query failed. 
    Output ONLY the corrected, valid SQL code for the configured database dialect: {sql_dialect}. No Markdown.
    RULES:
    1. Always use parenthesis for aggregation functions, e.g., COUNT(*).
    2. Do not explain your answer or include any text other than the SQL.
    3. Fix the specific database error provided.
    """.format(sql_dialect=sql_dialect)

    user_prompt = f"Schema:\n{schema_context}\n\nQuestion: {user_query}\n\nFailed SQL:\n{bad_sql}\n\nDatabase Error:\n{error_message}\n\nPlease provide the corrected SQL."

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