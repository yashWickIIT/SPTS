import os
import sqlite3
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "bird_mini_dev.sqlite")
client = Groq(api_key=os.getenv("API_KEY"))

def get_database_schema():
    """
    Dynamically extracts the schema to feed into the LLM context.
    """
    if not os.path.exists(DB_PATH):
        return "Error: Database file not found."
        
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        schema_str = ""
        for table_name in tables:
            t_name = table_name[0]
            cursor.execute(f"PRAGMA table_info({t_name})")
            columns = cursor.fetchall()
            col_names = [f"{col[1]} ({col[2]})" for col in columns]
            schema_str += f"Table: {t_name}\nColumns: {', '.join(col_names)}\n\n"
            
        conn.close()
        return schema_str
    except Exception as e:
        return f"Error loading schema: {e}"

# Load schema once at startup
SCHEMA_CONTEXT = get_database_schema()

def generate_sql_with_llm(user_query, mode="Baseline"):
    system_prompt = f"""
    You are a SQL expert. Output ONLY valid SQLite code.
    
    DATABASE SCHEMA:
    {SCHEMA_CONTEXT}
    
    Rules:
    1. Do not use Markdown (no ```sql).
    2. Output the SQL string directly.
    3. Use 'COUNT(*)' for counting.
    """
    
    user_prompt = f"Question: {user_query}"
    
    if mode == "SPTS":
        user_prompt += "\nIMPORTANT: The query is already GROUNDED. Use the exact string values provided in the question for the WHERE clause."

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0
        )
        return completion.choices[0].message.content.replace("```sql", "").replace("```", "").strip()
    except Exception as e:
        return f"Error: {e}"

def baseline_text_to_sql(query: str):
    return generate_sql_with_llm(query, mode="Baseline")

def spts_text_to_sql(grounded_query: str):
    return generate_sql_with_llm(grounded_query, mode="SPTS")