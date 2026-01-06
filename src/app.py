import streamlit as st
import json
from groq import Groq

API_KEY = "your_key"
client = Groq(api_key=API_KEY)

try:
    with open("semantic_map.json", "r") as f:
        SEMANTIC_MAP = json.load(f)
except FileNotFoundError:
    SEMANTIC_MAP = {}


def get_sql_from_llm(question, use_map=False):
    # Schema Definition (Update based on check_db.py)
    schema_info = "Table: schools, Columns: County"
    system_msg = "You are a SQL expert. Output ONLY valid SQLite code. No markdown."

    # Base Prompt
    user_msg = f"""
    Convert this question into a SQL query.
    Schema: {schema_info}
    Question: {question}
    """

    # SPTS Logic
    if use_map and SEMANTIC_MAP:
        matches = []
        for dirty, clean in SEMANTIC_MAP.items():
            if dirty.lower() in question.lower():
                matches.append(f"- '{dirty}' refers to '{clean}'")

        if matches:
            user_msg += "\n\nCRITICAL DATA MAPPINGS:\n" + "\n".join(matches)
            user_msg += "\n(Use the mapped values in your WHERE clauses)"

    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
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
        return f"Error: {str(e)}"


# --- UI ---
st.title("SPTS: Robust Text-to-SQL (Powered by Groq)")
st.caption("Final Year Project Prototype")

question = st.text_input("Enter your query:", "Show me districts in N.Y.")

if st.button("Generate SQL"):
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🟧 Standard")
        st.code(get_sql_from_llm(question, use_map=False), language="sql")

    with col2:
        st.subheader("🟩 SPTS")
        st.code(get_sql_from_llm(question, use_map=True), language="sql")
