import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from grounding import ground_query
from text_to_sql import baseline_text_to_sql, spts_text_to_sql
from database import execute_sql

app = FastAPI()

#Enabling CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "..", "frontend")

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

@app.get("/")
async def read_root():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

@app.post("/query")
def query(payload: dict):
    user_query = payload["query"]
    grounded_query, mappings = ground_query(user_query)
    
    baseline_sql = baseline_text_to_sql(user_query)
    spts_sql = spts_text_to_sql(grounded_query)

    baseline_result = execute_sql(baseline_sql)
    spts_result = execute_sql(spts_sql)

    return {
        "baseline_sql": baseline_sql,
        "baseline_result": baseline_result,
        "spts_sql": spts_sql,
        "spts_result": spts_result,
        "mappings": mappings
    }