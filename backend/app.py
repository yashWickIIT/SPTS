import os
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from grounding import ground_query
from text_to_sql import baseline_text_to_sql, spts_text_to_sql, fix_sql_with_llm
from database import execute_sql
from db_users import create_user, get_user_by_username
from auth import verify_password, get_password_hash, create_access_token, get_current_user, ACCESS_TOKEN_EXPIRE_MINUTES
from datetime import timedelta

app = FastAPI()

# Enabling CORS
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

class UserCreate(BaseModel):
    username: str
    password: str

@app.post("/register")
def register(user: UserCreate):
    hashed_password = get_password_hash(user.password)
    success = create_user(user.username, hashed_password)
    if not success:
        raise HTTPException(status_code=400, detail="Username already registered")
    return {"message": "User created successfully"}

@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = get_user_by_username(form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/query")
def query(payload: dict, current_user: dict = Depends(get_current_user)):
    user_query = payload["query"]
    
    # FIX: Use '_' to ignore the returned query string since we only need the mappings now
    _, mappings = ground_query(user_query)
    
    baseline_sql = baseline_text_to_sql(user_query)
    
    # We pass the original untouched user_query, plus our new Vector DB hints
    spts_sql = spts_text_to_sql(user_query, mappings)

    baseline_result = execute_sql(baseline_sql)
    spts_result = execute_sql(spts_sql)

    # 1-pass auto-correction loop for SPTS
    if not spts_result["success"]:
        spts_sql = fix_sql_with_llm(user_query, spts_sql, spts_result["error"], mappings)
        spts_result = execute_sql(spts_sql)

    # Safely format result for frontend compatibility (`app.js` expects arrays)
    def format_res(res):
        if res["success"]:
            return res["data"]
        return [(res["error"],)]

    return {
        "baseline_sql": baseline_sql,
        "baseline_result": format_res(baseline_result),
        "spts_sql": spts_sql,
        "spts_result": format_res(spts_result),
        "mappings": mappings,
    }