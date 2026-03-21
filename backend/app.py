import os
import sys
from typing import Annotated
from fastapi import FastAPI, Depends, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from datetime import timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

try:
    from . import grounding
    from .text_to_sql import baseline_text_to_sql, spts_text_to_sql, fix_sql_with_llm
    from .database import execute_sql
    from .db_users import ALLOWED_ROLES, create_user, get_user_by_username, normalize_role
    from .auth import verify_password, get_password_hash, create_access_token, get_current_user, require_roles, ACCESS_TOKEN_EXPIRE_MINUTES
    from . import session_logger
    from .sanitizer import sanitize_sql, SecurityViolationError
    from kg.update_vlkg import delta_update
except ImportError:
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)

    import grounding
    from text_to_sql import baseline_text_to_sql, spts_text_to_sql, fix_sql_with_llm
    from database import execute_sql
    from db_users import ALLOWED_ROLES, create_user, get_user_by_username, normalize_role
    from auth import verify_password, get_password_hash, create_access_token, get_current_user, require_roles, ACCESS_TOKEN_EXPIRE_MINUTES
    import session_logger
    from sanitizer import sanitize_sql, SecurityViolationError
    from kg.update_vlkg import delta_update

limiter = Limiter(key_func=get_remote_address)

app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

scheduler = BackgroundScheduler()

@app.on_event("startup")
def start_scheduler():
    """Starts the background task to run delta_update during off-peak hours."""
    vlkg_ready = grounding.ensure_vlkg_ready()
    if vlkg_ready:
        print("VLKG ready at startup.")
    else:
        print("Warning: VLKG is not ready at startup. Grounding may be unavailable until bootstrap succeeds.")

    # Schedule to run every day at 2:00 AM
    scheduler.add_job(delta_update, CronTrigger(hour=2, minute=0))
    scheduler.start()
    print("Background scheduler started: VLKG Delta updates scheduled for 2:00 AM daily.")

@app.on_event("shutdown")
def stop_scheduler():
    scheduler.shutdown()

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
    role: str = "analyst"


QUERY_ALLOWED_ROLES = set(ALLOWED_ROLES)

@app.post(
    "/register",
    responses={
        400: {"description": "Invalid role or username already registered"},
    },
)
def register(user: UserCreate):
    normalized_input_role = (user.role or "").strip().lower()
    if normalized_input_role not in ALLOWED_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")

    requested_role = normalize_role(normalized_input_role)

    hashed_password = get_password_hash(user.password)
    success = create_user(user.username, hashed_password, requested_role)
    if not success:
        raise HTTPException(status_code=400, detail="Username already registered")
    return {"message": "User created successfully", "role": requested_role}


@app.post(
    "/admin/register",
    responses={
        400: {"description": "Invalid role or username already registered"},
        403: {"description": "Insufficient role permissions"},
    },
)
def admin_register(
    user: UserCreate,
    _: Annotated[dict, Depends(require_roles("admin"))],
):
    normalized_input_role = (user.role or "").strip().lower()
    if normalized_input_role not in ALLOWED_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")

    requested_role = normalize_role(normalized_input_role)
    hashed_password = get_password_hash(user.password)
    success = create_user(user.username, hashed_password, requested_role)
    if not success:
        raise HTTPException(status_code=400, detail="Username already registered")

    return {
        "message": "User created successfully",
        "role": requested_role,
        "created_by": "admin",
    }

@app.post("/token")
async def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    user = get_user_by_username(form_data.username)
    if not user or not verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"], "role": user.get("role", "analyst")}, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "role": user.get("role", "analyst"),
        "username": user["username"],
    }

def _sanitize_or_raise(sql: str, label: str) -> str:
    """Sanitize LLM-generated SQL; raise HTTP 400 on any security violation."""
    try:
        return sanitize_sql(sql)
    except SecurityViolationError as e:
        raise HTTPException(status_code=400, detail=f"Unsafe {label} SQL blocked: {e}")


@app.post(
    "/query",
    responses={
        400: {"description": "Unsafe SQL blocked by sanitizer"},
        429: {"description": "Rate limit exceeded"},
        503: {"description": "SQL generation service unavailable"},
    },
)
@limiter.limit("10/minute")
def query(request: Request, payload: dict, current_user: Annotated[dict, Depends(require_roles(*QUERY_ALLOWED_ROLES))]):
    user_query = payload["query"]

    def extract_api_error(sql_text: str):
        marker = "-- API Error:"
        if not isinstance(sql_text, str):
            return None
        if marker not in sql_text:
            return None
        return sql_text.split(marker, 1)[1].strip() or "LLM service unavailable"
    
    # FIX: Use '_' to ignore the returned query string since we only need the mappings now
    _, mappings = grounding.ground_query(user_query)
    
    # Now returns {"sql": "...", "rationale": {...}}
    baseline_response = baseline_text_to_sql(user_query)
    baseline_sql = baseline_response["sql"]
    baseline_rationale = baseline_response["rationale"]

    baseline_sql = _sanitize_or_raise(baseline_sql, "baseline")

    baseline_api_error = extract_api_error(baseline_sql)
    if baseline_api_error:
        raise HTTPException(
            status_code=503,
            detail=f"Baseline SQL generation unavailable: {baseline_api_error}",
        )
    
    # We pass the original untouched user_query, plus our new Vector DB hints
    spts_response = spts_text_to_sql(user_query, mappings)
    spts_sql = spts_response["sql"]
    spts_rationale = spts_response["rationale"]

    spts_sql = _sanitize_or_raise(spts_sql, "SPTS")

    spts_api_error = extract_api_error(spts_sql)
    if spts_api_error:
        raise HTTPException(
            status_code=503,
            detail=f"SPTS SQL generation unavailable: {spts_api_error}",
        )

    baseline_result = execute_sql(baseline_sql)
    spts_result = execute_sql(spts_sql)

    # 1-pass auto-correction loop for SPTS
    if not spts_result["success"]:
        # fix_sql_with_llm still returns just the SQL string based on previous signature
        spts_sql = fix_sql_with_llm(user_query, spts_sql, spts_result["error"], mappings)
        spts_sql = _sanitize_or_raise(spts_sql, "auto-corrected SPTS")
        spts_fix_api_error = extract_api_error(spts_sql)
        if spts_fix_api_error:
            raise HTTPException(
                status_code=503,
                detail=f"SPTS SQL auto-correction unavailable: {spts_fix_api_error}",
            )
        spts_result = execute_sql(spts_sql)
        # Optional: we update rationale to indicate a fix occurred, but keep the original latency/tokens for simplicity or add a flag
        spts_rationale["auto_corrected"] = True

    # Safely format result for frontend compatibility (`app.js` expects arrays)
    def format_res(res):
        if res["success"]:
            return res["data"]
        return [(res["error"],)]

    response = {
        "baseline_sql": baseline_sql,
        "baseline_result": format_res(baseline_result),
        "baseline_rationale": baseline_rationale,
        "spts_sql": spts_sql,
        "spts_result": format_res(spts_result),
        "spts_rationale": spts_rationale,
        "mappings": mappings,
    }

    # Log to the per-user session file (no-op if sessions dir doesn't exist)
    try:
        query_index = session_logger.log_query(
            username=current_user["username"],
            role=current_user.get("role", "analyst"),
            query=user_query,
            payload=response,
        )
        response["query_index"] = query_index
    except Exception as log_err:
        print(f"[session_logger] Warning: could not write session file: {log_err}")
        response["query_index"] = None

    return response



class FeedbackPayload(BaseModel):
    query_index: int
    baseline_rating: str | None = None
    spts_rating: str | None = None


_VALID_RATINGS = {"helpful", "unhelpful"}


@app.post(
    "/feedback",
    responses={
        400: {"description": "Invalid rating value or no session found for this query index"},
    },
)
def submit_feedback(
    payload: FeedbackPayload,
    current_user: Annotated[dict, Depends(get_current_user)],
):
    if payload.baseline_rating is not None and payload.baseline_rating not in _VALID_RATINGS:
        raise HTTPException(status_code=400, detail="Invalid baseline_rating value")
    if payload.spts_rating is not None and payload.spts_rating not in _VALID_RATINGS:
        raise HTTPException(status_code=400, detail="Invalid spts_rating value")
    ok = session_logger.update_feedback(
        username=current_user["username"],
        query_index=payload.query_index,
        baseline_rating=payload.baseline_rating,
        spts_rating=payload.spts_rating,
    )
    if not ok:
        raise HTTPException(status_code=400, detail="Could not record feedback")
    return {"status": "ok"}


@app.get(
    "/session-log",
    responses={
        404: {"description": "No session log found for current user"},
    },
)
def download_session_log(current_user: Annotated[dict, Depends(get_current_user)]):
    username = current_user.get("username", "")
    session_file = session_logger.get_session_file_path(username)

    if not os.path.exists(session_file):
        raise HTTPException(status_code=404, detail="No session log yet. Run at least one query first.")

    return FileResponse(
        path=session_file,
        media_type="application/json",
        filename=f"session_{username}.json",
    )


@app.get("/me")
def me(current_user: Annotated[dict, Depends(get_current_user)]):
    return {
        "id": current_user.get("id"),
        "username": current_user.get("username"),
        "role": current_user.get("role", "analyst"),
    }