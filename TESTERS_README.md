# SPTS Tester Guide

SPTS is a natural-language to SQL testing application.

## Requirements

| Requirement | Notes |
|---|---|
| Docker Desktop | Install and make sure it is running |
| Groq API key | Obtain from https://console.groq.com |
| Internet connection | Required to pull the Docker image |

## Quick Setup

### 1) Create `.env`

In the same folder as `docker-compose.test.yml`, create `.env`:

```env
API_KEY=your_actual_groq_api_key_here
```

### 2) Start SPTS

```bash
docker compose -f docker-compose.test.yml up
```

Wait until startup completes.

### 3) Open the app

Open `http://localhost:8000` in your browser.

If this is your first run, create an account and sign in.

### 4) Run queries

Run several natural-language queries in the app.

### 5) Submit feedback

1. Click **Download Session Log**.
2. Open the feedback form from the app.
3. Upload the downloaded `session_<username>.json` file in the form.

Do not edit the JSON file before upload.

## Stop the app

```bash
docker compose -f docker-compose.test.yml down
```

## Troubleshooting

| Problem | Fix |
|---|---|
| Port 8000 already in use | Change compose port mapping to `8001:8000` |
| API key error | Verify `API_KEY` in `.env` is correct |
| DNS/network pull error | Run `docker pull yashwick/spts-test:latest` and retry |
| Docker command not found | Start Docker Desktop and retry |

## Optional: Use your own SQLite database

By default, the image contains a sample database. To use your own:

1. Create `custom_db` beside `docker-compose.test.yml`.
2. Put your database file there (example: `custom_db/my_work_database.sqlite`).
3. In `docker-compose.test.yml`, uncomment:

```yaml
- ./custom_db:/app/custom_db:ro
```

4. Add this to `.env`:

```env
SPTS_MAIN_DB_PATH=/app/custom_db/my_work_database.sqlite
```

5. Restart:

```bash
docker compose -f docker-compose.test.yml down
docker compose -f docker-compose.test.yml up
```
