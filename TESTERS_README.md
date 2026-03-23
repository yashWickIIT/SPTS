# SPTS Tester Guide

Welcome! You've been asked to test **SPTS** — a natural-language to SQL query tool.

---

## What you need

| Requirement | Notes |
|---|---|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Free, install and start it |
| A **Groq API key** | Free at [console.groq.com](https://console.groq.com) — takes ~1 min to sign up |
| Internet connection | Docker will pull the image automatically on first run |

---

## Step-by-step setup

### 1 — Create your API key file

In the **same folder as this file**, create or update a file called `.env` with this content:

```
API_KEY=your_actual_groq_api_key_here
```

> Replace `your_actual_groq_api_key_here` with your real key from [console.groq.com](https://console.groq.com).

---

### 2 — Start the app

```bash
docker compose -f docker-compose.test.yml up
```

Wait until you see a line like:
```
INFO:     Application startup complete.
```

---

### 3 — Open the app

Go to **[http://localhost:8000](http://localhost:8000)** in your browser.

If this is your first time, click **Create an Account**, create your own account, and then log in.

---

### 4 — Run your queries

Type natural-language questions into the query box and submit them. For example:
- *"Which airports are in California?"*
- *"Show the top 5 airlines by number of flights"*

---

### 5 — Download your session file

After running your queries, click **Download Session Log** in the app.

This downloads a file named like:

```
session_tester.json
```

If you click download before running a query, the app will prompt you to run at least one query first.

### 6 — Upload to Google Form

Open the feedback form section in the app and submit your responses.

When asked for the session log, upload the downloaded `session_<username>.json` file.

> ⚠️ Do not edit the JSON file. Upload it as-is.

---

### Stopping the app

Press `Ctrl+C` in the terminal where Docker is running, or run:

```bash
docker compose -f docker-compose.test.yml down
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Port 8000 already in use | Stop any other app on port 8000, or change `"8000:8000"` to `"8001:8000"` in `docker-compose.test.yml` |
| `API_KEY` error at startup | Check `.env` — make sure there are no extra spaces or quotes |
| `Temporary failure in name resolution` on startup | Pull the latest image again (`docker pull yashwick/spts-test:latest`) and restart compose; this usually indicates temporary DNS/network issues on the tester machine |
| Docker not found | Make sure Docker Desktop is running (check the system tray) |
| Page won't load | Wait a few more seconds for startup; the ML models take ~30s to initialise |

---

## 🚀 Testing Your Own "Messy" Database

By default, the image loads a sample flight/birds database. If you want to connect your own real-world database to see how well SPTS handles messy schemas:

1. Create a folder named `custom_db` next to the `docker-compose.test.yml` file.
2. Place your SQLite database file inside it (e.g., `my_work_database.sqlite`).
3. Open `docker-compose.test.yml` in a text editor.
4. Uncomment the custom database volume mount by removing the `#`:
   ```yaml
   volumes:
     - ./sessions:/app/sessions
     - ./custom_db:/app/custom_db:ro
   ```
5. Open your `.env` file and instruct the app to point to your new database:
   ```env
   API_KEY=your_actual_groq_api_key_here
   SPTS_MAIN_DB_PATH=/app/custom_db/my_work_database.sqlite
   ```
   *(Make sure the filename matches exactly)*
6. Restart the application:
   ```bash
   docker compose -f docker-compose.test.yml down
   docker compose -f docker-compose.test.yml up
   ```

> **Note:** The first time you start up with a new database, it may take some time to load. SPTS has to build a Vector-Level Knowledge Graph (VLKG) of your entirely new schema in the background to learn the definitions!

If you do not want to use your own database, you can skip all of the steps above. The default sample database 'califonia schools BIRD_dev_mini' is already included in the Docker image.

---

*Thank you for helping test SPTS! 🙏*
