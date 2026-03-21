# SPTS Tester Guide

Welcome! You've been asked to test **SPTS (Smart Passenger Transport System)** — a natural-language to SQL query tool.

---

## What you need

| Requirement | Notes |
|---|---|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Free, install and start it |
| A **Groq API key** | Free at [console.groq.com](https://console.groq.com) — takes ~1 min to sign up |
| The image file `spts-test.tar` | Shared with you separately |

---

## Step-by-step setup

### 1 — Load the Docker image

Open a terminal (PowerShell on Windows, Terminal on Mac/Linux) and run:

```bash
docker load -i spts-test.tar
```

You should see: `Loaded image: spts-test:latest`

---

### 2 — Create your API key file

In the **same folder as this file**, create or update a file called `.env` with this content:

```
API_KEY=your_actual_groq_api_key_here
```

> Replace `your_actual_groq_api_key_here` with your real key from [console.groq.com](https://console.groq.com).

---

### 3 — Start the app

```bash
docker compose -f docker-compose.test.yml up
```

Wait until you see a line like:
```
INFO:     Application startup complete.
```

---

### 4 — Open the app

Go to **[http://localhost:8000](http://localhost:8000)** in your browser.

Login with:
- **Username:** `tester`
- **Password:** `spts-test-2024`

---

### 5 — Run your queries

Type natural-language questions into the query box and submit them. For example:
- *"Which airports are in California?"*
- *"Show the top 5 airlines by number of flights"*

---

### 6 — Download your session file

After running your queries, click **Download Session Log** in the app.

This downloads a file named like:

```
session_tester.json
```

If you click download before running a query, the app will prompt you to run at least one query first.

### 7 — Upload to Google Form

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

*Thank you for helping test SPTS! 🙏*
