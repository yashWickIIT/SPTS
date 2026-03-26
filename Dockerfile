FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV SPTS_EMBEDDING_MODEL=BAAI/bge-small-en-v1.5

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.docker.txt .
RUN pip install -r requirements.docker.txt

COPY backend ./backend
COPY frontend ./frontend
COPY kg ./kg
COPY data ./data

EXPOSE 8000

CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]