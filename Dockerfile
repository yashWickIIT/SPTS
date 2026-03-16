# Use a lightweight Python base image
FROM python:3.11-slim

# Prevent Python from writing pyc files and keep stdout unbuffered
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

# Set the working directory inside the container
WORKDIR /app

# Install only minimal runtime system libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy runtime requirements and install CPU-only ML stack
COPY requirements.docker.txt .
RUN pip install --extra-index-url https://download.pytorch.org/whl/cpu -r requirements.docker.txt

# Copy only runtime application code
COPY backend ./backend
COPY frontend ./frontend
COPY kg ./kg

# Expose the port FastAPI runs on
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]