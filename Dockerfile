FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# App
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Make sure Python can import the local package
ENV PYTHONPATH=/app

# Expose ports: frontend (8000), supervisor (9000), KV nodes (8080-8082)
EXPOSE 8000 8080 8081 8082 9000

# Tuning
ENV UVICORN_LOG_LEVEL=info

# Entrypoint: starts supervisor, frontend, then boots the 3-node cluster
CMD ["./start.sh"]
