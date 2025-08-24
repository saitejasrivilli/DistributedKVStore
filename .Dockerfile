FROM python:3.12-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends bash ca-certificates && rm -rf /var/lib/apt/lists/*

# App
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# Make sure Python can import the local package
ENV PYTHONPATH=/app

# Expose all ports we’ll publish on Fly
EXPOSE 8000 9000 8080 8081 8082

# Default environment for demo (backend enables CORS already)
ENV UVICORN_LOG_LEVEL=info

# Entrypoint
CMD ["./start.sh"]
