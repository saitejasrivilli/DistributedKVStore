#!/usr/bin/env bash
set -e

# 1) start the supervisor (port 9000) in background
python -m uvicorn kv_supervisor:app --host 0.0.0.0 --port 9000 &
SUP_PID=$!

# 2) start the static frontend (port 8000) in background
python -m uvicorn frontend_server:app --host 0.0.0.0 --port 8000 &
FE_PID=$!

# 3) give them a moment, then ask supervisor to start the default cluster
sleep 2
# curl may not exist in base image; use Python one-liner to POST
python - <<'PY'
import urllib.request, json
try:
    req = urllib.request.Request("http://127.0.0.1:9000/cluster/start-default", method="POST")
    urllib.request.urlopen(req, timeout=5).read()
except Exception as e:
    print("autostart warning:", e)
PY

# 4) keep container alive by waiting on either service
wait -n $SUP_PID $FE_PID
