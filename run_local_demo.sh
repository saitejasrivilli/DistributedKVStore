#!/usr/bin/env bash
set -euo pipefail

# ---------- config ----------
SUPERVISOR_TOKEN="${SUPERVISOR_TOKEN:-demo123}"   # change if you like

# ---------- prereq checks ----------
need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing: $1"; exit 1; }; }
need cloudflared
need python
need curl

# ---------- cleanup ----------
PIDS=()
cleanup() {
  echo
  echo "[cleanup] stopping background processes..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup INT TERM EXIT

# ---------- start backend ----------
export SUPERVISOR_TOKEN
echo "Starting supervisor :9000..."
python -m uvicorn kv_supervisor:app --host 0.0.0.0 --port 9000 >/tmp/sup.log 2>&1 & PIDS+=("$!")

echo "Starting frontend :8000..."
python -m uvicorn frontend_server:app --host 0.0.0.0 --port 8000 >/tmp/fe.log 2>&1 & PIDS+=("$!")

sleep 1

# ---------- helper: start one tunnel and extract the public URL ----------
start_tunnel() {
  local port="$1" log="/tmp/cf_${port}.log"
  cloudflared tunnel --url "http://localhost:${port}" --no-autoupdate >"$log" 2>&1 & PIDS+=("$!")
  # wait until the URL appears
  for i in {1..60}; do
    if grep -oE 'https://[a-z0-9.-]+trycloudflare\.com' "$log" >/dev/null; then
      grep -oE 'https://[a-z0-9.-]+trycloudflare\.com' "$log" | tail -1
      return 0
    fi
    sleep 0.25
  done
  echo "ERROR: tunnel for :$port did not produce a URL (see $log)" >&2
  exit 1
}

echo "Starting Cloudflare tunnels..."
URL_FE=$(start_tunnel 8000)
URL_SUP=$(start_tunnel 9000)
URL_N1=$(start_tunnel 8080)
URL_N2=$(start_tunnel 8081)
URL_N3=$(start_tunnel 8082)

# ---------- autostart the 3-node cluster locally ----------
echo "Starting default 3-node cluster via supervisor..."
curl -s -X POST -H "x-demo-token: ${SUPERVISOR_TOKEN}" \
  http://127.0.0.1:9000/cluster/start-default >/tmp/cluster_start.json || true

# ---------- share link ----------
SHARE="${URL_FE}/?sup=${URL_SUP}&n1=${URL_N1}&n2=${URL_N2}&n3=${URL_N3}&token=${SUPERVISOR_TOKEN}"

cat <<EOF

Frontend:   ${URL_FE}
Supervisor: ${URL_SUP}
Nodes:      ${URL_N1}, ${URL_N2}, ${URL_N3}

Share link: ${SHARE}

(Keep this terminal open. Ctrl-C to stop everything.)
EOF

# auto-open on mac
command -v open >/dev/null && open "${SHARE}"

# ---------- keep alive ----------
wait
