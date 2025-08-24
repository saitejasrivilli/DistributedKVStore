# app/api.py
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import time

from .config import settings
from .storage import SQLiteStore
from .wal import WAL
from .replication import Replicator
from .hashring import HashRing
from .metrics import metrics, start_exporter_if_enabled

app = FastAPI()

# Allow browser frontend in dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # dev only; tighten for prod
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- State & singletons ---
store = SQLiteStore(settings.DB_PATH)
wal = WAL()
repl = Replicator(settings.PEERS)
ring: Optional[HashRing] = None

# In-memory admin toggles (for frontend-driven testing)
state = {
    "down": False,        # if True => node rejects normal traffic
    "block_repl": False,  # if True => reject /internal/replicate to simulate partition
}

# --- Models ---
class KVPut(BaseModel):
    value: str

class AdminToggle(BaseModel):
    down: Optional[bool] = None
    block_repl: Optional[bool] = None

# --- Middleware: latency histogram samples ---
@app.middleware("http")
async def record_latency(request: Request, call_next):
    t0 = time.time()
    try:
        resp = await call_next(request)
        return resp
    finally:
        ms = (time.time() - t0) * 1000.0
        metrics["latencies_ms"].append(ms)

# --- Startup: build hash ring, replay WAL, start optional exporter ---
@app.on_event("startup")
async def boot():
    global ring
    me = f"http://localhost:{settings.HTTP_PORT}"
    ring = HashRing([me] + settings.PEERS)
    for rec in wal.replay():
        if rec.get("op") == "put":
            store.put(rec["k"], rec["v"].encode(), rec["ver"])
        elif rec.get("op") == "del":
            store.delete(rec["k"])
    start_exporter_if_enabled()

def _is_owner(key: str) -> bool:
    owners = ring.owners(key, settings.REPLICATION_FACTOR) if ring else []
    me = f"http://localhost:{settings.HTTP_PORT}"
    return me in owners

# --- Health & Metrics ---
@app.get("/health")
def health():
    return {"ok": not state["down"], "state": state}
# --- Admin/diagnostics ---
from pydantic import BaseModel
from typing import Optional

@app.get("/admin/config")
def admin_config():
    return {
        "node_id": settings.NODE_ID,
        "is_leader": settings.IS_LEADER,     # <— frontend expects this exact key
        "peers": settings.PEERS,
        "state": state,                      # {down, block_repl, (delay_ms if you added it)}
    }

class AdminToggle(BaseModel):
    down: Optional[bool] = None
    block_repl: Optional[bool] = None
    # delay_ms: Optional[int] = None        # if you added latency simulation

@app.post("/admin/toggle")
def admin_toggle(t: AdminToggle):
    if t.down is not None: state["down"] = bool(t.down)
    if t.block_repl is not None: state["block_repl"] = bool(t.block_repl)
    # if t.delay_ms is not None: state["delay_ms"] = max(0, int(t.delay_ms))
    return {"ok": True, "state": state}

@app.get("/metrics")
def metrics_endpoint():
    return {
        "requests_total": metrics["requests_total"],
        "errors_total": metrics["errors_total"],
        "latency_samples": len(metrics["latencies_ms"]),
        "replication_ack_samples": len(metrics["replication_acks"]),
    }

# --- Admin (frontend-controlled simulation) ---
@app.post("/admin/toggle")
def admin_toggle(t: AdminToggle):
    if t.down is not None:
        state["down"] = t.down
    if t.block_repl is not None:
        state["block_repl"] = t.block_repl
    return {"state": state}

@app.get("/admin/config")
def admin_config():
    return {
        "node_id": settings.NODE_ID,
        "http_port": settings.HTTP_PORT,
        "is_leader": settings.IS_LEADER,
        "peers": settings.PEERS,
        "rf": settings.REPLICATION_FACTOR,
        "quorum_w": settings.QUORUM_W,
        "quorum_r": settings.QUORUM_R,
        "db_path": settings.DB_PATH,
        "state": state,
    }

# --- KV API ---
@app.get("/kv/{key}")
async def get_key(key: str):
    if state["down"]:
        raise HTTPException(503, "node is down (simulated)")
    metrics["requests_total"] += 1
    row = store.get(key)
    if not row:
        raise HTTPException(404, "not found")
    return {"value": row["value"], "version": row["version"]}

@app.put("/kv/{key}")
async def put_key(key: str, body: KVPut):
    if state["down"]:
        raise HTTPException(503, "node is down (simulated)")
    metrics["requests_total"] += 1

    if not settings.IS_LEADER:
        raise HTTPException(status_code=307, detail="redirect-to-leader")

    current = store.get(key)
    new_ver = 1 if not current else current["version"] + 1
    rec = {"op": "put", "k": key, "v": body.value, "ver": new_ver}

    # Write-ahead log before replication
    wal.append(rec)

    # Replicate and wait for quorum
    acks = 1
    if settings.PEERS:
        acks = await repl.replicate_to_followers(rec)

    if acks < settings.QUORUM_W:
        metrics["errors_total"] += 1
        raise HTTPException(503, f"quorum write failed: acks={acks}")

    # Commit locally
    store.put(key, body.value.encode(), new_ver)
    return {"ok": True, "version": new_ver, "acks": acks}

@app.delete("/kv/{key}")
async def delete_key(key: str):
    if state["down"]:
        raise HTTPException(503, "node is down (simulated)")
    metrics["requests_total"] += 1

    if not settings.IS_LEADER:
        raise HTTPException(status_code=307, detail="redirect-to-leader")

    cur = store.get(key)
    new_ver = 1 if not cur else cur["version"] + 1
    rec = {"op": "del", "k": key, "v": "", "ver": new_ver}
    wal.append(rec)

    acks = 1
    if settings.PEERS:
        acks = await repl.replicate_to_followers(rec)

    if acks < settings.QUORUM_W:
        metrics["errors_total"] += 1
        raise HTTPException(503, "quorum delete failed")

    store.delete(key)
    return {"ok": True, "acks": acks}

# --- Internal replication endpoint (called by leader) ---
@app.post("/internal/replicate")
async def internal_replicate(rec: dict):
    # Simulate partition or node down
    if state["down"] or state["block_repl"]:
        return Response(status_code=503)

    op = rec.get("op")
    if op not in ("put", "del"):
        return Response(status_code=400)

    if op == "put":
        store.put(rec["k"], rec["v"].encode(), rec["ver"])
    else:
        store.delete(rec["k"])

    # Persist follower WAL so recovery replays too
    wal.append(rec)
    return {"ack": True}
