# app/api.py
import asyncio
import os
import time
import uuid
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response as StarletteResponse
from pydantic import BaseModel

from .config import settings
from .hashring import HashRing
from .metrics import metrics, start_exporter_if_enabled
from .replication import Replicator
from .storage import SQLiteStore
from .wal import WAL

app = FastAPI(title="DistributedKVStore", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Prometheus latency histogram ---
# try/except handles importlib.reload() in tests re-registering the same metric
try:
    REQUEST_LATENCY = Histogram(
        "kvstore_request_duration_ms",
        "Per-request latency in milliseconds",
        ["method", "path"],
        buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
    )
except ValueError:
    from prometheus_client import REGISTRY as _REG
    REQUEST_LATENCY = _REG._names_to_collectors["kvstore_request_duration_ms"]

# --- State & singletons ---
store = SQLiteStore(settings.DB_PATH)
wal = WAL()
repl = Replicator(settings.PEERS)
ring: Optional[HashRing] = None

# In-memory admin toggles (for frontend-driven testing)
state = {
    "down": False,
    "block_repl": False,
}


# --- Models ---
class KVPut(BaseModel):
    value: str


class AdminToggle(BaseModel):
    down: Optional[bool] = None
    block_repl: Optional[bool] = None


# --- Middleware: request ID ---
@app.middleware("http")
async def attach_request_id(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# --- Middleware: latency histogram ---
@app.middleware("http")
async def record_latency(request: Request, call_next):
    t0 = time.time()
    try:
        resp = await call_next(request)
        return resp
    finally:
        ms = (time.time() - t0) * 1000.0
        metrics["latencies_ms"].append(ms)
        REQUEST_LATENCY.labels(method=request.method, path=request.url.path).observe(ms)


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


# --- Shutdown: drain in-flight replication ---
@app.on_event("shutdown")
async def shutdown():
    # Yield the event loop so any in-flight httpx replication calls can finish
    await asyncio.sleep(0.1)


def _is_owner(key: str) -> bool:
    owners = ring.owners(key, settings.REPLICATION_FACTOR) if ring else []
    me = f"http://localhost:{settings.HTTP_PORT}"
    return me in owners


def _wal_size() -> int:
    wal_path = wal.path
    if not os.path.exists(wal_path):
        return 0
    count = 0
    try:
        with open(wal_path, "r") as f:
            for line in f:
                if line.strip():
                    count += 1
    except OSError:
        pass
    return count


async def _quorum_available() -> bool:
    """Concurrent async probe — all peers checked in parallel, 1 s timeout each."""
    if not settings.PEERS:
        return True
    try:
        async with httpx.AsyncClient(timeout=1.0) as client:
            results = await asyncio.gather(
                *[client.get(f"{peer}/health") for peer in settings.PEERS],
                return_exceptions=True,
            )
        reachable = 1 + sum(
            1 for r in results
            if not isinstance(r, Exception) and getattr(r, "status_code", 0) == 200
        )
        return reachable >= settings.QUORUM_W
    except Exception:
        return False


# --- Health endpoint ---
@app.get("/health")
async def health():
    """
    Returns node status suitable for supervisor health polling.
    Format: {"status": "healthy"|"down", "role": "leader"|"follower",
             "wal_size": int, "node_id": str, "quorum_available": bool}
    """
    if state["down"]:
        raise HTTPException(status_code=503, detail={
            "status": "down",
            "role": "leader" if settings.IS_LEADER else "follower",
            "wal_size": _wal_size(),
            "node_id": settings.NODE_ID,
            "quorum_available": False,
        })
    return {
        "status": "healthy",
        "role": "leader" if settings.IS_LEADER else "follower",
        "wal_size": _wal_size(),
        "node_id": settings.NODE_ID,
        "quorum_available": await _quorum_available(),
    }


# --- Admin / diagnostics ---
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


@app.post("/admin/toggle")
def admin_toggle(t: AdminToggle):
    if t.down is not None:
        state["down"] = bool(t.down)
    if t.block_repl is not None:
        state["block_repl"] = bool(t.block_repl)
    return {"ok": True, "state": state}


@app.get("/metrics")
def metrics_endpoint():
    return {
        "requests_total": metrics["requests_total"],
        "errors_total": metrics["errors_total"],
        "latency_samples": len(metrics["latencies_ms"]),
        "replication_ack_samples": len(metrics["replication_acks"]),
    }


@app.get("/metrics/prometheus", include_in_schema=False)
def prometheus_metrics():
    return StarletteResponse(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# --- KV API ---
@app.get("/kv/{key}")
async def get_key(
    key: str,
    consistency: str = Query(default="eventual", pattern="^(strong|eventual)$"),
):
    if state["down"]:
        raise HTTPException(503, "node is down (simulated)")
    metrics["requests_total"] += 1

    if consistency == "eventual":
        # Read from local store only
        row = store.get(key)
        if not row:
            raise HTTPException(404, "not found")
        return {"value": row["value"], "version": row["version"], "consistency": "eventual"}

    # strong: read from quorum (R = QUORUM_R out of total nodes)
    # Gather reads from self + all peers, require QUORUM_R agreeing on same value
    local = store.get(key)
    responses = []
    if local:
        responses.append({"value": local["value"], "version": local["version"]})

    if settings.PEERS:
        async with httpx.AsyncClient(timeout=1.0) as client:
            tasks = [
                client.get(f"{peer}/kv/{key}?consistency=eventual")
                for peer in settings.PEERS
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                continue
            if getattr(r, "status_code", 500) == 200:
                data = r.json()
                responses.append({"value": data.get("value"), "version": data.get("version")})
            elif getattr(r, "status_code", 500) == 404:
                responses.append({"value": None, "version": 0})

    if not responses:
        raise HTTPException(503, "quorum read failed: no responses")

    # Determine quorum: group by (value, version), need QUORUM_R agreement
    from collections import Counter
    counts: Counter = Counter()
    for resp in responses:
        key_tuple = (resp.get("value"), resp.get("version", 0))
        counts[key_tuple] += 1

    best, best_count = counts.most_common(1)[0]
    if best_count < settings.QUORUM_R:
        metrics["errors_total"] += 1
        raise HTTPException(
            503,
            f"quorum read failed: only {best_count}/{settings.QUORUM_R} nodes agree"
        )

    val, ver = best
    if val is None:
        raise HTTPException(404, "not found (quorum)")
    return {"value": val, "version": ver, "consistency": "strong", "quorum_acks": best_count}


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

    wal.append(rec)

    acks = 1
    if settings.PEERS:
        acks = await repl.replicate_to_followers(rec)

    if acks < settings.QUORUM_W:
        metrics["errors_total"] += 1
        raise HTTPException(503, f"quorum write failed: acks={acks}")

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
    if state["down"] or state["block_repl"]:
        return Response(status_code=503)

    op = rec.get("op")
    if op not in ("put", "del"):
        return Response(status_code=400)

    if op == "put":
        current = store.get(rec["k"])
        incoming_ver = rec.get("ver", 0)
        if current is not None and current["version"] >= incoming_ver:
            # Stale or duplicate replication record — discard silently
            return Response(status_code=409)
        store.put(rec["k"], rec["v"].encode(), incoming_ver)
    else:
        store.delete(rec["k"])

    wal.append(rec)
    return {"ack": True}
