# kv_supervisor.py — top-level entry point; delegates to app.kv_supervisor
# This file exists so `uvicorn kv_supervisor:app` works from the repo root.
import asyncio
import os
import pathlib
import subprocess
import sys
import time
from collections import defaultdict
from typing import Dict, Optional

import httpx
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# Import HashRing from the app package (repo root is on PYTHONPATH at runtime)
from app.hashring import HashRing

ROOT = pathlib.Path(__file__).resolve().parent
LOG_DIR = ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="KVStore Supervisor", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TOKEN = os.getenv("SUPERVISOR_TOKEN")


def _check(token: Optional[str]):
    if TOKEN and token != TOKEN:
        raise HTTPException(401, "bad token")


# ---- three localhost nodes we manage ----
NODES = {
    "n1": {"port": 8080, "is_leader": True},
    "n2": {"port": 8081, "is_leader": False},
    "n3": {"port": 8082, "is_leader": False},
}
PROCS: Dict[str, subprocess.Popen] = {}

# Health tracking: consecutive failure counts per node URL
_health_failures: Dict[str, int] = defaultdict(int)
_node_healthy: Dict[str, bool] = {}
_HEALTH_FAIL_THRESHOLD = 3

# Consistent hash ring over healthy node URLs
_ring: Optional[HashRing] = None


def _all_node_urls() -> list:
    return [f"http://localhost:{cfg['port']}" for cfg in NODES.values()]


def _build_ring(urls: Optional[list] = None) -> HashRing:
    return HashRing(urls or _all_node_urls())


def _leader_url() -> Optional[str]:
    for cfg in NODES.values():
        if cfg.get("is_leader"):
            url = f"http://localhost:{cfg['port']}"
            if _node_healthy.get(url, True):
                return url
    return None


def _healthy_nodes() -> list:
    return [url for url in _all_node_urls() if _node_healthy.get(url, True)]


def _env_for(node_id: str) -> Dict[str, str]:
    cfg = NODES[node_id]
    port = cfg["port"]
    leader_port = next(v["port"] for v in NODES.values() if v.get("is_leader"))
    follower_ports = [v["port"] for v in NODES.values() if not v.get("is_leader")]
    peers = (
        ",".join(f"http://localhost:{p}" for p in follower_ports)
        if cfg["is_leader"]
        else f"http://localhost:{leader_port}"
    )
    (ROOT / "data").mkdir(exist_ok=True)
    db_ix = "1" if port == 8080 else "2" if port == 8081 else "3"
    env = dict(os.environ)
    env.update({
        "IS_LEADER": "true" if cfg["is_leader"] else "false",
        "HTTP_PORT": str(port),
        "NODE_ID": node_id,
        "PEERS": peers,
        "REPLICATION_FACTOR": "3",
        "QUORUM_W": "2",
        "QUORUM_R": "2",
        "DB_PATH": str((ROOT / f"data/kv{db_ix}.sqlite").resolve()),
        "PYTHONPATH": os.pathsep.join(
            filter(None, [env.get("PYTHONPATH", ""), str(ROOT)])
        ),
    })
    return env


def _cmd_for(port: int):
    return [
        sys.executable, "-m", "uvicorn", "app.api:app",
        "--host", "0.0.0.0", "--port", str(port), "--log-level", "info",
    ]


def _is_running(node_id: str) -> bool:
    p = PROCS.get(node_id)
    return p is not None and p.poll() is None


async def _wait_ready(url: str, timeout_total: float = 8.0) -> Optional[str]:
    deadline = time.time() + timeout_total
    last_err = None
    async with httpx.AsyncClient(timeout=2.0) as client:
        while time.time() < deadline:
            try:
                r = await client.get(f"{url}/health")
                if r.status_code == 200:
                    return None
                last_err = f"HTTP {r.status_code}"
            except Exception as e:
                last_err = str(e)
            await asyncio.sleep(0.25)
    return last_err


# --- Health polling background task ---
async def _health_poll_loop():
    """Poll each node /health every 5s. Mark unavailable after 3 consecutive failures."""
    global _ring
    async with httpx.AsyncClient(timeout=2.0) as client:
        while True:
            for node_id, cfg in NODES.items():
                url = f"http://localhost:{cfg['port']}"
                if not _is_running(node_id):
                    _health_failures[url] = _HEALTH_FAIL_THRESHOLD
                    _node_healthy[url] = False
                    continue
                try:
                    r = await client.get(f"{url}/health")
                    if r.status_code == 200:
                        _health_failures[url] = 0
                        _node_healthy[url] = True
                    else:
                        _health_failures[url] += 1
                except Exception:
                    _health_failures[url] += 1

                if _health_failures[url] >= _HEALTH_FAIL_THRESHOLD:
                    _node_healthy[url] = False

            healthy = _healthy_nodes()
            if healthy:
                _ring = _build_ring(healthy)

            await asyncio.sleep(5)


@app.on_event("startup")
async def startup():
    global _ring
    _ring = _build_ring()
    for cfg in NODES.values():
        url = f"http://localhost:{cfg['port']}"
        _node_healthy[url] = True
    asyncio.create_task(_health_poll_loop())


# --- Write routing via consistent hash ring ---
async def _route_write(key: str, method: str, json_body: Optional[dict] = None) -> dict:
    """
    Use the hash ring to pick the responsible node for `key`.
    Writes must ultimately go through the leader (for quorum replication).
    Hash ring selects the preferred node; if that node is the leader we go direct.
    Otherwise we route to the leader and it handles replication.
    """
    preferred_url = None
    if _ring:
        owners = _ring.owners(key, 1)
        if owners:
            preferred_url = owners[0]

    leader = _leader_url()
    # Prefer the ring-determined node if it's healthy; otherwise fall back to leader
    target = (
        preferred_url
        if preferred_url and _node_healthy.get(preferred_url, True)
        else leader
    )
    if not target:
        raise HTTPException(503, "no healthy write node available")

    async with httpx.AsyncClient(timeout=3.0) as client:
        if method == "PUT":
            r = await client.put(f"{target}/kv/{key}", json=json_body)
        else:
            r = await client.delete(f"{target}/kv/{key}")

    # Follower returned 307 → retry on leader
    if r.status_code == 307 and leader and target != leader:
        async with httpx.AsyncClient(timeout=3.0) as client2:
            if method == "PUT":
                r = await client2.put(f"{leader}/kv/{key}", json=json_body)
            else:
                r = await client2.delete(f"{leader}/kv/{key}")

    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text)
    return r.json()


# --- Supervisor KV proxy endpoints ---
@app.put("/kv/{key}")
async def supervisor_put(key: str, body: dict, x_demo_token: Optional[str] = Header(None)):
    _check(x_demo_token)
    return await _route_write(key, "PUT", body)


@app.delete("/kv/{key}")
async def supervisor_delete(key: str, x_demo_token: Optional[str] = Header(None)):
    _check(x_demo_token)
    return await _route_write(key, "DELETE")


@app.get("/kv/{key}")
async def supervisor_get(
    key: str,
    consistency: str = Query(default="eventual", pattern="^(strong|eventual)$"),
    x_demo_token: Optional[str] = Header(None),
):
    _check(x_demo_token)
    target = None
    if _ring:
        owners = _ring.owners(key, 1)
        if owners and _node_healthy.get(owners[0], True):
            target = owners[0]
    if not target:
        healthy = _healthy_nodes()
        if not healthy:
            raise HTTPException(503, "no healthy nodes available")
        target = healthy[0]

    async with httpx.AsyncClient(timeout=2.0) as client:
        r = await client.get(f"{target}/kv/{key}?consistency={consistency}")

    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text)
    return r.json()


# --- Node lifecycle endpoints ---
@app.post("/node/{node_id}/start")
async def start_node(node_id: str, x_demo_token: Optional[str] = Header(None)):
    _check(x_demo_token)
    if node_id not in NODES:
        return {"ok": False, "error": "unknown node"}
    if _is_running(node_id):
        p = PROCS[node_id]
        return {"ok": True, "pid": p.pid, "url": f"http://localhost:{NODES[node_id]['port']}", "ready": True}

    env = _env_for(node_id)
    port = NODES[node_id]["port"]
    logf = open(LOG_DIR / f"{node_id}.log", "ab", buffering=0)
    proc = subprocess.Popen(_cmd_for(port), env=env, cwd=str(ROOT), stdout=logf, stderr=logf)
    PROCS[node_id] = proc

    url = f"http://localhost:{port}"
    err = await _wait_ready(url, timeout_total=8.0)
    ready = err is None and _is_running(node_id)
    if ready:
        _node_healthy[url] = True
        _health_failures[url] = 0
    return {"ok": ready, "pid": proc.pid, "url": url, "ready": ready, "error": err}


@app.post("/node/{node_id}/stop")
def stop_node(node_id: str, x_demo_token: Optional[str] = Header(None)):
    _check(x_demo_token)
    p = PROCS.get(node_id)
    if not p:
        return {"ok": True, "stopped": False}
    try:
        p.terminate()
        p.wait(timeout=5)
    except Exception:
        try:
            p.kill()
        except Exception:
            pass
    PROCS.pop(node_id, None)
    url = f"http://localhost:{NODES[node_id]['port']}"
    _node_healthy[url] = False
    _health_failures[url] = _HEALTH_FAIL_THRESHOLD
    return {"ok": True, "stopped": True}


@app.post("/node/{node_id}/restart")
async def restart_node(node_id: str, x_demo_token: Optional[str] = Header(None)):
    _check(x_demo_token)
    stop_node(node_id, x_demo_token)
    await asyncio.sleep(0.2)
    return await start_node(node_id, x_demo_token)


@app.post("/cluster/start-default")
async def cluster_start_default(x_demo_token: Optional[str] = Header(None)):
    _check(x_demo_token)
    r1 = await start_node("n1", x_demo_token)
    await asyncio.sleep(0.3)
    r2 = await start_node("n2", x_demo_token)
    r3 = await start_node("n3", x_demo_token)
    return {"ok": True, "nodes": [r1, r2, r3]}


@app.post("/cluster/stop-all")
def cluster_stop_all(x_demo_token: Optional[str] = Header(None)):
    _check(x_demo_token)
    for nid in list(PROCS.keys()):
        stop_node(nid, x_demo_token)
    return {"ok": True}


@app.get("/cluster/status")
async def cluster_status():
    out = []
    async with httpx.AsyncClient(timeout=2.0) as client:
        for nid, cfg in NODES.items():
            url = f"http://localhost:{cfg['port']}"
            running = _is_running(nid)
            health_data = None
            role = cfg.get("is_leader")
            if running:
                try:
                    h = await client.get(f"{url}/health")
                    health_data = h.json()
                    role = health_data.get("role") == "leader"
                except Exception:
                    pass
            out.append({
                "id": nid,
                "url": url,
                "running": running,
                "health": health_data,
                "is_leader": role,
                "supervisor_healthy": _node_healthy.get(url, True),
                "consecutive_failures": _health_failures.get(url, 0),
            })
    return {"ok": True, "nodes": out}


@app.get("/logs/{node_id}")
def get_logs(node_id: str):
    p = LOG_DIR / f"{node_id}.log"
    if not p.exists():
        return {"ok": False, "error": "no log"}
    return {"ok": True, "log": p.read_text(errors="ignore")[-10000:]}


@app.post("/cluster/make-leader")
async def make_leader(
    nid: str = Query(..., pattern="^(n1|n2|n3)$"),
    x_demo_token: Optional[str] = Header(None),
):
    _check(x_demo_token)
    if nid not in NODES:
        return {"ok": False, "error": "unknown node"}
    for k in NODES:
        NODES[k]["is_leader"] = k == nid

    order = [nid] + [k for k in NODES if k != nid]
    for k in order:
        stop_node(k, x_demo_token)
    await asyncio.sleep(0.3)
    results = [await start_node(k, x_demo_token) for k in order]
    return {"ok": True, "leader": nid, "nodes": results}
