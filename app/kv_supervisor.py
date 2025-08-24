# kv_supervisor.py
import os, sys, time, subprocess, pathlib
from typing import Dict, Optional
import httpx
from fastapi import FastAPI, Query, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

ROOT = pathlib.Path(__file__).resolve().parent
LOG_DIR = ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],  # includes custom token header
)

# ---- simple bearer for public demo protection ----
TOKEN = os.getenv("SUPERVISOR_TOKEN")  # set to a secret string (or leave unset to disable)
def _check(token: str | None):
    if TOKEN and token != TOKEN:
        raise HTTPException(401, "bad token")

# ---- three localhost nodes we manage ----
NODES = {
    "n1": {"port": 8080, "is_leader": True},
    "n2": {"port": 8081, "is_leader": False},
    "n3": {"port": 8082, "is_leader": False},
}
PROCS: Dict[str, subprocess.Popen] = {}

def _env_for(node_id: str) -> Dict[str, str]:
    cfg = NODES[node_id]
    port = cfg["port"]
    # leader replicates to followers; follower points to leader
    leader_port = [v["port"] for k, v in NODES.items() if v.get("is_leader")][0]
    follower_ports = [v["port"] for k, v in NODES.items() if not v.get("is_leader")]
    peers = ",".join([f"http://localhost:{p}" for p in follower_ports]) if cfg["is_leader"] \
            else f"http://localhost:{leader_port}"

    (ROOT / "data").mkdir(exist_ok=True)
    db_ix = "1" if port == 8080 else "2" if port == 8081 else "3"
    env = dict(os.environ)
    env.update({
        "IS_LEADER": "true" if cfg["is_leader"] else "false",
        "HTTP_PORT": str(port),
        "PEERS": peers,
        "REPLICATION_FACTOR": "3",
        "QUORUM_W": "2",
        "QUORUM_R": "2",
        "DB_PATH": str((ROOT / f"data/kv{db_ix}.sqlite").resolve()),
        "PYTHONPATH": os.pathsep.join(filter(None, [env.get("PYTHONPATH",""), str(ROOT)])),
    })
    return env

def _cmd_for(port: int):
    return [sys.executable, "-m", "uvicorn", "app.api:app",
            "--host", "0.0.0.0", "--port", str(port), "--log-level", "info"]

def _is_running(node_id: str) -> bool:
    p = PROCS.get(node_id)
    return p is not None and p.poll() is None

async def _wait_ready(url: str, timeout_total: float = 8.0) -> Optional[str]:
    import asyncio
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

# ----------------- endpoints -----------------

@app.post("/node/{node_id}/start")
async def start_node(node_id: str, x_demo_token: str | None = Header(None)):
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
    return {"ok": ready, "pid": proc.pid, "url": url, "ready": ready, "error": err}

@app.post("/node/{node_id}/stop")
def stop_node(node_id: str, x_demo_token: str | None = Header(None)):
    _check(x_demo_token)
    p = PROCS.get(node_id)
    if not p:
        return {"ok": True, "stopped": False}
    try:
        p.terminate(); p.wait(timeout=5)
    except Exception:
        try: p.kill()
        except Exception: pass
    PROCS.pop(node_id, None)
    return {"ok": True, "stopped": True}

@app.post("/node/{node_id}/restart")
async def restart_node(node_id: str, x_demo_token: str | None = Header(None)):
    _check(x_demo_token)
    stop_node(node_id, x_demo_token)
    import asyncio; await asyncio.sleep(0.2)
    return await start_node(node_id, x_demo_token)

@app.post("/cluster/start-default")
async def cluster_start_default(x_demo_token: str | None = Header(None)):
    _check(x_demo_token)
    r1 = await start_node("n1", x_demo_token)
    import asyncio; await asyncio.sleep(0.3)
    r2 = await start_node("n2", x_demo_token)
    r3 = await start_node("n3", x_demo_token)
    return {"ok": True, "nodes": [r1, r2, r3]}

@app.post("/cluster/stop-all")
def cluster_stop_all(x_demo_token: str | None = Header(None)):
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
            health = None; role = cfg.get("is_leader")
            if running:
                try:
                    h = await client.get(f"{url}/health"); health = h.json()
                except Exception:
                    pass
                try:
                    c = await client.get(f"{url}/admin/config"); role = c.json().get("is_leader", role)
                except Exception:
                    pass
            out.append({"id": nid, "url": url, "running": running, "health": health, "is_leader": role})
    return {"ok": True, "nodes": out}

@app.get("/logs/{node_id}")
def get_logs(node_id: str):
    p = LOG_DIR / f"{node_id}.log"
    if not p.exists():
        return {"ok": False, "error": "no log"}
    return {"ok": True, "log": p.read_text(errors="ignore")[-10000:]}

@app.post("/cluster/make-leader")
async def make_leader(
    nid: str = Query(..., regex="^(n1|n2|n3)$"),
    x_demo_token: str | None = Header(None)
):
    _check(x_demo_token)
    # Update roles
    if nid not in NODES:
        return {"ok": False, "error": "unknown node"}
    for k in NODES:
        NODES[k]["is_leader"] = (k == nid)

    # Restart all with correct env/peers (leader first)
    order = [nid] + [k for k in NODES if k != nid]
    for k in order: stop_node(k, x_demo_token)
    import asyncio; await asyncio.sleep(0.3)
    results = [await start_node(k, x_demo_token) for k in order]
    return {"ok": True, "leader": nid, "nodes": results}
    