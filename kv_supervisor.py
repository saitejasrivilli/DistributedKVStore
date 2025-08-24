import os, sys, time, subprocess, pathlib
from typing import Dict, Optional
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

ROOT = pathlib.Path(__file__).resolve().parent
LOG_DIR = ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Local demo nodes
NODES = {
    "n1": {"port": 8080, "is_leader": True},
    "n2": {"port": 8081, "is_leader": False},
    "n3": {"port": 8082, "is_leader": False},
}
PROCS: Dict[str, subprocess.Popen] = {}

def _env_for(node_id: str) -> Dict[str, str]:
    cfg = NODES[node_id]
    port = cfg["port"]
    peers = "http://localhost:8081,http://localhost:8082" if cfg["is_leader"] else "http://localhost:8080"
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
        # ensure our repo is importable as a package (so "app.api:app" works)
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

@app.post("/node/{node_id}/start")
async def start_node(node_id: str):
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

    # wait for readiness (health)
    url = f"http://localhost:{port}"
    err = await _wait_ready(url, timeout_total=8.0)
    ready = err is None and _is_running(node_id)
    return {"ok": ready, "pid": proc.pid, "url": url, "ready": ready, "error": err}

@app.post("/node/{node_id}/stop")
def stop_node(node_id: str):
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
async def restart_node(node_id: str):
    stop_node(node_id)
    import asyncio; await asyncio.sleep(0.2)
    return await start_node(node_id)

@app.post("/cluster/start-default")
async def cluster_start_default():
    r1 = await start_node("n1")  # leader first
    import asyncio; await asyncio.sleep(0.3)
    r2 = await start_node("n2")
    r3 = await start_node("n3")
    return {"ok": True, "nodes": [r1, r2, r3]}

@app.post("/cluster/stop-all")
def cluster_stop_all():
    for nid in list(PROCS.keys()):
        stop_node(nid)
    return {"ok": True}

@app.get("/cluster/status")
async def cluster_status():
    out = []
    async with httpx.AsyncClient(timeout=2.0) as client:
        for nid, cfg in NODES.items():
            url = f"http://localhost:{cfg['port']}"
            running = _is_running(nid)
            health = None; role = None
            if running:
                try:
                    h = await client.get(f"{url}/health"); health = h.json()
                except Exception:
                    pass
                try:
                    c = await client.get(f"{url}/admin/config"); role = c.json().get("is_leader", None)
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
