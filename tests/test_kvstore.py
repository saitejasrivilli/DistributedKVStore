# tests/test_kvstore.py
import asyncio
import importlib
import os
import shutil
import tempfile
from typing import List

import pytest
from httpx import ASGITransport, AsyncClient


def _make_app(
    is_leader: bool = True,
    peers: List[str] = None,
    db_path: str = ":memory:",
    wal_path: str = None,
    node_id: str = "node-test",
    port: int = 8080,
    quorum_w: int = 1,
    quorum_r: int = 1,
    replication_factor: int = 3,
):
    env_patch = {
        "NODE_ID": node_id,
        "HTTP_PORT": str(port),
        "IS_LEADER": "true" if is_leader else "false",
        "PEERS": ",".join(peers or []),
        "DB_PATH": db_path,
        "QUORUM_W": str(quorum_w),
        "QUORUM_R": str(quorum_r),
        "REPLICATION_FACTOR": str(replication_factor),
        "ENABLE_CLOUDWATCH": "0",
        "GRPC_PORT": "0",
    }
    import unittest.mock as _mock
    with _mock.patch.dict(os.environ, env_patch):
        import app.config as cfg_mod;    importlib.reload(cfg_mod)
        import app.storage as storage_mod; importlib.reload(storage_mod)
        import app.wal as wal_mod;       importlib.reload(wal_mod)
        import app.replication as repl_mod; importlib.reload(repl_mod)
        import app.api as api_mod;       importlib.reload(api_mod)

        api_mod.store = storage_mod.SQLiteStore(db_path)
        if wal_path:
            api_mod.wal = wal_mod.WAL(wal_path)
        else:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".wal")
            tmp.close()
            api_mod.wal = wal_mod.WAL(tmp.name)
        api_mod.repl = repl_mod.Replicator(peers or [])
        api_mod.state = {"down": False, "block_repl": False}
        return api_mod.app, api_mod


# ---------------------------------------------------------------------------
# 1. Write then read back — value and version must match what was written
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_then_read_returns_correct_value():
    the_app, _ = _make_app(is_leader=True, peers=[], quorum_w=1)
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        await c.put("/kv/k1", json={"value": "hello"})
        resp = await c.get("/kv/k1?consistency=eventual")
    assert resp.status_code == 200
    assert resp.json()["value"] == "hello"
    assert resp.json()["version"] == 1


# ---------------------------------------------------------------------------
# 2. Three successive writes — each overwrites the previous, version climbs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successive_writes_latest_value_wins():
    the_app, _ = _make_app(is_leader=True, peers=[], quorum_w=1)
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        for i, val in enumerate(["a", "b", "c"], start=1):
            await c.put("/kv/key", json={"value": val})
        resp = await c.get("/kv/key?consistency=eventual")
    assert resp.json()["value"] == "c"
    assert resp.json()["version"] == 3


# ---------------------------------------------------------------------------
# 3. Write → delete → write — key is recreated with new value, readable
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_write_delete_write_cycle():
    the_app, _ = _make_app(is_leader=True, peers=[], quorum_w=1)
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        await c.put("/kv/cycle", json={"value": "first"})
        await c.delete("/kv/cycle")
        assert (await c.get("/kv/cycle?consistency=eventual")).status_code == 404
        await c.put("/kv/cycle", json={"value": "second"})
        resp = await c.get("/kv/cycle?consistency=eventual")
    assert resp.status_code == 200
    assert resp.json()["value"] == "second"


# ---------------------------------------------------------------------------
# 4. Quorum write failure — key must NOT be stored (failed write is invisible)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_quorum_failure_does_not_store_key():
    the_app, _ = _make_app(is_leader=True, peers=[], quorum_w=2)
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        put_resp = await c.put("/kv/ghost", json={"value": "should-not-exist"})
        get_resp = await c.get("/kv/ghost?consistency=eventual")
    assert put_resp.status_code == 503
    assert get_resp.status_code == 404


# ---------------------------------------------------------------------------
# 5. Node down — ALL write/read/delete endpoints reject, not just health
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_down_node_rejects_all_operations():
    the_app, api_mod = _make_app(is_leader=True, peers=[], quorum_w=1)
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        await c.put("/kv/pre", json={"value": "stored"})
        api_mod.state["down"] = True
        put_resp = await c.put("/kv/pre", json={"value": "new"})
        get_resp = await c.get("/kv/pre?consistency=eventual")
        del_resp = await c.delete("/kv/pre")
    assert put_resp.status_code == 503
    assert get_resp.status_code == 503
    assert del_resp.status_code == 503


# ---------------------------------------------------------------------------
# 6. Node recovers — after down=False, pre-existing data is still readable
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_node_recovers_and_data_persists():
    the_app, api_mod = _make_app(is_leader=True, peers=[], quorum_w=1)
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        await c.put("/kv/persist", json={"value": "durable"})
        api_mod.state["down"] = True
        assert (await c.get("/kv/persist?consistency=eventual")).status_code == 503
        api_mod.state["down"] = False
        resp = await c.get("/kv/persist?consistency=eventual")
    assert resp.status_code == 200
    assert resp.json()["value"] == "durable"


# ---------------------------------------------------------------------------
# 7. WAL replay: multiple ops including a delete — final state is correct
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wal_replay_multiple_ops_correct_final_state():
    import app.wal as wal_mod
    import app.storage as storage_mod

    tmpdir = tempfile.mkdtemp()
    try:
        wal_path = os.path.join(tmpdir, "r.wal")
        db_path  = os.path.join(tmpdir, "r.sqlite")

        w = wal_mod.WAL(wal_path)
        w.append({"op": "put", "k": "a", "v": "v1", "ver": 1})
        w.append({"op": "put", "k": "a", "v": "v2", "ver": 2})  # overwrite
        w.append({"op": "put", "k": "b", "v": "keep", "ver": 1})
        w.append({"op": "del", "k": "b", "v": "",    "ver": 2})  # deleted

        store = storage_mod.SQLiteStore(db_path)
        for rec in wal_mod.WAL(wal_path).replay():
            if rec["op"] == "put":
                store.put(rec["k"], rec["v"].encode(), rec["ver"])
            else:
                store.delete(rec["k"])

        the_app, api_mod = _make_app(
            is_leader=True, peers=[], quorum_w=1, quorum_r=1,
            db_path=db_path, wal_path=wal_path,
        )
        api_mod.store = store

        async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
            a_resp = await c.get("/kv/a?consistency=eventual")
            b_resp = await c.get("/kv/b?consistency=eventual")

        assert a_resp.status_code == 200
        assert a_resp.json()["value"] == "v2"   # latest overwrite
        assert b_resp.status_code == 404         # deleted
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# 8. Follower does NOT store the write — 307 redirects, key stays absent
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_follower_does_not_store_write():
    the_app, _ = _make_app(is_leader=False, peers=[], quorum_w=1)
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        put_resp = await c.put("/kv/fkey", json={"value": "x"}, follow_redirects=False)
        get_resp = await c.get("/kv/fkey?consistency=eventual")
    assert put_resp.status_code == 307
    assert get_resp.status_code == 404


# ---------------------------------------------------------------------------
# 9. Strong read below quorum — returns 503, not stale data
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_strong_read_below_quorum_returns_503_not_data():
    the_app, api_mod = _make_app(is_leader=True, peers=[], quorum_w=1, quorum_r=2)
    api_mod.store.put("qk", b"secret", 1)
    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        resp = await c.get("/kv/qk?consistency=strong")
    assert resp.status_code == 503
    assert "value" not in resp.json()   # must not leak local data


# ---------------------------------------------------------------------------
# 10. Stale replication does NOT overwrite newer data
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_replication_does_not_corrupt_data():
    the_app, api_mod = _make_app(is_leader=False, peers=[], quorum_w=1)
    api_mod.store.put("mk", b"current", 5)

    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        await c.post("/internal/replicate", json={"op": "put", "k": "mk", "v": "stale", "ver": 3})
        await c.post("/internal/replicate", json={"op": "put", "k": "mk", "v": "stale", "ver": 5})
        resp = await c.get("/kv/mk?consistency=eventual")

    assert resp.json()["value"] == "current"   # stale writes rejected, data intact
    assert resp.json()["version"] == 5


# ---------------------------------------------------------------------------
# 11. Version monotonicity — fresh replication updates the stored value
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fresh_replication_updates_value():
    the_app, api_mod = _make_app(is_leader=False, peers=[], quorum_w=1)
    api_mod.store.put("mk2", b"old", 2)

    async with AsyncClient(transport=ASGITransport(app=the_app), base_url="http://test") as c:
        await c.post("/internal/replicate", json={"op": "put", "k": "mk2", "v": "new", "ver": 3})
        resp = await c.get("/kv/mk2?consistency=eventual")

    assert resp.json()["value"] == "new"
    assert resp.json()["version"] == 3


# ---------------------------------------------------------------------------
# 12. gRPC: WAL is appended on successful replicate (durability check)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_grpc_wal_appended_on_replicate():
    import grpc, grpc.aio, os as _os
    from app.grpc_server import ReplicationServicer
    from app import replication_pb2, replication_pb2_grpc
    import app.storage as storage_mod, app.wal as wal_mod

    store = storage_mod.SQLiteStore(":memory:")
    tmpf  = tempfile.NamedTemporaryFile(delete=False, suffix=".wal"); tmpf.close()
    wal   = wal_mod.WAL(tmpf.name)
    state = {"down": False, "block_repl": False}

    server = grpc.aio.server()
    replication_pb2_grpc.add_ReplicationServicer_to_server(
        ReplicationServicer(store, wal, state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
            stub = replication_pb2_grpc.ReplicationStub(ch)
            await stub.Replicate(replication_pb2.ReplicateRequest(
                op="put", k="wal-key", v="wal-val", ver=1))

        records = list(wal_mod.WAL(tmpf.name).replay())
        assert any(r["k"] == "wal-key" and r["v"] == "wal-val" for r in records)
    finally:
        await server.stop(grace=0)
        _os.unlink(tmpf.name)


# ---------------------------------------------------------------------------
# 13. gRPC: concurrent replication of distinct keys — all acked and readable
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_grpc_concurrent_replication_all_acked():
    import grpc, grpc.aio, os as _os
    from app.grpc_server import ReplicationServicer
    from app import replication_pb2, replication_pb2_grpc
    import app.storage as storage_mod, app.wal as wal_mod

    store = storage_mod.SQLiteStore(":memory:")
    tmpf  = tempfile.NamedTemporaryFile(delete=False, suffix=".wal"); tmpf.close()
    wal   = wal_mod.WAL(tmpf.name)
    state = {"down": False, "block_repl": False}

    server = grpc.aio.server()
    replication_pb2_grpc.add_ReplicationServicer_to_server(
        ReplicationServicer(store, wal, state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
            stub = replication_pb2_grpc.ReplicationStub(ch)
            results = await asyncio.gather(*[
                stub.Replicate(replication_pb2.ReplicateRequest(
                    op="put", k=f"ck{i}", v=f"val{i}", ver=1))
                for i in range(10)
            ])

        assert all(r.ack is True for r in results)
        for i in range(10):
            row = store.get(f"ck{i}")
            assert row is not None and row["version"] == 1
    finally:
        await server.stop(grace=0)
        _os.unlink(tmpf.name)


# ---------------------------------------------------------------------------
# 14. gRPC: blocked node rejects replication, store unchanged
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_grpc_blocked_node_does_not_store():
    import grpc, grpc.aio, os as _os
    from app.grpc_server import ReplicationServicer
    from app import replication_pb2, replication_pb2_grpc
    import app.storage as storage_mod, app.wal as wal_mod

    store = storage_mod.SQLiteStore(":memory:")
    tmpf  = tempfile.NamedTemporaryFile(delete=False, suffix=".wal"); tmpf.close()
    wal   = wal_mod.WAL(tmpf.name)
    state = {"down": False, "block_repl": True}

    server = grpc.aio.server()
    replication_pb2_grpc.add_ReplicationServicer_to_server(
        ReplicationServicer(store, wal, state), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as ch:
            stub = replication_pb2_grpc.ReplicationStub(ch)
            r = await stub.Replicate(replication_pb2.ReplicateRequest(
                op="put", k="blocked-key", v="x", ver=1))

        assert r.ack is False
        assert store.get("blocked-key") is None   # nothing stored
    finally:
        await server.stop(grace=0)
        _os.unlink(tmpf.name)


# ---------------------------------------------------------------------------
# 15. Replicator degrades to leader-only ack when all peers unreachable
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replicator_degrades_gracefully_on_peer_failure():
    from app.replication import Replicator

    repl = Replicator(["http://127.0.0.1:19991", "http://127.0.0.1:19992"])
    acks = await repl.replicate_to_followers(
        {"op": "put", "k": "k", "v": "v", "ver": 1}, timeout=0.1)
    assert acks == 1   # leader only — no crash, no hang
