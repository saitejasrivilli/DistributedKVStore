# tests/test_kvstore.py
"""
Pytest suite for DistributedKVStore.
Tests cover: quorum writes, WAL replay, health endpoint, and consistency modes.
Uses httpx.AsyncClient with FastAPI's ASGI test transport — no real server needed.
"""
import importlib
import json
import os
import shutil
import tempfile
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_app(
    is_leader: bool = True,
    peers: List[str] = None,
    db_path: str = ":memory:",
    wal_path: str = None,
    node_id: str = "node-test",
    port: int = 8080,
    quorum_w: int = 2,
    quorum_r: int = 2,
    replication_factor: int = 3,
):
    """
    Build an isolated api.app with injected settings and fresh storage/WAL.
    Reloads all submodules so env-var-based settings are re-evaluated.
    """
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
    }

    with patch.dict(os.environ, env_patch):
        import app.config as cfg_mod
        importlib.reload(cfg_mod)

        import app.storage as storage_mod
        importlib.reload(storage_mod)

        import app.wal as wal_mod
        importlib.reload(wal_mod)

        import app.replication as repl_mod
        importlib.reload(repl_mod)

        import app.api as api_mod
        importlib.reload(api_mod)

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
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def leader_client():
    the_app, _ = _make_app(is_leader=True, peers=[], quorum_w=1, quorum_r=1)
    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        yield client


@pytest_asyncio.fixture
async def follower_client():
    the_app, _ = _make_app(is_leader=False, peers=[], quorum_w=1, quorum_r=1)
    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# 1. test_quorum_write_succeeds_with_2_of_3_nodes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_quorum_write_succeeds_with_2_of_3_nodes():
    """
    Leader + 2 peers; replicator returns 2 acks (leader=1 + 1 follower).
    Write should succeed because 2 >= QUORUM_W=2.
    """
    the_app, api_mod = _make_app(
        is_leader=True,
        peers=["http://peer1:8081", "http://peer2:8082"],
        quorum_w=2,
    )

    mock_repl = AsyncMock()
    mock_repl.replicate_to_followers = AsyncMock(return_value=2)
    api_mod.repl = mock_repl

    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        resp = await client.put("/kv/mykey", json={"value": "hello"})

    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["acks"] == 2
    assert data["version"] == 1


# ---------------------------------------------------------------------------
# 2. test_quorum_write_fails_returns_503
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_quorum_write_fails_returns_503():
    """
    replicator returns only 1 ack (both followers unreachable).
    Write must fail with 503 because 1 < QUORUM_W=2.
    """
    the_app, api_mod = _make_app(
        is_leader=True,
        peers=["http://peer1:8081", "http://peer2:8082"],
        quorum_w=2,
    )

    mock_repl = AsyncMock()
    mock_repl.replicate_to_followers = AsyncMock(return_value=1)
    api_mod.repl = mock_repl

    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        resp = await client.put("/kv/quorum-fail", json={"value": "should-fail"})

    assert resp.status_code == 503
    assert "quorum write failed" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# 3. test_wal_replay_restores_state
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_wal_replay_restores_state():
    """
    Pre-seed a WAL file with a put record; manually run the replay logic.
    The key should be readable from the restored store.
    """
    import app.wal as wal_mod
    import app.storage as storage_mod

    tmpdir = tempfile.mkdtemp()
    try:
        wal_path = os.path.join(tmpdir, "test.wal")
        db_path = os.path.join(tmpdir, "test.sqlite")

        # Write a WAL record directly (simulates prior node lifetime)
        w = wal_mod.WAL(wal_path)
        w.append({"op": "put", "k": "restored-key", "v": "restored-value", "ver": 1})

        # Fresh store and WAL pointing at the same files (simulating restart)
        fresh_store = storage_mod.SQLiteStore(db_path)
        fresh_wal = wal_mod.WAL(wal_path)

        # Replay (mirrors what boot() does)
        for rec in fresh_wal.replay():
            if rec.get("op") == "put":
                fresh_store.put(rec["k"], rec["v"].encode(), rec["ver"])
            elif rec.get("op") == "del":
                fresh_store.delete(rec["k"])

        # Build app with the replayed store
        the_app, api_mod = _make_app(
            is_leader=True,
            peers=[],
            quorum_w=1,
            quorum_r=1,
            db_path=db_path,
            wal_path=wal_path,
        )
        # Inject the already-replayed store so the test doesn't rely on startup hooks
        api_mod.store = fresh_store

        async with AsyncClient(
            transport=ASGITransport(app=the_app), base_url="http://test"
        ) as client:
            resp = await client.get("/kv/restored-key?consistency=eventual")

        assert resp.status_code == 200
        data = resp.json()
        assert data["value"] == "restored-value"
        assert data["version"] == 1
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# 4. test_health_endpoint_returns_role
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health_endpoint_returns_role():
    """
    /health must return status=healthy, node_id, wal_size, and the correct role.
    """
    the_app, _ = _make_app(is_leader=True, peers=[], quorum_w=1, node_id="leader-node")
    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        resp = await client.get("/health")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["role"] == "leader"
    assert data["node_id"] == "leader-node"
    assert "wal_size" in data
    assert "quorum_available" in data

    the_app2, _ = _make_app(is_leader=False, peers=[], quorum_w=1, node_id="follower-node")
    async with AsyncClient(
        transport=ASGITransport(app=the_app2), base_url="http://test"
    ) as client2:
        resp2 = await client2.get("/health")

    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["role"] == "follower"
    assert data2["node_id"] == "follower-node"


@pytest.mark.asyncio
async def test_health_returns_503_when_node_is_down():
    """When state['down'] is True the health endpoint must return 503."""
    the_app, api_mod = _make_app(is_leader=True, peers=[], quorum_w=1)
    api_mod.state["down"] = True

    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        resp = await client.get("/health")

    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# 5. test_strong_consistency_read_requires_quorum
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_strong_consistency_read_requires_quorum():
    """
    strong read requires QUORUM_R=2 nodes to agree.
    We mock the Replicator-level peer calls by patching the Replicator class
    and directly testing the quorum logic via the store + mocked peer responses.
    """
    the_app, api_mod = _make_app(
        is_leader=True,
        peers=["http://peer1:8081", "http://peer2:8082"],
        quorum_r=2,
        quorum_w=1,
    )

    # Pre-seed local store so self responds with a value
    api_mod.store.put("qkey", b"qvalue", 1)

    # Case A: peers all return the same value → quorum met → 200
    # Patch asyncio.gather inside api.py to return mock peer responses
    peer_ok = MagicMock()
    peer_ok.status_code = 200
    peer_ok.json = MagicMock(return_value={"value": "qvalue", "version": 1})

    import asyncio as _asyncio

    async def _mock_gather_ok(*coros, **kwargs):
        return [peer_ok, peer_ok]

    with patch("app.api.asyncio.gather", side_effect=_mock_gather_ok):
        async with AsyncClient(
            transport=ASGITransport(app=the_app), base_url="http://test"
        ) as client:
            resp_ok = await client.get("/kv/qkey?consistency=strong")

    assert resp_ok.status_code == 200
    data_ok = resp_ok.json()
    assert data_ok["value"] == "qvalue"
    assert data_ok["consistency"] == "strong"
    assert data_ok["quorum_acks"] >= 2

    # Case B: all peer calls raise exceptions → only self responded (1 ack < QUORUM_R=2) → 503
    async def _mock_gather_fail(*coros, **kwargs):
        return [Exception("timeout"), Exception("timeout")]

    with patch("app.api.asyncio.gather", side_effect=_mock_gather_fail):
        async with AsyncClient(
            transport=ASGITransport(app=the_app), base_url="http://test"
        ) as client:
            resp_fail = await client.get("/kv/qkey?consistency=strong")

    assert resp_fail.status_code == 503
    assert "quorum read failed" in resp_fail.json()["detail"]


# ---------------------------------------------------------------------------
# 6. test_eventual_consistency_read_local
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_eventual_consistency_read_local():
    """
    ?consistency=eventual reads only from the local store without contacting peers.
    We verify that asyncio.gather (which drives peer fan-out) is NOT called.
    """
    the_app, api_mod = _make_app(
        is_leader=True,
        peers=["http://peer1:8081"],
        quorum_w=1,
        quorum_r=2,
    )
    api_mod.store.put("ekey", b"evalue", 3)

    # asyncio.gather is only called for strong reads; eventual must bypass it
    with patch("app.api.asyncio.gather", new_callable=AsyncMock) as mock_gather:
        async with AsyncClient(
            transport=ASGITransport(app=the_app), base_url="http://test"
        ) as client:
            resp = await client.get("/kv/ekey?consistency=eventual")
        mock_gather.assert_not_called()

    assert resp.status_code == 200
    data = resp.json()
    assert data["value"] == "evalue"
    assert data["version"] == 3
    assert data["consistency"] == "eventual"


# ---------------------------------------------------------------------------
# Additional edge-case tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_eventual_consistency_read_404_when_missing():
    """eventual read returns 404 if key not in local store."""
    the_app, _ = _make_app(is_leader=True, peers=[], quorum_w=1)
    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        resp = await client.get("/kv/missing-key?consistency=eventual")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_put_increments_version():
    """Each successive PUT to the same key must increment the version."""
    the_app, _ = _make_app(is_leader=True, peers=[], quorum_w=1)

    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        r1 = await client.put("/kv/versioned", json={"value": "v1"})
        r2 = await client.put("/kv/versioned", json={"value": "v2"})

    assert r1.json()["version"] == 1
    assert r2.json()["version"] == 2


@pytest.mark.asyncio
async def test_follower_rejects_write():
    """A follower node must return 307 on PUT."""
    the_app, _ = _make_app(is_leader=False, peers=[], quorum_w=1)
    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        resp = await client.put("/kv/somekey", json={"value": "x"}, follow_redirects=False)
    assert resp.status_code == 307


@pytest.mark.asyncio
async def test_delete_removes_key():
    """DELETE should remove the key from local storage."""
    the_app, api_mod = _make_app(is_leader=True, peers=[], quorum_w=1)
    api_mod.store.put("delkey", b"todelete", 1)

    async with AsyncClient(
        transport=ASGITransport(app=the_app), base_url="http://test"
    ) as client:
        del_resp = await client.delete("/kv/delkey")
        get_resp = await client.get("/kv/delkey?consistency=eventual")

    assert del_resp.status_code == 200
    assert get_resp.status_code == 404
