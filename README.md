# Distributed KVStore (Python + FastAPI) — Quorum, WAL, and Live Demo

**Live demo:** (run `./run_cloudflare_demo.sh` then paste the printed Share link here)

### Try this in the UI
1. **Probe /health** → see leader & followers.
2. **PUT (auto-leader)** a key, then **GET** from followers.
3. Toggle **Block repl** on one follower → writes still succeed (W=2/3).
4. Toggle both followers **Down** → write fails with 503 (no quorum).
5. **Restart** a node → state recovers (WAL replay).
# DistributedKVStore
