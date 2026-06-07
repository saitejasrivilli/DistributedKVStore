# DistributedKVStore

[![CI](https://github.com/saitejasrivilli/DistributedKVStore/actions/workflows/ci.yml/badge.svg)](https://github.com/saitejasrivilli/DistributedKVStore/actions/workflows/ci.yml)

A distributed key-value store built in Python and FastAPI demonstrating quorum writes, WAL-backed durability, consistent hash routing, and configurable read consistency — the same core primitives used in systems like DynamoDB and Cassandra.

---

## Architecture

```
                         ┌──────────────────────────────┐
        Client           │        Supervisor (9000)      │
       PUT /kv/foo  ───► │  ┌─────────────────────────┐ │
                         │  │  Consistent Hash Ring    │ │
                         │  │  (HashRing, 100 vnodes)  │ │
                         │  └────────────┬────────────┘ │
                         │   health poll │ every 5s      │
                         │   mark unhealthy after 3 fails│
                         └──────────────┼───────────────┘
                                        │ routes write to owner node
                    ┌───────────────────┴────────────────────┐
                    ▼                                         ▼
        ┌───────────────────────┐             ┌─────────────────────────┐
        │   Leader Node (8080)  │             │  Follower Node (8081/2) │
        │                       │             │                         │
        │  1. WAL.append(rec)   │  replicate  │  /internal/replicate    │
        │     (fsync to disk)   │ ──────────► │  WAL.append(rec)        │
        │  2. await quorum acks │             │  store.put(k, v)        │
        │     W = 2 of 3        │             │                         │
        │  3. store.put(k,v)    │             │  SQLite (WAL journal)   │
        │     SQLite (WAL)      │             └─────────────────────────┘
        └───────────────────────┘
```

**Request flow:**
1. Client sends a write to the Supervisor.
2. Supervisor uses the consistent hash ring to pick the preferred node for that key; falls back to the leader on unhealthy nodes.
3. Leader writes to WAL (fsync), replicates to followers, waits for W=2/3 acks.
4. On ack quorum, leader commits to SQLite and returns success.
5. On restart, each node replays its WAL to restore state before accepting traffic.

---

## Features

| Feature | Detail |
|---|---|
| **Quorum writes** | W = 2/3 — a write succeeds if at least 2 nodes acknowledge |
| **Leader/follower replication** | Leader fans out via `/internal/replicate`; followers 307-redirect writes to leader |
| **WAL with fsync** | Every write is appended and fsynced before replication; replayed on node restart |
| **Consistent hash routing** | 100-vnode SHA-1 ring; supervisor routes each key to its preferred node |
| **Health checking** | Supervisor polls `/health` every 5 s; marks node unavailable after 3 consecutive failures; writes bypass unhealthy nodes |
| **Strong/eventual reads** | `?consistency=strong` gathers R=2/3 quorum reads and requires agreement; `eventual` (default) reads from local node only |
| **Node simulation** | Admin toggles for `down` and `block_repl` states to simulate partitions without killing processes |
| **Optional CloudWatch metrics** | Set `ENABLE_CLOUDWATCH=1` to export request counts, latency P50/P95, and replication ack averages |

---

## Consistency Model

This store makes explicit **CP tradeoffs**:

- **Strong reads** (`?consistency=strong`): requires R=2/3 nodes to agree on value and version. Slower; consistent even if one node has stale data.
- **Eventual reads** (`?consistency=eventual`, default): reads from the local node only. Fast; may return stale data immediately after a write before replication propagates.
- **Writes** always require quorum (W=2/3). If only one node is reachable, writes are rejected with 503 rather than silently accepting partial writes.

---

## Running Locally

### Prerequisites

```
Python 3.11+
```

### Install

```bash
pip install -r requirements.txt
```

### Start the 3-node cluster

```bash
# Terminal 1: supervisor (manages node lifecycle + routes requests)
uvicorn kv_supervisor:app --host 0.0.0.0 --port 9000

# Supervisor auto-starts n1 (leader), n2, n3 via POST /cluster/start-default
# Or start nodes individually:
curl -X POST http://localhost:9000/cluster/start-default
```

Individual nodes listen on ports 8080 (leader), 8081, 8082.

### Docker

```bash
docker build -t kvstore .
docker run -p 8000:8000 -p 9000:9000 -p 8080:8080 -p 8081:8081 -p 8082:8082 kvstore
```

### Run tests

```bash
pytest tests/ -v
```

---

## API Reference

### Node KV API (ports 8080 / 8081 / 8082)

| Method | Path | Query | Description |
|---|---|---|---|
| `GET` | `/kv/{key}` | `consistency=eventual\|strong` | Read a key. `eventual` = local only; `strong` = quorum read (R=2/3) |
| `PUT` | `/kv/{key}` | — | Write a key (leader only; body: `{"value": "..."}`) |
| `DELETE` | `/kv/{key}` | — | Delete a key (leader only; quorum required) |
| `GET` | `/health` | — | Node health: status, role, wal_size, node_id, quorum_available |
| `GET` | `/admin/config` | — | Node configuration and simulation state |
| `POST` | `/admin/toggle` | — | Toggle `down` or `block_repl` simulation flags |
| `GET` | `/metrics` | — | Request counts, error counts, latency sample count |
| `POST` | `/internal/replicate` | — | Leader-to-follower replication (internal use) |

### Supervisor API (port 9000)

| Method | Path | Description |
|---|---|---|
| `GET` | `/kv/{key}` | Hash-ring-routed read |
| `PUT` | `/kv/{key}` | Hash-ring-routed write (auto-routes to leader) |
| `DELETE` | `/kv/{key}` | Hash-ring-routed delete |
| `GET` | `/cluster/status` | Health + role of all nodes |
| `POST` | `/cluster/start-default` | Start n1 (leader), n2, n3 |
| `POST` | `/cluster/stop-all` | Stop all nodes |
| `POST` | `/node/{id}/start` | Start a specific node |
| `POST` | `/node/{id}/stop` | Stop a specific node |
| `POST` | `/node/{id}/restart` | Restart a specific node |
| `POST` | `/cluster/make-leader` | Promote a node to leader (restarts cluster) |
| `GET` | `/logs/{node_id}` | Last 10 KB of node stdout/stderr |

---

## Design Decisions

### CP over AP

When fewer than W=2 nodes are reachable, writes fail with 503 rather than accepting the write on a single node. This avoids split-brain scenarios where two partitions accept divergent writes for the same key. The system prioritises consistency and partition-tolerance at the cost of availability during network splits.

### WAL before replication

The leader appends and fsyncs the WAL record *before* sending it to followers. This ensures that if the leader crashes mid-replication, it can replay the WAL on restart and retry replication (or the follower's WAL covers their own state). The alternative — committing first then replicating — risks losing acknowledged writes on crash.

### Consistent hash ring

The 100-vnode SHA-1 ring distributes keys evenly across nodes and minimises reshuffling when nodes join or leave. The supervisor uses it to route requests to the node most likely to hold the key locally, reducing cross-node hops for reads. Writes still flow through the leader for quorum enforcement.

### SQLite with WAL journal mode

Each node uses a separate SQLite file with `PRAGMA journal_mode=WAL`. This allows concurrent reads during writes, which matters for follower nodes that serve reads while accepting replication writes concurrently.

### Health polling + failure threshold

Three consecutive health failures (configurable via `_HEALTH_FAIL_THRESHOLD`) are required before a node is marked unavailable. This tolerates transient blips (GC pause, slow response) without prematurely removing a node from the ring and redirecting all its traffic.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `NODE_ID` | `node-1` | Unique node identifier |
| `HTTP_PORT` | `8080` | Port this node listens on |
| `IS_LEADER` | `true` | Whether this node is the write leader |
| `PEERS` | `` | Comma-separated peer URLs |
| `REPLICATION_FACTOR` | `3` | Number of replicas |
| `QUORUM_W` | `2` | Minimum acks for a successful write |
| `QUORUM_R` | `2` | Minimum agreements for a strong read |
| `DB_PATH` | `data/kv.sqlite` | SQLite database path |
| `ENABLE_CLOUDWATCH` | `0` | Set `1` to emit metrics to CloudWatch |
| `AWS_REGION` | `us-east-1` | AWS region for CloudWatch |
| `SUPERVISOR_TOKEN` | `` | Optional bearer token for supervisor API |
