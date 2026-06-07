import grpc
from app import replication_pb2, replication_pb2_grpc


class ReplicationServicer(replication_pb2_grpc.ReplicationServicer):
    """
    gRPC servicer for leader→follower replication.
    Mirrors the /internal/replicate HTTP endpoint but over a persistent
    binary-framed HTTP/2 channel — lower per-message overhead than JSON/REST.
    """

    def __init__(self, store, wal, state):
        self._store = store
        self._wal = wal
        self._state = state

    async def Replicate(self, request, context):
        if self._state.get("down") or self._state.get("block_repl"):
            return replication_pb2.ReplicateResponse(ack=False, status=503)

        if request.op not in ("put", "del"):
            return replication_pb2.ReplicateResponse(ack=False, status=400)

        if request.op == "put":
            current = self._store.get(request.k)
            if current is not None and current["version"] >= request.ver:
                # Stale or duplicate — reject with 409 (version monotonicity)
                return replication_pb2.ReplicateResponse(ack=False, status=409)
            self._store.put(request.k, request.v.encode(), request.ver)
        else:
            self._store.delete(request.k)

        self._wal.append({"op": request.op, "k": request.k, "v": request.v, "ver": request.ver})
        return replication_pb2.ReplicateResponse(ack=True, status=200)
