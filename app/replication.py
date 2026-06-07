import asyncio
import random
from typing import List, Dict, Any
from urllib.parse import urlparse

import grpc

from app import replication_pb2, replication_pb2_grpc
from .metrics import metrics

MAX_RETRIES = 3
_BACKOFF_BASE = 0.05   # 50 ms
_BACKOFF_CAP  = 0.5    # 500 ms cap


def _grpc_addr(peer_http_url: str) -> str:
    """http://host:8081  →  host:9081  (gRPC port = HTTP port + 1000)"""
    parsed = urlparse(peer_http_url)
    return f"{parsed.hostname}:{parsed.port + 1000}"


async def _grpc_replicate_once(
    stub: replication_pb2_grpc.ReplicationStub,
    record: Dict[str, Any],
    timeout: float,
) -> int | None:
    """Single gRPC Replicate call. Returns HTTP-equivalent status or None on error."""
    try:
        resp = await stub.Replicate(
            replication_pb2.ReplicateRequest(
                op=record["op"],
                k=record["k"],
                v=record.get("v", ""),
                ver=record.get("ver", 0),
            ),
            timeout=timeout,
        )
        return resp.status
    except Exception:
        return None


async def _grpc_replicate_with_retry(
    addr: str,
    record: Dict[str, Any],
    timeout: float,
) -> int | None:
    """
    gRPC unary Replicate call with exponential backoff + full jitter.
    Channel is reused across attempts. Retries only on transport failures,
    not on application-level responses (409, 503).
    """
    async with grpc.aio.insecure_channel(addr) as channel:
        stub = replication_pb2_grpc.ReplicationStub(channel)
        for attempt in range(MAX_RETRIES):
            status = await _grpc_replicate_once(stub, record, timeout)
            if status is not None:
                return status          # got a real response — don't retry
            if attempt < MAX_RETRIES - 1:
                cap = min(_BACKOFF_CAP, _BACKOFF_BASE * (2 ** attempt))
                await asyncio.sleep(random.uniform(0, cap))
    return None


class Replicator:
    def __init__(self, peers: List[str]):
        self.peers = [p for p in peers if p]

    async def replicate_to_followers(
        self,
        record: Dict[str, Any],
        timeout: float = 0.75,
    ) -> int:
        """
        Fan out to all peers in parallel over gRPC (HTTP/2, binary Protobuf).
        Each peer retried up to MAX_RETRIES times with exponential backoff +
        full jitter on transport failures. Leader always counts as 1 ack.
        """
        if not self.peers:
            return 1

        results = await asyncio.gather(
            *[
                _grpc_replicate_with_retry(_grpc_addr(peer), record, timeout)
                for peer in self.peers
            ],
            return_exceptions=True,
        )

        acks = 1  # leader counts itself
        for status in results:
            if status == 200:
                acks += 1

        metrics["replication_acks"].append(acks)
        return acks
