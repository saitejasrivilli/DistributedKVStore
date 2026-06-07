# app/replication.py
import asyncio
import random
from typing import List, Dict, Any

import httpx
from .metrics import metrics

MAX_RETRIES = 3
_BACKOFF_BASE = 0.05   # 50 ms
_BACKOFF_CAP  = 0.5    # 500 ms cap


async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    record: Dict[str, Any],
) -> int | None:
    """
    POST to one peer with exponential backoff + full jitter.
    Returns HTTP status code on any response, None after all retries exhausted.
    Retries only on network/transport errors, not on 4xx/5xx peer responses.
    """
    for attempt in range(MAX_RETRIES):
        try:
            r = await client.post(url, json=record)
            return r.status_code
        except Exception:
            if attempt == MAX_RETRIES - 1:
                return None
            # full jitter: sleep in [0, base * 2^attempt] capped at _BACKOFF_CAP
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
        Fan out to all peers in parallel; each peer retried up to MAX_RETRIES
        times with exponential backoff + full jitter on transport failures.
        Leader always counts as 1 ack.
        """
        if not self.peers:
            return 1

        async with httpx.AsyncClient(timeout=timeout) as client:
            results = await asyncio.gather(
                *[
                    _post_with_retry(client, f"{peer}/internal/replicate", record)
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
