# app/replication.py
import asyncio
from typing import List, Dict, Any

import httpx
from .metrics import metrics


class Replicator:
    def __init__(self, peers: List[str]):
        # filter out empty/None peers
        self.peers = [p for p in peers if p]

    async def replicate_to_followers(
        self,
        record: Dict[str, Any],
        timeout: float = 0.75,
    ) -> int:
        """
        POST the record to each peer's /internal/replicate endpoint.
        Count 200 OK responses as acks. Leader counts as 1.
        """
        if not self.peers:
            return 1  # only leader/self

        async with httpx.AsyncClient(timeout=timeout) as client:
            tasks = [
                client.post(f"{peer}/internal/replicate", json=record)
                for peer in self.peers
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        acks = 1  # leader counts itself
        for r in results:
            if isinstance(r, Exception):
                # network/timeout/etc. -> no ack
                continue
            if getattr(r, "status_code", 500) == 200:
                acks += 1

        metrics["replication_acks"].append(acks)
        return acks
