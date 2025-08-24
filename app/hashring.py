import hashlib, bisect
from typing import List

class HashRing:
    def __init__(self, nodes: List[str], replicas: int = 100):
        self.ring = []
        for n in nodes:
            for r in range(replicas):
                h = self._h(f"{n}:{r}")
                self.ring.append((h, n))
        self.ring.sort()

    def _h(self, s: str) -> int:
        return int(hashlib.sha1(s.encode()).hexdigest(), 16)

    def owners(self, key: str, rf: int) -> List[str]:
        if not self.ring: return []
        h = self._h(key)
        i = bisect.bisect(self.ring, (h, ""))
        seen, out, j = set(), [], 0
        while len(out) < rf and j < len(self.ring) * 2:
            n = self.ring[(i + j) % len(self.ring)][1]
            if n not in seen:
                out.append(n); seen.add(n)
            j += 1
        return out
