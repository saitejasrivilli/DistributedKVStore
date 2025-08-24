import json, os, threading
from typing import Iterable, Dict, Any

class WAL:
    def __init__(self, path="data/wal.log"):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.path = path
        self.lock = threading.Lock()

    def append(self, record: Dict[str, Any]) -> int:
        line = json.dumps(record, separators=(",",":")) + "\n"
        with self.lock, open(self.path, "a") as f:
            off = f.tell()
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
            return off

    def replay(self) -> Iterable[Dict[str, Any]]:
        if not os.path.exists(self.path): return []
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    yield json.loads(line)
