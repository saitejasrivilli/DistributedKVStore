import sqlite3, time, threading
from typing import Optional, Dict, Any
import os
class SQLiteStore:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("""
          CREATE TABLE IF NOT EXISTS kv (
            k   TEXT PRIMARY KEY,
            v   BLOB NOT NULL,
            ver INTEGER NOT NULL,
            ts  REAL NOT NULL
          )
        """)
        self.conn.commit()

    def get(self, k: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT v, ver, ts FROM kv WHERE k=?", (k,))
        row = cur.fetchone()
        return None if not row else {"value": row[0], "version": row[1], "ts": row[2]}

    def put(self, k: str, v: bytes, ver: Optional[int] = None) -> int:
        now = time.time()
        with self._lock:
            if ver is None:
                row = self.get(k)
                ver = 1 if not row else row["version"] + 1
            self.conn.execute(
              "INSERT INTO kv(k,v,ver,ts) VALUES(?,?,?,?) "
              "ON CONFLICT(k) DO UPDATE SET v=excluded.v, ver=excluded.ver, ts=excluded.ts",
              (k, v, ver, now)
            )
            self.conn.commit()
            return ver

    def delete(self, k: str):
        with self._lock:
            self.conn.execute("DELETE FROM kv WHERE k=?", (k,))
            self.conn.commit()
