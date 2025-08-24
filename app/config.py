import os

def _split_peers(s: str):
    if not s: return []
    return [p.strip() for p in s.split(",") if p.strip()]

class Settings:
    NODE_ID = os.getenv("NODE_ID", "node-1")
    HTTP_PORT = int(os.getenv("HTTP_PORT", "8080"))
    IS_LEADER = os.getenv("IS_LEADER", "true").lower() == "true"
    PEERS = _split_peers(os.getenv("PEERS", ""))  # e.g. "http://n2:8081,http://n3:8082"
    REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
    QUORUM_W = int(os.getenv("QUORUM_W", "2"))
    QUORUM_R = int(os.getenv("QUORUM_R", "2"))
    DB_PATH = os.getenv("DB_PATH", "data/kv.sqlite")
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    ENABLE_CLOUDWATCH = os.getenv("ENABLE_CLOUDWATCH", "0") == "1"

settings = Settings()
