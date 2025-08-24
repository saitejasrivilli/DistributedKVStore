import os, time, threading, statistics
from typing import Dict, List, Any

metrics: Dict[str, Any] = {
    "requests_total": 0,
    "errors_total": 0,
    "latencies_ms": [],         # type: List[float]
    "replication_acks": [],     # type: List[int]
}

def start_exporter_if_enabled():
    # CloudWatch exporter is optional (off by default)
    if os.getenv("ENABLE_CLOUDWATCH","0") != "1":
        return
    try:
        import boto3
    except Exception:
        return

    def export_loop():
        cw = boto3.client("cloudwatch", region_name=os.getenv("AWS_REGION","us-east-1"))
        while True:
            time.sleep(60)
            try:
                lat = metrics["latencies_ms"]; acks = metrics["replication_acks"]
                p50 = statistics.median(lat) if lat else 0.0
                p95 = (sorted(lat)[int(0.95*len(lat))-1] if len(lat) else 0.0)
                cw.put_metric_data(
                  Namespace=os.getenv("CLOUDWATCH_NS","KVStore"),
                  MetricData=[
                    {"MetricName":"RequestsTotal","Value":metrics["requests_total"],"Unit":"Count"},
                    {"MetricName":"ErrorsTotal","Value":metrics["errors_total"],"Unit":"Count"},
                    {"MetricName":"LatencyP50","Value":p50,"Unit":"Milliseconds"},
                    {"MetricName":"LatencyP95","Value":p95,"Unit":"Milliseconds"},
                    {"MetricName":"ReplicationAcksAvg","Value":(sum(acks)/len(acks) if acks else 0),"Unit":"Count"},
                  ])
                lat.clear(); acks.clear()
            except Exception:
                pass

    t = threading.Thread(target=export_loop, daemon=True)
    t.start()
