#!/usr/bin/env python3
"""Collect Kubernetes and PostgreSQL benchmark telemetry as JSON Lines."""

import argparse
from datetime import datetime, timezone
import json
import os
import subprocess
import time

try:
    import psycopg
except ImportError as exc:  # pragma: no cover
    raise SystemExit("benchmark_observer.py requires psycopg 3") from exc


def command_json(*args):
    result = subprocess.run(args, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def cpu_cores(value):
    units = {"n": 1e-9, "u": 1e-6, "m": 1e-3}
    if value[-1:] in units:
        return float(value[:-1]) * units[value[-1]]
    return float(value)


def memory_bytes(value):
    units = {
        "Ki": 1024,
        "Mi": 1024 ** 2,
        "Gi": 1024 ** 3,
        "K": 1000,
        "M": 1000 ** 2,
        "G": 1000 ** 3,
    }
    for suffix, multiplier in units.items():
        if value.endswith(suffix):
            return int(float(value[:-len(suffix)]) * multiplier)
    return int(value)


DB_SQL = """
SELECT json_build_object(
  'connections', (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()),
  'active_connections', (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'active'),
  'available', count(*) FILTER (WHERE status IN ('available', 'scheduled', 'retryable')),
  'executing', count(*) FILTER (WHERE status = 'executing'),
  'completed', count(*) FILTER (WHERE status = 'completed'),
  'discarded', count(*) FILTER (WHERE status = 'discarded'),
  'jobs_total', count(*),
  'xact_commit', (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()),
  'xact_rollback', (SELECT xact_rollback FROM pg_stat_database WHERE datname = current_database()),
  'blks_read', (SELECT blks_read FROM pg_stat_database WHERE datname = current_database()),
  'blks_hit', (SELECT blks_hit FROM pg_stat_database WHERE datname = current_database()),
  'temp_bytes', (SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()),
  'wal_bytes', (SELECT wal_bytes::text FROM pg_stat_wal)
) FROM quasar_jobs
"""


def collect_database(connection):
    with connection.cursor() as cursor:
        cursor.execute(DB_SQL)
        return cursor.fetchone()[0]


def collect_kubernetes(namespace):
    pod_state = command_json(
        "kubectl", "-n", namespace, "get", "pods", "-l", "app.kubernetes.io/name=report-service", "-o", "json"
    )
    application_pods = {item["metadata"]["name"] for item in pod_state.get("items", [])}
    metrics = command_json(
        "kubectl", "get", "--raw", f"/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods"
    )
    pods = []
    for item in metrics.get("items", []):
        name = item["metadata"]["name"]
        if name not in application_pods:
            continue
        cpu = sum(cpu_cores(c["usage"]["cpu"]) for c in item.get("containers", []))
        memory = sum(memory_bytes(c["usage"]["memory"]) for c in item.get("containers", []))
        pods.append({"name": name, "cpu_cores": round(cpu, 6), "memory_bytes": memory})

    hpa = command_json("kubectl", "-n", namespace, "get", "hpa", "report-service", "-o", "json")
    deployment = command_json("kubectl", "-n", namespace, "get", "deployment", "report-service", "-o", "json")
    restarts = 0
    ready = 0
    for pod in pod_state.get("items", []):
        statuses = pod.get("status", {}).get("containerStatuses", [])
        restarts += sum(status.get("restartCount", 0) for status in statuses)
        ready += int(bool(statuses) and all(status.get("ready", False) for status in statuses))

    return {
        "pods": pods,
        "application": {
            "desired_replicas": deployment.get("spec", {}).get("replicas"),
            "available_replicas": deployment.get("status", {}).get("availableReplicas", 0),
            "ready_pods": ready,
            "restarts": restarts,
        },
        "hpa": {
            "min_replicas": hpa.get("spec", {}).get("minReplicas"),
            "max_replicas": hpa.get("spec", {}).get("maxReplicas"),
            "current_replicas": hpa.get("status", {}).get("currentReplicas"),
            "desired_replicas": hpa.get("status", {}).get("desiredReplicas"),
            "current_metrics": hpa.get("status", {}).get("currentMetrics", []),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="reports")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--duration", type=float, default=3600.0)
    parser.add_argument("--label", default="benchmark-suite")
    parser.add_argument(
        "--source",
        choices=["all", "kubernetes", "postgresql"],
        default="all",
        help="Telemetry source; split collection keeps PostgreSQL traffic inside the cluster.",
    )
    args = parser.parse_args()
    database_url = os.environ.get("DATABASE_URL")
    if args.source in ("all", "postgresql") and not database_url:
        raise SystemExit("DATABASE_URL is required")

    started = time.monotonic()
    connection = None
    while time.monotonic() - started < args.duration:
        sample = {
            "type": "observer_sample",
            "label": args.label,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timestamp_epoch_ms": int(time.time() * 1000),
            "elapsed_s": round(time.monotonic() - started, 3),
        }
        if args.source in ("all", "kubernetes"):
            try:
                sample["kubernetes"] = collect_kubernetes(args.namespace)
            except Exception as exc:
                sample["kubernetes_error"] = f"{type(exc).__name__}: {exc}"
        if args.source in ("all", "postgresql"):
            try:
                if connection is None or connection.closed:
                    connection = psycopg.connect(database_url, connect_timeout=5, autocommit=True)
                sample["postgresql"] = collect_database(connection)
            except Exception as exc:
                sample["postgresql_error"] = f"{type(exc).__name__}: {exc}"
                if connection is not None:
                    connection.close()
                connection = None
        print(json.dumps(sample, sort_keys=True), flush=True)
        sleep_for = args.interval - ((time.monotonic() - started) % args.interval)
        time.sleep(max(0.05, sleep_for))
    if connection is not None:
        connection.close()


if __name__ == "__main__":
    main()
