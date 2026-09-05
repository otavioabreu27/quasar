#!/usr/bin/env python3
"""Collect Kubernetes and PostgreSQL benchmark telemetry as JSON Lines."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
import subprocess
import time
from urllib.request import urlopen

try:
    import psycopg
except ImportError as exc:  # pragma: no cover
    raise SystemExit("benchmark_observer.py requires psycopg 3") from exc


def command_json(*args):
    result = subprocess.run(args, check=True, capture_output=True, text=True, timeout=10)
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
  'available', (SELECT count(*) FROM quasar_jobs WHERE queue = 'reports' AND status IN ('available', 'scheduled', 'retryable')),
  'executing', (SELECT count(*) FROM quasar_jobs WHERE status = 'executing'),
  'jobs_total_estimate', (SELECT n_live_tup FROM pg_stat_user_tables WHERE relid = 'quasar_jobs'::regclass),
  'dead_tuples_estimate', (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relid = 'quasar_jobs'::regclass),
  'autovacuum_count', (SELECT autovacuum_count FROM pg_stat_user_tables WHERE relid = 'quasar_jobs'::regclass),
  'jobs_table_bytes', pg_table_size('quasar_jobs'),
  'jobs_index_bytes', pg_indexes_size('quasar_jobs'),
  'database_stats_reset', (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()),
  'wal_stats_reset', (SELECT stats_reset FROM pg_stat_wal),
  'waits', (SELECT coalesce(json_agg(w), '[]') FROM (
      SELECT application_name, state, wait_event_type, wait_event, count(*) AS connections
      FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()
      GROUP BY application_name, state, wait_event_type, wait_event
    ) w),
  'xact_commit', (SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()),
  'xact_rollback', (SELECT xact_rollback FROM pg_stat_database WHERE datname = current_database()),
  'blks_read', (SELECT blks_read FROM pg_stat_database WHERE datname = current_database()),
  'blks_hit', (SELECT blks_hit FROM pg_stat_database WHERE datname = current_database()),
  'temp_bytes', (SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()),
  'wal_bytes', (SELECT wal_bytes::text FROM pg_stat_wal),
  'wal_records', (SELECT wal_records FROM pg_stat_wal),
  'wal_sync', (SELECT wal_sync FROM pg_stat_wal)
)
"""


def collect_database(connection):
    with connection.cursor() as cursor:
        cursor.execute(DB_SQL)
        sample = cursor.fetchone()[0]
        sample["scope"] = {"available": "reports_queue", "executing": "all_queues",
                           "transactions": "database_including_reads", "wal": "postgresql_instance"}
        cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')")
        if cursor.fetchone()[0]:
            # No query text/parameters in output. Match operation, aggregate by
            # stable queryid. Never reset shared statistics during a benchmark.
            try:
                cursor.execute("SELECT stats_reset FROM pg_stat_statements_info")
                sample["statements_stats_reset"] = cursor.fetchone()[0].isoformat()
                cursor.execute("""SELECT queryid::text, calls, rows, total_exec_time,
                    shared_blks_hit, shared_blks_read, wal_bytes::text,
                    CASE WHEN query LIKE '%%WITH claimable%%' THEN 'claim'
                         WHEN query LIKE '%%WITH guard%%' THEN 'reaper'
                         WHEN query LIKE '%%FROM completion%%' THEN 'complete_many'
                         WHEN query LIKE '%%FROM failure%%' THEN 'fail_many'
                         WHEN query LIKE '%%pg_notify%%' THEN 'enqueue'
                         WHEN query LIKE '%%LEFT JOIN example_reports%%' THEN 'status'
                         WHEN query LIKE '%%UPDATE quasar_jobs%%' THEN 'job_update'
                         WHEN query LIKE '%%INSERT INTO example_reports%%' THEN 'business_insert'
                         WHEN query LIKE '%%retention_lock%%' THEN 'retention'
                         ELSE 'other' END AS operation
                    FROM pg_stat_statements
                    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                      AND (query LIKE '%%quasar_jobs%%' OR query LIKE 'INSERT INTO example_reports%%')
                      AND query NOT LIKE '%%pg_stat_%%'""")
                names = [field.name for field in cursor.description]
                sample["statements"] = [dict(zip(names, row)) for row in cursor.fetchall()]
            except psycopg.Error as exc:
                sample["statements_unavailable"] = type(exc).__name__
        else:
            sample["statements_unavailable"] = "extension_not_installed"
        return sample


def collect_kubernetes(namespace):
    pod_state = command_json(
        "kubectl", "-n", namespace, "get", "pods", "-l", "app.kubernetes.io/name=report-service", "-o", "json"
    )
    application_pods = {item["metadata"]["name"] for item in pod_state.get("items", [])}
    metrics = command_json(
        "kubectl", "get", "--raw", f"/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods"
    )
    pods = []
    generator_pods = []
    for item in metrics.get("items", []):
        name = item["metadata"]["name"]
        cpu = sum(cpu_cores(c["usage"]["cpu"]) for c in item.get("containers", []))
        memory = sum(memory_bytes(c["usage"]["memory"]) for c in item.get("containers", []))
        target = pods if name in application_pods else generator_pods if name.startswith("quasar-bench-") else None
        if target is not None:
            target.append({"name": name, "cpu_cores": round(cpu, 6), "memory_bytes": memory,
                           "timestamp": item.get("timestamp"), "window": item.get("window")})

    def runtime_metrics(pod):
        name = pod['metadata']['name']
        try:
            value = command_json("kubectl", "get", "--raw",
                f"/api/v1/namespaces/{namespace}/pods/{name}:8080/proxy/internal/metrics")
            return name, {"uid": pod['metadata']['uid'], "metrics": value}
        except Exception as exc:
            return name, {"error": type(exc).__name__}
    with ThreadPoolExecutor(max_workers=8) as executor:
        runtime = dict(executor.map(runtime_metrics, pod_state.get('items', [])))

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
        "generator_pods": generator_pods,
        "runtime": runtime,
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
    parser.add_argument("--runtime-url", help="Direct pod/service URL; prefer each pod for counter deltas")
    parser.add_argument("--once", action="store_true", help="One sample, nonzero exit if collection fails")
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
                    connection = psycopg.connect(database_url, connect_timeout=5, autocommit=True,
                        application_name="quasar-benchmark-observer", options="-c statement_timeout=2000")
                sample["postgresql"] = collect_database(connection)
            except Exception as exc:
                sample["postgresql_error"] = type(exc).__name__
                if connection is not None:
                    connection.close()
                connection = None
        if args.runtime_url:
            try:
                with urlopen(args.runtime_url.rstrip('/') + '/internal/metrics', timeout=2) as response:
                    sample["runtime"] = json.load(response)
            except Exception as exc:
                sample["runtime_error"] = type(exc).__name__
        print(json.dumps(sample, sort_keys=True), flush=True)
        if args.once:
            if connection is not None:
                connection.close()
            return 2 if any(key.endswith("_error") for key in sample) else 0
        sleep_for = args.interval - ((time.monotonic() - started) % args.interval)
        time.sleep(max(0.05, sleep_for))
    if connection is not None:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
