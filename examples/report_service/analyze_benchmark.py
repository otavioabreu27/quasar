#!/usr/bin/env python3
"""Aggregate benchmark JSONL and observer telemetry into CSV and JSON."""

import argparse
import csv
from collections import defaultdict
import json
import math
from pathlib import Path
import re
import statistics


def load_jsonl(path):
    records = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.startswith("{"):
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return records


def median(values):
    clean = [value for value in values if value is not None]
    return round(statistics.median(clean), 4) if clean else None


def percentile(values, fraction):
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return None
    return round(clean[max(0, math.ceil(len(clean) * fraction) - 1)], 4)


def pod_totals(sample, prefix):
    pods = [pod for pod in sample.get("kubernetes", {}).get("pods", []) if pod["name"].startswith(prefix)]
    return (
        sum(pod["cpu_cores"] for pod in pods),
        sum(pod["memory_bytes"] for pod in pods),
        len(pods),
    )


def counter_at(samples, timestamp_ms, key):
    points = []
    for sample in samples:
        value = sample.get("postgresql", {}).get(key)
        if value is not None:
            points.append((sample["timestamp_epoch_ms"], float(value)))
    if not points:
        return None
    before = [point for point in points if point[0] <= timestamp_ms]
    after = [point for point in points if point[0] >= timestamp_ms]
    left = before[-1] if before else points[0]
    right = after[0] if after else points[-1]
    if left[0] == right[0]:
        return left[1]
    ratio = (timestamp_ms - left[0]) / (right[0] - left[0])
    return left[1] + ratio * (right[1] - left[1])


def resource_metrics(summary, observer):
    start = summary["started_epoch_ms"]
    end = summary["finished_epoch_ms"]
    samples = [sample for sample in observer if start <= sample.get("timestamp_epoch_ms", 0) <= end]
    cpu = []
    memory = []
    memory_per_pod = []
    replicas = []
    connections = []
    restarts = []
    for sample in samples:
        total_cpu, total_memory, pod_count = pod_totals(sample, "report-service-")
        if pod_count:
            cpu.append(total_cpu)
            memory.append(total_memory)
            memory_per_pod.append(total_memory / pod_count)
        application = sample.get("kubernetes", {}).get("application", {})
        if application.get("available_replicas") is not None:
            replicas.append(application["available_replicas"])
        database = sample.get("postgresql", {})
        if database.get("connections") is not None:
            connections.append(database["connections"])
        if application.get("restarts") is not None:
            restarts.append(application["restarts"])

    duration = summary["total_elapsed_s"]
    completed = summary["completed"]
    avg_cpu = statistics.mean(cpu) if cpu else None
    posts_and_polls = summary["http"]["total_requests"]
    result = {
        "observer_samples": len(samples),
        "app_cpu_cores_avg": round(avg_cpu, 4) if avg_cpu is not None else None,
        "app_cpu_cores_p95": percentile(cpu, 0.95),
        "app_cpu_cores_peak": round(max(cpu), 4) if cpu else None,
        "app_cpu_ms_per_job": round(avg_cpu * duration * 1000 / completed, 4) if avg_cpu is not None and completed else None,
        "app_cpu_ms_per_http_request": round(avg_cpu * duration * 1000 / posts_and_polls, 4) if avg_cpu is not None and posts_and_polls else None,
        "app_memory_mib_avg": round(statistics.mean(memory) / 2**20, 3) if memory else None,
        "app_memory_mib_peak": round(max(memory) / 2**20, 3) if memory else None,
        "app_memory_mib_per_pod_avg": round(statistics.mean(memory_per_pod) / 2**20, 3) if memory_per_pod else None,
        "replicas_min": min(replicas) if replicas else None,
        "replicas_max": max(replicas) if replicas else None,
        "db_connections_max": max(connections) if connections else None,
        "pod_restarts_max": max(restarts) if restarts else None,
    }
    for key, output_key in [
        ("wal_bytes", "wal_bytes_per_job"),
        ("xact_commit", "db_commits_per_job"),
        ("blks_read", "db_blocks_read_per_job"),
    ]:
        first = counter_at(observer, start, key)
        last = counter_at(observer, end, key)
        result[output_key] = round((last - first) / completed, 4) if first is not None and last is not None and completed else None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result_dir", type=Path)
    args = parser.parse_args()
    result_dir = args.result_dir
    observer = load_jsonl(result_dir / "observer.jsonl")

    fixed_runs = []
    pattern = re.compile(r"fixed-r(\d+)-t(\d+)-n(\d+)\.jsonl$")
    for path in sorted(result_dir.glob("fixed-*.jsonl")):
        match = pattern.match(path.name)
        if not match:
            continue
        summary = next(record for record in load_jsonl(path) if record.get("type") == "summary")
        resources = resource_metrics(summary, observer)
        fixed_runs.append({
            "replicas": int(match.group(1)),
            "target_rate": int(match.group(2)),
            "repetition": int(match.group(3)),
            "accepted": summary["accepted"],
            "completed": summary["completed"],
            "accept_rate": summary["achieved_accept_rate_jobs_s"],
            "total_elapsed_s": summary["total_elapsed_s"],
            "enqueue_p95_ms": summary["latency"]["enqueue"]["p95_ms"],
            "enqueue_p99_ms": summary["latency"]["enqueue"]["p99_ms"],
            "e2e_p50_ms": summary["latency"]["server_end_to_end"]["p50_ms"],
            "e2e_p95_ms": summary["latency"]["server_end_to_end"]["p95_ms"],
            "e2e_p99_ms": summary["latency"]["server_end_to_end"]["p99_ms"],
            "e2e_max_ms": summary["latency"]["server_end_to_end"]["max_ms"],
            "max_pending": summary["max_client_pending"],
            "schedule_lag_p95_ms": summary["latency"]["schedule_lag"]["p95_ms"],
            "submit_errors": sum(summary["submit_errors"].values()),
            "terminal_failures": summary["terminal_failures"],
            **resources,
        })

    grouped = defaultdict(list)
    for run in fixed_runs:
        grouped[(run["replicas"], run["target_rate"])].append(run)
    fixed_summary = []
    metric_keys = [
        "accept_rate", "total_elapsed_s", "enqueue_p95_ms", "enqueue_p99_ms",
        "e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms", "e2e_max_ms", "max_pending",
        "schedule_lag_p95_ms", "app_cpu_cores_avg", "app_cpu_ms_per_job",
        "app_cpu_ms_per_http_request", "app_memory_mib_avg", "app_memory_mib_peak",
        "app_memory_mib_per_pod_avg", "db_connections_max", "wal_bytes_per_job",
        "db_commits_per_job", "db_blocks_read_per_job",
    ]
    for (replicas, target), runs in sorted(grouped.items()):
        row = {
            "replicas": replicas,
            "target_rate": target,
            "runs": len(runs),
            "scheduled_total": sum(run["accepted"] + run["submit_errors"] for run in runs),
            "accepted_total": sum(run["accepted"] for run in runs),
            "completed_total": sum(run["completed"] for run in runs),
            "submit_errors_total": sum(run["submit_errors"] for run in runs),
            "terminal_failures_total": sum(run["terminal_failures"] for run in runs),
        }
        for key in metric_keys:
            row[f"{key}_median"] = median([run[key] for run in runs])
        e2e_values = [run["e2e_p95_ms"] for run in runs]
        row["e2e_p95_coefficient_of_variation"] = round(
            statistics.pstdev(e2e_values) / statistics.mean(e2e_values), 4
        ) if statistics.mean(e2e_values) else 0.0
        fixed_summary.append(row)

    elastic_summary = next(
        record for record in load_jsonl(result_dir / "elastic-30m-rerun.jsonl")
        if record.get("type") == "summary"
    )
    elastic_observer = load_jsonl(result_dir / "observer-elastic.jsonl")
    elastic_resources = resource_metrics(elastic_summary, elastic_observer)
    elastic_samples = [
        record for record in load_jsonl(result_dir / "elastic-30m-rerun.jsonl")
        if record.get("type") == "sample"
    ]
    stages = []
    offset = 0.0
    for index, stage in enumerate(elastic_summary["configured_stages"], start=1):
        stage_start = elastic_summary["started_epoch_ms"] + int(offset * 1000)
        stage_end = stage_start + int(stage["duration_s"] * 1000)
        observer_window = [s for s in elastic_observer if stage_start <= s.get("timestamp_epoch_ms", 0) < stage_end]
        client_window = [s for s in elastic_samples if stage_start <= s.get("timestamp_epoch_ms", 0) < stage_end]
        cpu = [pod_totals(sample, "report-service-")[0] for sample in observer_window]
        memory_per_pod = []
        replicas = []
        connections = []
        for sample in observer_window:
            _, total_memory, count = pod_totals(sample, "report-service-")
            if count:
                memory_per_pod.append(total_memory / count / 2**20)
            app = sample.get("kubernetes", {}).get("application", {})
            if app.get("available_replicas") is not None:
                replicas.append(app["available_replicas"])
            db = sample.get("postgresql", {})
            if db.get("connections") is not None:
                connections.append(db["connections"])
        wal_start = counter_at(elastic_observer, stage_start, "wal_bytes")
        wal_end = counter_at(elastic_observer, stage_end, "wal_bytes")
        planned_jobs = int(stage["duration_s"] * stage["rate_jobs_s"])
        stages.append({
            "stage": index,
            "offset_s": offset,
            "duration_s": stage["duration_s"],
            "target_rate": stage["rate_jobs_s"],
            "planned_jobs": planned_jobs,
            "max_pending": max((sample["pending"] for sample in client_window), default=None),
            "app_cpu_cores_avg": round(statistics.mean(cpu), 4) if cpu else None,
            "app_cpu_cores_p95": percentile(cpu, 0.95),
            "memory_mib_per_pod_avg": round(statistics.mean(memory_per_pod), 3) if memory_per_pod else None,
            "replicas_min": min(replicas) if replicas else None,
            "replicas_max": max(replicas) if replicas else None,
            "db_connections_max": max(connections) if connections else None,
            "wal_bytes_per_planned_job": round((wal_end - wal_start) / planned_jobs, 3) if wal_start is not None and wal_end is not None else None,
        })
        offset += stage["duration_s"]

    replica_timeline = []
    previous = None
    for sample in elastic_observer:
        timestamp = sample.get("timestamp_epoch_ms", 0)
        if not elastic_summary["started_epoch_ms"] <= timestamp <= elastic_summary["finished_epoch_ms"]:
            continue
        replicas = sample.get("kubernetes", {}).get("hpa", {}).get("current_replicas")
        if replicas is not None and replicas != previous:
            replica_timeline.append({
                "elapsed_s": round((timestamp - elastic_summary["started_epoch_ms"]) / 1000, 3),
                "replicas": replicas,
            })
            previous = replicas

    output = {
        "fixed_runs": fixed_runs,
        "fixed_summary": fixed_summary,
        "elastic": {
            "summary": elastic_summary,
            "resources": elastic_resources,
            "stages": stages,
            "replica_timeline": replica_timeline,
        },
    }
    (result_dir / "analysis.json").write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")

    for filename, rows in [("fixed-runs.csv", fixed_runs), ("fixed-summary.csv", fixed_summary), ("elastic-stages.csv", stages)]:
        with (result_dir / filename).open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()
