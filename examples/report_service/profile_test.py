#!/usr/bin/env python3
"""Isolated profiling: disposable PostgreSQL + four separate BEAM instances.

Requires Docker, Gleam, mise/Rebar, and OTP 27+. Never uses the normal DATABASE_URL.
All resources created here are stopped in finally; no persistent DB volumes.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import uuid

from load_test import call

ROOT = Path(__file__).resolve().parent


def command(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()


class Instance:
    def __init__(self, port, database_url, workers, connections, trace):
        self.port = port
        self.latest = {}
        self.runtime_metrics = {}
        self.lines = []
        env = dict(os.environ, DATABASE_URL=database_url, HOST="127.0.0.1",
                   PORT=str(port), INSTANCE_ID=f"profile-{port}",
                   PROFILE_WORKERS=str(workers), PROFILE_CONNECTIONS=str(connections),
                   PROFILE_TRACE="1" if trace else "0")
        self.process = subprocess.Popen(
            ["mise", "exec", "rebar@3.27.0", "--", "gleam", "run", "-m", "profile_service"],
            cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, start_new_session=True,
        )
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        for line in self.process.stdout:
            if line.startswith("PROFILE "):
                self.latest = json.loads(line[len("PROFILE "):])
            elif line.startswith("RUNTIME_METRICS "):
                self.runtime_metrics = json.loads(line[len("RUNTIME_METRICS "):])
            else:
                self.lines.append(line.rstrip())
                self.lines = self.lines[-20:]

    def ready(self):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("Instance failed: " + "\n".join(self.lines))
            try:
                if call(f"http://127.0.0.1:{self.port}/health/ready")[0] == 200:
                    return
            except Exception:
                # Connection libraries wrap startup refusal in different
                # exception types. The process status and deadline below are
                # the authoritative failure checks during readiness.
                pass
            time.sleep(.1)
        raise TimeoutError("Instance not ready: " + "\n".join(self.lines))

    def stop(self):
        # Only our explicitly created process group, never another BEAM node.
        try:
            os.killpg(self.process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=5)
        self.reader.join(timeout=1)


def submit_jobs(urls, jobs, clients):
    def submit(index):
        started = time.monotonic()
        status, body = call(urls[index % len(urls)] + "/reports/1000000", "POST")
        assert status == 202, (status, body)
        return int(body["job_id"]), (time.monotonic() - started) * 1000
    with ThreadPoolExecutor(max_workers=clients) as pool:
        rows = list(pool.map(submit, range(jobs)))
    assert len({row[0] for row in rows}) == jobs
    return rows


def merge_trace(before, after):
    totals = {}
    for old, new in zip(before, after):
        for label, current in new.items():
            prior = old.get(label, {})
            item = totals.setdefault(label, dict(count=0, total_us=0, histogram=Counter(),
                                                summed_peak_concurrency=0))
            item["count"] += current["count"] - prior.get("count", 0)
            item["total_us"] += current["total_us"] - prior.get("total_us", 0)
            item["summed_peak_concurrency"] += current["peak"]
            for key, count in current["histogram"].items():
                item["histogram"][int(key)] += count - prior.get("histogram", {}).get(key, 0)
    for item in totals.values():
        hist = item.pop("histogram")
        assert all(count >= 0 for count in hist.values()), "trace counter reset"
        cumulative = 0
        item["p95_upper_ms"] = None
        for bucket, count in sorted(hist.items()):
            cumulative += count
            if cumulative >= math.ceil(item["count"] * .95) and item["count"]:
                item["p95_upper_ms"] = bucket / 1000
                break
        item["mean_ms"] = round(item["total_us"] / max(1, item["count"]) / 1000, 3)
        item["summed_duration_ms"] = round(item.pop("total_us") / 1000, 3)
    return totals


def merge_runtime(before, after):
    """Merge cumulative reporter snapshots and calculate latency percentiles."""
    counters = ["claims", "claim_requested", "claim_returned", "empty_claims",
                "jobs_started", "jobs_completed", "lease_renewals",
                "persistence_failures", "claim_failures"]
    merged = {key: 0 for key in counters}
    merged["jobs_executing"] = sum(item.get("jobs_executing", 0) for item in after)
    merged["summed_peak_jobs_executing"] = sum(
        item.get("jobs_executing_peak", 0) for item in after)
    for old, new in zip(before, after):
        for key in counters:
            merged[key] += new.get(key, 0) - old.get(key, 0)
    for metric in ["claim_duration_ms", "completion_duration_ms"]:
        histogram = Counter()
        for old, new in zip(before, after):
            for bucket, count in new.get(metric, {}).items():
                histogram[int(bucket)] += count - old.get(metric, {}).get(bucket, 0)
        total = sum(histogram.values())
        cumulative = 0
        p95 = None
        weighted = sum(bucket * count for bucket, count in histogram.items())
        for bucket, count in sorted(histogram.items()):
            cumulative += count
            if total and cumulative >= math.ceil(total * .95):
                p95 = bucket
                break
        merged[metric] = {
            "count": total,
            "mean_ms": round(weighted / total, 3) if total else None,
            "p95_upper_ms": p95,
        }
    return merged


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args()
    if min(args.jobs, args.clients, args.repeats) < 1:
        parser.error("jobs, clients and repeats must be positive")
    container = "quasar-profile-" + uuid.uuid4().hex[:12]
    created = False
    instances = []
    results = []

    def sql(query):
        return command("docker", "exec", container, "psql", "-X", "-tA", "-v",
                       "ON_ERROR_STOP=1", "-U", "reports", "-d", "reports", "-c", query)

    def wait_batch(low, high):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            # Read one aggregate instead of 1000+ sequential HTTP requests.
            rows = json.loads(sql(f"""SELECT json_build_object(
                'total', count(*), 'completed', count(*) FILTER (WHERE status='completed'),
                'failed', count(*) FILTER (WHERE status IN ('discarded','cancelled')))
                FROM quasar_jobs WHERE id BETWEEN {low} AND {high}"""))
            assert not rows["failed"], rows
            if rows["completed"] == rows["total"]:
                return
            time.sleep(.1)
        raise TimeoutError("Batch did not complete")

    try:
        for port in range(19181, 19185):
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", port))  # Fail rather than replace a listener.
        command("docker", "run", "--detach", "--rm", "--name", container,
                "-e", "POSTGRES_USER=reports", "-e", "POSTGRES_PASSWORD=profile_local_only",
                "-e", "POSTGRES_DB=reports", "-p", "127.0.0.1::5432", "postgres:16-alpine",
                "-c", "shared_preload_libraries=pg_stat_statements",
                "-c", "track_io_timing=on", "-c", "pg_stat_statements.track=all")
        created = True
        port = command("docker", "port", container, "5432/tcp").rsplit(":", 1)[1]
        db_url = f"postgresql://reports:profile_local_only@127.0.0.1:{port}/reports"
        for attempt in range(100):
            try:
                sql("SELECT 1")
                break
            except subprocess.CalledProcessError:
                if attempt == 99:
                    raise
                time.sleep(.1)
        sql("CREATE EXTENSION pg_stat_statements")
        print("Disposable PostgreSQL ready; existing services untouched.", flush=True)
        # Hold HTTP load and DB pool size fixed when changing worker capacity.
        cases = [("control_2_workers", 2, 12, False),
                 ("traced_2_workers", 2, 12, True),
                 ("traced_8_workers", 8, 12, True)]
        for name, workers, connections, trace in cases:
            for instance in instances:
                instance.stop()
            instances = []
            for port in range(19181, 19185):
                instance = Instance(port, db_url, workers, connections, trace)
                instances.append(instance)
                instance.ready()  # Schema creation is serialized.
            urls = [f"http://127.0.0.1:{i.port}" for i in instances]
            for repetition in range(args.repeats):
                # Equalize table size across trials. This database is disposable
                # and only this harness writes jobs; all prior jobs are finished.
                sql("TRUNCATE example_reports, quasar_jobs RESTART IDENTITY")
                warm = submit_jobs(urls, 20, 4)
                wait_batch(min(row[0] for row in warm), max(row[0] for row in warm))
                sql("VACUUM ANALYZE quasar_jobs")
                sql("VACUUM ANALYZE example_reports")
                time.sleep(1)
                print(f"Running {name}, repetition {repetition + 1}...", flush=True)
                sql("SELECT pg_stat_statements_reset()")  # Only the disposable DB.
                before = [i.latest for i in instances]
                runtime_before = [i.runtime_metrics for i in instances]
                waits = Counter()
                sample_count = []
                sample_errors = []
                stop_sampling = threading.Event()

                def sample_waits():
                    while not stop_sampling.is_set():
                        try:
                            snapshot = json.loads(sql("""SELECT coalesce(json_object_agg(label,n),'{}')
                                FROM (SELECT CASE WHEN state='idle' THEN 'idle'
                                  ELSE coalesce(wait_event_type || ':' || wait_event, state) END AS label,
                                  count(*) n FROM pg_stat_activity WHERE datname='reports'
                                  AND pid <> pg_backend_pid() GROUP BY 1) t"""))
                            waits.update(snapshot)
                            sample_count.append(1)
                        except Exception as error:
                            sample_errors.append(type(error).__name__)
                            return
                        stop_sampling.wait(.05)

                sampler = threading.Thread(target=sample_waits, daemon=True)
                sampler.start()
                try:
                    started = time.monotonic()
                    submitted = submit_jobs(urls, args.jobs, args.clients)
                    submission_time = time.monotonic() - started
                    low, high = min(r[0] for r in submitted), max(r[0] for r in submitted)
                    assert high - low + 1 == args.jobs, "unexpected writer in isolated DB"
                    wait_batch(low, high)
                    observed = time.monotonic() - started
                finally:
                    stop_sampling.set()
                    sampler.join(timeout=15)
                assert not sampler.is_alive() and not sample_errors, sample_errors
                # Capture SQL before issuing verification queries.
                sql_metrics = json.loads(sql("""SELECT coalesce(json_agg(t), '[]') FROM (
                    SELECT left(query,120) AS query, calls,
                      round(mean_exec_time::numeric,3) AS mean_ms,
                      round(total_exec_time::numeric,3) AS total_ms,
                      round(max_exec_time::numeric,3) AS max_ms,
                      round(blk_read_time::numeric,3) AS block_read_ms,
                      round(blk_write_time::numeric,3) AS block_write_ms
                    FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat%'
                    ORDER BY total_exec_time DESC LIMIT 15) t"""))
                batch = json.loads(sql(f"""SELECT json_build_object(
                    'jobs', count(*), 'attempt_max', max(attempt),
                    'queue_p95_ms', percentile_cont(.95) WITHIN GROUP (ORDER BY attempted_at-inserted_at),
                    'claim_to_completion_p95_ms', percentile_cont(.95) WITHIN GROUP (ORDER BY completed_at-attempted_at),
                    'end_to_end_p95_ms', percentile_cont(.95) WITHIN GROUP (ORDER BY completed_at-inserted_at),
                    'batch_span_ms', max(completed_at)-min(inserted_at))
                    FROM quasar_jobs WHERE id BETWEEN {low} AND {high}"""))
                distribution = json.loads(sql(f"""SELECT json_object_agg(processed_by,n)
                    FROM (SELECT processed_by, count(*) n FROM example_reports
                    WHERE job_id BETWEEN {low} AND {high} GROUP BY processed_by) t"""))
                valid = int(sql(f"""SELECT count(*) FROM example_reports
                    WHERE job_id BETWEEN {low} AND {high} AND total=500000500000"""))
                assert valid == args.jobs, "missing or incorrect business results"
                time.sleep(1)  # Allow tracer snapshots to catch up; excluded from timing.
                trace_metrics = merge_trace(before, [i.latest for i in instances]) if trace else {}
                runtime_metrics = merge_runtime(
                    runtime_before, [i.runtime_metrics for i in instances])
                if trace:
                    assert trace_metrics["worker_execute"]["count"] == args.jobs, trace_metrics
                latencies = sorted(r[1] for r in submitted)
                result = dict(case=name, repetition=repetition + 1, instances=4,
                              workers_per_instance=workers, connections_per_instance=connections,
                              enqueue_p95_ms=round(latencies[math.ceil(len(latencies)*.95)-1],3),
                              submission_seconds=round(submission_time,3),
                              aggregate_observed_seconds=round(observed,3),
                              batch=batch, processed_by=distribution,
                              trace=trace_metrics, runtime=runtime_metrics,
                              sql=sql_metrics,
                              pg_wait_samples=len(sample_count),
                              pg_backend_state_observations=dict(waits))
                results.append(result)
                print("RESULT " + json.dumps(result), flush=True)
        print("PROFILE_RESULTS " + json.dumps(results), flush=True)
    finally:
        for instance in instances:
            instance.stop()
        if created:
            command("docker", "stop", container)
            print("Removed disposable PostgreSQL and all profiling instances.", flush=True)


if __name__ == "__main__":
    main()
