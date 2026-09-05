#!/usr/bin/env python3
"""HTTP scheduling smoke/load test with concurrent polling and rich telemetry."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import sys
import threading
import time
from urllib.request import Request, urlopen

try:
    import urllib3
    _HTTP_POOL = urllib3.PoolManager(
        maxsize=128,
        retries=urllib3.Retry(total=3, backoff_factor=0.05, status_forcelist=[502, 503, 504]),
        timeout=urllib3.Timeout(connect=5.0, read=15.0),
    )
except ImportError:
    _HTTP_POOL = None


def call(url, method="GET"):
    """Execute HTTP call with connection pool when available, fallback to urllib."""
    if _HTTP_POOL is not None:
        resp = _HTTP_POOL.request(method, url)
        return resp.status, json.loads(resp.data.decode("utf-8"))
    with urlopen(Request(url, method=method), timeout=15) as response:
        return response.status, json.load(response)


def calculate_percentiles(values):
    """Compute rich statistical percentiles for latency measurements in milliseconds."""
    if not values:
        return {}
    s = sorted(values)
    n = len(s)
    mean = sum(s) / n
    variance = sum((x - mean) ** 2 for x in s) / n if n > 1 else 0.0
    return {
        "min_ms": round(s[0], 2),
        "p50_ms": round(s[math.ceil(n * 0.50) - 1], 2),
        "p90_ms": round(s[math.ceil(n * 0.90) - 1], 2),
        "p95_ms": round(s[math.ceil(n * 0.95) - 1], 2),
        "p99_ms": round(s[math.ceil(n * 0.99) - 1], 2),
        "max_ms": round(s[-1], 2),
        "mean_ms": round(mean, 2),
        "std_dev_ms": round(math.sqrt(variance), 2),
    }


class ResourceSampler:
    """Samples BEAM and system resources continuously during load execution."""
    def __init__(self, interval=0.5):
        self.interval = interval
        self.stopped = threading.Event()
        self.samples = []
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopped.set()
        self.thread.join(timeout=2.0)

    def _get_beam_pids(self):
        import glob
        pids = []
        for p in glob.glob("/proc/[0-9]*/cmdline"):
            try:
                with open(p, "rb") as f:
                    content = f.read()
                    if b"report_service" in content and b"beam.smp" in content:
                        pid = int(p.split("/")[2])
                        pids.append(pid)
            except Exception:
                pass
        return pids

    def _run(self):
        last_times = {}
        last_clock = time.monotonic()
        while not self.stopped.is_set():
            pids = self._get_beam_pids()
            now = time.monotonic()
            dt = now - last_clock
            last_clock = now
            total_rss_kb = 0
            per_node_rss = {}
            total_cpu_pct = 0.0

            for pid in pids:
                try:
                    with open(f"/proc/{pid}/status") as f:
                        for line in f:
                            if line.startswith("VmRSS:"):
                                rss_kb = int(line.split()[1])
                                total_rss_kb += rss_kb
                                per_node_rss[str(pid)] = round(rss_kb / 1024, 2)
                                break
                    with open(f"/proc/{pid}/stat") as f:
                        fields = f.read().split()
                        utime = int(fields[13])
                        stime = int(fields[14])
                        total_time = utime + stime
                        if pid in last_times and dt > 0:
                            cpu_usage = ((total_time - last_times[pid]) / 100.0) / dt * 100.0
                            total_cpu_pct += cpu_usage
                        last_times[pid] = total_time
                except Exception:
                    pass

            if total_rss_kb > 0:
                self.samples.append({
                    "cluster_ram_mb": round(total_rss_kb / 1024, 2),
                    "cluster_cpu_percent": round(total_cpu_pct, 1),
                    "node_count": len(pids),
                    "per_node_ram_mb": per_node_rss
                })
            self.stopped.wait(self.interval)

    def stats(self):
        if not self.samples:
            return {}
        ram_vals = [s["cluster_ram_mb"] for s in self.samples]
        cpu_vals = [s["cluster_cpu_percent"] for s in self.samples if s["cluster_cpu_percent"] > 0]
        nodes = max((s["node_count"] for s in self.samples), default=0)
        return {
            "monitored_beam_nodes": nodes,
            "cluster_ram_mb": {
                "min_mb": round(min(ram_vals), 2) if ram_vals else 0.0,
                "peak_mb": round(max(ram_vals), 2) if ram_vals else 0.0,
                "avg_mb": round(sum(ram_vals) / len(ram_vals), 2) if ram_vals else 0.0,
                "per_node_avg_mb": round((sum(ram_vals) / len(ram_vals)) / max(nodes, 1), 2) if ram_vals else 0.0,
            },
            "cluster_cpu_percent": {
                "peak_pct": round(max(cpu_vals), 1) if cpu_vals else 0.0,
                "avg_pct": round(sum(cpu_vals) / len(cpu_vals), 1) if cpu_vals else 0.0,
            }
        }


def run_single_benchmark(title, jobs, clients, urls, target_tps=0.0, chaos_kill_port=None, timeout=120, poll_clients=0):
    """Executes a single benchmark run with live progress and rich telemetry."""
    poll_workers = poll_clients if poll_clients > 0 else min(max(clients * 2, 16), 64)
    bases = list(dict.fromkeys(url.rstrip("/") for url in urls))

    # Verify all targets are ready
    for base in bases:
        assert call(base + "/health/ready")[0] == 200

    sampler = ResourceSampler(interval=0.25)
    sampler.start()

    started = time.monotonic()
    submitted = []
    submission_lock = threading.Lock()
    submitted_count = 0
    target_interval = (1.0 / target_tps) if target_tps > 0 else 0.0

    chaos_killed = False
    chaos_pid = None

    if chaos_kill_port:
        try:
            import subprocess
            cmd = f"ss -tulpn | grep {chaos_kill_port}"
            out = subprocess.check_output(cmd, shell=True, text=True)
            chaos_pid = int(out.split("pid=")[1].split(",")[0])
        except Exception as e:
            sys.stderr.write(f"[!] Chaos prep warning: could not find PID on port {chaos_kill_port}: {e}\n")

    def submit(index):
        nonlocal submitted_count, chaos_killed
        if target_interval > 0:
            target_time = started + (index * target_interval)
            now = time.monotonic()
            if target_time > now:
                time.sleep(target_time - now)

        # Trigger chaos kill if configured
        if chaos_kill_port and chaos_pid and not chaos_killed and index >= 50:
            chaos_killed = True
            try:
                import os, signal
                os.kill(chaos_pid, signal.SIGKILL)
                sys.stderr.write(f"\n[💥 CHAOS] Killed instance on port {chaos_kill_port} (PID {chaos_pid}) with SIGKILL!\n")
                sys.stderr.flush()
            except Exception as e:
                sys.stderr.write(f"\n[!] Chaos kill failed: {e}\n")

        base = bases[index % len(bases)]
        before = time.monotonic()
        status, body = call(base + "/reports/1000000", "POST")
        latency = time.monotonic() - before
        assert status == 202, (status, body)
        job_id = body["job_id"]
        submit_time = time.monotonic()
        with submission_lock:
            submitted_count += 1
            submitted.append((job_id, latency, base, submit_time))
        return job_id, latency, base, submit_time

    sys.stderr.write(f"\n>>> Running: {title} ({jobs} jobs, {clients} threads" + (f", {target_tps} TPS" if target_tps > 0 else ", burst") + ")\n")
    sys.stderr.flush()

    with ThreadPoolExecutor(max_workers=clients) as pool:
        list(pool.map(submit, range(jobs)))

    submitted_at = time.monotonic()
    submission_duration = submitted_at - started
    job_submit_times = {job_id: submit_time for job_id, _, _, submit_time in submitted}
    pending = {job_id for job_id, _, _, _ in submitted}
    assert len(pending) == jobs, "duplicate job IDs returned by API"

    distribution = Counter()
    attempts = Counter()
    posts_by_api = Counter(base for _, _, base, _ in submitted)
    queries_by_api = Counter()
    e2e_latencies = []
    poll_latencies = []
    query_counter = 0
    query_lock = threading.Lock()

    deadline = started + timeout
    last_report_time = time.monotonic()

    # If chaos killed a node, exclude it from polling queries
    active_poll_bases = [b for b in bases if not (chaos_kill_port and f":{chaos_kill_port}" in b)]
    if not active_poll_bases:
        active_poll_bases = bases

    def check_job(job_id):
        nonlocal query_counter
        with query_lock:
            base = active_poll_bases[query_counter % len(active_poll_bases)]
            query_counter += 1
            queries_by_api[base] += 1

        t0 = time.monotonic()
        status, body = call(base + "/jobs/" + job_id)
        t_poll = (time.monotonic() - t0) * 1000

        assert status == 200, (status, body)
        assert body["status"] not in ("discarded", "cancelled"), body
        return job_id, status, body, t_poll

    while pending:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{len(pending)} jobs still pending after {timeout}s timeout")

        now = time.monotonic()
        if now - last_report_time >= 0.5:
            done = jobs - len(pending)
            elapsed_now = now - started
            current_tps = done / elapsed_now if elapsed_now > 0 else 0
            sys.stderr.write(f"\r[-] Progress: {done}/{jobs} completed ({(done/jobs)*100:.1f}%) | Pending: {len(pending)} | Elapsed: {elapsed_now:.1f}s | Speed: {current_tps:.1f} jobs/s")
            sys.stderr.flush()
            last_report_time = now

        batch = list(pending)[:min(len(pending), poll_workers * 4)]
        with ThreadPoolExecutor(max_workers=min(len(batch), poll_workers)) as poll_pool:
            futures = [poll_pool.submit(check_job, jid) for jid in batch]
            for fut in as_completed(futures):
                job_id, status, body, t_poll = fut.result()
                poll_latencies.append(t_poll)
                if body["status"] == "completed":
                    assert body["total"] == 500000500000, body
                    assert body["processed_by"], body
                    distribution[body["processed_by"]] += 1
                    attempts[body["attempt"]] += 1
                    if job_id in pending:
                        pending.remove(job_id)
                    e2e = (time.monotonic() - job_submit_times[job_id]) * 1000
                    e2e_latencies.append(e2e)

        if pending and len(batch) == len(pending):
            time.sleep(0.05)

    sampler.stop()
    resource_stats = sampler.stats()
    elapsed = time.monotonic() - started

    sys.stderr.write(f"\r[✔] Finished in {elapsed:.2f}s ({jobs/elapsed:.1f} jobs/s)!\n")
    sys.stderr.flush()

    enqueue_latencies_ms = [latency * 1000 for _, latency, _, _ in submitted]
    enqueue_stats = calculate_percentiles(enqueue_latencies_ms)
    e2e_stats = calculate_percentiles(e2e_latencies)
    poll_stats = calculate_percentiles(poll_latencies)

    res = {
        "title": title,
        "jobs_completed": jobs,
        "submitted_to": bases[0] if len(bases) == 1 else bases,
        "posts_by_api": dict(posts_by_api),
        "status_queries_by_api": dict(queries_by_api),
        "processed_by": dict(distribution),
        "attempts": dict(attempts),
        "submission_seconds": round(submitted_at - started, 3),
        "observed_completion_seconds": round(elapsed, 3),
        "observed_jobs_per_second": round(jobs / elapsed, 2),
        "enqueue_p95_ms": enqueue_stats.get("p95_ms", 0.0),
        "telemetry": {
            "enqueue_latency": enqueue_stats,
            "end_to_end_job_latency": e2e_stats,
            "poll_query_latency": poll_stats,
            "total_http_requests": jobs + sum(queries_by_api.values()),
        },
        "resource_utilization": resource_stats
    }

    # Print formatted Round Card
    print_round_card(res)

    # If chaos killed a node, restart it for subsequent rounds
    if chaos_kill_port:
        restart_node(chaos_kill_port)

    return res


def restart_node(port):
    """Restarts a killed instance in the background and waits for health."""
    import subprocess, os
    sys.stderr.write(f"[*] Restoring node on port {port}...\n")
    sys.stderr.flush()
    db_url = os.environ.get("DATABASE_URL", "postgresql://reports:reports_local_only@127.0.0.1:15432/reports")
    cmd = ["mise", "exec", "rebar@3.27.0", "--", "gleam", "run"]
    env = dict(os.environ, DATABASE_URL=db_url, INSTANCE_ID=f"reports-{port}", PORT=str(port))
    subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if call(f"http://127.0.0.1:{port}/health/ready")[0] == 200:
                sys.stderr.write(f"[✔] Node on port {port} restored and healthy!\n")
                sys.stderr.flush()
                return True
        except Exception:
            pass
        time.sleep(0.2)
    sys.stderr.write(f"[!] Warning: Node on port {port} failed to restore within timeout.\n")
    return False


def print_round_card(r):
    """Prints a beautiful summary card for the finished round."""
    t = r["telemetry"]["enqueue_latency"]
    res = r["resource_utilization"]
    ram_str = f"{res['cluster_ram_mb']['avg_mb']} MB (avg: {res['cluster_ram_mb']['per_node_avg_mb']} MB/node)" if res else "N/A"
    attempts_str = ", ".join(f"Att {k}: {v}" for k, v in sorted(r["attempts"].items()))

    card = f"""
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ 📊 {r['title']:<83} │
├────────────────────────────────────────────────────────────────────────────────────────┤
│  Throughput:    {r['observed_jobs_per_second']:>8.1f} jobs/s     │  Total Time:     {r['observed_completion_seconds']:>8.2f} s              │
│  Enqueue Mean:  {t.get('mean_ms', 0):>8.2f} ms         │  Enqueue P95:    {t.get('p95_ms', 0):>8.2f} ms              │
│  Enqueue P99:   {t.get('p99_ms', 0):>8.2f} ms         │  HTTP Requests:  {r['telemetry']['total_http_requests']:>8d}                 │
│  Attempts:      {attempts_str:<22} │  Cluster RAM:    {ram_str:<26} │
└────────────────────────────────────────────────────────────────────────────────────────┘
"""
    print(card)


def print_consolidated_summary(results):
    """Prints a full markdown table consolidating all benchmark rounds."""
    header = """
==========================================================================================
🏆 CONSOLIDATED BENCHMARK SUMMARY TABLE
==========================================================================================

| Scenario | Jobs | Throughput | Enqueue Mean | Enqueue P95 | Enqueue P99 | Cluster RAM | Attempts / Reliability |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |"""
    rows = []
    for r in results:
        t = r["telemetry"]["enqueue_latency"]
        res = r["resource_utilization"]
        ram_str = f"{res['cluster_ram_mb']['avg_mb']} MB" if res else "N/A"
        attempts_str = f"100% (1st att)" if list(r["attempts"].keys()) == ["1"] or list(r["attempts"].keys()) == [1] else f"{r['attempts'].get('1', 0)} att1 / {r['attempts'].get('2', 0)} att2"
        rows.append(f"| **{r['title']}** | {r['jobs_completed']:,} | **{r['observed_jobs_per_second']:.1f} jobs/s** | {t.get('mean_ms', 0):.2f} ms | {t.get('p95_ms', 0):.2f} ms | {t.get('p99_ms', 0):.2f} ms | {ram_str} | {attempts_str} |")

    print(header + "\n" + "\n".join(rows) + "\n\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suite", choices=["all", "fast", "single"], default="single",
        help="Benchmark suite mode: 'all' (runs 5 comprehensive rounds), 'fast' (quick sanity check), 'single' (custom run)"
    )
    parser.add_argument(
        "--url", action="append",
        help="API URL; repeat to distribute requests round-robin across instances",
    )
    parser.add_argument("--jobs", type=int, default=200, help="Total jobs to submit (single mode)")
    parser.add_argument("--clients", type=int, default=8, help="Concurrent HTTP worker threads")
    parser.add_argument("--timeout", type=int, default=120, help="Max execution timeout in seconds")
    parser.add_argument("--target-tps", type=float, default=0.0,
                        help="Target submissions/sec rate limit (0 = max speed burst)")
    parser.add_argument("--poll-clients", type=int, default=0,
                        help="Concurrent polling workers (0 = auto-scale with clients)")
    args = parser.parse_args()

    default_urls = [
        "http://127.0.0.1:8080",
        "http://127.0.0.1:8081",
        "http://127.0.0.1:18083",
        "http://127.0.0.1:18084"
    ]
    urls = args.url or default_urls

    if args.suite == "single":
        res = run_single_benchmark(
            title="Custom Benchmark Run",
            jobs=args.jobs,
            clients=args.clients,
            urls=urls,
            target_tps=args.target_tps,
            timeout=args.timeout,
            poll_clients=args.poll_clients
        )
        print(json.dumps(res, indent=2))
        return

    # Suite runner
    suite_definitions = []
    if args.suite == "fast":
        suite_definitions = [
            ("1. Steady-State", 1000, 16, urls, 300.0, None, 60),
            ("2. Burst Peak", 2000, 32, urls, 0.0, None, 60),
            ("3. Single Ingress", 1000, 16, ["http://127.0.0.1:8080"], 0.0, None, 60),
        ]
    elif args.suite == "all":
        suite_definitions = [
            ("1. Steady-State", 3000, 16, urls, 300.0, None, 90),
            ("2. Peak Burst", 5000, 32, urls, 0.0, None, 90),
            ("3. Single Ingress", 3000, 24, ["http://127.0.0.1:8080"], 0.0, None, 90),
            ("4. High-Volume Sustained", 10000, 32, urls, 400.0, None, 120),
            ("5. Chaos Crash & Recovery", 1000, 16, [u for u in urls if ":18083" not in u], 0.0, 18083, 90),
        ]

    results = []
    for title, jobs, clients, round_urls, tps, chaos_port, t_out in suite_definitions:
        res = run_single_benchmark(
            title=title,
            jobs=jobs,
            clients=clients,
            urls=round_urls,
            target_tps=tps,
            chaos_kill_port=chaos_port,
            timeout=t_out,
            poll_clients=args.poll_clients
        )
        results.append(res)
        time.sleep(1.0)  # brief cooldown between rounds

    print_consolidated_summary(results)


if __name__ == "__main__":
    main()
