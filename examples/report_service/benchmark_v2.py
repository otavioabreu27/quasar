#!/usr/bin/env python3
"""Open-loop report-service benchmark with concurrent completion tracking.

The generator emits JSON Lines. Samples have ``type=sample`` and the final
machine-readable result has ``type=summary``. HTTP retries are deliberately
disabled: overload and transport failures are measurements, not noise to hide.
"""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import heapq
import json
import math
import queue
import threading
import time

try:
    import urllib3
except ImportError as exc:  # pragma: no cover - explicit runtime guard
    raise SystemExit("benchmark_v2.py requires urllib3") from exc


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)], 3)


def statistics(values):
    if not values:
        return {"count": 0}
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return {
        "count": len(values),
        "min_ms": round(min(values), 3),
        "p50_ms": percentile(values, 0.50),
        "p90_ms": percentile(values, 0.90),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "max_ms": round(max(values), 3),
        "mean_ms": round(mean, 3),
        "stddev_ms": round(math.sqrt(variance), 3),
    }


@dataclass
class TrackedJob:
    job_id: str
    accepted_monotonic: float
    polls: int = 0


class Benchmark:
    def __init__(self, args):
        self.args = args
        self.base_url = args.url.rstrip("/")
        self.http = urllib3.PoolManager(
            num_pools=4,
            maxsize=args.submit_workers + args.poll_workers + 16,
            block=True,
            retries=False,
            timeout=urllib3.Timeout(connect=args.connect_timeout, read=args.read_timeout),
        )
        self.lock = threading.Lock()
        self.start = 0.0
        self.stop_submitting = 0.0
        self.accepted = 0
        self.completed = 0
        self.terminal_failures = 0
        self.submit_errors = Counter()
        self.poll_errors = Counter()
        self.statuses = Counter()
        self.attempts = Counter()
        self.processed_by = Counter()
        self.ids = set()
        self.enqueue_ms = []
        self.e2e_server_ms = []
        self.e2e_observed_ms = []
        self.schedule_lag_ms = []
        self.poll_ms = []
        self.total_polls = 0
        self.max_pending = 0
        self.ready = queue.Queue()
        self.delayed = []
        self.delayed_cv = threading.Condition()
        self.pending = 0
        self.submission_finished = threading.Event()
        self.stop_polling = threading.Event()
        self.drain_timed_out = False

    def request_json(self, method, path):
        response = self.http.request(method, self.base_url + path)
        try:
            payload = json.loads(response.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = {"invalid_json": response.data[:200].decode("utf-8", "replace")}
        return response.status, payload

    def verify_ready(self):
        status, body = self.request_json("GET", "/health/ready")
        if status != 200:
            raise SystemExit(f"service is not ready: HTTP {status}: {body}")

    def submit(self, sequence, scheduled_at):
        actual_start = time.monotonic()
        before = actual_start
        try:
            status, body = self.request_json("POST", f"/reports/{self.args.work_size}")
            latency_ms = (time.monotonic() - before) * 1000
        except Exception as exc:  # record transport errors without aborting the run
            with self.lock:
                self.submit_errors[type(exc).__name__] += 1
            return

        with self.lock:
            self.enqueue_ms.append(latency_ms)
            self.schedule_lag_ms.append(max(0.0, (actual_start - scheduled_at) * 1000))
            if status != 202 or "job_id" not in body:
                self.submit_errors[f"http_{status}"] += 1
                return
            job_id = str(body["job_id"])
            if job_id in self.ids:
                self.submit_errors["duplicate_job_id"] += 1
                return
            self.ids.add(job_id)
            self.accepted += 1
            self.pending += 1
            self.max_pending = max(self.max_pending, self.pending)
        self.schedule(TrackedJob(job_id, time.monotonic()), time.monotonic() + self.args.initial_poll_delay)

    def schedule(self, job, due):
        with self.delayed_cv:
            heapq.heappush(self.delayed, (due, int(job.job_id), job))
            self.delayed_cv.notify()

    def scheduler(self):
        while not self.stop_polling.is_set():
            with self.delayed_cv:
                while not self.delayed and not self.stop_polling.is_set():
                    self.delayed_cv.wait(0.2)
                if self.stop_polling.is_set():
                    return
                due, _, job = self.delayed[0]
                wait = due - time.monotonic()
                if wait > 0:
                    self.delayed_cv.wait(min(wait, 0.2))
                    continue
                heapq.heappop(self.delayed)
            self.ready.put(job)

    def poll_worker(self):
        terminal = {"completed", "discarded", "cancelled"}
        while not self.stop_polling.is_set():
            try:
                job = self.ready.get(timeout=0.2)
            except queue.Empty:
                continue
            before = time.monotonic()
            try:
                status, body = self.request_json("GET", "/jobs/" + job.job_id)
                latency_ms = (time.monotonic() - before) * 1000
            except Exception as exc:
                with self.lock:
                    self.poll_errors[type(exc).__name__] += 1
                self.schedule(job, time.monotonic() + self.args.poll_interval)
                continue

            job.polls += 1
            with self.lock:
                self.total_polls += 1
                self.poll_ms.append(latency_ms)
            if status != 200:
                with self.lock:
                    self.poll_errors[f"http_{status}"] += 1
                self.schedule(job, time.monotonic() + self.args.poll_interval)
                continue

            state = body.get("status", "unknown")
            if state not in terminal:
                self.schedule(job, time.monotonic() + self.args.poll_interval)
                continue

            now = time.monotonic()
            with self.lock:
                self.statuses[state] += 1
                self.pending -= 1
                if state == "completed":
                    self.completed += 1
                    self.attempts[str(body.get("attempt"))] += 1
                    self.processed_by[str(body.get("processed_by"))] += 1
                    inserted = body.get("inserted_at")
                    completed = body.get("completed_at")
                    if isinstance(inserted, int) and isinstance(completed, int):
                        self.e2e_server_ms.append(float(completed - inserted))
                    self.e2e_observed_ms.append((now - job.accepted_monotonic) * 1000)
                else:
                    self.terminal_failures += 1

    def snapshot(self):
        now = time.monotonic()
        with self.lock:
            return {
                "type": "sample",
                "timestamp_epoch_ms": int(time.time() * 1000),
                "elapsed_s": round(now - self.start, 3),
                "accepted": self.accepted,
                "completed": self.completed,
                "pending": self.pending,
                "submit_errors": sum(self.submit_errors.values()),
                "poll_errors": sum(self.poll_errors.values()),
                "completion_rate_jobs_s": round(self.completed / max(now - self.start, 0.001), 3),
            }

    def sample_loop(self):
        while not self.stop_polling.wait(self.args.sample_interval):
            print(json.dumps(self.snapshot(), sort_keys=True), flush=True)

    def run(self):
        self.verify_ready()
        started_epoch_ms = int(time.time() * 1000)
        self.start = time.monotonic()
        scheduler = threading.Thread(target=self.scheduler, daemon=True)
        sampler = threading.Thread(target=self.sample_loop, daemon=True)
        scheduler.start()
        sampler.start()
        pollers = [threading.Thread(target=self.poll_worker, daemon=True) for _ in range(self.args.poll_workers)]
        for worker in pollers:
            worker.start()

        stages = self.args.parsed_stages
        schedule = []
        stage_offset = 0.0
        sequence = 0
        for stage_duration, stage_rate in stages:
            stage_jobs = math.floor(stage_duration * stage_rate)
            for stage_sequence in range(stage_jobs):
                schedule.append((sequence, self.start + stage_offset + (stage_sequence / stage_rate)))
                sequence += 1
            stage_offset += stage_duration
        total_scheduled = len(schedule)
        max_submit_backlog = self.args.submit_workers * 4
        submit_slots = threading.Semaphore(max_submit_backlog)

        def release_slot(future):
            submit_slots.release()

        with ThreadPoolExecutor(max_workers=self.args.submit_workers) as executor:
            for sequence, scheduled_at in schedule:
                delay = scheduled_at - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                submit_slots.acquire()
                future = executor.submit(self.submit, sequence, scheduled_at)
                future.add_done_callback(release_slot)

        self.stop_submitting = time.monotonic()
        self.submission_finished.set()
        drain_deadline = self.stop_submitting + self.args.drain_timeout
        while True:
            with self.lock:
                pending = self.pending
            if pending == 0:
                break
            if time.monotonic() >= drain_deadline:
                self.drain_timed_out = True
                break
            time.sleep(0.1)

        finished = time.monotonic()
        self.stop_polling.set()
        with self.delayed_cv:
            self.delayed_cv.notify_all()
        for worker in pollers:
            worker.join(timeout=1)
        scheduler.join(timeout=1)
        sampler.join(timeout=1)

        with self.lock:
            result = {
                "type": "summary",
                "schema_version": 2,
                "started_epoch_ms": started_epoch_ms,
                "finished_epoch_ms": int(time.time() * 1000),
                "scenario": self.args.scenario,
                "url": self.base_url,
                "work_size": self.args.work_size,
                "configured_rate_jobs_s": self.args.rate,
                "configured_duration_s": self.args.duration,
                "configured_stages": [
                    {"duration_s": duration, "rate_jobs_s": rate}
                    for duration, rate in stages
                ],
                "scheduled": total_scheduled,
                "accepted": self.accepted,
                "completed": self.completed,
                "pending_at_end": self.pending,
                "terminal_failures": self.terminal_failures,
                "drain_timed_out": self.drain_timed_out,
                "submission_elapsed_s": round(self.stop_submitting - self.start, 3),
                "total_elapsed_s": round(finished - self.start, 3),
                "achieved_accept_rate_jobs_s": round(self.accepted / max(self.stop_submitting - self.start, 0.001), 3),
                "completion_rate_during_total_jobs_s": round(self.completed / max(finished - self.start, 0.001), 3),
                "max_client_pending": self.max_pending,
                "submit_errors": dict(self.submit_errors),
                "poll_errors": dict(self.poll_errors),
                "terminal_statuses": dict(self.statuses),
                "attempts": dict(self.attempts),
                "processed_by": dict(self.processed_by),
                "http": {
                    "post_requests": len(self.enqueue_ms),
                    "poll_requests": self.total_polls,
                    "total_requests": len(self.enqueue_ms) + self.total_polls,
                },
                "latency": {
                    "enqueue": statistics(self.enqueue_ms),
                    "server_end_to_end": statistics(self.e2e_server_ms),
                    "observed_end_to_end": statistics(self.e2e_observed_ms),
                    "poll": statistics(self.poll_ms),
                    "schedule_lag": statistics(self.schedule_lag_ms),
                },
            }
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0 if not self.drain_timed_out and not self.submit_errors and not self.terminal_failures else 2


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://report-service:8080")
    parser.add_argument("--scenario", default="unnamed")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument(
        "--stages",
        help="Comma-separated DURATION:RATE stages; overrides --duration and --rate",
    )
    parser.add_argument("--work-size", type=int, default=1_000_000)
    parser.add_argument("--submit-workers", type=int, default=64)
    parser.add_argument("--poll-workers", type=int, default=64)
    parser.add_argument("--initial-poll-delay", type=float, default=0.05)
    parser.add_argument("--poll-interval", type=float, default=0.20)
    parser.add_argument("--drain-timeout", type=float, default=300.0)
    parser.add_argument("--sample-interval", type=float, default=5.0)
    parser.add_argument("--connect-timeout", type=float, default=3.0)
    parser.add_argument("--read-timeout", type=float, default=10.0)
    args = parser.parse_args()
    if args.duration <= 0 or args.rate <= 0:
        parser.error("duration and rate must be positive")
    if args.submit_workers <= 0 or args.poll_workers <= 0:
        parser.error("worker counts must be positive")
    if args.stages:
        try:
            args.parsed_stages = [
                tuple(float(value) for value in stage.split(":"))
                for stage in args.stages.split(",")
            ]
        except (TypeError, ValueError):
            parser.error("stages must use DURATION:RATE,DURATION:RATE format")
        if not args.parsed_stages or any(duration <= 0 or rate <= 0 for duration, rate in args.parsed_stages):
            parser.error("every stage duration and rate must be positive")
        args.duration = sum(duration for duration, _ in args.parsed_stages)
        args.rate = round(
            sum(duration * rate for duration, rate in args.parsed_stages) / args.duration,
            3,
        )
    else:
        args.parsed_stages = [(args.duration, args.rate)]
    return args


if __name__ == "__main__":
    raise SystemExit(Benchmark(parse_args()).run())
