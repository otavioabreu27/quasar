"""Small deterministic tests of the load generator; no cluster required."""
import contextlib
import io
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchmark_v2 import Benchmark
from analyze_release_benchmark import bracket, delta
from benchmark_parallel import merge


def args(**changes):
    result = dict(url="http://unused", submit_workers=2, poll_workers=2,
                  connect_timeout=1, read_timeout=1, parsed_stages=[(0.2, 100)],
                  max_schedule_lag_ms=50, work_size=100, batch_size=1,
                  initial_poll_delay=0, poll_interval=0.001, sample_interval=1,
                  drain_timeout=2, scenario="unit", duration=0.2, rate=100)
    result.update(changes)
    return SimpleNamespace(**result)


class FakeBenchmark(Benchmark):
    def __init__(self, config, delay=0, wrong_result=False):
        super().__init__(config)
        self.delay = delay
        self.next_id = 0
        self.fake_lock = threading.Lock()
        self.wrong_result = wrong_result

    def request_json(self, method, path):
        if path == "/health/ready":
            return 200, {}
        if method == "POST":
            time.sleep(self.delay)
            with self.fake_lock:
                ids = [str(self.next_id + i + 1) for i in range(self.args.batch_size)]
                self.next_id += len(ids)
            return 202, {"job_ids": ids}
        return 200, dict(job_id=path.rsplit('/', 1)[1], status="completed", attempt=1,
                         total=0 if self.wrong_result else 5050, processed_by="fake",
                         inserted_at=100, completed_at=101)


def run(benchmark):
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        code = benchmark.run()
    return code, json.loads(output.getvalue().splitlines()[-1])


class GeneratorTest(unittest.TestCase):
    def test_parallel_merge_counts_but_does_not_average_percentiles(self):
        _, first = run(FakeBenchmark(args(parsed_stages=[(0.1, 20)])))
        _, second = run(FakeBenchmark(args(parsed_stages=[(0.1, 20)])))
        second['accepted_ids'] = [str(int(value) + 100) for value in second['accepted_ids']]
        combined = merge([first, second], 'parallel')
        self.assertEqual(combined['completed'], 4)
        self.assertEqual(combined['configured_stages'][0]['rate_requests_s'], 40)
        self.assertEqual(combined['phases'][0]['scheduled'], 4)
        self.assertNotIn('p95_ms', combined['latency']['enqueue'])
        self.assertEqual(len(combined['phases'][0]['generators']), 2)
        self.assertIsNone(combined['max_client_pending'])

    def test_parallel_merge_rejects_overlapping_job_ids(self):
        _, first = run(FakeBenchmark(args(parsed_stages=[(0.1, 20)])))
        with self.assertRaises(AssertionError):
            merge([first, first], 'parallel')

    def test_resource_analysis_requires_both_boundaries(self):
        self.assertIsNone(bracket([dict(timestamp_epoch_ms=15)], 10, 20))
        pair = bracket([dict(timestamp_epoch_ms=5), dict(timestamp_epoch_ms=25)], 10, 20)
        self.assertEqual([s['timestamp_epoch_ms'] for s in pair], [5, 25])
        self.assertIsNone(delta(dict(x=10), dict(x=2), 'x'))
        self.assertIsNone(delta({}, dict(x=2), 'x'))

    def test_already_late_request_is_not_sent(self):
        benchmark = FakeBenchmark(args())
        benchmark.submit(0, time.monotonic() - 1, 0)
        self.assertEqual(benchmark.attempted_requests, 0)
        self.assertEqual(benchmark.dropped['executor_late'], 1)

    def test_overloaded_producer_drops_instead_of_replaying_backlog(self):
        code, result = run(FakeBenchmark(args(submit_workers=1), delay=0.08))
        self.assertEqual(code, 2)
        self.assertGreater(sum(result['generator_dropped'].values()), 0)
        self.assertEqual(result['attempted_requests'] + sum(result['generator_dropped'].values()), 20)
        self.assertLess(result['submission_elapsed_s'], 0.5)

    def test_batch_counts_jobs_and_phases_separately(self):
        code, result = run(FakeBenchmark(args(batch_size=4, parsed_stages=[(0.1, 20), (0.1, 20)])))
        self.assertEqual(code, 0)
        self.assertEqual(result['attempted_requests'], 4)
        self.assertEqual(result['completed'], 16)
        self.assertEqual([p['completed'] for p in result['phases']], [8, 8])
        self.assertEqual(len(set(result['accepted_ids'])), 16)

    def test_incorrect_business_result_fails_run(self):
        code, result = run(FakeBenchmark(args(parsed_stages=[(0.1, 10)]), wrong_result=True))
        self.assertEqual(code, 2)
        self.assertEqual(result['invalid_results'], 1)


if __name__ == '__main__':
    unittest.main()
