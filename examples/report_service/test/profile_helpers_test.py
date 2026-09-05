import unittest

from profile_test import merge_runtime, merge_trace


class TraceAggregationTest(unittest.TestCase):
    def test_uses_deltas_and_merges_histograms(self):
        before = [{"pool_checkout": {
            "count": 10, "total_us": 1000, "histogram": {"100": 10}, "peak": 1,
        }}, {}]
        after = [{"pool_checkout": {
            "count": 15, "total_us": 2000,
            "histogram": {"100": 10, "200": 5}, "peak": 2,
        }}, {"pool_checkout": {
            "count": 5, "total_us": 1500, "histogram": {"300": 5}, "peak": 1,
        }}]
        result = merge_trace(before, after)["pool_checkout"]
        self.assertEqual(result["count"], 10)
        self.assertEqual(result["mean_ms"], .25)
        self.assertEqual(result["p95_upper_ms"], .3)
        self.assertEqual(result["summed_duration_ms"], 2.5)
        self.assertEqual(result["summed_peak_concurrency"], 3)

    def test_no_new_calls_has_no_percentile(self):
        state = {"pool_checkout": {
            "count": 2, "total_us": 200, "histogram": {"100": 2}, "peak": 1,
        }}
        self.assertIsNone(merge_trace([state], [state])["pool_checkout"]["p95_upper_ms"])

    def test_rejects_reset_counters(self):
        before = [{"pool_checkout": {
            "count": 2, "total_us": 200, "histogram": {"100": 2}, "peak": 1,
        }}]
        after = [{"pool_checkout": {
            "count": 1, "total_us": 100, "histogram": {"100": 1}, "peak": 1,
        }}]
        with self.assertRaises(AssertionError):
            merge_trace(before, after)


class RuntimeAggregationTest(unittest.TestCase):
    def test_merges_event_deltas_and_latency_histograms(self):
        before = [{
            "claims": 2, "claim_requested": 4, "claim_returned": 3,
            "empty_claims": 0, "jobs_started": 3, "jobs_completed": 3,
            "lease_renewals": 3, "persistence_failures": 0,
            "claim_failures": 0, "jobs_executing": 0,
            "jobs_executing_peak": 2,
            "claim_duration_ms": {"1": 2},
            "completion_duration_ms": {"2": 3},
        }]
        after = [{
            "claims": 5, "claim_requested": 10, "claim_returned": 7,
            "empty_claims": 1, "jobs_started": 7, "jobs_completed": 7,
            "lease_renewals": 7, "persistence_failures": 0,
            "claim_failures": 0, "jobs_executing": 0,
            "jobs_executing_peak": 4,
            "claim_duration_ms": {"1": 2, "3": 3},
            "completion_duration_ms": {"2": 3, "4": 4},
        }]
        result = merge_runtime(before, after)
        self.assertEqual(result["claims"], 3)
        self.assertEqual(result["claim_returned"], 4)
        self.assertEqual(result["empty_claims"], 1)
        self.assertEqual(result["claim_duration_ms"]["mean_ms"], 3)
        self.assertEqual(result["completion_duration_ms"]["p95_upper_ms"], 4)
