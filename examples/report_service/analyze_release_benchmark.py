#!/usr/bin/env python3
"""Conservative analysis of schema-v3 phased benchmark evidence.

Resource counters use actual bracketing samples, never invented interpolation.
WAL is instance-scoped; database commits include reads and other clients.
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path


def records(path):
    with path.open() as stream:
        return [value for line in stream if line.strip()
                for value in [json.loads(line)] if isinstance(value, dict)]


def bracket(samples, start, end):
    before = [s for s in samples if s['timestamp_epoch_ms'] <= start]
    after = [s for s in samples if s['timestamp_epoch_ms'] >= end]
    if not before or not after:
        return None
    return max(before, key=lambda s: s['timestamp_epoch_ms']), min(after, key=lambda s: s['timestamp_epoch_ms'])


def delta(first, last, key):
    a, b = first.get(key), last.get(key)
    if a is None or b is None or float(b) < float(a):
        return None
    return float(b) - float(a)


def analyze(summary, database, kubernetes):
    start, end = summary['started_epoch_ms'], summary['finished_epoch_ms']
    completed = summary['completed']
    issues = []
    if summary.get('schema_version') != 3:
        issues.append('Historical generator: no reliable dropped-load/per-phase accounting')
    if summary.get('generator_dropped'):
        issues.append('Generator saturated: offered rate was NOT delivered')
    if summary['submit_errors']:
        issues.append('Submit failures can have unknown persistence outcomes; reconcile IDs with database')
    if summary.get('invalid_results') or summary.get('pending_at_end') or summary.get('terminal_failures'):
        issues.append('Correctness/drain gate failed')
    database = [s for s in database if 'postgresql' in s]
    bounds = bracket(database, start, end)
    db_cost = {}
    if not bounds:
        issues.append('Missing PostgreSQL baseline/final samples; no per-job database costs')
    else:
        first, last = bounds
        a, b = first['postgresql'], last['postgresql']
        db_cost['window_epoch_ms'] = [first['timestamp_epoch_ms'], last['timestamp_epoch_ms']]
        for key, reset, label in (
            ('xact_commit', 'database_stats_reset', 'database_transactions_including_reads_per_job'),
            ('wal_bytes', 'wal_stats_reset', 'instance_wal_bytes_per_job'),
            ('wal_sync', 'wal_stats_reset', 'instance_wal_syncs_per_job'),
        ):
            value = delta(a, b, key) if a.get(reset) == b.get(reset) else None
            db_cost[label] = value / completed if value is not None and completed else None
        old_queries = {q['queryid']: q for q in a.get('statements', [])}
        query_cost = defaultdict(lambda: dict(calls=0, rows=0, total_exec_time_ms=0, wal_bytes=0))
        for query in b.get('statements', []):
            if a.get('statements_stats_reset') != b.get('statements_stats_reset'):
                continue
            if query['queryid'] not in old_queries:
                continue  # A missing baseline is not evidence of a zero counter.
            old = old_queries[query['queryid']]
            diffs = {out: delta(old, query, key) for key, out in (
                ('calls', 'calls'), ('rows', 'rows'), ('total_exec_time', 'total_exec_time_ms'), ('wal_bytes', 'wal_bytes'))}
            if all(value is not None for value in diffs.values()):
                for key, value in diffs.items():
                    query_cost[query['operation']][key] += value
        db_cost['query_deltas_with_baseline'] = dict(query_cost)
        db_cost['attribution'] = 'Includes other database/instance activity and bracketing idle time; not isolated job cost'

    pods = defaultdict(list)
    for sample in kubernetes:
        for name, item in sample.get('kubernetes', {}).get('runtime', {}).items():
            metrics = item.get('metrics', {})
            if 'started_epoch_ms' in metrics:
                pods[(name, item['uid'], metrics['started_epoch_ms'])].append(metrics)
    runtime = defaultdict(float)
    covered, incomplete = [], []
    for identity, samples in pods.items():
        bounds = bracket(samples, start, end)
        if not bounds:
            incomplete.append(identity[0])
            continue
        first, last = bounds
        covered.append(identity[0])
        for key in ('claims', 'claim_returned', 'empty_claims', 'lease_renewals',
                    'jobs_completed', 'persistence_failures', 'reaped_jobs', 'pool_checkout_count',
                    'pool_checkout_sum_ms', 'pool_checkout_failures'):
            value = delta(first, last, key)
            if value is not None:
                runtime[key] += value
        for key in ('usage_usec', 'throttled_usec', 'nr_throttled'):
            value = delta(first.get('cgroup_cpu', {}), last.get('cgroup_cpu', {}), key)
            if value is not None:
                runtime['cgroup_' + key] += value
    if incomplete:
        issues.append('Pods created/deleted during load lack full-run boundaries; CPU/runtime totals are partial')
    if not covered:
        issues.append('No pod has full-run runtime telemetry; application CPU cost is unavailable')
    cpu_ms = runtime.get('cgroup_usage_usec')
    return dict(scenario=summary['scenario'], scheduled_requests=summary['scheduled'],
        attempted_requests=summary.get('attempted_requests'), accepted=summary['accepted'], completed=completed,
        submit_errors=summary['submit_errors'], poll_errors=summary['poll_errors'],
        generator_dropped=summary.get('generator_dropped', {}), invalid_results=summary.get('invalid_results'),
        phases=summary.get('phases'), latency=summary['latency'], limitations=issues,
        database_costs=db_cost, runtime_deltas=dict(runtime), full_window_pods=covered,
        partial_window_pods=sorted(set(incomplete)),
        app_cpu_ms_per_job=(cpu_ms / 1000 / completed if cpu_ms is not None and completed and not incomplete else None),
        notes=['No memory-per-request estimate: resident RAM is shared, not an allocation cost.',
               'CPU uses container cgroup counters only for complete, non-reset pod lifetimes.',
               'Server latency ends at completion timestamp before commit; observed latency includes polling.',
               'Use fixed replicas to compare resource cost; HPA run tests elasticity separately.'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('result_dir', type=Path)
    args = parser.parse_args()
    summary = next(s for s in records(args.result_dir / 'load.jsonl') if s.get('type') == 'summary')
    result = analyze(summary, records(args.result_dir / 'postgresql-observer.jsonl'),
                     records(args.result_dir / 'kubernetes-observer.jsonl'))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
