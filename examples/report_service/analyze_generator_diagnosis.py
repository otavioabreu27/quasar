#!/usr/bin/env python3
"""Per-phase server histograms and per-generator (not pooled) HTTP percentiles."""
import argparse
from collections import Counter, defaultdict
import json
import math
from pathlib import Path

from analyze_release_benchmark import bracket, records


def histogram(totals, prefix):
    count = totals.get(prefix + '_count', 0)
    if not count:
        return None
    cumulative, upper = 0, None
    for bound in ('1', '5', '20', '100', '1000', 'inf'):
        cumulative += totals.get(prefix + '_bucket_' + bound, 0)
        if cumulative >= math.ceil(count * .95):
            upper = '>1000' if bound == 'inf' else int(bound)
            break
    return dict(count=count, mean_ms=totals[prefix + '_sum_ms'] / count,
                p95_bucket_upper_ms=upper, p95_bound_available=cumulative >= math.ceil(count * .95))


def analyze(directory):
    load = records(directory / 'load.jsonl')
    summary = next(value for value in load if value.get('type') == 'summary')
    samples = records(directory / 'kubernetes-observer.jsonl')
    pods = defaultdict(list)
    for sample in samples:
        for name, value in sample.get('kubernetes', {}).get('runtime', {}).items():
            metrics = value.get('metrics', {})
            if 'started_epoch_ms' in metrics:
                pods[(name, value['uid'], metrics['started_epoch_ms'])].append(metrics)
    start = summary['started_epoch_ms']
    output = []
    for phase in summary['phases']:
        end = start + phase['duration_s'] * 1000
        totals, missing = Counter(), []
        for identity, values in pods.items():
            bounds = bracket(values, start, end)
            if bounds is None:
                missing.append(identity[0])
                continue
            first, last = bounds
            for key, value in last.items():
                if isinstance(value, (int, float)) and key not in ('timestamp_epoch_ms', 'started_epoch_ms'):
                    previous = first.get(key, 0)
                    if isinstance(previous, (int, float)) and value >= previous:
                        totals[key] += value - previous
        timing = {key: histogram(totals, key) for key in (
            'http_post_handler', 'http_get_handler', 'http_enqueue', 'http_status_query', 'pool_checkout')}
        output.append(dict(rate_requests_s=phase['rate_requests_s'], accepted=phase['accepted'],
            dropped=phase['dropped'], per_generator=[dict(enqueue_p95_ms=p['enqueue_ms']['p95_ms'],
                enqueue_mean_ms=p['enqueue_ms']['mean_ms'], server_job_p95_ms=p['server_e2e_ms']['p95_ms'],
                observed_p95_ms=p['e2e_ms']['p95_ms']) for p in phase.get('generators', [phase])],
            timed_segments=timing, full_window_pods=len(pods)-len(missing), missing_pods=missing,
            claims=totals['claims'], empty_claims=totals['empty_claims'],
            empty_fraction=totals['empty_claims']/totals['claims'] if totals['claims'] else None,
            window_ms=[start,end]))
        start = end
    return dict(scenario=summary['scenario'], phases=output, notes=[
        'Server histogram deltas bracket each phase independently per pod; windows include sampling slack.',
        'Pool checkout is aggregated across API, worker and control pools and includes acquisition overhead.',
        'Server handler stops at response construction; it excludes socket send, network and client parsing.',
        'Exact HTTP percentiles belong to each generator; server percentiles are bucket upper bounds.',
        'Missing initial histogram keys mean no observations yet in an initialized diagnostic process.',
        'Timing phases with missing pod boundaries are incomplete; do not treat them as full service totals.'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    print(json.dumps(analyze(parser.parse_args().directory), indent=2))
