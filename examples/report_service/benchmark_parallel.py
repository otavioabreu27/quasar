#!/usr/bin/env python3
"""Independent Python generators sharing one pod's fixed CPU/memory budget.

Split offered load, not duration. Preserve each generator's exact percentiles;
never average percentiles or call the sum of independent backlog peaks a peak.
"""
import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

from benchmark_v2 import parse_args


def merge(summaries, scenario):
    first = summaries[0]
    assert all(s['schema_version'] == 3 and s['batch_size'] == first['batch_size']
               and s['configured_stages'] == first['configured_stages'] for s in summaries)
    result = dict(first)
    result.update(scenario=scenario, generator_count=len(summaries),
                  started_epoch_ms=min(s['started_epoch_ms'] for s in summaries),
                  finished_epoch_ms=max(s['finished_epoch_ms'] for s in summaries))
    result['generator_start_spread_ms'] = max(s['started_epoch_ms'] for s in summaries) - result['started_epoch_ms']
    for key in ('scheduled', 'scheduled_jobs', 'attempted_requests', 'accepted', 'completed',
                'pending_at_end', 'terminal_failures', 'invalid_results'):
        result[key] = sum(s[key] for s in summaries)
    for key in ('generator_dropped', 'submit_errors', 'poll_errors', 'attempts', 'processed_by',
                'http', 'terminal_statuses'):
        total = Counter()
        for summary in summaries:
            total.update(summary[key])
        result[key] = dict(total)
    ids = [job for s in summaries for job in s['accepted_ids']]
    assert len(ids) == len(set(ids)), 'Duplicate job IDs across generators'
    result['accepted_ids'] = sorted(ids, key=int)
    result['error_samples'] = [v for s in summaries for v in s['error_samples']]
    result['drain_timed_out'] = any(s['drain_timed_out'] for s in summaries)
    result['submission_elapsed_s'] = max(s['started_epoch_ms'] / 1000 + s['submission_elapsed_s'] for s in summaries) - result['started_epoch_ms'] / 1000
    result['total_elapsed_s'] = (result['finished_epoch_ms'] - result['started_epoch_ms']) / 1000
    result['achieved_accept_rate_jobs_s'] = result['accepted'] / result['submission_elapsed_s']
    result['completion_rate_during_total_jobs_s'] = result['completed'] / result['total_elapsed_s']
    result['configured_rate_requests_s'] = sum(s['configured_rate_requests_s'] for s in summaries)
    result['configured_stages'] = [dict(duration_s=stage['duration_s'],
        rate_requests_s=sum(s['configured_stages'][i]['rate_requests_s'] for s in summaries),
        rate_jobs_s=sum(s['configured_stages'][i]['rate_jobs_s'] for s in summaries))
        for i, stage in enumerate(first['configured_stages'])]
    result['max_client_pending'] = None
    result['client_pending_upper_bound'] = sum(s['max_client_pending'] for s in summaries)
    result['latency'] = {key: dict(count=sum(s['latency'][key]['count'] for s in summaries))
                         for key in first['latency']}
    result['latency_note'] = 'Exact percentiles in generator_summary records only; no pooled percentiles inferred'
    result['phases'] = []
    for i, phase in enumerate(first['phases']):
        merged = dict(index=i, duration_s=phase['duration_s'],
                      rate_requests_s=sum(s['phases'][i]['rate_requests_s'] for s in summaries))
        for key in ('scheduled', 'attempted', 'accepted', 'completed', 'dropped', 'submit_errors'):
            merged[key] = sum(s['phases'][i][key] for s in summaries)
        merged['generators'] = [s['phases'][i] for s in summaries]
        result['phases'].append(merged)
    return result


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--generators', type=int, default=2)
    control, arguments = parser.parse_known_args()
    if control.generators not in (1, 2):
        raise SystemExit('Supported generator counts: 1 or 2')
    config = parse_args(arguments)
    stages = ','.join(f'{duration}:{rate / control.generators}' for duration, rate in config.parsed_stages)
    # Do not silently truncate an odd total number of scheduled requests.
    if any(int(duration * rate) % control.generators for duration, rate in config.parsed_stages):
        raise SystemExit('Each stage request count must be divisible by generator count')
    target = int(time.time() * 1000) + 15000
    summaries, lock = {}, threading.Lock()

    def relay(process, index):
        for line in process.stdout:
            value = json.loads(line)
            with lock:
                if value.get('type') == 'summary':
                    summaries[index] = dict(value)
                    value['type'] = 'generator_summary'
                value['generator'] = index
                print(json.dumps(value), flush=True)

    processes, readers = [], []
    try:
        for i in range(control.generators):
            process = subprocess.Popen([sys.executable, str(Path(__file__).with_name('benchmark_v2.py')),
                *arguments, '--stages', stages, '--scenario', f'{config.scenario}-g{i + 1}',
                '--start-at-epoch-ms', str(target)], stdout=subprocess.PIPE, text=True)
            processes.append(process)
            reader = threading.Thread(target=relay, args=(process, i + 1))
            readers.append(reader)
            reader.start()
        codes = [p.wait() for p in processes]
        for reader in readers:
            reader.join()
        if len(summaries) != control.generators:
            raise SystemExit('Missing generator summary; partial evidence retained')
        result = merge([summaries[i + 1] for i in range(control.generators)], config.scenario)
        print(json.dumps(result), flush=True)
        return 0 if all(code == 0 for code in codes) and result['generator_start_spread_ms'] <= 50 else 2
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)


if __name__ == '__main__':
    raise SystemExit(main())
