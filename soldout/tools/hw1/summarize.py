#!/usr/bin/env python3
"""Сводка сохранённых артефактов HW1 без обращения к работающему стенду."""
import collections
import json
from pathlib import Path

ARTIFACTS = Path(__file__).resolve().parents[3] / 'hw/hw1/artifacts'


def trace_totals(value, result):
    if isinstance(value, dict):
        if 'name' in value and 'startTimeUnixNano' in value:
            result[value['name']]['count'] += 1
            result[value['name']]['ms'] += (int(value['endTimeUnixNano']) - int(value['startTimeUnixNano'])) / 1e6
        for child in value.values():
            trace_totals(child, result)
    elif isinstance(value, list):
        for child in value:
            trace_totals(child, result)


def main():
    comparison = {}
    for stage in ('baseline', 'opt1', 'opt2', 'acceptance'):
        directory = ARTIFACTS / stage
        if not (directory/'k6-summary.json').exists():
            continue
        metrics = json.loads((directory/'k6-summary.json').read_text())['metrics']
        statements = json.loads((directory/'pg-statements.json').read_text())
        trace = collections.defaultdict(lambda: {'count': 0, 'ms': 0.0})
        trace_totals(json.loads((directory/'slow-trace.json').read_text()), trace)
        row = {
            'purchases': metrics['friends_purchases']['count'],
            'http_rps': metrics['http_reqs']['rate'],
            'group_5xx_rate': metrics['friends_5xx']['value'],
            'latency_ms': {step: {p: metrics[f'friends_{step}_ms'][p] for p in ('p(50)', 'p(95)', 'p(99)')} for step in ('join', 'group', 'order', 'pay', 'tickets')},
            'db_top5_seconds': sum(q['total_exec_time'] for q in statements[:5]) / 1000,
            'trace': dict(trace),
        }
        if (directory/'storm-summary.json').exists():
            storm = json.loads((directory/'storm-summary.json').read_text())['metrics']
            row['storm_hold_p99_ms'] = storm['http_req_duration{name:hold}']['p(99)']
            row['storm_http_failed_rate'] = storm['http_req_failed']['value']
            row['storm_http_requests'] = storm['http_reqs']['count']
        comparison[stage] = row
        latency = row['latency_ms']['group']
        print(f'{stage}: {row["purchases"]} покупок, {row["http_rps"]:.2f} RPS, 5xx={row["group_5xx_rate"]}, group p50/p95/p99: {latency["p(50)"]:.2f}/{latency["p(95)"]:.2f}/{latency["p(99)"]:.2f} ms, DB top5={row["db_top5_seconds"]:.3f} s')
    if 'baseline' in comparison and 'opt2' in comparison:
        before = comparison['baseline']['latency_ms']['group']['p(99)']
        after = comparison['opt2']['latency_ms']['group']['p(99)']
        print(f'Изменение p99 baseline → opt2: {(after/before-1)*100:.2f}%')
    (ARTIFACTS/'comparison.json').write_text(json.dumps(comparison, ensure_ascii=False, indent=2)+'\n')


if __name__ == '__main__':
    main()
