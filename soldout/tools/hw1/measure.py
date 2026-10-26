#!/usr/bin/env python3
"""Воспроизводимый эксперимент HW1 на отдельном локальном compose-стенде soldout.

Удаляет только данные покупки/проекции учебного стенда. Запускать из soldout:
python3 tools/hw1/measure.py baseline --rate 8 --duration 60s
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ['docker', 'compose', '-f', 'docker-compose.yml', '-f', 'compose/lesson-02.yml', '-f', 'compose/lesson-04.yml']


def run(args, *, input=None, check=True):
    result = subprocess.run(args, cwd=ROOT, input=input, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if check and result.returncode:
        raise RuntimeError(f'{args}: exit {result.returncode}\n{result.stdout[-5000:]}')
    return result


def sql(query):
    return run(COMPOSE + ['exec', '-T', 'postgres', 'psql', '-U', 'soldout', '-d', 'soldout', '-At', '-v', 'ON_ERROR_STOP=1', '-c', query]).stdout.strip()


def get(path):
    with urllib.request.urlopen(path, timeout=15) as response:
        return response.read().decode()


def error_logs(since):
    # Шторм пишет сотни тысяч строк. Сохраняем все ошибки/предупреждения, без полного access log.
    proc = subprocess.Popen(COMPOSE + ['logs', '--no-log-prefix', '--since', str(since), 'soldout'], cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    lines = [line for line in proc.stdout if 'level=ERROR' in line or 'level=WARN' in line or '"level":"ERROR"' in line or '"level":"WARN"' in line]
    if proc.wait(): raise RuntimeError('Не удалось прочитать журналы soldout')
    return ''.join(lines) if lines else 'Нет строк уровня ERROR/WARN.\n'


def quiet():
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        metrics = get('http://localhost:8080/metrics')
        depths = [float(line.split()[-1]) for line in metrics.splitlines() if line.startswith('eventbus_queue_depth')]
        pending = int(sql("SELECT count(*) FROM orders o LEFT JOIN notifications n ON n.order_id=o.id AND n.kind='tickets_issued' WHERE o.status='paid' AND n.id IS NULL"))
        if depths and max(depths) == 0 and pending == 0:
            return metrics
        time.sleep(2)
    raise RuntimeError('Очередь/уведомления не догнали за 90 с')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage')
    parser.add_argument('--rate', type=int, default=8)
    parser.add_argument('--duration', default='60s')
    parser.add_argument('--storm', action='store_true')
    args = parser.parse_args()
    out = ROOT.parent / 'hw/hw1/artifacts' / args.stage
    out.mkdir(parents=True, exist_ok=False)
    started = int(time.time())
    print(f'{args.stage}: осушаем очереди и очищаем данные отдельного учебного стенда', flush=True)
    quiet()
    sql('TRUNCATE hold_group_items, hold_groups, order_items, payments, tickets, notifications, orders, holds, admissions, catalog_seat_state, notifier.processed_events CASCADE; UPDATE catalog_sector_version SET version=version+1; ANALYZE;')
    run(COMPOSE + ['exec', '-T', 'valkey', 'valkey-cli', 'FLUSHDB'])
    hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(ROOT.rglob('*.go')) if 'tmp' not in p.parts}
    (out/'source-sha256.json').write_text(json.dumps(hashes, indent=2)+'\n')
    inspected = json.loads(run(['docker', 'inspect', 'soldout-postgres-1', 'soldout-soldout-1']).stdout)
    limits = [{'name': c['Name'], 'image': c['Config']['Image'], 'image_id': c['Image'], 'nano_cpus': c['HostConfig']['NanoCpus'], 'memory_bytes': c['HostConfig']['Memory']} for c in inspected]
    (out/'conditions.json').write_text(json.dumps({
        'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'stage': args.stage,
        'rate': args.rate, 'duration': args.duration, 'seed': 20261026, 'seats_per_event': 80000,
        'head': run(['git', 'rev-parse', 'HEAD']).stdout.strip(),
        'dirty': run(['git', 'status', '--porcelain']).stdout,
        'limits': limits,
    }, indent=2)+'\n')
    run(['make', 'pg-reset'])
    env = dict(os.environ, FRIENDS_RATE=str(args.rate), FRIENDS_DURATION=args.duration)
    cmd = ['make', 'friends', f'STEP={args.stage}', f'FRIENDS_MIN_PURCHASES={max(1, int(args.rate*float(args.duration.rstrip("s"))*0.95))}']
    print(f'{args.stage}: k6 {args.rate} групп/с, {args.duration}, снимаем CPU-профиль во время нагрузки', flush=True)
    with (out/'k6.log').open('w') as log:
        load = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        time.sleep(5)
        (out/'docker-stats.txt').write_text(run(['make', 'docker-stats'], check=False).stdout)
        (out/'pg-activity.txt').write_text(run(['make', 'pg-activity'], check=False).stdout)
        (out/'pprof-command.log').write_text(run(['make', 'pprof-top', f'STEP={args.stage}'], check=False).stdout)
        profile = ROOT/f'k6/results/pprof-top-{args.stage}.txt'
        if not profile.exists() or 'flat' not in profile.read_text():
            load.wait()
            raise RuntimeError('make pprof-top не сохранил непустой профиль')
        (out/'pprof.txt').write_bytes(profile.read_bytes())
        k6_rc = load.wait()
    summary = ROOT/f'k6/results/friends-{args.stage}.json'
    if summary.exists(): (out/'k6-summary.json').write_bytes(summary.read_bytes())
    (out/'pg-top.txt').write_text(run(['make', 'pg-top']).stdout)
    (out/'pg-statements.json').write_text(sql("SELECT coalesce(json_agg(t),'[]'::json) FROM (SELECT calls,total_exec_time,mean_exec_time,rows,query FROM pg_stat_statements WHERE query NOT LIKE '%pg_stat_statements%' AND query NOT LIKE '%pg_stat_activity%' ORDER BY total_exec_time DESC LIMIT 20) t")+'\n')
    (out/'explain.txt').write_text(sql("EXPLAIN (ANALYZE,BUFFERS) SELECT EXISTS(SELECT 1 FROM holds WHERE event_id='10000000-0000-4000-8000-000000000002' AND seat_id=80001 AND status IN ('active','confirmed'))")+'\n')
    print(f'{args.stage}: k6 exit={k6_rc}, ждём проекцию и уведомления', flush=True)
    (out/'metrics.txt').write_text(quiet())
    (out/'soldout-errors.log').write_text(error_logs(started))
    inv = run(['make', 'invariants'], check=False)
    (out/'invariants.txt').write_text(inv.stdout)
    time.sleep(5)
    q = '{ name = "booking.CreateGroupHold" }'
    url = 'http://localhost:3200/api/search?' + urllib.parse.urlencode({'q': q, 'start': started, 'end': int(time.time()), 'limit': 1000})
    traces = json.loads(get(url))
    (out/'traces-search.json').write_text(json.dumps(traces, indent=2)+'\n')
    candidates = sorted(traces.get('traces', []), key=lambda x: x.get('durationMs', 0), reverse=True)
    if candidates:
        traceid = candidates[0]['traceID'].rjust(32, '0')
        (out/'slow-trace.json').write_text(get(f'http://localhost:3200/api/v2/traces/{traceid}'))
        (out/'slow-trace-id.txt').write_text(traceid+'\n')
    storm_rc = storm_invariants_rc = None
    if args.storm:
        print(f'{args.stage}: контрольный make storm на мероприятии 1', flush=True)
        storm = run(['make', 'storm', f'STEP={args.stage}'], check=False)
        storm_rc = storm.returncode
        (out/'storm.log').write_text(storm.stdout)
        result = ROOT/f'k6/results/storm-{args.stage}.json'
        if result.exists(): (out/'storm-summary.json').write_bytes(result.read_bytes())
        (out/'metrics-after-storm.txt').write_text(quiet())
        after = run(['make', 'invariants'], check=False)
        storm_invariants_rc = after.returncode
        (out/'invariants-after-storm.txt').write_text(after.stdout)
        (out/'soldout-errors-after-storm.log').write_text(error_logs(started))
    (out/'exit-codes.json').write_text(json.dumps({'k6': k6_rc, 'invariants': inv.returncode, 'storm': storm_rc, 'storm_invariants': storm_invariants_rc}, indent=2)+'\n')
    print(f'{args.stage}: артефакты {out}, invariants exit={inv.returncode}', flush=True)
    if k6_rc or inv.returncode:
        print((out/'k6.log').read_text()[-5000:])
    raise SystemExit(1 if k6_rc or inv.returncode or storm_rc or storm_invariants_rc else 0)


if __name__ == '__main__':
    main()
