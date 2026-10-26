// V3: детерминированная нагрузка на весь путь join → group → order → pay → tickets.
import http from 'k6/http';
import { check, randomSeed } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import exec from 'k6/execution';
import { uuid } from './lib.js';

const base = __ENV.BASE_URL || 'http://localhost:8080';
const eventID = '10000000-0000-4000-8000-000000000002';
const sectors = Number(__ENV.SEATS_PER_EVENT || '80000') / 1000 - 2;
const failures = new Rate('friends_5xx');
const purchases = new Counter('friends_purchases');
const conflicts = new Counter('friends_conflicts');
const stages = {};
for (const step of ['join', 'group', 'order', 'pay', 'tickets']) stages[step] = new Trend(`friends_${step}_ms`, true);

export const options = {
  scenarios: { friends: {
    executor: 'constant-arrival-rate', rate: Number(__ENV.FRIENDS_RATE || '20'), timeUnit: '1s',
    duration: __ENV.FRIENDS_DURATION || '60s', preAllocatedVUs: 30, maxVUs: 150,
    gracefulStop: '15s',
  } },
  thresholds: {
    friends_5xx: ['rate==0'], checks: ['rate==1'], dropped_iterations: ['count==0'],
    friends_purchases: [`count>=${__ENV.FRIENDS_MIN_PURCHASES || '1'}`],
    friends_group_ms: ['p(95)<300', 'p(99)<500'],
    friends_join_ms: ['p(99)<500'], friends_order_ms: ['p(99)<500'],
    friends_pay_ms: ['p(99)<1000'], friends_tickets_ms: ['p(99)<500'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

function call(step, path, body, headers = {}) {
  const params = { headers: Object.assign({ 'Content-Type': 'application/json' }, headers), tags: { name: `friends_${step}` } };
  const response = step === 'tickets' ? http.get(base + path, params) : http.post(base + path, JSON.stringify(body), params);
  stages[step].add(response.timings.duration);
  failures.add(response.status === 0 || response.status >= 500);
  return response;
}

export default function () {
  // Каждая итерация имеет собственный seed: сравнение вариантов использует тот же набор входов.
  const iteration = exec.scenario.iterationInTest;
  randomSeed(20261026 + iteration);
  const user = uuid();
  const n = iteration % 10;
  const size = n < 1 ? 1 : n < 5 ? 2 : n < 8 ? 3 : 4;
  const sectorNo = iteration % 5 < 4 ? 1 + (Math.floor(iteration / 5) % Math.min(4, sectors)) : 1 + (iteration % sectors);
  const sector = '30000000-0000-4000-8000-' + (2000 + sectorNo).toString(16).padStart(12, '0');
  const j = call('join', `/v1/events/${eventID}/queue/join`, { user_id: user });
  if (!check(j, { 'join 200': r => r.status === 200 })) return;
  const g = call('group', '/v1/holds/group', { event_id: eventID, sector_id: sector, user_id: user, size },
    { 'X-Admission-Token': j.json('token'), 'Idempotency-Key': uuid() });
  if (g.status === 409 && g.json('error.code') === 'no_adjacent_seats') {
    conflicts.add(1); return;
  }
  if (!check(g, { 'group 201 и N hold': r => r.status === 201 && r.json('holds').length === size })) return;
  const group = g.json();
  check(group, { 'группа в одном ряду': x => x.row_no > 0 && x.size === size });
  const o = call('order', '/v1/orders', { user_id: user, hold_ids: group.holds.map(h => h.id) }, { 'Idempotency-Key': uuid() });
  if (!check(o, { 'order 201 и сумма N билетов': r => r.status === 201 && r.json('amount_minor') === size * 500000 })) return;
  const p = call('pay', `/v1/orders/${o.json('id')}/pay`, null, { 'Idempotency-Key': uuid() });
  if (!check(p, { 'pay 200 paid': r => r.status === 200 && r.json('status') === 'paid' })) return;
  const t = call('tickets', `/v1/users/${user}/tickets`, null);
  if (check(t, { 'ровно N билетов на места группы': r => r.status === 200 && (r.json('tickets') || []).length === size && r.json('tickets').every(ticket => group.holds.some(h => h.seat_id === ticket.seat_id)) })) purchases.add(1);
}
