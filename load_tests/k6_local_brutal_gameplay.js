import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

const VUS_MAX = Number(__ENV.VUS_MAX || 4000);
const USER_ID_OFFSET = Number(__ENV.USER_ID_OFFSET || 2_000_000);
const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8001';
const MOBILE_HEADER = __ENV.MOBILE_HEADER || '1';

export const options = {
  discardResponseBodies: false,
  stages: [
    { duration: '1m', target: 1000 },
    { duration: '1m', target: 2500 },
    { duration: '1m', target: 4000 },
    { duration: '1m', target: 4000 },
    { duration: '1m', target: 2000 },
    { duration: '1m', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.20'],
    http_req_duration: ['p(95)<4000', 'p(99)<7000'],
    errors: ['rate<0.20'],
  },
};

export function setup() {
  const tokens = new Array(VUS_MAX).fill('');
  for (let i = 0; i < VUS_MAX; i += 1) {
    const uid = USER_ID_OFFSET + i + 1;
    const res = http.post(
      `${BASE_URL}/api/debug/session`,
      JSON.stringify({ user_id: uid }),
      { headers: { 'Content-Type': 'application/json' }, tags: { name: 'debug_session_setup' } },
    );
    if (res.status === 200) {
      try {
        tokens[i] = (res.json('token') || '').trim();
      } catch (_) {
        tokens[i] = '';
      }
    }
  }
  return { tokens };
}

export default function (data) {
  const idx = __VU - 1;
  const token = data.tokens[idx] || '';
  if (!token) {
    errorRate.add(1);
    sleep(0.2);
    return;
  }

  const uid = USER_ID_OFFSET + __VU;
  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${token}`,
    'X-Client-Mobile': MOBILE_HEADER,
  };

  if (Math.random() < 0.9) {
    const clickPayload = JSON.stringify({
      user_id: uid,
      clicks: Math.floor(Math.random() * 15) + 1,
      batch_id: `${uid}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`,
    });
    const clickRes = http.post(`${BASE_URL}/api/clicks`, clickPayload, {
      headers,
      tags: { name: 'clicks' },
    });
    const ok = check(clickRes, { 'click status 200': (r) => r.status === 200 });
    errorRate.add(ok ? 0 : 1);
  } else {
    const profileRes = http.get(`${BASE_URL}/api/user/${uid}`, {
      headers,
      tags: { name: 'user' },
    });
    const ok = check(profileRes, { 'user status 200': (r) => r.status === 200 });
    errorRate.add(ok ? 0 : 1);
  }

  sleep(Math.random() * 0.2 + 0.03);
}
