import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Counter } from 'k6/metrics';

const errorRate = new Rate('errors');
const authFailures = new Counter('auth_failures');

export const options = {
  discardResponseBodies: false,
  stages: [
    { duration: '1m', target: 500 },
    { duration: '2m', target: 1500 },
    { duration: '1m', target: 2500 },
    { duration: '1m', target: 1500 },
    { duration: '1m', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.10'],
    http_req_duration: ['p(95)<2000', 'p(99)<4000'],
    errors: ['rate<0.10'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8001';
const USER_ID_OFFSET = Number(__ENV.USER_ID_OFFSET || 2_000_000);
const MOBILE_HEADER = __ENV.MOBILE_HEADER || '1';

let token = '';
let registered = false;
let userId = 0;

function getUserId() {
  if (!userId) userId = USER_ID_OFFSET + __VU;
  return userId;
}

function getAuthHeaders() {
  return {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${token}`,
    'X-Client-Mobile': MOBILE_HEADER,
  };
}

function ensureSession() {
  if (token) return true;
  const uid = getUserId();
  const res = http.post(
    `${BASE_URL}/api/debug/session`,
    JSON.stringify({ user_id: uid }),
    { headers: { 'Content-Type': 'application/json' }, tags: { name: 'debug_session' } },
  );
  if (res.status !== 200) {
    authFailures.add(1);
    errorRate.add(1);
    return false;
  }
  try {
    token = (res.json('token') || '').trim();
  } catch (_) {
    token = '';
  }
  if (!token) {
    authFailures.add(1);
    errorRate.add(1);
    return false;
  }
  return true;
}

export default function () {
  if (!ensureSession()) {
    sleep(0.2);
    return;
  }
  registered = true;

  const uid = getUserId();
  const headers = getAuthHeaders();
  const scenario = Math.random();

  if (scenario < 0.85) {
    const clickPayload = JSON.stringify({
      user_id: uid,
      clicks: Math.floor(Math.random() * 15) + 1,
      timestamp: Date.now(),
      idempotency_key: `${uid}_${Date.now()}_${Math.random()}`,
    });
    const clickRes = http.post(`${BASE_URL}/api/clicks`, clickPayload, {
      headers,
      tags: { name: 'clicks' },
    });
    const ok = check(clickRes, {
      'click status 200': (r) => r.status === 200,
    });
    errorRate.add(ok ? 0 : 1);
  } else {
    const profileRes = http.get(`${BASE_URL}/api/user/${uid}`, {
      headers,
      tags: { name: 'user' },
    });
    const ok = check(profileRes, {
      'user status 200': (r) => r.status === 200,
    });
    errorRate.add(ok ? 0 : 1);
  }

  sleep(Math.random() * 0.35 + 0.05);
}
