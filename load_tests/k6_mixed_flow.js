import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');

// Test configuration - mixed realistic flow
export const options = {
  stages: [
    { duration: '2m', target: 50 },    // Warm up
    { duration: '5m', target: 200 },   // Normal load
    { duration: '5m', target: 500 },   // High load
    { duration: '3m', target: 0 },     // Cool down
  ],
  thresholds: {
    'http_req_duration': ['p(95)<800', 'p(99)<1500'],
    'http_req_failed': ['rate<0.02'],
    'errors': ['rate<0.02'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

function getTestUserId() {
  return 1000000 + Math.floor(Math.random() * 10000);
}

export default function () {
  const userId = getTestUserId();
  const headers = {
    'Content-Type': 'application/json',
    'X-Telegram-User-Id': `${userId}`,
  };

  // Scenario weights (realistic user behavior)
  const scenario = Math.random();

  if (scenario < 0.6) {
    // 60% - Click flow
    executeClicks(userId, headers);
  } else if (scenario < 0.85) {
    // 25% - Profile check
    checkProfile(userId, headers);
  } else if (scenario < 0.95) {
    // 10% - Boost activation
    activateBoost(userId, headers);
  } else {
    // 5% - Passive income claim
    claimPassiveIncome(userId, headers);
  }

  sleep(Math.random() * 3 + 1); // 1-4 seconds between actions
}

function executeClicks(userId, headers) {
  const payload = JSON.stringify({
    user_id: userId,
    clicks: Math.floor(Math.random() * 15) + 1,
    timestamp: Date.now(),
    idempotency_key: `${userId}_${Date.now()}_${Math.random()}`,
  });

  const res = http.post(`${BASE_URL}/api/clicks`, payload, {
    headers,
    tags: { name: 'clicks' },
  });

  const success = check(res, {
    'clicks status 200': (r) => r.status === 200,
  });

  errorRate.add(success ? 0 : 1);
}

function checkProfile(userId, headers) {
  const res = http.get(`${BASE_URL}/api/profile/${userId}`, {
    headers,
    tags: { name: 'profile' },
  });

  const success = check(res, {
    'profile status 200': (r) => r.status === 200,
    'profile has coins': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.coins !== undefined;
      } catch {
        return false;
      }
    },
  });

  errorRate.add(success ? 0 : 1);
}

function activateBoost(userId, headers) {
  // Create ad session first
  const sessionPayload = JSON.stringify({
    user_id: userId,
    action: 'mega_boost',
  });

  const sessionRes = http.post(
    `${BASE_URL}/api/ads/action/start`,
    sessionPayload,
    { headers, tags: { name: 'ad_session' } }
  );

  if (sessionRes.status !== 200) {
    errorRate.add(1);
    return;
  }

  let adSessionId;
  try {
    const body = JSON.parse(sessionRes.body);
    adSessionId = body.ad_session_id;
  } catch {
    errorRate.add(1);
    return;
  }

  // Wait for ad "completion"
  sleep(2);

  // Activate boost
  const boostPayload = JSON.stringify({
    user_id: userId,
    ad_session_id: adSessionId,
  });

  const boostRes = http.post(
    `${BASE_URL}/api/activate-mega-boost`,
    boostPayload,
    { headers, tags: { name: 'boost_activate' } }
  );

  const success = check(boostRes, {
    'boost status 200 or 429': (r) => r.status === 200 || r.status === 429,
  });

  errorRate.add(success ? 0 : 1);
}

function claimPassiveIncome(userId, headers) {
  const payload = JSON.stringify({
    user_id: userId,
  });

  const res = http.post(`${BASE_URL}/api/passive-income`, payload, {
    headers,
    tags: { name: 'passive_income' },
  });

  const success = check(res, {
    'passive status 200': (r) => r.status === 200,
  });

  errorRate.add(success ? 0 : 1);
}
