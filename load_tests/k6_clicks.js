import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');

// Test configuration
export const options = {
  stages: [
    { duration: '2m', target: 100 },   // Ramp up to 100 users
    { duration: '5m', target: 100 },   // Stay at 100 users
    { duration: '2m', target: 500 },   // Ramp up to 500 users
    { duration: '5m', target: 500 },   // Stay at 500 users
    { duration: '2m', target: 1000 },  // Ramp up to 1000 users
    { duration: '10m', target: 1000 }, // Stay at 1000 users (peak load)
    { duration: '3m', target: 0 },     // Ramp down to 0
  ],
  thresholds: {
    'http_req_duration': ['p(95)<500', 'p(99)<1000'], // 95% < 500ms, 99% < 1s
    'http_req_failed': ['rate<0.01'],                  // Error rate < 1%
    'errors': ['rate<0.01'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

// Generate test user IDs (simulate real user distribution)
function getTestUserId() {
  return 1000000 + Math.floor(Math.random() * 10000);
}

export default function () {
  const userId = getTestUserId();
  const headers = {
    'Content-Type': 'application/json',
    'X-Telegram-User-Id': `${userId}`,
  };

  // Click request payload
  const clickPayload = JSON.stringify({
    user_id: userId,
    clicks: Math.floor(Math.random() * 10) + 1, // 1-10 clicks
    timestamp: Date.now(),
    idempotency_key: `${userId}_${Date.now()}_${Math.random()}`,
  });

  // Execute click
  const clickRes = http.post(
    `${BASE_URL}/api/clicks`,
    clickPayload,
    { headers, tags: { name: 'clicks' } }
  );

  // Check response
  const clickSuccess = check(clickRes, {
    'click status is 200': (r) => r.status === 200,
    'click response has coins': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.coins !== undefined;
      } catch {
        return false;
      }
    },
  });

  if (!clickSuccess) {
    errorRate.add(1);
  } else {
    errorRate.add(0);
  }

  // Simulate realistic user behavior: wait between clicks
  sleep(Math.random() * 2 + 0.5); // 0.5-2.5 seconds
}
