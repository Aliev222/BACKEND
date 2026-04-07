import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');

// Spike test configuration - sudden traffic surge
export const options = {
  stages: [
    { duration: '1m', target: 100 },   // Normal baseline
    { duration: '30s', target: 2000 }, // SPIKE to 2000 users
    { duration: '3m', target: 2000 },  // Hold spike
    { duration: '1m', target: 100 },   // Drop back to normal
    { duration: '2m', target: 100 },   // Recovery period
    { duration: '30s', target: 0 },    // Ramp down
  ],
  thresholds: {
    'http_req_duration': ['p(95)<1000', 'p(99)<2000'], // More lenient during spike
    'http_req_failed': ['rate<0.05'],                   // Allow 5% errors during spike
    'errors': ['rate<0.05'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

function getTestUserId() {
  return 1000000 + Math.floor(Math.random() * 20000); // Larger user pool for spike
}

export default function () {
  const userId = getTestUserId();
  const headers = {
    'Content-Type': 'application/json',
    'X-Telegram-User-Id': `${userId}`,
  };

  // During spike, focus on high-traffic endpoints
  const scenario = Math.random();

  if (scenario < 0.8) {
    // 80% - Clicks (main load)
    const payload = JSON.stringify({
      user_id: userId,
      clicks: Math.floor(Math.random() * 20) + 1,
      timestamp: Date.now(),
      idempotency_key: `${userId}_${Date.now()}_${Math.random()}`,
    });

    const res = http.post(`${BASE_URL}/api/clicks`, payload, {
      headers,
      tags: { name: 'clicks_spike' },
    });

    const success = check(res, {
      'spike clicks status ok': (r) => r.status === 200 || r.status === 429,
    });

    errorRate.add(success ? 0 : 1);
  } else {
    // 20% - Profile reads
    const res = http.get(`${BASE_URL}/api/profile/${userId}`, {
      headers,
      tags: { name: 'profile_spike' },
    });

    const success = check(res, {
      'spike profile status ok': (r) => r.status === 200,
    });

    errorRate.add(success ? 0 : 1);
  }

  // Minimal sleep during spike (aggressive load)
  sleep(Math.random() * 0.5 + 0.1); // 0.1-0.6 seconds
}
