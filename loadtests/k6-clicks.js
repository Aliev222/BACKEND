import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: Number(__ENV.VUS || 50),
  duration: __ENV.DURATION || "2m",
  thresholds: {
    http_req_duration: ["p(95)<400", "p(99)<800"],
    http_req_failed: ["rate<0.01"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const INIT_DATA = __ENV.INIT_DATA || ""; // Telegram init data (signed)
const USER_ID = Number(__ENV.USER_ID || 1);

export default function () {
  // Health
  check(http.get(`${BASE_URL}/health`), { "health 200": (r) => r.status === 200 });

  // Fetch user
  const userRes = http.get(`${BASE_URL}/api/user/${USER_ID}`, {
    headers: { "X-Telegram-Init-Data": INIT_DATA },
  });
  check(userRes, { "user 200": (r) => r.status === 200 });

  // Click batch with minimal payload
  const clicksRes = http.post(
    `${BASE_URL}/api/clicks`,
    JSON.stringify({ user_id: USER_ID, clicks: 10 }),
    {
      headers: {
        "Content-Type": "application/json",
        "X-Telegram-Init-Data": INIT_DATA,
      },
    }
  );
  check(clicksRes, { "clicks <=429": (r) => r.status === 200 || r.status === 429 });

  sleep(1);
}
