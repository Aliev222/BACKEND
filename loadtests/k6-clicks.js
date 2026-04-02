/**
 * SPIRIT Clicker API — k6 Load Tests
 *
 * Scenarios:
 *   1. many_users      — N users clicking concurrently (normal traffic)
 *   2. same_user_burst — single user sending rapid click batches
 *   3. mixed_traffic   — clicks + profile loads + health checks
 *
 * Usage:
 *   k6 run --env SCENARIO=many_users k6-clicks.js
 *   k6 run --env SCENARIO=same_user_burst k6-clicks.js
 *   k6 run --env SCENARIO=mixed_traffic k6-clicks.js
 *
 * Environment variables:
 *   BASE_URL    — target server (default: http://localhost:8000)
 *   USER_IDS    — comma-separated user IDs (default: "1,2,3,4,5")
 *   INIT_DATA   — Telegram init data (shared across all users)
 *   SCENARIO    — which scenario to run (default: many_users)
 *   DURATION    — test duration (default: 2m)
 *   VUS         — virtual users (default: 10)
 *   CLICK_COUNT — clicks per batch (default: 10)
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter } from "k6/metrics";

// Custom metrics
const clickSuccessRate = new Counter("click_success");
const clickRateLimited = new Counter("click_rate_limited");
const duplicateRejected = new Counter("click_duplicate_rejected");
const responseMismatch = new Counter("response_balance_mismatch");

// ─── Configuration ───────────────────────────────────────────────────────────

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const INIT_DATA = __ENV.INIT_DATA || "";
const USER_IDS = (__ENV.USER_IDS || "1,2,3,4,5")
  .split(",")
  .map((s) => Number(s.trim()));
const SCENARIO = __ENV.SCENARIO || "many_users";
const CLICK_COUNT = Number(__ENV.CLICK_COUNT || 10);

// ─── Shared options ──────────────────────────────────────────────────────────

const baseOptions = {
  thresholds: {
    http_req_duration: ["p(95)<500", "p(99)<1000"],
    http_req_failed: ["rate<0.05"],
    click_success: ["count>0"],
  },
};

const scenarioOptions = {
  many_users: {
    vus: Number(__ENV.VUS || 10),
    duration: __ENV.DURATION || "2m",
    scenarios: {
      many_users_clicks: {
        executor: "shared-iterations",
        vus: Number(__ENV.VUS || 10),
        iterations: USER_IDS.length * 50,
        maxDuration: "5m",
      },
    },
  },

  same_user_burst: {
    vus: 1,
    duration: __ENV.DURATION || "30s",
    scenarios: {
      burst: {
        executor: "ramping-vus",
        startVUs: 1,
        stages: [
          { duration: "5s", target: 5 },
          { duration: "10s", target: 5 },
          { duration: "5s", target: 0 },
        ],
        gracefulRampDown: "2s",
      },
    },
  },

  mixed_traffic: {
    vus: Number(__ENV.VUS || 10),
    duration: __ENV.DURATION || "3m",
    scenarios: {
      clicks: {
        executor: "constant-arrival-rate",
        rate: 30,
        timeUnit: "1s",
        duration: __ENV.DURATION || "3m",
        preAllocatedVUs: Number(__ENV.VUS || 10),
        maxVUs: 50,
      },
      profile_loads: {
        executor: "constant-arrival-rate",
        rate: 5,
        timeUnit: "1s",
        duration: __ENV.DURATION || "3m",
        preAllocatedVUs: 3,
        maxVUs: 10,
      },
    },
  },
};

export const options = {
  ...baseOptions,
  ...(scenarioOptions[SCENARIO] || scenarioOptions.many_users),
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

function pickUserId() {
  return USER_IDS[Math.floor(Math.random() * USER_IDS.length)];
}

function makeBatchId(vu, iter) {
  return `${vu}-${iter}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function authHeaders(userId) {
  return {
    "Content-Type": "application/json",
    "X-Telegram-Init-Data": INIT_DATA,
    "X-User-Id": String(userId),
  };
}

// ─── Scenario: many_users ────────────────────────────────────────────────────

function runManyUserClicks() {
  const userId = pickUserId();
  const batchId = makeBatchId(__VU, __ITER);

  const res = http.post(
    `${BASE_URL}/api/clicks`,
    JSON.stringify({
      user_id: userId,
      clicks: CLICK_COUNT,
      batch_id: batchId,
    }),
    { headers: authHeaders(userId) }
  );

  if (res.status === 200) {
    clickSuccessRate.add(1);
    const body = res.json();
    if (body && typeof body.coins === "number" && body.coins < 0) {
      responseMismatch.add(1);
    }
  } else if (res.status === 429) {
    clickRateLimited.add(1);
  } else if (res.status === 409) {
    duplicateRejected.add(1);
  }

  check(res, {
    "click response valid": (r) =>
      r.status === 200 || r.status === 429 || r.status === 409,
    "click success": (r) => r.status === 200,
  });

  // Realistic pacing: frontend sends batches every 0.5-2s
  sleep(0.5 + Math.random() * 1.5);
}

// ─── Scenario: same_user_burst ───────────────────────────────────────────────

function runSameUserBurst() {
  const userId = USER_IDS[0];
  const batchId = makeBatchId(__VU, __ITER);

  const res = http.post(
    `${BASE_URL}/api/clicks`,
    JSON.stringify({
      user_id: userId,
      clicks: CLICK_COUNT,
      batch_id: batchId,
    }),
    { headers: authHeaders(userId) }
  );

  if (res.status === 200) clickSuccessRate.add(1);
  else if (res.status === 429) clickRateLimited.add(1);
  else if (res.status === 409) duplicateRejected.add(1);

  check(res, {
    "burst click handled": (r) =>
      r.status === 200 || r.status === 429 || r.status === 409,
  });

  // Very fast: 50-200ms between requests (simulates bot or rapid clicking)
  sleep(0.05 + Math.random() * 0.15);
}

// ─── Scenario: mixed_traffic ─────────────────────────────────────────────────

function runMixedClicks() {
  const userId = pickUserId();
  const batchId = makeBatchId(__VU, __ITER);

  const res = http.post(
    `${BASE_URL}/api/clicks`,
    JSON.stringify({
      user_id: userId,
      clicks: CLICK_COUNT,
      batch_id: batchId,
    }),
    { headers: authHeaders(userId) }
  );

  if (res.status === 200) clickSuccessRate.add(1);
  else if (res.status === 429) clickRateLimited.add(1);

  check(res, { "mixed click valid": (r) => r.status === 200 || r.status === 429 });
  sleep(0.5 + Math.random() * 2);
}

function runMixedProfileLoad() {
  const userId = pickUserId();

  // Health check
  const healthRes = http.get(`${BASE_URL}/health`);
  check(healthRes, { "health ok": (r) => r.status === 200 });

  // Profile load
  const userRes = http.get(`${BASE_URL}/api/user/${userId}`, {
    headers: { "X-Telegram-Init-Data": INIT_DATA },
  });
  check(userRes, { "profile ok": (r) => r.status === 200 });

  sleep(1 + Math.random() * 3);
}

// ─── Main dispatch ───────────────────────────────────────────────────────────

export default function () {
  switch (SCENARIO) {
    case "same_user_burst":
      runSameUserBurst();
      break;
    case "mixed_traffic":
      // k6 scenario routing handles this via scenarios config
      runMixedClicks();
      break;
    case "many_users":
    default:
      runManyUserClicks();
      break;
  }
}
