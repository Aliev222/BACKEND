# Load Testing Pack for SPIRIT Backend

**Purpose:** Validate production readiness under high traffic before launch

---

## Prerequisites

### Install k6
```bash
# macOS
brew install k6

# Windows
choco install k6

# Linux
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update
sudo apt-get install k6
```

### Setup Test Environment
```bash
# Set backend URL
export BASE_URL=https://staging.yourapp.com

# Or for local testing
export BASE_URL=http://localhost:8000
```

---

## Test Scenarios

### 1. Click Load Test (`k6_clicks.js`)

**Purpose:** Test click endpoint under sustained high load

**Load Profile:**
- Ramp up: 0 → 100 → 500 → 1000 users over 6 minutes
- Peak load: 1000 concurrent users for 10 minutes
- Ramp down: 1000 → 0 over 3 minutes

**What it tests:**
- Click endpoint throughput
- Redis hot state performance
- Worker flush lag under load
- Database write performance

**Run:**
```bash
k6 run load_tests/k6_clicks.js
```

**Expected Results:**
- p95 latency < 500ms
- p99 latency < 1000ms
- Error rate < 1%
- No worker lag > 60 seconds

---

### 2. Mixed Flow Test (`k6_mixed_flow.js`)

**Purpose:** Simulate realistic user behavior with mixed operations

**Load Profile:**
- Ramp up: 0 → 50 → 200 → 500 users over 7 minutes
- Peak load: 500 concurrent users for 5 minutes
- Ramp down: 500 → 0 over 3 minutes

**Traffic Mix:**
- 60% clicks
- 25% profile reads
- 10% boost activations
- 5% passive income claims

**What it tests:**
- Mixed workload performance
- Cache hit rates
- Boost activation concurrency
- Passive income flush

**Run:**
```bash
k6 run load_tests/k6_mixed_flow.js
```

**Expected Results:**
- p95 latency < 800ms
- p99 latency < 1500ms
- Error rate < 2%
- No boost activation failures

---

### 3. Spike Test (`k6_spike.js`)

**Purpose:** Test system resilience under sudden traffic surge

**Load Profile:**
- Baseline: 100 users for 1 minute
- SPIKE: 100 → 2000 users in 30 seconds
- Hold: 2000 users for 3 minutes
- Recovery: 2000 → 100 users in 1 minute
- Stabilize: 100 users for 2 minutes

**What it tests:**
- Redis connection pool under spike
- Database connection pool under spike
- Worker recovery after spike
- Rate limiting effectiveness

**Run:**
```bash
k6 run load_tests/k6_spike.js
```

**Expected Results:**
- p95 latency < 1000ms (during spike)
- p99 latency < 2000ms (during spike)
- Error rate < 5% (during spike)
- System recovers within 1 minute after spike

---

## Monitoring During Tests

### Key Metrics to Watch

**Application Metrics:**
- Request latency (p50, p95, p99)
- Error rate (4xx, 5xx)
- Throughput (requests/second)

**Redis Metrics:**
```bash
# Monitor Redis
redis-cli INFO stats | grep instantaneous
redis-cli INFO clients | grep connected_clients
redis-cli DBSIZE
```

**Database Metrics:**
```sql
-- Active connections
SELECT count(*) FROM pg_stat_activity;

-- Long-running queries
SELECT pid, now() - query_start as duration, query 
FROM pg_stat_activity 
WHERE state = 'active' 
ORDER BY duration DESC;

-- Lock waits
SELECT * FROM pg_locks WHERE NOT granted;
```

**Worker Metrics:**
```bash
# Check worker lag
redis-cli ZCARD coins_pending_queue
redis-cli ZCARD referral_pending_queue
redis-cli ZCARD tournament_key

# Check flush logs
psql -c "SELECT COUNT(*) FROM coins_flush_log WHERE flushed_at > NOW() - INTERVAL '5 minutes';"
```

---

## Interpreting Results

### Success Criteria

✅ **PASS** if all of:
- p95 latency meets thresholds
- Error rate < threshold
- No worker lag > 60 seconds
- No database deadlocks
- Redis pool not exhausted
- System recovers after spike

⚠️ **WARNING** if any of:
- p95 latency 10-20% above threshold
- Error rate 1-2% above threshold
- Worker lag 60-120 seconds
- Occasional connection pool warnings

❌ **FAIL** if any of:
- p95 latency > 2x threshold
- Error rate > 5%
- Worker lag > 300 seconds
- Database deadlocks
- Redis OOM
- System does not recover

---

## Troubleshooting

### High Latency
- Check database query times
- Check Redis latency
- Check worker flush lag
- Review slow query logs

### High Error Rate
- Check application logs for exceptions
- Check rate limiting (429 errors)
- Check database connection pool exhaustion
- Check Redis connection errors

### Worker Lag
- Check worker logs for errors
- Check database write performance
- Check Redis SCAN performance
- Verify queue producers are working

### Memory Issues
- Check Redis memory usage
- Check application memory usage
- Check for memory leaks in workers
- Review connection pool sizes

---

## Next Steps After Load Testing

### If Tests Pass
1. Document baseline metrics
2. Set up production monitoring alerts
3. Proceed to soft launch (50% traffic)
4. Monitor for 48 hours
5. Ramp to 100% traffic

### If Tests Fail
1. Identify bottleneck (DB, Redis, workers, app)
2. Apply targeted fixes
3. Re-run failed test
4. Verify fix effectiveness
5. Repeat until all tests pass

---

## Load Test Checklist

Before running load tests:
- [ ] Backend deployed to staging
- [ ] Database has production-like data volume
- [ ] Redis configured with production settings
- [ ] Workers running (coins_flush, referral_flush, tournament_flush)
- [ ] Monitoring dashboards ready
- [ ] Rollback plan prepared

During load tests:
- [ ] Monitor application metrics
- [ ] Monitor Redis metrics
- [ ] Monitor database metrics
- [ ] Monitor worker lag
- [ ] Check error logs in real-time

After load tests:
- [ ] Review all metrics
- [ ] Document any issues found
- [ ] Verify system recovery
- [ ] Update thresholds if needed
- [ ] Sign off on production readiness
