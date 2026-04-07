# Load Test Thresholds and Rollback Triggers

**Date:** 2026-04-07  
**Purpose:** Define success criteria and rollback triggers for production launch

---

## Performance Thresholds

### Click Endpoint (`/api/clicks`)

**Normal Load (100-500 users):**
- p50 latency: < 100ms
- p95 latency: < 500ms
- p99 latency: < 1000ms
- Error rate: < 1%
- Throughput: > 500 req/s

**High Load (500-1000 users):**
- p50 latency: < 150ms
- p95 latency: < 800ms
- p99 latency: < 1500ms
- Error rate: < 2%
- Throughput: > 800 req/s

**Spike Load (2000 users):**
- p50 latency: < 300ms
- p95 latency: < 1000ms
- p99 latency: < 2000ms
- Error rate: < 5%
- Throughput: > 1000 req/s

---

### Profile Endpoint (`/api/profile/{user_id}`)

**Normal Load:**
- p50 latency: < 50ms (cache hit)
- p95 latency: < 200ms
- p99 latency: < 500ms
- Error rate: < 0.5%
- Cache hit rate: > 90%

**High Load:**
- p50 latency: < 100ms
- p95 latency: < 300ms
- p99 latency: < 800ms
- Error rate: < 1%
- Cache hit rate: > 85%

---

### Boost Activation (`/api/activate-mega-boost`, `/api/activate-ghost-boost`)

**Normal Load:**
- p50 latency: < 200ms
- p95 latency: < 800ms
- p99 latency: < 1500ms
- Error rate: < 2% (excluding 429 cooldown)
- Success rate: > 95%

**High Load:**
- p50 latency: < 300ms
- p95 latency: < 1200ms
- p99 latency: < 2000ms
- Error rate: < 3%
- Success rate: > 93%

---

### Passive Income (`/api/passive-income`)

**Normal Load:**
- p50 latency: < 150ms
- p95 latency: < 600ms
- p99 latency: < 1200ms
- Error rate: < 1%

---

## Infrastructure Thresholds

### Redis

**Metrics:**
- Connected clients: < 500 (pool limit: 1000)
- Memory usage: < 80% of max
- Evicted keys: 0
- Keyspace hits ratio: > 90%
- Command latency p99: < 10ms

**Queue Depths:**
- `coins_pending_queue`: < 5000 keys
- `referral_pending_queue`: < 1000 keys
- `tournament_key`: < 10000 entries

**Thresholds:**
- ⚠️ WARNING: Queue depth > 3000 (coins), > 500 (referral)
- ❌ CRITICAL: Queue depth > 10000 (coins), > 2000 (referral)

---

### PostgreSQL

**Metrics:**
- Active connections: < 80 (pool limit: 100)
- Connection wait time: < 100ms
- Query p95 latency: < 200ms
- Query p99 latency: < 500ms
- Lock waits: 0
- Deadlocks: 0

**Thresholds:**
- ⚠️ WARNING: Active connections > 60
- ❌ CRITICAL: Active connections > 90
- ❌ CRITICAL: Any deadlocks
- ❌ CRITICAL: Lock wait > 5 seconds

---

### Workers

**coins_flush Worker:**
- Flush interval: 30 seconds
- Flush lag: < 60 seconds
- Flushed per cycle: > 0 (if queue not empty)
- Stuck keys: 0

**referral_flush Worker:**
- Flush interval: 30 seconds
- Flush lag: < 60 seconds
- Flushed per cycle: > 0 (if queue not empty)
- Stuck keys: 0

**tournament_flush Worker:**
- Flush interval: 60 seconds
- Flush lag: < 120 seconds
- Flushed per cycle: > 0 (if leaderboard not empty)

**Thresholds:**
- ⚠️ WARNING: Flush lag > 60 seconds
- ❌ CRITICAL: Flush lag > 300 seconds (5 minutes)
- ❌ CRITICAL: Stuck keys > 100
- ❌ CRITICAL: Worker not running

---

## Rollback Triggers

### IMMEDIATE ROLLBACK (Critical Issues)

**Trigger if ANY of:**
1. **Error rate > 10%** sustained for 5 minutes
2. **Database deadlocks** detected
3. **Redis OOM** (out of memory)
4. **Worker lag > 10 minutes** sustained
5. **p99 latency > 5 seconds** sustained for 5 minutes
6. **Data corruption** detected (reconciliation mismatches)
7. **Worker crash loop** (restarts > 5 in 10 minutes)
8. **Database connection pool exhausted** (all connections in use)

**Action:**
```bash
# Immediate rollback
git revert <commit-hash>
kubectl rollout undo deployment/backend
# Or
docker-compose down && docker-compose up -d --build <previous-tag>
```

---

### GRADUAL ROLLBACK (Warning Issues)

**Trigger if ANY of:**
1. **Error rate 3-10%** sustained for 10 minutes
2. **p95 latency > 2x threshold** sustained for 10 minutes
3. **Worker lag 2-10 minutes** sustained
4. **Redis connection pool > 80%** sustained
5. **Database connection pool > 80%** sustained
6. **Cache hit rate < 70%** sustained

**Action:**
1. Reduce traffic to 50% (feature flag)
2. Investigate root cause
3. If not resolved in 30 minutes → full rollback
4. If resolved → gradually ramp back to 100%

---

### MONITORING ALERTS (No Rollback Yet)

**Trigger if ANY of:**
1. **Error rate 1-3%** sustained for 5 minutes
2. **p95 latency > threshold** sustained for 5 minutes
3. **Worker lag 60-120 seconds**
4. **Queue depth > warning threshold**
5. **Redis memory > 70%**
6. **Database connections > 60**

**Action:**
1. Alert on-call engineer
2. Monitor closely
3. Prepare rollback plan
4. Investigate in parallel

---

## Success Criteria for Soft Launch

### Phase 1: 50% Traffic (48 hours)

**Must achieve ALL of:**
- ✅ Error rate < 1%
- ✅ p95 latency within thresholds
- ✅ Worker lag < 60 seconds
- ✅ No database deadlocks
- ✅ No Redis OOM
- ✅ No data corruption
- ✅ Cache hit rate > 85%

**If successful → proceed to Phase 2**

---

### Phase 2: 100% Traffic (1 week)

**Must achieve ALL of:**
- ✅ Error rate < 1%
- ✅ p95 latency within thresholds
- ✅ Worker lag < 60 seconds
- ✅ No critical incidents
- ✅ System stable for 7 days

**If successful → launch complete**

---

## Monitoring Dashboard Checklist

### Real-Time Metrics (1-minute resolution)

**Application:**
- [ ] Request rate (req/s)
- [ ] Error rate (%)
- [ ] Latency (p50, p95, p99)
- [ ] Active users

**Redis:**
- [ ] Connected clients
- [ ] Memory usage
- [ ] Queue depths
- [ ] Command latency

**Database:**
- [ ] Active connections
- [ ] Query latency
- [ ] Lock waits
- [ ] Deadlocks

**Workers:**
- [ ] Flush lag
- [ ] Flushed per cycle
- [ ] Stuck keys
- [ ] Worker status

---

## Load Test Sign-Off Checklist

Before production launch:
- [ ] All load tests passed
- [ ] Thresholds documented
- [ ] Rollback triggers defined
- [ ] Monitoring dashboards configured
- [ ] Alerts configured
- [ ] On-call rotation scheduled
- [ ] Rollback procedure tested
- [ ] Incident response plan ready

---

## Post-Launch Monitoring Schedule

**First 24 hours:**
- Monitor every 15 minutes
- On-call engineer available 24/7

**Days 2-7:**
- Monitor every hour
- On-call engineer available during business hours

**Week 2+:**
- Monitor daily
- Standard on-call rotation

---

## Incident Response Procedure

### If Rollback Trigger Activated:

1. **Alert** (< 1 minute)
   - Page on-call engineer
   - Notify team in Slack

2. **Assess** (< 5 minutes)
   - Check monitoring dashboards
   - Identify root cause
   - Determine severity

3. **Decide** (< 10 minutes)
   - Immediate rollback? (critical)
   - Gradual rollback? (warning)
   - Monitor and fix? (alert)

4. **Execute** (< 15 minutes)
   - Execute rollback if needed
   - Verify rollback success
   - Confirm system stability

5. **Post-Mortem** (< 24 hours)
   - Document incident
   - Identify root cause
   - Plan fix
   - Update thresholds if needed

---

## Contact Information

**On-Call Engineer:** [Your contact]  
**Backup Engineer:** [Backup contact]  
**Incident Channel:** #incidents-backend  
**Monitoring Dashboard:** [Dashboard URL]  
**Runbook:** [Runbook URL]
