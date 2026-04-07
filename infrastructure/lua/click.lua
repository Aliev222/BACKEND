local user_rl_key = KEYS[1]
local ip_rl_key = KEYS[2]
local idem_key = KEYS[3]
local energy_key = KEYS[4]
local hot_key = KEYS[5]
local pending_key = KEYS[6]
local tournament_key = KEYS[7]
local click_buf_key = KEYS[8]
local referral_pending_key = KEYS[9]
local activity_key = KEYS[10]
local click_guard_key = KEYS[11]
local user_hot_key = KEYS[12]

local user_limit = tonumber(ARGV[1])
local ip_limit = tonumber(ARGV[2])
local window_seconds = tonumber(ARGV[3])
local idem_ttl = tonumber(ARGV[4])
local now_ts = tonumber(ARGV[5])
local now_iso = ARGV[6]
local requested_clicks = tonumber(ARGV[7])
local max_click_batch_size = tonumber(ARGV[8])
local max_real_cps = tonumber(ARGV[9])
local click_burst_allowance = tonumber(ARGV[10])
local click_time_cap = tonumber(ARGV[11])
local initial_click_allowance = tonumber(ARGV[12])
local suspicious_overshoot = tonumber(ARGV[13])
local suspicion_soft_limit = tonumber(ARGV[14])
local regen_seconds = tonumber(ARGV[15])
local user_id = ARGV[16]
local base_max_energy = tonumber(ARGV[17])
local ghost_boost_multiplier_default = tonumber(ARGV[18])
local initial_boosts_json = ARGV[19] or '{}'
local hot_state_version_default = 1

local function json_decode_or_empty(raw)
    if not raw or raw == '' then
        return {}
    end
    local ok, parsed = pcall(cjson.decode, raw)
    if ok and type(parsed) == 'table' then
        return parsed
    end
    return {}
end

-- user rate limit
local user_current = redis.call('INCR', user_rl_key)
if user_current == 1 then
    redis.call('EXPIRE', user_rl_key, window_seconds)
end
if user_current > user_limit then
    return {1, 0, 0, 0, 0, 0, 0, 0}
end

-- ip rate limit
local ip_current = redis.call('INCR', ip_rl_key)
if ip_current == 1 then
    redis.call('EXPIRE', ip_rl_key, window_seconds)
end
if ip_current > ip_limit then
    return {4, 0, 0, 0, 0, 0, 0, 0}
end

-- idempotency
local idempotent_ok = redis.call('SET', idem_key, '1', 'EX', idem_ttl, 'NX')
if not idempotent_ok then
    return {2, 0, 0, 0, 0, 0, 0, 0}
end

if redis.call('EXISTS', user_hot_key) == 0 then
    redis.call('HSET', user_hot_key,
        'coins', '0',
        'energy', tostring(base_max_energy),
        'last_energy_ts', tostring(now_ts),
        'tap_power', '1',
        'energy_regen', tostring(regen_seconds),
        'level', '0',
        'rebirth_count', '0',
        'max_energy', tostring(base_max_energy),
        'click_streak', '0',
        'suspicion_score', '0',
        'version', tostring(hot_state_version_default),
        'skin_multiplier', '1',
        'boosts', initial_boosts_json,
        'flags', '{}'
    )
end

local hot_coins = tonumber(redis.call('HGET', user_hot_key, 'coins') or '0')
local hot_energy = tonumber(redis.call('HGET', user_hot_key, 'energy') or tostring(base_max_energy))
local hot_last_energy_ts = tonumber(redis.call('HGET', user_hot_key, 'last_energy_ts') or tostring(now_ts))
local hot_tap_power = tonumber(redis.call('HGET', user_hot_key, 'tap_power') or '1')
local level = tonumber(redis.call('HGET', user_hot_key, 'level') or '0')
local rebirth_count = tonumber(redis.call('HGET', user_hot_key, 'rebirth_count') or '0')
local hot_click_streak = tonumber(redis.call('HGET', user_hot_key, 'click_streak') or '0')
local hot_suspicion_score = tonumber(redis.call('HGET', user_hot_key, 'suspicion_score') or '0')
local hot_version = tonumber(redis.call('HGET', user_hot_key, 'version') or tostring(hot_state_version_default))
local skin_multiplier = tonumber(redis.call('HGET', user_hot_key, 'skin_multiplier') or '1')
local boosts = json_decode_or_empty(redis.call('HGET', user_hot_key, 'boosts'))
local flags = json_decode_or_empty(redis.call('HGET', user_hot_key, 'flags'))
local click_guard = {}
if type(flags['click_guard']) == 'table' then
    click_guard = flags['click_guard']
end
local prev_suspicion_score = tonumber(click_guard['suspicion_score'] or '0')
if prev_suspicion_score <= 0 and hot_suspicion_score > 0 then
    prev_suspicion_score = hot_suspicion_score
end
if hot_version < 1 then
    hot_version = hot_state_version_default
end

local max_energy = math.min(1000, base_max_energy + (level * 5))
if max_energy <= 0 then
    max_energy = base_max_energy
end
local computed_regen_seconds = regen_seconds
if computed_regen_seconds <= 0 then
    computed_regen_seconds = 1
end
local rebirth_bonus_per_level = 1 + math.max(0, rebirth_count)
local tap_value = 1 + (level * rebirth_bonus_per_level)
if tap_value <= 0 then
    tap_value = hot_tap_power
end
local profit_per_hour = 100 + (level * 35) + (level * level * 7)

local mega_boost_active = boosts['mega_boost_active'] == true
local ghost_boost_active = boosts['ghost_boost_active'] == true
local daily_infinite_energy_active = boosts['daily_infinite_energy_active'] == true
local task_tap_boost_active = boosts['task_tap_boost_active'] == true
local task_tap_boost_multiplier = tonumber(boosts['task_tap_boost_multiplier'] or '1')
if task_tap_boost_multiplier < 1 then
    task_tap_boost_multiplier = 1
end
local ghost_boost_multiplier = tonumber(boosts['ghost_boost_multiplier'] or tostring(ghost_boost_multiplier_default))
if ghost_boost_multiplier < 1 then
    ghost_boost_multiplier = ghost_boost_multiplier_default
end

local free_energy_clicks = 0
if mega_boost_active or ghost_boost_active or daily_infinite_energy_active then
    free_energy_clicks = 1
end

local coin_per_tap = math.max(1, math.floor(tap_value * skin_multiplier))
if mega_boost_active then
    coin_per_tap = coin_per_tap * 2
end
if ghost_boost_active then
    coin_per_tap = coin_per_tap * ghost_boost_multiplier
end
if task_tap_boost_active then
    coin_per_tap = coin_per_tap * task_tap_boost_multiplier
end

if redis.call('EXISTS', energy_key) == 0 then
    redis.call('HSET', energy_key,
        'value', tostring(hot_energy),
        'updated_at', tostring(now_ts),
        'max_energy', tostring(max_energy),
        'click_updated_at', tostring(hot_last_energy_ts)
    )
end

local stored_value = tonumber(redis.call('HGET', energy_key, 'value') or tostring(hot_energy))
local stored_updated = tonumber(redis.call('HGET', energy_key, 'updated_at') or tostring(now_ts))
local stored_max = tonumber(redis.call('HGET', energy_key, 'max_energy') or tostring(max_energy))
local click_updated = tonumber(redis.call('HGET', energy_key, 'click_updated_at') or tostring(hot_last_energy_ts))

if stored_max ~= max_energy then
    stored_max = max_energy
    if stored_value > stored_max then
        stored_value = stored_max
    end
end

local elapsed = now_ts - stored_updated
if elapsed < 0 then
    elapsed = 0
end
local effective_regen_seconds = computed_regen_seconds
local regen = math.floor(elapsed / effective_regen_seconds)
local current_energy = stored_value
if regen > 0 then
    current_energy = math.min(stored_max, stored_value + regen)
end

local allowed_clicks = 0
if click_updated and click_updated > 0 then
    local elapsed_click = now_ts - click_updated
    if elapsed_click < 0 then
        elapsed_click = 0
    end
    elapsed_click = math.min(elapsed_click, click_time_cap)
    local allowed_by_time = math.floor(elapsed_click * max_real_cps) + click_burst_allowance
    allowed_clicks = math.max(1, math.min(allowed_by_time, max_click_batch_size))
    allowed_clicks = math.min(requested_clicks, allowed_clicks)
else
    allowed_clicks = math.min(requested_clicks, initial_click_allowance, max_click_batch_size)
end

if requested_clicks > (allowed_clicks + suspicious_overshoot)
   and requested_clicks > math.max(allowed_clicks * 2, click_burst_allowance * 2) then
    return {3, 0, current_energy, 0, 0, allowed_clicks, 0, prev_suspicion_score}
end

local effective_clicks = allowed_clicks
if free_energy_clicks ~= 1 then
    effective_clicks = math.min(allowed_clicks, current_energy)
end

local gained = effective_clicks * coin_per_tap
local new_energy = current_energy
if free_energy_clicks ~= 1 then
    new_energy = math.max(0, current_energy - effective_clicks)
end

if redis.call('EXISTS', hot_key) == 0 then
    redis.call('SET', hot_key, tostring(hot_coins))
end

local new_coins = redis.call('INCRBY', hot_key, gained)
redis.call('INCRBY', pending_key, gained)
redis.call('ZADD', 'coins_pending_queue', now_ts, user_id)
redis.call('HSET', energy_key,
    'value', tostring(new_energy),
    'updated_at', tostring(now_ts),
    'max_energy', tostring(max_energy),
    'click_updated_at', tostring(now_ts)
)

local referral_bonus = 0
if gained > 0 then
    redis.call('ZINCRBY', tournament_key, gained, user_id)
    redis.call('HINCRBY', click_buf_key, 'coins', gained)
    redis.call('HINCRBY', click_buf_key, 'clicks', effective_clicks)
    redis.call('EXPIRE', click_buf_key, 300)
    local referrer_id = tonumber(flags['referrer_id'] or '0')
    if referrer_id and referrer_id > 0 then
        referral_bonus = math.max(1, math.floor(gained * 0.05))
        if referral_pending_key and string.len(referral_pending_key) > 0 then
            redis.call('HINCRBY', referral_pending_key, 'coins', referral_bonus)
            redis.call('HINCRBY', referral_pending_key, 'clicks', 1)
            redis.call('EXPIRE', referral_pending_key, 300)
            redis.call('ZADD', 'referral_pending_queue', now_ts, tostring(referrer_id))
        end
    end
end

redis.call('SETEX', activity_key, 300, now_iso)

local new_suspicion_score = tonumber(prev_suspicion_score or '0')
local last_reason = cjson.null
if requested_clicks > allowed_clicks then
    new_suspicion_score = math.min(12, prev_suspicion_score + 1)
    last_reason = "requested_gt_allowed"
elseif prev_suspicion_score > 0 then
    new_suspicion_score = math.max(0, prev_suspicion_score - 1)
end

local guard_payload = {
    suspicion_score = new_suspicion_score,
    last_click_at = now_iso,
    last_requested_clicks = requested_clicks,
    last_allowed_clicks = allowed_clicks,
    last_effective_clicks = effective_clicks,
    updated_at = now_iso
}
if last_reason ~= cjson.null then
    guard_payload["last_reason"] = last_reason
end
if new_suspicion_score >= suspicion_soft_limit then
    guard_payload["flagged_at"] = now_iso
end
redis.call('SET', click_guard_key, cjson.encode(guard_payload), 'EX', 300)
flags['click_guard'] = guard_payload
local new_click_streak = hot_click_streak
if effective_clicks > 0 then
    new_click_streak = hot_click_streak + 1
else
    new_click_streak = 0
end

redis.call('HSET', user_hot_key,
    'coins', tostring(new_coins),
    'energy', tostring(new_energy),
    'last_energy_ts', tostring(now_ts),
    'tap_power', tostring(tap_value),
    'energy_regen', tostring(effective_regen_seconds),
    'max_energy', tostring(max_energy),
    'click_streak', tostring(new_click_streak),
    'suspicion_score', tostring(new_suspicion_score),
    'version', tostring(hot_version),
    'profit_per_hour', tostring(profit_per_hour),
    'boosts', cjson.encode(boosts),
    'flags', cjson.encode(flags)
)

return {
    0,
    new_coins,
    new_energy,
    effective_clicks,
    gained,
    allowed_clicks,
    referral_bonus,
    new_suspicion_score,
    tap_value,
    profit_per_hour,
    coin_per_tap,
    max_energy,
    mega_boost_active and 1 or 0,
    ghost_boost_active and 1 or 0,
    daily_infinite_energy_active and 1 or 0,
    task_tap_boost_active and 1 or 0,
    task_tap_boost_multiplier,
    ghost_boost_multiplier,
    effective_regen_seconds,
    new_click_streak,
    hot_version
}
