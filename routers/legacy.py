from fastapi import APIRouter
router = APIRouter()

@router.post("/api/auth/session")
async def create_api_session(request: Request):
    ensure_mobile_only_game_access(request)
    telegram_user = verify_telegram_init_data(
        request.headers.get("X-Telegram-Init-Data", "")
    )
    token, expires_at = issue_session_token(telegram_user)
    return {
        "success": True,
        "token": token,
        "token_type": "Bearer",
        "expires_in": SESSION_TOKEN_TTL_SECONDS,
        "expires_at": expires_at,
        "user_id": int(telegram_user.get("id", 0)),
    }


# middleware removed
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    path = request.url.path
    method = request.method
    status_code = 500
    try:
        if path.startswith("/api/") and path not in {
            "/api/ads/monetag/postback",
            "/api/ads/adsgram/reward",
        }:
            request_ip = get_request_ip(request)
            ip_allowed = await redis_rate_limit(
                f"rl:global_api:ip:{request_ip}", 240, 60
            )
            if not ip_allowed:
                RATE_LIMIT_REJECTS.labels(namespace="global_api_ip").inc()
                status_code = 429
                return JSONResponse(
                    status_code=429,
                    content={"detail": "Too many requests from this IP"},
                )

        response = await call_next(request)
        status_code = response.status_code
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = (
            "camera=(), microphone=(), geolocation=()"
        )
        response.headers["Content-Security-Policy"] = (
            "default-src 'none'; "
            "frame-ancestors 'none'; "
            "base-uri 'none'; "
            "form-action 'none'; "
            "object-src 'none'"
        )
        return response
    finally:
        duration = time.perf_counter() - start
        HTTP_REQUESTS_TOTAL.labels(
            method=method, path=path, status=str(status_code)
        ).inc()
        HTTP_REQUEST_DURATION.labels(method=method, path=path).observe(duration)
        record_endpoint_diagnostic(method, path, status_code, duration)


@router.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ==================== РњРћР”Р•Р›Р ====================


# ==================== Р’РЎРџРћРњРћР“РђРўР•Р›Р¬РќР«Р• Р¤РЈРќРљР¦РР ====================
def normalize_diagnostics_path(path: str) -> str:
    normalized = str(path or "/")
    normalized = re.sub(r"/\d{4}-\d{2}-\d{2}(?=/|$)", "/{season_key}", normalized)
    normalized = re.sub(r"/-?\d+(?=/|$)", "/{id}", normalized)
    return normalized


def record_endpoint_diagnostic(
    method: str, path: str, status_code: int, duration_seconds: float
) -> None:
    if path in {"/metrics", "/health"} or path.startswith("/api/admin/diagnostics"):
        return

    normalized_path = normalize_diagnostics_path(path)
    key = (str(method or "GET").upper(), normalized_path)
    stats = ENDPOINT_DIAGNOSTICS.get(key)
    if stats is None:
        stats = {
            "method": key[0],
            "path": normalized_path,
            "requests": 0,
            "errors": 0,
            "status_counts": defaultdict(int),
            "durations_ms": deque(maxlen=DIAGNOSTICS_DURATION_WINDOW),
            "last_error": None,
            "last_error_at": None,
            "last_status": None,
            "last_duration_ms": None,
        }
        ENDPOINT_DIAGNOSTICS[key] = stats

    duration_ms = max(0.0, float(duration_seconds or 0.0) * 1000.0)
    stats["requests"] += 1
    stats["status_counts"][int(status_code or 0)] += 1
    stats["durations_ms"].append(duration_ms)
    stats["last_status"] = int(status_code or 0)
    stats["last_duration_ms"] = round(duration_ms, 2)

    if int(status_code or 0) >= 400:
        stats["errors"] += 1
        stats["last_error"] = f"HTTP {int(status_code or 0)}"
        stats["last_error_at"] = datetime.utcnow().isoformat()
        RECENT_DIAGNOSTIC_ERRORS.appendleft(
            {
                "method": stats["method"],
                "path": normalized_path,
                "status": int(status_code or 0),
                "at": stats["last_error_at"],
                "duration_ms": round(duration_ms, 2),
            }
        )


def percentile_from_sorted(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    rank = max(0, min(len(values) - 1, int(round((len(values) - 1) * percentile))))
    return float(values[rank])


def serialize_endpoint_diagnostic(stats: dict) -> dict:
    durations = sorted(float(item) for item in stats.get("durations_ms", []))
    status_counts = stats.get("status_counts", {}) or {}
    return {
        "method": stats.get("method"),
        "path": stats.get("path"),
        "requests": int(stats.get("requests", 0)),
        "errors": int(stats.get("errors", 0)),
        "status_2xx": int(
            sum(
                count for code, count in status_counts.items() if 200 <= int(code) < 300
            )
        ),
        "status_4xx": int(
            sum(
                count for code, count in status_counts.items() if 400 <= int(code) < 500
            )
        ),
        "status_429": int(status_counts.get(429, 0)),
        "status_5xx": int(
            sum(
                count for code, count in status_counts.items() if 500 <= int(code) < 600
            )
        ),
        "avg_ms": round(sum(durations) / len(durations), 2) if durations else 0.0,
        "p95_ms": round(percentile_from_sorted(durations, 0.95), 2)
        if durations
        else 0.0,
        "p99_ms": round(percentile_from_sorted(durations, 0.99), 2)
        if durations
        else 0.0,
        "last_status": stats.get("last_status"),
        "last_duration_ms": stats.get("last_duration_ms"),
        "last_error": stats.get("last_error"),
        "last_error_at": stats.get("last_error_at"),
    }


async def acquire_once_lock(key: str, ttl: float = 10) -> bool:
    conn = await get_redis_or_none()
    if conn:
        try:
            ttl_ms = max(1, int(float(ttl) * 1000))
            result = await conn.set(key, "1", px=ttl_ms, nx=True)
            return bool(result)
        except Exception as e:
            logger.warning(f"Redis acquire_once_lock failed, fallback to local: {e}")

    now = time.monotonic()
    expires_at = LOCAL_LOCKS.get(key)
    if expires_at and expires_at > now:
        return False

    LOCAL_LOCKS[key] = now + ttl
    return True


async def acquire_idempotency_key(key: str, ttl: int = 60) -> bool:
    conn = await get_redis_or_none()
    if conn:
        try:
            result = await conn.set(key, "1", ex=ttl, nx=True)
            return bool(result)
        except Exception as e:
            logger.warning(
                f"Redis acquire_idempotency_key failed, fallback to local: {e}"
            )

    now = time.monotonic()
    expires_at = LOCAL_IDEMPOTENCY_KEYS.get(key)
    if expires_at and expires_at > now:
        return False

    LOCAL_IDEMPOTENCY_KEYS[key] = now + ttl
    return True


async def require_user_action_lock(namespace: str, user_id: int, ttl: float = 5):
    lock_key = f"lock:{namespace}:{user_id}"
    locked = await acquire_once_lock(lock_key, ttl=ttl)
    if not locked:
        raise HTTPException(status_code=429, detail="Action already in progress")


async def ensure_redis_available() -> redis.Redis:
    """
    Try reconnect once and guarantee redis_client or raise 503.
    """
    return await redis_or_503()


# ==================== Р­РќР”РџРћРРќРўР« ====================


def _local_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    now = time.monotonic()
    bucket = LOCAL_RATE_LIMITS_STATE[key]

    while bucket and (now - bucket[0]) >= window_seconds:
        bucket.popleft()

    if len(bucket) >= limit:
        return False

    bucket.append(now)
    return True


async def redis_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    """
    True = РјРѕР¶РЅРѕ РїСЂРѕРїСѓСЃС‚РёС‚СЊ
    False = Р»РёРјРёС‚ РїСЂРµРІС‹С€РµРЅ
    """
    global redis_client

    conn = await get_redis_or_none()

    if conn is None:
        return _local_rate_limit(key, limit, window_seconds)

    try:
        current = await conn.incr(key)
        if current == 1:
            await conn.expire(key, window_seconds)
        return current <= limit
    except Exception as e:
        logger.warning(f"Redis rate_limit failed, fallback to local: {e}")
        REDIS_ERRORS.inc()
        redis_client = None
        return _local_rate_limit(key, limit, window_seconds)


async def require_redis_rate_limit(
    namespace: str, user_id: int, limit: int, window_seconds: int
):
    allowed = await redis_rate_limit(f"rl:{namespace}:{user_id}", limit, window_seconds)
    if not allowed:
        RATE_LIMIT_REJECTS.labels(namespace=namespace).inc()
        raise HTTPException(status_code=429, detail="Too many requests")


def get_request_ip(request: Request) -> str:
    forwarded_ip = (request.headers.get("cf-connecting-ip") or "").strip()
    if forwarded_ip:
        return forwarded_ip

    x_forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()

    return (request.client.host if request.client else "") or "unknown"


async def require_ip_rate_limit(
    namespace: str, request: Request, limit: int, window_seconds: int
):
    request_ip = get_request_ip(request)
    allowed = await redis_rate_limit(
        f"rl:{namespace}:ip:{request_ip}", limit, window_seconds
    )
    if not allowed:
        RATE_LIMIT_REJECTS.labels(namespace=f"{namespace}_ip").inc()
        raise HTTPException(status_code=429, detail="Too many requests from this IP")


async def require_dual_rate_limit(
    namespace: str,
    request: Request,
    user_id: int,
    user_limit: int,
    window_seconds: int,
    *,
    ip_limit: int | None = None,
):
    await require_redis_rate_limit(namespace, user_id, user_limit, window_seconds)
    await require_ip_rate_limit(
        namespace, request, ip_limit or user_limit, window_seconds
    )


async def flush_click_buffer_loop():
    while True:
        try:
            conn = await get_redis_or_none()
            if not conn:
                await asyncio.sleep(5)
                continue

            data = await conn.hgetall(CLICK_BUFFER_KEY)

            if not data:
                await asyncio.sleep(5)
                continue

            for user_id, coins in data.items():
                user_id = int(user_id)
                coins = int(coins)

                if coins <= 0:
                    continue

                user = await get_user(user_id)
                if not user:
                    continue

                new_coins = int(user.get("coins", 0)) + coins

                await update_user(user_id, {"coins": new_coins})

                await invalidate_user_cache(user_id)

            await conn.delete(CLICK_BUFFER_KEY)

            logger.info(f"Flushed {len(data)} users from Redis buffer")

        except Exception as e:
            logger.error(f"Flush error: {e}")

        await asyncio.sleep(5)


@router.get("/health")
async def health():
    details: dict[str, Any] = {}
    overall = "ok"

    # Redis check
    redis_status = "skipped"
    if REDIS_URL:
        try:
            conn = await get_redis_or_none()
            if conn:
                await asyncio.wait_for(conn.ping(), timeout=0.5)
                redis_status = "ok"
            else:
                redis_status = "unavailable"
        except Exception as e:
            redis_status = f"error: {e}"
            logger.warning(f"Health redis check failed: {e}")
            REDIS_ERRORS.inc()
    details["redis"] = redis_status

    # DB check
    db_status = "ok"
    try:
        async with AsyncSessionLocal() as session:
            await asyncio.wait_for(session.execute(select(1)), timeout=0.5)
    except Exception as e:
        db_status = f"error: {e}"
        logger.warning(f"Health db check failed: {e}")
        DB_ERRORS.inc()
    details["db"] = db_status

    if any(s != "ok" and not str(s).startswith("skipped") for s in details.values()):
        overall = "degraded"

    return {"status": overall, "details": details}


@router.get("/api/user/{user_id}")
async def get_user_data(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        await touch_user_activity(user_id, user)

        now = datetime.utcnow()
        current_energy = calculate_current_energy(user, now)
        max_energy = resolve_max_energy(user)

        if (
            int(user.get("max_energy", max_energy)) != max_energy
            or int(user.get("energy", current_energy)) > max_energy
        ):
            await update_user(
                user_id,
                {
                    "max_energy": max_energy,
                    "energy": min(current_energy, max_energy),
                },
            )
            await invalidate_user_cache(user_id)

        extra = parse_extra_data(user.get("extra_data"))

        owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        selected_skin = normalize_selected_skin(
            extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
        )
        ghost_boost_active, ghost_boost_expires_at = get_ghost_boost_status(user)
        daily_infinite_energy_active, daily_infinite_energy_expires_at = (
            is_daily_infinite_energy_active(user)
        )
        task_tap_boost_active, task_tap_boost_expires_at, task_tap_boost_multiplier = (
            get_active_video_task_boost(extra, "tap_boost")
        )
        (
            task_passive_boost_active,
            task_passive_boost_expires_at,
            task_passive_boost_multiplier,
        ) = get_active_video_task_boost(extra, "passive_boost")
        multitap_level = int(user.get("multitap_level", 0))
        profit_level = int(user.get("profit_level", 0))
        energy_level = int(user.get("energy_level", 0))
        profit_per_tap = get_tap_value(multitap_level)
        profit_per_hour = get_hour_value(profit_level)

        if owned_skins != extra.get("owned_skins") or selected_skin != extra.get(
            "selected_skin", DEFAULT_SKIN_ID
        ):
            extra["owned_skins"] = owned_skins
            extra["selected_skin"] = selected_skin
            await update_user(user_id, {"extra_data": extra})
            await invalidate_user_cache(user_id)

        ton_wallet = get_ton_wallet_from_user({"extra_data": extra})

        return {
            "user_id": user["user_id"],
            "username": user.get("username"),
            "coins": user.get("coins", 0),
            "energy": current_energy,
            "max_energy": max_energy,
            "profit_per_tap": profit_per_tap,
            "profit_per_hour": profit_per_hour,
            "multitap_level": multitap_level,
            "profit_level": profit_level,
            "energy_level": energy_level,
            "owned_skins": owned_skins,
            "selected_skin": selected_skin,
            "ads_watched": extra.get("ads_watched", 0),
            "ghost_boost_active": ghost_boost_active,
            "ghost_boost_expires_at": ghost_boost_expires_at,
            "task_tap_boost_active": task_tap_boost_active,
            "task_tap_boost_expires_at": task_tap_boost_expires_at,
            "task_tap_boost_multiplier": task_tap_boost_multiplier,
            "task_passive_boost_active": task_passive_boost_active,
            "task_passive_boost_expires_at": task_passive_boost_expires_at,
            "task_passive_boost_multiplier": task_passive_boost_multiplier,
            "daily_infinite_energy_active": daily_infinite_energy_active,
            "daily_infinite_energy_expires_at": daily_infinite_energy_expires_at,
            "skin_ad_progress": get_skin_ad_progress(extra),
            "skin_ad_last_watch": get_skin_ad_last_watch(extra),
            "ton_wallet": ton_wallet,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_user_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/ton/wallet/{user_id}")
async def get_ton_wallet_status(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {
            "success": True,
            "user_id": user_id,
            "wallet": get_ton_wallet_from_user(user),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_ton_wallet_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/ton/wallet/proof-payload/{user_id}")
async def get_ton_wallet_proof_payload(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        payload, expires_at = await issue_ton_proof_payload(user_id)
        return {
            "success": True,
            "user_id": user_id,
            "payload": payload,
            "expires_at": expires_at,
            "expires_in_seconds": max(0, expires_at - int(time.time())),
            "allowed_domains": sorted(ton_proof_allowed_domains(request)),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_ton_wallet_proof_payload: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/ton/wallet/connect")
async def connect_ton_wallet(payload: TonWalletConnectRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        wallet_address = (payload.wallet_address or "").strip()
        if not is_valid_ton_wallet_address(wallet_address):
            raise HTTPException(status_code=400, detail="Invalid TON wallet address")

        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        existing_wallet = (
            extra.get("ton_wallet") if isinstance(extra.get("ton_wallet"), dict) else {}
        )

        wallet_verified = False
        verification_error = None
        verified_at = None
        if payload.ton_proof:
            wallet_verified, verification_error = await verify_ton_wallet_proof(
                payload.user_id,
                wallet_address,
                payload.ton_proof,
                request,
                payload.wallet_public_key,
                payload.wallet_state_init,
            )
            if not wallet_verified:
                logger.warning(
                    "TON wallet proof verification failed for user %s: %s (domain=%s, address=%s)",
                    payload.user_id,
                    verification_error or "unknown error",
                    getattr(getattr(payload.ton_proof, "domain", None), "value", None),
                    wallet_address,
                )
        elif bool(existing_wallet.get("verified")) and ton_addresses_match(
            existing_wallet.get("address"), wallet_address
        ):
            wallet_verified = True
            verification_error = None
            verified_at = existing_wallet.get("verified_at")

        extra["ton_wallet"] = {
            "address": wallet_address,
            "provider": (payload.wallet_provider or "").strip(),
            "app_name": (payload.wallet_app_name or "").strip(),
            "network": (payload.wallet_network or "").strip(),
            "connected_at": datetime.utcnow().isoformat(),
            "verified": wallet_verified,
            "verified_at": verified_at
            or (datetime.utcnow().isoformat() if wallet_verified else None),
            "verification_error": verification_error or None,
        }
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "user_id": payload.user_id,
            "wallet": get_ton_wallet_from_user({"extra_data": extra}),
            "verification_error": verification_error,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in connect_ton_wallet: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/ton/wallet/disconnect")
async def disconnect_ton_wallet(payload: TonWalletDisconnectRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        extra.pop("ton_wallet", None)
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "user_id": payload.user_id,
            "wallet": get_ton_wallet_from_user({"extra_data": extra}),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in disconnect_ton_wallet: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/mega-boost-status/{user_id}")
async def get_mega_boost_status(user_id: int, request: Request):
    """Get mega boost status"""
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}
        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()

        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(
                    active_boosts["mega_boost"]["expires_at"]
                )
                if now > expires:
                    del active_boosts["mega_boost"]
                    extra["active_boosts"] = active_boosts
                    await update_user(user_id, {"extra_data": extra})
                    await invalidate_user_cache(user_id)
                    cooldown_until = parse_iso_datetime(
                        extra.get("mega_boost_cooldown_until")
                    )
                    if cooldown_until and cooldown_until > now:
                        return {
                            "active": False,
                            "cooldown_active": True,
                            "cooldown_until": cooldown_until.isoformat(),
                            "cooldown_remaining_seconds": int(
                                (cooldown_until - now).total_seconds()
                            ),
                        }
                    return {"active": False, "cooldown_active": False}
                else:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"],
                        "remaining_seconds": remaining,
                    }
            except:
                pass

        cooldown_until = parse_iso_datetime(extra.get("mega_boost_cooldown_until"))
        if cooldown_until and cooldown_until > now:
            return {
                "active": False,
                "cooldown_active": True,
                "cooldown_until": cooldown_until.isoformat(),
                "cooldown_remaining_seconds": int(
                    (cooldown_until - now).total_seconds()
                ),
            }
        if cooldown_until and cooldown_until <= now:
            extra.pop("mega_boost_cooldown_until", None)
            await update_user(user_id, {"extra_data": extra})
            await invalidate_user_cache(user_id)

        return {"active": False, "cooldown_active": False}
    except Exception as e:
        logger.error(f"Error in get_mega_boost_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/activate-mega-boost")
async def activate_mega_boost(payload: AdActionClaimRequest, request: Request):
    """Activate mega boost (x2 coins + infinite energy for 1 minute)"""
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "activate_mega_boost", request, payload.user_id, 10, 60, ip_limit=20
        )
        await consume_ad_action_session(
            payload.user_id, payload.ad_session_id, "mega_boost"
        )
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}

        active_boosts = extra.get("active_boosts", {})
        now = datetime.utcnow()

        # РџСЂРѕРІРµСЂСЏРµРј, РЅРµ Р°РєС‚РёРІРµРЅ Р»Рё СѓР¶Рµ Р±СѓСЃС‚
        if "mega_boost" in active_boosts:
            try:
                expires = datetime.fromisoformat(
                    active_boosts["mega_boost"]["expires_at"]
                )
                if now < expires:
                    remaining = int((expires - now).total_seconds())
                    return {
                        "success": False,
                        "message": f"Boost already active! {remaining // 60}:{remaining % 60:02d} remaining",
                        "already_active": True,
                        "expires_at": active_boosts["mega_boost"]["expires_at"],
                    }
            except:
                del active_boosts["mega_boost"]

        cooldown_until = parse_iso_datetime(extra.get("mega_boost_cooldown_until"))
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Mega boost cooldown {remaining // 60}:{remaining % 60:02d}",
            )
        if cooldown_until and now >= cooldown_until:
            extra.pop("mega_boost_cooldown_until", None)

        expires_at = (now + timedelta(minutes=MEGA_BOOST_MINUTES)).isoformat()
        cooldown_minutes = MEGA_BOOST_COOLDOWN_MAX_MINUTES
        cooldown_until_value = (now + timedelta(minutes=cooldown_minutes)).isoformat()
        active_boosts["mega_boost"] = {"active": True, "expires_at": expires_at}
        extra["mega_boost_cooldown_until"] = cooldown_until_value
        extra["active_boosts"] = active_boosts
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(
            payload.user_id, "boost", {"source_action": "mega_boost"}
        )

        return {
            "success": True,
            "message": "рџ”ҐвљЎ MEGA BOOST activated for 1 minute! x2 coins + infinite energy",
            "expires_at": expires_at,
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": cooldown_minutes,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_mega_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/ghost-boost-status/{user_id}")
async def get_ghost_boost_status_endpoint(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        active, expires_at = get_ghost_boost_status(user)
        if not active or not expires_at:
            return {"active": False}

        remaining = max(
            0,
            int(
                (datetime.fromisoformat(expires_at) - datetime.utcnow()).total_seconds()
            ),
        )
        return {
            "active": True,
            "expires_at": expires_at,
            "remaining_seconds": remaining,
            "multiplier": GHOST_BOOST_MULTIPLIER,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_ghost_boost_status_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/activate-ghost-boost")
async def activate_ghost_boost(payload: AdActionClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "activate_ghost_boost", request, payload.user_id, 10, 60, ip_limit=20
        )
        await consume_ad_action_session(
            payload.user_id, payload.ad_session_id, "ghost_boost"
        )
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))

        active_boosts = extra.get("active_boosts", {})
        if not isinstance(active_boosts, dict):
            active_boosts = {}

        now = datetime.utcnow()
        ghost_boost = active_boosts.get("ghost_boost")
        if ghost_boost and ghost_boost.get("expires_at"):
            try:
                expires = datetime.fromisoformat(ghost_boost["expires_at"])
                if now < expires:
                    remaining = max(0, int((expires - now).total_seconds()))
                    return {
                        "success": False,
                        "already_active": True,
                        "expires_at": ghost_boost["expires_at"],
                        "remaining_seconds": remaining,
                        "multiplier": GHOST_BOOST_MULTIPLIER,
                    }
            except Exception:
                pass

        expires_at = (now + timedelta(minutes=GHOST_BOOST_MINUTES)).isoformat()
        active_boosts["ghost_boost"] = {
            "active": True,
            "expires_at": expires_at,
            "multiplier": GHOST_BOOST_MULTIPLIER,
        }
        extra["active_boosts"] = active_boosts

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(
            payload.user_id, "ghost", {"source_action": "ghost_boost"}
        )

        return {
            "success": True,
            "expires_at": expires_at,
            "remaining_seconds": GHOST_BOOST_MINUTES * 60,
            "multiplier": GHOST_BOOST_MULTIPLIER,
            "message": "Ghost boost activated",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_ghost_boost: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/reward-video")
async def reward_video(payload: RewardVideoClaimRequest, request: Request):
    raise HTTPException(status_code=410, detail="Deprecated reward flow")


@router.post("/api/reward-video/start")
async def reward_video_start(payload: RewardVideoStartRequest, request: Request):
    raise HTTPException(status_code=410, detail="Deprecated reward flow")


@router.post("/api/ad-action/start")
async def ad_action_start(payload: AdActionStartRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "ad_action_start", request, payload.user_id, 20, 60, ip_limit=40
        )

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        ad_session_id = await create_ad_action_session(payload.user_id, payload.action)
        return {
            "success": True,
            "ad_session_id": ad_session_id,
            "action": payload.action,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in ad_action_start: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/ads/adsgram/complete")
async def adsgram_complete_locally(payload: AdActionClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "adsgram_complete", request, payload.user_id, 20, 60, ip_limit=40
        )
        await require_user_action_lock("adsgram_complete", payload.user_id, ttl=2)

        verified = await mark_ad_action_session_verified_for_user(
            payload.user_id,
            payload.ad_session_id,
            {"source": "adsgram_sdk", "confirmed_at": datetime.utcnow().isoformat()},
        )
        if not verified:
            raise HTTPException(
                status_code=400, detail="Ad completion was not confirmed yet"
            )

        return {"success": True, "verified": True}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in adsgram_complete_locally: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.api_route("/api/ads/monetag/postback", methods=["GET", "POST"])
async def monetag_postback(request: Request):
    try:
        params = {str(k): str(v) for k, v in request.query_params.items()}

        if request.method == "POST":
            content_type = (request.headers.get("content-type") or "").lower()
            if (
                "application/x-www-form-urlencoded" in content_type
                or "multipart/form-data" in content_type
            ):
                form = await request.form()
                for key, value in form.items():
                    params[str(key)] = str(value)
            elif "application/json" in content_type:
                payload = await request.json()
                if isinstance(payload, dict):
                    for key, value in payload.items():
                        params[str(key)] = str(value)

        if MONETAG_POSTBACK_SECRET:
            provided_secret = extract_first_value(params, MONETAG_POSTBACK_SECRET_KEYS)
            if provided_secret != MONETAG_POSTBACK_SECRET:
                logger.warning("Monetag postback rejected: invalid secret")
                raise HTTPException(status_code=403, detail="Invalid postback secret")

        status_hints = [
            params.get("status"),
            params.get("state"),
            params.get("event"),
            params.get("result"),
            params.get("rewarded"),
            params.get("completed"),
        ]
        negative_status = any(
            hint is not None
            and str(hint).strip().lower() in MONETAG_POSTBACK_NEGATIVE_VALUES
            for hint in status_hints
        )
        if negative_status:
            logger.info("Monetag postback ignored as incomplete: %s", params)
            return Response(content="IGNORED", media_type="text/plain", status_code=200)

        ad_session_id = extract_first_value(params, MONETAG_POSTBACK_ID_KEYS)
        if not ad_session_id:
            logger.debug("Monetag postback without session id ignored: %s", params)
            return Response(content="IGNORED", media_type="text/plain", status_code=200)

        verified = await mark_ad_action_session_verified(ad_session_id, params)
        if not verified:
            logger.warning(
                "Monetag postback could not find ad session %s", ad_session_id
            )
            return Response(
                content="SESSION_NOT_FOUND", media_type="text/plain", status_code=404
            )

        return Response(content="OK", media_type="text/plain", status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in monetag_postback: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.api_route("/api/ads/adsgram/reward", methods=["GET", "POST"])
async def adsgram_reward_callback(request: Request):
    try:
        params = {str(k): str(v) for k, v in request.query_params.items()}

        if request.method == "POST":
            content_type = (request.headers.get("content-type") or "").lower()
            if (
                "application/x-www-form-urlencoded" in content_type
                or "multipart/form-data" in content_type
            ):
                form = await request.form()
                for key, value in form.items():
                    params[str(key)] = str(value)
            elif "application/json" in content_type:
                payload = await request.json()
                if isinstance(payload, dict):
                    for key, value in payload.items():
                        params[str(key)] = str(value)

        if ADSGRAM_REWARD_ENFORCED:
            provided_secret = extract_first_value(params, ADSGRAM_REWARD_SECRET_KEYS)
            if provided_secret != ADSGRAM_REWARD_SECRET:
                logger.warning("AdsGram reward callback rejected: invalid secret")
                raise HTTPException(
                    status_code=403, detail="Invalid AdsGram reward secret"
                )

        ad_session_id = extract_first_value(params, ADSGRAM_REWARD_SESSION_KEYS)
        if ad_session_id:
            verified = await mark_ad_action_session_verified(ad_session_id, params)
            if not verified:
                logger.warning(
                    "AdsGram callback could not find ad session %s", ad_session_id
                )
                return Response(
                    content="SESSION_NOT_FOUND",
                    media_type="text/plain",
                    status_code=404,
                )
            return Response(content="OK", media_type="text/plain", status_code=200)

        user_id_raw = extract_first_value(params, ADSGRAM_REWARD_USER_KEYS)
        if not user_id_raw or not str(user_id_raw).strip().isdigit():
            logger.warning("AdsGram reward callback missing valid user id: %s", params)
            raise HTTPException(status_code=400, detail="Missing user id")

        user_id = int(str(user_id_raw).strip())
        matched_session_id = await mark_latest_ad_action_session_verified_for_user(
            user_id, params
        )
        if not matched_session_id:
            logger.warning(
                "AdsGram callback could not match an active ad session for user %s",
                user_id,
            )
            return Response(
                content="SESSION_NOT_FOUND", media_type="text/plain", status_code=404
            )

        return Response(content="OK", media_type="text/plain", status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in adsgram_reward_callback: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/ad-watched")
async def ad_watched(payload: dict, request: Request):
    """Track ad watch statistics"""
    try:
        user_id = payload.get("user_id")
        reward_type = payload.get("reward_type")
        await require_telegram_user(request, user_id)
        await require_user_action_lock("ad_watched", user_id, ttl=3)

        user = await get_user_cached(user_id)
        if not user:
            return {"success": False}

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}

        # РЎРѕС…СЂР°РЅСЏРµРј СЃС‚Р°С‚РёСЃС‚РёРєСѓ
        ads_history = extra.get("ads_history", [])
        ads_history.append(
            {"type": reward_type, "timestamp": datetime.utcnow().isoformat()}
        )
        extra["ads_history"] = ads_history[-50:]

        await update_user(user_id, {"extra_data": extra})
        await invalidate_user_cache(user_id)

        return {"success": True}

    except Exception as e:
        logger.error(f"Error in ad_watched: {e}")
        return {"success": False}


@router.post("/api/ads/increment")
async def increment_ads_watched(payload: AdActionClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "ads_increment", request, payload.user_id, 20, 60, ip_limit=40
        )
        await consume_ad_action_session(
            payload.user_id, payload.ad_session_id, "ads_increment"
        )
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        lock_key = f"lock:ads_increment:{payload.user_id}"
        locked = await acquire_once_lock(lock_key, ttl=5)
        if not locked:
            raise HTTPException(status_code=429, detail="Ad already being processed")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}

        ads_watched = int(extra.get("ads_watched", 0)) + 1
        extra["ads_watched"] = ads_watched

        skin_id = (
            LEGACY_SKIN_ID_MAP.get(payload.skin_id, payload.skin_id)
            if payload.skin_id
            else None
        )
        current_count = 0
        required_count = 0
        cooldown_remaining_seconds = 0
        ready_to_unlock = False

        if skin_id:
            if skin_id not in VIDEO_SKIN_IDS:
                raise HTTPException(status_code=400, detail="Unknown ad skin")

            progress = get_skin_ad_progress(extra)
            last_watch = get_skin_ad_last_watch(extra)
            required_count = int(SKIN_REQUIREMENTS.get(skin_id, {}).get("count", 1))
            current_count = int(progress.get(skin_id, 0) or 0)

            if current_count >= required_count:
                ready_to_unlock = True
            else:
                last_watch_at = parse_iso_datetime(last_watch.get(skin_id))
                now = datetime.utcnow()
                if last_watch_at:
                    next_allowed = last_watch_at + timedelta(
                        minutes=SKIN_AD_COOLDOWN_MINUTES
                    )
                    if next_allowed > now:
                        cooldown_remaining_seconds = int(
                            (next_allowed - now).total_seconds()
                        )
                        raise HTTPException(
                            status_code=429,
                            detail=f"Skin ad cooldown {cooldown_remaining_seconds // 60}:{cooldown_remaining_seconds % 60:02d}",
                        )

                current_count = min(required_count, current_count + 1)
                progress[skin_id] = current_count
                last_watch[skin_id] = now.isoformat()
                extra["skin_ad_progress"] = progress
                extra["skin_ad_last_watch"] = last_watch
                ready_to_unlock = current_count >= required_count

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(
            payload.user_id,
            "skins",
            {"source_action": "ads_increment", "skin_id": skin_id},
        )

        return {
            "success": True,
            "ads_watched": ads_watched,
            "skin_id": skin_id,
            "current_count": current_count,
            "required_count": required_count,
            "ready_to_unlock": ready_to_unlock,
            "cooldown_minutes": SKIN_AD_COOLDOWN_MINUTES,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in increment_ads_watched: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


def get_global_upgrade_level(user: dict) -> int:
    return max(
        int(user.get("multitap_level", 0)),
        int(user.get("profit_level", 0)),
        int(user.get("energy_level", 0)),
    )


async def apply_global_upgrade_for_user(user_id: int, user: dict) -> dict:
    current_level = get_global_upgrade_level(user)
    if current_level >= MAX_UPGRADE_LEVEL:
        raise HTTPException(status_code=400, detail="Max level reached")

    price = GLOBAL_UPGRADE_PRICES[current_level]
    current_coins = int(user.get("coins", 0))
    if current_coins < price:
        raise HTTPException(status_code=400, detail="Not enough coins")

    new_level = current_level + 1
    new_profit_per_tap = get_tap_value(new_level)
    new_profit_per_hour = get_hour_value(new_level)
    new_max_energy = get_max_energy(new_level)
    new_coins = current_coins - price

    ml = int(user.get("multitap_level", 0))
    pl = int(user.get("profit_level", 0))
    el = int(user.get("energy_level", 0))

    expected = {
        "coins": current_coins,
        "multitap_level": ml,
        "profit_level": pl,
        "energy_level": el,
    }
    updates = {
        "coins": new_coins,
        "multitap_level": new_level,
        "profit_level": new_level,
        "energy_level": new_level,
        "profit_per_tap": new_profit_per_tap,
        "profit_per_hour": new_profit_per_hour,
        "max_energy": new_max_energy,
        "energy": new_max_energy,
    }

    updated_user = await update_user_if_matches(user_id, expected, updates)
    if not updated_user:
        raise HTTPException(status_code=409, detail="Upgrade state changed, retry")
    await invalidate_user_cache(user_id)

    next_cost = (
        GLOBAL_UPGRADE_PRICES[new_level]
        if new_level < len(GLOBAL_UPGRADE_PRICES)
        else 0
    )
    return {
        "success": True,
        "coins": int(updated_user.get("coins", new_coins)),
        "new_level": new_level,
        "levels": {
            "multitap": new_level,
            "profit": new_level,
            "energy": new_level,
        },
        "prices": {
            "global": next_cost,
        },
        "next_cost": next_cost,
        "profit_per_tap": new_profit_per_tap,
        "profit_per_hour": new_profit_per_hour,
        "max_energy": new_max_energy,
        "energy": new_max_energy,
    }


@router.post("/api/upgrade")
async def process_upgrade(payload: UpgradeRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "upgrade", request, payload.user_id, 25, 60, ip_limit=50
        )
        await require_user_action_lock("upgrade", payload.user_id, ttl=0.35)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return await apply_global_upgrade_for_user(payload.user_id, user)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/upgrade-all")
async def process_upgrade_all(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "upgrade_all", request, payload.user_id, 25, 60, ip_limit=50
        )
        await require_user_action_lock("upgrade_all", payload.user_id, ttl=0.35)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return await apply_global_upgrade_for_user(payload.user_id, user)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_upgrade_all: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/update-energy")
async def update_energy(payload: AdActionClaimRequest, request: Request):
    try:
        user_id = payload.user_id
        await require_telegram_user(request, user_id)
        await require_dual_rate_limit(
            "update_energy", request, user_id, 10, 60, ip_limit=20
        )
        await consume_ad_action_session(
            user_id, payload.ad_session_id, "energy_refill_max"
        )
        await require_user_action_lock("update_energy", user_id, ttl=3)
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id required")
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()
        max_energy = resolve_max_energy(user)
        extra = parse_extra_data(user.get("extra_data"))
        cooldown_until = parse_iso_datetime(extra.get("energy_refill_cooldown_until"))
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Energy refill cooldown active. Try again in {format_duration(remaining)}",
            )
        if cooldown_until and now >= cooldown_until:
            extra.pop("energy_refill_cooldown_until", None)

        cooldown_until_value = (
            now + timedelta(minutes=ENERGY_REFILL_COOLDOWN_MINUTES)
        ).isoformat()
        extra["energy_refill_cooldown_until"] = cooldown_until_value

        await update_user(
            user_id,
            {
                "max_energy": max_energy,
                "energy": max_energy,
                "last_energy_update": now,
                "extra_data": extra,
            },
        )
        await invalidate_user_cache(user_id)
        await record_rewarded_ad_claim(
            user_id, "energy_restore", {"source_action": "energy_refill_max"}
        )

        return {
            "success": True,
            "energy": max_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": ENERGY_REFILL_COOLDOWN_MINUTES,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in update_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/autoclicker/activate")
async def activate_autoclicker(payload: AdActionClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "activate_autoclicker", request, payload.user_id, 10, 60, ip_limit=20
        )
        await consume_ad_action_session(
            payload.user_id, payload.ad_session_id, "autoclicker"
        )
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        now = datetime.utcnow()
        cooldown_until = parse_iso_datetime(extra.get("autoclicker_cooldown_until"))
        if cooldown_until and now < cooldown_until:
            remaining = int((cooldown_until - now).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Autoclicker cooldown {remaining // 60}:{remaining % 60:02d}",
            )
        if cooldown_until and now >= cooldown_until:
            extra.pop("autoclicker_cooldown_until", None)

        cooldown_until_value = (
            now + timedelta(minutes=AUTOCLICKER_COOLDOWN_MINUTES)
        ).isoformat()
        extra["autoclicker_cooldown_until"] = cooldown_until_value
        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)
        await record_rewarded_ad_claim(
            payload.user_id, "autoclicker", {"source_action": "autoclicker"}
        )
        return {
            "success": True,
            "duration_seconds": 60,
            "cooldown_until": cooldown_until_value,
            "cooldown_minutes": AUTOCLICKER_COOLDOWN_MINUTES,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in activate_autoclicker: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/recover-energy")
async def recover_energy_legacy(payload: UserIdRequest, request: Request):
    raise HTTPException(status_code=410, detail="Legacy endpoint disabled")


@router.post("/api/sync-energy")
async def sync_energy(payload: EnergySyncRequest, request: Request):
    """РЎРµСЂРІРµСЂРЅС‹Р№ sync СЌРЅРµСЂРіРёРё Р±РµР· СЃР±СЂРѕСЃР° С‚Р°Р№РјРµСЂР° СЂРµРіРµРЅР°."""
    try:
        await require_telegram_user(request, payload.user_id)
        user = await get_user_cached(payload.user_id)

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        now = datetime.utcnow()

        old_energy = int(user.get("energy", 0))
        max_energy = resolve_max_energy(user)
        last_update = normalize_dt(user.get("last_energy_update"))

        current_energy = calculate_current_energy(user, now)

        update_data = {}
        if int(user.get("max_energy", max_energy)) != max_energy:
            update_data["max_energy"] = max_energy

        # РћР±РЅРѕРІР»СЏРµРј baseline С‚РѕР»СЊРєРѕ РµСЃР»Рё СЌРЅРµСЂРіРёСЏ СЂРµР°Р»СЊРЅРѕ РІС‹СЂРѕСЃР»Р°
        if current_energy != old_energy:
            update_data["energy"] = current_energy

            if last_update:
                seconds_passed = max(0, int((now - last_update).total_seconds()))
                gained = seconds_passed // ENERGY_REGEN_SECONDS

                if gained > 0:
                    update_data["last_energy_update"] = last_update + timedelta(
                        seconds=gained * ENERGY_REGEN_SECONDS
                    )
            else:
                update_data["last_energy_update"] = now

        # Р•СЃР»Рё СЌРЅРµСЂРіРёСЏ СѓР¶Рµ РїРѕР»РЅР°СЏ, РґРµСЂР¶РёРј baseline РєРѕРЅСЃРёСЃС‚РµРЅС‚РЅС‹Рј
        if current_energy >= max_energy and not update_data.get("last_energy_update"):
            update_data["last_energy_update"] = now
            update_data["energy"] = max_energy

        if update_data:
            await update_user(payload.user_id, update_data)
            await invalidate_user_cache(payload.user_id)

        return {
            "success": True,
            "energy": current_energy,
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in sync_energy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


DEFAULT_SKIN_ID = "default.pngSP"

SKIN_MULTIPLIERS = {
    DEFAULT_SKIN_ID: 1.0,
    REFERRAL_SPECIAL_SKIN_ID: 1.8,
    DAILY_REWARD_SKIN_ID: 1.8,
    "10lvl.pngSP": 1.2,
    "25lvl.pngSP": 1.2,
    "50lvl.pngSP": 1.2,
    "75lvl.pngSP": 1.2,
    "100lvl.pngSP": 1.2,
    "video.pngSP": 1.5,
    "video2.pngSP": 1.5,
    "video3.pngSP": 1.5,
    "video4.pngSP": 1.5,
    "video5.pngSP": 1.5,
    "video6.pngSP": 1.5,
    "video7.pngSP": 1.5,
    "video8.pngSP": 1.5,
    "stars1.pngSP": 2.0,
    "stars2.pngSP": 2.0,
    "stars3.pngSP": 2.0,
    "stars4.pngSP": 2.0,
    "stars5.pngSP": 2.0,
    "stars6.pngSP": 2.0,
    "stars7.pngSP": 2.0,
    "stars8.pngSP": 2.0,
    "telega.pngSP": 1.5,
    "tiktok.pngSP": 1.5,
    "insta.pngSP": 1.5,
}

LEGACY_SKIN_ID_MAP = {
    "referral-special.pngSP": REFERRAL_SPECIAL_SKIN_ID,
    "daily30.pngSP": DAILY_REWARD_SKIN_ID,
    "telegram-social.pngSP": "telega.pngSP",
    "tiktok-social.pngSP": "tiktok.pngSP",
    "instagram-social.pngSP": "insta.pngSP",
}

SOCIAL_SUB_TASK_SKINS = {
    "telegram_sub": "telega.pngSP",
    "tiktok_sub": "tiktok.pngSP",
    "instagram_sub": "insta.pngSP",
}

VALID_SKIN_IDS = set(SKIN_MULTIPLIERS.keys())


def normalize_owned_skins(raw_owned) -> list[str]:
    if isinstance(raw_owned, list):
        owned = [str(item) for item in raw_owned]
    else:
        owned = []

    normalized = []
    seen = set()
    for skin_id in owned:
        skin_id = LEGACY_SKIN_ID_MAP.get(skin_id, skin_id)
        if skin_id in VALID_SKIN_IDS and skin_id not in seen:
            seen.add(skin_id)
            normalized.append(skin_id)

    if DEFAULT_SKIN_ID not in seen:
        normalized.insert(0, DEFAULT_SKIN_ID)

    return normalized


def normalize_selected_skin(selected_skin: str | None, owned_skins: list[str]) -> str:
    selected_skin = LEGACY_SKIN_ID_MAP.get(selected_skin, selected_skin)
    if selected_skin in owned_skins:
        return selected_skin
    return DEFAULT_SKIN_ID


def get_selected_skin_multiplier(user: dict) -> float:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
    selected_skin = normalize_selected_skin(
        extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
    )
    return SKIN_MULTIPLIERS.get(selected_skin, 1.0)


def is_mega_boost_active(user: dict) -> bool:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("mega_boost")
    if not boost:
        return False

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False

    try:
        expires_dt = datetime.fromisoformat(expires_at)
        return datetime.utcnow() < expires_dt
    except Exception:
        return False


def get_ghost_boost_status(user: dict) -> tuple[bool, str | None]:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("ghost_boost")
    if not boost:
        return False, None

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None

    try:
        expires_dt = datetime.fromisoformat(expires_at)
    except Exception:
        return False, None

    if datetime.utcnow() >= expires_dt:
        return False, None

    return True, expires_at


def get_daily_reward_progress(extra: dict) -> tuple[int, str | None]:
    claimed_days = int(extra.get("daily_reward_claimed_days", 0) or 0)
    claimed_days = max(0, min(DAILY_REWARD_MAX_DAYS, claimed_days))
    last_claim_date = extra.get("daily_reward_last_claim_date")
    if not isinstance(last_claim_date, str):
        last_claim_date = None
    return claimed_days, last_claim_date


def is_daily_infinite_energy_active(user: dict) -> tuple[bool, str | None]:
    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except Exception:
            extra = {}

    active_boosts = extra.get("active_boosts", {})
    boost = active_boosts.get("daily_infinite_energy")
    if not boost:
        return False, None

    expires_at = boost.get("expires_at")
    if not expires_at:
        return False, None

    try:
        expires_dt = datetime.fromisoformat(expires_at)
    except Exception:
        return False, None

    if datetime.utcnow() >= expires_dt:
        return False, None

    return True, expires_at


SKIN_REQUIREMENTS = {
    "10lvl.pngSP": {"type": "level", "value": 10},
    "25lvl.pngSP": {"type": "level", "value": 25},
    "50lvl.pngSP": {"type": "level", "value": 50},
    "75lvl.pngSP": {"type": "level", "value": 75},
    "100lvl.pngSP": {"type": "level", "value": 100},
    "video.pngSP": {"type": "ads", "count": 10},
    "video2.pngSP": {"type": "ads", "count": 10},
    "video3.pngSP": {"type": "ads", "count": 10},
    "video4.pngSP": {"type": "ads", "count": 10},
    "video5.pngSP": {"type": "ads", "count": 10},
    "video6.pngSP": {"type": "ads", "count": 10},
    "video7.pngSP": {"type": "ads", "count": 10},
    "video8.pngSP": {"type": "ads", "count": 10},
}


async def can_unlock_skin(user: dict, skin_id: str) -> bool:
    req = SKIN_REQUIREMENTS.get(skin_id)
    if not req:
        return False

    extra = user.get("extra_data", {}) or {}
    if isinstance(extra, str):
        try:
            extra = json.loads(extra)
        except:
            extra = {}

    if req["type"] == "level":
        display_level = int(user.get("multitap_level", 0)) + 1
        return display_level >= int(req["value"])

    if req["type"] == "ads":
        progress = get_skin_ad_progress(extra)
        current = int(progress.get(skin_id, 0) or 0)
        return current >= int(req["count"])

    if req["type"] == "friends":
        referral_count = int(user.get("referral_count", 0))
        return referral_count >= int(req["count"])

    return False


@router.post("/api/clicks")
async def process_clicks_batch(payload: ClicksBatchRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "clicks", request, payload.user_id, 90, 60, ip_limit=180
        )
        user = await get_user_cached(payload.user_id)

        if payload.clicks > MAX_CLICK_BATCH_SIZE:
            raise HTTPException(status_code=400, detail="Too many clicks in batch")

        batch_key = f"idem:clicks:{payload.user_id}:{payload.batch_id}"
        is_new_batch = await acquire_idempotency_key(batch_key, ttl=120)
        if not is_new_batch:
            logger.warning(
                f"Duplicate click batch rejected: user={payload.user_id}, batch_id={payload.batch_id}"
            )
            raise HTTPException(status_code=409, detail="Duplicate batch")

        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        await touch_user_activity(payload.user_id, user)

        now = datetime.utcnow()

        max_energy = resolve_max_energy(user)
        current_energy = calculate_current_energy(user, now)

        multitap_level = int(user.get("multitap_level", 0))
        tap_value = get_tap_value(multitap_level)

        extra = parse_extra_data(user.get("extra_data"))
        click_guard = get_click_guard_state(extra)
        last_click_at = parse_iso_datetime(click_guard.get("last_click_at"))

        owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        selected_skin = normalize_selected_skin(
            extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
        )
        skin_multiplier = float(SKIN_MULTIPLIERS.get(selected_skin, 1.0))

        mega_boost_active = is_mega_boost_active(user)
        ghost_boost_active, ghost_boost_expires_at = get_ghost_boost_status(user)
        task_tap_boost_active, _, task_tap_boost_multiplier = (
            get_active_video_task_boost(extra, "tap_boost")
        )
        daily_infinite_energy_active, _ = is_daily_infinite_energy_active(user)
        free_energy_clicks = (
            mega_boost_active or daily_infinite_energy_active or ghost_boost_active
        )

        coin_per_tap = max(1, int(tap_value * skin_multiplier))
        if mega_boost_active:
            coin_per_tap *= 2
        if ghost_boost_active:
            coin_per_tap *= GHOST_BOOST_MULTIPLIER
        if task_tap_boost_active:
            coin_per_tap *= max(1, task_tap_boost_multiplier)

        # Р·Р°С‰РёС‚Р°: режем накопление "законных" кликов и опираемся на server-side last_click_at.
        safe_requested_clicks = min(payload.clicks, MAX_CLICK_BATCH_SIZE)
        allowed_clicks = get_allowed_clicks(
            user,
            now,
            safe_requested_clicks,
            last_click_at=last_click_at,
        )

        severe_overshoot = (
            safe_requested_clicks > allowed_clicks + CLICK_SUSPICIOUS_OVERSHOOT
            and safe_requested_clicks
            > max(allowed_clicks * 2, CLICK_BURST_ALLOWANCE * 2)
        )
        if severe_overshoot:
            click_guard["hard_rejections"] = (
                int(click_guard.get("hard_rejections", 0)) + 1
            )
            click_guard["last_rejection_at"] = now.isoformat()
            click_guard["last_reason"] = (
                f"Click batch overshoot: requested={safe_requested_clicks}, allowed={allowed_clicks}"
            )
            write_click_guard_state(extra, click_guard)
            await update_user(payload.user_id, {"extra_data": extra})
            await invalidate_user_cache(payload.user_id)
            logger.warning(
                "Rejected suspicious click batch user=%s ip=%s requested=%s allowed=%s",
                payload.user_id,
                get_request_ip(request),
                safe_requested_clicks,
                allowed_clicks,
            )
            raise HTTPException(status_code=429, detail="Click rate too high")

        effective_clicks = (
            allowed_clicks
            if free_energy_clicks
            else min(allowed_clicks, current_energy)
        )
        gained = effective_clicks * coin_per_tap

        # РЅРѕРІС‹Рµ Р·РЅР°С‡РµРЅРёСЏ
        new_energy = (
            current_energy
            if free_energy_clicks
            else max(0, current_energy - effective_clicks)
        )
        new_coins = int(user.get("coins", 0)) + gained

        update_data = {
            "coins": new_coins,
            "max_energy": max_energy,
        }

        suspicion_score = int(click_guard.get("suspicion_score", 0))
        if safe_requested_clicks > allowed_clicks:
            suspicion_score += 1
            click_guard["last_reason"] = (
                f"Requested {safe_requested_clicks} clicks while server allowed {allowed_clicks}"
            )
        elif suspicion_score > 0:
            suspicion_score -= 1
            click_guard.pop("last_reason", None)

        click_guard["suspicion_score"] = min(12, max(0, suspicion_score))
        click_guard["last_click_at"] = now.isoformat()
        click_guard["last_requested_clicks"] = safe_requested_clicks
        click_guard["last_allowed_clicks"] = allowed_clicks
        click_guard["last_effective_clicks"] = effective_clicks
        click_guard["updated_at"] = now.isoformat()
        if click_guard["suspicion_score"] >= CLICK_SUSPICION_SOFT_LIMIT:
            click_guard["flagged_at"] = now.isoformat()

        write_click_guard_state(extra, click_guard)
        update_data["extra_data"] = extra

        if free_energy_clicks:
            stored_energy = int(user.get("energy", 0))
            last_update = normalize_dt(user.get("last_energy_update"))

            if stored_energy != current_energy:
                update_data["energy"] = current_energy

            if last_update:
                seconds_passed = max(0, int((now - last_update).total_seconds()))
                gained_energy = seconds_passed // ENERGY_REGEN_SECONDS
                if gained_energy > 0:
                    update_data["last_energy_update"] = last_update + timedelta(
                        seconds=gained_energy * ENERGY_REGEN_SECONDS
                    )
            elif "energy" in update_data:
                update_data["last_energy_update"] = now
        else:
            update_data["energy"] = new_energy
            update_data["last_energy_update"] = now

        # РЎРѕС…СЂР°РЅСЏРµРј СЌРЅРµСЂРіРёСЋ Рё Р±Р°Р»Р°РЅСЃ РѕРґРЅРёРј atomic update, С‡С‚РѕР±С‹ РіРѕРЅРєРё
        # РјРµР¶РґСѓ РєР»РёРєР°РјРё/РїР°СЃСЃРёРІРєРѕР№/СЌРЅРµСЂРіРёРµР№ РЅРµ РїРµСЂРµС‚РёСЂР°Р»Рё СЃРѕСЃС‚РѕСЏРЅРёРµ.
        updated_user = await update_user_if_matches(
            payload.user_id,
            {
                "coins": int(user.get("coins", 0)),
                "energy": int(user.get("energy", 0)),
                "last_energy_update": normalize_dt(user.get("last_energy_update")),
            },
            update_data,
        )
        if not updated_user:
            logger.warning("Atomic click update conflict for user=%s", payload.user_id)
            raise HTTPException(status_code=409, detail="Click state changed, retry")

        conn = await get_redis_or_none()
        if conn and gained > 0:
            # РўСѓСЂРЅРёСЂ РѕСЃС‚Р°РІР»СЏРµРј РІ Redis РєР°Рє Р±С‹СЃС‚СЂС‹Р№ leaderboard СЃР»РѕР№.
            await conn.zincrby(TOURNAMENT_KEY, gained, str(payload.user_id))
        if gained > 0:
            current_display_level = (
                max(
                    int(user.get("multitap_level", 0)),
                    int(user.get("profit_level", 0)),
                    int(user.get("energy_level", 0)),
                )
                + 1
            )
            await add_weekly_tournament_score(
                payload.user_id,
                user.get("username"),
                current_display_level,
                gained,
            )

        # вњ… РёРЅРІР°Р»РёРґРёСЂСѓРµРј РєСЌС€
        await invalidate_user_cache(payload.user_id)
        referral_bonus = await grant_referral_share_bonus(updated_user, gained)

        return {
            "success": True,
            "coins": int(updated_user.get("coins", new_coins)),
            "energy": int(updated_user.get("energy", new_energy)),
            "max_energy": max_energy,
            "regen_seconds": ENERGY_REGEN_SECONDS,
            "server_time": now.isoformat(),
            "gained": gained,
            "effective_clicks": effective_clicks,
            "coin_per_tap": coin_per_tap,
            "profit_per_tap": tap_value,
            "profit_per_hour": get_hour_value(int(user.get("profit_level", 0))),
            "mega_boost_active": mega_boost_active,
            "ghost_boost_active": ghost_boost_active,
            "ghost_boost_expires_at": ghost_boost_expires_at,
            "daily_infinite_energy_active": daily_infinite_energy_active,
            "click_guard_suspicion_score": click_guard["suspicion_score"],
            "referral_bonus_paid": referral_bonus,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_clicks_batch: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/upgrade-prices/{user_id}")
async def get_upgrade_prices(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        global_level = get_global_upgrade_level(user)
        global_price = (
            GLOBAL_UPGRADE_PRICES[global_level]
            if global_level < len(GLOBAL_UPGRADE_PRICES)
            else 0
        )

        return {
            "global": global_price,
            "multitap": global_price,
            "profit": global_price,
            "energy": global_price,
        }
    except Exception as e:
        logger.error(f"Error in get_upgrade_prices: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/register")
async def register_user(payload: RegisterRequest, request: Request):
    try:
        telegram_user = await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "register", request, payload.user_id, 10, 60, ip_limit=20
        )
        await require_user_action_lock("register", payload.user_id, ttl=5)
        existing = await get_user(payload.user_id)
        valid_referrer_id = None
        requested_referrer_id = int(payload.referrer_id or 0)
        if requested_referrer_id and requested_referrer_id != payload.user_id:
            referrer = await get_user_cached(requested_referrer_id)
            if referrer and int(referrer.get("referrer_id") or 0) != payload.user_id:
                valid_referrer_id = requested_referrer_id

        if existing:
            username = telegram_user.get("username") or payload.username
            updates = {}
            if username and username != existing.get("username"):
                updates["username"] = username

            # Allow a one-time referral attachment for a fresh account that was
            # created before the WebApp received the referral param.
            can_attach_referrer = (
                valid_referrer_id
                and not existing.get("referrer_id")
                and int(existing.get("coins", 0) or 0) == 0
                and int(existing.get("level", 0) or 0) == 0
                and int(existing.get("referral_count", 0) or 0) == 0
                and int(existing.get("referral_earnings", 0) or 0) == 0
            )
            if can_attach_referrer:
                updates["referrer_id"] = valid_referrer_id

            if updates:
                await update_user(payload.user_id, updates)
                await invalidate_user_cache(payload.user_id)
                if can_attach_referrer:
                    await add_referral_bonus(valid_referrer_id, payload.user_id)
                    await invalidate_user_cache(valid_referrer_id)

            refreshed = await get_user_cached(payload.user_id)
            return {"status": "exists", "user": refreshed or existing}

        await create_user(
            user_id=payload.user_id,
            username=telegram_user.get("username") or payload.username,
            referrer_id=valid_referrer_id,
        )

        created_user = await get_user_cached(payload.user_id)
        return {"status": "created", "user": created_user}
    except Exception as e:
        logger.error(f"Error in register_user: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== REFERRALS ====================


@router.get("/api/referral-data/{user_id}")
async def get_referral_data(user_id: int, request: Request):
    """Get referral statistics"""
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "count": user.get("referral_count", 0),
            "earnings": user.get("referral_earnings", 0),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_referral_data: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== TOURNAMENT ENDPOINTS ====================


@router.post("/api/online/heartbeat")
async def online_heartbeat(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        online_now = await touch_online_user(payload.user_id)
        return {"success": True, "online_now": online_now}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in online heartbeat: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/online/count")
async def get_online_count():
    try:
        online_now = await get_online_users_count()
        return {"success": True, "online_now": online_now}
    except Exception as e:
        logger.error(f"Error getting online count: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/weekly-tournament/overview/{user_id}")
async def get_weekly_tournament_overview(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        now = datetime.utcnow()
        starts_at, ends_at = get_weekly_tournament_season_window(now)
        season_key = get_weekly_tournament_season_key(now)
        season_rows = await list_weekly_tournament_seasons(limit=12)
        active_season = (
            next(
                (item for item in season_rows if item.get("season_key") == season_key),
                None,
            )
            or {}
        )
        player = await get_weekly_tournament_player_entry(user_id, season_key)
        pending_ton_notice = await get_pending_ton_wallet_notice(user_id)

        return {
            "success": True,
            "season_key": season_key,
            "season_status": active_season.get("status") or "active",
            "starts_at": starts_at.isoformat(),
            "ends_at": ends_at.isoformat(),
            "time_left_seconds": max(0, int((ends_at - now).total_seconds())),
            "payout_fund_cents": int(active_season.get("payout_fund_cents") or 0),
            "gross_ad_revenue_cents": int(
                active_season.get("gross_ad_revenue_cents") or 0
            ),
            "leagues": WEEKLY_LEAGUE_LEVEL_RANGES,
            "fund_splits": WEEKLY_LEAGUE_FUND_SPLITS,
            "top3_splits": WEEKLY_TOP3_PAYOUT_SPLITS,
            "rest_split": max(0.0, 1.0 - sum(WEEKLY_TOP3_PAYOUT_SPLITS.values())),
            "payout_splits": {
                "top": WEEKLY_TOP3_PAYOUT_SPLITS,
                "ranges": WEEKLY_RANGE_PAYOUT_SPLITS,
            },
            "player": player,
            "pending_ton_notice": pending_ton_notice,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/weekly-tournament/leaderboard/{league}")
async def get_weekly_tournament_leaderboard_endpoint(
    league: str, season_key: str | None = None, limit: int = 50
):
    try:
        league = (league or "").strip().lower()
        if league not in WEEKLY_LEAGUE_ORDER:
            raise HTTPException(status_code=400, detail="Unknown league")

        effective_season_key = season_key or get_weekly_tournament_season_key()
        rows = await get_weekly_tournament_leaderboard(
            season_key=effective_season_key,
            league=league,
            limit=limit,
        )
        return {
            "success": True,
            "season_key": effective_season_key,
            "league": league,
            "players": rows,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_leaderboard_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/weekly-tournament/player/{user_id}")
async def get_weekly_tournament_player_endpoint(
    user_id: int, request: Request, season_key: str | None = None
):
    try:
        await require_telegram_user(request, user_id)
        effective_season_key = season_key or get_weekly_tournament_season_key()
        player = await get_weekly_tournament_player_entry(user_id, effective_season_key)
        return {
            "success": True,
            "season_key": effective_season_key,
            "player": player,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_player_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/weekly-tournament/results/{league}")
async def get_weekly_tournament_results_endpoint(
    league: str, season_key: str | None = None, limit: int = 50
):
    try:
        league = (league or "").strip().lower()
        if league not in WEEKLY_LEAGUE_ORDER:
            raise HTTPException(status_code=400, detail="Unknown league")

        season_rows = await list_weekly_tournament_seasons(limit=52)
        if season_key:
            season = next(
                (
                    item
                    for item in season_rows
                    if item["season_key"] == season_key
                    and item["status"] == "finalized"
                ),
                None,
            )
        else:
            season = next(
                (item for item in season_rows if item["status"] == "finalized"), None
            )

        if not season:
            return {
                "success": True,
                "league": league,
                "season": None,
                "players": [],
            }

        winners = await get_weekly_tournament_winners(
            season["season_key"], league=league
        )
        async with AsyncSessionLocal() as session:
            payouts_result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season["season_key"],
                    WeeklyTournamentTonPayout.league == league,
                )
            )
            payout_rows = {
                int(row.user_id): row for row in payouts_result.scalars().all()
            }

        enriched_winners = []
        for winner in winners:
            payout_row = payout_rows.get(int(winner["user_id"]))
            winner_payload = dict(winner)
            winner_payload["ton_amount_nano"] = int(
                getattr(payout_row, "ton_amount_nano", 0) or 0
            )
            winner_payload["ton_payout_status"] = getattr(payout_row, "status", None)
            winner_payload["ton_wallet_address"] = getattr(
                payout_row, "wallet_address", None
            )
            enriched_winners.append(winner_payload)
        return {
            "success": True,
            "league": league,
            "season": season,
            "players": enriched_winners[: max(1, min(50, int(limit or 50)))],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_weekly_tournament_results_endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/weekly-tournament/seasons")
async def admin_weekly_tournament_seasons(request: Request, limit: int = 12):
    try:
        await require_admin_access(request)
        seasons = await list_weekly_tournament_seasons(limit=limit)
        return {
            "success": True,
            "seasons": seasons,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_weekly_tournament_seasons: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/overview")
async def admin_overview(request: Request):
    try:
        await require_admin_access(request)
        now = datetime.utcnow()
        starts_at, ends_at = get_weekly_tournament_season_window(now)
        season_key = get_weekly_tournament_season_key(now)
        online_now = await get_online_users_count()
        season_rows = await list_weekly_tournament_seasons(limit=12)
        active_season = next(
            (item for item in season_rows if item["season_key"] == season_key), None
        )

        async with AsyncSessionLocal() as session:
            total_users_result = await session.execute(select(func.count(User.id)))
            total_users = int(total_users_result.scalar() or 0)

            league_counts_result = await session.execute(
                select(
                    WeeklyTournamentEntry.league, func.count(WeeklyTournamentEntry.id)
                )
                .where(WeeklyTournamentEntry.season_key == season_key)
                .group_by(WeeklyTournamentEntry.league)
            )
            league_counts = {league: 0 for league in WEEKLY_LEAGUE_ORDER}
            for league, count in league_counts_result.all():
                if league in league_counts:
                    league_counts[league] = int(count or 0)

        top_preview = {}
        for league in WEEKLY_LEAGUE_ORDER:
            players = await get_weekly_tournament_leaderboard(
                season_key=season_key, league=league, limit=3
            )
            top_preview[league] = players

        return {
            "success": True,
            "generated_at": now.isoformat(),
            "online_now": online_now,
            "total_users": total_users,
            "season_key": season_key,
            "starts_at": starts_at.isoformat(),
            "ends_at": ends_at.isoformat(),
            "time_left_seconds": max(0, int((ends_at - now).total_seconds())),
            "active_season": active_season,
            "league_counts": league_counts,
            "top_preview": top_preview,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/diagnostics/endpoints")
async def admin_endpoint_diagnostics(
    request: Request, limit: int = 20, sort: str = "requests"
):
    try:
        await require_admin_access(request)
        max_items = max(1, min(100, int(limit or 20)))
        sort_key = (sort or "requests").strip().lower()
        supported_sort = {
            "requests",
            "errors",
            "p95_ms",
            "avg_ms",
            "status_429",
            "status_5xx",
        }
        if sort_key not in supported_sort:
            sort_key = "requests"

        rows = [
            serialize_endpoint_diagnostic(stats)
            for stats in ENDPOINT_DIAGNOSTICS.values()
        ]
        rows.sort(key=lambda item: float(item.get(sort_key, 0) or 0), reverse=True)
        top_rows = rows[:max_items]

        summary = {
            "tracked_endpoints": len(rows),
            "total_requests": sum(int(item.get("requests", 0)) for item in rows),
            "total_errors": sum(int(item.get("errors", 0)) for item in rows),
            "total_429": sum(int(item.get("status_429", 0)) for item in rows),
            "total_5xx": sum(int(item.get("status_5xx", 0)) for item in rows),
            "window_size": DIAGNOSTICS_DURATION_WINDOW,
        }

        return {
            "success": True,
            "sort": sort_key,
            "limit": max_items,
            "summary": summary,
            "endpoints": top_rows,
            "recent_errors": list(RECENT_DIAGNOSTIC_ERRORS)[:20],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_endpoint_diagnostics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/rewarded-ads/summary")
async def admin_rewarded_ads_summary(request: Request, hours: int = 24):
    try:
        await require_admin_access(request)
        summary = await get_rewarded_ads_admin_summary(hours=hours)
        tracked_actions = (
            "boost",
            "autoclicker",
            "tasks",
            "ghost",
            "energy_restore",
            "skins",
        )
        actions = {
            action: {
                "total": int(summary["actions_total"].get(action, 0)),
                "recent": int(summary["actions_recent"].get(action, 0)),
            }
            for action in tracked_actions
        }
        return {
            "success": True,
            "hours_window": summary["hours_window"],
            "total_claims": summary["total_claims"],
            "recent_claims": summary["recent_claims"],
            "actions": actions,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_rewarded_ads_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/stars-skins/summary")
async def admin_stars_skins_summary(request: Request, limit: int = 20):
    try:
        await require_admin_access(request)
        summary = await get_stars_skin_sales_admin_summary(limit=limit)
        return {"success": True, **summary}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_stars_skins_summary: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/fraud/overview")
async def admin_fraud_overview(request: Request, season_key: str | None = None):
    try:
        await require_admin_access(request)
        effective_season_key = season_key or get_weekly_tournament_season_key()
        players = await build_admin_fraud_overview(effective_season_key)
        return {
            "success": True,
            "season_key": effective_season_key,
            "players": players,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_fraud_overview: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/players/search")
async def admin_players_search(
    request: Request,
    query: str = "",
    season_key: str | None = None,
    limit: int = 20,
):
    try:
        await require_admin_access(request)
        effective_season_key = (
            season_key or get_weekly_tournament_season_key() or ""
        ).strip()
        search_query = (query or "").strip()
        normalized_query = search_query.lstrip("@").strip()
        search_limit = max(1, min(50, int(limit or 20)))

        async with AsyncSessionLocal() as session:
            stmt = select(User)
            if normalized_query:
                if normalized_query.isdigit():
                    stmt = stmt.where(User.user_id == int(normalized_query))
                else:
                    lowered = normalized_query.lower()
                    matching_user_ids: set[int] = set()

                    users_match_result = await session.execute(
                        select(User.user_id)
                        .where(
                            func.lower(func.coalesce(User.username, "")).like(
                                f"%{lowered}%"
                            )
                        )
                        .limit(search_limit * 3)
                    )
                    matching_user_ids.update(
                        int(row[0])
                        for row in users_match_result.all()
                        if row and row[0] is not None
                    )

                    entry_match_result = await session.execute(
                        select(WeeklyTournamentEntry.user_id)
                        .where(
                            func.lower(
                                func.coalesce(WeeklyTournamentEntry.username, "")
                            ).like(f"%{lowered}%")
                        )
                        .limit(search_limit * 3)
                    )
                    matching_user_ids.update(
                        int(row[0])
                        for row in entry_match_result.all()
                        if row and row[0] is not None
                    )

                    winner_match_result = await session.execute(
                        select(WeeklyTournamentWinner.user_id)
                        .where(
                            func.lower(
                                func.coalesce(WeeklyTournamentWinner.username, "")
                            ).like(f"%{lowered}%")
                        )
                        .limit(search_limit * 3)
                    )
                    matching_user_ids.update(
                        int(row[0])
                        for row in winner_match_result.all()
                        if row and row[0] is not None
                    )

                    payout_match_result = await session.execute(
                        select(WeeklyTournamentTonPayout.user_id)
                        .where(
                            func.lower(
                                func.coalesce(WeeklyTournamentTonPayout.username, "")
                            ).like(f"%{lowered}%")
                        )
                        .limit(search_limit * 3)
                    )
                    matching_user_ids.update(
                        int(row[0])
                        for row in payout_match_result.all()
                        if row and row[0] is not None
                    )

                    if matching_user_ids:
                        stmt = stmt.where(User.user_id.in_(sorted(matching_user_ids)))
                    else:
                        stmt = stmt.where(
                            or_(
                                func.lower(func.coalesce(User.username, "")).like(
                                    f"%{lowered}%"
                                ),
                                User.user_id == -1,
                            )
                        )

            stmt = stmt.order_by(User.created_at.desc()).limit(search_limit)
            users = (await session.execute(stmt)).scalars().all()
            user_ids = [int(user.user_id) for user in users]

            entry_map: dict[int, WeeklyTournamentEntry] = {}
            if user_ids and effective_season_key:
                entries_result = await session.execute(
                    select(WeeklyTournamentEntry).where(
                        WeeklyTournamentEntry.season_key == effective_season_key,
                        WeeklyTournamentEntry.user_id.in_(user_ids),
                    )
                )
                entry_map = {
                    int(row.user_id): row for row in entries_result.scalars().all()
                }

        reviews_map = await get_admin_fraud_reviews(user_ids) if user_ids else {}

        players = []
        for user in users:
            extra = parse_extra_data_object(user.extra_data)
            wallet = get_ton_wallet_from_user({"extra_data": extra})
            owned_skins = normalize_owned_skins(
                extra.get("owned_skins", [DEFAULT_SKIN_ID])
            )
            selected_skin = normalize_selected_skin(
                extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
            )
            entry = entry_map.get(int(user.user_id))
            review = reviews_map.get(int(user.user_id), {}) or {}

            players.append(
                {
                    "user_id": int(user.user_id),
                    "username": user.username,
                    "created_at": user.created_at.isoformat()
                    if user.created_at
                    else None,
                    "coins": int(user.coins or 0),
                    "energy": int(user.energy or 0),
                    "max_energy": int(user.max_energy or 0),
                    "level": int(user.level or 0),
                    "referral_count": int(user.referral_count or 0),
                    "owned_skins_count": int(len(owned_skins)),
                    "selected_skin": selected_skin,
                    "wallet_connected": bool(wallet.get("connected")),
                    "wallet_verified": bool(wallet.get("verified")),
                    "wallet_masked": wallet.get("masked_address"),
                    "season_key": effective_season_key,
                    "season_entry": {
                        "league": entry.league,
                        "score": int(entry.score or 0),
                        "display_level": int(entry.display_level or 1),
                        "eligible_for_payout": bool(entry.eligible_for_payout),
                        "fraud_flag": bool(entry.fraud_flag),
                    }
                    if entry
                    else None,
                    "fraud_review": {
                        "status": review.get("status") or "ok",
                        "reason": review.get("reason"),
                        "disqualify_from_payout": bool(
                            review.get("disqualify_from_payout")
                        ),
                    },
                }
            )

        return {
            "success": True,
            "query": normalized_query,
            "season_key": effective_season_key,
            "players": players,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_players_search: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/players/{user_id}")
async def admin_player_detail(
    user_id: int, request: Request, season_key: str | None = None
):
    try:
        await require_admin_access(request)
        effective_season_key = (
            season_key or get_weekly_tournament_season_key() or ""
        ).strip()

        async with AsyncSessionLocal() as session:
            user_result = await session.execute(
                select(User).where(User.user_id == user_id)
            )
            user = user_result.scalar_one_or_none()
            if not user:
                raise HTTPException(status_code=404, detail="Player not found")

            extra = parse_extra_data_object(user.extra_data)
            wallet = get_ton_wallet_from_user({"extra_data": extra})
            owned_skins = normalize_owned_skins(
                extra.get("owned_skins", [DEFAULT_SKIN_ID])
            )
            selected_skin = normalize_selected_skin(
                extra.get("selected_skin", DEFAULT_SKIN_ID), owned_skins
            )

            selected_entry = None
            selected_winner = None
            selected_payout = None
            selected_rank = None

            if effective_season_key:
                selected_entry_result = await session.execute(
                    select(WeeklyTournamentEntry).where(
                        WeeklyTournamentEntry.season_key == effective_season_key,
                        WeeklyTournamentEntry.user_id == user_id,
                    )
                )
                selected_entry = selected_entry_result.scalar_one_or_none()

                selected_winner_result = await session.execute(
                    select(WeeklyTournamentWinner).where(
                        WeeklyTournamentWinner.season_key == effective_season_key,
                        WeeklyTournamentWinner.user_id == user_id,
                    )
                )
                selected_winner = selected_winner_result.scalar_one_or_none()

                selected_payout_result = await session.execute(
                    select(WeeklyTournamentTonPayout).where(
                        WeeklyTournamentTonPayout.season_key == effective_season_key,
                        WeeklyTournamentTonPayout.user_id == user_id,
                    )
                )
                selected_payout = selected_payout_result.scalar_one_or_none()

                if selected_entry:
                    rank_result = await session.execute(
                        select(func.count(WeeklyTournamentEntry.id)).where(
                            WeeklyTournamentEntry.season_key == effective_season_key,
                            WeeklyTournamentEntry.league == selected_entry.league,
                            WeeklyTournamentEntry.score > selected_entry.score,
                        )
                    )
                    selected_rank = int(rank_result.scalar() or 0) + 1

            reward_rows_result = await session.execute(
                select(RewardedAdClaim)
                .where(RewardedAdClaim.user_id == user_id)
                .order_by(RewardedAdClaim.created_at.desc())
                .limit(20)
            )
            reward_rows = reward_rows_result.scalars().all()

            reward_summary_result = await session.execute(
                select(RewardedAdClaim.action, func.count(RewardedAdClaim.id))
                .where(RewardedAdClaim.user_id == user_id)
                .group_by(RewardedAdClaim.action)
            )
            reward_summary = {
                str(action): int(total or 0)
                for action, total in reward_summary_result.all()
                if action
            }

            skin_purchases_result = await session.execute(
                select(StarsSkinPurchase)
                .where(StarsSkinPurchase.user_id == user_id)
                .order_by(StarsSkinPurchase.created_at.desc())
                .limit(20)
            )
            skin_purchases = skin_purchases_result.scalars().all()

            payout_rows_result = await session.execute(
                select(WeeklyTournamentTonPayout)
                .where(WeeklyTournamentTonPayout.user_id == user_id)
                .order_by(
                    WeeklyTournamentTonPayout.updated_at.desc(),
                    WeeklyTournamentTonPayout.created_at.desc(),
                )
                .limit(20)
            )
            payout_rows = payout_rows_result.scalars().all()

            task_rows_result = await session.execute(
                select(UserTask)
                .where(UserTask.user_id == user_id)
                .order_by(UserTask.completed_at.desc())
                .limit(20)
            )
            task_rows = task_rows_result.scalars().all()

            recent_entries_result = await session.execute(
                select(WeeklyTournamentEntry)
                .where(WeeklyTournamentEntry.user_id == user_id)
                .order_by(WeeklyTournamentEntry.season_key.desc())
                .limit(8)
            )
            recent_entries = recent_entries_result.scalars().all()

            recent_winners_result = await session.execute(
                select(WeeklyTournamentWinner)
                .where(WeeklyTournamentWinner.user_id == user_id)
                .order_by(WeeklyTournamentWinner.season_key.desc())
                .limit(8)
            )
            recent_winners = recent_winners_result.scalars().all()

        referrals = (await get_referrals_list(user_id))[:20]
        referral_stats = await get_referral_stats(user_id)
        review = (await get_admin_fraud_reviews([user_id])).get(user_id, {}) or {}

        return {
            "success": True,
            "season_key": effective_season_key,
            "player": {
                "user_id": int(user.user_id),
                "username": user.username,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "profile": {
                    "coins": int(user.coins or 0),
                    "profit_per_hour": int(user.profit_per_hour or 0),
                    "profit_per_tap": int(user.profit_per_tap or 0),
                    "energy": int(user.energy or 0),
                    "max_energy": int(user.max_energy or 0),
                    "level": int(user.level or 0),
                    "referrer_id": int(user.referrer_id) if user.referrer_id else None,
                },
                "upgrades": {
                    "multitap_level": int(user.multitap_level or 0),
                    "profit_level": int(user.profit_level or 0),
                    "energy_level": int(user.energy_level or 0),
                    "boost_level": int(user.boost_level or 0),
                    "luck_level": int(user.luck_level or 0),
                },
                "skins": {
                    "selected_skin": selected_skin,
                    "owned_skins": owned_skins,
                    "owned_count": int(len(owned_skins)),
                },
                "wallet": wallet,
                "referrals": {
                    "count": int(referral_stats.get("count", 0) or 0),
                    "earnings": int(referral_stats.get("earnings", 0) or 0),
                    "recent": referrals,
                },
                "selected_season": {
                    "season_key": effective_season_key,
                    "entry": {
                        "league": selected_entry.league,
                        "score": int(selected_entry.score or 0),
                        "display_level": int(selected_entry.display_level or 1),
                        "eligible_for_payout": bool(selected_entry.eligible_for_payout),
                        "fraud_flag": bool(selected_entry.fraud_flag),
                        "rank_estimate": selected_rank,
                    }
                    if selected_entry
                    else None,
                    "winner": {
                        "league": selected_winner.league,
                        "rank": int(selected_winner.rank or 0),
                        "score": int(selected_winner.score or 0),
                        "payout_cents": int(selected_winner.payout_cents or 0),
                        "stars_reward": int(selected_winner.stars_reward or 0),
                        "eligible_for_payout": bool(
                            selected_winner.eligible_for_payout
                        ),
                        "fraud_flag": bool(selected_winner.fraud_flag),
                    }
                    if selected_winner
                    else None,
                    "ton_payout": {
                        "status": selected_payout.status,
                        "payout_cents": int(selected_payout.payout_cents or 0),
                        "ton_amount_nano": int(selected_payout.ton_amount_nano or 0),
                        "wallet_address": selected_payout.wallet_address,
                        "tx_hash": selected_payout.tx_hash,
                        "note": selected_payout.note,
                        "updated_at": selected_payout.updated_at.isoformat()
                        if selected_payout.updated_at
                        else None,
                    }
                    if selected_payout
                    else None,
                },
                "recent_tournament_entries": [
                    {
                        "season_key": row.season_key,
                        "league": row.league,
                        "score": int(row.score or 0),
                        "display_level": int(row.display_level or 1),
                        "eligible_for_payout": bool(row.eligible_for_payout),
                        "fraud_flag": bool(row.fraud_flag),
                        "updated_at": row.updated_at.isoformat()
                        if row.updated_at
                        else None,
                    }
                    for row in recent_entries
                ],
                "recent_tournament_wins": [
                    {
                        "season_key": row.season_key,
                        "league": row.league,
                        "rank": int(row.rank or 0),
                        "score": int(row.score or 0),
                        "payout_cents": int(row.payout_cents or 0),
                        "stars_reward": int(row.stars_reward or 0),
                        "eligible_for_payout": bool(row.eligible_for_payout),
                        "fraud_flag": bool(row.fraud_flag),
                        "created_at": row.created_at.isoformat()
                        if row.created_at
                        else None,
                    }
                    for row in recent_winners
                ],
                "reward_ads": {
                    "summary_by_action": reward_summary,
                    "recent": [
                        {
                            "action": row.action,
                            "created_at": row.created_at.isoformat()
                            if row.created_at
                            else None,
                            "metadata": parse_json_object(row.metadata_json),
                        }
                        for row in reward_rows
                    ],
                },
                "completed_tasks": [
                    {
                        "task_id": row.task_id,
                        "completed_at": row.completed_at.isoformat()
                        if row.completed_at
                        else None,
                    }
                    for row in task_rows
                ],
                "stars_skin_purchases": [
                    {
                        "skin_id": row.skin_id,
                        "stars_amount": int(row.stars_amount or 0),
                        "currency": row.currency,
                        "telegram_charge_id": row.telegram_charge_id,
                        "created_at": row.created_at.isoformat()
                        if row.created_at
                        else None,
                    }
                    for row in skin_purchases
                ],
                "payout_history": [
                    {
                        "season_key": row.season_key,
                        "league": row.league,
                        "rank": int(row.rank or 0),
                        "status": row.status,
                        "wallet_address": row.wallet_address,
                        "payout_cents": int(row.payout_cents or 0),
                        "ton_amount_nano": int(row.ton_amount_nano or 0),
                        "tx_hash": row.tx_hash,
                        "note": row.note,
                        "updated_at": row.updated_at.isoformat()
                        if row.updated_at
                        else None,
                    }
                    for row in payout_rows
                ],
                "fraud_review": {
                    "status": review.get("status") or "ok",
                    "reason": review.get("reason"),
                    "disqualify_from_payout": bool(
                        review.get("disqualify_from_payout")
                    ),
                    "updated_at": review.get("updated_at"),
                },
                "support": {
                    "reward_failures_available": False,
                    "duplicate_reward_attempts_available": False,
                    "moderation_timeline_available": False,
                },
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_player_detail: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/fraud/user/{user_id}")
async def admin_update_fraud_status(
    user_id: int, payload: AdminFraudUpdateRequest, request: Request
):
    try:
        await require_admin_access(request)
        status = (payload.status or "").strip().lower()
        if status not in {"fraud", "ok"}:
            raise HTTPException(status_code=400, detail="status must be fraud or ok")

        effective_season_key = payload.season_key or get_weekly_tournament_season_key()
        disqualify = bool(payload.disqualify_from_payout)
        await upsert_admin_fraud_review(user_id, status, payload.reason, disqualify)

        async with AsyncSessionLocal() as session:
            entry_result = await session.execute(
                select(WeeklyTournamentEntry).where(
                    WeeklyTournamentEntry.season_key == effective_season_key,
                    WeeklyTournamentEntry.user_id == user_id,
                )
            )
            entry = entry_result.scalar_one_or_none()
            if entry:
                entry.fraud_flag = status == "fraud"
                entry.eligible_for_payout = not disqualify

            winner_result = await session.execute(
                select(WeeklyTournamentWinner).where(
                    WeeklyTournamentWinner.season_key == effective_season_key,
                    WeeklyTournamentWinner.user_id == user_id,
                )
            )
            winner = winner_result.scalar_one_or_none()
            if winner:
                winner.fraud_flag = status == "fraud"
                winner.eligible_for_payout = not disqualify
                if disqualify:
                    winner.payout_cents = 0
                    winner.stars_reward = 0

            await session.commit()

        return {
            "success": True,
            "user_id": user_id,
            "season_key": effective_season_key,
            "status": status,
            "disqualify_from_payout": disqualify,
            "reason": payload.reason,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_update_fraud_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/weekly-tournament/season/{season_key}")
async def admin_weekly_tournament_season_detail(season_key: str, request: Request):
    try:
        await require_admin_access(request)
        season_rows = await list_weekly_tournament_seasons(limit=52)
        season = next(
            (item for item in season_rows if item["season_key"] == season_key), None
        )
        winners = await get_weekly_tournament_winners(season_key)

        leagues = {}
        for league in WEEKLY_LEAGUE_ORDER:
            leagues[league] = {
                "range": WEEKLY_LEAGUE_LEVEL_RANGES[league],
                "fund_split": WEEKLY_LEAGUE_FUND_SPLITS[league],
                "top50": await get_weekly_tournament_leaderboard(
                    season_key=season_key, league=league, limit=50
                ),
                "winners": [winner for winner in winners if winner["league"] == league],
            }

        return {
            "success": True,
            "season": season,
            "season_key": season_key,
            "leagues": leagues,
            "top3_splits": WEEKLY_TOP3_PAYOUT_SPLITS,
            "rest_split": max(0.0, 1.0 - sum(WEEKLY_TOP3_PAYOUT_SPLITS.values())),
            "payout_splits": {
                "top": WEEKLY_TOP3_PAYOUT_SPLITS,
                "ranges": WEEKLY_RANGE_PAYOUT_SPLITS,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_weekly_tournament_season_detail: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


async def fetch_ton_transactions_for_accounts(
    accounts: list[str], start_utime: int
) -> list[dict]:
    if not accounts:
        return []

    params: list[tuple[str, str]] = [
        ("limit", "500"),
        ("sort", "desc"),
        ("start_utime", str(max(0, int(start_utime or 0)))),
    ]
    for account in accounts:
        params.append(("account", account))

    headers = {}
    if TON_VERIFIER_API_KEY:
        headers["X-API-Key"] = TON_VERIFIER_API_KEY

    async with httpx.AsyncClient(timeout=TON_VERIFIER_TIMEOUT_SECONDS) as client:
        response = await client.get(
            f"{TON_VERIFIER_API_BASE}/transactions", params=params, headers=headers
        )
        response.raise_for_status()
        payload = response.json()

    if isinstance(payload, dict):
        if isinstance(payload.get("transactions"), list):
            return payload["transactions"]
        if isinstance(payload.get("result"), list):
            return payload["result"]
    return []


def match_ton_queue_rows_to_transactions(
    rows: list[WeeklyTournamentTonPayout],
    transactions: list[dict],
    sender_wallet_address: str,
) -> tuple[list[dict], set[int]]:
    matched_rows: list[dict] = []
    matched_user_ids: set[int] = set()
    sender_variants = ton_wallet_normalized_variants(sender_wallet_address)

    for row in rows:
        recipient_variants = ton_wallet_normalized_variants(row.wallet_address)
        if not recipient_variants:
            continue

        for tx in transactions:
            tx_account = tx.get("account")
            if tx_account and not ton_wallets_equal(tx_account, row.wallet_address):
                continue

            in_msg = tx.get("in_msg") or {}
            source = in_msg.get("source") or ""
            destination = in_msg.get("destination") or tx_account or ""
            value = int(in_msg.get("value") or 0)
            tx_hash = tx.get("hash") or in_msg.get("hash") or tx.get("trace_id") or ""
            tx_now = int(tx.get("now") or 0)
            aborted = bool((tx.get("description") or {}).get("aborted"))

            if aborted:
                continue
            if sender_variants and not ton_wallet_normalized_variants(
                source
            ).intersection(sender_variants):
                continue
            if destination and not ton_wallets_equal(destination, row.wallet_address):
                continue
            if value != int(row.ton_amount_nano or 0):
                continue

            matched_rows.append(
                {
                    "user_id": int(row.user_id),
                    "tx_hash": tx_hash,
                    "confirmed_at": tx_now,
                }
            )
            matched_user_ids.add(int(row.user_id))
            break

    return matched_rows, matched_user_ids


async def build_weekly_ton_payout_candidates(
    season_key: str, ton_price_usd: float
) -> tuple[dict | None, list[dict], dict]:
    season_rows = await list_weekly_tournament_seasons(limit=52)
    season = next(
        (item for item in season_rows if item["season_key"] == season_key), None
    )
    if not season:
        return None, [], {"with_wallet": 0, "without_wallet": 0, "without_payout": 0}

    total_fund_cents = max(0, int(season.get("payout_fund_cents") or 0))
    if total_fund_cents <= 0:
        return season, [], {"with_wallet": 0, "without_wallet": 0, "without_payout": 0}

    entries_by_league: dict[str, list[dict]] = {}
    for league in WEEKLY_LEAGUE_ORDER:
        entries_by_league[league] = await get_weekly_tournament_leaderboard(
            season_key=season_key, league=league, limit=50
        )

    user_ids = [
        int(entry["user_id"])
        for league_entries in entries_by_league.values()
        for entry in league_entries
    ]
    wallet_map: dict[int, dict] = {}

    if user_ids:
        async with AsyncSessionLocal() as session:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            user_rows = users_result.scalars().all()
            for row in user_rows:
                extra_data = {}
                if row.extra_data:
                    try:
                        extra_data = json.loads(row.extra_data)
                    except json.JSONDecodeError:
                        extra_data = {}
                wallet = get_ton_wallet_from_user({"extra_data": extra_data})
                if wallet["connected"] and wallet["verified"] and wallet["address"]:
                    wallet_map[int(row.user_id)] = wallet

    payouts: list[dict] = []
    stats = {"with_wallet": 0, "without_wallet": 0, "without_payout": 0}
    ton_price_usd_micros = int(round(float(ton_price_usd) * 1_000_000))

    for league in WEEKLY_LEAGUE_ORDER:
        entries = entries_by_league.get(league) or []
        league_fund_cents = int(
            total_fund_cents * WEEKLY_LEAGUE_FUND_SPLITS.get(league, 0)
        )

        top_payouts = {
            rank: int(league_fund_cents * share)
            for rank, share in WEEKLY_TOP3_PAYOUT_SPLITS.items()
        }
        range_payouts = []
        for payout_range in WEEKLY_RANGE_PAYOUT_SPLITS:
            start_rank = int(payout_range["start"])
            end_rank = int(payout_range["end"])
            pool_cents = int(league_fund_cents * float(payout_range["share"]))
            eligible_entries = [
                entry
                for entry in entries
                if start_rank <= int(entry["rank"]) <= end_rank
                and bool(entry.get("eligible_for_payout", True))
                and not bool(entry.get("fraud_flag", False))
            ]
            share_cents = 0
            remainder_cents = 0
            if eligible_entries:
                share_cents = pool_cents // len(eligible_entries)
                remainder_cents = pool_cents % len(eligible_entries)
            range_payouts.append(
                {
                    "start": start_rank,
                    "end": end_rank,
                    "share_cents": share_cents,
                    "remainder_cents": remainder_cents,
                }
            )

        for entry in entries:
            rank = int(entry["rank"])
            payout_cents = 0
            if bool(entry.get("eligible_for_payout", True)) and not bool(
                entry.get("fraud_flag", False)
            ):
                if rank in top_payouts:
                    payout_cents = top_payouts[rank]
                else:
                    for payout_range in range_payouts:
                        if payout_range["start"] <= rank <= payout_range["end"]:
                            payout_cents = payout_range["share_cents"]
                            if payout_range["remainder_cents"] > 0:
                                payout_cents += 1
                                payout_range["remainder_cents"] -= 1
                            break

            if payout_cents <= 0:
                stats["without_payout"] += 1
                continue

            wallet = wallet_map.get(int(entry["user_id"]))
            if not wallet:
                stats["without_wallet"] += 1
                continue

            ton_amount_nano = max(
                0, int(round(((payout_cents / 100.0) / ton_price_usd) * TON_NANO))
            )
            if ton_amount_nano <= 0:
                stats["without_payout"] += 1
                continue

            stats["with_wallet"] += 1
            payouts.append(
                {
                    "user_id": int(entry["user_id"]),
                    "username": entry.get("username"),
                    "league": league,
                    "rank": rank,
                    "wallet_address": wallet["address"],
                    "masked_wallet": mask_ton_wallet(wallet["address"]),
                    "payout_cents": int(payout_cents),
                    "ton_amount_nano": int(ton_amount_nano),
                    "ton_price_usd": ton_price_usd_micros / 1_000_000,
                    "status": "preview"
                    if season.get("status") != "finalized"
                    else "queued",
                    "tx_hash": None,
                    "note": "preview queue"
                    if season.get("status") != "finalized"
                    else None,
                }
            )

    payouts.sort(
        key=lambda row: (WEEKLY_LEAGUE_ORDER.index(row["league"]), int(row["rank"]))
    )
    return season, payouts, stats


async def build_weekly_ton_payout_view(
    season_key: str, ton_price_usd: float, league: str | None = None
) -> tuple[dict | None, dict[str, list[dict]], dict]:
    season_rows = await list_weekly_tournament_seasons(limit=52)
    season = next(
        (item for item in season_rows if item["season_key"] == season_key), None
    )
    if not season:
        return None, {}, {}

    selected_leagues = [league] if league else list(WEEKLY_LEAGUE_ORDER)
    total_fund_cents = max(0, int(season.get("payout_fund_cents") or 0))
    entries_by_league: dict[str, list[dict]] = {}
    for league_key in selected_leagues:
        entries_by_league[league_key] = await get_weekly_tournament_leaderboard(
            season_key=season_key, league=league_key, limit=50
        )

    user_ids = [
        int(entry["user_id"])
        for league_entries in entries_by_league.values()
        for entry in league_entries
    ]
    wallet_map: dict[int, dict] = {}
    wallet_reminders_map: dict[int, dict] = {}
    existing_payouts_map: dict[int, WeeklyTournamentTonPayout] = {}

    async with AsyncSessionLocal() as session:
        if user_ids:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            user_rows = users_result.scalars().all()
            for row in user_rows:
                extra_data = {}
                if row.extra_data:
                    try:
                        extra_data = json.loads(row.extra_data)
                    except json.JSONDecodeError:
                        extra_data = {}
                wallet = get_ton_wallet_from_user({"extra_data": extra_data})
                if wallet["connected"] and wallet["verified"] and wallet["address"]:
                    wallet_map[int(row.user_id)] = wallet
                reminders_by_season = extra_data.get("ton_wallet_reminders") or {}
                if isinstance(reminders_by_season, dict):
                    reminder = reminders_by_season.get(season_key) or {}
                    if isinstance(reminder, dict) and reminder.get("sent_at"):
                        wallet_reminders_map[int(row.user_id)] = reminder

        if user_ids:
            payouts_result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key,
                    WeeklyTournamentTonPayout.user_id.in_(user_ids),
                )
            )
            existing_payouts_map = {
                int(row.user_id): row for row in payouts_result.scalars().all()
            }

    winner_map_by_league: dict[str, dict[int, dict]] = {}
    if season.get("status") == "finalized":
        winners = await get_weekly_tournament_winners(season_key)
        for league_key in selected_leagues:
            winner_map_by_league[league_key] = {
                int(winner["user_id"]): winner
                for winner in winners
                if winner["league"] == league_key
            }

    leagues_payload: dict[str, list[dict]] = {}
    summary = {
        "with_wallet": 0,
        "without_wallet": 0,
        "eligible": 0,
        "without_payout": 0,
        "wallet_reminder_sent": 0,
    }

    for league_key in selected_leagues:
        entries = entries_by_league.get(league_key, [])
        league_fund_cents = int(
            total_fund_cents * WEEKLY_LEAGUE_FUND_SPLITS.get(league_key, 0)
        )
        top_payouts = {
            rank: int(league_fund_cents * share)
            for rank, share in WEEKLY_TOP3_PAYOUT_SPLITS.items()
        }
        range_payouts = []
        for payout_range in WEEKLY_RANGE_PAYOUT_SPLITS:
            start_rank = int(payout_range["start"])
            end_rank = int(payout_range["end"])
            pool_cents = int(league_fund_cents * float(payout_range["share"]))
            eligible_entries = [
                entry
                for entry in entries
                if start_rank <= int(entry["rank"]) <= end_rank
                and bool(entry.get("eligible_for_payout", True))
                and not bool(entry.get("fraud_flag", False))
            ]
            share_cents = 0
            remainder_cents = 0
            if eligible_entries:
                share_cents = pool_cents // len(eligible_entries)
                remainder_cents = pool_cents % len(eligible_entries)
            range_payouts.append(
                {
                    "start": start_rank,
                    "end": end_rank,
                    "share_cents": share_cents,
                    "remainder_cents": remainder_cents,
                }
            )

        winner_map = winner_map_by_league.get(league_key, {})
        league_rows: list[dict] = []
        for entry in entries:
            user_id = int(entry["user_id"])
            wallet = wallet_map.get(user_id)
            existing_row = existing_payouts_map.get(user_id)
            winner = winner_map.get(user_id)

            payout_cents = 0
            if season.get("status") == "finalized" and winner:
                payout_cents = int(winner.get("payout_cents") or 0)
            elif bool(entry.get("eligible_for_payout", True)) and not bool(
                entry.get("fraud_flag", False)
            ):
                rank = int(entry["rank"])
                if rank in top_payouts:
                    payout_cents = top_payouts[rank]
                else:
                    for payout_range in range_payouts:
                        if payout_range["start"] <= rank <= payout_range["end"]:
                            payout_cents = payout_range["share_cents"]
                            if payout_range["remainder_cents"] > 0:
                                payout_cents += 1
                                payout_range["remainder_cents"] -= 1
                            break

            if payout_cents > 0:
                summary["eligible"] += 1
            else:
                summary["without_payout"] += 1

            wallet_connected = bool(
                wallet and wallet.get("address") and wallet.get("verified")
            )
            if wallet_connected:
                summary["with_wallet"] += 1
            else:
                summary["without_wallet"] += 1

            wallet_reminder = wallet_reminders_map.get(user_id) or {}
            wallet_reminder_sent_at = wallet_reminder.get("sent_at")
            if wallet_reminder_sent_at:
                summary["wallet_reminder_sent"] += 1

            ton_amount_nano = 0
            if existing_row:
                ton_amount_nano = int(existing_row.ton_amount_nano or 0)
            elif wallet_connected and payout_cents > 0 and ton_price_usd > 0:
                ton_amount_nano = max(
                    0, int(round(((payout_cents / 100.0) / ton_price_usd) * TON_NANO))
                )

            derived_status = "wallet_missing"
            if payout_cents <= 0:
                derived_status = "no_payout"
            elif existing_row:
                derived_status = existing_row.status or "queued"
            elif season.get("status") == "finalized":
                derived_status = "queued" if wallet_connected else "wallet_missing"
            else:
                derived_status = (
                    "preview_ready" if wallet_connected else "wallet_missing"
                )

            league_rows.append(
                {
                    "user_id": user_id,
                    "username": entry.get("username"),
                    "league": league_key,
                    "rank": int(entry["rank"]),
                    "display_level": int(entry.get("display_level") or 1),
                    "score": int(entry.get("score") or 0),
                    "eligible_for_payout": bool(entry.get("eligible_for_payout", True)),
                    "fraud_flag": bool(entry.get("fraud_flag", False)),
                    "wallet_connected": wallet_connected,
                    "wallet_address": wallet.get("address")
                    if wallet_connected
                    else None,
                    "masked_wallet": wallet.get("masked_address")
                    if wallet_connected
                    else None,
                    "payout_cents": int(payout_cents),
                    "ton_amount_nano": int(ton_amount_nano),
                    "status": derived_status,
                    "tx_hash": getattr(existing_row, "tx_hash", None),
                    "note": getattr(existing_row, "note", None),
                    "wallet_reminder_sent_at": wallet_reminder_sent_at,
                    "wallet_reminder_hours_until_deadline": wallet_reminder.get(
                        "hours_until_deadline"
                    ),
                }
            )

        leagues_payload[league_key] = league_rows

    return season, leagues_payload, summary


async def recalculate_finalized_weekly_winner_payouts(
    session: AsyncSession,
    season_key: str,
    total_fund_cents: int,
) -> None:
    winners_result = await session.execute(
        select(WeeklyTournamentWinner)
        .where(WeeklyTournamentWinner.season_key == season_key)
        .order_by(
            WeeklyTournamentWinner.league.asc(),
            WeeklyTournamentWinner.rank.asc(),
            WeeklyTournamentWinner.user_id.asc(),
        )
    )
    winners = winners_result.scalars().all()
    if not winners:
        return

    winners_by_league: dict[str, list[WeeklyTournamentWinner]] = {}
    for winner in winners:
        winners_by_league.setdefault((winner.league or "bronze").lower(), []).append(
            winner
        )

    for league in WEEKLY_LEAGUE_ORDER:
        league_winners = winners_by_league.get(league, [])
        if not league_winners:
            continue

        league_fund_cents = int(
            max(0, int(total_fund_cents or 0))
            * WEEKLY_LEAGUE_FUND_SPLITS.get(league, 0)
        )
        top_payouts = {
            rank: int(league_fund_cents * share)
            for rank, share in WEEKLY_TOP3_PAYOUT_SPLITS.items()
        }

        range_payouts = []
        for payout_range in WEEKLY_RANGE_PAYOUT_SPLITS:
            start_rank = int(payout_range["start"])
            end_rank = int(payout_range["end"])
            pool_cents = int(league_fund_cents * float(payout_range["share"]))
            eligible_winners = [
                row
                for row in league_winners
                if start_rank <= int(row.rank or 0) <= end_rank
                and bool(row.eligible_for_payout)
                and not bool(row.fraud_flag)
            ]
            share_cents = 0
            remainder_cents = 0
            if eligible_winners:
                share_cents = pool_cents // len(eligible_winners)
                remainder_cents = pool_cents % len(eligible_winners)
            range_payouts.append(
                {
                    "start": start_rank,
                    "end": end_rank,
                    "share_cents": share_cents,
                    "remainder_cents": remainder_cents,
                }
            )

        for winner in league_winners:
            payout_cents = 0
            rank = int(winner.rank or 0)
            if bool(winner.eligible_for_payout) and not bool(winner.fraud_flag):
                if rank in top_payouts:
                    payout_cents = top_payouts[rank]
                else:
                    for payout_range in range_payouts:
                        if payout_range["start"] <= rank <= payout_range["end"]:
                            payout_cents = payout_range["share_cents"]
                            if payout_range["remainder_cents"] > 0:
                                payout_cents += 1
                                payout_range["remainder_cents"] -= 1
                            break

            winner.payout_cents = int(payout_cents)


@router.get("/api/admin/weekly-tournament/season/{season_key}/ton-queue")
async def admin_get_ton_payout_queue(season_key: str, request: Request):
    try:
        await require_admin_access(request)
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(WeeklyTournamentTonPayout)
                .where(WeeklyTournamentTonPayout.season_key == season_key)
                .order_by(
                    WeeklyTournamentTonPayout.league.asc(),
                    WeeklyTournamentTonPayout.rank.asc(),
                )
            )
            rows = result.scalars().all()

        return {
            "success": True,
            "season_key": season_key,
            "payouts": [
                {
                    "user_id": int(row.user_id),
                    "username": row.username,
                    "league": row.league,
                    "rank": int(row.rank or 0),
                    "wallet_address": row.wallet_address,
                    "masked_wallet": mask_ton_wallet(row.wallet_address),
                    "payout_cents": int(row.payout_cents or 0),
                    "ton_amount_nano": int(row.ton_amount_nano or 0),
                    "ton_price_usd": (int(row.ton_price_usd_micros or 0) / 1_000_000)
                    if row.ton_price_usd_micros
                    else 0,
                    "status": row.status,
                    "tx_hash": row.tx_hash,
                    "note": row.note,
                }
                for row in rows
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_get_ton_payout_queue: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-queue/preview")
async def admin_preview_ton_payout_queue(
    season_key: str, payload: AdminTonPayoutQueueRequest, request: Request
):
    try:
        await require_admin_access(request)
        ton_price_usd = float(payload.ton_price_usd or 0)
        if ton_price_usd <= 0:
            raise HTTPException(
                status_code=400, detail="ton_price_usd must be greater than zero"
            )

        season, payouts, stats = await build_weekly_ton_payout_candidates(
            season_key, ton_price_usd
        )
        if not season:
            raise HTTPException(status_code=404, detail="Season not found")

        return {
            "success": True,
            "season_key": season_key,
            "preview": True,
            "season_status": season.get("status"),
            "queued": len(payouts),
            "with_wallet": int(stats["with_wallet"]),
            "without_wallet": int(stats["without_wallet"]),
            "without_payout": int(stats["without_payout"]),
            "payouts": payouts,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_preview_ton_payout_queue: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/admin/weekly-tournament/season/{season_key}/ton-view")
async def admin_get_ton_payout_view(
    season_key: str,
    request: Request,
    ton_price_usd: float,
    league: str | None = None,
):
    try:
        await require_admin_access(request)
        if ton_price_usd <= 0:
            raise HTTPException(
                status_code=400, detail="ton_price_usd must be greater than zero"
            )
        league_key = (league or "").strip().lower() or None
        if league_key and league_key not in WEEKLY_LEAGUE_ORDER:
            raise HTTPException(status_code=400, detail="Unknown league")

        season, leagues_payload, summary = await build_weekly_ton_payout_view(
            season_key, float(ton_price_usd), league=league_key
        )
        if not season:
            raise HTTPException(status_code=404, detail="Season not found")

        return {
            "success": True,
            "season_key": season_key,
            "season_status": season.get("status"),
            "preview": season.get("status") != "finalized",
            "league": league_key,
            "summary": summary,
            "leagues": leagues_payload,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_get_ton_payout_view: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/wallet-reminders")
async def admin_send_wallet_reminders(
    season_key: str,
    payload: AdminWalletReminderRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        season_rows = await list_weekly_tournament_seasons(limit=52)
        season = next(
            (item for item in season_rows if item["season_key"] == season_key), None
        )
        if not season:
            raise HTTPException(status_code=404, detail="Season not found")
        if season.get("status") != "finalized":
            raise HTTPException(
                status_code=400,
                detail="Wallet reminders are available only after the season is finalized",
            )

        league_key = (payload.league or "").strip().lower() or None
        if league_key and league_key not in WEEKLY_LEAGUE_ORDER:
            raise HTTPException(status_code=400, detail="Unknown league")

        _, leagues_payload, _ = await build_weekly_ton_payout_view(
            season_key, 1.0, league=league_key
        )
        rows = [
            row
            for league_rows in leagues_payload.values()
            for row in league_rows
            if int(row.get("payout_cents") or 0) > 0
            and not bool(row.get("wallet_connected"))
        ]
        if not rows:
            return {
                "success": True,
                "season_key": season_key,
                "league": league_key,
                "candidates": 0,
                "sent": 0,
                "failed": 0,
                "results": [],
            }

        user_ids = sorted({int(row["user_id"]) for row in rows})
        reminder_results: list[dict] = []
        cache_ids_to_invalidate: list[int] = []

        async with AsyncSessionLocal() as session:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            user_map = {int(row.user_id): row for row in users_result.scalars().all()}

            for row in rows:
                user_id = int(row["user_id"])
                user_row = user_map.get(user_id)
                if not user_row:
                    reminder_results.append(
                        {
                            "user_id": user_id,
                            "league": row.get("league"),
                            "sent": False,
                            "error": "User not found",
                        }
                    )
                    continue

                ok, error = await send_telegram_wallet_reminder_message(
                    user_id=user_id,
                    season_key=season_key,
                    league=str(row.get("league") or league_key or ""),
                    hours_until_deadline=int(payload.hours_until_deadline),
                )
                if ok:
                    extra = parse_extra_data(user_row.extra_data)
                    reminders_by_season = extra.get("ton_wallet_reminders") or {}
                    if not isinstance(reminders_by_season, dict):
                        reminders_by_season = {}
                    reminders_by_season[season_key] = {
                        "sent_at": datetime.utcnow().isoformat(),
                        "league": str(row.get("league") or league_key or ""),
                        "hours_until_deadline": int(payload.hours_until_deadline),
                    }
                    extra["ton_wallet_reminders"] = reminders_by_season
                    user_row.extra_data = json.dumps(extra, ensure_ascii=False)
                    cache_ids_to_invalidate.append(user_id)

                reminder_results.append(
                    {
                        "user_id": user_id,
                        "league": row.get("league"),
                        "sent": ok,
                        "error": error,
                    }
                )

            await session.commit()

        for user_id in cache_ids_to_invalidate:
            await invalidate_user_cache(user_id)

        sent_count = sum(1 for item in reminder_results if item["sent"])
        failed_count = len(reminder_results) - sent_count
        return {
            "success": True,
            "season_key": season_key,
            "league": league_key,
            "candidates": len(rows),
            "sent": sent_count,
            "failed": failed_count,
            "results": reminder_results,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_send_wallet_reminders: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-queue")
async def admin_build_ton_payout_queue(
    season_key: str, payload: AdminTonPayoutQueueRequest, request: Request
):
    try:
        await require_admin_access(request)
        ton_price_usd = float(payload.ton_price_usd or 0)
        if ton_price_usd <= 0:
            raise HTTPException(
                status_code=400, detail="ton_price_usd must be greater than zero"
            )

        winners = await get_weekly_tournament_winners(season_key)
        if not winners:
            return {
                "success": True,
                "season_key": season_key,
                "created": 0,
                "queued": 0,
                "skipped_without_wallet": 0,
                "skipped_without_payout": 0,
            }

        user_ids = [int(item["user_id"]) for item in winners]
        async with AsyncSessionLocal() as session:
            users_result = await session.execute(
                select(User).where(User.user_id.in_(user_ids))
            )
            user_rows = users_result.scalars().all()
            wallet_map = {}
            for row in user_rows:
                extra_data = {}
                if row.extra_data:
                    try:
                        extra_data = json.loads(row.extra_data)
                    except json.JSONDecodeError:
                        extra_data = {}
                wallet = get_ton_wallet_from_user({"extra_data": extra_data})
                if wallet["connected"] and wallet["verified"] and wallet["address"]:
                    wallet_map[int(row.user_id)] = wallet

            existing_result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key
                )
            )
            existing_rows = {
                int(row.user_id): row for row in existing_result.scalars().all()
            }

            created = 0
            queued = 0
            skipped_without_wallet = 0
            skipped_without_payout = 0
            skipped_locked = 0
            ton_price_usd_micros = int(round(ton_price_usd * 1_000_000))

            for winner in winners:
                user_id = int(winner["user_id"])
                payout_cents = int(winner.get("payout_cents") or 0)
                if (
                    payout_cents <= 0
                    or not bool(winner.get("eligible_for_payout", True))
                    or bool(winner.get("fraud_flag", False))
                ):
                    skipped_without_payout += 1
                    continue

                wallet = wallet_map.get(user_id)
                if not wallet:
                    skipped_without_wallet += 1
                    continue

                ton_amount_nano = max(
                    0, int(round(((payout_cents / 100.0) / ton_price_usd) * TON_NANO))
                )
                if ton_amount_nano <= 0:
                    skipped_without_payout += 1
                    continue

                row = existing_rows.get(user_id)
                if row is None:
                    row = WeeklyTournamentTonPayout(
                        season_key=season_key,
                        user_id=user_id,
                        username=winner.get("username"),
                        league=winner.get("league") or "bronze",
                        rank=int(winner.get("rank") or 0),
                        wallet_address=wallet["address"],
                        payout_cents=payout_cents,
                        ton_amount_nano=ton_amount_nano,
                        ton_price_usd_micros=ton_price_usd_micros,
                        status="queued",
                    )
                    session.add(row)
                    existing_rows[user_id] = row
                    created += 1
                else:
                    existing_status = str(row.status or "").strip().lower()
                    if existing_status not in {"", "queued", "failed", "cancelled"}:
                        skipped_locked += 1
                        continue
                    row.username = winner.get("username")
                    row.league = winner.get("league") or row.league
                    row.rank = int(winner.get("rank") or row.rank or 0)
                    row.wallet_address = wallet["address"]
                    row.payout_cents = payout_cents
                    row.ton_amount_nano = ton_amount_nano
                    row.ton_price_usd_micros = ton_price_usd_micros
                    row.updated_at = datetime.utcnow()
                    if existing_status in {"failed", "cancelled"}:
                        row.status = "queued"
                queued += 1

            await session.commit()

        return {
            "success": True,
            "season_key": season_key,
            "created": created,
            "queued": queued,
            "skipped_without_wallet": skipped_without_wallet,
            "skipped_without_payout": skipped_without_payout,
            "skipped_locked": skipped_locked,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_build_ton_payout_queue: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-payout-status")
async def admin_update_ton_payout_status(
    season_key: str, payload: AdminTonPayoutStatusUpdateRequest, request: Request
):
    try:
        await require_admin_access(request)
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key,
                    WeeklyTournamentTonPayout.user_id == payload.user_id,
                )
            )
            row = result.scalar_one_or_none()
            if not row:
                raise HTTPException(status_code=404, detail="TON payout row not found")

            row.status = (payload.status or "queued").strip().lower()
            row.tx_hash = (payload.tx_hash or "").strip() or None
            row.note = (payload.note or "").strip() or None
            row.updated_at = datetime.utcnow()
            await session.commit()

            return {
                "success": True,
                "season_key": season_key,
                "user_id": payload.user_id,
                "status": row.status,
                "tx_hash": row.tx_hash,
                "note": row.note,
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_update_ton_payout_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-payout-status/bulk")
async def admin_update_ton_payout_status_bulk(
    season_key: str,
    payload: AdminTonPayoutBulkStatusUpdateRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        user_ids = sorted(
            {int(user_id) for user_id in (payload.user_ids or []) if int(user_id) > 0}
        )
        if not user_ids:
            raise HTTPException(status_code=400, detail="user_ids are required")

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(WeeklyTournamentTonPayout).where(
                    WeeklyTournamentTonPayout.season_key == season_key,
                    WeeklyTournamentTonPayout.user_id.in_(user_ids),
                )
            )
            rows = result.scalars().all()
            if not rows:
                raise HTTPException(status_code=404, detail="TON payout rows not found")

            status = (payload.status or "queued").strip().lower()
            tx_hash = (payload.tx_hash or "").strip() or None
            note = (payload.note or "").strip() or None
            updated_user_ids = []

            for row in rows:
                row.status = status
                row.tx_hash = tx_hash
                row.note = note
                row.updated_at = datetime.utcnow()
                updated_user_ids.append(int(row.user_id))

            await session.commit()

        return {
            "success": True,
            "season_key": season_key,
            "status": status,
            "updated_count": len(updated_user_ids),
            "updated_user_ids": updated_user_ids,
            "tx_hash": tx_hash,
            "note": note,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_update_ton_payout_status_bulk: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/ton-payouts/confirm")
async def admin_confirm_ton_payouts(
    season_key: str,
    payload: AdminTonPayoutConfirmRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        sender_wallet_address = (payload.sender_wallet_address or "").strip()
        if not is_valid_ton_wallet_address(sender_wallet_address):
            raise HTTPException(status_code=400, detail="Invalid sender_wallet_address")

        lookback_minutes = max(5, int(payload.lookback_minutes or 180))
        requested_user_ids = sorted(
            {int(user_id) for user_id in (payload.user_ids or []) if int(user_id) > 0}
        )

        async with AsyncSessionLocal() as session:
            query = select(WeeklyTournamentTonPayout).where(
                WeeklyTournamentTonPayout.season_key == season_key,
                WeeklyTournamentTonPayout.status.in_(["queued", "submitted"]),
            )
            if requested_user_ids:
                query = query.where(
                    WeeklyTournamentTonPayout.user_id.in_(requested_user_ids)
                )

            result = await session.execute(
                query.order_by(WeeklyTournamentTonPayout.rank.asc())
            )
            rows = result.scalars().all()
            if not rows:
                return {
                    "success": True,
                    "season_key": season_key,
                    "checked": 0,
                    "confirmed": 0,
                    "confirmed_user_ids": [],
                    "missing_user_ids": [],
                }

            recipient_accounts = sorted(
                {row.wallet_address for row in rows if row.wallet_address}
            )
            start_utime = int(
                (datetime.utcnow() - timedelta(minutes=lookback_minutes)).timestamp()
            )
            transactions = []
            for index in range(0, len(recipient_accounts), 50):
                transactions.extend(
                    await fetch_ton_transactions_for_accounts(
                        recipient_accounts[index : index + 50], start_utime
                    )
                )

            matched_rows, matched_user_ids = match_ton_queue_rows_to_transactions(
                rows, transactions, sender_wallet_address
            )
            confirmed_user_ids: list[int] = []
            tx_hash_by_user = {
                int(item["user_id"]): item.get("tx_hash") or None
                for item in matched_rows
            }
            confirmed_at_by_user = {
                int(item["user_id"]): item.get("confirmed_at") or 0
                for item in matched_rows
            }

            for row in rows:
                user_id = int(row.user_id)
                if user_id not in matched_user_ids:
                    continue
                row.status = "sent"
                row.tx_hash = tx_hash_by_user.get(user_id) or row.tx_hash
                confirmed_at = confirmed_at_by_user.get(user_id)
                note_suffix = ""
                if confirmed_at:
                    note_suffix = f"confirmed_at={datetime.utcfromtimestamp(int(confirmed_at)).isoformat()}Z"
                row.note = " | ".join(
                    part
                    for part in [row.note or "", "verified_on_chain", note_suffix]
                    if part
                )
                row.updated_at = datetime.utcnow()
                confirmed_user_ids.append(user_id)

            await session.commit()

        missing_user_ids = [
            int(row.user_id)
            for row in rows
            if int(row.user_id) not in set(confirmed_user_ids)
        ]
        return {
            "success": True,
            "season_key": season_key,
            "checked": len(rows),
            "confirmed": len(confirmed_user_ids),
            "confirmed_user_ids": confirmed_user_ids,
            "missing_user_ids": missing_user_ids,
        }
    except HTTPException:
        raise
    except httpx.HTTPError as e:
        logger.error(f"TON verifier HTTP error: {e}")
        raise HTTPException(status_code=502, detail="Failed to verify TON transactions")
    except Exception as e:
        logger.error(f"Error in admin_confirm_ton_payouts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/fund")
async def admin_set_weekly_tournament_fund(
    season_key: str,
    payload: WeeklyTournamentFundRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        starts_at = datetime.strptime(season_key, "%Y-%m-%d")
        ends_at = starts_at + timedelta(days=7)

        async with AsyncSessionLocal() as session:
            season = await ensure_weekly_tournament_season(
                session, season_key, starts_at, ends_at
            )
            season.gross_ad_revenue_cents = int(payload.gross_ad_revenue_cents or 0)
            season.payout_fund_cents = int(payload.payout_fund_cents or 0)
            if season.status == "finalized":
                await recalculate_finalized_weekly_winner_payouts(
                    session,
                    season_key,
                    int(payload.payout_fund_cents or 0),
                )
            await session.commit()

        return {
            "success": True,
            "season_key": season_key,
            "gross_ad_revenue_cents": int(payload.gross_ad_revenue_cents or 0),
            "payout_fund_cents": int(payload.payout_fund_cents or 0),
            "league_splits": WEEKLY_LEAGUE_FUND_SPLITS,
            "rank_splits": {
                "top1": WEEKLY_TOP3_PAYOUT_SPLITS[1],
                "top2": WEEKLY_TOP3_PAYOUT_SPLITS[2],
                "top3": WEEKLY_TOP3_PAYOUT_SPLITS[3],
                "ranks_4_10": WEEKLY_RANGE_PAYOUT_SPLITS[0]["share"],
                "ranks_11_20": WEEKLY_RANGE_PAYOUT_SPLITS[1]["share"],
                "ranks_21_50": WEEKLY_RANGE_PAYOUT_SPLITS[2]["share"],
            },
        }
    except ValueError:
        raise HTTPException(
            status_code=400, detail="season_key must use YYYY-MM-DD format"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_set_weekly_tournament_fund: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/admin/weekly-tournament/season/{season_key}/winner-stars")
async def admin_set_weekly_tournament_winner_stars(
    season_key: str,
    payload: AdminWinnerStarsUpdateRequest,
    request: Request,
):
    try:
        await require_admin_access(request)
        async with AsyncSessionLocal() as session:
            winner_result = await session.execute(
                select(WeeklyTournamentWinner).where(
                    WeeklyTournamentWinner.season_key == season_key,
                    WeeklyTournamentWinner.user_id == payload.user_id,
                )
            )
            winner = winner_result.scalar_one_or_none()
            if not winner:
                raise HTTPException(
                    status_code=404, detail="Winner not found for this season"
                )

            winner.stars_reward = int(payload.stars_reward or 0)
            await session.commit()

            return {
                "success": True,
                "season_key": season_key,
                "user_id": payload.user_id,
                "stars_reward": int(winner.stars_reward or 0),
                "league": winner.league,
                "rank": int(winner.rank or 0),
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in admin_set_weekly_tournament_winner_stars: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/skins/stars-invoice")
async def create_skin_stars_invoice(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("stars_skin_invoice", payload.user_id, ttl=3)

        price = get_stars_skin_price(payload.skin_id)
        if price is None:
            raise HTTPException(status_code=400, detail="Skin is not sold for Stars")

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}

        owned_skins = extra.get("owned_skins", [DEFAULT_SKIN_ID])
        if payload.skin_id in owned_skins:
            raise HTTPException(status_code=400, detail="Skin already owned")

        invoice_link = await create_telegram_stars_invoice_link(
            user_id=payload.user_id, skin_id=payload.skin_id, price=price
        )

        return {
            "success": True,
            "invoice_link": invoice_link,
            "price": price,
            "skin_id": payload.skin_id,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating Stars invoice: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/tournament/leaderboard")
async def get_tournament_leaderboard():
    """Get top 3 players from Redis leaderboard"""
    try:
        players = []

        conn = await get_redis_or_none()
        if conn:
            top_players = await conn.zrevrange(TOURNAMENT_KEY, 0, 2, withscores=True)

            for idx, (user_id_str, score) in enumerate(top_players):
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    continue

                user = await get_user_cached(user_id)

                username = user.get("username") if user else None
                avatar_url = (
                    f"https://t.me/i/userpic/320/{username}.jpg"
                    if username
                    else "/imgg/default_avatar.png"
                )

                players.append(
                    {
                        "rank": idx + 1,
                        "user_id": user_id,
                        "name": mask_username(username),
                        "avatar": avatar_url,
                        "score": int(score),
                    }
                )

        now = datetime.utcnow()
        tomorrow = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        time_left = int((tomorrow - now).total_seconds())

        return {
            "success": True,
            "players": players,
            "prize_pool": TOURNAMENT_PRIZE_POOL,
            "time_left": time_left,
            "online_now": await get_online_users_count(),
        }

    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/tournament/player-rank/{user_id}")
async def get_player_rank(user_id: int, request: Request):
    """Get player's rank from Redis leaderboard"""
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            return {
                "success": True,
                "rank": 0,
                "score": 0,
                "next_rank_score": 0,
                "avatar": "/imgg/default_avatar.png",
                "name": "Player",
            }

        username = user.get("username")
        avatar_url = (
            f"https://t.me/i/userpic/320/{username}.jpg"
            if username
            else "/imgg/default_avatar.png"
        )

        redis_conn = await ensure_redis_available()

        score = await redis_conn.zscore(TOURNAMENT_KEY, str(user_id))
        score = int(score) if score is not None else 0

        rev_rank = await redis_conn.zrevrank(TOURNAMENT_KEY, str(user_id))
        rank = (rev_rank + 1) if rev_rank is not None else 0

        next_rank_score = 0
        if rev_rank is not None and rev_rank > 0:
            higher_player = await redis_conn.zrevrange(
                TOURNAMENT_KEY, rev_rank - 1, rev_rank - 1, withscores=True
            )
            if higher_player:
                _, higher_score = higher_player[0]
                next_rank_score = max(0, int(higher_score) - score)

        return {
            "success": True,
            "rank": rank,
            "score": score,
            "next_rank_score": next_rank_score,
            "avatar": avatar_url,
            "name": mask_username(username),
        }

    except Exception as e:
        logger.error(f"Error getting player rank: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== Р—РђР”РђР§Р ====================

_task_completion_store = {}


@router.get("/api/tasks/{user_id}")
async def get_tasks(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        completed_tasks = await get_completed_tasks(user_id) or []

        tasks = [
            {
                "id": "daily_bonus",
                "title": "рџ“… Daily Bonus",
                "description": "Come back every day",
                "reward": "25000 coins",
                "icon": "рџ“…",
                "completed": "daily_bonus" in completed_tasks,
            },
            {
                "id": "energy_refill",
                "title": "вљЎ Infinite Energy",
                "description": "5 minutes of unlimited energy",
                "reward": "вљЎ 5 minutes",
                "icon": "вљЎ",
                "completed": "energy_refill" in completed_tasks,
            },
            {
                "id": "link_click",
                "title": "рџ”— Follow Link",
                "description": "Click the link and get reward",
                "reward": "25000 coins",
                "icon": "рџ”—",
                "completed": "link_click" in completed_tasks,
            },
            {
                "id": "telegram_sub",
                "title": "Telegram Channel",
                "description": "Subscribe to Telegram channel",
                "reward": "20000 coins + skin",
                "icon": "📣",
                "completed": "telegram_sub" in completed_tasks,
            },
            {
                "id": "tiktok_sub",
                "title": "TikTok",
                "description": "Subscribe to TikTok",
                "reward": "20000 coins + skin",
                "icon": "🎵",
                "completed": "tiktok_sub" in completed_tasks,
            },
            {
                "id": "instagram_sub",
                "title": "Instagram",
                "description": "Subscribe to Instagram",
                "reward": "20000 coins + skin",
                "icon": "📸",
                "completed": "instagram_sub" in completed_tasks,
            },
            {
                "id": "invite_5_friends",
                "title": "рџ‘Ґ Invite 5 Friends",
                "description": "Invite 5 friends",
                "reward": "20000 coins",
                "icon": "рџ‘Ґ",
                "completed": "invite_5_friends" in completed_tasks,
                "progress": min(user.get("referral_count", 0), 5),
                "total": 5,
            },
        ]
        return tasks
    except Exception as e:
        logger.error(f"Error in get_tasks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/complete-task")
async def complete_task(payload: TaskCompleteRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "complete_task",
            request,
            payload.user_id,
            RATE_LIMITS["complete_task"][0],
            RATE_LIMITS["complete_task"][1],
            ip_limit=RATE_LIMITS["complete_task"][0] * 2,
        )
        await require_user_action_lock("complete_task", payload.user_id, ttl=5)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        task_id = payload.task_id

        completed = await get_completed_tasks(payload.user_id) or []
        if task_id in completed:
            raise HTTPException(status_code=400, detail="Task already completed")

        if task_id == "link_click":
            updated_user = await complete_task_reward_atomically(
                payload.user_id,
                task_id,
                {"coins": int(user.get("coins", 0)) + 25000},
            )
            await invalidate_user_cache(payload.user_id)
            return {
                "success": True,
                "message": "рџ”— +25000 coins!",
                "coins": int(updated_user.get("coins", 0)),
            }

        if task_id == "daily_bonus":
            updated_user = await complete_task_reward_atomically(
                payload.user_id,
                task_id,
                {"coins": int(user.get("coins", 0)) + 25000},
            )
            await invalidate_user_cache(payload.user_id)
            return {
                "success": True,
                "message": "рџЋЃ +25000 coins!",
                "coins": int(updated_user.get("coins", 0)),
            }

        elif task_id == "energy_refill":
            await complete_task_reward_atomically(payload.user_id, task_id)
            await invalidate_user_cache(payload.user_id)
            return {"success": True, "message": "вљЎ Energy refill activated!"}

        elif task_id == "invite_5_friends":
            if user.get("referral_count", 0) >= 5:
                updated_user = await complete_task_reward_atomically(
                    payload.user_id,
                    task_id,
                    {"coins": int(user.get("coins", 0)) + 20000},
                )
                await invalidate_user_cache(payload.user_id)
                return {
                    "success": True,
                    "message": "рџ‘Ґ +20000 coins!",
                    "coins": int(updated_user.get("coins", 0)),
                }
            else:
                raise HTTPException(status_code=400, detail="Not enough friends")

        elif task_id in SOCIAL_SUB_TASK_SKINS:
            extra = user.get("extra_data", {}) or {}
            if isinstance(extra, str):
                try:
                    extra = json.loads(extra)
                except Exception:
                    extra = {}

            owned_skins = normalize_owned_skins(
                extra.get("owned_skins", [DEFAULT_SKIN_ID])
            )
            social_skin_id = SOCIAL_SUB_TASK_SKINS[task_id]

            if task_id == "telegram_sub":
                is_verified = await verify_telegram_channel_subscription(
                    payload.user_id
                )
                if not is_verified:
                    raise HTTPException(
                        status_code=400,
                        detail="Telegram subscription was not verified yet",
                    )
            else:
                raise HTTPException(
                    status_code=400, detail="Task verification is not available yet"
                )

            if social_skin_id not in owned_skins:
                owned_skins.append(social_skin_id)

            extra["owned_skins"] = normalize_owned_skins(owned_skins)
            updated_user = await complete_task_reward_atomically(
                payload.user_id,
                task_id,
                {
                    "coins": int(user.get("coins", 0)) + 20000,
                    "extra_data": extra,
                },
            )
            await invalidate_user_cache(payload.user_id)
            return {
                "success": True,
                "message": "✅ +20000 coins + skin!",
                "coins": int(updated_user.get("coins", 0)),
                "skin_id": social_skin_id,
                "verified": task_id == "telegram_sub",
            }

        raise HTTPException(status_code=400, detail="Unknown task")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in complete_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/api/video-tasks/status/{user_id}")
async def get_video_tasks_status(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        last_claims = get_video_task_last_claims(extra)
        now = datetime.utcnow()
        tasks = []

        for task_id, config in VIDEO_TASK_DEFINITIONS.items():
            claimed_at = parse_iso_datetime(last_claims.get(task_id))
            cooldown_seconds = int(config["cooldown_minutes"] * 60)
            remaining_seconds = 0
            available = True

            if claimed_at:
                elapsed = (now - claimed_at).total_seconds()
                remaining_seconds = max(0, cooldown_seconds - int(elapsed))
                available = remaining_seconds <= 0

            tasks.append(
                {
                    "task_id": task_id,
                    "available": available,
                    "remaining_seconds": remaining_seconds,
                    "cooldown_minutes": config["cooldown_minutes"],
                }
            )

        return {"success": True, "tasks": tasks}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_video_tasks_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/video-tasks/claim")
async def claim_video_task(payload: VideoTaskClaimRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "video_task_claim", request, payload.user_id, 20, 60, ip_limit=40
        )
        await consume_ad_action_session(
            payload.user_id, payload.ad_session_id, "video_task"
        )
        await require_user_action_lock(
            f"video_task:{payload.task_id}", payload.user_id, ttl=3
        )

        config = VIDEO_TASK_DEFINITIONS.get(payload.task_id)
        if not config:
            raise HTTPException(status_code=400, detail="Unknown video task")

        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))
        last_claims = get_video_task_last_claims(extra)
        boosts = get_video_task_boosts(extra)
        now = datetime.utcnow()
        claimed_at = parse_iso_datetime(last_claims.get(payload.task_id))
        cooldown_seconds = int(config["cooldown_minutes"] * 60)

        if claimed_at and (now - claimed_at).total_seconds() < cooldown_seconds:
            remaining = cooldown_seconds - int((now - claimed_at).total_seconds())
            raise HTTPException(
                status_code=429,
                detail=f"Task cooldown {remaining // 60}:{remaining % 60:02d}",
            )

        response = {
            "success": True,
            "task_id": payload.task_id,
            "coins": int(user.get("coins", 0)),
        }
        updates = {}

        if config["type"] == "coin_drop":
            reward = resolve_video_task_coin_drop()
            response["coins_reward"] = reward
            response["coins"] = int(user.get("coins", 0)) + reward
            response["message"] = f"+{reward} coins"
            updates["coins"] = response["coins"]
        elif config["type"] == "tap_boost":
            expires_at = (
                now + timedelta(minutes=config["duration_minutes"])
            ).isoformat()
            boosts["tap_boost"] = {
                "expires_at": expires_at,
                "multiplier": int(config["multiplier"]),
            }
            response["message"] = (
                f"x{config['multiplier']} tap boost for {config['duration_minutes']} min"
            )
            response["task_tap_boost_active"] = True
            response["task_tap_boost_expires_at"] = expires_at
            response["task_tap_boost_multiplier"] = int(config["multiplier"])
        elif config["type"] == "passive_boost":
            expires_at = (
                now + timedelta(minutes=config["duration_minutes"])
            ).isoformat()
            boosts["passive_boost"] = {
                "expires_at": expires_at,
                "multiplier": int(config["multiplier"]),
            }
            response["message"] = (
                f"x{config['multiplier']} passive income for {config['duration_minutes']} min"
            )
            response["task_passive_boost_active"] = True
            response["task_passive_boost_expires_at"] = expires_at
            response["task_passive_boost_multiplier"] = int(config["multiplier"])

        last_claims[payload.task_id] = now.isoformat()
        extra["video_task_last_claims"] = last_claims
        extra["video_task_boosts"] = boosts
        updates["extra_data"] = extra

        await update_user(payload.user_id, updates)
        await invalidate_user_cache(payload.user_id)
        refreshed_user = await get_user_cached(payload.user_id)
        response["coins"] = int((refreshed_user or {}).get("coins", response["coins"]))
        await record_rewarded_ad_claim(
            payload.user_id, "tasks", {"task_id": payload.task_id}
        )
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in claim_video_task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== РџРђРЎРЎРР’РќР«Р™ Р”РћРҐРћР” ====================


@router.post("/api/passive-income")
async def passive_income(payload: PassiveIncomeRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_dual_rate_limit(
            "passive_income", request, payload.user_id, 20, 60, ip_limit=40
        )
        await require_user_action_lock("passive_income", payload.user_id, ttl=5)
        user = await get_user(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        last_income = normalize_dt(user.get("last_passive_income"))
        now = datetime.utcnow()

        if not last_income:
            initialized_user = await update_user_if_matches(
                payload.user_id,
                {"last_passive_income": None},
                {"last_passive_income": now},
            )
            if initialized_user is None:
                raise HTTPException(
                    status_code=409, detail="Passive income baseline changed, retry"
                )
            await invalidate_user_cache(payload.user_id)
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        elapsed_seconds = max(0.0, (now - last_income).total_seconds())
        elapsed_seconds = min(elapsed_seconds, 24 * 3600)

        extra = parse_extra_data(user.get("extra_data"))

        passive_boost_active, _, passive_boost_multiplier = get_active_video_task_boost(
            extra, "passive_boost"
        )
        base_hour_value = int(
            user.get("profit_per_hour", get_hour_value(user.get("profit_level", 0)))
        )
        hour_value = (
            base_hour_value * max(1, passive_boost_multiplier)
            if passive_boost_active
            else base_hour_value
        )
        if hour_value <= 0 or elapsed_seconds <= 0:
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        total_income = int((hour_value * elapsed_seconds) // 3600)
        if total_income <= 0:
            return {"success": True, "coins": user["coins"], "income": 0, "message": ""}

        consumed_seconds = (total_income * 3600) / hour_value
        new_last_income = min(now, last_income + timedelta(seconds=consumed_seconds))
        new_coins = int(user.get("coins", 0)) + total_income

        updated_user = await update_user_if_matches(
            payload.user_id,
            {
                "coins": int(user.get("coins", 0)),
                "last_passive_income": last_income,
            },
            {
                "coins": new_coins,
                "last_passive_income": new_last_income,
            },
        )
        if not updated_user:
            logger.warning(
                "Atomic passive-income update conflict for user=%s", payload.user_id
            )
            raise HTTPException(
                status_code=409, detail="Passive income state changed, retry"
            )

        await invalidate_user_cache(payload.user_id)
        referral_bonus = await grant_referral_share_bonus(updated_user, total_income)

        return {
            "success": True,
            "coins": int(updated_user.get("coins", new_coins)),
            "income": total_income,
            "referral_bonus_paid": referral_bonus,
            "message": f"+{total_income} passive income",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in passive_income: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== DAILY REWARDS ====================


@router.get("/api/daily-reward/status/{user_id}")
async def get_daily_reward_status(user_id: int, request: Request):
    try:
        await require_telegram_user(request, user_id)
        user = await get_user_cached(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = parse_extra_data(user.get("extra_data"))

        claimed_days, last_claim_date = get_daily_reward_progress(extra)
        today = datetime.utcnow().date().isoformat()
        claim_available = (
            claimed_days < DAILY_REWARD_MAX_DAYS and last_claim_date != today
        )
        next_day = min(claimed_days + 1, DAILY_REWARD_MAX_DAYS)
        infinite_energy_active, infinite_energy_expires_at = (
            is_daily_infinite_energy_active(user)
        )

        return {
            "success": True,
            "claimed_days": claimed_days,
            "claim_available": claim_available,
            "next_day": next_day,
            "infinite_energy_active": infinite_energy_active,
            "infinite_energy_expires_at": infinite_energy_expires_at,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_daily_reward_status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/daily-reward/claim")
async def claim_daily_reward(payload: UserIdRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_redis_rate_limit("claim_daily_reward", payload.user_id, 10, 60)
        await require_user_action_lock("claim_daily_reward", payload.user_id, ttl=5)
        today = datetime.utcnow().date().isoformat()

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(User).where(User.user_id == payload.user_id).with_for_update()
            )
            user_row = result.scalar_one_or_none()
            if not user_row:
                raise HTTPException(status_code=404, detail="User not found")

            extra = {}
            if user_row.extra_data:
                try:
                    extra = json.loads(user_row.extra_data)
                except json.JSONDecodeError:
                    extra = {}

            claimed_days, last_claim_date = get_daily_reward_progress(extra)

            if claimed_days >= DAILY_REWARD_MAX_DAYS:
                raise HTTPException(status_code=400, detail="Daily rewards completed")

            if last_claim_date == today:
                raise HTTPException(
                    status_code=400, detail="Reward already claimed today"
                )

            day = claimed_days + 1
            coins_reward = day * DAILY_REWARD_BASE_COINS
            new_coins = int(user_row.coins or 0) + coins_reward

            extra["daily_reward_claimed_days"] = day
            extra["daily_reward_last_claim_date"] = today

            response_payload = {
                "success": True,
                "day": day,
                "coins_reward": coins_reward,
                "coins": new_coins,
                "claim_available": False,
            }

            if day % 7 == 0 and day < DAILY_REWARD_MAX_DAYS:
                active_boosts = extra.get("active_boosts", {})
                if not isinstance(active_boosts, dict):
                    active_boosts = {}
                expires_at = (
                    datetime.utcnow()
                    + timedelta(minutes=DAILY_REWARD_INFINITE_ENERGY_MINUTES)
                ).isoformat()
                active_boosts["daily_infinite_energy"] = {
                    "active": True,
                    "expires_at": expires_at,
                }
                extra["active_boosts"] = active_boosts
                response_payload["infinite_energy_expires_at"] = expires_at

            if day == DAILY_REWARD_MAX_DAYS:
                owned_skins = normalize_owned_skins(
                    extra.get("owned_skins", [DEFAULT_SKIN_ID])
                )
                if DAILY_REWARD_SKIN_ID not in owned_skins:
                    owned_skins.append(DAILY_REWARD_SKIN_ID)
                extra["owned_skins"] = normalize_owned_skins(owned_skins)
                response_payload["skin_id"] = DAILY_REWARD_SKIN_ID

            user_row.coins = new_coins
            user_row.extra_data = json.dumps(extra)
            await session.commit()

        await invalidate_user_cache(payload.user_id)
        refreshed_user = await get_user_cached(payload.user_id)
        response_payload["coins"] = int(
            (refreshed_user or {}).get("coins", response_payload["coins"])
        )

        return response_payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in claim_daily_reward: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== РЎРљРРќР« ====================


@router.post("/api/select-skin")
async def select_skin(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("select_skin", payload.user_id, ttl=3)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except Exception:
                extra = {}
        if not isinstance(extra, dict):
            extra = {}

        owned_skins = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        selected_skin = normalize_selected_skin(payload.skin_id, owned_skins)
        if selected_skin not in owned_skins:
            raise HTTPException(status_code=400, detail="Skin not owned")

        extra["owned_skins"] = owned_skins
        extra["selected_skin"] = selected_skin

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {"success": True, "selected_skin": selected_skin}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in select_skin: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/api/unlock-skin")
async def unlock_skin(payload: SkinRequest, request: Request):
    try:
        await require_telegram_user(request, payload.user_id)
        await require_user_action_lock("unlock_skin", payload.user_id, ttl=5)
        user = await get_user_cached(payload.user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        extra = user.get("extra_data", {}) or {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra)
            except:
                extra = {}

        skin_id = LEGACY_SKIN_ID_MAP.get(payload.skin_id, payload.skin_id)
        if skin_id not in VALID_SKIN_IDS:
            raise HTTPException(status_code=400, detail="Unknown skin")

        owned = normalize_owned_skins(extra.get("owned_skins", [DEFAULT_SKIN_ID]))
        skin_ad_progress = get_skin_ad_progress(extra)

        if skin_id in owned:
            return {"success": True}

        if skin_id in SKIN_REQUIREMENTS and SKIN_REQUIREMENTS[skin_id]["type"] == "ads":
            required = int(SKIN_REQUIREMENTS[skin_id]["count"])
            current_progress = int(skin_ad_progress.get(skin_id, 0) or 0)
            if current_progress < required:
                raise HTTPException(status_code=400, detail="Not enough ads watched")

        # вњ… РґРѕР±Р°РІР»СЏРµРј СЃРєРёРЅ
        owned.append(skin_id)
        extra["owned_skins"] = normalize_owned_skins(owned)

        await update_user(payload.user_id, {"extra_data": extra})
        await invalidate_user_cache(payload.user_id)

        return {"success": True}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unlock skin error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ==================== Р—РђРџРЈРЎРљ ====================
