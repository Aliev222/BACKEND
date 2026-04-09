import os
import logging
import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from infrastructure.database import engine, AsyncSessionLocal, healthcheck_db
from infrastructure.redis import init_redis, close_redis
from core.startup_validation import validate_startup_config, StartupValidationError
from routers import legacy
from routers.admin_legacy import router as admin_legacy_router
from routers.infra import router as infra_router
from routers.online import (
    router as online_router,
    router_legacy as online_legacy_router,
)
from routers.daily_reward import router as daily_reward_router
from routers.tasks import router_legacy as tasks_legacy_router
from routers.passive import router as passive_router
from routers.referrals import router_legacy as referrals_legacy_router
from routers.ton_wallet import router as ton_wallet_router
from workers import referral_flush, tournament_flush, coins_flush
from observability.metrics import observe_http_request

logger = logging.getLogger(__name__)

BOT_MODE = os.getenv("BOT_MODE", "api")
ALLOW_DEGRADED_START = (
    os.getenv("ALLOW_DEGRADED_START", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
REDIS_STARTUP_RETRIES = max(
    1, int((os.getenv("REDIS_STARTUP_RETRIES", "8") or "8").strip())
)
REDIS_STARTUP_RETRY_DELAY_SECONDS = max(
    0.25, float((os.getenv("REDIS_STARTUP_RETRY_DELAY_SECONDS", "1.5") or "1.5").strip())
)
WORKER_TASKS = []


def _worker_done_callback(task: asyncio.Task) -> None:
    """Called immediately when a worker task finishes."""
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    except BaseException:
        return

    if exc is not None:
        logger.critical(
            "WORKER_CRASH name=%s exception=%s: %s",
            task.get_name(),
            type(exc).__name__,
            exc,
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global WORKER_TASKS

    logger.info("Starting SPIRIT API (legacy endpoints)")
    try:
        validate_startup_config(bot_mode=BOT_MODE)
    except StartupValidationError as e:
        logger.critical("Startup configuration validation failed: %s", e)
        raise

    redis_conn = None
    for attempt in range(1, REDIS_STARTUP_RETRIES + 1):
        redis_conn = await init_redis()
        if redis_conn is not None:
            if attempt > 1:
                logger.warning(
                    "Redis connected on retry attempt %s/%s",
                    attempt,
                    REDIS_STARTUP_RETRIES,
                )
            break
        if attempt < REDIS_STARTUP_RETRIES:
            logger.warning(
                "Redis unavailable on startup attempt %s/%s, retrying in %.2fs",
                attempt,
                REDIS_STARTUP_RETRIES,
                REDIS_STARTUP_RETRY_DELAY_SECONDS,
            )
            await asyncio.sleep(REDIS_STARTUP_RETRY_DELAY_SECONDS)

    if BOT_MODE == "api" and redis_conn is None and not ALLOW_DEGRADED_START:
        logger.critical(
            "Redis unavailable after %s attempts while BOT_MODE=api; refusing to start",
            REDIS_STARTUP_RETRIES,
        )
        raise RuntimeError("Redis unavailable at startup")

    if BOT_MODE == "api" and redis_conn is not None:
        WORKER_TASKS = [
            asyncio.create_task(coins_flush.coins_flush_loop(), name="coins_flush"),
            asyncio.create_task(
                referral_flush.referral_flush_loop(), name="referral_flush"
            ),
            asyncio.create_task(
                tournament_flush.tournament_flush_loop(), name="tournament_flush"
            ),
        ]
        for task in WORKER_TASKS:
            task.add_done_callback(_worker_done_callback)
        logger.info("Background flush workers started (coins, referrals, tournament)")
    elif BOT_MODE == "api":
        logger.error(
            "Starting in degraded mode (no Redis). Workers are not started; "
            "requests that require Redis will fail until Redis recovers."
        )

    logger.info("SPIRIT API ready")
    yield

    # Shutdown: cancel all workers and wait for cleanup
    for task in WORKER_TASKS:
        task.cancel()

    if WORKER_TASKS:
        results = await asyncio.gather(*WORKER_TASKS, return_exceptions=True)
        for task, result in zip(WORKER_TASKS, results):
            if isinstance(result, asyncio.CancelledError):
                logger.info("Worker %s cancelled cleanly", task.get_name())
            elif isinstance(result, Exception):
                logger.error("Worker %s exited with error: %s", task.get_name(), result)
            else:
                logger.info("Worker %s exited normally", task.get_name())

    await close_redis()
    await engine.dispose()
    logger.info("SPIRIT API shut down")


app = FastAPI(title="SPIRIT Clicker API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://spirix.vercel.app",
        "https://websitecard.vercel.app",
        "https://web.telegram.org",
        "https://telegram.org",
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost:5174",
        "http://127.0.0.1:8080",
        "http://127.0.0.1:5174",
    ],
    # Allow Vercel deployments for admin/frontend previews.
    allow_origin_regex=r"^https://[a-zA-Z0-9-]+\.vercel\.app$",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(legacy.router)
app.include_router(admin_legacy_router)
app.include_router(infra_router)
app.include_router(online_router)
app.include_router(online_legacy_router)
app.include_router(daily_reward_router)
app.include_router(tasks_legacy_router)
app.include_router(passive_router)
app.include_router(referrals_legacy_router)
app.include_router(ton_wallet_router)


@app.middleware("http")
async def api_metrics_middleware(request: Request, call_next):
    started_at = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        try:
            observe_http_request(
                request.method,
                request.url.path,
                500,
                time.perf_counter() - started_at,
            )
        except Exception:
            pass
        raise

    try:
        observe_http_request(
            request.method,
            request.url.path,
            response.status_code,
            time.perf_counter() - started_at,
        )
    except Exception:
        pass
    return response


@app.get("/health")
async def health():
    db_ok = await healthcheck_db()
    from infrastructure.redis import get_redis_or_none

    redis_conn = await get_redis_or_none()
    redis_ok = redis_conn is not None

    # Check worker health from Redis heartbeats
    worker_health = {}
    if redis_conn:
        try:
            from workers.worker_health import get_flush_lag, HEALTH_KEY_PREFIX

            lag = await get_flush_lag(redis_conn)
            for worker in ["coins_flush", "referral_flush", "tournament_flush"]:
                hkey = f"{HEALTH_KEY_PREFIX}{worker}"
                hdata = await redis_conn.hgetall(hkey)
                worker_health[worker] = hdata if hdata else {"status": "unknown"}
        except Exception:
            pass

    status = "ok" if (db_ok and redis_ok) else "degraded"
    return {
        "status": status,
        "db": "ok" if db_ok else "error",
        "redis": "ok" if redis_ok else "unavailable",
        "workers": worker_health,
    }
