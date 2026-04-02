import os
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from infrastructure.database import engine, AsyncSessionLocal, healthcheck_db
from infrastructure.redis import init_redis, close_redis
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
from workers import referral_flush, tournament_flush, coins_flush

logger = logging.getLogger(__name__)

BOT_MODE = os.getenv("BOT_MODE", "api")
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

    await init_redis()

    if BOT_MODE == "api":
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
        "https://web.telegram.org",
        "http://localhost:3000",
        "http://localhost:8080",
    ],
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
