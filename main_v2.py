import os
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from infrastructure.database import engine, AsyncSessionLocal, healthcheck_db
from infrastructure.redis import init_redis, close_redis
from routers import legacy
from workers import click_flush

logger = logging.getLogger(__name__)

BOT_MODE = os.getenv("BOT_MODE", "api")
FLUSH_TASK = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global FLUSH_TASK

    logger.info("Starting SPIRIT API (legacy endpoints)")

    await init_redis()

    if BOT_MODE == "api":
        FLUSH_TASK = asyncio.create_task(click_flush.click_flush_loop())
        logger.info("Background flush worker started")

    logger.info("SPIRIT API ready")
    yield

    if FLUSH_TASK:
        FLUSH_TASK.cancel()
        try:
            await FLUSH_TASK
        except asyncio.CancelledError:
            pass

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


@app.get("/health")
async def health():
    db_ok = await healthcheck_db()
    from infrastructure.redis import get_redis_or_none

    redis_conn = await get_redis_or_none()
    redis_ok = redis_conn is not None

    status = "ok" if (db_ok and redis_ok) else "degraded"
    return {
        "status": status,
        "db": "ok" if db_ok else "error",
        "redis": "ok" if redis_ok else "unavailable",
    }
