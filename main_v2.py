import os
import logging
import asyncio
from contextlib import asynccontextmanager
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from infrastructure.database import engine, AsyncSessionLocal, healthcheck_db
from infrastructure.redis import init_redis, close_redis
from routers import (
    auth,
    clicks,
    user,
    economy,
    tournament,
    ads,
    skins,
    tasks,
    ton,
    referrals,
    admin,
    online,
)
from workers import click_flush

logger = logging.getLogger(__name__)

BOT_MODE = os.getenv("BOT_MODE", "api")

FLUSH_TASK = None

# Маппинг старых путей (/api/...) на новые (/api/v2/...)
# Фронт шлёт старые пути — middleware автоматически переписывает
OLD_TO_NEW_PATHS = {
    "/api/auth/session": "/api/v2/auth/session",
    "/api/register": "/api/v2/user",
    "/api/clicks": "/api/v2/clicks",
    "/api/sync-energy": "/api/v2/energy/sync",
    "/api/update-energy": "/api/v2/energy/sync",
    "/api/upgrade": "/api/v2/upgrade",
    "/api/upgrade-all": "/api/v2/upgrade",
    "/api/passive-income": "/api/v2/upgrade",
    "/api/ad-action/start": "/api/v2/ads/start",
    "/api/ads/adsgram/complete": "/api/v2/ads/complete",
    "/api/ads/increment": "/api/v2/ads/complete",
    "/api/activate-mega-boost": "/api/v2/boost/mega",
    "/api/activate-ghost-boost": "/api/v2/boost/ghost",
    "/api/autoclicker/activate": "/api/v2/boost/autoclicker",
    "/api/select-skin": "/api/v2/skins/select",
    "/api/unlock-skin": "/api/v2/skins/unlock-level",
    "/api/skins/stars-invoice": "/api/v2/skins/stars-invoice",
    "/api/complete-task": "/api/v2/tasks/complete",
    "/api/daily-reward/claim": "/api/v2/daily-reward/claim",
    "/api/ton/wallet/connect": "/api/v2/ton/connect",
    "/api/ton/wallet/disconnect": "/api/v2/ton/disconnect",
    "/api/online/heartbeat": "/api/v2/online/heartbeat",
    "/api/online/count": "/api/v2/online/count",
}


class PathRewriteMiddleware(BaseHTTPMiddleware):
    """Переписывает старые /api/... пути на новые (/api/v2/...)"""

    async def dispatch(self, request: StarletteRequest, call_next):
        path = request.url.path

        # Уже /api/v2/ или /health — пропускаем
        if path.startswith("/api/v2/") or path == "/health":
            return await call_next(request)

        # Точное совпадение
        if path in OLD_TO_NEW_PATHS:
            request.scope["path"] = OLD_TO_NEW_PATHS[path]
            request.scope["raw_path"] = OLD_TO_NEW_PATHS[path].encode()
            return await call_next(request)

        # ВАЖНО: более специфичные паттерны — первыми!

        # /api/ton/wallet/proof-payload/{id} → /api/v2/ton/proof
        if path.startswith("/api/ton/wallet/proof-payload/"):
            request.scope["path"] = "/api/v2/ton/proof"
            return await call_next(request)

        # /api/ton/wallet/connect, disconnect
        if path.startswith("/api/ton/wallet/connect"):
            request.scope["path"] = "/api/v2/ton/connect"
            return await call_next(request)
        if path.startswith("/api/ton/wallet/disconnect"):
            request.scope["path"] = "/api/v2/ton/disconnect"
            return await call_next(request)

        # /api/ton/wallet/{id} → /api/v2/ton/wallet
        if path.startswith("/api/ton/wallet/"):
            request.scope["path"] = "/api/v2/ton/wallet"
            return await call_next(request)

        # /api/weekly-tournament/results/{league} → /api/v2/tournament/weekly/league/{league}
        if path.startswith("/api/weekly-tournament/results/"):
            league = path.split("/")[-1]
            request.scope["path"] = f"/api/v2/tournament/weekly/league/{league}"
            return await call_next(request)

        # /api/weekly-tournament/leaderboard/{league} → /api/v2/tournament/weekly/league/{league}
        if path.startswith("/api/weekly-tournament/leaderboard/"):
            league = path.split("/")[-1]
            request.scope["path"] = f"/api/v2/tournament/weekly/league/{league}"
            return await call_next(request)

        # /api/weekly-tournament/overview/{id} → /api/v2/tournament/weekly
        if path.startswith("/api/weekly-tournament/overview/"):
            request.scope["path"] = "/api/v2/tournament/weekly"
            return await call_next(request)

        # /api/video-tasks/claim → /api/v2/tasks/complete
        if path == "/api/video-tasks/claim":
            request.scope["path"] = "/api/v2/tasks/complete"
            return await call_next(request)

        # /api/video-tasks/status/{id} → /api/v2/tasks
        if path.startswith("/api/video-tasks/status/"):
            request.scope["path"] = "/api/v2/tasks"
            return await call_next(request)

        # /api/ghost-boost-status/{id} → /api/v2/boost/ghost
        if path.startswith("/api/ghost-boost-status/"):
            request.scope["path"] = "/api/v2/boost/ghost"
            return await call_next(request)

        # /api/mega-boost-status/{id} → /api/v2/boost/mega
        if path.startswith("/api/mega-boost-status/"):
            request.scope["path"] = "/api/v2/boost/mega"
            return await call_next(request)

        # /api/daily-reward/status/{id} → /api/v2/daily-reward
        if path.startswith("/api/daily-reward/status/"):
            request.scope["path"] = "/api/v2/daily-reward"
            return await call_next(request)

        # /api/upgrade-prices/{id} → /api/v2/upgrade-prices
        if path.startswith("/api/upgrade-prices/"):
            request.scope["path"] = "/api/v2/upgrade-prices"
            return await call_next(request)

        # /api/referral-data/{id} → /api/v2/referrals
        if path.startswith("/api/referral-data/"):
            request.scope["path"] = "/api/v2/referrals"
            return await call_next(request)

        # /api/tasks/{id} → /api/v2/tasks
        if path.startswith("/api/tasks/"):
            request.scope["path"] = "/api/v2/tasks"
            return await call_next(request)

        # /api/user/{id} → /api/v2/user
        if path.startswith("/api/user/"):
            request.scope["path"] = "/api/v2/user"
            return await call_next(request)

        # /api/admin/... → /api/v2/admin/...
        if path.startswith("/api/admin/"):
            request.scope["path"] = path.replace("/api/admin/", "/api/v2/admin/", 1)
            return await call_next(request)

        # Мини-игры — удалены, возвращаем 404
        if path.startswith("/api/game/"):
            from fastapi.responses import JSONResponse

            return JSONResponse(
                status_code=404, content={"detail": "Mini-games removed"}
            )

        return await call_next(request)

        # Точное совпадение
        if path in OLD_TO_NEW_PATHS:
            request.scope["path"] = OLD_TO_NEW_PATHS[path]
            request.scope["raw_path"] = OLD_TO_NEW_PATHS[path].encode()
            return await call_next(request)

        # Паттерн: /api/user/{id} → /api/v2/user
        if path.startswith("/api/user/"):
            request.scope["path"] = "/api/v2/user"
            return await call_next(request)

        # Паттерн: /api/upgrade-prices/{id} → /api/v2/upgrade-prices
        if path.startswith("/api/upgrade-prices/"):
            request.scope["path"] = "/api/v2/upgrade-prices"
            return await call_next(request)

        # Паттерн: /api/ghost-boost-status/{id} → /api/v2/boost/ghost
        if path.startswith("/api/ghost-boost-status/"):
            request.scope["path"] = "/api/v2/boost/ghost"
            return await call_next(request)

        # Паттерн: /api/mega-boost-status/{id} → /api/v2/boost/mega
        if path.startswith("/api/mega-boost-status/"):
            request.scope["path"] = "/api/v2/boost/mega"
            return await call_next(request)

        # Паттерн: /api/daily-reward/status/{id} → /api/v2/daily-reward
        if path.startswith("/api/daily-reward/status/"):
            request.scope["path"] = "/api/v2/daily-reward"
            return await call_next(request)

        # Паттерн: /api/tasks/{id} → /api/v2/tasks
        if path.startswith("/api/tasks/"):
            request.scope["path"] = "/api/v2/tasks"
            return await call_next(request)

        # Паттерн: /api/video-tasks/status/{id} → /api/v2/tasks
        if path.startswith("/api/video-tasks/status/"):
            request.scope["path"] = "/api/v2/tasks"
            return await call_next(request)

        # Паттерн: /api/video-tasks/claim → /api/v2/tasks/complete
        if path == "/api/video-tasks/claim":
            request.scope["path"] = "/api/v2/tasks/complete"
            return await call_next(request)

        # Паттерн: /api/referral-data/{id} → /api/v2/referrals
        if path.startswith("/api/referral-data/"):
            request.scope["path"] = "/api/v2/referrals"
            return await call_next(request)

        # Паттерн: /api/ton/wallet/{id} → /api/v2/ton/wallet
        if (
            path.startswith("/api/ton/wallet/")
            and not path.startswith("/api/ton/wallet/connect")
            and not path.startswith("/api/ton/wallet/disconnect")
        ):
            request.scope["path"] = "/api/v2/ton/wallet"
            return await call_next(request)

        # Паттерн: /api/ton/wallet/proof-payload/{id} → /api/v2/ton/proof
        if path.startswith("/api/ton/wallet/proof-payload/"):
            request.scope["path"] = "/api/v2/ton/proof"
            return await call_next(request)

        # Паттерн: /api/weekly-tournament/results/{league} → /api/v2/tournament/weekly/league/{league}
        if path.startswith("/api/weekly-tournament/results/"):
            league = path.split("/")[-1]
            request.scope["path"] = f"/api/v2/tournament/weekly/league/{league}"
            return await call_next(request)

        # Паттерн: /api/weekly-tournament/overview/{id} → /api/v2/tournament/weekly
        if path.startswith("/api/weekly-tournament/overview/"):
            request.scope["path"] = "/api/v2/tournament/weekly"
            return await call_next(request)

        # Паттерн: /api/weekly-tournament/leaderboard/{league} → /api/v2/tournament/weekly/league/{league}
        if path.startswith("/api/weekly-tournament/leaderboard/"):
            league = path.split("/")[-1]
            request.scope["path"] = f"/api/v2/tournament/weekly/league/{league}"
            return await call_next(request)

        # Паттерн: /api/admin/... → /api/v2/admin/...
        if path.startswith("/api/admin/"):
            request.scope["path"] = path.replace("/api/admin/", "/api/v2/admin/", 1)
            return await call_next(request)

        # Мини-игры — удалены, возвращаем 404
        if path.startswith("/api/game/"):
            from fastapi.responses import JSONResponse

            return JSONResponse(
                status_code=404, content={"detail": "Mini-games removed"}
            )

        return await call_next(request)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global FLUSH_TASK

    logger.info("Starting SPIRIT API v2")

    # Tables already exist on Render PostgreSQL — skip init_db()
    logger.info("Skipping init_db — tables exist on Render")

    await init_redis()

    if BOT_MODE == "api":
        FLUSH_TASK = asyncio.create_task(click_flush.click_flush_loop())
        logger.info("Background flush worker started")

    logger.info("SPIRIT API v2 ready")
    yield

    if FLUSH_TASK:
        FLUSH_TASK.cancel()
        try:
            await FLUSH_TASK
        except asyncio.CancelledError:
            pass

    await close_redis()
    await engine.dispose()
    logger.info("SPIRIT API v2 shut down")


app = FastAPI(title="SPIRIT Clicker API v2", lifespan=lifespan)

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

app.add_middleware(PathRewriteMiddleware)

app.include_router(auth.router)
app.include_router(clicks.router)
app.include_router(user.router)
app.include_router(economy.router)
app.include_router(tournament.router)
app.include_router(ads.router)
app.include_router(skins.router)
app.include_router(tasks.router)
app.include_router(ton.router)
app.include_router(referrals.router)
app.include_router(admin.router)
app.include_router(online.router)


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
