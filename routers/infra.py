"""
Infrastructure routes extracted from legacy.py (Patch 7.2).

- GET /metrics - Prometheus metrics export
- GET /health /healthz - liveness-friendly status
- GET /readyz - readiness check (Redis + DB)
"""

import asyncio
import logging

from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy import select

from DATABASE.base import AsyncSessionLocal
from infrastructure.redis import get_redis_or_none
from routers.legacy import REDIS_URL, REDIS_ERRORS, DB_ERRORS

router = APIRouter(tags=["infra"])
logger = logging.getLogger(__name__)


@router.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def _check_redis() -> tuple[bool, str]:
    if not REDIS_URL:
        return True, "skipped"

    try:
        conn = await get_redis_or_none()
        if conn:
            await asyncio.wait_for(conn.ping(), timeout=0.5)
            return True, "ok"
        return False, "unavailable"
    except Exception as e:
        logger.warning(f"Health redis check failed: {e}")
        REDIS_ERRORS.inc()
        return False, f"error: {e}"


async def _check_db() -> tuple[bool, str]:
    try:
        async with AsyncSessionLocal() as session:
            await asyncio.wait_for(session.execute(select(1)), timeout=0.5)
        return True, "ok"
    except Exception as e:
        logger.warning(f"Health db check failed: {e}")
        DB_ERRORS.inc()
        return False, f"error: {e}"


@router.get("/healthz")
async def healthz():
    return {"status": "ok"}


@router.get("/readyz")
async def readyz():
    redis_ok, redis_status = await _check_redis()
    db_ok, db_status = await _check_db()
    ready = redis_ok and db_ok
    return {
        "status": "ready" if ready else "not_ready",
        "checks": {
            "redis": redis_status,
            "db": db_status,
        },
    }


@router.get("/health")
async def health():
    redis_ok, redis_status = await _check_redis()
    db_ok, db_status = await _check_db()

    return {
        "status": "ok" if redis_ok and db_ok else "degraded",
        "details": {
            "redis": redis_status,
            "db": db_status,
        },
    }
