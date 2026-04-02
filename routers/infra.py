"""
Infrastructure routes extracted from legacy.py (Patch 7.2).

- GET /metrics — Prometheus metrics export
- GET /health — health check (Redis + DB)
"""

import asyncio
import logging

from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from sqlalchemy import select

from DATABASE.base import AsyncSessionLocal
from infrastructure.redis import get_redis_or_none
from routers.legacy import REDIS_URL, REDIS_ERRORS, DB_ERRORS

router = APIRouter(tags=["infra"])
logger = logging.getLogger(__name__)


@router.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/health")
async def health():
    details: dict[str, str] = {}
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
