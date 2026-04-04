import os
from urllib.parse import urlparse


class StartupValidationError(RuntimeError):
    pass


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _require_positive_int(env_name: str) -> None:
    raw = os.getenv(env_name)
    if raw is None or str(raw).strip() == "":
        return
    try:
        parsed = int(str(raw).strip())
    except ValueError as e:
        raise StartupValidationError(f"{env_name} must be integer, got: {raw}") from e
    if parsed <= 0:
        raise StartupValidationError(f"{env_name} must be > 0, got: {parsed}")


def validate_startup_config(*, bot_mode: str) -> None:
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        raise StartupValidationError("DATABASE_URL is required")

    parsed_db = urlparse(database_url)
    if not parsed_db.scheme:
        raise StartupValidationError("DATABASE_URL is malformed: missing scheme")

    redis_url = (os.getenv("REDIS_URL") or "").strip()
    require_redis = _as_bool(os.getenv("REQUIRE_REDIS"), default=(bot_mode == "api"))
    if require_redis and not redis_url:
        raise StartupValidationError(
            "REDIS_URL is required for this runtime mode (set REQUIRE_REDIS=0 only for non-critical local runs)"
        )
    if redis_url:
        parsed_redis = urlparse(redis_url)
        if parsed_redis.scheme not in {"redis", "rediss"}:
            raise StartupValidationError(
                f"REDIS_URL must use redis/rediss scheme, got: {parsed_redis.scheme or 'missing'}"
            )

    for env_name in (
        "DB_POOL_SIZE",
        "DB_MAX_OVERFLOW",
        "DB_POOL_RECYCLE",
        "DB_POOL_TIMEOUT",
    ):
        _require_positive_int(env_name)

