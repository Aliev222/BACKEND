import re
from prometheus_client import Counter, Histogram


API_ENDPOINT_REQUESTS_TOTAL = Counter(
    "api_endpoint_requests_total",
    "API requests by endpoint and status",
    ["method", "endpoint", "status_class", "status_code"],
)

API_ENDPOINT_DURATION_SECONDS = Histogram(
    "api_endpoint_duration_seconds",
    "API endpoint latency",
    ["method", "endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)

API_ENDPOINT_EXPLICIT_ERRORS_TOTAL = Counter(
    "api_endpoint_explicit_errors_total",
    "Explicitly tracked error statuses by endpoint",
    ["method", "endpoint", "status_code"],
)

STORAGE_OPERATION_DURATION_SECONDS = Histogram(
    "storage_operation_duration_seconds",
    "Storage operation duration (Redis/DB)",
    ["store", "operation", "domain", "outcome"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

STORAGE_OPERATION_ERRORS_TOTAL = Counter(
    "storage_operation_errors_total",
    "Storage operation errors",
    ["store", "operation", "domain"],
)

WORKER_LOOP_DURATION_SECONDS = Histogram(
    "worker_loop_duration_seconds",
    "Worker loop/flush duration",
    ["worker", "phase"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)

WORKER_ERRORS_TOTAL = Counter(
    "worker_errors_total",
    "Worker errors",
    ["worker", "phase", "error_type"],
)

WORKER_FLUSHED_ITEMS_TOTAL = Counter(
    "worker_flushed_items_total",
    "Total flushed items by worker",
    ["worker"],
)

_TARGET_ENDPOINTS = {
    ("POST", "/api/clicks"),
    ("GET", "/api/user/{id}"),
    ("POST", "/api/upgrade"),
    ("POST", "/api/upgrade-all"),
    ("POST", "/api/ad-action/start"),
    ("POST", "/api/ads/adsgram/complete"),
    ("POST", "/api/activate-mega-boost"),
    ("POST", "/api/activate-ghost-boost"),
    ("POST", "/api/ads/increment"),
    ("POST", "/api/sync-energy"),
}

_EXPLICIT_STATUSES = {"401", "403", "429", "500"}


def normalize_metrics_path(path: str) -> str:
    normalized = str(path or "/")
    normalized = re.sub(r"/\d{4}-\d{2}-\d{2}(?=/|$)", "/{season_key}", normalized)
    normalized = re.sub(r"/-?\d+(?=/|$)", "/{id}", normalized)
    return normalized


def observe_http_request(
    method: str, path: str, status_code: int, duration_seconds: float
) -> None:
    try:
        safe_method = str(method or "GET").upper()
        endpoint = normalize_metrics_path(path)
        if (safe_method, endpoint) not in _TARGET_ENDPOINTS:
            return

        safe_status_code = int(status_code or 500)
        status_code_label = str(safe_status_code)
        status_class = f"{safe_status_code // 100}xx"

        API_ENDPOINT_DURATION_SECONDS.labels(
            method=safe_method,
            endpoint=endpoint,
        ).observe(max(0.0, float(duration_seconds or 0.0)))

        API_ENDPOINT_REQUESTS_TOTAL.labels(
            method=safe_method,
            endpoint=endpoint,
            status_class=status_class,
            status_code=status_code_label,
        ).inc()

        if status_code_label in _EXPLICIT_STATUSES:
            API_ENDPOINT_EXPLICIT_ERRORS_TOTAL.labels(
                method=safe_method,
                endpoint=endpoint,
                status_code=status_code_label,
            ).inc()
    except Exception:
        return


def observe_storage_timing(
    store: str, operation: str, domain: str, duration_seconds: float, outcome: str = "ok"
) -> None:
    try:
        STORAGE_OPERATION_DURATION_SECONDS.labels(
            store=store,
            operation=operation,
            domain=domain,
            outcome=outcome,
        ).observe(max(0.0, float(duration_seconds or 0.0)))
    except Exception:
        return


def observe_storage_error(store: str, operation: str, domain: str) -> None:
    try:
        STORAGE_OPERATION_ERRORS_TOTAL.labels(
            store=store,
            operation=operation,
            domain=domain,
        ).inc()
    except Exception:
        return


def observe_worker_loop(
    worker: str,
    phase: str,
    duration_seconds: float,
    *,
    error: Exception | str | None = None,
    flushed: int | None = None,
) -> None:
    try:
        WORKER_LOOP_DURATION_SECONDS.labels(worker=worker, phase=phase).observe(
            max(0.0, float(duration_seconds or 0.0))
        )
        if error is not None:
            error_type = error.__class__.__name__ if isinstance(error, Exception) else "error"
            WORKER_ERRORS_TOTAL.labels(
                worker=worker,
                phase=phase,
                error_type=error_type,
            ).inc()
        if flushed and int(flushed) > 0:
            WORKER_FLUSHED_ITEMS_TOTAL.labels(worker=worker).inc(int(flushed))
    except Exception:
        return
