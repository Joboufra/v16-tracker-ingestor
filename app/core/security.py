import time
import uuid
import logging
from itertools import count
from typing import Optional, Callable, Awaitable

from fastapi import Depends, HTTPException, Request
from fastapi.security import APIKeyHeader

from .config import settings

logger = logging.getLogger("v16-backend")

api_key_header = APIKeyHeader(name=settings.api_key_header, auto_error=False)


def require_api_key(api_key: Optional[str] = Depends(api_key_header)) -> None:
    """Dependency for protected endpoints; fuerza API_KEY salvo que esté deshabilitada explícitamente."""
    if not settings.api_key:
        if settings.api_key_required:
            # Settings ya valida en __post_init__, esto es un guardia adicional
            raise HTTPException(status_code=401, detail="Unauthorized")
        return
    if not api_key or api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _forwarded_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if not forwarded_for:
        return ""
    # Se queda con el primer IP declarado (cliente original)
    return forwarded_for.split(",")[0].strip()


def client_ip(request: Request) -> str:
    if settings.trust_x_forwarded_for:
        ip = _forwarded_client_ip(request)
        if ip:
            return ip
    return request.client.host if request.client else ""


def log_requests_middleware(app):
    request_counter = count(1)

    @app.middleware("http")
    async def log_requests(request: Request, call_next: Callable[[Request], Awaitable]):
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        start_time = time.time()
        try:
            response = await call_next(request)
        except Exception:
            logger.exception(
                "Request failed",
                extra={
                    "request_id": request_id,
                    "path": request.url.path,
                    "method": request.method,
                },
            )
            raise
        duration_ms = (time.time() - start_time) * 1000
        req_num = next(request_counter)
        client = client_ip(request)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "REQ#%d %s %s -> %s (%.2fms) client=%s rid=%s",
            req_num,
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            client,
            request_id,
        )
        return response

    return log_requests
