from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from ..core.config import settings
import logging

from ..core.security import require_api_key, client_ip
from ..elastic import get_event_from_elasticsearch, get_events_from_elasticsearch
from ..models import V16Event
from ..docs import EXAMPLE_EVENT, EXAMPLE_EVENTS_LIST

limiter = Limiter(
    key_func=lambda request: client_ip(request) or get_remote_address(request),
    default_limits=[settings.rate_limit],
    storage_uri=settings.rate_limit_storage_uri or "memory://",
)

router = APIRouter()
logger = logging.getLogger("v16-backend")


@router.get(
    "/health",
    tags=["meta"],
    summary="Comprobar estado del servicio",
    description="Devuelve estado simple del backend y recuento de eventos cacheados en memoria.",
    responses={200: {"description": "Estado OK", "content": {"application/json": {"example": {"status": "ok", "events_cached": 2}}}}},
)
@limiter.limit("10/minute")
async def health(request: Request, _: None = Depends(require_api_key)) -> Dict[str, Any]:
    async with request.app.state.events_lock:
        count = len(request.app.state.events)
    return {"status": "ok", "events_cached": count, "source": settings.etraffic_endpoint}


@router.get(
    "/v16",
    tags=["v16"],
    summary="Listar balizas V16",
    description="Obtiene el trackeo de balizas V16 desde Elasticsearch si está activo; en caso contrario usa la caché en memoria. Ordenados por `last_seen` descendente.",
    responses={
        200: {
            "description": "Listado de eventos",
            "content": {"application/json": {"example": EXAMPLE_EVENTS_LIST}},
        }
    },
)
@limiter.limit("30/minute")
async def list_events(request: Request, _: None = Depends(require_api_key)) -> List[V16Event]:
    if settings.elasticsearch_enabled and request.app.state.es_client:
        events = await get_events_from_elasticsearch(
            request.app.state.es_client, settings, settings.elasticsearch_bootstrap_limit
        )
        if events is not None:
            return _maybe_strip_raw(events)
    async with request.app.state.events_lock:
        events = list(request.app.state.events.values())
    events.sort(key=lambda evt: evt.last_seen, reverse=True)
    return _maybe_strip_raw(events)


@router.get(
    "/v16/{event_id}",
    tags=["v16"],
    summary="Detalle de una baliza V16",
    description="Busca una baliza por ID en Elasticsearch (si está activo) o en la caché en memoria.",
    responses={
        200: {"description": "Evento encontrado", "content": {"application/json": {"example": EXAMPLE_EVENT}}},
        404: {"description": "Evento no encontrado", "content": {"application/json": {"example": {"detail": "Evento no encontrado"}}}},
    },
)
@limiter.limit("30/minute")
async def get_event(event_id: str, request: Request, _: None = Depends(require_api_key)) -> V16Event:
    if settings.elasticsearch_enabled and request.app.state.es_client:
        event = await get_event_from_elasticsearch(request.app.state.es_client, settings, event_id)
        if event:
            return _maybe_strip_raw([event])[0]
    async with request.app.state.events_lock:
        event = request.app.state.events.get(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Evento no encontrado")
    return _maybe_strip_raw([event])[0]


def _maybe_strip_raw(events: List[V16Event]) -> List[V16Event]:
    if settings.api_include_raw:
        return events
    return [evt.model_copy(update={"raw": {}}) for evt in events]


def register_exception_handlers(app):
    app.state.limiter = limiter
    async def rate_limit_handler(request, exc):
        logger.warning(
            "Rate limit exceeded: %s %s client=%s",
            request.method,
            request.url.path,
            client_ip(request),
        )
        return _rate_limit_exceeded_handler(request, exc)

    app.add_exception_handler(RateLimitExceeded, rate_limit_handler)
