import asyncio
import logging
import random
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.middleware import SlowAPIMiddleware

from .core.config import settings
from .core.security import require_api_key, log_requests_middleware
from .api.routes import router as api_router, register_exception_handlers
from .elastic import (
    bootstrap_from_elasticsearch,
    close_elasticsearch_client,
    init_elasticsearch_client,
    persist_events_to_elastic,
)
from .etraffic import EtrafficService
from .models import EventStatus, V16Event
from .docs import APP_DESCRIPTION, register_docs_routes

logger = logging.getLogger("v16-backend")
LOG_LEVEL = getattr(logging, settings.log_level.upper(), logging.INFO)
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.es_client = await init_elasticsearch_client(settings)
    if app.state.es_client:
        restored = await bootstrap_from_elasticsearch(app.state.es_client, settings)
        async with app.state.events_lock:
            app.state.events = restored
    if settings.poller_enabled:
        app.state.poller_task = asyncio.create_task(_polling_worker())
    else:
        logger.info("Poller no iniciado (POLLING_ENABLED=false)")
    register_docs_routes(app, require_api_key)
    try:
        yield
    finally:
        task: Optional[asyncio.Task] = getattr(app.state, "poller_task", None)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await app.state.etraffic.close()
        await close_elasticsearch_client(app.state.es_client)


app = FastAPI(
    title="V16 Tracker",
    version="0.1.0",
    description=APP_DESCRIPTION,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)
register_exception_handlers(app)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=False,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)
app.add_middleware(SlowAPIMiddleware)
log_requests_middleware(app)
app.include_router(api_router)

app.state.events: Dict[str, V16Event] = {}
app.state.events_lock = asyncio.Lock()
app.state.es_client: Optional[AsyncElasticsearch] = None
app.state.etraffic = EtrafficService(settings)


def _gc_events(store: Dict[str, V16Event], now: datetime) -> int:
    cutoff = now - timedelta(seconds=settings.lost_gc_seconds)
    removed = 0
    for eid, evt in list(store.items()):
        if evt.estado == EventStatus.lost and evt.last_seen < cutoff:
            store.pop(eid, None)
            removed += 1
    return removed


async def _refresh_events() -> None:
    now = datetime.now(tz=timezone.utc)
    payload, response = await app.state.etraffic.fetch_payload()
    if payload is None:
        raw_events = []
    else:
        raw_events = app.state.etraffic.extract_records(payload)

    candidates = app.state.etraffic.extract_candidates(raw_events, now)
    active_updates: List[V16Event] = []
    lost_updates: List[V16Event] = []
    async with app.state.events_lock:
        store: Dict[str, V16Event] = app.state.events
        for event in candidates:
            existing = store.get(event.id)
            if existing:
                updated = existing.copy(
                    update={"last_seen": now, "raw": event.raw, "estado": EventStatus.active}
                )
                store[event.id] = updated
            else:
                updated = event
                store[event.id] = updated
            active_updates.append(updated)

        stale_cutoff = now - timedelta(seconds=settings.stale_after_seconds)
        for eid, evt in list(store.items()):
            if evt.estado == EventStatus.active and evt.last_seen < stale_cutoff:
                lost_event = evt.copy(update={"estado": EventStatus.lost})
                store[eid] = lost_event
                lost_updates.append(lost_event)
                logger.info("Evento %s marcado como lost (last_seen=%s)", eid, evt.last_seen)
        removed = _gc_events(store, now)
        if removed:
            logger.info("Eventos perdidos eliminados de la cache: %s", removed)

    await persist_events_to_elastic(app.state.es_client, settings, active_updates, lost_updates, now)

    if candidates:
        logger.info("Actualizados %s eventos V16 (almacenados: %s)", len(candidates), len(app.state.events))


async def _polling_worker() -> None:
    if not settings.poller_enabled:
        logger.info("Poller desactivado por configuración (POLLING_ENABLED=false)")
        return
    await asyncio.sleep(1)  # ligera espera para que el servidor arranque
    backoff = settings.poller_backoff_base_seconds
    while True:
        try:
            await _refresh_events()
            backoff = settings.poller_backoff_base_seconds
            sleep_for = settings.poll_interval_seconds
        except Exception as exc:
            logger.error("Error en poller, reintentando con backoff: %s", exc)
            backoff = min(backoff * 2, settings.poller_backoff_max_seconds)
            sleep_for = backoff
        jitter = random.uniform(0, 1)
        await asyncio.sleep(sleep_for + jitter)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("Excepción no controlada: %s", exc)
    return JSONResponse(status_code=500, content={"detail": "Error interno"})
