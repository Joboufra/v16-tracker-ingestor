import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from .core.config import Settings
from .etraffic import parse_datetime
from .models import EventStatus, V16Event

logger = logging.getLogger("v16-backend.elastic")

_ELASTIC_INDEX_SETTINGS = {
    "mappings": {
        "properties": {
            "estado": {"type": "keyword"},
            "carretera": {"type": "keyword"},
            "km": {"type": "keyword"},
            "causa": {"type": "keyword"},
            "tipo": {"type": "keyword"},
            "provincia": {"type": "keyword"},
            "municipio": {"type": "keyword"},
            "situationId": {"type": "keyword"},
            "fuente": {"type": "keyword"},
            "first_seen": {"type": "date"},
            "last_seen": {"type": "date"},
            "lost_at": {"type": "date"},
            "latitud": {"type": "double"},
            "longitud": {"type": "double"},
            "ubicacion": {"type": "geo_point"},
            "raw": {"type": "object", "enabled": False},
        }
    }
}


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compose_elastic_doc(event: V16Event, lost_at: Optional[datetime]) -> Dict[str, Any]:
    raw = event.raw or {}
    situation_id = str(
        raw.get("situationId") or raw.get("situation_id") or raw.get("id") or event.id
    ).strip()
    doc = {
        "estado": event.estado.value,
        "latitud": event.latitud,
        "longitud": event.longitud,
        "ubicacion": {"lat": event.latitud, "lon": event.longitud},
        "carretera": event.carretera,
        "km": event.km,
        "causa": event.causa,
        "tipo": event.tipo,
        "provincia": event.provincia,
        "municipio": event.municipio,
        "situationId": situation_id,
        "fuente": event.fuente,
        "first_seen": event.first_seen.isoformat(),
        "last_seen": event.last_seen.isoformat(),
        "raw": raw,
    }
    doc["lost_at"] = lost_at.isoformat() if lost_at else None
    return doc


def _elastic_update_action(settings: Settings, event: V16Event, lost_at: Optional[datetime]) -> Dict[str, Any]:
    doc = _compose_elastic_doc(event, lost_at)
    return {
        "_op_type": "update",
        "_index": settings.elasticsearch_index,
        "_id": event.id,
        "doc": doc,
        "doc_as_upsert": True,
    }


def parse_elastic_event(doc_id: str, source: Dict[str, Any], tz_name: str) -> Optional[V16Event]:
    lat = _to_float(source.get("latitud"))
    lon = _to_float(source.get("longitud"))
    if lat is None or lon is None:
        location = source.get("ubicacion") or {}
        lat = _to_float(location.get("lat"))
        lon = _to_float(location.get("lon"))
    if lat is None or lon is None:
        return None
    first_seen = parse_datetime(source.get("first_seen"), tz_name)
    last_seen = parse_datetime(source.get("last_seen"), tz_name)
    if first_seen is None or last_seen is None:
        return None
    first_seen = first_seen.astimezone(timezone.utc)
    last_seen = last_seen.astimezone(timezone.utc)
    status_raw = str(source.get("estado") or EventStatus.active.value)
    try:
        status = EventStatus(status_raw)
    except ValueError:
        status = EventStatus.active
    return V16Event(
        id=doc_id,
        latitud=lat,
        longitud=lon,
        causa=str(source.get("causa") or ""),
        tipo=str(source.get("tipo") or ""),
        carretera=str(source.get("carretera") or ""),
        km=str(source.get("km") or ""),
        provincia=str(source.get("provincia") or ""),
        municipio=str(source.get("municipio") or ""),
        fuente=str(source.get("fuente") or ""),
        first_seen=first_seen,
        last_seen=last_seen,
        raw=source.get("raw") or {},
        estado=status,
    )


async def init_elasticsearch_client(settings: Settings) -> Optional[AsyncElasticsearch]:
    if not settings.elasticsearch_enabled:
        logger.info("Elasticsearch desactivado (ELASTICSEARCH_URL vacio)")
        return None
    parsed = urlparse(settings.elasticsearch_url)
    if parsed.scheme not in {"https", "http"}:
        logger.error("ELASTICSEARCH_URL esquema no soportado (usar http(s))")
        return None
    if parsed.scheme != "https" and not settings.elasticsearch_allow_insecure:
        logger.error(
            "Conexion insegura a Elasticsearch bloqueada (use https o ELASTICSEARCH_ALLOW_INSECURE=true bajo su responsabilidad)"
        )
        return None
    kwargs: Dict[str, Any] = {
        "hosts": [settings.elasticsearch_url],
        "request_timeout": settings.elasticsearch_request_timeout,
        "max_retries": settings.elasticsearch_max_retries,
        "retry_on_timeout": settings.elasticsearch_retry_on_timeout,
        "verify_certs": settings.elasticsearch_verify_certs,
    }
    if settings.elasticsearch_ca_certs:
        kwargs["ca_certs"] = settings.elasticsearch_ca_certs
    if settings.elasticsearch_api_key:
        kwargs["api_key"] = settings.elasticsearch_api_key
    elif settings.elasticsearch_username and settings.elasticsearch_password:
        kwargs["basic_auth"] = (settings.elasticsearch_username, settings.elasticsearch_password)
    client = AsyncElasticsearch(**kwargs)
    try:
        if not await client.ping():
            raise RuntimeError("Ping a Elasticsearch fallido")
        logger.info("Conexion a Elasticsearch establecida")
        await ensure_elasticsearch_index(client, settings)
        return client
    except Exception as exc:
        logger.error("Error inicializando Elasticsearch: %s", exc)
        await client.close()
        return None


async def ensure_elasticsearch_index(client: AsyncElasticsearch, settings: Settings) -> None:
    try:
        exists = await client.indices.exists(index=settings.elasticsearch_index)
        if not exists:
            await client.indices.create(index=settings.elasticsearch_index, **_ELASTIC_INDEX_SETTINGS)
            logger.info("Indice Elasticsearch creado: %s", settings.elasticsearch_index)
        else:
            logger.info("Indice Elasticsearch disponible: %s", settings.elasticsearch_index)
    except Exception as exc:
        logger.error("No se pudo asegurar el índice de Elasticsearch: %s", exc)


async def bootstrap_from_elasticsearch(
    client: AsyncElasticsearch, settings: Settings
) -> Dict[str, V16Event]:
    now = datetime.now(tz=timezone.utc)
    stale_cutoff = now - timedelta(seconds=settings.stale_after_seconds)
    try:
        await client.update_by_query(
            index=settings.elasticsearch_index,
            conflicts="proceed",
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"estado": EventStatus.active.value}},
                            {"range": {"last_seen": {"lt": stale_cutoff.isoformat()}}},
                        ]
                    }
                },
                "script": {
                    "source": "ctx._source.estado='lost'; ctx._source.lost_at=params.lost_at;",
                    "lang": "painless",
                    "params": {"lost_at": now.isoformat()},
                },
            },
        )
    except Exception as exc:
        logger.warning("No se pudo recalcular lost en Elasticsearch: %s", exc)

    try:
        response = await client.search(
            index=settings.elasticsearch_index,
            size=settings.elasticsearch_bootstrap_limit,
            sort=["last_seen:desc"],
        )
        hits = response.get("hits", {}).get("hits", [])
    except Exception as exc:
        logger.warning("No se pudo cargar eventos desde Elasticsearch: %s", exc)
        return {}

    restored: Dict[str, V16Event] = {}
    for hit in hits:
        doc_id = str(hit.get("_id") or "")
        source = hit.get("_source") or {}
        event = parse_elastic_event(doc_id, source, settings.etraffic_timezone)
        if event:
            restored[event.id] = event
    logger.info("Eventos restaurados desde Elasticsearch: %s", len(restored))
    return restored


async def persist_events_to_elastic(
    client: Optional[AsyncElasticsearch],
    settings: Settings,
    active_events: List[V16Event],
    lost_events: List[V16Event],
    lost_at: datetime,
) -> None:
    if not settings.elasticsearch_enabled or client is None:
        return
    actions: List[Dict[str, Any]] = []
    for event in active_events:
        actions.append(_elastic_update_action(settings, event, None))
    for event in lost_events:
        actions.append(_elastic_update_action(settings, event, lost_at))
    if not actions:
        return
    try:
        success, errors = await async_bulk(
            client,
            actions,
            raise_on_error=False,
            stats_only=True,
        )
        if errors:
            logger.warning(
                "Errores al persistir eventos en Elasticsearch (errores=%s, acciones=%s)",
                errors,
                len(actions),
            )
        logger.info("Persistidos en Elasticsearch (bulk): %s acciones", success)
    except Exception as exc:
        logger.warning("No se pudo persistir eventos en Elasticsearch: %s", exc)


async def get_events_from_elasticsearch(
    client: Optional[AsyncElasticsearch], settings: Settings, limit: int
) -> Optional[List[V16Event]]:
    if client is None:
        return None
    try:
        response = await client.search(
            index=settings.elasticsearch_index,
            size=limit,
            sort=["last_seen:desc"],
        )
    except Exception as exc:
        logger.warning("No se pudo leer eventos desde Elasticsearch: %s", exc)
        return None
    hits = response.get("hits", {}).get("hits", [])
    events: List[V16Event] = []
    for hit in hits:
        doc_id = str(hit.get("_id") or "")
        source = hit.get("_source") or {}
        event = parse_elastic_event(doc_id, source, settings.etraffic_timezone)
        if event:
            events.append(event)
    return events


async def get_event_from_elasticsearch(
    client: Optional[AsyncElasticsearch], settings: Settings, event_id: str
) -> Optional[V16Event]:
    if client is None:
        return None
    try:
        response = await client.get(index=settings.elasticsearch_index, id=event_id)
    except Exception as exc:
        logger.warning("No se pudo leer evento %s desde Elasticsearch: %s", event_id, exc)
        return None
    source = response.get("_source") or {}
    return parse_elastic_event(event_id, source, settings.etraffic_timezone)


async def close_elasticsearch_client(client: Optional[AsyncElasticsearch]) -> None:
    if client:
        await client.close()
