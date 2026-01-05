import base64
import binascii
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
from zoneinfo import ZoneInfo

from .core.config import Settings
from .models import EventStatus, V16Event

logger = logging.getLogger("v16-backend.etraffic")

_BASE64_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=")


def _looks_base64(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    return all(ch in _BASE64_CHARS for ch in stripped)


def _decode_xor_base64(payload: str, key: str) -> str:
    if not key:
        raise ValueError("clave XOR vacia")
    text = payload.strip()
    missing_padding = len(text) % 4
    if missing_padding:
        text += "=" * (4 - missing_padding)
    try:
        raw = base64.b64decode(text)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("payload no es base64") from exc
    key_byte = key.encode("utf-8")[0]
    decoded_bytes = bytes(b ^ key_byte for b in raw)
    return decoded_bytes.decode("utf-8")


def _parse_etraffic_payload(response: httpx.Response, xor_key: str) -> Optional[Any]:
    content_type = (response.headers.get("content-type") or "").lower()
    text = response.text or ""

    if "application/json" in content_type:
        try:
            return response.json()
        except ValueError:
            pass

    if text.strip() and ("text/plain" in content_type or _looks_base64(text)):
        try:
            decoded = _decode_xor_base64(text, xor_key)
        except ValueError as exc:
            logger.warning("No se pudo decodificar base64+xor: %s", exc)
        else:
            try:
                return json.loads(decoded)
            except json.JSONDecodeError as exc:
                logger.warning("No se pudo parsear JSON tras decodificar: %s", exc)

    if text.strip():
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Respuesta no JSON (content-type=%s)", content_type)
    return None


def _filter_by_source(payload: Any, source: str) -> Any:
    def _is_match(item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        return str(item.get("fuente") or "").strip() == source

    if isinstance(payload, list):
        return [item for item in payload if _is_match(item)]
    if isinstance(payload, dict):
        filtered = dict(payload)
        for key in ("incidencias", "features", "data", "situationsRecords"):
            if isinstance(filtered.get(key), list):
                filtered[key] = [item for item in filtered[key] if _is_match(item)]
        return filtered
    return payload


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def parse_datetime(value: Any, tz_name: str) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            try:
                parsed = parsed.replace(tzinfo=ZoneInfo(tz_name))
            except Exception:
                parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _extract_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("situationsRecords", "incidencias", "features", "data"):
            if isinstance(payload.get(key), list):
                return [item for item in payload.get(key) if isinstance(item, dict)]
    return []


def _extract_coordinates(raw: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    geometry = raw.get("geometria") or raw.get("geometry")
    if geometry:
        if isinstance(geometry, str):
            try:
                geometry = json.loads(geometry)
            except json.JSONDecodeError:
                geometry = None
        if isinstance(geometry, dict):
            coords = geometry.get("coordinates")
            geom_type = geometry.get("type")
            if geom_type == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
                return _to_float(coords[1]), _to_float(coords[0])
            if geom_type in ("LineString", "MultiPoint") and isinstance(coords, list) and coords:
                first = coords[0]
                if isinstance(first, (list, tuple)) and len(first) >= 2:
                    return _to_float(first[1]), _to_float(first[0])

    lat = _to_float(raw.get("lat") or raw.get("latitud") or raw.get("latitude"))
    lon = _to_float(raw.get("lon") or raw.get("longitud") or raw.get("longitude"))
    return lat, lon


def _event_key(raw: Dict[str, Any], lat: float, lon: float) -> str:
    raw_id = str(raw.get("id") or "").strip()
    if raw_id:
        return raw_id
    road = str(raw.get("carretera") or raw.get("via") or raw.get("road") or "").strip()
    pk = str(
        raw.get("pkIni")
        or raw.get("pkFin")
        or raw.get("pk")
        or raw.get("pK")
        or raw.get("puntoKilometrico")
        or ""
    ).strip()
    cause = str(raw.get("subcausa") or raw.get("causa") or raw.get("causaIncidencia") or "").strip()
    ev_type = str(raw.get("subtipoVialidad") or raw.get("tipo") or raw.get("tipoIncidencia") or "").strip()
    fingerprint = f"{lat:.5f}|{lon:.5f}|{road}|{pk}|{cause}|{ev_type}"
    return hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()


def _is_v16_candidate(raw: Dict[str, Any]) -> bool:
    return (
        str(raw.get("fuente") or "").strip() == "DGT3.0"
        and _normalize_text(raw.get("subtipoVialidad") or raw.get("tipo")) == "advertencia".lower()
        and _normalize_text(raw.get("subcausa") or raw.get("causa")) == "vehículo detenido".lower()
    )


class EtrafficService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: Optional[httpx.AsyncClient] = None

    async def get_client(self) -> httpx.AsyncClient:
        if self.client is None:
            self.client = httpx.AsyncClient(
                timeout=self.settings.request_timeout_seconds,
                follow_redirects=True,
                headers={
                    "Accept": "application/json, text/plain;q=0.9, */*;q=0.8",
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0 (compatible; V16Tracker/0.1)",
                    "Origin": "https://etraffic.dgt.es",
                    "Referer": "https://etraffic.dgt.es/etrafficWEB/",
                },
            )
        return self.client

    async def _fetch_etraffic_response(self) -> httpx.Response:
        parsed = urlparse(self.settings.etraffic_endpoint)
        if parsed.scheme not in {"https", "http"}:
            raise ValueError("ETRAFFIC_ENDPOINT debe ser http(s)")
        host = (parsed.hostname or "").lower()
        allowed_hosts = [h.lower() for h in self.settings.etraffic_allowed_hosts]
        if host not in allowed_hosts:
            raise ValueError("Host de ETRAFFIC_ENDPOINT no permitido")
        client = await self.get_client()
        method = self.settings.etraffic_method.upper()
        if method == "POST":
            return await client.post(parsed.geturl(), json=self.settings.payload_json())
        return await client.get(parsed.geturl())

    async def fetch_payload(self) -> Tuple[Optional[Any], Optional[httpx.Response]]:
        try:
            response = await self._fetch_etraffic_response()
            response.raise_for_status()
            data = _parse_etraffic_payload(response, self.settings.etraffic_xor_key)
            if data is None:
                logger.warning("No se pudo parsear respuesta de %s", self.settings.etraffic_endpoint)
                return None, response
            data = _filter_by_source(data, "DGT3.0")
            return data, response
        except httpx.HTTPStatusError as exc:
            body_preview = exc.response.text[:500] if exc.response else ""
            logger.error(
                "HTTP %s al obtener datos de DGT (%s). Body: %s",
                exc.response.status_code if exc.response else "NA",
                self.settings.etraffic_endpoint,
                body_preview,
            )
            return None, exc.response
        except Exception as exc:
            logger.error("Error al obtener datos de DGT: %s", exc)
            return None, None

    def extract_records(self, payload: Any) -> List[Dict[str, Any]]:
        return _extract_records(payload)

    def parse_event(self, raw: Dict[str, Any], now: datetime) -> Optional[V16Event]:
        lat, lon = _extract_coordinates(raw)
        if lat is None or lon is None:
            return None
        event_id = _event_key(raw, lat, lon)
        started = parse_datetime(raw.get("fechaInicio"), self.settings.etraffic_timezone) or parse_datetime(
            raw.get("fecha_inicio"), self.settings.etraffic_timezone
        )
        if started:
            started = started.astimezone(timezone.utc)
        if started and started > now:
            started = now
        first_seen = started or now
        return V16Event(
            id=event_id,
            latitud=lat,
            longitud=lon,
            causa=str(raw.get("subcausa") or raw.get("causa") or raw.get("causaIncidencia") or ""),
            tipo=str(raw.get("subtipoVialidad") or raw.get("tipo") or raw.get("tipoIncidencia") or ""),
            carretera=str(raw.get("carretera") or raw.get("via") or raw.get("road") or ""),
            km=str(
                raw.get("pkIni")
                or raw.get("pkFin")
                or raw.get("pk")
                or raw.get("pK")
                or raw.get("puntoKilometrico")
                or ""
            ),
            provincia=str(raw.get("provinciaIni") or raw.get("provincia") or raw.get("province") or ""),
            municipio=str(raw.get("municipioIni") or raw.get("municipio") or raw.get("poblacion") or ""),
            fuente=str(raw.get("fuente") or ""),
            first_seen=first_seen,
            last_seen=now,
            raw=raw,
            estado=EventStatus.active,
        )

    def extract_candidates(self, raw_events: List[Dict[str, Any]], now: datetime) -> List[V16Event]:
        candidates: List[V16Event] = []
        for raw in raw_events:
            if not _is_v16_candidate(raw):
                continue
            parsed = self.parse_event(raw, now)
            if parsed:
                candidates.append(parsed)
        return candidates

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None
