from fastapi import Depends, FastAPI, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse

from .core.config import settings

APP_DESCRIPTION = (
    "Ingesta y exposición del trackeo de balizas V16 desde eTraffic DGT. "
    "El poller normaliza los eventos y opcionalmente los persiste en Elasticsearch."
)

EXAMPLE_EVENT = {
    "id": "v16-demo-1",
    "latitud": 40.4168,
    "longitud": -3.7038,
    "causa": "Vehículo detenido",
    "tipo": "Advertencia",
    "carretera": "A-5",
    "km": "12.4",
    "provincia": "Madrid",
    "municipio": "Alcorcón",
    "fuente": "DGT3.0",
    "first_seen": "2024-11-20T08:15:00Z",
    "last_seen": "2024-11-20T08:45:00Z",
    "estado": "active",
    "raw": {"situationId": "12345"},
}

EXAMPLE_EVENTS_LIST = [EXAMPLE_EVENT]
HEALTH_EXAMPLE = {"status": "ok", "events_cached": 2, "source": settings.etraffic_endpoint}


def register_docs_routes(app: FastAPI, auth_dependency):
    """Registra rutas /docs y /openapi.json con la dependencia de auth indicada."""

    @app.get("/docs", include_in_schema=False)
    async def custom_swagger(request: Request, _=Depends(auth_dependency)):
        return get_swagger_ui_html(openapi_url="/openapi.json", title="V16 Tracker Docs")

    @app.get("/openapi.json", include_in_schema=False)
    async def custom_openapi(_=Depends(auth_dependency)):
        return JSONResponse(app.openapi())
