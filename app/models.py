from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict


class EventStatus(str, Enum):
    active = "active"
    lost = "lost"


class V16Event(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
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
                "raw": {
                    "situationId": "12345",
                    "sentido": "positive",
                    "orientacion": "westBound",
                    "fechaInicio": "2024-11-20T08:15:00Z",
                },
            }
        }
    )

    id: str
    latitud: float
    longitud: float
    causa: str
    tipo: str
    carretera: Optional[str] = None
    km: Optional[str] = None
    provincia: Optional[str] = None
    municipio: Optional[str] = None
    fuente: Optional[str] = None
    first_seen: datetime
    last_seen: datetime
    raw: Dict[str, Any]
    estado: EventStatus = EventStatus.active
