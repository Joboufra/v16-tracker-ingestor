import os
from dataclasses import dataclass, field
import json
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_list(value: Optional[str], default: List[str]) -> List[str]:
    if value is None:
        return default
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or default


@dataclass
class Settings:
    etraffic_endpoint: str = os.getenv(
        "ETRAFFIC_ENDPOINT",
        # Endpoint JSON observado en la SPA de eTraffic (llamada XHR).
        "https://etraffic.dgt.es/etrafficWEB/api/cache/getFilteredData",
    )
    etraffic_method: str = os.getenv("ETRAFFIC_METHOD", "POST")
    etraffic_payload: str = os.getenv(
        "ETRAFFIC_PAYLOAD",
        # Payload observado en la SPA para getFilteredData; se puede ajustar si cambia.
        '{"filtrosVia":["Carreteras cortadas","Tráfico lento","Circulación restringida",'
        '"Desvíos y embolsamientos","Otras vialidades"],'
        '"filtrosCausa":["Obras","Accidente","Meteorológicos","Restricciones de circulación","Otras incidencias","Otras afecciones"]}',
    )
    etraffic_xor_key: str = os.getenv("ETRAFFIC_XOR_KEY", "K")
    poller_enabled: bool = _parse_bool(os.getenv("POLLING_ENABLED"), True)
    poller_backoff_base_seconds: int = int(os.getenv("POLLING_BACKOFF_BASE_SECONDS", "5"))
    poller_backoff_max_seconds: int = int(os.getenv("POLLING_BACKOFF_MAX_SECONDS", "60"))
    poll_interval_seconds: int = int(os.getenv("POLL_INTERVAL_SECONDS", "45"))
    stale_after_seconds: int = int(os.getenv("STALE_AFTER_SECONDS", "180"))
    lost_gc_seconds: int = int(os.getenv("LOST_GC_SECONDS", "86400"))
    request_timeout_seconds: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
    rate_limit: str = os.getenv("RATE_LIMIT", "60/minute")
    rate_limit_storage_uri: Optional[str] = os.getenv("RATE_LIMIT_STORAGE_URI")
    elasticsearch_url: str = os.getenv("ELASTICSEARCH_URL", "")
    elasticsearch_index: str = os.getenv("ELASTICSEARCH_INDEX", "v16-events")
    elasticsearch_username: Optional[str] = os.getenv("ELASTICSEARCH_USERNAME")
    elasticsearch_password: Optional[str] = os.getenv("ELASTICSEARCH_PASSWORD")
    elasticsearch_api_key: Optional[str] = os.getenv("ELASTICSEARCH_API_KEY")
    elasticsearch_request_timeout: int = int(os.getenv("ELASTICSEARCH_REQUEST_TIMEOUT", "10"))
    elasticsearch_max_retries: int = int(os.getenv("ELASTICSEARCH_MAX_RETRIES", "3"))
    elasticsearch_retry_on_timeout: bool = _parse_bool(
        os.getenv("ELASTICSEARCH_RETRY_ON_TIMEOUT"), True
    )
    elasticsearch_verify_certs: bool = _parse_bool(
        os.getenv("ELASTICSEARCH_VERIFY_CERTS"), True
    )
    elasticsearch_ca_certs: Optional[str] = os.getenv("ELASTICSEARCH_CA_CERTS")
    elasticsearch_bootstrap_limit: int = int(
        os.getenv("ELASTICSEARCH_BOOTSTRAP_LIMIT", "5000")
    )
    etraffic_timezone: str = os.getenv("ETRAFFIC_TIMEZONE", "Europe/Madrid")
    api_key: Optional[str] = os.getenv("API_KEY")
    api_key_header: str = os.getenv("API_KEY_HEADER", "X-API-Key")
    api_key_required: bool = _parse_bool(os.getenv("API_KEY_REQUIRED"), True)
    trust_x_forwarded_for: bool = _parse_bool(os.getenv("TRUST_X_FORWARDED_FOR"), False)
    api_include_raw: bool = _parse_bool(os.getenv("API_INCLUDE_RAW"), False)
    elasticsearch_allow_insecure: bool = _parse_bool(
        os.getenv("ELASTICSEARCH_ALLOW_INSECURE"), False
    )
    etraffic_allowed_hosts: List[str] = field(
        default_factory=lambda: _parse_list(os.getenv("ETRAFFIC_ALLOWED_HOSTS"), ["etraffic.dgt.es"])
    )
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    def payload_json(self) -> dict:
        try:
            return json.loads(self.etraffic_payload)
        except json.JSONDecodeError:
            return {}

    @property
    def elasticsearch_enabled(self) -> bool:
        return bool(self.elasticsearch_url)

    def __post_init__(self) -> None:
        if self.api_key_required and not self.api_key:
            raise ValueError("API_KEY requerido para arrancar el servicio")

settings = Settings()
