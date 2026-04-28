import asyncio
import json
import logging
import queue
import sys
from datetime import datetime, timezone
from logging.handlers import QueueHandler, QueueListener
from typing import Any, Dict, Optional

from elasticsearch import Elasticsearch

from .config import Settings


class JsonConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        timestamp = (
            datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        return f"{timestamp} {record.levelname} {record.getMessage()}"


class ElasticsearchLogHandler(logging.Handler):
    def __init__(self, settings: Settings):
        super().__init__()
        self.settings = settings
        self.client: Optional[Elasticsearch] = None
        if settings.elasticsearch_enabled:
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
                kwargs["basic_auth"] = (
                    settings.elasticsearch_username,
                    settings.elasticsearch_password,
                )
            self.client = Elasticsearch(**kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        if self.client is None:
            return
        try:
            document = {
                "@timestamp": (
                    datetime.fromtimestamp(record.created, tz=timezone.utc)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z")
                ),
                "log_level": record.levelname,
                "message": record.getMessage(),
                "app_name": self.settings.app_name,
            }
            self.client.index(index=self.settings.elasticsearch_logs_index, document=document)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        try:
            if self.client is not None:
                self.client.close()
        finally:
            super().close()


_log_listener: Optional[QueueListener] = None


class ExcludeElasticsearchTransportFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "POST https://elastic.joboufra.es:443/logs-apps/_doc" in message:
            return False
        return True


def configure_logging(settings: Settings) -> None:
    global _log_listener

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(JsonConsoleFormatter())

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(-1)
    queue_handler = QueueHandler(log_queue)
    root_logger.addHandler(queue_handler)

    handlers = [console_handler]
    if settings.elasticsearch_enabled:
        elastic_handler = ElasticsearchLogHandler(settings)
        elastic_handler.addFilter(ExcludeElasticsearchTransportFilter())
        handlers.append(elastic_handler)

    _log_listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
    _log_listener.start()

    # Reducir ruido del cliente de transporte de Elasticsearch: solo debug/trace local,
    # y además sus mensajes de indexación de logs quedan filtrados por los handlers.
    logging.getLogger("elastic_transport.transport").setLevel(logging.DEBUG)
    logging.getLogger("elastic_transport.node_pool").setLevel(logging.DEBUG)


def shutdown_logging() -> None:
    global _log_listener
    if _log_listener is not None:
        _log_listener.stop()
        _log_listener = None
