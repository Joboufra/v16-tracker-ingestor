# V16 Ingestor (FastAPI)

Servicio de ingesta y normalización de eventos V16 desde eTraffic DGT. El poller consulta periódicamente el endpoint público de eTraffic, filtra los eventos V16 y los mantiene en memoria; opcionalmente los persiste en Elasticsearch. El API expone la caché para consumo interno.

Actualmente este proceso sirve como fuente de datos para los dashboards de mi Kibana: [Ver dashboard](https://kibana.joboufra.es/s/demo/app/dashboards#/view/50fede58-fc2b-4119-a2ee-e7e46cb2f2c7?_g=(filters:!(),refreshInterval:(pause:!t,value:60000),time:(from:'2025-12-31T23:00:00.000Z',to:'2026-12-31T22:59:59.000Z')))

## Cómo funciona
- **Poller**: consulta eTraffic cada `POLL_INTERVAL_SECONDS` con backoff exponencial y jitter. Se puede desactivar (`POLLING_ENABLED=false`).
- **Normalización**: filtra por `fuente=DGT3.0`, `tipo=Advertencia`, `causa=Vehículo detenido`. Calcula IDs estables, marca como `lost` los eventos sin refresco tras `STALE_AFTER_SECONDS` y purga antiguos con `LOST_GC_SECONDS`.
- **Persistencia**: si `ELASTICSEARCH_URL` está definido, escribe/lee en el índice y reconstruye caché en arranque.
- **API**: expone `/health`, `/v16` y `/v16/{id}`. Swagger está disponible en `/docs` (protegido por API key).

## Requisitos
- Python 3.10+
- Acceso HTTP al endpoint eTraffic

## Puesta en marcha rápida
```bash
cd ingestor
python -m venv .venv
source .venv/bin/activate  # En Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Si lo prefieres, copia `./.env.example` a `./.env` y ajusta valores antes de arrancar.

## Configuración (variables de entorno)
Claves principales:
- `ETRAFFIC_ENDPOINT`, `ETRAFFIC_METHOD`, `ETRAFFIC_PAYLOAD`, `ETRAFFIC_XOR_KEY`, `ETRAFFIC_TIMEZONE`
- `ETRAFFIC_ALLOWED_HOSTS` (lista separada por comas de hosts permitidos para `ETRAFFIC_ENDPOINT`)
- `POLLING_ENABLED` (true/false), `POLL_INTERVAL_SECONDS`, `POLLING_BACKOFF_BASE_SECONDS`, `POLLING_BACKOFF_MAX_SECONDS`
- `STALE_AFTER_SECONDS`, `LOST_GC_SECONDS`, `REQUEST_TIMEOUT_SECONDS`
- `RATE_LIMIT` (p.ej. `60/minute`), `RATE_LIMIT_STORAGE_URI` (`memory://` o `redis://host:6379/0` en multi-nodo)
- `API_KEY` (obligatoria por defecto), `API_KEY_HEADER` (por defecto `X-API-Key`)
- `API_KEY_REQUIRED` (pon a `false` solo en desarrollo), `TRUST_X_FORWARDED_FOR` (true si confías en el proxy)
- `API_INCLUDE_RAW` (incluye/excluye `raw` en las respuestas)
- `ELASTICSEARCH_URL` (vacío para desactivar), `ELASTICSEARCH_INDEX`, credenciales (`ELASTICSEARCH_API_KEY` o `ELASTICSEARCH_USERNAME`/`ELASTICSEARCH_PASSWORD`), timeouts/retries/CA
- `ELASTICSEARCH_ALLOW_INSECURE` (solo para entornos sin https)
- `LOG_LEVEL` (INFO por defecto)
