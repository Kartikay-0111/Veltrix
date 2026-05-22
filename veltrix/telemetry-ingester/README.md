# Telemetry Ingester

Small HTTP service that receives bot-fleet telemetry on `POST /metrics` and forwards it to Redpanda.

## Run

```bash
uvicorn main:app --host 0.0.0.0 --port 8090
```

## Endpoints

- `GET /health`
- `POST /metrics`
- `GET /metrics/latest`
