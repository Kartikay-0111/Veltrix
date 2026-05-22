from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    telemetry_bind_host: str = "0.0.0.0"
    telemetry_bind_port: int = 8090
    redpanda_proxy_url: str = "http://redpanda:8082"
    redpanda_topic: str = "order_metrics"


class MetricEnvelope(BaseModel):
    ts: int
    submission_id: str
    thread_id: int
    http_200: int = 0
    http_4xx: int = 0
    http_5xx: int = 0
    timeout: int = 0
    econnref: int = 0
    other_err: int = 0
    avg_latency_ms: float = 0.0
    samples: int = 0
    hist: list[int]


cfg = Settings()
app = FastAPI(title="Veltrix Telemetry Ingester")
last_metric: dict[str, Any] | None = None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/metrics")
def ingest_metrics(metric: MetricEnvelope) -> dict[str, str]:
    global last_metric
    payload = metric.model_dump()
    last_metric = payload

    record = {"records": [{"key": metric.submission_id, "value": payload}]}
    url = f"{cfg.redpanda_proxy_url}/topics/{cfg.redpanda_topic}"

    try:
        response = httpx.post(
            url,
            json=record,
            headers={
                "Content-Type": "application/vnd.kafka.json.v2+json",
                "Accept": "application/vnd.kafka.v2+json",
            },
            timeout=5.0,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Forwarding failed: {exc}") from exc

    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Redpanda proxy error: {response.text}")

    return {"status": "accepted"}

@app.get("/metrics/latest")
def latest_metric() -> dict[str, Any]:
    if last_metric is None:
        raise HTTPException(status_code=404, detail="No metrics received yet")
    return last_metric