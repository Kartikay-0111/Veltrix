# Test Submission Folder

This folder is a self-contained benchmark submission for the orderbook engine.

## Runtime contract

- Build target: Linux container
- Entry point: `main.cpp`
- Listening port: `8080`
- HTTP routes:
  - `POST /order`
  - `DELETE /order/{id}`
  - `GET /book/{ticker}`
  - `GET /health`
- WebSocket route:
  - `ws://host:8080/stream`

## Payloads

### POST /order

Example body:

```json
{
  "ticker": "ABC",
  "side": "buy",
  "price": 100,
  "qty": 25
}
```

### DELETE /order/{id}

Cancels a previously accepted order id.

### GET /book/{ticker}

Returns a JSON snapshot of the top 10 bid and ask levels for that ticker.

## Build

The provided `Dockerfile` compiles the service and starts it on port `8080`.
