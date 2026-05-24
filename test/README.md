# Test Submission Folder

This folder is a self-contained benchmark submission for the orderbook engine.

## Runtime contract

- Build target: Linux container
- Entry point: `main.cpp`
- Listening port: `9999`
- HTTP routes:
  - `POST /order`
  - `DELETE /order/{id}`
  - `GET /book/{ticker}`
  - `GET /health`
- WebSocket route:
  - `ws://host:9999/stream`

## Payloads

### POST /order

Accepted order types: `LIMIT` (default), `MARKET`, `FOK`, `FAK`, `GTC`, `GFD`, `CANCEL`.

Field notes:
- `side` is case-insensitive (`buy`/`sell` or `BUY`/`SELL`).
- `quantity` or `qty` is accepted.
- `price` accepts integers or decimals; it is rounded to the nearest tick.
- `type`/`order_type` are optional; missing means `LIMIT` (GTC).

Example (LIMIT):

```json
{
  "type": "LIMIT",
  "ticker": "ABC",
  "side": "BUY",
  "price": 100.00,
  "quantity": 25
}
```

Example (MARKET):

```json
{
  "type": "MARKET",
  "ticker": "ABC",
  "side": "SELL",
  "quantity": 10
}
```

Example (CANCEL via POST):

```json
{
  "type": "CANCEL",
  "order_id": 12345
}
```

### DELETE /order/{id}

Cancels a previously accepted order id.

### GET /book/{ticker}

Returns a JSON snapshot of the top 10 bid and ask levels for that ticker.

## Build

The provided `Dockerfile` compiles the service and starts it on port `9999`.
