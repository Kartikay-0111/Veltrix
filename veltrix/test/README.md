# Sample Submission Bundle

This folder contains a reference submission that matches the sandbox contract.

## Submission Contract

- Archive format: `.tar.gz` of this project root.
- Required build file: `CMakeLists.txt` at the root.
- Required binary name: `server`.
- Required listen port: `9999`.
- Required HTTP routes:
  - `POST /order` — submit an order **and** cancels (`{"type":"cancel","order_id":<int>}`)
  - `DELETE /order/{id}` — cancel by id (also accepted, but the grader cancels via `POST /order`)
  - `GET /book/{ticker}`
  - `GET /health`
- Response shape is part of the contract — see `../docs/matching-spec.md`
  ("Wire contract"). Accepted orders return **HTTP 200** with `order_id` +
  `trades[]`; rejected/malformed requests return a non-2xx status.

## Build

```bash
cmake -S . -B build
cmake --build build
./build/server
```

## Runtime Notes

- The server listens on port `9999`.
- `GET /health` returns a status JSON response.
- `GET /book/{ticker}` returns the current order book snapshot.
- `POST /order` accepts order submissions and cancels (`{"type":"cancel","order_id":<int>}`).
- `DELETE /order/{id}` also cancels an existing order.