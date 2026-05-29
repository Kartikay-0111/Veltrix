# Sample Submission Bundle

This folder contains a reference submission that matches the sandbox contract.

## Submission Contract

- Archive format: `.tar.gz` of this project root.
- Required build file: `CMakeLists.txt` at the root.
- Required binary name: `server`.
- Required listen port: `9999`.
- Required HTTP routes:
  - `POST /order`
  - `DELETE /order/{id}`
  - `GET /book/{ticker}`
  - `GET /health`

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
- `POST /order` accepts order submissions.
- `DELETE /order/{id}` cancels an existing order.