# Test Submission Folder

This folder is a self-contained submission bundle for the orderbook engine.

## Submission contract

- Archive format: `.tar.gz` of this project root
- Root build file: `CMakeLists.txt`
- Binary name: `server`
- Listening port: `9999`
- Required HTTP routes:
  - `POST /order`
  - `DELETE /order/{id}`
  - `GET /book/{ticker}`
  - `GET /health`

## Build

The root `CMakeLists.txt` builds `server` from `main.cpp`.

Example build:

```bash
cmake -S . -B build
cmake --build build
./build/server
```

## Runtime notes

- The service listens on port `9999`.
- `GET /health` returns a basic status JSON response.
- `GET /book/{ticker}` returns the current order book snapshot.
- `POST /order` accepts order submissions.
- `DELETE /order/{id}` cancels an existing order.