Team submits code
        ↓
Sandbox manager builds image, runs container named "sandbox-{submission_id}"
        ↓
Container joins sandbox-net, gets DNS entry "sandbox-{submission_id}"
        ↓
Sandbox manager POSTs to bot-fleet:
  {
    "target_host": "sandbox-a3f8c2d1",
    "target_port": "9999"
  }
        ↓
Bot fleet fires 10,000 requests to:
  POST http://sandbox-a3f8c2d1:9999/order
  GET  http://sandbox-a3f8c2d1:9999/book/AAPL
        ↓
Artifact checker correctness probe:
  POST http://sandbox-a3f8c2d1:9999/order   ← watermark
  GET  http://sandbox-a3f8c2d1:9999/book/AAPL