# Veltrix Matching-Engine Specification (contestant contract)

A submission is **correct** if, for every order it receives, its trades match a
standard price-time matching engine fed the identical order stream in the same
order. Veltrix verifies this by **golden-model differential replay**: a dedicated
serialized correctness run issues orders one-at-a-time (single writer, fixed
seed), and the artifact-checker replays that exact sequence through a trusted
reference engine and compares, per order, the reference's trades against the
contestant's reported fills.

This document is the contract that comparison enforces. `test/main.cpp` is a
reference implementation that conforms to it.

## Order book

- One independent book per `ticker`.
- **Price-time priority (FIFO):** the best price matches first; among orders at
  the same price, the earliest-arriving resting order matches first.
- A resting **buy** is a bid; a resting **sell** is an ask. An incoming order
  crosses when `best_bid_price ≥ best_ask_price`; matching continues until the
  spread no longer crosses or the incoming order is exhausted.

## Order types (`type` field)

| Type   | Behavior |
|--------|----------|
| LIMIT  | Match against crossing liquidity; **rest** any remainder in the book (good-till-cancel). |
| GFD    | Same as LIMIT within a grading session (good-for-day). |
| MARKET | Match against the best available opposing liquidity at any price; **cancel** any unfilled remainder (never rests). No opposing liquidity ⇒ fills nothing. |
| FOK    | Fill-or-kill: if the order cannot be **fully** filled immediately, it is rejected entirely (no trades, does not rest). |
| FAK    | Fill-and-kill (IOC): match what is immediately available; **cancel** the remainder. |

`CANCEL` removes a resting order by its server-assigned id.

## Execution price

For each fill between an incoming order and a resting maker, the execution price
must lie **within the crossing band**: the inclusive range between the maker's
resting limit price and the incoming order's limit price.

A **MARKET order** has no limit of its own. Standard engines synthesize one at
the *worst opposing price present in the book when the order arrives* (a market
buy is willing to pay up to the highest resting ask; a market sell down to the
lowest resting bid). Its crossing band therefore spans the maker's resting price
and that worst-opposing price — equivalently, a market fill may execute anywhere
within the range of opposing prices resting when the order arrived.

Any convention inside the band is accepted — maker price, aggressor price, or
mid — so a correct engine is never failed for its price-reporting choice. A price
**outside** the band (one no resting order ever offered) is a matching error.

## What the checker verifies (per order)

- **Counterparty** selected by price-time priority: **exact**.
- **Per-fill quantity** and **order-level outcome** (fully filled / rested /
  dropped): **exact**.
- **Execution price**: within the crossing band (tolerant, per above).

Over-fills, under-fills, spurious fills, fills against the wrong counterparty,
filling an order that should have been killed (or vice versa), and out-of-band
prices are all reported as incorrect with a specific reason.

## Wire contract (how the grader talks to your server)

The grader validates your matching logic by reading the HTTP responses your
server returns, so the **response shape is part of the contract**, not just the
routes. Port `9999`; build a binary named `server` from a root `CMakeLists.txt`.

### Routes

| Method + path        | Purpose |
|----------------------|---------|
| `POST /order`        | Submit an order (LIMIT/MARKET/FOK/FAK/GFD) **and** cancels. |
| `DELETE /order/{id}` | Cancel a resting order by server id (also accepted). |
| `GET /book/{ticker}` | Order-book snapshot. |
| `GET /health`        | Liveness — returns a JSON status. |

**Cancels:** the grader issues cancels as `POST /order` with body
`{"type":"cancel","order_id":<int>}` — **not** `DELETE`. Your server must accept
this form. `DELETE /order/{id}` is also accepted by the reference, but it is not
the path the grader exercises; implement the POST-body cancel.

### Request body (`POST /order`)

```json
{"order_id":"<client-string>","type":"LIMIT","ticker":"AAPL","side":"BUY","price":100.00,"quantity":10}
```

`type` ∈ `LIMIT | MARKET | FOK | FAK | GFD | cancel`. MARKET omits `price`.
`quantity` (or `qty`) is an integer. The client `order_id` is informational — your
server assigns its own id and returns it (below).

### Response body — REQUIRED for grading

An order your server **accepts and processes** must return **HTTP 200** with a
JSON body containing:

```json
{"order_id":<int>,"ticker":"AAPL","trades":[
  {"buy_order_id":<int>,"sell_order_id":<int>,"price":<num>,"qty":<int>}
]}
```

- `order_id` — the **server-assigned integer id** for this order. The grader maps
  counterparties by this id, so it must be present and numeric on every accepted
  order (including pure rests and kills).
- `trades` — one entry per fill this order produced, an **empty `[]`** when the
  order rested, was killed (FOK), or hit an empty book (MARKET). Each entry names
  both sides by server id (`buy_order_id` / `sell_order_id`), the execution
  `price`, and `qty` (or `quantity`).

A **rejected or malformed** request must return a **non-2xx** status (the
reference returns 400 for a bad payload, 404 for cancelling an unknown id). The
grader treats a non-2xx response as *the order was not processed* — neither your
server nor the golden model applies it. **Do not** return 201/202/204 for a
*successful* order, or use different trade field names: the grader would drop that
order from the replay and could misjudge later matches against it. If a 200
response cannot be parsed, the run is reported **unverified** (below), never a
pass — but conform to this shape so your run is graded, not skipped.

## Verdict states

Each submission is graded into one of three states:

| Verdict | Meaning |
|---------|---------|
| **correct**    | The full stream replayed and every order agreed with the golden model. |
| **incorrect**  | A real matching divergence, with a specific reason (over/under-fill, wrong counterparty, out-of-band price, a killed order that filled, …). |
| **unverified** | The run could **not** be conclusively checked — the end-of-run marker never arrived (truncated stream), or a fill referenced a counterparty/aggressor that was never captured (lost telemetry or an unrecognized response shape). Unverified is **not** a pass and **not** a failure; it is surfaced so the run can be retried. It is also the fail-safe **default**: a submission that was never verified never silently reads as correct. |
