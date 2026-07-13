package models

import "encoding/json"

// OrderEvent is the canonical execution-log event consumed from Redpanda.
//
// EventTimestamp is epoch time in microseconds. It is the event-time clock used
// by the watermark processor to rebuild the globally ordered stream before the
// shadow matching engine validates price-time correctness.
type OrderEvent struct {
	SubmissionID   string  `json:"submission_id"`
	EventTimestamp int64   `json:"event_timestamp"`
	OrderID        string  `json:"order_id"`
	Action         string  `json:"action"`
	Price          float64 `json:"price"`
	Volume         int     `json:"volume"`

	// Ticker names the order book this event belongs to. The golden-model replay
	// engine keeps one book per ticker, so this must be present on every intent
	// and fill.
	Ticker string `json:"ticker,omitempty"`

	// OrderType is the matching semantic carried from the bot's intent:
	// LIMIT | MARKET | FOK | FAK | GFD (CANCEL is signalled via Action). The
	// producer historically overwrote this with the side; it is now carried
	// explicitly because the golden model matches each type differently.
	OrderType string `json:"order_type,omitempty"`

	// Seq is a monotonic per-submission sequence number stamped by the single
	// correctness-run writer at send time. It defines the exact order the golden
	// model replays and doubles as the idempotency key for dedup.
	Seq uint64 `json:"seq,omitempty"`

	// Server-assigned identifiers. ContestantOrderID is the id the contestant
	// engine assigned to this order (on an intent) or to the aggressor (on a
	// fill); MatchedOrderID is the resting maker's server id (on a fill);
	// CancelTargetID is the server id a CANCEL intent targets. These map the
	// contestant's id namespace back to bot order_ids for counterparty checks
	// and cancel replay.
	ContestantOrderID uint64  `json:"contestant_order_id,omitempty"`
	MatchedOrderID    uint64  `json:"matched_order_id,omitempty"`
	CancelTargetID    uint64  `json:"cancel_target_id,omitempty"`
	ExecutionPrice    float64 `json:"execution_price,omitempty"`

	// AggressorOrderID is the bot-generated order_id of the aggressing order that
	// produced this fill. It is the join key tying a FILL event back to the
	// OrderSubmitted intent that caused it. Only present on FILL events.
	AggressorOrderID string `json:"aggressor_order_id,omitempty"`

	// EndOfRun marks the sentinel event the correctness-run writer emits when the
	// serialized order stream is complete, letting the engine finalize the verdict.
	EndOfRun bool `json:"end_of_run,omitempty"`

	// Outcome is the attempt result the bot stamps on every intent so a lost or
	// rejected response is never a silent hole in the seq stream:
	//   "" / "OK"   — clean 200, the server applied the order (normal replay).
	//   "REJECTED"  — clean 4xx, the server rejected it; the book is unchanged, so
	//                 the golden model treats it as a no-op (seq stays contiguous).
	//   "UNKNOWN"   — timeout / 5xx / parse error; the server may have applied it,
	//                 so the book cannot be trusted → the whole run is Unverified.
	// Only present on intents (BUY/SELL/CANCEL), never on FILL events.
	Outcome string `json:"outcome,omitempty"`
}

func (event *OrderEvent) UnmarshalJSON(data []byte) error {
	type orderEventJSON struct {
		SubmissionID      string  `json:"submission_id"`
		EventTimestamp    int64   `json:"event_timestamp"`
		Timestamp         int64   `json:"timestamp"`
		OrderID           string  `json:"order_id"`
		Action            string  `json:"action"`
		Price             float64 `json:"price"`
		Volume            int     `json:"volume"`
		Quantity          int     `json:"quantity"`
		Ticker            string  `json:"ticker"`
		OrderType         string  `json:"order_type"`
		Seq               uint64  `json:"seq"`
		ContestantOrderID uint64  `json:"contestant_order_id"`
		MatchedOrderID    uint64  `json:"matched_order_id"`
		CancelTargetID    uint64  `json:"cancel_target_id"`
		ExecutionPrice    float64 `json:"execution_price"`
		AggressorOrderID  string  `json:"aggressor_order_id"`
		EndOfRun          bool    `json:"end_of_run"`
		Outcome           string  `json:"outcome"`
	}

	var decoded orderEventJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}

	event.SubmissionID = decoded.SubmissionID
	event.EventTimestamp = normalizeEpochMicros(decoded.EventTimestamp, decoded.Timestamp)
	event.OrderID = decoded.OrderID
	event.Action = decoded.Action
	event.Price = decoded.Price
	event.Volume = decoded.Volume
	if event.Volume == 0 {
		event.Volume = decoded.Quantity
	}
	event.Ticker = decoded.Ticker
	event.OrderType = decoded.OrderType
	event.Seq = decoded.Seq
	event.ContestantOrderID = decoded.ContestantOrderID
	event.MatchedOrderID = decoded.MatchedOrderID
	event.CancelTargetID = decoded.CancelTargetID
	event.ExecutionPrice = decoded.ExecutionPrice
	event.AggressorOrderID = decoded.AggressorOrderID
	event.EndOfRun = decoded.EndOfRun
	event.Outcome = decoded.Outcome

	return nil
}

// MetricsBatch is a pre-aggregated latency/throughput batch emitted by one
// load-generator thread for a single contestant submission.
type MetricsBatch struct {
	SubmissionID string `json:"submission_id"`
	ThreadID     int    `json:"thread_id"`
	TotalReqs    int    `json:"total_reqs"`
	Http200      int    `json:"http_200"`
	Histogram    []int  `json:"histogram"`
}

func (batch *MetricsBatch) UnmarshalJSON(data []byte) error {
	type metricsBatchJSON struct {
		SubmissionID string `json:"submission_id"`
		ThreadID     int    `json:"thread_id"`
		TotalReqs    int    `json:"total_reqs"`
		Samples      int    `json:"samples"`
		Http200      int    `json:"http_200"`
		Histogram    []int  `json:"histogram"`
		Hist         []int  `json:"hist"`
	}

	var decoded metricsBatchJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}

	batch.SubmissionID = decoded.SubmissionID
	batch.ThreadID = decoded.ThreadID
	batch.TotalReqs = decoded.TotalReqs
	if batch.TotalReqs == 0 {
		batch.TotalReqs = decoded.Samples
	}
	batch.Http200 = decoded.Http200
	batch.Histogram = decoded.Histogram
	if batch.Histogram == nil {
		batch.Histogram = decoded.Hist
	}

	return nil
}

// Verdict is the tri-state outcome of correctness validation.
//
// Unverified is the fail-safe default and the whole point of a third state: a
// submission that was never conclusively checked — no end-of-run marker, a
// truncated event stream, or a fill whose counterparty could not be mapped —
// is neither correct nor incorrect. It must never silently read as correct
// (that was the original fail-open bug) nor wrongly read as incorrect (that
// would violate the paramount "never fail correct code" constraint). Only an
// engine that fully replays to a clean agreement is Correct.
type Verdict string

const (
	VerdictUnverified Verdict = "unverified"
	VerdictCorrect    Verdict = "correct"
	VerdictIncorrect  Verdict = "incorrect"
)

// CorrectnessUpdate is emitted by the replay engine when a submission's verdict
// is finalized.
type CorrectnessUpdate struct {
	SubmissionID string
	Verdict      Verdict
	Reason       string
}

// Score is the composite benchmark state flushed to Redis and TimescaleDB.
type Score struct {
	SubmissionID string
	TeamName     string
	TPS          int
	P50Ms        float64
	P90Ms        float64
	P99Ms        float64
	P99Bucket    int
	Verdict      Verdict
}

func normalizeEpochMicros(eventTimestamp, alternateTimestamp int64) int64 {
	timestamp := eventTimestamp
	if timestamp == 0 {
		timestamp = alternateTimestamp
	}

	// Bot-generated request timestamps in the existing codebase are epoch
	// nanoseconds. The artifact checker watermark contract is epoch
	// microseconds, so normalize obvious nanosecond values at the boundary.
	if timestamp > 10_000_000_000_000_000 {
		return timestamp / 1_000
	}

	return timestamp
}
