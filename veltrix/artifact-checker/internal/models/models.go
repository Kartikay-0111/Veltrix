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

	// Optional execution-log fields. The base architecture only requires the
	// fields above, but real checker streams often include the resting order and
	// execution price selected by the contestant engine. When present, the shadow
	// engine uses these to catch "worse than top-of-book" and FIFO violations.
	MatchedOrderID string  `json:"matched_order_id,omitempty"`
	ExecutionPrice float64 `json:"execution_price,omitempty"`

	// AggressorOrderID is the bot-generated order_id of the aggressing order that
	// produced this fill. It is the join key tying a FILL event back to the
	// OrderSubmitted intent that caused it. Only present on FILL events.
	AggressorOrderID string `json:"aggressor_order_id,omitempty"`
}

func (event *OrderEvent) UnmarshalJSON(data []byte) error {
	type orderEventJSON struct {
		SubmissionID   string  `json:"submission_id"`
		EventTimestamp int64   `json:"event_timestamp"`
		Timestamp      int64   `json:"timestamp"`
		OrderID        string  `json:"order_id"`
		Action         string  `json:"action"`
		Price          float64 `json:"price"`
		Volume         int     `json:"volume"`
		Quantity         int     `json:"quantity"`
		MatchedOrderID   string  `json:"matched_order_id"`
		ExecutionPrice   float64 `json:"execution_price"`
		AggressorOrderID string  `json:"aggressor_order_id"`
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
	event.MatchedOrderID = decoded.MatchedOrderID
	event.ExecutionPrice = decoded.ExecutionPrice
	event.AggressorOrderID = decoded.AggressorOrderID

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

// CorrectnessUpdate is emitted by the shadow engine whenever a submission's
// validation state changes.
type CorrectnessUpdate struct {
	SubmissionID string
	IsCorrect    bool
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
	Correct      bool
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
