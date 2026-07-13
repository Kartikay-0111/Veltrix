package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	pb "veltrix/telemetry-ingester/internal/pb"
)

// Config controls the Redpanda producer.
type Config struct {
	Brokers      []string
	OrderTopic   string
	MetricsTopic string
	Logger       *log.Logger
}

// Producer wraps a franz-go client for async producing to Redpanda topics.
type Producer struct {
	client       *kgo.Client
	orderTopic   string
	metricsTopic string
	logger       *log.Logger
	wg           sync.WaitGroup

	// bgCtx bounds the async produce promises to the producer's own lifetime,
	// NOT to any single gRPC stream. The durable write to Redpanda must outlive
	// the ingest RPC: if we used the stream context, the last batch of a stream
	// (which for a correctness run carries the END-of-run marker) would be
	// canceled the instant the RPC returns and silently dropped. Close() waits
	// on wg before closing the client, so pending produces still flush on exit.
	bgCtx context.Context
}

// OrderEventJSON is the JSON shape consumed by the Go artifact-checker.
// It matches the models.OrderEvent struct in artifact-checker.
type OrderEventJSON struct {
	SubmissionID      string  `json:"submission_id"`
	EventTimestamp    int64   `json:"event_timestamp"`
	OrderID           string  `json:"order_id"`
	Action            string  `json:"action"`     // BUY | SELL | CANCEL | FILL (routing)
	OrderType         string  `json:"order_type,omitempty"` // LIMIT | MARKET | FOK | FAK | GFD
	Ticker            string  `json:"ticker,omitempty"`
	Price             float64 `json:"price"`
	Volume            int     `json:"volume"`
	Seq               uint64  `json:"seq,omitempty"`
	ContestantOrderID uint64  `json:"contestant_order_id,omitempty"`
	MatchedOrderID    uint64  `json:"matched_order_id,omitempty"`
	CancelTargetID    uint64  `json:"cancel_target_id,omitempty"`
	ExecutionPrice    float64 `json:"execution_price,omitempty"`
	AggressorOrderID  string  `json:"aggressor_order_id,omitempty"`
	EndOfRun          bool    `json:"end_of_run,omitempty"`
	// Outcome tags an intent's attempt result so a lost/rejected response is not a
	// silent seq gap: "" (OK), "REJECTED" (clean 4xx, replay no-op), "UNKNOWN"
	// (timeout/5xx/parse error, forces Unverified). Only present on intents.
	Outcome string `json:"outcome,omitempty"`
}

// MetricsJSON is the JSON shape consumed by the Go artifact-checker aggregator.
type MetricsJSON struct {
	SubmissionID string  `json:"submission_id"`
	ThreadID     int     `json:"thread_id"`
	TotalReqs    int     `json:"total_reqs"`
	Http200      int     `json:"http_200"`
	Http4xx      int     `json:"http_4xx"`
	Http5xx      int     `json:"http_5xx"`
	Timeout      int     `json:"timeout"`
	Econnref     int     `json:"econnref"`
	OtherErr     int     `json:"other_err"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	Samples      int     `json:"samples"`
	Hist         []int64 `json:"hist"`
}

func New(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create franz-go producer: %w", err)
	}

	return &Producer{
		client:       client,
		orderTopic:   cfg.OrderTopic,
		metricsTopic: cfg.MetricsTopic,
		logger:       cfg.Logger,
		bgCtx:        context.Background(),
	}, nil
}

// outcomeString maps the protobuf outcome enum (0=OK, 1=REJECTED, 2=UNKNOWN) to
// the string the checker consumes. OK maps to "" so it is omitted for the common
// case and read as the default OK verdict input downstream.
func outcomeString(outcome int32) string {
	switch outcome {
	case 1:
		return "REJECTED"
	case 2:
		return "UNKNOWN"
	default:
		return ""
	}
}

// PublishOrderEvents converts protobuf OrderSubmitted messages into JSON
// OrderEvent records and publishes them to the order_events topic.
func (p *Producer) PublishOrderEvents(orders []*pb.OrderSubmitted) {
	for _, order := range orders {
		// order.Action carries the order type (LIMIT/MARKET/CANCEL/FOK/FAK/GFD).
		// Action (routing) is the side, except for cancels.
		orderType := order.Action
		action := order.Side
		if orderType == "CANCEL" {
			action = "CANCEL"
		}
		event := OrderEventJSON{
			SubmissionID:      order.SubmissionId,
			EventTimestamp:    order.TimestampUs,
			OrderID:           order.OrderId,
			Action:            action,
			OrderType:         orderType,
			Ticker:            order.Ticker,
			Price:             order.Price,
			Volume:            int(order.Quantity),
			Seq:               order.Seq,
			ContestantOrderID: order.ContestantOrderId,
			CancelTargetID:    order.CancelTargetId,
			EndOfRun:          order.EndOfRun,
			Outcome:           outcomeString(order.Outcome),
		}

		data, err := json.Marshal(event)
		if err != nil {
			p.logger.Printf("[producer] marshal order event failed: %v", err)
			continue
		}

		p.produce(p.orderTopic, order.SubmissionId, data)
	}
}

// PublishTradeEvents converts protobuf TradeExecuted messages into JSON
// OrderEvent records (with execution fields) and publishes to order_events.
func (p *Producer) PublishTradeEvents(trades []*pb.TradeExecuted) {
	for _, trade := range trades {
		event := OrderEventJSON{
			SubmissionID:      trade.SubmissionId,
			EventTimestamp:    trade.TimestampUs,
			OrderID:           fmt.Sprintf("%d", trade.ContestantOrderId),
			Action:            "FILL",
			Ticker:            trade.Ticker,
			ContestantOrderID: trade.ContestantOrderId,
			MatchedOrderID:    trade.MatchedOrderId,
			ExecutionPrice:    trade.ExecutionPrice,
			Volume:            int(trade.ExecutionQuantity),
			AggressorOrderID:  trade.AggressorOrderId,
			Seq:               trade.Seq,
		}

		data, err := json.Marshal(event)
		if err != nil {
			p.logger.Printf("[producer] marshal trade event failed: %v", err)
			continue
		}

		p.produce(p.orderTopic, trade.SubmissionId, data)
	}
}

// PublishMetrics converts a protobuf MetricsBatch into JSON and publishes
// to the order_metrics topic.
func (p *Producer) PublishMetrics(metrics *pb.MetricsBatch) {
	if metrics == nil {
		return
	}

	batch := MetricsJSON{
		SubmissionID: metrics.SubmissionId,
		ThreadID:     int(metrics.ThreadId),
		TotalReqs:    int(metrics.Samples),
		Http200:      int(metrics.Http_200),
		Http4xx:      int(metrics.Http_4Xx),
		Http5xx:      int(metrics.Http_5Xx),
		Timeout:      int(metrics.Timeout),
		Econnref:     int(metrics.Econnref),
		OtherErr:     int(metrics.OtherErr),
		AvgLatencyMs: metrics.AvgLatencyMs,
		Samples:      int(metrics.Samples),
		Hist:         metrics.Histogram,
	}

	data, err := json.Marshal(batch)
	if err != nil {
		p.logger.Printf("[producer] marshal metrics failed: %v", err)
		return
	}

	p.produce(p.metricsTopic, metrics.SubmissionId, data)
}

func (p *Producer) produce(topic, key string, value []byte) {
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}

	p.wg.Add(1)
	p.client.Produce(p.bgCtx, record, func(_ *kgo.Record, err error) {
		defer p.wg.Done()
		if err != nil {
			p.logger.Printf("[producer] publish to %s failed: %v", topic, err)
		}
	})
}

// Close flushes pending produces and closes the client.
func (p *Producer) Close() {
	p.wg.Wait()
	p.client.Close()
}
