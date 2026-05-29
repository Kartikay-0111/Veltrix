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
}

// OrderEventJSON is the JSON shape consumed by the Go artifact-checker.
// It matches the models.OrderEvent struct in artifact-checker-go.
type OrderEventJSON struct {
	SubmissionID   string  `json:"submission_id"`
	EventTimestamp int64   `json:"event_timestamp"`
	OrderID        string  `json:"order_id"`
	Action         string  `json:"action"`
	Price          float64 `json:"price"`
	Volume         int     `json:"volume"`
	MatchedOrderID string  `json:"matched_order_id,omitempty"`
	ExecutionPrice float64 `json:"execution_price,omitempty"`
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
	}, nil
}

// PublishOrderEvents converts protobuf OrderSubmitted messages into JSON
// OrderEvent records and publishes them to the order_events topic.
func (p *Producer) PublishOrderEvents(ctx context.Context, orders []*pb.OrderSubmitted) {
	for _, order := range orders {
		event := OrderEventJSON{
			SubmissionID:   order.SubmissionId,
			EventTimestamp: order.TimestampUs,
			OrderID:        order.OrderId,
			Action:         order.Action,
			Price:          order.Price,
			Volume:         int(order.Quantity),
		}

		data, err := json.Marshal(event)
		if err != nil {
			p.logger.Printf("[producer] marshal order event failed: %v", err)
			continue
		}

		p.produce(ctx, p.orderTopic, order.SubmissionId, data)
	}
}

// PublishTradeEvents converts protobuf TradeExecuted messages into JSON
// OrderEvent records (with execution fields) and publishes to order_events.
func (p *Producer) PublishTradeEvents(ctx context.Context, trades []*pb.TradeExecuted) {
	for _, trade := range trades {
		event := OrderEventJSON{
			SubmissionID:   trade.SubmissionId,
			EventTimestamp: trade.TimestampUs,
			OrderID:        fmt.Sprintf("%d", trade.ContestantOrderId),
			Action:         "FILL",
			MatchedOrderID: fmt.Sprintf("%d", trade.MatchedOrderId),
			ExecutionPrice: trade.ExecutionPrice,
			Volume:         int(trade.ExecutionQuantity),
		}

		data, err := json.Marshal(event)
		if err != nil {
			p.logger.Printf("[producer] marshal trade event failed: %v", err)
			continue
		}

		p.produce(ctx, p.orderTopic, trade.SubmissionId, data)
	}
}

// PublishMetrics converts a protobuf MetricsBatch into JSON and publishes
// to the order_metrics topic.
func (p *Producer) PublishMetrics(ctx context.Context, metrics *pb.MetricsBatch) {
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

	p.produce(ctx, p.metricsTopic, metrics.SubmissionId, data)
}

func (p *Producer) produce(ctx context.Context, topic, key string, value []byte) {
	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}

	p.wg.Add(1)
	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
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
