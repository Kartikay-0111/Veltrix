package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"veltrix/artifact-checker/internal/models"
)

const (
	defaultEventBuffer   = 65_536
	defaultMetricsBuffer = 65_536
)

// Config controls the Redpanda consumer. The service consumes both topics from
// one franz-go client so topic ordering is delegated to Redpanda partitions,
// while downstream per-submission ordering is handled by watermark processors.
type Config struct {
	Brokers       []string
	ConsumerGroup string
	OrderTopic    string
	MetricsTopic  string

	EventBuffer   int
	MetricsBuffer int

	Logger *log.Logger
}

// Service owns the franz-go client and exposes decoded, buffered Go channels.
type Service struct {
	client       *kgo.Client
	orderTopic   string
	metricsTopic string
	events       chan models.OrderEvent
	metrics      chan models.MetricsBatch
	logger       *log.Logger
}

func New(config Config) (*Service, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("consumer requires at least one Redpanda broker")
	}
	if config.ConsumerGroup == "" {
		return nil, errors.New("consumer group is required")
	}
	if config.OrderTopic == "" && config.MetricsTopic == "" {
		return nil, errors.New("at least one topic is required")
	}

	eventBuffer := config.EventBuffer
	if eventBuffer <= 0 {
		eventBuffer = defaultEventBuffer
	}

	metricsBuffer := config.MetricsBuffer
	if metricsBuffer <= 0 {
		metricsBuffer = defaultMetricsBuffer
	}

	topics := uniqueTopics(config.OrderTopic, config.MetricsTopic)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers...),
		kgo.ConsumerGroup(config.ConsumerGroup),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, fmt.Errorf("create franz-go client: %w", err)
	}

	logger := config.Logger
	if logger == nil {
		logger = log.Default()
	}

	return &Service{
		client:       client,
		orderTopic:   config.OrderTopic,
		metricsTopic: config.MetricsTopic,
		events:       make(chan models.OrderEvent, eventBuffer),
		metrics:      make(chan models.MetricsBatch, metricsBuffer),
		logger:       logger,
	}, nil
}

func (service *Service) Events() <-chan models.OrderEvent {
	return service.events
}

func (service *Service) Metrics() <-chan models.MetricsBatch {
	return service.metrics
}

// Run must be started in its own goroutine. It polls Redpanda, decodes records,
// and pushes typed payloads onto buffered channels for the rest of the pipeline.
func (service *Service) Run(ctx context.Context) error {
	defer service.client.Close()
	defer close(service.events)
	defer close(service.metrics)

	for {
		fetches := service.client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}

		for _, fetchErr := range fetches.Errors() {
			service.logger.Printf("[consumer] fetch error topic=%s partition=%d: %v",
				fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
		}

		var runErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if runErr != nil {
				return
			}
			runErr = service.handleRecord(ctx, record)
		})
		if runErr != nil {
			return runErr
		}
	}
}

func (service *Service) handleRecord(ctx context.Context, record *kgo.Record) error {
	switch service.classify(record) {
	case recordKindOrder:
		var event models.OrderEvent
		if err := json.Unmarshal(record.Value, &event); err != nil {
			service.logger.Printf("[consumer] bad order event topic=%s: %v", record.Topic, err)
			return nil
		}
		if event.SubmissionID == "" {
			service.logger.Printf("[consumer] dropping order event without submission_id")
			return nil
		}
		return send(ctx, service.events, event)

	case recordKindMetrics:
		var batch models.MetricsBatch
		if err := json.Unmarshal(record.Value, &batch); err != nil {
			service.logger.Printf("[consumer] bad metrics batch topic=%s: %v", record.Topic, err)
			return nil
		}
		if batch.SubmissionID == "" {
			service.logger.Printf("[consumer] dropping metrics batch without submission_id")
			return nil
		}
		return send(ctx, service.metrics, batch)

	default:
		service.logger.Printf("[consumer] dropping record from unknown topic=%s", record.Topic)
		return nil
	}
}

type recordKind int

const (
	recordKindUnknown recordKind = iota
	recordKindOrder
	recordKindMetrics
)

func (service *Service) classify(record *kgo.Record) recordKind {
	if record.Topic == service.orderTopic && record.Topic != service.metricsTopic {
		return recordKindOrder
	}
	if record.Topic == service.metricsTopic && record.Topic != service.orderTopic {
		return recordKindMetrics
	}

	// If a deployment reuses a single topic during migration, sniff by payload
	// field names. This keeps the service tolerant while the architecture moves
	// from the C++ checker topic shape to split event/metrics topics.
	payload := string(record.Value)
	if strings.Contains(payload, `"hist"`) || strings.Contains(payload, `"histogram"`) || strings.Contains(payload, `"http_200"`) {
		return recordKindMetrics
	}
	if strings.Contains(payload, `"action"`) || strings.Contains(payload, `"event_timestamp"`) {
		return recordKindOrder
	}

	return recordKindUnknown
}

func uniqueTopics(topics ...string) []string {
	seen := make(map[string]struct{}, len(topics))
	unique := make([]string, 0, len(topics))
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		unique = append(unique, topic)
	}
	return unique
}

func send[T any](ctx context.Context, ch chan<- T, value T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- value:
		return nil
	}
}
