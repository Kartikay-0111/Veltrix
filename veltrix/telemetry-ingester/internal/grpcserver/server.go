package grpcserver

import (
	"io"
	"log"
	"sync"
	"sync/atomic"

	pb "veltrix/telemetry-ingester/internal/pb"
	"veltrix/telemetry-ingester/internal/producer"
)

// Server implements the TelemetryService gRPC service.
type Server struct {
	pb.UnimplementedTelemetryServiceServer

	producer *producer.Producer
	logger   *log.Logger

	// Thread-safe storage for the latest metrics (for GET /metrics/latest)
	latestMu      sync.RWMutex
	latestMetrics *producer.MetricsJSON

	batchesTotal atomic.Int64
}

// New creates a new gRPC telemetry server.
func New(prod *producer.Producer, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	return &Server{
		producer: prod,
		logger:   logger,
	}
}

// StreamTelemetry implements the client-side streaming RPC.
// The C++ bot-fleet sends a stream of AuditBatch messages (one every 500ms
// per thread). We process each batch inline and ack once when the stream ends.
func (s *Server) StreamTelemetry(stream pb.TelemetryService_StreamTelemetryServer) error {
	var batchCount int64

	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			// Stream ended normally — send final ack
			s.logger.Printf("[gRPC] stream completed, received %d batches", batchCount)
			return stream.SendAndClose(&pb.StreamTelemetryResponse{
				BatchesReceived: batchCount,
				Status:          "ok",
			})
		}
		if err != nil {
			s.logger.Printf("[gRPC] stream recv error: %v", err)
			return err
		}

		batchCount++
		s.batchesTotal.Add(1)

		// ── Publish order events to Redpanda ─────────────────────────────
		// NOTE: publishes are async and intentionally decoupled from this
		// RPC's context — see Producer.bgCtx. Binding them to stream.Context()
		// dropped the final batch (incl. the correctness END marker) on EOF.
		if len(batch.Orders) > 0 {
			s.producer.PublishOrderEvents(batch.Orders)
		}

		// ── Publish trade events to Redpanda ─────────────────────────────
		if len(batch.Trades) > 0 {
			s.producer.PublishTradeEvents(batch.Trades)
		}

		// ── Publish metrics to Redpanda ──────────────────────────────────
		if batch.Metrics != nil {
			s.producer.PublishMetrics(batch.Metrics)
			s.storeLatestMetrics(batch.Metrics)
		}

		if batchCount%100 == 0 {
			s.logger.Printf("[gRPC] processed %d batches (%d orders, %d trades in last batch)",
				batchCount, len(batch.Orders), len(batch.Trades))
		}
	}
}

// LatestMetrics returns the most recently received metrics batch for the
// HTTP GET /metrics/latest endpoint. Returns nil if no metrics have been
// received yet.
func (s *Server) LatestMetrics() *producer.MetricsJSON {
	s.latestMu.RLock()
	defer s.latestMu.RUnlock()
	return s.latestMetrics
}

func (s *Server) storeLatestMetrics(metrics *pb.MetricsBatch) {
	m := &producer.MetricsJSON{
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

	s.latestMu.Lock()
	s.latestMetrics = m
	s.latestMu.Unlock()
}
