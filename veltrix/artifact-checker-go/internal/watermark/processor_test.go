package watermark

import (
	"context"
	"testing"

	"veltrix/artifact-checker-go/internal/models"
)

func TestProcessorEmitsOnlyEventsBehindWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	processor := NewProcessor(2_000)
	out := make(chan models.OrderEvent, 4)

	input := []models.OrderEvent{
		{OrderID: "late-held", EventTimestamp: 5_000},
		{OrderID: "old-ready", EventTimestamp: 3_000},
		{OrderID: "advance-watermark", EventTimestamp: 9_000},
	}

	for _, event := range input {
		if err := processor.Process(ctx, event, out); err != nil {
			t.Fatalf("process event: %v", err)
		}
	}

	if got := drainOrderIDs(out); !equalStrings(got, []string{"old-ready", "late-held"}) {
		t.Fatalf("emitted order ids = %v, want %v", got, []string{"old-ready", "late-held"})
	}

	if err := processor.Flush(ctx, out); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if got := drainOrderIDs(out); !equalStrings(got, []string{"advance-watermark"}) {
		t.Fatalf("flushed order ids = %v, want %v", got, []string{"advance-watermark"})
	}
}

func TestProcessorPreservesArrivalOrderForEqualTimestamps(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	processor := NewProcessor(1_000)
	out := make(chan models.OrderEvent, 4)

	input := []models.OrderEvent{
		{OrderID: "first", EventTimestamp: 1_000},
		{OrderID: "second", EventTimestamp: 1_000},
		{OrderID: "watermark-advance", EventTimestamp: 3_000},
	}

	for _, event := range input {
		if err := processor.Process(ctx, event, out); err != nil {
			t.Fatalf("process event: %v", err)
		}
	}

	if got := drainOrderIDs(out); !equalStrings(got, []string{"first", "second"}) {
		t.Fatalf("emitted order ids = %v, want %v", got, []string{"first", "second"})
	}
}

func TestRunDrainsOnInputClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	processor := NewProcessor(DefaultAllowedLatenessMicros)
	in := make(chan models.OrderEvent, 2)
	out := make(chan models.OrderEvent, 2)
	done := make(chan error, 1)

	in <- models.OrderEvent{OrderID: "newer", EventTimestamp: 20}
	in <- models.OrderEvent{OrderID: "older", EventTimestamp: 10}
	close(in)

	go func() {
		done <- processor.Run(ctx, in, out)
		close(out)
	}()

	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	if got := collectOrderIDs(out); !equalStrings(got, []string{"older", "newer"}) {
		t.Fatalf("drained order ids = %v, want %v", got, []string{"older", "newer"})
	}
}

func drainOrderIDs(ch <-chan models.OrderEvent) []string {
	var orderIDs []string
	for {
		select {
		case event := <-ch:
			orderIDs = append(orderIDs, event.OrderID)
		default:
			return orderIDs
		}
	}
}

func collectOrderIDs(ch <-chan models.OrderEvent) []string {
	var orderIDs []string
	for event := range ch {
		orderIDs = append(orderIDs, event.OrderID)
	}

	return orderIDs
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}
