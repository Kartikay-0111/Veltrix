package aggregator

import (
	"context"
	"testing"

	"veltrix/artifact-checker-go/internal/models"
)

func TestAggregatorMergesHistogramAndFindsP99Bucket(t *testing.T) {
	t.Parallel()

	agg := New(0)
	agg.AddBatch(models.MetricsBatch{
		SubmissionID: "s1",
		TotalReqs:    60,
		Http200:      50,
		Histogram:    []int{10, 20, 20, 0, 0},
	})
	agg.AddBatch(models.MetricsBatch{
		SubmissionID: "s1",
		TotalReqs:    50,
		Http200:      50,
		Histogram:    []int{0, 20, 20, 10, 0},
	})

	out := make(chan models.Score, 1)
	if err := agg.Flush(context.Background(), out); err != nil {
		t.Fatalf("flush: %v", err)
	}

	score := <-out
	if score.TPS != 100 {
		t.Fatalf("TPS = %d, want 100", score.TPS)
	}
	if score.P99Bucket != 3 {
		t.Fatalf("P99Bucket = %d, want 3", score.P99Bucket)
	}
	if score.P99Ms != 50 {
		t.Fatalf("P99Ms = %f, want 50", score.P99Ms)
	}
}
