package aggregator

import (
	"context"
	"math"
	"time"

	"veltrix/artifact-checker-go/internal/models"
)

var defaultBucketUpperBoundsMs = []float64{1, 5, 10, 50, 1000}

// Aggregator merges per-thread MetricsBatch messages into one composite score
// per submission per flush interval.
type Aggregator struct {
	FlushInterval time.Duration
	states        map[string]*submissionState
}

type submissionState struct {
	totalReqs int
	http200   int
	histogram []int
	correct   bool
}

func New(flushInterval time.Duration) *Aggregator {
	if flushInterval <= 0 {
		flushInterval = time.Second
	}

	return &Aggregator{
		FlushInterval: flushInterval,
		states:        make(map[string]*submissionState),
	}
}

func (aggregator *Aggregator) Run(
	ctx context.Context,
	metrics <-chan models.MetricsBatch,
	correctness <-chan models.CorrectnessUpdate,
	out chan<- models.Score,
) error {
	defer close(out)

	ticker := time.NewTicker(aggregator.FlushInterval)
	defer ticker.Stop()

	for metrics != nil || correctness != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-metrics:
			if !ok {
				metrics = nil
				continue
			}
			aggregator.AddBatch(batch)
		case update, ok := <-correctness:
			if !ok {
				correctness = nil
				continue
			}
			aggregator.ApplyCorrectness(update)
		case <-ticker.C:
			if err := aggregator.Flush(ctx, out); err != nil {
				return err
			}
		}
	}

	return aggregator.Flush(ctx, out)
}

func (aggregator *Aggregator) AddBatch(batch models.MetricsBatch) {
	if batch.SubmissionID == "" {
		return
	}

	state := aggregator.state(batch.SubmissionID)
	state.totalReqs += batch.TotalReqs
	state.http200 += batch.Http200
	mergeHistogram(&state.histogram, batch.Histogram)
}

func (aggregator *Aggregator) ApplyCorrectness(update models.CorrectnessUpdate) {
	if update.SubmissionID == "" {
		return
	}

	state := aggregator.state(update.SubmissionID)
	state.correct = update.IsCorrect
}

func (aggregator *Aggregator) Flush(ctx context.Context, out chan<- models.Score) error {
	for submissionID, state := range aggregator.states {
		if state.totalReqs == 0 && state.http200 == 0 && histogramTotal(state.histogram) == 0 {
			continue
		}

		score := models.Score{
			SubmissionID: submissionID,
			TeamName:     submissionID,
			TPS:          state.http200,
			P50Ms:        PercentileMs(state.histogram, state.http200, 50),
			P90Ms:        PercentileMs(state.histogram, state.http200, 90),
			P99Ms:        PercentileMs(state.histogram, state.http200, 99),
			P99Bucket:    PercentileBucketIndex(state.histogram, state.http200, 99),
			Correct:      state.correct,
		}

		if err := send(ctx, out, score); err != nil {
			return err
		}

		state.totalReqs = 0
		state.http200 = 0
		for i := range state.histogram {
			state.histogram[i] = 0
		}
	}

	return nil
}

func (aggregator *Aggregator) state(submissionID string) *submissionState {
	state := aggregator.states[submissionID]
	if state == nil {
		state = &submissionState{correct: true}
		aggregator.states[submissionID] = state
	}
	return state
}

func PercentileBucketIndex(histogram []int, successfulRequests int, percentile float64) int {
	total := histogramTotal(histogram)
	if total == 0 || successfulRequests <= 0 {
		return -1
	}

	target := int(math.Ceil(float64(successfulRequests) * percentile / 100.0))
	if target < 1 {
		target = 1
	}
	if target > total {
		target = total
	}

	cumulative := 0
	for index, count := range histogram {
		cumulative += count
		if cumulative >= target {
			return index
		}
	}

	return len(histogram) - 1
}

func PercentileMs(histogram []int, successfulRequests int, percentile float64) float64 {
	index := PercentileBucketIndex(histogram, successfulRequests, percentile)
	if index < 0 {
		return 0
	}
	if index < len(defaultBucketUpperBoundsMs) {
		return defaultBucketUpperBoundsMs[index]
	}
	return float64(index)
}

func mergeHistogram(dst *[]int, src []int) {
	if len(src) == 0 {
		return
	}
	if len(*dst) < len(src) {
		next := make([]int, len(src))
		copy(next, *dst)
		*dst = next
	}
	for i, count := range src {
		(*dst)[i] += count
	}
}

func histogramTotal(histogram []int) int {
	total := 0
	for _, count := range histogram {
		total += count
	}
	return total
}

func send(ctx context.Context, ch chan<- models.Score, value models.Score) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- value:
		return nil
	}
}
