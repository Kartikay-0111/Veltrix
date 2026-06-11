package aggregator

import (
	"context"
	"math"
	"time"

	"veltrix/artifact-checker/internal/models"
)

// BucketUpperBoundsMs matches the C++ HdrLatencyHistogram::UPPER_BOUNDS_MS
// in bot-fleet/include/telemetry.hpp exactly. This is the 18-bucket HDR layout
// that the C++ bot-fleet uses when filling the AuditBatch histogram field.
// Keep this in sync with any changes to the C++ histogram definition.
var BucketUpperBoundsMs = []float64{
	0.050, 0.100, 0.250, 0.500,
	0.750, 1.000, 2.000, 3.000,
	5.000, 7.500, 10.000, 15.000,
	25.000, 50.000, 100.000, 250.000,
	500.000, 1000.000,
}

// Aggregator merges per-thread MetricsBatch messages into one composite score
// per submission per flush interval.
type Aggregator struct {
	FlushInterval time.Duration
	states        map[string]*submissionState
}

type submissionState struct {
	// Cumulative http200 count since last flush — used to compute TPS.
	http200Delta int
	// Wall-clock time of the last flush for this submission.
	lastFlush time.Time
	// Dynamic histogram: grows automatically to accommodate however many
	// buckets the producer sends (the C++ bot currently sends 18).
	histogram []int64
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
	state.http200Delta += batch.Http200
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
	now := time.Now()

	for submissionID, state := range aggregator.states {
		total := histogramTotal(state.histogram)
		if state.http200Delta == 0 && total == 0 {
			continue
		}

		// TPS = successful requests in this flush window / elapsed seconds.
		elapsed := now.Sub(state.lastFlush).Seconds()
		tps := 0
		if elapsed > 0 {
			tps = int(math.Round(float64(state.http200Delta) / elapsed))
		}

		// Percentiles computed against the dynamic histogram using the known
		// C++ bucket upper bounds. Any extra buckets beyond our bounds table
		// are treated as the last known upper bound value.
		score := models.Score{
			SubmissionID: submissionID,
			TeamName:     submissionID,
			TPS:          tps,
			P50Ms:        PercentileMs(state.histogram, 50),
			P90Ms:        PercentileMs(state.histogram, 90),
			P99Ms:        PercentileMs(state.histogram, 99),
			P99Bucket:    PercentileBucketIndex(state.histogram, 99),
			Correct:      state.correct,
		}

		if err := send(ctx, out, score); err != nil {
			return err
		}

		// Reset delta counters; keep correctness flag and histogram structure.
		state.http200Delta = 0
		state.lastFlush = now
		for i := range state.histogram {
			state.histogram[i] = 0
		}
	}

	return nil
}

func (aggregator *Aggregator) state(submissionID string) *submissionState {
	state := aggregator.states[submissionID]
	if state == nil {
		state = &submissionState{
			correct:   true,
			lastFlush: time.Now(),
		}
		aggregator.states[submissionID] = state
	}
	return state
}

// PercentileBucketIndex returns the histogram bucket index that contains the
// given percentile of observations. Returns -1 if the histogram is empty.
func PercentileBucketIndex(histogram []int64, percentile float64) int {
	total := histogramTotal(histogram)
	if total == 0 {
		return -1
	}

	target := int64(math.Ceil(float64(total) * percentile / 100.0))
	if target < 1 {
		target = 1
	}

	cumulative := int64(0)
	for index, count := range histogram {
		cumulative += count
		if cumulative >= target {
			return index
		}
	}

	return len(histogram) - 1
}

// PercentileMs converts a bucket index to a millisecond value using the shared
// bucket upper-bounds table. If the bucket index exceeds the table, the upper
// bound of the last known bucket is returned (≥ 1000ms).
func PercentileMs(histogram []int64, percentile float64) float64 {
	index := PercentileBucketIndex(histogram, percentile)
	if index < 0 {
		return 0
	}
	if index < len(BucketUpperBoundsMs) {
		return BucketUpperBoundsMs[index]
	}
	// Beyond the known table — clamp to last upper bound.
	return BucketUpperBoundsMs[len(BucketUpperBoundsMs)-1]
}

// mergeHistogram accumulates src counts into dst, growing dst dynamically if
// the incoming histogram has more buckets than what we have seen so far.
func mergeHistogram(dst *[]int64, src []int) {
	if len(src) == 0 {
		return
	}
	if len(*dst) < len(src) {
		next := make([]int64, len(src))
		copy(next, *dst)
		*dst = next
	}
	for i, count := range src {
		(*dst)[i] += int64(count)
	}
}

func histogramTotal(histogram []int64) int64 {
	total := int64(0)
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
