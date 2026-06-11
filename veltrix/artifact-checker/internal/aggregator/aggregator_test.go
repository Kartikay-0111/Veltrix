package aggregator

import (
	"testing"
)

func TestPercentileMs_18BucketHDR(t *testing.T) {
	t.Parallel()

	// 18-bucket histogram matching C++ HdrLatencyHistogram::UPPER_BOUNDS_MS.
	// Buckets: 0.05, 0.10, 0.25, 0.50, 0.75, 1.0, 2.0, 3.0, 5.0, 7.5,
	//          10.0, 15.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0
	hist := []int64{
		0, 0, 10, 40, 0, 0, 0, 0, // buckets 0-7   (0.05ms–3ms)
		0, 0, 0, 0, 0, 0, 0, 0, // buckets 8-15  (5ms–250ms)
		0, 0,                    // buckets 16-17 (500ms, 1000ms)
	}
	// 50 samples total: 10 in bucket[2]=0.25ms, 40 in bucket[3]=0.50ms

	p50 := PercentileMs(hist, 50)
	if p50 != 0.500 {
		t.Errorf("p50 = %.3f, want 0.500", p50)
	}

	p90 := PercentileMs(hist, 90)
	if p90 != 0.500 {
		t.Errorf("p90 = %.3f, want 0.500", p90)
	}

	p99 := PercentileMs(hist, 99)
	if p99 != 0.500 {
		t.Errorf("p99 = %.3f, want 0.500", p99)
	}
}

func TestMergeHistogram_DynamicGrowth(t *testing.T) {
	t.Parallel()

	var dst []int64
	// First batch: 5 buckets
	mergeHistogram(&dst, []int{1, 2, 3, 4, 5})
	if len(dst) != 5 {
		t.Fatalf("expected len=5, got %d", len(dst))
	}

	// Second batch: 18 buckets — dst should grow
	mergeHistogram(&dst, []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7})
	if len(dst) != 18 {
		t.Fatalf("expected len=18 after growth, got %d", len(dst))
	}
	if dst[17] != 7 {
		t.Errorf("bucket[17] = %d, want 7", dst[17])
	}
	// Original buckets preserved
	if dst[0] != 1 {
		t.Errorf("bucket[0] = %d, want 1", dst[0])
	}
}

func TestHistogramTotal(t *testing.T) {
	t.Parallel()

	hist := []int64{10, 20, 30}
	if got := histogramTotal(hist); got != 60 {
		t.Errorf("total = %d, want 60", got)
	}
	if got := histogramTotal(nil); got != 0 {
		t.Errorf("empty total = %d, want 0", got)
	}
}
