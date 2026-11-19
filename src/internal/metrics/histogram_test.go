package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestHistogramFixedBuckets(t *testing.T) {
	var h Histogram
	h.Observe(100 * time.Microsecond)
	h.Observe(11 * time.Second)
	h.Observe(-time.Second)
	s := h.Snapshot()
	if s.Count != 3 || s.Overflow != 1 || s.Buckets[0] != 2 || s.SumSeconds != 11.0001 || len(s.Buckets) != len(s.UpperSeconds) {
		t.Fatalf("unexpected histogram: %+v", s)
	}
}

func TestHistogramConcurrentSnapshot(t *testing.T) {
	var h Histogram
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				h.Observe(time.Millisecond)
				_ = h.Snapshot()
			}
		})
	}
	wg.Wait()
	s := h.Snapshot()
	var total uint64
	for _, b := range s.Buckets {
		total += b
	}
	if total+s.Overflow != s.Count || s.Count != 400 {
		t.Fatal(s)
	}
}
