// Package metrics provides a fixed-size duration histogram shared by ingress,
// HTTP and authentication. It has no dynamic registry or caller-supplied labels.
package metrics

import (
	"sync"
	"time"
)

var upperSeconds = [...]float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Histogram holds non-cumulative fixed buckets. The zero value is ready to use.
type Histogram struct {
	mu              sync.Mutex
	buckets         [len(upperSeconds)]uint64
	count, overflow uint64
	sum             time.Duration
}

// HistogramSnapshot is a bounded, consistent copy, in seconds.
type HistogramSnapshot struct {
	UpperSeconds [len(upperSeconds)]float64 `json:"upper_seconds"`
	Buckets      [len(upperSeconds)]uint64  `json:"buckets"`
	Count        uint64                     `json:"count"`
	Overflow     uint64                     `json:"overflow"`
	SumSeconds   float64                    `json:"sum_seconds"`
}

// Observe records one duration; negative durations are clamped to zero.
func (h *Histogram) Observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += d
	for i, upper := range upperSeconds {
		if d.Seconds() <= upper {
			h.buckets[i]++
			return
		}
	}
	h.overflow++
}

// Snapshot copies bounded state without retaining samples.
func (h *Histogram) Snapshot() HistogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return HistogramSnapshot{UpperSeconds: upperSeconds, Buckets: h.buckets, Count: h.count, Overflow: h.overflow, SumSeconds: h.sum.Seconds()}
}
