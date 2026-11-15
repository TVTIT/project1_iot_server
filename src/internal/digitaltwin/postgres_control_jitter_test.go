package digitaltwin

import (
	"math"
	"sync"
	"testing"
)

// The jitter source is sampled once per reservation. Injection used by tests
// must not race with a live reservation, so the setter and the reader are
// synchronized.
func TestJitterSourceInjectionIsRaceFree(t *testing.T) {
	r := &PostgresControlRepository{jitter: func() float64 { return 0.5 }}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r.SetJitterFunc(func() float64 { return 1 })
			r.SetJitterFunc(nil)
		}
	}()
	for i := 0; i < 10000; i++ {
		factor := r.jitterFactor()
		if factor < 0 || factor > 1 {
			t.Fatalf("jitter factor out of range: %v", factor)
		}
	}
	close(stop)
	wg.Wait()
}

// A non-finite sample from the jitter source must be clamped before it can
// reach the schedule computation.
func TestJitterFactorClampsNonFiniteSamples(t *testing.T) {
	r := &PostgresControlRepository{jitter: func() float64 { return math.NaN() }}
	if got := r.jitterFactor(); got != 0.5 {
		t.Fatalf("NaN sample = %v, want the neutral 0.5", got)
	}
	r.SetJitterFunc(func() float64 { return math.Inf(1) })
	if got := r.jitterFactor(); got != 0.5 {
		t.Fatalf("Inf sample = %v, want the neutral 0.5", got)
	}
	r.SetJitterFunc(func() float64 { return 0.25 })
	if got := r.jitterFactor(); got != 0.25 {
		t.Fatalf("finite sample = %v, want 0.25", got)
	}
}
