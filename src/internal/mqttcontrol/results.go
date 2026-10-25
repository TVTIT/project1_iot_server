package mqttcontrol

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/metrics"
)

// ResultRepository commits validated device results without human actor scope.
type ResultRepository interface {
	AcceptControlResult(context.Context, string, []byte, time.Time) (digitaltwin.ControlResultOutcome, error)
}

// ResultMetrics uses fixed counters and local processing durations only.
type ResultMetrics struct {
	accepted, duplicate, late, rejected, dbError, queueFull atomic.Int64
	processing                                              metrics.Histogram
}

// ResultMetricsSnapshot distinguishes application commit from broker PUBACK.
type ResultMetricsSnapshot struct {
	Accepted   int64                     `json:"accepted"`
	Duplicate  int64                     `json:"duplicate"`
	Late       int64                     `json:"late"`
	Rejected   int64                     `json:"rejected"`
	DBError    int64                     `json:"db_error"`
	QueueFull  int64                     `json:"queue_full"`
	Processing metrics.HistogramSnapshot `json:"processing"`
}

// Snapshot contains no payload, credential or command/device/user identifiers.
func (m *ResultMetrics) Snapshot() ResultMetricsSnapshot {
	return ResultMetricsSnapshot{Accepted: m.accepted.Load(), Duplicate: m.duplicate.Load(), Late: m.late.Load(), Rejected: m.rejected.Load(), DBError: m.dbError.Load(), QueueFull: m.queueFull.Load(), Processing: m.processing.Snapshot()}
}

// ResultConsumer owns a fixed worker set and joins within one caller budget.
type ResultConsumer struct {
	workers          int
	timeout          time.Duration
	repo             ResultRepository
	source           <-chan RawResult
	metrics          *ResultMetrics
	mu               sync.Mutex
	started, stopped bool
	running          atomic.Bool
	active           atomic.Int64
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	done             chan struct{}
}

// NewResultConsumer creates an inert consumer with explicitly bounded workers/DB calls.
func NewResultConsumer(workers int, timeout time.Duration, repo ResultRepository, source <-chan RawResult, m *ResultMetrics) (*ResultConsumer, error) {
	if workers < 1 || workers > 16 || timeout < time.Millisecond || timeout > time.Minute || nilDependency(repo) || source == nil {
		return nil, digitaltwin.ErrInvalidInput
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ResultConsumer{workers: workers, timeout: timeout, repo: repo, source: source, metrics: m, ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}

// Start launches only the fixed workers; never one goroutine per delivery.
func (r *ResultConsumer) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped {
		return digitaltwin.ErrInvalidInput
	}
	r.started = true
	r.running.Store(true)
	r.wg.Add(r.workers)
	for n := 0; n < r.workers; n++ {
		go r.worker()
	}
	go func() { r.wg.Wait(); close(r.done) }()
	return nil
}

// Ready requires the actual worker set to be running.
func (r *ResultConsumer) Ready() bool { return r.running.Load() && r.active.Load() == int64(r.workers) }

// Stop cancels intake and in-flight DB calls, not already committed results.
func (r *ResultConsumer) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	r.running.Store(false)
	r.cancel()
}

// Close joins under the caller's shared shutdown budget.
func (r *ResultConsumer) Close(ctx context.Context) error {
	r.Stop()
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *ResultConsumer) worker() {
	defer r.wg.Done()
	r.active.Add(1)
	defer r.active.Add(-1)
	for {
		if r.ctx.Err() != nil {
			return
		}
		select {
		case <-r.ctx.Done():
			return
		case raw, ok := <-r.source:
			if !ok {
				return
			}
			r.process(raw)
		}
	}
}

func (r *ResultConsumer) process(raw RawResult) {
	if r.metrics != nil {
		start := time.Now()
		defer func() { r.metrics.processing.Observe(time.Since(start)) }()
	}
	_, err := digitaltwin.ParseMQTTResult(raw.Topic, raw.Payload, false)
	if err != nil || raw.ReceivedAt.IsZero() {
		if r.metrics != nil {
			r.metrics.rejected.Add(1)
		}
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()
	outcome, err := r.repo.AcceptControlResult(ctx, raw.Topic, raw.Payload, raw.ReceivedAt)
	if err != nil {
		slog.Error("result consumer accept error", "error", err, "topic", raw.Topic)
		if r.metrics != nil {
			if errors.Is(err, digitaltwin.ErrInvalidInput) || errors.Is(err, digitaltwin.ErrNotFound) || errors.Is(err, digitaltwin.ErrControlResultConflict) {
				r.metrics.rejected.Add(1)
			} else {
				r.metrics.dbError.Add(1)
			}
		}
		return
	}
	if r.metrics == nil {
		return
	}
	switch outcome {
	case digitaltwin.ControlResultAccepted:
		r.metrics.accepted.Add(1)
	case digitaltwin.ControlResultDuplicate:
		r.metrics.duplicate.Add(1)
	case digitaltwin.ControlResultLate:
		r.metrics.late.Add(1)
	default:
		r.metrics.rejected.Add(1)
	}
}
