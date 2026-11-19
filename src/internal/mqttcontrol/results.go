package mqttcontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

// ReceiptRepository commits a strictly validated durable Gateway receipt.
type ReceiptRepository interface {
	AcceptControlReceipt(context.Context, string, []byte, time.Time) (digitaltwin.ControlReceiptOutcome, error)
}

// SnapshotHandler receives validated raw snapshot candidates on the existing
// bounded consumer workers. It must remain synchronous and bounded.
type SnapshotHandler func(context.Context, RawResult)

// ReceiptMetrics contains fixed receipt counters; it is independent of command-result metrics/timelines.
type ReceiptMetrics struct {
	accepted, duplicate, late, rejected, dependencyError, dbError, queueFull atomic.Int64
}

// ReceiptMetricsSnapshot contains no command/device identifiers.
type ReceiptMetricsSnapshot struct {
	Accepted        int64 `json:"accepted"`
	Duplicate       int64 `json:"duplicate"`
	Late            int64 `json:"late"`
	Rejected        int64 `json:"rejected"`
	DependencyError int64 `json:"dependency_error"`
	DBError         int64 `json:"db_error"`
	QueueFull       int64 `json:"queue_full"`
}

// Snapshot returns fixed receipt counters without identifiers or payload data.
func (m *ReceiptMetrics) Snapshot() ReceiptMetricsSnapshot {
	return ReceiptMetricsSnapshot{
		Accepted: m.accepted.Load(), Duplicate: m.duplicate.Load(), Late: m.late.Load(),
		Rejected: m.rejected.Load(), DependencyError: m.dependencyError.Load(),
		DBError: m.dbError.Load(), QueueFull: m.queueFull.Load(),
	}
}

// ResultMetrics uses fixed counters and local processing durations only.
type ResultMetrics struct {
	accepted, duplicate, late, rejected, dbError, queueFull atomic.Int64
	processing, queueWait, receivedToCommit                 metrics.Histogram
}

// ResultMetricsSnapshot distinguishes application commit from broker PUBACK.
type ResultMetricsSnapshot struct {
	Accepted                  int64                     `json:"accepted"`
	Duplicate                 int64                     `json:"duplicate"`
	Late                      int64                     `json:"late"`
	Rejected                  int64                     `json:"rejected"`
	DBError                   int64                     `json:"db_error"`
	QueueFull                 int64                     `json:"queue_full"`
	Processing                metrics.HistogramSnapshot `json:"processing"`
	QueueWait                 metrics.HistogramSnapshot `json:"queue_wait"`
	ReceivedToConfirmedCommit metrics.HistogramSnapshot `json:"received_to_confirmed_commit"`
}

// Snapshot contains no payload, credential or command/device/user identifiers.
func (m *ResultMetrics) Snapshot() ResultMetricsSnapshot {
	return ResultMetricsSnapshot{Accepted: m.accepted.Load(), Duplicate: m.duplicate.Load(), Late: m.late.Load(), Rejected: m.rejected.Load(), DBError: m.dbError.Load(), QueueFull: m.queueFull.Load(), Processing: m.processing.Snapshot(), QueueWait: m.queueWait.Snapshot(), ReceivedToConfirmedCommit: m.receivedToCommit.Snapshot()}
}

// ResultConsumer owns a fixed worker set and joins within one caller budget.
type ResultConsumer struct {
	workers          int
	timeout          time.Duration
	repo             ResultRepository
	source           <-chan RawResult
	metrics          *ResultMetrics
	receiptRepo      ReceiptRepository
	receiptMetrics   *ReceiptMetrics
	receiptsEnabled  bool
	snapshotsEnabled bool
	snapshotHandler  SnapshotHandler
	mu               sync.Mutex
	started, stopped bool
	running          atomic.Bool
	active           atomic.Int64
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	done             chan struct{}
}

// ConfigureReceipts enables receipt routing on the existing bounded response
// worker. It must be called before Start; Stage 5 remains result-only.
func (r *ResultConsumer) ConfigureReceipts(enabled bool, repo ReceiptRepository, m *ReceiptMetrics) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped || (enabled && nilDependency(repo)) {
		return digitaltwin.ErrInvalidInput
	}
	r.receiptsEnabled = enabled
	if enabled {
		r.receiptRepo = repo
	}
	r.receiptMetrics = m
	return nil
}

// ConfigureSnapshots enables strict snapshot routing on the same bounded
// response worker set. Stage 5 remains result-only.
func (r *ResultConsumer) ConfigureSnapshots(enabled bool, handler SnapshotHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped || (enabled && handler == nil) {
		return digitaltwin.ErrInvalidInput
	}
	r.snapshotsEnabled = enabled
	r.snapshotHandler = handler
	return nil
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
	messageType, typeErr := responseMessageType(raw.Payload)
	if messageType == "command_receipt" {
		r.processReceipt(raw)
		return
	}
	if messageType == "configuration_snapshot" {
		r.processSnapshot(raw, typeErr)
		return
	}
	if r.metrics != nil {
		start := time.Now()
		defer func() { r.metrics.processing.Observe(time.Since(start)) }()
		if !raw.QueuedAt.IsZero() {
			r.metrics.queueWait.Observe(start.Sub(raw.QueuedAt))
		}
	}
	if typeErr != nil || messageType != "command_result" {
		if r.metrics != nil {
			r.metrics.rejected.Add(1)
		}
		return
	}
	if raw.Retained {
		if r.metrics != nil {
			r.metrics.rejected.Add(1)
		}
		return
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
	if r.metrics != nil && !raw.ReceivedAt.IsZero() {
		ctx = digitaltwin.WithControlResultCommitObserver(ctx, func(committedAt time.Time) {
			d := committedAt.Sub(raw.ReceivedAt)
			if d >= 0 {
				r.metrics.receivedToCommit.Observe(d)
			}
		})
	}
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

func (r *ResultConsumer) processSnapshot(raw RawResult, typeErr error) {
	r.mu.Lock()
	enabled, handler := r.snapshotsEnabled, r.snapshotHandler
	r.mu.Unlock()
	if !enabled || typeErr != nil || raw.Retained || raw.ReceivedAt.IsZero() {
		return
	}
	if _, err := digitaltwin.ParseMQTTConfigurationSnapshot(raw.Topic, raw.Payload, false); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()
	handler(ctx, raw)
}

// responseMessageType is only a narrow discriminator. Repository calls always
// follow the complete strict type-specific parser, which rejects duplicate and
// unknown envelope fields.
func responseMessageType(payload []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return "", digitaltwin.ErrInvalidInput
	}
	seen := make(map[string]struct{})
	messageType := ""
	duplicates := false
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return messageType, digitaltwin.ErrInvalidInput
		}
		if _, exists := seen[key]; exists {
			duplicates = true
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return messageType, digitaltwin.ErrInvalidInput
		}
		if key == "message_type" {
			var parsed string
			if json.Unmarshal(value, &parsed) != nil {
				return messageType, digitaltwin.ErrInvalidInput
			}
			if messageType == "" {
				messageType = parsed
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return messageType, digitaltwin.ErrInvalidInput
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF || messageType == "" || duplicates {
		return messageType, digitaltwin.ErrInvalidInput
	}
	return messageType, nil
}

func (r *ResultConsumer) processReceipt(raw RawResult) {
	m := r.receiptMetrics
	reject := func() {
		if m != nil {
			m.rejected.Add(1)
		}
	}
	if !r.receiptsEnabled || raw.Retained {
		reject()
		return
	}
	if _, err := digitaltwin.ParseMQTTReceipt(raw.Topic, raw.Payload, false); err != nil || raw.ReceivedAt.IsZero() {
		reject()
		return
	}
	if nilDependency(r.receiptRepo) {
		if m != nil {
			m.dependencyError.Add(1)
		}
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()
	outcome, err := r.receiptRepo.AcceptControlReceipt(ctx, raw.Topic, raw.Payload, raw.ReceivedAt)
	if err != nil {
		if m != nil {
			switch {
			case errors.Is(err, digitaltwin.ErrDependencyUnavailable):
				m.dependencyError.Add(1)
			case errors.Is(err, digitaltwin.ErrInvalidInput),
				errors.Is(err, digitaltwin.ErrNotFound),
				errors.Is(err, digitaltwin.ErrControlResultConflict),
				errors.Is(err, digitaltwin.ErrCommandReceiptUnavailable):
				m.rejected.Add(1)
			default:
				m.dbError.Add(1)
			}
		}
		return
	}
	if m == nil {
		return
	}
	switch outcome {
	case digitaltwin.ControlReceiptAccepted:
		m.accepted.Add(1)
	case digitaltwin.ControlReceiptDuplicate:
		m.duplicate.Add(1)
	case digitaltwin.ControlReceiptLate:
		m.late.Add(1)
	default:
		m.rejected.Add(1)
	}
}
