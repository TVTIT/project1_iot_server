package telemetryws

import (
	"context"
	"errors"
	"sync"
	"time"

	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

const (
	// DefaultEventQueueSlots bounds accepted committed events including active workers.
	DefaultEventQueueSlots = 64
	// DefaultEventPendingBytes bounds global logical event work to 8 MiB.
	DefaultEventPendingBytes = 8 << 20
	// 64 bytes/sample bounds retained float storage, DTO duplication and JSON's
	// worst ordinary finite-float rendering (24 bytes), plus alignment/overhead.
	eventSampleReservationBytes = 64
	eventFixedReservationBytes  = 2048
)

// EventEncoder serializes one validated DTO into its single shared event payload.
type EventEncoder func(TelemetryEventDTO, int) (*CommittedBatchEvent, error)

// EventRuntimeConfig sets global committed-event bounds. Worker count defaults to one.
type EventRuntimeConfig struct {
	QueueSlots      int
	MaxPendingBytes int
	MaxFrameBytes   int
	Workers         int
}

type eventJob struct {
	batch             telemetry.BatchDTO
	reservation       int
	confirmedObserved time.Time
}

// EventRuntime owns a bounded post-commit snapshot queue and fixed encoders.
// The global logical reservation covers queued AND worker-held snapshot/DTO/encoded
// representations until fan-out returns; it is not peak RSS. Once shared payload
// references enter session queues, their independent per-session queued+in-flight
// byte budgets account each reference. The encoded backing bytes are shared, never
// copied for each subscription.
type EventRuntime struct {
	hub      *Hub
	cfg      EventRuntimeConfig
	encode   EventEncoder
	jobs     chan eventJob
	gap      chan struct{}
	mu       sync.Mutex
	closed   bool
	slots    int
	bytes    int
	workers  sync.WaitGroup
	stop     chan struct{}
	stopOnce sync.Once
	metrics  *Metrics
}

// NewEventRuntime returns nil for a disabled/nil hub, allowing ingress to use a
// direct optional call without special lifecycle dependencies.
func NewEventRuntime(h *Hub, cfg EventRuntimeConfig, encoder EventEncoder) *EventRuntime {
	if h == nil {
		return nil
	}
	if cfg.QueueSlots <= 0 {
		cfg.QueueSlots = DefaultEventQueueSlots
	}
	if cfg.MaxPendingBytes <= 0 {
		cfg.MaxPendingBytes = DefaultEventPendingBytes
	}
	if cfg.MaxFrameBytes <= 0 {
		cfg.MaxFrameBytes = DefaultMaxEventBytes
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.Workers > cfg.QueueSlots {
		cfg.Workers = cfg.QueueSlots
	}
	if encoder == nil {
		encoder = EncodeTelemetryEvent
	}
	r := &EventRuntime{hub: h, cfg: cfg, encode: encoder, jobs: make(chan eventJob, cfg.QueueSlots), gap: make(chan struct{}, 1), stop: make(chan struct{})}
	for i := 0; i < cfg.Workers; i++ {
		r.workers.Add(1)
		go r.worker()
	}
	return r
}

// SetMetrics attaches the process observer without affecting queue behavior.
func (r *EventRuntime) SetMetrics(m *Metrics) {
	if r != nil {
		r.metrics = m
	}
}

// TrySubmitBatch does only bounded admission, lightweight validation and sample
// snapshotting. On any loss it emits one coalesced gap signal; if that signal is
// already pending, the worker's conservative close-all policy still covers it.
func (r *EventRuntime) TrySubmitBatch(batch telemetry.BatchDTO, confirmedObserved time.Time) bool {
	if r == nil {
		return true
	}
	if r.metrics != nil {
		r.metrics.Submitted(confirmedObserved)
	}
	estimate, ok := reservedEventBytes(batch)
	if !ok {
		if r.metrics != nil {
			r.metrics.Drop()
		}
		if r != nil {
			r.signalGap()
		}
		return false
	}
	r.mu.Lock()
	if r.closed || r.slots >= r.cfg.QueueSlots || estimate > r.cfg.MaxPendingBytes-r.bytes {
		r.mu.Unlock()
		if r.metrics != nil {
			r.metrics.Drop()
		}
		r.signalGap()
		return false
	}
	r.slots++
	r.bytes += estimate
	r.mu.Unlock()
	// Reserve first: only then allocate the bounded immutable sample snapshot.
	snapshot := batch
	snapshot.Samples = append([]float64(nil), batch.Samples...)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		r.bytes -= estimate
		r.slots--
		r.signalGap()
		if r.metrics != nil {
			r.metrics.Drop()
		}
		return false
	}
	select {
	case r.jobs <- eventJob{batch: snapshot, reservation: estimate, confirmedObserved: confirmedObserved}:
		return true
	default:
		r.bytes -= estimate
		r.slots--
		r.signalGap()
		if r.metrics != nil {
			r.metrics.Drop()
		}
		return false
	}
}

func reservedEventBytes(b telemetry.BatchDTO) (int, bool) {
	if b.SampleCount <= 0 || int64(b.SampleCount) != int64(len(b.Samples)) || len(b.Samples) > (int(^uint(0)>>1)-eventFixedReservationBytes)/eventSampleReservationBytes {
		return 0, false
	}
	if len(b.GatewayID) > 128 || len(b.SensorID) > 128 || len(b.MeasuredAt) > 128 {
		return 0, false
	}
	return eventFixedReservationBytes + len(b.Samples)*eventSampleReservationBytes, true
}

func (r *EventRuntime) signalGap() {
	select {
	case r.gap <- struct{}{}:
	default:
	}
}
func (r *EventRuntime) release(n int) {
	r.mu.Lock()
	r.bytes -= n
	r.slots--
	if r.bytes < 0 {
		r.bytes = 0
	}
	if r.slots < 0 {
		r.slots = 0
	}
	r.mu.Unlock()
}

func (r *EventRuntime) worker() {
	defer r.workers.Done()
	for {
		select {
		case <-r.stop:
			return
		case <-r.gap:
			r.hub.CloseAllSessions(RevocationReasonGap)
		case job := <-r.jobs:
			r.process(job)
		}
	}
}
func (r *EventRuntime) process(job eventJob) {
	defer r.release(job.reservation)
	dto, err := NewTelemetryEvent(job.batch)
	if err == nil {
		var encoded *CommittedBatchEvent
		encoded, err = r.encode(dto, r.cfg.MaxFrameBytes)
		if err == nil && encoded == nil {
			err = errors.New("telemetryws: encoder returned nil event")
		}
		if err == nil {
			var shared *SharedEvent
			shared, err = NewSharedEventFromCommitted(*encoded, r.cfg.MaxFrameBytes)
			if err == nil {
				shared.setConfirmedObservedAt(job.confirmedObserved)
				if r.metrics != nil {
					r.metrics.NewEvent(job.confirmedObserved)
				}
				_, err = r.hub.FanOut(shared)
			}
		}
	}
	if err != nil {
		if r.metrics != nil {
			r.metrics.BuildFailure()
		}
		if target, ok := trustedBatchTarget(job.batch); ok {
			r.hub.CloseTargetSessions(target, RevocationReasonGap)
		} else {
			// Without a valid composite identity the lost event cannot be scoped;
			// preserve the conservative global-gap policy rather than lose it silently.
			r.hub.CloseAllSessions(RevocationReasonGap)
		}
	}
}

func trustedBatchTarget(batch telemetry.BatchDTO) (TargetKey, bool) {
	if gateway.ValidateGatewayID(batch.GatewayID) != nil || gateway.ValidateSensorID(batch.SensorID) != nil {
		return TargetKey{}, false
	}
	return TargetKey{GatewayID: batch.GatewayID, SensorID: batch.SensorID}, true
}

// PendingSlots reports queued plus worker-held reservations.
func (r *EventRuntime) PendingSlots() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.slots
}

// PendingBytes reports logical snapshot/DTO/encoded reservation bytes, not RSS.
func (r *EventRuntime) PendingBytes() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

// PendingWork returns queued plus worker-held reserved events.
func (r *EventRuntime) PendingWork() int { return r.PendingSlots() }

// Close stops admission, discards queued work, lets in-progress bounded encodes
// finish, and joins workers subject to ctx. Reservations are released exactly once.
func (r *EventRuntime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.stopOnce.Do(func() { close(r.stop) })
	done := make(chan struct{})
	go func() { r.workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	for {
		select {
		case j := <-r.jobs:
			r.release(j.reservation)
		default:
			return nil
		}
	}
}
