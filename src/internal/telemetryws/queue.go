package telemetryws

import (
	"context"
	"errors"
	"sync"
)

// Default per-session queue limits.
const (
	// DefaultSessionQueueSlots is the default capacity in events for a single session queue.
	DefaultSessionQueueSlots = 64

	// DefaultSessionQueueBytes is the default logical byte cap for pending and in-flight frames (256 KiB).
	DefaultSessionQueueBytes = 256 * 1024
)

// Common queue errors.
var (
	// ErrQueueFull indicates the session queue exceeded slot or logical byte limits.
	ErrQueueFull = errors.New("telemetryws: session queue full")

	// ErrQueueClosed indicates operations on a closed session queue.
	ErrQueueClosed = errors.New("telemetryws: session queue closed")
)

// QueuedFrame pairs an immutable SharedEvent with explicit byte ownership accounting.
//
// IN-FLIGHT WRITE ALLOWANCE & P7.6 INTEGRATION:
// Once a frame is dequeued via Dequeue or DequeueContext, it enters the in-flight state:
// its bytes transition from queuedBytes to inFlightBytes against the session's 256 KiB budget.
// The socket writer (to be wired in P7.6) MUST call frame.Release() exactly once upon completing
// or aborting the network write.
//
// If a session is closed or revoked while a write is in-flight:
//  1. All unsent frames in the queue are discarded immediately, resetting queuedBytes to 0.
//  2. Data already buffered in the network stack or actively being written cannot be recalled over the wire.
//  3. The socket owner will close the transport connection, and the write loop calls frame.Release()
//     which decrements inFlightBytes to 0 without double-free.
type QueuedFrame struct {
	event       *SharedEvent
	queue       *SessionQueue
	byteSize    int
	releaseOnce sync.Once
}

// Event returns the underlying immutable SharedEvent.
func (f *QueuedFrame) Event() *SharedEvent {
	return f.event
}

// Payload returns the pre-encoded immutable JSON byte slice.
func (f *QueuedFrame) Payload() []byte {
	if f.event == nil {
		return nil
	}
	return f.event.Payload()
}

// ByteSize returns the logical size of the frame in bytes.
func (f *QueuedFrame) ByteSize() int {
	return f.byteSize
}

// Release releases the in-flight byte accounting for this frame.
// It is idempotent and safe for concurrent or repeated invocations via sync.Once.
func (f *QueuedFrame) Release() {
	f.releaseOnce.Do(func() {
		if f.queue != nil {
			f.queue.releaseInFlight(f.byteSize)
		}
	})
}

// SessionQueue implements a bounded FIFO event queue for a single WebSocket session.
// It enforces both event slot caps (default 64) and total logical byte caps (default 256 KiB),
// accounting for both queued frames and dequeued in-flight frames.
//
// CONCURRENCY & SAFETY:
//   - All state transitions are protected by an internal mutex.
//   - Signal notifications use a dedicated non-blocking channel separate from ownership buffers.
//   - Enqueue never sends on a closed channel and never performs network I/O.
//   - When queue bounds are exceeded, Enqueue returns ErrQueueFull; the hub isolates the slow session
//     by closing it and requesting REST catch-up, never blocking the publisher or other viewers.
type SessionQueue struct {
	sessionID     string
	mu            sync.Mutex
	items         []*SharedEvent
	queuedBytes   int
	inFlightBytes int
	inFlightCount int
	controlBytes  int
	controlCount  int
	maxSlots      int
	maxBytes      int
	maxFrameBytes int
	closed        bool
	notifyCh      chan struct{}
	closeCh       chan struct{}
	closeOnce     sync.Once
}

// QueueReservation accounts a control frame against the same per-session byte cap.
type QueueReservation struct {
	queue *SessionQueue
	size  int
	once  sync.Once
}

// Release returns the reserved control-frame bytes and slot to the queue.
func (r *QueueReservation) Release() {
	if r == nil || r.queue == nil {
		return
	}
	r.once.Do(func() {
		r.queue.mu.Lock()
		r.queue.controlBytes -= r.size
		r.queue.controlCount--
		r.queue.mu.Unlock()
	})
}

// ReserveControl accounts one queued or in-flight application control frame.
func (q *SessionQueue) ReserveControl(size, maxControls int) (*QueueReservation, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrQueueClosed
	}
	if size < 0 || len(q.items)+q.inFlightCount+q.controlCount >= q.maxSlots || q.controlCount >= maxControls || q.queuedBytes+q.inFlightBytes+q.controlBytes+size > q.maxBytes {
		return nil, ErrQueueFull
	}
	q.controlCount++
	q.controlBytes += size
	return &QueueReservation{queue: q, size: size}, nil
}

// NewSessionQueue initializes a new SessionQueue for sessionID with bounded limits.
func NewSessionQueue(sessionID string, maxSlots, maxBytes, maxFrameBytes int) *SessionQueue {
	if maxSlots <= 0 {
		maxSlots = DefaultSessionQueueSlots
	}
	if maxBytes <= 0 {
		maxBytes = DefaultSessionQueueBytes
	}
	if maxFrameBytes <= 0 {
		maxFrameBytes = DefaultMaxEventBytes
	}

	return &SessionQueue{
		sessionID:     sessionID,
		items:         make([]*SharedEvent, 0, min(maxSlots, 16)),
		maxSlots:      maxSlots,
		maxBytes:      maxBytes,
		maxFrameBytes: maxFrameBytes,
		notifyCh:      make(chan struct{}, 1),
		closeCh:       make(chan struct{}),
	}
}

// SessionID returns the identifier of the session owning this queue.
func (q *SessionQueue) SessionID() string {
	return q.sessionID
}

// Enqueue adds an immutable SharedEvent to the queue.
// Returns ErrQueueClosed if the queue is closed.
// Returns ErrQueueFull if slot capacity or byte capacity would be exceeded.
// Returns EventTooLargeError if the event exceeds maxFrameBytes.
// Enqueue NEVER blocks and NEVER performs network I/O.
func (q *SessionQueue) Enqueue(evt *SharedEvent) error {
	if evt == nil {
		return errors.New("telemetryws: nil event")
	}

	byteSize := evt.ByteSize()
	if q.maxFrameBytes > 0 && byteSize > q.maxFrameBytes {
		return &EventTooLargeError{Size: byteSize, MaxBytes: q.maxFrameBytes}
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}

	// Slots bound all admitted application work, including writer-held telemetry and controls.
	if len(q.items)+q.inFlightCount+q.controlCount >= q.maxSlots {
		q.mu.Unlock()
		return ErrQueueFull
	}

	// Byte cap check (includes both queued and in-flight bytes)
	if q.queuedBytes+q.inFlightBytes+q.controlBytes+byteSize > q.maxBytes {
		q.mu.Unlock()
		return ErrQueueFull
	}

	q.items = append(q.items, evt)
	q.queuedBytes += byteSize
	q.mu.Unlock()

	// Notify reader without holding queue lock and without blocking
	select {
	case q.notifyCh <- struct{}{}:
	default:
	}

	return nil
}

// Dequeue removes the oldest event from the queue and transitions its byte accounting
// to in-flight. The returned QueuedFrame must be released by the caller via frame.Release().
// If the queue is empty, returns (nil, nil) if open, or (nil, ErrQueueClosed) if closed.
func (q *SessionQueue) Dequeue() (*QueuedFrame, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		if q.closed {
			return nil, ErrQueueClosed
		}
		return nil, nil
	}

	evt := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]

	byteSize := evt.ByteSize()
	q.queuedBytes -= byteSize
	q.inFlightBytes += byteSize
	q.inFlightCount++

	return &QueuedFrame{
		event:    evt,
		queue:    q,
		byteSize: byteSize,
	}, nil
}

// DequeueContext blocks until an event is available, the queue is closed, or ctx is canceled.
func (q *SessionQueue) DequeueContext(ctx context.Context) (*QueuedFrame, error) {
	for {
		frame, err := q.Dequeue()
		if err != nil {
			return nil, err
		}
		if frame != nil {
			return frame, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.closeCh:
			return nil, ErrQueueClosed
		case <-q.notifyCh:
			// Loop around to Dequeue under lock
		}
	}
}

// releaseInFlight is called by QueuedFrame.Release() to decrement inFlightBytes.
func (q *SessionQueue) releaseInFlight(byteSize int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.inFlightBytes -= byteSize
	if q.inFlightBytes < 0 {
		q.inFlightBytes = 0
	}
	q.inFlightCount--
	if q.inFlightCount < 0 {
		q.inFlightCount = 0
	}
}

// PurgeMatching removes unsent events from the queue matching the given predicate,
// releasing their queued byte counters immediately.
func (q *SessionQueue) PurgeMatching(predicate func(evt *SharedEvent) bool) int {
	if predicate == nil {
		return 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed || len(q.items) == 0 {
		return 0
	}

	retained := make([]*SharedEvent, 0, len(q.items))
	purgedCount := 0

	for _, item := range q.items {
		if predicate(item) {
			q.queuedBytes -= item.ByteSize()
			purgedCount++
		} else {
			retained = append(retained, item)
		}
	}

	q.items = retained
	if q.queuedBytes < 0 {
		q.queuedBytes = 0
	}

	return purgedCount
}

// Close terminates the queue, clears all unsent items, and releases queued byte counters.
// In-flight items will decrement inFlightBytes when their Release() is called.
// Close is idempotent and safe for repeated calls.
func (q *SessionQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}

	q.closed = true
	for i := range q.items {
		q.items[i] = nil
	}
	q.items = nil
	q.queuedBytes = 0
	q.mu.Unlock()

	q.closeOnce.Do(func() {
		close(q.closeCh)
	})

	return nil
}

// IsClosed reports whether the queue has been closed.
func (q *SessionQueue) IsClosed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

// CloseNotify returns a channel that is closed when the session queue is closed.
func (q *SessionQueue) CloseNotify() <-chan struct{} {
	return q.closeCh
}

// QueuedSlots returns the number of unsent frames currently waiting in the queue.
func (q *SessionQueue) QueuedSlots() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// QueuedBytes returns the logical byte count of unsent frames currently in the queue.
func (q *SessionQueue) QueuedBytes() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queuedBytes
}

// InFlightBytes returns the logical byte count of dequeued frames currently being written.
func (q *SessionQueue) InFlightBytes() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inFlightBytes
}

// ControlBytes returns bytes reserved by queued or in-flight control frames.
func (q *SessionQueue) ControlBytes() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.controlBytes
}

// InFlightCount returns the count of dequeued frames currently being written.
func (q *SessionQueue) InFlightCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.inFlightCount
}

// TotalBytes returns the total logical bytes currently allocated (queued + in-flight).
func (q *SessionQueue) TotalBytes() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queuedBytes + q.inFlightBytes + q.controlBytes
}
