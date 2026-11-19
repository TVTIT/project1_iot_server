package mqttingress

import (
	"context"
	"errors"
	"sync"
)

var errSchedulerNoReadySensor = errors.New("mqttingress: no ready sensor")

type sensorKey struct {
	gatewayID string
	sensorID  string
}

type sensorQueue struct {
	key    sensorKey
	items  []item
	leased bool
	ready  bool
}

// sensorScheduler bounds queued items globally and active Sensor leases by the
// fixed worker count. Its changed channel is a broadcast generation: waiters
// snapshot it while holding mu, then wait outside the lock to avoid lost wakes.
type sensorScheduler struct {
	mu       sync.Mutex
	capacity int
	workers  int
	pending  int
	active   int
	queues   map[sensorKey]*sensorQueue
	ready    []*sensorQueue
	changed  chan struct{}
}

// sensorLease owns one Sensor until Release. The first item is removed at
// acquisition; Pop removes additional FIFO items from only that leased Sensor.
type sensorLease struct {
	scheduler *sensorScheduler
	state     *sensorQueue
	GatewayID string
	SensorID  string
	Item      item
	released  bool
}

func newSensorScheduler(capacity, workers int) *sensorScheduler {
	if capacity < 1 || workers < 1 {
		panic("mqttingress: scheduler bounds must be positive")
	}
	return &sensorScheduler{capacity: capacity, workers: workers, queues: make(map[sensorKey]*sensorQueue), changed: make(chan struct{})}
}

// Enqueue is nonblocking. On rejection ownership of value remains with caller.
// The returned depth is aggregate pending items, excluding items already
// acquired by leases.
func (s *sensorScheduler) Enqueue(gatewayID, sensorID string, value item) (accepted bool, depth int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == s.capacity {
		return false, s.pending
	}
	k := sensorKey{gatewayID, sensorID}
	q := s.queues[k]
	if q == nil {
		q = &sensorQueue{key: k}
		s.queues[k] = q
	}
	q.items = append(q.items, value)
	s.pending++
	if !q.leased && !q.ready {
		s.ready = append(s.ready, q)
		q.ready = true
	}
	s.signalLocked()
	return true, s.pending
}

// TryAcquire starts a lease without waiting. Acquired items leave pending depth.
func (s *sensorScheduler) TryAcquire() (*sensorLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active >= s.workers || len(s.ready) == 0 {
		return nil, errSchedulerNoReadySensor
	}
	return s.acquireLocked(), nil
}

// Acquire waits for a ready Sensor, available worker lease, or context cancel.
func (s *sensorScheduler) Acquire(ctx context.Context) (*sensorLease, error) {
	for {
		s.mu.Lock()
		if s.active < s.workers && len(s.ready) > 0 {
			l := s.acquireLocked()
			s.mu.Unlock()
			return l, nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// AcquireUntil is Acquire with an atomic stop-and-empty check. A closed stop
// channel drains ready work, then returns (nil, nil); changed is always
// snapshotted under mu before waiting, so enqueue/release cannot be lost.
func (s *sensorScheduler) AcquireUntil(ctx context.Context, stop <-chan struct{}) (*sensorLease, error) {
	for {
		s.mu.Lock()
		if s.active < s.workers && len(s.ready) > 0 {
			l := s.acquireLocked()
			s.mu.Unlock()
			return l, nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-stop:
			// Recheck under the scheduler lock: an accepted item may have been
			// enqueued immediately before stop closed.
			s.mu.Lock()
			empty := len(s.ready) == 0
			s.mu.Unlock()
			if empty {
				return nil, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// PopOrChanged atomically observes either an item or the generation channel
// used to wait for the next item, avoiding a pop-then-sleep lost wake.
func (l *sensorLease) PopOrChanged() (item, bool, <-chan struct{}) {
	s := l.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.released || len(l.state.items) == 0 {
		return item{}, false, s.changed
	}
	v := l.state.items[0]
	l.state.items[0] = item{}
	l.state.items = l.state.items[1:]
	s.pending--
	return v, true, s.changed
}

func (s *sensorScheduler) acquireLocked() *sensorLease {
	q := s.ready[0]
	copy(s.ready, s.ready[1:])
	s.ready[len(s.ready)-1] = nil
	s.ready = s.ready[:len(s.ready)-1]
	q.ready = false
	value := q.items[0]
	q.items[0] = item{}
	q.items = q.items[1:]
	s.pending--
	q.leased = true
	s.active++
	s.signalLocked()
	return &sensorLease{scheduler: s, state: q, GatewayID: q.key.gatewayID, SensorID: q.key.sensorID, Item: value}
}

func (l *sensorLease) Pop() (item, bool) {
	s := l.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.released || len(l.state.items) == 0 {
		return item{}, false
	}
	value := l.state.items[0]
	l.state.items[0] = item{}
	l.state.items = l.state.items[1:]
	s.pending--
	return value, true
}

// Release ends exclusivity and queues remaining work at the FIFO tail.
func (l *sensorLease) Release() {
	s := l.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	q := l.state
	q.leased = false
	s.active--
	if len(q.items) != 0 {
		if !q.ready {
			q.ready = true
			s.ready = append(s.ready, q)
		}
	} else {
		delete(s.queues, q.key)
		q.items = nil
	}
	s.signalLocked()
}

func (l *sensorLease) PendingDepth() int     { return l.scheduler.PendingDepth() }
func (s *sensorScheduler) PendingDepth() int { s.mu.Lock(); defer s.mu.Unlock(); return s.pending }
func (s *sensorScheduler) Capacity() int     { return s.capacity }

// DiscardPending transfers all queued items to the caller exactly once. Items
// already held by leases are not included; leased empty states live to Release.
func (s *sensorScheduler) DiscardPending() []item {
	s.mu.Lock()
	defer s.mu.Unlock()
	discarded := make([]item, 0, s.pending)
	for key, q := range s.queues {
		for n := range q.items {
			discarded = append(discarded, q.items[n])
			q.items[n] = item{}
		}
		q.items = nil
		q.ready = false
		if !q.leased {
			delete(s.queues, key)
		}
	}
	s.ready = nil
	s.pending = 0
	s.signalLocked()
	return discarded
}

func (s *sensorScheduler) signalLocked() { close(s.changed); s.changed = make(chan struct{}) }
