// Package mqttcontrol owns bounded command dispatch, not result confirmation.
package mqttcontrol

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/metrics"

	"github.com/google/uuid"
)

// Config bounds per-process dispatch and expiry work; one backend is active.
type Config struct {
	Workers, BatchSize, QueueCapacity, ResultQueueCapacity, MaxPayloadBytes int
	ScanInterval, ExpireInterval, RetryBackoff, PublishTimeout              time.Duration
	WakeupEnabled                                                           bool
}

// Validate rejects unbounded or inconsistent work settings.
func (c Config) Validate() error {
	if c.Workers < 1 || c.Workers > 16 || c.BatchSize < 1 || c.BatchSize > 1024 || c.QueueCapacity < 1 || c.QueueCapacity > 4096 || c.ResultQueueCapacity < 1 || c.ResultQueueCapacity > 4096 || c.MaxPayloadBytes != digitaltwin.MaxControlPayloadBytes {
		return errors.New("invalid control bounds")
	}
	for _, d := range []time.Duration{c.ScanInterval, c.ExpireInterval, c.RetryBackoff, c.PublishTimeout} {
		if d < time.Millisecond || d > time.Minute {
			return errors.New("invalid control timeout")
		}
	}
	return nil
}

// DefaultConfig supplies conservative starting bounds, not a capacity claim.
func DefaultConfig() Config {
	return Config{Workers: 1, BatchSize: 32, QueueCapacity: 32, ResultQueueCapacity: 128, MaxPayloadBytes: 8192, ScanInterval: time.Second, ExpireInterval: time.Second, RetryBackoff: time.Second, PublishTimeout: 3 * time.Second}
}

// Repository never holds a SQL transaction across a Publisher call.
type Repository interface {
	PendingControl(context.Context, int) ([]uuid.UUID, error)
	PrepareControl(context.Context, uuid.UUID) (*digitaltwin.ControlDispatch, error)
	MarkControlPublished(context.Context, uuid.UUID) error
	ExpireControl(context.Context, int) error
}

// Publisher returns nil only after MQTT PUBACK; that is not execution success.
type Publisher interface {
	Ready() bool
	Publish(context.Context, digitaltwin.ControlDispatch) error
}

// Runtime bounds publisher workers, discovery and expiry scans.
type Runtime struct {
	cfg      Config
	repo     Repository
	pub      Publisher
	mu       sync.Mutex
	started  bool
	stopped  bool
	inflight map[uuid.UUID]bool
	running  atomic.Bool
	scanOK   atomic.Bool
	expireOK atomic.Bool
	ctx      context.Context
	cancel   context.CancelFunc
	jobs     chan dispatchJob
	wakeup   chan struct{}
	metrics  *Metrics
	timeline *metrics.ControlTimeline
	done     chan struct{}
	wg       sync.WaitGroup
}

// SetControlTimeline installs a shared observer before fixed workers start.
func (r *Runtime) SetControlTimeline(t *metrics.ControlTimeline) { r.timeline = t }

// New builds an inert runtime.
func New(cfg Config, repo Repository, pub Publisher, observers ...*Metrics) (*Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if nilDependency(repo) || nilDependency(pub) {
		return nil, errors.New("control dependencies required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runtime{cfg: cfg, repo: repo, pub: pub, ctx: ctx, cancel: cancel, jobs: make(chan dispatchJob, cfg.QueueCapacity), wakeup: make(chan struct{}, 1), done: make(chan struct{}), inflight: map[uuid.UUID]bool{}}
	if len(observers) > 0 {
		r.metrics = observers[0]
		if r.metrics != nil {
			r.metrics.queueCapacity.Store(int64(cap(r.jobs)))
		}
	}
	return r, nil
}

// Notify requests a bounded, coalesced discovery pass. It is intentionally
// inert until wake-up dispatch consumption is enabled; no channel is closed.
func (r *Runtime) Notify() {
	if r == nil || !r.cfg.WakeupEnabled || !r.running.Load() {
		return
	}
	select {
	case r.wakeup <- struct{}{}:
		if r.metrics != nil {
			r.metrics.notifications.Add(1)
		}
	default:
		if r.metrics != nil {
			r.metrics.coalesced.Add(1)
		}
	}
}

type dispatchJob struct {
	id         uuid.UUID
	enqueuedAt time.Time
}

func nilDependency(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	return r.Kind() == reflect.Pointer && r.IsNil()
}

// Start launches the fixed worker set.
func (r *Runtime) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped {
		return errors.New("control runtime already started or stopped")
	}
	r.started = true
	r.running.Store(true)
	if r.metrics != nil {
		r.metrics.queueCapacity.Store(int64(cap(r.jobs)))
	}
	r.wg.Add(r.cfg.Workers + 2)
	for i := 0; i < r.cfg.Workers; i++ {
		go r.worker()
	}
	go r.scan()
	go r.expire()
	go func() { r.wg.Wait(); close(r.done) }()
	return nil
}

// Stop stops scans/dispatch and marks readiness false.
func (r *Runtime) Stop() {
	r.mu.Lock()
	r.stopped = true
	r.running.Store(false)
	r.cancel()
	r.mu.Unlock()
}

// Close joins workers within the caller's shared shutdown budget.
func (r *Runtime) Close(ctx context.Context) error {
	r.Stop()
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return nil
	}
	select {
	case <-r.done:
		r.mu.Lock()
		for {
			select {
			case <-r.jobs:
			default:
				goto drained
			}
		}
	drained:
		clear(r.inflight)
		if r.metrics != nil {
			r.metrics.liveWorkers.Store(0)
			r.metrics.inFlight.Store(0)
			r.metrics.queueDepth.Store(int64(len(r.jobs)))
		}
		r.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Ready requires an active runtime and a verified, subscribed transport.
func (r *Runtime) Ready() bool {
	return r.running.Load() && r.scanOK.Load() && r.expireOK.Load() && r.pub.Ready()
}

func (r *Runtime) scan() {
	defer r.wg.Done()
	timer := time.NewTicker(r.cfg.ScanInterval)
	defer timer.Stop()
	for {
		if r.ctx.Err() != nil {
			return
		}
		r.discover()
		select {
		case <-r.ctx.Done():
			return
		case <-timer.C:
		case <-r.wakeup:
		}
	}
}

// discover is called only by the single scan goroutine for startup, timer, and
// notifier hints. Durable rows are always re-read; hints carry no dispatch data.
func (r *Runtime) discover() {
	if !r.pub.Ready() {
		return
	}
	var start time.Time
	if r.metrics != nil {
		start = time.Now()
		r.metrics.scans.Add(1)
	}
	ids, err := r.repo.PendingControl(r.ctx, r.cfg.BatchSize)
	if r.metrics != nil {
		r.metrics.discovery.Observe(time.Since(start))
	}
	r.scanOK.Store(err == nil)
	if err != nil && r.metrics != nil {
		r.metrics.scanErrors.Add(1)
	}
	if err != nil {
		return
	}
	discoveredAt := time.Now()
	for _, id := range ids {
		if r.timeline != nil {
			r.timeline.Discovered(id.String(), discoveredAt)
		}
		r.mu.Lock()
		if !r.inflight[id] && len(r.inflight) < r.cfg.QueueCapacity+r.cfg.Workers {
			r.inflight[id] = true
			job := dispatchJob{id: id}
			if r.metrics != nil {
				job.enqueuedAt = time.Now()
			}
			select {
			case r.jobs <- job:
				if r.metrics != nil {
					r.metrics.enqueued.Add(1)
					r.metrics.queueDepth.Store(int64(len(r.jobs)))
					r.metrics.inFlight.Store(int64(len(r.inflight)))
				}
				if r.timeline != nil {
					r.timeline.Enqueued(id.String(), time.Now())
				}
			default:
				delete(r.inflight, id)
				if r.metrics != nil {
					r.metrics.queueFull.Add(1)
					r.metrics.queueDepth.Store(int64(len(r.jobs)))
					r.metrics.inFlight.Store(int64(len(r.inflight)))
				}
			}
		}
		r.mu.Unlock()
	}
}
func (r *Runtime) worker() {
	defer r.wg.Done()
	if r.metrics != nil {
		r.metrics.liveWorkers.Add(1)
		defer r.metrics.liveWorkers.Add(-1)
	}
	for {
		select {
		case <-r.ctx.Done():
			return
		case job := <-r.jobs:
			if r.metrics != nil {
				r.metrics.queueDepth.Store(int64(len(r.jobs)))
			}
			id := job.id
			if r.metrics != nil {
				r.metrics.queueWait.Observe(time.Since(job.enqueuedAt))
			}
			if r.ctx.Err() != nil {
				return
			}
			var err error
			if r.pub.Ready() {
				var d *digitaltwin.ControlDispatch
				prepareStart := time.Now()
				d, err = r.repo.PrepareControl(r.ctx, id)
				if r.metrics != nil {
					r.metrics.prepare.Observe(time.Since(prepareStart))
				}
				if err == nil && d != nil && r.pub.Ready() {
					ctx, cancel := context.WithTimeout(r.ctx, r.cfg.PublishTimeout)
					var start time.Time
					if r.metrics != nil {
						start = time.Now()
					}
					err = r.pub.Publish(ctx, *d)
					if err == nil && r.timeline != nil {
						r.timeline.PUBACK(id.String(), time.Now())
					}
					if r.metrics != nil {
						r.metrics.publish.Observe(time.Since(start))
						if err == nil {
							r.metrics.puback.Add(1)
						} else {
							r.metrics.publishFailures.Add(1)
						}
					}
					cancel()
					if err == nil {
						markStart := time.Now()
						err = r.repo.MarkControlPublished(r.ctx, id)
						if r.metrics != nil {
							r.metrics.mark.Observe(time.Since(markStart))
							if err == nil {
								r.metrics.markSuccessCalls.Add(1)
							} else {
								r.metrics.markErrors.Add(1)
							}
						}
					}
				}
			}
			if err != nil {
				timer := time.NewTimer(r.cfg.RetryBackoff)
				select {
				case <-timer.C:
				case <-r.ctx.Done():
				}
				timer.Stop()
			}
			r.mu.Lock()
			delete(r.inflight, id)
			if r.metrics != nil {
				r.metrics.inFlight.Store(int64(len(r.inflight)))
			}
			r.mu.Unlock()
			// Once the bounded retry delay has elapsed, one coalesced pass can
			// refill a batch whose durable pending prefix was previously in-flight.
			// Disabled wake-up retains timer-only polling behavior.
			if r.cfg.WakeupEnabled {
				select {
				case r.wakeup <- struct{}{}:
				default:
				}
			}
		}
	}
}
func (r *Runtime) expire() {
	defer r.wg.Done()
	timer := time.NewTicker(r.cfg.ExpireInterval)
	defer timer.Stop()
	for {
		if r.ctx.Err() != nil {
			return
		}
		err := r.repo.ExpireControl(r.ctx, r.cfg.BatchSize)
		r.expireOK.Store(err == nil)
		select {
		case <-r.ctx.Done():
			return
		case <-timer.C:
		}
	}
}
