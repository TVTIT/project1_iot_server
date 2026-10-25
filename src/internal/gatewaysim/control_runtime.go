package gatewaysim

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"iot-platform/internal/digitaltwin"
)

const controlQueueCapacity = 32

type rawCommand struct {
	topic   string
	payload []byte
}

// CommandInbox demultiplexes own commands from the unchanged telemetry ACK path.
// Admit only bounds/copies/enqueues, never parses JSON, writes SQL or publishes.
type CommandInbox struct {
	gateway        string
	ack            ACKAdmitter
	queue          chan rawCommand
	stopped        atomic.Bool
	rejected, full atomic.Int64
}

// NewCommandInbox creates the fixed 32-message, 8-KiB-per-message command queue.
func NewCommandInbox(gateway string, ack ACKAdmitter) *CommandInbox {
	return &CommandInbox{gateway: gateway, ack: ack, queue: make(chan rawCommand, controlQueueCapacity)}
}

// Admit leaves ACK deletion semantics entirely to the existing ACK processor.
func (i *CommandInbox) Admit(topic string, payload []byte, retained bool) {
	if !strings.HasPrefix(topic, "gateways/"+i.gateway+"/commands/") {
		i.ack.Admit(topic, payload, retained)
		return
	}
	if i.stopped.Load() || retained || len(topic) > 256 || len(payload) == 0 || len(payload) > digitaltwin.MaxControlPayloadBytes {
		i.rejected.Add(1)
		return
	}
	select {
	case i.queue <- rawCommand{topic, append([]byte(nil), payload...)}:
	default:
		i.full.Add(1)
	}
}

// ControlRuntimeStats contains fixed counters only, with no device/command labels.
type ControlRuntimeStats struct{ Rejected, QueueFull, Ignored, Conflict, PublishOK, PublishError int64 }

// ControlRuntime has one apply worker and one independent bounded resend worker.
type ControlRuntime struct {
	controller                                 *Controller
	inbox                                      *CommandInbox
	out                                        *Outbox
	pub                                        Publisher
	timeout                                    time.Duration
	mu                                         sync.Mutex
	started, stopped                           bool
	ctx                                        context.Context
	cancel                                     context.CancelFunc
	done, fatal                                chan struct{}
	fatalOnce                                  sync.Once
	results                                    chan resultRequest
	wg                                         sync.WaitGroup
	ignored, conflict, publishOK, publishError atomic.Int64
}

type resultRequest struct {
	command digitaltwin.Command
	payload []byte
}
type resultRetry struct {
	next, expires time.Time
	attempts      int
}

// NewControlRuntime constructs inert workers; startup recovers before sampling.
func NewControlRuntime(c *Controller, i *CommandInbox, pub Publisher, timeout time.Duration) (*ControlRuntime, error) {
	if c == nil || i == nil || pub == nil || timeout <= 0 {
		return nil, digitaltwin.ErrInvalidInput
	}
	return &ControlRuntime{controller: c, inbox: i, out: c.out, pub: pub, timeout: timeout, done: make(chan struct{}), fatal: make(chan struct{}), results: make(chan resultRequest, controlQueueCapacity)}, nil
}

// Start recovers durable work and starts the fixed workers.
func (r *ControlRuntime) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.stopped || ctx == nil {
		return digitaltwin.ErrInvalidInput
	}
	if err := r.controller.Recover(ctx); err != nil {
		return err
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.started = true
	r.wg.Add(2)
	go r.applyLoop()
	go r.resultLoop()
	go func() { r.wg.Wait(); close(r.done) }()
	return nil
}

// Close stops intake/workers under the caller's shared shutdown budget.
func (r *ControlRuntime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.stopped = true
	r.inbox.stopped.Store(true)
	if r.cancel != nil {
		r.cancel()
	}
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

// FatalSignal closes on a nonrecoverable apply/storage error.
func (r *ControlRuntime) FatalSignal() <-chan struct{} { return r.fatal }

// Stats returns a payload-free snapshot.
func (r *ControlRuntime) Stats() ControlRuntimeStats {
	return ControlRuntimeStats{Rejected: r.inbox.rejected.Load(), QueueFull: r.inbox.full.Load(), Ignored: r.ignored.Load(), Conflict: r.conflict.Load(), PublishOK: r.publishOK.Load(), PublishError: r.publishError.Load()}
}

func (r *ControlRuntime) fail() {
	r.inbox.stopped.Store(true)
	r.fatalOnce.Do(func() { close(r.fatal) })
	r.cancel()
}

func (r *ControlRuntime) applyLoop() {
	defer r.wg.Done()
	for {
		if r.ctx.Err() != nil {
			return
		}
		select {
		case <-r.ctx.Done():
			return
		case raw := <-r.inbox.queue:
			ctx, cancel := context.WithTimeout(r.ctx, 3*r.out.opTimeout)
			result, err := r.controller.Handle(ctx, raw.topic, raw.payload, false)
			cancel()
			if err != nil {
				switch {
				case errors.Is(err, ErrControlSensor):
					r.ignored.Add(1)
				case errors.Is(err, ErrControlConflict):
					r.conflict.Add(1)
				case errors.Is(err, digitaltwin.ErrInvalidInput), errors.Is(err, ErrControlFull), errors.Is(err, ErrControlBusy):
					r.inbox.rejected.Add(1)
				default:
					if r.ctx.Err() == nil {
						r.fail()
					}
					return
				}
				continue
			}
			command, err := digitaltwin.ParseMQTTCommand(raw.topic, raw.payload, false)
			if err != nil {
				r.fail()
				return
			}
			select {
			case r.results <- resultRequest{command, result}:
			default:
				r.inbox.full.Add(1)
			}
		}
	}
}

func (r *ControlRuntime) resultLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	retries := make(map[string]resultRetry)
	for {
		if r.ctx.Err() != nil {
			return
		}
		select {
		case <-r.ctx.Done():
			return
		case result := <-r.results:
			r.publishResult(result, retries)
		case <-ticker.C:
			now := time.Now()
			for id, retry := range retries {
				if !now.Before(retry.expires) {
					delete(retries, id)
				}
			}
			receipts, err := r.out.ControlReceipts(r.ctx)
			if err != nil {
				if r.ctx.Err() == nil {
					r.fail()
				}
				return
			}
			attempted := 0
			for _, receipt := range receipts {
				if len(receipt.Result) == 0 || !now.Before(receipt.Command.ExpiresAt) || now.Before(retries[receipt.Command.CommandID].next) {
					continue
				}
				r.publishResult(resultRequest{receipt.Command, receipt.Result}, retries)
				attempted++
				if attempted >= 8 {
					break
				}
			}
		}
	}
}

func (r *ControlRuntime) publishResult(result resultRequest, retries map[string]resultRetry) {
	if r.ctx.Err() != nil {
		return
	}
	c := result.command
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()
	// A duplicate may request one cached response after expiry. Periodic resend
	// is independently limited to the original command window, never renewed.
	if time.Now().Before(c.ExpiresAt) {
		ctx, cancel = context.WithDeadline(ctx, c.ExpiresAt)
		defer cancel()
	}
	topic := "gateways/" + c.GatewayID + "/responses/" + c.CommandID
	err := r.pub.Publish(ctx, topic, result.payload)
	if !r.pub.Ready() || err != nil {
		slog.Warn("gateway publish result error", "error", err, "ready", r.pub.Ready(), "command", c.CommandID)
		r.publishError.Add(1)
	} else {
		slog.Info("gateway publish result ok", "command", c.CommandID)
		r.publishOK.Add(1)
	}
	if time.Now().Before(c.ExpiresAt) {
		old := retries[c.CommandID]
		delay := retryDelay(time.Second, 30*time.Second, old.attempts, func() float64 { return .5 })
		if len(retries) < controlMaxReceipts || old.attempts > 0 {
			retries[c.CommandID] = resultRetry{next: time.Now().Add(delay), expires: c.ExpiresAt, attempts: old.attempts + 1}
		}
	}
}
