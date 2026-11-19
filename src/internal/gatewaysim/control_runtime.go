package gatewaysim

import (
	"context"
	"encoding/json"
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
type ControlRuntimeStats struct {
	Rejected, QueueFull, Ignored, Conflict, PublishOK, PublishError  int64
	QueryAccepted, QueryRejected, SnapshotPublished, SnapshotDropped int64
}

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
	results, receipts, snapshots               chan resultRequest
	wg                                         sync.WaitGroup
	ignored, conflict, publishOK, publishError atomic.Int64
	queryAccepted, queryRejected               atomic.Int64
	snapshotPublished, snapshotDropped         atomic.Int64
}

type resultRequest struct {
	command  digitaltwin.Command
	topic    string
	payload  []byte
	receipt  bool
	snapshot bool
	expires  time.Time
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
	r := &ControlRuntime{controller: c, inbox: i, out: c.out, pub: pub, timeout: timeout, done: make(chan struct{}), fatal: make(chan struct{}), results: make(chan resultRequest, controlQueueCapacity), receipts: make(chan resultRequest, controlQueueCapacity), snapshots: make(chan resultRequest, controlQueueCapacity)}
	c.enqueueReceipt = r.enqueueReceipt
	return r, nil
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
	return ControlRuntimeStats{
		Rejected: r.inbox.rejected.Load(), QueueFull: r.inbox.full.Load(), Ignored: r.ignored.Load(),
		Conflict: r.conflict.Load(), PublishOK: r.publishOK.Load(), PublishError: r.publishError.Load(),
		QueryAccepted: r.queryAccepted.Load(), QueryRejected: r.queryRejected.Load(),
		SnapshotPublished: r.snapshotPublished.Load(), SnapshotDropped: r.snapshotDropped.Load(),
	}
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
			var envelope struct {
				MessageType string `json:"message_type"`
			}
			_ = json.Unmarshal(raw.payload, &envelope)
			if envelope.MessageType == "configuration_query" {
				r.handleConfigurationQuery(raw)
				continue
			}
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
			case r.results <- resultRequest{command: command, payload: result}:
			default:
				r.inbox.full.Add(1)
			}
		}
	}
}

func (r *ControlRuntime) handleConfigurationQuery(raw rawCommand) {
	if !r.controller.stage6Enabled || !r.pub.Ready() {
		r.queryRejected.Add(1)
		return
	}
	query, err := digitaltwin.ParseMQTTConfigurationQuery(raw.topic, raw.payload, false)
	if err != nil || !query.ValidAt(time.Now()) {
		r.queryRejected.Add(1)
		return
	}
	deadline := query.ExpiresAt
	if capDeadline := time.Now().Add(3 * r.out.opTimeout); capDeadline.Before(deadline) {
		deadline = capDeadline
	}
	ctx, cancel := context.WithDeadline(r.ctx, deadline)
	snapshot, err := r.controller.Readback(ctx, query)
	cancel()
	if err != nil || !query.ValidAt(time.Now()) {
		r.queryRejected.Add(1)
		return
	}
	payload, err := digitaltwin.MarshalMQTTConfigurationSnapshot(snapshot)
	if err != nil {
		r.queryRejected.Add(1)
		return
	}
	response := resultRequest{
		topic:    "gateways/" + query.GatewayID + "/responses/" + query.RequestID,
		payload:  payload,
		snapshot: true,
		expires:  deadline,
	}
	select {
	case r.snapshots <- response:
		r.queryAccepted.Add(1)
	default:
		r.snapshotDropped.Add(1)
	}
}

func (r *ControlRuntime) resultLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	resultRetries := make(map[string]resultRetry)
	receiptRetries := make(map[string]resultRetry)
	nextLane := 0
	nextScan := time.Now().Add(time.Second)
	for {
		if r.ctx.Err() != nil {
			return
		}
		if !time.Now().Before(nextScan) {
			if !r.scanResponses(time.Now(), resultRetries, receiptRetries) {
				return
			}
			nextScan = time.Now().Add(time.Second)
			continue
		}
		if request, lane, ok := r.takeResponse(nextLane); ok {
			nextLane = (lane + 1) % 3
			if request.receipt {
				r.publishResponse(request, receiptRetries)
			} else if request.snapshot {
				r.publishResponse(request, nil)
			} else {
				r.publishResponse(request, resultRetries)
			}
			continue
		}
		select {
		case <-r.ctx.Done():
			return
		case result := <-r.results:
			nextLane = 1
			r.publishResponse(result, resultRetries)
		case receipt := <-r.receipts:
			nextLane = 2
			r.publishResponse(receipt, receiptRetries)
		case snapshot := <-r.snapshots:
			nextLane = 0
			r.publishResponse(snapshot, nil)
		case <-ticker.C:
			if !r.scanResponses(time.Now(), resultRetries, receiptRetries) {
				return
			}
			nextScan = time.Now().Add(time.Second)
		}
	}
}

// scanResponses gives durable retry/expiry work a bounded opportunity even
// while immediate response queues remain nonempty. Stage 5 retains its former
// eight-result scan budget; Stage 6 splits that same total budget fairly.
func (r *ControlRuntime) scanResponses(now time.Time, resultRetries, receiptRetries map[string]resultRetry) bool {
	for id, retry := range resultRetries {
		if !now.Before(retry.expires) {
			delete(resultRetries, id)
		}
	}
	for id, retry := range receiptRetries {
		if !now.Before(retry.expires) {
			delete(receiptRetries, id)
		}
	}
	receipts, err := r.out.ControlReceipts(r.ctx)
	if err != nil {
		if r.ctx.Err() == nil {
			r.fail()
		}
		return false
	}
	maxResults, maxReceipts := 8, 0
	if r.controller.stage6Enabled {
		maxResults, maxReceipts = 4, 4
	}
	var dueReceipts, dueResults []resultRequest
	for _, receipt := range receipts {
		id := receipt.Command.CommandID
		if r.controller.stage6Enabled && len(receipt.ReceiptBytes) > 0 && now.Before(receipt.Command.ExpiresAt) &&
			!now.Before(receiptRetries[id].next) && len(dueReceipts) < maxReceipts {
			dueReceipts = append(dueReceipts, resultRequest{command: receipt.Command, payload: receipt.ReceiptBytes, receipt: true})
		}
		if len(receipt.Result) > 0 && now.Before(receipt.Command.ExpiresAt) &&
			!now.Before(resultRetries[id].next) && len(dueResults) < maxResults {
			dueResults = append(dueResults, resultRequest{command: receipt.Command, payload: receipt.Result})
		}
		if len(dueReceipts) == maxReceipts && len(dueResults) == maxResults {
			break
		}
	}
	for i := 0; i < maxResults || i < maxReceipts; i++ {
		if i < len(dueReceipts) {
			r.publishResponse(dueReceipts[i], receiptRetries)
		}
		if i < len(dueResults) {
			r.publishResponse(dueResults[i], resultRetries)
		}
	}
	return true
}

func (r *ControlRuntime) takeResponse(start int) (resultRequest, int, bool) {
	lanes := [3]<-chan resultRequest{r.results, r.receipts, r.snapshots}
	for offset := 0; offset < len(lanes); offset++ {
		lane := (start + offset) % len(lanes)
		select {
		case req := <-lanes[lane]:
			return req, lane, true
		default:
		}
	}
	return resultRequest{}, 0, false
}

func (r *ControlRuntime) enqueueReceipt(receipt ControlReceipt) {
	if !r.controller.stage6Enabled {
		return
	}
	req := resultRequest{command: receipt.Command, payload: append([]byte(nil), receipt.ReceiptBytes...), receipt: true}
	select {
	case r.receipts <- req:
	default:
		r.inbox.full.Add(1)
	}
}

func (r *ControlRuntime) publishResponse(result resultRequest, retries map[string]resultRetry) {
	if r.ctx.Err() != nil {
		if result.snapshot {
			r.snapshotDropped.Add(1)
		}
		return
	}
	if result.snapshot {
		if !r.controller.stage6Enabled || !time.Now().Before(result.expires) || !r.pub.Ready() {
			r.snapshotDropped.Add(1)
			return
		}
		publishDeadline := result.expires
		if timeoutDeadline := time.Now().Add(r.timeout); timeoutDeadline.Before(publishDeadline) {
			publishDeadline = timeoutDeadline
		}
		ctx, cancel := context.WithDeadline(r.ctx, publishDeadline)
		defer cancel()
		if err := r.pub.Publish(ctx, result.topic, result.payload); err != nil {
			r.snapshotDropped.Add(1)
			return
		}
		r.snapshotPublished.Add(1)
		return
	}
	c := result.command
	now := time.Now()
	if result.receipt && !r.controller.stage6Enabled {
		return
	}
	if result.receipt && !now.Before(c.ExpiresAt) {
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()
	// A duplicate may request one cached result after expiry. Receipt and
	// periodic resends are limited to the original command window.
	if now.Before(c.ExpiresAt) {
		ctx, cancel = context.WithDeadline(ctx, c.ExpiresAt)
		defer cancel()
	}
	topic := "gateways/" + c.GatewayID + "/responses/" + c.CommandID
	var err error
	ready := r.pub.Ready()
	if ready {
		err = r.pub.Publish(ctx, topic, result.payload)
	}
	kind := "result"
	if result.receipt {
		kind = "receipt"
	}
	if !ready || err != nil {
		slog.Warn("gateway publish response error", "kind", kind, "error", err, "ready", ready, "command", c.CommandID)
		r.publishError.Add(1)
	} else {
		slog.Info("gateway publish response ok", "kind", kind, "command", c.CommandID)
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
