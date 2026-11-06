package paperload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gatewaysim"
)

// TelemetryTransport is the narrow application-level contract used by the
// synthetic telemetry engine. Publish confirmation is only MQTT PUBACK.
type TelemetryTransport interface {
	Start() error
	Publish(context.Context, string, []byte) error
	WaitReady(context.Context) error
	StopReconnect()
	Stop(context.Context) error
}

type pendingMessage struct {
	batch            gatewaysim.BuiltBatch
	cohort           string
	firstPublishedAt time.Time
	attempts         int
	nextRetry        time.Time
	queued           bool
	inFlight         bool
}

// TelemetryCounters distinguishes scheduled opportunities, unique payloads,
// publish attempts/PUBACKs and committed application ACKs.
type TelemetryCounters struct {
	Scheduled                   int64  `json:"scheduled_opportunities"`
	Missed                      int64  `json:"missed_opportunities"`
	Deferred                    int64  `json:"deferred_opportunities"`
	Generated                   int64  `json:"generated_unique"`
	OfferedUnique               int64  `json:"offered_unique"`
	GeneratedPayloadBytes       int64  `json:"generated_payload_bytes"`
	OfferedUniquePayloadBytes   int64  `json:"offered_unique_payload_bytes"`
	AttemptedPayloadBytes       int64  `json:"attempted_payload_bytes"`
	ACKQueueDropped             int64  `json:"ack_queue_dropped"`
	UnmatchedACKs               int64  `json:"unmatched_acks"`
	WarmupScheduled             int64  `json:"warmup_scheduled"`
	WarmupGenerated             int64  `json:"warmup_generated"`
	MeasureScheduled            int64  `json:"measure_scheduled"`
	MeasureGenerated            int64  `json:"measure_generated"`
	BurstScheduled              int64  `json:"burst_scheduled"`
	BurstGenerated              int64  `json:"burst_generated"`
	PublishAttempts             int64  `json:"publish_attempts"`
	PublishFailures             int64  `json:"publish_failures"`
	BrokerPUBACKs               int64  `json:"broker_pubacks"`
	ApplicationACKs             int64  `json:"application_acks"`
	ReplayScheduled             int64  `json:"replay_scheduled"`
	ReplayOffered               int64  `json:"replay_offered"`
	ReplayApplicationACKs       int64  `json:"replay_application_acks"`
	IgnoredValidACKs            int64  `json:"ignored_valid_acks"`
	WarmupApplicationACKs       int64  `json:"warmup_application_acks"`
	MeasureApplicationACKs      int64  `json:"measure_application_acks"`
	BurstApplicationACKs        int64  `json:"burst_application_acks"`
	InvalidACKs                 int64  `json:"invalid_acks"`
	RetainedACKs                int64  `json:"retained_acks"`
	Retries                     int64  `json:"retries"`
	PendingMessages             int64  `json:"pending_messages"`
	PendingBytes                int64  `json:"pending_bytes"`
	SchedulingLagMaxNS          int64  `json:"scheduling_lag_max_ns"`
	AckLatencyCount             int64  `json:"ack_latency_count"`
	AckLatencyTotalNS           int64  `json:"ack_latency_total_ns"`
	AckLatencyAvailable         bool   `json:"ack_latency_available"`
	AckLatencyUnavailableReason string `json:"ack_latency_unavailable_reason,omitempty"`
}

// TelemetryEngine is a bounded in-memory retry engine. It provides no
// restart/durable-outbox guarantee; pending work is reported, never discarded
// to claim a successful drain.
type TelemetryEngine struct {
	m               Manifest
	transports      map[string]TelemetryTransport
	mu              sync.Mutex
	pending         map[uuid.UUID]*pendingMessage
	pendingBytes    int64
	jobs            chan uuid.UUID
	acks            chan ackInput
	counts          telemetryAtomic
	boot            map[string]uuid.UUID
	nextSequence    map[string]int64
	targets         map[string][]Target
	stopGeneration  atomic.Bool
	cleanupErrors   []string
	identitySamples []ViewerIdentitySample
	phaseObserver   func(PhaseEvent) error
	phaseOrigin     time.Time
	phaseEvents     []PhaseEvent
	phaseFailed     bool
	journal         *E4Journal
	replay          []E4ReplayBatch
	replayIndex     int
	ignoreACKBudget int
	fatalGeneration error
	anchorTime      time.Time
}

// PhaseEvent is a bounded, identity-free phase snapshot. elapsed_ns is local
// to this telemetry process and must not be compared with another process.
type PhaseEvent struct {
	Event            string            `json:"event"`
	ElapsedNS        int64             `json:"elapsed_ns"`
	ConfiguredPhases Phases            `json:"configured_phases"`
	Counters         TelemetryCounters `json:"counters"`
}

func (e *TelemetryEngine) observePhase(name string) {
	if len(e.phaseEvents) >= 16 {
		e.phaseFailed = true
		return
	}
	event := PhaseEvent{Event: name, ElapsedNS: time.Since(e.phaseOrigin).Nanoseconds(), ConfiguredPhases: e.m.Phases, Counters: e.Counters()}
	e.phaseEvents = append(e.phaseEvents, event)
	if e.phaseObserver != nil && e.phaseObserver(event) != nil {
		e.phaseFailed = true
	}
}

type ackInput struct {
	gatewayID, topic string
	payload          []byte
	retained         bool
	at               time.Time
}
type telemetryAtomic struct {
	scheduled, missed, deferred, generated, offered, ackQueueDropped, unmatchedACKs, attempts, failures, pubacks, appacks, invalid, retained, retries, lag, latencyCount, latencyNS atomic.Int64
	warmupScheduled, warmupGenerated, measureScheduled, measureGenerated, burstScheduled, burstGenerated                                                                            atomic.Int64
	warmupACKs, measureACKs, burstACKs                                                                                                                                              atomic.Int64
	generatedPayloadBytes, offeredPayloadBytes, attemptedPayloadBytes                                                                                                               atomic.Int64
	replayScheduled, replayOffered, replayACKs, ignoredACKs                                                                                                                         atomic.Int64
}

// NewTelemetryEngine validates and constructs the bounded synthetic engine.
func NewTelemetryEngine(m Manifest, transports map[string]TelemetryTransport) (*TelemetryEngine, error) {
	if _, _, err := syntheticStreamBounds(m); err != nil {
		return nil, err
	}
	if len(transports) == 0 {
		return nil, errors.New("telemetry transports required")
	}
	byGateway := map[string][]Target{}
	for _, target := range m.Targets {
		byGateway[target.GatewayID] = append(byGateway[target.GatewayID], target)
	}
	if len(transports) != len(byGateway) {
		return nil, errors.New("one transport per selected gateway required")
	}
	for id := range byGateway {
		if transports[id] == nil {
			return nil, errors.New("gateway transport missing")
		}
	}
	boot := map[string]uuid.UUID{}
	if m.E4.Mode != "replay" {
		for id := range byGateway {
			boot[id] = uuid.New()
		}
	}
	var anchorTime time.Time
	if m.EqualSamples != nil {
		var err error
		anchorTime, err = ParseRFC3339Anchor(m.EqualSamples.TimestampAnchor)
		if err != nil {
			return nil, err
		}
	}
	return &TelemetryEngine{m: m, transports: transports, pending: map[uuid.UUID]*pendingMessage{}, jobs: make(chan uuid.UUID, m.Limits.PendingMessages), acks: make(chan ackInput, m.Limits.PendingMessages), boot: boot, nextSequence: map[string]int64{}, targets: byGateway, anchorTime: anchorTime}, nil
}

// Admit is safe for a Paho callback: bounded copy/enqueue only. Parsing occurs
// in the engine's fixed run loop, never in the transport callback.
func (e *TelemetryEngine) Admit(gatewayID, topic string, payload []byte, retained bool) bool {
	in := ackInput{gatewayID: gatewayID, topic: topic, retained: retained, at: time.Now()}
	if len(payload) <= 1024 {
		in.payload = append([]byte(nil), payload...)
	}
	select {
	case e.acks <- in:
		return true
	default:
		e.counts.ackQueueDropped.Add(1)
		return false
	}
}

// Counters returns an atomic snapshot of workload counters and pending bounds.
func (e *TelemetryEngine) Counters() TelemetryCounters {
	e.mu.Lock()
	pending, bytes := len(e.pending), e.pendingBytes
	e.mu.Unlock()
	c := TelemetryCounters{Scheduled: e.counts.scheduled.Load(), Missed: e.counts.missed.Load(), Deferred: e.counts.deferred.Load(), Generated: e.counts.generated.Load(), OfferedUnique: e.counts.offered.Load(), GeneratedPayloadBytes: e.counts.generatedPayloadBytes.Load(), OfferedUniquePayloadBytes: e.counts.offeredPayloadBytes.Load(), AttemptedPayloadBytes: e.counts.attemptedPayloadBytes.Load(), ACKQueueDropped: e.counts.ackQueueDropped.Load(), UnmatchedACKs: e.counts.unmatchedACKs.Load(), WarmupScheduled: e.counts.warmupScheduled.Load(), WarmupGenerated: e.counts.warmupGenerated.Load(), MeasureScheduled: e.counts.measureScheduled.Load(), MeasureGenerated: e.counts.measureGenerated.Load(), BurstScheduled: e.counts.burstScheduled.Load(), BurstGenerated: e.counts.burstGenerated.Load(), PublishAttempts: e.counts.attempts.Load(), PublishFailures: e.counts.failures.Load(), BrokerPUBACKs: e.counts.pubacks.Load(), ApplicationACKs: e.counts.appacks.Load(), ReplayScheduled: e.counts.replayScheduled.Load(), ReplayOffered: e.counts.replayOffered.Load(), ReplayApplicationACKs: e.counts.replayACKs.Load(), IgnoredValidACKs: e.counts.ignoredACKs.Load(), WarmupApplicationACKs: e.counts.warmupACKs.Load(), MeasureApplicationACKs: e.counts.measureACKs.Load(), BurstApplicationACKs: e.counts.burstACKs.Load(), InvalidACKs: e.counts.invalid.Load(), RetainedACKs: e.counts.retained.Load(), Retries: e.counts.retries.Load(), PendingMessages: int64(pending), PendingBytes: bytes, SchedulingLagMaxNS: e.counts.lag.Load(), AckLatencyCount: e.counts.latencyCount.Load(), AckLatencyTotalNS: e.counts.latencyNS.Load()}
	c.AckLatencyAvailable = c.AckLatencyCount > 0
	if !c.AckLatencyAvailable {
		c.AckLatencyUnavailableReason = "no_application_ack_samples"
	}
	return c
}

// CleanupErrors returns sanitized transport teardown outcomes.
func (e *TelemetryEngine) CleanupErrors() []string { return append([]string(nil), e.cleanupErrors...) }

// IdentitySamples returns the bounded header/hash-only provenance sample.
func (e *TelemetryEngine) IdentitySamples() []ViewerIdentitySample {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]ViewerIdentitySample(nil), e.identitySamples...)
}

// Run executes warmup, measured open-loop generation, finite burst, then a
// bounded ACK drain. Cancellation stops generation first and still drains.
func (e *TelemetryEngine) Run(ctx context.Context) (string, error) {
	e.phaseOrigin = time.Now()
	runCtx, cancelRun := context.WithTimeout(ctx, time.Duration(e.m.Limits.WallSeconds)*time.Second)
	defer cancelRun()
	workCtx, cancelWorkers := context.WithCancel(runCtx)
	var wg sync.WaitGroup
	var finishOnce sync.Once
	finishWorkers := func() { finishOnce.Do(func() { cancelWorkers(); close(e.jobs); wg.Wait() }) }
	defer finishWorkers()
	for i := 0; i < e.m.Limits.Workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); e.worker(workCtx) }()
	}
	for _, t := range e.transports {
		if err := t.WaitReady(runCtx); err != nil {
			e.stopTransports(runCtx)
			finishWorkers()
			return "FAILED", errors.New("mqtt_suback_or_tls_failed")
		}
	}
	e.observePhase("warmup_begin")
	if !e.runOpenLoop(runCtx, time.Duration(e.m.Phases.WarmupSeconds)*time.Second, 0, true) {
		e.stopGeneration.Store(true)
	}
	e.observePhase("warmup_end")
	if !e.stopGeneration.Load() {
		e.observePhase("measure_begin")
		if e.m.EqualSamples != nil {
			_ = e.runEqualSamples(runCtx)
		} else {
			_ = e.runOpenLoop(runCtx, time.Duration(e.m.Phases.MeasureSeconds)*time.Second, time.Duration(e.m.Traffic.RampSeconds)*time.Second, false)
		}
		e.observePhase("measure_end")
	}
	if !e.stopGeneration.Load() {
		e.observePhase("burst_begin")
		for i := 0; i < e.m.Traffic.BurstMessages; i++ {
			e.counts.burstScheduled.Add(1)
			if e.m.E4.Mode == "replay" {
				e.counts.replayScheduled.Add(1)
			}
			before := e.counts.generated.Load()
			if err := e.generate(time.Now(), "burst"); err != nil {
				e.counts.deferred.Add(1)
				if e.counts.generated.Load() > before {
					e.counts.burstGenerated.Add(1)
				}
				break
			}
			e.counts.burstGenerated.Add(1)
		}
		e.observePhase("burst_end")
	}
	e.stopGeneration.Store(true)
	e.observePhase("generation_stopped")
	for _, t := range e.transports {
		t.StopReconnect()
	}
	drainBudget := remainingBudget(runCtx, time.Duration(e.m.Phases.DrainSeconds)*time.Second)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainBudget)
	defer drainCancel()
	drainTick := time.NewTicker(time.Duration(e.m.RetryIntervalMS) * time.Millisecond)
	defer drainTick.Stop()
	e.observePhase("drain_begin")
	for e.Counters().PendingMessages != 0 {
		select {
		case a := <-e.acks:
			e.handleACK(a)
		case now := <-drainTick.C:
			if runCtx.Err() == nil {
				e.retryDue(now)
			}
		case <-drainCtx.Done():
			goto drained
		}
	}
drained:
	e.observePhase("drain_end")
	if e.m.Phases.CooldownSeconds > 0 && runCtx.Err() == nil {
		cooldown := remainingBudget(runCtx, time.Duration(e.m.Phases.CooldownSeconds)*time.Second)
		_ = e.waitPhase(runCtx, cooldown)
	}
	e.mu.Lock()
	remain := len(e.pending)
	e.mu.Unlock()
	cancelWorkers()
	e.stopTransports(runCtx)
	finishWorkers()
	e.observePhase("workers_joined")
	if e.phaseFailed {
		return "PARTIAL", nil
	}
	if len(e.cleanupErrors) > 0 {
		return "PARTIAL", nil
	}
	if remain > 0 {
		return "PARTIAL", nil
	}
	if e.fatalGeneration != nil {
		return "FAILED", e.fatalGeneration
	}
	if runCtx.Err() != nil {
		return "PARTIAL", nil
	}
	// Never label a clean drain COMPLETE when a generator/build/queue error
	// silently reduced the unique workload. The counters remain in workload.json.
	want := int64(math.Ceil(e.m.Traffic.RatePerSecond*float64(e.m.Phases.MeasureSeconds))) +
		int64(math.Ceil(e.m.Traffic.RatePerSecond*float64(e.m.Phases.WarmupSeconds))) +
		int64(e.m.Traffic.BurstMessages)
	if e.m.Experiment == "E4" && e.m.E4.Mode == "unique" {
		want = int64(e.m.E4.MaxMessages)
	}
	if e.m.EqualSamples != nil {
		want = int64(e.m.EqualSamples.ExpectedMessages)
	}
	counters := e.Counters()
	if e.m.E4.Mode == "replay" {
		if counters.ReplayScheduled != int64(len(e.replay)) || counters.ReplayOffered != int64(len(e.replay)) || counters.ReplayApplicationACKs != int64(len(e.replay)) {
			return "FAILED", errors.New("telemetry_replay_incomplete")
		}
	} else if counters.Generated != want || counters.OfferedUnique != want || counters.ApplicationACKs != want {
		return "FAILED", errors.New("telemetry_generation_incomplete")
	}
	return "COMPLETE", nil
}

func (e *TelemetryEngine) runEqualSamples(ctx context.Context) bool {
	eq := e.m.EqualSamples
	if eq == nil {
		return false
	}
	totalMessages := eq.ExpectedMessages
	if totalMessages <= 0 {
		return ctx.Err() == nil
	}
	s := int64(e.m.SamplesPerMessage)
	intervalUS := eq.SampleIntervalUS
	duration := time.Duration(eq.DurationSeconds) * time.Second

	// Separate measure generation deadline with bounded startup/lateness grace
	latenessGrace := 5 * time.Second
	measureDeadlineDuration := duration + latenessGrace

	start := time.Now()
	measureDeadline := start.Add(measureDeadlineDuration)
	if d, ok := ctx.Deadline(); ok && d.Before(measureDeadline) {
		measureDeadline = d
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	nextIndex := 0
	for !e.stopGeneration.Load() && nextIndex < totalMessages {
		select {
		case a := <-e.acks:
			e.handleACK(a)
		case <-ctx.Done():
			e.stopGeneration.Store(true)
			return false
		case now := <-ticker.C:
			for {
				select {
				case a := <-e.acks:
					e.handleACK(a)
				default:
					goto acksDrained
				}
			}
		acksDrained:
			if now.After(measureDeadline) {
				e.counts.missed.Add(int64(totalMessages - nextIndex))
				e.fatalGeneration = errors.New("equal_samples_schedule_deadline_exceeded")
				e.stopGeneration.Store(true)
				return false
			}

			for nextIndex < totalMessages && !e.stopGeneration.Load() {
				dueOffset := EqualSamplesDueOffset(nextIndex, int(s), intervalUS)
				due := start.Add(dueOffset)
				if now.Before(due) {
					break
				}

				lag := now.Sub(due).Nanoseconds()
				if lag > e.counts.lag.Load() {
					e.counts.lag.Store(lag)
				}
				if lag > int64(time.Second) {
					e.counts.missed.Add(1)
				}

				e.counts.scheduled.Add(1)
				e.counts.measureScheduled.Add(1)

				before := e.counts.generated.Load()
				if err := e.generate(now, "measure"); err != nil {
					e.counts.deferred.Add(1)
					e.fatalGeneration = err
					e.stopGeneration.Store(true)
					return false
				}
				if e.counts.generated.Load() > before {
					e.counts.measureGenerated.Add(1)
				}
				nextIndex++
			}
			e.retryDue(now)
		}
	}

	return ctx.Err() == nil && e.fatalGeneration == nil && nextIndex == totalMessages
}

func remainingBudget(ctx context.Context, configured time.Duration) time.Duration {
	if configured < 0 {
		configured = 0
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < configured {
			configured = remaining
		}
	}
	if configured < 0 {
		return 0
	}
	return configured
}

func (e *TelemetryEngine) runOpenLoop(ctx context.Context, duration, ramp time.Duration, warmup bool) bool {
	if duration <= 0 {
		return ctx.Err() == nil
	}
	start := time.Now()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	var opportunity int64
	for !e.stopGeneration.Load() && !e.e4PhaseTargetReached() && time.Since(start) < duration {
		select {
		case a := <-e.acks:
			e.handleACK(a)
		case <-ctx.Done():
			e.stopGeneration.Store(true)
			return false
		case now := <-tick.C:
			should := ScheduledOpportunities(now.Sub(start), e.m.Traffic.RatePerSecond, ramp)
			if first, skipped := BoundedOpportunityRange(opportunity, should, 64); skipped > 0 {
				opportunity = first
				e.counts.scheduled.Add(skipped)
				if e.m.E4.Mode == "replay" {
					e.counts.replayScheduled.Add(skipped)
				}
				e.counts.missed.Add(skipped)
				if warmup {
					e.counts.warmupScheduled.Add(skipped)
				} else {
					e.counts.measureScheduled.Add(skipped)
				}
			}
			for opportunity < should && !e.stopGeneration.Load() && !e.e4PhaseTargetReached() {
				opportunity++
				e.counts.scheduled.Add(1)
				if e.m.E4.Mode == "replay" {
					e.counts.replayScheduled.Add(1)
				}
				if warmup {
					e.counts.warmupScheduled.Add(1)
				} else {
					e.counts.measureScheduled.Add(1)
				}
				due := start.Add(opportunityOffset(opportunity, e.m.Traffic.RatePerSecond, ramp))
				lag := now.Sub(due).Nanoseconds()
				if lag > e.counts.lag.Load() {
					e.counts.lag.Store(lag)
				}
				if lag > int64(time.Second) {
					e.counts.missed.Add(1)
					continue
				}
				before := e.counts.generated.Load()
				cohort := "measure"
				if warmup {
					cohort = "warmup"
				}
				if err := e.generate(now, cohort); err != nil {
					e.counts.deferred.Add(1)
					if err.Error() == "pending_message_cap" || err.Error() == "pending_byte_cap" || err.Error() == "pending_cap" {
						e.fatalGeneration = err
						e.stopGeneration.Store(true)
						break
					}
				}
				if e.counts.generated.Load() > before {
					if warmup {
						e.counts.warmupGenerated.Add(1)
					} else {
						e.counts.measureGenerated.Add(1)
					}
				}
			}
			e.retryDue(now)
		}
	}
	if e.e4PhaseTargetReached() {
		return ctx.Err() == nil
	}
	if !e.stopGeneration.Load() && ctx.Err() == nil {
		should := ScheduledOpportunities(duration, e.m.Traffic.RatePerSecond, ramp)
		if first, skipped := BoundedOpportunityRange(opportunity, should, 64); skipped > 0 {
			opportunity = first
			e.counts.scheduled.Add(skipped)
			if e.m.E4.Mode == "replay" {
				e.counts.replayScheduled.Add(skipped)
			}
			e.counts.missed.Add(skipped)
			if warmup {
				e.counts.warmupScheduled.Add(skipped)
			} else {
				e.counts.measureScheduled.Add(skipped)
			}
		}
		now := time.Now()
		for opportunity < should && !e.stopGeneration.Load() && !e.e4PhaseTargetReached() {
			opportunity++
			e.counts.scheduled.Add(1)
			if e.m.E4.Mode == "replay" {
				e.counts.replayScheduled.Add(1)
			}
			if warmup {
				e.counts.warmupScheduled.Add(1)
			} else {
				e.counts.measureScheduled.Add(1)
			}
			due := start.Add(opportunityOffset(opportunity, e.m.Traffic.RatePerSecond, ramp))
			lag := now.Sub(due).Nanoseconds()
			if lag > e.counts.lag.Load() {
				e.counts.lag.Store(lag)
			}
			if lag > int64(time.Second) {
				e.counts.missed.Add(1)
				continue
			}
			before := e.counts.generated.Load()
			cohort := "measure"
			if warmup {
				cohort = "warmup"
			}
			if err := e.generate(now, cohort); err != nil {
				e.counts.deferred.Add(1)
				if err.Error() == "pending_message_cap" || err.Error() == "pending_byte_cap" || err.Error() == "pending_cap" {
					e.fatalGeneration = err
					e.stopGeneration.Store(true)
					break
				}
			}
			if e.counts.generated.Load() > before {
				if warmup {
					e.counts.warmupGenerated.Add(1)
				} else {
					e.counts.measureGenerated.Add(1)
				}
			}
		}
	}
	return ctx.Err() == nil
}

func (e *TelemetryEngine) e4PhaseTargetReached() bool {
	if e.m.Experiment != "E4" {
		return false
	}
	switch e.m.E4.Mode {
	case "unique":
		return e.m.E4.MaxMessages > 0 && e.counts.generated.Load() >= int64(e.m.E4.MaxMessages)
	case "replay":
		return e.m.E4.ReplayCount > 0 && e.counts.replayScheduled.Load() >= int64(e.m.E4.ReplayCount)
	default:
		return false
	}
}

// BoundedOpportunityRange skips stale schedule debt instead of allowing a
// delayed scheduler to perform an unbounded catch-up burst.
func BoundedOpportunityRange(completed, target, maxCatchup int64) (first int64, skipped int64) {
	if target <= completed {
		return completed, 0
	}
	if maxCatchup < 1 {
		maxCatchup = 1
	}
	debt := target - completed
	if debt > maxCatchup {
		skipped = debt - maxCatchup
	}
	return completed + skipped, skipped
}
func opportunityOffset(n int64, rate float64, ramp time.Duration) time.Duration {
	if rate <= 0 {
		return 0
	}
	x := float64(n) / rate
	r := ramp.Seconds()
	seconds := x
	if r > 0 {
		if x <= r/2 {
			seconds = math.Sqrt(2 * r * x)
		} else {
			seconds = x + r/2
		}
	}
	return time.Duration(seconds * float64(time.Second))
}

// ScheduledOpportunities returns the integrated open-loop opportunity count.
// A ramp linearly increases the target rate from zero to the configured rate.
func ScheduledOpportunities(elapsed time.Duration, rate float64, ramp time.Duration) int64 {
	if elapsed <= 0 || rate <= 0 {
		return 0
	}
	seconds := elapsed.Seconds()
	rampSeconds := ramp.Seconds()
	total := seconds
	if rampSeconds > 0 {
		if seconds < rampSeconds {
			total = seconds * seconds / (2 * rampSeconds)
		} else {
			total = seconds - rampSeconds/2
		}
	}
	return int64(math.Floor(total * rate))
}

func (e *TelemetryEngine) stopTransports(parent context.Context) {
	for _, t := range e.transports {
		budget := remainingBudget(parent, 2*time.Second)
		c, cancel := context.WithTimeout(context.Background(), budget)
		if t.Stop(c) != nil {
			e.cleanupErrors = append(e.cleanupErrors, "mqtt_transport_stop_failed")
		}
		cancel()
	}
}
func (e *TelemetryEngine) waitPhase(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case a := <-e.acks:
			e.handleACK(a)
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
}

func (e *TelemetryEngine) generate(now time.Time, cohort string) error {
	if e.m.E4.Mode == "replay" {
		return e.enqueueReplay(now, cohort)
	}
	if !e.stopGeneration.Load() && e.Counters().PendingMessages >= int64(e.m.Limits.PendingMessages) {
		return errors.New("pending_message_cap")
	}
	gateways := make([]string, 0, len(e.targets))
	for id := range e.targets {
		gateways = append(gateways, id)
	}
	sort.Strings(gateways)
	if len(gateways) == 0 {
		return errors.New("no_targets")
	}
	idx := int(e.counts.generated.Load() % int64(len(gateways)))
	gw := gateways[idx]
	targets := e.targets[gw]
	target := targets[(e.counts.generated.Load()/int64(len(gateways)))%int64(len(targets))]
	key := gw + "\x00" + target.SensorID

	var first time.Time
	var interval int64
	seq := e.nextSequence[key]
	sampleCount := int64(e.m.SamplesPerMessage)

	if e.m.EqualSamples != nil {
		expectedMessages := int64(e.m.EqualSamples.ExpectedMessages)
		expectedSamples := int64(e.m.EqualSamples.ExpectedSamples)
		if e.counts.generated.Load() >= expectedMessages {
			return errors.New("equal_samples_count_exhausted")
		}
		if seq < 0 || sampleCount > math.MaxInt64-seq || seq+sampleCount > expectedSamples {
			return errors.New("sequence_range_exhausted")
		}
		interval = e.m.EqualSamples.SampleIntervalUS
		offsetUS := seq * interval
		first = e.anchorTime.Add(time.Duration(offsetUS) * time.Microsecond).UTC()
	} else {
		interval = int64(500000) // 2 Hz synthetic samples, independent from offered batch rate.
		sampleBound, horizon, err := syntheticStreamBounds(e.m)
		if err != nil {
			return errors.New("synthetic_horizon_rejected")
		}
		const intervalNS int64 = 500 * int64(time.Millisecond)
		if seq < 0 || sampleCount > math.MaxInt64-seq || seq > sampleBound-sampleCount || seq > (int64(horizon)-int64(time.Second))/intervalNS {
			return errors.New("sequence_range_exhausted")
		}
		offset := time.Duration(seq * intervalNS)
		first = now.Add(-horizon).Add(offset).UTC().Truncate(time.Microsecond)
	}

	messageID, err := uuid.NewV7()
	if err != nil {
		messageID = uuid.New()
	}
	batch, err := gatewaysim.BuildSyntheticBatch(gatewaysim.BatchRequest{GatewayID: gw, SensorID: target.SensorID, BootID: e.boot[gw], MessageID: messageID, FirstSequence: seq, MeasuredAt: first, SampleCount: int32(e.m.SamplesPerMessage), SampleIntervalUS: interval}, gatewaysim.ServerLimits(), now)
	if err != nil {
		return errors.New("synthetic_batch_rejected")
	}
	e.nextSequence[key] = seq + sampleCount
	if int64(len(batch.Payload)) > e.m.Limits.PendingBytes {
		e.nextSequence[key] = seq
		return errors.New("pending_byte_cap")
	}
	e.mu.Lock()
	if len(e.pending) >= e.m.Limits.PendingMessages || e.pendingBytes+int64(len(batch.Payload)) > e.m.Limits.PendingBytes {
		e.mu.Unlock()
		e.nextSequence[key] = seq
		return errors.New("pending_cap")
	}
	if e.journal != nil {
		if err := e.journal.Append(batch); err != nil {
			e.mu.Unlock()
			e.nextSequence[key] = seq
			e.fatalGeneration = err
			e.stopGeneration.Store(true)
			return err
		}
	}
	e.pending[messageID] = &pendingMessage{batch: batch, cohort: cohort, nextRetry: now.Add(time.Duration(e.m.RetryIntervalMS) * time.Millisecond), queued: true}
	if len(e.identitySamples) < 20 {
		identity := struct {
			GatewayID     string `json:"gateway_id"`
			SensorID      string `json:"sensor_id"`
			MessageID     string `json:"message_id"`
			BootID        string `json:"boot_id"`
			FirstSequence string `json:"first_sequence"`
			SampleCount   int32  `json:"sample_count"`
		}{gw, target.SensorID, messageID.String(), e.boot[gw].String(), strconv.FormatInt(seq, 10), int32(e.m.SamplesPerMessage)}
		header, _ := json.Marshal(identity)
		hash := sha256.Sum256(header)
		e.identitySamples = append(e.identitySamples, ViewerIdentitySample{GatewayID: gw, SensorID: target.SensorID, MessageID: messageID.String(), BootID: e.boot[gw].String(), FirstSequence: identity.FirstSequence, SampleCount: identity.SampleCount, HeaderSHA256: hex.EncodeToString(hash[:])})
	}
	e.pendingBytes += int64(len(batch.Payload))
	e.mu.Unlock()
	e.counts.generated.Add(1)
	e.counts.generatedPayloadBytes.Add(int64(len(batch.Payload)))
	select {
	case e.jobs <- messageID:
		return nil
	default:
		e.mu.Lock()
		delete(e.pending, messageID)
		e.pendingBytes -= int64(len(batch.Payload))
		e.nextSequence[key] = seq
		e.mu.Unlock()
		return errors.New("publish_queue_full")
	}
}

func (e *TelemetryEngine) enqueueReplay(now time.Time, cohort string) error {
	if e.replayIndex >= len(e.replay) {
		return errors.New("replay_entries_exhausted")
	}
	entry := e.replay[e.replayIndex]
	batch := entry.Batch
	if int64(len(batch.Payload)) > e.m.Limits.PendingBytes {
		return errors.New("pending_byte_cap")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.pending) >= e.m.Limits.PendingMessages || e.pendingBytes+int64(len(batch.Payload)) > e.m.Limits.PendingBytes {
		return errors.New("pending_cap")
	}
	if _, exists := e.pending[batch.Batch.MessageID]; exists {
		return errors.New("replay_duplicate_id")
	}
	e.pending[batch.Batch.MessageID] = &pendingMessage{batch: batch, cohort: cohort, nextRetry: now.Add(time.Duration(e.m.RetryIntervalMS) * time.Millisecond), queued: true}
	e.pendingBytes += int64(len(batch.Payload))
	e.replayIndex++
	select {
	case e.jobs <- batch.Batch.MessageID:
		return nil
	default:
		delete(e.pending, batch.Batch.MessageID)
		e.pendingBytes -= int64(len(batch.Payload))
		e.replayIndex--
		return errors.New("publish_queue_full")
	}
}

func syntheticStreamBounds(m Manifest) (int64, time.Duration, error) {
	if (m.SamplesPerMessage != 1 && m.SamplesPerMessage != 100) || m.Traffic.RatePerSecond <= 0 || math.IsNaN(m.Traffic.RatePerSecond) || math.IsInf(m.Traffic.RatePerSecond, 0) || m.Phases.WarmupSeconds < 0 || m.Phases.MeasureSeconds < 0 || m.Traffic.BurstMessages < 0 {
		return 0, 0, errors.New("synthetic stream bounds rejected")
	}
	warm := math.Ceil(m.Traffic.RatePerSecond * float64(m.Phases.WarmupSeconds))
	measure := math.Ceil(m.Traffic.RatePerSecond * float64(m.Phases.MeasureSeconds))
	if warm >= float64(math.MaxInt64) || measure >= float64(math.MaxInt64) {
		return 0, 0, errors.New("synthetic stream message bound overflow")
	}
	warmMessages, measureMessages := int64(warm), int64(measure)
	burst := int64(m.Traffic.BurstMessages)
	if warmMessages > math.MaxInt64-measureMessages || warmMessages+measureMessages > math.MaxInt64-burst {
		return 0, 0, errors.New("synthetic stream message bound overflow")
	}
	messages := warmMessages + measureMessages + burst
	sampleCount := int64(m.SamplesPerMessage)
	if messages > math.MaxInt64/sampleCount {
		return 0, 0, errors.New("synthetic sample bound overflow")
	}
	samples := messages * sampleCount
	const sampleIntervalNS int64 = 500 * int64(time.Millisecond)
	const reserveNS int64 = int64(time.Second)
	if samples > (math.MaxInt64-reserveNS)/sampleIntervalNS {
		return 0, 0, errors.New("synthetic timestamp horizon overflow")
	}
	return samples, time.Duration(samples*sampleIntervalNS + reserveNS), nil
}

func (e *TelemetryEngine) worker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case id, ok := <-e.jobs:
			if !ok || ctx.Err() != nil {
				return
			}
			e.processJob(ctx, id)
		}
	}
}

func (e *TelemetryEngine) processJob(ctx context.Context, id uuid.UUID) bool {
	if ctx.Err() != nil {
		return false
	}
	e.mu.Lock()
	p := e.pending[id]
	if p == nil || !p.queued || p.inFlight {
		e.mu.Unlock()
		return false
	}
	p.queued = false
	p.inFlight = true
	retry := p.attempts > 0
	p.attempts++
	if p.firstPublishedAt.IsZero() {
		p.firstPublishedAt = time.Now()
		if e.m.E4.Mode == "replay" {
			e.counts.replayOffered.Add(1)
		} else {
			e.counts.offered.Add(1)
			e.counts.offeredPayloadBytes.Add(int64(len(p.batch.Payload)))
		}
	}
	e.counts.attempts.Add(1)
	e.counts.attemptedPayloadBytes.Add(int64(len(p.batch.Payload)))
	if retry {
		e.counts.retries.Add(1)
	}
	e.mu.Unlock()
	err := e.transports[p.batch.Batch.GatewayID].Publish(ctx, p.batch.Topic, p.batch.Payload)
	e.handlePublishResult(id, err)
	return true
}

func (e *TelemetryEngine) handlePublishResult(id uuid.UUID, publishErr error) {
	e.mu.Lock()
	p := e.pending[id]
	if p != nil {
		p.inFlight = false
		p.nextRetry = time.Now().Add(time.Duration(e.m.RetryIntervalMS) * time.Millisecond)
	}
	e.mu.Unlock()
	if publishErr != nil {
		e.counts.failures.Add(1)
	} else {
		e.counts.pubacks.Add(1)
	}
}
func (e *TelemetryEngine) retryDue(now time.Time) {
	e.mu.Lock()
	deferred := int64(0)
	for id, p := range e.pending {
		if p.attempts > 0 && !p.queued && !p.inFlight && !p.nextRetry.After(now) && p.attempts <= e.m.Limits.Retries {
			p.queued = true
			select {
			case e.jobs <- id:
			default:
				p.queued = false
				p.nextRetry = now.Add(time.Duration(e.m.RetryIntervalMS) * time.Millisecond)
				deferred++
			}
		}
	}
	e.mu.Unlock()
	e.counts.deferred.Add(deferred)
}
func (e *TelemetryEngine) handleACK(a ackInput) {
	if a.retained {
		e.counts.retained.Add(1)
		return
	}
	ack, err := gatewaysim.ParseCommitACK(a.topic, a.payload, a.gatewayID)
	if err != nil {
		e.counts.invalid.Add(1)
		return
	}
	e.mu.Lock()
	p := e.pending[ack.MessageID]
	if p != nil && (p.batch.Batch.GatewayID != a.gatewayID || p.attempts == 0) {
		p = nil
		e.counts.unmatchedACKs.Add(1)
	}
	if p != nil {
		if e.ignoreACKBudget > 0 {
			e.ignoreACKBudget--
			e.counts.ignoredACKs.Add(1)
			e.mu.Unlock()
			return
		}
		delete(e.pending, ack.MessageID)
		e.pendingBytes -= int64(len(p.batch.Payload))
	}
	e.mu.Unlock()
	if p == nil {
		return
	}
	if e.m.E4.Mode == "replay" {
		e.counts.replayACKs.Add(1)
	} else {
		e.counts.appacks.Add(1)
		switch p.cohort {
		case "warmup":
			e.counts.warmupACKs.Add(1)
		case "measure":
			e.counts.measureACKs.Add(1)
		case "burst":
			e.counts.burstACKs.Add(1)
		}
	}
	d := a.at.Sub(p.firstPublishedAt).Nanoseconds()
	if !p.firstPublishedAt.IsZero() && d >= 0 {
		e.counts.latencyCount.Add(1)
		e.counts.latencyNS.Add(d)
	}
}
