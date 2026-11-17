package mqttcontrol

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

type receiptRepoFunc func(context.Context, string, []byte, time.Time) (digitaltwin.ControlReceiptOutcome, error)

func (f receiptRepoFunc) AcceptControlReceipt(ctx context.Context, topic string, payload []byte, at time.Time) (digitaltwin.ControlReceiptOutcome, error) {
	return f(ctx, topic, payload, at)
}

type resultRepoFunc func(context.Context, string, []byte, time.Time) (digitaltwin.ControlResultOutcome, error)

func (f resultRepoFunc) AcceptControlResult(ctx context.Context, topic string, payload []byte, at time.Time) (digitaltwin.ControlResultOutcome, error) {
	return f(ctx, topic, payload, at)
}

func validRawResult(t *testing.T) RawResult {
	t.Helper()
	r := digitaltwin.CommandResult{ProtocolVersion: 1, MessageType: "command_result", CommandID: uuid.NewString(), GatewayID: "fixture_gw", EntityID: "urn:ngsi-ld:Sensor:fixture_gw:s1", DesiredVersion: 1, Status: digitaltwin.CommandSucceeded, ReportedState: &digitaltwin.SamplingParameters{SampleIntervalUS: 10000}, CompletedAt: time.Now().UTC().Truncate(time.Microsecond)}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return RawResult{Topic: "gateways/fixture_gw/responses/" + r.CommandID, Payload: b, ReceivedAt: time.Now()}
}

func validRawReceipt(t *testing.T) RawResult {
	t.Helper()
	id := uuid.NewString()
	now := time.Now().UTC().Truncate(time.Microsecond)
	commandBytes := []byte(`{"original":"command bytes"}`)
	payload, err := digitaltwin.MarshalMQTTReceipt(digitaltwin.CommandReceipt{
		ProtocolVersion: 1,
		MessageType:     "command_receipt",
		CommandID:       id,
		GatewayID:       "fixture_gw",
		EntityID:        "urn:ngsi-ld:Sensor:fixture_gw:s1",
		DesiredVersion:  1,
		ReceivedAt:      now,
		CommandHash:     fmt.Sprintf("%x", sha256.Sum256(commandBytes)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return RawResult{Topic: "gateways/fixture_gw/responses/" + id, Payload: payload, ReceivedAt: now}
}

func waitResult(t *testing.T, what string, f func() bool) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(what)
}

func TestResultConsumerRealWorkersValidateAndClassify(t *testing.T) {
	m := &ResultMetrics{}
	q := make(chan RawResult, 16)
	queuedAt := time.Now().Add(-20 * time.Millisecond)
	var calls atomic.Int64
	repo := resultRepoFunc(func(ctx context.Context, _ string, _ []byte, _ time.Time) (digitaltwin.ControlResultOutcome, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("DB call without deadline")
		}
		switch calls.Add(1) {
		case 1:
			return digitaltwin.ControlResultAccepted, nil
		case 2:
			return digitaltwin.ControlResultDuplicate, nil
		case 3:
			return digitaltwin.ControlResultLate, nil
		case 4:
			return "", digitaltwin.ErrControlResultConflict
		default:
			return "", errors.New("injected DB failure")
		}
	})
	c, err := NewResultConsumer(2, time.Second, repo, q, m)
	if err != nil {
		t.Fatal(err)
	}
	if c.Ready() {
		t.Fatal("inert consumer ready")
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	waitResult(t, "workers did not start", c.Ready)
	bad := validRawResult(t)
	bad.Payload = []byte("malformed")
	bad.QueuedAt = queuedAt
	q <- bad
	for i := 0; i < 5; i++ {
		result := validRawResult(t)
		result.QueuedAt = queuedAt
		q <- result
	}
	waitResult(t, "result outcomes not consumed", func() bool {
		s := m.Snapshot()
		return s.Accepted == 1 && s.Duplicate == 1 && s.Late == 1 && s.Rejected == 2 && s.DBError == 1
	})
	if calls.Load() != 5 {
		t.Fatal("malformed entered transaction")
	}
	if snapshot := m.Snapshot(); snapshot.QueueWait.Count != 6 || snapshot.Processing.Count != 6 || snapshot.ReceivedToConfirmedCommit.Count != 0 {
		t.Fatalf("timings = %+v; fake repo does not prove commit point", snapshot)
	}
}

func TestResponseConsumerRoutesStrictReceiptSeparatelyFromResultMetrics(t *testing.T) {
	resultMetrics, receiptMetrics := &ResultMetrics{}, &ReceiptMetrics{}
	var resultCalls, receiptCalls atomic.Int64
	resultRepo := resultRepoFunc(func(context.Context, string, []byte, time.Time) (digitaltwin.ControlResultOutcome, error) {
		resultCalls.Add(1)
		return digitaltwin.ControlResultAccepted, nil
	})
	receiptRepo := receiptRepoFunc(func(context.Context, string, []byte, time.Time) (digitaltwin.ControlReceiptOutcome, error) {
		switch receiptCalls.Add(1) {
		case 1:
			return digitaltwin.ControlReceiptAccepted, nil
		case 2:
			return digitaltwin.ControlReceiptDuplicate, nil
		case 3:
			return digitaltwin.ControlReceiptLate, nil
		default:
			return "", digitaltwin.ErrControlResultConflict
		}
	})
	q := make(chan RawResult, 16)
	c, err := NewResultConsumer(1, time.Second, resultRepo, q, resultMetrics)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ConfigureReceipts(true, receiptRepo, receiptMetrics); err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	receipt := validRawReceipt(t)
	q <- receipt
	q <- receipt
	q <- receipt
	badDuplicateKey := receipt
	badDuplicateKey.Payload = append(append([]byte(nil), receipt.Payload[:len(receipt.Payload)-1]...), []byte(`,"message_type":"command_receipt"}`)...)
	q <- badDuplicateKey
	wrongIdentity := receipt
	wrongIdentity.Topic = "gateways/other_gateway/responses/" + strings.TrimPrefix(receipt.Topic, "gateways/fixture_gw/responses/")
	q <- wrongIdentity
	retained := receipt
	retained.Retained = true
	q <- retained

	result := validRawResult(t)
	q <- result
	waitResult(t, "response outcomes not consumed", func() bool {
		return receiptMetrics.Snapshot().Accepted == 1 &&
			receiptMetrics.Snapshot().Duplicate == 1 &&
			receiptMetrics.Snapshot().Late == 1 &&
			receiptMetrics.Snapshot().Rejected == 3 &&
			resultMetrics.Snapshot().Accepted == 1
	})
	if receiptCalls.Load() != 3 || resultCalls.Load() != 1 {
		t.Fatalf("repository calls receipt/result=%d/%d", receiptCalls.Load(), resultCalls.Load())
	}
	if got := resultMetrics.Snapshot(); got.Accepted != 1 || got.ReceivedToConfirmedCommit.Count != 0 {
		t.Fatalf("receipt contaminated result metrics/timeline: %+v", got)
	}
}

func TestResponseConsumerRoutesOnlyStrictStage6Snapshots(t *testing.T) {
	var resultCalls atomic.Int64
	repo := resultRepoFunc(func(context.Context, string, []byte, time.Time) (digitaltwin.ControlResultOutcome, error) {
		resultCalls.Add(1)
		return digitaltwin.ControlResultAccepted, nil
	})
	q := make(chan RawResult, 1)
	consumer, err := NewResultConsumer(1, time.Second, repo, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	var routed atomic.Int64
	if err := consumer.ConfigureSnapshots(true, func(ctx context.Context, _ RawResult) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("snapshot handler was not bounded")
		}
		routed.Add(1)
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	query := digitaltwin.ConfigurationQuery{
		ProtocolVersion: 1, MessageType: "configuration_query", RequestID: uuid.NewString(),
		GatewayID: "fixture_gw", EntityID: "urn:ngsi-ld:Sensor:fixture_gw:s1",
		IssuedAt: now, ExpiresAt: now.Add(5 * time.Second),
	}
	snapshot := snapshotForQuery(t, query)
	consumer.process(snapshot)
	if routed.Load() != 1 || resultCalls.Load() != 0 {
		t.Fatalf("snapshot routing/result path=%d/%d", routed.Load(), resultCalls.Load())
	}
	retained := snapshot
	retained.Retained = true
	consumer.process(retained)
	malformed := snapshot
	malformed.Payload = []byte(`{"message_type":"configuration_snapshot","message_type":"configuration_snapshot"}`)
	consumer.process(malformed)
	if routed.Load() != 1 {
		t.Fatalf("retained or malformed snapshot routed: %d", routed.Load())
	}

	stage5, err := NewResultConsumer(1, time.Second, repo, make(chan RawResult), nil)
	if err != nil {
		t.Fatal(err)
	}
	stage5.process(snapshot)
	if routed.Load() != 1 || resultCalls.Load() != 0 {
		t.Fatal("Stage 5-only consumer routed snapshot")
	}
}

func TestResponseConsumerStage5RejectsReceiptAndRetainedFlagSurvivesQueueBoundary(t *testing.T) {
	var resultCalls, receiptCalls atomic.Int64
	resultRepo := resultRepoFunc(func(context.Context, string, []byte, time.Time) (digitaltwin.ControlResultOutcome, error) {
		resultCalls.Add(1)
		return digitaltwin.ControlResultAccepted, nil
	})
	receiptRepo := receiptRepoFunc(func(context.Context, string, []byte, time.Time) (digitaltwin.ControlReceiptOutcome, error) {
		receiptCalls.Add(1)
		return digitaltwin.ControlReceiptAccepted, nil
	})
	c, err := NewResultConsumer(1, time.Second, resultRepo, make(chan RawResult), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ConfigureReceipts(false, receiptRepo, nil); err != nil {
		t.Fatal(err)
	}
	receipt := validRawReceipt(t)
	c.process(receipt)
	if receiptCalls.Load() != 0 || resultCalls.Load() != 0 {
		t.Fatal("Stage 5-only consumer called a repository for a receipt")
	}

	// Simulate a retained delivery that crossed RawResult admission. The
	// consumer boundary must reject it even if transport admission regresses.
	result := validRawResult(t)
	result.Retained = true
	c.process(result)
	if resultCalls.Load() != 0 {
		t.Fatal("retained flag was lost across the raw-result queue boundary")
	}
}

func TestResultConsumerWithoutMetrics(t *testing.T) {
	for _, outcome := range []digitaltwin.ControlResultOutcome{
		digitaltwin.ControlResultAccepted, digitaltwin.ControlResultDuplicate,
		digitaltwin.ControlResultLate, "unknown",
	} {
		t.Run(string(outcome), func(t *testing.T) {
			var calls int
			repo := resultRepoFunc(func(ctx context.Context, _ string, _ []byte, _ time.Time) (digitaltwin.ControlResultOutcome, error) {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Error("DB call without deadline")
				}
				return outcome, nil
			})
			c, err := NewResultConsumer(1, time.Second, repo, make(chan RawResult), nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Stop)
			// Keep panic recovery in the test goroutine so all outcomes are exercised.
			defer func() {
				if v := recover(); v != nil {
					t.Errorf("metrics-disabled result processing panicked: %v", v)
				}
			}()
			c.process(validRawResult(t))
			c.process(validRawResult(t))
			if calls != 2 {
				t.Fatalf("repository calls = %d, want 2 even without metrics", calls)
			}
		})
	}
}

func TestResultConsumerWithoutMetricsKeepsWorkerRunning(t *testing.T) {
	q := make(chan RawResult, 8)
	var calls atomic.Int64
	repo := resultRepoFunc(func(context.Context, string, []byte, time.Time) (digitaltwin.ControlResultOutcome, error) {
		switch calls.Add(1) {
		case 1:
			return digitaltwin.ControlResultAccepted, nil
		case 2:
			return digitaltwin.ControlResultDuplicate, nil
		case 3:
			return digitaltwin.ControlResultLate, nil
		case 4:
			return "", digitaltwin.ErrControlResultConflict
		case 5:
			return "", errors.New("injected DB failure")
		default:
			return digitaltwin.ControlResultAccepted, nil
		}
	})
	c, err := NewResultConsumer(1, time.Second, repo, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	bad := validRawResult(t)
	bad.Payload = []byte("malformed")
	q <- bad
	for i := 0; i < 6; i++ {
		q <- validRawResult(t)
	}
	waitResult(t, "metrics-disabled worker did not continue processing", func() bool { return calls.Load() == 6 })
	if !c.Ready() {
		t.Fatal("worker exited after metrics-disabled result processing")
	}
}

func TestResultConsumerShutdownCancelsBoundedDBAndRejectsInvalidOptions(t *testing.T) {
	q := make(chan RawResult, 4)
	entered := make(chan struct{}, 2)
	repo := resultRepoFunc(func(ctx context.Context, _ string, _ []byte, _ time.Time) (digitaltwin.ControlResultOutcome, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return "", ctx.Err()
	})
	for _, n := range []int{0, 17} {
		if _, err := NewResultConsumer(n, time.Second, repo, q, nil); err == nil {
			t.Fatal("unbounded workers accepted")
		}
	}
	c, err := NewResultConsumer(2, time.Minute, repo, q, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	q <- validRawResult(t)
	q <- validRawResult(t)
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("fixed workers not active")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Ready() {
		t.Fatal("stopped consumer ready")
	}
	if err := c.Start(); err == nil {
		t.Fatal("stopped worker restarted")
	}
}
