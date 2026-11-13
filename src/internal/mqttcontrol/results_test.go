package mqttcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
)

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
