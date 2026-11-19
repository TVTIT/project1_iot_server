package gatewaysim

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/telemetry"
)

func runtimeSampling(t *testing.T) (*Scheduler, *fakeClock, *Outbox) {
	t.Helper()
	clock := newFakeClock()
	cfg := baseSchedulerConfig()
	out := openTestOutbox(t, schedOutboxConfig(filepath.Join(t.TempDir(), "outbox.db"), cfg))
	ack, err := NewACKProcessor(out, 16)
	if err != nil {
		t.Fatal(err)
	}
	pub := newFakePublisher(clock)
	pub.setReady(true)
	s, err := NewScheduler(SchedulerOptions{Outbox: out, Publisher: pub, Ack: ack, Config: cfg, RuntimeSampling: true, clock: clock, newUUID: seqUUIDGen(100)})
	if err != nil {
		t.Fatal(err)
	}
	// Drive the exact loop methods synchronously; no sleeps or network timing.
	s.ctx = context.Background()
	s.nextGenAt = clock.Now()
	return s, clock, out
}

func TestRuntimeSamplingWakeFlushAndStablePending(t *testing.T) {
	s, clock, out := runtimeSampling(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("sampling shutdown budget")
		}
	})
	waitFor(t, "sampling loop startup", func() bool { s.startMu.Lock(); defer s.startMu.Unlock(); return s.started })
	// A round trip through the update channel synchronizes loop startup.
	if err := s.SetSampleInterval(ctx, 10000); err != nil {
		t.Fatal(err)
	}
	clock.Advance(250 * time.Millisecond)
	// Synchronize the due samples before submitting the boundary update.
	// The loop must collect samples due at the boundary before flushing them.
	if err := s.SetSampleInterval(ctx, 10000000); err != nil {
		t.Fatal(err)
	}
	items, err := out.Pending(ctx, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("boundary batches=%d err=%v", len(items), err)
	}
	var old telemetry.BatchDTO
	if err := json.Unmarshal(items[0].Payload, &old); err != nil {
		t.Fatal(err)
	}
	if old.SampleCount != 25 || old.SampleIntervalUS != 10000 {
		t.Fatalf("old boundary=%+v", old)
	}
	before := append([]byte(nil), items[0].Payload...)
	// Wake while the old sampling timer is ten seconds away, with no ACK.
	if err := s.SetSampleInterval(ctx, 666667); err != nil {
		t.Fatal(err)
	}
	if s.ActiveSampleInterval() != 666667 {
		t.Fatal("readback did not change independently of sample timer")
	}
	got, found, err := out.Lookup(ctx, items[0].MessageID)
	if err != nil || !found || !bytes.Equal(got.Payload, before) {
		t.Fatalf("pending retagged: %v/%v", found, err)
	}
}

func TestRuntimeSamplingCapacityRejectsBoundaryWithoutFalseReadback(t *testing.T) {
	s, clock, out := runtimeSampling(t)
	out.maxRows = 1
	s.cfg.maxInflight = 1
	if err := s.reconcile(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err := s.reconcile(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(100 * time.Millisecond)
	if err := s.reconcile(); err != nil {
		t.Fatal(err)
	}
	if err := s.SetSampleInterval(context.Background(), 10000000); !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("boundary full=%v", err)
	}
	if s.ActiveSampleInterval() != 10000 || s.sampling.count != 10 {
		t.Fatal("full outbox falsely applied config or silently dropped buffered samples")
	}
}

func TestRuntimeSamplingReopenConfigurationWinsOverEnvironment(t *testing.T) {
	s, _, out := runtimeSampling(t)
	topic, payload, c := controlTestCommand(t, 1, 10000000)
	if _, err := out.ReserveControl(context.Background(), topic, payload, outboxBaseTime); err != nil {
		t.Fatal(err)
	}
	if err := out.CompleteControl(context.Background(), c.CommandID, controlTestResult(t, c, digitaltwin.CommandSucceeded), outboxBaseTime.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// Even a telemetry-only reopen must not reset committed runtime configuration.
	reopened, err := NewScheduler(SchedulerOptions{Outbox: out, Publisher: s.pub, Ack: s.ack, Config: baseSchedulerConfig()})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.ActiveSampleInterval() != 10000000 || reopened.sampling == nil {
		t.Fatal("environment reset saved configuration")
	}
}

func TestRuntimeSamplingCommitAmbiguityNeverRegenerates(t *testing.T) {
	s, clock, out := runtimeSampling(t)
	if err := s.reconcile(); err != nil {
		t.Fatal(err)
	}
	out.commit = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return errors.New("injected ambiguous commit response")
	}
	clock.Advance(time.Second)
	if err := s.reconcile(); !errors.Is(err, ErrOutboxCommitUnknown) {
		t.Fatalf("commit ambiguity: %v", err)
	}
	if err := s.flushSamples(context.Background()); !errors.Is(err, ErrOutboxCommitUnknown) {
		t.Fatalf("ambiguous flush retried: %v", err)
	}
	if got := pendingRows(t, out); got != 1 {
		t.Fatalf("ambiguous batch regenerated: rows=%d", got)
	}
}

func TestRuntimeSamplingNoBackdatedOrFutureSamples(t *testing.T) {
	s, clock, out := runtimeSampling(t)
	start := clock.Now()
	if err := s.reconcile(); err != nil {
		t.Fatal(err)
	}
	if rows := pendingRows(t, out); rows != 0 {
		t.Fatalf("generated %d batches before any sample timer", rows)
	}
	clock.Advance(time.Second)
	if err := s.reconcile(); err != nil {
		t.Fatal(err)
	}
	items, err := out.Pending(context.Background(), 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("one second: %d/%v", len(items), err)
	}
	var b telemetry.BatchDTO
	if err := json.Unmarshal(items[0].Payload, &b); err != nil {
		t.Fatal(err)
	}
	first, err := time.Parse(time.RFC3339Nano, b.MeasuredAt)
	if err != nil || !first.Equal(start.Add(10*time.Millisecond)) || b.SampleCount != 100 || b.SampleIntervalUS != 10000 {
		t.Fatalf("batch time=%v count=%d interval=%d err=%v", first, b.SampleCount, b.SampleIntervalUS, err)
	}
}

func TestRuntimeSamplingDecimalLowHzBoundary(t *testing.T) {
	s, clock, out := runtimeSampling(t)
	if err := s.reconcile(); err != nil {
		t.Fatal(err)
	}
	for i, tc := range []struct {
		hz       string
		interval int64
		count    int32
	}{{"100", 10000, 100}, {"60", 16667, 59}, {"1.5", 666667, 1}, {"0.1", 10000000, 1}} {
		r, err := digitaltwin.ParseDesiredRequest([]byte(`{"frequency_hz":`+tc.hz+`,"expected_desired_version":"0"}`), "sampling")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SetSampleInterval(context.Background(), r.Frequency.SampleIntervalUS()); err != nil {
			t.Fatalf("set %s: %v", tc.hz, err)
		}
		if got := s.ActiveSampleInterval(); got != tc.interval {
			t.Fatalf("readback=%d", got)
		}
		if tc.interval > 1000000 {
			clock.Advance(9 * time.Second)
			if err := s.reconcile(); err != nil {
				t.Fatal(err)
			}
			if got := s.uniqueEnqueued.Load(); got != int64(i) {
				t.Fatalf("low Hz empty batch: %d", got)
			}
			clock.Advance(time.Second)
		} else {
			clock.Advance(time.Second)
		}
		if err := s.reconcile(); err != nil {
			t.Fatal(err)
		}
		items, err := out.Pending(context.Background(), 10)
		if err != nil || len(items) != i+1 {
			t.Fatalf("batches=%d err=%v", len(items), err)
		}
		var b telemetry.BatchDTO
		if err := json.Unmarshal(items[i].Payload, &b); err != nil {
			t.Fatal(err)
		}
		if b.SampleIntervalUS != tc.interval || b.SampleCount != tc.count {
			t.Fatalf("Hz %s interval=%d count=%d", tc.hz, b.SampleIntervalUS, b.SampleCount)
		}
		if b.FirstSequence != int64(0) && b.BootID != s.BootID() {
			t.Fatal("boot identity changed")
		}
	}
}
