package gatewaysim

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
)

type testSampler struct {
	interval      int64
	sets          int
	fail          error
	wrongReadback bool
}

func (s *testSampler) SetSampleInterval(_ context.Context, v int64) error {
	s.sets++
	if s.fail != nil {
		return s.fail
	}
	if !s.wrongReadback {
		s.interval = v
	}
	return nil
}
func (s *testSampler) ActiveSampleInterval() int64 { return s.interval }

func testController(t *testing.T) (*Controller, *testSampler, *Outbox) {
	t.Helper()
	out := openTestOutbox(t, testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db")))
	sampler := &testSampler{interval: 10000}
	c, err := NewController(out, sampler)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return outboxBaseTime.Add(time.Second) }
	return c, sampler, out
}

func TestControllerApplyReadbackDuplicateAndInvalid(t *testing.T) {
	c, sampler, out := testController(t)
	ctx := context.Background()
	topic, payload, command := controlTestCommand(t, 1, 10000000)
	result, err := c.Handle(ctx, topic, payload, false)
	if err != nil || len(result) == 0 || sampler.sets != 1 || sampler.interval != 10000000 {
		t.Fatalf("apply sets=%d interval=%d err=%v", sampler.sets, sampler.interval, err)
	}
	parsed, err := digitaltwin.ParseMQTTResult("gateways/gateway_001/responses/"+command.CommandID, result, false)
	if err != nil || parsed.Status != digitaltwin.CommandSucceeded {
		t.Fatalf("result %v/%v", parsed.Status, err)
	}
	c.now = func() time.Time { return command.ExpiresAt.Add(time.Hour) }
	duplicate, err := c.Handle(ctx, topic, payload, false)
	if err != nil || !bytes.Equal(result, duplicate) || sampler.sets != 1 {
		t.Fatalf("duplicate reapplied: %d/%v", sampler.sets, err)
	}
	if _, err := c.Handle(ctx, topic, bytes.Replace(payload, []byte("10000000"), []byte("20000000"), 1), false); !errors.Is(err, ErrControlConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if _, err := c.Handle(ctx, topic, payload, true); err == nil {
		t.Fatal("retained command accepted")
	}
	_, found, err := out.Configuration(ctx)
	if err != nil || !found {
		t.Fatalf("config persistence %v/%v", found, err)
	}
	// Wrong Sensor in the same Gateway is ignored, not given UNKNOWN_SENSOR.
	topic, payload, _ = controlTestCommand(t, 2, 20000)
	payload = bytes.Replace(payload, []byte("sensor_001"), []byte("sensor_002"), 1)
	if r, err := c.Handle(ctx, topic, payload, false); !errors.Is(err, ErrControlSensor) || len(r) != 0 {
		t.Fatalf("wrong sensor produced result: %v", err)
	}
}

func TestControllerStaleExpiredAndConcurrentDuplicate(t *testing.T) {
	c, sampler, _ := testController(t)
	ctx := context.Background()
	topic, payload, _ := controlTestCommand(t, 2, 16667)
	var wg sync.WaitGroup
	results := make(chan []byte, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r, err := c.Handle(ctx, topic, payload, false); results <- r; errs <- err }()
	}
	wg.Wait()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(<-results, <-results) || sampler.sets != 1 {
		t.Fatal("concurrent duplicate applied twice")
	}
	for _, tc := range []struct {
		version digitaltwin.Version
		expired bool
		code    string
	}{{1, false, "STALE_DESIRED_VERSION"}, {2, false, "STALE_DESIRED_VERSION"}, {3, true, "COMMAND_EXPIRED"}} {
		topic, payload, command := controlTestCommand(t, tc.version, 20000)
		if tc.expired {
			c.now = func() time.Time { return command.ExpiresAt.Add(time.Second) }
		}
		r, err := c.Handle(ctx, topic, payload, false)
		if err != nil {
			t.Fatal(err)
		}
		var result digitaltwin.CommandResult
		if err := json.Unmarshal(r, &result); err != nil || result.Status != digitaltwin.CommandFailed || result.ErrorCode != tc.code {
			t.Fatalf("failed code=%s err=%v", result.ErrorCode, err)
		}
	}
	if sampler.sets != 1 || sampler.interval != 16667 {
		t.Fatal("stale/expired changed active config")
	}
}

func TestControllerPersistFailurePreparedRecoveryAndReadbackFailure(t *testing.T) {
	c, sampler, out := testController(t)
	ctx := context.Background()
	topic, payload, _ := controlTestCommand(t, 1, 666667)
	commit := out.commit
	calls := 0
	out.commit = func(tx *sql.Tx) error {
		calls++
		if calls == 2 {
			return errors.New("injected finalize failure")
		}
		return tx.Commit()
	}
	if r, err := c.Handle(ctx, topic, payload, false); err == nil || len(r) != 0 {
		t.Fatal("persist error returned success")
	}
	out.commit = commit
	if sampler.interval != 666667 {
		t.Fatal("test did not reach activation boundary")
	}
	if _, found, err := out.Configuration(ctx); err != nil || found {
		t.Fatal("uncommitted config became confirmed")
	}
	if r, err := c.Handle(ctx, topic, payload, false); !errors.Is(err, ErrControlBusy) || len(r) != 0 {
		t.Fatalf("prepared duplicate retried apply: %v", err)
	}
	// A controlled real file reopen at the apply/finalize boundary. The new
	// in-memory sampler starts from bootstrap, not the old process's timer.
	var path string
	if err := out.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	out = openTestOutbox(t, testOutboxConfig(path))
	sampler = &testSampler{interval: 10000}
	reopened, err := NewController(out, sampler)
	if err != nil {
		t.Fatal(err)
	}
	reopened.now = c.now
	if err := reopened.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if cfg, found, err := out.Configuration(ctx); err != nil || !found || cfg.SampleIntervalUS != 666667 {
		t.Fatalf("recovery config=%+v found=%v err=%v", cfg, found, err)
	}
	before := sampler.sets
	if _, err := reopened.Handle(ctx, topic, payload, false); err != nil || sampler.sets != before {
		t.Fatal("recovered duplicate reapplied")
	}
	topic, payload, _ = controlTestCommand(t, 2, 20000)
	sampler.wrongReadback = true
	if r, err := reopened.Handle(ctx, topic, payload, false); err == nil || len(r) != 0 {
		t.Fatal("wrong readback created success")
	}
}
