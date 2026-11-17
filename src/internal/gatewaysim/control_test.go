package gatewaysim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"

	"github.com/google/uuid"
)

type testSampler struct {
	interval      int64
	sets          int
	fail          error
	wrongReadback bool
	beforeSet     func()
}

func (s *testSampler) SetSampleInterval(_ context.Context, v int64) error {
	if s.beforeSet != nil {
		s.beforeSet()
	}
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

func readbackQueryForTest(gateway, entity string, now time.Time) digitaltwin.ConfigurationQuery {
	return digitaltwin.ConfigurationQuery{
		ProtocolVersion: 1,
		MessageType:     "configuration_query",
		RequestID:       uuid.NewString(),
		GatewayID:       gateway,
		EntityID:        entity,
		IssuedAt:        now.UTC().Truncate(time.Microsecond).Add(-time.Second),
		ExpiresAt:       now.UTC().Truncate(time.Microsecond).Add(5 * time.Second),
	}
}

func TestControllerReadbackReportsDurableAndActualWithoutMutation(t *testing.T) {
	c, sampler, out := testController(t)
	c.stage6Enabled = true
	c.now = time.Now
	if err := c.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	topic, _, command := controlTestCommand(t, 1, 20000)
	command.IssuedAt = time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
	command.ExpiresAt = command.IssuedAt.Add(10 * time.Second)
	if _, err := c.Handle(context.Background(), topic, mustCommandJSON(t, command), false); err != nil {
		t.Fatal(err)
	}
	before, err := out.ControlReceipts(context.Background())
	if err != nil || len(before) != 1 {
		t.Fatalf("read initial durable evidence: %d/%v", len(before), err)
	}
	sets := sampler.sets
	sampler.interval = 30000 // Deliberately diverge from durable confirmed configuration.
	query := readbackQueryForTest(command.GatewayID, command.EntityID, time.Now())
	got, err := c.Readback(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	after, err := out.ControlReceipts(context.Background())
	cfg, found, configErr := out.Configuration(context.Background())
	if err != nil || configErr != nil || len(after) != 1 || !found ||
		!bytes.Equal(before[0].Payload, after[0].Payload) || !bytes.Equal(before[0].Result, after[0].Result) ||
		!bytes.Equal(before[0].ReceiptBytes, after[0].ReceiptBytes) ||
		got.ConfigurationState != "configured" || got.WatermarkVersion != command.DesiredVersion ||
		got.DurableSampleIntervalUS == nil || *got.DurableSampleIntervalUS != 20000 ||
		got.ActualSampleIntervalUS == nil || *got.ActualSampleIntervalUS != 30000 ||
		cfg.SampleIntervalUS != 20000 || sampler.sets != sets {
		t.Fatalf("readback mutated or conflated state: snapshot=%+v cfg=%+v receipts=%d/%v config=%v sets=%d/%d",
			got, cfg, len(after), err, configErr, sampler.sets, sets)
	}
}

func TestControllerReadbackUnconfiguredAndUnavailableAreExplicit(t *testing.T) {
	c, sampler, out := testController(t)
	c.stage6Enabled = true
	c.now = time.Now
	if err := c.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	sampler.interval = 1000000
	q := readbackQueryForTest("gateway_001", "urn:ngsi-ld:Sensor:gateway_001:sensor_001", time.Now())
	unconfigured, err := c.Readback(context.Background(), q)
	if err != nil || unconfigured.ConfigurationState != "unconfigured" ||
		unconfigured.WatermarkVersion != 0 || unconfigured.DurableSampleIntervalUS != nil ||
		unconfigured.ActualSampleIntervalUS == nil || *unconfigured.ActualSampleIntervalUS != 1000000 {
		t.Fatalf("unconfigured readback=%+v err=%v", unconfigured, err)
	}
	if _, err := out.db.Exec("DROP TABLE sensor_configuration"); err != nil {
		t.Fatal(err)
	}
	q.RequestID = uuid.NewString()
	unavailable, err := c.Readback(context.Background(), q)
	if err != nil || unavailable.ConfigurationState != "unavailable" ||
		unavailable.WatermarkVersion != 0 || unavailable.DurableSampleIntervalUS != nil ||
		unavailable.ActualSampleIntervalUS == nil || *unavailable.ActualSampleIntervalUS != 1000000 {
		t.Fatalf("unavailable readback=%+v err=%v", unavailable, err)
	}
	sampler.interval = 0 // The sampler API has no error channel; invalid cadence is unavailable.
	q.RequestID = uuid.NewString()
	unavailable, err = c.Readback(context.Background(), q)
	if err != nil || unavailable.ConfigurationState != "unavailable" || unavailable.ActualSampleIntervalUS != nil {
		t.Fatalf("sampler-unavailable readback=%+v err=%v", unavailable, err)
	}
}

func TestControllerReadbackWaitIsBoundedByQueryContext(t *testing.T) {
	c, sampler, _ := testController(t)
	c.stage6Enabled = true
	c.now = time.Now
	if err := c.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	sampler.beforeSet = func() {
		once.Do(func() { close(entered); <-release })
	}
	topic, _, command := controlTestCommand(t, 1, 20000)
	command.IssuedAt = time.Now().UTC().Truncate(time.Microsecond).Add(-time.Second)
	command.ExpiresAt = command.IssuedAt.Add(5 * time.Second)
	payload := mustCommandJSON(t, command)
	applyDone := make(chan error, 1)
	go func() {
		_, err := c.Handle(context.Background(), topic, payload, false)
		applyDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("apply did not enter sampler")
	}
	q := readbackQueryForTest(command.GatewayID, command.EntityID, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.Readback(ctx, q); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("readback lock wait was not bounded: %v", err)
	}
	close(release)
	if err := <-applyDone; err != nil {
		t.Fatal(err)
	}
	q.RequestID = uuid.NewString()
	if _, err := c.Readback(context.Background(), q); err != nil {
		t.Fatalf("readback after serialized apply: %v", err)
	}
}

func TestControllerStage6ReceiptPersistsBeforeApplyAndDuplicateReusesEvidence(t *testing.T) {
	out := openTestOutbox(t, testOutboxConfig(filepath.Join(t.TempDir(), "outbox.db")))
	sampler := &testSampler{interval: 10000}
	c, err := NewController(out, sampler, true)
	if err != nil {
		t.Fatal(err)
	}
	receiptAt := outboxBaseTime.Add(time.Second).UTC().Truncate(time.Microsecond)
	c.now = func() time.Time { return receiptAt }
	topic, payload, command := controlTestCommand(t, 1, 20000)
	var beforeApply ControlReceipt
	sampler.beforeSet = func() {
		receipts, readErr := out.ControlReceipts(context.Background())
		if readErr != nil || len(receipts) != 1 || len(receipts[0].ReceiptBytes) == 0 {
			t.Fatalf("receipt was not durable before SetSampleInterval: %d/%v", len(receipts), readErr)
		}
		beforeApply = receipts[0]
	}
	result, err := c.Handle(context.Background(), topic, payload, false)
	if err != nil || len(result) == 0 || sampler.sets != 1 {
		t.Fatalf("stage6 apply sets=%d err=%v", sampler.sets, err)
	}
	parsed, err := digitaltwin.ParseMQTTReceipt("gateways/"+command.GatewayID+"/responses/"+command.CommandID, beforeApply.ReceiptBytes, false)
	if err != nil || parsed.CommandHash != fmt.Sprintf("%x", sha256.Sum256(payload)) ||
		!parsed.ReceivedAt.Equal(receiptAt) || !beforeApply.ReceivedAt.Equal(receiptAt) {
		t.Fatalf("receipt did not preserve canonical original evidence: %+v/%+v err=%v", parsed, beforeApply, err)
	}
	c.now = func() time.Time { return command.ExpiresAt.Add(time.Hour) }
	duplicate, err := c.Handle(context.Background(), topic, payload, false)
	if err != nil || !bytes.Equal(result, duplicate) || sampler.sets != 1 {
		t.Fatalf("completed duplicate changed result/applied again: %v sets=%d", err, sampler.sets)
	}
	conflict := bytes.Replace(payload, []byte("20000"), []byte("30000"), 1)
	if _, err := c.Handle(context.Background(), topic, conflict, false); !errors.Is(err, ErrControlConflict) || sampler.sets != 1 {
		t.Fatalf("changed original command bytes accepted: err=%v sets=%d", err, sampler.sets)
	}
	after, err := out.ControlReceipts(context.Background())
	if err != nil || len(after) != 1 || !bytes.Equal(after[0].ReceiptBytes, beforeApply.ReceiptBytes) ||
		after[0].ReceiptHash != beforeApply.ReceiptHash || !after[0].ReceivedAt.Equal(beforeApply.ReceivedAt) {
		t.Fatalf("duplicate changed durable receipt: %+v/%v", after, err)
	}
}

func TestControllerStage6ExpiredCommandKeepsOriginalDeadline(t *testing.T) {
	c, sampler, out := testController(t)
	c.stage6Enabled = true
	receivedAt := outboxBaseTime.Add(time.Second).UTC().Truncate(time.Microsecond)
	c.now = func() time.Time { return receivedAt }
	topic, _, command := controlTestCommand(t, 1, 20000)
	command.IssuedAt = receivedAt.Add(-2 * time.Second)
	command.ExpiresAt = receivedAt.Add(-time.Second)
	payload := mustCommandJSON(t, command)
	result, err := c.Handle(context.Background(), topic, payload, false)
	if err != nil || len(result) == 0 || sampler.sets != 0 {
		t.Fatalf("expired Stage 6 command result=%d applies=%d err=%v", len(result), sampler.sets, err)
	}
	receipts, err := out.ControlReceipts(context.Background())
	var storedResult digitaltwin.CommandResult
	if len(receipts) == 1 {
		storedResult, err = digitaltwin.ParseMQTTResult("gateways/"+command.GatewayID+"/responses/"+command.CommandID, receipts[0].Result, false)
	}
	if err != nil || len(receipts) != 1 || !receipts[0].Command.ExpiresAt.Equal(command.ExpiresAt) ||
		len(receipts[0].ReceiptBytes) == 0 || storedResult.Status != digitaltwin.CommandFailed || storedResult.ErrorCode != "COMMAND_EXPIRED" {
		t.Fatalf("receipt extended/changed original deadline: %+v err=%v", receipts, err)
	}
}

func TestControllerStage6ReservationCommitUncertaintyDoesNotApply(t *testing.T) {
	c, sampler, out := testController(t)
	c.stage6Enabled = true
	topic, payload, _ := controlTestCommand(t, 1, 20000)
	commit := out.commit
	out.commit = func(*sql.Tx) error { return errors.New("injected reservation commit uncertainty") }
	result, err := c.Handle(context.Background(), topic, payload, false)
	out.commit = commit
	if !errors.Is(err, ErrOutboxCommitUnknown) || len(result) != 0 || sampler.sets != 0 {
		t.Fatalf("uncertain reservation result=%d applies=%d err=%v", len(result), sampler.sets, err)
	}
	receipts, err := out.ControlReceipts(context.Background())
	if err != nil || len(receipts) != 0 {
		t.Fatalf("uncertain intent synthesized evidence: %+v err=%v", receipts, err)
	}
}

func TestControllerStage6PreparedReopenPreservesReceiptAndRecoversResult(t *testing.T) {
	c, _, out := testController(t)
	c.stage6Enabled = true
	receiptAt := outboxBaseTime.Add(time.Second).UTC().Truncate(time.Microsecond)
	c.now = func() time.Time { return receiptAt }
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
	if result, err := c.Handle(context.Background(), topic, payload, false); err == nil || len(result) != 0 {
		t.Fatal("ambiguous result finalization reported success")
	}
	out.commit = commit
	prepared, err := out.ControlReceipts(context.Background())
	if err != nil || len(prepared) != 1 || len(prepared[0].ReceiptBytes) == 0 ||
		!prepared[0].ReceivedAt.Equal(receiptAt) || len(prepared[0].Result) != 0 {
		t.Fatalf("prepared durable evidence=%+v err=%v", prepared, err)
	}
	var path string
	if err := out.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	out = openTestOutbox(t, testOutboxConfig(path))
	recoveredSampler := &testSampler{interval: 10000}
	recovered, err := NewController(out, recoveredSampler, true)
	if err != nil {
		t.Fatal(err)
	}
	recovered.now = func() time.Time { return receiptAt }
	if err := recovered.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := out.ControlReceipts(context.Background())
	if err != nil || len(after) != 1 || !bytes.Equal(after[0].ReceiptBytes, prepared[0].ReceiptBytes) ||
		after[0].ReceiptHash != prepared[0].ReceiptHash || !after[0].ReceivedAt.Equal(prepared[0].ReceivedAt) ||
		len(after[0].Result) == 0 {
		t.Fatalf("recovery did not preserve receipt and complete result: %+v err=%v", after, err)
	}
	before := recoveredSampler.sets
	duplicate, err := recovered.Handle(context.Background(), topic, payload, false)
	if err != nil || len(duplicate) == 0 || recoveredSampler.sets != before {
		t.Fatalf("recovered duplicate re-applied: sets=%d err=%v", recoveredSampler.sets, err)
	}
}

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
