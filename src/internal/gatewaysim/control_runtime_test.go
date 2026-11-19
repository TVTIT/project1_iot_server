package gatewaysim

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"iot-platform/internal/digitaltwin"
)

func mustCommandJSON(t *testing.T, c digitaltwin.Command) []byte {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestControlRuntimeReceiptQueueDropDoesNotBlockApplyOrResult(t *testing.T) {
	c, _, out := testController(t)
	c.now = time.Now
	c.stage6Enabled = true
	inbox := NewCommandInbox("gateway_001", &recordAdmitter{})
	pub := newFakePublisher(realClock{})
	pub.setReady(true)
	entered, release := make(chan struct{}), make(chan struct{})
	var first sync.Once
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	pub.setPublishFn(func(string, []byte) error {
		first.Do(func() {
			close(entered)
			<-release
		})
		return nil
	})
	r, err := NewControlRuntime(c, inbox, pub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	_, _, dummy := controlTestCommand(t, 99, 20000)
	dummy.IssuedAt = time.Now().UTC().Truncate(time.Microsecond)
	dummy.ExpiresAt = dummy.IssuedAt.Add(20 * time.Second)
	dummyRequest := resultRequest{command: dummy, payload: []byte("bounded filler"), receipt: true}
	r.receipts <- dummyRequest
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("response worker did not enter bounded publisher")
	}
	pub.setReady(false)
	for range controlQueueCapacity {
		r.receipts <- dummyRequest
	}

	topic, _, command := controlTestCommand(t, 1, 10000000)
	command.IssuedAt = time.Now().UTC().Truncate(time.Microsecond)
	command.ExpiresAt = command.IssuedAt.Add(20 * time.Second)
	payload := mustCommandJSON(t, command)
	inbox.Admit(topic, payload, false)
	waitFor(t, "durable result despite full receipt queue", func() bool {
		receipts, readErr := out.ControlReceipts(context.Background())
		return readErr == nil && len(receipts) == 1 && len(receipts[0].Result) > 0
	})
	if r.Stats().QueueFull < 1 {
		t.Fatal("full receipt queue was not observed")
	}
	unblock()
	pub.setReady(true)
	waitFor(t, "result publication after receipt queue drop", func() bool {
		records := pub.snapshot()
		for _, record := range records {
			if record.topic == "gateways/"+command.GatewayID+"/responses/"+command.CommandID {
				var got digitaltwin.CommandResult
				if json.Unmarshal(record.payload, &got) == nil && got.MessageType == "command_result" {
					return true
				}
			}
		}
		return false
	})
	waitFor(t, "receipt recovered from durable scan", func() bool {
		records := pub.snapshot()
		for _, record := range records {
			if record.topic == "gateways/"+command.GatewayID+"/responses/"+command.CommandID {
				var got map[string]any
				if json.Unmarshal(record.payload, &got) == nil && got["message_type"] == "command_receipt" {
					return true
				}
			}
		}
		return false
	})
}

func TestControlRuntimeReceiptResendStopsAtOriginalExpiry(t *testing.T) {
	c, _, _ := testController(t)
	pub := newFakePublisher(realClock{})
	pub.setReady(true)
	r, err := NewControlRuntime(c, NewCommandInbox("gateway_001", &recordAdmitter{}), pub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.ctx = context.Background()
	_, _, command := controlTestCommand(t, 1, 20000)
	command.ExpiresAt = time.Now().UTC().Add(-time.Second)
	r.publishResponse(resultRequest{command: command, payload: []byte(`{"message_type":"command_receipt"}`), receipt: true}, make(map[string]resultRetry))
	if pub.count() != 0 {
		t.Fatal("expired receipt was published or renewed")
	}
}

func TestControlRuntimeStage5ReopenDoesNotPublishStoredStage6Receipt(t *testing.T) {
	c, sampler, out := testController(t)
	c.stage6Enabled = true
	c.now = time.Now
	topic, _, command := controlTestCommand(t, 1, 20000)
	command.IssuedAt = time.Now().UTC().Truncate(time.Microsecond)
	command.ExpiresAt = command.IssuedAt.Add(10 * time.Second)
	payload := mustCommandJSON(t, command)
	if _, err := c.Handle(context.Background(), topic, payload, false); err != nil {
		t.Fatal(err)
	}
	var dbPath string
	if err := out.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&dbPath); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTestOutbox(t, testOutboxConfig(dbPath))
	stage5Controller, err := NewController(reopened, &testSampler{interval: sampler.interval}, false)
	if err != nil {
		t.Fatal(err)
	}
	pub := newFakePublisher(realClock{})
	pub.setReady(true)
	r, err := NewControlRuntime(stage5Controller, NewCommandInbox("gateway_001", &recordAdmitter{}), pub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	waitFor(t, "periodic result resend from reopened Stage 5 runtime", func() bool {
		for _, record := range pub.snapshot() {
			if record.topic != "gateways/"+command.GatewayID+"/responses/"+command.CommandID {
				continue
			}
			var got digitaltwin.CommandResult
			if json.Unmarshal(record.payload, &got) == nil && got.MessageType == "command_result" {
				return true
			}
		}
		return false
	})
	for _, record := range pub.snapshot() {
		var got map[string]any
		if json.Unmarshal(record.payload, &got) == nil && got["message_type"] == "command_receipt" {
			t.Fatalf("Stage 5 runtime leaked stored Stage 6 receipt on %s", record.topic)
		}
	}
}

func TestControlRuntimePeriodicScanRunsWithContinuouslyAvailableResponseQueue(t *testing.T) {
	c, _, _ := testController(t)
	c.stage6Enabled = true
	c.now = time.Now
	topic, _, command := controlTestCommand(t, 1, 20000)
	command.IssuedAt = time.Now().UTC().Truncate(time.Microsecond)
	command.ExpiresAt = command.IssuedAt.Add(10 * time.Second)
	payload := mustCommandJSON(t, command)
	if _, err := c.Handle(context.Background(), topic, payload, false); err != nil {
		t.Fatal(err)
	}

	pub := newFakePublisher(realClock{})
	pub.setReady(true)
	r, err := NewControlRuntime(c, NewCommandInbox("gateway_001", &recordAdmitter{}), pub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, _, fillerCommand := controlTestCommand(t, 99, 20000)
	fillerCommand.IssuedAt = time.Now().UTC().Truncate(time.Microsecond)
	fillerCommand.ExpiresAt = fillerCommand.IssuedAt.Add(10 * time.Second)
	filler := resultRequest{command: fillerCommand, payload: []byte(`{"message_type":"command_result"}`)}
	for range controlQueueCapacity {
		r.results <- filler
	}
	pub.setPublishFn(func(string, []byte) error {
		// Refill the slot just consumed by resultLoop before it can reach select.
		select {
		case r.results <- filler:
		default:
		}
		return nil
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	waitFor(t, "periodic durable receipt scan despite continuously available result queue", func() bool {
		for _, record := range pub.snapshot() {
			if record.topic != "gateways/"+command.GatewayID+"/responses/"+command.CommandID {
				continue
			}
			var got map[string]any
			if json.Unmarshal(record.payload, &got) == nil && got["message_type"] == "command_receipt" {
				return true
			}
		}
		return false
	})
}

func TestControlInboxBoundsAndACKSeparation(t *testing.T) {
	ack := &recordAdmitter{}
	inbox := NewCommandInbox("gateway_001", ack)
	topic, payload, _ := controlTestCommand(t, 1, 10000)
	inbox.Admit(topic, payload, true)
	inbox.Admit(topic, make([]byte, digitaltwin.MaxControlPayloadBytes+1), false)
	for n := 0; n < controlQueueCapacity+1; n++ {
		inbox.Admit(topic, payload, false)
	}
	if len(inbox.queue) != controlQueueCapacity || inbox.full.Load() != 1 || inbox.rejected.Load() != 2 {
		t.Fatal("unbounded command callback")
	}
	payload[0] = 'x'
	if (<-inbox.queue).payload[0] == 'x' {
		t.Fatal("callback did not copy")
	}
	inbox.Admit("gateways/gateway_001/acks/test", []byte("ack"), false)
	if len(ack.calls()) != 1 {
		t.Fatal("ACK handler was bypassed")
	}
}

func TestControlRuntimePublishesDurableResultAndResendsAfterPUBACK(t *testing.T) {
	c, sampler, out := testController(t)
	c.now = time.Now
	inbox := NewCommandInbox("gateway_001", &recordAdmitter{})
	pub := newFakePublisher(realClock{})
	pub.setReady(true)
	r, err := NewControlRuntime(c, inbox, pub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	topic, _, command := controlTestCommand(t, 1, 10000000)
	// Reuse the helper envelope, with a real current, bounded command window.
	command.IssuedAt = time.Now().UTC().Truncate(time.Microsecond)
	command.ExpiresAt = command.IssuedAt.Add(10 * time.Second)
	payload := mustCommandJSON(t, command)
	inbox.Admit(topic, payload, false)
	waitFor(t, "result publication", func() bool { return pub.count() >= 1 })
	first := pub.snapshot()[0]
	if first.topic != "gateways/gateway_001/responses/"+command.CommandID {
		t.Fatal("wrong result namespace")
	}
	waitFor(t, "PUBACK must not stop result resend", func() bool { return pub.count() >= 2 })
	if !bytes.Equal(first.payload, pub.snapshot()[1].payload) {
		t.Fatal("resend changed canonical bytes")
	}
	receipts, err := out.ControlReceipts(context.Background())
	if err != nil || len(receipts) != 1 || !bytes.Equal(receipts[0].Result, first.payload) {
		t.Fatal("published bytes were not durable")
	}
	if sampler.sets != 1 {
		t.Fatal("resend reapplied command")
	}
	inbox.Admit(topic, payload, false)
	waitFor(t, "duplicate cached result", func() bool { return pub.count() >= 3 })
}

func TestControlRuntimeConfigurationQueryPublishesReadOnlySnapshot(t *testing.T) {
	c, sampler, out := testController(t)
	c.stage6Enabled = true
	c.now = time.Now
	inbox := NewCommandInbox("gateway_001", &recordAdmitter{})
	pub := newFakePublisher(realClock{})
	pub.setReady(true)
	r, err := NewControlRuntime(c, inbox, pub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	q := readbackQueryForTest("gateway_001", "urn:ngsi-ld:Sensor:gateway_001:sensor_001", time.Now())
	payload, err := digitaltwin.MarshalMQTTConfigurationQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	inbox.Admit("gateways/gateway_001/commands/"+q.RequestID, payload, false)
	waitFor(t, "transient configuration snapshot", func() bool { return pub.count() == 1 })
	record := pub.snapshot()[0]
	if record.topic != "gateways/gateway_001/responses/"+q.RequestID {
		t.Fatalf("snapshot topic=%s", record.topic)
	}
	got, err := digitaltwin.ParseMQTTConfigurationSnapshot(record.topic, record.payload, false)
	if err != nil || got.RequestID != q.RequestID || got.ConfigurationState != "unconfigured" ||
		got.WatermarkVersion != 0 || got.DurableSampleIntervalUS != nil ||
		got.ActualSampleIntervalUS == nil || *got.ActualSampleIntervalUS != sampler.interval {
		t.Fatalf("snapshot=%+v err=%v", got, err)
	}
	receipts, err := out.ControlReceipts(context.Background())
	stats := r.Stats()
	if err != nil || len(receipts) != 0 || sampler.sets != 0 ||
		stats.QueryAccepted != 1 || stats.SnapshotPublished != 1 ||
		stats.PublishOK != 0 || stats.PublishError != 0 {
		t.Fatalf("query entered command path: receipts=%d err=%v sets=%d stats=%+v", len(receipts), err, sampler.sets, stats)
	}
}

func TestControlRuntimeRejectsFeatureOffUnreadyAndExpiredQueries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stage6  bool
		ready   bool
		expired bool
		future  bool
	}{
		{name: "feature off", stage6: false, ready: true},
		{name: "subscription not ready", stage6: true, ready: false},
		{name: "expired", stage6: true, ready: true, expired: true},
		{name: "not yet issued", stage6: true, ready: true, future: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, _ := testController(t)
			c.stage6Enabled = tc.stage6
			c.now = time.Now
			pub := newFakePublisher(realClock{})
			pub.setReady(tc.ready)
			r, err := NewControlRuntime(c, NewCommandInbox("gateway_001", &recordAdmitter{}), pub, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			r.ctx = context.Background()
			q := readbackQueryForTest("gateway_001", "urn:ngsi-ld:Sensor:gateway_001:sensor_001", time.Now())
			if tc.expired {
				q.IssuedAt = time.Now().Add(-time.Second).UTC().Truncate(time.Microsecond)
				q.ExpiresAt = time.Now().Add(-time.Millisecond).UTC().Truncate(time.Microsecond)
			}
			if tc.future {
				q.IssuedAt = time.Now().Add(time.Second).UTC().Truncate(time.Microsecond)
			}
			payload, err := digitaltwin.MarshalMQTTConfigurationQuery(q)
			if err != nil {
				t.Fatal(err)
			}
			r.handleConfigurationQuery(rawCommand{
				topic:   "gateways/gateway_001/commands/" + q.RequestID,
				payload: payload,
			})
			if pub.count() != 0 || r.Stats().QueryAccepted != 0 || r.Stats().QueryRejected != 1 ||
				len(r.snapshots) != 0 {
				t.Fatalf("unsafe query was accepted/published: stats=%+v queue=%d", r.Stats(), len(r.snapshots))
			}
		})
	}
}

func TestControlRuntimeDoesNotStartQueryWorkerAfterRecoveryFailure(t *testing.T) {
	c, _, out := testController(t)
	c.stage6Enabled = true
	if _, err := out.db.Exec("DROP TABLE sensor_configuration"); err != nil {
		t.Fatal(err)
	}
	pub := newFakePublisher(realClock{})
	pub.setReady(true)
	r, err := NewControlRuntime(c, NewCommandInbox("gateway_001", &recordAdmitter{}), pub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("runtime started despite failed recovery")
	}
	q := readbackQueryForTest("gateway_001", "urn:ngsi-ld:Sensor:gateway_001:sensor_001", time.Now())
	payload, err := digitaltwin.MarshalMQTTConfigurationQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	r.inbox.Admit("gateways/gateway_001/commands/"+q.RequestID, payload, false)
	if pub.count() != 0 || r.Stats().SnapshotPublished != 0 {
		t.Fatal("query produced a snapshot before successful recovery")
	}
}

func TestControlRuntimeBoundsSnapshotQueueAndDropsExpiredResponse(t *testing.T) {
	c, _, _ := testController(t)
	c.stage6Enabled = true
	c.now = time.Now
	if err := c.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	pub := newFakePublisher(realClock{})
	pub.setReady(true)
	r, err := NewControlRuntime(c, NewCommandInbox("gateway_001", &recordAdmitter{}), pub, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r.ctx = context.Background()
	for range controlQueueCapacity {
		r.snapshots <- resultRequest{snapshot: true, expires: time.Now().Add(time.Second)}
	}
	q := readbackQueryForTest("gateway_001", "urn:ngsi-ld:Sensor:gateway_001:sensor_001", time.Now())
	payload, err := digitaltwin.MarshalMQTTConfigurationQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	r.handleConfigurationQuery(rawCommand{topic: "gateways/gateway_001/commands/" + q.RequestID, payload: payload})
	if r.Stats().SnapshotDropped != 1 || r.Stats().QueryAccepted != 0 {
		t.Fatalf("full snapshot queue counters=%+v", r.Stats())
	}
	r.publishResponse(resultRequest{snapshot: true, topic: "gateways/gateway_001/responses/" + q.RequestID,
		payload: []byte(`{}`), expires: time.Now().Add(-time.Second)}, nil)
	if pub.count() != 0 || r.Stats().SnapshotDropped != 2 {
		t.Fatalf("expired snapshot published or not counted: count=%d stats=%+v", pub.count(), r.Stats())
	}
}
