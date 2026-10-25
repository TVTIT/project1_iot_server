package gatewaysim

import (
	"bytes"
	"context"
	"encoding/json"
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
