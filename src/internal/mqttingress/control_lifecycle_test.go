package mqttingress

import (
	"context"
	"errors"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestControlConnectionPrerequisiteAndQoS(t *testing.T) {
	h := newHarness()
	grant := byte(1)
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{subscribeFn: func(_ *fakeClient, topic string, _ byte) subscribeToken {
			return completedSubscribe(map[string]byte{topic: grant}, nil)
		}}
	}
	calls := 0
	allowed := false
	tr := newTestTransport(t, &recordAdmitter{}, h, func(opts *TransportOptions) {
		opts.Filter = "gateways/+/responses/#"
		opts.RequireQoS1 = true
		opts.VerifyConnection = func(ctx context.Context) error {
			calls++
			if _, ok := ctx.Deadline(); !ok {
				t.Error("unbounded verification")
			}
			if !allowed {
				return errors.New("ACL denied")
			}
			return nil
		}
	})
	for i := 0; i < 2; i++ {
		if lt, err := tr.connectOnce(context.Background()); err == nil {
			tr.closeLifetime(lt, 0)
			t.Error("missing ACL prerequisite")
		}
	}
	if calls != 2 || h.clientCount() != 0 {
		t.Fatalf("verification calls=%d data connects=%d", calls, h.clientCount())
	}
	allowed = true
	grant = 0
	if lt, err := tr.connectOnce(context.Background()); err == nil {
		tr.closeLifetime(lt, 0)
		t.Error("downgraded SUBACK accepted")
	}
	grant = 1
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tr.closeLifetime(lt, 0)
	if calls != 4 {
		t.Fatalf("missing reconnect revalidation: %d", calls)
	}
}
