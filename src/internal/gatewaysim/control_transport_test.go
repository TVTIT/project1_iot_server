package gatewaysim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestControlGatewayConfigurationOptIn(t *testing.T) {
	env := validGatewayEnv()
	cfg, err := Load(mapLookup(env))
	if err != nil || cfg.ControlEnabled {
		t.Fatalf("default opt-in %v/%v", cfg.ControlEnabled, err)
	}
	env["GATEWAY_CONTROL_ENABLED"] = "true"
	cfg, err = Load(mapLookup(env))
	if err != nil || !cfg.ControlEnabled || !TransportOptionsFromConfig(cfg).ControlEnabled {
		t.Fatalf("control flag not mapped %v/%v", cfg.ControlEnabled, err)
	}
	env["GATEWAY_CONTROL_ENABLED"] = "typo"
	if _, err := Load(mapLookup(env)); err == nil {
		t.Fatal("invalid flag accepted")
	}
}

func TestControlTransportRequiresBothSubscriptions(t *testing.T) {
	for _, granted := range []byte{0, 1, 0x80} {
		t.Run(fmt.Sprint(granted), func(t *testing.T) {
			h := newHarness()
			h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
				return &fakeClient{openOnConnect: true, subscribeFn: func(_ *fakeClient, topic string, _ byte) subscribeToken {
					q := byte(1)
					if topic == "gateways/gateway_001/commands/#" {
						q = granted
					}
					return completedSubscribe(map[string]byte{topic: q}, nil)
				}}
			}
			tr := newTestTransport(t, &recordAdmitter{}, h, func(o *TransportOptions) { o.ControlEnabled = true })
			lt, err := tr.connectOnce(context.Background())
			if granted != 1 {
				if err == nil {
					t.Fatal("control accepted denied/downgraded command subscription")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			tr.closeLifetime(lt, 0)
			h.mu.Lock()
			c := h.clients[len(h.clients)-1]
			h.mu.Unlock()
			c.mu.Lock()
			n, filter := c.subscribes, c.lastSubTopic
			c.mu.Unlock()
			if n != 2 || filter != "gateways/gateway_001/commands/#" {
				t.Fatalf("control subscriptions=%d last=%s", n, filter)
			}
			lt, err = tr.connectOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			tr.closeLifetime(lt, 0)
			if h.client(1).subscribes != 2 {
				t.Fatal("replacement lifetime did not restore both subscriptions")
			}
		})
	}
}

func TestTransportStagesDeliveryUntilInstall(t *testing.T) {
	for _, control := range []bool{false, true} {
		t.Run(fmt.Sprint(control), func(t *testing.T) {
			h := newHarness()
			admit := &recordAdmitter{}
			ackTopic := "gateways/gateway_001/acks/early"
			commandTopic := "gateways/gateway_001/commands/early"
			h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
				return &fakeClient{subscribeFn: func(c *fakeClient, filter string, _ byte) subscribeToken {
					topic := ackTopic
					if filter == "gateways/gateway_001/commands/#" {
						topic = commandTopic
					}
					payload := []byte("before-suback")
					c.invokeMessage(topic, payload, false)
					// Paho owns the incoming buffer; staging must copy it.
					payload[0] = 'X'
					if len(admit.calls()) != 0 {
						t.Error("delivery admitted before subscription validation/install")
					}
					return completedSubscribe(map[string]byte{filter: 1}, nil)
				}}
			}
			tr := newTestTransport(t, admit, h, func(o *TransportOptions) { o.ControlEnabled = control })
			lt, err := tr.connectOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { tr.closeLifetime(lt, 0) })
			h.client(0).invokeMessage(ackTopic, []byte("before-install"), false)
			if tr.Ready() || len(admit.calls()) != 0 {
				t.Fatal("transport ready or delivery admitted before install")
			}
			if !tr.installCurrent(lt) {
				t.Fatal("valid lifetime not installed")
			}
			h.client(0).invokeMessage(ackTopic, []byte("after-install"), false)
			want := []string{"before-suback", "before-install", "after-install"}
			if control {
				want = []string{"before-suback", "before-suback", "before-install", "after-install"}
			}
			got := admit.calls()
			if len(got) != len(want) {
				t.Fatalf("admitted %d deliveries, want %d", len(got), len(want))
			}
			for i, payload := range want {
				if string(got[i].payload) != payload {
					t.Fatalf("delivery %d = %q, want %q", i, got[i].payload, payload)
				}
			}
			if control && got[1].topic != commandTopic {
				t.Fatal("early command not routed to command inbox")
			}
			if !tr.Ready() || tr.Snapshot().StaleDropped != 0 {
				t.Fatal("successful startup marked early deliveries stale")
			}
		})
	}
}

func TestTransportEarlyValidCommandReachesCommandInbox(t *testing.T) {
	h := newHarness()
	ack := &recordAdmitter{}
	inbox := NewCommandInbox("gateway_001", ack)
	topic, payload, _ := controlTestCommand(t, 1, 10000)
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
		return &fakeClient{subscribeFn: func(c *fakeClient, filter string, _ byte) subscribeToken {
			if filter == "gateways/gateway_001/commands/#" {
				c.invokeMessage(topic, payload, false)
			} else {
				c.invokeMessage("gateways/gateway_001/acks/early", []byte("ack"), false)
			}
			return completedSubscribe(map[string]byte{filter: 1}, nil)
		}}
	}
	tr := newTestTransport(t, inbox, h, func(o *TransportOptions) { o.ControlEnabled = true })
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.closeLifetime(lt, 0) })
	if len(inbox.queue) != 0 || len(ack.calls()) != 0 {
		t.Fatal("unvalidated lifetime reached command/ACK processor")
	}
	if !tr.installCurrent(lt) || len(inbox.queue) != 1 || len(ack.calls()) != 1 {
		t.Fatal("early command/ACK lost at install")
	}
	raw := <-inbox.queue
	if raw.topic != topic || !bytes.Equal(raw.payload, payload) {
		t.Fatal("early command envelope changed")
	}
}

func TestTransportDiscardsStartupDeliveriesOnFailure(t *testing.T) {
	for _, mode := range []string{"denied", "downgraded", "lost", "closed", "stop-reconnect"} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness()
			admit := &recordAdmitter{}
			h.clientFn = func(*mqtt.ClientOptions) *fakeClient {
				return &fakeClient{subscribeFn: func(c *fakeClient, filter string, _ byte) subscribeToken {
					c.invokeMessage("gateways/gateway_001/acks/early", []byte("early"), false)
					qos := byte(1)
					if filter == "gateways/gateway_001/commands/#" {
						c.invokeMessage("gateways/gateway_001/commands/early", []byte("early"), false)
						switch mode {
						case "denied":
							qos = 0x80
						case "downgraded":
							qos = 0
						case "lost":
							c.lose(errors.New("injected loss before install"))
						}
					}
					return completedSubscribe(map[string]byte{filter: qos}, nil)
				}}
			}
			tr := newTestTransport(t, admit, h, func(o *TransportOptions) { o.ControlEnabled = true })
			lt, err := tr.connectOnce(context.Background())
			if mode == "closed" || mode == "stop-reconnect" {
				if err != nil {
					t.Fatal(err)
				}
				if mode == "closed" {
					if err := tr.Stop(context.Background()); err != nil {
						t.Fatal(err)
					}
				} else {
					tr.StopReconnect()
				}
				if tr.installCurrent(lt) {
					t.Fatal("shutdown lifetime installed")
				}
				tr.closeLifetime(lt, 0)
			} else if err == nil {
				tr.closeLifetime(lt, 0)
				t.Fatal("failed subscription/lifetime accepted")
			}
			h.client(0).invokeMessage("gateways/gateway_001/commands/stale", []byte("stale"), false)
			if tr.Ready() || len(admit.calls()) != 0 {
				t.Fatal("failed startup delivery escaped to inbox")
			}
			if lt != nil && len(lt.staged) != 0 {
				t.Fatal("startup buffer retained after teardown")
			}
		})
	}
}

func TestTransportStartupDeliveryLimitsFailClosed(t *testing.T) {
	for _, mode := range []string{"capacity", "payload", "topic"} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness()
			h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
			admit := &recordAdmitter{}
			tr := newTestTransport(t, admit, h, func(o *TransportOptions) { o.ControlEnabled = true })
			lt, err := tr.connectOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { tr.closeLifetime(lt, 0) })
			topic := "gateways/gateway_001/commands/early"
			payload := []byte("early")
			switch mode {
			case "capacity":
				for i := 0; i < startupDeliveryCapacity; i++ {
					h.client(0).invokeMessage(topic, payload, false)
				}
			case "payload":
				payload = make([]byte, startupPayloadBytes+1)
			case "topic":
				topic = string(make([]byte, startupTopicBytes+1))
			}
			h.client(0).invokeMessage(topic, payload, false)
			if tr.installCurrent(lt) || tr.Ready() || len(admit.calls()) != 0 {
				t.Fatal("startup overflow left lifetime usable or drained partial buffer")
			}
			if !errors.Is(tr.Fatal(), ErrTransportStartupOverflow) || len(lt.staged) != 0 {
				t.Fatal("startup overflow not surfaced or buffer not discarded")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := tr.WaitReady(ctx); !errors.Is(err, ErrTransportStartupOverflow) {
				t.Fatalf("WaitReady = %v, want startup overflow", err)
			}
			if err := tr.Stop(ctx); !errors.Is(err, ErrTransportStartupOverflow) {
				t.Fatalf("Stop = %v, want startup overflow", err)
			}
		})
	}
}

func TestTransportStartupDeliveryAtLimits(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	admit := &recordAdmitter{}
	tr := newTestTransport(t, admit, h, func(o *TransportOptions) { o.ControlEnabled = true })
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.closeLifetime(lt, 0) })
	topic := string(make([]byte, startupTopicBytes))
	payload := make([]byte, startupPayloadBytes)
	for i := 0; i < startupDeliveryCapacity; i++ {
		tr.onMessage(lt, topic, payload, false)
	}
	if !tr.installCurrent(lt) || tr.Fatal() != nil {
		t.Fatal("startup delivery limits refused inclusive boundary")
	}
	if got := len(admit.calls()); got != startupDeliveryCapacity {
		t.Fatalf("admitted = %d, want %d", got, startupDeliveryCapacity)
	}
}

func TestTransportInstalledLifetimeNeverRestagesStaleDelivery(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	admit := &recordAdmitter{}
	tr := newTestTransport(t, admit, h, func(o *TransportOptions) { o.ControlEnabled = true })
	old, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.closeLifetime(old, 0) })
	tr.installCurrent(old)
	next, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.closeLifetime(next, 0) })
	h.client(1).invokeMessage("gateways/gateway_001/commands/next", []byte("next"), false)
	tr.installCurrent(next)
	h.client(0).invokeMessage("gateways/gateway_001/commands/old", []byte("old"), false)
	if got := admit.calls(); len(got) != 1 || string(got[0].payload) != "next" {
		t.Fatalf("stale lifetime delivery admitted: %v", got)
	}
	if len(old.staged) != 0 || tr.Snapshot().StaleDropped != 1 {
		t.Fatal("stale installed lifetime returned to staging")
	}
}

// The pause exists only in this test; production Admit is non-blocking.
type startupDrainAdmitter struct {
	recordAdmitter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (a *startupDrainAdmitter) Admit(topic string, payload []byte, retained bool) {
	a.once.Do(func() {
		close(a.entered)
		<-a.release
	})
	a.recordAdmitter.Admit(topic, payload, retained)
}

func TestTransportLiveCallbackCannotOvertakeStartupDrain(t *testing.T) {
	h := newHarness()
	h.clientFn = func(*mqtt.ClientOptions) *fakeClient { return &fakeClient{} }
	admit := &startupDrainAdmitter{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(admit.release) }) }
	t.Cleanup(release)
	tr := newTestTransport(t, admit, h, func(o *TransportOptions) { o.ControlEnabled = true })
	lt, err := tr.connectOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { release(); tr.closeLifetime(lt, 0) })
	topic := "gateways/gateway_001/commands/ordered"
	tr.onMessage(lt, topic, []byte("first"), false)
	tr.onMessage(lt, topic, []byte("second"), false)
	installed := make(chan bool, 1)
	go func() { installed <- tr.installCurrent(lt) }()
	select {
	case <-admit.entered:
	case <-time.After(time.Second):
		t.Fatal("startup drain did not begin")
	}
	liveStarted, liveDone := make(chan struct{}), make(chan struct{})
	go func() {
		close(liveStarted)
		tr.onMessage(lt, topic, []byte("third"), false)
		close(liveDone)
	}()
	<-liveStarted
	release()
	select {
	case ok := <-installed:
		if !ok {
			t.Fatal("lifetime not installed")
		}
	case <-time.After(time.Second):
		t.Fatal("install deadlocked")
	}
	select {
	case <-liveDone:
	case <-time.After(time.Second):
		t.Fatal("live callback deadlocked")
	}
	got := admit.calls()
	if len(got) != 3 {
		t.Fatalf("deliveries = %d, want 3", len(got))
	}
	for i, want := range []string{"first", "second", "third"} {
		if string(got[i].payload) != want {
			t.Fatalf("delivery %d = %q, want %q", i, got[i].payload, want)
		}
	}
}
