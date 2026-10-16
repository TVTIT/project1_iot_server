package gatewaysim

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// The types below are the narrow Paho boundary the simulator transport uses.
// They mirror the shapes already used by internal/mqttingress (pinned
// Paho v1.5.1) so the transport can be driven by a small fake in tests without
// importing the full Paho client. Nothing here is exported: the simulator never
// leaks Paho types to its callers.

// waitToken is the minimal Paho token surface the transport waits on.
type waitToken interface {
	Done() <-chan struct{}
	Error() error
}

// subscribeToken additionally exposes the per-filter SUBACK result. A completed
// token is not proof the subscription was granted, so the transport always
// reads Result() and requires a positive granted QoS for its own ACK filter.
type subscribeToken interface {
	waitToken
	Result() map[string]byte
}

// mqttMessageHandler is the inbound callback shape. Paho invokes it from its
// ordered router goroutine, never a per-message goroutine.
type mqttMessageHandler func(topic string, payload []byte, retained bool)

// mqttClient is the narrow Paho client boundary the supervisor uses. The real
// *mqtt.Client is adapted by pahoClient; tests inject a fake.
type mqttClient interface {
	Connect() waitToken
	Disconnect(quiesce uint)
	Subscribe(topic string, qos byte, handler mqttMessageHandler) subscribeToken
	Publish(topic string, qos byte, retained bool, payload []byte) waitToken
}

// pahoClient adapts a real *mqtt.Client to mqttClient.
type pahoClient struct{ c mqtt.Client }

func newPahoClient(opts *mqtt.ClientOptions) mqttClient {
	return &pahoClient{c: mqtt.NewClient(opts)}
}

func (p *pahoClient) Connect() waitToken { return pahoWait{token: p.c.Connect()} }

func (p *pahoClient) Disconnect(quiesce uint) { p.c.Disconnect(quiesce) }

func (p *pahoClient) Publish(topic string, qos byte, retained bool, payload []byte) waitToken {
	return pahoWait{token: p.c.Publish(topic, qos, retained, payload)}
}

func (p *pahoClient) Subscribe(topic string, qos byte, handler mqttMessageHandler) subscribeToken {
	token := p.c.Subscribe(topic, qos, func(_ mqtt.Client, message mqtt.Message) {
		handler(message.Topic(), message.Payload(), message.Retained())
	})
	if st, ok := token.(*mqtt.SubscribeToken); ok {
		return &pahoSubscribe{token: st}
	}
	return &pahoSubscribe{fallback: pahoWait{token: token}}
}

type pahoWait struct{ token mqtt.Token }

func (w pahoWait) Done() <-chan struct{} { return w.token.Done() }
func (w pahoWait) Error() error          { return w.token.Error() }

type pahoSubscribe struct {
	token    *mqtt.SubscribeToken
	fallback pahoWait
}

func (s *pahoSubscribe) Done() <-chan struct{} {
	if s.token != nil {
		return s.token.Done()
	}
	return s.fallback.Done()
}

func (s *pahoSubscribe) Error() error {
	if s.token != nil {
		return s.token.Error()
	}
	return s.fallback.Error()
}

func (s *pahoSubscribe) Result() map[string]byte {
	if s.token != nil {
		return s.token.Result()
	}
	return nil
}
