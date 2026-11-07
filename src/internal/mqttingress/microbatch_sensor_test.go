package mqttingress

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

const sensorSchedulingWait = 2 * time.Second

type firstValidationGate struct {
	Validator
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (v *firstValidationGate) Validate(topic string, payload []byte, received time.Time) (*telemetry.Message, error) {
	blocked := false
	v.once.Do(func() {
		blocked = true
		close(v.entered)
	})
	if blocked {
		<-v.release
	}
	return v.Validator.Validate(topic, payload, received)
}

func (v *firstValidationGate) unblock() {
	select {
	case <-v.release:
	default:
		close(v.release)
	}
}

type sensorBatchCall struct {
	identities []telemetry.MessageIdentity
}

func sensorTestBatch(messages []*telemetry.Message) []telemetry.BatchPersistResult {
	results := make([]telemetry.BatchPersistResult, len(messages))
	for index, message := range messages {
		results[index] = telemetry.BatchPersistResult{
			InputIndex:   index,
			Identity:     identityOf(message),
			Outcome:      telemetry.CommitOutcomeCommitted,
			SamplesSaved: message.SampleCount(),
		}
	}
	return results
}

func waitSensorSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(sensorSchedulingWait):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func startSensorTestIngress(t *testing.T, opts Options, validator Validator, batch telemetry.BatchRepository, publisher Publisher) *Ingress {
	t.Helper()
	ing := newIngressWithBatch(t, opts, validator, &fakeRepo{}, batch, publisher)
	mustStart(t, ing)
	t.Cleanup(func() {
		ing.cancelRun()
		ctx, cancel := context.WithTimeout(context.Background(), sensorSchedulingWait)
		defer cancel()
		_ = ing.Drain(ctx)
	})
	return ing
}

func admitSensorTestMessage(t *testing.T, ing *Ingress, gatewayID, sensorID string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	topic, payload, receivedAt := testPayloadFor(t, gatewayID, sensorID, id, testBootID, 1)
	if got := ing.Admit(topic, payload, receivedAt, false); got != AdmissionAccepted {
		t.Fatalf("Admit(%s,%s) = %v", gatewayID, sensorID, got)
	}
	return id
}

func TestMicrobatchSensorSchedulingNeverMixesSensorsInPersistBatch(t *testing.T) {
	gate := &firstValidationGate{Validator: newValidator(t), entered: make(chan struct{}), release: make(chan struct{})}
	calls := make(chan sensorBatchCall, 4)
	batch := batchRepoFunc(func(ctx context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		call := sensorBatchCall{identities: make([]telemetry.MessageIdentity, len(messages))}
		for index, message := range messages {
			call.identities[index] = identityOf(message)
		}
		select {
		case calls <- call:
		case <-ctx.Done():
			return nil
		}
		return sensorTestBatch(messages)
	})
	opts := microOptions()
	opts.WorkerCount = 1
	opts.Microbatch.MaxMessages = 4
	opts.QueueCapacity = 8
	ing := startSensorTestIngress(t, opts, gate, batch, &fakePublisher{})
	t.Cleanup(gate.unblock)

	expected := map[uuid.UUID]struct{}{
		admitSensorTestMessage(t, ing, "gateway_001", "sensor_a"): {},
	}
	waitSensorSignal(t, gate.entered, "first validation barrier")
	expected[admitSensorTestMessage(t, ing, "gateway_001", "sensor_b")] = struct{}{}
	expected[admitSensorTestMessage(t, ing, "gateway_001", "sensor_a")] = struct{}{}
	expected[admitSensorTestMessage(t, ing, "gateway_001", "sensor_b")] = struct{}{}
	gate.unblock()

	drainCtx, cancel := context.WithTimeout(context.Background(), sensorSchedulingWait)
	defer cancel()
	if err := ing.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	close(calls)
	accounted := make(map[uuid.UUID]struct{}, 4)
	for call := range calls {
		if len(call.identities) == 0 {
			t.Fatal("PersistBatch called with an empty group")
		}
		wantGateway, wantSensor := call.identities[0].GatewayID, call.identities[0].SensorID
		for _, identity := range call.identities {
			if _, ok := expected[identity.MessageID]; !ok {
				t.Errorf("PersistBatch returned unadmitted message %s", identity.MessageID)
			}
			if _, duplicate := accounted[identity.MessageID]; duplicate {
				t.Errorf("PersistBatch returned message %s more than once", identity.MessageID)
			}
			accounted[identity.MessageID] = struct{}{}
		}
		for _, identity := range call.identities[1:] {
			if identity.GatewayID != wantGateway || identity.SensorID != wantSensor {
				t.Errorf("PersistBatch mixed Sensors: group identities = %+v", call.identities)
				break
			}
		}
	}
	if len(accounted) != 4 || len(expected) != 4 {
		t.Fatalf("PersistBatch accounted for %d unique messages, want all 4 admitted messages", len(accounted))
	}
	for id := range expected {
		if _, ok := accounted[id]; !ok {
			t.Errorf("admitted message %s did not reach PersistBatch", id)
		}
	}
}

func TestMicrobatchSensorSchedulingHoldsLeaseThroughACKAndUsesGatewayIdentity(t *testing.T) {
	opts := microOptions()
	opts.WorkerCount = 2
	opts.Microbatch.MaxMessages = 1
	opts.QueueCapacity = 8
	firstAckEntered, releaseFirstAck := make(chan struct{}), make(chan struct{})
	var ackOnce sync.Once
	publisher := &fakePublisher{onPublish: func(ctx context.Context, delivery Delivery) error {
		if strings.HasPrefix(delivery.Topic, "gateways/gateway_001/acks/") {
			ackOnce.Do(func() { close(firstAckEntered) })
			select {
			case <-releaseFirstAck:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	calls := make(chan sensorBatchCall, 8)
	batch := batchRepoFunc(func(ctx context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		call := sensorBatchCall{identities: make([]telemetry.MessageIdentity, len(messages))}
		for index, message := range messages {
			call.identities[index] = identityOf(message)
		}
		select {
		case calls <- call:
		case <-ctx.Done():
			return nil
		}
		return sensorTestBatch(messages)
	})
	ing := startSensorTestIngress(t, opts, newValidator(t), batch, publisher)
	t.Cleanup(func() {
		select {
		case <-releaseFirstAck:
		default:
			close(releaseFirstAck)
		}
	})

	a1ID := admitSensorTestMessage(t, ing, "gateway_001", "sensor_same")
	select {
	case call := <-calls:
		if !sensorCallOnly(call, "gateway_001", "sensor_same") || len(call.identities) != 1 || call.identities[0].MessageID != a1ID {
			t.Fatalf("first repository call = %+v", call.identities)
		}
	case <-time.After(sensorSchedulingWait):
		t.Fatal("first message did not reach repository")
	}
	waitSensorSignal(t, firstAckEntered, "first message post-commit ACK")
	// The held ACK occupies one worker. FIFO runtime dispatches A2 to the other
	// worker; a per-(gateway,sensor) scheduler must instead leave it queued while
	// allowing the distinct gateway's sensor with the same sensor_id to run.
	a2ID := admitSensorTestMessage(t, ing, "gateway_001", "sensor_same")
	b1ID := admitSensorTestMessage(t, ing, "gateway_002", "sensor_same")
	select {
	case call := <-calls:
		if !sensorCallOnly(call, "gateway_002", "sensor_same") || len(call.identities) != 1 || call.identities[0].MessageID != b1ID {
			t.Fatalf("while gateway_001 ACK is blocked, next repository call = %+v; want gateway_002/sensor_same and no second gateway_001 call", call.identities)
		}
	case <-time.After(sensorSchedulingWait):
		t.Fatal("distinct gateway's same-named Sensor did not reach repository while first ACK was blocked")
	}
	select {
	case call := <-calls:
		t.Fatalf("another gateway_001/sensor_same repository call overlapped blocked ACK: %+v", call.identities)
	default:
	}
	close(releaseFirstAck)

	drainCtx, cancel := context.WithTimeout(context.Background(), sensorSchedulingWait)
	defer cancel()
	if err := ing.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// The first A1 and distinct-gateway B1 repository calls were consumed and
	// asserted above. Include them when checking the complete three-call ledger.
	seen := map[string]int{
		"gateway_001/sensor_same": 1,
		"gateway_002/sensor_same": 1,
	}
	totalCalls := 2
	for {
		select {
		case call := <-calls:
			if len(call.identities) != 1 {
				t.Fatalf("unexpected repository call size after Drain: %d", len(call.identities))
			}
			id := call.identities[0]
			if id.GatewayID != "gateway_001" || id.SensorID != "sensor_same" || id.MessageID != a2ID {
				t.Fatalf("unexpected repository call after ACK release: %+v; want A2 %s", id, a2ID)
			}
			seen[fmt.Sprintf("%s/%s", id.GatewayID, id.SensorID)]++
			totalCalls++
		default:
			if totalCalls != 3 || seen["gateway_001/sensor_same"] != 2 || seen["gateway_002/sensor_same"] != 1 {
				t.Fatalf("repository call count=%d by full Sensor identity=%v, want exactly 3 calls (A1,A2,B1)", totalCalls, seen)
			}
			return
		}
	}
}

func TestMicrobatchSensorSchedulingGivesWaitingSensorNextFiniteBatchTurn(t *testing.T) {
	gate := &firstValidationGate{Validator: newValidator(t), entered: make(chan struct{}), release: make(chan struct{})}
	calls := make(chan sensorBatchCall, 8)
	batch := batchRepoFunc(func(ctx context.Context, messages []*telemetry.Message) []telemetry.BatchPersistResult {
		call := sensorBatchCall{identities: make([]telemetry.MessageIdentity, len(messages))}
		for index, message := range messages {
			call.identities[index] = identityOf(message)
		}
		select {
		case calls <- call:
		case <-ctx.Done():
			return nil
		}
		return sensorTestBatch(messages)
	})
	opts := microOptions()
	opts.WorkerCount = 1
	opts.Microbatch.MaxMessages = 2
	opts.QueueCapacity = 10
	ing := startSensorTestIngress(t, opts, gate, batch, &fakePublisher{})
	t.Cleanup(gate.unblock)

	admitSensorTestMessage(t, ing, "gateway_001", "sensor_hot")
	waitSensorSignal(t, gate.entered, "first hot-Sensor validation barrier")
	for range 5 {
		admitSensorTestMessage(t, ing, "gateway_001", "sensor_hot")
	}
	admitSensorTestMessage(t, ing, "gateway_001", "sensor_waiting")
	gate.unblock()

	select {
	case first := <-calls:
		if !sensorCallOnly(first, "gateway_001", "sensor_hot") {
			t.Fatalf("first batch = %+v; want first hot-Sensor batch", first.identities)
		}
	case <-time.After(sensorSchedulingWait):
		t.Fatal("first finite hot-Sensor batch did not flush")
	}
	select {
	case next := <-calls:
		if !sensorCallOnly(next, "gateway_001", "sensor_waiting") {
			t.Fatalf("waiting Sensor did not get the next finite batch turn: second batch identities = %+v", next.identities)
		}
	case <-time.After(sensorSchedulingWait):
		t.Fatal("waiting Sensor did not reach PersistBatch after one hot-Sensor batch")
	}

	drainCtx, cancel := context.WithTimeout(context.Background(), sensorSchedulingWait)
	defer cancel()
	if err := ing.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

func sensorCallOnly(call sensorBatchCall, gatewayID, sensorID string) bool {
	if len(call.identities) == 0 {
		return false
	}
	for _, identity := range call.identities {
		if identity.GatewayID != gatewayID || identity.SensorID != sensorID {
			return false
		}
	}
	return true
}
