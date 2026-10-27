package telemetryws

import (
	"testing"
	"time"
)

func TestMetricsTracksOriginAndSeparatesEventFromDeliveries(t *testing.T) {
	m := &Metrics{}
	now := time.Now()
	m.Submitted(now)
	m.NewEvent(now)
	m.DeliveryEnqueued(2)
	m.TelemetryWriteCompleted(now)
	m.TelemetryWriteCompleted(time.Time{})
	m.ControlWriteCompleted()
	s := m.Snapshot(NewHub(HubConfig{}))
	if s.Submissions != 1 || s.NewEvents != 1 || s.DeliveriesEnqueued != 2 || s.TelemetryWritesCompleted != 2 || s.ControlWritesCompleted != 1 {
		t.Fatalf("counter distinction lost: %+v", s)
	}
	if s.OriginMissing != 1 || s.ConfirmedToSubmit.Count != 1 || s.ConfirmedToWrite.Count != 1 {
		t.Fatalf("origin histogram/missing accounting: %+v", s)
	}
}

func TestMetricsNilSafe(t *testing.T) {
	var m *Metrics
	m.Submitted(time.Time{})
	if got := m.Snapshot(nil); got.Availability != "unavailable" {
		t.Fatalf("availability=%q", got.Availability)
	}
}
