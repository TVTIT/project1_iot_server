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

func TestRecheckMetricsAreFixedCardinalityAndIdentityFree(t *testing.T) {
	m := &Metrics{}
	m.RecheckSweep(SweepReport{SessionsChecked: 64, DistinctKeys: 3, Chunks: 1, Queries: 1, Timeouts: 1, Duration: 20 * time.Millisecond})
	s := m.Snapshot(NewHub(HubConfig{}))
	if s.RecheckSessionsExamined != 64 || s.RecheckDistinctKeys != 3 || s.RecheckChunks != 1 || s.RecheckQueries != 1 || s.RecheckTimeouts != 1 || s.RecheckDuration.Count != 1 {
		t.Fatalf("recheck observation missing: %+v", s)
	}
	var disabled *Metrics
	disabled.RecheckSweep(SweepReport{SessionsChecked: 1})
}
