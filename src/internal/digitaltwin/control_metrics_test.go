package digitaltwin

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestControlMetricsRejectedAdmission(t *testing.T) {
	m := &ControlMetrics{}
	r := &PostgresControlRepository{metrics: m}
	if _, err := r.Admit(context.Background(), uuid.Nil, "invalid", DesiredRequest{}); err == nil {
		t.Fatal("accepted invalid actor")
	}
	m.observe("accepted", time.Millisecond)
	m.observe("replay", time.Millisecond)
	m.observe("noop", time.Millisecond)
	s := m.Snapshot()
	if s.Accepted != 1 || s.Replay != 1 || s.Noop != 1 || s.Rejected != 1 || s.Admission.Count != 4 {
		t.Fatal(s)
	}
}
