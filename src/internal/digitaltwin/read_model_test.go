package digitaltwin_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/telemetry"
)

func TestEntityType_Valid(t *testing.T) {
	if !digitaltwin.EntityTypeGateway.Valid() {
		t.Error("expected EntityTypeGateway to be valid")
	}
	if !digitaltwin.EntityTypeSensor.Valid() {
		t.Error("expected EntityTypeSensor to be valid")
	}
	invalid := []digitaltwin.EntityType{"Device", "device", "gateway", "", "Other"}
	for _, it := range invalid {
		if it.Valid() {
			t.Errorf("expected %q to be invalid", it)
		}
	}
}

func TestHistorySelector_Valid(t *testing.T) {
	if !digitaltwin.SelectorValue.Valid() {
		t.Error("expected SelectorValue to be valid")
	}
	if !digitaltwin.SelectorSampleIntervalUS.Valid() {
		t.Error("expected SelectorSampleIntervalUS to be valid")
	}

	invalid := []digitaltwin.HistorySelector{
		"measurement",
		"sampleIntervalUs",
		"Device",
		"anomaly_score",
		"reconstruction_loss",
		"",
	}
	for _, is := range invalid {
		if is.Valid() {
			t.Errorf("expected %q to be invalid selector", is)
		}
	}
}

func TestParsedIdentity_String(t *testing.T) {
	gw := digitaltwin.ParsedIdentity{
		Type:      digitaltwin.EntityTypeGateway,
		GatewayID: "gw_001",
		URN:       "urn:ngsi-ld:Gateway:gw_001",
	}
	if gw.String() != "Gateway(gw_001)" {
		t.Errorf("unexpected string: %s", gw.String())
	}

	sen := digitaltwin.ParsedIdentity{
		Type:      digitaltwin.EntityTypeSensor,
		GatewayID: "gw_001",
		SensorID:  "sen_001",
		URN:       "urn:ngsi-ld:Sensor:gw_001:sen_001",
	}
	if sen.String() != "Sensor(gw_001:sen_001)" {
		t.Errorf("unexpected string: %s", sen.String())
	}
}

type fakeReadService struct{}

func (f *fakeReadService) ListTwins(
	_ context.Context,
	actor uuid.UUID,
	_ digitaltwin.TwinListQuery,
) (*digitaltwin.TwinListResult, error) {
	if actor == uuid.Nil {
		return nil, digitaltwin.ErrNilActor
	}
	return &digitaltwin.TwinListResult{
		Items:      []digitaltwin.TwinSummary{},
		NextCursor: nil,
	}, nil
}

func (f *fakeReadService) GetTwinDetail(
	_ context.Context,
	actor uuid.UUID,
	_ digitaltwin.TwinDetailQuery,
) (*digitaltwin.TwinDetailResult, error) {
	if actor == uuid.Nil {
		return nil, digitaltwin.ErrNilActor
	}
	return &digitaltwin.TwinDetailResult{
		ID:        "urn:ngsi-ld:Gateway:gw1",
		Type:      digitaltwin.EntityTypeGateway,
		Name:      "GW 1",
		GatewayID: "gw1",
		UpdatedAt: time.Now().UTC(),
	}, nil
}

func (f *fakeReadService) GetTwinState(
	_ context.Context,
	actor uuid.UUID,
	_ digitaltwin.TwinStateQuery,
) (*digitaltwin.TwinStateResult, error) {
	if actor == uuid.Nil {
		return nil, digitaltwin.ErrNilActor
	}
	return &digitaltwin.TwinStateResult{
		ID:              "urn:ngsi-ld:Gateway:gw1",
		Type:            digitaltwin.EntityTypeGateway,
		GatewayID:       "gw1",
		ReportedState:   json.RawMessage(`{}`),
		DesiredState:    json.RawMessage(`{}`),
		ReportedVersion: 1,
		DesiredVersion:  0,
		UpdatedAt:       time.Now().UTC(),
	}, nil
}

func (f *fakeReadService) GetTwinHistory(
	_ context.Context,
	actor uuid.UUID,
	q digitaltwin.TwinHistoryQuery,
) (*digitaltwin.TwinHistoryResult, error) {
	if actor == uuid.Nil {
		return nil, digitaltwin.ErrNilActor
	}
	if !q.Property.Valid() {
		return nil, digitaltwin.ErrInvalidProperty
	}
	return &digitaltwin.TwinHistoryResult{
		EntityID:   q.EntityID,
		Property:   q.Property,
		From:       q.Range.From,
		To:         q.Range.To,
		Mode:       "raw",
		Resolution: string(q.Resolution),
		PointCount: 0,
	}, nil
}

func TestReadService_Contract(t *testing.T) {
	var svc digitaltwin.ReadService = &fakeReadService{}
	ctx := context.Background()

	// Nil actor
	if _, err := svc.ListTwins(ctx, uuid.Nil, digitaltwin.TwinListQuery{}); err != digitaltwin.ErrNilActor {
		t.Fatalf("expected ErrNilActor on ListTwins, got %v", err)
	}
	if _, err := svc.GetTwinDetail(ctx, uuid.Nil, digitaltwin.TwinDetailQuery{}); err != digitaltwin.ErrNilActor {
		t.Fatalf("expected ErrNilActor on GetTwinDetail, got %v", err)
	}
	if _, err := svc.GetTwinState(ctx, uuid.Nil, digitaltwin.TwinStateQuery{}); err != digitaltwin.ErrNilActor {
		t.Fatalf("expected ErrNilActor on GetTwinState, got %v", err)
	}
	if _, err := svc.GetTwinHistory(ctx, uuid.Nil, digitaltwin.TwinHistoryQuery{}); err != digitaltwin.ErrNilActor {
		t.Fatalf("expected ErrNilActor on GetTwinHistory, got %v", err)
	}

	// Valid actor
	actor := uuid.New()
	listRes, err := svc.ListTwins(ctx, actor, digitaltwin.TwinListQuery{})
	if err != nil || listRes == nil {
		t.Fatalf("ListTwins failed: %v", err)
	}

	detailRes, err := svc.GetTwinDetail(ctx, actor, digitaltwin.TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"})
	if err != nil || detailRes.ID != "urn:ngsi-ld:Gateway:gw1" {
		t.Fatalf("GetTwinDetail failed: %v", err)
	}

	stateRes, err := svc.GetTwinState(ctx, actor, digitaltwin.TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"})
	if err != nil || stateRes.ReportedVersion != 1 {
		t.Fatalf("GetTwinState failed: %v", err)
	}

	// Invalid property selector
	_, err = svc.GetTwinHistory(ctx, actor, digitaltwin.TwinHistoryQuery{
		EntityID: "urn:ngsi-ld:Sensor:gw1:sen1",
		Property: "measurement", // invalid selector
	})
	if err != digitaltwin.ErrInvalidProperty {
		t.Fatalf("expected ErrInvalidProperty, got %v", err)
	}

	// Valid property selector
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(time.Second)
	histRes, err := svc.GetTwinHistory(ctx, actor, digitaltwin.TwinHistoryQuery{
		EntityID:   "urn:ngsi-ld:Sensor:gw1:sen1",
		Property:   digitaltwin.SelectorValue,
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  1000,
	})
	if err != nil || histRes.Property != digitaltwin.SelectorValue {
		t.Fatalf("GetTwinHistory failed: %v", err)
	}
}
