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

func TestTwinHistoryResult_Branches(t *testing.T) {
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(2 * time.Second)
	unit := "degC"
	intervalUnit := "us"

	t.Run("branch 1 - delegated telemetry result", func(t *testing.T) {
		bootID := uuid.MustParse("7f2c45f7-0a76-4e28-8a37-7793d7d85b04")
		msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
		idx0 := int32(0)

		res := &digitaltwin.TwinHistoryResult{
			EntityID:   "urn:ngsi-ld:Sensor:gw1:sen1",
			Property:   digitaltwin.SelectorValue,
			Unit:       &unit,
			From:       from,
			To:         to,
			Mode:       "raw",
			Resolution: "raw",
			PointCount: 2,
			TelemetryResult: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gw1",
				SensorID:   "sen1",
				Unit:       &unit,
				From:       from,
				To:         to,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 2,
				RawPoints: []telemetry.RawSamplePoint{
					{
						MeasuredAt:  from.Add(500 * time.Millisecond),
						Value:       25.1,
						SequenceNum: 12501,
						BootID:      &bootID,
						MessageID:   &msgID,
						SampleIndex: &idx0,
					},
					{
						MeasuredAt:  from.Add(1250 * time.Millisecond),
						Value:       25.2,
						SequenceNum: 9007199254740993, // > 2^53 integer precision
						BootID:      nil,              // nullable legacy provenance preserved
						MessageID:   nil,
						SampleIndex: nil,
					},
				},
			},
			TemporalPoints:  nil,
			TemporalBuckets: nil,
		}

		if res.TelemetryResult == nil {
			t.Fatal("expected non-nil TelemetryResult")
		}
		if len(res.TemporalPoints) != 0 {
			t.Fatalf("expected empty TemporalPoints in delegated branch, got %d", len(res.TemporalPoints))
		}
		if len(res.TemporalBuckets) != 0 {
			t.Fatalf("expected empty TemporalBuckets in delegated branch, got %d", len(res.TemporalBuckets))
		}
		if res.TelemetryResult.RawPoints[1].SequenceNum != 9007199254740993 {
			t.Fatalf("expected int64 sequenceNum > 2^53, got %d", res.TelemetryResult.RawPoints[1].SequenceNum)
		}
		if res.TelemetryResult.RawPoints[1].BootID != nil || res.TelemetryResult.RawPoints[1].MessageID != nil {
			t.Fatal("expected legacy NULL provenance preserved")
		}
	})

	t.Run("branch 2 - temporal raw observations typed preservation", func(t *testing.T) {
		msgID := uuid.MustParse("0195e18c-9fc1-7a42-9064-69ea49e63bf2")
		valNum := 10000.0
		valText := "auto"
		valBool := true

		metaNum := json.RawMessage(`{"unit":"us","boot_id":"7f2c45f7-0a76-4e28-8a37-7793d7d85b04","source":"telemetry_batch","seq":9007199254740993}`)
		metaText := json.RawMessage(`{}`)
		metaBool := json.RawMessage(`{"unit":null}`)

		res := &digitaltwin.TwinHistoryResult{
			EntityID:   "urn:ngsi-ld:Sensor:gw1:sen1",
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Unit:       &intervalUnit,
			From:       from,
			To:         to,
			Mode:       "raw",
			Resolution: "raw",
			PointCount: 3,
			TemporalPoints: []digitaltwin.TemporalSamplePoint{
				{
					ObservedAt:   from.Add(100 * time.Millisecond),
					ValueType:    "number",
					ValueNumber:  &valNum,
					ValueText:    nil,
					ValueBoolean: nil,
					Metadata:     metaNum,
					MessageID:    &msgID,
				},
				{
					ObservedAt:   from.Add(500 * time.Millisecond),
					ValueType:    "text",
					ValueNumber:  nil,
					ValueText:    &valText,
					ValueBoolean: nil,
					Metadata:     metaText,
					MessageID:    nil, // nullable message ID
				},
				{
					ObservedAt:   from.Add(900 * time.Millisecond),
					ValueType:    "boolean",
					ValueNumber:  nil,
					ValueText:    nil,
					ValueBoolean: &valBool,
					Metadata:     metaBool,
					MessageID:    nil,
				},
			},
			TemporalBuckets: nil,
			TelemetryResult: nil,
		}

		if res.TelemetryResult != nil {
			t.Fatal("expected nil TelemetryResult in temporal raw branch")
		}
		if len(res.TemporalBuckets) != 0 {
			t.Fatalf("expected empty TemporalBuckets in temporal raw branch, got %d", len(res.TemporalBuckets))
		}
		if len(res.TemporalPoints) != 3 {
			t.Fatalf("expected 3 TemporalPoints, got %d", len(res.TemporalPoints))
		}

		// Verify numeric typed preservation
		p0 := res.TemporalPoints[0]
		if p0.ValueType != "number" || p0.ValueNumber == nil || *p0.ValueNumber != 10000.0 {
			t.Fatalf("unexpected number point: %+v", p0)
		}
		if p0.ValueText != nil || p0.ValueBoolean != nil {
			t.Fatal("expected text and bool pointers nil for number point")
		}
		if string(p0.Metadata) != string(metaNum) {
			t.Fatalf("metadata JSONB mutated: %s", string(p0.Metadata))
		}
		if p0.MessageID == nil || *p0.MessageID != msgID {
			t.Fatalf("expected messageID %s, got %v", msgID, p0.MessageID)
		}

		// Verify text typed preservation
		p1 := res.TemporalPoints[1]
		if p1.ValueType != "text" || p1.ValueText == nil || *p1.ValueText != "auto" {
			t.Fatalf("unexpected text point: %+v", p1)
		}
		if p1.ValueNumber != nil || p1.ValueBoolean != nil {
			t.Fatal("expected number and bool pointers nil for text point")
		}
		if p1.MessageID != nil {
			t.Fatal("expected nil message ID preserved for text point")
		}

		// Verify boolean typed preservation
		p2 := res.TemporalPoints[2]
		if p2.ValueType != "boolean" || p2.ValueBoolean == nil || *p2.ValueBoolean != true {
			t.Fatalf("unexpected bool point: %+v", p2)
		}
		if p2.ValueNumber != nil || p2.ValueText != nil {
			t.Fatal("expected number and text pointers nil for bool point")
		}
	})

	t.Run("branch 3 - temporal numeric buckets without fake observations or message IDs", func(t *testing.T) {
		res := &digitaltwin.TwinHistoryResult{
			EntityID:   "urn:ngsi-ld:Sensor:gw1:sen1",
			Property:   digitaltwin.SelectorSampleIntervalUS,
			Unit:       &intervalUnit,
			From:       from,
			To:         to,
			Mode:       "bucket",
			Resolution: "1s",
			PointCount: 2,
			TemporalBuckets: []digitaltwin.TemporalBucketPoint{
				{
					BucketStart: from,
					Avg:         10000.0,
					Min:         10000.0,
					Max:         10000.0,
					Count:       6000000000, // int64 count > 2^31
				},
				{
					BucketStart: from.Add(time.Second),
					Avg:         20000.0,
					Min:         15000.0,
					Max:         25000.0,
					Count:       100,
				},
			},
			TemporalPoints:  nil,
			TelemetryResult: nil,
		}

		if res.TelemetryResult != nil {
			t.Fatal("expected nil TelemetryResult in temporal bucket branch")
		}
		if len(res.TemporalPoints) != 0 {
			t.Fatalf("expected empty TemporalPoints in temporal bucket branch, got %d", len(res.TemporalPoints))
		}
		if len(res.TemporalBuckets) != 2 {
			t.Fatalf("expected 2 TemporalBuckets, got %d", len(res.TemporalBuckets))
		}

		b0 := res.TemporalBuckets[0]
		if b0.Count != 6000000000 {
			t.Fatalf("expected int64 count 6000000000, got %d", b0.Count)
		}
		if b0.Avg != 10000.0 || b0.Min != 10000.0 || b0.Max != 10000.0 {
			t.Fatalf("unexpected bucket values: %+v", b0)
		}
		if !b0.BucketStart.Equal(from) {
			t.Fatalf("expected BucketStart %v, got %v", from, b0.BucketStart)
		}
	})

	t.Run("branch 4 - authorized empty series", func(t *testing.T) {
		res := &digitaltwin.TwinHistoryResult{
			EntityID:        "urn:ngsi-ld:Sensor:gw1:sen1",
			Property:        digitaltwin.SelectorSampleIntervalUS,
			Unit:            &intervalUnit,
			From:            from,
			To:              to,
			Mode:            "bucket",
			Resolution:      "1s",
			PointCount:      0,
			TemporalPoints:  nil,
			TemporalBuckets: nil,
			TelemetryResult: nil,
		}

		if res.PointCount != 0 {
			t.Fatalf("expected PointCount 0, got %d", res.PointCount)
		}
		if res.TemporalPoints != nil || res.TemporalBuckets != nil || res.TelemetryResult != nil {
			t.Fatal("expected all data slices to be nil for authorized empty result")
		}
	})
}
