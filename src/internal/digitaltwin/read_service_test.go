package digitaltwin

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/telemetry"
)

// fakeEntityResolver implements EntityResolver for read service unit tests.
type fakeEntityResolver struct {
	entity    *ResolvedEntity
	err       error
	onResolve func(ctx context.Context)
}

func (f *fakeEntityResolver) ResolveEntity(ctx context.Context, _ uuid.UUID, _ ParsedIdentity) (*ResolvedEntity, error) {
	if f.onResolve != nil {
		f.onResolve(ctx)
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.entity, nil
}

// fakeTwinReader implements TwinReader for read service unit tests.
type fakeTwinReader struct {
	listResult    *TwinListResult
	listErr       error
	detailResult  *TwinDetailResult
	detailErr     error
	stateResult   *TwinStateResult
	stateErr      error
	historyResult *TwinHistoryResult
	historyErr    error

	lastActor         uuid.UUID
	lastListQuery     TwinListQuery
	lastDetailQuery   TwinDetailQuery
	lastDetailEntity  ResolvedEntity
	lastStateEntity   ResolvedEntity
	lastHistoryActor  uuid.UUID
	lastHistoryEntity ResolvedEntity
	lastHistoryQuery  TwinHistoryQuery
	onList            func(ctx context.Context)
	onDetail          func(ctx context.Context)
	onState           func(ctx context.Context)
	onHistory         func(ctx context.Context)
}

func (f *fakeTwinReader) ListEntities(ctx context.Context, actor uuid.UUID, q TwinListQuery) (*TwinListResult, error) {
	f.lastActor = actor
	f.lastListQuery = q
	if f.onList != nil {
		f.onList(ctx)
	}
	if f.listErr != nil {
		return f.listResult, f.listErr
	}
	return f.listResult, nil
}

func (f *fakeTwinReader) GetDetail(ctx context.Context, actor uuid.UUID, entity ResolvedEntity, q TwinDetailQuery) (*TwinDetailResult, error) {
	f.lastActor = actor
	f.lastDetailEntity = entity
	f.lastDetailQuery = q
	if f.onDetail != nil {
		f.onDetail(ctx)
	}
	if f.detailErr != nil {
		return f.detailResult, f.detailErr
	}
	return f.detailResult, nil
}

func (f *fakeTwinReader) GetState(ctx context.Context, actor uuid.UUID, entity ResolvedEntity) (*TwinStateResult, error) {
	f.lastActor = actor
	f.lastStateEntity = entity
	if f.onState != nil {
		f.onState(ctx)
	}
	if f.stateErr != nil {
		return f.stateResult, f.stateErr
	}
	return f.stateResult, nil
}

func (f *fakeTwinReader) ReadTemporalHistory(ctx context.Context, actor uuid.UUID, entity ResolvedEntity, q TwinHistoryQuery) (*TwinHistoryResult, error) {
	f.lastHistoryActor = actor
	f.lastHistoryEntity = entity
	f.lastHistoryQuery = q
	if f.onHistory != nil {
		f.onHistory(ctx)
	}
	if f.historyErr != nil {
		return f.historyResult, f.historyErr
	}
	return f.historyResult, nil
}

// fakeTelemetryHistoryDelegator implements TelemetryHistoryDelegator for read service unit tests.
type fakeTelemetryHistoryDelegator struct {
	result    *telemetry.TelemetryHistoryResult
	err       error
	lastActor uuid.UUID
	lastQuery telemetry.TelemetryHistoryQuery
	lastCtx   context.Context
	onExecute func(ctx context.Context)
}

func (f *fakeTelemetryHistoryDelegator) ExecuteEffectiveHistoryQuery(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
	f.lastCtx = ctx
	f.lastActor = actor
	f.lastQuery = q
	if f.onExecute != nil {
		f.onExecute(ctx)
	}
	if f.err != nil {
		return f.result, f.err
	}
	return f.result, nil
}

var _ TwinReader = (*fakeTwinReader)(nil)
var _ EntityResolver = (*fakeEntityResolver)(nil)
var _ TelemetryHistoryDelegator = (*fakeTelemetryHistoryDelegator)(nil)

func setupTestReadServiceWithDelegator(t *testing.T, resolver EntityResolver, reader TwinReader, delegator TelemetryHistoryDelegator) (ReadService, *ReadPreparation) {
	t.Helper()
	cfg := DefaultReadPreparationConfig()
	prep, err := NewReadPreparation(resolver, cfg)
	if err != nil {
		t.Fatalf("failed to create ReadPreparation: %v", err)
	}
	svc, err := NewReadService(prep, reader, delegator)
	if err != nil {
		t.Fatalf("failed to create ReadService: %v", err)
	}
	return svc, prep
}

func setupTestReadService(t *testing.T, resolver EntityResolver, reader TwinReader) (ReadService, *ReadPreparation) {
	t.Helper()
	return setupTestReadServiceWithDelegator(t, resolver, reader, &fakeTelemetryHistoryDelegator{})
}

func TestNewReadService_Dependencies(t *testing.T) {
	cfg := DefaultReadPreparationConfig()
	prep, err := NewReadPreparation(&fakeEntityResolver{}, cfg)
	if err != nil {
		t.Fatalf("unexpected error creating prep: %v", err)
	}
	reader := &fakeTwinReader{}
	delegator := &fakeTelemetryHistoryDelegator{}

	// 1. Nil preparation
	if _, err := NewReadService(nil, reader, delegator); err == nil {
		t.Fatal("expected error on nil preparation, got nil")
	}

	// 2. Typed-nil preparation
	var typedNilPrep *ReadPreparation
	if _, err := NewReadService(typedNilPrep, reader, delegator); err == nil {
		t.Fatal("expected error on typed-nil preparation, got nil")
	}

	// 3. Nil reader
	if _, err := NewReadService(prep, nil, delegator); err == nil {
		t.Fatal("expected error on nil reader, got nil")
	}

	// 4. Typed-nil reader
	var typedNilReader *fakeTwinReader
	if _, err := NewReadService(prep, typedNilReader, delegator); err == nil {
		t.Fatal("expected error on typed-nil reader, got nil")
	}

	// 5. Nil telemetryHistory
	if _, err := NewReadService(prep, reader, nil); err == nil {
		t.Fatal("expected error on nil telemetryHistory, got nil")
	}

	// 6. Typed-nil telemetryHistory
	var typedNilDelegator *fakeTelemetryHistoryDelegator
	if _, err := NewReadService(prep, reader, typedNilDelegator); err == nil {
		t.Fatal("expected error on typed-nil telemetryHistory, got nil")
	}

	// 7. Valid dependencies
	svc, err := NewReadService(prep, reader, delegator)
	if err != nil {
		t.Fatalf("unexpected error with valid dependencies: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil service, got nil")
	}
	var _ ReadService = svc
}

func TestReadService_DependenciesFailClosed(t *testing.T) {
	cfg := DefaultReadPreparationConfig()
	prep, err := NewReadPreparation(&fakeEntityResolver{}, cfg)
	if err != nil {
		t.Fatalf("failed to create prep: %v", err)
	}
	reader := &fakeTwinReader{}
	actor := uuid.New()

	// Struct directly instantiated with nil / typed-nil telemetryHistory
	// Existing list/detail/state must work with their own dependencies and NOT fail closed due to missing history dependency.
	svcWithNilDelegator := &readService{
		preparation:      prep,
		reader:           reader,
		telemetryHistory: nil,
	}

	ctx := context.Background()
	// Reader returns (nil, nil) so if preparation succeeds, reader returns ErrUnexpected (proving it did not fail with ErrDependencyUnavailable).
	if _, err := svcWithNilDelegator.ListTwins(ctx, actor, TwinListQuery{Limit: 10}); errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("ListTwins must not fail with ErrDependencyUnavailable when telemetryHistory is nil, got %v", err)
	}
	if _, err := svcWithNilDelegator.GetTwinDetail(ctx, actor, TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1", RelationshipLimit: 10}); errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("GetTwinDetail must not fail with ErrDependencyUnavailable when telemetryHistory is nil, got %v", err)
	}
	if _, err := svcWithNilDelegator.GetTwinState(ctx, actor, TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}); errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("GetTwinState must not fail with ErrDependencyUnavailable when telemetryHistory is nil, got %v", err)
	}

	var typedNilDelegator *fakeTelemetryHistoryDelegator
	svcWithTypedNilDelegator := &readService{
		preparation:      prep,
		reader:           reader,
		telemetryHistory: typedNilDelegator,
	}
	if _, err := svcWithTypedNilDelegator.ListTwins(ctx, actor, TwinListQuery{Limit: 10}); errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("ListTwins must not fail with ErrDependencyUnavailable when telemetryHistory is typed-nil, got %v", err)
	}
	if _, err := svcWithTypedNilDelegator.GetTwinDetail(ctx, actor, TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1", RelationshipLimit: 10}); errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("GetTwinDetail must not fail with ErrDependencyUnavailable when telemetryHistory is typed-nil, got %v", err)
	}
	if _, err := svcWithTypedNilDelegator.GetTwinState(ctx, actor, TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}); errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("GetTwinState must not fail with ErrDependencyUnavailable when telemetryHistory is typed-nil, got %v", err)
	}

	// However, if preparation or reader is nil, all methods must fail closed with ErrDependencyUnavailable.
	svcWithNilReader := &readService{
		preparation:      prep,
		reader:           nil,
		telemetryHistory: &fakeTelemetryHistoryDelegator{},
	}
	if _, err := svcWithNilReader.ListTwins(ctx, actor, TwinListQuery{Limit: 10}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on ListTwins with nil reader, got %v", err)
	}
	if _, err := svcWithNilReader.GetTwinDetail(ctx, actor, TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on GetTwinDetail with nil reader, got %v", err)
	}
	if _, err := svcWithNilReader.GetTwinState(ctx, actor, TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on GetTwinState with nil reader, got %v", err)
	}
	if _, err := svcWithNilReader.GetTwinHistory(ctx, actor, TwinHistoryQuery{}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on GetTwinHistory with nil reader, got %v", err)
	}

	svcWithNilPrep := &readService{
		preparation:      nil,
		reader:           reader,
		telemetryHistory: &fakeTelemetryHistoryDelegator{},
	}
	if _, err := svcWithNilPrep.ListTwins(ctx, actor, TwinListQuery{Limit: 10}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on ListTwins with nil prep, got %v", err)
	}
	if _, err := svcWithNilPrep.GetTwinDetail(ctx, actor, TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on GetTwinDetail with nil prep, got %v", err)
	}
	if _, err := svcWithNilPrep.GetTwinState(ctx, actor, TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on GetTwinState with nil prep, got %v", err)
	}
	if _, err := svcWithNilPrep.GetTwinHistory(ctx, actor, TwinHistoryQuery{}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable on GetTwinHistory with nil prep, got %v", err)
	}
}

func TestReadService_ListTwins(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	nextCur := "v1.k.g.t"

	t.Run("success with items", func(t *testing.T) {
		expectedItems := []TwinSummary{
			{
				ID:        "urn:ngsi-ld:Gateway:gw1",
				Type:      EntityTypeGateway,
				GatewayID: "gw1",
				UpdatedAt: time.Now().UTC(),
			},
		}
		reader := &fakeTwinReader{
			listResult: &TwinListResult{
				Items:      expectedItems,
				NextCursor: &nextCur,
			},
		}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		q := TwinListQuery{Limit: 10}
		res, err := svc.ListTwins(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result")
		}
		if len(res.Items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(res.Items))
		}
		if res.NextCursor == nil || *res.NextCursor != nextCur {
			t.Fatalf("expected next cursor %q, got %v", nextCur, res.NextCursor)
		}
		if reader.lastActor != actor {
			t.Fatalf("expected actor %v, got %v", actor, reader.lastActor)
		}
	})

	t.Run("empty list success", func(t *testing.T) {
		reader := &fakeTwinReader{
			listResult: &TwinListResult{
				Items:      []TwinSummary{},
				NextCursor: nil,
			},
		}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		res, err := svc.ListTwins(context.Background(), actor, TwinListQuery{Limit: 20})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result")
		}
		if len(res.Items) != 0 {
			t.Fatalf("expected 0 items, got %d", len(res.Items))
		}
		if res.NextCursor != nil {
			t.Fatalf("expected nil next cursor, got %v", res.NextCursor)
		}
	})

	t.Run("actor uuid.Nil rejected", func(t *testing.T) {
		reader := &fakeTwinReader{}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		res, err := svc.ListTwins(context.Background(), uuid.Nil, TwinListQuery{Limit: 10})
		if !errors.Is(err, ErrNilActor) {
			t.Fatalf("expected ErrNilActor, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %v", res)
		}
	})

	t.Run("invalid query limit rejected", func(t *testing.T) {
		reader := &fakeTwinReader{}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		res, err := svc.ListTwins(context.Background(), actor, TwinListQuery{Limit: 9999})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %v", res)
		}
	})

	t.Run("reader error propagation", func(t *testing.T) {
		dbErr := errors.New("database connection refused")
		reader := &fakeTwinReader{listErr: dbErr}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		res, err := svc.ListTwins(context.Background(), actor, TwinListQuery{Limit: 10})
		if !errors.Is(err, dbErr) {
			t.Fatalf("expected dbErr, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %v", res)
		}
	})

	t.Run("context canceled returns ErrDependencyUnavailable without partial result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // pre-cancel context

		partialItems := []TwinSummary{
			{ID: "urn:ngsi-ld:Gateway:gw1", Type: EntityTypeGateway},
		}
		reader := &fakeTwinReader{
			listResult: &TwinListResult{Items: partialItems},
		}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		res, err := svc.ListTwins(ctx, actor, TwinListQuery{Limit: 10})
		if !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on context cancellation, got partial %v", res)
		}
	})

	t.Run("deadline propagation to reader", func(t *testing.T) {
		var observedDeadline time.Time
		var hasDeadline bool

		reader := &fakeTwinReader{
			onList: func(ctx context.Context) {
				observedDeadline, hasDeadline = ctx.Deadline()
			},
			listResult: &TwinListResult{Items: []TwinSummary{}},
		}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		_, err := svc.ListTwins(context.Background(), actor, TwinListQuery{Limit: 10})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !hasDeadline {
			t.Fatal("expected scoped context to have deadline")
		}
		if time.Until(observedDeadline) <= 0 || time.Until(observedDeadline) > 6*time.Second {
			t.Fatalf("unexpected deadline duration: %v", time.Until(observedDeadline))
		}
	})
}

func TestReadService_GetTwinDetail(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	gwURN := "urn:ngsi-ld:Gateway:gw1"
	sensorURN := "urn:ngsi-ld:Sensor:gw1:sensor1"

	t.Run("gateway success", func(t *testing.T) {
		gwEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  gwURN,
			Type:       EntityTypeGateway,
			GatewayID:  "gw1",
		}
		expectedDetail := &TwinDetailResult{
			ID:        gwURN,
			Type:      EntityTypeGateway,
			Name:      "Gateway 1",
			GatewayID: "gw1",
			UpdatedAt: time.Now().UTC(),
			Relationships: []RelationshipItem{
				{
					Type:             "Relationship",
					RelationshipType: "hasSensor",
					TargetURN:        sensorURN,
					CreatedAt:        time.Now().UTC(),
				},
			},
			RelationshipPage: &RelationshipPage{
				RelationshipType: "hasSensor",
				Limit:            10,
				Count:            1,
				Partial:          false,
			},
		}

		resolver := &fakeEntityResolver{entity: gwEntity}
		reader := &fakeTwinReader{detailResult: expectedDetail}
		svc, _ := setupTestReadService(t, resolver, reader)

		q := TwinDetailQuery{EntityID: gwURN, RelationshipLimit: 10}
		res, err := svc.GetTwinDetail(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil detail result")
		}
		if res.ID != gwURN || res.Name != "Gateway 1" {
			t.Fatalf("unexpected detail result: %+v", res)
		}
		if len(res.Relationships) != 1 || res.Relationships[0].TargetURN != sensorURN {
			t.Fatalf("unexpected relationships: %+v", res.Relationships)
		}
		if reader.lastActor != actor {
			t.Fatalf("expected actor %v, got %v", actor, reader.lastActor)
		}
		if reader.lastDetailEntity.PublicURN != gwURN {
			t.Fatalf("expected entity URN %q, got %q", gwURN, reader.lastDetailEntity.PublicURN)
		}
	})

	t.Run("sensor success", func(t *testing.T) {
		unit := "celsius"
		sensorEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  sensorURN,
			Type:       EntityTypeSensor,
			GatewayID:  "gw1",
			SensorID:   "sensor1",
		}
		expectedDetail := &TwinDetailResult{
			ID:        sensorURN,
			Type:      EntityTypeSensor,
			Name:      "Sensor 1",
			Unit:      &unit,
			GatewayID: "gw1",
			UpdatedAt: time.Now().UTC(),
		}

		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{detailResult: expectedDetail}
		svc, _ := setupTestReadService(t, resolver, reader)

		q := TwinDetailQuery{EntityID: sensorURN, RelationshipLimit: 10}
		res, err := svc.GetTwinDetail(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil detail result")
		}
		if res.ID != sensorURN || res.Unit == nil || *res.Unit != "celsius" {
			t.Fatalf("unexpected detail result: %+v", res)
		}
	})

	t.Run("actor uuid.Nil rejected", func(t *testing.T) {
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, &fakeTwinReader{})

		res, err := svc.GetTwinDetail(context.Background(), uuid.Nil, TwinDetailQuery{EntityID: gwURN, RelationshipLimit: 10})
		if !errors.Is(err, ErrNilActor) {
			t.Fatalf("expected ErrNilActor, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("invalid URN rejected", func(t *testing.T) {
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, &fakeTwinReader{})

		res, err := svc.GetTwinDetail(context.Background(), actor, TwinDetailQuery{EntityID: "invalid-urn", RelationshipLimit: 10})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("not found error propagation from resolver", func(t *testing.T) {
		resolver := &fakeEntityResolver{err: ErrNotFound}
		svc, _ := setupTestReadService(t, resolver, &fakeTwinReader{})

		res, err := svc.GetTwinDetail(context.Background(), actor, TwinDetailQuery{EntityID: gwURN, RelationshipLimit: 10})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("not found error propagation from reader", func(t *testing.T) {
		gwEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  gwURN,
			Type:       EntityTypeGateway,
			GatewayID:  "gw1",
		}
		resolver := &fakeEntityResolver{entity: gwEntity}
		reader := &fakeTwinReader{detailErr: ErrNotFound}
		svc, _ := setupTestReadService(t, resolver, reader)

		res, err := svc.GetTwinDetail(context.Background(), actor, TwinDetailQuery{EntityID: gwURN, RelationshipLimit: 10})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("context expired returns ErrDependencyUnavailable without partial result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		gwEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  gwURN,
			Type:       EntityTypeGateway,
			GatewayID:  "gw1",
		}
		resolver := &fakeEntityResolver{entity: gwEntity}
		reader := &fakeTwinReader{
			detailResult: &TwinDetailResult{ID: gwURN, Name: "Partial Gateway"},
		}
		svc, _ := setupTestReadService(t, resolver, reader)

		res, err := svc.GetTwinDetail(ctx, actor, TwinDetailQuery{EntityID: gwURN, RelationshipLimit: 10})
		if !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on cancel, got %v", res)
		}
	})
}

func TestReadService_GetTwinState(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	gwURN := "urn:ngsi-ld:Gateway:gw1"
	sensorURN := "urn:ngsi-ld:Sensor:gw1:sensor1"

	t.Run("gateway state success", func(t *testing.T) {
		gwEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  gwURN,
			Type:       EntityTypeGateway,
			GatewayID:  "gw1",
		}
		now := time.Now().UTC()
		expectedState := &TwinStateResult{
			ID:              gwURN,
			Type:            EntityTypeGateway,
			GatewayID:       "gw1",
			ReportedState:   json.RawMessage(`{"status":"online"}`),
			DesiredState:    json.RawMessage(`{"mode":"auto"}`),
			ReportedVersion: 5,
			DesiredVersion:  6,
			LastReportedAt:  &now,
			LastDesiredAt:   &now,
			UpdatedAt:       now,
		}

		resolver := &fakeEntityResolver{entity: gwEntity}
		reader := &fakeTwinReader{stateResult: expectedState}
		svc, _ := setupTestReadService(t, resolver, reader)

		q := TwinStateQuery{EntityID: gwURN}
		res, err := svc.GetTwinState(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil state result")
		}
		if res.ID != gwURN || res.ReportedVersion != 5 || res.DesiredVersion != 6 {
			t.Fatalf("unexpected state result: %+v", res)
		}
		if string(res.ReportedState) != `{"status":"online"}` {
			t.Fatalf("unexpected reported state: %s", string(res.ReportedState))
		}
		if reader.lastActor != actor {
			t.Fatalf("expected actor %v, got %v", actor, reader.lastActor)
		}
		if reader.lastStateEntity.PublicURN != gwURN {
			t.Fatalf("expected entity URN %q, got %q", gwURN, reader.lastStateEntity.PublicURN)
		}
	})

	t.Run("sensor state success", func(t *testing.T) {
		sensorEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  sensorURN,
			Type:       EntityTypeSensor,
			GatewayID:  "gw1",
			SensorID:   "sensor1",
		}
		now := time.Now().UTC()
		expectedState := &TwinStateResult{
			ID:              sensorURN,
			Type:            EntityTypeSensor,
			GatewayID:       "gw1",
			ReportedState:   json.RawMessage(`{"temperature":25.4}`),
			DesiredState:    json.RawMessage(`{}`),
			ReportedVersion: 10,
			DesiredVersion:  0,
			LastReportedAt:  &now,
			UpdatedAt:       now,
		}

		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{stateResult: expectedState}
		svc, _ := setupTestReadService(t, resolver, reader)

		q := TwinStateQuery{EntityID: sensorURN}
		res, err := svc.GetTwinState(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil state result")
		}
		if res.ID != sensorURN || res.ReportedVersion != 10 {
			t.Fatalf("unexpected state result: %+v", res)
		}
	})

	t.Run("actor uuid.Nil rejected", func(t *testing.T) {
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, &fakeTwinReader{})

		res, err := svc.GetTwinState(context.Background(), uuid.Nil, TwinStateQuery{EntityID: gwURN})
		if !errors.Is(err, ErrNilActor) {
			t.Fatalf("expected ErrNilActor, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("invalid URN rejected", func(t *testing.T) {
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, &fakeTwinReader{})

		res, err := svc.GetTwinState(context.Background(), actor, TwinStateQuery{EntityID: "not-a-urn"})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("not found error propagation from resolver", func(t *testing.T) {
		resolver := &fakeEntityResolver{err: ErrNotFound}
		svc, _ := setupTestReadService(t, resolver, &fakeTwinReader{})

		res, err := svc.GetTwinState(context.Background(), actor, TwinStateQuery{EntityID: gwURN})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("not found error propagation from reader", func(t *testing.T) {
		gwEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  gwURN,
			Type:       EntityTypeGateway,
			GatewayID:  "gw1",
		}
		resolver := &fakeEntityResolver{entity: gwEntity}
		reader := &fakeTwinReader{stateErr: ErrNotFound}
		svc, _ := setupTestReadService(t, resolver, reader)

		res, err := svc.GetTwinState(context.Background(), actor, TwinStateQuery{EntityID: gwURN})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("context expired returns ErrDependencyUnavailable without partial result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		gwEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  gwURN,
			Type:       EntityTypeGateway,
			GatewayID:  "gw1",
		}
		resolver := &fakeEntityResolver{entity: gwEntity}
		reader := &fakeTwinReader{
			stateResult: &TwinStateResult{ID: gwURN},
		}
		svc, _ := setupTestReadService(t, resolver, reader)

		res, err := svc.GetTwinState(ctx, actor, TwinStateQuery{EntityID: gwURN})
		if !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on cancel, got %v", res)
		}
	})

	t.Run("nil service receiver", func(t *testing.T) {
		var nilSvc *readService
		if _, err := nilSvc.ListTwins(context.Background(), actor, TwinListQuery{Limit: 10}); !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable on nil receiver, got %v", err)
		}
		if _, err := nilSvc.GetTwinDetail(context.Background(), actor, TwinDetailQuery{EntityID: gwURN, RelationshipLimit: 10}); !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable on nil receiver, got %v", err)
		}
		if _, err := nilSvc.GetTwinState(context.Background(), actor, TwinStateQuery{EntityID: gwURN}); !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable on nil receiver, got %v", err)
		}
	})
}

func TestReadService_GetTwinHistory(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sensorURN := "urn:ngsi-ld:Sensor:gw1:sensor1"
	unit := "us"
	now := time.Date(2026, 8, 21, 10, 15, 10, 0, time.UTC)

	validSensorEntity := func() *ResolvedEntity {
		return &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  sensorURN,
			Type:       EntityTypeSensor,
			GatewayID:  "gw1",
			SensorID:   "sensor1",
		}
	}

	validSampleIntervalQuery := TwinHistoryQuery{
		EntityID: sensorURN,
		Property: SelectorSampleIntervalUS,
		Range: telemetry.TimeRange{
			From: now.Add(-5 * time.Second),
			To:   now,
		},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  1000,
	}

	t.Run("valid assigned owner, operator and viewer reads for sample_interval_us", func(t *testing.T) {
		actors := []struct {
			role  string
			actor uuid.UUID
		}{
			{"owner", uuid.MustParse("00000000-0000-0000-0000-000000000001")},
			{"operator", uuid.MustParse("00000000-0000-0000-0000-000000000002")},
			{"viewer", uuid.MustParse("00000000-0000-0000-0000-000000000003")},
		}
		for _, tc := range actors {
			t.Run(tc.role, func(t *testing.T) {
				sensorEntity := validSensorEntity()
				expectedResult := &TwinHistoryResult{
					EntityID:   sensorURN,
					Property:   SelectorSampleIntervalUS,
					Unit:       &unit,
					From:       validSampleIntervalQuery.Range.From,
					To:         validSampleIntervalQuery.Range.To,
					Mode:       "raw",
					Resolution: "raw",
					PointCount: 1,
					TemporalPoints: []TemporalSamplePoint{
						{
							ObservedAt:  now.Add(-2 * time.Second),
							ValueType:   "number",
							ValueNumber: func() *float64 { v := 10000.0; return &v }(),
						},
					},
				}

				resolver := &fakeEntityResolver{entity: sensorEntity}
				reader := &fakeTwinReader{historyResult: expectedResult}
				svc, _ := setupTestReadService(t, resolver, reader)

				res, err := svc.GetTwinHistory(context.Background(), tc.actor, validSampleIntervalQuery)
				if err != nil {
					t.Fatalf("unexpected error for role %s: %v", tc.role, err)
				}
				if res == nil {
					t.Fatalf("expected non-nil result for role %s", tc.role)
				}
				if res.PointCount != 1 || res.Property != SelectorSampleIntervalUS {
					t.Fatalf("unexpected history result: %+v", res)
				}
				if reader.lastHistoryActor != tc.actor {
					t.Fatalf("expected actor %v, got %v", tc.actor, reader.lastHistoryActor)
				}
				if reader.lastHistoryEntity.PublicURN != sensorURN {
					t.Fatalf("expected entity URN %s, got %s", sensorURN, reader.lastHistoryEntity.PublicURN)
				}
				if reader.lastHistoryQuery.EntityID != sensorURN {
					t.Fatalf("expected query entity ID %s, got %s", sensorURN, reader.lastHistoryQuery.EntityID)
				}
			})
		}
	})

	t.Run("property=value raw delegation success", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{}
		delegator := &fakeTelemetryHistoryDelegator{
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gw1",
				SensorID:   "sensor1",
				Unit:       &unit,
				From:       validSampleIntervalQuery.Range.From,
				To:         validSampleIntervalQuery.Range.To,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 2,
				RawPoints: []telemetry.RawSamplePoint{
					{MeasuredAt: now.Add(-3 * time.Second), Value: 24.5, SequenceNum: 100},
					{MeasuredAt: now.Add(-1 * time.Second), Value: 24.7, SequenceNum: 101},
				},
			},
		}
		svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

		q := validSampleIntervalQuery
		q.Property = SelectorValue

		res, err := svc.GetTwinHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error on raw value delegation: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result on raw value delegation")
		}
		if res.EntityID != sensorURN {
			t.Fatalf("expected EntityID %s, got %s", sensorURN, res.EntityID)
		}
		if res.Property != SelectorValue {
			t.Fatalf("expected Property %s, got %s", SelectorValue, res.Property)
		}
		if res.Unit == nil || *res.Unit != unit {
			t.Fatalf("expected Unit %v, got %v", unit, res.Unit)
		}
		if !res.From.Equal(delegator.result.From) || !res.To.Equal(delegator.result.To) {
			t.Fatalf("expected Range [%v, %v), got [%v, %v)", delegator.result.From, delegator.result.To, res.From, res.To)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected Mode=raw, Resolution=raw, got Mode=%s, Resolution=%s", res.Mode, res.Resolution)
		}
		if res.PointCount != 2 {
			t.Fatalf("expected PointCount=2, got %d", res.PointCount)
		}
		if res.TelemetryResult != delegator.result {
			t.Fatalf("expected TelemetryResult pointer preserved, got %+v", res.TelemetryResult)
		}
		if res.TemporalPoints != nil || res.TemporalBuckets != nil {
			t.Fatalf("temporal points and buckets must be nil for value delegation, got points=%v, buckets=%v", res.TemporalPoints, res.TemporalBuckets)
		}
		if delegator.lastActor != actor {
			t.Fatalf("expected delegator actor %v, got %v", actor, delegator.lastActor)
		}
		if delegator.lastQuery.GatewayID != "gw1" || delegator.lastQuery.SensorID != "sensor1" {
			t.Fatalf("expected delegator target gw1:sensor1, got %s:%s", delegator.lastQuery.GatewayID, delegator.lastQuery.SensorID)
		}
		if delegator.lastQuery.Range != q.Range || delegator.lastQuery.Resolution != q.Resolution || delegator.lastQuery.MaxPoints != q.MaxPoints {
			t.Fatalf("unexpected query forwarded to delegator: %+v", delegator.lastQuery)
		}
		if reader.lastHistoryActor != uuid.Nil {
			t.Fatalf("reader ReadTemporalHistory must not be called for value, got actor %v", reader.lastHistoryActor)
		}
	})

	t.Run("property=value bucket delegation success", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{}
		delegator := &fakeTelemetryHistoryDelegator{
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gw1",
				SensorID:   "sensor1",
				Unit:       &unit,
				From:       validSampleIntervalQuery.Range.From,
				To:         validSampleIntervalQuery.Range.To,
				Mode:       "bucket",
				Resolution: "1s",
				PointCount: 1,
				BucketPoints: []telemetry.BucketSamplePoint{
					{BucketStart: validSampleIntervalQuery.Range.From, Avg: 24.6, Min: 24.5, Max: 24.7, Count: 2},
				},
			},
		}
		svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

		q := validSampleIntervalQuery
		q.Property = SelectorValue
		q.Resolution = telemetry.Resolution("1s")

		res, err := svc.GetTwinHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error on bucket value delegation: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result on bucket value delegation")
		}
		if res.Mode != "bucket" || res.Resolution != "1s" {
			t.Fatalf("expected Mode=bucket, Resolution=1s, got Mode=%s, Resolution=%s", res.Mode, res.Resolution)
		}
		if res.PointCount != 1 {
			t.Fatalf("expected PointCount=1, got %d", res.PointCount)
		}
		if res.TelemetryResult != delegator.result {
			t.Fatalf("expected TelemetryResult pointer preserved")
		}
		if res.TemporalPoints != nil || res.TemporalBuckets != nil {
			t.Fatalf("temporal points and buckets must be nil for value delegation")
		}
		if reader.lastHistoryActor != uuid.Nil {
			t.Fatalf("reader must not be called for value")
		}
	})

	t.Run("property=value auto delegation success", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{}
		delegator := &fakeTelemetryHistoryDelegator{
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gw1",
				SensorID:   "sensor1",
				Unit:       &unit,
				From:       validSampleIntervalQuery.Range.From,
				To:         validSampleIntervalQuery.Range.To,
				Mode:       "bucket",
				Resolution: "5s",
				PointCount: 1,
				BucketPoints: []telemetry.BucketSamplePoint{
					{BucketStart: validSampleIntervalQuery.Range.From, Avg: 24.6, Min: 24.5, Max: 24.7, Count: 2},
				},
			},
		}
		svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

		q := validSampleIntervalQuery
		q.Property = SelectorValue
		q.Resolution = telemetry.ResolutionAuto

		res, err := svc.GetTwinHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error on auto value delegation: %v", err)
		}
		if res == nil {
			t.Fatal("expected non-nil result on auto value delegation")
		}
		if res.Mode != "bucket" || res.Resolution != "5s" {
			t.Fatalf("expected Mode=bucket, Resolution=5s, got Mode=%s, Resolution=%s", res.Mode, res.Resolution)
		}
		if delegator.lastQuery.Resolution != telemetry.ResolutionAuto {
			t.Fatalf("expected query resolution auto forwarded, got %s", delegator.lastQuery.Resolution)
		}
		if reader.lastHistoryActor != uuid.Nil {
			t.Fatalf("reader must not be called for value")
		}
	})

	t.Run("property=value uses resolved entity identity", func(t *testing.T) {
		sensorEntity := &ResolvedEntity{
			InternalID: uuid.New(),
			PublicURN:  "urn:ngsi-ld:Sensor:gw-res:sensor-res",
			Type:       EntityTypeSensor,
			GatewayID:  "gw-res",
			SensorID:   "sensor-res",
		}
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{}
		delegator := &fakeTelemetryHistoryDelegator{
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gw-res",
				SensorID:   "sensor-res",
				Unit:       &unit,
				From:       validSampleIntervalQuery.Range.From,
				To:         validSampleIntervalQuery.Range.To,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 0,
			},
		}
		svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

		q := validSampleIntervalQuery
		q.EntityID = "urn:ngsi-ld:Sensor:gw-res:sensor-res"
		q.Property = SelectorValue

		res, err := svc.GetTwinHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.EntityID != "urn:ngsi-ld:Sensor:gw-res:sensor-res" {
			t.Fatalf("expected EntityID %s, got %s", "urn:ngsi-ld:Sensor:gw-res:sensor-res", res.EntityID)
		}
		if delegator.lastQuery.GatewayID != "gw-res" || delegator.lastQuery.SensorID != "sensor-res" {
			t.Fatalf("expected delegator target gw-res:sensor-res, got %s:%s", delegator.lastQuery.GatewayID, delegator.lastQuery.SensorID)
		}
	})

	t.Run("property=value exact actor and scoped deadline context propagation", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{}
		var observedDeadline time.Time
		var hasDeadline bool
		delegator := &fakeTelemetryHistoryDelegator{
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gw1",
				SensorID:   "sensor1",
				From:       validSampleIntervalQuery.Range.From,
				To:         validSampleIntervalQuery.Range.To,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 0,
			},
			onExecute: func(ctx context.Context) {
				observedDeadline, hasDeadline = ctx.Deadline()
			},
		}
		svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

		q := validSampleIntervalQuery
		q.Property = SelectorValue

		_, err := svc.GetTwinHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if delegator.lastActor != actor {
			t.Fatalf("expected actor %v forwarded to delegator, got %v", actor, delegator.lastActor)
		}
		if !hasDeadline {
			t.Fatal("expected delegator to receive context with active deadline")
		}
		if time.Until(observedDeadline) <= 0 || time.Until(observedDeadline) > 6*time.Second {
			t.Fatalf("expected deadline within preparation budget (5s), got remaining: %v", time.Until(observedDeadline))
		}
	})

	t.Run("property=value delegator returning (nil, nil) returns ErrUnexpected", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{}
		delegator := &fakeTelemetryHistoryDelegator{result: nil, err: nil}
		svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

		q := validSampleIntervalQuery
		q.Property = SelectorValue

		res, err := svc.GetTwinHistory(context.Background(), actor, q)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected when delegator returns (nil, nil), got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("property=value delegator non-nil result with error suppresses result", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{}
		delegator := &fakeTelemetryHistoryDelegator{
			result: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gw1",
				SensorID:   "sensor1",
				PointCount: 10,
			},
			err: telemetry.ErrHistoryDependency,
		}
		svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

		q := validSampleIntervalQuery
		q.Property = SelectorValue

		res, err := svc.GetTwinHistory(context.Background(), actor, q)
		if !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on error, got %v", res)
		}
	})

	t.Run("property=value error translations", func(t *testing.T) {
		testCases := []struct {
			name         string
			telemetryErr error
			expectedErr  error
			assertCause  bool
		}{
			{
				name:         "not found mapped to ErrNotFound",
				telemetryErr: telemetry.ErrHistoryNotFound,
				expectedErr:  ErrNotFound,
				assertCause:  true,
			},
			{
				name:         "dependency mapped to ErrDependencyUnavailable",
				telemetryErr: telemetry.ErrHistoryDependency,
				expectedErr:  ErrDependencyUnavailable,
				assertCause:  true,
			},
			{
				name:         "unexpected mapped to ErrUnexpected",
				telemetryErr: telemetry.ErrHistoryUnexpected,
				expectedErr:  ErrUnexpected,
				assertCause:  true,
			},
			{
				name:         "nil actor mapped to ErrNilActor",
				telemetryErr: telemetry.ErrHistoryNilActor,
				expectedErr:  ErrNilActor,
				assertCause:  true,
			},
			{
				name:         "invalid input mapped to ErrInvalidInput",
				telemetryErr: telemetry.ErrHistoryInvalidInput,
				expectedErr:  ErrInvalidInput,
				assertCause:  true,
			},
			{
				name:         "raw limit exceeded preserved",
				telemetryErr: telemetry.ErrHistoryRawLimitExceeded,
				expectedErr:  ErrRawLimitExceeded,
				assertCause:  true,
			},
			{
				name:         "range too large preserved",
				telemetryErr: telemetry.ErrHistoryRangeTooLarge,
				expectedErr:  ErrRangeTooLarge,
				assertCause:  true,
			},
			{
				name:         "invalid resolution preserved",
				telemetryErr: telemetry.ErrHistoryInvalidResolution,
				expectedErr:  ErrInvalidResolution,
				assertCause:  true,
			},
			{
				name:         "resolution too fine preserved",
				telemetryErr: telemetry.ErrHistoryResolutionTooFine,
				expectedErr:  ErrResolutionTooFine,
				assertCause:  true,
			},
			{
				name:         "arbitrary error mapped to ErrUnexpected",
				telemetryErr: errors.New("underlying postgres timeout"),
				expectedErr:  ErrUnexpected,
				assertCause:  true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				sensorEntity := validSensorEntity()
				resolver := &fakeEntityResolver{entity: sensorEntity}
				reader := &fakeTwinReader{}
				delegator := &fakeTelemetryHistoryDelegator{err: tc.telemetryErr}
				svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

				q := validSampleIntervalQuery
				q.Property = SelectorValue

				res, err := svc.GetTwinHistory(context.Background(), actor, q)
				if !errors.Is(err, tc.expectedErr) {
					t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
				}
				if tc.assertCause && !errors.Is(err, tc.telemetryErr) {
					t.Fatalf("expected cause %v preserved in errors.Is, got %v", tc.telemetryErr, err)
				}
				if res != nil {
					t.Fatalf("expected nil result on error, got %v", res)
				}
			})
		}
	})

	t.Run("property=value result suppression after context cancellation", func(t *testing.T) {
		t.Run("pre-canceled context", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			sensorEntity := validSensorEntity()
			resolver := &fakeEntityResolver{entity: sensorEntity}
			reader := &fakeTwinReader{}
			delegator := &fakeTelemetryHistoryDelegator{
				result: &telemetry.TelemetryHistoryResult{
					GatewayID: "gw1",
					SensorID:  "sensor1",
				},
			}
			svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

			q := validSampleIntervalQuery
			q.Property = SelectorValue

			res, err := svc.GetTwinHistory(ctx, actor, q)
			if !errors.Is(err, ErrDependencyUnavailable) {
				t.Fatalf("expected ErrDependencyUnavailable on canceled context, got %v", err)
			}
			if res != nil {
				t.Fatalf("expected nil result on canceled context, got %v", res)
			}
		})

		t.Run("context canceled during delegator execution", func(t *testing.T) {
			sensorEntity := validSensorEntity()
			resolver := &fakeEntityResolver{entity: sensorEntity}
			reader := &fakeTwinReader{}
			ctx, cancel := context.WithCancel(context.Background())
			delegator := &fakeTelemetryHistoryDelegator{
				result: &telemetry.TelemetryHistoryResult{
					GatewayID:  "gw1",
					SensorID:   "sensor1",
					PointCount: 5,
				},
				onExecute: func(_ context.Context) {
					cancel()
				},
			}
			svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

			q := validSampleIntervalQuery
			q.Property = SelectorValue

			res, err := svc.GetTwinHistory(ctx, actor, q)
			if !errors.Is(err, ErrDependencyUnavailable) {
				t.Fatalf("expected ErrDependencyUnavailable when context canceled in execution, got %v", err)
			}
			if res != nil {
				t.Fatalf("expected nil result when canceled during execution, got %v", res)
			}
		})
	})

	t.Run("property=value unauthorized or invalid preparation does not invoke delegator", func(t *testing.T) {
		t.Run("missing or non-member entity", func(t *testing.T) {
			resolver := &fakeEntityResolver{err: ErrNotFound}
			reader := &fakeTwinReader{}
			delegator := &fakeTelemetryHistoryDelegator{}
			svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

			q := validSampleIntervalQuery
			q.Property = SelectorValue

			res, err := svc.GetTwinHistory(context.Background(), actor, q)
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("expected ErrNotFound, got %v", err)
			}
			if res != nil {
				t.Fatalf("expected nil result, got %v", res)
			}
			if delegator.lastActor != uuid.Nil {
				t.Fatal("delegator must not be invoked on non-member entity")
			}
			if reader.lastHistoryActor != uuid.Nil {
				t.Fatal("reader must not be invoked on non-member entity")
			}
		})

		t.Run("nil actor", func(t *testing.T) {
			resolver := &fakeEntityResolver{}
			reader := &fakeTwinReader{}
			delegator := &fakeTelemetryHistoryDelegator{}
			svc, _ := setupTestReadServiceWithDelegator(t, resolver, reader, delegator)

			q := validSampleIntervalQuery
			q.Property = SelectorValue

			res, err := svc.GetTwinHistory(context.Background(), uuid.Nil, q)
			if !errors.Is(err, ErrNilActor) {
				t.Fatalf("expected ErrNilActor, got %v", err)
			}
			if res != nil {
				t.Fatalf("expected nil result, got %v", res)
			}
			if delegator.lastActor != uuid.Nil {
				t.Fatal("delegator must not be invoked on nil actor")
			}
		})
	})

	t.Run("invalid actor uuid.Nil rejected before data access", func(t *testing.T) {
		reader := &fakeTwinReader{}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		res, err := svc.GetTwinHistory(context.Background(), uuid.Nil, validSampleIntervalQuery)
		if !errors.Is(err, ErrNilActor) {
			t.Fatalf("expected ErrNilActor, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
		if reader.lastHistoryActor != uuid.Nil {
			t.Fatal("reader must not be invoked on nil actor")
		}
	})

	t.Run("invalid property rejected before data access", func(t *testing.T) {
		reader := &fakeTwinReader{}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		q := validSampleIntervalQuery
		q.Property = HistorySelector("invalid_property")

		res, err := svc.GetTwinHistory(context.Background(), actor, q)
		if !errors.Is(err, ErrInvalidProperty) {
			t.Fatalf("expected ErrInvalidProperty, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
		if reader.lastHistoryActor != uuid.Nil {
			t.Fatal("reader must not be invoked on invalid property")
		}
	})

	t.Run("Gateway type entity rejected before data access", func(t *testing.T) {
		reader := &fakeTwinReader{}
		svc, _ := setupTestReadService(t, &fakeEntityResolver{}, reader)

		q := validSampleIntervalQuery
		q.EntityID = "urn:ngsi-ld:Gateway:gw1"

		res, err := svc.GetTwinHistory(context.Background(), actor, q)
		if !errors.Is(err, ErrInvalidProperty) {
			t.Fatalf("expected ErrInvalidProperty for non-sensor entity, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
		if reader.lastHistoryActor != uuid.Nil {
			t.Fatal("reader must not be invoked for Gateway entity")
		}
	})

	t.Run("inaccessible or missing graph returns ErrNotFound without data access", func(t *testing.T) {
		resolver := &fakeEntityResolver{err: ErrNotFound}
		reader := &fakeTwinReader{}
		svc, _ := setupTestReadService(t, resolver, reader)

		res, err := svc.GetTwinHistory(context.Background(), actor, validSampleIntervalQuery)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
		if reader.lastHistoryActor != uuid.Nil {
			t.Fatal("reader must not be invoked on missing entity")
		}
	})

	t.Run("temporal reader returns (nil, nil) returns ErrUnexpected", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{historyResult: nil, historyErr: nil}
		svc, _ := setupTestReadService(t, resolver, reader)

		res, err := svc.GetTwinHistory(context.Background(), actor, validSampleIntervalQuery)
		if !errors.Is(err, ErrUnexpected) {
			t.Fatalf("expected ErrUnexpected when reader returns (nil, nil), got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result, got %v", res)
		}
	})

	t.Run("temporal reader dependency error and result suppression", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{
			historyResult: &TwinHistoryResult{EntityID: sensorURN},
			historyErr:    ErrDependencyUnavailable,
		}
		svc, _ := setupTestReadService(t, resolver, reader)

		res, err := svc.GetTwinHistory(context.Background(), actor, validSampleIntervalQuery)
		if !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on reader error, got %v", res)
		}
	})

	t.Run("canceled context returns ErrDependencyUnavailable without partial result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		reader := &fakeTwinReader{
			historyResult: &TwinHistoryResult{EntityID: sensorURN},
		}
		svc, _ := setupTestReadService(t, resolver, reader)

		res, err := svc.GetTwinHistory(ctx, actor, validSampleIntervalQuery)
		if !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable, got %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result on canceled context, got %v", res)
		}
	})

	t.Run("single deadline context propagated to reader", func(t *testing.T) {
		sensorEntity := validSensorEntity()
		resolver := &fakeEntityResolver{entity: sensorEntity}
		var observedDeadline time.Time
		var hasDeadline bool

		reader := &fakeTwinReader{
			historyResult: &TwinHistoryResult{EntityID: sensorURN, Property: SelectorSampleIntervalUS},
			onHistory: func(ctx context.Context) {
				observedDeadline, hasDeadline = ctx.Deadline()
			},
		}
		svc, _ := setupTestReadService(t, resolver, reader)

		_, err := svc.GetTwinHistory(context.Background(), actor, validSampleIntervalQuery)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !hasDeadline {
			t.Fatal("expected reader to receive context with deadline from preparation")
		}
		if time.Until(observedDeadline) <= 0 || time.Until(observedDeadline) > 6*time.Second {
			t.Fatalf("expected deadline within preparation budget (5s), got remaining: %v", time.Until(observedDeadline))
		}
	})

	t.Run("missing telemetryHistory does not disable list/detail/state or sample_interval_us", func(t *testing.T) {
		cfg := DefaultReadPreparationConfig()
		sensorEntity := validSensorEntity()
		prep, err := NewReadPreparation(&fakeEntityResolver{entity: sensorEntity}, cfg)
		if err != nil {
			t.Fatalf("failed to create prep: %v", err)
		}
		expectedHistory := &TwinHistoryResult{
			EntityID: sensorURN,
			Property: SelectorSampleIntervalUS,
		}
		reader := &fakeTwinReader{
			listResult:    &TwinListResult{Items: []TwinSummary{}},
			detailResult:  &TwinDetailResult{ID: "urn:ngsi-ld:Gateway:gw1"},
			stateResult:   &TwinStateResult{ID: "urn:ngsi-ld:Gateway:gw1"},
			historyResult: expectedHistory,
		}
		svcWithNilDelegator := &readService{
			preparation:      prep,
			reader:           reader,
			telemetryHistory: nil,
		}

		ctx := context.Background()
		if _, err := svcWithNilDelegator.ListTwins(ctx, actor, TwinListQuery{Limit: 10}); err != nil {
			t.Fatalf("expected ListTwins to succeed with nil delegator, got %v", err)
		}
		if _, err := svcWithNilDelegator.GetTwinDetail(ctx, actor, TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1", RelationshipLimit: 10}); err != nil {
			t.Fatalf("expected GetTwinDetail to succeed with nil delegator, got %v", err)
		}
		if _, err := svcWithNilDelegator.GetTwinState(ctx, actor, TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}); err != nil {
			t.Fatalf("expected GetTwinState to succeed with nil delegator, got %v", err)
		}
		// sample_interval_us does not need telemetryHistory and must succeed
		res, err := svcWithNilDelegator.GetTwinHistory(ctx, actor, validSampleIntervalQuery)
		if err != nil {
			t.Fatalf("expected GetTwinHistory(sample_interval_us) to succeed with nil delegator, got %v", err)
		}
		if res == nil || res.EntityID != sensorURN {
			t.Fatalf("unexpected history result: %+v", res)
		}
		// value requires telemetryHistory and must fail closed with ErrDependencyUnavailable
		valueQuery := validSampleIntervalQuery
		valueQuery.Property = SelectorValue
		if _, err := svcWithNilDelegator.GetTwinHistory(ctx, actor, valueQuery); !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable for value with nil delegator, got %v", err)
		}
	})

	t.Run("nil service receiver", func(t *testing.T) {
		var nilSvc *readService
		if _, err := nilSvc.GetTwinHistory(context.Background(), actor, validSampleIntervalQuery); !errors.Is(err, ErrDependencyUnavailable) {
			t.Fatalf("expected ErrDependencyUnavailable on nil receiver, got %v", err)
		}
	})
}

func TestReadService_ReaderNilResultReturnsUnexpected(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	gwURN := "urn:ngsi-ld:Gateway:gw1"
	gwEntity := &ResolvedEntity{
		InternalID: uuid.New(),
		PublicURN:  gwURN,
		Type:       EntityTypeGateway,
		GatewayID:  "gw1",
	}

	tests := []struct {
		name   string
		entity *ResolvedEntity
		call   func(svc ReadService) error
	}{
		{
			name:   "ListTwins returns (nil, nil)",
			entity: gwEntity,
			call: func(svc ReadService) error {
				_, err := svc.ListTwins(context.Background(), actor, TwinListQuery{Limit: 10})
				return err
			},
		},
		{
			name:   "GetTwinDetail returns (nil, nil)",
			entity: gwEntity,
			call: func(svc ReadService) error {
				_, err := svc.GetTwinDetail(context.Background(), actor, TwinDetailQuery{EntityID: gwURN, RelationshipLimit: 10})
				return err
			},
		},
		{
			name:   "GetTwinState returns (nil, nil)",
			entity: gwEntity,
			call: func(svc ReadService) error {
				_, err := svc.GetTwinState(context.Background(), actor, TwinStateQuery{EntityID: gwURN})
				return err
			},
		},
		{
			name: "GetTwinHistory sample_interval_us returns (nil, nil)",
			entity: &ResolvedEntity{
				InternalID: uuid.New(),
				PublicURN:  "urn:ngsi-ld:Sensor:gw1:sensor1",
				Type:       EntityTypeSensor,
				GatewayID:  "gw1",
				SensorID:   "sensor1",
			},
			call: func(svc ReadService) error {
				now := time.Date(2026, 8, 21, 10, 15, 10, 0, time.UTC)
				_, err := svc.GetTwinHistory(context.Background(), actor, TwinHistoryQuery{
					EntityID: "urn:ngsi-ld:Sensor:gw1:sensor1",
					Property: SelectorSampleIntervalUS,
					Range: telemetry.TimeRange{
						From: now.Add(-5 * time.Second),
						To:   now,
					},
					Resolution: telemetry.ResolutionRaw,
					MaxPoints:  1000,
				})
				return err
			},
		},
		{
			name: "GetTwinHistory value delegation returns (nil, nil)",
			entity: &ResolvedEntity{
				InternalID: uuid.New(),
				PublicURN:  "urn:ngsi-ld:Sensor:gw1:sensor1",
				Type:       EntityTypeSensor,
				GatewayID:  "gw1",
				SensorID:   "sensor1",
			},
			call: func(svc ReadService) error {
				now := time.Date(2026, 8, 21, 10, 15, 10, 0, time.UTC)
				_, err := svc.GetTwinHistory(context.Background(), actor, TwinHistoryQuery{
					EntityID: "urn:ngsi-ld:Sensor:gw1:sensor1",
					Property: SelectorValue,
					Range: telemetry.TimeRange{
						From: now.Add(-5 * time.Second),
						To:   now,
					},
					Resolution: telemetry.ResolutionRaw,
					MaxPoints:  1000,
				})
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &fakeEntityResolver{entity: tc.entity}
			// reader returns (nil, nil) for all methods
			reader := &fakeTwinReader{}
			svc, _ := setupTestReadService(t, resolver, reader)

			callErr := tc.call(svc)
			if !errors.Is(callErr, ErrUnexpected) {
				t.Fatalf("expected ErrUnexpected when reader returns (nil, nil), got %v", callErr)
			}
		})
	}
}

func TestReadService_ConfiguredRawWindow_TemporalBranching(t *testing.T) {
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sensorEntity := &ResolvedEntity{
		InternalID: uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		PublicURN:  "urn:ngsi-ld:Sensor:gw1:sensor1",
		Type:       EntityTypeSensor,
		GatewayID:  "gw1",
		SensorID:   "sensor1",
	}
	resolver := &fakeEntityResolver{entity: sensorEntity}

	obsTime := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	val := 10000.0
	rawRec := func(dest ...any) error {
		*dest[0].(*uuid.UUID) = sensorEntity.InternalID
		*dest[1].(*string) = sensorEntity.PublicURN
		*dest[2].(**time.Time) = &obsTime
		*dest[3].(**float64) = &val
		*dest[4].(**string) = nil
		*dest[5].(**bool) = nil
		*dest[6].(*[]byte) = []byte(`{"unit":"us"}`)
		*dest[7].(**uuid.UUID) = nil
		return nil
	}

	bucketTime := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	avg := 10000.0
	minV := 10000.0
	maxV := 10000.0
	countV := int64(1)
	unitStr := "us"
	bucketSummary := &TemporalWindowSummary{
		TotalCount:        1,
		NumberCount:       1,
		DistinctUnitCount: 1,
		MinUnit:           &unitStr,
	}
	bucketRec := makeBucketRecord(*sensorEntity, &bucketTime, &avg, &minV, &maxV, &countV, bucketSummary)

	var lastExecutedQuery string
	db := &fakeTwinDatabase{
		queryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
			lastExecutedQuery = query
			if strings.Contains(query, "bounded_observations AS (") {
				return &fakeTwinRows{records: []func(dest ...any) error{rawRec}}, nil
			}
			if strings.Contains(query, "bucket_aggregates AS (") {
				return &fakeTwinRows{records: []func(dest ...any) error{bucketRec}}, nil
			}
			return &fakeTwinRows{}, nil
		},
	}
	reader, err := NewPostgresTwinReader(db)
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}

	delegator := &fakeTelemetryHistoryDelegator{
		result: &telemetry.TelemetryHistoryResult{
			GatewayID:  "gw1",
			SensorID:   "sensor1",
			From:       time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC),
			To:         time.Date(2026, 8, 21, 10, 15, 5, 0, time.UTC),
			Mode:       "bucket",
			Resolution: "1s",
			PointCount: 1,
		},
	}

	// 1. Service configured with narrow 1s raw window
	prepCfg1s := DefaultReadPreparationConfig()
	prepCfg1s.HistoryRawWindow = 1 * time.Second
	prep1s, err := NewReadPreparation(resolver, prepCfg1s)
	if err != nil {
		t.Fatalf("failed to create narrow prep: %v", err)
	}
	svc1s, err := NewReadService(prep1s, reader, delegator)
	if err != nil {
		t.Fatalf("failed to create narrow service: %v", err)
	}

	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)

	t.Run("configured 1s raw window: 5s auto below point cap selects bucket", func(t *testing.T) {
		lastExecutedQuery = ""
		q5sAuto := TwinHistoryQuery{
			EntityID:   sensorEntity.PublicURN,
			Property:   SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: from, To: from.Add(5 * time.Second)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		res, err := svc1s.GetTwinHistory(context.Background(), actor, q5sAuto)
		if err != nil {
			t.Fatalf("GetTwinHistory failed: %v", err)
		}
		if res.Mode != "bucket" {
			t.Fatalf("expected mode=bucket, got %q", res.Mode)
		}
		if res.Resolution == "raw" {
			t.Fatalf("expected bucket resolution, got %q", res.Resolution)
		}
		if !strings.Contains(lastExecutedQuery, "bucket_aggregates AS (") {
			t.Fatalf("expected bucket aggregation query execution, got query: %s", lastExecutedQuery)
		}
		if strings.Contains(lastExecutedQuery, "bounded_observations AS (") {
			t.Fatalf("raw probe must NOT be executed when window exceeds configured raw window")
		}
	})

	t.Run("configured 1s raw window: 800ms auto fitting cap returns raw", func(t *testing.T) {
		lastExecutedQuery = ""
		q800msAuto := TwinHistoryQuery{
			EntityID:   sensorEntity.PublicURN,
			Property:   SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: from, To: from.Add(800 * time.Millisecond)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		res, err := svc1s.GetTwinHistory(context.Background(), actor, q800msAuto)
		if err != nil {
			t.Fatalf("GetTwinHistory failed: %v", err)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected mode=raw res=raw, got mode=%q res=%q", res.Mode, res.Resolution)
		}
		if !strings.Contains(lastExecutedQuery, "bounded_observations AS (") {
			t.Fatalf("expected raw observation query execution, got query: %s", lastExecutedQuery)
		}
	})

	t.Run("configured 1s raw window: explicit raw >1s fails ErrRawLimitExceeded", func(t *testing.T) {
		q5sRaw := TwinHistoryQuery{
			EntityID:   sensorEntity.PublicURN,
			Property:   SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: from, To: from.Add(5 * time.Second)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		_, err := svc1s.GetTwinHistory(context.Background(), actor, q5sRaw)
		if !errors.Is(err, ErrRawLimitExceeded) {
			t.Fatalf("expected ErrRawLimitExceeded for explicit raw >1s, got %v", err)
		}
	})

	t.Run("configured 1s raw window: explicit raw <=1s returns raw", func(t *testing.T) {
		lastExecutedQuery = ""
		q800msRaw := TwinHistoryQuery{
			EntityID:   sensorEntity.PublicURN,
			Property:   SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: from, To: from.Add(800 * time.Millisecond)},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  100,
		}
		res, err := svc1s.GetTwinHistory(context.Background(), actor, q800msRaw)
		if err != nil {
			t.Fatalf("GetTwinHistory failed: %v", err)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected mode=raw res=raw, got mode=%q res=%q", res.Mode, res.Resolution)
		}
		if !strings.Contains(lastExecutedQuery, "bounded_observations AS (") {
			t.Fatalf("expected raw observation query execution, got query: %s", lastExecutedQuery)
		}
	})

	// 2. Service configured with default 10s raw window
	prepDef, err := NewReadPreparation(resolver, DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("failed to create default prep: %v", err)
	}
	svcDef, err := NewReadService(prepDef, reader, delegator)
	if err != nil {
		t.Fatalf("failed to create default service: %v", err)
	}

	t.Run("default 10s raw window: 5s auto probes and returns raw", func(t *testing.T) {
		lastExecutedQuery = ""
		q5sAutoDef := TwinHistoryQuery{
			EntityID:   sensorEntity.PublicURN,
			Property:   SelectorSampleIntervalUS,
			Range:      telemetry.TimeRange{From: from, To: from.Add(5 * time.Second)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		res, err := svcDef.GetTwinHistory(context.Background(), actor, q5sAutoDef)
		if err != nil {
			t.Fatalf("GetTwinHistory failed: %v", err)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected default 10s to return mode=raw res=raw for 5s auto, got mode=%q res=%q", res.Mode, res.Resolution)
		}
		if !strings.Contains(lastExecutedQuery, "bounded_observations AS (") {
			t.Fatalf("expected raw observation query execution, got query: %s", lastExecutedQuery)
		}
	})

	t.Run("value delegation remains unchanged", func(t *testing.T) {
		qValue := TwinHistoryQuery{
			EntityID:   sensorEntity.PublicURN,
			Property:   SelectorValue,
			Range:      telemetry.TimeRange{From: from, To: from.Add(5 * time.Second)},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  100,
		}
		res, err := svc1s.GetTwinHistory(context.Background(), actor, qValue)
		if err != nil {
			t.Fatalf("GetTwinHistory failed: %v", err)
		}
		if res.Property != SelectorValue {
			t.Fatalf("expected property=value, got %s", res.Property)
		}
		if res.Mode != "bucket" || res.Resolution != "1s" {
			t.Fatalf("expected delegated bucket result, got mode=%q res=%q", res.Mode, res.Resolution)
		}
		if delegator.lastQuery.GatewayID != "gw1" || delegator.lastQuery.SensorID != "sensor1" {
			t.Fatalf("delegator called with wrong gateway/sensor: %+v", delegator.lastQuery)
		}
	})
}
