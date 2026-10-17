package digitaltwin

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
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

	lastActor        uuid.UUID
	lastListQuery    TwinListQuery
	lastDetailQuery  TwinDetailQuery
	lastDetailEntity ResolvedEntity
	lastStateEntity  ResolvedEntity
	onList           func(ctx context.Context)
	onDetail         func(ctx context.Context)
	onState          func(ctx context.Context)
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

func (f *fakeTwinReader) ReadTemporalHistory(_ context.Context, _ uuid.UUID, _ ResolvedEntity, _ TwinHistoryQuery) (*TwinHistoryResult, error) {
	if f.historyErr != nil {
		return f.historyResult, f.historyErr
	}
	return f.historyResult, nil
}

var _ TwinReader = (*fakeTwinReader)(nil)
var _ EntityResolver = (*fakeEntityResolver)(nil)

func setupTestReadService(t *testing.T, resolver EntityResolver, reader TwinReader) (ReadService, *ReadPreparation) {
	t.Helper()
	cfg := DefaultReadPreparationConfig()
	prep, err := NewReadPreparation(resolver, cfg)
	if err != nil {
		t.Fatalf("failed to create ReadPreparation: %v", err)
	}
	svc, err := NewReadService(prep, reader)
	if err != nil {
		t.Fatalf("failed to create ReadService: %v", err)
	}
	return svc, prep
}

func TestNewReadService_Dependencies(t *testing.T) {
	cfg := DefaultReadPreparationConfig()
	prep, err := NewReadPreparation(&fakeEntityResolver{}, cfg)
	if err != nil {
		t.Fatalf("unexpected error creating prep: %v", err)
	}
	reader := &fakeTwinReader{}

	// 1. Nil preparation
	if _, err := NewReadService(nil, reader); err == nil {
		t.Fatal("expected error on nil preparation, got nil")
	}

	// 2. Typed-nil preparation
	var typedNilPrep *ReadPreparation
	if _, err := NewReadService(typedNilPrep, reader); err == nil {
		t.Fatal("expected error on typed-nil preparation, got nil")
	}

	// 3. Nil reader
	if _, err := NewReadService(prep, nil); err == nil {
		t.Fatal("expected error on nil reader, got nil")
	}

	// 4. Typed-nil reader
	var typedNilReader *fakeTwinReader
	if _, err := NewReadService(prep, typedNilReader); err == nil {
		t.Fatal("expected error on typed-nil reader, got nil")
	}

	// 5. Valid dependencies
	svc, err := NewReadService(prep, reader)
	if err != nil {
		t.Fatalf("unexpected error with valid dependencies: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil service, got nil")
	}
	var _ ReadService = svc
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
	svc, _ := setupTestReadService(t, &fakeEntityResolver{}, &fakeTwinReader{})

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	q := TwinHistoryQuery{
		EntityID: "urn:ngsi-ld:Sensor:gw1:sensor1",
		Property: SelectorValue,
	}

	res, err := svc.GetTwinHistory(context.Background(), actor, q)
	if !errors.Is(err, ErrUnexpected) {
		t.Fatalf("expected ErrUnexpected in Task 4.3 for GetTwinHistory, got %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result, got %v", res)
	}
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
		name string
		call func(svc ReadService) error
	}{
		{
			name: "ListTwins returns (nil, nil)",
			call: func(svc ReadService) error {
				_, err := svc.ListTwins(context.Background(), actor, TwinListQuery{Limit: 10})
				return err
			},
		},
		{
			name: "GetTwinDetail returns (nil, nil)",
			call: func(svc ReadService) error {
				_, err := svc.GetTwinDetail(context.Background(), actor, TwinDetailQuery{EntityID: gwURN, RelationshipLimit: 10})
				return err
			},
		},
		{
			name: "GetTwinState returns (nil, nil)",
			call: func(svc ReadService) error {
				_, err := svc.GetTwinState(context.Background(), actor, TwinStateQuery{EntityID: gwURN})
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &fakeEntityResolver{entity: gwEntity}
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
