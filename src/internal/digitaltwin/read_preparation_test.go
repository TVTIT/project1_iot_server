package digitaltwin_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/telemetry"
)

type mockEntityResolver struct {
	calls     int
	lastCtx   context.Context
	lastActor uuid.UUID
	lastURN   string
	lastType  digitaltwin.EntityType
	entity    *digitaltwin.ResolvedEntity
	err       error
	onResolve func()
}

func (m *mockEntityResolver) ResolveEntity(ctx context.Context, actor uuid.UUID, parsed digitaltwin.ParsedIdentity) (*digitaltwin.ResolvedEntity, error) {
	m.calls++
	m.lastCtx = ctx
	m.lastActor = actor
	m.lastURN = parsed.URN
	m.lastType = parsed.Type
	if m.onResolve != nil {
		m.onResolve()
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.entity, nil
}

func validGatewayEntity() *digitaltwin.ResolvedEntity {
	return &digitaltwin.ResolvedEntity{
		InternalID: uuid.MustParse("10000000-0000-0000-0000-000000000001"),
		PublicURN:  "urn:ngsi-ld:Gateway:gateway_001",
		Type:       digitaltwin.EntityTypeGateway,
		GatewayID:  "gateway_001",
	}
}

func validSensorEntity() *digitaltwin.ResolvedEntity {
	return &digitaltwin.ResolvedEntity{
		InternalID: uuid.MustParse("20000000-0000-0000-0000-000000000001"),
		PublicURN:  "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Type:       digitaltwin.EntityTypeSensor,
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
	}
}

func TestNewReadPreparation_Validation(t *testing.T) {
	defaultCfg := digitaltwin.DefaultReadPreparationConfig()

	// 1. Nil resolver rejection
	if _, err := digitaltwin.NewReadPreparation(nil, defaultCfg); err == nil {
		t.Fatal("expected error on nil resolver")
	}

	// 2. Typed nil resolver rejection
	var typedNil *mockEntityResolver
	if _, err := digitaltwin.NewReadPreparation(typedNil, defaultCfg); err == nil {
		t.Fatal("expected error on typed nil resolver")
	}

	// 3. Valid resolver + valid config
	res := &mockEntityResolver{}
	prep, err := digitaltwin.NewReadPreparation(res, defaultCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if prep == nil {
		t.Fatal("expected non-nil ReadPreparation")
	}

	// 4. Config bounds validation table
	tests := []struct {
		name        string
		cfg         digitaltwin.ReadPreparationConfig
		errContains string
	}{
		{
			name: "zero query timeout",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.QueryTimeout = 0
				return c
			}(),
			errContains: "READ_QUERY_TIMEOUT must be a positive duration",
		},
		{
			name: "query timeout exceeds hard cap of 5s",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.QueryTimeout = 6 * time.Second
				return c
			}(),
			errContains: "READ_QUERY_TIMEOUT cannot exceed contract hard cap of 5s",
		},
		{
			name: "list max limit zero",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.ListMaxLimit = 0
				return c
			}(),
			errContains: "TWIN_LIST_MAX_LIMIT cannot exceed contract hard cap of 200",
		},
		{
			name: "list max limit exceeds 200",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.ListMaxLimit = 201
				return c
			}(),
			errContains: "TWIN_LIST_MAX_LIMIT cannot exceed contract hard cap of 200",
		},
		{
			name: "relationship max limit exceeds 200",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.RelationshipMaxLimit = 201
				return c
			}(),
			errContains: "TWIN_RELATIONSHIP_MAX_LIMIT cannot exceed contract hard cap of 200",
		},
		{
			name: "history max range zero",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.HistoryMaxRange = 0
				return c
			}(),
			errContains: "HISTORY_MAX_RANGE must be a positive duration",
		},
		{
			name: "history max range exceeds 744h",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.HistoryMaxRange = 745 * time.Hour
				return c
			}(),
			errContains: "HISTORY_MAX_RANGE cannot exceed contract hard cap of 744h0m0s",
		},
		{
			name: "history raw window exceeds 10s",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.HistoryRawWindow = 11 * time.Second
				return c
			}(),
			errContains: "HISTORY_RAW_WINDOW cannot exceed contract hard cap of 10s",
		},
		{
			name: "min points below 100",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.HistoryMinPoints = 99
				return c
			}(),
			errContains: "HISTORY_MIN_POINTS cannot be less than contract lower limit of 100",
		},
		{
			name: "max points exceeds 2000",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.HistoryMaxPoints = 2001
				return c
			}(),
			errContains: "HISTORY_MAX_POINTS cannot exceed contract hard cap of 2000",
		},
		{
			name: "min points greater than max points",
			cfg: func() digitaltwin.ReadPreparationConfig {
				c := defaultCfg
				c.HistoryMinPoints = 1500
				c.HistoryMaxPoints = 1000
				return c
			}(),
			errContains: "HISTORY_MIN_POINTS must be less than or equal to HISTORY_MAX_POINTS",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := digitaltwin.NewReadPreparation(res, tc.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("error %q does not contain expected substring %q", err.Error(), tc.errContains)
			}
		})
	}
}

func TestPrepareList_ValidScopeAndNoResourcePrecheck(t *testing.T) {
	res := &mockEntityResolver{}
	prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	gwID := "gateway_001"
	q := digitaltwin.TwinListQuery{
		GatewayID: &gwID,
		Limit:     50,
	}

	var callbackCalled bool
	var callbackQuery digitaltwin.TwinListQuery

	err = prep.PrepareList(context.Background(), actor, q, func(_ context.Context, query digitaltwin.TwinListQuery) error {
		callbackCalled = true
		callbackQuery = query
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !callbackCalled {
		t.Fatal("expected callback to be called")
	}
	// CRITICAL: List operations MUST NOT perform a resource precheck
	if res.calls != 0 {
		t.Fatalf("expected 0 resolver calls for list preparation, got %d", res.calls)
	}
	if callbackQuery.Limit != 50 || *callbackQuery.GatewayID != "gateway_001" {
		t.Fatalf("unexpected query in callback: %+v", callbackQuery)
	}
}

func TestPrepareDetail_ValidScope(t *testing.T) {
	entity := validGatewayEntity()
	res := &mockEntityResolver{entity: entity}
	prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	q := digitaltwin.TwinDetailQuery{
		EntityID:          "urn:ngsi-ld:Gateway:gateway_001",
		RelationshipLimit: 50,
	}

	var callbackCalled bool
	var callbackEntity digitaltwin.ResolvedEntity
	var callbackQuery digitaltwin.TwinDetailQuery

	err = prep.PrepareDetail(context.Background(), actor, q, func(_ context.Context, ent digitaltwin.ResolvedEntity, query digitaltwin.TwinDetailQuery) error {
		callbackCalled = true
		callbackEntity = ent
		callbackQuery = query
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !callbackCalled {
		t.Fatal("expected callback to be called")
	}
	if res.calls != 1 {
		t.Fatalf("expected 1 resolver call, got %d", res.calls)
	}
	if callbackEntity.PublicURN != "urn:ngsi-ld:Gateway:gateway_001" {
		t.Fatalf("entity URN = %s, want urn:ngsi-ld:Gateway:gateway_001", callbackEntity.PublicURN)
	}
	if callbackQuery.RelationshipLimit != 50 {
		t.Fatalf("relationship limit = %d, want 50", callbackQuery.RelationshipLimit)
	}
}

func TestPrepareState_ValidScope(t *testing.T) {
	entity := validSensorEntity()
	res := &mockEntityResolver{entity: entity}
	prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	q := digitaltwin.TwinStateQuery{
		EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
	}

	var callbackCalled bool
	var callbackEntity digitaltwin.ResolvedEntity

	err = prep.PrepareState(context.Background(), actor, q, func(_ context.Context, ent digitaltwin.ResolvedEntity, _ digitaltwin.TwinStateQuery) error {
		callbackCalled = true
		callbackEntity = ent
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !callbackCalled {
		t.Fatal("expected callback to be called")
	}
	if res.calls != 1 {
		t.Fatalf("expected 1 resolver call, got %d", res.calls)
	}
	if callbackEntity.PublicURN != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Fatalf("entity URN = %s, want urn:ngsi-ld:Sensor:gateway_001:sensor_001", callbackEntity.PublicURN)
	}
}

func TestPrepareHistory_ValidScope(t *testing.T) {
	entity := validSensorEntity()
	res := &mockEntityResolver{entity: entity}
	prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	q := digitaltwin.TwinHistoryQuery{
		EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:   digitaltwin.SelectorValue,
		Range:      telemetry.TimeRange{From: now.Add(-10 * time.Minute), To: now},
		Resolution: telemetry.Resolution1s,
		MaxPoints:  1000,
	}

	var callbackCalled bool
	var callbackEntity digitaltwin.ResolvedEntity
	var callbackQuery digitaltwin.TwinHistoryQuery

	err = prep.PrepareHistory(context.Background(), actor, q, func(_ context.Context, ent digitaltwin.ResolvedEntity, query digitaltwin.TwinHistoryQuery) error {
		callbackCalled = true
		callbackEntity = ent
		callbackQuery = query
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !callbackCalled {
		t.Fatal("expected callback to be called")
	}
	if res.calls != 1 {
		t.Fatalf("expected 1 resolver call, got %d", res.calls)
	}
	if callbackEntity.PublicURN != "urn:ngsi-ld:Sensor:gateway_001:sensor_001" {
		t.Fatalf("entity URN = %s, want urn:ngsi-ld:Sensor:gateway_001:sensor_001", callbackEntity.PublicURN)
	}
	if callbackQuery.Property != digitaltwin.SelectorValue {
		t.Fatalf("property = %s, want value", callbackQuery.Property)
	}
}

func TestPrepareTwin_InvalidNoCalls(t *testing.T) {
	res := &mockEntityResolver{entity: validSensorEntity()}
	prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	validActor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	t.Run("PrepareList invalid inputs", func(t *testing.T) {
		tests := []struct {
			name      string
			actor     uuid.UUID
			q         digitaltwin.TwinListQuery
			nilFn     bool
			wantErrIs error
		}{
			{name: "nil actor", actor: uuid.Nil, q: digitaltwin.TwinListQuery{Limit: 50}, wantErrIs: digitaltwin.ErrNilActor},
			{name: "nil callback", actor: validActor, q: digitaltwin.TwinListQuery{Limit: 50}, nilFn: true, wantErrIs: digitaltwin.ErrInvalidInput},
			{name: "limit zero", actor: validActor, q: digitaltwin.TwinListQuery{Limit: 0}, wantErrIs: digitaltwin.ErrInvalidInput},
			{name: "limit exceeds 200", actor: validActor, q: digitaltwin.TwinListQuery{Limit: 201}, wantErrIs: digitaltwin.ErrInvalidInput},
			{
				name:  "invalid gateway id",
				actor: validActor,
				q: func() digitaltwin.TwinListQuery {
					badGW := "invalid!gw"
					return digitaltwin.TwinListQuery{Limit: 50, GatewayID: &badGW}
				}(),
				wantErrIs: digitaltwin.ErrInvalidInput,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res.calls = 0
				var callbackCalls int
				var fn digitaltwin.ListCallback = func(_ context.Context, _ digitaltwin.TwinListQuery) error {
					callbackCalls++
					return nil
				}
				if tc.nilFn {
					fn = nil
				}
				err := prep.PrepareList(context.Background(), tc.actor, tc.q, fn)
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("error %v, want errors.Is %v", err, tc.wantErrIs)
				}
				if res.calls != 0 {
					t.Fatalf("expected 0 resolver calls, got %d", res.calls)
				}
				if callbackCalls != 0 {
					t.Fatalf("expected 0 callback calls, got %d", callbackCalls)
				}
			})
		}
	})

	t.Run("PrepareDetail invalid inputs", func(t *testing.T) {
		tests := []struct {
			name      string
			actor     uuid.UUID
			q         digitaltwin.TwinDetailQuery
			nilFn     bool
			wantErrIs error
		}{
			{name: "nil actor", actor: uuid.Nil, q: digitaltwin.TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1", RelationshipLimit: 50}, wantErrIs: digitaltwin.ErrNilActor},
			{name: "nil callback", actor: validActor, q: digitaltwin.TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1", RelationshipLimit: 50}, nilFn: true, wantErrIs: digitaltwin.ErrInvalidInput},
			{name: "invalid entity URN", actor: validActor, q: digitaltwin.TwinDetailQuery{EntityID: "not-a-urn", RelationshipLimit: 50}, wantErrIs: digitaltwin.ErrInvalidInput},
			{name: "relationship limit zero", actor: validActor, q: digitaltwin.TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1", RelationshipLimit: 0}, wantErrIs: digitaltwin.ErrInvalidInput},
			{name: "relationship limit exceeds 200", actor: validActor, q: digitaltwin.TwinDetailQuery{EntityID: "urn:ngsi-ld:Gateway:gw1", RelationshipLimit: 201}, wantErrIs: digitaltwin.ErrInvalidInput},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res.calls = 0
				var callbackCalls int
				var fn digitaltwin.DetailCallback = func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinDetailQuery) error {
					callbackCalls++
					return nil
				}
				if tc.nilFn {
					fn = nil
				}
				err := prep.PrepareDetail(context.Background(), tc.actor, tc.q, fn)
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("error %v, want errors.Is %v", err, tc.wantErrIs)
				}
				if res.calls != 0 {
					t.Fatalf("expected 0 resolver calls, got %d", res.calls)
				}
				if callbackCalls != 0 {
					t.Fatalf("expected 0 callback calls, got %d", callbackCalls)
				}
			})
		}
	})

	t.Run("PrepareState invalid inputs", func(t *testing.T) {
		tests := []struct {
			name      string
			actor     uuid.UUID
			q         digitaltwin.TwinStateQuery
			nilFn     bool
			wantErrIs error
		}{
			{name: "nil actor", actor: uuid.Nil, q: digitaltwin.TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}, wantErrIs: digitaltwin.ErrNilActor},
			{name: "nil callback", actor: validActor, q: digitaltwin.TwinStateQuery{EntityID: "urn:ngsi-ld:Gateway:gw1"}, nilFn: true, wantErrIs: digitaltwin.ErrInvalidInput},
			{name: "invalid entity URN", actor: validActor, q: digitaltwin.TwinStateQuery{EntityID: "urn:invalid:urn"}, wantErrIs: digitaltwin.ErrInvalidInput},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res.calls = 0
				var callbackCalls int
				var fn digitaltwin.StateCallback = func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinStateQuery) error {
					callbackCalls++
					return nil
				}
				if tc.nilFn {
					fn = nil
				}
				err := prep.PrepareState(context.Background(), tc.actor, tc.q, fn)
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("error %v, want errors.Is %v", err, tc.wantErrIs)
				}
				if res.calls != 0 {
					t.Fatalf("expected 0 resolver calls, got %d", res.calls)
				}
				if callbackCalls != 0 {
					t.Fatalf("expected 0 callback calls, got %d", callbackCalls)
				}
			})
		}
	})

	t.Run("PrepareHistory invalid inputs", func(t *testing.T) {
		validHQ := digitaltwin.TwinHistoryQuery{
			EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			Property:   digitaltwin.SelectorValue,
			Range:      telemetry.TimeRange{From: now.Add(-10 * time.Minute), To: now},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  1000,
		}

		tests := []struct {
			name      string
			actor     uuid.UUID
			q         digitaltwin.TwinHistoryQuery
			nilFn     bool
			wantErrIs error
		}{
			{name: "nil actor", actor: uuid.Nil, q: validHQ, wantErrIs: digitaltwin.ErrNilActor},
			{name: "nil callback", actor: validActor, q: validHQ, nilFn: true, wantErrIs: digitaltwin.ErrInvalidInput},
			{
				name:  "gateway entity type rejected for history (must be sensor)",
				actor: validActor,
				q: func() digitaltwin.TwinHistoryQuery {
					q := validHQ
					q.EntityID = "urn:ngsi-ld:Gateway:gateway_001"
					return q
				}(),
				wantErrIs: digitaltwin.ErrInvalidProperty,
			},
			{
				name:  "invalid property selector",
				actor: validActor,
				q: func() digitaltwin.TwinHistoryQuery {
					q := validHQ
					q.Property = "invalid_property"
					return q
				}(),
				wantErrIs: digitaltwin.ErrInvalidProperty,
			},
			{
				name:  "max points below 100",
				actor: validActor,
				q: func() digitaltwin.TwinHistoryQuery {
					q := validHQ
					q.MaxPoints = 50
					return q
				}(),
				wantErrIs: digitaltwin.ErrInvalidInput,
			},
			{
				name:  "max points above 2000",
				actor: validActor,
				q: func() digitaltwin.TwinHistoryQuery {
					q := validHQ
					q.MaxPoints = 2500
					return q
				}(),
				wantErrIs: digitaltwin.ErrInvalidInput,
			},
			{
				name:  "range exceeds 744h",
				actor: validActor,
				q: func() digitaltwin.TwinHistoryQuery {
					q := validHQ
					q.Range = telemetry.TimeRange{From: now.Add(-745 * time.Hour), To: now}
					return q
				}(),
				wantErrIs: digitaltwin.ErrRangeTooLarge,
			},
			{
				name:  "raw resolution window exceeds 10s",
				actor: validActor,
				q: func() digitaltwin.TwinHistoryQuery {
					q := validHQ
					q.Resolution = telemetry.ResolutionRaw
					q.Range = telemetry.TimeRange{From: now.Add(-11 * time.Second), To: now}
					return q
				}(),
				wantErrIs: digitaltwin.ErrRawLimitExceeded,
			},
			{
				name:  "resolution too fine for window",
				actor: validActor,
				q: func() digitaltwin.TwinHistoryQuery {
					q := validHQ
					q.Resolution = telemetry.Resolution100ms
					q.MaxPoints = 100
					q.Range = telemetry.TimeRange{From: now.Add(-time.Hour), To: now}
					return q
				}(),
				wantErrIs: digitaltwin.ErrResolutionTooFine,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				res.calls = 0
				var callbackCalls int
				var fn digitaltwin.HistoryCallback = func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinHistoryQuery) error {
					callbackCalls++
					return nil
				}
				if tc.nilFn {
					fn = nil
				}
				err := prep.PrepareHistory(context.Background(), tc.actor, tc.q, fn)
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("error %v, want errors.Is %v", err, tc.wantErrIs)
				}
				if res.calls != 0 {
					t.Fatalf("expected 0 resolver calls, got %d", res.calls)
				}
				if callbackCalls != 0 {
					t.Fatalf("expected 0 callback calls, got %d", callbackCalls)
				}
			})
		}
	})
}

func TestPrepareDetail_ResolverErrorMappingAndDeniedAuth(t *testing.T) {
	tests := []struct {
		name         string
		resolverErr  error
		entity       *digitaltwin.ResolvedEntity
		wantErrIs    error
		wantCallback bool
	}{
		{
			name:         "owner authorized entity resolved",
			entity:       validGatewayEntity(),
			wantErrIs:    nil,
			wantCallback: true,
		},
		{
			name:         "operator authorized entity resolved",
			entity:       validGatewayEntity(),
			wantErrIs:    nil,
			wantCallback: true,
		},
		{
			name:         "viewer authorized entity resolved",
			entity:       validGatewayEntity(),
			wantErrIs:    nil,
			wantCallback: true,
		},
		{
			name:         "missing or non-member returns not found unchanged",
			resolverErr:  digitaltwin.ErrNotFound,
			wantErrIs:    digitaltwin.ErrNotFound,
			wantCallback: false,
		},
		{
			name:         "dependency unavailable returned unchanged",
			resolverErr:  digitaltwin.ErrDependencyUnavailable,
			wantErrIs:    digitaltwin.ErrDependencyUnavailable,
			wantCallback: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := &mockEntityResolver{
				entity: tc.entity,
				err:    tc.resolverErr,
			}
			prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
			var callbackCalled bool
			err = prep.PrepareDetail(context.Background(), actor, digitaltwin.TwinDetailQuery{
				EntityID:          "urn:ngsi-ld:Gateway:gateway_001",
				RelationshipLimit: 50,
			}, func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinDetailQuery) error {
				callbackCalled = true
				return nil
			})

			if tc.wantErrIs == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("got error %v, want errors.Is %v", err, tc.wantErrIs)
				}
			}

			if callbackCalled != tc.wantCallback {
				t.Fatalf("callback called = %v, want %v", callbackCalled, tc.wantCallback)
			}
		})
	}
}

func TestPrepareTwin_SameContextAcrossAuthAndCallback(t *testing.T) {
	res := &mockEntityResolver{entity: validGatewayEntity()}
	prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	var callbackCtx context.Context

	err = prep.PrepareDetail(context.Background(), actor, digitaltwin.TwinDetailQuery{
		EntityID:          "urn:ngsi-ld:Gateway:gateway_001",
		RelationshipLimit: 50,
	}, func(ctx context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinDetailQuery) error {
		callbackCtx = ctx
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.lastCtx == nil {
		t.Fatal("resolver context was nil")
	}
	if callbackCtx == nil {
		t.Fatal("callback context was nil")
	}
	if res.lastCtx != callbackCtx {
		t.Fatal("expected exact same context passed to resolver and callback")
	}
}

func TestPrepareTwin_ParentDeadlineRetained(t *testing.T) {
	res := &mockEntityResolver{entity: validGatewayEntity()}
	prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parentDeadline := time.Now().Add(500 * time.Millisecond)
	parentCtx, cancel := context.WithDeadline(context.Background(), parentDeadline)
	defer cancel()

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	var observedDeadline time.Time

	err = prep.PrepareDetail(parentCtx, actor, digitaltwin.TwinDetailQuery{
		EntityID:          "urn:ngsi-ld:Gateway:gateway_001",
		RelationshipLimit: 50,
	}, func(ctx context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinDetailQuery) error {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected deadline on context")
		}
		observedDeadline = dl
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !observedDeadline.Equal(parentDeadline) {
		t.Fatalf("observed deadline %v, want parent deadline %v", observedDeadline, parentDeadline)
	}
}

func TestPrepareTwin_CanceledOrExpiredNilSuccessRejected(t *testing.T) {
	res := &mockEntityResolver{entity: validGatewayEntity()}
	prep, err := digitaltwin.NewReadPreparation(res, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	// 1. Context canceled during callback, but callback returns nil (fake returns nil-success)
	parentCtx, cancel := context.WithCancel(context.Background())
	err = prep.PrepareDetail(parentCtx, actor, digitaltwin.TwinDetailQuery{
		EntityID:          "urn:ngsi-ld:Gateway:gateway_001",
		RelationshipLimit: 50,
	}, func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinDetailQuery) error {
		cancel() // cancel during callback
		return nil
	})

	if err == nil {
		t.Fatal("expected error when context canceled during callback, got nil")
	}
	if !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatalf("error %v, want errors.Is ErrDependencyUnavailable", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want errors.Is context.Canceled", err)
	}

	// 2. Already canceled context at entry
	canceledCtx, cancel2 := context.WithCancel(context.Background())
	cancel2()

	err = prep.PrepareDetail(canceledCtx, actor, digitaltwin.TwinDetailQuery{
		EntityID:          "urn:ngsi-ld:Gateway:gateway_001",
		RelationshipLimit: 50,
	}, func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinDetailQuery) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error on already canceled context, got nil")
	}
	if !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatalf("error %v, want errors.Is ErrDependencyUnavailable", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want errors.Is context.Canceled", err)
	}

	// 3. PrepareDetail: context canceled during ResolveEntity, resolver returns nil error (fake returns nil-success)
	// Callback must NOT execute under dead context.
	parentCtx3, cancel3 := context.WithCancel(context.Background())
	callbackCalled3 := false
	res3 := &mockEntityResolver{
		onResolve: func() {
			cancel3()
		},
		entity: validGatewayEntity(),
	}
	prep3, err := digitaltwin.NewReadPreparation(res3, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = prep3.PrepareDetail(parentCtx3, actor, digitaltwin.TwinDetailQuery{
		EntityID:          "urn:ngsi-ld:Gateway:gateway_001",
		RelationshipLimit: 50,
	}, func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinDetailQuery) error {
		callbackCalled3 = true
		return nil
	})
	if err == nil {
		t.Fatal("expected error when context canceled during resolve, got nil")
	}
	if callbackCalled3 {
		t.Fatal("PrepareDetail callback must NOT execute under dead context")
	}
	if !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatalf("error %v, want errors.Is ErrDependencyUnavailable", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want errors.Is context.Canceled", err)
	}

	// 4. PrepareState: context canceled during ResolveEntity, resolver returns nil error
	parentCtx4, cancel4 := context.WithCancel(context.Background())
	callbackCalled4 := false
	res4 := &mockEntityResolver{
		onResolve: func() {
			cancel4()
		},
		entity: validGatewayEntity(),
	}
	prep4, err := digitaltwin.NewReadPreparation(res4, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = prep4.PrepareState(parentCtx4, actor, digitaltwin.TwinStateQuery{
		EntityID: "urn:ngsi-ld:Gateway:gateway_001",
	}, func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinStateQuery) error {
		callbackCalled4 = true
		return nil
	})
	if err == nil {
		t.Fatal("expected error when context canceled during resolve, got nil")
	}
	if callbackCalled4 {
		t.Fatal("PrepareState callback must NOT execute under dead context")
	}
	if !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatalf("error %v, want errors.Is ErrDependencyUnavailable", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want errors.Is context.Canceled", err)
	}

	// 5. PrepareHistory: context canceled during ResolveEntity, resolver returns nil error
	parentCtx5, cancel5 := context.WithCancel(context.Background())
	callbackCalled5 := false
	res5 := &mockEntityResolver{
		onResolve: func() {
			cancel5()
		},
		entity: validSensorEntity(),
	}
	prep5, err := digitaltwin.NewReadPreparation(res5, digitaltwin.DefaultReadPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	err = prep5.PrepareHistory(parentCtx5, actor, digitaltwin.TwinHistoryQuery{
		EntityID:   "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Property:   digitaltwin.SelectorValue,
		Range:      telemetry.TimeRange{From: now.Add(-10 * time.Minute), To: now},
		Resolution: telemetry.Resolution1s,
		MaxPoints:  1000,
	}, func(_ context.Context, _ digitaltwin.ResolvedEntity, _ digitaltwin.TwinHistoryQuery) error {
		callbackCalled5 = true
		return nil
	})
	if err == nil {
		t.Fatal("expected error when context canceled during resolve, got nil")
	}
	if callbackCalled5 {
		t.Fatal("PrepareHistory callback must NOT execute under dead context")
	}
	if !errors.Is(err, digitaltwin.ErrDependencyUnavailable) {
		t.Fatalf("error %v, want errors.Is ErrDependencyUnavailable", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want errors.Is context.Canceled", err)
	}
}
