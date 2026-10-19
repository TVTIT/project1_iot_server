package telemetry_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

type mockSensorResolver struct {
	calls      int
	lastCtx    context.Context
	lastActor  uuid.UUID
	lastGW     string
	lastSensor string
	sensor     *telemetry.ResolvedSensor
	err        error
	onResolve  func()
}

func (m *mockSensorResolver) ResolveSensor(ctx context.Context, actor uuid.UUID, gatewayID, sensorID string) (*telemetry.ResolvedSensor, error) {
	m.calls++
	m.lastCtx = ctx
	m.lastActor = actor
	m.lastGW = gatewayID
	m.lastSensor = sensorID
	if m.onResolve != nil {
		m.onResolve()
	}
	if m.err != nil {
		return nil, m.err
	}
	return m.sensor, nil
}

func validHistoryQuery() telemetry.TelemetryHistoryQuery {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	return telemetry.TelemetryHistoryQuery{
		GatewayID:  "gateway_001",
		SensorID:   "sensor_001",
		Range:      telemetry.TimeRange{From: now.Add(-10 * time.Minute), To: now},
		Resolution: telemetry.Resolution1s,
		MaxPoints:  1000,
	}
}

func TestNewHistoryPreparation_Validation(t *testing.T) {
	defaultCfg := telemetry.DefaultHistoryPreparationConfig()

	// 1. Nil resolver rejection
	if _, err := telemetry.NewHistoryPreparation(nil, defaultCfg); err == nil {
		t.Fatal("expected error for nil resolver")
	}

	// 2. Typed nil resolver rejection
	var typedNil *mockSensorResolver
	if _, err := telemetry.NewHistoryPreparation(typedNil, defaultCfg); err == nil {
		t.Fatal("expected error for typed nil resolver")
	}

	// 3. Valid resolver + valid config
	res := &mockSensorResolver{}
	prep, err := telemetry.NewHistoryPreparation(res, defaultCfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if prep == nil {
		t.Fatal("expected non-nil preparation orchestrator")
	}

	// 4. Config bounds validation table
	tests := []struct {
		name        string
		cfg         telemetry.HistoryPreparationConfig
		errContains string
	}{
		{
			name: "zero query timeout",
			cfg: telemetry.HistoryPreparationConfig{
				QueryTimeout: 0,
				MaxRange:     defaultCfg.MaxRange,
				RawWindow:    defaultCfg.RawWindow,
				MinPoints:    defaultCfg.MinPoints,
				MaxPoints:    defaultCfg.MaxPoints,
			},
			errContains: "HISTORY_QUERY_TIMEOUT must be a positive duration",
		},
		{
			name: "query timeout exceeds hard cap of 5s",
			cfg: telemetry.HistoryPreparationConfig{
				QueryTimeout: 6 * time.Second,
				MaxRange:     defaultCfg.MaxRange,
				RawWindow:    defaultCfg.RawWindow,
				MinPoints:    defaultCfg.MinPoints,
				MaxPoints:    defaultCfg.MaxPoints,
			},
			errContains: "HISTORY_QUERY_TIMEOUT cannot exceed contract hard cap of 5s",
		},
		{
			name: "zero max range",
			cfg: telemetry.HistoryPreparationConfig{
				QueryTimeout: defaultCfg.QueryTimeout,
				MaxRange:     0,
				RawWindow:    defaultCfg.RawWindow,
				MinPoints:    defaultCfg.MinPoints,
				MaxPoints:    defaultCfg.MaxPoints,
			},
			errContains: "HISTORY_MAX_RANGE must be a positive duration",
		},
		{
			name: "max range exceeds hard cap 744h",
			cfg: telemetry.HistoryPreparationConfig{
				QueryTimeout: defaultCfg.QueryTimeout,
				MaxRange:     745 * time.Hour,
				RawWindow:    defaultCfg.RawWindow,
				MinPoints:    defaultCfg.MinPoints,
				MaxPoints:    defaultCfg.MaxPoints,
			},
			errContains: "HISTORY_MAX_RANGE cannot exceed contract hard cap of 744h0m0s",
		},
		{
			name: "raw window exceeds hard cap 10s",
			cfg: telemetry.HistoryPreparationConfig{
				QueryTimeout: defaultCfg.QueryTimeout,
				MaxRange:     defaultCfg.MaxRange,
				RawWindow:    11 * time.Second,
				MinPoints:    defaultCfg.MinPoints,
				MaxPoints:    defaultCfg.MaxPoints,
			},
			errContains: "HISTORY_RAW_WINDOW cannot exceed contract hard cap of 10s",
		},
		{
			name: "min points below 100",
			cfg: telemetry.HistoryPreparationConfig{
				QueryTimeout: defaultCfg.QueryTimeout,
				MaxRange:     defaultCfg.MaxRange,
				RawWindow:    defaultCfg.RawWindow,
				MinPoints:    99,
				MaxPoints:    defaultCfg.MaxPoints,
			},
			errContains: "HISTORY_MIN_POINTS cannot be less than contract lower limit of 100",
		},
		{
			name: "max points above 2000",
			cfg: telemetry.HistoryPreparationConfig{
				QueryTimeout: defaultCfg.QueryTimeout,
				MaxRange:     defaultCfg.MaxRange,
				RawWindow:    defaultCfg.RawWindow,
				MinPoints:    defaultCfg.MinPoints,
				MaxPoints:    2001,
			},
			errContains: "HISTORY_MAX_POINTS cannot exceed contract hard cap of 2000",
		},
		{
			name: "min points greater than max points",
			cfg: telemetry.HistoryPreparationConfig{
				QueryTimeout: defaultCfg.QueryTimeout,
				MaxRange:     defaultCfg.MaxRange,
				RawWindow:    defaultCfg.RawWindow,
				MinPoints:    1500,
				MaxPoints:    1000,
			},
			errContains: "HISTORY_MIN_POINTS must be less than or equal to HISTORY_MAX_POINTS",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := telemetry.NewHistoryPreparation(res, tc.cfg)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("error %q does not contain expected substring %q", err.Error(), tc.errContains)
			}
		})
	}
}

func TestPrepareTelemetryHistory_ValidScope(t *testing.T) {
	unit := "degC"
	res := &mockSensorResolver{
		sensor: &telemetry.ResolvedSensor{
			GatewayID: "gateway_001",
			SensorID:  "sensor_001",
			Unit:      &unit,
		},
	}
	prep, err := telemetry.NewHistoryPreparation(res, telemetry.DefaultHistoryPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	q := validHistoryQuery()

	var callbackCalled bool
	var callbackSensor telemetry.ResolvedSensor
	var callbackQuery telemetry.TelemetryHistoryQuery

	err = prep.PrepareTelemetryHistory(context.Background(), actor, q, func(_ context.Context, sensor telemetry.ResolvedSensor, query telemetry.TelemetryHistoryQuery) error {
		callbackCalled = true
		callbackSensor = sensor
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
	if res.lastActor != actor {
		t.Fatalf("resolver actor = %s, want %s", res.lastActor, actor)
	}
	if callbackSensor.GatewayID != "gateway_001" || callbackSensor.SensorID != "sensor_001" {
		t.Fatalf("sensor mismatch: %+v", callbackSensor)
	}
	if callbackQuery.MaxPoints != 1000 {
		t.Fatalf("query max points = %d, want 1000", callbackQuery.MaxPoints)
	}
}

func TestPrepareTelemetryHistory_InvalidNoCalls(t *testing.T) {
	res := &mockSensorResolver{
		sensor: &telemetry.ResolvedSensor{GatewayID: "gateway_001", SensorID: "sensor_001"},
	}
	prep, err := telemetry.NewHistoryPreparation(res, telemetry.DefaultHistoryPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	validActor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	validQ := validHistoryQuery()
	now := validQ.Range.To

	tests := []struct {
		name      string
		actor     uuid.UUID
		q         telemetry.TelemetryHistoryQuery
		nilFn     bool
		wantErrIs error
	}{
		{
			name:      "nil actor",
			actor:     uuid.Nil,
			q:         validQ,
			wantErrIs: telemetry.ErrHistoryNilActor,
		},
		{
			name:      "nil callback",
			actor:     validActor,
			q:         validQ,
			nilFn:     true,
			wantErrIs: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "invalid gateway id",
			actor:     validActor,
			q:         func() telemetry.TelemetryHistoryQuery { q := validQ; q.GatewayID = "invalid!gw"; return q }(),
			wantErrIs: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "invalid sensor id",
			actor:     validActor,
			q:         func() telemetry.TelemetryHistoryQuery { q := validQ; q.SensorID = "invalid@sensor"; return q }(),
			wantErrIs: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "max points below minimum 100",
			actor:     validActor,
			q:         func() telemetry.TelemetryHistoryQuery { q := validQ; q.MaxPoints = 50; return q }(),
			wantErrIs: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "max points above maximum 2000",
			actor:     validActor,
			q:         func() telemetry.TelemetryHistoryQuery { q := validQ; q.MaxPoints = 2500; return q }(),
			wantErrIs: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:  "range from equals to",
			actor: validActor,
			q: func() telemetry.TelemetryHistoryQuery {
				q := validQ
				q.Range = telemetry.TimeRange{From: now, To: now}
				return q
			}(),
			wantErrIs: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:  "range from after to",
			actor: validActor,
			q: func() telemetry.TelemetryHistoryQuery {
				q := validQ
				q.Range = telemetry.TimeRange{From: now.Add(time.Hour), To: now}
				return q
			}(),
			wantErrIs: telemetry.ErrHistoryInvalidInput,
		},
		{
			name:  "range exceeds 744h",
			actor: validActor,
			q: func() telemetry.TelemetryHistoryQuery {
				q := validQ
				q.Range = telemetry.TimeRange{From: now.Add(-745 * time.Hour), To: now}
				return q
			}(),
			wantErrIs: telemetry.ErrHistoryRangeTooLarge,
		},
		{
			name:  "raw resolution window exceeds 10s",
			actor: validActor,
			q: func() telemetry.TelemetryHistoryQuery {
				q := validQ
				q.Resolution = telemetry.ResolutionRaw
				q.Range = telemetry.TimeRange{From: now.Add(-11 * time.Second), To: now}
				return q
			}(),
			wantErrIs: telemetry.ErrHistoryRawLimitExceeded,
		},
		{
			name:  "resolution too fine for window",
			actor: validActor,
			q: func() telemetry.TelemetryHistoryQuery {
				q := validQ
				q.Resolution = telemetry.Resolution100ms
				q.MaxPoints = 100
				// 1 hour at 100ms resolution = 36000 buckets, which exceeds 100 maxPoints
				q.Range = telemetry.TimeRange{From: now.Add(-time.Hour), To: now}
				return q
			}(),
			wantErrIs: telemetry.ErrHistoryResolutionTooFine,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res.calls = 0
			var callbackCalls int

			var fn telemetry.HistoryCallback = func(_ context.Context, _ telemetry.ResolvedSensor, _ telemetry.TelemetryHistoryQuery) error {
				callbackCalls++
				return nil
			}
			if tc.nilFn {
				fn = nil
			}

			err := prep.PrepareTelemetryHistory(context.Background(), tc.actor, tc.q, fn)
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
}

func TestPrepareTelemetryHistory_ResolverErrorMappingAndDeniedAuth(t *testing.T) {
	tests := []struct {
		name         string
		resolverErr  error
		sensor       *telemetry.ResolvedSensor
		wantErrIs    error
		wantCallback bool
	}{
		{
			name:         "owner authorized sensor resolved",
			sensor:       &telemetry.ResolvedSensor{GatewayID: "gateway_001", SensorID: "sensor_001"},
			wantErrIs:    nil,
			wantCallback: true,
		},
		{
			name:         "operator authorized sensor resolved",
			sensor:       &telemetry.ResolvedSensor{GatewayID: "gateway_001", SensorID: "sensor_001"},
			wantErrIs:    nil,
			wantCallback: true,
		},
		{
			name:         "viewer authorized sensor resolved",
			sensor:       &telemetry.ResolvedSensor{GatewayID: "gateway_001", SensorID: "sensor_001"},
			wantErrIs:    nil,
			wantCallback: true,
		},
		{
			name:         "missing or non-member returns not found unchanged",
			resolverErr:  telemetry.ErrHistoryNotFound,
			wantErrIs:    telemetry.ErrHistoryNotFound,
			wantCallback: false,
		},
		{
			name:         "dependency unavailable returned unchanged",
			resolverErr:  telemetry.ErrHistoryDependency,
			wantErrIs:    telemetry.ErrHistoryDependency,
			wantCallback: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := &mockSensorResolver{
				sensor: tc.sensor,
				err:    tc.resolverErr,
			}
			prep, err := telemetry.NewHistoryPreparation(res, telemetry.DefaultHistoryPreparationConfig())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
			var callbackCalled bool
			err = prep.PrepareTelemetryHistory(context.Background(), actor, validHistoryQuery(), func(_ context.Context, _ telemetry.ResolvedSensor, _ telemetry.TelemetryHistoryQuery) error {
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

func TestPrepareTelemetryHistory_SameContextAcrossAuthAndCallback(t *testing.T) {
	res := &mockSensorResolver{
		sensor: &telemetry.ResolvedSensor{GatewayID: "gateway_001", SensorID: "sensor_001"},
	}
	prep, err := telemetry.NewHistoryPreparation(res, telemetry.DefaultHistoryPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	var callbackCtx context.Context

	err = prep.PrepareTelemetryHistory(context.Background(), actor, validHistoryQuery(), func(ctx context.Context, _ telemetry.ResolvedSensor, _ telemetry.TelemetryHistoryQuery) error {
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

func TestPrepareTelemetryHistory_ParentDeadlineRetained(t *testing.T) {
	res := &mockSensorResolver{
		sensor: &telemetry.ResolvedSensor{GatewayID: "gateway_001", SensorID: "sensor_001"},
	}
	prep, err := telemetry.NewHistoryPreparation(res, telemetry.DefaultHistoryPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parentDeadline := time.Now().Add(500 * time.Millisecond)
	parentCtx, cancel := context.WithDeadline(context.Background(), parentDeadline)
	defer cancel()

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	var observedDeadline time.Time

	err = prep.PrepareTelemetryHistory(parentCtx, actor, validHistoryQuery(), func(ctx context.Context, _ telemetry.ResolvedSensor, _ telemetry.TelemetryHistoryQuery) error {
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

func TestPrepareTelemetryHistory_CanceledOrExpiredNilSuccessRejected(t *testing.T) {
	res := &mockSensorResolver{
		sensor: &telemetry.ResolvedSensor{GatewayID: "gateway_001", SensorID: "sensor_001"},
	}
	prep, err := telemetry.NewHistoryPreparation(res, telemetry.DefaultHistoryPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	actor := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	// 1. Context canceled during callback, but callback returns nil (fake returns nil-success)
	parentCtx, cancel := context.WithCancel(context.Background())
	err = prep.PrepareTelemetryHistory(parentCtx, actor, validHistoryQuery(), func(_ context.Context, _ telemetry.ResolvedSensor, _ telemetry.TelemetryHistoryQuery) error {
		cancel() // cancel context during execution
		return nil
	})

	if err == nil {
		t.Fatal("expected error when context canceled during callback, got nil")
	}
	if !errors.Is(err, telemetry.ErrHistoryDependency) {
		t.Fatalf("error %v, want errors.Is ErrHistoryDependency", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want errors.Is context.Canceled", err)
	}

	// 2. Already canceled context at entry
	canceledCtx, cancel2 := context.WithCancel(context.Background())
	cancel2()

	err = prep.PrepareTelemetryHistory(canceledCtx, actor, validHistoryQuery(), func(_ context.Context, _ telemetry.ResolvedSensor, _ telemetry.TelemetryHistoryQuery) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error on already canceled context, got nil")
	}
	if !errors.Is(err, telemetry.ErrHistoryDependency) {
		t.Fatalf("error %v, want errors.Is ErrHistoryDependency", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want errors.Is context.Canceled", err)
	}

	// 3. Context canceled during ResolveSensor, but resolver returns nil error (fake returns nil-success)
	// Callback must NOT execute under dead context.
	parentCtx3, cancel3 := context.WithCancel(context.Background())
	callbackCalled3 := false
	res3 := &mockSensorResolver{
		onResolve: func() {
			cancel3() // cancel context during resolve
		},
		sensor: &telemetry.ResolvedSensor{GatewayID: "gateway_001", SensorID: "sensor_001"},
	}
	prep3, err := telemetry.NewHistoryPreparation(res3, telemetry.DefaultHistoryPreparationConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = prep3.PrepareTelemetryHistory(parentCtx3, actor, validHistoryQuery(), func(_ context.Context, _ telemetry.ResolvedSensor, _ telemetry.TelemetryHistoryQuery) error {
		callbackCalled3 = true
		return nil
	})
	if err == nil {
		t.Fatal("expected error when context canceled during resolve, got nil")
	}
	if callbackCalled3 {
		t.Fatal("callback must NOT execute under dead context")
	}
	if !errors.Is(err, telemetry.ErrHistoryDependency) {
		t.Fatalf("error %v, want errors.Is ErrHistoryDependency", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v, want errors.Is context.Canceled", err)
	}
}
