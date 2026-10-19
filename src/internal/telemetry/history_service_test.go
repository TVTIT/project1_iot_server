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

type mockHistoryReader struct {
	calls       int
	callTraces  []readerCallTrace
	onRead      func(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error)
	resultToRet *telemetry.TelemetryHistoryResult
	errToRet    error
	perCallFunc []func(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error)
}

type readerCallTrace struct {
	Ctx   context.Context
	Actor uuid.UUID
	Query telemetry.TelemetryHistoryQuery
}

func (m *mockHistoryReader) ReadTelemetryHistory(
	ctx context.Context,
	actor uuid.UUID,
	q telemetry.TelemetryHistoryQuery,
) (*telemetry.TelemetryHistoryResult, error) {
	callIdx := m.calls
	m.calls++
	m.callTraces = append(m.callTraces, readerCallTrace{
		Ctx:   ctx,
		Actor: actor,
		Query: q,
	})

	if callIdx < len(m.perCallFunc) && m.perCallFunc[callIdx] != nil {
		return m.perCallFunc[callIdx](ctx, actor, q)
	}

	if m.onRead != nil {
		return m.onRead(ctx, actor, q)
	}

	if m.errToRet != nil {
		return nil, m.errToRet
	}

	if m.resultToRet != nil {
		return m.resultToRet, nil
	}

	// Default stub result
	unit := "degC"
	return &telemetry.TelemetryHistoryResult{
		GatewayID:  q.GatewayID,
		SensorID:   q.SensorID,
		Unit:       &unit,
		From:       q.Range.From.UTC(),
		To:         q.Range.To.UTC(),
		Mode:       string(q.Resolution),
		Resolution: string(q.Resolution),
	}, nil
}

func TestSelectAutoBucketResolution(t *testing.T) {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("smallest_allowed_width_selection", func(t *testing.T) {
		// 100ms span with maxPoints=1000 -> 100ms width yields 1 bucket <= 1000 -> Resolution100ms
		res, err := telemetry.SelectAutoBucketResolution(epoch, epoch.Add(100*time.Millisecond), 1000)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution100ms {
			t.Fatalf("expected Resolution100ms, got %v", res)
		}

		// 2s span with maxPoints=10:
		// 100ms gives 20 buckets (> 10)
		// 1s gives 2 buckets (<= 10) -> Resolution1s
		res, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(2*time.Second), 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution1s {
			t.Fatalf("expected Resolution1s, got %v", res)
		}

		// 20s span with maxPoints=5:
		// 100ms: 200 (> 5), 1s: 20 (> 5), 10s: 2 (<= 5) -> Resolution10s
		res, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(20*time.Second), 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution10s {
			t.Fatalf("expected Resolution10s, got %v", res)
		}

		// 2m span with maxPoints=5:
		// 10s: 12 (> 5), 1m: 2 (<= 5) -> Resolution1m
		res, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(2*time.Minute), 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution1m {
			t.Fatalf("expected Resolution1m, got %v", res)
		}

		// 10m span with maxPoints=5:
		// 1m: 10 (> 5), 5m: 2 (<= 5) -> Resolution5m
		res, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(10*time.Minute), 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution5m {
			t.Fatalf("expected Resolution5m, got %v", res)
		}

		// 30m span with maxPoints=5:
		// 5m: 6 (> 5), 15m: 2 (<= 5) -> Resolution15m
		res, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(30*time.Minute), 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution15m {
			t.Fatalf("expected Resolution15m, got %v", res)
		}

		// 2h span with maxPoints=5:
		// 15m: 8 (> 5), 1h: 2 (<= 5) -> Resolution1h
		res, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(2*time.Hour), 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution1h {
			t.Fatalf("expected Resolution1h, got %v", res)
		}

		// 12h span with maxPoints=5:
		// 1h: 12 (> 5), 6h: 2 (<= 5) -> Resolution6h
		res, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(12*time.Hour), 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution6h {
			t.Fatalf("expected Resolution6h, got %v", res)
		}

		// 48h span with maxPoints=5:
		// 6h: 8 (> 5), 1d: 2 (<= 5) -> Resolution1d
		res, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(48*time.Hour), 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution1d {
			t.Fatalf("expected Resolution1d, got %v", res)
		}
	})

	t.Run("partial_edges_not_duration_only", func(t *testing.T) {
		// from = 00:00:00.050Z, to = 00:00:00.150Z. Duration is 100ms.
		// For 100ms width: bucket 0 is [0, 100ms), bucket 1 is [100ms, 200ms).
		// IntersectingBucketCount = 2 buckets!
		// If maxPoints = 1:
		// 100ms yields 2 buckets (> 1) -> does NOT fit!
		// Next resolution is 1s: bucket 0 is [0, 1s), count = 1 <= 1 -> selects Resolution1s!
		// (A naive duration-only calculation duration/width = 100ms/100ms = 1 would erroneously choose 100ms).
		from := epoch.Add(50 * time.Millisecond)
		to := epoch.Add(150 * time.Millisecond)
		res, err := telemetry.SelectAutoBucketResolution(from, to, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution1s {
			t.Fatalf("expected Resolution1s due to partial edge intersection, got %v", res)
		}
	})

	t.Run("exact_boundary", func(t *testing.T) {
		// [0, 1s) with width=1s: exactly 1 bucket [0, 1s). The point 1s is exclusive.
		from := epoch
		to := epoch.Add(time.Second)
		res, err := telemetry.SelectAutoBucketResolution(from, to, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution1s {
			t.Fatalf("expected Resolution1s, got %v", res)
		}
	})

	t.Run("pre_epoch", func(t *testing.T) {
		// Range before 1970-01-01:
		// from = 1969-12-31T23:59:50Z (-10s), to = 1970-01-01T00:00:10Z (+10s).
		// Duration = 20s. With width=10s, intersects 2 buckets: [-10s, 0) and [0, 10s).
		from := epoch.Add(-10 * time.Second)
		to := epoch.Add(10 * time.Second)
		res, err := telemetry.SelectAutoBucketResolution(from, to, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution10s {
			t.Fatalf("expected Resolution10s, got %v", res)
		}
	})

	t.Run("max_range", func(t *testing.T) {
		// 744 hours (31 days).
		from := epoch
		to := epoch.Add(744 * time.Hour)

		// With maxPoints = 2000:
		// 15m: 744 * 4 = 2976 buckets (> 2000)
		// 1h: 744 buckets (<= 2000) -> Resolution1h
		res, err := telemetry.SelectAutoBucketResolution(from, to, 2000)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution1h {
			t.Fatalf("expected Resolution1h for max range with 2000 points, got %v", res)
		}

		// With maxPoints = 100:
		// 6h: 744 / 6 = 124 buckets (> 100)
		// 1d: 31 buckets (<= 100) -> Resolution1d
		res, err = telemetry.SelectAutoBucketResolution(from, to, 100)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res != telemetry.Resolution1d {
			t.Fatalf("expected Resolution1d for max range with 100 points, got %v", res)
		}
	})

	t.Run("resolution_too_fine", func(t *testing.T) {
		// 744 hours with maxPoints = 10:
		// 1d has 31 buckets > 10. No allowed resolution fits!
		from := epoch
		to := epoch.Add(744 * time.Hour)
		_, err := telemetry.SelectAutoBucketResolution(from, to, 10)
		if !errors.Is(err, telemetry.ErrHistoryResolutionTooFine) {
			t.Fatalf("expected ErrHistoryResolutionTooFine, got %v", err)
		}
	})

	t.Run("invalid_inputs", func(t *testing.T) {
		// to <= from
		_, err := telemetry.SelectAutoBucketResolution(epoch, epoch, 100)
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput for equal range, got %v", err)
		}

		_, err = telemetry.SelectAutoBucketResolution(epoch.Add(time.Second), epoch, 100)
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput for inverted range, got %v", err)
		}

		// maxPoints <= 0
		_, err = telemetry.SelectAutoBucketResolution(epoch, epoch.Add(time.Second), 0)
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput for maxPoints=0, got %v", err)
		}
	})
}

func TestTelemetryHistoryService_Constructor(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	resolver := &mockSensorResolver{sensor: &telemetry.ResolvedSensor{GatewayID: "gw", SensorID: "s"}}
	reader := &mockHistoryReader{}

	t.Run("success", func(t *testing.T) {
		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if svc == nil {
			t.Fatal("expected non-nil service")
		}
	})

	t.Run("nil_resolver", func(t *testing.T) {
		_, err := telemetry.NewTelemetryHistoryService(nil, reader, cfg)
		if err == nil || !strings.Contains(err.Error(), "resolver") {
			t.Fatalf("expected resolver required error, got %v", err)
		}
	})

	t.Run("typed_nil_resolver", func(t *testing.T) {
		var typedNilResolver *mockSensorResolver
		_, err := telemetry.NewTelemetryHistoryService(typedNilResolver, reader, cfg)
		if err == nil || !strings.Contains(err.Error(), "resolver") {
			t.Fatalf("expected resolver required error, got %v", err)
		}
	})

	t.Run("nil_reader", func(t *testing.T) {
		_, err := telemetry.NewTelemetryHistoryService(resolver, nil, cfg)
		if err == nil || !strings.Contains(err.Error(), "reader") {
			t.Fatalf("expected reader required error, got %v", err)
		}
	})

	t.Run("typed_nil_reader", func(t *testing.T) {
		var typedNilReader *mockHistoryReader
		_, err := telemetry.NewTelemetryHistoryService(resolver, typedNilReader, cfg)
		if err == nil || !strings.Contains(err.Error(), "reader") {
			t.Fatalf("expected reader required error, got %v", err)
		}
	})

	t.Run("invalid_config", func(t *testing.T) {
		badCfg := cfg
		badCfg.QueryTimeout = 0
		_, err := telemetry.NewTelemetryHistoryService(resolver, reader, badCfg)
		if err == nil {
			t.Fatal("expected error for invalid config")
		}
	})

	t.Run("from_preparation_success_and_nil_checks", func(t *testing.T) {
		prep, err := telemetry.NewHistoryPreparation(resolver, cfg)
		if err != nil {
			t.Fatalf("unexpected prep error: %v", err)
		}
		svc, err := telemetry.NewTelemetryHistoryServiceFromPreparation(prep, reader)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if svc == nil {
			t.Fatal("expected non-nil service")
		}

		var typedNilPrep *telemetry.HistoryPreparation
		_, err = telemetry.NewTelemetryHistoryServiceFromPreparation(typedNilPrep, reader)
		if err == nil {
			t.Fatal("expected error for typed nil preparation")
		}

		_, err = telemetry.NewTelemetryHistoryServiceFromPreparation(prep, nil)
		if err == nil {
			t.Fatal("expected error for nil reader")
		}
	})
}

func TestTelemetryHistoryService_GetTelemetryHistory(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.New()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	unit := "degC"

	baseSensor := &telemetry.ResolvedSensor{
		GatewayID: "gateway_001",
		SensorID:  "sensor_001",
		Unit:      &unit,
	}

	t.Run("short_auto_within_raw_window_success", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			resultToRet: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				Unit:       &unit,
				From:       now.Add(-5 * time.Second),
				To:         now,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 1,
				RawPoints:  []telemetry.RawSamplePoint{{MeasuredAt: now.Add(-time.Second), Value: 23.5}},
			},
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  1000,
		}

		res, err := svc.GetTelemetryHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected raw mode/resolution, got mode=%q res=%q", res.Mode, res.Resolution)
		}
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected exactly 1 call to reader, got %d", len(reader.callTraces))
		}
		// Must have probed with ResolutionRaw, NEVER ResolutionAuto
		if reader.callTraces[0].Query.Resolution != telemetry.ResolutionRaw {
			t.Fatalf("expected reader called with ResolutionRaw, got %q", reader.callTraces[0].Query.Resolution)
		}
	})

	t.Run("raw_exact_mode", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			resultToRet: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				Unit:       &unit,
				From:       now.Add(-5 * time.Second),
				To:         now,
				Mode:       "raw",
				Resolution: "raw",
				PointCount: 1,
				RawPoints:  []telemetry.RawSamplePoint{{MeasuredAt: now.Add(-time.Second), Value: 23.5}},
			},
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  1000,
		}

		res, err := svc.GetTelemetryHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mode != "raw" || res.Resolution != "raw" {
			t.Fatalf("expected raw mode, got mode=%s res=%s", res.Mode, res.Resolution)
		}
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected exactly 1 call to reader, got %d", len(reader.callTraces))
		}
		if reader.callTraces[0].Query.Resolution != telemetry.ResolutionRaw {
			t.Fatalf("expected ResolutionRaw, got %q", reader.callTraces[0].Query.Resolution)
		}
	})

	t.Run("short_auto_raw_overflow_fallback_to_bucket", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			perCallFunc: []func(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error){
				// Call 0 (raw probe): returns ErrHistoryRawLimitExceeded
				func(_ context.Context, _ uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
					if q.Resolution != telemetry.ResolutionRaw {
						t.Errorf("call 0 expected ResolutionRaw, got %v", q.Resolution)
					}
					return nil, telemetry.ErrHistoryRawLimitExceeded
				},
				// Call 1 (fallback bucket): returns bucket result
				func(_ context.Context, _ uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
					// 5s window with maxPoints=1000 -> 100ms bucket width gives 50 buckets <= 1000 -> Resolution100ms
					if q.Resolution != telemetry.Resolution100ms {
						t.Errorf("call 1 expected Resolution100ms, got %v", q.Resolution)
					}
					return &telemetry.TelemetryHistoryResult{
						GatewayID:    "gateway_001",
						SensorID:     "sensor_001",
						Unit:         &unit,
						From:         q.Range.From.UTC(),
						To:           q.Range.To.UTC(),
						Mode:         "bucket",
						Resolution:   string(q.Resolution),
						PointCount:   1,
						BucketPoints: []telemetry.BucketSamplePoint{{Avg: 23.5, Min: 23.0, Max: 24.0, Count: 100}},
					}, nil
				},
			},
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  1000,
		}

		res, err := svc.GetTelemetryHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mode != "bucket" || res.Resolution != "100ms" {
			t.Fatalf("expected bucket mode with resolution 100ms, got mode=%q res=%q", res.Mode, res.Resolution)
		}
		if len(reader.callTraces) != 2 {
			t.Fatalf("expected exactly 2 calls to reader (probe + fallback), got %d", len(reader.callTraces))
		}
		if reader.callTraces[0].Query.Resolution != telemetry.ResolutionRaw {
			t.Fatalf("call 0 expected ResolutionRaw, got %v", reader.callTraces[0].Query.Resolution)
		}
		if reader.callTraces[1].Query.Resolution != telemetry.Resolution100ms {
			t.Fatalf("call 1 expected Resolution100ms, got %v", reader.callTraces[1].Query.Resolution)
		}
	})

	t.Run("long_auto_window_no_raw_probe", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			resultToRet: &telemetry.TelemetryHistoryResult{
				GatewayID:    "gateway_001",
				SensorID:     "sensor_001",
				Unit:         &unit,
				From:         now.Add(-10 * time.Minute),
				To:           now,
				Mode:         "bucket",
				Resolution:   "1s",
				PointCount:   1,
				BucketPoints: []telemetry.BucketSamplePoint{{Avg: 23.5, Min: 23.0, Max: 24.0, Count: 600}},
			},
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// 10 minutes > RawWindow (10s):
		// 10m with maxPoints=1000:
		// 100ms gives 6000 buckets (> 1000)
		// 1s gives 600 buckets (<= 1000) -> selects Resolution1s
		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-10 * time.Minute), To: now},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  1000,
		}

		res, err := svc.GetTelemetryHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mode != "bucket" {
			t.Fatalf("expected bucket mode, got %q", res.Mode)
		}
		// Exactly 1 call, direct to chosen bucket: NO probe!
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected exactly 1 call to reader (no raw probe), got %d", len(reader.callTraces))
		}
		if reader.callTraces[0].Query.Resolution != telemetry.Resolution1s {
			t.Fatalf("expected chosen bucket Resolution1s, got %v", reader.callTraces[0].Query.Resolution)
		}
	})

	t.Run("no_fallback_on_dependency_error", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			errToRet: telemetry.ErrHistoryDependency,
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  1000,
		}

		_, err = svc.GetTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryDependency) {
			t.Fatalf("expected ErrHistoryDependency, got %v", err)
		}
		// Probe failed with dependency error -> MUST NOT fall back to bucket!
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected exactly 1 call to reader (no fallback), got %d", len(reader.callTraces))
		}
	})

	t.Run("no_fallback_on_not_found_error", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			errToRet: telemetry.ErrHistoryNotFound,
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  1000,
		}

		_, err = svc.GetTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryNotFound) {
			t.Fatalf("expected ErrHistoryNotFound, got %v", err)
		}
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected exactly 1 call to reader (no fallback), got %d", len(reader.callTraces))
		}
	})

	t.Run("no_fallback_on_unexpected_error", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			errToRet: telemetry.ErrHistoryUnexpected,
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  1000,
		}

		_, err = svc.GetTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryUnexpected) {
			t.Fatalf("expected ErrHistoryUnexpected, got %v", err)
		}
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected exactly 1 call to reader (no fallback), got %d", len(reader.callTraces))
		}
	})

	t.Run("explicit_bucket_mode", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			resultToRet: &telemetry.TelemetryHistoryResult{
				GatewayID:    "gateway_001",
				SensorID:     "sensor_001",
				Unit:         &unit,
				From:         now.Add(-10 * time.Minute),
				To:           now,
				Mode:         "bucket",
				Resolution:   "1s",
				PointCount:   1,
				BucketPoints: []telemetry.BucketSamplePoint{{Avg: 23.5, Min: 23.0, Max: 24.0, Count: 600}},
			},
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-10 * time.Minute), To: now},
			Resolution: telemetry.Resolution1s,
			MaxPoints:  1000,
		}

		res, err := svc.GetTelemetryHistory(context.Background(), actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mode != "bucket" || res.Resolution != "1s" {
			t.Fatalf("expected bucket mode with 1s resolution, got %v", res)
		}
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected 1 call, got %d", len(reader.callTraces))
		}
		if reader.callTraces[0].Query.Resolution != telemetry.Resolution1s {
			t.Fatalf("expected Resolution1s, got %v", reader.callTraces[0].Query.Resolution)
		}
	})

	t.Run("explicit_raw_overflow_no_fallback", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			errToRet: telemetry.ErrHistoryRawLimitExceeded,
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionRaw,
			MaxPoints:  1000,
		}

		_, err = svc.GetTelemetryHistory(context.Background(), actor, q)
		if !errors.Is(err, telemetry.ErrHistoryRawLimitExceeded) {
			t.Fatalf("expected ErrHistoryRawLimitExceeded, got %v", err)
		}
		// Explicit raw MUST NOT fallback to bucket!
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected exactly 1 call to reader (no fallback), got %d", len(reader.callTraces))
		}
	})

	t.Run("nil_deps_and_invalid_inputs_no_reads", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Nil actor
		q := validHistoryQuery()
		_, err = svc.GetTelemetryHistory(context.Background(), uuid.Nil, q)
		if !errors.Is(err, telemetry.ErrHistoryNilActor) {
			t.Fatalf("expected ErrHistoryNilActor, got %v", err)
		}
		if resolver.calls != 0 || reader.calls != 0 {
			t.Fatalf("expected zero calls, got resolver=%d reader=%d", resolver.calls, reader.calls)
		}

		// Invalid gateway ID
		badQ := q
		badQ.GatewayID = "invalid/id"
		_, err = svc.GetTelemetryHistory(context.Background(), actor, badQ)
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput, got %v", err)
		}
		if resolver.calls != 0 || reader.calls != 0 {
			t.Fatalf("expected zero calls, got resolver=%d reader=%d", resolver.calls, reader.calls)
		}

		// Range too large
		badQ = q
		badQ.Range = telemetry.TimeRange{From: now.Add(-800 * time.Hour), To: now}
		_, err = svc.GetTelemetryHistory(context.Background(), actor, badQ)
		if !errors.Is(err, telemetry.ErrHistoryRangeTooLarge) {
			t.Fatalf("expected ErrHistoryRangeTooLarge, got %v", err)
		}
		if resolver.calls != 0 || reader.calls != 0 {
			t.Fatalf("expected zero calls, got resolver=%d reader=%d", resolver.calls, reader.calls)
		}

		// Max points out of range
		badQ = q
		badQ.MaxPoints = 50 // below minPoints (100)
		_, err = svc.GetTelemetryHistory(context.Background(), actor, badQ)
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput, got %v", err)
		}
		if resolver.calls != 0 || reader.calls != 0 {
			t.Fatalf("expected zero calls, got resolver=%d reader=%d", resolver.calls, reader.calls)
		}
	})

	t.Run("single_same_context_parent_deadline_preserved", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parentDeadline := time.Now().Add(50 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), parentDeadline)
		defer cancel()

		q := validHistoryQuery()
		_, err = svc.GetTelemetryHistory(ctx, actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if resolver.calls != 1 || reader.calls != 1 {
			t.Fatalf("expected 1 resolver and 1 reader call")
		}

		// Verify resolver context deadline matches parent deadline
		rDeadline, ok := resolver.lastCtx.Deadline()
		if !ok || rDeadline.Sub(parentDeadline).Abs() > time.Millisecond {
			t.Fatalf("expected resolver deadline to match parent deadline, got %v vs %v", rDeadline, parentDeadline)
		}

		// Verify reader context deadline matches parent deadline (no new 5s deadline added!)
		rdDeadline, ok := reader.callTraces[0].Ctx.Deadline()
		if !ok || rdDeadline.Sub(parentDeadline).Abs() > time.Millisecond {
			t.Fatalf("expected reader deadline to match parent deadline, got %v vs %v", rdDeadline, parentDeadline)
		}
	})

	t.Run("cancel_between_probe_and_fallback_prevents_next_query", func(t *testing.T) {
		resolver := &mockSensorResolver{sensor: baseSensor}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reader := &mockHistoryReader{
			perCallFunc: []func(ctx context.Context, actor uuid.UUID, q telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error){
				// Call 0 (raw probe): cancels context and returns ErrHistoryRawLimitExceeded
				func(_ context.Context, _ uuid.UUID, _ telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
					cancel()
					return nil, telemetry.ErrHistoryRawLimitExceeded
				},
				// Call 1 (fallback bucket): should NEVER be called!
				func(_ context.Context, _ uuid.UUID, _ telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
					t.Fatal("fallback bucket query must not be executed when context is canceled")
					return nil, nil
				},
			},
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  1000,
		}

		_, err = svc.GetTelemetryHistory(ctx, actor, q)
		if !errors.Is(err, telemetry.ErrHistoryDependency) {
			t.Fatalf("expected ErrHistoryDependency on canceled context, got %v", err)
		}
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected exactly 1 call (fallback was canceled), got %d", len(reader.callTraces))
		}
	})

	t.Run("canceled_resolver_no_reader", func(t *testing.T) {
		resolver := &mockSensorResolver{
			err: context.Canceled,
		}
		reader := &mockHistoryReader{}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := validHistoryQuery()
		_, err = svc.GetTelemetryHistory(context.Background(), actor, q)
		if err == nil {
			t.Fatal("expected error")
		}
		if reader.calls != 0 {
			t.Fatalf("expected 0 reader calls when resolver failed, got %d", reader.calls)
		}
	})

	t.Run("fake_returns_success_after_cancel_no_exposed_result", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		resolver := &mockSensorResolver{sensor: baseSensor}
		reader := &mockHistoryReader{
			onRead: func(_ context.Context, _ uuid.UUID, _ telemetry.TelemetryHistoryQuery) (*telemetry.TelemetryHistoryResult, error) {
				// Cancel during read, but return success anyway
				cancel()
				return &telemetry.TelemetryHistoryResult{
					GatewayID: "gateway_001",
					SensorID:  "sensor_001",
					Mode:      "bucket",
				}, nil
			},
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := validHistoryQuery()
		res, err := svc.GetTelemetryHistory(ctx, actor, q)
		// Must not expose result! Must return ErrHistoryDependency!
		if res != nil {
			t.Fatalf("expected nil result on canceled context, got %+v", res)
		}
		if !errors.Is(err, telemetry.ErrHistoryDependency) {
			t.Fatalf("expected ErrHistoryDependency, got %v", err)
		}
	})

	t.Run("data_query_notfound_after_prep_stays_notfound", func(t *testing.T) {
		// Resolver succeeds
		resolver := &mockSensorResolver{sensor: baseSensor}
		// Data reader returns ErrHistoryNotFound (e.g. membership deleted between prep and data read)
		reader := &mockHistoryReader{
			errToRet: telemetry.ErrHistoryNotFound,
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		q := validHistoryQuery()
		res, err := svc.GetTelemetryHistory(context.Background(), actor, q)
		if res != nil {
			t.Fatalf("expected nil result, got %+v", res)
		}
		if !errors.Is(err, telemetry.ErrHistoryNotFound) {
			t.Fatalf("expected ErrHistoryNotFound, got %v", err)
		}
	})
}

func TestTelemetryHistoryService_ExecuteEffectiveHistoryQuery(t *testing.T) {
	cfg := telemetry.DefaultHistoryPreparationConfig()
	actor := uuid.New()
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	unit := "degC"

	baseSensor := &telemetry.ResolvedSensor{
		GatewayID: "gateway_001",
		SensorID:  "sensor_001",
		Unit:      &unit,
	}
	resolver := &mockSensorResolver{sensor: baseSensor}

	t.Run("direct_execution_success_with_existing_context", func(t *testing.T) {
		reader := &mockHistoryReader{
			resultToRet: &telemetry.TelemetryHistoryResult{
				GatewayID:  "gateway_001",
				SensorID:   "sensor_001",
				Unit:       &unit,
				From:       now.Add(-5 * time.Second),
				To:         now,
				Mode:       "raw",
				Resolution: "raw",
			},
		}

		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		q := telemetry.TelemetryHistoryQuery{
			GatewayID:  "gateway_001",
			SensorID:   "sensor_001",
			Range:      telemetry.TimeRange{From: now.Add(-5 * time.Second), To: now},
			Resolution: telemetry.ResolutionAuto,
			MaxPoints:  1000,
		}

		res, err := svc.ExecuteEffectiveHistoryQuery(ctx, actor, q)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mode != "raw" {
			t.Fatalf("expected raw mode, got %q", res.Mode)
		}
		if len(reader.callTraces) != 1 {
			t.Fatalf("expected 1 reader call, got %d", len(reader.callTraces))
		}
		// Notice: Resolver is NOT called because caller already has resolved/scoped context
		if resolver.calls != 0 {
			t.Fatalf("expected resolver calls=0 for direct effective query, got %d", resolver.calls)
		}
	})

	t.Run("canceled_context_fails_fast", func(t *testing.T) {
		reader := &mockHistoryReader{}
		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		q := validHistoryQuery()
		_, err = svc.ExecuteEffectiveHistoryQuery(ctx, actor, q)
		if !errors.Is(err, telemetry.ErrHistoryDependency) {
			t.Fatalf("expected ErrHistoryDependency on canceled context, got %v", err)
		}
		if reader.calls != 0 {
			t.Fatalf("expected 0 reader calls, got %d", reader.calls)
		}
	})

	t.Run("validation_checks", func(t *testing.T) {
		reader := &mockHistoryReader{}
		svc, err := telemetry.NewTelemetryHistoryService(resolver, reader, cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// nil context
		q := validHistoryQuery()
		_, err = svc.ExecuteEffectiveHistoryQuery(nil, actor, q) //nolint:staticcheck // testing nil context handling explicitly
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput for nil ctx, got %v", err)
		}

		// nil actor
		_, err = svc.ExecuteEffectiveHistoryQuery(context.Background(), uuid.Nil, q)
		if !errors.Is(err, telemetry.ErrHistoryNilActor) {
			t.Fatalf("expected ErrHistoryNilActor, got %v", err)
		}

		// bad gateway ID
		badQ := q
		badQ.GatewayID = ""
		_, err = svc.ExecuteEffectiveHistoryQuery(context.Background(), actor, badQ)
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput, got %v", err)
		}

		// bad max points
		badQ = q
		badQ.MaxPoints = 50
		_, err = svc.ExecuteEffectiveHistoryQuery(context.Background(), actor, badQ)
		if !errors.Is(err, telemetry.ErrHistoryInvalidInput) {
			t.Fatalf("expected ErrHistoryInvalidInput, got %v", err)
		}
	})
}
