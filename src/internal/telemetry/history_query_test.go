package telemetry_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

func TestResolution_Valid(t *testing.T) {
	valid := []telemetry.Resolution{
		telemetry.ResolutionAuto,
		telemetry.ResolutionRaw,
		telemetry.Resolution100ms,
		telemetry.Resolution1s,
		telemetry.Resolution10s,
		telemetry.Resolution1m,
		telemetry.Resolution5m,
		telemetry.Resolution15m,
		telemetry.Resolution1h,
		telemetry.Resolution6h,
		telemetry.Resolution1d,
	}

	for _, r := range valid {
		if !r.Valid() {
			t.Errorf("expected resolution %q to be valid", r)
		}
	}

	invalid := []telemetry.Resolution{
		"",
		"invalid",
		"10ms",
		"2s",
		"auto_bucket",
		"1w",
	}

	for _, r := range invalid {
		if r.Valid() {
			t.Errorf("expected resolution %q to be invalid", r)
		}
	}
}

type fakeHistoryService struct{}

func (f *fakeHistoryService) GetTelemetryHistory(
	_ context.Context,
	actor uuid.UUID,
	q telemetry.TelemetryHistoryQuery,
) (*telemetry.TelemetryHistoryResult, error) {
	if actor == uuid.Nil {
		return nil, telemetry.ErrHistoryNilActor
	}
	return &telemetry.TelemetryHistoryResult{
		GatewayID:  q.GatewayID,
		SensorID:   q.SensorID,
		From:       q.Range.From,
		To:         q.Range.To,
		Mode:       "raw",
		Resolution: string(q.Resolution),
		PointCount: 0,
		RawPoints:  []telemetry.RawSamplePoint{},
	}, nil
}

func TestHistoryService_Contract(t *testing.T) {
	var svc telemetry.HistoryService = &fakeHistoryService{}
	ctx := context.Background()

	// Nil actor must fail
	_, err := svc.GetTelemetryHistory(ctx, uuid.Nil, telemetry.TelemetryHistoryQuery{
		GatewayID: "gw1",
		SensorID:  "sen1",
	})
	if err != telemetry.ErrHistoryNilActor {
		t.Fatalf("expected ErrHistoryNilActor, got %v", err)
	}

	// Valid actor
	actor := uuid.New()
	from := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	to := from.Add(5 * time.Second)
	res, err := svc.GetTelemetryHistory(ctx, actor, telemetry.TelemetryHistoryQuery{
		GatewayID:  "gw1",
		SensorID:   "sen1",
		Range:      telemetry.TimeRange{From: from, To: to},
		Resolution: telemetry.ResolutionRaw,
		MaxPoints:  1000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.GatewayID != "gw1" || res.SensorID != "sen1" {
		t.Errorf("mismatched result identifiers: %+v", res)
	}
}

func TestResolutionDuration(t *testing.T) {
	tests := []struct {
		res       telemetry.Resolution
		wantDur   time.Duration
		wantValid bool
	}{
		{telemetry.Resolution100ms, 100 * time.Millisecond, true},
		{telemetry.Resolution1s, time.Second, true},
		{telemetry.Resolution10s, 10 * time.Second, true},
		{telemetry.Resolution1m, time.Minute, true},
		{telemetry.Resolution5m, 5 * time.Minute, true},
		{telemetry.Resolution15m, 15 * time.Minute, true},
		{telemetry.Resolution1h, time.Hour, true},
		{telemetry.Resolution6h, 6 * time.Hour, true},
		{telemetry.Resolution1d, 24 * time.Hour, true},
		{telemetry.ResolutionAuto, 0, false},
		{telemetry.ResolutionRaw, 0, false},
		{telemetry.Resolution("unknown"), 0, false},
	}

	for _, tt := range tests {
		dur, ok := telemetry.ResolutionDuration(tt.res)
		if ok != tt.wantValid || dur != tt.wantDur {
			t.Errorf("ResolutionDuration(%q) = (%v, %v), want (%v, %v)",
				tt.res, dur, ok, tt.wantDur, tt.wantValid)
		}
	}
}

func TestIntersectingBucketCount_TableDriven(t *testing.T) {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		from      time.Time
		to        time.Time
		width     time.Duration
		wantCount int64
		wantErr   bool
	}{
		{
			name:      "exact 1s bucket at epoch",
			from:      epoch,
			to:        epoch.Add(time.Second),
			width:     time.Second,
			wantCount: 1,
		},
		{
			name:      "exact 10s bucket at epoch",
			from:      epoch,
			to:        epoch.Add(10 * time.Second),
			width:     10 * time.Second,
			wantCount: 1,
		},
		{
			name:      "exact 3 1s buckets",
			from:      epoch,
			to:        epoch.Add(3 * time.Second),
			width:     time.Second,
			wantCount: 3,
		},
		{
			name:      "partial edge start [500ms, 1000ms)",
			from:      epoch.Add(500 * time.Millisecond),
			to:        epoch.Add(time.Second),
			width:     time.Second,
			wantCount: 1,
		},
		{
			name:      "partial edge end [0ms, 1500ms) spans 2 1s buckets",
			from:      epoch,
			to:        epoch.Add(1500 * time.Millisecond),
			width:     time.Second,
			wantCount: 2,
		},
		{
			name:      "partial edge both sides [500ms, 2500ms) spans 3 1s buckets",
			from:      epoch.Add(500 * time.Millisecond),
			to:        epoch.Add(2500 * time.Millisecond),
			width:     time.Second,
			wantCount: 3,
		},
		{
			name:      "single microsecond range [0, 1us) spans 1 bucket",
			from:      epoch,
			to:        epoch.Add(time.Microsecond),
			width:     time.Second,
			wantCount: 1,
		},
		{
			name:      "single microsecond at boundary [999999us, 1000000us) spans 1 bucket",
			from:      epoch.Add(999999 * time.Microsecond),
			to:        epoch.Add(time.Second),
			width:     time.Second,
			wantCount: 1,
		},
		{
			name:      "boundary transition [999999us, 1000001us) spans 2 buckets",
			from:      epoch.Add(999999 * time.Microsecond),
			to:        epoch.Add(1000001 * time.Microsecond),
			width:     time.Second,
			wantCount: 2,
		},
		{
			name:      "pre-epoch exact 1s bucket [-1s, 0s)",
			from:      epoch.Add(-time.Second),
			to:        epoch,
			width:     time.Second,
			wantCount: 1,
		},
		{
			name:      "pre-epoch partial edge [-500ms, 0s)",
			from:      epoch.Add(-500 * time.Millisecond),
			to:        epoch,
			width:     time.Second,
			wantCount: 1,
		},
		{
			name:      "pre-epoch partial edge [-1500ms, 0s) spans 2 buckets",
			from:      epoch.Add(-1500 * time.Millisecond),
			to:        epoch,
			width:     time.Second,
			wantCount: 2,
		},
		{
			name:      "spanning epoch boundary [-500ms, 500ms) spans 2 1s buckets ([-1s, 0s) and [0s, 1s))",
			from:      epoch.Add(-500 * time.Millisecond),
			to:        epoch.Add(500 * time.Millisecond),
			width:     time.Second,
			wantCount: 2,
		},
		{
			name:      "extreme year 0001 pre-epoch",
			from:      time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
			to:        time.Date(1, 1, 1, 1, 0, 0, 0, time.UTC),
			width:     time.Hour,
			wantCount: 1,
		},
		{
			name:      "extreme year 9999 post-epoch",
			from:      time.Date(9999, 12, 31, 22, 0, 0, 0, time.UTC),
			to:        time.Date(9999, 12, 31, 23, 0, 0, 0, time.UTC),
			width:     time.Hour,
			wantCount: 1,
		},
		{
			name:    "invalid width zero",
			from:    epoch,
			to:      epoch.Add(time.Second),
			width:   0,
			wantErr: true,
		},
		{
			name:    "invalid width negative",
			from:    epoch,
			to:      epoch.Add(time.Second),
			width:   -time.Second,
			wantErr: true,
		},
		{
			name:    "invalid from equal to",
			from:    epoch,
			to:      epoch,
			width:   time.Second,
			wantErr: true,
		},
		{
			name:    "invalid from after to",
			from:    epoch.Add(time.Second),
			to:      epoch,
			width:   time.Second,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := telemetry.IntersectingBucketCount(tt.from, tt.to, tt.width)
			if (err != nil) != tt.wantErr {
				t.Fatalf("IntersectingBucketCount() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && count != tt.wantCount {
				t.Errorf("IntersectingBucketCount() = %d, want %d", count, tt.wantCount)
			}
		})
	}
}

func TestValidateHistoryTimeRange_TableDriven(t *testing.T) {
	now := time.Date(2026, 8, 21, 10, 15, 0, 0, time.UTC)
	maxRange := 744 * time.Hour
	rawWindow := 10 * time.Second
	maxPoints := 1000

	tests := []struct {
		name      string
		tr        telemetry.TimeRange
		res       telemetry.Resolution
		maxRange  time.Duration
		rawWindow time.Duration
		maxPoints int
		wantErr   error
	}{
		{
			name:      "valid normal range with auto",
			tr:        telemetry.TimeRange{From: now, To: now.Add(time.Hour)},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   nil,
		},
		{
			name:      "valid raw within 10s",
			tr:        telemetry.TimeRange{From: now, To: now.Add(10 * time.Second)},
			res:       telemetry.ResolutionRaw,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   nil,
		},
		{
			name:      "raw exceeds 10s",
			tr:        telemetry.TimeRange{From: now, To: now.Add(10*time.Second + time.Microsecond)},
			res:       telemetry.ResolutionRaw,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryRawLimitExceeded,
		},
		{
			name:      "from equals to",
			tr:        telemetry.TimeRange{From: now, To: now},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "from after to",
			tr:        telemetry.TimeRange{From: now.Add(time.Second), To: now},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "range exceeds max 744h",
			tr:        telemetry.TimeRange{From: now, To: now.Add(744*time.Hour + time.Microsecond)},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryRangeTooLarge,
		},
		{
			name:      "extreme year range without overflow (year 0001 to year 9999)",
			tr:        telemetry.TimeRange{From: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryRangeTooLarge,
		},
		{
			name:      "reject submicrosecond precision in From (nanosecond remainder)",
			tr:        telemetry.TimeRange{From: now.Add(500 * time.Nanosecond), To: now.Add(time.Hour)},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "reject submicrosecond precision in To (nanosecond remainder)",
			tr:        telemetry.TimeRange{From: now, To: now.Add(time.Hour + 100*time.Nanosecond)},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "reject year zero in From",
			tr:        telemetry.TimeRange{From: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC), To: now},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "reject negative year in From",
			tr:        telemetry.TimeRange{From: time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC), To: now},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "reject year above 9999 in To",
			tr:        telemetry.TimeRange{From: now, To: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "reject UTC conversion shifting year to zero",
			tr:        telemetry.TimeRange{From: time.Date(1, 1, 1, 0, 30, 0, 0, time.FixedZone("UTC+1", 3600)), To: now},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "reject UTC conversion shifting year to 10000",
			tr:        telemetry.TimeRange{From: now, To: time.Date(9999, 12, 31, 23, 30, 0, 0, time.FixedZone("UTC-1", -3600))},
			res:       telemetry.ResolutionAuto,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidInput,
		},
		{
			name:      "invalid resolution token",
			tr:        telemetry.TimeRange{From: now, To: now.Add(time.Hour)},
			res:       telemetry.Resolution("invalid"),
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: maxPoints,
			wantErr:   telemetry.ErrHistoryInvalidResolution,
		},
		{
			name:      "explicit bucket resolution too fine (1s bucket on 1001s window with maxPoints 1000)",
			tr:        telemetry.TimeRange{From: now, To: now.Add(1001 * time.Second)},
			res:       telemetry.Resolution1s,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: 1000,
			wantErr:   telemetry.ErrHistoryResolutionTooFine,
		},
		{
			name:      "explicit bucket resolution acceptable (1s bucket on 1000s window with maxPoints 1000)",
			tr:        telemetry.TimeRange{From: now, To: now.Add(1000 * time.Second)},
			res:       telemetry.Resolution1s,
			maxRange:  maxRange,
			rawWindow: rawWindow,
			maxPoints: 1000,
			wantErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := telemetry.ValidateHistoryTimeRange(tt.tr, tt.res, tt.maxRange, tt.rawWindow, tt.maxPoints)
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("ValidateHistoryTimeRange() unexpected error = %v", err)
				}
			} else {
				if err != tt.wantErr {
					t.Errorf("ValidateHistoryTimeRange() error = %v, want %v", err, tt.wantErr)
				}
			}
		})
	}
}
