package telemetry

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// Resolution is a supported history query resolution token.
type Resolution string

// Supported history query resolution tokens.
const (
	ResolutionAuto  Resolution = "auto"
	ResolutionRaw   Resolution = "raw"
	Resolution100ms Resolution = "100ms"
	Resolution1s    Resolution = "1s"
	Resolution10s   Resolution = "10s"
	Resolution1m    Resolution = "1m"
	Resolution5m    Resolution = "5m"
	Resolution15m   Resolution = "15m"
	Resolution1h    Resolution = "1h"
	Resolution6h    Resolution = "6h"
	Resolution1d    Resolution = "1d"
)

// Valid reports whether the resolution token belongs to the project allowlist.
func (r Resolution) Valid() bool {
	switch r {
	case ResolutionAuto, ResolutionRaw, Resolution100ms, Resolution1s,
		Resolution10s, Resolution1m, Resolution5m, Resolution15m,
		Resolution1h, Resolution6h, Resolution1d:
		return true
	default:
		return false
	}
}

// ResolutionDuration returns the duration of an explicit bucket resolution token.
// For ResolutionAuto, ResolutionRaw, or unknown tokens, it returns (0, false).
func ResolutionDuration(r Resolution) (time.Duration, bool) {
	switch r {
	case Resolution100ms:
		return 100 * time.Millisecond, true
	case Resolution1s:
		return time.Second, true
	case Resolution10s:
		return 10 * time.Second, true
	case Resolution1m:
		return time.Minute, true
	case Resolution5m:
		return 5 * time.Minute, true
	case Resolution15m:
		return 15 * time.Minute, true
	case Resolution1h:
		return time.Hour, true
	case Resolution6h:
		return 6 * time.Hour, true
	case Resolution1d:
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}

// ToMicrosecondsSinceEpoch converts a time.Time to microseconds since Unix epoch
// safely without using UnixNano to prevent overflow for years 0000–9999.
func ToMicrosecondsSinceEpoch(t time.Time) int64 {
	u := t.UTC()
	return u.Unix()*1_000_000 + int64(u.Nanosecond()/1_000)
}

// floorDiv implements mathematical floor division for int64, correctly handling negative dividends.
func floorDiv(a, b int64) int64 {
	d := a / b
	r := a % b
	if r != 0 && ((r < 0) != (b < 0)) {
		d--
	}
	return d
}

// IntersectingBucketCount computes the number of buckets of width W intersecting [from, to)
// relative to Unix epoch origin (1970-01-01T00:00:00Z) using mathematical floor division:
// B(W) = floor((to - 1us - O)/W) - floor((from - O)/W) + 1.
func IntersectingBucketCount(from, to time.Time, width time.Duration) (int64, error) {
	if width <= 0 {
		return 0, errors.New("bucket width must be positive")
	}
	if !from.Before(to) {
		return 0, errors.New("invalid time range: from must be before to")
	}

	widthMicros := width.Microseconds()
	if widthMicros <= 0 {
		return 0, errors.New("bucket width in microseconds must be positive")
	}

	fromMicros := ToMicrosecondsSinceEpoch(from)
	toMinusOneMicros := ToMicrosecondsSinceEpoch(to) - 1

	b := floorDiv(toMinusOneMicros, widthMicros) - floorDiv(fromMicros, widthMicros) + 1
	return b, nil
}

// ValidateHistoryTimeRange validates time range, resolution, raw limits, and bucket bounds
// independently of HTTP transports.
func ValidateHistoryTimeRange(tr TimeRange, res Resolution, maxRange, rawWindow time.Duration, maxPoints int) error {
	if tr.From.Year() < 1 || tr.From.Year() > 9999 || tr.To.Year() < 1 || tr.To.Year() > 9999 {
		return ErrHistoryInvalidInput
	}
	fromUTC := tr.From.UTC()
	toUTC := tr.To.UTC()
	if fromUTC.Year() < 1 || fromUTC.Year() > 9999 || toUTC.Year() < 1 || toUTC.Year() > 9999 {
		return ErrHistoryInvalidInput
	}
	if fromUTC.Nanosecond()%1000 != 0 || toUTC.Nanosecond()%1000 != 0 {
		return ErrHistoryInvalidInput
	}
	if !fromUTC.Before(toUTC) {
		return ErrHistoryInvalidInput
	}

	fromSec := fromUTC.Unix()
	toSec := toUTC.Unix()
	diffSec := toSec - fromSec
	maxSec := int64(maxRange / time.Second)
	if diffSec > maxSec+1 {
		return ErrHistoryRangeTooLarge
	}

	fromMicros := ToMicrosecondsSinceEpoch(fromUTC)
	toMicros := ToMicrosecondsSinceEpoch(toUTC)
	diffMicros := toMicros - fromMicros
	if diffMicros > maxRange.Microseconds() {
		return ErrHistoryRangeTooLarge
	}

	if !res.Valid() {
		return ErrHistoryInvalidResolution
	}

	if res == ResolutionRaw {
		if diffMicros > rawWindow.Microseconds() {
			return ErrHistoryRawLimitExceeded
		}
		return nil
	}

	if res != ResolutionAuto {
		w, ok := ResolutionDuration(res)
		if !ok {
			return ErrHistoryInvalidResolution
		}
		count, err := IntersectingBucketCount(tr.From, tr.To, w)
		if err != nil {
			return ErrHistoryInvalidInput
		}
		if count > int64(maxPoints) {
			return ErrHistoryResolutionTooFine
		}
	}

	return nil
}

// TimeRange represents an immutable half-open measurement interval [From, To).
type TimeRange struct {
	From time.Time
	To   time.Time
}

// TelemetryHistoryQuery contains parameters for GET /v1/telemetry/history.
// The actor (caller identity) is passed as a separate argument to service/repository methods.
//
//nolint:revive // Telemetry prefix disambiguates from Digital Twin history query types.
type TelemetryHistoryQuery struct {
	GatewayID  string
	SensorID   string
	Range      TimeRange
	Resolution Resolution
	MaxPoints  int
}

// RawSamplePoint is one raw sensor sample in history series.
type RawSamplePoint struct {
	MeasuredAt  time.Time
	Value       float64
	SequenceNum int64
	BootID      *uuid.UUID
	MessageID   *uuid.UUID
	SampleIndex *int32
}

// BucketSamplePoint is one bucket aggregation in downsampled series.
type BucketSamplePoint struct {
	BucketStart time.Time
	Avg         float64
	Min         float64
	Max         float64
	Count       int64
}

// TelemetryHistoryResult represents the data returned by telemetry history queries.
//
//nolint:revive // Telemetry prefix disambiguates from Digital Twin history result types.
type TelemetryHistoryResult struct {
	GatewayID    string
	SensorID     string
	Unit         *string
	From         time.Time
	To           time.Time
	Mode         string
	Resolution   string
	PointCount   int
	RawPoints    []RawSamplePoint
	BucketPoints []BucketSamplePoint
}

// ResolvedSensor carries validated and authorized sensor identity.
type ResolvedSensor struct {
	GatewayID string
	SensorID  string
	Unit      *string
}

// HistoryReader executes authorized read queries for telemetry history data.
type HistoryReader interface {
	ReadTelemetryHistory(ctx context.Context, actor uuid.UUID, q TelemetryHistoryQuery) (*TelemetryHistoryResult, error)
}

// SensorResolver resolves sensor existence and authorization under the actor's membership.
type SensorResolver interface {
	ResolveSensor(ctx context.Context, actor uuid.UUID, gatewayID, sensorID string) (*ResolvedSensor, error)
}

// HistoryService defines the use-case interface for GET /v1/telemetry/history.
type HistoryService interface {
	GetTelemetryHistory(ctx context.Context, actor uuid.UUID, q TelemetryHistoryQuery) (*TelemetryHistoryResult, error)
}

// Domain errors for history queries and authorization.
var (
	ErrHistoryNilActor          = errors.New("actor must not be nil")
	ErrHistoryInvalidInput      = errors.New("invalid history query input")
	ErrHistoryNotFound          = errors.New("telemetry resource not found")
	ErrHistoryRangeTooLarge     = errors.New("time range exceeds maximum allowed")
	ErrHistoryInvalidResolution = errors.New("invalid resolution")
	ErrHistoryRawLimitExceeded  = errors.New("raw window or point limit exceeded")
	ErrHistoryResolutionTooFine = errors.New("resolution too fine for window")
	ErrHistoryDependency        = errors.New("history dependency unavailable")
	ErrHistoryUnexpected        = errors.New("unexpected history error")
)
