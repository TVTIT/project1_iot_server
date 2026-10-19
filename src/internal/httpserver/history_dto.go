package httpserver

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

const rfc3339MicroUTC = "2006-01-02T15:04:05.000000Z"

func formatRFC3339Micro(t time.Time) string {
	return t.UTC().Format(rfc3339MicroUTC)
}

// TelemetryHistoryResponseDTO represents the wire format of GET /v1/telemetry/history
// conforming to Stage 4 read contract sections 8.6 and 8.7.
type TelemetryHistoryResponseDTO struct {
	GatewayID  string  `json:"gateway_id"`
	SensorID   string  `json:"sensor_id"`
	Unit       *string `json:"unit"`
	From       string  `json:"from"`
	To         string  `json:"to"`
	Mode       string  `json:"mode"`
	Resolution string  `json:"resolution"`
	PointCount int     `json:"point_count"`
	Points     any     `json:"points"`
}

// RawPoints returns the typed points if Mode == "raw", or (nil, false) otherwise.
func (d *TelemetryHistoryResponseDTO) RawPoints() ([]RawHistoryPointDTO, bool) {
	if d == nil {
		return nil, false
	}
	pts, ok := d.Points.([]RawHistoryPointDTO)
	return pts, ok
}

// BucketPoints returns the typed points if Mode == "bucket", or (nil, false) otherwise.
func (d *TelemetryHistoryResponseDTO) BucketPoints() ([]BucketHistoryPointDTO, bool) {
	if d == nil {
		return nil, false
	}
	pts, ok := d.Points.([]BucketHistoryPointDTO)
	return pts, ok
}

// RawHistoryPointDTO is one point in a raw history series.
type RawHistoryPointDTO struct {
	MeasuredAt  string  `json:"measured_at"`
	Value       float64 `json:"value"`
	SequenceNum string  `json:"sequence_num"`
	BootID      *string `json:"boot_id"`
	MessageID   *string `json:"message_id"`
	SampleIndex *int32  `json:"sample_index"`
}

// BucketHistoryPointDTO is one bucket aggregation in a downsampled history series.
type BucketHistoryPointDTO struct {
	BucketStart string  `json:"bucket_start"`
	Avg         float64 `json:"avg"`
	Min         float64 `json:"min"`
	Max         float64 `json:"max"`
	Count       string  `json:"count"`
}

// NewTelemetryHistoryResponseDTO maps a TelemetryHistoryResult into the wire response DTO,
// validating identity, timestamps, mode, resolution, and point branch consistency.
func NewTelemetryHistoryResponseDTO(res *telemetry.TelemetryHistoryResult) (*TelemetryHistoryResponseDTO, error) {
	if res == nil {
		return nil, errors.New("telemetry history result must not be nil")
	}

	if err := gateway.ValidateGatewayID(res.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in history result: %w", err)
	}
	if err := gateway.ValidateSensorID(res.SensorID); err != nil {
		return nil, fmt.Errorf("invalid sensor_id in history result: %w", err)
	}

	if !res.From.Before(res.To) {
		return nil, errors.New("from timestamp must be before to timestamp")
	}
	fromYear := res.From.UTC().Year()
	toYear := res.To.UTC().Year()
	if fromYear < 1 || fromYear > 9999 || toYear < 1 || toYear > 9999 {
		return nil, errors.New("from and to timestamp years must be in 0001-9999")
	}

	if res.PointCount < 0 {
		return nil, fmt.Errorf("negative point_count: %d", res.PointCount)
	}

	if res.Resolution == string(telemetry.ResolutionAuto) {
		return nil, errors.New("auto resolution must not be reported as effective resolution")
	}

	switch res.Mode {
	case string(telemetry.ResolutionRaw):
		if res.Resolution != string(telemetry.ResolutionRaw) {
			return nil, fmt.Errorf("raw mode must have resolution raw, got %q", res.Resolution)
		}
		if len(res.BucketPoints) > 0 {
			return nil, fmt.Errorf("raw mode history result must not contain bucket points (got %d)", len(res.BucketPoints))
		}
		if len(res.RawPoints) != res.PointCount {
			return nil, fmt.Errorf("point_count (%d) does not match raw points length (%d)", res.PointCount, len(res.RawPoints))
		}

		rawDTOs := make([]RawHistoryPointDTO, len(res.RawPoints))
		for i, pt := range res.RawPoints {
			ptYear := pt.MeasuredAt.UTC().Year()
			if ptYear < 1 || ptYear > 9999 {
				return nil, fmt.Errorf("raw point %d: measured_at year must be in 0001-9999", i)
			}
			if math.IsNaN(pt.Value) || math.IsInf(pt.Value, 0) {
				return nil, fmt.Errorf("raw point %d: value must be a finite number", i)
			}
			if pt.SampleIndex != nil && *pt.SampleIndex < 0 {
				return nil, fmt.Errorf("raw point %d: sample_index must not be negative", i)
			}

			var bootIDStr *string
			if pt.BootID != nil {
				s := pt.BootID.String()
				bootIDStr = &s
			}
			var msgIDStr *string
			if pt.MessageID != nil {
				s := pt.MessageID.String()
				msgIDStr = &s
			}

			rawDTOs[i] = RawHistoryPointDTO{
				MeasuredAt:  formatRFC3339Micro(pt.MeasuredAt),
				Value:       pt.Value,
				SequenceNum: strconv.FormatInt(pt.SequenceNum, 10),
				BootID:      bootIDStr,
				MessageID:   msgIDStr,
				SampleIndex: pt.SampleIndex,
			}
		}

		return &TelemetryHistoryResponseDTO{
			GatewayID:  res.GatewayID,
			SensorID:   res.SensorID,
			Unit:       res.Unit,
			From:       formatRFC3339Micro(res.From),
			To:         formatRFC3339Micro(res.To),
			Mode:       string(telemetry.ResolutionRaw),
			Resolution: string(telemetry.ResolutionRaw),
			PointCount: len(rawDTOs),
			Points:     rawDTOs,
		}, nil

	case "bucket":
		dur, ok := telemetry.ResolutionDuration(telemetry.Resolution(res.Resolution))
		if !ok || dur <= 0 {
			return nil, fmt.Errorf("invalid effective bucket resolution: %q", res.Resolution)
		}
		if len(res.RawPoints) > 0 {
			return nil, fmt.Errorf("bucket mode history result must not contain raw points (got %d)", len(res.RawPoints))
		}
		if len(res.BucketPoints) != res.PointCount {
			return nil, fmt.Errorf("point_count (%d) does not match bucket points length (%d)", res.PointCount, len(res.BucketPoints))
		}

		bucketDTOs := make([]BucketHistoryPointDTO, len(res.BucketPoints))
		for i, pt := range res.BucketPoints {
			ptYear := pt.BucketStart.UTC().Year()
			if ptYear < 1 || ptYear > 9999 {
				return nil, fmt.Errorf("bucket point %d: bucket_start year must be in 0001-9999", i)
			}
			if math.IsNaN(pt.Avg) || math.IsInf(pt.Avg, 0) {
				return nil, fmt.Errorf("bucket point %d: avg must be a finite number", i)
			}
			if math.IsNaN(pt.Min) || math.IsInf(pt.Min, 0) {
				return nil, fmt.Errorf("bucket point %d: min must be a finite number", i)
			}
			if math.IsNaN(pt.Max) || math.IsInf(pt.Max, 0) {
				return nil, fmt.Errorf("bucket point %d: max must be a finite number", i)
			}
			if pt.Count < 0 {
				return nil, fmt.Errorf("bucket point %d: count must not be negative", i)
			}

			bucketDTOs[i] = BucketHistoryPointDTO{
				BucketStart: formatRFC3339Micro(pt.BucketStart),
				Avg:         pt.Avg,
				Min:         pt.Min,
				Max:         pt.Max,
				Count:       strconv.FormatInt(pt.Count, 10),
			}
		}

		return &TelemetryHistoryResponseDTO{
			GatewayID:  res.GatewayID,
			SensorID:   res.SensorID,
			Unit:       res.Unit,
			From:       formatRFC3339Micro(res.From),
			To:         formatRFC3339Micro(res.To),
			Mode:       "bucket",
			Resolution: res.Resolution,
			PointCount: len(bucketDTOs),
			Points:     bucketDTOs,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported history mode: %q", res.Mode)
	}
}
