package httpserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// TemporalRawPointDTO represents one typed observation in a temporal raw history series.
type TemporalRawPointDTO struct {
	ObservedAt string          `json:"observed_at"`
	ValueType  string          `json:"value_type"`
	Value      any             `json:"value"`
	Metadata   json.RawMessage `json:"metadata"`
	MessageID  *string         `json:"message_id"`
}

// TwinHistoryResponseDTO represents the wire format of GET /v1/digital-twins/{entity_id}/history
// conforming to Stage 4 read contract sections 8.8-8.12.
type TwinHistoryResponseDTO struct {
	EntityID   string  `json:"entity_id"`
	Property   string  `json:"property"`
	Unit       *string `json:"unit"`
	From       string  `json:"from"`
	To         string  `json:"to"`
	Mode       string  `json:"mode"`
	Resolution string  `json:"resolution"`
	PointCount int     `json:"point_count"`
	Points     any     `json:"points"`
}

// RawPoints returns the typed delegated raw points if Points contains []RawHistoryPointDTO, or (nil, false) otherwise.
func (d *TwinHistoryResponseDTO) RawPoints() ([]RawHistoryPointDTO, bool) {
	if d == nil {
		return nil, false
	}
	pts, ok := d.Points.([]RawHistoryPointDTO)
	return pts, ok
}

// BucketPoints returns the typed bucket points if Points contains []BucketHistoryPointDTO, or (nil, false) otherwise.
func (d *TwinHistoryResponseDTO) BucketPoints() ([]BucketHistoryPointDTO, bool) {
	if d == nil {
		return nil, false
	}
	pts, ok := d.Points.([]BucketHistoryPointDTO)
	return pts, ok
}

// TemporalRawPoints returns the typed temporal raw points if Points contains []TemporalRawPointDTO, or (nil, false) otherwise.
func (d *TwinHistoryResponseDTO) TemporalRawPoints() ([]TemporalRawPointDTO, bool) {
	if d == nil {
		return nil, false
	}
	pts, ok := d.Points.([]TemporalRawPointDTO)
	return pts, ok
}

// NewTwinHistoryResponseDTO maps a digitaltwin.TwinHistoryResult to the wire response DTO,
// validating entity identity, timestamps, mode, resolution, and point branch consistency.
func NewTwinHistoryResponseDTO(res *digitaltwin.TwinHistoryResult) (*TwinHistoryResponseDTO, error) {
	if res == nil {
		return nil, errors.New("twin history result must not be nil")
	}

	parsed, err := digitaltwin.ParseEntityURN(res.EntityID)
	if err != nil {
		return nil, fmt.Errorf("invalid entity id in history result %q: %w", res.EntityID, err)
	}
	if parsed.Type != digitaltwin.EntityTypeSensor {
		return nil, fmt.Errorf("history is only supported for Sensor entities, got %q (%q)", parsed.Type, res.EntityID)
	}
	if err := gateway.ValidateGatewayID(parsed.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in entity URN: %w", err)
	}
	if err := gateway.ValidateSensorID(parsed.SensorID); err != nil {
		return nil, fmt.Errorf("invalid sensor_id in entity URN: %w", err)
	}

	propStr := string(res.Property)
	if propStr == "" {
		return nil, errors.New("property in history result must not be empty")
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

	if res.Property == digitaltwin.SelectorValue {
		if res.TelemetryResult == nil {
			return nil, errors.New("property value requires non-nil delegated TelemetryResult")
		}
		if len(res.TemporalPoints) > 0 || len(res.TemporalBuckets) > 0 {
			return nil, errors.New("delegated history result must not contain active temporal points branch")
		}

		telemDTO, err := NewTelemetryHistoryResponseDTO(res.TelemetryResult)
		if err != nil {
			return nil, fmt.Errorf("invalid delegated telemetry result: %w", err)
		}

		if parsed.GatewayID != res.TelemetryResult.GatewayID || parsed.SensorID != res.TelemetryResult.SensorID {
			return nil, fmt.Errorf("entity URN (%s/%s) does not match delegated telemetry identity (%s/%s)",
				parsed.GatewayID, parsed.SensorID, res.TelemetryResult.GatewayID, res.TelemetryResult.SensorID)
		}

		return &TwinHistoryResponseDTO{
			EntityID:   res.EntityID,
			Property:   propStr,
			Unit:       telemDTO.Unit,
			From:       telemDTO.From,
			To:         telemDTO.To,
			Mode:       telemDTO.Mode,
			Resolution: telemDTO.Resolution,
			PointCount: telemDTO.PointCount,
			Points:     telemDTO.Points,
		}, nil
	}

	if res.TelemetryResult != nil {
		return nil, errors.New("non-value property history result must not contain delegated TelemetryResult")
	}

	switch res.Mode {
	case string(telemetry.ResolutionRaw):
		if res.Resolution != string(telemetry.ResolutionRaw) {
			return nil, fmt.Errorf("raw mode must have resolution raw, got %q", res.Resolution)
		}
		if len(res.TemporalBuckets) > 0 {
			return nil, fmt.Errorf("raw mode history result must not contain bucket points (got %d)", len(res.TemporalBuckets))
		}
		if len(res.TemporalPoints) != res.PointCount {
			return nil, fmt.Errorf("point_count (%d) does not match temporal points length (%d)", res.PointCount, len(res.TemporalPoints))
		}

		ptsDTO := make([]TemporalRawPointDTO, len(res.TemporalPoints))
		for i, pt := range res.TemporalPoints {
			ptYear := pt.ObservedAt.UTC().Year()
			if ptYear < 1 || ptYear > 9999 {
				return nil, fmt.Errorf("temporal point %d: observed_at year must be in 0001-9999", i)
			}

			var val any
			switch pt.ValueType {
			case "number":
				if pt.ValueNumber == nil {
					return nil, fmt.Errorf("temporal point %d: value_type is number but ValueNumber is nil", i)
				}
				if pt.ValueText != nil || pt.ValueBoolean != nil {
					return nil, fmt.Errorf("temporal point %d: exactly one value column must be non-null", i)
				}
				if math.IsNaN(*pt.ValueNumber) || math.IsInf(*pt.ValueNumber, 0) {
					return nil, fmt.Errorf("temporal point %d: value must be a finite number", i)
				}
				val = *pt.ValueNumber
			case "text":
				if pt.ValueText == nil {
					return nil, fmt.Errorf("temporal point %d: value_type is text but ValueText is nil", i)
				}
				if pt.ValueNumber != nil || pt.ValueBoolean != nil {
					return nil, fmt.Errorf("temporal point %d: exactly one value column must be non-null", i)
				}
				val = *pt.ValueText
			case "boolean":
				if pt.ValueBoolean == nil {
					return nil, fmt.Errorf("temporal point %d: value_type is boolean but ValueBoolean is nil", i)
				}
				if pt.ValueNumber != nil || pt.ValueText != nil {
					return nil, fmt.Errorf("temporal point %d: exactly one value column must be non-null", i)
				}
				val = *pt.ValueBoolean
			default:
				return nil, fmt.Errorf("temporal point %d: unsupported value_type %q", i, pt.ValueType)
			}

			metadata := pt.Metadata
			trimmed := bytes.TrimSpace(metadata)
			if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
				metadata = json.RawMessage("{}")
			} else {
				if !bytes.HasPrefix(trimmed, []byte("{")) || !bytes.HasSuffix(trimmed, []byte("}")) || !json.Valid(trimmed) {
					return nil, fmt.Errorf("temporal point %d: metadata must be a valid JSON object", i)
				}
				metadata = trimmed
			}

			var msgIDStr *string
			if pt.MessageID != nil {
				s := pt.MessageID.String()
				msgIDStr = &s
			}

			ptsDTO[i] = TemporalRawPointDTO{
				ObservedAt: formatRFC3339Micro(pt.ObservedAt),
				ValueType:  pt.ValueType,
				Value:      val,
				Metadata:   metadata,
				MessageID:  msgIDStr,
			}
		}

		return &TwinHistoryResponseDTO{
			EntityID:   res.EntityID,
			Property:   propStr,
			Unit:       res.Unit,
			From:       formatRFC3339Micro(res.From),
			To:         formatRFC3339Micro(res.To),
			Mode:       string(telemetry.ResolutionRaw),
			Resolution: string(telemetry.ResolutionRaw),
			PointCount: len(ptsDTO),
			Points:     ptsDTO,
		}, nil

	case "bucket":
		dur, ok := telemetry.ResolutionDuration(telemetry.Resolution(res.Resolution))
		if !ok || dur <= 0 {
			return nil, fmt.Errorf("invalid effective bucket resolution: %q", res.Resolution)
		}
		if len(res.TemporalPoints) > 0 {
			return nil, fmt.Errorf("bucket mode history result must not contain raw points (got %d)", len(res.TemporalPoints))
		}
		if len(res.TemporalBuckets) != res.PointCount {
			return nil, fmt.Errorf("point_count (%d) does not match bucket points length (%d)", res.PointCount, len(res.TemporalBuckets))
		}

		bucketDTOs := make([]BucketHistoryPointDTO, len(res.TemporalBuckets))
		for i, pt := range res.TemporalBuckets {
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

		return &TwinHistoryResponseDTO{
			EntityID:   res.EntityID,
			Property:   propStr,
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
