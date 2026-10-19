package httpserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/digitaltwin/mapper"
	"iot-platform/internal/gateway"
	"iot-platform/internal/telemetry"
)

// TemporalRawPropertyDTO represents a raw temporal Property with values array.
type TemporalRawPropertyDTO struct {
	Type   string  `json:"type"`
	Values [][]any `json:"values"`
}

// TemporalAggregatedPropertyDTO represents an aggregated temporal Property with projected method arrays.
type TemporalAggregatedPropertyDTO struct {
	Type    string             `json:"type"`
	Methods map[string][][]any `json:"-"`
}

// MarshalJSON encodes the aggregated property with each method serialized as an array of triples.
func (p TemporalAggregatedPropertyDTO) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, len(p.Methods)+1)
	m["type"] = p.Type
	for k, v := range p.Methods {
		if v == nil {
			m[k] = [][]any{}
		} else {
			m[k] = v
		}
	}
	return json.Marshal(m)
}

// TemporalHistoryResponseDTO represents the NGSI-LD V1.9.1 temporal representation.
type TemporalHistoryResponseDTO struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	PublicAttr string `json:"-"`
	Property   any    `json:"-"`
	Context    []any  `json:"@context"`
}

// MarshalJSON serializes the temporal history entity with its dynamic public attribute key.
func (d *TemporalHistoryResponseDTO) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, 4)
	m["id"] = d.ID
	m["type"] = d.Type
	m[d.PublicAttr] = d.Property
	m["@context"] = d.Context
	return json.Marshal(m)
}

// NewTemporalHistoryResponseDTO converts domain TwinHistoryResult into wire TemporalHistoryResponseDTO.
func NewTemporalHistoryResponseDTO(res *digitaltwin.TwinHistoryResult, pres *TemporalPresentationOptions) (*TemporalHistoryResponseDTO, error) {
	if res == nil {
		return nil, errors.New("twin history result must not be nil")
	}
	if pres == nil {
		return nil, errors.New("temporal presentation options must not be nil")
	}

	parsed, err := digitaltwin.ParseEntityURN(res.EntityID)
	if err != nil {
		return nil, fmt.Errorf("invalid entity id in history result %q: %w", res.EntityID, err)
	}
	if parsed.Type != digitaltwin.EntityTypeSensor {
		return nil, fmt.Errorf("temporal history is only supported for Sensor entities, got %q (%q)", parsed.Type, res.EntityID)
	}
	if err := gateway.ValidateGatewayID(parsed.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in entity URN: %w", err)
	}
	if err := gateway.ValidateSensorID(parsed.SensorID); err != nil {
		return nil, fmt.Errorf("invalid sensor_id in entity URN: %w", err)
	}

	if !res.From.Before(res.To) {
		return nil, errors.New("from timestamp must be before to timestamp")
	}
	fromYear := res.From.UTC().Year()
	toYear := res.To.UTC().Year()
	if fromYear < 1 || fromYear > 9999 || toYear < 1 || toYear > 9999 {
		return nil, errors.New("from and to timestamp years must be in 0001-9999")
	}

	if res.Resolution == string(telemetry.ResolutionAuto) {
		return nil, errors.New("auto resolution must not be reported as effective resolution")
	}

	// Branch validation: value vs sample_interval_us
	if res.Property == digitaltwin.SelectorValue {
		if res.TelemetryResult == nil {
			return nil, errors.New("property value requires non-nil delegated TelemetryResult")
		}
		if len(res.TemporalPoints) > 0 || len(res.TemporalBuckets) > 0 {
			return nil, errors.New("delegated history result must not contain active temporal points branch")
		}
		if parsed.GatewayID != res.TelemetryResult.GatewayID || parsed.SensorID != res.TelemetryResult.SensorID {
			return nil, fmt.Errorf("entity URN (%s/%s) does not match delegated telemetry identity (%s/%s)",
				parsed.GatewayID, parsed.SensorID, res.TelemetryResult.GatewayID, res.TelemetryResult.SensorID)
		}
	} else {
		if res.TelemetryResult != nil {
			return nil, errors.New("non-value property history result must not contain delegated TelemetryResult")
		}
	}

	var propObj any

	switch pres.Format {
	case "temporalValues":
		if res.Mode != string(telemetry.ResolutionRaw) {
			return nil, fmt.Errorf("raw mode must have resolution raw, got %q", res.Resolution)
		}
		if len(res.TemporalBuckets) > 0 {
			return nil, fmt.Errorf("raw mode history result must not contain bucket points (got %d)", len(res.TemporalBuckets))
		}

		var values [][]any
		if res.Property == digitaltwin.SelectorValue {
			rawPts := res.TelemetryResult.RawPoints
			values = make([][]any, 0, len(rawPts))
			for i, pt := range rawPts {
				ptYear := pt.MeasuredAt.UTC().Year()
				if ptYear < 1 || ptYear > 9999 {
					return nil, fmt.Errorf("raw point %d: measured_at year must be in 0001-9999", i)
				}
				if math.IsNaN(pt.Value) || math.IsInf(pt.Value, 0) {
					return nil, fmt.Errorf("raw point %d: value must be a finite number", i)
				}
				values = append(values, []any{pt.Value, formatRFC3339Micro(pt.MeasuredAt)})
			}
		} else {
			if len(res.TemporalPoints) != res.PointCount {
				return nil, fmt.Errorf("point_count (%d) does not match temporal points length (%d)", res.PointCount, len(res.TemporalPoints))
			}
			values = make([][]any, 0, len(res.TemporalPoints))
			for i, pt := range res.TemporalPoints {
				ptYear := pt.ObservedAt.UTC().Year()
				if ptYear < 1 || ptYear > 9999 {
					return nil, fmt.Errorf("temporal point %d: observed_at year must be in 0001-9999", i)
				}
				if pt.ValueType != "number" || pt.ValueNumber == nil {
					return nil, fmt.Errorf("temporal point %d: value_type must be number with non-nil ValueNumber", i)
				}
				if math.IsNaN(*pt.ValueNumber) || math.IsInf(*pt.ValueNumber, 0) {
					return nil, fmt.Errorf("temporal point %d: value must be a finite number", i)
				}
				values = append(values, []any{*pt.ValueNumber, formatRFC3339Micro(pt.ObservedAt)})
			}
		}

		propObj = TemporalRawPropertyDTO{
			Type:   "Property",
			Values: values,
		}

	case "aggregatedValues":
		if res.Mode != "bucket" {
			return nil, fmt.Errorf("bucket format requires bucket mode, got %q", res.Mode)
		}
		dur, ok := telemetry.ResolutionDuration(telemetry.Resolution(res.Resolution))
		if !ok || dur <= 0 {
			return nil, fmt.Errorf("invalid effective bucket resolution: %q", res.Resolution)
		}
		if len(res.TemporalPoints) > 0 {
			return nil, fmt.Errorf("bucket mode history result must not contain raw points (got %d)", len(res.TemporalPoints))
		}

		methodsMap := make(map[string][][]any, len(pres.AggrMethods))
		for _, m := range pres.AggrMethods {
			methodsMap[m] = make([][]any, 0, res.PointCount)
		}

		if res.Property == digitaltwin.SelectorValue {
			bucketPts := res.TelemetryResult.BucketPoints
			for i, pt := range bucketPts {
				ptYear := pt.BucketStart.UTC().Year()
				if ptYear < 1 || ptYear > 9999 {
					return nil, fmt.Errorf("bucket point %d: bucket_start year must be in 0001-9999", i)
				}
				if math.IsNaN(pt.Avg) || math.IsInf(pt.Avg, 0) || math.IsNaN(pt.Min) || math.IsInf(pt.Min, 0) || math.IsNaN(pt.Max) || math.IsInf(pt.Max, 0) {
					return nil, fmt.Errorf("bucket point %d: aggregate values must be finite numbers", i)
				}
				if pt.Count < 0 {
					return nil, fmt.Errorf("bucket point %d: count must not be negative", i)
				}
				startStr := formatRFC3339Micro(pt.BucketStart)
				endStr := formatRFC3339Micro(pt.BucketStart.Add(dur))

				if _, ok := methodsMap["avg"]; ok {
					methodsMap["avg"] = append(methodsMap["avg"], []any{pt.Avg, startStr, endStr})
				}
				if _, ok := methodsMap["min"]; ok {
					methodsMap["min"] = append(methodsMap["min"], []any{pt.Min, startStr, endStr})
				}
				if _, ok := methodsMap["max"]; ok {
					methodsMap["max"] = append(methodsMap["max"], []any{pt.Max, startStr, endStr})
				}
				if _, ok := methodsMap["totalCount"]; ok {
					methodsMap["totalCount"] = append(methodsMap["totalCount"], []any{pt.Count, startStr, endStr})
				}
			}
		} else {
			if len(res.TemporalBuckets) != res.PointCount {
				return nil, fmt.Errorf("point_count (%d) does not match temporal buckets length (%d)", res.PointCount, len(res.TemporalBuckets))
			}
			for i, pt := range res.TemporalBuckets {
				ptYear := pt.BucketStart.UTC().Year()
				if ptYear < 1 || ptYear > 9999 {
					return nil, fmt.Errorf("temporal bucket %d: bucket_start year must be in 0001-9999", i)
				}
				if math.IsNaN(pt.Avg) || math.IsInf(pt.Avg, 0) || math.IsNaN(pt.Min) || math.IsInf(pt.Min, 0) || math.IsNaN(pt.Max) || math.IsInf(pt.Max, 0) {
					return nil, fmt.Errorf("temporal bucket %d: aggregate values must be finite numbers", i)
				}
				if pt.Count < 0 {
					return nil, fmt.Errorf("temporal bucket %d: count must not be negative", i)
				}
				startStr := formatRFC3339Micro(pt.BucketStart)
				endStr := formatRFC3339Micro(pt.BucketStart.Add(dur))

				if _, ok := methodsMap["avg"]; ok {
					methodsMap["avg"] = append(methodsMap["avg"], []any{pt.Avg, startStr, endStr})
				}
				if _, ok := methodsMap["min"]; ok {
					methodsMap["min"] = append(methodsMap["min"], []any{pt.Min, startStr, endStr})
				}
				if _, ok := methodsMap["max"]; ok {
					methodsMap["max"] = append(methodsMap["max"], []any{pt.Max, startStr, endStr})
				}
				if _, ok := methodsMap["totalCount"]; ok {
					methodsMap["totalCount"] = append(methodsMap["totalCount"], []any{pt.Count, startStr, endStr})
				}
			}
		}

		propObj = TemporalAggregatedPropertyDTO{
			Type:    "Property",
			Methods: methodsMap,
		}

	default:
		return nil, fmt.Errorf("unsupported presentation format %q", pres.Format)
	}

	return &TemporalHistoryResponseDTO{
		ID:         res.EntityID,
		Type:       string(digitaltwin.EntityTypeSensor),
		PublicAttr: pres.PublicAttr,
		Property:   propObj,
		Context:    mapper.ProjectContextArray(),
	}, nil
}

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
