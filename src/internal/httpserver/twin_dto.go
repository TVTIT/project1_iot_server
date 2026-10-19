package httpserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/digitaltwin/mapper"
	"iot-platform/internal/gateway"
)

// TwinSummaryDTO represents a single entity summary in the list projection.
type TwinSummaryDTO struct {
	Context    any                 `json:"@context"`
	ID         string              `json:"id"`
	Type       string              `json:"type"`
	Name       *mapper.PropertyDTO `json:"name,omitempty"`
	GatewayID  mapper.PropertyDTO  `json:"gateway_id"`
	ModifiedAt string              `json:"modifiedAt"`
}

// TwinListResponseDTO represents the wire format of GET /v1/digital-twins (root summary array).
type TwinListResponseDTO = []TwinSummaryDTO

// twinSummaryProjection is the validated, presentation-neutral list item shared by the
// normalized JSON-LD list and the flat project simplified list. Both DTO constructors
// derive from it so the two representations cannot drift on identity, type, gateway
// linkage, or timestamp validation.
type twinSummaryProjection struct {
	ID         string
	Type       string
	Name       *string
	GatewayID  string
	ModifiedAt string
}

// projectTwinSummaries validates entity identity, type, gateway linkage, and timestamp
// for every list item and returns the shared projection.
func projectTwinSummaries(res *digitaltwin.TwinListResult) ([]twinSummaryProjection, error) {
	if res == nil {
		return nil, errors.New("twin list result must not be nil")
	}

	projections := make([]twinSummaryProjection, 0, len(res.Items))
	for i, item := range res.Items {
		parsed, err := digitaltwin.ParseEntityURN(item.ID)
		if err != nil {
			return nil, fmt.Errorf("item %d: invalid entity id %q: %w", i, item.ID, err)
		}
		if item.Type != parsed.Type || !item.Type.Valid() {
			return nil, fmt.Errorf("item %d: entity type %q does not match URN %q", i, item.Type, item.ID)
		}
		if item.GatewayID != parsed.GatewayID {
			return nil, fmt.Errorf("item %d: gateway_id %q does not match URN %q", i, item.GatewayID, item.ID)
		}
		if err := gateway.ValidateGatewayID(item.GatewayID); err != nil {
			return nil, fmt.Errorf("item %d: invalid gateway_id: %w", i, err)
		}

		modifiedAtStr, err := mapper.FormatRFC3339Micro(item.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("item %d: invalid updated_at: %w", i, err)
		}

		projections = append(projections, twinSummaryProjection{
			ID:         item.ID,
			Type:       string(item.Type),
			Name:       item.Name,
			GatewayID:  item.GatewayID,
			ModifiedAt: modifiedAtStr,
		})
	}

	return projections, nil
}

// NewTwinListResponseDTO maps a digitaltwin.TwinListResult to the normalized JSON-LD list
// response wire DTO, validating entity identity, types, and timestamps.
func NewTwinListResponseDTO(res *digitaltwin.TwinListResult) ([]TwinSummaryDTO, error) {
	projections, err := projectTwinSummaries(res)
	if err != nil {
		return nil, err
	}

	items := make([]TwinSummaryDTO, 0, len(projections))
	for _, p := range projections {
		var nameProp *mapper.PropertyDTO
		if p.Name != nil {
			nameProp = &mapper.PropertyDTO{
				Type:  "Property",
				Value: *p.Name,
			}
		}

		gwProp := mapper.PropertyDTO{
			Type:  "Property",
			Value: p.GatewayID,
		}

		items = append(items, TwinSummaryDTO{
			Context:    mapper.ProjectContextArray(),
			ID:         p.ID,
			Type:       p.Type,
			Name:       nameProp,
			GatewayID:  gwProp,
			ModifiedAt: p.ModifiedAt,
		})
	}

	return items, nil
}

// TwinSummaryFlatDTO is the flat project simplified SUMMARY representation used by
// GET /v1/digital-twins?format=simplified. It is neither the NGSI-LD JSON-LD list nor
// the detail simplified STATE view: no @context, Property wrappers, state, history,
// or relationships.
type TwinSummaryFlatDTO struct {
	ID         string  `json:"id"`
	Type       string  `json:"type"`
	Name       *string `json:"name,omitempty"`
	GatewayID  string  `json:"gateway_id"`
	ModifiedAt string  `json:"modifiedAt"`
}

// TwinSimplifiedListResponseDTO is the wire format of the simplified list (root array).
type TwinSimplifiedListResponseDTO = []TwinSummaryFlatDTO

// NewTwinSimplifiedListResponseDTO maps a digitaltwin.TwinListResult to the flat
// simplified list DTO, reusing the shared validation so normalized and simplified
// output cannot drift. An empty result yields a non-nil empty slice ([] not null).
func NewTwinSimplifiedListResponseDTO(res *digitaltwin.TwinListResult) ([]TwinSummaryFlatDTO, error) {
	projections, err := projectTwinSummaries(res)
	if err != nil {
		return nil, err
	}

	items := make([]TwinSummaryFlatDTO, 0, len(projections))
	for _, p := range projections {
		items = append(items, TwinSummaryFlatDTO(p))
	}

	return items, nil
}

// GatewayHasSensorDTO represents the NGSI-LD v1.9.1 hasSensor relationship on a Gateway.
type GatewayHasSensorDTO struct {
	Type       string   `json:"type"`
	Object     []string `json:"object"`
	ObjectType string   `json:"objectType"`
}

// SensorIsHostedByDTO represents the NGSI-LD v1.9.1 isHostedBy relationship on a Sensor.
type SensorIsHostedByDTO struct {
	Type   string `json:"type"`
	Object string `json:"object"`
}

// TwinDetailResponseDTO represents the JSON-LD entity representation for GET /v1/digital-twins/{entity_id}.
type TwinDetailResponseDTO struct {
	Context          any                  `json:"@context"`
	ID               string               `json:"id"`
	Type             string               `json:"type"`
	Name             mapper.PropertyDTO   `json:"name"`
	Description      *mapper.PropertyDTO  `json:"description,omitempty"`
	IsHostedBy       *SensorIsHostedByDTO `json:"isHostedBy,omitempty"`
	HasSensor        *GatewayHasSensorDTO `json:"hasSensor,omitempty"`
	MeasurementUnit  *mapper.PropertyDTO  `json:"measurementUnit,omitempty"`
	Measurement      *mapper.PropertyDTO  `json:"measurement,omitempty"`
	SampleIntervalUS *mapper.PropertyDTO  `json:"sample_interval_us,omitempty"`
}

// NewTwinDetailResponseDTO maps a digitaltwin.TwinDetailResult to the entity detail JSON-LD DTO,
// strictly checking entity type rules, URN validity, and relationship integrity.
func NewTwinDetailResponseDTO(res *digitaltwin.TwinDetailResult, stateRes ...*digitaltwin.TwinStateResult) (*TwinDetailResponseDTO, error) {
	if res == nil {
		return nil, errors.New("twin detail result must not be nil")
	}

	parsed, err := digitaltwin.ParseEntityURN(res.ID)
	if err != nil {
		return nil, fmt.Errorf("invalid entity id %q: %w", res.ID, err)
	}
	if res.Type != parsed.Type || !res.Type.Valid() {
		return nil, fmt.Errorf("entity type %q does not match URN %q", res.Type, res.ID)
	}
	if res.GatewayID != parsed.GatewayID {
		return nil, fmt.Errorf("gateway_id %q does not match URN %q", res.GatewayID, res.ID)
	}
	if err := gateway.ValidateGatewayID(res.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in detail result: %w", err)
	}
	if res.Name == "" {
		return nil, errors.New("entity name must not be empty")
	}

	modifiedAtStr, err := mapper.FormatRFC3339Micro(res.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("invalid updated_at: %w", err)
	}

	switch res.Type {
	case digitaltwin.EntityTypeGateway:
		if res.Unit != nil {
			return nil, errors.New("gateway entity must not have unit")
		}

		var descProp *mapper.PropertyDTO
		if res.Description != nil {
			descProp = &mapper.PropertyDTO{
				Type:       "Property",
				Value:      *res.Description,
				ModifiedAt: modifiedAtStr,
			}
		}

		var hasSensorDTO *GatewayHasSensorDTO
		if res.RelationshipPage != nil && res.RelationshipPage.Count != len(res.Relationships) {
			return nil, fmt.Errorf("relationship_page.count (%d) does not match relationships length (%d)", res.RelationshipPage.Count, len(res.Relationships))
		}
		if len(res.Relationships) > 0 {
			targetURNs := make([]string, 0, len(res.Relationships))
			for i, rel := range res.Relationships {
				relParsed, err := digitaltwin.ParseEntityURN(rel.TargetURN)
				if err != nil {
					return nil, fmt.Errorf("relationship %d: invalid target URN %q: %w", i, rel.TargetURN, err)
				}
				if relParsed.Type != digitaltwin.EntityTypeSensor {
					return nil, fmt.Errorf("relationship %d: target must be Sensor, got %q", i, rel.TargetURN)
				}
				if relParsed.GatewayID != res.GatewayID {
					return nil, fmt.Errorf("relationship %d: target gateway %q does not match parent %q", i, relParsed.GatewayID, res.GatewayID)
				}
				targetURNs = append(targetURNs, rel.TargetURN)
			}
			hasSensorDTO = &GatewayHasSensorDTO{
				Type:       "Relationship",
				Object:     targetURNs,
				ObjectType: "Sensor",
			}
		}

		return &TwinDetailResponseDTO{
			Context:     mapper.ProjectContextArray(),
			ID:          res.ID,
			Type:        string(res.Type),
			Name:        mapper.NewProperty(res.Name, modifiedAtStr),
			Description: descProp,
			HasSensor:   hasSensorDTO,
		}, nil

	case digitaltwin.EntityTypeSensor:
		if res.Description != nil {
			return nil, errors.New("sensor entity must not have description")
		}
		if len(res.Relationships) > 0 {
			return nil, errors.New("sensor entity must not have outgoing relationships")
		}
		if res.RelationshipPage != nil {
			return nil, errors.New("sensor entity must not have relationship_page")
		}

		isHostedByDTO := &SensorIsHostedByDTO{
			Type:   "Relationship",
			Object: "urn:ngsi-ld:Gateway:" + res.GatewayID,
		}

		var mappedUnitCode *string
		if res.Unit != nil && *res.Unit != "" {
			if code, ok := mapper.MapUNCEFACTUnit(*res.Unit); ok {
				mappedUnitCode = &code
			}
		}

		var measurementProp *mapper.PropertyDTO
		var sampleIntervalProp *mapper.PropertyDTO

		if len(stateRes) > 0 && stateRes[0] != nil && len(stateRes[0].ReportedState) > 0 {
			var stateMap map[string]any
			dec := json.NewDecoder(bytes.NewReader(stateRes[0].ReportedState))
			dec.UseNumber()
			if err := dec.Decode(&stateMap); err == nil {
				var formattedObservedAt string
				if obsAtRaw, ok := stateMap["observed_at"].(string); ok && obsAtRaw != "" {
					if t, err := time.Parse(time.RFC3339Nano, obsAtRaw); err == nil {
						if formatted, err := mapper.FormatRFC3339Micro(t); err == nil {
							formattedObservedAt = formatted
						}
					}
				}

				if formattedObservedAt != "" {
					// Numeric telemetry validation: value must be a valid json.Number (rejecting strings, bools, objects)
					if valRaw, ok := stateMap["value"]; ok && valRaw != nil {
						if num, ok := valRaw.(json.Number); ok {
							if _, err := num.Float64(); err == nil {
								p := mapper.NewObservationPropertyWithUnit(num, formattedObservedAt, mappedUnitCode)
								measurementProp = &p
							}
						}
					}

					// Interval bounds validation: sample_interval_us must be a positive number
					if intRaw, ok := stateMap["sample_interval_us"]; ok && intRaw != nil {
						if num, ok := intRaw.(json.Number); ok {
							if parsedInt, err := num.Int64(); err == nil && parsedInt > 0 {
								p := mapper.NewObservationProperty(num, "", formattedObservedAt)
								sampleIntervalProp = &p
							} else if parsedFloat, err := num.Float64(); err == nil && parsedFloat > 0 {
								p := mapper.NewObservationProperty(num, "", formattedObservedAt)
								sampleIntervalProp = &p
							}
						}
					}
				}
			}
		}

		// Root unit representation:
		// - unitCode is Property metadata on measurement, not an entity attribute.
		// - For unobserved sensors, expose measurementUnit with original unit for discoverability.
		// - For unknown units, expose measurementUnit.
		var measUnitProp *mapper.PropertyDTO
		if res.Unit != nil && *res.Unit != "" {
			if measurementProp == nil || mappedUnitCode == nil {
				p := mapper.NewProperty(*res.Unit, modifiedAtStr)
				measUnitProp = &p
			}
		}

		return &TwinDetailResponseDTO{
			Context:          mapper.ProjectContextArray(),
			ID:               res.ID,
			Type:             string(res.Type),
			Name:             mapper.NewProperty(res.Name, modifiedAtStr),
			IsHostedBy:       isHostedByDTO,
			MeasurementUnit:  measUnitProp,
			Measurement:      measurementProp,
			SampleIntervalUS: sampleIntervalProp,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported entity type: %q", res.Type)
	}
}

// TwinStateResponseDTO represents the wire format of GET /v1/digital-twins/{entity_id}/state.
type TwinStateResponseDTO struct {
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	GatewayID       string          `json:"gateway_id"`
	ReportedState   json.RawMessage `json:"reported_state"`
	DesiredState    json.RawMessage `json:"desired_state"`
	ReportedVersion string          `json:"reported_version"`
	DesiredVersion  string          `json:"desired_version"`
	LastReportedAt  *string         `json:"last_reported_at,omitempty"`
	LastDesiredAt   *string         `json:"last_desired_at,omitempty"`
	UpdatedAt       string          `json:"updated_at"`
}

// NewTwinStateResponseDTO maps a digitaltwin.TwinStateResult to the state response wire DTO,
// preserving exact raw bytes of reported and desired JSONB states without float64 distortion.
func NewTwinStateResponseDTO(res *digitaltwin.TwinStateResult) (*TwinStateResponseDTO, error) {
	if res == nil {
		return nil, errors.New("twin state result must not be nil")
	}

	parsed, err := digitaltwin.ParseEntityURN(res.ID)
	if err != nil {
		return nil, fmt.Errorf("invalid entity id %q: %w", res.ID, err)
	}
	if res.Type != parsed.Type || !res.Type.Valid() {
		return nil, fmt.Errorf("entity type %q does not match URN %q", res.Type, res.ID)
	}
	if res.GatewayID != parsed.GatewayID {
		return nil, fmt.Errorf("gateway_id %q does not match URN %q", res.GatewayID, res.ID)
	}
	if err := gateway.ValidateGatewayID(res.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in state result: %w", err)
	}

	if res.ReportedVersion < 0 {
		return nil, fmt.Errorf("reported_version must not be negative: %d", res.ReportedVersion)
	}
	if res.DesiredVersion < 0 {
		return nil, fmt.Errorf("desired_version must not be negative: %d", res.DesiredVersion)
	}

	updatedAtStr, err := mapper.FormatRFC3339Micro(res.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("invalid updated_at: %w", err)
	}

	var lastReportedStr *string
	if res.LastReportedAt != nil {
		s, err := mapper.FormatRFC3339Micro(*res.LastReportedAt)
		if err != nil {
			return nil, fmt.Errorf("invalid last_reported_at: %w", err)
		}
		lastReportedStr = &s
	}

	var lastDesiredStr *string
	if res.LastDesiredAt != nil {
		s, err := mapper.FormatRFC3339Micro(*res.LastDesiredAt)
		if err != nil {
			return nil, fmt.Errorf("invalid last_desired_at: %w", err)
		}
		lastDesiredStr = &s
	}

	reportedState := res.ReportedState
	if len(reportedState) == 0 {
		reportedState = json.RawMessage("{}")
	} else if !json.Valid(reportedState) {
		return nil, errors.New("reported_state is not valid JSON")
	}

	desiredState := res.DesiredState
	if len(desiredState) == 0 {
		desiredState = json.RawMessage("{}")
	} else if !json.Valid(desiredState) {
		return nil, errors.New("desired_state is not valid JSON")
	}

	return &TwinStateResponseDTO{
		ID:              res.ID,
		Type:            string(res.Type),
		GatewayID:       res.GatewayID,
		ReportedState:   reportedState,
		DesiredState:    desiredState,
		ReportedVersion: strconv.FormatInt(res.ReportedVersion, 10),
		DesiredVersion:  strconv.FormatInt(res.DesiredVersion, 10),
		LastReportedAt:  lastReportedStr,
		LastDesiredAt:   lastDesiredStr,
		UpdatedAt:       updatedAtStr,
	}, nil
}
