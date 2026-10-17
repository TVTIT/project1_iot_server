package httpserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/digitaltwin/mapper"
	"iot-platform/internal/gateway"
)

// TwinSummaryDTO represents a single entity summary in the list projection.
type TwinSummaryDTO struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	Name      *string `json:"name"`
	GatewayID string  `json:"gateway_id"`
	UpdatedAt string  `json:"updated_at"`
}

// TwinListResponseDTO represents the wire format of GET /v1/digital-twins.
type TwinListResponseDTO struct {
	Items      []TwinSummaryDTO `json:"items"`
	NextCursor *string          `json:"next_cursor"`
}

// NewTwinListResponseDTO maps a digitaltwin.TwinListResult to the list response wire DTO,
// validating entity identity, types, and timestamps.
func NewTwinListResponseDTO(res *digitaltwin.TwinListResult) (*TwinListResponseDTO, error) {
	if res == nil {
		return nil, errors.New("twin list result must not be nil")
	}

	items := make([]TwinSummaryDTO, 0, len(res.Items))
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

		updatedAtStr, err := mapper.FormatRFC3339Micro(item.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("item %d: invalid updated_at: %w", i, err)
		}

		items = append(items, TwinSummaryDTO{
			ID:        item.ID,
			Type:      string(item.Type),
			Name:      item.Name,
			GatewayID: item.GatewayID,
			UpdatedAt: updatedAtStr,
		})
	}

	var nextCursor *string
	if res.NextCursor != nil {
		s := *res.NextCursor
		nextCursor = &s
	}

	return &TwinListResponseDTO{
		Items:      items,
		NextCursor: nextCursor,
	}, nil
}

// TwinDetailResponseDTO represents the JSON-LD entity representation for GET /v1/digital-twins/{entity_id}.
type TwinDetailResponseDTO struct {
	Context          any                         `json:"@context"`
	ID               string                      `json:"id"`
	Type             string                      `json:"type"`
	Name             mapper.PropertyDTO          `json:"name"`
	Description      *mapper.PropertyDTO         `json:"description,omitempty"`
	Unit             *mapper.PropertyDTO         `json:"unit,omitempty"`
	GatewayID        string                      `json:"gateway_id"`
	HasSensor        []mapper.RelationshipDTO    `json:"hasSensor,omitempty"`
	RelationshipPage *mapper.RelationshipPageDTO `json:"relationship_page,omitempty"`
}

// MarshalJSON customizes serialization so that hasSensor is always an array (even if empty) for Gateways,
// while remaining omitted for Sensors.
func (d TwinDetailResponseDTO) MarshalJSON() ([]byte, error) {
	type Alias TwinDetailResponseDTO
	if d.Type == string(digitaltwin.EntityTypeGateway) {
		aux := struct {
			Alias
			HasSensor []mapper.RelationshipDTO `json:"hasSensor"`
		}{
			Alias:     Alias(d),
			HasSensor: d.HasSensor,
		}
		if aux.HasSensor == nil {
			aux.HasSensor = make([]mapper.RelationshipDTO, 0)
		}
		return json.Marshal(aux)
	}

	aux := struct {
		Alias
		HasSensor any `json:"hasSensor,omitempty"`
	}{
		Alias:     Alias(d),
		HasSensor: nil,
	}
	return json.Marshal(aux)
}

// NewTwinDetailResponseDTO maps a digitaltwin.TwinDetailResult to the entity detail JSON-LD DTO,
// strictly checking entity type rules, URN validity, and relationship integrity.
func NewTwinDetailResponseDTO(res *digitaltwin.TwinDetailResult) (*TwinDetailResponseDTO, error) {
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

		descProp := &mapper.PropertyDTO{
			Type:       "Property",
			Value:      res.Description,
			ModifiedAt: modifiedAtStr,
		}

		hasSensor := make([]mapper.RelationshipDTO, 0, len(res.Relationships))
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

			createdAtStr, err := mapper.FormatRFC3339Micro(rel.CreatedAt)
			if err != nil {
				return nil, fmt.Errorf("relationship %d: invalid created_at: %w", i, err)
			}

			hasSensor = append(hasSensor, mapper.NewRelationship(rel.TargetURN, createdAtStr))
		}

		var relPage *mapper.RelationshipPageDTO
		if res.RelationshipPage != nil {
			if res.RelationshipPage.Count != len(res.Relationships) {
				return nil, fmt.Errorf("relationship_page.count (%d) does not match relationships length (%d)", res.RelationshipPage.Count, len(res.Relationships))
			}
			relPage = &mapper.RelationshipPageDTO{
				RelationshipType: res.RelationshipPage.RelationshipType,
				Limit:            res.RelationshipPage.Limit,
				Count:            res.RelationshipPage.Count,
				Partial:          res.RelationshipPage.Partial,
				NextCursor:       res.RelationshipPage.NextCursor,
			}
		} else {
			relPage = &mapper.RelationshipPageDTO{
				RelationshipType: "hasSensor",
				Limit:            len(hasSensor),
				Count:            len(hasSensor),
				Partial:          false,
				NextCursor:       nil,
			}
		}

		return &TwinDetailResponseDTO{
			Context:          mapper.ProjectInlineContext(),
			ID:               res.ID,
			Type:             string(res.Type),
			Name:             mapper.NewProperty(res.Name, modifiedAtStr),
			Description:      descProp,
			Unit:             nil,
			GatewayID:        res.GatewayID,
			HasSensor:        hasSensor,
			RelationshipPage: relPage,
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

		unitProp := &mapper.PropertyDTO{
			Type:       "Property",
			Value:      res.Unit,
			ModifiedAt: modifiedAtStr,
		}

		return &TwinDetailResponseDTO{
			Context:          mapper.ProjectInlineContext(),
			ID:               res.ID,
			Type:             string(res.Type),
			Name:             mapper.NewProperty(res.Name, modifiedAtStr),
			Description:      nil,
			Unit:             unitProp,
			GatewayID:        res.GatewayID,
			HasSensor:        nil,
			RelationshipPage: nil,
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
	LastReportedAt  *string         `json:"last_reported_at"`
	LastDesiredAt   *string         `json:"last_desired_at"`
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
