package httpserver

import (
	"errors"
	"fmt"

	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
)

// TwinQueryParser parses and validates HTTP query strings for Twin list and detail endpoints.
type TwinQueryParser struct {
	cfg config.TwinConfig
}

// NewTwinQueryParser constructs a TwinQueryParser with validated TwinConfig.
func NewTwinQueryParser(cfg *config.TwinConfig) (*TwinQueryParser, error) {
	if cfg == nil {
		return nil, errors.New("twin config must not be nil")
	}
	if err := config.ValidateTwinConfig(*cfg); err != nil {
		return nil, fmt.Errorf("invalid twin config: %w", err)
	}
	return &TwinQueryParser{cfg: *cfg}, nil
}

// ParseTwinListQuery parses and validates query parameters for GET /v1/digital-twins.
func (p *TwinQueryParser) ParseTwinListQuery(rawQuery string) (*digitaltwin.TwinListQuery, error) {
	allowedKeys := map[string]struct{}{
		"gateway_id":  {},
		"entity_type": {},
		"limit":       {},
		"after":       {},
	}

	params, err := parseRawQuery(rawQuery, allowedKeys)
	if err != nil {
		return nil, err
	}

	var gatewayID *string
	if val, ok := params["gateway_id"]; ok {
		if err := gateway.ValidateGatewayID(val); err != nil {
			return nil, &QueryError{
				Code:    "invalid_request",
				Message: "invalid gateway_id parameter",
				Err:     digitaltwin.ErrInvalidInput,
			}
		}
		gatewayID = &val
	}

	var entityType *digitaltwin.EntityType
	if val, ok := params["entity_type"]; ok {
		et := digitaltwin.EntityType(val)
		if !et.Valid() {
			return nil, &QueryError{
				Code:    "invalid_request",
				Message: "invalid entity_type parameter",
				Err:     digitaltwin.ErrInvalidInput,
			}
		}
		entityType = &et
	}

	limit := p.cfg.ListDefaultLimit
	if val, ok := params["limit"]; ok {
		parsed, err := parseASCIIInteger(val)
		if err != nil || parsed < 1 || parsed > p.cfg.ListMaxLimit {
			return nil, &QueryError{
				Code:    "invalid_request",
				Message: "invalid limit parameter",
				Err:     digitaltwin.ErrInvalidInput,
			}
		}
		limit = parsed
	}

	var after *string
	if val, ok := params["after"]; ok {
		if _, err := digitaltwin.ParseListCursor(val, gatewayID, entityType); err != nil {
			return nil, &QueryError{
				Code:    "invalid_cursor",
				Message: "invalid cursor",
				Err:     digitaltwin.ErrInvalidCursor,
			}
		}
		after = &val
	}

	return &digitaltwin.TwinListQuery{
		GatewayID:  gatewayID,
		EntityType: entityType,
		Limit:      limit,
		After:      after,
	}, nil
}

// ParseTwinDetailQuery parses and validates query parameters for GET /v1/digital-twins/{entity_id}.
func (p *TwinQueryParser) ParseTwinDetailQuery(rawQuery string, entityID string) (*digitaltwin.TwinDetailQuery, error) {
	if _, err := digitaltwin.ParseEntityURN(entityID); err != nil {
		return nil, &QueryError{
			Code:    "invalid_request",
			Message: "invalid entity_id",
			Err:     digitaltwin.ErrInvalidInput,
		}
	}

	allowedKeys := map[string]struct{}{
		"relationship_limit": {},
		"relationship_after": {},
	}

	params, err := parseRawQuery(rawQuery, allowedKeys)
	if err != nil {
		return nil, err
	}

	relLimit := p.cfg.RelationshipDefaultLimit
	if val, ok := params["relationship_limit"]; ok {
		parsed, err := parseASCIIInteger(val)
		if err != nil || parsed < 1 || parsed > p.cfg.RelationshipMaxLimit {
			return nil, &QueryError{
				Code:    "invalid_request",
				Message: "invalid relationship_limit parameter",
				Err:     digitaltwin.ErrInvalidInput,
			}
		}
		relLimit = parsed
	}

	var relAfter *string
	if val, ok := params["relationship_after"]; ok {
		if _, err := digitaltwin.ParseRelationshipCursor(val, entityID); err != nil {
			return nil, &QueryError{
				Code:    "invalid_cursor",
				Message: "invalid cursor",
				Err:     digitaltwin.ErrInvalidCursor,
			}
		}
		relAfter = &val
	}

	return &digitaltwin.TwinDetailQuery{
		EntityID:          entityID,
		RelationshipLimit: relLimit,
		RelationshipAfter: relAfter,
	}, nil
}
