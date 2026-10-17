package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/digitaltwin/mapper"
	"iot-platform/internal/gateway"
)

// PostGatewayResponseDTO represents the plain metadata wire representation
// for POST /v1/digital-twins when creating or replaying a Gateway entity.
// It exposes no internal UUIDs, state, context, or relationships.
type PostGatewayResponseDTO struct {
	GatewayID   string    `json:"gateway_id"`
	Name        string    `json:"name"`
	Description *string   `json:"description"`
	OwnerUserID uuid.UUID `json:"owner_user_id"`
	EntityID    string    `json:"entity_id"`
	CreatedAt   *string   `json:"created_at"`
}

// PostSensorResponseDTO represents the plain metadata wire representation
// for POST /v1/digital-twins when creating or replaying a Sensor entity.
// It exposes no internal UUIDs, state, context, or relationships.
type PostSensorResponseDTO struct {
	GatewayID string  `json:"gateway_id"`
	SensorID  string  `json:"sensor_id"`
	Name      string  `json:"name"`
	Unit      *string `json:"unit"`
	EntityID  string  `json:"entity_id"`
	CreatedAt *string `json:"created_at"`
}

// PatchGatewayResponseDTO represents the plain metadata wire representation
// for PATCH /v1/digital-twins/{entity_id} on a Gateway entity.
// It exposes no internal UUIDs, state, context, or relationships.
type PatchGatewayResponseDTO struct {
	ID          string  `json:"id"`
	Type        string  `json:"type"`
	GatewayID   string  `json:"gateway_id"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
	UpdatedAt   string  `json:"updated_at"`
}

// PatchSensorResponseDTO represents the plain metadata wire representation
// for PATCH /v1/digital-twins/{entity_id} on a Sensor entity.
// It exposes no internal UUIDs, state, context, or relationships.
type PatchSensorResponseDTO struct {
	ID        string  `json:"id"`
	Type      string  `json:"type"`
	GatewayID string  `json:"gateway_id"`
	SensorID  string  `json:"sensor_id"`
	Name      string  `json:"name"`
	Unit      *string `json:"unit"`
	UpdatedAt string  `json:"updated_at"`
}

// PostEntityStatus returns 201 Created for new entity creations, and 200 OK for exact idempotent replays.
func PostEntityStatus(created bool) int {
	if created {
		return http.StatusCreated
	}
	return http.StatusOK
}

// NewPostGatewayResponseDTO projects a ProvisionGatewayResult into PostGatewayResponseDTO.
// It normalizes CreatedAt to a microsecond UTC string (or nil if the legacy timestamp is NULL),
// verifies GatewayID and Entity URN consistency, and rejects invalid inputs.
func NewPostGatewayResponseDTO(res *gateway.ProvisionGatewayResult) (*PostGatewayResponseDTO, error) {
	if res == nil {
		return nil, errors.New("provision gateway result must not be nil")
	}
	if err := gateway.ValidateGatewayID(res.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in gateway result: %w", err)
	}
	if res.Name == "" {
		return nil, errors.New("gateway name must not be empty")
	}
	if res.OwnerUserID == uuid.Nil {
		return nil, errors.New("gateway owner_user_id must not be nil")
	}

	parsed, err := digitaltwin.ParseEntityURN(res.EntityID)
	if err != nil {
		return nil, fmt.Errorf("invalid entity_id %q: %w", res.EntityID, err)
	}
	if parsed.Type != digitaltwin.EntityTypeGateway {
		return nil, fmt.Errorf("entity type %q does not match gateway result", parsed.Type)
	}
	if parsed.GatewayID != res.GatewayID {
		return nil, fmt.Errorf("entity URN gateway_id %q does not match result gateway_id %q", parsed.GatewayID, res.GatewayID)
	}

	var createdAtStr *string
	if res.CreatedAt != nil {
		formatted, err := mapper.FormatRFC3339Micro(*res.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("invalid created_at: %w", err)
		}
		createdAtStr = &formatted
	}

	return &PostGatewayResponseDTO{
		GatewayID:   res.GatewayID,
		Name:        res.Name,
		Description: res.Description,
		OwnerUserID: res.OwnerUserID,
		EntityID:    res.EntityID,
		CreatedAt:   createdAtStr,
	}, nil
}

// NewPostSensorResponseDTO projects a ProvisionSensorResult into PostSensorResponseDTO.
// It normalizes CreatedAt to a microsecond UTC string (or nil if the legacy timestamp is NULL),
// verifies GatewayID, SensorID, and Entity URN consistency, and rejects invalid inputs.
func NewPostSensorResponseDTO(res *gateway.ProvisionSensorResult) (*PostSensorResponseDTO, error) {
	if res == nil {
		return nil, errors.New("provision sensor result must not be nil")
	}
	if err := gateway.ValidateGatewayID(res.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in sensor result: %w", err)
	}
	if err := gateway.ValidateSensorID(res.SensorID); err != nil {
		return nil, fmt.Errorf("invalid sensor_id in sensor result: %w", err)
	}
	if res.Name == "" {
		return nil, errors.New("sensor name must not be empty")
	}

	parsed, err := digitaltwin.ParseEntityURN(res.EntityID)
	if err != nil {
		return nil, fmt.Errorf("invalid entity_id %q: %w", res.EntityID, err)
	}
	if parsed.Type != digitaltwin.EntityTypeSensor {
		return nil, fmt.Errorf("entity type %q does not match sensor result", parsed.Type)
	}
	if parsed.GatewayID != res.GatewayID {
		return nil, fmt.Errorf("entity URN gateway_id %q does not match result gateway_id %q", parsed.GatewayID, res.GatewayID)
	}
	if parsed.SensorID != res.SensorID {
		return nil, fmt.Errorf("entity URN sensor_id %q does not match result sensor_id %q", parsed.SensorID, res.SensorID)
	}

	var createdAtStr *string
	if res.CreatedAt != nil {
		formatted, err := mapper.FormatRFC3339Micro(*res.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("invalid created_at: %w", err)
		}
		createdAtStr = &formatted
	}

	return &PostSensorResponseDTO{
		GatewayID: res.GatewayID,
		SensorID:  res.SensorID,
		Name:      res.Name,
		Unit:      res.Unit,
		EntityID:  res.EntityID,
		CreatedAt: createdAtStr,
	}, nil
}

// NewCreateEntityResponseDTO maps a digitaltwin.CreateEntityResult to its appropriate wire DTO
// and HTTP status (201 Created or 200 OK). It enforces branch consistency between Type and the
// populated inner result and rejects malformed outcomes.
func NewCreateEntityResponseDTO(res digitaltwin.CreateEntityResult) (status int, body any, err error) {
	status = PostEntityStatus(res.Created)
	switch res.Type {
	case digitaltwin.EntityTypeGateway:
		if res.Gateway == nil {
			return 0, nil, errors.New("create entity result: missing gateway result for gateway type")
		}
		if res.Sensor != nil {
			return 0, nil, errors.New("create entity result: unexpected sensor result for gateway type")
		}
		if res.Created != res.Gateway.Created {
			return 0, nil, errors.New("create entity result: mismatched created outcome")
		}
		if res.EntityID != "" && res.Gateway.EntityID != "" && res.EntityID != res.Gateway.EntityID {
			return 0, nil, errors.New("create entity result: mismatched entity ID")
		}
		dto, err := NewPostGatewayResponseDTO(res.Gateway)
		if err != nil {
			return 0, nil, err
		}
		return status, dto, nil

	case digitaltwin.EntityTypeSensor:
		if res.Sensor == nil {
			return 0, nil, errors.New("create entity result: missing sensor result for sensor type")
		}
		if res.Gateway != nil {
			return 0, nil, errors.New("create entity result: unexpected gateway result for sensor type")
		}
		if res.Created != res.Sensor.Created {
			return 0, nil, errors.New("create entity result: mismatched created outcome")
		}
		if res.EntityID != "" && res.Sensor.EntityID != "" && res.EntityID != res.Sensor.EntityID {
			return 0, nil, errors.New("create entity result: mismatched entity ID")
		}
		dto, err := NewPostSensorResponseDTO(res.Sensor)
		if err != nil {
			return 0, nil, err
		}
		return status, dto, nil

	default:
		return 0, nil, fmt.Errorf("create entity result: unsupported entity type %q", res.Type)
	}
}

// NewPatchGatewayResponseDTO projects a digitaltwin.PatchGatewayResult into PatchGatewayResponseDTO.
// It normalizes UpdatedAt to a microsecond UTC string, preserves existing no-op timestamps,
// and verifies entity type and URN consistency.
func NewPatchGatewayResponseDTO(res digitaltwin.PatchGatewayResult) (*PatchGatewayResponseDTO, error) {
	if res.Type != digitaltwin.EntityTypeGateway {
		return nil, fmt.Errorf("patch gateway result: invalid entity type %q", res.Type)
	}
	if err := gateway.ValidateGatewayID(res.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in patch gateway result: %w", err)
	}
	if res.Name == "" {
		return nil, errors.New("patch gateway result: name must not be empty")
	}

	parsed, err := digitaltwin.ParseEntityURN(res.ID)
	if err != nil {
		return nil, fmt.Errorf("invalid entity id %q: %w", res.ID, err)
	}
	if parsed.Type != digitaltwin.EntityTypeGateway {
		return nil, fmt.Errorf("entity URN type %q does not match gateway result", parsed.Type)
	}
	if parsed.GatewayID != res.GatewayID {
		return nil, fmt.Errorf("entity URN gateway_id %q does not match result gateway_id %q", parsed.GatewayID, res.GatewayID)
	}

	updatedAtStr, err := mapper.FormatRFC3339Micro(res.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("invalid updated_at: %w", err)
	}

	return &PatchGatewayResponseDTO{
		ID:          res.ID,
		Type:        string(res.Type),
		GatewayID:   res.GatewayID,
		Name:        res.Name,
		Description: res.Description,
		UpdatedAt:   updatedAtStr,
	}, nil
}

// NewPatchSensorResponseDTO projects a digitaltwin.PatchSensorResult into PatchSensorResponseDTO.
// It normalizes UpdatedAt to a microsecond UTC string, preserves existing no-op timestamps,
// and verifies entity type, GatewayID, SensorID, and URN consistency.
func NewPatchSensorResponseDTO(res digitaltwin.PatchSensorResult) (*PatchSensorResponseDTO, error) {
	if res.Type != digitaltwin.EntityTypeSensor {
		return nil, fmt.Errorf("patch sensor result: invalid entity type %q", res.Type)
	}
	if err := gateway.ValidateGatewayID(res.GatewayID); err != nil {
		return nil, fmt.Errorf("invalid gateway_id in patch sensor result: %w", err)
	}
	if err := gateway.ValidateSensorID(res.SensorID); err != nil {
		return nil, fmt.Errorf("invalid sensor_id in patch sensor result: %w", err)
	}
	if res.Name == "" {
		return nil, errors.New("patch sensor result: name must not be empty")
	}

	parsed, err := digitaltwin.ParseEntityURN(res.ID)
	if err != nil {
		return nil, fmt.Errorf("invalid entity id %q: %w", res.ID, err)
	}
	if parsed.Type != digitaltwin.EntityTypeSensor {
		return nil, fmt.Errorf("entity URN type %q does not match sensor result", parsed.Type)
	}
	if parsed.GatewayID != res.GatewayID {
		return nil, fmt.Errorf("entity URN gateway_id %q does not match result gateway_id %q", parsed.GatewayID, res.GatewayID)
	}
	if parsed.SensorID != res.SensorID {
		return nil, fmt.Errorf("entity URN sensor_id %q does not match result sensor_id %q", parsed.SensorID, res.SensorID)
	}

	updatedAtStr, err := mapper.FormatRFC3339Micro(res.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("invalid updated_at: %w", err)
	}

	return &PatchSensorResponseDTO{
		ID:        res.ID,
		Type:      string(res.Type),
		GatewayID: res.GatewayID,
		SensorID:  res.SensorID,
		Name:      res.Name,
		Unit:      res.Unit,
		UpdatedAt: updatedAtStr,
	}, nil
}

// NewPatchEntityResponseDTO maps either a Gateway or Sensor patch result depending on target type.
func NewPatchEntityResponseDTO(entityType digitaltwin.EntityType, gwRes *digitaltwin.PatchGatewayResult, sensorRes *digitaltwin.PatchSensorResult) (any, error) {
	switch entityType {
	case digitaltwin.EntityTypeGateway:
		if gwRes == nil {
			return nil, errors.New("patch entity result: missing gateway result for gateway type")
		}
		if sensorRes != nil {
			return nil, errors.New("patch entity result: unexpected sensor result for gateway type")
		}
		return NewPatchGatewayResponseDTO(*gwRes)

	case digitaltwin.EntityTypeSensor:
		if sensorRes == nil {
			return nil, errors.New("patch entity result: missing sensor result for sensor type")
		}
		if gwRes != nil {
			return nil, errors.New("patch entity result: unexpected gateway result for sensor type")
		}
		return NewPatchSensorResponseDTO(*sensorRes)

	default:
		return nil, fmt.Errorf("patch entity result: unsupported entity type %q", entityType)
	}
}

// ClassifyMutationError maps domain, infrastructure, and database errors from mutation operations
// (POST /v1/digital-twins and PATCH /v1/digital-twins/{entity_id}) into standard HTTP status codes,
// stable API error codes, and safe client-facing messages.
// It inspects causes via errors.Is and ensures no internal SQL, DSN, or token details leak.
func ClassifyMutationError(err error) (status int, code string, message string) {
	if err == nil {
		return http.StatusOK, "", ""
	}

	switch {
	// 503: Context cancellation, deadline exceeded, admin lookup failure, dependency unavailability, or uncertain commit outcome.
	case errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, digitaltwin.ErrDependencyUnavailable),
		errors.Is(err, digitaltwin.ErrMetadataPatchUnavailable),
		errors.Is(err, gateway.ErrGatewayProvisioningUnavailable),
		errors.Is(err, gateway.ErrAdminOrOwnerProvisioningUnavailable),
		errors.Is(err, gateway.ErrOwnerProvisioningUnavailable),
		errors.Is(err, gateway.ErrProvisioningAdminLookup),
		errors.Is(err, gateway.ErrAdminLookupFailed),
		errors.Is(err, gateway.ErrInventoryUnavailable):
		return http.StatusServiceUnavailable, "service_unavailable", "service unavailable"

	// 400: Invalid domain input, bad IDs, blank name, nil actor.
	case errors.Is(err, digitaltwin.ErrInvalidInput),
		errors.Is(err, digitaltwin.ErrNilActor),
		errors.Is(err, digitaltwin.ErrInvalidCursor),
		errors.Is(err, digitaltwin.ErrInvalidProperty),
		errors.Is(err, gateway.ErrInvalidProvisioningInput),
		errors.Is(err, gateway.ErrInvalidGatewayID),
		errors.Is(err, gateway.ErrInvalidSensorID),
		errors.Is(err, gateway.ErrInvalidUser),
		errors.Is(err, gateway.ErrInvalidInventoryQuery):
		return http.StatusBadRequest, "invalid_request", "invalid request"

	// 403: Forbidden - non-admin actor on admin endpoint, or operator/viewer role on owner action.
	case errors.Is(err, digitaltwin.ErrForbidden),
		errors.Is(err, digitaltwin.ErrProvisioningForbidden),
		errors.Is(err, gateway.ErrProvisioningForbidden),
		errors.Is(err, gateway.ErrAdminForbidden):
		return http.StatusForbidden, "forbidden", "access denied"

	// 404: Resource not found, missing parent gateway, missing owner profile, or non-member sensor.
	case errors.Is(err, digitaltwin.ErrNotFound),
		errors.Is(err, digitaltwin.ErrProvisioningNotFound),
		errors.Is(err, gateway.ErrProvisioningNotFound),
		errors.Is(err, gateway.ErrNotFound):
		return http.StatusNotFound, "not_found", "resource not found"

	// 409: Exact-replay representation conflict (same identity, mismatched representation or owner).
	case errors.Is(err, digitaltwin.ErrProvisioningConflict),
		errors.Is(err, gateway.ErrProvisioningConflict):
		return http.StatusConflict, "conflict", "resource conflict"

	// 500: Inconsistent relational/Twin graph, unexpected domain error, or unclassified error.
	case errors.Is(err, digitaltwin.ErrInconsistentGraph),
		errors.Is(err, digitaltwin.ErrInconsistentUnit),
		errors.Is(err, digitaltwin.ErrProvisioningInconsistent),
		errors.Is(err, digitaltwin.ErrUnexpected),
		errors.Is(err, gateway.ErrProvisioningInconsistent):
		return http.StatusInternalServerError, "internal_error", "internal server error"

	default:
		// Any other error (unhandled SQL error, scan error, constraint violation, etc.)
		// defaults to 500 internal_error without exposing internal details.
		return http.StatusInternalServerError, "internal_error", "internal server error"
	}
}

// ClassifyMutationParserStatus converts HTTP parser status codes (400, 413, 415)
// into standard API error codes and client-safe messages.
func ClassifyMutationParserStatus(status int) (code string, message string) {
	switch status {
	case http.StatusRequestEntityTooLarge:
		return "payload_too_large", "payload too large"
	case http.StatusUnsupportedMediaType:
		return "unsupported_media_type", "unsupported media type"
	default:
		return "invalid_request", "invalid request"
	}
}
