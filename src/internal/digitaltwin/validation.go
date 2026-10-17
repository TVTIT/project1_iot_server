package digitaltwin

import "iot-platform/internal/gateway"

// ValidateTwinListQuery revalidates a TwinListQuery without HTTP dependencies.
// This is used by service layers to enforce boundary constraints.
func ValidateTwinListQuery(q TwinListQuery, maxLimit int) error {
	if maxLimit <= 0 {
		maxLimit = ContractMaxLimit
	}
	if q.Limit < 1 || q.Limit > maxLimit {
		return ErrInvalidInput
	}
	if q.GatewayID != nil {
		if err := gateway.ValidateGatewayID(*q.GatewayID); err != nil {
			return ErrInvalidInput
		}
	}
	if q.EntityType != nil {
		if !q.EntityType.Valid() {
			return ErrInvalidInput
		}
	}
	if q.After != nil {
		if _, err := ParseListCursor(*q.After, q.GatewayID, q.EntityType); err != nil {
			return ErrInvalidCursor
		}
	}
	return nil
}

// ValidateTwinDetailQuery revalidates a TwinDetailQuery without HTTP dependencies.
func ValidateTwinDetailQuery(q TwinDetailQuery, maxLimit int) error {
	if maxLimit <= 0 {
		maxLimit = ContractMaxLimit
	}
	if _, err := ParseEntityURN(q.EntityID); err != nil {
		return ErrInvalidInput
	}
	if q.RelationshipLimit < 1 || q.RelationshipLimit > maxLimit {
		return ErrInvalidInput
	}
	if q.RelationshipAfter != nil {
		if _, err := ParseRelationshipCursor(*q.RelationshipAfter, q.EntityID); err != nil {
			return ErrInvalidCursor
		}
	}
	return nil
}

// ValidateTwinStateQuery revalidates a TwinStateQuery without HTTP dependencies.
func ValidateTwinStateQuery(q TwinStateQuery) error {
	if _, err := ParseEntityURN(q.EntityID); err != nil {
		return ErrInvalidInput
	}
	return nil
}

// ValidateTwinHistoryQuery revalidates a TwinHistoryQuery without HTTP dependencies.
func ValidateTwinHistoryQuery(q TwinHistoryQuery) error {
	parsed, err := ParseEntityURN(q.EntityID)
	if err != nil {
		return ErrInvalidInput
	}
	if parsed.Type != EntityTypeSensor {
		return ErrInvalidProperty
	}
	if !q.Property.Valid() {
		return ErrInvalidProperty
	}
	return nil
}
