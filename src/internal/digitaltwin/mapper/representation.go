package mapper

import (
	"errors"
	"time"
)

// ErrInvalidTimestampYear is returned when a timestamp's UTC year is < 1 or > 9999.
var ErrInvalidTimestampYear = errors.New("timestamp year must be in 0001-9999")

const rfc3339MicroUTC = "2006-01-02T15:04:05.000000Z"

// FormatRFC3339Micro formats t as UTC with exact 6 fractional digits and Z suffix.
// It returns ErrInvalidTimestampYear if the year is < 1 or > 9999.
func FormatRFC3339Micro(t time.Time) (string, error) {
	utc := t.UTC()
	year := utc.Year()
	if year < 1 || year > 9999 {
		return "", ErrInvalidTimestampYear
	}
	return utc.Format(rfc3339MicroUTC), nil
}

// PropertyDTO represents an NGSI-LD Property attribute in entity JSON-LD detail projection.
type PropertyDTO struct {
	Type       string  `json:"type"`
	Value      any     `json:"value"`
	ModifiedAt string  `json:"modifiedAt"`
	ObservedAt *string `json:"observedAt,omitempty"`
}

// NewProperty creates a standard PropertyDTO with modifiedAt timestamp.
func NewProperty(value any, modifiedAt string) PropertyDTO {
	return PropertyDTO{
		Type:       "Property",
		Value:      value,
		ModifiedAt: modifiedAt,
	}
}

// NewObservationProperty creates a PropertyDTO with both modifiedAt and observedAt timestamps.
func NewObservationProperty(value any, modifiedAt string, observedAt string) PropertyDTO {
	return PropertyDTO{
		Type:       "Property",
		Value:      value,
		ModifiedAt: modifiedAt,
		ObservedAt: &observedAt,
	}
}

// RelationshipDTO represents an NGSI-LD Relationship attribute in entity JSON-LD detail projection.
type RelationshipDTO struct {
	Type      string `json:"type"`
	Object    string `json:"object"`
	CreatedAt string `json:"createdAt"`
}

// NewRelationship creates a new RelationshipDTO with object public URN and createdAt timestamp.
func NewRelationship(object string, createdAt string) RelationshipDTO {
	return RelationshipDTO{
		Type:      "Relationship",
		Object:    object,
		CreatedAt: createdAt,
	}
}

// RelationshipPageDTO contains pagination metadata for entity relationships.
type RelationshipPageDTO struct {
	RelationshipType string  `json:"relationship_type"`
	Limit            int     `json:"limit"`
	Count            int     `json:"count"`
	Partial          bool    `json:"partial"`
	NextCursor       *string `json:"next_cursor"`
}
