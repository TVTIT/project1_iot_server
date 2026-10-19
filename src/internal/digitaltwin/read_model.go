package digitaltwin

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/telemetry"
)

// HistorySelector defines the allowlisted property selector for Twin history queries.
type HistorySelector string

// Allowlisted property selectors for Twin history queries.
const (
	SelectorValue            HistorySelector = "value"
	SelectorSampleIntervalUS HistorySelector = "sample_interval_us"
)

// Valid reports whether the selector matches allowlisted history properties.
func (s HistorySelector) Valid() bool {
	switch s {
	case SelectorValue, SelectorSampleIntervalUS:
		return true
	default:
		return false
	}
}

// TwinListQuery represents the query parameters for GET /v1/digital-twins.
// Actor identity is passed separately to service/repository methods.
type TwinListQuery struct {
	GatewayID  *string
	EntityType *EntityType
	Limit      int
	After      *string
}

// TwinDetailQuery represents the query parameters for GET /v1/digital-twins/{entity_id}.
type TwinDetailQuery struct {
	EntityID          string
	RelationshipLimit int
	RelationshipAfter *string
}

// TwinStateQuery represents parameters for GET /v1/digital-twins/{entity_id}/state.
type TwinStateQuery struct {
	EntityID string
}

// TwinHistoryQuery represents parameters for GET /v1/digital-twins/{entity_id}/history.
type TwinHistoryQuery struct {
	EntityID   string
	Property   HistorySelector
	Range      telemetry.TimeRange
	Resolution telemetry.Resolution
	MaxPoints  int

	// EffectiveRawWindow is an internal server-propagated effective raw window bound (<= ContractMaxRawWindow).
	// It is populated by ReadPreparation from validated server configuration and governs raw/auto branching.
	// Callers must not set this to enlarge configured limits beyond ContractMaxRawWindow.
	EffectiveRawWindow time.Duration
}

// TwinSummary represents an entity in list projection.
type TwinSummary struct {
	ID        string
	Type      EntityType
	Name      *string
	GatewayID string
	UpdatedAt time.Time
}

// TwinListResult contains the items and optional next page cursor.
type TwinListResult struct {
	Items      []TwinSummary
	NextCursor *string
}

// RelationshipItem describes a single relationship item in detail projection.
type RelationshipItem struct {
	Type             string
	RelationshipType string
	TargetURN        string
	CreatedAt        time.Time
}

// RelationshipPage provides pagination metadata for relationships in detail projection.
type RelationshipPage struct {
	RelationshipType string
	Limit            int
	Count            int
	Partial          bool
	NextCursor       *string
}

// TwinDetailResult represents the entity detail projection data.
type TwinDetailResult struct {
	ID               string
	Type             EntityType
	Name             string
	Description      *string
	Unit             *string
	GatewayID        string
	UpdatedAt        time.Time
	Relationships    []RelationshipItem
	RelationshipPage *RelationshipPage
}

// TwinStateResult represents current reported and desired states verbatim.
type TwinStateResult struct {
	ID              string
	Type            EntityType
	GatewayID       string
	ReportedState   json.RawMessage
	DesiredState    json.RawMessage
	ReportedVersion int64
	DesiredVersion  int64
	LastReportedAt  *time.Time
	LastDesiredAt   *time.Time
	UpdatedAt       time.Time
}

// TemporalSamplePoint represents one point from the twin_temporal_values hypertable.
type TemporalSamplePoint struct {
	ObservedAt   time.Time
	ValueType    string
	ValueNumber  *float64
	ValueText    *string
	ValueBoolean *bool
	Metadata     json.RawMessage
	MessageID    *uuid.UUID
}

// TemporalBucketPoint represents one bucket aggregation in downsampled temporal series.
// Semantics match telemetry.BucketSamplePoint (BucketStart, Avg, Min, Max, Count).
type TemporalBucketPoint = telemetry.BucketSamplePoint

// TwinHistoryResult represents the data returned by Twin history queries.
// If Property is SelectorValue, TelemetryResult carries the delegated telemetry result.
// If Property is SelectorSampleIntervalUS and Mode is raw, TemporalPoints carries the typed temporal rows.
// If Property is SelectorSampleIntervalUS and Mode is bucket, TemporalBuckets carries numeric bucket aggregations.
type TwinHistoryResult struct {
	EntityID        string
	Property        HistorySelector
	Unit            *string
	From            time.Time
	To              time.Time
	Mode            string
	Resolution      string
	PointCount      int
	TelemetryResult *telemetry.TelemetryHistoryResult
	TemporalPoints  []TemporalSamplePoint
	TemporalBuckets []TemporalBucketPoint
}
