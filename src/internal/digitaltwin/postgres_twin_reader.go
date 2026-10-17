package digitaltwin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"

	"iot-platform/internal/gateway"
)

// PostgresTwinReader implements TwinReader for Digital Twin data queries.
type PostgresTwinReader struct {
	database Database
}

var _ TwinReader = (*PostgresTwinReader)(nil)

// NewPostgresTwinReader creates a new PostgresTwinReader with the provided database connection.
// It verifies that database is non-nil (rejecting typed nils via reflection).
func NewPostgresTwinReader(database Database) (*PostgresTwinReader, error) {
	if isNilDependency(database) {
		return nil, fmt.Errorf("database is required")
	}
	return &PostgresTwinReader{database: database}, nil
}

// ListEntities queries public.twin_entities scoped by actor's membership in user_gateways.
func (r *PostgresTwinReader) ListEntities(ctx context.Context, actor uuid.UUID, q TwinListQuery) (*TwinListResult, error) {
	if actor == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if ctx.Err() != nil {
		return nil, classifyReaderError(ctx.Err())
	}

	limit := q.Limit
	if limit <= 0 {
		limit = ContractDefaultLimit
	} else if limit > ContractMaxLimit {
		return nil, ErrInvalidInput
	}

	if q.GatewayID != nil {
		if err := gateway.ValidateGatewayID(*q.GatewayID); err != nil {
			return nil, ErrInvalidInput
		}
	}

	if q.EntityType != nil {
		if !q.EntityType.Valid() {
			return nil, ErrInvalidInput
		}
	}

	var afterCursor *ListCursor
	if q.After != nil && *q.After != "" {
		c, err := ParseListCursor(*q.After, q.GatewayID, q.EntityType)
		if err != nil {
			return nil, ErrInvalidCursor
		}
		if err := ValidateListCursor(c, q.GatewayID, q.EntityType); err != nil {
			return nil, ErrInvalidCursor
		}
		afterCursor = c
	}

	query := `SELECT e.entity_id, e.entity_type, e.name, e.gateway_id, e.updated_at
FROM public.twin_entities AS e
JOIN public.user_gateways AS ug ON ug.gateway_id = e.gateway_id AND ug.user_id = $1`

	args := []any{actor}
	whereClauses := []string{"e.entity_type IN ('Gateway', 'Sensor')"}

	if q.GatewayID != nil {
		args = append(args, *q.GatewayID)
		whereClauses = append(whereClauses, fmt.Sprintf("e.gateway_id = $%d", len(args)))
	}
	if q.EntityType != nil {
		args = append(args, string(*q.EntityType))
		whereClauses = append(whereClauses, fmt.Sprintf("e.entity_type = $%d", len(args)))
	}
	if afterCursor != nil {
		args = append(args, afterCursor.EntityID)
		whereClauses = append(whereClauses, fmt.Sprintf(`e.entity_id COLLATE "C" > $%d`, len(args)))
	}

	query += "\nWHERE " + strings.Join(whereClauses, "\n  AND ")

	query += "\nORDER BY e.entity_id COLLATE \"C\" ASC"

	args = append(args, limit+1)
	query += fmt.Sprintf("\nLIMIT $%d", len(args))

	rows, err := r.database.Query(ctx, query, args...)
	if err != nil {
		return nil, classifyReaderError(err)
	}
	defer rows.Close()

	items := make([]TwinSummary, 0, limit)
	for rows.Next() {
		var (
			entityID   string
			entityType string
			name       *string
			gatewayID  string
			updatedAt  time.Time
		)
		if err := rows.Scan(&entityID, &entityType, &name, &gatewayID, &updatedAt); err != nil {
			return nil, classifyReaderError(err)
		}
		items = append(items, TwinSummary{
			ID:        entityID,
			Type:      EntityType(entityType),
			Name:      name,
			GatewayID: gatewayID,
			UpdatedAt: updatedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, classifyReaderError(err)
	}

	var nextCursor *string
	if len(items) > limit {
		lastItem := items[limit-1]
		items = items[:limit]
		cStr, err := EncodeListCursor(lastItem.ID, q.GatewayID, q.EntityType)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to encode next cursor: %w", ErrUnexpected, err)
		}
		nextCursor = &cStr
	}

	return &TwinListResult{
		Items:      items,
		NextCursor: nextCursor,
	}, nil
}

// GetDetail queries the detail graph and relationship page for an entity.
func (r *PostgresTwinReader) GetDetail(ctx context.Context, actor uuid.UUID, entity ResolvedEntity, q TwinDetailQuery) (*TwinDetailResult, error) {
	if actor == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if ctx.Err() != nil {
		return nil, classifyReaderError(ctx.Err())
	}

	if entity.PublicURN == "" || entity.GatewayID == "" || !entity.Type.Valid() || entity.InternalID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if entity.Type == EntityTypeSensor && entity.SensorID == "" {
		return nil, ErrInvalidInput
	}

	if q.EntityID != "" && q.EntityID != entity.PublicURN {
		return nil, ErrInvalidInput
	}

	relLimit := q.RelationshipLimit
	if relLimit <= 0 {
		relLimit = ContractDefaultRelationshipLimit
	} else if relLimit > ContractMaxRelationshipLimit {
		return nil, ErrInvalidInput
	}

	var afterCursor *RelationshipCursor
	if q.RelationshipAfter != nil && *q.RelationshipAfter != "" {
		c, err := ParseRelationshipCursor(*q.RelationshipAfter, entity.PublicURN)
		if err != nil {
			return nil, ErrInvalidCursor
		}
		if err := ValidateRelationshipCursor(c, entity.PublicURN); err != nil {
			return nil, ErrInvalidCursor
		}
		if c.RelType != "hasSensor" || c.SourceURN != entity.PublicURN {
			return nil, ErrInvalidCursor
		}
		afterCursor = c
	}

	switch entity.Type {
	case EntityTypeGateway:
		return r.getGatewayDetail(ctx, actor, entity, relLimit, afterCursor)
	case EntityTypeSensor:
		return r.getSensorDetail(ctx, actor, entity)
	default:
		return nil, ErrInvalidInput
	}
}

func (r *PostgresTwinReader) getGatewayDetail(ctx context.Context, actor uuid.UUID, entity ResolvedEntity, relLimit int, afterCursor *RelationshipCursor) (*TwinDetailResult, error) {
	query := `WITH authorized_source AS (
    SELECT e.id AS source_id,
           e.entity_id AS source_urn,
           e.entity_type AS source_type,
           g.name AS source_name,
           g.description AS source_description,
           e.gateway_id AS source_gateway_id,
           e.updated_at AS source_updated_at
    FROM public.twin_entities AS e
    JOIN public.gateways AS g ON g.gateway_id = e.gateway_id
    JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id AND ug.user_id = $1
    WHERE e.id = $2
      AND e.entity_id = $3
      AND e.entity_type = $4
      AND e.gateway_id = $5
)
SELECT src.source_urn,
       src.source_type,
       src.source_name,
       src.source_description,
       src.source_gateway_id,
       src.source_updated_at,
       rel.relationship_type,
       rel.target_urn,
       rel.created_at
FROM authorized_source AS src
LEFT JOIN LATERAL (
    SELECT r.relationship_type,
           tgt.entity_id AS target_urn,
           r.created_at
    FROM public.twin_relationships AS r
    JOIN public.twin_entities AS tgt
      ON tgt.id = r.target_entity_id
     AND tgt.entity_type = 'Sensor'
     AND tgt.gateway_id = src.source_gateway_id
    JOIN public.sensors AS s
      ON s.gateway_id = tgt.gateway_id
     AND ('urn:ngsi-ld:Sensor:' || s.gateway_id || ':' || s.sensor_id) = tgt.entity_id
    WHERE r.source_entity_id = src.source_id
      AND r.relationship_type = 'hasSensor'`

	args := []any{actor, entity.InternalID, entity.PublicURN, string(entity.Type), entity.GatewayID}
	if afterCursor != nil {
		args = append(args, afterCursor.TargetURN)
		query += fmt.Sprintf("\n      AND tgt.entity_id COLLATE \"C\" > $%d", len(args))
	}

	query += "\n    ORDER BY tgt.entity_id COLLATE \"C\" ASC"
	args = append(args, relLimit+1)
	query += fmt.Sprintf("\n    LIMIT $%d\n) AS rel ON true\nORDER BY rel.target_urn COLLATE \"C\" ASC", len(args))

	rows, err := r.database.Query(ctx, query, args...)
	if err != nil {
		return nil, classifyReaderError(err)
	}
	defer rows.Close()

	var (
		sourceName        string
		sourceDescription *string
		sourceGatewayID   string
		sourceUpdatedAt   time.Time
		metadataSet       bool
		rawItems          = make([]RelationshipItem, 0, relLimit)
	)

	for rows.Next() {
		var (
			sURN       string
			sType      string
			sName      string
			sDesc      *string
			sGW        string
			sUpdated   time.Time
			relType    *string
			targetURN  *string
			relCreated *time.Time
		)
		if err := rows.Scan(
			&sURN,
			&sType,
			&sName,
			&sDesc,
			&sGW,
			&sUpdated,
			&relType,
			&targetURN,
			&relCreated,
		); err != nil {
			rows.Close()
			return nil, classifyReaderError(err)
		}

		if !metadataSet {
			sourceName = sName
			sourceDescription = sDesc
			sourceGatewayID = sGW
			sourceUpdatedAt = sUpdated
			metadataSet = true
		}

		if relType != nil && targetURN != nil && relCreated != nil {
			rawItems = append(rawItems, RelationshipItem{
				Type:             "Relationship",
				RelationshipType: *relType,
				TargetURN:        *targetURN,
				CreatedAt:        relCreated.UTC(),
			})
		}
	}

	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, classifyReaderError(err)
	}

	if !metadataSet {
		return nil, ErrNotFound
	}

	var (
		items      []RelationshipItem
		partial    bool
		nextCursor *string
	)

	if len(rawItems) > relLimit {
		items = rawItems[:relLimit]
		partial = true
		lastItem := items[relLimit-1]
		cStr, err := EncodeRelationshipCursor(entity.PublicURN, "hasSensor", lastItem.TargetURN)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to encode next cursor: %w", ErrUnexpected, err)
		}
		nextCursor = &cStr
	} else {
		items = rawItems
		partial = false
		nextCursor = nil
	}

	if items == nil {
		items = []RelationshipItem{}
	}

	page := &RelationshipPage{
		RelationshipType: "hasSensor",
		Limit:            relLimit,
		Count:            len(items),
		Partial:          partial,
		NextCursor:       nextCursor,
	}

	return &TwinDetailResult{
		ID:               entity.PublicURN,
		Type:             EntityTypeGateway,
		Name:             sourceName,
		Description:      sourceDescription,
		Unit:             nil,
		GatewayID:        sourceGatewayID,
		UpdatedAt:        sourceUpdatedAt.UTC(),
		Relationships:    items,
		RelationshipPage: page,
	}, nil
}

func (r *PostgresTwinReader) getSensorDetail(ctx context.Context, actor uuid.UUID, entity ResolvedEntity) (*TwinDetailResult, error) {
	query := `SELECT e.entity_id, e.entity_type, s.name, s.unit, e.gateway_id, e.updated_at
FROM public.twin_entities AS e
JOIN public.sensors AS s
  ON s.gateway_id = e.gateway_id AND s.sensor_id = $1
JOIN public.twin_entities AS parent_e
  ON parent_e.gateway_id = e.gateway_id AND parent_e.entity_type = 'Gateway'
JOIN public.twin_relationships AS r
  ON r.source_entity_id = parent_e.id AND r.target_entity_id = e.id AND r.relationship_type = 'hasSensor'
JOIN public.gateways AS g
  ON g.gateway_id = e.gateway_id
JOIN public.user_gateways AS ug
  ON ug.gateway_id = g.gateway_id AND ug.user_id = $2
WHERE e.id = $3
  AND e.entity_id = $4
  AND e.entity_type = 'Sensor'`

	var (
		entityID   string
		entityType string
		name       string
		unit       *string
		gatewayID  string
		updatedAt  time.Time
	)
	err := r.database.QueryRow(ctx, query, entity.SensorID, actor, entity.InternalID, entity.PublicURN).
		Scan(&entityID, &entityType, &name, &unit, &gatewayID, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, classifyReaderError(err)
	}

	return &TwinDetailResult{
		ID:               entity.PublicURN,
		Type:             EntityTypeSensor,
		Name:             name,
		Description:      nil,
		Unit:             unit,
		GatewayID:        gatewayID,
		UpdatedAt:        updatedAt.UTC(),
		Relationships:    nil,
		RelationshipPage: nil,
	}, nil
}

// GetState queries the direct reported and desired state for an entity.
func (r *PostgresTwinReader) GetState(ctx context.Context, actor uuid.UUID, entity ResolvedEntity) (*TwinStateResult, error) {
	if actor == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if ctx.Err() != nil {
		return nil, classifyReaderError(ctx.Err())
	}

	if entity.PublicURN == "" || entity.GatewayID == "" || !entity.Type.Valid() || entity.InternalID == uuid.Nil {
		return nil, ErrInvalidInput
	}
	if entity.Type == EntityTypeSensor && entity.SensorID == "" {
		return nil, ErrInvalidInput
	}

	switch entity.Type {
	case EntityTypeGateway:
		return r.getGatewayState(ctx, actor, entity)
	case EntityTypeSensor:
		return r.getSensorState(ctx, actor, entity)
	default:
		return nil, ErrInvalidInput
	}
}

func (r *PostgresTwinReader) getGatewayState(ctx context.Context, actor uuid.UUID, entity ResolvedEntity) (*TwinStateResult, error) {
	query := `SELECT e.entity_id, e.entity_type, e.gateway_id,
       s.entity_id IS NOT NULL AS has_state,
       s.reported_state, s.desired_state,
       s.reported_version, s.desired_version,
       s.last_reported_at, s.last_desired_at, s.updated_at
FROM public.twin_entities AS e
JOIN public.gateways AS g ON g.gateway_id = e.gateway_id
JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id AND ug.user_id = $1
LEFT JOIN public.twin_states AS s ON s.entity_id = e.id
WHERE e.id = $2
  AND e.entity_id = $3
  AND e.entity_type = $4
  AND e.gateway_id = $5`

	rows, err := r.database.Query(ctx, query, actor, entity.InternalID, entity.PublicURN, string(entity.Type), entity.GatewayID)
	if err != nil {
		return nil, classifyReaderError(err)
	}
	defer rows.Close()

	return scanStateRow(rows, entity)
}

func (r *PostgresTwinReader) getSensorState(ctx context.Context, actor uuid.UUID, entity ResolvedEntity) (*TwinStateResult, error) {
	query := `SELECT e.entity_id, e.entity_type, e.gateway_id,
       s.entity_id IS NOT NULL AS has_state,
       s.reported_state, s.desired_state,
       s.reported_version, s.desired_version,
       s.last_reported_at, s.last_desired_at, s.updated_at
FROM public.twin_entities AS e
JOIN public.sensors AS sens ON sens.gateway_id = e.gateway_id AND sens.sensor_id = $1
JOIN public.twin_entities AS parent_e ON parent_e.gateway_id = e.gateway_id AND parent_e.entity_type = 'Gateway'
JOIN public.twin_relationships AS r ON r.source_entity_id = parent_e.id AND r.target_entity_id = e.id AND r.relationship_type = 'hasSensor'
JOIN public.gateways AS g ON g.gateway_id = e.gateway_id
JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id AND ug.user_id = $2
LEFT JOIN public.twin_states AS s ON s.entity_id = e.id
WHERE e.id = $3
  AND e.entity_id = $4
  AND e.entity_type = $5
  AND e.gateway_id = $6`

	rows, err := r.database.Query(ctx, query, entity.SensorID, actor, entity.InternalID, entity.PublicURN, string(entity.Type), entity.GatewayID)
	if err != nil {
		return nil, classifyReaderError(err)
	}
	defer rows.Close()

	return scanStateRow(rows, entity)
}

func scanStateRow(rows pgx.Rows, entity ResolvedEntity) (*TwinStateResult, error) {
	if !rows.Next() {
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, classifyReaderError(err)
		}
		return nil, ErrNotFound
	}

	var (
		entityID        string
		entityType      string
		gatewayID       string
		hasState        bool
		reportedBytes   []byte
		desiredBytes    []byte
		reportedVersion *int64
		desiredVersion  *int64
		lastReportedAt  *time.Time
		lastDesiredAt   *time.Time
		updatedAt       *time.Time
	)
	if err := rows.Scan(
		&entityID,
		&entityType,
		&gatewayID,
		&hasState,
		&reportedBytes,
		&desiredBytes,
		&reportedVersion,
		&desiredVersion,
		&lastReportedAt,
		&lastDesiredAt,
		&updatedAt,
	); err != nil {
		rows.Close()
		return nil, classifyReaderError(err)
	}

	// Consume and close stream before checking rows.Err()
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, classifyReaderError(err)
	}

	if !hasState {
		return nil, fmt.Errorf("%w: missing twin_states row for entity %s", ErrUnexpected, entity.PublicURN)
	}

	var repVer int64
	if reportedVersion != nil {
		repVer = *reportedVersion
	}
	var desVer int64
	if desiredVersion != nil {
		desVer = *desiredVersion
	}

	var lastReportedUTC *time.Time
	if lastReportedAt != nil {
		t := lastReportedAt.UTC()
		lastReportedUTC = &t
	}
	var lastDesiredUTC *time.Time
	if lastDesiredAt != nil {
		t := lastDesiredAt.UTC()
		lastDesiredUTC = &t
	}

	var updUTC time.Time
	if updatedAt != nil {
		updUTC = updatedAt.UTC()
	}

	return &TwinStateResult{
		ID:              entity.PublicURN,
		Type:            entity.Type,
		GatewayID:       entity.GatewayID,
		ReportedState:   json.RawMessage(reportedBytes),
		DesiredState:    json.RawMessage(desiredBytes),
		ReportedVersion: repVer,
		DesiredVersion:  desVer,
		LastReportedAt:  lastReportedUTC,
		LastDesiredAt:   lastDesiredUTC,
		UpdatedAt:       updUTC,
	}, nil
}

// ReadTemporalHistory is deferred to Task 4.4.
func (r *PostgresTwinReader) ReadTemporalHistory(_ context.Context, _ uuid.UUID, _ ResolvedEntity, _ TwinHistoryQuery) (*TwinHistoryResult, error) {
	return nil, ErrUnexpected
}

func classifyReaderError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, err)
	}
	if errors.Is(err, puddle.ErrClosedPool) {
		return fmt.Errorf("%w: connection pool closed", ErrDependencyUnavailable)
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return fmt.Errorf("%w: %w", ErrDependencyUnavailable, err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "53") || strings.HasPrefix(pgErr.Code, "57") {
			return fmt.Errorf("%w: database unavailable (code %s)", ErrDependencyUnavailable, pgErr.Code)
		}
	}
	return fmt.Errorf("%w: %w", ErrUnexpected, err)
}
