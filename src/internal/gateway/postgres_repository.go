package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Database is the pgxpool subset required by the PostgreSQL adapter.
type Database interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type postgresRepository struct{ database Database }

// NewPostgresRepository rejects missing dependencies, including typed nils.
func NewPostgresRepository(database Database) (Repository, error) {
	if isNilDependency(database) {
		return nil, fmt.Errorf("gateway database is required")
	}
	return &postgresRepository{database: database}, nil
}

const listGatewaysQuery = `SELECT g.gateway_id, g.name, g.description, ug.role, g.created_at
FROM public.gateways AS g
JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id
WHERE ug.user_id = $1
ORDER BY g.gateway_id COLLATE "C" ASC`

const listSensorsQuery = `SELECT g.gateway_id, s.sensor_id, s.name, s.unit, s.created_at
FROM public.gateways AS g
JOIN public.user_gateways AS ug ON ug.gateway_id = g.gateway_id
LEFT JOIN public.sensors AS s ON s.gateway_id = g.gateway_id
WHERE ug.user_id = $1 AND g.gateway_id = $2
ORDER BY s.sensor_id COLLATE "C" ASC`

const gatewayRoleQuery = `SELECT ug.role
FROM public.user_gateways AS ug
JOIN public.gateways AS g ON g.gateway_id = ug.gateway_id
WHERE ug.user_id = $1 AND g.gateway_id = $2`

func (r *postgresRepository) ListGatewaysForUser(ctx context.Context, userID uuid.UUID) ([]Gateway, error) {
	rows, err := r.database.Query(ctx, listGatewaysQuery, userID)
	if err != nil {
		return nil, fmt.Errorf("query user gateways: %w", err)
	}
	defer rows.Close()
	items := make([]Gateway, 0)
	for rows.Next() {
		var item Gateway
		if err := rows.Scan(&item.GatewayID, &item.Name, &item.Description, &item.Role, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan user gateway: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user gateways: %w", err)
	}
	return items, nil
}

func (r *postgresRepository) ListSensorsForUserAndGateway(ctx context.Context, userID uuid.UUID, gatewayID string) ([]Sensor, error) {
	rows, err := r.database.Query(ctx, listSensorsQuery, userID, gatewayID)
	if err != nil {
		return nil, fmt.Errorf("query gateway sensors: %w", err)
	}
	defer rows.Close()
	items := make([]Sensor, 0)
	found := false
	for rows.Next() {
		var parent string
		var sensorID, name, unit *string
		var item Sensor
		if err := rows.Scan(&parent, &sensorID, &name, &unit, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan gateway sensor: %w", err)
		}
		found = true
		if sensorID == nil {
			continue
		} // Authorized parent with no sensors.
		if name == nil {
			return nil, fmt.Errorf("sensor name unexpectedly null")
		}
		item.SensorID, item.Name, item.Unit = *sensorID, *name, unit
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gateway sensors: %w", err)
	}
	if !found {
		return nil, ErrNotFound
	}
	return items, nil
}

func (r *postgresRepository) GetGatewayRole(ctx context.Context, userID uuid.UUID, gatewayID string) (Role, error) {
	var role Role
	if err := r.database.QueryRow(ctx, gatewayRoleQuery, userID, gatewayID).Scan(&role); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("query gateway role: %w", err)
	}
	return role, nil
}
