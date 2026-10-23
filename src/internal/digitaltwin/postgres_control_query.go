package digitaltwin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ControlCommandView is a safe projection, never an outbox/raw diagnostics DTO.
type ControlCommandView struct {
	CommandID      string              `json:"command_id"`
	EntityID       string              `json:"entity_id"`
	Status         CommandStatus       `json:"status"`
	DesiredVersion *Version            `json:"desired_version,omitempty"`
	Parameters     *SamplingParameters `json:"parameters,omitempty"`
	IssuedAt       time.Time           `json:"issued_at"`
	ExpiresAt      time.Time           `json:"expires_at"`
	CompletedAt    *time.Time          `json:"completed_at,omitempty"`
	ErrorCode      string              `json:"error_code,omitempty"`
	ErrorMessage   string              `json:"error_message,omitempty"`
	Result         *CommandResult      `json:"result,omitempty"`
	Legacy         bool                `json:"legacy,omitempty"`
}

// ControlCommandPage is a bounded keyset page; NextCursor is empty at the end.
type ControlCommandPage struct {
	Commands   []ControlCommandView
	NextCursor string
}

// ListCommands returns only this actor's currently in-scope Sensor commands.
func (r *PostgresControlRepository) ListCommands(ctx context.Context, actor uuid.UUID, urn string, limit int, cursor string) (ControlCommandPage, error) {
	if ctx != nil && r != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.config.DBTimeout)
		defer cancel()
	}
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > 100 {
		return ControlCommandPage{}, ErrInvalidInput
	}
	var before *time.Time
	var beforeID *string
	if cursor != "" {
		if len(cursor) > 512 {
			return ControlCommandPage{}, ErrInvalidInput
		}
		data, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return ControlCommandPage{}, ErrInvalidInput
		}
		var parts []string
		if json.Unmarshal(data, &parts) != nil || len(parts) != 3 || parts[0] != urn {
			return ControlCommandPage{}, ErrInvalidInput
		}
		timeJSON, _ := json.Marshal(parts[1])
		ts, err := controlTime(timeJSON)
		if err != nil {
			return ControlCommandPage{}, ErrInvalidInput
		}
		id, err := uuid.Parse(parts[2])
		if err != nil || id.String() != parts[2] {
			return ControlCommandPage{}, ErrInvalidInput
		}
		before = &ts
		beforeID = &parts[2]
	}
	tx, entity, cancel, err := r.controlRead(ctx, actor, urn)
	if err != nil {
		return ControlCommandPage{}, err
	}
	defer cancel()
	defer rollbackTx(tx)
	rows, err := tx.Query(ctx, controlViewSelect+` WHERE c.entity_id=$1 AND ug.user_id=$2 AND ($3::timestamptz IS NULL OR (c.issued_at,c.command_id)<($3,$4::uuid)) ORDER BY c.issued_at DESC,c.command_id DESC LIMIT $5`, entity.InternalID, actor, before, beforeID, limit+1)
	if err != nil {
		return ControlCommandPage{}, classifyAvailabilityError(err)
	}
	page := ControlCommandPage{Commands: make([]ControlCommandView, 0, limit)}
	for rows.Next() {
		view, err := scanControlView(rows, entity)
		if err != nil {
			rows.Close()
			return ControlCommandPage{}, classifyAvailabilityError(err)
		}
		page.Commands = append(page.Commands, view)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ControlCommandPage{}, classifyAvailabilityError(err)
	}
	if len(page.Commands) > limit {
		page.Commands = page.Commands[:limit]
		last := page.Commands[limit-1]
		data, _ := json.Marshal([3]string{urn, last.IssuedAt.Format(time.RFC3339Nano), last.CommandID})
		page.NextCursor = base64.RawURLEncoding.EncodeToString(data)
	}
	if err = tx.Commit(ctx); err != nil {
		return ControlCommandPage{}, classifyAvailabilityError(err)
	}
	return page, nil
}

// GetCommand returns only a command belonging to the exact in-scope Sensor.
func (r *PostgresControlRepository) GetCommand(ctx context.Context, actor uuid.UUID, urn string, commandID string) (ControlCommandView, error) {
	if ctx != nil && r != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.config.DBTimeout)
		defer cancel()
	}
	id, err := uuid.Parse(commandID)
	if err != nil || id.String() != commandID {
		return ControlCommandView{}, ErrInvalidInput
	}
	tx, entity, cancel, err := r.controlRead(ctx, actor, urn)
	if err != nil {
		return ControlCommandView{}, err
	}
	defer cancel()
	defer rollbackTx(tx)
	view, err := scanControlView(tx.QueryRow(ctx, controlViewSelect+` WHERE c.entity_id=$1 AND ug.user_id=$2 AND c.command_id=$3`, entity.InternalID, actor, id), entity)
	if err != nil {
		return ControlCommandView{}, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
	}
	if err = tx.Commit(ctx); err != nil {
		return ControlCommandView{}, classifyAvailabilityError(err)
	}
	return view, nil
}

const controlViewSelect = `SELECT c.command_id::text,c.status,c.desired_version,
CASE WHEN c.control_version=1 THEN c.parameters ELSE NULL END,
c.issued_at,c.expires_at,c.completed_at,
CASE WHEN c.error_code IN('COMMAND_EXPIRED','STALE_DESIRED_VERSION','UNKNOWN_SENSOR','UNSUPPORTED_CONFIGURATION','APPLY_FAILED') THEN c.error_code ELSE NULL END,
c.control_version,c.result_bytes
FROM public.twin_commands c JOIN public.twin_entities e ON e.id=c.entity_id JOIN public.user_gateways ug ON ug.gateway_id=e.gateway_id`

func (r *PostgresControlRepository) controlRead(ctx context.Context, actor uuid.UUID, urn string) (pgx.Tx, *ResolvedEntity, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, nil, ErrInvalidInput
	}
	if actor == uuid.Nil {
		return nil, nil, nil, ErrNilActor
	}
	p, err := ParseEntityURN(urn)
	if err != nil {
		return nil, nil, nil, err
	}
	if r == nil || isNilDependency(r.db) {
		return nil, nil, nil, ErrDependencyUnavailable
	}
	bounded, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	tx, err := r.db.BeginTx(bounded, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadOnly})
	if err != nil {
		cancel()
		return nil, nil, nil, classifyAvailabilityError(err)
	}
	resolver, _ := NewPostgresResolver(tx)
	entity, err := resolver.ResolveEntity(bounded, actor, p)
	if err == nil && p.Type != EntityTypeSensor {
		err = ErrUnsupportedCommandTarget
	}
	if err != nil {
		rollbackTx(tx)
		cancel()
		return nil, nil, nil, err
	}
	return tx, entity, cancel, nil
}

type controlRow interface{ Scan(...any) error }

func scanControlView(row controlRow, entity *ResolvedEntity) (ControlCommandView, error) {
	var view ControlCommandView
	var version *int64
	var contract *int
	var parameters, result []byte
	var errorCode *string
	err := row.Scan(&view.CommandID, &view.Status, &version, &parameters, &view.IssuedAt, &view.ExpiresAt, &view.CompletedAt, &errorCode, &contract, &result)
	if err != nil {
		return ControlCommandView{}, err
	}
	view.EntityID = entity.PublicURN
	view.IssuedAt = view.IssuedAt.UTC()
	view.ExpiresAt = view.ExpiresAt.UTC()
	if view.CompletedAt != nil {
		ts := view.CompletedAt.UTC()
		view.CompletedAt = &ts
	}
	if version != nil {
		v := Version(*version)
		view.DesiredVersion = &v
	}
	if errorCode != nil {
		view.ErrorMessage = SafeCommandErrorMessage(*errorCode)
		if view.ErrorMessage != "" {
			view.ErrorCode = *errorCode
		}
	}
	view.Legacy = contract == nil || *contract != 1
	if !view.Legacy {
		params, err := controlInterval(parameters)
		if err != nil {
			return ControlCommandView{}, errors.Join(ErrUnexpected, err)
		}
		view.Parameters = &params
		if len(result) > 0 && version != nil {
			parsed, err := ParseMQTTResult("gateways/"+entity.GatewayID+"/responses/"+view.CommandID, result, false)
			command := Command{ProtocolVersion: 1, MessageType: "command", CommandID: view.CommandID, GatewayID: entity.GatewayID, EntityID: entity.PublicURN, Action: ActionUpdateConfiguration, Parameters: params, DesiredVersion: Version(*version), IssuedAt: view.IssuedAt, ExpiresAt: view.ExpiresAt}
			if err == nil && parsed.Status == view.Status && ValidateResultForCommand(parsed, command) == nil {
				view.Result = &parsed
			}
		}
	}
	return view, nil
}
