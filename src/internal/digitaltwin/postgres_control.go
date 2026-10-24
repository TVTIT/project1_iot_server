package digitaltwin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrControlCapacity indicates a bounded durable-intent store is full. No intent
// is discarded to admit another, and same-key replay is still available.
var ErrControlCapacity = errors.New("control_capacity_exceeded")

// ErrControlCommitUncertain requires retry with the same key, not a new intent.
var ErrControlCommitUncertain = errors.New("control_commit_uncertain")

// ControlConfig bounds admission work and retained intent capacity.
type ControlConfig struct {
	TTL              time.Duration
	DBTimeout        time.Duration
	MaxPendingOutbox int
	MaxReceipts      int
}

// DefaultControlConfig supplies conservative admission defaults, not a throughput claim.
func DefaultControlConfig() ControlConfig {
	return ControlConfig{TTL: time.Minute, DBTimeout: 5 * time.Second, MaxPendingOutbox: 10000, MaxReceipts: 100000}
}

// PostgresControlRepository persists intent; it performs no network publication.
type PostgresControlRepository struct {
	db      TxDatabase
	config  ControlConfig
	metrics *ControlMetrics
}

// NewPostgresControlRepository constructs a bounded intent repository.
func NewPostgresControlRepository(db TxDatabase, config ControlConfig, observers ...*ControlMetrics) (*PostgresControlRepository, error) {
	if isNilDependency(db) || config.TTL < time.Second || config.TTL > 24*time.Hour || config.TTL%time.Microsecond != 0 ||
		config.DBTimeout < time.Millisecond || config.DBTimeout > time.Minute || config.MaxPendingOutbox < 1 || config.MaxPendingOutbox > 1000000 ||
		config.MaxReceipts < 1 || config.MaxReceipts > 1000000 {
		return nil, ErrInvalidInput
	}
	r := &PostgresControlRepository{db: db, config: config}
	if len(observers) > 0 {
		r.metrics = observers[0]
	}
	return r, nil
}

// Admit atomically accepts or replays one actor-scoped Sensor intent.
func (r *PostgresControlRepository) Admit(ctx context.Context, actor uuid.UUID, urn string, request DesiredRequest) (RequestReceipt, error) {
	outcome := "rejected"
	if r != nil && r.metrics != nil {
		start := time.Now()
		defer func() { r.metrics.observe(outcome, time.Since(start)) }()
	}
	if ctx == nil {
		return RequestReceipt{}, ErrInvalidInput
	}
	if actor == uuid.Nil {
		return RequestReceipt{}, ErrNilActor
	}
	identity, err := ParseEntityURN(urn)
	if err != nil {
		return RequestReceipt{}, err
	}
	// Revalidate even callers that bypass the HTTP parser; Frequency is opaque,
	// but the request's version/key are exported and can be constructed directly.
	body, err := json.Marshal(struct {
		Frequency json.Number `json:"frequency_hz"`
		Version   Version     `json:"expected_desired_version"`
	}{request.Frequency.RequestedHz(), request.ExpectedDesiredVersion})
	if err != nil {
		return RequestReceipt{}, ErrInvalidInput
	}
	checked, err := ParseDesiredRequest(body, request.IdempotencyKey)
	if err != nil || checked.Frequency.SampleIntervalUS() != request.Frequency.SampleIntervalUS() {
		return RequestReceipt{}, ErrInvalidInput
	}
	if r == nil || isNilDependency(r.db) {
		return RequestReceipt{}, ErrDependencyUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.DBTimeout)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	defer rollbackTx(tx)
	if _, err = tx.Exec(ctx, `SELECT set_config('lock_timeout',$1,true)`, fmt.Sprintf("%dms", r.config.DBTimeout.Milliseconds())); err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	entity, err := lockControlTarget(ctx, tx, actor, identity, true)
	if err != nil {
		return RequestReceipt{}, err
	}
	var desired, reported []byte
	var version int64
	err = tx.QueryRow(ctx, `SELECT desired_state,reported_state,desired_version FROM public.twin_states WHERE entity_id=$1 FOR UPDATE`, entity).Scan(&desired, &reported, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return RequestReceipt{}, ErrInconsistentGraph
	}
	if err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	response := RequestReceipt{ActorID: actor.String(), EntityID: urn, IdempotencyKey: request.IdempotencyKey, Fingerprint: request.Fingerprint()}
	var storedFingerprint string
	err = tx.QueryRow(ctx, `SELECT fingerprint,status_code,response_bytes,command_id::text,created_at FROM public.twin_command_requests WHERE actor_id=$1 AND entity_id=$2 AND idempotency_key=$3`, actor, entity, request.IdempotencyKey).
		Scan(&storedFingerprint, &response.StatusCode, &response.Response, &response.CommandID, &response.CreatedAt)
	if err == nil {
		if storedFingerprint != response.Fingerprint {
			return RequestReceipt{}, ErrIdempotencyConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return RequestReceipt{}, controlCommitError(err)
		}
		outcome = "replay"
		return response, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	if version != int64(request.ExpectedDesiredVersion) {
		return RequestReceipt{}, ErrDesiredVersionConflict
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	now = now.UTC()
	if err = ExpireSensorCommandsTx(ctx, tx, entity, now); err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.twin_commands WHERE entity_id=$1 AND status IN('pending','published','acknowledged'))`, entity).Scan(&busy); err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	if busy {
		return RequestReceipt{}, ErrCommandInProgress
	}
	unchanged := storedInterval(desired, false) == request.Frequency.SampleIntervalUS() && storedInterval(reported, true) == request.Frequency.SampleIntervalUS()
	if !unchanged && version == math.MaxInt64 {
		return RequestReceipt{}, ErrDesiredVersionOverflow
	}
	// This short final-write critical section bounds total durable storage across
	// concurrent Sensors. It is NOT an active-command/Gateway lock. No network or
	// device execution occurs here; replay and rejected busy requests skip it.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(3290,52)`); err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	var receiptCount, outboxCount int
	if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM public.twin_command_requests),(SELECT count(*) FROM public.twin_outbox WHERE status='pending')`).Scan(&receiptCount, &outboxCount); err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	if receiptCount >= r.config.MaxReceipts || (!unchanged && outboxCount >= r.config.MaxPendingOutbox) {
		return RequestReceipt{}, errors.Join(ErrDependencyUnavailable, ErrControlCapacity)
	}
	response.CreatedAt = now
	response.StatusCode = 200
	status := RequestUnchanged
	var expires *time.Time
	statusURL := ""
	if !unchanged {
		version++
		id := uuid.NewString()
		response.CommandID = &id
		deadline := now.Add(r.config.TTL)
		expires = &deadline
		status = string(CommandPending)
		response.StatusCode = 202
		statusURL = "/v1/digital-twins/" + url.PathEscape(urn) + "/commands/" + id
		command := Command{ProtocolVersion: 1, MessageType: "command", CommandID: id, GatewayID: identity.GatewayID, EntityID: urn, Action: ActionUpdateConfiguration, Parameters: SamplingParameters{request.Frequency.SampleIntervalUS()}, DesiredVersion: Version(version), IssuedAt: now, ExpiresAt: deadline}
		payload, marshalErr := json.Marshal(command)
		if marshalErr != nil || len(payload) > MaxControlPayloadBytes {
			return RequestReceipt{}, ErrUnexpected
		}
		parameters, _ := json.Marshal(command.Parameters)
		if _, err = tx.Exec(ctx, `UPDATE public.twin_states SET desired_state=desired_state || $2::jsonb,desired_version=$3,last_desired_by=$4,last_desired_at=$5,updated_at=$5 WHERE entity_id=$1`, entity, parameters, version, actor, now); err != nil {
			return RequestReceipt{}, classifyAvailabilityError(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO public.twin_commands(command_id,entity_id,action,parameters,status,desired_version,idempotency_key,issued_by,issued_at,expires_at,control_version,requested_frequency_hz)
		 VALUES($1,$2,'update_configuration',$3::jsonb,'pending',$4,$5,$6,$7,$8,1,$9)`, id, entity, parameters, version, request.IdempotencyKey, actor, now, deadline, string(request.Frequency.RequestedHz())); err != nil {
			return RequestReceipt{}, classifyAvailabilityError(err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO public.twin_outbox(command_id,topic,payload,payload_bytes,status,next_attempt_at,created_at) VALUES($1,$2,$3::jsonb,$4,'pending',$5,$5)`, id, "gateways/"+identity.GatewayID+"/commands/"+id, payload, payload, now); err != nil {
			return RequestReceipt{}, classifyAvailabilityError(err)
		}
	}
	response.Response, err = json.Marshal(struct {
		EntityID           string      `json:"entity_id"`
		CommandID          *string     `json:"command_id"`
		Status             string      `json:"status"`
		RequestedFrequency json.Number `json:"requested_frequency_hz"`
		EffectiveFrequency float64     `json:"effective_frequency_hz"`
		SampleIntervalUS   int64       `json:"sample_interval_us"`
		DesiredVersion     Version     `json:"desired_version"`
		ExpiresAt          *time.Time  `json:"expires_at,omitempty"`
		StatusURL          string      `json:"status_url,omitempty"`
	}{urn, response.CommandID, status, request.Frequency.RequestedHz(), request.Frequency.EffectiveHz(), request.Frequency.SampleIntervalUS(), Version(version), expires, statusURL})
	if err != nil || len(response.Response) > MaxControlPayloadBytes {
		return RequestReceipt{}, ErrUnexpected
	}
	if _, err = tx.Exec(ctx, `INSERT INTO public.twin_command_requests(actor_id,entity_id,idempotency_key,fingerprint,status_code,response_bytes,command_id,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, actor, entity, request.IdempotencyKey, response.Fingerprint, response.StatusCode, []byte(response.Response), response.CommandID, now); err != nil {
		return RequestReceipt{}, classifyAvailabilityError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return RequestReceipt{}, controlCommitError(err)
	}
	outcome = "accepted"
	if unchanged {
		outcome = "noop"
	}
	return response, nil
}

func controlCommitError(err error) error {
	return errors.Join(ErrDependencyUnavailable, ErrControlCommitUncertain, err)
}

// lockControlTarget follows parent SHARE -> membership SHARE -> Sensor UPDATE ->
// Entity UPDATE. It does not trust a prior resolver or platform-admin JWT claim.
func lockControlTarget(ctx context.Context, tx pgx.Tx, actor uuid.UUID, p ParsedIdentity, mutation bool) (uuid.UUID, error) {
	var gatewayID, role string
	err := tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR SHARE`, p.GatewayID).Scan(&gatewayID)
	if err != nil {
		return uuid.Nil, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
	}
	err = tx.QueryRow(ctx, `SELECT role FROM public.user_gateways WHERE gateway_id=$1 AND user_id=$2 FOR SHARE`, p.GatewayID, actor).Scan(&role)
	if err != nil {
		return uuid.Nil, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
	}
	if mutation && role != "owner" && role != "operator" {
		return uuid.Nil, ErrForbidden
	}
	if p.Type != EntityTypeSensor {
		return uuid.Nil, ErrUnsupportedCommandTarget
	}
	var sensorID string
	err = tx.QueryRow(ctx, `SELECT sensor_id FROM public.sensors WHERE gateway_id=$1 AND sensor_id=$2 FOR UPDATE`, p.GatewayID, p.SensorID).Scan(&sensorID)
	if err != nil {
		return uuid.Nil, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
	}
	var id uuid.UUID
	err = tx.QueryRow(ctx, resolveSensorEntityQuery+` FOR UPDATE OF e`, p.SensorID, GatewayURNPrefix+p.GatewayID, actor, p.URN, p.GatewayID).Scan(&id, new(string), new(string), new(string), new(string))
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrInconsistentGraph
	}
	if err != nil {
		return uuid.Nil, classifyAvailabilityError(err)
	}
	var parentState bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.twin_states s JOIN public.twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1 AND e.entity_type='Gateway' AND e.gateway_id=$2)`, GatewayURNPrefix+p.GatewayID, p.GatewayID).Scan(&parentState)
	if err != nil {
		return uuid.Nil, classifyAvailabilityError(err)
	}
	if !parentState {
		return uuid.Nil, ErrInconsistentGraph
	}
	return id, nil
}

func storedInterval(raw []byte, configuration bool) int64 {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return 0
	}
	if configuration {
		fragment := object["configuration"]
		object = nil // Unmarshal into an existing map otherwise retains root keys.
		if json.Unmarshal(fragment, &object) != nil {
			return 0
		}
	}
	var interval int64
	if json.Unmarshal(object["sample_interval_us"], &interval) != nil {
		return 0
	}
	return interval
}

// ExpireSensorCommandsTx releases all three expired active statuses, including
// legacy acknowledged. The caller MUST hold Gateway SHARE, Sensor UPDATE, Entity
// and state locks in the documented order. It neither commits nor rolls back,
// publishes, reverts desired, or changes reported. Reused by later bounded workers.
func ExpireSensorCommandsTx(ctx context.Context, tx pgx.Tx, entity uuid.UUID, now time.Time) error {
	rows, err := tx.Query(ctx, `UPDATE public.twin_commands SET status='timeout',completed_at=$2,error_code='COMMAND_EXPIRED',error_message='command expired' WHERE entity_id=$1 AND status IN('pending','published','acknowledged') AND expires_at<=$2 RETURNING command_id`, entity, now)
	if err != nil {
		return err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	_, err = tx.Exec(ctx, `UPDATE public.twin_outbox SET status='failed' WHERE command_id=ANY($1) AND status IN('pending','published')`, ids)
	return err
}
