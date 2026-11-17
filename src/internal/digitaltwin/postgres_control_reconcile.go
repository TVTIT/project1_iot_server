package digitaltwin

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type reconciliationCommand struct {
	id                   uuid.UUID
	status               string
	version              int64
	issuedAt             time.Time
	expiresAt            time.Time
	stage6Managed        bool
	controlVersion       int
	action               string
	targetType           string
	origin               string
	rootID               *uuid.UUID
	parentID             *uuid.UUID
	originalActorID      *uuid.UUID
	idempotencyKey       *string
	requestedFrequency   *string
	parameters           []byte
	blocked              bool
	autoSuccessorsUsed   int
	errorCode            *string
	lateResultBytes      []byte
	lateResultHash       *string
	lateResultReceivedAt *time.Time
}

// ReconcileSnapshot rechecks a transient R6 candidate under the Sensor locks,
// records one finite decision, and may atomically create at most one successor.
// It never writes reported configuration or creates an HTTP request receipt.
func (r *PostgresControlRepository) ReconcileSnapshot(
	ctx context.Context,
	query ControlQueryContext,
	snapshot ConfigurationSnapshot,
	localDeadline time.Time,
) (ReconciliationOutcome, error) {
	if ctx == nil || localDeadline.IsZero() {
		return ReconciliationOutcome{}, ErrInvalidInput
	}
	if err := validateReconciliationCandidate(query, snapshot); err != nil {
		return ReconciliationOutcome{}, err
	}
	if !time.Now().Before(localDeadline) {
		return ReconciliationOutcome{}, ErrControlQueryExpired
	}
	if r == nil || isNilDependency(r.db) {
		return ReconciliationOutcome{}, ErrDependencyUnavailable
	}
	if !r.config.Stage6Enabled {
		return ReconciliationOutcome{}, ErrDependencyUnavailable
	}

	bounded, deadlineCancel := context.WithDeadline(ctx, localDeadline)
	defer deadlineCancel()
	bounded, timeoutCancel := context.WithTimeout(bounded, r.config.DBTimeout)
	defer timeoutCancel()
	tx, err := r.db.BeginTx(bounded, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
	}
	defer rollbackTx(tx)
	if _, err = tx.Exec(bounded, `SELECT set_config('lock_timeout',$1,true)`,
		fmt.Sprintf("%dms", r.config.DBTimeout.Milliseconds())); err != nil {
		return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
	}

	identity, _ := ParseEntityURN(query.EntityURN) // validated before opening the transaction
	entity, desired, reported, desiredVersion, confirmedVersion, confirmedVersionValid, err :=
		lockReconciliationTarget(bounded, tx, query, identity)
	if err != nil {
		return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
	}
	if desiredVersion != int64(query.DesiredVersion) {
		return ReconciliationOutcome{}, ErrControlQueryContextChanged
	}
	if !time.Now().Before(localDeadline) {
		return ReconciliationOutcome{}, ErrControlQueryExpired
	}
	now, err := databaseNow(bounded, tx)
	if err != nil {
		return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
	}
	if err = ExpireSensorCommandsTx(bounded, tx, entity, now); err != nil {
		return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
	}
	active, err := hasActiveControlCommand(bounded, tx, entity)
	if err != nil {
		return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
	}

	desiredInterval, desiredOK := controlIntervalFromState(desired, false)
	reportedInterval, reportedOK := controlIntervalFromState(reported, true)
	confirmedConfigurationVersion := Version(0)
	if confirmedVersion != nil {
		confirmedConfigurationVersion = *confirmedVersion
	}
	if !desiredOK || desiredInterval == nil ||
		*desiredInterval < MinSampleIntervalUS || *desiredInterval > MaxSampleIntervalUS {
		desiredOK = false
		desiredInterval = int64Pointer(MinSampleIntervalUS)
	}
	input := ReconciliationInput{
		DesiredVersion: Version(desiredVersion), DesiredIntervalUS: *desiredInterval,
		ConfirmedConfigurationVersion: confirmedConfigurationVersion, ReportedIntervalUS: reportedInterval,
		ActiveCommand: active, Snapshot: snapshot,
	}
	if !desiredOK || !reportedOK || !confirmedVersionValid {
		input.ManualUnconfirmed = true
	}

	current, found, err := currentReconciliationCommand(bounded, tx, entity, desiredVersion)
	if err != nil {
		return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
	}
	var root reconciliationCommand
	var lineageValid bool
	if found {
		input.HasCommittedUserIntent = true
		root, lineageValid, err = lockAndValidateReconciliationLineage(bounded, tx, entity, current)
		if err != nil {
			return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
		}
		if lineageValid {
			frequency, freqErr := parseFrequency(json.RawMessage(*root.requestedFrequency))
			currentInterval, currentParamsOK := current.sampleInterval()
			rootInterval, rootParamsOK := root.sampleInterval()
			if freqErr != nil || frequency.SampleIntervalUS() != *desiredInterval ||
				!currentParamsOK || !rootParamsOK || currentInterval != *desiredInterval || rootInterval != *desiredInterval {
				lineageValid = false
			}
		}
		if lineageValid {
			input.Cancelled = current.blocked || root.blocked || current.status == string(CommandCancelled)
			input.AutoSuccessorsUsed = root.autoSuccessorsUsed
			input.ManualUnconfirmed = input.ManualUnconfirmed ||
				reconciliationFailureNeedsManual(current.errorCode) ||
				reconciliationFailureNeedsManual(root.errorCode)
			currentLateFailureManual, evidenceErr := lateResultNeedsManual(bounded, tx, current, query.GatewayID, query.EntityURN)
			if evidenceErr != nil {
				return ReconciliationOutcome{}, reconciliationDBError(evidenceErr, localDeadline)
			}
			rootLateFailureManual, evidenceErr := lateResultNeedsManual(bounded, tx, root, query.GatewayID, query.EntityURN)
			if evidenceErr != nil {
				return ReconciliationOutcome{}, reconciliationDBError(evidenceErr, localDeadline)
			}
			input.ManualUnconfirmed = input.ManualUnconfirmed ||
				currentLateFailureManual || rootLateFailureManual
		} else {
			input.ManualUnconfirmed = true
		}
	}
	decision, err := decideControlReconciliation(input)
	if err != nil {
		return ReconciliationOutcome{}, err
	}
	outcome := ReconciliationOutcome{Decision: decision, DesiredVersion: Version(desiredVersion)}
	if decision == ReconciliationSuccessorCreated {
		if desiredVersion == math.MaxInt64 {
			return ReconciliationOutcome{}, ErrDesiredVersionOverflow
		}
		if !time.Now().Before(localDeadline) {
			return ReconciliationOutcome{}, ErrControlQueryExpired
		}
		if _, err = tx.Exec(bounded, `SELECT pg_advisory_xact_lock(3290,52)`); err != nil {
			return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
		}
		var activeCount int
		if err = tx.QueryRow(bounded, `SELECT count(*) FROM public.twin_commands
			WHERE target_entity_type='Sensor' AND status IN ('pending','published','acknowledged')`).Scan(&activeCount); err != nil {
			return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
		}
		if activeCount >= r.config.MaxPendingOutbox {
			return ReconciliationOutcome{}, errors.Join(ErrDependencyUnavailable, ErrControlCapacity)
		}
		if !time.Now().Before(localDeadline) {
			return ReconciliationOutcome{}, ErrControlQueryExpired
		}
		outcome, err = createReconciliationSuccessor(bounded, tx, r, entity, query, snapshot,
			current, root, *desiredInterval, desiredVersion, now)
		if err != nil {
			return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
		}
	} else {
		if !time.Now().Before(localDeadline) {
			return ReconciliationOutcome{}, ErrControlQueryExpired
		}
		if _, err = tx.Exec(bounded, `UPDATE public.twin_states
			SET last_reconciled_at=$2,last_reconcile_decision=$3
			WHERE entity_id=$1 AND desired_version=$4`, entity, now, string(decision), desiredVersion); err != nil {
			return ReconciliationOutcome{}, reconciliationDBError(err, localDeadline)
		}
	}
	if !time.Now().Before(localDeadline) {
		return ReconciliationOutcome{}, ErrControlQueryExpired
	}
	if err = tx.Commit(bounded); err != nil {
		return ReconciliationOutcome{}, controlCommitError(err)
	}
	if decision == ReconciliationSuccessorCreated {
		if r.timeline != nil {
			r.timeline.MissingOrigin()
		}
		if r.notify != nil {
			r.notify()
		}
	}
	return outcome, nil
}

func validateReconciliationCandidate(query ControlQueryContext, snapshot ConfigurationSnapshot) error {
	identity, err := ParseEntityURN(query.EntityURN)
	if err != nil || identity.Type != EntityTypeSensor || identity.GatewayID != query.GatewayID ||
		identity.SensorID != query.SensorID || query.EntityID == uuid.Nil || query.DesiredVersion < 0 {
		return ErrInvalidInput
	}
	if snapshot.GatewayID != query.GatewayID || snapshot.EntityID != query.EntityURN ||
		snapshot.WatermarkVersion < 0 {
		return ErrInvalidInput
	}
	topic := "gateways/" + snapshot.GatewayID + "/responses/" + snapshot.RequestID
	payload, err := MarshalMQTTConfigurationSnapshot(snapshot)
	if err != nil {
		return err
	}
	if _, err = ParseMQTTConfigurationSnapshot(topic, payload, false); err != nil {
		return err
	}
	return nil
}

func lockReconciliationTarget(
	ctx context.Context,
	tx pgx.Tx,
	query ControlQueryContext,
	identity ParsedIdentity,
) (uuid.UUID, []byte, []byte, int64, *Version, bool, error) {
	var gateway string
	if err := tx.QueryRow(ctx, `SELECT gateway_id FROM public.gateways WHERE gateway_id=$1 FOR SHARE`,
		query.GatewayID).Scan(&gateway); err != nil {
		return uuid.Nil, nil, nil, 0, nil, false, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
	}
	var sensor string
	if err := tx.QueryRow(ctx, `SELECT sensor_id FROM public.sensors WHERE gateway_id=$1 AND sensor_id=$2 FOR UPDATE`,
		query.GatewayID, query.SensorID).Scan(&sensor); err != nil {
		return uuid.Nil, nil, nil, 0, nil, false, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
	}
	var entity uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM public.twin_entities
		WHERE id=$1 AND entity_id=$2 AND entity_type='Sensor' AND gateway_id=$3 FOR UPDATE`,
		query.EntityID, query.EntityURN, query.GatewayID).Scan(&entity); err != nil {
		return uuid.Nil, nil, nil, 0, nil, false, classifyResolverError(err, ErrNotFound, ErrDependencyUnavailable, ErrUnexpected)
	}
	var desired, reported []byte
	var desiredVersion int64
	if err := tx.QueryRow(ctx, `SELECT desired_state,reported_state,desired_version
		FROM public.twin_states WHERE entity_id=$1 FOR UPDATE`, entity).
		Scan(&desired, &reported, &desiredVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, nil, nil, 0, nil, false, ErrInconsistentGraph
		}
		return uuid.Nil, nil, nil, 0, nil, false, err
	}
	confirmedVersion, confirmedVersionValid := reportedConfigurationVersion(reported)
	var parentState bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.twin_states st
		JOIN public.twin_entities e ON e.id=st.entity_id
		WHERE e.entity_id=$1 AND e.entity_type='Gateway' AND e.gateway_id=$2)`,
		GatewayURNPrefix+identity.GatewayID, identity.GatewayID).Scan(&parentState); err != nil {
		return uuid.Nil, nil, nil, 0, nil, false, err
	}
	if !parentState {
		return uuid.Nil, nil, nil, 0, nil, false, ErrInconsistentGraph
	}
	return entity, desired, reported, desiredVersion, confirmedVersion, confirmedVersionValid, nil
}

func databaseNow(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now.UTC(), nil
}

func hasActiveControlCommand(ctx context.Context, tx pgx.Tx, entity uuid.UUID) (bool, error) {
	rows, err := tx.Query(ctx, `SELECT command_id FROM public.twin_commands
		WHERE entity_id=$1 AND status IN ('pending','published','acknowledged') FOR UPDATE`, entity)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	active := rows.Next()
	if err := rows.Err(); err != nil {
		return false, err
	}
	return active, nil
}

func controlIntervalFromState(raw []byte, configuration bool) (*int64, bool) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil || root == nil {
		return nil, false
	}
	object := root
	if configuration {
		value, exists := root["configuration"]
		if !exists {
			return nil, true
		}
		if err := json.Unmarshal(value, &object); err != nil || object == nil {
			return nil, false
		}
	}
	value, exists := object["sample_interval_us"]
	if !exists {
		return nil, true
	}
	var interval int64
	if err := json.Unmarshal(value, &interval); err != nil ||
		interval < MinSampleIntervalUS || interval > MaxSampleIntervalUS {
		return nil, false
	}
	return &interval, true
}

// reportedConfigurationVersion reads the config-specific version written by a
// confirmed command result. The aggregate twin_states.reported_version is a
// measurement/report counter and must not be used for configuration decisions.
func reportedConfigurationVersion(raw []byte) (*Version, bool) {
	var state map[string]json.RawMessage
	if err := json.Unmarshal(raw, &state); err != nil || state == nil {
		return nil, false
	}
	configuration, exists := state["configuration"]
	if !exists {
		return nil, true
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(configuration, &object); err != nil || object == nil {
		return nil, false
	}
	versionBytes, exists := object["desired_version"]
	if !exists {
		return nil, true
	}
	var version Version
	if err := json.Unmarshal(versionBytes, &version); err != nil || version < 0 {
		return nil, false
	}
	return &version, true
}

func currentReconciliationCommand(ctx context.Context, tx pgx.Tx, entity uuid.UUID, version int64) (reconciliationCommand, bool, error) {
	command, err := scanReconciliationCommand(tx.QueryRow(ctx, `SELECT command_id,status,desired_version,issued_at,expires_at,
		stage6_managed,control_version,action,target_entity_type,origin,root_command_id::text,parent_command_id::text,
		original_actor_id::text,idempotency_key,requested_frequency_hz,parameters,reconcile_blocked,auto_successors_used,error_code,
		late_result_bytes,late_result_hash,late_result_received_at
		FROM public.twin_commands WHERE entity_id=$1 AND desired_version=$2 AND control_version=1
		ORDER BY issued_at DESC,command_id DESC LIMIT 1 FOR UPDATE`, entity, version))
	if errors.Is(err, pgx.ErrNoRows) {
		return reconciliationCommand{}, false, nil
	}
	return command, err == nil, err
}

func scanReconciliationCommand(row pgx.Row) (reconciliationCommand, error) {
	var command reconciliationCommand
	var rootText, parentText, actorText *string
	if err := row.Scan(&command.id, &command.status, &command.version, &command.issuedAt, &command.expiresAt, &command.stage6Managed,
		&command.controlVersion, &command.action, &command.targetType, &command.origin,
		&rootText, &parentText, &actorText, &command.idempotencyKey, &command.requestedFrequency,
		&command.parameters, &command.blocked, &command.autoSuccessorsUsed, &command.errorCode,
		&command.lateResultBytes, &command.lateResultHash, &command.lateResultReceivedAt); err != nil {
		return reconciliationCommand{}, err
	}
	var err error
	if command.rootID, err = optionalUUID(rootText); err != nil {
		return reconciliationCommand{}, err
	}
	if command.parentID, err = optionalUUID(parentText); err != nil {
		return reconciliationCommand{}, err
	}
	if command.originalActorID, err = optionalUUID(actorText); err != nil {
		return reconciliationCommand{}, err
	}
	return command, nil
}

func optionalUUID(text *string) (*uuid.UUID, error) {
	if text == nil {
		return nil, nil
	}
	id, err := uuid.Parse(*text)
	if err != nil || id == uuid.Nil {
		return nil, ErrInvalidInput
	}
	return &id, nil
}

func lockAndValidateReconciliationLineage(
	ctx context.Context,
	tx pgx.Tx,
	entity uuid.UUID,
	current reconciliationCommand,
) (reconciliationCommand, bool, error) {
	if !current.stage6Managed || current.controlVersion != 1 || current.targetType != "Sensor" ||
		current.action != ActionUpdateConfiguration || current.rootID == nil ||
		current.originalActorID == nil || current.requestedFrequency == nil {
		return reconciliationCommand{}, false, nil
	}
	var root reconciliationCommand
	var err error
	if *current.rootID == current.id {
		root = current
	} else {
		root, err = scanReconciliationCommand(tx.QueryRow(ctx, `SELECT command_id,status,desired_version,issued_at,expires_at,
			stage6_managed,control_version,action,target_entity_type,origin,root_command_id::text,parent_command_id::text,
			original_actor_id::text,idempotency_key,requested_frequency_hz,parameters,reconcile_blocked,auto_successors_used,error_code,
			late_result_bytes,late_result_hash,late_result_received_at
			FROM public.twin_commands WHERE command_id=$1 AND entity_id=$2 FOR UPDATE`, *current.rootID, entity))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return reconciliationCommand{}, false, nil
			}
			return reconciliationCommand{}, false, err
		}
	}
	if !root.stage6Managed || root.controlVersion != 1 || root.targetType != "Sensor" ||
		root.action != ActionUpdateConfiguration || root.origin != "client" ||
		root.id != *current.rootID || root.rootID == nil || *root.rootID != root.id ||
		root.parentID != nil || root.originalActorID == nil || root.idempotencyKey == nil ||
		*root.idempotencyKey == "" || root.autoSuccessorsUsed < 0 ||
		root.autoSuccessorsUsed > maxAutomaticReconcileSuccessors ||
		*root.originalActorID != *current.originalActorID {
		return root, false, nil
	}
	switch current.origin {
	case "client":
		if current.id != root.id || current.version != root.version || current.parentID != nil {
			return root, false, nil
		}
	case "reconciliation":
		if current.parentID == nil || current.rootID == nil || *current.rootID != root.id ||
			current.version != root.version+1 || root.autoSuccessorsUsed != maxAutomaticReconcileSuccessors {
			return root, false, nil
		}
		parent, err := scanReconciliationCommand(tx.QueryRow(ctx, `SELECT command_id,status,desired_version,issued_at,expires_at,
			stage6_managed,control_version,action,target_entity_type,origin,root_command_id::text,parent_command_id::text,
			original_actor_id::text,idempotency_key,requested_frequency_hz,parameters,reconcile_blocked,auto_successors_used,error_code,
			late_result_bytes,late_result_hash,late_result_received_at
			FROM public.twin_commands WHERE command_id=$1 AND entity_id=$2 FOR UPDATE`, *current.parentID, entity))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return root, false, nil
			}
			return root, false, err
		}
		if !parent.stage6Managed || parent.controlVersion != 1 || parent.targetType != "Sensor" ||
			parent.action != ActionUpdateConfiguration || parent.rootID == nil || *parent.rootID != root.id ||
			parent.version+1 != current.version || parent.originalActorID == nil ||
			*parent.originalActorID != *root.originalActorID || parent.id != root.id || parent.origin != "client" {
			return root, false, nil
		}
	default:
		return root, false, nil
	}
	return root, true, nil
}

func lateResultNeedsManual(
	ctx context.Context,
	tx pgx.Tx,
	command reconciliationCommand,
	gatewayID, entityURN string,
) (bool, error) {
	hasBytes := len(command.lateResultBytes) != 0
	hasHash := command.lateResultHash != nil
	hasReceivedAt := command.lateResultReceivedAt != nil
	if !hasBytes && !hasHash && !hasReceivedAt {
		return false, nil
	}
	if !hasBytes || !hasHash || !hasReceivedAt || command.status != string(CommandTimeout) ||
		len(command.lateResultBytes) > MaxControlPayloadBytes ||
		*command.lateResultHash != fmt.Sprintf("%x", sha256.Sum256(command.lateResultBytes)) ||
		command.lateResultReceivedAt.IsZero() || command.lateResultReceivedAt.Before(command.issuedAt) {
		return true, nil
	}

	var commandTopic string
	var commandBytes []byte
	err := tx.QueryRow(ctx, `SELECT topic,payload_bytes FROM public.twin_outbox WHERE command_id=$1`, command.id).
		Scan(&commandTopic, &commandBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return reconciliationLateEvidenceNeedsManual(command, gatewayID, entityURN, commandTopic, commandBytes), nil
}

func reconciliationLateEvidenceNeedsManual(
	command reconciliationCommand,
	gatewayID, entityURN, commandTopic string,
	commandBytes []byte,
) bool {
	if len(command.lateResultBytes) == 0 || command.lateResultHash == nil ||
		command.lateResultReceivedAt == nil || command.status != string(CommandTimeout) ||
		len(command.lateResultBytes) > MaxControlPayloadBytes ||
		*command.lateResultHash != fmt.Sprintf("%x", sha256.Sum256(command.lateResultBytes)) ||
		command.lateResultReceivedAt.IsZero() || command.lateResultReceivedAt.Before(command.issuedAt) {
		return true
	}
	wireCommand, err := ParseMQTTCommand(commandTopic, commandBytes, false)
	if err != nil {
		return true
	}
	interval, intervalOK := command.sampleInterval()
	if commandTopic != "gateways/"+gatewayID+"/commands/"+command.id.String() ||
		wireCommand.CommandID != command.id.String() || wireCommand.GatewayID != gatewayID ||
		wireCommand.EntityID != entityURN || wireCommand.DesiredVersion != Version(command.version) ||
		!wireCommand.IssuedAt.Equal(command.issuedAt) || !wireCommand.ExpiresAt.Equal(command.expiresAt) ||
		!intervalOK || wireCommand.Parameters.SampleIntervalUS != interval {
		return true
	}

	result, err := ParseMQTTResult(
		"gateways/"+gatewayID+"/responses/"+command.id.String(),
		command.lateResultBytes, false,
	)
	if err != nil || ValidateResultForCommand(result, wireCommand) != nil ||
		result.CommandID != command.id.String() || result.GatewayID != gatewayID ||
		result.EntityID != entityURN || result.DesiredVersion != Version(command.version) {
		return true
	}
	return reconciliationLateFailureNeedsManual(result)
}

func reconciliationFailureNeedsManual(errorCode *string) bool {
	if errorCode == nil {
		return false
	}
	switch *errorCode {
	case "COMMAND_EXPIRED", "STALE_DESIRED_VERSION":
		return false
	case "UNKNOWN_SENSOR", "UNSUPPORTED_CONFIGURATION", "APPLY_FAILED":
		return true
	default:
		// The protocol allowlist is finite. Unknown persisted evidence is not
		// safe input for automatic recovery.
		return true
	}
}

func reconciliationLateFailureNeedsManual(result CommandResult) bool {
	if result.Status != CommandFailed {
		return false
	}
	return reconciliationFailureNeedsManual(&result.ErrorCode)
}

func (command reconciliationCommand) sampleInterval() (int64, bool) {
	var params struct {
		SampleIntervalUS int64 `json:"sample_interval_us"`
	}
	if err := json.Unmarshal(command.parameters, &params); err != nil ||
		params.SampleIntervalUS < MinSampleIntervalUS || params.SampleIntervalUS > MaxSampleIntervalUS {
		return 0, false
	}
	return params.SampleIntervalUS, true
}

func createReconciliationSuccessor(
	ctx context.Context,
	tx pgx.Tx,
	repo *PostgresControlRepository,
	entity uuid.UUID,
	query ControlQueryContext,
	snapshot ConfigurationSnapshot,
	current, root reconciliationCommand,
	desiredInterval, desiredVersion int64,
	now time.Time,
) (ReconciliationOutcome, error) {
	_ = snapshot // validated by the decision engine; it never supplies write values.
	if root.id == uuid.Nil || root.autoSuccessorsUsed != 0 || root.blocked ||
		current.blocked || current.version != desiredVersion {
		return ReconciliationOutcome{}, ErrControlQueryContextChanged
	}
	frequency, err := parseFrequency(json.RawMessage(*root.requestedFrequency))
	if err != nil || frequency.SampleIntervalUS() != desiredInterval {
		return ReconciliationOutcome{}, ErrInvalidInput
	}
	newVersion := desiredVersion + 1
	id, err := uuid.NewRandom()
	if err != nil {
		return ReconciliationOutcome{}, err
	}
	expiresAt := now.Add(repo.config.TTL)
	if !expiresAt.After(now) {
		return ReconciliationOutcome{}, ErrInvalidInput
	}
	command := Command{
		ProtocolVersion: 1, MessageType: "command", CommandID: id.String(),
		GatewayID: query.GatewayID, EntityID: query.EntityURN, Action: ActionUpdateConfiguration,
		Parameters:     SamplingParameters{SampleIntervalUS: desiredInterval},
		DesiredVersion: Version(newVersion), IssuedAt: now, ExpiresAt: expiresAt,
	}
	payload, err := json.Marshal(command)
	if err != nil || len(payload) > MaxControlPayloadBytes {
		return ReconciliationOutcome{}, ErrControlPayloadTooLarge
	}
	parameters, _ := json.Marshal(command.Parameters)
	tag, err := tx.Exec(ctx, `UPDATE public.twin_commands SET auto_successors_used=1
		WHERE command_id=$1 AND entity_id=$2 AND origin='client' AND stage6_managed=true
		AND auto_successors_used=0 AND reconcile_blocked=false`, root.id, entity)
	if err != nil {
		return ReconciliationOutcome{}, err
	}
	if tag.RowsAffected() != 1 {
		return ReconciliationOutcome{}, ErrControlQueryContextChanged
	}
	tag, err = tx.Exec(ctx, `UPDATE public.twin_states SET desired_version=$2,updated_at=$3,
		last_reconciled_at=$3,last_reconcile_decision='successor_created'
		WHERE entity_id=$1 AND desired_version=$4`, entity, newVersion, now, desiredVersion)
	if err != nil {
		return ReconciliationOutcome{}, err
	}
	if tag.RowsAffected() != 1 {
		return ReconciliationOutcome{}, ErrControlQueryContextChanged
	}
	if _, err = tx.Exec(ctx, `INSERT INTO public.twin_commands(command_id,entity_id,action,parameters,status,
		desired_version,idempotency_key,issued_by,issued_at,expires_at,control_version,requested_frequency_hz,
		stage6_managed,origin,root_command_id,parent_command_id,original_actor_id,reconcile_blocked,auto_successors_used)
		VALUES($1,$2,'update_configuration',$3::jsonb,'pending',$4,NULL,NULL,$5,$6,1,$7,true,'reconciliation',
		$8,$9,$10,false,0)`, id, entity, parameters, newVersion, now, expiresAt,
		*root.requestedFrequency, root.id, current.id, *root.originalActorID); err != nil {
		return ReconciliationOutcome{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO public.twin_outbox(command_id,topic,payload,payload_bytes,status,next_attempt_at,created_at)
		VALUES($1,$2,$3::jsonb,$4,'pending',$5,$5)`, id,
		"gateways/"+query.GatewayID+"/commands/"+id.String(), payload, payload, now); err != nil {
		return ReconciliationOutcome{}, err
	}
	return ReconciliationOutcome{
		Decision: ReconciliationSuccessorCreated, CommandID: id.String(), DesiredVersion: Version(newVersion),
	}, nil
}

func reconciliationDBError(err error, deadline time.Time) error {
	if !time.Now().Before(deadline) {
		return ErrControlQueryExpired
	}
	return classifyAvailabilityError(err)
}

func int64Pointer(value int64) *int64 { return &value }
