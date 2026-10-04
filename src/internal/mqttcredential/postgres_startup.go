package mqttcredential

import "context"

// Scan the current projection, not historical winners. A legacy/null authority
// is diagnostic failure. Deleted actors on modern historical rows remain valid.
func (r *PostgresRepository) ListStartupInventory(ctx context.Context, cursor string, limit int) ([]Metadata, error) {
	if limit < 1 || limit > r.scanLimit {
		return nil, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	rows, e := r.pool.Query(ctx, `SELECT c.gateway_id FROM public.gateway_mqtt_credentials c WHERE c.gateway_id>$1 ORDER BY c.gateway_id LIMIT $2`, cursor, limit)
	if e != nil {
		return nil, safeReadError(e)
	}
	var keys []string
	for rows.Next() {
		var key string
		if e = rows.Scan(&key); e != nil {
			rows.Close()
			return nil, safeReadError(e)
		}
		keys = append(keys, key)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, safeReadError(e)
	}
	result := make([]Metadata, 0, len(keys))
	for _, key := range keys {
		m, e := r.GetMetadata(ctx, key)
		if e != nil {
			return nil, e
		}
		if m.LastOperationID == [16]byte{} {
			return nil, ErrRecoveryRequired
		}
		o, e := r.ResolveCommitAmbiguity(ctx, m.LastOperationID)
		if e != nil {
			return nil, e
		}
		cp, e := r.ResolveMaintenance(ctx, m.LastOperationID)
		if e != nil {
			return nil, e
		}
		if cp.Status != MaintenanceCompleted || o.GatewayID != key {
			return nil, ErrRecoveryRequired
		}
		if m.Status == CredentialActive && (o.Status != OperationSucceeded || !o.Evidence.FreshPositiveVerified || !o.Evidence.RAMApplied || !o.Evidence.SnapshotObserved || cp.RecoveryID != nil) {
			return nil, ErrRecoveryRequired
		}
		if m.Status != CredentialActive && m.Status != CredentialRevoked {
			return nil, ErrRecoveryRequired
		}
		result = append(result, m)
	}
	return result, nil
}
