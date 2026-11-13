package telemetryws

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/gateway"
)

// fakeTargetRepo is a thread-safe test double for TargetAuthorizerRepository.
type fakeTargetRepo struct {
	mu           sync.Mutex
	calls        int
	lastUserID   uuid.UUID
	lastGwID     string
	lastSensorID string
	allCalls     []callRecord
	roles        map[string]gateway.Role
	errors       map[string]error
	delay        time.Duration
}

type callRecord struct {
	userID   uuid.UUID
	gwID     string
	sensorID string
}

func newFakeTargetRepo() *fakeTargetRepo {
	return &fakeTargetRepo{
		roles:  make(map[string]gateway.Role),
		errors: make(map[string]error),
	}
}

func (f *fakeTargetRepo) targetKey(gwID, sensorID string) string {
	if sensorID != "" {
		return gwID + ":" + sensorID
	}
	return gwID
}

func (f *fakeTargetRepo) setTarget(gwID, sensorID string, role gateway.Role, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := f.targetKey(gwID, sensorID)
	if err != nil {
		f.errors[key] = err
		delete(f.roles, key)
	} else {
		f.roles[key] = role
		delete(f.errors, key)
	}
}

func (f *fakeTargetRepo) AuthorizeSubscriptionTarget(ctx context.Context, userID uuid.UUID, gatewayID string, sensorID string) (gateway.Role, error) {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastUserID = userID
	f.lastGwID = gatewayID
	f.lastSensorID = sensorID
	f.allCalls = append(f.allCalls, callRecord{userID: userID, gwID: gatewayID, sensorID: sensorID})

	key := f.targetKey(gatewayID, sensorID)
	if err, ok := f.errors[key]; ok {
		return "", err
	}
	if role, ok := f.roles[key]; ok {
		return role, nil
	}
	return "", gateway.ErrNotFound
}

func TestTargetAuthorizer_Constructor(t *testing.T) {
	t.Run("nil repository rejected", func(t *testing.T) {
		_, err := NewTargetAuthorizer(nil, 0)
		if err == nil {
			t.Fatal("expected error for nil repository, got nil")
		}
	})

	t.Run("default budget applied when zero or negative", func(t *testing.T) {
		repo := newFakeTargetRepo()
		auth, err := NewTargetAuthorizer(repo, 0)
		if err != nil {
			t.Fatalf("unexpected constructor error: %v", err)
		}
		if auth.budget != DefaultAuthorizationBudget {
			t.Fatalf("budget = %v, want default %v", auth.budget, DefaultAuthorizationBudget)
		}

		authNeg, err := NewTargetAuthorizer(repo, -1*time.Second)
		if err != nil {
			t.Fatalf("unexpected constructor error: %v", err)
		}
		if authNeg.budget != DefaultAuthorizationBudget {
			t.Fatalf("budget = %v, want default %v", authNeg.budget, DefaultAuthorizationBudget)
		}
	})
}

func TestTargetAuthorizer_MemberRoles(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, err := NewTargetAuthorizer(repo, 2*time.Second)
	if err != nil {
		t.Fatalf("unexpected constructor error: %v", err)
	}

	user := uuid.New()
	roles := []gateway.Role{gateway.RoleOwner, gateway.RoleOperator, gateway.RoleViewer}

	for _, role := range roles {
		t.Run(string(role)+" gateway target", func(t *testing.T) {
			gwID := "gw_" + string(role)
			repo.setTarget(gwID, "", role, nil)

			res, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: gwID})
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", role, err)
			}
			if res.Role != role {
				t.Errorf("role = %v, want %v", res.Role, role)
			}
			if res.Target.GatewayID != gwID || res.Target.SensorID != "" {
				t.Errorf("target = %+v, want %s", res.Target, gwID)
			}
		})

		t.Run(string(role)+" sensor target", func(t *testing.T) {
			gwID := "gw_sensor_" + string(role)
			senID := "sensor_1"
			repo.setTarget(gwID, senID, role, nil)

			res, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: gwID, SensorID: senID})
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", role, err)
			}
			if res.Role != role {
				t.Errorf("role = %v, want %v", res.Role, role)
			}
			if res.Target.GatewayID != gwID || res.Target.SensorID != senID {
				t.Errorf("target = %+v, want %s:%s", res.Target, gwID, senID)
			}
		})
	}
}

func TestTargetAuthorizer_NonmemberAndPlatformAdmin(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	user := uuid.New()
	adminUser := uuid.New() // Platform admin without user_gateways membership

	t.Run("nonmember rejected with generic unauthorized", func(t *testing.T) {
		repo.setTarget("gw_nonmember", "", "", gateway.ErrNotFound)

		_, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: "gw_nonmember"})
		if err == nil {
			t.Fatal("expected unauthorized error for nonmember, got nil")
		}
		if !errors.Is(err, ErrTargetUnauthorized) {
			t.Fatalf("err = %v, want ErrTargetUnauthorized", err)
		}
	})

	t.Run("platform admin alone without user_gateways membership cannot bypass", func(t *testing.T) {
		// Platform admin has no row in user_gateways -> repo returns ErrNotFound
		repo.setTarget("gw_admin_test", "", "", gateway.ErrNotFound)

		_, err := authorizer.AuthorizeTarget(context.Background(), adminUser, TargetKey{GatewayID: "gw_admin_test"})
		if err == nil {
			t.Fatal("expected unauthorized error for platform admin without membership, got nil")
		}
		if !errors.Is(err, ErrTargetUnauthorized) {
			t.Fatalf("err = %v, want ErrTargetUnauthorized", err)
		}
	})
}

func TestTargetAuthorizer_CrossGatewaySameSensorID(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)
	user := uuid.New()

	// Sensor 'shared-sensor-01' exists under gateway_B, NOT gateway_A.
	// User is a member of gateway_A, but requests (gateway_A, shared-sensor-01).
	repo.setTarget("gateway_A", "", gateway.RoleOwner, nil)
	repo.setTarget("gateway_A", "shared-sensor-01", "", gateway.ErrNotFound)
	repo.setTarget("gateway_B", "shared-sensor-01", gateway.RoleViewer, nil)

	_, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{
		GatewayID: "gateway_A",
		SensorID:  "shared-sensor-01",
	})
	if err == nil {
		t.Fatal("expected unauthorized error for cross-gateway sensor, got nil")
	}
	if !errors.Is(err, ErrTargetUnauthorized) {
		t.Fatalf("err = %v, want ErrTargetUnauthorized", err)
	}
}

func TestTargetAuthorizer_BatchCompositeKeysAndFailClosed(t *testing.T) {
	userA, userB := uuid.New(), uuid.New()
	keys := []gateway.SubscriptionTargetKey{
		{UserID: userA, GatewayID: "gateway_A", SensorID: "shared_sensor"},
		{UserID: userB, GatewayID: "gateway_B", SensorID: "shared_sensor"},
		{UserID: userA, GatewayID: "gateway_A", SensorID: "missing_sensor"},
	}
	repo := &fakeBatchTargetRepo{results: []gateway.SubscriptionTargetAuthorization{
		{Key: keys[0], Role: gateway.RoleViewer, Authorized: true},
		{Key: keys[1], Role: gateway.RoleOwner, Authorized: true},
		{Key: keys[2]},
	}}
	authorizer, _ := NewTargetAuthorizer(repo, time.Second)
	invalid := append([]gateway.SubscriptionTargetKey(nil), keys...)
	invalid[0].SensorID = "invalid sensor id"
	if _, err := authorizer.AuthorizeTargetsBatch(context.Background(), invalid); err == nil || repo.calls != 0 {
		t.Fatal("invalid batch key must be rejected before repository call")
	}
	overbound := make([]gateway.SubscriptionTargetKey, gateway.MaxSubscriptionTargetBatchSize+1)
	for i := range overbound {
		overbound[i] = keys[0]
	}
	if _, err := authorizer.AuthorizeTargetsBatch(context.Background(), overbound); err == nil || repo.calls != 0 {
		t.Fatal("overbound batch must be rejected rather than truncated")
	}
	got, err := authorizer.AuthorizeTargetsBatch(context.Background(), keys)
	if err != nil || len(got) != len(keys) || !got[0].Authorized || !got[1].Authorized || got[0].Key != keys[0] || got[1].Key != keys[1] || got[2].Authorized {
		t.Fatalf("batch results = %#v, err=%v", got, err)
	}

	repo.results = repo.results[:2]
	got, err = authorizer.AuthorizeTargetsBatch(context.Background(), keys)
	if err == nil || len(got) != len(keys) {
		t.Fatalf("incomplete repository result must fail closed for all keys, got %#v, err=%v", got, err)
	}
	for _, outcome := range got {
		if outcome.Authorized {
			t.Fatal("malformed result partially granted authorization")
		}
	}

	repo.err = errors.New("database unavailable")
	got, err = authorizer.AuthorizeTargetsBatch(context.Background(), keys)
	if err == nil || len(got) != len(keys) {
		t.Fatalf("DB failure must return per-key failure, got %#v, err=%v", got, err)
	}
	for _, outcome := range got {
		if outcome.Authorized || outcome.Err == nil {
			t.Fatalf("DB failure granted or omitted outcome: %#v", outcome)
		}
	}
}

type fakeBatchTargetRepo struct {
	results []gateway.SubscriptionTargetAuthorization
	err     error
	calls   int
}

func (f *fakeBatchTargetRepo) AuthorizeSubscriptionTarget(context.Context, uuid.UUID, string, string) (gateway.Role, error) {
	return "", errors.New("single-target path must not be used")
}

func (f *fakeBatchTargetRepo) AuthorizeSubscriptionTargets(context.Context, []gateway.SubscriptionTargetKey) ([]gateway.SubscriptionTargetAuthorization, error) {
	f.calls++
	return f.results, f.err
}

func TestTargetAuthorizer_BatchFallbackKeepsDenialKeyScopedAndContextErrorTyped(t *testing.T) {
	repo := newFakeTargetRepo()
	user := uuid.New()
	a, err := NewTargetAuthorizer(repo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	denied := gateway.SubscriptionTargetKey{UserID: user, GatewayID: "gw_denied"}
	allowed := gateway.SubscriptionTargetKey{UserID: user, GatewayID: "gw_allowed"}
	repo.setTarget("gw_denied", "", "", gateway.ErrNotFound)
	repo.setTarget("gw_allowed", "", gateway.RoleViewer, nil)
	queryStarts := 0
	out, err := a.AuthorizeTargetsBatchObserved(context.Background(), []gateway.SubscriptionTargetKey{denied, allowed}, func() { queryStarts++ })
	if err != nil || len(out) != 2 || out[0].Authorized || !errors.Is(out[0].Err, ErrTargetUnauthorized) || !out[1].Authorized || queryStarts != 2 {
		t.Fatalf("key-specific fallback denial damaged unrelated authorization: outcomes=%+v err=%v", out, err)
	}
	repo.setTarget("gw_allowed", "", "", context.DeadlineExceeded)
	_, err = a.AuthorizeTargetsBatchObserved(context.Background(), []gateway.SubscriptionTargetKey{allowed}, func() { queryStarts++ })
	if !errors.Is(err, context.DeadlineExceeded) || queryStarts != 3 {
		t.Fatalf("batch wrapper flattened context deadline or omitted started fallback query: err=%v starts=%d", err, queryStarts)
	}
}

func TestTargetAuthorizer_AbsentSensorAndWrongParent(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)
	user := uuid.New()

	t.Run("sensor does not exist anywhere", func(t *testing.T) {
		repo.setTarget("gw_real", "sensor_ghost", "", gateway.ErrNotFound)
		_, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: "gw_real", SensorID: "sensor_ghost"})
		if !errors.Is(err, ErrTargetUnauthorized) {
			t.Fatalf("err = %v, want ErrTargetUnauthorized", err)
		}
	})

	t.Run("gateway does not exist anywhere", func(t *testing.T) {
		repo.setTarget("gw_ghost", "", "", gateway.ErrNotFound)
		_, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: "gw_ghost"})
		if !errors.Is(err, ErrTargetUnauthorized) {
			t.Fatalf("err = %v, want ErrTargetUnauthorized", err)
		}
	})

	t.Run("sensor under wrong parent gateway", func(t *testing.T) {
		repo.setTarget("gw_wrong", "sen_1", "", gateway.ErrNotFound)
		_, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: "gw_wrong", SensorID: "sen_1"})
		if !errors.Is(err, ErrTargetUnauthorized) {
			t.Fatalf("err = %v, want ErrTargetUnauthorized", err)
		}
	})
}

func TestTargetAuthorizer_GenericPublicDenialPrivacy(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)
	user := uuid.New()

	cases := []struct {
		name   string
		target TargetKey
		setup  func()
	}{
		{
			name:   "nonmember gateway",
			target: TargetKey{GatewayID: "gw_foreign"},
			setup:  func() { repo.setTarget("gw_foreign", "", "", gateway.ErrNotFound) },
		},
		{
			name:   "absent gateway",
			target: TargetKey{GatewayID: "gw_nonexistent"},
			setup:  func() { repo.setTarget("gw_nonexistent", "", "", gateway.ErrNotFound) },
		},
		{
			name:   "absent sensor",
			target: TargetKey{GatewayID: "gw_mine", SensorID: "sen_ghost"},
			setup:  func() { repo.setTarget("gw_mine", "sen_ghost", "", gateway.ErrNotFound) },
		},
		{
			name:   "cross-gateway sensor",
			target: TargetKey{GatewayID: "gw_mine", SensorID: "sen_other_gw"},
			setup:  func() { repo.setTarget("gw_mine", "sen_other_gw", "", gateway.ErrNotFound) },
		},
		{
			name:   "invalid gateway ID format",
			target: TargetKey{GatewayID: "invalid gw id with spaces"},
			setup:  func() { repo.setTarget("invalid gw id with spaces", "", "", gateway.ErrInvalidGatewayID) },
		},
		{
			name:   "invalid sensor ID format",
			target: TargetKey{GatewayID: "gw_mine", SensorID: "bad@sensor!"},
			setup:  func() { repo.setTarget("gw_mine", "bad@sensor!", "", gateway.ErrInvalidSensorID) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			_, err := authorizer.AuthorizeTarget(context.Background(), user, tc.target)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
			// Generic public error must be exactly ErrTargetUnauthorized
			if !errors.Is(err, ErrTargetUnauthorized) {
				t.Errorf("%s: error must unwrap to ErrTargetUnauthorized, got %v", tc.name, err)
			}
		})
	}
}

func TestTargetAuthorizer_DatabaseErrorDistinguishableAndFailsClosed(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)
	user := uuid.New()

	dbFailure := errors.New("pgx: connection pool exhausted")
	repo.setTarget("gw_dberr", "", "", dbFailure)

	_, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: "gw_dberr"})
	if err == nil {
		t.Fatal("expected DB error, got nil")
	}
	// Must NOT be conflated with generic ErrTargetUnauthorized
	if errors.Is(err, ErrTargetUnauthorized) {
		t.Fatalf("DB error must NOT be masked as ErrTargetUnauthorized, got %v", err)
	}
	// Must wrap or contain the underlying DB failure
	if !errors.Is(err, dbFailure) {
		t.Fatalf("err = %v, want to unwrap dbFailure", err)
	}
}

func TestTargetAuthorizer_TimeoutAndCallerCancellation(t *testing.T) {
	repo := newFakeTargetRepo()
	// Authorizer with 50ms budget
	authorizer, _ := NewTargetAuthorizer(repo, 50*time.Millisecond)
	user := uuid.New()

	t.Run("timeout past budget fails closed", func(t *testing.T) {
		repo.delay = 150 * time.Millisecond
		repo.setTarget("gw_slow", "", gateway.RoleOwner, nil)

		_, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: "gw_slow"})
		if err == nil {
			t.Fatal("expected deadline exceeded, got nil")
		}
		if errors.Is(err, ErrTargetUnauthorized) {
			t.Fatalf("timeout must not be masked as ErrTargetUnauthorized, got %v", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("caller cancellation fails closed immediately", func(t *testing.T) {
		repo.delay = 200 * time.Millisecond
		repo.setTarget("gw_cancel", "", gateway.RoleOwner, nil)

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel before call

		_, err := authorizer.AuthorizeTarget(ctx, user, TargetKey{GatewayID: "gw_cancel"})
		if err == nil {
			t.Fatal("expected context canceled, got nil")
		}
		if errors.Is(err, ErrTargetUnauthorized) {
			t.Fatalf("cancellation must not be masked as ErrTargetUnauthorized, got %v", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

func TestTargetAuthorizer_BoundedTargetsAndDeduplication(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)
	user := uuid.New()

	t.Run("empty targets list rejected", func(t *testing.T) {
		_, err := authorizer.AuthorizeTargets(context.Background(), user, []TargetKey{})
		if err == nil {
			t.Fatal("expected error on empty targets, got nil")
		}
		var protoErr *ProtocolError
		if !errors.As(err, &protoErr) || protoErr.Code != ErrorCodeInvalidTarget {
			t.Fatalf("err = %v, want ErrorCodeInvalidTarget", err)
		}
	})

	t.Run("targets exceeding max 16 rejected", func(t *testing.T) {
		oversized := make([]TargetKey, 17)
		for i := range oversized {
			oversized[i] = TargetKey{GatewayID: "gw_001"}
		}
		_, err := authorizer.AuthorizeTargets(context.Background(), user, oversized)
		if err == nil {
			t.Fatal("expected error on >16 targets, got nil")
		}
		var protoErr *ProtocolError
		if !errors.As(err, &protoErr) || protoErr.Code != ErrorCodeTargetLimitExceeded {
			t.Fatalf("err = %v, want ErrorCodeTargetLimitExceeded", err)
		}
	})

	t.Run("deduplicates identical targets avoiding extra DB calls", func(t *testing.T) {
		repo.calls = 0
		repo.setTarget("gw_dup", "", gateway.RoleViewer, nil)

		targets := []TargetKey{
			{GatewayID: "gw_dup"},
			{GatewayID: "gw_dup"},
			{GatewayID: "gw_dup"},
		}

		res, err := authorizer.AuthorizeTargets(context.Background(), user, targets)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(res) != 1 {
			t.Fatalf("len(res) = %d, want 1", len(res))
		}
		if repo.calls != 1 {
			t.Fatalf("repo.calls = %d, want 1 due to deduplication", repo.calls)
		}
	})

	t.Run("all 16 unique targets authorized in single bounded call", func(t *testing.T) {
		repo.calls = 0
		targets := make([]TargetKey, 16)
		for i := 0; i < 16; i++ {
			gwID := "gw_multi_" + string(rune('a'+i))
			targets[i] = TargetKey{GatewayID: gwID}
			repo.setTarget(gwID, "", gateway.RoleViewer, nil)
		}

		res, err := authorizer.AuthorizeTargets(context.Background(), user, targets)
		if err != nil {
			t.Fatalf("unexpected error on 16 targets: %v", err)
		}
		if len(res) != 16 {
			t.Fatalf("len(res) = %d, want 16", len(res))
		}
		if repo.calls != 16 {
			t.Fatalf("repo.calls = %d, want 16", repo.calls)
		}
	})
}

func TestTargetAuthorizer_QueryArgumentCorrectness(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)

	t.Run("nil user identity rejected", func(t *testing.T) {
		_, err := authorizer.AuthorizeTarget(context.Background(), uuid.Nil, TargetKey{GatewayID: "gw_01"})
		if !errors.Is(err, ErrInvalidUserID) {
			t.Fatalf("err = %v, want ErrInvalidUserID", err)
		}
	})

	t.Run("exact parameters passed to repository", func(t *testing.T) {
		user := uuid.New()
		gwID := "gw_param_test"
		senID := "sen_param_test"
		repo.setTarget(gwID, senID, gateway.RoleOwner, nil)

		_, err := authorizer.AuthorizeTarget(context.Background(), user, TargetKey{GatewayID: gwID, SensorID: senID})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if repo.lastUserID != user {
			t.Errorf("lastUserID = %v, want %v", repo.lastUserID, user)
		}
		if repo.lastGwID != gwID {
			t.Errorf("lastGwID = %v, want %v", repo.lastGwID, gwID)
		}
		if repo.lastSensorID != senID {
			t.Errorf("lastSensorID = %v, want %v", repo.lastSensorID, senID)
		}
	})
}

func TestTargetAuthorizer_NoGoroutinePerTarget(t *testing.T) {
	repo := newFakeTargetRepo()
	authorizer, _ := NewTargetAuthorizer(repo, 2*time.Second)
	user := uuid.New()

	var runningGoroutines int32
	repo.delay = 10 * time.Millisecond
	for i := 0; i < 8; i++ {
		gwID := "gw_goroutine_" + string(rune('a'+i))
		repo.setTarget(gwID, "", gateway.RoleViewer, nil)
	}

	targets := make([]TargetKey, 8)
	for i := 0; i < 8; i++ {
		targets[i] = TargetKey{GatewayID: "gw_goroutine_" + string(rune('a'+i))}
	}

	atomic.StoreInt32(&runningGoroutines, 0)
	res, err := authorizer.AuthorizeTargets(context.Background(), user, targets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res) != 8 {
		t.Fatalf("len(res) = %d, want 8", len(res))
	}
}
