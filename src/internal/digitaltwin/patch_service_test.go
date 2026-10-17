package digitaltwin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeGatewayMetadataPatcher struct {
	calls      int
	lastCtx    context.Context
	lastActor  uuid.UUID
	lastGWID   string
	lastInput  PatchGatewayInput
	stubResult PatchGatewayResult
	stubErr    error
	delay      time.Duration
}

func (f *fakeGatewayMetadataPatcher) PatchGatewayMetadata(ctx context.Context, actor uuid.UUID, gatewayID string, input PatchGatewayInput) (PatchGatewayResult, error) {
	f.calls++
	f.lastCtx = ctx
	f.lastActor = actor
	f.lastGWID = gatewayID
	f.lastInput = input
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return PatchGatewayResult{}, ctx.Err()
		case <-time.After(f.delay):
		}
	}
	return f.stubResult, f.stubErr
}

func TestNewGatewayPatchService_ConstructorValidation(t *testing.T) {
	patcher := &fakeGatewayMetadataPatcher{}
	var typedNil *fakeGatewayMetadataPatcher

	tests := []struct {
		name    string
		patcher GatewayMetadataPatcher
		timeout time.Duration
		wantErr bool
	}{
		{"nil patcher", nil, 5 * time.Second, true},
		{"typed nil patcher", typedNil, 5 * time.Second, true},
		{"zero timeout", patcher, 0, true},
		{"negative timeout", patcher, -1 * time.Second, true},
		{"valid patcher and timeout", patcher, 5 * time.Second, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewGatewayPatchService(tc.patcher, tc.timeout)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if svc != nil {
					t.Fatalf("expected nil service, got %#v", svc)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if svc == nil {
				t.Fatal("expected non-nil service")
			}
		})
	}
}

func TestGatewayPatchService_InputValidation_NoPatcherCall(t *testing.T) {
	patcher := &fakeGatewayMetadataPatcher{}
	svc, err := NewGatewayPatchService(patcher, 5*time.Second)
	if err != nil {
		t.Fatalf("NewGatewayPatchService failed: %v", err)
	}

	validName := "AM5728 Gateway 001"
	blankName := "   "
	oversizedName := strings.Repeat("a", 257)
	nameWithNul := "Gateway\x00One"
	oversizedDesc := strings.Repeat("b", 4097)
	descWithNul := "Desc\x00WithNul"

	validInput := PatchGatewayInput{
		Name: &validName,
	}

	tests := []struct {
		name      string
		actor     uuid.UUID
		gatewayID string
		input     PatchGatewayInput
		wantErr   error
	}{
		{
			name:      "nil actor",
			actor:     uuid.Nil,
			gatewayID: "gw_001",
			input:     validInput,
			wantErr:   ErrNilActor,
		},
		{
			name:      "empty gateway id",
			actor:     uuid.New(),
			gatewayID: "",
			input:     validInput,
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "gateway id backend_service reserved",
			actor:     uuid.New(),
			gatewayID: "backend_service",
			input:     validInput,
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "gateway id with spaces",
			actor:     uuid.New(),
			gatewayID: "gw 001",
			input:     validInput,
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "empty patch input",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			input:     PatchGatewayInput{},
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "blank name after trim",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			input: PatchGatewayInput{
				Name: &blankName,
			},
			wantErr: ErrInvalidInput,
		},
		{
			name:      "oversized name",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			input: PatchGatewayInput{
				Name: &oversizedName,
			},
			wantErr: ErrInvalidInput,
		},
		{
			name:      "name with NUL",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			input: PatchGatewayInput{
				Name: &nameWithNul,
			},
			wantErr: ErrInvalidInput,
		},
		{
			name:      "oversized description",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			input: PatchGatewayInput{
				DescriptionSet: true,
				Description:    &oversizedDesc,
			},
			wantErr: ErrInvalidInput,
		},
		{
			name:      "description with NUL",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			input: PatchGatewayInput{
				DescriptionSet: true,
				Description:    &descWithNul,
			},
			wantErr: ErrInvalidInput,
		},
		{
			name:      "description supplied without DescriptionSet",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			input: PatchGatewayInput{
				DescriptionSet: false,
				Description:    &validName,
			},
			wantErr: ErrInvalidInput,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			patcher.calls = 0
			res, err := svc.PatchGateway(context.Background(), tc.actor, tc.gatewayID, tc.input)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
			if res != (PatchGatewayResult{}) {
				t.Fatalf("expected empty result on error, got %#v", res)
			}
			if patcher.calls != 0 {
				t.Fatalf("expected 0 patcher calls on validation failure, got %d", patcher.calls)
			}
		})
	}
}

func TestGatewayPatchService_ContextAndDependencies(t *testing.T) {
	patcher := &fakeGatewayMetadataPatcher{}
	svc, err := NewGatewayPatchService(patcher, 5*time.Second)
	if err != nil {
		t.Fatalf("NewGatewayPatchService failed: %v", err)
	}

	validName := "AM5728 Gateway 001"
	validInput := PatchGatewayInput{Name: &validName}

	// Nil context
	var nilCtx context.Context
	if _, err := svc.PatchGateway(nilCtx, uuid.New(), "gw_001", validInput); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil context, got %v", err)
	}

	// Pre-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.PatchGateway(ctx, uuid.New(), "gw_001", validInput); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Nil receiver
	var nilSvc *gatewayPatchService
	if _, err := nilSvc.PatchGateway(context.Background(), uuid.New(), "gw_001", validInput); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable for nil receiver, got %v", err)
	}

	// Service with nil patcher
	brokenSvc := &gatewayPatchService{patcher: nil, timeout: 5 * time.Second}
	if _, err := brokenSvc.PatchGateway(context.Background(), uuid.New(), "gw_001", validInput); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable for broken patcher, got %v", err)
	}
}

func TestGatewayPatchService_DelegationAndTimeout(t *testing.T) {
	actor := uuid.New()
	gatewayID := "gw_001"
	newName := "Updated Gateway"
	now := time.Now().UTC()

	expectedResult := PatchGatewayResult{
		ID:        "urn:ngsi-ld:Gateway:gw_001",
		Type:      EntityTypeGateway,
		GatewayID: gatewayID,
		Name:      newName,
		UpdatedAt: now,
	}

	patcher := &fakeGatewayMetadataPatcher{
		stubResult: expectedResult,
	}
	svc, err := NewGatewayPatchService(patcher, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewGatewayPatchService failed: %v", err)
	}

	input := PatchGatewayInput{Name: &newName}
	res, err := svc.PatchGateway(context.Background(), actor, gatewayID, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != expectedResult {
		t.Fatalf("res = %#v, want %#v", res, expectedResult)
	}
	if patcher.calls != 1 {
		t.Fatalf("patcher calls = %d, want 1", patcher.calls)
	}
	if patcher.lastActor != actor || patcher.lastGWID != gatewayID {
		t.Fatalf("actor or gatewayID mismatch in delegation")
	}

	// Deadline applied: verify that patcher receives context with deadline
	deadline, ok := patcher.lastCtx.Deadline()
	if !ok {
		t.Fatal("expected context to have a deadline")
	}
	if time.Until(deadline) > 50*time.Millisecond {
		t.Fatalf("deadline too far in future: %v", time.Until(deadline))
	}

	// Timeout triggered
	patcher.delay = 100 * time.Millisecond
	_, err = svc.PatchGateway(context.Background(), actor, gatewayID, input)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

type fakeSensorMetadataPatcher struct {
	calls      int
	lastCtx    context.Context
	lastActor  uuid.UUID
	lastGWID   string
	lastSensID string
	lastInput  PatchSensorInput
	stubResult PatchSensorResult
	stubErr    error
	delay      time.Duration
}

func (f *fakeSensorMetadataPatcher) PatchSensorMetadata(ctx context.Context, actor uuid.UUID, gatewayID string, sensorID string, input PatchSensorInput) (PatchSensorResult, error) {
	f.calls++
	f.lastCtx = ctx
	f.lastActor = actor
	f.lastGWID = gatewayID
	f.lastSensID = sensorID
	f.lastInput = input
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return PatchSensorResult{}, ctx.Err()
		case <-time.After(f.delay):
		}
	}
	return f.stubResult, f.stubErr
}

func TestNewSensorPatchService_ConstructorValidation(t *testing.T) {
	patcher := &fakeSensorMetadataPatcher{}
	var typedNil *fakeSensorMetadataPatcher

	tests := []struct {
		name    string
		patcher SensorMetadataPatcher
		timeout time.Duration
		wantErr bool
	}{
		{"nil patcher", nil, 5 * time.Second, true},
		{"typed nil patcher", typedNil, 5 * time.Second, true},
		{"zero timeout", patcher, 0, true},
		{"negative timeout", patcher, -1 * time.Second, true},
		{"valid patcher and timeout", patcher, 5 * time.Second, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewSensorPatchService(tc.patcher, tc.timeout)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if svc != nil {
					t.Fatalf("expected nil service, got %#v", svc)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if svc == nil {
				t.Fatal("expected non-nil service")
			}
		})
	}
}

func TestSensorPatchService_InputValidation_NoPatcherCall(t *testing.T) {
	patcher := &fakeSensorMetadataPatcher{}
	svc, err := NewSensorPatchService(patcher, 5*time.Second)
	if err != nil {
		t.Fatalf("NewSensorPatchService failed: %v", err)
	}

	validName := "Temperature Sensor"
	blankName := "   "
	oversizedName := strings.Repeat("a", 257)
	nameWithNul := "Sensor\x00One"

	validInput := PatchSensorInput{
		Name: &validName,
	}

	tests := []struct {
		name      string
		actor     uuid.UUID
		gatewayID string
		sensorID  string
		input     PatchSensorInput
		wantErr   error
	}{
		{
			name:      "nil actor",
			actor:     uuid.Nil,
			gatewayID: "gw_001",
			sensorID:  "sensor_001",
			input:     validInput,
			wantErr:   ErrNilActor,
		},
		{
			name:      "invalid gateway ID",
			actor:     uuid.New(),
			gatewayID: "invalid/gw",
			sensorID:  "sensor_001",
			input:     validInput,
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "reserved gateway ID",
			actor:     uuid.New(),
			gatewayID: "backend_service",
			sensorID:  "sensor_001",
			input:     validInput,
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "invalid sensor ID",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			sensorID:  "invalid*sensor",
			input:     validInput,
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "nil name in input",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			sensorID:  "sensor_001",
			input:     PatchSensorInput{Name: nil},
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "blank name in input",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			sensorID:  "sensor_001",
			input:     PatchSensorInput{Name: &blankName},
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "oversized name in input",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			sensorID:  "sensor_001",
			input:     PatchSensorInput{Name: &oversizedName},
			wantErr:   ErrInvalidInput,
		},
		{
			name:      "name with NUL",
			actor:     uuid.New(),
			gatewayID: "gw_001",
			sensorID:  "sensor_001",
			input:     PatchSensorInput{Name: &nameWithNul},
			wantErr:   ErrInvalidInput,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			patcher.calls = 0
			res, err := svc.PatchSensor(context.Background(), tc.actor, tc.gatewayID, tc.sensorID, tc.input)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
			if res != (PatchSensorResult{}) {
				t.Fatalf("expected empty result on error, got %#v", res)
			}
			if patcher.calls != 0 {
				t.Fatalf("expected 0 patcher calls on validation failure, got %d", patcher.calls)
			}
		})
	}
}

func TestSensorPatchService_ContextAndDependencies(t *testing.T) {
	patcher := &fakeSensorMetadataPatcher{}
	svc, err := NewSensorPatchService(patcher, 5*time.Second)
	if err != nil {
		t.Fatalf("NewSensorPatchService failed: %v", err)
	}

	validName := "Temperature Sensor"
	validInput := PatchSensorInput{Name: &validName}

	// Nil context
	var nilCtx context.Context
	if _, err := svc.PatchSensor(nilCtx, uuid.New(), "gw_001", "sensor_001", validInput); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput for nil context, got %v", err)
	}

	// Pre-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.PatchSensor(ctx, uuid.New(), "gw_001", "sensor_001", validInput); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Nil receiver
	var nilSvc *sensorPatchService
	if _, err := nilSvc.PatchSensor(context.Background(), uuid.New(), "gw_001", "sensor_001", validInput); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable for nil receiver, got %v", err)
	}

	// Service with nil patcher
	brokenSvc := &sensorPatchService{patcher: nil, timeout: 5 * time.Second}
	if _, err := brokenSvc.PatchSensor(context.Background(), uuid.New(), "gw_001", "sensor_001", validInput); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("expected ErrDependencyUnavailable for broken patcher, got %v", err)
	}
}

func TestSensorPatchService_DelegationAndTimeout(t *testing.T) {
	actor := uuid.New()
	gatewayID := "gw_001"
	sensorID := "sensor_001"
	newName := "Updated Sensor"
	unit := "degC"
	now := time.Now().UTC()

	expectedResult := PatchSensorResult{
		ID:        "urn:ngsi-ld:Sensor:gw_001:sensor_001",
		Type:      EntityTypeSensor,
		GatewayID: gatewayID,
		SensorID:  sensorID,
		Name:      newName,
		Unit:      &unit,
		UpdatedAt: now,
	}

	patcher := &fakeSensorMetadataPatcher{
		stubResult: expectedResult,
	}
	svc, err := NewSensorPatchService(patcher, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewSensorPatchService failed: %v", err)
	}

	input := PatchSensorInput{Name: &newName}
	res, err := svc.PatchSensor(context.Background(), actor, gatewayID, sensorID, input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != expectedResult {
		t.Fatalf("res = %#v, want %#v", res, expectedResult)
	}
	if patcher.calls != 1 {
		t.Fatalf("patcher calls = %d, want 1", patcher.calls)
	}
	if patcher.lastActor != actor || patcher.lastGWID != gatewayID || patcher.lastSensID != sensorID {
		t.Fatalf("actor or IDs mismatch in delegation: actor=%v gw=%v s=%v", patcher.lastActor, patcher.lastGWID, patcher.lastSensID)
	}

	// Deadline applied: verify that patcher receives context with deadline
	deadline, ok := patcher.lastCtx.Deadline()
	if !ok {
		t.Fatal("expected context to have a deadline")
	}
	if time.Until(deadline) > 50*time.Millisecond {
		t.Fatalf("deadline too far in future: %v", time.Until(deadline))
	}

	// Timeout triggered
	patcher.delay = 100 * time.Millisecond
	_, err = svc.PatchSensor(context.Background(), actor, gatewayID, sensorID, input)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}
