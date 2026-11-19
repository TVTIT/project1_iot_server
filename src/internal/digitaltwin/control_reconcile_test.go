package digitaltwin

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDecideControlReconciliationGuardPrecedence(t *testing.T) {
	reported := int64(500_000)
	actual := int64(500_000)
	durable := int64(500_000)
	base := ReconciliationInput{
		DesiredVersion: 3, DesiredIntervalUS: 500_000,
		ConfirmedConfigurationVersion: 3, ReportedIntervalUS: &reported,
		HasCommittedUserIntent: true, AutoSuccessorsUsed: 0,
		Snapshot: ConfigurationSnapshot{
			ProtocolVersion: 1, MessageType: "configuration_snapshot", RequestID: "b8d1db94-f5f5-4dad-8296-174e97765320",
			GatewayID: "gateway_001", EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			WatermarkVersion: 3, ConfigurationState: "configured",
			DurableSampleIntervalUS: &durable, ActualSampleIntervalUS: &actual,
		},
	}
	tests := []struct {
		name string
		edit func(*ReconciliationInput)
		want ReconciliationDecision
	}{
		{"active precedes every other guard", func(in *ReconciliationInput) {
			in.ActiveCommand = true
			in.HasCommittedUserIntent = false
			in.Cancelled = true
			in.AutoSuccessorsUsed = 1
		}, ReconciliationActiveBusy},
		{"missing committed intent", func(in *ReconciliationInput) {
			in.HasCommittedUserIntent = false
		}, ReconciliationPreconditionUnmet},
		{"higher Gateway version is manual", func(in *ReconciliationInput) {
			in.Snapshot.WatermarkVersion = 4
		}, ReconciliationManualUnconfirmed},
		{"unavailable configuration is corrupt", func(in *ReconciliationInput) {
			in.Snapshot.ConfigurationState = "unavailable"
		}, ReconciliationCorruptUnavailable},
		{"cancel block precedes budget and convergence", func(in *ReconciliationInput) {
			in.Cancelled = true
			in.AutoSuccessorsUsed = 1
		}, ReconciliationBlockedCancelled},
		{"spent budget precedes convergence", func(in *ReconciliationInput) {
			in.AutoSuccessorsUsed = 1
		}, ReconciliationBlockedBudget},
		{"all state and version values converge", func(*ReconciliationInput) {}, ReconciliationConverged},
		{"missing confirmation needs a successor", func(in *ReconciliationInput) {
			in.ReportedIntervalUS = nil
			in.ConfirmedConfigurationVersion = 2
		}, ReconciliationSuccessorCreated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := base
			tt.edit(&input)
			got, err := decideControlReconciliation(input)
			if err != nil {
				t.Fatalf("decideControlReconciliation error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("decision=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestDecideControlReconciliationRejectsInvalidIntervals(t *testing.T) {
	input := ReconciliationInput{
		DesiredVersion: 1, DesiredIntervalUS: 1,
		HasCommittedUserIntent: true,
		Snapshot: ConfigurationSnapshot{
			ProtocolVersion: 1, MessageType: "configuration_snapshot", RequestID: "b8d1db94-f5f5-4dad-8296-174e97765320",
			GatewayID: "gateway_001", EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
			ConfigurationState: "unconfigured",
		},
	}
	if _, err := decideControlReconciliation(input); err == nil {
		t.Fatal("invalid desired sample interval was accepted")
	}
}

func TestDecideControlReconciliationNeedsActualForConfiguredSnapshot(t *testing.T) {
	durable := int64(500_000)
	input := ReconciliationInput{
		DesiredVersion: 3, DesiredIntervalUS: durable,
		ConfirmedConfigurationVersion: 3, ReportedIntervalUS: &durable,
		HasCommittedUserIntent: true,
		Snapshot: ConfigurationSnapshot{
			WatermarkVersion: 3, ConfigurationState: "configured",
			DurableSampleIntervalUS: &durable, ActualSampleIntervalUS: nil,
		},
	}
	got, err := decideControlReconciliation(input)
	if err != nil {
		t.Fatalf("decideControlReconciliation error: %v", err)
	}
	if got != ReconciliationManualUnconfirmed {
		t.Fatalf("configured snapshot without actual readback decision=%q want=%q", got, ReconciliationManualUnconfirmed)
	}
}

func TestDecideControlReconciliationNeedsActualForUnconfiguredSnapshot(t *testing.T) {
	input := ReconciliationInput{
		DesiredVersion: 1, DesiredIntervalUS: 500_000,
		HasCommittedUserIntent: true,
		Snapshot: ConfigurationSnapshot{
			WatermarkVersion: 0, ConfigurationState: "unconfigured",
		},
	}
	got, err := decideControlReconciliation(input)
	if err != nil {
		t.Fatalf("decideControlReconciliation error: %v", err)
	}
	if got != ReconciliationManualUnconfirmed {
		t.Fatalf("unconfigured snapshot without actual readback decision=%q want=%q", got, ReconciliationManualUnconfirmed)
	}

	actual := int64(1_000_000)
	input.Snapshot.ActualSampleIntervalUS = &actual
	got, err = decideControlReconciliation(input)
	if err != nil {
		t.Fatalf("decideControlReconciliation with actual error: %v", err)
	}
	if got != ReconciliationSuccessorCreated {
		t.Fatalf("unconfigured snapshot with valid actual readback decision=%q want=%q", got, ReconciliationSuccessorCreated)
	}
}

func TestReportedConfigurationVersionIsIndependentAndFailClosed(t *testing.T) {
	tests := []struct {
		name      string
		state     string
		want      *Version
		wantValid bool
	}{
		{name: "missing configuration is unconfirmed", state: `{"value":42}`, wantValid: true},
		{name: "missing config version is unconfirmed", state: `{"configuration":{"sample_interval_us":500000}}`, wantValid: true},
		{name: "configuration version is parsed", state: `{"reported_version":99,"configuration":{"desired_version":"3","sample_interval_us":500000}}`, want: versionPointer(3), wantValid: true},
		{name: "numeric config version is malformed", state: `{"configuration":{"desired_version":3}}`, wantValid: false},
		{name: "overflow config version is malformed", state: `{"configuration":{"desired_version":"9223372036854775808"}}`, wantValid: false},
		{name: "malformed configuration branch is invalid", state: `{"configuration":null}`, wantValid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, valid := reportedConfigurationVersion([]byte(tt.state))
			if valid != tt.wantValid {
				t.Fatalf("valid=%t want=%t", valid, tt.wantValid)
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("version=%v want missing", *got)
				}
				return
			}
			if got == nil || *got != *tt.want {
				t.Fatalf("version=%v want=%v", got, *tt.want)
			}
		})
	}
}

func versionPointer(v Version) *Version { return &v }

func TestReconciliationFailureEvidenceKeepsTimeoutRecoverable(t *testing.T) {
	for _, tt := range []struct {
		code string
		want bool
	}{
		{code: "UNKNOWN_SENSOR", want: true},
		{code: "UNSUPPORTED_CONFIGURATION", want: true},
		{code: "APPLY_FAILED", want: true},
		{code: "COMMAND_EXPIRED", want: false},
		{code: "STALE_DESIRED_VERSION", want: false},
		{code: "UNRECOGNIZED", want: true},
	} {
		t.Run(tt.code, func(t *testing.T) {
			got := reconciliationFailureNeedsManual(&tt.code)
			if got != tt.want {
				t.Fatalf("manual=%t want=%t", got, tt.want)
			}
		})
	}
	if reconciliationFailureNeedsManual(nil) {
		t.Fatal("missing result error code must leave lost-result/timeout recovery available")
	}
}

func TestLateResultOnlyPermanentFailureBlocksRecovery(t *testing.T) {
	for _, tt := range []struct {
		name   string
		result CommandResult
		want   bool
	}{
		{name: "permanent late failure", result: CommandResult{Status: CommandFailed, ErrorCode: "UNSUPPORTED_CONFIGURATION"}, want: true},
		{name: "transient late failure", result: CommandResult{Status: CommandFailed, ErrorCode: "COMMAND_EXPIRED"}, want: false},
		{name: "late success does not confirm or block recovery", result: CommandResult{Status: CommandSucceeded}, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := reconciliationLateFailureNeedsManual(tt.result); got != tt.want {
				t.Fatalf("manual=%t want=%t", got, tt.want)
			}
		})
	}
}

func TestReconciliationLateEvidenceFailsClosedWhenUnverifiable(t *testing.T) {
	issued := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	command := Command{
		ProtocolVersion: 1, MessageType: "command", CommandID: "123e4567-e89b-12d3-a456-426614174000",
		GatewayID: "gateway_001", EntityID: "urn:ngsi-ld:Sensor:gateway_001:sensor_001",
		Action: ActionUpdateConfiguration, Parameters: SamplingParameters{SampleIntervalUS: 500_000},
		DesiredVersion: 1, IssuedAt: issued, ExpiresAt: issued.Add(time.Second),
	}
	commandBytes, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	receivedAt := command.IssuedAt.Add(500 * time.Millisecond)
	resultBytes, err := json.Marshal(CommandResult{
		ProtocolVersion: 1, MessageType: "command_result", CommandID: command.CommandID,
		GatewayID: command.GatewayID, EntityID: command.EntityID, DesiredVersion: 1,
		Status: CommandFailed, CompletedAt: receivedAt, ErrorCode: "UNSUPPORTED_CONFIGURATION",
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(resultBytes))
	intervalBytes, _ := json.Marshal(command.Parameters)
	evidence := reconciliationCommand{
		id: uuid.MustParse(command.CommandID), status: string(CommandTimeout), version: 1,
		issuedAt: command.IssuedAt, expiresAt: command.ExpiresAt, parameters: intervalBytes,
		lateResultBytes: resultBytes, lateResultHash: &hash, lateResultReceivedAt: &receivedAt,
	}
	topic := "gateways/" + command.GatewayID + "/commands/" + command.CommandID
	if !reconciliationLateEvidenceNeedsManual(evidence, command.GatewayID, command.EntityID, topic, commandBytes) {
		t.Fatal("valid permanent late failure did not require manual review")
	}

	validSuccess := CommandResult{
		ProtocolVersion: 1, MessageType: "command_result", CommandID: command.CommandID,
		GatewayID: command.GatewayID, EntityID: command.EntityID, DesiredVersion: 1,
		Status: CommandSucceeded, ReportedState: &command.Parameters, CompletedAt: receivedAt,
	}
	successBytes, err := json.Marshal(validSuccess)
	if err != nil {
		t.Fatal(err)
	}
	successHash := fmt.Sprintf("%x", sha256.Sum256(successBytes))
	successEvidence := evidence
	successEvidence.lateResultBytes = successBytes
	successEvidence.lateResultHash = &successHash
	if reconciliationLateEvidenceNeedsManual(successEvidence, command.GatewayID, command.EntityID, topic, commandBytes) {
		t.Fatal("valid late success blocked recoverable lost-result reconciliation")
	}

	corruptHash := evidence
	badHash := fmt.Sprintf("%064d", 0)
	corruptHash.lateResultHash = &badHash
	if !reconciliationLateEvidenceNeedsManual(corruptHash, command.GatewayID, command.EntityID, topic, commandBytes) {
		t.Fatal("late-result hash mismatch did not fail closed")
	}

	malformed := evidence
	malformed.lateResultBytes = []byte(`{"message_type":"command_result"}`)
	malformedHash := fmt.Sprintf("%x", sha256.Sum256(malformed.lateResultBytes))
	malformed.lateResultHash = &malformedHash
	if !reconciliationLateEvidenceNeedsManual(malformed, command.GatewayID, command.EntityID, topic, commandBytes) {
		t.Fatal("malformed late-result envelope did not fail closed")
	}

	invalidIdentity := evidence
	wrongIdentityResult := CommandResult{
		ProtocolVersion: 1, MessageType: "command_result", CommandID: command.CommandID,
		GatewayID: command.GatewayID, EntityID: "urn:ngsi-ld:Sensor:gateway_001:other",
		DesiredVersion: 1, Status: CommandFailed, CompletedAt: receivedAt,
		ErrorCode: "UNSUPPORTED_CONFIGURATION",
	}
	invalidIdentity.lateResultBytes, err = json.Marshal(wrongIdentityResult)
	if err != nil {
		t.Fatal(err)
	}
	identityHash := fmt.Sprintf("%x", sha256.Sum256(invalidIdentity.lateResultBytes))
	invalidIdentity.lateResultHash = &identityHash
	if !reconciliationLateEvidenceNeedsManual(invalidIdentity, command.GatewayID, command.EntityID, topic, commandBytes) {
		t.Fatal("late-result identity mismatch did not fail closed")
	}

	tooEarly := evidence
	beforeIssue := command.IssuedAt.Add(-time.Microsecond)
	tooEarly.lateResultReceivedAt = &beforeIssue
	if !reconciliationLateEvidenceNeedsManual(tooEarly, command.GatewayID, command.EntityID, topic, commandBytes) {
		t.Fatal("late-result arrival before command issue did not fail closed")
	}
}
