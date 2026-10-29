package paperload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"iot-platform/internal/gatewaysim"
)

type gatewayCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type inboundACK struct {
	engine    **TelemetryEngine
	gatewayID string
}

func (a inboundACK) Admit(topic string, payload []byte, retained bool) {
	if a.engine != nil && *a.engine != nil {
		(*a.engine).Admit(a.gatewayID, topic, payload, retained)
	}
}

// RunResult is a bounded safe summary; it never contains credentials or payloads.
type RunResult struct {
	Status              string                 `json:"status"`
	Implementation      string                 `json:"implementation_status"`
	Experiment          string                 `json:"experiment"`
	Arm                 string                 `json:"arm"`
	Profile             string                 `json:"profile"`
	Mode                string                 `json:"mode"`
	RunID               string                 `json:"run_id"`
	DurabilityMode      string                 `json:"durability_mode"`
	DurableGatewayCount int                    `json:"durable_gateway_count"`
	StartedAt           string                 `json:"started_at"`
	FinishedAt          string                 `json:"finished_at"`
	ConfiguredPhases    Phases                 `json:"configured_phases"`
	Counters            TelemetryCounters      `json:"telemetry"`
	PrimaryError        string                 `json:"primary_error,omitempty"`
	CleanupErrors       []string               `json:"cleanup_errors,omitempty"`
	PayloadHashSample   string                 `json:"payload_hash_sample,omitempty"`
	IdentitySamples     []ViewerIdentitySample `json:"identity_samples,omitempty"`
	IdentitySampling    string                 `json:"identity_sampling_qualification"`
	PhaseEvents         []PhaseEvent           `json:"phase_events,omitempty"`
	PhaseObservation    string                 `json:"phase_observation_status,omitempty"`
	E4                  *E4RuntimeSummary      `json:"e4,omitempty"`
}

// E4RuntimeSummary contains bounded public E4 provenance and accounting.
type E4RuntimeSummary struct {
	Mode                  string            `json:"mode"`
	JournalEntries        int               `json:"journal_entry_count"`
	Headers               []E4JournalHeader `json:"safe_header_samples,omitempty"`
	SamplingQualification string            `json:"sampling_qualification"`
	IgnoredValidACKs      int64             `json:"ignored_valid_acks"`
	ReplayApplicationACKs int64             `json:"replay_application_acks"`
}

// RunTelemetry creates one Paho TLS transport for each selected real Gateway,
// waits for its own positive ACK SUBACK, then executes a bounded synthetic run.
func RunTelemetry(ctx context.Context, m Manifest, manifestPath, outputDir string, observers ...func(PhaseEvent) error) (result RunResult, retErr error) {
	result = RunResult{Implementation: "P8.4b telemetry synthetic-load; no durable restart assurance", Experiment: m.Experiment, Arm: m.Arm, Profile: m.Profile, Mode: m.Mode, RunID: m.RunID, DurabilityMode: m.DurabilityMode, DurableGatewayCount: 0, ConfiguredPhases: m.Phases, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), IdentitySampling: "header_sha256_only; maximum 20 globally; not every payload; not a full-content audit"}
	if strings.TrimSpace(outputDir) == "" {
		return result, errors.New("output_directory_required")
	}
	if err := os.Mkdir(outputDir, 0700); err != nil {
		return result, errors.New("output_directory_must_be_fresh")
	}
	var journal *E4Journal
	var replay []E4ReplayBatch
	var err error
	if m.Experiment == "E4" && m.E4.Mode != "" {
		journalPath := m.E4.JournalPath
		if !filepath.IsAbs(journalPath) {
			journalPath = filepath.Join(filepath.Dir(manifestPath), journalPath)
		}
		if m.E4.Mode == "unique" {
			journal, err = CreateE4Journal(journalPath, m.E4.MaxMessages, m.E4.MaxBytes)
		} else {
			if _, _, boundsErr := syntheticStreamBounds(m); boundsErr != nil {
				return result, errors.New("e4_replay_schedule_rejected")
			}
			want64 := ScheduledOpportunities(time.Duration(m.Phases.WarmupSeconds)*time.Second, m.Traffic.RatePerSecond, 0) +
				ScheduledOpportunities(time.Duration(m.Phases.MeasureSeconds)*time.Second, m.Traffic.RatePerSecond, time.Duration(m.Traffic.RampSeconds)*time.Second) + int64(m.Traffic.BurstMessages)
			if want64 < 0 || int64(int(want64)) != want64 {
				return result, errors.New("e4_replay_schedule_rejected")
			}
			want := int(want64)
			if want != m.E4.ReplayCount {
				return result, errors.New("e4_replay_count_mismatch")
			}
			replay, err = ReadE4Journal(journalPath, m.E4.MaxMessages, m.E4.MaxBytes, m.E4.ReplayCount, m.Targets)
			if err == nil {
				for _, entry := range replay {
					if int(entry.Header.SampleCount) != m.SamplesPerMessage {
						err = errors.New("e4_replay_sample_count_mismatch")
						replay = nil
						break
					}
				}
			}
		}
		if err != nil {
			result.Status = "FAILED"
			result.PrimaryError = "e4_journal_rejected"
			result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
			_ = writeResult(outputDir, result)
			return result, err
		}
		result.E4 = &E4RuntimeSummary{Mode: m.E4.Mode, SamplingQualification: "synthetic_load_only; not SQLite/durable assurance"}
		if m.E4.Mode == "replay" {
			result.E4.JournalEntries = len(replay)
			result.E4.Headers = safeHeaderSample(replay)
		}
		defer func() {
			if journal != nil {
				if closeErr := journal.Close(); closeErr != nil {
					result.Status = "PARTIAL"
					result.PrimaryError = "e4_journal_close_failed"
					retErr = closeErr
				}
				result.E4.JournalEntries = len(journal.Headers())
				result.E4.Headers = journal.Headers()
				if len(result.E4.Headers) > 20 {
					result.E4.Headers = result.E4.Headers[:20]
				}
				_ = writeResult(outputDir, result)
			}
		}()
	}
	base := filepath.Dir(manifestPath)
	resolve := func(ref string) string {
		if filepath.IsAbs(ref) {
			return ref
		}
		return filepath.Join(base, ref)
	}
	transports := map[string]TelemetryTransport{}
	var engine *TelemetryEngine
	gateways := map[string]bool{}
	for _, target := range m.Targets {
		gateways[target.GatewayID] = true
	}
	for _, gatewayID := range sortedKeys(gateways) {
		path := resolve(m.GatewayIdentityFiles[gatewayID])
		cred, readErr := readCredential(path, gatewayID)
		if readErr != nil {
			result.Status = "FAILED"
			result.PrimaryError = "gateway_credential_rejected"
			result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
			_ = writeResult(outputDir, result)
			return result, errors.New("gateway_credential_rejected")
		}
		u, _ := url.Parse(m.TLS.MQTTEndpoint)
		u.Scheme = "ssl"
		transport, createErr := gatewaysim.NewTransport(gatewaysim.TransportOptions{GatewayID: gatewayID, BrokerURL: u.String(), ClientID: clientID(m.RunID, gatewayID), Username: cred.Username, Password: cred.Password, CAFile: resolve(m.TLS.MqttCAFile), ServerName: m.TLS.MQTTServerName, FailFastConnect: true, ConnectTimeout: 5 * time.Second, SubscribeTimeout: 5 * time.Second, PublishTimeout: time.Duration(m.PublishTimeoutMS) * time.Millisecond, MaxInflight: m.Limits.Workers}, inboundACK{engine: &engine, gatewayID: gatewayID})
		if createErr != nil {
			result.Status = "FAILED"
			result.PrimaryError = "mqtt_tls_configuration_rejected"
			result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
			_ = writeResult(outputDir, result)
			return result, errors.New("mqtt_tls_configuration_rejected")
		}
		transports[gatewayID] = transport
	}
	engine, err = NewTelemetryEngine(m, transports)
	if err != nil {
		result.Status = "FAILED"
		result.PrimaryError = "engine_initialization_failed"
		result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
		_ = writeResult(outputDir, result)
		return result, err
	}
	engine.journal = journal
	engine.replay = replay
	engine.ignoreACKBudget = m.E4.IgnoreFirstValidACKs
	if len(observers) > 0 && observers[0] != nil {
		engine.phaseObserver = observers[0]
	}
	runCtx, cancelRun := context.WithTimeout(ctx, time.Duration(m.Limits.WallSeconds)*time.Second)
	defer cancelRun()
	for _, tr := range transports {
		if err := tr.Start(); err != nil {
			for _, cleanup := range transports {
				budget := remainingBudget(runCtx, 2*time.Second)
				c, cancel := context.WithTimeout(context.Background(), budget)
				if cleanup.Stop(c) != nil {
					result.CleanupErrors = append(result.CleanupErrors, "mqtt_transport_stop_failed")
				}
				cancel()
			}
			result.Status = "FAILED"
			result.PrimaryError = "mqtt_start_failed"
			result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
			_ = writeResult(outputDir, result)
			return result, errors.New("mqtt_start_failed")
		}
	}
	status, runErr := engine.Run(runCtx)
	result.PhaseEvents = append([]PhaseEvent(nil), engine.phaseEvents...)
	result.PhaseObservation = "observed_process_local_elapsed; runner_pipe_observation_lag_unknown"
	if engine.phaseFailed {
		result.PhaseObservation = "unavailable_observer_write_failed"
		status = "PARTIAL"
		result.Status = status
	}
	result.Status = status
	result.Counters = engine.Counters()
	if result.E4 != nil {
		result.E4.IgnoredValidACKs = result.Counters.IgnoredValidACKs
		result.E4.ReplayApplicationACKs = result.Counters.ReplayApplicationACKs
	}
	result.IdentitySamples = engine.IdentitySamples()
	result.CleanupErrors = append(result.CleanupErrors, engine.CleanupErrors()...)
	result.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if runErr != nil {
		result.PrimaryError = "telemetry_run_failed"
	}
	if writeErr := writeResult(outputDir, result); writeErr != nil && runErr == nil {
		runErr = errors.New("result_artifact_write_failed")
	}
	return result, runErr
}

func safeHeaderSample(entries []E4ReplayBatch) []E4JournalHeader {
	limit := len(entries)
	if limit > 20 {
		limit = 20
	}
	out := make([]E4JournalHeader, limit)
	for i := range out {
		out[i] = entries[i].Header
	}
	return out
}

func readCredential(path, gatewayID string) (gatewayCredential, error) {
	var c gatewayCredential
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() < 1 || info.Size() > 16*1024 {
		return c, errors.New("credential_file_rejected")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return c, errors.New("credential_file_rejected")
	}
	if json.Unmarshal(b, &c) != nil || c.Username != gatewayID || c.Password == "" {
		return gatewayCredential{}, errors.New("credential_file_rejected")
	}
	return c, nil
}
func clientID(runID, gatewayID string) string {
	h := sha256.Sum256([]byte(runID + "\x00" + gatewayID))
	return "pl-" + runID[:min(len(runID), 20)] + "-" + gatewayID[:min(len(gatewayID), 20)] + "-" + hex.EncodeToString(h[:8])
}
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func writeResult(dir string, r RunResult) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "workload.json"), append(data, '\n'), 0600)
}
