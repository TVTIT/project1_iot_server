package paperload

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const maxControlResponse = 64 << 10

// ControlResult is the bounded, credential-free summary written by the REST producer.
type ControlResult struct {
	Status           string              `json:"status"`
	Implementation   string              `json:"implementation_status"`
	Experiment       string              `json:"experiment"`
	Arm              string              `json:"arm"`
	Profile          string              `json:"profile"`
	RunID            string              `json:"run_id"`
	StartedAt        string              `json:"started_at"`
	FinishedAt       string              `json:"finished_at"`
	OriginMissing    string              `json:"server_origin_latency"`
	Commands         []ControlRecord     `json:"commands"`
	PhaseEvents      []ControlPhaseEvent `json:"phase_events,omitempty"`
	PhaseObservation string              `json:"phase_observation_status,omitempty"`
	Planned          int                 `json:"planned"`
	Sent             int                 `json:"sent"`
	Skipped          int                 `json:"skipped"`
	Pending          int                 `json:"pending"`
	PrimaryError     string              `json:"primary_error,omitempty"`
}

// ControlPhaseEvent contains only bounded command ordinals and process-local elapsed time.
type ControlPhaseEvent struct {
	Event              string `json:"event"`
	CommandOrdinal     int    `json:"command_ordinal,omitempty"`
	ElapsedNS          int64  `json:"elapsed_ns"`
	ClockQualification string `json:"clock_qualification"`
}

const maxControlPhaseEvents = 3003 // three lifecycle events per bounded command plus stop events

// ControlRecord contains one bounded command outcome and client-side timing only.
type ControlRecord struct {
	GatewayID               string      `json:"gateway_id"`
	SensorID                string      `json:"sensor_id"`
	ActorLabel              string      `json:"actor_label"`
	FrequencyHz             json.Number `json:"frequency_hz"`
	ExpectedDesiredVersion  string      `json:"expected_desired_version"`
	IdempotencyKey          string      `json:"idempotency_key"`
	Classification          string      `json:"classification"`
	AdmissionClassification string      `json:"admission_classification"`
	FreshnessQualification  string      `json:"freshness_qualification,omitempty"`
	TerminalStatus          string      `json:"terminal_status,omitempty"`
	CommandID               string      `json:"command_id,omitempty"`
	HTTPStatus              int         `json:"http_status,omitempty"`
	RoundTripMS             int64       `json:"client_round_trip_ms,omitempty"`
	RoundTripMeasurement    string      `json:"round_trip_measurement"`
	Error                   string      `json:"error,omitempty"`
}

type controlReceipt struct {
	CommandID      string `json:"command_id"`
	Status         string `json:"status"`
	DesiredVersion string `json:"desired_version"`
}
type controlView struct {
	CommandID string `json:"command_id"`
	Status    string `json:"status"`
	Result    *struct {
		CommandID      string `json:"command_id"`
		DesiredVersion string `json:"desired_version"`
		Status         string `json:"status"`
	} `json:"result,omitempty"`
}
type controlState struct {
	DesiredVersion string          `json:"desired_version"`
	ReportedState  json.RawMessage `json:"reported_state"`
}

var errControlResponseTooLarge = errors.New("response_limit_exceeded")

// RunControl submits a finite command stream using only the existing HTTPS REST API.
func RunControl(ctx context.Context, m Manifest, manifestPath, outputDir string, observers ...func(ControlPhaseEvent) error) (ControlResult, error) {
	r := ControlResult{Status: "FAILED", Implementation: "P8.4.10 HTTPS REST control producer only; external durable simulator required", Experiment: m.Experiment, Arm: m.Arm, Profile: m.Profile, RunID: m.RunID, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), OriginMissing: "unavailable: client cannot observe Go HTTP-entry/PUBACK/result-commit origin", Planned: len(m.Commands)}
	if err := os.Mkdir(outputDir, 0700); err != nil {
		return r, errors.New("output_directory_must_be_fresh")
	}
	base := filepath.Dir(manifestPath)
	resolve := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(base, path)
	}
	tokens, err := readViewerTokens(resolve(m.IdentityFiles["control"]))
	if err != nil {
		r.PrimaryError = "control_identity_rejected"
		return finishControl(outputDir, r, err)
	}
	ca, err := readBoundedRegular(resolve(m.TLS.HTTPCAFile), 1<<20)
	if err != nil {
		r.PrimaryError = "https_ca_rejected"
		return finishControl(outputDir, r, errors.New("https_ca_rejected"))
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		r.PrimaryError = "https_ca_rejected"
		return finishControl(outputDir, r, errors.New("https_ca_rejected"))
	}
	endpoint, _ := url.Parse(m.TLS.HTTPEndpoint)
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: endpoint.Hostname()}, MaxIdleConns: 2, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second, DisableCompression: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	origin := time.Now()
	var observer func(ControlPhaseEvent) error
	if len(observers) > 0 {
		observer = observers[0]
	}
	measurementFailed := false
	emit := func(name string, ordinal int) {
		if len(r.PhaseEvents) >= maxControlPhaseEvents {
			return
		}
		event := ControlPhaseEvent{Event: name, CommandOrdinal: ordinal, ElapsedNS: time.Since(origin).Nanoseconds(), ClockQualification: "process-local elapsed from RunControl origin; not server latency"}
		r.PhaseEvents = append(r.PhaseEvents, event)
		if observer != nil && observer(event) != nil {
			measurementFailed = true
		}
	}
	deadline := origin.Add(time.Duration(m.Limits.WallSeconds) * time.Second)
	runCtx, cancelRun := context.WithDeadline(ctx, deadline)
	defer cancelRun()
	drainBudget := time.Duration(m.Phases.DrainSeconds) * time.Second
	requestHistory := make(map[string]bool, len(m.Commands))
	covered := 0
	incomplete := false
	for index, spec := range m.Commands {
		if runCtx.Err() != nil {
			break
		}
		targetAt := origin.Add(time.Duration(spec.OffsetMS) * time.Millisecond)
		if !waitUntil(runCtx, targetAt) {
			break
		}
		if time.Until(deadline) < drainBudget+time.Second {
			break
		}
		covered = index + 1
		record := ControlRecord{GatewayID: spec.GatewayID, SensorID: spec.SensorID, ActorLabel: spec.ActorLabel, FrequencyHz: spec.FrequencyHz, ExpectedDesiredVersion: spec.ExpectedDesiredVersion, IdempotencyKey: spec.IdempotencyKey, RoundTripMeasurement: "PATCH HTTP client round trip; not server dispatch/E2E latency"}
		token := tokens[spec.ActorLabel]
		if token == "" {
			record.Classification = "rejected"
			record.Error = "actor_identity_missing"
			r.Commands = append(r.Commands, record)
			r.Skipped++
			continue
		}
		keyPreviouslySent := requestHistory[spec.IdempotencyKey]
		requestHistory[spec.IdempotencyKey] = true
		r.Sent++
		// Preserve lifecycle evidence for every command; the manifest caps the stream.
		emit("request_begin", index+1)
		patchStart := time.Now()
		body := []byte(fmt.Sprintf(`{"frequency_hz":%s,"expected_desired_version":%q}`, string(spec.FrequencyHz), spec.ExpectedDesiredVersion))
		path := controlEntityPath(spec.GatewayID, spec.SensorID)
		resp, data, requestErr := doControl(runCtx, client, m.TLS.HTTPEndpoint, "PATCH", path+"/desired-state", token, spec.IdempotencyKey, body)
		if requestErr != nil && ctx.Err() != nil && time.Now().Before(deadline) {
			// Resolve an interrupted admission with the exact same key/body: the
			// existing receipt contract makes this safe and avoids orphaning work.
			retryCtx, retryCancel := context.WithDeadline(context.Background(), deadline)
			resp, data, requestErr = doControl(retryCtx, client, m.TLS.HTTPEndpoint, "PATCH", path+"/desired-state", token, spec.IdempotencyKey, body)
			retryCancel()
			keyPreviouslySent = true
		}
		record.RoundTripMS = time.Since(patchStart).Milliseconds()
		emit("admission_observed", index+1)
		if requestErr != nil {
			record.Classification = "admission_unresolved"
			record.Error = "https_request_failed"
			if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
				record.Classification = "timeout"
				record.Error = "request_deadline"
			}
			r.Commands = append(r.Commands, record)
			r.Pending++ // No response means admission may have committed before the disconnect.
			incomplete = true
			break
		}
		record.HTTPStatus = resp.StatusCode
		if resp.StatusCode == 200 {
			record.Classification = classifyControlHTTP(resp.StatusCode, data)
			record.AdmissionClassification = "noop_200"
			record.FreshnessQualification = "no_op_receipt_replay_indistinguishable_from_fresh_no_op"
			r.Commands = append(r.Commands, record)
			continue
		}
		if resp.StatusCode != 202 {
			record.Classification = classifyControlError(resp.StatusCode, data)
			record.AdmissionClassification = record.Classification
			r.Commands = append(r.Commands, record)
			continue
		}
		var receipt controlReceipt
		if json.Unmarshal(data, &receipt) != nil || receipt.CommandID == "" || receipt.DesiredVersion == "" || receipt.Status != "pending" {
			record.Classification = "admission_unresolved"
			record.Error = "receipt_invalid"
			r.Commands = append(r.Commands, record)
			r.Pending++
			incomplete = true
			break
		}
		record.CommandID = receipt.CommandID
		record.Classification = "accepted_202"
		record.AdmissionClassification = classify202Admission(keyPreviouslySent)
		if keyPreviouslySent {
			record.FreshnessQualification = "same_process_key_retry; replay_and_original_admission_are_indistinguishable"
		} else {
			record.FreshnessQualification = "unique_uuid_key_declared_by_manifest; server cannot prove absence of prior receipt"
		}
		r.Pending++
		pollDeadline := minTime(deadline, time.Now().Add(drainBudget))
		terminal := false
		pollFailure := ""
		for time.Now().Before(pollDeadline) {
			if !sleepUntilContext(context.Background(), time.Now().Add(500*time.Millisecond), pollDeadline) {
				break
			}
			pollCtx, pollCancel := context.WithDeadline(context.Background(), pollDeadline)
			viewResp, viewData, e := doControl(pollCtx, client, m.TLS.HTTPEndpoint, "GET", path+"/commands/"+url.PathEscape(receipt.CommandID), token, "", nil)
			pollCancel()
			if e != nil {
				if errors.Is(e, errControlResponseTooLarge) {
					pollFailure = "malformed_response"
					break
				}
				continue
			}
			if viewResp.StatusCode != 200 {
				pollFailure = classifyControlPollStatus(viewResp.StatusCode, viewData)
				if pollFailure != "" {
					break
				}
				continue
			}
			var view controlView
			if json.Unmarshal(viewData, &view) != nil || view.CommandID != receipt.CommandID {
				pollFailure = "malformed_response"
				break
			}
			status := view.Status
			if status != "pending" && status != "published" && status != "acknowledged" && status != "succeeded" && status != "failed" && status != "timeout" {
				pollFailure = "malformed_response"
				break
			}
			switch status {
			case "succeeded", "failed", "timeout":
				record.TerminalStatus = status
				record.Classification = status
				if status == "succeeded" {
					if view.Status != "succeeded" || view.Result == nil || view.Result.CommandID != receipt.CommandID || view.Result.DesiredVersion != receipt.DesiredVersion || view.Result.Status != "succeeded" {
						record.Classification = "succeeded_unverified"
						record.Error = "command_result_identity_mismatch"
						terminal = true
						break
					}
					stateCtx, stateCancel := context.WithDeadline(context.Background(), pollDeadline)
					stateResp, stateData, stateErr := doControl(stateCtx, client, m.TLS.HTTPEndpoint, "GET", path+"?format=simplified", token, "", nil)
					stateCancel()
					var state controlState
					var reported map[string]json.RawMessage
					var config map[string]json.RawMessage
					if stateErr != nil || stateResp.StatusCode != 200 || json.Unmarshal(stateData, &state) != nil || state.DesiredVersion != receipt.DesiredVersion || json.Unmarshal(state.ReportedState, &reported) != nil || json.Unmarshal(reported["configuration"], &config) != nil || string(config["sample_interval_us"]) == "" {
						record.Classification = "succeeded_unverified"
						record.Error = "reported_configuration_unavailable"
					} else {
						var interval int64
						wantInterval, _ := parseControlManifestRequest([]byte(fmt.Sprintf(`{"frequency_hz":%s,"expected_desired_version":%q}`, string(spec.FrequencyHz), spec.ExpectedDesiredVersion)), spec.IdempotencyKey)
						var reportedDesiredVersion string
						versionJSONErr := json.Unmarshal(config["desired_version"], &reportedDesiredVersion)
						if versionJSONErr != nil || reportedDesiredVersion != receipt.DesiredVersion || json.Unmarshal(config["sample_interval_us"], &interval) != nil || interval != wantInterval {
							record.Classification = "succeeded_unverified"
							record.Error = "reported_configuration_mismatch"
						}
					}
				}
				terminal = true
			}
			if terminal {
				break
			}
		}
		if terminal {
			r.Pending--
			emit("terminal_observed", index+1)
		}
		if pollFailure != "" {
			record.Classification = "poll_" + pollFailure
			record.Error = pollFailure
			incomplete = true
		}
		if !terminal {
			if pollFailure == "" {
				record.Classification = "timeout"
				record.Error = "terminal_result_deadline"
			}
		}
		r.Commands = append(r.Commands, record)
		if record.Classification == "timeout" || record.Classification == "succeeded_unverified" || pollFailure != "" {
			// Do not issue another command on a Sensor whose durable active slot
			// may still be occupied or whose result has not been verified.
			incomplete = true
			break
		}
	}
	emit("generation_stopped", 0)
	emit("producers_stopped", 0)
	emit("workers_joined", 0)
	for i := covered; i < len(m.Commands); i++ {
		spec := m.Commands[i]
		r.Commands = append(r.Commands, ControlRecord{GatewayID: spec.GatewayID, SensorID: spec.SensorID, ActorLabel: spec.ActorLabel, FrequencyHz: spec.FrequencyHz, ExpectedDesiredVersion: spec.ExpectedDesiredVersion, IdempotencyKey: spec.IdempotencyKey, Classification: "skipped_not_attempted", RoundTripMeasurement: "not measured"})
		r.Skipped++
	}
	if ctx.Err() != nil && r.Pending == 0 {
		r.Status = "STOPPED_DRAINED"
	} else if r.Pending > 0 || incomplete {
		r.Status = "INCOMPLETE"
	} else if r.Skipped > 0 {
		r.Status = "PARTIAL"
	} else {
		r.Status = "COMPLETE"
	}
	r.PhaseObservation = "observed; all command ordinals; process-local clock"
	if measurementFailed {
		r.PhaseObservation = "partial; phase observer failed; command outcomes retained"
		if r.Status == "COMPLETE" {
			r.Status = "PARTIAL"
		}
	}
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return finishControl(outputDir, r, nil)
}

func controlEntityPath(gateway, sensor string) string {
	urn := "urn:ngsi-ld:Sensor:" + gateway + ":" + sensor
	return "/v1/digital-twins/" + url.PathEscape(urn)
}

func doControl(ctx context.Context, client *http.Client, endpoint, method, path, token, key string, body []byte) (*http.Response, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(endpoint, "/")+path, strings.NewReader(string(body)))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if method == "PATCH" {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxControlResponse+1))
	if err != nil || len(data) > maxControlResponse {
		return resp, nil, errControlResponseTooLarge
	}
	return resp, data, nil
}

func classifyControlError(status int, data []byte) string {
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &envelope)
	switch envelope.Error.Code {
	case "command_in_progress":
		return "busy_command_in_progress"
	case "desired_version_conflict":
		return "stale_version"
	case "idempotency_conflict":
		return "replay_conflict"
	}
	if status == 408 || status == 504 {
		return "timeout"
	}
	if status == 409 {
		return "conflict"
	}
	if status >= 400 {
		return "rejected"
	}
	return "unknown"
}

func classifyControlHTTP(status int, data []byte) string {
	if status == 200 {
		return "noop"
	}
	if status == 202 {
		return "accepted_202_unqualified"
	}
	return classifyControlError(status, data)
}

func classifyControlPollStatus(status int, data []byte) string {
	switch status {
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusNotFound:
		return "command_not_found"
	case http.StatusTooManyRequests:
		return ""
	default:
		if status >= 500 {
			return ""
		}
		if status >= 400 {
			return classifyControlError(status, data)
		}
		return "malformed_response"
	}
}

func classify202Admission(keyPreviouslySent bool) string {
	if keyPreviouslySent {
		return "receipt_replay_or_original_response_after_retry"
	}
	return "accepted_202_fresh_key_declared_unverified"
}

func waitUntil(ctx context.Context, at time.Time) bool {
	d := time.Until(at)
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func sleepUntilContext(ctx context.Context, at, deadline time.Time) bool {
	if !time.Now().Before(deadline) {
		return false
	}
	if at.After(deadline) {
		at = deadline
	}
	return waitUntil(ctx, at)
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func finishControl(dir string, r ControlResult, err error) (ControlResult, error) {
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if e := writeControlResult(dir, r); e != nil && err == nil {
		err = errors.New("result_artifact_write_failed")
	}
	return r, err
}

func writeControlResult(dir string, r ControlResult) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "workload.json"), append(data, '\n'), 0600)
}
