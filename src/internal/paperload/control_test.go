package paperload

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testIdempotencyKey = "b686bd6e-c70a-4dda-b90a-384b0d82f046"

func controlTestManifest(t *testing.T, server *httptest.Server) (Manifest, string, string) {
	t.Helper()
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	cert, _ := x509.ParseCertificate(server.Certificate().Raw)
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(dir, "role_jwts.json")
	if err := os.WriteFile(identityPath, []byte(`{"operator-a":"opaque-test-token"}`), 0600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	m := Manifest{SchemaVersion: 1, Experiment: "E3", Arm: "rest-control", Profile: "pilot", Mode: "local", RunID: "run-control", Roles: []string{"control"}, Topology: Topology{}, Phases: Phases{DrainSeconds: 2}, Limits: Limits{WallSeconds: 8, Workers: 1, Connections: 1}, TLS: TLS{HTTPEndpoint: server.URL, HTTPCAFile: caPath}, IdentityFiles: map[string]string{"control": identityPath}, Commands: []ControlCommand{{GatewayID: "gw-a", SensorID: "s-a", FrequencyHz: json.Number("1.5"), OffsetMS: 0, ActorLabel: "operator-a", IdempotencyKey: testIdempotencyKey, ExpectedDesiredVersion: "9007199254740993"}}}
	if err := os.WriteFile(manifestPath, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	return m, manifestPath, filepath.Join(dir, "out")
}

func TestRunControlPATCH202PollsRealResultAndReportedConfiguration(t *testing.T) {
	var polls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer opaque-test-token" {
			http.Error(w, "auth", http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == "PATCH" && strings.HasSuffix(r.URL.Path, "/desired-state"):
			if r.Header.Get("Idempotency-Key") != testIdempotencyKey {
				http.Error(w, "key", http.StatusBadRequest)
				return
			}
			var body struct {
				Frequency json.Number `json:"frequency_hz"`
				Version   string      `json:"expected_desired_version"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || string(body.Frequency) != "1.5" || body.Version != "9007199254740993" {
				http.Error(w, "body", http.StatusBadRequest)
				return
			}
			w.WriteHeader(202)
			_, _ = w.Write([]byte(`{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","status":"pending","desired_version":"9007199254740994","sample_interval_us":666667,"status_url":"ignored"}`))
		case r.Method == "GET" && strings.Contains(r.URL.Path, "/commands/"):
			polls.Add(1)
			_, _ = w.Write([]byte(`{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","issued_at":"2000-01-01T00:00:00Z","status":"succeeded","desired_version":"9007199254740994","result":{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","desired_version":"9007199254740994","status":"succeeded","reported_state":{"sample_interval_us":666667}}}`))
		case r.Method == "GET" && r.URL.RawQuery == "format=simplified":
			_, _ = w.Write([]byte(`{"desired_version":"9007199254740994","reported_version":"9","reported_state":{"configuration":{"sample_interval_us":666667,"desired_version":"9007199254740994"}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	m, manifest, out := controlTestManifest(t, server)
	if _, err := Validate(m, false); err != nil {
		t.Fatalf("control-only manifest rejected: %v", err)
	}
	low := m
	low.Commands = append([]ControlCommand(nil), m.Commands...)
	low.Commands[0].FrequencyHz = json.Number("0.1")
	low.Commands[0].IdempotencyKey = "19dabc9f-5790-43af-aeac-df1a1e1913a4"
	if _, err := Validate(low, false); err != nil {
		t.Fatalf("sub-1Hz decimal rejected: %v", err)
	}
	var observed []ControlPhaseEvent
	got, err := RunControl(context.Background(), m, manifest, out, func(event ControlPhaseEvent) error {
		observed = append(observed, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "COMPLETE" || len(got.Commands) != 1 || got.Commands[0].Classification != "succeeded" || got.Commands[0].AdmissionClassification != "accepted_202_fresh_key_declared_unverified" || got.Commands[0].RoundTripMeasurement == "" || got.OriginMissing == "" || polls.Load() == 0 {
		t.Fatalf("result: %+v polls=%d", got, polls.Load())
	}
	if len(observed) < 6 || observed[0].Event != "request_begin" || observed[1].Event != "admission_observed" || observed[len(observed)-1].Event != "workers_joined" {
		t.Fatalf("phase ordering: %+v", observed)
	}
	terminal := false
	for _, event := range observed {
		if event.Event == "terminal_observed" {
			terminal = true
		}
	}
	if !terminal {
		t.Fatalf("terminal event missing: %+v", observed)
	}
	data, err := os.ReadFile(filepath.Join(out, "workload.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "opaque-test-token") || strings.Contains(string(data), "reported_state") {
		t.Fatalf("unsafe artifact: %s", data)
	}
}

func TestControlClassificationSeparatesNoopReplayBusyStaleAndRejection(t *testing.T) {
	if classifyControlHTTP(200, []byte(`{}`)) != "noop" || classifyControlHTTP(202, []byte(`{}`)) != "accepted_202_unqualified" {
		t.Fatal("successful HTTP outcomes misclassified")
	}
	if classify202Admission(false) != "accepted_202_fresh_key_declared_unverified" || classify202Admission(true) != "receipt_replay_or_original_response_after_retry" {
		t.Fatal("202 request-history classification incorrect")
	}
	if classifyControlPollStatus(http.StatusUnauthorized, nil) != "unauthorized" || classifyControlPollStatus(http.StatusForbidden, nil) != "forbidden" || classifyControlPollStatus(http.StatusNotFound, nil) != "command_not_found" || classifyControlPollStatus(http.StatusBadGateway, nil) != "" {
		t.Fatal("poll HTTP status classification incorrect")
	}
	cases := []struct {
		status     int
		body, want string
	}{{409, `{"error":{"code":"command_in_progress"}}`, "busy_command_in_progress"}, {409, `{"error":{"code":"desired_version_conflict"}}`, "stale_version"}, {409, `{"error":{"code":"idempotency_conflict"}}`, "replay_conflict"}, {400, `{"error":{"code":"invalid_input"}}`, "rejected"}, {504, `{}`, "timeout"}}
	for _, tc := range cases {
		got := classifyControlError(tc.status, []byte(tc.body))
		if got != tc.want {
			t.Errorf("%d %s => %s want %s", tc.status, tc.body, got, tc.want)
		}
	}
}

func TestRunControlPhaseEventsCoverNoopAndRejectedAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   string
	}{
		{"noop", http.StatusOK, "noop"}, {"rejected", http.StatusBadRequest, "rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPatch {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(`{}`))
					return
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			m, manifest, out := controlTestManifest(t, server)
			var events []ControlPhaseEvent
			result, err := RunControl(context.Background(), m, manifest, out, func(event ControlPhaseEvent) error {
				events = append(events, event)
				return fmt.Errorf("measurement sink unavailable")
			})
			if err != nil || result.Status != "PARTIAL" || result.Commands[0].Classification != tc.want {
				t.Fatalf("outcome lost: result=%+v err=%v", result, err)
			}
			if len(events) < 5 || events[0].Event != "request_begin" || events[1].Event != "admission_observed" || events[len(events)-1].Event != "workers_joined" {
				t.Fatalf("events: %+v", events)
			}
			for _, event := range events {
				if event.Event == "terminal_observed" {
					t.Fatalf("admission-only outcome got terminal event: %+v", events)
				}
			}
		})
	}
}

func TestRunControlPhaseEventJournalIsBoundedAndSamplesLongStreams(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	m, manifest, out := controlTestManifest(t, server)
	for i := 1; i < 12; i++ {
		command := m.Commands[0]
		command.IdempotencyKey = fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		m.Commands = append(m.Commands, command)
	}
	result, err := RunControl(context.Background(), m, manifest, out)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.PhaseEvents) > maxControlPhaseEvents || result.PhaseObservation == "" {
		t.Fatalf("unbounded/unqualified journal: events=%d qualification=%q", len(result.PhaseEvents), result.PhaseObservation)
	}
	for _, event := range result.PhaseEvents {
		if event.CommandOrdinal > 9 {
			t.Fatalf("unsampled ordinal: %+v", event)
		}
	}
}

func TestRunControlRejectsWrongReportedConfigurationDesiredVersion(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch:
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","status":"pending","desired_version":"9007199254740994"}`))
		case strings.Contains(r.URL.Path, "/commands/"):
			_, _ = w.Write([]byte(`{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","status":"succeeded","result":{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","desired_version":"9007199254740994","status":"succeeded"}}`))
		case r.URL.RawQuery == "format=simplified":
			_, _ = w.Write([]byte(`{"desired_version":"9007199254740994","reported_version":"9","reported_state":{"configuration":{"sample_interval_us":666667,"desired_version":"1"}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	m, manifest, out := controlTestManifest(t, server)
	got, err := RunControl(context.Background(), m, manifest, out)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "INCOMPLETE" || len(got.Commands) != 1 || got.Commands[0].Classification != "succeeded_unverified" || got.Commands[0].Error != "reported_configuration_mismatch" {
		t.Fatalf("wrong reported configuration accepted: %+v", got)
	}
}

func TestRunControlDeadlineAccountsSkippedCommands(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	defer server.Close()
	m, manifest, out := controlTestManifest(t, server)
	m.Limits.WallSeconds = 1
	m.Phases.DrainSeconds = 1
	got, err := RunControl(context.Background(), m, manifest, out)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "PARTIAL" || got.Planned != 1 || got.Sent != 0 || got.Skipped != 1 || got.Pending != 0 || len(got.Commands) != 1 || got.Commands[0].Classification != "skipped_not_attempted" {
		t.Fatalf("deadline accounting: %+v", got)
	}
}

func TestRunControlPollErrorsAreClassifiedWithoutTimeoutRetry(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{{"unauthorized", http.StatusUnauthorized, `{}`, "poll_unauthorized"}, {"forbidden", http.StatusForbidden, `{}`, "poll_forbidden"}, {"missing", http.StatusNotFound, `{}`, "poll_command_not_found"}, {"malformed", http.StatusOK, `{`, "poll_malformed_response"}, {"oversized", http.StatusOK, strings.Repeat("x", maxControlResponse+1), "poll_malformed_response"}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPatch {
					w.WriteHeader(http.StatusAccepted)
					_, _ = w.Write([]byte(`{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","status":"pending","desired_version":"9007199254740994"}`))
					return
				}
				if strings.Contains(r.URL.Path, "/commands/") {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			m, manifest, out := controlTestManifest(t, server)
			got, err := RunControl(context.Background(), m, manifest, out)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Commands) != 1 || got.Commands[0].Classification != tc.want || got.Pending != 1 || got.Commands[0].Classification == "timeout" {
				t.Fatalf("poll error handling: %+v", got)
			}
		})
	}
}

func TestControlResponseLimitAndTLSAuthenticationFailClosed(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer opaque-test-token" {
			http.Error(w, `{"error":{"code":"unauthorized"}}`, http.StatusUnauthorized)
			return
		}
		_, _ = fmt.Fprint(w, strings.Repeat("x", maxControlResponse+1))
	}))
	defer server.Close()
	m, manifest, out := controlTestManifest(t, server)
	// CA is trusted, but the oversized PATCH response is rejected and recorded safely.
	got, err := RunControl(context.Background(), m, manifest, out)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Commands) != 1 || got.Commands[0].Error != "https_request_failed" {
		t.Fatalf("oversized body: %+v", got)
	}
	transport := &http.Transport{TLSClientConfig: nil}
	client := &http.Client{Transport: transport, Timeout: time.Second}
	defer transport.CloseIdleConnections()
	resp, _, err := doControl(context.Background(), client, server.URL, "GET", "/", "token", "", nil)
	if err == nil || resp != nil {
		t.Fatalf("untrusted CA accepted: resp=%v err=%v", resp, err)
	}
	cert, _ := x509.ParseCertificate(server.Certificate().Raw)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	trustedTransport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: "wrong.example"}}
	trustedClient := &http.Client{Transport: trustedTransport, Timeout: time.Second}
	defer trustedTransport.CloseIdleConnections()
	if _, _, err = doControl(context.Background(), trustedClient, server.URL, "GET", "/", "opaque-test-token", "", nil); err == nil {
		t.Fatal("hostname mismatch accepted")
	}
	u, _ := url.Parse(server.URL)
	trustedTransport.TLSClientConfig.ServerName = u.Hostname()
	resp, data, err := doControl(context.Background(), trustedClient, server.URL, "GET", "/", "bad-token", "", nil)
	if err != nil || resp.StatusCode != http.StatusUnauthorized || classifyControlError(resp.StatusCode, data) != "rejected" {
		t.Fatalf("auth rejection classification: resp=%v err=%v", resp, err)
	}
}

func TestControlStopStopsFutureCommandsAndDrainsAcceptedOne(t *testing.T) {
	var cancel context.CancelFunc
	var polls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "PATCH":
			w.WriteHeader(202)
			_, _ = w.Write([]byte(`{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","status":"pending","desired_version":"9007199254740994"}`))
		case strings.Contains(r.URL.Path, "/commands/"):
			polls.Add(1)
			cancel()
			_, _ = w.Write([]byte(`{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","status":"succeeded","result":{"command_id":"a504e8a0-53ba-4f47-8d67-163d2e00489d","desired_version":"9007199254740994","status":"succeeded"}}`))
		case r.URL.RawQuery == "format=simplified":
			_, _ = w.Write([]byte(`{"desired_version":"9007199254740994","reported_version":"9007199254740994","reported_state":{"configuration":{"sample_interval_us":666667}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	m, manifest, out := controlTestManifest(t, server)
	m.Phases.DrainSeconds = 2
	m.Commands = append(m.Commands, m.Commands[0])
	m.Commands[1].SensorID = "s-b"
	m.Commands[1].OffsetMS = 1000
	m.Commands[1].IdempotencyKey = "19dabc9f-5790-43af-aeac-df1a1e1913a4"
	ctx, stop := context.WithCancel(context.Background())
	cancel = stop
	got, err := RunControl(ctx, m, manifest, out)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Commands) != 2 || polls.Load() == 0 || got.Status != "STOPPED_DRAINED" || got.Planned != 2 || got.Sent != 1 || got.Skipped != 1 || got.Pending != 0 || got.Commands[1].Classification != "skipped_not_attempted" {
		t.Fatalf("stop/drain result %+v polls=%d", got, polls.Load())
	}
}

func TestControlCAAndIdentityFileErrorsAreSafe(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	defer server.Close()
	m, manifest, out := controlTestManifest(t, server)
	if err := os.WriteFile(m.TLS.HTTPCAFile, []byte("not a CA"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := RunControl(context.Background(), m, manifest, out)
	if err == nil || got.PrimaryError != "https_ca_rejected" {
		t.Fatalf("CA failure: %+v %v", got, err)
	}
}
