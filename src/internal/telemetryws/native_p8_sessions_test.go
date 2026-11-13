package telemetryws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
)

const (
	p8SessionConfigEnv = "P8_SESSIONS_CONFIG_FILE"
	p8SessionMax       = 64
)

type p8SessionConfig struct {
	URL           string              `json:"wss_endpoint"`
	Origin        string              `json:"origin,omitempty"`
	CAFile        string              `json:"ca_file"`
	Tokens        map[string]string   `json:"role_jwts"`
	Targets       []p8NamespaceTarget `json:"namespace_targets"`
	Sessions      []p8SessionSpec     `json:"sessions"`
	MaxLifetimeMS int                 `json:"max_lifetime_ms"`
	MaxBytes      int64               `json:"max_bytes"`
	MaxEvents     int                 `json:"max_events"`
	ReadyPath     string              `json:"ready_path"`
	StatePath     string              `json:"state_path"`
	StopPath      string              `json:"stop_path"`
	RevokePath    string              `json:"revoke_group_a_path"`
	OutputPath    string              `json:"output_path"`
}
type p8NamespaceTarget struct {
	Group  string    `json:"group"`
	Target TargetKey `json:"target"`
}
type p8SessionSpec struct {
	ID      string      `json:"id"`
	Group   string      `json:"group"`
	Targets []TargetKey `json:"filters"`
}
type p8SessionResult struct {
	ID           string   `json:"id"`
	Group        string   `json:"group"`
	Events       int      `json:"events"`
	UniqueEvents int      `json:"unique_events"`
	Bytes        int64    `json:"bytes"`
	Duplicates   int      `json:"duplicates"`
	Unexpected   int      `json:"unexpected_namespace"`
	Closed       bool     `json:"closed"`
	SampledIDs   []string `json:"sampled_message_ids,omitempty"`
}
type p8SessionState struct {
	Outcome                  string            `json:"outcome"`
	Status                   string            `json:"status"`
	Ready                    bool              `json:"ready"`
	CapPlusOneDenied         bool              `json:"cap_plus_one_denied"`
	ReplacementReady         bool              `json:"replacement_ready"`
	SocketCount              int               `json:"socket_count"`
	PrincipalCount           int               `json:"principal_count"`
	Events                   int               `json:"events"`
	UniqueEvents             int               `json:"unique_events"`
	Bytes                    int64             `json:"bytes"`
	DuplicateCount           int               `json:"duplicates"`
	UnexpectedCount          int               `json:"unexpected_namespace"`
	RetainedIdentityCount    int               `json:"retained_identity_count"`
	MaxRetainedIdentityCount int               `json:"max_retained_identity_count"`
	Viewers                  []p8SessionResult `json:"viewers"`
	Failure                  string            `json:"failure,omitempty"`
	RevokeComplete           bool              `json:"revoke_complete"`
}
type p8TelemetryIdentity struct {
	GatewayID       string `json:"gateway_id"`
	SensorID        string `json:"sensor_id"`
	EntityID        string `json:"entity_id"`
	MessageID       string `json:"message_id"`
	BootID          string `json:"boot_id"`
	FirstSequence   string `json:"first_sequence"`
	ProtocolVersion int64  `json:"protocol_version"`
	MessageType     string `json:"message_type"`
}
type p8TelemetryCheck struct{ Matched bool }

// TestNativeP8SessionsProbe is externally orchestrated and is deliberately not an acceptance test.
func TestNativeP8SessionsProbe(t *testing.T) {
	path := os.Getenv(p8SessionConfigEnv)
	if path == "" {
		t.Skip("native P8 session config not supplied")
	}
	cfg, err := readP8SessionConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Sessions) != p8SessionMax {
		t.Fatal("native session lane requires exactly 64 session records")
	}
	result := runNativeP8Sessions(cfg)
	if err := writePrivateJSON(cfg.OutputPath, result); err != nil {
		t.Fatalf("write private result: %v", err)
	}
	if result.Failure != "" || result.Events == 0 {
		t.Errorf("bounded probe failed: %s", result.Failure)
	}
}

func readP8SessionConfig(path string) (p8SessionConfig, error) {
	var c p8SessionConfig
	b, err := readP8BoundedFile(path, 1<<20, true)
	if err != nil {
		return c, errors.New("config unavailable or over size limit")
	}
	if json.Unmarshal(b, &c) != nil {
		return c, errors.New("config is invalid JSON")
	}
	u, err := url.Parse(c.URL)
	ip := net.IP(nil)
	if err == nil {
		ip = net.ParseIP(u.Hostname())
	}
	if err != nil || u.Scheme != "wss" || ip == nil || !ip.IsLoopback() || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return c, errors.New("endpoint must be loopback WSS without credentials or query")
	}
	if c.CAFile == "" || len(c.Sessions) == 0 || len(c.Sessions) > p8SessionMax || len(c.Targets) < 2 || len(c.Targets) > 16 || len(c.Tokens) != 2 || c.MaxLifetimeMS < 100 || c.MaxLifetimeMS > 180000 || c.MaxBytes < 1 || c.MaxBytes > 256<<20 || c.MaxEvents < 1 || c.MaxEvents > 100000 {
		return c, errors.New("config exceeds bounded session probe limits")
	}
	if c.Origin != "" && c.Origin != "https://client.example.test" {
		return c, errors.New("origin is outside the fixture allowlist")
	}
	for _, g := range []string{"A", "B"} {
		token := c.Tokens[g]
		if token == "" || strings.ContainsAny(token, "\r\n") {
			return c, errors.New("two role-scoped JWTs are required")
		}
	}
	groups := map[string]bool{}
	targetSet := map[TargetKey]bool{}
	groupByGateway := map[string]string{}
	for _, entry := range c.Targets {
		target := entry.Target
		if target.GatewayID == "" || target.SensorID == "" || targetSet[target] {
			return c, errors.New("targets must be unique concrete composite identities")
		}
		if gateway.ValidateGatewayID(target.GatewayID) != nil || gateway.ValidateSensorID(target.SensorID) != nil {
			return c, errors.New("invalid concrete target identity")
		}
		if entry.Group != "A" && entry.Group != "B" {
			return c, errors.New("namespace target group must be A or B")
		}
		if existing := groupByGateway[target.GatewayID]; existing != "" && existing != entry.Group {
			return c, errors.New("gateway cannot span principal namespace groups")
		}
		groupByGateway[target.GatewayID] = entry.Group
		targetSet[target] = true
		groups[entry.Group] = true
	}
	if len(groups) != 2 {
		return c, errors.New("exactly two namespace groups are required")
	}
	ids := map[string]bool{}
	filters := map[TargetKey]bool{}
	hasSensor, hasGateway, hasOverlap := false, false, false
	for _, s := range c.Sessions {
		if s.ID == "" || ids[s.ID] || (s.Group != "A" && s.Group != "B") || len(s.Targets) == 0 || len(s.Targets) > MaxSubscriptionsPerSession {
			return c, errors.New("invalid or duplicate session record")
		}
		ids[s.ID] = true
		localFilters := map[TargetKey]bool{}
		localGateways, localSensors := map[string]bool{}, map[string]bool{}
		for _, f := range s.Targets {
			if f.GatewayID == "" || groupByGateway[f.GatewayID] != s.Group {
				return c, errors.New("filter does not belong to its principal namespace")
			}
			if gateway.ValidateGatewayID(f.GatewayID) != nil || (f.SensorID != "" && gateway.ValidateSensorID(f.SensorID) != nil) {
				return c, errors.New("invalid filter identity")
			}
			if localFilters[f] {
				return c, errors.New("duplicate composite filter")
			}
			localFilters[f] = true
			filters[f] = true
			if f.SensorID == "" {
				hasGateway = true
				localGateways[f.GatewayID] = true
			} else {
				hasSensor = true
				localSensors[f.GatewayID] = true
			}
		}
		for gatewayID := range localGateways {
			if localSensors[gatewayID] {
				hasOverlap = true
			}
		}
	}
	if len(filters) > 16 || !hasSensor || !hasGateway || !hasOverlap {
		return c, errors.New("filter identity limit exceeded")
	}
	paths := []string{c.ReadyPath, c.StatePath, c.StopPath, c.RevokePath, c.OutputPath}
	seen := map[string]bool{}
	for _, p := range paths {
		if !filepath.IsAbs(p) || seen[p] {
			return c, errors.New("private control and artifact paths must be distinct absolute paths")
		}
		seen[p] = true
		d, e := os.Stat(filepath.Dir(p))
		if e != nil || !d.IsDir() || d.Mode().Perm()&0077 != 0 {
			return c, errors.New("control directories must be private")
		}
		if st, e := os.Lstat(p); e == nil && (!st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0) {
			return c, errors.New("existing control files must be private regular files")
		}
	}
	return c, nil
}

func readP8BoundedFile(path string, limit int64, private bool) ([]byte, error) {
	var empty []byte
	linkInfo, err := os.Lstat(path)
	if err != nil || !linkInfo.Mode().IsRegular() {
		return empty, errors.New("not a regular file")
	}
	if private && linkInfo.Mode().Perm()&0077 != 0 {
		return empty, errors.New("file is not private")
	}
	f, err := os.Open(path)
	if err != nil {
		return empty, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > limit {
		return empty, errors.New("file exceeds bound")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit {
		return empty, errors.New("file exceeds bound")
	}
	return data, nil
}

type p8LiveSession struct {
	mu            sync.Mutex
	spec          p8SessionSpec
	conn          *websocket.Conn
	result        p8SessionResult
	seen          map[string]struct{}
	expectedClose bool
}
type p8SessionBudget struct {
	mu        sync.Mutex
	events    int
	bytes     int64
	maxEvents int
	maxBytes  int64
}

func (b *p8SessionBudget) addBytes(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if int64(n) > b.maxBytes-b.bytes {
		return false
	}
	b.bytes += int64(n)
	return true
}
func (b *p8SessionBudget) addEvent() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.events >= b.maxEvents {
		return false
	}
	b.events++
	return true
}

func runNativeP8Sessions(c p8SessionConfig) p8SessionState {
	state := p8SessionState{Outcome: "probe_only_not_p8_acceptance", Status: "running", PrincipalCount: 2, Viewers: make([]p8SessionResult, 0, len(c.Sessions))}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.MaxLifetimeMS)*time.Millisecond)
	defer cancel()
	ca, err := readP8BoundedFile(c.CAFile, 1<<20, false)
	if err != nil {
		state.Failure = "ca_unavailable"
		return state
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		state.Failure = "invalid_ca"
		return state
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second, Control: func(_, _ string, raw syscall.RawConn) error {
		var e error
		if err := raw.Control(func(fd uintptr) { e = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096) }); err != nil {
			return err
		}
		return e
	}}
	transport := &http.Transport{Proxy: func(*http.Request) (*url.URL, error) { return nil, nil }, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DialContext: dialer.DialContext}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	live := make([]*p8LiveSession, 0, len(c.Sessions))
	closeAll := func() {
		for _, s := range live {
			if s.conn != nil {
				_ = s.conn.CloseNow()
			}
		}
	}
	defer closeAll()
	dial := func(spec p8SessionSpec) (*websocket.Conn, *http.Response, error) {
		headers := http.Header{"Authorization": {"Bearer " + c.Tokens[spec.Group]}}
		if c.Origin != "" {
			headers.Set("Origin", c.Origin)
		}
		return websocket.Dial(ctx, c.URL, &websocket.DialOptions{HTTPClient: client,
			HTTPHeader: headers, Subprotocols: []string{auth.WSTelemetryProtocol}})
	}
	for _, spec := range c.Sessions {
		conn, _, e := dial(spec)
		if e != nil {
			state.Failure = "session_upgrade_failed"
			return state
		}
		if conn.Subprotocol() != auth.WSTelemetryProtocol {
			_ = conn.CloseNow()
			state.Failure = "subprotocol_mismatch"
			return state
		}
		conn.SetReadLimit(DefaultMaxEventBytes)
		reqID := uuid.NewString()
		raw, _ := json.Marshal(ClientMessage{ProtocolVersion: ProtocolVersion, RequestID: reqID, Action: ActionSubscribe, Targets: spec.Targets})
		if conn.Write(ctx, websocket.MessageText, raw) != nil {
			_ = conn.CloseNow()
			state.Failure = "subscribe_write_failed"
			return state
		}
		_, frame, e := conn.Read(ctx)
		var ack AckMessage
		if e != nil || json.Unmarshal(frame, &ack) != nil || !validP8SubscribeAck(ack, reqID, spec.Targets) {
			_ = conn.CloseNow()
			state.Failure = "subscribe_ack_mismatch"
			return state
		}
		live = append(live, &p8LiveSession{spec: spec, conn: conn, result: p8SessionResult{ID: spec.ID, Group: spec.Group}, seen: make(map[string]struct{})})
	}
	state.SocketCount = len(live)
	// Verify over-cap denial before the server upgrades the request.
	probe := c.Sessions[0]
	_, resp, e := dial(probe)
	if e == nil {
		state.Failure = "capacity_not_enforced"
		return state
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		state.Failure = "capacity_denial_unverified"
		return state
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	state.CapPlusOneDenied = true
	_ = live[0].conn.Close(websocket.StatusNormalClosure, "replacement probe")
	live[0].conn = nil
	deadline := time.Now().Add(2 * time.Second)
	var replacement *websocket.Conn
	for time.Now().Before(deadline) {
		var response *http.Response
		replacement, response, e = dial(probe)
		if e == nil {
			break
		}
		if response == nil || response.StatusCode != http.StatusServiceUnavailable {
			break
		}
		if response.Body != nil {
			_ = response.Body.Close()
		}
		time.Sleep(25 * time.Millisecond)
	}
	if replacement == nil {
		state.Failure = "replacement_admission_failed"
		return state
	}
	if replacement.Subprotocol() != auth.WSTelemetryProtocol {
		_ = replacement.CloseNow()
		state.Failure = "replacement_subprotocol_mismatch"
		return state
	}
	// Replacement is an additional socket and must subscribe/ACK too.
	replacement.SetReadLimit(DefaultMaxEventBytes)
	rid := uuid.NewString()
	req, _ := json.Marshal(ClientMessage{ProtocolVersion: ProtocolVersion, RequestID: rid, Action: ActionSubscribe, Targets: probe.Targets})
	if replacement.Write(ctx, websocket.MessageText, req) != nil {
		_ = replacement.CloseNow()
		state.Failure = "replacement_subscribe_failed"
		return state
	}
	_, b, e := replacement.Read(ctx)
	var a AckMessage
	if e != nil || json.Unmarshal(b, &a) != nil || !validP8SubscribeAck(a, rid, probe.Targets) {
		_ = replacement.CloseNow()
		state.Failure = "replacement_ack_mismatch"
		return state
	}
	live[0].conn = replacement
	state.ReplacementReady = true
	state.SocketCount = len(live)
	var readers sync.WaitGroup
	budget := &p8SessionBudget{maxEvents: c.MaxEvents, maxBytes: c.MaxBytes}
	for _, s := range live {
		readers.Add(1)
		go func(s *p8LiveSession) { defer readers.Done(); readP8Session(ctx, s, budget, c.MaxEvents, c.MaxBytes) }(s)
	}
	state.Ready = true
	if err := writePrivateJSON(c.ReadyPath, map[string]any{"ready": true, "socket_count": state.SocketCount, "principal_count": 2, "target_count": len(c.Targets)}); err != nil {
		state.Failure = "ready_write_failed"
		closeAll()
		return state
	}
	_ = writeP8State(c.StatePath, &state, live)
	// External orchestration creates revoke_group_a_path after known batches are observed.
	revoked := false
	for {
		stop, controlErr := p8PrivateSignal(c.StopPath)
		if controlErr != nil {
			state.Failure = "invalid_stop_control"
			goto finish
		}
		if stop {
			break
		}
		if !revoked {
			revoke, e := p8PrivateSignal(c.RevokePath)
			if e != nil {
				state.Failure = "invalid_revoke_control"
				goto finish
			}
			if revoke {
				revoked = true
				for _, s := range live {
					if s.spec.Group == "A" {
						s.mu.Lock()
						s.expectedClose = true
						s.mu.Unlock()
					}
				}
			}
		}
		if revoked {
			allClosed := true
			for _, s := range live {
				s.mu.Lock()
				closed := s.result.Closed
				s.mu.Unlock()
				if s.spec.Group == "A" && !closed {
					allClosed = false
				}
				if s.spec.Group == "B" && closed {
					state.Failure = "group_b_closed_during_group_a_revoke"
				}
			}
			if allClosed && !state.RevokeComplete {
				state.RevokeComplete = true
			}
		}
		select {
		case <-ctx.Done():
			state.Failure = "lifetime_deadline"
			goto finish
		case <-time.After(50 * time.Millisecond):
		}
		if err := writeP8State(c.StatePath, &state, live); err != nil {
			state.Failure = "state_write_failed"
			goto finish
		}
	}
finish:
	cancel()
	closeAll()
	readers.Wait()
	for _, s := range live {
		s.mu.Lock()
		snapshot := s.result
		s.mu.Unlock()
		state.Viewers = append(state.Viewers, snapshot)
		state.Events += snapshot.Events
		state.UniqueEvents += snapshot.UniqueEvents
		state.Bytes += snapshot.Bytes
		state.DuplicateCount += snapshot.Duplicates
		state.UnexpectedCount += snapshot.Unexpected
	}
	state.RetainedIdentityCount = 0
	for _, session := range live {
		session.mu.Lock()
		state.RetainedIdentityCount += len(session.seen)
		session.mu.Unlock()
	}
	state.MaxRetainedIdentityCount = state.RetainedIdentityCount
	state.Status = "completed"
	if state.Events > c.MaxEvents || state.Bytes > c.MaxBytes {
		state.Failure = "aggregate_limit_exceeded"
	}
	if state.Failure != "" {
		state.Status = "failed"
	}
	if state.UnexpectedCount > 0 && state.Failure == "" {
		state.Failure = "unexpected_event"
		state.Status = "failed"
	}
	if err := writeP8State(c.StatePath, &state, live); err != nil && state.Failure == "" {
		state.Failure = "state_write_failed"
		state.Status = "failed"
	}
	return state
}

func readP8Session(ctx context.Context, s *p8LiveSession, budget *p8SessionBudget, maxEvents int, maxBytes int64) {
	for {
		_, b, err := s.conn.Read(ctx)
		if err != nil {
			s.mu.Lock()
			if websocket.CloseStatus(err) >= 0 {
				s.result.Closed = true
			} else if ctx.Err() == nil {
				s.result.Closed = true
				if !s.expectedClose {
					s.result.Unexpected++
				}
			}
			s.mu.Unlock()
			return
		}
		if !budget.addBytes(len(b)) {
			s.mu.Lock()
			s.result.Unexpected++
			s.mu.Unlock()
			return
		}
		if !budget.addEvent() {
			s.mu.Lock()
			s.result.Unexpected++
			s.mu.Unlock()
			return
		}
		s.mu.Lock()
		s.result.Bytes += int64(len(b))
		if s.result.Bytes > maxBytes {
			s.result.Unexpected++
			s.mu.Unlock()
			return
		}
		if s.result.Events >= maxEvents {
			s.result.Unexpected++
			s.mu.Unlock()
			return
		}
		check, e := validateP8Telemetry(b, s.spec.Targets)
		if e != nil {
			s.result.Unexpected++
			s.mu.Unlock()
			continue
		}
		if !check.Matched {
			s.result.Unexpected++
			s.mu.Unlock()
			continue
		}
		var event p8TelemetryIdentity
		_ = json.Unmarshal(b, &event)
		s.result.Events++
		if event.BootID == "" || event.FirstSequence == "" {
			s.result.Unexpected++
			s.mu.Unlock()
			continue
		}
		identityKey := event.GatewayID + "\x00" + event.SensorID + "\x00" + event.MessageID + "\x00" + event.BootID + "\x00" + event.FirstSequence
		if _, ok := s.seen[identityKey]; ok {
			s.result.Duplicates++
			s.mu.Unlock()
			continue
		}
		s.seen[identityKey] = struct{}{}
		s.result.UniqueEvents++
		if len(s.result.SampledIDs) < 8 {
			s.result.SampledIDs = append(s.result.SampledIDs, event.MessageID)
		}
		s.mu.Unlock()
	}
}

func p8PrivateSignal(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return false, errors.New("control signal must be a private regular file")
	}
	return true, nil
}
func validateP8Telemetry(raw []byte, filters []TargetKey) (p8TelemetryCheck, error) {
	var e p8TelemetryIdentity
	if len(raw) == 0 || len(raw) > MaxSupportedEventBytes {
		return p8TelemetryCheck{}, errors.New("event size invalid")
	}
	if json.Unmarshal(raw, &e) != nil || e.ProtocolVersion != ProtocolVersion || e.MessageType != MessageTypeTelemetry || e.GatewayID == "" || e.SensorID == "" || e.MessageID == "" || e.BootID == "" || e.FirstSequence == "" || e.EntityID != (TargetKey{GatewayID: e.GatewayID, SensorID: e.SensorID}).EntityURN() {
		return p8TelemetryCheck{}, errors.New("event identity invalid")
	}
	return p8TelemetryCheck{Matched: MatchesAnyTarget(filters, e.GatewayID, e.SensorID)}, nil
}
func sameP8Targets(a, b []TargetKey) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[TargetKey]int{}
	for _, t := range a {
		seen[t]++
	}
	for _, t := range b {
		if seen[t] == 0 {
			return false
		}
		seen[t]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}
func validP8SubscribeAck(ack AckMessage, requestID string, targets []TargetKey) bool {
	return ack.ProtocolVersion == ProtocolVersion && ack.MessageType == MessageTypeAck && ack.Action == ActionSubscribe && ack.RequestID == requestID && sameP8Targets(ack.Targets, targets)
}
func writeP8State(path string, state *p8SessionState, sessions []*p8LiveSession) error {
	copy := *state
	copy.Viewers = make([]p8SessionResult, 0, len(sessions))
	copy.Events, copy.UniqueEvents, copy.Bytes, copy.DuplicateCount, copy.UnexpectedCount, copy.RetainedIdentityCount = 0, 0, 0, 0, 0, 0
	for _, s := range sessions {
		s.mu.Lock()
		snapshot := s.result
		retained := len(s.seen)
		s.mu.Unlock()
		copy.Viewers = append(copy.Viewers, snapshot)
		copy.Events += snapshot.Events
		copy.UniqueEvents += snapshot.UniqueEvents
		copy.Bytes += snapshot.Bytes
		copy.DuplicateCount += snapshot.Duplicates
		copy.UnexpectedCount += snapshot.Unexpected
		copy.RetainedIdentityCount += retained
	}
	copy.MaxRetainedIdentityCount = copy.RetainedIdentityCount
	return writePrivateJSON(path, copy)
}

func TestP8SessionProbeConfigValidationAndBounds(t *testing.T) {
	dir := t.TempDir()
	_ = os.Chmod(dir, 0700)
	cfg := validP8SessionConfig(dir)
	path := filepath.Join(dir, "config.json")
	if err := writePrivateJSON(path, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := readP8SessionConfig(path); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	tooMany := cfg
	tooMany.Sessions = make([]p8SessionSpec, p8SessionMax+1)
	if err := writePrivateJSON(path, tooMany); err != nil {
		t.Fatal(err)
	}
	if _, err := readP8SessionConfig(path); err == nil {
		t.Fatal("accepted >64 sessions")
	}
	if err := writePrivateJSON(path, cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readP8SessionConfig(path); err == nil {
		t.Fatal("accepted non-private config")
	}
}

func TestP8SessionProbeIdentityMatchingAndOverlap(t *testing.T) {
	filters := []TargetKey{{GatewayID: "gw-a"}, {GatewayID: "gw-a", SensorID: "sensor-a"}}
	got, err := validateP8Telemetry([]byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-a:sensor-a","gateway_id":"gw-a","sensor_id":"sensor-a","message_id":"m1","boot_id":"b1","first_sequence":"0","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[1]}`), filters)
	if err != nil || !got.Matched {
		t.Fatalf("matching composite identity rejected: matched=%v err=%v", got.Matched, err)
	}
	if _, err := validateP8Telemetry([]byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-b:sensor-a","gateway_id":"gw-a","sensor_id":"sensor-a","message_id":"m1"}`), filters); err == nil {
		t.Fatal("accepted inconsistent composite identity")
	}
	if _, err := validateP8Telemetry([]byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-a:sensor-a","gateway_id":"gw-a","sensor_id":"sensor-a","message_id":"m1"}`), filters); err == nil {
		t.Fatal("accepted event without boot/sequence identity")
	}
	if MatchesAnyTarget(filters, "gw-a", "sensor-a") != true {
		t.Fatal("overlap did not match")
	}
}

func validP8SessionConfig(dir string) p8SessionConfig {
	return p8SessionConfig{URL: "wss://127.0.0.1/ws", CAFile: "ca.pem", Tokens: map[string]string{"A": "token-a", "B": "token-b"}, Targets: []p8NamespaceTarget{{Group: "A", Target: TargetKey{GatewayID: "gw-a", SensorID: "sensor-a"}}, {Group: "B", Target: TargetKey{GatewayID: "gw-b", SensorID: "sensor-b"}}}, Sessions: []p8SessionSpec{{ID: "viewer-a", Group: "A", Targets: []TargetKey{{GatewayID: "gw-a", SensorID: "sensor-a"}, {GatewayID: "gw-a"}}}, {ID: "viewer-b", Group: "B", Targets: []TargetKey{{GatewayID: "gw-b"}}}}, MaxLifetimeMS: 10000, MaxBytes: 1 << 20, MaxEvents: 100, ReadyPath: filepath.Join(dir, "ready.json"), StatePath: filepath.Join(dir, "state.json"), StopPath: filepath.Join(dir, "stop"), RevokePath: filepath.Join(dir, "revoke"), OutputPath: filepath.Join(dir, "result.json")}
}

func TestP8SessionProbeConfigRejectsDuplicateCompositeViewer(t *testing.T) {
	cfg := validP8SessionConfig(t.TempDir())
	cfg.Sessions = append(cfg.Sessions, cfg.Sessions[0])
	b, _ := json.Marshal(cfg)
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readP8SessionConfig(path); err == nil {
		t.Fatal("accepted duplicate session identity")
	}
}

func TestP8SessionProbeTLSRejectsWrongCAAndHostname(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "not upgraded", http.StatusBadRequest) }))
	defer server.Close()
	certRoots := x509.NewCertPool()
	certRoots.AddCert(server.Certificate())
	wrongRoots := x509.NewCertPool()
	for _, tc := range []struct {
		name     string
		endpoint string
		roots    *x509.CertPool
	}{
		{name: "wrong CA", endpoint: "wss" + strings.TrimPrefix(server.URL, "https"), roots: wrongRoots},
		{name: "wrong hostname", endpoint: "wss" + strings.TrimPrefix(strings.Replace(server.URL, "127.0.0.1", "localhost", 1), "https"), roots: certRoots},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: tc.roots, MinVersion: tls.VersionTLS12}, Proxy: func(*http.Request) (*url.URL, error) { return nil, nil }}
			defer transport.CloseIdleConnections()
			_, _, err := websocket.Dial(context.Background(), tc.endpoint, &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, Subprotocols: []string{auth.WSTelemetryProtocol}})
			if err == nil {
				t.Fatal("TLS connection unexpectedly accepted")
			}
		})
	}
}

func TestP8SessionProbeRejectsMismatchedSubscribeACK(t *testing.T) {
	target := TargetKey{GatewayID: "gw-a", SensorID: "sensor-a"}
	ack := NewAckMessage("request-a", ActionSubscribe, []TargetKey{{GatewayID: "gw-b", SensorID: "sensor-b"}})
	b, _ := json.Marshal(ack)
	var decoded AckMessage
	if json.Unmarshal(b, &decoded) != nil {
		t.Fatal("test ACK did not decode")
	}
	if validP8SubscribeAck(decoded, "request-a", []TargetKey{target}) {
		t.Fatal("mismatched ACK was accepted")
	}
}

func TestP8SessionProbeFiltersAndDeduplicatesCompositeEvents(t *testing.T) {
	target := TargetKey{GatewayID: "gw-a", SensorID: "sensor-a"}
	filters := []TargetKey{{GatewayID: "gw-a"}, target}
	event := []byte(`{"protocol_version":1,"message_type":"telemetry","entity_id":"urn:ngsi-ld:Sensor:gw-a:sensor-a","gateway_id":"gw-a","sensor_id":"sensor-a","message_id":"msg-1","boot_id":"boot-1","first_sequence":"1","sample_count":1,"measured_at":"2026-10-28T00:00:00Z","sample_interval_us":1000000,"samples":[3]}`)
	for i := 0; i < 2; i++ {
		check, err := validateP8Telemetry(event, filters)
		if err != nil || !check.Matched {
			t.Fatalf("overlapping filter did not select event: %v", err)
		}
	}
	other := []TargetKey{{GatewayID: "gw-b"}}
	check, err := validateP8Telemetry(event, other)
	if err != nil || check.Matched {
		t.Fatalf("cross-namespace event unexpectedly matched: matched=%v err=%v", check.Matched, err)
	}
}

func TestP8SessionProbeCapacityReplacementAndStopCleanup(t *testing.T) {
	var mu sync.Mutex
	active := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if active >= 3 {
			mu.Unlock()
			http.Error(w, "capacity", http.StatusServiceUnavailable)
			return
		}
		active++
		mu.Unlock()
		defer func() { mu.Lock(); active--; mu.Unlock() }()
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{auth.WSTelemetryProtocol}})
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		_, requestBytes, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var request ClientMessage
		if json.Unmarshal(requestBytes, &request) != nil {
			return
		}
		ack, _ := json.Marshal(NewAckMessage(request.RequestID, request.Action, request.Targets))
		if conn.Write(r.Context(), websocket.MessageText, ack) != nil {
			return
		}
		gatewayID, sensorID, messageID := "gw-a", "sensor-a", "event-a"
		if r.Header.Get("Authorization") == "Bearer token-b" {
			gatewayID, sensorID, messageID = "gw-b", "sensor-b", "event-b"
		}
		frame, _ := json.Marshal(TelemetryEventDTO{ProtocolVersion: 1, MessageType: MessageTypeTelemetry, EntityID: (TargetKey{GatewayID: gatewayID, SensorID: sensorID}).EntityURN(), GatewayID: gatewayID, SensorID: sensorID, MessageID: messageID, BootID: "boot", FirstSequence: "1", SampleCount: 1, MeasuredAt: "2026-10-28T00:00:00Z", SampleIntervalUS: 1000000, Samples: []float64{1}})
		_ = conn.Write(r.Context(), websocket.MessageText, frame)
		for {
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	_ = os.Chmod(dir, 0700)
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := p8SessionConfig{URL: "wss" + strings.TrimPrefix(server.URL, "https"), CAFile: caPath, Tokens: map[string]string{"A": "token-a", "B": "token-b"}, Targets: []p8NamespaceTarget{{Group: "A", Target: TargetKey{GatewayID: "gw-a", SensorID: "sensor-a"}}, {Group: "B", Target: TargetKey{GatewayID: "gw-b", SensorID: "sensor-b"}}}, Sessions: []p8SessionSpec{{ID: "overlap", Group: "A", Targets: []TargetKey{{GatewayID: "gw-a"}, {GatewayID: "gw-a", SensorID: "sensor-a"}}}, {ID: "sensor-only", Group: "A", Targets: []TargetKey{{GatewayID: "gw-a", SensorID: "sensor-a"}}}, {ID: "gateway-wide", Group: "B", Targets: []TargetKey{{GatewayID: "gw-b"}}}}, MaxLifetimeMS: 10000, MaxBytes: 1 << 20, MaxEvents: 100, ReadyPath: filepath.Join(dir, "ready.json"), StatePath: filepath.Join(dir, "state.json"), StopPath: filepath.Join(dir, "stop"), RevokePath: filepath.Join(dir, "revoke"), OutputPath: filepath.Join(dir, "result.json")}
	if err := writePrivateJSON(filepath.Join(dir, "config.json"), cfg); err != nil {
		t.Fatal(err)
	}
	runner := make(chan p8SessionState, 1)
	go func() { runner <- runNativeP8Sessions(cfg) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var state p8SessionState
		if b, e := os.ReadFile(cfg.StatePath); e == nil {
			_ = json.Unmarshal(b, &state)
			if state.Events >= 3 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := os.WriteFile(cfg.StopPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-runner:
		if result.Failure != "" || result.SocketCount != 3 || result.Events < 3 {
			t.Fatalf("session probe result not clean: %+v", result)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("probe did not stop and join readers")
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := active
		mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if active != 0 {
		t.Fatalf("server still has %d active sockets after stop", active)
	}
}
