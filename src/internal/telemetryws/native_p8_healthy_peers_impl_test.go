package telemetryws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
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

const p8HealthyPeersConfigEnv = "P8_HEALTHY_PEERS_CONFIG_FILE"

type healthyPeersConfig struct {
	URL           string            `json:"wss_endpoint"`
	CAFile        string            `json:"ca_file"`
	Tokens        map[string]string `json:"role_jwts"`
	Peers         []healthyPeerSpec `json:"peers"`
	MaxLifetimeMS int               `json:"max_lifetime_ms"`
	MaxBytes      int64             `json:"max_bytes"`
	MaxEvents     int               `json:"max_events"`
	ReadyPath     string            `json:"ready_path"`
	StatePath     string            `json:"state_path"`
	StopPath      string            `json:"stop_path"`
	OutputPath    string            `json:"output_path"`
}

type healthyPeerSpec struct {
	ID     string    `json:"id"`
	Role   string    `json:"role"`
	Target TargetKey `json:"target"`
}

type healthyPeerResult struct {
	ID           string    `json:"id"`
	Role         string    `json:"role"`
	Target       TargetKey `json:"target"`
	Events       int       `json:"events"`
	UniqueEvents int       `json:"unique_events"`
	Bytes        int64     `json:"bytes"`
	Duplicates   int       `json:"duplicates"`
	Unexpected   int       `json:"unexpected_namespace"`
	Closed       bool      `json:"closed"`
	SampledIDs   []string  `json:"sampled_message_ids,omitempty"`
}

type healthyPeersState struct {
	Outcome      string              `json:"outcome"`
	Status       string              `json:"status"`
	Ready        bool                `json:"ready"`
	SocketCount  int                 `json:"socket_count"`
	Events       int                 `json:"events"`
	UniqueEvents int                 `json:"unique_events"`
	Bytes        int64               `json:"bytes"`
	Duplicates   int                 `json:"duplicates"`
	Unexpected   int                 `json:"unexpected_namespace"`
	Peers        []healthyPeerResult `json:"peers"`
	Failure      string              `json:"failure,omitempty"`
}

type healthyPeerLive struct {
	mu   sync.Mutex
	spec healthyPeerSpec
	conn *websocket.Conn
	out  healthyPeerResult
	seen map[string]struct{}
}

// TestNativeP8HealthyPeersProbe is an externally orchestrated, probe-only lane.
func TestNativeP8HealthyPeersProbe(t *testing.T) {
	path := os.Getenv(p8HealthyPeersConfigEnv)
	if path == "" {
		t.Skip("native P8 healthy-peer config not supplied")
	}
	cfg, err := readP8HealthyPeersConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	result := runNativeP8HealthyPeers(cfg)
	if err := writePrivateJSON(cfg.OutputPath, result); err != nil {
		t.Fatalf("write private healthy-peer result: %v", err)
	}
	if result.Failure != "" || result.Events == 0 {
		t.Errorf("healthy-peer probe failed: %s", result.Failure)
	}
}

func readP8HealthyPeersConfig(path string) (healthyPeersConfig, error) {
	var c healthyPeersConfig
	b, err := readP8BoundedFile(path, 1<<20, true)
	if err != nil || json.Unmarshal(b, &c) != nil {
		return c, errors.New("config unavailable or invalid JSON")
	}
	u, err := url.Parse(c.URL)
	var ip net.IP
	if err == nil {
		ip = net.ParseIP(u.Hostname())
	}
	if err != nil || u.Scheme != "wss" || ip == nil || !ip.IsLoopback() || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return c, errors.New("endpoint must be loopback WSS without credentials or query")
	}
	if c.CAFile == "" || len(c.Peers) != 2 || len(c.Tokens) != 2 || c.MaxLifetimeMS < 100 || c.MaxLifetimeMS > 180000 || c.MaxBytes < 1 || c.MaxBytes > 256<<20 || c.MaxEvents < 1 || c.MaxEvents > 100000 {
		return c, errors.New("config must define two peers and finite bounded lifetime, bytes and events")
	}
	for _, role := range []string{"A", "B"} {
		token := c.Tokens[role]
		if token == "" || strings.ContainsAny(token, "\r\n") {
			return c, errors.New("exactly two role JWTs are required")
		}
	}
	ids, targets, roles := map[string]bool{}, map[TargetKey]bool{}, map[string]bool{}
	for _, peer := range c.Peers {
		if peer.ID == "" || ids[peer.ID] || (peer.Role != "A" && peer.Role != "B") || peer.Target.GatewayID == "" || peer.Target.SensorID == "" || targets[peer.Target] || gateway.ValidateGatewayID(peer.Target.GatewayID) != nil || gateway.ValidateSensorID(peer.Target.SensorID) != nil {
			return c, errors.New("peers require distinct IDs and concrete unique Gateway/Sensor targets")
		}
		ids[peer.ID], targets[peer.Target], roles[peer.Role] = true, true, true
	}
	if len(roles) != 2 {
		return c, errors.New("peers must use both role JWTs")
	}
	paths := []string{c.ReadyPath, c.StatePath, c.StopPath, c.OutputPath}
	seen := map[string]bool{}
	for _, p := range paths {
		if !filepath.IsAbs(p) || seen[p] {
			return c, errors.New("control and output paths must be distinct absolute paths")
		}
		seen[p] = true
		d, e := os.Stat(filepath.Dir(p))
		if e != nil || !d.IsDir() || d.Mode().Perm()&0077 != 0 {
			return c, errors.New("control and output parent directories must be private")
		}
		if st, e := os.Lstat(p); e == nil && (!st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0) {
			return c, errors.New("existing control and output files must be private regular files")
		}
	}
	return c, nil
}

func runNativeP8HealthyPeers(c healthyPeersConfig) healthyPeersState {
	state := healthyPeersState{Outcome: "probe_only_not_p8_acceptance", Status: "running", Peers: make([]healthyPeerResult, 0, 2)}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.MaxLifetimeMS)*time.Millisecond)
	defer cancel()
	ca, err := readP8BoundedFile(c.CAFile, 1<<20, false)
	if err != nil {
		state.Failure = "ca_unavailable"
		state.Status = "failed"
		return state
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		state.Failure, state.Status = "invalid_ca", "failed"
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
	live := make([]*healthyPeerLive, 0, 2)
	closeAll := func() {
		for _, p := range live {
			if p.conn != nil {
				_ = p.conn.CloseNow()
			}
		}
	}
	defer closeAll()
	for _, spec := range c.Peers {
		headers := http.Header{"Authorization": {"Bearer " + c.Tokens[spec.Role]}}
		conn, _, e := websocket.Dial(ctx, c.URL, &websocket.DialOptions{HTTPClient: client, HTTPHeader: headers, Subprotocols: []string{auth.WSTelemetryProtocol}})
		if e != nil {
			state.Failure = "peer_upgrade_failed"
			break
		}
		if conn.Subprotocol() != auth.WSTelemetryProtocol {
			_ = conn.CloseNow()
			state.Failure = "subprotocol_mismatch"
			break
		}
		conn.SetReadLimit(DefaultMaxEventBytes)
		reqID := uuid.NewString()
		targets := []TargetKey{spec.Target}
		request, _ := json.Marshal(ClientMessage{ProtocolVersion: ProtocolVersion, RequestID: reqID, Action: ActionSubscribe, Targets: targets})
		if conn.Write(ctx, websocket.MessageText, request) != nil {
			_ = conn.CloseNow()
			state.Failure = "subscribe_write_failed"
			break
		}
		_, frame, e := conn.Read(ctx)
		var ack AckMessage
		if e != nil || json.Unmarshal(frame, &ack) != nil || !validP8SubscribeAck(ack, reqID, targets) {
			_ = conn.CloseNow()
			state.Failure = "subscribe_ack_mismatch"
			break
		}
		live = append(live, &healthyPeerLive{spec: spec, conn: conn, seen: map[string]struct{}{}, out: healthyPeerResult{ID: spec.ID, Role: spec.Role, Target: spec.Target}})
	}
	if state.Failure != "" || len(live) != 2 {
		closeAll()
		state.Status = "failed"
		return state
	}
	state.Ready, state.SocketCount = true, 2
	var readers sync.WaitGroup
	readerErr := make(chan string, 2)
	var totalsMu sync.Mutex
	totalEvents, totalBytes := 0, int64(0)
	for _, peer := range live {
		readers.Add(1)
		go func(p *healthyPeerLive) {
			defer readers.Done()
			for {
				_, raw, readErr := p.conn.Read(ctx)
				if readErr != nil {
					if ctx.Err() == nil {
						p.mu.Lock()
						p.out.Closed = true
						p.mu.Unlock()
						select {
						case readerErr <- "unexpected_disconnect":
						default:
						}
					}
					return
				}
				totalsMu.Lock()
				if int64(len(raw)) > c.MaxBytes-totalBytes || totalEvents >= c.MaxEvents {
					totalsMu.Unlock()
					select {
					case readerErr <- "limit_reached":
					default:
					}
					return
				}
				totalBytes += int64(len(raw))
				totalEvents++
				limitReached := totalEvents >= c.MaxEvents || totalBytes >= c.MaxBytes
				totalsMu.Unlock()
				p.mu.Lock()
				p.out.Events++
				p.out.Bytes += int64(len(raw))
				p.mu.Unlock()
				if limitReached {
					select {
					case readerErr <- "limit_reached":
					default:
					}
					return
				}
				check, checkErr := validateP8Telemetry(raw, []TargetKey{p.spec.Target})
				if checkErr != nil || !check.Matched {
					p.mu.Lock()
					p.out.Unexpected++
					p.mu.Unlock()
					select {
					case readerErr <- "unexpected_namespace_or_invalid_event":
					default:
					}
					return
				}
				var event p8TelemetryIdentity
				if json.Unmarshal(raw, &event) != nil {
					select {
					case readerErr <- "invalid_event":
					default:
					}
					return
				}
				key := event.GatewayID + "\x00" + event.SensorID + "\x00" + event.MessageID + "\x00" + event.BootID + "\x00" + event.FirstSequence
				p.mu.Lock()
				if _, duplicate := p.seen[key]; duplicate {
					p.out.Duplicates++
				} else {
					p.seen[key] = struct{}{}
					p.out.UniqueEvents++
					if len(p.out.SampledIDs) < 8 {
						p.out.SampledIDs = append(p.out.SampledIDs, event.MessageID)
					}
				}
				p.mu.Unlock()
			}
		}(peer)
	}
	if err := writePrivateJSON(c.ReadyPath, map[string]any{"ready": true, "socket_count": 2, "probe_only": true}); err != nil {
		state.Failure = "ready_write_failed"
	}
	if state.Failure == "" {
		if err := writeHealthyPeersState(c.StatePath, state, live); err != nil {
			state.Failure = "state_write_failed"
		}
	}
	for state.Failure == "" {
		select {
		case state.Failure = <-readerErr:
		default:
		}
		if state.Failure != "" {
			break
		}
		stop, e := p8PrivateSignal(c.StopPath)
		if e != nil {
			state.Failure = "invalid_stop_control"
			break
		}
		if stop {
			break
		}
		select {
		case failure := <-readerErr:
			state.Failure = failure
		case <-ctx.Done():
			state.Failure = "lifetime_deadline"
		case <-time.After(50 * time.Millisecond):
		}
		if state.Failure == "" {
			if err := writeHealthyPeersState(c.StatePath, state, live); err != nil {
				state.Failure = "state_write_failed"
			}
		}
	}
	cancel()
	closeAll()
	readers.Wait()
	state.Status = "completed"
	if state.Failure != "" {
		state.Status = "failed"
	}
	applyHealthyPeerTotals(&state, live)
	if err := writeHealthyPeersState(c.StatePath, state, live); err != nil && state.Failure == "" {
		state.Failure, state.Status = "state_write_failed", "failed"
	}
	return state
}

func applyHealthyPeerTotals(state *healthyPeersState, peers []*healthyPeerLive) {
	state.Peers = make([]healthyPeerResult, 0, len(peers))
	state.Events, state.UniqueEvents, state.Bytes, state.Duplicates, state.Unexpected = 0, 0, 0, 0, 0
	for _, p := range peers {
		p.mu.Lock()
		snapshot := p.out
		p.mu.Unlock()
		state.Peers = append(state.Peers, snapshot)
		state.Events += snapshot.Events
		state.UniqueEvents += snapshot.UniqueEvents
		state.Bytes += snapshot.Bytes
		state.Duplicates += snapshot.Duplicates
		state.Unexpected += snapshot.Unexpected
	}
}

func writeHealthyPeersState(path string, state healthyPeersState, peers []*healthyPeerLive) error {
	applyHealthyPeerTotals(&state, peers)
	return writePrivateJSON(path, state)
}
