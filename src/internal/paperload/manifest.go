// Package paperload contains the bounded P8 workload contract and telemetry runner.
package paperload

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
)

// Viewer subscriptions are serialized per viewer; a valid 10k-viewer E1 plan
// can exceed 1 MiB. Keep decoding bounded while matching the portable bundle cap.
const maxManifestBytes = 8 << 20

const (
	// MaxViewers is the absolute finite viewer bound, reserved for official opt-in.
	MaxViewers = 10000
	// MaxFiltersPerSession retains P8's bounded per-session subscription contract.
	MaxFiltersPerSession = 16
	// MaxRatePerSecond is the highest allowed offered message rate.
	MaxRatePerSecond = 20000
	// MaxDurationSeconds bounds any phase and run wall deadline.
	MaxDurationSeconds = 7200
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Manifest is the deliberately small schema-v1 P8 workload input.
type Manifest struct {
	SchemaVersion        int                   `json:"schema_version"`
	Experiment           string                `json:"experiment"`
	Arm                  string                `json:"arm"`
	Profile              string                `json:"profile"`
	Mode                 string                `json:"mode"`
	RunID                string                `json:"run_id"`
	Seed                 uint64                `json:"seed"`
	Roles                []string              `json:"roles"`
	SamplesPerMessage    int                   `json:"samples_per_message"`
	Topology             Topology              `json:"topology"`
	Targets              []Target              `json:"targets"`
	Subscriptions        []Subscription        `json:"subscriptions"`
	Traffic              Traffic               `json:"traffic"`
	Phases               Phases                `json:"phases"`
	Limits               Limits                `json:"limits"`
	TLS                  TLS                   `json:"tls"`
	IdentityFiles        map[string]string     `json:"identity_files"`
	GatewayIdentityFiles map[string]string     `json:"gateway_identity_files"`
	BundlePath           string                `json:"bundle_path"`
	DurabilityMode       string                `json:"durability_mode"`
	RetryIntervalMS      int                   `json:"retry_interval_ms"`
	PublishTimeoutMS     int                   `json:"publish_timeout_ms"`
	Viewer               ViewerSettings        `json:"viewer,omitempty"`
	Commands             []ControlCommand      `json:"commands,omitempty"`
	E4                   E4Settings            `json:"e4,omitempty"`
	EqualSamples         *EqualSamplesSettings `json:"equal_samples,omitempty"`
}

// E4Settings describes a finite unique/replay experiment. It is inert until
// the E4 runtime integration consumes it; validation never opens JournalPath.
type E4Settings struct {
	Mode                 string `json:"mode,omitempty"`
	JournalPath          string `json:"journal_path,omitempty"`
	MaxMessages          int    `json:"max_messages,omitempty"`
	MaxBytes             int64  `json:"max_bytes,omitempty"`
	ReplayCount          int    `json:"replay_count,omitempty"`
	IgnoreFirstValidACKs int    `json:"ignore_first_valid_acks,omitempty"`
	ExpiryTTLSeconds     int    `json:"expiry_ttl_seconds,omitempty"`
}

// ControlCommand is one finite REST command-stream entry. Frequency is kept as
// its exact JSON decimal token; no float conversion participates in admission.
type ControlCommand struct {
	GatewayID              string      `json:"gateway_id"`
	SensorID               string      `json:"sensor_id"`
	FrequencyHz            json.Number `json:"frequency_hz"`
	OffsetMS               int         `json:"offset_ms"`
	ActorLabel             string      `json:"actor_label"`
	IdempotencyKey         string      `json:"idempotency_key"`
	ExpectedDesiredVersion string      `json:"expected_desired_version"`
}

// ViewerSettings keeps viewer-only fault modes and accounting bounded.
type ViewerSettings struct {
	Mode           string `json:"mode,omitempty"` // healthy (default), slow, reconnect
	SlowPauseMS    int    `json:"slow_pause_ms,omitempty"`
	ReconnectCount int    `json:"reconnect_count,omitempty"`
	IdentityWindow int    `json:"identity_window,omitempty"`
	MaxMessages    int    `json:"max_messages,omitempty"`
	MaxBytes       int64  `json:"max_bytes,omitempty"`
}

// Topology counts identities and test resources described in a manifest.
type Topology struct {
	Gateways          int `json:"gateways"`
	SensorsPerGateway int `json:"sensors_per_gateway"`
	Users             int `json:"users"`
	Viewers           int `json:"viewers"`
}

// Target identifies one Sensor using its Gateway/Sensor composite identity.
type Target struct {
	GatewayID string `json:"gateway_id"`
	SensorID  string `json:"sensor_id"`
}

// Subscription maps a viewer's filter to its authenticated user and target.
type Subscription struct {
	ViewerID  string `json:"viewer_id"`
	UserID    string `json:"user_id"`
	GatewayID string `json:"gateway_id"`
	SensorID  string `json:"sensor_id,omitempty"`
}

// Traffic defines finite offered rate, ramp and burst inputs.
type Traffic struct {
	RatePerSecond      float64 `json:"rate_per_second"`
	TotalRatePerSecond float64 `json:"total_rate_per_second,omitempty"`
	RampSeconds        int     `json:"ramp_seconds"`
	BurstMessages      int     `json:"burst_messages"`
}

// Phases defines bounded warmup, measurement, drain and cooldown windows.
type Phases struct {
	WarmupSeconds   int `json:"warmup_seconds"`
	MeasureSeconds  int `json:"measure_seconds"`
	DrainSeconds    int `json:"drain_seconds"`
	CooldownSeconds int `json:"cooldown_seconds"`
}

// Limits defines finite retry, concurrency, pending, disk and wall bounds.
type Limits struct {
	Retries         int   `json:"retries"`
	Workers         int   `json:"workers"`
	Connections     int   `json:"connections"`
	PendingMessages int   `json:"pending_messages"`
	PendingBytes    int64 `json:"pending_bytes"`
	DiskGrowthBytes int64 `json:"disk_growth_bytes"`
	WallSeconds     int   `json:"wall_seconds"`
}

// TLS holds credential-free secure endpoints and CA file references.
type TLS struct {
	HTTPEndpoint   string `json:"https_endpoint"`
	MQTTEndpoint   string `json:"mqtt_tls_endpoint"`
	HTTPCAFile     string `json:"https_ca_file"`
	MqttCAFile     string `json:"mqtt_tls_ca_file"`
	MQTTServerName string `json:"mqtt_tls_server_name,omitempty"`
}

// Estimate contains deterministic preflight arithmetic, not measured values.
type Estimate struct {
	ExpectedRecipients          int   `json:"expected_recipients"`
	ExpectedSubscriptionMatches int   `json:"expected_subscription_matches"`
	UniqueUsers                 int   `json:"unique_users"`
	Connections                 int   `json:"connections"`
	SelectedRoleConnections     int   `json:"selected_role_connections"`
	ProposedMessages            int64 `json:"proposed_messages"`
	ProposedRawSamples          int64 `json:"proposed_raw_samples"`
	WindowSeconds               int   `json:"window_seconds"`
	PendingMessageLimit         int   `json:"pending_message_limit"`
	PendingByteLimit            int64 `json:"pending_byte_limit"`
	DiskGrowthLimit             int64 `json:"disk_growth_limit_bytes"`
	WallLimitSeconds            int   `json:"wall_limit_seconds"`
}

// Decode rejects unknown fields and trailing JSON. Error strings never echo input.
func Decode(r io.Reader) (Manifest, error) {
	var m Manifest
	data, err := io.ReadAll(io.LimitReader(r, maxManifestBytes+1))
	if err != nil || len(data) > maxManifestBytes {
		return m, errors.New("manifest_json_invalid")
	}
	var rawTop map[string]json.RawMessage
	if err := json.Unmarshal(data, &rawTop); err != nil {
		return m, errors.New("manifest_json_invalid")
	}
	if raw, ok := rawTop["equal_samples"]; ok && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return m, errors.New("manifest_json_invalid")
	}
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, errors.New("manifest_json_invalid")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return m, errors.New("manifest_json_invalid")
	}
	return m, nil
}

// Validate checks manifest bounds and combinations without file or network access.
func Validate(m Manifest, officialOptIn bool) (Estimate, error) {
	return ValidateAt(m, officialOptIn, time.Now().UTC())
}

// ValidateAt checks manifest bounds and combinations against a specific wall clock time.
func ValidateAt(m Manifest, officialOptIn bool, now time.Time) (Estimate, error) {
	bad := func() (Estimate, error) { return Estimate{}, errors.New("manifest_contract_rejected") }
	if m.SchemaVersion != 1 || !contains([]string{"E1", "E2", "E3", "E4", "E5"}, m.Experiment) || !safeID.MatchString(m.Arm) || !safeID.MatchString(m.RunID) {
		return bad()
	}
	if m.Profile != "pilot" && m.Profile != "official" || m.Mode != "local" && m.Mode != "remote" {
		return bad()
	}
	if m.Profile == "official" && !officialOptIn {
		return Estimate{}, errors.New("official_profile_requires_opt_in")
	}
	if m.EqualSamples != nil {
		if m.Experiment != "E2" {
			return bad()
		}
		if err := ValidateEqualSamples(m, now, true); err != nil {
			return bad()
		}
	}
	controlOnly := len(m.Roles) == 1 && m.Roles[0] == "control"
	if !controlOnly && m.SamplesPerMessage != 1 && m.SamplesPerMessage != 100 {
		return bad()
	}
	if (!controlOnly && (m.Topology.Gateways < 1 || m.Topology.Gateways > 1000 || m.Topology.SensorsPerGateway < 1 || m.Topology.SensorsPerGateway > 1000)) || m.Topology.Users < 0 || m.Topology.Users > MaxViewers || m.Topology.Viewers < 0 || m.Topology.Viewers > MaxViewers {
		return bad()
	}
	if m.Profile == "pilot" && m.Topology.Viewers > 64 {
		return bad()
	}
	if !controlOnly && (math.IsNaN(m.Traffic.RatePerSecond) || math.IsInf(m.Traffic.RatePerSecond, 0) || m.Traffic.RatePerSecond <= 0 || m.Traffic.RatePerSecond > MaxRatePerSecond || m.Traffic.RampSeconds < 0 || m.Traffic.RampSeconds > MaxDurationSeconds || m.Traffic.BurstMessages < 0 || m.Traffic.BurstMessages > 1000000) {
		return bad()
	}
	if m.Traffic.TotalRatePerSecond != 0 && (math.IsNaN(m.Traffic.TotalRatePerSecond) || math.IsInf(m.Traffic.TotalRatePerSecond, 0) || m.Traffic.TotalRatePerSecond < m.Traffic.RatePerSecond || m.Traffic.TotalRatePerSecond > MaxRatePerSecond) {
		return bad()
	}
	phaseValues := []int{m.Phases.WarmupSeconds, m.Phases.MeasureSeconds, m.Phases.DrainSeconds, m.Phases.CooldownSeconds}
	window := 0
	for _, n := range phaseValues {
		if n < 0 || n > MaxDurationSeconds {
			return bad()
		}
		window += n
	}
	if (!controlOnly && (m.Phases.MeasureSeconds == 0 || window > m.Limits.WallSeconds || m.Traffic.RampSeconds > m.Limits.WallSeconds)) || m.Limits.WallSeconds < 1 || m.Limits.WallSeconds > MaxDurationSeconds {
		return bad()
	}
	if m.Limits.Workers < 1 || m.Limits.Workers > 4096 || (!controlOnly && (m.Limits.Retries < 0 || m.Limits.Retries > 100 || m.Limits.Connections < 1 || m.Limits.Connections > 20000 || m.Limits.PendingMessages < 1 || m.Limits.PendingMessages > 100000 || m.Limits.PendingBytes < 1 || m.Limits.PendingBytes > 1<<30 || m.Limits.DiskGrowthBytes < 1 || m.Limits.DiskGrowthBytes > 1<<40)) {
		return bad()
	}
	if !controlOnly && (m.RetryIntervalMS < 100 || m.RetryIntervalMS > 60000 || m.PublishTimeoutMS < 100 || m.PublishTimeoutMS > 120000) {
		return bad()
	}
	if (m.Mode == "remote" && m.BundlePath == "" && !controlOnly) || (!controlOnly && !safeFileRef(m.BundlePath)) || !safeFileRef(m.TLS.HTTPCAFile) || (!controlOnly && !safeFileRef(m.TLS.MqttCAFile)) {
		return bad()
	}
	if !safeEndpoint(m.TLS.HTTPEndpoint, "https") || (!controlOnly && !safeEndpoint(m.TLS.MQTTEndpoint, "mqtts")) {
		return Estimate{}, errors.New("manifest_endpoint_rejected")
	}
	if m.TLS.MQTTServerName != "" && (len(m.TLS.MQTTServerName) > 253 || strings.ContainsAny(m.TLS.MQTTServerName, "/:@\x00\r\n \t")) {
		return bad()
	}
	if ip := net.ParseIP(m.TLS.MQTTServerName); ip != nil && ip.String() != m.TLS.MQTTServerName {
		return bad()
	}
	if len(m.Roles) == 0 {
		return bad()
	}
	roleSet := map[string]bool{}
	for _, role := range m.Roles {
		if !contains([]string{"telemetry", "viewer", "control"}, role) || roleSet[role] {
			return bad()
		}
		roleSet[role] = true
	}
	if !roleSet["viewer"] && len(m.Subscriptions) != 0 {
		return bad()
	}
	if m.Experiment == "E1" && (!roleSet["viewer"] || roleSet["telemetry"] || roleSet["control"]) ||
		m.Experiment == "E2" && (!roleSet["telemetry"] || roleSet["viewer"] || roleSet["control"]) ||
		m.Experiment == "E3" && (!roleSet["control"] || roleSet["viewer"]) ||
		m.Experiment == "E4" && (!roleSet["telemetry"] || roleSet["control"] || roleSet["viewer"]) {
		return bad()
	}
	if m.Experiment == "E4" {
		if len(m.Roles) != 1 || m.Roles[0] != "telemetry" || m.DurabilityMode != "synthetic-load" || m.E4 != (E4Settings{}) && !validE4Settings(m.E4) {
			return bad()
		}
	} else if m.E4 != (E4Settings{}) {
		return bad()
	}
	if roleSet["telemetry"] && roleSet["viewer"] {
		return Estimate{}, errors.New("only_telemetry_role_is_implemented")
	}
	if roleSet["telemetry"] && m.DurabilityMode != "synthetic-load" {
		if m.DurabilityMode == "durable" {
			return Estimate{}, errors.New("durable_profile_not_implemented")
		}
		return bad()
	}
	if !roleSet["telemetry"] && m.DurabilityMode != "" {
		return bad()
	}
	if roleSet["viewer"] {
		v := m.Viewer
		if v.Mode == "" {
			v.Mode = "healthy"
		}
		if v.Mode != "healthy" && v.Mode != "slow" && v.Mode != "reconnect" {
			return bad()
		}
		if v.SlowPauseMS < 0 || v.SlowPauseMS > 30000 || (v.Mode == "slow" && v.SlowPauseMS < 1) || v.ReconnectCount < 0 || v.ReconnectCount > 10 || (v.Mode != "reconnect" && v.ReconnectCount != 0) || v.IdentityWindow < 0 || v.IdentityWindow > 100000 || v.MaxMessages < 0 || v.MaxMessages > 10000000 || v.MaxBytes < 0 || v.MaxBytes > 1<<30 {
			return bad()
		}
	} else if m.Viewer != (ViewerSettings{}) {
		return bad()
	}
	if roleSet["telemetry"] {
		if _, _, err := syntheticStreamBounds(m); err != nil {
			return bad()
		}
	}
	if roleSet["telemetry"] && (m.Topology.Users != 0 || m.Topology.Viewers != 0) || !roleSet["telemetry"] && !controlOnly && (m.Topology.Users < 1 || m.Topology.Viewers < 1) {
		return bad()
	}
	if len(m.Targets) == 0 && !controlOnly || len(m.Targets) > 10000 {
		return bad()
	}
	targets := make(map[string]bool, len(m.Targets))
	gateways := map[string]bool{}
	sensorsPerGateway := map[string]int{}
	for _, target := range m.Targets {
		if gateway.ValidateGatewayID(target.GatewayID) != nil || gateway.ValidateSensorID(target.SensorID) != nil {
			return bad()
		}
		key := target.GatewayID + "\x00" + target.SensorID
		if targets[key] {
			return bad()
		}
		targets[key] = true
		gateways[target.GatewayID] = true
		sensorsPerGateway[target.GatewayID]++
		if sensorsPerGateway[target.GatewayID] > m.Topology.SensorsPerGateway {
			return bad()
		}
	}
	if len(gateways) > m.Topology.Gateways || len(targets) > m.Topology.Gateways*m.Topology.SensorsPerGateway {
		return bad()
	}
	users, viewers := map[string]bool{}, map[string]bool{}
	viewerUsers, subscriptionSet := map[string]string{}, map[string]bool{}
	viewerFilterCounts := map[string]int{}
	deliverySet := map[string]bool{}
	matches := 0
	for _, sub := range m.Subscriptions {
		if !safeID.MatchString(sub.ViewerID) || !safeID.MatchString(sub.UserID) || gateway.ValidateGatewayID(sub.GatewayID) != nil || sub.SensorID != "" && gateway.ValidateSensorID(sub.SensorID) != nil {
			return bad()
		}
		users[sub.UserID], viewers[sub.ViewerID] = true, true
		viewerFilterCounts[sub.ViewerID]++
		if viewerFilterCounts[sub.ViewerID] > MaxFiltersPerSession {
			return bad()
		}
		if prior, ok := viewerUsers[sub.ViewerID]; ok && prior != sub.UserID {
			return bad()
		}
		viewerUsers[sub.ViewerID] = sub.UserID
		subscriptionKey := strings.Join([]string{sub.ViewerID, sub.UserID, sub.GatewayID, sub.SensorID}, "\x00")
		if subscriptionSet[subscriptionKey] {
			return bad()
		}
		subscriptionSet[subscriptionKey] = true
		if sub.SensorID != "" && !targets[sub.GatewayID+"\x00"+sub.SensorID] {
			return bad()
		}
		matchedTarget := false
		for target := range targets {
			parts := strings.SplitN(target, "\x00", 2)
			if parts[0] == sub.GatewayID && (sub.SensorID == "" || parts[1] == sub.SensorID) {
				matchedTarget = true
				matches++
				deliverySet[sub.ViewerID+"\x00"+target] = true
			}
		}
		if !matchedTarget {
			return bad()
		}
	}
	if len(viewers) > m.Topology.Viewers || len(users) > m.Topology.Users || len(m.Subscriptions) > m.Topology.Viewers*MaxFiltersPerSession {
		return bad()
	}
	if roleSet["viewer"] && (len(viewers) != m.Topology.Viewers || len(m.Subscriptions) == 0) || !roleSet["viewer"] && len(m.Subscriptions) != 0 {
		return bad()
	}
	for role := range roleSet {
		if !safeFileRef(m.IdentityFiles[role]) {
			return bad()
		}
	}
	if len(m.IdentityFiles) != len(roleSet) {
		return bad()
	}
	if roleSet["telemetry"] {
		if len(m.GatewayIdentityFiles) != len(gateways) {
			return bad()
		}
		for gatewayID := range gateways {
			if !safeFileRef(m.GatewayIdentityFiles[gatewayID]) {
				return bad()
			}
		}
	} else if len(m.GatewayIdentityFiles) != 0 {
		return bad()
	}
	var messages int64
	if controlOnly {
		messages = int64(len(m.Commands))
	} else {
		messagesF := math.Ceil(m.Traffic.RatePerSecond*float64(m.Phases.MeasureSeconds)) + math.Ceil(m.Traffic.RatePerSecond*float64(m.Phases.WarmupSeconds)) + float64(m.Traffic.BurstMessages)
		if messagesF > math.MaxInt64 || messagesF < 0 {
			return bad()
		}
		messages = int64(math.Ceil(messagesF))
		if messages > math.MaxInt64/int64(m.SamplesPerMessage) {
			return bad()
		}
	}
	if controlOnly {
		if m.Experiment != "E3" || len(m.Commands) < 1 || len(m.Commands) > 1000 || len(m.IdentityFiles) != 1 || m.IdentityFiles["control"] == "" || m.TLS.HTTPCAFile == "" || m.TLS.MqttCAFile != "" || m.TLS.MQTTEndpoint != "" || m.BundlePath != "" {
			return bad()
		}
		seenKeys := map[string]bool{}
		for _, command := range m.Commands {
			keyUUID, keyErr := uuid.Parse(command.IdempotencyKey)
			if gateway.ValidateGatewayID(command.GatewayID) != nil || gateway.ValidateSensorID(command.SensorID) != nil || command.OffsetMS < 0 || command.OffsetMS > m.Limits.WallSeconds*1000 || !safeID.MatchString(command.ActorLabel) || keyErr != nil || keyUUID.Version() != 4 || keyUUID.String() != command.IdempotencyKey || seenKeys[command.IdempotencyKey] || command.ExpectedDesiredVersion == "" {
				return bad()
			}
			seenKeys[command.IdempotencyKey] = true
			body, _ := json.Marshal(struct {
				Frequency json.Number `json:"frequency_hz"`
				Version   string      `json:"expected_desired_version"`
			}{command.FrequencyHz, command.ExpectedDesiredVersion})
			if _, err := parseControlManifestRequest(body, command.IdempotencyKey); err != nil {
				return bad()
			}
		}
		if m.Phases.DrainSeconds == 0 {
			return bad()
		}
	} else if len(m.Commands) != 0 {
		return bad()
	}
	roleConnections := 0
	if roleSet["telemetry"] {
		roleConnections += len(gateways)
	}
	if roleSet["viewer"] {
		roleConnections += m.Topology.Viewers
	}
	if !controlOnly && (roleConnections < 1 || roleConnections > m.Limits.Connections) {
		return bad()
	}
	if controlOnly {
		roleConnections = 1
	}
	return Estimate{ExpectedRecipients: len(deliverySet), ExpectedSubscriptionMatches: matches, UniqueUsers: len(users),
		Connections: roleConnections, SelectedRoleConnections: roleConnections, ProposedMessages: messages, ProposedRawSamples: messages * int64(m.SamplesPerMessage),
		WindowSeconds: window, PendingMessageLimit: m.Limits.PendingMessages, PendingByteLimit: m.Limits.PendingBytes,
		DiskGrowthLimit: m.Limits.DiskGrowthBytes, WallLimitSeconds: m.Limits.WallSeconds}, nil
}

func validE4Settings(s E4Settings) bool {
	if s.Mode != "unique" && s.Mode != "replay" || !safePrivateJournalRef(s.JournalPath) ||
		s.MaxMessages < 1 || s.MaxMessages > 10000 || s.MaxBytes < 1 || s.MaxBytes > 64<<20 ||
		s.IgnoreFirstValidACKs < 0 || s.IgnoreFirstValidACKs > 10 {
		return false
	}
	if s.Mode == "unique" {
		return s.ReplayCount == 0 && s.ExpiryTTLSeconds == 0
	}
	return s.ReplayCount >= 1 && s.ReplayCount <= s.MaxMessages && s.IgnoreFirstValidACKs <= s.MaxMessages &&
		(s.ExpiryTTLSeconds == 0 || s.ExpiryTTLSeconds == 600)
}

// SafePrivateJournalRef applies the manifest's non-secret relative-file policy.
func SafePrivateJournalRef(path string) bool { return safePrivateJournalRef(path) }

func safePrivateJournalRef(path string) bool {
	return safeFileRef(path) && !strings.Contains(path, "\\") &&
		!strings.HasPrefix(path, "~") && !strings.Contains(path, ":")
}

func parseControlManifestRequest(body []byte, key string) (int64, error) {
	// The same strict rational parser as REST admission is used via the public
	// desired-request parser, preserving exact decimal and BIGINT semantics.
	request, err := digitaltwin.ParseDesiredRequest(body, key)
	if err != nil {
		return 0, err
	}
	return request.Frequency.SampleIntervalUS(), nil
}

func safeFileRef(s string) bool {
	return s != "" && !strings.ContainsAny(s, "\x00\n\r") && !strings.Contains(s, "..")
}
func safeEndpoint(raw, scheme string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != scheme || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return false
	}
	if p := u.Port(); p != "" && (p == "1883" || p == "5432" || p == "5433" || p == "2375" || p == "2376") {
		return false
	}
	return true
}
func contains(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}
