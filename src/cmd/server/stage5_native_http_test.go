package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/httpserver"
)

type controlHTTPFixture struct {
	t            *testing.T
	ctx          context.Context
	admin, app   *pgxpool.Pool
	server       *httptest.Server
	gateway, urn string
	actors       map[string]uuid.UUID
	tokens       map[string]string
}

func newControlHTTPFixture(t *testing.T) *controlHTTPFixture {
	t.Helper()
	if os.Getenv("CONTROL_TEST_ISOLATED") != "1" {
		t.Skip("requires owned Task 5 PostgreSQL fixture; never reads deployment .env")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	f := &controlHTTPFixture{t: t, ctx: ctx, gateway: "h" + strings.ReplaceAll(uuid.NewString(), "-", ""), actors: make(map[string]uuid.UUID), tokens: make(map[string]string)}
	var err error
	f.admin, err = pgxpool.New(ctx, os.Getenv("CONTROL_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.admin.Close)
	f.app, err = pgxpool.New(ctx, os.Getenv("CONTROL_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.app.Close)
	var principal string
	if err = f.app.QueryRow(ctx, `SELECT current_user`).Scan(&principal); err != nil || principal != "iot_backend_app" {
		t.Fatalf("runtime principal %q: %v", principal, err)
	}
	secretBytes := make([]byte, 32)
	if _, err = rand.Read(secretBytes); err != nil {
		t.Fatal(err)
	}
	secret := hex.EncodeToString(secretBytes)
	f.exec(`INSERT INTO gateways(gateway_id,name) VALUES($1,'http fixture')`, f.gateway)
	f.exec(`INSERT INTO twin_entities(entity_id,entity_type,gateway_id,name) VALUES($1,'Gateway',$2,'http fixture')`, digitaltwin.GatewayURNPrefix+f.gateway, f.gateway)
	f.exec(`INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities WHERE gateway_id=$1`, f.gateway)
	for _, role := range []string{"owner", "operator", "viewer", "nonmember", "admin"} {
		actor := uuid.New()
		f.actors[role] = actor
		f.exec(`INSERT INTO auth.users(id,email) VALUES($1,$2)`, actor, actor.String()+"@fixture.invalid")
		if role == "owner" || role == "operator" || role == "viewer" {
			f.exec(`INSERT INTO user_gateways(user_id,gateway_id,role) VALUES($1,$2,$3)`, actor, f.gateway, role)
		}
		if role == "admin" {
			f.exec(`INSERT INTO platform_admins(user_id) VALUES($1)`, actor)
		}
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": actor.String(), "role": "authenticated", "iss": "https://fixture.invalid/auth", "aud": "authenticated", "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix(), "gateway_role": "owner"})
		f.tokens[role], err = token.SignedString([]byte(secret))
		if err != nil {
			t.Fatal(err)
		}
	}
	f.urn = f.sensor("s1")
	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{Secret: secret, Issuer: "https://fixture.invalid/auth", Audience: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := validTestConfig()
	cfg.Stage5 = config.Stage5Config{Enabled: true, Control: digitaltwin.DefaultControlConfig()}
	deps := controlHTTPDependencies(t, cfg, f.app, verifier)
	if err = populateStage5Dependencies(&deps, cfg, f.app); err != nil {
		t.Fatal(err)
	}
	handler, err := httpserver.NewRouter(deps)
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(handler)
	t.Cleanup(f.server.Close)
	return f
}

func controlHTTPDependencies(t *testing.T, cfg config.Config, pool *pgxpool.Pool, verifier auth.Verifier) httpserver.RouterDependencies {
	t.Helper()
	admin, err := auth.NewPostgresPlatformAdminChecker(pool)
	if err != nil {
		t.Fatal(err)
	}
	gwRepo, err := gateway.NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := gateway.NewService(gwRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	provRepo, err := gateway.NewPostgresProvisioningRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	prov, err := gateway.NewProvisioningService(provRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	ownerRepo, err := gateway.NewPostgresOwnerSensorProvisioningRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := gateway.NewOwnerSensorProvisioningService(ownerRepo, cfg.AuthorizationTimeout)
	if err != nil {
		t.Fatal(err)
	}
	listRepo, err := gateway.NewPostgresAdminGatewayRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	list, err := gateway.NewAdminGatewayService(listRepo, cfg.AuthorizationTimeout, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	deps := httpserver.RouterDependencies{ReadinessChecker: pool, ReadinessTimeout: time.Second, AuthorizationTimeout: cfg.AuthorizationTimeout, TokenVerifier: verifier, PlatformAdminChecker: admin, GatewayReader: gw, SensorReader: gw, GatewayProvisioner: prov, SensorProvisioner: prov, OwnerSensorProvisioner: owner, AdminGatewayLister: list, AdminMaxBodyBytes: 16384, CORSAllowedOrigins: []string{"https://client.invalid"}}
	if err = populateStage4Dependencies(&deps, cfg, pool, prov); err != nil {
		t.Fatal(err)
	}
	return deps
}

func (f *controlHTTPFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.admin.Exec(f.ctx, sql, args...); err != nil {
		f.t.Fatal(err)
	}
}

func (f *controlHTTPFixture) snapshot() string {
	f.t.Helper()
	var state string
	err := f.admin.QueryRow(f.ctx, `SELECT jsonb_build_array(to_jsonb(s),
 (SELECT count(*) FROM twin_commands WHERE entity_id=s.entity_id),
 (SELECT count(*) FROM twin_outbox WHERE command_id IN(SELECT command_id FROM twin_commands WHERE entity_id=s.entity_id)),
 (SELECT count(*) FROM twin_command_requests WHERE entity_id=s.entity_id))::text
 FROM twin_states s JOIN twin_entities e ON e.id=s.entity_id WHERE e.entity_id=$1`, f.urn).Scan(&state)
	if err != nil {
		f.t.Fatal(err)
	}
	return state
}
func (f *controlHTTPFixture) sensor(id string) string {
	f.t.Helper()
	urn := digitaltwin.SensorURNPrefix + f.gateway + ":" + id
	f.exec(`INSERT INTO sensors(gateway_id,sensor_id,name,unit) VALUES($1,$2,'sensor','degC')`, f.gateway, id)
	f.exec(`INSERT INTO twin_entities(entity_id,entity_type,gateway_id,name) VALUES($1,'Sensor',$2,'sensor')`, urn, f.gateway)
	f.exec(`INSERT INTO twin_states(entity_id) SELECT id FROM twin_entities WHERE entity_id=$1`, urn)
	f.exec(`INSERT INTO twin_relationships(source_entity_id,relationship_type,target_entity_id) SELECT g.id,'hasSensor',s.id FROM twin_entities g,twin_entities s WHERE g.entity_id=$1 AND s.entity_id=$2`, digitaltwin.GatewayURNPrefix+f.gateway, urn)
	return urn
}

func (f *controlHTTPFixture) request(method, path, role, key, body string, headers map[string]string, want int) (http.Header, []byte) {
	f.t.Helper()
	req, err := http.NewRequestWithContext(f.ctx, method, f.server.URL+path, strings.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	if role != "" {
		req.Header.Set("Authorization", "Bearer "+f.tokens[role])
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", "task53-native")
	req.Header.Set("Origin", "https://client.invalid")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := f.server.Client().Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	if resp.StatusCode != want {
		f.t.Fatalf("%s %s role=%s: status %d want %d body %s", method, path, role, resp.StatusCode, want, data)
	}
	if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("X-Request-ID") != "task53-native" || resp.Header.Get("Access-Control-Allow-Origin") != "https://client.invalid" {
		f.t.Fatalf("boundary headers: %v", resp.Header)
	}
	if want >= 400 {
		var envelope httpapi.APIErrorEnvelope
		if json.Unmarshal(data, &envelope) != nil || envelope.Error.Code == "" || envelope.Error.RequestID != "task53-native" {
			f.t.Fatalf("unsafe error envelope %s", data)
		}
	}
	return resp.Header, data
}

func TestNativeControlHTTPIntentAndScope(t *testing.T) {
	f := newControlHTTPFixture(t)
	path := "/v1/digital-twins/" + url.PathEscape(f.urn)
	body := `{"frequency_hz":0.1,"expected_desired_version":"0"}`
	before := f.snapshot()
	for _, role := range []string{"", "viewer", "nonmember", "admin"} {
		want := 404
		if role == "" {
			want = 401
		}
		if role == "viewer" {
			want = 403
		}
		f.request("PATCH", path+"/desired-state", role, "denied", body, nil, want)
	}
	if f.snapshot() != before {
		t.Fatal("denied HTTP request changed intent/state")
	}
	f.exec(`UPDATE twin_states SET reported_state='{"value":42,"sample_interval_us":10000}',reported_version=4 WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, f.urn)
	h, original := f.request("PATCH", path+"/desired-state", "owner", "first", body, nil, 202)
	var receipt struct {
		CommandID, StatusURL, Status, DesiredVersion string
		SampleIntervalUS                             int64
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(original, &fields); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(fields["command_id"], &receipt.CommandID)
	_ = json.Unmarshal(fields["status_url"], &receipt.StatusURL)
	_ = json.Unmarshal(fields["status"], &receipt.Status)
	if h.Get("Location") != receipt.StatusURL || receipt.Status != "pending" || receipt.CommandID == "" {
		t.Fatalf("receipt %s headers %v", original, h)
	}
	var commandStatus, outboxStatus string
	if err := f.admin.QueryRow(f.ctx, `SELECT c.status,o.status FROM twin_commands c JOIN twin_outbox o USING(command_id) WHERE c.command_id=$1`, receipt.CommandID).Scan(&commandStatus, &outboxStatus); err != nil || commandStatus != "pending" || outboxStatus != "pending" {
		t.Fatalf("intent was published/executed without Gateway: %s/%s %v", commandStatus, outboxStatus, err)
	}
	for _, role := range []string{"owner", "operator", "viewer"} {
		_, data := f.request("GET", receipt.StatusURL, role, "", "", nil, 200)
		if !bytes.Contains(data, []byte(`"status":"pending"`)) {
			t.Fatalf("not pending: %s", data)
		}
	}
	f.request("GET", receipt.StatusURL, "admin", "", "", nil, 404)
	other := f.sensor("s2")
	f.request("GET", "/v1/digital-twins/"+other+"/commands/"+receipt.CommandID, "viewer", "", "", nil, 404)
	_, conflict := f.request("PATCH", path+"/desired-state", "owner", "first", `{"frequency_hz":0.10000000001,"expected_desired_version":"0"}`, nil, 409)
	assertControlHTTPError(t, conflict, "idempotency_conflict")
	_, conflict = f.request("PATCH", path+"/desired-state", "owner", "stale", body, nil, 409)
	assertControlHTTPError(t, conflict, "desired_version_conflict")
	_, conflict = f.request("PATCH", path+"/desired-state", "operator", "busy", `{"frequency_hz":1,"expected_desired_version":"1"}`, nil, 409)
	assertControlHTTPError(t, conflict, "command_in_progress")
	var reported, desired string
	var version int64
	if err := f.admin.QueryRow(f.ctx, `SELECT reported_state::text,desired_state::text,reported_version FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, f.urn).Scan(&reported, &desired, &version); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(reported, "configuration") || !strings.Contains(reported, "10000") || !strings.Contains(desired, "10000000") || version != 4 {
		t.Fatalf("forged reported state: %s %s %d", reported, desired, version)
	}
	_, stateBody := f.request("GET", path+"?format=simplified", "viewer", "", "", nil, 200)
	var stateDTO httpserver.TwinStateResponseDTO
	if err := json.Unmarshal(stateBody, &stateDTO); err != nil || stateDTO.ReportedVersion != "4" || stateDTO.DesiredVersion != "1" || bytes.Contains(stateDTO.ReportedState, []byte("configuration")) || !bytes.Contains(stateDTO.DesiredState, []byte("10000000")) {
		t.Fatalf("state view forged confirmation: %s (%v)", stateBody, err)
	}
	f.request("GET", path, "viewer", "", "", nil, 200)
	f.request("PATCH", path+"/attrs", "owner", "", `{"sample_interval_us":20000}`, nil, 400)
	f.request("PATCH", path+"/attrs", "owner", "", `{"configuration":{"sample_interval_us":20000}}`, nil, 400)
	// Fixture-only terminal status permits a second version without implementing
	// the later Gateway/result runtime. Original receipt must remain pending.
	f.exec(`UPDATE twin_commands SET status='failed',completed_at=now() WHERE command_id=$1`, receipt.CommandID)
	f.request("PATCH", path+"/desired-state", "operator", "second", `{"frequency_hz":1.5,"expected_desired_version":"1"}`, nil, 202)
	_, replayed := f.request("PATCH", path+"/desired-state", "owner", "first", `{"frequency_hz":1e-1,"expected_desired_version":"0"}`, nil, 202)
	if !bytes.Equal(original, replayed) {
		t.Fatalf("replay regenerated %s vs %s", original, replayed)
	}
	f.exec(`DELETE FROM user_gateways WHERE user_id=$1 AND gateway_id=$2`, f.actors["owner"], f.gateway)
	f.request("PATCH", path+"/desired-state", "owner", "first", body, nil, 404)
	f.request("GET", receipt.StatusURL, "owner", "", "", nil, 404)
	var retained int
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, f.urn).Scan(&retained); err != nil || retained != 2 {
		t.Fatalf("membership removal cancelled accepted intent: count=%d err=%v", retained, err)
	}
	// A separately confirmed fixture proves no-op and exact 200 replay at >2^53
	// version precision; this seed is not claimed as result-runtime verification.
	noop := f.sensor("noop")
	np := "/v1/digital-twins/" + noop + "/desired-state"
	f.exec(`UPDATE twin_states SET desired_version=9007199254740993,desired_state='{"sample_interval_us":10000000}',reported_state='{"configuration":{"sample_interval_us":10000000}}' WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, noop)
	_, n := f.request("PATCH", np, "operator", "noop", `{"frequency_hz":0.1,"expected_desired_version":"9007199254740993"}`, nil, 200)
	if !bytes.Contains(n, []byte(`"command_id":null`)) || !bytes.Contains(n, []byte(`"desired_version":"9007199254740993"`)) {
		t.Fatalf("noop %s", n)
	}
	var noopWrites int
	if err := f.admin.QueryRow(f.ctx, `SELECT count(*) FROM twin_commands WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, noop).Scan(&noopWrites); err != nil || noopWrites != 0 {
		t.Fatalf("no-op created commands/outbox: count=%d err=%v", noopWrites, err)
	}
	f.exec(`UPDATE twin_states SET desired_version=desired_version+1 WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, noop)
	_, nr := f.request("PATCH", np, "operator", "noop", `{"frequency_hz":0.10,"expected_desired_version":"9007199254740993"}`, nil, 200)
	if !bytes.Equal(n, nr) {
		t.Fatal("no-op replay changed")
	}
	maxURN := f.sensor("ceiling")
	_, ceiling := f.request("PATCH", "/v1/digital-twins/"+maxURN+"/desired-state", "operator", "ceiling", `{"frequency_hz":0.00000000011102230246251565404236316680908203125,"expected_desired_version":"0"}`, nil, 202)
	if !bytes.Contains(ceiling, []byte(`"sample_interval_us":9007199254740992`)) {
		t.Fatalf("interval ceiling corrupted: %s", ceiling)
	}
}

func assertControlHTTPError(t *testing.T, data []byte, code string) {
	t.Helper()
	var envelope httpapi.APIErrorEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Error.Code != code {
		t.Fatalf("want %s: %s (%v)", code, data, err)
	}
}

func TestNativeControlHTTPValidationAndPagination(t *testing.T) {
	f := newControlHTTPFixture(t)
	base := "/v1/digital-twins/" + f.urn
	valid := `{"frequency_hz":1,"expected_desired_version":"0"}`
	before := f.snapshot()
	f.request("PATCH", base+"/desired-state", "owner", "invalid-token", "{", map[string]string{"Authorization": "Bearer invalid"}, 401)
	for _, body := range []string{`{}`, `{"frequency_hz":1,"frequency_hz":2,"expected_desired_version":"0"}`, `{"frequency_hz":101,"expected_desired_version":"0"}`, `{"frequency_hz":1e-128,"expected_desired_version":"0"}`, `{"frequency_hz":"1","expected_desired_version":"0"}`, `{"frequency_hz":null,"expected_desired_version":"0"}`, `{"frequency_hz":1,"expected_desired_version":0}`, `{"frequency_hz":1,"expected_desired_version":"9223372036854775808"}`, `{"frequency_hz":1,"expected_desired_version":"0","role":"owner"}`} {
		f.request("PATCH", base+"/desired-state", "owner", "invalid", body, nil, 400)
	}
	f.request("PATCH", base+"/desired-state", "owner", "", valid, nil, 400)
	f.request("PATCH", base+"/desired-state", "owner", strings.Repeat("a", 129), valid, nil, 400)
	f.request("PATCH", base+"/desired-state", "owner", "media", valid, map[string]string{"Content-Type": "text/plain"}, 415)
	f.request("PATCH", base+"/desired-state", "owner", "large", strings.Repeat(" ", 8193), nil, 413)
	f.request("PATCH", base+"/desired-state?limit=1", "owner", "query", valid, nil, 400)
	f.request("PATCH", base+"/desired-state", "owner", "accept", valid, map[string]string{"Accept": "application/ld+json"}, 406)
	for _, q := range []string{"?limit=0", "?limit=101", "?limit=-1", "?limit=1&limit=2", "?cursor=", "?cursor=bad", "?cursor=" + strings.Repeat("a", 513), "?other=1", "?limit=%zz", "?"} {
		f.request("GET", base+"/commands"+q, "viewer", "", "", nil, 400)
	}
	for _, role := range []string{"nonmember", "admin", "viewer"} {
		want := 404
		if role == "viewer" {
			want = 403
		}
		f.request("PATCH", base+"/desired-state", role, "bad", "{", nil, want)
	}
	gw := "/v1/digital-twins/" + digitaltwin.GatewayURNPrefix + f.gateway + "/desired-state"
	f.request("PATCH", gw, "owner", "target", valid, nil, 400)
	f.request("PATCH", gw, "nonmember", "target", valid, nil, 404)
	f.request("GET", "/v1/digital-twins/"+strings.ReplaceAll(f.urn, ":", "%253A")+"/commands", "owner", "", "", nil, 400)
	f.request("GET", "/v1/digital-twins/"+strings.ReplaceAll(f.urn, ":", "%3A")+"/commands", "owner", "", "", nil, 200)
	f.request("GET", base+"/commands/not-a-uuid", "viewer", "", "", nil, 400)
	f.request("GET", base+"/commands/"+uuid.NewString()+"?limit=1", "viewer", "", "", nil, 400)
	if f.snapshot() != before {
		t.Fatal("invalid HTTP requests changed intent/state")
	}
	// 105 safe terminal records, including timestamp ties, test HTTP keyset bounds.
	f.exec(`INSERT INTO twin_commands(command_id,entity_id,action,parameters,status,desired_version,issued_at,expires_at,control_version,requested_frequency_hz) SELECT gen_random_uuid(),e.id,'update_configuration','{"sample_interval_us":10000}','failed',n,'2026-01-01Z','2026-01-02Z',1,'100' FROM twin_entities e CROSS JOIN generate_series(1,105) n WHERE e.entity_id=$1`, f.urn)
	f.exec(`UPDATE twin_commands SET error_code='APPLY_FAILED',error_message='PRIVATE_DEVICE_DIAGNOSTIC' WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, f.urn)
	seen := map[string]bool{}
	next := base + "/commands?limit=100"
	for page := 0; next != ""; page++ {
		h, data := f.request("GET", next, "viewer", "", "", nil, 200)
		for _, forbidden := range []string{"PRIVATE_DEVICE_DIAGNOSTIC", "payload_bytes", "outbox", "@context", "result_bytes"} {
			if bytes.Contains(data, []byte(forbidden)) {
				t.Fatalf("unsafe command JSON field %s", forbidden)
			}
		}
		var result struct {
			Commands []digitaltwin.ControlCommandView `json:"commands"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Commands) > 100 || len(result.Commands) == 0 {
			t.Fatalf("page size %d", len(result.Commands))
		}
		for _, c := range result.Commands {
			if c.ErrorCode != "APPLY_FAILED" || c.ErrorMessage != digitaltwin.SafeCommandErrorMessage("APPLY_FAILED") {
				t.Fatal("raw device error escaped safe projection")
			}
			if seen[c.CommandID] || c.EntityID != f.urn {
				t.Fatal("scope/repeated cursor")
			}
			seen[c.CommandID] = true
		}
		link := h.Get("Link")
		if page == 0 && link != "" {
			other := f.sensor("cursor_other")
			cross := strings.Replace(strings.Split(strings.TrimPrefix(link, "<"), ">")[0], f.urn, other, 1)
			f.request("GET", cross, "viewer", "", "", nil, 400)
		}
		next = ""
		if link != "" {
			next = strings.Split(strings.TrimPrefix(link, "<"), ">")[0]
		}
		if page > 2 {
			t.Fatal("unbounded pagination")
		}
	}
	if len(seen) != 105 {
		t.Fatalf("listed %d", len(seen))
	}
	_, data := f.request("GET", base+"/commands", "viewer", "", "", nil, 200)
	var defaultPage struct {
		Commands []json.RawMessage `json:"commands"`
	}
	_ = json.Unmarshal(data, &defaultPage)
	if len(defaultPage.Commands) != 20 {
		t.Fatal("wrong default limit")
	}
	h, _ := f.request("OPTIONS", base+"/desired-state", "", "", "", map[string]string{"Access-Control-Request-Method": "PATCH", "Access-Control-Request-Headers": "authorization,content-type,idempotency-key,x-request-id"}, 204)
	if !strings.Contains(h.Get("Access-Control-Expose-Headers"), "Location") {
		t.Fatal("CORS Location hidden")
	}
}
