package httpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
)

func TestGatewayProvisioningHTTPIntegration(t *testing.T) {
	if os.Getenv("PROVISIONING_TEST_ISOLATED") != "1" {
		t.Skip("run sh scripts/test-stage2-provisioning.sh")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	setup, err := pgxpool.New(ctx, os.Getenv("PROVISIONING_TEST_SETUP_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	app, err := pgxpool.New(ctx, os.Getenv("PROVISIONING_TEST_BACKEND_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	var role string
	if err := app.QueryRow(ctx, "SELECT current_user").Scan(&role); err != nil || role != "iot_backend_app" {
		t.Fatal("wrong DB role")
	}
	admin, owner, operator, viewer := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	parent, target := "fixture_"+uuid.NewString(), "fixture_"+uuid.NewString()
	second := "fixture_" + uuid.NewString()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("INSERT INTO profiles(id) VALUES ($1),($2),($3),($4)", admin, owner, operator, viewer)
	exec("INSERT INTO platform_admins(user_id) VALUES ($1)", admin)
	exec("INSERT INTO gateways(gateway_id,name) VALUES ($1,'fixture')", parent)
	exec("INSERT INTO user_gateways(user_id,gateway_id,role) VALUES ($1,$4,'owner'),($2,$4,'operator'),($3,$4,'viewer')", owner, operator, viewer, parent)
	defer func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := setup.Exec(c, "DELETE FROM twin_entities WHERE gateway_id IN ($1,$2)", target, second); err != nil {
			t.Error(err)
		}
		if _, err := setup.Exec(c, "DELETE FROM gateways WHERE gateway_id IN ($1,$2,$3)", parent, target, second); err != nil {
			t.Error(err)
		}
		if _, err := setup.Exec(c, "DELETE FROM profiles WHERE id IN ($1,$2,$3,$4)", admin, owner, operator, viewer); err != nil {
			t.Error(err)
		}
	}()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	secret := hex.EncodeToString(random)
	verifier, err := auth.NewHS256Verifier(auth.VerifierConfig{Secret: secret, Issuer: "http://auth.test.local/auth/v1", Audience: "authenticated"})
	if err != nil {
		t.Fatal(err)
	}
	checker, err := auth.NewPostgresPlatformAdminChecker(app)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := gateway.NewPostgresRepository(app)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := gateway.NewService(repo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	writeRepo, err := gateway.NewPostgresProvisioningRepository(app)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := gateway.NewProvisioningService(writeRepo, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewRouter(RouterDependencies{ReadinessChecker: app, ReadinessTimeout: time.Second, AuthorizationTimeout: time.Second, TokenVerifier: verifier, PlatformAdminChecker: checker, GatewayReader: reader, SensorReader: reader, GatewayProvisioner: writer, SensorProvisioner: writer, AdminMaxBodyBytes: 16384})
	if err != nil {
		t.Fatal(err)
	}
	token := func(id uuid.UUID) string {
		t.Helper()
		now := time.Now()
		// Even signed, client metadata is never platform-admin authority.
		s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": id.String(), "role": "authenticated", "iss": "http://auth.test.local/auth/v1", "aud": "authenticated", "iat": now.Unix(), "exp": now.Add(time.Minute).Unix(), "user_metadata": map[string]any{"platform_admin": true, "role": "service_role"}, "app_metadata": map[string]any{"platform_admin": true}}).SignedString([]byte(secret))
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	request := func(id uuid.UUID, method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if id != uuid.Nil {
			req.Header.Set("Authorization", "Bearer "+token(id))
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	body := `{"name":" raw ","description":null,"owner_user_id":"` + owner.String() + `"}`
	path := "/v1/admin/gateways/" + target
	for _, id := range []uuid.UUID{uuid.Nil, owner, operator, viewer} {
		want := 403
		if id == uuid.Nil {
			want = 401
		}
		if rec := request(id, "PUT", path, body); rec.Code != want {
			t.Fatalf("denial=%d want=%d", rec.Code, want)
		}
	}
	for _, want := range []int{201, 200} {
		rec := request(admin, "PUT", path, body)
		if rec.Code != want {
			t.Fatalf("PUT=%d body=%s", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("cache")
		}
	}
	if rec := request(admin, "PUT", path, strings.Replace(body, " raw ", "changed", 1)); rec.Code != 409 {
		t.Fatalf("conflict=%d", rec.Code)
	}
	if rec := request(admin, "PUT", path+"_missing", strings.Replace(body, owner.String(), uuid.NewString(), 1)); rec.Code != 404 {
		t.Fatalf("missing owner=%d", rec.Code)
	}
	for _, id := range []uuid.UUID{admin, owner, operator, viewer} {
		rec := request(id, "GET", "/v1/gateways", "")
		if rec.Code != 200 {
			t.Fatal("read failed")
		}
		var response struct {
			Items []gatewayDTO `json:"items"`
		}
		if json.Unmarshal(rec.Body.Bytes(), &response) != nil {
			t.Fatal("bad response")
		}
		found := false
		for _, item := range response.Items {
			if item.GatewayID == target {
				found = true
				if item.Role != gateway.Role("owner") {
					t.Fatal("owner role")
				}
			}
		}
		if found != (id == owner) {
			t.Fatal("provisioning granted unexpected reads")
		}
	}
	var members, states int
	if err := setup.QueryRow(ctx, "SELECT count(*) FROM user_gateways WHERE gateway_id=$1", target).Scan(&members); err != nil || members != 1 {
		t.Fatal("membership invariant")
	}
	if err := setup.QueryRow(ctx, `SELECT count(*) FROM twin_entities e JOIN twin_states s ON s.entity_id=e.id WHERE e.gateway_id=$1 AND e.entity_type='Gateway' AND s.reported_state='{}'::jsonb AND s.desired_state='{}'::jsonb AND s.reported_version=0 AND s.desired_version=0 AND s.last_desired_by IS NULL`, target).Scan(&states); err != nil || states != 1 {
		t.Fatalf("state invariant: %d %v", states, err)
	}
	if rec := request(owner, "PUT", path+"/sensors/test", `{"name":"test"}`); rec.Code != 403 {
		t.Fatal("Sensor route must deny non-admin")
	}

	sensorPath := path + "/sensors/shared"
	sensorBody := `{"name":" raw sensor ","unit":null}`
	for _, id := range []uuid.UUID{uuid.Nil, owner, operator, viewer} {
		want := 403
		if id == uuid.Nil {
			want = 401
		}
		if rec := request(id, "PUT", sensorPath, sensorBody); rec.Code != want {
			t.Fatalf("sensor denial=%d want=%d", rec.Code, want)
		}
	}
	for _, want := range []int{201, 200} {
		rec := request(admin, "PUT", sensorPath, sensorBody)
		if rec.Code != want || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("sensor PUT=%d body=%s", rec.Code, rec.Body.String())
		}
		var dto map[string]any
		if json.Unmarshal(rec.Body.Bytes(), &dto) != nil || len(dto) != 6 || dto["entity_id"] != "urn:ngsi-ld:Sensor:"+target+":shared" || dto["unit"] != nil {
			t.Fatalf("sensor DTO=%s", rec.Body.String())
		}
	}
	if rec := request(admin, "PUT", sensorPath, `{"name":" raw sensor ","unit":""}`); rec.Code != 409 {
		t.Fatalf("null/empty conflict=%d", rec.Code)
	}
	if rec := request(admin, "PUT", "/v1/admin/gateways/absent_"+uuid.NewString()+"/sensors/shared", sensorBody); rec.Code != 404 {
		t.Fatalf("parent not found=%d", rec.Code)
	}
	// A legacy parent without its Twin graph is not repaired by this API.
	if rec := request(admin, "PUT", "/v1/admin/gateways/"+parent+"/sensors/shared", sensorBody); rec.Code != 500 {
		t.Fatalf("inconsistent parent=%d", rec.Code)
	}
	for _, id := range []uuid.UUID{admin, operator, viewer} {
		if rec := request(id, "GET", "/v1/gateways/"+target+"/sensors", ""); rec.Code != 404 {
			t.Fatalf("non-member sensor read=%d", rec.Code)
		}
	}
	rec := request(owner, "GET", "/v1/gateways/"+target+"/sensors", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"sensor_id":"shared"`) || !strings.Contains(rec.Body.String(), `"name":" raw sensor "`) || !strings.Contains(rec.Body.String(), `"unit":null`) {
		t.Fatalf("owner read=%d %s", rec.Code, rec.Body.String())
	}
	// The admin has no membership anywhere and provisioning must not grant one.
	if rec := request(admin, "GET", "/v1/gateways", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"items":[]`) {
		t.Fatalf("admin read isolation=%d %s", rec.Code, rec.Body.String())
	}
	secondPath := "/v1/admin/gateways/" + second
	if rec := request(admin, "PUT", secondPath, strings.Replace(body, owner.String(), viewer.String(), 1)); rec.Code != 201 {
		t.Fatalf("second gateway=%d %s", rec.Code, rec.Body.String())
	}
	if rec := request(admin, "PUT", secondPath+"/sensors/shared", `{"name":"second","unit":"C"}`); rec.Code != 201 {
		t.Fatalf("same ID different gateway=%d %s", rec.Code, rec.Body.String())
	}
	if rec := request(owner, "GET", "/v1/gateways/"+second+"/sensors", ""); rec.Code != 404 {
		t.Fatalf("other gateway=%d", rec.Code)
	}
	if rec := request(viewer, "GET", "/v1/gateways/"+second+"/sensors", ""); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"name":"second"`) {
		t.Fatalf("second sensor read=%d %s", rec.Code, rec.Body.String())
	}
	var graphCount int
	if err := setup.QueryRow(ctx, `SELECT count(*) FROM sensors s JOIN twin_entities e ON e.gateway_id=s.gateway_id AND e.entity_id='urn:ngsi-ld:Sensor:'||s.gateway_id||':'||s.sensor_id JOIN twin_states st ON st.entity_id=e.id JOIN twin_relationships r ON r.target_entity_id=e.id AND r.relationship_type='hasSensor' JOIN twin_entities p ON p.id=r.source_entity_id AND p.gateway_id=s.gateway_id AND p.entity_type='Gateway' WHERE s.sensor_id='shared' AND s.gateway_id IN ($1,$2) AND st.reported_state='{}'::jsonb AND st.desired_state='{}'::jsonb AND st.reported_version=0 AND st.desired_version=0 AND st.last_desired_by IS NULL`, target, second).Scan(&graphCount); err != nil || graphCount != 2 {
		t.Fatalf("sensor graph=%d %v", graphCount, err)
	}
	// Retry must not reset existing reported/desired state, or repair missing state.
	exec(`UPDATE twin_states SET reported_state='{"value":42}', desired_state='{"enabled":true}', reported_version=7, desired_version=8 WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, "urn:ngsi-ld:Sensor:"+target+":shared")
	if rec := request(admin, "PUT", sensorPath, sensorBody); rec.Code != 200 {
		t.Fatalf("stateful retry=%d %s", rec.Code, rec.Body.String())
	}
	var preserved bool
	if err := setup.QueryRow(ctx, `SELECT reported_state='{"value":42}'::jsonb AND desired_state='{"enabled":true}'::jsonb AND reported_version=7 AND desired_version=8 FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, "urn:ngsi-ld:Sensor:"+target+":shared").Scan(&preserved); err != nil || !preserved {
		t.Fatalf("retry reset state: %v", err)
	}
	exec(`DELETE FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, "urn:ngsi-ld:Sensor:"+target+":shared")
	if rec := request(admin, "PUT", sensorPath, sensorBody); rec.Code != 500 {
		t.Fatalf("missing state=%d %s", rec.Code, rec.Body.String())
	}
	if err := setup.QueryRow(ctx, `SELECT count(*) FROM twin_states WHERE entity_id=(SELECT id FROM twin_entities WHERE entity_id=$1)`, "urn:ngsi-ld:Sensor:"+target+":shared").Scan(&states); err != nil || states != 0 {
		t.Fatal("API repaired missing state")
	}
	t.Run("concurrent HTTP creates and conflicts", func(t *testing.T) {
		for _, sensor := range []bool{false, true} {
			for _, differing := range []bool{false, true} {
				p := "/v1/admin/gateways/" + target + "/sensors/test_" + uuid.NewString()
				b := `{"name":"concurrent"}`
				if !sensor {
					// Reuse the two tracked Gateways for cleanup, removing the
					// disposable graph before each pair.
					exec("DELETE FROM twin_entities WHERE gateway_id=$1", second)
					exec("DELETE FROM gateways WHERE gateway_id=$1", second)
					p = secondPath
					b = `{"name":"concurrent","owner_user_id":"` + owner.String() + `"}`
				}
				bodies := []string{b, b}
				if differing {
					bodies[1] = strings.Replace(b, "concurrent", "different", 1)
				}
				start := make(chan struct{})
				codes := make(chan int, 2)
				for _, payload := range bodies {
					go func(payload string) {
						<-start
						codes <- request(admin, "PUT", p, payload).Code
					}(payload)
				}
				close(start)
				counts := map[int]int{}
				for range 2 {
					select {
					case code := <-codes:
						counts[code]++
					case <-ctx.Done():
						t.Fatal("HTTP concurrency exceeded deadline")
					}
				}
				other := 200
				if differing {
					other = 409
				}
				if counts[201] != 1 || counts[other] != 1 {
					t.Fatalf("HTTP concurrency statuses: %v", counts)
				}
			}
		}
	})
	t.Run("revoked owner retry does not restore access", func(t *testing.T) {
		exec("DELETE FROM user_gateways WHERE gateway_id=$1", target)
		if rec := request(admin, "PUT", path, body); rec.Code != 500 {
			t.Fatal("revoked owner retry should fail closed")
		}
		var n int
		if err := setup.QueryRow(ctx, "SELECT count(*) FROM user_gateways WHERE gateway_id=$1", target).Scan(&n); err != nil || n != 0 {
			t.Fatal("owner restored")
		}
	})
	t.Run("admin revoked after middleware before transaction", func(t *testing.T) {
		for _, sensor := range []bool{false, true} {
			exec("INSERT INTO platform_admins(user_id) VALUES ($1) ON CONFLICT DO NOTHING", admin)
			wrapper := &revokeBeforeProvisioning{delegate: writer, before: func() { exec("DELETE FROM platform_admins WHERE user_id=$1", admin) }}
			r, err := NewRouter(RouterDependencies{ReadinessChecker: app, ReadinessTimeout: time.Second, AuthorizationTimeout: time.Second, TokenVerifier: verifier, PlatformAdminChecker: checker, GatewayReader: reader, SensorReader: reader, GatewayProvisioner: wrapper, SensorProvisioner: wrapper, AdminMaxBodyBytes: 16384})
			if err != nil {
				t.Fatal(err)
			}
			p, b := path, body
			if sensor {
				p, b = path+"/sensors/revoked", sensorBody
			}
			req := httptest.NewRequest("PUT", p, strings.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+token(admin))
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != 403 || !wrapper.called {
				t.Fatalf("transaction recheck denial=%d reached=%t", rec.Code, wrapper.called)
			}
			if strings.Contains(rec.Body.String(), "postgres") || strings.Contains(rec.Body.String(), secret) {
				t.Fatal("sensitive error response")
			}
		}
	})
}

// Controlled test seam runs only after route middleware has authorized the
// actor; no production endpoint or authorization bypass is introduced.
type revokeBeforeProvisioning struct {
	delegate interface {
		ProvisionGateway(context.Context, uuid.UUID, gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error)
		ProvisionSensor(context.Context, uuid.UUID, gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error)
	}
	before func()
	once   sync.Once
	called bool
}

func (w *revokeBeforeProvisioning) ProvisionGateway(ctx context.Context, actor uuid.UUID, in gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error) {
	w.once.Do(func() { w.called = true; w.before() })
	return w.delegate.ProvisionGateway(ctx, actor, in)
}
func (w *revokeBeforeProvisioning) ProvisionSensor(ctx context.Context, actor uuid.UUID, in gateway.ProvisionSensorInput) (gateway.ProvisionSensorResult, error) {
	w.once.Do(func() { w.called = true; w.before() })
	return w.delegate.ProvisionSensor(ctx, actor, in)
}
