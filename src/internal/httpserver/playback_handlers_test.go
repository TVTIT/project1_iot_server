package httpserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/media"
	"iot-platform/internal/mediastorage"
)

type mockPlaybackReader struct {
	calledGatewayID string
	calledMediaID   uuid.UUID
	calledUserID    uuid.UUID
	path            string
	err             error
}

func (m *mockPlaybackReader) LookupPlaybackPath(_ context.Context, gatewayID string, mediaID, userID uuid.UUID) (string, error) {
	m.calledGatewayID = gatewayID
	m.calledMediaID = mediaID
	m.calledUserID = userID
	return m.path, m.err
}

type mockPlaybackSigner struct {
	calledPath  string
	calledCount int
	grant       mediastorage.ReadGrant
	err         error
}

func (m *mockPlaybackSigner) SignRead(_ context.Context, objectPath string) (mediastorage.ReadGrant, error) {
	m.calledCount++
	m.calledPath = objectPath
	return m.grant, m.err
}

type mockMediaListReader struct {
	calledGatewayID string
	calledUserID    uuid.UUID
	calledLimit     int
	calledCursor    string
	page            media.ListPage
	err             error
}

func (m *mockMediaListReader) ListGatewayMedia(_ context.Context, gatewayID string, userID uuid.UUID, limit int, cursor string) (media.ListPage, error) {
	m.calledGatewayID = gatewayID
	m.calledUserID = userID
	m.calledLimit = limit
	m.calledCursor = cursor
	return m.page, m.err
}

func testPlaybackRouter(t *testing.T, enabled bool, reader PlaybackPathReader, signer PlaybackSigner, verifier auth.Verifier) http.Handler {
	return testPlaybackRouterWithList(t, enabled, reader, signer, &mockMediaListReader{}, verifier)
}

func testPlaybackRouterWithList(t *testing.T, enabled bool, reader PlaybackPathReader, signer PlaybackSigner, listReader GatewayMediaListReader, verifier auth.Verifier) http.Handler {
	t.Helper()
	deps := DefaultTestRouterDependencies()
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		return false, nil
	})
	deps.TokenVerifier = verifier
	deps.MediaPlaybackEnabled = enabled
	deps.PlaybackReader = reader
	deps.PlaybackSigner = signer
	deps.MediaListReader = listReader
	router, err := NewRouter(deps)
	if err != nil {
		t.Fatalf("NewRouter failed: %v", err)
	}
	return router
}

func testJWTWithClaims(exp int64, urlStr string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d,"url":%q}`, exp, urlStr)))
	return header + "." + payload + ".c2ln"
}

func TestPlaybackRouteOffReturnsPlain404AndNoStore(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	verifierCalled := false
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		verifierCalled = true
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouter(t, false, reader, signer, verifier)

	mediaID := uuid.New()
	req := httptest.NewRequest(http.MethodPost, "/v1/gateways/gw-01/media/"+mediaID.String()+"/playback-url", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for route off, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store on route off, got %q", rec.Header().Get("Cache-Control"))
	}
	if verifierCalled {
		t.Fatal("auth verifier must not be called when route is off")
	}
	if signer.calledCount != 0 {
		t.Fatal("signer must not be called when route is off")
	}
}

func TestPlaybackRouteMissingOrInvalidJWT(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	verifier := tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token == "valid-token" {
			return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
		}
		return auth.Principal{}, errors.New("invalid token")
	})

	router := testPlaybackRouter(t, true, reader, signer, verifier)
	mediaID := uuid.New()
	path := "/v1/gateways/gw-01/media/" + mediaID.String() + "/playback-url"

	for _, tc := range []struct {
		name   string
		header string
	}{
		{"missing authorization header", ""},
		{"malformed authorization header", "Bearer "},
		{"invalid token", "Bearer bad-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, path, nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401 for %s, got %d", tc.name, rec.Code)
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("expected Cache-Control: no-store on 401, got %q", rec.Header().Get("Cache-Control"))
			}
			if signer.calledCount != 0 {
				t.Fatal("signer must not be called on unauthorized request")
			}
		})
	}
}

func TestPlaybackRouteInputValidation(t *testing.T) {
	userID := uuid.New()
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: userID, Role: "authenticated"}, nil
	})

	router := testPlaybackRouter(t, true, reader, signer, verifier)
	validMediaID := uuid.New().String()

	for _, tc := range []struct {
		name string
		path string
	}{
		{"invalid gateway format with special char", "/v1/gateways/invalid@gw/media/" + validMediaID + "/playback-url"},
		{"malformed media UUID", "/v1/gateways/gw-01/media/not-a-valid-uuid/playback-url"},
		{"zero media UUID", "/v1/gateways/gw-01/media/00000000-0000-0000-0000-000000000000/playback-url"},
		{"uppercase media UUID", "/v1/gateways/gw-01/media/" + strings.ToUpper(validMediaID) + "/playback-url"},
		{"braced media UUID", "/v1/gateways/gw-01/media/{" + validMediaID + "}/playback-url"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, nil)
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got %d", tc.name, rec.Code)
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
			}
			if signer.calledCount != 0 {
				t.Fatal("signer must not be called when inputs are invalid")
			}
		})
	}
}

func TestPlaybackRouteForwardsAuthPrincipalTypedUUID(t *testing.T) {
	mediaID := uuid.New()
	expectedUserID := uuid.New()
	expectedGatewayID := "gw-01"

	reader := &mockPlaybackReader{path: mediaID.String() + ".mp4"}
	signer := &mockPlaybackSigner{}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: expectedUserID, Role: "authenticated"}, nil
	})

	router := testPlaybackRouter(t, true, reader, signer, verifier)
	req := httptest.NewRequest(http.MethodPost, "/v1/gateways/"+expectedGatewayID+"/media/"+mediaID.String()+"/playback-url", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if reader.calledGatewayID != expectedGatewayID {
		t.Fatalf("expected gateway ID %q, got %q", expectedGatewayID, reader.calledGatewayID)
	}
	if reader.calledMediaID != mediaID {
		t.Fatalf("expected media UUID %v, got %v", mediaID, reader.calledMediaID)
	}
	if reader.calledUserID != expectedUserID {
		t.Fatalf("expected user UUID %v, got %v", expectedUserID, reader.calledUserID)
	}
}

func TestPlaybackRouteDatabaseErrors(t *testing.T) {
	mediaID := uuid.New()
	userID := uuid.New()
	path := "/v1/gateways/gw-01/media/" + mediaID.String() + "/playback-url"

	for _, tc := range []struct {
		name       string
		dbErr      error
		wantStatus int
	}{
		{"DB not found maps to identical 404", media.ErrPlaybackNotFound, http.StatusNotFound},
		{"DB timeout/dependency maps to 503", media.ErrDependency, http.StatusServiceUnavailable},
		{"DB inconsistent path fails closed to 502", media.ErrPlaybackInconsistentPath, http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &mockPlaybackReader{err: tc.dbErr}
			signer := &mockPlaybackSigner{}
			verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
				return auth.Principal{UserID: userID, Role: "authenticated"}, nil
			})

			router := testPlaybackRouter(t, true, reader, signer, verifier)
			req := httptest.NewRequest(http.MethodPost, path, nil)
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d; body: %s", tc.wantStatus, rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
			}
			if signer.calledCount != 0 {
				t.Fatalf("signer must not be called on DB error %s", tc.name)
			}
		})
	}
}

func TestPlaybackRouteSignerErrors(t *testing.T) {
	mediaID := uuid.New()
	userID := uuid.New()
	path := "/v1/gateways/gw-01/media/" + mediaID.String() + "/playback-url"

	for _, tc := range []struct {
		name       string
		signerErr  error
		wantStatus int
	}{
		{"signer unavailable maps to 503", mediastorage.ErrSignerUnavailable, http.StatusServiceUnavailable},
		{"signer contract failure maps to 502", mediastorage.ErrInvalidSignerContract, http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &mockPlaybackReader{path: mediaID.String() + ".mp4"}
			signer := &mockPlaybackSigner{err: tc.signerErr}
			verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
				return auth.Principal{UserID: userID, Role: "authenticated"}, nil
			})

			router := testPlaybackRouter(t, true, reader, signer, verifier)
			req := httptest.NewRequest(http.MethodPost, path, nil)
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d; body: %s", tc.wantStatus, rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestPlaybackRouteSuccessReturns200WithGrant(t *testing.T) {
	mediaID := uuid.New()
	userID := uuid.New()
	storagePath := mediaID.String() + ".mp4"
	expectedExpiresAt := time.Now().Add(120 * time.Second).Truncate(time.Second)
	token := testJWTWithClaims(expectedExpiresAt.Unix(), "media-video/"+storagePath)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"signedURL":"/object/sign/media-video/` + storagePath + `?token=` + token + `"}`))
	}))
	defer server.Close()

	playbackClient, err := mediastorage.NewPlayback(mediastorage.Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PlaybackOrigin:  "https://playback.example.test",
		ServiceToken:    "token",
		Timeout:         time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	reader := &mockPlaybackReader{path: storagePath}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: userID, Role: "authenticated"}, nil
	})

	router := testPlaybackRouter(t, true, reader, playbackClient, verifier)

	// Include dummy body and query parameters to verify API ignores request body and never sources path from body/query
	reqBody := `{"path":"evil.mp4","url":"https://evil.test"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/gateways/gw-01/media/"+mediaID.String()+"/playback-url?path=evil.mp4", strings.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer valid-token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store on 200, got %q", rec.Header().Get("Cache-Control"))
	}

	var resp struct {
		URL       string `json:"url"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !strings.HasPrefix(resp.URL, "https://playback.example.test/storage/v1/object/sign/media-video/"+storagePath+"?token=") {
		t.Fatalf("unexpected url in response: %s", resp.URL)
	}
	parsedTime, err := time.Parse(time.RFC3339, resp.ExpiresAt)
	if err != nil {
		t.Fatalf("expires_at not valid RFC3339: %v", err)
	}
	if parsedTime.Location() != time.UTC {
		t.Fatalf("expires_at must be UTC, got %v", parsedTime.Location())
	}
}

func TestPlaybackRouterDependencyValidation(t *testing.T) {
	deps := DefaultTestRouterDependencies()
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		return false, nil
	})
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})
	deps.MediaPlaybackEnabled = true
	deps.PlaybackReader = nil
	deps.PlaybackSigner = &mockPlaybackSigner{}

	if _, err := NewRouter(deps); err == nil {
		t.Fatal("expected error when MediaPlaybackEnabled is true and PlaybackReader is nil")
	}

	deps.PlaybackReader = &mockPlaybackReader{}
	deps.PlaybackSigner = nil
	if _, err := NewRouter(deps); err == nil {
		t.Fatal("expected error when MediaPlaybackEnabled is true and PlaybackSigner is nil")
	}

	deps.PlaybackSigner = &mockPlaybackSigner{}
	deps.MediaListReader = nil
	if _, err := NewRouter(deps); err == nil {
		t.Fatal("expected error when MediaPlaybackEnabled is true and MediaListReader is nil")
	}
}

func TestMediaListRouteOffReturnsPlain404AndNoStoreWithoutAuth(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	listReader := &mockMediaListReader{}
	verifierCalled := false
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		verifierCalled = true
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, false, reader, signer, listReader, verifier)

	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for route off, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store on route off, got %q", rec.Header().Get("Cache-Control"))
	}
	if verifierCalled {
		t.Fatal("auth verifier must not be called when route is off")
	}
	if listReader.calledGatewayID != "" {
		t.Fatal("list reader must not be called when route is off")
	}
}

func TestMediaListAbsentOrInvalidJWT(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	listReader := &mockMediaListReader{}
	verifier := tokenVerifierFunc(func(_ context.Context, token string) (auth.Principal, error) {
		if token != "valid-token" {
			return auth.Principal{}, auth.ErrInvalidToken
		}
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	// 1. Absent JWT
	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for absent JWT, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store on 401, got %q", rec.Header().Get("Cache-Control"))
	}

	// 2. Invalid JWT
	req = httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid JWT, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store on 401, got %q", rec.Header().Get("Cache-Control"))
	}
}

func TestMediaListInvalidGatewayID(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	listReader := &mockMediaListReader{}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw@invalid/media", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid gateway id, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store on 400, got %q", rec.Header().Get("Cache-Control"))
	}
}

func TestMediaListInvalidQueryParams(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	listReader := &mockMediaListReader{}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	for _, tc := range []struct {
		name  string
		query string
	}{
		{"unknown param", "?foo=bar"},
		{"duplicate limit", "?limit=10&limit=20"},
		{"limit zero", "?limit=0"},
		{"limit negative", "?limit=-5"},
		{"limit above max 100", "?limit=101"},
		{"limit non-numeric", "?limit=abc"},
		{"blank cursor", "?cursor="},
		{"duplicate cursor", "?cursor=c1&cursor=c2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media"+tc.query, nil)
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got %d; body: %s", tc.name, rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestMediaListNonmemberOrMissingGateway(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	listReader := &mockMediaListReader{err: media.ErrPlaybackNotFound}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for nonmember/missing, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to unmarshal error body: %v", err)
	}
	if body.Error.Code != "not_found" || body.Error.Message != "resource not found" {
		t.Fatalf("unexpected error envelope: %+v", body)
	}
}

func TestMediaListDatabaseError(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	listReader := &mockMediaListReader{err: media.ErrDependency}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for db dependency error, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
	}
}

func TestMediaListEmptyReturns200AndEmptyArray(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	listReader := &mockMediaListReader{
		page: media.ListPage{
			Items:      []media.ListItem{},
			NextCursor: "",
		},
	}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for empty media list, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
	}
	if rec.Header().Get("Link") != "" {
		t.Fatalf("expected no Link header on empty page, got %q", rec.Header().Get("Link"))
	}
	bodyStr := strings.TrimSpace(rec.Body.String())
	if bodyStr != "[]" {
		t.Fatalf("expected root JSON array [], got: %s", bodyStr)
	}
}

func TestMediaListSuccessContractFields(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	m1 := uuid.New()
	t1 := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	v1 := time.Date(2026, 11, 23, 10, 5, 0, 0, time.UTC)
	size1 := int64(1048576)

	listReader := &mockMediaListReader{
		page: media.ListPage{
			Items: []media.ListItem{
				{
					MediaID:            m1,
					CapturedAt:         t1,
					LastVerificationAt: v1,
					ActualSize:         size1,
				},
			},
			NextCursor: "",
		},
	}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("expected Cache-Control: no-store, got %q", rec.Header().Get("Cache-Control"))
	}

	var rawItems []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &rawItems); err != nil {
		t.Fatalf("failed to unmarshal root array: %v", err)
	}
	if len(rawItems) != 1 {
		t.Fatalf("expected 1 item, got %d", len(rawItems))
	}
	item := rawItems[0]

	// Verify exact fields present
	expectedFields := map[string]bool{
		"media_id":    true,
		"file_name":   true,
		"captured_at": true,
		"ready_at":    true,
		"size_bytes":  true,
	}
	if len(item) != len(expectedFields) {
		t.Fatalf("expected exactly %d fields, got %d: %+v", len(expectedFields), len(item), item)
	}
	for k := range item {
		if !expectedFields[k] {
			t.Errorf("unexpected secret/forbidden field in response: %q", k)
		}
	}

	if item["media_id"] != m1.String() {
		t.Errorf("media_id = %v, want %v", item["media_id"], m1.String())
	}
	if item["file_name"] != m1.String()+".mp4" {
		t.Errorf("file_name = %v, want %v", item["file_name"], m1.String()+".mp4")
	}
	if item["captured_at"] != t1.Format(time.RFC3339Nano) {
		t.Errorf("captured_at = %v, want %v", item["captured_at"], t1.Format(time.RFC3339Nano))
	}
	if item["ready_at"] != v1.Format(time.RFC3339Nano) {
		t.Errorf("ready_at = %v, want %v", item["ready_at"], v1.Format(time.RFC3339Nano))
	}
	if float64(size1) != item["size_bytes"].(float64) {
		t.Errorf("size_bytes = %v, want %v", item["size_bytes"], size1)
	}
}

func TestMediaListPaginationLinkHeader(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	nextCursor := "test-cursor-token"
	listReader := &mockMediaListReader{
		page: media.ListPage{
			Items: []media.ListItem{
				{MediaID: uuid.New(), CapturedAt: time.Now(), LastVerificationAt: time.Now(), ActualSize: 100},
			},
			NextCursor: nextCursor,
		},
	}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media?limit=10", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}
	wantLink := "</v1/gateways/gw-01/media?limit=10&cursor=" + nextCursor + ">; rel=\"next\""
	gotLink := rec.Header().Get("Link")
	if gotLink != wantLink {
		t.Fatalf("Link header mismatch:\ngot:  %q\nwant: %q", gotLink, wantLink)
	}
}

func TestMediaListMicrosecondPrecisionInSameSecond(t *testing.T) {
	reader := &mockPlaybackReader{}
	signer := &mockPlaybackSigner{}
	m1 := uuid.New()
	m2 := uuid.New()
	t1 := time.Date(2026, 11, 23, 10, 0, 0, 123456000, time.UTC)
	t2 := time.Date(2026, 11, 23, 10, 0, 0, 789012000, time.UTC)
	v1 := time.Date(2026, 11, 23, 10, 5, 0, 345678000, time.UTC)
	v2 := time.Date(2026, 11, 23, 10, 5, 0, 901234000, time.UTC)

	listReader := &mockMediaListReader{
		page: media.ListPage{
			Items: []media.ListItem{
				{MediaID: m1, CapturedAt: t1, LastVerificationAt: v1, ActualSize: 1000},
				{MediaID: m2, CapturedAt: t2, LastVerificationAt: v2, ActualSize: 2000},
			},
		},
	}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouterWithList(t, true, reader, signer, listReader, verifier)

	req := httptest.NewRequest(http.MethodGet, "/v1/gateways/gw-01/media", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var items []mediaItemJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}

	// Two items in the same second must differ in formatted output and retain microseconds
	if items[0].CapturedAt == items[1].CapturedAt {
		t.Fatalf("captured_at values in the same second must differ: %q vs %q", items[0].CapturedAt, items[1].CapturedAt)
	}
	if items[0].CapturedAt != t1.Format(time.RFC3339Nano) {
		t.Fatalf("captured_at %q != expected RFC3339Nano %q", items[0].CapturedAt, t1.Format(time.RFC3339Nano))
	}
	if items[0].ReadyAt != v1.Format(time.RFC3339Nano) {
		t.Fatalf("ready_at %q != expected RFC3339Nano %q", items[0].ReadyAt, v1.Format(time.RFC3339Nano))
	}
}

func TestPlaybackURLDelayedStorageServiceUnavailableJSON503(t *testing.T) {
	// A delayed fake Storage server that takes 60ms to respond
	fakeStorage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(60 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"signedURL": "/object/sign/media-video/test.mp4?token=fake.token.jwt",
		})
	}))
	defer fakeStorage.Close()

	// Playback client with a short timeout of 15ms
	playbackClient, err := mediastorage.NewPlayback(mediastorage.Config{
		InternalBaseURL: fakeStorage.URL,
		PlaybackOrigin:  "https://playback.example.test",
		ServiceToken:    "token",
		Timeout:         15 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPlayback failed: %v", err)
	}

	mediaID := uuid.New()
	reader := &mockPlaybackReader{path: mediaID.String() + ".mp4"}
	verifier := tokenVerifierFunc(func(_ context.Context, _ string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router := testPlaybackRouter(t, true, reader, playbackClient, verifier)

	// Run actual HTTP request against the router through an httptest.Server
	ts := httptest.NewServer(router)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/gateways/gw-01/media/"+mediaID.String()+"/playback-url", nil)
	if err != nil {
		t.Fatalf("NewRequest error: %v", err)
	}
	req.Header.Set("Authorization", "Bearer valid-token")

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("expected HTTP response, got client error: %v", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 Service Unavailable, got %d", resp.StatusCode)
	}

	var errResp struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode JSON 503 response: %v", err)
	}
	if errResp.Error.Code != "service_unavailable" || errResp.Error.Message != "storage service unavailable" {
		t.Fatalf("unexpected error response envelope: %+v", errResp)
	}
}
