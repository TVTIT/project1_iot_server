package mediastorage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, handler http.Handler) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	client, err := New(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PublicOrigin:    "https://media.example.test",
		ServiceToken:    "private-storage-token",
		Timeout:         time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

func jwtWithExp(exp int64) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, exp)))
	return header + "." + payload + ".c2ln"
}

func jwtWithClaims(exp int64, urlClaim string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := map[string]any{"exp": exp}
	if urlClaim != "" {
		claims["url"] = urlClaim
	}
	payloadBytes, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(payloadBytes)
	return header + "." + payload + ".c2ln"
}

func TestSignUploadBuildsPublicURLAndExpiry(t *testing.T) {
	objectPath := "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4"
	token := jwtWithExp(time.Now().Add(time.Minute).Unix())
	signedPath := "/object/upload/sign/" + VideoBucketID + "/" + objectPath + "?token=" + url.QueryEscape(token)
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/storage/v1/object/upload/sign/"+VideoBucketID+"/"+objectPath {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.Header.Get("Authorization") != "Bearer private-storage-token" {
			t.Errorf("missing internal bearer")
		}
		if r.Header.Get("x-upsert") != "false" || r.Header.Get("Content-Type") != "video/mp4" {
			t.Errorf("unexpected signing headers: %#v", r.Header)
		}
		if _, err := fmt.Fprintf(w, `{"url":%q,"token":%q}`, signedPath, token); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	grant, err := client.SignUpload(context.Background(), objectPath)
	if err != nil {
		t.Fatal(err)
	}
	if grant.SignedURL() != "https://media.example.test/storage/v1"+signedPath {
		t.Fatalf("unexpected signed URL: %s", grant.SignedURL())
	}
	if !grant.ExpiresAt.After(time.Now()) {
		t.Fatalf("expiry not exposed: %v", grant.ExpiresAt)
	}
}

func TestSignUploadRejectsInvalidResponseAndExpiry(t *testing.T) {
	tests := []struct {
		name, response string
	}{
		{"wrong path", `{"url":"/object/upload/sign/media-video/other.mp4?token=x","token":"x"}`},
		{"absolute origin injection", `{"url":"https://attacker.test/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=x","token":"x"}`},
		{"fragment", `{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=x#fragment","token":"x"}`},
		{"userinfo origin injection", `{"url":"https://user@attacker.test/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=x","token":"x"}`},
		{"URL token mismatch", `{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=one","token":"two"}`},
		{"expired token", `{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=expired","token":"expired"}`},
		{"quoted expiry", ""},
		{"unreasonable expiry", ""},
		{"invalid token", `{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=bad","token":"bad"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				body := tt.response
				if tt.name == "expired token" {
					expired := jwtWithExp(time.Now().Add(-time.Minute).Unix())
					body = fmt.Sprintf(`{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=%s","token":%q}`, expired, expired)
				}
				if tt.name == "quoted expiry" {
					header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`))
					payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":"%d"}`, time.Now().Add(time.Minute).Unix())))
					quoted := header + "." + payload + ".c2ln"
					body = fmt.Sprintf(`{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=%s","token":%q}`, quoted, quoted)
				}
				if tt.name == "unreasonable expiry" {
					farFuture := jwtWithExp(time.Now().Add(48 * time.Hour).Unix())
					body = fmt.Sprintf(`{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=%s","token":%q}`, farFuture, farFuture)
				}
				if _, err := fmt.Fprint(w, body); err != nil {
					t.Errorf("write response: %v", err)
				}
			}))
			defer server.Close()
			_, err := client.SignUpload(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
			if err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestSignUploadRedactsResponseAndRequestFailure(t *testing.T) {
	secret := "do-not-leak-token"
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		if _, err := fmt.Fprint(w, secret); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()
	_, err := client.SignUpload(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("error should be generic and redacted: %v", err)
	}
}

func TestOpenStreamsAuthenticatedObjectAndDistinguishesNotFound(t *testing.T) {
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/storage/v1/object/authenticated/"+VideoBucketID+"/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
		}
		if r.Header.Get("Authorization") != "Bearer private-storage-token" {
			t.Error("missing bearer")
		}
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Length", "4")
		if _, err := io.WriteString(w, "data"); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()
	obj, err := client.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := obj.Body.Close(); err != nil {
			t.Errorf("close object body: %v", err)
		}
	}()
	if obj.ContentType != "video/mp4" || obj.ContentLength != 4 {
		t.Fatalf("unexpected metadata: %#v", obj)
	}
	body, _ := io.ReadAll(obj.Body)
	if string(body) != "data" {
		t.Fatalf("unexpected body %q", body)
	}

	missing, missingServer := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		if _, err := fmt.Fprint(w, `{"statusCode":"404","error":"not_found","message":"Object not found","code":"NoSuchKey"}`); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer missingServer.Close()
	_, err = missing.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err != ErrNotFound {
		t.Fatalf("expected definite not found, got %v", err)
	}
}

func TestDeleteOnlyTreatsObjectNoSuchKeyAsIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{"deleted", http.StatusOK, "", false},
		{"object missing", http.StatusNotFound, `{"statusCode":"404","error":"not_found","message":"Object not found","code":"NoSuchKey"}`, false},
		{"native storage object missing", http.StatusBadRequest, `{"statusCode":"404","error":"not_found","message":"Object not found","code":"NoSuchKey"}`, false},
		{"bucket missing", http.StatusNotFound, `{"statusCode":"404","error":"Bucket not found","message":"Bucket not found","code":"NoSuchBucket"}`, true},
		{"proxy 404", http.StatusNotFound, `{"statusCode":"404","error":"Not Found","message":"Route not found","code":"InvalidRequest"}`, true},
		{"opaque 404", http.StatusNotFound, `not found`, true},
		{"forbidden", http.StatusForbidden, `{"code":"AccessDenied"}`, true},
		{"server error", http.StatusInternalServerError, `internal`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete || r.Header.Get("Authorization") != "Bearer private-storage-token" {
					t.Errorf("unexpected delete request")
				}
				w.WriteHeader(tc.status)
				if _, err := fmt.Fprint(w, tc.body); err != nil {
					t.Errorf("write response: %v", err)
				}
			}))
			defer server.Close()
			err := client.Delete(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
			if (err != nil) != tc.wantErr {
				t.Fatalf("Delete returned %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestStorageMissingRequiresMachineCodedNoSuchKey(t *testing.T) {
	for _, body := range []string{
		`{"statusCode":"404","error":"Bucket not found","message":"Bucket not found","code":"NoSuchBucket"}`,
		`{"statusCode":"404","error":"Not Found","message":"Route not found","code":"InvalidRequest"}`,
		`not json`,
		`{"statusCode":"404","code":"NoSuchKey"}`,
	} {
		client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			if _, err := fmt.Fprint(w, body); err != nil {
				t.Errorf("write response: %v", err)
			}
		}))
		_, err := client.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
		server.Close()
		if err == nil || err == ErrNotFound {
			t.Fatalf("404 body %q misclassified as missing: %v", body, err)
		}
	}
}

func TestStorageReadMapsNativeNoSuchKeyBadRequest(t *testing.T) {
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		if _, err := fmt.Fprint(w, `{"statusCode":"404","error":"not_found","message":"Object not found","code":"NoSuchKey"}`); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()
	_, err := client.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err != ErrNotFound {
		t.Fatalf("expected native Storage NoSuchKey to map to ErrNotFound, got %v", err)
	}
}

func TestSensitiveValuesAreFmtAndJSONSafe(t *testing.T) {
	token := jwtWithExp(time.Now().Add(time.Minute).Unix())
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := fmt.Fprintf(w, `{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=%s","token":%q}`,
			token, token); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()
	cfg := Config{InternalBaseURL: server.URL + "/storage/v1", PublicOrigin: "https://media.example.test", ServiceToken: "private-storage-token"}
	grant, err := client.SignUpload(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err != nil {
		t.Fatal(err)
	}
	values := []any{cfg, client, grant}
	for _, value := range values {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			formatted := fmt.Sprintf(format, value)
			if strings.Contains(formatted, "private-storage-token") ||
				strings.Contains(formatted, grant.SignedURL()) {
				t.Fatalf("%s disclosed a sensitive value using %s", format, format)
			}
		}
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "private-storage-token") {
		t.Fatal("JSON exposed Storage bearer token")
	}
	encoded, err = json.Marshal(grant)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), grant.SignedURL()) || strings.Contains(string(encoded), "token=") {
		t.Fatal("JSON exposed signed upload URL")
	}

	readGrant := ReadGrant{url: "https://playback.example.test/storage/v1/object/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=secret-token", ExpiresAt: time.Now().Add(time.Minute)}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		formatted := fmt.Sprintf(format, readGrant)
		if formatted != "mediastorage.ReadGrant{redacted}" {
			t.Fatalf("%s produced unexpected format: %q", format, formatted)
		}
		if strings.Contains(formatted, readGrant.SignedURL()) || strings.Contains(formatted, "secret-token") {
			t.Fatalf("%s disclosed sensitive value: %s", format, formatted)
		}
	}
	encoded, err = json.Marshal(readGrant)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), readGrant.SignedURL()) || strings.Contains(string(encoded), "secret-token") {
		t.Fatal("JSON exposed signed playback URL")
	}
}

func testPlaybackClient(t *testing.T, handler http.Handler) (*PlaybackClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	client, err := NewPlayback(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PlaybackOrigin:  "https://playback.example.test",
		ServiceToken:    "private-storage-token",
		Timeout:         time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

func TestSignReadBuildsPlaybackURLAndExpiry(t *testing.T) {
	objectPath := "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4"
	token := jwtWithClaims(time.Now().Add(120*time.Second).Unix(), VideoBucketID+"/"+objectPath)
	signedPath := "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + url.QueryEscape(token)
	client, server := testPlaybackClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/storage/v1/object/sign/"+VideoBucketID+"/"+objectPath {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.Header.Get("Authorization") != "Bearer private-storage-token" {
			t.Errorf("missing internal bearer")
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected content type: %s", r.Header.Get("Content-Type"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		var payload struct {
			ExpiresIn int `json:"expiresIn"`
		}
		if err := json.Unmarshal(body, &payload); err != nil || payload.ExpiresIn != 120 {
			t.Errorf("unexpected request body: %s", string(body))
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := fmt.Fprintf(w, `{"signedURL":%q}`, signedPath); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	grant, err := client.SignRead(context.Background(), objectPath)
	if err != nil {
		t.Fatal(err)
	}
	expectedURL := "https://playback.example.test/storage/v1" + signedPath
	if grant.SignedURL() != expectedURL {
		t.Fatalf("unexpected signed URL:\n got: %s\nwant: %s", grant.SignedURL(), expectedURL)
	}
	if !grant.ExpiresAt.After(time.Now()) {
		t.Fatalf("expiry not exposed: %v", grant.ExpiresAt)
	}
	if grant.ExpiresAt.Location() != time.UTC {
		t.Fatalf("expected UTC expiry location, got: %v", grant.ExpiresAt.Location())
	}
}

func TestSignReadRejectsInvalidPathBeforeNetwork(t *testing.T) {
	client, server := testPlaybackClient(t, http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("network call made for invalid object path")
	}))
	defer server.Close()

	for _, invalidPath := range []string{
		"",
		"../other.mp4",
		"2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.png",
		"2FBC3F6F-6FB8-4B6E-8F2E-0C0E5E560BBB.mp4",
		"not-a-uuid.mp4",
		"2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4/extra",
	} {
		_, err := client.SignRead(context.Background(), invalidPath)
		if err == nil {
			t.Fatalf("path %q should be rejected", invalidPath)
		}
		if !errors.Is(err, ErrInvalidPath) {
			t.Fatalf("expected ErrInvalidPath for %q, got: %v", invalidPath, err)
		}
	}
}

func TestSignReadTableResponseRejections(t *testing.T) {
	objectPath := "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4"
	validToken := jwtWithClaims(time.Now().Add(120*time.Second).Unix(), VideoBucketID+"/"+objectPath)

	tests := []struct {
		name       string
		handler    func(w http.ResponseWriter, r *http.Request)
		rawBody    string
		signedPath string
	}{
		{
			name:       "wrong object path",
			signedPath: "/object/sign/" + VideoBucketID + "/00000000-0000-0000-0000-000000000000.mp4?token=" + validToken,
		},
		{
			name:       "wrong bucket in path",
			signedPath: "/object/sign/other-bucket/" + objectPath + "?token=" + validToken,
		},
		{
			name:       "absolute origin URL",
			signedPath: "https://attacker.test/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + validToken,
		},
		{
			name:       "network-path URL",
			signedPath: "//attacker.test/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + validToken,
		},
		{
			name:       "opaque URL",
			signedPath: "mailto:object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + validToken,
		},
		{
			name:       "userinfo origin injection",
			signedPath: "https://user:pass@attacker.test/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + validToken,
		},
		{
			name:       "fragment in URL",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + validToken + "#frag",
		},
		{
			name:       "alternate encoded path with percent hex",
			signedPath: "/object/sign/" + VideoBucketID + "/%32%66%62%63%33%66%36%66-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=" + validToken,
		},
		{
			name:       "alternate encoded path with dotdot",
			signedPath: "/object/sign/" + VideoBucketID + "/%2e%2e/" + VideoBucketID + "/" + objectPath + "?token=" + validToken,
		},
		{
			name:       "duplicate token query parameter",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + validToken + "&token=" + validToken,
		},
		{
			name:       "foreign query parameter",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + validToken + "&extra=injection",
		},
		{
			name:       "bare token query without value",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token",
		},
		{
			name:       "bare token query with empty value",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=",
		},
		{
			name:       "missing query string entirely",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath,
		},
		{
			name: "expired token",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				jwtWithClaims(time.Now().Add(-60*time.Second).Unix(), VideoBucketID+"/"+objectPath),
		},
		{
			name: "materially too short exp (60s instead of 120s)",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				jwtWithClaims(time.Now().Add(60*time.Second).Unix(), VideoBucketID+"/"+objectPath),
		},
		{
			name: "materially too long exp (300s instead of 120s)",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				jwtWithClaims(time.Now().Add(300*time.Second).Unix(), VideoBucketID+"/"+objectPath),
		},
		{
			name:       "bad JWT shape (not 3 parts)",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=not.a.valid.jwt.four.parts",
		},
		{
			name: "bad JWT header JSON",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				base64.RawURLEncoding.EncodeToString([]byte("not-json")) + "." +
				base64.RawURLEncoding.EncodeToString([]byte(`{"exp":1000}`)) + ".sig",
		},
		{
			name: "bad JWT payload JSON",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." +
				base64.RawURLEncoding.EncodeToString([]byte("not-json")) + ".sig",
		},
		{
			name: "missing exp in JWT claims",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." +
				base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user"}`)) + ".sig",
		},
		{
			name: "quoted string exp in JWT claims",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." +
				base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":"%d"}`, time.Now().Add(120*time.Second).Unix()))) + ".sig",
		},
		{
			name: "float exp in JWT claims",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." +
				base64.RawURLEncoding.EncodeToString([]byte(`{"exp":12345.67}`)) + ".sig",
		},
		{
			name: "negative exp in JWT claims",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" +
				base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." +
				base64.RawURLEncoding.EncodeToString([]byte(`{"exp":-100}`)) + ".sig",
		},
		{
			name: "redirect response from storage",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "https://attacker.test/redirect", http.StatusFound)
			},
		},
		{
			name: "non-2xx 400 Bad Request",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"bad_request"}`))
			},
		},
		{
			name: "non-2xx 401 Unauthorized",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			},
		},
		{
			name: "non-2xx 403 Forbidden",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"forbidden"}`))
			},
		},
		{
			name: "non-2xx 404 Not Found",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"not_found"}`))
			},
		},
		{
			name:    "response with url instead of signedURL (no fallback permitted)",
			rawBody: `{"url":"/object/sign/` + VideoBucketID + `/` + objectPath + `?token=` + validToken + `"}`,
		},
		{
			name:       "empty fragment marker at end of signed URL",
			signedPath: "/object/sign/" + VideoBucketID + "/" + objectPath + "?token=" + validToken + "#",
		},
		{
			name:    "oversize response body > 16KB",
			rawBody: `{"signedURL":"` + strings.Repeat("x", 20000) + `"}`,
		},
		{
			name:    "malformed JSON response body",
			rawBody: `not valid json`,
		},
		{
			name:    "empty JSON object in response",
			rawBody: `{}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := testPlaybackClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.handler != nil {
					tt.handler(w, r)
					return
				}
				if tt.rawBody != "" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(tt.rawBody))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"signedURL":%q}`, tt.signedPath)
			}))
			defer server.Close()

			_, err := client.SignRead(context.Background(), objectPath)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, ErrInvalidSignerContract) {
				t.Fatalf("expected ErrInvalidSignerContract, got: %v", err)
			}
		})
	}
}

func TestSignReadServiceUnavailable(t *testing.T) {
	objectPath := "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4"

	for _, status := range []int{
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
		http.StatusTooManyRequests,
	} {
		t.Run(fmt.Sprintf("HTTP %d", status), func(t *testing.T) {
			client, server := testPlaybackClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`service temporarily unavailable`))
			}))
			defer server.Close()

			_, err := client.SignRead(context.Background(), objectPath)
			if err == nil {
				t.Fatalf("expected error for HTTP %d, got nil", status)
			}
			if !errors.Is(err, ErrSignerUnavailable) {
				t.Fatalf("expected ErrSignerUnavailable for HTTP %d, got: %v", status, err)
			}
		})
	}

	t.Run("dial failure / closed server", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
		serverURL := server.URL
		server.Close()

		client, err := NewPlayback(Config{
			InternalBaseURL: serverURL + "/storage/v1",
			PlaybackOrigin:  "https://playback.example.test",
			ServiceToken:    "private-token",
			Timeout:         time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.SignRead(context.Background(), objectPath)
		if err == nil {
			t.Fatal("expected error for closed server, got nil")
		}
		if !errors.Is(err, ErrSignerUnavailable) {
			t.Fatalf("expected ErrSignerUnavailable for dial failure, got: %v", err)
		}
	})

	t.Run("context deadline timeout before headers", func(t *testing.T) {
		done := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case <-done:
			case <-r.Context().Done():
			}
		}))
		defer func() {
			close(done)
			server.CloseClientConnections()
			server.Close()
		}()

		client, err := NewPlayback(Config{
			InternalBaseURL: server.URL + "/storage/v1",
			PlaybackOrigin:  "https://playback.example.test",
			ServiceToken:    "private-token",
			Timeout:         time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err = client.SignRead(ctx, objectPath)
		if err == nil {
			t.Fatal("expected timeout error, got nil")
		}
		if !errors.Is(err, ErrSignerUnavailable) {
			t.Fatalf("expected ErrSignerUnavailable for timeout, got: %v", err)
		}
	})

	t.Run("flushes 200 headers then stalls body until context timeout", func(t *testing.T) {
		done := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			select {
			case <-done:
			case <-r.Context().Done():
			}
		}))
		defer func() {
			close(done)
			server.CloseClientConnections()
			server.Close()
		}()

		client, err := NewPlayback(Config{
			InternalBaseURL: server.URL + "/storage/v1",
			PlaybackOrigin:  "https://playback.example.test",
			ServiceToken:    "private-token",
			Timeout:         time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		_, err = client.SignRead(ctx, objectPath)
		if err == nil {
			t.Fatal("expected timeout error for stalled response body, got nil")
		}
		if !errors.Is(err, ErrSignerUnavailable) {
			t.Fatalf("expected ErrSignerUnavailable for stalled body timeout, got: %v", err)
		}
	})

	t.Run("flushes 200 headers then stalls body with context.Background and Config.Timeout", func(t *testing.T) {
		done := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			select {
			case <-done:
			case <-r.Context().Done():
			}
		}))
		defer func() {
			close(done)
			server.CloseClientConnections()
			server.Close()
		}()

		client, err := NewPlayback(Config{
			InternalBaseURL: server.URL + "/storage/v1",
			PlaybackOrigin:  "https://playback.example.test",
			ServiceToken:    "private-token",
			Timeout:         20 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = client.SignRead(context.Background(), objectPath)
		if err == nil {
			t.Fatal("expected timeout error for stalled response body with client timeout, got nil")
		}
		if !errors.Is(err, ErrSignerUnavailable) {
			t.Fatalf("expected ErrSignerUnavailable for client timeout during body read, got: %v", err)
		}
	})
}

func TestSignReadSecretsNotLeakedInErrors(t *testing.T) {
	objectPath := "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4"
	secretToken := "very-secret-bearer-token-12345"
	secretBody := "storage internal stack trace with db password and raw token"

	client, server := testPlaybackClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(secretBody))
	}))
	defer server.Close()

	_, err := client.SignRead(context.Background(), objectPath)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	errStr := err.Error()
	if strings.Contains(errStr, secretToken) {
		t.Fatalf("error leaked secret token: %s", errStr)
	}
	if strings.Contains(errStr, secretBody) {
		t.Fatalf("error leaked response body: %s", errStr)
	}
	if strings.Contains(errStr, server.URL) {
		t.Fatalf("error leaked internal server URL: %s", errStr)
	}
}

func TestNewPlaybackConstructorAndUploadDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	defer server.Close()

	// 1. NewPlayback works with PlaybackOrigin and empty PublicOrigin (uploads disabled)
	client, err := NewPlayback(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PlaybackOrigin:  "https://playback.example.test",
		ServiceToken:    "secret-token",
		Timeout:         time.Second,
	})
	if err != nil {
		t.Fatalf("NewPlayback failed: %v", err)
	}

	// 2. PlaybackClient implements SignRead and has no SignUpload/Open/Delete methods
	type signReadOnly interface {
		SignRead(ctx context.Context, objectPath string) (ReadGrant, error)
	}
	var _ signReadOnly = client

	// 3. NewPlayback must not fall back to PublicOrigin when PlaybackOrigin is empty
	_, err = NewPlayback(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PublicOrigin:    "https://upload.example.test",
		PlaybackOrigin:  "",
		ServiceToken:    "secret-token",
	})
	if err == nil {
		t.Fatal("NewPlayback must reject empty PlaybackOrigin even if PublicOrigin is provided")
	}

	// 4. Upload-only New must not implicitly enable SignRead
	uploadOnlyClient, err := New(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PublicOrigin:    "https://upload.example.test",
		PlaybackOrigin:  "",
		ServiceToken:    "secret-token",
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	_, err = uploadOnlyClient.SignRead(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err == nil {
		t.Fatal("upload-only client must reject SignRead calls")
	}
	if !errors.Is(err, ErrInvalidSignerContract) {
		t.Fatalf("expected ErrInvalidSignerContract for unconfigured playback, got: %v", err)
	}

	// 5. Dual-configured New allows both SignUpload and SignRead
	dualClient, err := New(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PublicOrigin:    "https://upload.example.test",
		PlaybackOrigin:  "https://playback.example.test",
		ServiceToken:    "secret-token",
	})
	if err != nil {
		t.Fatalf("dual New failed: %v", err)
	}
	if dualClient.public == nil || dualClient.playback == nil {
		t.Fatal("dual client must configure both origins")
	}

	// 6. Strict pure HTTPS origin rejection (fragments, queries, empty hostname, path, userinfo, invalid ports)
	for _, invalidOrigin := range []string{
		"http://insecure.example.test",
		"https://host.example.test/path",
		"https://user:pass@host.example.test",
		"https://host.example.test#fragment",
		"https://host.example.test#",
		"https://host.example.test?query=1",
		"https://host.example.test?",
		"https://:8443",
		"https://host.example.test:0",
		"https://host.example.test:65536",
		"https://host.example.test:",
		"https://host.example.test:-1",
		"https://host.example.test:abc",
		"https://host.example.test:080",
		"",
	} {
		_, err := NewPlayback(Config{
			InternalBaseURL: server.URL + "/storage/v1",
			PlaybackOrigin:  invalidOrigin,
			ServiceToken:    "secret-token",
		})
		if err == nil {
			t.Fatalf("NewPlayback should reject origin %q", invalidOrigin)
		}
	}

	// 6b. Valid origins with no port or explicit valid port must be accepted
	for _, validOrigin := range []string{
		"https://playback.example.test",
		"https://playback.example.test:443",
		"https://playback.example.test:8443",
		"https://playback.example.test:1",
		"https://playback.example.test:65535",
	} {
		pClient, err := NewPlayback(Config{
			InternalBaseURL: server.URL + "/storage/v1",
			PlaybackOrigin:  validOrigin,
			ServiceToken:    "secret-token",
		})
		if err != nil || pClient == nil {
			t.Fatalf("NewPlayback should accept valid origin %q, got err: %v", validOrigin, err)
		}
	}

	// 7. New(Config) still requires PublicOrigin (upload contract preserved)
	_, err = New(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PublicOrigin:    "",
		ServiceToken:    "secret-token",
	})
	if err == nil {
		t.Fatal("New(Config) with empty PublicOrigin must be rejected")
	}
}

func TestRejectsInvalidOwnedPathAndDoesNotFollowRedirects(t *testing.T) {
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://attacker.test/steal", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	if _, err := client.Open(context.Background(), "../other.mp4"); err == nil {
		t.Fatal("accepted invalid object path")
	}
	_, err := client.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err == nil {
		t.Fatal("expected redirect response error")
	}
}

func TestStorageRequestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := New(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PublicOrigin:    "https://media.example.test",
		ServiceToken:    "private-storage-token",
		Timeout:         20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err == nil {
		t.Fatal("expected timeout")
	}
	if strings.Contains(err.Error(), server.URL) {
		t.Fatalf("error disclosed internal URL: %v", err)
	}
}

func TestOpenStreamHonorsCallerCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	client, err := New(Config{
		InternalBaseURL: server.URL + "/storage/v1",
		PublicOrigin:    "https://media.example.test",
		ServiceToken:    "private-storage-token",
		Timeout:         time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	object, err := client.Open(ctx, "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	defer func() {
		if err := object.Body.Close(); err != nil {
			t.Errorf("close object body: %v", err)
		}
	}()
	if _, err := io.ReadAll(object.Body); err == nil {
		t.Fatal("expected canceled stream read")
	}
}

func TestStorageClientTransportProxyExplicitlyDisabled(t *testing.T) {
	client, err := New(Config{
		InternalBaseURL: "http://supabase-storage:5000/storage/v1",
		PublicOrigin:    "https://media.example.test",
		ServiceToken:    "private-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.http == nil || client.http.Transport == nil {
		t.Fatal("client.http.Transport must not be nil (would fall back to DefaultTransport)")
	}
	if client.http.Transport == http.DefaultTransport {
		t.Fatal("client.http.Transport must not be global DefaultTransport")
	}
	tr, ok := client.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", client.http.Transport)
	}
	if tr.Proxy != nil {
		req, _ := http.NewRequest("GET", "http://supabase-storage:5000/storage/v1", nil)
		proxyURL, err := tr.Proxy(req)
		if err != nil || proxyURL != nil {
			t.Fatalf("tr.Proxy must not resolve proxy for internal origin; got %v, %v", proxyURL, err)
		}
	}
}

func TestStorageClientIgnoresEnvironmentProxySubprocess(t *testing.T) {
	if os.Getenv("TEST_STORAGE_PROXY_SUBPROCESS") == "1" {
		var proxyRequests int32
		dummyProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			atomic.AddInt32(&proxyRequests, 1)
			w.WriteHeader(http.StatusBadGateway)
		}))
		defer dummyProxy.Close()

		t.Setenv("HTTP_PROXY", dummyProxy.URL)
		t.Setenv("http_proxy", dummyProxy.URL)
		t.Setenv("ALL_PROXY", dummyProxy.URL)
		t.Setenv("all_proxy", dummyProxy.URL)
		t.Setenv("NO_PROXY", "")
		t.Setenv("no_proxy", "")

		client, err := New(Config{
			InternalBaseURL: "http://supabase-storage.internal:5000/storage/v1",
			PublicOrigin:    "https://media.example.test",
			ServiceToken:    "synthetic-secret-token",
			Timeout:         200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New client error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = client.Delete(ctx, "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")

		if atomic.LoadInt32(&proxyRequests) > 0 {
			t.Fatalf("internal Storage request leaked to environment proxy; proxy received %d requests", proxyRequests)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestStorageClientIgnoresEnvironmentProxySubprocess$", "-test.v")
	cmd.Env = append(os.Environ(), "TEST_STORAGE_PROXY_SUBPROCESS=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\noutput:\n%s", err, string(out))
	}
}

func TestSignReadObjectIdentityClaimConsistency(t *testing.T) {
	objectPath := "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4"

	// 1. Missing "url" claim in signed token must fail with ErrInvalidSignerContract
	missingURLToken := jwtWithClaims(time.Now().Add(120*time.Second).Unix(), "")
	clientMissing, serverMissing := testPlaybackClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"signedURL":%q}`, "/object/sign/"+VideoBucketID+"/"+objectPath+"?token="+url.QueryEscape(missingURLToken))
	}))
	defer serverMissing.Close()

	if _, err := clientMissing.SignRead(context.Background(), objectPath); err == nil {
		t.Fatal("expected SignRead to fail when token lacks url claim")
	} else if !errors.Is(err, ErrInvalidSignerContract) {
		t.Fatalf("expected ErrInvalidSignerContract for missing url claim, got: %v", err)
	}

	// 2. Wrong object path in "url" claim must fail with ErrInvalidSignerContract
	wrongObjToken := jwtWithClaims(time.Now().Add(120*time.Second).Unix(), VideoBucketID+"/other-object.mp4")
	clientWrongObj, serverWrongObj := testPlaybackClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"signedURL":%q}`, "/object/sign/"+VideoBucketID+"/"+objectPath+"?token="+url.QueryEscape(wrongObjToken))
	}))
	defer serverWrongObj.Close()

	if _, err := clientWrongObj.SignRead(context.Background(), objectPath); err == nil {
		t.Fatal("expected SignRead to fail when token has wrong object path")
	} else if !errors.Is(err, ErrInvalidSignerContract) {
		t.Fatalf("expected ErrInvalidSignerContract for wrong object path, got: %v", err)
	}

	// 3. Wrong bucket in "url" claim must fail with ErrInvalidSignerContract
	wrongBucketToken := jwtWithClaims(time.Now().Add(120*time.Second).Unix(), "media-images/"+objectPath)
	clientWrongBucket, serverWrongBucket := testPlaybackClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"signedURL":%q}`, "/object/sign/"+VideoBucketID+"/"+objectPath+"?token="+url.QueryEscape(wrongBucketToken))
	}))
	defer serverWrongBucket.Close()

	if _, err := clientWrongBucket.SignRead(context.Background(), objectPath); err == nil {
		t.Fatal("expected SignRead to fail when token has wrong bucket")
	} else if !errors.Is(err, ErrInvalidSignerContract) {
		t.Fatalf("expected ErrInvalidSignerContract for wrong bucket, got: %v", err)
	}

	// 4. Exact canonical bucket and object path in "url" claim succeeds
	correctToken := jwtWithClaims(time.Now().Add(120*time.Second).Unix(), VideoBucketID+"/"+objectPath)
	clientCorrect, serverCorrect := testPlaybackClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"signedURL":%q}`, "/object/sign/"+VideoBucketID+"/"+objectPath+"?token="+url.QueryEscape(correctToken))
	}))
	defer serverCorrect.Close()

	grant, err := clientCorrect.SignRead(context.Background(), objectPath)
	if err != nil {
		t.Fatalf("expected SignRead to succeed with matching object identity claim: %v", err)
	}
	if grant.SignedURL() == "" {
		t.Fatal("expected non-empty signed URL")
	}
}
