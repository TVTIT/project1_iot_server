package mediastorage

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
		fmt.Fprintf(w, `{"url":%q,"token":%q}`, signedPath, token)
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
			client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				fmt.Fprint(w, body)
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
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		fmt.Fprint(w, secret)
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
		io.WriteString(w, "data")
	}))
	defer server.Close()
	obj, err := client.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Body.Close()
	if obj.ContentType != "video/mp4" || obj.ContentLength != 4 {
		t.Fatalf("unexpected metadata: %#v", obj)
	}
	body, _ := io.ReadAll(obj.Body)
	if string(body) != "data" {
		t.Fatalf("unexpected body %q", body)
	}

	missing, missingServer := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"statusCode":"404","error":"not_found","message":"Object not found","code":"NoSuchKey"}`)
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
				fmt.Fprint(w, tc.body)
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
		client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, body)
		}))
		_, err := client.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
		server.Close()
		if err == nil || err == ErrNotFound {
			t.Fatalf("404 body %q misclassified as missing: %v", body, err)
		}
	}
}

func TestStorageReadMapsNativeNoSuchKeyBadRequest(t *testing.T) {
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"statusCode":"404","error":"not_found","message":"Object not found","code":"NoSuchKey"}`)
	}))
	defer server.Close()
	_, err := client.Open(context.Background(), "2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4")
	if err != ErrNotFound {
		t.Fatalf("expected native Storage NoSuchKey to map to ErrNotFound, got %v", err)
	}
}

func TestSensitiveValuesAreFmtAndJSONSafe(t *testing.T) {
	token := jwtWithExp(time.Now().Add(time.Minute).Unix())
	client, server := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"url":"/object/upload/sign/media-video/2fbc3f6f-6fb8-4b6e-8f2e-0c0e5e560bbb.mp4?token=%s","token":%q}`,
			token, token)
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer object.Body.Close()
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
		dummyProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&proxyRequests, 1)
			w.WriteHeader(http.StatusBadGateway)
		}))
		defer dummyProxy.Close()

		os.Setenv("HTTP_PROXY", dummyProxy.URL)
		os.Setenv("http_proxy", dummyProxy.URL)
		os.Setenv("ALL_PROXY", dummyProxy.URL)
		os.Setenv("all_proxy", dummyProxy.URL)
		os.Setenv("NO_PROXY", "")
		os.Setenv("no_proxy", "")

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
