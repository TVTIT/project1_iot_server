// Package mediastorage contains the backend's narrowly scoped client for the
// private Stage 8 video bucket in Supabase Storage.
package mediastorage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// VideoBucketID is the fixed private bucket used by managed Stage 8 videos.
	VideoBucketID      = "media-video"
	maxControlBody     = 16 << 10
	defaultHTTPTimeout = 30 * time.Second
	maxHTTPTimeout     = 5 * time.Minute
	maxGrantHorizon    = 24 * time.Hour
)

// ErrNotFound indicates that the requested object does not exist in storage.
var (
	ErrNotFound  = errors.New("storage object not found")
	objectPathRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.mp4$`)
)

// Config provides connection settings, origins, and timeout configuration for the Storage client.
type Config struct {
	// InternalBaseURL is the trusted Storage API base, normally ending in
	// /storage/v1. It is never used to construct a client-facing URL.
	InternalBaseURL string
	// PublicOrigin is the configured HTTPS origin used by Gateway PUT requests.
	PublicOrigin string
	ServiceToken string `json:"-"`
	Timeout      time.Duration
}

func (Config) String() string { return "mediastorage.Config{redacted}" }

// GoString returns a redacted string representation of Config.
func (Config) GoString() string { return "mediastorage.Config{redacted}" }

// Client manages interactions with Supabase Storage for object signing, streaming, and deletion.
type Client struct {
	internal *url.URL
	public   *url.URL
	token    string
	http     *http.Client
}

func (*Client) String() string { return "mediastorage.Client{redacted}" }

// GoString returns a redacted string representation of Client.
func (*Client) GoString() string { return "mediastorage.Client{redacted}" }

// UploadGrant holds the signed upload URL and expiry for a granted media upload.
type UploadGrant struct {
	url       string
	ExpiresAt time.Time
}

// SignedURL returns the ephemeral upload URL. It is deliberately not an
// exported field, so fmt and JSON struct formatting do not reveal its token.
func (g UploadGrant) SignedURL() string { return g.url }

func (UploadGrant) String() string { return "mediastorage.UploadGrant{redacted}" }

// GoString returns a redacted string representation of UploadGrant.
func (UploadGrant) GoString() string { return "mediastorage.UploadGrant{redacted}" }

// Object represents an authenticated media object opened from storage for verification reading.
type Object struct {
	Body          io.ReadCloser
	ContentType   string
	ContentLength int64
}

// New constructs and validates a new Supabase Storage client with hardened transports.
func New(cfg Config) (*Client, error) {
	internal, err := url.Parse(cfg.InternalBaseURL)
	if err != nil || (internal.Scheme != "http" && internal.Scheme != "https") ||
		internal.Host == "" || internal.User != nil || internal.RawQuery != "" ||
		internal.Fragment != "" {
		return nil, errors.New("invalid internal Storage API base URL")
	}
	public, err := url.Parse(cfg.PublicOrigin)
	if err != nil || public.Scheme != "https" || public.Host == "" ||
		public.User != nil || public.Path != "" || public.RawPath != "" ||
		public.RawQuery != "" || public.Fragment != "" {
		return nil, errors.New("invalid public Storage origin")
	}
	if strings.TrimSpace(cfg.ServiceToken) == "" {
		return nil, errors.New("storage service token is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	if timeout > maxHTTPTimeout {
		return nil, errors.New("storage HTTP timeout exceeds the maximum")
	}
	return &Client{
		internal: internal,
		public:   public,
		token:    cfg.ServiceToken,
		http: &http.Client{
			Timeout:   timeout,
			Transport: newPrivateStorageTransport(),
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func newPrivateStorageTransport() *http.Transport {
	if def, ok := http.DefaultTransport.(*http.Transport); ok {
		tr := def.Clone()
		tr.Proxy = nil
		return tr
	}
	return &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// SignUpload requests an ephemeral upload URL from the storage service for the given object path.
func (c *Client) SignUpload(ctx context.Context, objectPath string) (UploadGrant, error) {
	if !validObjectPath(objectPath) {
		return UploadGrant{}, errors.New("invalid Storage object path")
	}
	path := "/object/upload/sign/" + VideoBucketID + "/" + objectPath
	req, err := c.request(ctx, http.MethodPost, path, nil)
	if err != nil {
		return UploadGrant{}, err
	}
	req.Header.Set("x-upsert", "false")
	req.Header.Set("Content-Type", "video/mp4")
	resp, err := c.http.Do(req)
	if err != nil {
		return UploadGrant{}, errors.New("storage signing request failed")
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			_, _ = readControlBody(resp.Body)
		}
		return UploadGrant{}, fmt.Errorf("storage signing returned HTTP %d", resp.StatusCode)
	}
	body, err := readControlBody(resp.Body)
	if err != nil {
		return UploadGrant{}, errors.New("invalid Storage signing response")
	}
	var result struct {
		URL   string `json:"url"`
		Token string `json:"token"`
	}
	if json.Unmarshal(body, &result) != nil || result.URL == "" || result.Token == "" {
		return UploadGrant{}, errors.New("invalid Storage signing response")
	}
	expiresAt, err := tokenExpiry(result.Token)
	now := time.Now()
	if err != nil || !expiresAt.After(now) || expiresAt.After(now.Add(maxGrantHorizon)) {
		return UploadGrant{}, errors.New("storage returned an invalid or expired upload token")
	}
	publicURL, err := c.publicSignedURL(result.URL, result.Token, objectPath)
	if err != nil {
		return UploadGrant{}, errors.New("storage returned an invalid upload URL")
	}
	return UploadGrant{url: publicURL, ExpiresAt: expiresAt}, nil
}

// Open streams an authenticated object from storage, transferring Body ownership to the caller on success.
func (c *Client) Open(ctx context.Context, objectPath string) (*Object, error) {
	if !validObjectPath(objectPath) {
		return nil, errors.New("invalid Storage object path")
	}
	req, err := c.request(ctx, http.MethodGet, "/object/authenticated/"+VideoBucketID+"/"+objectPath, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, errors.New("storage read request failed")
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest {
		body, readErr := readControlBody(resp.Body)
		_ = resp.Body.Close()
		if readErr == nil && isObjectNotFound(resp.StatusCode, body) {
			return nil, ErrNotFound
		}
		return nil, errors.New("storage read returned an unclassified HTTP 404")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("storage read returned HTTP %d", resp.StatusCode)
	}
	length := int64(-1)
	if resp.ContentLength >= 0 {
		length = resp.ContentLength
	}
	return &Object{Body: resp.Body, ContentType: resp.Header.Get("Content-Type"), ContentLength: length}, nil
}

// Delete removes only the exact, server-owned video object path. A Storage 404
// is treated as already absent; all other failures remain errors.
func (c *Client) Delete(ctx context.Context, objectPath string) error {
	if !validObjectPath(objectPath) {
		return errors.New("invalid Storage object path")
	}
	req, err := c.request(ctx, http.MethodDelete, "/object/"+VideoBucketID+"/"+objectPath, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New("storage delete request failed")
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusBadRequest {
		body, readErr := readControlBody(resp.Body)
		if readErr == nil && isObjectNotFound(resp.StatusCode, body) {
			return nil
		}
		return errors.New("storage delete returned an unclassified HTTP 404")
	}
	return fmt.Errorf("storage delete returned HTTP %d", resp.StatusCode)
}

func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	base := *c.internal
	base.Path = strings.TrimRight(base.Path, "/") + path
	base.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, method, base.String(), body)
	if err != nil {
		return nil, errors.New("invalid Storage request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	return req, nil
}

func (c *Client) publicSignedURL(signedURL, token, objectPath string) (string, error) {
	parsed, err := url.Parse(signedURL)
	expectedPath := "/object/upload/sign/" + VideoBucketID + "/" + objectPath
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.User != nil ||
		parsed.Fragment != "" || parsed.Path != expectedPath || parsed.RawPath != "" {
		return "", errors.New("invalid signed URL")
	}
	values, err := url.ParseQuery(parsed.RawQuery)
	if err != nil || len(values) != 1 || len(values["token"]) != 1 || values.Get("token") != token {
		return "", errors.New("signed URL token mismatch")
	}
	return c.public.String() + "/storage/v1" + parsed.Path + "?" + parsed.RawQuery, nil
}

func tokenExpiry(token string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return time.Time{}, errors.New("invalid token")
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return time.Time{}, errors.New("invalid token")
	}
	var headerFields map[string]json.RawMessage
	if json.Unmarshal(header, &headerFields) != nil || len(headerFields) == 0 {
		return time.Time{}, errors.New("invalid token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, errors.New("invalid token")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) == 0 {
		return time.Time{}, errors.New("invalid token")
	}
	var claims map[string]json.RawMessage
	if json.Unmarshal(payload, &claims) != nil {
		return time.Time{}, errors.New("invalid token")
	}
	rawExp, ok := claims["exp"]
	if !ok {
		return time.Time{}, errors.New("missing expiry")
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(string(rawExp)), 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}, errors.New("invalid expiry")
	}
	return time.Unix(seconds, 0), nil
}

func isObjectNotFound(status int, body []byte) bool {
	if status != http.StatusNotFound && status != http.StatusBadRequest {
		return false
	}
	var failure struct {
		StatusCode string `json:"statusCode"`
		Error      string `json:"error"`
		Message    string `json:"message"`
		Code       string `json:"code"`
	}
	return json.Unmarshal(body, &failure) == nil &&
		failure.StatusCode == "404" &&
		failure.Code == "NoSuchKey" &&
		failure.Error == "not_found" &&
		failure.Message == "Object not found"
}

func readControlBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, maxControlBody+1))
	if err != nil || len(body) > maxControlBody {
		return nil, errors.New("bounded response read failed")
	}
	return body, nil
}

func validObjectPath(path string) bool {
	return objectPathRE.MatchString(path)
}
