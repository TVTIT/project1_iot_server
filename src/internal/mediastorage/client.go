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
	ErrNotFound              = errors.New("storage object not found")
	ErrInvalidPath           = errors.New("invalid Storage object path")
	ErrSignerUnavailable     = errors.New("storage signer unavailable")
	ErrInvalidSignerContract = errors.New("invalid storage signer contract")
	objectPathRE             = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.mp4$`)
)

const (
	// DefaultReadGrantTTL is the fixed validity window requested from Storage for playback.
	DefaultReadGrantTTL = 120 * time.Second
	// ReadGrantClockTolerance is the maximum acceptable skew between the backend
	// clock and the Storage token's exp claim.
	ReadGrantClockTolerance = 15 * time.Second
)

// Config provides connection settings, origins, and timeout configuration for the Storage client.
type Config struct {
	// InternalBaseURL is the trusted Storage API base, normally ending in
	// /storage/v1. It is never used to construct a client-facing URL.
	InternalBaseURL string
	// PublicOrigin is the configured HTTPS origin used by Gateway PUT requests.
	PublicOrigin string
	// PlaybackOrigin is the configured HTTPS origin used by Playback GET requests.
	PlaybackOrigin string
	ServiceToken   string `json:"-"`
	Timeout        time.Duration
}

func (Config) String() string { return "mediastorage.Config{redacted}" }

// GoString returns a redacted string representation of Config.
func (Config) GoString() string { return "mediastorage.Config{redacted}" }

// Client manages interactions with Supabase Storage for object signing, streaming, and deletion.
type Client struct {
	internal *url.URL
	public   *url.URL
	playback *url.URL
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

// ReadGrant holds the signed playback URL and expiry for a granted media stream.
type ReadGrant struct {
	url       string
	ExpiresAt time.Time
}

// SignedURL returns the ephemeral playback URL. It is deliberately not an
// exported field, so fmt and JSON struct formatting do not reveal its token.
func (g ReadGrant) SignedURL() string { return g.url }

func (ReadGrant) String() string { return "mediastorage.ReadGrant{redacted}" }

// GoString returns a redacted string representation of ReadGrant.
func (ReadGrant) GoString() string { return "mediastorage.ReadGrant{redacted}" }

// Object represents an authenticated media object opened from storage for verification reading.
type Object struct {
	Body          io.ReadCloser
	ContentType   string
	ContentLength int64
}

func parseHTTPSOrigin(rawOrigin, kind string) (*url.URL, error) {
	if strings.ContainsAny(rawOrigin, "#?") || strings.TrimSpace(rawOrigin) != rawOrigin {
		return nil, fmt.Errorf("invalid %s Storage origin", kind)
	}
	parsed, err := url.Parse(rawOrigin)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid %s Storage origin", kind)
	}
	return parsed, nil
}

// ValidatePlaybackOrigin validates an HTTPS origin for media playback, ensuring strict scheme and port rules.
func ValidatePlaybackOrigin(rawOrigin string) (*url.URL, error) {
	if strings.ContainsAny(rawOrigin, "#?") || strings.TrimSpace(rawOrigin) != rawOrigin {
		return nil, errors.New("invalid playback HTTPS origin")
	}
	parsed, err := url.Parse(rawOrigin)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return nil, errors.New("invalid playback HTTPS origin")
	}

	// Validate explicitly specified port: must be numeric 1-65535, nonempty, no leading zeros
	if parsed.Host != parsed.Hostname() {
		portStr := parsed.Port()
		if portStr == "" {
			return nil, errors.New("invalid playback HTTPS origin port: empty port")
		}
		portNum, err := strconv.Atoi(portStr)
		if err != nil || portNum < 1 || portNum > 65535 || strconv.Itoa(portNum) != portStr {
			return nil, errors.New("invalid playback HTTPS origin port: must be 1-65535")
		}
	}

	return parsed, nil
}

func initBaseClient(cfg Config) (*Client, error) {
	internal, err := url.Parse(cfg.InternalBaseURL)
	if err != nil || (internal.Scheme != "http" && internal.Scheme != "https") ||
		internal.Host == "" || internal.User != nil || internal.RawQuery != "" ||
		internal.Fragment != "" {
		return nil, errors.New("invalid internal Storage API base URL")
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

// New constructs and validates a new Supabase Storage client with hardened transports.
func New(cfg Config) (*Client, error) {
	public, err := parseHTTPSOrigin(cfg.PublicOrigin, "public")
	if err != nil {
		return nil, err
	}
	var playback *url.URL
	if cfg.PlaybackOrigin != "" {
		p, err := parseHTTPSOrigin(cfg.PlaybackOrigin, "playback")
		if err != nil {
			return nil, err
		}
		playback = p
	}
	client, err := initBaseClient(cfg)
	if err != nil {
		return nil, err
	}
	client.public = public
	client.playback = playback
	return client, nil
}

// NewPlayback constructs and validates a read-only Supabase Storage client for playback URL signing.
// It requires a pure HTTPS playback origin and does not permit upload signing calls.
// NewPlayback constructs and validates a read-only Supabase Storage client for playback URL signing.
// It requires a pure HTTPS playback origin and returns a PlaybackClient restricted solely to SignRead.
func NewPlayback(cfg Config) (*PlaybackClient, error) {
	if cfg.PlaybackOrigin == "" {
		return nil, errors.New("invalid playback Storage origin")
	}
	playback, err := ValidatePlaybackOrigin(cfg.PlaybackOrigin)
	if err != nil {
		return nil, err
	}
	client, err := initBaseClient(cfg)
	if err != nil {
		return nil, err
	}
	client.playback = playback
	return &PlaybackClient{client: client}, nil
}

// PlaybackClient is a specialized Supabase Storage client whose only capability is SignRead.
type PlaybackClient struct {
	client *Client
}

// SignRead requests an ephemeral signed read URL from the storage service for playback of the given object path.
func (p *PlaybackClient) SignRead(ctx context.Context, objectPath string) (ReadGrant, error) {
	if p == nil || p.client == nil {
		return ReadGrant{}, errors.New("nil playback client")
	}
	return p.client.SignRead(ctx, objectPath)
}

// SignRead requests an ephemeral signed read URL from the storage service for playback of the given object path.
func (c *Client) SignRead(ctx context.Context, objectPath string) (ReadGrant, error) {
	if !validObjectPath(objectPath) {
		return ReadGrant{}, ErrInvalidPath
	}
	if c.playback == nil {
		return ReadGrant{}, fmt.Errorf("%w: playback signing is not configured on this client", ErrInvalidSignerContract)
	}

	path := "/object/sign/" + VideoBucketID + "/" + objectPath
	bodyReader := strings.NewReader(`{"expiresIn":120}`)
	req, err := c.request(ctx, http.MethodPost, path, bodyReader)
	if err != nil {
		return ReadGrant{}, fmt.Errorf("%w: invalid storage signing request", ErrInvalidSignerContract)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ReadGrant{}, fmt.Errorf("%w: storage signing request timed out or canceled: %w", ErrSignerUnavailable, ctx.Err())
		}
		return ReadGrant{}, fmt.Errorf("%w: storage signing request failed", ErrSignerUnavailable)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if (resp.StatusCode >= 500 && resp.StatusCode <= 599) || resp.StatusCode == http.StatusTooManyRequests {
		_, _ = readControlBody(resp.Body)
		return ReadGrant{}, fmt.Errorf("%w: storage service temporarily unavailable", ErrSignerUnavailable)
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = readControlBody(resp.Body)
		return ReadGrant{}, fmt.Errorf("%w: storage signing request rejected", ErrInvalidSignerContract)
	}

	body, err := readControlBody(resp.Body)
	if err != nil {
		if ctx.Err() != nil || isTimeoutError(err) {
			return ReadGrant{}, fmt.Errorf("%w: storage response read timed out or canceled", ErrSignerUnavailable)
		}
		return ReadGrant{}, fmt.Errorf("%w: bounded response read failed", ErrInvalidSignerContract)
	}

	var result struct {
		SignedURL string `json:"signedURL"`
	}
	if err := json.Unmarshal(body, &result); err != nil || result.SignedURL == "" {
		return ReadGrant{}, fmt.Errorf("%w: invalid storage signing response", ErrInvalidSignerContract)
	}

	publicURL, token, err := c.parsePlaybackSignedURL(result.SignedURL, objectPath)
	if err != nil {
		return ReadGrant{}, err
	}

	expiresAt, err := parsePlaybackTokenClaims(token, VideoBucketID, objectPath)
	if err != nil {
		return ReadGrant{}, err
	}

	now := time.Now().UTC()
	remaining := expiresAt.Sub(now)
	if remaining <= 0 {
		return ReadGrant{}, fmt.Errorf("%w: storage token is expired", ErrInvalidSignerContract)
	}
	if remaining < DefaultReadGrantTTL-ReadGrantClockTolerance {
		return ReadGrant{}, fmt.Errorf("%w: storage token expiry is materially too short", ErrInvalidSignerContract)
	}
	if remaining > DefaultReadGrantTTL+ReadGrantClockTolerance {
		return ReadGrant{}, fmt.Errorf("%w: storage token expiry is materially too long", ErrInvalidSignerContract)
	}

	return ReadGrant{
		url:       publicURL,
		ExpiresAt: expiresAt,
	}, nil
}

func (c *Client) parsePlaybackSignedURL(rawSignedURL, objectPath string) (publicURL, token string, err error) {
	expectedPrefix := "/object/sign/" + VideoBucketID + "/" + objectPath + "?token="
	if !strings.HasPrefix(rawSignedURL, expectedPrefix) {
		return "", "", fmt.Errorf("%w: invalid signed URL prefix or path", ErrInvalidSignerContract)
	}

	token = strings.TrimPrefix(rawSignedURL, expectedPrefix)
	parts := strings.Split(token, ".")
	if len(parts) != 3 || !isBase64URL(parts[0]) || !isBase64URL(parts[1]) || !isBase64URL(parts[2]) {
		return "", "", fmt.Errorf("%w: invalid token query format in signed URL", ErrInvalidSignerContract)
	}

	publicURL = c.playback.String() + "/storage/v1" + rawSignedURL
	return publicURL, token, nil
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

func isBase64URL(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
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
	if c.public == nil {
		return UploadGrant{}, errors.New("upload signing is not configured on this client")
	}
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
	return time.Unix(seconds, 0).UTC(), nil
}

func parsePlaybackTokenClaims(token, expectedBucket, expectedObjectPath string) (time.Time, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
		return time.Time{}, fmt.Errorf("%w: invalid token format", ErrInvalidSignerContract)
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid token header", ErrInvalidSignerContract)
	}
	var headerFields map[string]json.RawMessage
	if json.Unmarshal(header, &headerFields) != nil || len(headerFields) == 0 {
		return time.Time{}, fmt.Errorf("%w: invalid token header JSON", ErrInvalidSignerContract)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid token payload", ErrInvalidSignerContract)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) == 0 {
		return time.Time{}, fmt.Errorf("%w: invalid token signature", ErrInvalidSignerContract)
	}
	var claims map[string]json.RawMessage
	if json.Unmarshal(payload, &claims) != nil {
		return time.Time{}, fmt.Errorf("%w: invalid token claims JSON", ErrInvalidSignerContract)
	}

	// 1. Verify exact object identity claim: url must match expectedBucket + "/" + expectedObjectPath
	rawURL, ok := claims["url"]
	if !ok {
		return time.Time{}, fmt.Errorf("%w: missing url claim in signed token", ErrInvalidSignerContract)
	}
	var urlClaim string
	if err := json.Unmarshal(rawURL, &urlClaim); err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid url claim type", ErrInvalidSignerContract)
	}
	expectedURL := expectedBucket + "/" + expectedObjectPath
	if urlClaim != expectedURL {
		return time.Time{}, fmt.Errorf("%w: signed token url claim %q does not match expected %q", ErrInvalidSignerContract, urlClaim, expectedURL)
	}

	// 2. Verify expiry claim
	rawExp, ok := claims["exp"]
	if !ok {
		return time.Time{}, fmt.Errorf("%w: missing expiry claim", ErrInvalidSignerContract)
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(string(rawExp)), 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}, fmt.Errorf("%w: invalid expiry claim", ErrInvalidSignerContract)
	}
	return time.Unix(seconds, 0).UTC(), nil
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
	if err != nil {
		return nil, err
	}
	if len(body) > maxControlBody {
		return nil, errors.New("bounded response read failed")
	}
	return body, nil
}

func validObjectPath(path string) bool {
	return objectPathRE.MatchString(path)
}
