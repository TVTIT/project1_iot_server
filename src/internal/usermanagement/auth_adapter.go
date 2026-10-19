// Package usermanagement provides user account and gateway membership management.
package usermanagement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	defaultAuthAdminTimeout      = 10 * time.Second
	defaultAuthAdminMaxBodyBytes = 64 * 1024 // 64 KiB
)

// SupabaseAuthAdapterConfig holds configuration for the Supabase Auth Admin client.
type SupabaseAuthAdapterConfig struct {
	BaseURL        string
	ServiceRoleKey string
	Timeout        time.Duration
	MaxBodyBytes   int64
	HTTPClient     *http.Client
}

// SupabaseAuthAdapter implements AuthAdminClient against Supabase Auth / Envoy API Gateway.
type SupabaseAuthAdapter struct {
	endpoint     string
	apiKey       string
	authHeader   string
	client       *http.Client
	maxBodyBytes int64
}

// NewSupabaseAuthAdapter creates a new SupabaseAuthAdapter with the given configuration.
func NewSupabaseAuthAdapter(cfg SupabaseAuthAdapterConfig) (*SupabaseAuthAdapter, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("BaseURL is required")
	}
	parsedURL, err := url.Parse(cfg.BaseURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return nil, errors.New("BaseURL must be a valid HTTP or HTTPS URL")
	}

	key := strings.TrimSpace(cfg.ServiceRoleKey)
	if key == "" {
		return nil, errors.New("ServiceRoleKey is required")
	}
	rawKey := strings.TrimPrefix(key, "Bearer ")

	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	var endpoint string
	if strings.HasSuffix(baseURL, "/auth/v1") {
		endpoint = baseURL + "/admin/users"
	} else {
		endpoint = baseURL + "/auth/v1/admin/users"
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultAuthAdminTimeout
	}

	maxBodyBytes := cfg.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = defaultAuthAdminMaxBodyBytes
	}

	var httpClient *http.Client
	if cfg.HTTPClient != nil {
		copiedClient := *cfg.HTTPClient
		copiedClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return errors.New("redirects prohibited")
		}
		if copiedClient.Timeout <= 0 {
			copiedClient.Timeout = timeout
		}
		httpClient = &copiedClient
	} else {
		httpClient = &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return errors.New("redirects prohibited")
			},
		}
	}

	return &SupabaseAuthAdapter{
		endpoint:     endpoint,
		apiKey:       rawKey,
		authHeader:   "Bearer " + rawKey,
		client:       httpClient,
		maxBodyBytes: maxBodyBytes,
	}, nil
}

type authAdminCreateUserPayload struct {
	Email        string            `json:"email"`
	Password     string            `json:"password"`
	EmailConfirm bool              `json:"email_confirm"`
	UserMetadata map[string]string `json:"user_metadata"`
}

// CreateUser sends an administrative user creation request to Supabase GoTrue via Envoy.
func (a *SupabaseAuthAdapter) CreateUser(ctx context.Context, input AuthAdminCreateUserInput) (AuthAdminUser, error) {
	if err := ctx.Err(); err != nil {
		return AuthAdminUser{}, err
	}

	payload := authAdminCreateUserPayload{
		Email:        input.Email,
		Password:     input.Password,
		EmailConfirm: true,
		UserMetadata: map[string]string{
			"full_name": input.FullName,
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return AuthAdminUser{}, fmt.Errorf("%w: failed to marshal create user request", ErrInvalidInput)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return AuthAdminUser{}, fmt.Errorf("%w: failed to construct request", ErrAuthUnavailable)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", a.authHeader)
	req.Header.Set("apikey", a.apiKey)

	resp, err := a.client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return AuthAdminUser{}, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return AuthAdminUser{}, context.DeadlineExceeded
		}
		return AuthAdminUser{}, fmt.Errorf("%w: transport failure", ErrAuthUnavailable)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		limitReader := io.LimitReader(resp.Body, a.maxBodyBytes+1)
		respBytes, err := io.ReadAll(limitReader)
		if err != nil {
			return AuthAdminUser{}, fmt.Errorf("%w: failed to read response", ErrAuthInvalidResponse)
		}
		if int64(len(respBytes)) > a.maxBodyBytes {
			return AuthAdminUser{}, fmt.Errorf("%w: response exceeds maximum body size", ErrAuthInvalidResponse)
		}

		var authResp struct {
			ID        string `json:"id"`
			Email     string `json:"email"`
			CreatedAt string `json:"created_at"`
		}
		if err := json.Unmarshal(respBytes, &authResp); err != nil {
			return AuthAdminUser{}, fmt.Errorf("%w: malformed JSON response", ErrAuthInvalidResponse)
		}

		parsedUUID, err := uuid.Parse(authResp.ID)
		if err != nil || parsedUUID == uuid.Nil {
			return AuthAdminUser{}, fmt.Errorf("%w: invalid user UUID in response", ErrAuthInvalidResponse)
		}

		var createdAt time.Time
		if authResp.CreatedAt != "" {
			if parsedTime, err := time.Parse(time.RFC3339Nano, authResp.CreatedAt); err == nil {
				createdAt = parsedTime.UTC()
			}
		}
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}

		return AuthAdminUser{
			ID:        parsedUUID,
			Email:     authResp.Email,
			CreatedAt: createdAt,
		}, nil
	}

	// Non-200/201 response classification
	errReader := io.LimitReader(resp.Body, a.maxBodyBytes)
	errBytes, _ := io.ReadAll(errReader)

	var errObj struct {
		Code      any    `json:"code"`
		ErrorCode string `json:"error_code"`
		Msg       string `json:"msg"`
		Message   string `json:"message"`
		Error     string `json:"error"`
		ErrorDesc string `json:"error_description"`
	}
	_ = json.Unmarshal(errBytes, &errObj)

	combinedMsg := strings.ToLower(strings.Join([]string{
		errObj.ErrorCode,
		errObj.Msg,
		errObj.Message,
		errObj.Error,
		errObj.ErrorDesc,
	}, " "))

	if resp.StatusCode == http.StatusConflict ||
		strings.Contains(combinedMsg, "already been registered") ||
		strings.Contains(combinedMsg, "already registered") ||
		strings.Contains(combinedMsg, "already exists") ||
		strings.Contains(combinedMsg, "email_exists") ||
		strings.Contains(combinedMsg, "duplicate") {
		return AuthAdminUser{}, ErrUserAlreadyExists
	}

	if strings.Contains(combinedMsg, "password should be") ||
		strings.Contains(combinedMsg, "weak_password") ||
		strings.Contains(combinedMsg, "password is too short") ||
		(strings.Contains(combinedMsg, "password") && (strings.Contains(combinedMsg, "characters") || strings.Contains(combinedMsg, "complexity") || strings.Contains(combinedMsg, "least"))) {
		return AuthAdminUser{}, fmt.Errorf("%w: password does not meet auth policy", ErrInvalidInput)
	}

	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity {
		return AuthAdminUser{}, fmt.Errorf("%w: invalid request to auth service", ErrInvalidInput)
	}

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return AuthAdminUser{}, fmt.Errorf("%w: auth administrative credential rejected (status %d)", ErrAuthUnavailable, resp.StatusCode)
	}

	return AuthAdminUser{}, fmt.Errorf("%w: auth service returned status %d", ErrAuthUnavailable, resp.StatusCode)
}
