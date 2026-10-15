package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/mqttcredential"
)

// CredentialManager is the existing service use-case boundary, not a broker API.
type CredentialManager interface {
	Metadata(context.Context, uuid.UUID, string) (mqttcredential.Metadata, error)
	Provision(context.Context, uuid.UUID, mqttcredential.MutationInput) (mqttcredential.ProvisionResponse, error)
	Rotate(context.Context, uuid.UUID, mqttcredential.MutationInput) (mqttcredential.ProvisionResponse, error)
	Revoke(context.Context, uuid.UUID, mqttcredential.MutationInput) (mqttcredential.MutationResult, error)
}

// credentialResetter is a narrow internal capability for manual supervised reset.
type credentialResetter interface {
	Reset(context.Context, uuid.UUID, mqttcredential.ResetInput) (mqttcredential.ProvisionResponse, error)
}

// CredentialStartupReadiness proves the completed startup barrier only. It does
// not introduce a continuous broker health or post-OPEN database-loss policy.
type CredentialStartupReadiness interface{ Ready() bool }

// Runs before authentication so sensitive errors and unsupported methods cannot
// be cached either. Route authorization still belongs exclusively to groups.admin.
func credentialCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/admin/gateways/") && strings.Contains(c.Request.URL.Path, "/mqtt-credential") {
			c.Header("Cache-Control", "no-store")
			c.Header("Pragma", "no-cache")
		}
		c.Next()
	}
}

func registerCredentialRoutes(admin *gin.RouterGroup, deps RouterDependencies) {
	path := "/gateways/:gateway_id/mqtt-credential"
	admin.GET(path, credentialHandler(deps, ""))
	admin.POST(path, credentialHandler(deps, mqttcredential.ActionProvision))
	admin.POST(path+"/rotate", credentialHandler(deps, mqttcredential.ActionRotate))
	admin.DELETE(path, credentialHandler(deps, mqttcredential.ActionRevoke))
	admin.POST(path+"/reset", credentialResetHandler(deps))
}

func credentialHandler(deps RouterDependencies, action mqttcredential.Action) gin.HandlerFunc {
	return func(c *gin.Context) {
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil || principal.Role != "authenticated" {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if !deps.CredentialAPIEnabled {
			writeCredentialError(c, &mqttcredential.DomainError{Code: mqttcredential.CodeRuntimeDisabled})
			return
		}
		if auth.IsNilDependency(deps.CredentialManager) || auth.IsNilDependency(deps.CredentialStartupReadiness) || !deps.CredentialStartupReadiness.Ready() {
			writeCredentialError(c, &mqttcredential.DomainError{Code: mqttcredential.CodeServiceUnavailable})
			return
		}
		id := c.Param("gateway_id")
		key, status := parseCredentialRequest(c.Request, id, action != "", deps.AdminMaxBodyBytes)
		if status != 0 {
			if status == http.StatusRequestEntityTooLarge {
				httpapi.WriteError(c, status, "request_too_large", "request body too large")
			} else {
				httpapi.WriteError(c, status, "invalid_request", "invalid request")
			}
			return
		}
		manager := deps.CredentialManager
		ctx, cancel := context.WithTimeout(c.Request.Context(), deps.CredentialRequestTimeout)
		defer cancel()
		if action == "" {
			metadata, err := manager.Metadata(ctx, principal.UserID, id)
			if err != nil {
				writeCredentialError(c, err)
				return
			}
			c.JSON(http.StatusOK, metadata)
			return
		}
		input := mqttcredential.MutationInput{GatewayID: id, IdempotencyKey: key, Action: action}
		if action == mqttcredential.ActionRevoke {
			result, err := manager.Revoke(ctx, principal.UserID, input)
			if err != nil {
				writeCredentialError(c, err)
				return
			}
			writeCredentialMutation(c, result, http.StatusOK)
			return
		}
		var result mqttcredential.ProvisionResponse
		var err error
		if action == mqttcredential.ActionProvision {
			result, err = manager.Provision(ctx, principal.UserID, input)
		} else {
			result, err = manager.Rotate(ctx, principal.UserID, input)
		}
		if result.Secret != nil {
			defer result.Secret.ClearSecret()
		}
		if err != nil {
			writeCredentialError(c, err)
			return
		}
		status = http.StatusOK
		if action == mqttcredential.ActionProvision {
			status = http.StatusCreated
		}
		if result.Secret != nil {
			c.JSON(status, result.Secret)
			return
		}
		writeCredentialMutation(c, result.MutationResult, status)
	}
}

func writeCredentialMutation(c *gin.Context, result mqttcredential.MutationResult, status int) {
	// A failed replay is the same safe outcome, not a new successful mutation.
	if result.ReplayedStatus == mqttcredential.OperationFailed {
		code := mqttcredential.CodeInternalError
		if result.ErrorCode != nil {
			code = *result.ErrorCode
		}
		writeCredentialError(c, &mqttcredential.DomainError{Code: code})
		return
	}
	c.JSON(status, result)
}

func writeCredentialError(c *gin.Context, err error) {
	code, status := mqttcredential.CodeInternalError, http.StatusInternalServerError
	var domain *mqttcredential.DomainError
	if errors.As(err, &domain) && domain != nil {
		switch domain.Code {
		case mqttcredential.CodeInvalidRequest:
			status = http.StatusBadRequest
		case mqttcredential.CodeForbidden:
			status = http.StatusForbidden
		case mqttcredential.CodeNotFound:
			status = http.StatusNotFound
		case mqttcredential.CodeCredentialConflict, mqttcredential.CodeOperationInProgress, mqttcredential.CodeIdempotencyConflict:
			status = http.StatusConflict
		case mqttcredential.CodeRuntimeDisabled, mqttcredential.CodeRuntimeBusy, mqttcredential.CodeVerificationUnavailable, mqttcredential.CodeRecoveryRequired, mqttcredential.CodeFinalizationPending, mqttcredential.CodeServiceUnavailable:
			status = http.StatusServiceUnavailable
		case mqttcredential.CodeInternalError:
			status = http.StatusInternalServerError
		default:
			httpapi.WriteError(c, status, string(code), "credential request failed")
			return
		}
		code = domain.Code
	}
	// DomainError carries no committed intent identity. Never invent operation_id
	// from a request or from possibly partial/unconfirmed service output.
	httpapi.WriteError(c, status, string(code), "credential request failed")
}

func credentialResetHandler(deps RouterDependencies) gin.HandlerFunc {
	return func(c *gin.Context) {
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil || principal.Role != "authenticated" {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if !deps.CredentialAPIEnabled {
			writeCredentialError(c, &mqttcredential.DomainError{Code: mqttcredential.CodeRuntimeDisabled})
			return
		}
		if auth.IsNilDependency(deps.CredentialManager) || auth.IsNilDependency(deps.CredentialStartupReadiness) || !deps.CredentialStartupReadiness.Ready() {
			writeCredentialError(c, &mqttcredential.DomainError{Code: mqttcredential.CodeServiceUnavailable})
			return
		}
		resetter, ok := deps.CredentialManager.(credentialResetter)
		if !ok || auth.IsNilDependency(resetter) {
			writeCredentialError(c, &mqttcredential.DomainError{Code: mqttcredential.CodeServiceUnavailable})
			return
		}
		id := c.Param("gateway_id")
		key, reason, status := parseCredentialResetRequest(c.Request, id, deps.AdminMaxBodyBytes)
		if status != 0 {
			if status == http.StatusRequestEntityTooLarge {
				httpapi.WriteError(c, status, "request_too_large", "request body too large")
			} else {
				httpapi.WriteError(c, status, "invalid_request", "invalid request")
			}
			return
		}
		input := mqttcredential.ResetInput{
			GatewayID:      id,
			IdempotencyKey: key,
			Reason:         reason,
		}
		if err := mqttcredential.ValidateResetInput(principal.UserID, input); err != nil {
			writeCredentialError(c, err)
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), deps.CredentialRequestTimeout)
		defer cancel()
		result, err := resetter.Reset(ctx, principal.UserID, input)
		if result.Secret != nil {
			defer result.Secret.ClearSecret()
		}
		if err != nil {
			writeCredentialError(c, err)
			return
		}
		if result.Secret != nil {
			c.JSON(http.StatusOK, result.Secret)
			return
		}
		writeCredentialMutation(c, result.MutationResult, http.StatusOK)
	}
}

func parseCredentialResetRequest(r *http.Request, id string, limit int64) (uuid.UUID, mqttcredential.ResetReason, int) {
	if gateway.ValidateGatewayID(id) != nil || r.URL.RawQuery != "" || r.URL.ForceQuery {
		return uuid.Nil, "", http.StatusBadRequest
	}
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 {
		return uuid.Nil, "", http.StatusBadRequest
	}
	key, err := uuid.Parse(values[0])
	if err != nil || key == uuid.Nil {
		return uuid.Nil, "", http.StatusBadRequest
	}
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return uuid.Nil, "", http.StatusUnsupportedMediaType
	}
	for k, v := range params {
		if k != "charset" || !strings.EqualFold(v, "utf-8") {
			return uuid.Nil, "", http.StatusUnsupportedMediaType
		}
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if int64(len(data)) > limit {
		return uuid.Nil, "", http.StatusRequestEntityTooLarge
	}
	if err != nil || len(data) == 0 || !validJSONUnicode(data) {
		return uuid.Nil, "", http.StatusBadRequest
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return uuid.Nil, "", http.StatusBadRequest
	}
	seen := map[string]bool{}
	var reasonVal string
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return uuid.Nil, "", http.StatusBadRequest
		}
		fieldName, ok := token.(string)
		if !ok || seen[fieldName] {
			return uuid.Nil, "", http.StatusBadRequest
		}
		seen[fieldName] = true
		if fieldName != "reason" {
			return uuid.Nil, "", http.StatusBadRequest
		}
		var raw json.RawMessage
		if dec.Decode(&raw) != nil {
			return uuid.Nil, "", http.StatusBadRequest
		}
		if bytes.Equal(raw, []byte("null")) {
			return uuid.Nil, "", http.StatusBadRequest
		}
		if json.Unmarshal(raw, &reasonVal) != nil {
			return uuid.Nil, "", http.StatusBadRequest
		}
	}
	if token, err = dec.Token(); err != nil || token != json.Delim('}') {
		return uuid.Nil, "", http.StatusBadRequest
	}
	if _, err = dec.Token(); err != io.EOF {
		return uuid.Nil, "", http.StatusBadRequest
	}
	if !seen["reason"] {
		return uuid.Nil, "", http.StatusBadRequest
	}
	reason := mqttcredential.ResetReason(reasonVal)
	if reason != mqttcredential.ResetReasonRecoveryAfterUnknown && reason != mqttcredential.ResetReasonLostSecretRecovery {
		return uuid.Nil, "", http.StatusBadRequest
	}
	return key, reason, 0
}
