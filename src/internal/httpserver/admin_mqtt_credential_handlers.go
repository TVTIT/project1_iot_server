package httpserver

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
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
