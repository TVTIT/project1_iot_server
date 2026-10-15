package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
)

// AdminGatewayInventoryLister lists the global Gateway inventory for a verified platform admin.
type AdminGatewayInventoryLister interface {
	ListAdminGatewayInventory(context.Context, uuid.UUID, gateway.AdminGatewayInventoryQuery) (gateway.AdminGatewayInventoryPage, error)
}

const adminGatewayInventoryLoggedKey = "admin_gateway_inventory_logged"

func isAdminGatewayInventoryRoute(path, method string) bool {
	if method != http.MethodGet {
		return false
	}
	p := strings.TrimSuffix(path, "/")
	return p == "/v1/admin/gateways"
}

// adminGatewayCacheMiddleware ensures no-store on admin gateway inventory endpoints.
func adminGatewayCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/v1/admin/gateways" || strings.TrimSuffix(c.Request.URL.Path, "/") == "/v1/admin/gateways" {
			c.Header("Cache-Control", "no-store")
			c.Header("Pragma", "no-cache")
		}
		c.Next()
	}
}

func emitAdminGatewayAudit(c *gin.Context, level slog.Level, msg string, actor, outcome string) {
	c.Set(adminGatewayInventoryLoggedKey, true)
	reqID := httpapi.RequestIDFrom(c)
	switch level {
	case slog.LevelInfo:
		slog.InfoContext(c.Request.Context(), msg,
			"actor", actor,
			"request_id", reqID,
			"action", "list_admin_gateways",
			"outcome", outcome,
		)
	case slog.LevelWarn:
		slog.WarnContext(c.Request.Context(), msg,
			"actor", actor,
			"request_id", reqID,
			"action", "list_admin_gateways",
			"outcome", outcome,
		)
	default:
		slog.ErrorContext(c.Request.Context(), msg,
			"actor", actor,
			"request_id", reqID,
			"action", "list_admin_gateways",
			"outcome", outcome,
		)
	}
}

// adminGatewayAuditMiddleware ensures safe structured audit logging for GET /v1/admin/gateways
// requests that are rejected before reaching or emitting from the business handler (e.g. 401, 403, 503, recovered panic 500).
func adminGatewayAuditMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !isAdminGatewayInventoryRoute(c.Request.URL.Path, c.Request.Method) {
			c.Next()
			return
		}

		defer func() {
			panicked := recover()
			if panicked != nil {
				// Re-panic so existing outer recovery remains authoritative.
				// Emit safe internal_error audit event before re-panicking without leaking panic value.
				defer panic(panicked)

				if !c.GetBool(adminGatewayInventoryLoggedKey) {
					actor := "anonymous"
					if p, ok := auth.PrincipalFrom(c); ok && p.UserID != uuid.Nil {
						actor = p.UserID.String()
					}
					emitAdminGatewayAudit(c, slog.LevelError, "admin gateway inventory failed", actor, "internal_error")
				}
				return
			}

			if c.GetBool(adminGatewayInventoryLoggedKey) {
				return
			}

			status := c.Writer.Status()
			outcome := "internal_error"
			switch status {
			case http.StatusUnauthorized:
				outcome = "unauthorized"
			case http.StatusForbidden:
				outcome = "forbidden"
			case http.StatusServiceUnavailable:
				outcome = "service_unavailable"
			case http.StatusBadRequest:
				outcome = "invalid_request"
			}

			actor := "anonymous"
			if p, ok := auth.PrincipalFrom(c); ok && p.UserID != uuid.Nil {
				actor = p.UserID.String()
			}

			level := slog.LevelWarn
			msg := "admin gateway inventory denied"
			if status >= http.StatusInternalServerError {
				level = slog.LevelError
				msg = "admin gateway inventory failed"
			}
			emitAdminGatewayAudit(c, level, msg, actor, outcome)
		}()

		c.Next()
	}
}

type adminGatewayInventoryDTO struct {
	GatewayID   string     `json:"gateway_id"`
	Name        string     `json:"name"`
	Description *string    `json:"description"`
	CreatedAt   *time.Time `json:"created_at"`
}

type adminGatewayInventoryResponse struct {
	Items      []adminGatewayInventoryDTO `json:"items"`
	NextCursor *string                    `json:"next_cursor"`
}

func listAdminGateways(lister AdminGatewayInventoryLister, defaultLimit, maxLimit int) gin.HandlerFunc {
	if auth.IsNilDependency(lister) || defaultLimit <= 0 || maxLimit < defaultLimit {
		panic("invalid admin gateway inventory handler dependencies")
	}

	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Header("Pragma", "no-cache")

		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory denied",
				"anonymous",
				"unauthorized",
			)
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}

		rawQuery := c.Request.URL.RawQuery
		if strings.ContainsRune(rawQuery, ';') {
			emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory rejected semicolon in query",
				p.UserID.String(),
				"invalid_request",
			)
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		queryParams, err := url.ParseQuery(rawQuery)
		if err != nil {
			emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory malformed query string",
				p.UserID.String(),
				"invalid_request",
			)
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		for key := range queryParams {
			if key != "limit" && key != "after" {
				emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory rejected unknown query",
					p.UserID.String(),
					"invalid_request",
				)
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
				return
			}
		}

		if len(queryParams["limit"]) > 1 || len(queryParams["after"]) > 1 {
			emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory rejected duplicate query",
				p.UserID.String(),
				"invalid_request",
			)
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
			return
		}

		limit := defaultLimit
		if limitVals, ok := queryParams["limit"]; ok && len(limitVals) == 1 {
			raw := limitVals[0]
			if raw == "" {
				emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory empty limit",
					p.UserID.String(),
					"invalid_request",
				)
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
				return
			}
			for _, r := range raw {
				if r < '0' || r > '9' {
					emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory non-digit limit",
						p.UserID.String(),
						"invalid_request",
					)
					httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
					return
				}
			}
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > maxLimit {
				emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory limit out of bounds",
					p.UserID.String(),
					"invalid_request",
				)
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
				return
			}
			limit = parsed
		}

		after := ""
		if afterVals, ok := queryParams["after"]; ok && len(afterVals) == 1 {
			raw := afterVals[0]
			if raw == "" || gateway.ValidateGatewayID(raw) != nil {
				emitAdminGatewayAudit(c, slog.LevelWarn, "admin gateway inventory invalid after cursor",
					p.UserID.String(),
					"invalid_request",
				)
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
				return
			}
			after = raw
		}

		page, err := lister.ListAdminGatewayInventory(c.Request.Context(), p.UserID, gateway.AdminGatewayInventoryQuery{
			Limit: limit,
			After: after,
		})
		if err != nil {
			status, code, message, outcome := http.StatusInternalServerError, "internal_error", "internal server error", "internal_error"
			switch {
			case errors.Is(err, gateway.ErrAdminForbidden):
				status, code, message, outcome = http.StatusForbidden, "forbidden", "access denied", "forbidden"
			case errors.Is(err, gateway.ErrInvalidInventoryQuery), errors.Is(err, gateway.ErrInvalidGatewayID), errors.Is(err, gateway.ErrInvalidUser):
				status, code, message, outcome = http.StatusBadRequest, "invalid_request", "invalid request", "invalid_request"
			case errors.Is(err, gateway.ErrAdminLookupFailed), errors.Is(err, gateway.ErrInventoryUnavailable), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				status, code, message, outcome = http.StatusServiceUnavailable, "service_unavailable", "service unavailable", "service_unavailable"
			}
			emitAdminGatewayAudit(c, slog.LevelError, "admin gateway inventory failed",
				p.UserID.String(),
				outcome,
			)
			httpapi.WriteError(c, status, code, message)
			return
		}

		response := adminGatewayInventoryResponse{
			Items:      make([]adminGatewayInventoryDTO, 0, len(page.Items)),
			NextCursor: page.NextCursor,
		}
		for _, item := range page.Items {
			var stamp *time.Time
			if item.CreatedAt != nil {
				utc := item.CreatedAt.UTC()
				stamp = &utc
			}
			response.Items = append(response.Items, adminGatewayInventoryDTO{
				GatewayID:   item.GatewayID,
				Name:        item.Name,
				Description: item.Description,
				CreatedAt:   stamp,
			})
		}
		c.JSON(http.StatusOK, response)

		emitAdminGatewayAudit(c, slog.LevelInfo, "admin gateway inventory listed",
			p.UserID.String(),
			"success",
		)
	}
}

// GatewayProvisioner exposes the atomic Gateway provisioning use case.
type GatewayProvisioner interface {
	ProvisionGateway(context.Context, uuid.UUID, gateway.ProvisionGatewayInput) (gateway.ProvisionGatewayResult, error)
}

func provisionGateway(provisioner GatewayProvisioner, limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		p, ok := auth.PrincipalFrom(c)
		if !ok || p.UserID == uuid.Nil {
			httpapi.WriteError(c, 401, "unauthorized", "authentication required")
			return
		}
		input, status := parseGatewayProvisioning(c.Request, limit)
		if status != 0 {
			code, message := "invalid_request", "invalid request"
			if status == 413 {
				code, message = "payload_too_large", "payload too large"
			}
			if status == 415 {
				code, message = "unsupported_media_type", "unsupported media type"
			}
			httpapi.WriteError(c, status, code, message)
			return
		}
		input.GatewayID = c.Param("gateway_id")
		result, err := provisioner.ProvisionGateway(c.Request.Context(), p.UserID, input)
		if err != nil {
			status, code, message := 500, "internal_error", "internal server error"
			switch {
			case errors.Is(err, gateway.ErrInvalidProvisioningInput), errors.Is(err, gateway.ErrInvalidGatewayID):
				status, code, message = 400, "invalid_request", "invalid request"
			case errors.Is(err, gateway.ErrProvisioningNotFound):
				status, code, message = 404, "not_found", "resource not found"
			case errors.Is(err, gateway.ErrProvisioningConflict):
				status, code, message = 409, "conflict", "resource conflict"
			case errors.Is(err, gateway.ErrProvisioningForbidden):
				status, code, message = 403, "forbidden", "access denied"
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), errors.Is(err, gateway.ErrProvisioningAdminLookup):
				status, code, message = 503, "service_unavailable", "service unavailable"
			}
			slog.ErrorContext(c.Request.Context(), "gateway provisioning failed", "request_id", httpapi.RequestIDFrom(c), "error_code", code)
			httpapi.WriteError(c, status, code, message)
			return
		}
		var stamp *time.Time
		if result.CreatedAt != nil {
			utc := result.CreatedAt.UTC()
			stamp = &utc
		}
		status = http.StatusOK
		if result.Created {
			status = http.StatusCreated
		}
		c.JSON(status, struct {
			GatewayID   string     `json:"gateway_id"`
			Name        string     `json:"name"`
			Description *string    `json:"description"`
			OwnerUserID uuid.UUID  `json:"owner_user_id"`
			EntityID    string     `json:"entity_id"`
			CreatedAt   *time.Time `json:"created_at"`
		}{result.GatewayID, result.Name, result.Description, result.OwnerUserID, result.EntityID, stamp})
	}
}
