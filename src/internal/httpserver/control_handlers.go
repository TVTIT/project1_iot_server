package httpserver

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/digitaltwin"
	"iot-platform/internal/httpapi"
)

// ControlRepository commits intent and reads safe command projections. It has no
// MQTT method; execution confirmation belongs to the later result runtime.
type ControlRepository interface {
	AuthorizeControlTarget(context.Context, uuid.UUID, string, bool) error
	Admit(context.Context, uuid.UUID, string, digitaltwin.DesiredRequest) (digitaltwin.RequestReceipt, error)
	ListCommands(context.Context, uuid.UUID, string, int, string) (digitaltwin.ControlCommandPage, error)
	GetCommand(context.Context, uuid.UUID, string, string) (digitaltwin.ControlCommandView, error)
}

func controlHandler(deps RouterDependencies, operation string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !deps.ControlAPIEnabled || auth.IsNilDependency(deps.ControlRepository) {
			writeControlError(c, digitaltwin.ErrDependencyUnavailable)
			return
		}
		if !negotiateAccept(c, mimeJSON) {
			return
		}
		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		// One deadline spans authorization, parsing and all DB work, rather than
		// granting a fresh budget to each repository invocation.
		ctx, cancel := context.WithTimeout(c.Request.Context(), deps.ControlRequestTimeout)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		if operation == "admit" {
			// Context cancellation alone does not interrupt net/http Body.Read.
			// Interrupt it only when this budget ends, preserving any earlier
			// server read deadline. Join a started callback before connection reuse.
			done := make(chan struct{})
			stop := context.AfterFunc(ctx, func() {
				_ = http.NewResponseController(c.Writer).SetReadDeadline(time.Now())
				close(done)
			})
			defer func() {
				if !stop() {
					<-done
				}
			}()
		}
		urn := c.Param("entity_id") // net/http decoded once; never unescape again.
		if err := deps.ControlRepository.AuthorizeControlTarget(ctx, principal.UserID, urn, operation == "admit"); err != nil {
			writeControlError(c, err)
			return
		}
		switch operation {
		case "admit":
			input, _, err := parseDesiredStateRequest(c.Request, urn, deps.TwinMaxBodyBytes)
			if err != nil {
				if ctx.Err() != nil {
					err = ctx.Err()
				}
				writeControlError(c, err)
				return
			}
			receipt, err := deps.ControlRepository.Admit(ctx, principal.UserID, urn, input.Input)
			if err != nil {
				writeControlError(c, err)
				return
			}
			if receipt.CommandID != nil {
				c.Header("Location", controlStatusURL(urn, *receipt.CommandID))
			}
			// Data writes the original receipt bytes, without JSON re-encoding or
			// querying current command status (including after newer versions).
			c.Data(receipt.StatusCode, "application/json; charset=utf-8", receipt.Response)
		case "list":
			limit, cursor, err := parseControlQuery(c.Request, false)
			if err != nil {
				writeControlError(c, err)
				return
			}
			page, err := deps.ControlRepository.ListCommands(ctx, principal.UserID, urn, limit, cursor)
			if err != nil {
				writeControlError(c, err)
				return
			}
			if page.NextCursor != "" {
				q := url.Values{"limit": {strconv.Itoa(limit)}, "cursor": {page.NextCursor}}
				c.Header("Link", "</v1/digital-twins/"+url.PathEscape(urn)+"/commands?"+q.Encode()+">; rel=\"next\"")
			}
			c.JSON(http.StatusOK, gin.H{"commands": page.Commands})
		case "detail":
			if _, _, err := parseControlQuery(c.Request, true); err != nil {
				writeControlError(c, err)
				return
			}
			view, err := deps.ControlRepository.GetCommand(ctx, principal.UserID, urn, c.Param("command_id"))
			if err != nil {
				writeControlError(c, err)
				return
			}
			c.JSON(http.StatusOK, view)
		}
	}
}

func controlStatusURL(urn, commandID string) string {
	return "/v1/digital-twins/" + url.PathEscape(urn) + "/commands/" + commandID
}

func parseControlQuery(r *http.Request, detail bool) (int, string, error) {
	if len(r.URL.RawQuery) > digitaltwin.MaxRawQueryBytes || r.URL.ForceQuery || (detail && r.URL.RawQuery != "") {
		return 0, "", digitaltwin.ErrInvalidInput
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return 0, "", digitaltwin.ErrInvalidInput
	}
	limit, cursor := 20, ""
	for key, v := range values {
		if len(v) != 1 || v[0] == "" {
			return 0, "", digitaltwin.ErrInvalidInput
		}
		switch key {
		case "limit":
			if len(v[0]) > 3 || strings.Trim(v[0], "0123456789") != "" {
				return 0, "", digitaltwin.ErrInvalidInput
			}
			limit, err = strconv.Atoi(v[0])
			if err != nil || limit < 1 || limit > 100 {
				return 0, "", digitaltwin.ErrInvalidInput
			}
		case "cursor":
			cursor = v[0]
			if len(cursor) > 512 {
				return 0, "", digitaltwin.ErrInvalidInput
			}
		default:
			return 0, "", digitaltwin.ErrInvalidInput
		}
	}
	return limit, cursor, nil
}

func writeControlError(c *gin.Context, err error) {
	status, code := controlErrorResponse(err)
	httpapi.WriteError(c, status, code, strings.ReplaceAll(code, "_", " "))
}

func controlCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if isControlRoute(c.Request.URL.Path) {
			c.Header("Cache-Control", "no-store")
			if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
				c.Header("Pragma", "no-cache")
			}
		}
		c.Next()
	}
}

func isControlRoute(path string) bool {
	p := strings.Split(strings.TrimSuffix(path, "/"), "/")
	return len(p) >= 5 && p[1] == "v1" && p[2] == "digital-twins" && p[3] != "" &&
		((len(p) == 5 && (p[4] == "desired-state" || p[4] == "commands")) ||
			(len(p) == 6 && p[4] == "commands" && p[5] != ""))
}

func validateControlDependencies(deps RouterDependencies) error {
	if deps.ControlAPIEnabled && (auth.IsNilDependency(deps.ControlRepository) || deps.ControlRequestTimeout < time.Millisecond || deps.ControlRequestTimeout > time.Minute) {
		return digitaltwin.ErrDependencyUnavailable
	}
	return nil
}
