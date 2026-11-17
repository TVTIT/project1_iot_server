package httpserver

import (
	"context"
	"errors"
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
	CancelControl(context.Context, uuid.UUID, string, string) (digitaltwin.CommandCancellation, error)
}

func controlHandler(deps RouterDependencies, operation string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !deps.ControlAPIEnabled || auth.IsNilDependency(deps.ControlRepository) {
			writeControlError(c, digitaltwin.ErrDependencyUnavailable)
			return
		}
		if operation == "cancel" {
			if !negotiateAccept(c, mimeJSON, func() { setBodyRejectionClosePolicy(c) }) {
				return
			}
		} else if !negotiateAccept(c, mimeJSON) {
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

		// For admit/cancel operations, install a read deadline interrupter that fires
		// when the request budget expires. This interrupts any blocked Body.Read.
		// Join the callback before handler/context reuse on ALL paths.
		var (
			readDeadlineStop func() bool
			readDeadlineDone chan struct{}
		)
		if operation == "admit" || operation == "cancel" {
			readDeadlineDone = make(chan struct{})
			readDeadlineStop = context.AfterFunc(ctx, func() {
				_ = http.NewResponseController(c.Writer).SetReadDeadline(time.Now())
				close(readDeadlineDone)
			})
			defer func() {
				if !readDeadlineStop() {
					<-readDeadlineDone
				}
			}()
		}

		urn := c.Param("entity_id") // net/http decoded once; never unescape again.
		if err := deps.ControlRepository.AuthorizeControlTarget(ctx, principal.UserID, urn, operation == "admit" || operation == "cancel"); err != nil {
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
		case "cancel":
			if err := parseCancelRequest(c.Request); err != nil {
				if ctx.Err() != nil {
					err = ctx.Err()
				}
				// On non-empty body rejection, synchronously interrupt post-handler drain
				// and prevent connection reuse BEFORE writing the error response.
				if isBodyRejected(err) {
					setBodyRejectionClosePolicy(c)
				}
				writeControlError(c, err)
				return
			}
			outcome, err := deps.ControlRepository.CancelControl(ctx, principal.UserID, urn, c.Param("command_id"))
			if err != nil {
				writeControlError(c, err)
				return
			}
			// The outcome is built from persisted cancellation evidence, so a
			// repeated request returns exactly the same body.
			c.JSON(http.StatusOK, outcome)
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

// isBodyRejected reports whether the error is a non-empty body rejection
// (ErrInvalidInput) rather than a context deadline/cancellation.
func isBodyRejected(err error) bool {
	return (errors.Is(err, digitaltwin.ErrInvalidInput) || errors.Is(err, errControlMediaType)) &&
		!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)
}

// setBodyRejectionClosePolicy synchronously configures the response to prevent
// post-handler body drain and connection reuse on early body rejection.
// Must be called BEFORE writeControlError / WriteHeader.
func setBodyRejectionClosePolicy(c *gin.Context) {
	// Immediately interrupt any post-handler body read.
	_ = http.NewResponseController(c.Writer).SetReadDeadline(time.Now())
	// For HTTP/1.x, mark connection for close so net/http doesn't attempt to drain/reuse.
	// For HTTP/2, Connection header is not valid; net/http handles stream reset.
	if !c.Request.ProtoAtLeast(2, 0) {
		c.Header("Connection", "close")
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
	if len(p) < 5 || p[1] != "v1" || p[2] != "digital-twins" || p[3] == "" {
		return false
	}
	switch len(p) {
	case 5:
		return p[4] == "desired-state" || p[4] == "commands"
	case 6:
		return p[4] == "commands" && p[5] != ""
	case 7:
		return p[4] == "commands" && p[5] != "" && p[6] == "cancel"
	default:
		return false
	}
}

func validateControlDependencies(deps RouterDependencies) error {
	if deps.ControlAPIEnabled && (auth.IsNilDependency(deps.ControlRepository) || deps.ControlRequestTimeout < time.Millisecond || deps.ControlRequestTimeout > time.Minute) {
		return digitaltwin.ErrDependencyUnavailable
	}
	if deps.ControlCancelEnabled && !deps.ControlAPIEnabled {
		// The cancellation surface exists only together with the control API.
		return digitaltwin.ErrDependencyUnavailable
	}
	return nil
}
