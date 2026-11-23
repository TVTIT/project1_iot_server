package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/gateway"
	"iot-platform/internal/httpapi"
	"iot-platform/internal/media"
	"iot-platform/internal/mediastorage"
)

// PlaybackPathReader discovers the canonical verified object path for an authorized media playback.
type PlaybackPathReader interface {
	LookupPlaybackPath(ctx context.Context, gatewayID string, mediaID, userID uuid.UUID) (string, error)
}

// PlaybackSigner issues ephemeral signed playback read URLs.
type PlaybackSigner interface {
	SignRead(ctx context.Context, objectPath string) (mediastorage.ReadGrant, error)
}

// GatewayMediaListReader discovers verified ready media items for an authorized gateway.
type GatewayMediaListReader interface {
	ListGatewayMedia(ctx context.Context, gatewayID string, userID uuid.UUID, limit int, cursor string) (media.ListPage, error)
}

var (
	playbackExactRouteRE  = regexp.MustCompile(`^/v1/gateways/[^/]+/media/[^/]+/playback-url$`)
	mediaListExactRouteRE = regexp.MustCompile(`^/v1/gateways/[^/]+/media$`)
)

func playbackCacheMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if (c.Request.Method == http.MethodPost && playbackExactRouteRE.MatchString(c.Request.URL.Path)) ||
			(c.Request.Method == http.MethodGet && mediaListExactRouteRE.MatchString(c.Request.URL.Path)) {
			c.Header("Cache-Control", "no-store")
		}
		c.Next()
	}
}

type mediaItemJSON struct {
	MediaID    string `json:"media_id"`
	FileName   string `json:"file_name"`
	CapturedAt string `json:"captured_at"`
	ReadyAt    string `json:"ready_at"`
	SizeBytes  int64  `json:"size_bytes"`
}

func parseMediaListQuery(r *http.Request) (int, string, error) {
	if len(r.URL.RawQuery) > 2048 || r.URL.ForceQuery {
		return 0, "", media.ErrInvalidRequest
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return 0, "", media.ErrInvalidRequest
	}
	limit := 50
	cursor := ""
	for key, v := range values {
		if len(v) != 1 || v[0] == "" {
			return 0, "", media.ErrInvalidRequest
		}
		switch key {
		case "limit":
			if len(v[0]) > 3 || strings.Trim(v[0], "0123456789") != "" {
				return 0, "", media.ErrInvalidRequest
			}
			limit, err = strconv.Atoi(v[0])
			if err != nil || limit < 1 || limit > 100 {
				return 0, "", media.ErrInvalidRequest
			}
		case "cursor":
			cursor = v[0]
			if len(cursor) > 512 {
				return 0, "", media.ErrInvalidRequest
			}
		default:
			return 0, "", media.ErrInvalidRequest
		}
	}
	return limit, cursor, nil
}

func listGatewayMediaHandler(reader GatewayMediaListReader) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, cursor, err := parseMediaListQuery(c.Request)
		if err != nil {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request query")
			return
		}

		gatewayID := c.Param("gateway_id")
		if gateway.ValidateGatewayID(gatewayID) != nil {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid gateway id")
			return
		}

		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "unauthorized")
			return
		}

		page, err := reader.ListGatewayMedia(c.Request.Context(), gatewayID, principal.UserID, limit, cursor)
		if err != nil {
			if errors.Is(err, media.ErrPlaybackNotFound) {
				httpapi.WriteError(c, http.StatusNotFound, "not_found", "resource not found")
				return
			}
			if errors.Is(err, media.ErrInvalidRequest) {
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
				return
			}
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "database unavailable")
			return
		}

		if page.NextCursor != "" {
			link := fmt.Sprintf("</v1/gateways/%s/media?limit=%d&cursor=%s>; rel=\"next\"",
				url.PathEscape(gatewayID), limit, url.QueryEscape(page.NextCursor))
			c.Header("Link", link)
		}

		items := make([]mediaItemJSON, 0, len(page.Items))
		for _, it := range page.Items {
			items = append(items, mediaItemJSON{
				MediaID:    it.MediaID.String(),
				FileName:   it.MediaID.String() + ".mp4",
				CapturedAt: it.CapturedAt.UTC().Format(time.RFC3339Nano),
				ReadyAt:    it.LastVerificationAt.UTC().Format(time.RFC3339Nano),
				SizeBytes:  it.ActualSize,
			})
		}

		c.JSON(http.StatusOK, items)
	}
}

func playbackURLHandler(reader PlaybackPathReader, signer PlaybackSigner) gin.HandlerFunc {
	return func(c *gin.Context) {
		gatewayID := c.Param("gateway_id")
		if gateway.ValidateGatewayID(gatewayID) != nil {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid gateway id")
			return
		}

		mediaIDStr := c.Param("media_id")
		mediaID, err := uuid.Parse(mediaIDStr)
		if err != nil || mediaID == uuid.Nil || mediaIDStr != mediaID.String() {
			httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid media id")
			return
		}

		principal, ok := auth.PrincipalFrom(c)
		if !ok || principal.UserID == uuid.Nil {
			httpapi.WriteError(c, http.StatusUnauthorized, "unauthorized", "unauthorized")
			return
		}

		storagePath, err := reader.LookupPlaybackPath(c.Request.Context(), gatewayID, mediaID, principal.UserID)
		if err != nil {
			if errors.Is(err, media.ErrPlaybackNotFound) {
				httpapi.WriteError(c, http.StatusNotFound, "not_found", "resource not found")
				return
			}
			if errors.Is(err, media.ErrInvalidRequest) {
				httpapi.WriteError(c, http.StatusBadRequest, "invalid_request", "invalid request")
				return
			}
			if errors.Is(err, media.ErrPlaybackInconsistentPath) {
				httpapi.WriteError(c, http.StatusBadGateway, "bad_gateway", "inconsistent media metadata")
				return
			}
			httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "database unavailable")
			return
		}

		grant, err := signer.SignRead(c.Request.Context(), storagePath)
		if err != nil {
			if errors.Is(err, mediastorage.ErrSignerUnavailable) {
				httpapi.WriteError(c, http.StatusServiceUnavailable, "service_unavailable", "storage service unavailable")
				return
			}
			httpapi.WriteError(c, http.StatusBadGateway, "bad_gateway", "invalid storage signer response")
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"url":        grant.SignedURL(),
			"expires_at": grant.ExpiresAt.UTC().Format(time.RFC3339),
		})
	}
}
