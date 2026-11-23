package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"iot-platform/internal/gateway"
)

// ListItem represents a verified ready media item for gateway listing.
type ListItem struct {
	MediaID            uuid.UUID
	CapturedAt         time.Time
	LastVerificationAt time.Time
	ActualSize         int64
}

// ListPage represents a page of media items and optional next page cursor.
type ListPage struct {
	Items      []ListItem
	NextCursor string
}

type listDB interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func isNilListDB(db listDB) bool {
	if db == nil {
		return true
	}
	v := reflect.ValueOf(db)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

// ListReader manages authorized media listing queries from PostgreSQL.
type ListReader struct {
	db      listDB
	timeout time.Duration
}

// NewListReader constructs a ListReader with a validated database connection and timeout.
func NewListReader(db listDB, timeout time.Duration) (*ListReader, error) {
	if isNilListDB(db) || timeout <= 0 {
		return nil, ErrInvalidRequest
	}
	return &ListReader{db: db, timeout: timeout}, nil
}

const mediaListQuery = `SELECT
    true,
    m.media_id,
    m.captured_at,
    m.last_verification_at,
    m.actual_size
FROM public.user_gateways ug
LEFT JOIN LATERAL (
    SELECT
        m.media_id,
        m.captured_at,
        m.last_verification_at,
        m.actual_size
    FROM public.media_objects m
    WHERE m.gateway_id = ug.gateway_id
      AND m.managed = true
      AND m.validation_status = 'ready'
      AND m.actual_size IS NOT NULL
      AND m.last_verification_at IS NOT NULL
      AND (
          $3::timestamptz IS NULL
          OR m.captured_at < $3::timestamptz
          OR (m.captured_at = $3::timestamptz AND m.media_id < $4::uuid)
      )
    ORDER BY m.captured_at DESC, m.media_id DESC
    LIMIT $5
) m ON true
WHERE ug.gateway_id = $1
  AND ug.user_id = $2
  AND ug.role IN ('owner', 'operator', 'viewer')
ORDER BY m.captured_at DESC NULLS LAST, m.media_id DESC NULLS LAST`

// ListGatewayMedia queries verified ready media items for a gateway.
func (r *ListReader) ListGatewayMedia(ctx context.Context, gatewayID string, userID uuid.UUID, limit int, cursor string) (ListPage, error) {
	if gateway.ValidateGatewayID(gatewayID) != nil || userID == uuid.Nil || limit < 1 || limit > 100 {
		return ListPage{}, ErrInvalidRequest
	}

	var beforeTS *time.Time
	var beforeID *uuid.UUID

	if cursor != "" {
		if len(cursor) > 512 {
			return ListPage{}, ErrInvalidRequest
		}
		data, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return ListPage{}, ErrInvalidRequest
		}
		var parts []string
		if err := json.Unmarshal(data, &parts); err != nil || len(parts) != 3 {
			return ListPage{}, ErrInvalidRequest
		}
		if parts[0] != gatewayID {
			return ListPage{}, ErrInvalidRequest
		}
		ts, err := time.Parse(time.RFC3339Nano, parts[1])
		if err != nil || ts.IsZero() {
			return ListPage{}, ErrInvalidRequest
		}
		id, err := uuid.Parse(parts[2])
		if err != nil || id == uuid.Nil || id.String() != parts[2] {
			return ListPage{}, ErrInvalidRequest
		}
		utcTS := ts.UTC()
		beforeTS = &utcTS
		beforeID = &id
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	limitPlusOne := limit + 1
	var beforeTSArg any
	if beforeTS != nil {
		beforeTSArg = *beforeTS
	}
	var beforeIDArg any
	if beforeID != nil {
		beforeIDArg = *beforeID
	}

	rows, err := r.db.Query(ctx, mediaListQuery, gatewayID, userID, beforeTSArg, beforeIDArg, limitPlusOne)
	if err != nil {
		if ctx.Err() != nil {
			return ListPage{}, fmt.Errorf("%w: %w", ErrDependency, ctx.Err())
		}
		return ListPage{}, safeDependencyError(err)
	}
	defer rows.Close()

	items := make([]ListItem, 0, limit)
	rowCount := 0

	for rows.Next() {
		rowCount++
		var authorized bool
		var mediaID *uuid.UUID
		var capturedAt *time.Time
		var lastVerificationAt *time.Time
		var actualSize *int64

		if err := rows.Scan(&authorized, &mediaID, &capturedAt, &lastVerificationAt, &actualSize); err != nil {
			return ListPage{}, safeDependencyError(err)
		}

		if mediaID == nil {
			// user_gateways matched, but media_objects had 0 results
			continue
		}

		if capturedAt == nil || lastVerificationAt == nil || actualSize == nil {
			return ListPage{}, safeDependencyError(errors.New("inconsistent ready media row"))
		}

		items = append(items, ListItem{
			MediaID:            *mediaID,
			CapturedAt:         *capturedAt,
			LastVerificationAt: *lastVerificationAt,
			ActualSize:         *actualSize,
		})
	}

	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return ListPage{}, fmt.Errorf("%w: %w", ErrDependency, ctx.Err())
		}
		return ListPage{}, safeDependencyError(err)
	}

	if rowCount == 0 {
		return ListPage{}, ErrPlaybackNotFound
	}

	page := ListPage{
		Items: items,
	}

	if len(page.Items) > limit {
		page.Items = page.Items[:limit]
		lastItem := page.Items[limit-1]
		cursorData, err := json.Marshal([3]string{
			gatewayID,
			lastItem.CapturedAt.UTC().Format(time.RFC3339Nano),
			lastItem.MediaID.String(),
		})
		if err != nil {
			return ListPage{}, safeDependencyError(err)
		}
		page.NextCursor = base64.RawURLEncoding.EncodeToString(cursorData)
	}

	return page, nil
}
