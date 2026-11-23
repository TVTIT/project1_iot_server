package media

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type listTestRow struct {
	authorized         bool
	mediaID            *uuid.UUID
	capturedAt         *time.Time
	lastVerificationAt *time.Time
	actualSize         *int64
}

type listTestRows struct {
	rows []listTestRow
	idx  int
	err  error
}

func (r *listTestRows) Close() {}
func (r *listTestRows) Err() error {
	return r.err
}
func (r *listTestRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}
func (r *listTestRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}
func (r *listTestRows) TypeMap() *pgtype.Map {
	return nil
}
func (r *listTestRows) Next() bool {
	if r.idx < len(r.rows) {
		r.idx++
		return true
	}
	return false
}
func (r *listTestRows) Scan(dest ...any) error {
	if r.idx == 0 || r.idx > len(r.rows) {
		return errors.New("scan out of bounds")
	}
	cur := r.rows[r.idx-1]
	*dest[0].(*bool) = cur.authorized
	*dest[1].(**uuid.UUID) = cur.mediaID
	*dest[2].(**time.Time) = cur.capturedAt
	*dest[3].(**time.Time) = cur.lastVerificationAt
	*dest[4].(**int64) = cur.actualSize
	return nil
}
func (r *listTestRows) Values() ([]any, error) {
	return nil, nil
}
func (r *listTestRows) RawValues() [][]byte {
	return nil
}
func (r *listTestRows) Conn() *pgx.Conn {
	return nil
}

type listTestDB struct {
	calledSQL  string
	calledArgs []any
	rows       *listTestRows
	err        error
}

func (db *listTestDB) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	db.calledSQL = sql
	db.calledArgs = args
	if db.err != nil {
		return nil, db.err
	}
	if db.rows == nil {
		return &listTestRows{}, nil
	}
	return db.rows, nil
}

func TestNewMediaListReaderValidation(t *testing.T) {
	db := &listTestDB{}
	if _, err := NewListReader(nil, time.Second); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for nil DB, got %v", err)
	}
	if _, err := NewListReader(db, 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for 0 timeout, got %v", err)
	}
	if _, err := NewListReader(db, -time.Second); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for negative timeout, got %v", err)
	}
	reader, err := NewListReader(db, 5*time.Second)
	if err != nil || reader == nil {
		t.Fatalf("expected valid reader, got err=%v", err)
	}
}

func TestListGatewayMediaInputValidation(t *testing.T) {
	db := &listTestDB{}
	reader, _ := NewListReader(db, time.Second)
	validGW := "gw_test_01"
	validUser := uuid.New()

	for _, tc := range []struct {
		name    string
		gwID    string
		userID  uuid.UUID
		limit   int
		cursor  string
		wantErr error
	}{
		{"invalid gateway id", "gw@invalid!", validUser, 50, "", ErrInvalidRequest},
		{"empty gateway id", "", validUser, 50, "", ErrInvalidRequest},
		{"nil user id", validGW, uuid.Nil, 50, "", ErrInvalidRequest},
		{"negative limit", validGW, validUser, -1, "", ErrInvalidRequest},
		{"limit 0", validGW, validUser, 0, "", ErrInvalidRequest},
		{"limit over 100", validGW, validUser, 101, "", ErrInvalidRequest},
		{"oversized cursor", validGW, validUser, 50, strings.Repeat("x", 513), ErrInvalidRequest},
		{"malformed base64 cursor", validGW, validUser, 50, "!!!not-base64", ErrInvalidRequest},
		{"cursor with non-json", validGW, validUser, 50, base64.RawURLEncoding.EncodeToString([]byte("plain text")), ErrInvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reader.ListGatewayMedia(context.Background(), tc.gwID, tc.userID, tc.limit, tc.cursor)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got err=%v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestListGatewayMediaNonmemberReturnsNotFound(t *testing.T) {
	db := &listTestDB{
		rows: &listTestRows{rows: nil}, // 0 rows returned from SQL because user_gateways had no match
	}
	reader, _ := NewListReader(db, time.Second)

	_, err := reader.ListGatewayMedia(context.Background(), "gw_test_01", uuid.New(), 50, "")
	if !errors.Is(err, ErrPlaybackNotFound) {
		t.Fatalf("expected ErrPlaybackNotFound for nonmember, got %v", err)
	}
}

func TestListGatewayMediaEmptyReturnsEmptySlice(t *testing.T) {
	gwID := "gw_test_01"
	db := &listTestDB{
		// 1 row from left join lateral where user_gateways matched, but media_id IS NULL
		rows: &listTestRows{
			rows: []listTestRow{
				{authorized: true, mediaID: nil},
			},
		},
	}
	reader, _ := NewListReader(db, time.Second)

	page, err := reader.ListGatewayMedia(context.Background(), gwID, uuid.New(), 50, "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if page.Items == nil || len(page.Items) != 0 {
		t.Fatalf("expected empty non-nil slice, got %#v", page.Items)
	}
	if page.NextCursor != "" {
		t.Fatalf("expected empty next cursor, got %q", page.NextCursor)
	}
}

func TestListGatewayMediaQueriesReadyManagedOnly(t *testing.T) {
	gwID := "gw_test_01"
	userID := uuid.New()
	mediaID := uuid.New()
	capturedAt := time.Date(2026, 11, 23, 10, 0, 0, 123456000, time.UTC)
	verifiedAt := time.Date(2026, 11, 23, 10, 5, 0, 654321000, time.UTC)
	size := int64(1048576)

	db := &listTestDB{
		rows: &listTestRows{
			rows: []listTestRow{
				{
					authorized:         true,
					mediaID:            &mediaID,
					capturedAt:         &capturedAt,
					lastVerificationAt: &verifiedAt,
					actualSize:         &size,
				},
			},
		},
	}
	reader, _ := NewListReader(db, time.Second)

	page, err := reader.ListGatewayMedia(context.Background(), gwID, userID, 50, "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(page.Items))
	}
	item := page.Items[0]
	if item.MediaID != mediaID || !item.CapturedAt.Equal(capturedAt) || !item.LastVerificationAt.Equal(verifiedAt) || item.ActualSize != size {
		t.Fatalf("unexpected item content: %+v", item)
	}

	// Verify required SQL clauses
	sql := db.calledSQL
	for _, clause := range []string{
		"public.user_gateways",
		"public.media_objects",
		"LEFT JOIN LATERAL",
		"m.managed = true",
		"m.validation_status = 'ready'",
		"m.actual_size IS NOT NULL",
		"m.last_verification_at IS NOT NULL",
		"ug.role IN ('owner', 'operator', 'viewer')",
		"ORDER BY m.captured_at DESC, m.media_id DESC",
	} {
		if !strings.Contains(sql, clause) {
			t.Errorf("SQL missing required clause %q:\n%s", clause, sql)
		}
	}

	// Check arguments passed: gwID, userID, before_captured_at (nil), before_media_id (nil), limit+1 (51)
	if len(db.calledArgs) != 5 {
		t.Fatalf("expected 5 arguments, got %d", len(db.calledArgs))
	}
	if db.calledArgs[0] != gwID {
		t.Errorf("arg[0] = %v, want %v", db.calledArgs[0], gwID)
	}
	if db.calledArgs[1] != userID {
		t.Errorf("arg[1] = %v, want %v", db.calledArgs[1], userID)
	}
	if db.calledArgs[2] != nil {
		t.Errorf("arg[2] = %v, want nil", db.calledArgs[2])
	}
	if db.calledArgs[3] != nil {
		t.Errorf("arg[3] = %v, want nil", db.calledArgs[3])
	}
	if db.calledArgs[4] != 51 {
		t.Errorf("arg[4] = %v, want 51 (limit+1)", db.calledArgs[4])
	}
}

func TestListGatewayMediaCursorPreservationAndGatewayBinding(t *testing.T) {
	gwID := "gw_test_01"
	otherGW := "gw_test_02"
	userID := uuid.New()
	mediaID := uuid.New()
	capturedAt := time.Date(2026, 11, 23, 10, 0, 0, 123456000, time.UTC)

	// Valid cursor bound to otherGW
	otherCursorData, _ := json.Marshal([3]string{
		otherGW,
		capturedAt.Format(time.RFC3339Nano),
		mediaID.String(),
	})
	otherCursor := base64.RawURLEncoding.EncodeToString(otherCursorData)

	db := &listTestDB{}
	reader, _ := NewListReader(db, time.Second)

	// Passing otherGW cursor to gwID must return ErrInvalidRequest (cross-scope reuse returns 400, not auth bypass!)
	_, err := reader.ListGatewayMedia(context.Background(), gwID, userID, 50, otherCursor)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for cross-gateway cursor, got %v", err)
	}

	// Valid cursor bound to gwID
	validCursorData, _ := json.Marshal([3]string{
		gwID,
		capturedAt.Format(time.RFC3339Nano),
		mediaID.String(),
	})
	validCursor := base64.RawURLEncoding.EncodeToString(validCursorData)

	size := int64(100)
	verifiedAt := capturedAt.Add(time.Minute)
	db.rows = &listTestRows{
		rows: []listTestRow{
			{authorized: true, mediaID: &mediaID, capturedAt: &capturedAt, lastVerificationAt: &verifiedAt, actualSize: &size},
		},
	}

	_, err = reader.ListGatewayMedia(context.Background(), gwID, userID, 10, validCursor)
	if err != nil {
		t.Fatalf("unexpected err for valid cursor: %v", err)
	}

	// Verify cursor timestamp microsecond precision was passed to arg[2]
	parsedTS, ok := db.calledArgs[2].(time.Time)
	if !ok {
		t.Fatalf("arg[2] expected time.Time, got %T", db.calledArgs[2])
	}
	if !parsedTS.Equal(capturedAt) || parsedTS.Nanosecond() != capturedAt.Nanosecond() {
		t.Fatalf("arg[2] timestamp mismatch: got %v, want %v", parsedTS, capturedAt)
	}
	parsedID, ok := db.calledArgs[3].(uuid.UUID)
	if !ok || parsedID != mediaID {
		t.Fatalf("arg[3] uuid mismatch: got %v, want %v", db.calledArgs[3], mediaID)
	}
}

func TestListGatewayMediaPaginationHasMore(t *testing.T) {
	gwID := "gw_test_01"
	userID := uuid.New()

	// Limit is 2, but we return 3 rows (limit+1)
	m1 := uuid.New()
	m2 := uuid.New()
	m3 := uuid.New()
	t1 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 11, 23, 11, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	size := int64(500)

	db := &listTestDB{
		rows: &listTestRows{
			rows: []listTestRow{
				{authorized: true, mediaID: &m1, capturedAt: &t1, lastVerificationAt: &t1, actualSize: &size},
				{authorized: true, mediaID: &m2, capturedAt: &t2, lastVerificationAt: &t2, actualSize: &size},
				{authorized: true, mediaID: &m3, capturedAt: &t3, lastVerificationAt: &t3, actualSize: &size},
			},
		},
	}
	reader, _ := NewListReader(db, time.Second)

	page, err := reader.ListGatewayMedia(context.Background(), gwID, userID, 2, "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(page.Items))
	}
	if page.NextCursor == "" {
		t.Fatal("expected non-empty next cursor when limit+1 fetched")
	}

	// Verify next cursor decodes and points to m2 (the last item on page 1)
	decodedBytes, err := base64.RawURLEncoding.DecodeString(page.NextCursor)
	if err != nil {
		t.Fatalf("cursor not base64url: %v", err)
	}
	var parts [3]string
	if err := json.Unmarshal(decodedBytes, &parts); err != nil {
		t.Fatalf("cursor unmarshal error: %v", err)
	}
	if parts[0] != gwID {
		t.Errorf("cursor gw = %q, want %q", parts[0], gwID)
	}
	if parts[1] != t2.Format(time.RFC3339Nano) {
		t.Errorf("cursor ts = %q, want %q", parts[1], t2.Format(time.RFC3339Nano))
	}
	if parts[2] != m2.String() {
		t.Errorf("cursor media_id = %q, want %q", parts[2], m2.String())
	}
}
