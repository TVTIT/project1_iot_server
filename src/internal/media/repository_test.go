package media

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestValidateAdmissionCanonicalizesMetadata(t *testing.T) {
	gatewayID := "fixture_gateway"
	captured := time.Date(2026, 11, 20, 1, 2, 3, 456789123, time.FixedZone("offset", 3600))
	req := Admission{
		GatewayID: gatewayID, UploadID: "upload_1", CapturedAt: captured,
		ExpectedSize: 42, DeclaredSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	got, err := validateAdmission(req)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CapturedAt.Equal(captured.UTC().Round(time.Microsecond)) || got.CapturedAt.Location() != time.UTC {
		t.Fatalf("captured_at not canonical UTC microseconds: %s", got.CapturedAt)
	}
	for name, mutate := range map[string]func(*Admission){
		"upload key":       func(r *Admission) { r.UploadID = "../foreign" },
		"uppercase digest": func(r *Admission) { r.DeclaredSHA256 = "A" + r.DeclaredSHA256[1:] },
		"size above cap":   func(r *Admission) { r.ExpectedSize = MaxUploadBytes + 1 },
		"invalid gateway":  func(r *Admission) { r.GatewayID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := req
			mutate(&bad)
			if _, err := validateAdmission(bad); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("got %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestSafeErrorsDoNotExposeDriverDetails(t *testing.T) {
	got := safeDependencyError(errors.New("postgres password=secret"))
	if !errors.Is(got, ErrDependency) || got.Error() == "" || got.Error() == "postgres password=secret" {
		t.Fatalf("unsafe dependency error: %v", got)
	}
}

func TestAdmissionRecordPathIsServerGenerated(t *testing.T) {
	id := uuid.New()
	r := Record{MediaID: id}
	if got, want := r.GeneratedStoragePath(), id.String()+".mp4"; got != want {
		t.Fatalf("path = %q, want %q", got, want)
	}
}

type playbackTestRow struct {
	path string
	err  error
}

func (r playbackTestRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		if p, ok := dest[0].(*string); ok {
			*p = r.path
		}
	}
	return nil
}

type playbackTestDB struct {
	calledSQL  string
	calledArgs []any
	row        playbackTestRow
}

func (db *playbackTestDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	db.calledSQL = sql
	db.calledArgs = args
	return db.row
}

func TestLookupPlaybackPathFound(t *testing.T) {
	mediaID := uuid.New()
	userID := uuid.New()
	gatewayID := "gw_test_01"
	expectedPath := mediaID.String() + ".mp4"

	db := &playbackTestDB{
		row: playbackTestRow{path: expectedPath},
	}
	reader, err := NewPlaybackReader(db, 2*time.Second)
	if err != nil {
		t.Fatalf("NewPlaybackReader failed: %v", err)
	}

	path, err := reader.LookupPlaybackPath(context.Background(), gatewayID, mediaID, userID)
	if err != nil {
		t.Fatalf("LookupPlaybackPath failed: %v", err)
	}
	if path != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, path)
	}

	// Verify typed arguments: exactly typed Gateway ID (string), media UUID (uuid.UUID), user UUID (uuid.UUID)
	if len(db.calledArgs) != 3 {
		t.Fatalf("expected 3 arguments, got %d", len(db.calledArgs))
	}
	argGW, ok := db.calledArgs[0].(string)
	if !ok || argGW != gatewayID {
		t.Fatalf("arg[0] expected string %q, got %T (%v)", gatewayID, db.calledArgs[0], db.calledArgs[0])
	}
	argMedia, ok := db.calledArgs[1].(uuid.UUID)
	if !ok || argMedia != mediaID {
		t.Fatalf("arg[1] expected uuid.UUID %v, got %T (%v)", mediaID, db.calledArgs[1], db.calledArgs[1])
	}
	argUser, ok := db.calledArgs[2].(uuid.UUID)
	if !ok || argUser != userID {
		t.Fatalf("arg[2] expected uuid.UUID %v, got %T (%v)", userID, db.calledArgs[2], db.calledArgs[2])
	}

	// Verify SQL contains required clauses and parameters
	sql := db.calledSQL
	for _, expectedClause := range []string{
		"public.media_objects",
		"public.user_gateways",
		"m.gateway_id = $1",
		"m.media_id = $2",
		"ug.user_id = $3",
		"managed",
		"ready",
		"owner",
		"operator",
		"viewer",
	} {
		if !strings.Contains(sql, expectedClause) {
			t.Errorf("SQL missing required clause %q:\n%s", expectedClause, sql)
		}
	}
}

func TestLookupPlaybackPathNoRowsSentinel(t *testing.T) {
	mediaID := uuid.New()
	userID := uuid.New()
	gatewayID := "gw_test_02"

	db := &playbackTestDB{
		row: playbackTestRow{err: pgx.ErrNoRows},
	}
	reader, err := NewPlaybackReader(db, 2*time.Second)
	if err != nil {
		t.Fatalf("NewPlaybackReader failed: %v", err)
	}

	path, err := reader.LookupPlaybackPath(context.Background(), gatewayID, mediaID, userID)
	if path != "" {
		t.Fatalf("expected empty path on ErrNoRows, got %q", path)
	}
	if !errors.Is(err, ErrPlaybackNotFound) {
		t.Fatalf("expected ErrPlaybackNotFound on ErrNoRows, got: %v", err)
	}
}

func TestLookupPlaybackPathCanceledOrTimeout(t *testing.T) {
	mediaID := uuid.New()
	userID := uuid.New()
	gatewayID := "gw_test_03"

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"context canceled", context.Canceled},
		{"context deadline exceeded", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &playbackTestDB{
				row: playbackTestRow{err: tc.err},
			}
			reader, err := NewPlaybackReader(db, 2*time.Second)
			if err != nil {
				t.Fatalf("NewPlaybackReader failed: %v", err)
			}

			path, err := reader.LookupPlaybackPath(context.Background(), gatewayID, mediaID, userID)
			if path != "" {
				t.Fatalf("expected empty path on %s, got %q", tc.name, path)
			}
			if err == nil {
				t.Fatalf("expected error on %s, got nil", tc.name)
			}
			if errors.Is(err, ErrPlaybackNotFound) {
				t.Fatalf("expected error to NOT be ErrPlaybackNotFound on %s, got: %v", tc.name, err)
			}
		})
	}
}

func TestLookupPlaybackPathRejectsInconsistentPath(t *testing.T) {
	mediaID := uuid.New()
	userID := uuid.New()
	gatewayID := "gw_test_04"

	for _, badPath := range []string{
		"other.mp4",
		mediaID.String() + ".mov",
		"00000000-0000-0000-0000-000000000000.mp4",
		"../" + mediaID.String() + ".mp4",
		"",
	} {
		t.Run(badPath, func(t *testing.T) {
			db := &playbackTestDB{
				row: playbackTestRow{path: badPath},
			}
			reader, err := NewPlaybackReader(db, 2*time.Second)
			if err != nil {
				t.Fatalf("NewPlaybackReader failed: %v", err)
			}

			path, err := reader.LookupPlaybackPath(context.Background(), gatewayID, mediaID, userID)
			if path != "" {
				t.Fatalf("expected empty path on inconsistent path %q, got %q", badPath, path)
			}
			if !errors.Is(err, ErrPlaybackInconsistentPath) {
				t.Fatalf("expected ErrPlaybackInconsistentPath on %q, got: %v", badPath, err)
			}
		})
	}
}

func TestLookupPlaybackPathInputValidation(t *testing.T) {
	mediaID := uuid.New()
	userID := uuid.New()
	gatewayID := "gw_valid_05"

	db := &playbackTestDB{
		row: playbackTestRow{path: mediaID.String() + ".mp4"},
	}
	reader, err := NewPlaybackReader(db, 2*time.Second)
	if err != nil {
		t.Fatalf("NewPlaybackReader failed: %v", err)
	}

	for _, tc := range []struct {
		name string
		gw   string
		mID  uuid.UUID
		uID  uuid.UUID
	}{
		{"empty gateway", "", mediaID, userID},
		{"invalid gateway format", "invalid/gateway", mediaID, userID},
		{"nil media UUID", gatewayID, uuid.Nil, userID},
		{"nil user UUID", gatewayID, mediaID, uuid.Nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db.calledArgs = nil
			path, err := reader.LookupPlaybackPath(context.Background(), tc.gw, tc.mID, tc.uID)
			if path != "" {
				t.Fatalf("expected empty path, got %q", path)
			}
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("expected ErrInvalidRequest, got: %v", err)
			}
			if len(db.calledArgs) != 0 {
				t.Fatal("DB must not be called when inputs are invalid")
			}
		})
	}
}

func TestNewPlaybackReaderValidation(t *testing.T) {
	db := &playbackTestDB{}
	if _, err := NewPlaybackReader(nil, time.Second); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for nil DB, got: %v", err)
	}
	if _, err := NewPlaybackReader(db, 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for 0 timeout, got: %v", err)
	}
	if _, err := NewPlaybackReader(db, -time.Second); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for negative timeout, got: %v", err)
	}
}
