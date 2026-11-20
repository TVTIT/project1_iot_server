package media

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
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
