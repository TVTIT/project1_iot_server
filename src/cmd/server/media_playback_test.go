package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"iot-platform/internal/auth"
	"iot-platform/internal/config"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/media"
	"iot-platform/internal/mediastorage"
)

func TestPopulateMediaPlaybackDependencies(t *testing.T) {
	// 1. Off state is inert and populates nil components
	var deps httpserver.RouterDependencies
	cfg := config.Config{
		MediaPlaybackEnabled: false,
	}
	if err := populateMediaPlaybackDependencies(&deps, cfg, nil); err != nil {
		t.Fatalf("expected populateMediaPlaybackDependencies to succeed when off: %v", err)
	}
	if deps.MediaPlaybackEnabled || deps.PlaybackReader != nil || deps.PlaybackSigner != nil || deps.MediaListReader != nil {
		t.Fatal("expected nil components when playback is off")
	}

	// 2. Enabled without pool must fail
	cfg.MediaPlaybackEnabled = true
	cfg.MediaPlayback.StorageInternalURL = "http://supabase-storage:5000"
	cfg.MediaPlayback.PublicOrigin = "https://playback.example.test"
	cfg.MediaPlayback.StorageServiceToken = "token"
	cfg.MediaPlayback.SigningTimeout = 5 * time.Second
	cfg.AuthorizationTimeout = 2 * time.Second
	if err := populateMediaPlaybackDependencies(&deps, cfg, nil); err == nil {
		t.Fatal("expected error when enabled with nil pool")
	}

	// 3. Nil destination deps must fail
	if err := populateMediaPlaybackDependencies(nil, cfg, nil); err == nil {
		t.Fatal("expected error when deps is nil")
	}
}

func TestMediaPlaybackCompositionOffIsInert(t *testing.T) {
	cfg := config.Config{
		MediaPlaybackEnabled: false,
	}
	deps := httpserver.DefaultTestRouterDependencies()
	deps.MediaPlaybackEnabled = cfg.MediaPlaybackEnabled
	deps.PlaybackReader = nil
	deps.PlaybackSigner = nil
	deps.MediaListReader = nil
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		return false, nil
	})
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	router, err := httpserver.NewRouter(deps)
	if err != nil {
		t.Fatalf("expected NewRouter to succeed when playback is off with nil playback dependencies: %v", err)
	}
	if router == nil {
		t.Fatal("expected non-nil router")
	}
}

func TestMediaPlaybackCompositionWiring(t *testing.T) {
	// 1. Nil pool must fail for NewPlaybackReader and NewListReader
	if _, err := media.NewPlaybackReader(nil, time.Second); err == nil {
		t.Fatal("expected NewPlaybackReader(nil, timeout) to fail")
	}
	if _, err := media.NewListReader(nil, time.Second); err == nil {
		t.Fatal("expected NewListReader(nil, timeout) to fail")
	}

	// 2. Enabled playback must reject nil dependencies in router
	deps := httpserver.DefaultTestRouterDependencies()
	deps.MediaPlaybackEnabled = true
	deps.PlaybackReader = nil
	deps.PlaybackSigner = nil
	deps.MediaListReader = nil
	deps.PlatformAdminChecker = platformAdminCheckerFunc(func(context.Context, uuid.UUID) (bool, error) {
		return false, nil
	})
	deps.TokenVerifier = tokenVerifierFunc(func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{UserID: uuid.New(), Role: "authenticated"}, nil
	})

	if _, err := httpserver.NewRouter(deps); err == nil {
		t.Fatal("expected NewRouter to reject nil playback dependencies when MediaPlaybackEnabled is true")
	}

	// 3. NewPlayback constructor requires valid HTTPS origin
	_, err := mediastorage.NewPlayback(mediastorage.Config{
		InternalBaseURL: "http://supabase-storage:5000",
		PlaybackOrigin:  "https://playback.example.test",
		ServiceToken:    "token",
		Timeout:         5 * time.Second,
	})
	if err != nil {
		t.Fatalf("expected NewPlayback to succeed with valid config: %v", err)
	}
}

type platformAdminCheckerFunc func(context.Context, uuid.UUID) (bool, error)

func (f platformAdminCheckerFunc) IsPlatformAdmin(ctx context.Context, userID uuid.UUID) (bool, error) {
	return f(ctx, userID)
}

type tokenVerifierFunc func(context.Context, string) (auth.Principal, error)

func (f tokenVerifierFunc) Verify(ctx context.Context, token string) (auth.Principal, error) {
	return f(ctx, token)
}
