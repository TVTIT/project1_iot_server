package main

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"iot-platform/internal/config"
	"iot-platform/internal/httpserver"
	"iot-platform/internal/media"
	"iot-platform/internal/mediastorage"
)

// populateMediaPlaybackDependencies wires only media playback HTTP dependencies into RouterDependencies.
func populateMediaPlaybackDependencies(deps *httpserver.RouterDependencies, cfg config.Config, pool *pgxpool.Pool) error {
	if deps == nil {
		return errors.New("nil router dependencies destination")
	}
	deps.MediaPlaybackEnabled = cfg.MediaPlaybackEnabled
	if !cfg.MediaPlaybackEnabled {
		deps.PlaybackReader = nil
		deps.PlaybackSigner = nil
		deps.MediaListReader = nil
		return nil
	}
	if pool == nil {
		return errors.New("database pool is required when media playback is enabled")
	}
	playbackReader, err := media.NewPlaybackReader(pool, cfg.AuthorizationTimeout)
	if err != nil {
		return fmt.Errorf("create media playback reader: %w", err)
	}
	mediaListReader, err := media.NewListReader(pool, cfg.AuthorizationTimeout)
	if err != nil {
		return fmt.Errorf("create media list reader: %w", err)
	}
	playbackSigner, err := mediastorage.NewPlayback(mediastorage.Config{
		InternalBaseURL: cfg.MediaPlayback.StorageInternalURL,
		PlaybackOrigin:  cfg.MediaPlayback.PublicOrigin,
		ServiceToken:    cfg.MediaPlayback.StorageServiceToken,
		Timeout:         cfg.MediaPlayback.SigningTimeout,
	})
	if err != nil {
		return fmt.Errorf("create media playback signer: %w", err)
	}
	deps.PlaybackReader = playbackReader
	deps.PlaybackSigner = playbackSigner
	deps.MediaListReader = mediaListReader
	return nil
}
