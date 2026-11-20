package mediastorage

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// TestNativeStage8SignedUpload is opt-in and is run by the owned local Compose
// fixture. The signed URL is written only to its mode-0600 private result file;
// it is never included in test output or fixture artifacts.
func TestNativeStage8SignedUpload(t *testing.T) {
	internalURL := os.Getenv("MEDIA_STORAGE_NATIVE_INTERNAL_URL")
	if internalURL == "" {
		t.Skip("native Stage 8 Storage fixture not enabled")
	}
	publicOrigin := os.Getenv("MEDIA_STORAGE_NATIVE_PUBLIC_ORIGIN")
	serviceToken := os.Getenv("MEDIA_STORAGE_NATIVE_TOKEN")
	objectPath := os.Getenv("MEDIA_STORAGE_NATIVE_OBJECT_PATH")
	resultPath := os.Getenv("MEDIA_STORAGE_NATIVE_RESULT_FILE")
	if publicOrigin == "" || serviceToken == "" || objectPath == "" || resultPath == "" {
		t.Fatal("native Stage 8 fixture inputs incomplete")
	}
	client, err := New(Config{
		InternalBaseURL: internalURL,
		PublicOrigin:    publicOrigin,
		ServiceToken:    serviceToken,
		Timeout:         20 * time.Second,
	})
	if err != nil {
		t.Fatal("native Storage client initialization failed")
	}
	grant, err := client.SignUpload(context.Background(), objectPath)
	if err != nil {
		t.Fatal("native Storage signing failed")
	}
	result, err := json.Marshal(struct {
		URL       string    `json:"url"`
		ExpiresAt time.Time `json:"expires_at"`
	}{URL: grant.SignedURL(), ExpiresAt: grant.ExpiresAt})
	if err != nil {
		t.Fatal("native signed grant encoding failed")
	}
	file, err := os.OpenFile(resultPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal("native signed grant private result creation failed")
	}
	if _, err := file.Write(result); err != nil {
		if cErr := file.Close(); cErr != nil {
			t.Errorf("file close failed: %v", cErr)
		}
		if rErr := os.Remove(resultPath); rErr != nil && !os.IsNotExist(rErr) {
			t.Errorf("cleanup remove failed: %v", rErr)
		}
		t.Fatal("native signed grant private result write failed")
	}
	if err := file.Close(); err != nil {
		if rErr := os.Remove(resultPath); rErr != nil && !os.IsNotExist(rErr) {
			t.Errorf("cleanup remove failed: %v", rErr)
		}
		t.Fatal("native signed grant private result close failed")
	}
}
