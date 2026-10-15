package claudecode

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func prohibitCatalogCLI(t *testing.T) {
	t.Helper()
	previous := claudeModelAuthReport
	claudeModelAuthReport = func(context.Context, string, string, map[string]string) (claudeAuthReport, bool) {
		t.Error("reading a catalog fingerprint launched the auth CLI")
		return claudeAuthReport{}, false
	}
	t.Cleanup(func() { claudeModelAuthReport = previous })
}

func TestProviderCatalogFingerprintLocalInputsNoCLI(t *testing.T) {
	clearClaudeCredentialEnv(t)
	prohibitCatalogCLI(t)
	env := map[string]string{"ANTHROPIC_API_KEY": "synthetic-secret-one"}
	ctx := context.Background()
	first := ProviderCatalogFingerprint(ctx, "/missing/claude", "", env)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if got := ProviderCatalogFingerprint(ctx, "/missing/claude", "", env); got != first {
				t.Error("unchanged local inputs changed the fingerprint")
			}
		})
	}
	wg.Wait()
	if strings.Contains(first, env["ANTHROPIC_API_KEY"]) {
		t.Fatal("fingerprint exposed the credential")
	}
	env["ANTHROPIC_API_KEY"] = "synthetic-secret-two"
	rotated := ProviderCatalogFingerprint(ctx, "", "", env)
	if rotated == first {
		t.Fatal("API key rotation did not invalidate the catalog")
	}
	env["ANTHROPIC_BASE_URL"] = "https://gateway-one.invalid"
	gateway := ProviderCatalogFingerprint(ctx, "", "", env)
	if gateway == rotated {
		t.Fatal("provider change did not invalidate the catalog")
	}
	env["ANTHROPIC_BASE_URL"] = "https://gateway-two.invalid"
	if ProviderCatalogFingerprint(ctx, "", "", env) == gateway {
		t.Fatal("gateway endpoint change did not invalidate the catalog")
	}
}

func TestProviderCatalogFingerprintCredentialFileRotationAndLogout(t *testing.T) {
	clearClaudeCredentialEnv(t)
	prohibitCatalogCLI(t)
	dir := os.Getenv("CLAUDE_CONFIG_DIR")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, ".credentials.json")
	fingerprint := func() string { return ProviderCatalogFingerprint(context.Background(), "", "", nil) }
	loggedOut := fingerprint()
	if err := os.WriteFile(p, []byte(`{"claudeAiOauth":{"accessToken":"synthetic-one"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	one := fingerprint()
	if one == loggedOut {
		t.Fatal("login did not invalidate the catalog")
	}
	if err := os.WriteFile(p, []byte(`{"claudeAiOauth":{"accessToken":"synthetic-two"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if fingerprint() == one {
		t.Fatal("stored token rotation did not invalidate the catalog")
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if fingerprint() != loggedOut {
		t.Fatal("logout did not restore the unsigned fingerprint")
	}
}

func TestProviderCatalogFingerprintTracksProjectSettingsWithoutCLI(t *testing.T) {
	clearClaudeCredentialEnv(t)
	prohibitCatalogCLI(t)
	dir := t.TempDir()
	settings := filepath.Join(dir, ".claude", "settings.local.json")
	if err := os.MkdirAll(filepath.Dir(settings), 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(key string) {
		t.Helper()
		if err := os.WriteFile(settings, []byte(`{"env":{"ANTHROPIC_BASE_URL":"https://gateway.invalid","ANTHROPIC_AUTH_TOKEN":"`+key+`"}}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("synthetic-one")
	one := ProviderCatalogFingerprint(context.Background(), "", dir, nil)
	write("synthetic-two")
	if ProviderCatalogFingerprint(context.Background(), "", dir, nil) == one {
		t.Fatal("workspace credential settings change did not invalidate the catalog")
	}
}
