package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPreservedQueueRecoveryCLIOnlyPostsExplicitMetadata(t *testing.T) {
	cfg := setConfigEnv(t)
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/internal/") {
			w.WriteHeader(202)
			return
		}
		if r.Method != "POST" || r.URL.Path != "/api/v1/dev/sessions/synthetic/preserved-queue/recover" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		calls++
		raw, _ := io.ReadAll(r.Body)
		if !json.Valid(raw) {
			t.Error("not metadata JSON")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"restored":2,"previouslyRestored":0,"requestSha256":"digest-only","dispatch":"held_busy"}`)
	}))
	defer srv.Close()
	writeRunFileFor(t, cfg, srv)
	p := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(p, []byte(`{"protocol":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	out, errOut, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, "session", "recover-preserved-queue", "synthetic", "--request", p)
	if err != nil {
		t.Fatalf("metadata call: %v %s", err, errOut)
	}
	if calls != 1 || !strings.Contains(out, "held_busy") {
		t.Fatal("missing audit")
	}
	if _, _, err := executeCLI(t, Deps{}, "session", "recover-preserved-queue", "synthetic"); err == nil || ExitCode(err) != 2 {
		t.Fatal("missing request accepted")
	}
}
