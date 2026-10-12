package cli

import (
	"context"
	"encoding/json"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// This test compiles unchanged on the frozen parent: a host daemon is healthy
// while the caller's PID namespace cannot see it. No real review is submitted.
func TestInvisiblePIDHealthyDaemonReproduction(t *testing.T) {
	cfg := setConfigEnv(t)
	started := time.Unix(100, 42).UTC()
	posts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "service": daemonmeta.ServiceName, "pid": 4242, "startedAt": started, "appRunId": "launch"})
			return
		}
		posts++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	if err := runfile.Write(cfg.runFile, runfile.Info{PID: 4242, Port: serverPort(t, srv.URL), StartedAt: started, AppRunID: "launch"}); err != nil {
		t.Fatal(err)
	}
	c := &commandContext{deps: Deps{HTTPClient: &http.Client{}, ProcessAlive: func(int) bool { return false }}}
	if err := c.postJSON(context.Background(), "sessions/worker/reviews/submit", map[string]string{"runId": "synthetic", "verdict": "approved"}, nil); err != nil {
		t.Fatalf("healthy host daemon rejected when PID invisible: %v", err)
	}
	if posts != 1 {
		t.Fatalf("callback requests=%d, want 1", posts)
	}
}
