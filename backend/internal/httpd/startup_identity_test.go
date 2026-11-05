package httpd

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
)

func TestHealthStartupMatchesPublishedRunfile(t *testing.T) {
	cfg := config.Config{Host: "127.0.0.1", Port: 0, ShutdownTimeout: time.Second, RunFilePath: filepath.Join(t.TempDir(), "running.json"), AppRunID: "startup-integration"}
	srv, err := NewWithDeps(cfg, discardLogger(), nil, APIDeps{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AO_OWNER", "persistent")
	t.Setenv("AO_BROWSER_RUNTIME_ADDRESS", "/task-owned-browser.sock")
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- srv.RunWithReady(ctx, func() { close(ready) }) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("startup timeout")
	}
	info, err := runfile.Read(cfg.RunFilePath)
	if err != nil || info == nil {
		t.Fatalf("run-file: %v %v", info, err)
	}
	if info.Owner != "persistent" || info.BrowserRuntimeAddress != "/task-owned-browser.sock" {
		t.Fatal("startup altered Run-time ownership inputs")
	}
	resp, err := http.Get("http://" + srv.Addr().String() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var p struct {
		Status    string    `json:"status"`
		Service   string    `json:"service"`
		PID       int       `json:"pid"`
		StartedAt time.Time `json:"startedAt"`
		AppRunID  string    `json:"appRunId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Status != "ok" || p.Service != daemonmeta.ServiceName || p.PID != info.PID || p.StartedAt.IsZero() || !p.StartedAt.Equal(info.StartedAt) || p.AppRunID != info.AppRunID || p.AppRunID != cfg.AppRunID {
		t.Fatalf("probe=%+v handshake=%+v", p, info)
	}
}
