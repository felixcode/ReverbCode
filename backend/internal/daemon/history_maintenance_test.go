package daemon

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func TestOptionalHistoryMaintenanceKeepsBusinessAvailable(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	private := filepath.Join(dir, "private")
	if err = os.Mkdir(private, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(private, "stale.sock")
	if err = os.WriteFile(path, []byte("foreign-preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET", path)
	server := startHistoryMaintenance(slog.New(slog.NewTextHandler(io.Discard, nil)), s)
	if server != nil {
		server.Close()
		t.Fatal("unknown path should not be adopted")
	}
	if err = s.UpsertProject(context.Background(), domain.ProjectRecord{ID: "business", Path: "/tmp/synthetic", RegisteredAt: time.Now()}); err != nil {
		t.Fatal("maintenance failure blocked store", err)
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "foreign-preserved" {
		t.Fatal("stale path was modified")
	}
}
