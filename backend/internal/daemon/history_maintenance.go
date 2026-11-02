package daemon

import (
	"github.com/aoagents/agent-orchestrator/backend/internal/maintenance"
	sqlitestore "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
	"log/slog"
)

// A missing/stale/unsafe optional maintenance socket must never stop the main
// daemon or prevent live persistent hosts from being adopted after a crash.
func startHistoryMaintenance(log *slog.Logger, s *sqlitestore.Store) *maintenance.Server {
	server, err := maintenance.StartConfigured(s)
	if err != nil {
		log.Warn("private history maintenance unavailable", "error_type", "maintenance_socket_start_failed")
		return nil
	}
	return server
}
