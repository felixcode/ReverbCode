//go:build !linux

package maintenance

import (
	"errors"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
	"os"
)

type Server struct{}

func StartConfigured(*store.Store) (*Server, error) {
	if os.Getenv("ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET") != "" {
		return nil, errors.New("private authenticated maintenance requires Linux SO_PEERCRED")
	}
	return nil, nil
}
func (*Server) Close() error { return nil }
