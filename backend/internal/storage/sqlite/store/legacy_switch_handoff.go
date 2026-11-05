package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// LegacySwitchHandoffPath is the finalized handoff file the agent switching on
// main wrote for the switch that brought harness into the session on nativeID,
// or "" when there is none. That handoff rode the agent's hidden system prompt,
// so a native restore must carry it again.
func (s *Store) LegacySwitchHandoffPath(ctx context.Context, sessionID domain.SessionID, harness domain.AgentHarness, nativeID string) (string, error) {
	var path string
	err := s.readDB.QueryRowContext(ctx, `
SELECT sw.final_handoff_path FROM agent_switches sw
JOIN agent_native_sessions n ON n.id = sw.target_native_session_ref
WHERE sw.session_id = ? AND sw.target_harness = ? AND sw.final_handoff_path <> ''
  AND sw.state IN ('target_ready', 'delivering_context', 'completed', 'failed')
  AND n.ao_session_id = sw.session_id AND n.harness = sw.target_harness AND n.native_session_id = ?
ORDER BY sw.requested_at DESC LIMIT 1`, string(sessionID), string(harness), nativeID).Scan(&path)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("legacy switch handoff for %s: %w", sessionID, err)
	}
	return path, nil
}

// LegacySwitchRecoveryRequired is a bounded read against the original session
// keys. Old rows remain recovery/retention evidence; no old executor is started.
func (s *Store) LegacySwitchRecoveryRequired(ctx context.Context, sessionID domain.SessionID) (bool, error) {
	var blocked bool
	err := s.readDB.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT 1 FROM agent_switches WHERE session_id=? AND state NOT IN ('completed','failed') LIMIT 1)
 OR EXISTS(SELECT 1 FROM agent_switches sw JOIN agent_switch_failure_outbox o ON o.switch_id=sw.id
  WHERE sw.session_id=? AND o.gate_retained='true' AND o.delivered_at IS NULL AND o.discarded_at IS NULL LIMIT 1)
 OR coalesce((SELECT phase='recovery_required' FROM session_interface_transitions
  WHERE session_id=? ORDER BY rowid DESC LIMIT 1),0)`, string(sessionID), string(sessionID), string(sessionID)).Scan(&blocked)
	return blocked, err
}
