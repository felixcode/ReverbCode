package store

import (
	"context"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// Same active-lineage predicate as SelectConversationTurns. Only running control
// fields are needed for live ownership; body columns are never selected.
const liveConversationTurnsSQL = `WITH RECURSIVE active_path(branch_id, max_sequence) AS (
    SELECT conversations.active_branch_id, CAST(NULL AS INTEGER)
    FROM conversations
    WHERE conversations.id = ?
    UNION ALL
    SELECT branch.parent_branch_id,
           CASE
               WHEN path.max_sequence IS NULL THEN branch.fork_after_sequence
               WHEN branch.fork_after_sequence < path.max_sequence THEN branch.fork_after_sequence
               ELSE path.max_sequence
           END
    FROM active_path AS path
    JOIN conversation_branches AS branch ON branch.id = path.branch_id
    WHERE branch.parent_branch_id IS NOT NULL
)
SELECT conversation_turns.id, conversation_turns.conversation_id,
       conversation_turns.handled_by_session_id, conversation_turns.branch_id,
       conversation_turns.provider_turn_id, conversation_turns.state,
       conversation_turns.requested_at
FROM conversation_turns
JOIN active_path AS path ON path.branch_id = conversation_turns.branch_id
WHERE conversation_turns.conversation_id = ?
  AND conversation_turns.promoted_to_turn_id IS NULL
  AND conversation_turns.state = 'running'
  AND conversation_turns.rolled_back_at IS NULL
  AND (path.max_sequence IS NULL OR EXISTS (
      SELECT 1 FROM conversation_messages AS lineage_message
      WHERE lineage_message.turn_id = conversation_turns.id
        AND lineage_message.sequence <= path.max_sequence
      UNION ALL
      SELECT 1 FROM conversation_activities AS lineage_activity
      WHERE lineage_activity.turn_id = conversation_turns.id
        AND lineage_activity.sequence <= path.max_sequence
  ))
ORDER BY conversation_turns.requested_at, conversation_turns.rowid;`

// LoadConversationLiveTurns preserves snapshot/authentication and provider-scope
// visibility without reading messages, activities, plans, diffs or raw history.
func (s *Store) LoadConversationLiveTurns(ctx context.Context, conversationID string) ([]domain.ConversationTurn, error) {
	tx, q, _, err := s.beginConversationSnapshot(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	branches, err := conversationBranches(ctx, q, conversationID)
	if err != nil {
		return nil, err
	}
	presentation := conversationHistoryPresentation{branchProviderScopes: make(map[string]string, len(branches))}
	for _, branch := range branches {
		presentation.branchProviderScopes[branch.ID] = branch.ProviderScopeID
		if branch.Active {
			presentation.activeProviderScopeID = branch.ProviderScopeID
		}
	}
	rows, err := tx.QueryContext(ctx, liveConversationTurnsSQL, conversationID, conversationID)
	if err != nil {
		return nil, fmt.Errorf("select live turn control: %w", err)
	}
	defer rows.Close()
	var turns []domain.ConversationTurn
	for rows.Next() {
		var turn domain.ConversationTurn
		if err := rows.Scan(&turn.ID, &turn.ConversationID, &turn.HandledBySessionID, &turn.BranchID, &turn.ProviderTurnID, &turn.State, &turn.RequestedAt); err != nil {
			return nil, fmt.Errorf("scan live turn control: %w", err)
		}
		presentation.filterInactiveProviderTurn(&turn)
		turns = append(turns, turn)
	}
	return turns, rows.Err()
}
