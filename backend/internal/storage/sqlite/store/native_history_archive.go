package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// ArchivedProviderEventIDs is a bounded identity-only read. Existing archive rows
// prove archive+projection committed together; no payload is loaded or rewritten.
func (s *Store) ArchivedProviderEventIDs(ctx context.Context, conversationID string, session domain.SessionID, reviewID, generation string, ids []string) (map[string]bool, error) {
	if len(ids) > 256 {
		return nil, fmt.Errorf("native history identity batch too large")
	}
	if db, ok := ctx.Value(projectionDBKey{}).(gen.DBTX); ok && db != nil {
		return archivedProviderEventIDs(ctx, db, conversationID, session, reviewID, generation, ids)
	}
	tx, err := s.readDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return archivedProviderEventIDs(ctx, tx, conversationID, session, reviewID, generation, ids)
}

func archivedProviderEventIDs(ctx context.Context, db gen.DBTX, conversationID string, session domain.SessionID, reviewID, generation string, ids []string) (map[string]bool, error) {
	var current string
	if reviewID != "" {
		var owner, mode string
		if err := db.QueryRowContext(ctx, `SELECT controller_generation,session_id,interface_mode FROM review WHERE id=?`, reviewID).Scan(&current, &owner, &mode); err != nil {
			return nil, err
		}
		if owner != string(session) || mode != "chat" {
			return nil, fmt.Errorf("native history reviewer owner changed")
		}
	} else if err := db.QueryRowContext(ctx, `SELECT controller_generation FROM sessions WHERE id=?`, session).Scan(&current); err != nil {
		return nil, err
	}
	if current != generation {
		return nil, fmt.Errorf("native history controller generation changed")
	}
	known := make(map[string]bool)
	args := []any{conversationID}
	for _, id := range ids {
		if id != "" {
			args = append(args, id)
		}
	}
	if len(args) == 1 {
		return known, nil
	}
	query := `SELECT provider_event_id FROM conversation_provider_events WHERE conversation_id=? AND provider_event_id<>'' AND provider_event_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(args)-1), ",") + `)`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		known[id] = true
	}
	return known, rows.Err()
}
