package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// RecoverLiveTransportTurn restores only the unique prompt proven by the same
// live host's journal and AO's archived event IDs. The existing provider-event
// transaction fences ownership, archives the repair, and changes the original
// turn atomically. It neither submits a prompt nor asserts authentication.
func (s *Store) RecoverLiveTransportTurn(ctx context.Context, conversationID string, session domain.SessionID, generation, nativeID string, eventIDs []string, now time.Time) (domain.ConversationTurn, error) {
	if nativeID == "" || len(eventIDs) == 0 {
		return domain.ConversationTurn{}, errors.New("live turn recovery evidence missing")
	}
	encoded, err := json.Marshal(eventIDs)
	if err != nil {
		return domain.ConversationTurn{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var restored domain.ConversationTurn
	err = s.inTx(ctx, "recover live transport turn", func(q *gen.Queries) error {
		owner, err := q.GetSession(ctx, session)
		if err != nil {
			return err
		}
		if owner.ControllerGeneration != generation || owner.ProviderConversationID != nativeID || owner.SessionMode != "chat" || owner.IsTerminated {
			return errors.New("live turn recovery owner changed")
		}
		conv, err := q.SelectConversationByID(ctx, conversationID)
		if err != nil {
			return err
		}
		if conv.CurrentSessionID == nil || *conv.CurrentSessionID != session || conv.CurrentReviewID.Valid {
			return errors.New("live turn recovery conversation owner mismatch")
		}
		branch, err := q.SelectConversationBranch(ctx, gen.SelectConversationBranchParams{ConversationID: conversationID, BranchID: conv.ActiveBranchID})
		if err != nil {
			return err
		}
		if branch.ProviderConversationID != nativeID {
			return errors.New("live turn recovery native branch mismatch")
		}
		candidates, err := q.SelectLiveRecoveryProviderTurns(ctx, gen.SelectLiveRecoveryProviderTurnsParams{EventIds: string(encoded), ConversationID: conversationID, BranchID: conv.ActiveBranchID})
		if err != nil {
			return err
		}
		if len(candidates) != 1 {
			return fmt.Errorf("live turn recovery requires one archived prompt, found %d", len(candidates))
		}
		turn, err := q.SelectConversationTurnByProviderID(ctx, gen.SelectConversationTurnByProviderIDParams{ConversationID: conversationID, ProviderTurnID: candidates[0]})
		if err != nil {
			return err
		}
		if turn.BranchID != conv.ActiveBranchID || turn.HandledBySessionID != session || turn.HandledByReviewID.Valid || turn.RolledBackAt.Valid || turn.PromotedToTurnID.Valid || !turn.StartedAt.Valid {
			return errors.New("live turn recovery candidate ownership mismatch")
		}
		turns, err := q.SelectConversationTurns(ctx, conversationID)
		if err != nil {
			return err
		}
		for _, other := range turns {
			if other.ID != turn.ID && other.BranchID == conv.ActiveBranchID && other.State == domain.TurnStateRunning && !other.RolledBackAt.Valid {
				return errors.New("live turn recovery has competing running work")
			}
		}
		if turn.State == domain.TurnStateRunning && turn.ControllerGeneration == generation {
			restored = turnToDomain(turn)
			return nil
		}
		if turn.State != domain.TurnStateFailed || turn.ErrorMessage != "controller ended before the turn completed" || !turn.CompletedAt.Valid {
			return errors.New("live turn recovery requires transport cleanup failure")
		}
		payload, _ := json.Marshal(map[string]any{"providerTurnId": turn.ProviderTurnID, "turnState": "running", "reason": "transport_cleanup_reconciled", "previousGeneration": turn.ControllerGeneration, "evidenceCount": len(eventIDs)})
		projected, projectErr := projectProviderEventTx(ctx, q, conversationID, session, "", generation, "live-turn-recovery:"+turn.ID+":"+turn.CompletedAt.Time.UTC().Format(time.RFC3339Nano), "turn.transport_recovered", string(payload), now, func(txCtx context.Context) error {
			rows, err := q.RestoreTransportFailedConversationTurn(txCtx, gen.RestoreTransportFailedConversationTurnParams{Generation: generation, TurnID: turn.ID, ConversationID: conversationID, BranchID: conv.ActiveBranchID})
			if err != nil {
				return err
			}
			if rows != 1 {
				return errors.New("live turn recovery candidate changed")
			}
			return nil
		})
		if projectErr != nil {
			return projectErr
		}
		if !projected {
			return errors.New("live turn recovery audit already exists without restored state")
		}
		turn.State = domain.TurnStateRunning
		turn.ErrorMessage = ""
		turn.CompletedAt = sql.NullTime{}
		turn.ControllerGeneration = generation
		restored = turnToDomain(turn)
		return nil
	})
	return restored, err
}
