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
		candidates, err := q.SelectLiveRecoveryProviderTurns(ctx, gen.SelectLiveRecoveryProviderTurnsParams{EventIds: string(encoded), ConversationID: conversationID, BranchID: conv.ActiveBranchID, NativeID: nativeID})
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
		if turn.State == domain.TurnStateCompleted {
			committed, err := q.HasLiveRecoveryCommittedCompletion(ctx, gen.HasLiveRecoveryCommittedCompletionParams{EventIds: string(encoded), ConversationID: conversationID, BranchID: conv.ActiveBranchID, ProviderTurnID: turn.ProviderTurnID, NativeID: nativeID})
			if err != nil {
				return err
			}
			if committed {
				return nil
			} // Original activator may ACK this proven durable terminal.
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

// RestoreRecoveredChatActivity is the lifecycle reconnect correction for a
// transport-failed turn already restored from exact journal/archive evidence.
// Proof, pending interactions, terminal state and revision CAS share one write
// transaction, so a newer input/terminal/owner can never be painted working.
func (s *Store) RestoreRecoveredChatActivity(ctx context.Context, rec domain.SessionRecord, now time.Time) (bool, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	applied := false
	err := s.inTx(ctx, "restore recovered Chat activity", func(q *gen.Queries) error {
		proofs, err := q.SelectLiveTransportRecoveryProjection(ctx, gen.SelectLiveTransportRecoveryProjectionParams{SessionID: &rec.ID, NativeID: rec.Metadata.ProviderConversationID})
		if err != nil {
			return err
		}
		if len(proofs) != 1 {
			return nil
		}
		proof := proofs[0]
		var thread domain.ConversationThreadState
		if proof.ThreadStateJson.Valid {
			if err := json.Unmarshal([]byte(proof.ThreadStateJson.String), &thread); err != nil {
				return err
			}
			// A newer provider lifecycle fact, including a terminal idle, is
			// authoritative even if a stale session exit still needs correction.
			if thread.UpdatedAt.After(proof.ReceivedAt) || len(thread.WaitingOn) > 0 || thread.ArchivedAt != nil || thread.ClosedAt != nil {
				return nil
			}
		}
		rows, err := q.RestoreRecoveredChatActivity(ctx, gen.RestoreRecoveredChatActivityParams{Now: now, SessionID: rec.ID, ExpectedRevision: rec.Revision, Generation: rec.Metadata.ControllerGeneration, NativeID: rec.Metadata.ProviderConversationID, RecoveredAt: proof.ReceivedAt})
		if err != nil || rows != 1 {
			return err
		}
		applied = true
		if thread.Status == domain.ThreadStatusIdle {
			thread.Status = domain.ThreadStatusActive
			thread.UpdatedAt = now
			encoded, err := json.Marshal(thread)
			if err != nil {
				return err
			}
			return q.UpdateConversationThreadState(ctx, gen.UpdateConversationThreadStateParams{ID: proof.ConversationID, ThreadStateJson: sql.NullString{String: string(encoded), Valid: true}})
		}
		return nil
	})
	return applied && err == nil, err
}
