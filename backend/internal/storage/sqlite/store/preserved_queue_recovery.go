package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

const queueCleanupFailure = "controller ended before the turn completed"
const queueRecoveryMethod = "queue.preserved_recovered"

type recoveryAudit struct {
	RequestSHA256        string `json:"requestSha256"`
	Restored             int    `json:"restored"`
	BeforeSnapshotSHA256 string `json:"beforeSnapshotSha256"`
}

func queueRecoveryAudit(ctx context.Context, db gen.DBTX, session domain.SessionID, req domain.PreservedQueueRecoveryRequest, digest string) (domain.PreservedQueueRecoveryResult, bool, error) {
	var method, payload string
	err := db.QueryRowContext(ctx, `SELECT method,payload_json FROM conversation_provider_events WHERE conversation_id=? AND provider_event_id<>'' AND provider_event_id=? AND session_id=?`, req.ConversationID, "preserved-queue-recovery:"+digest, session).Scan(&method, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.PreservedQueueRecoveryResult{}, false, nil
	}
	if err != nil {
		return domain.PreservedQueueRecoveryResult{}, false, err
	}
	var audit recoveryAudit
	if method != queueRecoveryMethod || json.Unmarshal([]byte(payload), &audit) != nil || audit.RequestSHA256 != digest || audit.Restored != len(req.Turns) || audit.BeforeSnapshotSHA256 != req.BeforeSnapshotSHA256 {
		return domain.PreservedQueueRecoveryResult{}, false, domain.ErrPreservedQueueRecovery
	}
	return domain.PreservedQueueRecoveryResult{RequestSHA256: digest, PreviouslyRestored: audit.Restored, Dispatch: "already_applied"}, true, nil
}
func (s *Store) LookupPreservedQueueRecovery(ctx context.Context, session domain.SessionID, req domain.PreservedQueueRecoveryRequest) (domain.PreservedQueueRecoveryResult, bool, error) {
	digest, err := domain.PreservedQueueRecoveryDigest(session, req)
	if err != nil {
		return domain.PreservedQueueRecoveryResult{}, false, err
	}
	return queueRecoveryAudit(ctx, s.readDB, session, req, digest)
}

// RestorePreservedQueue changes only the exact never-started original turns.
// All proof checks, owner fences, original rows and the durable audit share one
// existing application-writer transaction. It never calls a provider.
func (s *Store) RestorePreservedQueue(ctx context.Context, session domain.SessionID, generation string, req domain.PreservedQueueRecoveryRequest, terminalEvidence []string, now time.Time) (domain.PreservedQueueRecoveryResult, error) {
	digest, err := domain.PreservedQueueRecoveryDigest(session, req)
	if err != nil {
		return domain.PreservedQueueRecoveryResult{}, err
	}
	result := domain.PreservedQueueRecoveryResult{RequestSHA256: digest, Dispatch: "held_busy"}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	err = s.inTxDB(ctx, "restore preserved queue", func(q *gen.Queries, db gen.DBTX) error {
		if previous, found, err := queueRecoveryAudit(ctx, db, session, req, digest); err != nil {
			return err
		} else if found {
			result = previous
			return nil
		}
		owner, err := q.GetSession(ctx, session)
		if err != nil {
			return err
		}
		if owner.IsTerminated || owner.SessionMode != "chat" || owner.Harness != domain.HarnessClaudeCode || owner.ControllerGeneration != generation || owner.ProviderConversationID != req.NativeID {
			return domain.ErrPreservedQueueRecovery
		}
		conv, err := q.SelectConversationByID(ctx, req.ConversationID)
		if err != nil {
			return err
		}
		if conv.CurrentSessionID == nil || *conv.CurrentSessionID != session || conv.CurrentReviewID.Valid || conv.ActiveBranchID != req.BranchID {
			return domain.ErrPreservedQueueRecovery
		}
		branch, err := q.SelectConversationBranch(ctx, gen.SelectConversationBranchParams{ConversationID: req.ConversationID, BranchID: req.BranchID})
		if err != nil {
			return err
		}
		if branch.ProviderConversationID != req.NativeID {
			return domain.ErrPreservedQueueRecovery
		}
		bound, err := q.SelectConversationTurnByID(ctx, req.BoundTurnID)
		if err != nil {
			return err
		}
		if bound.ConversationID != req.ConversationID || bound.HandledBySessionID != session || bound.HandledByReviewID.Valid || bound.BranchID != req.BranchID || bound.ProviderTurnID != req.BoundProviderTurnID || !bound.StartedAt.Valid || bound.RolledBackAt.Valid || bound.PromotedToTurnID.Valid {
			return domain.ErrPreservedQueueRecovery
		}
		var running int
		if err = db.QueryRowContext(ctx, `SELECT count(*) FROM conversation_turns WHERE conversation_id=? AND branch_id=? AND state='running' AND rolled_back_at IS NULL`, req.ConversationID, req.BranchID).Scan(&running); err != nil {
			return err
		}
		if bound.State == domain.TurnStateRunning {
			if running != 1 || bound.ControllerGeneration != generation || bound.CompletedAt.Valid || len(terminalEvidence) != 0 {
				return domain.ErrPreservedQueueRecovery
			}
		} else if bound.State == domain.TurnStateCompleted && bound.CompletedAt.Valid && bound.ErrorMessage == "" && running == 0 && len(terminalEvidence) > 0 {
			ids, _ := json.Marshal(terminalEvidence)
			proof, err := q.HasLiveRecoveryCommittedCompletion(ctx, gen.HasLiveRecoveryCommittedCompletionParams{EventIds: string(ids), ConversationID: req.ConversationID, BranchID: req.BranchID, ProviderTurnID: req.BoundProviderTurnID, NativeID: req.NativeID})
			if err != nil {
				return err
			}
			if !proof {
				return domain.ErrPreservedQueueRecovery
			}
			result.Dispatch = "ready_after_commit"
		} else {
			return domain.ErrPreservedQueueRecovery
		}
		// Validate the complete batch before any lifecycle row is changed.
		// Check reverse references and membership once for the whole batch;
		// never rescan a large conversation once per original queued item.
		slots := strings.TrimSuffix(strings.Repeat("?,", len(req.Turns)), ",")
		ids := make([]any, 0, len(req.Turns))
		for _, candidate := range req.Turns {
			ids = append(ids, candidate.ID)
		}
		var referenced bool
		if err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM conversation_turns WHERE retry_of_turn_id IN (`+slots+`) OR promoted_to_turn_id IN (`+slots+`))`, append(append([]any{}, ids...), ids...)...).Scan(&referenced); err != nil {
			return err
		}
		if referenced {
			return domain.ErrPreservedQueueRecovery
		}
		counts := map[string]int{}
		membership, err := db.QueryContext(ctx, `SELECT turn_id,count(*) FROM conversation_messages WHERE conversation_id=? AND turn_id IN (`+slots+`) GROUP BY turn_id`, append([]any{req.ConversationID}, ids...)...)
		if err != nil {
			return err
		}
		for membership.Next() {
			var id string
			var count int
			if err = membership.Scan(&id, &count); err != nil {
				membership.Close()
				return err
			}
			counts[id] = count
		}
		err = membership.Err()
		membership.Close()
		if err != nil {
			return err
		}
		for _, candidate := range req.Turns {
			var valid bool
			err = db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM conversation_turns t WHERE id=? AND conversation_id=? AND handled_by_session_id=? AND handled_by_review_id IS NULL AND branch_id=? AND controller_generation=? AND state='failed' AND error_message=? AND CAST(completed_at AS TEXT)=? AND CAST(requested_at AS TEXT)=? AND started_at IS NULL AND provider_turn_id='' AND rolled_back_at IS NULL AND promotion_started_at IS NULL AND promoted_to_turn_id IS NULL AND retry_of_turn_id IS NULL AND NOT EXISTS(SELECT 1 FROM ao_history_retention r WHERE r.object_kind='turn' AND r.object_id=t.id))`, candidate.ID, req.ConversationID, session, req.BranchID, candidate.PreviousGeneration, queueCleanupFailure, req.FailureAt, candidate.RequestedAt).Scan(&valid)
			if err != nil {
				return err
			}
			if !valid {
				return domain.ErrPreservedQueueRecovery
			}
			inputSlots := strings.TrimSuffix(strings.Repeat("?,", len(candidate.Inputs)), ",")
			inputArgs := []any{req.ConversationID, candidate.ID, req.BranchID}
			for _, input := range candidate.Inputs {
				inputArgs = append(inputArgs, input.ID)
			}
			rows, err := db.QueryContext(ctx, `SELECT id,client_message_id,COALESCE(client_payload_hash,''),text,delivery_content_json FROM conversation_messages m WHERE conversation_id=? AND turn_id=? AND role='user' AND streaming=0 AND provider_item_id='' AND branch_id=? AND id IN (`+inputSlots+`) AND NOT EXISTS(SELECT 1 FROM ao_history_retention r WHERE r.object_kind='message' AND r.object_id=m.id)`, inputArgs...)
			if err != nil {
				return err
			}
			expected := map[string]domain.PreservedQueueInputProof{}
			for _, m := range candidate.Inputs {
				expected[m.ID] = m
			}
			matched := 0
			for rows.Next() {
				var id, client, payload, text, delivery string
				if err = rows.Scan(&id, &client, &payload, &text, &delivery); err != nil {
					rows.Close()
					return err
				}
				proof, ok := expected[id]
				if !ok || client != proof.ClientMessageID || payload != proof.ClientPayloadHash || recoverySHA(text) != proof.TextSHA256 || recoverySHA(delivery) != proof.DeliverySHA256 {
					rows.Close()
					return domain.ErrPreservedQueueRecovery
				}
				matched++
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			if matched != len(expected) {
				return domain.ErrPreservedQueueRecovery
			}
			if counts[candidate.ID] != matched {
				return domain.ErrPreservedQueueRecovery
			}
		}
		audit, _ := json.Marshal(recoveryAudit{RequestSHA256: digest, Restored: len(req.Turns), BeforeSnapshotSHA256: req.BeforeSnapshotSHA256})
		projected, err := projectProviderEventTx(ctx, q, db, req.ConversationID, session, "", generation, "preserved-queue-recovery:"+digest, queueRecoveryMethod, string(audit), now, func(txCtx context.Context) error {
			for _, candidate := range req.Turns {
				changed, err := db.ExecContext(txCtx, `UPDATE conversation_turns SET state='queued',error_message='',completed_at=NULL,controller_generation=? WHERE id=? AND state='failed' AND controller_generation=? AND error_message=? AND CAST(completed_at AS TEXT)=? AND started_at IS NULL AND provider_turn_id=''`, generation, candidate.ID, candidate.PreviousGeneration, queueCleanupFailure, req.FailureAt)
				if err != nil {
					return err
				}
				n, err := changed.RowsAffected()
				if err != nil {
					return err
				}
				if n != 1 {
					return domain.ErrPreservedQueueRecovery
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !projected {
			return domain.ErrPreservedQueueRecovery
		}
		result.Restored = len(req.Turns)
		return nil
	})
	return result, err
}
func recoverySHA(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}
