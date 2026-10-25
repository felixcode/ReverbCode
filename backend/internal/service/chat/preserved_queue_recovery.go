package chat

import (
	"context"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type preservedQueueRecoveryStore interface {
	LookupPreservedQueueRecovery(context.Context, domain.SessionID, domain.PreservedQueueRecoveryRequest) (domain.PreservedQueueRecoveryResult, bool, error)
	RestorePreservedQueue(context.Context, domain.SessionID, string, domain.PreservedQueueRecoveryRequest, []string, time.Time) (domain.PreservedQueueRecoveryResult, error)
}

// RecoverPreservedQueue is explicit local maintenance, never a generic retry.
// The only dispatch is the existing post-commit FIFO drain after exact completed
// host evidence. A live original prompt stays busy and receives no Send call.
func (s *Service) RecoverPreservedQueue(ctx context.Context, session domain.SessionID, req domain.PreservedQueueRecoveryRequest) (domain.PreservedQueueRecoveryResult, error) {
	if _, err := domain.PreservedQueueRecoveryDigest(session, req); err != nil {
		return domain.PreservedQueueRecoveryResult{}, err
	}
	store, ok := s.store.(preservedQueueRecoveryStore)
	if !ok {
		return domain.PreservedQueueRecoveryResult{}, domain.ErrPreservedQueueRecovery
	}
	gate := s.controllerGate(domain.SessionConversationOwner(session))
	if err := gate.lock(ctx); err != nil {
		return domain.PreservedQueueRecoveryResult{}, err
	}
	defer gate.unlock()
	// An acknowledged durable receipt survives response loss, later normal
	// dispatch, and controller replacement. A repeat never resets or drains rows.
	if previous, found, err := store.LookupPreservedQueueRecovery(ctx, session, req); err != nil {
		return domain.PreservedQueueRecoveryResult{}, err
	} else if found {
		return previous, nil
	}
	c, err := s.Controller(session)
	if err != nil {
		return domain.PreservedQueueRecoveryResult{}, err
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	live, ok := c.conv.(ports.ChatLiveReconnector)
	if !ok || !live.ReconnectedLive() {
		return domain.PreservedQueueRecoveryResult{}, domain.ErrPreservedQueueRecovery
	}
	if _, ok = c.conv.(ports.ChatDeferredTurnStarter); !ok || c.harness != domain.AgentHarness("claude-code") {
		return domain.PreservedQueueRecoveryResult{}, domain.ErrPreservedQueueRecovery
	}
	observer, ok := c.conv.(ports.ChatDisconnectObserver)
	if !ok || observer.DisconnectedProviderState() != ports.ChatDisconnectNotPersistent {
		return domain.PreservedQueueRecoveryResult{}, domain.ErrPreservedQueueRecovery
	}
	if c.disconnectedDispatchBlocked(ctx) {
		return domain.PreservedQueueRecoveryResult{}, domain.ErrPreservedQueueRecovery
	}
	c.mu.Lock()
	ready := c.state == ports.ChatControllerReady
	c.mu.Unlock()
	var evidence []string
	if ready {
		reader, ok := c.conv.(ports.ChatLiveTurnRecoveryEvidenceReader)
		if !ok {
			return domain.PreservedQueueRecoveryResult{}, domain.ErrPreservedQueueRecovery
		}
		evidence, err = reader.ReadLiveTurnRecoveryEvidence(ctx)
		if err != nil {
			return domain.PreservedQueueRecoveryResult{}, err
		}
		if len(evidence) == 0 {
			return domain.PreservedQueueRecoveryResult{}, domain.ErrPreservedQueueRecovery
		}
	}
	c.mu.Lock()
	valid := c.conversation.ID == req.ConversationID && c.conversation.ActiveBranchID == req.BranchID && c.conv.ProviderConversationID() == req.NativeID && c.reviewID == "" && c.handoff == controllerHandoffNone && !c.terminateRequested && !c.preserveProviderOnStop && c.cancelQueuedAt.IsZero() && c.dispatchingTurnID == "" && !c.compactionPending && c.operations == 0
	busy := c.state == ports.ChatControllerBusy && c.pendingTurnID == req.BoundProviderTurnID && c.ackedTurnID == req.BoundProviderTurnID
	idle := c.state == ports.ChatControllerReady && c.pendingTurnID == "" && c.ackedTurnID == "" && len(evidence) > 0
	if !valid || (!busy && !idle) {
		c.mu.Unlock()
		return domain.PreservedQueueRecoveryResult{}, domain.ErrPreservedQueueRecovery
	}
	result, err := store.RestorePreservedQueue(ctx, session, c.generation, req, evidence, s.now())
	c.mu.Unlock()
	if err != nil {
		return domain.PreservedQueueRecoveryResult{}, err
	}
	if result.Restored > 0 && idle {
		// A's disconnectedDispatchBlocked remains authoritative inside drainLocked.
		// No bespoke prompt, new turn, retry action or reorder is constructed here.
		if err := c.drainLocked(ctx, true); err != nil {
			result.Dispatch = "normal_drain_failed"
		} else {
			result.Dispatch = "normal_drain_requested"
		}
	}
	return result, nil
}
