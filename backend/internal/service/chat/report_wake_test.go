package chat_test

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func TestReportWakeFailedSendIsNotAcceptedOnRetry(t *testing.T) {
	h := newHarness(t)
	h.conv.mu.Lock()
	h.conv.sendErr = errors.New("provider unavailable; acceptance unknown")
	h.conv.mu.Unlock()
	ctx := context.Background()
	if _, err := h.svc.RelaySessionChatTurn(ctx, testSession, "real report", "report-batch:failed", ports.MessageDeliveryOptions{}); err == nil {
		t.Fatal("first provider failure was accepted")
	}
	if _, err := h.svc.RelaySessionChatTurn(ctx, testSession, "real report", "report-batch:failed", ports.MessageDeliveryOptions{}); err == nil {
		t.Fatal("failed report was falsely accepted by idempotent retry")
	}
	if got := h.conv.sendCallCount(); got != 1 {
		t.Fatalf("uncertain provider send was repeated: %d", got)
	}
}

func TestReportWakeIdleAndActiveAreSingleFlight(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	for _, key := range []string{"report-batch:idle", "report-batch:active", "report-batch:active"} {
		if _, err := h.svc.RelaySessionChatTurn(ctx, testSession, key, key, ports.MessageDeliveryOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if got := h.conv.sendCallCount(); got != 1 {
		t.Fatalf("active provider calls = %d", got)
	}
	snapshot, err := h.st.LoadConversationSnapshot(ctx, h.ctrl.ConversationID())
	if err != nil || len(snapshot.Turns) != 2 {
		t.Fatalf("durable turns = %d, err=%v", len(snapshot.Turns), err)
	}
	h.conv.emit(ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderTurnID: "provider-turn-1"})
	h.conv.emit(ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderTurnID: "provider-turn-1", TurnState: domain.TurnStateCompleted})
	h.awaitSnapshot(t, func(s store.ConversationSnapshot) bool { return len(h.conv.sentTexts()) == 2 })
	if got := h.conv.sendCallCount(); got != 2 {
		t.Fatalf("queue did not wake exactly once: %d", got)
	}
}

type reportAppendOnceStore struct {
	chatsvc.Store
	failed bool
}

func (s *reportAppendOnceStore) AppendUserMessage(ctx context.Context, conversationID string, session domain.SessionID, generation string, msg domain.ConversationMessage, turnID string, now time.Time) (bool, error) {
	if !s.failed {
		s.failed = true
		return false, syscall.ENOSPC
	}
	return s.Store.AppendUserMessage(ctx, conversationID, session, generation, msg, turnID, now)
}

func TestReportWakeRetriesBeforeProviderBoundary(t *testing.T) {
	h := newHarnessWithConversationAndStore(t, nil, func(st *store.Store) chatsvc.Store { return &reportAppendOnceStore{Store: st} })
	ctx := context.Background()
	if _, err := h.svc.RelaySessionChatTurn(ctx, testSession, "real report", "report-batch:write", ports.MessageDeliveryOptions{}); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("err=%v", err)
	}
	if _, err := h.svc.RelaySessionChatTurn(ctx, testSession, "real report", "report-batch:write", ports.MessageDeliveryOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := h.conv.sendCallCount(); got != 1 {
		t.Fatalf("provider calls=%d", got)
	}
}
