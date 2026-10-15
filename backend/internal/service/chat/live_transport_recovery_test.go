package chat_test

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	store "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

type transportRecoveryConversation struct {
	*liveReconnectedConversation
	ids       []string
	activated string
}

func (c *transportRecoveryConversation) ReadLiveTurnRecoveryEvidence(context.Context) ([]string, error) {
	return c.ids, nil
}
func (c *transportRecoveryConversation) ActivateLiveReconnect(_ context.Context, id string) error {
	c.activated = id
	if id == "" {
		return ports.ErrChatRecoveryInconclusive
	}
	return nil
}

func TestServiceLiveTransportRecoveryDoesNotRepeatPrompt(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	var count atomic.Int32
	newID := func() string { return fmt.Sprintf("transport-recovery-%d", count.Add(1)) }
	firstProvider := &terminatingConversation{fakeConversation: newFakeConversation()}
	first := chatsvc.New(chatsvc.Options{Store: st, Reader: fullSnapshotReader(st), Sessions: st, Drivers: fakeRegistry{driver: fakeDriver{conv: firstProvider}}, Log: slog.New(slog.DiscardHandler), NewID: newID})
	firstController, err := first.Start(ctx, chatsvc.StartConfig{SessionID: testSession, ProjectID: testProject, Harness: domain.HarnessCodex, WorkspacePath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := firstController.Send(ctx, ports.ChatUserMessage{Text: "synthetic ongoing prompt"})
	if err != nil {
		t.Fatal(err)
	}
	firstProvider.emit(ports.ChatEvent{Kind: ports.ChatEventTurnStarted, ProviderEventID: "prompt-event:0", ProviderTurnID: turn.ProviderTurnID, ProviderConversationID: firstProvider.ProviderConversationID()})
	h := &harness{st: st, ctrl: firstController}
	h.awaitSnapshot(t, func(snapshot store.ConversationSnapshot) bool {
		for _, item := range snapshot.Turns {
			if item.ID == turn.ID && item.State == domain.TurnStateRunning {
				return true
			}
		}
		return false
	})
	deadline := time.Now().Add(time.Second)
	for {
		events, err := st.ProviderEventsSince(ctx, firstController.ConversationID(), 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, event := range events {
			if event.ProviderEventID == "prompt-event:0" {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("provider evidence did not project")
		}
		time.Sleep(time.Millisecond)
	}
	// Simulate the old transport overflow: stream ends while its host survives.
	_ = firstProvider.Close()
	firstController.Wait()
	before, err := st.TurnByID(ctx, turn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.State != domain.TurnStateFailed || before.ErrorMessage != "controller ended before the turn completed" {
		t.Fatalf("old cleanup did not reproduce: %+v", before)
	}
	secondProvider := &transportRecoveryConversation{liveReconnectedConversation: &liveReconnectedConversation{nativeHistoryConversation: &nativeHistoryConversation{fakeConversation: newFakeConversation()}}, ids: []string{"prompt-event"}}
	second := chatsvc.New(chatsvc.Options{Store: st, Reader: fullSnapshotReader(st), Sessions: st, Drivers: fakeRegistry{driver: fakeDriver{conv: secondProvider}}, Log: slog.New(slog.DiscardHandler), NewID: newID})
	t.Cleanup(func() { second.StopAll(context.Background()) })
	secondController, err := second.Start(ctx, chatsvc.StartConfig{SessionID: testSession, ProjectID: testProject, Harness: domain.HarnessCodex, WorkspacePath: t.TempDir(), ProviderConversationID: firstProvider.ProviderConversationID()})
	if err != nil {
		t.Fatal(err)
	}
	if secondController.State() != ports.ChatControllerBusy || secondProvider.activated != turn.ProviderTurnID {
		t.Fatalf("turn not restored: state=%s activated=%s", secondController.State(), secondProvider.activated)
	}
	secondProvider.mu.Lock()
	sends := secondProvider.sendCalls
	secondProvider.mu.Unlock()
	if sends != 0 {
		t.Fatalf("live recovery repeated prompt: %d", sends)
	}
	after, err := st.TurnByID(ctx, turn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != domain.TurnStateRunning || after.CompletedAt != nil {
		t.Fatalf("original turn not restored: %+v", after)
	}
}
