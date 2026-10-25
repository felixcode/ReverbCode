package chat_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type countingArchiveStore struct {
	chatsvc.Store
	actual      *sqlite.Store
	projections atomic.Int32
	lookups     atomic.Int32
}

func (s *countingArchiveStore) ArchivedProviderEventIDs(ctx context.Context, conv string, session domain.SessionID, review, generation string, ids []string) (map[string]bool, error) {
	s.lookups.Add(1)
	return s.actual.ArchivedProviderEventIDs(ctx, conv, session, review, generation, ids)
}
func (s *countingArchiveStore) ProjectProviderEvent(ctx context.Context, conv string, session domain.SessionID, generation, id, method, payload string, now time.Time, project func(context.Context) error) (bool, error) {
	s.projections.Add(1)
	return s.Store.ProjectProviderEvent(ctx, conv, session, generation, id, method, payload, now, project)
}

func TestColdHistorySkipsCommittedArchiveBeforeMarshalAndWrites(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	conv, err := st.CreateConversation(ctx, "cold-archive", domain.ConversationScopeSession, testProject, testSession, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err = st.ClaimChatControllerGeneration(ctx, testSession, "seed"); err != nil {
		t.Fatal(err)
	}
	var events []ports.ChatEvent
	padding := strings.Repeat("s", 100*1024)
	for i := 0; i < 1024; i++ {
		id := fmt.Sprintf("archive-%04d:suffix", i)
		text := fmt.Sprintf("%d%s", i, padding)
		payload := fmt.Sprintf(`{"kind":"controller_state","text":%q}`, text)
		// Original control-state events have no durable projection beyond archive;
		// this commits the same atomic archive boundary as the normal callback.
		if _, err := st.ProjectProviderEvent(ctx, conv.ID, testSession, "seed", id, string(ports.ChatEventControllerState), payload, time.Now(), func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
		events = append(events, ports.ChatEvent{Kind: ports.ChatEventControllerState, ProviderEventID: id, Text: text, ControllerState: ports.ChatControllerReady})
	}
	// Same prefix, different exact ID must still take original atomic projection.
	events = append(events, ports.ChatEvent{Kind: ports.ChatEventControllerState, ProviderEventID: "archive-0000:new-suffix", ControllerState: ports.ChatControllerReady})
	wrapped := &countingArchiveStore{Store: st, actual: st}
	provider := &nativeHistoryConversation{fakeConversation: newFakeConversation(), events: events}
	var seq atomic.Int32
	svc := chatsvc.New(chatsvc.Options{Store: wrapped, Sessions: st, Reader: fullSnapshotReader(st), Drivers: fakeRegistry{driver: fakeDriver{conv: provider}}, Log: slog.New(slog.DiscardHandler), NewID: func() string { return fmt.Sprintf("cold-%d", seq.Add(1)) }})
	t.Cleanup(func() { svc.StopAll(context.Background()) })
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ctrl, err := svc.Start(bounded, chatsvc.StartConfig{SessionID: testSession, ProjectID: testProject, Harness: domain.HarnessCodex, WorkspacePath: t.TempDir(), ProviderConversationID: "thread-1", HistoryMode: ports.ChatHistoryImport})
	if err != nil {
		t.Fatal(err)
	}
	if wrapped.projections.Load() != 1 || wrapped.lookups.Load() != 5 {
		t.Fatalf("archive skip did not precede marshal/write: projections=%d lookups=%d", wrapped.projections.Load(), wrapped.lookups.Load())
	}
	if provider.reads.Load() != 1 || len(provider.sentTexts()) != 0 {
		t.Fatal("cold validation was skipped or prompt resent")
	}
	// A subsequent import checks fresh committed identities, not a sticky cache.
	ids, err := st.ArchivedProviderEventIDs(ctx, conv.ID, testSession, "", ctrl.Generation(), []string{"archive-0000:new-suffix"})
	if err != nil || !ids["archive-0000:new-suffix"] {
		t.Fatalf("new suffix was not committed: %v", err)
	}
}
