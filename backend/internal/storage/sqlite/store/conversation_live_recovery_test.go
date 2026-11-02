package store_test

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"testing"
	"time"
)

func TestRecoverLiveTransportTurnRequiresExactJournalArchiveOwnership(t *testing.T) {
	for _, scenario := range []string{"recover", "terminal-committed", "terminal-uncommitted", "cancelled", "provider-error", "completed", "wrong-native", "wrong-generation", "no-proof", "wrong-branch", "multiple-prompts", "competing-running"} {
		t.Run(scenario, func(t *testing.T) {
			s, session, conversation := conversationFixture(t)
			ctx := context.Background()
			branch := domain.ConversationBranch{ID: "live-branch", ConversationID: conversation, SessionID: session, ProviderConversationID: "native", ParentBranchID: conversation + ":root", CreatedAt: histClock}
			if err := s.CreateAndActivateConversationBranch(ctx, session, branch, "gen-1", histClock); err != nil {
				t.Fatal(err)
			}
			failureAt := histClock.Add(-time.Second)
			if err := s.RecordAccount(ctx, conversation, domain.ConversationAccount{AuthenticationState: "required", ReauthRequiredAt: &failureAt, AuthFailureID: "unchanged"}, histClock); err != nil {
				t.Fatal(err)
			}
			if err := s.AdoptProviderTurn(ctx, conversation, session, "gen-1", "turn", "provider-turn", histClock); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ProjectProviderEvent(ctx, conversation, session, "gen-1", "journal-1:0", "message.delta", `{"providerTurnId":"provider-turn"}`, histClock, func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err := s.CleanupOwnedControllerWork(ctx, session, conversation, "gen-1", histClock.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := s.ClaimChatControllerGeneration(ctx, session, "gen-2"); err != nil {
				t.Fatal(err)
			}
			ids := []string{"journal-1"}
			native, generation := "native", "gen-2"
			switch scenario {
			case "terminal-committed":
				ids = append(ids, "terminal-event")
				if _, err := s.ProjectProviderEvent(ctx, conversation, session, "gen-2", "terminal-event", "turn.completed", `{"providerTurnId":"provider-turn","turnState":"completed"}`, histClock.Add(2*time.Second), func(txCtx context.Context) error {
					return s.SettleTurn(txCtx, conversation, "provider-turn", domain.TurnStateCompleted, "", histClock.Add(2*time.Second))
				}); err != nil {
					t.Fatal(err)
				}
			case "terminal-uncommitted":
				ids = append(ids, "terminal-event")
			case "cancelled":
				if err := s.SettleTurn(ctx, conversation, "provider-turn", domain.TurnStateInterrupted, "", histClock.Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
			case "provider-error":
				if err := s.SettleTurn(ctx, conversation, "provider-turn", domain.TurnStateFailed, "provider failure", histClock.Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
			case "completed":
				if err := s.SettleTurn(ctx, conversation, "provider-turn", domain.TurnStateCompleted, "", histClock.Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
			case "wrong-native":
				native = "foreign-native"
			case "wrong-generation":
				generation = "stale-generation"
			case "no-proof":
				ids = []string{"unknown-event"}
			case "wrong-branch":
				branch.ID = "new-branch"
				branch.ParentBranchID = "live-branch"
				if err := s.CreateAndActivateConversationBranch(ctx, session, branch, "gen-2", histClock); err != nil {
					t.Fatal(err)
				}
			case "multiple-prompts", "competing-running":
				if err := s.AdoptProviderTurn(ctx, conversation, session, "gen-2", "other-turn", "other-provider", histClock); err != nil {
					t.Fatal(err)
				}
				if scenario == "multiple-prompts" {
					if _, err := s.ProjectProviderEvent(ctx, conversation, session, "gen-2", "journal-2:0", "message.delta", `{"providerTurnId":"other-provider"}`, histClock, func(context.Context) error { return nil }); err != nil {
						t.Fatal(err)
					}
					ids = append(ids, "journal-2")
					if _, err := s.CleanupOwnedControllerWork(ctx, session, conversation, "gen-2", histClock.Add(time.Second)); err != nil {
						t.Fatal(err)
					}
				}
			}
			recovered, err := s.RecoverLiveTransportTurn(ctx, conversation, session, generation, native, ids, histClock.Add(3*time.Second))
			if scenario == "terminal-committed" {
				if err != nil || recovered.ProviderTurnID != "" {
					t.Fatalf("durable terminal ACK admission failed: %+v %v", recovered, err)
				}
				return
			}
			if scenario != "recover" && scenario != "terminal-uncommitted" {
				if err == nil {
					t.Fatalf("unsafe recovery accepted: %+v", recovered)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if recovered.ID != "turn" || recovered.ProviderTurnID != "provider-turn" || recovered.State != domain.TurnStateRunning || recovered.CompletedAt != nil || recovered.StartedAt == nil || !recovered.StartedAt.Equal(histClock) {
				t.Fatalf("recovered=%+v", recovered)
			}
			if _, err := s.RecoverLiveTransportTurn(ctx, conversation, session, generation, native, ids, histClock.Add(4*time.Second)); err != nil {
				t.Fatalf("idempotent recovery: %v", err)
			}
			events, err := s.ProviderEventsSince(ctx, conversation, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			repairs := 0
			for _, event := range events {
				if event.Method == "turn.transport_recovered" {
					repairs++
				}
			}
			if repairs != 1 {
				t.Fatalf("repair events=%d", repairs)
			}
			account, err := s.ConversationAccount(ctx, conversation)
			if err != nil {
				t.Fatal(err)
			}
			if account.AuthenticationState != "required" || account.AuthFailureID != "unchanged" || account.AuthVerifiedAt != nil {
				t.Fatalf("recovery changed authentication: %+v", account)
			}
		})
	}
}
