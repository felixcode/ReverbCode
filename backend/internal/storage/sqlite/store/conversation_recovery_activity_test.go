package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
)

func TestRecoveredChatActivityProjection(t *testing.T) {
	for _, scenario := range []string{"recover", "ordinary-reconnect", "newer-exit", "input", "approval", "terminal", "newer-thread", "thread-waiting", "revision-race", "generation-race", "pending-input", "pending-approval"} {
		t.Run(scenario, func(t *testing.T) {
			s, session, conversation := conversationFixture(t)
			ctx := context.Background()
			if err := s.CreateAndActivateConversationBranch(ctx, session, domain.ConversationBranch{ID: "recovery-branch", ConversationID: conversation, SessionID: session, ProviderConversationID: "native", ParentBranchID: conversation + ":root", CreatedAt: histClock}, "old", histClock); err != nil {
				t.Fatal(err)
			}
			if err := s.AdoptProviderTurn(ctx, conversation, session, "old", "turn", "provider-turn", histClock); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ProjectProviderEvent(ctx, conversation, session, "old", "journal:0", "message.delta", `{"providerTurnId":"provider-turn"}`, histClock, func(context.Context) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err := s.CleanupOwnedControllerWork(ctx, session, conversation, "old", histClock.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := s.ClaimChatControllerGeneration(ctx, session, "current"); err != nil {
				t.Fatal(err)
			}
			recoveredAt := histClock.Add(3 * time.Second)
			if scenario != "ordinary-reconnect" {
				if _, err := s.RecoverLiveTransportTurn(ctx, conversation, session, "current", "native", []string{"journal"}, recoveredAt); err != nil {
					t.Fatal(err)
				}
			}
			rec, found, err := s.GetSession(ctx, session)
			if err != nil || !found {
				t.Fatal(err)
			}
			rec.Activity = domain.Activity{State: domain.ActivityExited, LastActivityAt: histClock.Add(time.Second)}
			if err := s.UpdateSession(ctx, rec); err != nil {
				t.Fatal(err)
			}
			thread := domain.ConversationThreadState{Status: domain.ThreadStatusIdle, UpdatedAt: histClock.Add(time.Second)}
			if scenario == "newer-thread" {
				thread.UpdatedAt = recoveredAt.Add(time.Second)
			}
			if scenario == "thread-waiting" {
				thread.WaitingOn = []string{"waiting_on_approval"}
			}
			if err := s.RecordThreadState(ctx, conversation, thread); err != nil {
				t.Fatal(err)
			}
			rec, _, err = s.GetSession(ctx, session)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "pending-input", "pending-approval":
				kind := domain.ActivityKindUserInput
				if scenario == "pending-approval" {
					kind = domain.ActivityKindApproval
				}
				if err := s.UpsertActivity(ctx, conversation, "provider-turn", domain.ConversationActivity{ID: "pending", Kind: kind, Status: domain.ActivityStatusPending, RequestID: "request"}, recoveredAt.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			case "newer-exit":
				rec.Activity.LastActivityAt = recoveredAt.Add(time.Second)
				if err := s.UpdateSession(ctx, rec); err != nil {
					t.Fatal(err)
				}
			case "input", "approval", "revision-race":
				changed := rec
				changed.Activity.State = domain.ActivityWaitingInput
				if scenario == "approval" {
					changed.Activity.State = domain.ActivityBlocked
				}
				if err := s.UpdateSession(ctx, changed); err != nil {
					t.Fatal(err)
				}
			case "terminal":
				if err := s.SettleTurn(ctx, conversation, "provider-turn", domain.TurnStateCompleted, "", recoveredAt.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			case "generation-race":
				if err := s.ClaimChatControllerGeneration(ctx, session, "replacement"); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "revision-race" || scenario == "generation-race" {
				applied, err := s.RestoreRecoveredChatActivity(ctx, rec, recoveredAt.Add(2*time.Second))
				if err != nil || applied {
					t.Fatalf("stale CAS applied=%v err=%v", applied, err)
				}
			} else {
				current, _, err := s.GetSession(ctx, session)
				if err != nil {
					t.Fatal(err)
				}
				if err := lifecycle.New(s, nil).MarkChatReconnected(ctx, session, current.Metadata); err != nil {
					t.Fatal(err)
				}
			}
			after, _, err := s.GetSession(ctx, session)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := s.LoadConversationSnapshot(ctx, conversation)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "recover" {
				if after.Activity.State != domain.ActivityActive || snapshot.Conversation.ThreadState.Status != domain.ThreadStatusActive {
					t.Fatalf("not working: activity=%s thread=%s", after.Activity.State, snapshot.Conversation.ThreadState.Status)
				}
				revision := after.Revision
				if err := lifecycle.New(s, nil).MarkChatReconnected(ctx, session, after.Metadata); err != nil {
					t.Fatal(err)
				}
				again, _, _ := s.GetSession(ctx, session)
				if again.Revision != revision {
					t.Fatal("reconnect repeated the activity update")
				}
			} else if after.Activity.State == domain.ActivityActive || snapshot.Conversation.ThreadState.Status != thread.Status {
				t.Fatalf("overwrote newer/ordinary state: activity=%s thread=%s", after.Activity.State, snapshot.Conversation.ThreadState.Status)
			}
		})
	}
}
