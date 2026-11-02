package chat_test

import (
	"context"
	"errors"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	chatsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/chat"
	"sync/atomic"
	"testing"
	"time"
)

type observedDisconnectConversation struct {
	*terminatingConversation
	disposition  ports.ChatDisconnectState
	terminations atomic.Int32
	disconnected atomic.Bool
}

func (c *observedDisconnectConversation) DisconnectedProviderState() ports.ChatDisconnectState {
	if c.terminated.Load() {
		return ports.ChatDisconnectEnded
	}
	if !c.disconnected.Load() {
		return ports.ChatDisconnectNotPersistent
	}
	return c.disposition
}
func (c *observedDisconnectConversation) Terminate() error {
	c.terminations.Add(1)
	return c.terminatingConversation.Terminate()
}
func TestPersistentDisconnectRetainsOnlyUncertainOrSurvivingWork(t *testing.T) {
	for _, state := range []ports.ChatDisconnectState{ports.ChatDisconnectPreserved, ports.ChatDisconnectUnknown, ports.ChatDisconnectEnded, ports.ChatDisconnectNotPersistent} {
		for _, stoppedEvent := range []bool{false, true} {
			t.Run(string(state)+map[bool]string{false: "-bareEOF", true: "-stopped"}[stoppedEvent], func(t *testing.T) {
				provider := &observedDisconnectConversation{terminatingConversation: &terminatingConversation{fakeConversation: newFakeConversation()}, disposition: state}
				h := newHarnessWithConversation(t, provider)
				ctx := context.Background()
				turn, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "original bound input"})
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 3; i++ {
					if _, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "queued input"}); err != nil {
						t.Fatal(err)
					}
				}
				provider.disconnected.Store(true)
				if stoppedEvent {
					provider.emit(ports.ChatEvent{Kind: ports.ChatEventControllerState, ControllerState: ports.ChatControllerStopped})
				}
				_ = provider.fakeConversation.Close()
				h.ctrl.Wait()
				snap, err := h.st.LoadConversationSnapshot(ctx, h.ctrl.ConversationID())
				if err != nil {
					t.Fatal(err)
				}
				retain := state == ports.ChatDisconnectPreserved || state == ports.ChatDisconnectUnknown
				if h.ctrl.State() != ports.ChatControllerStopped {
					t.Fatal("disconnect still advertised working")
				}
				for _, row := range snap.Turns {
					if retain {
						want := domain.TurnStateQueued
						if row.ID == turn.ID {
							want = domain.TurnStateRunning
						}
						if row.State != want {
							t.Fatalf("work changed on disconnect: %s", row.State)
						}
					} else if row.State != domain.TurnStateFailed {
						t.Fatalf("confirmed end did not settle: %s", row.State)
					}
				}
				if len(snap.Turns) != 4 {
					t.Fatal("input identity/count changed")
				}
				// Close's once may already have fired, but later explicit Exit must still
				// terminate the host and settle work exactly once at the provider boundary.
				limit, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()
				if err := h.ctrl.Terminate(limit); err != nil {
					t.Fatal(err)
				}
				if err := h.ctrl.Terminate(limit); err != nil {
					t.Fatal(err)
				}
				if provider.terminations.Load() != 1 {
					t.Fatal("explicit termination lost or repeated")
				}
				after, err := h.st.LoadConversationSnapshot(ctx, h.ctrl.ConversationID())
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range after.Turns {
					if row.State != domain.TurnStateFailed {
						t.Fatal("explicit exit retained work")
					}
				}
			})
		}
	}
}
func TestExplicitTerminateWinsBeforeTransportClose(t *testing.T) {
	provider := &observedDisconnectConversation{terminatingConversation: &terminatingConversation{fakeConversation: newFakeConversation()}, disposition: ports.ChatDisconnectPreserved}
	h := newHarnessWithConversation(t, provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	turn, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "not resend"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.ctrl.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	row, err := h.st.TurnByID(ctx, turn.ID)
	if err != nil || row.State != domain.TurnStateFailed {
		t.Fatal("explicit exit lost to disconnect")
	}
}

func TestExplicitExitStillTerminatesAfterOrdinaryClose(t *testing.T) {
	provider := &observedDisconnectConversation{terminatingConversation: &terminatingConversation{fakeConversation: newFakeConversation()}, disposition: ports.ChatDisconnectPreserved}
	h := newHarnessWithConversation(t, provider)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	turn, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "original input"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.ctrl.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := h.st.TurnByID(ctx, turn.ID)
	if err != nil || before.State != domain.TurnStateRunning {
		t.Fatal("ordinary detach changed live work")
	}
	if err := h.ctrl.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.ctrl.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := h.st.TurnByID(ctx, turn.ID)
	if err != nil || after.State != domain.TurnStateFailed || provider.terminations.Load() != 1 {
		t.Fatal("ordinary Close once swallowed explicit Exit")
	}
}

// The transport has already closed, but the event buffer has not reached its
// Stopped/EOF boundary. A successful old completion must not release new work.
func TestDisconnectedBufferedCompletionKeepsQueueBeforeStopped(t *testing.T) {
	for _, disposition := range []ports.ChatDisconnectState{ports.ChatDisconnectPreserved, ports.ChatDisconnectUnknown} {
		for _, stopped := range []bool{false, true} {
			t.Run(string(disposition)+map[bool]string{false: "-EOF", true: "-Stopped"}[stopped], func(t *testing.T) {
				provider := &observedDisconnectConversation{terminatingConversation: &terminatingConversation{fakeConversation: newFakeConversation()}, disposition: disposition}
				h := newHarnessWithConversation(t, provider)
				ctx := context.Background()
				first, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "first"})
				if err != nil {
					t.Fatal(err)
				}
				queued, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "original queued input", ClientMessageID: "original-client"})
				if err != nil {
					t.Fatal(err)
				}
				provider.disconnected.Store(true)
				provider.emit(ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderTurnID: first.ProviderTurnID, TurnState: domain.TurnStateCompleted})
				if stopped {
					provider.emit(ports.ChatEvent{Kind: ports.ChatEventControllerState, ControllerState: ports.ChatControllerStopped})
				}
				_ = provider.fakeConversation.Close()
				h.ctrl.Wait()
				row, err := h.st.TurnByID(ctx, queued.ID)
				if err != nil || row.State != domain.TurnStateQueued || row.ProviderTurnID != "" || provider.sendCallCount() != 1 {
					t.Fatal("buffered completion consumed retained queued input")
				}
				if h.ctrl.State() != ports.ChatControllerStopped {
					t.Fatal("disconnected controller not stopped")
				}
				// An API caller may already hold this controller reference.
				if _, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "must not append or send"}); !errors.Is(err, chatsvc.ErrNoController) {
					t.Fatalf("stale controller Send: %v", err)
				}
				snap, err := h.st.LoadConversationSnapshot(ctx, h.ctrl.ConversationID())
				if err != nil || len(snap.Turns) != 2 {
					t.Fatal("rejected stale send mutated durable input set")
				}
			})
		}
	}
}

type observedCompactionDisconnect struct {
	*compactingConversation
	disconnected atomic.Bool
}

func (c *observedCompactionDisconnect) DisconnectedProviderState() ports.ChatDisconnectState {
	if c.disconnected.Load() {
		return ports.ChatDisconnectUnknown
	}
	return ports.ChatDisconnectNotPersistent
}
func TestDisconnectedBufferedCompactionAndManualDrainKeepQueue(t *testing.T) {
	provider := &observedCompactionDisconnect{compactingConversation: newCompactingConversation()}
	h := newHarnessWithConversation(t, provider)
	ctx := context.Background()
	if _, err := h.ctrl.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	queued, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "after compact"})
	if err != nil || queued.State != domain.TurnStateQueued {
		t.Fatal(err)
	}
	provider.disconnected.Store(true)
	// Explicit drain must preserve the row even before the buffered compaction.
	if err := h.svc.DrainQueued(ctx, testSession); err != nil {
		t.Fatal(err)
	}
	provider.emit(ports.ChatEvent{Kind: ports.ChatEventCompacted})
	_ = provider.fakeConversation.Close()
	h.ctrl.Wait()
	row, err := h.st.TurnByID(ctx, queued.ID)
	if err != nil || row.State != domain.TurnStateQueued || row.ProviderTurnID != "" || provider.sendCallCount() != 0 {
		t.Fatal("compaction or explicit drain consumed retained input")
	}
}

func TestConnectedCompletionStillDrainsExactlyOneInput(t *testing.T) {
	provider := &observedDisconnectConversation{terminatingConversation: &terminatingConversation{fakeConversation: newFakeConversation()}, disposition: ports.ChatDisconnectPreserved}
	h := newHarnessWithConversation(t, provider)
	ctx := context.Background()
	first, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "first"})
	if err != nil {
		t.Fatal(err)
	}
	next, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ctrl.Send(ctx, ports.ChatUserMessage{Text: "third"}); err != nil {
		t.Fatal(err)
	}
	provider.emit(ports.ChatEvent{Kind: ports.ChatEventTurnCompleted, ProviderTurnID: first.ProviderTurnID, TurnState: domain.TurnStateCompleted})
	deadline := time.Now().Add(3 * time.Second)
	for {
		row, err := h.st.TurnByID(ctx, next.ID)
		if err != nil {
			t.Fatal(err)
		}
		if row.State == domain.TurnStateRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connected queue did not drain")
		}
		time.Sleep(time.Millisecond)
	}
	if provider.sendCallCount() != 2 {
		t.Fatal("connected completion dispatched wrong number of inputs")
	}
}
