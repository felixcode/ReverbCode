package chat

import (
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"testing"
	"time"
)

func TestRetentionReplayDigestCountsAndPromptAliases(t *testing.T) {
	turns := []domain.ConversationTurn{{ID: "old", ProviderTurnID: "original", State: domain.TurnStateCompleted}}
	messages := []domain.ConversationMessage{
		{ID: "u", TurnID: "old", Role: domain.MessageRoleUser, Text: "[AO history expired]", ContentExpired: true, RetentionSemanticSHA256: retentionDigest(nativeHistoryMessageFingerprint(domain.MessageRoleUser, "hello")), RetentionPromptSHA256: retentionDigest("hello")},
		{ID: "a1", TurnID: "old", Role: domain.MessageRoleAssistant, Text: "[AO history expired]", ContentExpired: true, RetentionSemanticSHA256: retentionDigest(nativeHistoryMessageFingerprint(domain.MessageRoleAssistant, "yes"))},
		{ID: "a2", TurnID: "old", Role: domain.MessageRoleAssistant, Text: "[AO history expired]", ContentExpired: true, RetentionSemanticSHA256: retentionDigest(nativeHistoryMessageFingerprint(domain.MessageRoleAssistant, "yes"))},
	}
	events := []ports.ChatEvent{
		{Kind: ports.ChatEventUserMessageCompleted, ProviderTurnID: "changed", ProviderItemID: "new-u", Text: "hello"},
		{Kind: ports.ChatEventMessageCompleted, ProviderTurnID: "changed", ProviderItemID: "new-a1", Text: "yes"},
		{Kind: ports.ChatEventMessageCompleted, ProviderTurnID: "changed", ProviderItemID: "new-a2", Text: "yes"},
		{Kind: ports.ChatEventMessageCompleted, ProviderTurnID: "changed", ProviderItemID: "legitimate-third", Text: "yes"},
		{Kind: ports.ChatEventUserMessageCompleted, ProviderTurnID: "genuinely-new", ProviderItemID: "next", Text: "hello"},
	}
	got := reconcileNativeHistory(events, turns, messages, nil)
	if len(got) != 2 || got[0].ProviderItemID != "legitimate-third" || got[1].ProviderTurnID != "genuinely-new" {
		t.Fatalf("digest count/turn scoping failed: %#v", got)
	}
}

func TestRetentionActivityDigestAndHighWater(t *testing.T) {
	now := time.Now()
	turns := []domain.ConversationTurn{{ID: "t", ProviderTurnID: "native", State: domain.TurnStateCompleted, HandledBySessionID: "s", RequestedAt: now}}
	detail := []byte(`{"b":2,"a":1}`)
	activities := []domain.ConversationActivity{{ID: "a", TurnID: "t", Sequence: 2, Kind: domain.ActivityKindCommand, Status: domain.ActivityStatusCompleted, Summary: "[AO history expired]", ContentExpired: true, RetentionSemanticSHA256: retentionDigest(nativeHistoryActivityFingerprint(domain.ActivityKindCommand, domain.ActivityStatusCompleted, "original", detail))}}
	events := []ports.ChatEvent{{Kind: ports.ChatEventActivityCompleted, ProviderTurnID: "native", ProviderItemID: "changed", ActivityKind: domain.ActivityKindCommand, ActivityStatus: domain.ActivityStatusCompleted, Summary: "original", Detail: detail}, {Kind: ports.ChatEventTurnCompleted, ProviderTurnID: "native", TurnState: domain.TurnStateCompleted}}
	if got := reconcileNativeHistory(events, turns, nil, activities); len(got) != 1 || got[0].Kind != ports.ChatEventTurnCompleted {
		t.Fatalf("expired activity replay duplicated: %#v", got)
	}
	checkpoint := nativeHistoryCheckpoint{}
	checkpoint.captureAOHighWater("s", turns, nil, activities)
	if got := checkpoint.mismatches(events, turns, nil, activities); len(got) != 0 {
		t.Fatalf("expired highwater rejected valid original replay: %v", got)
	}
}

func TestRetentionApproximateReplayRejectsExpiredStub(t *testing.T) {
	_, _, err := buildApproximateReplayContext([]domain.ConversationMessage{{Sequence: 1, Role: domain.MessageRoleUser, Text: "[AO history expired]", ContentExpired: true}}, 0, 1)
	if err != domain.ErrHistoryContentExpired {
		t.Fatalf("expired stub became replay context: %v", err)
	}
}
